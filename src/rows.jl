using ChunkedBase
using ChunkedCSV
using SentinelArrays
using Parsers: PosLen31
using Tables: Schema

_subset_columns!(_, ::Nothing, ::Nothing) = nothing
_subset_columns!(parsing_ctx, select::T, ::Nothing) where {T} = __subset_columns!(parsing_ctx, true, select)
_subset_columns!(parsing_ctx, ::Nothing, drop::T) where {T} = __subset_columns!(parsing_ctx, false, drop)
_subset_columns!(_, ::T, ::S) where {T,S} = throw(ArgumentError("cannot specify both select and drop"))

function _drop_col!(ctx, idx)
    deleteat!(ctx.header, idx)
    deleteat!(ctx.schema, idx)
    ctx.enum_schema[idx] = ChunkedCSV.Enums.SKIP
    return nothing
end

function __subset_columns!(ctx, flip::Bool, cols::Function)
    idx = length(ctx.header)
    for name in Iterators.reverse(ctx.header)
        xor(flip, cols(idx, name)) && _drop_col!(ctx, idx)
        idx -= 1
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{Bool})
    idx = length(ctx.header)
    idx != length(cols) && throw(ArgumentError("invalid number of columns: $(length(cols))"))
    for flag in Iterators.reverse(cols)
        xor(flip, flag) && _drop_col!(ctx, idx)
        idx -= 1
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{<:Integer})
    isempty(cols) && throw(ArgumentError("empty column index"))
    cols = sort(cols)
    last_idx = length(ctx.header)
    cols[begin] < 1 && throw(ArgumentError("invalid column index: $(cols[begin])"))
    cols[end] > last_idx && throw(ArgumentError("invalid column index: $(cols[end])"))

    for idx in last_idx:-1:1
        xor(flip, insorted(idx, cols)) && _drop_col!(ctx, idx)
    end
    return nothing
end
function __subset_columns!(ctx, flip::Bool, cols::AbstractVector{Symbol})
    isempty(cols) && throw(ArgumentError("empty column name"))
    s = Set(cols)
    idx = length(ctx.header)
    for name in Iterators.reverse(ctx.header)
        xor(flip, !isnothing(pop!(s, name, nothing))) && _drop_col!(ctx, idx)
        idx -= 1
    end
    !isempty(s) && throw(ArgumentError("invalid column names: $(collect(s))"))
    return nothing
end
__subset_columns!(ctx, flip::Bool, cols::AbstractVector{<:AbstractString}) = __subset_columns!(ctx, flip, map(Symbol, cols))

# TODO: this is a WIP
_sentinel(ms::Missing, mss::Missing) = missing
_sentinel(ms::Missing, mss::String) = [mss]
_sentinel(ms::Missing, mss::Nothing) = mss
_sentinel(ms::Missing, mss::Vector{String}) = mss
_sentinel(ms, mss) = _sentinel(mss, ms)

const CSVPayload = ChunkedBase.ParsedPayload{ChunkedCSV.TaskResultBuffer, ChunkedCSV.ParsingContext}
const CSVPayloadOrderer = ChunkedBase.PayloadOrderer{ChunkedCSV.TaskResultBuffer, ChunkedCSV.ParsingContext}

# To overload consume! method in ChunkedCSV and to gather the results
struct CSVRowsContext <: ChunkedBase.AbstractConsumeContext
    queue::CSVPayloadOrderer
end

# The `state` in `Base.iterate(iter::Rows, state)`
mutable struct CSVRowsIteratorState
    payload::CSVPayload
    row::Int
    indicators_idx::Int
end

# The return type of `Base.iterate(iter::Rows, state::CSVRowsIteratorState)`
struct Row2{V,StringType} <: Tables.AbstractRow
    values::Vector{V}
    state::CSVRowsIteratorState
end

# What we return to the user when that user calls CSV.CSVRows
struct Rows{V,CustomTypes,StringType}
    name::String
    consume_ctx::CSVRowsContext
    bufferschema::Vector{DataType}
    names::Vector{Symbol}
    buffer::Vector{V}
    options::Parsers.Options # needed for detect/parse in Row2
end

function Base.show(io::IO, r::Rows)
    println(io, "CSV.Rows(\"$(r.name)\"):")
    println(io, "Size: $(length(r.bufferschema))")
    show(io, Tables.schema(r))
end

struct PosLen31String
    buf::Vector{UInt8}
    poslen::PosLen31
    e::UInt8
end

# TODO: something better
Base.show(io::IO, s::PosLen31String) = print(io, "pls$(repr(Parsers.getstring(s.buf, s.poslen, s.e)))31")
Base.collect(s::PosLen31String) = Parsers.getstring(s.buf, s.poslen, s.e)

# The `AbstractRow` overides `getproperty` to allow for field access
getnames(r::Row2) = getfield(r, :state).payload.parsing_ctx.header
getschema(r::Row2) = getfield(r, :state).payload.parsing_ctx.schema
getvalues(r::Row2) = getfield(r, :values)
getbuf(r::Row2) = getfield(r, :state).payload.chunking_ctx.bytes
getescapechar(r::Row2) = getfield(r, :state).payload.parsing_ctx.escapechar
getoptions(r::Row2) = getfield(r, :state).payload.parsing_ctx.options

Base.checkbounds(r::Row2, i) = 0 < i < length(r)

# Tables.jl interface
Tables.columnnames(r::Row2) = getnames(r)

Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
Base.@propagate_inbounds function Tables.getcolumn(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    type = @inbounds getschema(r)[i]
    return unsafe_getcolumn(r, type, i)
end
Base.@propagate_inbounds function Tables.getcolumn(r::Row2{S}, ::Type{T}, i::Int, nm::Symbol) where {S,T <: S}
    @boundscheck checkbounds(r, i)
    return unsafe_getcolumn(r, T, i)
end

function unsafe_getcolumn(r::Row2{Union{PosLen31,Missing},ST}, @nospecialize(type), i::Int) where {ST}
    @inbounds if type === PosLen31
        pl = getvalues(r)[i]::Union{Missing,PosLen31}
        ismissing(pl) && return missing
        buf = getbuf(r)
        e = getescapechar(r)
        if ST === String
            return Parsers.getstring(buf, pl::PosLen31, e)::ST
        else
            return PosLen31String(buf, pl::PosLen31, e)::ST
        end
    else
        return missing
    end
end
function unsafe_getcolumn(r::Row2{Union{T,S,Missing}}, @nospecialize(type), i::Int) where {T,S}
    return @inbounds getvalues(r)[i]
end
function unsafe_getcolumn(r::Row2{Union{PosLen31,S,Missing},ST}, @nospecialize(type), i::Int) where {S,ST}
    @inbounds if type === PosLen31
        pl = getvalues(r)[i]::Union{Missing,PosLen31}
        ismissing(pl) && return missing
        buf = getbuf(r)
        e = getescapechar(r)
        if ST === String
            return Parsers.getstring(buf, pl::PosLen31, e)::ST
        else
            return PosLen31String(buf, pl::PosLen31, e)::ST
        end
    elseif type === Missing
        return missing
    else
        return getvalues(r)[i]::S
    end
end

stringsonly() = throw(ArgumentError("only strings are allowed in this column"))

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2{V,String}, i::Int) where {T,V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa String || stringsonly()
        res = Parsers.xparse(T, val, 1, ncodeunits(val), getoptions(r))
    end
    return Parsers.ok(res.code) ? (res.val::T) : missing
end
Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2{V,PosLen31String}, i::Int) where {T,V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa PosLen31String || stringsonly()
        poslen = val.poslen
        poslen.missingvalue && return missing
        pos = poslen.pos
        res = Parsers.xparse(T, getbuf(r), pos, pos + poslen.len - 1, getoptions(r))
    end
    return Parsers.ok(res.code) ? (res.val::T) : missing
end


Base.@propagate_inbounds function detect(r::Row2{V,String}, i::Int) where {V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa String || stringsonly()
        code, tlen, x, xT = detect(pass, val, 1, ncodeunits(val), getoptions(r))
        return x === nothing ? r[i] : x
    end
end
Base.@propagate_inbounds function detect(r::Row2{V,PosLen31String}, i::Int) where {V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa PosLen31String || stringsonly()
        poslen = val.poslen
        poslen.missingvalue && return missing
        pos = poslen.pos
        code, tlen, x, xT = detect(pass, val, pos, pos + poslen.len - 1, getoptions(r))
        return x === nothing ? r[i] : x
    end
end

function Parsers.parse(::Type{T}, r::Row2, nm::Symbol) where {T}
    @inbounds x = Parsers.parse(T, r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
    return x
end

function detect(r::Row2, nm::Symbol)
    @inbounds x = detect(r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
    return x
end

# We do our own ntask decrements in the iterate function
ChunkedCSV.task_done!(::CSVRowsContext, ::ChunkedCSV.ChunkingContext) = nothing
ChunkedCSV.consume!(ctx::CSVRowsContext, payload::CSVPayload) = put!(ctx.queue, payload)

"""
    CSV.Rows(source; kwargs...) => CSV.Rows

Read a csv input returning a `CSV.Rows` object.

The [`input`](@ref input) argument can be one of:
  * filename given as a string or FilePaths.jl type
  * a `Vector{UInt8}` or `SubArray{UInt8, 1, Vector{UInt8}}` byte buffer
  * a `CodeUnits` object, which wraps a `String`, like `codeunits(str)`
  * a csv-formatted string can also be passed like `IOBuffer(str)`
  * a `Cmd` or other `IO`
  * a gzipped file (or gzipped data in any of the above), which will automatically be decompressed for parsing


To read a csv file from a url, use the HTTP.jl package, where the `HTTP.Response` body can be passed like:
```julia
f = CSV.Rows(HTTP.get(url).body)
```

For other `IO` or `Cmd` inputs, you can pass them like: `f = CSV.Rows(read(obj))`.

While similar to [`CSV.File`](@ref), `CSV.Rows` provides a slightly different interface, the tradeoffs including:
  * Very minimal memory footprint; while iterating, only the current row values are buffered
  * Only provides row access via iteration; to access columns, one can stream the rows into a table type
  * Performs no type inference; each column/cell is essentially treated as `Union{String, Missing}`, users can utilize the performant `Parsers.parse(T, str)` to convert values to a more specific type if needed, or pass types upon construction using the `type` or `types` keyword arguments

Opens the file and uses passed arguments to detect the number of columns, ***but not*** column types (column types default to `String` unless otherwise manually provided).
The returned `CSV.Rows` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate rows. Each row object supports `propertynames`, `getproperty`, and `getindex` to access individual row values.
Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name `a` will become `a_1`).
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.Rows(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

$KEYWORD_DOCS
"""
function Rows(
    input;
    types=nothing,
    _force::Symbol=:default,
    reusebuffer::Bool=false,
    select=nothing,
    drop=nothing,
    transpose::Bool=false,
    pool::Bool=false,
    footerskip::Int=0,
    missingstring=missing,
    missingstrings=missing,
    kwargs...,
)
    transpose && throw(ArgumentError("transpose is not supported for CSV.Rows"))
    pool && throw(ArgumentError("pool is not supported for CSV.Rows"))
    footerskip > 0 && throw(ArgumentError("footerskip is not supported for CSV.Rows"))
    should_close, parsing_ctx, chunking_ctx, lexer = try
        ChunkedCSV.setup_parser(input, types; kwargs..., sentinel=_sentinel(missingstring, missingstrings))
    catch e
        if e isa ChunkedBase.FatalLexingError
            e = Error(e.msg)
        end
        rethrow(e)
    end
    _subset_columns!(parsing_ctx, select, drop)

    bufferschema = ChunkedCSV._translate_to_buffer_type.(parsing_ctx.schema, reusebuffer)
    unique_types = unique(bufferschema)
    V = length(unique_types) > 2 ? Any : Union{unique_types..., Missing}
    CT = ChunkedCSV._custom_types(parsing_ctx.schema)

    consume_ctx = CSVRowsContext(CSVPayloadOrderer())

    Base.errormonitor(Threads.@spawn begin
        try
            ChunkedCSV.parse_file(lexer, parsing_ctx, consume_ctx, chunking_ctx, _force)
        catch e
            if e isa ChunkedBase.FatalLexingError
                e = Error(e.msg)
            end
            rethrow(e)
        finally
            should_close && close(lexer.io)
            close(consume_ctx.queue)
        end
    end)

    return Rows{V, CT, reusebuffer ? PosLen31String : String}(
        getname(input),
        consume_ctx,
        bufferschema,
        parsing_ctx.header,
        Vector{V}(undef, length(parsing_ctx.schema)),
        parsing_ctx.options,
    )
end

@inline function setcustom!(::Type{customtypes}, values, cols, i, j, _type) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(eltype(cols))")
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    values[i] = (cols[i]::BufferedVector{$T})[j]::$T
                    return
                end
            end)
        end
        pushfirst!(block.args, :(type = _type))
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        @inbounds values[i] = cols[i][j]
        return
    end
end

@inline function _fill_row_buffer!(rows::Rows{V,CT}, buffer::Vector{V}, state::CSVRowsIteratorState) where {V,CT}
    row = state.row
    payload = state.payload
    indicators = payload.results.column_indicators
    N = length(payload.parsing_ctx.schema)
    cols = payload.results.cols
    @inbounds begin
        row_status = payload.results.row_statuses[row]
        has_missing = false
        row_ok = true
        if row_status != ChunkedCSV.RowStatus.Ok
            row_ok = false
            fill!(buffer, missing)
            has_missing = (row_status & ChunkedCSV.RowStatus.HasColumnIndicators) > 0
            state.indicators_idx += has_missing
        end
        for col in 1:N
            if has_missing && indicators[state.indicators_idx, col]
                buffer[col] = missing
                continue
            end
            dsttype = rows.bufferschema[col]
            if dsttype === Int
                buffer[col] = (cols[col]::BufferedVector{Int})[row]::Int
            elseif dsttype === Float64
                buffer[col] = (cols[col]::BufferedVector{Float64})[row]::Float64
            elseif dsttype === Bool
                buffer[col] = (cols[col]::BufferedVector{Bool})[row]::Bool
            elseif dsttype === DateTime
                buffer[col] = (cols[col]::BufferedVector{DateTime})[row]::DateTime
            elseif dsttype === Date
                buffer[col] = (cols[col]::BufferedVector{Date})[row]::Date
            elseif dsttype === Time
                buffer[col] = (cols[col]::BufferedVector{Time})[row]::Time
            elseif dsttype === Char
                buffer[col] = (cols[col]::BufferedVector{Char})[row]::Char
            elseif dsttype === Parsers.PosLen31
                pl = (cols[col]::BufferedVector{Parsers.PosLen31})[row]::Parsers.PosLen31
                buffer[col] = pl
            else
                setcustom!(CT, buffer, cols, col, row, dsttype)
            end
        end
    end
    return nothing
end

function _skip_missing_rows!(rows::Rows{V,CT,ST}, state) where {V,CT,ST}
    keep_skipping = true
    while true
        rs = state.payload.results.row_statuses
        @inbounds for i in state.row:length(rs)
            if (rs[i] & ChunkedCSV.RowStatus.SkippedRow) > 0
                state.row += 1
                state.indicators_idx += 1
            else
                keep_skipping = false
                break
            end
        end
        !keep_skipping && break
        ChunkedBase.dec!(state.payload.chunking_ctx.counter)
        state.payload = take!(rows.consume_ctx.queue)
        state.row = 1
        state.indicators_idx = 0
    end
    return nothing
end

function Base.iterate(rows::Rows{V,CT,ST}) where {V,CT,ST}
    try
        payload = take!(rows.consume_ctx.queue)
        state = CSVRowsIteratorState(payload, 1, 0)
        _skip_missing_rows!(rows, state)

        buffer = _get_buffer(rows)
        _fill_row_buffer!(rows, buffer, state)
        row = Row2{V,ST}(buffer, state)
        state.row += 1
        return row, state
    catch e
        if isa(e, Base.InvalidStateException) # Closing of the channel signal work is done
            return nothing
        else
            rethrow(e)
        end
    end
end

_get_buffer(rows::Rows{V,CT,PosLen31String}) where {V,CT} = rows.buffer
_get_buffer(rows::Rows{V,CT,String}) where {V,CT} = similar(rows.buffer)

function Base.iterate(rows::Rows{V,CT,ST}, state::CSVRowsIteratorState) where {V,CT,ST}
    payload = state.payload
    try
        if state.row == length(payload) + 1
            ChunkedBase.dec!(payload.chunking_ctx.counter)
            state.payload = take!(rows.consume_ctx.queue)
            state.row = 1
            state.indicators_idx = 0
        end
        _skip_missing_rows!(rows, state)
    catch e
        if isa(e, Base.InvalidStateException) # Closing of the channel signal work is done
            return nothing
        else
            rethrow(e)
        end
    end

    buffer = _get_buffer(rows)
    _fill_row_buffer!(rows, buffer, state)
    row = Row2{V,ST}(buffer, state)
    state.row += 1
    return row, state
end

Base.eltype(::Rows{V,CT,ST}) where {V,CT,ST} = Row2{V,ST}
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()
Base.IteratorEltype(::Rows) = Base.HasEltype()
Tables.isrowtable(::Type{<:Rows}) = true
_coltype(::Type{T}) where {T} = Union{T,Missing}
_coltype(::Type{Any}) = Any
Tables.schema(r::Rows) = Tables.Schema(r.names, map(_coltype, r.bufferschema))
