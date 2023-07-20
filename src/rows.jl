using ChunkedBase
using ChunkedCSV
using SentinelArrays
using Parsers: PosLen31
using Tables: Schema

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
    names_to_pos::Dict{Symbol,Int}
    values::Vector{V}
    state::CSVRowsIteratorState
    _bytes_age::Tuple{Int,Int}
end

function isvalid(r::Row2)
    chunking_ctx = getfield(r, :state).payload.chunking_ctx
    age = getfield(r, :_bytes_age)
    return age == (chunking_ctx.id, chunking_ctx.buffer_refills[])
end
# What we return to the user when that user calls CSV.CSVRows
struct Rows{V,CustomTypes,StringType}
    name::String
    consume_ctx::CSVRowsContext
    bufferschema::Vector{DataType}
    names::Vector{Symbol}
    names_to_pos::Dict{Symbol,Int}
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
getlookup(r::Row2) = getfield(r, :names_to_pos)
getnames(r::Row2) = getfield(r, :state).payload.parsing_ctx.header
getschema(r::Row2) = getfield(r, :state).payload.parsing_ctx.schema
getvalues(r::Row2) = getfield(r, :values)
getbuf(r::Row2) = getfield(r, :state).payload.chunking_ctx.bytes
getescapechar(r::Row2) = getfield(r, :state).payload.parsing_ctx.escapechar
getoptions(r::Row2) = getfield(r, :state).payload.parsing_ctx.options

Base.checkbounds(r::Row2, i) = 0 < i <= length(r)

# Tables.jl interface
Tables.columnnames(r::Row2) = getnames(r)
Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, getlookup(r)[nm])
Base.@propagate_inbounds function Tables.getcolumn(r::Row2{S,ST}, ::Type{T}, i::Int, nm::Symbol) where {S,T,ST}
    @boundscheck checkbounds(r, i)
    val = @inbounds getvalues(r)[i]
    if val isa PosLen31
        pl = getvalues(r)[i]::PosLen31
        buf = getbuf(r)
        e = getescapechar(r)
        if ST === String
            return Parsers.getstring(buf, pl::PosLen31, e)::ST
        else
            return PosLen31String(buf, pl::PosLen31, e)::ST
        end
    else
        return val
    end
end
Base.@propagate_inbounds function Tables.getcolumn(r::Row2{T,ST}, i::Int) where {T,ST}
    @boundscheck checkbounds(r, i)
    @inbounds val = getvalues(r)[i]
    ismissing(val) && return missing
    @inbounds if val isa PosLen31
        pl = val::PosLen31
        buf = getbuf(r)
        e = getescapechar(r)
        if ST === String
            return Parsers.getstring(buf, pl::PosLen31, e)::ST
        else
            return PosLen31String(buf, pl::PosLen31, e)::ST
        end
    else
        return val
    end
end

stringsonly() = throw(ArgumentError("Only string columns can be parsed"))

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = getvalues(r)[i]
        val.missingvalue && return missing
        val isa Parsers.PosLen31 || stringsonly()
        res = Parsers.xparse(T, getbuf(r), val.pos, val.pos + val.len - 1, getoptions(r))
    end
    return Parsers.ok(res.code) ? (res.val::T) : missing
end

Base.@propagate_inbounds function detect(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = getvalues(r)[i]
        val isa Parsers.PosLen31 || stringsonly()
        val.missingvalue && return missing
        code, tlen, x, xT = detect(pass, getbuf(r), val.pos, val.pos + val.len - 1, getoptions(r))
        return x === nothing ? r[i] : x
    end
end

Parsers.parse(::Type{T}, r::Row2, nm::Symbol) where {T} = @inbounds Parsers.parse(T, r, getlookup(r)[nm])
detect(r::Row2, nm::Symbol) = @inbounds detect(r, getlookup(r)[nm])

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
    delim=nothing,
    newlinechar=nothing,
    select=nothing,
    drop=nothing,
    transpose::Bool=false,
    pool::Union{Bool,AbstractFloat,AbstractDict}=false,
    footerskip::Int=0,
    missingstring=missing,
    missingstrings=missing,
    debug=false, # ignored
    normalizenames::Bool=false,
    default_colname_prefix="Column",
    stringtype=nothing,
    kwargs...,
)
    transpose && throw(ArgumentError("`transpose` argument is not supported for CSV.Rows"))
    !(pool isa Bool && !pool) && (@warn "`pool` argument is not supported for CSV.Rows")
    footerskip > 0 && throw(ArgumentError("`footerskip` argument is not supported for CSV.Rows"))
    stringtype !== nothing && (@warn "`stringtype` argument is not supported for CSV.Rows")
    sentinel = _sentinel(missingstring, missingstrings)
    should_close, parsing_ctx, chunking_ctx, lexer = try
        input = input isa IO ? input : string(input)
        ChunkedCSV.setup_parser(
            input, types;
            kwargs..., delim, newlinechar, sentinel, default_colname_prefix, deduplicate_names=true
        )
    catch e
        if e isa ChunkedBase.FatalLexingError
            e = Error(e.msg)
        end
        rethrow(e)
    end
    normalizenames && (parsing_ctx.header .= normalizename.(string.(parsing_ctx.header)))
    ChunkedCSV._subset_columns!(parsing_ctx, select, drop)

    bufferschema = [
        #=type === Parsers.PosLen31 && !reusebuffer ? String :=#
        ChunkedCSV._translate_to_buffer_type(type)
        for type
        in parsing_ctx.schema
    ]
    unique_types = unique(bufferschema)
    V = length(unique_types) > 2 ? Any : Union{unique_types..., Missing}
    CT = ChunkedCSV._custom_types(parsing_ctx.schema)

    consume_ctx = CSVRowsContext(CSVPayloadOrderer())

    Base.errormonitor(Threads.@spawn begin
        _lexer = $lexer
        _consume_ctx = $consume_ctx
        try
            ChunkedCSV.parse_file(_lexer, $parsing_ctx, _consume_ctx, $chunking_ctx, $_force)
        catch e
            if e isa ChunkedBase.FatalLexingError
                e = Error(e.msg)
            end
            rethrow(e)
        finally
            $should_close && close(_lexer.io)
            close(_consume_ctx.queue)
        end
    end)

    return Rows{V, CT, reusebuffer ? PosLen31String : String}(
        getname(input),
        consume_ctx,
        bufferschema,
        parsing_ctx.header,
        Dict{Symbol,Int}(k => i for (i, k) in enumerate(parsing_ctx.header)),
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
            # elseif dsttype === String
            #     pl = (cols[col]::BufferedVector{Parsers.PosLen31})[row]::Parsers.PosLen31
            #     buffer[col] = Parsers.getstring(payload.chunking_ctx.bytes, pl, payload.parsing_ctx.escapechar)
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
        row = Row2{V,ST}(rows.names_to_pos, buffer, state, (payload.chunking_ctx.id, payload.chunking_ctx.buffer_refills[]))
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
    row = Row2{V,ST}(rows.names_to_pos, buffer, state, (payload.chunking_ctx.id, payload.chunking_ctx.buffer_refills[]))
    state.row += 1
    return row, state
end

Base.eltype(::Rows{V,CT,ST}) where {V,CT,ST} = Row2{V,ST}
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()
Base.IteratorEltype(::Rows) = Base.HasEltype()
Tables.isrowtable(::Type{<:Rows}) = true
_coltype(::Type{T}, ::Type{ST}) where {T,ST} = Union{T,Missing}
_coltype(::Type{Parsers.PosLen31}, ::Type{ST}) where {ST} = Union{ST,Missing}
_coltype(::Type{Any}, ::Type{ST}) where {ST} = Any
Tables.schema(r::Rows{T,CT,ST}) where {T,CT,ST} = Tables.Schema(r.names, map(type->_coltype(type, ST), r.bufferschema))


function foo()
    out = 0
    itr = CSV.Rows("../../../_proj/datasets/tst_4xint_big.csv", types=[Int,Int,Int,Int], reusebuffer=true)
    for row in itr
        out += 1
    end
    return out
end


function baz(iter)
    out = 0
    next = iterate(iter)
    while next !== nothing
        (item, state) = next
        out += 1
        next = iterate(iter, state)
    end
    return out
end

function bar0()
    iter = CSV.Rows("../../../_proj/datasets/tst_4xint_big.csv", types=[Int,Int,Int,Int], reusebuffer=true, no_quoted_newlines=true)
    return baz(iter)
end


function bar1()
    iter = CSV.Rows("../../../_proj/datasets/tst_4xint_big.csv", types=[Int,Int,Int,Int], reusebuffer=true)
    return baz(iter)
end

function bar2()
    iter = CSV.Rows("../../../_proj/datasets/tst_4xint_big.csv", types=[Int,Int,Int,Int], reusebuffer=true)
    out = 0
    next = iterate(iter)
    while next !== nothing
        (item, state) = next
        # out += item.a
        next = iterate(iter, state)
    end
    return out
end


function bar3()
    iter = CSV.Rows{Union{Missing,Int}}("../../../_proj/datasets/tst_4xint_big.csv", types=[Int,Int,Int,Int], reusebuffer=true)
    out = 0
    next = iterate(iter)
    while next !== nothing
        (item, state) = next
        out += item.a
        next = iterate(iter, state)
    end
    return out
end
