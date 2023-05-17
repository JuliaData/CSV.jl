using ChunkedCSV
using Parsers: PosLen31

struct CSVRowsParsedPayload
    row_num::Int
    results::ChunkedCSV.TaskResultBuffer
    parsing_ctx::ChunkedCSV.ParsingContext
end
Base.length(payload::CSVRowsParsedPayload) = length(payload.results.row_statuses)
last_row(payload::CSVRowsParsedPayload) = (payload.row_num + length(payload) - 1)

function reenqueue_ordered!(queue::Channel{T}, waiting_room::Vector{T}, payload::T) where {T}
    row = payload.row_num
    for _ in 1:length(waiting_room)
        nrows = length(payload)
        payload = first(waiting_room)
        if payload.row_num == (nrows + row)
            put!(queue, popfirst!(waiting_room))
        else
            break
        end
    end
end

function reorder!(queue::Channel{T}, waiting_room::Vector{T}, payload::T, expected_row::Int) where{T}
    row = payload.row_num
    # @info "id=$(payload.parsing_ctx.id), expected=$expected_row rownum=$(payload.row_num) nrows=$(length(payload)) len_waiting=$(length(waiting_room)) nums=$(join(map(x->x.row_num, waiting_room)[1:min(end,3)], ", "))"
    if row == expected_row
        reenqueue_ordered!(queue, waiting_room, payload)
        return false
    end

    ChunkedCSV.ConsumeContexts.insertsorted!(waiting_room, payload, x->x.row_num)

    if waiting_room[1].row_num == expected_row
        payload = popfirst!(waiting_room)
        put!(queue, payload)
        reenqueue_ordered!(queue, waiting_room, payload)
    end

    return true
end

# To overload consume! method in ChunkedCSV and to gather the results
struct CSVRowsContext <: ChunkedCSV.AbstractConsumeContext
    queue::Channel{CSVRowsParsedPayload}
end

# The `state` in `Base.iterate(iter::CSVRowsIterator, state)`
mutable struct CSVRowsIteratorState
    payload::CSVRowsParsedPayload
    row::Int
    indicators_idx::Int
end

# The return type of `Base.iterate(iter::CSVRowsIterator, state::CSVRowsIteratorState)`
struct CSVRow2{V,StringType} <: Tables.AbstractRow
    values::Vector{V}
    state::CSVRowsIteratorState
end

# What we return to the user when that user calls CSV.CSVRows
struct CSVRowsIterator{V,CustomTypes,StringType}
    consume_ctx::CSVRowsContext
    waiting_room::Vector{CSVRowsParsedPayload}
    bufferschema::Vector{DataType}
    names::Vector{Symbol}
    buffer::Vector{V}
    options::Parsers.Options # needed for detect/parse in CSVRow2
end

struct PosLen31String
    buf::Vector{UInt8}
    poslen::PosLen31
    e::UInt8
end

# TODO: something better
Base.show(io::IO, s::PosLen31String) = print(io, "pls$(repr(Parsers.getstring(s.buf, s.poslen, s.e)))31")

# The `AbstractRow` overides `getproperty` to allow for field access
getnames(r::CSVRow2) = getfield(r, :state).payload.parsing_ctx.header
getvalues(r::CSVRow2) = getfield(r, :values)
getbuf(r::CSVRow2) = getfield(r, :state).payload.parsing_ctx.bytes
getV(::CSVRow2{V}) where {V} = V
getescapechar(r::CSVRow2) = getfield(r, :state).payload.parsing_ctx.escapechar
getoptions(r::CSVRow2) = getfield(r, :state).payload.parsing_ctx.options

Base.checkbounds(r::CSVRow2, i) = 0 < i < length(r)

# Tables.jl interface
Tables.columnnames(r::CSVRow2) = getnames(r)

Tables.getcolumn(r::CSVRow2, nm::Symbol) = Tables.getcolumn(r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
Base.@propagate_inbounds function Tables.getcolumn(r::CSVRow2, i::Int)
    @boundscheck checkbounds(r, i)
    type = @inbounds typeof(getvalues(r)[i])
    return unsafe_getcolumn(r, type, i)
end
Base.@propagate_inbounds function Tables.getcolumn(r::CSVRow2{S}, ::Type{T}, i::Int, nm::Symbol) where {S,T <: S}
    @boundscheck checkbounds(r, i)
    return unsafe_getcolumn(r, T, i)
end

function unsafe_getcolumn(r::CSVRow2{Union{PosLen31,Missing},ST}, @nospecialize(type), i::Int) where {ST}
    @inbounds if type === PosLen31
        if ST == String
            return getvalues(r)[i]::ST
        else
            return PosLen31String(getbuf(r), getvalues(r)[i]::PosLen31, getescapechar(r))::ST
        end
    else
        return missing
    end
end
function unsafe_getcolumn(r::CSVRow2{Union{T,S,Missing}}, @nospecialize(type), i::Int) where {T,S}
    return @inbounds getvalues(r)[i]
end
function unsafe_getcolumn(r::CSVRow2{Union{PosLen31,S,Missing},ST}, @nospecialize(type), i::Int) where {S,ST}
    @inbounds if type === PosLen31
        if ST == String
            return getvalues(r)[i]::ST
        else
            return PosLen31String(getbuf(r), getvalues(r)[i]::PosLen31, getescapechar(r))::ST
        end
    elseif type === Missing
        return missing
    else
        return getvalues(r)[i]::S
    end
end

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::CSVRow2{V,String}, i::Int) where {T,V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa String || stringsonly()
        res = Parsers.xparse(T, val, 1, ncodeunits(val), getoptions(r))
    end
    return Parsers.ok(res.code) ? (res.val::T) : missing
end
Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::CSVRow2{V,PosLen31String}, i::Int) where {T,V}
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


Base.@propagate_inbounds function detect(r::CSVRow2{V,String}, i::Int) where {V}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        val = Tables.getcolumn(r, i)
        val isa String || stringsonly()
        code, tlen, x, xT = detect(pass, val, 1, ncodeunits(val), getoptions(r))
        return x === nothing ? r[i] : x
    end
end
Base.@propagate_inbounds function detect(r::CSVRow2{V,PosLen31String}, i::Int) where {V}
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

function Parsers.parse(::Type{T}, r::CSVRow2, nm::Symbol) where {T}
    @inbounds x = Parsers.parse(T, r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
    return x
end

function detect(r::CSVRow2, nm::Symbol)
    @inbounds x = detect(r, @something(findfirst(==(nm), getnames(r)), throw(KeyError(nm))))
    return x
end

# We do our own ntask decrements in the iterate function
ChunkedCSV.task_done!(::CSVRowsContext, ::ChunkedCSV.ParsingContext) = nothing

function ChunkedCSV.consume!(consume_ctx::CSVRowsContext, parsing_ctx::ChunkedCSV.ParsingContext, task_buf::ChunkedCSV.TaskResultBuffer, row_num::Int, eol_idx::Int32)
    put!(consume_ctx.queue, CSVRowsParsedPayload(row_num, task_buf, parsing_ctx))
end

function CSVRows(
    input;
    types=nothing,
    _force::Symbol=:default,
    reusebuffer::Bool=false,
    kwargs...,
)
    should_close, parsing_ctx, lexer, options = ChunkedCSV.setup_parser(input, types; kwargs...)

    bufferschema = ChunkedCSV._translate_to_buffer_type.(parsing_ctx.schema, reusebuffer)
    unique_types = unique(bufferschema)
    V = length(unique_types) > 2 ? Any : Union{unique_types..., Missing}
    CT = ChunkedCSV._custom_types(parsing_ctx.schema)

    consume_ctx = CSVRowsContext(Channel{CSVRowsParsedPayload}(Inf))

    Base.errormonitor(Threads.@spawn begin
        ChunkedCSV.parse_file(lexer, parsing_ctx, consume_ctx, options, _force)
        should_close && close(lexer.io)
        close(consume_ctx.queue)
    end)

    return CSVRowsIterator{V, CT, reusebuffer ? PosLen31String : String}(
        consume_ctx,
        sizehint!(CSVRowsParsedPayload[], Threads.nthreads()),
        bufferschema,
        parsing_ctx.header,
        Vector{V}(undef, length(parsing_ctx.schema)),
        options,
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
                    values[i] = (cols[i]::ChunkedCSV.BufferedVector{$T})[j]::$T
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

@inline function _fill_row_buffer!(rows::CSVRowsIterator{V,CT}, buffer::Vector{V}, state::CSVRowsIteratorState) where {V,CT}
    row = state.row
    payload = state.payload
    bytes = payload.parsing_ctx.bytes
    indicators = payload.results.column_indicators
    e = payload.parsing_ctx.escapechar
    N = length(payload.parsing_ctx.schema)
    @inbounds begin
        row_status = payload.results.row_statuses[row]
        has_missing = false
        row_ok = true
        if row_status != ChunkedCSV.RowStatus.Ok
            row_ok = false
            fill!(buffer, missing)
            has_missing = (row_status | ChunkedCSV.RowStatus.HasColumnIndicators) > 0
            has_missing && (state.indicators_idx += 1)
        end
        for col in 1:N
            has_missing && indicators[state.indicators_idx, col] && continue
            dsttype = rows.bufferschema[col]
            if dsttype === Int
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Int})[row]::Int
            elseif dsttype === Float64
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Float64})[row]::Float64
            elseif dsttype === Bool
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Bool})[row]::Bool
            elseif dsttype === DateTime
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{DateTime})[row]::DateTime
            elseif dsttype === Date
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Date})[row]::Date
            elseif dsttype === Time
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Time})[row]::Time
            elseif dsttype === Char
                buffer[col] = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Char})[row]::Char
            elseif dsttype === Parsers.PosLen31
                pl = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Parsers.PosLen31})[row]::Parsers.PosLen31
                buffer[col] = pl
            elseif dsttype === String
                pl = (payload.results.cols[col]::ChunkedCSV.BufferedVector{Parsers.PosLen31})[row]::Parsers.PosLen31
                buffer[col] = Parsers.getstring(bytes, pl, e)::String
            else
                setcustom!(CT, buffer, payload.results.cols, col, row, dsttype)
            end
        end
    end
    return nothing
end

function Base.iterate(rows::CSVRowsIterator{V,CT,ST}) where {V,CT,ST}
    local state, payload
    try
        payload = take!(rows.consume_ctx.queue)
        state = CSVRowsIteratorState(payload, 1, 1)
        while reorder!(rows.consume_ctx.queue, rows.waiting_room, state.payload, state.row)
            state.payload = take!(rows.consume_ctx.queue)
        end
    catch e
        if isa(e, Base.InvalidStateException) # Closing of the channel signal work is done
            return nothing
        else
            rethrow(e)
        end
    end

    buffer = _get_buffer(rows)
    _fill_row_buffer!(rows, buffer, state)
    row = CSVRow2{V,ST}(buffer, state)
    return row, state
    # return buffer, state
end

_get_buffer(rows::CSVRowsIterator{V,CT,PosLen31String}) where {V,CT} = rows.buffer
_get_buffer(rows::CSVRowsIterator{V,CT,String}) where {V,CT} = similar(rows.buffer)

function Base.iterate(rows::CSVRowsIterator{V,CT,ST}, state::CSVRowsIteratorState) where {V,CT,ST}
    payload = state.payload
    if state.row == length(payload)
        ChunkedCSV.dec!(payload.parsing_ctx.counter)

        expected_next_row = last_row(payload) + 1

        local payload
        try
            payload = take!(rows.consume_ctx.queue)
            while reorder!(rows.consume_ctx.queue, rows.waiting_room, payload, expected_next_row)
                payload = take!(rows.consume_ctx.queue)
            end
        catch e
            if isa(e, Base.InvalidStateException) # Closing of the channel signal work is done
                return nothing
            else
                rethrow(e)
            end
        end
        state.row = 1
    else
        state.row += 1
    end
    state.payload = payload
    buffer = _get_buffer(rows)
    _fill_row_buffer!(rows, buffer, state)
    row = CSVRow2{V,ST}(buffer, state)
    return row, state
    # return buffer, state
end

Base.eltype(::CSVRowsIterator{V,CT,ST}) where {V,CT,ST} = CSVRow2{V,ST}
Base.IteratorSize(::Type{<:CSVRowsIterator}) = Base.SizeUnknown()
Base.IteratorEltype(::CSVRowsIterator) = Base.HasEltype()
Tables.isrowtable(::Type{<:CSVRowsIterator}) = true
coltype2(::Type{T}) where {T} = Union{T,Missing}
coltype2(::Type{Any}) = Any
Tables.schema(r::CSVRowsIterator) = Tables.Schema(r.names, map(coltype2, r.bufferschema))

mutable struct TempFileWrapper
    file::Union{String, Nothing}
end

# structure for iterating over a csv file
# no automatic type inference is done, but types are allowed to be passed
# for as many columns as desired; `CSV.detect(row, i)` can also be used to
# use the same inference logic used in `CSV.File` for determining a cell's typed value
struct Rows{IO, customtypes, V, stringtype}
    name::String
    names::Vector{Symbol} # only includes "select"ed columns
    columns::Vector{Column}
    columnmap::Vector{Int} # maps "select"ed column index to actual file column index
    buf::IO
    datapos::Int
    datarow::Int
    len::Int
    limit::Int
    options::Parsers.Options
    reusebuffer::Bool
    values::Vector{V} # once values are parsed, put in values; allocated on each iteration if reusebuffer=false
    lookup::Dict{Symbol, Int}
    numwarnings::Base.RefValue{Int}
    maxwarnings::Int
    ctx::Context
    tempfile::TempFileWrapper
end

function Base.show(io::IO, r::Rows)
    println(io, "CSV.Rows(\"$(r.name)\"):")
    println(io, "Size: $(length(r.columns))")
    show(io, Tables.schema(r))
end

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
function Rows(source::ValidSources;
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Integer=-1,
    skipto::Integer=-1,
    footerskip::Integer=0,
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    ignoreemptyrows::Bool=true,
    ignoreemptylines=nothing,
    select=nothing,
    drop=nothing,
    limit::Union{Integer, Nothing}=nothing,
    buffer_in_memory::Bool=false,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Nothing, Char, String}=nothing,
    ignorerepeated::Bool=false,
    quoted::Bool=true,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='"',
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
    dateformats=nothing,
    decimal::Union{UInt8, Char}=UInt8('.'),
    truestrings::Union{Vector{String}, Nothing}=TRUE_STRINGS,
    falsestrings::Union{Vector{String}, Nothing}=FALSE_STRINGS,
    stripwhitespace::Bool=false,
    # type options
    type=nothing,
    types=nothing,
    typemap::AbstractDict=IdDict{Type, Type}(),
    pool=false,
    downcast::Bool=false,
    stringtype::StringTypes=PosLenString,
    lazystrings::Bool=stringtype === PosLenString,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=100,
    debug::Bool=false,
    parsingdebug::Bool=false,
    validate::Bool=true,
    reusebuffer::Bool=false,
    )
    ctx = @refargs Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptyrows, ignoreemptylines, select, drop, limit, buffer_in_memory, nothing, nothing, nothing, 0, nothing, missingstrings, missingstring, delim, ignorerepeated, quoted, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, stripwhitespace, type, types, typemap, pool, downcast, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, validate, true)
    foreach(col -> col.pool = 0.0, ctx.columns)
    allocate!(ctx.columns, 1)
    values = all(x->x.type === ctx.stringtype && x.anymissing, ctx.columns) && ctx.stringtype === PosLenString ? Vector{PosLen}(undef, ctx.cols) : Vector{Any}(undef, ctx.cols)
    columnmap = collect(1:ctx.cols)
    for i = ctx.cols:-1:1
        col = ctx.columns[i]
        if col.willdrop
            deleteat!(ctx.names, i)
            deleteat!(columnmap, i)
        end
    end
    lookup = Dict(nm=>i for (i, nm) in enumerate(ctx.names))
    tempfile = TempFileWrapper(ctx.tempfile)
    if tempfile.file !== nothing
        finalizer(tempfile) do x
            rm(x.file; force=true)
        end
    end
    return Rows{typeof(ctx.buf), ctx.customtypes, eltype(values), ctx.stringtype}(
        ctx.name,
        ctx.names,
        ctx.columns,
        columnmap,
        ctx.buf,
        ctx.datapos,
        ctx.datarow,
        ctx.len,
        ctx.limit,
        ctx.options,
        reusebuffer,
        values,
        lookup,
        Ref(0),
        ctx.maxwarnings,
        ctx,
        tempfile
    )
end

Tables.isrowtable(::Type{<:Rows}) = true
Tables.schema(r::Rows) = Tables.Schema(r.names, [coltype(x) for x in view(r.columns, r.columnmap)])
Base.eltype(::Rows) = Row2
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()

@inline function setcustom!(::Type{customtypes}, values, columns, i, setmissing) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(typeof(column))")
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            vT = vectype(T)
            pushfirst!(block.args, quote
                column = columns[i].column
                if column isa $vT
                    if setmissing
                        setmissing!(column, 1)
                    else
                        @inbounds values[i] = column[1]
                    end
                    return
                end
            end)
        end
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        @inbounds values[i] = columns[i].column[1]
        return
    end
end

function checkwidencolumns!(r::Rows{ct, V}, cols) where {ct, V}
    if cols > length(r.values)
        # we widened while parsing this row, need to widen other supporting objects
        for i = (length(r.values) + 1):cols
            push!(r.values, V === Any ? missing : Base.bitcast(PosLen, Parsers.MISSING_BIT))
            nm = Symbol(:Column, i)
            push!(r.names, nm)
            r.lookup[nm] = length(r.values)
            push!(r.columnmap, i)
        end
    end
    return
end

macro unrollcolumns(setmissing, values, ex)
    return esc(quote
        if column isa MissingVector
            if !($setmissing)
                @inbounds $(values)[i] = missing
            end
        elseif column isa Vector{PosLen}
            $ex
        elseif column isa Vector{Union{Missing, Int8}}
            $ex
        elseif column isa Vector{Union{Missing, Int16}}
            $ex
        elseif column isa Vector{Union{Missing, Int32}}
            $ex
        elseif column isa SVec{Int32}
            $ex
        elseif column isa SVec{Int64}
            $ex
        elseif column isa SVec{Int128}
            $ex
        elseif column isa SVec{Float16}
            $ex
        elseif column isa SVec{Float32}
            $ex
        elseif column isa SVec{Float64}
            $ex
        elseif column isa SVec{InlineString1}
            $ex
        elseif column isa SVec{InlineString3}
            $ex
        elseif column isa SVec{InlineString7}
            $ex
        elseif column isa SVec{InlineString15}
            $ex
        elseif column isa SVec{InlineString31}
            $ex
        elseif column isa SVec{InlineString63}
            $ex
        elseif column isa SVec{InlineString127}
            $ex
        elseif column isa SVec{InlineString255}
            $ex
        elseif column isa SVec2{String}
            $ex
        elseif column isa SVec{Date}
            $ex
        elseif column isa SVec{DateTime}
            $ex
        elseif column isa SVec{Time}
            $ex
        elseif column isa Vector{Union{Missing, Bool}}
            $ex
        elseif customtypes !== Tuple{}
            setcustom!(customtypes, $values, columns, i, $setmissing)
        else
            error("bad array type: $(typeof(column))")
        end
    end)
end

@inline function Base.iterate(r::Rows{IO, customtypes, V, stringtype}, (pos, len, row)=(r.datapos, r.len, 1)) where {IO, customtypes, V, stringtype}
    (pos > len || row > r.limit) && return nothing
    columns = r.columns
    cols = length(columns)
    for i = 1:cols
        @inbounds column = columns[i].column
        @unrollcolumns true nothing begin
            setmissing!(column, 1)
        end
    end
    pos = parserow(1, 1, r.numwarnings, r.ctx, r.buf, pos, len, 1, r.datarow + row - 2, columns, customtypes)
    cols = length(columns)
    values = r.reusebuffer ? r.values : Vector{V}(undef, cols)
    checkwidencolumns!(r, cols)
    for i = 1:cols
        @inbounds column = columns[i].column
        @unrollcolumns false values begin
            @inbounds values[i] = column[1]
        end
    end
    return Row2{V, stringtype}(r.names, r.columns, r.columnmap, r.lookup, values, r.buf), (pos, len, row + 1)
end

struct Row2{V, stringtype} <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{Column}
    columnmap::Vector{Int}
    lookup::Dict{Symbol, Int}
    values::Vector{V}
    buf::Vector{UInt8}
end

getnames(r::Row2) = getfield(r, :names)
getcolumns(r::Row2) = getfield(r, :columns)
getcolumnmap(r::Row2) = getfield(r, :columnmap)
getlookup(r::Row2) = getfield(r, :lookup)
getvalues(r::Row2) = getfield(r, :values)
getbuf(r::Row2) = getfield(r, :buf)
getV(::Row2{V}) where {V} = V
getstringtype(::Row2{V, stringtype}) where {V, stringtype} = stringtype

Tables.columnnames(r::Row2) = getnames(r)

Base.checkbounds(r::Row2, i) = 0 < i < length(r)

Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, getlookup(r)[nm])
Tables.getcolumn(r::Row2, i::Int) = Tables.getcolumn(r, coltype(getcolumns(r)[i]), i, getnames(r)[i])

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{T}, i::Int, nm::Symbol) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    values = getvalues(r)
    V = getV(r)
    @inbounds val = j > length(values) ? (V === PosLen ? Parsers.MISSING_BIT : missing) : values[j]
    stringtype = getstringtype(r)
    if V === PosLen
        # column type must be stringtype
        # @show T, stringtype
        @assert T === Union{stringtype, Missing}
        e = getcolumns(r)[j].options.e
        if (val isa PosLen && val.missingvalue) || val == Parsers.MISSING_BIT
            return missing
        elseif stringtype === PosLenString
            return PosLenString(getbuf(r), val, e)
        elseif stringtype === String
            return Parsers.getstring(getbuf(r), val, e)
        end
    else
        # at least some column types were manually provided
        if val isa PosLen
            if val.missingvalue
                return missing
            else
                e = getcolumns(r)[j].options.e
                return PosLenString(getbuf(r), val, e)
            end
        else
            return val
        end
    end
end

@noinline stringsonly() = error("Parsers.parse only allowed on String column types")

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    @inbounds begin
        j = getcolumnmap(r)[i]
        col = getcolumns(r)[j]
        col.type isa StringTypes || stringsonly()
        poslen = getvalues(r)[j]
        poslen.missingvalue && return missing
        pos = poslen.pos
        res = Parsers.xparse(T, getbuf(r), pos, pos + poslen.len, col.options)
    end
    return Parsers.ok(res.code) ? (res.val::T) : missing
end

Base.@propagate_inbounds function detect(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    @inbounds begin
        j = getcolumnmap(r)[i]
        col = getcolumns(r)[j]
        col.type isa StringTypes || stringsonly()
        poslen = getvalues(r)[j]
        poslen.missingvalue && return missing
        pos = poslen.pos
        code, tlen, x, xT = detect(pass, getbuf(r), pos, pos + poslen.len - 1, col.options)
        return x === nothing ? r[i] : x
    end
end

function Parsers.parse(::Type{T}, r::Row2, nm::Symbol) where {T}
    @inbounds x = Parsers.parse(T, r, getlookup(r)[nm])
    return x
end

function detect(r::Row2, nm::Symbol)
    @inbounds x = detect(r, getlookup(r)[nm])
    return x
end
