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
    groupmark::Union{Char, Nothing}=nothing,
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
    ctx = @refargs Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptyrows, ignoreemptylines, select, drop, limit, buffer_in_memory, nothing, nothing, nothing, 0, nothing, missingstrings, missingstring, delim, ignorerepeated, quoted, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, groupmark, truestrings, falsestrings, stripwhitespace, type, types, typemap, pool, downcast, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, validate, true)
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
