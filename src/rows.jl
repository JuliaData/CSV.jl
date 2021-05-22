# structure for iterating over a csv file
# no automatic type inference is done, but types are allowed to be passed
# for as many columns as desired; `CSV.detect(row, i)` can also be used to
# use the same inference logic used in `CSV.File` for determing a cell's typed value
struct Rows{transpose, O, O2, IO, customtypes, V, stringtype}
    name::String
    names::Vector{Symbol} # only includes "select"ed columns
    columns::Vector{Column}
    columnmap::Vector{Int} # maps "select"ed column index to actual file column index
    buf::IO
    datapos::Int64
    datarow::Int
    len::Int
    limit::Int64
    options::O # Parsers.Options
    coloptions::O2 # Union{Nothing, Vector{Parsers.Options}}
    reusebuffer::Bool
    values::Vector{V} # once values are parsed, put in values; allocated on each iteration if reusebuffer=false
    lookup::Dict{Symbol, Int}
    numwarnings::Base.RefValue{Int}
    maxwarnings::Int
    ctx::Context
end

function Base.show(io::IO, r::Rows)
    println(io, "CSV.Rows(\"$(r.name)\"):")
    println(io, "Size: $(length(r.columns))")
    show(io, Tables.schema(r))
end

"""
    CSV.Rows(source; kwargs...) => CSV.Rows

Read a csv input returning a `CSV.Rows` object.

The `source` argument can be one of:
  * filename given as a string or FilePaths.jl type
  * an `AbstractVector{UInt8}` like a byte buffer or `codeunits(string)`
  * an `IOBuffer`

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

Supported keyword arguments include:

### File layout options:

   - `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.). Note that if a row number header and `comment` or `ignoreemtpylines` are provided, the header row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header row will actually be the next non-commented row.
   - `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
   - `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data; providing a `datarow` or `skipto` argument does _not_ affect the `header` argument. Note that if a row number `datarow` and `comment` or `ignoreemtpylines` are provided, the data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the data row will actually be the next non-commented row.
   - `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
   - `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
   - `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
   - `comment`: rows that begin with this `String` will be skipped while parsing. Note that if a row number header or `datarow` and `comment` are provided, the header/data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header/data row will actually be the next non-commented row.
   - `ignoreemptylines::Bool=true`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)

### Parsing options:

   - `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
   - `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
   - `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
   - `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
   - `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
   - `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted for the entire file
   - `decimal='.'`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
   - `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented; by default only `true` and `false` are treated as `Bool`

### Column Type Options:

   - `type`: a single type to use for parsing an entire file; i.e. all columns will be treated as the same type; useful for matrix-like data files
   - `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64; if a `Vector` if provided, it must match the # of columns provided or detected in `header`
   - `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `String`
   - `lazystrings::Bool=true`: avoid allocating full strings while parsing; accessing a string column will materialize the full `String`
   - `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
   - `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
   - `maxwarnings::Int=100`: if more than `maxwarnings` number of warnings are printed while parsing, further warnings will be silenced by default

### Iteration options:

   - `reusebuffer=false`: while iterating, whether a single row buffer should be allocated and reused on each iteration; only use if each row will be iterated once and not re-used (e.g. it's not safe to use this option if doing `collect(CSV.Rows(file))` because only current iterated row is "valid")
"""
function Rows(source;
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Integer=-1,
    skipto::Union{Nothing, Integer}=nothing,
    footerskip::Integer=0,
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    ignoreemptylines::Bool=true,
    select=nothing,
    drop=nothing,
    limit::Union{Integer, Nothing}=nothing,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Nothing, Char, String}=nothing,
    ignorerepeated::Bool=false,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='"',
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    dateformats::Union{AbstractDict, Nothing}=nothing,
    decimal::Union{UInt8, Char}=UInt8('.'),
    truestrings::Union{Vector{String}, Nothing}=["true", "True", "TRUE"],
    falsestrings::Union{Vector{String}, Nothing}=["false", "False", "FALSE"],
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    pool::Union{Bool, Real, AbstractVector, AbstractDict}=NaN,
    stringtype::StringTypes=PosLenString,
    lazystrings::Bool=stringtype === PosLenString,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=100,
    debug::Bool=false,
    parsingdebug::Bool=false,
    reusebuffer::Bool=false,
    )
    ctx = Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptylines, select, drop, limit, false, 1, 0, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, pool, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, true)
    allocate!(ctx.columns, 1)
    values = all(x->x.type === stringtype && x.anymissing, ctx.columns) && lazystrings ? Vector{PosLen}(undef, ctx.cols) : Vector{Any}(undef, ctx.cols)
    columnmap = collect(1:ctx.cols)
    for i = ctx.cols:-1:1
        col = ctx.columns[i]
        if col.willdrop
            deleteat!(ctx.names, i)
            deleteat!(columnmap, i)
        end
    end
    lookup = Dict(nm=>i for (i, nm) in enumerate(ctx.names))
    return Rows{transpose, typeof(ctx.options), typeof(ctx.coloptions), typeof(ctx.buf), ctx.customtypes, eltype(values), stringtype}(
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
        ctx.coloptions,
        reusebuffer,
        values,
        lookup,
        Ref(0),
        maxwarnings,
        ctx
    )
end

Tables.rowtable(::Type{<:Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = Tables.Schema(r.names, [coltype(x) for x in view(r.columns, r.columnmap)])
Base.eltype(::Rows) = Row2
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()

@inline function setcustom!(::Type{customtypes}, values, columns, i) where {customtypes}
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
                    @inbounds values[i] = column[1]
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

function checkwidencolumns!(r::Rows{t, o, o2, ct, V}, cols) where {t, o, o2, ct, V}
    if cols > length(r.names)
        # we widened while parsing this row, need to widen other supporting objects
        for i = (length(r.names) + 1):cols
            push!(r.values, V === Any ? missing : WeakRefStrings.MISSING_BIT)
            nm = Symbol(:Column, i)
            push!(r.names, nm)
            r.lookup[nm] = length(r.names)
            push!(r.columnmap, i)
        end
    end
    return
end

@inline function Base.iterate(r::Rows{transpose, O, O2, IO, customtypes, V, stringtype}, (pos, len, row)=(r.datapos, r.len, 1)) where {transpose, O, O2, IO, customtypes, V, stringtype}
    (pos > len || row > r.limit) && return nothing
    pos = parserow(1, 1, r.numwarnings, r.ctx, r.buf, pos, len, 1, r.datarow + row - 2, r.columns, Val(transpose), r.options, r.coloptions, customtypes)
    columns = r.columns
    cols = length(columns)
    checkwidencolumns!(r, cols)
    values = r.reusebuffer ? r.values : Vector{V}(undef, cols)
    for i = 1:cols
        @inbounds column = columns[i].column
        if column isa MissingVector
            @inbounds values[i] = missing
        elseif column isa Vector{PosLen}
            @inbounds values[i] = column[1]
        elseif column isa SVec{Int64}
            @inbounds values[i] = column[1]
        elseif column isa SVec{Float64}
            @inbounds values[i] = column[1]
        elseif column isa SVec2{String}
            @inbounds values[i] = column[1]
        elseif column isa SVec{Date}
            @inbounds values[i] = column[1]
        elseif column isa SVec{DateTime}
            @inbounds values[i] = column[1]
        elseif column isa SVec{Time}
            @inbounds values[i] = column[1]
        elseif column isa Vector{Union{Missing, Bool}}
            @inbounds values[i] = column[1]
        elseif column isa Vector{UInt32}
            @inbounds values[i] = column[1]
        elseif customtypes !== Tuple{}
            setcustom!(customtypes, values, columns, i)
        else
            error("bad array type: $(typeof(column))")
        end
    end
    return Row2{O, O2, V, stringtype}(r.names, r.columns, r.columnmap, r.lookup, values, r.buf, r.options, r.coloptions), (pos, len, row + 1)
end

struct Row2{O, O2, V, stringtype} <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{Column}
    columnmap::Vector{Int}
    lookup::Dict{Symbol, Int}
    values::Vector{V}
    buf::Vector{UInt8}
    options::O
    coloptions::O2
end

getnames(r::Row2) = getfield(r, :names)
getcolumns(r::Row2) = getfield(r, :columns)
getcolumnmap(r::Row2) = getfield(r, :columnmap)
getlookup(r::Row2) = getfield(r, :lookup)
getvalues(r::Row2) = getfield(r, :values)
getbuf(r::Row2) = getfield(r, :buf)
getoptions(r::Row2) = getfield(r, :options)
getcoloptions(r::Row2) = getfield(r, :coloptions)
getV(::Row2{O, O2, V}) where {O, O2, V} = V
getstringtype(::Row2{O, O2, V, stringtype}) where {O, O2, V, stringtype} = stringtype

Tables.columnnames(r::Row2) = getnames(r)

Base.checkbounds(r::Row2, i) = 0 < i < length(r)

Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, getlookup(r)[nm])
Tables.getcolumn(r::Row2, i::Int) = Tables.getcolumn(r, coltype(getcolumns(r)[i]), i, getnames(r)[i])

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{T}, i::Int, nm::Symbol) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    values = getvalues(r)
    V = getV(r)
    @inbounds val = j > length(values) ? (V === PosLen ? WeakRefStrings.MISSING_BIT : missing) : values[j]
    stringtype = getstringtype(r)
    if V === PosLen
        # column type must be stringtype
        # @show T, stringtype
        @assert T === Union{stringtype, Missing}
        if WeakRefStrings.missingvalue(val)
            return missing
        elseif stringtype === PosLenString
            return PosLenString(getbuf(r), val, getoptions(r).e)
        elseif stringtype === String
            return String(getbuf(r), val, getoptions(r).e)
        end
    else
        # at least some column types were manually provided
        if val isa PosLen
            if WeakRefStrings.missingvalue(val)
                return missing
            else
                return PosLenString(getbuf(r), val, getoptions(r).e)
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
        col = getcolumns(r)[i]
        col.type isa StringTypes || stringsonly()
        poslen = getvalues(r)[j]
        WeakRefStrings.missingvalue(poslen) && return missing
        pos = WeakRefStrings.pos(poslen)
        colopts = getcoloptions(r)
        opts = colopts === nothing ? getoptions(r) : colopts[j]
        x, code, vpos, vlen, tlen = Parsers.xparse(T, getbuf(r), pos, pos + WeakRefStrings.len(poslen), opts)
    end
    return Parsers.ok(code) ? x : missing
end

Base.@propagate_inbounds function detect(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    @inbounds begin
        j = getcolumnmap(r)[i]
        col = getcolumns(r)[i]
        col.type isa StringTypes || stringsonly()
        poslen = getvalues(r)[j]
        WeakRefStrings.missingvalue(poslen) && return missing
        pos = WeakRefStrings.pos(poslen)
        colopts = getcoloptions(r)
        opts = colopts === nothing ? getoptions(r) : colopts[j]
        x, _, _ = detect(getbuf(r), pos, pos + WeakRefStrings.len(poslen) - 1, opts)
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
