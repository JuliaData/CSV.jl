# structure for iterating over a csv file
# no automatic type inference is done, but types are allowed to be passed
# for as many columns as desired; `CSV.detect(row, i)` can also be used to
# use the same inference logic used in `CSV.File` for determing a cell's typed value
struct Rows{transpose, O, O2, IO, T, V}
    name::String
    names::Vector{Symbol} # only includes "select"ed columns
    finaltypes::Vector{Type} # only includes "select"ed columns
    columnmap::Vector{Int} # maps "select"ed column index to actual file column index
    types::Vector{Type} # includes *all* columns (whether or not selected)
    flags::Vector{UInt8}
    cols::Int64
    e::UInt8
    buf::IO
    datapos::Int64
    datarow::Int
    len::Int
    limit::Int64
    options::O # Parsers.Options
    coloptions::O2 # Union{Nothing, Vector{Parsers.Options}}
    customtypes::T
    positions::Vector{Int64}
    reusebuffer::Bool
    columns::Vector{AbstractVector} # for parsing, allocated once and used for each iteration
    values::Vector{V} # once values are parsed, put in values; allocated on each iteration if reusebuffer=false
    lookup::Dict{Symbol, Int}
    numwarnings::Base.RefValue{Int}
    maxwarnings::Int
end

function Base.show(io::IO, r::Rows)
    println(io, "CSV.Rows(\"$(r.name)\"):")
    println(io, "Size: $(r.cols)")
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
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.). Note that if a row number header and `comment` or `ignoreemtpylines` are provided, the header row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header row will actually be the next non-commented row.
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data; providing a `datarow` or `skipto` argument does _not_ affect the `header` argument. Note that if a row number `datarow` and `comment` or `ignoreemtpylines` are provided, the data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the data row will actually be the next non-commented row.
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing. Note that if a row number header or `datarow` and `comment` are provided, the header/data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header/data row will actually be the next non-commented row.
  * `ignoreemptylines::Bool=true`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted for the entire file
  * `decimal='.'`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented; by default only `true` and `false` are treated as `Bool`
* Column Type Options:
  * `type`: a single type to use for parsing an entire file; i.e. all columns will be treated as the same type; useful for matrix-like data files
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64; if a `Vector` if provided, it must match the # of columns provided or detected in `header`
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `String`
  * `lazystrings::Bool=true`: avoid allocating full strings while parsing; accessing a string column will materialize the full `String`
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
  * `maxwarnings::Int=100`: if more than `maxwarnings` number of warnings are printed while parsing, further warnings will be silenced by default
* Iteration options:
  * `reusebuffer=false`: while iterating, whether a single row buffer should be allocated and reused on each iteration; only use if each row will be iterated once and not re-used (e.g. it's not safe to use this option if doing `collect(CSV.Rows(file))` because only current iterated row is "valid")
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
    limit::Integer=typemax(Int64),
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    use_mmap=nothing,
    ignoreemptylines::Bool=true,
    select=nothing,
    drop=nothing,
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
    pool::Union{Bool, Real}=0.1,
    lazystrings::Bool=true,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    reusebuffer::Bool=false,
    maxwarnings::Int=100,
    kw...)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, use_mmap, ignoreemptylines, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, pool, lazystrings, strict, silencewarnings, debug, parsingdebug, true)
    columns = allocate(1, h.cols, h.types, h.flags, nothing)
    values = all(x->x == Union{String, Missing}, h.types) && lazystrings ? Vector{PosLen}(undef, h.cols) : Vector{Any}(undef, h.cols)
    finaltypes = copy(h.types)
    columnmap = [i for i = 1:h.cols]
    deleteat!(h.names, h.todrop)
    deleteat!(finaltypes, h.todrop)
    deleteat!(columnmap, h.todrop)
    lookup = Dict(nm=>i for (i, nm) in enumerate(h.names))
    return Rows{transpose, typeof(h.options), typeof(h.coloptions), typeof(h.buf), typeof(h.customtypes), eltype(values)}(
        h.name,
        h.names,
        finaltypes,
        columnmap,
        h.types,
        h.flags,
        h.cols,
        h.e,
        h.buf,
        h.datapos,
        h.datarow,
        h.len,
        limit,
        h.options,
        h.coloptions,
        h.customtypes,
        h.positions,
        reusebuffer,
        columns,
        values,
        lookup,
        Ref(0),
        maxwarnings
    )
end

Tables.rowtable(::Type{<:Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = Tables.Schema(r.names, r.finaltypes)
Base.eltype(r::Rows) = Row2
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()

const EMPTY_TYPEMAP = Dict{Type, Type}()
const EMPTY_REFS = RefPool[]

@inline function setcustom!(::Type{T}, values, columns, i) where {T}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(typeof(column))")
        end)
        for i = 1:fieldcount(T)
            vec = fieldtype(T, i)
            pushfirst!(block.args, quote
                if column isa $(fieldtype(vec, 1))
                    @inbounds values[i] = column[1]
                    return
                end
            end)
        end
        pushfirst!(block.args, quote
            @inbounds column = columns[col]
        end)
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        @inbounds column = columns[i]
        @inbounds values[i] = column[1]
        return
    end
end

@inline function Base.iterate(r::Rows{transpose, O, O2, IO, T, V}, (pos, len, row)=(r.datapos, r.len, 1)) where {transpose, O, O2, IO, T, V}
    (pos > len || row > r.limit) && return nothing
    pos > len && return nothing
    pos = parserow(1, Val(transpose), r.cols, EMPTY_TYPEMAP, r.columns, r.datapos, r.buf, pos, len, r.positions, 0.0, EMPTY_REFS, 1, r.datarow + row - 2, r.types, r.flags, false, r.options, r.coloptions, T, r.numwarnings, r.maxwarnings)
    cols = r.cols
    values = r.reusebuffer ? r.values : Vector{V}(undef, cols)
    columns = r.columns
    for i = 1:cols
        @inbounds column = columns[i]
        if column isa Vector{PosLen}
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
        elseif T !== Tuple{}
            setcustom!(T, values, columns, i)
        else
            error("bad array type: $(typeof(column))")
        end
    end
    return Row2{O, O2, V}(r.names, r.finaltypes, r.columnmap, r.types, r.lookup, values, r.buf, r.e, r.options, r.coloptions), (pos, len, row + 1)
end

struct Row2{O, O2, V} <: Tables.AbstractRow
    names::Vector{Symbol}
    finaltypes::Vector{Type}
    columnmap::Vector{Int}
    types::Vector{Type}
    lookup::Dict{Symbol, Int}
    values::Vector{V}
    buf::Vector{UInt8}
    e::UInt8
    options::O
    coloptions::O2
end

getnames(r::Row2) = getfield(r, :names)
getfinaltypes(r::Row2) = getfield(r, :finaltypes)
getcolumnmap(r::Row2) = getfield(r, :columnmap)
gettypes(r::Row2) = getfield(r, :types)
getlookup(r::Row2) = getfield(r, :lookup)
getvalues(r::Row2) = getfield(r, :values)
getbuf(r::Row2) = getfield(r, :buf)
gete(r::Row2) = getfield(r, :e)
getoptions(r::Row2) = getfield(r, :options)
getcoloptions(r::Row2) = getfield(r, :coloptions)

Tables.columnnames(r::Row2) = getnames(r)

Base.checkbounds(r::Row2, i) = 0 < i < length(r)

Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, getlookup(r)[nm])
Tables.getcolumn(r::Row2, i::Int) = Tables.getcolumn(r, gettypes(r)[i], i, getnames(r)[i])

Tables.getcolumn(r::Row2{O, O2, PosLen}, nm::Symbol) where {O, O2} = @inbounds Tables.getcolumn(r, getlookup(r)[nm])

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{Missing}, i::Int, nm::Symbol)
    @boundscheck checkbounds(r, i)
    return missing
end

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{T}, i::Int, nm::Symbol) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    @inbounds x = getvalues(r)[j]
    return x
end

Base.@propagate_inbounds Tables.getcolumn(r::Row2{O, O2, PosLen}, ::Type{T}, i::Int, nm::Symbol) where {T, O, O2} = error("row values are string only; requested type $T not supported; see `Parsers.parse(row, $T, $i)`")

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Union{Type{Union{Missing, String}}, Type{String}}, i::Int, nm::Symbol)
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    @inbounds poslen = getvalues(r)[j]
    if poslen isa Missing
        return missing
    elseif poslen isa String
        return poslen
    else
        return str(getbuf(r), gete(r), poslen)
    end
end

Tables.getcolumn(r::Row2{O, O2, PosLen}, ::Union{Type{Union{Missing, String}}, Type{String}}, i::Int, nm::Symbol) where {O, O2} = Tables.getcolumn(r, i)
Base.@propagate_inbounds function Tables.getcolumn(r::Row2{O, O2, PosLen}, i::Int) where {O, O2}
    @boundscheck checkbounds(r, i)
    @inbounds j = getcolumnmap(r)[i]
    @inbounds poslen = getvalues(r)[j]
    if poslen isa Missing
        return missing
    else
        return @inbounds str(getbuf(r), gete(r), poslen)
    end
end

@noinline stringsonly() = error("Parsers.parse only allowed on String column types")

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    type = gettypes(r)[j]
    (type == String || type == Union{String, Missing}) || stringsonly()
    @inbounds poslen = getvalues(r)[j]
    missingvalue(poslen) && return missing
    pos = getpos(poslen)
    colopts = getcoloptions(r)
    opts = colopts === nothing ? getoptions(r) : colopts[j]
    x, code, vpos, vlen, tlen = Parsers.xparse(T, getbuf(r), pos, pos + getlen(poslen), opts)
    return Parsers.ok(code) ? x : missing
end

Base.@propagate_inbounds function detect(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    T = gettypes(r)[j]
    (T == String || T == Union{String, Missing}) || stringsonly()
    @inbounds offlen = getvalues(r)[j]
    missingvalue(offlen) && return missing
    pos = getpos(offlen)
    colopts = getcoloptions(r)
    opts = colopts === nothing ? getoptions(r) : colopts[j]
    x = detect(getbuf(r), pos, pos + getlen(offlen) - 1, opts)
    return x === nothing ? r[i] : x
end

function Parsers.parse(::Type{T}, r::Row2, nm::Symbol) where {T}
    @inbounds x = Parsers.parse(T, r, getlookup(r)[nm])
    return x
end

function detect(r::Row2, nm::Symbol)
    @inbounds x = detect(r, getlookup(r)[nm])
    return x
end
