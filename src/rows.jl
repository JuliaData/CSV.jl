# structure for iterating over a csv file
# no automatic type inference is done, but types are allowed to be passed
# for as many columns as desired; `CSV.detect(row, i)` can also be used to
# use the same inference logic used in `CSV.File` for determing a cell's typed value
struct Rows{transpose, O, IO}
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
    len::Int
    limit::Int64
    options::O # Parsers.Options
    coloptions::Union{Nothing, Vector{Parsers.Options}}
    positions::Vector{Int64}
    reusebuffer::Bool
    tapes::Vector{AbstractVector}
    lookup::Dict{Symbol, Int}
end

function Base.show(io::IO, r::Rows)
    println(io, "CSV.Rows(\"$(r.name)\"):")
    println(io, "Size: $(r.cols)")
    show(io, Tables.schema(r))
end

"""
    CSV.Rows(source; kwargs...) => CSV.Rows

Read a csv input (a filename given as a String or FilePaths.jl type, or any other IO source), returning a `CSV.Rows` object.

While similar to [`CSV.File`](@ref), `CSV.Rows` provides a slightly different interface, the tradeoffs including:
  * Very minimal memory footprint; while iterating, only the current row values are buffered
  * Only provides row access via iteration; to access columns, one can stream the rows into a table type
  * Performs no type inference; each column/cell is essentially treated as `Union{String, Missing}`, users can utilize the performant `Parsers.parse(T, str)` to convert values to a  more specific type if needed

Opens the file and uses passed arguments to detect the number of columns, ***but not*** column types.
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
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.)
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
  * `ignoreemptylines::Bool=false`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether warnings should be silenced
* Iteration options:
  * `reusebuffer=false`: while iterating, whether a single row buffer should be allocated and reused on each iteration; only use if each row will be iterated once and not re-used (e.g. it's not safe to use this option if doing `collect(CSV.Rows(file))`)
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
    use_mmap::Bool=!Sys.iswindows(),
    ignoreemptylines::Bool=false,
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
    categorical::Union{Bool, Real}=false,
    pool::Union{Bool, Real}=0.1,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    reusebuffer::Bool=false,
    kw...)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, limit, transpose, comment, use_mmap, ignoreemptylines, false, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, categorical, pool, strict, silencewarnings, debug, parsingdebug, true)
    tapes, poslens = allocate(1, h.cols, h.types, h.flags)
    finaltypes = copy(h.types)
    columnmap = [i for i = 1:h.cols]
    deleteat!(h.names, h.todrop)
    deleteat!(finaltypes, h.todrop)
    deleteat!(columnmap, h.todrop)
    lookup = Dict(nm=>i for (i, nm) in enumerate(h.names))
    return Rows{transpose, typeof(h.options), typeof(h.buf)}(
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
        h.len,
        limit,
        h.options,
        h.coloptions,
        h.positions,
        reusebuffer,
        tapes,
        lookup
    )
end

Tables.rowtable(::Type{<:Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = Tables.Schema(r.names, r.finaltypes)
Base.eltype(r::Rows) = Row2
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()

const EMPTY_TYPEMAP = Dict{Type, Type}()
const EMPTY_POSLENS = Vector{Vector{UInt64}}()
const EMPTY_REFS = RefPool[]

@inline function Base.iterate(r::Rows{transpose}, (pos, len, row)=(r.datapos, r.len, 1)) where {transpose}
    (pos > len || row > r.limit) && return nothing
    pos > len && return nothing
    tapes = r.reusebuffer ? r.tapes : allocate(1, r.cols, r.types, r.flags)[1]
    pos = parserow(1, Val(transpose), r.cols, EMPTY_TYPEMAP, tapes, EMPTY_POSLENS, r.buf, pos, len, r.positions, 0.0, EMPTY_REFS, 1, 1, r.types, r.flags, false, r.options, r.coloptions)
    return Row2(r.names, r.finaltypes, r.columnmap, r.types, r.lookup, tapes, r.buf, r.e, r.options, r.coloptions), (pos, len, row + 1)
end

struct Row2{O} <: Tables.AbstractRow
    names::Vector{Symbol}
    finaltypes::Vector{Type}
    columnmap::Vector{Int}
    types::Vector{Type}
    lookup::Dict{Symbol, Int}
    tapes::Vector{AbstractVector}
    buf::Vector{UInt8}
    e::UInt8
    options::O
    coloptions::Union{Nothing, Vector{Parsers.Options}}
end

getnames(r::Row2) = getfield(r, :names)
getfinaltypes(r::Row2) = getfield(r, :finaltypes)
getcolumnmap(r::Row2) = getfield(r, :columnmap)
gettypes(r::Row2) = getfield(r, :types)
getlookup(r::Row2) = getfield(r, :lookup)
gettapes(r::Row2) = getfield(r, :tapes)
getbuf(r::Row2) = getfield(r, :buf)
gete(r::Row2) = getfield(r, :e)
getoptions(r::Row2) = getfield(r, :options)
getcoloptions(r::Row2) = getfield(r, :coloptions)

Tables.columnnames(r::Row2) = getnames(r)

Base.checkbounds(r::Row2, i) = 0 < i < length(r)

Tables.getcolumn(r::Row2, nm::Symbol) = Tables.getcolumn(r, getlookup(r)[nm])
Tables.getcolumn(r::Row2, i::Int) = Tables.getcolumn(r, gettypes(r)[i], i, getnames(r)[i])

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{Missing}, i::Int, nm::Symbol)
    @boundscheck checkbounds(r, i)
    return missing
end

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Type{T}, i::Int, nm::Symbol) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    @inbounds x = gettapes(r)[j][1]
    return x
end

Base.@propagate_inbounds function Tables.getcolumn(r::Row2, ::Union{Type{Union{Missing, String}}, Type{String}}, i::Int, nm::Symbol)
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    @inbounds poslen = gettapes(r)[j][1]
    if poslen isa Missing
        return missing
    elseif poslen isa String
        return poslen
    else
        return str(getbuf(r), gete(r), poslen)
    end
end

@noinline stringsonly() = error("Parsers.parse only allowed on String column types")

Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    j = getcolumnmap(r)[i]
    type = gettypes(r)[j]
    (type == String || type == Union{String, Missing}) || stringsonly()
    @inbounds poslen = gettapes(r)[j][1]
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
    @inbounds offlen = gettapes(r)[j][1]
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
