# a Row "view" type for iterating `CSV.File`
struct Row <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{Column}
    lookup::Dict{Symbol, Column}
    row::Int
end

getnames(r::Row) = getfield(r, :names)
getcolumn(r::Row, col::Int) = getfield(r, :columns)[col].column
getcolumn(r::Row, col::Symbol) = getfield(r, :lookup)[col].column
getrow(r::Row) = getfield(r, :row)

Tables.columnnames(r::Row) = getnames(r)

@inline function Tables.getcolumn(row::Row, ::Type{T}, i::Int, nm::Symbol) where {T}
    column = getcolumn(row, i)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Tables.getcolumn(row::Row, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Tables.getcolumn(row::Row, col::Int)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

"""
    CSV.File(input; kwargs...) => CSV.File

Read a UTF-8 CSV input and return a `CSV.File` object, which is like a lightweight table/dataframe, allowing dot-access to columns
and iterating rows. Satisfies the Tables.jl interface, so can be passed to any valid sink, yet to avoid unnecessary copies of data,
use `CSV.read(input, sink; kwargs...)` instead if the `CSV.File` intermediate object isn't needed.

The [`input`](@ref input) argument can be one of:
  * filename given as a string or FilePaths.jl type
  * a `Vector{UInt8}` or `SubArray{UInt8, 1, Vector{UInt8}}` byte buffer
  * a `CodeUnits` object, which wraps a `String`, like `codeunits(str)`
  * a csv-formatted string can also be passed like `IOBuffer(str)`
  * a `Cmd` or other `IO`
  * a gzipped file (or gzipped data in any of the above), which will automatically be decompressed for parsing
  * a `Vector` of any of the above, which will parse and vertically concatenate each source, returning a single, "long" `CSV.File`

To read a csv file from a url, use the Downloads.jl stdlib or HTTP.jl package, where the resulting downloaded tempfile or `HTTP.Response` body can be passed like:
```julia
using Downloads, CSV
f = CSV.File(Downloads.download(url))

# or

using HTTP, CSV
f = CSV.File(HTTP.get(url).body)
```

Opens the file or files and uses passed arguments to detect the number of columns and column types, unless column types are provided
manually via the `types` keyword argument. Note that passing column types manually can slightly increase performance
for each column type provided (column types can be given as a `Vector` for all columns, or specified per column via
name or index in a `Dict`).

When a `Vector` of inputs is provided, the column names and types of each separate file/input must match to be vertically concatenated. Separate threads will
be used to parse each input, which will each parse their input using just the single thread. The results of all threads are then vertically concatenated using
`ChainedVector`s to lazily concatenate each thread's columns.

For text encodings other than UTF-8, load the [StringEncodings.jl](https://github.com/JuliaStrings/StringEncodings.jl)
package and call e.g. `CSV.File(open(read, input, enc"ISO-8859-1"))`.

The returned `CSV.File` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values. `CSV.File`
also supports entire column access like a `DataFrame` via direct property access on the file object, like `f = CSV.File(file); f.col1`.
Or by getindex access with column names, like `f[:col1]` or `f["col1"]`. The returned columns are `AbstractArray` subtypes, including:
`SentinelVector` (for integers), regular `Vector`, `PooledVector` for pooled columns, `MissingVector` for columns of all `missing` values,
`PosLenStringVector` when `stringtype=PosLenString` is passed, and `ChainedVector` will chain one of the previous array types together for
data inputs that use multiple threads to parse (each thread parses a single "chain" of the input).
Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name `a` will become `a_1`).
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.File(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

By supporting the Tables.jl interface, a `CSV.File` can also be a table input to any other table sink function. Like:

```julia
# materialize a csv file as a DataFrame, copying columns from CSV.File
df = CSV.File(file) |> DataFrame

# to avoid making a copy of parsed columns, use CSV.read
df = CSV.read(file, DataFrame)

# load a csv file directly into an sqlite database table
db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

$KEYWORD_DOCS
"""
struct File <: AbstractVector{Row}
    name::String
    names::Vector{Symbol}
    types::Vector{Type}
    rows::Int
    cols::Int
    columns::Vector{Column}
    lookup::Dict{Symbol, Column}
end

getname(f::File) = getfield(f, :name)
getnames(f::File) = getfield(f, :names)
gettypes(f::File) = getfield(f, :types)
getrows(f::File) = getfield(f, :rows)
getcols(f::File) = getfield(f, :cols)
getcolumns(f::File) = getfield(f, :columns)
getlookup(f::File) = getfield(f, :lookup)
getcolumn(f::File, col::Int) = getfield(f, :columns)[col]
getcolumn(f::File, col::Symbol) = getfield(f, :lookup)[col]

function Base.show(io::IO, f::File)
    println(io, "CSV.File(\"$(getname(f))\"):")
    println(io, "Size: $(getrows(f)) x $(getcols(f))")
    show(io, Tables.schema(f))
end

Base.IndexStyle(::Type{File}) = Base.IndexLinear()
Base.eltype(::File) = Row
Base.size(f::File) = (getrows(f),)

Tables.isrowtable(::Type{File}) = true
Tables.columnaccess(::Type{File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), gettypes(f))
Tables.columns(f::File) = f
Tables.columnnames(f::File) = getnames(f)
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return haskey(lookup, col) ? lookup[col].column : getfield(f, col)
end

function Base.getindex(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return haskey(lookup, col) ? lookup[col].column : getfield(f, col)
end

Base.getindex(f::File, col::String) = getindex(f, Symbol(col))

Tables.getcolumn(f::File, nm::Symbol) = getcolumn(f, nm).column
Tables.getcolumn(f::File, i::Int) = getcolumn(f, i).column

Base.@propagate_inbounds function Base.getindex(f::File, row::Int)
    @boundscheck checkbounds(f, row)
    return Row(getnames(f), getcolumns(f), getlookup(f), row)
end

function File(source::ValidSources;
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
    threaded::Union{Bool, Nothing}=nothing,
    ntasks::Union{Nothing, Integer}=nothing,
    tasks::Union{Nothing, Integer}=nothing,
    rows_to_check::Integer=DEFAULT_ROWS_TO_CHECK,
    lines_to_check=nothing,
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
    pool=DEFAULT_POOL,
    downcast::Bool=false,
    lazystrings::Bool=false,
    stringtype::StringTypes=DEFAULT_STRINGTYPE,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=DEFAULT_MAX_WARNINGS,
    debug::Bool=false,
    parsingdebug::Bool=false,
    validate::Bool=true,
    )
    
end