module CSV

using Mmap, Dates, Random, Unicode, Parsers, Tables, CategoricalArrays, PooledArrays, WeakRefStrings, DataFrames

function validate(fullpath::Union{AbstractString,IO}; kwargs...)
    Base.depwarn("`CSV.validate` is deprecated. `CSV.read` now prints warnings on misshapen files.", :validate)
    Tables.columns(File(fullpath; kwargs...))
    return
end

import Tables.transform
export transform

include("utils.jl")

struct File{transpose, P, KW, C}
    name::String
    io::IOBuffer
    datapos::Int
    parsinglayers::P
    names::Vector{Symbol}
    kwargs::KW
    cmt::C
    typecodes::Vector{TypeCode}
    typemap::Dict{TypeCode, TypeCode}
    escapestring
    ignorerepeated::Bool
    categorical::Union{Bool, Float64}
    pool::Union{Bool, Float64}
    rowsguess::Int64
    positions::Vector{Int64}
    limit::Int64
    strict::Bool
    silencewarnings::Bool
end

function Base.show(io::IO, f::File{transpose}) where {transpose}
    println(io, "CSV.File(\"$(f.name)\"):")
    println(io, f.names)
end

const EMPTY_POSITIONS = Int64[]
const EMPTY_TYPEMAP = Dict{TypeCode, TypeCode}()

"""
    CSV.File(source::Union{String, IO}; kwargs...) => CSV.File

Read a csv input (a filename given as a String, or any other IO source), returning a `CSV.File` object.
Opens the file and uses passed arguments to detect the number of columns and column types.
The returned `CSV.File` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values.
Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name `a` will become `a_1`).
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.File(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

By supporting the Tables.jl interface, a `CSV.File` can also be a table input to any other table sink function. Like:

```julia
# materialize a csv file as a DataFrame
df = CSV.File(file) |> DataFrame

# load a csv file directly into an sqlite database table
db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

Supported keyword arguments include:
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be combined together as column names; or an entire `Vector of Symbols` or `Strings` to use as column names
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: a `String` that occurs at the beginning of a line to signal parsing that row should be skipped
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a text field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted in a delimited file
  * `decimal`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented
* Column Type Options:
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `Strings`
  * `allowmissing=:all`: indicate how missing values are allowed in columns; possible values are `:all` - all columns may contain missings, `:auto` - auto-detect columns that contain missings or, `:none` - no columns may contain missings
  * `categorical::Union{Bool, Float64}=false`: if `true`, columns detected as `String` are returned as a `CategoricalArray`; alternatively, the proportion of unique values below which `String` columns should be treated as categorical (for example 0.1 for 10%)
  * `pool::Union{Bool, Float64}=false`: if `true`, columns detected as `String` are returned as a `PooledArray`; alternatively, the proportion of unique values below which `String` columns should be pooled (for example 0.1 for 10%)
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with missing values
  * `silencewarnings::Bool=false`: whether invalid value warnings should be silenced (requires `strict=false`)
"""
function File(source::Union{String, IO};
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, UnitRange{Int}, Vector}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Int=-1,
    skipto::Union{Nothing, Int}=nothing,
    footerskip=nothing,
    limit::Union{Nothing, Int}=nothing,
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    use_mmap::Bool=!Sys.iswindows(),
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
    decimal::Union{UInt8, Char, Nothing}=nothing,
    truestrings::Union{Vector{String}, Nothing}=nothing,
    falsestrings::Union{Vector{String}, Nothing}=nothing,
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=EMPTY_TYPEMAP,
    allowmissing::Symbol=:all,
    categorical::Union{Bool, Float64}=false,
    pool::Union{Bool, Float64}=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    kw...)

    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    footerskip !== nothing && @warn "`footerskip` keyword argument no longer supported; use `skipto` and `limit` keyword arguments to control which rows are parsed"
    io = getio(source, use_mmap)

    consumeBOM!(io)

    kwargs = getkwargs(dateformat, decimal, getbools(truestrings, falsestrings))
    if delim === nothing
        if isa(source, AbstractString)
            if endswith(source, ".tsv")
                d = "\t"
            elseif endswith(source, ".wsv")
                d = " "
            else
                d = ","
            end
        else
            d = ","
        end
    else
        d = string(delim)
    end
    whitespacedelim = d == " " || d == "\t"
    missingstrings = isempty(missingstrings) ? [missingstring] : missingstrings
    parsinglayers = Parsers.Sentinel(missingstrings) |>
                    x->Parsers.Strip(x, d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t') |>
                    (openquotechar !== nothing ? x->Parsers.Quoted(x, openquotechar, closequotechar, escapechar, !whitespacedelim) : x->Parsers.Quoted(x, quotechar, escapechar, !whitespacedelim)) |>
                    x->Parsers.Delimited(x, d; ignorerepeated=ignorerepeated, newline=true)

    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    datarow = skipto !== nothing ? skipto : datarow
    lim = something(limit, typemax(Int64))
    escapestring = WeakRefStrings.EscapeString{something(openquotechar, quotechar) % UInt8, something(closequotechar, quotechar) % UInt8, escapechar % UInt8}
    cmt = comment === nothing ? nothing : Parsers.Trie(comment)

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rowsguess, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, normalizenames)
        lim = min(lim, rowsguess)
        datapos = positions[1]
    else
        positions = EMPTY_POSITIONS
        names, datapos = datalayout(header, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
        rowsguess = guessnrows(io, quotechar % UInt8, escapechar % UInt8, parsinglayers, cmt, ignorerepeated)
    end

    T = type === nothing ? EMPTY : (typecode(type) | USER)
    if types isa Vector
        tcs = [typecode(T) | USER for T in types]
    elseif types isa AbstractDict
        tcs = initialtypes(T, types, names)
    else
        tcs = [T for _ = 1:length(names)]
    end

    return File{transpose, typeof(parsinglayers), typeof(kwargs), typeof(cmt)}(
        getname(source), io, datapos, parsinglayers, names, kwargs, cmt, tcs, typecodes(typemap), escapestring, ignorerepeated, categorical, pool, rowsguess, positions, lim, strict, silencewarnings)
end

initialtypes(T, x::AbstractDict{String}, names) = [haskey(x, string(nm)) ? typecode(x[string(nm)]) : T for nm in names]
initialtypes(T, x::AbstractDict{Symbol}, names) = [haskey(x, nm) ? typecode(x[nm]) : T for nm in names]
initialtypes(T, x::AbstractDict{Int}, names)    = [haskey(x, i) ? typecode(x[i]) : T for i = 1:length(names)]

include("filedetection.jl")
include("tables.jl")
include("write.jl")

function __init__()
    Threads.resize_nthreads!(VALUE_BUFFERS)
    Threads.resize_nthreads!(READSPLITLINE_RESULT)
    Threads.resize_nthreads!(READLINE_RESULT)
    return
end

"""
`CSV.read(source::Union{AbstractString,IO}; kwargs...)` => `DataFrame`

Parses a delimited file into a DataFrame.

Minimal error-reporting happens w/ `CSV.read` for performance reasons; for problematic csv files, try [`CSV.validate`](@ref) which takes exact same arguments as `CSV.read` and provides much more information for why reading the file failed.

Positional arguments:

* `source`: can be a file name (String) of the location of the csv file or `IO` object to read the csv from directly

Supported keyword arguments include:
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be combined together as column names; or an entire `Vector of Symbols` or `Strings` to use as column names
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: a `String` that occurs at the beginning of a line to signal parsing that row should be skipped
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters, newline characters, or quote characters
  * `escapechar='\\'`: the `Char` used to escape quote characters in a text field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted in a delimited file
  * `decimal`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented
* Column Type Options:
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `Strings`
  * `allowmissing=:all`: indicate how missing values are allowed in columns; possible values are `:all` - all columns may contain missings, `:auto` - auto-detect columns that contain missings or, `:none` - no columns may contain missings
  * `categorical::Union{Bool, Real}=false`: if `true`, columns detected as `String` are returned as a `CategoricalArray`; alternatively, the proportion of unique values below which `String` columns should be treated as categorical (for example 0.1 for 10%)
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with missing values
"""
read(source::Union{AbstractString, IO}; kwargs...) = CSV.File(source; kwargs...) |> DataFrame

end # module
