module CSV

using Mmap, Dates, Random, Unicode, Parsers, Tables, CategoricalArrays, WeakRefStrings

newline(code::Parsers.ReturnCode) = (code & Parsers.NEWLINE) > 0

const CatStr = CategoricalString{UInt32}

struct Error <: Exception
    error::Parsers.Error
    row::Int
    col::Int
end

function Base.showerror(io::IO, e::Error)
    println(io, "CSV.Error on row=$(e.row), column=$(e.col):")
    showerror(io, e.error)
end

struct File{transpose, columnaccess, I, P, KW}
    names::Vector{Symbol}
    types::Vector{Type}
    name::String
    io::I
    parsinglayers::P
    positions::Vector{Int}
    originalpositions::Vector{Int}
    currentrow::Base.RefValue{Int}
    lastparsedcol::Base.RefValue{Int}
    lastparsedcode::Base.RefValue{Parsers.ReturnCode}
    kwargs::KW
    pools::Vector{CategoricalPool{String, UInt32, CatStr}}
    strict::Bool
    silencewarnings::Bool
end

function Base.show(io::IO, f::File{transpose}) where {transpose}
    println(io, "CSV.File(\"$(f.name)\", rows=$(transpose ? missing : length(f.positions))):")
    show(io, Tables.schema(f))
end

const EMPTY_TYPEMAP = Dict{Type, Type}()

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
  * `categorical::Union{Bool, Real}=false`: if `true`, columns detected as `String` are returned as a `CategoricalArray`; alternatively, the proportion of unique values below which `String` columns should be treated as categorical (for example 0.1 for 10%)
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
    footerskip::Int=0,
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
    types=nothing,
    typemap::Dict=EMPTY_TYPEMAP,
    allowmissing::Symbol=:all,
    categorical::Union{Bool, Real}=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    kw...)

    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    io = getio(source, use_mmap)

    consumeBOM!(io)

    kwargs = getkwargs(dateformat, decimal, getbools(truestrings, falsestrings))
    if delim === nothing
        if isa(source, AbstractString)
            if endswith(source, ".tsv")
                delim = "\t"
            elseif endswith(source, ".wsv")
                delim = " "
            else
                delim = ","
            end
        else
            delim = ","
        end
    end
    d = string(delim)
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

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rows, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, footerskip, normalizenames)
        originalpositions = copy(positions)
        ref = Ref{Int}(min(something(limit, typemax(Int)), rows))
        columnaccess = false
    else
        cmt = comment === nothing ? nothing : Parsers.Trie(comment)
        names, datapos = datalayout(header, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
        eof(io) && return File{false, true, typeof(io), typeof(parsinglayers), typeof(kwargs)}(names, Type[Missing for _ in names], getname(source), io, parsinglayers, Int64[], Int64[], Ref{Int}(0), Ref{Int}(0), Ref(Parsers.SUCCESS), kwargs, CategoricalPool{String, UInt32, CatStr}[], strict, silencewarnings)
        positions = rowpositions(io, quotechar % UInt8, escapechar % UInt8, limit, parsinglayers, cmt, ignorerepeated)
        originalpositions = Int64[]
        footerskip > 0 && resize!(positions, length(positions) - footerskip)
        ref = Ref{Int}(0)
        # if the # of cells in the file is less than 500K
        columnaccess = (length(names) * length(positions)) < 500_000
        # debug && @show positions
    end

    if types isa Vector
        pools = Vector{CategoricalPool{String, UInt32, CatStr}}(undef, length(types))
        for col = 1:length(types)
            T = types[col]
            if T !== Missing && Base.nonmissingtype(T) <: CatStr
                pools[col] = CategoricalPool{String, UInt32}()
            end
        end
        finaltypes = types
    else
        finaltypes, pools = detect(initialtypes(initialtype(allowmissing), types, names), io, positions, parsinglayers, kwargs, typemap, categorical, transpose, ref, debug)
        if allowmissing === :none
            # make sure we didn't detect any missing values in any columns
            if any(T->T >: Missing, finaltypes)
                throw(ArgumentError("`allowmissing=:none`, but missing values were detected in csv file: $finaltypes"))
            end
        end
    end

    !transpose && Parsers.fastseek!(io, positions[1])
    return File{transpose, columnaccess, typeof(io), typeof(parsinglayers), typeof(kwargs)}(names, finaltypes, getname(source), io, parsinglayers, positions, originalpositions, Ref(1), ref, Ref(Parsers.SUCCESS), kwargs, pools, strict, silencewarnings)
end

include("filedetection.jl")
include("typedetection.jl")
include("tables.jl")

getio(str::String, use_mmap) = use_mmap ? IOBuffer(Mmap.mmap(str)) : Parsers.BufferedIO(open(str))
getio(io::IO, use_mmap) = Parsers.BufferedIO(io)
getname(str::String) = str
getname(io::I) where {I <: IO} = string("<", I, ">")

function consumeBOM!(io)
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        (!eof(io) && Parsers.readbyte(io) == 0xbb) || Parsers.fastseek!(io, startpos)
        (!eof(io) && Parsers.readbyte(io) == 0xbf) || Parsers.fastseek!(io, startpos)
    end
    return
end

getbools(::Nothing, ::Nothing) = nothing
getbools(trues::Vector{String}, falses::Vector{String}) = Parsers.Trie(append!([x=>true for x in trues], [x=>false for x in falses]))
getbools(trues::Vector{String}, ::Nothing) = Parsers.Trie(append!([x=>true for x in trues], ["false"=>false]))
getbools(::Nothing, falses::Vector{String}) = Parsers.Trie(append!(["true"=>true], [x=>false for x in falses]))

getkwargs(df::Nothing, dec::Nothing, bools::Nothing) = NamedTuple()
getkwargs(df::String, dec::Nothing, bools::Nothing) = (dateformat=Dates.DateFormat(df),)
getkwargs(df::Dates.DateFormat, dec::Nothing, bools::Nothing) = (dateformat=df,)

getkwargs(df::Nothing, dec::Union{UInt8, Char}, bools::Nothing) = (decimal=dec % UInt8,)
getkwargs(df::String, dec::Union{UInt8, Char}, bools::Nothing) = (dateformat=Dates.DateFormat(df), decimal=dec % UInt8)
getkwargs(df::Dates.DateFormat, dec::Union{UInt8, Char}, bools::Nothing) = (dateformat=df, decimal=dec % UInt8)

getkwargs(df::Nothing, dec::Nothing, bools::Parsers.Trie) = (bools=bools,)
getkwargs(df::String, dec::Nothing, bools::Parsers.Trie) = (dateformat=Dates.DateFormat(df), bools=bools)
getkwargs(df::Dates.DateFormat, dec::Nothing, bools::Parsers.Trie) = (dateformat=df, bools=bools)

getkwargs(df::Nothing, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (decimal=dec % UInt8, bools=bools)
getkwargs(df::String, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (dateformat=Dates.DateFormat(df), decimal=dec % UInt8, bools=bools)
getkwargs(df::Dates.DateFormat, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (dateformat=df, decimal=dec % UInt8, bools=bools)

include("write.jl")
include("deprecated.jl")
include("validate.jl")
include("transforms.jl")

function __init__()
    Threads.resize_nthreads!(VALUE_BUFFERS)
    FUNCTIONMAP[Int64] = @cfunction(getsetInt!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[Float64] = @cfunction(getsetFloat!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[Date] = @cfunction(getsetDate!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[DateTime] = @cfunction(getsetDateTime!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[Bool] = @cfunction(getsetBool!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[String] = @cfunction(getsetString!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[Missing] = @cfunction(getsetMissing!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[WeakRefString{UInt8}] = @cfunction(getsetWeakRefString!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNCTIONMAP[CatStr] = @cfunction(getsetCatStr!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    FUNC_ANY[] = @cfunction(getsetAny!, Cvoid, (Any, Any, Cssize_t, Cssize_t))
    return
end

"""
`CSV.read(fullpath::Union{AbstractString,IO}, sink=DataFrame; kwargs...)` => `typeof(sink)`

Parses a delimited file into a Julia structure (a DataFrame by default, but any valid Tables.jl sink function can be provided).

Minimal error-reporting happens w/ `CSV.read` for performance reasons; for problematic csv files, try [`CSV.validate`](@ref) which takes exact same arguments as `CSV.read` and provides much more information for why reading the file failed.

Positional arguments:

* `fullpath`: can be a file name (String) of the location of the csv file or `IO` object to read the csv from directly
* `sink`: `DataFrame` by default, but may also be any other Tables.jl sink function

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
function read end

function read(fullpath::Union{AbstractString,IO}, sink=DataFrame, args...; append::Bool=false, transforms::AbstractDict=Dict{Int,Function}(), kwargs...)
    if !isempty(args)
        Base.depwarn("`CSV.read(source, $sink, $args)` is deprecated; pass a valid Tables.jl sink function as the 2nd argument directly", nothing)
        sink = sink(args...)
    end
    if append
        Base.depwarn("`CSV.read(source; append=true)` is deprecated in favor of sink-specific options; e.g. DataFrames supports `CSV.File(filename) |> x->append!(existing_df, x)` to append the rows of a csv file to an existing DataFrame", nothing)
        if sink isa DataFrame
            sink = x->append!(sink, x)
        end
    end
    f = CSV.File(fullpath; kwargs...)
    if !isempty(transforms)
        Base.depwarn("`CSV.read(source; transforms=Dict(...)` is deprecated in favor of `CSV.File(source) |> transform(col1=x->...) |> DataFrame`", nothing)
        return sink(transform(f, transforms))
    end
    return f |> sink
end

end # module
