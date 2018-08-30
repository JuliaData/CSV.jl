module CSV

using Mmap, Dates, Random, Parsers, Tables, CategoricalArrays, WeakRefStrings

@inline Parsers.intern(::Type{WeakRefString{UInt8}}, x::Tuple{Ptr{UInt8}, Int}) = WeakRefString(x)

substitute(::Type{Union{T, Missing}}, ::Type{T1}) where {T, T1} = Union{T1, Missing}
substitute(::Type{T}, ::Type{T1}) where {T, T1} = T1
substitute(::Type{Missing}, ::Type{T1}) where {T1} = Missing

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

struct Row{F}
    file::F
    row::Int
end

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, 2))) of:")
    show(io, getfield(r, 1))
end

# could just store NT internally and have Row take it as type parameter
# could separate kwargs out into individual fields and remove type parameter
struct File{NT, transpose, I, P, KW}
    name::String
    io::I
    parsinglayers::P
    positions::Vector{Int}
    originalpositions::Vector{Int}
    lastparsedcol::Base.RefValue{Int}
    kwargs::KW
    pools::Vector{CategoricalPool{String, UInt32, CategoricalString{UInt32}}}
    strict::Bool
end

function Base.show(io::IO, f::File{NT, transpose}) where {NT, transpose}
    println(io, "CSV.File($(f.name), rows=$(transpose ? missing : length(f.positions))):")
    println(io, "Columns:")
    Base.print_matrix(io, hcat(collect(Tables.names(NT)), collect(Tables.types(NT))))
    return
end

Base.eltype(f::F) where {F <: File} = Row{F}
Tables.AccessStyle(::Type{F}) where {F <: File} = Tables.RowAccess()
Tables.istable(::Type{<:File}) = true
Tables.schema(f::File{NT}) where {NT} = NT
Tables.rows(f::File) = f
Base.length(f::File{NT, transpose}) where {NT, transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
Base.size(f::File{NamedTuple{names, types}}) where {names, types} = (length(f), length(names))
Base.size(f::File{NamedTuple{}}) = (0, 0)

@inline function Base.iterate(f::File{NT, transpose}, st=1) where {NT, transpose}
    st > length(f) && return nothing
    if transpose
        if st == 1
            f.positions .= f.originalpositions
        end
    end
    return Row(f, st), st + 1
end

parsingtype(::Type{Missing}) = Missing
parsingtype(::Type{Union{Missing, T}}) where {T} = T
parsingtype(::Type{T}) where {T} = T

@inline function get🐱(pool::CategoricalPool, val::Tuple{Ptr{UInt8}, Int})
    index = Base.ht_keyindex2!(pool.invindex, val)
    if index > 0
        @inbounds v = pool.invindex.vals[index]
        return CatStr(v, pool)
    else
        v = CategoricalArrays.push_level!(pool, val)
        return CatStr(v, pool)
    end
end

@inline function parsefield(f, ::Type{CatStr}, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int})
    if r.result isa Missing
        return missing
    else
        @inbounds pool = f.pools[col]
        return get🐱(pool, r.result::Tuple{Ptr{UInt8}, Int})
    end
end

@inline function parsefield(f, T, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    if !Parsers.ok(r.code)
        strict ? throw(Error(Parsers.Error(f.io, r), row, col)) :
            println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")
    end
    return r.result
end

@noinline skipcells(f, n) = foreach(x->Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int}), 1:n)

Base.getproperty(csvrow::Row{F}, name::Symbol) where {F <: File{NT}} where {NT} =
    getproperty(csvrow, Tables.columntype(NT, name), Tables.columnindex(NT, name), name)

function Base.getproperty(csvrow::Row{F}, ::Type{T}, col::Int, name::Symbol) where {T, F <: File{NT, transpose}} where {NT, transpose}
    f = getfield(csvrow, 1)
    row = getfield(csvrow, 2)
    if transpose
        @inbounds Parsers.fastseek!(f.io, f.positions[col])
    else
        lastparsed = f.lastparsedcol[]
        if col === lastparsed + 1
        elseif col === 1
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
        elseif col > lastparsed + 1
            # skipping cells
            skipcells(f, col - lastparsed+1)
        elseif col !== lastparsed + 1
            # randomly seeking within row
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            skipcells(f, col - 1)
        end
    end
    r = parsefield(f, parsingtype(T), row, col, f.strict)
    if transpose
        @inbounds f.positions[col] = position(f.io)
    else
        f.lastparsedcol[] = col
    end
    return r
end
Base.propertynames(row::Row{F}) where {F <: File{NamedTuple{names, T}}} where {names, T} = names

"""
    CSV.File(source::Union{String, IO}; kwargs...) => CSV.File

Read a csv input (a filename given as a String, or any other IO source), returning a `CSV.File` object.
Opens the file and uses passed arguments to detect the # of columns and column types.
The returned object, `CSV.File`, supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values.
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.File(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

By supporting the tables interface, a `CSV.File` can also be a table input to any other table sink function. Like:

```julia
df = CSV.File(file) |> DataFrame # materialize a csv file as a DataFrame

db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

Supported keyword arguments include:
* File layout options:
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, can be faster in some cases
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a Range, indicating a span of rows to be combined together as column names; or an entire Vector of Symbols or Strings to use as column names
  * `datarow::Int`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit::Int`: an `Int` to indicate a limited number of rows to parse in a csv file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment::String`: string that occurs at the beginning of a line to signal parsing that row should be skipped
* Parsing options:
  * `missingstrings`, `missingstring`: either a single, or Vector of Strings to use as sentinel values that will be parsed as `missing`, by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a character or string that indicates how columns are delimited in a file
  * `ignore_repeated_delimiters=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: character (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='\\'`: character used to escape quote characters in a text field
  * `dateformat`: a date format string to indicate how Date/DateTime columns are formatted in a delimited file
  * `decimal`: how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: Vectors of Strings that indicate how `true` or `false` values are represented
* Column Type Options:
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected Float64 column to be parsed as Strings
  * `allowmissing=:all`: possible values are `:none`, `:auto`, and `:all`, to indicate how missing values are allow in columns; no columns contain missings, auto-detect columns that contain missings, or all columns may contain missings
  * `categorical=false`: whether columns with low cardinality (small number of unique values) should be read directly as a CategoricalArray
  * `strict=false`: whether invalid values should throw a parsing error or be replaced with missing values
"""
File(source::Union{String, IO};
    # file options
    use_mmap::Bool=!Sys.iswindows(),
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, UnitRange{Int}, Vector}=1,
    # by default, data starts immediately after header or start of file
    datarow::Int=-1,
    skipto::Union{Nothing, Int}=nothing,
    footerskip::Int=0,
    limit::Union{Nothing, Int}=nothing,
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Char, String}=",",
    ignore_repeated_delimiters::Bool=false,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='\\',
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    decimal::Union{UInt8, Char, Nothing}=nothing,
    truestrings::Union{Vector{String}, Nothing}=nothing,
    falsestrings::Union{Vector{String}, Nothing}=nothing,
    # type options
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    allowmissing::Symbol=:all,
    categorical::Bool=false,
    strict::Bool=false,
    debug::Bool=false) =
    File(source, use_mmap, header, datarow, skipto, footerskip, limit, transpose, comment, missingstrings, missingstring, delim, ignore_repeated_delimiters, quotechar, openquotechar, closequotechar, escapechar, dateformat, decimal, truestrings, falsestrings, types, typemap, allowmissing, categorical, strict, debug)

# File(file, true, 1, -1, 0, false, String[], "", ",", '"', nothing, nothing, '\\', nothing, nothing, nothing, nothing, nothing, Dict{Type, Type}(), :all, false)
function File(source::Union{String, IO},
    # file options
    use_mmap::Bool,
    header::Union{Integer, UnitRange{Int}, Vector},
    datarow::Int,
    skipto::Union{Nothing, Int},
    footerskip::Int,
    limit::Union{Nothing, Int},
    transpose::Bool,
    comment::Union{String, Nothing},
    # parsing options
    missingstrings,
    missingstring,
    delim::Union{Char, String},
    ignore_repeated_delimiters::Bool,
    quotechar::Union{UInt8, Char},
    openquotechar::Union{UInt8, Char, Nothing},
    closequotechar::Union{UInt8, Char, Nothing},
    escapechar::Union{UInt8, Char},
    dateformat::Union{String, Dates.DateFormat, Nothing},
    decimal::Union{UInt8, Char, Nothing},
    truestrings::Union{Vector{String}, Nothing},
    falsestrings::Union{Vector{String}, Nothing},
    # type options
    types,
    typemap::Dict,
    allowmissing::Symbol,
    categorical::Bool,
    strict::Bool,
    debug::Bool)
    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa Dict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    io = getio(source, use_mmap)

    consumeBOM!(io)

    kwargs = getkwargs(dateformat, decimal, getbools(truestrings, falsestrings))
    d = string(delim)
    missingstrings = isempty(missingstrings) ? [missingstring] : missingstrings
    parsinglayers = Parsers.Sentinel(missingstrings) |>
                    x->Parsers.Strip(x, d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t') |>
                    (openquotechar !== nothing ? x->Parsers.Quoted(x, openquotechar, closequotechar, escapechar) : x->Parsers.Quoted(x, quotechar, escapechar)) |>
                    x->Parsers.Delimited(x, d, "\n", "\r", "\r\n"; ignore_repeated=ignore_repeated_delimiters)

    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    datarow = skipto !== nothing ? skipto : datarow

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rows, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, footerskip)
        originalpositions = copy(positions)
        ref = Ref{Int}(min(something(limit, typemax(Int)), rows))
    else
        names, datapos = datalayout(header, parsinglayers, io, datarow)
        eof(io) && return File{NamedTuple{names, Tuple{(Missing for _ in names)...}}, false, typeof(io), typeof(parsinglayers), typeof(kwargs)}(getname(source), io, parsinglayers, Int64[], Int64[], Ref{Int}(0), kwargs, CategoricalPool{String, UInt32, CatStr}[], strict)
        positions = rowpositions(io, quotechar % UInt8, escapechar % UInt8, limit, parsinglayers, comment === nothing ? nothing : Parsers.Trie(comment))
        originalpositions = Int64[]
        footerskip > 0 && resize!(positions, length(positions) - footerskip)
        ref = Ref{Int}(0)
        debug && @show positions
    end

    if types isa Vector
        pools = CategoricalPool{String, UInt32, CatStr}[]
    else
        types, pools = detect(initialtypes(initialtype(allowmissing), types, names), io, positions, parsinglayers, kwargs, typemap, categorical, transpose, ref, debug)
    end

    !transpose && seek(io, positions[1])
    return File{NamedTuple{names, Tuple{types...}}, transpose, typeof(io), typeof(parsinglayers), typeof(kwargs)}(getname(source), io, parsinglayers, positions, originalpositions, ref, kwargs, pools, strict)
end

include("filedetection.jl")
include("typedetection.jl")

getio(str::String, use_mmap) = IOBuffer(use_mmap ? Mmap.mmap(str) : Base.read(str))
getio(io::IO, use_mmap) = io
getname(str::String) = str
getname(io::I) where {I <: IO} = string("<", I, ">")

function consumeBOM!(io)
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        (!eof(io) && Parsers.readbyte(io) == 0xbb) || seek(io, startpos)
        (!eof(io) && Parsers.readbyte(io) == 0xbf) || seek(io, startpos)
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

function __init__()
    CSV.File(joinpath(@__DIR__, "../test/testfiles/test_utf8.csv"), allowmissing=:auto) |> columntable
    return
end

end # module
