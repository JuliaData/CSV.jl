module CSV

using Mmap, Dates, Random, Unicode, Parsers, Tables, CategoricalArrays, WeakRefStrings

@inline Parsers.intern(::Type{WeakRefString{UInt8}}, x::Tuple{Ptr{UInt8}, Int}) = WeakRefString(x)

substitute(::Type{Union{T, Missing}}, ::Type{T1}) where {T, T1} = Union{T1, Missing}
substitute(::Type{T}, ::Type{T1}) where {T, T1} = T1
substitute(::Type{Missing}, ::Type{T1}) where {T1} = Missing

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
    currentrow::Base.RefValue{Int}
    lastparsedcol::Base.RefValue{Int}
    lastparsedcode::Base.RefValue{Parsers.ReturnCode}
    kwargs::KW
    pools::Vector{CategoricalPool{String, UInt32, CategoricalString{UInt32}}}
    strict::Bool
end

function Base.show(io::IO, f::File{NT, transpose}) where {NT, transpose}
    println(io, "CSV.File($(f.name), rows=$(transpose ? missing : length(f.positions))):")
    show(io, Tables.schema(f))
end

Tables.istable(::Type{<:File}) = true
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f
Tables.schema(f::File{NamedTuple{names, types}}) where {names, types} = Tables.Schema(names, types)
Base.eltype(f::F) where {F <: File} = Row{F}
Base.length(f::File{NT, transpose}) where {NT, transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
Base.size(f::File{NamedTuple{names, types}}) where {names, types} = (length(f), length(names))
Base.size(f::File{NamedTuple{}}) = (0, 0)

@inline function Base.iterate(f::File{NT, transpose}, st=1) where {NT, transpose}
    st > length(f) && return nothing
    # println("row=$st")
    if transpose
        st === 1 && (f.positions .= f.originalpositions)
    else
        @inbounds Parsers.fastseek!(f.io, f.positions[st])
        f.currentrow[] = st
        f.lastparsedcol[] = 0
        f.lastparsedcode[] = Parsers.SUCCESS
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
    f.lastparsedcode[] = r.code
    if r.result isa Missing
        return missing
    else
        @inbounds pool = f.pools[col]
        return get🐱(pool, r.result::Tuple{Ptr{UInt8}, Int})
    end
end

@inline function parsefield(f, T, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    f.lastparsedcode[] = r.code
    if !Parsers.ok(r.code)
        strict ? throw(Error(Parsers.Error(f.io, r), row, col)) :
            println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")
    end
    return r.result
end

@noinline function skipcells(f, n)
    r = Parsers.Result(Tuple{Ptr{UInt8}, Int})
    for _ = 1:n
        r.code = Parsers.SUCCESS
        Parsers.parse!(f.parsinglayers, f.io, r)
        if newline(r.code)
            f.lastparsedcode[] = r.code
            return false
        end
    end
    f.lastparsedcode[] = r.code
    return true
end

@inline Base.getproperty(csvrow::Row{F}, name::Symbol) where {F <: File{NamedTuple{names, types}}} where {names, types} =
    getproperty(csvrow, Tables.columntype(names, types, name), Tables.columnindex(names, name), name)

function Base.getproperty(csvrow::Row{F}, ::Type{T}, col::Int, name::Symbol) where {T, F <: File{NT, transpose}} where {NT, transpose}
    col === 0 && return missing
    f = getfield(csvrow, 1)
    row = getfield(csvrow, 2)
    if transpose
        @inbounds Parsers.fastseek!(f.io, f.positions[col])
    else
        if f.currentrow[] != row
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            f.lastparsedcol[] = 0
        end
        lastparsed = f.lastparsedcol[]
        if col === lastparsed + 1
            if newline(f.lastparsedcode[])
                f.lastparsedcol[] = col
                return missing
            end
        elseif col > lastparsed + 1
            # skipping cells
            if newline(f.lastparsedcode[]) || !skipcells(f, col - (lastparsed + 1))
                f.lastparsedcol[] = col
                return missing
            end
        else
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            # randomly seeking within row
            if !skipcells(f, col - 1)
                f.lastparsedcol[] = col
                return missing
            end
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
Opens the file and uses passed arguments to detect the number of columns and column types.
The returned `CSV.File` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values.
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
  * `normalizenames=true`: whether column names should be "normalized" into valid Julia identifier symbols
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
  * `categorical::Bool=false`: whether columns with low cardinality (small number of unique values) should be read directly as a `CategoricalArray`
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with missing values
"""
function File(source::Union{String, IO};
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, UnitRange{Int}, Vector}=1,
    normalizenames::Bool=true,
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
    delim::Union{Char, String}=",",
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
    typemap::Dict=Dict{Type, Type}(),
    allowmissing::Symbol=:all,
    categorical::Bool=false,
    strict::Bool=false,
    debug::Bool=false)

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
                    x->Parsers.Delimited(x, d, "\n", "\r", "\r\n"; ignore_repeated=ignorerepeated)

    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    datarow = skipto !== nothing ? skipto : datarow

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rows, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, footerskip, normalizenames)
        originalpositions = copy(positions)
        ref = Ref{Int}(min(something(limit, typemax(Int)), rows))
    else
        names, datapos = datalayout(header, parsinglayers, io, datarow, normalizenames)
        eof(io) && return File{NamedTuple{names, Tuple{(Missing for _ in names)...}}, false, typeof(io), typeof(parsinglayers), typeof(kwargs)}(getname(source), io, parsinglayers, Int64[], Int64[], Ref{Int}(0), Ref{Int}(0), Ref(Parsers.SUCCESS), kwargs, CategoricalPool{String, UInt32, CatStr}[], strict)
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
    return File{NamedTuple{names, Tuple{types...}}, transpose, typeof(io), typeof(parsinglayers), typeof(kwargs)}(getname(source), io, parsinglayers, positions, originalpositions, Ref(1), ref, Ref(Parsers.SUCCESS), kwargs, pools, strict)
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
    # read a dummy file to "precompile" a bunch of methods; until the `precompile` function
    # can propertly store machine code, this is our best option
    CSV.File(joinpath(@__DIR__, "../test/testfiles/test_utf8.csv"), allowmissing=:auto) |> columntable
    return
end

end # module
