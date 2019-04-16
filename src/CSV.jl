module CSV

# stdlib
using Mmap, Dates, Random, Unicode
using Parsers, Tables, CategoricalArrays, PooledArrays, WeakRefStrings, DataFrames

function validate(fullpath::Union{AbstractString,IO}; kwargs...)
    Base.depwarn("`CSV.validate` is deprecated. `CSV.read` now prints warnings on misshapen files.", :validate)
    Tables.columns(File(fullpath; kwargs...))
    return
end

include("utils.jl")
include("filedetection.jl")

struct Error <: Exception
    error::Parsers.Error
    row::Int
    col::Int
end

function Base.showerror(io::IO, e::Error)
    println(io, "CSV.Error on row=$(e.row), column=$(e.col):")
    showerror(io, e.error)
end

struct File
    name::String
    io::IOBuffer
    names::Vector{Symbol}
    types::Vector{Type}
    typecodes::Vector{TypeCode}
    escapestrings::Vector{Bool}
    quotedstringtype
    refs::Vector{Dict{Union{Missing, String}, UInt32}}
    pool::Float64
    categorical::Bool
    categoricalpools::Vector{CategoricalPool{String, UInt32, CatStr}}
    rows::Int64
    cols::Int64
    tape::Vector{UInt64}
end

function Base.show(io::IO, f::File)
    println(io, "CSV.File(\"$(f.name)\"):")
    println(io, "Size: $(f.rows) x $(f.cols)")
    show(io, Tables.schema(f))
end

const EMPTY_POSITIONS = Int64[]
const EMPTY_TYPEMAP = Dict{TypeCode, TypeCode}()
const EMPTY_REFS = Dict{Union{Missing, String}, UInt32}[]
const EMPTY_LAST_REFS = UInt32[]
const EMPTY_CATEGORICAL_POOLS = CategoricalPool{String, UInt32, CatStr}[]

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
function File(source::Union{Vector{UInt8}, String, IO};
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Int=-1,
    skipto::Union{Nothing, Int}=nothing,
    footerskip::Int=0,
    limit::Int=typemax(Int64),
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
    categorical::Union{Bool, Real}=false,
    pool::Union{Bool, Real}=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false)

    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    delim !== nothing && ((delim isa Char && iscntrl(delim) && delim != '\t') || (delim isa String && any(iscntrl, delim) && !all(==('\t'), delim))) && throw(ArgumentError("invalid delim argument = '$(escape_string(string(delim)))', must be a non-control character or string without control characters"))
    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = skipto !== nothing ? skipto : (datarow == -1 ? (isa(header, Vector{Symbol}) || isa(header, Vector{String}) ? 0 : last(header)) + 1 : datarow) # by default, data starts on line after header

    debug && println("header is: $header, datarow computed as: $datarow")
    io = getio(source, use_mmap)
    consumeBOM!(io)

    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    cmt = comment === nothing ? nothing : Parsers.Trie(comment)
    rowsguess, d = guessnrows(io, oq, cq, eq, source, delim, cmt, debug)
    debug && println("estimated rows: $rowsguess")
    debug && println("detected delimiter: \"$(escape_string(d))\"")

    kwargs = getkwargs(dateformat, decimal, getbools(truestrings, falsestrings))
    whitespacedelim = d == " " || d == "\t"
    missingstrings = isempty(missingstrings) ? [missingstring] : missingstrings
    parsinglayers = Parsers.Sentinel(missingstrings) |>
                    x->Parsers.Strip(x, d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t') |>
                    (openquotechar !== nothing ? x->Parsers.Quoted(x, openquotechar, closequotechar, escapechar, !whitespacedelim) : x->Parsers.Quoted(x, quotechar, escapechar, !whitespacedelim)) |>
                    x->Parsers.Delimited(x, d; ignorerepeated=ignorerepeated, newline=true)
    quotedstringtype = WeakRefStrings.QuotedString{oq, cq, eq}

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rowsguess, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, normalizenames)
        datapos = positions[1]
    else
        positions = EMPTY_POSITIONS
        names, datapos = datalayout(header, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    catg = false
    T = type === nothing ? EMPTY : (typecode(type) | USER)
    if types isa Vector
        typecodes = TypeCode[typecode(T) | USER for T in types]
        catg = any(T->T <: CatStr, types)
    elseif types isa AbstractDict
        typecodes = initialtypes(T, types, names)
        catg = any(T->T <: CatStr, values(types))
    else
        typecodes = TypeCode[T for _ = 1:length(names)]
    end
    debug && println("computed typecodes are: $typecodes")

    # might as well round up to the next largest pagesize, since mmap will allocate it anyway
    ncols = length(names)
    tape = Mmap.mmap(Vector{UInt64}, roundup((rowsguess * ncols * 2) << 1, Mmap.PAGESIZE))
    escapestrings = fill(false, ncols)
    catg |= categorical === true || categorical isa Float64
    pool = Ref{Float64}(pool === true || categorical === true || any(pooled, typecodes) ? 1.0 :
                        pool isa Float64 ? pool :
                        categorical isa Float64 ? categorical : 0.0)
    refs = pool[] > 0.0 ? [Dict{Union{Missing, String}, UInt32}() for i = 1:ncols] : EMPTY_REFS
    lastrefs = pool[] > 0.0 ? [UInt32(0) for i = 1:ncols] : EMPTY_LAST_REFS
    t = time()
    rows = parsetape(Val(transpose), ncols, gettypecodes(typemap), tape, escapestrings, io, limit, parsinglayers, cmt, ignorerepeated, positions, pool, refs, lastrefs, rowsguess, typecodes, kwargs, strict, silencewarnings)
    debug && println("time for initial parsing to tape: $(time() - t)")
    foreach(1:ncols) do i
        typecodes[i] &= ~USER
    end
    types = [pooled(T) ? catg ? CatStr : PooledString : TYPECODES[T] for T in typecodes]
    debug && println("types after parsing: $types")
    if catg
        foreach(x->delete!(x, missing), refs)
        categoricalpools = [CategoricalPool(convert(Dict{String, UInt32}, r)) for r in refs]
        foreach(x->levels!(x, sort(levels(x))), categoricalpools)
    else
        categoricalpools = EMPTY_CATEGORICAL_POOLS
    end
    return File(getname(source), io, names, types, typecodes, escapestrings, quotedstringtype, refs, pool[], catg, categoricalpools, rows - footerskip, ncols, tape)
end

# using Parsers, Mmap
# transpose = false; ncols = 20; typemap = Dict{CSV.TypeCode, CSV.TypeCode}(); rowsguess = 1100000; escapestrings = fill(false, ncols); io = IOBuffer(Mmap.mmap(file)); limit = typemax(Int64); parsinglayers = Parsers.defaultparser; cmt = nothing; ignorerepeated = false; positions = Int64[]; pool = Ref(0.0); refs = Dict{Union{String, Missing}, UInt32}[]; lastrefs = UInt32[]; typecodes = Int8[]; kwargs = NamedTuple; strict = false; silencewarnings = false;
# tape = Mmap.mmap(Vector{UInt64}, CSV.roundup((rowsguess * ncols * 2) << 1, Mmap.PAGESIZE))
# @code_warntype debuginfo=:source CSV.parsetape(Val(transpose), ncols, CSV.gettypecodes(typemap), tape, escapestrings, io, limit, parsinglayers, cmt, ignorerepeated, positions, pool, refs, lastrefs, rowsguess, typecodes, kwargs, strict, silencewarnings)
function parsetape(::Val{transpose}, ncols, typemap, tape, escapestrings, io, limit, parsinglayers, cmt, ignorerepeated, positions, pool, refs, lastrefs, rowsguess, typecodes, kwargs, strict, silencewarnings) where {transpose}
    lastcode = Ref{Parsers.ReturnCode}()
    row = 0
    tapeidx = 1
    len = length(tape)
    if !eof(io)
        while row < limit
            row += 1
            consumecommentedline!(parsinglayers, io, cmt)
            ignorerepeated && Parsers.checkdelim!(parsinglayers, io)
            for i = 1:ncols
                if transpose
                    @inbounds Parsers.fastseek!(io, positions[i])
                end
                @inbounds T = typecodes[i]
                type = typebits(T)
                if type === EMPTY
                    nT = parseempty!(i, tape, tapeidx, io, parsinglayers, kwargs, typemap, lastcode, escapestrings, pool, refs, lastrefs)
                elseif type === MISSINGTYPE
                    nT = parsemissing!(i, tape, tapeidx, io, parsinglayers, kwargs, typemap, lastcode, escapestrings, pool, refs, lastrefs)
                elseif type === INT
                    nT = parseint!(tape, tapeidx, io, parsinglayers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, i)
                elseif type === FLOAT
                    nT = parsevalue!(Float64, tape, tapeidx, io, parsinglayers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, i)
                elseif type === DATE
                    nT = parsevalue!(Date, tape, tapeidx, io, parsinglayers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, i)
                elseif type === DATETIME
                    nT = parsevalue!(DateTime, tape, tapeidx, io, parsinglayers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, i)
                elseif type === BOOL
                    nT = parsevalue!(Bool, tape, tapeidx, io, parsinglayers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, i)
                elseif type === POOL
                    nT = parsepooled!(i, tape, tapeidx, io, parsinglayers, T, lastcode, rowsguess, pool, refs[i], lastrefs)
                else # STRING
                    nT = parsestring!(i, tape, tapeidx, io, parsinglayers, T, lastcode, escapestrings)
                end
                if nT !== T
                    typecodes[i] = nT
                end
                tapeidx += 2
                if transpose
                    @inbounds positions[i] = position(io)
                else
                    if i < ncols
                        if newline(lastcode[])
                            silencewarnings || notenoughcolumns(i, ncols, row)
                            for j = (i + 1):ncols
                                # put in dummy missing values on the tape for missing columns
                                tape[tapeidx] = MISSING_BIT
                                T = typecodes[j]
                                if T > MISSINGTYPE
                                    typecodes[j] |= MISSING
                                end
                                tapeidx += 2
                            end
                            break
                        end
                    else
                        if !eof(io) && !newline(lastcode[])
                            silencewarnings || toomanycolumns(ncols, row)
                            # ignore the rest of the line
                            readline!(parsinglayers, io)
                        end
                    end
                end
            end
            eof(io) && break
            if tapeidx > len
                println("WARNING: didn't pre-allocate enough while parsing: preallocated=$(row)")
                break
            end
        end
    end
    return row
end

@noinline notenoughcolumns(cols, ncols, row) = println("warning: only found $cols / $ncols columns on data row: $row. Filling remaining columns with `missing`...")
@noinline toomanycolumns(cols, row) = println("warning: parsed expected $cols columns, but didn't reach end of line on data row: $row. Ignoring any extra columns on this row...")
@noinline stricterror(io, r, row, col) = throw(Error(Parsers.Error(io, r), row, col))
@noinline warning(T, row, col, r) = println("warning: failed parsing $(TYPECODES[T & ~USER]) on row=$row, col=$col, error=$(Parsers.codes(r.code))")

@inline function setposlen!(tape, tapeidx, r)
    pos = sentinel(r.code) ? MISSING_BIT : (UInt64(r.pos) << 16)
    @inbounds tape[tapeidx] = pos | UInt64(r.len)
    return
end

function parseempty!(col, tape, tapeidx, io, layers, kwargs, typemap, lastcode, escapestrings, pool, refs, lastrefs)
    r = detecttype(io, layers, kwargs)
    T = typecode(r.result)
    T = get(typemap, T, T)
    @inbounds lastcode[] = r.code
    setposlen!(tape, tapeidx, r)
    if MISSINGTYPE < T < STRING
        @inbounds tape[tapeidx + 1] = uint64(r.result)
    elseif T === STRING
        if pool[] > 0.0
            T = POOL
            ref = getref!(refs[col], r.result, lastrefs, col)
            @inbounds tape[tapeidx + 1] = uint64(ref)
        else
            @inbounds escapestrings[col] |= escapestring(r.code)
        end
    end
    return T
end

function parsemissing!(col, tape, tapeidx, io, layers, kwargs, typemap, lastcode, escapestrings, pool, refs, lastrefs)
    r = detecttype(io, layers, kwargs)
    T = typecode(r.result)
    T = get(typemap, T, T)
    @inbounds lastcode[] = r.code
    setposlen!(tape, tapeidx, r)
    if MISSINGTYPE < T < STRING
        @inbounds tape[tapeidx + 1] = uint64(r.result)
    elseif T === STRING
        if pool[] > 0.0
            T = POOL
            ref = getref!(refs[col], r.result, lastrefs, col)
            @inbounds tape[tapeidx + 1] = uint64(ref)
        else
            @inbounds escapestrings[col] |= escapestring(r.code)
        end
    end
    return T === MISSINGTYPE ? T : T | MISSING
end

@inline function trytype(io, pos, layers, T, kwargs)
    Parsers.fastseek!(io, pos)
    res = Parsers.parse(layers, io, T; kwargs...)
    return res
end

function detecttype(io, layers, kwargs)
    pos = position(io)
    int = trytype(io, pos, layers, Int64, kwargs)
    Parsers.ok(int.code) && return int
    float = trytype(io, pos, layers, Float64, kwargs)
    Parsers.ok(float.code) && return float
    if !haskey(kwargs, :dateformat)
        try
            date = trytype(io, pos, layers, Date, kwargs)
            Parsers.ok(date.code) && return date
        catch e
        end
        try
            datetime = trytype(io, pos, layers, DateTime, kwargs)
            Parsers.ok(datetime.code) && return datetime
        catch e
        end
    else
        # use user-provided dateformat
        T = timetype(kwargs.dateformat)
        dt = trytype(io, pos, layers, T, kwargs)
        Parsers.ok(dt.code) && return dt
    end
    bool = trytype(io, pos, layers, Bool, kwargs)
    Parsers.ok(bool.code) && return bool
    return trytype(io, pos, layers, Tuple{Ptr{UInt8}, Int}, kwargs)
end

function parseint!(tape, tapeidx, io, layers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, col)
    r = Parsers.parse(layers, io, Int64)
    @inbounds lastcode[] = r.code
    if Parsers.ok(r.code)
        if r.result !== missing
            @inbounds tape[tapeidx + 1] = uint64(r.result)
        else
            T |= MISSING
        end
    else
        if user(T)
            if !strict
                r.code |= Parsers.SENTINEL
                silencewarnings || warning(Int64, row, col, r)
                T |= MISSING
                tape[tapeidx] = MISSING_BIT
            else
                stricterror(io, r, row, col)
            end
        else
            Parsers.fastseek!(io, r.pos - (quotedstring(r.code) ? 1 : 0))
            s = Parsers.parse(layers, io, Float64; kwargs...)
            if Parsers.ok(s.code)
                @inbounds tape[tapeidx + 1] = uint64(s.result)
                T = (T & ~INT) | FLOAT
            else
                T = (T & ~INT) | STRING
                @inbounds escapestrings[col] |= escapestring(s.code)
            end
        end
    end
    if !user(T)
        setposlen!(tape, tapeidx, r)
    end
    return T
end

function parsevalue!(::Type{type}, tape, tapeidx, io, layers, T, kwargs, strict, silencewarnings, lastcode, escapestrings, row, col) where {type}
    r = Parsers.parse(layers, io, type; kwargs...)
    @inbounds lastcode[] = r.code
    if Parsers.ok(r.code)
        if r.result !== missing
            tape[tapeidx + 1] = uint64(r.result)
        else
            T |= MISSING
        end
    else
        if user(T)
            if !strict
                r.code |= Parsers.SENTINEL
                silencewarnings || warning(T, row, col, r)
                T |= MISSING
                tape[tapeidx] = MISSING_BIT
            else
                stricterror(io, r, row, col)
            end
        else
            T = STRING | (missingtype(T) ? MISSING : EMPTY)
            @inbounds escapestrings[col] |= escapestring(r.code)
        end
    end
    if !user(T)
        setposlen!(tape, tapeidx, r)
    end
    return T
end

@inline function parsestring!(col, tape, tapeidx, io, layers, T, lastcode, escapestrings)
    r = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
    @inbounds lastcode[] = r.code
    setposlen!(tape, tapeidx, r)
    @inbounds escapestrings[col] |= escapestring(r.code)
    return T | (sentinel(r.code) ? MISSING : EMPTY)
end

@inline function getref!(x::Dict, key::Tuple{Ptr{UInt8}, Int}, lastrefs, col)
    index = Base.ht_keyindex2!(x, key)
    if index > 0
        @inbounds found_key = x.vals[index]
        return found_key::UInt32
    else
        @inbounds new = (lastrefs[col] += UInt32(1))
        @inbounds Base._setindex!(x, new, unsafe_string(key[1], key[2]), -index)
        return new
    end
end

function parsepooled!(col, tape, tapeidx, io, parsinglayers, T, lastcode, rowsguess, pool, refs, lastrefs)
    r = Parsers.parse(parsinglayers, io, Tuple{Ptr{UInt8}, Int})
    @inbounds lastcode[] = r.code
    setposlen!(tape, tapeidx, r)
    if sentinel(r.code)
        T |= MISSING
        ref = UInt32(0)
    else
        ref = getref!(refs, r.result, lastrefs, col)
    end
    if !user(T) && (length(refs) / rowsguess) > pool[]
        T = STRING | (missingtype(T) ? MISSING : EMPTY)
    else
        @inbounds tape[tapeidx + 1] = uint64(ref)
    end
    return T
end

include("tables.jl")
include("iteration.jl")
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
