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
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

struct File
    name::String
    buf::Vector{UInt8}
    names::Vector{Symbol}
    types::Vector{Type}
    typecodes::Vector{TypeCode}
    escapedstringtype
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
    CSV.File(source; kwargs...) => CSV.File

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
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.)
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
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
  * `categorical::Union{Bool, Float64}=false`: if `true`, columns detected as `String` are returned as a `CategoricalArray`; alternatively, the proportion of unique values below which `String` columns should be treated as categorical (for example 0.1 for 10%, which means if the # of unique strings in a column is under 10%, it will be parsed as a CategoricalArray)
  * `pool::Union{Bool, Float64}=0.1`: if `true`, columns detected as `String` are returned as a `PooledArray`; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
"""
function File(source;
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
    decimal::Union{UInt8, Char}=UInt8('.'),
    truestrings::Union{Vector{String}, Nothing}=nothing,
    falsestrings::Union{Vector{String}, Nothing}=nothing,
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=EMPTY_TYPEMAP,
    categorical::Union{Bool, Real}=false,
    pool::Union{Bool, Real}=0.1,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    allowmissing::Union{Nothing, Symbol}=nothing)

    # initial argument validation and adjustment
    !isa(source, IO) && !isa(source, Vector{UInt8}) && !isfile(source) && throw(ArgumentError("\"$source\" is not a valid file"))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    delim !== nothing && ((delim isa Char && iscntrl(delim) && delim != '\t') || (delim isa String && any(iscntrl, delim) && !all(==('\t'), delim))) && throw(ArgumentError("invalid delim argument = '$(escape_string(string(delim)))', must be a non-control character or string without control characters"))
    allowmissing !== nothing && @warn "`allowmissing` is a deprecated keyword argument"
    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = skipto !== nothing ? skipto : (datarow == -1 ? (isa(header, Vector{Symbol}) || isa(header, Vector{String}) ? 0 : last(header)) + 1 : datarow) # by default, data starts on line after header
    debug && println("header is: $header, datarow computed as: $datarow")
    # getsource will turn any input into a `Vector{UInt8}`
    buf = getsource(source, use_mmap)
    len = length(buf)
    # skip over initial BOM character, if present
    pos = consumeBOM!(buf)

    # we call guessnrows upfront to simultaneously guess how many rows are in a file (based on average # of bytes in first 10 rows), and to figure out our delimiter: if provided, we use that, otherwise, we auto-detect based on filename or detected common delimiters in first 10 rows
    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    escapedstringtype = WeakRefStrings.EscapedString{eq}
    cmt = comment === nothing ? nothing : (pointer(comment), sizeof(comment))
    rowsguess, del = guessnrows(buf, oq, cq, eq, source, delim, cmt, debug)
    debug && println("estimated rows: $rowsguess")
    debug && println("detected delimiter: \"$(escape_string(del isa UInt8 ? string(Char(del)) : del))\"")

    # build Parsers.Options w/ parsing arguments
    wh1 = del == UInt(' ') || delim == " " ? 0x00 : UInt8(' ')
    wh2 = del == UInt8('\t') || delim == "\t" ? 0x00 : UInt8('\t')
    trues = truestrings === nothing ? nothing : truestrings
    falses = falsestrings === nothing ? nothing : falsestrings
    sentinel = ((isempty(missingstrings) && missingstring == "") || (length(missingstrings) == 1 && missingstrings[1] == "")) ? missing : isempty(missingstrings) ? [missingstring] : missingstrings
    options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, del, decimal, trues, falses, dateformat, ignorerepeated, true, parsingdebug, strict, silencewarnings)

    # determine column names and where the data starts in the file; for transpose, we note the starting byte position of each column
    if transpose
        rowsguess, names, positions = datalayout_transpose(header, buf, pos, len, options, datarow, normalizenames)
        datapos = positions[1]
    else
        positions = EMPTY_POSITIONS
        names, datapos = datalayout(header, buf, pos, len, options, datarow, normalizenames, cmt, ignorerepeated)
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    # deduce initial column types for parsing based on whether any user-provided types were provided or not
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

    # we now do our parsing pass over the file, starting at datapos
    # we fill in our "tape", which has two UInt64 slots for each cell in row-major order (linearly indexed)
    # the 1st UInt64 is used for noting the byte position, len, and other metadata of the field within the file:
        # top bit indicates a missing value
        # 2nd bit indicates a cell initially parsed as Int (used if column later gets promoted to Float64)
        # 3rd bit indicates if a field was quoted and included escape chararacters (will have to be unescaped later)
        # 45 bits for position (allows for maximum file of 35TB)
        # 16 bits for field length (allows for maximum field size of 65K)
    # the 2nd UInt64 is used for storing the raw bits of a parsed, typed value: Int64, Float64, Date, DateTime, Bool, or categorical/pooled UInt32 ref
    ncols = length(names)
    # might as well round up to the next largest pagesize, since mmap aligns to it anyway
    tape = Mmap.mmap(Vector{UInt64}, roundup((rowsguess * ncols * 2), Mmap.PAGESIZE))
    catg |= categorical === true || categorical isa Float64
    pool = (pool === true || categorical === true || any(pooled, typecodes)) ? 1.0 :
            categorical isa Float64 ? categorical : pool isa Float64 ? pool : 0.0
    refs = pool > 0.0 ? [Dict{Union{Missing, String}, UInt32}() for i = 1:ncols] : EMPTY_REFS
    lastrefs = pool > 0.0 ? [UInt32(0) for i = 1:ncols] : EMPTY_LAST_REFS
    t = time()
    rows = parsetape(Val(transpose), ncols, gettypecodes(typemap), tape, buf, datapos, len, limit, cmt, positions, pool, refs, lastrefs, rowsguess, typecodes, debug, options)
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
    return File(getname(source), buf, names, types, typecodes, escapedstringtype, refs, pool, catg, categoricalpools, rows - footerskip, ncols, tape)
end

function parsetape(::Val{transpose}, ncols, typemap, tape, buf, pos, len, limit, cmt, positions, pool, refs, lastrefs, rowsguess, typecodes, debug, options::Parsers.Options{ignorerepeated}) where {transpose, ignorerepeated}
    row = 0
    tapeidx = 1
    tapelen = length(tape)
    if pos <= len
        while row < limit
            row += 1
            pos = consumecommentedline!(buf, pos, len, cmt)
            if ignorerepeated
                pos = Parsers.checkdelim!(buf, pos, len, options)
            end
            for col = 1:ncols
                if transpose
                    @inbounds pos = positions[col]
                end
                @inbounds T = typecodes[col]
                type = typebits(T)
                if type === EMPTY
                    pos, code = parseempty!(tape, tapeidx, buf, pos, len, options, col, typemap, pool, refs, lastrefs, debug, typecodes)
                elseif type === MISSINGTYPE
                    pos, code = parsemissing!(tape, tapeidx, buf, pos, len, options, col, typemap, pool, refs, lastrefs, debug, typecodes)
                elseif type === INT
                    pos, code = parseint!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === FLOAT
                    pos, code = parsevalue!(Float64, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === DATE
                    pos, code = parsevalue!(Date, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === DATETIME
                    pos, code = parsevalue!(DateTime, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === BOOL
                    pos, code = parsevalue!(Bool, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === POOL
                    pos, code = parsepooled!(T, tape, tapeidx, buf, pos, len, options, col, rowsguess, pool, refs[col], lastrefs, typecodes)
                else # STRING
                    pos, code = parsestring!(T, tape, tapeidx, buf, pos, len, options, col, typecodes)
                end
                tapeidx += 2
                if transpose
                    @inbounds positions[col] = pos
                else
                    if col < ncols
                        if Parsers.newline(code)
                            options.silencewarnings || notenoughcolumns(col, ncols, row)
                            for j = (col + 1):ncols
                                # put in dummy missing values on the tape for missing columns
                                tape[tapeidx] = MISSING_BIT
                                T = typecodes[j]
                                if T > MISSINGTYPE
                                    typecodes[j] |= MISSING
                                end
                                tapeidx += 2
                            end
                            break # from for col = 1:ncols
                        end
                    else
                        if pos <= len && !Parsers.newline(code)
                            options.silencewarnings || toomanycolumns(ncols, row)
                            # ignore the rest of the line
                            pos = readline!(buf, pos, len, options)
                        end
                    end
                end
            end
            pos > len && break
            if tapeidx > tapelen
                println("WARNING: didn't pre-allocate enough while parsing: preallocated=$(row)")
                break
            end
        end
    end
    return row
end

@noinline notenoughcolumns(cols, ncols, row) = println("warning: only found $cols / $ncols columns on data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = println("warning: parsed expected $cols columns, but didn't reach end of line on data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = println("warnings: error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("fatal error, encountered an invalidly quoted field while parsing on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))

const INTVALUE = Val(true)
const NONINTVALUE = Val(false)

@inline function setposlen!(tape, tapeidx, code, pos, len, ::Val{IntValue}=NONINTVALUE) where {IntValue}
    pos <<= 16
    if Parsers.sentinel(code)
        pos |= MISSING_BIT
    end
    if Parsers.escapedstring(code)
        pos |= ESCAPE_BIT
    end
    if IntValue
        pos |= INT_BIT
    end
    @inbounds tape[tapeidx] = Core.bitcast(UInt64, pos) | Core.bitcast(UInt64, len)
    return
end

function parseempty!(tape, tapeidx, buf, pos, len, options, col, typemap, pool, refs, lastrefs, debug, typecodes)
    x, code, vpos, vlen, tlen = detecttype(buf, pos, len, options, debug)
    T = Parsers.sentinel(code) ? MISSINGTYPE : typecode(x)
    T = get(typemap, T, T)
    if T == INT
        setposlen!(tape, tapeidx, code, vpos, vlen, INTVALUE)
        @inbounds tape[tapeidx + 1] = uint64(x)
    else
        setposlen!(tape, tapeidx, code, vpos, vlen)
        if MISSINGTYPE < T < STRING
            @inbounds tape[tapeidx + 1] = uint64(x)
        elseif T === STRING
            if pool > 0.0
                T = POOL
                ref = getref!(refs[col], WeakRefString(pointer(buf, vpos), vlen), lastrefs, col)
                @inbounds tape[tapeidx + 1] = uint64(ref)
            end
        end
    end
    @inbounds typecodes[col] = T
    return pos + tlen, code
end

function parsemissing!(tape, tapeidx, buf, pos, len, options, col, typemap, pool, refs, lastrefs, debug, typecodes)
    x, code, vpos, vlen, tlen = detecttype(buf, pos, len, options, debug)
    T = Parsers.sentinel(code) ? MISSINGTYPE : typecode(x)
    T = get(typemap, T, T)
    if T == INT
        setposlen!(tape, tapeidx, code, vpos, vlen, INTVALUE)
        @inbounds tape[tapeidx + 1] = uint64(x)
    else
        setposlen!(tape, tapeidx, code, vpos, vlen)
        if MISSINGTYPE < T < STRING
            @inbounds tape[tapeidx + 1] = uint64(x)
        elseif T === STRING
            if pool > 0.0
                T = POOL
                ref = getref!(refs[col], WeakRefString(pointer(buf, vpos), vlen), lastrefs, col)
                @inbounds tape[tapeidx + 1] = uint64(ref)
            end
        end
    end
    @inbounds typecodes[col] = T === MISSINGTYPE ? T : T | MISSING
    return pos + tlen, code
end

function detecttype(buf, pos, len, options, debug)
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if debug
        println("type detection on: \"$(escape_string(unsafe_string(pointer(buf, pos), tlen)))\"")
        println("attempted Int: $(Parsers.codes(code))")
    end
    Parsers.ok(code) && return int, code, vpos, vlen, tlen
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if debug
        println("attempted Float64: $(Parsers.codes(code))")
    end
    Parsers.ok(code) && return float, code, vpos, vlen, tlen
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if debug
                println("attempted Date: $(Parsers.codes(code))")
            end 
            Parsers.ok(code) && return date, code, vpos, vlen, tlen
        catch e
            # @error exception=(e, stacktrace(catch_backtrace()))
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, options)
            if debug
                println("attempted DateTime: $(Parsers.codes(code))")
            end
            Parsers.ok(code) && return datetime, code, vpos, vlen, tlen
        catch e
            # @error exception=(e, stacktrace(catch_backtrace()))
        end
    else
        # use user-provided dateformat
        T = timetype(options.dateformat)
        dt, code, vpos, vlen, tlen = Parsers.xparse(T, buf, pos, len, options)
        if debug
            println("attempted $T: $(Parsers.codes(code))")
        end
        Parsers.ok(code) && return dt, code, vpos, vlen, tlen
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, options)
    if debug
        println("attempted Bool: $(Parsers.codes(code))")
    end
    Parsers.ok(code) && return bool, code, vpos, vlen, tlen
    str, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    return "", code, vpos, vlen, tlen
end

@inline function parseint!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.succeeded(code)
        if !Parsers.sentinel(code)
            @inbounds tape[tapeidx + 1] = uint64(x)
            if !user(T)
                setposlen!(tape, tapeidx, code, vpos, vlen, INTVALUE)
            end
        else
            @inbounds typecodes[col] = INT | MISSING
            @inbounds tape[tapeidx] = MISSING_BIT
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, len, code, row, col)
        end
        if user(T)
            if !options.strict
                options.silencewarnings || warning(Int64, buf, pos, tlen, code, row, col)
                @inbounds typecodes[col] = INT | MISSING
                @inbounds tape[tapeidx] = MISSING_BIT
            else
                stricterror(Int64, buf, pos, tlen, code, row, col)
            end
        else
            y, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
            if Parsers.succeeded(code)
                @inbounds tape[tapeidx + 1] = uint64(y)
                @inbounds typecodes[col] = FLOAT | (missingtype(T) ? MISSING : EMPTY)
            else
                _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
                @inbounds typecodes[col] = STRING | (missingtype(T) ? MISSING : EMPTY)
            end
            setposlen!(tape, tapeidx, code, vpos, vlen)
        end
    end
    return pos + tlen, code
end

function parsevalue!(::Type{type}, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes) where {type}
    x, code, vpos, vlen, tlen = Parsers.xparse(type, buf, pos, len, options)
    if Parsers.succeeded(code)
        if !Parsers.sentinel(code)
            @inbounds tape[tapeidx + 1] = uint64(x)
        else
            @inbounds typecodes[col] = T | MISSING
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, len, code, row, col)
        end
        if user(T)
            if !options.strict
                code |= Parsers.SENTINEL
                options.silencewarnings || warning(type, buf, pos, tlen, code, row, col)
                @inbounds typecodes[col] = T | MISSING
                @inbounds tape[tapeidx] = MISSING_BIT
            else
                stricterror(type, buf, pos, tlen, code, row, col)
            end
        else
            _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
            @inbounds typecodes[col] = STRING | (missingtype(T) ? MISSING : EMPTY)
        end
    end
    if !user(T)
        setposlen!(tape, tapeidx, code, vpos, vlen)
    end
    return pos + tlen, code
end

@inline function parsestring!(T, tape, tapeidx, buf, pos, len, options, col, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if Parsers.sentinel(code)
        @inbounds typecodes[col] = STRING | MISSING
    end
    return pos + tlen, code
end

@inline function getref!(x::Dict, key::WeakRefString, lastrefs, col)
    index = Base.ht_keyindex2!(x, key)
    if index > 0
        @inbounds found_key = x.vals[index]
        return found_key::UInt32
    else
        @inbounds new = (lastrefs[col] += UInt32(1))
        @inbounds Base._setindex!(x, new, String(key), -index)
        return new
    end
end

function parsepooled!(T, tape, tapeidx, buf, pos, len, options, col, rowsguess, pool, refs, lastrefs, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if Parsers.sentinel(code)
        @inbounds typecodes[col] = T | MISSING
        ref = UInt32(0)
    else
        ref = getref!(refs, WeakRefString(pointer(buf, vpos), vlen), lastrefs, col)
    end
    if !user(T) && (length(refs) / rowsguess) > pool
        @inbounds typecodes[col] = STRING | (missingtype(typecodes[col]) ? MISSING : EMPTY)
    else
        @inbounds tape[tapeidx + 1] = uint64(ref)
    end
    return pos + tlen, code
end

include("tables.jl")
include("iteration.jl")
include("write.jl")

"""
`CSV.read(source; kwargs...)` => `DataFrame`

Parses a delimited file into a `DataFrame`.

Positional arguments:

* `source`: can be a file name (String or FilePaths.jl type) of the location of the csv file or an `IO` or `Vector{UInt8}` buffer to read the csv from directly

Supported keyword arguments include:
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.)
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
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
  * `categorical::Union{Bool, Float64}=false`: if `true`, columns detected as `String` are returned as a `CategoricalArray`; alternatively, the proportion of unique values below which `String` columns should be treated as categorical (for example 0.1 for 10%, which means if the # of unique strings in a column is under 10%, it will be parsed as a CategoricalArray)
  * `pool::Union{Bool, Float64}=0.1`: if `true`, columns detected as `String` are returned as a `PooledArray`; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
"""
read(source; kwargs...) = CSV.File(source; kwargs...) |> DataFrame!

function __init__()
    Threads.resize_nthreads!(VALUE_BUFFERS)
    return
end

end # module
