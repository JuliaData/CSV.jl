module CSV

# stdlib
using Mmap, Dates, Unicode
using Parsers, Tables
using PooledArrays, CategoricalArrays, WeakRefStrings, DataFrames

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
    names::Vector{Symbol}
    types::Vector{Type}
    rows::Int64
    cols::Int64
    e::UInt8
    categorical::Bool
    refs::Vector{Vector{String}}
    buf::Vector{UInt8}
    tapes::Vector{Vector{UInt64}}
end

getname(f::File) = getfield(f, :name)
getnames(f::File) = getfield(f, :names)
gettypes(f::File) = getfield(f, :types)
getrows(f::File) = getfield(f, :rows)
getcols(f::File) = getfield(f, :cols)
gete(f::File) = getfield(f, :e)
getcategorical(f::File) = getfield(f, :categorical)
function getrefs(f::File, col)
    @inbounds r = getfield(f, :refs)[col]
    return r
end
getbuf(f::File) = getfield(f, :buf)
function gettape(f::File, col)
    @inbounds t = getfield(f, :tapes)[col]
    return t
end

function Base.show(io::IO, f::File)
    println(io, "CSV.File(\"$(getname(f))\"):")
    println(io, "Size: $(getrows(f)) x $(getcols(f))")
    show(io, Tables.schema(f))
end

const EMPTY_POSITIONS = Int64[]
const EMPTY_TYPEMAP = Dict{TypeCode, TypeCode}()
const EMPTY_REFS = Vector{String}[]
const EMPTY_REFVALUES = String[]

"""
    CSV.File(source; kwargs...) => CSV.File

Read a csv input (a filename given as a String or FilePaths.jl type, or any other IO source), returning a `CSV.File` object.

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
df = CSV.File(file) |> DataFrame!

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
  * `ignoreemptylines::Bool=false`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)
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
  * `pool::Union{Bool, Float64}=0.1`: if `true`, *all* columns detected as `String` will be internally pooled; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `categorical::Bool=false`: whether pooled columns should be copied as CategoricalArray instead of PooledArray
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
    ignoreemptylines::Bool=false,
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
    file(source, header, normalizenames, datarow, skipto, footerskip,
        limit, transpose, comment, use_mmap, ignoreemptylines, missingstrings, missingstring,
        delim, ignorerepeated, quotechar, openquotechar, closequotechar,
        escapechar, dateformat, decimal, truestrings, falsestrings, type,
        types, typemap, categorical, pool, strict, silencewarnings, debug,
        parsingdebug, allowmissing)
end

function file(source,
    # file options
    # header can be a row number, range of rows, or actual string vector
    header=1,
    normalizenames=false,
    # by default, data starts immediately after header or start of file
    datarow=-1,
    skipto=nothing,
    footerskip=0,
    limit=typemax(Int64),
    transpose=false,
    comment=nothing,
    use_mmap=!Sys.iswindows(),
    ignoreemptylines=false,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim=nothing,
    ignorerepeated=false,
    quotechar='"',
    openquotechar=nothing,
    closequotechar=nothing,
    escapechar='"',
    dateformat=nothing,
    decimal=UInt8('.'),
    truestrings=nothing,
    falsestrings=nothing,
    # type options
    type=nothing,
    types=nothing,
    typemap=EMPTY_TYPEMAP,
    categorical=false,
    pool=0.1,
    strict=false,
    silencewarnings=false,
    debug=false,
    parsingdebug=false,
    allowmissing=nothing)

    # initial argument validation and adjustment
    !isa(source, IO) && !isa(source, Vector{UInt8}) && !isfile(source) && throw(ArgumentError("\"$source\" is not a valid file"))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    if type !== nothing && typecode(type) == EMPTY
        throw(ArgumentError("$type isn't supported in the `type` keyword argument; must be one of: `Int64`, `Float64`, `Date`, `DateTime`, `Bool`, `Missing`, `PooledString`, `CategoricalString{UInt32}`, or `String`"))
    elseif types !== nothing && any(x->typecode(x) == EMPTY, types isa AbstractDict ? values(types) : types)
        T = nothing
        for x in (types isa AbstractDict ? values(types) : types)
            if typecode(x) == EMPTY
                T = x
                break
            end
        end
        throw(ArgumentError("unsupported type $T in the `types` keyword argument; must be one of: `Int64`, `Float64`, `Date`, `DateTime`, `Bool`, `Missing`, `PooledString`, `CategoricalString{UInt32}`, or `String`"))
    end
    delim !== nothing && ((delim isa Char && iscntrl(delim) && delim != '\t') || (delim isa String && any(iscntrl, delim) && !all(==('\t'), delim))) && throw(ArgumentError("invalid delim argument = '$(escape_string(string(delim)))', must be a non-control character or string without control characters"))
    ignorerepeated && delim === nothing && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter via `delim=','`"))
    allowmissing !== nothing && @warn "`allowmissing` is a deprecated keyword argument"
    if !(categorical isa Bool)
        @warn "categorical=$categorical is deprecated in favor of `pool=$categorical`; categorical is only used to determine CategoricalArray vs. PooledArrays"
        pool = categorical
        categorical = categorical > 0.0
    elseif categorical === true
        pool = categorical
    end
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
    cmt = comment === nothing ? nothing : (pointer(comment), sizeof(comment))
    IG = Val(ignoreemptylines)
    rowsguess, del = guessnrows(buf, oq, cq, eq, source, delim, cmt, IG, debug)
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
        datapos = isempty(positions) ? 0 : positions[1]
    else
        positions = EMPTY_POSITIONS
        names, datapos = datalayout(header, buf, pos, len, options, datarow, normalizenames, cmt, IG)
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    # deduce initial column types for parsing based on whether any user-provided types were provided or not
    T = type === nothing ? EMPTY : (typecode(type) | USER)
    if types isa Vector
        typecodes = TypeCode[typecode(T) | USER for T in types]
        categorical = categorical | any(x->x == CategoricalString{UInt32}, types)
    elseif types isa AbstractDict
        typecodes = initialtypes(T, types, names)
        categorical = categorical | any(x->x == CategoricalString{UInt32}, values(types))
    else
        typecodes = TypeCode[T for _ = 1:length(names)]
    end
    debug && println("computed typecodes are: $typecodes")

    # we now do our parsing pass over the file, starting at datapos
    # we fill in our "tape", which has two UInt64 slots for each cell in row-major order (linearly indexed)
    # the 1st UInt64 is used for noting the byte position, len, and other metadata of the field within the file:
        # leftmost bit indicates a sentinel value was detected while parsing, resulting cell value will be `missing`
        # 2nd leftmost bit indicates a cell initially parsed as Int (used if column later gets promoted to Float64)
        # 3rd leftmost bit indicates if a field was quoted and included escape chararacters (will have to be unescaped later)
        # 45 bits for position (allows for maximum file size of 35TB)
        # 16 bits for field length (allows for maximum field size of 65K)
    # the 2nd UInt64 is used for storing the raw bits of a parsed, typed value: Int64, Float64, Date, DateTime, Bool, or categorical/pooled UInt32 ref
    ncols = length(names)
    tapelen = rowsguess * 2
    tapes = Vector{UInt64}[Mmap.mmap(Vector{UInt64}, tapelen) for i = 1:ncols]
    pool = pool === true ? 1.0 : pool isa Float64 ? pool : 0.0
    refs = Vector{Dict{String, UInt64}}(undef, ncols)
    lastrefs = zeros(UInt64, ncols)
    t = Base.time()
    rows, tapes = parsetape(Val(transpose), IG, ncols, gettypecodes(typemap), tapes, tapelen, buf, datapos, len, limit, cmt, positions, pool, refs, lastrefs, rowsguess, typecodes, debug, options)
    debug && println("time for initial parsing to tape: $(Base.time() - t)")
    for i = 1:ncols
        typecodes[i] &= ~USER
    end
    finaltypes = Type[TYPECODES[T] for T in typecodes]
    debug && println("types after parsing: $finaltypes, pool = $pool")
    finalrefs = Vector{Vector{String}}(undef, ncols)
    if pool > 0.0
        for i = 1:ncols
            if isassigned(refs, i)
                finalrefs[i] = map(x->x[1], sort!(collect(refs[i]), by=x->x[2]))
            elseif typecodes[i] == POOL || typecodes[i] == (POOL | MISSING)
                # case where user manually specified types, but no rows were parsed
                # so the refs never got initialized; initialize them here to empty
                finalrefs[i] = Vector{String}[]
            end
        end
    end
    return File(getname(source), names, finaltypes, rows - footerskip, ncols, eq, categorical, finalrefs, buf, tapes)
end

function parsetape(::Val{transpose}, ignoreemptylines, ncols, typemap, tapes, tapelen, buf, pos, len, limit, cmt, positions, pool, refs, lastrefs, rowsguess, typecodes, debug, options::Parsers.Options{ignorerepeated}) where {transpose, ignorerepeated}
    row = 0
    tapeidx = 1
    if pos <= len && len > 0
        while row < limit
            pos = consumecommentedline!(buf, pos, len, cmt, ignoreemptylines)
            if ignorerepeated
                pos = Parsers.checkdelim!(buf, pos, len, options)
            end
            pos > len && break
            row += 1
            for col = 1:ncols
                if transpose
                    @inbounds pos = positions[col]
                end
                @inbounds T = typecodes[col]
                @inbounds tape = tapes[col]
                type = typebits(T)
                if type === EMPTY
                    pos, code = detect(tape, tapeidx, buf, pos, len, options, row, col, typemap, pool, refs, lastrefs, debug, typecodes)
                elseif type === MISSINGTYPE
                    pos, code = detect(tape, tapeidx, buf, pos, len, options, row, col, typemap, pool, refs, lastrefs, debug, typecodes)
                elseif type === INT
                    pos, code = parseint!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === FLOAT
                    pos, code = parsevalue!(Float64, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === DATE
                    pos, code = parsevalue!(Date, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === DATETIME
                    pos, code = parsevalue!(DateTime, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === TIME
                    pos, code = parsevalue!(Time, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === BOOL
                    pos, code = parsevalue!(Bool, T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                elseif type === POOL
                    pos, code = parsepooled!(T, tape, tapeidx, buf, pos, len, options, row, col, rowsguess, pool, refs, lastrefs, typecodes)
                else # STRING
                    pos, code = parsestring!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
                end
                if transpose
                    @inbounds positions[col] = pos
                else
                    if col < ncols
                        if Parsers.newline(code) || pos > len
                            options.silencewarnings || notenoughcolumns(col, ncols, row)
                            for j = (col + 1):ncols
                                # put in dummy missing values on the tape for missing columns
                                @inbounds tape = tapes[j]
                                tape[tapeidx] = MISSING_BIT
                                T = typecodes[j]
                                if T > MISSINGTYPE
                                    typecodes[j] |= MISSING
                                end
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
            tapeidx += 2
            pos > len && break
            if tapeidx + 1 > tapelen
                debug && reallocatetape()
                oldtapes = tapes
                newtapelen = ceil(Int64, tapelen * 1.4)
                newtapes = Vector{UInt64}[Mmap.mmap(Vector{UInt64}, newtapelen) for i = 1:ncols]
                for i = 1:ncols
                    copyto!(newtapes[i], 1, oldtapes[i], 1, tapelen)
                end
                tapes = newtapes
                tapelen = newtapelen
                for i = 1:ncols
                    finalize(oldtapes[i])
                end
            end
        end
    end
    return row, tapes
end

@noinline reallocatetape() = println("warning: didn't pre-allocate enough tape while parsing, re-allocating...")
@noinline notenoughcolumns(cols, ncols, row) = println("warning: only found $cols / $ncols columns on data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = println("warning: parsed expected $cols columns, but didn't reach end of line on data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = println("warnings: error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("fatal error, encountered an invalidly quoted field while parsing on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))

const INTVALUE = Val(true)
const NONINTVALUE = Val(false)

@inline function setposlen!(tape, tapeidx, code, pos, len, ::Val{IntValue}=NONINTVALUE) where {IntValue}
    pos = Core.bitcast(UInt64, pos) << 16
    pos |= ifelse(Parsers.sentinel(code), MISSING_BIT, UInt64(0))
    pos |= ifelse(Parsers.escapedstring(code), ESCAPE_BIT, UInt64(0))
    if IntValue
        pos |= INT_BIT
    end
    @inbounds tape[tapeidx] = pos | Core.bitcast(UInt64, len)
    return
end

function detect(tape, tapeidx, buf, pos, len, options, row, col, typemap, pool, refs, lastrefs, debug, typecodes)
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        setposlen!(tape, tapeidx, code, vpos, vlen)
        @inbounds typecodes[col] = MISSINGTYPE
        @goto done
    end
    @inbounds T = typecodes[col]
    if Parsers.ok(code) && !haskey(typemap, INT)
        setposlen!(tape, tapeidx, code, vpos, vlen, INTVALUE)
        @inbounds tape[tapeidx + 1] = uint64(int)
        @inbounds typecodes[col] = T == MISSINGTYPE ? (INT | MISSING) : INT
        @goto done
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if Parsers.ok(code) && !haskey(typemap, FLOAT)
        setposlen!(tape, tapeidx, code, vpos, vlen)
        @inbounds tape[tapeidx + 1] = uint64(float)
        @inbounds typecodes[col] = T == MISSINGTYPE ? (FLOAT | MISSING) : FLOAT
        @goto done
    end
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, DATE)
                setposlen!(tape, tapeidx, code, vpos, vlen)
                @inbounds tape[tapeidx + 1] = uint64(date)
                @inbounds typecodes[col] = T == MISSINGTYPE ? (DATE | MISSING) : DATE
                @goto done
            end
        catch e
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, DATETIME)
                setposlen!(tape, tapeidx, code, vpos, vlen)
                @inbounds tape[tapeidx + 1] = uint64(datetime)
                @inbounds typecodes[col] = T == MISSINGTYPE ? (DATETIME | MISSING) : DATETIME
                @goto done
            end
        catch e
        end
    else
        # use user-provided dateformat
        DT = timetype(options.dateformat)
        dt, code, vpos, vlen, tlen = Parsers.xparse(DT, buf, pos, len, options)
        if Parsers.ok(code)
            setposlen!(tape, tapeidx, code, vpos, vlen)
            @inbounds tape[tapeidx + 1] = uint64(dt)
            @inbounds typecodes[col] = DT == Date ? (T == MISSINGTYPE ? (DATE | MISSING) : DATE) : DT == DateTime ? (T == MISSINGTYPE ? (DATETIME | MISSING) : DATETIME) : (T == MISSINGTYPE ? (TIME | MISSING) : TIME)
            @goto done
        end
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, options)
    if Parsers.ok(code) && !haskey(typemap, BOOL)
        setposlen!(tape, tapeidx, code, vpos, vlen)
        @inbounds tape[tapeidx + 1] = uint64(bool)
        @inbounds typecodes[col] = T == MISSINGTYPE ? (BOOL | MISSING) : BOOL
        @goto done
    end
    _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if pool > 0.0
        r = Dict{String, UInt64}()
        @inbounds refs[col] = r
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), lastrefs, col, code, options)
        @inbounds tape[tapeidx + 1] = ref
        @inbounds typecodes[col] = T == MISSINGTYPE ? (POOL | MISSING) : POOL
    else
        @inbounds typecodes[col] = T == MISSINGTYPE ? (STRING | MISSING) : STRING
    end
@label done
    return pos + tlen, code
end

@inline function parseint!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if code > 0
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
            fatalerror(buf, pos, tlen, code, row, col)
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
            if code > 0
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
    if code > 0
        if !Parsers.sentinel(code)
            @inbounds tape[tapeidx + 1] = uint64(x)
        else
            @inbounds typecodes[col] = T | MISSING
            @inbounds tape[tapeidx] = MISSING_BIT
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, row, col)
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

@inline function parsestring!(T, tape, tapeidx, buf, pos, len, options, row, col, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code)
        @inbounds typecodes[col] = STRING | MISSING
    end
    return pos + tlen, code
end

@inline function getref!(x::Dict, key::PointerString, lastrefs, col, code, options)
    index = Base.ht_keyindex2!(x, key)
    if index > 0
        @inbounds found_key = x.vals[index]
        return found_key::UInt64
    else
        @inbounds new = (lastrefs[col] += UInt64(1))
        @inbounds Base._setindex!(x, new, Parsers.escapedstring(code) ? unescape(key, options.e) : String(key), -index)
        return new
    end
end

function parsepooled!(T, tape, tapeidx, buf, pos, len, options, row, col, rowsguess, pool, refs, lastrefs, typecodes)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code)
        @inbounds typecodes[col] = T | MISSING
        ref = UInt64(0)
    else
        if !isassigned(refs, col)
            r = Dict{String, UInt64}()
            @inbounds refs[col] = r
        else
            @inbounds r = refs[col]
        end
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), lastrefs, col, code, options)
    end
    if !user(T) && isassigned(refs, col) && (length(refs[col]) / rowsguess) > pool
        @inbounds typecodes[col] = STRING | (missingtype(typecodes[col]) ? MISSING : EMPTY)
    else
        @inbounds tape[tapeidx + 1] = ref
    end
    return pos + tlen, code
end

include("tables.jl")
include("iteration.jl")
include("rows.jl")
include("write.jl")

"""
`CSV.read(source; copycols::Bool=false, kwargs...)` => `DataFrame`

Parses a delimited file into a `DataFrame`. `copycols` determines whether a copy of columns should be made when creating the DataFrame; by default, no copy is made, and the DataFrame is built with immutable, read-only `CSV.Column` vectors. If mutable operations are needed on the DataFrame columns, set `copycols=true`.

`CSV.read` supports the same keyword arguments as [`CSV.File`](@ref).
"""
read(source; copycols::Bool=false, kwargs...) = DataFrame(CSV.File(source; kwargs...), copycols=copycols)

function __init__()
    Threads.resize_nthreads!(VALUE_BUFFERS)
    return
end

end # module
