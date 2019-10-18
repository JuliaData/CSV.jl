struct Rows{transpose, O}
    name::String
    names::Vector{Symbol}
    cols::Int64
    e::UInt8
    buf::Vector{UInt8}
    datapos::Int64
    limit::Int64
    options::O # Parsers.Options
    positions::Vector{Int64}
    cmt::Union{Tuple{Ptr{UInt8}, Int}, Nothing}
    ignoreemptylines::Bool
    reusebuffer::Bool
    tape::Vector{UInt64}
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
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Nothing, Char, String}=nothing,
    ignorerepeated::Bool=false,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='"',
    # type options
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    reusebuffer::Bool=false,
    kw...)

    # initial argument validation and adjustment
    checkvalidsource(source)
    checkvaliddelim(delim)
    ignorerepeated && delim === nothing && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter via `delim=','`"))
    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = skipto !== nothing ? skipto : (datarow == -1 ? (isa(header, Vector{Symbol}) || isa(header, Vector{String}) ? 0 : last(header)) + 1 : datarow) # by default, data starts on line after header
    debug && println("header is: $header, datarow computed as: $datarow")
    # getsource will turn any input into a `Vector{UInt8}`
    buf = getsource(source, use_mmap)
    len = length(buf)
    # skip over initial BOM character, if present
    pos = consumeBOM(buf)

    oq = something(openquotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    sentinel = ((isempty(missingstrings) && missingstring == "") || (length(missingstrings) == 1 && missingstrings[1] == "")) ? missing : isempty(missingstrings) ? [missingstring] : missingstrings
    
    if delim === nothing
        del = isa(source, AbstractString) && endswith(source, ".tsv") ? UInt8('\t') :
            isa(source, AbstractString) && endswith(source, ".wsv") ? UInt8(' ') :
            UInt8('\n')
    else
        del = (delim isa Char && isascii(delim)) ? delim % UInt8 :
            (sizeof(delim) == 1 && isascii(delim)) ? delim[1] % UInt8 : delim
    end
    cmt = comment === nothing ? nothing : (pointer(comment), sizeof(comment))

    if !transpose
        # step 1: detect the byte position where the column names start (headerpos)
        # and where the first data row starts (datapos)
        headerpos, datapos = detectheaderdatapos(buf, pos, len, oq, eq, cq, cmt, ignoreemptylines, header, datarow)
        debug && println("headerpos = $headerpos, datapos = $datapos")

        # step 2: detect delimiter (or use given) and detect number of (estimated) rows and columns
        d, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, del, cmt, ignoreemptylines)
        debug && println("estimated rows: $rowsguess")
        debug && println("detected delimiter: \"$(escape_string(d isa UInt8 ? string(Char(d)) : d))\"")

        # step 3: build Parsers.Options w/ parsing arguments
        wh1 = d == UInt(' ') ? 0x00 : UInt8(' ')
        wh2 = d == UInt8('\t') ? 0x00 : UInt8('\t')
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, UInt8('.'), nothing, nothing, nothing, ignorerepeated, true, parsingdebug, strict, silencewarnings)

        # step 4: generate or parse column names
        names = detectcolumnnames(buf, headerpos, datapos, len, options, header, normalizenames)
        ncols = length(names)
        positions = EMPTY_POSITIONS
    else
        # transpose
        d, rowsguess = detectdelimandguessrows(buf, pos, pos, len, oq, eq, cq, del, cmt, ignoreemptylines)
        wh1 = d == UInt(' ') ? 0x00 : UInt8(' ')
        wh2 = d == UInt8('\t') ? 0x00 : UInt8('\t')
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, UInt8('.'), nothing, nothing, nothing, ignorerepeated, true, parsingdebug, strict, silencewarnings)
        rowsguess, names, positions = detecttranspose(buf, pos, len, options, header, datarow, normalizenames)
        ncols = length(names)
        datapos = positions[1]
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")
    lookup = Dict(nm=>i for (i, nm) in enumerate(names))
    return Rows{transpose, typeof(options)}(getname(source), names, length(names), eq, buf, datapos, limit, options, positions, cmt, ignoreemptylines, reusebuffer, Vector{UInt64}(undef, length(names)), lookup)
end

Tables.rowaccess(::Type{<:Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = Tables.Schema(r.names, fill(Union{String, Missing}, r.cols))
Base.eltype(r::Rows) = Row2
Base.IteratorSize(::Type{<:Rows}) = Base.SizeUnknown()

getignorerepeated(p::Parsers.Options{ignorerepeated}) where {ignorerepeated} = ignorerepeated

@inline function Base.iterate(r::Rows{transpose}, (pos, len, row)=(r.datapos, length(r.buf), 1)) where {transpose}
    (pos > len || row > r.limit) && return nothing
    buf, positions, ncols, options = r.buf, r.positions, r.cols, r.options
    pos = checkcommentandemptyline(buf, pos, len, r.cmt, r.ignoreemptylines)
    getignorerepeated(options) && (pos = Parsers.checkdelim!(buf, pos, len, options))
    pos > len && return nothing
    tape = r.reusebuffer ? r.tape : Vector{UInt64}(undef, ncols)
    for col = 1:ncols
        if transpose
            @inbounds pos = positions[col]
        end
        x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
        setposlen!(tape, col, code, vpos, vlen)
        if Parsers.invalidquotedfield(code)
            fatalerror(buf, pos, tlen, code, row, col)
        end
        pos += tlen
        if transpose
            @inbounds positions[col] = pos
        else
            if col < ncols
                if Parsers.newline(code) || pos > len
                    options.silencewarnings || notenoughcolumns(col, ncols, row)
                    for j = (col + 1):ncols
                        tape[j] = MISSING_BIT
                    end
                    break
                end
            else
                if pos <= len && !Parsers.newline(code)
                    options.silencewarnings || toomanycolumns(ncols, row)
                    pos = skiptorow(buf, pos, len, options.oq, options.e, options.cq, 1, 2)
                end
            end
        end
    end
    return Row2(r.names, r.lookup, tape, r.buf, r.e, r.options), (pos, len, row + 1)
end

struct Row2{O} <: AbstractVector{Union{String, Missing}}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    tape::Vector{UInt64}
    buf::Vector{UInt8}
    e::UInt8
    options::O
end

getnames(r::Row2) = getfield(r, :names)
getlookup(r::Row2) = getfield(r, :lookup)
gettape(r::Row2) = getfield(r, :tape)
getbuf(r::Row2) = getfield(r, :buf)
gete(r::Row2) = getfield(r, :e)
getoptions(r::Row2) = getfield(r, :options)

Base.IndexStyle(::Type{Row2}) = Base.IndexLinear()
Base.size(r::Row2) = (length(getnames(r)),)

@inline Base.@propagate_inbounds function Base.getindex(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    @inbounds offlen = gettape(r)[i]
    missingvalue(offlen) && return missing
    s = PointerString(pointer(getbuf(r), getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, gete(r)) : String(s)
end

Base.propertynames(r::Row2) = getnames(r)

function Base.getproperty(r::Row2, nm::Symbol)
    @inbounds x = r[getlookup(r)[nm]]
    return x
end

@inline Base.@propagate_inbounds function Parsers.parse(::Type{T}, r::Row2, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    @inbounds offlen = gettape(r)[i]
    missingvalue(offlen) && return missing
    pos = getpos(offlen)
    x, code, vpos, vlen, tlen = Parsers.xparse(T, getbuf(r), pos, pos + getlen(offlen), getoptions(r))
    return Parsers.ok(code) ? x : missing
end

@inline Base.@propagate_inbounds function detect(r::Row2, i::Int)
    @boundscheck checkbounds(r, i)
    @inbounds offlen = gettape(r)[i]
    missingvalue(offlen) && return missing
    pos = getpos(offlen)
    x = detect(getbuf(r), pos, pos + getlen(offlen) - 1, getoptions(r))
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
