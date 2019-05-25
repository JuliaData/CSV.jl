struct View
    name::String
    names::Vector{Symbol}
    rows::Int64
    cols::Int64
    e::UInt8
    buf::Vector{UInt8}
    tapes::Vector{Vector{UInt64}}
    anymissing::Vector{Bool}
end

getname(f::View) = getfield(f, :name)
getnames(f::View) = getfield(f, :names)
getrows(f::View) = getfield(f, :rows)
getcols(f::View) = getfield(f, :cols)
gete(f::View) = getfield(f, :e)
function getrefs(f::View, col)
    @inbounds r = getfield(f, :refs)[col]
    return r
end
getbuf(f::View) = getfield(f, :buf)
function gettape(f::View, col)
    @inbounds t = getfield(f, :tapes)[col]
    return t
end
function getanymissing(f::View, col)
    @inbounds t = getfield(f, :anymissing)[col]
    return t
end

function Base.show(io::IO, f::View)
    println(io, "CSV.View(\"$(getname(f))\"):")
    println(io, "Size: $(getrows(f)) x $(getcols(f))")
    show(io, Tables.schema(f))
end

Tables.istable(::Type{<:View}) = true
Tables.columnaccess(::Type{<:View}) = true
Tables.schema(f::View)  = Tables.Schema(getnames(f), [getanymissing(f, i) ? Union{String, Missing} : String for i = 1:getcols(f)])
Tables.columns(f::View) = f
Base.propertynames(f::View) = getnames(f)

struct ColumnView{T} <: AbstractVector{T}
    view::View
    col::Int
end

function ColumnView(f::View, i::Int)
    @inbounds any = getanymissing(f, i)
    return ColumnView{any ? Union{String, Missing} : String}(f, i)
end

Base.size(c::ColumnView) = (Int(getrows(c.view)),)
Base.IndexStyle(::Type{<:ColumnView}) = Base.IndexLinear()

function Base.copy(c::ColumnView{T}) where {T <: Union{String, Union{String, Missing}}}
    len = length(c)
    A = StringVector{T}(undef, len)
    @simd for i = 1:len
        @inbounds A[i] = c[i]
    end
    return A
end

@inline Base.@propagate_inbounds function Base.getindex(c::ColumnView{String}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.view, c.col)[row]
    s = PointerString(pointer(getbuf(c.view), getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, gete(c.view)) : String(s)
end

@inline Base.@propagate_inbounds function Base.getindex(c::ColumnView{Union{String, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.view, c.col)[row]
    if missingvalue(offlen)
        return missing
    else
        s = PointerString(pointer(getbuf(c.view), getpos(offlen)), getlen(offlen))
        return escapedvalue(offlen) ? unescape(s, gete(c.view)) : String(s)
    end
end

function Base.getproperty(f::View, col::Symbol)
    i = findfirst(==(col), getnames(f))
    i === nothing && return getfield(f, col)
    return ColumnView(f, i)
end

Tables.rowaccess(::Type{<:View}) = true
Tables.rows(f::View) = f

struct RowView
    view::View
    row::Int
end

getview(r::RowView) = getfield(r, :view)
getrow(r::RowView) = getfield(r, :row)

Base.propertynames(r::RowView) = getnames(getview(r))

function Base.show(io::IO, r::RowView)
    println(io, "CSV.RowView($(getrow(r))) of:")
    show(io, getview(r))
end

Base.eltype(f::View) = RowView
Base.length(f::View) = getrows(f)

@inline function Base.iterate(f::View, st=1)
    st > length(f) && return nothing
    return RowView(f, st), st + 1
end

@inline function Base.getproperty(row::RowView, name::Symbol)
    f = getview(row)
    i = findfirst(x->x===name, getnames(f))
    i === nothing && badcolumnerror(name)
    return getcell(f, i, getrow(row))
end

@inline Base.getproperty(row::RowView, ::Type{T}, col::Int, name::Symbol) where {T} =
    getcell(getview(row), col, getrow(row))

@inline function getcell(f::View, col::Int, row::Int)
    @inbounds offlen = gettape(f, col)[row]
    missingvalue(offlen) && return missing
    s = PointerString(pointer(getbuf(f), getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, gete(f)) : String(s)
end

function View(source;
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
    # type options
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    kw...)

        # initial argument validation and adjustment
    !isa(source, IO) && !isa(source, Vector{UInt8}) && !isfile(source) && throw(ArgumentError("\"$source\" is not a valid file"))
    delim !== nothing && ((delim isa Char && iscntrl(delim) && delim != '\t') || (delim isa String && any(iscntrl, delim) && !all(==('\t'), delim))) && throw(ArgumentError("invalid delim argument = '$(escape_string(string(delim)))', must be a non-control character or string without control characters"))
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
    rowsguess, del = guessnrows(buf, oq, cq, eq, source, delim, cmt, debug)
    debug && println("estimated rows: $rowsguess")
    debug && println("detected delimiter: \"$(escape_string(del isa UInt8 ? string(Char(del)) : del))\"")

    # build Parsers.Options w/ parsing arguments
    wh1 = del == UInt(' ') || delim == " " ? 0x00 : UInt8(' ')
    wh2 = del == UInt8('\t') || delim == "\t" ? 0x00 : UInt8('\t')
    trues = falses = nothing
    sentinel = ((isempty(missingstrings) && missingstring == "") || (length(missingstrings) == 1 && missingstrings[1] == "")) ? missing : isempty(missingstrings) ? [missingstring] : missingstrings
    options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, del, UInt8('.'), trues, falses, nothing, ignorerepeated, true, parsingdebug, strict, silencewarnings)

    # determine column names and where the data starts in the file; for transpose, we note the starting byte position of each column
    if transpose
        rowsguess, names, positions = datalayout_transpose(header, buf, pos, len, options, datarow, normalizenames)
        datapos = isempty(positions) ? 0 : positions[1]
    else
        positions = EMPTY_POSITIONS
        names, datapos = datalayout(header, buf, pos, len, options, datarow, normalizenames, cmt)
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

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
    tapelen = rowsguess
    tapes = Vector{UInt64}[Mmap.mmap(Vector{UInt64}, tapelen) for i = 1:ncols]
    anymissing = fill(false, ncols)
    t = time()
    rows, tapes = parsetape(Val(transpose), ncols, tapes, tapelen, buf, datapos, len, limit, cmt, positions, rowsguess, debug, options, anymissing)
    debug && println("time for initial parsing to tape: $(time() - t)")
    return View(getname(source), names, rows - footerskip, ncols, eq, buf, tapes, anymissing)
end

function parsetape(::Val{transpose}, ncols, tapes, tapelen, buf, pos, len, limit, cmt, positions, rowsguess, debug, options::Parsers.Options{ignorerepeated}, anymissing) where {transpose, ignorerepeated}
    row = 0
    tapeidx = 1
    if pos <= len && len > 0
        while row < limit
            pos = consumecommentedline!(buf, pos, len, cmt)
            if ignorerepeated
                pos = Parsers.checkdelim!(buf, pos, len, options)
            end
            pos > len && break
            row += 1
            for col = 1:ncols
                if transpose
                    @inbounds pos = positions[col]
                end
                @inbounds tape = tapes[col]
                pos, code = parseviewstring!(tape, tapeidx, buf, pos, len, options, row, col, anymissing)
                if transpose
                    @inbounds positions[col] = pos
                else
                    if col < ncols
                        if Parsers.newline(code)
                            options.silencewarnings || notenoughcolumns(col, ncols, row)
                            for j = (col + 1):ncols
                                # put in dummy missing values on the tape for missing columns
                                @inbounds tape = tapes[j]
                                tape[tapeidx] = MISSING_BIT
                                @inbounds anymissing[col] = true
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
            tapeidx += 1
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

@inline function parseviewstring!(tape, tapeidx, buf, pos, len, options, row, col, anymissing)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, tapeidx, code, vpos, vlen)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code)
        @inbounds anymissing[col] = true
    end
    return pos + tlen, code
end