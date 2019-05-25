struct Rows
    name::String
    names::Vector{Symbol}
    cols::Int64
    e::UInt8
    buf::Vector{UInt8}
end

getname(f::Rows) = getfield(f, :name)
getnames(f::Rows) = getfield(f, :names)
getcols(f::Rows) = getfield(f, :cols)
gete(f::Rows) = getfield(f, :e)
getbuf(f::Rows) = getfield(f, :buf)

function Base.show(io::IO, f::Rows)
    println(io, "CSV.Rows(\"$(getname(f))\"):")
    println(io, "Size: $(getcols(f))")
    show(io, Tables.schema(f))
end

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
    parsingdebug::Bool=false)

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
    return File(getname(source), names, finaltypes, rows - footerskip, ncols, eq, categorical, finalrefs, buf, tapes)
end