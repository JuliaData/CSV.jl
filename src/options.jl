function Options(;
        delim=COMMA,
        quotechar=QUOTE,
        escapechar=ESCAPE,
        null::AbstractString="",

        header::Union{Integer, UnitRange{Int}, AbstractVector}=1, # header can be a row number, range of rows, or actual string vector
        datarow::Int=-1, # by default, data starts immediately after header or start of file
        types=Type[],
        nullable::Union{Bool, Missing}=missing,
        dateformat=nothing,
        decimal=PERIOD,
        truestring="true",
        falsestring="false",
        categorical::Bool=true,
        weakrefstrings::Bool=true,#false,

        rows::Int=0,
        rows_for_type_detect::Int=20,
        footerskip::Int=0,
        use_mmap::Bool=true)
    # argument checks and preprocessing

    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))

    headerlast = isa(header, Integer) ? header : (isa(header, Range) ? maximum(header) : 0)
    if datarow == -1
        # by default, data starts on the line after the header
        datarow = headerlast + 1
    else
        if isa(header, Integer) && header == datarow == 1
            # default header=1 is overridden by user-specified datarow==1
            header = -1 # no header
        else
            (datarow > headerlast) || throw(ArgumentError("data row ($datarow) must come after the header row ($headerlast)"))
        end
    end

    return Options(typeof(delim) <: String ? UInt8(first(delim)) : (delim % UInt8),
                   typeof(quotechar) <: String ? UInt8(first(quotechar)) : (quotechar % UInt8),
                   typeof(escapechar) <: String ? UInt8(first(escapechar)) : (escapechar % UInt8),
                   map(UInt8, collect(ascii(String(null)))), null != "",
                   isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat,
                   decimal % UInt8,
                   map(UInt8, collect(truestring)),
                   map(UInt8, collect(falsestring)),
                   datarow,
                   rows, rows_for_type_detect, footerskip,
                   header,
                   types,
                   categorical,
                   nullable,
                   weakrefstrings,
                   use_mmap,
    )
end

function Base.show(io::IO, op::Options)
    println(io, "    CSV.Options:")
    println(io, "        delim: '", Char(op.delim), "'")
    println(io, "        quotechar: '", Char(op.quotechar), "'")
    print(io,   "        escapechar: '"); escape_string(io, string(Char(op.escapechar)), "\\"); println(io, "'")
    print(io,   "        null: \""); escape_string(io, isempty(op.null) ? "" : String(collect(op.null)), "\\"); println(io, "\"")
    println(io, "        dateformat: ", op.dateformat)
    println(io, "        decimal: '", Char(op.decimal), "'")
    println(io, "        truestring: '", String(op.truestring), "'")
    print(io,   "        falsestring: '", String(op.falsestring), "'")
end
