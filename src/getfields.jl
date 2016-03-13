# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a null field
# also return `b` which is last byte read
@inline function checknullstart(io::Union{IOBuffer,UnsafeBuffer},opt::CSV.Options)
    eof(io) && return 0x00, true
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar
        eof(io) && return b, true
        b = read(io, UInt8)
    end
    if b == opt.delim || b == NEWLINE # check for empty field
        return b, true
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return b, true
    end
    return b, false
end
# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
@inline function checkdone(io::Union{IOBuffer,UnsafeBuffer},b::UInt8,opt::CSV.Options)
    b == opt.quotechar && !eof(io) && (b = read(io, UInt8))
    if b == opt.delim || b == NEWLINE
        return b, true
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return b, true
    elseif eof(io) && b == opt.quotechar
        return b, true
    end
    return b, false
end

CSVError{T}(::Type{T}, b, row, col) = CSV.CSVError("error parsing a `$T` value on column $col, row $row; encountered '$(@compat(Char(b)))'")

# as a last ditch effort, after we've trying parsing the correct type,
# we check if the field is equal to a custom null type
# otherwise we give up and throw an error
@inline function checknullend{T}(io::Union{IOBuffer,UnsafeBuffer}, ::Type{T}, b::UInt8, opt::CSV.Options, row, col)
    !opt.nullcheck && throw(CSVError(T, b, row, col))
    i = 1
    while true
        b == opt.null[i] || throw(CSVError(T, b, row, col))
        (eof(io) || i == length(opt.null)) && break
        b = read(io, UInt8)
        i += 1
    end
    if !eof(io)
        b = read(io, UInt8)
        b, done = checkdone(io, b, opt)
        done ? (return true) : throw(CSVError(T, b, row, col))
    end
    return true
end
"""
`io` is an `IOBuffer` that is positioned at the first byte/character of an `Integer` field
leading whitespace is ignored
'-' and '+' are accounted for
returns a `Tuple{T<:Integer,Bool}` with a value & bool saying whether the field contains a null value or not
field is null if the next delimiter or newline is encountered before any digits
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown
"""
function parsefield{T<:Integer}(io::Union{IOBuffer,UnsafeBuffer}, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)
    v = zero(T)
    b::UInt8, null::Bool = CSV.checknullstart(io,opt)
    null && return v, true
    negative = false
    if b == CSV.MINUS # check for leading '-' or '+'
        negative = true
        b = read(io, UInt8)
    elseif b == CSV.PLUS
        b = read(io, UInt8)
    end
    while CSV.NEG_ONE < b < CSV.TEN
        # process digits
        v *= 10
        v += b - CSV.ZERO
        eof(io) && return ifelse(negative,-v,v), false
        b = read(io, UInt8)
    end
    b, done::Bool = CSV.checkdone(io,b,opt)
    if done
        return ifelse(negative,-v,v), false
    elseif CSV.checknullend(io,T,b,opt,row,col)
        return v, true
    else
        throw(CSVError("couldn't parse Int"))
    end
end

const REF = Array(Ptr{UInt8},1)
"""
`io` is an `IOBuffer` that is positioned at the first byte/character of an `AbstractFloat` field
leading whitespace is ignored
returns a `Tuple{T<:AbstractFloat,Bool}` with a value & bool saying whether the field contains a null value or not
field is null if the next delimiter or newline is encountered before any digits
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown
"""
function parsefield{T<:AbstractFloat}(io::Union{IOBuffer,UnsafeBuffer}, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)
    b, null = CSV.checknullstart(io,opt)
    v = zero(T)
    null && return v, true
    # subtract 1 because we just read a valid byte, so position(io) is +1 from where a digit should be
    ptr = pointer(io.data) + position(io) - 1
    v = convert(T, ccall(:jl_strtod_c, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF))
    io.ptr += REF[1] - ptr - 1 # Hopefully io.ptr doesn't change for IOBuffer?
    eof(io) && return v, false
    b = read(io, UInt8)
    b, done = checkdone(io,b,opt)
    if done
        return v, false
    elseif checknullend(io,T,b,opt,row,col)
        return v, true
    else
        throw(CSVError("couldn't parse $T"))
    end
end
"""
`io` is an `IOBuffer` that is positioned at the first byte/character of an `AbstractString` field
leading/trailing whitespace is *not* ignored
returns a `Tuple{PointerString,Bool}` with a value & bool saying whether the field contains a null value or not
field is null if the next delimiter or newline is encountered before any characters
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
"""
function parsefield{T<:AbstractString}(io::Union{IOBuffer,UnsafeBuffer}, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)
    eof(io) && return NULLSTRING, true
    ptr = pointer(io.data) + position(io)
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null)
    @inbounds while !eof(io)
        b = read(io, UInt8)
        if b == opt.quotechar
            ptr += 1
            while !eof(io)
                b = read(io, UInt8)
                if b == opt.escapechar
                    b = read(io, UInt8)
                    len += 1
                elseif b == opt.quotechar
                    break
                end
                (nullcheck && len+1 <= nulllen && b == opt.null[len+1]) || (nullcheck = false)
                len += 1
            end
        elseif b == opt.delim || b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null[len+1]) || (nullcheck = false)
            len += 1
        end
    end
    return (len == 0 || nullcheck) ? (NULLSTRING, true) : (PointerString(ptr, len), false)
end

@inline itr(io,n,val) = (for i = 1:n; val *= 10; val += read(io, UInt8) - CSV.ZERO; end; return val)
"""
`io` is an `IOBuffer` that is positioned at the first byte/character of a `Date` field
leading whitespace is ignored
returns a `Tuple{Date,Bool}` with a value & bool saying whether the field contains a null value or not
field is null if the next delimiter or newline is encountered before any digits/characters of the `opt.dateformat`
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
if field contains digits/characters no compatible with `opt.dateformat`, an error is thrown
"""
function parsefield(io::Union{IOBuffer,UnsafeBuffer}, ::Type{Date}, opt::CSV.Options=CSV.Options(), row=0, col=0)
    b, null = CSV.checknullstart(io,opt)
    null && return Date(0,1,1), true
    if opt.datecheck # optimize for default yyyy-mm-dd
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io,3,Int(b - CSV.ZERO))
            read(io, UInt8)
            month = CSV.itr(io,2,0)
            read(io, UInt8)
            day = CSV.itr(io,2,0)
            eof(io) && return Date(year,month,day), false
            b = read(io, UInt8)
        end
        b, done = checkdone(io,b,opt)
        if done
            return Date(year,month,day), false
        elseif checknullend(io,Date,b,opt,row,col)
            return Date(0,1,1), true
        else
            throw(CSVError("couldn't parse Date"))
        end
    elseif opt.dateformat == EMPTY_DATEFORMAT
        throw(ArgumentError("Can't parse a `Date` type with $EMPTY_DATEFORMAT; please provide a valid Dates.DateFormat or date format string"))
    else
        pos = position(io) - 1 # current position of start of date string (even if date is quoted, and we know it's not empty)
        quoted = false
        end_of_file = false
        while true
            b = read(io, UInt8)
            if b == opt.delim || b == CSV.NEWLINE
                break
            elseif b == CSV.RETURN
                !eof(io) && peek(io) == CSV.NEWLINE && read(io, UInt8)
                break
            elseif eof(io)
                end_of_file = true
                break
            elseif b == opt.quotechar
                b, done = checkdone(io,b,opt)
                quoted = true
                break
            end
        end
        val = bytestring(pointer(io.data)+pos, position(io)-pos-quoted-1+end_of_file)
        val == opt.null && return Date(0,1,1), true
        return Date(val, opt.dateformat)::Date, false
    end
end
"""
`io` is an `IOBuffer` that is positioned at the first byte/character of a `DateTime` field
leading whitespace is ignored
returns a `Tuple{DateTime,Bool}` with a value & bool saying whether the field contains a null value or not
field is null if the next delimiter or newline is encountered before any digits/characters of the `opt.dateformat`
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
if field contains digits/characters not compatible with `opt.dateformat`, an error is thrown
"""
function parsefield(io::Union{IOBuffer,UnsafeBuffer}, ::Type{DateTime}, opt::CSV.Options=CSV.Options(), row=0, col=0)
    b, null = CSV.checknullstart(io,opt)
    null && return DateTime(0,1,1), true
    if opt.datecheck # optimize for default yyyy-mm-ddTHH:MM:SS
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io,3,Int(b - CSV.ZERO))
            read(io, UInt8)
            month = CSV.itr(io,2,0)
            read(io, UInt8)
            day = CSV.itr(io,2,0)
            eof(io) && return DateTime(year,month,day), false
            b = read(io, UInt8) # read the `T`
            hour = CSV.itr(io,2,0)
            read(io, UInt8)
            minute = CSV.itr(io,2,0)
            read(io, UInt8)
            second = CSV.itr(io,2,0)
            b = read(io, UInt8) # read the `T`
            eof(io) && return DateTime(year,month,day,hour,minute,second), false
            b = read(io, UInt8)
        end
        b, done = checkdone(io,b,opt)
        if done
            return DateTime(year,month,day,hour,minute,second), false
        elseif checknullend(io,DateTime,b,opt,row,col)
            return DateTime(0,1,1), true
        else
            throw(CSVError("couldn't parse DateTime"))
        end
    elseif opt.dateformat == EMPTY_DATEFORMAT
        throw(ArgumentError("Can't parse a `DateTime` type with $EMPTY_DATEFORMAT; please provide a valid Dates.DateFormat or date format string"))
    else
        pos = position(io) - 1 # current position of start of date string (even if date is quoted, and we know it's not empty)
        quoted = false
        end_of_file = false
        while true
            b = read(io, UInt8)
            if b == opt.delim || b == CSV.NEWLINE
                break
            elseif b == CSV.RETURN
                !eof(io) && peek(io) == CSV.NEWLINE && read(io, UInt8)
                break
            elseif eof(io)
                end_of_file = true
                break
            elseif b == opt.quotechar
                b, done = checkdone(io,b,opt)
                quoted = true
                break
            end
        end
        val = bytestring(pointer(io.data)+pos, position(io)-pos-quoted-1+end_of_file)
        val == opt.null && return DateTime(0,1,1), true
        return DateTime(val, opt.dateformat)::DateTime, false
    end
end
