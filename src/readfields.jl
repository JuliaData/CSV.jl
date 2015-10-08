readfield!(io::IOBuffer,dest::NullableVector,typ;null::AbstractString="",format::Dates.DateFormat=EMPTY_DATEFORMAT) =
    readfield!(Source("",COMMA,QUOTE,ESCAPE,COMMA,PERIOD,null,null != "",Schema(UTF8String[],DataType[],0,0),typ == Date && format == EMPTY_DATEFORMAT ? Dates.ISODateFormat : format,io.data,1,1),
    dest,typ,1,1)

function Base.isnull{T}(io::Source,::Type{T}, b,row,col)
    !io.nullcheck && return false
    i = 1
    while true
        b == io.null[i] || return false
        (eof(io) || i == length(io.null)) && break
        b = read(io, UInt8)
        i += 1
    end
    if !eof(io)
        b = read(io, UInt8)
        b == io.quotechar && !eof(io) && (b = read(io, UInt8))
        if b == io.delim || b == NEWLINE
            return true
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            return true
        elseif eof(io) && b == io.quotechar
            return true
        else
            throw(CSV.CSVError("error parsing $T on column $col, row $row; parsed '$(@compat(Char(b)))'"))
        end
    end
    return true
end

# `io` points to the first byte/character of an Integer field
function readfield{T<:Integer}(io::Source, ::Type{T}, row=0, col=0)
    v = zero(T)
    eof(io) && return 0,true
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return 0,true
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE # check for empty field
        return 0,true
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return 0,true
    end
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
    b == io.quotechar && !eof(io) && (b = read(io, UInt8))
    if b == io.delim || b == NEWLINE
        return ifelse(negative,-v,v), false
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return ifelse(negative,-v,v), false
    elseif eof(io) && b == io.quotechar
        return ifelse(negative,-v,v), false
    elseif isnull(io,T,b,row,col)
        return 0,true
    else
        throw(CSV.CSVError("error parsing $T on column $col, row $row; parsed $v before invalid digit '$(@compat(Char(b)))'"))
    end
end

const REF = Array(Ptr{UInt8},1)

function readfield{T<:AbstractFloat}(io::Source, ::Type{T}, row=0, col=0)
    eof(io) && return NaN, true
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return NaN, true
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE
        return NaN, true
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return NaN, true
    end
    # subtract 1 because we just read a valid byte, so position(io) is +1 from where a digit should be
    ptr = pointer(io.data) + position(io) - 1
    v = ccall(:strtod, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF)
    io.ptr += REF[1] - ptr - 1 # Hopefully io.ptr doesn't change for IOBuffer?
    eof(io) && return v, false
    b = read(io, UInt8)
    b == io.quotechar && !eof(io) && (b = read(io, UInt8))
    if b == io.delim || b == NEWLINE
        return v, false
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return v, false
    elseif eof(io) && b == io.quotechar
        return v, false
    elseif isnull(io,T,b,row,col)
        return NaN, true
    else
        throw(CSV.CSVError("error parsing Float64 on row $row, column $col; parsed '$v' before invalid digit '$(@compat(Char(b)))'"))
    end
end

function readfield{T<:AbstractString}(io::Source, ::Type{T}, row=0, col=0)
    eof(io) && return NULLSTRING, true
    ptr = pointer(io.data) + position(io)
    len = 0
    nullcheck = io.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(io.null)
    @inbounds while !eof(io)
        b = read(io, UInt8)
        if b == io.quotechar
            ptr += 1
            while !eof(io)
                b = read(io, UInt8)
                if b == io.escapechar
                    b = read(io, UInt8)
                    len += 1
                elseif b == io.quotechar
                    break
                end
                (nullcheck && len+1 <= nulllen && b == io.null[len+1]) || (nullcheck = false)
                len += 1
            end
        elseif b == io.delim || b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == io.null[len+1]) || (nullcheck = false)
            len += 1
        end
    end
    return (len == 0 || nullcheck) ? (NULLSTRING, true) : (PointerString(ptr, len), false)
end

@inline itr(io,n,val) = (for i = 1:n; val *= 10; val += read(io, UInt8) - CSV.ZERO; end; return val)

function readfield(io::Source, ::Type{Date}, row=0, col=0)
    eof(io) && return Date(0,1,1), true
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return Date(0,1,1), true
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE
        return Date(0,1,1), true
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return Date(0,1,1), true
    end
    if io.dateformat == Dates.ISODateFormat # optimize for default yyyy-mm-dd
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io,3,b - CSV.ZERO)
            read(io, UInt8)
            month = CSV.itr(io,2,0)
            read(io, UInt8)
            day = CSV.itr(io,2,0)
            eof(io) && return Date(year,month,day), false
            b = read(io, UInt8)
            b == io.quotechar && !eof(io) && (b = read(io, UInt8))
        else
            if CSV.isnull(io,Date,b,row,col)
                return Date(0,1,1), true
            else
                throw(CSV.CSVError("error parsing Date on row $row, column $col; parsed '$year-$month-$day' before invalid character '$(@compat(Char(b)))'"))
            end
        end
        if b == io.delim || b == NEWLINE || eof(io)
            return Date(year,month,day), false
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            return Date(year,month,day), false
        else
            throw(CSV.CSVError("error parsing Date on row $row, column $col; parsed '$year-$month-$day' before invalid character '$(@compat(Char(b)))'"))
        end
    elseif io.dateformat == EMPTY_DATEFORMAT
        throw(ArgumentError("Can't parse a `Date` type with $EMPTY_DATEFORMAT; please provide a valid Dates.DateFormat or date format string"))
    else
        pos = position(io) - 1 # current position of start of date string (even if date is quoted, and we know it's not empty)
        quoted = false
        while true
            b = read(io, UInt8)
            if b == io.delim || b == NEWLINE || eof(io)
                break
            elseif b == RETURN
                !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
                break
            elseif b == io.quotechar
                !eof(io) && read(io, UInt8)
                quoted = true
                break
            end
        end
        val = bytestring(pointer(io.data)+pos, position(io)-pos-quoted-1)
        val == io.null && return Date(0,1,1), true
        return Date(val, io.dateformat), false
    end
end
