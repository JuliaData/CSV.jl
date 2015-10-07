readfield!(io::IOBuffer,dest::NullableVector,typ;null::AbstractString="",format::Dates.DateFormat=EMPTY_DATEFORMAT) =
    readfield!(Source("",COMMA,QUOTE,ESCAPE,COMMA,PERIOD,null,null != "",Schema(UTF8String[],DataType[],0,0),typ == Date && format == EMPTY_DATEFORMAT ? Dates.ISODateFormat : format,io.data,1),
    dest,typ,1,1)

function Base.isnull(io::Source,b,row,col)
    !io.nullcheck && return false
    i = 1
    while !eof(io) && i <= length(io.null)
        b == io.null[i] || return false
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
            throw(CSV.CSVError("error parsing $T on column $col, row $row; parsed $v before invalid digit '$(@compat(Char(b)))'"))
        end
    end
    return true
end

@inline function setfield!(dest::NullableVector, row, val, null)
    @inbounds dest.values[row], dest.isnull[row] = val, null
    return
end

# `io` points to the first byte/character of an Integer field
function readfield!{T<:Integer}(io::Source, dest::NullableVector{T}, ::Type{T}, row=0, col=0)
    v = zero(T)
    eof(io) && return setfield!(dest,row,0,true)
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return setfield!(dest,row,0,true)
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE # check for empty field
        return setfield!(dest,row,0,true)
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return setfield!(dest,row,0,true)
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
        eof(io) && return setfield!(dest,row,ifelse(negative,-v,v), false)
        b = read(io, UInt8)
    end
    b == io.quotechar && !eof(io) && (b = read(io, UInt8))
    if b == io.delim || b == NEWLINE
        return setfield!(dest,row,ifelse(negative,-v,v), false)
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return setfield!(dest,row,ifelse(negative,-v,v), false)
    elseif eof(io) && b == io.quotechar
        return setfield!(dest,row,ifelse(negative,-v,v), false)
    elseif isnull(io,b,row,col)
        return setfield!(dest,row,0,true)
    else
        throw(CSV.CSVError("error parsing $T on column $col, row $row; parsed $v before invalid digit '$(@compat(Char(b)))'"))
    end
end

const REF = Array(Ptr{UInt8},1)

function readfield!{T<:AbstractFloat}(io::Source, dest::NullableVector{T}, ::Type{T}, row=0, col=0)
    eof(io) && return setfield!(dest, row, NaN, true)
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return setfield!(dest, row, NaN, true)
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE
        return setfield!(dest, row, NaN, true)
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return setfield!(dest, row, NaN, true)
    end
    # subtract 1 because we just read a valid byte, so position(io) is +1 from where a digit should be
    ptr = pointer(io.data) + position(io) - 1
    v = ccall(:strtod, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF)
    io.ptr += REF[1] - ptr - 1 # Hopefully io.ptr doesn't change for IOBuffer?
    eof(io) && return setfield!(dest, row, v, false)
    b = read(io, UInt8)
    b == io.quotechar && !eof(io) && (b = read(io, UInt8))
    if b == io.delim || b == NEWLINE
        return setfield!(dest, row, v, false)
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return setfield!(dest, row, v, false)
    elseif eof(io) && b == io.quotechar
        return setfield!(dest, row, v, false)
    elseif isnull(io,b,row,col)
        return setfield!(dest, row, NaN, true)
    else
        throw(CSV.CSVError("error parsing Float64 on row $row, column $col; parsed '$v' before invalid digit '$(@compat(Char(b)))'"))
    end
end

function readfield!{T<:AbstractString}(io::Source, dest::NullableVector{T}, ::Type{T}, row=0, col=0)
    eof(io) && return setfield!(dest, row, NULLSTRING, true)
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
    return (len == 0 || nullcheck) ? setfield!(dest, row, NULLSTRING, true) : setfield!(dest, row, PointerString(ptr, len), false)
end

@inline itr(io,n,val) = (for i = 1:n; val *= 10; val += read(io, UInt8) - CSV.ZERO; end; return val)

function readfield!(io::Source, dest::NullableVector{Date}, ::Type{Date}, row=0, col=0)
    eof(io) && return setfield!(dest, row, Date(0,1,1), true)
    b = read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == io.quotechar
        eof(io) && return setfield!(dest, row, Date(0,1,1), true)
        b = read(io, UInt8)
    end
    if b == io.delim || b == NEWLINE
        return setfield!(dest, row, Date(0,1,1), true)
    elseif b == RETURN
        !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
        return setfield!(dest, row, Date(0,1,1), true)
    end
    if io.dateformat == Dates.ISODateFormat # optimize for default yyyy-mm-dd
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io,3,b - CSV.ZERO)
            read(io, UInt8)
            month = itr(io,2,0)
            read(io, UInt8)
            day = itr(io,2,0)
            eof(io) && return setfield!(dest, row, Date(year,month,day), false)
            b = read(io, UInt8)
            b == io.quotechar && !eof(io) && (b = read(io, UInt8))
        else
            if isnull(io,b,row,col)
                return setfield!(dest, row, Date(0,1,1), true)
            else
                throw(CSV.CSVError("error parsing Date on row $row, column $col; parsed '$year-$month-$day' before invalid character '$(@compat(Char(b)))'"))
            end
        end
        if b == io.delim || b == NEWLINE || eof(io)
            return setfield!(dest, row, Date(year,month,day), false)
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            return setfield!(dest, row, Date(year,month,day), false)
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
        # println(val)
        val == io.null && return setfield!(dest, row, Date(0,1,1), true)
        return setfield!(dest, row, Date(val, io.dateformat), false)
    end
end
