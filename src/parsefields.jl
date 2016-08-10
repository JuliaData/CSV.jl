@enum ParsingState None Delimiter EOF Newline
# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a null field
# also return `b` which is last byte read
@inline function checknullstart(io::IO, opt::CSV.Options, state)
    eof(io) && (state[] = EOF; return 0x00, true)
    b = Base.read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar
        eof(io) && (state[] = EOF; return b, true)
        b = Base.read(io, UInt8)
    end
    if b == opt.delim
        state[] = Delimiter
        return b, true
    elseif b == NEWLINE # check for empty field
        state[] = Newline
        return b, true
    elseif b == RETURN
        state[] = Newline
        !eof(io) && unsafe_peek(io) == NEWLINE && Base.read(io, UInt8)
        return b, true
    end
    return b, false
end
# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
@inline function checkdone(io::IO, b::UInt8, opt::CSV.Options, state)
    b == opt.quotechar && !eof(io) && (b = Base.read(io, UInt8))
    if b == opt.delim
        state[] = Delimiter
        return b, true
    elseif b == NEWLINE
        state[] = Newline
        return b, true
    elseif b == RETURN
        state[] = Newline
        !eof(io) && unsafe_peek(io) == NEWLINE && Base.read(io, UInt8)
        return b, true
    elseif eof(io) && b == opt.quotechar
        state[] = EOF
        return b, true
    end
    return b, false
end

CSVError{T}(::Type{T}, b, row, col) = CSV.CSVError("error parsing a `$T` value on column $col, row $row; encountered '$(Char(b))'")

# as a last ditch effort, after we've trying parsing the correct type,
# we check if the field is equal to a custom null type
# otherwise we give up and throw an error
@inline function checknullend{T}(io::IO, ::Type{T}, b::UInt8, opt::CSV.Options, row, col, state)
    !opt.nullcheck && throw(CSVError(T, b, row, col))
    i = 1
    while true
        b == opt.null.data[i] || throw(CSVError(T, b, row, col))
        (eof(io) || i == length(opt.null)) && break
        b = Base.read(io, UInt8)
        i += 1
    end
    if !eof(io)
        b = Base.read(io, UInt8)
        b, done = checkdone(io, b, opt, state)
        done ? (return true) : throw(CSVError(T, b, row, col))
    end
    state[] = EOF
    return true
end
"""
`CSV.parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)` => `Nullable{T}`

`io` is an `IO` type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell)
leading whitespace is ignored for Integer and Float types.
returns a `Nullable{T}` saying whether the field contains a null value or not (empty field, missing value)
field is null if the next delimiter or newline is encountered before any other characters.
Specialized methods exist for Integer, Float, String, Date, and DateTime.
For other types `T`, a generic fallback requires `parse(T, str::String)` to be defined.
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
For numeric fields, if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown
"""
function parsefield end

function parsefield{T<:Integer}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    v = zero(T)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{T}()
    negative = false
    if b == CSV.MINUS # check for leading '-' or '+'
        negative = true
        b = Base.read(io, UInt8)
    elseif b == CSV.PLUS
        b = Base.read(io, UInt8)
    end
    while CSV.NEG_ONE < b < CSV.TEN
        # process digits
        v *= 10
        v += b - CSV.ZERO
        eof(io) && (state[] = EOF; return Nullable{T}(ifelse(negative,-v,v)))
        b = Base.read(io, UInt8)
    end
    b, done::Bool = CSV.checkdone(io, b, opt, state)
    if done
        return Nullable{T}(ifelse(negative,-v,v))
    elseif CSV.checknullend(io, T, b, opt, row, col, state)
        return Nullable{T}()
    else
        throw(CSVError("couldn't parse Int"))
    end
end

const REF = Array(Ptr{UInt8},1)

function parsefield{T<:AbstractFloat}(io::IOBuffer, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    b, null = CSV.checknullstart(io, opt, state)
    v = zero(T)
    null && return Nullable{T}()
    # subtract 1 because we just read a valid byte, so position(io) is +1 from where a digit should be
    ptr = pointer(io.data) + position(io) - 1
    v = convert(T, ccall(:jl_strtod_c, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF))
    io.ptr += REF[1] - ptr - 1 # Hopefully io.ptr doesn't change for IOBuffer?
    eof(io) && (state[] = EOF; return Nullable{T}(v))
    b = Base.read(io, UInt8)
    b, done = checkdone(io, b, opt, state)
    if done
        return Nullable{T}(v)
    elseif checknullend(io, T, b, opt, row, col, state)
        return Nullable{T}()
    else
        throw(CSVError("couldn't parse $T"))
    end
end

function getfloat(io, T, b, opt, row, col, buf, state)
    ptr = pointer(buf.data)
    v = convert(T, ccall(:jl_strtod_c, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, CSV.REF))
    if (CSV.REF[1] - ptr) != position(buf)
        reset(io)
        b, null = CSV.checknullstart(io, opt, state)
        CSV.checknullend(io, T, b, opt, row, col, state) && return Nullable{T}()
        throw(CSVError(T, b, row, col))
    end
    return Nullable{T}(v)
end

function parsefield{T<:AbstractFloat}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    mark(io)
    b, null = CSV.checknullstart(io, opt, state)
    v = zero(T)
    null && return Nullable{T}()
    buf = IOBuffer(zeros(UInt8, 18), true, true)
    Base.write(buf, b)
    eof(io) && return getfloat(io, T, b, opt, row, col, buf, state)
    while true
        b = Base.read(io, UInt8)
        b, isdone = CSV.checkdone(io, b, opt, state)
        isdone && return getfloat(io, T, b, opt, row, col, buf, state)
        Base.write(buf, b)
        eof(io) && return getfloat(io, T, b, opt, row, col, buf, state)
    end
    throw(CSVError("couldn't parse $T"))
end

function parsefield{T<:AbstractString}(io::IOBuffer, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    eof(io) && (state[] = EOF; return Nullable{T}(WeakRefStrings.NULLSTRING, true))
    ptr = pointer(io.data) + position(io)
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null)
    @inbounds while !eof(io)
        b = Base.read(io, UInt8)
        if b == opt.quotechar
            ptr += 1
            while !eof(io)
                b = Base.read(io, UInt8)
                if b == opt.escapechar
                    b = Base.read(io, UInt8)
                    len += 1
                elseif b == opt.quotechar
                    break
                end
                (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
                len += 1
            end
        elseif b == opt.delim
            state[] = Delimiter
            break
        elseif b == b == NEWLINE
            state[] = Newline
            break
        elseif b == RETURN
            state[] = Newline
            !eof(io) && unsafe_peek(io) == NEWLINE && Base.read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
            len += 1
        end
    end
    eof(io) && (state[] = EOF)
    return (len == 0 || nullcheck) ? Nullable{T}(WeakRefStrings.NULLSTRING, true) : Nullable{T}(WeakRefString(ptr, len))
end

function parsefield{T<:AbstractString}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    eof(io) && (state[] = EOF; return Nullable{T}())
    buf = IOBuffer()
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null)
    @inbounds while !eof(io)
        b = Base.read(io, UInt8)
        if b == opt.quotechar
            while !eof(io)
                b = Base.read(io, UInt8)
                if b == opt.escapechar
                    Base.write(buf, b)
                    b = Base.read(io, UInt8)
                    len += 1
                elseif b == opt.quotechar
                    break
                end
                (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
                Base.write(buf, b)
                len += 1
            end
        elseif b == opt.delim
            state[] = Delimiter
            break
        elseif b == NEWLINE
            state[] = Newline
            break
        elseif b == RETURN
            state[] = Newline
            !eof(io) && unsafe_peek(io) == NEWLINE && Base.read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
            Base.write(buf, b)
            len += 1
        end
    end
    eof(io) && (state[] = EOF)
    return (len == 0 || nullcheck) ? Nullable{T}() : Nullable{T}(takebuf_string(buf))
end

@inline itr(io,n,val) = (for i = 1:n; val *= 10; val += Base.read(io, UInt8) - CSV.ZERO; end; return val)

function parsefield(io::IO, ::Type{Date}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{Date}()
    if opt.datecheck # optimize for default yyyy-mm-dd
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io, 3, Int(b - CSV.ZERO))
            Base.read(io, UInt8)
            month = CSV.itr(io, 2, 0)
            Base.read(io, UInt8)
            day = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{Date}(Date(year, month, day)))
            b = Base.read(io, UInt8)
        end
        b, done = checkdone(io, b, opt, state)
        if done
            return Nullable{Date}(Date(year, month, day))
        elseif checknullend(io, Date, b, opt, row, col, state)
            return Nullable{Date}()
        else
            throw(CSVError("couldn't parse Date"))
        end
    elseif opt.dateformat == EMPTY_DATEFORMAT
        throw(ArgumentError("Can't parse a `Date` type with $EMPTY_DATEFORMAT; please provide a valid Dates.DateFormat or date format string"))
    else
        buf = IOBuffer()
        Base.write(buf, b)
        while true
            b = Base.read(io, UInt8)
            if b == opt.delim
                state[] = Delimiter
                break
            elseif b == CSV.NEWLINE
                state[] = Newline
                break
            elseif b == CSV.RETURN
                state[] = Newline
                !eof(io) && unsafe_peek(io) == CSV.NEWLINE && Base.read(io, UInt8)
                break
            elseif eof(io)
                state[] = EOF
                Base.write(buf, b)
                break
            elseif b == opt.quotechar
                b, done = checkdone(io, b, opt, state)
                break
            end
            Base.write(buf, b)
        end
        val = takebuf_string(buf)
        val == opt.null && return Nullable{Date}()
        return Nullable{Date}(Date(val, opt.dateformat)::Date)
    end
end

function parsefield(io::IO, ::Type{DateTime}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{DateTime}()
    if opt.datecheck # optimize for default yyyy-mm-ddTHH:MM:SS
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io, 3, Int(b - CSV.ZERO))
            Base.read(io, UInt8)
            month = CSV.itr(io, 2, 0)
            Base.read(io, UInt8)
            day = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{DateTime}(DateTime(year, month, day)))
            b = Base.read(io, UInt8) # read the `T`
            hour = CSV.itr(io, 2, 0)
            Base.read(io, UInt8)
            minute = CSV.itr(io, 2, 0)
            Base.read(io, UInt8)
            second = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{DateTime}(DateTime(year, month, day, hour, minute, second)))
            b = Base.read(io, UInt8)
        end
        b, done = checkdone(io, b, opt, state)
        if done
            return Nullable{DateTime}(DateTime(year, month, day, hour, minute, second))
        elseif checknullend(io, DateTime, b, opt, row, col, state)
            return Nullable{DateTime}()
        else
            throw(CSVError("couldn't parse DateTime"))
        end
    elseif opt.dateformat == EMPTY_DATEFORMAT
        throw(ArgumentError("Can't parse a `DateTime` type with $EMPTY_DATEFORMAT; please provide a valid Dates.DateFormat or date format string"))
    else
        buf = IOBuffer()
        Base.write(buf, b)
        while true
            b = Base.read(io, UInt8)
            if b == opt.delim
                state[] = Delimter
                break
            elseif b == CSV.NEWLINE
                state[] = Newline
                break
            elseif b == CSV.RETURN
                state[] = Newline
                !eof(io) && unsafe_peek(io) == CSV.NEWLINE && Base.read(io, UInt8)
                break
            elseif eof(io)
                state[] = EOF
                Base.write(buf, b)
                break
            elseif b == opt.quotechar
                b, done = checkdone(io, b, opt, state)
                break
            end
            Base.write(buf, b)
        end
        val = takebuf_string(buf)
        val == opt.null && return Nullable{DateTime}()
        return Nullable{DateTime}(DateTime(val, opt.dateformat)::DateTime)
    end
end

# Generic fallback
function getgeneric(io, T, b, opt, row, col, buf, state)
    str = takebuf_string(buf)
    try
        val = parse(T, str)::T
    catch e
        reset(io)
        b, null = CSV.checknullstart(io, opt, state)
        CSV.checknullend(io, T, b, opt, row, col, state) && return Nullable{T}()
        throw(CSVError(T, b, row, col))
    end
    return Nullable{T}(val)
end

function parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state=Ref{ParsingState}(None))
    mark(io)
    b, null = CSV.checknullstart(io, opt, state)
    v = zero(T)
    null && return Nullable{T}()
    buf = IOBuffer()
    Base.write(buf, b)
    eof(io) && (state[] = EOF; return getgeneric(io, T, b, opt, row, col, buf, state))
    while true
        b = Base.read(io, UInt8)
        b, isdone = CSV.checkdone(io, b, opt, state)
        isdone && return getgeneric(io, T, b, opt, row, col, buf, state)
        Base.write(buf, b)
        eof(io) && return getgeneric(io, T, b, opt, row, col, buf, state)
    end
    throw(CSVError("couldn't parse $T"))
end
