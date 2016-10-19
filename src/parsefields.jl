@enum ParsingState None Delimiter EOF Newline

const STATE = Ref(None)

# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a null field
# also return `b` which is last byte read
@inline function checknullstart(io::IO, opt::CSV.Options, state)
    eof(io) && (state[] = EOF; return 0x00, true)
    b = unsafe_read(io, UInt8)
    while b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar
        eof(io) && (state[] = EOF; return b, true)
        b = unsafe_read(io, UInt8)
    end
    if b == opt.delim
        state[] = Delimiter
        return b, true
    elseif b == NEWLINE # check for empty field
        state[] = Newline
        return b, true
    elseif b == RETURN
        state[] = Newline
        !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
        return b, true
    end
    return b, false
end

# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
@inline function checkdone(io::IO, b::UInt8, opt::CSV.Options, state)
    b == opt.quotechar && !eof(io) && (b = unsafe_read(io, UInt8))
    if b == opt.delim
        state[] = Delimiter
        return b, true
    elseif b == NEWLINE
        state[] = Newline
        return b, true
    elseif b == RETURN
        state[] = Newline
        !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
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
        b = unsafe_read(io, UInt8)
        i += 1
    end
    if !eof(io)
        b = unsafe_read(io, UInt8)
        b, done = checkdone(io, b, opt, state)
        done ? (return true) : throw(CSVError(T, b, row, col))
    end
    state[] = EOF
    return true
end
"""
`CSV.parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)` => `Nullable{T}`
`CSV.parsefield{T}(s::CSV.Source, ::Type{T}, row=0, col=0)` => `Nullable{T}``

`io` is an `IO` type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell)
leading whitespace is ignored for Integer and Float types.
returns a `Nullable{T}` saying whether the field contains a null value or not (empty field, missing value)
field is null if the next delimiter or newline is encountered before any other characters.
Specialized methods exist for Integer, Float, String, Date, and DateTime.
For other types `T`, a generic fallback requires `parse(T, str::String)` to be defined.
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
For numeric fields, if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown

The second method of `CSV.parsefield` operates on a `CSV.Source` directly allowing for easy usage when writing custom parsing routines.
Do note, however, that the `row` and `col` arguments are for error-reporting purposes only. A `CSV.Source` maintains internal state with
regards to the underlying data buffer and can **only** parse fields sequentially. This means that `CSV.parsefield` needs to be called somewhat like:

```julia
source = CSV.Source(file)

types = Data.types(source)

for col = 1:length(types)
    println(get(CSV.parsefield(source, types[col]), "\"\""))
end
```
"""
function parsefield end

parsefield{T}(source::CSV.Source, ::Type{T}, row=0, col=0) = get(CSV.parsefield(source.io, T, source.options, row, col, STATE))
parsefield{T}(source::CSV.Source, ::Type{Nullable{T}}, row=0, col=0) = CSV.parsefield(source.io, T, source.options, row, col, STATE)
parsefield(source::CSV.Source, ::Type{Nullable{WeakRefString{UInt8}}}, row=0, col=0) = CSV.parsefield(source.io, WeakRefString{UInt8}, source.options, row, col, STATE, source.ptr)

@inline function parsefield{T<:Integer}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    v = zero(T)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{T}()
    negative = false
    if b == CSV.MINUS # check for leading '-' or '+'
        negative = true
        b = unsafe_read(io, UInt8)
    elseif b == CSV.PLUS
        b = unsafe_read(io, UInt8)
    end
    while CSV.NEG_ONE < b < CSV.TEN
        # process digits
        v *= 10
        v += b - CSV.ZERO
        eof(io) && (state[] = EOF; return Nullable{T}(ifelse(negative, -v, v)))
        b = unsafe_read(io, UInt8)
    end
    b, done::Bool = CSV.checkdone(io, b, opt, state)
    if done
        return Nullable{T}(ifelse(negative, -v, v))
    elseif CSV.checknullend(io, T, b, opt, row, col, state)
        return Nullable{T}()
    end
end

const REF = Array(Ptr{UInt8},1)

@inline function parsefield{T<:AbstractFloat}(io::IOBuffer, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    b, null = CSV.checknullstart(io, opt, state)
    v = zero(T)
    null && return Nullable{T}()
    # subtract 1 because we just read a valid byte, so position(io) is +1 from where a digit should be
    ptr = pointer(io.data) + position(io) - 1
    v = convert(T, ccall(:jl_strtod_c, Float64, (Ptr{UInt8}, Ptr{Ptr{UInt8}}), ptr, REF))
    io.ptr += REF[1] - ptr - 1 # Hopefully io.ptr doesn't change for IOBuffer?
    eof(io) && (state[] = EOF; return Nullable{T}(v))
    b = unsafe_read(io, UInt8)
    b, done = checkdone(io, b, opt, state)
    if done
        return Nullable{T}(v)
    elseif checknullend(io, T, b, opt, row, col, state)
        return Nullable{T}()
    end
end

function getfloat(io, T, b, opt, row, col, buf, state)
    ptr = pointer(buf.data)
    v = convert(T, ccall(:jl_strtod_c, Float64, (Ptr{UInt8}, Ptr{Ptr{UInt8}}), ptr, CSV.REF))
    if (CSV.REF[1] - ptr) != position(buf)
        reset(io)
        b, null = CSV.checknullstart(io, opt, state)
        CSV.checknullend(io, T, b, opt, row, col, state) && return Nullable{T}()
        throw(CSVError(T, b, row, col))
    end
    return Nullable{T}(v)
end

@inline function parsefield{T<:AbstractFloat}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    mark(io)
    b, null = CSV.checknullstart(io, opt, state)
    v = zero(T)
    null && return Nullable{T}()
    buf = IOBuffer(zeros(UInt8, 18), true, true)
    Base.write(buf, b)
    eof(io) && return getfloat(io, T, b, opt, row, col, buf, state)
    while true
        b = unsafe_read(io, UInt8)
        b, isdone = CSV.checkdone(io, b, opt, state)
        isdone && return getfloat(io, T, b, opt, row, col, buf, state)
        Base.write(buf, b)
        eof(io) && return getfloat(io, T, b, opt, row, col, buf, state)
    end
end

const NULLSTRING = Nullable{WeakRefString{UInt8}}(WeakRefStrings.NULLSTRING, true)

@inline function parsefield(io::IOBuffer, ::Type{WeakRefString{UInt8}}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE, start_ptr=Int(pointer(io.data)))
    eof(io) && (state[] = EOF; return NULLSTRING)
    ptr = start_ptr + position(io)
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null.data)
    @inbounds while !eof(io)
        b = unsafe_read(io, UInt8)
        if b == opt.quotechar
            ptr += 1
            while !eof(io)
                b = unsafe_read(io, UInt8)
                if b == opt.escapechar
                    b = unsafe_read(io, UInt8)
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
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
            len += 1
        end
    end
    eof(io) && (state[] = EOF)
    return (len == 0 || nullcheck) ? NULLSTRING : Nullable{WeakRefString{UInt8}}(WeakRefString(Ptr{UInt8}(ptr), len, ptr - start_ptr))
end

function parsefield{T<:AbstractString}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    eof(io) && (state[] = EOF; return Nullable{T}())
    buf = IOBuffer()
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null)
    @inbounds while !eof(io)
        b = unsafe_read(io, UInt8)
        if b == opt.quotechar
            while !eof(io)
                b = unsafe_read(io, UInt8)
                if b == opt.escapechar
                    Base.write(buf, b)
                    b = unsafe_read(io, UInt8)
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
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null.data[len+1]) || (nullcheck = false)
            Base.write(buf, b)
            len += 1
        end
    end
    eof(io) && (state[] = EOF)
    return (len == 0 || nullcheck) ? Nullable{T}() : Nullable{T}(convert(T, takebuf_string(buf)))
end

@inline itr(io,n,val) = (for i = 1:n; val *= 10; val += unsafe_read(io, UInt8) - CSV.ZERO; end; return val)

function parsefield(io::IO, ::Type{Date}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{Date}()
    if opt.datecheck # optimize for default yyyy-mm-dd
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io, 3, Int(b - CSV.ZERO))
            unsafe_read(io, UInt8)
            month = CSV.itr(io, 2, 0)
            unsafe_read(io, UInt8)
            day = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{Date}(Date(year, month, day)))
            b = unsafe_read(io, UInt8)
        end
        b, done = checkdone(io, b, opt, state)
        if done
            return Nullable{Date}(Date(year, month, day))
        elseif checknullend(io, Date, b, opt, row, col, state)
            return Nullable{Date}()
        end
    else
        buf = IOBuffer()
        Base.write(buf, b)
        while true
            b = unsafe_read(io, UInt8)
            if b == opt.delim
                state[] = Delimiter
                break
            elseif b == CSV.NEWLINE
                state[] = Newline
                break
            elseif b == CSV.RETURN
                state[] = Newline
                !eof(io) && unsafe_peek(io) == CSV.NEWLINE && unsafe_read(io, UInt8)
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

function parsefield(io::IO, ::Type{DateTime}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{DateTime}()
    if opt.datecheck # optimize for default yyyy-mm-ddTHH:MM:SS
        if CSV.NEG_ONE < b < CSV.TEN
            year = CSV.itr(io, 3, Int(b - CSV.ZERO))
            unsafe_read(io, UInt8)
            month = CSV.itr(io, 2, 0)
            unsafe_read(io, UInt8)
            day = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{DateTime}(DateTime(year, month, day)))
            b = unsafe_read(io, UInt8) # read the `T`
            hour = CSV.itr(io, 2, 0)
            unsafe_read(io, UInt8)
            minute = CSV.itr(io, 2, 0)
            unsafe_read(io, UInt8)
            second = CSV.itr(io, 2, 0)
            eof(io) && (state[] = EOF; return Nullable{DateTime}(DateTime(year, month, day, hour, minute, second)))
            b = unsafe_read(io, UInt8)
        end
        b, done = checkdone(io, b, opt, state)
        if done
            return Nullable{DateTime}(DateTime(year, month, day, hour, minute, second))
        elseif checknullend(io, DateTime, b, opt, row, col, state)
            return Nullable{DateTime}()
        end
    else
        buf = IOBuffer()
        Base.write(buf, b)
        while true
            b = unsafe_read(io, UInt8)
            if b == opt.delim
                state[] = Delimiter
                break
            elseif b == CSV.NEWLINE
                state[] = Newline
                break
            elseif b == CSV.RETURN
                state[] = Newline
                !eof(io) && unsafe_peek(io) == CSV.NEWLINE && unsafe_read(io, UInt8)
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
        return Nullable{T}(val)
    catch e
        reset(io)
        b, null = CSV.checknullstart(io, opt, state)
        CSV.checknullend(io, T, b, opt, row, col, state) && return Nullable{T}()
        throw(CSVError(T, b, row, col))
    end
end

function parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    mark(io)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{T}()
    buf = IOBuffer()
    Base.write(buf, b)
    eof(io) && (state[] = EOF; return getgeneric(io, T, b, opt, row, col, buf, state))
    while true
        b = unsafe_read(io, UInt8)
        b, isdone = CSV.checkdone(io, b, opt, state)
        isdone && return getgeneric(io, T, b, opt, row, col, buf, state)
        Base.write(buf, b)
        eof(io) && return getgeneric(io, T, b, opt, row, col, buf, state)
    end
end

@inline function parsefield(io::IO, ::Type{Char}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::Ref{ParsingState}=STATE)
    b, null = CSV.checknullstart(io, opt, state)
    null && return Nullable{Char}()
    eof(io) && (state[] = EOF; return Nullable{Char}(Char(b)))
    if !isempty(opt.null.data) && b == opt.null.data[1]
        CSV.checknullend(io, Char, b, opt, row, col, state) && return Nullable{Char}()
    else
        c = unsafe_read(io, UInt8)
        c, done::Bool = CSV.checkdone(io, c, opt, state)
        if done
            return Nullable{Char}(b)
        elseif CSV.checknullend(io, Char, c, opt, row, col, state)
            return Nullable{Char}()
        end
    end
end
