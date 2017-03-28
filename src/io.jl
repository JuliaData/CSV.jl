"""
    CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) => String
    CSV.readline(source::CSV.Source) => String

Read a single line from `io` (any `IO` type) or a `CSV.Source` as a `String` object.
This function mirrors `Base.readline` except that the newlines within quoted
fields are ignored (e.g. value1, value2, \"value3 with \n embedded newlines\").
Uses `buf::IOBuffer` for intermediate IO operations, if specified.
"""
function readline end

function readline(io::IO, q::UInt8, e::UInt8, buf::IOBuffer=IOBuffer())
    while !eof(io)
        b = unsafe_read(io, UInt8)
        Base.write(buf, b)
        if b == q
            while !eof(io)
                b = unsafe_read(io, UInt8)
                Base.write(buf, b)
                if b == e
                    b = unsafe_read(io, UInt8)
                    Base.write(buf, b)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && unsafe_peek(io) == NEWLINE && Base.write(buf, unsafe_read(io, UInt8))
            break
        end
    end
    return String(take!(buf))
end
readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) = readline(io, UInt8(q), UInt8(e), buf)
readline(source::CSV.Source) = readline(source.io, source.options.quotechar, source.options.escapechar)

# contents of a single  CSV table field as returned by readsplitline!()
immutable RawField
    value::String   # uparsed contents
    isquoted::Bool  # whether the field value was quoted or not
end

Base.:(==)(a::RawField, b::RawField) = (a.isquoted == b.isquoted) && (a.value == b.value)

"""
    CSV.readsplitline!(vals::Vector{RawField}, io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())
    CSV.readsplitline!(vals::Vector{RawField}, source::CSV.Source)

Read a single, delimited line from `io` (any `IO` type) or a `CSV.Source` as a `Vector{String}` and
store the values in `vals`.
Delimited fields are separated by `d`, quoted by `q` and escaped by `e` ASCII characters.
The contents of `vals` are replaced.
Uses `buf::IOBuffer` for intermediate IO operations, if specified.
"""
function readsplitline! end

@enum ReadSplitLineState RSL_IN_FIELD RSL_IN_QUOTE RSL_AFTER_QUOTE RSL_AFTER_DELIM RSL_AFTER_NEWLINE

function readsplitline!(vals::Vector{RawField}, io::IO, d::UInt8, q::UInt8, e::UInt8, buf::IOBuffer=IOBuffer())
    empty!(vals)
    state = RSL_AFTER_DELIM
    push_buf_to_vals!() = push!(vals, RawField(String(take!(buf)), state==RSL_AFTER_QUOTE))
    while !eof(io)
        b = unsafe_read(io, UInt8)
        if state == RSL_IN_QUOTE # in the quoted string
            if b == q # end the quoted string
                state = RSL_AFTER_QUOTE
            elseif b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                b = unsafe_read(io, UInt8)
                Base.write(buf, b)
            else
                Base.write(buf, b)
            end
        elseif b == d # delimiter
            if state == RSL_AFTER_DELIM # empty field
                push!(vals, RawField("", false))
            else
                push_buf_to_vals!()
            end
            state = RSL_AFTER_DELIM
        elseif b == q # start of quote
            if state == RSL_AFTER_DELIM
                state = RSL_IN_QUOTE
            else
                throw(CSVError("Unexpected start of quote ($q), use \"$e$q\" to type \"$q\""))
            end
        elseif b == NEWLINE
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        elseif b == RETURN
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        else
            if state == RSL_AFTER_QUOTE
                throw(CSVError("Unexpected character ($b) after the end of quote ($q)"))
            elseif b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                b = unsafe_read(io, UInt8)
            end
            Base.write(buf, b)
            state = RSL_IN_FIELD
        end
    end
    if state == RSL_IN_QUOTE
        @assert eof(io)
        throw(CSVError("EOF while trying to read the closing quote"))
    elseif state == RSL_IN_FIELD || state == RSL_AFTER_DELIM # file ended without the newline, store the current buf
        @assert eof(io)
        push_buf_to_vals!()
    end
    return vals
end
readsplitline!(vals::Vector{RawField}, io::IO, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer()) = readsplitline!(vals, io, UInt8(d), UInt8(q), UInt8(e), buf)
readsplitline!(vals::Vector{RawField}, source::CSV.Source) = readsplitline!(vals, source.io, source.options.delim, source.options.quotechar, source.options.escapechar)

readsplitline(io::IO, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer()) =
    readsplitline!(Vector{RawField}(), io, d, q, e, buf)
readsplitline(args...) = readsplitline!(Vector{RawField}(), args...)

"""
    CSV.countlines(io::IO, quotechar, escapechar) => Int
    CSV.countlines(source::CSV.Source) => Int

Count the number of lines in a file, accounting for potentially embedded newlines in quoted fields.
"""
function countlines(io::IO, q::UInt8, e::UInt8)
    nl = 1
    b = 0x00
    while !eof(io)
        b = unsafe_read(io, UInt8)
        if b == q
            while !eof(io)
                b = unsafe_read(io, UInt8)
                if b == e
                    b = unsafe_read(io, UInt8)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            nl += 1
        elseif b == RETURN
            nl += 1
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
        end
    end
    return nl - (b == NEWLINE || b == RETURN)
end
countlines(io::IO, q='"', e='\\') = countlines(io, UInt8(q), UInt8(e))
countlines(source::CSV.Source) = countlines(source.io, source.options.quotechar, source.options.escapechar)

function skipto!(f::IO, cur, dest, q, e)
    cur >= dest && return
    for _ = 1:(dest-cur)
        CSV.readline(f,q,e)
    end
    return
end

# NullField is used only during the type detection process
immutable NullField end

# try to infer the type of the value in `val`. The precedence of type checking is `Int` => `Float64` => `Date` => `DateTime` => `String`
if VERSION > v"0.6.0-dev.2307"
    timetype(df::Dates.DateFormat) = any(typeof(T) in (Dates.DatePart{'H'}, Dates.DatePart{'M'}, Dates.DatePart{'S'}, Dates.DatePart{'s'}) for T in df.tokens) ? DateTime : Date
else
    slottype{T}(df::Dates.Slot{T}) = T
    timetype(df::Dates.DateFormat) = any(slottype(T) in (Dates.Hour,Dates.Minute,Dates.Second,Dates.Millisecond) for T in df.slots) ? DateTime : Date
end

function detecttype(val::RawField, format, datecheck, null)
    val.isquoted && return WeakRefString{UInt8} # quoted is always a string
    (isempty(val.value) || val.value == null) && return NullField
    try
        v = parsefield(IOBuffer(replace(val.value, Char(COMMA), "")), Int)
        !isnull(v) && return Int
    end
    try
        v = CSV.parsefield(IOBuffer(replace(val.value, Char(COMMA), "")), Float64)
        !isnull(v) && return Float64
    end
    if !datecheck
        try
            T = timetype(format)
            T(val.value, format)
            return T
        end
    else
        try
            v = CSV.parsefield(IOBuffer(val.value), Date, CSV.Options(dateformat=Dates.ISODateFormat))
            !isnull(v) && return Date
        end
        try
            v = CSV.parsefield(IOBuffer(val.value), DateTime, CSV.Options(dateformat=Dates.ISODateTimeFormat))
            !isnull(v) && return DateTime
        end
    end
    return WeakRefString{UInt8}
end
