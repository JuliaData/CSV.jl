"""
`CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer())` => `String`
`CSV.readline(source::CSV.Source)` => `String`

read a single line from `io` (any `IO` type) or a `CSV.Source` as a string,
accounting for potentially embedded newlines in quoted fields
(e.g. value1, value2, \"value3 with \n embedded newlines\").
Can optionally provide a `buf::IOBuffer` type for buffer reuse

This function basically mirrors `Base.readline` except it can account for quoted newlines to **not** as the true end of a line.
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

"""
`CSV.readsplitline(io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())` => `Vector{String}`
`CSV.readsplitline(source::CSV.Source)` => `Vector{String}`

read a single, delimited line from `io` (any `IO` type) or a `CSV.Source` as a `Vector{String}`
delimited fields are separated by an ascii character `d`).
Can optionally provide a `buf::IOBuffer` type for buffer reuse
"""
function readsplitline end

function readsplitline(io::IO, d::UInt8, q::UInt8, e::UInt8, buf::IOBuffer=IOBuffer())
    vals = String[]
    while !eof(io)
        b = unsafe_read(io, UInt8)
        if b == q
            while !eof(io)
                b = unsafe_read(io, UInt8)
                if b == e
                    Base.write(buf, b)
                    b = unsafe_read(io, UInt8)
                    Base.write(buf, b)
                elseif b == q
                    break
                else
                    Base.write(buf, b)
                end
            end
        elseif b == d
            push!(vals,String(take!(buf)))
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            break
        else
            Base.write(buf, b)
        end
    end
    return push!(vals,String(take!(buf)))
end
readsplitline(io::IO, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer()) = readsplitline(io, UInt8(d), UInt8(q), UInt8(e), buf)
readsplitline(source::CSV.Source) = readsplitline(source.io, source.options.delim, source.options.quotechar, source.options.escapechar)

"""
`CSV.countlines(io::IO, quotechar, escapechar)` => `Int`
`CSV.countlines(source::CSV.Source)` => `Int`

count the number of lines in a file, accounting for potentially embedded newlines in quoted fields
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
slottype{T}(df::Dates.Slot{T}) = T
timetype(df::Dates.DateFormat) = any(slottype(T) in (Dates.Hour,Dates.Minute,Dates.Second,Dates.Millisecond) for T in df.slots) ? DateTime : Date

function detecttype(val::AbstractString, format, datecheck, null)
    (val == "" || val == null) && return NullField
    try
        v = parsefield(IOBuffer(replace(val, Char(COMMA), "")), Int)
        !isnull(v) && return Int
    end
    try
        v = CSV.parsefield(IOBuffer(replace(val, Char(COMMA), "")), Float64)
        !isnull(v) && return Float64
    end
    if !datecheck
        try
            T = timetype(format)
            T(val, format)
            return T
        end
    else
        try
            v = CSV.parsefield(IOBuffer(val), Date, CSV.Options(dateformat=Dates.ISODateFormat))
            !isnull(v) && return Date
        end
        try
            v = CSV.parsefield(IOBuffer(val), DateTime, CSV.Options(dateformat=Dates.ISODateTimeFormat))
            !isnull(v) && return DateTime
        end
    end
    return WeakRefString{UInt8}
end
