"""
`CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer())` => `String`

read a single line from `io` (any `IO` type) as a string, accounting for potentially embedded newlines in quoted fields (e.g. value1, value2, \"value3 with \n embedded newlines\"). Can optionally provide a `buf::IOBuffer` type for buffer reuse
"""
function readline(io::IO,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())
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
            !eof(io) && unsafe_peek(io) == NEWLINE && Base.write(buf,unsafe_read(io, UInt8))
            break
        end
    end
    return takebuf_string(buf)
end
readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) = readline(io, UInt8(q), UInt8(e), buf)

# read and split a line into string values;
# write(t,"\"hey there\",1000,\"1000\",\"\",,1.0,\"hey \n \\\"quote\\\" there\"\n"); seekstart(t)
"""
`CSV.readsplitline(io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())` => `Vector{String}`

read a single line from `io` (any `IO` type) as a `Vector{String}` with elements being delimited fields. Can optionally provide a `buf::IOBuffer` type for buffer reuse
"""
function readsplitline(io::IO,d::UInt8,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())
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
            push!(vals,takebuf_string(buf))
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            break
        else
            Base.write(buf, b)
        end
    end
    return push!(vals,takebuf_string(buf))
end
readsplitline(io::IO,d=',',q='"',e='\\',buf::IOBuffer=IOBuffer()) = readsplitline(io,UInt8(d),UInt8(q),UInt8(e),buf)

"""
`CSV.countlines(io::IO, quotechar, escapechar)` => Int

count the number of lines in a file, accounting for potentially embedded newlines in quoted fields
"""
function countlines(io::IO,q::UInt8,e::UInt8)
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
countlines(io::IO, q, e) = countlines(io, UInt8(q), UInt8(e))

function skipto!(f::IO,cur,dest,q,e)
    cur >= dest && return
    for _ = 1:(dest-cur)
        CSV.readline(f,q,e)
    end
    return
end

# NullField is used only during the type detection process
immutable NullField end

# try to infer the type of the value in `val`. The precedence of type checking is `Int` => `Float64` => `Date` => `DateTime` => `String`
function detecttype(val::AbstractString,format,null)
    (val == "" || val == null) && return NullField
    try
        v = parsefield(IOBuffer(replace(val, Char(COMMA), "")), Int)
        !isnull(v) && return Int
    end
    try
        v = CSV.parsefield(IOBuffer(replace(val, Char(COMMA), "")), Float64)
        !isnull(v) && return Float64
    end
    if format != EMPTY_DATEFORMAT
        try # it might be nice to throw an error when a format is specifically given but doesn't parse
            Date(IOBuffer(val),format)
            return Date
        end
        try
            DateTime(IOBuffer(val),format)
            return DateTime
        end
    else
        try
            v = CSV.parsefield(IOBuffer(val), Date)
            !isnull(v) && return Date
        end
        try
            v = parsefield(IOBuffer(val), DateTime)
            !isnull(v) && return DateTime
        end
    end
    return WeakRefString{UInt8}
end
