"read a single line from `io` (any `IO` type) as a string, accounting for potentially embedded newlines in quoted fields (e.g. value1, value2, \"value3 with \n embedded newlines\"). Can optionally provide a `buf::IOBuffer` type for buffer resuse"
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

# read and split a line into string values;
# write(t,"\"hey there\",1000,\"1000\",\"\",,1.0,\"hey \n \\\"quote\\\" there\"\n"); seekstart(t)
"read a single line from `io` (any `IO` type) as a `Vector{String}` with elements being delimited fields. Can optionally provide a `buf::IOBuffer` type for buffer resuse"
function readsplitline(io::IO,d::UInt8=COMMA,q::UInt8=QUOTE,e::UInt8=ESCAPE,buf::IOBuffer=IOBuffer())
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
"count the number of lines in a file, accounting for potentially embedded newlines in quoted fields"
function countlines(f::IO,q::UInt8,e::UInt8)
    nl = 1
    b = 0x00
    while !eof(f)
        b = unsafe_read(f, UInt8)
        if b == q
            while !eof(f)
                b = unsafe_read(f, UInt8)
                if b == e
                    b = unsafe_read(f, UInt8)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            nl += 1
        elseif b == RETURN
            nl += 1
            !eof(f) && unsafe_peek(f) == NEWLINE && unsafe_read(f, UInt8)
        end
    end
    return nl - (b == NEWLINE || b == RETURN)
end

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
        v, n = parsefield(IOBuffer(replace(val, Char(COMMA), "")), Int)
        !n && return Int
    end
    # our strtod only works on period decimal points (e.g. "1.0")
    try
        v, n = parsefield(IOBuffer(replace(val, Char(COMMA), "")), Float64)
        !n && return Float64
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
            v, n = CSV.parsefield(IOBuffer(val), Date)
            !n && return Date
        end
        try
            v, n = parsefield(IOBuffer(val), DateTime)
            !n && return DateTime
        end
    end
    return WeakRefString{UInt8}
end
