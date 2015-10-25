"read a single line from `io` (any `IO` type) as a string, accounting for potentially embedded newlines in quoted fields (e.g. value1, value2, \"value3 with \n embedded newlines\"). Can optionally provide a `buf::IOBuffer` type for buffer resuse"
function Base.readline(io::IO,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())
    while !eof(io)
        b = read(io, UInt8)
        write(buf, b)
        if b == q
            while !eof(io)
                b = read(io, UInt8)
                write(buf, b)
                if b == e
                    b = read(io, UInt8)
                    write(buf, b)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && write(buf,read(io, UInt8))
            break
        end
    end
    return takebuf_string(buf)
end

# read and split a line into string values;
# write(t,"\"hey there\",1000,\"1000\",\"\",,1.0,\"hey \n \\\"quote\\\" there\"\n"); seekstart(t)
"read a single line from `io` (any `IO` type) as a `Vector{UTF8String}` with elements being delimited fields. Can optionally provide a `buf::IOBuffer` type for buffer resuse"
function readsplitline(io::IO,d::UInt8,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())
    vals = UTF8String[]
    while !eof(io)
        b = read(io, UInt8)
        if b == q
            while !eof(io)
                b = read(io, UInt8)
                if b == e
                    write(buf, b)
                    b = read(io, UInt8)
                    write(buf, b)
                elseif b == q
                    break
                else
                    write(buf, b)
                end
            end
        elseif b == d
            push!(vals,takebuf_string(buf))
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peek(io) == NEWLINE && read(io, UInt8)
            break
        else
            write(buf, b)
        end
    end
    return push!(vals,takebuf_string(buf))
end
"count the number of lines in a file, accounting for potentially embedded newlines in quoted fields"
function Base.countlines(f::IO,q::UInt8,e::UInt8)
    nl = 1
    b = 0x00
    while !eof(f)
        b = read(f, UInt8)
        if b == q
            while !eof(f)
                b = read(f, UInt8)
                if b == e
                    b = read(f, UInt8)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            nl += 1
        elseif b == RETURN
            nl += 1
            !eof(f) && peek(f) == NEWLINE && read(f, UInt8)
        end
    end
    return nl - (b == NEWLINE || b == RETURN)
end

function skipto!(f::IO,cur,dest,q,e)
    cur >= dest && return
    for _ = 1:(dest-cur)
        readline(f,q,e)
    end
    return
end

# NullField is used only during the type detection process
immutable NullField end

"try to infer the type of the value in `val`. The precedence of type checking is `Int` => `Float64` => `Date` => `DateTime` => `String`"
function detecttype(val::AbstractString,format,null)
    (val == "" || val == null) && return NullField
    val2 = replace(val, @compat(Char(COMMA)), "") # remove potential comma separators from integers
    t = tryparse(Int,val2)
    !isnull(t) && return Int
    # our strtod only works on period decimal points (e.g. "1.0")
    t = tryparse(Float64,val2)
    !isnull(t) && return Float64
    if format != EMPTY_DATEFORMAT
        try # it might be nice to throw an error when a format is specifically given but doesn't parse
            Date(val,format)
            return Date
        end
        try
            DateTime(val,format)
            return DateTime
        end
    else
        try
            Date(val)
            return Date
        end
        try
            DateTime(val)
            return DateTime
        end
    end
    return PointerString
end
