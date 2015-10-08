# for potentially embedded newlines/delimiters in quoted fields
function Base.readline(f::IO,q::UInt8,e::UInt8)
    buf = IOBuffer()
    while !eof(f)
        b = read(f, UInt8)
        write(buf, b)
        if b == q
            while !eof(f)
                b = read(f, UInt8)
                write(buf, b)
                if b == e
                    b = read(f, UInt8)
                    write(buf, b)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(f) && peek(f) == NEWLINE && write(buf,read(f, UInt8))
            break
        end
    end
    return takebuf_string(buf)
end

# read and split a line into string values;
# write(t,"\"hey there\",1000,\"1000\",\"\",,1.0,\"hey \n \\\"quote\\\" there\"\n"); seekstart(t)
function readsplitline(f::IO,d::UInt8,q::UInt8,e::UInt8)
    vals = UTF8String[]
    buf = IOBuffer()
    while !eof(f)
        b = read(f, UInt8)
        if b == q
            while !eof(f)
                b = read(f, UInt8)
                if b == e
                    write(buf, b)
                    b = read(f, UInt8)
                    write(buf, b)
                elseif b == q
                    # push!(vals,takebuf_string(buf))
                    # !eof(f) && read(f, UInt8) # read the delim, '\r', or '\n'
                    # !eof(f) && peek(f) == NEWLINE && read(f, UInt8)
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
            !eof(f) && peek(f) == NEWLINE && read(f, UInt8)
            break
        else
            write(buf, b)
        end
    end
    return push!(vals,takebuf_string(buf))
end

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
