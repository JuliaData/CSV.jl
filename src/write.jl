"""
    CSV.write(file, table; kwargs...) => file
    table |> CSV.write(file; kwargs...) => file

Write a [Tables.jl interface input](https://github.com/JuliaData/Tables.jl) to a csv file, given as an `IO` argument or `String`/FilePaths.jl type representing the file name to write to.

Supported keyword arguments include:
* `delim::Union{Char, String}=','`: a character or string to print out as the file's delimiter
* `quotechar::Char='"'`: ascii character to use for quoting text fields that may contain delimiters or newlines
* `openquotechar::Char`: instead of `quotechar`, use `openquotechar` and `closequotechar` to support different starting and ending quote characters
* `escapechar::Char='"'`: ascii character used to escape quote characters in a text field
* `missingstring::String=""`: string to print for `missing` values 
* `dateformat=Dates.default_format(T)`: the date format string to use for printing out `Date` & `DateTime` columns
* `append=false`: whether to append writing to an existing file/IO, if `true`, it will not write column names by default
* `writeheader=!append`: whether to write an initial row of delimited column names, not written by default if appending
* `header`: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table
* `newline='\\n'`: character or string to use to separate rows (lines in the csv file)
* `quotestrings=false`: whether to force all strings to be quoted or not
* `decimal='.'`: character to use as the decimal point when writing floating point numbers
"""
function write end

_seekstart(p::Base.Process) = return
_seekstart(io::IO) = seekstart(io)

function with(f::Function, io::IO, append)
    !append && _seekstart(io)
    f(io)
end

function with(f::Function, file::String, append)
    open(file, append ? "a" : "w") do io
        f(io)
    end
end

const VALUE_BUFFERS = [IOBuffer()]

function _reset(io::IOBuffer)
    io.ptr = 1
    io.size = 0
end

function bufferedwrite(io, x::Union{Float16, Float32, Float64, BigFloat}, df, decimal)
    dot = false
    n = 0
    if isnan(x)
        print(io, "NaN")
        return false
    end
    x < 0 && print(io,'-')
    if isinf(x)
        print(io, "Inf")
        return false
    end
    @static if VERSION < v"1.1.0"
        buffer = Base.Grisu.DIGITSs[Threads.threadid()]
    else
        buffer = Base.Grisu.getbuf()
    end
    len, pt, neg = Base.Grisu.grisu(x,Base.Grisu.SHORTEST,n,buffer)
    pdigits = pointer(buffer)
    e = pt-len
    k = -9<=e<=9 ? 1 : 2
    if -pt > k+1 || e+dot > k+1
        # => ########e###
        unsafe_write(io, pdigits+0, len)
        print(io, 'e')
        print(io, string(e))
        return false
    elseif pt <= 0
        # => 0.000########
        print(io, "0$decimal")
        while pt < 0
            print(io, '0')
            pt += 1
        end
        unsafe_write(io, pdigits+0, len)
    elseif e >= dot
        # => ########000.
        unsafe_write(io, pdigits+0, len)
        while e > 0
            print(io, '0')
            e -= 1
        end
        if dot
            print(io, decimal)
        end
    else # => ####.####
        unsafe_write(io, pdigits+0, pt)
        print(io, decimal)
        unsafe_write(io, pdigits+pt, len-pt)
    end
    return false
end

function bufferedwrite(io, val::Number, df, decimal)
    print(io, val)
    return false
end

getvalue(x, df) = x
getvalue(x::T, df) where {T <: Dates.TimeType} = Dates.format(x, df === nothing ? Dates.default_format(T) : df)

function bufferedwrite(io, val, df, decimal)
    VALUE_BUFFER = VALUE_BUFFERS[Threads.threadid()]
    _reset(VALUE_BUFFER)
    print(VALUE_BUFFER, getvalue(val, df))
    return true
end

function bufferedescape(io, delim, oq, cq, e, newline, quotestrings)
    VALUE_BUFFER = VALUE_BUFFERS[Threads.threadid()]
    n = position(VALUE_BUFFER)
    n == 0 && return
    needtoescape, needtoquote = check(n, delim, oq, cq, newline)
    needtoquote |= quotestrings
    seekstart(VALUE_BUFFER)
    if (needtoquote | needtoescape)
        if needtoescape
            Base.write(io, oq)
            buf = VALUE_BUFFER.data
            for i = 1:n
                @inbounds b = buf[i]
                if b === cq || b === oq
                    Base.write(io, e, b)
                else
                    Base.write(io, b)
                end
            end
            Base.write(io, cq)
        else
            Base.write(io, oq, VALUE_BUFFER, cq)
        end
    else
        Base.write(io, VALUE_BUFFER)
    end
    return
end

function check(n, delim::Char, oq, cq, newline::Char)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    d = delim % UInt8
    new = newline % UInt8
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoquote |= (b === d) | (b === new)
        needtoescape |= b === cq
    end
    return needtoescape, needtoquote
end

function check(n, delim::Char, oq, cq, newline::String)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    d = delim % UInt8
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoquote |= b === d
        needtoescape |= b === cq
    end
    needtoquote |= occursin(newline, String(buf[1:n]))
    return needtoescape, needtoquote
end

function check(n, delim::String, oq, cq, newline::Char)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    new = newline % UInt8
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoquote |= b === new
        needtoescape |= b === cq
    end
    needtoquote |= occursin(delim, String(buf[1:n]))
    return needtoescape, needtoquote
end

function check(n, delim::String, oq, cq, newline::String)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoescape |= b === cq
    end
    str = String(buf[1:n])
    needtoquote |= occursin(delim, str)
    needtoquote |= occursin(newline, str)
    return needtoescape, needtoquote
end

write(file; kwargs...) = x->write(file, x; kwargs...)
function write(file, itr;
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='"',
    newline::Union{Char, String}='\n',
    decimal::Char='.',
    kwargs...)
    (isascii(something(openquotechar, quotechar)) && isascii(something(closequotechar, quotechar)) && isascii(escapechar)) || throw(ArgumentError("quote and escape characters must be ASCII characters "))
    oq, cq = openquotechar !== nothing ? (openquotechar % UInt8, closequotechar % UInt8) : (quotechar % UInt8, quotechar % UInt8)
    e = escapechar % UInt8
    rows = Tables.rows(itr)
    sch = Tables.schema(rows)
    return write(sch, rows, file, oq, cq, e, newline, decimal; kwargs...)
end

function printheader(io, header, delim, oq, cq, e, newline)
    cols = length(header)
    for (col, nm) in enumerate(header)
        bufferedwrite(io, string(nm), nothing, UInt8('.')) && bufferedescape(io, delim, oq, cq, e, newline, false)
        Base.write(io, ifelse(col == cols, newline, delim))
    end
    return
end

function write(sch::Tables.Schema{names}, rows, file, oq, cq, e, newline, decimal;
    delim::Union{Char, String}=',',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    quotestrings::Bool=false,
    kwargs...) where {names}
    colnames = isempty(header) ? names : header
    cols = length(colnames)
    with(file, append) do io
        writeheader && printheader(io, colnames, delim, oq, cq, e, newline)
        for row in rows
            Tables.eachcolumn(sch, row) do val, col, nm
                bufferedwrite(io, coalesce(val, missingstring), dateformat, decimal) && bufferedescape(io, delim, oq, cq, e, newline, quotestrings)
                Base.write(io, ifelse(col == cols, newline, delim))
            end
        end
    end
    return file
end

# handle unknown schema case
function write(::Nothing, rows, file, oq, cq, e, newline, decimal;
    delim::Union{Char, String}=',',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    quotestrings::Bool=false,
    kwargs...)
    state = iterate(rows)
    if state === nothing
        if writeheader && !isempty(header)
            with(file, append) do io
                printheader(io, header, delim, oq, cq, e, newline)
            end
        end
        return file
    end
    row, st = state
    names = isempty(header) ? propertynames(row) : header
    sch = Tables.Schema(names, nothing)
    cols = length(names)
    with(file, append) do io
        writeheader && printheader(io, names, delim, oq, cq, e, newline)
        while true
            Tables.eachcolumn(sch, row) do val, col, nm
                bufferedwrite(io, coalesce(val, missingstring), dateformat, decimal) && bufferedescape(io, delim, oq, cq, e, newline, quotestrings)
                Base.write(io, ifelse(col == cols, newline, delim))
            end
            state = iterate(rows, st)
            state === nothing && break
            row, st = state
        end
    end
    return file
end
