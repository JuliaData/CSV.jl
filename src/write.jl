"""
    CSV.write(file::Union{String, IO}, file; kwargs...) => file
    table |> CSV.write(file::Union{String, IO}; kwargs...) => file

Write a [Tables.jl interface input](https://github.com/JuliaData/Tables.jl) to a csv file, given as an `IO` argument or String representing the file name to write to.

Keyword arguments include:
* `delim::Union{Char, String}=','`: a character or string to print out as the file's delimiter
* `quotechar::Char='"'`: character to use for quoting text fields that may contain delimiters or newlines
* `openquotechar::Char`: instead of `quotechar`, use `openquotechar` and `closequotechar` to support different starting and ending quote characters
* `escapechar::Char='\\'`: character used to escape quote characters in a text field
* `missingstring::String=""`: string to print 
* `dateformat=Dates.default_format(T)`: the date format string to use for printing out Date & DateTime columns
* `append=false`: whether to append writing to an existing file/IO, if `true`, it will not write column names by default
* `writeheader=!append`: whether to write an initial row of delimited column names, not written by default if appending
* `header`: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table
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

function bufferedwrite(io, val::Number, df)
    print(io, val)
    return false
end

getvalue(x, df) = x
getvalue(x::T, df) where {T <: Dates.TimeType} = Dates.format(x, df === nothing ? Dates.default_format(T) : df)

function bufferedwrite(io, val, df)
    VALUE_BUFFER = VALUE_BUFFERS[Threads.threadid()]
    _reset(VALUE_BUFFER)
    print(VALUE_BUFFER, getvalue(val, df))
    return true
end

function bufferedescape(io, delim, oq, cq, e)
    VALUE_BUFFER = VALUE_BUFFERS[Threads.threadid()]
    n = position(VALUE_BUFFER)
    n == 0 && return
    needtoescape, needtoquote = check(n, delim, oq, cq)
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

function check(n, delim::Char, oq, cq)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    d = delim % UInt8
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoquote |= (b === d) | (b === UInt8('\n')) | (b === UInt8('\r'))
        needtoescape |= b === cq
    end
    return needtoescape, needtoquote
end

function check(n, delim::String, oq, cq)
    needtoescape = false
    buf = VALUE_BUFFERS[Threads.threadid()].data
    @inbounds needtoquote = buf[1] === oq
    @simd for i = 1:n
        @inbounds b = buf[i]
        needtoquote |= (b === UInt8('\n')) | (b === UInt8('\r'))
        needtoescape |= b === cq
    end
    needtoquote |= occursin(delim, String(buf[1:n]))
    return needtoescape, needtoquote
end

write(file::Union{String, IO}; kwargs...) = x->write(file, x; kwargs...)
function write(file::Union{String, IO}, itr; kwargs...)
    rows = Tables.rows(itr)
    sch = Tables.schema(rows)
    return write(sch, rows, file; kwargs...)
end

function printheader(io, header, delim, oq, cq, e, df)
    cols = length(header)
    for (col, nm) in enumerate(header)
        bufferedwrite(io, string(nm), nothing) && bufferedescape(io, delim, oq, cq, e)
        Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
    end
    return
end

function write(sch::Tables.Schema{schema_names}, rows, file::Union{String, IO};
    delim::Union{Char, String}=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='\\',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    kwargs...) where {schema_names}
    oq, cq = openquotechar !== nothing ? (openquotechar % UInt8, closequotechar % UInt8) : (quotechar % UInt8, quotechar % UInt8)
    e = escapechar % UInt8
    names = isempty(header) ? schema_names : header
    cols = length(names)
    with(file, append) do io
        writeheader && printheader(io, names, delim, oq, cq, escapechar, dateformat)
        for row in rows
            Tables.eachcolumn(sch, row) do val, col, nm
                bufferedwrite(io, coalesce(val, missingstring), dateformat) && bufferedescape(io, delim, oq, cq, e)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
    end
    return file
end

# handle unknown schema case
function write(::Nothing, rows, file::Union{String, IO};
    delim::Union{Char, String}=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='\\',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    kwargs...)
    oq, cq = openquotechar !== nothing ? (openquotechar % UInt8, closequotechar % UInt8) : (quotechar % UInt8, quotechar % UInt8)
    e = escapechar % UInt8
    state = iterate(rows)
    if state === nothing
        if writeheader && !isempty(header)
            with(file, append) do io
                printheader(io, header, delim, oq, cq, escapechar, dateformat)
            end
        end
        return file
    end
    row, st = state
    names = isempty(header) ? propertynames(row) : header
    sch = Tables.Schema(names, nothing)
    cols = length(names)
    with(file, append) do io
        writeheader && printheader(io, names, delim, oq, cq, escapechar, dateformat)
        while true
            Tables.eachcolumn(sch, row) do val, col, nm
                bufferedwrite(io, coalesce(val, missingstring), dateformat) && bufferedescape(io, delim, oq, cq, e)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
            state = iterate(rows, st)
            state === nothing && break
            row, st = state
        end
    end
    return file
end
