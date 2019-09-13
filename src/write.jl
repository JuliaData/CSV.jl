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

mutable struct Options{D, N, DF, M}
    delim::D
    openquotechar::UInt8
    closequotechar::UInt8
    escapechar::UInt8
    newline::N # Union{UInt8, NTuple{N, UInt8}}
    decimal::UInt8
    dateformat::DF
    quotestrings::Bool
    missingstring::M
end

tup(x::Char) = x % UInt8
tup(x::AbstractString) = Tuple(codeunits(x))
tlen(::UInt8) = 1
tlen(::NTuple{N, UInt8}) where {N} = N

write(file; kwargs...) = x->write(file, x; kwargs...)
function write(file, itr;
    delim::Union{Char, String}=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='"',
    newline::Union{Char, String}='\n',
    decimal::Char='.',
    dateformat=nothing,
    quotestrings::Bool=false,
    missingstring::AbstractString="",
    kwargs...)
    checkvaliddelim(delim)
    (isascii(something(openquotechar, quotechar)) && isascii(something(closequotechar, quotechar)) && isascii(escapechar)) || throw(ArgumentError("quote and escape characters must be ASCII characters "))
    oq, cq = openquotechar !== nothing ? (openquotechar % UInt8, closequotechar % UInt8) : (quotechar % UInt8, quotechar % UInt8)
    e = escapechar % UInt8
    opts = Options(tup(delim), oq, cq, e, tup(newline), decimal % UInt8, dateformat, quotestrings, tup(missingstring))
    rows = Tables.rows(itr)
    sch = Tables.schema(rows)
    return write(sch, rows, file, opts; kwargs...)
end

function write(sch::Tables.Schema{names}, rows, file, opts;
        append::Bool=false,
        writeheader::Bool=!append,
        header::Vector=String[],
    ) where {names}
    colnames = isempty(header) ? names : header
    cols = length(colnames)
    len = 2^22
    buf = Vector{UInt8}(undef, len)
    pos = 1
    with(file, append) do io
        Base.@_inline_meta
        if writeheader
            pos = writenames(buf, pos, len, io, colnames, cols, opts)
        end
        ref = Ref{Int}(pos)
        for row in rows
            writerow(buf, ref, len, io, sch, row, cols, opts)
        end
        Base.write(io, resize!(buf, ref[] - 1))
    end
    return file
end

# handle unknown schema case
function write(::Nothing, rows, file, opts;
        append::Bool=false,
        writeheader::Bool=!append,
        header::Vector=String[],
    )
    len = 2^22
    buf = Vector{UInt8}(undef, len)
    pos = 1
    state = iterate(rows)
    if state === nothing
        if writeheader && !isempty(header)
            with(file, append) do io
                pos = writenames(buf, pos, len, io, header, length(header), opts)
                Base.write(io, resize!(buf, pos - 1))
            end
        end
        return file
    end
    row, st = state
    names = isempty(header) ? propertynames(row) : header
    sch = Tables.Schema(names, nothing)
    cols = length(names)
    with(file, append) do io
        if writeheader
            pos = writenames(buf, pos, len, io, names, cols, opts)
        end
        ref = Ref{Int}(pos)
        while true
            writerow(buf, ref, len, io, sch, row, cols, opts)
            state = iterate(rows, st)
            state === nothing && break
            row, st = state
        end
        Base.write(io, resize!(buf, ref[] - 1))
    end
    return file
end

_seekstart(p::Base.Process) = return
_seekstart(io::IO) = seekstart(io)

@inline function with(f::Function, io::IO, append)
    !append && _seekstart(io)
    f(io)
end

function with(f::Function, io::Union{Base.TTY, Base.Pipe, Base.PipeEndpoint, Base.DevNull}, append)
    # seeking in an unbuffered pipe makes no sense...
    f(io)
end

function with(f::Function, file::String, append)
    open(file, append ? "a" : "w") do io
        f(io)
    end
end

macro check(n)
    esc(quote
        if (pos + $n - 1) > len
            Base.write(io, view(buf, 1:(pos - 1)))
            pos = 1
        end
    end)
end

function writedelimnewline(buf, pos, len, io, x::UInt8)
    @check 1
    @inbounds buf[pos] = x
    return pos + 1
end

function writedelimnewline(buf, pos, len, io, x)
    @check tlen(x)
    for i = 1:tlen(x)
        @inbounds buf[pos] = x[i]
        pos += 1
    end
    return pos
end

function writenames(buf, pos, len, io, header, cols, opts)
    cols = length(header)
    for (col, nm) in enumerate(header)
        pos = writecell(buf, pos, len, io, nm, opts)
        pos = writedelimnewline(buf, pos, len, io, ifelse(col == cols, opts.newline, opts.delim))
    end
    return pos
end

function writerow(buf, pos, len, io, sch, row, cols, opts)
    # ref = Ref{Int}(pos)
    n, d = opts.newline, opts.delim
    Tables.eachcolumn(sch, row, pos) do val, col, nm, pos
        Base.@_inline_meta
        posx = writecell(buf, pos[], len, io, val, opts)
        pos[] = writedelimnewline(buf, posx, len, io, ifelse(col == cols, n, d))
    end
    return
end

function writecell(buf, pos, len, io, ::Missing, opts)
    str = opts.missingstring
    t = tlen(str)
    t == 0 && return pos
    @check t
    Base.@nexprs 12 i -> begin
        buf[pos] = str[i]
        pos += 1
        (i + 1) > t && return pos
    end
    for i = 13:t
        buf[pos] = str[i]
        pos += 1
    end
    return pos
end

function writecell(buf, pos, len, io, x::Bool, opts)
    @inbounds if x
        @check 4
        buf[pos] = UInt8('t')
        buf[pos + 1] = UInt8('r')
        buf[pos + 2] = UInt8('u')
        buf[pos + 3] = UInt8('e')
        pos += 4
    else
        @check 5
        buf[pos] = UInt8('f')
        buf[pos + 1] = UInt8('a')
        buf[pos + 2] = UInt8('l')
        buf[pos + 3] = UInt8('s')
        buf[pos + 4] = UInt8('e')
        pos += 5
    end
    return pos
end

function writecell(buf, pos, len, io, y::Integer, opts)
    x, neg = Base.split_sign(y)
    if neg
        @inbounds buf[pos] = UInt8('-')
        pos += 1
    end
    n = i = ndigits(x, base=10, pad=1)
    @check i
    while i > 0
        @inbounds buf[pos + i - 1] = 48 + rem(x, 10)
        x = oftype(x, div(x, 10))
        i -= 1
    end
    return pos + n
end

function writecell(buf, pos, len, io, x::AbstractFloat, opts)
    bytes = codeunits(string(x))
    sz = sizeof(bytes)
    @check sz
    for i = 1:sz
        @inbounds buf[pos] = bytes[i]
        pos += 1
    end
    return pos
end

function writecell(buf, pos, len, io, x::T, opts) where {T <: Base.IEEEFloat}
    @check Parsers.neededdigits(T)
    return Parsers.writeshortest(buf, pos, x, false, false, true, -1, UInt8('e'), false, opts.decimal)
end

getvalue(x::T, df) where {T <: Dates.TimeType} = Dates.format(x, df === nothing ? Dates.default_format(T) : df)

function writecell(buf, pos, len, io, x::Dates.TimeType, opts)
    bytes = codeunits(getvalue(x, opts.dateformat))
    @check sizeof(bytes)
    for i = 1:sizeof(bytes)
        @inbounds buf[pos] = bytes[i]
        pos += 1
    end
    return pos
end

function writecell(buf, pos, len, io, x::Symbol, opts)
    ptr = Base.unsafe_convert(Ptr{UInt8}, x)
    sz = ccall(:strlen, Csize_t, (Cstring,), ptr)
    @check sz
    for i = 1:sz
        @inbounds buf[pos] = unsafe_load(ptr, i)
        pos += 1
    end
    return pos
end

# generic fallback; convert to string
writecell(buf, pos, len, io, x, opts) =
    writecell(buf, pos, len, io, Base.string(x), opts)

writecell(buf, pos, len, io, x::CategoricalString, opts) =
    writecell(buf, pos, len, io, Base.string(x), opts)

function writecell(buf, pos, len, io, x::AbstractString, opts)
    bytes = codeunits(x)
    sz = sizeof(bytes)
    # check if need to escape
    needtoescape, needtoquote = check(bytes, sz, opts.delim, opts.openquotechar, opts.closequotechar, opts.newline)
    needtoquote |= needtoescape
    needtoquote |= opts.quotestrings
    @check (sz + (needtoquote * 2) + (needtoescape * 2 * sz))
    if needtoquote
        @inbounds buf[pos] = opts.openquotechar
        pos += 1
    end
    if needtoescape
        oq, cq, e = opts.openquotechar, opts.closequotechar, opts.escapechar
        for i = 1:sz
            @inbounds b = bytes[i]
            if b == cq || b == oq
                @inbounds buf[pos] = e
                pos += 1
            end
            @inbounds buf[pos] = b
            pos += 1
        end
    else
        for i = 1:sz
            @inbounds buf[pos] = bytes[i]
            pos += 1
        end
    end
    if needtoquote
        @inbounds buf[pos] = opts.closequotechar
        pos += 1
    end
    return pos
end

function check(bytes, sz, delim::UInt8, oq, cq, newline::UInt8)
    needtoescape = false
    @inbounds needtoquote = bytes[1] == oq
    @simd for i = 1:sz
        @inbounds b = bytes[i]
        needtoquote |= (b == delim) | (b == newline)
        needtoescape |= b == cq
    end
    return needtoescape, needtoquote
end

function check(bytes, sz, delim::UInt8, oq, cq, newline::Tuple)
    needtoescape = false
    @inbounds needtoquote = bytes[1] == oq
    j = 1
    for i = 1:sz
        @inbounds b = bytes[i]
        needtoquote |= b == delim
        needtoescape |= b == cq
        if b == newline[j]
            j += 1
            if j > length(newline)
                needtoquote = true
                j = 1
            end
        else
            j = 1
        end
    end
    return needtoescape, needtoquote
end

function check(bytes, sz, delim::Tuple, oq, cq, newline::UInt8)
    needtoescape = false
    @inbounds needtoquote = bytes[1] == oq
    j = 1
    for i = 1:sz
        @inbounds b = bytes[i]
        needtoquote |= b == newline
        needtoescape |= b == cq
        if b == delim[j]
            j += 1
            if j > length(delim)
                needtoquote = true
                j = 1
            end
        else
            j = 1
        end
    end
    return needtoescape, needtoquote
end

function check(bytes, sz, delim::Tuple, oq, cq, newline::Tuple)
    needtoescape = false
    @inbounds needtoquote = bytes[1] === oq
    j = 1
    k = 1
    @simd for i = 1:sz
        @inbounds b = bytes[i]
        needtoescape |= b == cq
        if b == newline[j]
            j += 1
            if j > length(newline)
                needtoquote = true
                j = 1
            end
        else
            j = 1
        end
        if b == delim[k]
            k += 1
            if k > length(delim)
                needtoquote = true
                k = 1
            end
        else
            k = 1
        end
    end
    return needtoescape, needtoquote
end
