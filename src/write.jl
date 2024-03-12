import Base: Ryu

"""
    CSV.write(file, table; kwargs...) => file
    table |> CSV.write(file; kwargs...) => file

Write a [Tables.jl interface input](https://github.com/JuliaData/Tables.jl) to a csv file, given as an `IO` argument or `String`/FilePaths.jl type representing the file name to write to.
Alternatively, `CSV.RowWriter` creates a row iterator, producing a csv-formatted string for each row in an input table.

Supported keyword arguments include:
* `bufsize::Int=2^22`: The length of the buffer to use when writing each csv-formatted row; default 4MB; if a row is larger than the `bufsize` an error is thrown
* `delim::Union{Char, String}=','`: a character or string to print out as the file's delimiter
* `quotechar::Char='"'`: ascii character to use for quoting text fields that may contain delimiters or newlines
* `openquotechar::Char`: instead of `quotechar`, use `openquotechar` and `closequotechar` to support different starting and ending quote characters
* `escapechar::Char='"'`: ascii character used to escape quote characters in a text field
* `missingstring::String=""`: string to print for `missing` values
* `dateformat=Dates.default_format(T)`: the date format string to use for printing out `Date` & `DateTime` columns
* `append=false`: whether to append writing to an existing file/IO, if `true`, it will not write column names by default
* `compress=false`: compress the written output using standard gzip compression (provided by the CodecZlib.jl package); note that a compression stream can always be provided as the first "file" argument to support other forms of compression; passing `compress=true` is just for convenience to avoid needing to manually setup a GzipCompressorStream
* `writeheader=!append`: whether to write an initial row of delimited column names, not written by default if appending
* `header`: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table
* `newline='\\n'`: character or string to use to separate rows (lines in the csv file)
* `quotestrings=false`: whether to force all strings to be quoted or not
* `decimal='.'`: character to use as the decimal point when writing floating point numbers
* `transform=(col,val)->val`: a function that is applied to every cell e.g. we can transform all `nothing` values to `missing` using `(col, val) -> something(val, missing)`
* `bom=false`: whether to write a UTF-8 BOM header (0xEF 0xBB 0xBF) or not
* `partition::Bool=false`: by passing `true`, the `table` argument is expected to implement `Tables.partitions` and the `file` argument can either be an indexable collection of `IO`, file `String`s, or a single file `String` that will have an index appended to the name

## Examples

```julia
using CSV, Tables, DataFrames

# write out a DataFrame to csv file
df = DataFrame(rand(10, 10), :auto)
CSV.write("data.csv", df)

# write a matrix to an in-memory IOBuffer
io = IOBuffer()
mat = rand(10, 10)
CSV.write(io, Tables.table(mat))
```
"""
function write end

mutable struct Options{D, N, DF, M, TF}
    delim::D
    openquotechar::UInt8
    closequotechar::UInt8
    escapechar::UInt8
    newline::N # Union{UInt8, NTuple{N, UInt8}}
    decimal::UInt8
    dateformat::DF
    quotestrings::Bool
    missingstring::M
    transform::TF  # Function
    bom::Bool
end

tup(x::Char) = x % UInt8
tup(x::AbstractString) = Tuple(codeunits(x))
tlen(::UInt8) = 1
tlen(::NTuple{N, UInt8}) where {N} = N

function Options(;
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
    transform::Function=_identity,
    bom::Bool=false,
    kw...
    )
    checkvaliddelim(delim)
    (isascii(something(openquotechar, quotechar)) && isascii(something(closequotechar, quotechar)) && isascii(escapechar)) || throw(ArgumentError("quote and escape characters must be ASCII characters "))
    oq, cq = openquotechar !== nothing ? (openquotechar % UInt8, closequotechar % UInt8) : (quotechar % UInt8, quotechar % UInt8)
    e = escapechar % UInt8
    return Options(tup(delim), oq, cq, e, tup(newline), decimal % UInt8, dateformat, quotestrings, tup(missingstring), transform, bom)
end

"""
    CSV.RowWriter(table; kwargs...)

Creates an iterator that produces csv-formatted strings for each row in the input table.

Supported keyword arguments include:
* `bufsize::Int=2^22`: The length of the buffer to use when writing each csv-formatted row; default 4MB; if a row is larger than the `bufsize` an error is thrown
* `delim::Union{Char, String}=','`: a character or string to print out as the file's delimiter
* `quotechar::Char='"'`: ascii character to use for quoting text fields that may contain delimiters or newlines
* `openquotechar::Char`: instead of `quotechar`, use `openquotechar` and `closequotechar` to support different starting and ending quote characters
* `escapechar::Char='"'`: ascii character used to escape quote characters in a text field
* `missingstring::String=""`: string to print for `missing` values
* `dateformat=Dates.default_format(T)`: the date format string to use for printing out `Date` & `DateTime` columns
* `header`: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table
* `newline='\\n'`: character or string to use to separate rows (lines in the csv file)
* `quotestrings=false`: whether to force all strings to be quoted or not
* `decimal='.'`: character to use as the decimal point when writing floating point numbers
* `transform=(col,val)->val`: a function that is applied to every cell e.g. we can transform all `nothing` values to `missing` using `(col, val) -> something(val, missing)`
* `bom=false`: whether to write a UTF-8 BOM header (0xEF 0xBB 0xBF) or not
"""
struct RowWriter{T, S, O}
    source::T
    schema::S
    options::O
    buf::Vector{UInt8}
    header::Vector
    writeheader::Bool
end

Base.IteratorSize(::Type{RowWriter{T, S, O}}) where {T, S, O} = Base.IteratorSize(T)
Base.length(r::RowWriter) = length(r.source) + r.writeheader
Base.size(r::RowWriter) = (length(r.source) + r.writeheader,)
Base.eltype(r::RowWriter) = String

_identity(col, val) = val

function RowWriter(table;
    header::Vector=String[],
    writeheader::Bool=true,
    bufsize::Int=2^22,
    kw...)
    opts = Options(; kw...)
    source = Tables.rows(table)
    sch = Tables.schema(source)
    return RowWriter(source, sch, opts, Vector{UInt8}(undef, bufsize), header, writeheader)
end

struct DummyIO <: IO end
Base.write(io::DummyIO, a::SubArray{T,N,<:Array}) where {T,N} = error("`bufsize` for `CSV.RowWriter` was too small (default 4MB); try again passing a larger value for `bufsize`")

# first iteration produces column names
function Base.iterate(r::RowWriter)
    state = iterate(r.source)
    state === nothing && return nothing
    row, st = state
    colnames = isempty(r.header) ? Tables.columnnames(row) : r.header
    pos = 1
    if r.options.bom
        pos = writebom(r.buf, pos, length(r.buf))
    end
    cols = length(colnames)
    !r.writeheader && return iterate(r, (state, cols))
    pos = writenames(r.buf, pos, length(r.buf), DummyIO(), colnames, cols, r.options)
    return unsafe_string(pointer(r.buf), pos - 1), (state, cols)
end

function Base.iterate(r::RowWriter, (state, cols))
    state === nothing && return nothing
    row, st = state
    ref = Ref{Int}(1)
    writerow(r.buf, ref, length(r.buf), DummyIO(), r.schema, row, cols, r.options)
    return unsafe_string(pointer(r.buf), ref[] - 1), (iterate(r.source, st), cols)
end

write(file; kwargs...) = x->write(file, x; kwargs...)
function write(file, itr;
    append::Bool=false,
    compress::Bool=false,
    writeheader=nothing,
    partition::Bool=false,
    kw...)
    if writeheader !== nothing
        Base.depwarn("`writeheader=$writeheader` is deprecated in favor of `header=$writeheader`", :write)
        header = writeheader
    else
        header = !append
    end
    opts = Options(; kw...)
    if partition
        if file isa IO
            throw(ArgumentError("must pass single file name as a String, or iterable of filenames or IO arguments"))
        end
        outfiles = file isa String ? String[] : file
        @sync for (i, part) in enumerate(Tables.partitions(itr))
            @static if VERSION >= v"1.3-DEV"
                Threads.@spawn begin
                    if file isa String
                        push!(outfiles, string(file, "_$i"))
                    end
                    write(outfiles[i], part; append=append, compress=compress, writeheader=writeheader, partition=false, kw...)
                end
            else
                if file isa String
                    push!(outfiles, string(file, "_$i"))
                end
                write(outfiles[i], part; append=append, compress=compress, writeheader=writeheader, partition=false, kw...)
            end
        end
        return outfiles
    else
        rows = Tables.rows(itr)
        sch = Tables.schema(rows)
        return write(sch, rows, file, opts; append=append, compress=compress, header=header, kw...)
    end
end

function write(sch::Tables.Schema, rows, file, opts;
        append::Bool=false,
        compress::Bool=false,
        header::Union{Bool, Vector}=String[],
        bufsize::Int=2^22,
        kw...
    )
    colnames = !(header isa Vector) || isempty(header) ? sch.names : header
    cols = length(colnames)
    len = bufsize
    buf = Vector{UInt8}(undef, len)
    pos = 1
    with(file, append, compress) do io
        Base.@_inline_meta
        if !append && opts.bom
            pos = writebom(buf, pos, len)
        end
        if header === true || (header isa Vector && !append)
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
        compress::Bool=false,
        header::Union{Bool, Vector}=String[],
        bufsize::Int=2^22,
        kw...
    )
    len = bufsize
    buf = Vector{UInt8}(undef, len)
    pos = 1
    state = iterate(rows)
    if state === nothing
        if header isa Vector && !isempty(header)
            with(file, append, compress) do io
                !append && opts.bom && (pos = writebom(buf, pos, len) )
                pos = writenames(buf, pos, len, io, header, length(header), opts)
                Base.write(io, resize!(buf, pos - 1))
            end
        end
        return file
    end
    row, st = state
    names = header isa Bool || isempty(header) ? Tables.columnnames(row) : header
    sch = Tables.Schema(Tables.columnnames(row), nothing)
    cols = length(names)
    with(file, append, compress) do io
        if !append && opts.bom
            pos = writebom(buf, pos, len)
        end
        if header === true || (header isa Vector && !append)
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

_seekstart(io::T) where {T <: IO} = hasmethod(seek, Tuple{T, Integer}) ? seekstart(io) : nothing

function with(f::Function, @nospecialize(io), append, compress)
    needtoclose = false
    if io isa Union{Base.TTY, Base.Pipe, Base.PipeEndpoint, Base.DevNull}
        # pass, can't seek these
    elseif io isa IO
        !append && _seekstart(io)
    else
        io = open(io, append ? "a" : "w")
        needtoclose = true
    end
    if compress
        io = GzipCompressorStream(io)
        needtoclose = true
    end
    try
        return f(io)
    finally
        needtoclose && close(io)
    end
end

@noinline buffertoosmall(pos, len) = throw(ArgumentError("row size ($pos) too large for writing buffer ($len), pass a larger value to `bufsize` keyword argument"))

macro check(n)
    esc(quote
        $n > length(buf) && buffertoosmall(pos + $n - 1, length(buf))
        if (pos + $n - 1) > len
            Base.write(io, view(buf, 1:(pos - 1)))
            pos = 1
        end
    end)
end

function writebom(buf, pos, len)
    @check 3
    @inbounds buf[pos] = 0xEF
    @inbounds buf[pos+1] = 0xBB
    @inbounds buf[pos+2] = 0xBF
    return pos + 3
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
        pos = writecell(buf, pos, len, io, String(nm), opts)
        pos = writedelimnewline(buf, pos, len, io, ifelse(col == cols, opts.newline, opts.delim))
    end
    return pos
end

@noinline nothingerror(col) = error(
    """
    A `nothing` value was found in column $col and it is not a printable value.
    There are several ways to handle this situation:
    1) fix the data, perhaps replace `nothing` with `missing`,
    2) use `transform` option with a function to replace `nothing` with whatever value (including `missing`), like `CSV.write(...; transform=(col, val) -> something(val, missing))` or
    3) use `TableOperations.transform` option to transform specific columns
    """)

writerow(buf, pos, len, io, ::Nothing, row, cols, opts) =
    writerow(buf, pos, len, io, Tables.Schema(Tables.columnnames(row), nothing), row, cols, opts)

function writerow(buf, pos, len, io, sch, row, cols, opts)
    n, d = opts.newline, opts.delim
    Tables.eachcolumn(sch, row) do val, col, nm
        Base.@_inline_meta
        val = opts.transform(col, val)
        val === nothing && nothingerror(col)
        posx = writecell(buf, pos[], len, io, val, opts)
        pos[] = writedelimnewline(buf, posx, len, io, ifelse(col == cols, n, d))
    end
    return
end

function writerow(io::IO, row; opts::Union{Options, Nothing}=nothing, bufsize::Int=2^22, buf=Vector{UInt8}(undef, bufsize), len=bufsize, kw...)
    if opts === nothing
        opts = Options(; kw...)
    end
    nms = Tables.columnnames(row)
    pos = 1
    for i = 1:length(nms)
        val = opts.transform(i, Tables.getcolumn(row, i))
        val === nothing && nothingerror(col)
        pos = writecell(buf, pos, len, io, val, opts)
        pos = writedelimnewline(buf, pos, len, io, ifelse(i == length(nms), opts.newline, opts.delim))
    end
    Base.write(io, resize!(buf, pos - 1))
end

function writerow(row; opts::Union{Options, Nothing}=nothing, bufsize::Int=2^22, buf=Vector{UInt8}(undef, bufsize), len=bufsize, kw...)
    if opts === nothing
        opts = Options(; kw...)
    end
    io = DummyIO()
    nms = Tables.columnnames(row)
    pos = 1
    for i = 1:length(nms)
        val = opts.transform(i, Tables.getcolumn(row, i))
        val === nothing && nothingerror(col)
        pos = writecell(buf, pos, len, io, val, opts)
        pos = writedelimnewline(buf, pos, len, io, ifelse(i == length(nms), opts.newline, opts.delim))
    end
    return unsafe_string(pointer(buf), pos - 1)
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

function writecell(buf, pos, len, io, x::Integer, opts)
    if x < 0
        x *= -1
        @check 1
        @inbounds buf[pos] = UInt8('-')
        pos += 1
    end
    n = i = ndigits(x)
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
    if opts.decimal != UInt8('.')
        for i = 1:sz
            @inbounds buf[pos] = bytes[i] == UInt8('.') ? opts.decimal : bytes[i]
            pos += 1
        end
    else
        for i = 1:sz
            @inbounds buf[pos] = bytes[i]
            pos += 1
        end
    end
    return pos
end

function writecell(buf, pos, len, io, x::T, opts) where {T <: Base.IEEEFloat}
    @check Ryu.neededdigits(T)
    return Ryu.writeshortest(buf, pos, x, false, false, true, -1, UInt8('e'), false, opts.decimal)
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
    isempty(bytes) && return false, false
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
        if !isempty(newline) && b == newline[j]
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
        if !isempty(newline) && b == newline[j]
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
