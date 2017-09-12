@enum ParsingState None Delimiter EOF Newline

# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a null field
# also return `b` which is last byte read
macro checknullstart()
    return esc(quote
        state = None
        eof(io) && (state = EOF; @goto null)
        b = readbyte(io)
        while b != opt.delim && (b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar)
            eof(io) && (state = EOF; @goto null)
            b = readbyte(io)
        end
        if b == opt.delim
            state = Delimiter
            @goto null
        elseif b == NEWLINE
            state = Newline
            @goto null
        elseif b == RETURN
            state = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            @goto null
        end
    end)
end

# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
macro checkdone(label)
    return esc(quote
        b == opt.quotechar && !eof(io) && (b = readbyte(io))
        if b == opt.delim
            state = Delimiter
            @goto $label
        elseif b == NEWLINE
            state = Newline
            @goto $label
        elseif b == RETURN
            state = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            @goto $label
        elseif b == opt.quotechar && eof(io)
            state = EOF
            @goto $label
        elseif b == CSV.SPACE || b == CSV.TAB
            # trailing whitespace
            while !eof(io) && (b == CSV.SPACE || b == CSV.TAB)
                b = readbyte(io)
            end
            if b == opt.delim
                state = Delimiter
                @goto $label
            elseif b == NEWLINE
                state = Newline
                @goto $label
            elseif b == RETURN
                state = Newline
                !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
                @goto $label
            elseif eof(io)
                state = EOF
                @goto $label
            end
        end
    end)
end

ParsingException(::Type{T}, b, row, col) where {T} = CSV.ParsingException("error parsing a `$T` value on column $col, row $row; encountered '$(Char(b))'")

# as a last ditch effort, after we've trying parsing the correct type,
# we check if the field is equal to a custom null type
# otherwise we give up and throw an error
macro checknullend()
    return esc(quote
        !opt.nullcheck && @goto error
        i = 1
        while true
            b == opt.null[i] || @goto error
            (i == length(opt.null) || eof(io)) && break
            b = readbyte(io)
            i += 1
        end
        if !eof(io)
            b = readbyte(io)
            @checkdone(null)
        end
        state = EOF
        @goto null
    end)
end
"""
`CSV.parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)` => `Nullable{T}`
`CSV.parsefield{T}(s::CSV.Source, ::Type{T}, row=0, col=0)` => `Nullable{T}``

`io` is an `IO` type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell)
leading whitespace is ignored for Integer and Float types.
returns a `Nullable{T}` saying whether the field contains a null value or not (empty field, missing value)
field is null if the next delimiter or newline is encountered before any other characters.
Specialized methods exist for Integer, Float, String, Date, and DateTime.
For other types `T`, a generic fallback requires `parse(T, str::String)` to be defined.
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field
`opt.null` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
For numeric fields, if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown

The second method of `CSV.parsefield` operates on a `CSV.Source` directly allowing for easy usage when writing custom parsing routines.
Do note, however, that the `row` and `col` arguments are for error-reporting purposes only. A `CSV.Source` maintains internal state with
regards to the underlying data buffer and can **only** parse fields sequentially. This means that `CSV.parsefield` needs to be called somewhat like:

```julia
source = CSV.Source(file)

types = Data.types(source)

for col = 1:length(types)
    println(get(CSV.parsefield(source, types[col]), "\"\""))
end
```
"""
function parsefield end

const NULLTHROW = (row, col)->throw(Data.NullException("encountered a null value for a non-null column type on row = $row, col = $col"))
const NULLRETURN = (row, col)->null

parsefield(source::CSV.Source, ::Type{T}, row=0, col=0) where {T} = CSV.parsefield(source.io, T, source.options, row, col, NULLTHROW)
parsefield(source::CSV.Source, ::Type{Union{T, Null}}, row=0, col=0) where {T} = CSV.parsefield(source.io, T, source.options, row, col, NULLRETURN)

@inline parsefield(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0) where {T} = parsefield(io, T, opt, row, col, NULLTHROW)
@inline parsefield(io::IO, ::Type{Union{T, Null}}, opt::CSV.Options=CSV.Options(), row=0, col=0) where {T} = parsefield(io, T, opt, row, col, NULLRETURN)

@inline function parsefield(io::IO, ::Type{T}, opt::CSV.Options, row, col, ifnull::Function) where {T <: Integer}
    @checknullstart()
    v = zero(T)
    negative = false
    if b == MINUS # check for leading '-' or '+'
        c = peekbyte(io) 
        if NEG_ONE < c < TEN
            negative = true
            b = readbyte(io)
        end
    elseif b == PLUS
        c = peekbyte(io) 
        if NEG_ONE < c < TEN
            b = readbyte(io)
        end
    end
    while NEG_ONE < b < TEN
        # process digits
        v, ov_mul = Base.mul_with_overflow(v, T(10))
        v, ov_add = Base.add_with_overflow(v, T(b - ZERO))
        (ov_mul | ov_add) && throw(OverflowError("overflow parsing $T, parsed $v"))
        eof(io) && (state = EOF; @goto done)
        b = readbyte(io)
    end
    @checkdone(done)
    @checknullend()

    @label done
    return ifelse(negative, -v, v)

    @label null
    return ifnull(row, col)

    @label error
    throw(ParsingException(T, b, row, col))
end

const BUF = IOBuffer()

getptr(io::IO) = C_NULL
getptr(io::IOBuffer) = pointer(io.data, io.ptr)
incr(io::IO, b) = Base.write(BUF, b)
incr(io::IOBuffer, b) = 1

make(io::IOBuffer, ::Type{WeakRefString{UInt8}}, ptr, len) = WeakRefString(Ptr{UInt8}(ptr), len)
make(io::IOBuffer, ::Type{String}, ptr, len) = unsafe_string(ptr, len)
make(io::IO, ::Type{WeakRefString{UInt8}}, ptr, len) = String(take!(BUF))
make(io::IO, ::Type{String}, ptr, len) = String(take!(BUF))

@inline function parsefield(io::IO, T::Type{<:AbstractString}, opt::CSV.Options, row, col, ifnull::Function)
    eof(io) && (state = EOF; @goto null)
    ptr = getptr(io)
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    n = opt.null
    q = opt.quotechar
    e = opt.escapechar
    d = opt.delim
    nulllen = length(n)
    @inbounds while !eof(io)
        b = readbyte(io)
        if b == q
            ptr += 1
            while !eof(io)
                b = readbyte(io)
                if b == e
                    if eof(io)
                        break
                    elseif e == q && peekbyte(io) != q
                        break
                    end
                    len += incr(io, b)
                    b = readbyte(io)
                elseif b == q
                    break
                end
                (nullcheck && len+1 <= nulllen && b == n[len+1]) || (nullcheck = false)
                len += incr(io, b)
            end
        elseif b == d
            state = Delimiter
            break
        elseif b == NEWLINE
            state = Newline
            break
        elseif b == RETURN
            state = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            break
        else
            (nullcheck && len+1 <= nulllen && b == n[len+1]) || (nullcheck = false)
            len += incr(io, b)
        end
    end
    eof(io) && (state = EOF)
    (len == 0 || nullcheck) && @goto null
    return make(io, T, ptr, len)

    @label null
    take!(BUF)
    return ifnull(row, col)
end

@inline function parsefield(io::IO, ::Type{Date}, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, WeakRefString{UInt8}, opt, row, col, ifnull)
    return v isa Null ? ifnull(row, col) : Date(v, opt.dateformat)
end
@inline function parsefield(io::IO, ::Type{DateTime}, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, WeakRefString{UInt8}, opt, row, col, ifnull)
    return v isa Null ? ifnull(row, col) : DateTime(v, opt.dateformat)
end

@inline function parsefield(io::IO, ::Type{Char}, opt::CSV.Options, row, col, ifnull::Function)
    @checknullstart()
    c = b
    eof(io) && (state = EOF; @goto done)
    opt.nullcheck && b == opt.null[1] && @goto null
    b = readbyte(io)
    @checkdone(done)
    @checknullend()

    @label done
    return Char(c)

    @label null
    return ifnull(row, col)

    @label error
    throw(ParsingException(Char, b, row, col))
end

# Generic fallback
@inline function parsefield(io::IO, T, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, String, opt, row, col, ifnull)
    return v isa Null ? ifnull(row, col) : parse(T, v)
end