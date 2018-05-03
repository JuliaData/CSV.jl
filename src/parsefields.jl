@enum ParsingState None Delimiter EOF Newline
const P = Ref{ParsingState}

# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a missing field
# also return `b` which is last byte read
macro checkmissingstart()
    return esc(quote
        state[] = None
        eof(io) && (state[] = EOF; @goto missing)
        b = readbyte(io)
        while b != opt.delim && (b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar)
            eof(io) && (state[] = EOF; @goto missing)
            b = readbyte(io)
       end
        if b == opt.delim
            state[] = Delimiter
            @goto missing
        elseif b == NEWLINE
            state[] = Newline
            @goto missing
        elseif b == RETURN
            state[] = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            @goto missing
        end
    end)
end

# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
macro checkdone(label)
    return esc(quote
        b == opt.quotechar && !eof(io) && (b = readbyte(io))
        if b == opt.delim
            state[] = Delimiter
            @goto $label
        elseif b == NEWLINE
            state[] = Newline
            @goto $label
        elseif b == RETURN
            state[] = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            @goto $label
        elseif b == opt.quotechar && eof(io)
            state[] = EOF
            @goto $label
        elseif b == CSV.SPACE || b == CSV.TAB
            # trailing whitespace
            while !eof(io) && (b == CSV.SPACE || b == CSV.TAB)
                b = readbyte(io)
            end
            if b == opt.delim
                state[] = Delimiter
                @goto $label
            elseif b == NEWLINE
                state[] = Newline
                @goto $label
            elseif b == RETURN
                state[] = Newline
                !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
                @goto $label
            elseif eof(io)
                state[] = EOF
                @goto $label
            end
        end
    end)
end

ParsingException(::Type{T}, b, row, col) where {T} = CSV.ParsingException("error parsing a `$T` value on column $col, row $row; encountered '$(Char(b))'")

# as a last ditch effort, after we've trying parsing the correct type,
# we check if the field is equal to a custom missing type
# otherwise we give up and throw an error
macro checkmissingend()
    return esc(quote
        !opt.missingcheck && @goto error
        i = 1
        while true
            b == opt.missingstring[i] || @goto error
            (i == length(opt.missingstring) || eof(io)) && break
            b = readbyte(io)
            i += 1
        end
        if !eof(io)
            b = readbyte(io)
            @checkdone(missing)
        end
        state[] = EOF
        @goto missing
    end)
end
"""
`CSV.parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)` => `Union{T, Missing}`
`CSV.parsefield{T}(s::CSV.Source, ::Type{T}, row=0, col=0)` => `Union{T, Missing}``

`io` is an `IO` type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell)
leading whitespace is ignored for Integer and Float types.
Specialized methods exist for Integer, Float, String, Date, and DateTime.
For other types `T`, a generic fallback requires `parse(T, str::String)` to be defined.
the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a missing field
`opt.missingstring` is also checked if there is a custom value provided (i.e. "NA", "\\N", etc.)
For numeric fields, if field is non-missing and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown

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

const MISSINGTHROW = (row, col)->throw(Missings.MissingException("encountered a missing value for a column type which does not support them on row = $row, col = $col"))
const MISSINGRETURN = (row, col)->missing

parsefield(source::CSV.Source, ::Type{T}, row=0, col=0, state::P=P()) where {T} = CSV.parsefield(source.io, T, source.options, row, col, state, T !== Missing ? MISSINGTHROW : MISSINGRETURN)
parsefield(source::CSV.Source, ::Type{Union{T, Missing}}, row=0, col=0, state::P=P()) where {T} = CSV.parsefield(source.io, T, source.options, row, col, state, MISSINGRETURN)
parsefield(source::CSV.Source, ::Type{Missing}, row=0, col=0, state::P=P()) = CSV.parsefield(source.io, WeakRefString{UInt8}, source.options, row, col, state, MISSINGRETURN)

@inline parsefield(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::P=P()) where {T} = parsefield(io, T, opt, row, col, state, T !== Missing ? MISSINGTHROW : MISSINGRETURN)
@inline parsefield(io::IO, ::Type{Union{T, Missing}}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::P=P()) where {T} = parsefield(io, T, opt, row, col, state, MISSINGRETURN)
@inline parsefield(io::IO, ::Type{Missing}, opt::CSV.Options=CSV.Options(), row=0, col=0, state::P=P()) = parsefield(io, WeakRefString{UInt8}, opt, row, col, state, MISSINGRETURN)

function parsefield(io::IO, ::Type{T}, opt::CSV.Options, row, col, state, ifmissing::Function) where {T <: Integer}
    @checkmissingstart()
    v = zero(T)
    negative = false
    if b == MINUS # check for leading '-' or '+'
        if !eof(io)
            c = peekbyte(io)
            if NEG_ONE < c < TEN
                negative = true
                b = readbyte(io)
	    end
        end
    elseif b == PLUS
        if !eof(io)
            c = peekbyte(io)
            if NEG_ONE < c < TEN
                b = readbyte(io)
	    end
        end
    end
    while NEG_ONE < b < TEN
        # process digits
        v, ov_mul = Base.mul_with_overflow(v, T(10))
        v, ov_add = Base.add_with_overflow(v, T(b - ZERO))
        (ov_mul | ov_add) && throw(OverflowError("overflow parsing $T, parsed $v"))
        eof(io) && (state[] = EOF; @goto done)
        b = readbyte(io)
    end
    @checkdone(done)
    @checkmissingend()

    @label done
    return ifelse(negative, -v, v)

    @label missing
    return ifmissing(row, col)

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

function parsefield(io::IO, T::Type{<:AbstractString}, opt::CSV.Options, row, col, state, ifmissing::Function)
    eof(io) && (state[] = EOF; @goto missing)
    ptr = getptr(io)
    len = 0
    missingcheck = opt.missingcheck # if missingstring is "", then we don't need to byte match it
    n = opt.missingstring
    q = opt.quotechar
    e = opt.escapechar
    d = opt.delim
    missinglen = length(n)
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
                (missingcheck && len+1 <= missinglen && b == n[len+1]) || (missingcheck = false)
                len += incr(io, b)
            end
        elseif b == d
            state[] = Delimiter
            break
        elseif b == NEWLINE
            state[] = Newline
            break
        elseif b == RETURN
            state[] = Newline
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            break
        else
            (missingcheck && len+1 <= missinglen && b == n[len+1]) || (missingcheck = false)
            len += incr(io, b)
        end
    end
    eof(io) && (state[] = EOF)
    (len == 0 || missingcheck) && @goto missing
    return make(io, T, ptr, len)

    @label missing
    take!(BUF)
    return ifmissing(row, col)
end

function parsefield(io::IO, ::Type{Date}, opt::CSV.Options, row, col, state, ifmissing::Function)
    v = parsefield(io, String, opt, row, col, state, ifmissing)
    return v isa Missing ? ifmissing(row, col) : Date(v, opt.dateformat)
end
function parsefield(io::IO, ::Type{DateTime}, opt::CSV.Options, row, col, state, ifmissing::Function)
    v = parsefield(io, String, opt, row, col, state, ifmissing)
    return v isa Missing ? ifmissing(row, col) : DateTime(v, opt.dateformat)
end

function parsefield(io::IO, ::Type{Char}, opt::CSV.Options, row, col, state, ifmissing::Function)
    @checkmissingstart()
    c = b
    eof(io) && (state[] = EOF; @goto done)
    opt.missingcheck && b == opt.missingstring[1] && @goto missing
    b = readbyte(io)
    @checkdone(done)
    @checkmissingend()

    @label done
    return Char(c)

    @label missing
    return ifmissing(row, col)

    @label error
    throw(ParsingException(Char, b, row, col))
end

function parsefield(io::IO, ::Type{Bool}, opt::CSV.Options, row, col, state, ifmissing::Function)
    @checkmissingstart()
    truestring = opt.truestring
    falsestring = opt.falsestring
    i = 1
    if b == truestring[i]
        v = true
        while true
            if eof(io)
                if i == length(truestring)
                    state[] = EOF
                    @goto done
                end
                @goto error
            end
            b = readbyte(io)
            i += 1
            i > length(truestring) && break
            b == truestring[i] || @goto error
        end
        @checkdone(done)
    elseif b == falsestring[i]
        v = false
        while true
            if eof(io)
                if i == length(falsestring)
                    state[] = EOF
                    @goto done
                end
                @goto error
            end
            b = readbyte(io)
            i += 1
            i > length(falsestring) && break
            b == falsestring[i] || @goto error
        end
        @checkdone(done)
    end
    @checkmissingend()

    @label done
    return v

    @label missing
    return ifmissing(row, col)

    @label error
    throw(ParsingException(Bool, b, row, col))
end

function parsefield(io::IO, ::Type{<:Union{CategoricalValue, CategoricalString}}, opt::CSV.Options, row, col, state, ifmissing::Function)
    v = parsefield(io, WeakRefString{UInt8}, opt, row, col, state, ifmissing)
    return v isa Missing ? ifmissing(row, col) : v
end

# Generic fallback
function parsefield(io::IO, T, opt::CSV.Options, row, col, state, ifmissing::Function)
    v = parsefield(io, String, opt, row, col, state, ifmissing)
    ismissing(v) && return ifmissing(row, col)
    T === Missing && throw(ParsingException("encountered non-missing value for a missing-only column on row = $row, col = $col: '$v'"))
    return parse(T, v)
end
