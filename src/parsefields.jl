@enum ParsingState None Delimiter EOF Newline

# at start of field: check if eof, remove leading whitespace, check if empty field
# returns `true` if result of initial parsing is a null field
# also return `b` which is last byte read
macro checknullstart()
    return esc(quote
        state = None
        eof(io) && (state = EOF; @goto null)
        b = Base.read(io, UInt8)
        # println("read char = $(Char(b))")
        while b != opt.delim && (b == CSV.SPACE || b == CSV.TAB || b == opt.quotechar)
            eof(io) && (state = EOF; @goto null)
            b = Base.read(io, UInt8)
        end
        if b == opt.delim
            state = Delimiter
            @goto null
        elseif b == NEWLINE
            state = Newline
            @goto null
        elseif b == RETURN
            state = Newline
            !eof(io) && Base.peek(io) == NEWLINE && Base.read(io, UInt8)
            @goto null
        end
    end)
end

# check if we've successfully finished parsing a field by whether
# we've encountered a delimiter or newline break or reached eof
macro checkdone(label)
    return esc(quote
        b == opt.quotechar && !eof(io) && (b = Base.read(io, UInt8))
        if b == opt.delim
            state = Delimiter
            @goto $label
        elseif b == NEWLINE
            state = Newline
            @goto $label
        elseif b == RETURN
            state = Newline
            !eof(io) && Base.peek(io) == NEWLINE && Base.read(io, UInt8)
            @goto $label
        elseif b == opt.quotechar && (position(io) == 1 || eof(io))
            state = EOF
            @goto $label
        end
    end)
end

CSVError{T}(::Type{T}, b, row, col) = CSV.CSVError("error parsing a `$T` value on column $col, row $row; encountered '$(Char(b))'")

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
            b = Base.read(io, UInt8)
            i += 1
        end
        if !eof(io)
            b = Base.read(io, UInt8)
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

# parsefield{T}(source::CSV.Source, ::Type{T}, row=0, col=0) = get(CSV.parsefield(source.io, T, source.options, row, col, STATE))
# parsefield{T}(source::CSV.Source, ::Type{Nullable{T}}, row=0, col=0) = CSV.parsefield(source.io, T, source.options, row, col, STATE)
# parsefield(source::CSV.Source, ::Type{Nullable{WeakRefString{UInt8}}}, row=0, col=0) = CSV.parsefield(source.io, WeakRefString{UInt8}, source.options, row, col, STATE, source.ptr)

const NULLTHROW = ()->throw(NullException())
const NULLRETURN = ()->null

@inline parsefield(io::Buffer, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0) where {T} = parsefield(io, T, opt, row, col, NULLTHROW)
@inline parsefield(io::Buffer, ::Type{Union{T, Null}}, opt::CSV.Options=CSV.Options(), row=0, col=0) where {T} = parsefield(io, T, opt, row, col, NULLRETURN)

@inline function parsefield(io::Buffer, ::Type{T}, opt::CSV.Options, row, col, ifnull::Function) where {T <: Integer}
    @checknullstart()
    v = zero(T)
    negative = false
    if b == MINUS # check for leading '-' or '+'
        negative = true
        b = Base.read(io, UInt8)
    elseif b == PLUS
        b = Base.read(io, UInt8)
    end
    while NEG_ONE < b < TEN
        # process digits
        v *= T(10)
        v += T(b - ZERO)
        eof(io) && (state = EOF; @goto done)
        b = Base.read(io, UInt8)
    end
    @checkdone(done)
    @checknullend()

    @label done
    return ifelse(negative, -v, v)

    @label null
    return ifnull()

    @label error
    throw(CSVError(T, b, row, col))
end

make(::Type{WeakRefString{UInt8}}, ptr, len, pos) = WeakRefString(Ptr{UInt8}(ptr), len, pos)
make(::Type{String}, ptr, len, pos) = unsafe_string(ptr, len)

@inline function parsefield(io::Buffer, T::Type{<:AbstractString}, opt::CSV.Options, row, col, ifnull::Function)
    eof(io) && (state = EOF; @goto null)
    ptr = pointer(io.data, io.pos)
    pos = io.pos
    len = 0
    nullcheck = opt.nullcheck # if null is "", then we don't need to byte match it
    nulllen = length(opt.null)
    @inbounds while !eof(io)
        b = Base.read(io, UInt8)
        if b == opt.quotechar
            ptr += 1
            pos += 1
            while !eof(io)
                b = Base.read(io, UInt8)
                if b == opt.escapechar
                    b = Base.read(io, UInt8)
                    len += 1
                elseif b == opt.quotechar
                    break
                end
                (nullcheck && len+1 <= nulllen && b == opt.null[len+1]) || (nullcheck = false)
                len += 1
            end
        elseif b == opt.delim
            state = Delimiter
            break
        elseif b == NEWLINE
            state = Newline
            break
        elseif b == RETURN
            state = Newline
            !eof(io) && Base.peek(io) == NEWLINE && incr!(io)
            break
        else
            (nullcheck && len+1 <= nulllen && b == opt.null[len+1]) || (nullcheck = false)
            len += 1
        end
    end
    eof(io) && (state = EOF)
    (len == 0 || nullcheck) && @goto null
    return make(T, ptr, len, pos)

    @label null
    return ifnull()
end

@inline function parsefield(io::Buffer, ::Type{Date}, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, WeakRefString{UInt8}, opt, row, col, ifnull)
    return v isa Null ? ifnull() : Date(v, opt.dateformat)
end
@inline function parsefield(io::Buffer, ::Type{DateTime}, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, WeakRefString{UInt8}, opt, row, col, ifnull)
    return v isa Null ? ifnull() : DateTime(v, opt.dateformat)
end

@inline function parsefield(io::Buffer, ::Type{Char}, opt::CSV.Options, row, col, ifnull::Function)
    @checknullstart()
    c = b
    eof(io) && (state = EOF; @goto done)
    opt.nullcheck && b == opt.null[1] && @goto null
    b = Base.read(io, UInt8)
    @checkdone(done)
    @checknullend()

    @label done
    return Char(c)

    @label null
    return ifnull()

    @label error
    throw(CSVError(Char, b, row, col))
end

# Generic fallback
@inline function parsefield(io::Buffer, T, opt::CSV.Options, row, col, ifnull::Function)
    v = parsefield(io, String, opt, row, col, ifnull)
    return v isa Null ? ifnull() : parse(T, v)
end