"""
    CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) => String
    CSV.readline(source::CSV.Source) => String

Read a single line from `io` (any `IO` type) or a `CSV.Source` as a `String` object.
This function mirrors `Base.readline` except that the newlines within quoted
fields are ignored (e.g. value1, value2, \"value3 with \n embedded newlines\").
Uses `buf::IOBuffer` for intermediate IO operations, if specified.
"""
function readline end

function readline(io::IO, q::UInt8, e::UInt8, buf::IOBuffer=IOBuffer())
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
            !eof(io) && unsafe_peek(io) == NEWLINE && Base.write(buf, unsafe_read(io, UInt8))
            break
        end
    end
    return String(take!(buf))
end
readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) = readline(io, UInt8(q), UInt8(e), buf)
readline(source::CSV.Source) = readline(source.io, source.options.quotechar, source.options.escapechar)

# contents of a single CSV table field as returned by readsplitline!()
immutable RawField
    value::String   # uparsed contents
    isquoted::Bool  # whether the field value was quoted or not
end

Base.:(==)(a::RawField, b::RawField) = (a.isquoted == b.isquoted) && (a.value == b.value)

"""
    CSV.readsplitline!(vals::Vector{RawField}, io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())
    CSV.readsplitline!(vals::Vector{RawField}, source::CSV.Source)

Read a single, delimited line from `io` (any `IO` type) or a `CSV.Source` as a `Vector{String}` and
store the values in `vals`.
Delimited fields are separated by `d`, quoted by `q` and escaped by `e` ASCII characters.
The contents of `vals` are replaced.
Uses `buf::IOBuffer` for intermediate IO operations, if specified.
"""
function readsplitline! end

@enum ReadSplitLineState RSL_IN_FIELD RSL_IN_QUOTE RSL_AFTER_QUOTE RSL_AFTER_DELIM RSL_AFTER_NEWLINE

function readsplitline!(vals::Vector{RawField}, io::IO, d::UInt8, q::UInt8, e::UInt8, buf::IOBuffer=IOBuffer())
    empty!(vals)
    state = RSL_AFTER_DELIM
    push_buf_to_vals!() = push!(vals, RawField(String(take!(buf)), state==RSL_AFTER_QUOTE))
    while !eof(io)
        b = unsafe_read(io, UInt8)
        if state == RSL_IN_QUOTE # in the quoted string
            if b == q # end the quoted string
                state = RSL_AFTER_QUOTE
            elseif b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                b = unsafe_read(io, UInt8)
                Base.write(buf, b)
            else
                Base.write(buf, b)
            end
        elseif b == d # delimiter
            if state == RSL_AFTER_DELIM # empty field
                push!(vals, RawField("", false))
            else
                push_buf_to_vals!()
            end
            state = RSL_AFTER_DELIM
        elseif b == q # start of quote
            if state == RSL_AFTER_DELIM
                state = RSL_IN_QUOTE
            else
                throw(CSVError("Unexpected start of quote ($q), use \"$e$q\" to type \"$q\""))
            end
        elseif b == NEWLINE
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        elseif b == RETURN
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        else
            if state == RSL_AFTER_QUOTE
                throw(CSVError("Unexpected character ($b) after the end of quote ($q)"))
            elseif b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                b = unsafe_read(io, UInt8)
            end
            Base.write(buf, b)
            state = RSL_IN_FIELD
        end
    end
    if state == RSL_IN_QUOTE
        @assert eof(io)
        throw(CSVError("EOF while trying to read the closing quote"))
    elseif state == RSL_IN_FIELD || state == RSL_AFTER_DELIM # file ended without the newline, store the current buf
        @assert eof(io)
        push_buf_to_vals!()
    end
    return vals
end
readsplitline!(vals::Vector{RawField}, io::IO, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer()) = readsplitline!(vals, io, UInt8(d), UInt8(q), UInt8(e), buf)
readsplitline!(vals::Vector{RawField}, source::CSV.Source) = readsplitline!(vals, source.io, source.options.delim, source.options.quotechar, source.options.escapechar)

readsplitline(io::IO, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer()) =
    readsplitline!(Vector{RawField}(), io, d, q, e, buf)
readsplitline(args...) = readsplitline!(Vector{RawField}(), args...)

"""
    CSV.countlines(io::IO, quotechar, escapechar) => Int
    CSV.countlines(source::CSV.Source) => Int

Count the number of lines in a file, accounting for potentially embedded newlines in quoted fields.
"""
function countlines(io::IO, q::UInt8, e::UInt8)
    nl = 1
    b = 0x00
    while !eof(io)
        b = unsafe_read(io, UInt8)
        if b == q
            while !eof(io)
                b = unsafe_read(io, UInt8)
                if b == e
                    b = unsafe_read(io, UInt8)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            nl += 1
        elseif b == RETURN
            nl += 1
            !eof(io) && unsafe_peek(io) == NEWLINE && unsafe_read(io, UInt8)
        end
    end
    return nl - (b == NEWLINE || b == RETURN)
end
countlines(io::IO, q='"', e='\\') = countlines(io, UInt8(q), UInt8(e))
countlines(source::CSV.Source) = countlines(source.io, source.options.quotechar, source.options.escapechar)

function skipto!(f::IO, cur, dest, q, e)
    cur >= dest && return
    for _ = 1:(dest-cur)
        CSV.readline(f,q,e)
    end
    return
end

# try to infer the type of the value in `val`. The precedence of type checking is `Int` => `Float64` => `Date` => `DateTime` => `String`
if VERSION > v"0.6.0-dev.2307"
    timetype{D <: Dates.DateFormat}(df::D) = any(typeof(T) in (Dates.DatePart{'H'}, Dates.DatePart{'M'}, Dates.DatePart{'S'}, Dates.DatePart{'s'}) for T in df.tokens) ? DateTime : Date
else
    slottype{T}(df::Dates.Slot{T}) = T
    timetype{D <: Dates.DateFormat}(df::D) = any(slottype(T) in (Dates.Hour,Dates.Minute,Dates.Second,Dates.Millisecond) for T in df.slots) ? DateTime : Date
end

# column types start out as Any, but we get rid of them as soon as possible
promote_type2(T::Type{<:Any}, ::Type{Any}) = T
promote_type2(::Type{Any}, T::Type{<:Any}) = T
# same types
promote_type2{T}(::Type{T}, ::Type{T}) = T
# if we come across a Null field, turn that column type into a ?T
promote_type2(T::Type{<:Any}, ::Type{Null}) = Union{T, Null}
promote_type2(::Type{Null}, T::Type{<:Any}) = Union{T, Null}
# these definitions allow ?Int to promote to ?Float64
promote_type2{T, S}(::Type{Union{T, Null}}, ::Type{S}) = Union{promote_type2(T, S), Null}
promote_type2{T, S}(::Type{S}, ::Type{Union{T, Null}}) = Union{promote_type2(T, S), Null}
promote_type2{T, S}(::Type{Union{T, Null}}, ::Type{Union{S, Null}}) = Union{promote_type2(T, S), Null}
# basic promote type definitions from Base
promote_type2(::Type{Int}, ::Type{Float64}) = Float64
promote_type2(::Type{Float64}, ::Type{Int}) = Float64
promote_type2(::Type{Date}, ::Type{DateTime}) = DateTime
promote_type2(::Type{DateTime}, ::Type{Date}) = DateTime
# for cases when our current type can't widen, just promote to WeakRefString
promote_type2(::Type{Int}, ::Type{<:Dates.TimeType}) = WeakRefString{UInt8}
promote_type2(::Type{<:Dates.TimeType}, ::Type{Int}) = WeakRefString{UInt8}
promote_type2(::Type{Float64}, ::Type{<:Dates.TimeType}) = WeakRefString{UInt8}
promote_type2(::Type{<:Dates.TimeType}, ::Type{Float64}) = WeakRefString{UInt8}
promote_type2(::Type{<:Any}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{<:Any}) = WeakRefString{UInt8}
# avoid ambiguity
promote_type2(::Type{Any}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{Any}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{Null}) = ?WeakRefString{UInt8}
promote_type2(::Type{Null}, ::Type{WeakRefString{UInt8}}) = ?WeakRefString{UInt8}

const DATE_OPTIONS = CSV.Options(dateformat=Dates.ISODateFormat)
const DATETIME_OPTIONS = CSV.Options(dateformat=Dates.ISODateTimeFormat)

function detecttype{D}(source, options::CSV.Options{D}, T)
    # val.isquoted && return WeakRefString{UInt8} # quoted is always a string
    # (isempty(val.value) || val.value == null) && return Null
    unmark(source)
    try
        mark(source)
        v1 = CSV.parsefield(source, Nulls.?Int, options)
        return v1 isa Null ? Null : Int
    end
    try
        reset(source)
        mark(source)
        v2 = CSV.parsefield(source, Nulls.?Float64, options)
        return v2 isa Null ? Null : Float64
    end
    if D == Dates.ISODateTimeFormat
        try
            reset(source)
            mark(source)
            v3 = CSV.parsefield(source, Nulls.?Date, DATE_OPTIONS)
            return v3 isa Null ? Null : Date
        end
        try
            reset(source)
            mark(source)
            v4 = CSV.parsefield(source, Nulls.?DateTime, DATETIME_OPTIONS)
            return v4 isa Null ? Null : DateTime
        end
    else
        try
            reset(source)
            mark(source)
            v5 = CSV.parsefield(source, Nulls.?T, options)
            return v5 isa Null ? Null : T
        end
    end
    reset(source)
    b = unsafe_read(source)
    b, done = checkdone(source, b, options, STATE)
    while !done
        b = unsafe_read(source)
        b, done = checkdone(source, b, options, STATE)
    end
    return WeakRefString{UInt8}
end
