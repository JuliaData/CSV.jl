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
        b = readbyte(io)
        Base.write(buf, b)
        if b == q
            while !eof(io)
                b = readbyte(io)
                Base.write(buf, b)
                if b == e
                    if eof(io)
                        break
                    elseif e == q && peekbyte(io) != q
                        break
                    end
                    b = readbyte(io)
                    Base.write(buf, b)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peekbyte(io) == NEWLINE && Base.write(buf, readbyte(io))
            break
        end
    end
    return String(take!(buf))
end
readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer()) = readline(io, UInt8(q), UInt8(e), buf)
readline(source::CSV.Source) = readline(source.io, source.options.quotechar, source.options.escapechar)

# contents of a single CSV table field as returned by readsplitline!()
struct RawField
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
        b = readbyte(io)
        if state == RSL_IN_QUOTE # in the quoted string
            if b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                if e == q && peekbyte(io) != q
                    state = RSL_AFTER_QUOTE
                    break
                end
                b = readbyte(io)
                Base.write(buf, b)
            elseif b == q # end the quoted string
                state = RSL_AFTER_QUOTE
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
                throw(ParsingException("Unexpected start of quote ($q), use \"$e$q\" to type \"$q\""))
            end
        elseif b == NEWLINE
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        elseif b == RETURN
            !eof(io) && peekbyte(io) == NEWLINE && readbyte(io)
            push_buf_to_vals!() # add the last field
            state = RSL_AFTER_NEWLINE
            break
        else
            if state == RSL_AFTER_QUOTE
                throw(ParsingException("Unexpected character ($b) after the end of quote ($q)"))
            elseif b == e # the escape character, read the next after it
                Base.write(buf, b)
                @assert !eof(io)
                b = readbyte(io)
            end
            Base.write(buf, b)
            state = RSL_IN_FIELD
        end
    end
    if state == RSL_IN_QUOTE
        @assert eof(io)
        throw(ParsingException("EOF while trying to read the closing quote"))
    elseif state == RSL_IN_FIELD || state == RSL_AFTER_DELIM # file ended without the newline, store the current buf
        eof(io)
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
        b = readbyte(io)
        if b == q
            while !eof(io)
                b = readbyte(io)
                if b == e
                    if eof(io)
                        break
                    elseif e == q && peekbyte(io) != q
                        break
                    end
                    b = readbyte(io)
                elseif b == q
                    break
                end
            end
        elseif b == CSV.NEWLINE
            nl += 1
        elseif b == CSV.RETURN
            nl += 1
            !eof(io) && peekbyte(io) == CSV.NEWLINE && readbyte(io)
        end
    end
    return nl - (b == CSV.NEWLINE || b == CSV.RETURN)
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
timetype(df::Dates.DateFormat) = any(typeof(T) in (Dates.DatePart{'H'}, Dates.DatePart{'M'}, Dates.DatePart{'S'}, Dates.DatePart{'s'}) for T in df.tokens) ? DateTime : Date

# column types start out as Any, but we get rid of them as soon as possible
promote_type2(T::Type{<:Any}, ::Type{Any}) = T
promote_type2(::Type{Any}, T::Type{<:Any}) = T
# same types
promote_type2(::Type{T}, ::Type{T}) where {T} = T
# if we come across a Null field, turn that column type into a Union{T, Null}
promote_type2(T::Type{<:Any}, ::Type{Null}) = Union{T, Null}
promote_type2(::Type{Null}, T::Type{<:Any}) = Union{T, Null}
# these definitions allow Union{Int, Null} to promote to Union{Float64, Null}
promote_type2(::Type{Union{T, Null}}, ::Type{S}) where {T, S} = Union{promote_type2(T, S), Null}
promote_type2(::Type{S}, ::Type{Union{T, Null}}) where {T, S} = Union{promote_type2(T, S), Null}
promote_type2(::Type{Union{T, Null}}, ::Type{Union{S, Null}}) where {T, S} = Union{promote_type2(T, S), Null}
promote_type2(::Type{Union{WeakRefString{UInt8}, Null}}, ::Type{WeakRefString{UInt8}}) = Union{WeakRefString{UInt8}, Null}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{Union{WeakRefString{UInt8}, Null}}) = Union{WeakRefString{UInt8}, Null}
# basic promote type definitions from Base
promote_type2(::Type{Int}, ::Type{Float64}) = Float64
promote_type2(::Type{Float64}, ::Type{Int}) = Float64
promote_type2(::Type{Date}, ::Type{DateTime}) = DateTime
promote_type2(::Type{DateTime}, ::Type{Date}) = DateTime
# for cases when our current type can't widen, just promote to WeakRefString
promote_type2(::Type{<:Real}, ::Type{<:Dates.TimeType}) = WeakRefString{UInt8}
promote_type2(::Type{<:Dates.TimeType}, ::Type{<:Real}) = WeakRefString{UInt8}
promote_type2(::Type{<:Any}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{<:Any}) = WeakRefString{UInt8}
# avoid ambiguity
promote_type2(::Type{Any}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{Any}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{WeakRefString{UInt8}}) = WeakRefString{UInt8}
promote_type2(::Type{WeakRefString{UInt8}}, ::Type{Null}) = Union{WeakRefString{UInt8}, Null}
promote_type2(::Type{Null}, ::Type{WeakRefString{UInt8}}) = Union{WeakRefString{UInt8}, Null}
promote_type2(::Type{Any}, ::Type{Null}) = Null
promote_type2(::Type{Null}, ::Type{Null}) = Null

function detecttype(io, opt::CSV.Options{D}, prevT, seen) where {D}
    pos = position(io)
    if Int <: prevT || prevT == Null
        try
            v1 = CSV.parsefield(io, Union{Int, Null}, opt)
            # print("...parsed = '$v1'...")
            return v1 isa Null ? Null : Int
        end
    end
    if Float64 <: prevT || Int <: prevT || prevT == Null
        try
            seek(io, pos)
            v2 = CSV.parsefield(io, Union{Float64, Null}, opt)
            # print("...parsed = '$v2'...")
            return v2 isa Null ? Null : Float64
        end
    end
    if Date <: prevT || DateTime <: prevT || prevT == Null
        if D == Null
            # try to auto-detect TimeType
            try
                seek(io, pos)
                v3 = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null}, opt)
                # print("...parsed = '$v3'...")
                return v3 isa Null ? Null : (Date(v3, Dates.ISODateFormat); Date)
            end
            try
                seek(io, pos)
                v4 = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null}, opt)
                # print("...parsed = '$v4'...")
                return v4 isa Null ? Null : (DateTime(v4, Dates.ISODateTimeFormat); DateTime)
            end
        else
            # use user-provided dateformat
            try
                seek(io, pos)
                T = timetype(opt.dateformat)
                v5 = CSV.parsefield(io, Union{T, Null}, opt)
                return v5 isa Null ? Null : T
            end
        end
    end
    if Bool <: prevT || prevT == Null
        try
            seek(io, pos)
            v6 = CSV.parsefield(io, Union{Bool, Null}, opt)
            return v6 isa Null ? Null : Bool
        end
    end
    try
        seek(io, pos)
        v7 = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null}, opt)
        push!(seen, v7)
        # print("...parsed = '$v1'...")
        return v7 isa Null ? Null : WeakRefString{UInt8}
    end
    return Null
end
