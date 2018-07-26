import Parsers: readbyte, peekbyte

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

function readsplitline!(io::Parsers.Delimited)
    vals = Union{String, Missing}[]
    eof(io) && return vals
    col = 1
    while true
        result = Parsers.xparse(io, String)
        @debug "readsplitline!: result=$result"
        result.code === Parsers.OK || throw(Error(res, 1, col))
        push!(vals, result.result)
        col += 1
        (result.b == NEWLINE || result.b == RETURN || eof(io)) && break
    end
    return vals
end

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

# try to infer the type of the value in `val`. The precedence of type checking is `Int64` => `Float64` => `Date` => `DateTime` => `String`

# column types start out as Any, but we get rid of them as soon as possible
promote_type2(T::Type{<:Any}, ::Type{Any}) = T
promote_type2(::Type{Any}, T::Type{<:Any}) = T
# same types
promote_type2(::Type{T}, ::Type{T}) where {T} = T
# if we come across a Missing field, turn that column type into a Union{T, Missing}
promote_type2(T::Type{<:Any}, ::Type{Missing}) = Union{T, Missing}
promote_type2(::Type{Missing}, T::Type{<:Any}) = Union{T, Missing}
# these definitions allow Union{Int64, Missing} to promote to Union{Float64, Missing}
promote_type2(::Type{Union{T, Missing}}, ::Type{S}) where {T, S} = Union{promote_type2(T, S), Missing}
promote_type2(::Type{S}, ::Type{Union{T, Missing}}) where {T, S} = Union{promote_type2(T, S), Missing}
promote_type2(::Type{Union{T, Missing}}, ::Type{Union{S, Missing}}) where {T, S} = Union{promote_type2(T, S), Missing}
promote_type2(::Type{Union{String, Missing}}, ::Type{String}) = Union{String, Missing}
promote_type2(::Type{String}, ::Type{Union{String, Missing}}) = Union{String, Missing}
# basic promote type definitions from Base
promote_type2(::Type{Int64}, ::Type{Float64}) = Float64
promote_type2(::Type{Float64}, ::Type{Int64}) = Float64
promote_type2(::Type{Date}, ::Type{DateTime}) = DateTime
promote_type2(::Type{DateTime}, ::Type{Date}) = DateTime
# for cases when our current type can't widen, just promote to WeakRefString
promote_type2(::Type{<:Real}, ::Type{<:Dates.TimeType}) = String
promote_type2(::Type{<:Dates.TimeType}, ::Type{<:Real}) = String
promote_type2(::Type{T}, ::Type{String}) where T = String
promote_type2(::Type{Union{T, Missing}}, ::Type{String}) where T = Union{String, Missing}
promote_type2(::Type{String}, ::Type{T}) where T = String
promote_type2(::Type{String}, ::Type{Union{T, Missing}}) where T = Union{String, Missing}
# avoid ambiguity
promote_type2(::Type{Any}, ::Type{String}) = String
promote_type2(::Type{String}, ::Type{Any}) = String
promote_type2(::Type{String}, ::Type{String}) = String
promote_type2(::Type{String}, ::Type{Missing}) = Union{String, Missing}
promote_type2(::Type{Missing}, ::Type{String}) = Union{String, Missing}
promote_type2(::Type{Any}, ::Type{Missing}) = Missing
promote_type2(::Type{Missing}, ::Type{Missing}) = Missing

function timetype(df::Dates.DateFormat)
    date = false
    time = false
    for token in df.tokens
        T = typeof(token)
        if T == Dates.DatePart{'H'}
            time = true
        elseif T == Dates.DatePart{'y'} || T == Dates.DatePart{'Y'}
            date = true
        end
    end
    return ifelse(date & time, DateTime, ifelse(time, Time, Date))
end

function detecttype(io, prevT, levels, row, col; dateformat::Union{Dates.DateFormat, Nothing}=nothing, kwargs...) where {D}
    io2 = Parsers.getio(io)
    pos = position(io2)

    res = Parsers.xparse(io, String; kwargs...)
    res.code === Parsers.OK || throw(Error(res, row, col))
    @debug "res = $res"
    res.result === missing && return Missing
    # update levels
    levels[res.result] = get!(levels, res.result, 0) + 1

    if Int64 <: prevT || prevT == Missing
        seek(io2, pos)
        res_int = Parsers.xparse(io, Int; kwargs...)
        @debug "res_int = $res_int"
        res_int.code === Parsers.OK && return Int64
    end
    if Float64 <: prevT || Int64 <: prevT || prevT == Missing
        seek(io2, pos)
        res_float = Parsers.xparse(io, Float64; kwargs...)
        @debug "res_float = $res_float"
        res_float.code === Parsers.OK && return Float64
    end
    if Date <: prevT || DateTime <: prevT || prevT == Missing
        if dateformat === nothing
            # try to auto-detect TimeType
            seek(io2, pos)
            res_date = Parsers.xparse(io, Date; kwargs...)
            @debug "res_date = $res_date"
            res_date.code === Parsers.OK && return Date
            seek(io2, pos)
            res_datetime = Parsers.xparse(io, DateTime; kwargs...)
            @debug "res_datetime = $res_datetime"
            res_datetime.code === Parsers.OK && return DateTime
        else
            # use user-provided dateformat
            T = timetype(dateformat)
            @debug "T = $T"
            seek(io2, pos)
            res_dt = Parsers.xparse(io, T; dateformat=dateformat, kwargs...)
            @debug "res_dt = $res_dt"
            res_dt.code === Parsers.OK && return T
        end
    end
    if Bool <: prevT || prevT == Missing
        seek(io2, pos)
        res_bool = Parsers.xparse(io, Bool; kwargs...)
        @debug "res_bool = $res_bool"
        res_bool.code === Parsers.OK && return Bool
    end
    return String
end
