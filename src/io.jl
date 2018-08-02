import Parsers: readbyte, peekbyte

const COMMA_NEWLINES = Parsers.Trie([",", "\n", "\r", "\r\n"], Parsers.DELIMITED)
const READLINE_RESULT = Parsers.Result(Tuple{Ptr{UInt8}, Int})

# readline! is used for implementation of skipto!
readline!(io::IO) = readline!(Parsers.Delimited(Parsers.Quoted(), COMMA_NEWLINES), io)
readline!(s::Source) = readline!(s.parsinglayers, s.io)
function readline!(layers, io::IO)
    eof(io) && return
    while true
        READLINE_RESULT.code = Parsers.SUCCESS
        res = Parsers.parse!(layers, io, READLINE_RESULT)
        Parsers.ok(res.code) || throw(Parsers.Error(res))
        ((res.code & Parsers.NEWLINE) > 0 || eof(io)) && break
    end
    return
end

function skipto!(layers, io::IO, cur, dest)
    cur >= dest && return
    for _ = 1:(dest-cur)
        readline!(layers, io)
    end
    return
end

const READSPLITLINE_RESULT = Parsers.Result(String)
const DELIM_NEWLINE = Parsers.DELIMITED | Parsers.NEWLINE

readsplitline(io::IO) = readsplitline(Parsers.Delimited(Parsers.Quoted(), COMMA_NEWLINES), io)
function readsplitline(layers::Parsers.Delimited, io::IO)
    vals = Union{String, Missing}[]
    eof(io) && return vals
    col = 1
    result = READSPLITLINE_RESULT
    while true
        result.code = Parsers.SUCCESS
        Parsers.parse!(layers, io, result)
        @debug "readsplitline!: result=$result"
        Parsers.ok(result.code) || throw(Error(result, 1, col))
        # @show result
        push!(vals, result.result)
        col += 1
        xor(result.code & DELIM_NEWLINE, Parsers.DELIMITED) == 0 && continue
        ((result.code & Parsers.NEWLINE) > 0 || eof(io)) && break
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
        if b === q
            while !eof(io)
                b = readbyte(io)
                if b === e
                    if eof(io)
                        break
                    elseif e === q && peekbyte(io) != q
                        break
                    end
                    b = readbyte(io)
                elseif b === q
                    break
                end
            end
        elseif b === CSV.NEWLINE
            nl += 1
        elseif b === CSV.RETURN
            nl += 1
            !eof(io) && peekbyte(io) === CSV.NEWLINE && readbyte(io)
        end
    end
    return nl - (b === CSV.NEWLINE || b === CSV.RETURN)
end
countlines(io::IO, q='"', e='\\') = countlines(io, UInt8(q), UInt8(e))
# countlines(source::CSV.Source) = countlines(source.io, source.options.quotechar, source.options.escapechar)

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

function incr!(dict::Dict{String, Int}, key::Tuple{Ptr{UInt8}, Int})
    index = Base.ht_keyindex2!(dict, key)
    if index > 0
        @inbounds dict.vals[index] += 1
        return
    else
        kk::String = convert(String, key)
        @inbounds Base._setindex!(dict, 1, kk, -index)
        return
    end
end

function detecttype(layers, io, prevT, levels, row, col, bools, dateformat, dec)
    pos = position(io)
    result = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
    Parsers.ok(result.code) || throw(Error(result, row, col))
    @debug "res = $result"
    result.result === missing && return Missing
    # update levels
    incr!(levels, result.result::Tuple{Ptr{UInt8}, Int})

    if Int64 <: prevT || prevT == Missing
        seek(io, pos)
        res_int = Parsers.parse(layers, io, Int64)
        @debug "res_int = $res_int"
        Parsers.ok(res_int.code) && return Int64
    end
    if Float64 <: prevT || Int64 <: prevT || prevT == Missing
        seek(io, pos)
        res_float = Parsers.parse(layers, io, Float64; decimal=dec)
        @debug "res_float = $res_float"
        Parsers.ok(res_float.code) && return Float64
    end
    if Date <: prevT || DateTime <: prevT || prevT == Missing
        if dateformat === nothing
            # try to auto-detect TimeType
            seek(io, pos)
            res_date = Parsers.parse(layers, io, Date)
            @debug "res_date = $res_date"
            Parsers.ok(res_date.code) && return Date
            seek(io, pos)
            res_datetime = Parsers.parse(layers, io, DateTime)
            @debug "res_datetime = $res_datetime"
            Parsers.ok(res_datetime.code) && return DateTime
        else
            # use user-provided dateformat
            T = timetype(dateformat)
            @debug "T = $T"
            seek(io, pos)
            res_dt = Parsers.parse(layers, io, T; dateformat=dateformat)
            @debug "res_dt = $res_dt"
            Parsers.ok(res_dt.code) && return T
        end
    end
    if Bool <: prevT || prevT == Missing
        seek(io, pos)
        res_bool = Parsers.parse(layers, io, Bool; bools=bools)
        @debug "res_bool = $res_bool"
        Parsers.ok(res_bool.code) && return Bool
    end
    return String
end
