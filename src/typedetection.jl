# initialization case
promote_type2(::Type{Union{}}, ::Type{T}) where {T} = T
promote_type2(::Type{Union{}}, ::Type{String}) = String
promote_type2(::Type{Missing}, ::Type{T}) where {T} = Union{T, Missing}
promote_type2(::Type{Missing}, ::Type{String}) = Union{String, Missing}
promote_type2(::Type{Int64}, ::Type{Missing}) = Union{Int64, Missing}
promote_type2(::Type{Float64}, ::Type{Missing}) = Union{Float64, Missing}
promote_type2(::Type{Date}, ::Type{Missing}) = Union{Date, Missing}
promote_type2(::Type{DateTime}, ::Type{Missing}) = Union{DateTime, Missing}
promote_type2(::Type{String}, ::Type{Missing}) = Union{String, Missing}
# basic promote type definitions from Base
promote_type2(::Type{Int64}, ::Type{Float64}) = Float64
promote_type2(::Type{Float64}, ::Type{Int64}) = Float64
promote_type2(::Type{Int64}, ::Type{Date}) = String
promote_type2(::Type{Int64}, ::Type{DateTime}) = String
promote_type2(::Type{Date}, ::Type{Int64}) = String
promote_type2(::Type{DateTime}, ::Type{Int64}) = String
promote_type2(::Type{Float64}, ::Type{Date}) = String
promote_type2(::Type{Float64}, ::Type{DateTime}) = String
promote_type2(::Type{Date}, ::Type{Float64}) = String
promote_type2(::Type{DateTime}, ::Type{Float64}) = String
promote_type2(::Type{Date}, ::Type{DateTime}) = DateTime
promote_type2(::Type{DateTime}, ::Type{Date}) = DateTime
# for cases when our current type can't widen, just promote to String
promote_type2(::Type{T}, ::Type{String}) where {T} = String
promote_type2(::Type{String}, ::Type{T}) where {T} = String
promote_type2(::Type{Union{T, Missing}}, ::Type{String}) where {T} = Union{String, Missing}
promote_type2(::Type{Union{String, Missing}}, ::Type{String}) = Union{String, Missing}
# this definitions allow Union{Int64, Missing} to promote to Union{Float64, Missing}
promote_type2(::Type{Union{T, Missing}}, ::Type{S}) where {T, S} = Union{promote_type2(T, S), Missing}
promote_type2(::Type{T}, ::Type{T}) where {T} = T
promote_type2(::Type{String}, ::Type{String}) = String

# providedtypes: Dict{String, Type}, Dict{Int, Type}, Vector{Type}
initialtype(allowmissing) = (allowmissing === :auto || allowmissing === :none) ? Union{} : Missing
initialtypes(T, ::Nothing, names) = Any[T for _ = 1:length(names)]
initialtypes(T, t::Dict{String, V}, names) where {V} = Any[get(t, string(nm), T) for nm in names]
initialtypes(T, t::Dict{Int, V}, names) where {V} = Any[get(t, i, T) for i = 1:length(names)]
initialtypes(T, t::Vector, names) = length(t) == length(names) ? collect(Any, t) : throw(ArgumentError("length of user provided types ($(length(t))) does not match length of header (?$(length(names)))"))

Z(cv, p, moe) = (cv^2 * p * (1 - p)) / moe^2
# 99% CI, 50% proportion, 3.5% margin of error
const X = Z(2.58, 0.5, 0.025)
samplesize(N) = ceil(Int, (N * X) / (X + N - 1))

function detect(types, io, rows, parsinglayers, kwargs, typemap, categorical, debug)
    len = length(rows)
    cols = length(types)

    if len < 1000
        # just type detect the whole file
        r = 1:len
    else
        # calculate statistical sample size for # of rows to use for type detection
        S = samplesize(len)
        # here we divide the file rows up into even STEPS chunks
        # then also include an extra subsample-long chunk
        # at the very end of the file
        Random.seed!(0)
        r = Iterators.flatten((1:50, sort!(rand(51:len-51, S)), (len-50):len))
        debug && @show S, r
    end

    defaultstringtype = len > 100_000 ? WeakRefString{UInt8} : String
    # prep if categorical
    levels = categorical ? Any[Dict{String, Int}() for _ = 1:cols] : []

    for row in r
        seek(io, rows[row])
        for col = 1:cols
            if debug
                pos = position(io)
                debug && @show pos
                result = Parsers.parse(parsinglayers, io, String)
                seek(io, pos)
                debug && @show "col: $col, detecting type for: $(result.result)"
            end
            T = detecttype(types[col], io, parsinglayers, kwargs, levels, row, col, categorical, defaultstringtype)
            debug && @show "col: $col, detected type: $T"
            types[col] = promote_type2(types[col], T)
            debug && @show "col: $col, promoted to: $(types[col])"
        end
    end
    debug && @show types

    if categorical
        pools = Vector{CategoricalPool{String, UInt32, CatStr}}(undef, cols)
        for col = 1:cols
            T = types[col]
            if length(levels[col]) / sum(values(levels[col])) < .67 && T !== Missing && Base.nonmissingtype(T) <: String
                types[col] = substitute(T, CategoricalArrays.catvaluetype(Base.nonmissingtype(T), UInt32))
                pools[col] = CategoricalPool{String, UInt32}(collect(keys(levels[col])))
            end
        end
    else
        pools = CategoricalPool{String, UInt32, CatStr}[]
    end
    if !isempty(typemap)
        for icol = 1:cols
            types[col] = get(typemap, types[col], types[col])
        end
    end
    return types, pools
end

empty(::Type{Union{}}) = true
empty(::Type{Missing}) = true
empty(x) = false
strtype(::Type{T}, str) where {T} = str
strtype(::Type{T}) where {T <: AbstractString} = T

function detecttype(prevT, io, layers, kwargs, levels, row, col, categorical, defaultstringtype)
    pos = position(io)
    if categorical
        result = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
        Parsers.ok(result.code) || throw(Error(Parsers.Error(io, result), row, col))
        # @debug "res = $result"
        result.result === missing && return Missing
        # update levels
        incr!(levels[col], result.result::Tuple{Ptr{UInt8}, Int})
    end

    if Int64 <: prevT || empty(prevT)
        seek(io, pos)
        res_int = Parsers.parse(layers, io, Int64)
        # @debug "res_int = $res_int"
        Parsers.ok(res_int.code) && return typeof(res_int.result)
    end
    if Float64 <: prevT || Int64 <: prevT || empty(prevT)
        seek(io, pos)
        res_float = Parsers.parse(layers, io, Float64; kwargs...)
        # @debug "res_float = $res_float"
        Parsers.ok(res_float.code) && return typeof(res_float.result)
    end
    if Date <: prevT || DateTime <: prevT || empty(prevT)
        if !haskey(kwargs, :dateformat)
            # try to auto-detect TimeType
            seek(io, pos)
            res_date = Parsers.parse(layers, io, Date)
            # @debug "res_date = $res_date"
            Parsers.ok(res_date.code) && return typeof(res_date.result)
            seek(io, pos)
            res_datetime = Parsers.parse(layers, io, DateTime)
            # @debug "res_datetime = $res_datetime"
            Parsers.ok(res_datetime.code) && return typeof(res_datetime.result)
        else
            # use user-provided dateformat
            T = timetype(kwargs.dateformat)
            # @debug "T = $T"
            seek(io, pos)
            res_dt = Parsers.parse(layers, io, T; dateformat=kwargs.dateformat)
            # @debug "res_dt = $res_dt"
            Parsers.ok(res_dt.code) && return typeof(res_dt.result)
        end
    end
    if Bool <: prevT || empty(prevT)
        seek(io, pos)
        res_bool = Parsers.parse(layers, io, Bool; kwargs...)
        # @debug "res_bool = $res_bool"
        Parsers.ok(res_bool.code) && return typeof(res_bool.result)
    end
    seek(io, pos)
    Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
    return strtype(prevT, defaultstringtype)
end

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
