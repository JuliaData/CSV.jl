# initialization case
promote_type2(::Type{Union{}}, ::Type{T}) where {T} = T
promote_type2(::Type{Union{}}, ::Type{String}) = String
promote_type2(::Type{Missing}, ::Type{T}) where {T} = Union{T, Missing}
promote_type2(::Type{Missing}, ::Type{String}) = Union{String, Missing}
promote_type2(::Type{Int64}, ::Type{Missing}) = Union{Int64, Missing}
promote_type2(::Type{Float64}, ::Type{Missing}) = Union{Float64, Missing}
promote_type2(::Type{Bool}, ::Type{Missing}) = Union{Bool, Missing}
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
initialtypes(T, ::Nothing, names) = Type[T for _ = 1:length(names)]
initialtypes(T, t::Dict{String, V}, names) where {V} = Type[get(t, string(nm), T) for nm in names]
initialtypes(T, t::Dict{Symbol, V}, names) where {V} = Type[get(t, nm, T) for nm in names]
initialtypes(T, t::Dict{Int, V}, names) where {V} = Type[get(t, i, T) for i = 1:length(names)]
initialtypes(T, t::Vector, names) = length(t) == length(names) ? collect(Type, t) : throw(ArgumentError("length of user provided types ($(length(t))) does not match length of header (?$(length(names)))"))

struct Fib{N}
    len::Int
end
Fib(len) = Fib{1024}(len)

@inline function Base.iterate(f::Fib)
    f.len == 0 && return nothing
    return 1, (1, 1, 1, 1)
end

@inline function Base.iterate(f::Fib{N}, (off, len, prevfib, fib)) where {N}
    (off + fib) > f.len && return nothing
    if rem(len, N) == 0
        prevfib, fib = fib, prevfib + fib
    end
    return off + fib, (off + fib, len + 1, prevfib, fib)
end

function detect(types, io, positions, parsinglayers, kwargs, typemap, categorical, transpose, ref, debug)
    len = transpose ? ref[] : length(positions)
    cols = length(types)

    defaultstringtype = len > 100_000 ? WeakRefString{UInt8} : String
    # prep if categorical
    levels = categorical ? Any[Dict{String, Int}() for _ = 1:cols] : []

    lastcode = Base.RefValue(Parsers.SUCCESS)
    rows = 0
    for row in Fib(len)
        rows += 1
        !transpose && seek(io, positions[row])
        lastcode[] = Parsers.SUCCESS
        for col = 1:cols
            if !transpose && newline(lastcode[])
                types[col] = promote_type2(types[col], Missing)
                continue
            end
            transpose && seek(io, positions[col])
            if debug
                pos = position(io)
                debug && println("pos=$pos")
                result = Parsers.parse(parsinglayers, io, String)
                seek(io, pos)
                debug && println("col: $col, detecting type for: '$(result.result)'")
            end
            T = detecttype(types[col], io, parsinglayers, kwargs, levels, row, col, categorical, defaultstringtype, lastcode)
            debug && println("col: $col, detected type: $T")
            types[col] = promote_type2(types[col], T)
            debug && println("col: $col, promoted to: $(types[col])")
            transpose && setindex!(positions, position(io), col)
        end
    end
    debug && println("scanned $rows / $len = $((rows / len) * 100)% of file to infer types")
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
        for col = 1:cols
            types[col] = get(typemap, types[col], types[col])
        end
    end
    return types, pools
end

empty(::Type{Union{}}) = true
empty(::Type{Missing}) = true
empty(x) = false
strtype(::Type{Missing}, T) = Missing
strtype(x, T) = T

function detecttype(prevT, io, layers, kwargs, levels, row, col, categorical, defaultstringtype, lastcode)
    pos = position(io)
    if categorical
        result = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
        Parsers.ok(result.code) || throw(Error(Parsers.Error(io, result), row, col))
        lastcode[] = result.code
        # @debug "res = $result"
        result.result === missing && return Missing
        # update levels
        incr!(levels[col], result.result::Tuple{Ptr{UInt8}, Int})
    end

    if Int64 <: prevT || empty(prevT)
        seek(io, pos)
        res_int = Parsers.parse(layers, io, Int64)
        lastcode[] = res_int.code
        # @debug "res_int = $res_int"
        Parsers.ok(res_int.code) && return typeof(res_int.result)
    end
    if Float64 <: prevT || Int64 <: prevT || empty(prevT)
        seek(io, pos)
        res_float = Parsers.parse(layers, io, Float64; kwargs...)
        lastcode[] = res_float.code
        # @debug "res_float = $res_float"
        Parsers.ok(res_float.code) && return typeof(res_float.result)
    end
    if Date <: prevT || DateTime <: prevT || empty(prevT)
        if !haskey(kwargs, :dateformat)
            # try to auto-detect TimeType
            seek(io, pos)
            res_date = Parsers.parse(layers, io, Date)
            lastcode[] = res_date.code
            # @debug "res_date = $res_date"
            Parsers.ok(res_date.code) && return typeof(res_date.result)
            seek(io, pos)
            res_datetime = Parsers.parse(layers, io, DateTime)
            lastcode[] = res_datetime.code
            # @debug "res_datetime = $res_datetime"
            Parsers.ok(res_datetime.code) && return typeof(res_datetime.result)
        else
            # use user-provided dateformat
            T = timetype(kwargs.dateformat)
            # @debug "T = $T"
            seek(io, pos)
            res_dt = Parsers.parse(layers, io, T; dateformat=kwargs.dateformat)
            lastcode[] = res_dt.code
            # @debug "res_dt = $res_dt"
            Parsers.ok(res_dt.code) && return typeof(res_dt.result)
        end
    end
    if Bool <: prevT || empty(prevT)
        seek(io, pos)
        res_bool = Parsers.parse(layers, io, Bool; kwargs...)
        lastcode[] = res_bool.code
        # @debug "res_bool = $res_bool"
        Parsers.ok(res_bool.code) && return typeof(res_bool.result)
    end
    seek(io, pos)
    res_str = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
    lastcode[] = res_str.code
    return strtype(typeof(res_str.result), defaultstringtype)
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
