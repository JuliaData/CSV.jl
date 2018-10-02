# initialization case
function promote_type2(@nospecialize(T), @nospecialize(S))
    if T === Union{} || T === S
        return S
    elseif T === Missing || S === Missing
        return Union{T, S}
    elseif T === Int64
        return S === Float64 ? Float64 : String
    elseif T === Float64
        return S === Int64 ? Float64 : String
    elseif T === Date
        return S === DateTime ? DateTime : String
    elseif T === DateTime
        return S === Date ? DateTime : String
    elseif T === String
        return String
    else
        return S === String ? Union{String, Missing} : Union{promote_type2(Base.nonmissingtype(T), Base.nonmissingtype(S)), Missing}
    end
end

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

    # prep if categorical
    levels = categorical ? Dict{String, Int}[Dict{String, Int}() for _ = 1:cols] : Dict{String, Int}[]

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
            # if debug
            #     pos = position(io)
            #     debug && println("pos=$pos")
            #     result = Parsers.parse(parsinglayers, io, String)
            #     seek(io, pos)
            #     debug && println("col: $col, detecting type for: '$(result.result)'")
            # end
            T = detecttype(Base.nonmissingtype(types[col]), io, parsinglayers, kwargs, levels, row, col, categorical, lastcode)
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
            if T === String || T === Union{String, Missing}
                types[col] = substitute(T, CatStr)
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

@inline function trytype(io, pos, layers, T, kwargs, lastcode)
    seek(io, pos)
    res = Parsers.parse(layers, io, T; kwargs...)
    lastcode[] = res.code
    return Parsers.ok(res.code) ? typeof(res.result) : nothing
end

function detecttype(@nospecialize(prevT), io, layers, kwargs, levels, row, col, categorical, lastcode)
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
    if prevT === Union{} || prevT === Missing || prevT === Int64
        int = trytype(io, pos, layers, Int64, kwargs, lastcode)
        int !== nothing && return int
    end
    if prevT === Union{} || prevT === Missing || prevT === Int64 || prevT === Float64
        float = trytype(io, pos, layers, Float64, kwargs, lastcode)
        float !== nothing && return float
    end
    if prevT === Union{} || prevT === Missing || prevT === Date || prevT === DateTime
        if !haskey(kwargs, :dateformat)
            date = trytype(io, pos, layers, Date, kwargs, lastcode)
            date !== nothing && return date
            try
                datetime = trytype(io, pos, layers, DateTime, kwargs, lastcode)
                datetime !== nothing && return datetime
            catch e
            end
        else
            # use user-provided dateformat
            T = timetype(kwargs.dateformat)
            dt = trytype(io, pos, layers, T, kwargs, lastcode)
            dt !== nothing && return dt
        end
    end
    if prevT === Union{} || prevT === Missing || prevT === Bool
        bool = trytype(io, pos, layers, Bool, kwargs, lastcode)
        bool !== nothing && return bool
    end
    str = trytype(io, pos, layers, Tuple{Ptr{UInt8}, Int}, kwargs, lastcode)
    return ifelse(str === nothing, String, ifelse(str === Missing, Missing, String))
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
