Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getfield(f, :names), getfield(f, :types))
Tables.columns(f::File) = f
Base.propertynames(f::File) = getfield(f, :names)

struct Column{T} <: AbstractVector{T}
    f::File
    col::Int
    r::StepRange{Int, Int}
end

function Column(f::File, i::Int)
    T = getfield(f, :types)[i]
    r = range(2 + ((i - 1) * 2), step=getfield(f, :cols) * 2, length=getfield(f, :rows))
    return Column{T}(f, i, r)
end

Base.size(c::Column) = (length(c.r),)
Base.IndexStyle(::Type{<:Column}) = Base.IndexLinear()
Base.copy(c::Column{T}) where {T} = T[x for x in c]

reinterp_func(::Type{Int64}) = int64
reinterp_func(::Type{Float64}) = float64
reinterp_func(::Type{Date}) = date
reinterp_func(::Type{DateTime}) = datetime
reinterp_func(::Type{Bool}) = bool

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Missing}, row::Int)
    @boundscheck checkbounds(c, row)
    return missing
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{T}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds x = reinterp_func(T)(getfield(c.f, :tape)[c.r[row]])
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{T, Missing}}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    @inbounds x = ifelse(missingvalue(offlen), missing, reinterp_func(T)(getfield(c.f, :tape)[c.r[row]]))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Float64}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    @inbounds v = getfield(c.f, :tape)[c.r[row]]
    @inbounds x = ifelse(intvalue(offlen), Float64(int64(v)), float64(v))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{Float64, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    @inbounds v = getfield(c.f, :tape)[c.r[row]]
    @inbounds x = ifelse(missingvalue(offlen), missing, ifelse(intvalue(offlen), Float64(int64(v)), float64(v)))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{PooledString}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds x = PooledString(getfield(c.f, :tape)[c.r[row]], getfield(c.f, :refs)[c.col])
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{PooledString, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    if missingvalue(offlen)
        return missing
    else
        @inbounds x = PooledString(getfield(c.f, :tape)[c.r[row]], getfield(c.f, :refs)[c.col])
        return x
    end
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{String}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    s = unsafe_string(pointer(getfield(c.f, :buf), getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, getfield(c.f, :e)) : s
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{String, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = getfield(c.f, :tape)[c.r[row] - 1]
    if missingvalue(offlen)
        return missing
    else
        s = unsafe_string(pointer(getfield(c.f, :buf), getpos(offlen)), getlen(offlen))
        return escapedvalue(offlen) ? unescape(s, getfield(c.f, :e)) : s
    end
end

function Base.getproperty(f::File, col::Symbol)
    i = findfirst(==(col), getfield(f, :names))
    i === nothing && return getfield(f, col)
    return Column(f, i)
end
