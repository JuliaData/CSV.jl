Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), _eltype.(gettypes(f)))
Tables.columns(f::File) = f
Base.propertynames(f::File) = getnames(f)

struct Column{T, P} <: AbstractVector{T}
    file::File
    col::Int
end

_eltype(::Type{T}) where {T} = T
_eltype(::Type{PooledString}) = String
_eltype(::Type{Union{PooledString, Missing}}) = Union{String, Missing}

function Column(f::File, i::Int)
    @inbounds T = gettypes(f)[i]
    return Column{_eltype(T), T}(f, i)
end

Base.size(c::Column) = (Int(getrows(c.file)),)
Base.IndexStyle(::Type{<:Column}) = Base.IndexLinear()
metaind(x) = 2 * x - 1
valind(x) = 2 * x

Base.setindex!(c::Column, x, i::Int) = throw(ArgumentError("CSV.Column is read-only; to get a mutable vector, do `copy(col)` or to make all columns mutable do `CSV.read(file; copycols=true)` or `CSV.File(file) |> DataFrame`"))

function Base.copy(c::Column{T}) where {T}
    len = length(c)
    A = Vector{T}(undef, len)
    @simd for i = 1:len
        @inbounds A[i] = c[i]
    end
    return A
end

function Base.copy(c::Column{T, T}) where {T <: Union{String, Union{String, Missing}}}
    len = length(c)
    A = StringVector{T}(undef, len)
    @simd for i = 1:len
        @inbounds A[i] = c[i]
    end
    return A
end

function Base.copy(c::Column{T, S}) where {T <: Union{String, Union{String, Missing}}, S <: Union{PooledString, Union{PooledString, Missing}}}
    len = length(c)
    catg = getcategorical(c.file)
    tape = gettape(c.file, c.col)
    if S === PooledString
        refs = Dict{String, UInt32}()
        foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(getrefs(c.file, c.col)))
        values = Vector{UInt32}(undef, len)
        @simd for i = 1:len
            @inbounds values[i] = ref(tape[valind(i)])
        end
    else
        if catg
            refs = Dict{String, UInt32}()
            foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(getrefs(c.file, c.col)))
            missingref = UInt32(0)
            values = Vector{UInt32}(undef, len)
        else
            refs = Dict{Union{String, Missing}, UInt32}()
            foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(getrefs(c.file, c.col)))
            missingref = UInt32(length(refs) + 1)
            refs[missing] = missingref
            values = Vector{UInt32}(undef, len)
        end
        @simd for i = 1:len
            @inbounds offlen = tape[metaind(i)]
            @inbounds values[i] = ifelse(missingvalue(offlen), missingref, ref(tape[valind(i)]))
        end
    end
    if catg
        pool = CategoricalPool(refs)
        levels!(pool, sort(levels(pool)))
        A = CategoricalArray{T, 1}(values, pool)
    else
        A = PooledArray(PooledArrays.RefArray(values), refs)
    end
    return A
end

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
    @inbounds x = reinterp_func(T)(gettape(c.file, c.col)[valind(row)])
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{T, Missing}}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    @inbounds x = ifelse(missingvalue(offlen), missing, reinterp_func(T)(gettape(c.file, c.col)[valind(row)]))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Float64}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    @inbounds v = gettape(c.file, c.col)[valind(row)]
    @inbounds x = ifelse(intvalue(offlen), Float64(int64(v)), float64(v))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{Float64, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    @inbounds v = gettape(c.file, c.col)[valind(row)]
    @inbounds x = ifelse(missingvalue(offlen), missing, ifelse(intvalue(offlen), Float64(int64(v)), float64(v)))
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{String, PooledString}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds x = getrefs(c.file, c.col)[gettape(c.file, c.col)[valind(row)]]
    return x
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{String, Missing}, Union{PooledString, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    if missingvalue(offlen)
        return missing
    else
        @inbounds x = getrefs(c.file, c.col)[gettape(c.file, c.col)[valind(row)]]
        return x
    end
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{String}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    s = PointerString(pointer(getbuf(c.file), getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, gete(c.file)) : String(s)
end

@inline Base.@propagate_inbounds function Base.getindex(c::Column{Union{String, Missing}}, row::Int)
    @boundscheck checkbounds(c, row)
    @inbounds offlen = gettape(c.file, c.col)[metaind(row)]
    if missingvalue(offlen)
        return missing
    else
        s = PointerString(pointer(getbuf(c.file), getpos(offlen)), getlen(offlen))
        return escapedvalue(offlen) ? unescape(s, gete(c.file)) : String(s)
    end
end

function Base.getproperty(f::File, col::Symbol)
    i = findfirst(==(col), getnames(f))
    i === nothing && return getfield(f, col)
    return Column(f, i)
end
