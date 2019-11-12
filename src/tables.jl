Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), _eltype.(gettypes(f)))
Tables.columns(f::File) = f
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return get(lookup, col) do
        getfield(f, col)
    end
end

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
    catg = c.catg
    tape = c.tape
    crefs = c.refs
    if S === PooledString
        refs = Dict{String, UInt32}()
        foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(crefs))
        values = Vector{UInt32}(undef, len)
        @simd for i = 1:len
            @inbounds values[i] = ref(tape[i])
        end
    else
        if catg
            refs = Dict{String, UInt32}()
            foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(crefs))
            missingref = UInt32(0)
            values = Vector{UInt32}(undef, len)
        else # Union{PooledString, Missing}
            refs = Dict{Union{String, Missing}, UInt32}()
            foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(crefs))
            missingref = UInt32(length(refs) + 1)
            refs[missing] = missingref
            values = Vector{UInt32}(undef, len)
        end
        @simd for i = 1:len
            @inbounds v = ref(tape[i])
            @inbounds values[i] = ifelse(v == UInt32(0), missingref, v)
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

Base.@propagate_inbounds function Base.getindex(c::Column{Missing}, row::Int)
    @boundscheck checkbounds(c, row)
    return missing
end

Base.@propagate_inbounds function Base.getindex(c::Column{T, S}, row::Int) where {T, S}
    @boundscheck checkbounds(c, row)
    @inbounds x = reinterp_func(T)(c.tape[row])
    return x
end

Base.@propagate_inbounds function Base.getindex(c::Column{Union{T, Missing}, S}, row::Int) where {T, S}
    @boundscheck checkbounds(c, row)
    @inbounds x = c.tape[row]
    return ifelse(x === c.sentinel, missing, reinterp_func(T)(x))
end

Base.@propagate_inbounds function Base.getindex(c::Column{T, PooledString}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds x = c.tape[row]
    @inbounds str = c.refs[x]
    return str
end

Base.@propagate_inbounds function Base.getindex(c::Column{T, Union{PooledString, Missing}}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds x = c.tape[row]
    if x == 0
        return missing
    else
        @inbounds y = c.refs[x]
        return y
    end
end

Base.@propagate_inbounds function Base.getindex(c::Column{T, String}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds offlen = c.tape[row]
    s = PointerString(pointer(c.buf, getpos(offlen)), getlen(offlen))
    return escapedvalue(offlen) ? unescape(s, c.e) : String(s)
end

Base.@propagate_inbounds function Base.getindex(c::Column{T, Union{String, Missing}}, row::Int) where {T}
    @boundscheck checkbounds(c, row)
    @inbounds offlen = c.tape[row]
    if missingvalue(offlen)
        return missing
    else
        s = PointerString(pointer(c.buf, getpos(offlen)), getlen(offlen))
        return escapedvalue(offlen) ? unescape(s, c.e) : String(s)
    end
end
