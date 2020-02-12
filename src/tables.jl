Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), _eltype.(gettypes(f)))
Tables.columns(f::File) = f
Tables.columnnames(f::File) = getnames(f)
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return get(lookup, col) do
        getfield(f, col)
    end
end

Tables.getcolumn(f::File, nm::Symbol) = getcolumn(f, nm)
Tables.getcolumn(f::File, i::Int) = getcolumn(f, i)

# Column2
 # copy
 # BroadcastStyle/broadcasted
Base.mapreduce(f, op, xs::Column2; kwargs...) = Base.mapfoldl(f, op, xs; kwargs...)

@inline function Base.iterate(c::Column2)
    length(c) == 0 && return nothing
    array_lens = [length(x) for x in c.columns]
    st = ThreadedIterationState(2, 1, 2, array_lens[1], array_lens)
    return c.columns[1][1], st
end

@inline function Base.iterate(c::Column2, st)
    row = st.row
    array_index = st.array_index
    array_i = st.array_i
    row > length(c) && return nothing
    if array_i + 1 > st.array_len
        st.array_index += 1
        st.array_i = 1
        st.array_len = st.array_lens[min(end, st.array_index)]
    else
        st.array_i += 1
    end
    st.row += 1
    return c.columns[array_index][array_i], st
end

Base.@propagate_inbounds function Base.getindex(c::Column2, i::Integer)
    i′ = i
    for C in c.columns
        n = length(C)
        i′ ≤ n && return C[i′]
        i′ -= n
    end
    throw(BoundsError(c, i))
end

Base.copy(c::Union{Column{Missing, Missing}, Column2{Missing, Missing}}) = missings(length(c))

function Base.copy(c::Union{Column{T}, Column2{T}}) where {T}
    len = length(c)
    A = (T == String || T == Union{String, Missing}) ? StringVector{T}(undef, len) : Vector{T}(undef, len)
    if T <: Int64 || T <: Float64 || T <: Dates.TimeType
        if c isa Column
            memcpy!(pointer(A), 1, pointer(c.tape), 1, len * 8)
            return A
        else
            doff = 1
            for cx in c.columns
                n = length(cx) * 8
                memcpy!(pointer(A), doff, pointer(cx.tape), 1, n)
                doff += n
            end
            return A
        end
    end
    for (i, x) in enumerate(c)
        @inbounds A[i] = x
    end
    return A
end

function Base.copy(col::Union{Column{T, S}, Column2{T, S}}) where {T <: Union{String, Union{String, Missing}}, S <: Union{PooledString, Union{PooledString, Missing}}}
    len = length(col)
    c = col isa Column ? col : col.columns[1]
    catg = c.catg
    crefs = c.refs
    if S === PooledString
        refs = Dict{String, UInt32}()
        foreach(x->setindex!(refs, UInt32(x[1]), x[2]), enumerate(crefs))
        values = Vector{UInt32}(undef, len)
        if col isa Column
            tape = c.tape
            @simd for i = 1:len
                @inbounds values[i] = ref(tape[i])
            end
        else
            i = 1
            for cx in col.columns
                tape = cx.tape
                @simd for j = 1:length(cx)
                    @inbounds values[i] = ref(tape[j])
                    i += 1
                end
            end
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
        if col isa Column
            tape = c.tape
            @simd for i = 1:len
                @inbounds v = ref(tape[i])
                @inbounds values[i] = ifelse(v == UInt32(0), missingref, v)
            end
        else
            i = 1
            for cx in col.columns
                tape = cx.tape
                @simd for j = 1:length(cx)
                    @inbounds v = ref(tape[j])
                    @inbounds values[i] = ifelse(v == UInt32(0), missingref, v)
                    i += 1
                end
            end
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
