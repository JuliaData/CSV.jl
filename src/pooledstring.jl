"""
    PooledString <: AbstractString

String pointing to a pool. Behaves exactly like `String`, except
that [`Tables.allocatecolumn(PooledString, len)`](@ref) and
`Tables.allocatecolumn(Union{PooledString, Missing}, len)`
return a `PooledVector{String}` and `PooledVector{Union{String, Missing}}`
rather than a `Vector{String}` and a `Vector{Union{String,Missing}}`.
"""
struct PooledString <: AbstractString
    i::UInt32
    invpool::Dict{String,UInt32}
    s::String
end

Base.String(s::PooledString) = s.s
Base.convert(::Type{String}, s::PooledString) = String(s)
Base.string(x::PooledString) = String(s)
Base.:(==)(x::PooledString, y::PooledString) =
    (x.pool === y.pool && x.i == y.i) || String(x) == String(y)
Base.:(==)(x::PooledString, y::AbstractString) = String(x) == y
Base.:(==)(x::AbstractString, y::PooledString) = x == String(y)
Base.isless(x::PooledString, y::PooledString) = isless(String(x), String(y))
Base.isless(x::PooledString, y::String) = isless(String(x), y)
Base.isless(x::String, y::PooledString) = isless(x, String(y))
Base.length(x::PooledString) = length(String(x))
Base.lastindex(x::PooledString) = lastindex(String(x))
Base.sizeof(x::PooledString) = sizeof(String(x))
Base.nextind(x::PooledString, i::Int) = nextind(String(x), i)
Base.prevind(x::PooledString, i::Int) = prevind(String(x), i)
Base.iterate(x::PooledString) = iterate(String(x))
Base.iterate(x::PooledString, i::Int) = iterate(String(x), i)
@inline Base.getindex(x::PooledString, i::Int) = getindex(String(x), i)
Base.codeunit(x::PooledString, i::Integer) = codeunit(String(x), i)
Base.ascii(x::PooledString) = ascii(String(x))
Base.isvalid(x::PooledString) = isvalid(String(x))
Base.isvalid(x::PooledString, i::Integer) = isvalid(String(x), i)
Base.match(r::Regex, s::PooledString,
           idx::Integer=firstindex(s), add_opts::UInt32=UInt32(0)) =
    match(r, String(s), idx, add_opts)
Base.collect(x::PooledString) = collect(String(x))
Base.reverse(x::PooledString) = reverse(String(x))
Base.ncodeunits(x::PooledString) = ncodeunits(String(x))

Tables.allocatecolumn(::Type{PooledString}, len::Integer) =
    PooledArray(PooledArrays.RefArray(zeros(UInt32, len)),
                Dict{String,UInt32}())
Tables.allocatecolumn(::Type{Union{PooledString, Missing}}, len::Integer) =
    PooledArray(PooledArrays.RefArray(zeros(UInt32, len)),
                Dict{Union{String,Missing},UInt32}())

@inline function Base.setindex!(A::PooledArray, v::PooledString, i::Integer)
    @boundscheck checkbounds(A, i)
    if A.invpool === v.invpool
        r = v.i
        @inbounds A.refs[i] = r
    else
        invoke(setindex!, Tuple{PooledArray, Any, Integer}, A, v, i)
    end
end