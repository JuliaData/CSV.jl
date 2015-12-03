type UnsafeBuffer <: IO
    data::Vector{UInt8}
    ptr::Int
    size::Int
end

UnsafeBuffer(data::Vector{UInt8}) = UnsafeBuffer(data,1,length(data))
Base.convert(::Type{UnsafeBuffer}, io::IOBuffer) = UnsafeBuffer(io.data, io.ptr, io.size)

@inline function Base.read(from::UnsafeBuffer, ::Type{UInt8}=UInt8)
    @inbounds byte = from.data[from.ptr]
    from.ptr = from.ptr + 1
    return byte
end

@inline function Base.peek(from::UnsafeBuffer)
    @inbounds byte = from.data[from.ptr]
    return byte
end

Base.eof(io::UnsafeBuffer) = (io.ptr-1 == io.size)
Base.position(io::UnsafeBuffer) = io.ptr-1
Base.seekstart(io::UnsafeBuffer) = (io.ptr = 1; return nothing)
Base.seek(io::UnsafeBuffer, n::Integer) = (io.ptr = n+1; return nothing)
