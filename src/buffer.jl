const MAXBUFFERSIZE = 8192

mutable struct Buffer{I, P1, P2} <: IO
    io::I
    data::Vector{UInt8}
    pos::Int
    len::Int
end

# function Buffer(io::IOStream)
#     buf = Vector{UInt8}(MAXBUFFERSIZE)
#     P1 = Base.unsafe_convert(Ptr{Void}, io.ios)
#     P2 = Base.unsafe_convert(Ptr{Void}, buf)
#     len = Int(ccall(:ios_read, Csize_t, (Ptr{Void}, Ptr{Void}, Csize_t), P1, P2, MAXBUFFERSIZE))
#     return Buffer{IOStream, P1, P2}(io, buf, 1, len)
# end
#
# function Buffer(io::I) where I <: IO
#     buf = Vector{UInt8}(MAXBUFFERSIZE)
#     len = readbytes!(io, buf, MAXBUFFERSIZE)
#     return Buffer{I, C_NULL, C_NULL}(io, buf, 1, len)
# end
Buffer(io::IO) = Buffer(Base.read(io))
Buffer(str::String) = Buffer(Vector{UInt8}(str))
Buffer(io::Vector{UInt8}) = Buffer{Vector{UInt8}, C_NULL, C_NULL}(io, io, 1, length(io))

Base.position(io::Buffer) = io.pos

Base.peek(io::Buffer) = (@inbounds byte = io.data[io.pos]; return byte)
Base.read(io::Buffer, ::Type{UInt8}=UInt8) = (@inbounds byte = io.data[io.pos]; io.pos += 1; return byte)
incr!(io::Buffer) = (io.pos += 1; return nothing)
Base.seek(io::Buffer, pos::Int) = (io.pos = pos; return nothing)

# @inline function Base.eof(io::Buffer{IOStream, P1, P2}) where {P1, P2}
#     io.pos <= io.len && return false
#     io.len = Int(ccall(:ios_read, Csize_t, (Ptr{Void}, Ptr{Void}, Csize_t), P1, P2, MAXBUFFERSIZE))
#     io.len == 0 && return true
#     io.pos = 1
#     return false
# end

# @inline function Base.eof(io::Buffer{I, P1, P2}) where {I, P1, P2}
#     io.pos <= io.len && return false
#     eof(io.io) && return true
#     io.len = readbytes!(io.io, io.data, MAXBUFFERSIZE)
#     io.pos = 1
#     return false
# end

@inline function Base.eof(io::Buffer{Vector{UInt8}})
    if io.pos > io.len
        io.pos = 1
        return true
    end
    return false
end
