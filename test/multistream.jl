# Previous versions assumed that nb_available could accurately check for an empty CSV, but
# this doesn't work reliably for streams because nb_available only checks buffered bytes
# (see issue #77). This test verifies that even when nb_available would return 0 on a stream
# the full stream is still read.

mutable struct MultiStream{S<:IO} <: IO
    streams::Array{S}
    index::Int
end

function MultiStream{S<:IO}(streams::AbstractArray{S})
    MultiStream(streams, 1)
end

function refill(s::MultiStream)
    while eof(s.streams[s.index]) && s.index < length(s.streams)
        close(s.streams[s.index])
        s.index += 1
    end
end

function Base.close(s::MultiStream)
    for i in s.index:length(s.streams)
        close(s.streams[i])
    end
    s.index = length(s.streams)
    nothing
end

function Base.eof(s::MultiStream)
    eof(s.streams[s.index]) && s.index == length(s.streams)
end

function Base.read(s::MultiStream, ::Type{UInt8})
    refill(s)
    read(s.streams[s.index], UInt8)::UInt8
end

function Base.nb_available(s::MultiStream)
    nb_available(s.streams[s.index])
end

stream = MultiStream(
    [IOBuffer(""), IOBuffer("a,b,c\n1,2,3\n"), IOBuffer(""), IOBuffer("4,5,6")]
)

@test nb_available(stream) == 0
@test CSV.read(stream) == CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"))
