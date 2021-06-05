struct Files
    sources
    kw
end

Files(sources; kw...) = Files(sources, kw)

Base.IteratorSize(x::Files) = Base.IteratorSize(x.sources)
Base.IteratorEltype(::Type{Files}) = File{false}
Base.length(x::Files) = length(x.sources)

function Base.iterate(x::Files)
    state = iterate(x.sources)
    state === nothing && return nothing
    source, st = state
    return File(source; threaded=false, x.kw...), st
end

function Base.iterate(x::Files, st)
    state = iterate(x.sources, st)
    state === nothing && return nothing
    source, st = state
    return File(source; threaded=false, x.kw...), st
end

Tables.partitions(x::Files) = x
