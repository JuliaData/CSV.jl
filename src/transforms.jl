export transform

using Tables

struct TransformsRow{T, F}
    row::T
    funcs::F
end

Base.getproperty(row::TransformsRow, ::Type{T}, col::Int, nm::Symbol) where {T} = (getfunc(row, getfield(row, 2), col, nm))(getproperty(getfield(row, 1), T, col, nm))
Base.getproperty(row::TransformsRow, nm::Symbol) = (getfunc(row, getfield(row, 2), nm))(getproperty(getfield(row, 1), nm))
Base.propertynames(row::TransformsRow) = propertynames(getfield(row, 1))

struct Transforms{T, F, C}
    source::T
    funcs::F # NamedTuple of columnname=>transform function
end
Base.propertynames(t::Transforms) = propertynames(getfield(t, 1))
Base.getproperty(t::Transforms, nm::Symbol) = map(getfunc(t, getfield(t, 2), nm), getproperty(getfield(t, 1), nm))

transform(funcs) = x->transform(x, funcs)
transform(; kw...) = transform(kw.data)
function transform(src::T, funcs) where {T}
    cols = false
    if Tables.columnaccess(T)
        x = Tables.columns(src)
        cols = true
    else
        x = Tables.rows(src)
    end
    return Transforms{typeof(x), typeof(funcs), cols}(x, funcs)
end

getfunc(row, nt::NamedTuple, i, nm) = get(nt, i, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, i, nm) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, i, nm) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, i, nm) = get(d, i, identity)

getfunc(row, nt::NamedTuple, nm) = get(nt, nm, identity)
getfunc(row, d::Dict{String, <:Base.Callable}, nm) = get(d, String(nm), identity)
getfunc(row, d::Dict{Symbol, <:Base.Callable}, nm) = get(d, nm, identity)
getfunc(row, d::Dict{Int, <:Base.Callable}, nm) = get(d, findfirst(isequal(nm), propertynames(row)), identity)

Tables.istable(::Type{<:Transforms}) = true
Tables.rowaccess(::Type{<:Transforms}) = true
Tables.rows(t::Transforms{T, F, false}) where {T, F} = t
Tables.columnaccess(::Type{Transforms{T, F, C}}) where {T, F, C} = C
Tables.columns(t::Transforms{T, F, true}) where {T, F} = t
# vaoid relying on inference here and just let sinks figure things out
Tables.schema(t::Transforms) = nothing

Base.IteratorSize(::Type{<:Transforms{T}}) where {T} = Base.IteratorSize(T)
Base.length(t::Transforms) = length(getfield(t, 1))
Base.eltype(t::Transforms{T, F}) where {T, F} = TransformsRow{eltype(getfield(t, 1)), F}

@inline function Base.iterate(t::Transforms, st=())
    state = iterate(getfield(t, 1), st...)
    state === nothing && return nothing
    return TransformsRow(state[1], getfield(t, 2)), (state[2],)
end
