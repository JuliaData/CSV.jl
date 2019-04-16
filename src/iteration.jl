# File iteration
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

struct Row{F}
    file::F
    row::Int
end
Base.propertynames(r::Row) = getfield(r, 1).names

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, 2))) of:")
    show(io, getfield(r, 1))
end

Base.eltype(f::F) where {F <: File} = Row{F}
Base.length(f::File) = f.rows

@inline function Base.iterate(f::File, st=1)
    st > f.rows && return nothing
    return Row(f, st), st + 1
end

@noinline badcolumnerror(name) = throw(ArgumentError("`$name` is not a valid column name"))

@inline function Base.getproperty(row::Row, name::Symbol)
    f = getfield(row, 1)
    i = findfirst(x->x===name, f.names)
    i === nothing && badcolumnerror(name)
    return getcell(f, f.types[i], i, getfield(row, 2))
end

Base.getproperty(row::Row, ::Type{T}, col::Int, name::Symbol) where {T} =
    getcell(getfield(row, 1), T, col, getfield(row, 2))

# internal method for getting a cell value
function getcell(f::File, ::Type{T}, col::Int, row::Int) where {T}
    indexoffset = ((f.cols * (row - 1) * 2) + (col - 1) * 2) + 1
    offlen = f.tape[indexoffset]
    missingvalue(offlen) && return missing
    type = typebits(f.typecodes[col])
    if type === INT
        return int64(f.tape[indexoffset + 1])
    elseif type === FLOAT
        return float64(f.tape[indexoffset + 1])
    elseif type === DATE
        return date(f.tape[indexoffset + 1])
    elseif type === DATETIME
        return datetime(f.tape[indexoffset + 1])
    elseif type === BOOL
        return bool(f.tape[indexoffset + 1])
    elseif type === MISSINGTYPE
        return missing
    elseif type === POOL && f.categorical
        x = ref(f.tape[indexoffset + 1])
        return CatStr(x, f.categoricalpools[col])
    else # STRING
        return unsafe_string(pointer(f.io.data, offlen >> 16), offlen & 0x000000000000ffff)
    end
end
