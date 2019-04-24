# File iteration
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

struct Row{F}
    file::F
    row::Int
end
Base.propertynames(r::Row) = getfield(r, :file).names

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, :row))) of:")
    show(io, getfield(r, :file))
end

Base.eltype(f::F) where {F <: File} = Row{F}
Base.length(f::File) = f.rows

@inline function Base.iterate(f::File, st=1)
    st > f.rows && return nothing
    return Row(f, st), st + 1
end

@noinline badcolumnerror(name) = throw(ArgumentError("`$name` is not a valid column name"))

@inline function Base.getproperty(row::Row, name::Symbol)
    f = getfield(row, :file)
    i = findfirst(x->x===name, f.names)
    i === nothing && badcolumnerror(name)
    return getcell(f, f.types[i], i, getfield(row, :row))
end

@inline Base.getproperty(row::Row, ::Type{T}, col::Int, name::Symbol) where {T} =
    getcell(getfield(row, :file), T, col, getfield(row, :row))

# internal method for getting a cell value
@inline function getcell(f::File, ::Type{T}, col::Int, row::Int) where {T}
    indexoffset = ((f.cols * (row - 1) * 2) + (col - 1) * 2) + 1
    offlen = f.tape[indexoffset]
    missingvalue(offlen) && return missing
    return getvalue(Base.nonmissingtype(T), f, indexoffset, offlen, col)
end

getvalue(::Type{Int64}, f, indexoffset, offlen, col) = int64(f.tape[indexoffset + 1])
function getvalue(::Type{Float64}, f, indexoffset, offlen, col)
    @inbounds x = f.tape[indexoffset + 1]
    return intvalue(offlen) ? Float64(int64(x)) : float64(x)
end
getvalue(::Type{Date}, f, indexoffset, offlen, col) = date(f.tape[indexoffset + 1])
getvalue(::Type{DateTime}, f, indexoffset, offlen, col) = datetime(f.tape[indexoffset + 1])
getvalue(::Type{Bool}, f, indexoffset, offlen, col) = bool(f.tape[indexoffset + 1])
getvalue(::Type{Missing}, f, indexoffset, offlen, col) = missing
function getvalue(::Type{CatStr}, f, indexoffset, offlen, col)
    x = ref(f.tape[indexoffset + 1])
    return CatStr(x, f.categoricalpools[col])
end
function getvalue(::Union{Type{String}, Type{PooledString}}, f, indexoffset, offlen, col)
    if escapedvalue(offlen)
        return convert(f.escapedstringtype, WeakRefString(pointer(f.buf, getpos(offlen)), getlen(offlen)))
    else
        return unsafe_string(pointer(f.buf, getpos(offlen)), getlen(offlen))
    end
end
