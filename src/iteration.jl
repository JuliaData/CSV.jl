# File iteration
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

struct Row{T}
    file::File
    row::T
end

getfile(r::Row) = getfield(r, :file)
getrow(r::Row) = getfield(r, :row)

Base.propertynames(r::Row) = getnames(getfile(r))

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getrow(r))) of:")
    show(io, getfile(r))
end

Base.eltype(f::File) = Row
Base.length(f::File) = getrows(f)

function Base.iterate(f::File)
    cols = getcols(f)
    (cols == 0 || getrows(f) == 0) && return nothing
    c = getcolumn(f, 1)
    if typeof(c) <: LazyArrays.ApplyArray
        return Row(f, (1, 1, 1, c.args)), (1, 2, 2, c.args)
    else
        return Row(f, 1), 2
    end
end

@inline function Base.iterate(f::File, st::Int)
    st > length(f) && return nothing
    return Row(f, st), st + 1
end

@inline function Base.iterate(f::File, st)
    st[3] > length(f) && return nothing
    if st[2] + 1 > length(st[4][st[1]])
        st1 += 1
        st2 = 1
    else
        st1 = st[1]
        st2 = st[2]
    end
    return Row(f, st), (st1, st2, st + 1, st[4])
end

@noinline badcolumnerror(name) = throw(ArgumentError("`$name` is not a valid column name"))

@inline function Base.getproperty(row::Row{Int}, name::Symbol)
    column = getcolumn(getfile(row), name)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Base.getindex(row::Row{Int}, i::Int)
    column = getcolumn(getfile(row), i)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Base.getproperty(row::Row, name::Symbol)
    column = getcolumn(getfile(row), name)
    r = getrow(row)
    @inbounds x = column.args[r[1]][r[2]]
    return x
end

@inline function Base.getindex(row::Row, i::Int)
    column = getcolumn(getfile(row), i)
    r = getrow(row)
    @inbounds x = column.args[r[1]][r[2]]
    return x
end
