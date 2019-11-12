# File iteration
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

Base.@propagate_inbounds function Base.getindex(f::File{false}, row::Int)
    @boundscheck checkbounds(f, row)
    return Row{false}(getnames(f), getcolumns(f), getlookup(f), row, 0, 0)
end

Base.@propagate_inbounds function Base.getindex(f::File{true}, row::Int)
    @boundscheck checkbounds(f, row)
    c = getcolumn(f, 1)
    i = row
    for (j, A) in enumerate(c.args)
        n = length(A)
        i <= n && return Row{true}(getnames(f), getcolumns(f), getlookup(f), row, j, i)
        i -= n
    end
    return Row{true}(getnames(f), getcolumns(f), getlookup(f), row, 1, 1)
end

# non-threaded file
@inline function Base.iterate(f::File{false}, st::Int=1)
    st > length(f) && return nothing
    return Row{false}(getnames(f), getcolumns(f), getlookup(f), st, 0, 0), st + 1
end

# threaded file
mutable struct RowIterationState
    row::Int64
    array_index::Int64
    array_i::Int64
    array_len::Int64
    array_lens::Vector{Int64}
end

@inline function Base.iterate(f::File{true})
    cols = getcols(f)
    (cols == 0 || getrows(f) == 0) && return nothing
    c = getcolumn(f, 1)
    array_lens = [length(x) for x in c.args]
    st = RowIterationState(2, 1, 2, array_lens[1], array_lens)
    return Row{true}(getnames(f), getcolumns(f), getlookup(f), 1, 1, 1), st
end

@inline function Base.iterate(f::File{true}, st)
    row = st.row
    array_index = st.array_index
    array_i = st.array_i
    row > length(f) && return nothing
    if array_i + 1 > st.array_len
        st.array_index += 1
        st.array_i = 1
        st.array_len = st.array_lens[min(end, st.array_index)]
    else
        st.array_i += 1
    end
    st.row += 1
    return Row{true}(getnames(f), getcolumns(f), getlookup(f), row, array_index, array_i), st
end

@noinline badcolumnerror(name) = throw(ArgumentError("`$name` is not a valid column name"))

@inline function Base.getproperty(row::Row{false}, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Base.getindex(row::Row{false}, col::Int)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Base.getindex(row::Row{false}, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Base.getproperty(row::Row{true}, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column.args[getarrayindex(row)][getarrayi(row)]
    return x
end

@inline function Base.getindex(row::Row{true}, col::Int)
    column = getcolumn(row, col)
    @inbounds x = column.args[getarrayindex(row)][getarrayi(row)]
    return x
end

@inline function Base.getindex(row::Row{true}, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column.args[getarrayindex(row)][getarrayi(row)]
    return x
end
