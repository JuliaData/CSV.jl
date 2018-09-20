Tables.istable(::Type{<:File}) = true
Tables.schema(f::File) = Tables.Schema(f.names, f.types)

Tables.columnaccess(::Type{File{t, c, I, P, KW}}) where {t, c, I, P, KW} = c

struct Columns
    names
    columns
end
Base.propertynames(c::Columns) = getfield(c, 1)
function Base.getproperty(c::Columns, nm::Symbol)
    getfield(c, 2)[findfirst(x->x==nm, getfield(c, 1))]
end

function Tables.columns(@nospecialize(f::File{transpose, true})) where {transpose}
    len = length(f)
    types = f.types
    columns = Any[Tables.allocatecolumn(T, len) for T in types]
    for (i, row) in enumerate(f)
        for col = 1:length(types)
            columns[col][i] = getproperty(row, types[col], col, :_)
        end
    end
    return Columns(f.names, columns)
end

# row interfaces
abstract type Row end

struct UntypedRow{F} <: Row
    file::F
    row::Int
end
Base.propertynames(r::UntypedRow) = getfield(r, 1).names

struct RowIterator{names, types, F}
    file::F
end
Tables.schema(r::RowIterator) = Tables.Schema(r.file.names, r.file.types)

struct TypedRow{F} <: Row
    rowiterator::F
    row::Int
end
Base.propertynames(r::TypedRow{F}) where {F <: RowIterator{names}} where {names} = names

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, 2))) of:")
    show(io, getfield(r, 1))
end

getranspose(r::UntypedRow{F}) where {F <: File{transpose}} where {transpose} = transpose
getranspose(r::RowIterator{N, T, F}) where {N, T, F <: File{transpose}} where {transpose} = transpose
getranspose(r::TypedRow) = getranspose(getfield(r, 1))

# File iterates UntypedRows
Base.eltype(f::F) where {F <: File} = UntypedRow{F}
Base.length(f::File{transpose}) where {transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
Base.size(f::File) = (length(f), length(f.names))

@inline function Base.iterate(f::File{transpose}, st=1) where {transpose}
    st > length(f) && return nothing
    # println("row=$st")
    if transpose
        st === 1 && (f.positions .= f.originalpositions)
    else
        @inbounds Parsers.fastseek!(f.io, f.positions[st])
        f.currentrow[] = st
        f.lastparsedcol[] = 0
        f.lastparsedcode[] = Parsers.SUCCESS
    end
    return UntypedRow(f, st), st + 1
end

# Tables.rows(f) -> RowIterator
Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::F) where {F <: File} = RowIterator{Tuple(f.names), Tuple{f.types...}, F}(f)

Base.eltype(f::R) where {R <: RowIterator} = TypedRow{R}
Base.length(f::RowIterator) = getranspose(f) ? f.file.lastparsedcol[] : length(f.file.positions)
Base.size(f::RowIterator) = (length(f.file), length(f.file.names))

# RowIterator iterates TypedRows
@inline function Base.iterate(r::RowIterator, st=1)
    f = r.file
    st > length(f) && return nothing
    transpose = getranspose(r)
    # println("row=$st")
    if transpose
        st === 1 && (f.positions .= f.originalpositions)
    else
        @inbounds Parsers.fastseek!(f.io, f.positions[st])
        f.currentrow[] = st
        f.lastparsedcol[] = 0
        f.lastparsedcode[] = Parsers.SUCCESS
    end
    return TypedRow(r, st), st + 1
end

parsingtype(::Type{Missing}) = Missing
parsingtype(::Type{Union{Missing, T}}) where {T} = T
parsingtype(::Type{T}) where {T} = T

@inline function get🐱(pool::CategoricalPool, val::Tuple{Ptr{UInt8}, Int})
    index = Base.ht_keyindex2!(pool.invindex, val)
    if index > 0
        @inbounds v = pool.invindex.vals[index]
        return CatStr(v, pool)
    else
        v = CategoricalArrays.push_level!(pool, val)
        return CatStr(v, pool)
    end
end

@inline function parsefield(f, ::Type{CatStr}, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int})
    f.lastparsedcode[] = r.code
    if r.result isa Missing
        return missing
    else
        @inbounds pool = f.pools[col]
        return get🐱(pool, r.result::Tuple{Ptr{UInt8}, Int})
    end
end

@inline function parsefield(f, T, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    f.lastparsedcode[] = r.code
    if !Parsers.ok(r.code)
        strict ? throw(Error(Parsers.Error(f.io, r), row, col)) :
            println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")
    end
    return r.result
end

@noinline function skipcells(f, n)
    r = Parsers.Result(Tuple{Ptr{UInt8}, Int})
    for _ = 1:n
        r.code = Parsers.SUCCESS
        Parsers.parse!(f.parsinglayers, f.io, r)
        if newline(r.code)
            f.lastparsedcode[] = r.code
            return false
        end
    end
    f.lastparsedcode[] = r.code
    return true
end

@inline function Base.getproperty(csvrow::UntypedRow, name::Symbol)
    f = getfield(csvrow, 1)
    i = findfirst(x->x===name, f.names)
    return getproperty(csvrow, f.types[i], i, name)
end
@inline Base.getproperty(csvrow::TypedRow{F}, name::Symbol) where {F <: RowIterator{names, types}} where {names, types} =
    getproperty(csvrow, Tables.columntype(names, types, name), Tables.columnindex(names, name), name)

getfile(f::File) = f
getfile(r::RowIterator) = r.file

function Base.getproperty(csvrow::Row, ::Type{T}, col::Int, name::Symbol) where {T}
    col === 0 && return missing
    transpose = getranspose(csvrow)
    f = getfile(getfield(csvrow, 1))
    row = getfield(csvrow, 2)
    if transpose
        @inbounds Parsers.fastseek!(f.io, f.positions[col])
    else
        if f.currentrow[] != row
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            f.lastparsedcol[] = 0
        end
        lastparsed = f.lastparsedcol[]
        if col === lastparsed + 1
            if newline(f.lastparsedcode[])
                f.lastparsedcol[] = col
                return missing
            end
        elseif col > lastparsed + 1
            # skipping cells
            if newline(f.lastparsedcode[]) || !skipcells(f, col - (lastparsed + 1))
                f.lastparsedcol[] = col
                return missing
            end
        else
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            # randomly seeking within row
            if !skipcells(f, col - 1)
                f.lastparsedcol[] = col
                return missing
            end
        end
    end
    r = parsefield(f, parsingtype(T), row, col, f.strict)
    if transpose
        @inbounds f.positions[col] = position(f.io)
    else
        f.lastparsedcol[] = col
    end
    return r
end
