Tables.istable(::Type{<:File}) = true
Tables.schema(f::File{NamedTuple{names, types}}) where {names, types} = Tables.Schema(names, types)

struct Row{F}
    file::F
    row::Int
end

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, 2))) of:")
    show(io, getfield(r, 1))
end

Base.propertynames(row::Row{F}) where {F <: File{NamedTuple{names, T}}} where {names, T} = names

Base.eltype(f::F) where {F <: File} = Row{F}
Base.length(f::File{NT, transpose}) where {NT, transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
Base.size(f::File{NamedTuple{names, types}}) where {names, types} = (length(f), length(names))
Base.size(f::File{NamedTuple{}}) = (0, 0)

Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

@inline function Base.iterate(f::File{NT, transpose}, st=1) where {NT, transpose}
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
    return Row(f, st), st + 1
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

@inline Base.getproperty(csvrow::Row{F}, name::Symbol) where {F <: File{NamedTuple{names, types}}} where {names, types} =
    getproperty(csvrow, Tables.columntype(names, types, name), Tables.columnindex(names, name), name)

function Base.getproperty(csvrow::Row{F}, ::Type{T}, col::Int, name::Symbol) where {T, F <: File{NT, transpose}} where {NT, transpose}
    col === 0 && return missing
    f = getfield(csvrow, 1)
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

