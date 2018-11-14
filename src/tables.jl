Tables.istable(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(f.names, f.types)

Tables.columnaccess(::Type{File{t, c, I, P, KW}}) where {t, c, I, P, KW} = c

struct Columns
    names
    columns
end
Base.propertynames(c::Columns) = getfield(c, 1)
function Base.getproperty(c::Columns, nm::Symbol)
    getfield(c, 2)[findfirst(x->x==nm, getfield(c, 1))]
end

function Tables.columns(f::File{transpose, true}) where {transpose}
    len = length(f)
    types = f.types
    columns = Any[Tables.allocatecolumn(T, len) for T in types]
    funcs = [get(FUNCTIONMAP, parsingtype(T), FUNC_ANY[]) for T in types]
    transpose && (f.positions .= f.originalpositions)
    for row = 1:len
        @inbounds Parsers.fastseek!(f.io, f.positions[row])
        f.currentrow[] = row
        f.lastparsedcol[] = 0
        f.lastparsedcode[] = Parsers.SUCCESS
        for col = 1:length(types)
            @inbounds ccall(funcs[col], Cvoid, (Any, Any, Cssize_t, Cssize_t), columns[col], f, col, row)
        end
    end
    return Columns(f.names, columns)
end

struct Row{F}
    file::F
    row::Int
end
Base.propertynames(r::Row) = getfield(r, 1).names

function Base.show(io::IO, r::Row)
    println(io, "CSV.Row($(getfield(r, 2))) of:")
    show(io, getfield(r, 1))
end

# File iteration
Base.eltype(f::F) where {F <: File} = Row{F}
Base.length(f::File{transpose}) where {transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
tablesize(f::File) = (length(f), length(f.names))

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
    return Row(f, st), st + 1
end

Tables.rowaccess(::Type{<:File}) = true
Tables.rows(f::File) = f

parsingtype(::Type{Missing}) = Missing
parsingtype(::Type{Union{Missing, T}}) where {T} = T
parsingtype(::Type{T}) where {T} = T

@inline function getparsedvalue(f, r, ::Type{CatStr}, col)
    if r.result isa Missing
        return missing
    else
        @inbounds pool = f.pools[col]
        val = r.result::Tuple{Ptr{UInt8}, Int}
        i = get(pool, val, nothing)
        if i === nothing
            i = get!(pool, unsafe_string(val[1], val[2]))
            issorted(levels(pool)) || levels!(pool, sort(levels(pool)))
        end
        return CatStr(i, pool)
    end
end
getparsedvalue(f, r, T, col) = r.result

parsefield(f, ::Type{CatStr}, row, col) = parsefield(f, Tuple{Ptr{UInt8}, Int}, row, col)
@inline function parsefield(f, T, row, col)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    f.lastparsedcode[] = r.code
    if !Parsers.ok(r.code)
        f.strict ? throw(Error(Parsers.Error(f.io, r), row, col)) :
            f.silencewarnings ? nothing :
                println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")
    end
    return r
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

getsetInt!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, Int64, col, i), i); return nothing)
getsetFloat!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, Float64, col, i), i); return nothing)
getsetDate!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, Date, col, i), i); return nothing)
getsetDateTime!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, DateTime, col, i), i); return nothing)
getsetBool!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, Bool, col, i), i); return nothing)
getsetString!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, String, col, i), i); return nothing)
getsetMissing!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, Missing, col, i), i); return nothing)
getsetWeakRefString!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, WeakRefString{UInt8}, col, i), i); return nothing)
getsetCatStr!(column, f::File, col::Int, i::Int) = (r = setindex!(column, getproperty(f, CatStr, col, i), i); return nothing)
function getsetAny!(column, f::File, col::Int, i::Int)
    setindex!(column, getproperty(f, f.types[col], col, i), i)
    return
end
const FUNC_ANY = Ref{Ptr{Cvoid}}()

const FUNCTIONMAP = Dict(
    Int64 => C_NULL,
    Float64 => C_NULL,
    Date => C_NULL,
    DateTime => C_NULL,
    Bool => C_NULL,
    String => C_NULL,
    Missing => C_NULL,
    WeakRefString{UInt8} => C_NULL,
    CatStr => C_NULL,
)

@noinline badcolumnerror(name) = "`$name` is not a valid column name"

@inline function Base.getproperty(row::Row, name::Symbol)
    f = getfield(row, 1)
    i = findfirst(x->x===name, f.names)
    i === nothing && throw(ArgumentError(badcolumnerror(name)))
    return getproperty(f, f.types[i], i, getfield(row, 2))
end

Base.getproperty(row::Row, ::Type{T}, col::Int, name::Symbol) where {T} =
    getproperty(getfield(row, 1), T, col, getfield(row, 2))

function Base.getproperty(f::File{transpose}, ::Type{T}, col::Int, row::Int) where {transpose, T}
    col === 0 && return missing
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
    S = parsingtype(T)
    r = getparsedvalue(f, parsefield(f, S, row, col), S, col)
    if transpose
        @inbounds f.positions[col] = position(f.io)
    else
        f.lastparsedcol[] = col
    end
    return r
end
