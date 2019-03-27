# File iteration
struct Row{F}
    file::F
    row::Int
end
# Base.propertynames(r::Row) = getfield(r, 1).names

# function Base.show(io::IO, r::Row)
#     println(io, "CSV.Row($(getfield(r, 2))) of:")
#     show(io, getfield(r, 1))
# end

# Base.eltype(f::F) where {F <: File} = Row{F}
# Base.length(f::File{transpose}) where {transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
# tablesize(f::File) = (length(f), length(f.names))

# @inline function Base.iterate(f::File{transpose}, st=1) where {transpose}
#     st > length(f) && return nothing
#     # println("row=$st")
#     if transpose
#         st === 1 && (f.positions .= f.originalpositions)
#     else
#         @inbounds Parsers.fastseek!(f.io, f.positions[st])
#         f.currentrow[] = st
#         f.lastparsedcol[] = 0
#         f.lastparsedcode[] = Parsers.SUCCESS
#     end
#     return Row(f, st), st + 1
# end

# Tables.rowaccess(::Type{<:File}) = true
# Tables.rows(f::File) = f

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

@noinline parsererror(f, r, row, col) = throw(Error(Parsers.Error(f.io, r), row, col))
@noinline printwarning(T, row, col, r) = println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")

parsefield(f, ::Type{CatStr}, row, col) = parsefield(f, Tuple{Ptr{UInt8}, Int}, row, col)
@inline function parsefield(f, T, row, col)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    f.lastparsedcode[] = r.code
    if !Parsers.ok(r.code)
        f.strict ? parsererror(f, r, row, col) :
            f.silencewarnings ? nothing : printwarning(T, row, col, r)
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

@noinline badcolumnerror(name) = throw(ArgumentError("`$name` is not a valid column name"))

@inline function Base.getproperty(row::Row, name::Symbol)
    f = getfield(row, 1)
    i = findfirst(x->x===name, f.names)
    i === nothing && badcolumnerror(name)
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
