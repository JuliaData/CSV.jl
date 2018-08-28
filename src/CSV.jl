module CSV

using Mmap, Dates, Random, Parsers, Tables, CategoricalArrays, WeakRefStrings

@inline Parsers.intern(::Type{WeakRefString{UInt8}}, x::Tuple{Ptr{UInt8}, Int}) = WeakRefString(x)

substitute(::Type{Union{T, Missing}}, ::Type{T1}) where {T, T1} = Union{T1, Missing}
substitute(::Type{T}, ::Type{T1}) where {T, T1} = T1
substitute(::Type{Missing}, ::Type{T1}) where {T1} = Missing

const CatStr = CategoricalString{UInt32}

struct Error <: Exception
    error::Parsers.Error
    row::Int
    col::Int
end

function Base.showerror(io::IO, e::Error)
    println(io, "CSV.Error on row=$(e.row), column=$(e.col):")
    showerror(io, e.error)
end

struct Row{F}
    file::F
    row::Int
end

# could just store NT internally and have Row take it as type parameter
# could separate kwargs out into individual fields and remove type parameter
struct File{NT, transpose, I, P, KW}
    io::I
    parsinglayers::P
    positions::Vector{Int}
    originalpositions::Vector{Int}
    lastparsedcol::Base.RefValue{Int}
    kwargs::KW
    pools::Vector{CategoricalPool{String, UInt32, CategoricalString{UInt32}}}
    strict::Bool
end

Base.eltype(f::F) where {F <: File} = Row{F}
Tables.schema(f::File{NT}) where {NT} = NT
Base.length(f::File{NT, transpose}) where {NT, transpose} = transpose ? f.lastparsedcol[] : length(f.positions)
Base.size(f::File{NamedTuple{names, types}}) where {names, types} = (length(f), length(names))
Base.size(f::File{NamedTuple{}}) = (0, 0)

@inline function Base.iterate(f::File{NT, transpose}, st=1) where {NT, transpose}
    st > length(f) && return nothing
    if transpose
        if st == 1
            f.positions .= f.originalpositions
        end
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
    if r.result isa Missing
        return missing
    else
        @inbounds pool = f.pools[col]
        return get🐱(pool, r.result::Tuple{Ptr{UInt8}, Int})
    end
end

@inline function parsefield(f, T, row, col, strict)
    r = Parsers.parse(f.parsinglayers, f.io, T; f.kwargs...)
    if !Parsers.ok(r.code)
        strict ? throw(Error(Parsers.Error(f.io, r), row, col)) :
            println("warning: failed parsing $T on row=$row, col=$col, error=$(Parsers.codes(r.code))")
    end
    return r.result
end

@noinline skipcells(f, n) = foreach(x->Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int}), 1:n)

Base.getproperty(csvrow::Row{F}, name::Symbol) where {F <: File{NT}} where {NT} =
    getproperty(csvrow, Tables.columntype(NT, name), Tables.columnindex(NT, name), name)

function Base.getproperty(csvrow::Row{F}, ::Type{T}, col::Int, name::Symbol) where {T, F <: File{NT, transpose}} where {NT, transpose}
    f = getfield(csvrow, 1)
    row = getfield(csvrow, 2)
    if transpose
        @inbounds Parsers.fastseek!(f.io, f.positions[col])
    else
        lastparsed = f.lastparsedcol[]
        if col === lastparsed + 1
        elseif col === 1
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
        elseif col > lastparsed + 1
            # skipping cells
            skipcells(f, col - lastparsed+1)
        elseif col !== lastparsed + 1
            # randomly seeking within row
            @inbounds Parsers.fastseek!(f.io, f.positions[row])
            skipcells(f, col - 1)
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
Base.propertynames(row::Row{F}) where {F <: File{NamedTuple{names, T}}} where {names, T} = names

File(source::Union{String, IO};
    # file options
    use_mmap::Bool=true,
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, UnitRange{Int}, Vector}=1,
    # by default, data starts immediately after header or start of file
    datarow::Int=-1,
    footerskip::Int=0,
    limit::Union{Nothing, Int}=nothing,
    transpose::Bool=false,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Char, String}=",",
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='\\',
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    decimal::Union{UInt8, Char, Nothing}=nothing,
    truestrings::Union{Vector{String}, Nothing}=nothing,
    falsestrings::Union{Vector{String}, Nothing}=nothing,
    # type options
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    allowmissing::Symbol=:all,
    categorical::Bool=false,
    strict::Bool=false,
    debug::Bool=false) =
    File(source, use_mmap, header, datarow, footerskip, limit, transpose, missingstrings, missingstring, delim, quotechar, openquotechar, closequotechar, escapechar, dateformat, decimal, truestrings, falsestrings, types, typemap, allowmissing, categorical, strict, debug)

# File(file, true, 1, -1, 0, false, String[], "", ",", '"', nothing, nothing, '\\', nothing, nothing, nothing, nothing, nothing, Dict{Type, Type}(), :all, false)
function File(source::Union{String, IO},
    # file options
    use_mmap::Bool,
    header::Union{Integer, UnitRange{Int}, Vector},
    datarow::Int,
    footerskip::Int,
    limit::Union{Nothing, Int},
    transpose::Bool,
    # parsing options
    missingstrings,
    missingstring,
    delim::Union{Char, String},
    quotechar::Union{UInt8, Char},
    openquotechar::Union{UInt8, Char, Nothing},
    closequotechar::Union{UInt8, Char, Nothing},
    escapechar::Union{UInt8, Char},
    dateformat::Union{String, Dates.DateFormat, Nothing},
    decimal::Union{UInt8, Char, Nothing},
    truestrings::Union{Vector{String}, Nothing},
    falsestrings::Union{Vector{String}, Nothing},
    # type options
    types,
    typemap::Dict,
    allowmissing::Symbol,
    categorical::Bool,
    strict::Bool,
    debug::Bool)
    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    io = getio(source, use_mmap)
    
    consumeBOM!(io)

    kwargs = getkwargs(dateformat, decimal, getbools(truestrings, falsestrings))
    missingstrings = isempty(missingstrings) ? [missingstring] : missingstrings
    d = string(delim)
    parsinglayers = Parsers.Sentinel(missingstrings) |>
                    x->Parsers.Strip(x, d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t') |>
                    (openquotechar !== nothing ? x->Parsers.Quoted(x, openquotechar, closequotechar, escapechar) : x->Parsers.Quoted(x, quotechar, escapechar)) |>
                    x->Parsers.Delimited(x, d, "\n", "\r", "\r\n")
    
    header = (isa(header, Integer) && header == 1 && datarow == 1) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header

    if transpose
        # need to determine names, columnpositions (rows), and ref
        rows, names, positions = datalayout_transpose(header, parsinglayers, io, datarow, footerskip)
        originalpositions = copy(positions)
        ref = Ref{Int}(min(something(limit, typemax(Int)), rows))
    else
        names, datapos = datalayout(header, parsinglayers, io, datarow)
        eof(io) && return File{NamedTuple{names, Tuple{(Missing for _ in names)...}}, false, typeof(io), typeof(parsinglayers), typeof(kwargs)}(io, parsinglayers, Int64[], Int64[], Ref{Int}(0), kwargs, CategoricalPool{String, UInt32, CatStr}[], strict)
        positions = rowpositions(io, quotechar % UInt8, escapechar % UInt8, limit)
        originalpositions = Int64[]
        footerskip > 0 && resize!(positions, length(positions) - footerskip)
        ref = Ref{Int}(0)
        debug && @show positions
    end

    if types isa Vector
        pools = CategoricalPool{String, UInt32, CatStr}[]
    else
        types, pools = detect(initialtypes(initialtype(allowmissing), types, names), io, positions, parsinglayers, kwargs, typemap, categorical, transpose, ref, debug)
    end

    !transpose && seek(io, positions[1])
    return File{NamedTuple{names, Tuple{types...}}, transpose, typeof(io), typeof(parsinglayers), typeof(kwargs)}(io, parsinglayers, positions, originalpositions, ref, kwargs, pools, strict)
end

include("filedetection.jl")
include("typedetection.jl")

getio(str::String, use_mmap) = IOBuffer(use_mmap ? Mmap.mmap(str) : Base.read(str))
getio(io::IO, use_mmap) = io

function consumeBOM!(io)
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        (!eof(io) && Parsers.readbyte(io) == 0xbb) || seek(io, startpos)
        (!eof(io) && Parsers.readbyte(io) == 0xbf) || seek(io, startpos)
    end
    return
end

getbools(::Nothing, ::Nothing) = nothing
getbools(trues::Vector{String}, falses::Vector{String}) = Parsers.Trie(append!([x=>true for x in trues], [x=>false for x in falses]))
getbools(trues::Vector{String}, ::Nothing) = Parsers.Trie(append!([x=>true for x in trues], ["false"=>false]))
getbools(::Nothing, falses::Vector{String}) = Parsers.Trie(append!(["true"=>true], [x=>false for x in falses]))

getkwargs(df::Nothing, dec::Nothing, bools::Nothing) = NamedTuple()
getkwargs(df::String, dec::Nothing, bools::Nothing) = (dateformat=Dates.DateFormat(df),)
getkwargs(df::Dates.DateFormat, dec::Nothing, bools::Nothing) = (dateformat=df,)

getkwargs(df::Nothing, dec::Union{UInt8, Char}, bools::Nothing) = (decimal=dec % UInt8,)
getkwargs(df::String, dec::Union{UInt8, Char}, bools::Nothing) = (dateformat=Dates.DateFormat(df), decimal=dec % UInt8)
getkwargs(df::Dates.DateFormat, dec::Union{UInt8, Char}, bools::Nothing) = (dateformat=df, decimal=dec % UInt8)

getkwargs(df::Nothing, dec::Nothing, bools::Parsers.Trie) = (bools=bools,)
getkwargs(df::String, dec::Nothing, bools::Parsers.Trie) = (dateformat=Dates.DateFormat(df), bools=bools)
getkwargs(df::Dates.DateFormat, dec::Nothing, bools::Parsers.Trie) = (dateformat=df, bools=bools)

getkwargs(df::Nothing, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (decimal=dec % UInt8, bools=bools)
getkwargs(df::String, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (dateformat=Dates.DateFormat(df), decimal=dec % UInt8, bools=bools)
getkwargs(df::Dates.DateFormat, dec::Union{UInt8, Char}, bools::Parsers.Trie) = (dateformat=df, decimal=dec % UInt8, bools=bools)

include("write.jl")
include("deprecated.jl")
include("validate.jl")

function __init__()
    CSV.File(joinpath(@__DIR__, "../test/testfiles/test_utf8.csv"), allowmissing=:auto) |> columntable
    return
end

end # module
