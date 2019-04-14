module CSV

# stdlib
using Mmap, Dates, Random, Unicode
using SIMD, WeakRefStrings, Parsers, Tables

getoff(off::UInt64) = off >> 16
newline(len::UInt64) = len & 0x0000000000008000 == 0x0000000000008000
getrawlen(len::UInt64) = len & 0x000000000000ffff
getlen(len::UInt64) = Int(len & 0x0000000000007fff)
function getval(buf, tape, i)
    offlen = tape[i]
    pos = CSV.getoff(offlen)
    len = CSV.getlen(offlen)
    return WeakRefString(pointer(buf, pos), len)
end

include("utils.jl")
include("structuredetection.jl")

struct Tape <: AbstractArray{WeakRefString{UInt8}, 2}
    buf::Vector{UInt8}
    ptr::Ptr{UInt8}
    tape::Vector{UInt64}
    endidx::Int
    rows::Int
    cols::Int
end

Base.size(t::Tape) = (t.rows, t.cols)
function Base.getindex(t::Tape, I::Vararg{Int, 2})
    i = ((I[1] - 1) * t.cols) + I[2]
    offlen = t.tape[2 * (i - 1) + 1]
    pos = getoff(offlen)
    len = getlen(offlen)
    return WeakRefString(t.ptr + pos - 1, len)
end
Base.setindex!(t::Tape, v, args...) = throw(ArgumentError("CSV.Tape is read-only"))

struct File
    names::Vector{Symbol}
    tape::Tape
    dataidx::Int
    rows::Int
    typecodes::Vector{TypeCode}
end

function Base.show(io::IO, f::File)
    println(io, "CSV.File:")
    show(io, f.tape)
    return
end

function File2(buf)
    oq = cq = eq = UInt8('"')
    detectstructure(buf, UInt8(','), oq, cq, eq, Val(SAME), Val(NEWLINE), Val(nothing))
end

# function File(source::Union{Vector{UInt8}, String, IO};
#     # file options
#     # header can be a row number, range of rows, or actual string vector
#     header::Union{Integer, UnitRange{Int}, Vector}=1,
#     normalizenames::Bool=false,
#     # by default, data starts immediately after header or start of file
#     datarow::Int=-1,
#     skipto::Union{Nothing, Int}=nothing,
#     footerskip=nothing,
#     limit::Union{Integer, Nothing}=nothing,
#     transpose::Bool=false,
#     comment::Union{String, Nothing}=nothing,
#     use_mmap::Bool=!Sys.iswindows(),
#     # parsing options
#     missingstrings=String[],
#     missingstring="",
#     delim::Union{Nothing, Char, String}=nothing,
#     ignorerepeated::Bool=false,
#     quotechar::Union{UInt8, Char}='"',
#     openquotechar::Union{UInt8, Char, Nothing}=nothing,
#     closequotechar::Union{UInt8, Char, Nothing}=nothing,
#     escapechar::Union{UInt8, Char}='"',
#     dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
#     decimal::Union{UInt8, Char, Nothing}=nothing,
#     truestrings::Union{Vector{String}, Nothing}=nothing,
#     falsestrings::Union{Vector{String}, Nothing}=nothing,
#     # type options
#     type=nothing,
#     types=nothing,
#     typemap=nothing,
#     allowmissing::Symbol=:auto,
#     categorical::Union{Bool, Float64}=false,
#     pool::Union{Bool, Float64}=false,
#     strict::Bool=false,
#     silencewarnings::Bool=false,
#     debug::Bool=false,
#     kw...)

function File(source::Union{Vector{UInt8}, String, IO},
    type=nothing)
    # file options
    # header can be a row number, range of rows, or actual string vector
    header=1
    normalizenames=false
    # by default data starts immediately after header or start of file
    datarow=-1
    skipto=nothing
    footerskip=nothing
    limit=nothing
    transpose=false
    comment=nothing
    use_mmap=!Sys.iswindows()
    # parsing options
    missingstrings=String[]
    missingstring=""
    delim=nothing
    ignorerepeated=false
    quotechar='"'
    openquotechar=nothing
    closequotechar=nothing
    escapechar='"'
    dateformat=nothing
    decimal=nothing
    truestrings=nothing
    falsestrings=nothing
    # type options
    types=nothing
    typemap=nothing
    allowmissing=:auto
    categorical=false
    pool=false
    strict=false
    silencewarnings=false
    debug=false

    isa(source, AbstractString) && (isfile(source) || throw(ArgumentError("\"$source\" is not a valid file")))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    buf = getbuf(source, use_mmap)
    
    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    rowsguess, newlinetype, d = guessnrows(buf, oq, cq, eq, source, delim, comment)

    header = (isa(header, Integer) && header == 1 && (datarow == 1 || skipto == 1)) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = skipto !== nothing ? skipto : (datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow) # by default, data starts on line after header
    
    if comment === nothing && d isa Char && !ignorerepeated
        # fast, most common path
        t = time()
        tapetape, idx, rows = detectstructure(buf, d % UInt8, oq, cq, eq, Val(quotetype(oq, cq, eq)), Val(newlinetype), Val(limit))
        println(time() - t)
    else

    end

    # handle potential BOM bytes
    if length(buf) > 2 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf
        tapetape[1] = (UInt64(4) << 16) | getrawlen(tapetape[1])
    end

    # detect column names / dataidx
    dataidx, columnnames = getcolumnnames(buf, tapetape, header, datarow)
    names = makeunique(columnnames)
    if normalizenames
        names = normalizename.(names)
    end

    T = type === nothing ? EMPTY : (typecode(type) | USER)
    if types isa Vector
        typecodes = TypeCode[typecode(T) | USER for T in types]
    elseif types isa AbstractDict
        typecodes = initialtypes(T, types, names)
    else
        typecodes = TypeCode[T for _ = 1:length(names)]
    end

    t = time()
    ncols = length(names)
    i = dataidx
    for row = 1:(rows - 1)
        for col = 1:ncols
            @inbounds T = typecodes[col]
            TT = typebits(T)
            if TT === INT
                parseint!(buf, tapetape, i, T, NamedTuple(), strict, silencewarnings)
            end
            i += 2
        end
    end
    println(time() - t)

    tape = Tape(buf, pointer(buf), tapetape, idx, rows, length(names))
    return File(names, tape, dataidx, rows - 1, typecodes)
end

initialtypes(T, x::AbstractDict{String}, names) = TypeCode[haskey(x, string(nm)) ? typecode(x[string(nm)]) : T for nm in names]
initialtypes(T, x::AbstractDict{Symbol}, names) = TypeCode[haskey(x, nm) ? typecode(x[nm]) : T for nm in names]
initialtypes(T, x::AbstractDict{Int}, names)    = TypeCode[haskey(x, i) ? typecode(x[i]) : T for i = 1:length(names)]

# include("tables.jl")
function parseint!(buf, tape, i, T, kwargs, strict, silencewarnings)
    @inbounds offlen = tape[i]
    off, len = getoff(offlen), getlen(offlen)
    x, ok = parseint(buf, off, len)
    if ok
        if x !== missing
            @inbounds tape[i + 1] = Core.bitcast(UInt64, x)
            return T
        else
            @inbounds tape[i] = off << 16
            return T | MISSING
        end
    else
        if user(T)
            if !strict
                @inbounds tape[i] = off << 16
                silencewarnings || println("warning int")
                return T | MISSING
            else
                error("bad int")
            end
        else
            y, ok = parsefloat(buf, off, len, kwargs)
            if ok
                @inbounds tape[i + 1] = Core.bitcast(UInt64, y)
                return FLOAT | (missingtype(T) ? MISSING : EMPTY)
            else
                return STRING | (missingtype(T) ? MISSING : EMPTY)
            end
        end
    end
end

@inline function parseint(buf, pos, len)
    len == 0 && return 0, true
    @inbounds b = buf[pos]
    neg = b === UInt8('-')
    pos += neg || (b === UInt8('+'))
    len -= neg
    x = 0
    @inbounds b = buf[pos] - UInt8('0')
    for _ = 1:len
        x = 10 * x + b
        pos += 1
        @inbounds b = buf[pos] - UInt8('0')
    end
    return neg ? -x : x, true
end

function parsefloat(buf, pos, len, kwargs)

end

Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = nothing

function Tables.columns(f::File)
    t = time()
    ncol = length(f.names)
    resultcolumns = [Vector{Int64}(undef, f.rows) for _ = 1:ncol]
    tape = f.tape.tape
    chunk = div(256, ncol)
    rowidx = f.dataidx + 1
    for row = 1:chunk:f.rows
        for j = 1:ncol
            @inbounds col = resultcolumns[j]
            off = (j - 1) * 2
            for rowoff = 0:chunk-1
                @inbounds col[row + rowoff] = Core.bitcast(Int64, tape[rowidx + off + (rowoff * ncol * 2)])
            end
        end
        rowidx += ncol * 2 * chunk
    end
    println(time() - t)
    return resultcolumns
end

#TODO
 #support all types parsing
 #handle custom missing value inputs
 #figure out escaped strings
 #what to do about the avx instructions; how to support amd/arm
 

end # module