export PooledString
struct PooledString <: AbstractString end

# PointerString is an internal-only type for efficiently tracking string data + length
# all strings indexed from a column/row will always be a full String
# specifically, it allows avoiding materializing full Strings for pooled string columns while parsing
# and allows a fastpath for materializing a full String when no escaping is needed
struct PointerString <: AbstractString
    ptr::Ptr{UInt8}
    len::Int
end

function Base.hash(s::PointerString, h::UInt)
    h += Base.memhash_seed
    ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), s.ptr, s.len, h % UInt32) + h
end

import Base: ==
function ==(x::String, y::PointerString)
    sizeof(x) == y.len && ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), pointer(x), y.ptr, y.len) == 0
end
function ==(x::PointerString, y::PointerString)
    x.len == y.len && ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), x.ptr, y.ptr, y.len) == 0
end
==(y::PointerString, x::String) = x == y

Base.ncodeunits(s::PointerString) = s.len
@inline function Base.codeunit(s::PointerString, i::Integer)
    @boundscheck checkbounds(s, i)
    GC.@preserve s unsafe_load(s.ptr + i - 1)
end

Base.String(x::PointerString) = _unsafe_string(x.ptr, x.len)

# column bit flags

# whether the user provided the type or not
const USER       = 0b00000001
user(flag) = flag & USER > 0

# whether any missing values have been found in this column so far
const ANYMISSING = 0b00000010
anymissing(flag) = flag & ANYMISSING > 0

# whether a column type has been detected yet
const TYPEDETECTED = 0b00000100
typedetected(flag) = flag & TYPEDETECTED > 0

# whether a column will be "dropped"
const WILLDROP = 0b00001000
willdrop(flag) = flag & WILLDROP > 0

const LAZYSTRINGS = 0b00010000
lazystrings(flag) = flag & LAZYSTRINGS > 0

flag(T, lazystrings) = (T === Union{} ? 0x00 : ((USER | TYPEDETECTED) | (hasmissingtype(T) ? ANYMISSING : 0x00))) | (lazystrings ? LAZYSTRINGS : 0x00)

const PROMOTE_TO_STRING = 0b0100000000000000 % Int16
promote_to_string(code) = code & PROMOTE_TO_STRING > 0

hasmissingtype(T) = T === Missing || T !== Core.Compiler.typesubtract(T, Missing)

@inline function promote_types(@nospecialize(T), @nospecialize(S))
    if T === Union{} || S === Union{} || T === Missing || S === Missing || T === S || nonmissingtype(T) === nonmissingtype(S)
        return Union{T, S}
    elseif T === Int64
        return S === Float64 ? S : S === Union{Float64, Missing} ? S : hasmissingtype(S) ? Union{String, Missing} : String
    elseif T === Union{Int64, Missing}
        return S === Float64 || S === Union{Float64, Missing} ? Union{Float64, Missing} : Union{String, Missing}
    elseif T === Float64
        return S === Int64 ? T : S === Union{Int64, Missing} ? Union{Float64, Missing} : hasmissingtype(S) ? Union{String, Missing} : String
    elseif T === Union{Float64, Missing}
        return S === Int64 || S === Union{Int64, Missing} ? Union{Float64, Missing} : Union{String, Missing}
    elseif hasmissingtype(T) || hasmissingtype(S)
        return Union{String, Missing}
    else
        return String
    end
end

# bit patterns for missing value, int value, escaped string, position and len in tape parsing
const PosLen = UInt64

# primitive type PosLen 64 end
# PosLen(x::UInt64) = Core.bitcast(PosLen, x)
# UInt64(x::PosLen) = Core.bitcast(UInt64, x)

const MISSING_BIT = 0x8000000000000000
missingvalue(x) = (UInt64(x) & MISSING_BIT) == MISSING_BIT

const ESCAPE_BIT = 0x4000000000000000
escapedvalue(x) = (UInt64(x) & ESCAPE_BIT) == ESCAPE_BIT

getpos(x) = (UInt64(x) & 0x3ffffffffff00000) >> 20
getlen(x) = UInt64(x) & 0x00000000000fffff

_unsafe_string(p, len) = ccall(:jl_pchar_to_string, Ref{String}, (Ptr{UInt8}, Int), p, len)

@inline function str(buf, e, poslen)
    missingvalue(poslen) && return missing
    escapedvalue(poslen) && return unescape(PointerString(pointer(buf, getpos(poslen)), getlen(poslen)), e)
    pos, len = getpos(poslen), getlen(poslen)
    return _unsafe_string(pointer(buf, getpos(poslen)), getlen(poslen))
end

@inline function strnomiss(buf, e, poslen)
    escapedvalue(poslen) && return unescape(PointerString(pointer(buf, getpos(poslen)), getlen(poslen)), e)
    pos, len = getpos(poslen), getlen(poslen)
    return _unsafe_string(pointer(buf, getpos(poslen)), getlen(poslen))
end

struct LazyStringVector{T, A <: AbstractVector{PosLen}} <: AbstractVector{T}
    buffer::Vector{UInt8}
    e::UInt8
    poslens::A
end

LazyStringVector{T}(buffer, e, poslens::A) where {T, A} = LazyStringVector{T, A}(buffer, e, poslens)

Base.IndexStyle(::Type{LazyStringVector}) = Base.IndexLinear()
Base.size(x::LazyStringVector) = (length(x.poslens),)

Base.@propagate_inbounds function Base.getindex(x::LazyStringVector{Union{String, Missing}}, i::Int)
    @boundscheck checkbounds(x, i)
    @inbounds s = str(x.buffer, x.e, x.poslens[i])
    return s
end

Base.@propagate_inbounds function Base.getindex(x::LazyStringVector{String}, i::Int)
    @boundscheck checkbounds(x, i)
    @inbounds s = strnomiss(x.buffer, x.e, x.poslens[i])
    return s
end

# optimize iterate for ChainedVector
@inline function Base.iterate(x::LazyStringVector{T}) where {T}
    st = iterate(x.poslens)
    st === nothing && return nothing
    @inbounds s = T === String ? strnomiss(x.buffer, x.e, st[1]) : str(x.buffer, x.e, st[1])
    return s, st[2]
end

@inline function Base.iterate(x::LazyStringVector{T}, state) where {T}
    st = iterate(x.poslens, state)
    st === nothing && return nothing
    @inbounds s = T === String ? strnomiss(x.buffer, x.e, st[1]) : str(x.buffer, x.e, st[1])
    return s, st[2]
end

function allocate(rowsguess, ncols, types, flags)
    return AbstractVector[allocate(lazystrings(flags[i]) && types[i] >: String ? PosLen : types[i], rowsguess) for i = 1:ncols]
end

allocate(::Type{Union{}}, len) = MissingVector(len)
allocate(::Type{Missing}, len) = MissingVector(len)
function allocate(::Type{PosLen}, len)
    A = Vector{PosLen}(undef, len)
    memset!(pointer(A), typemax(UInt8), sizeof(A))
    return A
end
allocate(::Type{String}, len) = SentinelVector{String}(undef, len)
allocate(::Type{Union{String, Missing}}, len) = SentinelVector{String}(undef, len)
allocate(::Type{PooledString}, len) = fill(UInt32(0), len)
allocate(::Type{Union{PooledString, Missing}}, len) = fill(UInt32(0), len)
allocate(::Type{CategoricalValue{String, UInt32}}, len) = fill(UInt32(0), len)
allocate(::Type{Union{CategoricalValue{String, UInt32}, Missing}}, len) = fill(UInt32(0), len)
allocate(::Type{Bool}, len) = Vector{Union{Missing, Bool}}(undef, len)
allocate(::Type{Union{Missing, Bool}}, len) = Vector{Union{Missing, Bool}}(undef, len)
allocate(T, len) = SentinelVector{nonmissingtype(T)}(undef, len)

reallocate!(A, len) = resize!(A, len)
function reallocate!(A::Vector{PosLen}, len)
    oldlen = length(A)
    resize!(A, len)
    memset!(pointer(A, oldlen + 1), typemax(UInt8), (len - oldlen) * 8)
    return
end

const SVec{T} = SentinelVector{T, T, Missing, Vector{T}}
const StringVec = SentinelVector{String, typeof(undef), Missing, Vector{String}}

# one-liner suggested from ScottPJones
consumeBOM(buf) = (length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf) ? 4 : 1

function slurp(source)
    N = 2^22 # 4 mb increments
    ms = Tuple{Vector{UInt8}, Int}[]
    tot = 0
    while true
        m = Mmap.mmap(Vector{UInt8}, N)
        n = readbytes!(source, m, N)
        tot += n
        push!(ms, (m, n))
        n < N && break
    end
    final = Mmap.mmap(Vector{UInt8}, tot)
    pos = 1
    for (m, n) in ms
        copyto!(final, pos, m, 1, n)
        pos += n
    end
    return final
end

getsource(source::Vector{UInt8}, ::Any) = source
getsource(source::Cmd, ::Any) = Base.read(source)
getsource(source::AbstractPath, ::Any) = Base.read(open(source))
getsource(source::IO, ::Any) = slurp(source)
getsource(source::SystemPath, use_mmap) = getsource(string(source), use_mmap)
function getsource(source, use_mmap)
    m = Mmap.mmap(source)
    if use_mmap
        return m
    end
    m2 = Mmap.mmap(Vector{UInt8}, length(m))
    copyto!(m2, 1, m, 1, length(m))
    finalize(m)
    return m2
end

getname(buf::Vector{UInt8}) = "<raw buffer>"
getname(cmd::Cmd) = string(cmd)
getname(str) = string(str)
getname(io::I) where {I <: IO} = string("<", I, ">")

# normalizing column name utilities
const RESERVED = Set(["local", "global", "export", "let",
    "for", "struct", "while", "const", "continue", "import",
    "function", "if", "else", "try", "begin", "break", "catch",
    "return", "using", "baremodule", "macro", "finally",
    "module", "elseif", "end", "quote", "do"])

normalizename(name::Symbol) = name
function normalizename(name::String)::Symbol
    uname = strip(Unicode.normalize(name))
    id = Base.isidentifier(uname) ? uname : map(c->Base.is_id_char(c) ? c : '_', uname)
    cleansed = string((isempty(id) || !Base.is_id_start_char(id[1]) || id in RESERVED) ? "_" : "", id)
    return Symbol(replace(cleansed, r"(_)\1+"=>"_"))
end

function makeunique(names)
    set = Set(names)
    length(set) == length(names) && return Symbol[Symbol(x) for x in names]
    nms = Symbol[]
    for nm in names
        if nm in nms
            k = 1
            newnm = Symbol("$(nm)_$k")
            while newnm in set || newnm in nms
                k += 1
                newnm = Symbol("$(nm)_$k")
            end
            nm = newnm
        end
        push!(nms, nm)
    end
    return nms
end

standardize(::Type{T}) where {T <: Integer} = Int64
standardize(::Type{T}) where {T <: Real} = Float64
standardize(::Type{T}) where {T <: Dates.TimeType} = T
standardize(::Type{Bool}) = Bool
standardize(::Type{PooledString}) = PooledString
standardize(::Type{<:CategoricalValue}) = CategoricalValue{String, UInt32}
standardize(::Type{<:AbstractString}) = String
standardize(::Type{Union{T, Missing}}) where {T} = Union{Missing, standardize(T)}
standardize(::Type{Missing}) = Missing
standardize(T) = Union{}

initialtypes(T, x::AbstractDict{String}, names) = Type[haskey(x, string(nm)) ? standardize(x[string(nm)]) : T for nm in names]
initialtypes(T, x::AbstractDict{Symbol}, names) = Type[haskey(x, nm) ? standardize(x[nm]) : T for nm in names]
initialtypes(T, x::AbstractDict{Int}, names)    = Type[haskey(x, i) ? standardize(x[i]) : T for i = 1:length(names)]

initialflags(T, x::AbstractDict{String}, names, lazystrings) = UInt8[haskey(x, string(nm)) ? flag(x[string(nm)], lazystrings) : T for nm in names]
initialflags(T, x::AbstractDict{Symbol}, names, lazystrings) = UInt8[haskey(x, nm) ? flag(x[nm], lazystrings) : T for nm in names]
initialflags(T, x::AbstractDict{Int}, names, lazystrings)    = UInt8[haskey(x, i) ? flag(x[i], lazystrings) : T for i = 1:length(names)]

# given a DateFormat, is it meant for parsing Date, DateTime, or Time?
function timetype(df::Dates.DateFormat)
    date = false
    time = false
    for token in df.tokens
        T = typeof(token)
        if T in (Dates.DatePart{'H'}, Dates.DatePart{'I'}, Dates.DatePart{'M'}, Dates.DatePart{'S'}, Dates.DatePart{'s'})
            time = true
        elseif T in (Dates.DatePart{'y'}, Dates.DatePart{'Y'}, Dates.DatePart{'m'}, Dates.DatePart{'d'}, Dates.DatePart{'u'}, Dates.DatePart{'U'})
            date = true
        end
    end
    return ifelse(date & time, DateTime, ifelse(time, Time, Date))
end

# if a cell value of a csv file has escape characters, we need to unescape it
function unescape(s, e)
    n = ncodeunits(s)
    buf = Base.StringVector(n)
    len = 1
    i = 1
    @inbounds begin
        while i <= n
            b = codeunit(s, i)
            if b == e
                i += 1
                b = codeunit(s, i)
            end
            @inbounds buf[len] = b
            len += 1
            i += 1
        end
    end
    resize!(buf, len - 1)
    return String(buf)
end

"""
    CSV.detect(str::String)

Use the same logic used by `CSV.File` to detect column types, to parse a value from a plain string.
This can be useful in conjunction with the `CSV.Rows` type, which returns each cell of a file as a String.
The order of types attempted is: `Int64`, `Float64`, `Date`, `DateTime`, `Bool`, and if all fail, the input String is returned.
No errors are thrown.
For advanced usage, you can pass your own `Parsers.Options` type as a keyword argument `option=ops` for sentinel value detection.
"""
function detect end

detect(str::String; options=Parsers.OPTIONS) = something(detect(codeunits(str), 1, sizeof(str), options), str)

function detect(buf, pos, len, options)
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.sentinel(code) && code > 0
        return missing
    end
    if Parsers.ok(code) && vpos + vlen - 1 == len
        return int
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if Parsers.ok(code) && vpos + vlen - 1 == len
        return float
    end
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if Parsers.ok(code) && vpos + vlen - 1 == len
                return date
            end
        catch e
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, options)
            if Parsers.ok(code) && vpos + vlen - 1 == len
                return datetime
            end
        catch e
        end
    else
        try
            # use user-provided dateformat
            DT = timetype(options.dateformat)
            dt, code, vpos, vlen, tlen = Parsers.xparse(DT, buf, pos, len, options)
            if Parsers.ok(code) && vpos + vlen - 1 == len
                return dt
            end
        catch e
        end
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, options)
    if Parsers.ok(code) && vpos + vlen - 1 == len
        return bool
    end
    return nothing
end

struct ReversedBuf <: AbstractVector{UInt8}
    buf::Vector{UInt8}
end

Base.size(a::ReversedBuf) = size(a.buf)
Base.IndexStyle(::Type{ReversedBuf}) = Base.IndexLinear()
Base.getindex(a::ReversedBuf, i::Int) = a.buf[end + 1 - i]

function unset!(A::Vector, i::Int, row, x)
    ccall(:jl_arrayunset, Cvoid, (Array, Csize_t), A, i - 1)
    # println("deleting col = $i on thread = $(Threads.threadid()), row = $row, id = $x")
    return
end

memcpy!(d, doff, s, soff, n) = ccall(:memcpy, Cvoid, (Ptr{UInt8}, Ptr{UInt8}, Int), d + doff - 1, s + soff - 1, n)
memset!(ptr, value, num) = ccall(:memset, Ptr{Cvoid}, (Ptr{Cvoid}, Cint, Csize_t), ptr, value, num)

mutable struct RefPool
    refs::Dict{Union{String, Missing}, UInt32}
    lastref::UInt32
end

RefPool() = RefPool(Dict{Union{String, Missing}, UInt32}(), 0)
