export PooledString
"""
    PooledString

A singleton type that can be used for signaling that a column of a csv file should be pooled,
with the output array type being a `PooledArray`.
"""
struct PooledString <: AbstractString end

schematype(::Type{T}) where {T} = T
schematype(::Type{PooledString}) = String
schematype(::Type{Union{Missing, PooledString}}) = Union{Missing, String}

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

# column bit flags; useful so we don't have to pass a bunch of arguments/state around manually

# whether the user provided the type or not
const USER       = 0b00000001
user(flag) = flag & USER > 0

# whether any missing values have been found in this column so far
const ANYMISSING = 0b00000010
anymissing(flag) = flag & ANYMISSING > 0

# whether a column type has been detected yet
const TYPEDETECTED = 0b00000100
typedetected(flag) = flag & TYPEDETECTED > 0

# whether a column will be "dropped" from the select/drop keyword arguments
const WILLDROP = 0b00001000
willdrop(flag) = flag & WILLDROP > 0

# whether strings should be lazy; results in LazyStringVectors
# this setting isn't per column, but we store it on the column bit flags anyway for convenience
const LAZYSTRINGS = 0b00010000
lazystrings(flag) = flag & LAZYSTRINGS > 0

const MAYBEPOOLED = 0b00100000
maybepooled(flag) = flag & MAYBEPOOLED > 0
# ~95% z-score, 10% MoE
const POOLSAMPLESIZE = 100

flag(T, lazystrings) = (T === Union{} ? 0x00 : ((USER | TYPEDETECTED) | (hasmissingtype(T) ? ANYMISSING : 0x00))) | (lazystrings ? LAZYSTRINGS : 0x00)

# we define our own bit flag on a Parsers.ReturnCode to signal if a column needs to promote to string
const PROMOTE_TO_STRING = 0b0100000000000000 % Int16
promote_to_string(code) = code & PROMOTE_TO_STRING > 0

hasmissingtype(T) = T === Missing || T !== ts(T, Missing)

@inline function promote_types(@nospecialize(T), @nospecialize(S))
    if T === Union{} || S === Union{} || T === Missing || S === Missing || T === S || Base.nonmissingtype(T) === Base.nonmissingtype(S)
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

## lazy strings
# bit patterns for missing value, int value, escaped string, position and len in lazy string parsing

primitive type PosLen 64 end
PosLen(x::UInt64) = Core.bitcast(PosLen, x)
UInt64(x::PosLen) = Core.bitcast(UInt64, x)

Base.convert(::Type{PosLen}, x::UInt64) = PosLen(x)
Base.convert(::Type{UInt64}, x::PosLen) = UInt64(x)

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

## column array allocating
# we don't want to use SentinelVector for small integer types due to the higher risk of
# sentinel value collision, so we just use Vector{Union{T, Missing}} and convert to Vector{T} if no missings were found
const SmallIntegers = Union{Int8, UInt8, Int16, UInt16, Int32, UInt32}

# allocate columns for a full file
function allocate(rowsguess, ncols, types, flags, refs)
    columns = Vector{AbstractVector}(undef, ncols)
    for i = 1:ncols
        @inbounds columns[i] = allocate(lazystrings(flags[i]) && (types[i] === String || types[i] === Union{String, Missing}) ? PosLen : types[i], rowsguess)
        if types[i] === PooledString || types[i] ===  Union{PooledString, Missing}
            refs[i] = RefPool()
        end
    end
    return columns
end

# MissingVector is an efficient representation in SentinelArrays.jl package
allocate(::Type{Union{}}, len) = MissingVector(len)
allocate(::Type{Missing}, len) = MissingVector(len)
function allocate(::Type{PosLen}, len)
    A = Vector{PosLen}(undef, len)
    memset!(pointer(A), typemax(UInt8), sizeof(A))
    return A
end
allocate(::Type{String}, len) = SentinelVector{String}(undef, len)
allocate(::Type{Union{String, Missing}}, len) = SentinelVector{String}(undef, len)
allocate(::Type{PooledString}, len) = Vector{UInt32}(undef, len)
allocate(::Type{Union{PooledString, Missing}}, len) = Vector{UInt32}(undef, len)
allocate(::Type{Bool}, len) = Vector{Union{Missing, Bool}}(undef, len)
allocate(::Type{Union{Missing, Bool}}, len) = Vector{Union{Missing, Bool}}(undef, len)
allocate(::Type{T}, len) where {T <: SmallIntegers} = Vector{Union{Missing, T}}(undef, len)
allocate(::Type{Union{Missing, T}}, len) where {T <: SmallIntegers} = Vector{Union{Missing, T}}(undef, len)
allocate(T, len) = SentinelVector{Base.nonmissingtype(T)}(undef, len)

reallocate!(A, len) = resize!(A, len)
# when reallocating, we just need to make sure the missing bit is set for lazy string PosLen
function reallocate!(A::Vector{PosLen}, len)
    oldlen = length(A)
    resize!(A, len)
    memset!(pointer(A, oldlen + 1), typemax(UInt8), (len - oldlen) * 8)
    return
end

const SVec{T} = SentinelVector{T, T, Missing, Vector{T}}
const SVec2{T} = SentinelVector{T, typeof(undef), Missing, Vector{T}}

if applicable(Core.Compiler.typesubtract, Union{Int, Missing}, Missing)
ts(T, S) = Core.Compiler.typesubtract(T, S)
else
ts(T, S) = Core.Compiler.typesubtract(T, S, 16)
end

# when users pass non-standard types, we need to keep track of them in a Tuple{...} to generate efficient custom parsing kernel codes
function nonstandardtype(T)
    if T === Union{}
        return T
    end
    S = ts(ts(ts(ts(ts(ts(ts(ts(ts(T, Int64), Float64), String), PooledString), Bool), Date), DateTime), Time), Missing)
    if S === Union{}
        return S
    elseif S <: SmallIntegers
        return Tuple{Vector{Union{Missing, S}}, S}
    elseif isbitstype(S)
        return Tuple{SVec{S}, S}
    else
        return Tuple{SVec2{S}, S}
    end
end

# one-liner suggested from ScottPJones
consumeBOM(buf, pos) = (length(buf) >= 3 && buf[pos] == 0xef && buf[pos + 1] == 0xbb && buf[pos + 2] == 0xbf) ? pos + 3 : pos

# whatever input is given, turn it into an AbstractVector{UInt8} we can parse with
function getsource(x)
    if x isa AbstractVector{UInt8}
        return x, 1, length(x)
    elseif x isa Base.GenericIOBuffer
        return x.data, x.ptr, x.size
    elseif x isa Cmd || x isa IO
        Base.depwarn("`CSV.File` or `CSV.Rows` with `$(typeof(x))` object is deprecated; pass a filename, `IOBuffer`, or byte buffer directly (via `read(x)`)", :getsource)
        buf = Base.read(x)
        return buf, 1, length(buf)
    else
        try
            buf = Mmap.mmap(string(x))
            return buf, 1, length(buf)
        catch e
            # if we can't mmap, try just `read`ing the whole thing into a byte vector
            buf = read(x)
            return buf, 1, length(buf)
        end
    end
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

initialtypes(T, x::AbstractDict{String}, names) = Type[haskey(x, string(nm)) ? x[string(nm)] : T for nm in names]
initialtypes(T, x::AbstractDict{Symbol}, names) = Type[haskey(x, nm) ? x[nm] : T for nm in names]
initialtypes(T, x::AbstractDict{Int}, names)    = Type[haskey(x, i) ? x[i] : T for i = 1:length(names)]

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

# a ReversedBuf takes a byte vector and indexes backwards;
# used for the footerskip keyword argument, which starts at the bottom of the file
# and skips lines backwards
struct ReversedBuf <: AbstractVector{UInt8}
    buf::Vector{UInt8}
end

Base.size(a::ReversedBuf) = size(a.buf)
Base.IndexStyle(::Type{ReversedBuf}) = Base.IndexLinear()
Base.getindex(a::ReversedBuf, i::Int) = a.buf[end + 1 - i]
Base.pointer(a::ReversedBuf, pos::Integer=1) = pointer(a.buf, length(a.buf) + 1 - pos)

memset!(ptr, value, num) = ccall(:memset, Ptr{Cvoid}, (Ptr{Cvoid}, Cint, Csize_t), ptr, value, num)

# a RefPool holds our refs as a Dict, along with a lastref field which is incremented when a new ref is found while parsing pooled columns
mutable struct RefPool
    refs::Dict{Union{String, Missing}, UInt32}
    lastref::UInt32
end

RefPool() = RefPool(Dict{Union{String, Missing}, UInt32}(), 0)
