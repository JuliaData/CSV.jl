export PooledString
struct PooledString <: AbstractString end

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
==(y::PointerString, x::String) = x == y

Base.ncodeunits(s::PointerString) = s.len
@inline function Base.codeunit(s::PointerString, i::Integer)
    @boundscheck checkbounds(s, i)
    GC.@preserve s unsafe_load(s.ptr + i - 1)
end
Base.String(x::PointerString) = unsafe_string(x.ptr, x.len)

const TypeCode = Int8

# default value to signal that parsing should try to detect a type
const EMPTY       = 0b00000000 % TypeCode

# MISSING is a mask that can be combined w/ any other TypeCode for Union{T, Missing}
const MISSING     = 0b10000000 % TypeCode
missingtype(x::TypeCode) = (x & MISSING) == MISSING
usermissing(x::TypeCode) = x == (MISSING | USER)

# MISSINGTYPE is for a column like `Vector{Missing}`
# if we're trying to detect a column type and the 1st value of a column is `missing`
# we basically want to still treat it like EMPTY and try parsing other types on each row
const MISSINGTYPE = 0b00000001 % TypeCode

# enum-like type codes for basic supported types
const INT         = 0b00000010 % TypeCode
const FLOAT       = 0b00000011 % TypeCode
const DATE        = 0b00000100 % TypeCode
const DATETIME    = 0b00000101 % TypeCode
const TIME        = 0b00000110 % TypeCode
const BOOL        = 0b00000111 % TypeCode
const STRING      = 0b00001000 % TypeCode
stringtype(x::TypeCode) = (x & STRING) == STRING
const POOL        = 0b00010000 % TypeCode
pooled(x::TypeCode) = (x & POOL) == POOL

# a user-provided type; a mask that can be combined w/ basic types
const USER     = 0b00100000 % TypeCode
user(x::TypeCode) = (x & USER) == USER

const TYPEBITS = 0b00011111 % TypeCode
typebits(x::TypeCode) = x & TYPEBITS

typecode(::Type{Missing}) = MISSING
typecode(::Type{<:Integer}) = INT
typecode(::Type{<:AbstractFloat}) = FLOAT
typecode(::Type{Date}) = DATE
typecode(::Type{DateTime}) = DATETIME
typecode(::Type{Time}) = TIME
typecode(::Type{Bool}) = BOOL
typecode(::Type{<:AbstractString}) = STRING
typecode(::Type{PooledString}) = POOL
typecode(::Type{CategoricalString{UInt32}}) = POOL
typecode(::Type{Union{}}) = EMPTY
typecode(::Type{Union{T, Missing}}) where {T} = typecode(T) | MISSING
typecode(::Type{T}) where {T} = EMPTY
typecode(x::T) where {T} = typecode(T)

const TYPECODES = Dict(
    EMPTY => Missing,
    MISSINGTYPE => Missing,
    MISSING => Missing,
    INT => Int64,
    FLOAT => Float64,
    DATE => Date,
    DATETIME => DateTime,
    TIME => Time,
    BOOL => Bool,
    POOL => PooledString,
    STRING => String,
    INT | MISSING => Union{Int64, Missing},
    FLOAT | MISSING => Union{Float64, Missing},
    DATE | MISSING => Union{Date, Missing},
    DATETIME | MISSING => Union{DateTime, Missing},
    BOOL | MISSING => Union{Bool, Missing},
    POOL | MISSING => Union{PooledString, Missing},
    STRING | MISSING => Union{String, Missing}
)

@inline function promote_typecode(T, S)
    if T == EMPTY || T == S || user(T) || (T == MISSINGTYPE && S == MISSINGTYPE) ||
        (T & ~MISSING) == (S & ~MISSING)
        return T | S
    elseif T == MISSINGTYPE
        return S | MISSING
    elseif S == MISSINGTYPE
        return T | MISSING
    elseif T == INT
        return S == FLOAT ? S : S == (FLOAT | MISSING) ? S : missingtype(S) ? (STRING | MISSING) : STRING
    elseif T == FLOAT
        return S == INT ? FLOAT : S == (INT | MISSING) ? T | MISSING : missingtype(S) ? (STRING | MISSING) : STRING
    elseif missingtype(T) || missingtype(S)
        return STRING | MISSING
    else
        return STRING
    end
end

gettypecodes(x::Dict) = Dict(typecode(k)=>typecode(v) for (k, v) in x)
gettypecodes(x::Dict{TypeCode, TypeCode}) = x

# bit patterns for missing value, int value, escaped string, position and len in tape parsing
const MISSING_BIT = 0x8000000000000000
missingvalue(x::UInt64) = (x & MISSING_BIT) == MISSING_BIT
sentinelvalue(::Type{Float64}) = Core.bitcast(UInt64, NaN) | MISSING_BIT
sentinelvalue(::Type{T}) where {T} = MISSING_BIT
const INT_SENTINEL = -8899831978349840752
sentinelvalue(::Type{Int64}) = rand(typemin(Int64):div(typemin(Int64), 1000)) * ifelse(rand(Bool), 1, -1)

const ESCAPE_BIT = 0x4000000000000000
escapedvalue(x::UInt64) = (x & ESCAPE_BIT) == ESCAPE_BIT

getpos(x::UInt64) = (x & 0x3ffffffffff00000) >> 20
getlen(x::UInt64) = x & 0x00000000000fffff

# utilities to convert values to raw UInt64 and back for tape writing
int64(x::UInt64) = Core.bitcast(Int64, x)
float64(x::UInt64) = Core.bitcast(Float64, x)
bool(x::UInt64) = x == 0x0000000000000001
date(x::UInt64) = Date(Dates.UTD(int64(x)))
datetime(x::UInt64) = DateTime(Dates.UTM(int64(x)))
time(x::UInt64) = Time(Nanosecond(int64(x)))
ref(x::UInt64) = unsafe_trunc(UInt32, x)

uint64(x::Int64) = Core.bitcast(UInt64, x)
uint64(x::Float64) = Core.bitcast(UInt64, x)
uint64(x::Bool) = UInt64(x)
uint64(x::Union{Date, DateTime, Time}) = uint64(Dates.value(x))
uint64(x::UInt32) = UInt64(x)

reinterp_func(::Type{Int64}) = int64
reinterp_func(::Type{Float64}) = float64
reinterp_func(::Type{Date}) = date
reinterp_func(::Type{DateTime}) = datetime
reinterp_func(::Type{Time}) = time
reinterp_func(::Type{Bool}) = bool

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

initialtypes(T, x::AbstractDict{String}, names) = TypeCode[haskey(x, string(nm)) ? typecode(x[string(nm)]) | USER : T for nm in names]
initialtypes(T, x::AbstractDict{Symbol}, names) = TypeCode[haskey(x, nm) ? typecode(x[nm]) | USER : T for nm in names]
initialtypes(T, x::AbstractDict{Int}, names)    = TypeCode[haskey(x, i) ? typecode(x[i]) | USER : T for i = 1:length(names)]

function timetype(df::Dates.DateFormat)
    date = false
    time = false
    for token in df.tokens
        T = typeof(token)
        if T == Dates.DatePart{'H'}
            time = true
        elseif T == Dates.DatePart{'y'} || T == Dates.DatePart{'Y'}
            date = true
        end
    end
    return ifelse(date & time, DateTime, ifelse(time, Time, Date))
end

roundup(a, n) = (a + (n - 1)) & ~(n - 1)

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

# multithreading structures
const AtomicTypes = Union{Base.Threads.atomictypes...}

struct AtomicVector{T} <: AbstractArray{T, 1}
    A::Vector{T}

    function AtomicVector{T}(undef, n::Int) where {T}
        T <: AtomicTypes || throw(ArgumentError("invalid type for AtomicVector: $T"))
        return new{T}(zeros(T, n))
    end

    function AtomicVector(A::Vector{T}) where {T}
        T <: AtomicTypes || throw(ArgumentError("invalid type for AtomicVector: $T"))
        return new{T}(A)
    end
end

Base.size(a::AtomicVector) = size(a.A)
Base.IndexStyle(::Type{A}) where {A <: AtomicVector} = Base.IndexLinear()

for typ in Base.Threads.atomictypes
    lt = Base.Threads.llvmtypes[typ]
    ilt = Base.Threads.llvmtypes[Base.Threads.inttype(typ)]
    rt = "$lt, $lt*"
    irt = "$ilt, $ilt*"
    @eval Base.getindex(x::AtomicVector{$typ}, i::Int) =
        Base.llvmcall($"""
                 %ptr = inttoptr i$(Base.Threads.WORD_SIZE) %0 to $lt*
                 %rv = load atomic $rt %ptr acquire, align $(Base.Threads.gc_alignment(typ))
                 ret $lt %rv
                 """, $typ, Tuple{Ptr{$typ}}, pointer(x.A, i))
    @eval Base.setindex!(x::AtomicVector{$typ}, v::$typ, i::Int) =
        Base.llvmcall($"""
                 %ptr = inttoptr i$(Base.Threads.WORD_SIZE) %0 to $lt*
                 store atomic $lt %1, $lt* %ptr release, align $(Base.Threads.gc_alignment(typ))
                 ret void
                 """, Cvoid, Tuple{Ptr{$typ}, $typ}, pointer(x.A, i), v)
end

function unset!(A::Vector, i::Int, row, x)
    finalize(A[i])
    ccall(:jl_arrayunset, Cvoid, (Array, Csize_t), A, i - 1)
    # println("deleting col = $i on thread = $(Threads.threadid()), row = $row, id = $x")
    return
end

