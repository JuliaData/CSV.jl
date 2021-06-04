# custom types to represent some special parsing states a column might be in

# not just that a column type is Missing, but that we'll end up dropping the column
# eventually (i.e. implies col.willdrop) or the user specifically set the column
# type to Missing (i.e. col.userprovided && col.type === Missing)
# a column type may be Missing and we still want to try and detect a proper column type
# but in HardMissing case, we don't; we know the column will *always* be Missing
struct HardMissing end

willdrop!(columns, i) = setfield!(@inbounds(columns[i]), :willdrop, true)

struct NeedsTypeDetection end

nonmissingtypeunlessmissingtype(::Type{T}) where {T} = ifelse(T === Missing, Missing, nonmissingtype(T))
pooltype(col) = nonmissingtype(keytype(col.refpool.refs))
finaltype(T) = T
finaltype(::Type{HardMissing}) = Missing
finaltype(::Type{NeedsTypeDetection}) = Missing
coltype(col) = ifelse(col.anymissing, Union{finaltype(col.type), Missing}, finaltype(col.type))

pooled(col) = col.pool == 1.0
maybepooled(col) = 0.0 < col.pool < 1.0

getpool(x::Bool) = x ? 1.0 : 0.0
function getpool(x::Real)
    y = Float64(x)
    (isnan(y) || 0.0 <= y <= 1.0) || throw(ArgumentError("pool argument must be in the range: 0.0 <= x <= 1.0"))
    return y
end

tupcat(::Type{Tuple{}}, S) = Tuple{S}
tupcat(::Type{Tuple{T}}, S) where {T} = Tuple{T, S}
tupcat(::Type{Tuple{T, T2}}, S) where {T, T2} = Tuple{T, T2, S}
tupcat(::Type{Tuple{T, T2, T3}}, S) where {T, T2, T3} = Tuple{T, T2, T3, S}
tupcat(::Type{Tuple{T, T2, T3, T4}}, S) where {T, T2, T3, T4} = Tuple{T, T2, T3, T4, S}
tupcat(::Type{T}, S) where {T <: Tuple} = Tuple{Any[(fieldtype(T, i) for i = 1:fieldcount(T))..., S]...}

const StringTypes = Union{Type{String}, Type{PosLenString}, Type{<:InlineString}}
pickstringtype(T, maxstringsize) = T === InlineString ? (maxstringsize < DEFAULT_MAX_INLINE_STRING_LENGTH ? InlineStringType(maxstringsize) : String) : T

# we define our own bit flag on a Parsers.ReturnCode to signal if a column needs to promote to string
const PROMOTE_TO_STRING = 0b0100000000000000 % Int16
promote_to_string(code) = code & PROMOTE_TO_STRING > 0

@inline function promote_types(@nospecialize(T), @nospecialize(S))
    T === S && return T
    T === NeedsTypeDetection && return S
    S === NeedsTypeDetection && return T
    T === Missing && return S
    S === Missing && return T
    T === Int64 && S === Float64 && return S
    T === Float64 && S === Int64 && return T
    T <: InlineString && S <: InlineString && return promote_type(T, S)
    (T === String || S === String) && return String
    T <: InlineString && return T
    S <: InlineString && return S
    return nothing
end

# when users pass non-standard types, we need to keep track of them in a Tuple{...} to generate efficient custom parsing kernel codes
function nonstandardtype(T)
    T = nonmissingtype(T)
    if T === Union{} ||
       T isa StringTypes ||
       T === Int64 ||
       T === Float64 ||
       T === Bool ||
       T === Date ||
       T === DateTime ||
       T === Time
        return Union{}
    end
    return T
end

## column array allocating
# we don't want to use SentinelVector for small integer types due to the higher risk of
# sentinel value collision, so we just use Vector{Union{T, Missing}} and convert to Vector{T} if no missings were found
const SmallIntegers = Union{Int8, UInt8, Int16, UInt16, Int32, UInt32}

const SVec{T} = SentinelVector{T, T, Missing, Vector{T}}
const SVec2{T} = SentinelVector{T, typeof(undef), Missing, Vector{T}}

vectype(::Type{T}) where {T <: Union{Bool, SmallIntegers}} = Vector{Union{T, Missing}}
vectype(::Type{T}) where {T} = isbitstype(T) ? SVec{T} : SVec2{T}

struct Pooled end

# allocate columns for a full file
function allocate!(columns, rowsguess)
    for i = 1:length(columns)
        @inbounds col = columns[i]
        # if the type hasn't been detected yet, then column will get allocated
        # in the detect method while parsing
        if col.type !== NeedsTypeDetection
            if pooled(col) || maybepooled(col)
                col.column = allocate(Pooled, rowsguess)
                if !isdefined(col, :refpool)
                    col.refpool = RefPool(col.type)
                end
            else
                col.column = allocate(col.type, rowsguess)
            end
        end
    end
    return
end

# MissingVector is an efficient representation in SentinelArrays.jl package
allocate(::Type{NeedsTypeDetection}, len) = MissingVector(len)
allocate(::Type{HardMissing}, len) = MissingVector(len)
allocate(::Type{Missing}, len) = MissingVector(len)
function allocate(::Type{PosLenString}, len)
    A = Vector{PosLen}(undef, len)
    memset!(pointer(A), typemax(UInt8), sizeof(A))
    return A
end
allocate(::Type{String}, len) = SentinelVector{String}(undef, len)
allocate(::Type{Pooled}, len) = fill(UInt32(1), len) # initialize w/ all missing values
allocate(::Type{Bool}, len) = Vector{Union{Missing, Bool}}(undef, len)
allocate(::Type{T}, len) where {T <: SmallIntegers} = Vector{Union{Missing, T}}(undef, len)
allocate(T, len) = SentinelVector{T}(undef, len)

reallocate!(A, len) = resize!(A, len)
function reallocate!(A::Vector{UInt32}, len)
    oldlen = length(A)
    resize!(A, len)
    for i = (oldlen + 1):len
        @inbounds A[i] = UInt32(1)
    end
    return
end
# when reallocating, we just need to make sure the missing bit is set for lazy string PosLen
function reallocate!(A::Vector{PosLen}, len)
    oldlen = length(A)
    resize!(A, len)
    memset!(pointer(A, oldlen + 1), typemax(UInt8), (len - oldlen) * 8)
    return
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
            buf = Base.read(x)
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
    @assert length(names) == length(nms)
    return nms
end

getordefault(x::AbstractDict{String}, nm, i, def) = haskey(x, string(nm)) ? x[string(nm)] : def
getordefault(x::AbstractDict{Symbol}, nm, i, def) = haskey(x, nm) ? x[nm] : def
getordefault(x::AbstractDict{Int}, nm, i, def) = haskey(x, i) ? x[i] : def
getordefault(x::AbstractDict, nm, i, def) = haskey(x, i) ? x[i] : haskey(x, nm) ? x[nm] : haskey(x, string(nm)) ? x[string(nm)] : def

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

function detect(str::String; opts=Parsers.OPTIONS)
    x, code, tlen = detect(codeunits(str), 1, sizeof(str), opts, true)
    return something(x, str)
end

const DetectTypes = Union{Missing, Int64, Float64, Date, DateTime, Time, Bool, Nothing}

@inline function detect(buf, pos, len, opts, ensure_full_buf_consumed=true, row=0, col=0)::Tuple{DetectTypes, Int16, Int64}
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, opts)
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        return missing, code, tlen
    end
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
        return int, code, tlen
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, opts)
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
        return float, code, tlen
    end
    if opts.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, opts)
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
                return date, code, tlen
            end
        catch e
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, opts)
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
                return datetime, code, tlen
            end
        catch e
        end
        try
            time, code, vpos, vlen, tlen = Parsers.xparse(Time, buf, pos, len, opts)
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
                return time, code, tlen
            end
        catch e
        end
    else
        try
            # use user-provided dateformat
            DT = timetype(opts.dateformat)
            dt, code, vpos, vlen, tlen = Parsers.xparse(DT, buf, pos, len, opts)
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((vpos + vlen - 1) == len)))
                return dt, code, tlen
            end
        catch e
        end
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, opts)
    if Parsers.ok(code) && (ensure_full_buf_consumed == ((vpos + vlen - 1) == len))
        return bool, code, tlen
    end
    return nothing, code, Int64(0)
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

function promoteunion(T, S)
    new = promote_type(T, S)
    return isabstracttype(new) ? Union{T, S} : new
end

# lazily treat array with abstract eltype as having eltype of Union of all concrete elements
struct ConcreteEltype{T, A} <: AbstractVector{T}
    data::A
end

concrete_or_concreteunion(T) = isconcretetype(T) ||
    (T isa Union && concrete_or_concreteunion(T.a) && concrete_or_concreteunion(T.b))

function ConcreteEltype(x::A) where {A}
    (isempty(x) || concrete_or_concreteunion(eltype(x))) && return x
    T = typeof(x[1])
    for i = 2:length(x)
        T = promoteunion(T, typeof(x[i]))
    end
    return ConcreteEltype{T, A}(x)
end

Base.IndexStyle(::Type{<:ConcreteEltype}) = Base.IndexLinear()
Base.size(x::ConcreteEltype) = (length(x.data),)
Base.eltype(::ConcreteEltype{T, A}) where {T, A} = T
Base.getindex(x::ConcreteEltype{T}, i::Int) where {T} = getindex(x.data, i)

struct PointerString
    ptr::Ptr{UInt8}
    len::Int
end

function Base.hash(s::PointerString, h::UInt)
    h += Base.memhash_seed
    ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), s.ptr, s.len, h % UInt32) + h
end

import Base: ==
function ==(x::AbstractString, y::PointerString)
    sizeof(x) == y.len && ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), pointer(x), y.ptr, y.len) == 0
end
function ==(x::PointerString, y::PointerString)
    x.len == y.len && ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), x.ptr, y.ptr, y.len) == 0
end
==(y::PointerString, x::AbstractString) = x == y

Base.ncodeunits(s::PointerString) = s.len
@inline function Base.codeunit(s::PointerString, i::Integer)
    @boundscheck checkbounds(s, i)
    GC.@preserve s unsafe_load(s.ptr + i - 1)
end

_unsafe_string(p, len) = ccall(:jl_pchar_to_string, Ref{String}, (Ptr{UInt8}, Int), p, len)
Base.String(x::PointerString) = _unsafe_string(x.ptr, x.len)
WeakRefStrings.PosLenString(x::PointerString) = PosLenString(unsafe_wrap(Array, x.ptr, x.len), PosLen(1, x.len), 0x00)