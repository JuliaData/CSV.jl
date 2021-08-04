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

isinttype(T) = T === Int8 || T === Int16 || T === Int32 || T === Int64 || T === Int128

@inline function promote_types(@nospecialize(T), @nospecialize(S))
    T === S && return T
    T === NeedsTypeDetection && return S
    S === NeedsTypeDetection && return T
    T === Missing && return S
    S === Missing && return T
    isinttype(T) && isinttype(S) && return promote_type(T, S)
    isinttype(T) && S === Float64 && return S
    T === Float64 && isinttype(S) && return T
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
       isinttype(T) ||
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
promotevectype(::Type{T}) where {T <: Union{Bool, SmallIntegers}} = vectype(T)
promotevectype(::Type{T}) where {T} = SentinelVector{T}

struct Pooled end

# allocate columns for a full file
function allocate!(columns, rowsguess)
    for i = 1:length(columns)
        @inbounds col = columns[i]
        # if the type hasn't been detected yet, then column will get allocated
        # in the detect method while parsing
        if col.type !== NeedsTypeDetection
            if pooled(col) || maybepooled(col) || (isnan(col.pool) && col.type isa StringTypes)
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
    tfile = nothing
    if x isa AbstractVector{UInt8}
        return x, 1, length(x), tfile
    elseif x isa Base.GenericIOBuffer
        return x.data, x.ptr, x.size, tfile
    elseif x isa Cmd || x isa IO
        buf, tfile = buffer_to_tempfile(CodecZlib.TranscodingStreams.Noop(), x isa Cmd ? open(x) : x)
        return buf, 1, length(buf), tfile
    else
        try
            filename = string(x)
            buf = Mmap.mmap(filename)
            if endswith(filename, ".gz") || (length(buf) >= 2 && buf[1] == 0x1f && buf[2] == 0x8b)
                buf, tfile = buffer_to_tempfile(GzipDecompressor(), IOBuffer(buf))
            end
            return buf, 1, length(buf), tfile
        catch e
            # if we can't mmap, try just buffering the whole thing into a tempfile byte vector
            buf, tfile = buffer_to_tempfile(CodecZlib.TranscodingStreams.Noop(), x)
            return buf, 1, length(buf), tfile
        end
    end
end

function buffer_to_tempfile(codec, x)
    file = tempname()
    output = open(file, "w")
    stream = CodecZlib.TranscodingStream(codec, output)
    Base.write(stream, x)
    close(stream)
    return Mmap.mmap(file), file
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
function timetype(df::Parsers.Format)::Union{Type{Date}, Type{Time}, Type{DateTime}}
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

@inline pass(code, tlen, x, typecode) = (code, tlen, x, typecode)

function detect(str::String; opts=Parsers.OPTIONS)
    code, tlen, x, xT = detect(pass, codeunits(str), 1, sizeof(str), opts, true)
    return something(x, str)
end

const DetectTypes = Union{Missing, Int8, Int16, Int32, Int64, Float64, Date, DateTime, Time, Bool, Nothing, PosLen}

const NEEDSTYPEDETECTION = 0x01
const HARDMISSING = 0x02
const MISSING = 0x03
const INT8 = 0x04
const INT16 = 0x05
const INT32 = 0x06
const INT64 = 0x07
const FLOAT64 = 0x08
const DATE = 0x09
const DATETIME = 0x0a
const TIME = 0x0b
const BOOL = 0x0c
const STRING = 0x0d
const TYPES = Union{Nothing, Type}[NeedsTypeDetection, HardMissing, Missing, Int8, Int16, Int32, Int64, Float64, Date, DateTime, Time, Bool, nothing]
isinttypecode(T) = T === INT8 || T === INT16 || T === INT32 || T === INT64
promote_typecode(a, b) = max(a, b)
typecode(@nospecialize(T)) = T === Missing ? MISSING : T === Int8 ? INT8 : T === Int16 ? INT16 : T === Int32 ? INT32 : T === Int64 ? INT64 :
                           T === Float64 ? FLOAT64 : T === Date ? DATE : T === DateTime ? DATETIME : T === Time ? TIME : T === Bool ? BOOL :
                           T === NeedsTypeDetection ? NEEDSTYPEDETECTION : T === HardMissing ? HARDMISSING : STRING

concrete_or_concreteunion(T) = isconcretetype(T) ||
    (T isa Union && concrete_or_concreteunion(T.a) && concrete_or_concreteunion(T.b))

@inline smallestint(cb, code, tlen, x) = x < typemax(Int8) ? cb(code, tlen, unsafe_trunc(Int8, x), INT8) : x < typemax(Int16) ? cb(code, tlen, unsafe_trunc(Int16, x), INT16) : x < typemax(Int32) ? cb(code, tlen, unsafe_trunc(Int32, x), INT32) : cb(code, tlen, x, INT64)
_widen(T) = widen(T)
_widen(::Type{Int128}) = Float64
_widen(::Type{Float64}) = nothing

@noinline function _parseany(T, buf, pos, len, opts)::Parsers.Result{Any}
    return Parsers.xparse(T, buf, pos, len, opts, Any)
end

@inline function detect(cb, buf, pos, len, opts, ensure_full_buf_consumed=true, downcast=false, row=0, col=0)
    int = Parsers.xparse(Int64, buf, pos, len, opts)
    code, tlen = int.code, int.tlen
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        return cb(code, tlen, missing, NEEDSTYPEDETECTION)
    end
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
        return downcast ? smallestint(cb, code, tlen, int.val) : cb(code, tlen, int.val, INT64)
    end
    float = Parsers.xparse(Float64, buf, pos, len, opts)
    code, tlen = float.code, float.tlen
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
        return cb(code, tlen, float.val, FLOAT64)
    end
    if opts.dateformat === nothing
        date = Parsers.xparse(Date, buf, pos, len, opts, Date)
        code, tlen = date.code, date.tlen
        if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
            return cb(code, tlen, date.val, DATE)
        end
        datetime = Parsers.xparse(DateTime, buf, pos, len, opts)
        code, tlen = datetime.code, datetime.tlen
        if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
            return cb(code, tlen, datetime.val, DATETIME)
        end
        time = Parsers.xparse(Time, buf, pos, len, opts)
        code, tlen = time.code, time.tlen
        if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
            return cb(code, tlen, time.val, TIME)
        end
    else
        # use user-provided dateformat
        DT = timetype(opts.dateformat)
        if DT === Date
            dt = Parsers.xparse(DT, buf, pos, len, opts)
            code, tlen = dt.code, dt.tlen
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
                return cb(code, tlen, dt.val, DATE)
            end
        elseif DT === Time
            dt2 = Parsers.xparse(DT, buf, pos, len, opts)
            code, tlen = dt2.code, dt2.tlen
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
                return cb(code, tlen, dt2.val, TIME)
            end
        elseif DT === DateTime
            dt3 = Parsers.xparse(DT, buf, pos, len, opts)
            code, tlen = dt3.code, dt3.tlen
            if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
                return cb(code, tlen, dt3.val, DATETIME)
            end
        end
    end
    bool = Parsers.xparse(Bool, buf, pos, len, opts)
    code, tlen = bool.code, bool.tlen
    if Parsers.ok(code) && (ensure_full_buf_consumed == ((pos + tlen - 1) == len))
        return cb(code, tlen, bool.val, BOOL)
    end
    return cb(code, tlen, nothing, STRING)
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