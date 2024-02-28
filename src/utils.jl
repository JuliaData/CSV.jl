# custom types to represent some special parsing states a column might be in

# not just that a column type is Missing, but that we'll end up dropping the column
# eventually (i.e. implies col.willdrop) or the user specifically set the column
# type to Missing (i.e. col.userprovided && col.type === Missing)
# a column type may be Missing and we still want to try and detect a proper column type
# but in HardMissing case, we don't; we know the column will *always* be Missing
struct HardMissing end

function willdrop!(columns, i)
    @inbounds col = columns[i]
    col.willdrop = true
    col.type = HardMissing
    return
end

struct NeedsTypeDetection end

nonmissingtypeunlessmissingtype(::Type{T}) where {T} = ifelse(T === Missing, Missing, nonmissingtype(T))
finaltype(T) = T
finaltype(::Type{HardMissing}) = Missing
finaltype(::Type{NeedsTypeDetection}) = Missing
coltype(col) = ifelse(col.anymissing, Union{finaltype(col.type), Missing}, finaltype(col.type))

maybepooled(col) = col.pool isa Tuple ? (col.pool[1] > 0.0) : (col.pool > 0.0)

function getpool(x)::Union{Float64, Tuple{Float64, Int}}
    if x isa Bool
        return x ? 1.0 : 0.0
    elseif x isa Tuple
        y = Float64(x[1])
        (isnan(y) || 0.0 <= y <= 1.0) || throw(ArgumentError("pool tuple 1st argument must be in the range: 0.0 <= x <= 1.0"))
        try
            z = Int(x[2])
            @assert z > 0
            return (y, z)
        catch
            throw(ArgumentError("pool tuple 2nd argument must be a positive integer > 0"))
        end
    else
        y = Float64(x)
        (isnan(y) || 0.0 <= y <= 1.0) || throw(ArgumentError("pool argument must be in the range: 0.0 <= x <= 1.0"))
        return y
    end
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
@inline function nonstandardtype(T)
    T = nonmissingtype(T)
    if T === Union{} ||
       T === NeedsTypeDetection ||
       T isa StringTypes ||
       isinttype(T) ||
       T === Float16 ||
       T === Float32 ||
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

promotevectype(::Type{T}) where {T <: Union{Bool, SmallIntegers}} = vectype(T)
promotevectype(::Type{T}) where {T} = SentinelVector{T}

# allocate columns for a full file
function allocate!(columns, rowsguess)
    for i = 1:length(columns)
        @inbounds col = columns[i]
        # if the type hasn't been detected yet, then column will get allocated
        # in the detect method while parsing
        if col.type !== NeedsTypeDetection
            col.column = allocate(col.type, rowsguess)
        end
    end
    return
end

setmissing!(col, i) = col[i] = missing
const POSLEN_MISSING = PosLen(0, 0, true)
setmissing!(col::Vector{PosLen}, i) = col[i] = POSLEN_MISSING

@inline function allocate(T, len)
    if T === NeedsTypeDetection || T === HardMissing || T === Missing
        # MissingVector is an efficient representation in SentinelArrays.jl package
        return MissingVector(len)
    elseif T === PosLenString
        A = Vector{PosLen}(undef, len)
        memset!(pointer(A), typemax(UInt8), sizeof(A))
        return A
    elseif T === String
        return SentinelVector{String}(undef, len)
    elseif T === Bool
        return Vector{Union{Missing, Bool}}(undef, len)
    elseif T <: SmallIntegers
        return Vector{Union{Missing, T}}(undef, len)
    else
        return SentinelVector{T}(undef, len)
    end
end

function reallocate!(@nospecialize(A), len)
    if A isa Vector{PosLen}
        oldlen = length(A)
        resize!(A, len)
        # when reallocating, we just need to make sure the missing bit is set for lazy string PosLen
        memset!(pointer(A, oldlen + 1), typemax(UInt8), (len - oldlen) * 8)
    else
        resize!(A, len)
    end
    return
end

firstarray(x::ChainedVector) = x.arrays[1]

columntype(::Type{T}) where {T <: Union{Bool, SmallIntegers}} = Vector{Union{T, Missing}}
columntype(::Type{T}) where {T} = isbitstype(T) ? SVec{T} : SVec2{T}
columntype(::Type{PosLenString}) = Vector{PosLen}

vectype(::Type{T}) where {T <: Union{Bool, SmallIntegers}} = Vector{Union{T, Missing}}
vectype(::Type{T}) where {T} = isbitstype(T) ? SVec{T} : SVec2{T}
vectype(::Type{PosLenString}) = PosLenStringVector{Union{PosLenString, Missing}}
pooledvectype(::Type{T}) where {T} = PooledVector{Union{T, Missing}, UInt32, Vector{UInt32}}
pooledtype(::Type{T}) where {T} = PooledVector{T, UInt32, Vector{UInt32}}
# missingvectype(::PooledVector{T, R, AT}) where {T, R, AT} = PooledVector{Union{T, Missing}, R, AT}

_promote(::Type{A}, x::A) where {A} = x
_promote(::Type{A}, x::MissingVector) where {A <: AbstractVector{T}} where {T} = allocate(Base.nonmissingtype(T), length(x))
_promote(::Type{A}, x::Vector) where {A <: SentinelArray{T}} where {T} = convert(SentinelVector{T}, x)
_promote(::Type{Vector{T}}, x::Vector) where {T} = convert(Vector{T}, x)
_promote(::Type{A}, x::SentinelVector) where {A <: SentinelVector{T}} where {T} = convert(SentinelVector{T}, x)
_promote(::Type{PosLenStringVector{Union{PosLenString, Missing}}}, x::PosLenStringVector{PosLenString}) = PosLenStringVector{Union{PosLenString, Missing}}(x.data, x.poslens, x.e)
_promote(::Type{PosLenStringVector{Union{PosLenString, Missing}}}, x::MissingVector) = PosLenStringVector{Union{PosLenString, Missing}}(UInt8[], fill(POSLEN_MISSING, x.len), 0x00)
_promote(::Type{PooledVector{Union{T, Missing}, R, RA}}, x::PooledVector) where {T, R, RA} =
    PooledArray(PooledArrays.RefArray(x.refs), convert(Dict{Union{T, Missing}, UInt32}, x.invpool), convert(Vector{Union{T, Missing}}, x.pool))
_promote(::Type{PooledVector{Union{T, Missing}, R, RA}}, x::MissingVector) where {T, R, RA} =
    PooledArray(PooledArrays.RefArray(fill(UInt32(1), length(x))), Dict{Union{T, Missing}, UInt32}(missing => UInt32(1)), Union{T, Missing}[missing])
_promote(::Type{PooledVector{T, R, RA}}, x) where {T, R, RA} = PooledArray{T}(x)
_promote(::Type{PooledVector{T, R, RA}}, x::PooledVector{T, R, RA}) where {T, R, RA} = x  # avoid ambiguity
_promote(::Type{PooledVector{T, R, RA}}, x::PooledVector{T, R, RA}) where {T>:Missing, R, RA} = x  # avoid ambiguity

function chaincolumns!(@nospecialize(a), @nospecialize(b))
    if a isa PooledArray || b isa PooledArray
        # special-case PooledArrays apart from other container types
        # because we want the outermost array to be PooledArray instead of ChainedVector
        if eltype(a) == eltype(b)
            if a isa PooledArray
                return append!(a, b)
            else
                px = _promote(pooledtype(eltype(a)), a)
                return append!(px, b)
            end
        elseif a isa MissingVector
            P = pooledvectype(eltype(b))
        elseif b isa MissingVector
            P = pooledvectype(eltype(a))
        elseif eltype(a) >: Missing || eltype(b) >: Missing
            P = pooledvectype(promote_types(Base.nonmissingtype(eltype(a)), Base.nonmissingtype(eltype(b))))
        else
            # both arrays are non-missing, but not same eltype, just need to promote
            P = pooledtype(promote_types(eltype(a), eltype(b)))
        end
        px = _promote(P, a)
        py = _promote(P, b)
        return append!(px, py)
    end
    c = firstarray(a)
    if typeof(c) == typeof(b)
        # easiest case; vector types match, so just append
        return append!(a, b)
    elseif c isa MissingVector
        A = vectype(Base.nonmissingtype(eltype(b)))
    elseif b isa MissingVector
        A = vectype(Base.nonmissingtype(eltype(c)))
    elseif c isa Vector && b isa SentinelVector
        A = vectype(promote_types(eltype(c), Base.nonmissingtype(eltype(b))))
    elseif c isa SentinelVector && b isa Vector
        A = vectype(promote_types(Base.nonmissingtype(eltype(c)), eltype(b)))
    elseif c isa PosLenStringVector{PosLenString} && b isa PosLenStringVector{Union{PosLenString, Missing}}
        A = typeof(b)
    elseif c isa PosLenStringVector{Union{PosLenString, Missing}} && b isa PosLenStringVector{PosLenString}
        A = typeof(c)
    elseif c isa Vector{Bool} && b isa Vector{Union{Bool, Missing}}
        A = typeof(b)
    elseif c isa Vector{Union{Bool, Missing}} && b isa Vector{Bool}
        A = typeof(c)
    elseif c isa Vector{<:SmallIntegers} && b isa Vector{<:Union{SmallIntegers, Missing}}
        A = typeof(b)
    elseif c isa Vector{<:Union{SmallIntegers, Missing}} && b isa Vector{<:SmallIntegers}
        A = typeof(c)
    elseif c isa Vector && b isa Vector
        # two vectors, but we know eltype doesn't match, so try to promote
        A = Vector{promote_types(eltype(c), eltype(b))}
    elseif c isa SentinelVector && b isa SentinelVector
        A = vectype(promote_types(Base.nonmissingtype(eltype(c)), Base.nonmissingtype(eltype(b))))
    end
    x = ChainedVector([_promote(A, x) for x in a.arrays])
    y = _promote(A, b)
    return append!(x, y)
end

# one-liner suggested from ScottPJones
consumeBOM(buf, pos) = (length(buf) >= 3 && buf[pos] == 0xef && buf[pos + 1] == 0xbb && buf[pos + 2] == 0xbf) ? pos + 3 : pos

if isdefined(Base,:wrap)
    __wrap(x,pos) = Base.wrap(Array,x,pos)
else
    __wrap(x,pos) = x
end

# whatever input is given, turn it into an AbstractVector{UInt8} we can parse with
@inline function getbytebuffer(x, buffer_in_memory)
    tfile = nothing
    if x isa Vector{UInt8}
        return x, 1, length(x), tfile
    elseif x isa SubArray{UInt8, 1, Vector{UInt8}}
        return parent(x), first(x.indices[1]), last(x.indices[1]), tfile
    elseif x isa StringCodeUnits
        return unsafe_wrap(Vector{UInt8}, x.s), 1, length(x), tfile
    elseif x isa IOBuffer
        if x.data isa Vector{UInt8}
            return x.data, x.ptr, x.size, tfile
        elseif x.data isa SubArray{UInt8}
            x = x.data
            return parent(x), first(x.indices), last(x.indices), tfile
        else #support from IOBuffer containing Memory
            y = __wrap(x.data,length(x.data)) #generates a Vector{UInt8} from Memory{UInt8}
            return y, x.ptr, x.size, tfile
        end
    elseif x isa Cmd || x isa IO
        if buffer_in_memory
            buf = Base.read(x isa Cmd ? open(x) : x)
        else
            buf, tfile = buffer_to_tempfile(CodecZlib.TranscodingStreams.Noop(), x isa Cmd ? open(x) : x)
        end
        return buf, 1, length(buf), tfile
    else
        try
            buf = Mmap.mmap(string(x))
            return buf, 1, length(buf), tfile
        catch e
            # if we can't mmap, try just buffering the whole thing into a tempfile byte vector
            if buffer_in_memory
                buf = Base.read(x)
            else
                buf, tfile = buffer_to_tempfile(CodecZlib.TranscodingStreams.Noop(), open(x))
            end
            return buf, 1, length(buf), tfile
        end
    end
end

function getsource(@nospecialize(x), buffer_in_memory)
    buf, pos, len, tfile = getbytebuffer(x, buffer_in_memory)::Tuple{Vector{UInt8},Int,Int,Union{Nothing,String}}
    if length(buf) >= 2 && buf[1] == 0x1f && buf[2] == 0x8b
        # gzipped source, gunzip it
        if buffer_in_memory
            buf = transcode(GzipDecompressor, buf)
        else
            # 917; if we already buffered input to tempfile, make sure the compressed tempfile is
            # cleaned up since we're only passing the *uncompressed* tempfile up for removal post-parsing
            tfile1 = tfile === nothing ? nothing : tfile
            buf, tfile = buffer_to_tempfile(GzipDecompressor(), IOBuffer(buf))
            if tfile1 !== nothing
                rm(tfile1; force=true)
            end
        end
        pos = 1
        len = length(buf)
    end
    return buf, pos, len, tfile
end

@inline function buffer_to_tempfile(codec, x)
    file, output = mktemp()
    stream = CodecZlib.TranscodingStream(codec, output)
    Base.write(stream, x)
    close(stream)
    return Mmap.mmap(file), file
end

@inline function getname(x)
    if x isa AbstractVector{UInt8}
        return "<raw byte buffer: $(hash(x))>"
    elseif x isa IO
        return string("<", typeof(x), ": $(hash(x))>")
    else
        return string(x)
    end
end

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
    nextsuffix = Dict{eltype(names), UInt}()
    for nm in names
        if haskey(nextsuffix, nm)
            k = nextsuffix[nm]
            newnm = Symbol("$(nm)_$k")
            while newnm in set || haskey(nextsuffix, newnm)
                k += 1
                newnm = Symbol("$(nm)_$k")
            end
            nextsuffix[nm] = k + 1
            nm = newnm
        end
        push!(nms, nm)
        nextsuffix[nm] = 1
    end
    @assert length(names) == length(nms)
    return nms
end

getordefault(x::AbstractDict{String}, nm, i, def) = haskey(x, string(nm)) ? x[string(nm)] : def
getordefault(x::AbstractDict{Symbol}, nm, i, def) = haskey(x, nm) ? x[nm] : def
getordefault(x::AbstractDict{Int}, nm, i, def) = haskey(x, i) ? x[i] : def
function getordefault(x::AbstractDict{Regex}, nm, i, def)
    for (re, T) in x
        contains(string(nm), re) && return T
    end
    return def
end
function getordefault(x::AbstractDict, nm, i, def)
    return if haskey(x, i)
        x[i]
    elseif haskey(x, nm)
        x[nm]
    elseif haskey(x, string(nm))
        x[string(nm)]
    else
        val = _firstmatch(x, string(nm))
        val !== nothing ? val : def
    end
end

# return the first value in `x` with a `key::Regex` that matches on `nm`
function _firstmatch(x::AbstractDict, nm::AbstractString)
    for (k, T) in x
        k isa Regex && contains(nm, k) && return T
    end
    return nothing
end

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
The order of types attempted is: `Int`, `Float64`, `Date`, `DateTime`, `Bool`, and if all fail, the input String is returned.
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
    int = Parsers.xparse(Int, buf, pos, len, opts)
    code, tlen = int.code, int.tlen
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        return cb(code, tlen, missing, NEEDSTYPEDETECTION)
    end
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
        return downcast ? smallestint(cb, code, tlen, int.val) : cb(code, tlen, int.val, Int === Int64 ? INT64 : INT32)
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
    if Parsers.ok(code) && (!ensure_full_buf_consumed || (ensure_full_buf_consumed == ((pos + tlen - 1) == len)))
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

struct Arg
    x::Any
end
Base.getindex(x::Arg) = x.x

macro refargs(ex)
    ex isa Expr || throw(ArgumentError("must pass an expression to @refargs"))
    (ex.head == :call || ex.head == :function) || throw(ArgumentError("@refargs ex must be function call or definition"))
    if ex.head == :call
        for i = 2:length(ex.args)
            ex.args[i] = Expr(:call, :(CSV.Arg), ex.args[i])
        end
        return esc(ex)
    else # ex.head == :function
        refs = Expr(:block)
        fargs = ex.args[1].args
        for i = 2:length(fargs)
            arg = fargs[i]
            (arg isa Symbol || arg.head == :(::)) || throw(ArgumentError("unsupported argument expression: `$arg`"))
            nm = arg isa Symbol ? arg : arg.args[1]
            T = arg isa Symbol ? :Any : arg.args[2]
            push!(refs.args, Expr(:(=), Expr(:(::), nm, T), Expr(:ref, nm)))
            fargs[i] = Expr(:(::), nm, :(CSV.Arg))
        end
        pushfirst!(ex.args[2].args, refs)
        return esc(ex)
    end
end
