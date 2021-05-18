# column bit flags; useful so we don't have to pass a bunch of arguments/state around manually

# whether the user provided the type or not; implies TYPEDETECTED
const USER = 0b00000001
user(flag) = flag & USER > 0

# whether a column type has been detected yet
const TYPEDETECTED = 0b00000010
typedetected(flag) = flag & TYPEDETECTED > 0

# whether any missing values have been found in this column so far
const ANYMISSING = 0b00000100
anymissing(flag) = flag & ANYMISSING > 0

nonmissingtypeunlessmissingtype(::Type{T}) where {T} = ifelse(T === Missing, Missing, nonmissingtype(T))
coltype(col) = ifelse(anymissing(col.flag), Union{col.type, Missing}, col.type)

# not just that a column type is Missing, but that we'll end up dropping the column
# eventually (WILLDROP) or the user specifically set the column type to MISSING
# a column type may be Missing and we still want to try and detect a proper column type
# but in MISSING case, we don't; we know the column will *always* be Missing
const MISSING = 0b00001000
missingcol(flag) = flag & MISSING > 0

# whether a column will be "dropped" from the select/drop keyword arguments
const WILLDROP = 0b00010000
willdrop(flag) = flag & WILLDROP > 0
function willdrop!(columns, i)
    @inbounds col = columns[i]
    col.type = Missing
    col.flag |= WILLDROP | MISSING | TYPEDETECTED
    return
end

# when parsing and we think a column might be pooled, but need to check final cardinality
const MAYBEPOOLED = 0b00100000
maybepooled(flag) = flag & MAYBEPOOLED > 0

# once we've confirmed a column will be pooled
const POOLED = 0b01000000
pooled(flag) = flag & POOLED > 0
function pooled!(columns, i, p::Float64)
    @inbounds col = columns[i]
    col.pool = p
    if p == 1.0
        col.flag |= POOLED
    elseif !isnan(p) && p > 0.0
        col.flag |= MAYBEPOOLED
    end
    return
end
getpool(x::Bool) = x ? 1.0 : 0.0
getpool(x::Real) = Float64(x)

flag(T) = T === Union{} ? 0x00 :
    ((USER | TYPEDETECTED) |
     (T !== nonmissingtype(T) ? ANYMISSING : 0x00) |
     (T === Missing ? MISSING | ANYMISSING : 0x00)
    )

tupcat(::Type{Tuple{}}, S) = Tuple{S}
tupcat(::Type{Tuple{T}}, S) where {T} = Tuple{T, S}
tupcat(::Type{Tuple{T, T2}}, S) where {T, T2} = Tuple{T, T2, S}
tupcat(::Type{Tuple{T, T2, T3}}, S) where {T, T2, T3} = Tuple{T, T2, T3, S}
tupcat(::Type{Tuple{T, T2, T3, T4}}, S) where {T, T2, T3, T4} = Tuple{T, T2, T3, T4, S}
tupcat(::Type{T}, S) where {T <: Tuple} = Tuple{Any[(fieldtype(T, i) for i = 1:fieldcount(T))..., S]...}

const StringTypes = Union{Type{String}, Type{PosLenString}, #=Type{<:InlineString}=#}

# we define our own bit flag on a Parsers.ReturnCode to signal if a column needs to promote to string
const PROMOTE_TO_STRING = 0b0100000000000000 % Int16
promote_to_string(code) = code & PROMOTE_TO_STRING > 0

@inline function promote_types(@nospecialize(T), @nospecialize(S))
    T === S && return T
    T === Union{} && return S
    S === Union{} && return T
    T === Missing && return S
    S === Missing && return T
    T === Int64 && S === Float64 && return S
    T === Float64 && S === Int64 && return T
    return nothing
end

# when users pass non-standard types, we need to keep track of them in a Tuple{...} to generate efficient custom parsing kernel codes
function nonstandardtype(T)
    if T === Union{}
        return T
    end
    return ts(ts(ts(ts(ts(ts(ts(ts(ts(T, Int64), Float64), String), PosLenString), Bool), Date), DateTime), Time), Missing)
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
        if typedetected(col.flag)
            if pooled(col.flag) || maybepooled(col.flag)
                col.column = allocate(Pooled, rowsguess)
                col.refpool = RefPool(col.type)
            else
                col.column = allocate(col.type, rowsguess)
            end
        end
    end
    return
end

# MissingVector is an efficient representation in SentinelArrays.jl package
allocate(::Type{Union{}}, len) = MissingVector(len)
allocate(::Type{Missing}, len) = MissingVector(len)
function allocate(::Type{PosLenString}, len)
    A = Vector{PosLen}(undef, len)
    memset!(pointer(A), typemax(UInt8), sizeof(A))
    return A
end
allocate(::Type{String}, len) = SentinelVector{String}(undef, len)
allocate(::Type{Union{String, Missing}}, len) = SentinelVector{String}(undef, len)
allocate(::Type{Pooled}, len) = fill(UInt32(1), len) # initialize w/ all missing values
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

function detect(buf, pos, len, opts, ensure_full_buf_consumed=true, row=0, col=0)
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
    return nothing, code, 0
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
