# const CatStr = CategoricalString{UInt32}
struct PooledString end
export PooledString

const TypeCode = Int8

# default value to signal that parsing should try to detect a type
const EMPTY    = 0b00000000 % TypeCode

# MISSING is a mask that can be combined w/ any other TypeCode
const MISSING  = 0b10000000 % TypeCode

# enum-like type codes for basic supported types
const INT      = 0b00000001 % TypeCode
const FLOAT    = 0b00000010 % TypeCode
const DATE     = 0b00000011 % TypeCode
const DATETIME = 0b00000100 % TypeCode
const BOOL     = 0b00000101 % TypeCode
const STRING   = 0b00000110 % TypeCode

# POOL is also a mask that can be combined with PSTRING or CSTRING
const POOL     = 0b01000000 % TypeCode
const PSTRING  = 0b01000111 % TypeCode # PooledString
const CSTRING  = 0b01001000 % TypeCode # CategoricalString
const MISSINGTYPE = 0b00001001 % TypeCode

# a user-provided type; a mask that can be combined w/ basic types
const USER     = 0b00100000 % TypeCode

const TYPEBITS = 0b00001111 % TypeCode

missingtype(x::TypeCode) = (x & MISSING) === MISSING
pooled(x::TypeCode) = (x & POOL) === POOL
user(x::TypeCode) = (x & USER) === USER
typebits(x::TypeCode) = x & TYPEBITS

typecode(::Type{Missing}) = MISSINGTYPE
typecode(::Type{<:Integer}) = INT
typecode(::Type{<:AbstractFloat}) = FLOAT
typecode(::Type{Date}) = DATE
typecode(::Type{DateTime}) = DATETIME
typecode(::Type{Bool}) = BOOL
typecode(::Type{<:AbstractString}) = STRING
typecode(::Type{Tuple{Ptr{UInt8}, Int}}) = STRING
typecode(::Type{PooledString}) = PSTRING
# typecode(::Type{CatStr}) = CSTRING
typecode(::Type{Union{}}) = EMPTY
typecode(::Type{Union{T, Missing}}) where {T} = typecode(T) | MISSING
typecode(::Type{T}) where {T} = EMPTY
typecode(x::T) where {T} = typecode(T)

const TYPECODES = Dict(
    MISSING => Missing,
    INT => Int64,
    FLOAT => Float64,
    DATE => Date,
    DATETIME => DateTime,
    BOOL => Bool,
    INT | MISSING => Union{Int64, Missing},
    FLOAT | MISSING => Union{Float64, Missing},
    DATE | MISSING => Union{Date, Missing},
    DATETIME | MISSING => Union{DateTime, Missing},
    BOOL | MISSING => Union{Bool, Missing},
)

typecodes(x::Dict) = Dict(typecode(k)=>typecode(v) for (k, v) in x)
typecodes(x::Dict{TypeCode, TypeCode}) = x

getbuf(source::Vector{UInt8}, use_mmap) = source
function getbuf(source, use_mmap)
    if use_mmap && source isa String
        return Mmap.mmap(source)
    end
    iosource = source isa String ? open(source) : source
    A = Mmap.mmap(Vector{UInt8}, bytesavailable(iosource))
    read!(iosource, A)
    return A
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

const RESERVED = Set(["local", "global", "export", "let",
    "for", "struct", "while", "const", "continue", "import",
    "function", "if", "else", "try", "begin", "break", "catch",
    "return", "using", "baremodule", "macro", "finally",
    "module", "elseif", "end", "quote", "do"])

normalizename(name::Symbol) = name
function normalizename(name::String)
    uname = strip(Unicode.normalize(name))
    id = Base.isidentifier(uname) ? uname : map(c->Base.is_id_char(c) ? c : '_', uname)
    cleansed = string((isempty(id) || !Base.is_id_start_char(id[1]) || id in RESERVED) ? "_" : "", id)
    return Symbol(replace(cleansed, r"(_)\1+"=>"_"))
end

function makeunique(names)
    set = Set(names)
    length(set) == length(names) && return names
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
