VERSION >= v"0.4.0-dev+6521" && __precompile__(true)
module CSV

using DataStreams, DataFrames, WeakRefStrings

export Data, DataFrame

if !isdefined(Core, :String)
    typealias String UTF8String
end

if Base.VERSION < v"0.5.0-dev+4631"
    unsafe_wrap{A<:Array}(::Type{A}, ptr, len) = pointer_to_array(ptr, len)
    unsafe_string(ptr, len) = utf8(ptr, len)
    unsafe_wrap(::Type{String}, ptr, len) = unsafe_string(ptr, len)
    escape_string(io, str1, str2) = print_escaped(io, str1, str2)
end

immutable CSVError <: Exception
    msg::String
end

const RETURN  = UInt8('\r')
const NEWLINE = UInt8('\n')
const COMMA   = UInt8(',')
const QUOTE   = UInt8('"')
const ESCAPE  = UInt8('\\')
const PERIOD  = UInt8('.')
const SPACE   = UInt8(' ')
const TAB     = UInt8('\t')
const MINUS   = UInt8('-')
const PLUS    = UInt8('+')
const NEG_ONE = UInt8('0')-UInt8(1)
const ZERO    = UInt8('0')
const TEN     = UInt8('9')+UInt8(1)
Base.isascii(c::UInt8) = c < 0x80

@inline function unsafe_read(from::Base.AbstractIOBuffer, ::Type{UInt8}=UInt8)
    @inbounds byte = from.data[from.ptr]
    from.ptr = from.ptr + 1
    return byte
end
unsafe_read(from::IO, T) = Base.read(from, T)

@inline function unsafe_peek(from::Base.AbstractIOBuffer)
    @inbounds byte = from.data[from.ptr]
    return byte
end
unsafe_peek(from::IO) = (mark(from); v = Base.read(from, UInt8); reset(from); return v)

"""
Represents the various configuration settings for csv file parsing.

Keyword Arguments:

 * `delim`::Union{Char,UInt8} = how fields in the file are delimited
 * `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
 * `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
 * `null`::String = indicates how NULL values are represented in the dataset
 * `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset
"""
type Options
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    separator::UInt8
    decimal::UInt8
    null::String
    nullcheck::Bool
    dateformat::Dates.DateFormat
    datecheck::Bool
    # non-public for now
    datarow::Int
    rows::Int
    header::Union{Integer,UnitRange{Int},Vector}
    types::Union{Dict{Int,DataType},Dict{String,DataType},Vector{DataType}}
end

Options(;delim=COMMA,quotechar=QUOTE,escapechar=ESCAPE,null=String(""),dateformat=Dates.ISODateFormat, datarow=-1, rows=0, header=1, types=DataType[]) =
    Options(delim%UInt8,quotechar%UInt8,escapechar%UInt8,COMMA,PERIOD,
            null,null != "",isa(dateformat,Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat),dateformat == Dates.ISODateFormat, datarow, rows, header, types)
function Base.show(io::IO,op::Options)
    println(io,"    CSV.Options:")
    println(io,"        delim: '",Char(op.delim),"'")
    println(io,"        quotechar: '",Char(op.quotechar),"'")
    print(io,"        escapechar: '"); escape_string(io,string(Char(op.escapechar)),"\\"); println(io,"'")
    print(io,"        null: \""); escape_string(io,op.null,"\\"); println(io,"\"")
    print(io,"        dateformat: ",op.dateformat)
end

"""
constructs a `CSV.Source` file ready to start parsing data from

implements the `Data.Source` interface for providing convenient `Data.stream!` methods for various `Data.Sink` types
"""
type Source{I<:IO} <: Data.Source
    schema::Data.Schema
    options::Options
    data::I
    datapos::Int # the position in the IOBuffer where the rows of data begins
    fullpath::String
end

function Base.show(io::IO,f::Source)
    println(io,"CSV.Source: ",f.fullpath)
    println(io,f.options)
    show(io, f.schema)
end

"""
constructs a `CSV.Sink` file ready to start writing data to

implements the `Data.Sink` interface for providing convenient `Data.stream!` methods for various `Data.Source` types
"""
type Sink <: Data.Sink
    schema::Data.Schema
    options::Options
    data::IO
    datapos::Int # the byte position in `io` where the data rows start
    header::Bool
    # quotefields::Bool # whether to always quote string fields or not
end

include("parsefields.jl")
include("io.jl")
include("Source.jl")
include("Sink.jl")

end # module
