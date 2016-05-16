using DataStreams
module CSV

using Compat, NullableArrays, DataStreams

const PointerString = Data.PointerString
const NULLSTRING = Data.NULLSTRING

immutable CSVError <: Exception
    msg::@compat(String)
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
import Base.peek

include("unsafebuffer.jl")

"""
Represents the various configuration settings for csv file parsing.

 * `delim`::Union{Char,UInt8} = how fields in the file are delimited
 * `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
 * `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
 * `null`::String = the ascii string that indicates how NULL values are represented in the dataset
 * `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset
"""
type Options
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    separator::UInt8
    decimal::UInt8
    null::@compat(String) # how null is represented in the dataset
    nullcheck::Bool   # do we have a custom null value to check for
    dateformat::Dates.DateFormat
    datecheck::Bool   # do we have a custom dateformat to check for
end

Options(;delim=COMMA,quotechar=QUOTE,escapechar=ESCAPE,null::@compat(String)="",dateformat=Dates.ISODateFormat) =
    Options(delim%UInt8,quotechar%UInt8,escapechar%UInt8,COMMA,PERIOD,
            null,null != "",isa(dateformat,Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat),dateformat == Dates.ISODateFormat)
function Base.show(io::IO,op::Options)
    println("    CSV.Options:")
    println(io,"        delim: '",@compat(Char(op.delim)),"'")
    println(io,"        quotechar: '",@compat(Char(op.quotechar)),"'")
    print(io,"        escapechar: '"); print_escaped(io,string(@compat(Char(op.escapechar))),"\\"); println(io,"'")
    print(io,"        null: \""); print_escaped(io,op.null,"\\"); println(io,"\"")
    print(io,"        dateformat: ",op.dateformat)
end

"`CSV.Source` satisfies the `DataStreams` interface for data processing for delimited `IO`."
type Source <: Data.Source
    schema::Data.Schema
    options::Options
    data::UnsafeBuffer
    datapos::Int # the position in the IOBuffer where the rows of data begins
    fullpath::@compat(String)
end

function Base.show(io::IO,f::Source)
    println(io,"CSV.Source: ",f.fullpath)
    println(io,f.options)
    showcompact(io, f.schema)
end

type Sink{I} <: Data.Sink
    schema::Data.Schema
    options::Options
    data::I
    datapos::Int # the byte position in `io` where the data rows start
    quotefields::Bool # whether to always quote string fields or not
end

include("io.jl")
include("Source.jl")
include("getfields.jl")
include("Sink.jl")

end # module
