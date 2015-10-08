module CSV

using Compat, NullableArrays, Libz, DataStreams

immutable CSVError <: Exception
    msg::ASCIIString
end

const RETURN  = @compat UInt8('\r')
const NEWLINE = @compat UInt8('\n')
const COMMA   = @compat UInt8(',')
const QUOTE   = @compat UInt8('"')
const ESCAPE  = @compat UInt8('\\')
const PERIOD  = @compat UInt8('.')
const SPACE   = @compat UInt8(' ')
const TAB     = @compat UInt8('\t')
const MINUS   = @compat UInt8('-')
const PLUS    = @compat UInt8('+')
const NEG_ONE = @compat UInt8('0')-UInt8(1)
const ZERO    = @compat UInt8('0')
const TEN     = @compat UInt8('9')+UInt8(1)
Base.isascii(c::UInt8) = c < 0x80
import Base.peek

immutable PointerString <: AbstractString
    ptr::Ptr{UInt8}
    len::Int
end
const NULLSTRING = PointerString(C_NULL,0)
Base.show(io::IO, x::PointerString) = print(io,x == NULLSTRING ? "" : "\"$(bytestring(x.ptr,x.len))\"")
Base.endof(x::PointerString) = x.len
Base.string(x::PointerString) = x == NULLSTRING ? "" : "$(bytestring(x.ptr,x.len))"

include("io.jl")
include("Source.jl")
include("readfields.jl")
include("Sink.jl")

DataStreams.DataStream(source::CSV.Source) = DataStreams.DataStream(source.schema)

function readfield!{T}(source::Source, dest::NullableVector{T}, ::Type{T}, row, col)
    @inbounds val, null = CSV.readfield(source, T, row, col) # row + datarow
    @inbounds dest.values[row], dest.isnull[row] = val, null
    return
end

function DataStreams.stream!(source::CSV.Source,sink::DataStream)
    rows, cols = size(source)
    types = source.schema.types
    data = sink.data
    for row = 1:rows, col = 1:cols
        CSV.readfield!(source, data[col], types[col], row, col) # row + datarow
    end
    return sink
end

end # module

#TODO
 # Create Source(::IOBuffer)
