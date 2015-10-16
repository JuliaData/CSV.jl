module CSV

using Compat, NullableArrays, Libz, DataStreams

const PointerString = Data.PointerString
const NULLSTRING = Data.NULLSTRING

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

include("io.jl")
include("Source.jl")
include("getfields.jl")
include("Sink.jl")

function getfield!{T}(io::IOBuffer, dest::NullableVector{T}, ::Type{T}, opts, row, col)
    @inbounds val, null = CSV.getfield(io, T, opts, row, col) # row + datarow
    @inbounds dest.values[row], dest.isnull[row] = val, null
    return
end
#TODO: support other column types in DataTable{T} (Matrix, DataFrame, Vector{Vector{T}})

function Data.stream!(source::CSV.Source,sink::Data.Table)
    rows, cols = size(source)
    types = Data.types(source)
    io = source.data
    opts = source.options
    #TODO: check if we need more rows?
    for row = 1:rows, col = 1:cols
        @inbounds T = types[col]
        CSV.getfield!(io, Data.unsafe_column(sink, col, T), T, opts, row, col) # row + datarow
    end
    return sink
end
# creates a new DataTable according to `source` schema and streams `Source` data into it
function Data.Table(source::CSV.Source)
    sink = Data.Table(source.schema)
    sink.other = source.data # keep a reference to our mmapped array for PointerStrings
    return Data.stream!(source,sink)
end


end # module
