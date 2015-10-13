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

include("io.jl")
include("Source.jl")
include("readfields.jl")
include("Sink.jl")

function readfield!{T}(source::Source, dest::NullableVector{T}, ::Type{T}, row, col)
    @inbounds val, null = CSV.readfield(source, T, row, col) # row + datarow
    @inbounds dest.values[row], dest.isnull[row] = val, null
    return
end
#TODO: support other column types in DataTable{T} (Matrix, DataFrame, Vector{Vector{T}})

function DataStreams.stream!(source::CSV.Source,sink::DataTable)
    rows, cols = size(source)
    types = DataStreams.types(source)
    #TODO: check if we need more rows? should DataTable hold a `current_row` field for appending?
    for row = 1:rows, col = 1:cols
        @inbounds T = types[col]
        CSV.readfield!(source, DataStreams.unsafe_column(sink, col, T), T, row, col) # row + datarow
    end
    return sink
end
# creates a new DataTable according to `source` schema and streams `Source` data into it
function DataStreams.DataTable(source::CSV.Source)
    sink = DataStreams.DataTable(source.schema)
    sink.other = source.data # keep a reference to our mmapped array for PointerStrings
    return stream!(source,sink)
end


end # module
