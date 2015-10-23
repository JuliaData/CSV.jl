using DataStreams
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

end # module
