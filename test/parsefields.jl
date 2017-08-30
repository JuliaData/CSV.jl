using Base.Test, CSV, Libz, DecFP, WeakRefStrings, Nulls

@testset "Int" begin
# Int
io = IOBuffer("0")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("-1")
v = CSV.parsefield(io,Int)
@test v === -1

io = IOBuffer("+1")
v = CSV.parsefield(io,Int)
@test v === 1

io = IOBuffer("1")
v = CSV.parsefield(io,Int)
@test v === 1

io = IOBuffer("2000")
v = CSV.parsefield(io,Int)
@test v === 2000

io = IOBuffer("0.0")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = IOBuffer("0a")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = IOBuffer("")
@test_throws DataStreams.Data.NullException v = CSV.parsefield(io,Int)
@test isnull(CSV.parsefield(IOBuffer(""), Union{Int, Null}))

io = IOBuffer(" ")
@test isnull(CSV.parsefield(io, Union{Int, Null}))
@test_throws DataStreams.Data.NullException CSV.parsefield(IOBuffer(" "),Int)

@test_throws DataStreams.Data.NullException CSV.parsefield(IOBuffer("\t"),Int)
@test isnull(CSV.parsefield(IOBuffer("\t"), Union{Int, Null}))

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Int)
@test v === 10

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = IOBuffer("0\r")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = IOBuffer("0a\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Null})

io = IOBuffer("0,")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = IOBuffer("\r")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = IOBuffer("\"\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Int, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = IOBuffer("\"\\N\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int,CSV.Options(null="\\N"))

end # @testset "Int"

@testset "Int Libz" begin
# Int64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,Int)
@test v === -1

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io, Union{Int, Null})
@test v === 1

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Int)
@test v === 2000

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Null})
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Int)
@test v === 10

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Null})
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Int)
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Int)
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Null})

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Int)
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io, Union{Int, Null})
@test v === 0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Int,CSV.Options(null="\\N"))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io, Union{Int, Null}, CSV.Options(null="\\N"))
@test isnull(v)
end # @testset "Int Libz"

@testset "Float64 Libz" begin

# Float64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === -1.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 2000.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Null})
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Null})

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === .1234567890

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === .1234567890

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io,Float64)
@test v === NaN

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === NaN

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io,Float64)
@test v === Inf

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io, Union{Float64, Null})
@test v === Inf

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64,CSV.Options(null="\\N"))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io, Union{Float64, Null}, CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "Float64 Libz"

@testset "Float64" begin

# Float64
io = IOBuffer("1")
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = IOBuffer("-1.0")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === -1.0

io = IOBuffer("0")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("2000")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 2000.0

io = IOBuffer("0a")
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
io = IOBuffer("")
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = IOBuffer(" ")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Null})
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = IOBuffer("0\r")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = IOBuffer("0a\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Null})

io = IOBuffer("0,")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 0.0

io = IOBuffer("\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = IOBuffer("\r\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = IOBuffer(".1234567890")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === .1234567890

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === .1234567890

io = IOBuffer("-.1")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === -0.1

io = IOBuffer("1e4")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 1e4

io = IOBuffer("1e-4")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 1e-4

io = IOBuffer("1.1e4")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === 1.1e4

io = IOBuffer(".1E4")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === .1E4

io = IOBuffer("nan")
v = CSV.parsefield(io,Float64)
@test v === NaN

io = IOBuffer("NaN")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === NaN

io = IOBuffer("NaN,")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === NaN

io = IOBuffer("inf")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = IOBuffer("inf,")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = IOBuffer("infinity")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === Inf

io = IOBuffer("infinity,")
v = CSV.parsefield(io, Union{Float64, Null})
@test v === Inf

io = IOBuffer("\\N")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64,CSV.Options(null="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{Float64, Null}, CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "Float64"

@testset "DecFP Libz" begin

# DecFP Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v == Dec64(-1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(2000.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(1234567890.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v == Dec64(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io, Union{Dec64, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64,CSV.Options(null="\\N"))

end # @testset "DecFP Libz"

@testset "DecFP" begin

# DecFP
io = IOBuffer("1")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = IOBuffer("-1.0")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v == Dec64(-1.0)

io = IOBuffer("0")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("2000")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(2000.0)

io = IOBuffer("0a")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = IOBuffer("")
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = IOBuffer(" ")
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = IOBuffer("\t")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = IOBuffer("\"1_00a0\"")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(0.0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(0.0)

io = IOBuffer("0a\n")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Null})

io = IOBuffer("0,")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(0.0)

io = IOBuffer("\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64)

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = IOBuffer("\r\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64)

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{Dec64, Null})
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1234567890.0)

io = IOBuffer(".1234567890")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v == Dec64(.1234567890)

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v == Dec64(.1234567890)

io = IOBuffer("nan")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = IOBuffer("NaN")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(NaN)

io = IOBuffer("inf")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = IOBuffer("infinity")
v = CSV.parsefield(io, Union{Dec64, Null})
@test v === Dec64(Inf)

io = IOBuffer("\\N")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Dec64,CSV.Options(null="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{Dec64, Null}, CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "DecFP"

@testset "WeakRefString" begin

# WeakRefString
io = IOBuffer("0")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "0"

io = IOBuffer("-1")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "-1"

io = IOBuffer("1")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "1"

io = IOBuffer("2000")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "2000"

io = IOBuffer("0.0")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "0.0"

io = IOBuffer("0a")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = IOBuffer("")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " "

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "\t"

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " \t 010"

io = IOBuffer("\"1_00a0\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "1_00a0"

io = IOBuffer("\"0\"")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "0"

io = IOBuffer("0\r")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "0"

io = IOBuffer("0a\n")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "\t0\t"

io = IOBuffer("0,")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "0"

io = IOBuffer("\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,WeakRefString{UInt8})

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test isnull(v)

io = IOBuffer("\r\n")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,WeakRefString{UInt8})

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "1234567890"

io = IOBuffer("\"hey there\\\"quoted field\\\"\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test v == "hey there\\\"quoted field\\\""

io = IOBuffer("\\N")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null}, CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "WeakRefString"

@testset "String Libz" begin

# Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,String)
@test v == "-1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,String)
@test v == "1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,String)
@test v == "2000"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
v = CSV.parsefield(io,String)
@test v == "0.0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
v = CSV.parsefield(io,String)
@test v == "0a"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,String)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,String)
@test v == " "

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,String)
@test v == "\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,String)
@test v == " \t 010"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
v = CSV.parsefield(io,String)
@test v == "1_00a0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
v = CSV.parsefield(io,String)
@test v == "0a"

# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
v = CSV.parsefield(io,String)
@test v == "\t0\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,String)
@test v == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,String)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io, Union{String, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,String)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io, Union{String, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,String)
@test v == "1234567890"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"hey there\\\"quoted field\\\"\"")))
v = CSV.parsefield(io,String)
@test v == "hey there\\\"quoted field\\\""

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,String,CSV.Options(null="\\N"))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io, Union{String, Null}, CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "String Libz"

@testset "Date" begin

# Date
io = IOBuffer("")
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = IOBuffer(",")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = IOBuffer("\r")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = IOBuffer("\"\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Date, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = IOBuffer("\"\\N\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date,CSV.Options(null="\\N"))

io = IOBuffer("2015-10-05")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("\"2015-10-05\"")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05,")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\r")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\r\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("  \"2015-10-05\",")
@test_throws ArgumentError CSV.parsefield(io,Date)

io = IOBuffer("\"2015-10-05\"\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = IOBuffer("\"10/5/2015\"\n")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

io = IOBuffer("\"10/5/2015\"\r")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

io = IOBuffer("\"10/5/2015\"\r\n")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

end # @testset "Date"

@testset "Date Libz" begin

# Date Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io, Union{Date, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date,CSV.Options(null="\\N"))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05,")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\n")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r\n")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05\",")))
@test_throws ArgumentError CSV.parsefield(io,Date)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"\n")))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015\"\n")))
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")))
@test v === Date(2015,10,5)

end # @testset "Date Libz"

@testset "DateTime" begin

# DateTime
io = IOBuffer("")
v = CSV.parsefield(io, Union{DateTime, Null})
@test isnull(v)

io = IOBuffer(",")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{DateTime, Null})

io = IOBuffer("\r")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{DateTime, Null})
@test isnull(v)

io = IOBuffer("\"\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{DateTime, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = IOBuffer("\"\\N\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime,CSV.Options(null="\\N"))

io = IOBuffer("2015-10-05T00:00:01")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("\"2015-10-05T00:00:01\"")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01,")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\r")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\r\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("  \"2015-10-05T00:00:01\",")
@test_throws ArgumentError CSV.parsefield(io,DateTime)

io = IOBuffer("\"2015-10-05T00:00:01\"\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("\"10/5/2015 00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("\"10/5/2015 00:00:01\"\r")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("\"10/5/2015 00:00:01\"\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

end # @testset "DateTime"

@testset "DateTime Libz" begin

# DateTime Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io, Union{DateTime, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io, Union{DateTime, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io, Union{DateTime, Null})
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io, Union{DateTime, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
@test_throws DataStreams.Data.NullException CSV.parsefield(io,DateTime,CSV.Options(null="\\N"))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01,")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\n")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r\n")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05T00:00:01\",")))
@test_throws ArgumentError CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"\n")))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015 00:00:01\"\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

end # @testset "DateTime Libz"

@testset "All types" begin

# All types
io = IOBuffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io, Union{Date, Null})
@test isnull(v)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io, Union{Float64, Null})
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test isnull(v)
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Date)

v = CSV.parsefield(io, Union{Int, Null})
@test isnull(v)
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Float64)
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Null})
@test isnull(v)

# Char
io = IOBuffer("0")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("-")
v = CSV.parsefield(io,Char)
@test v === '-'

io = IOBuffer("+")
v = CSV.parsefield(io,Char)
@test v === '+'

io = IOBuffer("1")
v = CSV.parsefield(io,Char)
@test v === '1'

io = IOBuffer("2000")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)

io = IOBuffer("0.0")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)
io = IOBuffer("0a")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)
io = IOBuffer("")
v = CSV.parsefield(io, Union{Char, Null})
@test isnull(v)

io = IOBuffer(" ")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Char)

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Char, Null})
@test isnull(v)

io = IOBuffer(" \t 0")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0\r")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0a\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Char)

io = IOBuffer("0,")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Char, Null})
@test isnull(v)

io = IOBuffer("\r")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Char)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Char, Null})
@test isnull(v)

io = IOBuffer("\"\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Char)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Char, Null}, CSV.Options(null="\\N"))
@test isnull(v)

io = IOBuffer("\"\\N\"")
@test_throws DataStreams.Data.NullException CSV.parsefield(io,Char,CSV.Options(null="\\N"))

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Int64, Null}, CSV.Options(delim='\t'))
@test isnull(v)

end # @testset "All types"

# Custom type
struct CustomInt
    val::Int
end
Base.parse(::Type{CustomInt}, x) = CustomInt(parse(Int, x))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io, CustomInt, CSV.Options())
@test v.val == 1
