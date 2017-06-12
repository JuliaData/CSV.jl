# DataFrame = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

using Base.Test, CSV, Libz, DecFP, WeakRefStrings, Nulls

@testset "CSV.parsefield" begin
@testset "Int" begin
# Int
io = CSV.Buffer("0")
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer("-1")
v = CSV.parsefield(io,Int)
@test v === -1

io = CSV.Buffer("+1")
v = CSV.parsefield(io,Int)
@test v === 1

io = CSV.Buffer("1")
v = CSV.parsefield(io,Int)
@test v === 1

io = CSV.Buffer("2000")
v = CSV.parsefield(io,Int)
@test v === 2000

io = CSV.Buffer("0.0")
@test_throws CSV.CSVError CSV.parsefield(io,Int)
io = CSV.Buffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Int)
io = CSV.Buffer("")
@test_throws NullException v = CSV.parsefield(io,Int)
@test isnull(CSV.parsefield(CSV.Buffer(""),?Int))

io = CSV.Buffer(" ")
@test isnull(CSV.parsefield(io,?Int))
@test_throws NullException CSV.parsefield(CSV.Buffer(" "),Int)

@test_throws NullException CSV.parsefield(CSV.Buffer("\t"),Int)
@test isnull(CSV.parsefield(CSV.Buffer("\t"),?Int))

io = CSV.Buffer(" \t 010")
v = CSV.parsefield(io,Int)
@test v === 10

io = CSV.Buffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Int)
io = CSV.Buffer("\"0\"")
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer("0\n")
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer("0\r")
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer("0\r\n")
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Int)
# Should we handle trailing whitespace?
io = CSV.Buffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,?Int)

io = CSV.Buffer("0,")
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer("0,\n")
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer("\n")
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer("\r")
@test_throws NullException CSV.parsefield(io,Int)

io = CSV.Buffer("\r\n")
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer("\"\"")
@test_throws NullException CSV.parsefield(io,Int)

io = CSV.Buffer("1234567890")
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = CSV.Buffer("\\N")
v = CSV.parsefield(io,?Int,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer("\"\\N\"")
@test_throws NullException CSV.parsefield(io,Int,CSV.Options(null="\\N"))

end # @testset "Int"

@testset "Int Libz" begin
# Int64 Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0"))))
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1"))))
v = CSV.parsefield(io,Int)
@test v === -1

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1"))))
v = CSV.parsefield(io,?Int)
@test v === 1

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000"))))
v = CSV.parsefield(io,Int)
@test v === 2000

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0"))))
@test_throws CSV.CSVError CSV.parsefield(io,?Int)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a"))))
@test_throws CSV.CSVError CSV.parsefield(io,Int)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" "))))
@test_throws NullException CSV.parsefield(io,Int)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t"))))
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010"))))
v = CSV.parsefield(io,Int)
@test v === 10

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\""))))
@test_throws CSV.CSVError CSV.parsefield(io,?Int)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\""))))
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n"))))
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r"))))
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n"))))
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n"))))
@test_throws CSV.CSVError CSV.parsefield(io,Int)
# Should we handle trailing whitespace?
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n"))))
@test_throws CSV.CSVError CSV.parsefield(io,?Int)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,"))))
v = CSV.parsefield(io,Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n"))))
v = CSV.parsefield(io,?Int)
@test v === 0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
@test_throws NullException CSV.parsefield(io,Int)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
@test_throws NullException CSV.parsefield(io,Int)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
v = CSV.parsefield(io,?Int)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890"))))
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
@test_throws NullException CSV.parsefield(io,Int,CSV.Options(null="\\N"))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
v = CSV.parsefield(io,?Int,CSV.Options(null="\\N"))
@test isnull(v)
end # @testset "Int Libz"

@testset "Float64 Libz" begin

# Float64 Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1"))))
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0"))))
v = CSV.parsefield(io,?Float64)
@test v === -1.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0"))))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000"))))
v = CSV.parsefield(io,?Float64)
@test v === 2000.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a"))))
@test_throws CSV.CSVError CSV.parsefield(io,Float64)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" "))))
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t"))))
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010"))))
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\""))))
@test_throws CSV.CSVError CSV.parsefield(io,?Float64)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\""))))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n"))))
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r"))))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n"))))
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n"))))
@test_throws CSV.CSVError CSV.parsefield(io,Float64)
# Should we handle trailing whitespace?
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n"))))
@test_throws CSV.CSVError CSV.parsefield(io,?Float64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,"))))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n"))))
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890"))))
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890"))))
v = CSV.parsefield(io,?Float64)
@test v === .1234567890

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890"))))
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\""))))
v = CSV.parsefield(io,?Float64)
@test v === .1234567890

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan"))))
v = CSV.parsefield(io,Float64)
@test v === NaN

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN"))))
v = CSV.parsefield(io,?Float64)
@test v === NaN

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf"))))
v = CSV.parsefield(io,Float64)
@test v === Inf

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity"))))
v = CSV.parsefield(io,?Float64)
@test v === Inf

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
@test_throws NullException CSV.parsefield(io,Float64,CSV.Options(null="\\N"))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
v = CSV.parsefield(io,?Float64,CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "Float64 Libz"

@testset "Float64" begin

# Float64
io = CSV.Buffer("1")
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = CSV.Buffer("-1.0")
v = CSV.parsefield(io,?Float64)
@test v === -1.0

io = CSV.Buffer("0")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer("2000")
v = CSV.parsefield(io,?Float64)
@test v === 2000.0

io = CSV.Buffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Float64)
io = CSV.Buffer("")
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(" ")
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer("\t")
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer(" \t 010")
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = CSV.Buffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,?Float64)
io = CSV.Buffer("\"0\"")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer("0\n")
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer("0\r")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer("0\r\n")
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Float64)
# Should we handle trailing whitespace?
io = CSV.Buffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,?Float64)

io = CSV.Buffer("0,")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = CSV.Buffer("0,\n")
v = CSV.parsefield(io,?Float64)
@test v === 0.0

io = CSV.Buffer("\n")
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer("\r")
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer("\r\n")
@test_throws NullException CSV.parsefield(io,Float64)

io = CSV.Buffer("\"\"")
v = CSV.parsefield(io,?Float64)
@test isnull(v)

io = CSV.Buffer("1234567890")
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = CSV.Buffer(".1234567890")
v = CSV.parsefield(io,?Float64)
@test v === .1234567890

io = CSV.Buffer("0.1234567890")
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = CSV.Buffer("\"0.1234567890\"")
v = CSV.parsefield(io,?Float64)
@test v === .1234567890

io = CSV.Buffer("-.1")
v = CSV.parsefield(io,?Float64)
@test v === -0.1

io = CSV.Buffer("1e4")
v = CSV.parsefield(io,?Float64)
@test v === 1e4

io = CSV.Buffer("1e-4")
v = CSV.parsefield(io,?Float64)
@test v === 1e-4

io = CSV.Buffer("1.1e4")
v = CSV.parsefield(io,?Float64)
@test v === 1.1e4

io = CSV.Buffer(".1E4")
v = CSV.parsefield(io,?Float64)
@test v === .1E4

io = CSV.Buffer("nan")
v = CSV.parsefield(io,Float64)
@test v === NaN

io = CSV.Buffer("NaN")
v = CSV.parsefield(io,?Float64)
@test v === NaN

io = CSV.Buffer("NaN,")
v = CSV.parsefield(io,?Float64)
@test v === NaN

io = CSV.Buffer("inf")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = CSV.Buffer("inf,")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = CSV.Buffer("infinity")
v = CSV.parsefield(io,?Float64)
@test v === Inf

io = CSV.Buffer("infinity,")
v = CSV.parsefield(io,?Float64)
@test v === Inf

io = CSV.Buffer("\\N")
@test_throws NullException CSV.parsefield(io,Float64,CSV.Options(null="\\N"))

io = CSV.Buffer("\"\\N\"")
v = CSV.parsefield(io,?Float64,CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "Float64"

@testset "DecFP Libz" begin

# DecFP Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0"))))
v = CSV.parsefield(io,?Dec64)
@test v == Dec64(-1.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(2000.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a"))))
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" "))))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t"))))
@test_throws ArgumentError CSV.parsefield(io,?Dec64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\""))))
@test_throws ArgumentError CSV.parsefield(io,?Dec64)
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\""))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n"))))
@test_throws ArgumentError CSV.parsefield(io,?Dec64)
# Should we handle trailing whitespace?
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n"))))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
@test_throws NullException CSV.parsefield(io,Dec64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
@test_throws NullException CSV.parsefield(io,Dec64)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(1234567890.0)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890"))))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890"))))
v = CSV.parsefield(io,?Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\""))))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(NaN)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf"))))
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(Inf)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity"))))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
v = CSV.parsefield(io,?Dec64,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
@test_throws NullException CSV.parsefield(io,Dec64,CSV.Options(null="\\N"))

end # @testset "DecFP Libz"

@testset "DecFP" begin

# DecFP
io = CSV.Buffer("1")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = CSV.Buffer("-1.0")
v = CSV.parsefield(io,?Dec64)
@test v == Dec64(-1.0)

io = CSV.Buffer("0")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("2000")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(2000.0)

io = CSV.Buffer("0a")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = CSV.Buffer("")
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer(" ")
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = CSV.Buffer("\t")
@test_throws ArgumentError CSV.parsefield(io,?Dec64)

io = CSV.Buffer(" \t 010")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = CSV.Buffer("\"1_00a0\"")
@test_throws ArgumentError CSV.parsefield(io,?Dec64)
io = CSV.Buffer("\"0\"")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("0\n")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("0\r")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("0\r\n")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("0a\n")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
# Should we handle trailing whitespace?
io = CSV.Buffer("\t0\t\n")
@test_throws ArgumentError CSV.parsefield(io,?Dec64)

io = CSV.Buffer("0,")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("0,\n")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(0.0)

io = CSV.Buffer("\n")
@test_throws NullException CSV.parsefield(io,Dec64)

io = CSV.Buffer("\r")
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer("\r\n")
@test_throws NullException CSV.parsefield(io,Dec64)

io = CSV.Buffer("\"\"")
v = CSV.parsefield(io,?Dec64)
@test isnull(v)

io = CSV.Buffer("1234567890")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1234567890.0)

io = CSV.Buffer(".1234567890")
v = CSV.parsefield(io,?Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer("0.1234567890")
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer("\"0.1234567890\"")
v = CSV.parsefield(io,?Dec64)
@test v == Dec64(.1234567890)

io = CSV.Buffer("nan")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = CSV.Buffer("NaN")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(NaN)

io = CSV.Buffer("inf")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = CSV.Buffer("infinity")
v = CSV.parsefield(io,?Dec64)
@test v === Dec64(Inf)

io = CSV.Buffer("\\N")
@test_throws NullException CSV.parsefield(io,Dec64,CSV.Options(null="\\N"))

io = CSV.Buffer("\"\\N\"")
v = CSV.parsefield(io,?Dec64,CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "DecFP"

@testset "WeakRefString" begin

# WeakRefString
io = CSV.Buffer("0")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("-1")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "-1"

io = CSV.Buffer("1")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "1"

io = CSV.Buffer("2000")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "2000"

io = CSV.Buffer("0.0")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "0.0"

io = CSV.Buffer("0a")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = CSV.Buffer("")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test isnull(v)

io = CSV.Buffer(" ")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " "

io = CSV.Buffer("\t")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "\t"

io = CSV.Buffer(" \t 010")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " \t 010"

io = CSV.Buffer("\"1_00a0\"")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "1_00a0"

io = CSV.Buffer("\"0\"")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("0\n")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("0\r")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("0\r\n")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("0a\n")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = CSV.Buffer("\t0\t\n")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "\t0\t"

io = CSV.Buffer("0,")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("0,\n")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "0"

io = CSV.Buffer("\n")
@test_throws NullException CSV.parsefield(io,WeakRefString{UInt8})

io = CSV.Buffer("\r")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test isnull(v)

io = CSV.Buffer("\r\n")
@test_throws NullException CSV.parsefield(io,WeakRefString{UInt8})

io = CSV.Buffer("\"\"")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test isnull(v)

io = CSV.Buffer("1234567890")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "1234567890"

io = CSV.Buffer("\"hey there\\\"quoted field\\\"\"")
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test v == "hey there\\\"quoted field\\\""

io = CSV.Buffer("\\N")
@test_throws NullException CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"))

io = CSV.Buffer("\"\\N\"")
v = CSV.parsefield(io,?WeakRefString{UInt8},CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "WeakRefString"

@testset "String Libz" begin

# Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1"))))
v = CSV.parsefield(io,String)
@test v == "-1"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1"))))
v = CSV.parsefield(io,String)
@test v == "1"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000"))))
v = CSV.parsefield(io,String)
@test v == "2000"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0"))))
v = CSV.parsefield(io,String)
@test v == "0.0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a"))))
v = CSV.parsefield(io,String)
@test v == "0a"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
@test_throws NullException CSV.parsefield(io,String)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" "))))
v = CSV.parsefield(io,String)
@test v == " "

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t"))))
v = CSV.parsefield(io,String)
@test v == "\t"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010"))))
v = CSV.parsefield(io,String)
@test v == " \t 010"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\""))))
v = CSV.parsefield(io,String)
@test v == "1_00a0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\""))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n"))))
v = CSV.parsefield(io,String)
@test v == "0a"

# Should we handle trailing whitespace?
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n"))))
v = CSV.parsefield(io,String)
@test v == "\t0\t"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n"))))
v = CSV.parsefield(io,String)
@test v == "0"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
@test_throws NullException CSV.parsefield(io,String)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
v = CSV.parsefield(io,?String)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
@test_throws NullException CSV.parsefield(io,String)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
v = CSV.parsefield(io,?String)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890"))))
v = CSV.parsefield(io,String)
@test v == "1234567890"

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"hey there\\\"quoted field\\\"\""))))
v = CSV.parsefield(io,String)
@test v == "hey there\\\"quoted field\\\""

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
@test_throws NullException CSV.parsefield(io,String,CSV.Options(null="\\N"))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
v = CSV.parsefield(io,?String,CSV.Options(null="\\N"))
@test isnull(v)

end # @testset "String Libz"

@testset "Date" begin

# Date
io = CSV.Buffer("")
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer(",")
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer("\n")
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer("\r")
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer("\r\n")
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer("\"\"")
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer("\\N")
v = CSV.parsefield(io,?Date,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer("\"\\N\"")
@test_throws NullException CSV.parsefield(io,Date,CSV.Options(null="\\N"))

io = CSV.Buffer("2015-10-05")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("\"2015-10-05\"")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("2015-10-05,")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("2015-10-05\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("2015-10-05\r")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("2015-10-05\r\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("  \"2015-10-05\",")
@test_throws ArgumentError CSV.parsefield(io,Date)

io = CSV.Buffer("\"2015-10-05\"\n")
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer("\"10/5/2015\"\n")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

io = CSV.Buffer("\"10/5/2015\"\r")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

io = CSV.Buffer("\"10/5/2015\"\r\n")
v = CSV.parsefield(io,Date,CSV.Options(dateformat=dateformat"mm/dd/yyyy"))
@test v === Date(2015,10,5)

end # @testset "Date"

@testset "Date Libz" begin

# Date Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(","))))
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
v = CSV.parsefield(io,?Date)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
@test_throws NullException CSV.parsefield(io,Date)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
v = CSV.parsefield(io,?Date,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
@test_throws NullException CSV.parsefield(io,Date,CSV.Options(null="\\N"))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\""))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05,"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\n"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r\n"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05\","))))
@test_throws ArgumentError CSV.parsefield(io,Date)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"\n"))))
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015\"\n"))))
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")))
@test v === Date(2015,10,5)

end # @testset "Date Libz"

@testset "DateTime" begin

# DateTime
io = CSV.Buffer("")
v = CSV.parsefield(io,?DateTime)
@test isnull(v)

io = CSV.Buffer(",")
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer("\n")
v = CSV.parsefield(io,?DateTime)

io = CSV.Buffer("\r")
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer("\r\n")
v = CSV.parsefield(io,?DateTime)
@test isnull(v)

io = CSV.Buffer("\"\"")
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer("\\N")
v = CSV.parsefield(io,?DateTime,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer("\"\\N\"")
@test_throws NullException CSV.parsefield(io,DateTime,CSV.Options(null="\\N"))

io = CSV.Buffer("2015-10-05T00:00:01")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("\"2015-10-05T00:00:01\"")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("2015-10-05T00:00:01,")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("2015-10-05T00:00:01\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("2015-10-05T00:00:01\r")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("2015-10-05T00:00:01\r\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("  \"2015-10-05T00:00:01\",")
@test_throws ArgumentError CSV.parsefield(io,DateTime)

io = CSV.Buffer("\"2015-10-05T00:00:01\"\n")
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("\"10/5/2015 00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("\"10/5/2015 00:00:01\"\r")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer("\"10/5/2015 00:00:01\"\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

end # @testset "DateTime"

@testset "DateTime Libz" begin

# DateTime Libz
io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(""))))
v = CSV.parsefield(io,?DateTime)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(","))))
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n"))))
v = CSV.parsefield(io,?DateTime)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r"))))
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n"))))
v = CSV.parsefield(io,?DateTime)
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\""))))
@test_throws NullException CSV.parsefield(io,DateTime)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N"))))
v = CSV.parsefield(io,?DateTime,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\""))))
@test_throws NullException CSV.parsefield(io,DateTime,CSV.Options(null="\\N"))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\""))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01,"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\n"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r\n"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05T00:00:01\","))))
@test_throws ArgumentError CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"\n"))))
v = CSV.parsefield(io,DateTime)
@test v === DateTime(2015,10,5,0,0,1)

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015 00:00:01\"\n"))))
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

end # @testset "DateTime Libz"

@testset "All types" begin

# All types
io = CSV.Buffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

v = CSV.parsefield(io,?Int)
@test isnull(v)
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,?Date)
@test isnull(v)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,?Float64)
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date)
@test v === Date(2015,10,5)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test isnull(v)
@test_throws NullException CSV.parsefield(io,Date)

v = CSV.parsefield(io,?Int)
@test isnull(v)
@test_throws NullException CSV.parsefield(io,Float64)
v = CSV.parsefield(io,?WeakRefString{UInt8})
@test isnull(v)

# Char
io = CSV.Buffer("0")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("-")
v = CSV.parsefield(io,Char)
@test v === '-'

io = CSV.Buffer("+")
v = CSV.parsefield(io,Char)
@test v === '+'

io = CSV.Buffer("1")
v = CSV.parsefield(io,Char)
@test v === '1'

io = CSV.Buffer("2000")
@test_throws CSV.CSVError CSV.parsefield(io,Char)

io = CSV.Buffer("0.0")
@test_throws CSV.CSVError CSV.parsefield(io,Char)
io = CSV.Buffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Char)
io = CSV.Buffer("")
v = CSV.parsefield(io,?Char)
@test isnull(v)

io = CSV.Buffer(" ")
@test_throws NullException CSV.parsefield(io,Char)

io = CSV.Buffer("\t")
v = CSV.parsefield(io,?Char)
@test isnull(v)

io = CSV.Buffer(" \t 0")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Char)
io = CSV.Buffer("\"0\"")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("0\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("0\r")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("0\r\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Char)
# Should we handle trailing whitespace?
io = CSV.Buffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Char)

io = CSV.Buffer("0,")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("0,\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = CSV.Buffer("\n")
v = CSV.parsefield(io,?Char)
@test isnull(v)

io = CSV.Buffer("\r")
@test_throws NullException CSV.parsefield(io,Char)

io = CSV.Buffer("\r\n")
v = CSV.parsefield(io,?Char)
@test isnull(v)

io = CSV.Buffer("\"\"")
@test_throws NullException CSV.parsefield(io,Char)

io = CSV.Buffer("\\N")
v = CSV.parsefield(io,?Char,CSV.Options(null="\\N"))
@test isnull(v)

io = CSV.Buffer("\"\\N\"")
@test_throws NullException CSV.parsefield(io,Char,CSV.Options(null="\\N"))

io = CSV.Buffer("\t")
v = CSV.parsefield(io,?Int64,CSV.Options(delim='\t'))
@test isnull(v)

end # @testset "All types"
end # @testset "CSV.parsefield"

# Custom type
type CustomInt
    val::Int
end
Base.parse(::Type{CustomInt}, x) = CustomInt(parse(Int, x))

io = CSV.Buffer(ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1"))))
v = CSV.parsefield(io, CustomInt, CSV.Options())
@test v.val == 1
