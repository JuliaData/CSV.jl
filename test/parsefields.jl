# Int
io = IOBuffer("0")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("-1")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(-1)

io = IOBuffer("+1")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)

io = IOBuffer("1")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)

io = IOBuffer("2000")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(2000)

io = IOBuffer("0.0")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(10)

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1234567890)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Int64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(-1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(2000)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(10)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Float64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(-1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(2000.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(10.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1234567890.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Float64
io = IOBuffer("1")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)

io = IOBuffer("-1.0")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(-1.0)

io = IOBuffer("0")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("2000")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(2000.0)

io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(10.0)

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1234567890.0)

io = IOBuffer(".1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("nan")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = IOBuffer("NaN")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = IOBuffer("inf")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = IOBuffer("infinity")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# DecFP Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(-1.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(2000.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(10.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1234567890.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# DecFP
io = IOBuffer("1")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1.0))

io = IOBuffer("-1.0")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(-1.0))

io = IOBuffer("0")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("2000")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(2000.0))

io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(10.0))

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\r")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1234567890.0))

io = IOBuffer(".1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("nan")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = IOBuffer("NaN")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = IOBuffer("inf")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = IOBuffer("infinity")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = IOBuffer("\\N")
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# WeakRefString
io = IOBuffer("0")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("-1")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "-1"

io = IOBuffer("1")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1"

io = IOBuffer("2000")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "2000"

io = IOBuffer("0.0")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0.0"

io = IOBuffer("0a")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0a"

io = IOBuffer("")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == " "

io = IOBuffer("\t")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "\t"

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == " \t 010"

io = IOBuffer("\"1_00a0\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1_00a0"

io = IOBuffer("\"0\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\r")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0a\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0a"

# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "\t0\t"

io = IOBuffer("0,")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0,\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1234567890"

io = IOBuffer("\"hey there\\\"quoted field\\\"\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there\\\"quoted field\\\""

io = IOBuffer("\\N")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "-1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "2000"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0.0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0a"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == " "

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == " \t 010"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1_00a0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0a"

# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "\t0\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1234567890"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"hey there\\\"quoted field\\\"\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "hey there\\\"quoted field\\\""

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,String,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,String,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Date

io = IOBuffer("")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(",")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("2015-10-05")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05,")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("  \"2015-10-05\",")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"10/5/2015\"\n")
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"10/5/2015\"\r")
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"10/5/2015\"\r\n")
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

# Date Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05,")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05\",")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015\"\n")))
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

# DateTime

io = IOBuffer("")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(",")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\\N")
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("2015-10-05T00:00:01")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"2015-10-05T00:00:01\"")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01,")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\r")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("  \"2015-10-05T00:00:01\",")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"2015-10-05T00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"10/5/2015 00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"10/5/2015 00:00:01\"\r")
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"10/5/2015 00:00:01\"\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

# DateTime Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01,")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05T00:00:01\",")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015 00:00:01\"\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

# All types
io = IOBuffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

# Custom type
type CustomInt
    val::Int
end
Base.parse(::Type{CustomInt}, x) = CustomInt(parse(Int, x))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io, CustomInt, CSV.Options(),1,1)
@test get(v).val == 1
