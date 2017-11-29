using DecFP

# custom IO type to test non-IOBuffer code paths
mutable struct Buffer <: IO
    io::IOBuffer
end
Base.eof(b::Buffer) = eof(b.io)
Base.read(b::Buffer, ::Type{UInt8}) = Base.read(b.io, UInt8)
Base.peek(b::Buffer) = Base.peek(b.io)
Base.mark(b::Buffer) = Base.mark(b.io)
Base.reset(b::Buffer) = Base.reset(b.io)
Base.position(b::Buffer) = Base.position(b.io)

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
@test_throws Missings.MissingException v = CSV.parsefield(io,Int)
@test ismissing(CSV.parsefield(IOBuffer(""), Union{Int, Missing}))

io = IOBuffer(" ")
@test ismissing(CSV.parsefield(io, Union{Int, Missing}))
@test_throws Missings.MissingException CSV.parsefield(IOBuffer(" "),Int)

@test_throws Missings.MissingException CSV.parsefield(IOBuffer("\t"),Int)
@test ismissing(CSV.parsefield(IOBuffer("\t"), Union{Int, Missing}))

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Int)
@test v === 10

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = IOBuffer("0\r")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = IOBuffer("0a\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Int)

io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = IOBuffer("0,")
v = CSV.parsefield(io,Int)
@test v === 0

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = IOBuffer("\r")
@test_throws Missings.MissingException CSV.parsefield(io,Int)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = IOBuffer("\"\"")
@test_throws Missings.MissingException CSV.parsefield(io,Int)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Int, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"\\N\"")
@test_throws Missings.MissingException CSV.parsefield(io,Int,CSV.Options(missingstring="\\N"))

end # @testset "Int"

@testset "Int Custom IO" begin
# Int64 Libz
io = Buffer(IOBuffer("0"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = Buffer(IOBuffer("-1"))
v = CSV.parsefield(io,Int)
@test v === -1

io = Buffer(IOBuffer("1"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 1

io = Buffer(IOBuffer("2000"))
v = CSV.parsefield(io,Int)
@test v === 2000

io = Buffer(IOBuffer("0.0"))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Missing})
io = Buffer(IOBuffer("0a"))
@test_throws CSV.ParsingException CSV.parsefield(io,Int)
io = Buffer(IOBuffer(""))
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(" "))
@test_throws Missings.MissingException CSV.parsefield(io,Int)

io = Buffer(IOBuffer("\t"))
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(" \t 010"))
v = CSV.parsefield(io,Int)
@test v === 10

io = Buffer(IOBuffer("\"1_00a0\""))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Int, Missing})
io = Buffer(IOBuffer("\"0\""))
v = CSV.parsefield(io,Int)
@test v === 0

io = Buffer(IOBuffer("0\n"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = Buffer(IOBuffer("0\r"))
v = CSV.parsefield(io,Int)
@test v === 0

io = Buffer(IOBuffer("0\r\n"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = Buffer(IOBuffer("0a\n"))
@test_throws CSV.ParsingException CSV.parsefield(io,Int)

io = Buffer(IOBuffer("\t0\t\n"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = Buffer(IOBuffer("0,"))
v = CSV.parsefield(io,Int)
@test v === 0

io = Buffer(IOBuffer("0,\n"))
v = CSV.parsefield(io, Union{Int, Missing})
@test v === 0

io = Buffer(IOBuffer("\n"))
@test_throws Missings.MissingException CSV.parsefield(io,Int)

io = Buffer(IOBuffer("\r"))
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r\n"))
@test_throws Missings.MissingException CSV.parsefield(io,Int)

io = Buffer(IOBuffer("\"\""))
v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("1234567890"))
v = CSV.parsefield(io,Int)
@test v === 1234567890

io = Buffer(IOBuffer("\\N"))
@test_throws Missings.MissingException CSV.parsefield(io,Int,CSV.Options(missingstring="\\N"))

io = Buffer(IOBuffer("\"\\N\""))
v = CSV.parsefield(io, Union{Int, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)
end # @testset "Int Custom IO"

@testset "Float64 Custom IO" begin

# Float64 Libz
io = Buffer(IOBuffer("1"))
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = Buffer(IOBuffer("-1.0"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === -1.0

io = Buffer(IOBuffer("0"))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = Buffer(IOBuffer("2000"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 2000.0

io = Buffer(IOBuffer("0a"))
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
io = Buffer(IOBuffer(""))
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(" "))
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = Buffer(IOBuffer("\t"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(" \t 010"))
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = Buffer(IOBuffer("\"1_00a0\""))
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Missing})
io = Buffer(IOBuffer("\"0\""))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = Buffer(IOBuffer("0\n"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = Buffer(IOBuffer("0\r"))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = Buffer(IOBuffer("0\r\n"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = Buffer(IOBuffer("0a\n"))
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)

io = Buffer(IOBuffer("\t0\t\n"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = Buffer(IOBuffer("0,"))
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = Buffer(IOBuffer("0,\n"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = Buffer(IOBuffer("\n"))
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = Buffer(IOBuffer("\r"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r\n"))
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = Buffer(IOBuffer("\"\""))
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("1234567890"))
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = Buffer(IOBuffer(".1234567890"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === .1234567890

io = Buffer(IOBuffer("0.1234567890"))
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = Buffer(IOBuffer("\"0.1234567890\""))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === .1234567890

io = Buffer(IOBuffer("nan"))
v = CSV.parsefield(io,Float64)
@test v === NaN

io = Buffer(IOBuffer("NaN"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === NaN

io = Buffer(IOBuffer("inf"))
v = CSV.parsefield(io,Float64)
@test v === Inf

io = Buffer(IOBuffer("infinity"))
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === Inf

io = Buffer(IOBuffer("\\N"))
@test_throws Missings.MissingException CSV.parsefield(io,Float64,CSV.Options(missingstring="\\N"))

io = Buffer(IOBuffer("\"\\N\""))
v = CSV.parsefield(io, Union{Float64, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

end # @testset "Float64 Custom IO"

@testset "Float64" begin

# Float64
io = IOBuffer("1")
v = CSV.parsefield(io,Float64)
@test v === 1.0

io = IOBuffer("-1.0")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === -1.0

io = IOBuffer("0")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("2000")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 2000.0

io = IOBuffer("0a")
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)
io = IOBuffer("")
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = IOBuffer(" ")
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Float64)
@test v === 10.0

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.ParsingException CSV.parsefield(io, Union{Float64, Missing})
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = IOBuffer("0\r")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = IOBuffer("0a\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Float64)

io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = IOBuffer("0,")
v = CSV.parsefield(io,Float64)
@test v === 0.0

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 0.0

io = IOBuffer("\n")
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = IOBuffer("\r\n")
@test_throws Missings.MissingException CSV.parsefield(io,Float64)

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Float64)
@test v === 1234567890.0

io = IOBuffer(".1234567890")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === .1234567890

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Float64)
@test v === .1234567890

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === .1234567890

io = IOBuffer("-.1")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === -0.1

io = IOBuffer("1e4")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 1e4

io = IOBuffer("1e-4")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 1e-4

io = IOBuffer("1.1e4")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === 1.1e4

io = IOBuffer(".1E4")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === .1E4

io = IOBuffer("nan")
v = CSV.parsefield(io,Float64)
@test v === NaN

io = IOBuffer("NaN")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === NaN

io = IOBuffer("NaN,")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === NaN

io = IOBuffer("inf")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = IOBuffer("inf,")
v = CSV.parsefield(io,Float64)
@test v === Inf

io = IOBuffer("infinity")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === Inf

io = IOBuffer("infinity,")
v = CSV.parsefield(io, Union{Float64, Missing})
@test v === Inf

io = IOBuffer("\\N")
@test_throws Missings.MissingException CSV.parsefield(io,Float64,CSV.Options(missingstring="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{Float64, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

end # @testset "Float64"

@testset "DecFP Custom IO" begin

# DecFP Libz
io = Buffer(IOBuffer("1"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = Buffer(IOBuffer("-1.0"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v == Dec64(-1.0)

io = Buffer(IOBuffer("0"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = Buffer(IOBuffer("2000"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(2000.0)

io = Buffer(IOBuffer("0a"))
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = Buffer(IOBuffer(""))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(" "))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = Buffer(IOBuffer("\t"))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})

io = Buffer(IOBuffer(" \t 010"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = Buffer(IOBuffer("\"1_00a0\""))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})
io = Buffer(IOBuffer("\"0\""))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = Buffer(IOBuffer("0\n"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = Buffer(IOBuffer("0\r"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(0.0)

io = Buffer(IOBuffer("0\r\n"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = Buffer(IOBuffer("0a\n"))
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})
# Should we handle trailing whitespace?
io = Buffer(IOBuffer("\t0\t\n"))
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = Buffer(IOBuffer("0,"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(0.0)

io = Buffer(IOBuffer("0,\n"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = Buffer(IOBuffer("\n"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r"))
@test_throws Missings.MissingException CSV.parsefield(io,Dec64)

io = Buffer(IOBuffer("\r\n"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\"\""))
@test_throws Missings.MissingException CSV.parsefield(io,Dec64)

io = Buffer(IOBuffer("1234567890"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(1234567890.0)

io = Buffer(IOBuffer(".1234567890"))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = Buffer(IOBuffer("0.1234567890"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v == Dec64(.1234567890)

io = Buffer(IOBuffer("\"0.1234567890\""))
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = Buffer(IOBuffer("nan"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(NaN)

io = Buffer(IOBuffer("NaN"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = Buffer(IOBuffer("inf"))
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(Inf)

io = Buffer(IOBuffer("infinity"))
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = Buffer(IOBuffer("\\N"))
v = CSV.parsefield(io, Union{Dec64, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = Buffer(IOBuffer("\"\\N\""))
@test_throws Missings.MissingException CSV.parsefield(io,Dec64,CSV.Options(missingstring="\\N"))

end # @testset "DecFP Custom IO"

@testset "DecFP" begin

# DecFP
io = IOBuffer("1")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1.0)

io = IOBuffer("-1.0")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v == Dec64(-1.0)

io = IOBuffer("0")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("2000")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(2000.0)

io = IOBuffer("0a")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
io = IOBuffer("")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = IOBuffer(" ")
@test_throws ArgumentError CSV.parsefield(io,Dec64)

io = IOBuffer("\t")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(10.0)

io = IOBuffer("\"1_00a0\"")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(0.0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(0.0)

io = IOBuffer("0a\n")
@test_throws ArgumentError CSV.parsefield(io,Dec64)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws ArgumentError CSV.parsefield(io, Union{Dec64, Missing})

io = IOBuffer("0,")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(0.0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(0.0)

io = IOBuffer("\n")
@test_throws Missings.MissingException CSV.parsefield(io,Dec64)

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = IOBuffer("\r\n")
@test_throws Missings.MissingException CSV.parsefield(io,Dec64)

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test ismissing(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(1234567890.0)

io = IOBuffer(".1234567890")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v == Dec64(.1234567890)

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Dec64)
@test v == Dec64(.1234567890)

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v == Dec64(.1234567890)

io = IOBuffer("nan")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(NaN)

io = IOBuffer("NaN")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(NaN)

io = IOBuffer("inf")
v = CSV.parsefield(io,Dec64)
@test v === Dec64(Inf)

io = IOBuffer("infinity")
v = CSV.parsefield(io, Union{Dec64, Missing})
@test v === Dec64(Inf)

io = IOBuffer("\\N")
@test_throws Missings.MissingException CSV.parsefield(io,Dec64,CSV.Options(missingstring="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{Dec64, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

end # @testset "DecFP"

@testset "WeakRefString" begin

# WeakRefString
io = IOBuffer("0")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "0"

io = IOBuffer("-1")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "-1"

io = IOBuffer("1")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "1"

io = IOBuffer("2000")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "2000"

io = IOBuffer("0.0")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "0.0"

io = IOBuffer("0a")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = IOBuffer("")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test ismissing(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " "

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "\t"

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == " \t 010"

io = IOBuffer("\"1_00a0\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "1_00a0"

io = IOBuffer("\"0\"")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "0"

io = IOBuffer("0\r")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0\r\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "0"

io = IOBuffer("0a\n")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0a"

io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "\t0\t"

io = IOBuffer("0,")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "0"

io = IOBuffer("0,\n")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "0"

io = IOBuffer("\n")
@test_throws Missings.MissingException CSV.parsefield(io,WeakRefString{UInt8})

io = IOBuffer("\r")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test ismissing(v)

io = IOBuffer("\r\n")
@test_throws Missings.MissingException CSV.parsefield(io,WeakRefString{UInt8})

io = IOBuffer("\"\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test ismissing(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "1234567890"

io = IOBuffer("\"hey there\\\"quoted field\\\"\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test v == "hey there\\\"quoted field\\\""

io = IOBuffer("\\N")
@test_throws Missings.MissingException CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(missingstring="\\N"))

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"NORTH DAKOTA STATE \"\"A\"\" #1\"")
v = CSV.parsefield(io, WeakRefString{UInt8}, CSV.Options(escapechar='"'))
@test v == "NORTH DAKOTA STATE \"\"A\"\" #1"

io = IOBuffer("\"NORTH DAKOTA STATE \"\"A\"\" #1\",")
v = CSV.parsefield(io, WeakRefString{UInt8}, CSV.Options(escapechar='"'))
@test v == "NORTH DAKOTA STATE \"\"A\"\" #1"

end # @testset "WeakRefString"

@testset "String Custom IO" begin

# Libz
io = Buffer(IOBuffer("0"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("-1"))
v = CSV.parsefield(io,String)
@test v == "-1"

io = Buffer(IOBuffer("1"))
v = CSV.parsefield(io,String)
@test v == "1"

io = Buffer(IOBuffer("2000"))
v = CSV.parsefield(io,String)
@test v == "2000"

io = Buffer(IOBuffer("0.0"))
v = CSV.parsefield(io,String)
@test v == "0.0"

io = Buffer(IOBuffer("0a"))
v = CSV.parsefield(io,String)
@test v == "0a"

io = Buffer(IOBuffer(""))
@test_throws Missings.MissingException CSV.parsefield(io,String)

io = Buffer(IOBuffer(" "))
v = CSV.parsefield(io,String)
@test v == " "

io = Buffer(IOBuffer("\t"))
v = CSV.parsefield(io,String)
@test v == "\t"

io = Buffer(IOBuffer(" \t 010"))
v = CSV.parsefield(io,String)
@test v == " \t 010"

io = Buffer(IOBuffer("\"1_00a0\""))
v = CSV.parsefield(io,String)
@test v == "1_00a0"

io = Buffer(IOBuffer("\"0\""))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("0\n"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("0\r"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("0\r\n"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("0a\n"))
v = CSV.parsefield(io,String)
@test v == "0a"

io = Buffer(IOBuffer("\t0\t\n"))
v = CSV.parsefield(io,String)
@test v == "\t0\t"

io = Buffer(IOBuffer("0,"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("0,\n"))
v = CSV.parsefield(io,String)
@test v == "0"

io = Buffer(IOBuffer("\n"))
@test_throws Missings.MissingException CSV.parsefield(io,String)

io = Buffer(IOBuffer("\r"))
v = CSV.parsefield(io, Union{String, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r\n"))
@test_throws Missings.MissingException CSV.parsefield(io,String)
@test ismissing(v)

io = Buffer(IOBuffer("\"\""))
v = CSV.parsefield(io, Union{String, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("1234567890"))
v = CSV.parsefield(io,String)
@test v == "1234567890"

io = Buffer(IOBuffer("\"hey there\\\"quoted field\\\"\""))
v = CSV.parsefield(io,String)
@test v == "hey there\\\"quoted field\\\""

io = Buffer(IOBuffer("\\N"))
@test_throws Missings.MissingException CSV.parsefield(io,String,CSV.Options(missingstring="\\N"))

io = Buffer(IOBuffer("\"\\N\""))
v = CSV.parsefield(io, Union{String, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = Buffer(IOBuffer("\"NORTH DAKOTA STATE \"\"A\"\" #1\""))
v = CSV.parsefield(io, String, CSV.Options(escapechar='"'))
@test v == "NORTH DAKOTA STATE \"\"A\"\" #1"

io = Buffer(IOBuffer("\"NORTH DAKOTA STATE \"\"A\"\" #1\","))
v = CSV.parsefield(io, String, CSV.Options(escapechar='"'))
@test v == "NORTH DAKOTA STATE \"\"A\"\" #1"

end # @testset "String Custom IO"

@testset "Date" begin
opt = CSV.Options(dateformat=Dates.ISODateFormat)
# Date
io = IOBuffer("")
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = IOBuffer(",")
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = IOBuffer("\r")
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = IOBuffer("\"\"")
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Date, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"\\N\"")
@test_throws Missings.MissingException CSV.parsefield(io,Date,CSV.Options(missingstring="\\N"))

io = IOBuffer("2015-10-05")
v = CSV.parsefield(io, Date, opt)
@test v === Date(2015,10,5)

io = IOBuffer("\"2015-10-05\"")
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05,")
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\n")
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\r")
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = IOBuffer("2015-10-05\r\n")
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = IOBuffer("  \"2015-10-05\",")
@test_throws ArgumentError CSV.parsefield(io,Date,opt)

io = IOBuffer("\"2015-10-05\"\n")
v = CSV.parsefield(io,Date,opt)
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

@testset "Date Custom IO" begin
opt = CSV.Options(dateformat=Dates.ISODateFormat)
# Date Libz
io = Buffer(IOBuffer(""))
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(","))
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = Buffer(IOBuffer("\n"))
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r"))
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = Buffer(IOBuffer("\r\n"))
v = CSV.parsefield(io, Union{Date, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\"\""))
@test_throws Missings.MissingException CSV.parsefield(io,Date)

io = Buffer(IOBuffer("\\N"))
v = CSV.parsefield(io, Union{Date, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = Buffer(IOBuffer("\"\\N\""))
@test_throws Missings.MissingException CSV.parsefield(io,Date,CSV.Options(missingstring="\\N"))

io = Buffer(IOBuffer("2015-10-05"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("\"2015-10-05\""))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("2015-10-05,"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("2015-10-05\n"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("2015-10-05\r"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("2015-10-05\r\n"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("  \"2015-10-05\","))
@test_throws ArgumentError CSV.parsefield(io,Date,opt)

io = Buffer(IOBuffer("\"2015-10-05\"\n"))
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

io = Buffer(IOBuffer("\"10/5/2015\"\n"))
v = CSV.parsefield(io,Date,CSV.Options(missingstring="",dateformat=Dates.DateFormat("mm/dd/yyyy")))
@test v === Date(2015,10,5)

end # @testset "Date Custom IO"

@testset "DateTime" begin
opt = CSV.Options(dateformat=Dates.ISODateTimeFormat)
# DateTime
io = IOBuffer("")
v = CSV.parsefield(io, Union{DateTime, Missing})
@test ismissing(v)

io = IOBuffer(",")
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{DateTime, Missing})

io = IOBuffer("\r")
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{DateTime, Missing})
@test ismissing(v)

io = IOBuffer("\"\"")
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{DateTime, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"\\N\"")
@test_throws Missings.MissingException CSV.parsefield(io,DateTime,CSV.Options(missingstring="\\N"))

io = IOBuffer("2015-10-05T00:00:01")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("\"2015-10-05T00:00:01\"")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01,")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\n")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\r")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("2015-10-05T00:00:01\r\n")
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = IOBuffer("  \"2015-10-05T00:00:01\",")
@test_throws ArgumentError CSV.parsefield(io,DateTime,opt)

io = IOBuffer("\"2015-10-05T00:00:01\"\n")
v = CSV.parsefield(io,DateTime,opt)
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

@testset "DateTime Custom IO" begin
opt = CSV.Options(dateformat=Dates.ISODateTimeFormat)
# DateTime Libz
io = Buffer(IOBuffer(""))
v = CSV.parsefield(io, Union{DateTime, Missing})
@test ismissing(v)

io = Buffer(IOBuffer(","))
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = Buffer(IOBuffer("\n"))
v = CSV.parsefield(io, Union{DateTime, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\r"))
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = Buffer(IOBuffer("\r\n"))
v = CSV.parsefield(io, Union{DateTime, Missing})
@test ismissing(v)

io = Buffer(IOBuffer("\"\""))
@test_throws Missings.MissingException CSV.parsefield(io,DateTime)

io = Buffer(IOBuffer("\\N"))
v = CSV.parsefield(io, Union{DateTime, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = Buffer(IOBuffer("\"\\N\""))
@test_throws Missings.MissingException CSV.parsefield(io,DateTime,CSV.Options(missingstring="\\N"))

io = Buffer(IOBuffer("2015-10-05T00:00:01"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("\"2015-10-05T00:00:01\""))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("2015-10-05T00:00:01,"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("2015-10-05T00:00:01\n"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("2015-10-05T00:00:01\r"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("2015-10-05T00:00:01\r\n"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("  \"2015-10-05T00:00:01\","))
@test_throws ArgumentError CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("\"2015-10-05T00:00:01\"\n"))
v = CSV.parsefield(io,DateTime,opt)
@test v === DateTime(2015,10,5,0,0,1)

io = Buffer(IOBuffer("\"10/5/2015 00:00:01\"\n"))
v = CSV.parsefield(io,DateTime,CSV.Options(dateformat=dateformat"mm/dd/yyyy HH:MM:SS"))
@test v === DateTime(2015,10,5,0,0,1)

end # @testset "DateTime Custom IO"

@testset "Char" begin

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
v = CSV.parsefield(io, Union{Char, Missing})
@test ismissing(v)

io = IOBuffer(" ")
@test_throws Missings.MissingException CSV.parsefield(io,Char)

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Char, Missing})
@test ismissing(v)

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

io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0,")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Char)
@test v === '0'

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Char, Missing})
@test ismissing(v)

io = IOBuffer("\r")
@test_throws Missings.MissingException CSV.parsefield(io,Char)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Char, Missing})
@test ismissing(v)

io = IOBuffer("\"\"")
@test_throws Missings.MissingException CSV.parsefield(io,Char)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Char, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"\\N\"")
@test_throws Missings.MissingException CSV.parsefield(io,Char,CSV.Options(missingstring="\\N"))

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Char, Missing}, CSV.Options(delim='\t'))
@test ismissing(v)

end # @testset "Char"

@testset "Bool" begin

io = IOBuffer("")
v = CSV.parsefield(io, Union{Bool, Missing})
@test ismissing(v)

io = IOBuffer(" ")
@test_throws Missings.MissingException CSV.parsefield(io,Bool)

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Bool, Missing})
@test ismissing(v)

io = IOBuffer("true\n")
v = CSV.parsefield(io,Bool)
@test v

io = IOBuffer("true\r")
v = CSV.parsefield(io,Bool)
@test v

io = IOBuffer("true\r\n")
v = CSV.parsefield(io,Bool)
@test v

io = IOBuffer("truea\n")
@test_throws CSV.ParsingException CSV.parsefield(io,Bool)

io = IOBuffer("\ttrue\t\n")
v = CSV.parsefield(io,Bool)
@test v

io = IOBuffer("true,")
v = CSV.parsefield(io,Bool)
@test v

io = IOBuffer("false,\n")
v = CSV.parsefield(io,Bool)
@test !v

io = IOBuffer("\n")
v = CSV.parsefield(io, Union{Bool, Missing})
@test ismissing(v)

io = IOBuffer("\r")
@test_throws Missings.MissingException CSV.parsefield(io,Bool)

io = IOBuffer("\r\n")
v = CSV.parsefield(io, Union{Bool, Missing})
@test ismissing(v)

io = IOBuffer("\"\"")
@test_throws Missings.MissingException CSV.parsefield(io,Bool)

io = IOBuffer("\\N")
v = CSV.parsefield(io, Union{Bool, Missing}, CSV.Options(missingstring="\\N"))
@test ismissing(v)

io = IOBuffer("\"\\N\"")
@test_throws Missings.MissingException CSV.parsefield(io,Bool,CSV.Options(missingstring="\\N"))

io = IOBuffer("\t")
v = CSV.parsefield(io, Union{Bool, Missing}, CSV.Options(delim='\t'))
@test ismissing(v)

io = IOBuffer("T")
v = CSV.parsefield(io, Bool, CSV.Options(truestring="T"))
@test v

io = IOBuffer("T,")
v = CSV.parsefield(io, Bool, CSV.Options(truestring="T"))
@test v

io = IOBuffer("T\n")
v = CSV.parsefield(io, Bool, CSV.Options(truestring="T"))
@test v

io = IOBuffer("F")
v = CSV.parsefield(io, Bool, CSV.Options(falsestring="F"))
@test !v

io = IOBuffer("F,")
v = CSV.parsefield(io, Bool, CSV.Options(falsestring="F"))
@test !v

io = IOBuffer("F\n")
v = CSV.parsefield(io, Bool, CSV.Options(falsestring="F"))
@test !v

end # @testset "Bool"

@testset "All types" begin
opt = CSV.Options(dateformat=Dates.ISODateFormat)
# All types
io = IOBuffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io, Union{Date, Missing},opt)
@test ismissing(v)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io, Union{Float64, Missing})
@test ismissing(v)
v = CSV.parsefield(io,WeakRefString{UInt8})
@test v == "hey there sailor"
v = CSV.parsefield(io,Date,opt)
@test v === Date(2015,10,5)

v = CSV.parsefield(io,Int)
@test v === 1
v = CSV.parsefield(io,Float64)
@test v === 1.0
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test ismissing(v)
@test_throws Missings.MissingException CSV.parsefield(io,Date,opt)

v = CSV.parsefield(io, Union{Int, Missing})
@test ismissing(v)
@test_throws Missings.MissingException CSV.parsefield(io,Float64)
v = CSV.parsefield(io, Union{WeakRefString{UInt8}, Missing})
@test ismissing(v)

end # @testset "All types"

# Custom type
struct CustomInt
    val::Int
end
Base.parse(::Type{CustomInt}, x) = CustomInt(parse(Int, x))

io = Buffer(IOBuffer("1"))
v = CSV.parsefield(io, CustomInt, CSV.Options())
@test v.val == 1
