# Writer battery: round-trips through CSVApi.File, byte determinism across
# thread counts, and byte agreement with CSV.write where semantics coincide.
using Test, Dates, Tables, CodecZlib
using CSV
isdefined(Main, :CSVApi) ||
    include(joinpath(@__DIR__, "api.jl"))
isdefined(Main, :KernelWrite) || include(joinpath(@__DIR__, "write.jl"))
const A = CSVApi
const W = KernelWrite

buf() = IOBuffer()
str(f) = (io = buf(); f(io); String(take!(io)))

@testset "KernelWrite" begin
    tbl = (a=[1, 2, 3], b=[1.5, missing, -2.0], c=["x", "y,z", "q\"r"],
           d=[Date(2024, 1, 2), Date(2024, 3, 4), Date(2024, 5, 6)])

    # round-trip: values survive File(write(table))
    out = str(io -> W.write(io, tbl))
    f = A.File(IOBuffer(out))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    @test isequal(Tables.getcolumn(f, :b), [1.5, missing, -2.0])
    @test String.(Tables.getcolumn(f, :c)) == ["x", "y,z", "q\"r"]
    @test Tables.getcolumn(f, :d) == tbl.d

    # byte agreement with CSV.write on plain content (both quote minimally)
    plain = (x=[1, 2], y=["ab", "c,d"], z=[1.25, -3.5])
    ours = str(io -> W.write(io, plain))
    theirs = str(io -> CSV.write(io, plain))
    @test ours == theirs

    # determinism across thread splits
    big = (n=collect(1:50_000), s=[string("v", i % 97) for i in 1:50_000])
    @test str(io -> W.write(io, big; ntasks=1)) == str(io -> W.write(io, big; ntasks=8))

    # quotestyle
    q = (s=["plain", "with,delim", "wi\"th"],)
    @test str(io -> W.write(io, q; quotestyle=:all)) ==
          "\"s\"\n\"plain\"\n\"with,delim\"\n\"wi\"\"th\"\n"
    @test str(io -> W.write(io, q; quotestyle=:minimal)) ==
          "s\nplain\n\"with,delim\"\n\"wi\"\"th\"\n"
    @test_throws ArgumentError str(io -> W.write(io, q; quotestyle=:none))
    @test str(io -> W.write(io, (s=["a", "b"],); quotestyle=:none)) == "s\na\nb\n"
    # leading/trailing whitespace quotes under :minimal (round-trip safety)
    ws = str(io -> W.write(io, (s=[" pad "],)))
    @test ws == "s\n\" pad \"\n"
    @test String(Tables.getcolumn(A.File(IOBuffer(ws)), :s)[1]) == " pad "

    # floatformat (issue #492 surface)
    ff = str(io -> W.write(io, (x=[1.23456, 2.0],); floatformat="%.2f"))
    @test ff == "x\n1.23\n2.00\n"

    # dateformat + decimal + missingstring + delim + newline
    s = str(io -> W.write(io, (d=[Date(2024, 1, 2)], x=[1.5], m=[missing]);
                          dateformat="dd/mm/yyyy", decimal=',', missingstring="NA",
                          delim=';', newline="\r\n"))
    @test s == "d;x;m\r\n02/01/2024;1,5;NA\r\n"

    # escapechar distinct from quotechar
    s = str(io -> W.write(io, (s=["a\"b"],); escapechar='\\'))
    @test s == "s\n\"a\\\"b\"\n"

    # append / writeheader
    io = buf()
    W.write(io, (a=[1],))
    W.write(io, (a=[2],); append=true)
    @test String(take!(io)) == "a\n1\n2\n"

    # bom
    s = str(io -> W.write(io, (a=[1],); bom=true))
    @test codeunits(s)[1:3] == UInt8[0xef, 0xbb, 0xbf]

    # gzip: by extension and explicitly; File auto-decompresses both
    dir = mktempdir()
    gzpath = joinpath(dir, "t.csv.gz")
    W.write(gzpath, tbl)
    f = A.File(gzpath)
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    raw = read(gzpath)
    @test raw[1] == 0x1f && raw[2] == 0x8b
    io = buf()
    W.write(io, tbl; compress=:gzip)
    f = A.File(take!(io))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]

    # partition: one sink per partition, parallel
    parts = Tables.partitioner([(a=[1, 2],), (a=[3, 4],)])
    p1, p2 = joinpath(dir, "p1.csv"), joinpath(dir, "p2.csv")
    W.write([p1, p2], parts; partition=true)
    @test Tables.getcolumn(A.File(p1), :a) == [1, 2]
    @test Tables.getcolumn(A.File(p2), :a) == [3, 4]

    # types beyond the basics: Bool, Int128, unicode
    s = str(io -> W.write(io, (b=[true, false], w=[Int128(2)^100, Int128(-1)], u=["αβ", "cd"])))
    f = A.File(IOBuffer(s))
    @test Tables.getcolumn(f, :b) == [true, false]
    @test Tables.getcolumn(f, :w) == [Int128(2)^100, Int128(-1)]
    @test String.(Tables.getcolumn(f, :u)) == ["αβ", "cd"]

    # header override + writeheader=false
    @test str(io -> W.write(io, (a=[1],); header=["renamed"])) == "renamed\n1\n"
    @test str(io -> W.write(io, (a=[1],); writeheader=false)) == "1\n"
end
println("WRITE BATTERY OK")
