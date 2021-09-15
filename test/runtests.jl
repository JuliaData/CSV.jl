using Test, CSV, Mmap, Dates, Tables, PooledArrays, CodecZlib, FilePathsBase, SentinelArrays, Parsers, WeakRefStrings

const dir = joinpath(dirname(pathof(CSV)), "..", "test", "testfiles")

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

@testset "CSV.File" begin

include("basics.jl")
include("testfiles.jl")
include("iteration.jl")

end # @testset "CSV.File"

include("write.jl")

@testset "PooledArrays" begin

    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), pool=true)
    @test typeof(f.X) == PooledArrays.PooledArray{InlineString1,UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X == ["b", "c", "a", "c"]
    @test f.X.refs[2] == f.X.refs[4]

    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), pool=0.75)
    @test typeof(f.X) == PooledArrays.PooledArray{InlineString1,UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X == ["b", "c", "a", "c"]
    @test f.X.refs[2] == f.X.refs[4]

    f = CSV.File(IOBuffer("X\nb\nc\n\nc"), pool=true, ignoreemptyrows=false)
    @test typeof(f.X) == PooledArray{Union{Missing, InlineString1},UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X[3] === missing

    f = CSV.File(IOBuffer("X\nc\nc\n\nc\nc\nc\nc\nc\nc"), ignoreemptyrows=false)
    @test typeof(f.X) == PooledArray{Union{Missing, InlineString1},UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (9, 1)
    @test isequal(f.X, ["c", "c", missing, "c", "c", "c", "c", "c", "c"])

end

@testset "CSV.Rows" begin

    rows = CSV.Rows(IOBuffer("X\nb\nc\na\nc"))
    show(rows)
    row = first(rows)
    @test row.X == "b"
    @test row[1] == "b"

    # limit
    rows = collect(CSV.Rows(IOBuffer("X\nb\nc\na\nc"), limit=1))
    @test length(rows) == 1
    @test rows[1][1] == "b"
    rows = collect(CSV.Rows(IOBuffer("X\nb\nc\na\nc"), limit=0))
    @test length(rows) == 0

    # transpose
    rows = collect(CSV.Rows(IOBuffer("x,1\nx2,2\n"), transpose=true))
    @test length(rows) == 1
    @test rows[1].x == "1"
    @test rows[1].x2 == "2"

    # ignorerepeated
    rows = collect(CSV.Rows(IOBuffer("x   y   z\n   1   2   3"), ignorerepeated=true, delim=' '))
    @test length(rows) == 1
    @test length(rows[1]) == 3
    @test all(rows[1] .== ["1", "2", "3"])
    row = rows[1]
    @test CSV.Parsers.parse(Int, row, 1) == 1
    @test CSV.Parsers.parse(Int, row, :x) == 1
    @test CSV.detect(row, 2) == 2
    @test CSV.detect(row, :y) == 2

    # 448
    @test_throws ArgumentError CSV.Rows(IOBuffer("x\n1\n2\n3\n#4"), ignorerepeated=true)

    # 447
    rows = collect(CSV.Rows(IOBuffer("a,b,c\n1,2,3\n\n")))
    @test length(rows) == 1
    @test all(rows[1] .== ["1", "2", "3"])

    # not enough columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1\n2,3\n4,5,6")))
    @test length(rows) == 3
    @test all(isequal.(rows[1], ["1", missing, missing]))
    @test all(isequal.(rows[2], ["2", "3", missing]))
    @test all(rows[3] .== ["4", "5", "6"])

    # too many columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1,2,3\n4,5,6,7,8,")))
    @test length(rows) == 2
    @test isequal(values(rows[1]), ["1", "2", "3", missing, missing])
    @test isequal(values(rows[2]), ["4", "5", "6", "7", "8"])

    # fatal
    @test_throws CSV.Error collect(CSV.Rows(IOBuffer("x\n\"invalid quoted field")))

    # reusebuffer
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3")))
    @test length(rows) == 3
    @test rows[1][1] == "1"
    @test rows[2][1] == "2"
    @test rows[3][1] == "3"
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
    @test length(rows) == 3
    @test rows[1][1] == "3"
    @test rows[2][1] == "3"
    @test rows[3][1] == "3"

    for (i, row) in enumerate(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
        @test all(row .== [string(i)])
    end

    # 903
    rows = collect(CSV.Rows(IOBuffer("int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n,,,,,,,,\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"); types=[Int, Float64, Date, DateTime, Bool, Missing, String, String, Float64]))
    @test isequal(collect(rows[1]), [1, 3.14, Date(2019, 1, 1), DateTime(2019, 1, 1, 1, 2, 3), true, missing, "hey", "abc", 2.0])
    foreach(x -> @test(x === missing), rows[2])
    @test isequal(collect(rows[3]), [2, NaN, Date(2019, 1, 2), DateTime(2019, 1, 3, 1, 2, 3), false, missing, "there", "abc", 3.14])
end

@testset "CSV.detect" begin

@test CSV.detect("") == ""
@test CSV.detect("1") == 1
@test CSV.detect("1.1") == 1.1
@test CSV.detect("2015-01-01") == Date(2015)
@test CSV.detect("2015-01-01T03:04:05") == DateTime(2015, 1, 1, 3, 4, 5)
@test CSV.detect("03:04:05") == Time(3, 4, 5)
@test CSV.detect("true") === true
@test CSV.detect("false") === false
@test CSV.detect("abc") === "abc"

end

@testset "CSV.promote_types" begin

@test CSV.promote_types(Int64, Float64) == Float64
@test CSV.promote_types(Int64, Int64) == Int64
@test CSV.promote_types(CSV.NeedsTypeDetection, Float64) == Float64
@test CSV.promote_types(Float64, CSV.NeedsTypeDetection) == Float64
@test CSV.promote_types(Float64, Missing) == Float64
@test CSV.promote_types(Float64, String) === String

end

@testset "CSV.File with select/drop" begin

csv = """
a,b,c,d,e
1,2,3,4,5
6,7,8,9,10
"""

f = CSV.File(IOBuffer(csv), select=[1, 3, 5])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=[:a, :c, :e])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=["a", "c", "e"])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=[true, false, true, false, true])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=(i, nm) -> i in (1, 3, 5))
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=Int[])
@test length(f) == 2
@test length(f[1]) == 0

f = CSV.File(IOBuffer(csv), select=[1, 2, 3, 4, 5])
@test length(f) == 2
@test length(f[1]) == 5

f = CSV.File(IOBuffer(csv), drop=[2, 4])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=[:b, :d])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=["b", "d"])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=[false, true, false, true, false])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=(i, nm) -> i in (2, 4))
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=Int[])
@test length(f) == 2
@test length(f[1]) == 5

f = CSV.File(IOBuffer(csv), drop=[1, 2, 3, 4, 5])
@test length(f) == 2
@test length(f[1]) == 0

end

@testset "CSV.Chunks" begin

chunks = CSV.Chunks(transcode(GzipDecompressor, Mmap.mmap(joinpath(dir, "randoms.csv.gz"))); ntasks=2)
@test length(chunks) == 2
state = iterate(chunks)
f, st = state
@test length(f) == 35086
state = iterate(chunks, st)
f, st = state
@test length(f) == 34914

end

@testset "CSV.File vector inputs" begin

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a,b,c\n7,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

f = CSV.File(map(IOBuffer, data))
@test length(f) == 6
@test f.names == [:a, :b, :c]
@test f.a == [1, 4, 7, 10, 13, 16]

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a,b,c\n7.14,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

@test_throws ArgumentError CSV.File(map(IOBuffer, data))

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a2,b,c\n7,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

@test_throws ArgumentError CSV.File(map(IOBuffer, data))

end

end
