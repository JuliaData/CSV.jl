using Test, CSV, Dates, Tables, DataFrames, CategoricalArrays, PooledArrays, CodecZlib, FilePathsBase

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

@testset "CategoricalArray levels (including ordering)" begin
    f = CSV.read(IOBuffer("X\nb\nc\na\nc"), types=[CategoricalString{UInt32}], copycols=true)
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["a", "b", "c"]

    f = CSV.read(IOBuffer("X\nb\nc\na\nc"), categorical=true, copycols=true)
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["a", "b", "c"]

    f = CSV.read(IOBuffer("X\nb\nc\n\nc"), categorical=true, copycols=true)
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["b", "c"]
    @test typeof(f.X) == CategoricalArray{Union{Missing, String},1,UInt32,String,CategoricalString{UInt32},Missing}

end

@testset "PooledArrays" begin

    df = CSV.read(IOBuffer("X\nb\nc\na\nc"), pool=true, copycols=true)
    @test typeof(df.X) == PooledArrays.PooledArray{String,UInt32,1,Array{UInt32,1}}
    @test size(df) == (4, 1)
    @test df.X == ["b", "c", "a", "c"]
    @test df.X.refs[2] == df.X.refs[4]

    df = CSV.read(IOBuffer("X\nb\nc\na\nc"), pool=0.6, copycols=true)
    @test typeof(df.X) == PooledArrays.PooledArray{String,UInt32,1,Array{UInt32,1}}
    @test size(df) == (4, 1)
    @test df.X == ["b", "c", "a", "c"]
    @test df.X.refs[2] == df.X.refs[4]

    df = CSV.read(IOBuffer("X\nb\nc\n\nc"), pool=true, copycols=true)
    @test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
    @test size(df) == (4, 1)
    @test df.X[3] === missing

    df = CSV.read(IOBuffer("X\nc\nc\n\nc\nc\nc\nc\nc\nc"), copycols=true)
    @test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
    @test size(df) == (9, 1)
    @test isequal(df.X, ["c", "c", missing, "c", "c", "c", "c", "c", "c"])

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
    rows = collect(CSV.Rows(IOBuffer("a,b,c\n1,2,3\n\n"), ignoreemptylines=true))
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
    @test all(rows[1] .== ["1", "2", "3"])
    @test all(rows[2] .== ["4", "5", "6"])

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

end

@testset "CSV.detect" begin

@test CSV.detect("") == ""
@test CSV.detect("1") == 1
@test CSV.detect("1.1") == 1.1
@test CSV.detect("2015-01-01") == Date(2015)
@test CSV.detect("2015-01-01T03:04:05") == DateTime(2015, 1, 1, 3, 4, 5)
@test CSV.detect("true") === true
@test CSV.detect("false") === false
@test CSV.detect("abc") === "abc"

end

@testset "CSV.findrowstarts!" begin

rngs = [1, 1, 1]
buf = b"normal cell,next cell\nnormal cell2,next cell2\nhey"
CSV.findrowstarts!(buf, length(buf), CSV.Parsers.XOPTIONS, nothing, false, rngs, 2)
@test rngs[2] == 23

rngs = [1, 1, 1]
buf = b"quoted, cell\",next cell\n\"normal cell2\",next cell2\nhey"
CSV.findrowstarts!(buf, length(buf), CSV.Parsers.XOPTIONS, nothing, false, rngs, 2)
@test rngs[2] == 25

rngs = [1, 2, 1]
buf = b"\"\"quoted, cell\",next cell\n\"normal cell2\",next cell2\nhey"
CSV.findrowstarts!(buf, length(buf), CSV.Parsers.XOPTIONS, nothing, false, rngs, 2)
@test rngs[2] == 27

rngs = [1, 2, 1]
buf = b"quoted,\"\" cell\",next cell\n\"normal cell2\",next cell2\nhey"
CSV.findrowstarts!(buf, length(buf), CSV.Parsers.XOPTIONS, nothing, false, rngs, 2)
@test rngs[2] == 27

end

@testset "CSV.promote_typecode" begin

@test CSV.promote_typecode(CSV.INT | CSV.MISSING, CSV.FLOAT) == (CSV.FLOAT | CSV.MISSING)
@test CSV.promote_typecode(CSV.INT | CSV.MISSING, CSV.FLOAT | CSV.MISSING) == (CSV.FLOAT | CSV.MISSING)

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

end
