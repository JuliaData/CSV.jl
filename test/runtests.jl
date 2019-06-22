using Test, CSV, Dates, Tables, DataFrames, CategoricalArrays, PooledArrays

const dir = joinpath(dirname(pathof(CSV)), "../test/testfiles")

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

@testset "transform" begin
    csv = """A,B,C
    1,1.0,"hey"
    ,2.0,"there"
    3,3.0,"sailor"
    """

    df = CSV.read(IOBuffer(csv)) |> Tables.transform(Dict{String, Base.Callable}("C" => Symbol)) |> DataFrame
    @test df.C == [:hey, :there, :sailor]

    df = CSV.read(IOBuffer(csv)) |> Tables.transform(Dict("A"=>x->x+1)) |> DataFrame
    @test isequal(df.A, [2, missing, 4])
    @test typeof(df.A) == Vector{Union{Missing, Int64}}
    @test size(df) == (3, 3)

    df = CSV.read(IOBuffer(csv)) |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0))) |> DataFrame
    @test df.A == [2, 0, 4]
    @test eltype(df.A) <: Signed

    df = CSV.read(IOBuffer(csv)) |> Tables.transform(Dict("A"=>x->coalesce(x+1, 0.0))) |> DataFrame
    @test df.A == [2, 0.0, 4]
    @test eltype(df.A) <: Real

    df = CSV.read(IOBuffer(csv)) |> Tables.transform(Dict(2=>x->x==2.0 ? missing : x)) |> DataFrame
    @test size(df) == (3, 3)
    @test isequal(df.B, [1.0, missing, 3.0])
    @test typeof(df.B) == Vector{Union{Float64, Missing}}

    f = CSV.File(IOBuffer(csv))

    # transforms on CSV.Columns
    df = f |> Tables.transform(A=x->x+1) |> DataFrame
    @test size(df) == (3, 3)
    df = f |> Tables.transform(A=x->x+1) |> columntable
    @test length(df) == 3
    df = f |> Tables.transform(A=x->x+1) |> rowtable
    @test length(df) == 3

    # transforms on CSV.RowIterator
    f = CSV.File(joinpath(dir, "pandas_zeros.csv"));
    df = f |> Tables.transform(_0=x->x+1) |> DataFrame;
    @test size(df) == (100000, 50)
    df = f |> Tables.transform(_0=x->x+1) |> columntable;
    @test length(df) == 50
    # df = f |> Tables.transform(_0=x->x+1) |> rowtable;
    # @test length(df) == 100000
end

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

    df = CSV.read(IOBuffer("X\nb\nc\na\nc"), pool=0.5, copycols=true)
    @test typeof(df.X) == PooledArrays.PooledArray{String,UInt32,1,Array{UInt32,1}}
    @test size(df) == (4, 1)
    @test df.X == ["b", "c", "a", "c"]
    @test df.X.refs[2] == df.X.refs[4]

    df = CSV.read(IOBuffer("X\nb\nc\n\nc"), pool=true, copycols=true)
    @test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
    @test size(df) == (4, 1)
    @test df.X[3] === missing

    df = CSV.read(IOBuffer("X\nc\nc\n\nc\nc\nc\nc"), copycols=true)
    @test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
    @test size(df) == (7, 1)
    @test isequal(df.X, ["c", "c", missing, "c", "c", "c", "c"])

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
    @test rows[1] == ["1", "2", "3"]

    # 448
    @test_throws ArgumentError CSV.Rows(IOBuffer("x\n1\n2\n3\n#4"), ignorerepeated=true)

    # 447
    rows = collect(CSV.Rows(IOBuffer("a,b,c\n1,2,3\n\n"), ignoreemptylines=true))
    @test length(rows) == 1
    @test rows[1] == ["1", "2", "3"]

    # not enough columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1\n2,3\n4,5,6")))
    @test length(rows) == 3
    @test isequal(rows[1], ["1", missing, missing])
    @test isequal(rows[2], ["2", "3", missing])
    @test rows[3] == ["4", "5", "6"]

    # too many columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1,2,3\n4,5,6,7,8,")))
    @test length(rows) == 2
    @test rows[1] == ["1", "2", "3"]
    @test rows[2] == ["4", "5", "6"]

    # fatal
    @test_throws CSV.Error collect(CSV.Rows(IOBuffer("x\n\"invalid quoted field")))

    # reusebuffer
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3")))
    @test length(rows) == 3
    @test rows[1] == ["1"]
    @test rows[2] == ["2"]
    @test rows[3] == ["3"]
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
    @test length(rows) == 3
    @test rows[1] == ["3"]
    @test rows[2] == ["3"]
    @test rows[3] == ["3"]

    for (i, row) in enumerate(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
        @test row == [string(i)]
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

end
