using Test, CSV, Dates, Tables, WeakRefStrings, CategoricalArrays, DataFrames

const dir = joinpath(dirname(pathof(CSV)), "../test/testfiles")

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

include("files.jl")
include("iteration.jl")
include("write.jl")

@testset "CSV.validate" begin
    io = IOBuffer("""A,B,C
    1,1,10
    6,1""")

    @test_throws CSV.ExpectedMoreColumnsError CSV.validate(io)

    io = IOBuffer("""A;B;C
    1,1,10
    2,0,16""")
    @test_throws CSV.TooManyColumnsError CSV.validate(io)

    io = IOBuffer("""A;B;C
    1,1,10
    2,0,16""")
    @test_throws CSV.ExpectedMoreColumnsError CSV.validate(io; delim=';')

    io = IOBuffer("""a b c d e
    1 2  3 4 5
    1 2 3  4 5
    1  2 3  4 5""")
    @test_throws CSV.TooManyColumnsError CSV.validate(io; delim=' ')

    # 323
    CSV.validate(IOBuffer("a0001000\na0001000"), datarow=1, categorical=true)
end

@testset "transform" begin
    csv = """A,B,C
    1,1.0,"hey"
    ,2.0,"there"
    3,3.0,"sailor"
    """

    df = CSV.read(IOBuffer(csv), transforms=Dict{String, Base.Callable}("C" => Symbol))
    @test df.C == [:hey, :there, :sailor]

    df = CSV.read(IOBuffer(csv); transforms=Dict("A"=>x->x+1))
    @test isequal(df.A, [2, missing, 4])
    @test typeof(df.A) == Vector{Union{Missing, Int64}}
    @test size(df) == (3, 3)

    df = CSV.read(IOBuffer(csv); transforms=Dict("A"=>x->coalesce(x+1, 0)))
    @test df.A == [2, 0, 4]
    @test eltype(df.A) <: Signed

    df = CSV.read(IOBuffer(csv); transforms=Dict("A"=>x->coalesce(x+1, 0.0)))
    @test df.A == [2, 0.0, 4]
    @test eltype(df.A) <: Real

    df = CSV.read(IOBuffer(csv); transforms=Dict(2=>x->x==2.0 ? missing : x))
    @test size(df) == (3, 3)
    @test isequal(df.B, [1.0, missing, 3.0])
    @test typeof(df.B) == Vector{Union{Float64, Missing}}

    f = CSV.File(IOBuffer(csv); allowmissing=:auto)

    # transforms on CSV.Columns
    df = f |> transform(A=x->x+1) |> DataFrame
    @test size(df) == (3, 3)
    df = f |> transform(A=x->x+1) |> columntable
    @test length(df) == 3
    df = f |> transform(A=x->x+1) |> rowtable
    @test length(df) == 3

    # transforms on CSV.RowIterator
    f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none);
    df = f |> transform(_0=x->x+1) |> DataFrame;
    @test size(df) == (100000, 50)
    df = f |> transform(_0=x->x+1) |> columntable;
    @test length(df) == 50
    df = f |> transform(_0=x->x+1) |> rowtable;
    @test length(df) == 100000
end

@testset "CategoricalArray levels (including ordering)" begin
    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), types=[CategoricalString{UInt32}])
    v = iterate(f, 1)[1].X
    @test v == "b"
    @test levels(v.pool) == ["b"]
    v = iterate(f, 2)[1].X
    @test v == "c"
    @test levels(v.pool) == ["b", "c"]
    v = iterate(f, 3)[1].X
    @test v == "a"
    @test levels(v.pool) == ["a", "b", "c"]
    v = iterate(f, 4)[1].X
    @test v == "c"
    @test levels(v.pool) == ["a", "b", "c"]

    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), categorical=true)
    v = iterate(f, 1)[1].X
    @test v == "b"
    @test levels(v.pool) == ["a", "b", "c"]
    v = iterate(f, 2)[1].X
    @test v == "c"
    @test levels(v.pool) == ["a", "b", "c"]
    v = iterate(f, 3)[1].X
    @test v == "a"
    @test levels(v.pool) == ["a", "b", "c"]
    v = iterate(f, 4)[1].X
    @test v == "c"
    @test levels(v.pool) == ["a", "b", "c"]
end

include("deprecated.jl")

end
