using Test, CSV, Dates, Tables, WeakRefStrings, CategoricalArrays, DataFrames, PooledArrays

const dir = joinpath(dirname(pathof(CSV)), "../test/testfiles")

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

include("basics.jl")
include("files.jl")
include("iteration.jl")
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
    f = CSV.read(IOBuffer("X\nb\nc\na\nc"), types=[CategoricalString{UInt32}])
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["a", "b", "c"]
    
    f = CSV.read(IOBuffer("X\nb\nc\na\nc"), categorical=true)
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["a", "b", "c"]

    f = CSV.read(IOBuffer("X\nb\nc\n\nc"), categorical=true)
    v = f.X[1]
    @test v == "b"
    @test levels(v.pool) == ["b", "c"]
    @test typeof(f.X) == CategoricalArray{Union{Missing, String},1,UInt32,String,CategoricalString{UInt32},Missing}
end

@testset "PooledArrays" begin

df = CSV.read(IOBuffer("X\nb\nc\na\nc"), pool=true)
@test typeof(df.X) == PooledArrays.PooledArray{String,UInt32,1,Array{UInt32,1}}
@test size(df) == (4, 1)
@test df.X == ["b", "c", "a", "c"]
@test df.X.refs[2] == df.X.refs[4]

df = CSV.read(IOBuffer("X\nb\nc\na\nc"), pool=0.5)
@test typeof(df.X) == PooledArrays.PooledArray{String,UInt32,1,Array{UInt32,1}}
@test size(df) == (4, 1)
@test df.X == ["b", "c", "a", "c"]
@test df.X.refs[2] == df.X.refs[4]

df = CSV.read(IOBuffer("X\nb\nc\n\nc"), pool=true)
@test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
@test size(df) == (4, 1)
@test df.X[3] === missing

df = CSV.read(IOBuffer("X\nc\nc\n\nc\nc\nc\nc"))
@test typeof(df.X) == PooledArray{Union{Missing, String},UInt32,1,Array{UInt32,1}}
@test size(df) == (7, 1)
@test isequal(df.X, ["c", "c", missing, "c", "c", "c", "c"])

end

end
