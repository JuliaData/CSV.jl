using Test, CSV, Dates, Tables, DataFrames

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

end
