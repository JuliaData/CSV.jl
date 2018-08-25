using Test, CSV, Dates, WeakRefStrings, CategoricalArrays

const dir = joinpath(dirname(pathof(CSV)), "../test/testfiles")

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

# include("unittests.jl")
include("files.jl")
# include("deprecated.jl")

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
end

end
