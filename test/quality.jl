using Test
using Aqua
using CSV

@testset "package quality" begin
    Aqua.test_all(CSV)
end
