using Test, Aqua, CSV

@testset "package quality" begin
    Aqua.test_all(CSV)
end
