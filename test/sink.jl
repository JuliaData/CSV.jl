@testset "Write to IOBuffer" begin
    csv_string = chomp(read(joinpath(dir, "test_basic.csv"), String))
    df = CSV.read(IOBuffer(csv_string))
    io = IOBuffer()
    CSV.write(io, df)
    written = chomp(String(take!(io)))
    @test written == csv_string
end
