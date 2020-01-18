using DataFrames, CSV, BenchmarkTools

function test(n)
    df = DataFrame(x = rand(n), y = rand(n), z = rand(n))
    @benchmark CSV.write("/tmp/tom.csv", $df)
end

for i in 4:7
    rows = 10^i
    println(rows, " rows: ", test(rows))
end

