using DataFrames, CSV, BenchmarkTools

randnothing(pct_nothing) = let v = rand() 
    v < pct_nothing ? nothing : v
end

randnothing(n, pct_nothing) = [randnothing(pct_nothing) for _ in 1:n]

function test(n, pct_nothing)
    df = DataFrame(x = randnothing(n, pct_nothing), y = randnothing(n, pct_nothing), z = randnothing(n, pct_nothing))
    b1 = pct_nothing > 0 ? nothing : @benchmark CSV.write("/tmp/tom.csv", $df)
    b2 = @benchmark CSV.write("/tmp/tom.csv", $df, transform = (col,val) -> something(val, missing))
    return (default = b1, transform_to_missing = b2)
end

println("Performance test without any nothing values:")
for i in 4:7
    rows = 10^i
    println(rows, " rows: ", test(rows, 0))
end

println("Performance test with some nothing values:")
for i in 4:7
    rows = 10^i
    println(rows, " rows: ", test(rows, 0.1))
end
