using DataFrames, CSV, BenchmarkTools

randnothing(pct_nothing) = let v = rand() 
    v < pct_nothing ? nothing : v
end

randnothing(n, pct) = [randnothing(pct) for _ in 1:n]

df1(n) = DataFrame(x = rand(n), y = rand(n), z = rand(n))

df2(n, pct = 0.1) = 
    DataFrame(x = randnothing(n, pct), y = randnothing(n, pct), z = randnothing(n, pct))

test1(df) = @benchmark CSV.write("/tmp/tom.csv", $df)

test2(df) = @benchmark CSV.write("/tmp/tom.csv", $df, transform = (col,val) -> something(val, missing))

function test(msg, f, df)
    println(msg)
    for i in 4:7
        rows = 10^i
        println(rows, " rows: ", f(df(rows)))
    end
end

length(ARGS) > 0 || (println("Usage: julia perf_write.jl [master|new]"); exit(1))
command = ARGS[1]

if command == "master"
    test("Testing without nothing values", test1, df1)
elseif command == "new"
    test("Testing without nothing values (default transform)", test1, df1)
    test("Testing without nothing values (to-missing transform)", test2, df1)
    test("Testing with nothing values (to-missing transform)", test2, df2)
else 
    error("Unknown command: $command")
end
