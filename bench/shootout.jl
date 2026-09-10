# Cross-engine reader shootout, CSV.jl side. Generates the shape files once,
# times CSV.File(path) best-of-N, then runs bench/shootout.py for the others.
#   julia --project=test -t8 bench/shootout.jl <datadir> [sizeMiB] [reps]
using CSV, Tables, Printf
include(joinpath(@__DIR__, "bench_matrix.jl"))
datadir = isempty(ARGS) ? mktempdir() : ARGS[1]
mb = length(ARGS) >= 2 ? parse(Float64, ARGS[2]) : 200.0
reps = length(ARGS) >= 3 ? parse(Int, ARGS[3]) : 5
mkpath(datadir)
shapes = (:numeric, :mixed, :strings, :temporal, :quoted, :wide)
for shape in shapes
    path = joinpath(datadir, "$shape.csv")
    isfile(path) || write(path, makedata(shape, round(Int, mb * 2^20)))
end
out = open(joinpath(datadir, "shootout.tsv"), "a")
for nt in (Threads.nthreads(), 1)
    for shape in shapes
        path = joinpath(datadir, "$shape.csv")
        nbytes = filesize(path)
        f = () -> CSV.File(path; ntasks=nt, on_error=:collect)
        f()
        t = minimum((GC.gc(); @elapsed f()) for _ in 1:reps)
        @printf("%-8s %-12s %dT %9.1f ms %8.0f MiB/s\n", "CSV.jl", shape, nt, t * 1e3, nbytes / 2^20 / t)
        println(out, join(("CSV.jl", shape, nt, round(t * 1e3, digits=2), round(nbytes / 2^20 / t, digits=1)), '\t'))
        flush(out); flush(stdout)
    end
end
close(out)
py = joinpath(@__DIR__, "shootout.py")
for nt in (Threads.nthreads(), 1)
    run(`python3 $py $datadir $nt $reps`)
end
println("results: ", joinpath(datadir, "shootout.tsv"))
