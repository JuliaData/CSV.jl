# Writer throughput matrix for CSV.write and optional Polars `write_csv`,
# across shapes and sizes.
#
# Run:  julia --project=test -t8 bench/writebench.jl [rows...]
#       (default rows: 100_000 1_000_000)
using CSV, Random, Dates, Printf
const NROWS = isempty(ARGS) ? [100_000, 1_000_000] : parse.(Int, ARGS)

include(joinpath(@__DIR__, "writeshapes.jl"))

function best(f; reps=5)
    f(); b = Inf
    for _ in 1:reps
        GC.gc()
        t = @elapsed f()
        b = min(b, t)
    end
    return b
end
haspolars = try; success(`python3 -c "import polars"`); catch; false; end
mib(x) = x / 2^20
println("threads=", Threads.nthreads(), "  polars=", haspolars ? "yes" : "no")
header = rpad("shape", 10) * lpad("rows", 10) * lpad("MiB", 8) *
         lpad("CSV.write", 11) * lpad("polars", 9) * lpad("CSV MB/s", 11) *
         lpad("CSV/polars", 12)
println(header)
println("─"^length(header))
for kind in (:numeric, :mixed, :strings, :quoted, :wide, :datetime), n in NROWS
    rng = MersenneTwister(7)
    tbl = shape(kind, n, rng)
    io = IOBuffer(); CSV.write(io, tbl); bytes = take!(io)
    tcsv = best(() -> CSV.write(IOBuffer(), tbl))
    tpol = NaN
    if haspolars
        path = tempname() * ".csv"; write(path, bytes)
        script = """
import polars as pl, time, io
df = pl.read_csv($(repr(path)), try_parse_dates=True)
b = 1e9
for _ in range(5):
    buf = io.BytesIO(); t = time.perf_counter(); df.write_csv(buf); b = min(b, time.perf_counter() - t)
print(b)
"""
        tpol = try parse(Float64, strip(read(`python3 -c $script`, String))) catch; NaN end
        rm(path; force=true)
    end
    @printf("%-10s%10d%8.1f%11.1f%9s%11.0f%12s\n", kind, n, mib(length(bytes)),
            tcsv * 1000, isnan(tpol) ? "—" : @sprintf("%.1f", tpol * 1000),
            length(bytes) / tcsv / 1e6,
            isnan(tpol) ? "—" : @sprintf("%.2fx", tpol / tcsv))
end
println("\ntimes in ms; ratios > 1 mean CSV.write is faster than Polars")
