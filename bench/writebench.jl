# Writer throughput matrix for CSV.write and optional Polars `write_csv`,
# across shapes and sizes.
#
# Run:  julia --project=test -t8 bench/writebench.jl [rows...]
#       (default rows: 100_000 1_000_000)
using CSV, Random, Dates, Printf
const NROWS = isempty(ARGS) ? [100_000, 1_000_000] : parse.(Int, ARGS)

function shape(kind::Symbol, n::Int, rng)
    kind === :numeric   && return (a = rand(rng, Int64, n), b = rand(rng, n), c = rand(rng, Int32, n),
                                   d = rand(rng, n) .* 1e6, e = rand(rng, 1:1000, n))
    kind === :mixed     && return (id = collect(1:n), region = [rand(rng, ("north","south","east","west")) for _ in 1:n],
                                   price = rand(rng, n) .* 1000, qty = rand(rng, 1:1000, n),
                                   note = [rand(rng, Bool) ? "plain text" : "needs, quoting \"here\"" for _ in 1:n],
                                   flag = rand(rng, Bool, n), day = [Date(2020,1,1) + Day(i % 1000) for i in 1:n],
                                   maybe = [rand(rng) < 0.1 ? missing : rand(rng, Int32) for _ in 1:n])
    kind === :strings   && return (s1 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n],
                                   s2 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n],
                                   s3 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n])
    kind === :quoted    && return (s1 = ["v,$(i)" for i in 1:n], s2 = ["say \"hi\" $(i)" for i in 1:n],
                                   s3 = [String(rand(rng, 'a':'z', 8)) for _ in 1:n])
    kind === :wide      && return NamedTuple{Tuple(Symbol("c$i") for i in 1:60)}(Tuple(rand(rng, n) for _ in 1:60))
    kind === :datetime  && return (t = [DateTime(2020,1,1) + Second(i) for i in 1:n], d = [Date(2020,1,1) + Day(i % 3000) for i in 1:n],
                                   x = rand(rng, Int64, n))
    error("unknown shape")
end
function best(f; reps=5)
    f(); b = Inf
    for _ in 1:reps; t = @elapsed f(); b = min(b, t); end
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
