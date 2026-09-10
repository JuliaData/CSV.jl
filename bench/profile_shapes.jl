# Flat profiles of CSV.File per shape (self-time ranking), to find hot spots.
# Run: julia --project=test -t1 profile_shapes.jl [shape...]
using CSV, Profile, Tables, Printf
include(joinpath(@__DIR__, "bench_matrix.jl"))
const MB = 20
shapes = isempty(ARGS) ? [:numeric, :mixed, :strings, :quoted, :escaped, :longtext, :wide,
                          :verywide, :temporal, :pooled_high, :crlf, :sentinel, :missing90, :bools] :
                         Symbol.(ARGS)
Profile.init(n=10_000_000, delay=0.0005)
for shape in shapes
    buf = makedata(shape, MB * 2^20)
    kw = shapeoptions(shape)
    f = () -> CSV.File(buf; on_error=:collect, kw...)
    f(); f()
    t = @elapsed f()
    reps = max(3, ceil(Int, 1.5 / t))
    Profile.clear()
    GC.gc()
    @profile for _ in 1:reps; f(); end
    io = IOBuffer()
    Profile.print(IOContext(io, :displaysize => (60, 250)); format=:flat, sortedby=:overhead,
                  C=true, combine=true, noisefloor=0, mincount=0)
    lines = split(String(take!(io)), '\n')
    println("=== ", shape, "  (", reps, " reps × ", round(t*1000, digits=1), " ms, ",
            round(MB * reps / (t * reps), digits=0), " MiB/s at ", Threads.nthreads(), "T)")
    # header + top 28 self-time lines
    hdr = findfirst(l -> occursin("Overhead", l), lines)
    hdr === nothing && (println(join(lines[1:min(end, 30)], '\n')); continue)
    body = filter(l -> !isempty(strip(l)), lines[hdr + 2:end])
    for l in reverse(body[max(1, end - 34):end])
        println(l)
    end
    println()
    flush(stdout)
end
