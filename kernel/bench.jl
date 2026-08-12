# Quick throughput probe for the kernel — NOT a rigorous benchmark. The kernel is
# a prove-out: the SWAR scanner is word-sized (8 B/iter, no CLMUL/SIMD.jl yet) and
# value parsing is stock Parsers.jl. The numbers below are the *floor* the design
# starts from, useful for (a) sanity ("is the architecture in the right decade?")
# and (b) tracking the index-vs-parse cost split that motivates the layering.
#
# Run:   julia --project=kernel -t8 kernel/bench.jl [nrows]
# For a CSV.jl comparison, run from the repo project instead (it has CSV):
#        julia --project=. -t8 kernel/bench.jl [nrows]
#        (first: julia --project=. -e 'using Pkg; Pkg.instantiate()')

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))
using .CSVKernel
using Dates
const K = CSVKernel

function makedata(nrows::Int)
    io = IOBuffer()
    println(io, "id,value,ratio,label,when,flag")
    for i in 1:nrows
        label = i % 20 == 0 ? "\"text with, comma and \"\"quote\"\" $i\"" :
                i % 7 == 0  ? "" : "label$(i % 1000)"
        println(io, i, ",", i * 3, ",", i * 0.25, ",", label, ",",
                Date(2020, 1, 1) + Day(i % 1000), ",", isodd(i))
    end
    return take!(io)
end

function timeit(f, n=3)
    f()  # warmup/compile
    best = Inf
    for _ in 1:n
        GC.gc()
        best = min(best, @elapsed f())
    end
    return best
end

function main(nrows::Int)
    buf = makedata(nrows)
    mb = length(buf) / 2^20
    println("input: $nrows rows, $(round(mb, digits=1)) MiB, ",
            Threads.nthreads(), " thread(s)\n")
    d = K.Dialect()

    for (label, kw) in (
            "index scalar/sequential" => (parallel=false, fastindex=false),
            "index swar/sequential  " => (parallel=false, fastindex=true),
            "index swar/parallel    " => (parallel=true, fastindex=true),
        )
        t = timeit(() -> K.index(buf, d; kw...))
        println(label, "  ", lpad(round(mb / t, digits=0), 6), " MiB/s")
    end

    t = timeit(() -> K.parse(buf))
    println("full parse (typed)      ", lpad(round(mb / t, digits=0), 6), " MiB/s")

    # optional cross-check against CSV.jl when available in the active project
    csvmod = try
        @eval import CSV
        @eval CSV
    catch
        nothing
    end
    if csvmod !== nothing
        t = timeit(() -> Base.invokelatest(csvmod.File, copy(buf)))
        println("CSV.File (current pkg)  ", lpad(round(mb / t, digits=0), 6), " MiB/s")
    else
        println("(CSV.jl not in this project — run with --project=. for a comparison)")
    end
end

main(isempty(ARGS) ? 1_000_000 : Base.parse(Int, ARGS[1]))
