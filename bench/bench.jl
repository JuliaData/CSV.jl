# Throughput matrix: frozen CSV 0.10 vs the new kernel, across data shapes and sizes.
#
# NOT a rigorous benchmark suite — a breadth probe to check the architecture's
# performance is broadly applicable rather than tuned to one shape. Caveats on
# comparability, stated up front:
#   * CSV.File materializes columns eagerly (incl. InlineString/pooled string
#     columns); the kernel returns inline-or-view CompactString columns. The "kernel+str"
#     column therefore ALSO materializes every string column to Vector{String} —
#     that is the fair "owned data" comparison. Non-string columns are directly
#     comparable (both engines hand back materialized values).
#   * CSV.File is run with silencewarnings=true (the quoted shape triggers its
#     multithreaded chunking fallback warnings).
#   * Timings are best-of-N wall clock with auto-repetition for tiny inputs.
#
# Run:   julia --project=test -t8 bench/bench.jl           # full matrix vs LegacyCSV
#        julia --project=test -t8 bench/bench.jl 0.01 20   # just these sizes (MiB)
#        julia --project=test -t1 bench/bench.jl 20        # single-thread story

using CSV; const CSVKernel = CSV.CSVKernel
using .CSVKernel
using Dates, Random
const K = CSVKernel

include(joinpath(@__DIR__, "legacycsv.jl"))
const CSVMOD = LegacyCSV

# ---------------------------------------------------------------------------
# data shapes
# ---------------------------------------------------------------------------

const WORDS = [String(rand(MersenneTwister(7 + i), 'a':'z', rand(MersenneTwister(31 * i + 1), 3:12))) for i in 1:5000]

function genrows(io, shape::Symbol, targetbytes::Int, rng)
    if shape === :numeric
        println(io, "id,a,b,c,d,e")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i, ",", i * 3, ",", i % 1000, ",", i * 0.25, ",", i * 1.5e-3, ",", -i * 7)
        end
    elseif shape === :mixed
        println(io, "id,value,ratio,label,when,flag")
        i = 0
        while position(io) < targetbytes
            i += 1
            label = i % 20 == 0 ? "\"text with, comma and \"\"quote\"\" $i\"" :
                    i % 7 == 0  ? "" : "label$(i % 1000)"
            println(io, i, ",", i * 3, ",", i * 0.25, ",", label, ",",
                    Date(2020, 1, 1) + Day(i % 1000), ",", isodd(i))
        end
    elseif shape === :strings
        println(io, join(("s$j" for j in 1:8), ','))
        while position(io) < targetbytes
            vals = [rand(rng) < 0.10 ? "\"$(rand(rng, WORDS)), $(rand(rng, WORDS))\"" :
                    rand(rng, WORDS) for _ in 1:8]
            println(io, join(vals, ','))
        end
    elseif shape === :quoted
        # every field quoted; embedded delimiters, escaped quotes, and (5%)
        # embedded newlines — the shape that breaks speculative chunking
        println(io, "q1,q2,q3,q4,q5")
        while position(io) < targetbytes
            vals = map(1:5) do _
                w = rand(rng, WORDS)
                r = rand(rng)
                inner = r < 0.20 ? "$w, $(rand(rng, WORDS))" :
                        r < 0.25 ? "$w \"\"$(rand(rng, WORDS))\"\"" :
                        r < 0.30 ? "$w\n$(rand(rng, WORDS))" : w
                "\"$inner\""
            end
            println(io, join(vals, ','))
        end
    elseif shape === :wide
        ncols = 200
        println(io, join(("c$j" for j in 1:ncols), ','))
        i = 0
        while position(io) < targetbytes
            i += 1
            print(io, i)
            for j in 2:ncols
                print(io, ',', (i * j) % 977 + j * 0.5)
            end
            println(io)
        end
    elseif shape === :longnarrow
        println(io, "k,v")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i, ",", i * 0.125)
        end
    elseif shape === :sparse
        # 40% of cells empty ⇒ missing-handling dominates
        println(io, "a,b,c,d,e,f")
        i = 0
        while position(io) < targetbytes
            i += 1
            vals = [rand(rng) < 0.4 ? "" :
                    j <= 3 ? string(i * j) : string(i * 0.5) for j in 1:6]
            println(io, join(vals, ','))
        end
    else
        error("unknown shape $shape")
    end
end

function makedata(shape::Symbol, targetbytes::Int)
    io = IOBuffer()
    genrows(io, shape, targetbytes, MersenneTwister(20260812))
    return take!(io)
end

# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------

# best-of-N with automatic inner repetition so tiny inputs get measurable timings
function besttime(f; reps::Int=3, mintime::Float64=0.02)
    f()  # warmup / compile
    t1 = @elapsed f()
    inner = t1 >= mintime ? 1 : max(1, ceil(Int, mintime / max(t1, 1e-9)))
    best = Inf
    for _ in 1:reps
        GC.gc()
        t = @elapsed for _ in 1:inner
            f()
        end
        best = min(best, t / inner)
    end
    return best
end

mibs(bytes, t) = bytes / 2^20 / t
fmt(x) = x >= 100 ? string(round(Int, x)) : string(round(x, digits=1))
fmttime(t) = t >= 1 ? string(round(t, digits=2), " s ") :
             t >= 1e-3 ? string(round(t * 1e3, digits=1), " ms") :
             string(round(t * 1e6, digits=0), " µs")

materializestrings(t::K.ParsedTable) =
    foreach(c -> c isa K.CompactStringVector && K.materialize(c), K.columns(t))

function runcell(shape::Symbol, mb::Float64)
    buf = makedata(shape, round(Int, mb * 2^20))
    bytes = length(buf)
    # row-count sanity between engines
    tk = K.parse(buf)
    nrows = tk.nrows
    tkern = besttime(() -> K.parse(buf))
    tkernstr = besttime(() -> materializestrings(K.parse(buf)))
    tcsv = NaN
    if CSVMOD !== nothing
        f = Base.invokelatest(CSVMOD.File, buf; silencewarnings=true)
        Base.invokelatest(length, f) == nrows ||
            @warn "row count mismatch" shape mb kernel=nrows csv=Base.invokelatest(length, f)
        tcsv = besttime(() -> Base.invokelatest(CSVMOD.File, buf; silencewarnings=true))
    end
    return (; shape, bytes, nrows, tkern, tkernstr, tcsv)
end

function main(sizes)
    shapes = (:numeric, :mixed, :strings, :quoted, :wide, :longnarrow, :sparse)
    println("threads=$(Threads.nthreads())  julia=$(VERSION)  CSV.jl=",
            LEGACYCSV_VERSION)
    println()
    header = rpad("shape", 11) * rpad("size", 9) * lpad("rows", 10) * " │" *
             lpad("CSV.File", 10) * lpad("kernel", 10) * lpad("kernel+str", 12) * " │" *
             lpad("k/CSV", 7) * lpad("k+s/CSV", 9)
    println(header)
    println("─"^length(header))
    for mb in sizes, shape in shapes
        r = runcell(shape, mb)
        csvs = isnan(r.tcsv) ? lpad("—", 10) : lpad(fmt(mibs(r.bytes, r.tcsv)), 10)
        sizelabel = r.bytes >= 2^20 ? "$(round(Int, r.bytes / 2^20)) MiB" : "$(round(Int, r.bytes / 2^10)) KiB"
        line = rpad(string(r.shape), 11) * rpad(sizelabel, 9) *
               lpad(string(r.nrows), 10) * " │" *
               csvs *
               lpad(fmt(mibs(r.bytes, r.tkern)), 10) *
               lpad(fmt(mibs(r.bytes, r.tkernstr)), 12) * " │" *
               (isnan(r.tcsv) ? lpad("—", 7) : lpad(string(round(r.tcsv / r.tkern, digits=2)), 7)) *
               (isnan(r.tcsv) ? lpad("—", 9) : lpad(string(round(r.tcsv / r.tkernstr, digits=2)), 9))
        println(line)
        # absolute times matter more than MiB/s for tiny files
        if r.bytes < 2^20
            println(" "^11, "(abs: CSV.File ", isnan(r.tcsv) ? "—" : fmttime(r.tcsv),
                    "  kernel ", fmttime(r.tkern), "  kernel+str ", fmttime(r.tkernstr), ")")
        end
        flush(stdout)
    end
    println()
    println("ratios > 1 mean the kernel is faster; kernel+str collects string columns to Vector{String}")
end

main(isempty(ARGS) ? (0.01, 1.0, 20.0, 200.0) : Tuple(Base.parse(Float64, a) for a in ARGS))
