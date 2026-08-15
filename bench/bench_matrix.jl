# Broad performance matrix: CSVApi.File (the real front door) vs CSV.jl,
# across shapes × sizes × type combinations — the sharp-edge finder.
#
# Extends bench.jl's breadth probe up to the API layer and across many more
# type combinations: pooling tiers (low/high/over-cap), temporal/bool columns,
# missing-density and quote/escape-density sweeps, long text (CompactString view path),
# very wide, grouped digits, ignorerepeated, CRLF, dirty/ragged, sentinels.
# Same honesty rules as bench.jl: CSV.File runs silencewarnings=true; string
# columns stay in their native containers on both sides (CSV materializes
# InlineStrings/pooled, we return CompactString/PooledColumn) — per-cell work is what
# is being compared, and the `api_str` config adds stringtype=String where the
# owned-data comparison matters.
#
# Run:  julia --project=. -t8 bench/bench_matrix.jl LABEL [sizes...]
#       results append to kernel-bench-LABEL.tsv next to this file, table to stdout.
#       julia --project=. -t1 bench/bench_matrix.jl LABEL-1t 20 --core

using CSV; const CSVApi = CSV.CSVApi; const CSVKernel = CSV.CSVKernel
using Dates, Random, Tables
const K = CSVKernel
const A = CSVApi

const CSVMOD = try
    @eval import CSV
    @eval CSV
catch
    nothing
end

# ---------------------------------------------------------------------------
# shapes — each returns bytes; header names say what the type mix is
# ---------------------------------------------------------------------------

const WORDS = [String(rand(MersenneTwister(7 + i), 'a':'z', rand(MersenneTwister(31 * i + 1), 3:12))) for i in 1:5000]
const LEVELS8 = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"]
const LEVELS400 = ["lvl$(lpad(i, 4, '0'))" for i in 1:400]
const LEVELS5000 = ["item$(lpad(i, 5, '0'))" for i in 1:5000]

function genrows(io, shape::Symbol, targetbytes::Int, rng)
    if shape === :numeric
        println(io, "id,a,b,c,d,e")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i, ",", i * 3, ",", i % 1000, ",", i * 0.25, ",", i * 1.5e-3, ",", -i * 7)
        end
    elseif shape === :floatonly
        println(io, "a,b,c,d,e,f")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i * 0.25, ",", i * 1.5e-3, ",", -i * 0.7, ",", i * 3.25, ",",
                    i / 7, ",", i * 1e5)
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
    elseif shape === :escaped
        # EVERY string cell carries escaped quotes ⇒ the unescape/staging path
        println(io, "e1,e2,e3,n")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, "\"a\"\"b$(i % 97)\"\"c\",\"\"\"lead$(i % 89)\",\"tail$(i % 83)\"\"\",", i)
        end
    elseif shape === :longtext
        # 80–240 byte fields: every string takes the CompactString view (non-inline) path
        println(io, "id,doc1,doc2")
        i = 0
        while position(io) < targetbytes
            i += 1
            d1 = join(rand(rng, WORDS, 12), ' ')
            d2 = join(rand(rng, WORDS, 20), ' ')
            println(io, i, ",", d1, ",", d2)
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
    elseif shape === :verywide
        ncols = 1000
        println(io, join(("c$j" for j in 1:ncols), ','))
        i = 0
        while position(io) < targetbytes
            i += 1
            print(io, i)
            for j in 2:ncols
                print(io, ',', (i * j) % 97)
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
        println(io, "a,b,c,d,e,f")
        i = 0
        while position(io) < targetbytes
            i += 1
            vals = [rand(rng) < 0.4 ? "" :
                    j <= 3 ? string(i * j) : string(i * 0.5) for j in 1:6]
            println(io, join(vals, ','))
        end
    elseif shape === :missing90
        println(io, "a,b,c,d,e,f")
        i = 0
        while position(io) < targetbytes
            i += 1
            vals = [rand(rng) < 0.9 ? "" : string(i * j) for j in 1:6]
            println(io, join(vals, ','))
        end
    elseif shape === :pooled_low
        # 2 categorical (8 levels) + 2 numeric — the classic pooling win
        println(io, "region,status,qty,amt")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, LEVELS8[i % 8 + 1], ",", LEVELS8[(i * 3) % 8 + 1], ",",
                    i % 500, ",", i * 0.25)
        end
    elseif shape === :pooled_high
        # 400 levels: inside CSV.jl's (0.2, 500) cap but hash-heavy
        println(io, "sku,store,qty")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, LEVELS400[i % 400 + 1], ",", LEVELS400[(i * 7) % 400 + 1], ",", i % 100)
        end
    elseif shape === :pooled_overcap
        # 5000 levels: exceeds the 500-level cap ⇒ the pooling-abandon path
        println(io, "id,item,qty")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i, ",", LEVELS5000[i % 5000 + 1], ",", i % 100)
        end
    elseif shape === :temporal
        println(io, "d,dt,t,v")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, Date(2020, 1, 1) + Day(i % 2000), ",",
                    DateTime(2020, 1, 1, 0, 0, 0) + Second(i % 86400), ",",
                    Time(i % 24, i % 60, (i * 7) % 60), ",", i)
        end
    elseif shape === :bools
        println(io, "b1,b2,b3,n")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, isodd(i), ",", i % 3 == 0, ",", i % 11 == 0 ? "" : (i % 5 == 0), ",", i)
        end
    elseif shape === :groupmark
        println(io, "amt1;amt2;label")
        i = 0
        while position(io) < targetbytes
            i += 1
            a = string(i * 1234567); b = string(i * 891)
            g(s) = join(reverse([reverse(join(x)) for x in Iterators.partition(reverse(s), 3)]), ",")
            println(io, "\"", g(a), "\";\"", g(b), "\";x", i % 50)
        end
    elseif shape === :irspace
        println(io, "  id   region    qty     amt")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, lpad(i, 6), " ", rpad(LEVELS8[i % 8 + 1], 9), " ",
                    lpad(i % 500, 5), "  ", lpad(round(i * 0.25, digits=2), 9))
        end
    elseif shape === :dirty
        # 5% ragged short rows + 2% unparseable cells under inferred Int cols
        println(io, "a,b,c,d")
        i = 0
        while position(io) < targetbytes
            i += 1
            if i % 20 == 0
                println(io, i, ",", i * 2)                       # short row
            elseif i % 50 == 0
                println(io, i, ",oops,", i * 3, ",", i % 7)      # bad cell
            else
                println(io, i, ",", i * 2, ",", i * 3, ",", i % 7)
            end
        end
    elseif shape === :crlf
        print(io, "id,a,b,c\r\n")
        i = 0
        while position(io) < targetbytes
            i += 1
            print(io, i, ",", i * 3, ",", i * 0.25, ",v", i % 100, "\r\n")
        end
    elseif shape === :sentinel
        println(io, "a,b,c")
        i = 0
        while position(io) < targetbytes
            i += 1
            println(io, i % 9 == 0 ? "NA" : string(i), ",", i % 7 == 0 ? "NA" : string(i * 0.5),
                    ",v", i % 40)
        end
    else
        error("unknown shape $shape")
    end
end

function makedata(shape::Symbol, targetbytes::Int)
    io = IOBuffer()
    genrows(io, shape, targetbytes, MersenneTwister(20260814))
    return take!(io)
end

# per-shape kwargs (api side, csv side)
shapekw(shape) =
    shape === :groupmark ? ((; delim=';', groupmark=','), (; delim=';', groupmark=',')) :
    shape === :irspace   ? ((; delim=' ', ignorerepeated=true), (; delim=' ', ignorerepeated=true)) :
    shape === :sentinel  ? ((; missingstring="NA"), (; missingstring=["NA", ""])) :
                           (NamedTuple(), NamedTuple())

# shapes whose schemas deliberately diverge (long-row widening) — rowcount only
const NOPARITY = (:dirty,)

# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------

function besttime(f; reps::Int=4, mintime::Float64=0.02)
    f()
    t1 = @elapsed f()
    inner = t1 >= mintime ? 1 : max(1, ceil(Int, mintime / max(t1, 1e-9)))
    allocs = @allocated f()
    best = Inf
    for _ in 1:reps
        GC.gc()
        t = @elapsed for _ in 1:inner
            f()
        end
        best = min(best, t / inner)
    end
    return best, allocs
end

mibs(bytes, t) = bytes / 2^20 / t
fmt(x) = x >= 100 ? string(round(Int, x)) : string(round(x, digits=1))

const RESULTS = Ref{Union{Nothing, IOStream}}(nothing)

function record(label, shape, mb, config, bytes, nrows, t, allocs)
    io = RESULTS[]
    io === nothing && return
    println(io, join((label, shape, mb, config, bytes, nrows,
                      round(t * 1e6, digits=1), allocs,
                      round(mibs(bytes, t), digits=1)), '\t'))
    flush(io)
end

function runcell(label, shape::Symbol, mb::Float64; core::Bool)
    buf = makedata(shape, round(Int, mb * 2^20))
    bytes = length(buf)
    apikw, csvkw = shapekw(shape)
    fa = A.File(copy(buf); apikw...)
    nrows = Tables.rowcount(fa)
    cells = Vector{NamedTuple}()

    t, al = besttime(() -> A.File(copy(buf); apikw...))
    push!(cells, (; config="api", t, al))
    record(label, shape, mb, "api", bytes, nrows, t, al)

    if any(c -> eltype(c) <: Union{K.CompactString, Missing} || c isa K.PooledColumn,
           K.columns(Tables.columns(fa)))
        t2, al2 = besttime(() -> A.File(copy(buf); pool=false, apikw...))
        push!(cells, (; config="api_nopool", t=t2, al=al2))
        record(label, shape, mb, "api_nopool", bytes, nrows, t2, al2)
        t3, al3 = besttime(() -> A.File(copy(buf); stringtype=String, apikw...))
        push!(cells, (; config="api_str", t=t3, al=al3))
        record(label, shape, mb, "api_str", bytes, nrows, t3, al3)
    end

    if !core && shape in (:numeric, :mixed, :strings, :pooled_low)
        # these shapes take no api-only kwargs, so kparse sees the same options
        t4, al4 = besttime(() -> K.parse(copy(buf)))
        push!(cells, (; config="kparse", t=t4, al=al4))
        record(label, shape, mb, "kparse", bytes, nrows, t4, al4)
    end

    tcsv = NaN
    if CSVMOD !== nothing
        fc = Base.invokelatest(CSVMOD.File, copy(buf); silencewarnings=true, csvkw...)
        ncsv = Base.invokelatest(length, fc)
        shape in NOPARITY || ncsv == nrows ||
            @warn "row count mismatch" shape mb api=nrows csv=ncsv
        tcsv, alcsv = besttime(() -> Base.invokelatest(CSVMOD.File, copy(buf);
                                                       silencewarnings=true, csvkw...))
        record(label, shape, mb, "csv", bytes, nrows, tcsv, alcsv)
    end
    return (; shape, mb, bytes, nrows, cells, tcsv)
end

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

const ALLSHAPES = (:numeric, :floatonly, :mixed, :strings, :quoted, :escaped,
                   :longtext, :wide, :verywide, :longnarrow, :sparse, :missing90,
                   :pooled_low, :pooled_high, :pooled_overcap, :temporal, :bools,
                   :groupmark, :irspace, :dirty, :crlf, :sentinel)
const CORESHAPES = (:numeric, :mixed, :strings, :quoted, :wide, :pooled_low, :temporal)

function main(args)
    label = isempty(args) ? "run" : args[1]
    rest = args[2:end]
    core = "--core" in rest
    sizes = Tuple(Base.parse(Float64, a) for a in rest if a != "--core")
    isempty(sizes) && (sizes = (20.0,))
    path = joinpath(@__DIR__, "kernel-bench-$label.tsv")
    RESULTS[] = open(path, "a")
    println(RESULTS[], "# threads=$(Threads.nthreads()) julia=$(VERSION) at=$(now())")
    println("threads=$(Threads.nthreads())  label=$label  sizes=$sizes  → $path")
    header = rpad("shape", 15) * rpad("size", 9) * lpad("rows", 10) * " │" *
             lpad("CSV.File", 10) * lpad("api", 9) * lpad("api/CSV", 9) * "  other configs (MiB/s)"
    println(header); println("─"^length(header))
    for mb in sizes
        shapes = (core || mb >= 100) ? CORESHAPES : ALLSHAPES
        for shape in shapes
            r = runcell(label, shape, mb; core)
            api = first(c for c in r.cells if c.config == "api")
            sizelabel = r.bytes >= 2^20 ? "$(round(Int, r.bytes / 2^20)) MiB" : "$(round(Int, r.bytes / 2^10)) KiB"
            extras = join(("$(c.config)=$(fmt(mibs(r.bytes, c.t)))"
                           for c in r.cells if c.config != "api"), "  ")
            println(rpad(string(r.shape), 15), rpad(sizelabel, 9),
                    lpad(string(r.nrows), 10), " │",
                    lpad(isnan(r.tcsv) ? "—" : fmt(mibs(r.bytes, r.tcsv)), 10),
                    lpad(fmt(mibs(r.bytes, api.t)), 9),
                    lpad(isnan(r.tcsv) ? "—" : string(round(r.tcsv / api.t, digits=2)), 9),
                    "  ", extras)
            flush(stdout)
        end
        println()
    end
    close(RESULTS[])
end

abspath(PROGRAM_FILE) == (@__FILE__) && main(ARGS)
