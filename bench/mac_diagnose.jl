# Temporary paired diagnosis for PR 1196; not part of the release branch.
using CSV, Tables, Dates, Random, Statistics, Printf, Profile, InteractiveUtils
include(joinpath(@__DIR__, "bench_matrix.jl"))
include(joinpath(@__DIR__, "writeshapes.jl"))
const BASE_TREE, CANDIDATE_TREE, RESULTS_DIR = abspath.(ARGS[1:3])
mkpath(RESULTS_DIR)
const QUICK_PROBE = get(ENV, "CSV_MAC_PROBE_QUICK", "0") == "1"
const SAMPLES = QUICK_PROBE ? 4 : 20
const TEMP_TREES = String[]
function loadrevision(name::Symbol, tree::String; replacements=[])
    if !isempty(replacements)
        tmp = mktempdir()
        push!(TEMP_TREES, tmp)
        cp(joinpath(tree, "src"), joinpath(tmp, "src"))
        for (file, before, after) in replacements
            path = joinpath(tmp, "src", file)
            text = read(path, String)
            @assert occursin(before, text) "diagnostic replacement missing: $file"
            write(path, replace(text, before => after; count=1))
        end
        tree = tmp
    end
    parent = Core.eval(Main, :(module $name end))
    mod = Base.include(parent, joinpath(tree, "src", "CSV.jl"))
    Base.invokelatest(() -> mod.__init__())
    return mod
end
const OLD = loadrevision(:StartingRevision, BASE_TREE)
const NEW = loadrevision(:CurrentRevision, CANDIDATE_TREE)
const STR16 = loadrevision(:ShortScan16, CANDIDATE_TREE; replacements=[
    ("write.jl", "while k + 8 <= n", "while n >= 16 && k + 8 <= n")])
const UNGUARDED = if Sys.ARCH === :x86_64 && NEW.HAS_PCLMUL[]
    loadrevision(:GuardBypass, CANDIDATE_TREE; replacements=[
        ("core.jl", "@inline prefix_xor64(m::UInt64) = HAS_PCLMUL[] ? prefix_xor64_pclmul(m) : prefix_xor64_shift(m)",
         "@inline prefix_xor64(m::UInt64) = prefix_xor64_pclmul(m)")])
else
    nothing
end
const PCLMUL_ONES = if UNGUARDED !== nothing
    loadrevision(:AllOnesOperand, CANDIDATE_TREE; replacements=[
        ("core.jl", "%b0 = insertelement <2 x i64> zeroinitializer, i64 -1, i32 0",
         "%b0 = insertelement <2 x i64> <i64 -1, i64 -1>, i64 -1, i32 0")])
else
    nothing
end
const MIXED_INPUT = makedata(:mixed, (QUICK_PROBE ? 1 : 20) * 2^20)
const STRING_TABLE = shape(:strings, QUICK_PROBE ? 50_000 : 1_000_000, MersenneTwister(7))
const DIALECT_OLD, DIALECT_NEW = OLD.Dialect(), NEW.Dialect()
oldfile = OLD.File(MIXED_INPUT; ntasks=1, on_error=:collect)
for mod in filter(!isnothing, [NEW, STR16, UNGUARDED, PCLMUL_ONES])
    f = mod.File(MIXED_INPUT; ntasks=1, on_error=:collect)
    @assert isequal(Tables.columntable(f), Tables.columntable(oldfile))
    @assert take!(mod.write(IOBuffer(), STRING_TABLE)) == take!(OLD.write(IOBuffer(), STRING_TABLE))
end
function countquoted(f, opts, values)
    count = 0
    for s in values
        GC.@preserve s count += f(opts, pointer(s), ncodeunits(s))
    end
    return count
end
println("Paired value and byte checks pass; samples=", SAMPLES)
flush(stdout)
const OUTPUT = open(joinpath(RESULTS_DIR, "paired-times.tsv"), "w")
println(OUTPUT, "case\tsample\tfirst\tbaseline_ms\tcandidate_ms")
function measure(f)
    GC.gc()
    return @elapsed f()
end
function pair(name, a, b)
    for _ in 1:3
        a(); b()
    end
    ta, tb = Float64[], Float64[]
    for i in 1:SAMPLES
        if isodd(i)
            x, y = measure(a), measure(b)
        else
            y, x = measure(b), measure(a)
        end
        push!(ta, x); push!(tb, y)
        println(OUTPUT, join((name, i, isodd(i) ? "A" : "B", x * 1000, y * 1000), '\t'))
        flush(OUTPUT)
    end
    @printf("%-32s min %.3f / %.3f ms (%.3f); median %.3f / %.3f ms (%.3f)\n",
        name, minimum(ta)*1000, minimum(tb)*1000, minimum(tb)/minimum(ta),
        median(ta)*1000, median(tb)*1000, median(tb)/median(ta))
    flush(stdout)
end
pair("index/mixed/ntasks1", () -> OLD.index(MIXED_INPUT, DIALECT_OLD; ntasks=1),
     () -> NEW.index(MIXED_INPUT, DIALECT_NEW; ntasks=1))
pair("file/mixed/ntasks1", () -> OLD.File(MIXED_INPUT; ntasks=1, on_error=:collect),
     () -> NEW.File(MIXED_INPUT; ntasks=1, on_error=:collect))
pair("file/mixed/scalar", () -> OLD.File(MIXED_INPUT; ntasks=1, fastindex=false, on_error=:collect),
     () -> NEW.File(MIXED_INPUT; ntasks=1, fastindex=false, on_error=:collect))
pair("write/strings/ntasks1", () -> OLD.write(IOBuffer(), STRING_TABLE; ntasks=1),
     () -> NEW.write(IOBuffer(), STRING_TABLE; ntasks=1))
pair("write/strings/default", () -> OLD.write(IOBuffer(), STRING_TABLE),
     () -> NEW.write(IOBuffer(), STRING_TABLE))
const OLD_WRITE_OPTIONS, NEW_WRITE_OPTIONS = OLD._writeopts(), NEW._writeopts()
pair("quote-scan/short-strings", () -> countquoted(OLD._needsquotebytes, OLD_WRITE_OPTIONS, STRING_TABLE.s1),
     () -> countquoted(NEW._needsquotebytes, NEW_WRITE_OPTIONS, STRING_TABLE.s1))
pair("candidate/scan16/ntasks1", () -> NEW.write(IOBuffer(), STRING_TABLE; ntasks=1),
     () -> STR16.write(IOBuffer(), STRING_TABLE; ntasks=1))
pair("candidate/scan16/default", () -> NEW.write(IOBuffer(), STRING_TABLE),
     () -> STR16.write(IOBuffer(), STRING_TABLE))
if UNGUARDED !== nothing
    pair("candidate/guard/index", () -> NEW.index(MIXED_INPUT, DIALECT_NEW; ntasks=1),
         () -> UNGUARDED.index(MIXED_INPUT, UNGUARDED.Dialect(); ntasks=1))
    pair("candidate/guard/file", () -> NEW.File(MIXED_INPUT; ntasks=1, on_error=:collect),
         () -> UNGUARDED.File(MIXED_INPUT; ntasks=1, on_error=:collect))
    pair("candidate/ones/index", () -> NEW.index(MIXED_INPUT, DIALECT_NEW; ntasks=1),
         () -> PCLMUL_ONES.index(MIXED_INPUT, PCLMUL_ONES.Dialect(); ntasks=1))
    pair("candidate/ones/file", () -> NEW.File(MIXED_INPUT; ntasks=1, on_error=:collect),
         () -> PCLMUL_ONES.File(MIXED_INPUT; ntasks=1, on_error=:collect))
    for (label, mod) in [("original", NEW), ("unguarded", UNGUARDED), ("all-ones", PCLMUL_ONES)]
        open(joinpath(RESULTS_DIR, "prefix-$label.txt"), "w") do io
            code_native(io, mod.prefix_xor64, (UInt64,); debuginfo=:none)
        end
    end
end
close(OUTPUT)
for (label, mod) in [("baseline", OLD), ("candidate", NEW)]
    Profile.clear()
    Profile.@profile for _ in 1:(QUICK_PROBE ? 2 : 30)
        mod.File(MIXED_INPUT; ntasks=1, on_error=:collect)
    end
    open(joinpath(RESULTS_DIR, "profile-$label.txt"), "w") do io
        Profile.print(io; format=:flat, sortedby=:count, mincount=10)
    end
end
println("All paired cases complete")
