# Float sharp-edge sweep: kernel parsefloat64 vs Base.parse vs Parsers.xparse vs
# fast_float, over a corpus per shape. Same bytes everywhere; a checksum
# cross-checks the values (fast_float's from_chars rejects a leading '+', so
# the "+"-bearing shapes legitimately differ).
#
# Setup (once): drop fast_float's amalgamated header next to bench/ffbench.cpp
#   curl -sL -o bench/fast_float.h https://github.com/fastfloat/fast_float/releases/latest/download/fast_float.h
#   clang++ -O3 -std=c++17 -march=native -o bench/ffbench bench/ffbench.cpp
# Run:  julia --project=. bench/floatsweep.jl
using CSV, Parsers, Random, Printf
const V = CSV.CSVKernel.V
const DIR = @__DIR__
rng = MersenneTwister(2026)
N = 100_000
shortest() = string(reinterpret(Float64, rand(rng, UInt64) & 0x7fefffffffffffff))
shapes = [
    ("short x.y (3+3 dig)",  () -> string(round(rand(rng) * 1000, digits=3))),
    ("integer-like",         () -> string(rand(rng, 1:10^9))),
    ("shortest 17-digit",    shortest),
    ("subnormal shortest",   () -> string(reinterpret(Float64, rand(rng, UInt64) & 0x000fffffffffffff))),
    ("exp small e-30..30",   () -> string(rand(rng, 1:999)) * "." * string(rand(rng, 0:99)) * "e" * string(rand(rng, -30:30))),
    ("exp huge e300/e-300",  () -> string(rand(rng, 1:9)) * "." * string(rand(rng, 1:99)) * "e" * string(rand(rng, Bool) ? rand(rng, 280:308) : rand(rng, -320:-280))),
    ("leading zeros 0.000x", () -> "0." * "0"^rand(rng, 5:20) * string(rand(rng, 1:99999))),
    ("20-digit mantissa",    () -> String(rand(rng, '0':'9', 20)) * "." * String(rand(rng, '0':'9', 5))),
    ("40-digit mantissa",    () -> String(rand(rng, '0':'9', 40))),
    ("100-digit mantissa",   () -> "0." * String(rand(rng, '0':'9', 100))),
    ("400-digit mantissa",   () -> "0." * String(rand(rng, '0':'9', 400)) * "e" * string(rand(rng, -100:100))),
    ("halfway 2^53+1 style", () -> string(9007199254740993 + 2 * rand(rng, 0:10^6))),
    ("negative short",       () -> "-" * string(round(rand(rng) * 100, digits=2))),
    ("plus sign / e+",       () -> "+" * string(rand(rng, 1:999)) * "e+" * string(rand(rng, 0:20))),
    ("specials Inf/NaN",     () -> rand(rng, ("Inf", "-Inf", "NaN", "inf", "nan", "+Inf", "Infinity"))),
    ("invalid mix",          () -> rand(rng, ("1..2", "abc", "1e", "", "1.5x", "--1", ".", "e5"))),
]
println(rpad("shape", 24), lpad("kernel", 8), lpad("Base", 8), lpad("xparse", 8), lpad("fast_float", 12), "   (ns/value)  worst-vs-ff")
println("─"^78)
const OPTS = Parsers.Options()
function timeit(f, buf, spans)
    f(buf, spans); best = Inf
    for _ in 1:7; t = @elapsed f(buf, spans); best = min(best, t); end
    return best / length(spans) * 1e9
end
worst = String[]
for (name, gen) in shapes
    vals = [gen() for _ in 1:N]
    path = joinpath(DIR, "corpus.txt"); open(path, "w") do io; foreach(v -> println(io, v), vals); end
    buf = Vector{UInt8}(join(vals, '\n') * "\n" * " "^16)
    spans = Tuple{Int,Int}[]; let s = 1; for v in vals; push!(spans, (s, s + ncodeunits(v) - 1)); s += ncodeunits(v) + 1; end; end
    fk(b, ss) = (a = UInt64(0); for (i, j) in ss; v, rc = V.parsefloat64(b, i, j); a ⊻= (rc == V.RC_OK ? reinterpret(UInt64, v) : UInt64(0xdeadbeef)) + 0x9e3779b97f4a7c15 * UInt64(j - i + 1); end; a)
    fb(b, ss) = (a = UInt64(0); for (i, j) in ss; v = tryparse(Float64, String(b[i:j])); a ⊻= (v === nothing ? UInt64(0xdeadbeef) : reinterpret(UInt64, v)) + 0x9e3779b97f4a7c15 * UInt64(j - i + 1); end; a)
    fx(b, ss) = (a = UInt64(0); for (i, j) in ss; r = Parsers.xparse(Float64, b, i, j, OPTS); a ⊻= (Parsers.ok(r.code) && r.tlen == j - i + 1 ? reinterpret(UInt64, r.val) : UInt64(0xdeadbeef)) + 0x9e3779b97f4a7c15 * UInt64(j - i + 1); end; a)
    tk = timeit(fk, buf, spans); tb = timeit(fb, buf, spans)
    tx = try timeit(fx, buf, spans) catch e; NaN end   # Parsers 2.x throws InexactError on 400-digit mantissas
    ffout = read(`$(joinpath(DIR, "ffbench")) $path`, String)
    tff = parse(Float64, split(ffout)[1]); ffchk = parse(UInt64, split(ffout)[2]; base=16)
    kchk = fk(buf, spans)
    agree = kchk == ffchk ? "" : "  [values differ from fast_float: see note]"
    ratio = tk / tff
    ratio > 1.15 && push!(worst, "$name: $(round(ratio, digits=2))x")
    println(rpad(name, 24), lpad(round(tk, digits=1), 8), lpad(round(tb, digits=1), 8), lpad(round(tx, digits=1), 8), lpad(round(tff, digits=1), 12), lpad(round(ratio, digits=2), 8), "x", agree)
end
println("\nshapes where kernel > 1.15x fast_float: ", isempty(worst) ? "none" : join(worst, "; "))
