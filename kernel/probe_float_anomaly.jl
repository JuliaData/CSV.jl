include(joinpath(homedir(), ".julia/dev/CSV-kernel-proveout/kernel/values.jl"))
using .KernelValues; const V = KernelValues
using Random
function corpus()
    rng = MersenneTwister(7)
    vals = String[]
    for _ in 1:100_000
        x = reinterpret(Float64, rand(rng, UInt64))
        (isnan(x) || isinf(x)) && continue
        push!(vals, string(x))
    end
    buf = Vector{UInt8}(join(vals, "\n") * "\n")
    spans = Tuple{Int,Int}[]
    s = 1
    for v in vals; push!(spans, (s, s + ncodeunits(v) - 1)); s += ncodeunits(v) + 1; end
    buf, spans
end
pfMain(b, i, j) = begin
    v, rc, done = V._parsefloat_core(b, i, j, 0x2e)
    done && return (v, rc)
    return (V._sdc(b, i, j, v < 0, 0x2e), V.RC_OK)
end
function run()
    buf, spans = corpus()
    g1(b, ss) = (a = 0.0; for (i, j) in ss; v, _, _ = V._parsefloat_core(b, i, j, 0x2e); a += v; end; a)
    g2(b, ss) = (a = 0.0; for (i, j) in ss; v, _ = pfMain(b, i, j); a += v; end; a)
    g3(b, ss) = (a = 0.0; for (i, j) in ss; v, _ = V.parsefloat64(b, i, j, 0x2e); a += v; end; a)
    g1(buf, spans); g2(buf, spans); g3(buf, spans)
    println("core only:            ", round((@elapsed g1(buf, spans)) / length(spans) * 1e9, digits=1))
    println("Main wrapper (core+sdc): ", round((@elapsed g2(buf, spans)) / length(spans) * 1e9, digits=1))
    println("module parsefloat64:  ", round((@elapsed g3(buf, spans)) / length(spans) * 1e9, digits=1))
end
run()

# KNOWN ISSUE (priority for next review round): with the tier-3 _sdc call
# reachable, values taking the Eisel-Lemire path pay ~175ns extra (204 vs 32ns
# core-only). Survives @noinline on _sdc AND Base.inferencebarrier at the call
# site; a signature-identical allocating @noinline dummy callee shows NO
# penalty (27.6ns). Tier-1 (Clinger) values are unaffected (12.8ns measured).
# _sdc's inferred effects include unknown-termination (!t) from its
# data-dependent digit loops — suspected but not proven to be the mechanism.
# Likely needs a minimized upstream Julia issue once understood.
