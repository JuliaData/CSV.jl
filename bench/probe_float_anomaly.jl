using CSV
const V = CSV.CSVKernel.KernelValues
using Random
function corpus(; normalonly=false)
    rng = MersenneTwister(7)
    vals = String[]
    while length(vals) < 100_000
        bits = rand(rng, UInt64)
        exponent = (bits >> 52) & 0x7ff
        exponent == 0x7ff && continue
        normalonly && exponent == 0 && continue
        x = reinterpret(Float64, bits)
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
    g1(b, ss) = (a = 0.0; for (i, j) in ss; v, _, _ = V._parsefloat_core(b, i, j, 0x2e); a += v; end; a)
    g2(b, ss) = (a = 0.0; for (i, j) in ss; v, _ = pfMain(b, i, j); a += v; end; a)
    g3(b, ss) = (a = 0.0; for (i, j) in ss; v, _ = V.parsefloat64(b, i, j, 0x2e); a += v; end; a)
    for normalonly in (false, true)
        buf, spans = corpus(; normalonly)
        slow = count(spans) do (i, j)
            !V._parsefloat_core(buf, i, j, 0x2e)[3]
        end
        g1(buf, spans); g2(buf, spans); g3(buf, spans)
        label = normalonly ? "normal-only" : "all finite"
        println(label, ": tier-3 entries=", slow)
        println("  core only:              ", round((@elapsed g1(buf, spans)) / length(spans) * 1e9, digits=1))
        println("  Main wrapper (core+sdc): ", round((@elapsed g2(buf, spans)) / length(spans) * 1e9, digits=1))
        println("  module parsefloat64:    ", round((@elapsed g3(buf, spans)) / length(spans) * 1e9, digits=1))
    end
end
run()

# This probe originally appeared to show a compiler reachability penalty. The
# corpus actually contained about 1/2048 subnormals, and `_eisel_lemire`
# rejected all of them. Each real `_sdc` conversion then ran roughly 1,000
# decimal scaling loops. Once Eisel-Lemire performs its standard subnormal
# shift, both corpora have zero tier-3 entries and the wrapper stays at the
# composed-pipeline floor. There is no Julia compiler issue to file.
