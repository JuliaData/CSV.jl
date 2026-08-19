# Tier-1 value-layer microbenchmark: ns/value for the new kernels vs legacy
# Parsers.xparse vs Base.parse, across a value-shape grid. A kernel earns its
# place in the hot path only when this table says so.
#
# Run:  julia --project=test bench/valuebench.jl

using CSV; const KernelValues = CSV.CSVKernel.KernelValues; const KernelValuesDates = CSV.CSVKernel.KernelValuesDates
using .KernelValues, .KernelValuesDates
using Parsers, Dates, Random
const V = KernelValues

# Each case: many same-shape values concatenated; we time parsing all of them
# through precomputed spans so the measurement is pure value-parse cost.
function _group3(s::String)
    out = IOBuffer()
    n = length(s)
    for (k, c) in enumerate(s)
        print(out, c)
        r = n - k
        r > 0 && r % 3 == 0 && print(out, ',')
    end
    return String(take!(out))
end

function makecorpus(gen, n)
    rng = MersenneTwister(1234)
    vals = [gen(rng, i) for i in 1:n]
    buf = Vector{UInt8}(join(vals, '\n') * "\n")
    spans = Tuple{Int, Int}[]
    s = 1
    for v in vals
        push!(spans, (s, s + ncodeunits(v) - 1))
        s += ncodeunits(v) + 1
    end
    return buf, spans
end

function bench(f, buf, spans; reps=7)
    f(buf, spans)  # warmup
    best = Inf
    for _ in 1:reps
        t = @elapsed f(buf, spans)
        best = min(best, t)
    end
    return best / length(spans) * 1e9   # ns/value
end

fmt(x) = lpad(round(x, digits=1), 8)

const OPTS = Parsers.Options()
const GOPTS = Parsers.Options(groupmark=',', delim=';')
const DFD = Dates.default_format(Date)

function main()
    n = 100_000
    println(rpad("shape", 22), lpad("kernel", 9), lpad("xparse", 9), lpad("Base", 9), "   (ns/value)")
    println("─"^58)
    cases = [
        ("int 1-4 digits",   (rng, i) -> string(rand(rng, -9999:9999)),          :int),
        ("int 5-9 digits",   (rng, i) -> string(rand(rng, 10_000:999_999_999)),  :int),
        ("int 10-18 digits", (rng, i) -> string(rand(rng, Int64(10)^10:Int64(10)^17)), :int),
        ("float short (x.y)",(rng, i) -> string(round(rand(rng) * 1000, digits=3)), :float),
        ("float shortest",   (rng, i) -> string(reinterpret(Float64, rand(rng, UInt64) & 0x7fefffffffffffff)), :float),
        ("float subnormal",  (rng, i) -> string(reinterpret(Float64, max(rand(rng, UInt64) & 0x000fffffffffffff, UInt64(1)))), :float),
        ("float exp form",   (rng, i) -> string(rand(rng, 1:999)) * "." * string(rand(rng, 0:99)) * "e" * string(rand(rng, -30:30)), :float),
        ("int grouped",      (rng, i) -> _group3(string(rand(rng, 1_000_000:999_999_999))), :intg),
        ("bigint 30 digits", (rng, i) -> String(rand(rng, '1':'9', 30)), :bigint),
        ("bigfloat (256b)",  (rng, i) -> string(rand(rng, 1:999999)) * "." * String(rand(rng, '0':'9', 40)) * "e" * string(rand(rng, -100:100)), :bigfloat),
        ("uuid",             (rng, i) -> string(Base.UUID(rand(rng, UInt128))), :uuid),
        ("date ISO",         (rng, i) -> string(Date(2020, 1, 1) + Day(rand(rng, 0:2000))), :date),
        ("date ISO (interp)", (rng, i) -> string(Date(2020, 1, 1) + Day(rand(rng, 0:2000))), :dateinterp),
        ("datetime ISO",     (rng, i) -> string(DateTime(2020, 1, 1) + Second(rand(rng, 0:10^7))), :datetime),
        ("bool",             (rng, i) -> rand(rng, ("true", "false")), :bool),
        ("string unquoted",  (rng, i) -> String(rand(rng, 'a':'z', rand(rng, 3:14))), :str),
        ("string quoted",    (rng, i) -> "\"" * String(rand(rng, 'a':'z', 8)) * ", " * String(rand(rng, 'a':'z', 6)) * "\"", :str),
    ]
    for (name, gen, kind) in cases
        buf, spans = makecorpus(gen, n)
        tk = tx = tb = NaN
        if kind === :int
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; v, rc = V.parseint64(b, i, j); a += v; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(Int64, b, i, j, OPTS); a += r.val; end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0; for (i, j) in ss; a += parse(Int64, String(b[i:j])); end; a), buf, spans)
        elseif kind === :intg
            ko = kernelgroupopts()
            scratch = Vector{UInt8}(undef, 64)
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; v, ok = MainK.parsevalue(Int64, b, i, j, ko, scratch); a += v; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(Int64, b, i, j, GOPTS); a += r.val; end; a), buf, spans)
        elseif kind === :bigint
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; v, rc = V.parsebigint(b, i, j); a += v % Int64; end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0; for (i, j) in ss; a += parse(BigInt, String(b[i:j])) % Int64; end; a), buf, spans)
        elseif kind === :bigfloat
            ws = V.BigWork()
            tk = bench((b, ss) -> (a = 0.0; for (i, j) in ss; v, rc = V.parsebigfloat(b, i, j, UInt8('.'), ws); a += Float64(v); end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0.0; for (i, j) in ss; a += Float64(parse(BigFloat, String(b[i:j]))); end; a), buf, spans)
        elseif kind === :uuid
            tk = bench((b, ss) -> (a = UInt128(0); for (i, j) in ss; v, rc = V.parseuuid(b, i, j); a ⊻= v; end; a), buf, spans)
            tb = bench((b, ss) -> (a = UInt128(0); for (i, j) in ss; a ⊻= Base.tryparse(Base.UUID, String(b[i:j])).value; end; a), buf, spans)
        elseif kind === :float
            tk = bench((b, ss) -> (a = 0.0; for (i, j) in ss; v, rc = V.parsefloat64(b, i, j); a += v; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0.0; for (i, j) in ss; r = Parsers.xparse(Float64, b, i, j, OPTS); a += r.val; end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0.0; for (i, j) in ss; a += parse(Float64, String(b[i:j])); end; a), buf, spans)
        elseif kind === :date
            # the kernel's column path: fixed-width fast path (parsecivil agrees on every input)
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; c, rc = V.parseiso10(b, i); a += c.day; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(Date, b, i, j, OPTS); a += Dates.day(r.val); end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0; for (i, j) in ss; a += Dates.day(Date(String(b[i:j]), DFD)); end; a), buf, spans)
        elseif kind === :dateinterp
            # the format-program interpreter (custom dateformats take this path)
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; c, rc = V.parsecivil(b, i, j, V.ISO_DATE); a += c.day; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(Date, b, i, j, OPTS); a += Dates.day(r.val); end; a), buf, spans)
            tb = bench((b, ss) -> (a = 0; for (i, j) in ss; a += Dates.day(Date(String(b[i:j]), DFD)); end; a), buf, spans)
        elseif kind === :datetime
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; c, rc = V.parseiso19(b, i); a += c.second; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(DateTime, b, i, j, OPTS); a += Dates.second(r.val); end; a), buf, spans)
        elseif kind === :bool
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; v, rc = V.parsebool(b, i, j); a += v; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = Parsers.xparse(Bool, b, i, j, OPTS); a += r.val; end; a), buf, spans)
        else # :str — content discovery (the CompactString payload feed)
            q = UInt8('"')
            tk = bench((b, ss) -> (a = 0; for (i, j) in ss; c, l, e, rc = V.findcontent(b, i, j, q, q, q); a += l; end; a), buf, spans)
            tx = bench((b, ss) -> (a = 0; for (i, j) in ss; r = K_xparsestring(b, i, j); a += Int(r.val.len); end; a), buf, spans)
        end
        println(rpad(name, 22), fmt(tk), fmt(tx), isnan(tb) ? lpad("—", 9) : fmt(tb))
    end
end

# a minimal xparse-based string probe mirroring what the kernel used to do
K_xparsestring(b, i, j) = Parsers.xparse(String, b, i, j, OPTS, Parsers.PosLen31)

# the kernel's grouped path goes through core's parsevalue + ValueOpts
using CSV; const CSVKernel = CSV.CSVKernel
const MainK = CSVKernel
kernelgroupopts() = MainK.makevalueopts(MainK.Dialect(delim=';'); groupmark=',')

main()
