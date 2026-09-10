# Writer hot spots: GC share, scaling, and flat self-time profile.
using CSV, Profile, Random, Dates, Printf
include(joinpath(@__DIR__, "writeshapes.jl"))
Profile.init(n=10_000_000, delay=0.0005)
function timed(f; reps=5)
    f()
    best = (t=Inf, gc=0.0, bytes=0)
    for _ in 1:reps
        GC.gc()
        r = @timed f()
        r.time < best.t && (best = (t=r.time, gc=r.gctime, bytes=r.bytes))
    end
    return best
end
for kind in (:numeric, :mixed, :strings, :datetime, :wide)
    n = kind === :wide ? 100_000 : 1_000_000
    tbl = shape(kind, n, MersenneTwister(7))
    io = IOBuffer(); CSV.write(io, tbl); bytes = take!(io); mb = length(bytes) / 2^20
    for nt in (1, Threads.nthreads())
        f = () -> CSV.write(IOBuffer(), tbl; ntasks=nt)
        b = timed(f)
        @printf("%-9s ntasks=%2d  %7.1f ms  gc=%5.1f%%  alloc=%6.1f MiB  %6.0f MiB/s\n",
                kind, nt, b.t * 1e3, 100 * b.gc / b.t, b.bytes / 2^20, mb / b.t)
    end
    # sink kinds at full threads
    for (nm, mk) in (("IOBuffer", () -> IOBuffer()), ("devnull", () -> devnull),
                     ("path", () -> tempname()))
        f = () -> CSV.write(mk(), tbl)
        b = timed(f; reps=3)
        @printf("%-9s sink=%-8s %7.1f ms  gc=%5.1f%%  %6.0f MiB/s\n", kind, nm, b.t * 1e3, 100 * b.gc / b.t, mb / b.t)
    end
    if kind in (:numeric, :strings)
        f1 = () -> CSV.write(devnull, tbl; ntasks=1)
        f1()
        Profile.clear(); GC.gc()
        @profile for _ in 1:3; f1(); end
        io = IOBuffer()
        Profile.print(IOContext(io, :displaysize => (60, 250)); format=:flat, sortedby=:overhead, C=true, combine=true, noisefloor=0, mincount=0)
        lines = split(String(take!(io)), '\n')
        hdr = findfirst(l -> occursin("Overhead", l), lines)
        println("=== profile ", kind, " ntasks=1")
        hdr === nothing || foreach(println, reverse(filter(l -> !isempty(strip(l)), lines[hdr+2:end])[max(1, end - 30):end]))
        println()
    end
    flush(stdout)
end
