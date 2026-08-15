# CSV.jl test suite.
#
#   kernel.jl  — structural index, scanners, driver, columns, pooling (exhaustive
#                chunk-geometry determinism)
#   values.jl  — the value layer (ints, floats, dates, bools) incl. Ryu/strtod pins
#   api.jl     — File/read/Rows/Chunks/sniff differential vs the 0.10 implementation
#                (LegacyCSV = the frozen legacy/src, loaded as the oracle)
#   write.jl   — the writer, incl. round-trips through File and byte parity
#   scan.jl    — Tables.Scan pushdown (only when the Tables proposal is loaded)
#   legacy/    — ported 0.10 corpus tests (see legacy/README.md for the audit)
using Test, CSV
@testset "CSV" begin
    include("kernel.jl")
    include("values.jl")
    include("api.jl")
    include("write.jl")
    isdefined(CSV.Tables, :Scan) && include("scan.jl")
    include("legacy/runtests.jl")
end
