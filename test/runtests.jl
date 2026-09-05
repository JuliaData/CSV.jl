# CSV.jl test suite.
#
#   kernel.jl  — structural index, scanners, driver, columns, pooling (exhaustive
#                chunk-geometry determinism)
#   api.jl     — File/read/Rows/Chunks/sniff behavior and mode consistency
#   write.jl   — the writer, including round trips and exact output checks
#   fuzz.jl    — deterministic malformed and generated-input checks
#   scan.jl    — Tables.Scan pushdown (required by Tables 1.14)
using Test, CSV

@testset "CSV" begin
    include("kernel.jl")
    include("api.jl")
    include("decimals.jl")
    include("write.jl")
    include("fuzz.jl")
    include("scan.jl")
end
