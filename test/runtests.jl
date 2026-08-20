# CSV.jl test suite.
#
#   kernel.jl  — structural index, scanners, driver, columns, pooling (exhaustive
#                chunk-geometry determinism)
#   values.jl  — the value layer (ints, floats, dates, bools) incl. Ryu/strtod pins
#   api.jl     — File/read/Rows/Chunks/sniff differential vs the 0.10 implementation
#                (LegacyCSV = the frozen legacy/src, loaded as the oracle)
#   write.jl   — the writer, incl. round-trips through File and byte parity
#   scan.jl    — Tables.Scan pushdown (only when the Tables proposal is loaded)
#   legacy/    — ported 0.10 corpus tests (see test/legacy/AUDIT.md)
# LegacyCSV is frozen test source, not a registry dependency. Include its shim
# directly so normal `Pkg.test` works on Julia 1.10 without Project `[sources]`
# or a nested environment whose dependencies are not visible to the loader.
using Test, CSV, LazyArtifacts, SHA
include(joinpath(@__DIR__, "LegacyCSV", "src", "LegacyCSV.jl"))
import .LegacyCSV
const TESTFILES_DIR = artifact"testfiles"

@testset "CSV" begin
    @testset "test fixture artifact" begin
        checksum_file = joinpath(@__DIR__, "artifacts", "testfiles.sha256")
        entries = [split(line, "  "; limit=2) for line in readlines(checksum_file)]
        names = last.(entries)
        @test length(entries) == 24
        @test names == sort(names)
        @test sort(readdir(TESTFILES_DIR)) == names
        for (expected, name) in entries
            @test bytes2hex(sha256(read(joinpath(TESTFILES_DIR, name)))) == expected
        end
    end
    include("kernel.jl")
    include("values.jl")
    include("api.jl")
    include("write.jl")
    include("fuzz.jl")
    isdefined(CSV.Tables, :Scan) && include("scan.jl")
    include("legacy/runtests.jl")
end
