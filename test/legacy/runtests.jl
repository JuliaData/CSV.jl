# The 0.10 corpus, replayed as a differential battery against the 0.10
# implementation (LegacyCSV). See AUDIT.md for what was kept, inlined,
# regenerated, or dropped, and why.
include("harness.jl")
include("corpus_table.jl")
@testset "legacy corpus" begin
    @testset "LegacyCSV shim isolation" begin
        @test LegacyCSV.CSV === LegacyCSV
        @test pathof(LegacyCSV.CSV) == pathof(LegacyCSV)
        workload = Base.read(joinpath(LegacyCSV._LEGACY_SRC, "workload.jl"), String)
        rewritten = LegacyCSV._rewritesource(workload)
        @test !occursin("dirname(pathof(CSV))", rewritten)
        @test occursin("_LEGACY_SRC", rewritten)
        @test !occursin(r"\bMain\.", join((Base.read(joinpath(LegacyCSV._LEGACY_SRC, f), String)
                                            for f in readdir(LegacyCSV._LEGACY_SRC)
                                            if endswith(f, ".jl")), '\n'))
    end
    include("detector_fuzz.jl")
    @testset "corpus table (testfiles.jl)" begin
        for (i, entry) in enumerate(CORPUS_TABLE)
            file = entry[1]
            lbl = file isa IO ? "table:io#$i" : "table:" * file
            @case lbl corpuscase(entry...; label=lbl)
        end
    end
    include("cases_file.jl")
    report()
end
