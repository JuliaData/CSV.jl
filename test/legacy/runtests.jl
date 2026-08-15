# The 0.10 corpus, replayed as a differential battery against the 0.10
# implementation (LegacyCSV). See AUDIT.md for what was kept, inlined,
# regenerated, or dropped, and why.
include("harness.jl")
include("corpus_table.jl")
@testset "legacy corpus" begin
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
