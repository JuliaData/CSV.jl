# Load the frozen CSV 0.10 oracle as source. The test project supplies its
# dependencies, and the guard keeps test/legacy/generated.jl safe when it
# includes bench_matrix.jl after the oracle is already loaded.
if !isdefined(@__MODULE__, :LegacyCSV)
    include(joinpath(@__DIR__, "..", "test", "LegacyCSV", "src", "LegacyCSV.jl"))
end
const LEGACYCSV_VERSION = v"0.10.16"
