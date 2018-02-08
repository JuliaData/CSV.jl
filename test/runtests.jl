using Missings
using CSV
using DataStreams, WeakRefStrings, CategoricalArrays
using DataFrames
@static if VERSION < v"0.7.0-DEV.2005"
    using Base.Test
else
    using Test
end
if VERSION < v"0.7.0-DEV.2575"
    using Base.Dates
else
    using Dates
end

const dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = joinpath(Pkg.dir("CSV"), "test/test_files")

@testset "CSV" begin

include("parsefields.jl")
include("io.jl")

include("source.jl")
include("sink.jl")
include("multistream.jl")
include("validate.jl")

end
