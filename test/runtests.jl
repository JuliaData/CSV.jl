using CSV
using DataStreams, Missings, WeakRefStrings, CategoricalArrays
@static if VERSION < v"0.7.0-DEV.2005"
    using Base.Test
else
    using Test
end
include("parsefields.jl")
include("io.jl")

dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = joinpath(Pkg.dir("CSV"), "test/test_files")

include("source.jl")
include("multistream.jl")
include("validate.jl")