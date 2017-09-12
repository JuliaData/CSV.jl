using CSV
using Base.Test, DataStreams, Nulls, WeakRefStrings

include("parsefields.jl")
include("io.jl")

dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = joinpath(Pkg.dir("CSV"), "test/test_files")

include("source.jl")
include("multistream.jl")
include("validate.jl")