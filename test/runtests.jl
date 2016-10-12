using CSV
Pkg.checkout("DataStreams")
using Base.Test, DataFrames, DataStreams, NullableArrays, WeakRefStrings, Libz, DecFP

include("parsefields.jl")
include("io.jl")

dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = joinpath(Pkg.dir("CSV"), "test/test_files")

include("source.jl")

Pkg.clone("https://github.com/JuliaData/DataStreamsIntegrationTests")
using DataStreamsIntegrationTests

include("datastreams.jl")
