using CSV
using Base.Test, DataStreams, Nulls, DecFP, WeakRefStrings, Libz

include("parsefields.jl")
include("io.jl")

dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = joinpath(Pkg.dir("CSV"), "test/test_files")

include("source.jl")
include("multistream.jl")

using DataStreamsIntegrationTests

include("datastreams.jl")
