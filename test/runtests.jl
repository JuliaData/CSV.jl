using CSV, DataStreams, DataFrames, Test, Dates, WeakRefStrings, CategoricalArrays

const dir = joinpath(dirname(@__FILE__),"test_files/")
# dir = "/Users/jacobquinn/.julia/dev/CSV/test/test_files"

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

include("io.jl")
include("source.jl")
include("sink.jl")
include("multistream.jl")
include("validate.jl")

end
