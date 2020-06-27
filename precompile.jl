# generates precompile.jl files
using CSV, Parsers, SentinelArrays, SnoopCompile

const dir = joinpath(dirname(pathof(CSV)), "..", "test", "testfiles")

timings = @snoopi tmin=0.001 begin

f = CSV.File(joinpath(dir, "precompile.csv"))
f = CSV.File(joinpath(dir, "precompile_small.csv"))

end # @snoopi

pc = SnoopCompile.parcel(timings)
precom = joinpath(dirname(pathof(CSV)), "..", "precompile")
SnoopCompile.write(precom, pc)
mv(joinpath(precom, "precompile_CSV.jl"), joinpath(dirname(pathof(CSV)), "precompile.jl"); force=true)
mv(joinpath(precom, "precompile_Parsers.jl"), joinpath(dirname(pathof(Parsers)), "precompile.jl"); force=true)
mv(joinpath(precom, "precompile_SentinelArrays.jl"), joinpath(dirname(pathof(SentinelArrays)), "precompile.jl"); force=true)
