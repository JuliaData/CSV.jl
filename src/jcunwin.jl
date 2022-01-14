#==
Example code to demonstrate an issue when writing an array of `struct` values to a CSV.
==#
using Dates
using CSV

struct StructType
    adate::Date
    astring::Union{String, Nothing}
    anumber::Union{Real, Nothing}
end

structtable = [StructType(Date("2021-12-01"), "string 1", 123.45), StructType(Date("2021-12-31"), "string 2", 456.78)]

io = IOBuffer()

CSV.write(io, structtable)

println(String(take!(io)))

# Throws exception with unfixed `write.jl`
CSV.write(io, structtable; header=["Date Column", "String Column", "Number Column"])

println(String(take!(io)))
