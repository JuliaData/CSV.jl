using Tables

include("testfiles/testfiles.jl")

@testset "CSV.File" begin

for test in testfiles
    testfile(test...)
end

#test on non-existent file
@test_throws ArgumentError CSV.File("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.File(joinpath(dir, "test_no_header.csv"); datarow=1, header=2);

#test bad types
f = CSV.File(joinpath(dir, "test_float_in_int_column.csv"); types=[Int, Int, Int], strict=true)
@test_throws CSV.Error (f |> columntable)

# Integer overflow; #100
@test_throws CSV.Error (CSV.File(joinpath(dir, "int8_overflow.csv"); types=[Int8], strict=true) |> columntable)

# #137
tbl = (a=[11,22], dt=[Date(2017,12,7), Date(2017,12,14)], dttm=[DateTime(2017,12,7), DateTime(2017,12,14)])
io = IOBuffer()
CSV.write(tbl, io; delim='\t')
seekstart(io)
f = CSV.File(io; delim='\t', allowmissing=:auto)
@test (f |> columntable) == tbl

# #172
@test_throws ArgumentError CSV.File(joinpath(dir, "test_newline_line_endings.csv"), types=Dict(1=>Integer))

# @time f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none);
# @time f |> columntable;
# using Profile
# Profile.clear()
# @profile f |> columntable;
# Profile.print(C=true)
# Profile.print()

end # testset
