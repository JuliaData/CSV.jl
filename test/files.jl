using Dates, Tables
using DataStructures: OrderedDict

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
tbl |> CSV.write(io; delim='\t')
seekstart(io)
f = CSV.File(io; delim='\t', allowmissing=:auto)
@test (f |> columntable) == tbl

# #172
@test_throws ArgumentError CSV.File(joinpath(dir, "test_newline_line_endings.csv"), types=Dict(1=>Integer))

# #289
tmp = CSV.File(IOBuffer(" \"a, b\", \"c\" "), datarow=1) |> DataFrame
@test size(tmp) == (1, 2)
@test tmp.Column1[1] == "a, b"
@test tmp.Column2[1] == "c"

tmp = CSV.File(IOBuffer(" \"2018-01-01\", \"1\" ,1,2,3"), datarow=1) |> DataFrame
@test size(tmp) == (1, 5)
@test tmp.Column1[1] == Date(2018, 1, 1)
@test tmp.Column2[1] == 1
@test tmp.Column3[1] == 1
@test tmp.Column4[1] == 2
@test tmp.Column5[1] == 3

# #329
df = CSV.read(joinpath(dir, "test_types.csv"), types=Dict(:string=>Union{Missing,DateTime}), silencewarnings=true)
@test df.string[1] === missing

# #352
@test_throws ArgumentError first(CSV.File(joinpath(dir, "test_types.csv"))).a

# @time f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none);
# @time t = f |> columntable;
# @time t = Tables.buildcolumns(nothing, Tables.rows(f));
# using Profile
# Profile.clear()
# @profile f |> columntable;
# Profile.print(C=true)
# Profile.print()

# Test that using an OrderedDict for types doesn't error #373
df = CSV.read(joinpath(dir, "test_types.csv"), types=OrderedDict(:string=>Union{Missing,DateTime}))
@test df.string[1] === missing

end # testset
