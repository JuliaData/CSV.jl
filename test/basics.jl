@testset "basics" begin

@test_throws ArgumentError CSV.File(IOBuffer("a\0b\n1\02\n"); delim='\0')
@test_throws ArgumentError CSV.File(IOBuffer("a\0b\n1\02\n"); delim="\0")

f = CSV.File(IOBuffer("a,b\n1,2\n"); header=3)
@test isempty(f.types)
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); datarow=3)
@test f.names == [:a, :b]
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); skipto=3)
@test f.names == [:a, :b]
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); limit=0)
@test f.names == [:a, :b]
@test f.rows == 0

# delim same as quotechar
f = CSV.File(IOBuffer("a\"b\n1\"2\n"); delim='"')
@test f.rows == 1
@test f.cols == 2
@test f.types == [Int64, Int64]

# delim same as quotechar w/ quoted field
f = CSV.File(IOBuffer("a\"b\n1\"\"2\"\n"); delim='"')
@test f.rows == 1
@test f.cols == 2
@test f.types == [Int64, Int64]

# 387
f = CSV.File(IOBuffer("a,b\n1,1\n1,2\n1,3\n"))

for rows in Iterators.partition(f, 2)
    for row in rows
        # get last property
        getproperty(row, :b)
        @test row.a == 1
    end
end

# 391
rows = collect(f)
@test rows[1].a == 1
@test rows[1].a == 1

# 388
df = CSV.read(joinpath(dir, "GSM2230757_human1_umifm_counts.csv"))
@test size(df) == (3, 20128)

# 386
f = CSV.File(IOBuffer("fullVisitorId,PredictedLogRevenue\n18966949534117,0\n39738481224681,0\n"), limit=3)
@test f.rows == 2
rows = collect(f)
@test rows[1].fullVisitorId == 18966949534117
@test rows[2].fullVisitorId == 39738481224681

df = CSV.read(IOBuffer("col1,col2\n1.0,hi"), limit=3)
@test size(df) == (1, 2)

# 288
df = CSV.read(IOBuffer("x\n\",\"\n\",\""))
@test size(df) == (2, 1)

end