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

# delimiter auto-detection
df = CSV.read(IOBuffer("x\n1\n2\n"))
@test size(df) == (2, 1)

df = CSV.read(IOBuffer("x,y\n1,\n2,\n"))
@test size(df) == (2, 2)

df = CSV.read(IOBuffer("x y\n1 \n2 \n"))
@test size(df) == (2, 2)

df = CSV.read(IOBuffer("x\ty\n1\t\n2\t\n"))
@test size(df) == (2, 2)

df = CSV.read(IOBuffer("x|y\n1|\n2|\n"))
@test size(df) == (2, 2)

# type promotion
# int => float
df = CSV.read(IOBuffer("x\n1\n3.14"))
@test size(df) == (2, 1)
@test df.x[1] === 1.0
@test df.x[2] === 3.14

# int => missing
df = CSV.read(IOBuffer("x\n1\n\n"))
@test size(df) == (2, 1)
@test df.x[1] == 1
@test df.x[2] === missing

# missing => int
df = CSV.read(IOBuffer("x\n\n1\n"))
@test size(df) == (2, 1)
@test df.x[1] === missing
@test df.x[2] == 1

# missing => int => float
df = CSV.read(IOBuffer("x\n\n1\n3.14\n"))
@test size(df) == (3, 1)
@test df.x[1] === missing
@test df.x[2] === 1.0
@test df.x[3] === 3.14

# int => missing => float
df = CSV.read(IOBuffer("x\n1\n\n3.14\n"))
@test size(df) == (3, 1)
@test df.x[1] === 1.0
@test df.x[2] === missing
@test df.x[3] === 3.14

# int => float => missing
df = CSV.read(IOBuffer("x\n1\n3.14\n\n"))
@test size(df) == (3, 1)
@test df.x[1] === 1.0
@test df.x[2] === 3.14
@test df.x[3] === missing

# int => string
df = CSV.read(IOBuffer("x\n1\nabc"))
@test size(df) == (2, 1)
@test df.x[1] == "1"
@test df.x[2] == "abc"

# float => string
df = CSV.read(IOBuffer("x\n3.14\nabc"))
@test size(df) == (2, 1)
@test df.x[1] == "3.14"
@test df.x[2] == "abc"

# int => float => string
df = CSV.read(IOBuffer("x\n1\n3.14\nabc"))
@test size(df) == (3, 1)
@test df.x[1] == "1"
@test df.x[2] == "3.14"
@test df.x[3] == "abc"

# missing => int => float => string
df = CSV.read(IOBuffer("x\n\n1\n3.14\nabc"))
@test size(df) == (4, 1)
@test df.x[1] === missing
@test df.x[2] == "1"
@test df.x[3] == "3.14"
@test df.x[4] == "abc"

# missing => catg
df = CSV.read(IOBuffer("x\n\na\n"), categorical=true)
@test size(df) == (2, 1)
@test df.x[1] === missing
@test df.x[2] == "a"

# catg => missing
df = CSV.read(IOBuffer("x\na\n\n"), categorical=true)
@test size(df) == (2, 1)
@test df.x[1] == "a"
@test df.x[2] === missing

# catg => string
df = CSV.read(IOBuffer("x\na\nb\na\nb\na\nb\na\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nn\nm\no\np\nq\nr\n"), categorical=0.5)
@test typeof(df.x) == StringArray{String,1}

end
