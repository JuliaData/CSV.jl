struct CSV_Foo
end

@testset "CSV.File basics" begin

#test on non-existent file
@test_throws ArgumentError CSV.File("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.File(joinpath(dir, "test_no_header.csv"); datarow=1, header=2);

#test bad types
@test_throws CSV.Error CSV.File(joinpath(dir, "test_float_in_int_column.csv"); types=[Int, Int, Int], strict=true)

# Integer overflow; #100
@test_throws CSV.Error CSV.File(joinpath(dir, "int64_overflow.csv"); types=[Int8], strict=true)

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
@test f.rows == 0
@test f.cols == 2
@test f.types == [Missing, Missing]

# delim same as quotechar w/ quoted field
f = CSV.File(IOBuffer("a\"b\n1\"\"2\"\n"); delim='"')
@test f.rows == 0
@test f.cols == 2
@test f.types == [Missing, Missing]

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

# 459
rows = collect(CSV.File(joinpath(dir, "time.csv"); dateformat="H:M:S"))
@test rows[1].time == Time(0)
@test rows[2].time == Time(0, 10)

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
df = CSV.read(IOBuffer("x\n\na\n"), pool=true)
@test size(df) == (2, 1)
@test df.x[1] === missing
@test df.x[2] == "a"

# catg => missing
df = CSV.read(IOBuffer("x\na\n\n"), pool=true)
@test size(df) == (2, 1)
@test df.x[1] == "a"
@test df.x[2] === missing

# catg => string
df = CSV.read(IOBuffer("x\na\nb\na\nb\na\nb\na\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nn\nm\no\np\nq\nr\n"), pool=0.5)
@test typeof(df.x) == CSV.Column{String, String}

# a few corner cases for escape strings
df = CSV.read(IOBuffer("\"column name with \"\" escape character inside\"\n1\n"))
@test names(df)[1] == Symbol("column name with \" escape character inside")

df = CSV.read(IOBuffer("\"column name with \"\" escape character inside\",1\n,2"), transpose=true)
@test names(df)[1] == Symbol("column name with \" escape character inside")
@test names(df)[2] == :Column2

df = CSV.read(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true)
@test df.x[1] == "a"
@test df.x[3] == "quoted field with \" escape character inside"

df = CSV.read(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true)
@test df.x[1] == "a"
@test df.x[3] == "quoted field with \" escape character inside"

# invalid quoted field is fatal error
@test_throws CSV.Error CSV.read(IOBuffer("x\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.read(IOBuffer("x\nhey\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.read(IOBuffer("x\n\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.read(IOBuffer("x\n1\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.read(IOBuffer("x\n1.0\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.read(IOBuffer("x\na\n\"quoted field that never ends"), pool=true)

# invalid integer
df = CSV.read(IOBuffer("x\nabc\n"), type=Int)
@test size(df) == (1, 1)
@test df.x[1] === missing

@test_throws CSV.Error CSV.read(IOBuffer("x\nabc\n"), type=Int, strict=true)

# transpose corner cases
df = CSV.read(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=2)
@test names(df) == [:y, :y2]
@test size(df) == (1, 2)
@test df.y[1] == 1
@test df.y2[1] == 2

df = CSV.read(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=1, datarow=3)
@test names(df) == [:x, :x2]
@test size(df) == (1, 2)
@test df.x[1] == 1
@test df.x2[1] == 2

df = CSV.read(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=false, datarow=3)
@test names(df) == [:Column1, :Column2]
@test size(df) == (1, 2)
@test df.Column1[1] == 1
@test df.Column2[1] == 2

df = CSV.read(IOBuffer(""), transpose=true, header=false)
@test size(df) == (0, 0)

df = CSV.read(IOBuffer(""), transpose=true, header=Symbol[])
@test size(df) == (0, 0)

# providing empty header vector
df = CSV.read(IOBuffer("x\nabc\n"), header=Symbol[])
@test names(df) == [:Column1]

# Union{Bool, Missing}
df = CSV.read(IOBuffer("x\ntrue\n\n"))
@test size(df) == (2, 1)
@test df.x[1] === true
@test df.x[2] === missing

# Union{Date, Missing}
df = CSV.read(IOBuffer("x\n2019-01-01\n\n"))
@test size(df) == (2, 1)
@test df.x[1] === Date(2019, 1, 1)
@test df.x[2] === missing

# use_mmap=false
df = CSV.read(IOBuffer("x\n2019-01-01\n\n"), use_mmap=false)
@test size(df) == (2, 1)
@test df.x[1] === Date(2019, 1, 1)
@test df.x[2] === missing

# types is Dict{String, Type}
df = CSV.read(IOBuffer("x\n2019-01-01\n\n"), types=Dict("x"=>Date))
@test size(df) == (2, 1)
@test df.x[1] === Date(2019, 1, 1)
@test df.x[2] === missing

# various CSV.File/CSV.Row properties
f = CSV.File(IOBuffer("int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"), pool=0.3)
@test Tables.istable(f)
@test Tables.rowaccess(typeof(f))
@test Tables.columnaccess(typeof(f))
@test Tables.schema(f) == Tables.Schema([:int, :float, :date, :datetime, :bool, :null, :str, :catg, :int_float], [Int64, Float64, Date, DateTime, Bool, Missing, String, String, Float64])
@test Tables.rows(f) === f
@test eltype(f) == CSV.Row
row = first(f)
@test propertynames(row) == [:int, :float, :date, :datetime, :bool, :null, :str, :catg, :int_float]
@test row.int == 1
@test row.float == 3.14
@test row.date == Date(2019, 1, 1)
@test row.datetime == DateTime(2019, 1, 1, 1, 2, 3)
@test row.bool == true
@test row.null === missing
@test row.str == "hey"
@test row.catg == "abc"
@test typeof(row.catg) == String
@test row.int_float === 2.0
row = iterate(f, 2)[1]
@test row.int_float === 3.14

# 448
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"), ignorerepeated=true)

# reported by oxinabox on slack; issue w/ manually specified pool column type and 0 rows
df = CSV.read(IOBuffer("x\n"), types=[CSV.PooledString], copycols=true)
@test size(df) == (0, 1)

df = CSV.read(IOBuffer("x\n"), types=[Union{CSV.PooledString, Missing}], copycols=true)
@test size(df) == (0, 1)

f = CSV.File(IOBuffer("x\n1\n2\n3\n#4"), comment="#")
@test length(f.x) == 3
@test f.x[end] == 3

# 453
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"), types=[CSV_Foo])
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"), types=Dict(:x=>CSV_Foo))

# 447
df = CSV.read(IOBuffer("a,b,c\n1,2,3\n\n"), ignoreemptylines=true)
@test size(df) == (1, 3)

df = CSV.read(IOBuffer("zip\n11111-1111\n"), dateformat = "y-m-dTH:M:S.s")
@test size(df) == (1, 1)

# Supporting commands across multiple platforms cribbed from julia/test/spawn.jl
catcmd = `cat`
havebb = false
if Sys.iswindows()
    busybox = download("https://frippery.org/files/busybox/busybox.exe", joinpath(tempdir(), "busybox.exe"))
    havebb = try # use busybox-w32 on windows, if available
        success(`$busybox`)
        true
    catch
        false
    end
    if havebb
        catcmd = `$busybox cat`
    end
end
@test CSV.read(`$(catcmd) $(joinpath(dir, "test_basic.csv"))`) == CSV.read(joinpath(dir, "test_basic.csv"))

#476
df = CSV.read(GzipDecompressorStream(open(joinpath(dir, "randoms.csv.gz"))))
@test size(df) == (70000, 7)

end
