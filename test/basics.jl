struct CSV_Foo
end

struct CSVString
    s::String
end

Base.parse(::Type{CSVString}, s::String) = CSVString(s)
Base.tryparse(::Type{CSVString}, s::String) = CSVString(s)
Base.zero(::Type{CSVString}) = CSVString("")

struct Dec64
    x::Float64
end

Base.parse(::Type{Dec64}, s::String) = Dec64(parse(Float64, s))
Base.tryparse(::Type{Dec64}, s::String) = Dec64(parse(Float64, s))
Base.zero(::Type{Dec64}) = Dec64(0.0)

@testset "CSV.File basics" begin

#test on non-existent file
@test_throws ArgumentError CSV.File("");

#test where skipto > headerrow
@test_throws ArgumentError CSV.File(joinpath(dir, "test_no_header.csv"); skipto=1, header=2);

#test bad types
@test_throws CSV.Error CSV.File(joinpath(dir, "test_float_in_int_column.csv"); types=[Int, Int, Int], strict=true)

# Integer overflow; #100
@test_throws CSV.Error CSV.File(joinpath(dir, "int64_overflow.csv"); types=[Int8], strict=true)

# #172
@test_throws ArgumentError CSV.File(joinpath(dir, "test_newline_line_endings.csv"), types=Dict(1=>Integer))

# #289
tmp = CSV.File(IOBuffer(" \"a, b\", \"c\" "), skipto=1) |> columntable
@test length(tmp) == 2
@test length(tmp[1]) == 1
@test tmp.Column1[1] == "a, b"
@test tmp.Column2[1] == "c"

tmp = CSV.File(IOBuffer(" \"2018-01-01\", \"1\" ,1,2,3"), skipto=1) |> columntable
@test length(tmp) == 5
@test length(tmp[1]) == 1
@test tmp.Column1[1] == Date(2018, 1, 1)
@test tmp.Column2[1] == 1
@test tmp.Column3[1] == 1
@test tmp.Column4[1] == 2
@test tmp.Column5[1] == 3

# #329
f = CSV.File(joinpath(dir, "test_types.csv"), types=Dict(:string=>Union{Missing,DateTime}), silencewarnings=true)
@test f.string[1] === missing

# #352
@test_throws KeyError first(CSV.File(joinpath(dir, "test_types.csv"))).a

@test_throws ArgumentError CSV.File(IOBuffer("a\0b\n1\02\n"); delim='\0')
@test_throws ArgumentError CSV.File(IOBuffer("a\0b\n1\02\n"); delim="\0")

f = CSV.File(IOBuffer("a,b\n1,2\n"); header=3)
@test isempty(f.types)
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); skipto=3)
@test f.names == [:a, :b]
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); skipto=3)
@test f.names == [:a, :b]
@test f.rows == 0

f = CSV.File(IOBuffer("a,b\n1,2\n"); limit=0)
@test f.names == [:a, :b]
@test f.rows == 0

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
f = CSV.File(joinpath(dir, "GSM2230757_human1_umifm_counts.csv"); ntasks=1);
@test length(f.names) == 20128
@test length(f) == 3

# 386
f = CSV.File(IOBuffer("fullVisitorId,PredictedLogRevenue\n18966949534117,0\n39738481224681,0\n"), limit=3)
@test f.rows == 2
rows = collect(f)
@test rows[1].fullVisitorId == 18966949534117
@test rows[2].fullVisitorId == 39738481224681

f = CSV.File(IOBuffer("col1,col2\n1.0,hi"), limit=3)
@test (length(f), length(f.names)) == (1, 2)

# 288
f = CSV.File(IOBuffer("x\n\",\"\n\",\""))
@test (length(f), length(f.names)) == (2, 1)

# delimiter auto-detection
f = CSV.File(IOBuffer("x\n1\n2\n"))
@test (length(f), length(f.names)) == (2, 1)

f = CSV.File(IOBuffer("x,y\n1,\n2,\n"))
@test (length(f), length(f.names)) == (2, 2)

f = CSV.File(IOBuffer("x y\n1 \n2 \n"))
@test (length(f), length(f.names)) == (2, 2)

f = CSV.File(IOBuffer("x\ty\n1\t\n2\t\n"))
@test (length(f), length(f.names)) == (2, 2)

f = CSV.File(IOBuffer("x|y\n1|\n2|\n"))
@test (length(f), length(f.names)) == (2, 2)

# type promotion
# int => float
f = CSV.File(IOBuffer("x\n1\n3.14"))
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === 1.0
@test f.x[2] === 3.14

# int => missing
f = CSV.File(IOBuffer("x\n1\n\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] == 1
@test f.x[2] === missing

# missing => int
f = CSV.File(IOBuffer("x\n\n1\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === missing
@test f.x[2] == 1

# missing => int => float
f = CSV.File(IOBuffer("x\n\n1\n3.14\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (3, 1)
@test f.x[1] === missing
@test f.x[2] === 1.0
@test f.x[3] === 3.14

# int => missing => float
f = CSV.File(IOBuffer("x\n1\n\n3.14\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (3, 1)
@test f.x[1] === 1.0
@test f.x[2] === missing
@test f.x[3] === 3.14

# int => float => missing
f = CSV.File(IOBuffer("x\n1\n3.14\n\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (3, 1)
@test f.x[1] === 1.0
@test f.x[2] === 3.14
@test f.x[3] === missing

# int => string
f = CSV.File(IOBuffer("x\n1\nabc"))
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] == "1"
@test f.x[2] == "abc"

# float => string
f = CSV.File(IOBuffer("x\n3.14\nabc"))
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] == "3.14"
@test f.x[2] == "abc"

# int => float => string
f = CSV.File(IOBuffer("x\n1\n3.14\nabc"))
@test (length(f), length(f.names)) == (3, 1)
@test f.x[1] == "1"
@test f.x[2] == "3.14"
@test f.x[3] == "abc"

# missing => int => float => string
f = CSV.File(IOBuffer("x\n\n1\n3.14\nabc"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (4, 1)
@test f.x[1] === missing
@test f.x[2] == "1"
@test f.x[3] == "3.14"
@test f.x[4] == "abc"

# downcast
f = CSV.File(IOBuffer("x\n1"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Int8

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Int16

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Int32

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Int64

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Int128

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === Float64

f = CSV.File(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14\nabc"), downcast=true, ignoreemptyrows=true)
@test eltype(f.x) === String

f = CSV.File(IOBuffer("x\n\n1"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Int8}

f = CSV.File(IOBuffer("x\n\n1\n$(typemax(Int16))"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Int16}

f = CSV.File(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Int32}

f = CSV.File(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Int64}

f = CSV.File(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Int128}

f = CSV.File(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14"), downcast=true, ignoreemptyrows=false)
@test eltype(f.x) === Union{Missing, Float64}

# missing => catg
f = CSV.File(IOBuffer("x\n\na\n"), pool=true, ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === missing
@test f.x[2] == "a"

# catg => missing
f = CSV.File(IOBuffer("x\na\n\n"), pool=true, ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] == "a"
@test f.x[2] === missing

# catg => string
f = CSV.File(IOBuffer("x\na\nb\na\nb\na\nb\na\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nn\nm\no\np\nq\nr\n"), pool=0.5)
@test typeof(f.x) == Vector{InlineString1}

# a few corner cases for escape strings
f = CSV.File(IOBuffer("\"column name with \"\" escape character inside\"\n1\n"))
@test keys(f[1])[1] == Symbol("column name with \" escape character inside")

f = CSV.File(IOBuffer("\"column name with \"\" escape character inside\",1\n,2"), transpose=true)
@test keys(f[1])[1] == Symbol("column name with \" escape character inside")
@test keys(f[1])[2] == :Column2

f = CSV.File(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true)
@test f.x[1] == "a"
@test f.x[3] == "quoted field with \" escape character inside"

f = CSV.File(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true)
@test f.x[1] == "a"
@test f.x[3] == "quoted field with \" escape character inside"

# invalid quoted field is fatal error
@test_throws CSV.Error CSV.File(IOBuffer("x\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.File(IOBuffer("x\nhey\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.File(IOBuffer("x\n\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.File(IOBuffer("x\n1\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.File(IOBuffer("x\n1.0\n\"quoted field that never ends"))
@test_throws CSV.Error CSV.File(IOBuffer("x\na\n\"quoted field that never ends"), pool=true)

# invalid integer
f = CSV.File(IOBuffer("x\nabc\n"), types=Int)
@test (length(f), length(f.names)) == (1, 1)
@test f.x[1] === missing

@test_throws CSV.Error CSV.File(IOBuffer("x\nabc\n"), types=Int, strict=true)

# transpose corner cases
f = CSV.File(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=2)
@test f.names == [:y, :y2]
@test (length(f), length(f.names)) == (1, 2)
@test f.y[1] == 1
@test f.y2[1] == 2

f = CSV.File(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=1, skipto=3)
@test f.names == [:x, :x2]
@test (length(f), length(f.names)) == (1, 2)
@test f.x[1] == 1
@test f.x2[1] == 2

f = CSV.File(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=false, skipto=3)
@test f.names == [:Column1, :Column2]
@test (length(f), length(f.names)) == (1, 2)
@test f.Column1[1] == 1
@test f.Column2[1] == 2

f = CSV.File(IOBuffer(""), transpose=true, header=false)
@test (length(f), length(f.names)) == (0, 0)

f = CSV.File(IOBuffer(""), transpose=true, header=Symbol[])
@test (length(f), length(f.names)) == (0, 0)

# providing empty header vector
f = CSV.File(IOBuffer("x\nabc\n"), header=Symbol[])
@test f.names == [:Column1]

# Union{Bool, Missing}
f = CSV.File(IOBuffer("x\ntrue\n\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === true
@test f.x[2] === missing

# Union{Date, Missing}
f = CSV.File(IOBuffer("x\n2019-01-01\n\n"), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === Date(2019, 1, 1)
@test f.x[2] === missing

# types is Dict{String, Type}
f = CSV.File(IOBuffer("x\n2019-01-01\n\n"), types=Dict("x"=>Date), ignoreemptyrows=false)
@test (length(f), length(f.names)) == (2, 1)
@test f.x[1] === Date(2019, 1, 1)
@test f.x[2] === missing

# various CSV.File/CSV.Row properties
f = CSV.File(IOBuffer("int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"), pool=0.3)
@test Tables.istable(f)
@test Tables.rowaccess(typeof(f))
@test Tables.columnaccess(typeof(f))
@test Tables.schema(f) == Tables.Schema([:int, :float, :date, :datetime, :bool, :null, :str, :catg, :int_float], [Int, Float64, Date, DateTime, Bool, Missing, InlineString7, InlineString3, Float64])
@test Tables.rows(f) === f
@test eltype(f) <: CSV.Row
row = first(f)
@test propertynames(row) == [:int, :float, :date, :datetime, :bool, :null, :str, :catg, :int_float]
@test row.int == 1
@test row.float == 3.14
@test row.date == Date(2019, 1, 1)
@test row.datetime == DateTime(2019, 1, 1, 1, 2, 3)
@test row.bool === true
@test row.null === missing
@test row.str == "hey"
@test row.catg == "abc"
@test typeof(row.catg) == InlineString3
@test row.int_float === 2.0
row = iterate(f, 2)[1]
@test row.int_float === 3.14

# 448
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"); ignorerepeated=true)

# reported by oxinabox on slack; issue w/ manually specified pool column type and 0 rows
f = CSV.File(IOBuffer("x\n"), pool=true)
@test (length(f), length(f.names)) == (0, 1)

f = CSV.File(IOBuffer("x\n1\n2\n3\n#4"), comment="#")
@test length(f.x) == 3
@test f.x[end] == 3

# 453
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"), types=[CSV_Foo])
@test_throws ArgumentError CSV.File(IOBuffer("x\n1\n2\n3\n#4"), types=Dict(:x=>CSV_Foo))

# 447
f = CSV.File(IOBuffer("a,b,c\n1,2,3\n\n"))
@test (length(f), length(f.names)) == (1, 3)

f = CSV.File(IOBuffer("zip\n11111-1111\n"), dateformat = "y-m-dTH:M:S.s")
@test (length(f), length(f.names)) == (1, 1)

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
f = CSV.File(`$(catcmd) $(joinpath(dir, "test_basic.csv"))`)
@test columntable(f) == columntable(CSV.File(joinpath(dir, "test_basic.csv")))

#476
f = CSV.File(joinpath(dir, "randoms.csv.gz"); buffer_in_memory=true)
@test (length(f), length(f.names)) == (70000, 7)

f = CSV.File(IOBuffer("thistime\n10:00:00.0\n12:00:00.0"))
@test typeof(f.thistime[1]) <: Dates.Time
@test f.thistime[1] === Time(10)

# 530
f = CSV.File(IOBuffer(",column2\nNA,2\n2,3"), missingstring=["NA"])
@test f.names == [:Column1, :column2]

# reported on slack from Kevin Bonham
f = CSV.File(IOBuffer("x\n01:02:03\n\n04:05:06\n"), delim=',', ignoreemptyrows=false)
@test isequal(f.x, [Dates.Time(1,2,3), missing, Dates.Time(4,5,6)])

# 566
f = CSV.File(IOBuffer("x\r\n1\r\n2\r\n3\r\n4\r\n5\r\n"), footerskip=3)
@test length(f) == 2
@test f[1][1] == 1

# 578
f = CSV.File(IOBuffer("h1234567890123456\t"^2262 * "lasthdr\r\n" * "dummy dummy dummy\r\n" * ("1.23\t"^2262 * "2.46\r\n")^10), skipto=3, ntasks=1);
@test (length(f), length(f.names)) == (10, 2263)
@test all(x -> eltype(x) == Float64, Tables.Columns(f))

# Date constructor throws which breaks CSV parsing error handling flow
f = CSV.File(IOBuffer("date\n2020-05-05\n2020-05-32"))

# multiple dateformats
f = CSV.File(IOBuffer("time,date,datetime\n10:00:00.0,04/16/2020,2020-04-16 23:14:00\n"), dateformat=Dict(2=>"mm/dd/yyyy", 3=>"yyyy-mm-dd HH:MM:SS"))
@test length(f) == 1
@test f[1].time == Dates.Time(10)
@test f[1].date == Dates.Date(2020, 4, 16)
@test f[1].datetime == Dates.DateTime(2020, 4, 16, 23, 14)

# custom types
f = CSV.File(
    IOBuffer("int8,uint32,bigint,bigfloat,dec64,csvstring\n1,2,170141183460469231731687303715884105727,3.14,1.02,hey there sailor\n2,,,,,\n");
    types=[Int8, UInt32, BigInt, BigFloat, Dec64, CSVString]
)
@test f.int8 isa Vector{Int8}
@test f.int8 == Int8[1, 2]
@test f.uint32 isa Vector{Union{UInt32, Missing}}
@test isequal(f.uint32, [0x00000002, missing])
@test f.bigint isa CSV.SVec2{BigInt}
@test isequal(f.bigint, [BigInt(170141183460469231731687303715884105727), missing])
@test f.bigfloat isa CSV.SVec2{BigFloat}
@test isequal(f.bigfloat, [parse(BigFloat, "3.14"), missing])
@test f.dec64 isa CSV.SVec{Dec64}
@test isequal(f.dec64, [Dec64(1.02), missing])
@test f.csvstring isa CSV.SVec2{CSVString}
@test isequal(f.csvstring, [CSVString("hey there sailor"), missing])

f = CSV.File(joinpath(dir, "randoms.csv.gz"); types=[Int, CSVString, String, Float64, Dec64, Date, DateTime])
@test f.id isa AbstractVector{Int}
@test f.first isa AbstractVector{CSVString}
@test f.wage isa AbstractVector{Union{Missing, Dec64}}

f = CSV.File(joinpath(dir, "promotions.csv"); stringtype=PosLenString)
@test Tables.schema(f).types == (Float64, Union{Missing, Int}, Union{Missing, Float64}, PosLenString, Union{Missing, PosLenString}, PosLenString, PosLenString, Union{Missing, Int})

f = CSV.File(joinpath(dir, "promotions.csv"); limit=7500, ntasks=2)
@test length(f) == 7500

f = CSV.File(IOBuffer("1,2\r\n3,4\r\n\r\n5,6\r\n"); header=["col1", "col2"], ignoreemptyrows=true)
@test f.col1 == [1, 3, 5]

f = CSV.File(joinpath(dir, "escape_row_starts.csv"); ntasks=2)
@test length(f) == 10000
@test eltype(f.col1) == String
@test eltype(f.col2) == Int

f = CSV.File(IOBuffer("col1\nhey\nthere\nsailor"); stringtype=PosLenString)
@test f.col1 isa PosLenStringVector
@test Tables.columnnames(f) == [:col1]
@test propertynames(f) == [:col1]
@test occursin("IOBuffer", CSV.getname(f))
@test CSV.getcols(f) == 1
@test Base.IndexStyle(f) == Base.IndexLinear()
@test f.col1 === Tables.getcolumn(f, 1)
@test columntable(f) == columntable(collect(f))
show(f)

f = CSV.File(joinpath(dir, "big_types.csv"); stringtype=PosLenString, pool=false)
@test eltype(f.time) == Dates.Time
@test eltype(f.bool) == Bool
@test eltype(f.lazy) == PosLenString
@test eltype(f.lazy_missing) == Union{PosLenString, Missing}

r = CSV.Rows(joinpath(dir, "big_types.csv"); types=[Dates.Time, Bool, String, Union{String, Missing}])
row = first(r)
@test row.time == Dates.Time(12)
@test row.bool
@test row.lazy == "hey"
@test row.lazy_missing === missing

@test CSV.File(IOBuffer("col1\n1")).col1 == [1]

chunks = CSV.Chunks(joinpath(dir, "promotions.csv"); stringtype=PosLenString, ntasks=2)
@test sum(length, chunks) == 10000
@test Tables.partitions(chunks) === chunks

@test_throws ArgumentError CSV.Chunks(joinpath(dir, "promotions.csv"); stringtype=PosLenString, ntasks=1)

# Test `ntasks` has expected defaults.
chunks = CSV.Chunks(joinpath(dir, "promotions.csv"); stringtype=PosLenString)
if Threads.nthreads() == 1
    @test length(chunks) == 8
else
    @test length(chunks) == Threads.nthreads()
end

# 668
buf = IOBuffer("""
       garbage
       a,b
       1,2
       """)
readline(buf)
f = CSV.File(buf, header=["A", "B"])
@test length(f) == 2
@test f.names == [:A, :B]
@test (f[1].A, f[1].B) == ("a", "b")
@test (f[2].A, f[2].B) == ("1", "2")

# 680: ensure typemap works with custom types
for tm in (Dict(Int => (Int === Int64 ? Int32 : Int64)),
           IdDict(Int => (Int === Int64 ? Int32 : Int64)))
    f = CSV.File(IOBuffer("a\n1\n2\n3"); typemap=tm)
end
@test f.a isa Vector{Int === Int64 ? Int32 : Int64}
f = CSV.File(IOBuffer("a\n1\n2\n3"); typemap=IdDict(Int=>String))
@test f.a isa Vector{String}

# 678
f = CSV.File(IOBuffer("""x,y
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b

                                       """), ignoreemptyrows=false)
@test f.x[end] === missing
@test f.y[end] === missing

# 679
f = CSV.File(IOBuffer("a,b,c\n1,2,3\n4,5,6\n"); select=["a"], types=Dict(2=>Int8))
@test f.a == [1, 4]

f = CSV.File(transcode(GzipDecompressor, Mmap.mmap(joinpath(dir, "randoms.csv.gz"))); types=Dict(:id=>Int32), select=["first"])
@test length(f) == 70000
@test eltype(f.first) == InlineString15

# 723
f = CSV.File(IOBuffer("col1,col2,col3\n1.0,2.0,3.0\n1.0,2.0,3.0\n1.0,2.0,3.0\n1.0,2.0,3.0\n"); ntasks=2)
@test length(f) == 4
@test f isa CSV.File

# 726
f = CSV.File(IOBuffer("col1,col2,col3,col4,col5\na,b,c,d,e\n" * "a,b,c,d\n"^101))
@test length(f) == 102

# 743
f = CSV.File(IOBuffer("col1\n\n \n  \n1\n2\n3"), missingstring=["", " ", "  "], ignoreemptyrows=false)
@test length(f) == 6
@test isequal(f.col1, [missing, missing, missing, 1, 2, 3])

f = CSV.File(IOBuffer("a\n1"))
@test length(f) == 1
@test f.a == [1]

# 788
f = CSV.File(IOBuffer("""
# 1'2
name
junk
1
"""), comment="#", header=2, skipto=4)
@test length(f) == 1
@test f[1].name == 1

f = CSV.File(IOBuffer("""
# 1'2"
name
junk
1
"""), comment="#", header=2, skipto=4)
@test length(f) == 1
@test f[1].name == 1

# 799
f = CSV.File(IOBuffer("Created Date\nToday\n"))
@test length(f) == length(f.names)
@test f[1][Symbol("Created Date")] == "Today"

# 796
csv = "1, a\n2, b\n3, c\n4, d\n\n"
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=1)
@test length(f) == 4
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=2)
@test length(f) == 3

csv = "1, a\r\n2, b\r\n3, c\r\n4, d\r\n\r\n"
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=1)
@test length(f) == 4
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=2)
@test length(f) == 3

csv = "1, a\r2, b\r3, c\r4, d\r\r"
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=1)
@test length(f) == 4
f = CSV.File(IOBuffer(csv); skipto=1, footerskip=2)
@test length(f) == 3

f = CSV.File(IOBuffer(join(rand(["a", "b", "c"], 500), "\n")); header=false, ntasks=2)
rt = Tables.rowtable(f)
@test length(rt) == 500
@test eltype(rt) == NamedTuple{(:Column1,), Tuple{InlineString1}}

f = CSV.File(IOBuffer("a, 0.1, 0.2, 0.3\nb, 0.4"); transpose=true)
@test length(f) == 3
@test f.a == [0.1, 0.2, 0.3]
@test isequal(f.b, [0.4, missing, missing])

# 845
f = CSV.File(IOBuffer("x\n\0\n"))
@test length(f) == 1
@test f.x[1] == "\0"

# 836
f = CSV.File(IOBuffer("x\n\"abc\"\n"); quoted=false)
@test f.x[1] == "\"abc\""

# 768
f = CSV.File(IOBuffer("a,b,c\n1,2,3\n,null,4\n"), missingstring=nothing)
@test eltype(f.a) <: AbstractString
@test f.a[2] == ""

# 858
row = first(CSV.Rows(IOBuffer("a,b,c\n1,2,3\n\n"); select=[:a, :c]))
@test length(row) == 2
@test row.a == "1" && row.c == "3"

# 802; throw error when invalid columns passed in types/dateformat/pool keyword arguments
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); types=Dict(4 => Float64))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); types=Dict(:d => Float64))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); types=Dict("d" => Float64))

@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict(4 => "dd/mm/yyyy"))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict(:d => "dd/mm/yyyy"))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict("d" => "dd/mm/yyyy"))

@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); pool=Dict(4 => true))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); pool=Dict(:d => true))
@test_throws ArgumentError CSV.File(IOBuffer("a,b,c\n1,2,3"); pool=Dict("d" => true))

# 910; disable checking of invalid columns passed in types/dateformat/pool keyword arguments
@test (@test_logs CSV.File(
    IOBuffer("a,b,c\n1,2,3");
    types=Dict(4 => Float64),
    dateformat=Dict(:e => "dd/mm/yyyy"),
    pool=Dict("f" => true),
    validate=false
)) isa CSV.File

# 871

f = CSV.File(IOBuffer("a,b,c\n1,2,3\n3.14,5,6\n"); typemap=IdDict(Float64 => String))
@test f.a isa AbstractVector{<:AbstractString}

# support SubArray{UInt8} as source
f = CSV.File(IOBuffer(strip(""""column_name","data_type","is_nullable"\nfoobar,string,YES\nbazbat,timestamptz,YES""")))
@test length(f) == 2
@test f.column_name == ["foobar", "bazbat"]
data = Vector{UInt8}(""""column_name","data_type","is_nullable"\nfoobar,string,YES\nbazbat,timestamptz,YES""")
f = CSV.File(@view(data[:]))
@test length(f) == 2
@test f.column_name == ["foobar", "bazbat"]

# 901; nonstandard types passed via types=T
f = CSV.File(IOBuffer("a,b,c\n1.2,3.4,5.6\n"); types=Float32)
@test length(f) == 1
@test NamedTuple(f[1]) === (a=Float32(1.2), b=Float32(3.4), c=Float32(5.6))

f = CSV.File(IOBuffer("a,b,c\n1.2,3.4,5.6\n"); types=BigFloat)
@test length(f) == 1
@test f[1].a == BigFloat("1.2") && f[1].b == BigFloat("3.4") && f[1].c == BigFloat("5.6")

# 894
f = CSV.File(codeunits("a,b,c\n1.2,3.4,5.6\n"))
@test length(f) == 1
@test NamedTuple(f[1]) === (a=1.2, b=3.4, c=5.6)

# 896
f = CSV.File(codeunits("a,b,c,d\n1,2,3.14,hey\n4,2,6.5,hey\n");
                types=(i, nm) -> i == 1 ? Int8 : i == 2 ? BigInt : i == 3 ? Float64 : String,
                pool=(i, nm) -> i == 2 ? true : nothing)
@test f.a == [1, 4]
@test eltype(f.a) == Int8
@test f.b == [2, 2]
@test f.b isa PooledVector{BigInt}
@test f.c == [3.14, 6.5]
@test f.d == ["hey", "hey"]

# 929
str = """id,pos_1,pos_2,pos_3,has_prisoner,capture_time,shape
5,11,86,1,false,0,diamond
4,5,43,1,false,0,diamond
6,16,90,1,false,0,diamond
7,75,35,1,false,0,diamond
2,35,89,1,false,0,diamond
10,81,25,1,false,0,diamond
9,48,98,1,false,0,diamond
8,98,62,1,false,0,diamond
3,50,2,1,false,0,diamond
1,95,24,1,false,0,diamond"""
r = collect(CSV.Rows(IOBuffer(str); types=Dict(:shape => Symbol)))
@test length(r) == 10

# 932
f = CSV.File(joinpath(dir, "multithreadedpromote.csv"))
@test eltype(f.col1) == String7
@test length(f) == 5001

# 942
data = """
name, age
Jack, 12
Tom, 10
"""
f = CSV.File(IOBuffer(data); select=[2], type=Int32)
@test length(f) == 2
@test length(f.names) == 1

# 939
row = join((i == 1 ? string(i + 10000000000) : i == 60_000 ? "0\n" : rand(("-1", "0", "1")) for i = 1:60_000), " ")
data = repeat(row, 271);
f = CSV.File(IOBuffer(data); header=false, types=Dict(1 => String), typemap=IdDict(Int => Int8));
@test f.types == [i == 1 ? String : Int8 for i = 1:60_000]
f = CSV.File(IOBuffer(data); header=false, types=Dict(1 => String), downcast=true);
@test f.types == [i == 1 ? String : Int8 for i = 1:60_000]

f = CSV.File(IOBuffer(data); header=false, types=Dict(1 => String), typemap=Dict(Int => Int8), ntasks=16);
@test f.types == [i == 1 ? String : Int8 for i = 1:60_000]

# 948
f = CSV.File(IOBuffer("a,b\n1,2\n3,"))
@test f.a == [1, 3]
@test isequal(f.b, [2, missing])

# duplicate column names
f = CSV.File(IOBuffer("a,a,a\n"))
@test f.names == [:a, :a_1, :a_2]

f = CSV.File(IOBuffer("a,a_1,a\n"))
@test f.names == [:a, :a_1, :a_2]

f = CSV.File(IOBuffer("a,a,a_1\n")) # this case is not covered in test_duplicate_columnnames.csv
@test f.names == [:a, :a_2, :a_1]

# 951
data = """
| Name       |  Zip |
| Joe        |  123 |
| Mary Anne  | 1234 |
"""
f = CSV.File(IOBuffer(data); delim='|', normalizenames=true, stripwhitespace=false)
@test f.Name[1] == " Joe        "
f = CSV.File(IOBuffer(data); delim='|', stripwhitespace=true)
@test f.Name[2] == "Mary Anne"

# 963
f = CSV.File(IOBuffer(join((rand(("a,$(rand())", "b,$(rand())")) for _ = 1:10^6), "\n")), header=false, limit=10000)
@test length(f) == 10000

# fix bool detection
f = CSV.File(IOBuffer("a\nfalse\n"))
@test eltype(f.a) == Bool

# 1014
# types is Dict{Regex}
data = IOBuffer("a_col,b_col,c,d\n1,2,3.14,hey\n4,2,6.5,hey\n")
f = CSV.File(data; types=Dict(r"_col$" => Int16))
@test eltype(f.a_col) == Int16
@test eltype(f.b_col) == Int16
@test_throws ArgumentError CSV.File(data; types=Dict(r"_column$" => Int16))
# types is Dict{Any} including `Regex` key
f = CSV.File(data; types=Dict(r"_col$" => Int16, "c" => Float16))
@test eltype(f.a_col) == Int16
@test eltype(f.b_col) == Int16
@test eltype(f.c) == Float16
# Regex has lower precedence than exact column name/number match
f = CSV.File(data; types=Dict(r"_col$" => Int16, :a_col => Int8))
@test eltype(f.a_col) == Int8
@test eltype(f.b_col) == Int16
# dateformat supports Regex
f = CSV.File(IOBuffer("time,date1,date2\n10:00:00.0,04/16/2020,04/17/2022\n"); dateformat=Dict(r"^date"=>"mm/dd/yyyy"))
@test f[1].date1 == Dates.Date(2020, 4, 16)
@test f[1].date2 == Dates.Date(2022, 4, 17)

# 1021 - https://github.com/JuliaData/CSV.jl/issues/1021
# user-given types for columns only found later in file
str = """
    1 2 3
    1 2
    1 2 3 4
    1
    1 2 3 4 5
    """
f = CSV.File(IOBuffer(str); delim=" ", header=false, types=String)
@test String <: eltype(f.Column5)
# case where `types isa AbstractVector`
f = CSV.File(IOBuffer(str); delim=" ", header=false, types=[Int8, Int16, Int32, Int64, Int128])
@test Int128 <: eltype(f.Column5)
# case where `types isa Function`
f = CSV.File(IOBuffer(str); delim=" ", header=false, types=(i,nm) -> (i == 5 ? Int8 : String))
@test Int8 <: eltype(f.Column5)
# case where `types isa AbstractDict`
f = CSV.File(IOBuffer(str); delim=" ", header=false, types=Dict(r".*" => Float16))
@test Float16 <: eltype(f.Column5)

# 1080
# bug in reading multiple files when a column shares name with a field in File
f = CSV.File(map(IOBuffer, ["name\n2\n", "name\n11\n"]))
@test f.name == [2, 11]

end
