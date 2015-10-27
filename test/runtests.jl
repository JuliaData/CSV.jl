reload("CSV")
using Base.Test, NullableArrays, DataStreams

### File.jl

# dir = "/Users/jacobquinn/.julia/v0.4/CSV/test/test_files/"
dir = joinpath(dirname(@__FILE__),"test_files/")

# test 0.3 and 0.4

#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(dir * "test_no_header.csv";datarow=1,header=2);

#test various encodings
# f = CSV.Source(dir * "test_utf8_with_BOM.csv")
f = CSV.Source(dir * "test_utf8.csv")
@test f.options.delim == ','
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.types == [Float64,Float64,Float64]
@test position(f.data) == 15
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test ds.schema == f.schema

f = CSV.Source(dir * "test_utf8.csv")
si = CSV.Sink(f,dir * "new_test_utf8.csv")
Data.stream!(f,si)
@test Data.isdone(si)
@test si.schema == f.schema
so = CSV.Source(si)
@test so.options.delim == ','
@test so.schema.cols == 3
@test so.schema.rows == 3
@test so.schema.header == ["col1","col2","col3"]
@test so.schema.types == [Float64,Float64,Float64]
@test so.datapos == 21
ds = Data.stream!(so, Data.Table)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test ds.schema == f.schema == si.schema == so.schema
rm(dir * "new_test_utf8.csv")

# f = CSV.Source(dir * "test_utf16_be.csv")
# f = CSV.Source(dir * "test_utf16_le.csv")
# f = CSV.Source(dir * "test_utf16.csv")
# f = CSV.Source(dir * "test_windows.csv")

#test one column file
f = CSV.Source(dir * "test_single_column.csv")
@test f.options.delim == ','
@test f.options.quotechar == '"'
@test f.options.escapechar == '\\'
@test position(f.data) == 5
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.header == ["col1"]
@test f.schema.types == [Int]
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == 1
@test ds[2,1].value == 2
@test ds[3,1].value == 3

#test empty file
if VERSION > v"0.5.0-dev"
    f = CSV.Source(dir * "test_empty_file.csv")
    @test f.schema.rows == 0
else
    @test_throws ArgumentError CSV.Source(dir * "test_empty_file.csv")
end

#test file with just newlines
f = CSV.Source(dir * "test_empty_file_newlines.csv")
@test f.schema.cols == 1
@test f.schema.rows == 9
@test position(f.data) == 1
@test f.schema.header == [""]
@test f.schema.types == [CSV.PointerString]

#test with various quotechars, escapechars
f = CSV.Source(dir * "test_simple_quoted.csv")
@test f.schema.cols == 2
@test f.schema.rows == 1
ds = Data.stream!(f, Data.Table)
@test string(ds[1,1].value) == "quoted field 1"
@test string(ds[1,2].value) == "quoted field 2"
f = CSV.Source(dir * "test_quoted_delim_and_newline.csv")
@test f.schema.cols == 2
@test f.schema.rows == 1

#test various newlines
f = CSV.Source(dir * "test_crlf_line_endings.csv")
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == 1
f = CSV.Source(dir * "test_newline_line_endings.csv")
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]
f = CSV.Source(dir * "test_mac_line_endings.csv")
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]

#test headerrow, datarow, footerskips
f = CSV.Source(dir * "test_no_header.csv";header=0,datarow=1)
@test f.schema.header == ["Column1","Column2","Column3"]
@test f.schema.types == [Float64,Float64,Float64]
@test f.schema.cols == 3
@test f.schema.rows == 3
f = CSV.Source(dir * "test_2_footer_rows.csv";header=4,datarow=5,footerskip=2)
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.types == [Int,Int,Int]
@test f.schema.cols == 3
@test f.schema.rows == 3

#test dates, dateformats
f = CSV.Source(dir * "test_dates.csv";types=[Date],dateformat="yyyy-mm-dd")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [Date]
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == Date(2015,1,1)
@test ds[2,1].value == Date(2015,1,2)
@test ds[3,1].value == Date(2015,1,3)
f = CSV.Source(dir * "test_excel_date_formats.csv";types=[Date],dateformat="mm/dd/yy")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [Date]
f = CSV.Source(dir * "test_datetimes.csv";types=[DateTime],dateformat="yyyy-mm-dd HH:MM:SS.s")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [DateTime]
# f = CSV.Source(dir * "test_mixed_date_formats.csv";types=[Date],formats=["mm/dd/yyyy"])

#test bad types
# f = CSV.Source(dir * "test_float_in_int_column.csv";types=[Int,Int,Int])
# f = CSV.Source(dir * "test_floats.csv";types=[Float64,Float64,Float64])

#test null/missing values
f = CSV.Source(dir * "test_missing_value_NULL.csv")
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Float64,CSV.PointerString,Float64]
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test string(ds[2,2].value) == "NULL"
f = CSV.Source(dir * "test_missing_value_NULL.csv";null="NULL")
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.options.null == "NULL"
@test f.schema.types == [Float64,Float64,Float64]
ds = Data.stream!(f, Data.Table)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test isnull(ds[2,2])

# uses default missing value ""
f = CSV.Source(dir * "test_missing_value.csv")
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Float64,Float64,Float64]

#other various files found around the internet
f = CSV.Source(dir * "baseball.csv")
@test f.schema.cols == 15
@test f.schema.rows == 35
@test position(f.data) == 59
@test f.schema.header == UTF8String["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test f.schema.types == [Int64,Int64,Int64,CSV.PointerString,CSV.PointerString,CSV.PointerString,Int64,Int64,Float64,Int64,Float64,Int64,Int64,Float64,CSV.PointerString]
ds = Data.stream!(f, Data.Table)
# CSV.read(f)

f = CSV.Source(dir * "FL_insurance_sample.csv";types=Dict(10=>Float64,12=>Float64))
@test f.schema.cols == 18
@test f.schema.rows == 36634
@test position(f.data) == 243
@test f.schema.header == UTF8String["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test f.schema.types == [Int64,CSV.PointerString,CSV.PointerString,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CSV.PointerString,CSV.PointerString,Int64]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "SacramentocrimeJanuary2006.csv")
@test f.schema.cols == 9
@test f.schema.rows == 7584
@test position(f.data) == 81
@test f.schema.header == UTF8String["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test f.schema.types == [CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,Int64,CSV.PointerString,Int64,Float64,Float64]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "Sacramentorealestatetransactions.csv")
@test f.schema.cols == 12
@test f.schema.rows == 985
@test position(f.data) == 80
@test f.schema.header == UTF8String["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test f.schema.types == [CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,Int64,Int64,Int64,CSV.PointerString,CSV.PointerString,Int64,Float64,Float64]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "SalesJan2009.csv")
@test f.schema.cols == 12
@test f.schema.rows == 998
@test position(f.data) == 114
@test f.schema.header == UTF8String["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test f.schema.types == [CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString,Float64,Float64]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "stocks.csv")
@test f.schema.cols == 2
@test f.schema.rows == 30
@test position(f.data) == 24
@test f.schema.header == UTF8String["Stock Name","Company Name"]
@test f.schema.types == [CSV.PointerString,CSV.PointerString]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "TechCrunchcontinentalUSA.csv")
@test f.schema.cols == 10
@test f.schema.rows == 1460
@test position(f.data) == 88
@test f.schema.header == UTF8String["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test f.schema.types == [CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,CSV.PointerString]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "Fielding.csv")
@test f.schema.cols == 18
@test f.schema.rows == 167938
@test position(f.data) == 77
@test f.schema.header == UTF8String["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test f.schema.types == [CSV.PointerString,Int64,Int64,CSV.PointerString,CSV.PointerString,CSV.PointerString,Int64,CSV.PointerString,CSV.PointerString,Int64,Int64,Int64,Int64,Int64,CSV.PointerString,CSV.PointerString,CSV.PointerString,CSV.PointerString]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "latest (1).csv";header=0,null="\\N")
@test f.schema.cols == 25
@test f.schema.rows == 1000
@test f.schema.header == ["Column$i" for i = 1:f.schema.cols]
@test f.schema.types == [CSV.PointerString,CSV.PointerString,Int64,Int64,CSV.PointerString,Int64,CSV.PointerString,Int64,Date,Date,Int64,CSV.PointerString,Float64,Float64,Float64,Float64,Int64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,Float64]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "pandas_zeros.csv")
@test f.schema.cols == 50
@test f.schema.rows == 100000
@test f.schema.header == [string(i) for i = 0:49]
@test f.schema.types == repmat([Int],50)
@time ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_basic.csv.gz")
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.types == [Int,Int,Int]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_header_range.csv";header=1:3)
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_header_range.csv";header=["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"],datarow=4)
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_basic.csv";types=Dict(2=>Float64))
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Int,Float64,Int]
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_basic_pipe.csv";delim='|')
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Int,Int,Int]
@test f.options.delim == '|'
ds = Data.stream!(f, Data.Table)

f = CSV.Source(dir * "test_basic_pipe.csv";delim='|',footerskip=1)
@test f.schema.cols == 3
@test f.schema.rows == 2
@test f.schema.types == [Int,Int,Int]
@test f.options.delim == '|'
ds = Data.stream!(f, Data.Table)

### io.jl

# Int
io = IOBuffer("0")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("-1")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(-1)

io = IOBuffer("1")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1)

io = IOBuffer("2000")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(2000)

io = IOBuffer("0.0")
v = NullableArray(Int,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Int,CSV.Options(),1,1)

io = IOBuffer("0a")
v = NullableArray(Int,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Int,CSV.Options(),1,1)

io = IOBuffer("")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(" ")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\t")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(" \t 010")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(10)

io = IOBuffer("\"1_00a0\"")
v = NullableArray(Int,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Int,CSV.Options(),1,1)

io = IOBuffer("\"0\"")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("0\n")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("0\r")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("0\r\n")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("0a\n")
v = NullableArray(Int,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Int,CSV.Options(),1,1)

# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
v = NullableArray(Int,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Int,CSV.Options(),1,1)
#@test v == (0,false)

io = IOBuffer("0,")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("0,\n")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0)

io = IOBuffer("\n")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r\n")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\"")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("1234567890")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1234567890)

io = IOBuffer("\\N")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

# Float64
io = IOBuffer("1")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1.0)

io = IOBuffer("-1.0")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(-1.0)

io = IOBuffer("0")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("2000")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(2000.0)

io = IOBuffer("0a")
v = NullableArray(Float64,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Float64,CSV.Options(),1,1)

io = IOBuffer("")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(" ")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\t")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(" \t 010")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(10.0)

io = IOBuffer("\"1_00a0\"")
v = NullableArray(Float64,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Float64,CSV.Options(),1,1)

io = IOBuffer("\"0\"")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("0\n")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("0\r")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("0\r\n")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("0a\n")
v = NullableArray(Float64,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Float64,CSV.Options(),1,1)

# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
v = NullableArray(Float64,1)
@test_throws CSV.CSVError CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
#@test v == (0,false)

io = IOBuffer("0,")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("0,\n")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(0.0)

io = IOBuffer("\n")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r\n")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\"")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("1234567890")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1234567890.0)

io = IOBuffer(".1234567890")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(.1234567890)

io = IOBuffer("0.1234567890")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(.1234567890)

io = IOBuffer("\"0.1234567890\"")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(.1234567890)

io = IOBuffer("\\N")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

# String
io = IOBuffer("0")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("-1")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "-1"

io = IOBuffer("1")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "1"

io = IOBuffer("2000")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "2000"

io = IOBuffer("0.0")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0.0"

io = IOBuffer("0a")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0a"

io = IOBuffer("")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(" ")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == " "

io = IOBuffer("\t")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "\t"

io = IOBuffer(" \t 010")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == " \t 010"

io = IOBuffer("\"1_00a0\"")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "1_00a0"

io = IOBuffer("\"0\"")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("0\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("0\r")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("0\r\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("0a\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0a"

# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "\t0\t"

io = IOBuffer("0,")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("0,\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "0"

io = IOBuffer("\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r\n")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\"")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("1234567890")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "1234567890"

io = IOBuffer("\"hey there\\\"quoted field\\\"\"")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "hey there\\\"quoted field\\\""

io = IOBuffer("\\N")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

# Date

io = IOBuffer("")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer(",")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\r\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\"")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\\N")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(null="\\N"),1,1)
v = v[1]
@test isnull(v)

io = IOBuffer("2015-10-05")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05,")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("  \"2015-10-05\",")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

io = CSV.Source(IOBuffer("10/5/2015,");dateformat=Dates.DateFormat("mm/dd/yyyy"),header=0,datarow=1)
ds = Data.Table(io)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"10/5/2015\"\n")
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

# All types
io = IOBuffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1)
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1.0)
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "hey there sailor"
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1.0)
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "hey there sailor"
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1)
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test string(get(v)) == "hey there sailor"
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test v === Nullable{Date}(Date(2015,10,5))

v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1)
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test v === Nullable(1.0)
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)

v = NullableArray(Int,1)
CSV.getfield!(io,v,Int,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(Float64,1)
CSV.getfield!(io,v,Float64,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(CSV.PointerString,1)
CSV.getfield!(io,v,CSV.PointerString,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
v = NullableArray(Date,1)
CSV.getfield!(io,v,Date,CSV.Options(),1,1)
v = v[1]
@test isnull(v)
