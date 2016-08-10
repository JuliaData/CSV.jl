using CSV
using Base.Test, DataFrames, NullableArrays, DataStreams, WeakRefStrings, Libz, DecFP

if !isdefined(Core, :String)
    typealias String UTF8String
end

dir = "/Users/jacobquinn/.julia/v0.5/CSV/test/test_files/"
dir = joinpath(dirname(@__FILE__),"test_files/")

#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(joinpath(dir, "test_no_header.csv");datarow=1,header=2);

#test various encodings
# f = CSV.Source(joinpath(dir, "test_utf8_with_BOM.csv"))
f = CSV.Source(joinpath(dir, "test_utf8.csv"))
@test f.options.delim == UInt8(',')
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.types == [Float64,Float64,Float64]
@test position(f.data) == 15
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test Data.header(ds) == Data.header(f) && Data.types(ds) == Data.types(f)

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
si = CSV.write(joinpath(dir, "new_test_utf8.csv"), f)
# @test Data.isdone(si)
@test si.schema == f.schema
so = CSV.Source(si)
@test so.options.delim == UInt8(',')
@test so.schema.cols == 3
@test so.schema.rows == 3
@test so.schema.header == ["col1","col2","col3"]
@test so.schema.types == [Float64,Float64,Float64]
# @test so.datapos == 21
ds = Data.stream!(so, DataFrame)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test Data.types(ds) == Data.types(f) == Data.types(si) == Data.types(so)
f = si = so = ds = nothing; gc(); gc()
rm(joinpath(dir, "new_test_utf8.csv"))

# f = CSV.Source(joinpath(dir, "test_utf16_be.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16_le.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16.csv"))
# f = CSV.Source(joinpath(dir, "test_windows.csv"))

#test one column file
f = CSV.Source(joinpath(dir, "test_single_column.csv"))
@test f.options.delim == UInt8(',')
@test f.options.quotechar == UInt8('"')
@test f.options.escapechar == UInt8('\\')
@test position(f.data) == 5
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.header == ["col1"]
@test f.schema.types == [Int]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1
@test ds[2,1].value == 2
@test ds[3,1].value == 3

#test empty file
if VERSION > v"0.5.0-dev"
    f = CSV.Source(joinpath(dir, "test_empty_file.csv"))
    @test f.schema.rows == 0
else
    @test_throws ArgumentError CSV.Source(joinpath(dir, "test_empty_file.csv"))
end

#test file with just newlines
f = CSV.Source(joinpath(dir, "test_empty_file_newlines.csv"))
@test f.schema.cols == 1
@test f.schema.rows == 9
@test position(f.data) == 1
@test f.schema.header == [""]
@test f.schema.types == [WeakRefString{UInt8}]

#test with various quotechars, escapechars
f = CSV.Source(joinpath(dir, "test_simple_quoted.csv"))
@test f.schema.cols == 2
@test f.schema.rows == 1
ds = Data.stream!(f, DataFrame)
@test string(ds[1,1].value) == "quoted field 1"
@test string(ds[1,2].value) == "quoted field 2"
f = CSV.Source(joinpath(dir, "test_quoted_delim_and_newline.csv"))
@test f.schema.cols == 2
@test f.schema.rows == 1

#test various newlines
f = CSV.Source(joinpath(dir, "test_crlf_line_endings.csv"))
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1
f = CSV.Source(joinpath(dir, "test_newline_line_endings.csv"))
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]
f = CSV.Source(joinpath(dir, "test_mac_line_endings.csv"))
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.cols == 3
@test f.schema.types == [Int,Int,Int]

#test headerrow, datarow, footerskips
f = CSV.Source(joinpath(dir, "test_no_header.csv");header=0,datarow=1)
@test f.schema.header == ["Column1","Column2","Column3"]
@test f.schema.types == [Float64,Float64,Float64]
@test f.schema.cols == 3
@test f.schema.rows == 3
f = CSV.Source(joinpath(dir, "test_2_footer_rows.csv");header=4,datarow=5,footerskip=2)
@test f.schema.header == ["col1","col2","col3"]
@test f.schema.types == [Int,Int,Int]
@test f.schema.cols == 3
@test f.schema.rows == 3

#test dates, dateformats
f = CSV.Source(joinpath(dir, "test_dates.csv");types=[Date],dateformat="yyyy-mm-dd")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [Date]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == Date(2015,1,1)
@test ds[2,1].value == Date(2015,1,2)
@test ds[3,1].value == Date(2015,1,3)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv");types=[Date],dateformat="mm/dd/yy")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [Date]
f = CSV.Source(joinpath(dir, "test_datetimes.csv");types=[DateTime],dateformat="yyyy-mm-dd HH:MM:SS.s")
@test f.schema.cols == 1
@test f.schema.rows == 3
@test f.schema.types == [DateTime]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == DateTime(2015,1,1)
@test ds[2,1].value == DateTime(2015,1,2,0,0,1)
@test ds[3,1].value == DateTime(2015,1,3,0,12,0,1)
# f = CSV.Source(joinpath(dir, "test_mixed_date_formats.csv");types=[Date],formats=["mm/dd/yyyy"])

#test bad types
# f = CSV.Source(joinpath(dir, "test_float_in_int_column.csv");types=[Int,Int,Int])
# f = CSV.Source(joinpath(dir, "test_floats.csv");types=[Float64,Float64,Float64])

#test null/missing values
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"))
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Float64,WeakRefString{UInt8},Float64]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test string(ds[2,2].value) == "NULL"
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"); null="NULL")
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.options.null == "NULL"
@test f.schema.types == [Float64,Float64,Float64]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test isnull(ds[2,2])

# uses default missing value ""
f = CSV.Source(joinpath(dir, "test_missing_value.csv"))
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Float64,Float64,Float64]

# DataStreams interface
source_file = joinpath(dir, "test_utf8.csv")
sink_file = joinpath(dir, "test_utf8_new.csv")
header = "\"col1\",\"col2\",\"col3\"\n"
values = "1.0,2.0,3.0\n4.0,5.0,6.0\n7.0,8.0,9.0\n"

ds = CSV.read(source_file)
@test size(ds) == (3,3)
ds = CSV.read(source_file, CSV.Sink, sink_file)
@test readstring(sink_file) == header * values
ds = CSV.read(source_file, CSV.Sink, sink_file; append=true)
@test readstring(sink_file) == header * values * values

sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"))
ds = CSV.read(source_file, sink)
@test readstring(sink_file) == header * values
ds = CSV.read(source_file, sink; append=true)
@test readstring(sink_file) == header * values * values

source = CSV.Source(source_file)
ds = CSV.read(source)
@test size(ds) == (3,3)
source = CSV.Source(source_file)
ds = CSV.read(source, CSV.Sink, sink_file)
@test readstring(sink_file) == header * values
source = CSV.Source(source_file)
ds = CSV.read(source, CSV.Sink, sink_file; append=true)
@test readstring(sink_file) == header * values * values

sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"))
source = CSV.Source(source_file)
ds = CSV.read(source, sink)
@test readstring(sink_file) == header * values
source = CSV.Source(source_file)
ds = CSV.read(source, sink; append=true)
@test readstring(sink_file) == header * values * values

si = CSV.write(sink_file, CSV.Source, source_file)
@test readstring(sink_file) == header * values
si = CSV.write(sink_file, CSV.Source, source_file; append=true)
@test readstring(sink_file) == header * values * values

source = CSV.Source(source_file)
si = CSV.write(sink_file, source)
@test readstring(sink_file) == header * values
source = CSV.Source(source_file)
si = CSV.write(sink_file, source; append=true)
@test readstring(sink_file) == header * values * values

sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"))
si = CSV.write(sink, CSV.Source, source_file)
@test readstring(sink_file) == header * values
sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"); append=true)
si = CSV.write(sink, CSV.Source, source_file; append=true)
@test readstring(sink_file) == header * values * values

source = CSV.Source(source_file)
sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"))
si = CSV.write(sink, source)
@test readstring(sink_file) == header * values
source = CSV.Source(source_file)
sink = CSV.Sink(joinpath(dir, "test_utf8_new.csv"); append=true)
si = CSV.write(sink, source; append=true)
@test readstring(sink_file) == header * values * values
rm(sink_file)

#other various files found around the internet
f = CSV.Source(joinpath(dir, "baseball.csv"))
@test f.schema.cols == 15
@test f.schema.rows == 35
@test position(f.data) == 59
@test f.schema.header == ["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test f.schema.types == [Int,Int,Int,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Int,Int,Float64,Int,Float64,Int,Int,Float64,WeakRefString{UInt8}]
ds = Data.stream!(f, DataFrame)
# CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict(10=>Float64,12=>Float64))
@test f.schema.cols == 18
@test f.schema.rows == 36634
@test position(f.data) == 243
@test f.schema.header == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test f.schema.types == [Int,WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Float64,Float64,WeakRefString{UInt8},WeakRefString{UInt8},Int]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict{String,DataType}("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64))
@test f.schema.cols == 18
@test f.schema.rows == 36634
@test position(f.data) == 243
@test f.schema.header == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test f.schema.types == [Int,WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Float64,Float64,WeakRefString{UInt8},WeakRefString{UInt8},Int]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SacramentocrimeJanuary2006.csv"))
@test f.schema.cols == 9
@test f.schema.rows == 7584
@test position(f.data) == 81
@test f.schema.header == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,Float64,Float64]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Sacramentorealestatetransactions.csv"))
@test f.schema.cols == 12
@test f.schema.rows == 985
@test position(f.data) == 80
@test f.schema.header == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,Int,Int,WeakRefString{UInt8},WeakRefString{UInt8},Int,Float64,Float64]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SalesJan2009.csv"))
@test f.schema.cols == 12
@test f.schema.rows == 998
@test position(f.data) == 114
@test f.schema.header == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "stocks.csv"))
@test f.schema.cols == 2
@test f.schema.rows == 30
@test position(f.data) == 24
@test f.schema.header == ["Stock Name","Company Name"]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "TechCrunchcontinentalUSA.csv"))
@test f.schema.cols == 10
@test f.schema.rows == 1460
@test position(f.data) == 88
@test f.schema.header == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},WeakRefString{UInt8}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Fielding.csv"))
@test f.schema.cols == 18
@test f.schema.rows == 167938
@test position(f.data) == 77
@test f.schema.header == ["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test f.schema.types == [WeakRefString{UInt8},Int,Int,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},WeakRefString{UInt8},Int,Int,Int,Int,Int,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "latest (1).csv"); header=0, null="\\N")
@test f.schema.cols == 25
@test f.schema.rows == 1000
@test f.schema.header == ["Column$i" for i = 1:f.schema.cols]
@test f.schema.types == [WeakRefString{UInt8},WeakRefString{UInt8},Int,Int,WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,Date,Date,Int,WeakRefString{UInt8},Float64,Float64,Float64,Float64,Int,Float64,Float64,Float64,Float64,Int,Float64,Float64,Float64]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "pandas_zeros.csv"))
@test f.schema.cols == 50
@test f.schema.rows == 100000
@test f.schema.header == [string(i) for i = 0:49]
@test f.schema.types == repmat([Int],50)
@time ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=1:3)
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"],datarow=4)
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.header == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic.csv");types=Dict(2=>Float64))
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Int,Float64,Int]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|')
@test f.schema.cols == 3
@test f.schema.rows == 3
@test f.schema.types == [Int,Int,Int]
@test f.options.delim == UInt8('|')
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|',footerskip=1)
@test f.schema.cols == 3
@test f.schema.rows == 2
@test f.schema.types == [Int,Int,Int]
@test f.options.delim == UInt8('|')
ds = Data.stream!(f, DataFrame)

### io.jl

# Int
io = IOBuffer("0")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("-1")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(-1)

io = IOBuffer("1")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)

io = IOBuffer("2000")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(2000)

io = IOBuffer("0.0")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(10)

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = IOBuffer("\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1234567890)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Int64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(-1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(2000)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(10)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Int,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Int,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Float64 Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(-1.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(2000.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(10.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1234567890.0)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Float64
io = IOBuffer("1")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)

io = IOBuffer("-1.0")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(-1.0)

io = IOBuffer("0")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("2000")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(2000.0)

io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(10.0)

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\r")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Float64,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(0.0)

io = IOBuffer("\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1234567890.0)

io = IOBuffer(".1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(.1234567890)

io = IOBuffer("nan")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = IOBuffer("NaN")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(NaN)

io = IOBuffer("inf")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = IOBuffer("infinity")
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(Inf)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Float64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# DecFP Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1.0")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(-1.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(2000.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(10.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1234567890.0))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(".1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.1234567890")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0.1234567890\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("nan")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("NaN")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("inf")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("infinity")))
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# DecFP
io = IOBuffer("1")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1.0))

io = IOBuffer("-1.0")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(-1.0))

io = IOBuffer("0")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("2000")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(2000.0))

io = IOBuffer("0a")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = IOBuffer("")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\t")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(10.0))

io = IOBuffer("\"1_00a0\"")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
io = IOBuffer("\"0\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\r")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0a\n")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)
# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
@test_throws CSV.CSVError CSV.parsefield(io,Dec64,CSV.Options(),1,1)

io = IOBuffer("0,")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("0,\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(0.0))

io = IOBuffer("\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(1234567890.0))

io = IOBuffer(".1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("0.1234567890")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("\"0.1234567890\"")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(.1234567890))

io = IOBuffer("nan")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = IOBuffer("NaN")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(NaN))

io = IOBuffer("inf")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = IOBuffer("infinity")
v = CSV.parsefield(io,Dec64,CSV.Options(),1,1)
@test v === Nullable(Dec64(Inf))

io = IOBuffer("\\N")
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Dec64,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# WeakRefString
io = IOBuffer("0")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("-1")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "-1"

io = IOBuffer("1")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1"

io = IOBuffer("2000")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "2000"

io = IOBuffer("0.0")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0.0"

io = IOBuffer("0a")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0a"

io = IOBuffer("")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(" ")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == " "

io = IOBuffer("\t")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "\t"

io = IOBuffer(" \t 010")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == " \t 010"

io = IOBuffer("\"1_00a0\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1_00a0"

io = IOBuffer("\"0\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\r")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0\r\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0a\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0a"

# Should we handle trailing whitespace?
io = IOBuffer("\t0\t\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "\t0\t"

io = IOBuffer("0,")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("0,\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "0"

io = IOBuffer("\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("1234567890")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "1234567890"

io = IOBuffer("\"hey there\\\"quoted field\\\"\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there\\\"quoted field\\\""

io = IOBuffer("\\N")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("-1")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "-1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2000")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "2000"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0.0")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0.0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0a"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" ")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == " "

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(" \t 010")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == " \t 010"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"1_00a0\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1_00a0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"0\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0\r\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0a\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0a"

# Should we handle trailing whitespace?
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\t0\t\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "\t0\t"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("0,\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "0"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("1234567890")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "1234567890"

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"hey there\\\"quoted field\\\"\"")))
v = CSV.parsefield(io,String,CSV.Options(),1,1)
@test string(get(v)) == "hey there\\\"quoted field\\\""

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,String,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,String,CSV.Options(null="\\N"),1,1)
@test isnull(v)

# Date

io = IOBuffer("")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(",")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\\N")
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("2015-10-05")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05,")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("2015-10-05\r\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("  \"2015-10-05\",")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"2015-10-05\"\n")
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = IOBuffer("\"10/5/2015\"\n")
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

# Date Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,Date,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05,")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05\r\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05\",")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05\"\n")))
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015\"\n")))
v = CSV.parsefield(io,Date,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy")),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

# DateTime

io = IOBuffer("")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer(",")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\"\"")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = IOBuffer("\\N")
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("\"\\N\"")
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = IOBuffer("2015-10-05T00:00:01")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"2015-10-05T00:00:01\"")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01,")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\r")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("2015-10-05T00:00:01\r\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("  \"2015-10-05T00:00:01\",")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"2015-10-05T00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = IOBuffer("\"10/5/2015 00:00:01\"\n")
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

# DateTime Libz
io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer(",")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\r\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\\N")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"\\N\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="\\N"),1,1)
@test isnull(v)

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01,")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("2015-10-05T00:00:01\r\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("  \"2015-10-05T00:00:01\",")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"2015-10-05T00:00:01\"\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

io = ZlibInflateInputStream(ZlibDeflateInputStream(IOBuffer("\"10/5/2015 00:00:01\"\n")))
v = CSV.parsefield(io,DateTime,CSV.Options(null="",dateformat=Dates.DateFormat("mm/dd/yyyy HH:MM:SS")),1,1)
@test v === Nullable{DateTime}(DateTime(2015,10,5,0,0,1))

# All types
io = IOBuffer("1,1.0,hey there sailor,2015-10-05\n,1.0,hey there sailor,\n1,,hey there sailor,2015-10-05\n1,1.0,,\n,,,")
# io = CSV.Source(io)
v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test string(get(v)) == "hey there sailor"
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test v === Nullable{Date}(Date(2015,10,5))

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test v === Nullable(1)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test v === Nullable(1.0)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)

v = CSV.parsefield(io,Int,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Float64,CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,WeakRefString{UInt8},CSV.Options(),1,1)
@test isnull(v)
v = CSV.parsefield(io,Date,CSV.Options(),1,1)
@test isnull(v)
