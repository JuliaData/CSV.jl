using CSV
using Base.Test, DataFrames, NullableArrays, DataStreams, WeakRefStrings, Libz, DecFP

if !isdefined(Core, :String)
    typealias String UTF8String
end
if VERSION < v"0.5-dev"
    readstring = readall
end

# dir = joinpath(Pkg.dir("CSV"), "test/test_files")
dir = joinpath(dirname(@__FILE__),"test_files/")

#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(joinpath(dir, "test_no_header.csv");datarow=1,header=2);

#test various encodings
# f = CSV.Source(joinpath(dir, "test_utf8_with_BOM.csv"))
f = CSV.Source(joinpath(dir, "test_utf8.csv"))
@test f.options.delim == UInt8(',')
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.header(f) == ["col1","col2","col3"]
@test Data.types(f) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]
@test position(f.data) == 15
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test Data.header(ds) == Data.header(f)
@test Data.types(ds, Data.Column) == [NullableVector{Float64},NullableVector{Float64},NullableVector{Float64}]

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
si = CSV.write(joinpath(dir, "new_test_utf8.csv"), f)
# @test Data.isdone(si)
@test Data.header(si) == Data.header(f) && Data.types(si) == Data.types(f)
so = CSV.Source(si)
@test so.options.delim == UInt8(',')
@test size(so, 2) == 3
@test size(so, 1) == 3
@test Data.header(so) == ["col1","col2","col3"]
@test Data.types(so) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]
# @test so.datapos == 21
ds = Data.stream!(so, DataFrame)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test Data.types(f) == Data.types(si) == Data.types(so) == Data.types(ds)
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
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.header(f) == ["col1"]
@test Data.types(f) == [Nullable{Int}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1
@test ds[2,1].value == 2
@test ds[3,1].value == 3

#test empty file
if VERSION > v"0.5.0-dev"
    f = CSV.Source(joinpath(dir, "test_empty_file.csv"))
    @test size(f, 1) == 0
else
    @test_throws ArgumentError CSV.Source(joinpath(dir, "test_empty_file.csv"))
end

#test file with just newlines
f = CSV.Source(joinpath(dir, "test_empty_file_newlines.csv"))
@test size(f, 2) == 1
@test size(f, 1) == 9
@test position(f.data) == 1
@test Data.header(f) == [""]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}}]

#test with various quotechars, escapechars
f = CSV.Source(joinpath(dir, "test_simple_quoted.csv"))
@test size(f, 2) == 2
@test size(f, 1) == 1
ds = Data.stream!(f, DataFrame)
@test string(ds[1,1].value) == "quoted field 1"
@test string(ds[1,2].value) == "quoted field 2"
f = CSV.Source(joinpath(dir, "test_quoted_delim_and_newline.csv"))
@test size(f, 2) == 2
@test size(f, 1) == 1

#test various newlines
f = CSV.Source(joinpath(dir, "test_crlf_line_endings.csv"))
@test Data.header(f) == ["col1","col2","col3"]
@test size(f, 2) == 3
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1
f = CSV.Source(joinpath(dir, "test_newline_line_endings.csv"))
@test Data.header(f) == ["col1","col2","col3"]
@test size(f, 2) == 3
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
f = CSV.Source(joinpath(dir, "test_mac_line_endings.csv"))
@test Data.header(f) == ["col1","col2","col3"]
@test size(f, 2) == 3
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]

#test headerrow, datarow, footerskips
f = CSV.Source(joinpath(dir, "test_no_header.csv");header=0,datarow=1)
@test Data.header(f) == ["Column1","Column2","Column3"]
@test Data.types(f) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]
@test size(f, 2) == 3
@test size(f, 1) == 3
f = CSV.Source(joinpath(dir, "test_2_footer_rows.csv");header=4,datarow=5,footerskip=2)
@test Data.header(f) == ["col1","col2","col3"]
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
@test size(f, 2) == 3
@test size(f, 1) == 3

#test dates, dateformats
f = CSV.Source(joinpath(dir, "test_dates.csv");types=[Date],dateformat="yyyy-mm-dd")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Date}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == Date(2015,1,1)
@test ds[2,1].value == Date(2015,1,2)
@test ds[3,1].value == Date(2015,1,3)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv");types=[Date],dateformat="mm/dd/yy")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Date}]
f = CSV.Source(joinpath(dir, "test_datetimes.csv");types=[DateTime],dateformat="yyyy-mm-dd HH:MM:SS.s")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{DateTime}]
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
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Float64},Nullable{WeakRefString{UInt8}},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test string(ds[2,2].value) == "NULL"
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"); null="NULL")
@test size(f, 2) == 3
@test size(f, 1) == 3
@test f.options.null == "NULL"
@test Data.types(f) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1.0
@test string(ds[1,2].value) == "2.0"
@test isnull(ds[2,2])

# uses default missing value ""
f = CSV.Source(joinpath(dir, "test_missing_value.csv"))
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]

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
@test size(f, 2) == 15
@test size(f, 1) == 35
@test position(f.data) == 59
@test Data.header(f) == ["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Int},Nullable{Int},Nullable{Float64},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)
# CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict(10=>Float64,12=>Float64))
@test size(f, 2) == 18
@test size(f, 1) == 36634
@test position(f.data) == 243
@test Data.header(f) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(f) == [Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict{String,DataType}("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64))
@test size(f, 2) == 18
@test size(f, 1) == 36634
@test position(f.data) == 243
@test Data.header(f) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(f) == [Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SacramentocrimeJanuary2006.csv"))
@test size(f, 2) == 9
@test size(f, 1) == 7584
@test position(f.data) == 81
@test Data.header(f) == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Sacramentorealestatetransactions.csv"))
@test size(f, 2) == 12
@test size(f, 1) == 985
@test position(f.data) == 80
@test Data.header(f) == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SalesJan2009.csv"))
@test size(f, 2) == 12
@test size(f, 1) == 998
@test position(f.data) == 114
@test Data.header(f) == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "stocks.csv"))
@test size(f, 2) == 2
@test size(f, 1) == 30
@test position(f.data) == 24
@test Data.header(f) == ["Stock Name","Company Name"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "TechCrunchcontinentalUSA.csv"))
@test size(f, 2) == 10
@test size(f, 1) == 1460
@test position(f.data) == 88
@test Data.header(f) == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Fielding.csv"))
@test size(f, 2) == 18
@test size(f, 1) == 167938
@test position(f.data) == 77
@test Data.header(f) == ["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{Int},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "latest (1).csv"); header=0, null="\\N")
@test size(f, 2) == 25
@test size(f, 1) == 1000
@test Data.header(f) == ["Column$i" for i = 1:size(f, 2)]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Date},Nullable{Date},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "pandas_zeros.csv"))
@test size(f, 2) == 50
@test size(f, 1) == 100000
@test Data.header(f) == [string(i) for i = 0:49]
@test Data.types(f) == repmat([Nullable{Int}],50)
@time ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=1:3)
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.header(f) == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"],datarow=4)
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.header(f) == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic.csv");types=Dict(2=>Float64))
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Int},Nullable{Float64},Nullable{Int}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|')
@test size(f, 2) == 3
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
@test f.options.delim == UInt8('|')
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|',footerskip=1)
@test size(f, 2) == 3
@test size(f, 1) == 2
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
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
