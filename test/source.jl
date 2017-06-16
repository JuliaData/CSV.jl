#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(joinpath(dir, "test_no_header.csv");datarow=1,header=2);

#test various encodings
f = CSV.Source(joinpath(dir, "test_utf8_with_BOM.csv"))
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
sch = Data.schema(f)
@test f.options.delim == UInt8(',')
@test size(sch, 2) == 3
@test size(sch, 1) == 3
@test Data.header(sch) == ["col1","col2","col3"]
@test Data.types(sch) == (Float64,Float64,Float64)
ds = CSV.read(f)
@test ds[1][1] == 1.0
@test ds[1][2] == 4.0
@test ds[1][3] == 7.0
@test ds[2][1] == 2.0
sch2 = Data.schema(ds)
@test Data.header(sch2) == Data.header(sch)
@test Data.types(sch) == (Float64,Float64,Float64)

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
si = CSV.write(joinpath(dir, "new_test_utf8.csv"), f)
so = CSV.Source(si)
@test so.options.delim == UInt8(',')
@test size(Data.schema(so), 2) == 3
@test size(Data.schema(so), 1) == 3
@test Data.header(Data.schema(so)) == ["col1","col2","col3"]
@test Data.types(Data.schema(so)) == (Float64,Float64,Float64)
ds = CSV.read(so)
@test ds[1][1] == 1.0
@test ds[1][2] == 4.0
@test ds[1][3] == 7.0
@test ds[2][1] == 2.0
@test Data.types(Data.schema(f)) == Data.types(Data.schema(so)) == Data.types(Data.schema(ds))
f = si = so = ds = nothing; gc(); gc()
rm(joinpath(dir, "new_test_utf8.csv"))

# f = CSV.Source(joinpath(dir, "test_utf16_be.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16_le.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16.csv"))
f = CSV.Source(joinpath(dir, "test_windows.csv"))
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3

#test one column file
f = CSV.Source(joinpath(dir, "test_single_column.csv"))
@test f.options.delim == UInt8(',')
@test f.options.quotechar == UInt8('"')
@test f.options.escapechar == UInt8('\\')
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1"]
@test Data.types(Data.schema(f)) == (Int,)
ds = CSV.read(f)
@test ds[1][1] == 1
@test ds[1][2] == 2
@test ds[1][3] == 3

#test empty file
f = CSV.Source(joinpath(dir, "test_empty_file.csv"))
@test size(Data.schema(f), 1) == null

#test file with just newlines
f = CSV.Source(joinpath(dir, "test_empty_file_newlines.csv"))
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 9
@test Data.header(Data.schema(f)) == [""]
@test Data.types(Data.schema(f)) == (Null,)

#test with various quotechars, escapechars
f = CSV.Source(joinpath(dir, "test_simple_quoted.csv"))
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 1
ds = CSV.read(f)
@test string(ds[1][1]) == "quoted field 1"
@test string(ds[2][1]) == "quoted field 2"
f = CSV.Source(joinpath(dir, "test_quoted_delim_and_newline.csv"))
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 1

@testset "quoted numbers detected as string column" begin
    f = CSV.Source(joinpath(dir, "test_quoted_numbers.csv"))
    @test size(Data.schema(f), 2) == 3
    @test size(Data.schema(f), 1) == 3
    ds = CSV.read(f)
    @test Data.types(Data.schema(f)) == (WeakRefString{UInt8}, Int, Int)
end

#test various newlines
f = CSV.Source(joinpath(dir, "test_crlf_line_endings.csv"))
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int,Int,Int)
ds = CSV.read(f)
@test ds[1][1] == 1
f = CSV.Source(joinpath(dir, "test_newline_line_endings.csv"))
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int,Int,Int)
f = CSV.Source(joinpath(dir, "test_mac_line_endings.csv"))
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int,Int,Int)

#test headerrow, datarow, footerskips
f = CSV.Source(joinpath(dir, "test_no_header.csv"); header=0, datarow=1)
@test Data.header(Data.schema(f)) == ["Column1","Column2","Column3"]
@test Data.types(Data.schema(f)) == (Float64,Float64,Float64)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
f = CSV.Source(joinpath(dir, "test_2_footer_rows.csv"); header=4, datarow=5, footerskip=2)
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test Data.types(Data.schema(f)) == (Int,Int,Int)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3

#test dates, dateformats
f = CSV.Source(joinpath(dir, "test_dates.csv"); types=[Date], dateformat="yyyy-mm-dd")
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
ds = CSV.read(f)
@test ds[1][1] == Date(2015,1,1)
@test ds[1][2] == Date(2015,1,2)
@test ds[1][3] == Date(2015,1,3)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); dateformat="mm/dd/yy")
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); types=[Date], dateformat="mm/dd/yy")
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
f = CSV.Source(joinpath(dir, "test_datetimes.csv"); dateformat="yyyy-mm-dd HH:MM:SS.s")
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (DateTime,)
ds = CSV.read(f)
@test ds[1][1] == DateTime(2015,1,1)
@test ds[1][2] == DateTime(2015,1,2,0,0,1)
@test ds[1][3] == DateTime(2015,1,3,0,12,0,1)

#test bad types
f = CSV.Source(joinpath(dir, "test_float_in_int_column.csv"); types=[Int,Int,Int])
@test_throws CSV.ParsingException CSV.read(f)

#test null/missing values
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"))
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Float64,WeakRefString{UInt8},Float64)
ds = CSV.read(f)
@test ds[1][1] == 1.0
@test string(ds[2][1]) == "2.0"
@test string(ds[2][2]) == "NULL"
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"); null="NULL")
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test String(f.options.null) == "NULL"
@test Data.types(Data.schema(f)) == (Float64,?Float64,Float64)
ds = CSV.read(f)
@test ds[1][1] == 1.0
@test ds[2][1] == 2.0
@test isnull(ds[2][2])

# uses default missing value ""
f = CSV.Source(joinpath(dir, "test_missing_value.csv"))
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Float64,?Float64,Float64)

#other various files found around the internet
f = CSV.Source(joinpath(dir, "baseball.csv"); rows_for_type_detect=35)
@test size(Data.schema(f), 2) == 15
@test size(Data.schema(f), 1) == 35
@test Data.header(Data.schema(f)) == ["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test Data.types(Data.schema(f)) == (?Int,?Int,?Int,?WeakRefString{UInt8},?WeakRefString{UInt8},?WeakRefString{UInt8},?Int,?Int,?Float64,?Int,?Float64,?Int,?Int,?Float64,?WeakRefString{UInt8})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv"); types=Dict(10=>Float64,12=>Float64))
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int,WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Float64,Float64,WeakRefString{UInt8},WeakRefString{UInt8},Int)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64))
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int,WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Float64,Float64,WeakRefString{UInt8},WeakRefString{UInt8},Int)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "SacramentocrimeJanuary2006.csv"))
@test size(Data.schema(f), 2) == 9
@test size(Data.schema(f), 1) == 7584
@test Data.header(Data.schema(f)) == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "Sacramentorealestatetransactions.csv"))
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 985
@test Data.header(Data.schema(f)) == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},Int,Int,Int,WeakRefString{UInt8},WeakRefString{UInt8},Int,Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "SalesJan2009.csv"); types=Dict(3=>WeakRefString{UInt8},7=>?WeakRefString{UInt8}))
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 998
@test Data.header(Data.schema(f)) == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},?WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "stocks.csv"))
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 30
@test Data.header(Data.schema(f)) == ["Stock Name","Company Name"]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8},WeakRefString{UInt8})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "TechCrunchcontinentalUSA.csv"); types=Dict(4=>?WeakRefString{UInt8},5=>?WeakRefString{UInt8}))
@test size(Data.schema(f), 2) == 10
@test size(Data.schema(f), 1) == 1460
@test Data.header(Data.schema(f)) == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8},WeakRefString{UInt8},?Int,?WeakRefString{UInt8},?WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Int,WeakRefString{UInt8},WeakRefString{UInt8})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "Fielding.csv"); nullable=true, types=Dict("GS"=>Int,"InnOuts"=>Int,"WP"=>Int,"SB"=>Int,"CS"=>Int,"ZR"=>Int))
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 167938
@test Data.header(Data.schema(f)) == ["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test Data.types(Data.schema(f)) == (?WeakRefString{UInt8}, ?Int64, ?Int64, ?WeakRefString{UInt8}, ?WeakRefString{UInt8}, ?WeakRefString{UInt8}, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64, ?Int64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "latest (1).csv"); header=0, null="\\N", types=Dict(13=>?Float64,17=>?Int,18=>?Float64,20=>?Float64))
@test size(Data.schema(f), 2) == 25
@test size(Data.schema(f), 1) == 1000
@test Data.header(Data.schema(f)) == ["Column$i" for i = 1:size(Data.schema(f), 2)]
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8}, WeakRefString{UInt8}, Int64, Int64, WeakRefString{UInt8}, Int64, WeakRefString{UInt8}, Int64, Date, Date, Int64, WeakRefString{UInt8}, ?Float64, ?Float64, ?Float64, ?Float64, ?Int64, ?Float64, Float64, ?Float64, ?Float64, ?Int64, Float64, ?Float64, ?Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "pandas_zeros.csv"))
@test size(Data.schema(f), 2) == 50
@test size(Data.schema(f), 1) == 100000
@test Data.header(Data.schema(f)) == [string(i) for i = 0:49]
@test Data.types(Data.schema(f)) == (repmat([Int],50)...)
@time ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=1:3)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_header_range.csv");header=["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"],datarow=4)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1_sub1_part1","col2_sub2_part2","col3_sub3_part3"]
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic.csv");types=Dict(2=>Float64))
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Int,Float64,Int)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|')
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Int,Int,Int)
@test f.options.delim == UInt8('|')
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv");delim='|',footerskip=1)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 2
@test Data.types(Data.schema(f)) == (Int,Int,Int)
@test f.options.delim == UInt8('|')
ds = CSV.read(f)
@show f

t = tempname()
f = CSV.Sink(t)
@show f

f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"))
types = Data.types(Data.schema(f))
@test CSV.parsefield(f, types[1]) == 1.0
@test CSV.parsefield(f, types[2]) == "2.0"
@test CSV.parsefield(f, types[3]) == 3.0

t = tempname()
f = open(t, "w")
Base.write(f, readstring(joinpath(dir, "test_missing_value_NULL.csv")))
seekstart(f)
source = CSV.Source(f; header=[], datarow=2, nullable=false)
df = CSV.read(source)
@test Data.header(Data.schema(df)) == ["Column1", "Column2", "Column3"]

CSV.reset!(source)
df2 = CSV.read(source)
@test df == df2

@test_throws ArgumentError CSV.Source(f; types = [Int, Int, Int, Int])
close(f)
f = source = nothing; gc(); gc()
rm(t)

# test tab-delimited nulls
d = CSV.read(joinpath(dir, "test_tab_null_empty.txt"); delim='\t')
@test isnull(d[2][2])

d = CSV.read(joinpath(dir, "test_tab_null_string.txt"); delim='\t', null="NULL")
@test isnull(d[2][2])

# read a write protected file
let fn = tempname()
    open(fn, "w") do f
        write(f, "Julia")
    end
    chmod(fn, 0o444)
    CSV.read(fn)
    gc(); gc()
    rm(fn)
end

# CSV with header and no data is treated the same as an empty buffer with header supplied
df1 = CSV.read(IOBuffer("a,b,c"))
df2 = CSV.read(IOBuffer(""); header=["a", "b", "c"])
@test size(Data.schema(df1)) == (0, 3)
@test size(Data.schema(df2)) == (0, 3)
@test df1 == df2

# Adding transforms to CSV with header but no data returns empty frame as expected
# (previously the lack of a ::String dispatch in the transform function caused an error)
transforms = Dict{Int, Function}(2 => x::Integer -> "b$x")
df1 = CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"); nullable=false, transforms=transforms)
df2 = CSV.read(IOBuffer("a,b,c\n1,b2,3\n4,b5,6"); nullable=false)
@test size(Data.schema(df1)) == (2, 3)
@test size(Data.schema(df2)) == (2, 3)
@test df1 == df2
df3 = CSV.read(IOBuffer("a,b,c"); nullable=false, transforms=transforms)
df4 = CSV.read(IOBuffer("a,b,c"); nullable=false)
@test size(Data.schema(df3)) == (0, 3)
@test size(Data.schema(df4)) == (0, 3)
@test df3 == df4

let fn = tempname()
    df = CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"), CSV.Sink(fn); nullable=false, transforms=transforms)
    @test readstring(fn) == "a,b,c\n1,b2,3\n4,b5,6\n"
    rm(fn)
end

let fn = tempname()
    df = CSV.read(IOBuffer("a,b,c"), CSV.Sink(fn); nullable=false, transforms=transforms)
    @test readstring(fn) == "a,b,c\n"
    rm(fn)
end
