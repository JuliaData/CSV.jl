#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(joinpath(dir, "test_no_header.csv");datarow=1,header=2);

#test various encodings
f = CSV.Source(joinpath(dir, "test_utf8_with_BOM.csv"))
@test Data.header(f) == ["col1","col2","col3"]

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
sch = Data.schema(f)
@test f.options.delim == UInt8(',')
@test size(sch, 2) == 3
@test size(sch, 1) == 3
@test Data.header(sch) == ["col1","col2","col3"]
@test Data.types(sch) == [Float64,Float64,Float64]
ds = CSV.read(f)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
sch2 = Data.schema(ds)
@test Data.header(sch2) == Data.header(sch)
@test Data.types(sch) == [Vector{Float64},Vector{Float64},Vector{Float64}]

f = CSV.Source(joinpath(dir, "test_utf8.csv"))
si = CSV.write(joinpath(dir, "new_test_utf8.csv"), f)
so = CSV.Source(si)
@test so.options.delim == UInt8(',')
@test size(so, 2) == 3
@test size(so, 1) == 3
@test Data.header(so) == ["col1","col2","col3"]
@test Data.types(so) == [Float64,Float64,Float64]
# @test so.iopos == 21
ds = Data.stream!(so, DataFrame)
@test ds[1,1].value == 1.0
@test ds[2,1].value == 4.0
@test ds[3,1].value == 7.0
@test ds[1,2].value == 2.0
@test Data.types(f) == Data.types(so) == Data.types(ds)
f = si = so = ds = nothing; gc(); gc()
rm(joinpath(dir, "new_test_utf8.csv"))

# f = CSV.Source(joinpath(dir, "test_utf16_be.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16_le.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16.csv"))
f = CSV.Source(joinpath(dir, "test_windows.csv"))
@test size(f, 2) == 3
@test size(f, 1) == 3

#test one column file
f = CSV.Source(joinpath(dir, "test_single_column.csv"))
@test f.options.delim == UInt8(',')
@test f.options.quotechar == UInt8('"')
@test f.options.escapechar == UInt8('\\')
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.header(f) == ["col1"]
@test Data.types(f) == [Nullable{Int}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == 1
@test ds[2,1].value == 2
@test ds[3,1].value == 3

#test empty file
f = CSV.Source(joinpath(dir, "test_empty_file.csv"))
@test size(f, 1) == 0

#test file with just newlines
f = CSV.Source(joinpath(dir, "test_empty_file_newlines.csv"))
@test size(f, 2) == 1
@test size(f, 1) == 9
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

@testset "quoted numbers detected as string column" begin
    f = CSV.Source(joinpath(dir, "test_quoted_numbers.csv"))
    @test size(f, 2) == 3
    @test size(f, 1) == 3
    ds = Data.stream!(f, DataFrame)
    @test Data.types(f) == [Nullable{WeakRefString{UInt8}}, Nullable{WeakRefString{UInt8}}, Nullable{Int}]
end

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
f = CSV.Source(joinpath(dir, "test_no_header.csv"); header=0, datarow=1)
@test Data.header(f) == ["Column1","Column2","Column3"]
@test Data.types(f) == [Nullable{Float64},Nullable{Float64},Nullable{Float64}]
@test size(f, 2) == 3
@test size(f, 1) == 3
f = CSV.Source(joinpath(dir, "test_2_footer_rows.csv"); header=4, datarow=5, footerskip=2)
@test Data.header(f) == ["col1","col2","col3"]
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int}]
@test size(f, 2) == 3
@test size(f, 1) == 3

#test dates, dateformats
f = CSV.Source(joinpath(dir, "test_dates.csv"); types=[Date], dateformat="yyyy-mm-dd")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Date}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == Date(2015,1,1)
@test ds[2,1].value == Date(2015,1,2)
@test ds[3,1].value == Date(2015,1,3)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); dateformat="mm/dd/yy")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Date}]
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); types=[Date], dateformat="mm/dd/yy")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{Date}]
f = CSV.Source(joinpath(dir, "test_datetimes.csv"); dateformat="yyyy-mm-dd HH:MM:SS.s")
@test size(f, 2) == 1
@test size(f, 1) == 3
@test Data.types(f) == [Nullable{DateTime}]
ds = Data.stream!(f, DataFrame)
@test ds[1,1].value == DateTime(2015,1,1)
@test ds[2,1].value == DateTime(2015,1,2,0,0,1)
@test ds[3,1].value == DateTime(2015,1,3,0,12,0,1)

#test bad types
f = CSV.Source(joinpath(dir, "test_float_in_int_column.csv"); types=[Int,Int,Int])
@test_throws CSV.CSVError Data.stream!(f, DataFrame)

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

#other various files found around the internet
f = CSV.Source(joinpath(dir, "baseball.csv"))
@test size(f, 2) == 15
@test size(f, 1) == 35
@test Data.header(f) == ["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test Data.types(f) == [Nullable{Int},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Int},Nullable{Int},Nullable{Float64},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict(10=>Float64,12=>Float64))
@test size(f, 2) == 18
@test size(f, 1) == 36634
@test Data.header(f) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(f) == [Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict{String,DataType}("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64))
@test size(f, 2) == 18
@test size(f, 1) == 36634
@test Data.header(f) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(f) == [Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Float64},Nullable{Int},Nullable{Float64},Nullable{Float64},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SacramentocrimeJanuary2006.csv"))
@test size(f, 2) == 9
@test size(f, 1) == 7584
@test Data.header(f) == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Sacramentorealestatetransactions.csv"))
@test size(f, 2) == 12
@test size(f, 1) == 985
@test Data.header(f) == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Int},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "SalesJan2009.csv"))
@test size(f, 2) == 12
@test size(f, 1) == 998
@test Data.header(f) == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Float64},Nullable{Float64}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "stocks.csv"))
@test size(f, 2) == 2
@test size(f, 1) == 30
@test Data.header(f) == ["Stock Name","Company Name"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "TechCrunchcontinentalUSA.csv"))
@test size(f, 2) == 10
@test size(f, 1) == 1460
@test Data.header(f) == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test Data.types(f) == [Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}},Nullable{Int},Nullable{WeakRefString{UInt8}},Nullable{WeakRefString{UInt8}}]
ds = Data.stream!(f, DataFrame)

f = CSV.Source(joinpath(dir, "Fielding.csv"))
@test size(f, 2) == 18
@test size(f, 1) == 167938
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
@time ds = Data.stream!(f, NamedTuple)

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
@show f

t = tempname()
f = CSV.Sink(t)
@show f

f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"))
types = Data.types(f)
@test isequal(CSV.parsefield(f, types[1]), Nullable(1.0))
@test isequal(CSV.parsefield(f, types[2]), Nullable("2.0"))
@test isequal(CSV.parsefield(f, eltype(types[3])), 3.0)

t = tempname()
f = open(t, "w")
Base.write(f, readstring(joinpath(dir, "test_missing_value_NULL.csv")))
seekstart(f)
source = CSV.Source(f; header=[], datarow=2, nullable=false)
df = CSV.read(source)
@test Data.header(df) == ["Column1", "Column2", "Column3"]

CSV.reset!(source)
df2 = CSV.read(source)
@test isequal(df, df2)

@test_throws ArgumentError CSV.Source(f; types = [Int, Int, Int, Int])
close(f)
f = source = nothing; gc(); gc()
rm(t)

# test tab-delimited nulls
d = CSV.read(joinpath(dir, "test_tab_null_empty.txt"); delim='\t')
@test isnull(d[2, :B])

d = CSV.read(joinpath(dir, "test_tab_null_string.txt"); delim='\t', null="NULL")
@test isnull(d[2, :B])

# read a write protected file
let fn = tempname()
    open(fn, "w") do f
        write(f, "Julia")
    end
    chmod(fn, 0o444)
    names(CSV.read(fn))[1] == :Julia
    gc(); gc()
    rm(fn)
end

# CSV with header and no data is treated the same as an empty buffer with header supplied
df1 = CSV.read(IOBuffer("a,b,c"))
df2 = CSV.read(IOBuffer(""); header=["a", "b", "c"])
@test size(df1) == (0, 3)
@test size(df2) == (0, 3)
@test df1 == df2

# Adding transforms to CSV with header but no data returns empty frame as expected
# (previously the lack of a ::String dispatch in the transform function caused an error)
transforms = Dict{Int, Function}(2 => x::Integer -> "b$x")
df1 = CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"); nullable=false, transforms=transforms)
df2 = CSV.read(IOBuffer("a,b,c\n1,b2,3\n4,b5,6"); nullable=false)
@test size(df1) == (2, 3)
@test size(df2) == (2, 3)
@test df1 == df2
df3 = CSV.read(IOBuffer("a,b,c"); nullable=false, transforms=transforms)
df4 = CSV.read(IOBuffer("a,b,c"); nullable=false)
@test size(df3) == (0, 3)
@test size(df4) == (0, 3)
@test df3 == df4

let fn = tempname()
    df = CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"), CSV.Sink(fn); nullable=false, transforms=transforms)
    @test readstring(fn) == "\"a\",\"b\",\"c\"\n1,\"b2\",3\n4,\"b5\",6\n"
    rm(fn)
end

let fn = tempname()
    df = CSV.read(IOBuffer("a,b,c"), CSV.Sink(fn); nullable=false, transforms=transforms)
    @test readstring(fn) == "\"a\",\"b\",\"c\"\n"
    rm(fn)
end
