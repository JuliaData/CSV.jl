using DataStreams, DataFrames

# `CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer())` => `String`
str = "field1,field2,\"quoted \\\"field with \n embedded newline\",field3"
io = IOBuffer(str)
CSV.readline!(io)
@test eof(io)
io = IOBuffer(str * "\n" * str * "\r\n" * str)
CSV.readline!(io)
@test position(io) == 62
CSV.readline!(io)
@test position(io) == 125
CSV.readline!(io)
@test eof(io)

# `CSV.readline(source::CSV.Source)` => `String`
strsource = CSV.Source(IOBuffer(str); header=["col1","col2","col3","col4"])
CSV.readline!(strsource)
@test eof(strsource.io)

# `CSV.readsplitline(io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())` => `Vector{String}`
spl = ["field1", "field2", "quoted \"field with \n embedded newline", "field3"]
io = IOBuffer(str)
@test CSV.readsplitline(io) == spl
io = IOBuffer(str * "\n" * str * "\r\n" * str)
@test CSV.readsplitline(io) == spl
@test CSV.readsplitline(io) == spl
@test CSV.readsplitline(io) == spl

@testset "empty fields" begin
    str2 = "field1,,\"\",field3,"
    spl2 = ["field1", "", "", "field3", ""]
    ioo = IOBuffer(str2)
    @test CSV.readsplitline(ioo) == spl2
end

# `CSV.countlines(io::IO, quotechar, escapechar)` => `Int`
@test CSV.countlines(IOBuffer(str)) == 1
@test CSV.countlines(IOBuffer(str * "\n" * str)) == 2

@testset "misformatted CSV lines" begin
    @testset "missing quote" begin
        str1 = "field1,field2,\"quoted \\\"field with \n embedded newline,field3"
        io2 = IOBuffer(str1)
        @test_throws CSV.Error CSV.readsplitline(io2)
    end

    @testset "misplaced quote" begin
        str1 = "fi\"eld1\",field2,\"quoted \\\"field with \n embedded newline\",field3"
        io2 = IOBuffer(str1)
        # @test_throws CSV.ParsingException CSV.readsplitline(io2)

        str2 = "field1,field2,\"quoted \\\"field with \n\"\" embedded newline\",field3"
        io2 = IOBuffer(str2)
        @test_throws CSV.Error CSV.readsplitline(io2)

        str3 = "\"field\"1,field2,\"quoted \\\"field with \n embedded newline\",field3"
        io2 = IOBuffer(str3)
        @test_throws CSV.Error CSV.readsplitline(io2)
    end
end

@testset "Basic CSV.Source" begin

#test on non-existent file
@test_throws ArgumentError CSV.Source("");

#test where datarow > headerrow
@test_throws ArgumentError CSV.Source(joinpath(dir, "test_no_header.csv"); datarow=1, header=2);

#test various encodings
f = CSV.Source(joinpath(dir, "test_utf8_with_BOM.csv"))
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]

f = CSV.Source(joinpath(dir, "test_utf8.csv"), allowmissing=:auto)
sch = Data.schema(f)
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

# f = CSV.Source(joinpath(dir, "test_utf16_be.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16_le.csv"))
# f = CSV.Source(joinpath(dir, "test_utf16.csv"))
f = CSV.Source(joinpath(dir, "test_windows.csv"))
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3

#test one column file
f = CSV.Source(joinpath(dir, "test_single_column.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1"]
@test Data.types(Data.schema(f)) == (Int64,)
ds = CSV.read(f)
@test ds[1][1] == 1
@test ds[1][2] == 2
@test ds[1][3] == 3

#test empty file
f = CSV.Source(joinpath(dir, "test_empty_file.csv"))
@test ismissing(size(Data.schema(f), 1))

#test file with just newlines
f = CSV.Source(joinpath(dir, "test_empty_file_newlines.csv"))
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 9
@test Data.header(Data.schema(f)) == [""]
@test Data.types(Data.schema(f)) == (Missing,)

#test with various quotechars, escapechars
f = CSV.Source(joinpath(dir, "test_simple_quoted.csv"))
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 1
ds = CSV.read(f)
@test String(ds[1][1]) == "quoted field 1"
@test String(ds[2][1]) == "quoted field 2"
f = CSV.Source(joinpath(dir, "test_quoted_delim_and_newline.csv"))
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 1

f = CSV.Source(joinpath(dir, "test_quoted_numbers.csv"); categorical=false, strings=:weakref, allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
ds = CSV.read(f)
@test Data.types(Data.schema(f)) == (WeakRefString{UInt8}, Int64, Int64)

#test various newlines
f = CSV.Source(joinpath(dir, "test_crlf_line_endings.csv"), allowmissing=:auto)
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int64,Int64,Int64)
ds = CSV.read(f)
@test ds[1][1] == 1
f = CSV.Source(joinpath(dir, "test_newline_line_endings.csv"), allowmissing=:auto)
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int64,Int64,Int64)
f = CSV.Source(joinpath(dir, "test_mac_line_endings.csv"), allowmissing=:auto)
@test Data.header(Data.schema(f)) == ["col1","col2","col3"]
@test size(Data.schema(f), 2) == 3
@test Data.types(Data.schema(f)) == (Int64,Int64,Int64)

end # testset

@testset "CSV.Source keyword arguments" begin

#test headerrow, datarow, footerskips
f = CSV.Source(joinpath(dir, "test_no_header.csv"); header=0, datarow=1, allowmissing=:auto)
@test Data.header(Data.schema(f)) == ["Column1", "Column2", "Column3"]
@test Data.types(Data.schema(f)) == (Float64, Float64, Float64)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
f = CSV.Source(joinpath(dir, "test_2_footer_rows.csv"); header=4, datarow=5, footerskip=2, allowmissing=:auto)
@test Data.header(Data.schema(f)) == ["col1", "col2", "col3"]
@test Data.types(Data.schema(f)) == (Int64, Int64, Int64)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3

#test dates, dateformats
f = CSV.Source(joinpath(dir, "test_dates.csv"); types=[Date], dateformat="yyyy-mm-dd", allowmissing=:auto)
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
ds = CSV.read(f)
@test ds[1][1] == Date(2015,1,1)
@test ds[1][2] == Date(2015,1,2)
@test ds[1][3] == Date(2015,1,3)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); dateformat="mm/dd/yy", allowmissing=:auto)
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
f = CSV.Source(joinpath(dir, "test_excel_date_formats.csv"); types=[Date], dateformat="mm/dd/yy", allowmissing=:auto)
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Date,)
f = CSV.Source(joinpath(dir, "test_datetimes.csv"); dateformat="yyyy-mm-dd HH:MM:SS.s", allowmissing=:auto)
@test size(Data.schema(f), 2) == 1
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (DateTime,)
ds = CSV.read(f)
@test ds[1][1] == DateTime(2015,1,1)
@test ds[1][2] == DateTime(2015,1,2,0,0,1)
@test ds[1][3] == DateTime(2015,1,3,0,12,0,1)

#test bad types
f = CSV.Source(joinpath(dir, "test_float_in_int_column.csv"); types=[Int, Int, Int])
@test_throws CSV.Error CSV.read(f)

#test missing values
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"); categorical=false, allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Float64, String, Float64)
ds = CSV.read(f)
@test ds[1][1] == 1.0
@test string(ds[2][1]) == "2.0"
@test string(ds[2][2]) == "NULL"
f = CSV.Source(joinpath(dir, "test_missing_value_NULL.csv"); missingstring="NULL", allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Float64, Union{Float64, Missing}, Float64)
ds = CSV.read(f)
@test ds[1][1] == 1.0
@test ds[2][1] == 2.0
@test ismissing(ds[2][2])

# uses default missing value ""
f = CSV.Source(joinpath(dir, "test_missing_value.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Float64, Union{Float64, Missing}, Float64)

f = CSV.Source(joinpath(dir, "test_header_range.csv"); header=1:3, allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1_sub1_part1", "col2_sub2_part2", "col3_sub3_part3"]
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_header_range.csv"); header=["col1_sub1_part1", "col2_sub2_part2", "col3_sub3_part3"], datarow=4, allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.header(Data.schema(f)) == ["col1_sub1_part1", "col2_sub2_part2", "col3_sub3_part3"]
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic.csv"); types=Dict(2=>Float64), allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Int64, Float64, Int64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv"); delim='|', allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 3
@test Data.types(Data.schema(f)) == (Int64,Int64,Int64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "test_basic_pipe.csv"); delim='|', footerskip=1, allowmissing=:auto)
@test size(Data.schema(f), 2) == 3
@test size(Data.schema(f), 1) == 2
@test Data.types(Data.schema(f)) == (Int64,Int64,Int64)
ds = CSV.read(f)

t = tempname()
f = open(t, "w+")
Base.write(f, String(read(joinpath(dir, "test_missing_value_NULL.csv"))))
seekstart(f)
source = CSV.Source(f; header=[], datarow=2, allowmissing=:none)
df = CSV.read(source)
@test Data.header(Data.schema(df)) == ["Column1", "Column2", "Column3"]

Data.reset!(source)
df2 = CSV.read(source)
@test df == df2

@test_throws ArgumentError CSV.Source(f; types = [Int, Int, Int, Int])
close(f)
f = source = nothing; GC.gc(); GC.gc()
@try rm(t)

# test tab-delimited missing values
d = CSV.read(joinpath(dir, "test_tab_null_empty.txt"); delim='\t')
@test ismissing(d[2][2])

d = CSV.read(joinpath(dir, "test_tab_null_string.txt"); delim='\t', missingstring="NULL")
@test ismissing(d[2][2])

# read a write protected file
let fn = tempname()
    open(fn, "w") do f
        write(f, "Julia")
    end
    chmod(fn, 0o444)
    CSV.read(fn)
    GC.gc(); GC.gc()
    @try rm(fn)
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
df1 = CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"); allowmissing=:none, transforms=transforms)
df2 = CSV.read(IOBuffer("a,b,c\n1,b2,3\n4,b5,6"); allowmissing=:none)
@test size(Data.schema(df1)) == (2, 3)
@test size(Data.schema(df2)) == (2, 3)
@test df1 == df2
df3 = CSV.read(IOBuffer("a,b,c"); allowmissing=:none, transforms=transforms)
df4 = CSV.read(IOBuffer("a,b,c"); allowmissing=:none)
@test size(Data.schema(df3)) == (0, 3)
@test size(Data.schema(df4)) == (0, 3)
@test df3 == df4

source = IOBuffer("col1,col2,col3") # empty dataset
df = CSV.read(source; transforms=Dict(2 => floor))
@test size(Data.schema(df)) == (0, 3)
@test Data.types(Data.schema(df)) == (Missing, Missing, Missing)


# dash as missingstring; #92
df = CSV.read(joinpath(dir, "dash_as_null.csv"); missingstring="-")
@test ismissing(df[1][2])

df = CSV.read(joinpath(dir, "plus_as_null.csv"); missingstring="+")
@test ismissing(df[1][2])

# #83
df = CSV.read(joinpath(dir, "comma_decimal.csv"); delim=';', decimal=',')
@test df[1][1] === 3.14
@test df[1][2] === 1.0
@test df[2][1] === Int64(1)
@test df[2][2] === Int64(1)

# #86
df = CSV.read(joinpath(dir, "double_quote_quotechar_and_escapechar.csv"); escapechar='"')
@test size(df) == (24, 5)
@test df[5][24] == "NORTH DAKOTA STATE \"A\" #1"

# #84
df = CSV.read(joinpath(dir, "census.txt"); delim='\t', allowmissing=:auto)
@test eltype(df[9]) == Float64
@test size(df) == (3, 9)

# #79
df = CSV.read(joinpath(dir, "bools.csv"), allowmissing=:auto)
@test eltype(df[1]) == Bool
@test df[1] == [true, false, true, false]
@test df[2] == [false, true, true, false]
@test df[3] == [1, 2, 3, 4]

# #64
df = CSV.read(joinpath(dir, "attenu.csv"), missingstring="NA", types=Dict(3=>Union{Missing, String}))
@test size(df) == (182, 5)

f = CSV.Source(joinpath(dir, "test_null_only_column.csv"), categorical=false, missingstring="NA", allowmissing=:auto)
@test size(Data.schema(f)) == (3, 2)
ds = CSV.read(f)
@test Data.types(Data.schema(f)) == (String, Missing)
@test all(ismissing, ds[2])

# #107
df = CSV.read(IOBuffer("1,a,i\n2,b,ii\n3,c,iii"); datarow=1)
@test size(df) == (3, 3)

# #115 (Int64 -> Union{Int64, Missing} -> Union{String, Missing} promotion)
df = CSV.read(joinpath(dir, "attenu.csv"), missingstring="NA", rows_for_type_detect=200, allowmissing=:auto)
@test size(df) == (182, 5)
@test Data.types(Data.schema(df)) == (Int64, Float64, Union{Missing, String}, Float64, Float64)

# #137
tbl = DataFrame(a=[11,22], dt=[Date(2017,12,7), Date(2017,12,14)])
tbl[:dttm] = DateTime.(tbl[:dt])
CSV.write("test.tsv", tbl; delim='\t')
df = CSV.read("test.tsv"; delim='\t', allowmissing=:auto)
@test Data.types(Data.schema(df)) == (Int64, Date, DateTime)
df = nothing; GC.gc(); GC.gc()
@try rm("test.tsv")

end # testset

@testset "CSV.Source various files" begin

#other various files found around the internet
f = CSV.Source(joinpath(dir, "baseball.csv"); rows_for_type_detect=35, strings=:weakref)
@test size(Data.schema(f), 2) == 15
@test size(Data.schema(f), 1) == 35
@test Data.header(Data.schema(f)) == ["Rk","Year","Age","Tm","Lg","","W","L","W-L%","G","Finish","Wpost","Lpost","W-L%post",""]
@test Data.types(Data.schema(f)) == (Union{Int64, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{CategoricalString{UInt32}, Missing},Union{CategoricalString{UInt32}, Missing},Union{WeakRefString{UInt8}, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{CategoricalString{UInt32}, Missing})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv"); types=Dict(10=>Float64,12=>Float64), allowmissing=:auto)
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "FL_insurance_sample.csv");types=Dict("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64),allowmissing=:auto)
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "SacramentocrimeJanuary2006.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 9
@test size(Data.schema(f), 1) == 7584
@test Data.header(Data.schema(f)) == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test Data.types(Data.schema(f)) == (CategoricalString{UInt32},String,Int64,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "Sacramentorealestatetransactions.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 985
@test Data.header(Data.schema(f)) == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test Data.types(Data.schema(f)) == (String,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Int64,Int64,CategoricalString{UInt32},CategoricalString{UInt32},Int64,Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "SalesJan2009.csv"); types=Dict(3=>String,7=>Union{String, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 998
@test Data.header(Data.schema(f)) == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test Data.types(Data.schema(f)) == (String,CategoricalString{UInt32},String,CategoricalString{UInt32},String,String,Union{String, Missing},CategoricalString{UInt32},String,String,Float64,Float64)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "stocks.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 30
@test Data.header(Data.schema(f)) == ["Stock Name","Company Name"]
@test Data.types(Data.schema(f)) == (String,String)
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "TechCrunchcontinentalUSA.csv"); types=Dict(4=>Union{String, Missing},5=>Union{String, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 10
@test size(Data.schema(f), 1) == 1460
@test Data.header(Data.schema(f)) == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test Data.types(Data.schema(f)) == (CategoricalString{UInt32},CategoricalString{UInt32},Union{Int64, Missing},Union{String, Missing},Union{String, Missing},CategoricalString{UInt32},CategoricalString{UInt32},Int64,CategoricalString{UInt32},CategoricalString{UInt32})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "Fielding.csv"); allowmissing=:none, types=Dict("GS"=>Union{Int, Missing},"PO"=>Union{Int, Missing},"A"=>Union{Int, Missing},"E"=>Union{Int, Missing},"DP"=>Union{Int, Missing},"PB"=>Union{Int, Missing},"InnOuts"=>Union{Int, Missing},"WP"=>Union{Int, Missing},"SB"=>Union{Int, Missing},"CS"=>Union{Int, Missing},"ZR"=>Union{Int, Missing}))
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 167938
@test Data.header(Data.schema(f)) == ["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test Data.types(Data.schema(f)) == (CategoricalArrays.CategoricalString{UInt32}, Int64, Int64, CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, Int64, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "latest (1).csv"); header=0, missingstring="\\N", types=Dict(13=>Union{Float64, Missing},17=>Union{Int, Missing},18=>Union{Float64, Missing},20=>Union{Float64, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 25
@test size(Data.schema(f), 1) == 1000
@test Data.header(Data.schema(f)) == ["Column$i" for i = 1:size(Data.schema(f), 2)]
@test Data.types(Data.schema(f)) == (CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, Int64, Int64, CategoricalArrays.CategoricalString{UInt32}, Int64, CategoricalArrays.CategoricalString{UInt32}, Int64, Date, Date, Int64, CategoricalArrays.CategoricalString{UInt32}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Int, Missing}, Union{Float64, Missing}, Float64, Union{Float64, Missing}, Union{Float64, Missing}, Union{Int64, Missing}, Float64, Union{Float64, Missing}, Union{Float64, Missing})
ds = CSV.read(f)

f = CSV.Source(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
@test size(Data.schema(f), 2) == 50
@test size(Data.schema(f), 1) == 100000
@test Data.header(Data.schema(f)) == [string(i) for i = 0:49]
@test Data.types(Data.schema(f)) == (fill(Int64,50)...,)
@time ds = CSV.read(f)
# f = CSV.Source(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
# @time ds = CSV.read(f)
# f = CSV.Source(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
# using Profile
# Profile.clear()
# @profile CSV.read(f)
# Profile.print(C=true)

end # testset

@testset "CSV.TransposedSource" begin

# CSV.TransposedSource
df = CSV.read(joinpath(dir, "transposed.csv"); transpose=true)
@test size(df) == (3, 3)
@test Data.header(Data.schema(df)) == ["col1", "col2", "col3"]
@test df[1][1] == 1
@test df[1][2] == 2
@test df[1][3] == 3

df = CSV.read(joinpath(dir, "transposed_1row.csv"); transpose=true)
@test size(df) == (1, 1)

df = CSV.read(joinpath(dir, "transposed_emtpy.csv"); transpose=true)
@test size(df) == (0, 1)

df = CSV.read(joinpath(dir, "transposed_extra_newline.csv"); transpose=true)
@test size(df) == (2, 2)

df = CSV.read(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=0)
@test size(df) == (2, 3)
@test Data.header(Data.schema(df)) == ["Column1", "Column2", "Column3"]

df = CSV.read(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=["c1", "c2", "c3"])
@test size(df) == (2, 3)
@test Data.header(Data.schema(df)) == ["c1", "c2", "c3"]

end # testset

@testset "Write to IOBuffer" begin
    csv_string = chomp(read(joinpath(dir, "test_basic.csv"), String))
    df = CSV.read(IOBuffer(csv_string))
    io = IOBuffer()
    CSV.write(io, df)
    written = chomp(String(take!(io)))
    @test written == csv_string
end

# Previous versions assumed that bytesavailable could accurately check for an empty CSV, but
# this doesn't work reliably for streams because bytesavailable only checks buffered bytes
# (see issue #77). This test verifies that even when bytesavailable would return 0 on a stream
# the full stream is still read.

mutable struct MultiStream{S<:IO} <: IO
    streams::Array{S}
    index::Int
end

function MultiStream(streams::AbstractArray{S}) where {S <: IO}
    MultiStream(streams, 1)
end

function refill(s::MultiStream)
    while eof(s.streams[s.index]) && s.index < length(s.streams)
        close(s.streams[s.index])
        s.index += 1
    end
end

function Base.close(s::MultiStream)
    for i in s.index:length(s.streams)
        close(s.streams[i])
    end
    s.index = length(s.streams)
    nothing
end

function Base.eof(s::MultiStream)
    eof(s.streams[s.index]) && s.index == length(s.streams)
end

function Base.read(s::MultiStream, ::Type{UInt8})
    refill(s)
    read(s.streams[s.index], UInt8)::UInt8
end

function Base.bytesavailable(s::MultiStream)
    bytesavailable(s.streams[s.index])
end

stream = MultiStream(
    [IOBuffer(""), IOBuffer("a,b,c\n1,2,3\n"), IOBuffer(""), IOBuffer("4,5,6")]
)

@test bytesavailable(stream) == 0
@test CSV.read(CSV.Source(stream)) == CSV.read(IOBuffer("a,b,c\n1,2,3\n4,5,6"))
