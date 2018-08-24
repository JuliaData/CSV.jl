using CSV, Test, Dates, Tables, CategoricalArrays

# const dir = joinpath(dirname(@__FILE__),"test_files/")
dir = "/Users/jacobquinn/.julia/dev/CSV/test/test_files"

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

# @testset "CSV.File" begin

#test on non-existent file
@test_throws SystemError CSV.File("");
#test where datarow > headerrow
# @test_throws ArgumentError CSV.File(joinpath(dir, "test_no_header.csv"); datarow=1, header=2);

#test bad types
f = CSV.File(joinpath(dir, "test_float_in_int_column.csv"); types=[Int, Int, Int])
@test_throws CSV.Error CSV.File(f)

# read a write protected file
let fn = tempname()
    open(fn, "w") do f
        write(f, "Julia")
    end
    chmod(fn, 0o444)
    CSV.File(fn)
    GC.gc(); GC.gc()
    @try rm(fn)
end

# Adding transforms to CSV with header but no data returns empty frame as expected
# (previously the lack of a ::String dispatch in the transform function caused an error)
transforms = Dict{Int, Function}(2 => x::Integer -> "b$x")
df1 = CSV.File(IOBuffer("a,b,c\n1,2,3\n4,5,6"); allowmissing=:none, transforms=transforms)
df2 = CSV.File(IOBuffer("a,b,c\n1,b2,3\n4,b5,6"); allowmissing=:none)
@test size(Data.schema(df1)) == (2, 3)
@test size(Data.schema(df2)) == (2, 3)
@test df1 == df2
df3 = CSV.File(IOBuffer("a,b,c"); allowmissing=:none, transforms=transforms)
df4 = CSV.File(IOBuffer("a,b,c"); allowmissing=:none)
@test size(Data.schema(df3)) == (0, 3)
@test size(Data.schema(df4)) == (0, 3)
@test df3 == df4

let fn = tempname()
    CSV.File(IOBuffer("a,b,c\n1,2,3\n4,5,6"), CSV.Sink(fn); allowmissing=:none, transforms=transforms)
    @test String(read(fn)) == "a,b,c\n1,b2,3\n4,b5,6\n"
    @try rm(fn)
end

let fn = tempname()
    CSV.File(IOBuffer("a,b,c"), CSV.Sink(fn); allowmissing=:none, transforms=transforms)
    @test String(read(fn)) == "a,b,c\n"
    @try rm(fn)
end

source = IOBuffer("col1,col2,col3") # empty dataset
f = CSV.File(source; transforms=Dict(2 => floor))
@test size(Data.schema(df)) == (0, 3)
@test Data.types(Data.schema(df)) == (Any, Any, Any)

# Integer overflow; #100
@test_throws CSV.Error CSV.File(joinpath(dir, "int8_overflow.csv"); types=[Int8])

# #137
tbl = (a=[11,22], dt=[Date(2017,12,7), Date(2017,12,14)], dttm=[DateTime(2017,12,7), DateTime(2017,12,14)])
io = IOBuffer()
CSV.write(tbl, io; delim='\t')
seekstart(io)
f = CSV.File(io; delim='\t', allowmissing=:auto)
@test (f |> columntable) == tbl


catfile(file) = run(`cat $(joinpath(dir, file))`)
readfile(file; kwargs...) = CSV.File(joinpath(dir, file); kwargs...)
schema(file; kwargs...) = Tables.schema(CSV.File(joinpath(dir, file); kwargs...))
function testfile(file, kwargs, sz, sch, testfunc)
    f = CSV.File(file isa IO ? file : joinpath(dir, file); kwargs...)
    @test Tables.schema(f) == sch
    @test size(f) == sz
    if testfunc === nothing
        f |> columntable # just test that we read the file correctly
    elseif testfunc isa Function
        testfunc(f)
    else
        @test isequal(f |> columntable, testfunc)
    end
    return f
end

testfiles = [
    # (file_name, size, schema, kwargs, test_function)
    ("test_utf8_with_BOM.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64}}},
        (col1 = Union{Missing, Float64}[1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, 5.0, 8.0], col3 = Union{Missing, Float64}[3.0, 6.0, 9.0])
    ),
    ("test_utf8.csv", (allowmissing=:auto,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = Union{Missing, Float64}[1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, 5.0, 8.0], col3 = Union{Missing, Float64}[3.0, 6.0, 9.0])
    ),
    ("test_windows.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64}}},
        (col1 = Union{Missing, Float64}[1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, 5.0, 8.0], col3 = Union{Missing, Float64}[3.0, 6.0, 9.0])
    ),
    ("test_single_column.csv", (allowmissing=:auto,),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Int64}},
        (col1=[1,2,3],)
    ),
    ("test_empty_file.csv", NamedTuple(),
        (0, 0),
        NamedTuple{(), Tuple{}},
        NamedTuple()
    ),
    ("test_empty_file_newlines.csv", NamedTuple(),
        (9, 1),
        NamedTuple{(:Column1,), Tuple{Missing}},
        (Column1 = Missing[missing, missing, missing, missing, missing, missing, missing, missing, missing],)
    ),
    ("test_simple_quoted.csv", NamedTuple(),
        (1, 2),
        NamedTuple{(:col1, :col2), Tuple{Union{Missing, String}, Union{Missing, String}}},
        (col1 = Union{Missing, String}["quoted field 1"], col2 = Union{Missing, String}["quoted field 2"])
    ),
    ("test_quoted_delim_and_newline.csv", NamedTuple(),
        (1, 2),
        NamedTuple{(:col1, :col2), Tuple{Union{Missing, String}, Union{Missing, String}}},
        (col1 = Union{Missing, String}["quoted ,field 1"], col2 = Union{Missing, String}["quoted\n field 2"])
    ),
    ("test_quoted_numbers.csv", (allowmissing=:none,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{String,Int64,Int64}},
        (col1 = ["123", "abc", "123abc"], col2 = [1, 42, 12], col3 = [1, 42, 12])
    ),
    ("test_crlf_line_endings.csv", (allowmissing=:none,),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int64, Int64, Int64}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_newline_line_endings.csv", (allowmissing=:none,),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int64, Int64, Int64}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_mac_line_endings.csv", (allowmissing=:none,),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int64, Int64, Int64}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_no_header.csv", (header=0, allowmissing=:auto),
        (3, 3),
        NamedTuple{(:Column1, :Column2, :Column3), Tuple{Float64, Float64, Float64}},
        (Column1 = [1.0, 4.0, 7.0], Column2 = [2.0, 5.0, 8.0], Column3 = [3.0, 6.0, 9.0])
    ),
    ("test_2_footer_rows.csv", (header=4, footerskip=2, allowmissing=:auto),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Int64, Int64, Int64}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_dates.csv", (allowmissing=:auto,),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Date}},
        (col1 = Date[Date("2015-01-01"), Date("2015-01-02"), Date("2015-01-03")],)
    ),
    ("test_excel_date_formats.csv", (dateformat="mm/dd/yy", allowmissing=:auto),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Date}},
        (col1 = Date[Date("2015-01-01"), Date("2015-01-02"), Date("2015-01-03")],)
    ),
    ("test_datetimes.csv", (dateformat="yyyy-mm-dd HH:MM:SS.s", allowmissing=:auto),
        (3, 1),
        NamedTuple{(:col1,), Tuple{DateTime}},
        (col1 = DateTime[DateTime("2015-01-01"), DateTime("2015-01-02T00:00:01"), DateTime("2015-01-03T00:12:00.001")],)
    ),
    ("test_missing_value_NULL.csv", (allowmissing=:auto,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,String,Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = ["2.0", "NULL", "8.0"], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_missing_value_NULL.csv", (allowmissing=:auto, missingstring="NULL"),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Union{Missing, Float64},Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_missing_value.csv", (allowmissing=:auto,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Union{Missing, Float64},Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_header_range.csv", (header=1:3, allowmissing=:auto),
        (3, 3),
        NamedTuple{(:col1_sub1_part1, :col2_sub2_part2, :col3_sub3_part3),Tuple{Int64,Int64,Int64}},
        (col1_sub1_part1 = [1, 4, 7], col2_sub2_part2 = [2, 5, 8], col3_sub3_part3 = [3, 6, 9])
    ),
    ("test_basic.csv", (types=Dict(2=>Float64), allowmissing=:auto),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int64,Float64,Int64}},
        (col1 = [1, 4, 7], col2 = [2.0, 5.0, 8.0], col3 = [3, 6, 9])
    ),
    ("test_basic_pipe.csv", (delim='|', allowmissing=:auto),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Int64, Int64, Int64}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_tab_null_empty.txt", (delim='\t', allowmissing=:auto),
        (2, 4),
        NamedTuple{(:A, :B, :C, :D),Tuple{Int64,Union{Missing, Int64},String,Int64}},
        (A = [1, 2], B = Union{Missing, Int64}[2000, missing], C = ["x", "y"], D = [100, 200])
    ),
    ("test_tab_null_string.txt", (delim='\t', allowmissing=:auto, missingstring="NULL"),
        (2, 4),
        NamedTuple{(:A, :B, :C, :D),Tuple{Int64,Union{Missing, Int64},String,Int64}},
        (A = [1, 2], B = Union{Missing, Int64}[2000, missing], C = ["x", "y"], D = [100, 200])
    ),
    # CSV with header and no data is treated the same as an empty buffer with header supplied
    (IOBuffer("a,b,c"), NamedTuple(),
        (0, 3),
        NamedTuple{(:a, :b, :c), Tuple{Missing, Missing, Missing}},
        (a = Missing[], b = Missing[], c = Missing[])
    ),
    (IOBuffer(""), (header=["a", "b", "c"],),
        (0, 3),
        NamedTuple{(:a, :b, :c), Tuple{Missing, Missing, Missing}},
        (a = Missing[], b = Missing[], c = Missing[])
    ),
    # dash as missingstring; #92
    ("dash_as_null.csv", (missingstring="-",),
        (2, 2),
        NamedTuple{(:x, :y),Tuple{Union{Missing, Int64},Union{Missing, Int64}}},
        (x = Union{Missing, Int64}[1, missing], y = Union{Missing, Int64}[2, 4])
    ),
    ("plus_as_null.csv", (missingstring="+",),
        (2, 2),
        NamedTuple{(:x, :y),Tuple{Union{Missing, Int64},Union{Missing, Int64}}},
        (x = Union{Missing, Int64}[1, missing], y = Union{Missing, Int64}[2, 4])
    ),
    # #83
    ("comma_decimal.csv", (delim=';', decimal=','),
        (2, 2),
        NamedTuple{(:x, :y), Tuple{Float64, Int64}},
        (x = Union{Missing, Float64}[3.14, 1.0], y = Union{Missing, Int64}[1, 1])
    ),
    # #86
    ("double_quote_quotechar_and_escapechar.csv", (escapechar='"',),
        (24, 5),
        NamedTuple{(:APINo, :FileNo, :CurrentWellName, :LeaseName, :OriginalWellName),Tuple{Union{Missing, Float64},Union{Missing, Int64},Union{Missing, String},Union{Missing, String},Union{Missing, String}}},
        x->(x|>columntable).OriginalWellName[24] = "NORTH DAKOTA STATE \"\"A\"\" #1"
    ),
    # #84
    ("census.txt", (delim='\t', allowmissing=:auto),
        (3, 9),
        NamedTuple{(:GEOID, :POP10, :HU10, :ALAND, :AWATER, :ALAND_SQMI, :AWATER_SQMI, :INTPTLAT, :INTPTLONG),Tuple{Int64,Int64,Int64,Int64,Int64,Float64,Float64,Float64,Float64}},
        (GEOID = [601, 602, 603], POP10 = [18570, 41520, 54689], HU10 = [7744, 18073, 25653], ALAND = [166659789, 79288158, 81880442], AWATER = [799296, 4446273, 183425], ALAND_SQMI = [64.348, 30.613, 31.614], AWATER_SQMI = [0.309, 1.717, 0.071], INTPTLAT = [18.180555, 18.362268, 18.455183], INTPTLONG = [-66.749961, -67.17613, -67.119887])
    ),
    # #79
    ("bools.csv", (allowmissing=:auto,),
        (4, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Bool,Bool,Int64}},
        (col1 = Bool[true, false, true, false], col2 = Bool[false, true, true, false], col3 = [1, 2, 3, 4])
    ),
    # #64
    # #115 (Int64 -> Union{Int64, Missing} -> Union{String, Missing} promotion)
    ("attenu.csv", (missingstring="NA", allowmissing=:auto),
        (182, 5),
        NamedTuple{(:Event, :Mag, :Station, :Dist, :Accel),Tuple{Int64,Float64,Union{Missing, String},Float64,Float64}},
        x->map(y->y[1:10], x|>columntable) == (Event = [1, 2, 2, 2, 2, 2, 2, 2, 2, 2], Mag = [7.0, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4], Station = Union{Missing, String}["117", "1083", "1095", "283", "135", "475", "113", "1008", "1028", "2001"], Dist = [12.0, 148.0, 42.0, 85.0, 107.0, 109.0, 156.0, 224.0, 293.0, 359.0], Accel = [0.359, 0.014, 0.196, 0.135, 0.062, 0.054, 0.014, 0.018, 0.01, 0.004])
    ),
    ("test_null_only_column.csv", (missingstring="NA", allowmissing=:auto),
        (3, 2),
        NamedTuple{(:col1, :col2),Tuple{String,Missing}},
        (col1 = ["123", "abc", "123abc"], col2 = Missing[missing, missing, missing])
    ),
    # #107
    (IOBuffer("1,a,i\n2,b,ii\n3,c,iii"), (datarow=1,),
        (3, 3),
        NamedTuple{(:Column1, :Column2, :Column3),Tuple{Union{Missing, Int64},Union{Missing, String},Union{Missing, String}}},
        (Column1 = Union{Missing, Int64}[1, 2, 3], Column2 = Union{Missing, String}["a", "b", "c"], Column3 = Union{Missing, String}["i", "ii", "iii"])
    ),
    ("categorical.csv", (categorical=true, allowmissing=:auto),
        (15, 1),
        NamedTuple{(:cat,), Tuple{CategoricalString{UInt32}}},
        (cat = CategoricalVector{CategoricalString{UInt32}}(["a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "c", "c", "a"]),)
    ),
    # other various files from around the interwebs
    ("baseball.csv", (categorical=true,),
        (35, 15),
        NamedTuple{(:Rk, :Year, :Age, :Tm, :Lg, :Column6, :W, :L, Symbol("W-L%"), :G, :Finish, :Wpost, :Lpost, Symbol("W-L%post"), :Column15), Tuple{Union{Int64, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{CategoricalString{UInt32}, Missing},Union{CategoricalString{UInt32}, Missing},Union{String, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{Int64, Missing},Union{Int64, Missing},Union{Float64, Missing},Union{CategoricalString{UInt32}, Missing}}},
        
    ),

]

\epsilon    

#other various files found around the internet
f = CSV.File(joinpath(dir, "baseball.csv"); categorical=true)
@test size(Data.schema(f), 2) == 15
@test size(Data.schema(f), 1) == 35
@test Data.header(Data.schema(f)) == 
@test Data.types(Data.schema(f)) == ()
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "FL_insurance_sample.csv"); types=Dict(10=>Float64,12=>Float64), allowmissing=:auto)
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "FL_insurance_sample.csv");types=Dict("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64),allowmissing=:auto)
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 36634
@test Data.header(Data.schema(f)) == ["policyID","statecode","county","eq_site_limit","hu_site_limit","fl_site_limit","fr_site_limit","tiv_2011","tiv_2012","eq_site_deductible","hu_site_deductible","fl_site_deductible","fr_site_deductible","point_latitude","point_longitude","line","construction","point_granularity"]
@test Data.types(Data.schema(f)) == (Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "SacramentocrimeJanuary2006.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 9
@test size(Data.schema(f), 1) == 7584
@test Data.header(Data.schema(f)) == ["cdatetime","address","district","beat","grid","crimedescr","ucr_ncic_code","latitude","longitude"]
@test Data.types(Data.schema(f)) == (CategoricalString{UInt32},String,Int64,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Float64,Float64)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "Sacramentorealestatetransactions.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 985
@test Data.header(Data.schema(f)) == ["street","city","zip","state","beds","baths","sq__ft","type","sale_date","price","latitude","longitude"]
@test Data.types(Data.schema(f)) == (String,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Int64,Int64,CategoricalString{UInt32},CategoricalString{UInt32},Int64,Float64,Float64)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "SalesJan2009.csv"); types=Dict(3=>String,7=>Union{String, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 12
@test size(Data.schema(f), 1) == 998
@test Data.header(Data.schema(f)) == ["Transaction_date","Product","Price","Payment_Type","Name","City","State","Country","Account_Created","Last_Login","Latitude","Longitude"]
@test Data.types(Data.schema(f)) == (String,CategoricalString{UInt32},String,CategoricalString{UInt32},String,String,Union{String, Missing},CategoricalString{UInt32},String,String,Float64,Float64)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "stocks.csv"), allowmissing=:auto)
@test size(Data.schema(f), 2) == 2
@test size(Data.schema(f), 1) == 30
@test Data.header(Data.schema(f)) == ["Stock Name","Company Name"]
@test Data.types(Data.schema(f)) == (String,String)
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "TechCrunchcontinentalUSA.csv"); types=Dict(4=>Union{String, Missing},5=>Union{String, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 10
@test size(Data.schema(f), 1) == 1460
@test Data.header(Data.schema(f)) == ["permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round"]
@test Data.types(Data.schema(f)) == (CategoricalString{UInt32},CategoricalString{UInt32},Union{Int64, Missing},Union{String, Missing},Union{String, Missing},CategoricalString{UInt32},CategoricalString{UInt32},Int64,CategoricalString{UInt32},CategoricalString{UInt32})
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "Fielding.csv"); allowmissing=:none, types=Dict("GS"=>Union{Int, Missing},"PO"=>Union{Int, Missing},"A"=>Union{Int, Missing},"E"=>Union{Int, Missing},"DP"=>Union{Int, Missing},"PB"=>Union{Int, Missing},"InnOuts"=>Union{Int, Missing},"WP"=>Union{Int, Missing},"SB"=>Union{Int, Missing},"CS"=>Union{Int, Missing},"ZR"=>Union{Int, Missing}))
@test size(Data.schema(f), 2) == 18
@test size(Data.schema(f), 1) == 167938
@test Data.header(Data.schema(f)) == ["playerID","yearID","stint","teamID","lgID","POS","G","GS","InnOuts","PO","A","E","DP","PB","WP","SB","CS","ZR"]
@test Data.types(Data.schema(f)) == (CategoricalArrays.CategoricalString{UInt32}, Int64, Int64, CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, Int64, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing})
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "latest (1).csv"); header=0, missingstring="\\N", types=Dict(13=>Union{Float64, Missing},17=>Union{Int, Missing},18=>Union{Float64, Missing},20=>Union{Float64, Missing}), allowmissing=:auto)
@test size(Data.schema(f), 2) == 25
@test size(Data.schema(f), 1) == 1000
@test Data.header(Data.schema(f)) == ["Column$i" for i = 1:size(Data.schema(f), 2)]
@test Data.types(Data.schema(f)) == (CategoricalArrays.CategoricalString{UInt32}, CategoricalArrays.CategoricalString{UInt32}, Int64, Int64, CategoricalArrays.CategoricalString{UInt32}, Int64, CategoricalArrays.CategoricalString{UInt32}, Int64, Date, Date, Int64, CategoricalArrays.CategoricalString{UInt32}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Float64, Missing}, Union{Int, Missing}, Union{Float64, Missing}, Float64, Union{Float64, Missing}, Union{Float64, Missing}, Union{Int64, Missing}, Float64, Union{Float64, Missing}, Union{Float64, Missing})
ds = CSV.File(f)

f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
@test size(Data.schema(f), 2) == 50
@test size(Data.schema(f), 1) == 100000
@test Data.header(Data.schema(f)) == [string(i) for i = 0:49]
@test Data.types(Data.schema(f)) == (fill(Int64,50)...,)
@time ds = CSV.File(f)
# f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
# @time ds = CSV.File(f)
# f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
# using Profile
# Profile.clear()
# @profile CSV.File(f)
# Profile.print(C=true)

end # testset

@testset "CSV.TransposedSource" begin

# CSV.TransposedSource
f = CSV.File(joinpath(dir, "transposed.csv"); transpose=true)
@test size(df) == (3, 3)
@test Data.header(Data.schema(df)) == ["col1", "col2", "col3"]
@test df[1][1] == 1
@test df[1][2] == 2
@test df[1][3] == 3

f = CSV.File(joinpath(dir, "transposed_1row.csv"); transpose=true)
@test size(df) == (1, 1)

f = CSV.File(joinpath(dir, "transposed_emtpy.csv"); transpose=true)
@test size(df) == (0, 1)

f = CSV.File(joinpath(dir, "transposed_extra_newline.csv"); transpose=true)
@test size(df) == (2, 2)

f = CSV.File(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=0)
@test size(df) == (2, 3)
@test Data.header(Data.schema(df)) == ["Column1", "Column2", "Column3"]

f = CSV.File(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=["c1", "c2", "c3"])
@test size(df) == (2, 3)
@test Data.header(Data.schema(df)) == ["c1", "c2", "c3"]

for test in testfiles
    testfile(test...)
end

end # testset
