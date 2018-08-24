using CSV, Test, Dates, Tables, CategoricalArrays, WeakRefStrings

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
        NamedTuple{(:x, :y),Tuple{Union{Missing, Float64},Union{Missing, Int64}}},
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
        nothing
    ),
    ("FL_insurance_sample.csv", (types=Dict(10=>Float64,12=>Float64), allowmissing=:auto, categorical=true),
        (36634, 18),
        NamedTuple{(:policyID, :statecode, :county, :eq_site_limit, :hu_site_limit, :fl_site_limit, :fr_site_limit, :tiv_2011, :tiv_2012, :eq_site_deductible, :hu_site_deductible, :fl_site_deductible, :fr_site_deductible, :point_latitude, :point_longitude, :line, :construction, :point_granularity),Tuple{Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64}},
        nothing
    ),
    ("FL_insurance_sample.csv", (types=Dict("eq_site_deductible"=>Float64,"fl_site_deductible"=>Float64), allowmissing=:auto, categorical=true),
        (36634, 18),
        NamedTuple{(:policyID, :statecode, :county, :eq_site_limit, :hu_site_limit, :fl_site_limit, :fr_site_limit, :tiv_2011, :tiv_2012, :eq_site_deductible, :hu_site_deductible, :fl_site_deductible, :fr_site_deductible, :point_latitude, :point_longitude, :line, :construction, :point_granularity),Tuple{Int64,CategoricalString{UInt32},CategoricalString{UInt32},Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Float64,Float64,CategoricalString{UInt32},CategoricalString{UInt32},Int64}},
        nothing
    ),
    ("SacramentocrimeJanuary2006.csv", (allowmissing=:auto, categorical=true),
        (7584, 9),
        NamedTuple{(:cdatetime, :address, :district, :beat, :grid, :crimedescr, :ucr_ncic_code, :latitude, :longitude),Tuple{String,String,Int64,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Float64,Float64}},
        nothing
    ),
    ("Sacramentorealestatetransactions.csv", (allowmissing=:auto, categorical=true),
        (985, 12),
        NamedTuple{(:street, :city, :zip, :state, :beds, :baths, :sq__ft, :type, :sale_date, :price, :latitude, :longitude),Tuple{String,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Int64,Int64,CategoricalString{UInt32},CategoricalString{UInt32},Int64,Float64,Float64}},
        nothing
    ),
    ("SalesJan2009.csv", (allowmissing=:auto, categorical=true),
        (998, 12),
        NamedTuple{(:Transaction_date, :Product, :Price, :Payment_Type, :Name, :City, :State, :Country, :Account_Created, :Last_Login, :Latitude, :Longitude),Tuple{String,CategoricalString{UInt32},CategoricalString{UInt32},CategoricalString{UInt32},String,String,Union{Missing, CategoricalString{UInt32}},CategoricalString{UInt32},String,String,Float64,Float64}},
        nothing
    ),
    ("stocks.csv", (allowmissing=:auto,),
        (30, 2),
        NamedTuple{(Symbol("Stock Name"), Symbol("Company Name")), Tuple{String, String}},
        nothing
    ),
    ("TechCrunchcontinentalUSA.csv", (allowmissing=:auto, categorical=true),
        (1460, 10),
        NamedTuple{(:permalink, :company, :numEmps, :category, :city, :state, :fundedDate, :raisedAmt, :raisedCurrency, :round),Tuple{CategoricalString{UInt32},CategoricalString{UInt32},Union{Missing, Int64},Union{Missing, CategoricalString{UInt32}},Union{Missing, CategoricalString{UInt32}},CategoricalString{UInt32},CategoricalString{UInt32},Int64,CategoricalString{UInt32},CategoricalString{UInt32}}},
        nothing
    ),
    ("Fielding.csv", (allowmissing=:none,),
        (167938, 18),
        NamedTuple{(:playerID, :yearID, :stint, :teamID, :lgID, :POS, :G, :GS, :InnOuts, :PO, :A, :E, :DP, :PB, :WP, :SB, :CS, :ZR),Tuple{WeakRefString{UInt8},Int64,Int64,WeakRefString{UInt8},WeakRefString{UInt8},WeakRefString{UInt8},Int64,Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64},Union{Missing, Int64}}},
        nothing
    ),
    ("latest (1).csv", (header=0, missingstring="\\N", allowmissing=:auto, categorical=true),
        (1000, 25),
        NamedTuple{(:Column1, :Column2, :Column3, :Column4, :Column5, :Column6, :Column7, :Column8, :Column9, :Column10, :Column11, :Column12, :Column13, :Column14, :Column15, :Column16, :Column17, :Column18, :Column19, :Column20, :Column21, :Column22, :Column23, :Column24, :Column25),Tuple{CategoricalString{UInt32},CategoricalString{UInt32},Int64,Int64,CategoricalString{UInt32},Int64,CategoricalString{UInt32},Int64,Date,Date,Int64,CategoricalString{UInt32},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Int64},Union{Missing, Float64},Float64,Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Int64},Float64,Union{Missing, Float64},Union{Missing, Float64}}},
        nothing
    ),
    # #217
    (IOBuffer("aa,bb\n1,\"1,b,c\"\n"), (allowmissing=:auto,),
        (1, 2),
        NamedTuple{(:aa, :bb), Tuple{Int, String}},
        (aa = [1], bb = ["1,b,c"])
    ),
    # #198
    ("issue_198.csv", (decimal=',', delim=';', missingstring="-", datarow = 2, header = ["Date", "EONIA", "1m", "12m", "3m", "6m", "9m"]),
        (6, 7),
        NamedTuple{(:Date, :EONIA, Symbol("1m"), Symbol("12m"), Symbol("3m"), Symbol("6m"), Symbol("9m")),Tuple{Union{Missing, String},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64},Union{Missing, Float64}}},
        NamedTuple{(:Date, :EONIA, Symbol("1m"), Symbol("12m"), Symbol("3m"), Symbol("6m"), Symbol("9m"))}((Union{Missing, String}["18/04/2018", "17/04/2018", "16/04/2018", "15/04/2018", "14/04/2018", "13/04/2018"], Union{Missing, Float64}[-0.368, -0.368, -0.367, missing, missing, -0.364], Union{Missing, Float64}[-0.371, -0.371, -0.371, missing, missing, -0.371], Union{Missing, Float64}[-0.189, -0.189, -0.189, missing, missing, -0.19], Union{Missing, Float64}[-0.328, -0.328, -0.329, missing, missing, -0.329], Union{Missing, Float64}[-0.271, -0.27, -0.27, missing, missing, -0.271], Union{Missing, Float64}[-0.219, -0.219, -0.219, missing, missing, -0.219]))
    ),
    # #198 part 2
    ("issue_198_part2.csv", (missingstring="++", delim=';', decimal=','),
        (4, 3),
        NamedTuple{(:A, :B, :C),Tuple{Union{Missing, String},Union{Missing, Float64},Union{Missing, Float64}}},
        (A = Union{Missing, String}["a", "b", "c", "d"], B = Union{Missing, Float64}[-0.367, missing, missing, -0.364], C = Union{Missing, Float64}[-0.371, missing, missing, -0.371])
    ),
    # #207
    ("issue_207.csv", (allowmissing=:auto,),
        (2, 6),
        NamedTuple{(:a, :b, :c, :d, :e, :f),Tuple{Int64,Int64,Int64,Float64,String,Union{Missing, Float64}}},
        (a = [1863001, 1863209], b = [134, 137], c = [10000, 0], d = [1.0009, 1.0], e = ["1.0000", "2,773.9000"], f = Union{Missing, Float64}[-0.002033899, missing])
    ),
    # #120
    ("issue_120.csv", (allowmissing=:auto, header=0),
        (5, 20),
        NamedTuple{(:Column1, :Column2, :Column3, :Column4, :Column5, :Column6, :Column7, :Column8, :Column9, :Column10, :Column11, :Column12, :Column13, :Column14, :Column15, :Column16, :Column17, :Column18, :Column19, :Column20),Tuple{Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int64,Int64,Int64,Int64,Missing,Float64,Float64,Float64,Float64,Float64,Float64}},
        (Column1 = [3.52848962348857e9, 3.52848962448866e9, 3.52848962548857e9, 3.52848962648866e9, 3.52848962748875e9], Column2 = [312.73, 312.49, 312.74, 312.49, 312.62], Column3 = [0.0, 0.0, 0.0, 0.0, 0.0], Column4 = [41.87425, 41.87623, 41.87155, 41.86422, 41.87615], Column5 = [297.6302, 297.6342, 297.6327, 297.632, 297.6324], Column6 = [0.0, 0.0, 0.0, 0.0, 0.0], Column7 = [286.3423, 286.3563, 286.3723, 286.3837, 286.397], Column8 = [-99.99, -99.99, -99.99, -99.99, -99.99], Column9 = [-99.99, -99.99, -99.99, -99.99, -99.99], Column10 = [12716, 12716, 12716, 12716, 12716], Column11 = [0, 0, 0, 0, 0], Column12 = [0, 0, 0, 0, 0], Column13 = [0, 0, 0, 0, 0], Column14 = Missing[missing, missing, missing, missing, missing], Column15 = [-24.81942, -24.8206, -24.82111, -24.82091, -24.82035], Column16 = [853.8073, 852.1921, 853.4257, 854.1342, 851.171], Column17 = [0.0, 0.0, 0.0, 0.0, 0.0], Column18 = [0.0, 0.0, 0.0, 0.0, 0.0], Column19 = [60.07, 38.27, 61.38, 49.23, 42.49], Column20 = [132.356, 132.356, 132.356, 132.356, 132.356])
    ),
    ("pandas_zeros.csv", (allowmissing=:auto,),
        (100000, 50),
        NamedTuple{Tuple(Symbol("$i") for i = 0:49), NTuple{50, Int64}},
        nothing
    ),

]

# f = CSV.File(joinpath(dir, "pandas_zeros.csv"), allowmissing=:none)
# @time f |> columntable;
# using Profile
# Profile.clear()
# @profile f |> columntable;
# Profile.print(C=true)
# Profile.print()

# @testset "CSV.TransposedSource" begin

# # CSV.TransposedSource
# f = CSV.File(joinpath(dir, "transposed.csv"); transpose=true)
# @test size(df) == (3, 3)
# @test Data.header(Data.schema(df)) == ["col1", "col2", "col3"]
# @test df[1][1] == 1
# @test df[1][2] == 2
# @test df[1][3] == 3

# f = CSV.File(joinpath(dir, "transposed_1row.csv"); transpose=true)
# @test size(df) == (1, 1)

# f = CSV.File(joinpath(dir, "transposed_emtpy.csv"); transpose=true)
# @test size(df) == (0, 1)

# f = CSV.File(joinpath(dir, "transposed_extra_newline.csv"); transpose=true)
# @test size(df) == (2, 2)

# f = CSV.File(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=0)
# @test size(df) == (2, 3)
# @test Data.header(Data.schema(df)) == ["Column1", "Column2", "Column3"]

# f = CSV.File(joinpath(dir, "transposed_noheader.csv"); transpose=true, header=["c1", "c2", "c3"])
# @test size(df) == (2, 3)
# @test Data.header(Data.schema(df)) == ["c1", "c2", "c3"]

for test in testfiles
    testfile(test...)
end

end # testset
