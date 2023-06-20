nms(::Type{NamedTuple{names, types}}) where {names, types} = names
typs(::Type{NamedTuple{names, types}}) where {names, types} = Tuple(fieldtype(types, i) for i = 1:fieldcount(types))

function testfile(file, kwargs, expected_sz, expected_sch, testfunc; dir=dir)
    println("testing $file")
    if file isa IO
        seekstart(file)
    end
    f = CSV.File(file isa IO ? file : joinpath(dir, file); kwargs...)
    if file isa IO
        seekstart(file)
    end
    kwargs = Base.structdiff(kwargs, (ntasks=0,)) # Rows does not support the "ntasks" kwarg
    rows = CSV.Rows(file isa IO ? file : joinpath(dir, file); kwargs...) |> Tables.dictrowtable |> columntable
    t = f |> columntable
    actual_sch = Tables.schema(t)
    @test Tuple(typs(expected_sch)) == actual_sch.types
    @test Tuple(nms(expected_sch)) == actual_sch.names
    @test (length(t) == 0 ? 0 : length(t[1]), length(t)) == expected_sz
    if testfunc === nothing
    elseif testfunc isa Function
        testfunc(t)
    else
        @test isequal(t, testfunc)
    end
    if file isa IO
        seekstart(file)
    end
    kwargs = Base.structdiff(kwargs, (types=[],))
    types = haskey(kwargs, :select) ? Dict(nm=>T for (nm, T) in zip(f.names, getfield(f, :types))) : getfield(f, :types)
    try
        rows = CSV.Rows(file isa IO ? file : joinpath(dir, file); types=types, kwargs...) |> Tables.dictrowtable |> columntable
        actual_sch = Tables.schema(rows)
    catch e
        # check if just a column-widening issue to ignore
        !(e isa ArgumentError) && rethrow(e)
    end
    return t
end

testfiles = [
    # file, kwargs, expected_sz, expected_sch, testfunc =
    ("test_utf8_with_BOM.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = [2.0, 5.0, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_utf8.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = [2.0, 5.0, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_windows.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = [2.0, 5.0, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_single_column.csv", NamedTuple(),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Int}},
        (col1=[1,2,3],)
    ),
    ("test_empty_file.csv", NamedTuple(),
        (0, 0),
        NamedTuple{(), Tuple{}},
        NamedTuple()
    ),
    ("test_empty_file_newlines.csv", (normalizenames=true, ignoreemptyrows=false),
        (9, 1),
        NamedTuple{(:Column1,), Tuple{Missing}},
        (Column1 = Missing[missing, missing, missing, missing, missing, missing, missing, missing, missing],)
    ),
    ("test_simple_quoted.csv", (escapechar='\\', pool=false),
        (1, 2),
        NamedTuple{(:col1, :col2), Tuple{InlineString15, InlineString15}},
        (col1 = ["quoted field 1"], col2 = ["quoted field 2"])
    ),
    ("test_quoted_delim_and_newline.csv", (escapechar='\\', pool=false),
        (1, 2),
        NamedTuple{(:col1, :col2), Tuple{InlineString15, InlineString15}},
        (col1 = ["quoted ,field 1"], col2 = ["quoted\n field 2"])
    ),
    ("test_quoted_numbers.csv", (escapechar='\\',),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{InlineString7,Int,Int}},
        (col1 = ["123", "abc", "123abc"], col2 = [1, 42, 12], col3 = [1, 42, 12])
    ),
    ("test_crlf_line_endings.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_newline_line_endings.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_mac_line_endings.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1,:col2,:col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_no_header.csv", (header=0,),
        (3, 3),
        NamedTuple{(:Column1, :Column2, :Column3), Tuple{Float64, Float64, Float64}},
        (Column1 = [1.0, 4.0, 7.0], Column2 = [2.0, 5.0, 8.0], Column3 = [3.0, 6.0, 9.0])
    ),
    ("test_2_footer_rows.csv", (header=4, footerskip=2),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    # 418
    ("test_footer_missing.csv", NamedTuple(),
        (5, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}}},
        (col1 = Union{Missing, Int}[1, 4, 7, 10, missing], col2 = Union{Missing, Int}[2, 5, 8, 11, missing], col3 = Union{Missing, Int}[3, 6, 9, 12, missing])
    ),
    ("test_footer_missing.csv", (footerskip=1,),
        (4, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7, 10], col2 = [2, 5, 8, 11], col3 = [3, 6, 9, 12])
    ),
    ("test_dates.csv", NamedTuple(),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Date}},
        (col1 = Date[Date("2015-01-01"), Date("2015-01-02"), Date("2015-01-03")],)
    ),
    ("test_excel_date_formats.csv", (dateformat="mm/dd/yy",),
        (3, 1),
        NamedTuple{(:col1,), Tuple{Date}},
        (col1 = Date[Date("2015-01-01"), Date("2015-01-02"), Date("2015-01-03")],)
    ),
    ("test_datetimes.csv", (dateformat="yyyy-mm-dd HH:MM:SS.s",),
        (3, 1),
        NamedTuple{(:col1,), Tuple{DateTime}},
        (col1 = DateTime[DateTime("2015-01-01"), DateTime("2015-01-02T00:00:01"), DateTime("2015-01-03T00:12:00.001")],)
    ),
    ("test_missing_value_NULL.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,InlineString7,Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = ["2.0", "NULL", "8.0"], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_missing_value_NULL.csv", (missingstring="NULL",),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Union{Missing, Float64},Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_missing_value.csv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Union{Missing, Float64},Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, 8.0], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_header_range.csv", (header=1:3,),
        (3, 3),
        NamedTuple{(:col1_sub1_part1, :col2_sub2_part2, :col3_sub3_part3),Tuple{Int,Int,Int}},
        (col1_sub1_part1 = [1, 4, 7], col2_sub2_part2 = [2, 5, 8], col3_sub3_part3 = [3, 6, 9])
    ),
    # 343
    ("test_header_int_list.csv", (header=[1, 3, 5], ignoreemptyrows=false),
        (3, 3),
        NamedTuple{(:col1_sub1_part1, :col2_sub2_part2, :col3_sub3_part3),Tuple{Int,Int,Int}},
        (col1_sub1_part1 = [1, 4, 7], col2_sub2_part2 = [2, 5, 8], col3_sub3_part3 = [3, 6, 9])
    ),
    ("test_basic.csv", (types=Dict(2=>Float64),),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Float64,Int}},
        (col1 = [1, 4, 7], col2 = [2.0, 5.0, 8.0], col3 = [3, 6, 9])
    ),
    ("test_basic_pipe.csv", (delim='|',),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Int, Int, Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_tab_null_empty.txt", (delim='\t',),
        (2, 4),
        NamedTuple{(:A, :B, :C, :D),Tuple{Int,Union{Missing, Int},InlineString1,Int}},
        (A = [1, 2], B = Union{Missing, Int}[2000, missing], C = ["x", "y"], D = [100, 200])
    ),
    ("test_tab_null_string.txt", (delim='\t', missingstring="NULL"),
        (2, 4),
        NamedTuple{(:A, :B, :C, :D),Tuple{Int,Union{Missing, Int},InlineString1,Int}},
        (A = [1, 2], B = Union{Missing, Int}[2000, missing], C = ["x", "y"], D = [100, 200])
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
        NamedTuple{(:x, :y),Tuple{Union{Missing, Int}, Int}},
        (x = Union{Missing, Int}[1, missing], y = [2, 4])
    ),
    ("plus_as_null.csv", (missingstring="+",),
        (2, 2),
        NamedTuple{(:x, :y),Tuple{Union{Missing, Int}, Int}},
        (x = Union{Missing, Int}[1, missing], y = [2, 4])
    ),
    # #83
    ("comma_decimal.csv", (delim=';', decimal=','),
        (2, 2),
        NamedTuple{(:x, :y),Tuple{Float64,Int}},
        (x = [3.14, 1.0], y = [1, 1])
    ),
    # #86
    # ("double_quote_quotechar_and_escapechar.csv", (escapechar='"',),
    #     (24, 5),
    #     NamedTuple{(:APINo, :FileNo, :CurrentWellName, :LeaseName, :OriginalWellName),Tuple{Float64,Int,String,String,String}},
    #     x->x.OriginalWellName[24] == "NORTH DAKOTA STATE \"\"A\"\" #1"
    # ),
    # #84
    ("census.txt", (normalizenames=true,),
        (3, 9),
        NamedTuple{(:GEOID, :POP10, :HU10, :ALAND, :AWATER, :ALAND_SQMI, :AWATER_SQMI, :INTPTLAT, :INTPTLONG),Tuple{Int,Int,Int,Int,Int,Float64,Float64,Float64,Float64}},
        (GEOID = [601, 602, 603], POP10 = [18570, 41520, 54689], HU10 = [7744, 18073, 25653], ALAND = [166659789, 79288158, 81880442], AWATER = [799296, 4446273, 183425], ALAND_SQMI = [64.348, 30.613, 31.614], AWATER_SQMI = [0.309, 1.717, 0.071], INTPTLAT = [18.180555, 18.362268, 18.455183], INTPTLONG = [-66.749961, -67.17613, -67.119887])
    ),
    # #79
    ("bools.csv", NamedTuple(),
        (4, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Bool,Bool,Int}},
        (col1 = Bool[true, false, true, false], col2 = Bool[false, true, true, false], col3 = [1, 2, 3, 4])
    ),
    ("boolext.csv", NamedTuple(),
        (4, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Bool,Bool,Int}},
        (col1 = Bool[true, false, true, false], col2 = Bool[false, true, true, false], col3 = [1, 2, 3, 4])
    ),
    # #64
    # #115 (Int -> Union{Int, Missing} -> Union{String, Missing} promotion)
    ("attenu.csv", (missingstring="NA",),
        (182, 5),
        NamedTuple{(:Event, :Mag, :Station, :Dist, :Accel),Tuple{Int,Float64,Union{Missing, InlineString7},Float64,Float64}},
        x->map(y->y[1:10], x|>columntable) == (Event = [1, 2, 2, 2, 2, 2, 2, 2, 2, 2], Mag = [7.0, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4, 7.4], Station = Union{Missing, String}["117", "1083", "1095", "283", "135", "475", "113", "1008", "1028", "2001"], Dist = [12.0, 148.0, 42.0, 85.0, 107.0, 109.0, 156.0, 224.0, 293.0, 359.0], Accel = [0.359, 0.014, 0.196, 0.135, 0.062, 0.054, 0.014, 0.018, 0.01, 0.004])
    ),
    ("test_null_only_column.csv", (missingstring="NA",),
        (3, 2),
        NamedTuple{(:col1, :col2),Tuple{InlineString7,Missing}},
        (col1 = ["123", "abc", "123abc"], col2 = Missing[missing, missing, missing])
    ),
    # #107
    (IOBuffer("1,a,i\n2,b,ii\n3,c,iii"), (skipto=1,),
        (3, 3),
        NamedTuple{(:Column1, :Column2, :Column3),Tuple{Int,InlineString1,InlineString3}},
        (Column1 = [1, 2, 3], Column2 = ["a", "b", "c"], Column3 = ["i", "ii", "iii"])
    ),
    ("categorical.csv", (pool=true,),
        (15, 1),
        NamedTuple{(:cat,), Tuple{InlineString1}},
        (cat = ["a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "c", "c", "a"],)
    ),
    ("categorical.csv", (pool=0.3,),
        (15, 1),
        NamedTuple{(:cat,), Tuple{InlineString1}},
        (cat = ["a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "c", "c", "a"],)
    ),
    ("categorical.csv", (pool=false,),
        (15, 1),
        NamedTuple{(:cat,), Tuple{InlineString1}},
        (cat = ["a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "c", "c", "a"],)
    ),
    ("categorical.csv", (pool=0.0,),
        (15, 1),
        NamedTuple{(:cat,), Tuple{InlineString1}},
        (cat = ["a", "a", "a", "b", "b", "b", "b", "b", "b", "b", "c", "c", "c", "c", "a"],)
    ),
    # other various files from around the interwebs
    ("baseball.csv", (pool=true, normalizenames=true),
        (35, 15),
        NamedTuple{(:Rk, :Year, :Age, :Tm, :Lg, :Column6, :W, :L, :W_L_, :G, :Finish, :Wpost, :Lpost, :W_L_post, :Column15), Tuple{Union{Int, Missing},Union{Int, Missing},Union{Int, Missing},Union{InlineString31, Missing},Union{InlineString3, Missing},Union{InlineString15, Missing},Union{Int, Missing},Union{Int, Missing},Union{Float64, Missing},Union{Int, Missing},Union{Float64, Missing},Union{Int, Missing},Union{Int, Missing},Union{Float64, Missing},Union{String, Missing}}},
        nothing
    ),
    ("FL_insurance_sample.csv", (pool=true,),
        (36634, 18),
        NamedTuple{(:policyID, :statecode, :county, :eq_site_limit, :hu_site_limit, :fl_site_limit, :fr_site_limit, :tiv_2011, :tiv_2012, :eq_site_deductible, :hu_site_deductible, :fl_site_deductible, :fr_site_deductible, :point_latitude, :point_longitude, :line, :construction, :point_granularity),Tuple{Int,InlineString3,InlineString31,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Float64,Float64,InlineString15,InlineString31,Int}},
        nothing
    ),
    ("SacramentocrimeJanuary2006.csv", (pool=true, stringtype=String),
        (7584, 9),
        NamedTuple{(:cdatetime, :address, :district, :beat, :grid, :crimedescr, :ucr_ncic_code, :latitude, :longitude),Tuple{String, String, Int, String, Int, String, Int, Float64, Float64}},
        nothing
    ),
    ("Sacramentorealestatetransactions.csv", (pool=true, normalizenames=false, stringtype=String),
        (985, 12),
        NamedTuple{(:street, :city, :zip, :state, :beds, :baths, :sq__ft, :type, :sale_date, :price, :latitude, :longitude),Tuple{String, String, Int, String, Int, Int, Int, String, String, Int, Float64, Float64}},
        nothing
    ),
    ("SalesJan2009.csv", (pool=true,),
        (998, 12),
        NamedTuple{(:Transaction_date, :Product, :Price, :Payment_Type, :Name, :City, :State, :Country, :Account_Created, :Last_Login, :Latitude, :Longitude),Tuple{InlineString15,InlineString15,InlineString7,InlineString15,InlineString31,String,Union{Missing, InlineString31},InlineString31,InlineString15,InlineString15,Float64,Float64}},
        nothing
    ),
    ("stocks.csv", (normalizenames=true,),
        (30, 2),
        NamedTuple{(:Stock_Name, :Company_Name), Tuple{InlineString7, String}},
        nothing
    ),
    ("TechCrunchcontinentalUSA.csv", (pool=true, stringtype=String),
        (1460, 10),
        NamedTuple{(:permalink, :company, :numEmps, :category, :city, :state, :fundedDate, :raisedAmt, :raisedCurrency, :round),Tuple{String, String, Union{Missing, Int}, Union{Missing, String}, Union{Missing, String}, String, String, Int, String, String}},
        nothing
    ),
    ("Fielding.csv", (pool=false,),
        (167938, 18),
        NamedTuple{(:playerID, :yearID, :stint, :teamID, :lgID, :POS, :G, :GS, :InnOuts, :PO, :A, :E, :DP, :PB, :WP, :SB, :CS, :ZR),Tuple{InlineString15, Int, Int, InlineString3, InlineString3, InlineString3, Int, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}}},
        nothing
    ),
    ("latest (1).csv", (header=0, missingstring="\\N", pool=true),
        (1000, 25),
        NamedTuple{(:Column1, :Column2, :Column3, :Column4, :Column5, :Column6, :Column7, :Column8, :Column9, :Column10, :Column11, :Column12, :Column13, :Column14, :Column15, :Column16, :Column17, :Column18, :Column19, :Column20, :Column21, :Column22, :Column23, :Column24, :Column25),Tuple{InlineString7, String, Int, Int, InlineString31, Int, InlineString31, Int, Date, Date, Int, InlineString3, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Int}, Union{Missing, Float64}, Float64, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Int}, Float64, Union{Missing, Float64}, Union{Missing, Float64}}},
        t -> @test(t.Column17[end-2] === missing)
    ),
    # #217
    (IOBuffer("aa,bb\n1,\"1,b,c\"\n"), (pool=false,),
        (1, 2),
        NamedTuple{(:aa, :bb), Tuple{Int, InlineString7}},
        (aa = [1], bb = ["1,b,c"])
    ),
    # #198
    ("issue_198.csv", (decimal=',', delim=';', missingstring="-", skipto = 2, header = ["Date", "EONIA", "1m", "12m", "3m", "6m", "9m"], normalizenames=false),
        (6, 7),
        NamedTuple{(:Date, :EONIA, Symbol("1m"), Symbol("12m"), Symbol("3m"), Symbol("6m"), Symbol("9m")),Tuple{InlineString15, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}}},
        NamedTuple{(:Date, :EONIA, Symbol("1m"), Symbol("12m"), Symbol("3m"), Symbol("6m"), Symbol("9m"))}((String["18/04/2018", "17/04/2018", "16/04/2018", "15/04/2018", "14/04/2018", "13/04/2018"], Union{Missing, Float64}[-0.368, -0.368, -0.367, missing, missing, -0.364], Union{Missing, Float64}[-0.371, -0.371, -0.371, missing, missing, -0.371], Union{Missing, Float64}[-0.189, -0.189, -0.189, missing, missing, -0.19], Union{Missing, Float64}[-0.328, -0.328, -0.329, missing, missing, -0.329], Union{Missing, Float64}[-0.271, -0.27, -0.27, missing, missing, -0.271], Union{Missing, Float64}[-0.219, -0.219, -0.219, missing, missing, -0.219]))
    ),
    # #198 part 2
    ("issue_198_part2.csv", (missingstring="++", delim=';', decimal=','),
        (4, 4),
        NamedTuple{(:A, :B, :C, :Column4),Tuple{InlineString1, Union{Missing, Float64}, Union{Missing, Float64}, Missing}},
        (A = ["a", "b", "c", "d"], B = Union{Missing, Float64}[-0.367, missing, missing, -0.364], C = Union{Missing, Float64}[-0.371, missing, missing, -0.371], Column4 = [missing, missing, missing, missing])
    ),
    # #207
    ("issue_207.csv", NamedTuple(),
        (2, 6),
        NamedTuple{(:a, :b, :c, :d, :e, :f),Tuple{Int, Int, Int, Float64, InlineString15, Union{Missing, Float64}}},
        (a = [1863001, 1863209], b = [134, 137], c = [10000, 0], d = [1.0009, 1.0], e = ["1.0000", "2,773.9000"], f = Union{Missing, Float64}[-0.002033899, missing])
    ),
    # #120
    ("issue_120.csv", (header=0,),
        (5, 20),
        NamedTuple{(:Column1, :Column2, :Column3, :Column4, :Column5, :Column6, :Column7, :Column8, :Column9, :Column10, :Column11, :Column12, :Column13, :Column14, :Column15, :Column16, :Column17, :Column18, :Column19, :Column20),Tuple{Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Int,Int,Int,Int,Missing,Float64,Float64,Float64,Float64,Float64,Float64}},
        (Column1 = [3.52848962348857e9, 3.52848962448866e9, 3.52848962548857e9, 3.52848962648866e9, 3.52848962748875e9], Column2 = [312.73, 312.49, 312.74, 312.49, 312.62], Column3 = [0.0, 0.0, 0.0, 0.0, 0.0], Column4 = [41.87425, 41.87623, 41.87155, 41.86422, 41.87615], Column5 = [297.6302, 297.6342, 297.6327, 297.632, 297.6324], Column6 = [0.0, 0.0, 0.0, 0.0, 0.0], Column7 = [286.3423, 286.3563, 286.3723, 286.3837, 286.397], Column8 = [-99.99, -99.99, -99.99, -99.99, -99.99], Column9 = [-99.99, -99.99, -99.99, -99.99, -99.99], Column10 = [12716, 12716, 12716, 12716, 12716], Column11 = [0, 0, 0, 0, 0], Column12 = [0, 0, 0, 0, 0], Column13 = [0, 0, 0, 0, 0], Column14 = Missing[missing, missing, missing, missing, missing], Column15 = [-24.81942, -24.8206, -24.82111, -24.82091, -24.82035], Column16 = [853.8073, 852.1921, 853.4257, 854.1342, 851.171], Column17 = [0.0, 0.0, 0.0, 0.0, 0.0], Column18 = [0.0, 0.0, 0.0, 0.0, 0.0], Column19 = [60.07, 38.27, 61.38, 49.23, 42.49], Column20 = [132.356, 132.356, 132.356, 132.356, 132.356])
    ),
    ("pandas_zeros.csv", (normalizenames=true,),
        (100000, 50),
        NamedTuple{Tuple(Symbol("_$i") for i = 0:49), NTuple{50, Int}},
        nothing
    ),
    ("table_test.txt", NamedTuple(),
        (49, 770),
        NamedTuple{(:ind_50km, :nse_gsurf_cfg1, :r_gsurf_cfg1, :bias_gsurf_cfg1, :ngrids, :nse_hatmo_cfg1, :r_hatmo_cfg1, :bias_hatmo_cfg1, :nse_latmo_cfg1, :r_latmo_cfg1, :bias_latmo_cfg1, :nse_melt_cfg1, :r_melt_cfg1, :bias_melt_cfg1, :nse_rnet_cfg1, :r_rnet_cfg1, :bias_rnet_cfg1, :nse_rof_cfg1, :r_rof_cfg1, :bias_rof_cfg1, :nse_snowdepth_cfg1, :r_snowdepth_cfg1, :bias_snowdepth_cfg1, :nse_swe_cfg1, :r_swe_cfg1, :bias_swe_cfg1, :nse_gsurf_cfg2, :r_gsurf_cfg2, :bias_gsurf_cfg2, :nse_hatmo_cfg2, :r_hatmo_cfg2, :bias_hatmo_cfg2, :nse_latmo_cfg2, :r_latmo_cfg2, :bias_latmo_cfg2, :nse_melt_cfg2, :r_melt_cfg2, :bias_melt_cfg2, :nse_rnet_cfg2, :r_rnet_cfg2, :bias_rnet_cfg2, :nse_rof_cfg2, :r_rof_cfg2, :bias_rof_cfg2, :nse_snowdepth_cfg2, :r_snowdepth_cfg2, :bias_snowdepth_cfg2, :nse_swe_cfg2, :r_swe_cfg2, :bias_swe_cfg2, :nse_gsurf_cfg3, :r_gsurf_cfg3, :bias_gsurf_cfg3, :nse_hatmo_cfg3, :r_hatmo_cfg3, :bias_hatmo_cfg3, :nse_latmo_cfg3, :r_latmo_cfg3, :bias_latmo_cfg3, :nse_melt_cfg3, :r_melt_cfg3, :bias_melt_cfg3, :nse_rnet_cfg3, :r_rnet_cfg3, :bias_rnet_cfg3, :nse_rof_cfg3, :r_rof_cfg3, :bias_rof_cfg3, :nse_snowdepth_cfg3, :r_snowdepth_cfg3, :bias_snowdepth_cfg3, :nse_swe_cfg3, :r_swe_cfg3, :bias_swe_cfg3, :nse_gsurf_cfg4, :r_gsurf_cfg4, :bias_gsurf_cfg4, :nse_hatmo_cfg4, :r_hatmo_cfg4, :bias_hatmo_cfg4, :nse_latmo_cfg4, :r_latmo_cfg4, :bias_latmo_cfg4, :nse_melt_cfg4, :r_melt_cfg4, :bias_melt_cfg4, :nse_rnet_cfg4, :r_rnet_cfg4, :bias_rnet_cfg4, :nse_rof_cfg4, :r_rof_cfg4, :bias_rof_cfg4, :nse_snowdepth_cfg4, :r_snowdepth_cfg4, :bias_snowdepth_cfg4, :nse_swe_cfg4, :r_swe_cfg4, :bias_swe_cfg4, :nse_gsurf_cfg5, :r_gsurf_cfg5, :bias_gsurf_cfg5, :nse_hatmo_cfg5, :r_hatmo_cfg5, :bias_hatmo_cfg5, :nse_latmo_cfg5, :r_latmo_cfg5, :bias_latmo_cfg5, :nse_melt_cfg5, :r_melt_cfg5, :bias_melt_cfg5, :nse_rnet_cfg5, :r_rnet_cfg5, :bias_rnet_cfg5, :nse_rof_cfg5, :r_rof_cfg5, :bias_rof_cfg5, :nse_snowdepth_cfg5, :r_snowdepth_cfg5, :bias_snowdepth_cfg5, :nse_swe_cfg5, :r_swe_cfg5, :bias_swe_cfg5, :nse_gsurf_cfg6, :r_gsurf_cfg6, :bias_gsurf_cfg6, :nse_hatmo_cfg6, :r_hatmo_cfg6, :bias_hatmo_cfg6, :nse_latmo_cfg6, :r_latmo_cfg6, :bias_latmo_cfg6, :nse_melt_cfg6, :r_melt_cfg6, :bias_melt_cfg6, :nse_rnet_cfg6, :r_rnet_cfg6, :bias_rnet_cfg6, :nse_rof_cfg6, :r_rof_cfg6, :bias_rof_cfg6, :nse_snowdepth_cfg6, :r_snowdepth_cfg6, :bias_snowdepth_cfg6, :nse_swe_cfg6, :r_swe_cfg6, :bias_swe_cfg6, :nse_gsurf_cfg7, :r_gsurf_cfg7, :bias_gsurf_cfg7, :nse_hatmo_cfg7, :r_hatmo_cfg7, :bias_hatmo_cfg7, :nse_latmo_cfg7, :r_latmo_cfg7, :bias_latmo_cfg7, :nse_melt_cfg7, :r_melt_cfg7, :bias_melt_cfg7, :nse_rnet_cfg7, :r_rnet_cfg7, :bias_rnet_cfg7, :nse_rof_cfg7, :r_rof_cfg7, :bias_rof_cfg7, :nse_snowdepth_cfg7, :r_snowdepth_cfg7, :bias_snowdepth_cfg7, :nse_swe_cfg7, :r_swe_cfg7, :bias_swe_cfg7, :nse_gsurf_cfg8, :r_gsurf_cfg8, :bias_gsurf_cfg8, :nse_hatmo_cfg8, :r_hatmo_cfg8, :bias_hatmo_cfg8, :nse_latmo_cfg8, :r_latmo_cfg8, :bias_latmo_cfg8, :nse_melt_cfg8, :r_melt_cfg8, :bias_melt_cfg8, :nse_rnet_cfg8, :r_rnet_cfg8, :bias_rnet_cfg8, :nse_rof_cfg8, :r_rof_cfg8, :bias_rof_cfg8, :nse_snowdepth_cfg8, :r_snowdepth_cfg8, :bias_snowdepth_cfg8, :nse_swe_cfg8, :r_swe_cfg8, :bias_swe_cfg8, :nse_gsurf_cfg9, :r_gsurf_cfg9, :bias_gsurf_cfg9, :nse_hatmo_cfg9, :r_hatmo_cfg9, :bias_hatmo_cfg9, :nse_latmo_cfg9, :r_latmo_cfg9, :bias_latmo_cfg9, :nse_melt_cfg9, :r_melt_cfg9, :bias_melt_cfg9, :nse_rnet_cfg9, :r_rnet_cfg9, :bias_rnet_cfg9, :nse_rof_cfg9, :r_rof_cfg9, :bias_rof_cfg9, :nse_snowdepth_cfg9, :r_snowdepth_cfg9, :bias_snowdepth_cfg9, :nse_swe_cfg9, :r_swe_cfg9, :bias_swe_cfg9, :nse_gsurf_cfg10, :r_gsurf_cfg10, :bias_gsurf_cfg10, :nse_hatmo_cfg10, :r_hatmo_cfg10, :bias_hatmo_cfg10, :nse_latmo_cfg10, :r_latmo_cfg10, :bias_latmo_cfg10, :nse_melt_cfg10, :r_melt_cfg10, :bias_melt_cfg10, :nse_rnet_cfg10, :r_rnet_cfg10, :bias_rnet_cfg10, :nse_rof_cfg10, :r_rof_cfg10, :bias_rof_cfg10, :nse_snowdepth_cfg10, :r_snowdepth_cfg10, :bias_snowdepth_cfg10, :nse_swe_cfg10, :r_swe_cfg10, :bias_swe_cfg10, :nse_gsurf_cfg11, :r_gsurf_cfg11, :bias_gsurf_cfg11, :nse_hatmo_cfg11, :r_hatmo_cfg11, :bias_hatmo_cfg11, :nse_latmo_cfg11, :r_latmo_cfg11, :bias_latmo_cfg11, :nse_melt_cfg11, :r_melt_cfg11, :bias_melt_cfg11, :nse_rnet_cfg11, :r_rnet_cfg11, :bias_rnet_cfg11, :nse_rof_cfg11, :r_rof_cfg11, :bias_rof_cfg11, :nse_snowdepth_cfg11, :r_snowdepth_cfg11, :bias_snowdepth_cfg11, :nse_swe_cfg11, :r_swe_cfg11, :bias_swe_cfg11, :nse_gsurf_cfg12, :r_gsurf_cfg12, :bias_gsurf_cfg12, :nse_hatmo_cfg12, :r_hatmo_cfg12, :bias_hatmo_cfg12, :nse_latmo_cfg12, :r_latmo_cfg12, :bias_latmo_cfg12, :nse_melt_cfg12, :r_melt_cfg12, :bias_melt_cfg12, :nse_rnet_cfg12, :r_rnet_cfg12, :bias_rnet_cfg12, :nse_rof_cfg12, :r_rof_cfg12, :bias_rof_cfg12, :nse_snowdepth_cfg12, :r_snowdepth_cfg12, :bias_snowdepth_cfg12, :nse_swe_cfg12, :r_swe_cfg12, :bias_swe_cfg12, :nse_gsurf_cfg13, :r_gsurf_cfg13, :bias_gsurf_cfg13, :nse_hatmo_cfg13, :r_hatmo_cfg13, :bias_hatmo_cfg13, :nse_latmo_cfg13, :r_latmo_cfg13, :bias_latmo_cfg13, :nse_melt_cfg13, :r_melt_cfg13, :bias_melt_cfg13, :nse_rnet_cfg13, :r_rnet_cfg13, :bias_rnet_cfg13, :nse_rof_cfg13, :r_rof_cfg13, :bias_rof_cfg13, :nse_snowdepth_cfg13, :r_snowdepth_cfg13, :bias_snowdepth_cfg13, :nse_swe_cfg13, :r_swe_cfg13, :bias_swe_cfg13, :nse_gsurf_cfg14, :r_gsurf_cfg14, :bias_gsurf_cfg14, :nse_hatmo_cfg14, :r_hatmo_cfg14, :bias_hatmo_cfg14, :nse_latmo_cfg14, :r_latmo_cfg14, :bias_latmo_cfg14, :nse_melt_cfg14, :r_melt_cfg14, :bias_melt_cfg14, :nse_rnet_cfg14, :r_rnet_cfg14, :bias_rnet_cfg14, :nse_rof_cfg14, :r_rof_cfg14, :bias_rof_cfg14, :nse_snowdepth_cfg14, :r_snowdepth_cfg14, :bias_snowdepth_cfg14, :nse_swe_cfg14, :r_swe_cfg14, :bias_swe_cfg14, :nse_gsurf_cfg15, :r_gsurf_cfg15, :bias_gsurf_cfg15, :nse_hatmo_cfg15, :r_hatmo_cfg15, :bias_hatmo_cfg15, :nse_latmo_cfg15, :r_latmo_cfg15, :bias_latmo_cfg15, :nse_melt_cfg15, :r_melt_cfg15, :bias_melt_cfg15, :nse_rnet_cfg15, :r_rnet_cfg15, :bias_rnet_cfg15, :nse_rof_cfg15, :r_rof_cfg15, :bias_rof_cfg15, :nse_snowdepth_cfg15, :r_snowdepth_cfg15, :bias_snowdepth_cfg15, :nse_swe_cfg15, :r_swe_cfg15, :bias_swe_cfg15, :nse_gsurf_cfg16, :r_gsurf_cfg16, :bias_gsurf_cfg16, :nse_hatmo_cfg16, :r_hatmo_cfg16, :bias_hatmo_cfg16, :nse_latmo_cfg16, :r_latmo_cfg16, :bias_latmo_cfg16, :nse_melt_cfg16, :r_melt_cfg16, :bias_melt_cfg16, :nse_rnet_cfg16, :r_rnet_cfg16, :bias_rnet_cfg16, :nse_rof_cfg16, :r_rof_cfg16, :bias_rof_cfg16, :nse_snowdepth_cfg16, :r_snowdepth_cfg16, :bias_snowdepth_cfg16, :nse_swe_cfg16, :r_swe_cfg16, :bias_swe_cfg16, :nse_gsurf_cfg17, :r_gsurf_cfg17, :bias_gsurf_cfg17, :nse_hatmo_cfg17, :r_hatmo_cfg17, :bias_hatmo_cfg17, :nse_latmo_cfg17, :r_latmo_cfg17, :bias_latmo_cfg17, :nse_melt_cfg17, :r_melt_cfg17, :bias_melt_cfg17, :nse_rnet_cfg17, :r_rnet_cfg17, :bias_rnet_cfg17, :nse_rof_cfg17, :r_rof_cfg17, :bias_rof_cfg17, :nse_snowdepth_cfg17, :r_snowdepth_cfg17, :bias_snowdepth_cfg17, :nse_swe_cfg17, :r_swe_cfg17, :bias_swe_cfg17, :nse_gsurf_cfg18, :r_gsurf_cfg18, :bias_gsurf_cfg18, :nse_hatmo_cfg18, :r_hatmo_cfg18, :bias_hatmo_cfg18, :nse_latmo_cfg18, :r_latmo_cfg18, :bias_latmo_cfg18, :nse_melt_cfg18, :r_melt_cfg18, :bias_melt_cfg18, :nse_rnet_cfg18, :r_rnet_cfg18, :bias_rnet_cfg18, :nse_rof_cfg18, :r_rof_cfg18, :bias_rof_cfg18, :nse_snowdepth_cfg18, :r_snowdepth_cfg18, :bias_snowdepth_cfg18, :nse_swe_cfg18, :r_swe_cfg18, :bias_swe_cfg18, :nse_gsurf_cfg19, :r_gsurf_cfg19, :bias_gsurf_cfg19, :nse_hatmo_cfg19, :r_hatmo_cfg19, :bias_hatmo_cfg19, :nse_latmo_cfg19, :r_latmo_cfg19, :bias_latmo_cfg19, :nse_melt_cfg19, :r_melt_cfg19, :bias_melt_cfg19, :nse_rnet_cfg19, :r_rnet_cfg19, :bias_rnet_cfg19, :nse_rof_cfg19, :r_rof_cfg19, :bias_rof_cfg19, :nse_snowdepth_cfg19, :r_snowdepth_cfg19, :bias_snowdepth_cfg19, :nse_swe_cfg19, :r_swe_cfg19, :bias_swe_cfg19, :nse_gsurf_cfg20, :r_gsurf_cfg20, :bias_gsurf_cfg20, :nse_hatmo_cfg20, :r_hatmo_cfg20, :bias_hatmo_cfg20, :nse_latmo_cfg20, :r_latmo_cfg20, :bias_latmo_cfg20, :nse_melt_cfg20, :r_melt_cfg20, :bias_melt_cfg20, :nse_rnet_cfg20, :r_rnet_cfg20, :bias_rnet_cfg20, :nse_rof_cfg20, :r_rof_cfg20, :bias_rof_cfg20, :nse_snowdepth_cfg20, :r_snowdepth_cfg20, :bias_snowdepth_cfg20, :nse_swe_cfg20, :r_swe_cfg20, :bias_swe_cfg20, :nse_gsurf_cfg21, :r_gsurf_cfg21, :bias_gsurf_cfg21, :nse_hatmo_cfg21, :r_hatmo_cfg21, :bias_hatmo_cfg21, :nse_latmo_cfg21, :r_latmo_cfg21, :bias_latmo_cfg21, :nse_melt_cfg21, :r_melt_cfg21, :bias_melt_cfg21, :nse_rnet_cfg21, :r_rnet_cfg21, :bias_rnet_cfg21, :nse_rof_cfg21, :r_rof_cfg21, :bias_rof_cfg21, :nse_snowdepth_cfg21, :r_snowdepth_cfg21, :bias_snowdepth_cfg21, :nse_swe_cfg21, :r_swe_cfg21, :bias_swe_cfg21, :nse_gsurf_cfg22, :r_gsurf_cfg22, :bias_gsurf_cfg22, :nse_hatmo_cfg22, :r_hatmo_cfg22, :bias_hatmo_cfg22, :nse_latmo_cfg22, :r_latmo_cfg22, :bias_latmo_cfg22, :nse_melt_cfg22, :r_melt_cfg22, :bias_melt_cfg22, :nse_rnet_cfg22, :r_rnet_cfg22, :bias_rnet_cfg22, :nse_rof_cfg22, :r_rof_cfg22, :bias_rof_cfg22, :nse_snowdepth_cfg22, :r_snowdepth_cfg22, :bias_snowdepth_cfg22, :nse_swe_cfg22, :r_swe_cfg22, :bias_swe_cfg22, :nse_gsurf_cfg23, :r_gsurf_cfg23, :bias_gsurf_cfg23, :nse_hatmo_cfg23, :r_hatmo_cfg23, :bias_hatmo_cfg23, :nse_latmo_cfg23, :r_latmo_cfg23, :bias_latmo_cfg23, :nse_melt_cfg23, :r_melt_cfg23, :bias_melt_cfg23, :nse_rnet_cfg23, :r_rnet_cfg23, :bias_rnet_cfg23, :nse_rof_cfg23, :r_rof_cfg23, :bias_rof_cfg23, :nse_snowdepth_cfg23, :r_snowdepth_cfg23, :bias_snowdepth_cfg23, :nse_swe_cfg23, :r_swe_cfg23, :bias_swe_cfg23, :nse_gsurf_cfg24, :r_gsurf_cfg24, :bias_gsurf_cfg24, :nse_hatmo_cfg24, :r_hatmo_cfg24, :bias_hatmo_cfg24, :nse_latmo_cfg24, :r_latmo_cfg24, :bias_latmo_cfg24, :nse_melt_cfg24, :r_melt_cfg24, :bias_melt_cfg24, :nse_rnet_cfg24, :r_rnet_cfg24, :bias_rnet_cfg24, :nse_rof_cfg24, :r_rof_cfg24, :bias_rof_cfg24, :nse_snowdepth_cfg24, :r_snowdepth_cfg24, :bias_snowdepth_cfg24, :nse_swe_cfg24, :r_swe_cfg24, :bias_swe_cfg24, :nse_gsurf_cfg25, :r_gsurf_cfg25, :bias_gsurf_cfg25, :nse_hatmo_cfg25, :r_hatmo_cfg25, :bias_hatmo_cfg25, :nse_latmo_cfg25, :r_latmo_cfg25, :bias_latmo_cfg25, :nse_melt_cfg25, :r_melt_cfg25, :bias_melt_cfg25, :nse_rnet_cfg25, :r_rnet_cfg25, :bias_rnet_cfg25, :nse_rof_cfg25, :r_rof_cfg25, :bias_rof_cfg25, :nse_snowdepth_cfg25, :r_snowdepth_cfg25, :bias_snowdepth_cfg25, :nse_swe_cfg25, :r_swe_cfg25, :bias_swe_cfg25, :nse_gsurf_cfg26, :r_gsurf_cfg26, :bias_gsurf_cfg26, :nse_hatmo_cfg26, :r_hatmo_cfg26, :bias_hatmo_cfg26, :nse_latmo_cfg26, :r_latmo_cfg26, :bias_latmo_cfg26, :nse_melt_cfg26, :r_melt_cfg26, :bias_melt_cfg26, :nse_rnet_cfg26, :r_rnet_cfg26, :bias_rnet_cfg26, :nse_rof_cfg26, :r_rof_cfg26, :bias_rof_cfg26, :nse_snowdepth_cfg26, :r_snowdepth_cfg26, :bias_snowdepth_cfg26, :nse_swe_cfg26, :r_swe_cfg26, :bias_swe_cfg26, :nse_gsurf_cfg27, :r_gsurf_cfg27, :bias_gsurf_cfg27, :nse_hatmo_cfg27, :r_hatmo_cfg27, :bias_hatmo_cfg27, :nse_latmo_cfg27, :r_latmo_cfg27, :bias_latmo_cfg27, :nse_melt_cfg27, :r_melt_cfg27, :bias_melt_cfg27, :nse_rnet_cfg27, :r_rnet_cfg27, :bias_rnet_cfg27, :nse_rof_cfg27, :r_rof_cfg27, :bias_rof_cfg27, :nse_snowdepth_cfg27, :r_snowdepth_cfg27, :bias_snowdepth_cfg27, :nse_swe_cfg27, :r_swe_cfg27, :bias_swe_cfg27, :nse_gsurf_cfg28, :r_gsurf_cfg28, :bias_gsurf_cfg28, :nse_hatmo_cfg28, :r_hatmo_cfg28, :bias_hatmo_cfg28, :nse_latmo_cfg28, :r_latmo_cfg28, :bias_latmo_cfg28, :nse_melt_cfg28, :r_melt_cfg28, :bias_melt_cfg28, :nse_rnet_cfg28, :r_rnet_cfg28, :bias_rnet_cfg28, :nse_rof_cfg28, :r_rof_cfg28, :bias_rof_cfg28, :nse_snowdepth_cfg28, :r_snowdepth_cfg28, :bias_snowdepth_cfg28, :nse_swe_cfg28, :r_swe_cfg28, :bias_swe_cfg28, :nse_gsurf_cfg29, :r_gsurf_cfg29, :bias_gsurf_cfg29, :nse_hatmo_cfg29, :r_hatmo_cfg29, :bias_hatmo_cfg29, :nse_latmo_cfg29, :r_latmo_cfg29, :bias_latmo_cfg29, :nse_melt_cfg29, :r_melt_cfg29, :bias_melt_cfg29, :nse_rnet_cfg29, :r_rnet_cfg29, :bias_rnet_cfg29, :nse_rof_cfg29, :r_rof_cfg29, :bias_rof_cfg29, :nse_snowdepth_cfg29, :r_snowdepth_cfg29, :bias_snowdepth_cfg29, :nse_swe_cfg29, :r_swe_cfg29, :bias_swe_cfg29, :nse_gsurf_cfg30, :r_gsurf_cfg30, :bias_gsurf_cfg30, :nse_hatmo_cfg30, :r_hatmo_cfg30, :bias_hatmo_cfg30, :nse_latmo_cfg30, :r_latmo_cfg30, :bias_latmo_cfg30, :nse_melt_cfg30, :r_melt_cfg30, :bias_melt_cfg30, :nse_rnet_cfg30, :r_rnet_cfg30, :bias_rnet_cfg30, :nse_rof_cfg30, :r_rof_cfg30, :bias_rof_cfg30, :nse_snowdepth_cfg30, :r_snowdepth_cfg30, :bias_snowdepth_cfg30, :nse_swe_cfg30, :r_swe_cfg30, :bias_swe_cfg30, :nse_gsurf_cfg31, :r_gsurf_cfg31, :bias_gsurf_cfg31, :nse_hatmo_cfg31, :r_hatmo_cfg31, :bias_hatmo_cfg31, :nse_latmo_cfg31, :r_latmo_cfg31, :bias_latmo_cfg31, :nse_melt_cfg31, :r_melt_cfg31, :bias_melt_cfg31, :nse_rnet_cfg31, :r_rnet_cfg31, :bias_rnet_cfg31, :nse_rof_cfg31, :r_rof_cfg31, :bias_rof_cfg31, :nse_snowdepth_cfg31, :r_snowdepth_cfg31, :bias_snowdepth_cfg31, :nse_swe_cfg31, :r_swe_cfg31, :bias_swe_cfg31, :nse_gsurf_cfg32, :r_gsurf_cfg32, :bias_gsurf_cfg32, :nse_hatmo_cfg32, :r_hatmo_cfg32, :bias_hatmo_cfg32, :nse_latmo_cfg32, :r_latmo_cfg32, :bias_latmo_cfg32, :nse_melt_cfg32, :r_melt_cfg32, :bias_melt_cfg32, :nse_rnet_cfg32, :r_rnet_cfg32, :bias_rnet_cfg32, :nse_rof_cfg32, :r_rof_cfg32, :bias_rof_cfg32, :nse_snowdepth_cfg32, :r_snowdepth_cfg32, :bias_snowdepth_cfg32, :nse_swe_cfg32, :r_swe_cfg32, :bias_swe_cfg32),Tuple{Int,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64,Float64}},
        nothing
    ),
    ("transposed.csv", (transpose=true,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Int,Int}},
        (col1 = [1, 2, 3], col2 = [4, 5, 6], col3 = [7, 8, 9])
    ),
    ("transposed_1row.csv", (transpose=true,),
        (1, 1),
        NamedTuple{(:col1,),Tuple{Int}},
        (col1 = [1],)
    ),
    ("transposed_empty.csv", (transpose=true,),
        (0, 1),
        NamedTuple{(:col1,),Tuple{Missing}},
        (col1 = Missing[],)
    ),
    ("transposed_extra_newline.csv", (transpose=true,),
        (2, 2),
        NamedTuple{(:col1, :col2),Tuple{Int,Int}},
        (col1 = [1, 2], col2 = [3, 4])
    ),
    ("transposed_noheader.csv", (transpose=true, header=0),
        (2, 3),
        NamedTuple{(:Column1, :Column2, :Column3),Tuple{Int,Int,Int}},
        (Column1 = [1, 2], Column2 = [3, 4], Column3 = [5, 6])
    ),
    ("transposed_noheader.csv", (transpose=true, header=["c1", "c2", "c3"]),
        (2, 3),
        NamedTuple{(:c1, :c2, :c3),Tuple{Int,Int,Int}},
        (c1 = [1, 2], c2 = [3, 4], c3 = [5, 6])
    ),
    ("test_utf8.csv", (limit=2,),
        (2, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = Union{Missing, Float64}[1.0, 4.0], col2 = Union{Missing, Float64}[2.0, 5.0], col3 = Union{Missing, Float64}[3.0, 6.0])
    ),
    ("test_utf8.csv", (limit=4,),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Float64}},
        (col1 = Union{Missing, Float64}[1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, 5.0, 8.0], col3 = Union{Missing, Float64}[3.0, 6.0, 9.0])
    ),
    ("test_multiple_missing.csv", (missingstring=["NA", "NULL", "\\N"],),
        (4, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Float64, Union{Missing, Float64}, Float64}},
        (col1 = [1.0, 4.0, 7.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, missing, missing], col3 = [3.0, 6.0, 9.0, 9.0])
    ),
    ("test_openclosequotes.csv", (missingstring=["NA", "NULL"], openquotechar='{', closequotechar='}'),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{Float64, Union{Missing, Float64}, Float64}},
        (col1 = [1.0, 4.0, 7.0], col2 = Union{Missing, Float64}[2.0, missing, missing], col3 = [3.0, 6.0, 9.0])
    ),
    ("test_truestrings.csv", (truestrings=["T", "TRUE", "true"], falsestrings=["F", "FALSE", "false"]),
        (6, 2),
        NamedTuple{(:int, :bools), Tuple{Int, Bool}},
        (int = [1, 2, 3, 4, 5, 6], bools = Bool[true, true, true, false, false, false])
    ),
    ("test_truestrings.csv", (truestrings=["T", "TRUE", "true"], falsestrings=["F", "FALSE", "false"], typemap=IdDict(Bool=>String)),
        (6, 2),
        NamedTuple{(:int, :bools), Tuple{Int, String}},
        (int = [1, 2, 3, 4, 5, 6], bools = ["T", "TRUE", "true", "F", "FALSE", "false"])
    ),
    ("test_string_delimiters.csv", (delim="::",),
        (2, 4),
        NamedTuple{(:num1, :num2, :num3, :num4), Tuple{Int, Int, Int, Int}},
        (num1 = [1, 1], num2 = [1193, 661], num3 = [5, 3], num4 = [978300760, 978302109])
    ),
    (IOBuffer("1,a,i\n2,b,ii\n3,c,iii"), (skipto=1,),
        (3, 3),
        NamedTuple{(:Column1, :Column2, :Column3),Tuple{Int, InlineString1, InlineString3}},
        (Column1 = [1, 2, 3], Column2 = ["a", "b", "c"], Column3 = ["i", "ii", "iii"])
    ),
    # #249
    ("test_basic.csv", (types=Dict(:col2=>Float64),),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Float64,Int}},
        (col1 = [1, 4, 7], col2 = [2.0, 5.0, 8.0], col3 = [3, 6, 9])
    ),
    # #251
    ("test_basic.csv", (types=Dict(:col2=>Union{Float64, Missing}),),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Union{Missing,Float64},Int}},
        (col1 = [1, 4, 7], col2 = Union{Missing,Float64}[2.0, 5.0, 8.0], col3 = [3, 6, 9])
    ),
    ("test_truestrings.csv", (truestrings=["T", "TRUE", "true"], falsestrings=["F", "FALSE", "false"]),
        (6, 2),
        NamedTuple{(:int, :bools), Tuple{Int, Bool}},
        (int = [1, 2, 3, 4, 5, 6], bools = [true, true, true, false, false, false])
    ),
    ("test_repeated_delimiters.csv", (delim=" ", ignorerepeated=true),
        (3, 5),
        NamedTuple{(:a, :b, :c, :d, :e), Tuple{Int, Int, Int, Int, Int}},
        (a = [1, 1, 1], b = [2, 2, 2], c = [3, 3, 3], d = [4, 4, 4], e = [5, 5, 5])
    ),
    ("test_comments1.csv", (comment="#",),
        (2, 3),
        NamedTuple{(:a, :b, :c), Tuple{Int, Int, Int}},
        (a=[1,7], b=[2,8], c=[3,9])
    ),
    ("test_comments_multiple.csv", (comment="#",),
        (4, 3),
        NamedTuple{(:a, :b, :c), Tuple{Int, Int, Int}},
        (a = [1, 7, 10, 13], b = [2, 8, 11, 14], c = [3, 9, 12, 15])
    ),
    ("test_comments_multichar.csv", (comment="//",),
        (2, 3),
        NamedTuple{(:a, :b, :c), Tuple{Int, Int, Int}},
        (a=[1,7], b=[2,8], c=[3,9])
    ),
    # #230
    ("test_not_enough_columns.csv", NamedTuple(),
        (2, 5),
        NamedTuple{(:A, :B, :C, :D, :E),Tuple{Int,Int,Int,Missing,Missing}},
        (A = [1, 4], B = [2, 5], C = [3, 6], D = Missing[missing, missing], E = Missing[missing, missing])
    ),
    ("test_not_enough_columns2.csv", NamedTuple(),
        (2, 5),
        NamedTuple{(:A, :B, :C, :D, :E),Tuple{Int,Int,Int,Union{Int, Missing},Union{Int, Missing}}},
        (A = [1, 6], B = [2, 7], C = [3, 8], D = [4, missing], E = [5, missing])
    ),
    ("test_correct_trailing_missings.csv", NamedTuple(),
        (2, 5),
        NamedTuple{(:A, :B, :C, :D, :E),Tuple{Int,Int,Int,Missing,Missing}},
        (A = [1, 4], B = [2, 5], C = [3, 6], D = Missing[missing, missing], E = Missing[missing, missing])
    ),
    ("norwegian_data.csv", (delim=';', decimal=',', missingstring="NULL", dateformat="yyyy-mm-dd HH:MM:SS.s", pool=false),
        (1230, 83),
        NamedTuple{(:regine_area, :main_no, :point_no, :param_key, :version_no_end, :station_name, :station_status_name, :dt_start_date, :dt_end_date, :percent_missing_days, :first_year_regulation, :start_year, :end_year, :aktuell_avrenningskart, :excluded_years, :tilgang, :latitude, :longitude, :utm_east_z33, :utm_north_z33, :regulation_part_area, :regulation_part_reservoirs, :transfer_area_in, :transfer_area_out, :drainage_basin_key, :area_norway, :area_total, :comment, :drainage_dens, :dt_registration_date, :dt_regul_date, :gradient_1085, :gradient_basin, :gradient_river, :height_minimum, :height_hypso_10, :height_hypso_20, :height_hypso_30, :height_hypso_40, :height_hypso_50, :height_hypso_60, :height_hypso_70, :height_hypso_80, :height_hypso_90, :height_maximum, :length_km_basin, :length_km_river, :ocean_polar_angle, :ocean_polar_distance, :perc_agricul, :perc_bog, :perc_eff_bog, :perc_eff_lake, :perc_forest, :perc_glacier, :perc_lake, :perc_mountain, :perc_urban, :prec_intens_max, :utm_zone_gravi, :utm_east_gravi, :utm_north_gravi, :utm_zone_inlet, :utm_east_inlet, :utm_north_inlet, :br1_middelavrenning_1930_1960, :br2_Tilsigsberegning, :br3_Regional_flomfrekvensanalyse, :br5_Regional_lavvannsanalyse, :br6_Klimastudier, :br7_Klimascenarier, :br9_Flomvarsling, :br11_FRIEND, :br12_GRDC, :br23_HBV, :br24_middelavrenning_1961_1990, :br26_TotalAvlop, :br31_FlomserierPrim, :br32_FlomserierSekundar, :br33_Flomkart_aktive_ureg, :br34_Hydrologisk_referanseserier_klimastudier, :br38_Flomkart_aktive_ureg_periode, :br39_Flomkart_nedlagt_stasjon),Tuple{Int, Int, Int, Int, Int, String, InlineString15, DateTime, Union{Missing, DateTime}, Union{Missing, Float64}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, InlineString15}, Union{Missing, String}, InlineString7, Float64, Float64, Int, Int, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Int}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, String}, Missing, Union{Missing, DateTime}, Union{Missing, DateTime}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Missing, Union{Missing, Float64}, Union{Missing, Float64}, Missing, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Union{Missing, Float64}, Missing, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Missing, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}, Union{Missing, InlineString1}}},
        nothing
    ),
    # #276
    ("test_duplicate_columnnames.csv", NamedTuple(),
        (2, 8),
        NamedTuple{(:a, :b, :c, :a_1, :a_2, :a_3, :a_4, :b_1), Tuple{Int, Int, Int, Int, Int, Int, Int, Int}},
        (a = [1, 9], b = [2, 10], c = [3, 11], a_1 = [4, 12], a_2 = [5, 13], a_3 = [6, 14], a_4 = [7, 15], b_1 = [8, 16])
    ),
    # #310
    ("test_bad_datetime.csv", NamedTuple(),
        (2, 3),
        NamedTuple{(:event,:time,:typ), Tuple{InlineString15, InlineString31, InlineString7}},
        (event = ["StartMovie", "Type"], time = ["2018-09-20T18:00:30.12345+00:00", "2018-09-20T18:02:13.67188+00:00"], typ = ["Event", "Event"])
    ),
    ("test_types.csv", (pool=false,),
        (1, 8),
        NamedTuple{(:int,:float,:date,:datetime,:bool,:string,:weakrefstring,:missing), Tuple{Int,Float64,Date,DateTime,Bool,InlineString3,InlineString7,Missing}},
        (int = [1], float = [1.0], date = [Date("2018-01-01")], datetime = [DateTime("2018-01-01T00:00:00")], bool = [true], string = ["hey"], weakrefstring = ["there"], missing = [missing])
    ),
    # #326
    ("test_issue_326.wsv", (delim=" ", ignorerepeated=true),
        (2, 2),
        NamedTuple{(:A, :B),Tuple{Int,Int}},
        (A = [1, 11], B = [2, 22])
    ),
    ("test_missing_last_field.csv", NamedTuple(),
        (2, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Float64,Float64,Union{Float64, Missing}}},
        (col1 = [1.0, 4.0], col2 = [2.0, 5.0], col3 = [3.0, missing])
    ),
    # #340
    ("test_delim.tsv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Int,Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    ("test_delim.wsv", NamedTuple(),
        (3, 3),
        NamedTuple{(:col1, :col2, :col3),Tuple{Int,Int,Int}},
        (col1 = [1, 4, 7], col2 = [2, 5, 8], col3 = [3, 6, 9])
    ),
    # #351
    ("test_comment_first_row.csv", (comment="#",),
        (2, 3),
        NamedTuple{(:a, :b, :c), Tuple{Int, Int, Int}},
        (a=[1,7], b=[2,8], c=[3,9])
    ),
    ("test_comment_first_row.csv", (comment="#", header=2),
        (2, 3),
        NamedTuple{(:a, :b, :c), Tuple{Int, Int, Int}},
        (a=[1,7], b=[2,8], c=[3,9])
    ),
    # #371
    ("test_repeated_delim_371.csv", (ignorerepeated=true, delim=' ',),
        (15, 4),
        NamedTuple{(:FAMILY, :PERSON, :MARKER, :RATIO), Tuple{InlineString31, InlineString15, InlineString15, Float64}},
        (FAMILY = ["A", "A", "A", "A", "A", "A", "EPGP013951", "EPGP014065", "EPGP014065", "EPGP014065", "EP07", "83346_EPGP014244", "83346_EPGP014244", "83506", "87001"], PERSON = ["EP01223", "EP01227", "EP01228", "EP01228", "EP01227", "EP01228", "EPGP013952", "EPGP014066", "EPGP014065", "EPGP014068", "706", "T3011", "T3231", "T17255", "301"], MARKER = ["rs710865", "rs11249215", "rs11249215", "rs10903129", "rs621559", "rs1514175", "rs773564", "rs2794520", "rs296547", "rs296547", "rs10927875", "rs2251760", "rs2251760", "rs2475335", "rs2413583"], RATIO = [0.0214, 0.0107, 0.00253, 0.0116, 0.00842, 0.0202, 0.00955, 0.0193, 0.0135, 0.0239, 0.0157, 0.0154, 0.0154, 0.00784, 0.0112])
    ),
    # #154
    ("test_file_issue_154.csv", (normalizenames=true,),
        (2, 4),
        NamedTuple{(:a, :b, :_, :Column4), Tuple{Int, Int, InlineString1, Union{Missing, InlineString15}}},
        (a = [0, 12], b = [1, 5], _ = [" ", " "], Column4 = Union{Missing, String}[" comment ", missing])
    ),
    (IOBuffer("""A,B,C\n1,1,10\n6,1"""), NamedTuple(),
        (2, 3),
        NamedTuple{(:A, :B, :C), Tuple{Int, Int, Union{Int, Missing}}},
        (A = [1, 6], B = [1, 1], C = [10, missing])
    ),
    (IOBuffer("""A;B;C\n1,1,10\n2,0,16"""), (normalizenames=true,),
        (2, 3),
        NamedTuple{(:A, :B, :C), Tuple{InlineString7, Missing, Missing}},
        (A = ["1,1,10", "2,0,16"], B = [missing, missing], C = [missing, missing])
    ),
    (IOBuffer("""A;B;C\n1,1,10\n2,0,16"""), (delim=';',),
        (2, 3),
        NamedTuple{(:A, :B, :C), Tuple{InlineString7, Missing, Missing}},
        (A = ["1,1,10", "2,0,16"], B = [missing, missing], C = [missing, missing])
    ),
    (IOBuffer("""a b c d e\n1 2  3 4 5\n1 2 3  4 5\n1  2 3  4 5"""), (delim=' ',),
        (3, 7),
        NamedTuple{(:a, :b, :c, :d, :e, :Column6, :Column7), Tuple{Int, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Union{Int, Missing}, Int, Union{Int, Missing}}},
        (a = [1, 1, 1], b = [2, 2, missing], c = [missing, 3, 2], d = [3, missing, 3], e = [4, 4, missing], Column6 = [5, 5, 4], Column7 = [missing, missing, 5])
    ),
    # 323
    (IOBuffer("a0001000\na0001000"), (skipto=1, pool=true),
        (2, 1),
        NamedTuple{(:Column1,), Tuple{InlineString15}},
        (Column1 = ["a0001000", "a0001000"],)
    ),
    # 396
    ("heat_flux.dat", (delim=' ', ignorerepeated=true, skipto=2, header=[:t, :heat_flux]),
        (16001, 2),
        NamedTuple{(:t, :heat_flux), Tuple{Float64, Float64}},
        nothing
    ),
    # #422
    ("log001_vehicle_status_flags_0.txt", NamedTuple(),
        (281, 31),
        NamedTuple{(:timestamp, :condition_calibration_enabled, :condition_system_sensors_initialized, :condition_system_hotplug_timeout, :condition_system_returned_to_home, :condition_auto_mission_available, :condition_global_position_valid, :condition_home_position_valid, :condition_local_position_valid, :condition_local_velocity_valid, :condition_local_altitude_valid, :condition_power_input_valid, :condition_battery_healthy, :circuit_breaker_engaged_power_check, :circuit_breaker_engaged_airspd_check, :circuit_breaker_engaged_enginefailure_check, :circuit_breaker_engaged_gpsfailure_check, :circuit_breaker_flight_termination_disabled, :circuit_breaker_engaged_usb_check, :circuit_breaker_engaged_posfailure_check, :offboard_control_signal_found_once, :offboard_control_signal_lost, :offboard_control_set_by_command, :offboard_control_loss_timeout, :rc_signal_found_once, :rc_input_blocked, :rc_calibration_valid, :vtol_transition_failure, :usb_connected, :avoidance_system_required, :avoidance_system_valid), Tuple{Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int}},
        nothing
    ),
    # tape re-allocation
    ("fecal_samples.csv", (debug=true, normalizenames=true),
        (1079, 39),
        NamedTuple{(:SampleID, :Mother_Child, :SubjectID, :MaternalID, :TimePoint, :Fecal_EtOH, :CollectionRep, :DOC, :RAInitials_DOC, :DOF, :RAInitials_DOF, :Date_Brought_In, :RAInitials_Brought, :Date_Shipped, :RAInitials_Shipped, :Date_Aliquoted, :Number_Replicates, :RAInitials_Aliquot, :StorageBox, :DOE, :Extract_number, :AliquotRep, :DNABox, :KitUsed, :RAInitials_Extract, :DNAConc, :DOM, :Mgx_processed, :Mgx_batch, :DO16S, :_16S_processed, :_16S_batch, :_16S_plate, :Notes, :Discrepancies, :Batch_1_Mapping, :Mgx_batch_Mapping, :_16S_batch_Mapping, :Mother_Child_Dyads), Tuple{InlineString15, InlineString1, Int, InlineString7, Int, InlineString1, Int, Date, Union{Missing, InlineString7}, Union{Missing, Date}, Union{Missing, InlineString3}, Union{Missing, Date}, Union{Missing, InlineString7}, Union{Missing, Date}, Union{Missing, InlineString3}, Union{Missing, Date}, Union{Missing, InlineString7}, Union{Missing, InlineString3}, Union{Missing, InlineString7}, Union{Missing, Date}, Union{Missing, InlineString7}, Union{Missing, InlineString1}, Union{Missing, InlineString7}, Union{Missing, InlineString31}, Union{Missing, InlineString3}, Union{Missing, Float64}, Union{Missing, Date}, Union{Missing, InlineString15}, Union{Missing, InlineString7}, Union{Missing, Date}, Union{Missing, InlineString15}, Union{Missing, InlineString7}, Union{Missing, InlineString7}, Union{Missing, String}, Union{Missing, String}, Union{Missing, InlineString31}, Union{Missing, InlineString15}, Union{Missing, InlineString15}, Union{Missing, Int}}},
        nothing
    ),
    # 459, support parsing Time
    ("time.csv", (dateformat="H:M:S",),
        (2, 2),
        NamedTuple{(:time, :value), Tuple{Time, Int}},
        (time = [Time(0), Time(0, 10)], value = [1, 2])
    ),
    # https://github.com/JuliaData/DataFrames.jl/issues/1880
    ("escaped_pooled.csv", (header=[:a, :b, :c], pool=Dict(1=>true)),
        (2, 3),
        NamedTuple{(:a, :b, :c), Tuple{String, String, Float64}},
        (a = ["\\孕晚期下午和晚上血压高\"", "\\孕晚期下午和晚上血压高\""], b = ["为什么中老年人血压不高也要定期测量血压", "晚上睡眠不好会导致血压升高吗"], c = [0.744359, 0.714016])
    ),
    # https://github.com/JuliaData/CSV.jl/issues/478
    ("error_comment.txt", (comment="#",),
        (8, 4),
        NamedTuple{(:fluid, :col2, :col3, :acentric_factor), Tuple{InlineString7, Float64, Float64, Float64}},
        (fluid = ["Ar", "C2H4", "CO2", "CO", "CH4", "H2", "Kr", "Xe"], col2 = [150.86, 282.34, 304.12, 132.85, 190.56, 32.98, 209.4, 289.74], col3 = [48.98, 50.41, 73.74, 34.94, 45.99, 12.93, 55.0, 58.4], acentric_factor = [-0.002, 0.087, 0.225, 0.045, 0.011, -0.217, 0.005, 0.008])
    ),
    # https://github.com/JuliaData/CSV.jl/issues/508
    ("test_508.csv", (comment="#", header=[:eats_steak, :how_cooked]),
        (6, 2),
        NamedTuple{(:eats_steak, :how_cooked), Tuple{InlineString3, Union{InlineString15, Missing}}},
        (eats_steak=["Yes", "Yes", "Yes", "Yes", "No", "Yes"], how_cooked=["Medium rare", "Medium", "Medium", "Medium rare", missing, "Rare"])
    ),
    # https://github.com/JuliaData/CSV.jl/issues/555
    ("bug555.txt", (header=true,ignorerepeated=true,delim=' '),
        (3, 9),
        NamedTuple{(:RESULTAT, :NOM_CHAM, :INST, :NUME_ORDRE, :NOEUD, :COOR_X, :COOR_Y, :COOR_Z, :TEMP), Tuple{InlineString3, InlineString3, Float64, Int, InlineString3, Float64, Float64, Float64, Float64}},
        (RESULTAT = ["A0", "B0", "C0"], NOM_CHAM = ["A1", "B1", "C1"], INST = [0.0, 0.0, 0.0], NUME_ORDRE = [0, 0, 0], NOEUD = ["N1", "N2", "N3"], COOR_X = [0.0, 2.3, 2.5], COOR_Y = [2.27374e-15, 0.0, 0.0], COOR_Z = [0.0, 0.0, 0.0], TEMP = [0.0931399, 0.311013, 0.424537])
    ),
    # https://github.com/JuliaData/CSV.jl/issues/577
    ("csv_segfault.txt", (delim="\t", ignoreemptyrows=true),
        (468, 9),
        NamedTuple{(Symbol("Time (CEST)"), :Latitude, :Longitude, :Course, :kts, :mph, :feet, :Rate, Symbol("Reporting Facility")),Tuple{String, Union{Missing, InlineString15}, Union{Missing, InlineString15}, Union{Missing, InlineString31}, Union{Missing, InlineString3}, Union{Missing, InlineString3}, Union{Missing, InlineString7}, Union{Missing, InlineString31}, Union{Missing, InlineString31}}},
        nothing
    ),
    # https://github.com/JuliaData/CSV.jl/issues/575
    ("types_override.csv", (types=[String, Int, Int, Int, Int],),
        (3, 5),
        NamedTuple{(:col1,:col2,:col3,:col4,:col5), Tuple{String, Int, Int, Int, Int}},
        (col1 = ["A", "B", "C"], col2 = [1, 5, 9], col3 = [2, 6, 10], col4 = [3, 7, 11], col5 = [4, 8, 12])
    ),
    # https://github.com/JuliaData/CSV.jl/issues/592
    ("select.csv", (select = [:state, :dateChecked, :positive, :negative, :pending, :hospitalized, :death],),
        (1485, 7),
        NamedTuple{(:state, :positive, :negative, :pending, :hospitalized, :death, :dateChecked), Tuple{InlineString3, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}, InlineString31}},
        nothing
    ),
    # https://github.com/JuliaData/CSV.jl/issues/1010
    ("test_multithreaded_row_start_detection_singlecol.csv", (ntasks=120,),
        (3210, 1),
        NamedTuple{(:foobar,), Tuple{Float64}},
        nothing
    ),
    # https://github.com/JuliaData/CSV.jl/issues/1047
    ("test_multithreaded_row_start_detection_int.csv", (ntasks=8,),
        (151, 30),
        NamedTuple{ntuple(i -> Symbol("col", i), 30), NTuple{30,Int}},
        nothing
    ),
    # https://github.com/JuliaData/CSV.jl/pull/1073
    ("test_multithreaded_row_start_detection_float.csv", (ntasks=16,),
        (81, 55),
        NamedTuple{ntuple(Symbol, 55), NTuple{55,Float64}},
        nothing
    ),
    ("test_multithreaded_row_start_detection_float.csv", (ntasks=12,),
        (81, 55),
        NamedTuple{ntuple(Symbol, 55), NTuple{55,Float64}},
        nothing
    ),
    ("test_multithreaded_row_start_detection_float.csv", (ntasks=16, limit=82),
        (81, 55),
        NamedTuple{ntuple(Symbol, 55), NTuple{55,Float64}},
        nothing
    ),
    ("test_quoted_delim_multiple_newlines.csv", (escapechar='\\', pool=false, ntasks=2),
        (9, 3),
        NamedTuple{(:col1, :col2, :col3), Tuple{String, InlineString31, Union{Missing,InlineString15}}},
        (
            col1 = ["quoted ,field 1", "fieldA", "\n\nlarge multiline quoted field\n\n", "A", "U", "lastfieldmissing", "\nx", ",", "\n"],
            col2 = ["quoted\n field 2", "fieldB", "nextfieldmissing", "B", "\"\n\n\nV", "\n\n", "y\n", "", "quoted,field"],
            col3 = ["quoted field 3", "quoted\"field C", missing, "C", "w\n\n\n\"", missing, " z", "\\\n,", "unquotedfield"]
        )
    ),
    # https://github.com/JuliaData/CSV.jl/pull/1093
    ("groupmark_quote.csv", (groupmark=',',),
        (2, 2),
        NamedTuple{(:x, :y), Tuple{Int, Int}},
        (x = [15887, 23603], y = [24651, 14076])
    ),
    ("groupmark_space.csv", (groupmark=',',),
        (2, 2),
        NamedTuple{(:x, :y), Tuple{Int, Int}},
        (x = [15887, 23603], y = [24651, 14076])
    ),
    ("test_trailing_empty_column.csv", (ntasks=3,),
        (14,3),
        NamedTuple{(:X,:Y,:Z), Tuple{Int, Union{Missing,String7}, Union{Missing,Float64}}},
        nothing
    )
];

@static if VERSION >= v"1.3-DEV"
    # https://github.com/JuliaData/CSV.jl/issues/597
    push!(testfiles, ("ampm.csv", (dateformat="m/d/yyyy I:M:S p",),
        (2, 16),
        NamedTuple{(:ID, :INTERLOCK_NUMBER, :INTERLOCK_DESCRIPTION, :TYPE, :CREATE_DATE, :MODIFY_DATE, :USERNAME, :UNIT, :AREA, :PURPOSE, :PID, :LOCATION, :FUNC_DATE, :FUNC_BY, :TECHNICAL_DESCRIPTION, :types), Tuple{Int, Union{Missing, InlineString15}, String, Missing, DateTime, DateTime, InlineString15, InlineString15, InlineString15, InlineString31, Missing, Missing, DateTime, Missing, String, InlineString3}},
        x -> @test x.CREATE_DATE == [DateTime("2012-02-09T00:00:00"), DateTime("1998-07-22T16:37:01")]
    ))
    nothing
end

for test in testfiles
    testfile(test...)
end
# Test file with FilePaths
testfile("test_basic.csv", (types=Dict(2=>Float64),),
    (3, 3),
    NamedTuple{(:col1, :col2, :col3),Tuple{Int,Float64,Int}},
    (col1 = [1, 4, 7], col2 = [2.0, 5.0, 8.0], col3 = [3, 6, 9]);
    dir=Path(dir)
)

# https://github.com/JuliaData/CSV.jl/pull/1099
@info "The following test is expected to @error with \"Multithreaded parsing failed...\""
testfile("test_multiline_field_errorwarning.csv", (ntasks=3,),
    (20, 3),
    NamedTuple{(:col1, :col2, :col3), Tuple{String3, String, Int}},
    let col1 = [String3("A$i") for i in 1:19], col2 = [".$i" for i in 1:19], col3 = collect(1:19)
        insert!(col1, 14, String3("foo"))
        insert!(col2, 14, "a field to thwart all heuristics\n  ,,,\n, ,\n   , ,,\n, ,,\n  , ,,\n,,\n    ,,\n  ,    ,\n ,   ,\n ,,  ,  ,\n  ,  ,,,\n,   ,,\n\n, , ,    ,\n    ,     ,\n\n  ,,,\n,,,\n,,,\n ,,,\n\n,\n,\n")
        insert!(col3, 14, 0)
        (; col1, col2, col3)
    end
)