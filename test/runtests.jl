reload("CSV")
using Base.Test

dir = "/Users/jacobquinn/.julia/v0.4/CSV/test/test_files/"
dir = joinpath(dirname(@__FILE__),"test_files/")

# test 0.3 and 0.4

#test on non-existent file
@test_throws ArgumentError CSV.File("")

#test where datarow > headerrow
@test_throws ArgumentError CSV.File(dir * "test_no_header.csv";datarow=1,headerrow=2)

#test various encodings
@test CSV.validate(dir * "test_utf8_with_BOM.csv") == nothing
@test CSV.validate(dir * "test_utf8.csv") == nothing
@test CSV.validate(dir * "test_utf16_be.csv") == nothing
@test CSV.validate(dir * "test_utf16_le.csv") == nothing
@test CSV.validate(dir * "test_utf16.csv") == nothing
@test CSV.validate(dir * "test_windows.csv") == nothing

#test one column file
f = CSV.File(dir * "test_single_column.csv")
@test f.delim == ','
@test f.newline == '\n'
@test f.quotechar == '"'
@test f.escapechar == '\\'
@test f.headerpos == 1
@test f.datapos == 6
@test f.cols == 1
@test f.header == ["col1"]
@test f.types == [Int]
@test f.formats == [""]
@test CSV.validate(f) == nothing

#test empty file
f = CSV.File(dir * "test_empty_file.csv")
@test f.cols == 1
@test CSV.validate(f) == nothing

#test file with just newlines
f = CSV.File(dir * "test_empty_file_newlines.csv")
@test CSV.validate(f) == nothing

#test with various quotechars, escapechars
f = CSV.File(dir * "test_simple_quoted.csv")
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_quoted_delim_and_newline.csv")
@test CSV.validate(f) == nothing

#test unquoted newline, delimiter

#test various newlines
f = CSV.File(dir * "test_crlf_line_endings.csv")
@test f.header == ["col1","col2","col3"]
@test f.cols == 3
@test f.types == [Int,Int,Int]
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_newline_line_endings.csv")
@test CSV.validate(f) == nothing

#test headerrow, datarow, footerskips
f = CSV.File(dir * "test_no_header.csv";headerrow=0,datarow=1)
@test f.header == ["Column1","Column2","Column3"]
@test f.types == [Float64,Float64,Float64]
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_2_footer_rows.csv";headerrow=4,datarow=5)
@test_throws CSV.CSVError CSV.validate(f)

#test dates, dateformats
f = CSV.File(dir * "test_dates.csv";types=[Date],formats=["yyyy-mm-dd"])
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_excel_date_formats.csv";types=[Date],formats=["mm/dd/yy"])
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_datetimes.csv";types=[DateTime],formats=["yyyy-mm-dd HH:MM:SS.s"])
@test CSV.validate(f) == nothing
f = CSV.File(dir * "test_mixed_date_formats.csv";types=[Date],formats=["mm/dd/yyyy"])
@test_throws CSV.CSVError CSV.validate(f) == nothing

#test bad types
f = CSV.File(dir * "test_float_in_int_column.csv";types=[Int,Int,Int])
@test_throws CSV.CSVError CSV.validate(f) == nothing
f = CSV.File(dir * "test_floats.csv";types=[Float64,Float64,Float64])
@test CSV.validate(f) == nothing

#test null/missing values
f = CSV.File(dir * "test_missing_value_NULL.csv";types=[Float64,Float64,Float64])
@test_throws CSV.CSVError CSV.validate(f) == nothing
f = CSV.File(dir * "test_missing_value_NULL.csv";null="NULL")
@test CSV.validate(f) == nothing
# uses default missing value ""
f = CSV.File(dir * "test_missing_value.csv")
@test CSV.validate(f) == nothing

#other various files found around the internet
f = CSV.File(dir * "baseball.csv")
@test CSV.validate(f) == nothing
f = CSV.File(dir * "FL_insurance_sample.csv";newline='\r',coltypes=Dict(10=>Float64,12=>Float64))
@test CSV.validate(f) == nothing
f = CSV.File(dir * "SacramentocrimeJanuary2006.csv";newline='\r')
@test CSV.validate(f) == nothing
f = CSV.File(dir * "Sacramentorealestatetransactions.csv";newline='\r')
@test CSV.validate(f) == nothing
f = CSV.File(dir * "SalesJan2009.csv";newline='\r')
@test CSV.validate(f) == nothing
f = CSV.File(dir * "stocks.csv")
@test CSV.validate(f) == nothing
f = CSV.File(dir * "TechCrunchcontinentalUSA.csv";newline='\r')
@test CSV.validate(f) == nothing
f = CSV.File(dir * "Fielding.csv")
@test CSV.validate(f) == nothing

fullpath = "/Users/jacobquinn/domo/curse/data9.csv"
f = CSV.File(fullpath;rows_for_type_detect=1000)
@time CSV.validate(f)

fullpath = "/Users/jacobquinn/domo/curse/dfp2.csv"
f = CSV.File(fullpath;rows_for_type_detect=1000)
@time CSV.validate(f)
