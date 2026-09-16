# Examples

```@contents
Pages = ["examples.md"]
Depth = 2
```

Most examples use literal data in an `IOBuffer` so the
documentation build executes them. A `String` source is a file path or URL;
wrap literal text in `IOBuffer`. Load `DataStrings` when you want to name the
default text type.

## [Start here: read, inspect, and write](@id first_example)

Install CSV with `import Pkg; Pkg.add("CSV")`. Install any other package named
in an example before using it. File paths and URLs below are placeholders;
replace them with your own sources.

```@example ex-first
using CSV

file = CSV.File(IOBuffer("id,name\n1,Ada\n2,Grace\n"))
names(file)                 # column names
collect(file.name)          # access a column
first(file).id              # access a value in a row

output = IOBuffer()
CSV.write(output, file)
String(take!(output))
```

`CSV.File` returns a table with parsed columns. Text uses
`DataStrings.DataString` by default; pass `stringtype=String` for ordinary
Julia strings. Use `CSV.read(source, DataFrame)` when you need a DataFrame
(see [Read into another table package](@ref sink_example)).

## [Non-UTF-8 character encodings](@id stringencodings)

```julia
# assume I have csv text data encoded in ISO-8859-1 encoding
# I load the StringEncodings package, which provides encoding conversion functionality
using CSV, StringEncodings

# I open my `iso8859_encoded_file.csv` with the `enc"ISO-8859-1"` encoding
# and pass the opened IO object to `CSV.File`; the whole input is read into
# memory through the converting stream, then parsed
file = open("iso8859_encoded_file.csv", enc"ISO-8859-1") do io
    CSV.File(io)
end
```

## [Concatenate multiple inputs at once](@id vectorinputs)

```@example ex-vectorinputs
using CSV

# in this case, I have a vector of delimited data inputs that each have
# matching schema (the same column names and types). I'd like to process all
# of the inputs together and vertically concatenate them into one "long" table.
data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a,b,c\n7,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

# I can just pass a `Vector` of inputs, in this case `IOBuffer(::String)`, but it
# could also be a `Vector` of any valid input source, like `AbstractVector{UInt8}`,
# filenames, `IO`, etc. Inputs are parsed on separate tasks and concatenated
# in order. The first input defines the output columns. Later missing columns
# are filled with `missing`, and later extra columns are ignored. To
# send the parsed columns directly to a sink function, we can use `CSV.read`, like
# `df = CSV.read(map(IOBuffer, data), DataFrame)`.
f = CSV.File(map(IOBuffer, data))
```

Pass `source=:origin` to add a column that records which input each row came
from (paths for file sources, `"<source 1>"` and so on otherwise), or
`source=:origin => labels` to supply your own labels.

## [Gzipped input](@id gzipped_input)

```julia
# assume I have csv text data compressed via gzip
# no additional packages are needed; CSV.jl detects gzip by its magic bytes
using CSV

# pass name of gzipped input file directly; data is decompressed into memory,
# then parsed
file = CSV.File("data.gz")
```

## [Delimited data in a string](@id csv_string)

```@example ex-string
using CSV

# I have csv data in a string I want to parse
data = """
a,b,c
1,2,3
4,5,6
"""

# Calling `IOBuffer` on a string returns an in-memory IO object
# of the string data, which can be passed to `CSV.File` for parsing
file = CSV.File(IOBuffer(data))
```

## [Data from the web/a url](@id http)

```julia
using CSV

# an http(s) URL string is downloaded to a temporary file, read into memory,
# and parsed; the temporary file is removed afterwards
url = "https://example.com/data.csv"
file = CSV.File(url)

# alternatively, fetch the bytes yourself with the HTTP.jl package and pass
# the response body (a `Vector{UInt8}`) directly to `CSV.File`
using HTTP
http_response = HTTP.get(url)
file = CSV.File(http_response.body)

# or download with the Downloads stdlib and pass the temporary file path
using Downloads
file = CSV.File(Downloads.download(url))
```

## [Reading from a zip file](@id zip_example)

```julia
using ZipArchives, Mmap, CSV, DataFrames

a = DataFrame(a = 1:3)
CSV.write("a.csv", a)

# write directly into a zip archive
ZipWriter("a.zip") do z
    zip_newfile(z, "a.csv"; compress=true)
    CSV.write(z, a)
end

# read file from zip archive
z = ZipReader(open(mmap, "a.zip"))

# identify the right file in zip
a_copy = CSV.read(zip_readentry(z, "a.csv"), DataFrame)

a == a_copy
```

## [Column names on 2nd row](@id second_row_header)

```@example ex-header2
using CSV

data = """
descriptive row with information about the file that we'd like to ignore
a,b,c
1,2,3
4,5,6
"""

# by passing header=2, parsing will ignore the 1st row entirely
# then parse the column names on row 2, then by default, it assumes
# the data starts on the row after the column names (row 3 in this case)
# which is correct for this case
file = CSV.File(IOBuffer(data); header=2)
```

## [No column names in data](@id no_header)

```@example ex-noheader
using CSV

# in this case, our data doesn't have any column names
data = """
1,2,3
4,5,6
"""

# by passing `header=false`, parsing won't worry about looking for column names
# anywhere, but instead just start parsing the data and generate column names
# as needed, like `Column1`, `Column2`, and `Column3` in this case
file = CSV.File(IOBuffer(data); header=false)
```

## [Manually provide column names](@id manual_header)

```@example ex-manualheader
using CSV

# in this case, our data doesn't have any column names
data = """
1,2,3
4,5,6
"""

# instead of passing `header=false` and getting auto-generated column names,
# we can instead pass the column names ourselves
file = CSV.File(IOBuffer(data); header=["a", "b", "c"])

# we can also pass the column names as Symbols; a copy of the manually provided
# column names will always be made and then converted to `Vector{Symbol}`
file = CSV.File(IOBuffer(data); header=[:a, :b, :c])
```

## [Multi-row column names](@id multi_row_header)

```@example ex-multirowheader
using CSV

# in this case, our column names are `col_a`, `col_b`, and `col_c`,
# but split over the first and second rows
data = """
col,col,col
a,b,c
1,2,3
4,5,6
"""

# by passing a collection of integers, parsing will parse each row in the collection
# and concatenate the values for each column, separating rows with `_` character
file = CSV.File(IOBuffer(data); header=[1, 2])
```

## [Normalizing column names](@id normalize_header)

```@example ex-normalize
using CSV

# in this case, our data are single letters, with column names of "1", "2", and "3"
# A single digit isn't a valid identifier in Julia, meaning we couldn't do something
# like `1 = 2 + 2`, where `1` would be a variable name
data = """
1,2,3
a,b,c
d,e,f
h,i,j
"""

# in order to have valid identifiers for column names, we can pass
# `normalizenames=true`, which result in our column names becoming "_1", "_2", and "_3"
# note this isn't required, but can be convenient in certain cases
file = CSV.File(IOBuffer(data); normalizenames=true)

# we can access the first column like
file._1
```

```@example ex-normalize
# another example where we may want to normalize is column names with spaces in them
data = """
column one,column two, column three
1,2,3
4,5,6
"""

# normalizing will result in column names like "column_one", "column_two" and "column_three"
file = CSV.File(IOBuffer(data); normalizenames=true)
```

## [Skip to specific row where data starts](@id skipto_example)

```@example ex-skipto
using CSV

# our data has a first row that we'd like to ignore; our data also doesn't have
# column names, so we'd like them to be auto-generated
data = """
descriptive row that gives information about the data that we'd like to ignore
1,2,3
4,5,6
"""

# with no column names in the data, we first pass `header=false`; by itself,
# this would result in parsing starting on row 1 to parse the actual data;
# but we'd like to ignore the first row, so we pass `skipto=2` to skip over
# the first row; our column names will be generated like `Column1`, `Column2`, `Column3`
file = CSV.File(IOBuffer(data); header=false, skipto=2)
```

## [Skipping trailing useless rows](@id footerskip_example)

```@example ex-footerskip
using CSV

# our data has column names of "a", "b", and "c"
# but at the end of the data, we have 2 rows we'd like to ignore while parsing
# since they're not properly delimited
data = """
a,b,c
1,2,3
4,5,6
7,8,9
totals: 12, 15, 18
grand total: 45
"""

# `footerskip=2` excludes the last two rows from value parsing.
# CSV still scans the input to find the row boundaries.
file = CSV.File(IOBuffer(data); footerskip=2)
```

## [Reading transposed data](@id transpose_example)

```@example ex-transpose
using CSV

# our data is transposed, meaning our column names are in the first column,
# with the data for column "a" all on the first row, data for column "b"
# all on the second row, and so on.
data = """
a,1,4,7
b,2,5,8
c,3,6,9
"""

# by passing `transpose=true`, parsing will look for column names in the first
# column of data, then parse each row as a separate column
file = CSV.File(IOBuffer(data); transpose=true)
```

## [Ignoring commented rows](@id comment_example)

```@example ex-comment
using CSV

# here, we have several non-data rows that all begin with the "#" string
data = """
# row describing column names
a,b,c
# row describing first row of data
1,2,3
# row describing second row of data
4,5,6
"""

# we want to ignore these "commented" rows
file = CSV.File(IOBuffer(data); comment="#")
```

## [Ignoring empty rows](@id ignoreemptyrows_example)

```@example ex-emptyrows
using CSV

# here, we have a "gap" row in between the first and second row of data
# by default, these "empty" rows are ignored, but in our case, this is
# how a row of data is input when all columns have missing/null values
# so we don't want those rows to be ignored so we can know how many
# missing cases there are in our data
data = """
a,b,c
1,2,3

4,5,6
"""

# `ignoreemptyrows=false` keeps the empty row and fills its columns with missing;
# a kept empty row is not a parse problem
file = CSV.File(IOBuffer(data); ignoreemptyrows=false)
@assert isempty(CSV.problems(file)) # hide
file
```

## [Including/excluding columns](@id select_example)

```@example ex-select
using CSV

# simple dataset, but we know column "b" isn't needed
# so we'd like to save time by having parsing ignore it completely
data = """
a,b,c
1,2,3
4,5,6
7,8,9
"""

# there are quite a few ways to provide the select/drop arguments
# so we provide an example of each, first for selecting the columns
# "a" and "c" that we want to include or keep from parsing
file = CSV.File(IOBuffer(data); select=[1, 3])
file = CSV.File(IOBuffer(data); select=[:a, :c])
file = CSV.File(IOBuffer(data); select=["a", "c"])
file = CSV.File(IOBuffer(data); select=[true, false, true])
file = CSV.File(IOBuffer(data); select=r"[ac]")
# now examples of dropping, when we'd rather specify the column(s)
# we'd like to drop/exclude from parsing
file = CSV.File(IOBuffer(data); drop=[2])
file = CSV.File(IOBuffer(data); drop=[:b])
file = CSV.File(IOBuffer(data); drop=["b"])
file = CSV.File(IOBuffer(data); drop=[false, true, false])
file = CSV.File(IOBuffer(data); drop=:b)
```

`select` and `drop` take a list, one name or index,
or a `Regex`; the selected columns keep their file order.

## [Limiting number of rows from data](@id limit_example)

```@example ex-limit
using CSV

# here, we have quite a few rows of data (relative to other examples, lol)
# but we know we only need the first 3 for the analysis we need to do
# so instead of spending the time parsing the entire file, we'd like
# to just read the first 3 rows and ignore the rest
data = """
a,b,c
1,2,3
4,5,6
7,8,9
10,11,12
13,14,15
"""

# Only the first three data rows have their values parsed and returned.
# CSV still reads or maps the source and builds its structural index.
# The limit is exact at every thread count.
file = CSV.File(IOBuffer(data); limit=3)
```

## [Specifying custom missing strings](@id missing_string_example)

```@example ex-missingstring
using CSV

# in this data, our first column has "missing" values coded with -999
# but our score column has "NA" instead
# we'd like either of those values to show up as `missing` after we parse the data
data = """
code,age,score
0,21,3.42
1,42,6.55
-999,81,NA
-999,83,NA
"""

# by passing missingstring=["-999", "NA"], parsing will check each cell if it matches
# either string in order to set the value of the cell to `missing`
file = CSV.File(IOBuffer(data); missingstring=["-999", "NA"])
```

An unquoted empty field is always `missing`; `missingstring` adds spellings.

## [String delimiter](@id string_delim)

```@example ex-stringdelim
using CSV

# our data has two columns, separated by double colon
# characters ("::")
data = """
col1::col2
1::2
3::4
"""

# we can pass a single character or string for delim
file = CSV.File(IOBuffer(data); delim="::")
```

## [Fixed width files](@id ignorerepeated_example)

```@example ex-fixedwidth
using CSV

# This is an example of "fixed width" data, where each
# column is the same number of characters away from each
# other on each row. Fields are "padded" with extra
# delimiters (in this case `' '`) so that each column is
# the same number of characters each time
data = """
col1    col2 col3
123431  2    3421
2355    346  7543
"""
# In addition to our `delim`, we can pass
# `ignorerepeated=true`, which tells parsing that
# consecutive delimiters should be treated as a single
# delimiter.
file = CSV.File(IOBuffer(data); delim=' ', ignorerepeated=true)
```

## [Turning off quoted cell parsing](@id quoted_example)

```@example ex-quoted
using CSV

# by default, cells like the 1st column, 2nd row
# will be treated as "quoted" cells, where they start
# and end with the quote character '"'. The quotes will
# be removed from the final parsed value
# we may, however, want the "raw" value and _not_ ignore
# the quote characters in the final value
data = """
a,b,c
"hey",2,3
there,4,5
sailor,6,7
"""

# we can "turn off" the detection of quoted cells
# by passing `quoted=false`
file = CSV.File(IOBuffer(data); quoted=false)
```

## [Quoted & escaped fields](@id quotechar_example)

```@example ex-quotechar
using CSV

# In this data, we have a few "quoted" fields, which means the field's value starts and ends with `quotechar` (or
# `openquotechar` and `closequotechar`, respectively). Quoted fields allow the field to contain characters that would otherwise
# be significant to parsing, such as delimiters or newline characters. When quoted, parsing will ignore these otherwise
# significant characters until the closing quote character is found. For quoted fields that need to also include the quote
# character itself, an escape character is provided to tell parsing to ignore the next character when looking for a close quote
# character. In the syntax examples, the keyword arguments are passed explicitly, but these also happen to be the default
# values, so just doing `CSV.File(IOBuffer(data))` would result in successful parsing.
data = """
col1,col2
"quoted field with a delimiter , inside","quoted field that contains a
newline and ""inner quotes\"\"\"
unquoted field,unquoted field with "inner quotes"
"""

file = CSV.File(IOBuffer(data); quotechar='"', escapechar='"')

file = CSV.File(IOBuffer(data); openquotechar='"', closequotechar='"', escapechar='"')
```

A quote inside an unquoted field (`with "inner quotes"`) is content: the
structural scan notices it and rebuilds its index under the field-start rule.

## [DateFormat](@id dateformat_example)

```@example ex-dateformat
using CSV

# In this file, our `date` column has dates that are formatted like `yyyy/mm/dd`. We can pass just such a string to the
# `dateformat` keyword argument to tell parsing to use it when looking for date or date-time columns.
data = """
code,date
0,2019/01/01
1,2019/01/02
"""

file = CSV.File(IOBuffer(data); dateformat="yyyy/mm/dd")
```

```@example ex-dateformat
# columns with different formats take a dictionary keyed by column name or index
data = """
code,date,stamp
0,2019/01/01,01-02-2019 10:30
1,2019/01/02,02-02-2019 11:45
"""

file = CSV.File(IOBuffer(data); dateformat=Dict(:date => "yyyy/mm/dd", :stamp => "dd-mm-yyyy HH:MM"))
```

Without a `dateformat`, ISO dates, ISO date-times (with `T` or a space), and
times are detected. Date-times prefer `Durations.Timestamp{Dates.Nanosecond}`.
Wider dates use `Timestamp{Microsecond}` if every value fits exactly; otherwise
the column stays text. Load `Dates` and request `types=Dict(:stamp => DateTime)`
when a consumer needs `DateTime`. Fractions finer than whole milliseconds then
produce parse problems instead of being rounded.

## [Custom decimal separator](@id decimal_example)

```@example ex-decimal
using CSV

# In many places in the world, floating point number decimals are separated with a comma instead of a period (`3,14` vs. `3.14`).
# We can correctly parse these numbers by passing in the `decimal=','` keyword argument. If the file has no header or
# delimiter detection is otherwise ambiguous, pass `delim=';'` explicitly so commas are treated only as decimal markers.
data = """
col1;col2;col3
1,01;2,02;3,03
4,04;5,05;6,06
"""

file = CSV.File(IOBuffer(data); delim=';', decimal=',')
```

## [Thousands separator](@id thousands_example)

```@example ex-thousands
using CSV

# In many places in the world, digits to the left of the decimal place are broken into
# groups by a thousands separator. We can ignore those separators by passing the `groupmark`
# keyword argument.
data = """
x y
1 2
2 1,729
3 87,539,319
"""

file = CSV.File(IOBuffer(data); groupmark=',')
```

## [Custom groupmarks](@id groupmark_example)

```@example ex-groupmark
using CSV

# In some contexts, separators other than thousands separators group digits in a number.
# `groupmark` supports ignoring them as long as the separator character is ASCII and is not
# itself numeric syntax (a digit, sign, decimal point, or exponent letter).
# It must appear between digits in the integer part of the number.
data = """
name;part number
Ayodele Beren;5538_6111_0574
Trinidad Shiori;3017_9300_0776
Ori Cherokee;4682_5416_0636
"""

# Both spaces and semicolons divide each row consistently. Set the delimiter.
file = CSV.File(IOBuffer(data); delim=';', groupmark='_')
@assert file["part number"] == [553861110574, 301793000776, 468254160636] # hide
file
```

## [Custom bool strings](@id truestrings_example)

```@example ex-bools
using CSV

# By default, parsing considers `true`, `True`, `TRUE`, `false`, `False`, and `FALSE` as valid `Bool` values.
# To consider alternative values, we can pass a `Vector{String}` to the `truestrings` and `falsestrings` keyword arguments;
# a user list replaces the defaults.
data = """
id,paid,attended
0,T,TRUE
1,F,TRUE
2,T,FALSE
3,F,FALSE
"""

file = CSV.File(IOBuffer(data); truestrings=["T", "TRUE"], falsestrings=["F", "FALSE"])
```

## [Matrix-like Data](@id matrix_example)

```@example ex-matrix
using CSV, Tables

# This file contains a 3x3 identity matrix of `Float64`. By default, parsing will detect the delimiter and type, but we can
# also explicitly pass `delim=' '` and `types=Float64`, which tells parsing to explicitly treat each column as `Float64`,
# without having to guess the type on its own.
data = """
1.0 0.0 0.0
0.0 1.0 0.0
0.0 0.0 1.0
"""

file = CSV.File(IOBuffer(data); header=false)
file = CSV.File(IOBuffer(data); header=false, delim=' ', types=Float64)

# to convert the table to a `Matrix`, use `Tables.matrix`
B = Tables.matrix(file)
```

## [Providing types](@id types_example)

```@example ex-types
using CSV

# In this file, our 3rd column has an invalid value on the 2nd row `invalid`. Let's imagine we'd still like to treat it as an
# `Int` column, and ignore the `invalid` value. The syntax examples provide several ways we can tell parsing to treat the 3rd
# column as `Int`, by referring to column index `3`, or column name with `Symbol` or `String`. We can also provide an entire
# `Vector` of types for each column (and which needs to match the length of columns in the file). An invalid value becomes
# `missing` and is recorded as a problem; by default one summary warning is printed per read, `on_error=:collect` keeps the
# problems silently for `CSV.problems(file)`, and `on_error=:error` throws a `CSV.ParseError` at the first problem.
data = """
col1,col2,col3
1,2,3
4,5,invalid
6,7,8
"""

file = CSV.File(IOBuffer(data); types=Dict(3 => Int), on_error=:collect)
file = CSV.File(IOBuffer(data); types=Dict(:col3 => Int), on_error=:collect)
file = CSV.File(IOBuffer(data); types=Dict("col3" => Int), on_error=:collect)
file = CSV.File(IOBuffer(data); types=[Int, Int, Int], on_error=:collect)
CSV.problems(file)
```

```julia
# stop at the first invalid value instead
file = CSV.File(IOBuffer(data); types=[Int, Int, Int], on_error=:error)
```

```@example ex-types
# In this file we have lots of columns, and would like to specify the same type for all
# columns except one which should have a different type. We can do this by providing the
# exact name for the first column and a Regex to match the rest. Note that an exact
# column name always takes precedence over a regular expression.
data = """
col1,col2,col3,col4,col5,col6,col7
1,2,3,4,5,6,7
0,2,3,4,5,6,7
1,2,3,4,5,6,7
"""
# Numeric Boolean spellings need explicit lists, even with types=Bool.
file = CSV.File(IOBuffer(data); types=Dict(:col1 => Bool, r"^col\d" => Int8),
                truestrings=["1"], falsestrings=["0"])
@assert file.col1 == [true, false, true] # hide
@assert isempty(CSV.problems(file)) # hide
file
```

## [Typemap](@id typemap_example)

```@example ex-typemap
using CSV

# In this file, we have U.S. zipcodes in the first column that we'd rather not treat as `Int`, but parsing will detect it as
# such. In the first syntax example, we pass `typemap=Dict(Int => String)`, which tells parsing to treat any detected `Int`
# columns as text instead, using `stringtype` (DataString by default).
# In the second example, an explicit String type also requests ordinary Julia strings.
data = """
zipcode,score
03494,9.9
12345,6.7
84044,3.4
"""

file = CSV.File(IOBuffer(data); typemap=Dict(Int => String))
file = CSV.File(IOBuffer(data); types=Dict(:zipcode => String))
```

## [Pooled values](@id pool_example)

```@example ex-pool
using CSV

# In this file, we have an `id` column and a `code` column. There can be advantages with various DataFrame/table operations
# like joining and grouping when `String` values are "pooled", meaning each unique value is mapped to a `UInt32`. Pooling is
# off by default. Via the `pool` keyword argument, we can turn it on: `pool=true` pools every string column,
# `pool=0.4` means that if 40% or less of a column's values are unique, then it will be pooled, and `pool=(0.2, 500)`
# is the ratio-and-cap policy.
data = """
id,code
A18E9,AT
BF392,GC
93EBC,AT
54EE1,AT
8CD2E,GC
"""

file = CSV.File(IOBuffer(data); pool=true)
file = CSV.File(IOBuffer(data); pool=0.4)
file = CSV.File(IOBuffer(data); pool=0.6)
```

## [Non-string pooled values](@id nonstring_pool_example)

```@example ex-nonstringpool
using CSV, PooledArrays

# CSV pools text columns only. To pool a numeric category, convert that column
# after reading with PooledArrays.
data = """
category,amount
1,100.01
1,101.10
2,201.10
2,202.40
"""

file = CSV.File(IOBuffer(data))
category = PooledArray(file.category)
@assert category == [1, 1, 2, 2] # hide
category
```

## [Pool with absolute threshold](@id pool_absolute_threshold)

```@example ex-poolthreshold
using CSV

# In this file, we have an `id` column and a `code` column. Via the `pool` keyword argument, we can provide
# greater control: `pool=(0.5, 2)` means that if a column has 2 or fewer unique values _and_ the total number of unique
# values is at most 50% of all values, then it will be pooled.
data = """
id,code
A18E9,AT
BF392,GC
93EBC,AT
54EE1,AT
8CD2E,GC
"""

file = CSV.File(IOBuffer(data); pool=(0.5, 2))
```

## [Exact decimal columns](@id decimal_types_example)

```@example ex-decimaltypes
using CSV, DataDecimals

# Loading DataDecimals lets an explicitly requested decimal type parse exactly from the field bytes.
# CSV does not infer decimal types; fractional numbers infer as `Float64`.
data = """
item,amount
coffee,3.50
bagel,2.25
"""

file = CSV.File(IOBuffer(data); types=Dict(:amount => Decimal64{2}))
collect(file.amount)
```

With `Decimal64{2}`, a value such as `1.235` needs rounding and becomes a
parse problem. Extra trailing zeros, such as `1.2300`, are exact and accepted.
Use `on_error=:collect` to inspect problems or `on_error=:error` to throw.

## [Inspect bad values](@id problems_example)

```@example ex-problems
using CSV

text = "id,amount\n1,10\n2,not-a-number\n"
file = CSV.File(IOBuffer(text); types=Dict(:amount => Int), on_error=:collect)

[(problem.row, problem.col, problem.kind, problem.message)
 for problem in CSV.problems(file)]
```

## [Rows that do not match the header](@id ragged_example)

A row with extra fields keeps the header schema: the extra fields are
reported, not added as new columns. A row with fewer fields is padded with
`missing`. An unclosed quote is reported and the affected text is kept.

```@example ex-ragged
using CSV

text = "id,name\n1,Ada\n2,Grace,extra\n3\n4,\"unclosed\n"
file = CSV.File(IOBuffer(text); on_error=:collect)

(names(file), length(file), [(p.row, p.kind) for p in CSV.problems(file)])
```

## [Keep empty text distinct from missing](@id empty_text_example)

```@example ex-empty
using CSV

table = (value=Union{Missing, String}[missing, "", "text"],)
output = IOBuffer()
CSV.write(output, table)
bytes = String(take!(output))
# A missing value in a one-column file occupies an empty row. Keep that row.
roundtrip = CSV.File(IOBuffer(bytes); stringtype=String, ignoreemptyrows=false)
@assert isequal(roundtrip.value, table.value) # hide

(bytes, collect(roundtrip.value))
```

## [Write to an in-memory buffer](@id write_buffer_example)

```@example ex-write
using CSV

table = (id=[1, 2], note=["plain", "comma, inside"])
output = IOBuffer()
CSV.write(output, table; newline="\r\n")
String(take!(output))
```

An `IO` sink is written at its current position, like `Base.write`; a path
sink ending in `.gz` is compressed automatically.

## [Read into another table package](@id sink_example)

`CSV.read` calls any Tables.jl sink. For example, with DataFrames.jl installed:

```julia
using CSV, DataFrames

df = CSV.read("input.csv", DataFrame)
CSV.write("output.csv", df)
```

## [Process rows or batches](@id rows_chunks_example)

```@example ex-rows
using CSV

rows = CSV.Rows(IOBuffer("id,value\n1,10\n2,20\n"); types=[Int, Int])
total = sum(row[:value] for row in rows)
```

Use `CSV.Chunks` when a downstream operation accepts table partitions; every
batch is a `CSV.File` with the same column types:

```@example ex-chunks
using CSV

chunks = CSV.Chunks(IOBuffer("id,value\n1,10\n2,20\n3,30\n"); ntasks=2)
length(collect(chunks))
```

`CSV.Rows` defaults to text unless you supply `types`. Both readers retain the
source bytes; `CSV.Rows` also retains the whole structural index, while
`CSV.Chunks` indexes one batch at a time. `CSV.Chunks` checks values across the
input to choose one schema before iteration, so its constructor reads every row;
its batch count is only known once you iterate.

## [Index first and parse later](@id lazy_example)

```@example ex-lazy
using CSV

lazyfile = CSV.lazy(IOBuffer("id,value\n1,10.5\n2,20.0\n"))
first_id = String(lazyfile.id[1])
eager = CSV.File(lazyfile; types=Dict(:value => Float64))

(first_id, collect(eager.value))
```
