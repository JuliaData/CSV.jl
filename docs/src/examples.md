# Examples

```@contents
Pages = ["examples.md"]
```

## [Non-UTF-8 character encodings](@id stringencodings)

```julia
# assume I have csv text data encoded in ISO-8859-1 encoding
# I load the StringEncodings package, which provides encoding conversion functionality
using CSV, StringEncodings

# I open my `iso8859_encoded_file.csv` with the `enc"ISO-8859-1"` encoding
# and pass the opened IO object to `CSV.File`, which will read the entire
# input into a temporary file, then parse the data from the temp file
file = CSV.File(open("iso8859_encoded_file.csv", enc"ISO-8859-1"))

# to instead have the encoding conversion happen in memory, pass
# `buffer_in_memory=true`; this can be faster, but obviously results
# in more memory being used rather than disk via a temp file
file = CSV.File(open("iso8859_encoded_file.csv", enc"ISO-8859-1"); buffer_in_memory=true)
```

## [Concatenate multiple inputs at once](@id vectorinputs)

```julia
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
# filenames, `IO`, etc. Each input will be processed on a separate thread, with the results
# being vertically concatenated afterwards as a single `CSV.File`. Each thread's columns
# will be lazily concatenated using the `ChainedVector` type. As always, if we want to
# send the parsed columns directly to a sink function, we can use `CSV.read`, like
# `df = CSV.read(map(IOBuffer, data), DataFrame)`.
f = CSV.File(map(IOBuffer, data))
```

## [Gzipped input](@id gzipped_input)

```julia
# assume I have csv text data compressed via gzip
# no additional packages are needed; CSV.jl can decompress automatically
using CSV

# pass name of gzipped input file directly; data will be decompressed to a
# temporary file, then mmapped as a byte buffer for actual parsing
file = CSV.File("data.gz")

# to instead have the decompression happen in memory, pass
# `buffer_in_memory=true`; this can be faster, but obviously results
# in more memory being used rather than disk via a temp file
file = CSV.File("data.gz"; buffer_in_memory=true)
```

## [Delimited data in a string](@id csv_string)

```julia
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
# assume there's delimited data I want to read from the web
# one option is to use the HTTP.jl package
using CSV, HTTP

# I first make the web request to get the data via `HTTP.get` on the `url`
http_response = HTTP.get(url)

# I can then access the data of the response as a `Vector{UInt8}` and pass
# it directly to `CSV.File` for parsing
file = CSV.File(http_response.body)

# another option, with Julia 1.6+, is using the Downloads stdlib
using Downloads
http_response = Downloads.download(url)

# by default, `Downloads.download` writes the response data to a temporary file
# which can then be passed to `CSV.File` for parsing
file = CSV.File(http_response)
```

## [Reading from a zip file](@id zip_example)

```julia
using ZipFile, CSV, DataFrames

a = DataFrame(a = 1:3)
CSV.write("a.csv", a)

# zip the file; Windows users who do not have zip available on the PATH can manually zip the CSV
# or write directly into the zip archive as shown below
;zip a.zip a.csv

# alternatively, write directly into the zip archive (without creating an unzipped csv file first)
z = ZipFile.Writer("a2.zip")
f = ZipFile.addfile(z, "a.csv", method=ZipFile.Deflate)
a |> CSV.write(f)
close(z)

# read file from zip archive
z = ZipFile.Reader("a.zip") # or "a2.zip"

# identify the right file in zip
a_file_in_zip = filter(x->x.name == "a.csv", z.files)[1]

a_copy = CSV.File(a_file_in_zip) |> DataFrame

a == a_copy

close(z)
```

## [Column names on 2nd row](@id second_row_header)

```julia
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

```julia
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

```julia
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

```julia
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

```julia
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

```julia
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
# the first row; our colum names will be generated like `Column1`, `Column2`, `Column3`
file = CSV.File(IOBuffer(data); header=false, skipto=2)
```

## [Skipping trailing useless rows](@id footerskip_example)

```julia
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

# by passing `footerskip=2`, we tell parsing to start the end of the data and
# read 2 rows, ignoring their contents, then mark the ending position where
# the normal parsing process should finish
file = CSV.File(IOBuffer(data); footerskip=2)
```

## [Reading transposed data](@id transpose_example)

```julia
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

```julia
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

```julia
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

# by passing `ignoreemptyrows=false`, we ensure parsing treats an empty row
# as each column having a `missing` value set for that row
file = CSV.File(IOBuffer(data); ignoreemptyrows=true)
```

## [Including/excluding columns](@id select_example)

```julia
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
file = CSV.File(IOBuffer(data); select=(i, nm) -> i in (1, 3))
# now examples of dropping, when we'd rather specify the column(s)
# we'd like to drop/exclude from parsing
file = CSV.File(IOBuffer(data); drop=[2])
file = CSV.File(IOBuffer(data); drop=[:b])
file = CSV.File(IOBuffer(data); drop=["b"])
file = CSV.File(IOBuffer(data); drop=[false, true, false])
file = CSV.File(IOBuffer(data); drop=(i, nm) -> i == 2)
```

## [Limiting number of rows from data](@id limit_example)

```julia
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

# parsing will start reading rows, and once 3 have been read, it will
# terminate early, avoiding the parsing of the rest of the data entirely
file = CSV.File(IOBuffer(data); limit=3)
```

## [Specifying custom missing strings](@id missing_string_example)

```julia
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

## [String delimiter](@id string_delim)

```julia
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

```julia
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
#consecutive delimiters should be treated as a single
# delimiter.
file = CSV.File(IOBuffer(data); delim=' ', ignorerepeated=true)
```

## [Turning off quoted cell parsing](@id quoted_example)

```julia
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

```julia
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
"quoted field with a delimiter , inside","quoted field that contains a \\n newline and ""inner quotes\"\"\"
unquoted field,unquoted field with "inner quotes"
"""

file = CSV.File(IOBuffer(data); quotechar='"', escapechar='"')

file = CSV.File(IOBuffer(data); openquotechar='"' closequotechar='"', escapechar='"')
```

## [DateFormat](@id dateformat_example)

```julia
using CSV

# In this file, our `date` column has dates that are formatted like `yyyy/mm/dd`. We can pass just such a string to the
# `dateformat` keyword argument to tell parsing to use it when looking for `Date` or `DateTime` columns. Note that currently,
# only a single `dateformat` string can be passed to parsing, meaning multiple columns with different date formats cannot all
# be parsed as `Date`/`DateTime`.
data = """
code,date
0,2019/01/01
1,2019/01/02
"""

file = CSV.File(IOBuffer(data); dateformat="yyyy/mm/dd")
```

## [Custom decimal separator](@id decimal_example)

```julia
using CSV

# In many places in the world, floating point number decimals are separated with a comma instead of a period (`3,14` vs. `3.14`)
# . We can correctly parse these numbers by passing in the `decimal=','` keyword argument. Note that we probably need to
# explicitly pass `delim=';'` in this case, since the parser will probably think that it detected `','` as the delimiter.
data = """
col1;col2;col3
1,01;2,02;3,03
4,04;5,05;6,06
"""

file = CSV.File(IOBuffer(data); delim=';', decimal=',')
```

## [Thousands separator](@id thousands_example)

```julia
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

```julia
using CSV

# In some contexts, separators other than thousands separators group digits in a number.
# `groupmark` supports ignoring them as long as the separator character is ASCII
data = """
name;ssn;credit card number
Ayodele Beren;597-21-8366;5538-6111-0574-2633
Trinidad Shiori;387-35-5126;3017-9300-0776-5301
Ori Cherokee;731-12-4606;4682-5416-0636-3877
"""

file = CSV.File(IOBuffer(data); groupmark='-')
```

## [Custom bool strings](@id truestrings_example)

```julia
using CSV

# By default, parsing only considers the string values `true` and `false` as valid `Bool` values. To consider alternative
# values, we can pass a `Vector{String}` to the `truestrings` and `falsestrings` keyword arguments.
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

```julia
using CSV

# This file contains a 3x3 identity matrix of `Float64`. By default, parsing will detect the delimiter and type, but we can
# also explicitly pass `delim= ' '` and `types=Float64`, which tells parsing to explicitly treat each column as `Float64`,
# without having to guess the type on its own.
data = """
1.0 0.0 0.0
0.0 1.0 0.0
0.0 0.0 1.0
"""

file = CSV.File(IOBuffer(data); header=false)
file = CSV.File(IOBuffer(data); header=false, delim=' ', types=Float64)

# as a last step if you want to convert this to a Matrix, this can be done by reading in first as a DataFrame and then
# function chaining to a Matrix
using DataFrames
A = file|>DataFrame|>Matrix

# another alternative is to simply use CSV.Tables.matrix and say
B = file|>CSV.Tables.matrix # does not require DataFrames
```

## [Providing types](@id types_example)

```julia
using CSV

# In this file, our 3rd column has an invalid value on the 2nd row `invalid`. Let's imagine we'd still like to treat it as an
# `Int` column, and ignore the `invalid` value. The syntax examples provide several ways we can tell parsing to treat the 3rd
# column as `Int`, by referring to column index `3`, or column name with `Symbol` or `String`. We can also provide an entire
# `Vector` of types for each column (and which needs to match the length of columns in the file). There are two additional
# keyword arguments that control parsing behavior; in the first 4 syntax examples, we would see a warning printed like
# `"warning: invalid Int64 value on row 2, column 3"`. In the fifth example, passing `silencewarnings=true` will suppress this
# warning printing. In the last syntax example, passing `strict=true` will result in an error being thrown during parsing.
data = """
col1,col2,col3
1,2,3
4,5,invalid
6,7,8
"""

file = CSV.File(IOBuffer(data); types=Dict(3 => Int))
file = CSV.File(IOBuffer(data); types=Dict(:col3 => Int))
file = CSV.File(IOBuffer(data); types=Dict("col3" => Int))
file = CSV.File(IOBuffer(data); types=[Int, Int, Int])
file = CSV.File(IOBuffer(data); types=[Int, Int, Int], silencewarnings=true)
file = CSV.File(IOBuffer(data); types=[Int, Int, Int], strict=true)


# In this file we have lots of columns, and would like to specify the same type for all
# columns except one which should have a different type. We can do this by providing a
# function that takes the column index and column name and uses these to decide the type.
data = """
col1,col2,col3,col4,col5,col6,col7
1,2,3,4,5,6,7
0,2,3,4,5,6,7
1,2,3,4,5,6,7
"""
file = CSV.File(IOBuffer(data); types=(i, name) -> i == 1 ? Bool : Int8)
file = CSV.File(IOBuffer(data); types=(i, name) -> name == :col1 ? Bool : Int8)
# Alternatively by providing the exact name for the first column and a Regex to match the rest.
# Note that an exact column name always takes precedence over a regular expression.
file = CSV.File(IOBuffer(data); types=Dict(:col1 => Bool, r"^col\d" => Int8))
```

## [Typemap](@id typemap_example)

```julia
using CSV

# In this file, we have U.S. zipcodes in the first column that we'd rather not treat as `Int`, but parsing will detect it as
# such. In the first syntax example, we pass `typemap=IdDict(Int => String)`, which tells parsing to treat any detected `Int`
# columns as `String` instead. In the second syntax example, we alternatively set the `zipcode` column type manually.
data = """
zipcode,score
03494,9.9
12345,6.7
84044,3.4
"""

file = CSV.File(IOBuffer(data); typemap=IdDict(Int => String))
file = CSV.File(IOBuffer(data); types=Dict(:zipcode => String))
```

## [Pooled values](@id pool_example)

```julia
using CSV

# In this file, we have an `id` column and a `code` column. There can be advantages with various DataFrame/table operations
# like joining and grouping when `String` values are "pooled", meaning each unique value is mapped to a `UInt32`. By default,
# `pool=(0.2, 500)`, so string columns with low cardinality are pooled by default. Via the `pool` keyword argument, we can provide
# greater control: `pool=0.4` means that if 40% or less of a column's values are unique, then it will be pooled.
data = """
id,code
A18E9,AT
BF392,GC
93EBC,AT
54EE1,AT
8CD2E,GC
"""

file = CSV.File(IOBuffer(data))
file = CSV.File(IOBuffer(data); pool=0.4)
file = CSV.File(IOBuffer(data); pool=0.6)
```

## [Non-string pooled values](@id nonstring_pool_example)

```julia
using CSV

# in this data, our `category` column is an integer type, but represents a limited set of values that could benefit from
# pooling. Indeed, we may want to do various DataFrame grouping/joining operations on the column, which can be more
# efficient if the column type is a PooledVector. By default, passing `pool=true` will only pool string column types,
# if we pass a vector or dict however, we can specify how specific, non-string type, columns should be pooled.
data = """
category,amount
1,100.01
1,101.10
2,201.10
2,202.40
"""

file = CSV.File(IOBuffer(data); pool=Dict(1 => true))
file = CSV.File(IOBuffer(data); pool=[true, false])
```

## [Pool with absolute threshold](@id pool_absolute_threshold)

```julia
using CSV

# In this file, we have an `id` column and a `code` column. There can be advantages with various DataFrame/table operations
# like joining and grouping when `String` values are "pooled", meaning each unique value is mapped to a `UInt32`. By default,
# `pool=(0.2, 500)`, so string columns with low cardinality are pooled by default. Via the `pool` keyword argument, we can provide
# greater control: `pool=(0.5, 2)` means that if a column has 2 or fewer unique values _and_ the total number of unique values is less than 50% of all values, then it will be pooled.
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
