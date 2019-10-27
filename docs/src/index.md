# CSV.jl Documentation

CSV.jl is built to be a fast and flexible pure-Julia library for handling delimited text files.


```@contents
Depth = 3
```

## Key Functions
```@docs
CSV.File
CSV.read
CSV.Rows
CSV.write
```

## Examples
### Basic
#### File
```
col1,col2,col3,col4,col5,col6,col7,col8
,1,1.0,1,one,2019-01-01,2019-01-01T00:00:00,true
,2,2.0,2,two,2019-01-02,2019-01-02T00:00:00,false
,3,3.0,3.14,three,2019-01-03,2019-01-03T00:00:00,true
```
#### Syntax
```julia
CSV.File(file)
```
By default, `CSV.File` will automatically detect this file's delimiter `','`, and the type of each column. By default, it treats "empty fields" as `missing` (the entire first column in this example). It also automatically handles promoting types, like the 4th column, where the first two values are `Int`, but the 3rd row has a `Float64` value (`3.14`). The resulting column's type will be `Float64`. Parsing can detect `Int64`, `Float64`, `Date`, `DateTime`, and `Bool` types, with `String` as the fallback type for any column.

### Auto-Delimiter Detection
#### File
```
col1|col2
1|2
3|4
```
#### Syntax
```julia
CSV.File(file)
```
By default, `CSV.File` will try to detect a file's delimiter from the first 10 lines of the file; candidate delimiters include `','`, `'\t'`, `' '`, `'|'`, `';'`, and `':'`. If it can't auto-detect the delimiter, it will assume `','`. If your file includes a different character or string delimiter, just pass `delim=X` where `X` is the character or string. For this file you could also do `CSV.File(file; delim='|')`.

### String Delimiter
#### File
```
col1::col2
1::2
3::4
```
#### Syntax
```julia
CSV.File(file; delim="::")
```
In this example, our file has fields separated by the string `"::"`; we can pass this as the `delim` keyword argument.

### No Header
#### File
```
1,2,3
4,5,6
7,8,9
```
#### Syntax
```julia
CSV.File(file; header=false)
CSV.File(file; header=["col1", "col2", "col3"])
CSV.File(file; header=[:col1, :col2, :col3])
```
In this file, there is no header row that contains column names. In the first option, we pass `header=false`, and column names will be generated like `[:Column1, :Column2, :Column3]`. In the two latter examples, we pass our own explicit column names, either as strings or symbols.

### Normalize Column Names
#### File
```
column one,column two, column three
1,2,3
4,5,6
```
#### Syntax
```julia
CSV.File(file; normalizenames=true)
```
In this file, our column names have spaces in them. It can be convenient with a `CSV.File` or `DataFrame` to access entire columns via property access, e.g. if `f = CSV.File(file)` with column names like `[:col1, :col2]`, I can access the entire first column of the file like `f.col1`, or for the second, `f.col2`. The call of `f.col1` actually gets rewritten to the function call `getproperty(f, :col1)`, which is the function implemented in CSV.jl that returns the `col1` column from the file. When a column name is not a single atom Julia identifier, this is inconvient, because `f.column one` is not valid, so I would have to manually call `getproperty(f, Symbol("column one")`. `normalizenames=true` comes to our rescue; it will replace invalid identifier characters with underscores to ensure each column is a valid Julia identifier, so for this file, we would end up with column names like `[:column_one, :column_two]`. You can call `propertynames(f)` on any `CSV.File` to see the parsed column names.

### Datarow
#### File
```
col1,col2,col3
metadata1,metadata2,metadata3
extra1,extra2,extra3
1,2,3
4,5,6
7,8,9
```
#### Syntax
```julia
CSV.File(file; datarow=4)
CSV.File(file; skipto=4)
```
This file has extra rows in between our header row `col1,col2,col3` and the start of our data `1,2,3` on row 4. We can use the `datarow` or `skipto` keyword arguments to provide a row number where the "data" of our file begins.

### Reading Chunks
#### File
```
col1,col2,col3
1,2,3
4,5,6
7,8,9
10,11,12
13,14,15
16,17,18
19,20,21
```
#### Syntax
```julia
CSV.File(file; limit=3)
CSV.File(file; skipto=4, limit=1)
CSV.File(file; skipto=7, footerskip=1)
```
In this example, we desire to only read a subset of rows from the file. Using the `limit`, `skipto`, and `footerskip` keyword arguments, we can specify the exact rows we wish to parse.

### Transposed Data
#### File
```
col1,1,2,3
col2,4,5,6
col3,7,8,9
```
#### Syntax
```julia
CSV.File(file; transpose=true)
```
This file has the column names in the first column, and data that extends alongs rows horizontally. The data for `col1` is all on the first row, similarly for `col2` and its data on row 2. In this case, we wish to read the file "transposed", or treating rows as columns. By passing `transpose=true`, CSV.jl will read column names from the first column, and the data for each column from its corresponding row.

### Commented Rows
#### File
```
col1,col2,col3
# this row is commented and we'd like to ignore it while parsing
1,2,3
4,5,6
```
#### Syntax
```julia
CSV.File(file; comment="#")
CSV.File(file; datarow=3)
```
This file has some rows that begin with the `"#"` string and denote breaks in the data for commentary. We wish to ignore these rows for purposes of reading data. We can pass `comment="#"` and parsing will ignore any row that begins with this string. Alternatively, we can pass `datarow=3` for this example specifically since there is only the one row to skip.

### Missing Strings
#### File
```
code,age,score
0,21,3.42
1,42,6.55
-999,81,NA
-999,83,NA
```
#### Syntax
```julia
CSV.File(file; missingstring="-999")
CSV.File(file; missingstrings=["-999", "NA"])
```
In this file, our `code` column has two expected codes, `0` and `1`, but also a few "invalid" codes, which are input as `-999`. We'd like to read the column as `Int64`, but treat the `-999` values as "missing" values. By passing `missingstring="-999"`, we signal that this value should be replaced with the literal `missing` value builtin to the Julia language. We can then do things like `dropmissing(f.col1)` to ignore those values, for example. In the second recommended syntax, we also want to treat the `NA` values in our `score` column as `missing`, so we pass both strings like `missingstrings=["-999", "NA"]`.

### Fixed Width Files
#### File
```
col1    col2 col3
123431  2    3421
2355    346  7543
```
#### Syntax
```julia
CSV.File(file; delim=' ', ignorerepeated=true)
```
This is an example of a "fixed width" file, where each column is the same number of characters away from each other on each row. This is different from a normal delimited file where each occurence of a delimiter indicates a separate field. With fixed width, however, fields are "padded" with extra delimiters (in this case `' '`) so that each column is the same number of characters each time. In addition to our `delim`, we can pass `ignorerepeated=true`, which tells parsing that consecutive delimiters should be treated as a single delimiter.

### Quoted & Escaped Fields
#### File
```
col1,col2
"quoted field with a delimiter , inside","quoted field that contains a \\n newline and ""inner quotes"""
unquoted field,unquoted field with "inner quotes"
```
#### Syntax
```julia
CSV.File(file; quotechar='"', escapechar='"')
CSV.File(file; openquotechar='"', closequotechar='"', escapechar='"')
```
In this file, we have a few "quoted" fields, which means the field's value starts and ends with `quotechar` (or `openquotechar` and `closequotechar`, respectively). Quoted fields allow the field to contain characters that would otherwise be significant to parsing, such as delimiters or newline characters. When quoted, parsing will ignore these otherwise signficant characters until the closing quote character is found. For quoted fields that need to also include the quote character itself, an escape character is provided to tell parsing to ignore the next character when looking for a close quote character. In the syntax examples, the keyword arguments are passed explicitly, but these also happen to be the default values, so just doing `CSV.File(file)` would result in successful parsing.

### DateFormat
#### File
```
code,date
0,2019/01/01
1,2019/01/02
```
#### Syntax
```julia
CSV.File(file; dateformat="yyyy/mm/dd")
```
In this file, our `date` column has dates that are formatted like `yyyy/mm/dd`. We can pass just such a string to the `dateformat` keyword argument to tell parsing to use it when looking for `Date` or `DateTime` columns. Note that currently, only a single `dateformat` string can be passed to parsing, meaning multiple columns with different date formats cannot all be parsed as `Date`/`DateTime`.

### Custom Decimal Separator
#### File
```
col1;col2;col3
1,01;2,02;3,03
4,04;5,05;6,06
```
#### Syntax
```julia
CSV.File(file; delim=';', decimal=',')
```
In many places in the world, floating point number decimals are separated with a comma instead of a period (`3,14` vs. `3.14`). We can correctly parse these numbers by passing in the `decimal=','` keyword argument. Note that we probably need to explicitly pass `delim=';'` in this case, since the parser will probably think that it detected `','` as the delimiter.

### Custom Bool Strings
#### File
```
id,paid,attended
0,T,TRUE
1,F,TRUE
2,T,FALSE
3,F,FALSE
```
#### Syntax
```julia
CSV.File(file; truestrings=["T", "TRUE"], falsestrings=["F", "FALSE"])
```
By default, parsing only considers the string values `true` and `false` as valid `Bool` values. To consider alternative values, we can pass a `Vector{String}` to the `truestrings` and `falsestrings` keyword arguments.

### Matrix-like Data
#### File
```
1.0 0.0 0.0
0.0 1.0 0.0
0.0 0.0 1.0
```
#### Syntax
```julia
CSV.File(file; header=false)
CSV.File(file; header=false, delim=' ', type=Float64)
```
This file contains a 3x3 identity matrix of `Float64`. By default, parsing will detect the delimiter and type, but we can also explicitly pass `delim= ' '` and `type=Float64`, which tells parsing to explicitly treat each column as `Float64`, without having to guess the type on its own.

### Providing Types
#### File
```
col1,col2,col3
1,2,3
4,5,invalid
6,7,8
```
#### Syntax
```julia
CSV.File(file; types=Dict(3 => Int))
CSV.File(file; types=Dict(:col3 => Int))
CSV.File(file; types=Dict("col3" => Int))
CSV.File(file; types=[Int, Int, Int])
CSV.File(file; types=[Int, Int, Int], silencewarnings=true)
CSV.File(file; types=[Int, Int, Int], strict=true)
```
In this file, our 3rd column has an invalid value on the 2nd row `invalid`. Let's imagine we'd still like to treat it as an `Int` column, and ignore the `invalid` value. The syntax examples provide several ways we can tell parsing to treat the 3rd column as `Int`, by referring to column index `3`, or column name with `Symbol` or `String`. We can also provide an entire `Vector` of types for each column (and which needs to match the length of columns in the file). There are two additional keyword arguments that control parsing behavior; in the first 4 syntax examples, we would see a warning printed like `"warning: invalid Int64 value on row 2, column 3"`. In the fifth example, passing `silencewarnings=true` will suppress this warning printing. In the last syntax example, passing `strict=true` will result in an error being thrown during parsing.

### Typemap
#### File
```
zipcode,score
03494,9.9
12345,6.7
84044,3.4
```
#### Syntax
```julia
CSV.File(file; typemap=Dict(Int => String))
CSV.File(file; types=Dict(:zipcode => String))
```
In this file, we have U.S. zipcodes in the first column that we'd rather not treat as `Int`, but parsing will detect it as such. In the first syntax example, we pass `typemap=Dict(Int => String)`, which tells parsing to treat any detected `Int` columns as `String` instead. In the second syntax example, we alternatively set the `zipcode` column type manually.

### Pooled Values
#### File
```
id,code
A18E9,AT
BF392,GC
93EBC,AT
54EE1,AT
8CD2E,GC
```
#### Syntax
```julia
CSV.File(file)
CSV.File(file; pool=0.4)
CSV.File(file; pool=0.6)
```
In this file, we have an `id` column and a `code` column. There can be advantages with various DataFrame/table operations like joining and grouping when `String` values are "pooled", meaning each unique value is mapped to a `UInt64`. By default, `pool=0.1`, so string columns with low cardinality are pooled by default. Via the `pool` keyword argument, we can provide greater control: `pool=0.4` means that if 40% or less of a column's values are unique, then it will be pooled.

### Reading CSV from gzip (.gz) and zip files

#### Example: reading from a gzip (.gz) file
```julia
using CSV, DataFrames, CodecZlib
a = DataFrame(a = 1:3)
CSV.write("a.csv", a)

# Windows users who do not have gzip available on the PATH should manually gzip the CSV
;gzip a.csv

a_copy = open("a.csv.gz") do io
    CSV.read(GzipDecompressorStream(io))
end

a == a_copy # true; restored successfully

```

#### Example: reading from a zip file
```julia
using ZipFile, CSV, DataFrames

a = DataFrame(a = 1:3)
CSV.write("a.csv", a)

# zip the file; Windows users who do not have zip available on the PATH can manual zip the CSV
;zip a.zip a.csv

z = ZipFile.Reader("a.zip")

# identify the right file in zip
a_file_in_zip = filter(x->x.name == "a.csv", z.files)[1]

a_copy = CSV.read(a_file_in_zip)

a == a_copy
```
