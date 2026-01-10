# Reading

The format for this section will go through the various inputs/options supported by [`CSV.File`](@ref)/[`CSV.read`](@ref), with notes about compatibility with the other reading functionality ([`CSV.Rows`](@ref), [`CSV.Chunks`](@ref), etc.).

## [`input`](@id input)

A required argument for reading. Input data should be ASCII or UTF-8 encoded text; for other text encodings, use the [StringEncodings.jl](https://github.com/JuliaStrings/StringEncodings.jl) package to convert to UTF-8.

Any delimited input is ultimately converted to a byte buffer (`Vector{UInt8}`) for parsing/processing, so with that in mind, let's look at the various supported input types:

  * File name as a `String` or [`FilePath`](https://juliahub.com/docs/FilePaths/PrU4O/0.8.0/); parsing will call `Mmap.mmap(string(file))` to get a byte buffer to the file data. For gzip compressed inputs, like `file.gz`, the [CodecZlib.jl](https://juliahub.com/docs/CodecZlib/1TI30/0.7.0/) package will be used to decompress the data to a temporary file first, then mmapped to a byte buffer. Decompression can also be done in memory by passing `buffer_in_memory=true`. Note that only gzip-compressed data is automatically decompressed; for other forms of compressed data, seek out the appropriate package to decompress and pass an IO or `Vector{UInt8}` of decompressed data as input.
  * `Vector{UInt8}` or `SubArray{UInt8, 1, Vector{UInt8}}`: if you already have a byte buffer from wherever, you can just pass it in directly. If you have a csv-formatted string, you can pass it like `CSV.File(IOBuffer(str))`
  * `IO` or `Cmd`: you can pass an `IO` or `Cmd` directly, which will be consumed into a temporary file, then mmapped as a byte vector; to avoid a temp file and instead buffer data in memory, pass `buffer_in_memory=true`.
  * For files from the web, you can call `HTTP.get(url).body` to request the file, then access the data as a `Vector{UInt8}` from the `body` field, which can be passed directly for parsing. For Julia 1.6+, you can also use the `Downloads` stdlib, like `Downloads.download(url)` which can be passed to parsing

### Examples
  * [StringEncodings.jl example](@ref stringencodings)
  * [Vector of inputs example](@ref vectorinputs)
  * [Gzip input](@ref gzipped_input)
  * [Delimited data in a string](@ref csv_string)
  * [Data from the web](@ref http)
  * [Data in zip archive](@ref zip_example)

## [`header`](@id header)

The `header` keyword argument controls how column names are treated when processing files. By default, it is assumed that the column names are the first row/line of the input, i.e. `header=1`. Alternative valid augments for `header` include:
  * `Integer`, e.g. `header=2`: provide the row number as an `Integer` where the column names can be found
  * `Bool`, e.g. `header=false`: no column names exist in the data; column names will be auto-generated depending on the # of columns, like `Column1`, `Column2`, etc.
  * `Vector{String}` or `Vector{Symbol}`: manually provide column names as strings or symbols; should match the # of columns in the data. A copy of the `Vector` will be made and converted to `Vector{Symbol}`
  * `AbstractVector{<:Integer}`: in rare cases, there may be multi-row headers; by passing a collection of row numbers, each row will be parsed and the values for each row will be concatenated to form the final column names

### Examples
  * [Column names on second row](@ref second_row_header)
  * [No column names in the data](@ref no_header)
  * [Manually provide column names](@ref manual_header)
  * [Multi-row column names](@ref multi_row_header)

## [`normalizenames`](@id normalizenames)

Controls whether column names will be "normalized" to valid Julia identifiers. By default, this is `false`. If `normalizenames=true`, then column names with spaces, or that start with numbers, will be adjusted with underscores to become valid Julia identifiers. This is useful when you want to access columns via dot-access or `getproperty`, like `file.col1`. The identifier that comes after the `.` must be valid, so spaces or identifiers starting with numbers aren't allowed.

### Examples
  * [Normalizing column names](@ref normalize_header)

## [`skipto`](@id skipto)

An `Integer` can be provided that specifies the row number where the data is located. By default, the row immediately following the header row is assumed to be the start of data. If `header=false`, or column names are provided manually as `Vector{String}` or `Vector{Symbol}`, the data is assumed to start on row 1, i.e. `skipto=1`.

### Examples
  * [Skip to specific row where data starts](@ref skipto_example)

## [`footerskip`](@id footerskip)

An `Integer` argument specifying the number of rows to ignore at the end of a file. This works by the parser starting at the end of the file and parsing in reverse until `footerskip` # of rows have been parsed, then parsing the entire file, stopping at the newly adjusted "end of file".

### Examples
  * [Skipping trailing useless rows](@ref footerskip_example)

## [`transpose`](@id transpose)

If `transpose=true` is passed, data will be read "transposed", so each row will be parsed as a column, and each column in the data will be returned as a row. Useful when data is extremely wide (many columns), but you want to process it in a "long" format (many rows). Note that multithreaded parsing is not supported when parsing is transposed.

### Examples
  * [Reading transposed data](@ref transpose_example)

## [`comment`](@id comment)

A `String` argument that, when encountered at the start of a row while parsing, will cause the row to be skipped. When providing `header`, `skipto`, or `footerskip` arguments, it should be noted that commented rows, while ignored, still count as "rows" when skipping to a specific row. In this way, you can visually identify, for example, that column names are on row 6, and pass `header=6`, even if row 5 is a commented row and will be ignored.

### Examples
  * [Ignoring commented rows](@ref comment_example)

## [`ignoreemptyrows`](@id ignoreemptyrows)

This argument specifies whether "empty rows", where consecutive [newlines](@ref newlines) are parsed, should be ignored or not. By default, they are. If `ignoreemptyrows=false`, then for an empty row, all existing columns will have `missing` assigned to their value for that row. Similar to commented rows, empty rows also still count as "rows" when any of the `header`, `skipto`, or `footerskip` arguments are provided.

### Examples
  * [Ignoring empty rows](@ref ignoreemptyrows_example)

## [`select` / `drop`](@id select)

Arguments that control which columns from the input data will actually be parsed and available after processing. `select` controls which columns _will_ be accessible after parsing while `drop` controls which columns to _ignore_. Either argument can be provided as a vector of `Integer`, `String`, or `Symbol`, specifying the column numbers or names to include/exclude. A vector of `Bool` matching the number of columns in the input data can also be provided, where each element specifies whether the corresponding column should be included/excluded. Finally, these arguments can also be given as boolean functions, of the form `(i, name) -> Bool`, where each column number and name will be given as arguments and the result of the function will determine if the column will be included/excluded.

### Examples
  * [Including/excluding columns](@ref select_example)

## [`limit`](@id limit)

An `Integer` argument to specify the number of rows that should be read from the data. Can be used in conjunction with [`skipto`](@ref skipto) to read contiguous chunks of a file. Note that with multithreaded parsing (when the data is deemed large enough), it can be difficult for parsing to determine the exact # of rows to limit to, so it may or may not return exactly `limit` number of rows. To ensure an exact limit on larger files, also pass `ntasks=1` to force single-threaded parsing.

### Examples
  * [Limiting number of rows from data](@ref limit_example)

## [`ntasks`](@id ntasks)

NOTE: not applicable to `CSV.Rows`

For large enough data inputs, `ntasks` controls the number of multithreaded tasks used to concurrently parse the data. By default, it uses `Threads.nthreads()`, which is the number of threads the julia process was started with, either via `julia -t N` or the `JULIA_NUM_THREADS` environment variable. To avoid multithreaded parsing, even on large files, pass `ntasks=1`. This argument is only applicable to `CSV.File`, not `CSV.Rows`. For `CSV.Chunks`, it controls the total number of chunk iterations a large file will be split up into for parsing.

## [`rows_to_check`](@id rows_to_check)

NOTE: not applicable to `CSV.Rows`

When input data is large enough, parsing will attempt to "chunk" up the data for multithreaded tasks to parse concurrently. To chunk up the data, it is split up into even chunks, then initial parsers attempt to identify the correct start of the first row of that chunk. Once the start of the chunk's first row is found, each parser will check `rows_to_check` number of rows to ensure the expected number of columns are present.

## [`source`](@id source)

NOTE: only applicable to vector of inputs passed to `CSV.File`

A `Symbol`, `String`, or `Pair` of `Symbol` or `String` to `Vector`. As a single `Symbol` or `String`, provides the column name that will be added to the parsed columns, the values of the column will be the input "name" (usually file name) of the input from whence the value was parsed. As a `Pair`, the 2nd part of the pair should be a `Vector` of values matching the length of the # of inputs, where each value will be used instead of the input name for that inputs values in the auto-added column.

## [`missingstring`](@id missingstring)

Argument to control how `missing` values are handled while parsing input data. The default is `missingstring=""`, which means two consecutive delimiters, like `,,`, will result in a cell being set as a `missing` value. Otherwise, you can pass a single string to use as a "sentinel", like `missingstring="NA"`, or a vector of strings, where _each_ will be checked for when parsing, like `missingstring=["NA", "NAN", "NULL"]`, and if _any_ match, the cell will be set to `missing`. By passing `missingstring=nothing`, no `missing` values will be checked for while parsing.

### Examples
  * [Specifying custom missing strings](@ref missing_string_example)

## [`delim`](@id delim)

A `Char` or `String` argument that parsing looks for in the data input that separates distinct columns on each row. If no argument is provided (the default), parsing will try to detect the most consistent delimiter on the first 10 rows of the input, falling back to a single comma (`,`) if no other delimiter can be detected consistently.

### Examples
  * [String delimiter](@ref string_delim)

## [`ignorerepeated`](@id ignorerepeated)

A `Bool` argument, default `false`, that, if set to `true`, will cause parsing to ignore any number of consecutive delimiters between columns. This option can often be used to accurately parse fixed-width data inputs, where columns are delimited with a fixed number of delimiters, or a row is fixed-width and columns may have a variable number of delimiters between them based on the length of cell values.

### Examples
  * [Fixed width files](@ref ignorerepeated_example)

## [`quoted`](@id quoted)

A `Bool` argument that controls whether parsing will check for opening/closing quote characters at the start/end of cells. Default `true`. If you happen to know a file has no quoted cells, it can simplify parsing to pass `quoted=false`, so parsing avoids treating the `quotechar` or `openquotechar`/`closequotechar` arguments specially.

### Examples
  * [Turning off quoted cell parsing](@ref quoted_example)

## [`quotechar` / `openquotechar` / `closequotechar`](@id quotechar)

An ASCII `Char` argument (or arguments if both `openquotechar` and `closequotechar` are provided) that parsing uses to handle "quoted" cells. If a cell string value contains the [delim](@ref delim) argument, or a newline, it should start and end with `quotechar`, or start with `openquotechar` and end with `closequotechar` so parsing knows to treat the `delim` or newline as part of the cell _value_ instead of as significant parsing characters. If the `quotechar` or `closequotechar` characters also need to appear in the cell value, they should be properly escaped via the [escapechar](@ref escapechar) argument.

### Examples
  * [Quoted & escaped fields](@ref quotechar_example)

## [`escapechar`](@id escapechar)

An ASCII `Char` argument that parsing uses when parsing quoted cells and the `quotechar` or `closequotechar` characters appear in a cell string value. If the `escapechar` character is encountered inside a quoted cell, it will be "skipped", and the following character will not be checked for parsing significance, but just treated as another character in the value of the cell. Note the `escapechar` is _not_ included in the value of the cell, but is ignored completely.

## [`dateformat`](@id dateformat)

A `String` or `AbstractDict` argument that controls how parsing detects datetime values in the data input. As a single `String` (or `DateFormat`) argument, the same format will be applied to _all_ columns in the file. For columns without type information provided otherwise, parsing will use the provided format string to check if the cell is parseable and if so, will attempt to parse the entire column as the datetime type (`Time`, `Date`, or `DateTime`). By default, if no `dateformat` argument is explicitly provided, parsing will try to detect any of `Time`, `Date`, or `DateTime` types following the standard `Dates.ISOTimeFormat`, `Dates.ISODateFormat`, or `Dates.ISODateTimeFormat` formats, respectively. If a datetime type is provided for a column, (see the [types](@ref types) argument), then the `dateformat` format string needs to match the format of values in that column, otherwise, a warning will be emitted and the value will be replaced with a `missing` value (this behavior is also configurable via the [strict](@ref) and [silencewarnings](@ref strict) arguments). If an `AbstractDict` is provided, different `dateformat` strings can be provided for specific columns; the provided dict can map either an `Integer` for column number or a `String`, `Symbol` or `Regex` for column name to the dateformat string that should be used for that column. Columns not mapped in the dict argument will use the default format strings mentioned above.

### Examples
  * [DateFormat](@ref dateformat_example)

## [`decimal`](@id decimal)

An ASCII `Char` argument that is used when parsing float values that indicates where the fractional portion of the float value begins. i.e. for the truncated values of pie `3.14`, the `'.'` character separates the `3` and `14` values, whereas for `3,14` (common European notation), the `','` character separates the fractional portion. By default, `decimal='.'`.

### Examples
  * [Custom decimal separator](@ref decimal_example)

## [`groupmark` / thousands separator](@id groupmark)

A "groupmark" is a symbol that separates groups of digits so that it easier for humans to read a number. Thousands separators are a common example of groupmarks. The argument `groupmark`, if provided, must be an ASCII `Char` which will be ignored during parsing when it occurs between two digits on the left hand side of the decimal. e.g the groupmark in the integer `1,729` is `','` and the groupmark for the US social security number `875-39-3196` is `-`. By default, `groupmark=nothing` which indicates that there are no stray characters separating digits.

### Examples
  * [Thousands separator](@ref thousands_example)
  * [Custom groupmarks](@ref groupmark_example)

## [`truestrings` / `falsestrings`](@id truestrings)

These arguments can be provided as `Vector{String}` to specify custom values that should be treated as the `Bool` `true`/`false` values for all the columns of a data input. By default, `["true", "True", "TRUE", "T", "1"]` string values are used to detect `true` values, and `["false", "False", "FALSE", "F", "0"]` string values are used to detect `false` values. Note that even though `"1"` and `"0"` _can_ be used to parse `true`/`false` values, in terms of _auto_ detecting column types, those values will be parsed as `Int64` first, instead of `Bool`. To instead parse those values as `Bool`s for a column, you can manually provide that column's type as `Bool` (see the [type](@ref types) argument).

### Examples
  * [Custom bool strings](@ref truestrings_example)

## [`types`](@id types)

Argument to control the types of columns that get parsed in the data input. Can be provided as a single `Type`, an `AbstractVector` of types, an `AbstractDict`, or a function.
- If a single type is provided, like `types=Float64`, then _all_ columns in the data input will be parsed as `Float64`. If a column's value isn't a valid `Float64` value, then a warning will be emitted, unless `silencewarnings=false` is passed, then no warning will be printed. However, if `strict=true` is passed, then an error will be thrown instead, regarldess of the `silencewarnings` argument.
- If a `AbstractVector{Type}` is provided, then the length of the vector should match the number of columns in the data input, and each element gives the type of the corresponding column in order.
- If an `AbstractDict`, then specific columns can have their column type specified with the key of the dict being an `Integer` for column number, or `String` or `Symbol` for column name or `Regex` matching column names, and the dict value being the column type. Unspecified columns will have their column type auto-detected while parsing.
- If a function, then it should be of the form `(i, name) -> Union{T, Nothing}`, and will be applied to each detected column during initial parsing. Returning `nothing` from the function will result in the column's type being automatically detected during parsing.

By default `types=nothing`, which means all column types in the data input will be detected while parsing. Note that it isn't necessary to pass `types=Union{Float64, Missing}` if the data input contains `missing` values. Parsing will detect `missing` values if present, and promote any manually provided column types from the singular (`Float64`) to the missing equivalent (`Union{Float64, Missing}`) automatically. Standard types will be auto-detected in the following order when not otherwise specified:  `Int64`, `Float64`, `Date`, `DateTime`, `Time`, `Bool`, `String`.

Non-standard types can be provided, like `Dec64` from the DecFP.jl package, but must support the `Base.tryparse(T, str)` function for parsing a value from a string. This allows, for example, easily defining a custom type, like `struct Float64Array; values::Vector{Float64}; end`, as long as a corresponding `Base.tryparse` definition is defined, like `Base.tryparse(::Type{Float64Array}, str) = Float64Array(map(x -> parse(Float64, x), split(str, ';')))`, where a single cell in the data input is like `1.23;4.56;7.89`.

Note that the default [stringtype](@ref stringtype) can be overridden by providing a column's type manually, like `CSV.File(source; types=Dict(1 => String), stringtype=PosLenString)`, where the first column will be parsed as a `String`, while any other string columns will have the `PosLenString` type.

### Examples
  * [Matrix-like Data](@ref matrix_example)
  * [Providing types](@ref types_example)

## [`typemap`](@id typemap)

An `AbstractDict{Type, Type}` argument that allows replacing a non-`String` standard type with another type when a column's type is auto-detected. Most commonly, this would be used to force all numeric columns to be `Float64`, like `typemap=IdDict(Int64 => Float64)`, which would cause any columns detected as `Int64` to be parsed as `Float64` instead. Another common case would be wanting all columns of a specific type to be parsed as strings instead, like `typemap=IdDict(Date => String)`, which will cause any columns detected as `Date` to be parsed as `String` instead.

### Examples
  * [Typemap](@ref typemap_example)

## [`pool`](@id pool)

Argument that controls whether columns will be returned as `PooledArray`s. Can be provided as a `Bool`, `Float64`, `Tuple{Float64, Int}`, vector, dict, or a function of the form `(i, name) -> Union{Bool, Real,  Tuple{Float64, Int}, Nothing}`. As a `Bool`, controls absolutely whether a column will be pooled or not; if passed as a single `Bool` argument like `pool=true`, then all string columns will be pooled, regardless of cardinality. When passed as a `Float64`, the value should be between `0.0` and `1.0` to indicate the threshold under which the % of unique values found in the column will result in the column being pooled. For example, if `pool=0.1`, then all string columns with a unique value % less than 10% will be returned as `PooledArray`, while other string columns will be normal string vectors. If `pool` is provided as a tuple, like `(0.2, 500)`, the first tuple element is the same as a single `Float64` value, which represents the % cardinality allowed. The second tuple element is an upper limit on the # of unique values allowed to pool the column. So the example, `pool=(0.2, 500)` means if a String column has less than or equal to 500 unique values _and_ the # of unique values is less than 20% of total # of values, it will be pooled, otherwise, it won't. As mentioned, when the `pool` argument is a single `Bool`, `Real`, or `Tuple{Float64, Int}`, only string columns will be considered for pooling. When a vector or dict is provided, the pooling for any column can be provided as a `Bool`, `Float64`, or `Tuple{Float64, Int}`. Similar to the [types](@ref types) argument, providing a vector to `pool` should have an element for each column in the data input, while a dict argument can map column number/name to `Bool`, `Float64`, or `Tuple{Float64, Int}` for specific columns. Unspecified columns will not be pooled when the argument is a dict.

### Examples
  * [Pooled values](@ref pool_example)
  * [Non-string column pooling](@ref nonstring_pool_example)
  * [Pool with absolute threshold](@ref pool_absolute_threshold)

## [`downcast`](@id downcast)

A `Bool` argument that controls whether `Integer` detected column types will be "shrunk" to the smallest possible integer type. Argument is `false` by default. Only applies to auto-detected column types; i.e. if a column type is provided manually as `Int64`, it will not be shrunk. Useful for shrinking the overall memory footprint of parsed data, though care should be taken when processing the results as Julia by default as [integer overflow](https://en.wikipedia.org/wiki/Integer_overflow) behavior, which is increasingly likely the smaller the integer type.

## [`stringtype`](@id stringtype)

An argument that controls the precise type of string columns. Supported values are `InlineString` (the default), `PosLenString`, or `String`. The various string types are aimed at being mostly transparent to most users. In certain workflows, however, it can be advantageous to be more specific. Here's a quick rundown of the possible options:

  * `InlineString`: a set of fixed-width, stack-allocated primitive types. Can take memory pressure off the GC because they aren't reference types/on the heap. For very large files with string columns that have a fairly low variance in string length, this can provide much better GC interaction than `String`. When string length has a high variance, it can lead to lots of "wasted space", since an entire column will be promoted to the smallest InlineString type that fits the longest string value. For small strings, that can mean a lot of wasted space when they're promoted to a high fixed-width.
  * `PosLenString`: results in columns returned as `PosLenStringVector` (or `ChainedVector{PosLenStringVector}` for the multithreaded case), which holds a reference to the original input data, and acts as one large "view" vector into the original data where each cell begins/ends. Can result in the smallest memory footprint for string columns. `PosLenStringVector`, however, does not support traditional mutable operations like regular `Vector`s, like `push!`, `append!`, or `deleteat!`.
  * `String`: each string must be heap-allocated, which can result in higher GC pressure in very large files. But columns are returned as normal `Vector{String}` (or `ChainedVector{Vector{String}}`), which can be processed normally, including any mutating operations.

## [`strict` / `silencewarnings` / `maxwarnings`](@id strict)

Arguments that control error behavior when invalid values are encountered while parsing. Only applicable when types are provided manually by the user via the [types](@ref types) argument. If a column type is manually provided, but an invalid value is encountered, the default behavior is to set the value for that cell to `missing`, emit a warning (i.e. `silencewarnings=false` and `strict=false`), but only up to 100 total warnings and then they'll be silenced (i.e. `maxwarnings=100`). If `strict=true`, then invalid values will result in an error being thrown instead of any warnings emitted.

## [`debug`](@id debug)

A `Bool` argument that controls the printing of extra "debug" information while parsing. Can be useful if parsing doesn't produce the expected result or a bug is suspected in parsing somehow.

## API Reference

```@docs
CSV.read
CSV.File
CSV.Chunks
CSV.Rows
```

### Utilities
```@docs
CSV.detect
```

## Common terms

### Standard types

The types that are detected by default when column types are not provided by the user otherwise. They include: `Int64`, `Float64`, `Date`, `DateTime`, `Time`, `Bool`, and `String`.

### [Newlines](@id newlines)

For all parsing functionality, newlines are detected/parsed automatically, regardless if they're present in the data as a single newline character (`'\n'`), single return character ('`\r'`), or full CRLF sequence (`"\r\n"`).

### Cardinality

Refers to the ratio of unique values to total number of values in a column. Columns with "low cardinality" have a low % of unique values, or put another way, there are only a few unique values for the entire column of data where unique values are repeated many times. Columns with "high cardinality" have a high % of unique values relative to total number of values. Think of these as "id-like" columns where each or almost each value is a unique identifier with no (or few) repeated values.
