# Reading data

`CSV.File(source; keywords...)` is the main reader. It returns an eager,
column-oriented Tables.jl table.

```@example reading
using CSV

data = IOBuffer("id,name,active\n1,Ada,true\n2,Grace,false\n")
file = CSV.File(data)

(names(file), length(file), String.(file.name), collect(file.active))
```

Access columns with `file.name`, `file[:name]`, `file["name"]`, or
`Tables.getcolumn(file, :name)`. Index `file[i]` to get row `i`. The row
supports integer and name access through the Tables.jl row interface. Use
`names(file)` or `Tables.columnnames(file)` for names. The compatibility form
`file.names` also works unless the data has a column named `names`; data
columns take priority over compatibility properties.

## Input and memory behavior

Supported sources are:

- a local file path;
- an `http://` or `https://` URL;
- an `IO` or `Cmd`;
- a `Vector{UInt8}` or another byte vector;
- a FilePathsBase path when FilePathsBase is loaded; or
- a vector of the preceding source types.

A string is a path or URL. Wrap literal delimited text in `IOBuffer`.

Large regular local files are memory-mapped. Small local files, `IO` objects,
commands, URLs, and non-`Vector{UInt8}` byte containers are read into memory.
`buffer_in_memory=true` also copies a local file into memory. Gzip is detected
from its magic bytes for every source type and is fully decompressed into
memory before parsing. Other compression formats must be decompressed before
they are passed to CSV.jl.

Tables from `CSV.File` and `CSV.read`, and `CSV.Chunks` batches, own their
text: they never refer to the source, so rewriting or deleting the file
afterwards does not change them, and a `Vector{UInt8}` source is never
aliased. `CSV.Rows` and `CSV.lazy` cells are views into the source; keep the
source unchanged while you use them, or convert values with `String`.

### Gzipped text in other encodings

Gzip decompression does not convert the text encoding. For a non-UTF-8 gzip
file, decompress its bytes first, then decode them before calling CSV.jl.
Install `CodecZlib` and `StringEncodings` in your environment for this example.
For an ISO-8859-1 file at `path`:

```@example reading-encoded-gzip
using CSV, CodecZlib, StringEncodings

dir = mktempdir() # hide
path = joinpath(dir, "cities.csv.gz") # hide
write(path, transcode(GzipCompressor, encode("city,population\nMontréal,100\nMálaga,200\n", "ISO-8859-1"))) # hide
bytes = transcode(GzipDecompressor, read(path))
decoder = StringDecoder(IOBuffer(bytes), "ISO-8859-1")
file = try
    CSV.File(decoder)
finally
    close(decoder)
end
@assert names(file) == [:city, :population] # hide
@assert collect(file.population) == [100, 200] # hide
@assert String.(file.city) == ["Montréal", "Málaga"] # hide
rm(dir; recursive=true) # hide

String.(file.city)
```

CSV.jl does not detect encodings; name the file's actual encoding. Closing the
decoder checks for an incomplete final character.

## Headers and row windows

`header=1` reads names from the first row. Other accepted forms are:

- an integer row number;
- `false` or `0` to generate `Column1`, `Column2`, and so on;
- a vector of names; or
- an increasing vector of row numbers whose values are joined into names.

Duplicate names are made unique. `normalizenames=true` converts names to valid
Julia identifiers.

Use `skipto` for the first data row, `footerskip` to omit rows at the end, and
`limit` to cap the returned data rows. `header` and `skipto` count every
physical row, including empty and comment rows. `footerskip` counts empty rows
but not comment rows.

```@example reading-window
using CSV

data = IOBuffer("metadata\nfirst value,second value\n1,2\n3,4\n5,6\n")
file = CSV.File(data; header=2, normalizenames=true, limit=2)

(names(file), collect(file.first_value))
```

## Delimited-text syntax

`delim=nothing` detects one of comma, tab, space, pipe, semicolon, or colon
from a bounded sample. Pass a delimiter when the format is known.

The main dialect options are:

| Keyword | Default | Purpose |
|:--------|:--------|:--------|
| `delim` | `nothing` | Detect a delimiter, or use the supplied delimiter |
| `quoted` | `true` | Recognize quoted fields |
| `quotechar` | `'"'` | Set one opening and closing quote byte |
| `openquotechar`, `closequotechar` | unset | Set distinct quote bytes |
| `escapechar` | closing quote | Escape a structural byte in a quoted field |
| `comment` | unset | Ignore rows that start with this string |
| `ignoreemptyrows` | `true` | Omit physically empty rows |
| `ignorerepeated` | `false` | Treat consecutive delimiters as one delimiter |

Input must be ASCII or UTF-8. Convert other encodings before parsing.

A quote that does not start its field is content: `10,Pipe 3" long` and
`1,x"y` are ordinary two-field rows. Such a quote makes CSV.jl index the input
a second time with a slower scanner. A quote that starts a field and is never
closed is reported as an `:unclosed_quote` problem.

## Missing values and value syntax

An unquoted empty field is always `missing`. `missingstring`
adds one sentinel or a vector of sentinels. It does not turn off the empty-field
rule. The writer uses a quoted empty field for a present empty string.

```@example reading-missing
using CSV

data = IOBuffer("value,label\n,empty field\nNA,sentinel\n\"\",present empty string\n")
file = CSV.File(data; missingstring="NA", stringtype=String)

collect(file.value)
```

Boolean inference recognizes `true`, `True`, `TRUE`, `false`, `False`, and
`FALSE`. Add other spellings with `truestrings` and `falsestrings`; a user list
replaces the defaults. Other value options include:

- `dateformat` as one format string or `Dates.DateFormat`, or a per-column
  dictionary of them;
- `decimal` for a decimal separator (it cannot be a digit, sign, or exponent
  letter);
- `groupmark` for grouped digits;
- `stripwhitespace`; and
- `typemap` to replace an inferred type.

Numeric detection runs before custom date formats, so `20190101` infers as an
integer even with `dateformat="yyyymmdd"`. Load `Dates` and pass
`types=Dict(:date => Date)` to request date parsing for that column. See the
[DateFormat example](@ref dateformat_example).

Default date and time inference accepts the ISO forms `yyyy-mm-dd`,
`yyyy-mm-ddTHH:MM:SS`, `yyyy-mm-dd HH:MM:SS`, and `HH:MM:SS`. The time forms
accept fractional seconds. A date-time column infers as
`Timestamp{Nanosecond}` from [Durations.jl](https://github.com/JuliaData/Durations.jl),
which uses `Dates.Timestamp` when the standard library provides it. This 8-byte instant keeps
up to nine fraction digits. An instant outside the nanosecond range
(`1677-09-21T00:12:43.145224192` through `2262-04-11T23:47:16.854775807`)
widens the column to `Timestamp{Microsecond}` if every value fits that type
exactly. For example, a `9999-12-31T00:00:00` sentinel requires microseconds.
If another value requires finer precision, or a value exceeds the microsecond
range, the column stays text. A time column infers as
`Dates.Time`, which also keeps nanoseconds. Request `Dates.DateTime` with
`types`; it holds milliseconds, so a finer fraction such as
`2024-01-02T12:00:00.123456` is reported as a problem.
`typemap=Dict(Timestamp{Nanosecond} => DateTime)` changes an inferred
nanosecond type to `DateTime`; a rejected fraction promotes that column to
text. Add `Timestamp{Microsecond} => DateTime` to also map the wider range.
Explicit `Timestamp{Millisecond}` and `Timestamp{Second}` requests check
exactness at their resolution.

`Date` and `DateTime` parsing rejects calendar years outside the ranges
reported by `typemin` and `typemax` for those Dates types. An explicit type
request records a problem; an inferred date column becomes text.

## Types, columns, strings, and pools

`types` accepts one type, a vector with one entry per source column, or a
dictionary keyed by column index, name, or a `Regex`. A type vector must match
the header. Function-valued `types` is not supported. A requested
`String` (or `Union{Missing, String}`) names the output type: that column is a
`Vector{String}` or `Vector{Union{Missing, String}}` when pooling is off.
With pooling, its levels use `String`. Request `DataStrings.DataString` to keep the
parsed column as it is, or an InlineStrings.jl type when that package is loaded.
`stringtype` governs inferred text only.

A requested type that CSV does not know parses through the type's own
parser. Define `Parsers.tryparse(::Type{T}, buf::AbstractVector{UInt8},
i::Integer, j::Integer)` to parse the field bytes in place, or
`Base.tryparse(::Type{T}, ::String)` to parse a `String` made from them. The
method must return `nothing` for text it cannot parse; that cell is then a
problem. A type with neither method is an `ArgumentError` before parsing
starts, and a parser that throws aborts the read.

With DataDecimals.jl loaded, an
explicitly requested decimal type such as `types=Dict(:amount =>
DataDecimals.Decimal64{2})` parses exactly from the field bytes: a value that
needs rounding is a problem. CSV does not infer decimal types; fractional
numbers infer as `Float64`.

`select` and `drop` accept lists of indices, names, or a Boolean mask, one
name or index, or a `Regex` matched against the column names. They are
mutually exclusive. `CSV.File`, `CSV.lazy`, `CSV.Rows`, and `CSV.Chunks` all
return selected columns once, in file order, even when the list is repeated or
reordered. A function of the form `(i, name) -> Bool` receives each column's
position and name (a `Symbol`); `select` keeps and `drop` removes the columns
for which it returns `true`. It sees only header names, so it runs before any
value is read, and CSV can call it more than once for a column. Use a
`Tables.Scan` request for expression-based projection and filtering.

CSV.jl uses `DataStrings.DataString` for inferred text columns by default:

```@example reading-strings
using CSV

file = CSV.File(IOBuffer("value\nalpha\nbeta\n"))
(eltype(file.value), String(file.value[1]))
```

Use `stringtype=String` to materialize strings. When InlineStrings.jl is
loaded, its extension also accepts `InlineString` and fixed inline string
types. `stringtype=InlineString` picks the smallest width per column up to
`String31`; a column whose text is longer comes back as `String`.
A fixed type such as `String15` is an error when a value does not fit.
InlineStrings 1.4.6 and 2 are supported.

Pooling is independent of `stringtype`. `pool=false` is the default. The
accepted forms are:

- `pool=true` to pool every text column;
- a ratio such as `pool=0.2`;
- `(ratio, maximum_levels)` such as `pool=(0.2, 500)`; or
- a dictionary or vector of per-column policies for `CSV.File`.

Only text columns are pooled, into PooledArrays.jl vectors.

With `transpose=true`, input rows become output columns, and `types`,
`dateformat`, and `pool` dictionaries use the output column names. `select`,
`drop`, and `scan` are not supported with `transpose=true`.

## Parse problems

`CSV.File` and `CSV.Chunks` batches record malformed quotes, invalid typed
values, and short or long rows as `CSV.Problem` values instead of failing the
read. `CSV.problems(file)` returns them; each has `row`, `col`, `pos`, `kind`,
and `message` fields. The default `on_error=:warn` also prints one summary
warning per read; `on_error=:collect` records problems without the warning.

```@example reading-problems
using CSV

file = CSV.File(IOBuffer("count\n1\ninvalid\n"); types=Int, on_error=:collect)
[(p.row, p.col, p.kind) for p in CSV.problems(file)]
```

`maxproblems` caps retained diagnostics and defaults to 10,000.
`CSV.Chunks` warns once, for its first batch with problems; every batch still
carries its own `CSV.problems(batch)`. `on_error=:error` (or `strict=true`)
throws a `CSV.ParseError` carrying the first problem in source order, the
total `nproblems`, and the `source` label. `validate=false` ignores `types`,
`dateformat`, and `pool` dictionary keys that do not match an input column;
validation is on by default.

## Parallel parsing and sampling

Parsing is parallel by default when Julia has multiple threads. `ntasks=N`
uses at most `N` tasks; `ntasks=1` or `parallel=false` uses one. Results do not
depend on the number of tasks.

The input is split into chunks of about `chunkbytes` bytes: by default about
four per task, each between 64 KiB and 1 MiB. Keep the default unless a
measurement says otherwise: a chunk that fits in cache keeps parsing fast, and
chunks of several MiB slow down files with many columns. `CSV.File(lazyfile)`
reuses the lazy file's chunks.

Type inference starts from a sample of `nsample` rows and widens a column's
type when a later value needs it. Only the rows a read returns take part: rows
after `limit`, and rows that a scan's filter, offset, or limit removes, do not
widen a type. Such a read can therefore type a column more narrowly than a read
of the whole file: with `limit=2` over `007`, `2`, `abc`, the column is `Int64`
and the first value is `7`. Request the type, for example
`types=Dict(:zip => String)`, to make it independent of the rows read.
`samplebytes` sets how much input delimiter detection reads.
`fastindex=false` indexes with a slower byte-at-a-time scanner, to check that
it agrees with the default vector scanner.

## Multiple sources

Pass a vector of sources to concatenate them vertically:

```@example reading-multiple
using CSV

sources = [IOBuffer("id,value\n1,10\n"), IOBuffer("value,id\n20,2\n")]
file = CSV.File(sources; source=:origin => ["first", "second"], stringtype=String)

(collect(file.id), collect(file.value), collect(file.origin))
```

The first source defines the output columns. Later sources match columns by
name: a missing column is filled with `missing`, and an extra column is
ignored. Column types combine with `promote_type`, so `Int64` and `Float64`
give `Float64`.

`source=:origin` adds a pooled source-label column. Path sources use their
paths. Other sources use deterministic labels such as `"<source 1>"`. Use
`source=:origin => labels` to supply one label per source.

## Direct reads into a sink

`CSV.read(source, sink; keywords...)` parses with the same options as
`CSV.File` and calls the Tables.jl sink. It wraps the new columns in
`Tables.CopiedColumns`, so sinks such as DataFrames.jl can take ownership.

```julia
using CSV, DataFrames
df = CSV.read("input.csv", DataFrame)
```

## Lazy indexed access

`CSV.lazy` indexes the input but parses a cell only when it is accessed. Its
`CSV.LazyFile` result supports `lazyfile.column`, `lazyfile[:column]`,
`lazyfile[row, column]`, and the Tables.jl column interface. It does not infer
types: a cell is text unless `types` names its column's type.

```@example reading-lazy
using CSV

lazyfile = CSV.lazy(IOBuffer("id,price\n1,3.5\n2,4.0\n");
                    types=Dict(:price => Float64))
(String(lazyfile.id[2]), lazyfile[1, :price])
```

`CSV.File(lazyfile)` performs the full parse without indexing the input again.
Row-position, dialect, and input-buffer options were fixed when the lazy file
was created. Selection is sticky: an eager parse starts from the columns
visible on the lazy file and can only select or drop further columns.

## Row iteration

`CSV.Rows` avoids eager column allocation. It still reads or maps the source
and builds an index before iteration. Text cells are lazy `DataStrings.DataString`
views by default. Provide `types` for typed cell access, or
`stringtype=String` for standalone strings.

```@example reading-rows
using CSV

rows = CSV.Rows(IOBuffer("id,value\n1,10\n2,20\n"); types=[Int, Int])
[row[:value] for row in rows]
```

`length(rows)` and `names(rows)` are known before iteration. `reusebuffer` is
accepted and ignored.

The row type carries the column names and types, so a loop over rows that a
function receives as an argument reads `row.value` as a typed value with no
dynamic dispatch. Construct the rows outside the function that iterates them:

```julia
total(rows) = sum(row.value for row in rows)
total(CSV.Rows("input.csv"; types=Dict(:value => Int)))
```

A loop in the same function that calls `CSV.Rows` still works, but each row
then costs a dynamic dispatch.

An invalid or malformed cell becomes `missing` when it is accessed. Pass
`strict=true` or `on_error=:error` to throw a `CSV.ParseError` at that access
instead. `CSV.Rows` keeps no problem log, so it rejects `on_error=:warn`,
`maxproblems`, and `maxwarnings`; use `CSV.File` for `CSV.problems`. `select`
and `drop` work as in `CSV.File`.

## Reading with a known schema

When a program knows the columns before it reads, it can pass them as a
`Tables.Schema`. `CSV.Rows(source; schema)` checks the source against the
schema and iterates `NamedTuple` rows of exactly the schema's types, and
`CSV.File(source; schema)` reads the same way into one vector per column:

```@example reading-schema
using CSV, Tables

schema = Tables.Schema((:id, :x, :y), (String, Float64, Union{Missing, Int}))
data = "id,x,y\na,1.5,3\nb,2,\n"
rows = CSV.Rows(IOBuffer(data); schema)
file = CSV.File(IOBuffer(data); schema)
(eltype(rows), collect(rows), typeof(file.y))
```

`Tables.schema(rows)` returns `schema`, and `Tables.columntable(rows)` builds
one vector per column: `Vector{String}`, `Vector{Float64}`, and
`Vector{Union{Missing, Int}}` here. A schema `CSV.File` checks every row before
it returns. Its columns are those same vectors, except that a `DataString`
column is a `DataStrings.StringVector`, the text column `CSV.File` returns
without a schema, which stores values without allocating each one.
`CSV.read(source, sink; schema)` passes the columns to a sink.

The read checks the source this way:

- Header: the header must spell the schema's names in order, once each. A
  missing, extra, renamed, reordered, or repeated column fails the read before
  any row is returned. Names are compared after `normalizenames`. A repeated
  name fails even when the schema uses the `x_1` name that `CSV.File` would
  give it. With `header=false`, the schema names the columns.
- Rows: `CSV.Rows` parses each row when iteration reaches it, and `CSV.File`
  parses them all before it returns. A row must have exactly one field per
  column. With `ignoreemptyrows=false`, an empty row has one empty
  field.
- Values: every field parses as its column type under the usual value options
  (quotes and escapes, `decimal`, `groupmark`, `dateformat`, `truestrings`,
  and so on). Nothing is inferred or converted: `1.5` is not an `Int`, `1` is
  a valid `Float64`, and `1` is not a `Bool`. An integer outside its type's
  range, such as `128` for `Int8`, fails. A float rounds to the nearest value
  of its type, as `Base.parse` does, so `1e400` reads as `Inf`.
- Missing values: an empty field or a `missingstring` spelling is `missing`,
  and only a `Union{T, Missing}` column accepts it. A quoted empty field `""`
  is an empty string in a text column and an invalid value in any other.

The first problem in the source throws a [`CSV.ParseError`](@ref CSV.ParseError).
Its problem records the data row (0 for the header), the column (0 for a
whole row), the byte position, and a kind: `:header_mismatch`, `:short_row`,
`:long_row`, `:invalid_value`, `:missing_value`, `:invalid_quoted_field`, or
`:unclosed_quote`. `CSV.Rows` iteration has already returned the rows before
an invalid row. Rows past `limit` are not read.

A column type is a concrete type that `types` accepts, alone or in a `Union`
with `Missing`: integers, floats, `Bool`, `Char`, `Date`, `DateTime`, `Time`,
`Timestamp`, `String`, `DataStrings.DataString`, `Symbol`, and extension types
such as DataDecimals decimals. The schema must carry its names and types as
type parameters, as `Tables.Schema(names, types)` does for up to 65,535
columns. `types`, `stringtype`, `select`, and `drop` do not apply, nor do
`pool`, `downcast`, `transpose`, and `scan` for `CSV.File`, and `on_error`
and `strict` only accept the strict behavior. The other keywords of each
reader work as usual; `CSV.File` fills its columns on `ntasks` tasks.

### Compiling a reader with `juliac --trim`

No type in a schema read depends on the file, so a program compiled with
`juliac --trim=safe` can use it. Make each schema a constant. A program can
hold several schemas, for example for files with different numbers of numeric
columns, and choose one at run time:

```julia
using CSV, Tables

const POINTS = Tables.Schema((:id, :x, :y), (String, Float64, Float64))
const SAMPLES = Tables.Schema((:id, :a, :b, :c, :d),
                              (Int, Float64, Float64, Float64, Union{Missing, Float64}))

function (@main)(args::Vector{String})::Cint
    total = 0.0
    try
        if args[1] == "points"
            for row in CSV.Rows(args[2]; schema=POINTS)
                total += row.x * row.y
            end
        else
            file = CSV.File(args[2]; schema=SAMPLES)
            total = sum(file.a::Vector{Float64})
        end
    catch err
        err isa CSV.ParseError || rethrow()
        Core.println(err.problem.message)
        return 1
    end
    Core.println(string(total))
    return 0
end
```

```bash
juliac --output-exe reader --project=. --experimental --trim=safe reader.jl
```

In a trimmed program, the source must be a local path, an `IO`, or bytes; a
schema read rejects a URL instead of downloading it. A custom `dateformat`
needs Parsers.jl's format compiler, which `--trim` cannot compile yet: read
ISO dates and times, or read the field as text. `Tables.columntable` compiles
for schemas of up to 100 columns; iterate the rows of a wider schema, or
read it with `CSV.File`. `CSV.File` is not parametric, so a program checks a
column's type before using it, as in `x = file.x::Vector{Float64}` or
`s = file.s::DataStrings.StringVector{Union{Missing, DataString}}`. Julia
1.12 does not compile the tasks that a multithreaded read starts, so on 1.12
run the program with one thread (the default for an executable) or pass
`parallel=false, prefetch=false`; Julia 1.13 runs them.

## Batch iteration

`CSV.Chunks` yields batches, each a `CSV.File`, and implements
`Tables.partitions`. Every batch has the same column types; pooling is decided
per batch.

A batch covers about `chunkbytes` bytes of the input (64 MiB by default) and
ends at a row boundary, so one long row can make it larger. `ntasks` sets the
parallelism within a batch, not the number of batches. `pool` takes one policy
for every column, and `select` and `drop` apply to every batch.

Each batch is indexed when it is produced, and the index is released with it.
The source stays in memory, or memory-mapped, until the iterator is released.
Each batch owns its columns.

To settle one schema, the constructor checks every value it will return before
the first batch; rows after `limit` are not checked. `length(chunks)` is the
number of batches.

## Tables.Scan pushdown

`CSV.File` accepts `scan=Tables.Scan(...)` (Tables.jl 1.14 or later).
Projection, renaming, type overrides, filters,
offsets, and limits are applied inside the parser. Unselected columns are not
sampled or parsed. Rows rejected by the filter do not parse unrelated values.
Filters see native source values first. Offset and limit follow the filter.
Requested output types are converted last, so a filtered-out value cannot
create a conversion problem. A result column without a requested type is
inferred from the returned rows only, so its type can differ from
`Tables.scan(CSV.File(source), request)`, which types the whole file first.

```julia
using CSV, Tables

request = Tables.Scan(
    select=(:id, :amount => Float64),
    filter=Tables.col(:amount) > 100,
    offset=10,
    limit=50,
)
file = CSV.File("orders.csv"; scan=request)
```

A scan owns selection, types, and row bounds, so `scan` cannot be combined
with `select`, `drop`, `types`, or `limit`.
