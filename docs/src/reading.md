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

`CSV.File`, `CSV.read`, and `CSV.Chunks` return columns that own their bytes:
text longer than a `DataStrings.DataString` inline payload is copied into
column-owned buffers during parsing, so a finished table never refers to the
source. Finished worker tasks release their input references, so garbage
collection can unmap the file. Rewriting the file afterwards cannot affect
the table. One value retains at most the parse chunk's text buffer, and a
`Vector{UInt8}` input is never aliased. A `CSV.Chunks` iterator retains its
source for later batches; the returned batches own their bytes. `CSV.Rows` and `CSV.lazy`
are the exceptions: their cells are views into the retained source, so keep
the source unchanged while you use them, or convert values with `String`.

## Headers and row windows

`header=1` reads names from the first row. Other accepted forms are:

- an integer row number;
- `false` or `0` to generate `Column1`, `Column2`, and so on;
- a vector of names; or
- an increasing vector of row numbers whose values are joined into names.

Duplicate names are made unique. `normalizenames=true` converts names to valid
Julia identifiers.

Use `skipto` for the first data row, `footerskip` to omit rows at the end, and
`limit` to cap the returned data rows. `limit` is exact at every thread count.
Comment rows do not count toward `footerskip`. Empty rows and comment rows keep
their physical positions for header and `skipto` handling.

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

A quote that does not start its field is content:
`10,Pipe 3" long` and `1,x"y` are ordinary two-field rows. The parallel
structural scan treats every quote as a field boundary; when it meets a quote
that does not start its field, CSV.jl rebuilds the index with a serial pass
that applies the field-start rule. Well-formed input never takes that path. A
quote that does start a field and is never closed is still malformed input and
is reported as an `:unclosed_quote` problem.

## Missing values and value syntax

An unquoted empty field is always `missing` in CSV.jl 1.0. `missingstring`
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
reordered. Function-valued selection is not supported. Use a `Tables.Scan`
request for expression-based projection and filtering.

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
InlineStrings 2 is supported.

Pooling is independent of `stringtype`. `pool=false` is the 1.0 default. The
accepted forms are:

- `pool=true` to pool every text column;
- a ratio such as `pool=0.2`;
- `(ratio, maximum_levels)` such as `pool=(0.2, 500)`; or
- a dictionary or vector of per-column policies for `CSV.File`.

The ratio-and-cap policy is available as `pool=(0.2, 500)`. Pooled output
uses PooledArrays.jl. Pool levels own their strings even when
`stringtype=DataStrings.DataString`.

With `transpose=true`, input rows become output columns. `types`, per-output
`dateformat` dictionaries, `stringtype`, and `pool` use those output column
names. `select` and `drop` are not supported in transpose mode.
Transpose mode indexes the input and parses its output columns in parallel,
like the row-wise readers; `ntasks` and `parallel` apply to it as well.

## Parse problems

Every eager reader keeps rows and records malformed quotes, invalid typed
values, long rows, and other parse problems as `CSV.Problem` values. Inspect
the retained items with `CSV.problems(file)`. Each item contains `row`, `col`,
`pos`, `kind`, and `message` fields. The default `on_error=:warn` also prints
one summary warning per read, so a problem is never silent; `on_error=:collect`
records problems without the warning.

```@example reading-problems
using CSV

file = CSV.File(IOBuffer("count\n1\ninvalid\n"); types=Int, on_error=:collect)
[(p.row, p.col, p.kind) for p in CSV.problems(file)]
```

`maxproblems` caps retained diagnostics and defaults to 10,000.
`CSV.Chunks` warns once, for its first batch with problems; every batch still
carries its own `CSV.problems(batch)`. `on_error=:error`, or the compatibility
shorthand `strict=true`, throws a `CSV.ParseError` at the first problem in
source order;
the exception carries that `problem`, the total `nproblems`, and the `source`
label. `validate=false` ignores `types`, `dateformat`, and `pool` dictionary
keys that do not match an input column; validation is on by default.

## Parallel parsing and sampling

Parsing is parallel by default when Julia has multiple threads. Use
`ntasks=1` or `parallel=false` for one task. `ntasks=N` bounds parsing to at
most `N` worker tasks. A direct `CSV.File` targets about four structural
chunks per task, each between 64 KiB and 1 MiB, unless `chunkbytes` is
explicit. `CSV.File(lazyfile)`
keeps the existing index geometry but applies the same worker bound.
`chunkbytes` directly controls the target structural-index chunk size. Keep
it at the default unless a measurement says otherwise: each parsing pass
walks one chunk at a time, so a chunk that fits in cache keeps the value
loops fast, and a `chunkbytes` of several MiB or more slows wide files with
many columns.

Type inference samples indexed rows. `nsample` controls its row sample and
`samplebytes` controls the delimiter-detection sample. Parallel and
single-task parses have the same row order and exact row limit.

## Multiple sources

Pass a vector of sources to concatenate them vertically:

```@example reading-multiple
using CSV

sources = [IOBuffer("id,value\n1,10\n"), IOBuffer("value,id\n20,2\n")]
file = CSV.File(sources; source=:origin => ["first", "second"], stringtype=String)

(collect(file.id), collect(file.value), collect(file.origin))
```

The first source defines the output column set. Later sources match columns by
name. A missing column is filled with `missing`; an extra column is ignored.
Types promote across sources. Text columns concatenate as `DataString` columns
that own their bytes, as in a single-source read.

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

`CSV.lazy` builds the structural index but does not eagerly type and parse all
columns. Its `CSV.LazyFile` result supports `lazyfile.column`,
`lazyfile[:column]`, `lazyfile[row, column]`, and the Tables.jl column
interface.

Ordinary lazy text cells are zero-copy views into the retained source. If a
long cell starts beyond the compact view format's Int32 source-offset limit,
CSV copies only that cell into a bounded backing buffer when it is accessed.

```@example reading-lazy
using CSV

lazyfile = CSV.lazy(IOBuffer("id,price\n1,3.5\n2,4.0\n");
                    types=Dict(:price => Float64))
(String(lazyfile.id[2]), lazyfile[1, :price])
```

`CSV.File(lazyfile)` performs the full parse without repeating the structural
scan. Row-position, dialect, and input-buffer options were fixed when the lazy
file was created. Selection is sticky: an eager parse starts from the columns
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

`reusebuffer` is accepted but has no effect. The
row view does not allocate a reusable per-row buffer. Because the structural
index is complete before iteration, `length(rows)` and `names(rows)` are
available, and consumers such as `Tables.columntable` can preallocate.

An invalid or malformed cell becomes `missing` when it is accessed. Pass
`strict=true` or `on_error=:error` to throw a `CSV.ParseError` at that access
instead. `CSV.Rows` does not retain a problem log and does not accept
`maxproblems` or `maxwarnings`. It rejects `on_error=:warn` because it cannot
produce a read summary. Use `CSV.File` when you need `CSV.problems(file)`
or a summary warning.
List `select` and `drop` forms are supported and use the same stable file-order
semantics as `CSV.File`.

## Batch iteration

`CSV.Chunks` yields Tables.jl-compatible batches and implements
`Tables.partitions`. It infers one schema for the full row window, so each
batch has the same column types. Pooling is evaluated separately in each
batch.

`ntasks` sets the target batch count. Pass `chunkbytes` for direct size
control. A `CSV.Chunks` pool policy must be one `Bool`, ratio, or
`(ratio, maximum_levels)` value; per-column pool dictionaries and vectors are
only supported by `CSV.File`.
List `select` and `drop` forms project the same file-ordered column set in every
batch.

## Tables.Scan pushdown

CSV.jl 1.0 accepts `scan=Tables.Scan(...)` (Tables.jl 1.14 or later).
Projection, renaming, type overrides, filters,
offsets, and limits are applied inside the parser. Unselected columns are not
sampled or parsed. Rows rejected by the filter do not parse unrelated values.
Filters see native source values first. Offset and limit follow the filter.
Requested output types are converted last, so a filtered-out value cannot
create a conversion problem.

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

A scan owns selection, types, and row bounds. Do not combine `scan` with
`select`, `drop`, `types`, or `limit`. Tables 1.14 is the minimum dependency.
