# Migrating from 0.10 to 1.0

CSV.jl 1.0 replaces the parser and writer internals. It keeps the main
`CSV.File`, `CSV.read`, `CSV.Rows`, `CSV.Chunks`, `CSV.write`, and
`CSV.RowWriter` entry points. This page lists changes that can affect an
application.

## Runtime requirement

CSV.jl 1.0 requires Julia 1.10 or later. Keep CSV.jl 0.10 in environments that
must use an older Julia release.

## Reader defaults

| Area | CSV.jl 0.10 | CSV.jl 1.0 | Migration |
|:-----|:------------|:-----------|:----------|
| Text values | InlineStrings.jl values by default | `DataStrings.DataString` by default | Pass `stringtype=String`, or load InlineStrings.jl and select its type |
| Pooling | `(0.2, 500)` default policy | `pool=false` | Pass `pool=(0.2, 500)` to restore the old policy |
| Empty fields | `,,` and `,"",` read as `missing`; `missingstring=nothing` read both as empty strings | `,,` is always `missing`; `,"",` is an empty string | Quote a field to keep an empty string |
| Problems | One warning per problem during recovery | Structured `CSV.problems(file)` plus one summary warning | Inspect problems, set `on_error=:collect` to silence the summary, or `on_error=:error` to throw `CSV.ParseError` |
| Row limit | Could be approximate with multiple tasks | Exact at every thread count | Remove `ntasks=1` workarounds used only for exact limits |
| `footerskip` | Counted comment rows | Does not count comment rows | Lower `footerskip` by the number of trailing comment rows |
| Boolean values | Also inferred `T` and `F`, and read `1` and `0` for `types=Bool` | Only `true`, `True`, `TRUE`, `false`, `False`, and `FALSE` | Pass `truestrings` and `falsestrings` lists for other spellings |
| Date-time inference | `Dates.DateTime` for `yyyy-mm-ddTHH:MM:SS` with up to three fraction digits; other date-times stayed text | `Timestamp{Nanosecond}` (Durations.jl; `Dates.Timestamp` when available) for `T` or space separators and up to nine fraction digits; wider dates use `Timestamp{Microsecond}` only if every value fits exactly, otherwise text | Pass `typemap=Dict(Timestamp{Nanosecond} => DateTime)` for `DateTime` columns; as in 0.10, a column with finer fractions stays text. With `types=DateTime`, finer fractions are problems |

`DataStrings.DataString` is an `AbstractString`. Convert one value with `String(x)`
when a consumer requires `String`. Eager text columns own their bytes, so
`stringtype=String` is a choice of element type, not a safety measure. An
explicit `types=String`, alone or per column, still returns `String` columns
as it did in 0.10; `stringtype` applies to inferred text only.

CSV does not infer decimal types; fractional numbers infer as `Float64`. With
DataDecimals loaded, an explicitly requested type such as
`types=Dict(:amount => DataDecimals.Decimal64{2})` parses exactly from the
field bytes: a value that would need rounding is a problem (`strict=true`
throws), and extra trailing zeros are exact.

## Removed, replaced, or preferred reader options

A removed keyword throws an `ArgumentError` that names its replacement.

| 0.10 form | 1.0 form |
|:----------|:---------|
| `rows_to_check=n` or `lines_to_check=n` | `nsample=n` for type sampling |
| `threaded=false` | `ntasks=1` or `parallel=false` |
| `tasks=n` | `ntasks=n` |
| `lazystrings=true` | `stringtype=DataStrings.DataString`, which is the default |
| `silencewarnings=true` | `on_error=:collect` records problems without a summary warning |
| `maxwarnings=n` | `maxproblems=n`; `CSV.File` and `CSV.read` still accept `maxwarnings` |
| `strict=true` | Still accepted; `on_error=:error` is the explicit form. For `CSV.Rows`, either form throws when an invalid typed cell is accessed |
| `type=T` | `types=T` |
| `datarow=n` | `skipto=n` |
| `ignoreemptylines=b` | `ignoreemptyrows=b` |
| `missingstrings=values` | `missingstring=values` |
| `dateformats=formats` | `dateformat=formats` |
| `debug=true` | Removed; inspect structured problems |
| `parsingdebug=true` | Removed; inspect structured problems |
| `CSV.detect(...)` | Removed; use `delim=nothing` on a reader |

The `PosLenString` output type is retired. Use `DataStrings.DataString`, `String`,
or an InlineStrings.jl type.

`types` and `pool` no longer accept functions. Use type or policy values,
vectors, or dictionaries. `select` and `drop` take a list, one name or index, a
`Regex`, or the 0.10 `(i, name) -> Bool` function; they work on `CSV.File`,
`CSV.lazy`, `CSV.Rows`, and `CSV.Chunks` and return a unique file-ordered
column set. Use `Tables.Scan` for a serializable projection and filter
expression.

## Table access

Column and row access on `CSV.File` is unchanged. `CSV.lazy` is new: it
indexes the input and returns a `CSV.LazyFile` whose cells parse when they are
accessed. It supports `lazyfile[:amount]` and `lazyfile[row, :amount]` but is
not an iterable row table; `CSV.File(lazyfile)` parses it fully without
indexing the input again.

## Error and schema behavior

The default recovery mode returns a table and records problems. Important 1.0
rules are:

- a field that fails an explicitly requested type becomes `missing` and adds a
  problem;
- a quote inside a field (`5' 11"`) is content, as in 0.10;
- an unclosed quote adds a problem instead of stopping by default;
- a long row does not widen the schema; extra fields add a problem;
- a `types` vector must match the header width; and
- `validate=true` rejects dictionary keys that do not name an input column.

The default `on_error=:warn` prints one summary warning per read (once per
`CSV.Chunks`). Set `on_error=:collect` to record problems silently, or
`on_error=:error` to throw a `CSV.ParseError` that carries the first problem in
source order. Use `maxproblems` to cap retained problem objects.

Since 1.3, passing `schema=Tables.Schema(names, types)` to `CSV.Rows`,
`CSV.File`, or `CSV.read` opts into strict schema validation. These reads
always throw on a mismatch, and schema rows are typed `NamedTuple` values.
Existing calls need no change. See [Reading with a known schema](@ref) before
replacing `types` with `schema`; their error and row-access behavior differs.

`types=Char` and `types=Symbol` remain supported. A `Char` cell is exactly one
Unicode scalar; other text follows the normal missing-plus-problem policy.

## Source and memory behavior

CSV.jl 1.0 memory-maps large regular local files. It fully buffers small local
files, `IO` and `Cmd` sources, URLs, and gzip-decompressed data. This differs
from 0.10 paths that could use temporary files for these inputs. Account for
the full uncompressed size when a gzip file or non-file stream is large.

`CSV.Rows` no longer streams with minimal memory: 0.10 buffered only the
current row, while 1.0 keeps the source and an index of every row, about 4
bytes per field plus 8 bytes per row. `CSV.lazy` keeps the same index.
`CSV.Chunks` keeps one batch's index, so use it when an index of the whole
file would not fit in memory.

## Multiple sources

The first source defines the output columns, and later sources match them by
name. A column missing from a later source is filled with `missing` (0.10
threw an error); extra columns are ignored, as before. Column types combine
with `promote_type`. Non-path sources get labels such as `"<source 1>"`. Text
columns concatenate as `DataStrings.DataString` columns, as in a single-source
read; an explicitly requested string type such as `types=String` or
`stringtype=String15` applies to the concatenated column.

`source=:origin` also works with a one-element source vector. A source-label
name that conflicts with a data column is an error.

## Tables.Scan

`Tables.Scan` is new. A scan describes the columns to return (optionally
renamed or converted), a row filter, and an offset and limit.
`CSV.File(source; scan=request)` and `CSV.read` apply it inside the parser, so
a filtered read no longer parses the whole table. See
[Tables.Scan pushdown](@ref).

```julia
using CSV, Tables

# three columns out of many, one renamed and one converted
file = CSV.File("orders.csv"; scan=Tables.Scan(select=(:id, :region => :where, :amount => Float32)))

# rows that pass a filter, then a page of them
request = Tables.Scan(
    filter=(Tables.col(:amount) > 100) & Tables.colin(Tables.col(:region), ("east", "west")),
    offset=1000,
    limit=50,
)
page = CSV.File("orders.csv"; scan=request)
```

A scan cannot be combined with `select`, `drop`, `types`, or `limit`.

## Writer changes and compatibility

| 0.10 form | 1.0 form |
|:----------|:---------|
| `compress=true` | Still accepted; `compress=:gzip` is the explicit form |
| `compress=false` | Still accepted; `compress=:none` is the explicit form |
| explicit compression choice for `.gz` | `compress=:auto` detects the path suffix by default |
| `quotestrings=true` | Still accepted; `quotestyle=:all` is the explicit form |
| ordinary conditional quoting | `quotestyle=:minimal`, the default |
| `table |> CSV.write(path)` | Still accepted; `CSV.write(path, table)` is the direct form |
| `delim="::"` (multi-character) | Still accepted on both the reader and the writer |
| `CSV.write(io, table)` seeking a seekable `IO` to its start | An `IO` is written at its current position, like `Base.write` |
| one base path with `partition=true` | Still accepted and appends `_1`, `_2`, and so on; a sink vector gives explicit names |
| `transform=(column, value) -> value` | Still accepted; the compatibility path runs sequentially in row-major order |
| `bufsize` | Still accepted; it is the maximum rendered row size, not a whole-file buffer size |
| `header=true` or `header=false` | Still accepted; `writeheader` is the clearer control |

`quotestyle=:none` rejects values that require structural quoting instead of
writing ambiguous data. Unknown writer keywords are an `ArgumentError`.
`CSV.write(sink, CSV.Chunks(source))` streams every batch under one header.

An empty string is quoted. `missing` uses `missingstring` and is unquoted by
default, so empty text and `missing` read back differently; 0.10 wrote both
as an empty field.

`floatformat` is new and accepts a Printf-style format. Writer output is
deterministic across `ntasks` values.
