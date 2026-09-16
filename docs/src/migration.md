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
| Empty unquoted field | Missing sentinel behavior could be disabled | Always `missing` | Use a quoted empty field for present empty text |
| Problems | One warning per problem during recovery | Structured `CSV.problems(file)` plus one summary warning | Inspect problems, set `on_error=:collect` to silence the summary, or `on_error=:error` to throw `CSV.ParseError` |
| Row limit | Could be approximate with multiple tasks | Exact at every thread count | Remove `ntasks=1` workarounds used only for exact limits |
| Boolean inference | Accepted the 0.10 parser's broader spellings | `true`, `True`, `TRUE`, `false`, `False`, `FALSE` | Add explicit `truestrings` and `falsestrings` as required |
| Date-time inference | `Dates.DateTime`; extra fraction digits truncated | `Timestamp{Nanosecond}` (Durations.jl; `Dates.Timestamp` when available); wider dates use `Timestamp{Microsecond}` only if every value fits exactly, otherwise text | Pass `types=DateTime` for the old type; finer fractions are problems. With `typemap=Dict(Timestamp{Nanosecond} => DateTime)`, finer fractions promote to text |

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

| 0.10 form | 1.0 form |
|:----------|:---------|
| `rows_to_check=n` or `lines_to_check=n` | `nsample=n` for type sampling |
| `threaded=false` | `ntasks=1` or `parallel=false` |
| `tasks=n` | `ntasks=n` |
| `lazystrings=true` | `stringtype=DataStrings.DataString`, which is the default |
| `silencewarnings=true` | `on_error=:collect` records problems without a summary warning |
| `maxwarnings=n` | Still accepted by problem-retaining readers; `maxproblems=n` is the explicit form |
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

`types`, `select`, `drop`, and `pool` no longer accept functions. Use type or
policy values, vectors, or dictionaries. `select` and `drop` take a list, one
name or index, or a `Regex`; they work on `CSV.File`, `CSV.lazy`, `CSV.Rows`,
and `CSV.Chunks` and return a unique file-ordered column set. Use
`Tables.Scan` for a serializable projection and filter expression.

## Table access

Column and row access on `CSV.File` is unchanged. `CSV.lazy` is new: it builds
the structural index and returns a `CSV.LazyFile` whose cells parse when they
are accessed. It supports `lazyfile[:amount]` and indexed cell access because
it is a column-indexed view, not an iterable row table; `CSV.File(lazyfile)`
performs the full typed parse without repeating the structural scan.

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
`on_error=:error` for fail-fast behavior; the latter throws `CSV.ParseError`,
which carries the source-earliest `CSV.Problem`. Use `maxproblems` to cap
retained problem objects.

`types=Char` and `types=Symbol` remain supported. A `Char` cell is exactly one
Unicode scalar; other text follows the normal missing-plus-problem policy. A
`Symbol` column is parsed as text and converted once after parsing.

## Source and memory behavior

CSV.jl 1.0 memory-maps large regular local files. It fully buffers small local
files, `IO` and `Cmd` sources, URLs, and gzip-decompressed data. This differs
from 0.10 paths that could use temporary files for these inputs. Account for
the full uncompressed size when a gzip file or non-file stream is large.

`CSV.Rows` and `CSV.lazy` retain the source bytes and a complete structural
index. `CSV.Chunks` retains the source bytes and one batch's index, and defines
no `length`: its batch count is only known once every window is read. They
reduce value or column materialization. They do not provide unbounded
network-stream processing.

## Multiple sources

A vector of sources now promotes compatible column types across inputs and
uses deterministic labels for non-path sources. The first source defines the
output columns. Later missing columns are filled with `missing`; later extra
columns are ignored. Text columns concatenate as `DataStrings.DataString`
columns, as in a single-source read; an explicitly requested string type such
as `types=String` or `stringtype=String15` applies to the concatenated column.

`source=:origin` also works with a one-element source vector. A source-label
name that conflicts with a data column is an error.

## Tables.Scan

`Tables.Scan` is new. A scan is a single request object that describes which
columns to return (optionally renamed or converted), a row filter, and an
offset and limit. `CSV.File(source; scan=request)` and `CSV.read` apply the
whole request inside the parser: unselected columns are never sampled or
parsed, rows the filter rejects never parse their other columns, and the
requested output types convert last, so a filtered-out value cannot raise a
conversion problem. Filters run on the parsed native values.

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

A scan owns selection, types, and row bounds, so it is not combined with
`select`, `drop`, `types`, or `limit`. In 0.10 the same work needed a full
read followed by a filter; the scan reads less and returns the same table.

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
default. This guarantees a read/write distinction between present empty text
and a missing value.

`floatformat` is new and accepts a Printf-style format. Writer output is
deterministic across `ntasks` values.
