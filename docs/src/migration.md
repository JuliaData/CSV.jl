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

## Removed, replaced, or preferred reader options

| 0.10 form | 1.0 form |
|:----------|:---------|
| `rows_to_check=n` or `lines_to_check=n` | `nsample=n` for type sampling |
| `threaded=false` | `ntasks=1` or `parallel=false` |
| `tasks=n` | `ntasks=n` |
| `lazystrings=true` | `stringtype=DataStrings.DataString`, which is the default |
| `silencewarnings=true` | Do not inspect `CSV.problems(file)`, or set `maxproblems=0` |
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

Use property access such as `file.amount`, `Tables.getcolumn(file, :amount)`,
or `Tables.columns(file)`. Use `names(file)` or `Tables.columnnames(file)` for
column names. The 0.10 access forms `file[:amount]`, `file["amount"]`, and
`file.names` remain supported. A data column takes priority when it has the
compatibility-property name `names`. `file[i]` returns row `i`.

`CSV.lazy` is new. Its `CSV.LazyFile` supports `lazyfile[:amount]` and indexed
cell access because it is a column-indexed view, not an iterable row table.

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

`CSV.Rows`, `CSV.Chunks`, and `CSV.lazy` retain the source bytes and a complete
structural index. They reduce value or column materialization. They do not
provide unbounded network-stream processing.

## Multiple sources

A vector of sources now promotes compatible column types across inputs and
uses deterministic labels for non-path sources. The first source defines the
output columns. Later missing columns are filled with `missing`; later extra
columns are ignored. Concatenated text columns materialize as `String`.

`source=:origin` also works with a one-element source vector. A source-label
name that conflicts with a data column is an error.

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

## Shared data types and released dependencies

CSV now uses Parsers 3, InlineStrings 2, Tables 1.14, DataStrings 1,
DataDecimals 1, and Durations 1.1, all registered in General. Durations
provides `Timestamp{P}`, the inferred date-time type. It uses `Dates.Timestamp`
when the standard library provides that type, and a compatible implementation otherwise.
The draft rewrite's `CSV.CompactString` has moved to `DataStrings.DataString`.
Import DataStrings when referring to that type. Text columns are mutable
`DataStrings.StringVector` values. Shared string methods belong in DataStrings.

Explicit decimal schemas reject values that need rounding. Extra trailing zeros
are exact and accepted. Recoverable failures use the normal missing/problem
policy; `strict=true` throws. [Decimal columns](decimals.md) describes opt-in
inference. Ordinary Float64 inference stays unchanged.

Tables.Scan now resolves through Tables 1.14 in every CI job. CSV retains format
metadata independently of the opaque Parsers.DatePattern handle.

## Maintainer release-readiness checklist

Before the 1.0.0 tag:

- Verify a fresh registry-only installation resolves every dependency.
- Change 1.0.0-DEV only on the final reviewed release commit.
- Run the full platform matrix, lower-bound Julia tests, deterministic fuzz,
  strict documentation, and downstream compatibility tests.
- Run package evaluation and prepare updates for important reverse dependencies;
  packages bounded to CSV 0.10 will not select 1.0 automatically.
- Complete maintainer review and verify release CI, TagBot, documentation, and
  Codecov on the final source commit.
