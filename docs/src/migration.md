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
| Problems | Warnings printed during recovery | Structured `CSV.problems(file)` | Inspect problems, or set `on_error=:error` |
| Row limit | Could be approximate with multiple tasks | Exact at every thread count | Remove `ntasks=1` workarounds used only for exact limits |
| Boolean inference | Accepted the 0.10 parser's broader spellings | Exact lowercase `true` and `false` | Add explicit `truestrings` and `falsestrings` as required |

`DataStrings.DataString` is an `AbstractString`. Convert one value with `String(x)`
when a consumer requires `String`. Use `stringtype=String` when all text values
must own their bytes.

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
policy values, vectors, or dictionaries. Use list forms for `select` and
`drop`; they work on `CSV.File`, `CSV.lazy`, `CSV.Rows`, and `CSV.Chunks` and
return a unique file-ordered column set. Use `Tables.Scan` for a serializable
projection and filter expression.

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
- an unclosed quote adds a problem instead of stopping by default;
- a long row does not widen the schema; extra fields add a problem;
- a `types` vector must match the header width; and
- `validate=true` rejects dictionary keys that do not name an input column.

Set `on_error=:error` for fail-fast behavior. Use `maxproblems` to cap retained
problem objects.

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
| `table |> CSV.write(path)` | `CSV.write(path, table)` |
| one base path with `partition=true` | Still accepted and appends `_1`, `_2`, and so on; a sink vector gives explicit names |
| `transform=(column, value) -> value` | Still accepted; the compatibility path runs sequentially in row-major order |
| `bufsize` | Still accepted; it is the maximum rendered row size, not a whole-file buffer size |
| `header=true` or `header=false` | Still accepted; `writeheader` is the clearer control |

The writer accepts only a one-byte delimiter. `quotestyle=:none` rejects values
that require structural quoting instead of writing ambiguous data.

An empty string is quoted. `missing` uses `missingstring` and is unquoted by
default. This guarantees a read/write distinction between present empty text
and a missing value.

`floatformat` is new and accepts a Printf-style format. Writer output is
deterministic across `ntasks` values.

## Shared data types and released dependencies

CSV now uses Parsers 3, InlineStrings 2, Tables 1.14, DataStrings 1, and
DataDecimals 1. DataStrings and DataDecimals initial registrations are pending.
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

- Wait for DataStrings and DataDecimals registration, remove their temporary
  pins, and verify a fresh registry-only installation.
- Change 1.0.0-DEV only on the final reviewed release commit.
- Run the full platform matrix, lower-bound Julia tests, deterministic fuzz,
  strict documentation, and downstream compatibility tests.
- Run package evaluation and prepare updates for important reverse dependencies;
  packages bounded to CSV 0.10 will not select 1.0 automatically.
- Complete maintainer review and verify release CI, TagBot, documentation, and
  Codecov on the final source commit.

The documentation environment also pins JSON PR #480 at `bcb8e334682e8135c08913781bf8200832cf752e` until a JSON release supports Parsers 3. This is a docs dependency gate, not a CSV runtime dependency.
