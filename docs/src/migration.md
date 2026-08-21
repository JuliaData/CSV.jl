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
| Text values | InlineStrings.jl values by default | `CSV.CompactString` by default | Pass `stringtype=String`, or load InlineStrings.jl and select its type |
| Pooling | `(0.2, 500)` default policy | `pool=false` | Pass `pool=(0.2, 500)` to restore the old policy |
| Empty unquoted field | Missing sentinel behavior could be disabled | Always `missing` | Use a quoted empty field for present empty text |
| Problems | Warnings printed during recovery | Structured `CSV.problems(file)` | Inspect problems, or set `on_error=:error` |
| Row limit | Could be approximate with multiple tasks | Exact at every thread count | Remove `ntasks=1` workarounds used only for exact limits |
| Boolean inference | Accepted the 0.10 parser's broader spellings | Exact lowercase `true` and `false` | Add explicit `truestrings` and `falsestrings` as required |

`CSV.CompactString` is an `AbstractString`. Convert one value with `String(x)`
when a consumer requires `String`. Use `stringtype=String` when all text values
must own their bytes.

## Removed, replaced, or preferred reader options

| 0.10 form | 1.0 form |
|:----------|:---------|
| `rows_to_check=n` or `lines_to_check=n` | `nsample=n` for type sampling |
| `threaded=false` | `ntasks=1` or `parallel=false` |
| `tasks=n` | `ntasks=n` |
| `lazystrings=true` | `stringtype=CSV.CompactString`, which is the default |
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

The `PosLenString` output type is retired. Use `CSV.CompactString`, `String`,
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

## Parsers 3 and internal layout

CSV.jl 1.0 depends on Parsers 3 for the reviewed low-level value kernels. The
integration decision is complete. CI temporarily pins
[Parsers.jl PR #210](https://github.com/JuliaData/Parsers.jl/pull/210) at exact
`83c7142fb714cb87261ef38eec7ab103444eb30d` until registration. A registered
Parsers 3 release and removal of this pin are 1.0 tag gates. Registered
InlineStrings releases still require Parsers 2. Development CI pins
[InlineStrings.jl PR #93](https://github.com/JuliaStrings/InlineStrings.jl/pull/93)
at exact `ce4c3549691c4b3443cc14ffa90ebdd6636eff2f`. A compatible InlineStrings
release and removal of this pin are also tag gates.

The final source layout has one runtime module: `CSV`. Implementation files are
includes, not public submodules. Use only the public names documented in the
[API reference](reference.md).

## Tables.Scan transition

CSV.jl 1.0 supports parser pushdown through `Tables.Scan`. The feature requires
a Tables.jl release that defines the scan API. Until that dependency is
registered, development CI must install and test the reviewed Tables.jl commit
explicitly. A final CSV.jl 1.0 tag must set a Tables.jl compatibility bound that
cannot resolve to a release without `Tables.Scan`.

## Maintainer release-readiness checklist

Before the `1.0.0` tag:

- replace the development version with `1.0.0` and verify the package resolves
  from a clean environment;
- depend on a registered Tables.jl release that contains `Tables.Scan`, then
  replace the temporary exact-revision integration lane with that release;
- depend on a registered Parsers 3 release that contains the reviewed kernels,
  then remove the exact PR pin from every CI lane;
- depend on a registered InlineStrings release that supports Parsers 3, then
  remove its exact PR pin from every CI lane;
- verify the one-module source layout and the strict public docs check;
- run the full test matrix, fuzz suite, strict documentation build, downstream
  integration tests, and package evaluation;
- prepare compatibility updates for important reverse dependencies; packages
  bounded to CSV.jl 0.10 will not select 1.0 automatically;
- confirm that the source archive does not include large test-only fixtures;
- review every generated change by hand, as required by the disclosure in the
  repository README;
- verify TagBot, the documentation deploy key, CompatHelper, and Codecov on the
  release branch;
- verify that GitHub private vulnerability reports reach the maintainers named
  in [`SECURITY.md`](https://github.com/JuliaData/CSV.jl/security/policy); and
- prepare release notes that call out the runtime, string, pooling, missing,
  diagnostics, memory, and writer changes on this page.

Do not tag 1.0 while a mandatory integration lane is skipped because an
unreleased API is absent.
