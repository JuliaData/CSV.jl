# CSV.jl maintenance guide

CSV.jl reads and writes delimited text through Tables.jl. Keep its public entry
points qualified and unexported. The supported surface is listed in
`docs/src/reference.md`.

## Ownership and layout

One runtime module, CSV, includes the implementation files. `core.jl` owns
structural indexing, quote handling, inference, and column assembly. `api.jl`
owns reader options and source handling. `scan.jl` integrates Tables.Scan.
`write.jl` owns ordered rendering and writer workers.

DataStrings owns string scalars and columns; `strings.jl` contains CSV builder
glue only. Use the trusted column constructor only after CSV proves payload
ranges and missing-value invariants. Retained scalar values must survive column
edits. DataDecimals owns decimal arithmetic and conversion; the
`CSVDataDecimalsExt` extension requests `Parsers.RoundExact` for explicitly
requested decimal types. Extra trailing zeros are exact; discarded nonzero
digits are a parse error. CSV does not infer decimal types.

Parsers.DatePattern is opaque. Retain date/time inference metadata when compiling
a format instead of reading parser storage fields. Use Tables.resolve and the
resolved filter when evaluating projected predicate columns.

## Validation

Dependency setup lives in `test/dependencies.jl`. DataStrings 1, DataDecimals 1,
Parsers 3, InlineStrings 1.4.6 or 2, and Tables 1.14 resolve from General.
Run from the repository root:

```sh
julia --project=test test/dependencies.jl
julia --project=test --check-bounds=yes -t4 test/runtests.jl
julia --project=test test/quality.jl
julia --project=docs test/dependencies.jl
julia --project=docs docs/make.jl
```

Test Julia 1.10 and current Julia. Keep fuzz inputs deterministic. For hot-path
changes, compare time and allocations with the previous commit on fresh seeded
inputs, including eager reads, Rows, pooling, and writing. Verify task budgets
and source-ordered diagnostics for multi-file and parallel changes.

## Release boundary

Keep version 1.0.0-DEV until the final release is authorized. Before tagging,
verify clean registry resolution, run all CI and downstream checks, and update
migration and release notes. Preserve unrelated source checkouts.

The documentation environment requires JSON 1.8 or later for Parsers 3 compatibility.
