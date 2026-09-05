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
edits. DataDecimals owns decimal arithmetic and conversion; `decimals.jl` owns
CSV's exactness and inference policy. Infer scale from field bytes before any
rounding or Float64 conversion. Full-column decimal profiling must respect
selection, row windows, and filter masks.

Parsers.DatePattern is opaque. Retain date/time inference metadata when compiling
a format instead of reading parser storage fields. Use Tables.resolve and the
resolved filter when evaluating projected predicate columns.

## Validation

Dependency setup lives in `test/dependencies.jl`. It pins only the two pending
new package registrations. Parsers 3, InlineStrings 2, and Tables 1.14 resolve
from General. Run from the repository root:

```sh
julia --project=test test/dependencies.jl
julia --project=test --check-bounds=yes -t4 test/runtests.jl
julia --project=test test/quality.jl
julia --project=docs test/dependencies.jl
julia --project=docs docs/make.jl
```

Test Julia 1.10 and current Julia. Keep fuzz inputs deterministic. For hot-path
changes, compare time and allocations with the original PR head on fresh seeded
inputs, including eager reads, Rows, pooling, and writing. Verify task budgets
and source-ordered diagnostics for multi-file and parallel changes.

## Release boundary

Keep version 1.0.0-DEV until the final release is authorized. Before tagging,
remove the DataStrings/DataDecimals source pins after registration, verify clean
registry resolution, run all CI and downstream checks, and update migration and
release notes. Preserve unrelated source checkouts. Keep the existing PR draft
status while human review and release gates remain open.

The documentation environment also pins JSON PR #480 at `bcb8e334682e8135c08913781bf8200832cf752e` until a JSON release supports Parsers 3. This is a docs dependency gate, not a CSV runtime dependency.
