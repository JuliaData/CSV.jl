---
name: csv-jl
description: Use and maintain CSV.jl 1.0, including its reader, writer, diagnostics, Tables.jl integration, compatibility checks, and release gates.
---

# Using CSV.jl

Load CSV.jl and call its APIs through the package namespace. The package does
not export its entry points.

```julia
using CSV, DataStrings

file = CSV.File("input.csv")
CSV.write("output.csv", file)
```

Use `CSV.read(source, sink)` for a Tables.jl sink, `CSV.Rows` for row access,
`CSV.Chunks` for stable-schema batches, and `CSV.lazy` when values should parse
only on access. Call `CSV.problems(file)` after a recovering read, or pass
`on_error=:error` for fail-fast behavior.

Text columns use `DataStrings.DataString` by default. Pass `stringtype=String` when
the result must own each string. Pooling is independent and is off by default.

For repository work, read `AGENTS.md` before editing. Run focused tests while
editing, then the full minimum-version suite and strict documentation build.
Use registered Parsers 3, InlineStrings 2, and Tables 1.14. Pending DataStrings
and DataDecimals revisions are centralized in `test/dependencies.jl`; remove
those pins only after General registration. Keep the runtime in one module and
validate registry resolution before a 1.0 tag.

For exact numeric columns, supply `types=Dict(:amount => DataDecimals.Decimal64{2})`.
Use `inferdecimal=true` to opt into full-column consistent-scale inference for
File and Chunks. It is a formatting heuristic, not currency detection. See
`docs/src/decimals.md` for its exactness and fallback rules.

The documentation environment also pins JSON PR #480 at `bcb8e334682e8135c08913781bf8200832cf752e` until a JSON release supports Parsers 3. This is a docs dependency gate, not a CSV runtime dependency.
