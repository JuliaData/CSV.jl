---
name: csv-jl
description: Use and maintain CSV.jl 1.0, including its reader, writer, diagnostics, Tables.jl integration, compatibility checks, and release checks.
---

# Using CSV.jl

Load CSV.jl and call its APIs through the package namespace. The package does
not export its entry points.

```julia
using CSV

file = CSV.File("input.csv")
CSV.write("output.csv", file)
```

Use `CSV.read(source, sink)` for a Tables.jl sink, `CSV.Rows` for row access,
`CSV.Chunks` for stable-schema batches, and `CSV.lazy` when values should parse
only on access. Call `CSV.problems(file)` after a recovering read, pass
`on_error=:warn` for one summary warning, or `on_error=:error` for fail-fast
behavior (`CSV.ParseError`).

Text columns use `DataStrings.DataString` by default. Pass `stringtype=String` when
the result must own each string. Pooling is independent and is off by default.

For repository work, read `AGENTS.md` before editing.

For exact numeric columns, load DataDecimals and supply
`types=Dict(:amount => DataDecimals.Decimal64{2})`. CSV does not infer decimal
types.
