---
name: csv-jl
description: Use and maintain CSV.jl 1.0, including its reader, writer, diagnostics, Tables.jl integration, compatibility checks, and release gates.
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
only on access. Call `CSV.problems(file)` after a recovering read, or pass
`on_error=:error` for fail-fast behavior.

Text columns use `CSV.CompactString` by default. Pass `stringtype=String` when
the result must own each string. Pooling is independent and is off by default.

For repository work, read `AGENTS.md` before editing. Treat `legacy/` and
`test/LegacyCSV/` as immutable comparison sources. Run focused tests while
editing, then the full minimum-version suite and strict documentation build.
The Tables.Scan and Parsers 3 release gates in `AGENTS.md` are mandatory for a
CSV.jl 1.0 tag.
