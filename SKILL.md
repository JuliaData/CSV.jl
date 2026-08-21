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

For repository work, read `AGENTS.md` before editing. Run focused tests while
editing, then the full minimum-version suite and strict documentation build.
The Parsers 3 integration decision is complete. CSV.jl depends on Parsers 3 for
the reviewed low-level kernels. CI temporarily pins
[Parsers.jl PR #210](https://github.com/JuliaData/Parsers.jl/pull/210) at exact
commit `83c7142fb714cb87261ef38eec7ab103444eb30d` until registration. Do not restore
a local copy of those kernels. Registered InlineStrings releases still require
Parsers 2. CI pins
[InlineStrings.jl PR #93](https://github.com/JuliaStrings/InlineStrings.jl/pull/93)
at `ce4c3549691c4b3443cc14ffa90ebdd6636eff2f` until a compatible release exists.

The final runtime has one module: `CSV`. Implementation files are includes, not
public submodules. The Tables.Scan gate, a registered Parsers 3 release, and
a Parsers-3-compatible InlineStrings release are mandatory for a CSV.jl 1.0
tag. Remove both temporary source pins before the tag.
