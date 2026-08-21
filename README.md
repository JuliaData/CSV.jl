# CSV.jl

[![CI](https://github.com/JuliaData/CSV.jl/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/JuliaData/CSV.jl/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/JuliaData/CSV.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/JuliaData/CSV.jl)
[![version](https://juliahub.com/docs/CSV/version.svg)](https://juliahub.com/ui/Packages/CSV/HHBkp)
[![pkgeval](https://juliahub.com/docs/CSV/pkgeval.svg)](https://juliahub.com/ui/Packages/CSV/HHBkp)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8004128.svg)](https://doi.org/10.5281/zenodo.8004128)

CSV.jl reads and writes comma-separated and other delimited text data in Julia.
It implements the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface.

## Installation

Install the registered release from the Julia REPL:

```julia
] add CSV
```

The upcoming CSV.jl 1.0 release requires Julia 1.10 or later. See the
[0.10 to 1.0 migration guide](docs/src/migration.md) before you update an
existing application.

## Quick start

```julia
using CSV

file = CSV.File("input.csv")
CSV.write("output.csv", file)
```

Use `CSV.read("input.csv", DataFrame)` after you load DataFrames.jl. Use
`CSV.Rows` to process one row at a time, `CSV.Chunks` to process batches, or
`CSV.lazy` to index a source before you choose which cells to parse.

## Documentation

- [Stable documentation](https://JuliaData.github.io/CSV.jl/stable) describes
  the latest registered release.
- [Development documentation](https://JuliaData.github.io/CSV.jl/latest)
  describes the `main` branch.
- [Draft 1.0 release notes](docs/src/release-notes.md) summarize the rewrite.

## Project status

CSV.jl 1.0 CI tests Julia 1.10, the current stable release, and Julia nightly.
It also tests Linux, macOS, Windows, x86-64, 32-bit x86, and Apple silicon.

## Support

Use [GitHub Issues](https://github.com/JuliaData/CSV.jl/issues) for bug reports,
feature requests, and questions.

## Generative-AI contribution disclosure

The CSV.jl 1.0 internal rewrite contains substantial generative-AI
contributions. Claude drove the initial rewrite. Codex performed review,
fixes, documentation, and validation. Human maintainers must hand-review these
changes and own the final approval.

## Alternatives

- [DelimitedFiles](https://docs.julialang.org/en/v1/stdlib/DelimitedFiles/)
  is a Julia standard library for simple, homogeneous delimited matrices.
- [CSVFiles.jl](https://github.com/queryverse/CSVFiles.jl) provides FileIO.jl
  `load` and `save` integration.
- [DLMReader.jl](https://github.com/sl-solution/DLMReader.jl) reads delimited
  data and integrates with InMemoryDatasets.jl.
