# CSV.jl maintenance guide

## Scope

CSV.jl reads and writes delimited text through the Tables.jl interface. The
1.0 code supports Julia 1.10 and later.

Keep the user-facing namespace small. The public names are `CSV.File`,
`CSV.read`, `CSV.lazy`, `CSV.LazyFile`, `CSV.Rows`, `CSV.Chunks`,
`CSV.problems`, `CSV.write`, `CSV.RowWriter`, and `CSV.CompactString`. Do not
export them. Add a public name only when a namespace API is necessary.

## Source layout

- The final source layout has one runtime module: `CSV`. Implementation files
  are includes, not public submodules. Do not add internal submodules.
- `src/core.jl` builds the structural index and typed columns.
- `src/api.jl` implements the reader entry points and Tables.jl interfaces.
- `src/examples.jl` implements shared Tables.jl row and batch adapters.
- `src/write.jl` implements the writer and row iterator.
- `src/compactstring.jl` implements the default text value and column.
- `src/scan.jl` implements Tables.Scan pushdown when that API is available.

CSV.jl now depends on Parsers 3 for the reviewed low-level value kernels. CI
temporarily pins
[Parsers.jl PR #210](https://github.com/JuliaData/Parsers.jl/pull/210) at exact
commit `83c7142fb714cb87261ef38eec7ab103444eb30d`. Registered InlineStrings
releases still require Parsers 2, so CI also pins
[InlineStrings.jl PR #93](https://github.com/JuliaStrings/InlineStrings.jl/pull/93)
at `ce4c3549691c4b3443cc14ffa90ebdd6636eff2f`.

## Validation

Run the full suite from the repository root. Until compatible Parsers and
InlineStrings releases are registered, install both exact revisions in one
operation:

```sh
julia --project=. -e 'using Pkg; Pkg.add([PackageSpec(url="https://github.com/JuliaStrings/InlineStrings.jl.git", rev="ce4c3549691c4b3443cc14ffa90ebdd6636eff2f"), PackageSpec(url="https://github.com/JuliaData/Parsers.jl.git", rev="83c7142fb714cb87261ef38eec7ab103444eb30d")]); Pkg.test()'
```

Use Julia 1.10 for the minimum-version check. Use `--check-bounds=yes` and
multiple threads for parser, writer, or concurrency changes. Keep fuzz tests
deterministic and print the seed in any failure context.

Build documentation strictly:

```sh
julia --project=docs -e 'using Pkg; Pkg.add([PackageSpec(path=pwd()), PackageSpec(url="https://github.com/JuliaData/Parsers.jl.git", rev="83c7142fb714cb87261ef38eec7ab103444eb30d")]); Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

For performance changes, compare the change with the branch baseline on fresh,
seeded data. Use a separate temporary environment if a released-version
comparison is useful. Report the Julia version, platform, thread count, input
shape, time, and allocations. Do not use one selected benchmark as the only
proof.

## Release gates

Do not tag CSV.jl 1.0 until a registered Tables.jl release provides
`Tables.Scan`, a registered Parsers 3 release provides the reviewed low-level
kernel API, and a registered InlineStrings release supports Parsers 3. Remove
all temporary source pins. CSV.jl must resolve those releases through compatible
bounds. All mandatory CI jobs must pass. A human maintainer must review every
generated change.
