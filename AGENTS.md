# CSV.jl maintenance guide

## Scope

CSV.jl reads and writes delimited text through the Tables.jl interface. The
1.0 code supports Julia 1.10 and later.

Keep the user-facing namespace small. The public names are `CSV.File`,
`CSV.read`, `CSV.lazy`, `CSV.LazyFile`, `CSV.Rows`, `CSV.Chunks`,
`CSV.problems`, `CSV.write`, `CSV.RowWriter`, and `CSV.CompactString`. Do not
export them. Add a public name only when a namespace API is necessary.

## Source layout

- `src/core.jl` builds the structural index and typed columns.
- `src/api.jl` implements the reader entry points and Tables.jl interfaces.
- `src/write.jl` implements the writer and row iterator.
- `src/compactstring.jl` implements the default text value and column.
- `src/scan.jl` implements Tables.Scan pushdown when that API is available.
- `src/values.jl` is the temporary value-kernel source shared with the Parsers
  3.0 work. Replace it with a registered Parsers dependency before CSV.jl 1.0.

Do not edit `legacy/` or `test/LegacyCSV/` to make a comparison pass. They are
the frozen CSV.jl 0.10 oracle. Fix the new implementation, or record and
document an intentional 1.0 difference in the differential harness.

## Validation

Run the full suite from the repository root:

```sh
julia --project=. -e 'using Pkg; Pkg.test()'
```

Use Julia 1.10 for the minimum-version check. Use `--check-bounds=yes` and
multiple threads for parser, writer, or concurrency changes. Keep fuzz tests
deterministic and print the seed in any failure context.

Build documentation strictly:

```sh
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

For performance changes, compare with the frozen 0.10 implementation on fresh,
seeded data. Report Julia version, platform, thread count, input shape, time,
and allocations. Do not accept a curated benchmark alone as proof.

## Release gates

Do not tag CSV.jl 1.0 until a registered Tables.jl release provides
`Tables.Scan`, a registered Parsers 3 release provides the reviewed low-level
kernel API, CSV.jl uses those releases through compatible bounds, all mandatory
CI jobs pass, and a human maintainer has reviewed every generated change.
