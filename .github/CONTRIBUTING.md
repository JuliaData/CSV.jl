# Contributing to CSV.jl

Thank you for helping CSV.jl. Use Julia 1.10 or later for CSV.jl 1.0 work.

## Before you change code

Open an issue for a large API or behavior change. State the compatibility goal
and the data shapes that the change affects. Preserve the small public
namespace. Most implementation names should remain available only through the
`CSV` namespace or stay internal.

Do not add a performance workaround without a root-cause explanation. Add a
focused regression test for each bug fix.

## Run tests

From the repository root, run:

```sh
julia --project=. -e 'using Pkg; Pkg.test()'
```

The normal suite includes deterministic fuzz cases and the frozen CSV.jl 0.10
comparison corpus. The Tables.Scan tests require a Tables.jl version that
defines `Tables.Scan`. The mandatory CI integration job installs the exact
reviewed Tables.jl revision and fails if that API is absent.

Run a focused file from the test environment when you work on one area:

```sh
julia --project=test -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=test test/fuzz.jl
julia --project=test test/quality.jl
```

## Build documentation

The documentation build is strict. From the repository root, run:

```sh
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

The second command also works from another directory when you pass the full
path to `docs/make.jl` and its project.

## Submit a pull request

Keep changes focused. Describe user-visible compatibility changes. Include the
tests and documentation that establish the intended behavior. Include before
and after measurements for performance work, with the Julia version, thread
count, platform, and input shape.

Disclose substantial generative-AI contributions in the pull request. State
the tools and the work they performed. A human maintainer must read and approve
all generated code before merge.
