# Test suite

Run all tests from the repository root:

```sh
julia --project=. -e 'using Pkg; Pkg.add([PackageSpec(url="https://github.com/JuliaStrings/InlineStrings.jl.git", rev="ce4c3549691c4b3443cc14ffa90ebdd6636eff2f"), PackageSpec(url="https://github.com/JuliaData/Parsers.jl.git", rev="83c7142fb714cb87261ef38eec7ab103444eb30d")]); Pkg.test()'
```

CSV.jl depends on Parsers 3 for the reviewed low-level kernels. Until
registration, CI temporarily pins
[Parsers.jl PR #210](https://github.com/JuliaData/Parsers.jl/pull/210) at exact
commit `83c7142fb714cb87261ef38eec7ab103444eb30d`. Use the same commit for local
tests. Registered InlineStrings releases still require Parsers 2. Tests also
pin [InlineStrings.jl PR #93](https://github.com/JuliaStrings/InlineStrings.jl/pull/93)
at `ce4c3549691c4b3443cc14ffa90ebdd6636eff2f`. Install both source revisions in
one operation so the test environment can resolve.

The suite has focused checks for the parser kernel, typed values, public reader
APIs, the writer, and deterministic fuzz input. The fuzz tests print their fixed
seed in each failure context.

Tables.Scan tests run when the loaded Tables.jl version provides that API. CI
has a separate job that pins the reviewed Tables.Scan revision until a release
includes it.

Prepare the test environment before you run a benchmark script:

```sh
julia --project=test -e 'using Pkg; Pkg.add([PackageSpec(path=pwd()), PackageSpec(url="https://github.com/JuliaStrings/InlineStrings.jl.git", rev="ce4c3549691c4b3443cc14ffa90ebdd6636eff2f"), PackageSpec(url="https://github.com/JuliaData/Parsers.jl.git", rev="83c7142fb714cb87261ef38eec7ab103444eb30d")]); Pkg.develop(path=pwd()); Pkg.instantiate()'
julia --project=test -t4 bench/bench_matrix.jl local 0.01 --core
```
