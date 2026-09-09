# Test suite

From the repository root:

```sh
julia --project=test test/dependencies.jl
julia --project=test --check-bounds=yes -t4 test/runtests.jl
julia --project=test test/quality.jl
```

Every dependency (DataStrings 1, DataDecimals 1, Parsers 3, InlineStrings 2,
Tables 1.14) resolves from General; the dependency helper only develops this
checkout into the environment and instantiates it. Tables.Scan runs in every
main test job.

Tests cover structural geometry, reader modes, exact decimals and inference,
string ownership, ordered writers, and deterministic malformed-input fuzzing.
Run Julia 1.10 and current Julia. Benchmark scripts use the same test environment.
