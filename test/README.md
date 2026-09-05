# Test suite

From the repository root:

```sh
julia --project=test test/dependencies.jl
julia --project=test --check-bounds=yes -t4 test/runtests.jl
julia --project=test test/quality.jl
```

The dependency helper pins only DataStrings and DataDecimals while their initial
registrations are pending. Parsers 3, InlineStrings 2, and Tables 1.14 resolve
from General. Tables.Scan runs in every main test job.

Tests cover structural geometry, reader modes, exact decimals and inference,
string ownership, ordered writers, and deterministic malformed-input fuzzing.
Run Julia 1.10 and current Julia. Benchmark scripts use the same test environment.

The documentation environment also pins JSON PR #480 at `bcb8e334682e8135c08913781bf8200832cf752e` until a JSON release supports Parsers 3. This is a docs dependency gate, not a CSV runtime dependency.
