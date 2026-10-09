# Issue #1138 Triage Report

## Summary
`CSV.write()` with `append=true` allocates significantly more memory than expected when called repeatedly in a loop.

## Issue Type
Expected Behavior / Documentation

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Example
```julia
using CSV

data = [(a=1, b="test")]

# Writing 100 rows with append in a loop
rm("/tmp/test.csv", force=true)
CSV.write("/tmp/test.csv", [data[1]]; header=true)
@time for i in 2:100
    CSV.write("/tmp/test.csv", [data[1]]; append=true)
end
# Result: ~400 MiB allocations for 100 tiny writes!
```

## Root Cause Analysis
This is **expected behavior** based on how `CSV.write` works:

1. Each `CSV.write` call allocates a buffer (default `bufsize=2^22 = 4 MiB`)
2. When appending in a loop, each iteration allocates a fresh 4 MiB buffer
3. 100 iterations × 4 MiB = ~400 MiB total allocations

The buffer is used for efficient writing of large datasets. For small datasets written repeatedly, this overhead is significant.

## Workaround
Use the `bufsize` parameter to reduce memory allocation:

```julia
# With small bufsize
@time for i in 2:100
    CSV.write("/tmp/test.csv", [data[1]]; append=true, bufsize=1024)
end
# Result: ~3.5 MiB allocations (vs ~400 MiB with default)
```

Or better, collect all data and write once:

```julia
# Write all at once - most efficient
all_data = [(a=i, b="test") for i in 1:100]
CSV.write("/tmp/test.csv", all_data)
```

## Recommendation
1. **Document `bufsize` parameter** more prominently for append use cases
2. **Add guidance** about efficient patterns for incremental writes
3. **Consider auto-scaling** bufsize based on data size (enhancement)

## Alternative Approaches for Row-by-Row Writing
For truly incremental writing, users might prefer:

```julia
open("/tmp/test.csv", "a") do io
    println(io, "$(row.a),$(row.b)")
end
```

This avoids CSV.jl's buffer allocation overhead entirely.

## Status
**Expected behavior** - Use `bufsize` parameter or write all data at once.
