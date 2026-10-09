# Issue #1161 Triage Report

## Summary
Spurious integer overflow when parsing certain negative Int16 values (specifically -32760 and -32767) from CSV.

## Issue Type
Bug (Upstream - Parsers.jl)

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch (commit 04ec1cf)

### Minimal Reproducer
```julia
using CSV, DataFrames

# Values that trigger the bug
data = """-32767
-32760"""

# This incorrectly parses as positive values
df = CSV.read(IOBuffer(data), DataFrame; types=[Int16])
# Expected: [-32767, -32760]
# Actual: [32767, 32760] (sign inverted!)

# Int32 workaround works correctly
df2 = CSV.read(IOBuffer(data), DataFrame; types=[Int32])
# Returns correct values: [-32767, -32760]
```

### Values Affected
- `-32760` becomes `32760`
- `-32767` becomes `32767`
- `-32768` (Int16 minimum) parses correctly

## Root Cause Analysis
The bug is **NOT in CSV.jl** - it's in **Parsers.jl** (the underlying parsing library).

When Parsers.jl parses negative Int16 values near the type's minimum:
1. Values like `-32767` with a trailing newline get their sign inverted
2. The bug appears to be in the overflow detection/handling logic
3. The issue specifically affects values in the range close to but not equal to `typemin(Int16)`

This was confirmed by testing Parsers.jl directly:
```julia
using Parsers
Parsers.parse(Int16, "-32767\n")  # Returns 32767 (wrong!)
Parsers.parse(Int16, "-32768\n")  # Returns -32768 (correct!)
```

## Resolution
- [ ] Fix needed in Parsers.jl, not CSV.jl
- [ ] Workaround: Use Int32 type and convert after parsing

## Recommended Action
1. **Open issue in Parsers.jl** repository to report this bug
2. **Document workaround** in issue comment: use `types=[Int32]` then convert
3. **Close this issue** as upstream (or keep open to track the dependency fix)

## Workaround
```julia
# Instead of:
df = CSV.read(file, DataFrame; types=Dict(:col => Int16))

# Use:
df = CSV.read(file, DataFrame; types=Dict(:col => Int32))
df.col = Int16.(df.col)  # Convert after parsing
```

## Status
**Upstream bug** - Requires fix in Parsers.jl.
