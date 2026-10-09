# Issue #1141 Triage Report

## Summary
Segmentation fault when reading a CSV file with `transpose=true` that requires type promotion during parsing.

## Issue Type
Bug (Fixed)

## Reproduction
- Reproduced: Yes (with older CSV.jl)
- Julia version tested: 1.12.3
- Package version tested: main branch (fixed)

### Original Issue
```julia
using CSV
file = "table.csv"
df = CSV.File(file; transpose=true)  # segfaults
```

The segfault occurred in `promotetostring!` when parsing transposed CSV files.

## Root Cause Analysis
This issue has the **same root cause as #1172** - the `promotetostring!` function didn't properly handle transpose mode.

The segfault happened because:
1. When a column needed type promotion, `promotetostring!` re-parses the data
2. For transpose mode, each column has a specific position range in the buffer
3. The function wasn't copying position tracking fields for dummy columns
4. This caused out-of-bounds memory access during re-parsing

## Resolution
**Fixed by the #1172 fix** on branch `fix-issue-1172`.

The fix added:
1. `originalposition` field to `Column` struct
2. Proper position copying when creating dummy columns in `promotetostring!`
3. Position reset to original values before re-parsing

## Verification
```julia
using CSV
file = "/tmp/table_1141.csv"
result = CSV.File(file; transpose=true)
# SUCCESS! Parsed 128 rows
```

## Recommended Action
1. Merge the `fix-issue-1172` branch
2. Close this issue as duplicate of / fixed by #1172

## Status
**Fixed** - Same root cause as #1172, resolved by the transpose fix.
