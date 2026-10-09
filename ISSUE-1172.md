# Issue #1172 Triage Report

## Summary
When using `transpose=true` with data that requires type promotion (e.g., Int to String), data appears scrambled/incorrect.

## Issue Type
Bug

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch (commit 04ec1cf)

### Minimal Reproducer
```julia
using CSV, DataFrames

# Create transposed CSV data where column 2 has mixed types requiring promotion
data = """a,1,2
b,x,3"""

# Parse with transpose=true
df = CSV.read(IOBuffer(data), DataFrame; transpose=true)
# Expected: columns are (a,b), (1,x), (2,3)
# Actual: scrambled data due to position tracking bug
```

## Root Cause Analysis
The bug is in `promotetostring!` function in `src/file.jl`. When a column needs type promotion to String:

1. The function creates a new `columns` array with dummy columns for non-promoted columns
2. For transpose mode, each column tracks its position in the buffer (`position`, `endposition`)
3. **Bug**: The dummy columns weren't copying the position fields from the original columns
4. **Bug**: Before re-parsing, positions weren't reset to their original starting values
5. **Result**: The re-parse reads from wrong positions, causing scrambled output

## Resolution
- [x] Fix identified in `src/file.jl:promotetostring!`
- [x] Added `originalposition` field to `Column` struct in `src/context.jl`
- [x] Tests verify the fix works
- [x] Branch created: `fix-issue-1172`

### Files Modified
1. **src/context.jl**: Added `originalposition::Int` field to `Column` struct to track the starting position for transpose mode
2. **src/file.jl**: Updated `promotetostring!` to:
   - Copy position fields when creating dummy columns
   - Reset all column positions to `originalposition` before re-parsing

## Recommended Action
1. Review and merge branch `fix-issue-1172`
2. Add regression test for transpose + type promotion

## Status
**Fix implemented** - Branch `fix-issue-1172` ready for review.
