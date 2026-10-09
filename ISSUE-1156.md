# Issue #1156 Triage Report

## Summary
Columns with strings like `000100`, `000200` (integers with leading zeros) are detected as `Int`, losing the leading zeros.

## Issue Type
Feature Request / Expected Behavior

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Example
```julia
using CSV

csv = """id,code
1,000100
2,000200"""

file = CSV.File(IOBuffer(csv))
# code column detected as Int64
# Values: 100, 200 (leading zeros lost)
```

## Analysis
This is **expected behavior** based on CSV.jl's type inference:

1. `000100` is a valid integer literal (value: 100)
2. CSV.jl's type detection sees numeric-only content and infers `Int64`
3. Leading zeros are not preserved in integer representation

This matches behavior of most CSV parsers (Python's pandas, R's read.csv, etc.) - leading zeros in numeric columns are stripped during integer conversion.

## Workaround
Explicitly specify the column type as `String`:

```julia
CSV.File(file; types=Dict(:code => String))
# or
CSV.File(file; types=[Int, String])
```

## Potential Enhancement
Could add a `preserve_leading_zeros=true` option that forces columns starting with `0` followed by digits to be parsed as strings. However, this could have unintended consequences for legitimate integer columns that happen to have values like `0`, `01`, `007`, etc.

## Recommended Action
1. **Document this behavior** in the type inference section
2. **Close as expected behavior** with workaround documented
3. Consider adding `preserve_leading_zeros` option if there's significant demand

## Status
**Expected behavior** - Use `types` parameter to force String type for columns where leading zeros matter.
