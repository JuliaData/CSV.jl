# Issue #1143 Triage Report

## Summary
CSV.jl fails to parse files with multi-line quoted fields when using multiple threads, while DuckDB/QuackIO parse them successfully.

## Issue Type
Known Limitation

## Reproduction
- Reproduced: Yes (with ntasks > 1)
- Julia version tested: 1.12.3
- Package version tested: main branch

### Error
```
thread = 7 fatal error, encountered an invalidly quoted field while parsing
around row = 4573, col = 12: ""03.10.2018: 2330 UTC: Posn: 38:49.2N – 118:14.5E,
Tianjin Anchorage, China.
```

## Root Cause Analysis
The CSV file contains multi-line quoted fields (descriptions that span multiple lines):

```csv
2018-10-03,23:30 UTC,118.234...,Tianjin Anchorage,...,"03.10.2018: 2330 UTC: ...
During morning routine rounds onboard an anchored bulk carrier the Master
noticed a diesel oil tank manhole opened..."
```

When CSV.jl uses multiple threads (`ntasks > 1`), it:
1. Splits the file into chunks
2. Each thread parses a chunk in parallel
3. Threads need to find valid row boundaries within their chunk

**The problem**: If a chunk boundary falls inside a multi-line quoted field, the thread can't correctly identify where rows start/end, leading to parsing errors.

This is a fundamental challenge with parallel CSV parsing when files contain quoted fields with embedded newlines.

## Related Issues
- #1139: Fail to read multiple-line string with multiple tasks
- #1140: Cannot round-trip a file with multi-line fields
- #1157: read fallback to ntasks=1 not always working

## Workaround
```julia
# Disable multi-threading
CSV.File(file; ntasks=1)
```

This works correctly:
```julia
file = CSV.File("/tmp/pirate_attacks.csv"; ntasks=1)
# SUCCESS! Parsed 7511 rows
```

## Potential Improvements
1. **Better chunk boundary detection**: Scan for quoted regions when determining chunk boundaries
2. **Automatic fallback**: Detect multi-line quoted fields and fall back to single-threaded
3. **Documentation**: Clearly document this limitation

## Recommended Action
1. Document the limitation with multi-line quoted fields and multi-threading
2. Suggest `ntasks=1` workaround in error messages
3. Consider automatic fallback when multi-line quoted fields are detected

## Status
**Known limitation** - Use `ntasks=1` for files with multi-line quoted fields.
