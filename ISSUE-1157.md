# Issue #1157 Triage Report

## Summary
Multithreaded parsing fallback to single-threaded doesn't always trigger - sometimes parsing fails with fatal error instead of gracefully falling back.

## Issue Type
Known Limitation

## Reproduction
- Reproduced: Conceptually confirmed
- Julia version tested: 1.12.3
- Package version tested: main branch

## Root Cause Analysis
The multithreaded parsing uses a two-phase approach:

1. **Chunking Phase** (`findrowstarts!`): Find chunk boundaries by locating newlines
2. **Validation Phase** (`findchunkrowstart`): Validate by parsing `rows_to_check` rows (default: 5)
3. **Parsing Phase**: Each thread parses its chunk

The issue is that **validation only checks 5 rows per chunk**. If a multi-line quoted field appears deeper in the chunk (after those 5 rows), validation passes but parsing later fails with a fatal error.

### Why fallback sometimes works:
- If the multi-line field is detected during validation, fallback triggers gracefully
- The `@error` message appears when fallback succeeds

### Why fallback sometimes fails:
- If validation passes (5 rows look fine) but a problematic field is later in the chunk
- Parser encounters invalid row boundary during actual parsing
- Fatal error is thrown from `parsefilechunk!` instead of graceful fallback

## Related Issues
- #1139: Fail to read multiple-line string with multiple tasks
- #1140: Cannot round-trip a file with multi-line fields
- #1143: CSV.jl fails where DuckDB succeeds (same root cause)

## Workaround
```julia
CSV.File(file; ntasks=1)
```

## Potential Improvements
1. **Increase `rows_to_check`**: Check more rows during validation (tradeoff: slower startup)
2. **Better error handling**: Catch parsing errors and retry with `ntasks=1`
3. **Quoted field detection**: During chunking, detect if file has multi-line quoted fields and auto-disable threading

## Recommended Action
1. Document the limitation more clearly
2. Consider adding automatic retry with `ntasks=1` when multithreaded parsing fails
3. Consider adding `rows_to_check` as a user-configurable parameter

## Status
**Known limitation** - Use `ntasks=1` for files with multi-line quoted fields.
