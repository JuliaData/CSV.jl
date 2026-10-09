# Issue #1160 Triage Report

## Summary
`skipto` and `limit` count logical CSV rows, not physical file lines. Quoted fields containing newlines cause off-by-one errors when users expect physical line numbering.

## Issue Type
Bug / Feature Request

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Root Cause
CSV.jl correctly follows RFC 4180, treating newlines inside quoted fields as part of the field value. When counting rows for `skipto`/`limit`, these multi-line fields count as one row.

Example from the test file:
```
% Reference: A.B. Birchfield, ... "Grid Structural Characteristics as Validation
% Criteria for Synthetic Networks," IEEE Transactions on Power Systems...
```

Lines 5-6 contain a quoted string spanning two physical lines. CSV.jl counts this as one logical row, causing `skipto=15` to land on physical line 16.

## Workaround
Disable quote handling by using an unlikely quote character:

```julia
CSV.File(file; skipto=15, quotechar=UInt8(0x00))
```

This makes CSV.jl treat all quotes as literal characters, so physical lines equal logical rows.

## Potential Fix Options

1. **Document behavior**: Clarify that `skipto`/`limit` refer to logical CSV rows, not physical lines

2. **Add `linecount` mode**: A new option to count physical newlines instead of logical rows (as suggested in the issue)

3. **Smarter quote detection**: Only treat `"` as a quote character when it appears after a delimiter (as suggested by @luke-kiernan). This is a significant parsing change.

## Recommended Action
1. Document the current behavior clearly
2. Consider adding a `linecount` option for users who need physical line numbering
3. The smarter quote detection would be a breaking change requiring careful consideration

## Status
**Workaround available** - Use `quotechar=UInt8(0x00)` to disable quote handling.
