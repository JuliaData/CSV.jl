# Issue #1169 Triage Report

## Summary
Double-quote escaping is inconsistent between `stringtype=PosLenString` (default) and `stringtype=String`. The default returns incorrectly escaped strings.

## Issue Type
Bug (Upstream - WeakRefStrings.jl)

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch
- WeakRefStrings version: 1.4.2

### Minimal Reproducer
```julia
using CSV

# CSV content: x,y
#              1,"{""a"": 2, ""b"": """"}"

# Without stringtype=String (default PosLenString):
row = first(CSV.Rows("test.csv"))
println(row.y)  # {"a": 2, "b": """}  <- WRONG (extra quote)

# With stringtype=String:
row = first(CSV.Rows("test.csv"; stringtype=String))
println(row.y)  # {"a": 2, "b": ""}   <- CORRECT
```

## Root Cause Analysis
The bug is in **WeakRefStrings.jl**, not CSV.jl.

The `escapedcodeunits` function in WeakRefStrings has incorrect logic for handling consecutive escape characters:

```julia
# Current buggy logic in WeakRefStrings/src/poslenstrings.jl
[Int(x) for x = p.pos:maxpos if !((x == p.pos && d[x] == e) || (x > p.pos && d[x - 1] != e && d[x] == e))]
```

For input `""""` (which should become `""`):
- Position 1: `"` - skip (prev is non-quote)
- Position 2: `"` - keep (prev is quote)
- Position 3: `"` - keep (prev is quote) <- BUG: should be new escape pair
- Position 4: `"` - keep (prev is quote)

Result: `"""` instead of `""`

The algorithm doesn't correctly track escaped pairs - it only checks if the previous byte in the **original buffer** is an escape character, not whether we're in the middle of an escape sequence.

## Workaround
```julia
CSV.Rows(file; stringtype=String)
CSV.File(file; stringtype=String)
```

## Recommended Action
1. **File issue in WeakRefStrings.jl** describing the `escapedcodeunits` bug
2. **Document workaround** in CSV.jl: use `stringtype=String` for files with complex escaped quotes
3. Fix should be in WeakRefStrings.jl

## Status
**Upstream bug** - Requires fix in WeakRefStrings.jl.
