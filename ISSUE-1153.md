# Issue #1153 Triage Report

## Summary
`CSV.write` doesn't quote fields containing carriage return (`\r`), resulting in invalid CSV that can't be read back correctly.

## Issue Type
Bug

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Minimal Reproducer
```julia
using CSV

data = [(a="text with\rcarriage return",)]
CSV.write("/tmp/test.csv", data)

# Result: "a\ntext with\rcarriage return\n"
# The \r is NOT quoted, making the CSV invalid

# When read back:
file = CSV.File("/tmp/test.csv")
first(file).a  # Returns "text with" - data truncated!
```

## Root Cause Analysis
The bug is in the `check` function in `src/write.jl:539-549`:

```julia
function check(bytes, sz, delim::UInt8, oq, cq, newline::UInt8)
    isempty(bytes) && return false, false
    needtoescape = false
    @inbounds needtoquote = bytes[1] == oq
    @simd for i = 1:sz
        @inbounds b = bytes[i]
        needtoquote |= (b == delim) | (b == newline)  # Only checks \n
        needtoescape |= b == cq
    end
    return needtoescape, needtoquote
end
```

The function only checks if a byte matches the configured `newline` character (default `\n`). It doesn't check for `\r` (carriage return), which CSV.jl's reader treats as a valid line terminator.

### Characters that should trigger quoting:
- `\n` (LF, 0x0A) - Unix newline ✓ checked
- `\r` (CR, 0x0D) - Old Mac newline / part of CRLF ✗ NOT checked
- Delimiter character ✓ checked
- Quote character ✓ checked

## Proposed Fix
Modify the `check` function to also check for `\r`:

```julia
needtoquote |= (b == delim) | (b == newline) | (b == UInt8('\r'))
```

Or more robustly, always check for both `\r` and `\n` regardless of the configured newline:

```julia
const CR = UInt8('\r')
const LF = UInt8('\n')
needtoquote |= (b == delim) | (b == CR) | (b == LF)
```

## Files to Modify
- `src/write.jl`: Update all variants of the `check` function (lines 539, 551, 572, 593)

## Workaround
Currently none - users must manually quote fields or sanitize data before writing.

## Status
**Bug confirmed** - `check` function missing `\r` detection.
