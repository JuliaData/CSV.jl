# Issue #1168 Triage Report

## Summary
When writing CSV to an existing IOStream with `append=false` (default), CSV.write seeks to the beginning but doesn't truncate, leaving old content at the end of the file.

## Issue Type
Bug

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Minimal Reproducer
```julia
using CSV

open("tmp.csv", write=true) do io
    println(io, "# comment 1")
    println(io, "# comment 2")
    CSV.write(io, [(a=1, b=2), (a=10, b=20)])
end

read("tmp.csv", String)
# Result: "a,b\n1,2\n10,20\ncomment 2\n"
# Expected: "a,b\n1,2\n10,20\n"
```

## Root Cause Analysis
In `src/write.jl:284`, the `with` function seeks to the start of the IO stream when `append=false`:

```julia
function with(f::Function, @nospecialize(io), append, compress)
    # ...
    elseif io isa IO
        !append && _seekstart(io)  # Line 284
    # ...
end
```

The issue is that seeking to the start doesn't truncate the file. When the new content is shorter than the old content, the old tail remains.

### Position trace:
- After writing comments: position = 24
- After CSV.write: position = 14 (seeked back and wrote 14 bytes)
- File still contains 24 bytes (old tail not removed)

## Resolution
The fix should truncate the file after writing when `append=false` and the IO is seekable/truncatable.

### Proposed Fix
Add truncation after writing in `with` function or `write` function:

```julia
# After f(io) returns, if append=false and io is truncatable:
!append && truncate(io, position(io))
```

## Files to Modify
- `src/write.jl`: Add truncation logic after CSV content is written

## Workaround
Users should either:
1. Open files with `"w"` mode (truncates): `open("file.csv", "w")`
2. Or truncate manually after CSV.write completes

## Status
**Bug confirmed** - CSV.write should truncate after seeking when `append=false`.
