# Issue #1146 Triage Report

## Summary
When passing a vector with a single CSV file to `CSV.File` with `source` parameter, the source column is not added. Works correctly with multiple files.

## Issue Type
Bug

## Reproduction
- Reproduced: Yes
- Julia version tested: 1.12.3
- Package version tested: main branch

### Minimal Reproducer
```julia
using CSV

# Create test file
open("/tmp/test.csv", "w") do f
    write(f, "a,b\n1,2\n3,4")
end

# With multiple files - source column IS added
files = ["/tmp/test.csv", "/tmp/test.csv"]
tbl = CSV.File(files; source=:source)
:source in propertynames(tbl)  # true ✓

# With single file in vector - source column NOT added
files_single = ["/tmp/test.csv"]
tbl_single = CSV.File(files_single; source=:source)
:source in propertynames(tbl_single)  # false ✗
```

## Root Cause Analysis
The bug is in `src/file.jl:936`:

```julia
function File(sources::Vector;
    source::Union{Nothing, Symbol, AbstractString,
        Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing,
    kw...)
    # ...
    length(sources) == 1 && return File(sources[1]; kw...)  # Line 936 - BUG
    # ... source column handling happens AFTER this short-circuit
```

When there's only one source, the function short-circuits and calls the single-file `File` constructor, which doesn't process the `source` keyword argument. The `source` column handling code (lines 963-976) is never reached.

## Proposed Fix
Handle the `source` parameter before the short-circuit, or modify the short-circuit to pass through the source parameter:

```julia
function File(sources::Vector;
    source::Union{...}=nothing,
    kw...)
    isempty(sources) && throw(ArgumentError("..."))
    if source isa Pair
        length(source.second) == length(sources) || throw(ArgumentError("..."))
    end

    # Handle single source case WITH source column
    if length(sources) == 1
        f = File(sources[1]; kw...)
        if source !== nothing
            # Add source column for single file
            val = source isa Pair ? source.second[1] : getname(sources[1])
            colnm = Symbol(source isa Pair ? source.first : source)
            # ... add column logic
        end
        return f
    end
    # ... rest of multi-file handling
```

## Files to Modify
- `src/file.jl`: Modify the `File(sources::Vector; ...)` function around line 936

## Workaround
Add the source column manually after reading:

```julia
files = ["/tmp/test.csv"]
tbl = CSV.File(files[1])
# Manually add source column to resulting table/DataFrame
```

## Status
**Bug confirmed** - Single-file short-circuit bypasses source column handling.
