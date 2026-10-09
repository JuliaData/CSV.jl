# Issue #1164 Triage Report

## Summary
`Base.memhash` and `Base.memhash_seed` are being removed in Julia 1.13, breaking CSV.jl's `PointerString` hash implementation.

## Issue Type
Breaking Change (Julia Compatibility)

## Reproduction
- Reproduced: N/A (Julia 1.13 not yet released)
- Julia version affected: 1.13+
- Package version tested: main branch

### Affected Code
`src/utils.jl:581-583`:
```julia
function Base.hash(s::PointerString, h::UInt)
    h += Base.memhash_seed
    ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), s.ptr, s.len, h % UInt32) + h
end
```

## Root Cause Analysis
Julia PR #57509 replaces MurmurHash with RapidHash, removing the internal `memhash` and `memhash_seed` symbols. This is intentional - the Julia team considers external use of these internals to be unsound.

The `PointerString` struct is used in CSV.jl for zero-allocation string comparison during pooling. The hash function is needed for dictionary operations when building string pools.

## Resolution Path
Julia 1.13 introduces a new public API for byte hashing:

```julia
# New API in Julia 1.13+
Base.hash_bytes(pointer, size, h, Base.HASH_SECRET)
```

### Proposed Fix
Use conditional compilation based on Julia version:

```julia
@static if VERSION >= v"1.13"
    function Base.hash(s::PointerString, h::UInt)
        GC.@preserve s Base.hash_bytes(s.ptr, s.len, UInt64(h), Base.HASH_SECRET) % UInt
    end
else
    function Base.hash(s::PointerString, h::UInt)
        h += Base.memhash_seed
        ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), s.ptr, s.len, h % UInt32) + h
    end
end
```

## Files to Modify
1. **src/utils.jl**: Update `Base.hash(::PointerString, ::UInt)` with version-conditional implementation

## Testing Strategy
1. Test on Julia 1.12 (current) - verify existing behavior unchanged
2. Test on Julia 1.13-dev/nightly - verify new API works
3. Verify hash consistency: `hash(PointerString(...)) == hash(String(...))`

## Recommended Action
1. **Wait for Julia 1.13 release candidate** to confirm final API
2. **Implement version-conditional fix** as shown above
3. **Add CI job for Julia nightly** to catch future breakages early

## Priority
**Medium** - Not urgent until Julia 1.13 is released, but should be addressed before that.

## Related Links
- Julia PR #57509: RapidHash implementation
- Julia Issue #59151: Discussion on memhash removal

## Status
**Awaiting Julia 1.13** - Fix approach identified, implementation should wait for stable API.
