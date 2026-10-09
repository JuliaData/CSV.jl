# Issue #1177: `compress` test seems broken

## Summary
When using `CSV.write(io, table; compress=true)` with an IOBuffer, no data is written. Calling `take!(io)` returns an empty array.

## Reproduction
```julia
julia> using CSV
julia> default_table = (col1=[1,2,3], col2=[4,5,6], col3=[7,8,9])
julia> io = IOBuffer()
julia> CSV.write(io, default_table; compress=true)
julia> take!(io) |> println
UInt8[]  # nothing written!
```

## Root Cause Analysis
The issue is in the `with` function in `src/write.jl`. When `compress=true`:

1. A `GzipCompressorStream` wraps the user's IOBuffer
2. Data is written to the compressor stream
3. When the compressor is closed, it **also closes the underlying IOBuffer by default**
4. Calling `take!` on a closed IOBuffer returns empty bytes

The fix is to use `stop_on_end=true` when creating the `GzipCompressorStream` for user-provided IO streams. This prevents the compressor from closing the underlying stream when it's closed.

## Fix Applied
Modified `src/write.jl` in the `with` function:

```julia
function with(f::Function, @nospecialize(io), append, compress)
    needtoclose = false
    opened_stream = false  # Track if we opened the stream
    if io isa Union{Base.TTY, Base.Pipe, Base.PipeEndpoint, Base.DevNull}
        # pass, can't seek these
    elseif io isa IO
        !append && _seekstart(io)
    else
        io = open(io, append ? "a" : "w")
        needtoclose = true
        opened_stream = true
    end
    if compress
        # Use stop_on_end=true if we didn't open the underlying stream ourselves.
        # This prevents the GzipCompressorStream from closing the user's IO when
        # the compressor is closed, allowing take!(io) to work on IOBuffer, etc.
        io = GzipCompressorStream(io; stop_on_end=!opened_stream)
        needtoclose = true
    end
    try
        return f(io)
    finally
        needtoclose && close(io)
    end
end
```

## Tests Updated
Updated `test/write.jl` to properly test compressed writing:
- Verify IOBuffer stays open after write
- Verify data is written (non-empty)
- Verify gzip magic bytes are present
- Verify data decompresses to expected content
- Test file path compression (which always worked)

## Status
**Fixed** - The `compress=true` option now works correctly with IOBuffer.

## Note
There is a separate pre-existing issue with `CSV.read` from IOBuffer containing gzipped data - the `getsource` function uses the full backing array instead of just the valid portion. This causes decompression failures when the IOBuffer's backing array is larger than the actual data. This is a separate issue from #1177.
