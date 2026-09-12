# Run in a separate process so the delayed worker cannot affect other tests.
using CSV, Test

@eval CSV begin
    const _PREFETCH_TEST_DONE = Threads.Atomic{Int}(0)
    function _prefetchrange(m::Vector{UInt8}, lo::Int, hi::Int)
        sleep(0.2) # model a page read that completes after the value parse
        acc = UInt8(0)
        @inbounds for i in lo:PREFETCH_PAGE:hi
            acc ⊻= m[i]
        end
        Threads.atomic_add!(_PREFETCH_TEST_DONE, 1)
        return acc
    end
end

@testset "eager read joins source workers" begin
    CSV.File(IOBuffer("a,b\n1,x\n"); parallel=false)
    mktemp() do path, io
        write(io, "a,b\n" * repeat("1,long retained text value\n", 30_000))
        close(io)
        f = CSV.File(path; parallel=false)
        @test CSV._PREFETCH_TEST_DONE[] == min(4, Threads.nthreads())
        @test f.b[end] == "long retained text value"
        # Release the mapping before Windows removes the temporary file.
        GC.gc()
    end
end
