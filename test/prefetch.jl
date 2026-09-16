# Run in a separate process so the delayed worker cannot affect other tests.
using CSV, Test

@eval CSV begin
    const _PREFETCH_TEST_DONE = Threads.Atomic{Int}(0)
    # A closed gate makes "the workers have not finished" a fact about the
    # reader rather than a race with a sleep. The wait is bounded, so a reader
    # that joins too early fails an assertion instead of hanging.
    # -1: no gate (a page read that completes after the value parse). 0: closed.
    const _PREFETCH_TEST_GATE = Threads.Atomic{Int}(-1)
    function _prefetchrange(m::Vector{UInt8}, lo::Int, hi::Int)
        if _PREFETCH_TEST_GATE[] < 0
            sleep(0.2)
        else
            deadline = time() + 10
            while _PREFETCH_TEST_GATE[] == 0 && time() < deadline
                sleep(0.005)
            end
        end
        acc = UInt8(0)
        @inbounds for i in lo:PREFETCH_PAGE:hi
            acc ⊻= m[i]
        end
        Threads.atomic_add!(_PREFETCH_TEST_DONE, 1)
        return acc
    end
end

const NWORKERS = min(4, Threads.nthreads())

@testset "eager read joins source workers" begin
    CSV.File(IOBuffer("a,b\n1,x\n"); parallel=false)
    mktemp() do path, io
        write(io, "a,b\n" * repeat("1,long retained text value\n", 30_000))
        close(io)
        f = CSV.File(path; parallel=false)
        @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
        @test f.b[end] == "long retained text value"
        for transpose in (false, true)
            CSV._PREFETCH_TEST_DONE[] = 0
            @test_throws ArgumentError CSV.File(path; transpose, quotechar='λ')
            @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
            sleep(0.3) # failed assertions must not let workers outlive this file
        end
        # Chunks reads the source in windows, so it owns the workers across the
        # header prefix and the schema pre-pass and joins them before it returns.
        CSV._PREFETCH_TEST_DONE[] = 0
        c = CSV.Chunks(path; parallel=false)
        @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
        @test sum(length, c) == 30_000
        CSV._PREFETCH_TEST_DONE[] = 0
        @test_throws ArgumentError CSV.Chunks(path; quotechar='λ')
        @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
        sleep(0.3)
        # Release the mapping before Windows removes the temporary file.
        GC.gc()
    end
end

# The workers exist to overlap page faults with the reader's own passes, so a
# join that happens before those passes defeats them while still leaving every
# worker complete on return. Hold the workers open and walk the passes by hand.
@testset "source workers run through every indexing pass" begin
    mktemp() do path, io
        write(io, "a,b\n" * repeat("1,long retained text value\n", 30_000))
        close(io)
        CSV._PREFETCH_TEST_GATE[] = 0
        CSV._PREFETCH_TEST_DONE[] = 0
        try
            workers = Task[]
            buf = CSV.resolvesource(path; workers)
            @test length(workers) == NWORKERS
            # the header prefix
            p = CSV._prepare(buf)
            @test CSV._PREFETCH_TEST_DONE[] == 0
            # the data range, as one window (File, lazy, Rows, Scan)
            @test CSV._indexdata(p).nrows == 30_000
            @test CSV._PREFETCH_TEST_DONE[] == 0
            # the schema pre-pass, over windows (Chunks)
            plan = CSV.settlecolumns(p)
            @test CSV._settleschema(p, 1 << 12, plan, 2) !== nothing
            @test CSV._PREFETCH_TEST_DONE[] == 0
            CSV._PREFETCH_TEST_GATE[] = 1
            CSV._joinprefetch!(workers)
            @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
        finally
            CSV._PREFETCH_TEST_GATE[] = -1
        end
        GC.gc()
    end
end
