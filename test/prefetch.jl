# Run in a separate process so the delayed worker cannot affect other tests.
using CSV, Test

@eval CSV begin
    const _PREFETCH_TEST_DONE = Threads.Atomic{Int}(0)
    # Set by the join and read by every structural pass: a pass that runs once
    # this is 1 is a pass the page-touch workers could not overlap. Both are
    # facts about the reader's call order, so the checks below need no sleep
    # and cannot hang.
    const _PREFETCH_TEST_JOINED = Threads.Atomic{Int}(0)
    const _PREFETCH_TEST_LATE = Threads.Atomic{Int}(0)
    const _PREFETCH_TEST_PASSES = Threads.Atomic{Int}(0)
    # 1 while a public reader is under test.
    const _PREFETCH_TEST_WATCH = Threads.Atomic{Int}(0)

    # A worker that finishes late makes "the reader joined its workers" an
    # assertion rather than a coincidence.
    function _prefetchrange(m::Vector{UInt8}, lo::Int, hi::Int)
        sleep(0.2)
        acc = UInt8(0)
        @inbounds for i in lo:PREFETCH_PAGE:hi
            acc ⊻= m[i]
        end
        Threads.atomic_add!(_PREFETCH_TEST_DONE, 1)
        return acc
    end

    # The production body with one line added: mark that the reader has reached
    # its join. Detached workers are the only ones a reader owns across passes.
    function _joinprefetch!(tasks::Vector{Task})
        isempty(tasks) || (_PREFETCH_TEST_JOINED[] = 1)
        failure = nothing
        for task in tasks
            try
                wait(task)
            catch err
                failure === nothing && (failure = err)
            end
        end
        empty!(tasks)
        failure === nothing || throw(failure)
        return nothing
    end

    # Every structural pass of every reader indexes its chunks through this.
    function indexone!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, scanner::Symbol)
        if _PREFETCH_TEST_WATCH[] == 1
            Threads.atomic_add!(_PREFETCH_TEST_PASSES, 1)
            _PREFETCH_TEST_JOINED[] == 0 || Threads.atomic_add!(_PREFETCH_TEST_LATE, 1)
        end
        scanner === :lenient ? indexchunk_lenient!(ci, buf, d) :
        scanner === :scalar  ? indexchunk_scalar!(ci, buf, d) :
                               indexchunk_fast!(ci, buf, d)
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
# worker complete on return. Drive the public readers and count the structural
# passes that ran after the join: a premature join anywhere in `_prepareindexed`
# or in the `Chunks` constructor moves passes to the wrong side of it.
@testset "source workers run through every indexing pass" begin
    mktemp() do path, io
        write(io, "a,b\n" * repeat("1,long retained text value\n", 30_000))
        close(io)
        for reader in (CSV.File, CSV.Chunks)
            CSV._PREFETCH_TEST_DONE[] = 0
            CSV._PREFETCH_TEST_JOINED[] = 0
            CSV._PREFETCH_TEST_LATE[] = 0
            CSV._PREFETCH_TEST_PASSES[] = 0
            # A small window indexes the header prefix, then the data range one
            # small piece at a time, so a premature join leaves many passes
            # behind it.
            t = try
                CSV._PREFETCH_TEST_WATCH[] = 1
                reader(path; chunkbytes=1 << 12)
            finally
                CSV._PREFETCH_TEST_WATCH[] = 0
            end
            @test CSV._PREFETCH_TEST_PASSES[] > 1      # the check is not vacuous
            @test CSV._PREFETCH_TEST_LATE[] == 0
            @test CSV._PREFETCH_TEST_JOINED[] == 1
            @test CSV._PREFETCH_TEST_DONE[] == NWORKERS
            @test (t isa CSV.File ? length(t) : sum(length, t)) == 30_000
        end
        GC.gc()
    end
end
