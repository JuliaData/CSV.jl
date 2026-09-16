# Writer battery: round-trips through CSV.File, explicit byte contracts,
# and byte determinism across thread counts.
using Test, Dates, Tables, CodecZlib, FilePathsBase, Random, InlineStrings, PooledArrays, DataDecimals
using Durations: Timestamp
using CSV
const W = CSV

buf() = IOBuffer()
str(f) = (io = buf(); f(io); String(take!(io)))

mutable struct NoSchemaRows{T}
    values::Vector{T}
    next::Int
end
Base.iterate(r::NoSchemaRows, state=nothing) =
    r.next > length(r.values) ? nothing : (r.values[r.next], (r.next += 1))
Tables.istable(::Type{<:NoSchemaRows}) = true
Tables.rowaccess(::Type{<:NoSchemaRows}) = true
Tables.rows(r::NoSchemaRows) = r
Tables.schema(::NoSchemaRows) = nothing

mutable struct KnownSchemaRows{T}
    values::Vector{T}
    next::Int
end
Base.iterate(r::KnownSchemaRows, state=nothing) =
    r.next > length(r.values) ? nothing : (r.values[r.next], (r.next += 1))
Base.IteratorSize(::Type{<:KnownSchemaRows}) = Base.SizeUnknown()
Tables.istable(::Type{<:KnownSchemaRows}) = true
Tables.rowaccess(::Type{<:KnownSchemaRows}) = true
Tables.rows(r::KnownSchemaRows) = r
Tables.schema(::KnownSchemaRows) = Tables.Schema((:a, :b), (Int, String))

struct PartitionedTable{T}
    parts::T
end
Tables.partitions(t::PartitionedTable) = t.parts

mutable struct OneShotPartitions{T}
    values::Vector{T}
    next::Int
end
Base.IteratorSize(::Type{<:OneShotPartitions}) = Base.SizeUnknown()
Base.iterate(p::OneShotPartitions, state=nothing) =
    p.next > length(p.values) ? nothing : (p.values[p.next], (p.next += 1))

mutable struct PartitionWriteState
    lock::ReentrantLock
    active::Int
    highwater::Int
    completed::Int
end
struct PartitionSink <: IO
    state::PartitionWriteState
    fail::Bool
end
const PARTITION_SENTINEL = ErrorException("partition sentinel")
Base.isopen(::PartitionSink) = true
Base.iswritable(::PartitionSink) = true
function Base.unsafe_write(io::PartitionSink, ::Ptr{UInt8}, n::UInt)
    lock(io.state.lock) do
        io.state.active += 1
        io.state.highwater = max(io.state.highwater, io.state.active)
    end
    try
        for _ in 1:100
            yield()
        end
        io.fail && throw(PARTITION_SENTINEL)
        return n
    finally
        lock(io.state.lock) do
            io.state.active -= 1
            io.state.completed += 1
        end
    end
end
Base.flush(::PartitionSink) = nothing

struct WriterExplodes end
const WRITER_SENTINEL = ErrorException("writer sentinel")
Base.show(::IO, ::WriterExplodes) = throw(WRITER_SENTINEL)

mutable struct FailingWriterSink <: IO
    open::Bool
end
const WRITER_SINK_SENTINEL = ErrorException("writer sink sentinel")
Base.isopen(io::FailingWriterSink) = io.open
Base.iswritable(::FailingWriterSink) = true
Base.unsafe_write(::FailingWriterSink, ::Ptr{UInt8}, ::UInt) =
    throw(WRITER_SINK_SENTINEL)
Base.flush(::FailingWriterSink) = nothing
Base.close(io::FailingWriterSink) = (io.open = false)

struct ThrowingRows end
Tables.istable(::Type{ThrowingRows}) = true
Tables.rowaccess(::Type{ThrowingRows}) = true
Tables.rows(r::ThrowingRows) = r
Tables.schema(::ThrowingRows) = Tables.Schema((:a,), (Int,))
Base.iterate(::ThrowingRows, state=1) =
    state == 1 ? ((a=1,), 2) : throw(WRITER_SENTINEL)

@testset "CSV writer" begin
    tbl = (a=[1, 2, 3], b=[1.5, missing, -2.0], c=["x", "y,z", "q\"r"],
           d=[Date(2024, 1, 2), Date(2024, 3, 4), Date(2024, 5, 6)])

    # round-trip: values survive File(write(table))
    out = str(io -> W.write(io, tbl))
    f = CSV.File(IOBuffer(out))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    @test isequal(Tables.getcolumn(f, :b), [1.5, missing, -2.0])
    @test String.(Tables.getcolumn(f, :c)) == ["x", "y,z", "q\"r"]
    @test Tables.getcolumn(f, :d) == tbl.d

    # Plain content quotes only fields that need it.
    plain = (x=[1, 2], y=["ab", "c,d"], z=[1.25, -3.5])
    @test str(io -> W.write(io, plain)) ==
          "x,y,z\n1,ab,1.25\n2,\"c,d\",-3.5\n"

    # RowWriter uses the same renderer for every supported writer option.
    rowtable = (id=[1, 2], text=["a,b", "q\"r"], value=[1.25, missing],
                date=[Date(2024, 1, 2), Date(2025, 3, 4)])
    rowkwargs = [
        (;),
        (; delim=';', quotechar='\'', escapechar='\'', newline="\r\n"),
        (; openquotechar='<', closequotechar='>', escapechar='\\'),
        (; quotestyle=:all),
        (; floatformat="%.3f"),
        (; dateformat="dd/mm/yyyy"),
        (; decimal=',', delim=';'),
        (; missingstring="NA"),
        (; bom=true),
    ]
    for kwargs in rowkwargs
        bytes = str(io -> W.write(io, rowtable; kwargs...))
        @test join(CSV.RowWriter(rowtable; kwargs...)) == bytes
        cols = AbstractVector[values(rowtable)...]
        opts = W._writeopts(; kwargs...)
        expected = Vector{UInt8}(join(CSV.RowWriter(rowtable; kwargs..., writeheader=false, bom=false)))
        @test W._renderblock(cols, 1, 2, opts) == expected
    end
    for writeheader in (false, true)
        bytes = str(io -> W.write(io, rowtable; writeheader, bom=true))
        @test join(CSV.RowWriter(rowtable; writeheader, bom=true)) == bytes
    end
    bytes = str(io -> W.write(io, rowtable; header=["a", "b", "c", "d"]))
    @test join(CSV.RowWriter(rowtable; header=["a", "b", "c", "d"])) == bytes
    optionkw = (; quotestrings=true,
                transform=(col, value) -> col == 1 ? value + 10 : value,
                bufsize=32)
    bytes = str(io -> W.write(io, (id=[1, 2], text=["x", "y"]); optionkw...))
    @test bytes == "\"id\",\"text\"\n11,\"x\"\n12,\"y\"\n"
    @test join(CSV.RowWriter((id=[1, 2], text=["x", "y"]); optionkw...)) == bytes
    widenames = Tuple(Symbol("c", j) for j in 1:40)
    widetable = NamedTuple{widenames}(Tuple(fill(j, 2) for j in 1:40))
    seen = Int[]
    W.write(IOBuffer(), widetable; ntasks=8,
            transform=(col, value) -> (push!(seen, col); value))
    @test seen == repeat(collect(1:40), 2)
    @test_throws ArgumentError W.write(IOBuffer(), (a=[12345],);
                                       header=false, bufsize=4)
    @test_throws ArgumentError collect(CSV.RowWriter((a=[12345],);
                                                      writeheader=false, bufsize=4))
    sized = CSV.RowWriter(rowtable)
    @test Base.IteratorSize(typeof(sized)) isa Base.HasLength
    @test length(sized) == 3
    @test size(sized) == (3,)
    @test length(CSV.RowWriter(rowtable; writeheader=false)) == 2
    emptybom = CSV.RowWriter(NamedTuple(); writeheader=false, bom=true)
    @test Base.IteratorSize(typeof(emptybom)) isa Base.HasLength
    @test length(emptybom) == 1
    @test size(emptybom) == (1,)
    @test collect(emptybom) == ["\ufeff"]

    # Schema-free streams are prefetched once for names. The cached result is
    # also the first output row, including for stateful one-shot iterators.
    rowtype = NamedTuple{(:a, :b), Tuple{Int, String}}
    oneshot = NoSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1)
    @test collect(CSV.RowWriter(oneshot)) == ["a,b\n", "1,x\n", "2,y\n"]
    @test Base.IteratorSize(typeof(CSV.RowWriter(
        NoSchemaRows(rowtype[(a=1, b="x")], 1)))) isa Base.SizeUnknown
    @test isempty(collect(CSV.RowWriter(NoSchemaRows(rowtype[], 1))))
    @test collect(CSV.RowWriter(NoSchemaRows(rowtype[], 1); header=["a", "b"])) == ["a,b\n"]
    @test_throws ArgumentError CSV.RowWriter(NoSchemaRows(rowtype[(a=1, b="x")], 1);
                                              header=["only"])

    # CSV.write must not turn SizeUnknown row sources into columns or consume
    # the schema-probing first row. Known and inferred schemas both stream.
    known = KnownSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1)
    knownio = IOBuffer()
    @test W.write(knownio, known) === knownio
    @test String(take!(knownio)) == "a,b\n1,x\n2,y\n"
    inferred = NoSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1)
    inferredio = IOBuffer()
    W.write(inferredio, inferred)
    @test String(take!(inferredio)) == "a,b\n1,x\n2,y\n"
    emptyio = IOBuffer()
    W.write(emptyio, NoSchemaRows(rowtype[], 1); header=["a", "b"])
    @test String(take!(emptyio)) == "a,b\n"
    gzipio = IOBuffer()
    W.write(gzipio, NoSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1);
            compress=:gzip)
    @test isopen(gzipio) && iswritable(gzipio)
    @test String(transcode(GzipDecompressor, take!(gzipio))) == "a,b\n1,x\n2,y\n"
    rowerrorio = IOBuffer()
    rowerror = try
        W.write(rowerrorio,
                NoSchemaRows([(a="ok",), (a=WriterExplodes(),)], 1);
                compress=:gzip)
        nothing
    catch err
        err
    end
    @test rowerror === WRITER_SENTINEL
    @test isopen(rowerrorio) && iswritable(rowerrorio)
    @test String(transcode(GzipDecompressor, take!(rowerrorio))) == "a\nok\n"
    for compress in (:none, :gzip)
        io = IOBuffer()
        err = try
            W.write(io, ThrowingRows(); compress)
        catch e
            e
        end
        @test err === WRITER_SENTINEL
        @test isopen(io)
        bytes = take!(io)
        @test String(compress === :gzip ? transcode(GzipDecompressor, bytes) : bytes) == "a\n1\n"
    end
    calls = Tuple{Int, Any}[]
    transformio = IOBuffer()
    W.write(transformio, NoSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1);
            transform=(column, value) -> (push!(calls, (column, value)); value))
    @test calls == [(1, 1), (2, "x"), (1, 2), (2, "y")]
    @test String(take!(transformio)) == "a,b\n1,x\n2,y\n"
    appendio = IOBuffer()
    write(appendio, "a,b\n0,z\n")
    W.write(appendio, NoSchemaRows(rowtype[(a=1, b="x"), (a=2, b="y")], 1);
            append=true, bom=true)
    @test String(take!(appendio)) == "a,b\n0,z\n1,x\n2,y\n"
    @test_throws ArgumentError W.write(IOBuffer(),
        NoSchemaRows([(a="too long",)], 1); bufsize=4)

    mktempdir() do dir
        path = joinpath(FilePathsBase.Path(dir), "rows.csv")
        @test W.write(path, (a=[1, 2],)) === path
        @test read(string(path), String) == "a\n1\n2\n"
        base = joinpath(FilePathsBase.Path(dir), "parts.csv")
        parts = PartitionedTable([(a=[1],), (a=[2],)])
        @test W.write(base, parts; partition=true) === base
        @test read(string(base) * "_1", String) == "a\n1\n"
        @test read(string(base) * "_2", String) == "a\n2\n"
    end

    # Partition writes use the same bounded task ring as row blocks. The bound
    # applies to active sink writes and failures retain their original object.
    pstate = PartitionWriteState(ReentrantLock(), 0, 0, 0)
    psinks = [PartitionSink(pstate, false) for _ in 1:40]
    oneparts = OneShotPartitions([(a=[i],) for i in 1:40], 1)
    ptable = PartitionedTable(oneparts)
    @test W.write(psinks, ptable; partition=true, ntasks=2) === psinks
    @test oneparts.next == 41
    @test pstate.highwater <= min(2, Threads.nthreads())
    @test pstate.active == 0
    failstate = PartitionWriteState(ReentrantLock(), 0, 0, 0)
    failsinks = [PartitionSink(failstate, true), PartitionSink(failstate, false)]
    caughtpartition = try
        W.write(failsinks, PartitionedTable([(a=[1],), (a=[2],)]);
                partition=true, ntasks=2)
        nothing
    catch err
        err
    end
    @test caughtpartition === PARTITION_SENTINEL
    @test failstate.active == 0
    @test failstate.completed >= min(2, Threads.nthreads())
    @test_throws ArgumentError W.write(
        [IOBuffer()],
        PartitionedTable(OneShotPartitions([(a=[1],), (a=[2],)], 1));
        partition=true, ntasks=2)
    @test_throws ArgumentError W.write(
        [IOBuffer(), IOBuffer()],
        PartitionedTable(OneShotPartitions([(a=[1],)], 1));
        partition=true, ntasks=2)

    # Block rows adapt to a practical byte target. This wide shape uses the
    # staged renderer and has about a 1 MiB output row.
    largenames = Tuple(Symbol("large", j) for j in 1:33)
    largecell = repeat("x", 32 << 10)
    largetable = NamedTuple{largenames}(ntuple(_ -> fill(largecell, 6), 33))
    largecolumns = Tables.columns(largetable)
    largecols = AbstractVector[Tables.getcolumn(largecolumns, nm) for nm in largenames]
    largeopts = W._writeopts(bufsize=2 << 20)
    _, largebound = W._preparewritecolumns(largecols, length(largecols[1]), largeopts)
    largerows = W._writerblockrows(largebound, largeopts, W._identity_transform)
    @test largerows <= 4
    largeone = str(io -> W.write(io, largetable; bufsize=2 << 20, ntasks=1))
    largeparallel = str(io -> W.write(io, largetable; bufsize=2 << 20, ntasks=4))
    @test largeparallel == largeone

    # determinism across thread splits
    big = (n=collect(1:50_000), s=[string("v", i % 97) for i in 1:50_000])
    @test str(io -> W.write(io, big; ntasks=1)) == str(io -> W.write(io, big; ntasks=8))

    # Stateful transforms cross fixed-size render blocks without changing the
    # documented row-major callback order.
    transformed_n = W.WRITE_BLOCK_ROWS * 2 + 17
    transformed = (a=collect(1:transformed_n), b=collect(-1:-1:-transformed_n))
    calls = Tuple{Int, Int}[]
    transformed_bytes = str() do io
        W.write(io, transformed; ntasks=8,
                transform=(column, value) -> (push!(calls, (column, value)); value))
    end
    @test transformed_bytes == str(io -> W.write(io, transformed; ntasks=8))
    @test calls == [(column, transformed[column == 1 ? :a : :b][row])
                    for row in 1:transformed_n for column in 1:2]

    # An error in a later parallel block keeps its original exception type.
    bad = Any["ok" for _ in 1:(W.WRITE_BLOCK_ROWS + 1)]
    bad[end] = nothing
    @test_throws ArgumentError W.write(IOBuffer(), (a=bad,); ntasks=4,
                                       writeheader=false)
    exploding = Any[0 for _ in 1:(W.WRITE_BLOCK_ROWS + 1)]
    exploding[end] = WriterExplodes()
    caught = try
        W.write(IOBuffer(), (a=exploding,); ntasks=4, writeheader=false)
        nothing
    catch err
        err
    end
    @test caught === WRITER_SENTINEL

    # An IO sink is written at its current position: earlier content is kept,
    # and a later-block render error leaves the blocks emitted before it.
    prefixed = IOBuffer()
    write(prefixed, "prefix\n")
    @test_throws ArgumentError W.write(prefixed, (a=bad,); ntasks=4,
                                       writeheader=false)
    @test String(take!(prefixed)) == "prefix\n" * repeat("ok\n", W.WRITE_BLOCK_ROWS)

    # quotestyle
    q = (s=["plain", "with,delim", "wi\"th"],)
    @test str(io -> W.write(io, q; quotestyle=:all)) ==
          "\"s\"\n\"plain\"\n\"with,delim\"\n\"wi\"\"th\"\n"
    @test str(io -> W.write(io, q; quotestyle=:minimal)) ==
          "s\nplain\n\"with,delim\"\n\"wi\"\"th\"\n"
    @test_throws ArgumentError str(io -> W.write(io, q; quotestyle=:none))
    @test str(io -> W.write(io, (s=["a", "b"],); quotestyle=:none)) == "s\na\nb\n"
    empties = (id=[1, 2], s=Union{Missing, String}["", missing])
    emptyout = str(io -> W.write(io, empties))
    @test emptyout == "id,s\n1,\"\"\n2,\n"
    emptyfile = CSV.File(IOBuffer(emptyout); types=Dict(:s => String))
    @test isequal(Any[x === missing ? missing : String(x) for x in emptyfile.s],
                  Any["", missing])
    @test_throws ArgumentError str(io -> W.write(io, (s=[""],); quotestyle=:none))
    # leading/trailing whitespace quotes under :minimal (round-trip safety)
    ws = str(io -> W.write(io, (s=[" pad "],)))
    @test ws == "s\n\" pad \"\n"
    @test String(Tables.getcolumn(CSV.File(IOBuffer(ws)), :s)[1]) == " pad "

    # floatformat (issue #492 surface)
    ff = str(io -> W.write(io, (x=[1.23456, 2.0],); floatformat="%.2f"))
    @test ff == "x\n1.23\n2.00\n"
    @test str(io -> W.write(io, (x=[1.25],); floatformat="%.2f", decimal=',', delim=';')) ==
          "x\n1,25\n"

    # dateformat + decimal + missingstring + delim + newline
    s = str(io -> W.write(io, (d=[Date(2024, 1, 2)], x=[1.5], m=[missing]);
                          dateformat="dd/mm/yyyy", decimal=',', missingstring="NA",
                          delim=';', newline="\r\n"))
    @test s == "d;x;m\r\n02/01/2024;1,5;NA\r\n"

    # escapechar distinct from quotechar
    s = str(io -> W.write(io, (s=["a\"b"],); escapechar='\\'))
    @test s == "s\n\"a\\\"b\"\n"

    # append / writeheader: an IO sink is never rewound or truncated. `append`
    # only decides the header default, so a log already on the stream and a
    # prefix the caller wrote both survive (`julia script.jl > log`).
    io = buf()
    W.write(io, (a=[1],))
    W.write(io, (a=[2],); append=true)
    @test String(take!(io)) == "a\n1\n2\n"
    io = IOBuffer()
    write(io, "caller prefix\n")
    W.write(io, (a=[1],))
    @test String(take!(io)) == "caller prefix\na\n1\n"
    mktemp() do path, stream
        println(stream, "log line 1")
        W.write(stream, (a=[1],))
        println(stream, "log line 2")
        close(stream)
        @test read(path, String) == "log line 1\na\n1\nlog line 2\n"
    end
    @test str(io -> W.write(io, (a=[1],); header=false)) == "1\n"
    @test str(io -> W.write(io, (a=[1],); header=true)) == "a\n1\n"
    @test_throws ArgumentError str(io -> W.write(io, (a=[1],);
                                               header=false, writeheader=true))
    @test_throws ArgumentError str(io -> W.write(io, (a=[nothing],)))
    @test str(io -> W.write(io, (a=[nothing],);
                            transform=(_, value) -> something(value, missing))) == "a\n\n"
    io = IOBuffer()
    CSV.write(io)((a=[1],))
    @test String(take!(io)) == "a\n1\n"

    # bom
    s = str(io -> W.write(io, (a=[1],); bom=true))
    @test codeunits(s)[1:3] == UInt8[0xef, 0xbb, 0xbf]
    @test codeunits(str(io -> W.write(io, (a=[1],); bom=true, writeheader=false)))[1:3] ==
          UInt8[0xef, 0xbb, 0xbf]
    @test !startswith(str(io -> W.write(io, (a=[1],); append=true,
                                       writeheader=true, bom=true)), '\ufeff')

    # gzip: by extension and explicitly; File auto-decompresses both
    dir = mktempdir()
    gzpath = joinpath(dir, "t.csv.gz")
    W.write(gzpath, tbl)
    f = CSV.File(gzpath)
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    raw = read(gzpath)
    @test raw[1] == 0x1f && raw[2] == 0x8b
    io = buf()
    W.write(io, tbl; compress=:gzip)
    @test isopen(io)
    @test iswritable(io)
    f = CSV.File(take!(io))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    io = buf()
    W.write(io, tbl; compress=true)
    @test Tables.getcolumn(CSV.File(take!(io)), :a) == [1, 2, 3]
    emptygzip = IOBuffer()
    W.write(emptygzip, NamedTuple(); compress=:gzip, writeheader=false)
    @test isopen(emptygzip)
    @test iswritable(emptygzip)
    @test isempty(transcode(GzipDecompressor, take!(emptygzip)))
    gziperror = IOBuffer()
    @test_throws ArgumentError W.write(gziperror, (a=bad,); compress=:gzip,
                                       ntasks=4, writeheader=false)
    @test isopen(gziperror)
    @test iswritable(gziperror)
    @test transcode(GzipDecompressor, take!(gziperror)) ==
          codeunits(repeat("ok\n", W.WRITE_BLOCK_ROWS))
    failingsink = FailingWriterSink(true)
    sinkerror = try
        W.write(failingsink, (a=[1],); compress=:gzip)
        nothing
    catch err
        err
    end
    @test sinkerror === WRITER_SINK_SENTINEL
    @test isopen(failingsink)
    @test iswritable(failingsink)
    plain_gzpath = joinpath(dir, "plain.csv.gz")
    W.write(plain_gzpath, (a=[1],); compress=false)
    @test read(plain_gzpath, String) == "a\n1\n"

    # partition: one sink per partition, parallel
    parts = Tables.partitioner([(a=[1, 2],), (a=[3, 4],)])
    p1, p2 = joinpath(dir, "p1.csv"), joinpath(dir, "p2.csv")
    W.write([p1, p2], parts; partition=true)
    @test Tables.getcolumn(CSV.File(p1), :a) == [1, 2]
    @test Tables.getcolumn(CSV.File(p2), :a) == [3, 4]
    partitionbase = joinpath(dir, "partition-base")
    generated = W.write(partitionbase,
                        Tables.partitioner([(a=[5],), (a=[6],)]);
                        partition=true, compress=false)
    @test generated == [partitionbase * "_1", partitionbase * "_2"]
    @test CSV.File(generated[1]).a == [5] && CSV.File(generated[2]).a == [6]
    gzipbase = joinpath(dir, "partition.csv.gz")
    extensionparts = W.write(gzipbase,
                             Tables.partitioner([(a=[7],), (a=[8],)]);
                             partition=true)
    @test extensionparts == [gzipbase * "_1", gzipbase * "_2"]
    @test read(extensionparts[1])[1:2] == UInt8[0x1f, 0x8b]
    @test CSV.File(extensionparts[2]).a == [8]
    many = Tables.partitioner([(part=fill(i, 200), s=["p$(i),r$(j)" for j in 1:200])
                               for i in 1:12])
    paths = [joinpath(dir, "part-$i.csv.gz") for i in 1:12]
    @test W.write(paths, many; partition=true) === paths
    @test all(i -> begin
        pf = CSV.File(paths[i])
        pf.part == fill(i, 200) && String(pf.s[end]) == "p$(i),r200"
    end, eachindex(paths))

    # types beyond the basics: Bool, Int128, unicode
    s = str(io -> W.write(io, (b=[true, false], w=[Int128(2)^100, Int128(-1)], u=["αβ", "cd"])))
    f = CSV.File(IOBuffer(s))
    @test Tables.getcolumn(f, :b) == [true, false]
    @test Tables.getcolumn(f, :w) == [Int128(2)^100, Int128(-1)]
    @test String.(Tables.getcolumn(f, :u)) == ["αβ", "cd"]
    float_edges = [0.0, -0.0, Inf, -Inf, NaN, nextfloat(0.0),
                   floatmax(Float64), floatmin(Float64)]
    s = str(io -> W.write(io, (id=collect(eachindex(float_edges)), x=float_edges)))
    f = CSV.File(IOBuffer(s); types=[Int64, Float64])
    @test isequal(collect(f.x), float_edges)

    # header override + writeheader=false
    @test str(io -> W.write(io, (a=[1],); header=["renamed"])) == "renamed\n1\n"
    @test str(io -> W.write(io, (a=[1],); writeheader=false)) == "1\n"
    numericdialect = str(io -> W.write(io, (n=[-12], v=[3]); delim='-'))
    @test numericdialect == "n-v\n\"-12\"-3\n"
    numericfile = CSV.File(IOBuffer(numericdialect); delim='-', types=[Int64, Int64])
    @test numericfile.n == [-12] && numericfile.v == [3]
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2]); header=["one"]))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2]);
                                               header=["one", "two", "three"]))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2, 3])))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1],); ntasks=0))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1],); quotechar='α'))

    # Keyword hygiene: unknown keywords are ArgumentErrors, one-character
    # strings spell characters, quotestyle accepts a string, and
    # missingstring=nothing means the empty spelling.
    @test_throws ArgumentError W.write(IOBuffer(), (a=[1],); foo=1)
    @test_throws ArgumentError CSV.RowWriter((a=[1],); foo=1)
    @test str(io -> W.write(io, (a=["x,y"],); quotechar="'")) == "a\n'x,y'\n"
    @test str(io -> W.write(io, (a=["x"],); quotestyle="all")) == "\"a\"\n\"x\"\n"
    @test str(io -> W.write(io, (a=[1, missing],); missingstring=nothing)) == "a\n1\n\n"
    @test_throws ArgumentError W._writeopts(; quotechar="ab")
    @test_throws ArgumentError W._writeopts(; decimal='é')
    for decimal in (('0':'9')..., '+', '-', 'e', 'E', '"')
        @test_throws ArgumentError W.write(IOBuffer(), (x=[1.25],); decimal)
    end
    @test_throws ArgumentError W.write(IOBuffer(), (x=[1.25],); decimal='!', escapechar='!')
    @test_throws ArgumentError W.write(IOBuffer(), (x=[1.25],); decimal=']', openquotechar='[', closequotechar=']')

    # Multi-byte delimiters write on the tuple, staged, header, and row paths,
    # quote conservatively on their first byte, and round-trip.
    multi = (a=[1, 2], b=["x::y", "z"])
    multiout = str(io -> W.write(io, multi; delim="::"))
    @test multiout == "a::b\n1::\"x::y\"\n2::z\n"
    @test join(CSV.RowWriter(multi; delim="::")) == multiout
    multifile = CSV.File(IOBuffer(multiout); delim="::")
    @test multifile.a == [1, 2] && String.(multifile.b) == ["x::y", "z"]
    wide = NamedTuple{Tuple(Symbol("c", j) for j in 1:40)}(Tuple(fill(j, 3) for j in 1:40))
    wideout = str(io -> W.write(io, wide; delim="||"))
    @test wideout == str(io -> W.write(io, Tables.rowtable(wide); delim="||"))
    @test String.(Tables.columnnames(CSV.File(IOBuffer(wideout); delim="||"))) ==
          ["c$j" for j in 1:40]
    @test str(io -> W.write(io, (a=[1], b=["x"]); delim='§')) == "a§b\n1§x\n"
    @test_throws ArgumentError W._writeopts(; delim="")
    @test_throws ArgumentError W._writeopts(; delim="a\nb")
    @test_throws ArgumentError W._writeopts(; delim="\"")

    for src in ("a,b\n", "a,b\n1,2\n"), compress in (:none, :gzip)
        emptychunks = CSV.Chunks(IOBuffer(src); limit=0)
        io = IOBuffer()
        W.write(io, emptychunks; compress, header=[:x, :y])
        bytes = take!(io)
        @test String(compress === :gzip ? transcode(GzipDecompressor, bytes) : bytes) == "x,y\n"
        @test str(io -> W.write(io, emptychunks; append=true)) == ""
        @test_throws ArgumentError W.write(IOBuffer(), emptychunks; header=[:x])
    end
    # CSV.Chunks streams every batch under one header.
    chunks = CSV.Chunks(IOBuffer("a,b\n1,2\n3,4\n5,6\n"); chunkbytes=4)
    @test count(_ -> true, chunks) == 3
    @test str(io -> W.write(io, chunks)) == "a,b\n1,2\n3,4\n5,6\n"
    @test str(io -> W.write(io, chunks; writeheader=false)) == "1,2\n3,4\n5,6\n"
    @test str(io -> W.write(io, chunks; header=["x", "y"], delim=';')) == "x;y\n1;2\n3;4\n5;6\n"
    mktempdir() do dir
        chunkpath = joinpath(dir, "chunks.csv")
        @test W.write(chunkpath, chunks) == chunkpath
        @test read(chunkpath, String) == "a,b\n1,2\n3,4\n5,6\n"
        upper = joinpath(dir, "upper.CSV.GZ")
        W.write(upper, (a=[1],))
        @test read(upper)[1:2] == UInt8[0x1f, 0x8b]
        @test CSV.File(upper).a == [1]
    end

    # Seeded dialect fuzz: the parser is the oracle. Each table includes a
    # nonmissing key, so a missing one-column cell cannot become an ignored
    # blank row. Strings cover every structural byte and the empty/missing
    # distinction.
    rng = MersenneTwister(0x21c5)
    atoms = Union{Missing, String}[
        missing, "", "plain", "with,comma", "with;semi", "with\ttab",
        "quote\"", "single'", "slash\\", "has\rCR", "has\nLF",
        "has\r\nCRLF", " leading", "trailing ", "\tboth\t", "λ漢🙂",
        "<open", "close>", "a|b",
    ]
    dialects = [
        (; delim=',', newline='\n', quotechar='"', escapechar='"'),
        (; delim=';', newline="\r\n", quotechar='"', escapechar='\\'),
        (; delim='\t', newline='\n', quotechar='\'', escapechar='\''),
        (; delim='|', newline="\r\n", openquotechar='<', closequotechar='>', escapechar='\\'),
    ]
    for dialect in dialects, _ in 1:8
        n = rand(rng, 1:80)
        table = (id=collect(1:n),
                 x=randn(rng, n) .* 10.0 .^ rand(rng, -20:20, n),
                 flag=rand(rng, Bool, n),
                 text=[rand(rng, atoms) for _ in 1:n])
        bytes = str(io -> W.write(io, table; ntasks=rand(rng, 1:8), dialect...))
        @test join(CSV.RowWriter(table; dialect...)) == bytes
        f = CSV.File(IOBuffer(bytes);
                   delim=dialect.delim,
                   quotechar=get(dialect, :quotechar, '"'),
                   openquotechar=get(dialect, :openquotechar, nothing),
                   closequotechar=get(dialect, :closequotechar, nothing),
                   escapechar=dialect.escapechar,
                   types=[Int64, Float64, Bool, String])
        got = Tables.columns(f)
        @test isequal((collect(Tables.getcolumn(got, :id)),
                       collect(Tables.getcolumn(got, :x)),
                       collect(Tables.getcolumn(got, :flag)),
                       Any[v === missing ? missing : String(v)
                           for v in Tables.getcolumn(got, :text)]),
                      (table.id, table.x, table.flag,
                       Any[v === missing ? missing : String(v) for v in table.text]))
    end
end

@testset "contiguous string bytes preserve quoting" begin
    # Compare the optimized representations with a separate CSV encoder.
    # Include every short-payload size, the inline/view boundary, UTF-8,
    # embedded structural bytes, missing, and empty strings.
    values = Union{Missing, String}[missing, "", "λ漢🙂", " leading", "trailing "]
    for n in vcat(1:20, 63:65, 127:129, 255:257)
        push!(values, repeat("x", n))
        for b in (",", "\"", "\n", "\r", "\\", "<", ">"), pos in (0, n ÷ 2, n)
            push!(values, repeat("x", pos) * b * repeat("y", n - pos))
        end
    end
    function encoded(s, delim, oq, cq, e, style)
        s === missing && return ""
        structural = any(c -> c in (first(delim), oq, cq, '\n', '\r'), s)
        quoted = style === :all || isempty(s) || structural ||
                 startswith(s, " ") || endswith(s, " ")
        style === :none && return s
        quoted || return s
        escaped = join((c == cq || (e != cq && c == e)) ? string(e, c) : string(c) for c in s)
        return string(oq, escaped, cq)
    end
    for (delim, oq, cq, e) in ((",", '"', '"', '"'), ("::", '<', '>', '\\')),
        style in (:minimal, :all, :none)
        selected = style === :none ? filter(values) do s
            s === missing || (!isempty(s) && !any(c -> c in (first(delim), oq, cq, '\n', '\r'), s))
        end : values
        expected = join(string(i, delim, encoded(s, delim, oq, cq, e, style), '\n')
                        for (i, s) in enumerate(selected))
        strings = CSV.DataStrings.StringVector{Union{Missing, CSV.DataString}}(selected)
        for col in (selected, strings), nt in (1, 4)
            table = (id=collect(eachindex(selected)), text=col)
            @test str(io -> CSV.write(io, table; writeheader=false, ntasks=nt,
                delim, openquotechar=oq, closequotechar=cq, escapechar=e,
                quotestyle=style)) == expected
        end
    end
    for s in ("", "has,comma", "has\nnewline", "\""), value in (s, CSV.DataString(s))
        @test_throws ArgumentError CSV.write(IOBuffer(), (x=[value],);
            writeheader=false, quotestyle=:none)
    end
end

@testset "date syntax obeys the quote policy" begin
    dt = DateTime(2024, 1, 2, 3, 4, 5, 123)
    for x in (dt, Timestamp(dt)), q in ('T', ':', '.')
        bytes = str(io -> W.write(io, (a=[x],); quotechar=q, decimal=','))
        field = split(bytes, '\n')[2]
        @test startswith(field, string(q)) && endswith(field, string(q))
        f = W.File(IOBuffer(bytes); delim=',', quotechar=q, decimal=',', types=typeof(x), on_error=:error)
        @test only(f.a) == x
        @test_throws ArgumentError W.write(IOBuffer(), (a=[x],); quotechar=q, decimal=',', quotestyle=:none)
    end
end

@testset "float syntax obeys the quote policy" begin
    # https://github.com/JuliaData/CSV.jl/issues/1062: a dialect whose
    # delimiter or quote character can appear in a float rendering — including
    # a `decimal` that is also the delimiter — quotes the cell instead of
    # writing the digits raw, like every other numeric type already does.
    issue = str(io -> W.write(io, Tables.table([1.23 4.56]); decimal=',', delim=','))
    @test issue == "Column1,Column2\n\"1,23\",\"4,56\"\n"
    # Let the reader infer the types: quoted numbers must still be numbers.
    back = W.File(IOBuffer(issue); decimal=',', on_error=:error)
    @test eltype(back.Column1) === Float64 && eltype(back.Column2) === Float64
    @test back.Column1 == [1.23] && back.Column2 == [4.56]

    @test W._writeopts().floatfast
    values = Float64[1.5, -2.25e10, 1e-9, 0.0, -0.0, 1.0e100, NaN, Inf, -Inf]
    for kwargs in ((; delim='.'), (; decimal=',', delim=','), (; delim=';', decimal=';'),
                   (; delim='e'), (; delim='N'), (; delim='f'), (; quotechar='e'),
                   (; quotechar='-'), (; openquotechar='1', closequotechar='2'))
        @test !W._writeopts(; kwargs...).floatfast
        bytes = str(io -> W.write(io, (a=values, b=values); kwargs...))
        f = W.File(IOBuffer(bytes); kwargs..., on_error=:error)
        @test eltype(f.a) === Float64 && eltype(f.b) === Float64
        @test all(isequal.(f.a, values)) && all(isequal.(f.b, values))
        # :none cannot spell these cells at all, and says so
        @test_throws ArgumentError W.write(IOBuffer(), (a=values,); kwargs..., quotestyle=:none)
    end
    # Float32/Float16 and the staged path use the same policy.
    for T in (Float32, Float16)
        bytes = str(io -> W.write(io, (a=T[1.5, 2.25],); delim='.'))
        @test bytes == "a\n\"1.5\"\n\"2.25\"\n"
    end
    @test join(W.RowWriter(Tables.table([1.23 4.56]); decimal=',', delim=',')) == issue
    # A rendering that happens not to contain the structural byte stays
    # unquoted: the bytes are checked, not the dialect.
    @test str(io -> W.write(io, (a=[NaN], b=[2.0]); delim='n')) == "anb\nNaNn2.0\n"
end

@testset "Time columns use the shared clock renderer" begin
    rng = MersenneTwister(20260915)
    clocks = Time[Time(0), Time(23, 59, 59, 999, 999, 999),
                  Time(1, 2, 3, 120), Time(1, 2, 3, 0, 1), Time(1, 2, 3, 0, 0, 1)]
    append!(clocks, [Time(Nanosecond(rand(rng, Int64))) for _ in 1:10_000])
    expected = "id,clock\n" * join(string(i, ',', x, '\n') for (i, x) in enumerate(clocks))
    table = (id=collect(eachindex(clocks)), clock=clocks)
    @test str(io -> W.write(io, table; ntasks=1)) == expected
    @test str(io -> W.write(io, table; ntasks=4)) == expected
    @test join(W.RowWriter(table)) == expected
    nullable = (id=[1, 2, 3], clock=Union{Missing, Time}[clocks[1], missing, clocks[2]])
    @test str(io -> W.write(io, nullable)) == "id,clock\n1,00:00:00\n2,\n3,23:59:59.999999999\n"
    @test str(io -> W.write(io, table; transform=(j, x) -> x, ntasks=4)) == expected
    for q in (':', '.')
        bytes = str(io -> W.write(io, (clock=[clocks[2]],); quotechar=q, decimal=','))
        @test only(W.File(IOBuffer(bytes); quotechar=q, decimal=',', types=Time).clock) == clocks[2]
        @test_throws ArgumentError W.write(IOBuffer(), (clock=[clocks[2]],);
            quotechar=q, decimal=',', quotestyle=:none)
    end
    @test str(io -> W.write(io, (clock=[Time(1, 2, 3)],); dateformat="HH:MM")) == "clock\n01:02\n"
end

@testset "bounded ordered writer scheduler" begin
    # Count completed, not-yet-emitted blocks. This tests the actual retained
    # block bound without depending on allocator or RSS measurements.
    guard = ReentrantLock()
    live = Ref(0)
    highwater = Ref(0)
    emitted = Int[]
    renderblock = function (block, slot)
        1 <= slot <= 3 || error("slot $slot outside the ring")
        lock(guard) do
            live[] += 1
            highwater[] = max(highwater[], live[])
        end
        return UInt8[block]
    end
    emitblock = function (bytes)
        push!(emitted, Int(only(bytes)))
        lock(guard) do
            live[] -= 1
        end
    end
    W._ordered_parallel_blocks!(emitblock, renderblock, 37, 3)
    @test emitted == collect(1:37)
    @test highwater[] <= 3
    @test live[] == 0

    # A failed ordered block waits for every task that was already started.
    finished = Ref(0)
    failrender = function (block, _)
        try
            block == 1 && error("scheduled failure")
            return UInt8[block]
        finally
            lock(guard) do
                finished[] += 1
            end
        end
    end
    @test_throws ErrorException W._ordered_parallel_blocks!(_ -> nothing,
                                                             failrender, 20, 3)
    @test finished[] == 3
end

@testset "staged renderer: direct paths are byte-identical to the Base spellings" begin
    o = W._writeopts()
    @test W._writeopts(bufsize=big(typemax(Int)) + 1).bufsize == typemax(Int)
    function render(x)
        st = W.ColStage()
        W._stagecolumn!(st, [x], 1, 1, o)
        direct = W._WriteBuffer()
        W._appendcell!(direct, x, o)
        @test resize!(direct.bytes, direct.len) == st.bytes
        return String(copy(st.bytes))
    end
    # integers: every fixed width at its extremes and around zero
    for T in (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
        for x in (typemin(T), typemax(T), zero(T), one(T), T(9), T(10), T(99), T(100))
            @test render(x) == string(x)
        end
        rng = MersenneTwister(1)
        okall = true
        for _ in 1:2_000
            x = rand(rng, T)
            okall &= render(x) == string(x)
        end
        @test okall
    end
    @test render(big(10)^40) == string(big(10)^40)
    # dates: adversarial years and every millisecond value
    # Use non-machine integer widths so the helpers stay valid when Dates
    # returns Int64 on a 32-bit Julia process.
    dateparts = UInt8[]
    W._appendyear!(dateparts, Int32(-1))
    push!(dateparts, UInt8('-'))
    W._append2!(dateparts, Int16(7))
    @test String(dateparts) == "-0001-07"
    for y in (-12345, -1, 0, 1, 99, 999, 1000, 2024, 9999, 10000, 123456), m in (1, 12), d in (1, 28)
        @test render(Date(y, m, d)) == string(Date(y, m, d))
    end
    okall = true
    for ms in 0:999
        x = DateTime(2020, 1, 1, 0, 0, 0, ms)
        okall &= render(x) == string(x)
    end
    @test okall
    rng = MersenneTwister(2)
    okall = true
    for _ in 1:20_000
        x = DateTime(rand(rng, -100:12000), rand(rng, 1:12), rand(rng, 1:28),
                     rand(rng, 0:23), rand(rng, 0:59), rand(rng, 0:59), rand(rng, 0:999))
        okall &= render(x) == string(x)
    end
    @test okall
    # timestamps: every resolution, trailing zeros of the fraction removed
    okall = true
    for P in (Nanosecond, Microsecond, Millisecond, Second), _ in 1:5_000
        x = Timestamp{P}(Dates.UTInstant(P(rand(rng, Int64))))
        okall &= render(x) == string(x)
    end
    for frac in (0, 1, 10, 100, 1_000, 120_000_000, 999_999_999, 500_000_000)
        x = Timestamp{Nanosecond}(2020, 1, 2, 3, 4, 5, 0, 0, frac)
        okall &= render(x) == string(x)
    end
    @test okall
    # floats: shortest round-trip, incl. specials, and decimal=','
    # Exact binary fractions take the short-decimal path. Check both sides
    # of its denominator and coefficient limits against Base's formatter,
    # including neighboring floats that must still use general reduction.
    rng = MersenneTwister(20260915)
    fractions_ok = true
    for k in 1:12
        limit = UInt64(999_999_999_999_999) ÷ UInt64(5)^k
        numerators = vcat(UInt64[1, 3, 7, limit - 1, limit, limit + 1],
                          rand(rng, UInt64(1):max(UInt64(2), limit), 200))
        for n in numerators
            x = ldexp(Float64(n), -k)
            for y in (x, -x, prevfloat(x), nextfloat(x))
                fractions_ok &= render(y) == string(y)
            end
        end
    end
    @test fractions_ok
    okall = true
    for _ in 1:20_000
        x = reinterpret(Float64, rand(rng, UInt64))
        okall &= render(x) == string(x)
    end
    @test okall
    for x in (0.0, -0.0, 1.0, 1e10, 1e-10, Inf, -Inf, NaN, 1.7976931348623157e308, 5e-324, 1.0f0, Float16(1.5))
        @test render(x) == string(x)
    end
    # The fixed/scientific notation decision is shared by all float widths.
    # Exhaust Float16 and cover Float32 bit patterns plus notation boundaries.
    @test all(render(reinterpret(Float16, bits)) == string(reinterpret(Float16, bits))
              for bits in UInt16(0):typemax(UInt16))
    @test all(render(x) == string(x) for x in reinterpret(Float32, rand(rng, UInt32, 20_000)))
    for T in (Float16, Float32, Float64), exponent in -5:6
        x = T(10.0^exponent)
        for y in (prevfloat(x), x, nextfloat(x), -prevfloat(x), -x, -nextfloat(x))
            @test render(y) == string(y)
        end
    end
    oc = W._writeopts(; decimal=',', delim=';')
    st = W.ColStage(); W._stagecolumn!(st, [1.5, 2.25e10], 1, 2, oc)
    @test String(copy(st.bytes)) == "1,52,25e10"
    # bools, missings, and the union split
    @test render(true) * render(false) == "truefalse"
    st = W.ColStage(); W._stagecolumn!(st, Union{Int32, Missing}[1, missing, -7], 1, 3, o)
    @test String(copy(st.bytes)) == "1-7" && st.ends == [1, 1, 3]
    # whole-table byte identity across thread counts and vs an independent
    # per-cell reference written with Base spellings only
    rng = MersenneTwister(3)
    n = 20_000
    tbl = (id = collect(1:n), s = [rand(rng, ("a", "b,c", "d\"e", " lead", "")) for _ in 1:n],
           f = rand(rng, n), d = [Date(2020) + Day(i) for i in 1:n],
           t = [DateTime(2020) + Millisecond(i * 7) for i in 1:n],
           ts = [Timestamp{Nanosecond}(2020, 1, 1) + Nanosecond(i * 7_919_001) for i in 1:n],
           m = [rand(rng) < 0.2 ? missing : rand(rng, Int64) for _ in 1:n], b = rand(rng, Bool, n))
    function refcell(io, x::AbstractString)
        needsquote = isempty(x) || startswith(x, ' ') || endswith(x, ' ') ||
                     any(c -> c in (',', '"', '\n', '\r'), x)
        needsquote ? print(io, '"', replace(x, "\"" => "\"\""), '"') : print(io, x)
    end
    refcell(io, ::Missing) = nothing
    refcell(io, x) = print(io, x)
    ref = IOBuffer()
    for r in 1:n
        for (j, nm) in enumerate(keys(tbl))
            refcell(ref, tbl[nm][r])
            j < length(tbl) && write(ref, ',')
        end
        write(ref, '\n')
    end
    refbytes = take!(ref)
    cols = AbstractVector[values(tbl)...]
    @test W._renderblock(cols, 1, n, o) == refbytes
    for nt in (1, 3, 8)
        io = IOBuffer(); W.write(io, tbl; ntasks=nt, writeheader=false)
        @test take!(io) == refbytes
    end
    # gzip streams block by block; the member must be complete and decodable
    io = IOBuffer(); W.write(io, tbl; compress=:gzip, ntasks=4, writeheader=false)
    @test transcode(GzipDecompressor, take!(io)) == refbytes
end

@testset "DataString cells write the same bytes as String cells" begin
    # the DataString fast path (view bytes or a stack copy of an inline
    # payload) must agree with the String path on every quoting decision
    values = ["category 7", "a longer category value 12", "x", "", "quoted, value", " lead",
              "trail ", "mid\"quote", "twelve chars", "thirteen char", "line\nbreak", "tab\tin",
              "λ漢🙂", "a"^12, "b"^13, "\"", "  ", "c,", ",d", "e\r"]
    quoted = join(("\"" * replace(v, "\"" => "\"\"") * "\"" for v in values), "\n")
    parsed = CSV.File(IOBuffer("s\n" * quoted * "\n"))
    @test parsed.s isa CSV.DataStringVector && collect(parsed.s) == values
    for kwargs in ((;), (; quotestyle=:all), (; delim=';'), (; delim="::", quotechar='\''),
                   (; escapechar='\\'), (; newline="\r\n"))
        a = str(io -> W.write(io, (s=values,); kwargs...))
        b = str(io -> W.write(io, (s=parsed.s,); kwargs...))
        @test a == b
        c = str(io -> W.write(io, (s=Union{Missing, String}[missing; values],); kwargs...))
        d = str(io -> W.write(io, (s=CSV.File(IOBuffer("s\n\n" * quoted * "\n"); ignoreemptyrows=false).s,); kwargs...))
        @test c == d
    end
end

@testset "writer renderer corpus parity" begin
    # Check direct descriptors and staged fallback columns against the public
    # row iterator, including mixed direct/staged blocks and subranges.
    corpus = AbstractVector[]
    for T in (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
        push!(corpus, T[typemin(T), typemax(T), 0, 1])
    end
    for T in (Float16, Float32, Float64)
        push!(corpus, T[-0.0, Inf, -Inf, NaN])
        push!(corpus, T[1.5, 0.0001, 1e4, 0.0])
    end
    append!(corpus, AbstractVector[
        [true, false, true, false],
        [Date(-1), Date(0), Date(2024), Date(10000)],
        [DateTime(2024) + Millisecond(i) for i in (0, 1, 10, 999)],
        [Timestamp{Nanosecond}(2024) + Nanosecond(i) for i in (0, 1, 1_000, 123_456_789)],
        [Timestamp{Microsecond}(9999, 12, 31) + Microsecond(i) for i in (0, 1, 999, 123_456)],
        [Time(0), Time(1), Time(2), Time(3)],
        ["", "a,b", "q\"r", "two\nlines"],
        [" lead", "tail ", "é", "a\rb"],
        BigInt[0, -1, big(10)^40, -(big(10)^40)],
        [:a, :b, :c, :d],
        Any[1, "two", missing, 4.0],
    ])
    for col in copy(corpus)
        T = eltype(col)
        T === Any && continue
        push!(corpus, Union{Missing,T}[missing, col[2], col[3], col[4]])
    end
    for T in (String1, String3, String7, String15, String31, String63, String127, String255)
        col = T.(["", "a", "b", "c"])
        push!(corpus, col, Union{Missing,T}[missing, col[2], col[3], col[4]], PooledArray(col))
    end
    push!(corpus, CSV.File(IOBuffer("x\n1.23\n2.45\n3.00\n4.56\n"); types=DataDecimals.Decimal64{2}).x)
    parsed = CSV.File(IOBuffer("s,t\nhi,x\n\"\",x\nthere,y\n,\n"); pool=false)
    push!(corpus, parsed.s, parsed.t, PooledArray(["a", "a,b", "c", ""]),
          PooledArray(Union{Missing,String}[missing, "a", "b", "a"]))
    for col in corpus, kwargs in ((;), (; delim=';', newline="\r\n", missingstring="NA"),
                                  (; quotestyle=:all))
        cols = AbstractVector[collect(1:4), col, ["x", "y,z", "", "w"]]
        table = (id=cols[1], value=col, text=cols[3])
        opts = W._writeopts(; kwargs...)
        expected = Vector{UInt8}(join(CSV.RowWriter(table; writeheader=false, kwargs...)))
        @test W._renderblock(cols, 1, 4, opts) == expected
        subset = (id=cols[1][2:3], value=col[2:3], text=cols[3][2:3])
        expected = Vector{UInt8}(join(CSV.RowWriter(subset; writeheader=false, kwargs...)))
        @test W._renderblock(cols, 2, 3, opts) == expected
    end
end

@testset "parallel gzip members, widened narrow integers, in-place float formats, row sources" begin
    tbl = (a=collect(1:20_000), b=rand(MersenneTwister(1), 20_000),
           s=["s$(i)" for i in 1:20_000], m=[isodd(i) ? missing : i for i in 1:20_000])
    plain = str(io -> W.write(io, tbl))
    for nt in (1, 2, 8)
        gz = let io = IOBuffer(); W.write(io, tbl; compress=true, ntasks=nt); take!(io) end
        @test String(transcode(GzipDecompressor, gz)) == plain
        @test String(read(GzipDecompressorStream(IOBuffer(gz)))) == plain
        f = CSV.File(gz)
        @test f.a == tbl.a && f.s == tbl.s && isequal(f.m, tbl.m)
        # append keeps adding members
        io = IOBuffer(); W.write(io, tbl; compress=true, ntasks=nt)
        W.write(io, tbl; compress=true, ntasks=nt, append=true)
        @test String(transcode(GzipDecompressor, take!(io))) == plain * str(io -> W.write(io, tbl; append=true))
    end
    emptygz = let io = IOBuffer(); W.write(io, (a=Int[],); compress=true, bom=true); take!(io) end
    @test String(transcode(GzipDecompressor, emptygz)) == "\ufeffa\n"
    # narrow integers render through the Int64 descriptors; transforms still see them
    nar = (a=Int32[1, -2], b=Union{Missing, Int16}[3, missing], c=UInt8[255, 0],
           d=UInt64[typemax(UInt64), 1])
    @test str(io -> W.write(io, nar)) == "a,b,c,d\n1,3,255,18446744073709551615\n-2,,0,1\n"
    seen = Type[]
    W.write(devnull, nar; transform=(j, v) -> (push!(seen, typeof(v)); v))
    @test seen == [Int32, Int16, UInt8, UInt64, Int32, Missing, UInt8, UInt64]
    @test str(io -> W.write(io, nar; transform=(j, v) -> v isa Integer ? v + one(v) : v)) ==
          "a,b,c,d\n2,4,0,0\n-1,,1,2\n"
    # formatted floats: decimal separator and structural bytes in the rendering
    @test str(io -> W.write(io, (x=[1.5, -2.25],); floatformat="%.2f", delim='.')) ==
          "x\n\"1.50\"\n\"-2.25\"\n"
    @test str(io -> W.write(io, (x=[1.5],); floatformat="%.3e", decimal=',', delim=';')) ==
          "x\n1,500e+00\n"
    @test str(io -> W.write(io, (x=[1.5, missing],); floatformat="%08.3f")) == "x\n0001.500\n\n"
    @test_throws ArgumentError W.write(devnull, (x=[1.5],); floatformat="%.2f", delim='.',
                                       quotestyle=:none)
    # row sources with a known schema render like the column path
    rows = Tables.rowtable(tbl)
    @test str(io -> W.write(io, rows)) == plain
    neg = (j, v) -> v isa Number ? -v : v
    @test str(io -> W.write(io, rows; transform=neg)) == str(io -> W.write(io, tbl; transform=neg))
    @test str(io -> W.write(io, (r for r in rows))) == plain    # schema-less generator
end
