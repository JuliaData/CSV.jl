# Tables.Scan pushdown integration tests.
#
# Run:  julia --startup-file=no --project=test test/scan.jl
#
# THE contract: pushing a Scan into CSV must produce the same table as
# parsing everything and applying the Scan generically with Tables.scan. That equivalence is asserted across scan shapes, chunk
# geometries, and parallelism — with one deliberate, pinned divergence:
# phase-2 type inference sees only qualifying rows, so values excluded by the
# filter cannot degrade a column's inferred type (the generic path, which must
# parse everything first, has no way to offer this).

using Test, Random, Dates, Tables, PooledArrays

using CSV
const S = CSV
const T = Tables

# Name-and-value equivalence between a CSV table and a column table.
function sametable(a, b)
    ka = collect(Symbol, Tables.columnnames(Tables.columns(a)))
    kb = collect(Symbol, Tables.columnnames(Tables.columns(b)))
    ka == kb || return false
    ca, cb = Tables.columns(a), Tables.columns(b)
    return all(isequal(collect(Tables.getcolumn(ca, nm)), collect(Tables.getcolumn(cb, nm)))
               for nm in ka)
end

@testset "Tables.Scan pushdown" begin

rng = MersenneTwister(2026)
regions = ["east", "west", "south", "north"]
rows = String[]
for i in 1:2_000
    r = rand(rng, 1:20) == 1 ? "" : rand(rng, regions)
    push!(rows, "$r,$(round(rand(rng) * 300, digits=2)),$(rand(rng, 1:50))," *
                (rand(rng, 1:10) == 1 ? "" : "note $(rand(rng, 'a':'e'))") *
                ",$(rand(rng, Bool) ? "true" : "false")")
end
csv = "region,price,qty,notes,flag\n" * join(rows, "\n") * "\n"

scans = [
    T.Scan(select = (:region, :price)),
    T.Scan(select = (:price => :cost, 3, r"^fl")),
    T.Scan(select = T.Not((:notes, :flag))),
    T.Scan(select = (:qty => Int64, :qty => Int64 => :qty2)),      # dup w/ rename
    T.Scan(filter = T.col(:price) > 150.0),
    T.Scan(filter = (T.col(:price) > 100) & T.in_(T.col(:region), ("east", "west"))),
    T.Scan(filter = T.isnull(T.col(:region)) | startswith(T.col(:notes), "note a")),
    T.Scan(filter = !T.isnull(T.col(:notes)), select = (:notes, :region)),
    T.Scan(filter = T.coleq(T.col(:flag), true), select = (:qty, :flag => :f)),
    T.Scan(limit = 17),
    T.Scan(limit = 17, offset = 5),
    T.Scan(offset = 1990),
    T.Scan(filter = T.col(:qty) >= 25, limit = 100, offset = 10,
           select = (:qty, :price => Float64 => :p)),
    T.Scan(filter = T.colne(T.col(:region), "east")),              # != never matches missing
]

@testset "contract: pushdown ≅ generic finish" begin
    for (i, scan) in enumerate(scans)
        ref = T.scan(S.parse(csv), scan)
        for cb in (256, 4096, 1 << 20), par in (false, true)
            t = S._scanraw(csv, scan; chunkbytes=cb, parallel=par,
                           ntasks=par ? 2 : 1)
            @test sametable(t, ref) || error("scan $i, chunkbytes=$cb, parallel=$par diverged")
        end
    end
end

@testset "the public door: CSV.File(source; scan=) composes with File keywords" begin
    scan = T.Scan(select = (:region, :price => Float64 => :cost, :qty), filter = T.col(:qty) > 25, limit = 50)
    f = CSV.File(IOBuffer(csv); scan)
    ref = T.scan(S.parse(csv), scan)
    @test S.names(f) == collect(keys(ref))
    @test all(isequal(collect(Tables.getcolumn(f, nm)), collect(ref[nm])) for nm in keys(ref))
    # composes with header handling, missingstring, and skipto through _prepare
    f2 = CSV.File(IOBuffer("skip me\n" * csv);
                header=2, missingstring="", scan=T.Scan(select=(:qty,), limit=3))
    @test S.names(f2) == [:qty] && length(Tables.getcolumn(f2, :qty)) == 3
    # the classic keywords for the axes a Scan owns are refused, not merged
    @test_throws ArgumentError CSV.File(IOBuffer(csv); scan, select=[:qty])
    @test_throws ArgumentError CSV.File(IOBuffer(csv); scan, types=Dict(:qty => Int64))
    @test_throws ArgumentError CSV.File(IOBuffer(csv); scan, limit=3)
    @test_throws ArgumentError CSV.File(IOBuffer(csv); scan=:notascan)
    @test_throws ArgumentError CSV.File(IOBuffer(csv); scan, transpose=true)
    # CSV.read routes the keyword through File
    nt = CSV.read(IOBuffer(csv), Tables.columntable; scan=T.Scan(select=(:region,), limit=2))
    @test keys(nt) == (:region,) && length(nt.region) == 2
    # a residual handed to Tables.scan agrees with full pushdown
    partial = CSV.File(IOBuffer(csv); scan=T.Scan(scan; filter=nothing, limit=nothing))
    @test all(isequal(collect(Tables.getcolumn(T.scan(partial, T.Scan(scan; select=nothing)), nm)),
                      collect(ref[nm])) for nm in keys(ref))
end

@testset "constant filters preserve row counts with no predicate columns" begin
    for select in (nothing, ())
        truescan = select === nothing ? T.Scan(filter=T.AlwaysTrue()) :
                   T.Scan(select=select, filter=T.AlwaysTrue())
        falsescan = select === nothing ? T.Scan(filter=T.AlwaysFalse()) :
                    T.Scan(select=select, filter=T.AlwaysFalse())
        t = S._scanraw(csv, truescan)
        @test t.nrows == 2_000
        @test S.names(t) == (select === nothing ? [:region, :price, :qty, :notes, :flag] : Symbol[])
        t = S._scanraw(csv, falsescan)
        @test t.nrows == 0
        @test S.names(t) == (select === nothing ? [:region, :price, :qty, :notes, :flag] : Symbol[])
    end
end

@testset "pushdown composes with pool (API layer) and groupmark" begin
    scan = T.Scan(select = (:region, :qty), filter = T.col(:qty) > 25)
    ref = T.scan(S.parse(csv), scan)
    f = CSV.File(IOBuffer(csv); scan, pool=true, chunkbytes=512)
    @test all(isequal(collect(Tables.getcolumn(f, nm)), collect(ref[nm])) for nm in keys(ref))
    @test Tables.getcolumn(f, :region) isa PooledArrays.PooledArray   # pooled after the masked parse
    gcsv = "a;n\nx;\"1,234\"\ny;\"22\"\nz;\"5,678\"\n"
    gscan = T.Scan(filter = T.col(:n) > 1000)
    tg = S._scanraw(gcsv, gscan; delim=';', groupmark=',')
    @test tg[:n] == [1234, 5678] && collect(tg[:a]) == ["x", "z"]

    # ignorerepeated flows through both phases of the masked parse
    padded = "region   qty\n  east   10\nwest  30 \n east    40\n"
    irkw = (delim=' ', ignorerepeated=true)
    irscan = T.Scan(select = (:region,), filter = T.col(:qty) > 25)
    ref = T.scan(S.parse(padded; irkw...), irscan)
    for cb in (8, 1 << 20), par in (false, true)
        t = S._scanraw(padded, irscan; chunkbytes=cb, parallel=par, irkw...)
        @test sametable(t, ref)
        @test collect(String, t[:region]) == ["west", "east"]
    end
end

@testset "masked inference: excluded garbage cannot degrade a type (pinned divergence)" begin
    dirty = "region,qty\neast,1\nwest,oops\neast,3\n"
    scan = T.Scan(filter = T.coleq(T.col(:region), "east"))
    t = S._scanraw(dirty, scan)
    @test t[:qty] isa Vector{Int64} && t[:qty] == [1, 3]            # pushdown: Int64
    ref = T.scan(S.parse(dirty), scan)
    @test eltype(Tables.getcolumn(ref, :qty)) != Int64              # generic path: strings
end

@testset "problems reference input rows; excluded rows do not report" begin
    dirty = "a,b\n1,x\n2,y\nbad,z\n4,w\n"
    scan = T.Scan(select = (:a => Int64, :b), filter = T.colne(T.col(:b), "z"))
    t = S._scanraw(dirty, scan)
    @test isequal(collect(t[:a]), [1, 2, 4]) && isempty(S.problems(t))   # bad row excluded ⇒ silent
    scan2 = T.Scan(select = (:a => Int64,), filter = T.colne(T.col(:b), "y"))
    t2 = S._scanraw(dirty, scan2)
    @test any(p -> p.row == 3 && p.col == 1, S.problems(t2))        # row 3 in INPUT numbering

    unclosed = "a\n1\n\"unterminated"
    @test isempty(S.problems(S.parse(unclosed; rowmask=[true, false])))
    @test any(p -> p.kind == :unclosed_quote,
              S.problems(S.parse(unclosed; rowmask=[false, true])))
end

@testset "problem streams merge once under the global cap" begin
    # The predicate sees native source values before requested output
    # conversion. Keep it numeric so this test isolates the merged cap for the
    # two retained conversion failures in a and b.
    dirty = "keep,a,b\n1,bad,1\n1,1,bad\n1,2,2\n"
    scan = T.Scan(select = (:a => Int64, :b => Int64), filter = T.col(:keep) > 0)
    for cap in 0:3
        t = S._scanraw(dirty, scan; maxproblems=cap, chunkbytes=5, parallel=true)
        @test length(S.problems(t)) == min(cap, 2)
        @test t.droppedproblems == 2 - min(cap, 2)
        @test issorted(S.problems(t); by=S.problemkey)
    end

    ragged = "a,b\n1\n2,x\n"
    t = S._scanraw(ragged, T.Scan(select=:b, filter=T.col(:a) > 0); chunkbytes=5)
    @test count(p -> p.kind == :short_row, S.problems(t)) == 1

    malformed = "\"bad,header\na,b\n"
    ref = S.parse(malformed; chunkbytes=2, parallel=false)
    t = S._scanraw(malformed, T.Scan(); chunkbytes=2, parallel=false)
    @test S.names(t) == S.names(ref)
    @test S.problemkey.(S.problems(t)) == S.problemkey.(S.problems(ref))

    commentonly = "#\"unterminated"
    ref = S.parse(commentonly; comment="#", chunkbytes=2)
    t = S._scanraw(commentonly, T.Scan(); comment="#", chunkbytes=2)
    @test S.problemkey.(S.problems(t)) == S.problemkey.(S.problems(ref))
    @test_throws ErrorException S._scanraw(malformed, T.Scan(); maxproblems=0,
                                           on_error=:error, chunkbytes=2)
end

@testset "masked stitch preserves compact positions and escaped strings" begin
    prows = ["$(i)," * (i <= 20 ? "" : "value_$(i % 3)") for i in 1:80]
    pcsv = "id,s\n" * join(prows, "\n") * "\n"
    mask = [i % 4 != 0 for i in 1:80]
    want = Union{String, Missing}[
        i <= 20 ? missing : "value_$(i % 3)" for i in 1:80 if mask[i]
    ]
    for cb in (8, 16, 32), par in (false, true)
        t = S.parse(pcsv; select=[:s], rowmask=mask, nsample=1,
                    chunkbytes=cb, parallel=par)
        @test isequal([ismissing(x) ? missing : String(x) for x in t[:s]], want)
    end

    erows = ["$(i),\"long escaped \"\"value $(i)\"\" tail\"" for i in 1:60]
    ecsv = "id,s\n" * join(erows, "\n") * "\n"
    emask = [isodd(i) || i % 7 == 0 for i in 1:60]
    eplain = S.parse(ecsv; chunkbytes=13, parallel=false)
    ewant = [String(eplain[:s][i]) for i in eachindex(emask) if emask[i]]
    for cb in (13, 29), par in (false, true)
        t = S.parse(ecsv; select=[:s], rowmask=emask, chunkbytes=cb, parallel=par)
        @test String.(t[:s]) == ewant
    end
end

@testset "limit trims diagnostics deterministically" begin
    bad = "a\n" * join(fill("bad", 20), "\n") * "\n"
    expected = Dict(0 => (0, 0), 1 => (1, 0), 2 => (1, 1), 20 => (1, 19))
    for (lim, (nitems, ndropped)) in expected
        t = S.parse(bad; types=Int64, limit=lim, maxproblems=1,
                    chunkbytes=8, parallel=true)
        @test length(S.problems(t)) == nitems
        @test t.droppedproblems == ndropped
        @test all(p -> p.row <= lim, S.problems(t))
    end

    bad2 = "a,b\n" * join(("x,y" for _ in 1:500), "\n") * "\n"
    for _ in 1:20
        err = try
            S.parse(bad2; types=Int64, limit=500, maxproblems=10_000,
                    chunkbytes=16, parallel=true, on_error=:error)
            nothing
        catch e
            e
        end
        @test err isa ErrorException
        @test occursin("data row 1, column 1", sprint(showerror, err))
    end
    @test S.parse(bad; types=Int64, limit=0, on_error=:error).nrows == 0

    unclosed = "a\n1\n2\n\"unterminated"
    for lim in 0:2
        @test isempty(S.problems(S.parse(unclosed; limit=lim, chunkbytes=100)))
    end
    @test any(p -> p.kind == :unclosed_quote,
              S.problems(S.parse(unclosed; limit=3, chunkbytes=100)))

    escaped = "a\nx\ny\n\"long " * repeat("q", 40) * " \"\"escaped\"\" tail\"\n"
    t = S.parse(escaped; limit=2, chunkbytes=1 << 20, parallel=false)
    @test String.(t[:a]) == ["x", "y"]
    @test isempty(t[:a].extra)                                    # excluded row was not materialized

    # Direct String chunks own private escaped buffers. A limit inside a chunk
    # must concatenate and rebase only included rows, including with a reused
    # prebuilt index.
    directvalues = [isodd(i) ? "escaped value $i with \"quote\" tail" :
                               "plain view-backed value $i tail" for i in 1:40]
    directcells = [occursin('\"', v) ? "\"" * replace(v, "\"" => "\"\"") * "\"" : v
                   for v in directvalues]
    directcsv = "id,s\n" * join(("$i,$(directcells[i])" for i in 1:40), "\n") * "\n"
    directbuf = Vector{UInt8}(codeunits(directcsv))
    directindex = S.index(directbuf, S.Dialect(); chunkbytes=256, parallel=false)
    directindex.chunks[1].firstdatarow += 1
    directcounts = S.nrows.(directindex.chunks)
    directbases = cumsum([0; directcounts[1:end - 1]])
    boundary = findfirst(k -> k < length(directcounts) && directcounts[k] > 1,
                         eachindex(directcounts))
    @test boundary !== nothing
    directlimit = directbases[boundary] + directcounts[boundary] - 1
    @test directlimit > 0 && directlimit < sum(directcounts)
    @test directlimit ∉ cumsum(directcounts)
    escapedincluded = [v for v in directvalues[1:directlimit] if occursin('\"', v)]
    expectedextra = Vector{UInt8}(codeunits(join(escapedincluded)))
    for par in (false, true)
        t = S.parse(directbuf; index=directindex, header=[:id, :s], select=[:s],
                    limit=directlimit, parallel=par, nsample=1)
        @test String.(t[:s]) == directvalues[1:directlimit]
        @test t[:s].extra == expectedextra
        @test all(i -> (S.csbufidx(t[:s].payloads[i]) == 1) == occursin('\"', directvalues[i]),
                  1:directlimit)
    end
end

@testset "limit restricts inference and parsing to retained rows" begin
    dirty = "a,b\n1,x\n2,y\noops,z\n"
    scan = T.Scan(limit=2)
    for cb in (8, 16, 1 << 20), par in (false, true)
        t = S._scanraw(dirty, scan; chunkbytes=cb, parallel=par)
        @test t[:a] isa Vector{Int64}
        @test t[:a] == [1, 2]
        @test String.(t[:b]) == ["x", "y"]
        @test isempty(S.problems(t))
    end
end

@testset "errors: contradictions, not gaps" begin
    @test_throws ArgumentError S._scanraw(csv, T.Scan(select = :nope))
    @test_throws ArgumentError S._scanraw(csv, T.Scan(select = (:qty => Int64, :qty => Float64)))
    normalized = T.Scan(select = (:qty => Int64,
                                  :qty => Union{Int64, Missing} => :qty2))
    @test sametable(S._scanraw(csv, normalized), T.scan(S.parse(csv), normalized))
    @test_throws ArgumentError S._scanraw(csv, T.Scan(select = :region); types=Dict(:qty => Int64))
    @test_throws ArgumentError S._scanraw(csv, T.Scan(); limit=3)
    @test_throws ArgumentError S._scanraw(csv, T.Scan(); rowmask=fill(true, 2_000))
    @test_throws ArgumentError S._scanraw(csv, T.Scan(); index=nothing)
    # validate=false: unmatched reference quietly drops
    t = S._scanraw(csv, T.Scan(select = (:nope, :qty), validate = false))
    @test S.names(t) == [:qty]
end

@testset "driver primitives directly (select/limit/rowmask/index)" begin
    plain = S.parse(csv)
    t = S.parse(csv; select=[:price, :region])
    @test S.names(t) == [:region, :price]                           # file order kept
    @test isequal(collect(t[:region]), collect(plain[:region]))
    for lim in (0, 1, 7, 1999, 2000, 5000), cb in (256, 1 << 20)
        t = S.parse(csv; limit=lim, chunkbytes=cb)
        @test t.nrows == min(lim, 2000)
        @test isequal(collect(t[:qty]), collect(plain[:qty])[1:t.nrows])
    end
    mask = [i % 7 == 0 for i in 1:2000]
    for cb in (256, 1 << 20), par in (false, true)
        t = S.parse(csv; rowmask=mask, chunkbytes=cb, parallel=par)
        @test t.nrows == count(mask)
        @test isequal(collect(t[:price]), collect(plain[:price])[findall(mask)])
    end
    @test_throws ArgumentError S.parse(csv; limit=3, rowmask=mask)
    @test_throws ArgumentError S.parse(csv; rowmask=[true])
    @test_throws ArgumentError S.parse(csv; select=[:nope])
    @test_throws ArgumentError S.parse(csv; limit=-1)
end

end # testset

@testset "front-door Scan sources (File(src; scan=))" begin
    using CodecZlib
    csv = "a,b,c\n" * join(("$(i),$(i / 2),v$(i % 7)_abcdefghijklmnop" for i in 1:30_000), '\n') * "\n"
    bytes = Vector{UInt8}(codeunits(csv))
    gz = transcode(GzipCompressor, copy(bytes))
    scan = T.Scan(select=[:c, :a], limit=10)
    ref = Tables.scan(S.parse(copy(bytes)), scan)
    refcols = Tables.columns(ref)
    for src in (copy(bytes), gz, IOBuffer(csv), IOBuffer(gz))
        t = CSV.File(src; scan)
        cols = Tables.columns(t)
        @test collect(Tables.columnnames(cols)) == collect(Tables.columnnames(refcols))
        for nm in Tables.columnnames(refcols)
            @test isequal([x isa AbstractString ? String(x) : x
                           for x in Tables.getcolumn(cols, nm)],
                          [x isa AbstractString ? String(x) : x
                           for x in Tables.getcolumn(refcols, nm)])
        end
    end
    dir = mktempdir()
    path = joinpath(dir, "source.csv")
    gzpath = joinpath(dir, "source.data")
    write(path, bytes)
    write(gzpath, gz)
    @test filesize(path) >= CSV.MMAP_THRESHOLD
    for (src, prefetch) in ((path, true), (path, false), (gzpath, true), (gzpath, false))
        t = CSV.File(src; scan, prefetch)
        @test sametable(t, ref)
        GC.gc()
        @test String(Tables.getcolumn(Tables.columns(t), :c)[1]) == "v1_abcdefghijklmnop"
    end
    misscan = T.Scan(select=[:a])
    mt = CSV.File(Vector{UInt8}(codeunits("a\nNA\n")); scan=misscan,
                  missingstring="NA")
    @test isequal(collect(Tables.getcolumn(Tables.columns(mt), :a)), [missing])
    @test_throws ArgumentError CSV.File(bytes; scan, sentinels=["NA"])
end
