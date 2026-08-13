# Tables.Scan pushdown integration tests.
#
# Run:  julia --project=kernel kernel/test_scan.jl
#
# THE contract (from Tables.apply's docstring): pushing a Scan into the kernel
# must produce the same table as parsing everything and applying the Scan
# generically. That equivalence is asserted across scan shapes, chunk
# geometries, and parallelism — with one deliberate, pinned divergence:
# phase-2 type inference sees only qualifying rows, so values excluded by the
# filter cannot degrade a column's inferred type (the generic path, which must
# parse everything first, has no way to offer this).

using Test, Random, Dates, Tables

isdefined(Main, :KernelScan) || include(joinpath(@__DIR__, "scan.jl"))
const K = CSVKernel
const S = KernelScan
const T = Tables

# name-and-value equivalence between a kernel table and a columntable
function sametable(a, b)
    ka = collect(Symbol, Tables.columnnames(Tables.columns(a)))
    kb = collect(Symbol, Tables.columnnames(Tables.columns(b)))
    ka == kb || return false
    ca, cb = Tables.columns(a), Tables.columns(b)
    return all(isequal(collect(Tables.getcolumn(ca, nm)), collect(Tables.getcolumn(cb, nm)))
               for nm in ka)
end

@testset "KernelScan" begin

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
    T.Scan(filter = T.col(:flag) == true, select = (:qty, :flag => :f)),
    T.Scan(limit = 17),
    T.Scan(limit = 17, offset = 5),
    T.Scan(offset = 1990),
    T.Scan(filter = T.col(:qty) >= 25, limit = 100, offset = 10,
           select = (:qty, :price => Float64 => :p)),
    T.Scan(filter = T.col(:region) != "east"),                     # != never matches missing
]

@testset "contract: pushdown ≅ generic finish" begin
    for (i, scan) in enumerate(scans)
        ref = T.finish(K.parse(csv), scan)
        for cb in (256, 4096, 1 << 20), par in (false, true)
            t = S.read(csv, scan; chunkbytes=cb, parallel=par)
            @test sametable(t, ref) || error("scan $i, chunkbytes=$cb, parallel=$par diverged")
        end
    end
end

@testset "residual is empty; apply round-trips" begin
    t, residual = S.apply(csv, T.Scan(select = (:region,), filter = T.col(:qty) > 10, limit = 5))
    @test isempty(residual)
    @test T.finish(t, residual) === t
end

@testset "pushdown composes with pool and groupmark" begin
    scan = T.Scan(select = (:region, :qty), filter = T.col(:qty) > 25)
    ref = T.finish(K.parse(csv), scan)
    t = S.read(csv, scan; pool=true, chunkbytes=512)
    @test sametable(t, ref)
    @test t[:region] isa K.PooledColumn                             # masked pooling
    gcsv = "a;n\nx;\"1,234\"\ny;\"22\"\nz;\"5,678\"\n"
    gscan = T.Scan(filter = T.col(:n) > 1000)
    tg = S.read(gcsv, gscan; delim=';', groupmark=',')
    @test tg[:n] == [1234, 5678] && collect(tg[:a]) == ["x", "z"]
end

@testset "masked inference: excluded garbage cannot degrade a type (pinned divergence)" begin
    dirty = "region,qty\neast,1\nwest,oops\neast,3\n"
    scan = T.Scan(filter = T.col(:region) == "east")
    t = S.read(dirty, scan)
    @test t[:qty] isa Vector{Int64} && t[:qty] == [1, 3]            # pushdown: Int64
    ref = T.finish(K.parse(dirty), scan)
    @test eltype(Tables.getcolumn(ref, :qty)) != Int64              # generic path: strings
end

@testset "problems reference input rows; excluded rows do not report" begin
    dirty = "a,b\n1,x\n2,y\nbad,z\n4,w\n"
    scan = T.Scan(select = (:a => Int64, :b), filter = T.col(:b) != "z")
    t = S.read(dirty, scan)
    @test isequal(collect(t[:a]), [1, 2, 4]) && isempty(K.problems(t))   # bad row excluded ⇒ silent
    scan2 = T.Scan(select = (:a => Int64,), filter = T.col(:b) != "y")
    t2 = S.read(dirty, scan2)
    @test any(p -> p.row == 3 && p.col == 1, K.problems(t2))        # row 3 in INPUT numbering
end

@testset "masked stitch preserves compact positions and escaped strings" begin
    prows = ["$(i)," * (i <= 20 ? "" : "value_$(i % 3)") for i in 1:80]
    pcsv = "id,s\n" * join(prows, "\n") * "\n"
    mask = [i % 4 != 0 for i in 1:80]
    want = Union{String, Missing}[
        i <= 20 ? missing : "value_$(i % 3)" for i in 1:80 if mask[i]
    ]
    for cb in (8, 16, 32), par in (false, true)
        t = K.parse(pcsv; select=[:s], rowmask=mask, pool=true, nsample=1,
                    chunkbytes=cb, parallel=par)
        @test t[:s] isa K.PooledColumn
        @test isequal([ismissing(x) ? missing : String(x) for x in t[:s]], want)
    end

    erows = ["$(i),\"long escaped \"\"value $(i)\"\" tail\"" for i in 1:60]
    ecsv = "id,s\n" * join(erows, "\n") * "\n"
    emask = [isodd(i) || i % 7 == 0 for i in 1:60]
    eplain = K.parse(ecsv; chunkbytes=13, parallel=false)
    ewant = [String(eplain[:s][i]) for i in eachindex(emask) if emask[i]]
    for cb in (13, 29), par in (false, true)
        t = K.parse(ecsv; select=[:s], rowmask=emask, chunkbytes=cb, parallel=par)
        @test String.(t[:s]) == ewant
    end
end

@testset "errors: contradictions, not gaps" begin
    @test_throws ArgumentError S.read(csv, T.Scan(select = :nope))
    @test_throws ArgumentError S.read(csv, T.Scan(select = (:qty => Int64, :qty => Float64)))
    @test_throws ArgumentError S.read(csv, T.Scan(select = :region); types=Dict(:qty => Int64))
    # validate=false: unmatched reference quietly drops
    t = S.read(csv, T.Scan(select = (:nope, :qty), validate = false))
    @test K.names(t) == [:qty]
end

@testset "driver primitives directly (select/limit/rowmask/index)" begin
    plain = K.parse(csv)
    t = K.parse(csv; select=[:price, :region])
    @test K.names(t) == [:region, :price]                           # file order kept
    @test isequal(collect(t[:region]), collect(plain[:region]))
    for lim in (0, 1, 7, 1999, 2000, 5000), cb in (256, 1 << 20)
        t = K.parse(csv; limit=lim, chunkbytes=cb)
        @test t.nrows == min(lim, 2000)
        @test isequal(collect(t[:qty]), collect(plain[:qty])[1:t.nrows])
    end
    mask = [i % 7 == 0 for i in 1:2000]
    for cb in (256, 1 << 20), par in (false, true)
        t = K.parse(csv; rowmask=mask, chunkbytes=cb, parallel=par)
        @test t.nrows == count(mask)
        @test isequal(collect(t[:price]), collect(plain[:price])[findall(mask)])
    end
    @test_throws ArgumentError K.parse(csv; limit=3, rowmask=mask)
    @test_throws ArgumentError K.parse(csv; rowmask=[true])
    @test_throws ArgumentError K.parse(csv; select=[:nope])
    @test_throws ArgumentError K.parse(csv; limit=-1)
end

end # testset
