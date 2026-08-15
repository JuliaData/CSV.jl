# Differential battery: CSVApi (the kernel's front doors) vs the real CSV.jl.
#
# Run:  julia --project=. -t4 kernel/test_api.jl     (the root project IS dev
# CSV.jl, so `using CSV` loads the old implementation as the oracle.)
#
# Strategy: every behavior CSV.jl and the new API share is asserted by VALUE
# equality on the same input (string containers and pooling wrappers
# normalized away). The 1.0 divergences are each pinned in their own testset
# with the CSV.jl behavior shown alongside — a conscious delta, not a gap:
#   • empty unquoted cells are ALWAYS missing (custom missingstring ADDS)
#   • long rows do not widen the schema (extra fields ⇒ problem, not Column4)
#   • warnings are data (problems(f)), not log lines
#   • function-typed select/drop retired
#   • Int64 overflow that fits Int128 remains exact where CSV.jl widens to Float64

using Test, Dates, Tables, PooledArrays, CodecZlib
using CSV
isdefined(Main, :CSVApi) || include(joinpath(@__DIR__, "api.jl"))
const A = CSVApi
const K = CSVKernel

@testset "released Tables has no Scan front door" begin
    @test !isdefined(Tables, :Scan)
    @test all(m -> !occursin("Scan", string(m.sig)), methods(A.read))
end

_norm(x) = x isa AbstractString ? String(x) : x

function colvalues(f)
    names = collect(Symbol, Tables.columnnames(Tables.columns(f)))
    return names, [Any[_norm(x) for x in Tables.getcolumn(Tables.columns(f), nm)] for nm in names]
end

# CSV.jl side always runs silencewarnings=true: its warnings are our problems.
function against(input; kw=NamedTuple(), api=kw, csv=kw)
    fa = A.File(IOBuffer(input); api...)
    fc = CSV.File(IOBuffer(input); silencewarnings=true, csv...)
    na, va = colvalues(fa)
    nc, vc = colvalues(fc)
    @test na == nc
    if na == nc
        ok = isequal(va, vc)
        @test ok
        ok || @info "value mismatch" input api csv va vc
    end
    return fa
end

@testset "CSVApi" begin

@testset "values and inference agree with CSV.jl" begin
    against("a,b,c\n1,2,3\n4,5,6\n")
    against("a,b\n1.5,2\n-3.25e2,4\n")
    against("x\ntrue\nfalse\n")
    against("d,t,dt\n2024-01-02,01:02:03,2024-01-02T01:02:03\n")
    against("s\nhello\nworld\n")
    against("m,x\n,1\n,2\n")                       # all-missing column
    against("p\n1\n2.5\n")                         # int → float promotion
    against("p\n1\nx\n")                           # int → string promotion
    big = against("p\n1\n99999999999999999999999999\n"; kw=(; pool=false))
    @test eltype(big.p) === Int128
    @test collect(big.p) == Int128[1, 99999999999999999999999999]
    csvbig = CSV.File(IOBuffer("p\n1\n99999999999999999999999999\n"); pool=false)
    @test eltype(csvbig.p) === Int128
    wideinput = "p\n99999999999999999999999999\n"
    wide = A.File(IOBuffer(wideinput); pool=false)
    csvwide = CSV.File(IOBuffer(wideinput); pool=false)
    @test eltype(wide.p) === Int128
    @test wide.p[1] == Int128(99999999999999999999999999)
    @test eltype(csvwide.p) === Float64
    against("p\n9999999999999999999999999999999999999999\n"; kw=(; pool=false))
    against("q\n\"a,b\"\n\"c\nd\"\n\"e\"\"f\"\n")  # quoted delim/newline/escape
    against("u\nα\n∀\n")                           # unicode passthrough
    against("neg\n-1\n+2\n")
    against("sci\n1e3\n-2.5E-2\n")
end

@testset "dialects agree" begin
    against("a;b\n1;2\n"; kw=(; delim=';'))
    against("a\tb\n1\t2\n"; kw=(; delim='\t'))
    against("a|b\n1|2\n"; kw=(; delim='|'))
    against("a,b\n'x,y',2\n"; kw=(; quotechar='\''))
    against("a,b\n\"x\\\"y\",2\n"; kw=(; escapechar='\\'))
    against("a,b\n[x,y],2\n"; kw=(; openquotechar='[', closequotechar=']'))
    against("a,b\n#c\n1,2\n#d\n3,4\n"; kw=(; comment="#"))
    against("a,b\n\n1,2\n\n\n3,4\n")                              # empty rows dropped
    against("a,b\n\n1,2\n"; kw=(; ignoreemptyrows=false))
    against("a b\n1  2\n 3 4 \n"; kw=(; delim=' ', ignorerepeated=true))
    against("a::b\n1::2\n"; csv=(; delim="::"), api=(; delim="::"))
    # multi-byte delim + ignorerepeated
    against("a::b::::c\n1::2::3\n"; kw=(; delim="::", ignorerepeated=true))
end

@testset "delimiter sniffing agrees" begin
    for (d, s) in ((',', "a,b\n1,2\n3,4\n"), (';', "a;b\n1;2\n3;4\n"),
                   ('\t', "a\tb\n1\t2\n3\t4\n"), ('|', "a|b\n1|2\n3|4\n"))
        against(s)                                 # neither side told the delim
        spec = A.sniff(IOBuffer(s))
        @test spec.delim == d
        @test spec.header === true
        @test spec.names == [:a, :b]
    end
    # quoted delimiters cannot fool the quote-aware scorer
    spec = A.sniff(IOBuffer("a;b\n\"1;2;3;4;5\";6\n\"7;8\";9\n"))
    @test spec.delim == ';'
    # A colon repeated in Time values is not a delimiter when the header does
    # not contain it. Both front doors retain one Time column.
    against("t\n12:34:56\n13:45:00\n")
    @test A.sniff(IOBuffer("t\n12:34:56\n13:45:00\n")).delim == ','
    # Value and index options reach the post-detection parse without being sent
    # to Dialect, and a quote-cut bounded sample remains safe.
    spec = A.sniff(IOBuffer("a;b\n1,5;2\n3,5;4\n"); decimal=',', scanner=:scalar)
    @test spec.delim == ';' && spec.types == [Float64, Int64]
    spec = A.sniff(IOBuffer("a;b\n\"x\ny\";1\nz;2\n"); samplebytes=12)
    @test spec.delim == ';'
    @test A.sniff(IOBuffer("n\n1\n2\n")).header === true   # single col, text over ints
    @test A.sniff(IOBuffer("1,2\n3,4\n")).header === false # numbers all the way down
    @test_throws ArgumentError A.File(IOBuffer("a b\n1  2\n"); ignorerepeated=true)
    @test_throws ArgumentError CSV.File(IOBuffer("a b\n1  2\n"); ignorerepeated=true)
end

@testset "headers agree" begin
    against("1,2\n3,4\n"; kw=(; header=false))
    against("junk\na,b\n1,2\n"; kw=(; header=2))
    against("h1,h2\nx,y\n1,2\n"; kw=(; header=[1, 2]))
    against("h1,\nx,y\n1,2\n"; kw=(; header=[1, 2]))          # blank part → ColumnN_y
    # CSV.jl skips the comment while reading merged name parts, but starts data
    # at the raw row after `last(header)`, so the second part is also data.
    against("a,b\n#middle\nx,y\n1,2\n"; kw=(; header=[1, 2], comment="#"))
    against("a,b\n"; kw=(; header=[1, 2]))                    # partial header at EOF
    against("1,2\n3,4\n"; kw=(; header=["l", "r"]))
    against("1,2\n3,4\n"; kw=(; header=[:l, :r]))
    against("my col,2x,for,,my col\n1,2,3,4,5\n"; kw=(; normalizenames=true))
    against("a,a,a_1\n1,2,3\n")                               # makeunique
    against("a,b\n")                                          # only a header
    # header row consumed even when it is the only content in early chunks
    f = A.File(IOBuffer("a,b\n1,2\n"); chunkbytes=4)
    @test collect(f.a) == [1]
    @test_throws ArgumentError A.File(IOBuffer("a,b\nx,y\n1,2\n"); header=[1, 3])
end

@testset "row windowing agrees (raw-row semantics)" begin
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; limit=2))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; footerskip=2))
    against("a\n1\n\n2\n\n3\n"; kw=(; footerskip=2))       # empty rows COUNT
    against("a\n1\n\n2\n\n3\n"; kw=(; footerskip=2, ignoreemptyrows=false))
    against("a\n1\n#x\n2\n#y\n3\n"; kw=(; comment="#", footerskip=2))
    against("a,b\n\"x\ny\",1\nz,2\n"; kw=(; footerskip=1))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3))
    against("a,b\n1,2\n3,4\n5,6\n7,8\n"; kw=(; skipto=3, limit=1))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3, footerskip=1))
    against("a,b\n#skip\n1,2\n3,4\n"; kw=(; comment="#", skipto=3))   # comments COUNT
    against("junk\nmore junk\na,b\n1,2\n"; kw=(; header=3))
    against("a,b\n1,2\n"; kw=(; limit=0))
    against("a\n1\n2\n"; kw=(; limit=0, footerskip=1))
    fa0 = A.File(IOBuffer("a\n1\n2\n"); limit=0, footerskip=1)
    fc0 = CSV.File(IOBuffer("a\n1\n2\n"); limit=0, footerskip=1)
    @test Tables.schema(fa0).types == Tables.schema(fc0).types == (Missing,)
    against("﻿junk\n#ignore\na,b\n1,2\n3,4\n";
            kw=(; header=3, comment="#", skipto=5))
    against("a,b\n1,2\n"; kw=(; skipto=100))
    against("a,b\n1,2\n"; kw=(; header=100))
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); skipto=1)
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); limit=-1)
    @test A.File(IOBuffer("a,b\n1,2\n"); footerskip=5).table.nrows == 0
end

@testset "missingstring agrees (modulo the pinned empty delta)" begin
    # align semantics for comparison: CSV.jl gets "" appended so empties stay missing
    against("a,b\nNA,1\n2,NA\n"; api=(; missingstring="NA"), csv=(; missingstring=["NA", ""]))
    against("a,b\nNA,N/A\nx,2\n"; api=(; missingstring=["NA", "N/A"]),
            csv=(; missingstring=["NA", "N/A", ""]))
    against("a\n999\n1\n"; api=(; missingstring="999"), csv=(; missingstring=["999", ""]))
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); missingstring="N\"A")
    @test_throws ArgumentError CSV.File(IOBuffer("a\n1\n"); missingstring="N\"A")
    # the PINNED DELTA itself: ours keeps empties missing; CSV.jl makes them ""
    fa = A.File(IOBuffer("a\n\nx\n"); missingstring="NA", ignoreemptyrows=false)
    fc = CSV.File(IOBuffer("a\n\nx\n"); missingstring="NA", ignoreemptyrows=false,
                  silencewarnings=true)
    @test isequal(collect(fa.a), [missing, "x"])
    @test isequal([_norm(x) for x in Tables.getcolumn(fc, :a)], ["", "x"])
end

@testset "types agree" begin
    against("a,b\n1,2\n"; kw=(; types=Dict(:a => Float64)))
    against("a,b\n1,2\n"; kw=(; types=Dict(1 => Float64)))
    against("a,b\n1,2\n"; kw=(; types=[Float64, String]))
    against("a,b\n1,2\n"; kw=(; types=String))
    against("a,b\n1,2\n,3\n"; kw=(; types=Dict(:a => Union{Int64, Missing})))
    against("a\n1\nbad\n2\n"; kw=(; types=Int64))              # invalid → missing + diagnostic
    f = A.File(IOBuffer("a\n1\nbad\n"); types=Int64)
    @test any(p -> p.kind == :invalid_value, A.problems(f))
    @test_throws Exception A.File(IOBuffer("a\n1\nbad\n"); types=Int64, strict=true)
    @test_throws Exception CSV.File(IOBuffer("a\n1\nbad\n"); types=Int64, strict=true)
end

@testset "select and drop agree" begin
    input = "a,b,c\n1,2,3\n4,5,6\n"
    against(input; kw=(; select=[:a, :c]))
    against(input; kw=(; select=[1, 3]))
    against(input; kw=(; select=[1, 1, 3]))                  # duplicates collapse
    against(input; kw=(; select=[true, false, true]))
    against(input; kw=(; drop=[:b]))
    against(input; kw=(; drop=[2]))
    against(input; kw=(; drop=[false, true, false]))
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:a], drop=[:b])
    @test_throws ArgumentError A.File(IOBuffer(input); select=(nm, i) -> i == 1)
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:nope])
    f = against("my col,b\n1,2\n"; kw=(; normalizenames=true, select=[:my_col]))
    @test Base.names(f) == [:my_col]
end

@testset "pooling agrees on values" begin
    vals = rand(["alpha", "beta", "gamma"], 400)
    input = "k\n" * join(vals, "\n") * "\n"
    f = against(input)                                        # both default-pool
    @test Tables.getcolumn(Tables.columns(f), :k) isa PooledArrays.PooledArray
    f = against(input; kw=(; pool=false))
    @test !(Tables.getcolumn(Tables.columns(f), :k) isa PooledArrays.PooledArray)
    against(input; kw=(; pool=true))
    against(input; kw=(; pool=0.9))

    # Missing is an ordinary final pool level. Conversion remaps kernel ref 0
    # without changing the kernel-owned refs, while an all-present conversion
    # can transfer its exclusively owned refs at the File door.
    kernelmissing = K.parse("k\nx\n\ny\nx\n"; ignoreemptyrows=false, pool=true)[:k]
    oldrefs = copy(K.poolrefs(kernelmissing))
    missingpool = A._topooledarray(kernelmissing)
    @test K.poolrefs(kernelmissing) == oldrefs == UInt32[1, 0, 2, 1]
    @test all(!iszero, missingpool.refs)
    @test missingpool.pool[end] === missing
    @test missingpool.invpool[missing] == missingpool.refs[2]
    @test isequal(collect(missingpool), ["x", missing, "y", "x"])

    kernelpresent = K.parse(input; pool=true)[:k]
    presentpool = A._topooledarray(kernelpresent)
    @test presentpool.refs === K.poolrefs(kernelpresent)
    refsnapshot = copy(presentpool.refs)
    table = K.ParsedTable([:k], AbstractVector[presentpool], length(presentpool), K.Problem[], 0)
    @test A._downcast(A._materializestrings(table)).columns[1] === presentpool
    @test presentpool.refs == refsnapshot

    stringpool = A.File(IOBuffer(input); pool=true, stringtype=String).k
    @test stringpool isa PooledArrays.PooledArray{String}
end

@testset "value options agree" begin
    against("d\n15/01/2023\n16/01/2023\n"; kw=(; dateformat="dd/mm/yyyy"))
    against("x;y\n1,5;2\n"; kw=(; delim=';', decimal=','))
    against("b\nYES\nNO\n"; kw=(; truestrings=["YES"], falsestrings=["NO"]))
    against("n;m\n1,234;5\n"; kw=(; delim=';', groupmark=','))
    groupedinput = "n;m\n99,999,999,999,999,999,999,999,999;5\n"
    grouped = A.File(IOBuffer(groupedinput); delim=';', groupmark=',')
    csvgrouped = CSV.File(IOBuffer(groupedinput); delim=';', groupmark=',')
    @test eltype(grouped.n) === Int128
    @test grouped.n[1] == Int128(99999999999999999999999999)
    @test eltype(csvgrouped.n) === Float64
    against("s,t\n  x  ,1\n"; kw=(; delim=',', stripwhitespace=true))
    # NOTE: without an explicit delim, "s\n  x  \n" splits on ' ' under CSV.jl's
    # byte-divisibility detector (5 ragged columns); our sniffer requires
    # field-count consistency and keeps 1 column. Deliberate improvement.
end

@testset "stringtype=String materializes" begin
    f = A.File(IOBuffer("s,m\nx,\ny,z\n"); stringtype=String)
    @test Tables.getcolumn(Tables.columns(f), :s) isa Vector{String}
    @test eltype(Tables.getcolumn(Tables.columns(f), :m)) == Union{String, Missing}
    @test isequal(collect(f.m), [missing, "z"])

    # The bulk path must reconstruct every inline length byte for byte. It must
    # also preserve embedded NULs, malformed UTF-8, and view-size boundaries.
    payloads = [fill(UInt8('a') + UInt8(n), n) for n in 0:12]
    append!(payloads, [fill(UInt8('p'), n) for n in (15, 16, 17)])
    push!(payloads, UInt8[0x61, 0x00, 0x62])
    push!(payloads, UInt8[0x61, 0xff, 0x62])
    input = UInt8[]
    append!(input, codeunits("s,m\n"))
    for bytes in payloads
        isempty(bytes) ? append!(input, codeunits("\"\"")) : append!(input, bytes)
        append!(input, codeunits(",ok\n"))
    end
    escaped = "a long \"escaped\" value"
    append!(input, codeunits("\"a long \"\"escaped\"\" value\",\n"))
    f = A.File(IOBuffer(input); types=String, pool=false, stringtype=String)
    expected = String[String(copy(bytes)) for bytes in payloads]
    push!(expected, escaped)
    @test [collect(codeunits(x)) for x in f.s] == [collect(codeunits(x)) for x in expected]
    @test isequal(collect(f.m), [fill("ok", length(payloads)); missing])

    # Differential coverage must use the same String materialization route with
    # escaped long cells and missing values in one parse.
    differential = "s,m\n\"a long \"\"escaped\"\" value\",NA\nplain,\n"
    against(differential;
            api=(; stringtype=String, pool=false, missingstring="NA"),
            csv=(; stringtype=String, pool=false, missingstring=["NA", ""]))
end

@testset "structural edge cases agree" begin
    against("a,b\r\n1,2\r\n3,4\r\n")                          # CRLF
    against("a,b\r1,2\r3,4\r")                              # CR-only
    against("﻿a,b\n1,2\n")                               # BOM
    against("a,b\n1,2")                                       # no trailing newline
    against("a,b\n\"x\ny\",2\n")                              # quoted newline
    f = A.File(IOBuffer(""))
    @test length(f) == 0 && isempty(Base.names(f))
    # tiny chunks: same values as one-chunk parse (kernel-side determinism)
    input = "a,b\n" * join(("$(i),v$(i)" for i in 1:50), "\n") * "\n"
    ref = colvalues(A.File(IOBuffer(input)))
    for cb in (16, 64, 256)
        @test isequal(colvalues(A.File(IOBuffer(input); chunkbytes=cb)), ref)
    end
end

@testset "pinned delta: long rows do not widen the schema" begin
    fa = A.File(IOBuffer("a,b\n1,2,3\n4,5\n"))
    fc = CSV.File(IOBuffer("a,b\n1,2,3\n4,5\n"); silencewarnings=true)
    @test Base.names(fa) == [:a, :b]                          # extra field ⇒ problem
    @test any(p -> p.kind == :long_row, A.problems(fa))
    @test collect(fa.a) == [1, 4] && collect(fa.b) == [2, 5]
    @test :Column3 in Tables.columnnames(fc)                  # CSV.jl widens instead
end

@testset "File surface: rows, properties, show, problems" begin
    f = A.File(IOBuffer("name,score\nalice,1\nbob,2\n"))
    @test length(f) == 2
    @test f[1].name == "alice" && f[2].score == 2
    @test [r.name for r in f] == ["alice", "bob"]
    @test propertynames(f) == [:name, :score]
    @test collect(f.score) == [1, 2]
    @test A.rownumber(f[2]) == 2
    @test f[1][1] == "alice" && f[1][:name] == "alice"
    @test length(f[1]) == 2 && propertynames(f[1]) == [:name, :score]
    @test_throws BoundsError f[3]
    @test occursin("2 x 2", sprint(show, f))
    @test A.problems(f) isa Vector{K.Problem}
    @test Tables.schema(f).names == (:name, :score)
    @test Tables.rowaccess(A.File) && Tables.rows(f) === f
    fbad = A.File(IOBuffer("a\n\"unterminated"))
    @test any(p -> p.kind == :unclosed_quote, A.problems(fbad))
    @test occursin("problem", sprint(show, fbad))
    # columns named like internals cannot shadow the interface
    fsh = A.File(IOBuffer("table,name,lookup\n1,2,3\n"))
    @test collect(fsh.table) == [1] && collect(fsh.lookup) == [3]
    @test Tables.rowcount(fsh) == 1 && length(fsh) == 1
    @test fsh[1].name == 2
    @test Tables.columnnames(fsh[1]) == [:table, :name, :lookup]
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); ntasks=0)
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); stringtype=SubString{String})
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); silencewarnings=true)
end

@testset "header diagnostics merge before strict/capping" begin
    clean = K.ParsedTable(Symbol[], AbstractVector[], 0, K.Problem[], 0)
    same, firstproblem = A._mergeproblems(clean, nothing, 0)
    @test same === clean && firstproblem === nothing
    droppedheader = K.ProblemLog(0)
    K.pushproblem!(droppedheader, 0, 1, 1, :invalid_value, "header")
    merged, firstproblem = A._mergeproblems(clean, droppedheader, 0)
    @test isempty(merged.problems) && merged.droppedproblems == 1
    @test firstproblem !== nothing && firstproblem.kind == :invalid_value

    input = "\"bad\"x,a\nBAD,2\n"
    f = A.File(IOBuffer(input); types=Dict(1 => Int64), maxproblems=1)
    @test length(A.problems(f)) == 1
    @test first(A.problems(f)).kind == :invalid_quoted_field
    @test getfield(f, :table).droppedproblems == 1
    f0 = A.File(IOBuffer(input); types=Dict(1 => Int64), maxproblems=0)
    @test isempty(A.problems(f0)) && getfield(f0, :table).droppedproblems == 2
    @test occursin("2 problem(s) recorded — 0 retained", sprint(show, f0))
    err = try
        A.File(IOBuffer(input); types=Dict(1 => Int64), strict=true, maxproblems=0)
        nothing
    catch e
        e
    end
    @test err isa ErrorException
    @test occursin("invalid_quoted_field", sprint(showerror, err))
end

@testset "read into sinks" begin
    input = "a,b\n1,x\n2,y\n"
    ct = A.read(IOBuffer(input), Tables.columntable)
    @test ct.a == [1, 2] && String.(ct.b) == ["x", "y"]
    rt = Tables.rowtable(A.File(IOBuffer(input)))
    @test length(rt) == 2 && rt[1].a == 1
end

@testset "sources: path, IO, bytes, Cmd" begin
    path, io = mktemp()
    write(io, "a,b\n1,2\n"); close(io)
    @test collect(A.File(path).a) == [1]
    @test collect(A.File(Vector{UInt8}(codeunits("a,b\n1,2\n"))).a) == [1]
    @test collect(A.File(`cat $path`).a) == [1]
    @test_throws ArgumentError A.File("definitely-not-a-file.csv")
    rm(path)
    # Zero-byte files use the read path. Directories and FIFOs fail before a
    # read or mmap. A file exactly at the threshold uses the mmap branch.
    zeropath, zeroio = mktemp()
    close(zeroio)
    @test isempty(A.resolvesource(zeropath))
    rm(zeropath)
    dirpath = mktempdir()
    @test_throws ArgumentError A.resolvesource(dirpath)
    rm(dirpath)
    @static if Sys.isunix()
        fifodir = mktempdir()
        fifopath = joinpath(fifodir, "source.fifo")
        run(`mkfifo $fifopath`)
        @test_throws ArgumentError A.resolvesource(fifopath)
        rm(fifodir; recursive=true)
    end
    edgepath, edgeio = mktemp()
    write(edgeio, fill(UInt8('x'), A.MMAP_THRESHOLD))
    close(edgeio)
    @test length(A.resolvesource(edgepath)) == A.MMAP_THRESHOLD
    rm(edgepath)

    # A file across the mmap threshold parses identically when mapped or
    # buffered. Every lazy public surface must keep the mapping alive.
    bigpath, bigio = mktemp()
    write(bigio, "s,n\n" *
                 join(("word$(i % 977)_abcdefghijklmnop,$(i)" for i in 1:60_000), "\n") *
                 "\n")
    close(bigio)
    @test filesize(bigpath) >= A.MMAP_THRESHOLD
    mappedcol = let f = A.File(bigpath; pool=false)
        f.s
    end
    pooledcol = let f = A.File(bigpath; pool=true)
        f.s
    end
    mappedrow = first(A.Rows(bigpath))
    mappedbatch = first(A.Chunks(bigpath; ntasks=2))
    fb = A.File(bigpath; buffer_in_memory=true)
    fnoprefetch = A.File(bigpath; prefetch=false, pool=false)
    @test first(A.Rows(bigpath; buffer_in_memory=true)).s == mappedrow.s
    @test first(A.Rows(bigpath; prefetch=false)).s == mappedrow.s
    bufferedbatch = first(A.Chunks(bigpath; ntasks=2, buffer_in_memory=true))
    noprefetchbatch = first(A.Chunks(bigpath; ntasks=2, prefetch=false))
    @test colvalues(bufferedbatch) == colvalues(mappedbatch)
    @test colvalues(noprefetchbatch) == colvalues(mappedbatch)
    sm = A.sniff(bigpath)
    sb = A.sniff(bigpath; buffer_in_memory=true)
    sn = A.sniff(bigpath; prefetch=false)
    @test (sm.delim, sm.header, sm.names, sm.types) ==
          (sb.delim, sb.header, sb.names, sb.types)
    @test (sm.delim, sm.header, sm.names, sm.types) ==
          (sn.delim, sn.header, sn.names, sn.types)
    @test collect(String, mappedcol) == collect(String, Tables.getcolumn(fb, :s))
    @test collect(String, mappedcol) == collect(String, Tables.getcolumn(fnoprefetch, :s))
    @test pooledcol isa PooledArrays.PooledArray
    @test collect(Tables.getcolumn(mappedbatch, :n)) == collect(fb.n)[1:mappedbatch.nrows]
    # This also runs K.materialize while its source is a read-only mapping.
    fm = A.File(bigpath; pool=false, stringtype=String)
    @test fm.s == collect(String, mappedcol)
    GC.gc()
    @test String(mappedcol[1]) == "word1_abcdefghijklmnop"
    @test String(pooledcol[1]) == "word1_abcdefghijklmnop"
    @test mappedrow.s == "word1_abcdefghijklmnop"
    @test String(Tables.getcolumn(mappedbatch, :s)[1]) == "word1_abcdefghijklmnop"
    rm(bigpath)
end

@testset "Rows agrees with CSV.Rows" begin
    input = "a,b\n1,x\n2,\n3,z\n"
    ra = collect(A.Rows(IOBuffer(input)))
    rc = collect(CSV.Rows(IOBuffer(input)))
    @test length(ra) == length(rc) == 3
    for (x, y) in zip(ra, rc)
        @test isequal(_norm.(collect(Tables.getcolumn.(Ref(x), 1:2))),
                      _norm.(collect(Tables.getcolumn.(Ref(y), 1:2))))
    end
    @test isequal([r.b for r in A.Rows(IOBuffer(input))], ["x", missing, "z"])
    # typed access parses on demand through the kernel value layer
    typed = A.Rows(IOBuffer(input); types=Dict(:a => Int64))
    @test [r.a for r in typed] == [1, 2, 3]
    @test Tables.schema(typed).types[1] == Union{Int64, Missing}
    typedbad = A.Rows(IOBuffer("a\n1\nbad\n"); types=Union{Int64, Missing})
    csvbad = CSV.Rows(IOBuffer("a\n1\nbad\n"); types=Union{Int64, Missing},
                      silencewarnings=true)
    @test isequal([r.a for r in typedbad], [r.a for r in csvbad])
    # windowing composes
    @test length(collect(A.Rows(IOBuffer(input); limit=2))) == 2
    @test [r.a for r in A.Rows(IOBuffer(input); skipto=3)] == ["2", "3"]
    windowed = collect(A.Rows(IOBuffer(input); skipto=3, limit=1))
    @test [r.a for r in windowed] == ["2"] && A.rownumber(only(windowed)) == 1
    @test isequal([r.a for r in A.Rows(IOBuffer("a\nNA\n1\n"); missingstring="NA")],
                  [missing, "1"])
    quoted = "a,b\n\"x\ny\",1\nz,2\n"
    @test [_norm(r.a) for r in A.Rows(IOBuffer(quoted))] ==
          [_norm(r.a) for r in CSV.Rows(IOBuffer(quoted))]
    @test length(collect(A.Rows(IOBuffer(input); footerskip=2))) == 1
    footer = "a\n1\n\n2\n\n3\n"
    @test [r.a for r in A.Rows(IOBuffer(footer); footerskip=2)] ==
          [r.a for r in CSV.Rows(IOBuffer(footer); footerskip=2)]
    @test_throws ArgumentError A.Rows(IOBuffer(input); pool=true)
    @test_throws ArgumentError A.Rows(IOBuffer(input); nsample=2)
    @test_throws ArgumentError A.Rows(IOBuffer(input); maxproblems=1)
    @test_throws ArgumentError A.Rows(IOBuffer(input); select=[:a])
end

@testset "Chunks: stable schema, values concat to File" begin
    input = "a,b\n" * join(("$(i)," * (i == 40 ? "" : "v$(i)") for i in 1:60), "\n") * "\n"
    ref = colvalues(A.File(IOBuffer(input); pool=false))
    batches = collect(A.Chunks(IOBuffer(input); chunkbytes=64))
    @test length(batches) > 1
    @test all(b -> eltype(b[:a]) == eltype(batches[1][:a]), batches)      # stable
    @test all(b -> eltype(b[:b]) == eltype(batches[1][:b]), batches)      # even w/ late missing
    catted = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in batches))
              for j in (:a, :b)]
    @test isequal(catted, ref[2])
    # windowing composes with batching
    b2 = collect(A.Chunks(IOBuffer("a,b\n1,2\n3,4\n5,6\n"); chunkbytes=8, skipto=3))
    @test sum(b -> b.nrows, b2) == 2
    @test isempty(collect(A.Chunks(IOBuffer(input); chunkbytes=32, limit=0)))
    @test isempty(collect(A.Chunks(IOBuffer(input); chunkbytes=32, footerskip=60)))
    footerparts = collect(A.Chunks(IOBuffer("a\n1\n\n2\n\n3\n");
                                   chunkbytes=2, footerskip=2))
    @test reduce(vcat, (collect(b[:a]) for b in footerparts); init=Int[]) == [1, 2]
    # limit and footer windows trim the prepared chunks before the schema pass.
    for kw in ((; limit=17), (; footerskip=17), (; skipto=10, limit=13))
        file = colvalues(A.File(IOBuffer(input); pool=false, kw...))
        parts = collect(A.Chunks(IOBuffer(input); chunkbytes=32, kw...))
        got = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in parts); init=Any[])
               for j in (:a, :b)]
        @test isequal(got, file[2])
    end
    routed = "a;b\n1,5;NA\n2,5;3\n"
    file = colvalues(A.File(IOBuffer(routed); delim=';', decimal=',',
                            missingstring="NA", pool=false, scanner=:scalar))
    parts = collect(A.Chunks(IOBuffer(routed); delim=';', decimal=',',
                             missingstring="NA", chunkbytes=8, scanner=:scalar))
    got = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in parts); init=Any[])
           for j in (:a, :b)]
    @test isequal(got, file[2])
    bad = first(A.Chunks(IOBuffer("a\nBAD\nNOPE\n"); types=Int64,
                         chunkbytes=64, maxproblems=0))
    @test isempty(A.problems(bad)) && bad.droppedproblems == 2
    @test_throws ArgumentError A.Chunks(IOBuffer(input); pool=true)
    @test_throws ArgumentError A.Chunks(IOBuffer(input); select=[:a])
    @test_throws ArgumentError A.Chunks(IOBuffer(input); strict=true)
end

@testset "spec replays" begin
    input = "x;y\nalpha;1\nbeta;2\n"
    spec = A.sniff(IOBuffer(input))
    @test spec.delim == ';' && spec.header === true
    f = A.File(IOBuffer(input); delim=spec.delim, header=spec.header)
    @test Base.names(f) == [:x, :y] && collect(f.y) == [1, 2]
    @test occursin("delim=';'", sprint(show, spec))
end

end # @testset CSVApi

@testset "1.0 parity batch: gzip, typemap, dateformat/pool Dicts, downcast, transpose, deprecations" begin
    # auto-gzip: every source kind decompresses by magic bytes
    plain = "a,b\n1,x\n2,y\n"
    gz = transcode(CodecZlib.GzipCompressor, Vector{UInt8}(codeunits(plain)))
    for src in (gz, IOBuffer(gz))
        f = A.File(src)
        @test Tables.getcolumn(f, :a) == [1, 2]
    end
    gzpath = joinpath(mktempdir(), "t.csv.gz")
    write(gzpath, gz)
    @test Tables.getcolumn(A.File(gzpath), :a) == [1, 2]
    oracle = CSV.File(gzpath)
    @test Tables.getcolumn(oracle, :a) == [1, 2]

    # typemap: detected types remap; user-pinned ones don't
    input = "a,b\n1,1.5\n2,2.5\n"
    f = A.File(IOBuffer(input); typemap=Dict(Int64 => Float64))
    o = CSV.File(IOBuffer(input); typemap=Dict(Int64 => Float64))
    @test eltype(Tables.getcolumn(f, :a)) == eltype(Tables.getcolumn(o, :a)) == Float64
    f = A.File(IOBuffer(input); typemap=Dict(Int64 => String), types=Dict(:b => Float64))
    @test eltype(Tables.getcolumn(f, :b)) == Float64
    @test Tables.getcolumn(f, :a) isa AbstractVector{<:AbstractString}
    # A mapped parse type is a fixed point. Downward and cyclic maps must widen
    # instead of retrying the same rejecting type until the driver guard fires.
    for (mappedinput, tm) in (("a\n1.5\n2.5\n", IdDict(Float64 => Int64)),
                              ("a\nx\ny\n", IdDict(String => Int64)),
                              ("a\n1\n2.5\n", IdDict(Int64 => Float64,
                                                       Float64 => Int64)))
        mapped = A.File(IOBuffer(mappedinput); typemap=tm, pool=false)
        @test String.(mapped.a) == split(chomp(mappedinput), '\n')[2:end]
    end
    chained = A.File(IOBuffer("a\n1\n2\n");
                     typemap=IdDict(Int64 => Float64, Float64 => String), pool=false)
    @test chained.a isa Vector{Float64}
    pinned = A.File(IOBuffer("a\n1\n2\n"); types=Int64,
                    typemap=IdDict(Int64 => String), pool=false)
    @test pinned.a isa Vector{Int64}
    mappedmissing = A.File(IOBuffer("a\n1\n\n2\n"); ignoreemptyrows=false,
                           typemap=IdDict(Int64 => Float64), pool=false)
    @test eltype(mappedmissing.a) == Union{Float64, Missing}
    mappedpool = A.File(IOBuffer("a\n" * join((string(i % 3) for i in 1:300), '\n') * "\n");
                        typemap=IdDict(Int64 => String), pool=true)
    @test mappedpool.a isa PooledArrays.PooledArray{String}

    # per-column dateformat
    input = "d1,d2\n03/04/2020,2020-01-02\n05/06/2021,2021-07-08\n"
    f = A.File(IOBuffer(input); dateformat=Dict(:d1 => "dd/mm/yyyy"))
    o = CSV.File(IOBuffer(input); dateformat=Dict(:d1 => "dd/mm/yyyy"))
    @test Tables.getcolumn(f, :d1) == Tables.getcolumn(o, :d1) == [Date(2020, 4, 3), Date(2021, 6, 5)]
    @test Tables.getcolumn(f, :d2) == [Date(2020, 1, 2), Date(2021, 7, 8)]

    # per-column pool: Dict pools only the listed column
    input = "a,b\n" * join(("p$(i % 5),q$(i % 5)" for i in 1:5000), '\n') * "\n"
    f = A.File(IOBuffer(input); pool=Dict(:a => (1.0, 500)))
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    f = A.File(IOBuffer(input); pool=[(1.0, 500), false])
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    f = A.File(IOBuffer(input); pool=[(1.0, 500), nothing])
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    @test_throws ArgumentError A.File(IOBuffer(input); pool=Dict(:nope => true))
    @test_throws ArgumentError A.File(IOBuffer(input); pool=[true])
    @test_throws ArgumentError A.File(IOBuffer(input); pool=Dict(:a => "invalid"))

    # The pre-skip proof and parse-time degrade both bind the policy by column.
    proofinput = "a,b\n" * join(("unique$i,cat$(i % 3)" for i in 1:1000), '\n') * "\n"
    proof = A.File(IOBuffer(proofinput);
                   pool=Dict(:a => (1.0, 10), :b => (1.0, 500)))
    @test !(proof.a isa PooledArrays.PooledArray)
    @test proof.b isa PooledArrays.PooledArray
    degradeinput = "a,b\nx,u\ny,v\n"
    degraded = A.File(IOBuffer(degradeinput);
                      pool=Dict(:a => (1.0, 1), :b => (1.0, 2)))
    @test !(degraded.a isa PooledArrays.PooledArray)
    @test degraded.b isa PooledArrays.PooledArray

    # downcast (oracle agreement on eltypes and values)
    input = "a,b,c\n1,300,70000\n2,-40,100000\n"
    f = A.File(IOBuffer(input); downcast=true)
    o = CSV.File(IOBuffer(input); downcast=true)
    for nm in (:a, :b, :c)
        @test eltype(Tables.getcolumn(f, nm)) == eltype(Tables.getcolumn(o, nm))
        @test Tables.getcolumn(f, nm) == Tables.getcolumn(o, nm)
    end
    # downcast with missings keeps Union eltype
    f = A.File(IOBuffer("a\n1\n\n2\n"); downcast=true, ignoreemptyrows=false)
    @test eltype(Tables.getcolumn(f, :a)) == Union{Int8, Missing}
    for (T, lo, hi) in ((Int8, typemin(Int8), typemax(Int8)),
                        (Int16, typemin(Int16), typemax(Int16)),
                        (Int32, typemin(Int32), typemax(Int32)),
                        (Int64, typemin(Int64), typemax(Int64)))
        c = A.File(IOBuffer("a\n$lo\n$hi\n"); downcast=true).a
        @test eltype(c) == T
        @test c == T[lo, hi]
    end
    @test A.File(IOBuffer("a\n\n"); downcast=true, ignoreemptyrows=false).a isa Vector{Missing}

    # transpose: names in field 1, ragged pad, oracle value agreement
    input = "name,1,2,3\nscore,1.5,2.5,3.5\nnote,x,y\n"
    f = A.File(IOBuffer(input); transpose=true)
    o = CSV.File(IOBuffer(input); transpose=true)
    @test Tables.columnnames(Tables.columns(f)) == Tables.columnnames(Tables.columns(o))
    @test Tables.getcolumn(f, :name) == Tables.getcolumn(o, :name) == [1, 2, 3]
    @test Tables.getcolumn(f, :score) == [1.5, 2.5, 3.5]
    @test isequal(Tables.getcolumn(f, :note), ["x", "y", missing])
    @test isequal(String.(coalesce.(Tables.getcolumn(o, :note), "")),
                  String.(coalesce.(Tables.getcolumn(f, :note), "")))
    f = A.File(IOBuffer("1,2\n3,4\n"); transpose=true, header=false)
    @test Tables.getcolumn(f, :Column1) == [1, 2] && Tables.getcolumn(f, :Column2) == [3, 4]
    @test_throws ArgumentError A.File(IOBuffer(input); transpose=true, select=[:name])
    # Quoted newlines, escapes, empty rows, unicode, ragged tails, and pinned
    # types retain the same names and values as CSV.jl.
    transposedcases = [
        ("name,\"a\nb\",c\nnum,1,2\n", (;)),
        ("name,\"a\"\"b\",c\nnum,1,2\n", (;)),
        ("a,1,2\n\nb,3,4\n", (;)),
        ("α,β,γ\nδ,日,月\n", (;)),
        ("a,1,x,3\nb,2020-01-01,2020-01-02,\n",
         (; types=Dict(:a => Int64, :b => Date))),
    ]
    for (transposedinput, transposedkw) in transposedcases
        tf = A.File(IOBuffer(transposedinput); transpose=true, transposedkw...)
        to = CSV.File(IOBuffer(transposedinput); transpose=true, transposedkw...)
        @test Tables.columnnames(tf) == Tables.columnnames(to)
        for nm in Tables.columnnames(tf)
            av = Any[ismissing(x) ? missing : x isa AbstractString ? String(x) : x
                     for x in Tables.getcolumn(tf, nm)]
            ov = Any[ismissing(x) ? missing : x isa AbstractString ? String(x) : x
                     for x in Tables.getcolumn(to, nm)]
            @test isequal(av, ov)
        end
    end

    # legacy kwargs error with migration text
    for (kwname, kwval) in ((:silencewarnings, true), (:debug, true), (:lazystrings, true),
                            (:tasks, 2), (:threaded, true), (:rows_to_check, 5),
                            (:lines_to_check, 5), (:ignoreemptylines, true),
                            (:datarow, 2), (:type, Int64), (:missingstrings, ["NA"]),
                            (:dateformats, Dict(:a => "yyyy-mm-dd")),
                            (:parsingdebug, true), (:validate, false))
        err = try
            A.File(IOBuffer("a\n1\n"); kwname => kwval)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError && occursin("removed in 1.0", err.msg)
    end

    # reusebuffer: accepted and inert
    r = A.Rows(IOBuffer("a\n1\n2\n"); reusebuffer=true)
    @test length(collect(r)) == 2
end
