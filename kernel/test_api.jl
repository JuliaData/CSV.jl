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

using Test, Dates, Tables
using CSV
isdefined(Main, :CSVApi) || include(joinpath(@__DIR__, "api.jl"))
const A = CSVApi
const K = CSVKernel

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
    against("1,2\n3,4\n"; kw=(; header=["l", "r"]))
    against("1,2\n3,4\n"; kw=(; header=[:l, :r]))
    against("my col,2x,for,,my col\n1,2,3,4,5\n"; kw=(; normalizenames=true))
    against("a,a,a_1\n1,2,3\n")                               # makeunique
    against("a,b\n")                                          # only a header
    # header row consumed even when it is the only content in early chunks
    f = A.File(IOBuffer("a,b\n1,2\n"); chunkbytes=4)
    @test collect(f.a) == [1]
end

@testset "row windowing agrees (raw-row semantics)" begin
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; limit=2))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; footerskip=2))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3))
    against("a,b\n1,2\n3,4\n5,6\n7,8\n"; kw=(; skipto=3, limit=1))
    against("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3, footerskip=1))
    against("a,b\n#skip\n1,2\n3,4\n"; kw=(; comment="#", skipto=3))   # comments COUNT
    against("junk\nmore junk\na,b\n1,2\n"; kw=(; header=3))
    against("a,b\n1,2\n"; kw=(; limit=0))
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); skipto=1)
    @test A.File(IOBuffer("a,b\n1,2\n"); footerskip=5).table.nrows == 0
end

@testset "missingstring agrees (modulo the pinned empty delta)" begin
    # align semantics for comparison: CSV.jl gets "" appended so empties stay missing
    against("a,b\nNA,1\n2,NA\n"; api=(; missingstring="NA"), csv=(; missingstring=["NA", ""]))
    against("a,b\nNA,N/A\nx,2\n"; api=(; missingstring=["NA", "N/A"]),
            csv=(; missingstring=["NA", "N/A", ""]))
    against("a\n999\n1\n"; api=(; missingstring="999"), csv=(; missingstring=["999", ""]))
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
    against(input; kw=(; select=[true, false, true]))
    against(input; kw=(; drop=[:b]))
    against(input; kw=(; drop=[2]))
    against(input; kw=(; drop=[false, true, false]))
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:a], drop=[:b])
    @test_throws ArgumentError A.File(IOBuffer(input); select=(nm, i) -> i == 1)
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:nope])
end

@testset "pooling agrees on values" begin
    vals = rand(["alpha", "beta", "gamma"], 400)
    input = "k\n" * join(vals, "\n") * "\n"
    f = against(input)                                        # both default-pool
    @test Tables.getcolumn(Tables.columns(f), :k) isa K.PooledColumn
    f = against(input; kw=(; pool=false))
    @test !(Tables.getcolumn(Tables.columns(f), :k) isa K.PooledColumn)
    against(input; kw=(; pool=true))
    against(input; kw=(; pool=0.9))
end

@testset "value options agree" begin
    against("d\n15/01/2023\n16/01/2023\n"; kw=(; dateformat="dd/mm/yyyy"))
    against("x;y\n1,5;2\n"; kw=(; delim=';', decimal=','))
    against("b\nYES\nNO\n"; kw=(; truestrings=["YES"], falsestrings=["NO"]))
    against("n;m\n1,234;5\n"; kw=(; delim=';', groupmark=','))
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
end

@testset "structural edge cases agree" begin
    against("a,b\r\n1,2\r\n3,4\r\n")                          # CRLF
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
    @test_throws BoundsError f[3]
    @test occursin("2 x 2", sprint(show, f))
    @test A.problems(f) isa Vector{K.Problem}
    @test Tables.schema(f).names == (:name, :score)
    fbad = A.File(IOBuffer("a\n\"unterminated"))
    @test any(p -> p.kind == :unclosed_quote, A.problems(fbad))
    @test occursin("problem", sprint(show, fbad))
    # columns named like internals cannot shadow the interface
    fsh = A.File(IOBuffer("table,name,lookup\n1,2,3\n"))
    @test collect(fsh.table) == [1] && collect(fsh.lookup) == [3]
    @test Tables.rowcount(fsh) == 1 && length(fsh) == 1
    @test fsh[1].name == 2
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
    # windowing composes
    @test length(collect(A.Rows(IOBuffer(input); limit=2))) == 2
    @test [r.a for r in A.Rows(IOBuffer(input); skipto=3)] == ["2", "3"]
    @test isequal([r.a for r in A.Rows(IOBuffer("a\nNA\n1\n"); missingstring="NA")],
                  [missing, "1"])
    @test_throws ArgumentError A.Rows(IOBuffer(input); pool=true)
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
