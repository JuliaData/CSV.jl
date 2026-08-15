# Writer battery: round-trips through CSVApi.File, byte determinism across
# thread counts, and byte agreement with CSV.write where semantics coincide.
using Test, Dates, Tables, CodecZlib, Random
using CSV, LegacyCSV                # LegacyCSV = the 0.10 writer, byte-parity oracle
const A = CSV.CSVApi
const W = CSV.KernelWrite

buf() = IOBuffer()
str(f) = (io = buf(); f(io); String(take!(io)))

@testset "KernelWrite" begin
    tbl = (a=[1, 2, 3], b=[1.5, missing, -2.0], c=["x", "y,z", "q\"r"],
           d=[Date(2024, 1, 2), Date(2024, 3, 4), Date(2024, 5, 6)])

    # round-trip: values survive File(write(table))
    out = str(io -> W.write(io, tbl))
    f = A.File(IOBuffer(out))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    @test isequal(Tables.getcolumn(f, :b), [1.5, missing, -2.0])
    @test String.(Tables.getcolumn(f, :c)) == ["x", "y,z", "q\"r"]
    @test Tables.getcolumn(f, :d) == tbl.d

    # byte agreement with CSV.write on plain content (both quote minimally)
    plain = (x=[1, 2], y=["ab", "c,d"], z=[1.25, -3.5])
    ours = str(io -> W.write(io, plain))
    theirs = str(io -> LegacyCSV.write(io, plain))
    @test ours == theirs

    # determinism across thread splits
    big = (n=collect(1:50_000), s=[string("v", i % 97) for i in 1:50_000])
    @test str(io -> W.write(io, big; ntasks=1)) == str(io -> W.write(io, big; ntasks=8))

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
    emptyfile = A.File(IOBuffer(emptyout); types=Dict(:s => String))
    @test isequal(Any[x === missing ? missing : String(x) for x in emptyfile.s],
                  Any["", missing])
    @test str(io -> LegacyCSV.write(io, empties)) == "id,s\n1,\n2,\n" # pinned 1.0 delta: the 0.10 writer conflates "" and missing
    @test_throws ArgumentError str(io -> W.write(io, (s=[""],); quotestyle=:none))
    # leading/trailing whitespace quotes under :minimal (round-trip safety)
    ws = str(io -> W.write(io, (s=[" pad "],)))
    @test ws == "s\n\" pad \"\n"
    @test String(Tables.getcolumn(A.File(IOBuffer(ws)), :s)[1]) == " pad "

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

    # append / writeheader
    io = buf()
    W.write(io, (a=[1],))
    seekstart(io)
    W.write(io, (a=[2],); append=true)
    @test String(take!(io)) == "a\n1\n2\n"
    io = IOBuffer()
    write(io, "stale trailing bytes")
    W.write(io, (a=[1],))
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
    f = A.File(gzpath)
    @test Tables.getcolumn(f, :a) == [1, 2, 3]
    raw = read(gzpath)
    @test raw[1] == 0x1f && raw[2] == 0x8b
    io = buf()
    W.write(io, tbl; compress=:gzip)
    f = A.File(take!(io))
    @test Tables.getcolumn(f, :a) == [1, 2, 3]

    # partition: one sink per partition, parallel
    parts = Tables.partitioner([(a=[1, 2],), (a=[3, 4],)])
    p1, p2 = joinpath(dir, "p1.csv"), joinpath(dir, "p2.csv")
    W.write([p1, p2], parts; partition=true)
    @test Tables.getcolumn(A.File(p1), :a) == [1, 2]
    @test Tables.getcolumn(A.File(p2), :a) == [3, 4]
    many = Tables.partitioner([(part=fill(i, 200), s=["p$(i),r$(j)" for j in 1:200])
                               for i in 1:12])
    paths = [joinpath(dir, "part-$i.csv.gz") for i in 1:12]
    @test W.write(paths, many; partition=true) === paths
    @test all(i -> begin
        pf = A.File(paths[i])
        pf.part == fill(i, 200) && String(pf.s[end]) == "p$(i),r200"
    end, eachindex(paths))

    # types beyond the basics: Bool, Int128, unicode
    s = str(io -> W.write(io, (b=[true, false], w=[Int128(2)^100, Int128(-1)], u=["αβ", "cd"])))
    f = A.File(IOBuffer(s))
    @test Tables.getcolumn(f, :b) == [true, false]
    @test Tables.getcolumn(f, :w) == [Int128(2)^100, Int128(-1)]
    @test String.(Tables.getcolumn(f, :u)) == ["αβ", "cd"]
    float_edges = [0.0, -0.0, Inf, -Inf, NaN, nextfloat(0.0),
                   floatmax(Float64), floatmin(Float64)]
    s = str(io -> W.write(io, (id=collect(eachindex(float_edges)), x=float_edges)))
    f = A.File(IOBuffer(s); types=[Int64, Float64])
    @test isequal(collect(f.x), float_edges)

    # header override + writeheader=false
    @test str(io -> W.write(io, (a=[1],); header=["renamed"])) == "renamed\n1\n"
    @test str(io -> W.write(io, (a=[1],); writeheader=false)) == "1\n"
    numericdialect = str(io -> W.write(io, (n=[-12], v=[3]); delim='-'))
    @test numericdialect == "n-v\n\"-12\"-3\n"
    numericfile = A.File(IOBuffer(numericdialect); delim='-', types=[Int64, Int64])
    @test numericfile.n == [-12] && numericfile.v == [3]
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2]); header=["one"]))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2]);
                                               header=["one", "two", "three"]))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1], b=[2, 3])))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1],); ntasks=0))
    @test_throws ArgumentError str(io -> W.write(io, (a=[1],); quotechar='α'))

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
        f = A.File(IOBuffer(bytes);
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
println("WRITE BATTERY OK")
