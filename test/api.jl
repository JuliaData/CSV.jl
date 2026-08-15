# Differential battery: CSVApi (the kernel's front doors) vs frozen CSV 0.10.
#
# Run:  julia --startup-file=no --project=test -t4 test/api.jl
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

using Test, Dates, Tables, PooledArrays, CodecZlib, InlineStrings, FilePathsBase
using CSV, LegacyCSV
const A = CSV.CSVApi
const K = CSV.CSVKernel
corpusfile(name) = joinpath(@__DIR__, "legacy", "testfiles", name)

# Minimal ordered AbstractDict for precedence tests. Base.Dict iteration order
# is not an API contract, while CSV's rule is explicitly first matching Regex.
struct OrderedTestDict <: AbstractDict{Any, Any}
    entries::Vector{Pair{Any, Any}}
end
Base.length(d::OrderedTestDict) = length(d.entries)
Base.iterate(d::OrderedTestDict, state::Int=1) =
    state > length(d.entries) ? nothing : (d.entries[state], state + 1)
# The 0.10 implementation is the behavioral ORACLE throughout this file: every
# `LegacyCSV.File`/`LegacyCSV.write` below that means "the old behavior" is spelled LegacyCSV.

@testset "Scan front door tracks the Tables proposal" begin
    # the method exists exactly when the loaded Tables carries the Scan proposal
    hasscan = isdefined(Tables, :Scan)
    @test any(m -> occursin("Scan", string(m.sig)), methods(A.read)) == hasscan
end

_norm(x) = x isa AbstractString ? String(x) : x

function colvalues(f)
    names = collect(Symbol, Tables.columnnames(Tables.columns(f)))
    return names, [Any[_norm(x) for x in Tables.getcolumn(Tables.columns(f), nm)] for nm in names]
end

# LegacyCSV.jl side always runs silencewarnings=true: its warnings are our problems.
function against(input; kw=NamedTuple(), api=kw, csv=kw)
    fa = A.File(IOBuffer(input); api...)
    fc = LegacyCSV.File(IOBuffer(input); silencewarnings=true, csv...)
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

@testset "values and inference agree with LegacyCSV.jl" begin
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
    csvbig = LegacyCSV.File(IOBuffer("p\n1\n99999999999999999999999999\n"); pool=false)
    @test eltype(csvbig.p) === Int128
    wideinput = "p\n99999999999999999999999999\n"
    wide = A.File(IOBuffer(wideinput); pool=false)
    csvwide = LegacyCSV.File(IOBuffer(wideinput); pool=false)
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
    # The initial bound grows only when it has no complete row. A normal
    # bounded sample is unchanged; a single unterminated row returns whole.
    normal = Vector{UInt8}("a,b\n1,2\n")
    @test A._sample(normal, 6) == normal[1:4]
    @test A._sample(normal, 1) == normal[1:4]
    single = Vector{UInt8}("a;b;c")
    @test A._sample(single, 1) == single
    bom = vcat(UInt8[0xef, 0xbb, 0xbf], Vector{UInt8}("a;b\n1;2\n"))
    @test A.sniff(bom; samplebytes=1).delim == ';'
    @test A.sniff(IOBuffer("n\n1\n2\n")).header === true   # single col, text over ints
    @test A.sniff(IOBuffer("1,2\n3,4\n")).header === false # numbers all the way down
    @test A.sniff(IOBuffer("Created Date\n")).delim == ','  # one header row: space is not evidence
    @test A.sniff(IOBuffer("a;b;c\n")).delim == ';'
    @test A.sniff(IOBuffer("")).delim == ','
    @test A.sniff(IOBuffer(String(UInt8[0xef, 0xbb, 0xbf]) * "a;b\n1;2\n")).delim == ';'
    # Two rows are enough field-consistency evidence, including CRLF input.
    @test A.sniff(IOBuffer("x y:a:p,q:p,q:p,q\r\n\"p:q\":b:c:d:x y")).delim == ':'
    # Equal scorers retain candidate order. The old header-max tier still
    # outranks data-only evidence; data evidence is the final fallback only
    # when that tier has no non-space candidate.
    @test A.sniff(IOBuffer("a,b;c\n1,2;3\n")).delim == ','
    @test A.sniff(IOBuffer("header, text\n1:2\n3:4\n")).delim == ','
    @test A.sniff(IOBuffer("header text\n1:2\n3:4\n")).delim == ':'
    @test_throws ArgumentError A.File(IOBuffer("a b\n1  2\n"); ignorerepeated=true)
    @test_throws ArgumentError LegacyCSV.File(IOBuffer("a b\n1  2\n"); ignorerepeated=true)
end

@testset "headers agree" begin
    against("1,2\n3,4\n"; kw=(; header=false))
    against("junk\na,b\n1,2\n"; kw=(; header=2))
    against("h1,h2\nx,y\n1,2\n"; kw=(; header=[1, 2]))
    against("h1,\nx,y\n1,2\n"; kw=(; header=[1, 2]))          # blank part → ColumnN_y
    # LegacyCSV.jl skips the comment while reading merged name parts, but starts data
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
    # non-consecutive header rows join like 0.10 (rows 1 and 3, skipping row 2)
    against("a,b\nx,y\n1,2\n3,4\n"; kw=(; header=[1, 3]))
    @test_throws ArgumentError A.File(IOBuffer("a,b\nx,y\n1,2\n"); header=[3, 1])
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
    fc0 = LegacyCSV.File(IOBuffer("a\n1\n2\n"); limit=0, footerskip=1)
    @test Tables.schema(fa0).types == Tables.schema(fc0).types == (Missing,)
    against("﻿junk\n#ignore\na,b\n1,2\n3,4\n";
            kw=(; header=3, comment="#", skipto=5))
    against("a,b\n1,2\n"; kw=(; skipto=100))
    against("a,b\n1,2\n"; kw=(; header=100))
    # 0.10 rule: default header 1 + skipto=1 means "no header, data at row 1"
    against("a,b\n1,2\n"; kw=(; skipto=1))
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); header=2, skipto=1)
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); limit=-1)
    @test A.File(IOBuffer("a,b\n1,2\n"); footerskip=5).table.nrows == 0
    # Comment rows count for header/skipto positions but not footerskip. Quoted
    # physical lines that start with '#' remain one data row.
    commentheavy = "#lead\r\nh1,h2\r\n#between \" poison\r\n1,2\r\n\"top\r\n# content\r\nbottom\",3\r\n#tail"
    f = A.File(IOBuffer(commentheavy); comment="#", header=2, skipto=4,
               footerskip=1, delim=',')
    @test Base.names(f) == [:h1, :h2]
    @test f.h1 == [1]

    limitedtype = A.File(IOBuffer("a\n1\nx\n"); limit=1)
    @test limitedtype.a isa Vector{Int64}
    @test limitedtype.a == [1]

    # A blank or comment row may sit between explicitly listed header rows.
    against("a,b\n\nA,B\n1,2\n"; kw=(; header=[1, 2]))
    against("a,b\n# gap\nA,B\n1,2\n"; kw=(; header=[1, 2], comment="#"))
end

@testset "missingstring agrees (modulo the pinned empty delta)" begin
    # align semantics for comparison: LegacyCSV.jl gets "" appended so empties stay missing
    against("a,b\nNA,1\n2,NA\n"; api=(; missingstring="NA"), csv=(; missingstring=["NA", ""]))
    against("a,b\nNA,N/A\nx,2\n"; api=(; missingstring=["NA", "N/A"]),
            csv=(; missingstring=["NA", "N/A", ""]))
    against("a\n999\n1\n"; api=(; missingstring="999"), csv=(; missingstring=["999", ""]))
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); missingstring="N\"A")
    @test_throws ArgumentError LegacyCSV.File(IOBuffer("a\n1\n"); missingstring="N\"A")
    # the PINNED DELTA itself: ours keeps empties missing; LegacyCSV.jl makes them ""
    fa = A.File(IOBuffer("a\n\nx\n"); missingstring="NA", ignoreemptyrows=false)
    fc = LegacyCSV.File(IOBuffer("a\n\nx\n"); missingstring="NA", ignoreemptyrows=false,
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
    @test_throws Exception LegacyCSV.File(IOBuffer("a\n1\nbad\n"); types=Int64, strict=true)

    # Narrow conversion is an API-door operation. Requests remain indexed by
    # file column while selected output columns remain in file order.
    selected = A.File(IOBuffer("a,b,c\n1,2,300\n3,4,500\n");
                      types=Dict(:a => Int8, :c => Int16), select=[:c, :a])
    @test Base.names(selected) == [:a, :c]
    @test selected.a == Int8[1, 3]
    @test selected.c == Int16[300, 500]
    duplicate = A.File(IOBuffer("a,b,c\n1,2,300\n");
                       types=Dict(:a => Int8, :c => Int16), select=[3, 1, 3])
    @test Base.names(duplicate) == [:a, :c]

    overflow = A.File(IOBuffer("a\n127\n128\n"); types=Int8)
    @test isequal(collect(overflow.a), [Int8(127), missing])
    @test [(p.row, p.col, p.kind) for p in A.problems(overflow)] ==
          [(2, 1, :invalid_value)]
    unsigned = A.File(IOBuffer("u\n$(typemax(UInt64))\n"); types=UInt64)
    @test unsigned.u == UInt64[typemax(UInt64)]
    declarednarrow = A.File(IOBuffer("a\n1\n2\n"); types=Union{Missing, Int8})
    @test eltype(declarednarrow.a) == Union{Missing, Int8}
    declaredstring = A.File(IOBuffer("a\nx\ny\n"); types=Union{Missing, String},
                            pool=false)
    @test eltype(declaredstring.a) == Union{Missing, K.CompactString}
    declaredpoolinput = "a\n" * join(fill("x", 40), '\n') * "\n"
    declaredpool = A.File(IOBuffer(declaredpoolinput);
                          types=Union{Missing, String}, pool=true)
    @test declaredpool.a isa PooledArrays.PooledArray
    @test eltype(declaredpool.a) == Union{Missing, String}

    # Exact keys beat Regex keys. Among Regex keys, the first matching entry in
    # the AbstractDict wins for types, dateformat, and pool.
    typemap = OrderedTestDict(Any[r"_col$" => Int16, r"^a" => Int32,
                                  :a_col => Int8])
    regexfile = A.File(IOBuffer("a_col,b_col,c\n1,2,3\n"); types=typemap)
    @test eltype(regexfile.a_col) == Int8
    @test eltype(regexfile.b_col) == Int16
    firstregex = OrderedTestDict(Any[r"^a" => Int16, r"_col$" => Int32])
    @test eltype(A.File(IOBuffer("a_col\n1\n"); types=firstregex).a_col) == Int16

    dateformats = OrderedTestDict(Any[r"^date" => "dd/mm/yyyy",
                                      r"1$" => "mm/dd/yyyy"])
    dates = A.File(IOBuffer("date1,date2\n03/04/2020,05/06/2021\n");
                   dateformat=dateformats)
    @test dates.date1 == [Date(2020, 4, 3)]
    pools = OrderedTestDict(Any[r"^a" => true, r"_col$" => false])
    pooledinput = "a_col,b_col\n" * join(fill("x,y", 40), '\n') * "\n"
    pooled = A.File(IOBuffer(pooledinput); pool=pools)
    @test pooled.a_col isa PooledArrays.PooledArray
    @test !(pooled.b_col isa PooledArrays.PooledArray)
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
    csvgrouped = LegacyCSV.File(IOBuffer(groupedinput); delim=';', groupmark=',')
    @test eltype(grouped.n) === Int128
    @test grouped.n[1] == Int128(99999999999999999999999999)
    @test eltype(csvgrouped.n) === Float64
    against("s,t\n  x  ,1\n"; kw=(; delim=',', stripwhitespace=true))
    # NOTE: without an explicit delim, "s\n  x  \n" splits on ' ' under LegacyCSV.jl's
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
    fc = LegacyCSV.File(IOBuffer("a,b\n1,2,3\n4,5\n"); silencewarnings=true)
    @test Base.names(fa) == [:a, :b]                          # extra field ⇒ problem
    @test any(p -> p.kind == :long_row, A.problems(fa))
    @test collect(fa.a) == [1, 4] && collect(fa.b) == [2, 5]
    @test :Column3 in Tables.columnnames(fc)                  # LegacyCSV.jl widens instead
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
    @test collect(A.File(codeunits("a,b\n1,2\n")).a) == [1]
    parent = Vector{UInt8}(codeunits("prefixa,b\n1,stable value\nsuffix"))
    lo = ncodeunits("prefix") + 1
    hi = lo + ncodeunits("a,b\n1,stable value\n") - 1
    viewfile = A.File(@view(parent[lo:hi]); types=Dict(:b => String), pool=false)
    fill!(@view(parent[lo:hi]), UInt8('x'))
    @test viewfile.a == [1]
    @test String(viewfile.b[1]) == "stable value"
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

@testset "Rows agrees with LegacyCSV.Rows" begin
    input = "a,b\n1,x\n2,\n3,z\n"
    ra = collect(A.Rows(IOBuffer(input)))
    rc = collect(LegacyCSV.Rows(IOBuffer(input)))
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
    csvbad = LegacyCSV.Rows(IOBuffer("a\n1\nbad\n"); types=Union{Int64, Missing},
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
          [_norm(r.a) for r in LegacyCSV.Rows(IOBuffer(quoted))]
    @test length(collect(A.Rows(IOBuffer(input); footerskip=2))) == 1
    footer = "a\n1\n\n2\n\n3\n"
    @test [r.a for r in A.Rows(IOBuffer(footer); footerskip=2)] ==
          [r.a for r in LegacyCSV.Rows(IOBuffer(footer); footerskip=2)]
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
    # pool is now supported per batch (each batch is an independent table);
    # only a single policy — Dict/vector per-column forms stay File-only
    @test Tables.getcolumn(first(A.Chunks(IOBuffer("s\n" * "x\ny\n"^50); pool=true, chunkbytes=64)), :s) isa PooledArrays.PooledArray
    @test_throws ArgumentError A.Chunks(IOBuffer(input); pool=Dict(:a => true))
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
    oracle = LegacyCSV.File(gzpath)
    @test Tables.getcolumn(oracle, :a) == [1, 2]

    # typemap: detected types remap; user-pinned ones don't
    input = "a,b\n1,1.5\n2,2.5\n"
    f = A.File(IOBuffer(input); typemap=Dict(Int64 => Float64))
    o = LegacyCSV.File(IOBuffer(input); typemap=Dict(Int64 => Float64))
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
    o = LegacyCSV.File(IOBuffer(input); dateformat=Dict(:d1 => "dd/mm/yyyy"))
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
    o = LegacyCSV.File(IOBuffer(input); downcast=true)
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
    o = LegacyCSV.File(IOBuffer(input); transpose=true)
    @test Tables.columnnames(Tables.columns(f)) == Tables.columnnames(Tables.columns(o))
    @test Tables.getcolumn(f, :name) == Tables.getcolumn(o, :name) == [1, 2, 3]
    @test Tables.getcolumn(f, :score) == [1.5, 2.5, 3.5]
    @test isequal(Tables.getcolumn(f, :note), ["x", "y", missing])
    @test isequal(String.(coalesce.(Tables.getcolumn(o, :note), "")),
                  String.(coalesce.(Tables.getcolumn(f, :note), "")))
    f = A.File(IOBuffer("1,2\n3,4\n"); transpose=true, header=false)
    @test Tables.getcolumn(f, :Column1) == [1, 2] && Tables.getcolumn(f, :Column2) == [3, 4]
    @test_throws ArgumentError A.File(IOBuffer(input); transpose=true, select=[:name])
    paddedtranspose = A.File(IOBuffer("a, 1 , \" 2 \" \nb, 3.5 , 4.5 \n");
                               transpose=true)
    @test paddedtranspose.a == [1, 2]
    @test paddedtranspose.b == [3.5, 4.5]
    # Quoted newlines, escapes, empty rows, unicode, ragged tails, and pinned
    # types retain the same names and values as LegacyCSV.jl.
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
        to = LegacyCSV.File(IOBuffer(transposedinput); transpose=true, transposedkw...)
        @test Tables.columnnames(tf) == Tables.columnnames(to)
        for nm in Tables.columnnames(tf)
            av = Any[ismissing(x) ? missing : x isa AbstractString ? String(x) : x
                     for x in Tables.getcolumn(tf, nm)]
            ov = Any[ismissing(x) ? missing : x isa AbstractString ? String(x) : x
                     for x in Tables.getcolumn(to, nm)]
            @test isequal(av, ov)
        end
    end
    # limit scopes inference as well as output. Values after the retained
    # prefix cannot promote an Int column to Float64 or String.
    limited = A.File(IOBuffer("a,1,x\n"); transpose=true, limit=1)
    @test limited.a == [1] && eltype(limited.a) == Int64
    limited = A.File(IOBuffer("a,1,2.5\n"); transpose=true, limit=1, downcast=true)
    @test limited.a == Int8[1] && eltype(limited.a) == Int8
    zero = A.File(IOBuffer("a,1,2.5\nb,3,4\n"); transpose=true, limit=0)
    @test length(zero) == 0 && all(eltype(Tables.getcolumn(zero, nm)) == Missing
                                  for nm in Tables.columnnames(zero))
    numbered = A.File(IOBuffer("skip,a,1,2,x\nskip,b,3,4,5\n");
                        transpose=true, header=2, skipto=3, limit=2)
    @test numbered.a == [1, 2] && numbered.b == [3, 4]
    explicit = A.File(IOBuffer("1,2,x\n3,4,5\n");
                        transpose=true, header=[:a, :b], limit=2)
    @test explicit.a == [1, 2] && explicit.b == [3, 4]
    ragged = A.File(IOBuffer("a,1,2,3\nb,4\n"); transpose=true, limit=2)
    @test ragged.a == [1, 2] && isequal(ragged.b, [4, missing])
    @test A.File(IOBuffer("a,1,2\n"); transpose=true, limit=99).a == [1, 2]
    @test_throws ArgumentError A.File(IOBuffer("a,1\n"); transpose=true, limit=-1)

    # legacy kwargs error with migration text
    for (kwname, kwval) in ((:silencewarnings, true), (:debug, true), (:lazystrings, true),
                            (:tasks, 2), (:threaded, true), (:rows_to_check, 5),
                            (:lines_to_check, 5), (:ignoreemptylines, true),
                            (:datarow, 2), (:type, Int64), (:missingstrings, ["NA"]),
                            (:dateformats, Dict(:a => "yyyy-mm-dd")),
                            (:parsingdebug, true))
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

    # validate=false: types/dateformat/pool keys naming absent columns are
    # ignored (0.10 semantics); the default validates, Regex misses included
    novalidate = A.File(IOBuffer("a,b,c\n1,2,3\n"); types=Dict(4 => Float64, r"_x$" => Int8),
                        dateformat=Dict(:e => "dd/mm/yyyy"), pool=Dict("f" => true),
                        validate=false)
    @test length(novalidate) == 1 && novalidate.a == [1]
    @test_throws ArgumentError A.File(IOBuffer("a,b,c\n1,2,3\n"); types=Dict(r"_x$" => Int8))
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); types=Dict(r"z" => Int), limit=0)
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); dateformat=Dict(r"z" => "yyyy"))
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); pool=Dict(r"z" => true))
    @test_throws ArgumentError A.Rows(IOBuffer("a\n1\n"); types=Dict(:zz => Int))
    @test first(A.Rows(IOBuffer("a\n1\n"); types=Dict(:zz => Int), validate=false)).a isa AbstractString
    @test first(A.Chunks(IOBuffer("a\n1\n"); types=Dict(:zz => Int), validate=false, chunkbytes=1 << 20))[:a] == [1]
    @test_throws ArgumentError A.Rows(IOBuffer("a\n1\n"); dateformat=Dict(r"z" => "yyyy"))
    @test_throws ArgumentError A.Chunks(IOBuffer("a\n1\n"); types=Dict(r"z" => Int))
    @test_throws ArgumentError A.File(IOBuffer("a,1\n"); transpose=true,
                                      types=Dict(r"z" => Int))
    @test_throws ArgumentError K.parse(Vector{UInt8}("a\n1\n"); types=Dict(r"z" => Int))
    @test length(A.File(IOBuffer("a,1\n"); transpose=true,
                        types=Dict(:z => Int, 2 => Int, r"q" => Int), validate=false)) == 1
    @test K.parse(Vector{UInt8}("a\n1\n");
                  types=Dict(:z => Int, 2 => Int, r"q" => Int), validate=false).nrows == 1
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); select=[:z], validate=false)
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); drop=[:z], validate=false)
end

@testset "CompactString hash + stringtype extension hook" begin
    # hash contract: CompactString hashes like its String, allocation-free
    col = K.parse(Vector{UInt8}(codeunits("a\n" * join(("v$(i)_" * "x"^(i % 30) for i in 1:500), '\n') * "\n")); pool=false).columns[1]
    @test all(hash(col[i]) == hash(String(col[i])) for i in eachindex(col))
    @test all(hash(col[i], UInt(7)) == hash(String(col[i]), UInt(7)) for i in eachindex(col))
    hashall(c) = (h = UInt(0); @inbounds for i in eachindex(c); h ⊻= hash(c[i]); end; h)
    hashall(col)
    @test @allocated(hashall(col)) == 0
    for n in 0:14   # every inline length + the first view lengths, incl. escaped
        s = "y"^n
        c = K.parse(Vector{UInt8}(codeunits("a\n\"" * s * "\"\n\"" * s * "\"\"z\"\n")); pool=false).columns[1]
        @test hash(c[1]) == hash(s) && hash(c[2]) == hash(s * "\"z")
    end
    d = Dict{AbstractString, Int}(String(col[3]) => 3)
    @test d[col[3]] == 3                    # CompactString finds the String key
    # stringtype validation and String path unchanged
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); stringtype=Int)
    @test eltype(Tables.getcolumn(A.File(IOBuffer("a\nx\n"); stringtype=String), :a)) == String
end

@testset "InlineStrings extension" begin
    csv = "s,t,n\n" * join(("a$(i),$(i % 3 == 0 ? "" : "longer value number $(i)"),$(i)" for i in 1:300), '\n') * "\n"
    auto = A.File(IOBuffer(csv); stringtype=InlineString)
    @test eltype(Tables.getcolumn(auto, :s)) == String7           # auto width per column
    @test eltype(Tables.getcolumn(auto, :t)) == Union{Missing, String31}
    @test String(Tables.getcolumn(auto, :t)[1]) == "longer value number 1"
    @test Tables.getcolumn(auto, :t)[3] === missing
    f = A.File(IOBuffer(csv); stringtype=String31)
    @test eltype(Tables.getcolumn(f, :s)) == String31           # pinned width
    @test_throws ArgumentError A.File(IOBuffer(csv); stringtype=String7)   # too narrow
    widths = (String1, String3, String7, String15, String31, String63, String127, String255)
    for T in widths
        n = sizeof(T) - 1
        value = "x"^n
        @test String(only(A.File(IOBuffer("s\n$value\n"); stringtype=T, pool=false).s)) == value
        @test_throws ArgumentError A.File(IOBuffer("s\n" * "x"^(n + 1) * "\n");
                                          stringtype=T, pool=false)
    end
    @test_throws ArgumentError A.File(IOBuffer("s\n" * "x"^256 * "\n");
                                      stringtype=InlineString, pool=false)
    empty = A.File(IOBuffer("s\n"); types=String, stringtype=InlineString, pool=false)
    @test isempty(empty.s) && eltype(empty.s) == String1
    allmissing = A.File(IOBuffer("id,s\n1,\n2,\n"); types=Dict(:s => String),
                        stringtype=InlineString, pool=false)
    @test eltype(allmissing.s) == Union{Missing, String1}
    @test all(ismissing, allmissing.s)
    # pooled levels take the inline type; missing joins the pool
    f = A.File(IOBuffer(csv); stringtype=InlineString, pool=Dict(:s => false, :t => (1.0, 5000)))
    pooled = Tables.getcolumn(f, :t)
    @test pooled isa PooledArrays.PooledArray
    @test eltype(pooled) == Union{Missing, String31}
    @test all(getfield(pooled, :invpool)[getfield(pooled, :pool)[i]] == UInt32(i)
              for i in eachindex(getfield(pooled, :pool)))
    # escaped (extra-backed) values and unicode
    escaped = A.File(IOBuffer("a\n\"q\"\"x\"\n\"a long \"\"escaped\"\" value\"\nαβγδεζηθ\n");
                     stringtype=InlineString)
    @test collect(String.(Tables.getcolumn(escaped, :a))) ==
          ["q\"x", "a long \"escaped\" value", "αβγδεζηθ"]
    @test eltype(Tables.getcolumn(escaped, :a)) == String31
    # oracle: values agree with 0.10's InlineString default
    o = LegacyCSV.File(IOBuffer(csv))
    for nm in (:s, :t)
        ours = Any[x === missing ? missing : String(x) for x in Tables.getcolumn(auto, nm)]
        theirs = Any[x === missing ? missing : String(x) for x in Tables.getcolumn(o, nm)]
        @test isequal(ours, theirs)
    end
end

@testset "RowWriter" begin
    t = (a=[1, 2, 3], b=["x", "y,z", missing], c=[1.5, 2.0, 3.25],
         d=[Date(2024, 1, 2), Date(2024, 3, 4), Date(2024, 5, 6)])
    lines = collect(CSV.RowWriter(t))
    @test length(lines) == 4
    @test lines[1] == "a,b,c,d\n"
    @test lines[3] == "2,\"y,z\",2.0,2024-03-04\n"
    io = IOBuffer(); CSV.write(io, t)
    @test join(lines) == String(take!(io))                    # byte-identical to write
    @test collect(CSV.RowWriter(t; writeheader=false))[1] == "1,x,1.5,2024-01-02\n"
    @test collect(CSV.RowWriter(t; header=["p", "q", "r", "s"]))[1] == "p,q,r,s\n"
    @test collect(CSV.RowWriter(t; delim=';', quotestyle=:all, floatformat="%.1f"))[2] ==
          "1;\"x\";1.5;2024-01-02\n"
    @test_throws ArgumentError collect(CSV.RowWriter(t; header=["only", "three", "names"]))
    # streams over a row-access table (a File) without materializing columns
    f = A.File(IOBuffer("a,b\n1,x\n2,y\n"))
    @test collect(CSV.RowWriter(f)) == ["a,b\n", "1,x\n", "2,y\n"]
    # 0.10 oracle agreement on plain content
    io2 = IOBuffer(); LegacyCSV.write(io2, (x=[1, 2], y=["ab", "c,d"]))
    @test join(collect(CSV.RowWriter((x=[1, 2], y=["ab", "c,d"])))) == String(take!(io2))
end

@testset "stringtype × pool matrix: File / Rows / Chunks agree" begin
    csv = "a,b,n\n" * join(("p$(i % 4),v$(i),$(i)" for i in 1:2000), '\n') * "\n"
    # File: (stringtype, pool) -> (a is pooled?, eltype of a, eltype of b)
    expect = Dict(
        (K.CompactString, true)  => (true,  String,           K.CompactString),
        (K.CompactString, false) => (false, K.CompactString,  K.CompactString),
        (String, true)           => (true,  String,           String),
        (String, false)          => (false, String,           String),
        (InlineString, true)     => (true,  String3,          String7),
        (InlineString, false)    => (false, String3,          String7),
    )
    for ((st, pl), (pooled, ea, eb)) in expect
        pool = pl ? (0.2, 500) : false
        f = A.File(IOBuffer(csv); stringtype=st, pool)
        ca, cb = Tables.getcolumn(f, :a), Tables.getcolumn(f, :b)
        @test (ca isa PooledArrays.PooledArray) == pooled
        @test eltype(ca) == ea && eltype(cb) == eb
        @test String(ca[5]) == "p1" && String(cb[5]) == "v5"
        # Chunks batches leave through the SAME door
        c = first(A.Chunks(IOBuffer(csv); ntasks=2, stringtype=st, pool))
        @test (Tables.getcolumn(c, :a) isa PooledArrays.PooledArray) == pooled
        @test eltype(Tables.getcolumn(c, :a)) == ea && eltype(Tables.getcolumn(c, :b)) == eb
    end
    # Rows: lazy views by default; stringtype materializes per cell
    r = first(A.Rows(IOBuffer(csv)))
    @test r[:a] isa K.CompactString && r.b isa K.CompactString
    @test first(A.Rows(IOBuffer(csv); stringtype=String))[:a] isa String
    @test first(A.Rows(IOBuffer(csv); stringtype=InlineString))[:b] isa String3   # per-cell smallest fit ("v1")
    @test first(A.Rows(IOBuffer(csv); stringtype=String15))[:a] isa String15
    @test Tables.schema(A.Rows(IOBuffer(csv); stringtype=String)).types[1] == Union{String, Missing}
    @test_throws ArgumentError A.Rows(IOBuffer(csv); stringtype=Int)
    # Auto-width is per column for File and per accessed cell for Rows.
    asymmetry = "s\na\nabcdef\n"
    @test eltype(A.File(IOBuffer(asymmetry); stringtype=InlineString, pool=false).s) == String7
    rowvalues = [row.s for row in A.Rows(IOBuffer(asymmetry); stringtype=InlineString)]
    @test rowvalues[1] isa String1 && rowvalues[2] isa String7
    @test_throws ArgumentError first(A.Rows(IOBuffer("s\nab\n"); stringtype=String1)).s
    # Plain views retain the input buffer. Long escaped cells own the unescaped buffer.
    plainrow = first(A.Rows(IOBuffer("s\nabcdefghijklmnop\n")))
    plainbuf = getfield(getfield(getfield(plainrow, :view), :r), :buf)
    plainvalue = plainrow.s
    @test getfield(plainvalue, :data) === plainbuf
    escapedrow = first(A.Rows(IOBuffer("s\n\"abcdefghij\"\"klmnop\"\n")))
    inputbuf = getfield(getfield(getfield(escapedrow, :view), :r), :buf)
    escapedvalue = escapedrow.s
    @test String(escapedvalue) == "abcdefghij\"klmnop"
    @test getfield(escapedvalue, :data) !== inputbuf
    owned = WeakRef(getfield(escapedvalue, :data))
    GC.gc(true)
    @test owned.value !== nothing && String(escapedvalue) == "abcdefghij\"klmnop"
    @test first(A.Rows(IOBuffer("s\nabcdefghijklmnop\n"); types=String)).s isa String
end

@testset "Chunks: schema stable across batches (0.10 port)" begin
    # a promotion that only appears late must not change the batch schema
    data = "a,b\n" * join(("$(i),value$(i)" for i in 1:40), '\n') * "X"
    chunks = collect(A.Chunks(IOBuffer(data); ntasks=2, pool=false))
    nrows(t) = t.nrows
    @test sum(nrows, chunks) == 40
    @test String(last(Tables.getcolumn(last(chunks), :b))) == "value40X"
    # ints then a string in the last batch: every batch reports the widened type
    late = "x\n" * join(string.(1:6000), '\n') * "\nfinal\n"
    cs = collect(A.Chunks(IOBuffer(late); ntasks=4, chunkbytes=8_000, pool=false))
    @test length(cs) >= 2
    types = unique(eltype(Tables.getcolumn(c, :x)) for c in cs)
    @test length(types) == 1 && Base.nonmissingtype(types[1]) <: AbstractString
    # Pooling must preserve the whole-file missing-capable schema in batches
    # that do not themselves contain a missing value.
    pooledinput = "id,s\n" *
                  join(("$(i)," * (i == 60 ? "" : "x") for i in 1:120), '\n') * "\n"
    pooledchunks = collect(A.Chunks(IOBuffer(pooledinput); chunkbytes=64, pool=true))
    @test length(pooledchunks) > 1
    @test all(Tables.getcolumn(c, :s) isa PooledArrays.PooledArray for c in pooledchunks)
    @test all(eltype(Tables.getcolumn(c, :s)) == Union{Missing, String} for c in pooledchunks)
    @test any(any(ismissing, Tables.getcolumn(c, :s)) for c in pooledchunks)
    @test any(!any(ismissing, Tables.getcolumn(c, :s)) for c in pooledchunks)
    # Ratio and cap apply to each batch's own rows and levels.
    twolvl = "s\n" * join((isodd(i) ? "x" : "y" for i in 1:20), '\n') * "\n"
    threelvl = "s\n" * join((string(Char(Int('x') + (i % 3))) for i in 1:21), '\n') * "\n"
    @test first(A.Chunks(IOBuffer(twolvl); chunkbytes=1 << 20,
                         pool=(1.0, 2)))[:s] isa PooledArrays.PooledArray
    @test !(first(A.Chunks(IOBuffer(threelvl); chunkbytes=1 << 20,
                           pool=(1.0, 2)))[:s] isa PooledArrays.PooledArray)
    # oracle: same row counts and values as 0.10 on the corpus file
    gzpath = corpusfile("randoms.csv.gz")
    ours = collect(A.Chunks(gzpath; ntasks=2))
    theirs = collect(LegacyCSV.Chunks(gzpath; ntasks=2))
    @test sum(nrows, ours) == sum(length, theirs) == 70_000
end

@testset "vector of sources + source= column" begin
    data = ["a,b,c\n1,2,3\n4,5,6\n", "a,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]
    f = A.File(map(IOBuffer, data))
    @test length(f) == 6 && f.a == [1, 4, 7, 10, 13, 16]
    # element types promote across sources
    f = A.File(map(IOBuffer, ["a\n1\n", "a\n2.5\n"]))
    @test eltype(Tables.getcolumn(f, :a)) == Float64 && f.a == [1.0, 2.5]
    mixedinputs = [Vector{UInt8}("a\n1\n"), Vector{UInt8}("a\nlong-value\n")]
    f = A.File(mixedinputs; pool=false)
    @test eltype(f.a) == Any && isequal(f.a, Any[1, "long-value"])
    @test f.a[2] isa String
    fill!(mixedinputs[2], UInt8('z'))
    @test f.a[2] == "long-value"
    # a source missing a column missing-fills it; its extra columns are ignored
    shifted = ["a,b,c\n1,2,3\n4,5,6\n", "a2,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]
    f = A.File(map(IOBuffer, shifted))
    @test Tables.columnnames(Tables.columns(f)) == [:a, :b, :c]
    @test isequal(collect(Tables.getcolumn(f, :a)), [1, 4, missing, missing, 13, 16])
    # string columns concatenate as String (concatenation owns its memory)
    f = A.File(map(IOBuffer, ["a,b\nx,1\n", "a,b\n,2\n"]))
    @test eltype(Tables.getcolumn(f, :a)) == Union{Missing, String}
    @test isequal(collect(Tables.getcolumn(f, :a)), ["x", missing])
    pooled = A.File(map(IOBuffer, ["a\nx\nx\n", "a\ny\ny\n"]); pool=true)
    @test pooled.a isa PooledArrays.PooledArray
    @test eltype(pooled.a) == String && collect(pooled.a) == ["x", "x", "y", "y"]
    # The concatenated strings own their bytes, including pooled levels.
    ownedinputs = [Vector{UInt8}("a\nlong-value-one\n"),
                   Vector{UInt8}("a\nlong-value-two\n")]
    owned = A.File(ownedinputs; pool=true)
    fill!(ownedinputs[1], UInt8('z'))
    fill!(ownedinputs[2], UInt8('z'))
    @test collect(owned.a) == ["long-value-one", "long-value-two"]
    # source= appends a pooled provenance column; labels are deterministic
    f = A.File(map(IOBuffer, data); source=:origin)
    col = Tables.getcolumn(f, :origin)
    @test col isa PooledArrays.PooledArray && eltype(col) == String
    @test collect(col) == ["<source 1>", "<source 1>", "<source 2>", "<source 2>",
                           "<source 3>", "<source 3>"]
    f = A.File(map(IOBuffer, data); source="origin" => [10, 20, 30])
    @test collect(Tables.getcolumn(f, :origin)) == [10, 10, 20, 20, 30, 30]
    labels = Union{Missing, Int}[1, missing, 3]
    f = A.File(map(IOBuffer, data); source=:origin => labels)
    @test eltype(f.origin) == Union{Missing, Int}
    @test isequal(collect(f.origin), [1, 1, missing, missing, 3, 3])
    # path sources label with the path; single-element vectors keep the column
    mktempdir() do tmp
        p1, p2 = joinpath(tmp, "x.csv"), joinpath(tmp, "y.csv")
        write(p1, "a\n1\n"); write(p2, "a\n2\n")
        f = A.File([p1, p2]; source=:src)
        @test collect(Tables.getcolumn(f, :src)) == [p1, p2]
        f = A.File([p1]; source=:src)
        @test collect(Tables.getcolumn(f, :src)) == [p1]
    end
    # per-file problems merge with row offsets
    f = A.File(map(IOBuffer, ["a,b\n1,2\n", "a,b\n3,4,5\n"]))
    @test length(A.problems(f)) == 1 && A.problems(f)[1].row == 2
    f = A.File(map(IOBuffer, ["a\n1\n", "a\n\"x\n"]); pool=false)
    @test any(p -> p.kind == :invalid_quoted_field && p.row == 2, A.problems(f))
    @test any(p -> p.kind == :unclosed_quote && p.row == 0, A.problems(f))
    # kwargs apply per source
    f = A.File(map(IOBuffer, data); select=["a"], types=Dict(:a => Float64))
    @test Tables.columnnames(Tables.columns(f)) == [:a] && f.a == [1.0, 4.0, 7.0, 10.0, 13.0, 16.0]
    @test A.File(map(IOBuffer, data); limit=1).a == [1, 7, 13]
    readback = CSV.read(map(IOBuffer, data), Tables.columntable; limit=1)
    @test readback.a == [1, 7, 13]
    # byte buffers still route to the single-source door
    @test A.File(Vector{UInt8}("a\n1\n")).a == [1]
    @test A.File(codeunits("a\n1\n")).a == [1]
    bytes = Vector{UInt8}("xa\n1\ny")
    byteview = @view bytes[2:end-1]
    @test A.File(byteview).a == [1]
    # errors: empty vector, label-length mismatch, name collision, bad source form
    @test_throws ArgumentError A.File(IOBuffer[])
    @test_throws ArgumentError A.File(map(IOBuffer, data); source="s" => [1, 2])
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=:a)
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=1)
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=:s => (1, 2, 3))
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=1 => [1, 2, 3])
    @test_throws ArgumentError A.File(Vector{UInt8}("a\n1\n"); source=:src)
    @test_throws ArgumentError A.File(codeunits("a\n1\n"); source=:src)
    @test_throws ArgumentError A.File(byteview; source=:src)
    # all-missing everywhere stays Missing eltype
    f = A.File(map(IOBuffer, ["a,b\n1,\n", "a,b\n2,\n"]))
    @test eltype(Tables.getcolumn(f, :b)) == Missing
    # Empty and Missing-eltype pieces preserve their schema contribution. The
    # copy into the promoted Union final converts values and fills absent blocks.
    f = A.File(map(IOBuffer, ["a\n", "a\n1\n"]); pool=false)
    @test eltype(f.a) == Union{Missing, Int64} && f.a == [1]
    f = A.File(map(IOBuffer, ["a\n1\n\n", "a\n2.5\n"]);
               pool=false, ignoreemptyrows=false)
    @test eltype(f.a) == Union{Missing, Float64}
    @test isequal(f.a, [1.0, missing, 2.5])
    # A distinct empty Union{} piece is not the absent-column sentinel. Guard
    # the vacuous `Union{} <: AbstractString` relation in both chain passes.
    unionempty = Union{}[]
    chained = A._chaincolumn(AbstractVector[unionempty, Int64[1]], [0, 1], 1)
    @test chained == [1] && eltype(chained) == Int64
    f = A.File(map(IOBuffer, ["a,b\n1,2\n", "a\n3\n"]); pool=false)
    @test isequal(f.b, [2, missing])
    f = A.File(map(IOBuffer, ["", "a\n1\n"]); pool=false)
    @test isempty(Tables.columnnames(f)) && length(f) == 1
end

@testset "FilePathsBase extension" begin
    mktempdir() do tmp
        write(joinpath(tmp, "in.csv"), "x,y\n1,2\n3,4\n")
        p = joinpath(FilePathsBase.Path(tmp), "in.csv")
        f = A.File(p)
        @test f.x == [1, 3] && f.y == [2, 4]
        @test first(A.Rows(p)).x isa AbstractString
        @test first(A.Chunks(p; chunkbytes=1 << 20))[:x] == [1, 3]
        @test A.sniff(p).names == [:x, :y]
        @test CSV.read(p, Tables.columntable).x == [1, 3]
        # writer sink + compress=:auto by extension
        out = joinpath(FilePathsBase.Path(tmp), "out.csv")
        CSV.write(out, (a=[1],))
        CSV.write(out, (a=[2],); append=true)
        @test read(joinpath(tmp, "out.csv"), String) == "a\n1\n2\n"
        gz = joinpath(FilePathsBase.Path(tmp), "out.csv.gz")
        CSV.write(gz, (a=[1],))
        CSV.write(gz, (a=[2],); append=true)
        @test A.File(gz).a == [1, 2]
        @test first(A.Rows(gz)).a == "1"
        @test first(A.Chunks(gz; chunkbytes=1 << 20))[:a] == [1, 2]
        @test A.sniff(gz).names == [:a]
        # A large AbstractPath takes the mmap branch and accepts both source
        # controls through the extension method.
        big = joinpath(FilePathsBase.Path(tmp), "big.csv")
        open(string(big), "w") do io
            write(io, "x,y\n")
            for i in 1:140_000
                print(io, i, ',', i + 1, '\n')
            end
        end
        @test filesize(string(big)) >= A.MMAP_THRESHOLD
        @test A.File(big; prefetch=false, limit=2).x == [1, 2]
        @test A.File(big; buffer_in_memory=true, limit=2).y == [2, 3]
    end
end
