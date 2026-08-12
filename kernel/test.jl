# Kernel test suite.
#
# Run:  julia --project=kernel -t4 kernel/test.jl
#
# Strategy: the scalar scanner is the correctness oracle. Every structural case is
# run through each eligible scanner (scalar / SWAR) both sequentially and in
# parallel with deliberately tiny chunk sizes (3, 7, 16, 64 bytes) so that range
# boundaries land inside fields, inside quoted sections, and between the bytes of
# CRLF pairs. Results must be identical everywhere — that IS the parallelism
# correctness claim (determinism for any chunk geometry), so it is tested
# exhaustively rather than incidentally.

using Test, Random, Dates, Tables

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))
using .CSVKernel
const K = CSVKernel
include(joinpath(@__DIR__, "examples.jl"))
const E = KernelExamples

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

# Raw field text (quotes and escapes intact) for every row of an index — the
# structural layer's observable output.
function rawrows(buf::Vector{UInt8}, bi::K.BufferIndex)
    out = Vector{Vector{String}}()
    for ci in bi.chunks, lr in 1:K.totalrows(ci)
        row = String[]
        for c in 1:K.nfields(ci, lr)
            pos, len = K.fieldspan(ci, lr, c)
            push!(row, String(buf[pos:pos+len-1]))
        end
        push!(out, row)
    end
    return out
end

# Index `input` every way the kernel supports for this dialect, asserting all ways
# agree, and return the (single) raw-rows result.
function idxall(input::AbstractString; chunks=(3, 7, 16, 64), kw...)
    buf = Vector{UInt8}(codeunits(input))
    d = K.Dialect(; kw...)
    variants = Vector{Pair{String, Vector{Vector{String}}}}()
    push!(variants, "scalar/seq" => rawrows(buf, K.index(buf, d; parallel=false, fastindex=false)))
    if K.swareligible(d)
        push!(variants, "swar/seq" => rawrows(buf, K.index(buf, d; parallel=false, fastindex=true)))
    end
    if K.parityclean(d)
        for cb in chunks
            push!(variants, "scalar/par$cb" => rawrows(buf, K.index(buf, d; parallel=true, chunkbytes=cb, fastindex=false)))
            if K.swareligible(d)
                push!(variants, "swar/par$cb" => rawrows(buf, K.index(buf, d; parallel=true, chunkbytes=cb, fastindex=true)))
            end
        end
    end
    ref = variants[1].second
    for (label, got) in variants
        @test got == ref
        got == ref || @info "structural mismatch" input label got ref
    end
    return ref
end

# ---------------------------------------------------------------------------
@testset "CSVKernel" begin
# ---------------------------------------------------------------------------

@testset "structural: basics" begin
    @test idxall("a,b,c\n1,2,3\n") == [["a","b","c"], ["1","2","3"]]
    @test idxall("a,b,c\n1,2,3")   == [["a","b","c"], ["1","2","3"]]  # no trailing newline
    @test idxall("a\n")            == [["a"]]
    @test idxall("a")              == [["a"]]
    @test idxall("")               == []
    @test idxall("\n"; ignoreemptyrows=false) == [[""]]
    @test idxall("\n")             == []                               # empty row dropped by default
    @test idxall("a,\n")           == [["a",""]]                       # trailing delimiter = trailing empty field
    @test idxall("a,")             == [["a",""]]                       # same, at EOF
    @test idxall(",\n")            == [["",""]]                        # two empty fields is not an "empty row"
    @test idxall(",,,\n")          == [["","","",""]]
    @test idxall("a,b\n\n\nc,d\n") == [["a","b"], ["c","d"]]
    @test idxall("a,b\n\n\nc,d\n"; ignoreemptyrows=false) == [["a","b"], [""], [""], ["c","d"]]
end

@testset "structural: newlines" begin
    @test idxall("a,b\r\n1,2\r\n") == [["a","b"], ["1","2"]]
    @test idxall("a,b\r1,2\r")     == [["a","b"], ["1","2"]]           # lone CR
    @test idxall("a\r\nb\rc\nd")   == [["a"],["b"],["c"],["d"]]       # mixed terminators
    @test idxall("a\r\n\r\nb\r\n") == [["a"],["b"]]
    @test idxall("a\r\n\r\nb\r\n"; ignoreemptyrows=false) == [["a"],[""],["b"]]
end

@testset "structural: quotes" begin
    @test idxall("\"a,b\",c\n")            == [["\"a,b\"","c"]]        # quoted delimiter
    @test idxall("\"a\nb\",c\n")           == [["\"a\nb\"","c"]]       # quoted LF
    @test idxall("\"a\r\nb\",c\n")         == [["\"a\r\nb\"","c"]]     # quoted CRLF
    @test idxall("\"a\"\"b\",c\n")         == [["\"a\"\"b\"","c"]]     # escaped quote ("")
    @test idxall("\"\",c\n")               == [["\"\"","c"]]           # quoted empty field
    @test idxall("\"\"\"\",c\n")           == [["\"\"\"\"","c"]]       # field that is one escaped quote
    @test idxall("a,\"b\"\n\"c\",d\n")     == [["a","\"b\""], ["\"c\"","d"]]
    # a quoted field spanning many tiny chunks (the parity/boundary stress case)
    long = "\"" * join(fill("line with, commas", 20), "\n") * "\""
    @test idxall(long * ",x\n") == [[long, "x"]]
    # quotes disabled: quote bytes are ordinary content
    @test idxall("\"a,b\",c\n"; quoted=false) == [["\"a","b\"","c"]]
end

@testset "structural: pinned semantics for malformed quoting" begin
    # A bare quote mid-field opens a quoted region (structural quotes always
    # toggle — the Sep/simdcsv rule; see the module docstring). Pinned so any
    # future change to this tradeoff is a conscious one.
    # No closing quote ever appears here, so the field runs to EOF — trailing
    # newline included (it is "inside quotes"):
    @test idxall("ab\"cd,e\nf,g\n"; chunks=(3, 7)) == [["ab\"cd,e\nf,g\n"]]
    # With a closing quote, structure resumes at the toggle:
    @test idxall("ab\"cd,e\"f,g\n") == [["ab\"cd,e\"f", "g"]]
    # unclosed quote runs to EOF and flags the index
    buf = Vector{UInt8}(codeunits("a\n\"unclosed"))
    bi = K.index(buf, K.Dialect(); parallel=false)
    @test bi.unclosedquote
    @test rawrows(buf, bi) == [["a"], ["\"unclosed"]]
end

@testset "structural: comments" begin
    @test idxall("#c\na,b\n#d\n1,2\n"; comment="#")   == [["a","b"], ["1","2"]]
    @test idxall("#only\n"; comment="#")               == []
    @test idxall("#last no newline"; comment="#")      == []
    @test idxall("a,b\n# c, with, delims\n1,2\n"; comment="#") == [["a","b"], ["1","2"]]
    @test idxall("//x\na\n"; comment="//")             == [["a"]]
    # '#' mid-line is content, not a comment
    @test idxall("a#b,c\n"; comment="#")               == [["a#b","c"]]
end

@testset "structural: dialects" begin
    @test idxall("a;b\n1;2\n"; delim=';')          == [["a","b"], ["1","2"]]
    @test idxall("a\tb\n"; delim='\t')             == [["a","b"]]
    @test idxall("a::b::c\n"; delim="::")          == [["a","b","c"]]   # multi-byte delim (scalar path)
    @test idxall("a:b::c\n"; delim="::")           == [["a:b","c"]]
    # distinct escape char (backslash) — parity-unclean, scalar/sequential only
    @test idxall("\"a\\\"b\",c\n"; escapechar='\\') == [["\"a\\\"b\"","c"]]
    # unicode content passes through untouched (spans are byte-exact)
    @test idxall("α,β\n∀,∃\n") == [["α","β"], ["∀","∃"]]
    @test_throws ArgumentError K.Dialect(delim="")
    @test_throws ArgumentError K.makeoptions(K.Dialect(); decimal='é')
    @test_throws ArgumentError K.index(UInt8[0x61], K.Dialect(); datastart=0)
end

@testset "structural: randomized property (all scanners × all chunkings agree)" begin
    rng = MersenneTwister(20260812)
    specials = ['"', ',', '\n', '\r']
    for trial in 1:150
        nrows = rand(rng, 0:10)
        ncols = rand(rng, 1:5)
        expectedraw = Vector{Vector{String}}()
        expectedval = Vector{Vector{Union{String, Missing}}}()
        for r in 1:nrows
            rawrow = String[]
            valrow = Union{String, Missing}[]
            for c in 1:ncols
                kind = rand(rng, 1:5)
                content = kind == 1 ? string(rand(rng, -999:999)) :
                          kind == 2 ? join(rand(rng, 'a':'z', rand(rng, 1:6))) :
                          kind == 3 ? "" :
                          kind == 4 ? join(rand(rng, ['x', 'y', specials...], rand(rng, 1:8))) :
                                      string(rand(rng) * 100)
                mustquote = any(in(specials), content)
                doquote = mustquote || rand(rng, Bool)
                raw = doquote ? "\"" * replace(content, "\"" => "\"\"") * "\"" : content
                push!(rawrow, raw)
                push!(valrow, content == "" && !doquote ? missing : content)
            end
            push!(expectedraw, rawrow)
            push!(expectedval, valrow)
        end
        lines = [join(rawrow, ',') for rawrow in expectedraw]
        terms = [rand(rng, ("\n", "\r\n", "\r")) for _ in lines]
        # A "\r"-terminated row directly followed by an EMPTY row whose own
        # terminator starts with "\n" serializes to bytes identical to a single
        # CRLF — genuinely ambiguous CSV that no parser can round-trip. Steer the
        # generator away from manufacturing it.
        for r in 2:length(lines)
            isempty(lines[r]) && terms[r - 1] == "\r" && (terms[r - 1] = "\n")
        end
        io = IOBuffer()
        for r in eachindex(lines)
            print(io, lines[r])
            # random terminator; the last row sometimes has none — except that an
            # unterminated all-empty single-field row would vanish from the bytes,
            # so that shape always gets a terminator
            if r < length(lines) || rand(rng, Bool) || isempty(lines[r])
                print(io, terms[r])
            end
        end
        input = String(take!(io))
        got = idxall(input; ignoreemptyrows=false, chunks=(3, 16, 64))
        # single-column all-empty unquoted rows serialize as "" lines: structural
        # layer reports them as [""], which matches expectedraw already.
        @test got == expectedraw
        # typed layer: force String, values must equal unescaped content (missing
        # for unquoted-empty). Zero-row inputs have no columns to check (names
        # come from the first row and there isn't one).
        if nrows > 0
            t = K.parse(input; header=false, types=String, ignoreemptyrows=false,
                        chunkbytes=16, parallel=true)
            @test t.nrows == nrows
            for c in 1:ncols
                col = K.columns(t)[c]
                for r in 1:nrows
                    @test isequal(col[r], expectedval[r][c])
                end
            end
        end
    end
end

# ---------------------------------------------------------------------------

@testset "typed: inference & values" begin
    t = K.parse("a,b,c,d,e,f\n1,1.5,x,2023-01-15,true,10:30:00\n2,2.5,y,2023-01-16,false,11:30:00\n")
    @test K.names(t) == [:a, :b, :c, :d, :e, :f]
    @test t.nrows == 2
    @test K.columns(t)[1] isa Vector{Int64} && t[:a] == [1, 2]
    @test K.columns(t)[2] isa Vector{Float64} && t[:b] == [1.5, 2.5]
    @test eltype(t[:c]) == String && collect(t[:c]) == ["x", "y"]
    @test t[:d] == [Date(2023, 1, 15), Date(2023, 1, 16)]
    @test t[:e] == [true, false]
    @test t[:f] == [Time(10, 30), Time(11, 30)]
    @test isempty(K.problems(t))
end

@testset "typed: missing values" begin
    t = K.parse("a,b\n1,x\n,\n3,z\n")
    @test eltype(t[:a]) == Union{Int64, Missing}
    @test isequal(collect(t[:a]), [1, missing, 3])
    @test eltype(t[:b]) == Union{String, Missing}
    @test isequal(collect(t[:b]), ["x", missing, "z"])
    # quoted empty is an empty STRING, not missing
    t2 = K.parse("a\n\"\"\nx\n")
    @test isequal(collect(t2[:a]), ["", "x"])
    # whitespace-only field is CONTENT by default (stripwhitespace=false ⇒ the
    # column is String)…
    t3 = K.parse("a\n \n1\n")
    @test isequal(collect(t3[:a]), [" ", "1"])
    # …and becomes missing once whitespace stripping is on
    t3b = K.parse("a\n \n1\n"; stripwhitespace=true)
    @test isequal(collect(t3b[:a]), [missing, 1])
    # all-missing column
    t4 = K.parse("a,b\n1,\n2,\n")
    @test eltype(t4[:b]) == Missing && length(t4[:b]) == 2
    @test_throws BoundsError t4[:b][3]
end

@testset "typed: promotion" begin
    # int → float, conflict in the middle
    t = K.parse("a\n1\n2\n2.5\n4\n")
    @test t[:a] isa Vector{Float64} && t[:a] == [1.0, 2.0, 2.5, 4.0]
    # int → string
    t = K.parse("a\n1\n2\nxyz\n")
    @test collect(t[:a]) == ["1", "2", "xyz"]
    # date → string on mixed temporals
    t = K.parse("a\n2023-01-15\n10:30:00\n")
    @test eltype(t[:a]) == String
    # promotion across chunk boundaries with adversarially small chunks: early
    # chunks parse Int64, a late chunk hits a float ⇒ whole column re-parses
    input = "a\n" * join(1:50, "\n") * "\n99.5\n"
    for cb in (8, 32, 4096)
        t = K.parse(input; chunkbytes=cb, parallel=true)
        @test t[:a] isa Vector{Float64}
        @test t[:a] == [collect(1.0:50.0); 99.5]
    end
    # big integers overflow Int64 and promote to Float64 (documented CSV.jl parity)
    t = K.parse("a\n1\n99999999999999999999999999\n")
    @test t[:a] isa Vector{Float64}
    # Stratified sampling obeys its limit and includes the final row.
    input = "a\n" * join(1:999, "\n") * "\n3.5\n"
    buf = Vector{UInt8}(codeunits(input))
    bi = K.index(buf, K.Dialect(); chunkbytes=64)
    bi.chunks[1].firstdatarow += 1
    opts = K.makeoptions(K.Dialect())
    @test K.sampletypes(buf, bi.chunks, 1, opts; nsample=2) == [Float64]
    @test_throws ArgumentError K.sampletypes(buf, bi.chunks, 1, opts; nsample=0)
    @test_throws ArgumentError K.parse("a\n1\n"; types=Int64, nsample=0)
end

@testset "typed: user-provided types" begin
    # by name, by index, single Type, full Vector
    t = K.parse("a,b\n1,2\n"; types=Dict(:a => Float64))
    @test t[:a] isa Vector{Float64} && t[:a] == [1.0]
    t = K.parse("a,b\n1,2\n"; types=Dict(2 => String))
    @test collect(t[:b]) == ["2"]
    t = K.parse("a,b\n1,2\n"; types=String)
    @test collect(t[:a]) == ["1"]
    t = K.parse("a,b\n1,2\n"; types=[Int64, Float64])
    @test t[:b] == [2.0]
    # Union{T,Missing} collapses to T (missingness is per-value)
    t = K.parse("a\n1\n\n"; types=[Union{Int64, Missing}], ignoreemptyrows=false)
    @test isequal(collect(t[:a]), [1, missing])
    # invalid value under a user type ⇒ problem + missing, no promotion
    t = K.parse("a\n1\nxyz\n3\n"; types=Dict(:a => Int64))
    @test isequal(collect(t[:a]), [1, missing, 3])
    @test length(K.problems(t)) == 1
    p = K.problems(t)[1]
    @test p.kind == :invalid_value && p.row == 2 && p.col == 1
    # Explicit Missing validates every present value, not only the first value in
    # each chunk. Problem order and bounded retention are deterministic.
    t = K.parse("a\nx\ny\nz\n"; types=Missing, chunkbytes=2, maxproblems=10)
    @test isequal(collect(t[:a]), fill(missing, 3))
    @test [(p.row, p.col) for p in K.problems(t)] == [(1, 1), (2, 1), (3, 1)]
    bad = "a,b\n" * join(fill("x,y", 5), "\n") * "\n"
    expected = [(1, 1), (1, 2), (2, 1)]
    for _ in 1:10
        t = K.parse(bad; types=Int64, chunkbytes=3, maxproblems=3)
        @test [(p.row, p.col) for p in K.problems(t)] == expected
        @test t.droppedproblems == 7
    end
    # on_error=:error escalates the first problem
    @test_throws ErrorException K.parse("a\n1\nxyz\n"; types=Dict(:a => Int64), on_error=:error)
    @test_throws ErrorException K.parse("a\nxyz\n"; types=Int64, on_error=:error, maxproblems=0)
    @test_throws ArgumentError K.parse("a\n1\n"; maxproblems=-1)
    # bad types keyword arguments throw
    @test_throws ArgumentError K.parse("a,b\n1,2\n"; types=[Int64])
    @test_throws ArgumentError K.parse("a,b\n1,2\n"; types=Dict(:nope => Int64))
    @test_throws ArgumentError K.parse("a\n1\n"; types=Any)
    @test_throws ArgumentError K.parse("a\n1\n"; types=AbstractString)
    @test_throws ArgumentError K.parse("a\n1\n"; types=Union{Int64, String})
    @test_throws ArgumentError K.parse("a\n1\n"; types=["Int64"])
    # `nothing` leaves selected columns inferred.
    t = K.parse("a,b\n1,2\n"; types=[Float64, nothing])
    @test t[:a] == [1.0] && t[:b] == [2]
end

@testset "typed: ragged rows" begin
    t = K.parse("a,b,c\n1,2\n4,5,6,7\n8,9,10\n")
    @test t.nrows == 3
    @test isequal(collect(t[:c]), [missing, 6, 10])
    kinds = [p.kind for p in K.problems(t)]
    @test :short_row in kinds && :long_row in kinds
    short = K.problems(t)[findfirst(p -> p.kind == :short_row, K.problems(t))]
    @test short.row == 1 && short.col == 0
end

@testset "typed: headers" begin
    t = K.parse("1,2\n3,4\n"; header=false)
    @test K.names(t) == [:Column1, :Column2] && t.nrows == 2
    t = K.parse("1,2\n3,4\n"; header=[:x, :y])
    @test K.names(t) == [:x, :y] && t[:x] == [1, 3]
    t = K.parse("a,a,b,a\n1,2,3,4\n")
    @test K.names(t) == [:a, :a_1, :b, :a_2]
    # quoted header names unescape
    t = K.parse("\"my \"\"col\"\"\",b\n1,2\n")
    @test K.names(t)[1] == Symbol("my \"col\"")
    # empty header cell gets a generated name
    t = K.parse("a,,c\n1,2,3\n")
    @test K.names(t) == [:a, :Column2, :c]
    # A malformed header must not be silently truncated at a delimiter that the
    # structural quote rule assigned to the same field.
    t = K.parse("ab\"cd,e\"f,g\n1,2\n")
    @test K.names(t) == [Symbol("ab\"cd,e\"f"), :g]
end

@testset "typed: dialect passthrough" begin
    t = K.parse("a;b\n1,5;2\n"; delim=';', decimal=',')
    @test t[:a] == [1.5]
    t = K.parse("a\n15/01/2023\n"; dateformat="dd/mm/yyyy")
    @test t[:a] == [Date(2023, 1, 15)]
    t = K.parse("a\nYES\nNO\n"; truestrings=["YES"], falsestrings=["NO"])
    @test t[:a] == [true, false]
end

@testset "misc inputs & edges" begin
    # BOM
    t = K.parse(String(UInt8[0xef, 0xbb, 0xbf]) * "a,b\n1,2\n")
    @test K.names(t) == [:a, :b]
    # IO input
    t = K.parse(IOBuffer("a\n1\n"))
    @test t[:a] == [1]
    # header-only file: zero rows, Missing columns
    t = K.parse("a,b\n")
    @test t.nrows == 0 && K.names(t) == [:a, :b] && eltype(t[:a]) == Missing
    # completely empty input
    t = K.parse("")
    @test t.nrows == 0 && isempty(K.names(t))
    # unclosed quote at EOF: recorded as a problem, field still delivered
    t = K.parse("a\n\"unclosed")
    @test any(p -> p.kind == :unclosed_quote, K.problems(t))
    @test only(filter(p -> p.kind == :unclosed_quote, K.problems(t))).row == 0
    @test any(p -> p.kind == :invalid_quoted_field, K.problems(t))
    @test isequal(collect(t[:a]), [missing])
    # string escape materialization
    t = K.parse("a\n\"x\"\"y\"\n")
    @test collect(t[:a]) == ["x\"y"]
    # materialize detaches from the buffer
    v = K.materialize(t[:a])
    @test v == ["x\"y"] && v isa Vector{String}
    # String spans use Parsers.PosLen31, not PosLen's 20-bit length. The old
    # path silently returned only the final 7 bytes for this value.
    longvalue = repeat("x", (1 << 20) + 7)
    t = K.parse("a\n" * longvalue * "\n"; types=String, chunkbytes=1 << 16)
    @test t[:a][1] == longvalue
    # Every value parser must consume the full structural span. A bare quote is
    # structurally valid by design, but it makes this value malformed for
    # Parsers' field-start quote rule; report it instead of returning a prefix.
    t = K.parse("a,b\nab\"cd,e\"f,g\n"; types=String)
    @test isequal(collect(t[:a]), [missing])
    @test any(p -> p.kind == :invalid_value && p.row == 1 && p.col == 1, K.problems(t))
end

@testset "examples: the layered APIs" begin
    csv = "a,b,c\n1,x,2.5\n2,y,3.5\n3,\"z,w\",4.5\n"
    # eager reader is a Tables.jl table
    t = E.read(csv)
    nt = Tables.columntable(t)
    @test nt.a == [1, 2, 3]
    @test nt.c == [2.5, 3.5, 4.5]
    @test collect(nt.b) == ["x", "y", "z,w"]
    @test Tables.schema(t).names == (:a, :b, :c)
    # batches: stable schema across tiny chunks; concatenation covers everything
    total = 0
    for batch in E.batches(csv; chunkbytes=8)
        @test K.names(batch) == [:a, :b, :c]
        @test eltype(batch[:a]) == Int64          # global inference ⇒ same type every batch
        total += batch.nrows
    end
    @test total == 3
    @test Tables.partitions(E.batches(csv; chunkbytes=8)) isa E.Batches
    # global prepass fixes types even when an early batch is type-ambiguous:
    # first rows are ints, floats only appear later — every batch still Float64
    csv2 = "x\n" * join(1:20, "\n") * "\n3.5\n"
    for batch in E.batches(csv2; chunkbytes=16)
        @test eltype(batch[:x]) == Float64
    end
    # A sample cannot guarantee a stable schema. Put the only float beyond the
    # old 128-row sample and put a missing in only one batch; all batches must
    # still expose the same Union element type.
    csv3 = "x\n" * join(1:1000, "\n") * "\n\n3.5\n"
    bs = collect(E.batches(csv3; chunkbytes=256, ignoreemptyrows=false))
    @test all(eltype(batch[:x]) == Union{Float64, Missing} for batch in bs)
    @test last(bs)[:x][end] == 3.5
    # User types are strict in batches, as they are in the eager driver.
    bs = collect(E.batches("x\n1\nbad\n3\n"; types=Int64, chunkbytes=2))
    @test all(eltype(batch[:x]) == Union{Int64, Missing} for batch in bs)
    @test only([p for batch in bs for p in K.problems(batch)]).row == 2
    # Row-shape problems use global data-row ids across chunks.
    bs = collect(E.batches("a,b\n1\n2,3,4\n"; chunkbytes=3))
    probs = [p for batch in bs for p in K.problems(batch)]
    @test [(p.kind, p.row) for p in probs] == [(:short_row, 1), (:long_row, 2)]
    # row streaming: lazy untyped + on-demand typed access
    rs = collect(E.rows(csv))
    @test length(rs) == 3
    @test rs[1].a == "1"                          # untyped access materializes strings
    @test rs[3][:b] == "z,w"
    @test E.typedvalue(Int64, rs[1], :a) == 1
    @test E.typedvalue(Float64, rs[2], 3) == 3.5
    @test ismissing(E.typedvalue(Int64, rs[1], :b))  # not parseable as Int
    # ragged row: missing beyond the row's fields
    rs2 = collect(E.rows("a,b\n1\n"))
    @test ismissing(rs2[1][:b])
    @test_throws BoundsError rs2[1][0]
    @test_throws BoundsError E.typedvalue(Int64, rs2[1], 3)
    # Rows declares the Tables.jl row interface, including a concrete schema.
    rows = E.rows(csv)
    @test Tables.istable(typeof(rows)) && Tables.rowaccess(typeof(rows))
    @test Tables.rows(rows) === rows
    @test Tables.schema(rows).types ==
          (Union{String, Missing}, Union{String, Missing}, Union{String, Missing})
    @test Tables.rowtable(rows)[1] == (a="1", b="x", c="2.5")
    # A CSV column name takes priority over RowView's private storage fields.
    row = first(E.rows("r,rownumber\nvalue,7\n"))
    @test row.r == "value" && row.rownumber == "7"
end

@testset "determinism & moderate volume" begin
    # ~40k cells with quoted newlines sprinkled in, parsed under several chunk
    # geometries: identical results, exact row count, no reallocation guesswork.
    rng = MersenneTwister(42)
    io = IOBuffer()
    println(io, "id,val,txt,when")
    n = 10_000
    for i in 1:n
        txt = rand(rng) < 0.05 ? "\"multi\nline,text $i\"" : "plain$i"
        println(io, i, ",", i * 0.5, ",", txt, ",", Date(2023, 1, 1) + Day(i % 300))
    end
    input = String(take!(io))
    ref = nothing
    for (cb, par) in ((1 << 22, false), (1 << 12, true), (777, true))
        t = K.parse(input; chunkbytes=cb, parallel=par)
        @test t.nrows == n
        @test t[:id] isa Vector{Int64} && t[:id][end] == n
        @test t[:val][2] == 1.0
        @test eltype(t[:when]) == Date
        s = sum(t[:id])
        ref === nothing ? (ref = s) : @test(s == ref)
    end
end

end # top-level testset
