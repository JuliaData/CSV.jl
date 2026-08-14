# Kernel test suite.
#
# Run:  julia --project=kernel -t4 kernel/test.jl
#
# Strategy: the scalar scanner is the correctness oracle. Every structural case is
# run through each eligible scanner (scalar / SWAR / vector) both sequentially
# and in parallel with deliberately tiny chunk sizes (3, 7, 16, 64 bytes), so
# range boundaries land inside fields, inside quoted sections, and between bytes of
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

indexsnapshot(buf, bi) =
    (; rows=rawrows(buf, bi), nrows=bi.nrows, unclosedquote=bi.unclosedquote)

# Index `input` every way the kernel supports for this dialect, asserting all ways
# agree, and return the (single) raw-rows result.
function idxall(input::AbstractString; chunks=(3, 7, 16, 64), kw...)
    buf = Vector{UInt8}(codeunits(input))
    d = K.Dialect(; kw...)
    variants = Pair{String, Any}[]
    push!(variants, "scalar/seq" =>
          indexsnapshot(buf, K.index(buf, d; parallel=false, fastindex=false)))
    if K.swareligible(d)
        for sc in (:swar, :vec)
            push!(variants, "$sc/seq" =>
                  indexsnapshot(buf, K.index(buf, d; parallel=false, scanner=sc)))
        end
    end
    if K.parityclean(d)
        for cb in chunks
            push!(variants, "scalar/seq$cb" =>
                  indexsnapshot(buf, K.index(buf, d; parallel=false, chunkbytes=cb, fastindex=false)))
            push!(variants, "scalar/par$cb" =>
                  indexsnapshot(buf, K.index(buf, d; parallel=true, chunkbytes=cb, fastindex=false)))
            if K.swareligible(d)
                for sc in (:swar, :vec)
                    push!(variants, "$sc/seq$cb" =>
                          indexsnapshot(buf, K.index(buf, d; parallel=false, chunkbytes=cb, scanner=sc)))
                    push!(variants, "$sc/par$cb" =>
                          indexsnapshot(buf, K.index(buf, d; parallel=true, chunkbytes=cb, scanner=sc)))
                end
            end
        end
    end
    ref = variants[1].second
    for (label, got) in variants
        @test got == ref
        got == ref || @info "structural mismatch" input label got ref
    end
    return ref.rows
end

# Top-level (not testset-local) so the allocation probe measures the loop, not
# closure machinery.
function sumncodeunits(c::K.KStrVector{K.KStr})
    t = 0
    for i in eachindex(c)
        t += ncodeunits(c[i])
    end
    return t
end

function sumgrouped(buf::Vector{UInt8}, opts::K.ValueOpts, scratch::Vector{UInt8})
    total = Int64(0)
    for _ in 1:1000
        v, ok = K.parsevalue(Int64, buf, 1, length(buf), opts, scratch)
        ok && (total += v)
    end
    return total
end

function kstrfrombytes(bytes::Vector{UInt8})
    p = length(bytes) <= K.KSTR_INLINE ? K.inline_payload(bytes, 1, length(bytes)) :
                                         K.view_payload(bytes, 1, length(bytes), Int64(1))
    return K.KStr(p, length(bytes) <= K.KSTR_INLINE ? K.EMPTY_BYTES : bytes)
end

function tablesnapshot(t::K.ParsedTable)
    probs = [(p.row, p.col, p.pos, p.kind, p.message) for p in K.problems(t)]
    return (; names=K.names(t), types=map(eltype, K.columns(t)),
            values=map(collect, K.columns(t)), nrows=t.nrows,
            problems=probs, droppedproblems=t.droppedproblems)
end

# ---------------------------------------------------------------------------
@testset "CSVKernel" begin
# ---------------------------------------------------------------------------

@testset "structural: basics" begin
    tt = UInt32[]
    for block in 0:31
        n = 64 * block
        K.tape_room!(tt, n, 64)
        @test length(tt) >= n + 64
        for j in 1:64
            tt[n + j] = UInt32(n + j)
        end
    end
    @test tt[1:2048] == UInt32.(1:2048)
    maxci = K.ChunkIndex(1, K.MAX_TAPE_RELPOS)
    @test K.checktaperange(maxci) === maxci
    @test UInt32(K.MAX_TAPE_RELPOS) << 2 >> 2 == K.MAX_TAPE_RELPOS
    @test_throws ArgumentError K.checktaperange(K.ChunkIndex(1, K.MAX_TAPE_RELPOS + 1))
    @test idxall("a,b,c\n1,2,3\n") == [["a","b","c"], ["1","2","3"]]
    @test idxall("a,b,c\n1,2,3")   == [["a","b","c"], ["1","2","3"]]  # no trailing newline
    @test idxall("a\n")            == [["a"]]
    @test idxall("a")              == [["a"]]
    @test idxall("")               == []
    @test idxall("\n"; ignoreemptyrows=false) == [[""]]
    @test idxall("\n")             == []                               # empty row dropped by default
    @test idxall("a,\n")           == [["a",""]]                       # trailing delimiter = trailing empty field
    @test idxall("a,")             == [["a",""]]                       # same, at EOF
    @test idxall(","; ignoreemptyrows=false) == [["",""]]              # only a delimiter at EOF
    @test idxall(",\n")            == [["",""]]                        # two empty fields is not an "empty row"
    @test idxall(",,,\n")          == [["","","",""]]
    @test idxall("a,b\n\n\nc,d\n") == [["a","b"], ["c","d"]]
    @test idxall("a,b\n\n\nc,d\n"; ignoreemptyrows=false) == [["a","b"], [""], [""], ["c","d"]]
    buf = Vector{UInt8}(codeunits("a,b\n"))
    ci = only(K.index(buf).chunks)
    @test_throws BoundsError K.fieldspan(ci, 0, 1)
    @test_throws BoundsError K.fieldspan(ci, 1, 0)
    @test K.fieldspan(ci, 1, 3) === nothing
end

@testset "structural: newlines" begin
    @test idxall("a,b\r\n1,2\r\n") == [["a","b"], ["1","2"]]
    @test idxall("a,b\r1,2\r")     == [["a","b"], ["1","2"]]           # lone CR
    @test idxall("\r"; ignoreemptyrows=false) == [[""]]                  # lone CR at EOF
    @test idxall("a\r\nb\rc\nd")   == [["a"],["b"],["c"],["d"]]       # mixed terminators
    @test idxall("a\r\n\r\nb\r\n") == [["a"],["b"]]
    @test idxall("a\r\n\r\nb\r\n"; ignoreemptyrows=false) == [["a"],[""],["b"]]
end

@testset "structural: tape assembly geometry" begin
    for n in (62, 63)
        firstfield = "x"^n
        @test idxall(firstfield * "\r\na,b\r\n"; chunks=(63, 64, 65)) ==
              [[firstfield], ["a", "b"]]
    end
    longcomment = "#" * ("drop,"^30)
    input = longcomment * "\r\n\r\n#second\n\nH1,H2\r\nv1,v2"
    buf = Vector{UInt8}(codeunits(input))
    d = K.Dialect(comment="#")
    bi = K.index(buf, d; parallel=false, chunkbytes=length(buf) + 1)
    ci = only(bi.chunks)
    hpos = findfirst(==(UInt8('H')), buf)
    vpos = findfirst(==(UInt8('v')), buf)
    @test rawrows(buf, bi) == [["H1", "H2"], ["v1", "v2"]]
    @test ci.rowstartrel == UInt32[hpos - ci.start, vpos - ci.start]
    @test K.fieldspan(ci, 1, 1) == (hpos, 2)
    @test K.fieldspan(ci, 2, 1) == (vpos, 2)
    for sc in (:scalar, :swar, :vec)
        t = K.parse(buf; comment="#", chunkbytes=3, parallel=true, scanner=sc)
        @test K.names(t) == [:H1, :H2]
        @test t.nrows == 1
        @test collect(t[:H1]) == ["v1"]
        @test collect(t[:H2]) == ["v2"]
    end
    dense = ","^1024
    @test idxall(dense; chunks=(64, 65), ignoreemptyrows=false) == [fill("", 1025)]
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
    longdelim = "xy"^128
    @test idxall("left" * longdelim * "right\n"; delim=longdelim) == [["left", "right"]]
    # distinct escape char (backslash) — parity-unclean, scalar/sequential only
    @test idxall("\"a\\\"b\",c\n"; escapechar='\\') == [["\"a\\\"b\"","c"]]
    # unicode content passes through untouched (spans are byte-exact)
    @test idxall("α,β\n∀,∃\n") == [["α","β"], ["∀","∃"]]
    @test_throws ArgumentError K.Dialect(delim="")
    @test_throws ArgumentError K.makevalueopts(K.Dialect(); decimal='é')
    @test_throws ArgumentError K.index(UInt8[0x61], K.Dialect(); datastart=0)
end

@testset "structural: ignorerepeated" begin
    # Semantics pinned against CSV.jl (kernel/probe_ignorerepeated.jl):
    # runs collapse, leading runs are consumed, trailing runs fold into the row
    # end (also at EOF and before CRLF), an all-delimiter row is ONE empty field
    # (a short row, not an empty row), and comments only match at the raw line
    # start — "  #x" is data.
    sp = (delim=' ', ignorerepeated=true)
    @test idxall("a b\n1 2\n"; sp...)        == [["a","b"], ["1","2"]]
    @test idxall("a  b\n1   2\n"; sp...)     == [["a","b"], ["1","2"]]
    @test idxall("  a b\n   1 2\n"; sp...)   == [["a","b"], ["1","2"]]
    @test idxall("a b \n1 2  \n"; sp...)     == [["a","b"], ["1","2"]]
    @test idxall("a b\n1 2   "; sp...)       == [["a","b"], ["1","2"]]
    @test idxall("1 2 \r\n3 4  \r\n"; sp...) == [["1","2"], ["3","4"]]
    @test idxall("   a   b   \r\n"; sp...)   == [["a","b"]]
    @test idxall("a  b  \r  c   d \r"; sp...) == [["a","b"], ["c","d"]]
    @test idxall("a b\n   \n1 2\n"; sp...)   == [["a","b"], [""], ["1","2"]]
    @test idxall("   "; sp...)               == [[""]]
    @test idxall("a b\n  #x\n1 2\n"; sp..., comment="#") == [["a","b"], ["#x"], ["1","2"]]
    @test idxall("#x\na b\n"; sp..., comment="#")        == [["a","b"]]
    @test idxall("#  dropped\rx   y\r"; sp..., comment="#") == [["x","y"]]
    @test idxall("\n \n"; sp..., ignoreemptyrows=false)  == [[""], [""]]
    @test idxall("\n \n"; sp...)             == [[""]]   # " " is not byte-empty
    # quotes: padding is structural only outside them; quoted empties survive
    @test idxall("\"x  y\" 2\n"; sp...)      == [["\"x  y\"","2"]]
    @test idxall("\"\"  2\n"; sp...)         == [["\"\"","2"]]
    @test idxall("\"a\"  \"b\"\n"; sp...)    == [["\"a\"","\"b\""]]
    @test idxall("  \"a\" b\n"; sp...)       == [["\"a\"","b"]]
    @test idxall("a b c\r\"x  y\"   2\r"; sp..., quoted=false) ==
          [["a","b","c"], ["\"x","y\"","2"]]
    # other delimiters: comma, tab, multi-byte (scalar path)
    @test idxall("a,,b\n,1,,2,\n"; ignorerepeated=true)  == [["a","b"], ["1","2"]]
    @test idxall("a\t\tb\n1\t\t\t2\n"; delim='\t', ignorerepeated=true) == [["a","b"], ["1","2"]]
    @test idxall("a::::b\n1::2\n"; delim="::", ignorerepeated=true)     == [["a","b"], ["1","2"]]
    @test idxall("::a::b::::\n1::2::::"; delim="::", ignorerepeated=true) == [["a","b"], ["1","2"]]
    @test idxall("a:::b\n"; delim="::", ignorerepeated=true) == [["a",":b"]]  # odd byte is content
    # ext bookkeeping: populated only under the flag, run lengths exact
    buf = Vector{UInt8}(codeunits("a   b  c\n"))
    ci = only(K.index(buf, K.Dialect(delim=' ', ignorerepeated=true); parallel=false).chunks)
    @test ci.ext == UInt32[2, 1, 0]          # two runs + the row end's slot
    @test K.fieldspan(ci, 1, 2) == (5, 1)
    @test K.fieldspan(ci, 1, 3) == (8, 1)
    ci = only(K.index(buf, K.Dialect(delim=' '); parallel=false).chunks)
    @test isempty(ci.ext)
end

@testset "structural: scanner dispatch" begin
    fast = K.Dialect()
    scalaronly = K.Dialect(delim="::")
    @test K.resolvescanner(fast, true, :auto) === :vec
    @test K.resolvescanner(fast, true, :vec) === :vec
    @test K.resolvescanner(fast, true, :swar) === :swar
    @test K.resolvescanner(fast, true, :scalar) === :scalar
    @test K.resolvescanner(fast, false, :vec) === :scalar
    @test K.resolvescanner(scalaronly, true, :vec) === :scalar
    @test_throws ArgumentError K.index(UInt8[], fast; scanner=:bogus)
    @test_throws ArgumentError K.index(UInt8[0x61], fast; fastindex=false, scanner=:bogus)
    @test_throws ArgumentError K.parse(""; scanner=:bogus)

    input = "a,b\n" * join(("$i,\"value,$i\"" for i in 1:40), '\n') * "\n"
    ref = tablesnapshot(K.parse(input; chunkbytes=5, parallel=false, scanner=:scalar))
    for sc in (:auto, :vec, :swar, :scalar), par in (false, true)
        got = K.parse(input; chunkbytes=5, parallel=par, scanner=sc)
        @test isequal(tablesnapshot(got), ref)
    end
    got = K.parse(input; chunkbytes=5, parallel=true, fastindex=false, scanner=:vec)
    @test isequal(tablesnapshot(got), ref)
end

@testset "structural: vector masks and prefix XOR" begin
    rng = MersenneTwister(0x5a59f65)
    for _ in 1:1000
        m = rand(rng, UInt64)
        @test K.prefix_xor64(m) == K.prefix_xor64_shift(m)
    end
    bytes = fill(UInt8('x'), 66)
    GC.@preserve bytes begin
        p = pointer(bytes, 2)
        for bit in 0:63
            bytes[bit + 2] = UInt8(',')
            expected = UInt64(1) << bit
            @test K.byte_mask_vec(p, UInt8(',')) == expected
            @test K.specials_mask_vec(p, UInt8(',')) == expected
            bytes[bit + 2] = UInt8('x')
        end
    end
    for _ in 1:256
        rand!(rng, bytes)
        quoted = rand(rng, Bool)
        oq = rand(rng, UInt8)
        delim = rand(rng, UInt8)
        GC.@preserve bytes begin
            p = pointer(bytes, 2)
            @test K.blockmasks(Val(:vec), p, quoted, oq, delim) ==
                  K.blockmasks(Val(:swar), p, quoted, oq, delim)
        end
    end
end

@testset "structural: raw-byte scanner differential" begin
    rng = MersenneTwister(0x30bb1e)
    alphabet = ['a', 'b', '#', '/', '"', ',', '\r', '\n']
    lengths = [0:10; 62:66; 126:130; rand(rng, 0:320, 48)]
    for n in lengths
        input = String(rand(rng, alphabet, n))
        idxall(input; chunks=(63, 64, 65), quoted=rand(rng, Bool),
               comment=rand(rng, (nothing, "#", "//")), ignoreemptyrows=rand(rng, Bool))
    end
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
    @test eltype(t[:c]) == K.KStr && collect(t[:c]) == ["x", "y"]
    @test t[:d] == [Date(2023, 1, 15), Date(2023, 1, 16)]
    @test t[:e] == [true, false]
    @test t[:f] == [Time(10, 30), Time(11, 30)]
    @test isempty(K.problems(t))
end

@testset "typed: missing values" begin
    t = K.parse("a,b\n1,x\n,\n3,z\n")
    @test eltype(t[:a]) == Union{Int64, Missing}
    @test isequal(collect(t[:a]), [1, missing, 3])
    @test eltype(t[:b]) == Union{K.KStr, Missing}
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
    @test eltype(t[:a]) == K.KStr
    # Strictness principle: each kernel accepts exactly the spellings detection
    # assigns to it (Bool is true/false only; temporals are pattern-exact), so
    # values that OLD Parsers accepted more loosely ("1" as Bool, a bare date as
    # DateTime) now conflict and promote to String — sample-independently.
    for ns in (1, 2, 3)
        @test eltype(K.parse("a\nfalse\n1\n1\n"; nsample=ns)[:a]) == K.KStr
        @test eltype(K.parse("a\n2024-01-02T03:04:05\n2024-01-03\n"; nsample=ns)[:a]) == K.KStr
        @test eltype(K.parse("a\n03:04:05\n1\n"; nsample=ns)[:a]) == K.KStr
    end
    # Quotes strip before detection AND parsing, so a quoted numeric overlap
    # follows the same lattice.
    @test eltype(K.parse("a\nfalse\n\"1\"\n"; nsample=1)[:a]) == K.KStr

    # Detection pins across custom Bool spellings, explicit date formats,
    # numeric special values, custom decimal bytes, and quoted fields.
    dt(v, o) = K.detecttype(Vector{UInt8}(codeunits(v)), 1, ncodeunits(v), o)
    defaultopts = K.makevalueopts(K.Dialect())
    boolopts = K.makevalueopts(K.Dialect(); truestrings=["yes"], falsestrings=["no"])
    dateopts = K.makevalueopts(K.Dialect(); dateformat="u dd yyyy",
                               truestrings=["yes"], falsestrings=["no"])
    decimalopts = K.makevalueopts(K.Dialect(delim=';'); decimal='x')
    @test dt("yes", boolopts) === Bool
    @test dt("no", boolopts) === Bool
    @test dt("yellow", boolopts) === String
    @test dt("true", boolopts) === String        # user lists REPLACE the defaults
    @test dt("true", defaultopts) === Bool
    @test dt("1", defaultopts) === Int64         # strict: never Bool
    @test dateopts.customfmt
    @test dt("Jan 02 2024", dateopts) === Date
    @test dt("Inf", defaultopts) === Float64
    @test dt("-Inf", defaultopts) === Float64
    @test dt("NaN", defaultopts) === Float64
    @test dt("nan", defaultopts) === Float64
    @test dt("x5", decimalopts) === Float64
    @test dt("\"yes\"", boolopts) === Bool
    @test dt("\"yellow\"", boolopts) === String

    # A custom format detects as exactly the type its components spell —
    # but numeric spellings still classify EARLIER in the cascade, and the
    # strict kernels guarantee parsing agrees with detection either way.
    numericdateopts = K.makevalueopts(K.Dialect(); dateformat="yyyymmdd")
    @test dt("20240102", numericdateopts) === Int64
    @test K.parsevalue(Date, Vector{UInt8}(codeunits("20240102")), 1, 8,
                       numericdateopts) == (Date(2024, 1, 2), true)
    dtfmtopts = K.makevalueopts(K.Dialect(); dateformat="yyyy-mm-dd HH:MM")
    @test dt("2024-01-02 03:04", dtfmtopts) === DateTime
    timefmtopts = K.makevalueopts(K.Dialect(); dateformat="HHhMM")
    @test dt("03h04", timefmtopts) === Time
    # A custom Bool spelling that collides with an earlier cascade type takes
    # Bool out of the INFERENCE cascade (else the final schema would depend on
    # nsample); user-provided Bool columns still parse the lists.
    collideopts = K.makevalueopts(K.Dialect(); truestrings=["1"], falsestrings=["NO"])
    @test !collideopts.inferbool
    @test dt("1", collideopts) === Int64
    @test dt("NO", collideopts) === String        # whole Bool cascade entry is off
    @test K.parsevalue(Bool, Vector{UInt8}(codeunits("1")), 1, 1, collideopts) == (true, true)
    @test !K.makevalueopts(K.Dialect(delim=';'); decimal='x', truestrings=["x5"]).inferbool
    @test !K.makevalueopts(K.Dialect(); dateformat="u dd yyyy",
                           truestrings=["Jan 02 2024"]).inferbool
    @test !K.makevalueopts(K.Dialect(delim=';'); groupmark=',',
                           truestrings=["1,000"]).inferbool
    @test K.makevalueopts(K.Dialect(); truestrings=["YES"], falsestrings=["NO"]).inferbool
    @test_throws ArgumentError K.makevalueopts(K.Dialect();
                                               truestrings=["yes"], falsestrings=["yes"])
    @test_throws ArgumentError K.makevalueopts(K.Dialect(); truestrings="yes")
    @test_throws ArgumentError K.makevalueopts(K.Dialect(); dateformat="literal")
    for ns in (1, 2, 3)
        tb = K.parse("a\nNO\nYES\n"; truestrings=["YES"], falsestrings=["NO"], nsample=ns)
        @test tb[:a] == [false, true]
        # colliding lists: inference lands on Int64/String sample-independently...
        ti = K.parse("a\n1\n0\n"; truestrings=["1"], falsestrings=["0"], nsample=ns)
        @test ti[:a] isa Vector{Int64}
        # ...while a user-typed Bool column parses them as the lists say
        tu = K.parse("a\n1\n0\n"; types=Bool, truestrings=["1"], falsestrings=["0"], nsample=ns)
        @test tu[:a] == [true, false]
        # The disabled cascade entry is also stable when only one spelling
        # collides: inference joins Int64 and String, while typed Bool keeps both.
        timix = K.parse("a\nNO\n1\n"; truestrings=["1"], falsestrings=["NO"], nsample=ns)
        @test collect(timix[:a]) == ["NO", "1"]
        tumix = K.parse("a\nNO\n1\n"; types=Bool, truestrings=["1"],
                        falsestrings=["NO"], nsample=ns)
        @test tumix[:a] == [false, true]
    end

    # A custom pattern parses only the temporal type named by its components.
    bd = Vector{UInt8}(codeunits("2024-01-02 03:04"))
    @test !K.parsevalue(Date, bd, 1, length(bd), dtfmtopts)[2]
    @test K.parsevalue(DateTime, bd, 1, length(bd), dtfmtopts)[2]
    bt = Vector{UInt8}(codeunits("03h04"))
    @test !K.parsevalue(Date, bt, 1, length(bt), timefmtopts)[2]
    @test K.parsevalue(Time, bt, 1, length(bt), timefmtopts)[2]

    # A stripped-to-empty field is Missing to detection and to every kernel —
    # a Date column keeps its type and the field is cleanly missing. (The old
    # Parsers-based path materialized a default Date(1) here and needed a
    # conflict guard that force-promoted the whole column to String.)
    stripopts = K.makevalueopts(K.Dialect(); stripwhitespace=true)
    @test dt(" ", stripopts) === Missing
    tstrip = K.parse("a\n2024-01-02\n \n"; stripwhitespace=true, nsample=1)
    @test eltype(tstrip[:a]) == Union{Missing, Date}
    @test isequal(collect(tstrip[:a]), [Date(2024, 1, 2), missing])
    # promotion across chunk boundaries with adversarially small chunks: early
    # chunks parse Int64, a late chunk hits a float ⇒ whole column re-parses
    input = "a\n" * join(1:50, "\n") * "\n99.5\n"
    for cb in (8, 32, 4096)
        t = K.parse(input; chunkbytes=cb, parallel=true)
        @test t[:a] isa Vector{Float64}
        @test t[:a] == [collect(1.0:50.0); 99.5]
    end
    # big integers within Int128 stay exact; mixed integer/float columns widen
    t = K.parse("a\n1\n99999999999999999999999999\n")
    @test t[:a] isa Vector{Int128}
    @test t[:a] == Int128[1, 99999999999999999999999999]
    t = K.parse("a\n99999999999999999999999999\n1.5\n")
    @test t[:a] isa Vector{Float64}
    # Stratified sampling obeys its limit and includes the final row.
    input = "a\n" * join(1:999, "\n") * "\n3.5\n"
    buf = Vector{UInt8}(codeunits(input))
    bi = K.index(buf, K.Dialect(); chunkbytes=64)
    bi.chunks[1].firstdatarow += 1
    opts = K.makevalueopts(K.Dialect())
    @test K.sampletypes(buf, bi.chunks, 1, opts; nsample=2) == [Float64]
    @test_throws ArgumentError K.sampletypes(buf, bi.chunks, 1, opts; nsample=0)
    @test_throws ArgumentError K.parse("a\n1\n"; types=Int64, nsample=0)
    @test_throws ArgumentError K.parse("a\n1\n"; chunkbytes=0)
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
    # Task-local logs must not retain maxproblems entries for every chunk. Each
    # completed task folds into one globally capped reservoir and releases its
    # local entries, while retaining the exact total and source-first problem.
    for cap in (0, 1, 3)
        pending = K.PendingProblemLog(cap)
        for chunk in 20:-1:1
            local log = K.ProblemLog(cap)
            for j in 5:-1:1
                K.pushproblem!(log, j, 1, 10 * chunk + j, :invalid_value, "bad")
            end
            K.mergeproblems!(pending, log, chunk)
            @test length(pending.items) <= cap
            @test isempty(log.items) && log.first === nothing && !log.heaped
        end
        bounded = K.finishproblems(pending, 100 .* collect(0:19))
        K.sortproblems!(bounded)
        @test [p.pos for p in bounded.items] == collect(11:(10 + cap))
        @test bounded.dropped == 100 - cap
        @test bounded.first.pos == 11
    end
    # Heap retention matches a full stable sort for descending input, key ties,
    # and identical problems. Kind ties must stay allocation-free.
    tiea = K.Problem(1, 1, 1, :alpha, "same")
    tieb = K.Problem(1, 1, 1, :zeta, "same")
    K.problemless(tiea, tieb)
    @test (@allocated K.problemless(tiea, tieb)) == 0
    adversarial = K.Problem[
        K.Problem(pos, pos % 3, pos, :invalid_value, "bad $pos") for pos in 40:-1:1
    ]
    append!(adversarial, [K.Problem(2, 1, 20, kind, msg)
                          for kind in (:zeta, :alpha), msg in ("z", "a")])
    append!(adversarial, fill(K.Problem(2, 1, 20, :same, "same"), 10))
    expected = sort(copy(adversarial); by=K.problemkey)
    for cap in (0, 1, 7, length(adversarial))
        log = K.ProblemLog(cap)
        for p in adversarial
            K.pushproblem!(log, p.row, p.col, p.pos, p.kind, p.message)
        end
        K.sortproblems!(log)
        nkeep = min(cap, length(expected))
        @test K.problemkey.(log.items) == K.problemkey.(expected[1:nkeep])
        @test log.dropped == length(expected) - nkeep
        @test K.problemkey(log.first) == K.problemkey(first(expected))
        @test !log.heaped
    end
    # on_error=:error escalates the first problem
    @test_throws ErrorException K.parse("a\n1\nxyz\n"; types=Dict(:a => Int64), on_error=:error)
    @test_throws ErrorException K.parse("a\nxyz\n"; types=Int64, on_error=:error, maxproblems=0)
    err = try
        K.parse("\"unclosed"; header=false, types=String, on_error=:error)
        nothing
    catch e
        e
    end
    @test err isa ErrorException && occursin("invalid_quoted_field at data row 1", err.msg)
    @test_throws ArgumentError K.parse("a\n1\n"; maxproblems=-1)
    # bad types keyword arguments throw
    @test_throws ArgumentError K.parse("a,b\n1,2\n"; types=[Int64])
    @test_throws ArgumentError K.parse("a,b\n1,2\n"; types=Dict(:nope => Int64))
    @test_throws ArgumentError K.parse("a\n1\n"; types=Any)
    @test_throws ArgumentError K.parse("a\n1\n"; types=Number)
    # user-only arbitrary-precision & identifier columns (never inferred)
    tb = K.parse("u,n,x\n123e4567-e89b-12d3-a456-426614174000,123456789012345678901234567890,0.1\n,,\n";
                 types=Dict(:u => Base.UUID, :n => BigInt, :x => BigFloat))
    @test isequal(collect(tb[:u]), [Base.UUID("123e4567-e89b-12d3-a456-426614174000"), missing])
    @test isequal(collect(tb[:n]), [parse(BigInt, "123456789012345678901234567890"), missing])
    @test isequal(collect(tb[:x]), [parse(BigFloat, "0.1"), missing])
    ti = K.parse("u\n123e4567-e89b-12d3-a456-426614174000\n")
    @test eltype(ti[:u]) == K.KStr    # inference never yields UUID/Big types
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
    @test any(p -> p.row == 0 && p.col == 1 && p.kind == :invalid_value, K.problems(t))
end

@testset "typed: dialect passthrough" begin
    t = K.parse("a;b\n1,5;2\n"; delim=';', decimal=',')
    @test t[:a] == [1.5]
    for ns in (1, 2, 3)
        t = K.parse("a\n1,5\n2\n3,25e1\n"; delim=';', decimal=',', nsample=ns)
        @test t[:a] == [1.5, 2.0, 32.5]
    end
    t = K.parse("a\n15/01/2023\n"; dateformat="dd/mm/yyyy")
    @test t[:a] == [Date(2023, 1, 15)]
    for ns in (1, 2, 3)
        t = K.parse("a\n15/01/2023\noops\n16/01/2023\n";
                    dateformat="dd/mm/yyyy", nsample=ns)
        @test collect(t[:a]) == ["15/01/2023", "oops", "16/01/2023"]
    end
    t = K.parse("a\nYES\nNO\n"; truestrings=["YES"], falsestrings=["NO"])
    @test t[:a] == [true, false]
    # Outer whitespace around quotes is structural. Inner whitespace is content
    # unless stripwhitespace asks the cell layer to remove it.
    t = K.parse("a\n  \"  x  \"  \n  \"\"  \n"; types=String)
    @test collect(t[:a]) == ["  x  ", ""]
    t = K.parse("a\n  \"  x  \"  \n  \"\"  \n"; types=String, stripwhitespace=true)
    @test collect(t[:a]) == ["x", ""]

    # groupmark: separators strip between digits in the integer part; group
    # widths are deliberately lenient (Indian "12,34,567" is real data). The
    # marked and unmarked spellings must detect AND parse identically
    # (parse-set ≡ detect-set extends to grouped digits).
    gmo = K.makevalueopts(K.Dialect(delim=';'); groupmark=',')
    dtg(v) = K.detecttype(Vector{UInt8}(codeunits(v)), 1, ncodeunits(v), gmo)
    @test dtg("1,234") === Int64
    @test dtg("12,34,567") === Int64
    @test dtg("1,234.5") === Float64
    @test dtg("1,234e2") === Float64
    @test dtg(",123") === String
    @test dtg("1,23,") === String
    @test dtg("12,,34") === String
    @test dtg("1.5,0") === String          # mark in the fraction
    pvg(T, v) = K.parsevalue(T, Vector{UInt8}(codeunits(v)), 1, ncodeunits(v), gmo)
    @test pvg(Int64, "1,234") == (1234, true)
    @test pvg(Int64, "1234") == (1234, true)
    @test pvg(Float64, "5,678.25") == (5678.25, true)
    @test !pvg(Int64, ",123")[2] && !pvg(Float64, "1.5,0")[2]
    scratch = Vector{UInt8}(undef, 64)
    @test K.V.degroup!(scratch, Vector{UInt8}(codeunits("1.5,0")), 1, 5,
                       UInt8(','), UInt8('.')) == -2
    @test K.V.degroup!(scratch, Vector{UInt8}(codeunits("1e1,0")), 1, 5,
                       UInt8(','), UInt8('.')) == -2
    groupedbytes = Vector{UInt8}(codeunits("1,234,567"))
    sumgrouped(groupedbytes, gmo, scratch) # compile before the allocation probe
    @test @allocated(sumgrouped(groupedbytes, gmo, scratch)) == 0
    # groupmark == delim works through quoting (the mark is content there)
    for ns in (1, 2, 3)
        tg = K.parse("a,b\n\"1,234\",x\n\"5,678\",y\n"; groupmark=',', nsample=ns)
        @test tg[:a] == [1234, 5678]
    end
    tg = K.parse("a;b\n1.234.567;2,5\n"; delim=';', groupmark='.', decimal=',')
    @test tg[:a] == [1234567] && tg[:b] == [2.5]
    # User-only numeric kernels share the same degroup scratch route.
    tgb = K.parse("a;b\n12,345,678,901,234,567,890;1,234.5\n";
                  delim=';', groupmark=',', types=[BigInt, BigFloat])
    @test tgb[:a] == [big"12345678901234567890"]
    @test tgb[:b] == [BigFloat("1234.5")]
    @test isempty(K.problems(tgb))
    # off ⇒ marked numbers are strings, exactly as before the feature existed
    tg = K.parse("a;b\n1,234;9\n"; delim=';')
    @test eltype(tg[:a]) == K.KStr
    for gm in ('0', '5', '9', '.', 'e', 'E', '+', '-', '"', '\0')
        @test_throws ArgumentError K.makevalueopts(K.Dialect(); groupmark=gm)
    end
    asymmetric = K.Dialect(openquotechar='[', closequotechar=']', escapechar='\\')
    for gm in ('[', ']', '\\')
        @test_throws ArgumentError K.makevalueopts(asymmetric; groupmark=gm)
    end

    # sentinels (the CSV front end's missingstring): exact content match ⇒
    # missing, before ANY type machinery sees the span — detection and parsing
    # agree by construction, so a numeric sentinel cannot destabilize inference.
    t = K.parse("a,b\nNA,1\n2,NA\n"; sentinels=["NA"])
    @test isequal(collect(t[:a]), [missing, 2]) && isequal(collect(t[:b]), [1, missing])
    @test eltype(t[:a]) == Union{Int64, Missing}
    t = K.parse("a,b\n\"NA\",1\n2,3\n"; sentinels=["NA"])            # quoted matches too
    @test isequal(collect(t[:a]), [missing, 2])
    t = K.parse("a,b\nna,1\n2,3\n"; sentinels=["NA"])                # case-sensitive
    @test collect(String, t[:a]) == ["na", "2"]
    t = K.parse("a,b\n999,1\n2,999\n"; sentinels=["999"])            # numeric spelling
    @test isequal(collect(t[:a]), [missing, 2]) && eltype(t[:a]) == Union{Int64, Missing}
    t = K.parse("a,b\nNA,1\nNA,2\n"; sentinels=["NA"])               # all-sentinel column
    @test eltype(t[:a]) == Missing
    t = K.parse("a,b\n  NA  ,1\nx,2\n"; sentinels=["NA"], stripwhitespace=true)
    @test isequal(collect(t[:a]), [missing, "x"])                    # strip, then match
    t = K.parse("a,b\nNA,N/A\nx,2\n"; sentinels=["NA", "N/A"])
    @test isequal(collect(t[:a]), [missing, "x"]) && isequal(collect(t[:b]), [missing, 2])
    t = K.parse("NA,b\n1,2\n"; sentinels=["NA"])                     # sentinel header auto-names
    @test K.names(t) == [:Column1, :b]
    # PINNED DELTA vs CSV.jl: an empty unquoted cell is ALWAYS missing here —
    # structural, not spelling-dependent. (CSV.jl's custom missingstring
    # replaces the "" default, turning empties into present empty strings.)
    t = K.parse("a,b\n,1\nx,2\n"; sentinels=["NA"])
    @test isequal(collect(t[:a]), [missing, "x"])
    # a quoted empty is a present empty string even when sentinels are active
    t = K.parse("a,b\n\"\",1\nx,2\n"; sentinels=["NA"], types=Dict(1 => String))
    @test collect(String.(coalesce.(t[:a], "∅"))) == ["", "x"]
    @test_throws ArgumentError K.parse("a\n1\n"; sentinels=[""])
    @test_throws ArgumentError K.parse("a\n1\n"; sentinels="NA")     # must be a collection
    @test_throws ArgumentError K.parse("a\n1\n"; sentinels=["N\"A"])
    t = K.parse("a\nN\"A\n"; sentinels=["N\"A"], quoted=false)
    @test eltype(t[:a]) === Missing
end

@testset "typed: ignorerepeated end to end" begin
    aligned = "  x    y      z\n  1   2.5   true\n 10  -3.25  false\n"
    for cb in (8, 16, 1 << 20), par in (false, true)
        t = K.parse(aligned; delim=' ', ignorerepeated=true, chunkbytes=cb, parallel=par)
        @test K.names(t) == [:x, :y, :z]
        @test t[:x] == [1, 10]
        @test t[:y] == [2.5, -3.25]
        @test t[:z] == [true, false]
        @test isempty(K.problems(t))
    end
    # an all-delimiter data row is a short row of one missing, not a dropped row
    t = K.parse("a b\n   \n1 2\n"; delim=' ', ignorerepeated=true)
    @test t.nrows == 2
    @test isequal(collect(t[:a]), [missing, 1])
    @test isequal(collect(t[:b]), [missing, 2])
    @test [(p.row, p.col, p.kind) for p in K.problems(t)] == [(1, 0, :short_row)]
    # composes with pool, groupmark, select, limit, header choices
    pooled = "k v\n" * join(("$(iseven(i) ? "ee" : "oo")   $i" for i in 1:50), '\n') * "\n"
    t = K.parse(pooled; delim=' ', ignorerepeated=true, pool=true, nsample=1, chunkbytes=32)
    @test t[:k] isa K.PooledColumn
    @test collect(t[:k])[1:4] == ["oo", "ee", "oo", "ee"]
    t = K.parse("a b\n1,234  5\n"; delim=' ', ignorerepeated=true, groupmark=',')
    @test t[:a] == [1234] && t[:b] == [5]
    t = K.parse("a b\r\"1 234\"   5\r"; delim=' ', ignorerepeated=true, groupmark=' ')
    @test t[:a] == [1234] && t[:b] == [5]
    t = K.parse(aligned; delim=' ', ignorerepeated=true, select=[:z], limit=1)
    @test K.names(t) == [:z] && t.nrows == 1 && t[:z] == [true]
    t = K.parse(" 1  2 \r3   4\r"; delim=' ', ignorerepeated=true, header=false)
    @test K.names(t) == [:Column1, :Column2] && t[:Column1] == [1, 3]
    t = K.parse(" 1  2\n"; delim=' ', ignorerepeated=true, header=[:l, :r])
    @test K.names(t) == [:l, :r] && t.nrows == 1
    # rowmask length and selection count use surviving post-collapse data rows:
    # the comment and byte-empty row drop, while the all-delimiter row remains.
    masked = "a b\r# drop\r\r   \r1  2\r3   4 \r"
    mkw = (delim=' ', ignorerepeated=true, comment="#", rowmask=[false, true, false])
    mref = K.parse(masked; mkw..., chunkbytes=1 << 20, parallel=false)
    @test mref.nrows == 1 && mref[:a] == [1] && mref[:b] == [2] && isempty(K.problems(mref))
    for cb in (3, 1 << 20), par in (false, true)
        @test isequal(tablesnapshot(K.parse(masked; mkw..., chunkbytes=cb, parallel=par)),
                      tablesnapshot(mref))
    end
    @test_throws ArgumentError K.parse(masked; delim=' ', ignorerepeated=true,
                                       comment="#", rowmask=fill(true, 4))
    # A late type conflict re-parses old segments through the same collapsed spans.
    promoted = "a b\r1   x\r2  y\r3.5    z\r"
    for cb in (4, 1 << 20), par in (false, true)
        t = K.parse(promoted; delim=' ', ignorerepeated=true, nsample=1,
                    chunkbytes=cb, parallel=par)
        @test t[:a] == [1.0, 2.0, 3.5] && collect(t[:b]) == ["x", "y", "z"]
    end

    # property: padding with runs (leading / between / trailing) parses
    # identically to the single-delimiter serialization, and the flag itself is
    # a no-op on unpadded input
    rng = MersenneTwister(0x16407474)
    for (delim, dstr) in ((' ', " "), ('\t', "\t"), ("::", "::"))
        for trial in 1:25
            nr, nc = rand(rng, 0:7), rand(rng, 1:4)
            lines = Vector{String}(undef, nr + 1)
            padlines = similar(lines)
            for r in 0:nr
                cells = String[]
                for c in 1:nc
                    kind = r == 0 ? 0 : rand(rng, 1:4)
                    content = if kind == 0
                        "h$c"
                    elseif kind == 1
                        string(rand(rng, -999:999))
                    elseif kind == 2
                        join(rand(rng, 'a':'z', rand(rng, 1:6)))
                    elseif kind == 3
                        string(rand(rng) * 100)
                    else
                        inner = join(rand(rng, ['x', '"', '\n', '\r', '\t', ',', ' ', ':'],
                                          rand(rng, 0:5)))
                        "\"" * replace(inner, "\"" => "\"\"") * "\""
                    end
                    push!(cells, content)
                end
                lines[r + 1] = join(cells, dstr)
                io = IOBuffer()
                print(io, dstr^rand(rng, 0:3))
                for c in eachindex(cells)
                    c > 1 && print(io, dstr^rand(rng, 1:4))
                    print(io, cells[c])
                end
                print(io, dstr^rand(rng, 0:3))
                padlines[r + 1] = String(take!(io))
            end
            # Use identical, mixed row terminators for each pair. Only delimiter
            # padding differs, so a failure cannot come from EOF-row geometry.
            baseio, paddedio = IOBuffer(), IOBuffer()
            for r in eachindex(lines)
                print(baseio, lines[r]); print(paddedio, padlines[r])
                if r < length(lines)
                    term = rand(rng, ("\n", "\r", "\r\n"))
                    print(baseio, term); print(paddedio, term)
                end
            end
            if rand(rng, Bool)
                term = rand(rng, ("\n", "\r", "\r\n"))
                print(baseio, term); print(paddedio, term)
            end
            base, padded = String(take!(baseio)), String(take!(paddedio))
            ref = tablesnapshot(K.parse(base; delim=delim, parallel=false))
            flagonbase = K.parse(base; delim=delim, ignorerepeated=true, parallel=false)
            @test isequal(tablesnapshot(flagonbase), ref)
            for cb in (16, 1 << 20), par in (false, true)
                got = K.parse(padded; delim=delim, ignorerepeated=true,
                              chunkbytes=cb, parallel=par)
                @test isequal(tablesnapshot(got), ref)
            end
        end
    end
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
    # Spans are Int64/Int32 end to end — a >2^20-byte field survives exactly
    # (the old Parsers.PosLen intermediate silently kept only the final 7 bytes).
    longvalue = repeat("x", (1 << 20) + 7)
    t = K.parse("a\n" * longvalue * "\n"; types=String, chunkbytes=1 << 16)
    @test t[:a][1] == longvalue
    # A bare quote is structurally valid by design, but when it protects a
    # delimiter the value-level reading of the span disagrees with the
    # structural one; report it and preserve the exact structural-span bytes.
    t = K.parse("a,b\nab\"cd,e\"f,g\n"; types=String)
    @test collect(t[:a]) == ["ab\"cd,e\"f"]
    @test any(p -> p.kind == :invalid_value && p.row == 1 && p.col == 1, K.problems(t))
    t = K.parse("a::b\nab\"cd::ef\"gh::z\n"; delim="::", types=String)
    @test collect(t[:a]) == ["ab\"cd::ef\"gh"]
    @test any(p -> p.kind == :invalid_value && p.row == 1 && p.col == 1, K.problems(t))
    # Explicit Missing keeps malformed-quote diagnostics specific.
    t = K.parse("a\n\"unclosed"; types=Missing)
    @test any(p -> p.kind == :invalid_quoted_field && p.row == 1, K.problems(t))
end

@testset "pooled string columns" begin
    # pooling must be invisible to values: differential vs the plain parse
    # across chunk geometries, parallelism, escaped levels, and missings
    rng = Random.MersenneTwister(77)
    cats = ["aa", "bb", "a much longer categorical value", "q\"\"z"]
    rows = String[]
    for i in 1:500
        c = rand(rng, cats)
        cell = c == "q\"\"z" ? "\"q\"\"z\"" : c
        push!(rows, (rand(rng, 1:10) == 1 ? "" : cell) * "," * string(i))
    end
    csv = "cat,val\n" * join(rows, "\n") * "\n"
    plain = K.parse(csv)
    for cb in (64, 256, 1 << 20), par in (false, true)
        t = K.parse(csv; pool=true, chunkbytes=cb, parallel=par)
        c = t[:cat]
        @test c isa K.PooledColumn
        @test isequal(collect(c), collect(plain[:cat]))
        # deterministic level ids: first occurrence in row order
        lv = collect(K.poollevels(c))
        @test lv == unique([x for x in collect(plain[:cat]) if !ismissing(x)])
        @test all(lv .!== missing)
    end
    # ratio policy: 4 levels / 500 rows ⇒ pooled at 0.05, plain at 0.005
    @test K.parse(csv; pool=0.05)[:cat] isa K.PooledColumn
    @test K.parse(csv; pool=0.005)[:cat] isa K.KStrVector
    # absolute cap abandons early
    tall = K.parse("a\n" * join(1:200, "\n") * "\n"; types=String, pool=(1.0, 8))
    @test tall[:a] isa K.KStrVector
    # cap forms and validation
    @test_throws ArgumentError K.parse("a\nx\n"; pool=1.5)
    @test_throws ArgumentError K.parse("a\nx\n"; pool=(-0.1, 10))
    @test_throws ArgumentError K.parse("a\nx\n"; pool=(1.0, -1))
    # The ratio is a strict level/row bound: 2 levels exceed 0.5 * 3 rows.
    @test K.parse("a\nx\ny\nx\n"; types=String, pool=0.5)[:a] isa K.KStrVector
    # all-present column gets the concrete eltype; missing goes Union + ref 0
    tp = K.parse("a,b\nx,1\nx,2\n"; pool=true)
    @test tp[:a] isa K.PooledColumn{K.KStr}
    tm = K.parse("a,b\nx,1\n,2\ny,3\nx,4\n"; pool=true)
    @test tm[:a] isa K.PooledColumn{Union{K.KStr, Missing}}
    @test K.poolrefs(tm[:a]) == UInt32[1, 0, 2, 1]
    @test isequal(K.materialize(tm[:a]), ["x", missing, "y", "x"])
    @test K.materialize(tp[:a]) == ["x", "x"]
    # Generic AbstractVector operations must work without custom methods.
    firstvalue, iterstate = iterate(tp[:a])
    @test firstvalue == tp[:a][1]
    @test first(iterate(tp[:a], iterstate)) == tp[:a][2]
    @test similar(tp[:a]) isa Vector{K.KStr}
    @test collect(copy(tp[:a])) == collect(tp[:a])
    @test occursin("x", sprint(show, tp[:a]))
    # Header-only typed strings stay a valid empty vector; there is no pool to build.
    emptycol = K.parse("a\n"; types=String, pool=true)[:a]
    @test emptycol isa K.KStrVector{K.KStr} && isempty(emptycol)
    # Long escaped values live in per-chunk `extra` buffers. Pooling must intern
    # equal bytes across those buffers, copy one level, and keep negative offsets.
    longescaped = "a long \"escaped\" categorical value"
    inlineescaped = "abcdefghij\"k"             # 12 bytes after unescaping
    quote_csv(s) = "\"" * replace(s, "\"" => "\"\"") * "\""
    escapedcsv = "a\n" * join((quote_csv(longescaped), quote_csv(inlineescaped),
                                quote_csv(longescaped), quote_csv(inlineescaped)), "\n") * "\n"
    escapedplain = K.parse(escapedcsv; types=String, chunkbytes=16, parallel=false)
    for par in (false, true)
        escapedpool = K.parse(escapedcsv; types=String, pool=true,
                              chunkbytes=16, parallel=par)[:a]
        @test escapedpool isa K.PooledColumn{K.KStr}
        @test collect(escapedpool) == collect(escapedplain[:a])
        @test K.poolrefs(escapedpool) == UInt32[1, 2, 1, 2]
        levels = K.poollevels(escapedpool)
        @test K.kstroff(levels.payloads[1]) < 0
        @test K.kstrlen(levels.payloads[2]) == K.KSTR_INLINE
        dict = Dict(levels[1] => 7)
        @test dict[escapedplain[:a][1]] == 7
    end
    # A promotion to String must still pool after stale numeric segments reparse.
    promocsv = "a\n1\n2\nword\n1\n"
    promoref = K.parse(promocsv; nsample=1, pool=true, chunkbytes=3, parallel=false)[:a]
    @test promoref isa K.PooledColumn{K.KStr}
    for _ in 1:10
        promoc = K.parse(promocsv; nsample=1, pool=true, chunkbytes=3, parallel=true)[:a]
        @test K.poolrefs(promoc) == K.poolrefs(promoref)
        @test collect(K.poollevels(promoc)) == collect(K.poollevels(promoref))
    end
    # Abandoning after an extra-backed level must leave flat stitching untouched.
    abandoned = K.parse(escapedcsv; types=String, pool=(1.0, 1),
                        chunkbytes=16, parallel=true)[:a]
    @test abandoned isa K.KStrVector
    @test collect(abandoned) == collect(escapedplain[:a])
    # non-string columns ignore pool
    @test K.parse("a\n1\n2\n"; pool=true)[:a] isa Vector{Int64}

    # parse-time staging: a cap that fails MID-CHUNK degrades in place — the
    # remaining rows finish through the plain string path and every geometry
    # still equals the unpooled parse (incl. escaped cells after the abandon)
    manyrows = String[]
    for i in 1:300
        push!(manyrows, i % 4 == 0 ? "\"lv$(i % 40) \"\"q\"\"\"" : "lv$(i % 40)")
    end
    manycsv = "a\n" * join(manyrows, "\n") * "\n"
    manyplain = collect(K.parse(manycsv; types=String, pool=false)[:a])
    for cap in (1, 5, 39), cb in (32, 256, 1 << 20), par in (false, true)
        c = K.parse(manycsv; types=String, pool=(1.0, cap), chunkbytes=cb, parallel=par)[:a]
        @test c isa K.KStrVector          # 40 levels always exceed the cap
        @test collect(c) == manyplain
    end
    c = K.parse(manycsv; types=String, pool=(1.0, 40), chunkbytes=64, parallel=true)[:a]
    @test c isa K.PooledColumn && collect(c) == manyplain

    # duplicate escaped levels rewind the staging extra: interning 200 repeats
    # of one long escaped value stores its bytes ONCE per chunk at most
    dupcsv = "a\n" * join((quote_csv(longescaped) for _ in 1:200), "\n") * "\n"
    dup = K.parse(dupcsv; types=String, pool=true, chunkbytes=1 << 20, parallel=false)[:a]
    @test dup isa K.PooledColumn{K.KStr}
    @test length(K.poollevels(dup)) == 1
    @test K.poollevels(dup).extra == Vector{UInt8}(codeunits(longescaped))
end

@testset "KStr: inline-else-view strings" begin
    # inline/view boundary: 12 bytes inline, 13 views the buffer
    t = K.parse("a\n" * "x"^12 * "\n" * "y"^13 * "\n")
    col = t[:a]
    @test col isa K.KStrVector{K.KStr}
    @test col[1] == "x"^12 && col[2] == "y"^13
    @test ncodeunits(col[1]) == 12 && ncodeunits(col[2]) == 13
    @test String(col[1]) == "x"^12 && String(col[2]) == "y"^13
    # equality/hash/Dict interop with String (both directions), sorting
    @test col[1] == "x"^12 && "x"^12 == col[1]
    @test isequal(col[1], "x"^12) && hash(col[1]) == hash("x"^12)
    d = Dict("x"^12 => 1)
    @test d[col[1]] == 1
    d2 = Dict(col[2] => 2)
    @test d2["y"^13] == 2
    @test sort([col[2], col[1]]) == [col[1], col[2]]
    @test cmp(col[1], col[2]) == cmp("x"^12, "y"^13)
    # escaped values: short ones inline, long ones land in the extra buffer
    t2 = K.parse("a\n\"in\"\"line\"\n\"a long escaped \"\"string\"\" beyond inline\"\n")
    @test collect(t2[:a]) == ["in\"line", "a long escaped \"string\" beyond inline"]
    @test !isempty(t2[:a].extra)                      # long unescaped value stored out-of-line
    @test K.kstroff(t2[:a].payloads[2]) < 0           # negative offset ⇒ extra buffer
    # quoted empty vs missing, unicode, Symbol
    t3 = K.parse("a\n\"\"\n\nαβγδεζηθικλμ\n"; ignoreemptyrows=false)
    @test isequal(collect(t3[:a]), ["", missing, "αβγδεζηθικλμ"])
    @test Symbol(t3[:a][3]) == :αβγδεζηθικλμ
    # iteration parity with the String oracle, including invalid UTF-8
    rng = MersenneTwister(99)
    for trial in 1:200
        n = rand(rng, 0:24)
        bytes = rand(rng, UInt8, n)
        replace!(b -> b in (UInt8(0x22), UInt8(0x2c), K.LF, K.CR) ? UInt8('x') : b, bytes)
        s = String(copy(bytes))
        tt = K.parse("h\n" * "\"" * String(copy(bytes)) * "\"\n"; types=String)
        v = tt[:h][1]
        if !ismissing(v)   # n == 0 quoted-empty gives ""
            @test collect(v) == collect(s)
            @test v == s && hash(v) == hash(s)
            @test length(v) == length(s)
        end
    end
    # Character-index APIs must use the same tolerant partition as String.
    # A bare continuation byte is its own invalid Char and therefore starts at a
    # valid index; a continuation consumed by a preceding lead byte does not.
    invalidcases = (UInt8[0x80], UInt8[0x61, 0x80, 0x62], UInt8[0xc2],
                    UInt8[0xc2, 0x41], UInt8[0xe0, 0x80],
                    UInt8[0xf0, 0x80, 0x41], UInt8[0xc2, 0x80])
    result(f) = try
        (:value, f())
    catch e
        (:error, typeof(e))
    end
    for bytes in invalidcases
        s = String(copy(bytes))
        v = kstrfrombytes(copy(bytes))
        @test collect(eachindex(v)) == collect(eachindex(s))
        @test lastindex(v) == lastindex(s)
        for i in 0:(length(bytes) + 1)
            @test isvalid(v, i) == isvalid(s, i)
            @test result(() -> thisind(v, i)) == result(() -> thisind(s, i))
            @test result(() -> nextind(v, i)) == result(() -> nextind(s, i))
            @test result(() -> prevind(v, i)) == result(() -> prevind(s, i))
            @test result(() -> v[i]) == result(() -> s[i])
        end
        for i in 1:length(bytes), j in i:length(bytes)
            @test result(() -> String(SubString(v, i, j))) ==
                  result(() -> String(SubString(s, i, j)))
        end
    end
    # access is allocation-free for inline AND view strings
    big = K.parse("a\n" * join(("value$(i)_" * "p"^(i % 20) for i in 1:1000), "\n") * "\n")
    colv = big[:a]::K.KStrVector{K.KStr}
    sumncodeunits(colv)  # compile
    @test @allocated(sumncodeunits(colv)) == 0
    # materialize detaches to plain Strings
    m = K.materialize(colv)
    @test m isa Vector{String} && m[1] == "value1_p"
    # write/print round-trip
    io = IOBuffer()
    print(io, colv[2])
    @test String(take!(io)) == "value2_pp"
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
        @test batch.nrows > 0
        total += batch.nrows
    end
    @test total == 3
    @test Tables.partitions(E.batches(csv; chunkbytes=8)) isa E.Batches
    padded = "a   b\r 1  2 \r3   4\r"
    pbs = collect(E.batches(padded; delim=' ', ignorerepeated=true, chunkbytes=3))
    @test reduce(vcat, (collect(batch[:a]) for batch in pbs)) == [1, 3]
    @test reduce(vcat, (collect(batch[:b]) for batch in pbs)) == [2, 4]
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
    @test E.typedvalue(String, rs[3], :b) == "z,w"
    @test ismissing(E.typedvalue(Int64, rs[1], :b))  # not parseable as Int
    prs = collect(E.rows(padded; delim=' ', ignorerepeated=true, chunkbytes=3))
    @test [row.a for row in prs] == ["1", "3"]
    @test [E.typedvalue(Int64, row, :b) for row in prs] == [2, 4]
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

@testset "sequential multi-chunk driver parity" begin
    input = "i,mix,txt,strict\n" *
            "1,1,alpha,10\n" *
            "2,2,\"line\nbreak\",bad\n" *
            "3,3.5,beta,30,extra\n" *
            "4,,\"gamma,delta\",40\n" *
            "5,word,zeta\n" *
            "6,6,eta,60\n"
    kw = (; types=Dict(:strict => Int64), nsample=1, maxproblems=2)
    buf = Vector{UInt8}(codeunits(input))
    @test length(K.index(buf; chunkbytes=7, parallel=false).chunks) > 1
    seq = K.parse(buf; chunkbytes=7, parallel=false, kw...)
    one = K.parse(buf; chunkbytes=length(buf) + 1, parallel=false, kw...)
    @test isequal(tablesnapshot(seq), tablesnapshot(one))
    # repeated: the fused wave promotes this input across chunks, so hammer the
    # parallel path for scheduling-order races
    for _ in 1:25
        par = K.parse(buf; chunkbytes=7, parallel=true, kw...)
        @test isequal(tablesnapshot(seq), tablesnapshot(par))
    end
    @test K.names(seq) == [:i, :mix, :txt, :strict]
    @test seq.nrows == 6
    @test eltype(seq[:mix]) == Union{K.KStr, Missing}
    @test seq.droppedproblems == 1

    malformed = "a\n\"unclosed"
    malformedseq = K.parse(malformed; types=String, chunkbytes=3, parallel=false)
    malformedpar = K.parse(malformed; types=String, chunkbytes=3, parallel=true)
    malformedone = K.parse(malformed; types=String,
                           chunkbytes=ncodeunits(malformed) + 1, parallel=false)
    @test isequal(tablesnapshot(malformedseq), tablesnapshot(malformedpar))
    @test isequal(tablesnapshot(malformedseq), tablesnapshot(malformedone))
    @test any(p -> p.kind == :unclosed_quote, K.problems(malformedseq))
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
