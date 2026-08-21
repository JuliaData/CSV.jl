# Kernel test suite.
#
# Run:  julia --startup-file=no --project=test -t4 test/kernel.jl
#
# Strategy: use the scalar scanner as the expected result. Run every structural
# case through each supported scanner (scalar, SWAR, and vector) both sequentially
# and in parallel with deliberately tiny chunk sizes (3, 7, 16, 64 bytes), so
# range boundaries land inside fields, inside quoted sections, and between bytes of
# CRLF pairs. Results must be identical everywhere — that IS the parallelism
# correctness claim (determinism for any chunk geometry), so it is tested
# exhaustively rather than incidentally.

using Test, Random, Dates, Tables, Mmap

using CSV
import Parsers
const K = CSV
const E = CSV

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

# Build every index variant supported by a dialect. Kept separate so tests can
# pin the geometry matrix itself, not only the results produced by that matrix.
function idxvariants(input::AbstractString; chunks=(3, 7, 16, 64), kw...)
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
    return variants
end

# Index `input` every way the kernel supports for this dialect, assert agreement,
# and return the raw rows.
function idxall(input::AbstractString; chunks=(3, 7, 16, 64), kw...)
    variants = idxvariants(input; chunks, kw...)
    ref = variants[1].second
    for (label, got) in variants
        @test got == ref
        got == ref || @info "structural mismatch" input label got ref
    end
    return ref.rows
end

# Top-level (not testset-local) so the allocation probe measures the loop, not
# closure machinery.
function sumncodeunits(c::K.CompactStringVector{K.CompactString})
    t = 0
    for i in eachindex(c)
        t += ncodeunits(c[i])
    end
    return t
end

# Keep `@allocated` inside a compiled, typed function. Julia 1.10 otherwise
# boxes the scalar return at a testset-local measurement site (16 bytes), even
# when the loop itself is allocation-free.
allocsumncodeunits(c::K.CompactStringVector{K.CompactString}) =
    @allocated sumncodeunits(c)

function sumgrouped(buf::Vector{UInt8}, opts::K.ValueOpts, scratch::Vector{UInt8})
    total = Int64(0)
    for _ in 1:1000
        v, ok = K.parsevalue(Int64, buf, 1, length(buf), opts, scratch)
        ok && (total += v)
    end
    return total
end

allocsumgrouped(buf::Vector{UInt8}, opts::K.ValueOpts, scratch::Vector{UInt8}) =
    @allocated sumgrouped(buf, opts, scratch)

function scalar_delimclash(buf::Vector{UInt8}, cpos::Int, clen::Int,
                           delim::Vector{UInt8})
    n = length(delim)
    clen < n && return false
    @inbounds for k in cpos:(cpos + clen - n)
        if buf[k] == delim[1]
            m = 2
            while m <= n && buf[k + m - 1] == delim[m]
                m += 1
            end
            m > n && return true
        end
    end
    return false
end

function csfrombytes(bytes::Vector{UInt8})
    p = length(bytes) <= K.COMPACTSTRING_INLINE ? K.inline_payload(bytes, 1, length(bytes)) :
                                         K.view_payload(bytes, 1, length(bytes), 0, 0)
    return K.CompactString(p, length(bytes) <= K.COMPACTSTRING_INLINE ? K.EMPTY_BYTES : bytes)
end

function csscratchbytes(s::K.CompactString)
    r = Ref(K._cs_scratch(s))
    out = Vector{UInt8}(undef, 16)
    GC.@preserve r begin
        p = Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, r))
        unsafe_copyto!(pointer(out), p, 16)
    end
    return out
end

function foldcshash(v, h::UInt)
    @inbounds for x in v
        h = hash(x, h)
    end
    return h
end

allocfoldcshash(v::Vector{K.CompactString}, h::UInt) = @allocated foldcshash(v, h)

function foldcscmp(v)
    s = 0
    @inbounds for i in 2:length(v)
        s += cmp(v[i - 1], v[i]) + (v[i - 1] == v[i])
    end
    return s
end

allocfoldcscmp(v::Vector{K.CompactString}) = @allocated foldcscmp(v)

function tablesnapshot(t::K.ParsedTable)
    probs = [(p.row, p.col, p.pos, p.kind, p.message) for p in K.problems(t)]
    return (; names=K.names(t), types=map(eltype, K.columns(t)),
            values=map(collect, K.columns(t)), nrows=t.nrows,
            problems=probs, droppedproblems=t.droppedproblems)
end

# ---------------------------------------------------------------------------
@testset "CSV parser core" begin
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
    @test K._defaultchunkbytes(0, 8) == 1 << 16
    @test K._defaultchunkbytes(20 << 20, 10) == 1 << 19
    for nthreads in (1, 4, 8)
        old = clamp(cld(200 << 20, 2 * nthreads), 1 << 16, 1 << 20)
        @test K._defaultchunkbytes(200 << 20, nthreads) == old == 1 << 20
    end
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

@testset "structural: delimiter-clash SWAR differential" begin
    rng = MersenneTwister(0xd311)
    for align in 0:15, len in 0:64
        cpos = align + 1
        # A match immediately after the content span must never be observed.
        boundary = fill(UInt8(0x00), align + len + 8)
        boundary[cpos + len] = 0xff
        @test !K._delimclash(boundary, cpos, len, UInt8[0xff])

        for _ in 1:32
            buf = rand(rng, UInt8, align + len + 8)
            delim = UInt8[rand(rng, UInt8)]
            @test K._delimclash(buf, cpos, len, delim) ==
                  scalar_delimclash(buf, cpos, len, delim)
        end
    end

    # The multi-byte branch is the original scalar algorithm and stays exact.
    for delim in (UInt8[0x61, 0x62], UInt8[0xff, 0x00, 0xff])
        for align in 0:15, len in 0:64, _ in 1:4
            buf = rand(rng, UInt8, align + len + 8)
            @test K._delimclash(buf, align + 1, len, delim) ==
                  scalar_delimclash(buf, align + 1, len, delim)
        end
    end
end

@testset "structural: newlines" begin
    @test idxall("a,b\r\n1,2\r\n") == [["a","b"], ["1","2"]]
    @test idxall("a,b\r1,2\r")     == [["a","b"], ["1","2"]]           # lone CR
    @test idxall("\r"; ignoreemptyrows=false) == [[""]]                  # lone CR at EOF
    @test idxall("a\r\nb\rc\nd")   == [["a"],["b"],["c"],["d"]]       # mixed terminators
    @test idxall("a\r\n\r\nb\r\n") == [["a"],["b"]]
    @test idxall("a\r\n\r\nb\r\n"; ignoreemptyrows=false) == [["a"],[""],["b"]]
    @test idxall("a\r\r\nb"; ignoreemptyrows=false) == [["a"],[""],["b"]]   # CR + CRLF
    @test idxall("a\r\n\r\nb"; ignoreemptyrows=false) == [["a"],[""],["b"]] # CRLF + CRLF
    @test idxall("#drop\r\nx\r\n"; comment="#") == [["x"]]

    # finishscan! sees raw kind 3 at the CR. Its end test must account for the
    # LF byte and avoid synthesizing a second row end.
    d = K.Dialect(ignoreemptyrows=false)
    for (input, raw) in (("\r\n", UInt32(3)), ("\r", UInt32(1)), ("\n", UInt32(2)))
        buf = Vector{UInt8}(codeunits(input))
        ci = K.ChunkIndex(1, length(buf))
        push!(ci.tape, raw)
        K.finishscan!(ci, buf, d, 1, false)
        @test K.totalrows(ci) == 1
        @test length(ci.tape) == 1
    end
    emptyci = K.ChunkIndex(1, 0)
    K.finishscan!(emptyci, UInt8[], d, 0, false)
    @test K.totalrows(emptyci) == 0
    @test isempty(emptyci.tape)
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

    # Quote-close boundaries must not pair an in-quote CR with an outside LF.
    # The second case closes at byte 63, then carries an outside CRLF from the
    # last bit of the 64-byte block into the scalar tail.
    insidecr = "\"" * "x"^61 * "\r\""
    @test idxall(insidecr * "\n"; chunks=(63, 64, 65), ignoreemptyrows=false) == [[insidecr]]
    outsidecrlf = "\"" * "x"^61 * "\""
    @test idxall(outsidecrlf * "\r\n"; chunks=(63, 64, 65), ignoreemptyrows=false) ==
          [[outsidecrlf]]
    for n in 58:66
        field = "\"" * "x"^n * "\r\n\""
        @test idxall(field * "\r\n"; chunks=(63, 64, 65), ignoreemptyrows=false) == [[field]]
    end
end

@testset "structural: quotes" begin
    @test idxall("\"a,b\",c\n")            == [["\"a,b\"","c"]]        # quoted delimiter
    @test idxall("\"a\nb\",c\n")           == [["\"a\nb\"","c"]]       # quoted LF
    @test idxall("\"a\r\nb\",c\n")         == [["\"a\r\nb\"","c"]]     # quoted CRLF
    @test idxall("\"a\"\"b\",c\n")         == [["\"a\"\"b\"","c"]]     # escaped quote ("")
    @test idxall("\"\",c\n")               == [["\"\"","c"]]           # quoted empty field
    @test idxall("\"\"\"\",c\n")           == [["\"\"\"\"","c"]]       # field that is one escaped quote
    @test idxall("a,\"b\"\n\"c\",d\n")     == [["a","\"b\""], ["\"c\"","d"]]
    # A quoted field crosses many tiny byte ranges. This checks quote counts and
    # row-boundary selection together.
    long = "\"" * join(fill("line with, commas", 20), "\n") * "\""
    @test idxall(long * ",x\n") == [[long, "x"]]
    # quotes disabled: quote bytes are ordinary content
    @test idxall("\"a,b\",c\n"; quoted=false) == [["\"a","b\"","c"]]
end

@testset "structural: malformed quote behavior" begin
    # A bare quote in the middle of a field starts a quoted region during the
    # row scan. See the parser-core comment at the start of core.jl.
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
    # Comment bytes are opaque. An unmatched quote cannot poison later rows,
    # including CRLF rows and every tiny sequential/parallel chunk geometry.
    poisoned = "# unmatched \" quote,comma\r\na,b\r\n1,2\r\n"
    @test idxall(poisoned; comment="#") == [["a", "b"], ["1", "2"]]
    labels = first.(idxvariants(poisoned; comment="#"))
    @test labels == ["scalar/seq";
                     [x for cb in (3, 7, 16, 64) for x in ("scalar/seq$cb", "scalar/par$cb")]]
    # A comment marker at a physical line start inside a quoted multiline field
    # is content because the structural row has not ended.
    @test idxall("a,b\n\"top\n# content \"\" quote\nbottom\",1\n"; comment="#") ==
          [["a", "b"], ["\"top\n# content \"\" quote\nbottom\"", "1"]]
    # A shorter row cannot match a longer comment prefix. A matching final
    # comment row without a terminator is still dropped.
    @test idxall("##\na\n###last"; comment="###") == [["##"], ["a"]]

    # `nextrowstart` only treats `from` as comment-capable when the caller proves
    # it is a true row start. A mid-row range probe must honor quote state.
    midbuf = Vector{UInt8}(codeunits("x# \"\nstill quoted\"\nnext\n"))
    middialect = K.Dialect(comment="#")
    nextpos = findfirst(==(UInt8('n')), midbuf)
    @test K.nextrowstart(midbuf, 2, length(midbuf), middialect, false) == nextpos
    rowbuf = Vector{UInt8}(codeunits("# \" poison\nnext\n"))
    @test K.nextrowstart(rowbuf, 1, length(rowbuf), middialect, false, true) ==
          findlast(==(UInt8('n')), rowbuf)
end

@testset "structural: dialects" begin
    @test idxall("a;b\n1;2\n"; delim=';')          == [["a","b"], ["1","2"]]
    @test idxall("a\tb\n"; delim='\t')             == [["a","b"]]
    @test idxall("a::b::c\n"; delim="::")          == [["a","b","c"]]   # multi-byte delim (scalar path)
    @test idxall("a:b::c\n"; delim="::")           == [["a:b","c"]]
    longdelim = "xy"^128
    @test idxall("left" * longdelim * "right\n"; delim=longdelim) == [["left", "right"]]
    # A separate backslash escape needs the scalar scanner.
    @test idxall("\"a\\\"b\",c\n"; escapechar='\\') == [["\"a\\\"b\"","c"]]
    # unicode content passes through untouched (spans are byte-exact)
    @test idxall("α,β\n∀,∃\n") == [["α","β"], ["∀","∃"]]
    @test_throws ArgumentError K.Dialect(delim="")
    @test_throws ArgumentError K.makevalueopts(K.Dialect(); decimal='é')
    @test_throws ArgumentError K.index(UInt8[0x61], K.Dialect(); datastart=0)
end

@testset "structural: ignorerepeated" begin
    # These tests define how repeated delimiters work. A run becomes one
    # boundary. A leading run is removed. A trailing run becomes part of the row
    # ending, including at end of input and before CRLF. A row with only
    # delimiters has one empty field. A comment prefix must start at the first
    # byte of a row, so "  #x" is data.
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

@testset "Parsers 3 integration" begin
    @test !isdefined(K, :V)

    quotechar = UInt8('"')
    quoted = Vector{UInt8}(codeunits("\"a\"\"b\""))
    @test K.findcontent(quoted, 1, length(quoted), quotechar, quotechar, quotechar) ==
          (2, 4, true, Parsers.RC_OK)

    opts = K.makevalueopts(K.Dialect())
    intbytes = Vector{UInt8}(codeunits("99999999999999999999999999"))
    @test K.parsevalue(Int128, intbytes, 1, length(intbytes), opts) ==
          (Int128(99999999999999999999999999), true)

    overflow = Vector{UInt8}(codeunits("1e9999"))
    overvalue, overok = K.parsevalue(Float64, overflow, 1, length(overflow), opts)
    @test overok && overvalue == Inf
    @test K.detecttype(overflow, 1, length(overflow), opts) === Float64

    underflow = Vector{UInt8}(codeunits("-1e-9999"))
    undervalue, underok = K.parsevalue(Float64, underflow, 1, length(underflow), opts)
    @test underok && iszero(undervalue) && signbit(undervalue)
    @test K.detecttype(underflow, 1, length(underflow), opts) === Float64

    groupedopts = K.makevalueopts(K.Dialect(delim=';'); groupmark=',')
    grouped = Vector{UInt8}(codeunits("1,0e9999"))
    groupedvalue, groupedok =
        K.parsevalue(Float64, grouped, 1, length(grouped), groupedopts)
    @test groupedok && groupedvalue == Inf

    ranged = K.parse("x\n1e9999\n-1e-9999\n")
    @test ranged[:x] isa Vector{Float64}
    @test ranged[:x][1] == Inf
    @test iszero(ranged[:x][2]) && signbit(ranged[:x][2])
    @test isempty(K.problems(ranged))

    datebytes = Vector{UInt8}(codeunits("2024-02-29"))
    civil, rc = Parsers.parsecivil(datebytes, 1, length(datebytes), K._ISO_DATE_PATTERN)
    @test rc == Parsers.RC_OK
    @test K.todate(civil) == Date(2024, 2, 29)

    fartext = "1e70000"
    farbytes = Vector{UInt8}(codeunits(fartext))
    farexpected = Base.parse(BigFloat, fartext)
    @test Parsers.tryparse(BigFloat, farbytes, 1, length(farbytes)) == farexpected
    farvalue, farok = K.parsevalue(BigFloat, farbytes, 1, length(farbytes), opts)
    @test farok && farvalue == farexpected

    groupedfar = Vector{UInt8}(codeunits("1,0e70000"))
    groupedfarexpected = Base.parse(BigFloat, "10e70000")
    groupedfarvalue, groupedfarok =
        K.parsevalue(BigFloat, groupedfar, 1, length(groupedfar), groupedopts)
    @test groupedfarok && groupedfarvalue == groupedfarexpected
end

@testset "typed: inference & values" begin
    t = K.parse("a,b,c,d,e,f\n1,1.5,x,2023-01-15,true,10:30:00\n2,2.5,y,2023-01-16,false,11:30:00\n")
    @test K.names(t) == [:a, :b, :c, :d, :e, :f]
    @test t.nrows == 2
    @test K.columns(t)[1] isa Vector{Int64} && t[:a] == [1, 2]
    @test K.columns(t)[2] isa Vector{Float64} && t[:b] == [1.5, 2.5]
    @test eltype(t[:c]) == K.CompactString && collect(t[:c]) == ["x", "y"]
    @test t[:d] == [Date(2023, 1, 15), Date(2023, 1, 16)]
    @test t[:e] == [true, false]
    @test t[:f] == [Time(10, 30), Time(11, 30)]
    @test isempty(K.problems(t))
end

@testset "typed: default ISO patterns match Parsers" begin
    opts = K.makevalueopts(K.Dialect())
    function checktemporal(T, pat, adapt, s)
        buf = Vector{UInt8}(codeunits(s))
        c, rc = Parsers.parsecivil(buf, 1, length(buf), pat)
        value, ok = K.parsevalue(T, buf, 1, length(buf), opts)
        @test ok == (rc == Parsers.RC_OK)
        @test (K.detecttype(buf, 1, length(buf), opts) === T) == ok
        ok && @test value == adapt(c)
    end

    for s in ("0000-01-01", "9999-12-31", "2000-02-29", "1900-02-29",
              "2400-02-29", "2020-1-01", "2020-1-01x", "2020/01-01")
        checktemporal(Date, K._ISO_DATE_PATTERN, K.todate, s)
    end
    for s in ("2024-01-02T03:04:05", "2024-01-02 03:04:05",
              "2024-01-02T03:04:05.", "2024-01-02T03:04:05.1")
        checktemporal(DateTime, K._ISO_DATETIME_PATTERN, K.todatetime, s)
    end
    for s in ("00:00:00", "23:59:59", "24:00:00")
        checktemporal(Time, K._ISO_TIME_PATTERN, K.totime, s)
    end

    # A user format identical to a default is still custom. It must use the
    # compiled interpreter and retain the custom-format early type gates.
    customdate = K.makevalueopts(K.Dialect(); dateformat="yyyy-mm-dd")
    customdt = K.makevalueopts(K.Dialect(); dateformat="yyyy-mm-ddTHH:MM:SS.s")
    customtime = K.makevalueopts(K.Dialect(); dateformat="HH:MM:SS.s")
    @test customdate.customfmt && customdt.customfmt && customtime.customfmt
    bd = Vector{UInt8}(codeunits("2024-02-29"))
    bdt = Vector{UInt8}(codeunits("2024-02-29T03:04:05"))
    bt = Vector{UInt8}(codeunits("03:04:05"))
    @test K.parsevalue(Date, bd, 1, length(bd), customdate) == (Date(2024, 2, 29), true)
    @test !K.parsevalue(DateTime, bd, 1, length(bd), customdate)[2]
    @test K.detecttype(bd, 1, length(bd), customdate) === Date
    @test K.parsevalue(DateTime, bdt, 1, length(bdt), customdt) ==
          (DateTime(2024, 2, 29, 3, 4, 5), true)
    @test !K.parsevalue(Date, bdt, 1, length(bdt), customdt)[2]
    @test K.detecttype(bdt, 1, length(bdt), customdt) === DateTime
    @test K.parsevalue(Time, bt, 1, length(bt), customtime) == (Time(3, 4, 5), true)
    @test !K.parsevalue(Date, bt, 1, length(bt), customtime)[2]
    @test K.detecttype(bt, 1, length(bt), customtime) === Time
end

@testset "typed: missing values" begin
    t = K.parse("a,b\n1,x\n,\n3,z\n")
    @test eltype(t[:a]) == Union{Int64, Missing}
    @test isequal(collect(t[:a]), [1, missing, 3])
    @test eltype(t[:b]) == Union{K.CompactString, Missing}
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

    # Per-column options must apply even while a column is still seeded as
    # Missing. The direct Missing probe used to read the global options and
    # spuriously promote this all-sentinel column to String.
    sentinelopts = [K.makevalueopts(K.Dialect(); sentinels=["NA"])]
    for par in (false, true)
        tc = K.parse("a\nNA\nNA\n"; colopts=sentinelopts, nsample=1, parallel=par)
        @test tc[:a] isa Vector{Missing}
        @test isequal(tc[:a], fill(missing, 2))
    end
    @test_throws ArgumentError K.parse("a,b\n1,2\n"; colopts=sentinelopts)
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
    @test eltype(t[:a]) == K.CompactString
    # Each value parser accepts the same text that type detection accepts. Bool
    # accepts true and false. Date and time values must match their patterns.
    # Other text changes the column type to String for every sample size.
    for ns in (1, 2, 3)
        @test eltype(K.parse("a\nfalse\n1\n1\n"; nsample=ns)[:a]) == K.CompactString
        @test eltype(K.parse("a\n2024-01-02T03:04:05\n2024-01-03\n"; nsample=ns)[:a]) == K.CompactString
        @test eltype(K.parse("a\n03:04:05\n1\n"; nsample=ns)[:a]) == K.CompactString
    end
    # Quotes strip before detection AND parsing, so a quoted numeric overlap
    # follows the same lattice.
    @test eltype(K.parse("a\nfalse\n\"1\"\n"; nsample=1)[:a]) == K.CompactString

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
    # Direct finals converge under concurrent, different promotions. The third
    # column starts Missing, then upgrades without reparsing its missing chunks.
    storm = "a,b,c\n" * join(("1,1,", "99999999999999999999999999,2,",
                               "3.5,2024-01-02,7", "word,4,", "5,5,",
                               "6.25,6,", "2024-02-03,7,", "8,8,"), "\n") * "\n"
    stormref = K.parse(storm; nsample=1, chunkbytes=1, parallel=false)
    for _ in 1:25
        t = K.parse(storm; nsample=1, chunkbytes=1, parallel=true)
        @test String.(t[:a]) == String.(stormref[:a])
        @test String.(t[:b]) == String.(stormref[:b])
        @test isequal(collect(t[:c]), [missing, missing, 7, missing, missing,
                                      missing, missing, missing])
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
    # Type inference cannot inspect rows excluded by `limit`.
    limitedtype = K.parse("a\n1\nx\n"; limit=1)
    @test limitedtype[:a] isa Vector{Int64}
    @test limitedtype[:a] == [1]
    @test_throws ArgumentError K.sampletypes(buf, bi.chunks, 1, opts; nsample=0)
    @test_throws ArgumentError K.parse("a\n1\n"; types=Int64, nsample=0)
    @test_throws ArgumentError K.parse("a\n1\n"; chunkbytes=0)
end

@testset "typed: surrounding blanks on every parse path" begin
    # Detection and direct typed storage use the same trimmed span.
    padded = "a\n 1 \n 2 \n 3 \n"
    opts = K.makevalueopts(K.Dialect())
    bytes = Vector{UInt8}(codeunits("\t42 \t"))
    @test K.detecttype(bytes, 1, length(bytes), opts) === Int64
    direct = K.parse(padded; types=Int64, chunkbytes=4, parallel=true)
    @test direct[:a] == [1, 2, 3]

    # The masked driver stages compact output. A later Float64 promotion makes
    # earlier Int64 segments stale and exercises `restale!` with padded cells.
    promoted = "a\n 1 \n 2 \n 3.5 \n"
    masked = K.parse(promoted; rowmask=fill(true, 3), nsample=1,
                     chunkbytes=4, parallel=true)
    @test masked[:a] == [1.0, 2.0, 3.5]

    # The unmasked direct driver performs the same late promotion through
    # `redirect!` into the replacement final column.
    redirected = K.parse(promoted; nsample=1, chunkbytes=4, parallel=true)
    @test redirected[:a] == [1.0, 2.0, 3.5]

    # Typed quoted values trim their content, while String columns retain it.
    @test K.parse("a\n\" 2 \"\n"; types=Int64)[:a] == [2]
    @test collect(K.parse("a\n  x  \n\" y \"\n"; types=String)[:a]) ==
          ["  x  ", " y "]

    # Sentinel matching uses the same blank tolerance without requiring global
    # whitespace stripping, for quoted and unquoted cells.
    sent = K.parse("a\n  NA \n\"\tNA\t\"\n1\n"; sentinels=["NA"])
    @test isequal(collect(sent[:a]), [missing, missing, 1])
    exactblanksentinel = K.parse("a\n  NA \nNA\n"; sentinels=["  NA "], types=String)
    @test isequal(String.(coalesce.(exactblanksentinel[:a], "present-missing")),
                  ["present-missing", "NA"])

    # Interior spaces belong to the syntax. Only outer blanks are removed.
    temporal = K.parse("d\n  Jan 02 2024  \n"; dateformat="u dd yyyy")
    @test temporal[:d] == [Date(2024, 1, 2)]
    grouped = K.parse("n;x\n 1,234 ;ok\n"; delim=';', groupmark=',')
    @test grouped[:n] == [1234]
    bools = K.parse("b\n YES \n NO \n";
                    truestrings=["YES"], falsestrings=["NO"])
    @test bools[:b] == [true, false]
    stripped = K.parse("a\n \n 2 \n"; stripwhitespace=true,
                       ignoreemptyrows=false)
    @test isequal(collect(stripped[:a]), [missing, 2])
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
    @test eltype(ti[:u]) == K.CompactString    # inference never yields UUID/Big types
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
    @test K._degroup!(scratch, Vector{UInt8}(codeunits("1.5,0")), 1, 5,
                      UInt8(','), UInt8('.')) == -2
    @test K._degroup!(scratch, Vector{UInt8}(codeunits("1e1,0")), 1, 5,
                      UInt8(','), UInt8('.')) == -2
    groupedbytes = Vector{UInt8}(codeunits("1,234,567"))
    @test allocsumgrouped(groupedbytes, gmo, scratch) == 0
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
    @test eltype(tg[:a]) == K.CompactString
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
    # The map contains first bytes only. Multiple spellings can share a bit,
    # and a single-byte spelling still requires an exact full-cell match.
    t = K.parse("a,b,c,d,e\nN,NA,NULL,X,NAX\n";
                sentinels=["N", "NA", "NULL", "X"], types=String)
    @test isequal(collect(t[:a]), [missing])
    @test isequal(collect(t[:b]), [missing])
    @test isequal(collect(t[:c]), [missing])
    @test isequal(collect(t[:d]), [missing])
    @test collect(t[:e]) == ["NAX"]
    nosentinel = K.makevalueopts(K.Dialect())
    @test nosentinel.sentfirst == (UInt64(0), UInt64(0), UInt64(0), UInt64(0))
    @test all(!K._maybesentinel(nosentinel, b) for b in typemin(UInt8):typemax(UInt8))
    firstbytes = UInt8[0x00, 0x40, 0x80, 0xc0, 0xff]
    byteopts = K.makevalueopts(K.Dialect(quoted=false);
                               sentinels=[String(UInt8[b, 0x61]) for b in firstbytes])
    @test all(K._maybesentinel(byteopts, b) for b in firstbytes)
    @test !K._maybesentinel(byteopts, 0x7f)
    t = K.parse("NA,b\n1,2\n"; sentinels=["NA"])                     # sentinel header auto-names
    @test K.names(t) == [:Column1, :b]
    # An empty unquoted cell is always missing. A custom sentinel does not
    # change this rule.
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
    # composes with groupmark, select, limit, header choices
    padded2 = "k v\n" * join(("$(iseven(i) ? "ee" : "oo")   $i" for i in 1:50), '\n') * "\n"
    t = K.parse(padded2; delim=' ', ignorerepeated=true, nsample=1, chunkbytes=32)
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
    # unclosed quote at EOF: recorded as a problem, and the cell keeps its RAW
    # bytes (quote included) rather than becoming missing (#1118/#522)
    t = K.parse("a\n\"unclosed")
    @test any(p -> p.kind == :unclosed_quote, K.problems(t))
    @test only(filter(p -> p.kind == :unclosed_quote, K.problems(t))).row == 0
    @test any(p -> p.kind == :invalid_quoted_field, K.problems(t))
    @test isequal(collect(t[:a]), ["\"unclosed"])
    # string escape materialization
    t = K.parse("a\n\"x\"\"y\"\n")
    @test collect(t[:a]) == ["x\"y"]
    # materialize detaches from the buffer
    v = K.materialize(t[:a])
    @test v == ["x\"y"] && v isa Vector{String}
    # A field longer than 2^20 bytes keeps all of its bytes.
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


@testset "CompactString: inline-else-view strings" begin
    # inline/view boundary: 12 bytes inline, 13 views the buffer
    t = K.parse("a\n" * "x"^12 * "\n" * "y"^13 * "\n")
    col = t[:a]
    @test col isa K.CompactStringVector{K.CompactString}
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
    # Exhaust the payload-length byte and use data that includes NUL, invalid
    # UTF-8, all-one bytes, and sentinel-like runs. Odd out-of-line lengths use
    # negative offsets, as escaped values do in a materialized column.
    pattern = UInt8[0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, 0x80, 0xc0, 0x7f, 0x41, 0xfe]
    strings = String[]
    payloads = K.CompactString[]
    for n in 0:255
        bytes = UInt8[xor(pattern[mod1(i, length(pattern))], UInt8(i % 251)) for i in 1:n]
        push!(strings, String(copy(bytes)))
        if n <= K.COMPACTSTRING_INLINE
            push!(payloads, K.CompactString(K.inline_payload(bytes, 1, n), K.EMPTY_BYTES))
        else
            data = vcat(UInt8[0x11], bytes, UInt8[0x22])
            bufidx = isodd(n) ? 1 : 0                 # either buffer index hashes alike
            push!(payloads, K.CompactString(K.view_payload(data, 2, n, bufidx, 1), data))
        end
    end
    # Keep this list portable across word sizes and Julia releases. Private
    # Base hash seeds are not part of Julia's compatibility contract.
    seeds = UInt[0, 1, 7, typemax(UInt), 0x89abcdef]
    @test all(hash(payloads[i], h) == hash(strings[i], h)
              for i in eachindex(payloads), h in seeds)
    @test all([codeunit(payloads[i], j) for j in 1:ncodeunits(payloads[i])] ==
              collect(codeunits(strings[i])) for i in eachindex(payloads))
    for i in eachindex(payloads)
        io = IOBuffer()
        @test write(io, payloads[i]) == ncodeunits(payloads[i])
        @test take!(io) == collect(codeunits(strings[i]))
        @static if isdefined(Base, :AnnotatedIOBuffer)
            annotated = Base.AnnotatedIOBuffer(IOBuffer())
            @test write(annotated, payloads[i]) == ncodeunits(payloads[i])
            @test take!(annotated.io) == collect(codeunits(strings[i]))
        end
    end
    @test all(begin
        n = ncodeunits(payloads[i])
        bytes = csscratchbytes(payloads[i])
        bytes[1:n] == collect(codeunits(strings[i])) &&
            all(iszero, bytes[(n + 1):end])
    end for i in 1:(K.COMPACTSTRING_INLINE + 1))
    @test all(cmp(payloads[i], payloads[j]) == cmp(strings[i], strings[j]) &&
              isless(payloads[i], payloads[j]) == isless(strings[i], strings[j]) &&
              cmp(payloads[i], strings[j]) == cmp(strings[i], strings[j]) &&
              cmp(strings[i], payloads[j]) == cmp(strings[i], strings[j]) &&
              (payloads[i] == payloads[j]) == (strings[i] == strings[j]) &&
              (payloads[i] == strings[j]) == (strings[i] == strings[j])
              for i in eachindex(payloads), j in eachindex(payloads))
    @test sortperm(payloads) == sortperm(strings)
    valid = ["a", "abcdefgh1234", "abcdefgh12345", "α", "漢字", "z"^40, "a\0b"]
    validcs = [csfrombytes(Vector{UInt8}(codeunits(s))) for s in valid]
    substrings = [SubString("!" * s * "?", 2,
                            prevind("!" * s * "?", lastindex("!" * s * "?"))) for s in valid]
    @test all(cmp(validcs[i], substrings[j]) == cmp(valid[i], String(substrings[j])) &&
              cmp(substrings[j], validcs[i]) == cmp(String(substrings[j]), valid[i]) &&
              (validcs[i] == substrings[j]) == (valid[i] == String(substrings[j]))
              for i in eachindex(validcs), j in eachindex(substrings))
    @test isless(first(validcs), missing) == isless(first(valid), missing)
    @test isless(missing, first(validcs)) == isless(missing, first(valid))
    @test allocfoldcshash(payloads, UInt(9)) == 0
    # ordering and equality across every inline/view mix stay allocation-free
    # (the inline fast path in registers, the rest through stack scratches)
    @test allocfoldcscmp(payloads) == 0
    # escaped values: short ones inline, long ones land in the extra buffer
    t2 = K.parse("a\n\"in\"\"line\"\n\"a long escaped \"\"string\"\" beyond inline\"\n")
    @test collect(t2[:a]) == ["in\"line", "a long escaped \"string\" beyond inline"]
    @test !isempty(t2[:a].extra)                      # long unescaped value stored out-of-line
    @test K.csbufidx(t2[:a].payloads[2]) == 1       # buffer index 1 ⇒ extra buffer
    # Buffer indices above one select later bounded owned buffers.
    overflowvalue = "a value in the second owned buffer"
    overflowbytes = Vector{UInt8}(codeunits(overflowvalue))
    overflowpayload = K.view_payload(overflowbytes, 1, length(overflowbytes), 2, 0)
    overflowcol = K.CompactStringVector{K.CompactString}(
        [overflowpayload], UInt8[], UInt8[], [overflowbytes])
    @test String(overflowcol[1]) == overflowvalue
    @test K.materialize(overflowcol) == [overflowvalue]

    # Exercise the large-offset fallback and owned-buffer rollover on every
    # platform with small injected limits. The production limits remain Int32.
    forcedvalue = "forced owned-buffer value"
    forcedbuf = Vector{UInt8}(codeunits(forcedvalue * "\n"))
    forcedci = K.ChunkIndex(1, length(forcedbuf))
    forceddialect = K.Dialect()
    K.indexone!(forcedci, forcedbuf, forceddialect, :scalar)
    forcedcol = K.StringColumn(1, forcedbuf, UInt8('"'), UInt8('"'))
    forcedlog = K.ProblemLog(10)
    K.parsecolchunk!(forcedcol, forcedbuf, forcedci, 1, 0,
                     K.makevalueopts(forceddialect), true, forcedlog;
                     viewoffsetlimit=-1)
    @test K.csbufidx(forcedcol.payloads[1]) == 1
    @test forcedcol.extra == Vector{UInt8}(codeunits(forcedvalue))
    @test isempty(forcedlog.items)

    rollover = K.StringColumn(fill(K.PAYLOAD_MISSING, 1), forcedbuf,
                              fill(UInt8(0xaa), 8), ReentrantLock(),
                              UInt8('"'), UInt8('"'))
    maps = K._copyownedbuffers!(rollover, forcedcol, 32)
    repointed = K._repointowned(forcedcol.payloads[1], maps)
    rollovervec = K.CompactStringVector{K.CompactString}(
        [repointed], forcedbuf, rollover.extra, rollover.overflow)
    @test K.csbufidx(repointed) == 2
    @test String(rollovervec[1]) == forcedvalue
    @test length(rollover.overflow) == 1

    # A long string whose absolute source position is beyond Int32 is copied
    # into a small owned buffer. The sparse file consumes only the written
    # pages, while the mmap gives the parser a real >2 GiB Vector{UInt8}.
    if Sys.WORD_SIZE == 64 && Sys.isunix()
        mktemp() do _, io
            offset0 = Int(typemax(Int32)) + 4096
            largeposvalue = "large-offset string value"
            seek(io, offset0)
            write(io, largeposvalue, '\n')
            flush(io)
            seekstart(io)
            mapped = Mmap.mmap(io, Vector{UInt8}, filesize(io))
            ci = K.ChunkIndex(offset0 + 1, filesize(io))
            dialect = K.Dialect()
            K.indexone!(ci, mapped, dialect, :scalar)
            stringcol = K.StringColumn(1, mapped, UInt8('"'), UInt8('"'))
            log = K.ProblemLog(10)
            K.parsecolchunk!(stringcol, mapped, ci, 1, 0,
                             K.makevalueopts(dialect), true, log)
            parsed = K.finalizecolumn(String, stringcol, 1)
            @test String(parsed[1]) == largeposvalue
            @test K.csbufidx(parsed.payloads[1]) == 1
            @test parsed.extra == Vector{UInt8}(codeunits(largeposvalue))
            @test isempty(parsed.overflow)
            @test isempty(log.items)
        end
    end
    # quoted empty vs missing, unicode, Symbol
    t3 = K.parse("a\n\"\"\n\nαβγδεζηθικλμ\n"; ignoreemptyrows=false)
    @test isequal(collect(t3[:a]), ["", missing, "αβγδεζηθικλμ"])
    @test Symbol(t3[:a][3]) == :αβγδεζηθικλμ
    # Iteration must match String, including invalid UTF-8.
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
        v = csfrombytes(copy(bytes))
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
    colv = big[:a]::K.CompactStringVector{K.CompactString}
    @test allocsumncodeunits(colv) == 0
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
    t = E._readtable(csv)
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
    # Put the only float after 1,000 integer rows. Put a missing value in only
    # one batch. All batches must still have the same Union element type.
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
    rs = collect(E._indexedrows(csv))
    @test length(rs) == 3
    @test rs[1].a == "1"                          # untyped access materializes strings
    @test rs[3][:b] == "z,w"
    @test E._typedvalue(Int64, rs[1], :a) == 1
    @test E._typedvalue(Float64, rs[2], 3) == 3.5
    @test E._typedvalue(String, rs[3], :b) == "z,w"
    @test ismissing(E._typedvalue(Int64, rs[1], :b))  # not parseable as Int
    prs = collect(E._indexedrows(padded; delim=' ', ignorerepeated=true, chunkbytes=3))
    @test [row.a for row in prs] == ["1", "3"]
    @test [E._typedvalue(Int64, row, :b) for row in prs] == [2, 4]
    # ragged row: missing beyond the row's fields
    rs2 = collect(E._indexedrows("a,b\n1\n"))
    @test ismissing(rs2[1][:b])
    @test_throws BoundsError rs2[1][0]
    @test_throws BoundsError E._typedvalue(Int64, rs2[1], 3)
    # Rows declares the Tables.jl row interface, including a concrete schema.
    rows = E._indexedrows(csv)
    @test Tables.istable(typeof(rows)) && Tables.rowaccess(typeof(rows))
    @test Tables.rows(rows) === rows
    # untyped rows are lazy CompactString views (zero-copy); == against String literals holds
    @test Tables.schema(rows).types ==
          (Union{K.CompactString, Missing}, Union{K.CompactString, Missing}, Union{K.CompactString, Missing})
    @test Tables.rowtable(rows)[1] == (a="1", b="x", c="2.5")
    # A CSV column name takes priority over RowView's private storage fields.
    row = first(E._indexedrows("r,rownumber\nvalue,7\n"))
    @test row.r == "value" && row.rownumber == "7"
end

@testset "sequential multi-chunk driver consistency" begin
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
    @test eltype(seq[:mix]) == Union{K.CompactString, Missing}
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

@testset "direct final slice coverage" begin
    quote_csv(s) = "\"" * replace(s, "\"" => "\"\"") * "\""
    nums = Union{Int64, Missing}[]
    texts = Union{String, Missing}[]
    escaped = Bool[]
    lines = String[]
    for i in 1:80
        i % 11 == 0 && push!(lines, "# dropped row $i")
        i % 13 == 0 && push!(lines, "")
        n = i % 5 == 0 ? "" : string(i)
        push!(nums, isempty(n) ? missing : i)
        if i % 7 == 0
            s = ""
            push!(texts, missing)
            push!(escaped, false)
        elseif isodd(i)
            s = "escaped value $i with \"quote\" tail"
            push!(texts, s)
            push!(escaped, true)
            s = quote_csv(s)
        else
            s = "plain view-backed value $i tail"
            push!(texts, s)
            push!(escaped, false)
        end
        push!(lines, "$n,$s")
    end
    input = join(lines, '\n') * "\n"
    buf = Vector{UInt8}(codeunits(input))
    cb = 96
    d = K.Dialect(comment="#")
    bi = K.index(buf, d; chunkbytes=cb, parallel=false)
    counts = K.nrows.(bi.chunks)
    bases = cumsum([0; counts[1:end - 1]])
    boundaries = cumsum(counts)
    boundary = first(b for b in boundaries if 0 < b < length(nums))
    midchunk = findfirst(>(2), counts)::Int
    mid = bases[midchunk] + 1
    @test mid ∉ boundaries
    @test sum(counts) == length(nums) && length(counts) > 2
    limits = unique([0, boundary, mid, length(nums), length(nums) + 5])
    for useindex in (false, true), par in (false, true), lim in limits
        kw = useindex ? (; index=bi) : (; chunkbytes=cb)
        t = K.parse(buf; header=[:num, :txt], types=[Int64, String], comment="#",
                    limit=lim, parallel=par, kw...)
        n = min(lim, length(nums))
        @test isequal(collect(t[:num]), nums[1:n])
        @test isequal(K.materialize(t[:txt]), texts[1:n])
        expectedextra = Vector{UInt8}(codeunits(join(texts[i] for i in 1:n if escaped[i])))
        @test t[:txt].extra == expectedextra
    end
    one = K.parse(buf; header=[:num, :txt], types=[Int64, String], comment="#",
                  chunkbytes=length(buf) + 1, parallel=false)
    @test isequal(collect(one[:num]), nums)
    @test isequal(K.materialize(one[:txt]), texts)

    # nsample=1 seeds Missing from the first row. Sequential chunks complete as
    # Missing before the final value promotes the UNDEF direct final to Int64.
    missingfirst = join([fill("", 30); "7"], '\n') * "\n"
    for par in (false, true)
        for _ in 1:(par ? 5 : 1)
            t = K.parse(missingfirst; header=[:x], ignoreemptyrows=false, nsample=1,
                        chunkbytes=1, parallel=par)
            @test isequal(collect(t[:x]), [fill(missing, 30); 7])
            @test isempty(K.problems(t))
        end
    end
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

@testset "bounded reader task count" begin
    @test_throws ArgumentError K.parse("a\n1\n"; ntasks=0)
    @test_throws ArgumentError K.index(UInt8[0x61]; ntasks=0)

    # The worker helper visits every item once and never exceeds its bound.
    seen = zeros(Int, 64)
    active = Ref(0)
    peak = Ref(0)
    guard = ReentrantLock()
    K._taskforeach(eachindex(seen), 2) do i
        lock(guard) do
            active[] += 1
            peak[] = max(peak[], active[])
        end
        yield()
        sleep(0.001)
        seen[i] += 1
        lock(guard) do
            active[] -= 1
        end
    end
    @test all(==(1), seen)
    @test peak[] <= 2

    # Exercise the complete index path with many small byte ranges. The
    # observer counts live index workers. A short wait lets all allowed workers
    # start before any one worker can finish.
    indexactive = Ref(0)
    indexpeak = Ref(0)
    indexguard = ReentrantLock()
    function observeindex(started::Bool)
        if started
            lock(indexguard) do
                indexactive[] += 1
                indexpeak[] = max(indexpeak[], indexactive[])
            end
            sleep(0.01)
        else
            lock(indexguard) do
                indexactive[] -= 1
            end
        end
        return nothing
    end
    indexinput = Vector{UInt8}(codeunits(repeat("a,\"line\nvalue\"\n", 80)))
    indexreference = K.index(indexinput; chunkbytes=3, parallel=false)
    boundedindex = K.index(indexinput; chunkbytes=3, parallel=true, ntasks=2,
                           _taskobserver=observeindex)
    expectedlimit = min(2, Threads.nthreads())
    @test length(boundedindex.chunks) > 10
    @test indexsnapshot(indexinput, boundedindex) ==
          indexsnapshot(indexinput, indexreference)
    @test indexactive[] == 0
    @test indexpeak[] <= expectedlimit
    Threads.nthreads() > 1 && @test(indexpeak[] == expectedlimit)

    indexpeak[] = 0
    K.index(indexinput; chunkbytes=3, parallel=true,
            ntasks=Threads.nthreads() + 7, _taskobserver=observeindex)
    @test indexactive[] == 0
    @test indexpeak[] <= Threads.nthreads()

    lines = ["a,b,c"]
    for i in 1:500
        b = i % 17 == 0 ? "\"escaped \"\"value\"\" $i\"" : "plain value $i"
        c = i % 23 == 0 ? "" : string(i / 3)
        push!(lines, "$i,$b,$c")
    end
    input = join(lines, '\n') * "\n"
    reference = K.parse(input; chunkbytes=64, parallel=true, ntasks=1)
    mask = [isodd(i) for i in 1:500]
    maskedreference = K.parse(input; chunkbytes=64, parallel=true, ntasks=1,
                              rowmask=mask)
    for nt in (2, 4)
        parsed = K.parse(input; chunkbytes=64, parallel=true, ntasks=nt)
        @test parsed.names == reference.names
        @test collect(parsed[:a]) == collect(reference[:a])
        @test K.materialize(parsed[:b]) == K.materialize(reference[:b])
        @test isequal(collect(parsed[:c]), collect(reference[:c]))
        @test K.problems(parsed) == K.problems(reference)

        masked = K.parse(input; chunkbytes=64, parallel=true, ntasks=nt,
                         rowmask=mask)
        @test collect(masked[:a]) == collect(maskedreference[:a])
        @test K.materialize(masked[:b]) == K.materialize(maskedreference[:b])
        @test isequal(collect(masked[:c]), collect(maskedreference[:c]))
        @test K.problems(masked) == K.problems(maskedreference)
    end
end

@testset "concurrency hygiene: no boxed captures in spawn-containing methods" begin
    # A variable captured by a task body or closure that is assigned more than
    # once is lowered to a shared, mutable `Core.Box`: the exact hazard
    # `Threads.@spawn`'s `\$x` interpolation exists for (and, incidentally, an
    # `Any`-typed load inside every task that reads it). `for`-loop iteration
    # variables are fresh per iteration and never need `\$`; this invariant
    # guards the OTHER kind — driver locals reassigned in a later branch. Rather
    # than eyeball 17 spawn sites, lower every method that spawns and assert
    # no box survives.
    boxed = String[]
    for m in (CSV,)
        for name in names(m; all=true)
            f = try getfield(m, name) catch; continue end
            f isa Function || continue
            for meth in methods(f)
                meth.module === m || continue
                ci = try Base.uncompressed_ast(meth) catch; continue end
                lines = string.(ci.code)
                any(l -> occursin("@spawn", l) || occursin("Threads.Task", l) ||
                         occursin("Base.Threads", l), lines) || continue
                nb = count(l -> occursin("Core.Box()", l), lines)
                nb == 0 || push!(boxed, "$(meth.name): $nb boxed captures")
            end
        end
    end
    @test isempty(boxed)
    # Each task must keep its own loop value after the loop starts later tasks.
    seen = zeros(Int, 64)
    @sync for b in 1:64
        Threads.@spawn begin
            sleep(0.001 * (65 - b) / 64)   # later iterations finish FIRST
            seen[b] = b
        end
    end
    @test seen == 1:64
end

end # top-level testset
