# Bounded, deterministic parser fuzz tests.
#
# These cases test parser boundaries instead of adding many similar cases:
# malformed bytes must give the same table and diagnostics for every scanner,
# chunk geometry, and scheduling mode. Well-formed generated CSV must preserve
# the values that the generator wrote.

module CSVFuzzTests

using Test, Random, Tables
using CSV

const K = CSV

_normcell(x) = ismissing(x) ? missing : collect(codeunits(String(x)))
function _snapshot(t)
    return (
        names = K.names(t),
        types = map(eltype, K.columns(t)),
        values = [[_normcell(x) for x in c] for c in K.columns(t)],
        nrows = t.nrows,
        problems = [(p.row, p.col, p.pos, p.kind, p.message) for p in K.problems(t)],
        dropped = t.droppedproblems,
    )
end

function _parsesnapshot(bytes, kw, chunkbytes, parallel, scanner)
    t = K.parse(copy(bytes); header=false, types=String, chunkbytes,
                parallel, scanner, kw...)
    return _snapshot(t)
end

function _tablenorm(t)
    cols = Tables.columns(t)
    names = collect(Symbol, Tables.columnnames(cols))
    values = [[_normcell(x) for x in Tables.getcolumn(cols, nm)] for nm in names]
    return (; names, nrows=isempty(values) ? 0 : length(first(values)), values)
end

function _encodecell(x, delim::Char)
    ismissing(x) && return ""
    s = String(x)
    if occursin(delim, s) || occursin('"', s) || occursin('\r', s) || occursin('\n', s)
        return "\"" * replace(s, "\"" => "\"\"") * "\""
    end
    return s
end

@testset "deterministic parser fuzz" begin
    @testset "malformed bytes: scanner, chunk, and task determinism" begin
        seed = 0x43535631
        rng = MersenneTwister(seed)
        alphabet = UInt8[
            0x00, 0x01, 0x09, 0x0a, 0x0d, 0x20, 0x22, 0x23, 0x27, 0x2c,
            0x3b, 0x5c, 0x7c, 0x7f, 0x80, 0xc0, 0xef, 0xff,
            codeunits("abcXYZ019+-.eEtruefalseNA")...,
        ]
        edges = [0:12; 31:33; 62:66; 126:130; 254:258]
        for trial in 1:256
            n = trial <= length(edges) ? edges[trial] : rand(rng, 0:256)
            bytes = rand(rng, alphabet, n)
            kw = (
                delim = rand(rng, (',', ';', '|', ' ')),
                quoted = rand(rng, Bool),
                comment = rand(rng, (nothing, "#")),
                ignoreemptyrows = rand(rng, Bool),
                ignorerepeated = rand(rng, Bool),
                maxproblems = 19,
            )
            baseline = _parsesnapshot(bytes, kw, max(1, n + 1), false, :scalar)
            variants = (
                (1, false, :scalar),
                (3, false, :scalar),
                (63, false, :swar),
                (64, false, :vec),
                (65, true, :auto),
                (max(1, n + 1), true, :auto),
            )
            @testset "seed=$(string(seed, base=16)) trial=$trial" begin
                for (chunkbytes, parallel, scanner) in variants
                    got = _parsesnapshot(bytes, kw, chunkbytes, parallel, scanner)
                    @test isequal(got, baseline)
                end
            end
        end
    end

    @testset "well-formed public parse preserves generated rows" begin
        seed = 0x43535632
        rng = MersenneTwister(seed)
        atoms = Union{Missing, String}[
            missing, "plain", "with,comma", "with;semi", "with|pipe",
            "quote\"mark", "line\nfeed", "carriage\rreturn", " leading",
            "trailing ", "lambda-λ", "digits-00123",
        ]
        newlines = ("\n", "\r\n", "\r")
        for trial in 1:96
            delim = rand(rng, (',', ';', '\t', '|'))
            ncols = rand(rng, 1:5)
            nrows = rand(rng, 1:20)
            names = ["c$j" for j in 1:ncols]
            rows = [[rand(rng, atoms) for _ in 1:ncols] for _ in 1:nrows]
            newline = newlines[mod1(trial, length(newlines))]
            io = IOBuffer()
            print(io, join(names, delim))
            print(io, newline)
            for (r, row) in enumerate(rows)
                print(io, join((_encodecell(x, delim) for x in row), delim))
                r < nrows && print(io, newline)
            end
            trailingnewline = rand(rng, Bool)
            # A final empty row in a one-column file has no bytes of its own.
            # Add a line ending so the input contains that row.
            finalrowisempty = ncols == 1 && ismissing(rows[end][1])
            (trailingnewline || finalrowisempty) && print(io, newline)
            input = String(take!(io))
            expected = (
                names = Symbol.(names),
                nrows,
                values = [[_normcell(rows[r][j]) for r in 1:nrows] for j in 1:ncols],
            )
            @testset "seed=$(string(seed, base=16)) trial=$trial" begin
                for chunkbytes in (1, 3, 63, 64, 65, ncodeunits(input) + 1)
                    for parallel in (false, true)
                        new = CSV.File(IOBuffer(input); delim, types=String,
                                       ignoreemptyrows=false, chunkbytes, parallel)
                        @test isequal(_tablenorm(new), expected)
                    end
                end
            end
        end
    end
end


# Compare the structural output without depending on chunk-local offsets.
function _rawrows(buf, bi)
    [[begin
        pos, len = K.fieldspan(ci, r, j)
        String(buf[pos:pos + len - 1])
      end for j in 1:K.nfields(ci, r)]
     for ci in bi.chunks for r in 1:K.totalrows(ci)]
end

function _ownedcheck(f, expected)
    @test isequal(_tablenorm(f), expected)
    for col in Tables.Columns(f)
        col isa K.DataStringVector || continue
        @test col.buffers[1] === K.EMPTY_BYTES
        @test all(col.payloads) do p
            K.payloadlen(p) <= K.INLINE_MAX && return true
            idx = K.payloadbufidx(p) + 1
            2 <= idx <= length(col.buffers) || return false
            0 <= K.payloadoffset(p) && K.payloadoffset(p) + K.payloadlen(p) <= length(col.buffers[idx])
        end
    end
end

@testset "D1/D2 adversarial kernels" begin
    rng = MersenneTwister(0x43535633)
    sizes = (1, 7, 63, 64, 65, 127, 1024, 1 << 20)
    scanners = (:vec, :swar, :scalar)
    @testset "whitespace carry and bare quotes at every block position" begin
        for delim in (',', ' ', '\t'), pad in 0:130
            for bare in (false, true)
                field = bare ? "x\"y" : "\"x\""
                bytes = Vector{UInt8}("a" * delim * " "^pad * field * delim * "z\n" * "b"^80 * "\n")
                for sc in scanners, cb in (7, 64, 1 << 20)
                    bi = K.index(bytes, K.Dialect(; delim); scanner=sc, chunkbytes=cb)
                    @test bi.barequote == bare
                end
            end
        end
    end
    @testset "writer dialects, strict/lenient rows, and owned bytes" begin
        for trial in 1:32
            delim = rand(rng, (",", ";", "\t", " ", "::"))
            oq, cq, esc = rand(rng, (('"', '"', '"'), ('\'', '\'', '\''),
                                      ('<', '>', '\\'), ('"', '"', '\\')))
            newline = rand(rng, ("\n", "\r\n"))
            comment = rand(rng, (nothing, "#"))
            repeated = rand(rng, Bool)
            dialect = (; delim, openquotechar=oq, closequotechar=cq, escapechar=esc)
            atoms = ["plain", "", "λ漢🙂", "line\n#inside", "a" * delim * "b",
                     "q$(oq)u$(cq)ote", " slash\\tail ", "x"^rand(rng, 13:160)]
            tbl = (id=string.(1:16), s=[rand(rng, atoms) for _ in 1:16],
                   t=[rand(rng, atoms) for _ in 1:16])
            if !repeated
                tbl = merge(tbl, (; s=Union{Missing, String}[missing; tbl.s[2:end]]))
            end
            io = IOBuffer()
            K.write(io, tbl; dialect..., newline, quotestyle=rand(rng, (:all, :minimal)))
            bytes = take!(io)
            if comment !== nothing
                bytes = [Vector{UInt8}("# ignored $(oq) unmatched" * newline); bytes]
            end
            d = K.Dialect(; dialect..., comment, ignorerepeated=repeated)
            reference = K.index(bytes, d; scanner=:scalar, parallel=false, chunkbytes=1 << 20)
            rows = _rawrows(bytes, reference)
            expected = _tablenorm(tbl)
            @testset "trial=$trial" begin
                for cb in sizes, sc in scanners
                    bi = K.index(bytes, d; scanner=sc, chunkbytes=cb)
                    @test !bi.barequote
                    @test _rawrows(bytes, bi) == rows
                    li = K.index(bytes, K.withlenient(d); scanner=sc, chunkbytes=cb)
                    @test _rawrows(bytes, li) == rows
                end
                for cb in sizes, parallel in (false, true)
                    input = copy(bytes)
                    kw = (; dialect..., comment, ignorerepeated=repeated, chunkbytes=cb, parallel)
                    baseline = K.File(copy(input); kw..., types=String)
                    @test isequal(_tablenorm(baseline), expected)
                    f = K.File(input; kw..., types=K.DataString)
                    _ownedcheck(f, expected)
                    fill!(input, 0x00)
                    _ownedcheck(f, expected)
                end
            end
        end
    end
    @testset "injected bare quotes agree across readers" begin
        for trial in 1:24
            delim = rand(rng, (",", ";", "::", "\t"))
            oq, cq, esc = rand(rng, (('"', '"', '"'), ('<', '>', '\\'), ('"', '"', '\\')))
            dialect = (; delim, openquotechar=oq, closequotechar=cq, escapechar=esc)
            newline = rand(rng, ("\n", "\r\n"))
            values = ["value" * "x"^rand(rng, 1:80) for _ in 1:8]
            row = rand(rng, eachindex(values))
            pos = rand(rng, 2:length(values[row]))
            values[row] = values[row][1:pos-1] * oq * values[row][pos:end]
            input = "id$(delim)s$(newline)" * join(("$i$(delim)$(values[i])" for i in eachindex(values)), newline) * newline
            bytes = Vector{UInt8}(input)
            expected = _tablenorm((id=string.(1:8), s=values))
            for cb in (1, 63, 64, 65, 1 << 20), sc in scanners
                @test K.index(bytes, K.Dialect(; dialect...); scanner=sc, chunkbytes=cb).barequote
                kw = (; dialect..., chunkbytes=cb, scanner=sc, types=String)
                @test isequal(_tablenorm(K.File(copy(bytes); kw...)), expected)
                @test isequal(_tablenorm(K.lazy(copy(bytes); kw...)), expected)
                @test isequal(_tablenorm(Tables.columntable(K.Rows(copy(bytes); kw...))), expected)
                batches = collect(K.Chunks(copy(bytes); kw...))
                @test vcat((collect(b.s) for b in batches)...) == values
                @test vcat((collect(b.id) for b in batches)...) == string.(1:8)
                scan = Tables.Scan(select=(:id => String, :s => String), filter=Tables.col(:id) > 1)
                f = K.File(copy(bytes); dialect..., chunkbytes=cb, scanner=sc, scan)
                @test collect(f.s) == values[2:end]
                @test collect(f.id) == string.(2:8)
            end
            # quoted=false must never request a lenient re-index.
            @test !K.index(bytes, K.Dialect(; dialect..., quoted=false)).barequote
        end
    end
    @testset "late promotion, masked adoption, and retained scalars" begin
        values = [string(rand(rng, Int64(10)^14:Int64(10)^15)) for _ in 1:300]   # 32-bit safe
        values[151] = "long text " * "λ"^40
        values[end] = "escaped \""^20
        io = IOBuffer()
        K.write(io, (id=1:300, s=values))
        bytes = take!(io)
        for cb in sizes, parallel in (false, true), filtered in (false, true)
            kw = filtered ? (; scan=Tables.Scan(filter=Tables.col(:id) > 100)) : (;)
            baseline = K.File(copy(bytes); chunkbytes=cb, parallel, nsample=1, stringtype=String, kw...)
            input = copy(bytes)
            f = K.File(input; chunkbytes=cb, parallel, nsample=1, kw...)
            # id is numeric in this case; check the owned text column separately.
            col = f.s
            @test col.buffers[1] === K.EMPTY_BYTES
            @test String.(col) == baseline.s
            retained = col[end]
            before = String(retained)
            fill!(input, 0x00)
            @test String.(col) == values[(filtered ? 101 : 1):end]
            @test all(p -> K.payloadlen(p) <= K.INLINE_MAX ||
                      (1 <= K.payloadbufidx(p) < length(col.buffers) &&
                       K.payloadoffset(p) + K.payloadlen(p) <= length(col.buffers[K.payloadbufidx(p) + 1])), col.payloads)
            col[end] = "replacement text longer than inline"
            @test String(retained) == before
        end
    end
end

end # module CSVFuzzTests
