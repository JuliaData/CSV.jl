# Bounded, deterministic parser differential tests.
#
# These cases target composition boundaries rather than raising the test count:
# malformed bytes must give the same table and diagnostics for every scanner,
# chunk geometry, and scheduling mode; well-formed generated CSV must agree
# with the frozen 0.10 parser on the compatibility surface.
if !isdefined(@__MODULE__, :LegacyCSV)
    include(joinpath(@__DIR__, "LegacyCSV", "src", "LegacyCSV.jl"))
end

module CSVFuzzTests

using Test, Random, Tables
using CSV
import ..LegacyCSV

const K = CSV.CSVKernel

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

@testset "deterministic fuzz and differential" begin
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

    @testset "well-formed public parse agrees with the 0.10 oracle" begin
        seed = 0x43535632
        rng = MersenneTwister(seed)
        atoms = Union{Missing, String}[
            missing, "plain", "with,comma", "with;semi", "with|pipe",
            "quote\"mark", "line\nfeed", "carriage\rreturn", " leading",
            "trailing ", "lambda-λ", "digits-00123",
        ]
        for trial in 1:96
            delim = rand(rng, (',', ';', '\t', '|'))
            ncols = rand(rng, 1:5)
            nrows = rand(rng, 1:20)
            names = ["c$j" for j in 1:ncols]
            rows = [[rand(rng, atoms) for _ in 1:ncols] for _ in 1:nrows]
            io = IOBuffer()
            print(io, join(names, delim))
            print(io, rand(rng, ("\n", "\r\n", "\r")))
            for (r, row) in enumerate(rows)
                print(io, join((_encodecell(x, delim) for x in row), delim))
                r < nrows && print(io, rand(rng, ("\n", "\r\n", "\r")))
            end
            rand(rng, Bool) && print(io, rand(rng, ("\n", "\r\n", "\r")))
            input = String(take!(io))
            old = LegacyCSV.File(IOBuffer(input); delim, types=String,
                                 ignoreemptyrows=false, silencewarnings=true)
            expected = _tablenorm(old)
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

end # module CSVFuzzTests
