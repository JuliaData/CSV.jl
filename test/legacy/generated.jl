# The generated corpus: the tiny hand-made files the 0.10 suite accumulated
# each pinned one dialect/type edge. bench/bench_matrix.jl's shape generators
# produce every one of those edges parametrically — numeric/float/mixed/
# strings/quoted/escaped/longtext/wide/verywide/longnarrow/sparse/missing90/
# pooled_*/temporal/bools/groupmark/irspace/dirty/crlf/sentinel — so instead
# of N static files we run shapes × sizes × option variants through both
# implementations and assert agreement. Broader than the files it replaces
# (chunk-boundary rows land everywhere; every size class), zero bytes on disk.
#
# Uses the same `agree` harness (names, sizes, schema classes, values, error
# categories) and the same pinned-delta discipline.

include(joinpath(@__DIR__, "..", "..", "bench", "bench_matrix.jl"))   # generators only (main is guarded)

# shapes whose 0.10 behavior is a documented delta would be pinned here; the
# generator's `dirty` shape has SHORT rows + bad cells (both sides pad with
# missing and promote to String, so they agree) — long-row widening, the real
# delta, is pinned by the hand corpus (issue_198_part2.csv)
const GEN_DELTAS = Dict{Symbol, String}()
# shapes 0.10 cannot parse at all (its kwargs lack the feature): our side only
const GEN_OURS_ONLY = (:irspace,)   # 0.10 has ignorerepeated but auto-detect + repeated space differs

const GEN_SHAPES = (:numeric, :floatonly, :mixed, :strings, :quoted, :escaped,
                    :longtext, :wide, :verywide, :longnarrow, :sparse, :missing90,
                    :pooled_low, :pooled_high, :pooled_overcap, :temporal, :bools,
                    :groupmark, :crlf, :sentinel, :dirty)
const GEN_SIZES = (2_000, 40_000, 400_000)          # single-chunk .. several chunks at 64K floor

@testset "generated corpus: shapes × sizes vs 0.10" begin
    for shape in GEN_SHAPES, sz in GEN_SIZES
        shape in GEN_OURS_ONLY && continue
        bytes = makedata(shape, sz)
        apikw, csvkw = shapekw(shape)
        label = "gen:$(shape):$(sz)"
        delta = get(GEN_DELTAS, shape, nothing)
        # both sides get their own kwargs (they differ only where 0.10 spells
        # a feature differently); the harness compares outputs
        @case label begin
            fnew = try NEW.File(copy(bytes); _newkw(apikw)...) catch e; e end
            fold = try OLD.File(copy(bytes); _oldkw(csvkw)...) catch e; e end
            newerr, olderr = fnew isa Exception, fold isa Exception
            outcome = if newerr && olderr
                :both_error
            elseif newerr
                :new_errors
            elseif olderr
                :old_errors
            else
                nn, vn = _table(fnew)[1], _table(fnew)[end]
                no, vo = _table(fold)[1], _table(fold)[end]
                (nn == no && isequal(vn, vo)) ? :agree : :differ
            end
            _recordoutcome!(label, outcome)
            if delta === nothing
                ok = outcome in (:agree, :both_error)
                @test ok
                ok || @info "generated corpus disagreement" label outcome newerr olderr
            else
                @test outcome != :agree
            end
        end
        # our side must round-trip through the writer at every size/shape too
        @case label * ":roundtrip" begin
            fnew = NEW.File(copy(bytes); _newkw(apikw)...)
            io = IOBuffer(); NEW.write(io, fnew; _writekw(apikw)...)
            back = NEW.File(take!(io); _newkw(apikw)...)
            @test _table(back)[1] == _table(fnew)[1]
            @test isequal(_table(back)[end], _table(fnew)[end])
        end
    end
    expected = Set("gen:$(shape):$(sz)" for shape in GEN_SHAPES
                   for sz in GEN_SIZES if !(shape in GEN_OURS_ONLY))
    observed = Set(k for k in keys(OUTCOMES) if startswith(k, "gen:"))
    @test observed == expected
    @test !any(endswith(":roundtrip"), keys(OUTCOMES))
end
