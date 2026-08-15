# The legacy-corpus differential harness.
#
# The 0.10 test suite is ~800 assertions of the shape
#     f = CSV.File(<input>; <kwargs>); @test f.col == [...]
# Rather than hand-port each, we replay every File call against BOTH
# implementations and assert agreement on names, row count, and values
# (string wrappers and pooling normalized away). The 0.10 implementation is
# the oracle; a disagreement is either a kernel bug (fix it) or a pinned 1.0
# delta (record it in DELTAS below with a reason — the harness then asserts
# the NEW behavior instead so the delta stays pinned).
#
# `corpusfile(name)` resolves the LazyArtifact corpus; small files were
# inlined into cases.jl as literals during the audit (see AUDIT.md).

using Test, Tables, Dates, LazyArtifacts, PooledArrays
using CSV, LegacyCSV
const NEW = CSV
const OLD = LegacyCSV
# resolve the corpus through the PACKAGE's Artifacts.toml regardless of how
# this file is included (Pkg.test, or run standalone from test/)
const corpusdir = LazyArtifacts.ensure_artifact_installed("testfiles",
    joinpath(dirname(dirname(pathof(CSV))), "Artifacts.toml"))
corpusfile(name) = joinpath(corpusdir, name)

_norm(x) = x isa AbstractString ? String(x) : x
_normvec(v) = Any[_norm(x) for x in v]

function _table(f)
    cols = Tables.columns(f)
    names = collect(Symbol, Tables.columnnames(cols))
    return names, [_normvec(Tables.getcolumn(cols, nm)) for nm in names]
end

# kwargs the 0.10 side accepts but the new side spells differently / retired
function _newkw(kw)
    d = Dict{Symbol, Any}(pairs(kw))
    haskey(d, :silencewarnings) && delete!(d, :silencewarnings)
    haskey(d, :ntasks) && (d[:ntasks] = d[:ntasks])       # supported as-is
    if haskey(d, :lazystrings)
        delete!(d, :lazystrings)
    end
    return (; d...)
end
function _oldkw(kw)
    d = Dict{Symbol, Any}(pairs(kw))
    d[:silencewarnings] = true
    return (; d...)
end

"""
    agree(input; kw...)

Replay one File call on both implementations; assert equal names, sizes, and
values. Returns the NEW File. `input` may be a String literal (wrapped in
IOBuffer for both), an IO, bytes, or a corpus path.
"""
# Outcome classes, so one run yields the COMPLETE disagreement ledger:
#   :agree        both parse, same names/sizes/values
#   :differ       both parse, values/names differ           -> triage
#   :new_errors   old parses, new throws                    -> triage
#   :old_errors   new parses, old throws                    -> triage
#   :both_error   both throw (agreement on rejection)
const OUTCOMES = Dict{String, Symbol}()

function agree(input; expect_delta::Union{Nothing, String}=nothing, label::String="", kw...)
    src() = input isa AbstractString && !isfile(input) ? IOBuffer(input) :
            input isa IO ? (seekstart(input); input) : input
    fnew = try NEW.File(src(); _newkw(kw)...) catch e; e end
    fold = try OLD.File(src(); _oldkw(kw)...) catch e; e end
    newerr = fnew isa Exception
    olderr = fold isa Exception
    outcome = if newerr && olderr
        :both_error
    elseif newerr
        :new_errors
    elseif olderr
        :old_errors
    else
        nn, vn = _table(fnew)
        no, vo = _table(fold)
        (nn == no && isequal(vn, vo)) ? :agree : :differ
    end
    OUTCOMES[label] = outcome
    if expect_delta === nothing
        ok = outcome in (:agree, :both_error)
        @test ok
        ok || @info "legacy corpus disagreement" label outcome kw newerr olderr
    else
        # a pinned delta: agreement means the pin is stale
        @test outcome != :agree
    end
    return newerr ? nothing : fnew
end

# wrap a whole generated case so a free legacy name (custom types, helper
# variables the extractor could not see) records :unportable instead of
# aborting the run — those cases go to the manual queue
macro case(label, ex)
    quote
        try
            $(esc(ex))
        catch e
            e isa UndefVarError || rethrow()
            OUTCOMES[$(esc(label))] = :unportable
        end
    end
end

function report()
    counts = Dict{Symbol, Int}()
    for (_, o) in OUTCOMES; counts[o] = get(counts, o, 0) + 1; end
    println("legacy replay outcomes: ", counts)
    for o in (:differ, :new_errors, :old_errors)
        ks = sort([k for (k, v) in OUTCOMES if v == o])
        isempty(ks) || println("  ", o, ": ", join(ks, ", "))
    end
end

# --- the corpus table (testfiles.jl) -----------------------------------------
# The table's schema literals name the retired InlineString*/String* types; they
# only participate in the "is it a string column" comparison, so alias them.
const InlineString1 = String; const InlineString3 = String; const InlineString7 = String
const InlineString15 = String; const InlineString31 = String; const InlineString63 = String
const InlineString127 = String; const InlineString255 = String
const String1 = String; const String3 = String; const String7 = String; const String15 = String
const String31 = String; const String63 = String; const PosLenString = String
# One entry: (file, kwargs, (nrows, ncols), NamedTuple{names, types}, expected)
# The old schema names string types as InlineString*/String*/PosLenString: any
# AbstractString eltype counts as "string" here. Expected values compare
# through _norm (String wrappers away).
_nms(::Type{NamedTuple{names, types}}) where {names, types} = names
_typs(::Type{NamedTuple{names, types}}) where {names, types} =
    Tuple(fieldtype(types, i) for i = 1:fieldcount(types))
_isstringy(T) = Base.nonmissingtype(T) <: AbstractString ||
                Base.nonmissingtype(T) === String
_typeclass(T) = _isstringy(T) ? (Missing <: T ? Union{Missing, String} : String) :
                T === Missing ? Missing :
                Base.nonmissingtype(T) in (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64) ?
                    (Missing <: T ? Union{Missing, Integer} : Integer) :
                Base.nonmissingtype(T) in (Float16, Float32, Float64) ?
                    (Missing <: T ? Union{Missing, AbstractFloat} : AbstractFloat) : T
# Pinned 1.0 deltas on the corpus table (the file's OLD expectations no longer
# hold by design; the harness asserts the two implementations disagree)
const TABLE_DELTAS = Dict{String, String}(
    "boolext.csv" => "Bool columns are strictly true/false unless truestrings/falsestrings are given (0.10 accepted True/TRUE/T/1)",
    "issue_198_part2.csv" => "long rows do not widen the schema: extra fields are a reported problem (0.10 added Column4)",
    # IO entries are keyed by their table position
    "table:io#99" => "long rows do not widen the schema: extra fields are a reported problem (0.10 added Column6/7)",
)

function corpuscase(file, kwargs, expected_sz, expected_sch, expected;
                    label::String=file isa IO ? "table:<io>" : file)
    src = file isa IO ? file : corpusfile(file)
    kw = Dict{Symbol, Any}(pairs(kwargs))
    # legacy-only spellings on this table
    haskey(kw, :type) && (kw[:types] = pop!(kw, :type))
    haskey(kw, :lazystrings) && delete!(kw, :lazystrings)
    haskey(kw, :stringtype) && delete!(kw, :stringtype)   # InlineString/PosLen retired
    haskey(kw, :silencewarnings) && delete!(kw, :silencewarnings)
    haskey(kw, :debug) && delete!(kw, :debug)
    delta = file isa IO ? get(TABLE_DELTAS, label, nothing) : get(TABLE_DELTAS, file, nothing)
    fnew = agree(src; label, expect_delta=delta, kw...)
    (fnew === nothing || delta !== nothing) && return
    names, vals = _table(fnew)
    # the table's own expectations, independent of the oracle
    @test names == collect(Symbol, _nms(expected_sch))
    @test (length(vals) == 0 ? 0 : length(vals[1]), length(vals)) == expected_sz
    et = [eltype(Tables.getcolumn(Tables.columns(fnew), nm)) for nm in names]
    tcnew, tcexp = map(_typeclass, et), map(_typeclass, collect(_typs(expected_sch)))
    @test tcnew == tcexp
    tcnew == tcexp || @info "corpus table type-class mismatch" label tcnew tcexp
    if expected isa NamedTuple
        for (nm, col) in pairs(expected)
            j = findfirst(==(nm), names)
            j === nothing && continue
            @test isequal(vals[j], _normvec(col))
        end
    elseif expected isa Function
        expected(Tables.columntable(fnew))
    end
    return fnew
end
