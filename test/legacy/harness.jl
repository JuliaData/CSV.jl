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
# `corpusfile(name)` resolves the committed corpus (test/legacy/testfiles);
# small files were inlined into cases.jl as literals during the audit (see
# AUDIT.md).

using Test, Tables, Dates, PooledArrays, Random, CodecZlib, Mmap, FilePathsBase
using CSV, LegacyCSV
const NEW = CSV
const OLD = LegacyCSV

# Manual 0.10 cases used these user types. Keep the definitions here so the
# generator can replay them instead of leaving them in an unverified queue.
struct CSV_Foo end
struct CSVString
    s::String
end
Base.parse(::Type{CSVString}, s::String) = CSVString(s)
Base.tryparse(::Type{CSVString}, s::String) = CSVString(s)
Base.zero(::Type{CSVString}) = CSVString("")
struct Dec64
    x::Float64
end
Base.parse(::Type{Dec64}, s::String) = Dec64(parse(Float64, s))
Base.tryparse(::Type{Dec64}, s::String) = Dec64(parse(Float64, s))
Base.zero(::Type{Dec64}) = Dec64(0.0)
# The corpus lives in two places: the small files (<= 4 KiB) are inlined as
# byte literals in corpus_inline.jl and written to a scratch dir once per
# session (real paths, so path-dependent behavior — .gz by extension, the
# mmap threshold, Cmd sources — is exercised exactly as before); the 24 large
# real-world files are committed at test/legacy/testfiles (18 MB, ~5 MB in
# git's object store).
include("corpus_inline.jl")
const inlinedir = mktempdir(; prefix="csv-corpus-inline-")
for (name, bytes) in INLINE_FILES
    write(joinpath(inlinedir, name), bytes)
end
function corpusfile(name)
    haskey(INLINE_FILES, name) && return joinpath(inlinedir, name)
    return joinpath(@__DIR__, "testfiles", name)
end

_legacynorm(x) = x isa AbstractString ? String(x) : x
_legacynormvec(v) = Any[_legacynorm(x) for x in v]
function _normtype(T::Type)
    T === Missing && return Missing
    S = Base.nonmissingtype(T)
    S <: AbstractString || S === String || return T
    return Missing <: T ? Union{Missing, String} : String
end

function _table(f)
    cols = Tables.columns(f)
    names = collect(Symbol, Tables.columnnames(cols))
    schema = Tables.schema(cols)
    types = schema === nothing ? nothing : map(_normtype, schema.types)
    return names, length(f), types,
           [_legacynormvec(Tables.getcolumn(cols, nm)) for nm in names]
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
# writer kwargs implied by a parse dialect (delim/decimal/... round-trip)
function _writekw(kw)
    d = Dict{Symbol, Any}()
    haskey(kw, :delim) && (d[:delim] = kw[:delim])
    haskey(kw, :decimal) && (d[:decimal] = kw[:decimal])
    haskey(kw, :missingstring) && kw[:missingstring] isa AbstractString &&
        (d[:missingstring] = kw[:missingstring])
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
IOBuffer for both), an IO, bytes, a corpus path, or a zero-argument function
producing the source — the thunk form is for sources a `seekstart` cannot
reset (an IO whose position matters, a vector of one-shot IOs): each
implementation gets a fresh one.
"""
# Outcome classes, so one run yields the COMPLETE disagreement ledger:
#   :agree        both parse, same names/sizes/values
#   :differ       both parse, values/names differ           -> triage
#   :new_errors   old parses, new throws                    -> triage
#   :old_errors   new parses, old throws                    -> triage
#   :both_error   both throw in the same semantic category
#   :error_mismatch both throw, but for different reasons   -> triage
const OUTCOMES = Dict{String, Symbol}()
const ERROR_PAIRS = Dict{String, Any}()

function _recordoutcome!(label::String, outcome::Symbol)
    isempty(label) && error("legacy outcome label must not be empty")
    haskey(OUTCOMES, label) && error("duplicate legacy outcome label: $label")
    OUTCOMES[label] = outcome
    return
end

# Concrete package error wrappers are implementation details. Require the same
# useful category instead: bad arguments, unsupported methods/types, bounds, IO,
# or a data-parse rejection. This is stricter than counting any two throws as
# agreement, but does not equate LegacyCSV.Error with a public exception type.
_errorclass(e) = e isa ArgumentError ? :argument :
                 e isa MethodError ? :method :
                 e isa TypeError ? :type :
                 e isa BoundsError ? :bounds :
                 e isa EOFError || e isa Base.IOError || e isa SystemError ? :io :
                 e isa LegacyCSV.Error || e isa ErrorException ? :parse :
                 :other

function agree(input; expect_delta=nothing, label::String="", kw...)
    src() = input isa Function ? input() :
            input isa AbstractString && !isfile(input) ? IOBuffer(input) :
            input isa IO ? (seekstart(input); input) : input
    fnew = try NEW.File(src(); _newkw(kw)...) catch e; e end
    fold = try OLD.File(src(); _oldkw(kw)...) catch e; e end
    newerr = fnew isa Exception
    olderr = fold isa Exception
    outcome = if newerr && olderr
        nc, oc = _errorclass(fnew), _errorclass(fold)
        ERROR_PAIRS[label] = (typeof(fnew), nc, typeof(fold), oc)
        nc == oc ? :both_error : :error_mismatch
    elseif newerr
        :new_errors
    elseif olderr
        :old_errors
    else
        nn, rn, tn, vn = _table(fnew)
        no, ro, to, vo = _table(fold)
        (nn == no && rn == ro && tn == to && isequal(vn, vo)) ? :agree : :differ
    end
    _recordoutcome!(label, outcome)
    if expect_delta === nothing
        ok = outcome in (:agree, :both_error)
        @test ok
        ok || @info "legacy corpus disagreement" label outcome kw newerr olderr
    else
        # A pinned delta asserts its exact direction. Agreement or a reversed
        # error direction makes the pin stale and fails the replay.
        @test outcome == expect_delta.outcome
        outcome == expect_delta.outcome ||
            @info "legacy pinned delta changed" label outcome expected=expect_delta.outcome reason=expect_delta.reason
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
            _recordoutcome!($(esc(label)), :unportable)
        end
    end
end

function report()
    counts = Dict{Symbol, Int}()
    for (_, o) in OUTCOMES; counts[o] = get(counts, o, 0) + 1; end
    println("legacy replay outcomes: ", counts)
    for o in (:differ, :new_errors, :old_errors, :error_mismatch, :unportable)
        ks = sort([k for (k, v) in OUTCOMES if v == o])
        isempty(ks) || println("  ", o, ": ", join(ks, ", "))
    end
    for k in sort(collect(keys(ERROR_PAIRS)))
        OUTCOMES[k] == :error_mismatch && println("  error pair ", k, ": ", ERROR_PAIRS[k])
    end
end

# --- the corpus table (testfiles.jl) -----------------------------------------
# The table's schema literals name the retired InlineString*/String* types; they
# only participate in the "is it a string column" comparison, so alias them.
const InlineString1 = String; const InlineString3 = String; const InlineString7 = String
const InlineString15 = String; const InlineString31 = String; const InlineString63 = String
const InlineString127 = String; const InlineString255 = String
const String1 = String; const String3 = String; const String7 = String; const String15 = String
const String31 = String; const String63 = String
const PosLenString = LegacyCSV.PosLenString
# One entry: (file, kwargs, (nrows, ncols), NamedTuple{names, types}, expected)
# The old schema names string types as InlineString*/String*/PosLenString: any
# AbstractString eltype counts as "string" here. Expected values compare
# through `_legacynorm` (String wrappers away).
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
const TABLE_DELTAS = Dict(
    "boolext.csv" => (outcome=:differ, reason="Bool columns are strictly true/false unless truestrings/falsestrings are given (0.10 accepted True/TRUE/T/1)"),
    "issue_198_part2.csv" => (outcome=:differ, reason="long rows do not widen the schema: extra fields are a reported problem (0.10 added Column4)"),
    # IO entries are keyed by their table position
    "table:io#99" => (outcome=:differ, reason="long rows do not widen the schema: extra fields are a reported problem (0.10 added Column6/7)"),
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
    names, _, _, vals = _table(fnew)
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
            @test isequal(vals[j], _legacynormvec(col))
        end
    elseif expected isa Function
        expected(Tables.columntable(fnew))
    end
    return fnew
end
