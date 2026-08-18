"""
    CSVKernel

A stand-alone prove-out of the proposed CSV.jl internals rewrite: a small, layered
kernel where *structure* is separated from *values*.

The pipeline (and the file's layout) is:

    L0  bytes         : a `Vector{UInt8}` covering the whole input (mmap/read/gunzip
                        live above this file; the kernel only sees bytes)
    L1  structural    : a quote-aware scan producing a `ChunkIndex` per row-aligned
        index           chunk — a compact event tape plus assembled row boundaries.
                        Three interchangeable scanners: a scalar reference state
                        machine (all dialects and the test oracle), a width-generic
                        vector default, and a portable SWAR fallback. The fast
                        scanners share 64-byte prefix-XOR quote masks.
    L1' parallelism   : chunk entry quote-states are *computed*, not guessed:
                        quote-toggle parity is associative, so a parallel per-range
                        parity count + an exclusive XOR scan gives every range its
                        true entry state (the 2-state specialization of ParPaRaw's
                        FSM-composition; strictly stronger than DuckDB's
                        speculate-then-validate). Row starts follow deterministically.
    L2  schema        : type inference seeds from a *stratified* sample of the index
                        (evenly spaced rows, not a prefix), then...
    L3  values        : ...each column of each chunk is parsed in its own
                        *monomorphic* loop over the index (the self-contained
                        `KernelValues` kernels on exact field spans). Type
                        conflicts promote through a small lattice
                        and re-parse ONLY that column — never the whole chunk.
    L4  columns       : plain `Vector{T}` + `Vector{Bool}` presence (no sentinels),
                        string columns as lazy views into the input buffer
                        (unescaped on access), sized exactly (the index gives exact
                        row counts — no rowsguess, no reallocation).
    L5  driver        : `CSVKernel.parse` — eager typed table materialization
                        with task or plain-loop execution and problems-as-data.
                        `examples.jl` builds batched
                        (CSV.Chunks-like) and row-streaming (CSV.Rows-like) modes
                        on the same pieces.

The package-level layers for sniffing, pooling, transposed reading, streaming,
and writing are documented in `docs/kernel-README.md`. Multi-file input and
incremental IO remain separate follow-up work.

Semantics note (pinned by tests): the structural layer treats *every* quote byte as
toggling quote state, like Sep/simdcsv. This matches RFC-style well-formed fields. A
bare quote in the middle of an unquoted field therefore opens a quoted region.
CSV.jl's current value-level parser only honors quotes at field start; for such
malformed inputs the two designs can split rows differently. This tradeoff makes
quote state composable across parallel byte ranges.
"""
module CSVKernel

using Dates

# The value layer: self-contained typed parsers (int/float/bool/civil), span
# utilities (quote/content discovery, sentinels), and format programs. No
# Parsers.jl — this pair of modules is the Parsers-3.0 release-candidate shape.
include("values.jl")
using .KernelValues
using .KernelValuesDates
const V = KernelValues

export Dialect, index, ParsedTable, Problem

# ---------------------------------------------------------------------------
# Dialect: the structural options. Value-level options (sentinels, dateformats,
# true/false spellings, decimal char) live in `ValueOpts`, built once in
# `makevalueopts` and applied to exact field spans by the `KernelValues`
# kernels.
# ---------------------------------------------------------------------------

struct Dialect
    delim::Union{UInt8, Vector{UInt8}}  # single byte fast path; multi-byte handled by the scalar scanner
    oq::UInt8                           # open quote
    cq::UInt8                           # close quote
    e::UInt8                            # escape char (== cq for RFC ""-doubling)
    quoted::Bool                        # false = no quote handling at all
    comment::Union{Nothing, Vector{UInt8}}  # rows beginning with these bytes are dropped
    ignoreemptyrows::Bool
    ignorerepeated::Bool                # adjacent delimiters collapse into one boundary
end

const LF = UInt8('\n')
const CR = UInt8('\r')

function Dialect(; delim::Union{Char, String}=',',
                   quotechar::Char='"',
                   openquotechar::Union{Char, Nothing}=nothing,
                   closequotechar::Union{Char, Nothing}=nothing,
                   escapechar::Union{Char, Nothing}=nothing,
                   quoted::Bool=true,
                   comment::Union{String, Nothing}=nothing,
                   ignoreemptyrows::Bool=true,
                   ignorerepeated::Bool=false)
    isempty(delim) && throw(ArgumentError("delimiter must be non-empty"))
    d = delim isa Char ? (isascii(delim) ? delim % UInt8 : Vector{UInt8}(string(delim))) :
        sizeof(delim) == 1 ? codeunit(delim, 1) : Vector{UInt8}(delim)
    for (nm, c) in (("quotechar", quotechar), ("openquotechar", openquotechar),
                    ("closequotechar", closequotechar), ("escapechar", escapechar))
        c === nothing || isascii(c) || throw(ArgumentError("$nm must be ASCII (got $(repr(c)))"))
    end
    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    e  = something(escapechar, Char(cq)) % UInt8
    for b in (d isa UInt8 ? (d,) : d)
        (b == LF || b == CR) && throw(ArgumentError("delimiter may not contain \\r or \\n"))
        quoted && b == oq && throw(ArgumentError("delimiter may not equal the quote character"))
    end
    quoted && (oq in (LF, CR) || cq in (LF, CR) || e in (LF, CR)) &&
        throw(ArgumentError("quote/escape characters may not be \\r or \\n"))
    cmt = comment === nothing ? nothing :
          isempty(comment) ? throw(ArgumentError("comment must be non-empty")) : Vector{UInt8}(comment)
    cmt !== nothing && (LF in cmt || CR in cmt) &&
        throw(ArgumentError("comment may not contain \\r or \\n"))
    return Dialect(d, oq, cq, e, quoted, cmt, ignoreemptyrows, ignorerepeated)
end

# Quote-toggle parity composes across arbitrary byte ranges only when a quote byte
# always means "toggle": true for unquoted dialects and for RFC ""-style escaping
# (an escaped quote is two toggles — parity-neutral). A distinct escape char (e.g.
# backslash) or asymmetric open/close quotes breaks parity, so those dialects run
# on the sequential scalar path.
parityclean(d::Dialect) = !d.quoted || (d.oq == d.cq && d.e == d.cq)
# A comment dialect breaks PARALLEL parity composition (a `"` inside a comment
# row is not a quote toggle, and a mid-range popcount cannot see row starts) and
# needs the scalar scanner (comment rows are skipped whole at row starts). Such
# files still get bounded chunks: the planner walks true row starts.
commentaware(d::Dialect) = d.comment !== nothing

# The fast scanners additionally need a single-byte delimiter.
swareligible(d::Dialect) = parityclean(d) && d.delim isa UInt8 && !commentaware(d)

# Value-level options: everything a cell needs beyond structure. Temporal
# parsing always runs a compiled format program — the ISO trio by default; a
# user `dateformat` replaces all three (its `hasdate`/`hastime` flags say which
# single type it detects as). Empty trues/falses ⇒ canonical `true`/`false`.
# `sentinels` is the custom-missing-strings seam (the CSV front end's
# `missingstring`): spellings whose exact (possibly quoted) content ⇒ missing.
# Sentinels cannot break sample-independence — `cellcontent` resolves them
# before ANY type machinery sees the span, so detection and parsing agree by
# construction (contrast `inferbool`).
struct ValueOpts
    oq::UInt8
    cq::UInt8
    e::UInt8
    quoted::Bool
    delim::Vector{UInt8}
    decimal::UInt8
    stripws::Bool
    sentinels::Vector{Vector{UInt8}}
    sentfirst::NTuple{4, UInt64}  # first-byte bitmap: skip matchsentinel for ~every cell
    trues::Vector{Vector{UInt8}}
    falses::Vector{Vector{UInt8}}
    datepat::V.DatePattern
    datetimepat::V.DatePattern
    timepat::V.DatePattern
    customfmt::Bool
    inferbool::Bool   # false when a user Bool spelling collides with an earlier cascade type
    groupmark::UInt8  # digit-group separator for numeric cells; 0x00 = off
end

function _bytelist(x, name::Symbol)
    x === nothing && return Vector{UInt8}[]
    x isa AbstractString &&
        throw(ArgumentError("$name must be a collection of strings, not one string"))
    out = Vector{Vector{UInt8}}()
    for s in x
        s isa AbstractString ||
            throw(ArgumentError("$name entries must be strings (got $(typeof(s)))"))
        isempty(s) && throw(ArgumentError("$name cannot contain an empty spelling"))
        push!(out, Vector{UInt8}(codeunits(s)))
    end
    return out
end

function _earlierbooltype(s::Vector{UInt8}, decimal::UInt8,
                          dp::V.DatePattern, dtp::V.DatePattern, tp::V.DatePattern,
                          customfmt::Bool, gm::UInt8)
    i, j = 1, length(s)
    if gm != 0x00
        scratch = Vector{UInt8}(undef, 64)
        n = V.degroup!(scratch, s, i, j, gm, decimal)
        if n >= 0
            V.parseint64(scratch, 1, n)[2] == V.RC_OK && return Int64
            V.parsefloat64(scratch, 1, n, decimal)[2] == V.RC_OK && return Float64
        end
    end
    V.parseint64(s, i, j)[2] == V.RC_OK && return Int64
    V.parsefloat64(s, i, j, decimal)[2] == V.RC_OK && return Float64
    if customfmt
        if V.parsecivil(s, i, j, dp)[2] == V.RC_OK
            return dp.hasdate ? (dp.hastime ? DateTime : Date) : Time
        end
    else
        V.parsecivil(s, i, j, dp)[2] == V.RC_OK && return Date
        V.parsecivil(s, i, j, dtp)[2] == V.RC_OK && return DateTime
        V.parsecivil(s, i, j, tp)[2] == V.RC_OK && return Time
    end
    return nothing
end

# A user Bool spelling that an EARLIER cascade type also accepts (e.g.
# truestrings=["1"]) would make an inferred column's type depend on which rows
# the sampler saw — "1" detects as Int64, but a Bool-seeded column would accept
# it. Instead of rejecting the (common, legitimate) spellings outright, Bool
# leaves the INFERENCE cascade: such columns are never inferred as Bool, while
# user-provided Bool columns still parse the lists — deterministic either way.
function _validatebools(trues, falses, decimal, dp, dtp, tp, customfmt, gm)
    for t in trues, f in falses
        t == f && throw(ArgumentError("Bool spelling $(repr(String(t))) is both true and false"))
    end
    for s in Iterators.flatten((trues, falses))
        _earlierbooltype(s, decimal, dp, dtp, tp, customfmt, gm) === nothing || return false
    end
    return true
end

function makevalueopts(d::Dialect; dateformat=nothing, decimal::Char='.',
                       truestrings=nothing, falsestrings=nothing,
                       stripwhitespace::Bool=false,
                       groupmark::Union{Nothing, Char}=nothing,
                       sentinels=nothing)
    isascii(decimal) || throw(ArgumentError("decimal must be ASCII (got $(repr(decimal)))"))
    gm = 0x00
    if groupmark !== nothing
        isascii(groupmark) || throw(ArgumentError("groupmark must be ASCII (got $(repr(groupmark)))"))
        gm = groupmark % UInt8
        (gm == 0x00 || gm - UInt8('0') <= 0x09 || gm == decimal % UInt8 ||
         gm in (UInt8('e'), UInt8('E'), UInt8('+'), UInt8('-'), d.oq, d.cq, d.e)) &&
            throw(ArgumentError("groupmark $(repr(groupmark)) conflicts with numeric or quote syntax"))
        # groupmark == delim is allowed: such fields are only expressible quoted,
        # which the indexer already handles (the mark is content, not structure)
    end
    if dateformat === nothing
        dp, dtp, tp, custom = V.ISO_DATE, V.ISO_DATETIME, V.ISO_TIME, false
    else
        dateformat isa AbstractString ||
            throw(ArgumentError("dateformat must be a format String (got $(typeof(dateformat)))"))
        p = V.compilepattern(dateformat)
        (p.hasdate || p.hastime) ||
            throw(ArgumentError("dateformat must contain a date or time token"))
        dp = dtp = tp = p
        custom = true
    end
    delimbytes = d.delim isa UInt8 ? [d.delim] : copy(d.delim)
    trues = _bytelist(truestrings, :truestrings)
    falses = _bytelist(falsestrings, :falsestrings)
    sentinelbytes = _bytelist(sentinels, :sentinels)
    if d.quoted
        for s in sentinelbytes, b in s
            b in (d.oq, d.cq, d.e) &&
                throw(ArgumentError("sentinels cannot contain quote or escape characters"))
        end
    end
    inferbool = _validatebools(trues, falses, decimal % UInt8, dp, dtp, tp, custom, gm)
    sf = (zero(UInt64), zero(UInt64), zero(UInt64), zero(UInt64))
    for s in sentinelbytes
        b = s[1]
        sf = Base.setindex(sf, sf[(b >> 6) + 1] | (UInt64(1) << (b & 0x3f)), (b >> 6) + 1)
    end
    return ValueOpts(d.oq, d.cq, d.e, d.quoted, delimbytes, decimal % UInt8, stripwhitespace,
                     sentinelbytes, sf, trues, falses,
                     dp, dtp, tp, custom, inferbool, gm)
end

# --- the cell layer -----------------------------------------------------------
#
# One function turns a raw field span into a *content* span + disposition:
#     CELL_VALUE    content [cpos, cpos+clen) is a present value (maybe escaped)
#     CELL_MISSING  empty / whitespace-stripped-to-empty / sentinel ⇒ missing
#     CELL_BADQUOTE malformed quoting (unterminated, or bytes after the close)
# Rules: outer space/tab around a QUOTED field is structural, never content
# (matching every CSV reader surveyed); unquoted whitespace is significant
# unless `stripwhitespace`; a quoted empty field is a present empty string,
# never missing; sentinels match the (possibly unquoted) content exactly.
const CELL_VALUE    = 0x00
const CELL_MISSING  = 0x01
const CELL_BADQUOTE = 0x02

@inline _isot(b::UInt8) = (b == UInt8(' ')) | (b == UInt8('\t'))

# a cell can only be a sentinel if its first byte starts one — one bit test
# replaces the per-cell spelling comparisons (empty sentinel list ⇒ zero map)
@inline _maybesentinel(vo::ValueOpts, b::UInt8) =
    (vo.sentfirst[(b >> 6) + 1] >> (b & 0x3f)) & UInt64(1) != 0

# Typed values and sentinel matching tolerate surrounding blanks (0.10 /
# Parsers.jl semantics). String columns still retain these bytes because this
# helper only changes the span used by those two checks.
@inline function _trimblanks(buf::Vector{UInt8}, i::Int, j::Int)
    @inbounds while i <= j && _isot(buf[i]); i += 1; end
    @inbounds while j >= i && _isot(buf[j]); j -= 1; end
    return i, j
end

@inline function _matchsentinel(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    i <= j && _maybesentinel(vo, @inbounds(buf[i])) &&
        V.matchsentinel(buf, i, j, vo.sentinels) && return true
    ti, tj = _trimblanks(buf, i, j)
    return ti <= tj && (ti != i || tj != j) &&
           _maybesentinel(vo, @inbounds(buf[ti])) &&
           V.matchsentinel(buf, ti, tj, vo.sentinels)
end

@inline function cellcontent(buf::Vector{UInt8}, pos::Int, len::Int, vo::ValueOpts)
    i, j = pos, pos + len - 1
    @inbounds begin
        if vo.stripws
            while i <= j && _isot(buf[i]); i += 1; end
            while j >= i && _isot(buf[j]); j -= 1; end
        end
        i > j && return (i, 0, false, CELL_MISSING)
        if vo.quoted
            ii, jj = i, j
            while ii <= jj && _isot(buf[ii]); ii += 1; end
            if ii <= jj && buf[ii] == vo.oq
                while jj > ii && _isot(buf[jj]); jj -= 1; end
                cpos, clen, esc, rc = V.findcontent(buf, ii, jj, vo.oq, vo.cq, vo.e)
                rc == V.RC_OK || return (cpos, clen, esc, CELL_BADQUOTE)
                if vo.stripws
                    cj = cpos + clen - 1
                    while cpos <= cj && _isot(buf[cpos]); cpos += 1; end
                    while cj >= cpos && _isot(buf[cj]); cj -= 1; end
                    clen = cj - cpos + 1
                end
                if clen > 0 && !esc
                    _matchsentinel(buf, cpos, cpos + clen - 1, vo) &&
                        return (cpos, 0, false, CELL_MISSING)
                end
                return (cpos, clen, esc, CELL_VALUE)
            end
        end
        _matchsentinel(buf, i, j, vo) &&
            return (i, 0, false, CELL_MISSING)
        return (i, j - i + 1, false, CELL_VALUE)
    end
end

# An UNQUOTED span can only contain the delimiter when a bare mid-field quote
# engaged the indexer's structural protection — the value-level reading of the
# bytes disagrees with the structural one. String cells and headers surface
# that as a problem (typed kernels reject such spans naturally); the bytes are
# still preserved exactly where the caller keeps them.
function _delimclash(buf::Vector{UInt8}, cpos::Int, clen::Int, delim::Vector{UInt8})
    n = length(delim)
    clen < n && return false
    # this scan runs on EVERY unquoted string cell (protection detection, not
    # the exception path) — single-byte delimiters take the SWAR word walk
    if n == 1
        d = @inbounds delim[1]
        k = cpos
        last = cpos + clen - 1
        GC.@preserve buf begin
            p = pointer(buf)
            @inbounds while k + 7 <= last
                w = ltoh(unsafe_load(Ptr{UInt64}(p + k - 1)))
                movemask(eqmarks(w, d)) != 0 && return true
                k += 8
            end
        end
        @inbounds while k <= last
            buf[k] == d && return true
            k += 1
        end
        return false
    end
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

# Was this raw span a quoted field? (Re-derives cellcontent's entry condition;
# only called on cold/string paths.)
@inline function _wasquoted(buf::Vector{UInt8}, pos::Int, len::Int, vo::ValueOpts)
    vo.quoted || return false
    i, j = pos, pos + len - 1
    @inbounds while i <= j && _isot(buf[i])
        i += 1
    end
    return i <= j && buf[i] == vo.oq
end

# --- typed value dispatch ------------------------------------------------------
#
# `parsevalue(T, buf, i, j, vo) -> (value, ok)` over a CONTENT span. Strictness
# principle: each kernel accepts exactly the spellings `detecttype` assigns to
# it (Bool is `true`/`false` or the user lists; temporals are pattern-exact), so
# parse-set ≡ detect-set and an inferred column's type can never depend on which
# rows the sampler saw — the property the old per-value canonical-conflict guard
# existed to enforce, now free by construction.
const _DATE0 = Date(1)
const _DATETIME0 = DateTime(1)
const _TIME0 = Time(0)

# Numeric kernels take a scratch buffer so grouped digits (groupmark) degroup
# without per-cell allocation; the hot loops pass a per-(column × chunk)
# scratch, and the 5-arg convenience forms below allocate one lazily. With
# groupmark off, the extra argument is dead and the kernels run untouched.
@inline function parsevalue(::Type{Int64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = V.degroup!(scratch, buf, i, j, vo.groupmark, 0xff)  # ints: no fraction to guard
        n == -2 && return (Int64(0), false)
        if n >= 0
            v, rc = V.parseint64(scratch, 1, n)
            return (v, rc == V.RC_OK)
        end
    end
    v, rc = V.parseint64(buf, i, j)
    return (v, rc == V.RC_OK)
end
@inline function parsevalue(::Type{Int128}, buf::Vector{UInt8}, i::Int, j::Int,
                            vo::ValueOpts, scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = V.degroup!(scratch, buf, i, j, vo.groupmark, 0xff)
        n == -2 && return (Int128(0), false)
        if n >= 0
            v, rc = V.parseint128(scratch, 1, n)
            return (v, rc == V.RC_OK)
        end
    end
    v, rc = V.parseint128(buf, i, j)
    return (v, rc == V.RC_OK)
end
@inline function parsevalue(::Type{Float64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = V.degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (0.0, false)
        if n >= 0
            v, rc = V.parsefloat64(scratch, 1, n, vo.decimal)
            return (v, rc == V.RC_OK)
        end
    end
    v, rc = V.parsefloat64(buf, i, j, vo.decimal)
    return (v, rc == V.RC_OK)
end
@inline parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                   scratch::Vector{UInt8}) where {T} = parsevalue(T, buf, i, j, vo)
# user-only column types: never inferred, so the cascade and lattice are untouched
@inline function _parsebigint_direct(buf::Vector{UInt8}, i::Int, j::Int)
    v, rc = V.parsebigint(buf, i, j)
    return (v, rc == V.RC_OK)
end
@inline function parsevalue(::Type{BigInt}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = V.degroup!(scratch, buf, i, j, vo.groupmark, 0xff)
        n == -2 && return (BigInt(0), false)
        n >= 0 && return _parsebigint_direct(scratch, 1, n)
    end
    return _parsebigint_direct(buf, i, j)
end
@inline function _parsebigfloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    v, rc = V.parsebigfloat(buf, i, j, vo.decimal)
    return (v, rc == V.RC_OK)
end
@inline function parsevalue(::Type{BigFloat}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = V.degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (BigFloat(0), false)
        n >= 0 && return _parsebigfloat_direct(scratch, 1, n, vo)
    end
    return _parsebigfloat_direct(buf, i, j, vo)
end
@inline function parsevalue(::Type{Base.UUID}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    u, rc = V.parseuuid(buf, i, j)
    return (Base.UUID(u), rc == V.RC_OK)
end
_scratchfor(vo::ValueOpts) = vo.groupmark == 0x00 ? EMPTY_BYTES : Vector{UInt8}(undef, 64)
@inline parsevalue(::Type{Int64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int64, buf, i, j, vo, _scratchfor(vo))
@inline parsevalue(::Type{Int128}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int128, buf, i, j, vo, _scratchfor(vo))
@inline parsevalue(::Type{BigInt}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsebigint_direct(buf, i, j) :
                           parsevalue(BigInt, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline function _parsefloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    v, rc = V.parsefloat64(buf, i, j, vo.decimal)
    return (v, rc == V.RC_OK)
end
@inline parsevalue(::Type{Float64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsefloat_direct(buf, i, j, vo) :
                           parsevalue(Float64, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline parsevalue(::Type{BigFloat}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsebigfloat_direct(buf, i, j, vo) :
                           parsevalue(BigFloat, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline function parsevalue(::Type{Bool}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    if isempty(vo.trues) && isempty(vo.falses)
        v, rc = V.parsebool(buf, i, j)
        return (v, rc == V.RC_OK)
    end
    V.matchsentinel(buf, i, j, vo.trues) && return (true, true)
    V.matchsentinel(buf, i, j, vo.falses) && return (false, true)
    return (false, false)
end
@inline function parsevalue(::Type{Date}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (!vo.datepat.hasdate || vo.datepat.hastime) && return (_DATE0, false)
    if !vo.customfmt && j - i + 1 == 10
        c, rc = V.parseiso10(buf, i)
        rc == V.RC_OK && return (todate(c), true)
        # not the fixed shape or not a real date — the interpreter agrees either way
    end
    c, rc = V.parsecivil(buf, i, j, vo.datepat)
    rc == V.RC_OK || return (_DATE0, false)
    return (todate(c), true)
end
@inline function parsevalue(::Type{DateTime}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (!vo.datetimepat.hasdate || !vo.datetimepat.hastime) && return (_DATETIME0, false)
    if !vo.customfmt && j - i + 1 == 19
        c, rc = V.parseiso19(buf, i)
        rc == V.RC_OK && return (todatetime(c), true)
    end
    c, rc = V.parsecivil(buf, i, j, vo.datetimepat)
    rc == V.RC_OK || return (_DATETIME0, false)
    return (todatetime(c), true)
end
@inline function parsevalue(::Type{Time}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (vo.timepat.hasdate || !vo.timepat.hastime) && return (_TIME0, false)
    if !vo.customfmt && j - i + 1 == 8
        c, rc = V.parseiso8(buf, i)
        rc == V.RC_OK && return (totime(c), true)
    end
    c, rc = V.parsecivil(buf, i, j, vo.timepat)
    rc == V.RC_OK || return (_TIME0, false)
    return (totime(c), true)
end

# User-defined scalar types are a cold path. Match CSV 0.10's contract: a
# concrete type is accepted when it defines `tryparse(T, ::String)` or
# `parse(T, ::String)`. Keep failures as ordinary invalid cells; a custom
# parser must not escape the parse loop and abort the whole file.
@noinline function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                              ::ValueOpts) where {T}
    s = String(buf[i:j])
    try
        if hasmethod(Base.tryparse, Tuple{Type{T}, String})
            v = tryparse(T, s)
            return (v, v isa T)
        end
        v = parse(T, s)
        return (v, v isa T)
    catch
        return (nothing, false)
    end
end

# ---------------------------------------------------------------------------
# L1: the structural index — tape edition.
#
# The scanners emit ONE UInt32 per structural event into a flat tape:
#     (relpos << 2) | kind
#     kind: 0 = delimiter, 1 = CR, 2 = LF, 3 = CRLF at the CR
# and nothing else — no field structs, no row bookkeeping, no hygiene. Everything
# else that is row-shaped (comment/empty-row dropping and row boundaries) happens
# in `assemblerows!`, one cheap pass over the compact tape (4 bytes/event ≈ 1/10th
# of the input bytes) instead of inside the byte loop. This is the Sep/simdcsv tape
# design: the hot loop's only per-event work is one store and a cursor bump.
# After assembly, tape kinds become: 0 = delimiter (next field starts
# `delimskip` bytes later), 1 = row end (+1 byte), 2 = row end (+2 bytes, CRLF).
# Every event closes exactly one field, so a row's field count is its event count.
# relpos is chunk-relative and capped below 2^30 (chunks are ~1 MiB; only a single
# giant row can exceed this, and that is rejected up front).
# ---------------------------------------------------------------------------

mutable struct ChunkIndex
    start::Int                  # absolute (1-based) byte offset of the chunk in buf
    stop::Int                   # absolute offset of the chunk's last byte
    tape::Vector{UInt32}        # (relpos << 2) | kind, one per field-closing event
    ext::Vector{UInt32}         # ignorerepeated only (else empty): extra delimiters
                                # each kept delimiter event swallowed (its run - 1)
    rowfirst::Vector{Int32}     # rowfirst[r]..rowfirst[r+1]-1 index `tape` for row r
    rowstartrel::Vector{UInt32} # chunk-relative byte offset of each surviving row's start
    delimskip::Int              # bytes a delimiter event consumes (multi-byte delims)
    firstdatarow::Int           # local row where data begins (2 when this chunk holds the header row)
    unclosedquote::Bool         # buffer ended while inside a quoted field (malformed input)
end

ChunkIndex(start::Int, stop::Int) =
    ChunkIndex(start, stop, UInt32[], UInt32[], Int32[1], UInt32[], 1, 1, false)

nrows(ci::ChunkIndex) = length(ci.rowfirst) - 1 - (ci.firstdatarow - 1)
totalrows(ci::ChunkIndex) = length(ci.rowfirst) - 1
nfields(ci::ChunkIndex, localrow::Int) = Int(ci.rowfirst[localrow + 1] - ci.rowfirst[localrow])

# Absolute (pos, len) of field `col` in local row `localrow`, or `nothing` when the
# row is too short (ragged input). Field col is closed by the row's col-th event;
# it starts at the row start (col == 1) or just past the previous event.
@inline function fieldspan(ci::ChunkIndex, localrow::Int, col::Int)
    @boundscheck 1 <= localrow <= totalrows(ci) || throw(BoundsError(ci, localrow))
    @boundscheck col >= 1 || throw(BoundsError(ci, (localrow, col)))
    @inbounds first = Int(ci.rowfirst[localrow])
    @inbounds nextr = Int(ci.rowfirst[localrow + 1])
    col <= nextr - first || return nothing
    fi = first + col - 1
    @inbounds stop = ci.start + Int(ci.tape[fi] >> 2) - 1
    if col == 1
        @inbounds s = ci.start + Int(ci.rowstartrel[localrow])
    else
        @inbounds e = ci.tape[fi - 1]
        k = e & 0x03
        skip = Int(ci.delimskip)
        # ignorerepeated: the previous event closed a run of 1 + ext delimiters
        k == 0x00 && !isempty(ci.ext) && (skip += skip * Int(@inbounds ci.ext[fi - 1]))
        s = ci.start + Int(e >> 2) + (k == 0x00 ? skip : Int(k))
    end
    return (s, stop - s + 1)
end

struct BufferIndex
    chunks::Vector{ChunkIndex}
    nrows::Int                  # total rows across chunks (header still included at this layer)
    unclosedquote::Bool         # input ended inside a quoted field (captured before empty-chunk filtering)
end

# --- tape plumbing -----------------------------------------------------------

const MAX_TAPE_HINT = 1 << 20   # initial-capacity cap: a giant single row spans
                                # many bytes but holds few events
const MAX_TAPE_RELPOS = Int(typemax(UInt32) >> 2)

@inline function tape_room!(tape::Vector{UInt32}, n::Int, extra::Int)
    length(tape) < n + extra && resize!(tape, max(2 * length(tape), n + extra + 256))
    return tape
end

@inline function checktaperange(ci::ChunkIndex)
    ci.stop - ci.start < MAX_TAPE_RELPOS ||
        throw(ArgumentError("a single row is 1 GiB or larger; not supported by the prove-out kernel"))
    return ci
end

# raw event kinds during scanning
@inline rawkind(b::UInt8) = UInt32((b == CR) + 2 * (b == LF))   # 0 delim, 1 CR, 2 LF, 3 CRLF (pre-paired)

# --- row assembly (the deferred hygiene pass) --------------------------------
#
# Consumes pre-paired raw events in place, drops comment and (optionally) empty
# rows, records each surviving row's start offset, and builds rowfirst. Reads
# input bytes only at row starts (comment prefix check).
function assemblerows!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int)
    d.ignorerepeated && return assemblecollapsed!(ci, buf, d, n)
    tape = ci.tape
    ci.delimskip = d.delim isa UInt8 ? 1 : length(d.delim::Vector{UInt8})
    rowfirst = ci.rowfirst
    rowstartrel = ci.rowstartrel
    resize!(rowfirst, 1); @inbounds rowfirst[1] = Int32(1)
    empty!(rowstartrel)
    cmt = d.comment
    w = 0
    roweventw = 1          # tape index where the current row's events begin
    rowstart = ci.start    # absolute byte where the current row begins
    i = 1
    @inbounds while i <= n
        e = tape[i]
        k = e & 0x03
        if k == 0x00                       # delimiter: field boundary, row continues
            w += 1
            tape[w] = e
            i += 1
        else                               # row end: scanners pre-pair CRLF (kind 3)
            pos = ci.start + Int(e >> 2)
            wide = k == 0x03
            w += 1
            tape[w] = (e & ~UInt32(0x03)) | (wide ? UInt32(2) : UInt32(1))
            i += 1
            nextrow = pos + (wide ? 2 : 1)
            # hygiene, at row granularity, over the tape (never re-scanning bytes)
            drop = false
            if d.ignoreemptyrows && w == roweventw && pos == rowstart
                drop = true                # a row that is one empty field
            elseif cmt !== nothing && rowstart + length(cmt) - 1 <= length(buf)
                # a terminator byte can never match a comment byte (validated in
                # Dialect), so this compare cannot leak past the row
                match = true
                for c in eachindex(cmt)
                    if buf[rowstart + c - 1] != cmt[c]
                        match = false
                        break
                    end
                end
                drop = match
            end
            if drop
                w = roweventw - 1
            else
                push!(rowstartrel, UInt32(rowstart - ci.start))
                push!(rowfirst, Int32(w + 1))
                roweventw = w + 1
            end
            rowstart = nextrow
        end
    end
    resize!(tape, w)
    return ci
end

# `assemblerows!` under ignorerepeated: adjacent delimiter events collapse into
# one field boundary. The kept event is the run's FIRST delimiter (so the field
# before it stops cleanly) and `ext[w]` records how many extra delimiters the
# run swallowed (so `fieldspan` starts the next field past the whole run). A
# run at the row start is pure padding — it advances the row's field start and
# emits nothing. A run touching the row end collapses into it: the kept run
# event is dropped and the row-end event takes the run's first-delimiter
# position, excluding the padding from the last field (its kind bits are
# unread past assembly — only its relpos is, as that field's stop).
# Hygiene still reads the RAW row start: an all-delimiter row is one empty
# field (a short row), NOT an empty row, and comment bytes only match at the
# true line start ("  #x" is data). Both pinned against CSV.jl.
function assemblecollapsed!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int)
    tape = ci.tape
    skip = ci.delimskip = d.delim isa UInt8 ? 1 : length(d.delim::Vector{UInt8})
    ext = ci.ext
    length(ext) < n && resize!(ext, n)
    rowfirst = ci.rowfirst
    rowstartrel = ci.rowstartrel
    resize!(rowfirst, 1); @inbounds rowfirst[1] = Int32(1)
    empty!(rowstartrel)
    cmt = d.comment
    w = 0
    roweventw = 1          # tape index where the current row's events begin
    rowstart = ci.start    # RAW row start: hygiene (comment / empty-row) anchor
    fieldstart = ci.start  # row start advanced past leading delimiter padding
    runend = 0             # absolute byte just past the last kept event's run
    i = 1
    @inbounds while i <= n
        e = tape[i]
        k = e & 0x03
        if k == 0x00                       # delimiter
            pos = ci.start + Int(e >> 2)
            if w < roweventw && pos == fieldstart
                fieldstart = pos + skip    # leading padding: no boundary yet
            elseif w >= roweventw && (tape[w] & 0x03) == 0x00 && pos == runend
                ext[w] += UInt32(1)        # extends the previous run
            else
                w += 1
                tape[w] = e
                ext[w] = UInt32(0)
            end
            runend = pos + skip
            i += 1
        else                               # row end: scanners pre-pair CRLF (kind 3)
            pos = ci.start + Int(e >> 2)
            wide = k == 0x03
            endrel = e & ~UInt32(0x03)
            if w >= roweventw && (tape[w] & 0x03) == 0x00 && pos == runend
                endrel = tape[w] & ~UInt32(0x03)   # trailing padding: run folds
                w -= 1                             # into the row end
            end
            w += 1
            tape[w] = endrel | (wide ? UInt32(2) : UInt32(1))
            ext[w] = UInt32(0)
            i += 1
            nextrow = pos + (wide ? 2 : 1)
            drop = false
            if d.ignoreemptyrows && w == roweventw && pos == rowstart
                drop = true                # a row that is zero bytes
            elseif cmt !== nothing && rowstart + length(cmt) - 1 <= length(buf)
                match = true
                for c in eachindex(cmt)
                    if buf[rowstart + c - 1] != cmt[c]
                        match = false
                        break
                    end
                end
                drop = match
            end
            if drop
                w = roweventw - 1
            else
                push!(rowstartrel, UInt32(fieldstart - ci.start))
                push!(rowfirst, Int32(w + 1))
                roweventw = w + 1
            end
            rowstart = fieldstart = nextrow
        end
    end
    resize!(tape, w)
    resize!(ext, w)
    return ci
end

# End-of-chunk: synthesize a row end when the chunk does not finish on one — a
# trailing unterminated row ("a,b"), a trailing empty field ("a,b,"), or an
# unclosed quote running to EOF.
function finishscan!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int, inquote::Bool)
    start, stop = ci.start, ci.stop
    needsend = if n == 0
        stop >= start
    else
        e = @inbounds ci.tape[n]
        # a pre-paired CRLF event sits at the CR; its row end is the LF byte
        (e & 0x03) == 0x00 ||
            ci.start + Int(e >> 2) + ((e & 0x03) == 0x03 ? 1 : 0) < stop
    end
    if needsend
        tape_room!(ci.tape, n, 1)
        n += 1
        @inbounds ci.tape[n] = (UInt32(stop + 1 - start) << 2) | UInt32(2)  # LF-kind at EOF
    end
    ci.unclosedquote = inquote
    assemblerows!(ci, buf, d, n)
    return ci
end

# --- scalar reference scanner ----------------------------------------------
#
# A direct state machine over bytes. Handles every dialect (multi-byte delimiters,
# distinct escape chars, asymmetric quotes) and is the correctness oracle the fast
# paths are property-tested against. Entry state is always "outside quotes"
# because chunk starts are true row starts by construction.

function indexchunk_scalar!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    start, stop = ci.start, ci.stop
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    delim = d.delim
    tape = ci.tape
    n = 0
    pos = start
    inquote = false
    cmt = d.comment
    atrowstart = true      # comment rows are skipped whole: their bytes are not structural
    @inbounds while pos <= stop
        if atrowstart && cmt !== nothing && !inquote &&
           pos + length(cmt) - 1 <= stop && _matchbytes(buf, pos, cmt)
            # consume through the row terminator, emitting the row-end event
            # only (assembly drops the comment row by its start bytes)
            while pos <= stop && buf[pos] != LF && buf[pos] != CR
                pos += 1
            end
            pos > stop && break
            b = buf[pos]
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            continue
        end
        atrowstart = false
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2                       # escape consumes the next byte
            elseif b == cq
                if e == cq && pos < stop && buf[pos + 1] == cq
                    pos += 2                   # "" = escaped quote, still inside
                else
                    inquote = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif quoted && b == oq
            inquote = true                     # structural rule: any quote toggles
            pos += 1
        elseif delim isa UInt8 ? b == delim :
               (b == delim[1] && pos + length(delim) - 1 <= stop && _matchbytes(buf, pos, delim))
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = UInt32(pos - start) << 2         # kind 0
            pos += delim isa UInt8 ? 1 : length(delim)
        elseif b == LF || b == CR
            # CR immediately followed by LF emits ONE pre-paired event (kind 3):
            # half the row-end tape traffic, and assembly needs no pairing pass
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            atrowstart = true
        else
            pos += 1
        end
    end
    return finishscan!(ci, buf, d, n, inquote)
end

@inline function _matchbytes(buf::Vector{UInt8}, pos::Int, bytes::Vector{UInt8})
    @inbounds for k in eachindex(bytes)
        buf[pos + k - 1] == bytes[k] || return false
    end
    return true
end

# --- fast scanners: SWAR and vector block-mask engines -----------------------
#
# Both produce, per 64-byte block, a quote bitmask and a specials
# (delim|CR|LF) bitmask; a shared branch-light event loop turns the masked
# specials into tape entries. The engines differ only in how the masks are built:
#
#   :swar — portable 8-bytes-per-word marks + movemask multiplies (no SIMD
#           assumptions at all; the fallback everywhere).
#   :vec  — width-generic LLVM vector IR (<64 x i8> compare → <64 x i1> →
#           bitcast i64). LLVM lowers this per HOST: one vpcmpeqb into a mask
#           register on AVX-512, paired 32-byte compares + vpmovmskb on AVX2,
#           and compare + bit-select reductions on NEON. One implementation,
#           no per-platform intrinsics, optimal where the hardware has direct
#           support — this is deliberately not tuned to any single machine.
#
# The in-quote region mask is a prefix-XOR over the quote mask; on x86_64 and
# Apple aarch64 it uses the carry-less multiply instruction (PCLMULQDQ / PMULL),
# elsewhere the 6-step shift-XOR ladder.

const ONES8   = 0x0101010101010101
const LOWS7   = 0x7f7f7f7f7f7f7f7f
const MOVEMASK_MAGIC = 0x0102040810204080

# Exact per-byte equality marks: 0x80 at each byte of `w` equal to `b` (the
# subtract-borrow variant has false positives) — safe to OR across classes.
@inline function eqmarks(w::UInt64, b::UInt8)::UInt64
    x = w ⊻ (ONES8 * b)
    return ~(((x & LOWS7) + LOWS7) | x | LOWS7)
end

@inline movemask(marks::UInt64)::UInt64 = ((marks >> 7) * MOVEMASK_MAGIC) >> 56

@inline function prefix_xor64_shift(m::UInt64)::UInt64
    m ⊻= m << 1
    m ⊻= m << 2
    m ⊻= m << 4
    m ⊻= m << 8
    m ⊻= m << 16
    m ⊻= m << 32
    return m
end

@static if Sys.ARCH === :x86_64
    @inline function prefix_xor64(m::UInt64)::UInt64
        # clmul(m, ~0) = prefix XOR; PCLMULQDQ ships on every x86_64 CPU since ~2010
        v = Base.llvmcall(("""
            declare <2 x i64> @llvm.x86.pclmulqdq(<2 x i64>, <2 x i64>, i8)
            define i64 @entry(i64 %m) #0 {
                %a0 = insertelement <2 x i64> zeroinitializer, i64 %m, i32 0
                %b0 = insertelement <2 x i64> zeroinitializer, i64 -1, i32 0
                %r = call <2 x i64> @llvm.x86.pclmulqdq(<2 x i64> %a0, <2 x i64> %b0, i8 0)
                %lo = extractelement <2 x i64> %r, i32 0
                ret i64 %lo
            }
            attributes #0 = { alwaysinline }""", "entry"), UInt64, Tuple{UInt64}, m)
        return v
    end
elseif Sys.ARCH === :aarch64 && Sys.isapple()
    @inline function prefix_xor64(m::UInt64)::UInt64
        # PMULL (crypto extension — present on all Apple silicon)
        v = Base.llvmcall(("""
            declare <16 x i8> @llvm.aarch64.neon.pmull64(i64, i64)
            define i64 @entry(i64 %m) #0 {
                %r = call <16 x i8> @llvm.aarch64.neon.pmull64(i64 %m, i64 -1)
                %v = bitcast <16 x i8> %r to <2 x i64>
                %lo = extractelement <2 x i64> %v, i32 0
                ret i64 %lo
            }
            attributes #0 = { alwaysinline }""", "entry"), UInt64, Tuple{UInt64}, m)
        return v
    end
else
    @inline prefix_xor64(m::UInt64) = prefix_xor64_shift(m)
end

# vector mask kernels (width-generic IR; unaligned loads; element 0 = bit 0 on
# the little-endian targets this kernel supports). Julia 1.10's LLVM parser uses
# typed pointers; Julia 1.11+ uses opaque pointers.
@static if VERSION < v"1.11"
    const LLVM_BYTE_PTR = "i8*"
    const LLVM_LOAD64 = """
            %vp = bitcast i8* %p to <64 x i8>*
            %x = load <64 x i8>, <64 x i8>* %vp, align 1"""
else
    const LLVM_BYTE_PTR = "ptr"
    const LLVM_LOAD64 = "%x = load <64 x i8>, ptr %p, align 1"
end

const SPECIALS_MASK_VEC_IR = """
        define i64 @entry($LLVM_BYTE_PTR %p, i8 %d, i8 %cr, i8 %lf) #0 {
$LLVM_LOAD64
            %d0 = insertelement <64 x i8> undef, i8 %d, i32 0
            %dv = shufflevector <64 x i8> %d0, <64 x i8> undef, <64 x i32> zeroinitializer
            %c0 = insertelement <64 x i8> undef, i8 %cr, i32 0
            %cv = shufflevector <64 x i8> %c0, <64 x i8> undef, <64 x i32> zeroinitializer
            %l0 = insertelement <64 x i8> undef, i8 %lf, i32 0
            %lv = shufflevector <64 x i8> %l0, <64 x i8> undef, <64 x i32> zeroinitializer
            %e1 = icmp eq <64 x i8> %x, %dv
            %e2 = icmp eq <64 x i8> %x, %cv
            %e3 = icmp eq <64 x i8> %x, %lv
            %o1 = or <64 x i1> %e1, %e2
            %o2 = or <64 x i1> %o1, %e3
            %m = bitcast <64 x i1> %o2 to i64
            ret i64 %m
        }
        attributes #0 = { alwaysinline }"""

const BYTE_MASK_VEC_IR = """
        define i64 @entry($LLVM_BYTE_PTR %p, i8 %b) #0 {
$LLVM_LOAD64
            %b0 = insertelement <64 x i8> undef, i8 %b, i32 0
            %bv = shufflevector <64 x i8> %b0, <64 x i8> undef, <64 x i32> zeroinitializer
            %c = icmp eq <64 x i8> %x, %bv
            %m = bitcast <64 x i1> %c to i64
            ret i64 %m
        }
        attributes #0 = { alwaysinline }"""

@inline function specials_mask_vec(p::Ptr{UInt8}, d::UInt8)::UInt64
    Base.llvmcall((SPECIALS_MASK_VEC_IR, "entry"),
        UInt64, Tuple{Ptr{UInt8}, UInt8, UInt8, UInt8}, p, d, CR, LF)
end

@inline function byte_mask_vec(p::Ptr{UInt8}, b::UInt8)::UInt64
    Base.llvmcall((BYTE_MASK_VEC_IR, "entry"), UInt64, Tuple{Ptr{UInt8}, UInt8}, p, b)
end

@inline function blockmasks(::Val{:vec}, p::Ptr{UInt8}, quoted::Bool, oq::UInt8, delim::UInt8)
    q64 = quoted ? byte_mask_vec(p, oq) : zero(UInt64)
    return q64, specials_mask_vec(p, delim)
end

@inline function blockmasks(::Val{:swar}, p::Ptr{UInt8}, quoted::Bool, oq::UInt8, delim::UInt8)
    q64 = zero(UInt64)
    s64 = zero(UInt64)
    if quoted
        for k in 0:7   # fixed trip count; unrolled by the compiler
            # ltoh: the movemask constants number bytes from the least-
            # significant end, matching little-endian loads.
            w = ltoh(unsafe_load(Ptr{UInt64}(p + 8k)))
            q64 |= movemask(eqmarks(w, oq)) << (8k)
            sm = eqmarks(w, delim) | eqmarks(w, LF) | eqmarks(w, CR)
            s64 |= movemask(sm) << (8k)
        end
    else
        for k in 0:7
            w = ltoh(unsafe_load(Ptr{UInt64}(p + 8k)))
            sm = eqmarks(w, delim) | eqmarks(w, LF) | eqmarks(w, CR)
            s64 |= movemask(sm) << (8k)
        end
    end
    return q64, s64
end

function indexchunk_fast!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, ::Val{S}) where {S}
    @assert swareligible(d)
    start, stop = ci.start, ci.stop
    delim = d.delim::UInt8
    oq = d.oq
    quoted = d.quoted
    tape = ci.tape
    length(tape) < 256 && resize!(tape, min(max((stop - start + 1) >> 3, 256), MAX_TAPE_HINT))
    n = 0
    inq = false        # quote parity carried between blocks
    pairskip = false   # CR at a block's last bit already consumed the next LF
    pos = start
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= stop
            q64, s64 = blockmasks(Val(S), p + pos - 1, quoted, oq, delim)
            inmask = prefix_xor64(q64)
            inq && (inmask = ~inmask)
            specials = s64 & ~inmask
            pairskip && (specials &= ~one(UInt64))   # LF of a CRLF split across blocks
            pairskip = false
            if specials != zero(UInt64)
                tape = tape_room!(tape, n, 64)
                base = UInt32(pos - start)
                while specials != zero(UInt64)
                    tz = trailing_zeros(specials)
                    b = buf[pos + tz]
                    n += 1
                    if b == CR && pos + tz < stop && buf[pos + tz + 1] == LF
                        tape[n] = ((base + UInt32(tz)) << 2) | UInt32(3)
                        tz < 63 ? (specials &= ~(UInt64(1) << (tz + 1))) : (pairskip = true)
                    else
                        tape[n] = ((base + UInt32(tz)) << 2) | rawkind(b)
                    end
                    specials &= specials - one(UInt64)
                end
            end
            inq ⊻= isodd(count_ones(q64))
            pos += 64
        end
    end
    ci.tape = tape
    # Scalar tail for the last <64 bytes, continuing the carried quote state.
    @inbounds while pos <= stop
        b = buf[pos]
        if inq
            if b == d.cq
                if pos < stop && buf[pos + 1] == d.cq
                    pos += 2
                else
                    inq = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif quoted && b == oq
            inq = true
            pos += 1
        elseif b == delim || b == LF || b == CR
            if pairskip
                pairskip = false
                pos += 1                             # the LF a block-final CR consumed
            else
                crlf = b == CR && pos < stop && buf[pos + 1] == LF
                tape_room!(tape, n, 1)
                n += 1
                tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
                pos += crlf ? 2 : 1
            end
        else
            pos += 1
        end
    end
    return finishscan!(ci, buf, d, n, inq)
end

# --- parallel indexing -------------------------------------------------------
#
# Three deterministic steps (no speculation, no retry):
#   1. Split [datastart, len] into fixed-size ranges; compute each range's
#      quote-toggle parity in parallel (a pure popcount — memory-bandwidth bound).
#   2. Exclusive XOR-scan of parities ⇒ every range knows its TRUE entry quote
#      state. This is FSM composition specialized to the 2-state quote automaton.
#   3. From each range start, scan (with known state) to the first structural row
#      terminator ⇒ the range's first true row start. Consecutive distinct row
#      starts become row-aligned chunks, each indexed independently in parallel.
# A row larger than a range simply collapses that range's chunk to zero bytes
# (its row start equals the next range's), which is dropped.

function quoteparity(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect)::Bool
    d.quoted || return false
    q = d.oq
    n = 0
    i = from
    # word-at-a-time: the byte loop's autovectorization ran at ~7 GB/s; the
    # SWAR eq-mask popcount streams at memory speed
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while i + 7 <= to
            w = ltoh(unsafe_load(Ptr{UInt64}(p + i - 1)))
            n += count_ones(movemask(eqmarks(w, q)))
            i += 8
        end
    end
    @inbounds while i <= to
        n += buf[i] == q
        i += 1
    end
    return isodd(n)
end

# First row start after a terminator at or after `from`, given the entry quote
# state. `atrowstart=true` permits comment skipping because it proves `from` is
# not a mid-row range probe. CRLF returns the byte after LF. Returns `to + 1`
# when no terminator exists; callers treat a result > `to` as no row start.
function nextrowstart(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect, inquote::Bool,
                      atrowstart::Bool=false)::Int
    pos = from
    cq, oq, e = d.cq, d.oq, d.e
    # a comment row's bytes are not structural: when `from` is a row start
    # (inquote=false) that begins with the comment prefix, run straight to the
    # terminator without honoring quotes
    cmt = d.comment
    if atrowstart && !inquote && cmt !== nothing &&
       from + length(cmt) - 1 <= to && _matchbytes(buf, from, cmt)
        @inbounds while pos <= to
            b = buf[pos]
            b == LF && return pos + 1
            b == CR && return pos + 1 + (pos < to && buf[pos + 1] == LF)
            pos += 1
        end
        return to + 1
    end
    @inbounds while pos <= to
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2
            elseif b == cq
                if e == cq && pos < to && buf[pos + 1] == cq
                    pos += 2
                else
                    inquote = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif d.quoted && b == oq
            inquote = true
            pos += 1
        elseif b == LF
            return pos + 1
        elseif b == CR
            return pos + 1 + (pos < to && buf[pos + 1] == LF)
        else
            pos += 1
        end
    end
    return to + 1
end

# Plan row-aligned chunks WITHOUT indexing them: per-range quote parity (parallel
# popcount) → exclusive XOR scan → true entry states → first row start per range.
# Shared by `index` (which then indexes every chunk) and the fused driver in
# `parse` (which indexes each chunk inside the task that immediately parses it).
function chunkplan(buf::Vector{UInt8}, d::Dialect, datastart::Int, chunkbytes::Int,
                   parallel::Bool)
    len = length(buf)
    # Range splitting is gated on the DIALECT (parity composition must be sound),
    # not on `parallel`: sequential runs also want bounded chunks because the
    # column-at-a-time parse re-walks each chunk once per column and needs it
    # cache-resident. `parallel` only decides tasks vs a plain loop.
    if commentaware(d) && parityclean(d) && len - datastart + 1 > chunkbytes
        # comment dialect: parity popcounts cannot see comment rows, so chunk
        # bounds come from a sequential row-start walk (nextrowstart is
        # comment-aware). One pass at chunk granularity, no per-byte work
        # beyond the quote/terminator scan the index pays anyway.
        chunks = ChunkIndex[]
        b0 = datastart
        while b0 <= len
            target = min(b0 + chunkbytes - 1, len)
            # walk row starts from the known start b0 (a mid-row probe could not
            # know its quote state); quoted newlines and comment rows are honored
            b1 = target >= len ? len + 1 : _rowstartatorafter(buf, b0, target, len, d)
            push!(chunks, ChunkIndex(b0, b1 - 1))
            b0 = b1
        end
        foreach(checktaperange, chunks)
        return chunks
    end
    nranges = parityclean(d) ? max(1, cld(len - datastart + 1, chunkbytes)) : 1
    starts = [datastart + (i - 1) * chunkbytes for i in 1:nranges]
    entry = falses(nranges)
    if nranges > 1
        par = Vector{Bool}(undef, nranges)
        if parallel
            @sync for i in 1:nranges
                errormonitor(Threads.@spawn begin
                    to = i == nranges ? len : starts[i + 1] - 1
                    par[i] = quoteparity(buf, starts[i], to, d)
                end)
            end
        else
            for i in 1:nranges
                to = i == nranges ? len : starts[i + 1] - 1
                par[i] = quoteparity(buf, starts[i], to, d)
            end
        end
        acc = false
        for i in 2:nranges
            acc ⊻= par[i - 1]
            entry[i] = acc
        end
    end
    bounds = Vector{Int}(undef, nranges)
    bounds[1] = datastart
    if nranges > 1
        if parallel
            @sync for i in 2:nranges
                errormonitor(Threads.@spawn begin
                    bounds[i] = nextrowstart(buf, starts[i], len, d, entry[i])
                end)
            end
        else
            for i in 2:nranges
                bounds[i] = nextrowstart(buf, starts[i], len, d, entry[i])
            end
        end
    end
    push!(bounds, len + 1)
    # Row-aligned chunks (drop empties: a row spanning ≥1 whole range).
    chunks = ChunkIndex[]
    for i in 1:nranges
        b0, b1 = bounds[i], bounds[i + 1]
        b0 < b1 && push!(chunks, ChunkIndex(b0, b1 - 1))
    end
    # Tape offsets are chunk-relative and packed as (relpos << 2) in a UInt32. A
    # chunk only exceeds `chunkbytes` when a single row straddles whole ranges, so
    # this bound is about one giant row.
    foreach(checktaperange, chunks)
    return chunks
end

# first row start strictly after `target`, walking rows from the known row
# start `from` (quote- and comment-aware); len+1 at EOF
function _rowstartatorafter(buf::Vector{UInt8}, from::Int, target::Int, len::Int, d::Dialect)
    pos = from
    while pos <= target
        pos = nextrowstart(buf, pos, len, d, false, true)
    end
    return min(pos, len + 1)
end

function indexone!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, scanner::Symbol)
    scanner === :scalar ? indexchunk_scalar!(ci, buf, d) :
    scanner === :swar   ? indexchunk_fast!(ci, buf, d, Val(:swar)) :
                          indexchunk_fast!(ci, buf, d, Val(:vec))
end

# Resolve the scanner: exotic dialects need the scalar reference machine; the
# vector engine is the default fast path everywhere (LLVM lowers its generic IR
# per host), with :swar as the no-SIMD-assumptions fallback and for testing.
function resolvescanner(d::Dialect, fastindex::Bool, scanner::Symbol)
    scanner in (:auto, :vec, :swar, :scalar) ||
        throw(ArgumentError("scanner must be :auto, :vec, :swar, or :scalar (got $(repr(scanner)))"))
    return !(fastindex && swareligible(d)) ? :scalar :
           scanner === :auto ? :vec : scanner
end

"""
    index(buf, d::Dialect; datastart=1, chunkbytes=2^23, parallel=true,
          fastindex=true, scanner=:auto)

Build the structural index for `buf[datastart:end]`: row-aligned chunks, each with
per-field spans. Deterministic for any `chunkbytes`/thread count (pinned by tests).
"""
function index(buf::Vector{UInt8}, d::Dialect;
               datastart::Int=1,
               chunkbytes::Int=1 << 23,
               parallel::Bool=Threads.nthreads() > 1,
               fastindex::Bool=true,
               scanner::Symbol=:auto)
    len = length(buf)
    # No lower bound beyond 1: tests deliberately use tiny chunkbytes to force row
    # boundaries everywhere. The standalone index default is 8 MiB; `parse`
    # passes its size-aware 64 KiB–1 MiB default.
    chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    datastart >= 1 || throw(ArgumentError("datastart must be ≥ 1 (got $datastart)"))
    sc = resolvescanner(d, fastindex, scanner)
    datastart > len && return BufferIndex(ChunkIndex[], 0, false)

    chunks = chunkplan(buf, d, datastart, chunkbytes, parallel)
    if length(chunks) == 1 || !parallel
        for ci in chunks
            indexone!(ci, buf, d, sc)
        end
    else
        @sync for ci in chunks
            errormonitor(Threads.@spawn indexone!(ci, buf, d, sc))
        end
    end

    # Non-final chunks end at a row terminator, so they must exit outside quotes;
    # parity composition guarantees it for parityclean dialects. Defensive check —
    # a failure here is a kernel bug, not bad input.
    for (k, ci) in enumerate(chunks)
        k < length(chunks) && ci.unclosedquote &&
            error("internal error: chunk $(k) ended inside a quoted field despite parity pre-scan")
    end
    # Capture malformed-EOF before filtering: an unclosed quote inside a dropped
    # (e.g. all-comment) chunk must still surface as a Problem.
    unclosed = !isempty(chunks) && last(chunks).unclosedquote
    filter!(ci -> totalrows(ci) > 0, chunks)
    return BufferIndex(chunks, sum(totalrows, chunks; init=0), unclosed)
end

index(buf::Vector{UInt8}; kw...) = index(buf, Dialect(); kw...)

# ---------------------------------------------------------------------------
# L2/L3: typed parsing over the index.
# ---------------------------------------------------------------------------

# The kernel's standard column types. The promotion lattice is intentionally small:
#   Missing → Int64 → Int128 → Float64 → String
#   Missing → (Date | DateTime | Time | Bool) → String
# Everything else (mixed temporals, bool/number mixes, …) promotes to String.
# InlineString widths, Int downcasting, and user "typemap"s are API-layer concerns
# built on the same machinery (see README).
# `typemap` rewrites DETECTED types (never user-pinned ones): applied to the
# sampled seed and to every promotion join, so a mapped type is a fixed point
# of inference. Missing never maps (a mapped all-missing column would lose its
# free upgrade).
function _normalizetypemap(typemap)
    typemap === nothing && return nothing
    tm = Dict{Type, Type}()
    for (a, b) in typemap
        a isa Type && b isa Type ||
            throw(ArgumentError("typemap entries must be Type => Type (got $a => $b)"))
        tm[Base.nonmissingtype(a)] = Base.nonmissingtype(b)
    end
    return isempty(tm) ? nothing : tm
end
@inline _maptype(tm, T) = tm === nothing || T === Missing ? T : get(tm, T, T)
@inline function _promotemapped(tm, current::Type, detected::Type)
    joined = promote_kernel(current, detected)
    # A typemap result is already the column's chosen parse type. Do not feed
    # that result through the map again when a concurrent/stale join does not
    # widen it (for example Int64=>Float64, Float64=>String).
    joined === current && return current
    mapped = _maptype(tm, joined)
    # A downward or cyclic mapping can collapse a real promotion back to the
    # type that just rejected the cell (Float64=>Int64 is the minimal case).
    # String is the finite lattice join that accepts both spellings.
    return mapped === current ? String : mapped
end
@inline _copts(colopts, opts, j::Int) = colopts === nothing ? opts : @inbounds colopts[j]

promote_kernel(a::Type, b::Type) =
    a === b          ? a :
    a === Missing    ? b :
    b === Missing    ? a :
    a === Int64 && b === Int128 ? Int128 :
    a === Int128 && b === Int64 ? Int128 :
    a in (Int64, Int128) && b === Float64 ? Float64 :
    a === Float64 && b in (Int64, Int128) ? Float64 :
    String

# Detect the type of one raw field span (mirrors CSV.jl's cascade, minus the
# Int-width games). Detection and parsing run the SAME kernels on the same
# content span, so detect-set ≡ parse-set exactly and a conflict always
# advances the finite promotion lattice. The strict kernels reject foreign
# shapes within a byte or two, so a full cascade probe is cheap.
function detecttype(buf::Vector{UInt8}, pos::Int, len::Int, opts::ValueOpts)
    len == 0 && return Missing
    cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
    st == CELL_MISSING && return Missing
    st == CELL_BADQUOTE && return String    # malformed quoting reports at parse time
    (clen == 0 || esc) && return String     # quoted-empty / escape content is stringy
    cpos, cj = _trimblanks(buf, cpos, cpos + clen - 1)
    cpos > cj && return String              # blanks only: a present string
    if opts.groupmark != 0x00
        # sampling is cold: a fresh scratch per call keeps the signature small
        scratch = Vector{UInt8}(undef, 64)
        parsevalue(Int64, buf, cpos, cj, opts, scratch)[2] && return Int64
        parsevalue(Int128, buf, cpos, cj, opts, scratch)[2] && return Int128
        parsevalue(Float64, buf, cpos, cj, opts, scratch)[2] && return Float64
    else
        rc = V.parseint64(buf, cpos, cj)[2]
        rc == V.RC_OK && return Int64
        rc == V.RC_OVERFLOW && V.parseint128(buf, cpos, cj)[2] == V.RC_OK && return Int128
        V.parsefloat64(buf, cpos, cj, opts.decimal)[2] == V.RC_OK && return Float64
    end
    if opts.customfmt
        # one probe: the user format's own components say which type it detects
        if V.parsecivil(buf, cpos, cj, opts.datepat)[2] == V.RC_OK
            p = opts.datepat
            return p.hasdate ? (p.hastime ? DateTime : Date) : Time
        end
    else
        V.parsecivil(buf, cpos, cj, opts.datepat)[2] == V.RC_OK && return Date
        V.parsecivil(buf, cpos, cj, opts.datetimepat)[2] == V.RC_OK && return DateTime
        V.parsecivil(buf, cpos, cj, opts.timepat)[2] == V.RC_OK && return Time
    end
    opts.inferbool && parsevalue(Bool, buf, cpos, cj, opts)[2] && return Bool
    return String
end

# --- column storage ----------------------------------------------------------

# Fixed-size isbits values + presence bytes. `Vector{Bool}` (not BitVector): chunk
# tasks write disjoint row ranges concurrently and BitVector packs 64 rows per word
# (a data race); the production version uses a word-aligned bitmap per chunk slice.
struct TypedColumn{T}
    values::Vector{T}
    present::Vector{Bool}
end
TypedColumn{T}(n::Int) where {T} = TypedColumn{T}(Vector{T}(undef, n), fill(false, n))

# Direct-to-final storage for typed columns the SAMPLE showed missings in: the
# parse writes `Vector{Union{T,Missing}}` cells straight into the final — for a
# bits `T` that is a data store plus a tag-byte store, the same two stores as
# values+present — so finalize hands the Base vector back with zero copies.
# Missing-free columns keep TypedColumn and return the raw `Vector{T}`; a
# column whose (sparse) missings the sample missed converts once at finalize.
# Post-parse conversion measures 120-150% of a whole 20 MiB parse (bitsunion
# stores have no memcpy path), which is why the write-direct mode exists.
struct UnionColumn{T}
    uvalues::Vector{Union{T, Missing}}
end
UnionColumn{T}(n::Int) where {T} = UnionColumn{T}(Vector{Union{T, Missing}}(undef, n))

@inline function _storevalue!(col::TypedColumn{T}, i::Int, v::T) where {T}
    @inbounds col.values[i] = v
    @inbounds col.present[i] = true
    return
end
@inline function _storevalue!(col::UnionColumn{T}, i::Int, v::T) where {T}
    @inbounds col.uvalues[i] = v
    return
end

# --- inline-else-view strings (the "German strings" / Arrow StringView layout) --
#
# Every string cell is one 16-byte payload:
#   a: bits 0..31 = content length as Int32 (-1 ⇒ missing);
#      bits 32..63 = content bytes 1..4 (the full bytes when inline, the PREFIX
#      when a view — prefixes make equality's fast path branch-free)
#   b: len ≤ 12 ⇒ content bytes 5..12 (zero-padded);
#      len > 12 ⇒ Int64 byte offset of the content: positive into the input
#      buffer (zero copy), negative into the column's `extra` buffer (escaped
#      values are unescaped once at parse time and stored there)
# Byte packing is by explicit shifts, so the layout is endianness-independent.
# This maps 1:1 onto Arrow's StringView (12-byte inline, 4-byte prefix; Arrow's
# int32 buffer offsets correspond to the production plan's <2 GiB chunk-owned
# buffers) — the strategic bet: string columns that hand off to Arrow zero-copy.
struct CompactStringPayload
    a::UInt64
    b::UInt64
end
const PAYLOAD_MISSING = CompactStringPayload(UInt64(0xffffffff), zero(UInt64))
const COMPACTSTRING_INLINE = 12
const EMPTY_BYTES = UInt8[]

@inline cslen(p::CompactStringPayload) = reinterpret(Int32, p.a % UInt32)
@inline csoff(p::CompactStringPayload) = reinterpret(Int64, p.b)

# Two overlapping little-endian loads gather up to 12 content bytes branch-free;
# the byte-loop fallback only runs within 11 bytes of the buffer's end (loads
# must not read past it). This sits on the hot path of every short string cell.
@inline function inline_payload(src::Vector{UInt8}, pos::Int, len::Int)
    if pos + 11 <= length(src)
        GC.@preserve src begin
            p = pointer(src, pos)
            lo = ltoh(unsafe_load(Ptr{UInt64}(p)))           # content bytes 1..8
            hi = ltoh(unsafe_load(Ptr{UInt64}(p + 4)))       # content bytes 5..12
        end
        m4 = len >= 4 ? 0x00000000ffffffff : (UInt64(1) << (8 * len)) - 1
        nb = max(len - 4, 0)
        m8 = nb >= 8 ? typemax(UInt64) : (UInt64(1) << (8 * nb)) - 1
        return CompactStringPayload(UInt64(len % UInt32) | ((lo & m4) << 32), hi & m8)
    end
    a = UInt64(len % UInt32)
    b = zero(UInt64)
    @inbounds for i in 1:min(len, 4)
        a |= UInt64(src[pos + i - 1]) << (32 + 8 * (i - 1))
    end
    @inbounds for i in 5:len
        b |= UInt64(src[pos + i - 1]) << (8 * (i - 5))
    end
    return CompactStringPayload(a, b)
end

# Next `""` pair at or after i (RFC doubling; the span passed findcontent, so
# quotes only occur doubled) — word-scan for the quote byte, verify adjacency
@inline function _nextpair(buf::Vector{UInt8}, i::Int, last::Int, cq::UInt8)
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while i + 7 <= last
            mk = _eqmask8_c(ltoh(unsafe_load(Ptr{UInt64}(p + i - 1))), cq)
            if mk == 0
                i += 8
                continue
            end
            # Borrow propagation can mark bytes after the first match. The
            # first mark is exact; restart after a lone quote before searching
            # for another candidate.
            k = i + (trailing_zeros(mk) >> 3)
            k < last && buf[k + 1] == cq && return k
            i = k + 1
        end
    end
    @inbounds while i < last
        buf[i] == cq && buf[i + 1] == cq && return i
        i += 1
    end
    return 0
end

@inline function _eqmask8_c(w::UInt64, b::UInt8)
    x = w ⊻ (0x0101010101010101 * b)
    return (x - 0x0101010101010101) & ~x & 0x8080808080808080
end

# Unescape ≤12 result bytes straight into a payload — no allocation; returns
# `nothing` when the unescaped content exceeds the inline capacity.
@inline function _unescape_inline(buf::Vector{UInt8}, pos::Int, len::Int, e::UInt8, cq::UInt8)
    a = zero(UInt64)
    b = zero(UInt64)
    n = 0
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        c = buf[i]
        if c == e && i < last && (e != cq || buf[i + 1] == cq)
            c = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        n += 1
        n > COMPACTSTRING_INLINE && return nothing
        if n <= 4
            a |= UInt64(c) << (32 + 8 * (n - 1))
        else
            b |= UInt64(c) << (8 * (n - 5))
        end
    end
    return CompactStringPayload(a | UInt64(n % UInt32), b)
end

@inline function _unescape_append!(dst::Vector{UInt8}, buf::Vector{UInt8}, pos::Int, len::Int,
                                   e::UInt8, cq::UInt8)
    n0 = length(dst)
    if e == cq
        # run-copy: reserve the upper bound once, bulk-copy the bytes between
        # "" pairs, trim to the actual size — no per-byte push!/branch
        resize!(dst, n0 + len)
        w = n0
        i = pos
        last = pos + len - 1
        @inbounds while i <= last
            k = _nextpair(buf, i, last, cq)
            run = (k == 0 ? last + 1 : k + 1) - i    # keep one quote of the pair
            copyto!(dst, w + 1, buf, i, run)
            w += run
            k == 0 && break
            i = k + 2
        end
        resize!(dst, w)
        return w - n0
    end
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        c = buf[i]
        if c == e && i < last && (e != cq || buf[i + 1] == cq)
            c = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        push!(dst, c)
    end
    return length(dst) - n0
end

# `off` is the (positive) input-buffer position, or negative for `extra`.
# len > 12 guarantees the 4-byte prefix load is in-bounds.
@inline function view_payload(src::Vector{UInt8}, srcpos::Int, len::Int, off::Int64)
    GC.@preserve src begin
        pre = ltoh(unsafe_load(Ptr{UInt32}(pointer(src, srcpos))))
    end
    a = UInt64(len % UInt32) | (UInt64(pre) << 32)
    return CompactStringPayload(a, reinterpret(UInt64, off))
end

"""
    CompactString <: AbstractString

A kernel string value: 16-byte payload plus the byte vector long values view
into (a shared empty vector for inline values). Byte access, direct comparisons,
and iteration do not allocate; they use the inline bytes or retained buffer.
Hashing and ordering operate on the payload bytes without allocation and agree
with `String`. `String(s)` (or `materialize` on the column) copies out.
Lifetime: a view pins its buffer, exactly like today's `PosLenString` — the
production compaction story is `materialize`.
"""
struct CompactString <: AbstractString
    p::CompactStringPayload
    data::Vector{UInt8}    # dereferenced only when len > COMPACTSTRING_INLINE
end

Base.ncodeunits(s::CompactString) = Int(cslen(s.p))
Base.codeunit(::CompactString) = UInt8
Base.@propagate_inbounds function Base.codeunit(s::CompactString, i::Int)
    @boundscheck 1 <= i <= ncodeunits(s) || throw(BoundsError(s, i))
    len = cslen(s.p)
    if len <= COMPACTSTRING_INLINE
        return i <= 4 ? (s.p.a >> (32 + 8 * (i - 1))) % UInt8 :
                        (s.p.b >> (8 * (i - 5))) % UInt8
    else
        off = csoff(s.p)
        o = off < 0 ? -off : off
        return @inbounds s.data[o + i - 1]
    end
end

function Base.isvalid(s::CompactString, i::Int)
    1 <= i <= ncodeunits(s) || return false
    @inbounds b = codeunit(s, i)
    b & 0xc0 == 0x80 || return true
    i > 1 || return true
    @inbounds b = codeunit(s, i - 1)
    0xc0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 2 || return true
    @inbounds b = codeunit(s, i - 2)
    0xe0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 3 || return true
    @inbounds b = codeunit(s, i - 3)
    return !(0xf0 <= b <= 0xf7)
end

# UTF-8 iteration mirroring `String`'s tolerant behavior: Julia `Char`s ARE the
# UTF-8 bytes left-aligned in 32 bits, and a malformed sequence yields the bytes
# consumed so far as an (invalid) Char. Pinned against the String oracle by a
# randomized test.
function Base.iterate(s::CompactString, i::Int=1)
    i > ncodeunits(s) && return nothing
    @inbounds b1 = codeunit(s, i)
    b1 < 0x80 && return (reinterpret(Char, UInt32(b1) << 24), i + 1)
    l = b1 < 0xc0 ? 1 : b1 < 0xe0 ? 2 : b1 < 0xf0 ? 3 : b1 < 0xf8 ? 4 : 1
    n = ncodeunits(s)
    c = UInt32(b1) << 24
    j = 1
    @inbounds while j < l && i + j <= n
        nb = codeunit(s, i + j)
        (nb & 0xc0) == 0x80 || break
        c |= UInt32(nb) << (24 - 8 * j)
        j += 1
    end
    return (reinterpret(Char, c), i + j)
end

# Base's generic AbstractString length is isvalid-count-based, which undercounts
# malformed inputs (String yields each bare continuation byte as its own invalid
# Char). Count by iteration so length/collect agree with the String oracle.
function Base.length(s::CompactString)
    n = 0
    for _ in s
        n += 1
    end
    return n
end

function Base.:(==)(x::CompactString, y::CompactString)
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    if n <= COMPACTSTRING_INLINE
        return x.p.a == y.p.a && x.p.b == y.p.b   # payload holds the full content
    end
    x.p.a == y.p.a || return false                # length + 4-byte prefix reject
    GC.@preserve x y begin
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, abs(csoff(x.p))),
                     pointer(y.data, abs(csoff(y.p))), n) == 0
    end
end
# Direct byte comparison against String — Base's generic AbstractString ==
# decodes chars, which is an order of magnitude slower on this hot path
# (filtering/grouping compare CompactString columns against String literals constantly).
function Base.:(==)(x::CompactString, y::Union{String, SubString{String}})
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    GC.@preserve x y begin
        py = pointer(y)
        if n <= COMPACTSTRING_INLINE
            @inbounds for i in 1:n
                codeunit(x, i) == unsafe_load(py, i) || return false
            end
            return true
        end
        off = csoff(x.p)
        o = off < 0 ? -off : off
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, o), py, n) == 0
    end
end
Base.:(==)(y::Union{String, SubString{String}}, x::CompactString) = x == y

# Ordering: memcmp over the bytes, exactly like String's `cmp` (Base's generic
# AbstractString fallback iterates chars — measured 15-45x slower on sortperm).
# Inline×inline compares in registers; view×view goes straight to memcmp on
# the retained buffers; only the mixed case materializes a stack scratch.
# Raw payload words with content byte k at byte k (byte 1 = LSB of w1) —
# bit-defined, so endian-independent.
@inline _cs_words(s::CompactString) =
    ((s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32), s.p.b >> 32)
@inline _cs_scratch(s::CompactString) = map(htol, _cs_words(s))
function Base.cmp(x::CompactString, y::CompactString)
    nx, ny = ncodeunits(x), ncodeunits(y)
    if (nx <= COMPACTSTRING_INLINE) & (ny <= COMPACTSTRING_INLINE)
        # Register compare in memcmp order: payload words are zero-padded past
        # each length, so the first differing big-endian word decides by the
        # first differing byte; words all equal means the shared prefix
        # matches and any longer side is all-NUL past the shorter — exactly
        # memcmp(min bytes) then the length tiebreak. The non-short-circuit
        # `&` (one branch) and falling into the ORIGINAL unified tail below
        # measured strictly faster than a dedicated view×view branch — 2.7×
        # on inline-heavy sorts, 1.3× on view-heavy — adjudicated by
        # interleaved same-process A/B.
        w1x, w2x = _cs_words(x)
        w1y, w2y = _cs_words(y)
        a, b = bswap(w1x), bswap(w1y)
        a == b || return a < b ? -1 : 1
        a, b = bswap(w2x), bswap(w2y)
        a == b || return a < b ? -1 : 1
        return cmp(nx, ny)
    end
    n = min(nx, ny)
    rx = Ref(_cs_scratch(x)); ry = Ref(_cs_scratch(y))
    GC.@preserve x y rx ry begin
        px = nx <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, abs(csoff(x.p)))
        py = ny <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, ry)) :
             pointer(y.data, abs(csoff(y.p)))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, py, n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
function Base.cmp(x::CompactString, y::Union{String, SubString{String}})
    nx, ny = ncodeunits(x), ncodeunits(y)
    n = min(nx, ny)
    rx = Ref(_cs_scratch(x))
    GC.@preserve x y rx begin
        px = nx <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, abs(csoff(x.p)))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, pointer(y), n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
Base.cmp(y::Union{String, SubString{String}}, x::CompactString) = -cmp(x, y)
Base.isless(x::CompactString, y::CompactString) = cmp(x, y) < 0
Base.isless(x::CompactString, y::Union{String, SubString{String}}) = cmp(x, y) < 0
Base.isless(y::Union{String, SubString{String}}, x::CompactString) = cmp(y, x) < 0

# hash must agree with `String`'s so mixed Dict{String}/CompactString use is sound.
# is the correctness-first choice — the production version shares InlineStrings'
# memhash approach (which is exactly the private-API exposure CSV.jl #1164 is
# about, so the kernel does not copy it).
# hash contract: hash(cs) == hash(String(cs)) — CompactStrings are Dict keys
# next to Strings. Base hashes a String as memhash(bytes, len, seed) + seed;
# we run the same C hash over the bytes we already have: the retained buffer
# for views, a stack copy of the payload words for inline strings. No String
# allocation on either path.
function Base.hash(s::CompactString, h::UInt)
    h += Base.memhash_seed
    n = ncodeunits(s)
    if n > COMPACTSTRING_INLINE
        off = csoff(s.p)
        o = off < 0 ? -off : off
        GC.@preserve s begin
            return ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32),
                         pointer(s.data, o), n, h % UInt32) + h
        end
    end
    # inline: bytes 1-4 are the high 32 bits of `a`, bytes 5-12 are `b` —
    # pack them contiguously into two little-endian words: word 1 = bytes 1-8
    # = (a>>32) | (low 32 bits of b) << 32, word 2 = bytes 9-12 = b >> 32
    w1 = (s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32)
    w2 = s.p.b >> 32
    scratch = (htol(w1), htol(w2))
    r = Ref(scratch)
    GC.@preserve r begin
        p = Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, r))
        return ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), p, n, h % UInt32) + h
    end
end

function Base.String(s::CompactString)
    n = ncodeunits(s)
    if n > COMPACTSTRING_INLINE
        # view: one memcpy out of the retained buffer
        off = csoff(s.p)
        o = off < 0 ? -off : off
        GC.@preserve s begin
            return unsafe_string(pointer(s.data, o), n)
        end
    end
    out = Vector{UInt8}(undef, n)
    @inbounds for i in 1:n
        out[i] = codeunit(s, i)
    end
    return String(out)
end
Base.convert(::Type{String}, s::CompactString) = String(s)
Base.Symbol(s::CompactString) = Symbol(String(s))
Base.promote_rule(::Type{CompactString}, ::Type{String}) = String

function Base.write(io::IO, s::CompactString)
    n = 0
    @inbounds for i in 1:ncodeunits(s)
        n += write(io, codeunit(s, i))
    end
    return n
end
Base.print(io::IO, s::CompactString) = (write(io, s); nothing)

# The column builder: payloads + the two buffers views resolve into.
mutable struct StringColumn
    payloads::Vector{CompactStringPayload}
    buf::Vector{UInt8}
    extra::Vector{UInt8}          # unescaped long values (rare); guarded by extralock
    extralock::ReentrantLock
    e::UInt8                      # escape char
    cq::UInt8                     # close-quote char (e == cq for RFC ""-doubling)
end
StringColumn(n::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8) =
    StringColumn(fill(PAYLOAD_MISSING, n), buf, UInt8[], ReentrantLock(), e, cq)

# Parse-time pooling staging: one per (column × chunk), single-task-owned.
# Cells intern into a chunk-local table as they parse — the hash work rides the
# parallel per-chunk loops instead of a serial post-pass, and the stitch merge
# hashes only LEVELS (few) before remapping refs as a flat integer pass.
# `maxlevels` is the whole-column policy bound (a chunk exceeding it locally
# proves the column exceeds it globally); `aborted` is shared by every chunk of
# the column so one abandon stops the others' interning — checked only on the
# new-level slow path, never per cell. On abandon the segment DEGRADES in
# place: payloads reconstruct from levels (refs are dense indices into them),
# the extra buffer transfers whole (offsets already extra-relative), and the
# remaining rows parse through the plain string path. A failed gamble costs
# nothing but the interning already done.
# Interning table keys never allocate: inline payloads ARE their identity
# (canonical two-word bits ⇒ a UInt128 key), and view/extra payloads wrap in
# ViewKey, whose hash walks codeunits (FNV-1a) instead of materializing a
# String — the old Dict{CompactString,…} paid TWO allocations per cell lookup through
# hash(::CompactString) = hash(String(s)) and made pooling 3× the cost of the parse
# on 400-level columns.
# Open-addressing table for INLINE level keys: canonical payload bits are the
# key, a full-word mix picks the slot, and linear probing resolves. Empty slots
# hold typemax(UInt128), which is impossible as a payload because the length
# field cannot be all-ones. The all-zero quoted-empty key remains valid. Probes
# touch only the key array and load refs on hits. This is the per-cell interning
# hot path; Dict{UInt128,…} machinery measured 3-5× slower.
const IT_EMPTY = typemax(UInt128)

mutable struct InlineTable
    slots::Vector{UInt128}
    refs::Vector{UInt32}
    count::Int
    mask::UInt64
end
InlineTable() = InlineTable(fill(IT_EMPTY, 64), Vector{UInt32}(undef, 64), 0, UInt64(63))

@inline _inlinekey(p::CompactStringPayload) = (UInt128(p.a) << 64) | p.b
# Raw-span keys set bit 31 of the payload's length field (key bit 95): no
# canonical content payload can carry it (lengths are at most 12 here), so raw
# keys and content keys share the InlineTable without colliding — and
# typemax(UInt128) (IT_EMPTY) stays unreachable exactly as before.
const RAWKEY_BIT = UInt128(0x8000_0000) << 64

@inline function _itmix(k::UInt128)
    # Payload suffix bytes occupy progressively higher bits. Mix both words,
    # then avalanche so a power-of-two table does not see only byte 5.
    x = UInt64(k & typemax(UInt64)) ⊻
        UInt64(k >> 64) * UInt64(0x9e3779b97f4a7c15)
    x = (x ⊻ (x >> 30)) * UInt64(0xbf58476d1ce4e5b9)
    x = (x ⊻ (x >> 27)) * UInt64(0x94d049bb133111eb)
    return x ⊻ (x >> 31)
end

@inline function itget(t::InlineTable, k::UInt128)
    i = _itmix(k) & t.mask
    @inbounds while true
        s = t.slots[i + 1]
        s === k && return t.refs[i + 1]
        s === IT_EMPTY && return UInt32(0)
        i = (i + 1) & t.mask
    end
end

function itset!(t::InlineTable, k::UInt128, ref::UInt32)
    if (t.count + 1) << 1 > length(t.refs)          # ≤50% load
        oldslots, oldrefs = t.slots, t.refs
        n = length(oldrefs) << 2
        t.slots = fill(IT_EMPTY, n)
        t.refs = Vector{UInt32}(undef, n)
        t.mask = UInt64(n - 1)
        t.count = 0
        for x in eachindex(oldrefs)
            oldslots[x] === IT_EMPTY || itset!(t, oldslots[x], oldrefs[x])
        end
    end
    i = _itmix(k) & t.mask
    @inbounds while t.slots[i + 1] !== IT_EMPTY
        i = (i + 1) & t.mask
    end
    @inbounds t.slots[i + 1] = k
    @inbounds t.refs[i + 1] = ref
    t.count += 1
    return t
end

struct ViewKey
    s::CompactString
end
function Base.hash(k::ViewKey, h::UInt)
    s = k.s
    hv = 0xcbf29ce484222325 % UInt64
    @inbounds for i in 1:ncodeunits(s)
        hv = (hv ⊻ codeunit(s, i)) * 0x00000100000001b3
    end
    return hash(hv, h)
end
Base.:(==)(a::ViewKey, b::ViewKey) = a.s == b.s

mutable struct PoolSegment
    refs::Vector{UInt32}                 # 0 = missing/not-yet-parsed
    itable::InlineTable                  # inline levels: payload bits are the key
    vtable::Dict{ViewKey, UInt32}        # view/extra levels: content hash, no alloc
    rawvtable::Dict{ViewKey, UInt32}     # >12-byte RAW escaped spans -> ref (hits skip unescape)
    levelpayloads::Vector{CompactStringPayload}
    extra::Vector{UInt8}                 # unescaped bytes for NEW levels only
    buf::Vector{UInt8}
    e::UInt8
    cq::UInt8
    maxlevels::Int
    aborted::Threads.Atomic{Bool}
    degraded::Union{Nothing, StringColumn}
end
PoolSegment(n::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8, maxlevels::Int,
            aborted::Threads.Atomic{Bool}) =
    PoolSegment(zeros(UInt32, n), InlineTable(), Dict{ViewKey, UInt32}(),
                Dict{ViewKey, UInt32}(),
                CompactStringPayload[], UInt8[], buf, e, cq, maxlevels, aborted, nothing)

# per-column pooling context the driver threads through chunk tasks
struct PoolCtx
    maxlevels::Vector{Int}                  # per column (0 = column never pools)
    aborted::Vector{Threads.Atomic{Bool}}   # one per column
    skip::Vector{Bool}   # sample proved the policy bound must fail: never attempt
end

_levelcell(ps::PoolSegment, p::CompactStringPayload) =
    Int(cslen(p)) <= COMPACTSTRING_INLINE ? CompactString(p, EMPTY_BYTES) :
    CompactString(p, csoff(p) < 0 ? ps.extra : ps.buf)

# refs so far become payloads; the extra buffer moves across untouched
function _degradepool!(ps::PoolSegment)
    payloads = fill(PAYLOAD_MISSING, length(ps.refs))
    @inbounds for i in eachindex(ps.refs)
        r = ps.refs[i]
        r == 0 || (payloads[i] = ps.levelpayloads[Int(r)])
    end
    scol = StringColumn(payloads, ps.buf, ps.extra, ReentrantLock(), ps.e, ps.cq)
    ps.degraded = scol
    return scol
end

# The kernel's own unescape: `""` collapses to `"` when e == cq; `\X` drops the
# backslash when e != cq. Spans are Int64/Int32 end to end, so a single field
# may be arbitrarily wide (the root cause of CSV.jl issue #935 was a 20-bit
# length cap in an intermediate representation — there is no intermediate here).
function _unescape_bytes(buf::Vector{UInt8}, pos::Int64, len::Int32, e::UInt8, cq::UInt8)
    out = Vector{UInt8}(undef, len)
    n = 0
    i = Int(pos)
    last = i + Int(len) - 1
    @inbounds while i <= last
        b = buf[i]
        if b == e && i < last && (e != cq || buf[i + 1] == cq)
            n += 1
            out[n] = e == cq ? cq : buf[i + 1]
            i += 2
        else
            n += 1
            out[n] = b
            i += 1
        end
    end
    return resize!(out, n)
end
_unescape(buf::Vector{UInt8}, pos::Int64, len::Int32, e::UInt8, cq::UInt8) =
    String(_unescape_bytes(buf, pos, len, e, cq))

# All-missing column.
# The user-facing string column. getindex returns a `CompactString` (or `missing`) with
# NO allocation: inline values live in the payload, long values view into `buf`
# (input) or `extra` (unescaped-at-parse-time). `materialize` copies out to
# `Vector{String}`, detaching from both buffers.
struct CompactStringVector{ELT} <: AbstractVector{ELT}
    payloads::Vector{CompactStringPayload}
    buf::Vector{UInt8}
    extra::Vector{UInt8}
end
Base.size(v::CompactStringVector) = size(v.payloads)
Base.@propagate_inbounds @inline function Base.getindex(v::CompactStringVector{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = cslen(p)
    len < 0 && return missing
    len <= COMPACTSTRING_INLINE && return CompactString(p, EMPTY_BYTES)
    return CompactString(p, csoff(p) < 0 ? v.extra : v.buf)
end
# All-present columns skip the missing branch entirely — the concrete return
# type is what lets access compile down to zero allocations.
Base.@propagate_inbounds @inline function Base.getindex(v::CompactStringVector{CompactString}, i::Int)
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = cslen(p)
    len <= COMPACTSTRING_INLINE && return CompactString(p, EMPTY_BYTES)
    return CompactString(p, csoff(p) < 0 ? v.extra : v.buf)
end

function materialize(v::CompactStringVector{ELT}) where {ELT}
    out = Vector{ELT === CompactString ? String : Union{String, Missing}}(undef, length(v))
    scratch = Vector{UInt8}(undef, 16)   # inline payloads reconstruct via two word stores
    GC.@preserve scratch begin
        q = pointer(scratch)
        @inbounds for i in eachindex(v.payloads)
            p = v.payloads[i]
            len = cslen(p)
            if len < 0
                out[i] = missing
            elseif len <= COMPACTSTRING_INLINE
                unsafe_store!(Ptr{UInt64}(q), htol((p.a >> 32) | (p.b << 32)))
                unsafe_store!(Ptr{UInt64}(q + 8), htol(p.b >> 32))
                out[i] = unsafe_string(q, len)
            else
                off = csoff(p)
                o = off < 0 ? -off : off
                src = off < 0 ? v.extra : v.buf
                GC.@preserve src begin
                    out[i] = unsafe_string(pointer(src, o), len)
                end
            end
        end
    end
    return out
end

# --- per-(column × chunk) parse loops ---------------------------------------
#
# THE point of the whole design: each call below is monomorphic in the column type.
# Dynamic dispatch happens once per (column, chunk) — thousands of times per file —
# instead of once per cell. A mid-chunk type surprise returns the offending row so
# the driver can promote and re-run *this column only*.

# Strictness dividend: because every kernel accepts exactly the spellings the
# detection cascade assigns to it (parse-set ≡ detect-set), a value that parses
# as an inferred column's type T can never detect as a type earlier in the
# cascade. The per-value "canonical conflict" guard the Parsers-based kernel
# needed here — six prefix probes on the hot path of every inferred
# Bool/temporal column — is free by construction.

# Returns 0 on success, or the local row of the first conflicting value.
function parsecolchunk!(col::Union{TypedColumn{T}, UnionColumn{T}}, buf::Vector{UInt8},
                        ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int)) where {T}
    scratch = _scratchfor(opts)
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue                      # short row ⇒ missing (reported once per row by the driver)
        pos, len = sp
        len == 0 && continue                            # empty ⇒ missing
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        st == CELL_MISSING && continue                  # sentinel / stripped-to-empty
        if st == CELL_VALUE && clen > 0 && !esc
            ti, tj = _trimblanks(buf, cpos, cpos + clen - 1)
            if ti > tj                              # blanks only: parse the original (invalid) span
                ti, tj = cpos, cpos + clen - 1
            end
            v, ok = parsevalue(T, buf, ti, tj, opts, scratch)
            if ok
                _storevalue!(col, out, v)
                continue
            end
        end
        # invalid for T (also: malformed quoting, quoted-empty, escaped content)
        if userprovided
            problemrow = problemrowbase + localrow
            kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
            message = st == CELL_BADQUOTE ? "malformed quoting in " :
                      "cannot parse $(T) from "
            pushproblem!(problems, problemrow, j, pos, kind, message * excerpt(buf, pos, len))
            # value stays missing under strict=false semantics
        else
            return lr                                   # inference conflict ⇒ promote & re-parse column
        end
    end
    return 0
end

function parsecolchunk!(col::StringColumn, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int), fromrow::Int=0)
    payloads = col.payloads
    staging::Union{Nothing, NTuple{4, Vector}} = nothing  # (bytes, rows, offs, lens) for escaped-long cells
    @inbounds for lr in max(fromrow, ci.firstdatarow):totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        pos, len = sp
        len == 0 && continue                            # unquoted empty ⇒ missing; quoted "" survives below
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_quoted_field,
                         "malformed quoting in " * excerpt(buf, pos, len))
            continue
        end
        if st == CELL_MISSING
            continue
        end
        if !_wasquoted(buf, pos, len, opts) && _delimclash(buf, cpos, clen, opts.delim)
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_value,
                         "bare quote engaged structural protection in " * excerpt(buf, pos, len))
        end
        if esc
            # escaped values are unescaped ONCE, at parse time (CompactString needs O(1)
            # codeunit access): short results build inline payloads allocation-
            # free; long ones stage locally and flush to the shared extra buffer
            # under a single lock per (column × chunk), not per cell
            inl = _unescape_inline(buf, cpos, clen, col.e, col.cq)
            if inl !== nothing
                payloads[out] = inl
            else
                if staging === nothing
                    staging = (UInt8[], Int[], Int[], Int[])
                end
                _stageescaped!(staging, buf, cpos, clen, out, col.e, col.cq)
            end
        elseif clen <= COMPACTSTRING_INLINE
            payloads[out] = inline_payload(buf, cpos, clen)
        else
            payloads[out] = view_payload(buf, cpos, clen, Int64(cpos))
        end
    end
    staging === nothing || _flushstaging!(col, payloads, staging)
    return 0
end

# Named top-level helpers, NOT closures: the previous do-block flush captured
# locals that were also reassigned in the parse loop, so Julia boxed them —
# every staged cell then paid allocating Any arithmetic (~2M boxed Ints on a
# 200 MiB mixed file). Same bug class as the task-body war story; same rule.
@inline function _stageescaped!(staging::NTuple{4, Vector}, buf::Vector{UInt8},
                                cpos::Int, clen::Int, out::Int, e::UInt8, cq::UInt8)
    sbytes = staging[1]::Vector{UInt8}
    spos = length(sbytes) + 1
    n = _unescape_append!(sbytes, buf, cpos, clen, e, cq)
    push!(staging[2]::Vector{Int}, out)
    push!(staging[3]::Vector{Int}, spos)
    push!(staging[4]::Vector{Int}, n)
    return
end

function _flushstaging!(col::StringColumn, payloads::Vector{CompactStringPayload},
                        staging::NTuple{4, Vector})
    sbytes = staging[1]::Vector{UInt8}
    srows = staging[2]::Vector{Int}
    soffs = staging[3]::Vector{Int}
    slens = staging[4]::Vector{Int}
    lock(col.extralock)
    try
        base = Int64(length(col.extra))
        append!(col.extra, sbytes)
        @inbounds for k in eachindex(srows)
            payloads[srows[k]] = view_payload(sbytes, soffs[k], slens[k],
                                              -(base + Int64(soffs[k])))
        end
    finally
        unlock(col.extralock)
    end
    return
end

# The pooled twin of the String path: identical span/hygiene handling, but
# cells intern into the chunk-local table instead of materializing payloads.
# Escaped cells unescape into the segment extra and REWIND it when the level
# already exists — dedup storage for free. The new-level slow path is the only
# place the policy bound and the shared abort flag are consulted; on abandon
# the segment degrades and the remaining rows run the plain string loop.
function parsecolchunk!(ps::PoolSegment, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int))
    refs = ps.refs
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        pos, len = sp
        len == 0 && continue                            # unquoted empty ⇒ missing; quoted "" survives below
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_quoted_field,
                         "malformed quoting in " * excerpt(buf, pos, len))
            continue
        end
        if st == CELL_MISSING
            continue
        end
        if !_wasquoted(buf, pos, len, opts) && _delimclash(buf, cpos, clen, opts.delim)
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_value,
                         "bare quote engaged structural protection in " * excerpt(buf, pos, len))
        end
        rewind = -1
        if esc
            # raw-span fast path: identical raw spellings unescape to identical
            # values, so a repeat escaped cell resolves WITHOUT the unescape
            # walk. Inline raw spans key the shared InlineTable under
            # RAWKEY_BIT; longer ones use their own dict (an unquoted
            # bare-quote cell can equal an escaped raw span byte-for-byte, so
            # raw and content key spaces must stay disjoint). The key is
            # recomputed at registration below so nothing escapes this branch —
            # extra live state was measured as +5.7% on the escape-free hot
            # path.
            rref = clen <= COMPACTSTRING_INLINE ?
                   itget(ps.itable, _inlinekey(inline_payload(buf, cpos, clen)) | RAWKEY_BIT) :
                   get(ps.rawvtable,
                       ViewKey(CompactString(view_payload(buf, cpos, clen, Int64(cpos)), buf)),
                       UInt32(0))
            if rref != 0
                refs[out] = rref
                continue
            end
            inl = _unescape_inline(buf, cpos, clen, ps.e, ps.cq)
            if inl !== nothing
                p = inl
            else
                rewind = length(ps.extra)
                n = _unescape_append!(ps.extra, buf, cpos, clen, ps.e, ps.cq)
                p = view_payload(ps.extra, rewind + 1, n, -(Int64(rewind) + 1))
            end
        elseif clen <= COMPACTSTRING_INLINE
            p = inline_payload(buf, cpos, clen)
        else
            p = view_payload(buf, cpos, clen, Int64(cpos))
        end
        # Inline payloads are canonical (equal strings ⇒ identical payloads, and
        # an inline-length cell always produces an inline payload), so a small
        # level table resolves by comparing two UInt64s per level — no hashing.
        # A miss over the full scan is a REAL miss for inline cells. View/extra
        # payloads and big tables take the Dict.
        ref = UInt32(0)
        lp = ps.levelpayloads
        llen = length(lp)
        isinline = Int(cslen(p)) <= COMPACTSTRING_INLINE
        if isinline && llen <= 16
            @inbounds for l in 1:llen
                if lp[l].a === p.a && lp[l].b === p.b
                    ref = UInt32(l)
                    break
                end
            end
        elseif isinline
            ref = itget(ps.itable, _inlinekey(p))
        else
            ref = get(ps.vtable, ViewKey(_levelcell(ps, p)), UInt32(0))
        end
        if ref == 0
            if llen >= ps.maxlevels || ps.aborted[]
                ps.aborted[] = true
                # A long escaped miss appended its candidate bytes before the
                # level lookup. They are not referenced when the bound fails;
                # rewind them before transferring `extra` to the flat column.
                rewind >= 0 && resize!(ps.extra, rewind)
                scol = _degradepool!(ps)
                return parsecolchunk!(scol, buf, ci, j, rowbase, opts, userprovided,
                                      problems, problemrowbase, mask, maskbase,
                                      reportlimit, lr)
            end
            push!(ps.levelpayloads, p)
            ref = UInt32(llen + 1)
            if isinline
                itset!(ps.itable, _inlinekey(p), ref)
            else
                ps.vtable[ViewKey(_levelcell(ps, p))] = ref
            end
        elseif rewind >= 0
            resize!(ps.extra, rewind)              # duplicate escaped level: drop its bytes
        end
        if esc
            # next occurrence of this raw spelling hits before unescaping
            if clen <= COMPACTSTRING_INLINE
                itset!(ps.itable, _inlinekey(inline_payload(buf, cpos, clen)) | RAWKEY_BIT, ref)
            else
                ps.rawvtable[ViewKey(CompactString(view_payload(buf, cpos, clen,
                                                                Int64(cpos)), buf))] = ref
            end
        end
        refs[out] = ref
    end
    return 0
end

# A column believed all-missing: inferred columns report the first conflict so
# the driver can promote; explicit Missing columns report every present value.
function parsecolchunk_missing(buf::Vector{UInt8}, ci::ChunkIndex, j::Int,
                               rowbase::Int, opts::ValueOpts,
                               userprovided::Bool, problems,
                               mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                               reportlimit::Int=typemax(Int))
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        mask !== nothing && !mask[maskbase + localrow] && continue
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        _, len = sp
        len == 0 && continue
        st = cellcontent(buf, sp[1], len, opts)[4]
        if st != CELL_MISSING
            userprovided || return lr
            out = rowbase + localrow
            kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
            message = st == CELL_BADQUOTE ? "malformed quoting in " :
                      "column typed Missing contains "
            pushproblem!(problems, out, j, sp[1], kind,
                         message * excerpt(buf, sp[1], len))
        end
    end
    return 0
end

# ---------------------------------------------------------------------------
# Problems: errors as data. Bounded (maxproblems) so a pathological file cannot
# exhaust memory. Retention and final order use source order, not task arrival
# order; the count of omitted reports is itself recorded.
# ---------------------------------------------------------------------------

struct Problem
    row::Int          # 1-based data row (0 = file-level problem)
    col::Int          # 1-based column (0 = row-level problem)
    pos::Int          # absolute byte offset into the source buffer
    kind::Symbol      # :short_row | :long_row | :invalid_value | :invalid_quoted_field | :unclosed_quote
    message::String
end

mutable struct ProblemLog
    items::Vector{Problem}
    limit::Int
    dropped::Int
    first::Union{Nothing, Problem}
    heaped::Bool                  # items are a max-heap by source order (full logs)
end
function ProblemLog(limit::Int)
    limit >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $limit)"))
    return ProblemLog(Problem[], limit, 0, nothing, false)
end

problemkey(p::Problem) = (p.pos, p.row, p.col, String(p.kind), p.message)

# problemkey's order without its allocations: Symbol comparison uses the same
# lexical strcmp order as String(Symbol) without materializing either string.
@inline function problemless(a::Problem, b::Problem)
    a.pos != b.pos && return a.pos < b.pos
    a.row != b.row && return a.row < b.row
    a.col != b.col && return a.col < b.col
    a.kind != b.kind && return isless(a.kind, b.kind)
    return a.message < b.message
end

# Bounded retention keeps the `limit` SOURCE-EARLIEST problems. A full log
# maintains its items as a max-heap so displacing the worst retained entry is
# O(log limit) — the previous per-overflow findmax scan was O(limit) each,
# quadratic-by-cap on problem-dense files (measured: a 5%-ragged 20 MiB file
# spent seconds scanning a 10k reservoir per dropped report).
function _siftdown!(items::Vector, lt::F, i::Int) where {F}
    n = length(items)
    @inbounds while true
        l = 2i
        m = i
        l <= n && lt(items[m], items[l]) && (m = l)
        l + 1 <= n && lt(items[m], items[l + 1]) && (m = l + 1)
        m == i && return
        items[i], items[m] = items[m], items[i]
        i = m
    end
end

function _heapify!(items::Vector, lt::F) where {F}
    for i in (length(items) >> 1):-1:1
        _siftdown!(items, lt, i)
    end
end

function pushproblem!(log::ProblemLog, row::Int, col::Int, pos::Int, kind::Symbol, msg::String)
    p = Problem(row, col, pos, kind, msg)
    (log.first === nothing || problemless(p, log.first)) && (log.first = p)
    if length(log.items) < log.limit
        push!(log.items, p)
    else
        log.dropped += 1
        if log.limit > 0
            if !log.heaped
                _heapify!(log.items, problemless)
                log.heaped = true
            end
            @inbounds if problemless(p, log.items[1])
                log.items[1] = p
                _siftdown!(log.items, problemless, 1)
            end
        end
    end
    return
end

function sortproblems!(log::ProblemLog)
    log.heaped = false
    sort!(log.items; lt=problemless)
    return log.items
end

struct LocatedProblem
    problem::Problem
    chunk::Int
end

mutable struct PendingProblemLog
    items::Vector{LocatedProblem}
    limit::Int
    dropped::Int
    first::Union{Nothing, LocatedProblem}
    lock::ReentrantLock
    heaped::Bool
end
function PendingProblemLog(limit::Int)
    limit >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $limit)"))
    return PendingProblemLog(LocatedProblem[], limit, 0, nothing, ReentrantLock(), false)
end

@inline locatedless(a::LocatedProblem, b::LocatedProblem) = problemless(a.problem, b.problem)

# Fold one task-local log into the globally bounded reservoir, then release the
# local retained entries. Row ids stay chunk-local until every chunk is indexed.
# Absolute positions are the first problem-key field and chunks do not overlap,
# so later row rebasing cannot change which problems belong under the cap.
# The reservoir keeps the same max-heap-when-full discipline as ProblemLog —
# this loop runs under the lock, so a linear scan per overflow would serialize
# every chunk behind quadratic-by-cap work.
function mergeproblems!(out::PendingProblemLog, log::ProblemLog, chunk::Int)
    log.first === nothing && return
    lock(out.lock) do
        out.dropped += log.dropped
        if log.first !== nothing
            first = LocatedProblem(log.first, chunk)
            (out.first === nothing || locatedless(first, out.first)) &&
                (out.first = first)
        end
        for p in log.items
            lp = LocatedProblem(p, chunk)
            if length(out.items) < out.limit
                push!(out.items, lp)
            else
                out.dropped += 1
                if out.limit > 0
                    if !out.heaped
                        _heapify!(out.items, locatedless)
                        out.heaped = true
                    end
                    @inbounds if locatedless(lp, out.items[1])
                        out.items[1] = lp
                        _siftdown!(out.items, locatedless, 1)
                    end
                end
            end
        end
    end
    log.items = Problem[]
    log.dropped = 0
    log.first = nothing
    log.heaped = false
    return
end

function rebaseproblem(lp::LocatedProblem, rowbases)
    p = lp.problem
    p.row == 0 && return p
    return Problem(p.row + rowbases[lp.chunk], p.col, p.pos, p.kind, p.message)
end

function finishproblems(log::PendingProblemLog, rowbases)
    out = ProblemLog(log.limit)
    out.items = Problem[rebaseproblem(lp, rowbases) for lp in log.items]
    out.dropped = log.dropped
    out.first = log.first === nothing ? nothing : rebaseproblem(log.first, rowbases)
    return out
end

function excerpt(buf::Vector{UInt8}, pos::Int, len::Int; maxbytes::Int=32)
    n = min(len, maxbytes)
    s = String(buf[pos:pos + n - 1])
    return repr(len > maxbytes ? s * "…" : s)
end

function parseheader!(buf::Vector{UInt8}, ci::ChunkIndex, opts::ValueOpts,
                      d::Dialect, log::ProblemLog)
    hrow = ci.firstdatarow
    nh = nfields(ci, hrow)
    names = Vector{Symbol}(undef, nh)
    for j in 1:nh
        pos, len = fieldspan(ci, hrow, j)::Tuple{Int, Int}
        if len == 0
            names[j] = Symbol("Column", j)
            continue
        end
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            names[j] = Symbol(String(buf[pos:pos + len - 1]))
            pushproblem!(log, 0, j, pos, :invalid_quoted_field,
                         "malformed quoting in header " * excerpt(buf, pos, len))
        elseif st == CELL_MISSING || clen == 0
            names[j] = Symbol("Column", j)
        elseif !_wasquoted(buf, pos, len, opts) && _delimclash(buf, cpos, clen, opts.delim)
            names[j] = Symbol(String(buf[pos:pos + len - 1]))
            pushproblem!(log, 0, j, pos, :invalid_value,
                         "bare quote engaged structural protection in header " *
                         excerpt(buf, pos, len))
        else
            names[j] = Symbol(esc ?
                              _unescape(buf, Int64(cpos), Int32(clen), opts.e, d.cq) :
                              GC.@preserve(buf, unsafe_string(pointer(buf, cpos), clen)))
        end
    end
    ci.firstdatarow = hrow + 1
    return names
end

# ---------------------------------------------------------------------------
# L5: the driver.
# ---------------------------------------------------------------------------

struct ParsedTable
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
    problems::Vector{Problem}
    droppedproblems::Int
end

Base.names(t::ParsedTable) = t.names
columns(t::ParsedTable) = t.columns
problems(t::ParsedTable) = t.problems
function Base.getindex(t::ParsedTable, nm::Symbol)
    j = findfirst(==(nm), t.names)
    j === nothing && throw(KeyError(nm))
    return t.columns[j]
end

function Base.show(io::IO, t::ParsedTable)
    print(io, "CSVKernel.ParsedTable: $(t.nrows) × $(length(t.names))")
    for (nm, col) in zip(t.names, t.columns)
        print(io, "\n  ", nm, "::", eltype(col))
    end
    isempty(t.problems) || print(io, "\n  ($(length(t.problems)) problem(s) recorded)")
end

# Stratified type inference: sample up to `nsample` rows evenly spaced across the
# WHOLE index — the index makes late-file type surprises as visible as early ones,
# which is what keeps mid-parse promotions rare. (Contrast: prefix-only sampling,
# the root of most "worked until row 2 million" issues.)
function sampletypes(buf::Vector{UInt8}, chunks::Vector{ChunkIndex}, ncols::Int,
                     opts::ValueOpts; nsample::Int=128,
                     selected::Union{Nothing, Vector{Bool}}=nothing,
                     sawmissing::Union{Nothing, Vector{Bool}}=nothing,
                     colopts::Union{Nothing, Vector{ValueOpts}}=nothing,
                     maxrows::Union{Nothing, Int}=nothing)
    nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    total = sum(nrows, chunks; init=0)
    # rows past `limit` are never output: they must not seed union finals
    maxrows === nothing || (total = min(total, maxrows))
    total == 0 && return fill(Missing, ncols)
    types = fill(Missing, ncols)
    count = min(total, nsample)
    for k in 1:count
        # Exact integer interpolation includes both ends without duplicates.
        gr = count == 1 ? 1 :
             1 + Int(widemul(k - 1, total - 1) ÷ (count - 1))
        ci, lr = locate(chunks, gr)
        sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing, colopts)
    end
    return types
end

@inline function sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing=nothing,
                               colopts=nothing)
    for j in 1:ncols
        selected !== nothing && !selected[j] && continue
        sp = fieldspan(ci, lr, j)
        if sp === nothing
            sawmissing === nothing || (sawmissing[j] = true)
            continue
        end
        dt = detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j))
        sawmissing !== nothing && dt === Missing && (sawmissing[j] = true)
        types[j] = promote_kernel(types[j], dt)
    end
    return
end

# Content fingerprint of a sampled parsed string (FNV-1a over at most 64
# canonical bytes, then mixed with the canonical length). Quoting, outer
# whitespace, sentinels, and escapes must agree with `StringColumn`: equal
# parsed strings MUST have equal fingerprints or the distinct-count proof below
# would be unsound. A collision in the other direction merely manufactures a
# repeat, which lets pooling be attempted; the parse-time bound still decides.
@inline function _cellhash(buf::Vector{UInt8}, pos::Int, len::Int,
                           e::UInt8, cq::UInt8, escaped::Bool)
    h = 0xcbf29ce484222325
    if !escaped
        @inbounds for i in pos:(pos + min(len, 64) - 1)
            h = (h ⊻ buf[i]) * 0x00000100000001b3
        end
        return _splitmix64(h ⊻ UInt64(len))
    end
    n = 0
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        b = buf[i]
        if b == e && i < last && (e != cq || buf[i + 1] == cq)
            b = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        n += 1
        n <= 64 && (h = (h ⊻ b) * 0x00000100000001b3)
    end
    return _splitmix64(h ⊻ UInt64(n))
end

@inline function _splitmix64(x::UInt64)
    x += 0x9e3779b97f4a7c15
    x = (x ⊻ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ⊻ (x >> 27)) * 0x94d049bb133111eb
    return x ⊻ (x >> 31)
end

# Cell fingerprints for the pooling pre-skip, sampled at deterministically
# jittered positions: one draw per equal-width row bucket, offset by splitmix.
# Equal-stride sampling would alias periodic level patterns. `qrows` restricts
# the draw to qualifying rows. `sampletotal` clips an unmasked parse to its
# output prefix when `limit` retains only part of the final indexed chunk.
function samplerepeathashes(buf::Vector{UInt8}, chunks::Vector{ChunkIndex}, ncols::Int,
                            opts::ValueOpts, selected::Union{Nothing, Vector{Bool}},
                            qrows::Union{Nothing, Vector{Int}}=nothing,
                            rowbases0=nothing; nsample::Int=128,
                            sampletotal::Union{Nothing, Int}=nothing)
    total = qrows === nothing ? something(sampletotal, sum(nrows, chunks; init=0)) :
                                length(qrows)
    hashes = [UInt64[] for _ in 1:ncols]
    total == 0 && return hashes
    count = min(total, nsample)
    x = UInt64(total)
    for k in 1:count
        lo = 1 + Int(widemul(k - 1, total) ÷ count)
        hi = Int(widemul(k, total) ÷ count)
        x = _splitmix64(x)
        r = lo + Int(x % UInt64(max(hi - lo + 1, 1)))
        if qrows === nothing
            ci, lr = locate(chunks, r)
        else
            gr = qrows[r]
            ki = searchsortedlast(rowbases0, gr - 1)
            ci = chunks[ki]
            lr = ci.firstdatarow + (gr - rowbases0[ki]) - 1
        end
        for j in 1:ncols
            selected !== nothing && !selected[j] && continue
            sp = fieldspan(ci, lr, j)
            (sp === nothing || sp[2] == 0) && continue
            pos, len = sp
            firstbyte = @inbounds buf[pos]
            if !opts.stripws &&
               (!opts.quoted || (firstbyte != opts.oq && !_isot(firstbyte))) &&
               !_maybesentinel(opts, firstbyte)
                push!(hashes[j], _cellhash(buf, pos, len, opts.e, opts.cq, false))
                continue
            end
            cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
            st == CELL_VALUE || continue
            push!(hashes[j], _cellhash(buf, cpos, clen, opts.e, opts.cq, esc))
        end
    end
    return hashes
end

function _morethanunique(values::Vector{UInt64}, limit::Int)
    seen = Set{UInt64}()
    for value in values
        push!(seen, value)
        length(seen) > limit && return true
    end
    return false
end

# Stratified sampling restricted to a set of qualifying global rows (the
# filter's two-phase parse: inference must see only rows that will be output).
function sampletypesrows(buf::Vector{UInt8}, chunks::Vector{ChunkIndex}, rowbases0,
                         qrows::Vector{Int}, ncols::Int, opts::ValueOpts,
                         selected::Union{Nothing, Vector{Bool}}; nsample::Int=128,
                         sawmissing::Union{Nothing, Vector{Bool}}=nothing,
                         colopts::Union{Nothing, Vector{ValueOpts}}=nothing)
    types = fill(Missing, ncols)
    total = length(qrows)
    total == 0 && return types
    count = min(total, nsample)
    for k in 1:count
        gr = qrows[count == 1 ? 1 : 1 + Int(widemul(k - 1, total - 1) ÷ (count - 1))]
        # locate via the precomputed bases (all chunks are indexed on this path)
        ki = searchsortedlast(rowbases0, gr - 1)
        ci = chunks[ki]
        lr = ci.firstdatarow + (gr - rowbases0[ki]) - 1
        sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing, colopts)
    end
    return types
end

# Map a global data-row id to (chunk, local row).
function locate(chunks::Vector{ChunkIndex}, grow::Int)
    for ci in chunks
        n = nrows(ci)
        grow <= n && return (ci, ci.firstdatarow + grow - 1)
        grow -= n
    end
    throw(BoundsError(chunks, grow))
end

allocatecolumn(::Type{Missing}, n::Int, buf, e, cq) = nothing
allocatecolumn(::Type{String}, n::Int, buf, e, cq) = StringColumn(n, buf, e, cq)
allocatecolumn(::Type{T}, n::Int, buf, e, cq) where {T} = TypedColumn{T}(n)

# Number of leading chunks needed to cover `limit` data rows (all of them when
# the file is shorter than the limit).
function _limitchunks(chunks::Vector{ChunkIndex}, rowbases::Vector{Int}, limit::Int)
    lastk = 0
    for k in eachindex(chunks)
        lastk = k
        rowbases[k] + nrows(chunks[k]) >= limit && break
    end
    return lastk
end

# Resolve the `select` keyword against the discovered names: nothing (all) or
# a vector of Bool (positional mask), Int, Symbol, or String. Unselected
# columns are never sampled, parsed, or stitched — they simply don't exist to
# the value layer. Output keeps file order; reordering/renaming is the caller's
# (the Scan layer permutes the returned columns).
function resolveselect(select, names::Vector{Symbol}, ncols::Int)
    select === nothing && return nothing
    keep = fill(false, ncols)
    if select isa AbstractVector{Bool}
        length(select) == ncols ||
            throw(ArgumentError("select mask length $(length(select)) != $ncols columns"))
        copyto!(keep, select)
    elseif select isa AbstractVector
        for r in select
            j = r isa Integer ? Int(r) : findfirst(==(Symbol(r)), names)
            (j isa Int && 1 <= j <= ncols) ||
                throw(ArgumentError("select reference $(repr(r)) does not match any column"))
            keep[j] = true
        end
    else
        throw(ArgumentError("select must be a vector of Bool/Int/Symbol/String (got $(typeof(select)))"))
    end
    return keep
end

# Resolve the `types` keyword: nothing | Type | AbstractVector | AbstractDict
# (by name or index). `Union{T,Missing}` collapses to T (missingness is tracked
# per-value, never in the column type).
# Narrow numeric types parse through their native kernel type and convert at
# the API door (0.10 semantics: `types=Int8` parses Int64 and narrows; an
# out-of-range value is a problem, not a crash).
const NARROW_TYPES = Dict{Type, Type}(
    Int8 => Int64, Int16 => Int64, Int32 => Int64,
    UInt8 => Int64, UInt16 => Int64, UInt32 => Int64, UInt64 => Int128,
    Float16 => Float64, Float32 => Float64)
_nativetype(T::Type) = get(NARROW_TYPES, T, T)
_customparseable(T::Type) = isconcretetype(T) &&
    (hasmethod(Base.tryparse, Tuple{Type{T}, String}) ||
     hasmethod(Base.parse, Tuple{Type{T}, String}))

# Dict keys: Integer (position), name (Symbol/String), or Regex (every
# matching name; exact keys take precedence — 0.10 semantics)
function _resolvekeys(dict::AbstractDict, names::Vector{Symbol}, ncols::Int, what::String;
                      validate::Bool=true)
    out = Dict{Int, Any}()
    for (k, v) in dict
        k isa Regex && continue
        j = k isa Integer ? Int(k) : findfirst(==(Symbol(k)), names)
        if j === nothing || !(1 <= j <= ncols)
            validate || continue
            j === nothing && throw(ArgumentError("$what key $k does not match any column"))
            throw(ArgumentError("$what key $k out of range"))
        end
        out[j] = v
    end
    for (k, v) in dict
        k isa Regex || continue
        matched = false
        for (j, nm) in enumerate(names)
            occursin(k, String(nm)) || continue
            matched = true
            haskey(out, j) || (out[j] = v)
        end
        matched || !validate ||
            throw(ArgumentError("$what key $k does not match any column"))
    end
    return out
end

function resolvetypes(types, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    seed = Vector{Union{Nothing, Type}}(nothing, ncols)
    types === nothing && return seed
    function normalize(T)
        T === nothing && return nothing
        T isa Type || throw(ArgumentError("column type must be a Type or nothing (got $(repr(T)))"))
        T = T === Missing ? Missing : Base.nonmissingtype(T)
        T = _nativetype(T)
        parseable = T === Missing ||
                    T in (Int64, Int128, Float64, Bool, Date, DateTime, Time, String,
                          BigInt, BigFloat, Base.UUID) || _customparseable(T)
        parseable || throw(ArgumentError("unsupported column type $T"))
        return T
    end
    if types isa Type
        fill!(seed, normalize(types))
    elseif types isa AbstractVector
        length(types) == ncols || throw(ArgumentError("types vector length $(length(types)) != $ncols columns"))
        seed .= normalize.(types)
    elseif types isa AbstractDict
        for (j, T) in _resolvekeys(types, names, ncols, "types"; validate)
            seed[j] = normalize(T)
        end
    else
        throw(ArgumentError("unsupported types specification: $(typeof(types))"))
    end
    return seed
end

# columns the user declared as Union{Missing, T}: honored even when no missing
# value appears (0.10 semantics — the declared type is the column type)
function declaredmissing(types, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    wm = fill(false, ncols)
    types === nothing && return wm
    ismiss(T) = T isa Type && T !== Missing && Missing <: T
    if types isa Type
        fill!(wm, ismiss(types))
    elseif types isa AbstractVector
        length(types) == ncols && (wm .= ismiss.(types))
    elseif types isa AbstractDict
        for (j, T) in _resolvekeys(types, names, ncols, "types"; validate)
            wm[j] = ismiss(T)
        end
    end
    return wm
end

# the user-REQUESTED types per column (narrow ones included), for the API
# door's post-parse narrowing; nothing where the kernel type is the answer
function requestedtypes(types, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    req = Vector{Union{Nothing, Type}}(nothing, ncols)
    types === nothing && return req
    narrow(T) = (T isa Type && haskey(NARROW_TYPES, Base.nonmissingtype(T))) ?
                Base.nonmissingtype(T) : nothing
    if types isa Type
        fill!(req, narrow(types))
    elseif types isa AbstractVector
        length(types) == ncols && (req .= narrow.(types))
    elseif types isa AbstractDict
        for (j, T) in _resolvekeys(types, names, ncols, "types"; validate)
            req[j] = narrow(T)
        end
    end
    return req
end

@inline _defaultchunkbytes(nbytes::Int, nthreads::Int=Threads.nthreads()) =
    clamp(cld(nbytes, 4 * nthreads), 1 << 16, 1 << 20)

"""
    CSVKernel.parse(buf::Vector{UInt8}; kwargs...) -> ParsedTable
    CSVKernel.parse(str::AbstractString; kwargs...)
    CSVKernel.parse(io::IO; kwargs...)

Eagerly parse delimited data: plan row-aligned chunks, probe a stratified set for
the header and initial types, then fuse each chunk's index and monomorphic column
loops. Conflicts promote per column and stale segments re-parse under the joined
final type. `parallel` selects tasks or plain loops without changing the chunk
layout. The default `chunkbytes` is
`clamp(cld(length(buf), 4 * Threads.nthreads()), 64 KiB, 1 MiB)`; the default
`nsample` is `clamp(probe_rows >> 6, 8, 128)`. Explicit values override both
defaults.
The default records malformed data as problems;
`on_error=:error` escalates the source-earliest problem after parsing.

Keywords: `delim`, `quotechar`, `openquotechar`/`closequotechar`, `escapechar`,
`quoted`, `comment`, `ignoreemptyrows`, `ignorerepeated`, `header` (true | false | Vector), `types`
(Type | Vector | Dict), `dateformat`, `decimal`, `truestrings`/`falsestrings`,
`sentinels` (spellings that parse as missing), `stripwhitespace`, `groupmark`,
`pool`, `chunkbytes`, `parallel`, `fastindex`, `scanner`
(:auto | :vec | :swar | :scalar), `maxproblems`,
`on_error` (:collect | :error), `validate`, `nsample`.
"""
function parse(buf::Vector{UInt8};
               header::Union{Bool, AbstractVector}=true,
               types=nothing,
               dateformat=nothing,
               decimal::Char='.',
               truestrings=nothing,
               falsestrings=nothing,
               sentinels=nothing,
               stripwhitespace::Bool=false,
               groupmark::Union{Nothing, Char}=nothing,
               pool::Union{Bool, Real, Tuple{<:Real, <:Integer}}=false,
               typemap::Union{Nothing, AbstractDict}=nothing,
               poolspecs::Union{Nothing, Vector{Any}}=nothing,
               colopts::Union{Nothing, Vector{ValueOpts}}=nothing,
               chunkbytes::Union{Nothing, Int}=nothing,
               parallel::Bool=Threads.nthreads() > 1,
               fastindex::Bool=true,
               scanner::Symbol=:auto,
               maxproblems::Int=10_000,
               on_error::Symbol=:collect,
               validate::Bool=true,
               nsample::Union{Nothing, Int}=nothing,
               select=nothing,
               limit::Union{Nothing, Int}=nothing,
               rowmask::Union{Nothing, Vector{Bool}}=nothing,
               index::Union{Nothing, BufferIndex}=nothing,
               reportstructural::Bool=true,
               dialectkw...)
    on_error in (:collect, :error) || throw(ArgumentError("on_error must be :collect or :error"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    limit !== nothing && rowmask !== nothing &&
        throw(ArgumentError("limit and rowmask cannot be combined; bake the limit into the mask"))
    poolspec = _poolpolicy(pool)
    tm = _normalizetypemap(typemap)
    nsample === nothing || nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    # Size-aware defaults. chunkbytes: enough chunks to occupy every thread (4×
    # tasks per thread: at 20 MiB the straggler tail of 2×/thread measured
    # 10-17% across shapes; the 1 MiB cap keeps large-file geometry identical), capped at 1 MiB — the column-at-a-time
    # parse re-walks each chunk once per column, so chunks must stay cache-
    # resident (measured on a 200 MiB × 200-column file: 8 MiB chunks 623 MiB/s
    # → 1 MiB chunks 911 MiB/s), with a 64 KiB floor so per-chunk setup never
    # dominates. nsample: sampled rows pay the full detect cascade and are then
    # parsed again, so tiny files sample a handful of rows and lean on cheap
    # per-column promotion instead, while big files keep the 128-row stratified
    # sample that makes promotion rare.
    if chunkbytes === nothing
        chunkbytes = _defaultchunkbytes(length(buf))
    else
        chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    end
    d = Dialect(; dialectkw...)
    opts = makevalueopts(d; dateformat, decimal, truestrings, falsestrings, sentinels,
                         stripwhitespace, groupmark)
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1  # BOM
    sc = resolvescanner(d, fastindex, scanner)
    # A caller holding a prebuilt index (the Scan integration indexes once for
    # header binding and reuses it across the filter's two parse phases) hands
    # it in; the chunk geometry and dialect must match how it was built.
    # Single-assignment discipline for everything a spawned task or closure
    # captures (`allchunks`, `indexed`, `chunks`, `rowbases0`, the per-branch
    # `final*`): a captured variable assigned twice is boxed (`Core.Box`),
    # which is both the shared-mutable-capture hazard `\$x` interpolation exists
    # for and a type-instability inside every task body that reads it. The
    # test suite lowers this method and asserts no boxes remain.
    allchunks::Vector{ChunkIndex} = index === nothing ?
        chunkplan(buf, d, datastart, chunkbytes, parallel) : index.chunks
    indexed = fill(index !== nothing, length(allchunks))
    indexunclosed = index !== nothing && index.unclosedquote
    nchall = length(allchunks)
    headerlog = ProblemLog(maxproblems)

    # -- index wave -----------------------------------------------------------
    # Index EVERY chunk up front. Row counts and bases are then known before
    # any value work, which is what lets the unmasked driver write parsed
    # values straight into exact-size final columns — no per-chunk staging, no
    # stitch copies, no transient 2×-file-size allocation churn. The index pass
    # is a streaming scan (multi-GiB/s per core); giving up the fused
    # index-then-parse cache warmth on ONE column costs less than the copies.
    toindex = [k for k in 1:nchall if !indexed[k]]
    if parallel && length(toindex) > 1
        @sync for k in toindex
            errormonitor(Threads.@spawn begin
                indexone!(allchunks[k], buf, d, sc)
                indexed[k] = true
            end)
        end
    else
        for k in toindex
            indexone!(allchunks[k], buf, d, sc)
            indexed[k] = true
        end
    end
    # the header lives in the first chunk that still has rows after comment/empty
    # hygiene
    headerchunk = something(findfirst(k -> totalrows(allchunks[k]) > 0, 1:nchall), 0)

    # -- header & column names ------------------------------------------------
    local names::Vector{Symbol}
    if header === true && headerchunk > 0
        ci = allchunks[headerchunk]
        names = parseheader!(buf, ci, opts, d, headerlog)
    elseif header isa AbstractVector
        names = Symbol.(header)
    else
        ncg = headerchunk == 0 ? 0 :
              nfields(allchunks[headerchunk], allchunks[headerchunk].firstdatarow)
        names = [Symbol("Column", j) for j in 1:ncg]
    end
    names = makeunique!(names)
    ncols = length(names)
    fullrows = sum(nrows, allchunks; init=0)

    # -- selection & row geometry ----------------------------------------------
    selected = resolveselect(select, names, ncols)
    # every chunk is indexed: global row bases are simply known
    rowbasesall = cumsum([0; Int[nrows(ci) for ci in allchunks[1:max(nchall - 1, 0)]]])
    if rowmask !== nothing
        length(rowmask) == fullrows ||
            throw(ArgumentError("rowmask length $(length(rowmask)) != $fullrows data rows"))
    end
    # Keep whole chunks up to the limit boundary. `sampletypes(maxrows=limit)`
    # restricts inference to the retained prefix of the boundary chunk. The
    # working `chunks`/`rowbases0`/`nch` bind exactly once, here.
    nch = limit === nothing ? nchall : _limitchunks(allchunks, rowbasesall, limit)
    chunks = nch == nchall ? allchunks : allchunks[1:nch]
    rowbases0 = nch == nchall ? rowbasesall : rowbasesall[1:nch]

    # -- type seeding (stratified over the probe chunks) -----------------------
    seed = resolvetypes(types, names, ncols; validate)
    userprovided = [T !== nothing for T in seed]
    wantmissing = declaredmissing(types, names, ncols; validate)   # user wrote Union{Missing,T}
    # typed columns whose sample shows a missing cell get union-direct finals
    # (the parse writes Vector{Union{T,Missing}} in place; conversion is never
    # paid). Sample-missed sparse missings fall back to a finalize conversion.
    sawmissing = copy(wantmissing)   # declared Union{Missing,T} ⇒ union finals
    # Validate before sampling: `_copts` is deliberately inbounds in hot loops.
    colopts === nothing || length(colopts) == ncols ||
        throw(ArgumentError("colopts length $(length(colopts)) != $ncols columns"))
    if any(j -> seed[j] === nothing && (selected === nothing || selected[j]), 1:ncols)
        if rowmask === nothing
            probechunks = ChunkIndex[ci for ci in chunks if nrows(ci) > 0]
            probetotal = sum(nrows, probechunks; init=0)
            ns = nsample === nothing ? clamp(probetotal >> 6, 8, 128) : nsample
            inferred = sampletypes(buf, probechunks, ncols, opts; nsample=max(ns, 1), selected,
                                   sawmissing, colopts, maxrows=limit)
        else
            # inference reflects the rows that will actually be output: a
            # masked-out malformed value must not promote a qualifying column
            qrows = findall(rowmask)
            ns = nsample === nothing ? clamp(length(qrows) >> 6, 8, 128) : nsample
            inferred = sampletypesrows(buf, chunks, rowbases0, qrows, ncols, opts, selected;
                                       nsample=max(ns, 1), sawmissing, colopts)
        end
        for j in 1:ncols
            seed[j] === nothing && (seed[j] = _maptype(tm, inferred[j]))
        end
    end
    if selected !== nothing
        # unselected columns are never parsed; give unseeded ones a placeholder
        for j in 1:ncols
            !selected[j] && seed[j] === nothing && (seed[j] = Missing)
        end
    end

    # -- value wave ------------------------------------------------------------
    # Chunks are already indexed. Each chunk task reports its ragged rows with
    # chunk-local ids into a task-local log (folded once into the bounded
    # reservoir), parses every selected column, and promotes through the shared
    # `promo` register with an immediate hot re-parse on conflict. The unmasked
    # driver writes final columns directly; the masked driver stages and
    # stitches compactly.
    promo = Type[T for T in seed]
    promolock = ReentrantLock()
    # parse-time pooling: chunk tasks intern String cells as they parse. The
    # cap bounds chunk-local levels (exceeding it locally proves the column
    # exceeds it globally); the ratio×rows bound is enforced at the merge,
    # where ndata is exact.
    # per-column pool overrides: an entry of nothing means that column never
    # pools (its skip flag starts true); a (ratio, cap) entry overrides the
    # global policy for that column
    poolspecs === nothing || length(poolspecs) == ncols ||
        throw(ArgumentError("poolspecs length $(length(poolspecs)) != $ncols columns"))
    poolactive = poolspec !== nothing ||
                 (poolspecs !== nothing && any(sp -> sp !== nothing, poolspecs))
    poolctx = !poolactive ? nothing :
              PoolCtx(Int[(sp = _colspec(poolspecs, poolspec, j);
                           sp === nothing ? 0 : min(sp[2], Int(typemax(UInt32))))
                          for j in 1:ncols],
                      [Threads.Atomic{Bool}(false) for _ in 1:ncols],
                      Bool[_colspec(poolspecs, poolspec, j) === nothing for j in 1:ncols])
    segments = Vector{Vector{Any}}(undef, nch)
    segtypes = Vector{Vector{Type}}(undef, nch)
    pendingproblems = PendingProblemLog(maxproblems)
    mergeproblems!(pendingproblems, headerlog, 0)
    chunkrows = Int[nrows(ci) for ci in chunks]
    if limit !== nothing && nch > 0
        # only the retained prefix of the boundary chunk is written/reported
        chunkrows[end] = min(chunkrows[end], limit - rowbases0[end])
    end
    rowbases = cumsum([0; chunkrows[1:max(nch - 1, 0)]])
    ndata = rowmask === nothing ? sum(chunkrows; init=0) : count(rowmask)
    # Pooling pre-skip: more than `maxlevels` semantically distinct sampled cells
    # PROVES that the binding policy must fail. The proof is capped at the 500
    # levels used by the default front door; custom larger bounds use the normal
    # parse-time decision instead of making the sampling cost unbounded. Start
    # with 128 draws. A String-seeded column whose probe is all-distinct gets an
    # exact follow-up of up to twice the bound plus one rows; categorical columns
    # stop at their first sampled repeat. Tight bounds can still be proven before
    # a non-String seed promotes. Require at least 32 present cells. Skipped
    # columns take the plain direct String path: no segments, no interning, no
    # degrade, no stitch.
    if poolctx !== nothing
        colmax = Int[(sp = _colspec(poolspecs, poolspec, j);
                      sp === nothing ? typemax(Int) :
                          min(sp[1] == 1.0 ? ndata : floor(Int, sp[1] * ndata),
                              sp[2], Int(typemax(UInt32))))
                     for j in 1:ncols]
        maxlevels = minimum(j -> poolctx.skip[j] ? typemax(Int) : colmax[j], 1:ncols;
                            init=typemax(Int))
        if maxlevels < ndata && maxlevels <= 500
            proofsample = max(128, 2 * maxlevels + 1)
            active = [selected === nothing || selected[j] for j in 1:ncols]
            qrows = rowmask === nothing ? nothing : findall(rowmask)
            srh = rowmask === nothing ?
                  samplerepeathashes(buf, chunks, ncols, opts, active;
                                     sampletotal=ndata) :
                  samplerepeathashes(buf, chunks, ncols, opts, active,
                                     qrows, rowbases0)
            if proofsample > 128
                longmask = fill(false, ncols)
                for j in 1:ncols
                    h = srh[j]
                    longmask[j] = active[j] && promo[j] === String &&
                                  !poolctx.skip[j] && colmax[j] <= 500 &&
                                  length(h) >= 32 && allunique(h)
                end
                if any(longmask)
                    longhashes = rowmask === nothing ?
                                 samplerepeathashes(buf, chunks, ncols, opts, longmask;
                                                    nsample=proofsample,
                                                    sampletotal=ndata) :
                                 samplerepeathashes(buf, chunks, ncols, opts, longmask,
                                                    qrows, rowbases0;
                                                    nsample=proofsample)
                    for j in 1:ncols
                        longmask[j] && (srh[j] = longhashes[j])
                    end
                end
            end
            for j in 1:ncols
                selected === nothing || selected[j] || continue
                poolctx.skip[j] && continue
                mlj = colmax[j]
                mlj <= 500 || continue
                h = srh[j]
                length(h) >= 32 && length(h) > mlj &&
                    _morethanunique(h, mlj) &&
                    (poolctx.skip[j] = true)
            end
        end
    end
    cols = Vector{AbstractVector}(undef, ncols)
    stitchjs = selected === nothing ? collect(1:ncols) : findall(selected)
    mb = k -> rowmask === nothing ? 0 : rowbases0[k]
    rl = k -> limit === nothing ? typemax(Int) :
              clamp(limit - rowbases0[k], 0, nrows(chunks[k]))

    if rowmask === nothing
        # -- direct wave: parse straight into exact-size final columns --------
        # Row bases are known (everything is indexed), so every chunk writes
        # its values at its global offsets — no per-chunk staging and no stitch
        # copies. Pooled String columns are the one exception: they keep
        # chunk-local interning staging (levels must merge in chunk order) and
        # flow through the existing pooled merge below.
        finaldirect = directwave!(cols, chunks, buf, d, opts, ncols, userprovided, promo,
                                  promolock, pendingproblems, segments, segtypes, selected,
                                  rowbases, ndata, rl, reportstructural, parallel, poolctx,
                                  sawmissing, tm, colopts)
        for k in 1:(nch - 1)
            chunks[k].unclosedquote &&
                error("internal error: chunk $(k) ended inside a quoted field despite parity pre-scan")
        end
        # pooled columns merge chunk staging exactly as before
        pooljs = [j for j in stitchjs
                  if finaldirect[j] === String && poolctx !== nothing && !poolctx.skip[j]]
        pstitch = j -> (cols[j] = stitchcolumn(String, segments, segtypes, j, chunkrows,
                                               rowbases, ndata, buf, opts.e, d.cq,
                                               _colspec(poolspecs, poolspec, j),
                                               nothing, rowbases0))
        if parallel && length(pooljs) > 1
            @sync for j in pooljs
                errormonitor(Threads.@spawn pstitch(j))
            end
        else
            foreach(pstitch, pooljs)
        end
    else
        # -- masked wave: chunk-local staging + compacting stitch --------------
        # (the two-phase filter path; excluded rows never parse, output
        # positions gather compactly)
        if parallel && nch > 1
            @sync for k in 1:nch
                errormonitor(Threads.@spawn fusedchunk!(chunks[k], buf, d, ncols, opts,
                                                        userprovided, promo, promolock,
                                                        pendingproblems, segments, segtypes, k,
                                                        selected, rowmask, mb(k), rl(k),
                                                        reportstructural, poolctx, tm, colopts))
            end
        else
            for k in 1:nch
                fusedchunk!(chunks[k], buf, d, ncols, opts, userprovided,
                            promo, promolock, pendingproblems, segments, segtypes, k,
                            selected, rowmask, mb(k), rl(k), reportstructural, poolctx,
                            tm, colopts)
            end
        end
        for k in 1:(nch - 1)
            chunks[k].unclosedquote &&
                error("internal error: chunk $(k) ended inside a quoted field despite parity pre-scan")
        end
        # unify: re-parse the (rare) segments parsed under a stale type.
        # `promo` is frozen now; a Missing segment upgrades without work.
        finalstaged = Type[promo[j] for j in 1:ncols]
        stale = Tuple{Int, Int}[]
        for k in 1:nch, j in 1:ncols
            T = segtypes[k][j]
            T !== finalstaged[j] && T !== Missing && push!(stale, (k, j))
        end
        if !isempty(stale)
            if parallel && length(stale) > 1
                @sync for (k, j) in stale
                    errormonitor(Threads.@spawn restale!(chunks, finalstaged, segments, segtypes,
                                                         pendingproblems, buf, opts, d,
                                                         userprovided, k, j, rowmask, mb(k), rl(k),
                                                         poolctx, colopts))
                end
            else
                for (k, j) in stale
                    restale!(chunks, finalstaged, segments, segtypes, pendingproblems, buf,
                             opts, d, userprovided, k, j, rowmask, mb(k), rl(k), poolctx,
                             colopts)
                end
            end
        end
        stitchcol = j -> (cols[j] = stitchcolumn(finalstaged[j], segments, segtypes, j, chunkrows,
                                                 rowbases, ndata, buf, opts.e, d.cq,
                                                 poolctx !== nothing && poolctx.skip[j] ?
                                                     nothing : _colspec(poolspecs, poolspec, j),
                                                 rowmask, rowbases0))
        # single-chunk stitches are zero-copy finalizes — never worth a task spawn
        if parallel && length(stitchjs) > 1 && ndata > 0 && length(chunks) > 1
            @sync for j in stitchjs
                errormonitor(Threads.@spawn stitchcol(j))
            end
        else
            foreach(stitchcol, stitchjs)
        end
    end

    # -- problems: rebase chunk-local rows, merge, deterministic cap -----------
    # problem rows always reference INPUT data-row numbers (diagnostics point
    # at the file, not at the filtered output)
    log = finishproblems(pendingproblems, rowmask === nothing ? rowbases : rowbases0)
    hasunclosed = indexunclosed || (nch > 0 && last(chunks).unclosedquote)
    unclosedincluded = rowmask === nothing || fullrows == 0 || rowmask[end]
    if reportstructural && hasunclosed && unclosedincluded &&
       (limit === nothing || limit >= fullrows)
        pushproblem!(log, 0, 0, length(buf), :unclosed_quote,
                     "input ended inside a quoted field")
    end

    # -- finalize --------------------------------------------------------------
    sortproblems!(log)
    if on_error === :error && log.first !== nothing
        p = log.first
        nproblems = length(log.items) + log.dropped
        throw(ErrorException("CSVKernel: $(p.kind) at data row $(p.row), column $(p.col): $(p.message)" *
                             (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
    # a user-declared Union{Missing,T} is the column type even without missings
    for j in stitchjs
        wantmissing[j] || continue
        c = cols[j]
        Missing <: eltype(c) && continue
        cols[j] = _widenmissing(c)
    end
    selected === nothing && return ParsedTable(names, cols, ndata, log.items, log.dropped)
    return ParsedTable(names[stitchjs], cols[stitchjs], ndata, log.items, log.dropped)
end

parse(str::AbstractString; kw...) = parse(Vector{UInt8}(codeunits(str)); kw...)
parse(io::IO; kw...) = parse(read(io); kw...)

chunkrowbase(chunks::Vector{ChunkIndex}, target::ChunkIndex) =
    sum(nrows(c) for c in chunks if c.start < target.start; init=0)

# One masked-driver task: report ragged rows with chunk-local row ids and parse
# every selected column into chunk-local segment storage. All chunks are indexed
# by the unconditional index wave before this function can run.
function fusedchunk!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, ncols::Int,
                     opts::ValueOpts,
                     userprovided, promo, promolock, pendingproblems::PendingProblemLog,
                     segments, segtypes, k::Int,
                     selected::Union{Nothing, Vector{Bool}}=nothing,
                     mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                     reportlimit::Int=typemax(Int), reportstructural::Bool=true,
                     poolctx::Union{Nothing, PoolCtx}=nothing, tm=nothing, colopts=nothing)
    n = nrows(ci)
    log = ProblemLog(pendingproblems.limit)
    if reportstructural
        for lr in ci.firstdatarow:totalrows(ci)
            localrow = lr - ci.firstdatarow + 1
            mask !== nothing && !mask[maskbase + localrow] && continue
            localrow > reportlimit && continue
            nf = nfields(ci, lr)
            if nf < ncols
                sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :short_row,
                             "expected $ncols fields, found $nf (remaining columns set to missing)")
            elseif nf > ncols
                sp = fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :long_row,
                             "expected $ncols fields, found $nf (extra fields ignored)")
            end
        end
    end
    segs = Vector{Any}(undef, ncols)
    st = Vector{Type}(undef, ncols)
    for j in 1:ncols
        if selected !== nothing && !selected[j]
            # unselected columns simply don't exist to the value layer
            segs[j] = nothing
            st[j] = Missing
            continue
        end
        T = lock(() -> promo[j], promolock)
        attempts = 0
        while true
            (attempts += 1) > 8 && error("internal error: promotion did not converge")
            stg = T === String && poolctx !== nothing && !poolctx.skip[j] ?
                  PoolSegment(n, buf, opts.e, d.cq, poolctx.maxlevels[j], poolctx.aborted[j]) :
                  allocatecolumn(T, n, buf, opts.e, d.cq)
            conflict = T === Missing ?
                parsecolchunk_missing(buf, ci, j, 0, _copts(colopts, opts, j),
                                      userprovided[j], log, mask,
                                      maskbase, reportlimit) :
                parsecolchunk!(stg, buf, ci, j, 0, _copts(colopts, opts, j),
                               userprovided[j], log, 0, mask,
                               maskbase, reportlimit)
            if conflict == 0
                segs[j] = stg
                st[j] = T
                break
            end
            sp = fieldspan(ci, conflict, j)::Tuple{Int, Int}
            newT = promote_kernel(T, detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j)))
            newT = newT === T ? String : newT  # a conflicting value must move the type
            T = lock(promolock) do
                promo[j] = _promotemapped(tm, promo[j], newT)
            end
        end
    end
    segments[k] = segs
    segtypes[k] = st
    mergeproblems!(pendingproblems, log, k)
    return
end

# Re-parse one (chunk, column) segment under the final joined type. A top-level
# function on purpose: an earlier version was a closure inside `parse` whose
# `ci = chunks[k]` assignment REBOUND the enclosing function's boxed `ci`
# variable, silently shared across every concurrent task — the textbook Julia
# closure-capture race. Kernel rule: task bodies are named functions.
function restale!(chunks, final, segments, segtypes,
                  pendingproblems::PendingProblemLog, buf::Vector{UInt8},
                  opts::ValueOpts, d::Dialect, userprovided, k::Int, j::Int,
                  mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                  reportlimit::Int=typemax(Int),
                  poolctx::Union{Nothing, PoolCtx}=nothing, colopts=nothing)
    ci = chunks[k]
    stg = final[j] === String && poolctx !== nothing && !poolctx.skip[j] ?
          PoolSegment(nrows(ci), buf, opts.e, d.cq, poolctx.maxlevels[j], poolctx.aborted[j]) :
          allocatecolumn(final[j], nrows(ci), buf, opts.e, d.cq)
    log = ProblemLog(pendingproblems.limit)
    conflict = final[j] === Missing ? 0 :
        parsecolchunk!(stg, buf, ci, j, 0, _copts(colopts, opts, j), userprovided[j],
                       log, 0, mask, maskbase, reportlimit)
    conflict == 0 || error("internal error: re-parse under the joined type conflicted")
    segments[k][j] = stg
    segtypes[k][j] = final[j]
    mergeproblems!(pendingproblems, log, k)
    return
end

# --- the direct wave ---------------------------------------------------------
#
# The unmasked driver: every chunk writes its parsed values straight into
# exact-size final columns at its global row base (the parse loops always
# supported an offset `rowbase`; the staged driver simply passed 0). What this
# removes: per-(column × chunk) staging allocation (~2× the file size of
# transient churn per parse), the stitch's copy pass, and the GC pressure both
# fed. What it costs: on the rare promotion, completed chunks re-parse the
# column instead of stitch-time converting — promotions are what stratified
# sampling exists to make rare. Pooled String columns are the one carve-out:
# they keep chunk-local interning staging (levels must merge in chunk order)
# and the existing merge; only their `segments` slots are populated here.

# Direct finals allocate UNDEF: each chunk task fills its own slice right
# before parsing it (one page touch, in the task that writes it, parallel at
# chunk granularity instead of column granularity). The rewave fills the
# slices of promoted finals — including fill-only for chunks whose Missing
# parse upgrades for free.
function _allocdirect(::Type{T}, ndata::Int, buf::Vector{UInt8}, opts::ValueOpts,
                      d::Dialect, poolctx, j::Int, wantunion::Bool=false) where {T}
    T === Missing && return nothing
    T === String && poolctx !== nothing && !poolctx.skip[j] &&
        return nothing   # pooled: chunk staging
    T === String && return StringColumn(Vector{CompactStringPayload}(undef, ndata), buf,
                                        UInt8[], ReentrantLock(), opts.e, d.cq)
    wantunion && return UnionColumn{T}(ndata)
    return TypedColumn{T}(Vector{T}(undef, ndata), Vector{Bool}(undef, ndata))
end

# indexed @simd loops, not fill!(view(...)): the SubArray fill does not lower
# to a memset-class loop, and the missing-dense shapes (most rows per byte,
# most fill work per input byte) measurably paid for it
function _fillslice!(col::StringColumn, lo::Int, hi::Int)
    payloads = col.payloads
    @inbounds @simd for r in lo:hi
        payloads[r] = PAYLOAD_MISSING
    end
    return nothing
end
function _fillslice!(col::TypedColumn, lo::Int, hi::Int)
    present = col.present
    @inbounds @simd for r in lo:hi
        present[r] = false
    end
    return nothing
end
function _fillslice!(col::UnionColumn, lo::Int, hi::Int)
    uvalues = col.uvalues
    @inbounds for r in lo:hi
        uvalues[r] = missing
    end
    return nothing
end

function directwave!(cols, chunks, buf::Vector{UInt8}, d::Dialect, opts::ValueOpts,
                     ncols::Int, userprovided, promo, promolock,
                     pendingproblems::PendingProblemLog, segments, segtypes,
                     selected::Union{Nothing, Vector{Bool}},
                     rowbases::Vector{Int}, ndata::Int, rl,
                     reportstructural::Bool, parallel::Bool, poolctx,
                     unioncols::Vector{Bool}=fill(false, ncols), tm=nothing, colopts=nothing)
    nch = length(chunks)
    finals = Vector{Any}(nothing, ncols)
    allocjs = [j for j in 1:ncols if selected === nothing || selected[j]]
    # allocate per column in parallel: a Vector{Union{T,Missing}} final zero-
    # initializes its selector bytes at allocation, which is a serial memset
    # per union column if done on one task — measured +17-21% at 8T on
    # missing-heavy shapes before this went parallel
    if parallel && length(allocjs) > 1 && ndata > (1 << 16)
        @sync for j in allocjs
            errormonitor(Threads.@spawn begin
                finals[j] = _allocdirect(promo[j], ndata, buf, opts, d, poolctx, j,
                                         unioncols[j])
            end)
        end
    else
        for j in allocjs
            finals[j] = _allocdirect(promo[j], ndata, buf, opts, d, poolctx, j, unioncols[j])
        end
    end
    if parallel && nch > 1
        @sync for k in 1:nch
            errormonitor(Threads.@spawn directchunk!(chunks[k], buf, d, opts, ncols,
                                                     userprovided, promo, promolock, finals,
                                                     pendingproblems, segments, segtypes, k,
                                                     selected, rowbases[k], rl(k), ndata,
                                                     reportstructural, poolctx, unioncols,
                                                     tm, colopts))
        end
    else
        for k in 1:nch
            directchunk!(chunks[k], buf, d, opts, ncols, userprovided, promo, promolock,
                         finals, pendingproblems, segments, segtypes, k, selected,
                         rowbases[k], rl(k), ndata, reportstructural, poolctx, unioncols,
                         tm, colopts)
        end
    end

    # fold the chunks' private escaped-string extras into each final column, in
    # chunk order (before the rewave, so stale re-parses append consistently)
    final = Type[promo[j] for j in 1:ncols]
    for j in allocjs
        final[j] === String || continue
        poolctx !== nothing && !poolctx.skip[j] && continue
        scol = finals[j]
        scol isa StringColumn || continue
        payloads = scol.payloads
        ks = [k for k in 1:nch if segments[k][j] isa StringColumn &&
                                  !isempty((segments[k][j]::StringColumn).extra)]
        isempty(ks) && continue
        # reserve every chunk's region serially (bases are order-dependent),
        # then copy bytes and rebase each chunk's rows in parallel — regions
        # and row ranges are disjoint
        base0 = Int64(length(scol.extra))
        bases = Vector{Int64}(undef, length(ks))
        total = Int64(0)
        for (x, k) in enumerate(ks)
            bases[x] = base0 + total
            total += length((segments[k][j]::StringColumn).extra)
        end
        resize!(scol.extra, base0 + total)
        rebaseone = x -> begin
            k = ks[x]
            seg = segments[k][j]::StringColumn
            base = bases[x]
            copyto!(scol.extra, base + 1, seg.extra, 1, length(seg.extra))
            rhi = k < nch ? rowbases[k + 1] : ndata
            @inbounds for r in (rowbases[k] + 1):rhi
                pl = payloads[r]
                if cslen(pl) > COMPACTSTRING_INLINE && csoff(pl) < 0
                    payloads[r] = CompactStringPayload(pl.a, reinterpret(UInt64, csoff(pl) - base))
                end
            end
            segments[k][j] = nothing
        end
        if parallel && length(ks) > 1
            @sync for x in eachindex(ks)
                errormonitor(Threads.@spawn rebaseone(x))
            end
        else
            foreach(rebaseone, eachindex(ks))
        end
    end

    # promo is frozen: chunks that wrote under a stale type re-parse against the
    # final column. A Missing-parsed chunk upgrades for free (its rows are
    # already absent in the final); a stale chunk under a pooled final restales
    # into pooled staging for the merge.
    stale = Tuple{Int, Int}[]
    for k in 1:nch, j in allocjs
        T = segtypes[k][j]
        T === final[j] && continue
        if T === Missing
            # the free Missing upgrade still needs the promoted final's UNDEF
            # slice filled with the missing pattern
            (final[j] === Missing ||
             (final[j] === String && poolctx !== nothing && !poolctx.skip[j])) && continue
        end
        push!(stale, (k, j))
    end
    if !isempty(stale)
        redo = (k, j) -> begin
            if segtypes[k][j] === Missing
                lo = rowbases[k] + 1
                hi = rowbases[k] + min(nrows(chunks[k]), rl(k))
                hi >= lo && _fillslice!(finals[j], lo, hi)
            elseif final[j] === String && poolctx !== nothing && !poolctx.skip[j]
                restale!(chunks, final, segments, segtypes, pendingproblems, buf,
                         opts, d, userprovided, k, j, nothing, 0, rl(k), poolctx, colopts)
            else
                redirect!(chunks, final, finals, segtypes, pendingproblems, buf,
                          opts, userprovided, k, j, rowbases[k], rl(k), colopts)
            end
        end
        if parallel && length(stale) > 1
            @sync for (k, j) in stale
                errormonitor(Threads.@spawn redo(k, j))
            end
        else
            for (k, j) in stale
                redo(k, j)
            end
        end
    end

    # finalize the direct columns in place (pooled ones merge in the caller);
    # the presence scans are per-column independent — spread them
    finalizeone = j -> begin
        T = final[j]
        cols[j] = T === Missing ? fill(missing, ndata) :
                  T === String ? finalizecolumn(String, finals[j]::StringColumn, ndata) :
                  finalizecolumn(T, finals[j]::Union{TypedColumn{T}, UnionColumn{T}}, ndata)
    end
    finjs = [j for j in allocjs
             if !(final[j] === String && poolctx !== nothing && !poolctx.skip[j])]
    if parallel && length(finjs) > 1 && ndata > (1 << 18)
        @sync for j in finjs
            errormonitor(Threads.@spawn finalizeone(j))
        end
    else
        foreach(finalizeone, finjs)
    end
    return final
end

function directchunk!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, opts::ValueOpts,
                      ncols::Int, userprovided, promo, promolock, finals,
                      pendingproblems::PendingProblemLog, segments, segtypes, k::Int,
                      selected::Union{Nothing, Vector{Bool}}, rowbase::Int,
                      reportlimit::Int, ndata::Int, reportstructural::Bool, poolctx,
                      unioncols::Vector{Bool}=fill(false, ncols), tm=nothing, colopts=nothing)
    n = nrows(ci)
    log = ProblemLog(pendingproblems.limit)
    if reportstructural
        for lr in ci.firstdatarow:totalrows(ci)
            localrow = lr - ci.firstdatarow + 1
            localrow > reportlimit && continue
            nf = nfields(ci, lr)
            if nf < ncols
                sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :short_row,
                             "expected $ncols fields, found $nf (remaining columns set to missing)")
            elseif nf > ncols
                sp = fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :long_row,
                             "expected $ncols fields, found $nf (extra fields ignored)")
            end
        end
    end
    segs = Vector{Any}(undef, ncols)
    st = Vector{Type}(undef, ncols)
    for j in 1:ncols
        if selected !== nothing && !selected[j]
            segs[j] = nothing
            st[j] = Missing
            continue
        end
        T, dest = lock(() -> (promo[j], finals[j]), promolock)
        attempts = 0
        lo = rowbase + 1
        hi = rowbase + min(n, reportlimit)
        while true
            (attempts += 1) > 8 && error("internal error: promotion did not converge")
            local conflict::Int
            if T === Missing
                segs[j] = nothing
                conflict = parsecolchunk_missing(buf, ci, j, 0,
                                                 _copts(colopts, opts, j),
                                                 userprovided[j], log, nothing, 0,
                                                 reportlimit)
            elseif T === String && poolctx !== nothing && !poolctx.skip[j]
                ps = PoolSegment(n, buf, opts.e, d.cq, poolctx.maxlevels[j], poolctx.aborted[j])
                segs[j] = ps
                conflict = parsecolchunk!(ps, buf, ci, j, 0, _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
            elseif T === String
                # shared payloads, PRIVATE extra: escaped-cell flushes stay
                # uncontended, and the driver concatenates + rebases the (rare)
                # chunk extras in chunk order after the wave
                scol = dest::StringColumn
                hi >= lo && _fillslice!(scol, lo, hi)
                chunkcol = StringColumn(scol.payloads, buf, UInt8[], ReentrantLock(),
                                        scol.e, scol.cq)
                conflict = parsecolchunk!(chunkcol, buf, ci, j, rowbase,
                                          _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
                segs[j] = isempty(chunkcol.extra) ? nothing : chunkcol
            else
                segs[j] = nothing
                hi >= lo && _fillslice!(dest, lo, hi)
                conflict = parsecolchunk!(dest, buf, ci, j, rowbase, _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
            end
            if conflict == 0
                st[j] = T
                break
            end
            sp = fieldspan(ci, conflict, j)::Tuple{Int, Int}
            detected = promote_kernel(T, detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j)))
            # single assignment: promoT is captured by the lock closure below,
            # and a captured-and-reassigned local boxes (the staging war story)
            promoT = detected === T ? String : detected
            T, dest = lock(promolock) do
                joined = _promotemapped(tm, promo[j], promoT)
                if joined !== promo[j]
                    promo[j] = joined
                    finals[j] = _allocdirect(joined, ndata, buf, opts, d, poolctx, j,
                                             unioncols[j])
                end
                (promo[j], finals[j])
            end
        end
    end
    segments[k] = segs
    segtypes[k] = st
    mergeproblems!(pendingproblems, log, k)
    return
end

# re-parse one stale (chunk, column) straight into the final column
function redirect!(chunks, final, finals, segtypes,
                   pendingproblems::PendingProblemLog, buf::Vector{UInt8},
                   opts::ValueOpts, userprovided, k::Int, j::Int,
                   rowbase::Int, reportlimit::Int, colopts=nothing)
    ci = chunks[k]
    log = ProblemLog(pendingproblems.limit)
    hi = rowbase + min(nrows(ci), reportlimit)
    hi > rowbase && _fillslice!(finals[j], rowbase + 1, hi)
    conflict = parsecolchunk!(finals[j], buf, ci, j, rowbase, _copts(colopts, opts, j),
                              userprovided[j], log,
                              0, nothing, 0, reportlimit)
    conflict == 0 || error("internal error: re-parse under the joined type conflicted")
    segtypes[k][j] = final[j]
    mergeproblems!(pendingproblems, log, k)
    return
end

# --- pooled (dictionary-encoded) string columns --------------------------------
#
# Each chunk interns strings during parsing. The stitch merges those local level
# tables in chunk order, which preserves first-occurrence order. If the merged
# level count exceeds the policy, the caller degrades the staging and performs a
# flat string stitch without reparsing.
struct PooledColumn{ELT} <: AbstractVector{ELT}
    refs::Vector{UInt32}          # 0 = missing (ELT includes Missing then)
    levels::CompactStringVector{CompactString}
end
Base.size(c::PooledColumn) = size(c.refs)

# widen a missing-free column to its Union{Missing,T} counterpart, zero-copy
# where the container supports it (CompactString views / pooled refs), else a
# converted Base vector
_widenmissing(c::CompactStringVector{CompactString}) =
    CompactStringVector{Union{CompactString, Missing}}(c.payloads, c.buf, c.extra)
_widenmissing(c::PooledColumn{CompactString}) =
    PooledColumn{Union{CompactString, Missing}}(c.refs, c.levels)
_widenmissing(c::Vector{T}) where {T} = convert(Vector{Union{T, Missing}}, c)
_widenmissing(c::AbstractVector) = c

Base.@propagate_inbounds function Base.getindex(c::PooledColumn{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(c.refs, i)
    @inbounds r = c.refs[i]
    r == 0 && return missing
    return c.levels[Int(r)]
end
Base.@propagate_inbounds function Base.getindex(c::PooledColumn{CompactString}, i::Int)
    @boundscheck checkbounds(c.refs, i)
    @inbounds return c.levels[Int(c.refs[i])]
end
poolrefs(c::PooledColumn) = c.refs
poollevels(c::PooledColumn) = c.levels

# the effective (ratio, cap) for column j: per-column override, else global
@inline _colspec(poolspecs, poolspec, j::Int) =
    poolspecs === nothing ? poolspec :
    (@inbounds sp = poolspecs[j]; sp === nothing ? nothing : sp::Tuple{Float64, Int})

function _poolpolicy(pool)
    (pool === nothing || pool === false) && return nothing
    pool === true && return (1.0, typemax(Int))
    if pool isa Real
        0.0 <= pool <= 1.0 ||
            throw(ArgumentError("pool ratio must be in [0, 1] (got $pool)"))
        return (Float64(pool), typemax(Int))
    end
    pool isa Tuple{<:Real, <:Integer} ||
        throw(ArgumentError("pool spec must be Bool, Real, (Real, Integer), or nothing " *
                            "(got $(typeof(pool)))"))
    ratio = pool[1]
    0.0 <= ratio <= 1.0 ||
        throw(ArgumentError("pool ratio must be in [0, 1] (got $ratio)"))
    cap = Int(pool[2])
    cap >= 0 || throw(ArgumentError("pool cap must be nonnegative (got $cap)"))
    return (Float64(ratio), cap)
end

function materialize(c::PooledColumn{ELT}) where {ELT}
    lv = materialize(c.levels)
    out = Vector{ELT === CompactString ? String : Union{String, Missing}}(undef, length(c.refs))
    @inbounds for i in eachindex(c.refs)
        r = c.refs[i]
        out[i] = r == 0 ? missing : lv[Int(r)]
    end
    return out
end

# Build refs+levels from the per-chunk segments, or return nothing when the
# level count blows the policy bound (caller falls back to the flat stitch).
# Only each PoolSegment's levels hash here; refs remap in a flat integer pass.
# Level order is first occurrence in chunk order, which is file order.
function poolsegments(segments, j::Int, chunkrows, rowbases, ndata::Int,
                      buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                      pool::Tuple{Float64, Int},
                      mask::Union{Nothing, Vector{Bool}}=nothing, inbases=nothing)
    # both bounds hold: levels ≤ ratio×rows AND levels ≤ cap
    ratiolevels = pool[1] == 1.0 ? ndata : floor(Int, pool[1] * ndata)
    maxlevels = min(ratiolevels, pool[2], Int(typemax(UInt32)))
    # one chunk's abandon abandons the column (its cap ≤ global implies the
    # global bound already failed; a degraded segment carries the same signal)
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        seg isa PoolSegment && (seg.aborted[] || seg.degraded !== nothing) && return nothing
    end
    refs = zeros(UInt32, ndata)
    table = Dict{CompactString, UInt32}()
    levelpayloads = CompactStringPayload[]
    extra = UInt8[]
    npresent = 0
    dest = 0
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        if seg === nothing                       # all-missing segment
            if mask !== nothing
                @inbounds for i in 1:chunkrows[k]
                    mask[inbases[k] + i] && (dest += 1)
                end
            end
            continue
        end
        if seg isa PoolSegment
            # merge the chunk's level table, then remap refs without hashing
            remap = Vector{UInt32}(undef, length(seg.levelpayloads))
            for (l, p) in enumerate(seg.levelpayloads)
                len = Int(cslen(p))
                cell = len <= COMPACTSTRING_INLINE ? CompactString(p, EMPTY_BYTES) :
                       CompactString(p, csoff(p) < 0 ? seg.extra : buf)
                gref = get(table, cell, UInt32(0))
                if gref == 0
                    length(levelpayloads) >= maxlevels && return nothing
                    if len > COMPACTSTRING_INLINE && csoff(p) < 0
                        off = Int(-csoff(p))
                        base = length(extra)
                        append!(extra, @view seg.extra[off:off + len - 1])
                        p = view_payload(extra, base + 1, len, -(Int64(base) + 1))
                    end
                    push!(levelpayloads, p)
                    gref = UInt32(length(levelpayloads))
                    table[cell] = gref
                end
                remap[l] = gref
            end
            rb = rowbases[k]
            @inbounds for i in 1:chunkrows[k]
                if mask !== nothing
                    mask[inbases[k] + i] || continue
                end
                dest += 1
                r = seg.refs[i]
                r == 0 && continue               # missing ⇒ ref 0
                npresent += 1
                refs[mask === nothing ? rb + i : dest] = remap[Int(r)]
            end
            continue
        end
        # every String staging under pooling is a PoolSegment (parse-time
        # interning; the masked driver and restale allocate them too) — plain
        # StringColumn staging reaching a pooled merge is a driver bug
        error("internal error: pooled merge expects PoolSegment staging, got " *
              string(typeof(seg)))
    end
    levels = CompactStringVector{CompactString}(levelpayloads, buf, extra)
    return npresent == ndata ? PooledColumn{CompactString}(refs, levels) :
                               PooledColumn{Union{CompactString, Missing}}(refs, levels)
end

# Assemble one final exact-size column from its per-chunk segments. Segment
# copies are plain value memmoves (cheap relative to re-reading text from RAM);
# a Missing segment under a wider final type contributes all-absent rows with no
# re-parse. String segments concatenate their extra buffers, rebasing the
# negative (extra-relative) offsets as they copy.
function stitchcolumn(::Type{T}, segments, segtypes, j::Int, chunkrows, rowbases,
                      ndata::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                      pool::Union{Nothing, Tuple{Float64, Int}}=nothing,
                      mask::Union{Nothing, Vector{Bool}}=nothing, inbases=nothing) where {T}
    T === Missing && return fill(missing, ndata)
    if T === String && pool !== nothing
        if ndata > 0
            pooled = poolsegments(segments, j, chunkrows, rowbases, ndata, buf, e, cq, pool,
                                  mask, inbases)
            pooled !== nothing && return pooled
        end
        # bound exceeded (or empty): flatten any parse-time staging so the
        # plain paths below see ordinary StringColumns
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            seg isa PoolSegment &&
                (segments[k][j] = seg.degraded !== nothing ? seg.degraded : _degradepool!(seg))
        end
    end
    mask === nothing || return _stitchmasked(T, segments, j, chunkrows, ndata, buf, e, cq,
                                             mask, inbases)
    # Single-chunk files (every input below chunkbytes): the lone segment IS the
    # final column — finalize it directly, zero copies. This keeps the fused
    # driver's small-file cost identical to writing final columns in place.
    if length(chunkrows) == 1
        seg = segments[1][j]
        seg === nothing && return fill(missing, ndata)
        # a limit-clipped boundary segment is larger than the output; only the
        # untouched case may alias the staging directly
        if (seg isa StringColumn ? length(seg.payloads) : length((seg::TypedColumn{T}).values)) == ndata
            return T === String ? finalizecolumn(String, seg::StringColumn, ndata) :
                                  finalizecolumn(T, seg::TypedColumn{T}, ndata)
        end
    end
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        extra = UInt8[]
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            seg === nothing && continue          # all-missing segment
            scol = seg::StringColumn
            rb = rowbases[k]
            if isempty(scol.extra)
                copyto!(payloads, rb + 1, scol.payloads, 1, chunkrows[k])
            else
                base = Int64(length(extra))
                append!(extra, scol.extra)
                @inbounds for i in 1:chunkrows[k]
                    p = scol.payloads[i]
                    if cslen(p) > COMPACTSTRING_INLINE && csoff(p) < 0
                        p = CompactStringPayload(p.a, reinterpret(UInt64, csoff(p) - base))
                    end
                    payloads[rb + i] = p
                    end
            end
        end
        return finalizecolumn(String, StringColumn(payloads, buf, extra, ReentrantLock(), e, cq), ndata)
    end
    values = Vector{T}(undef, ndata)
    present = fill(false, ndata)
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        seg === nothing && continue              # all-missing segment: stays absent
        tcol = seg::TypedColumn{T}
        rb = rowbases[k]
        copyto!(values, rb + 1, tcol.values, 1, chunkrows[k])
        copyto!(present, rb + 1, tcol.present, 1, chunkrows[k])
    end
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata)
end

# Row-filtered stitch: gather only mask-qualifying rows into compact output
# positions (chunk order, so output order is input order). Cells for excluded
# rows were never parsed; their staging slots are simply skipped here.
function _stitchmasked(::Type{T}, segments, j::Int, chunkrows, ndata::Int,
                       buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                       mask::Vector{Bool}, inbases) where {T}
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        extra = UInt8[]
        dest = 0
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            if seg === nothing
                @inbounds for i in 1:chunkrows[k]
                    mask[inbases[k] + i] && (dest += 1)
                end
                continue
            end
            scol = seg::StringColumn
            base = Int64(length(extra))
            isempty(scol.extra) || append!(extra, scol.extra)
            @inbounds for i in 1:chunkrows[k]
                mask[inbases[k] + i] || continue
                dest += 1
                p = scol.payloads[i]
                if cslen(p) > COMPACTSTRING_INLINE && csoff(p) < 0
                    p = CompactStringPayload(p.a, reinterpret(UInt64, csoff(p) - base))
                end
                payloads[dest] = p
            end
        end
        return finalizecolumn(String, StringColumn(payloads, buf, extra, ReentrantLock(), e, cq), ndata)
    end
    values = Vector{T}(undef, ndata)
    present = fill(false, ndata)
    dest = 0
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        if seg === nothing
            @inbounds for i in 1:chunkrows[k]
                mask[inbases[k] + i] && (dest += 1)
            end
            continue
        end
        tcol = seg::TypedColumn{T}
        @inbounds for i in 1:chunkrows[k]
            mask[inbases[k] + i] || continue
            dest += 1
            values[dest] = tcol.values[i]
            present[dest] = tcol.present[i]
        end
    end
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata)
end

function finalizecolumn(::Type{Missing}, ::Nothing, n::Int)
    return fill(missing, n)
end
finalizecolumn(::Type{Missing}, ::Nothing, n::Int, ::Bool) = fill(missing, n)
function finalizecolumn(::Type{String}, col::StringColumn, n::Int)
    anymissing = any(p -> cslen(p) < 0, col.payloads)
    return anymissing ? CompactStringVector{Union{CompactString, Missing}}(col.payloads, col.buf, col.extra) :
                        CompactStringVector{CompactString}(col.payloads, col.buf, col.extra)
end
function finalizecolumn(::Type{String}, col::StringColumn, n::Int, force_missing::Bool)
    anymissing = force_missing || any(p -> cslen(p) < 0, col.payloads)
    return anymissing ? CompactStringVector{Union{CompactString, Missing}}(col.payloads, col.buf, col.extra) :
                        CompactStringVector{CompactString}(col.payloads, col.buf, col.extra)
end
# `all(::Vector{Bool})` short-circuits, so it compiles to a branchy scalar
# loop — 1.2 ms per 4M-row column. `count` vectorizes; missing-free columns
# (the common case) full-scan either way, 8× faster here.
_allpresent(present::Vector{Bool}) = count(present) == length(present)
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int) where {T}
    # no missings ⇒ hand back the raw Vector{T}, zero copies
    return _allpresent(col.present) ? col.values : _tounion(col)
end
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int, force_missing::Bool) where {T}
    return !force_missing && _allpresent(col.present) ? col.values : _tounion(col)
end
# union-direct finals ARE the output — zero copies either way
finalizecolumn(::Type{T}, col::UnionColumn{T}, n::Int) where {T} = col.uvalues
finalizecolumn(::Type{T}, col::UnionColumn{T}, n::Int, ::Bool) where {T} = col.uvalues

# The sample-missed fallback: sparse missings the 128-row probe didn't see.
# Bitsunion stores have no memcpy path (measured 120-150% of a whole 20 MiB
# parse serially), so slice it across tasks. Named helper, not a closure: the
# captured-and-reassigned boxing war story.
function _tounionrange!(out, values, present, lo::Int, hi::Int)
    @inbounds for i in lo:hi
        out[i] = present[i] ? values[i] : missing
    end
    return
end
function _tounion(col::TypedColumn{T}) where {T}
    values, present = col.values, col.present
    n = length(values)
    out = Vector{Union{T, Missing}}(undef, n)
    nt = Threads.nthreads()
    if n > (1 << 17) && nt > 1
        parts = min(nt, 8)
        @sync for c in 1:parts
            lo = 1 + (c - 1) * n ÷ parts
            hi = c * n ÷ parts
            errormonitor(Threads.@spawn _tounionrange!(out, values, present, lo, hi))
        end
    else
        _tounionrange!(out, values, present, 1, n)
    end
    return out
end

"""
    materialize(col) -> Vector

Convert a kernel column into an ordinary `Vector` (`Vector{T}` or
`Vector{Union{T,Missing}}`), detaching it from the input buffer. String views
allocate real `String`s here — the choice between views and copies is the caller's,
made after parsing instead of before it (this replaces CSV.jl's up-front
`stringtype=` commitment). A column that is already a `Vector` is returned as-is.
"""
materialize(v::AbstractVector) = collect(v)
materialize(v::Vector) = v

# CSV.jl-compatible: a duplicate takes the smallest `name_k` not used by ANY
# name — original or already assigned — so `a,a,a_1` becomes `a,a_2,a_1`
# (renames never collide with an original that appears later).
function makeunique!(names::Vector{Symbol})
    taken = Set(names)
    seen = Set{Symbol}()
    for i in eachindex(names)
        nm = names[i]
        if nm in seen
            k = 1
            newnm = Symbol(nm, :_, k)
            while newnm in taken
                k += 1
                newnm = Symbol(nm, :_, k)
            end
            push!(taken, newnm)
            names[i] = newnm
            push!(seen, newnm)
        else
            push!(seen, nm)
        end
    end
    return names
end

end # module CSVKernel
