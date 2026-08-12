"""
    CSVKernel

A stand-alone prove-out of the proposed CSV.jl internals rewrite: a small, layered
kernel where *structure* is separated from *values*.

The pipeline (and the file's layout) is:

    L0  bytes         : a `Vector{UInt8}` covering the whole input (mmap/read/gunzip
                        live above this file; the kernel only sees bytes)
    L1  structural    : a quote-aware scan producing a `ChunkIndex` per row-aligned
        index           chunk — every field's (offset, length) plus row boundaries.
                        Two interchangeable scanners: a scalar reference state
                        machine (handles every dialect, doubles as the test oracle)
                        and a branchless SWAR fast path (8 bytes/iteration,
                        prefix-XOR quote masks — the Langdale/Lemire technique,
                        word-sized).
    L1' parallelism   : chunk entry quote-states are *computed*, not guessed:
                        quote-toggle parity is associative, so a parallel per-range
                        parity count + an exclusive XOR scan gives every range its
                        true entry state (the 2-state specialization of ParPaRaw's
                        FSM-composition; strictly stronger than DuckDB's
                        speculate-then-validate). Row starts follow deterministically.
    L2  schema        : type inference seeds from a *stratified* sample of the index
                        (evenly spaced rows, not a prefix), then...
    L3  values        : ...each column of each chunk is parsed in its own
                        *monomorphic* loop over the index (`Parsers.xparse` on exact
                        field spans). Type conflicts promote through a small lattice
                        and re-parse ONLY that column — never the whole chunk.
    L4  columns       : plain `Vector{T}` + `Vector{Bool}` presence (no sentinels),
                        string columns as lazy views into the input buffer
                        (unescaped on access), sized exactly (the index gives exact
                        row counts — no rowsguess, no reallocation).
    L5  driver        : `CSVKernel.parse` — eager parallel table materialization
                        with problems-as-data. `examples.jl` builds batched
                        (CSV.Chunks-like) and row-streaming (CSV.Rows-like) modes
                        on the same pieces.

What this kernel deliberately does NOT include (extensions documented in
kernel/README.md): dialect sniffing, pooled columns, InlineString widths,
`ignorerepeated`, transposed reading, multi-file, incremental IO sources, and the
writer. Each has a designed seam here; none requires re-architecting.

Semantics note (pinned by tests): the structural layer treats *every* quote byte as
toggling quote state, like Sep/simdcsv. This matches RFC-style well-formed fields. A
bare quote in the middle of an unquoted field therefore opens a quoted region.
CSV.jl's current value-level parser only honors quotes at field start; for such
malformed inputs the two designs can split rows differently. This tradeoff makes
quote state composable across parallel byte ranges.
"""
module CSVKernel

using Parsers, Dates

export Dialect, index, ParsedTable, Problem

# ---------------------------------------------------------------------------
# Dialect: the structural options. Value-level options (sentinels, dateformats,
# true/false spellings, decimal char) live in `Parsers.Options`, built once in
# `makeoptions` and applied to exact field spans — the kernel never re-implements
# value parsing.
# ---------------------------------------------------------------------------

struct Dialect
    delim::Union{UInt8, Vector{UInt8}}  # single byte fast path; multi-byte handled by the scalar scanner
    oq::UInt8                           # open quote
    cq::UInt8                           # close quote
    e::UInt8                            # escape char (== cq for RFC ""-doubling)
    quoted::Bool                        # false = no quote handling at all
    comment::Union{Nothing, Vector{UInt8}}  # rows beginning with these bytes are dropped
    ignoreemptyrows::Bool
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
                   ignoreemptyrows::Bool=true)
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
    return Dialect(d, oq, cq, e, quoted, cmt, ignoreemptyrows)
end

# Quote-toggle parity composes across arbitrary byte ranges only when a quote byte
# always means "toggle": true for unquoted dialects and for RFC ""-style escaping
# (an escaped quote is two toggles — parity-neutral). A distinct escape char (e.g.
# backslash) or asymmetric open/close quotes breaks parity, so those dialects run
# on the sequential scalar path.
parityclean(d::Dialect) = !d.quoted || (d.oq == d.cq && d.e == d.cq)

# The SWAR scanner additionally needs a single-byte delimiter.
swareligible(d::Dialect) = parityclean(d) && d.delim isa UInt8

function makeoptions(d::Dialect; dateformat=nothing, decimal::Char='.',
                     truestrings=nothing, falsestrings=nothing,
                     stripwhitespace::Bool=false)
    isascii(decimal) || throw(ArgumentError("decimal must be ASCII (got $(repr(decimal)))"))
    return Parsers.Options(
        sentinel=missing,                    # empty field ⇒ missing
        openquotechar=d.oq, closequotechar=d.cq, escapechar=d.e,
        delim=d.delim isa UInt8 ? d.delim : String(copy(d.delim)),
        quoted=d.quoted,
        dateformat=dateformat, decimal=decimal % UInt8,
        trues=truestrings, falses=falsestrings,
        stripwhitespace=stripwhitespace)
end

# ---------------------------------------------------------------------------
# L1: the structural index.
#
# A FieldSpan is the *raw* span of one field — everything between two structural
# delimiters, including any quotes, whitespace, or escapes. Value parsing hands the
# exact span to Parsers.xparse, which owns unquoting/unescaping/whitespace, so the
# index never needs to understand values. Offsets are chunk-relative UInt32 (chunks
# are bounded well below 4 GiB), keeping the index at 8 bytes/field.
# ---------------------------------------------------------------------------

struct FieldSpan
    relpos::UInt32   # 0-based offset of the field's first byte from the chunk start
    len::UInt32      # raw byte length (0 = empty field)
end

mutable struct ChunkIndex
    start::Int                  # absolute (1-based) byte offset of the chunk in buf
    stop::Int                   # absolute offset of the chunk's last byte
    fields::Vector{FieldSpan}
    rowfirst::Vector{Int32}     # rowfirst[r]..rowfirst[r+1]-1 index `fields` for row r; length nrows+1
    firstdatarow::Int           # local row where data begins (2 when this chunk holds the header row)
    unclosedquote::Bool         # buffer ended while inside a quoted field (malformed input)
end

ChunkIndex(start::Int, stop::Int) =
    ChunkIndex(start, stop, FieldSpan[], Int32[1], 1, false)

nrows(ci::ChunkIndex) = length(ci.rowfirst) - 1 - (ci.firstdatarow - 1)
totalrows(ci::ChunkIndex) = length(ci.rowfirst) - 1
nfields(ci::ChunkIndex, localrow::Int) = Int(ci.rowfirst[localrow + 1] - ci.rowfirst[localrow])

# Absolute (pos, len) of field `col` in local row `localrow`, or `nothing` when the
# row is too short (ragged input).
@inline function fieldspan(ci::ChunkIndex, localrow::Int, col::Int)
    @boundscheck 1 <= localrow <= totalrows(ci) || throw(BoundsError(ci, localrow))
    @boundscheck col >= 1 || throw(BoundsError(ci, (localrow, col)))
    @inbounds first = Int(ci.rowfirst[localrow])
    @inbounds nextr = Int(ci.rowfirst[localrow + 1])
    col <= nextr - first || return nothing
    fi = first + col - 1
    @inbounds s = ci.fields[fi]
    return (ci.start + Int(s.relpos), Int(s.len))
end

struct BufferIndex
    chunks::Vector{ChunkIndex}
    nrows::Int                  # total rows across chunks (header still included at this layer)
    unclosedquote::Bool         # input ended inside a quoted field (captured before empty-chunk filtering)
end

# --- shared row-emission hygiene -------------------------------------------
#
# Comment rows and (optionally) empty rows are dropped here — at row granularity,
# after structure is known — which is what lets the byte-level scanners stay
# oblivious to both concepts. This one helper replaces CSV.jl's five separate
# comment/empty-row-aware byte loops.
#
# The scanners emit fields into `ci.fields` and call `endrow!` after the row's last
# field has been pushed. `rowstartabs` is the absolute offset of the row's first byte.
@inline function endrow!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, rowstartabs::Int)
    firstfield = Int(ci.rowfirst[end])
    nf = length(ci.fields) - firstfield + 1
    # empty row: exactly one zero-length field
    if d.ignoreemptyrows && nf == 1 && @inbounds(ci.fields[end].len) == 0x00000000
        pop!(ci.fields)
        return
    end
    # comment row: raw bytes at the row start match the comment prefix. The
    # comparison can never leak past this row's terminator: comment bytes may not
    # contain \r or \n (validated in Dialect), so a terminator byte always
    # mismatches first. Bounded by length(buf) for the unterminated-last-row case.
    cmt = d.comment
    if cmt !== nothing && rowstartabs + length(cmt) - 1 <= length(buf)
        match = true
        @inbounds for k in eachindex(cmt)
            if buf[rowstartabs + k - 1] != cmt[k]
                match = false
                break
            end
        end
        if match
            resize!(ci.fields, firstfield - 1)
            return
        end
    end
    push!(ci.rowfirst, Int32(length(ci.fields) + 1))
    return
end

@inline emitfield!(ci::ChunkIndex, fieldstart::Int, fieldstop::Int) =
    push!(ci.fields, FieldSpan(UInt32(fieldstart - ci.start), UInt32(fieldstop - fieldstart + 1)))

# --- scalar reference scanner ----------------------------------------------
#
# A direct state machine over bytes. Handles every dialect (multi-byte delimiters,
# distinct escape chars, asymmetric quotes) and is the correctness oracle the SWAR
# path is property-tested against. Entry state is always "outside quotes" because
# chunk starts are true row starts by construction (§ parallel indexing below).

function indexchunk_scalar!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    start, stop = ci.start, ci.stop
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    delim = d.delim
    pos = start
    fieldstart = start
    rowstart = start
    inquote = false
    @inbounds while pos <= stop
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
            emitfield!(ci, fieldstart, pos - 1)
            pos += delim isa UInt8 ? 1 : length(delim)
            fieldstart = pos
        elseif b == LF
            emitfield!(ci, fieldstart, pos - 1)
            endrow!(ci, buf, d, rowstart)
            pos += 1
            fieldstart = rowstart = pos
        elseif b == CR
            emitfield!(ci, fieldstart, pos - 1)
            endrow!(ci, buf, d, rowstart)
            pos += 1
            pos <= stop && buf[pos] == LF && (pos += 1)
            fieldstart = rowstart = pos
        else
            pos += 1
        end
    end
    _finishchunk!(ci, buf, d, fieldstart, rowstart, inquote)
    return ci
end

@inline function _matchbytes(buf::Vector{UInt8}, pos::Int, bytes::Vector{UInt8})
    @inbounds for k in eachindex(bytes)
        buf[pos + k - 1] == bytes[k] || return false
    end
    return true
end

# Shared end-of-chunk logic: emit a trailing row when the chunk doesn't end in a
# row terminator ("a,b" with no final newline), including the trailing-empty-field
# case ("a,b," parses as three fields). An EOF while inside a quoted field is
# recorded as malformed rather than throwing — the driver reports it as a Problem.
function _finishchunk!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect,
                       fieldstart::Int, rowstart::Int, inquote::Bool)
    stop = ci.stop
    pendinginrow = length(ci.fields) - (Int(ci.rowfirst[end]) - 1)
    if fieldstart <= stop || pendinginrow > 0
        emitfield!(ci, fieldstart, stop)  # zero-length when fieldstart == stop + 1
        endrow!(ci, buf, d, rowstart)
    end
    ci.unclosedquote = inquote
    return
end

# --- SWAR scanner ------------------------------------------------------------
#
# Branchless structural classification 8 bytes at a time. Per 8-byte word we build
# 8-bit "movemask"s for quote/delim/CR/LF, compute the in-quote region mask with a
# prefix-XOR over the quote mask (carrying parity between words), null out specials
# inside quotes, pair CRLF, and only then fall back to per-event scalar work — which
# runs once per *field*, not per byte. This is the word-sized version of the
# Langdale–Lemire structural pass; a 64-byte SIMD.jl/CLMUL variant drops in behind
# the same emission helpers (documented in README).

const ONES8   = 0x0101010101010101
const HIGHS8  = 0x8080808080808080
const LOWS7   = 0x7f7f7f7f7f7f7f7f

# Exact per-byte equality mask, compressed to 8 bits (bit i ⇔ byte i == b).
# Uses the exact zero-byte test (no false positives, unlike the subtract-borrow
# trick) then the classic movemask multiply.
@inline function bytemask8(w::UInt64, b::UInt8)::UInt8
    x = w ⊻ (ONES8 * b)
    z = ~(((x & LOWS7) + LOWS7) | x | LOWS7)   # 0x80 at each zero byte of x
    return UInt8(((z >> 7) * 0x0102040810204080) >> 56)
end

# prefix_xor8(m) bit i = XOR of m's bits 0..i — i.e. quote-toggle parity up to and
# including byte i. log2(8)=3 shift-xor steps.
@inline function prefix_xor8(m::UInt8)::UInt8
    m ⊻= m << 1
    m ⊻= m << 2
    m ⊻= m << 4
    return m
end

function indexchunk_swar!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    @assert swareligible(d)
    start, stop = ci.start, ci.stop
    delim = d.delim::UInt8
    oq = d.oq
    fieldstart = start
    rowstart = start
    inq = false          # quote parity carried between words
    crcarry = false      # last structural byte of previous word was CR (suppresses a leading LF)
    pos = start
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 7 <= stop
            # The movemask constants number bytes from the least-significant end.
            # Normalize the native load so the mapping is also correct on a
            # big-endian host.
            w = ltoh(unsafe_load(Ptr{UInt64}(p + pos - 1)))
            q  = d.quoted ? bytemask8(w, oq) : 0x00
            dm = bytemask8(w, delim)
            lf = bytemask8(w, LF)
            cr = bytemask8(w, CR)
            inmask = prefix_xor8(q)
            inq && (inmask = ~inmask)
            out = ~inmask
            sd  = dm & out
            scr = cr & out
            slf = lf & out & ~((scr << 1) | (crcarry ? 0x01 : 0x00))  # drop LF of a CRLF pair
            special = sd | scr | slf
            while special != 0x00
                tz = trailing_zeros(special)
                at = pos + tz
                b = buf[at]
                if b == delim
                    emitfield!(ci, fieldstart, at - 1)
                    fieldstart = at + 1
                else # CR or LF row terminator
                    emitfield!(ci, fieldstart, at - 1)
                    endrow!(ci, buf, d, rowstart)
                    nxt = at + 1
                    b == CR && nxt <= stop && buf[nxt] == LF && (nxt += 1)
                    fieldstart = rowstart = nxt
                end
                special &= special - 0x01
            end
            crcarry = (scr & 0x80) != 0x00
            inq ⊻= isodd(count_ones(q))
            pos += 8
        end
    end
    # Scalar tail for the last <8 bytes, continuing the carried state.
    _swar_tail!(ci, buf, d, pos, fieldstart, rowstart, inq, crcarry)
    return ci
end

function _swar_tail!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, pos::Int,
                     fieldstart::Int, rowstart::Int, inq::Bool, crcarry::Bool)
    stop = ci.stop
    delim = d.delim::UInt8
    # A CRLF pair split exactly at a word boundary: the CR (last byte of the previous
    # word) already terminated the row and advanced fieldstart past this LF; skip it
    # so the scalar loop doesn't emit a spurious empty row.
    if crcarry && pos <= stop && @inbounds(buf[pos]) == LF
        pos += 1
        fieldstart = rowstart = pos
    end
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
        elseif d.quoted && b == d.oq
            inq = true
            pos += 1
        elseif b == delim
            emitfield!(ci, fieldstart, pos - 1)
            pos += 1
            fieldstart = pos
        elseif b == LF
            emitfield!(ci, fieldstart, pos - 1)
            endrow!(ci, buf, d, rowstart)
            pos += 1
            fieldstart = rowstart = pos
        elseif b == CR
            emitfield!(ci, fieldstart, pos - 1)
            endrow!(ci, buf, d, rowstart)
            pos += 1
            pos <= stop && buf[pos] == LF && (pos += 1)
            fieldstart = rowstart = pos
        else
            pos += 1
        end
    end
    _finishchunk!(ci, buf, d, fieldstart, rowstart, inq)
    return
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
    @inbounds @simd for i in from:to
        n += buf[i] == q
    end
    return isodd(n)
end

# First row start after a terminator at or after `from`, given the entry quote
# state. CRLF returns the byte after LF. Returns `to + 1` when no terminator
# exists; callers treat a result > `to` as "no row starts in this range".
function nextrowstart(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect, inquote::Bool)::Int
    pos = from
    cq, oq, e = d.cq, d.oq, d.e
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

"""
    index(buf, d::Dialect; datastart=1, chunkbytes=2^23, parallel=true, fastindex=true)

Build the structural index for `buf[datastart:end]`: row-aligned chunks, each with
per-field spans. Deterministic for any `chunkbytes`/thread count (pinned by tests).
"""
function index(buf::Vector{UInt8}, d::Dialect;
               datastart::Int=1,
               chunkbytes::Int=1 << 23,
               parallel::Bool=Threads.nthreads() > 1,
               fastindex::Bool=true)
    len = length(buf)
    # No lower bound beyond 1: tests deliberately use tiny chunkbytes to force row
    # boundaries everywhere; production defaults keep chunks cache-sized (~8 MiB).
    chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    datastart >= 1 || throw(ArgumentError("datastart must be ≥ 1 (got $datastart)"))
    datastart > len && return BufferIndex(ChunkIndex[], 0, false)

    useswar = fastindex && swareligible(d)
    nranges = parallel && parityclean(d) ? max(1, cld(len - datastart + 1, chunkbytes)) : 1

    # Step 1+2: entry quote-state per range via parity composition.
    starts = [datastart + (i - 1) * chunkbytes for i in 1:nranges]
    entry = falses(nranges)
    if nranges > 1
        par = Vector{Bool}(undef, nranges)
        @sync for i in 1:nranges
            Threads.@spawn begin
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

    # Step 3: true row starts. Range 1 starts at datastart by definition.
    bounds = Vector{Int}(undef, nranges)
    bounds[1] = datastart
    if nranges > 1
        @sync for i in 2:nranges
            Threads.@spawn begin
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
    # FieldSpan offsets are chunk-relative UInt32. A chunk only exceeds `chunkbytes`
    # when a single row straddles whole ranges, so this bound is about one giant row.
    for ci in chunks
        ci.stop - ci.start < typemax(UInt32) ||
            throw(ArgumentError("a single row exceeds 4 GiB; not supported by the prove-out kernel"))
    end

    if length(chunks) == 1 || !parallel
        for ci in chunks
            useswar ? indexchunk_swar!(ci, buf, d) : indexchunk_scalar!(ci, buf, d)
        end
    else
        @sync for ci in chunks
            Threads.@spawn (useswar ? indexchunk_swar!(ci, buf, d) : indexchunk_scalar!(ci, buf, d))
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
#   Missing → Int64 → Float64 → String
#   Missing → (Date | DateTime | Time | Bool) → String
# Everything else (mixed temporals, bool/number mixes, …) promotes to String.
# InlineString widths, Int downcasting, and user "typemap"s are API-layer concerns
# built on the same machinery (see README).
promote_kernel(a::Type, b::Type) =
    a === b          ? a :
    a === Missing    ? b :
    b === Missing    ? a :
    a === Int64 && b === Float64 ? Float64 :
    a === Float64 && b === Int64 ? Float64 :
    String

# Detect the type of one raw field span (mirrors CSV.jl's cascade, minus the
# Int-width games). Detection and parsing use the same options and exact-span
# checks, so a conflict always advances the finite promotion lattice.
function detecttype(buf::Vector{UInt8}, pos::Int, len::Int, opts::Parsers.Options)
    len == 0 && return Missing
    stop = pos + len - 1
    # Missing-first: custom sentinel strings and stripwhitespace-emptied fields are
    # type-neutral. Probed via a String parse because Parsers' numeric parsers
    # report a stripped-to-empty field as INVALID rather than SENTINEL.
    sres = xparsestring(buf, pos, stop, opts)
    Parsers.sentinel(sres.code) && sres.tlen == len && return Missing
    for T in (Int64, Float64, Date, DateTime, Time, Bool)
        res = Parsers.xparse(T, buf, pos, stop, opts)
        if Parsers.ok(res.code)
            Parsers.sentinel(res.code) && return Missing
            res.tlen == len && return T
        end
    end
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

# String cells stay in the buffer until someone looks at them. We store the
# *content* span Parsers reports (quotes stripped) plus whether it contains escape
# sequences; getindex materializes — and unescapes — on access. `len == -1` marks
# missing so no separate presence vector is needed.
struct StrSpan
    pos::Int64
    len::Int32
    escaped::Bool
end
const STR_MISSING = StrSpan(0, Int32(-1), false)

mutable struct StringColumn
    spans::Vector{StrSpan}
    buf::Vector{UInt8}
    e::UInt8            # escape char, needed to unescape on materialization
    cq::UInt8           # close-quote char (e == cq for RFC ""-doubling)
end
StringColumn(n::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8) =
    StringColumn(fill(STR_MISSING, n), buf, e, cq)

# Parsers.PosLen stores only 20 length bits. Ask Parsers for its 31-bit span
# result so a long string is not truncated before we copy the span into StrSpan.
# This result type is available throughout the kernel's supported Parsers range.
@inline xparsestring(buf::Vector{UInt8}, pos::Int, stop::Int, opts::Parsers.Options) =
    Parsers.xparse(String, buf, pos, stop, opts, Parsers.PosLen31)

# The kernel's own unescape: `""` collapses to `"` when e == cq; `\X` drops the
# backslash when e != cq. We do NOT round-trip through Parsers.getstring here
# because Parsers.PosLen caps a field's length at 2^20-1 bytes (the root of
# CSV.jl issue #935); the kernel's spans are Int32 and must stay lossless.
function _unescape(buf::Vector{UInt8}, pos::Int64, len::Int32, e::UInt8, cq::UInt8)
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
    return String(resize!(out, n))
end

# All-missing column.
struct MissingColumn <: AbstractVector{Missing}
    n::Int
end
Base.size(c::MissingColumn) = (c.n,)
Base.@propagate_inbounds function Base.getindex(c::MissingColumn, i::Int)
    @boundscheck checkbounds(c, i)
    return missing
end

# The user-facing views. `materialize` (below) converts to plain Vectors when the
# caller prefers copies over views.
struct MaybeVector{T} <: AbstractVector{Union{T, Missing}}
    values::Vector{T}
    present::Vector{Bool}
end
Base.size(v::MaybeVector) = size(v.values)
Base.@propagate_inbounds function Base.getindex(v::MaybeVector, i::Int)
    @boundscheck checkbounds(v.values, i)
    @inbounds return v.present[i] ? v.values[i] : missing
end

struct StringViewVector{ELT} <: AbstractVector{ELT}
    spans::Vector{StrSpan}
    buf::Vector{UInt8}
    e::UInt8
    cq::UInt8
end
Base.size(v::StringViewVector) = size(v.spans)
Base.@propagate_inbounds function Base.getindex(v::StringViewVector{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(v.spans, i)
    @inbounds s = v.spans[i]
    s.len < 0 && return missing
    s.len == 0 && return ""
    if s.escaped
        return _unescape(v.buf, s.pos, s.len, v.e, v.cq)
    else
        GC.@preserve v begin
            return unsafe_string(pointer(v.buf, s.pos), s.len)
        end
    end
end

# --- per-(column × chunk) parse loops ---------------------------------------
#
# THE point of the whole design: each call below is monomorphic in the column type.
# Dynamic dispatch happens once per (column, chunk) — thousands of times per file —
# instead of once per cell. A mid-chunk type surprise returns the offending row so
# the driver can promote and re-run *this column only*.

# Returns 0 on success, or the local row of the first conflicting value.
function parsecolchunk!(col::TypedColumn{T}, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::Parsers.Options,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase) where {T}
    values, present = col.values, col.present
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        out = rowbase + (lr - ci.firstdatarow) + 1
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue                      # short row ⇒ missing (reported once per row by the driver)
        pos, len = sp
        len == 0 && continue                            # empty ⇒ missing (default sentinel)
        res = Parsers.xparse(T, buf, pos, pos + len - 1, opts)
        if Parsers.ok(res.code) && res.tlen == len
            if Parsers.sentinel(res.code)
                # user-configured sentinel string ⇒ missing
            else
                # Parsers intentionally accepts numeric spellings for Bool and
                # temporal targets. Inference classifies those spellings as
                # Int64, and the lattice says mixed numeric/bool/temporal data is
                # String. Reject the wider parse domain for inferred columns so
                # the final type does not depend on which rows were sampled.
                if !userprovided &&
                   (T === Bool || T === Date || T === DateTime || T === Time) &&
                   detecttype(buf, pos, len, opts) !== T
                    return lr
                end
                values[out] = res.val
                present[out] = true
            end
        else
            # Invalid for T. A field that is a *sentinel* under the full options
            # (e.g. whitespace-only with stripwhitespace=true) is missing, not a
            # conflict — Parsers only reports
            # SENTINEL through the String path, so probe it on this cold path.
            sres = xparsestring(buf, pos, pos + len - 1, opts)
            if Parsers.sentinel(sres.code) && sres.tlen == len
                # missing; value slot stays absent
            elseif userprovided
                problemrow = problemrowbase + (lr - ci.firstdatarow) + 1
                pushproblem!(problems, problemrow, j, pos, :invalid_value,
                             "cannot parse $(T) from " * excerpt(buf, pos, len))
                # value stays missing under strict=false semantics
            else
                return lr                               # inference conflict ⇒ promote & re-parse column
            end
        end
    end
    return 0
end

function parsecolchunk!(col::StringColumn, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::Parsers.Options,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase)
    spans = col.spans
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        out = rowbase + (lr - ci.firstdatarow) + 1
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        pos, len = sp
        len == 0 && continue                            # unquoted empty ⇒ missing; quoted "" survives below
        res = xparsestring(buf, pos, pos + len - 1, opts)
        if !Parsers.ok(res.code) || res.tlen != len
            kind = Parsers.invalidquotedfield(res.code) ? :invalid_quoted_field : :invalid_value
            message = kind === :invalid_quoted_field ? "malformed quoting in " :
                      "string parser did not consume the exact field span "
            problemrow = problemrowbase + (lr - ci.firstdatarow) + 1
            pushproblem!(problems, problemrow, j, pos, kind, message * excerpt(buf, pos, len))
            continue
        end
        if Parsers.sentinel(res.code)
            continue
        end
        pl = res.val
        spans[out] = StrSpan(Int64(pl.pos), Int32(pl.len), Parsers.escapedstring(res.code))
    end
    return 0
end

# A column believed all-missing: inferred columns report the first conflict so
# the driver can promote; explicit Missing columns report every present value.
function parsecolchunk_missing(buf::Vector{UInt8}, ci::ChunkIndex, j::Int,
                               rowbase::Int, opts::Parsers.Options,
                               userprovided::Bool, problems)
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        _, len = sp
        len == 0 && continue
        res = xparsestring(buf, sp[1], sp[1] + len - 1, opts)
        ismissing = Parsers.ok(res.code) && res.tlen == len && Parsers.sentinel(res.code)
        if !ismissing
            userprovided || return lr
            out = rowbase + (lr - ci.firstdatarow) + 1
            pushproblem!(problems, out, j, sp[1], :invalid_value,
                         "column typed Missing contains " * excerpt(buf, sp[1], len))
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
    lock::ReentrantLock
end
function ProblemLog(limit::Int)
    limit >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $limit)"))
    return ProblemLog(Problem[], limit, 0, nothing, ReentrantLock())
end

problemkey(p::Problem) = (p.row, p.col, p.pos, String(p.kind), p.message)

function pushproblem!(log::ProblemLog, row::Int, col::Int, pos::Int, kind::Symbol, msg::String)
    p = Problem(row, col, pos, kind, msg)
    lock(log.lock) do
        (log.first === nothing || problemkey(p) < problemkey(log.first)) && (log.first = p)
        if length(log.items) < log.limit
            push!(log.items, p)
        else
            log.dropped += 1
            if log.limit > 0
                _, maxi = findmax(problemkey, log.items)
                problemkey(p) < problemkey(log.items[maxi]) && (log.items[maxi] = p)
            end
        end
    end
    return
end

sortproblems!(log::ProblemLog) = sort!(log.items; by=problemkey)

function excerpt(buf::Vector{UInt8}, pos::Int, len::Int; maxbytes::Int=32)
    n = min(len, maxbytes)
    s = String(buf[pos:pos + n - 1])
    return repr(len > maxbytes ? s * "…" : s)
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
                     opts::Parsers.Options; nsample::Int=128)
    nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    total = sum(nrows, chunks; init=0)
    total == 0 && return fill(Missing, ncols)
    types = fill(Missing, ncols)
    count = min(total, nsample)
    for k in 1:count
        # Exact integer interpolation includes both ends without duplicates.
        gr = count == 1 ? 1 :
             1 + Int(widemul(k - 1, total - 1) ÷ (count - 1))
        ci, lr = locate(chunks, gr)
        for j in 1:ncols
            sp = fieldspan(ci, lr, j)
            sp === nothing && continue
            types[j] = promote_kernel(types[j], detecttype(buf, sp[1], sp[2], opts))
        end
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

# Resolve the `types` keyword: nothing | Type | AbstractVector | AbstractDict
# (by name or index). `Union{T,Missing}` collapses to T (missingness is tracked
# per-value, never in the column type).
function resolvetypes(types, names::Vector{Symbol}, ncols::Int)
    seed = Vector{Union{Nothing, Type}}(nothing, ncols)
    types === nothing && return seed
    function normalize(T)
        T === nothing && return nothing
        T isa Type || throw(ArgumentError("column type must be a Type or nothing (got $(repr(T)))"))
        T = T === Missing ? Missing : Base.nonmissingtype(T)
        parseable = T === Missing || T === Number ||
                    (isconcretetype(T) &&
                     (Parsers.supportedtype(T) || hasmethod(tryparse, Tuple{Type{T}, String})))
        parseable || throw(ArgumentError("unsupported column type $T"))
        return T
    end
    if types isa Type
        fill!(seed, normalize(types))
    elseif types isa AbstractVector
        length(types) == ncols || throw(ArgumentError("types vector length $(length(types)) != $ncols columns"))
        seed .= normalize.(types)
    elseif types isa AbstractDict
        for (k, T) in types
            j = k isa Integer ? Int(k) : findfirst(==(Symbol(k)), names)
            j === nothing && throw(ArgumentError("types key $k does not match any column"))
            1 <= j <= ncols || throw(ArgumentError("types key $k out of range"))
            seed[j] = normalize(T)
        end
    else
        throw(ArgumentError("unsupported types specification: $(typeof(types))"))
    end
    return seed
end

"""
    CSVKernel.parse(buf::Vector{UInt8}; kwargs...) -> ParsedTable
    CSVKernel.parse(str::AbstractString; kwargs...)
    CSVKernel.parse(io::IO; kwargs...)

Eagerly parse delimited data: index in parallel, infer types from a stratified
sample, then parse each column of each chunk in a monomorphic loop, promoting
per-column on conflict. The default records malformed data as problems;
`on_error=:error` escalates the source-earliest problem after parsing.

Keywords: `delim`, `quotechar`, `openquotechar`/`closequotechar`, `escapechar`,
`quoted`, `comment`, `ignoreemptyrows`, `header` (true | false | Vector), `types`
(Type | Vector | Dict), `dateformat`, `decimal`, `truestrings`/`falsestrings`,
`stripwhitespace`, `chunkbytes`, `parallel`, `fastindex`, `maxproblems`,
`on_error` (:collect | :error), `nsample`.
"""
function parse(buf::Vector{UInt8};
               header::Union{Bool, AbstractVector}=true,
               types=nothing,
               dateformat=nothing,
               decimal::Char='.',
               truestrings=nothing,
               falsestrings=nothing,
               stripwhitespace::Bool=false,
               chunkbytes::Int=1 << 23,
               parallel::Bool=Threads.nthreads() > 1,
               fastindex::Bool=true,
               maxproblems::Int=10_000,
               on_error::Symbol=:collect,
               nsample::Int=128,
               dialectkw...)
    on_error in (:collect, :error) || throw(ArgumentError("on_error must be :collect or :error"))
    nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    d = Dialect(; dialectkw...)
    opts = makeoptions(d; dateformat, decimal, truestrings, falsestrings, stripwhitespace)
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1  # BOM
    bi = index(buf, d; datastart, chunkbytes, parallel, fastindex)
    chunks = bi.chunks
    log = ProblemLog(maxproblems)

    # -- header & column names ------------------------------------------------
    local names::Vector{Symbol}
    if header === true && !isempty(chunks)
        ci = chunks[1]
        hrow = ci.firstdatarow
        nh = nfields(ci, hrow)
        names = Vector{Symbol}(undef, nh)
        for j in 1:nh
            sp = fieldspan(ci, hrow, j)::Tuple{Int, Int}
            pos, len = sp
            if len == 0
                names[j] = Symbol("Column", j)
            else
                res = xparsestring(buf, pos, pos + len - 1, opts)
                if Parsers.ok(res.code) && res.tlen == len
                    pl = res.val
                    names[j] = Symbol(Parsers.escapedstring(res.code) ?
                                      _unescape(buf, Int64(pl.pos), Int32(pl.len), opts.e, d.cq) :
                                      GC.@preserve(buf, unsafe_string(pointer(buf, pl.pos), pl.len)))
                else
                    # Keep malformed header bytes exact. The file-level malformed
                    # quote problem is also retained as data.
                    names[j] = Symbol(String(buf[pos:pos + len - 1]))
                    kind = Parsers.invalidquotedfield(res.code) ? :invalid_quoted_field : :invalid_value
                    message = kind === :invalid_quoted_field ? "malformed quoting in header " :
                              "header parser did not consume the exact field span "
                    pushproblem!(log, 0, j, pos, kind, message * excerpt(buf, pos, len))
                end
            end
        end
        ci.firstdatarow = hrow + 1
    elseif header isa AbstractVector
        names = Symbol.(header)
    else
        ncg = isempty(chunks) ? 0 : nfields(chunks[1], chunks[1].firstdatarow)
        names = [Symbol("Column", j) for j in 1:ncg]
    end
    names = makeunique!(names)
    ncols = length(names)
    ndata = sum(nrows, chunks; init=0)

    # -- row-shape sweep (ragged rows reported once, at row granularity) ------
    rowbase = 0
    for ci in chunks
        for lr in ci.firstdatarow:totalrows(ci)
            nf = nfields(ci, lr)
            grow = rowbase + (lr - ci.firstdatarow) + 1
            if nf < ncols
                sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
                pushproblem!(log, grow, 0, sp[1], :short_row,
                             "expected $ncols fields, found $nf (remaining columns set to missing)")
            elseif nf > ncols
                sp = fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
                pushproblem!(log, grow, 0, sp[1], :long_row,
                             "expected $ncols fields, found $nf (extra fields ignored)")
            end
        end
        rowbase += nrows(ci)
    end
    bi.unclosedquote &&
        pushproblem!(log, 0, 0, length(buf), :unclosed_quote,
                     "input ended inside a quoted field")

    # -- type seeding ----------------------------------------------------------
    seed = resolvetypes(types, names, ncols)
    userprovided = [T !== nothing for T in seed]
    if any(isnothing, seed)
        inferred = sampletypes(buf, chunks, ncols, opts; nsample)
        for j in 1:ncols
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    coltypes = Type[T for T in seed]

    # -- parse waves with per-column promotion --------------------------------
    # Wave: parse every (chunk × column) in parallel (one task per chunk; the
    # per-column dispatch inside is the once-per-column-chunk dynamic call).
    # Conflicted columns promote and re-parse — only those columns — next wave.
    storage = Vector{Any}(undef, ncols)
    dirty = trues(ncols)   # all columns need (re)allocation+parse in wave 1
    promo = copy(coltypes)
    promolock = ReentrantLock()
    wave = 0
    while any(dirty)
        wave += 1
        wave > 8 && error("internal error: promotion did not converge") # lattice height is 3
        todo = findall(dirty)
        for j in todo
            coltypes[j] = promo[j]
            storage[j] = allocatecolumn(coltypes[j], ndata, buf, opts.e, d.cq)
        end
        fill!(dirty, false)
        @sync for ci in chunks
            rb = chunkrowbase(chunks, ci)
            Threads.@spawn begin
                for j in todo
                    T = coltypes[j]
                    conflictrow = if T === Missing
                        parsecolchunk_missing(buf, ci, j, rb, opts, userprovided[j], log)
                    else
                        parsecolchunk!(storage[j], buf, ci, j, rb, opts, userprovided[j], log)
                    end
                    if conflictrow != 0
                        sp = fieldspan(ci, conflictrow, j)::Tuple{Int, Int}
                        newT = promote_kernel(T, detecttype(buf, sp[1], sp[2], opts))
                        newT = newT === T ? String : newT  # a conflicting value must move the type
                        lock(promolock) do
                            promo[j] = promote_kernel(promo[j], newT)
                            dirty[j] = true
                        end
                    end
                end
            end
        end
    end

    # -- finalize --------------------------------------------------------------
    cols = Vector{AbstractVector}(undef, ncols)
    for j in 1:ncols
        cols[j] = finalizecolumn(coltypes[j], storage[j], ndata)
    end
    sortproblems!(log)
    if on_error === :error && log.first !== nothing
        p = log.first
        nproblems = length(log.items) + log.dropped
        throw(ErrorException("CSVKernel: $(p.kind) at data row $(p.row), column $(p.col): $(p.message)" *
                             (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
    return ParsedTable(names, cols, ndata, log.items, log.dropped)
end

parse(str::AbstractString; kw...) = parse(Vector{UInt8}(codeunits(str)); kw...)
parse(io::IO; kw...) = parse(read(io); kw...)

chunkrowbase(chunks::Vector{ChunkIndex}, target::ChunkIndex) =
    sum(nrows(c) for c in chunks if c.start < target.start; init=0)

function finalizecolumn(::Type{Missing}, ::Nothing, n::Int)
    return MissingColumn(n)
end
finalizecolumn(::Type{Missing}, ::Nothing, n::Int, ::Bool) = MissingColumn(n)
function finalizecolumn(::Type{String}, col::StringColumn, n::Int)
    anymissing = any(s -> s.len < 0, col.spans)
    return anymissing ? StringViewVector{Union{String, Missing}}(col.spans, col.buf, col.e, col.cq) :
                        StringViewVector{String}(col.spans, col.buf, col.e, col.cq)
end
function finalizecolumn(::Type{String}, col::StringColumn, n::Int, force_missing::Bool)
    anymissing = force_missing || any(s -> s.len < 0, col.spans)
    return anymissing ? StringViewVector{Union{String, Missing}}(col.spans, col.buf, col.e, col.cq) :
                        StringViewVector{String}(col.spans, col.buf, col.e, col.cq)
end
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int) where {T}
    # no missings ⇒ hand back the raw Vector{T}, zero copies
    return all(col.present) ? col.values : MaybeVector{T}(col.values, col.present)
end
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int, force_missing::Bool) where {T}
    return !force_missing && all(col.present) ? col.values : MaybeVector{T}(col.values, col.present)
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

function makeunique!(names::Vector{Symbol})
    seen = Dict{Symbol, Int}()
    for i in eachindex(names)
        nm = names[i]
        if haskey(seen, nm)
            k = seen[nm]
            newnm = Symbol(nm, :_, k)
            while haskey(seen, newnm)
                k += 1
                newnm = Symbol(nm, :_, k)
            end
            seen[nm] = k + 1
            seen[newnm] = 1
            names[i] = newnm
        else
            seen[nm] = 1
        end
    end
    return names
end

end # module CSVKernel
