# Implementation of CSV.jl's public reading front doors and its internal
# delimiter/shape detection.
#
# Every entry point uses the same pipeline: resolve source bytes → settle the
# dialect (sniffing if asked) →
# index once → settle names/row-window (header/skipto/footerskip/limit as
# *index arithmetic*, before any value work) → hand the kernel driver or the
# streaming primitives the prepared index. There is no per-entrypoint parsing
# code and no mode flags inside the kernel: File/Rows/Chunks differ only in
# what they do AFTER `_prepare`.
#
# Compatibility decisions are pinned by tests:
#   • warnings are DATA: `problems(f)` replaces strict/silencewarnings logging
#     (`strict=true` maps to `on_error=:error`, `maxwarnings` to `maxproblems`)
#   • empty unquoted cells are ALWAYS missing; `missingstring` ADDS spellings
#     (CSV.jl 0.10 could turn empties into present "" values)
#   • function-typed `select`/`drop`/`types` are retired (Tables.Scan is the
#     expression channel); list/Dict forms keep working
#   • `stringtype` defaults to the kernel string (CompactString);
#     `stringtype=String` materializes; InlineStrings become an extension
#   • Bool columns are strictly `true`/`false` unless truestrings/falsestrings
#   • integer spellings that fit Int128 stay exact, including initially-wide
#     and grouped columns where CSV.jl can widen Int64 overflow to Float64
#
using Tables, Unicode, Mmap, PooledArrays, CodecZlib, Downloads

# `sniff`/`Spec` are internal (behind `delim=nothing`); not exported.

# 1.0: no pooling unless asked. Pooling by 0.10's default policy measured
# +65% parse time on a pool-friendly 39 MiB file (22.4 vs 13.6 ms), and every
# other reader surveyed (polars, pyarrow.csv, pandas, DuckDB, fread) makes
# dictionary/categorical encoding opt-in. `pool=(0.2, 500)` restores the old
# behavior; `pool=true` pools every string column.
const DEFAULT_POOL = false
# Pooled references are UInt32, but a 32-bit Julia process cannot represent
# UInt32's full maximum as Int. Cap at the smaller index space without an
# overflowing conversion during precompile.
const _MAX_POOL_LEVELS = Int(min(UInt64(typemax(Int)), UInt64(typemax(UInt32))))

const _DIALECTKW = (:quotechar, :openquotechar, :closequotechar, :escapechar,
                    :quoted, :comment, :ignoreemptyrows, :ignorerepeated)
const _VALUEKW = (:dateformat, :decimal, :truestrings, :falsestrings,
                  :stripwhitespace, :groupmark)
const _INDEXKW = (:fastindex, :scanner)
const _DRIVERKW = (:maxproblems, :nsample, :typemap)

function _pickkwargs(kw, allowed)
    return NamedTuple(p for p in pairs(kw) if p.first in allowed)
end

const _REMOVED_KW = Dict{Symbol, String}(
    :silencewarnings => "warnings are data now: problems(f) returns them; maxproblems caps retention",
    :debug => "removed in 1.0; parse problems and the structural index are inspectable directly",
    :lazystrings => "use stringtype=CSV.CompactString (the default) or stringtype=String",
    :tasks => "use ntasks",
    :threaded => "use ntasks (ntasks=1 disables threading)",
    :rows_to_check => "use nsample",
    :lines_to_check => "use nsample",
    :ignoreemptylines => "use ignoreemptyrows",
    :datarow => "use skipto",
    :type => "use types (a single Type applies to every column)",
    :missingstrings => "pass a string or vector to missingstring",
    :dateformats => "pass per-column formats to dateformat",
    :parsingdebug => "parse problems and the structural index are inspectable directly",
)

function _checkkwargs(context::AbstractString, kw, allowed)
    for k in keys(kw)
        if haskey(_REMOVED_KW, k)
            throw(ArgumentError("$k was removed in 1.0: $(_REMOVED_KW[k])"))
        end
        k in allowed || throw(ArgumentError("unsupported $context keyword $k"))
    end
    return
end

function _sentinels(missingstring)
    missingstring === nothing && return nothing
    if missingstring isa AbstractString
        return isempty(missingstring) ? nothing : [String(missingstring)]
    end
    sentinels = String[]
    for s in missingstring
        s isa AbstractString ||
            throw(ArgumentError("missingstring entries must be strings (got $(typeof(s)))"))
        isempty(s) || push!(sentinels, String(s))
    end
    return isempty(sentinels) ? nothing : sentinels
end

function _probedelim(dialectkw)
    quotechar = haskey(dialectkw, :quotechar) ? dialectkw.quotechar : '"'
    openquotechar = haskey(dialectkw, :openquotechar) ? dialectkw.openquotechar : nothing
    oq = something(openquotechar, quotechar)
    for c in ('\x1f', '\x1e', '\x1d')
        c == oq || return c
    end
    return '\x1c'
end

# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
# CSV.jl semantics: an AbstractString is a FILE PATH (use IOBuffer(str) or
# codeunits for literal data). Everything becomes one byte buffer at this seam;
# large regular files use a read-only mapping while other sources use a copy.

# Files at or above the threshold memory-map instead of copying: the kernel
# never writes to `buf`, columns retain a reference to the mapping (unmapped
# when the table is collected), and page faults amortize over the parallel
# chunk sweep. Small files still read() — one small copy beats fault setup.
# `buffer_in_memory=true` forces the copy (CSV.jl parity; e.g. when the file
# may be replaced while the table is alive).
const MMAP_THRESHOLD = 1 << 19

# gzip is detected by magic bytes on every source kind (CSV.jl parity): a
# compressed source decompresses to a fresh buffer before any parsing.
_isgzip(buf::AbstractVector{UInt8}) = length(buf) >= 2 && buf[1] == 0x1f && buf[2] == 0x8b
_maybegunzip(buf::Vector{UInt8}) = _isgzip(buf) ? transcode(GzipDecompressor, buf) : buf

resolvesource(buf::Vector{UInt8}; buffer_in_memory::Bool=false, prefetch::Bool=true) =
    _maybegunzip(buf)
# other byte containers (codeunits, views) copy into a Vector — the kernel's buffer contract
resolvesource(buf::AbstractVector{UInt8}; kw...) = resolvesource(Vector{UInt8}(buf); kw...)
resolvesource(io::IO; buffer_in_memory::Bool=false, prefetch::Bool=true) =
    _maybegunzip(Base.read(io))
resolvesource(cmd::Base.AbstractCmd; buffer_in_memory::Bool=false, prefetch::Bool=true) =
    _maybegunzip(Base.read(cmd))
const PREFETCH_PAGE = 16384

function _prefetchrange(m::Vector{UInt8}, lo::Int, hi::Int)
    acc = UInt8(0)
    @inbounds for i in lo:PREFETCH_PAGE:hi
        acc ⊻= m[i]
    end
    return acc
end

function _prefetch!(m::Vector{UInt8})
    n = length(m)
    parts = min(4, Threads.nthreads())
    for p in 1:parts
        lo = 1 + (p - 1) * n ÷ parts
        hi = p * n ÷ parts
        Threads.@spawn _prefetchrange(m, lo, hi)
    end
    return
end

function resolvesource(s::AbstractString; buffer_in_memory::Bool=false, prefetch::Bool=true)
    # a URL (#506): fetch to a temporary file with the Downloads stdlib, then
    # resolve that path exactly like any other (magic-byte gzip, mmap, ...)
    if startswith(s, r"^https?://")
        path = Downloads.download(String(s))
        try
            return resolvesource(path; buffer_in_memory=true, prefetch)   # temp file: read, don't map
        finally
            rm(path; force=true)
        end
    end
    isfile(s) || throw(ArgumentError("no file at $(repr(String(s))) — a String " *
                                     "source is a file path (or an http(s):// URL); " *
                                     "wrap literal data in IOBuffer"))
    return open(s, "r") do io
        isfile(io) || throw(ArgumentError("no regular file at $(repr(String(s)))"))
        sz = filesize(io)
        if sz >= 2
            magic = Base.read(io, 2)
            seekstart(io)
            magic[1] == 0x1f && magic[2] == 0x8b &&
                return transcode(GzipDecompressor, Base.read(io))
        end
        (buffer_in_memory || sz < MMAP_THRESHOLD) && return Base.read(io)
        # Use the descriptor that supplied `sz`. This prevents a path replacement
        # between filesize and mmap from mapping a different file at the old size.
        m = Mmap.mmap(io, Vector{UInt8}, sz; grow=false)
        # async readahead: faulting overlaps the parallel parse (measured 22% on a
        # warm 200 MiB file; larger when cold). madvise is a Unix API.
        @static Sys.isunix() && Mmap.madvise!(m, Mmap.MADV_WILLNEED)
        # cold-file IO/parse overlap: WILLNEED alone loses to demand faults on a
        # cold file (the serial quote-parity pre-scan walks the whole buffer
        # before the parallel chunk wave). Detached toucher tasks stride one
        # byte per page across disjoint regions, converting demand faults into
        # queued readahead that runs AHEAD of the parity scan. Warm files are
        # unaffected (touching resident pages is nanoseconds); the closures
        # keep the mapping alive for the toucher lifetime.
        prefetch && Threads.nthreads() > 1 && _prefetch!(m)
        return m
    end
end

_datastart(buf) = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1

# ---------------------------------------------------------------------------
# names — CSV.jl's normalizename, verbatim semantics
# ---------------------------------------------------------------------------

const RESERVED = Set(["local", "global", "export", "let",
    "for", "struct", "while", "const", "continue", "import",
    "function", "if", "else", "try", "begin", "break", "catch",
    "return", "using", "baremodule", "macro", "finally",
    "module", "elseif", "end", "quote", "do"])

function normalizename(name::String)
    uname = strip(Unicode.normalize(name))
    id = Base.isidentifier(uname) ? uname : map(c -> Base.is_id_char(c) ? c : '_', uname)
    cleansed = string((isempty(id) || !Base.is_id_start_char(id[1]) || id in RESERVED) ? "_" : "", id)
    return Symbol(replace(cleansed, r"(_)\1+" => "_"))
end

# ---------------------------------------------------------------------------
# sniff — dialect + shape detection, returning a replayable Spec
# ---------------------------------------------------------------------------
# The kernel index IS the detector: for each candidate delimiter, index a
# bounded quote-aware sample and score how consistent the per-row field counts
# are. Candidates represented in the first surviving row win before data-only
# punctuation, which prevents Time values from making `:` look like a delimiter.
# Candidate order is CSV.jl's, and breaks score ties.

const DELIM_CANDIDATES = (',', '\t', ' ', '|', ';', ':')

"""
    CSV.Spec

A replayable parse plan from [`CSV.sniff`](@ref CSV.sniff): splat it back —
`CSV.File(src; spec.delim, spec.header)` — or pass fields individually. Fields:
`delim`, `quoted`, `header` (likely-present), `ncols`, `names`, `types`.
"""
struct Spec
    delim::Char
    quoted::Bool
    header::Bool
    ncols::Int
    names::Vector{Symbol}
    types::Vector{Type}
end

function Base.show(io::IO, s::Spec)
    print(io, "CSV.Spec(delim=", repr(s.delim), ", header=", s.header,
          ", ", s.ncols, " column(s))")
    for (nm, T) in zip(s.names, s.types)
        print(io, "\n  ", nm, "::", T)
    end
end

# Quote-aware sample clip. When bounded, discard the final raw row because it
# may be cut. Row boundaries depend on quote syntax, not on the delimiter.
function _sample(buf::Vector{UInt8}, samplebytes::Int; start::Int=1, dialectkw...)
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    start = clamp(start, 1, length(buf) + 1)
    length(buf) - start + 1 <= samplebytes && return start == 1 ? buf : buf[start:end]
    d = Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    limit = samplebytes
    while true
        sample = buf[start:min(start + limit - 1, length(buf))]
        datastart = _datastart(sample)
        rowstart = datastart
        while rowstart <= length(sample)
            next = nextrowstart(sample, rowstart, length(sample), d, false, true)
            next > length(sample) && break
            rowstart = next
        end
        # keep only complete rows; when not even one row fits (a single row
        # wider than samplebytes — wide scientific files), grow until one does
        rowstart > datastart && return sample[1:rowstart - 1]
        limit >= length(buf) - start + 1 && return start == 1 ? buf : buf[start:end]
        limit = limit > typemax(Int) ÷ 8 ? length(buf) : min(8 * limit, length(buf))
    end
end

function _scoredelim(buf::Vector{UInt8}, delim::Char, datastart::Int,
                     dialectkw::NamedTuple, indexkw::NamedTuple)
    quoted = haskey(dialectkw, :quoted) ? dialectkw.quoted : true
    quotechar = haskey(dialectkw, :quotechar) ? dialectkw.quotechar : '"'
    openquotechar = haskey(dialectkw, :openquotechar) ? dialectkw.openquotechar : nothing
    quoted && delim == something(openquotechar, quotechar) && return (0.0, 0, 0, 0)
    d = Dialect(; delim, dialectkw...)
    bi = index(buf, d; datastart, parallel=false, indexkw...)
    counts = Int[]
    for ci in bi.chunks, lr in 1:totalrows(ci)
        push!(counts, nfields(ci, lr))
        length(counts) >= 11 && break
    end
    isempty(counts) && return (0.0, 0, 0, 0)
    # the modal field count is voted by the DATA rows when there are any; the
    # header row alone must not elect a delimiter (a space in "Created Date"
    # over one-word data rows) — it only has to agree with the winner
    voters = length(counts) > 1 ? counts[2:end] : counts
    modal = argmax(c -> count(==(c), voters), unique(voters))
    return (count(==(modal), voters) / length(voters), modal, first(counts), length(counts))
end

function _detectdelim(sample::Vector{UInt8}, dialectkw::NamedTuple, indexkw::NamedTuple)
    # Validate user syntax once. Candidate-only quote collisions are skipped in
    # `_scoredelim`; all other invalid options must reach the caller.
    d0 = Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    datastart = _datastart(sample)
    # scoring reads at most 11 rows per candidate, but indexes whatever it is
    # given — trim to the first 12 rows once (row boundaries are quote-aware
    # and delimiter-independent) so candidates don't each index the full sample
    stop, rows = datastart, 0
    while stop <= length(sample) && rows < 12
        stop = nextrowstart(sample, stop, length(sample), d0, false, true)
        rows += 1
    end
    scoresample = stop > length(sample) ? sample : sample[1:stop - 1]
    best, bestdelim = (false, false, 0.0, 0), first(DELIM_CANDIDATES)
    headercandidate = false
    for c in DELIM_CANDIDATES
        consistency, fields, firstfields, nrows = _scoredelim(scoresample, c, datastart,
                                                              dialectkw, indexkw)
        # a real delimiter splits the FIRST row and the data rows the same way:
        # a candidate that only appears in the header (a space in "Created
        # Date" over one-word data rows) is not represented in the data
        represented = firstfields > 1 && fields == firstfields
        # more fields only wins when the DATA rows established it: with a single
        # row (or an all-header sample) the field count is no evidence at all —
        # a one-line "\"a, b\", \"c\"" must not elect the space — so candidates
        # then tie on consistency and CSV.jl's candidate order decides
        evidence = nrows >= 2 && consistency > 0 && fields > 1
        # 0.10 tier order: a candidate PRESENT IN THE HEADER outranks one that
        # only appears in the data ("A;B;C" over "1,1,10" rows keeps ';')
        inheader = firstfields > 1
        headercandidate |= inheader
        score = represented && nrows >= 2 ?
                    (true, inheader, consistency, evidence ? fields : 0) :
                evidence ?
                    (true, false, consistency, evidence ? fields : 0) :   # data-only candidate
                    (false, false, 0.0, 0)
        if score > best
            best, bestdelim = score, c
        end
    end
    # a candidate that structures the header AND the data rows won above; the
    # remaining cases (delimiter only in the header, or the sample too short
    # for field-consistency evidence) follow 0.10's byte-count tiers exactly,
    # so files that detected one way for years keep detecting that way
    delim = (best[1] && best[2]) ? bestdelim :
            _detectdelim_bytecounts(scoresample, datastart, dialectkw, bestdelim,
                                    best[1] && headercandidate)
    # Space-ALIGNED files (#853): a run of blanks between fields is one
    # separator. Score the (' ', ignorerepeated=true) reading last, and elect it
    # only when it cannot change a file that detected before: the plain space
    # won (so the file was going to be space-delimited anyway — with a column
    # per blank), and the repeated reading is at least as consistent with
    # fewer, ≥2 fields; or nothing structured the sample at all.
    if !get(dialectkw, :ignorerepeated, false)
        cons, fields, firstfields, nrows = _scoredelim(scoresample, ' ', datastart,
                                                       merge(dialectkw, (; ignorerepeated=true)),
                                                       indexkw)
        aligned = nrows >= 2 && cons > 0 && fields > 1 && fields == firstfields
        if aligned && (delim == ' ' || !best[1])
            plaincons, plainfields, _, _ = delim == ' ' ?
                _scoredelim(scoresample, ' ', datastart, dialectkw, indexkw) : (0.0, typemax(Int), 0, 0)
            (cons >= plaincons && fields < plainfields) && return (' ', true)
        end
    end
    return (delim, false)
end

# 0.10's detector (detection.jl): count candidate bytes outside quotes over the
# header row and up to 10 data rows; tier 1 = present in header AND total count
# divisible by nlines; tier 2 = divisible by nlines; tier 3 = most frequent in
# the header, SPACE excluded; else ','. A one-row sample goes directly to tier
# 3 so a header phrase cannot elect space. `fallback` is the consistency
# scorer's data-only pick, used when it found real data evidence.
function _detectdelim_bytecounts(sample::Vector{UInt8}, datastart::Int, dialectkw::NamedTuple,
                                 fallback::Char, havedataevidence::Bool)
    quotechar = haskey(dialectkw, :quotechar) ? dialectkw.quotechar : '"'
    oq = UInt8(something(haskey(dialectkw, :openquotechar) ? dialectkw.openquotechar : nothing, quotechar))
    cq = UInt8(something(haskey(dialectkw, :closequotechar) ? dialectkw.closequotechar : nothing, quotechar))
    eq = UInt8(something(haskey(dialectkw, :escapechar) ? dialectkw.escapechar : nothing, Char(cq)))
    len = length(sample)
    hcounts = zeros(Int, 256); counts = zeros(Int, 256)
    pos = datastart; nlines = 0; inheader = true; parsedany = false; lastnl = false
    while pos <= len && nlines < 11
        parsedany = true
        b = sample[pos]; pos += 1
        if b == oq
            while pos <= len
                b = sample[pos]; pos += 1
                if b == eq
                    pos > len && break
                    (eq == cq && sample[pos] != cq) && break
                    pos += 1
                elseif b == cq
                    break
                end
            end
        elseif b == UInt8('\n') || b == UInt8('\r')
            b == UInt8('\r') && pos <= len && sample[pos] == UInt8('\n') && (pos += 1)
            nlines += 1; lastnl = true; inheader = false
        else
            lastnl = false
            inheader && (hcounts[b + 1] += 1)
            counts[b + 1] += 1
        end
    end
    nlines += parsedany && !lastnl
    nlines == 0 && return ','
    cands = (',', '\t', ' ', '|', ';', ':')
    # A single row can only be a header. Space is not evidence there (for
    # example `Created Date`); use the old detector's header-only tier, which
    # deliberately excludes space, and otherwise retain the comma default.
    if nlines == 1
        bestc, bestn = ',', 0
        for c in (',', '\t', '|', ';', ':')
            n = hcounts[UInt8(c) + 1]
            if n > bestn
                bestc, bestn = c, n
            end
        end
        return bestc
    end
    for c in cands   # tier 1
        h = hcounts[UInt8(c) + 1]; n = counts[UInt8(c) + 1]
        h > 0 && n > 0 && n % nlines == 0 && return c
    end
    for c in cands   # tier 2
        n = counts[UInt8(c) + 1]
        n > 0 && n % nlines == 0 && return c
    end
    bestc, bestn = ',', 0
    for c in (',', '\t', '|', ';', ':')   # tier 3: header max, no space
        n = hcounts[UInt8(c) + 1]
        if n > bestn            # NOT `cond && (a, b = c, d)`: that parses as a tuple
            bestc, bestn = c, n
        end
    end
    bestn > 0 && return bestc
    return havedataevidence ? fallback : ','
end

"""
    CSV.sniff(source; samplebytes=65536, kw...) -> Spec

Detect the delimiter (quote-aware field-count consistency over a bounded
sample, candidates $(DELIM_CANDIDATES) in CSV.jl's order), whether a header
row is likely (row 1 all text while later rows type differently), and the
resulting names/types. `samplebytes` is the initial sample size; a sample too
small to hold even one complete row grows until it does. `kw` may pin dialect, value, and index pieces
(`quotechar`, `comment`, `decimal`, `scanner`, ...) that sniffing should use.
`buffer_in_memory=true` copies a file source instead of mapping it.
"""
function sniff(source; samplebytes::Int=1 << 16, missingstring=nothing,
               buffer_in_memory::Bool=false, prefetch::Bool=true, kw...)
    allowed = (_DIALECTKW..., _VALUEKW..., _INDEXKW..., _DRIVERKW...)
    _checkkwargs("sniff", kw, allowed)
    dialectkw = _pickkwargs(kw, _DIALECTKW)
    valuekw = _pickkwargs(kw, _VALUEKW)
    indexkw = _pickkwargs(kw, _INDEXKW)
    driverkw = _pickkwargs(kw, _DRIVERKW)
    buf = resolvesource(source; buffer_in_memory, prefetch)
    sample = _sample(buf, samplebytes; dialectkw...)
    bestdelim, ir = _detectdelim(sample, dialectkw, indexkw)
    ir && (dialectkw = merge(dialectkw, (; ignorerepeated=true)))
    sentinels = _sentinels(missingstring)
    parsekw = merge(dialectkw, valuekw, indexkw, driverkw,
                    (; delim=bestdelim, sentinels, limit=100, parallel=false))
    # header detection: parse the sample twice — types with row 1 as data vs
    # header. A likely header = row 1 headerless-types degrade to String while
    # the with-header types do not (numbers under a text row 1).
    theader = parse(sample; header=true, parsekw...)
    tnoheader = parse(sample; header=false, parsekw...)
    headerlikely = tnoheader.nrows > theader.nrows &&
        any(zip(columns(theader), columns(tnoheader))) do (ch, cnh)
            Base.nonmissingtype(eltype(ch)) !== String && eltype(ch) !== Missing &&
                Base.nonmissingtype(eltype(cnh)) in (String, CompactString)
        end
    t = headerlikely ? theader : tnoheader
    return Spec(bestdelim, get(dialectkw, :quoted, true), headerlikely,
                length(names(t)), copy(names(t)), Type[eltype(c) for c in columns(t)])
end

# delimiter-only sniff for File(delim=nothing) — no second parse
# -> (delim, ignorerepeated)
function _sniffdelim(buf::Vector{UInt8}, samplebytes::Int,
                     dialectkw::NamedTuple, indexkw::NamedTuple; start::Int=1)
    sample = _sample(buf, samplebytes; start, dialectkw...)
    return _detectdelim(sample, dialectkw, indexkw)
end

# ---------------------------------------------------------------------------
# the shared front end
# ---------------------------------------------------------------------------
# Everything row-positional is settled BEFORE any value work, and always in
# RAW structural rows — quote-aware, counting comment and empty lines exactly
# as CSV.jl does (pinned by probe: a comment line between header and skipto
# still counts). Numbered headers shift `datastart` so skipped prefix rows
# never even enter the index; `skipto` advances `firstdatarow` by byte
# offset, so hygiene-dropped rows cannot skew the count.

# Public row/field positions accept any Integer for 0.10 compatibility. Keep
# oversized UInt/BigInt values as an unreachable sentinel instead of narrowing
# them before the source geometry is known. The saturated successor is needed
# for `header + 1` and EOF positions at the machine-Int boundary.
@inline _saturatedint(x::Integer) = x > typemax(Int) ? typemax(Int) :
                                    x < typemin(Int) ? typemin(Int) : Int(x)
@inline _saturatedinc(x::Int) = x == typemax(Int) ? x : x + 1

# byte offset of PHYSICAL line `n` (1-based from `start`): CR, LF, or CRLF end a
# line and quotes mean nothing. This is how skipped PREFIX rows are counted —
# rows before a numbered header, or before `skipto` when there is no header
# row — because a stray quote in a junk preamble must not swallow the file
# (issues #1012/#1079/#1160; polars' skip_lines has the same semantics).
function _physicallineoffset(buf::Vector{UInt8}, start::Int, n::Int)
    off = start
    len = length(buf)
    for _ in 1:(n - 1)
        off > len && return _saturatedinc(len)
        @inbounds while off <= len
            b = buf[off]
            if b == UInt8('\n')
                off += 1
                break
            elseif b == UInt8('\r')
                off += 1 + (off < len && buf[off + 1] == UInt8('\n'))
                break
            end
            off += 1
        end
    end
    return off
end

# byte offset of raw structural row `n` (1-based from `datastart`)
function _rawrowoffset(buf::Vector{UInt8}, d::Dialect, datastart::Int, n::Int)
    off = datastart
    for _ in 1:(n - 1)
        off > length(buf) && return _saturatedinc(length(buf))
        off = nextrowstart(buf, off, length(buf), d, false, true)
    end
    return off
end

# advance chunks past every row starting before `byteoff`
function _skiptobyte!(chunks::Vector{ChunkIndex}, byteoff::Int)
    for ci in chunks
        while nrows(ci) > 0 &&
              ci.start + Int(ci.rowstartrel[ci.firstdatarow]) < byteoff
            ci.firstdatarow += 1
        end
    end
end

function _iscommentrow(buf::Vector{UInt8}, rowstart::Int, d::Dialect)
    cmt = d.comment
    cmt === nothing && return false
    rowstart + length(cmt) - 1 <= length(buf) || return false
    @inbounds for j in eachindex(cmt)
        buf[rowstart + j - 1] == cmt[j] || return false
    end
    return true
end

# Byte start of the first raw footer row. Empty rows count even when hygiene
# drops them; comment rows do not count, matching CSV.jl's reverse scan.
function _footeroffset(buf::Vector{UInt8}, d::Dialect, rawstart::Int, footerskip::Int)
    footerskip == 0 && return _saturatedinc(length(buf))
    # Count first, then locate the first footer row. This is two structural
    # scans but constant memory; a ring of `footerskip` Ints lets a valid public
    # option allocate many GiB before discovering that the file has fewer rows.
    nrows = 0
    rowstart = rawstart
    while rowstart <= length(buf)
        !_iscommentrow(buf, rowstart, d) && (nrows += 1)
        rowstart = nextrowstart(buf, rowstart, length(buf), d, false, true)
    end
    footerskip >= nrows && return rawstart
    target = nrows - footerskip + 1
    seen = 0
    rowstart = rawstart
    while rowstart <= length(buf)
        if !_iscommentrow(buf, rowstart, d)
            seen += 1
            seen == target && return rowstart
        end
        rowstart = nextrowstart(buf, rowstart, length(buf), d, false, true)
    end
    return rawstart # target is guaranteed by the count pass
end

function _rowsbefore(chunks::Vector{ChunkIndex}, byteoff::Int)
    n = 0
    for ci in chunks, lr in ci.firstdatarow:totalrows(ci)
        ci.start + Int(ci.rowstartrel[lr]) < byteoff && (n += 1)
    end
    return n
end

function _limitrows!(chunks::Vector{ChunkIndex}, limit::Int)
    remaining = limit
    for ci in chunks
        n = nrows(ci)
        if remaining >= n
            remaining -= n
        elseif remaining > 0
            lastrow = ci.firstdatarow + remaining - 1
            resize!(ci.rowfirst, lastrow + 1)
            resize!(ci.rowstartrel, lastrow)
            remaining = 0
        else
            ci.firstdatarow = totalrows(ci) + 1
        end
    end
    filter!(ci -> nrows(ci) > 0, chunks)
    return chunks
end

_firstlive(chunks) = findfirst(ci -> nrows(ci) > 0, chunks)

struct Prepared
    buf::Vector{UInt8}
    bi::BufferIndex
    names::Vector{Symbol}
    ncols::Int
    limit::Union{Nothing, Int}
    opts::ValueOpts
    d::Dialect
    headerlog::ProblemLog
    # Header rows are consumed from the structural index during preparation.
    # Retain their compact structural locations so a later File(::LazyFile)
    # can replay diagnostics at its own cap without retaining every malformed
    # header field in memory.
    headerrefs::Vector{Tuple{ChunkIndex, Int}}
    parsekw::NamedTuple   # dialect + value + engine kwargs, ready to splat into parse
end

function _headerproblems(buf::Vector{UInt8}, refs::Vector{Tuple{ChunkIndex, Int}},
                         opts::ValueOpts, cap::Int)
    log = ProblemLog(cap)
    for (ci, hrow) in refs, j in 1:nfields(ci, hrow)
        pos, len = fieldspan(ci, hrow, j)::Tuple{Int, Int}
        len == 0 && continue
        cpos, clen, _, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            pushproblem!(log, 0, j, pos, :invalid_quoted_field,
                           "malformed quoting in header " * excerpt(buf, pos, len))
        elseif st != CELL_MISSING && clen != 0 &&
               !_wasquoted(buf, pos, len, opts) &&
               _delimclash(buf, cpos, clen, opts.delim)
            pushproblem!(log, 0, j, pos, :invalid_value,
                           "bare quote engaged structural protection in header " *
                           excerpt(buf, pos, len))
        end
    end
    sortproblems!(log)
    return log
end

function _prepare(source;
                  header::Union{Bool, Integer, AbstractVector}=1,
                  normalizenames::Bool=false,
                  skipto::Union{Nothing, Integer}=nothing,
                  footerskip::Integer=0,
                  missingstring=nothing,
                  delim=nothing,
                  limit::Union{Nothing, Integer}=nothing,
                  samplebytes::Int=1 << 16,
                  chunkbytes::Union{Nothing, Int}=nothing,
                  parallel::Bool=Threads.nthreads() > 1,
                  ntasks::Union{Nothing, Int}=nothing,
                  buffer_in_memory::Bool=false,
                  prefetch::Bool=true,
                  validate::Bool=true,
                  kw...)
    header isa Integer && header < 0 &&
        throw(ArgumentError("header must be ≥ 0 (got $header)"))
    if header isa AbstractVector{<:Integer} && !isempty(header)
        (issorted(header) && first(header) >= 1 && allunique(header)) ||
            throw(ArgumentError("header rows must be increasing and ≥ 1 (got $header)"))
    end
    skipto === nothing || skipto >= 1 ||
        throw(ArgumentError("skipto must be ≥ 1 (got $skipto)"))
    footerskip >= 0 || throw(ArgumentError("footerskip must be ≥ 0 (got $footerskip)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    allowed = (_DIALECTKW..., _VALUEKW..., _INDEXKW..., _DRIVERKW...)
    _checkkwargs("File/Rows/Chunks", kw, allowed)
    # 0.10 rule: the default header row 1 with skipto=1 means "no header, data
    # starts at row 1" (the header row and the first data row cannot coincide).
    if header isa Integer && header == 1 && skipto !== nothing && skipto == 1
        header = false
    end
    rawheaderrow = header === true ? 1 : header === false ? 0 :
                   header isa Integer ? header :
                   header isa AbstractVector{<:Integer} && !isempty(header) ? last(header) : 0
    buf = resolvesource(source; buffer_in_memory, prefetch)
    dialectonly = _pickkwargs(kw, _DIALECTKW)
    indexonly = _pickkwargs(kw, _INDEXKW)
    # The first row that MATTERS — the (first) header row, or `skipto` when
    # there is no header row. Everything before it is a skipped prefix: counted
    # as physical lines (quote-blind), never indexed, never sniffed. Row
    # offsets at or after it are quote-aware from that anchor.
    firstrow = header isa Integer && header > 1 ? _saturatedint(header) :
               header isa AbstractVector{<:Integer} && !isempty(header) ?
               _saturatedint(first(header)) :
               (header === false || (header isa Integer && header == 0) ||
                (header isa AbstractVector && !(header isa AbstractVector{<:Integer}))) &&
               skipto !== nothing ? _saturatedint(skipto) : 1
    rawstart = _datastart(buf)
    anchoroff = firstrow > 1 ? _physicallineoffset(buf, rawstart, firstrow) : rawstart
    if delim === nothing
        get(kw, :ignorerepeated, false) &&
            throw(ArgumentError("auto-delimiter detection is not supported with " *
                                "ignorerepeated=true; pass delim explicitly"))
        # Sniff from the first row that matters: skipped prefix rows are junk
        # and must not vote on the delimiter (a one-line "skip me" preamble
        # otherwise elects the space).
        delim, ir = _sniffdelim(buf, samplebytes, dialectonly, indexonly; start=anchoroff)
        # an elected (' ', ignorerepeated=true) reading becomes the dialect
        ir && (dialectonly = merge(dialectonly, (; ignorerepeated=true));
               kw = merge(NamedTuple(kw), (; ignorerepeated=true)))
    end
    # missingstring → kernel sentinels ("" entries are inert: empty is always missing)
    sentinels = _sentinels(missingstring)
    valuekw = _pickkwargs(kw, _VALUEKW)
    # per-column dateformat: a Dict defers to per-column ValueOpts built once
    # the names are known; the base opts (header parsing, sniffing) go without
    dfdict = nothing
    if haskey(valuekw, :dateformat) && valuekw.dateformat isa AbstractDict
        dfdict = valuekw.dateformat
        valuekw = NamedTuple(kv for kv in pairs(valuekw) if kv.first != :dateformat)
    end
    d = Dialect(; delim, dialectonly...)
    opts = makevalueopts(d; sentinels, valuekw...)
    cb = chunkbytes === nothing ?
         (ntasks === nothing ? _defaultchunkbytes(length(buf)) :
          min(max(cld(length(buf), ntasks), 1), 1 << 30)) : chunkbytes

    # -- the row window, in RAW rows: header rows, skipto, footerskip ---------
    header isa Integer && !(header isa Bool) &&
        (header = header == 0 ? false : _saturatedint(header))
    headerrows = header isa AbstractVector{<:Integer} ? _saturatedint.(header) :
                 header isa Int ? [header] : Int[]
    headerrow = header === true ? 1 : isempty(headerrows) ? 0 : last(headerrows)
    # Skipped prefix rows never enter the index (a generated column count would
    # otherwise come from a junk first row; 0.10 took it from the first DATA
    # row) — the index starts at the anchor. Row `n` at/after the anchor is
    # `n - firstrow + 1` quote-aware structural rows from it.
    datastart = anchoroff
    rowoff(n::Int) = n < firstrow ? _physicallineoffset(buf, rawstart, n) :
                                    _rawrowoffset(buf, d, anchoroff, n - firstrow + 1)
    bi = index(buf, d; datastart, chunkbytes=cb, parallel, ntasks, indexonly...)
    chunks = bi.chunks
    headerlog = ProblemLog(get(kw, :maxproblems, 10_000))
    headerrefs = Tuple{ChunkIndex, Int}[]

    names = if header isa AbstractVector && !(header isa AbstractVector{<:Integer}) &&
               !isempty(header)
        Symbol.(header)
    elseif header === false || isempty(chunks) ||
           (header isa AbstractVector && isempty(header))   # header=[] ⇒ generate ColumnN
        k = _firstlive(chunks)
        n = k === nothing ? 0 : nfields(chunks[k], chunks[k].firstdatarow)
        [Symbol("Column", j) for j in 1:n]
    elseif header === true || length(headerrows) == 1
        k = _firstlive(chunks)
        if k === nothing
            Symbol[]
        else
            push!(headerrefs, (chunks[k], chunks[k].firstdatarow))
            parseheader!(buf, chunks[k], opts, d, headerlog)
        end
    else
        # multi-row header: the LISTED raw rows (not necessarily consecutive —
        # blank rows may sit between them) join with "_"; blank cells resolve
        # to ColumnN first — pinned against CSV.jl. Each listed row is parsed
        # in place by advancing the chunk cursor to that raw row's byte offset.
        parts = Vector{Vector{Symbol}}()
        firstrows = Int[ci.firstdatarow for ci in chunks]
        for hr in headerrows
            _skiptobyte!(chunks, rowoff(hr))
            k = _firstlive(chunks)
            k === nothing && break
            push!(headerrefs, (chunks[k], chunks[k].firstdatarow))
            push!(parts, parseheader!(buf, chunks[k], opts, d, headerlog))
        end
        for (ci, firstrow) in zip(chunks, firstrows)
            ci.firstdatarow = firstrow
        end
        _skiptobyte!(chunks, rowoff(_saturatedinc(headerrow)))
        if isempty(parts)
            Symbol[]
        else
            n = maximum(length, parts)
            [Symbol(join((j <= length(p) ? String(p[j]) : "Column$j" for p in parts), "_"))
             for j in 1:n]
        end
    end
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = makeunique!(names)

    if skipto !== nothing
        skipto > rawheaderrow ||
            throw(ArgumentError("skipto=$skipto must be past the header (row $rawheaderrow)"))
        _skiptobyte!(chunks, rowoff(_saturatedint(skipto)))
    end
    # A non-comment physical row consumes at least one source byte. A footer
    # count larger than the buffer therefore removes every possible row; avoid
    # narrowing the count or scanning the source in that known-empty case.
    footer = footerskip > 0 && footerskip >= length(buf) ? rawstart :
             _footeroffset(buf, d, rawstart, Int(footerskip))
    keep = footerskip == 0 ? sum(nrows, chunks; init=0) : _rowsbefore(chunks, footer)
    lim = limit === nothing ? (footerskip > 0 ? keep : nothing) :
          limit >= keep ? keep : Int(limit)

    # engine + diagnostics kwargs the kernel driver consumes directly
    colopts = nothing
    if dfdict !== nothing
        overrides = _resolvekeys(dfdict, names, length(names), "dateformat"; validate)
        colopts = ValueOpts[haskey(overrides, j) ?
                              makevalueopts(d; sentinels, valuekw...,
                                              dateformat=overrides[j]) : opts
                              for j in 1:length(names)]
    end
    passthrough = _pickkwargs(kw, allowed)
    dfdict !== nothing &&
        (passthrough = NamedTuple(kv for kv in pairs(passthrough) if kv.first != :dateformat))
    parsekw = merge(passthrough,
                    (; delim, sentinels, chunkbytes=cb, parallel, ntasks, colopts, validate))
    return Prepared(buf, bi, names, length(names), lim, opts, d, headerlog, headerrefs, parsekw)
end

# kwargs _prepare consumes itself (not forwarded to the kernel driver)
const _PREPKW = (:header, :normalizenames, :skipto, :footerskip, :missingstring,
                 :delim, :limit, :samplebytes, :chunkbytes, :parallel,
                 :buffer_in_memory, :prefetch, :validate)

# select/drop: list forms only (function forms retired — Tables.Scan is the
# expression channel). Returns a kernel `select` Int vector or nothing.
function _resolveselect(select, drop, names::Vector{Symbol})
    select !== nothing && drop !== nothing &&
        throw(ArgumentError("select and drop are mutually exclusive"))
    spec = select === nothing ? drop : select
    spec === nothing && return nothing
    spec isa Base.Callable &&
        throw(ArgumentError("function-typed select/drop is retired; pass a list " *
                            "(or use Tables.Scan for expressions)"))
    idx = if spec isa AbstractVector{Bool}
        length(spec) == length(names) ||
            throw(ArgumentError("Bool select/drop length $(length(spec)) != $(length(names)) columns"))
        findall(spec)
    elseif spec isa AbstractVector{<:Integer}
        Int.(spec)
    else
        map(spec) do s
            # match the name as spelled OR as `normalizenames` would spell it
            # (#990: users pass the raw header text after normalization)
            j = findfirst(==(Symbol(s)), names)
            j === nothing && (j = findfirst(==(Symbol(normalizename(String(s)))), names))
            j === nothing && throw(ArgumentError("select/drop name $s does not match any column"))
            j
        end
    end
    all(j -> 1 <= j <= length(names), idx) ||
        throw(ArgumentError("select/drop index out of range"))
    # All classic reader doors use file-order, unique projection semantics.
    # This matches the kernel and CSV 0.10 even when callers repeat or reorder
    # entries in a list.
    return drop === nothing ? sort!(unique(idx)) : setdiff(1:length(names), idx)
end

# ---------------------------------------------------------------------------
# File — the eager table
# ---------------------------------------------------------------------------

# Keep the row independent of the concrete File definition so File can retain
# its long-standing `AbstractVector{FileRow}` contract without a recursive type
# declaration. Each row stores only references to the shared schema/columns.
struct FileRow <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol, Int}
    row::Int
end

struct File <: AbstractVector{FileRow}
    name::String
    table::ParsedTable
    lookup::Dict{Symbol, Int}
end

function File(source;
              types=nothing, select=nothing, drop=nothing,
              scan=nothing,
              pool=DEFAULT_POOL,
              downcast::Bool=false,
              transpose::Bool=false,
              stringtype::Type=CompactString,
              strict::Bool=false, on_error::Symbol=strict ? :error : :collect,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              validate::Bool=true,
              kw...)
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkstringtype(stringtype)
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    if transpose
        (select !== nothing || drop !== nothing) &&
            throw(ArgumentError("select/drop are not supported with transpose=true"))
        scan === nothing || throw(ArgumentError("scan is not supported with transpose=true"))
        return _transposedfile(source; types, pool, downcast, stringtype, on_error,
                               maxproblems, validate, kw...)
    end
    capturecap = max(maxproblems, 1)
    if scan !== nothing
        # -- Tables.Scan pushdown: the scan owns selection, types, and row
        # bounds; the classic keywords for those axes are refused rather than
        # merged, so a request means one thing --------------------------------
        (isdefined(Tables, :Scan) && scan isa Tables.Scan) ||
            throw(ArgumentError("scan must be a Tables.Scan (got $(typeof(scan)))"))
        select === nothing && drop === nothing ||
            throw(ArgumentError("pass the column selection through the Scan, not select=/drop="))
        types === nothing ||
            throw(ArgumentError("pass column types through the Scan's select items (`:col => T`), not types="))
        haskey(kw, :limit) &&
            throw(ArgumentError("pass the row limit through the Scan, not limit="))
        p = _prepare(source; parallel, ntasks, maxproblems=capturecap, validate, kw...)
        t = _executescan(p, scan; parsekw=p.parsekw, maxproblems, on_error)
        # pool keys name the scan's OUTPUT columns (the request already renamed
        # and reordered them)
        t = _poolcolumns(t, _resolvepool(pool, names(t), length(names(t)); validate); parallel)
        poolS = stringtype === CompactString ? String : stringtype
        t = _pooledarrays(t, poolS)
        downcast && (t = _downcast(t))
        stringtype === CompactString || (t = _materializestrings(t, stringtype))
        return File(_sourcename(source), t, Dict(n => j for (j, n) in enumerate(names(t))))
    end
    p = _prepare(source; parallel, ntasks, maxproblems=capturecap, validate, kw...)
    return _filefromprepared(p, _sourcename(source); types, select, drop, pool, downcast,
                             stringtype, on_error, maxproblems, parallel, ntasks, validate)
end

_sourcename(source) = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
_sourceprovenance(source, i::Int) =
    source isa AbstractString ? String(source) : "<source $i>"

# the typed parse over an existing index — File's classic path, and what
# File(::LazyFile) reuses so the scan is never repeated
function _remapcolumnspec(spec, names::Vector{Symbol}, sourceindices::Vector{Int},
                          fulln::Int, what::String; validate::Bool=true)
    spec === nothing && return nothing
    n = length(names)
    if spec isa Type
        return Dict(sourceindices[j] => spec for j in 1:n)
    elseif spec isa AbstractVector
        length(spec) == n ||
            throw(ArgumentError("$what vector length $(length(spec)) != $n columns"))
        return Dict(sourceindices[j] => spec[j] for j in 1:n)
    elseif spec isa AbstractDict
        resolved = _resolvekeys(spec, names, n, what; validate)
        return Dict(sourceindices[j] => value for (j, value) in resolved)
    end
    throw(ArgumentError("unsupported $what specification: $(typeof(spec))"))
end

function _filefromprepared(p::Prepared, nm::String; types=nothing, select=nothing, drop=nothing,
                           pool=DEFAULT_POOL, downcast::Bool=false, stringtype::Type=CompactString,
                           on_error::Symbol=:collect, maxproblems::Int=10_000,
                           parallel::Bool=Threads.nthreads() > 1, validate::Bool=true,
                           ntasks::Union{Nothing, Int}=nothing,
                           available::Union{Nothing, Vector{Int}}=nothing)
    sourceindices = available === nothing ? collect(1:p.ncols) : available
    viewnames = p.names[sourceindices]
    localsel = _resolveselect(select, drop, viewnames)
    keep = localsel === nothing ? collect(eachindex(sourceindices)) : localsel
    sel = sourceindices[keep]
    fulltypes = available === nothing ? types :
                _remapcolumnspec(types, viewnames, sourceindices, p.ncols, "types"; validate)
    parsetypes = if p.limit == 0
        Type[T === nothing ? Missing : T
             for T in resolvetypes(fulltypes, p.names, p.ncols; validate)]
    else
        fulltypes
    end
    # File(::LazyFile) resolves pool keys against the columns exposed by the
    # LazyFile, not against columns that were already dropped.
    poolspecs = _resolvepool(pool, viewnames, length(viewnames); validate)
    # Prepared records the options used to build the index, but value parsing
    # belongs to this File call. Override every value-driver option that this
    # method exposes; in particular, a LazyFile prepared with defaults must not
    # silently cap a later larger maxproblems request at 10,000.
    parsekw = merge(p.parsekw,
                    (; parallel, ntasks, validate, maxproblems=max(maxproblems, 1)))
    t = parse(p.buf; index=p.bi, header=p.names, types=parsetypes, select=sel, limit=p.limit,
                on_error=:collect, parsekw...)
    headerlog = _headerproblems(p.buf, p.headerrefs, p.opts, max(maxproblems, 1))
    t, firstproblem = _mergeproblems(t, headerlog, maxproblems)
    t, firstproblem = _narrowtypes(t,
        requestedtypes(fulltypes, p.names, p.ncols; validate), sel,
        p.bi.chunks, maxproblems, firstproblem)
    on_error === :error && firstproblem !== nothing && _throwproblem(t, firstproblem)
    t = _poolcolumns(t, poolspecs[keep]; parallel)
    poolS = stringtype === CompactString ? String : stringtype   # pool levels are never views
    t = _pooledarrays(t, poolS)
    downcast && (t = _downcast(t))
    stringtype === CompactString || (t = _materializestrings(t, stringtype))
    return File(nm, t, Dict(n => j for (j, n) in enumerate(names(t))))
end

# The optional scan implementation is included after this file. A Tables
# version without Scan never reaches this call because the type check above
# fails first with a clear error.
function _executescan(p::Prepared, scan; parsekw, maxproblems::Int, on_error::Symbol)
    return _executescanplan(p.buf, p.bi, p.names, scan;
                            parsekw, headerlog=p.headerlog,
                            maxproblems, on_error)
end

function File(sources::AbstractVector; source=nothing, kw...)
    if eltype(sources) === UInt8
        # a byte buffer IS a vector; route it to the single-source door
        source === nothing ||
            throw(ArgumentError("source= requires a vector of sources, not a byte buffer"))
        return invoke(File, Tuple{Any}, sources; kw...)
    end
    isempty(sources) &&
        throw(ArgumentError("unable to read delimited data from an empty sources vector"))
    if source isa Pair
        (source.first isa Symbol || source.first isa AbstractString) &&
            source.second isa AbstractVector ||
            throw(ArgumentError("source must be a column name or name => values pair"))
        length(source.second) == length(sources) ||
            throw(ArgumentError("source label list has $(length(source.second)) entries " *
                                "for $(length(sources)) sources"))
    elseif !(source === nothing || source isa Symbol || source isa AbstractString)
        throw(ArgumentError("source must be a column name or name => values pair"))
    end
    source === nothing && length(sources) == 1 && return File(first(sources); kw...)
    strict = get(kw, :strict, false)
    on_error = get(kw, :on_error, strict ? :error : :collect)
    maxwarnings = get(kw, :maxwarnings, nothing)
    maxproblems = haskey(kw, :maxproblems) ? get(kw, :maxproblems, 10_000) :
                  something(maxwarnings, 10_000)
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    capturecap = max(maxproblems, 1)
    # Parse children in collecting mode, then apply one diagnostic cap and one
    # strict decision to the logical concatenated input. Otherwise N sources
    # could retain N * maxproblems entries and a later source could throw before
    # the globally first problem was known.
    childkw = merge(NamedTuple(kw), (; strict=false, on_error=:collect,
                                     maxwarnings=nothing, maxproblems=capturecap))
    files = [File(s; childkw...) for s in sources]
    counts = [getfield(f, :table).nrows for f in files]
    total = sum(counts)
    outnames = copy(names(getfield(files[1], :table)))
    cols = AbstractVector[_chaincolumn(
        AbstractVector[_colpiece(f, nm) for f in files], counts, total) for nm in outnames]
    if source !== nothing
        srcname = Symbol(source isa Pair ? source.first : source)
        srcname in outnames &&
            throw(ArgumentError("source column name $srcname collides with a data column"))
        vals = source isa Pair ? source.second :
               [_sourceprovenance(s, i) for (i, s) in enumerate(sources)]
        expanded = eltype(vals)[vals[i] for i in eachindex(files) for _ in 1:counts[i]]
        push!(outnames, srcname)
        push!(cols, PooledArray(expanded))
    end
    log = ProblemLog(maxproblems)
    off = 0
    for f in files
        t = getfield(f, :table)
        for pr in t.problems
            adjusted = Problem(pr.row == 0 ? 0 : pr.row + off,
                                 pr.col, pr.pos, pr.kind, pr.message)
            log.first === nothing && (log.first = adjusted)
            if length(log.items) < log.limit
                push!(log.items, adjusted)
            else
                log.dropped += 1
            end
        end
        log.dropped += t.droppedproblems
        off += t.nrows
    end
    t = ParsedTable(outnames, cols, total, log.items, log.dropped)
    on_error === :error && log.first !== nothing && _throwproblem(t, log.first)
    return File("<$(length(sources)) sources>", t,
                Dict(n => j for (j, n) in enumerate(outnames)))
end

# a source that lacks a column contributes an all-missing block
const EMPTY_COLUMN = Union{}[]

function _colpiece(f::File, nm::Symbol)
    j = get(getfield(f, :lookup), nm, 0)
    return j == 0 ? EMPTY_COLUMN : getfield(f, :table).columns[j]
end

# Concatenate one column's per-source pieces (EMPTY_COLUMN ⇒ the source lacks
# the column: all-missing block). Element types promote across sources; any
# string type concatenates as String — the result owns its memory.
function _chaincolumn(pieces::Vector{AbstractVector}, counts::Vector{Int}, total::Int)
    T = Union{}
    anymissing = false
    pooled = false
    for (c, n) in zip(pieces, counts)
        if c === EMPTY_COLUMN
            n > 0 && (anymissing = true)
        else
            pooled |= c isa PooledArray
            et = eltype(c)
            anymissing |= Missing <: et
            S = Base.nonmissingtype(et)   # Union{} for an all-missing column
            S === Union{} || (T = promote_type(T, S <: AbstractString ? String : S))
        end
    end
    T === Union{} && return fill(missing, total)   # every source's block is all-missing
    E = anymissing ? Union{T, Missing} : T
    out = Vector{E}(undef, total)
    off = 0
    for (c, n) in zip(pieces, counts)
        if c === EMPTY_COLUMN
            fill!(view(out, off+1:off+n), missing)
        else
            S = Base.nonmissingtype(eltype(c))
            if S !== Union{} && S <: AbstractString
                @inbounds for (k, x) in enumerate(c)
                    out[off+k] = x === missing ? missing : String(x)
                end
            else
                copyto!(out, off + 1, c, 1, n)
            end
        end
        off += n
    end
    return pooled ? PooledArray(out) : out
end

function _mergeproblems(t::ParsedTable, headerlog::Union{Nothing, ProblemLog}, cap::Int)
    # clean parse: nothing to merge, sort, or cap — return the table as-is
    if isempty(t.problems) && t.droppedproblems == 0 &&
       (headerlog === nothing || (isempty(headerlog.items) && headerlog.dropped == 0))
        return t, nothing
    end
    log = ProblemLog(cap)
    if headerlog !== nothing
        if headerlog.first !== nothing
            first = headerlog.first
            (log.first === nothing || problemless(first, log.first)) && (log.first = first)
        end
        for pr in headerlog.items
            pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
        end
    end
    for pr in t.problems
        pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
    end
    log.dropped += t.droppedproblems +
                   (headerlog === nothing ? 0 : headerlog.dropped)
    sortproblems!(log)
    table = ParsedTable(t.names, t.columns, t.nrows, log.items, log.dropped)
    return table, log.first
end

@noinline function _throwproblem(t::ParsedTable, pr::Problem)
    nproblems = length(t.problems) + t.droppedproblems
    throw(ErrorException("CSV: $(pr.kind) at data row $(pr.row), column $(pr.col): " *
                         pr.message * (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
end

# Rows, lazy columns, and transpose parse cells directly instead of producing a
# native-width ParsedTable followed by `_narrowtypes`. Restore the user's narrow
# numeric request at those access doors; `parsevalue` still uses the native
# kernels internally and performs the checked conversion once.
function _accessparsetypes(types, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    seed = resolvetypes(types, names, ncols; validate)
    requested = requestedtypes(types, names, ncols; validate)
    for j in eachindex(seed)
        requested[j] === nothing || (seed[j] = requested[j])
    end
    return seed
end

# ---------------------------------------------------------------------------
# transpose=true — the compatibility path. Rows are columns: input row j is
# output column j; with header=true the first field of each row is that
# column's name. Types are inferred EXACTLY (every retained cell participates —
# these files are small by construction), or taken from `types`. Parsing is
# single-threaded; stringtype/pool finalize through File's common output path.
# select/drop are not supported here.
# ---------------------------------------------------------------------------
function _cellstring(buf::Vector{UInt8}, ci, lr::Int, f::Int, opts)
    sp = fieldspan(ci, lr, f)
    sp === nothing && return ""
    cpos, clen, esc, st = cellcontent(buf, sp[1], sp[2], opts)
    st == CELL_VALUE || return ""
    if esc
        tmp = UInt8[]
        _unescape_append!(tmp, buf, cpos, clen, opts.e, opts.cq)
        return String(tmp)
    end
    return String(buf[cpos:(cpos + clen - 1)])
end

function _transposedcolumn(buf::Vector{UInt8}, ci, lr::Int, startf::Int, n::Int,
                           T0, opts, log::ProblemLog, col::Int)
    nf = nfields(ci, lr)
    T = T0
    if T === nothing
        T = Missing
        for f in startf:min(nf, startf + n - 1)
            sp = fieldspan(ci, lr, f)
            sp === nothing && continue
            T = promote_kernel(T, detecttype(buf, sp[1], sp[2], opts))
        end
    end
    T === Missing && return fill(missing, n)
    if T === String
        scol = StringColumn(n, buf, opts.e, opts.cq)
        payloads = scol.payloads
        staging::Union{Nothing, NTuple{4, Vector}} = nothing
        sawmiss = nf - (startf - 1) < n
        for i in 1:min(n, nf - (startf - 1))
            f = startf + i - 1
            sp = fieldspan(ci, lr, f)
            if sp === nothing || sp[2] == 0
                sawmiss = true
                continue
            end
            cpos, clen, esc, st = cellcontent(buf, sp[1], sp[2], opts)
            if st != CELL_VALUE
                sawmiss = true
                continue
            end
            if esc
                inl = _unescape_inline(buf, cpos, clen, opts.e, opts.cq)
                if inl === nothing
                    staging === nothing &&
                        (staging = (UInt8[], Int[], Int[], Int[]))
                    _stageescaped!(staging, buf, cpos, clen, i, opts.e, opts.cq)
                else
                    payloads[i] = inl
                end
            elseif clen > COMPACTSTRING_INLINE && cpos - 1 > typemax(Int32)
                staging === nothing && (staging = (UInt8[], Int[], Int[], Int[]))
                _stageraw!(staging, buf, cpos, clen, i)
            else
                payloads[i] = clen <= COMPACTSTRING_INLINE ?
                              inline_payload(buf, cpos, clen) :
                              view_payload(buf, cpos, clen, 0, cpos - 1)
            end
        end
        staging === nothing || _flushstaging!(scol, payloads, staging)
        return finalizecolumn(String, scol, n, sawmiss)
    end
    out = Vector{Union{T, Missing}}(missing, n)
    scratch = _scratchfor(opts)
    sawmiss = nf - (startf - 1) < n
    for i in 1:min(n, nf - (startf - 1))
        f = startf + i - 1
        sp = fieldspan(ci, lr, f)
        if sp === nothing || sp[2] == 0
            sawmiss = true
            continue
        end
        cpos, clen, esc, st = cellcontent(buf, sp[1], sp[2], opts)
            if st != CELL_VALUE || esc || clen == 0
                st == CELL_VALUE && (esc || clen == 0) && T0 === nothing &&
                return _transposedcolumn(buf, ci, lr, startf, n, String, opts, log, col)
            if T0 !== nothing && st != CELL_MISSING
                kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
                pushproblem!(log, i, col, sp[1], kind,
                               "cannot parse transposed value as $T0")
            end
            sawmiss = true
            continue
        end
        ti, tj = _trimblanks(buf, cpos, cpos + clen - 1)   # typed values tolerate blanks
        if ti > tj
            ti, tj = cpos, cpos + clen - 1
        end
        v, ok = parsevalue(T, buf, ti, tj, opts, scratch)
        if !ok
            # exact inference cannot conflict; a user-pinned type leaves the
            # cell missing (strict=false File semantics)
            T0 === nothing &&
                return _transposedcolumn(buf, ci, lr, startf, n, String, opts, log, col)
            pushproblem!(log, i, col, sp[1], :invalid_value,
                           "cannot parse transposed value as $T0")
            sawmiss = true
            continue
        end
        out[i] = v
    end
    return sawmiss ? out : convert(Vector{T}, out)
end

function _transposedfile(source; types=nothing, pool=DEFAULT_POOL, downcast::Bool=false,
                         stringtype::Type=CompactString,
                         on_error::Symbol=:collect, maxproblems::Int=10_000,
                         header::Union{Bool, Integer, AbstractVector}=true,
                         skipto::Union{Nothing, Integer}=nothing,
                         missingstring=nothing, delim=',',
                         normalizenames::Bool=false, limit::Union{Nothing, Integer}=nothing,
                         validate::Bool=true,
                         buffer_in_memory::Bool=false, prefetch::Bool=true, kw...)
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    allowed = (_DIALECTKW..., _VALUEKW...)
    _checkkwargs("File(transpose=true)", kw, allowed)
    header isa Integer && header < 0 &&
        throw(ArgumentError("header must be ≥ 0 (got $header)"))
    header isa AbstractVector{<:Integer} &&
        throw(ArgumentError("transpose=true takes a single header field index, not a range"))
    skipto === nothing || skipto >= 1 ||
        throw(ArgumentError("skipto must be ≥ 1 (got $skipto)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    # transposed geometry (0.10 semantics): header=N takes each row's Nth field
    # as that column's name; skipto=M starts data at field M (default: the field
    # after the header, or field 1 without one); header=[names] is explicit
    rawnamefield = header === true ? 1 : header === false ? 0 :
                   header isa Integer ? header : 0
    namefield = _saturatedint(rawnamefield)
    explicitnames = header isa AbstractVector && !(header isa AbstractVector{<:Integer}) ?
                    Symbol.(header) : nothing
    hasnames = rawnamefield > 0
    startf = skipto === nothing ? _saturatedinc(namefield) : _saturatedint(skipto)
    skipto !== nothing && hasnames && skipto <= rawnamefield &&
        throw(ArgumentError("skipto=$skipto must be past the header field $rawnamefield"))
    buf = resolvesource(source; buffer_in_memory, prefetch)
    dialectkw = _pickkwargs(kw, _DIALECTKW)
    valuekw = _pickkwargs(kw, _VALUEKW)
    dfdict = nothing
    if haskey(valuekw, :dateformat) && valuekw.dateformat isa AbstractDict
        dfdict = valuekw.dateformat
        valuekw = NamedTuple(kv for kv in pairs(valuekw) if kv.first != :dateformat)
    end
    d = Dialect(; delim, dialectkw...)
    opts = makevalueopts(d; sentinels=_sentinels(missingstring), valuekw...)
    bi = index(buf, d; datastart=_datastart(buf), parallel=false)
    rows = Tuple{Any, Int}[]
    for ci in bi.chunks, lr in ci.firstdatarow:totalrows(ci)
        push!(rows, (ci, lr))
    end
    ncols = length(rows)
    n = ncols == 0 ? 0 :
        maximum(nfields(r[1], r[2]) - (startf - 1) for r in rows)
    n = max(n, 0)
    limit === nothing || (n = limit >= n ? n : Int(limit))
    _tname(j, r) = (nm = hasnames ? _cellstring(buf, r[1], r[2], namefield, opts) : "";
                    isempty(nm) ? Symbol("Column", j) : Symbol(nm))
    names = explicitnames !== nothing ? copy(explicitnames) :
            Symbol[_tname(j, r) for (j, r) in enumerate(rows)]
    explicitnames !== nothing && length(names) != ncols &&
        throw(ArgumentError("header has $(length(names)) names for $ncols transposed rows"))
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = makeunique!(names)
    seed = _accessparsetypes(types, names, ncols; validate)
    colopts = if dfdict === nothing
        fill(opts, ncols)
    else
        overrides = _resolvekeys(dfdict, names, ncols, "dateformat"; validate)
        ValueOpts[haskey(overrides, j) ?
                    makevalueopts(d; sentinels=_sentinels(missingstring), valuekw...,
                                    dateformat=overrides[j]) : opts
                    for j in 1:ncols]
    end
    log = ProblemLog(maxproblems)
    cols = AbstractVector[_transposedcolumn(buf, r[1], r[2], startf, n, seed[j], colopts[j],
                                            log, j)
                          for (j, r) in enumerate(rows)]
    sortproblems!(log)
    t = ParsedTable(names, cols, n, log.items, log.dropped)
    on_error === :error && log.first !== nothing && _throwproblem(t, log.first)
    t = _poolcolumns(t, _resolvepool(pool, names, ncols; validate); parallel=false)
    poolS = stringtype === CompactString ? String : stringtype
    t = _pooledarrays(t, poolS)
    downcast && (t = _downcast(t))
    stringtype === CompactString || (t = _materializestrings(t, stringtype))
    nm = _sourcename(source)
    return File(nm, t, Dict(nm2 => j for (j, nm2) in enumerate(names)))
end

# --- pooling: a finalize-time pass at the API layer ---------------------------
#
# The kernel never pools. When asked, each CompactString column is interned
# ONCE, allocation-free (CompactString hashing/equality are content-based),
# into first-occurrence levels; the policy bound `min(floor(ratio·n), cap)`
# abandons a column the moment its distinct count exceeds it (a unique-valued
# column costs the walk up to that bound, nothing more). Columns pool in
# parallel. This replaced ~500 lines of parse-time interning (open-addressing
# tables, per-column atomic abort/degrade, hash-sampled pre-skip) at the cost
# of a few ms on a pooled 39 MiB file — and only when pooling is requested.

# `pool` policy spellings → (ratio, cap) or nothing
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
    rawcap = pool[2]
    rawcap >= 0 || throw(ArgumentError("pool cap must be nonnegative (got $rawcap)"))
    # Normalize only after checking in the caller's integer domain. Unsigned
    # and arbitrary-precision caps can exceed typemax(Int), especially on a
    # 32-bit process; they mean "no practical cap" and clamp safely.
    cap = rawcap > typemax(Int) ? typemax(Int) : Int(rawcap)
    return (Float64(ratio), cap)
end

# pool as a scalar policy, Dict(col => spec), or per-column vector → one spec
# per column of `names` (nothing = never pool)
function _resolvepool(pool, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    if pool isa AbstractDict
        specs = Vector{Union{Nothing, Tuple{Float64, Int}}}(nothing, ncols)
        for (j, sp) in _resolvekeys(pool, names, ncols, "pool"; validate)
            specs[j] = _poolpolicy(sp)
        end
        return specs
    elseif pool isa AbstractVector
        length(pool) == ncols ||
            throw(ArgumentError("pool vector length $(length(pool)) != $ncols columns"))
        return Union{Nothing, Tuple{Float64, Int}}[_poolpolicy(sp) for sp in pool]
    end
    sp = _poolpolicy(pool)
    return Union{Nothing, Tuple{Float64, Int}}[sp for _ in 1:ncols]
end

# intern one CompactString column; nothing when the policy bound is exceeded.
# Rows split into contiguous ranges interned in parallel (each range's local
# table is a Dict{CompactString,UInt32}; the CompactString hash walks the
# bytes, no allocation); the local level lists then merge in range order — so
# level ids are first-occurrence-in-file order exactly as a serial pass would
# assign them — and each range's refs remap through a small local→global
# vector. A range exceeding the bound locally proves the column exceeds it.
function _poolcolumn(c::CompactStringVector, ps::Tuple{Float64, Int}; parallel::Bool=true)
    n = length(c)
    n == 0 && return nothing
    ratiolevels = ps[1] == 1.0 ? n : floor(Int, ps[1] * n)
    maxlevels = min(ratiolevels, ps[2], _MAX_POOL_LEVELS)
    maxlevels <= 0 && return nothing
    nt = parallel ? clamp(n ÷ 65_536, 1, 4 * Threads.nthreads()) : 1
    bounds = [1 + (t - 1) * n ÷ nt for t in 1:nt]
    push!(bounds, n + 1)
    refs = zeros(UInt32, n)
    locals = Vector{Tuple{Vector{CompactStringPayload}, Vector{CompactString}}}(undef, nt)
    aborted = Threads.Atomic{Bool}(false)
    # task bodies are named functions (a closure that assigned `levels`/`keys`
    # here would rebind the merge scope's variables — shared across tasks)
    if nt > 1
        @sync for t in 1:nt
            Threads.@spawn (locals[t] = _internrange!(refs, c, bounds[t], bounds[t + 1] - 1,
                                                       maxlevels, aborted))
        end
    else
        locals[1] = _internrange!(refs, c, 1, n, maxlevels, aborted)
    end
    aborted[] && return nothing
    if nt == 1
        levels = locals[1][1]
    else
        # merge levels in range order (first-occurrence-in-file ids); remap
        # each range's refs through its local→global vector
        levels = CompactStringPayload[]
        globalof = Dict{CompactString, UInt32}()
        remaps = Vector{Vector{UInt32}}(undef, nt)
        for t in 1:nt
            lkeys = locals[t][2]
            remap = Vector{UInt32}(undef, length(lkeys))
            for (li, x) in enumerate(lkeys)
                g = get(globalof, x, UInt32(0))
                if g == 0
                    length(levels) >= maxlevels && return nothing
                    push!(levels, x.p)
                    g = UInt32(length(levels))
                    globalof[x] = g
                end
                remap[li] = g
            end
            remaps[t] = remap
        end
        @sync for t in 1:nt
            Threads.@spawn _remaprange!(refs, remaps[t], bounds[t], bounds[t + 1] - 1)
        end
    end
    lv = CompactStringVector{CompactString}(levels, c.buf, c.extra, c.overflow)
    return Missing <: eltype(c) ? PooledColumn{Union{CompactString, Missing}}(refs, lv) :
                                  PooledColumn{CompactString}(refs, lv)
end

# intern rows lo..hi of `c` into a fresh local table; refs get LOCAL ids
function _internrange!(refs::Vector{UInt32}, c::CompactStringVector, lo::Int, hi::Int,
                       maxlevels::Int, aborted::Threads.Atomic{Bool})
    table = Dict{CompactString, UInt32}()
    levels = CompactStringPayload[]
    keys = CompactString[]
    @inbounds for i in lo:hi
        aborted[] && break
        x = c[i]
        x === missing && continue
        r = get(table, x, UInt32(0))
        if r == 0
            if length(levels) >= maxlevels
                aborted[] = true
                break
            end
            push!(levels, x.p)
            push!(keys, x)
            r = UInt32(length(levels))
            table[x] = r
        end
        refs[i] = r
    end
    return (levels, keys)
end

function _remaprange!(refs::Vector{UInt32}, remap::Vector{UInt32}, lo::Int, hi::Int)
    @inbounds for i in lo:hi
        r = refs[i]
        r == 0 || (refs[i] = remap[r])
    end
    return
end

# pool the table's CompactString columns per `specs` (one per output column),
# in parallel across columns
function _poolcolumns(t::ParsedTable, specs::AbstractVector; parallel::Bool=true)
    js = [j for (j, c) in enumerate(t.columns)
          if c isa CompactStringVector && j <= length(specs) && specs[j] !== nothing]
    isempty(js) && return t
    cols = AbstractVector[t.columns...]
    pooled = Vector{Any}(nothing, length(js))
    poolone = i -> (pooled[i] = _poolcolumn(cols[js[i]]::CompactStringVector, specs[js[i]]; parallel))
    if parallel && length(js) > 1
        @sync for i in eachindex(js)
            Threads.@spawn poolone(i)
        end
    else
        foreach(poolone, eachindex(js))
    end
    for (i, j) in enumerate(js)
        pooled[i] === nothing || (cols[j] = pooled[i])
    end
    return ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# Locate a parsed-table row in the structural index. `problemrowbase` is zero
# for a whole File and the number of preceding rows for a Chunks batch. Narrow
# conversion happens after native-width parsing, but its diagnostics must still
# carry the same global row and source-byte position as kernel diagnostics.
function _narrowlocation(chunks, row::Int, col::Int, problemrowbase::Int,
                         chunkidx::Int, indexedrowbase::Int)
    while chunkidx <= length(chunks)
        ci = chunks[chunkidx]
        nr = nrows(ci)
        if row <= indexedrowbase + nr
            lr = ci.firstdatarow + (row - indexedrowbase) - 1
            sp = fieldspan(ci, lr, col)
            sp === nothing && error("internal error: narrow value has no indexed field span")
            return problemrowbase + row, sp[1], chunkidx, indexedrowbase
        end
        indexedrowbase += nr
        chunkidx += 1
    end
    error("internal error: narrow value row is outside the structural index")
end

# user-requested narrow types (Int8/16/32, UInt*, Float16/32): the kernel
# parsed the native type; convert here. A value outside the narrow range
# becomes missing with a recorded problem (0.10 semantics, strict=false).
function _narrowtypes(t::ParsedTable, req, sel, chunks, maxproblems::Int,
                      firstproblem::Union{Nothing, Problem}=nothing;
                      problemrowbase::Int=0,
                      sourcerows::Union{Nothing, AbstractVector{Int}}=nothing)
    all(x -> x === nothing, req) && return t, firstproblem
    sourcerows === nothing || length(sourcerows) == t.nrows ||
        throw(ArgumentError("source-row map length $(length(sourcerows)) != $(t.nrows) rows"))
    # The kernel emits selected columns in file order. `sel` retains the user's
    # request order and may contain duplicates, so normalize it before mapping
    # output position back to the file-column-indexed `req` vector.
    keep = sel === nothing ? collect(1:length(req)) : sort!(unique(sel))
    cols = AbstractVector[t.columns...]
    # Narrow conversion is part of parsing, not a later unbounded side channel.
    # Seed the same bounded log with the parse/header diagnostics, then add
    # conversion failures through its source-earliest retention policy.
    log = ProblemLog(maxproblems)
    firstproblem !== nothing && (log.first = firstproblem)
    for pr in t.problems
        pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
    end
    log.dropped += t.droppedproblems
    for (o, j) in enumerate(keep)
        T = req[j]
        T === nothing && continue
        c = cols[o]
        Base.nonmissingtype(eltype(c)) in (Int64, Int128, Float64) || continue
        out = Vector{Union{T, Missing}}(undef, length(c))
        # The kernel widens a user-declared Union{Missing,T} before this door.
        # Preserve that declaration even when every value is present.
        anymissing = Missing <: eltype(c)
        chunkidx = 1
        indexedrowbase = 0
        @inbounds for i in eachindex(c)
            x = c[i]
            if x === missing
                out[i] = missing
                anymissing = true
            elseif T <: Integer && !(typemin(T) <= x <= typemax(T))
                out[i] = missing
                anymissing = true
                sourcei = sourcerows === nothing ? i : sourcerows[i]
                problemrow, problempos, chunkidx, indexedrowbase =
                    _narrowlocation(chunks, sourcei, j, problemrowbase,
                                    chunkidx, indexedrowbase)
                pushproblem!(log, problemrow, j, problempos, :invalid_value,
                               "value $x does not fit $T")
            else
                out[i] = convert(T, x)
            end
        end
        cols[o] = anymissing ? out : convert(Vector{T}, out)
    end
    sortproblems!(log)
    return ParsedTable(t.names, cols, t.nrows, log.items, log.dropped), log.first
end

# downcast=true: Int64 columns shrink to the smallest of Int8/Int16/Int32 that
# holds every value (CSV.jl parity; one extrema scan + one convert per column)
function _downcastint(lo::Int64, hi::Int64)
    typemin(Int8) <= lo && hi <= typemax(Int8) && return Int8
    typemin(Int16) <= lo && hi <= typemax(Int16) && return Int16
    typemin(Int32) <= lo && hi <= typemax(Int32) && return Int32
    return Int64
end
function _downcastcol(v::Vector{Int64})
    isempty(v) && return v
    lo, hi = extrema(v)
    T = _downcastint(lo, hi)
    return T === Int64 ? v : convert(Vector{T}, v)
end
function _downcastcol(v::Vector{Union{Int64, Missing}})
    lo, hi, n = typemax(Int64), typemin(Int64), 0
    for x in v
        x === missing && continue
        n += 1
        lo = min(lo, x)
        hi = max(hi, x)
    end
    n == 0 && return v
    T = _downcastint(lo, hi)
    return T === Int64 ? v : convert(Vector{Union{T, Missing}}, v)
end
_downcastcol(v::AbstractVector) = v
function _downcast(t::ParsedTable)
    cols = AbstractVector[_downcastcol(c) for c in t.columns]
    return ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# PooledColumn -> PooledArrays.PooledArray, the ecosystem dictionary type.
# Levels materialize to String (at most the pool cap of them); refs are shared
# outright for missing-free columns and remapped once — missing joins the pool,
# CSV.jl's convention — otherwise. Measured 0.2-0.8 ms on 20 MiB shapes.
function _topooledarray(c::PooledColumn{ELT}, ::Type{S0}=String) where {ELT, S0}
    n = length(c.levels)
    lv = _levelvector(S0, c.levels, n)   # an abstract S0 (InlineString) resolves to a width here
    S = eltype(lv)
    if !(Missing <: ELT)
        invpool = Dict{S, UInt32}(lv[i] => UInt32(i) for i in 1:n)
        return PooledArray(PooledArrays.RefArray(poolrefs(c)), invpool, lv)
    end
    pool = Vector{Union{S, Missing}}(undef, n + 1)
    @inbounds for i in 1:n
        pool[i] = lv[i]
    end
    pool[n + 1] = missing
    invpool = Dict{Union{S, Missing}, UInt32}(pool[i] => UInt32(i) for i in 1:(n + 1))
    oldrefs = poolrefs(c)
    refs = similar(oldrefs)
    mref = UInt32(n + 1)
    @inbounds @simd for i in eachindex(refs)
        r = oldrefs[i]
        refs[i] = r == 0 ? mref : r
    end
    return PooledArray(PooledArrays.RefArray(refs), invpool, pool)
end

function _pooledarrays(t::ParsedTable, ::Type{S}=String) where {S}
    any(c -> c isa PooledColumn, t.columns) || return t
    cols = AbstractVector[c isa PooledColumn ? _topooledarray(c, S) : c
                          for c in t.columns]
    return ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# --- the string-output hook -------------------------------------------------
# `stringtype` names the element type string columns come out as. The core
# knows CompactString (the default; zero-copy views) and String (bulk
# materialization). Extensions register more by adding methods to
# `_stringsink` (validation) and `_materializecolumn` / `_levelvector`
# (conversion): CSVInlineStringsExt registers InlineString (auto-width per
# column) and the fixed String1..String255.
_stringsink(::Type{CompactString}) = true
_stringsink(::Type{String}) = true
_stringsink(::Type) = false
_checkstringtype(T) =
    (T isa Type && _stringsink(T)) ||
        throw(ArgumentError("stringtype must be CSV.CompactString, String, or a " *
                            "type provided by an extension (e.g. InlineString with " *
                            "InlineStrings loaded); got $T"))

# a CompactStringVector to Vector{S} / Vector{Union{S,Missing}}. String goes
# through materialize's bulk path — one shared scratch, word-store inline
# reconstruction, unsafe_string per cell; a per-cell String() broadcast ran the
# generic AbstractString path and was a measured 55–110 MiB/s cliff on
# string-heavy shapes.
_materializecolumn(::Type{String}, col::CompactStringVector) = materialize(col)
# pool levels (a CompactStringVector) to Vector{S}
_levelvector(::Type{String}, levels::CompactStringVector, n::Int) =
    String[String(levels[i]) for i in 1:n]

function _materializestrings(t::ParsedTable, ::Type{S}=String) where {S}
    cols = AbstractVector[col isa CompactStringVector ? _materializecolumn(S, col) : col
                          for col in t.columns]
    return ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# --- Tables.jl + row access -------------------------------------------------

# NB: getproperty resolves COLUMNS first (f.score), so interface methods must
# reach internals via getfield — a column named `table` must not shadow them.
Tables.istable(::Type{File}) = true
Tables.columnaccess(::Type{File}) = true
Tables.rowaccess(::Type{File}) = true
Tables.columns(f::File) = getfield(f, :table)
Tables.rows(f::File) = f
Tables.columnnames(f::File) = names(getfield(f, :table))
Tables.getcolumn(f::File, i::Int) = columns(getfield(f, :table))[i]
Tables.getcolumn(f::File, nm::Symbol) = getfield(f, :table)[nm]
Tables.rowcount(f::File) = getfield(f, :table).nrows
Tables.schema(f::File) = Tables.schema(getfield(f, :table))

problems(f::File) = problems(getfield(f, :table))

Base.names(f::File) = names(getfield(f, :table))
Base.propertynames(f::File) = names(getfield(f, :table))
function _fileproperty(f::File, nm::Symbol)
    lk = getfield(f, :lookup)
    haskey(lk, nm) && return columns(getfield(f, :table))[lk[nm]]
    # CSV.File 0.10 stored `names` directly. Preserve `f.names` and `f[:names]`
    # without adding a second, drift-prone copy of the schema.
    nm === :names && return names(getfield(f, :table))
    return getfield(f, nm)
end
Base.getproperty(f::File, nm::Symbol) = _fileproperty(f, nm)

Base.length(f::File) = getfield(f, :table).nrows
Base.eltype(::Type{File}) = FileRow
Base.IndexStyle(::Type{File}) = IndexLinear()
Base.size(f::File) = (length(f),)
_filerow(f::File, i::Int) = FileRow(names(getfield(f, :table)),
                                    columns(getfield(f, :table)),
                                    getfield(f, :lookup), i)
Base.iterate(f::File, i::Int=1) = i > length(f) ? nothing : (_filerow(f, i), i + 1)
Base.getindex(f::File, i::Int) = (1 <= i <= length(f) || throw(BoundsError(f, i)); _filerow(f, i))
Base.getindex(f::File, nm::Symbol) = _fileproperty(f, nm)
Base.getindex(f::File, nm::AbstractString) = f[Symbol(nm)]

Tables.columnnames(r::FileRow) = getfield(r, :names)
Tables.getcolumn(r::FileRow, j::Int) =
    getfield(r, :columns)[j][getfield(r, :row)]
Tables.getcolumn(r::FileRow, nm::Symbol) =
    Tables.getcolumn(r, getfield(r, :lookup)[nm])
rownumber(r::FileRow) = getfield(r, :row)

function Base.show(io::IO, f::File)
    t = getfield(f, :table)
    println(io, "CSV.File($(repr(getfield(f, :name)))):")
    println(io, "Size: $(t.nrows) x $(length(names(t)))")
    show(io, Tables.schema(t))
    nproblems = length(t.problems) + t.droppedproblems
    nproblems > 0 &&
        print(io, "\n$nproblems problem(s) recorded — $(length(t.problems)) retained by problems(f)")
end

read(source, sink; kw...) = sink(Tables.CopiedColumns(File(source; kw...)))

# ---------------------------------------------------------------------------
# lazy / LazyFile — the structural index AS a table
# ---------------------------------------------------------------------------
function lazy(source; types=nothing, stringtype::Type=CompactString,
              select=nothing, drop=nothing, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW..., :validate)
    _checkkwargs("lazy", kw, allowed)
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    validate = get(kw, :validate, true)
    sel = _resolveselect(select, drop, p.names)
    seed = types === nothing ? nothing :
           _accessparsetypes(types, p.names, p.ncols; validate)
    chunks = p.bi.chunks
    rowbases = cumsum([0; Int[nrows(ci) for ci in chunks[1:max(length(chunks) - 1, 0)]]])
    total = sum(nrows, chunks; init=0)
    nr = p.limit === nothing ? total : min(total, p.limit)
    js = sel === nothing ? collect(1:p.ncols) : sel
    colopts = get(p.parsekw, :colopts, nothing)
    cols = AbstractVector[]
    for j in js
        T = seed === nothing ? nothing : seed[j]
        opts = colopts === nothing ? p.opts : colopts[j]
        c = T === nothing || T === String ?
                LazyColumn{_lazyeltype(stringtype)}(p.buf, chunks, rowbases, j, opts, nr, stringtype) :
                LazyColumn{Union{T, Missing}}(p.buf, chunks, rowbases, j, opts, nr, T)
        push!(cols, c)
    end
    names = p.names[js]
    return LazyFile(_sourcename(source), p, js, names, cols, nr,
                    Dict(nm => i for (i, nm) in enumerate(names)))
end
_lazyeltype(::Type{CompactString}) = Union{CompactString, Missing}
_lazyeltype(::Type{S}) where {S} = Union{S, Missing}

# Internal lazy-vector implementation. Public callers interact with it through
# `CSV.LazyFile` and the AbstractVector/Tables.jl interfaces.
struct LazyColumn{ELT, T} <: AbstractVector{ELT}   # T: CompactString | String | extension string type | a value type
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    rowbases::Vector{Int}
    j::Int
    opts::ValueOpts
    nrows::Int
    hint::Threads.Atomic{Int}  # last chunk touched; atomic because columns can be shared by tasks
end
LazyColumn{ELT}(buf, chunks, rowbases, j, opts, nrows, ::Type{T}) where {ELT, T} =
    LazyColumn{ELT, T}(buf, chunks, rowbases, j, opts, nrows, Threads.Atomic{Int}(1))
_lazytarget(::LazyColumn{ELT, T}) where {ELT, T} = T
Base.size(c::LazyColumn) = (c.nrows,)
Base.IndexStyle(::Type{<:LazyColumn}) = IndexLinear()

# global row → (chunk, local row); rowbases is nondecreasing. The hint makes
# a scan of the column O(1) per cell. Concurrent readers may replace the atomic
# hint, but every loaded value is validated before use.
@inline function _lazylocate(c::LazyColumn, i::Int)
    k = c.hint[]
    @inbounds if !(1 <= k <= length(c.chunks) &&
                   c.rowbases[k] < i <= c.rowbases[k] + nrows(c.chunks[k]))
        k = searchsortedlast(c.rowbases, i - 1)
        c.hint[] = k
    end
    ci = @inbounds c.chunks[k]
    return ci, ci.firstdatarow + (i - @inbounds(c.rowbases[k])) - 1
end

function Base.getindex(c::LazyColumn, i::Int)
    @boundscheck checkbounds(c, i)
    ci, lr = _lazylocate(c, i)
    sp = fieldspan(ci, lr, c.j)
    sp === nothing && return missing                       # short row
    pos, len = sp
    len == 0 && return missing
    return _lazyvalue(c, pos, len)
end
@inline function _lazyvalue(c::LazyColumn{ELT, T}, pos::Int, len::Int) where {ELT, T}
    cpos, clen, esc, st = cellcontent(c.buf, pos, len, c.opts)
    st == CELL_MISSING && return missing
    if _stringsink(T)
        # a string cell: zero-copy view, unless quoting demands unescaping
        # (or the structural quote reading is malformed — keep the raw bytes)
        if st == CELL_BADQUOTE
            cpos, clen, esc = pos, len, false
        end
        s = if esc
            bytes = _unescape_bytes(c.buf, Int64(cpos), Int32(clen), c.opts.e, c.opts.cq)
            _lazycompact(bytes, 1, length(bytes))
        else
            _lazycompact(c.buf, cpos, clen)
        end
        return T === CompactString ? s : convert(T, String(s))
    end
    # `types=Missing` is an intentional sink: every present value recovers to
    # missing, as it does in the eager parser's default collecting mode.
    T === Missing && return missing
    # a typed cell: the same kernels File uses, on demand
    (st == CELL_BADQUOTE || clen == 0 || esc) && return missing
    v, ok = parsevalue(T, c.buf, cpos, cpos + clen - 1, c.opts)
    return ok ? v : missing
end

# CompactString's view word has an Int32 offset. Lazy access normally retains
# the source buffer with no copy. For a long cell beyond that absolute offset,
# copy only the cell into its own small backing buffer. The returned value owns
# that buffer, so this fallback is lifetime- and concurrency-safe.
@inline function _lazycompact(buf::Vector{UInt8}, pos::Int, len::Int,
                              viewoffsetlimit::Int=Int(typemax(Int32)))
    len <= COMPACTSTRING_INLINE &&
        return CompactString(inline_payload(buf, pos, len), EMPTY_BYTES)
    pos - 1 <= viewoffsetlimit &&
        return CompactString(view_payload(buf, pos, len, 0, pos - 1), buf)
    bytes = Vector{UInt8}(undef, len)
    copyto!(bytes, 1, buf, pos, len)
    return CompactString(view_payload(bytes, 1, len, 0, 0), bytes)
end

# Sequential access (collect, sum, DataFrame(lf), display) walks chunk by
# chunk with no per-cell chunk lookup; only random access pays the search.
@inline function _lazycell(c::LazyColumn, ci::ChunkIndex, lr::Int)
    sp = fieldspan(ci, lr, c.j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    return _lazyvalue(c, pos, len)
end
function Base.iterate(c::LazyColumn, state=(1, 0, 0))
    k, lr, done = state
    done >= c.nrows && return nothing
    chunks = c.chunks
    @inbounds while k <= length(chunks)
        ci = chunks[k]
        lr == 0 && (lr = ci.firstdatarow)
        if lr <= totalrows(ci)
            return _lazycell(c, ci, lr), (k, lr + 1, done + 1)
        end
        k += 1
        lr = 0
    end
    return nothing
end

struct LazyFile
    name::String
    prepared::Prepared
    sourceindices::Vector{Int}
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
    lookup::Dict{Symbol, Int}
end
Base.names(lf::LazyFile) = getfield(lf, :names)
Base.size(lf::LazyFile) = (getfield(lf, :nrows), length(getfield(lf, :columns)))
Base.size(lf::LazyFile, d::Int) = size(lf)[d]
Base.length(lf::LazyFile) = getfield(lf, :nrows)
Base.getindex(lf::LazyFile, nm::Symbol) = getfield(lf, :columns)[getfield(lf, :lookup)[nm]]
Base.getindex(lf::LazyFile, j::Int) = getfield(lf, :columns)[j]
Base.getindex(lf::LazyFile, i::Int, j::Int) = getfield(lf, :columns)[j][i]
Base.getindex(lf::LazyFile, i::Int, nm::Symbol) = lf[nm][i]
Base.getproperty(lf::LazyFile, nm::Symbol) =
    haskey(getfield(lf, :lookup), nm) ? lf[nm] : getfield(lf, nm)
Base.propertynames(lf::LazyFile) = getfield(lf, :names)
Tables.istable(::Type{LazyFile}) = true
Tables.columnaccess(::Type{LazyFile}) = true
Tables.columns(lf::LazyFile) = lf
Tables.columnnames(lf::LazyFile) = getfield(lf, :names)
Tables.getcolumn(lf::LazyFile, i::Int) = getfield(lf, :columns)[i]
Tables.getcolumn(lf::LazyFile, nm::Symbol) = lf[nm]
Tables.rowcount(lf::LazyFile) = getfield(lf, :nrows)
Tables.schema(lf::LazyFile) =
    Tables.Schema(getfield(lf, :names), Type[eltype(c) for c in getfield(lf, :columns)])
function Base.show(io::IO, lf::LazyFile)
    n, m = size(lf)
    print(io, "CSV.LazyFile(", repr(getfield(lf, :name)), "): ", n, " row", n == 1 ? "" : "s",
          " × ", m, " column", m == 1 ? "" : "s", " (indexed, cells lazy)")
    for (nm, c) in zip(getfield(lf, :names), getfield(lf, :columns))
        print(io, "\n  ", nm, "::", eltype(c))
    end
end

function File(lf::LazyFile; types=nothing, select=nothing, drop=nothing, pool=DEFAULT_POOL,
              downcast::Bool=false, stringtype::Type=CompactString, strict::Bool=false,
              on_error::Symbol=strict ? :error : :collect,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              validate::Bool=true)
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    _checkstringtype(stringtype)
    return _filefromprepared(getfield(lf, :prepared), getfield(lf, :name); types, select, drop,
                             pool, downcast, stringtype, on_error, maxproblems, parallel, ntasks,
                             validate,
                             available=getfield(lf, :sourceindices))
end

# ---------------------------------------------------------------------------
# Rows — streaming
# ---------------------------------------------------------------------------
struct Rows
    inner::_IndexedRows
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    sourceindices::Vector{Int}
    types::Union{Nothing, Vector{Union{Nothing, Type}}}
    limit::Union{Nothing, Int}
    stringtype::Type
    on_error::Symbol
end

function Rows(source; types=nothing, reusebuffer::Bool=false, select=nothing, drop=nothing,
              stringtype::Type=CompactString, strict::Bool=false,
              on_error::Symbol=strict ? :error : :collect, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW...)
    _checkkwargs("Rows", kw, allowed)
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    js = something(_resolveselect(select, drop, p.names), collect(1:p.ncols))
    fullseed = types === nothing ? nothing :
               _accessparsetypes(types, p.names, p.ncols;
                                 validate=get(kw, :validate, true))
    seed = fullseed === nothing ? nothing : fullseed[js]
    names = p.names[js]
    colopts = get(p.parsekw, :colopts, nothing)
    inner = _IndexedRows(p.buf, p.bi.chunks, p.names,
                         Dict(nm => j for (j, nm) in enumerate(p.names)),
                         p.opts, colopts, p.d)
    return Rows(inner, names, Dict(nm => j for (j, nm) in enumerate(names)), js,
                seed, p.limit, stringtype, on_error)
end

Tables.istable(::Type{Rows}) = true
Tables.rowaccess(::Type{Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = r.types === nothing ?
    Tables.Schema(r.names, fill(Union{_rowstringtype(r.stringtype), Missing},
                                length(r.names))) :
    Tables.Schema(r.names,
                  Type[T === nothing ? Union{_rowstringtype(r.stringtype), Missing} :
                       Union{T, Missing} for T in r.types])
_rowstringtype(T) = T === CompactString ? CompactString : T

struct Row <: Tables.AbstractRow
    view::_IndexedRow
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    sourceindices::Vector{Int}
    types::Union{Nothing, Vector{Union{Nothing, Type}}}
    stringtype::Type
    on_error::Symbol
end

Base.eltype(::Type{Rows}) = Row
Base.IteratorSize(::Type{Rows}) = Base.SizeUnknown()

function Base.iterate(r::Rows, state=((1, nothing, 1)))
    r.limit !== nothing && state[3] > r.limit && return nothing
    it = iterate(r.inner, state)
    it === nothing && return nothing
    view, next = it
    return Row(view, r.names, r.lookup, r.sourceindices, r.types,
               r.stringtype, r.on_error), next
end

Tables.columnnames(row::Row) = getfield(row, :names)

@noinline function _throwrowproblem(view::_IndexedRow, j::Int, pos::Int,
                                    kind::Symbol, message::String)
    row = getfield(view, :rownumber)
    throw(ErrorException("CSV: $kind at data row $row, column $j: $message"))
end

# Rows has no retained diagnostic table. In fail-fast mode, validate and parse
# the requested cell at the access boundary, where its lazy value is first
# observed. This keeps the default allocation-free row view while restoring
# 0.10's strict keyword and the 1.0 on_error contract.
function _strictrowvalue(view::_IndexedRow, j::Int, T)
    r = getfield(view, :r)
    ci = getfield(view, :ci)
    lr = getfield(view, :localrow)
    sp = fieldspan(ci, lr, j)
    if sp === nothing
        pos = ci.start + Int(ci.rowstartrel[lr])
        _throwrowproblem(view, j, pos, :short_row, "row has no field $j")
    end
    pos, len = sp
    len == 0 && return missing
    opts = _rowopts(r, j)
    cpos, clen, esc, st = cellcontent(r.buf, pos, len, opts)
    st == CELL_MISSING && return missing
    st == CELL_BADQUOTE &&
        _throwrowproblem(view, j, pos, :invalid_quoted_field,
                         "malformed quoting in " * excerpt(r.buf, pos, len))
    if (T === nothing || T === String) &&
       !_wasquoted(r.buf, pos, len, r.opts) &&
       _delimclash(r.buf, cpos, clen, opts.delim)
        _throwrowproblem(view, j, pos, :invalid_value,
                         "bare quote engaged structural protection in " *
                         excerpt(r.buf, pos, len))
    end
    T === nothing && return view[j]
    T === String && return _typedvalue(String, view, j)
    T === Missing &&
        _throwrowproblem(view, j, pos, :invalid_value,
                         "non-missing value cannot be parsed as Missing in " *
                         excerpt(r.buf, pos, len))
    (clen > 0 && !esc) ||
        _throwrowproblem(view, j, pos, :invalid_value,
                         "cannot parse $T from " * excerpt(r.buf, pos, len))
    value, ok = parsevalue(T, r.buf, cpos, cpos + clen - 1, opts)
    ok || _throwrowproblem(view, j, pos, :invalid_value,
                           "cannot parse $T from " * excerpt(r.buf, pos, len))
    return value
end

function Tables.getcolumn(row::Row, j::Int)
    ts = getfield(row, :types)
    v = getfield(row, :view)
    sourcej = getfield(row, :sourceindices)[j]
    T = ts === nothing ? nothing : ts[j]
    x = if getfield(row, :on_error) === :error
        _strictrowvalue(v, sourcej, T)
    else
        T === nothing ? v[sourcej] : T === Missing ? missing :
                        _typedvalue(T, v, sourcej)
    end
    st = getfield(row, :stringtype)
    return st === CompactString || !(x isa CompactString) ? x : _rowstring(st, x)
end
# per-cell string materialization for Rows(stringtype=...); extensions may add
_rowstring(::Type{String}, x::CompactString) = String(x)
Tables.getcolumn(row::Row, nm::Symbol) =
    Tables.getcolumn(row, getfield(row, :lookup)[nm])
Base.getindex(row::Row, j::Int) = Tables.getcolumn(row, j)
Base.getindex(row::Row, nm::Symbol) = Tables.getcolumn(row, nm)
rownumber(row::Row) = getfield(getfield(row, :view), :rownumber)

# ---------------------------------------------------------------------------
# Chunks — batched
# ---------------------------------------------------------------------------

struct Chunks
    name::String
    inner::Batches
    headerlog::ProblemLog
    maxproblems::Int
    requestedtypes::Vector{Union{Nothing, Type}}
    sourceindices::Vector{Int}
    on_error::Symbol
    stringtype::Type
    poolspec::Union{Nothing, Tuple{Float64, Int}}
end

Base.length(c::Chunks) = length(getfield(c, :inner))
Base.eltype(::Type{Chunks}) = File
Tables.partitions(c::Chunks) = c

function Base.iterate(c::Chunks, state::Int=1)
    inner = getfield(c, :inner)
    it = iterate(inner, state)
    it === nothing && return nothing
    t, next = it
    headerlog = state == 1 ? getfield(c, :headerlog) : nothing
    cap = getfield(c, :maxproblems)
    t, firstproblem = _mergeproblems(t, headerlog, cap)
    ci = getfield(inner, :chunks)[state]
    problemrowbase = chunkrowbase(getfield(inner, :chunks), ci)
    t, firstproblem = _narrowtypes(t, getfield(c, :requestedtypes),
                                   getfield(c, :sourceindices),
                                   (ci,), cap, firstproblem; problemrowbase)
    getfield(c, :on_error) === :error && firstproblem !== nothing &&
        _throwproblem(t, firstproblem)
    # every batch leaves through the same door as File: PooledArray for
    # pooled columns (levels in the output string type), then the string
    # materialization the caller asked for
    st = getfield(c, :stringtype)
    poolS = st === CompactString ? String : st
    ps = getfield(c, :poolspec)
    ps === nothing || (t = _poolcolumns(t, fill(ps, length(t.columns))))
    t = _pooledarrays(t, poolS)
    st === CompactString || (t = _materializestrings(t, st))
    f = File(getfield(c, :name), t,
             Dict(nm => j for (j, nm) in enumerate(names(t))))
    return f, next
end

function Chunks(source; types=nothing, ntasks::Union{Nothing, Int}=nothing,
                maxproblems::Int=10_000, stringtype::Type=CompactString,
                pool=DEFAULT_POOL, select=nothing, drop=nothing, strict::Bool=false,
                on_error::Symbol=strict ? :error : :collect, kw...)
    nt = something(ntasks, Threads.nthreads())
    nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    _checkstringtype(stringtype)
    name = _sourcename(source)
    poolspec = _poolpolicy(pool)   # per-batch policy (Dict/vector forms: File only)
    pool isa Union{AbstractDict, AbstractVector} &&
        throw(ArgumentError("Chunks takes a single pool policy (Bool / ratio / (ratio, cap))"))
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW...)
    _checkkwargs("Chunks", kw, allowed)
    if !haskey(kw, :chunkbytes)
        buf = resolvesource(source;
                            buffer_in_memory=get(kw, :buffer_in_memory, false),
                            prefetch=get(kw, :prefetch, true))
        kw = (; kw..., chunkbytes=clamp(cld(length(buf), nt), 1 << 10, 1 << 22))
        source = buf
    end
    haskey(kw, :parallel) || (kw = (; kw..., parallel=nt > 1))
    capturecap = max(maxproblems, 1)
    p = _prepare(source; ntasks=nt, maxproblems=capturecap, kw...)
    chunks = p.bi.chunks
    fullrows = sum(nrows, chunks; init=0)
    p.limit === nothing || _limitrows!(chunks, p.limit)
    filter!(ci -> nrows(ci) > 0, chunks)
    requested = requestedtypes(types, p.names, p.ncols;
                                 validate=get(kw, :validate, true))
    seed = resolvetypes(types, p.names, p.ncols; validate=get(kw, :validate, true))
    js = something(_resolveselect(select, drop, p.names), collect(1:p.ncols))
    userprovided = [seed[j] !== nothing for j in js]
    if any(j -> seed[j] === nothing, js)
        total = sum(nrows, chunks; init=0)
        selected = fill(false, p.ncols)
        selected[js] .= true
        colopts = get(p.parsekw, :colopts, nothing)
        inferred = sampletypes(p.buf, chunks, p.ncols, p.opts;
                                 nsample=max(1, total), selected, colopts)
        for j in js
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    seedtypes = Type[seed[j] for j in js]
    colopts = get(p.parsekw, :colopts, nothing)
    allowmissing = schemamissing(p.buf, chunks, seedtypes, p.opts;
                                 sourceindices=js, colopts)
    unclosedquote = p.bi.unclosedquote && (p.limit === nothing || p.limit >= fullrows)
    inner = Batches(p.buf, chunks, p.names[js], js, p.ncols, seedtypes,
                    userprovided, allowmissing, p.opts, colopts, p.d,
                    capturecap, unclosedquote)
    return Chunks(name, inner, p.headerlog, maxproblems, requested, js, on_error,
                  stringtype, poolspec)
end
