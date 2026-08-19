# The public front doors — what CSV.jl's user-facing API becomes on the kernel.
#
#   CSVApi.File    ≈ CSV.File   — eager table, full kwarg surface, row access
#   CSVApi.read    ≈ CSV.read   — File piped into any Tables.jl sink
#   CSVApi.Rows    ≈ CSV.Rows   — streaming row iteration (lazy cells)
#   CSVApi.Chunks  ≈ CSV.Chunks — batch iteration with a STABLE schema
#   CSVApi.sniff   — dialect + shape detection returning a replayable Spec
#
# The point being proven at THIS layer: every entrypoint is the same short
# pipeline — resolve source bytes → settle the dialect (sniffing if asked) →
# index once → settle names/row-window (header/skipto/footerskip/limit as
# *index arithmetic*, before any value work) → hand the kernel driver or the
# streaming primitives the prepared index. There is no per-entrypoint parsing
# code and no mode flags inside the kernel: File/Rows/Chunks differ only in
# what they do AFTER `_prepare`.
#
# CSV.jl kwarg parity is the goal, with the 1.0 decisions applied and PINNED
# where they diverge (each has a test):
#   • warnings are DATA: `problems(f)` replaces strict/silencewarnings logging
#     (`strict=true` maps to `on_error=:error`, `maxwarnings` to `maxproblems`)
#   • empty unquoted cells are ALWAYS missing; `missingstring` ADDS spellings
#     (CSV.jl replaces the "" default, turning empties into present "" values)
#   • function-typed `select`/`drop`/`types` are retired (Tables.Scan is the
#     expression channel); list/Dict forms keep working
#   • `stringtype` defaults to the kernel string (CompactString);
#     `stringtype=String` materializes; InlineStrings become an extension
#   • Bool columns are strictly `true`/`false` unless truestrings/falsestrings
#   • integer spellings that fit Int128 stay exact, including initially-wide
#     and grouped columns where CSV.jl can widen Int64 overflow to Float64
#


module CSVApi

using ..CSVKernel, ..KernelExamples
using Tables, Dates, Unicode, Mmap, PooledArrays, CodecZlib, Downloads
const K = CSVKernel
const E = KernelExamples

# `sniff`/`Spec` are internal (behind `delim=nothing`); not exported.

# 1.0: no pooling unless asked. Pooling by 0.10's default policy measured
# +65% parse time on a pool-friendly 39 MiB file (22.4 vs 13.6 ms), and every
# other reader surveyed (polars, pyarrow.csv, pandas, DuckDB, fread) makes
# dictionary/categorical encoding opt-in. `pool=(0.2, 500)` restores the old
# behavior; `pool=true` pools every string column.
const DEFAULT_POOL = false

const _DIALECTKW = (:quotechar, :openquotechar, :closequotechar, :escapechar,
                    :quoted, :comment, :ignoreemptyrows, :ignorerepeated)
const _VALUEKW = (:dateformat, :decimal, :truestrings, :falsestrings,
                  :stripwhitespace, :groupmark)
const _INDEXKW = (:fastindex, :scanner)
const _DRIVERKW = (:maxproblems, :nsample, :typemap)

function _pickkwargs(kw, allowed)
    return NamedTuple(p for p in pairs(kw) if p.first in allowed)
end

const _LEGACYKW = Dict{Symbol, String}(
    :silencewarnings => "warnings are data now: problems(f) returns them; maxproblems caps retention",
    :debug => "removed in 1.0; parse problems and the structural index are inspectable directly",
    :lazystrings => "use stringtype=CSVKernel.CompactString (the default) or stringtype=String",
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
        if haskey(_LEGACYKW, k)
            throw(ArgumentError("$k was removed in 1.0: $(_LEGACYKW[k])"))
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
    CSVApi.Spec

A replayable parse plan from [`sniff`](@ref): splat it back —
`File(src; spec.delim, spec.header)` — or pass fields individually. Fields:
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
    print(io, "CSVApi.Spec(delim=", repr(s.delim), ", header=", s.header,
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
    d = K.Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    limit = samplebytes
    while true
        sample = buf[start:min(start + limit - 1, length(buf))]
        datastart = _datastart(sample)
        rowstart = datastart
        while rowstart <= length(sample)
            next = K.nextrowstart(sample, rowstart, length(sample), d, false, true)
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
    d = K.Dialect(; delim, dialectkw...)
    bi = K.index(buf, d; datastart, parallel=false, indexkw...)
    counts = Int[]
    for ci in bi.chunks, lr in 1:K.totalrows(ci)
        push!(counts, K.nfields(ci, lr))
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
    d0 = K.Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    datastart = _datastart(sample)
    # scoring reads at most 11 rows per candidate, but indexes whatever it is
    # given — trim to the first 12 rows once (row boundaries are quote-aware
    # and delimiter-independent) so candidates don't each index the full sample
    stop, rows = datastart, 0
    while stop <= length(sample) && rows < 12
        stop = K.nextrowstart(sample, stop, length(sample), d0, false, true)
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
    CSVApi.sniff(source; samplebytes=65536, kw...) -> Spec

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
    theader = K.parse(sample; header=true, parsekw...)
    tnoheader = K.parse(sample; header=false, parsekw...)
    headerlikely = tnoheader.nrows > theader.nrows &&
        any(zip(K.columns(theader), K.columns(tnoheader))) do (ch, cnh)
            Base.nonmissingtype(eltype(ch)) !== String && eltype(ch) !== Missing &&
                Base.nonmissingtype(eltype(cnh)) in (String, K.CompactString)
        end
    t = headerlikely ? theader : tnoheader
    return Spec(bestdelim, get(dialectkw, :quoted, true), headerlikely,
                length(K.names(t)), copy(K.names(t)), Type[eltype(c) for c in K.columns(t)])
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

# byte offset of PHYSICAL line `n` (1-based from `start`): CR, LF, or CRLF end a
# line and quotes mean nothing. This is how skipped PREFIX rows are counted —
# rows before a numbered header, or before `skipto` when there is no header
# row — because a stray quote in a junk preamble must not swallow the file
# (issues #1012/#1079/#1160; polars' skip_lines has the same semantics).
function _physicallineoffset(buf::Vector{UInt8}, start::Int, n::Int)
    off = start
    len = length(buf)
    for _ in 1:(n - 1)
        off > len && return len + 1
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
function _rawrowoffset(buf::Vector{UInt8}, d::K.Dialect, datastart::Int, n::Int)
    off = datastart
    for _ in 1:(n - 1)
        off > length(buf) && return length(buf) + 1
        off = K.nextrowstart(buf, off, length(buf), d, false, true)
    end
    return off
end

# advance chunks past every row starting before `byteoff`
function _skiptobyte!(chunks::Vector{K.ChunkIndex}, byteoff::Int)
    for ci in chunks
        while K.nrows(ci) > 0 &&
              ci.start + Int(ci.rowstartrel[ci.firstdatarow]) < byteoff
            ci.firstdatarow += 1
        end
    end
end

function _iscommentrow(buf::Vector{UInt8}, rowstart::Int, d::K.Dialect)
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
function _footeroffset(buf::Vector{UInt8}, d::K.Dialect, rawstart::Int, footerskip::Int)
    footerskip == 0 && return length(buf) + 1
    starts = fill(0, footerskip)
    nrows = 0
    rowstart = rawstart
    while rowstart <= length(buf)
        if !_iscommentrow(buf, rowstart, d)
            nrows += 1
            starts[mod1(nrows, footerskip)] = rowstart
        end
        rowstart = K.nextrowstart(buf, rowstart, length(buf), d, false, true)
    end
    footerskip >= nrows && return rawstart
    return starts[mod1(nrows - footerskip + 1, footerskip)]
end

function _rowsbefore(chunks::Vector{K.ChunkIndex}, byteoff::Int)
    n = 0
    for ci in chunks, lr in ci.firstdatarow:K.totalrows(ci)
        ci.start + Int(ci.rowstartrel[lr]) < byteoff && (n += 1)
    end
    return n
end

function _limitrows!(chunks::Vector{K.ChunkIndex}, limit::Int)
    remaining = limit
    for ci in chunks
        n = K.nrows(ci)
        if remaining >= n
            remaining -= n
        elseif remaining > 0
            lastrow = ci.firstdatarow + remaining - 1
            resize!(ci.rowfirst, lastrow + 1)
            resize!(ci.rowstartrel, lastrow)
            remaining = 0
        else
            ci.firstdatarow = K.totalrows(ci) + 1
        end
    end
    filter!(ci -> K.nrows(ci) > 0, chunks)
    return chunks
end

_firstlive(chunks) = findfirst(ci -> K.nrows(ci) > 0, chunks)

struct Prepared
    buf::Vector{UInt8}
    bi::K.BufferIndex
    names::Vector{Symbol}
    ncols::Int
    limit::Union{Nothing, Int}
    opts::K.ValueOpts
    d::K.Dialect
    headerlog::K.ProblemLog
    parsekw::NamedTuple   # dialect + value + engine kwargs, ready to splat into K.parse
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
                  buffer_in_memory::Bool=false,
                  prefetch::Bool=true,
                  validate::Bool=true,
                  kw...)
    footerskip >= 0 || throw(ArgumentError("footerskip must be ≥ 0 (got $footerskip)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    allowed = (_DIALECTKW..., _VALUEKW..., _INDEXKW..., _DRIVERKW...)
    _checkkwargs("File/Rows/Chunks", kw, allowed)
    buf = resolvesource(source; buffer_in_memory, prefetch)
    dialectonly = _pickkwargs(kw, _DIALECTKW)
    indexonly = _pickkwargs(kw, _INDEXKW)
    # The first row that MATTERS — the (first) header row, or `skipto` when
    # there is no header row. Everything before it is a skipped prefix: counted
    # as physical lines (quote-blind), never indexed, never sniffed. Row
    # offsets at or after it are quote-aware from that anchor.
    firstrow = header isa Integer && header > 1 ? Int(header) :
               header isa AbstractVector{<:Integer} && !isempty(header) ? Int(first(header)) :
               (header === false || (header isa Integer && header == 0) ||
                (header isa AbstractVector && !(header isa AbstractVector{<:Integer}))) &&
               skipto !== nothing ? Int(skipto) : 1
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
    d = K.Dialect(; delim, dialectonly...)
    opts = K.makevalueopts(d; sentinels, valuekw...)
    cb = chunkbytes === nothing ? K._defaultchunkbytes(length(buf)) : chunkbytes

    # -- the row window, in RAW rows: header rows, skipto, footerskip ---------
    header isa Integer && header < 0 &&
        throw(ArgumentError("header must be ≥ 0 (got $header)"))
    # 0.10 rule: the default header row 1 with skipto=1 means "no header, data
    # starts at row 1" (the header row and the first data row cannot coincide)
    if header isa Integer && header == 1 && skipto !== nothing && skipto == 1
        header = false
    end
    header isa Integer && (header = header == 0 ? false : Int(header))
    headerrows = header isa AbstractVector{<:Integer} ? Int.(header) :
                 header isa Int ? [header] : Int[]
    if !isempty(headerrows)
        (issorted(headerrows) && first(headerrows) >= 1 && allunique(headerrows)) ||
            throw(ArgumentError("header rows must be increasing and ≥ 1 (got $header)"))
    end
    headerrow = header === true ? 1 : isempty(headerrows) ? 0 : last(headerrows)
    # Skipped prefix rows never enter the index (a generated column count would
    # otherwise come from a junk first row; 0.10 took it from the first DATA
    # row) — the index starts at the anchor. Row `n` at/after the anchor is
    # `n - firstrow + 1` quote-aware structural rows from it.
    datastart = anchoroff
    rowoff(n::Int) = n < firstrow ? _physicallineoffset(buf, rawstart, n) :
                                    _rawrowoffset(buf, d, anchoroff, n - firstrow + 1)
    bi = K.index(buf, d; datastart, chunkbytes=cb, parallel, indexonly...)
    chunks = bi.chunks
    headerlog = K.ProblemLog(get(kw, :maxproblems, 10_000))

    names = if header isa AbstractVector && !(header isa AbstractVector{<:Integer}) &&
               !isempty(header)
        Symbol.(header)
    elseif header === false || isempty(chunks) ||
           (header isa AbstractVector && isempty(header))   # header=[] ⇒ generate ColumnN
        k = _firstlive(chunks)
        n = k === nothing ? 0 : K.nfields(chunks[k], chunks[k].firstdatarow)
        [Symbol("Column", j) for j in 1:n]
    elseif header === true || length(headerrows) == 1
        k = _firstlive(chunks)
        k === nothing ? Symbol[] : K.parseheader!(buf, chunks[k], opts, d, headerlog)
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
            push!(parts, K.parseheader!(buf, chunks[k], opts, d, headerlog))
        end
        for (ci, firstrow) in zip(chunks, firstrows)
            ci.firstdatarow = firstrow
        end
        _skiptobyte!(chunks, rowoff(headerrow + 1))
        if isempty(parts)
            Symbol[]
        else
            n = maximum(length, parts)
            [Symbol(join((j <= length(p) ? String(p[j]) : "Column$j" for p in parts), "_"))
             for j in 1:n]
        end
    end
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = K.makeunique!(names)

    if skipto !== nothing
        skipto > headerrow ||
            throw(ArgumentError("skipto=$skipto must be past the header (row $headerrow)"))
        _skiptobyte!(chunks, rowoff(Int(skipto)))
    end
    footer = _footeroffset(buf, d, rawstart, Int(footerskip))
    keep = footerskip == 0 ? sum(K.nrows, chunks; init=0) : _rowsbefore(chunks, footer)
    lim = limit === nothing ? (footerskip > 0 ? keep : nothing) : min(Int(limit), keep)

    # engine + diagnostics kwargs the kernel driver consumes directly
    colopts = nothing
    if dfdict !== nothing
        overrides = K._resolvekeys(dfdict, names, length(names), "dateformat"; validate)
        colopts = K.ValueOpts[haskey(overrides, j) ?
                              K.makevalueopts(d; sentinels, valuekw...,
                                              dateformat=overrides[j]) : opts
                              for j in 1:length(names)]
    end
    passthrough = _pickkwargs(kw, allowed)
    dfdict !== nothing &&
        (passthrough = NamedTuple(kv for kv in pairs(passthrough) if kv.first != :dateformat))
    parsekw = merge(passthrough, (; delim, sentinels, chunkbytes=cb, parallel, colopts, validate))
    return Prepared(buf, bi, names, length(names), lim, opts, d, headerlog, parsekw)
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
    return drop === nothing ? idx : setdiff(1:length(names), idx)
end

# ---------------------------------------------------------------------------
# File — the eager table
# ---------------------------------------------------------------------------

"""
    CSVApi.File(source; kw...)

`CSV.File` analog. `source` is a file path, `IO`, `Vector{UInt8}`, `Cmd`,
a `FilePathsBase` path (with that package loaded), or a vector of any of
these (see "A vector of sources" below).
Keywords (CSV.jl names): `header` (row number | Bool | names | rows-to-merge),
`normalizenames`, `skipto`, `footerskip`, `limit`, `missingstring`, `delim`
(`nothing` ⇒ sniffed), `quotechar`/`openquotechar`/`closequotechar`/`escapechar`,
`quoted`, `comment`, `ignoreemptyrows`, `ignorerepeated`, `dateformat`,
`decimal`, `truestrings`/`falsestrings`, `stripwhitespace`, `groupmark`,
`types` (Type | Vector | Dict), `select`/`drop` (lists), `pool`, `stringtype`,
`strict`/`on_error`, `maxwarnings`/`maxproblems`, `ntasks`/`parallel`,
`nsample`, `buffer_in_memory`, `validate` (`false` ⇒ `types`/`dateformat`/
`pool` keys naming absent columns are ignored instead of erroring).
Diagnostics are data: [`problems`](@ref)`(f)`.

## Pushdown: `scan`

`scan=Tables.Scan(select=..., filter=..., limit=..., offset=...)` is the
shared Tables.jl request for projection, renames, per-column types, row
predicates, and row bounds — every axis is pushed into the parser: unselected
columns are never sampled or parsed, filtered-out rows never cost value work
(the predicate's columns parse first, the rest parse only where it holds),
`limit`/`offset` are exact under any thread count, and a `:col => T` select
item seeds that column's type. The result equals `Tables.scan(File(source),
scan)` — the generic executor — except that type inference for the other
columns sees only qualifying rows. `scan` owns those axes, so `select`/`drop`/
`types`/`limit` are refused alongside it. Requires a Tables.jl with `Scan`.

## String columns: `stringtype` and `pool`

Two independent choices decide what a text column comes back as, and they
compose the same way in `File`, `Chunks`, and `Rows`:

  * `stringtype` — the ELEMENT type. `CompactString` (default): a 16-byte
    value that is the parsed payload itself — short values inline, long
    values zero-copy views into the retained input; hashes and compares
    like `String`, `String(x)` copies out. `String`: every cell
    materialized (one allocation each). With InlineStrings loaded,
    `InlineString` (smallest fitting width per column — 0.10's default
    behavior) or a fixed `String1`…`String255`.
  * `pool` — the CONTAINER for low-cardinality columns. Default `false`:
    nothing pools unless asked (0.10 pooled by default under `(0.2, 500)`;
    that policy is available verbatim by passing it). Accepts `(ratio, cap)`,
    a `Bool`, a ratio, or per-column via
    `Dict(col => spec)` / a vector. A column that pools comes back as a
    `PooledArrays.PooledArray` whose levels are `stringtype` values
    (`CompactString` levels materialize to `String`: pool levels are never
    views); a column that does not pool comes back as a plain vector of
    `stringtype` (`CompactString` ⇒ `CSV.CSVKernel.CompactStringVector`,
    else `Vector{T}`), `Union{T, Missing}` when missings are present.

`Chunks` applies `pool` per batch (each batch is an independent table);
`Rows` has no columns to pool and materializes each accessed cell as
`stringtype` (`CompactString` cells are lazy views).

## A vector of sources

`File(sources::AbstractVector; source=nothing, kw...)` parses each source
with the same keywords and vertically concatenates: the column set is the
FIRST source's columns; a later source contributes to a column by name,
`missing`-fills columns it lacks, and its extra columns are ignored (0.10
semantics). Element types promote across sources (`Int` + `Float64` ⇒
`Float64`); string columns come back as `String` (concatenation owns its
memory — the single-source zero-copy `CompactString` story does not span
buffers). `source=:name` (or `"name"`) appends a `PooledArray` column
recording each row's origin — the path for path sources, `"<source i>"`
otherwise; `source=:name => vals` supplies one label per source. Unlike
0.10, `source=` also works for a one-element vector, and a `source` name
colliding with a data column is an error.
"""
struct File
    name::String
    table::K.ParsedTable
    lookup::Dict{Symbol, Int}
end

function File(source;
              types=nothing, select=nothing, drop=nothing,
              scan=nothing,
              pool=DEFAULT_POOL,
              downcast::Bool=false,
              transpose::Bool=false,
              stringtype::Type=K.CompactString,
              strict::Bool=false, on_error::Symbol=strict ? :error : :collect,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              validate::Bool=true,
              kw...)
    if transpose
        (select !== nothing || drop !== nothing) &&
            throw(ArgumentError("select/drop are not supported with transpose=true"))
        scan === nothing || throw(ArgumentError("scan is not supported with transpose=true"))
        return _transposedfile(source; types, downcast, stringtype, validate, kw...)
    end
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkstringtype(stringtype)
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
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
        p = _prepare(source; parallel, maxproblems=capturecap, validate, kw...)
        t = _executescan(p, scan; parsekw=p.parsekw, maxproblems, on_error)
        # pool keys name the scan's OUTPUT columns (the request already renamed
        # and reordered them)
        t = _poolcolumns(t, _resolvepool(pool, K.names(t), length(K.names(t)); validate); parallel)
        poolS = stringtype === K.CompactString ? String : stringtype
        t = _pooledarrays(t, poolS)
        downcast && (t = _downcast(t))
        stringtype === K.CompactString || (t = _materializestrings(t, stringtype))
        return File(_sourcename(source), t, Dict(n => j for (j, n) in enumerate(K.names(t))))
    end
    p = _prepare(source; parallel, maxproblems=capturecap, validate, kw...)
    return _filefromprepared(p, _sourcename(source); types, select, drop, pool, downcast,
                             stringtype, on_error, maxproblems, parallel, validate)
end

_sourcename(source) = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"

# the typed parse over an existing index — File's classic path, and what
# File(::LazyFile) reuses so the scan is never repeated
function _filefromprepared(p::Prepared, nm::String; types=nothing, select=nothing, drop=nothing,
                           pool=DEFAULT_POOL, downcast::Bool=false, stringtype::Type=K.CompactString,
                           on_error::Symbol=:collect, maxproblems::Int=10_000,
                           parallel::Bool=Threads.nthreads() > 1, validate::Bool=true)
    sel = _resolveselect(select, drop, p.names)
    parsetypes = if p.limit == 0
        Type[T === nothing ? Missing : T for T in K.resolvetypes(types, p.names, p.ncols; validate)]
    else
        types
    end
    poolspecs = _resolvepool(pool, p.names, p.ncols; validate)   # by SOURCE column
    t = K.parse(p.buf; index=p.bi, header=p.names, types=parsetypes, select=sel, limit=p.limit,
                on_error=:collect, p.parsekw...)
    t, firstproblem = _mergeproblems(t, p.headerlog, maxproblems)
    if on_error === :error && firstproblem !== nothing
        pr = firstproblem
        nproblems = length(t.problems) + t.droppedproblems
        throw(ErrorException("CSVKernel: $(pr.kind) at data row $(pr.row), column $(pr.col): " *
                             pr.message * (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
    t = _narrowtypes(t, K.requestedtypes(types, p.names, p.ncols; validate), sel)
    # the kernel emits selected columns in file order: map source specs to them
    keep = sel === nothing ? collect(1:p.ncols) : sort!(unique(sel))
    t = _poolcolumns(t, poolspecs[keep]; parallel)
    poolS = stringtype === K.CompactString ? String : stringtype   # pool levels are never views
    t = _pooledarrays(t, poolS)
    downcast && (t = _downcast(t))
    stringtype === K.CompactString || (t = _materializestrings(t, stringtype))
    return File(nm, t, Dict(n => j for (j, n) in enumerate(K.names(t))))
end

# KernelScan is a sibling submodule included after CSVApi; resolve it through
# the parent at call time (a released Tables without Scan never reaches here —
# the `scan isa Tables.Scan` check above fails first with a clear error).
function _executescan(p::Prepared, scan; parsekw, maxproblems::Int, on_error::Symbol)
    S = Base.parentmodule(@__MODULE__).KernelScan
    return S.execute(p.buf, p.bi, p.names, scan; parsekw, headerlog=p.headerlog,
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
    files = [File(s; kw...) for s in sources]
    counts = [f.table.nrows for f in files]
    total = sum(counts)
    names = copy(K.names(files[1].table))
    cols = AbstractVector[_chaincolumn(
        AbstractVector[_colpiece(f, nm) for f in files], counts, total) for nm in names]
    if source !== nothing
        srcname = Symbol(source isa Pair ? source.first : source)
        srcname in names &&
            throw(ArgumentError("source column name $srcname collides with a data column"))
        vals = source isa Pair ? source.second :
               [s isa AbstractString ? String(s) : "<source $i>"
                for (i, s) in enumerate(sources)]
        expanded = eltype(vals)[vals[i] for i in eachindex(files) for _ in 1:counts[i]]
        push!(names, srcname)
        push!(cols, PooledArray(expanded))
    end
    probs = K.Problem[]
    dropped = 0
    off = 0
    for f in files
        t = f.table
        for pr in t.problems
            push!(probs, K.Problem(pr.row == 0 ? 0 : pr.row + off,
                                   pr.col, pr.pos, pr.kind, pr.message))
        end
        dropped += t.droppedproblems
        off += t.nrows
    end
    t = K.ParsedTable(names, cols, total, probs, dropped)
    return File("<$(length(sources)) sources>", t,
                Dict(n => j for (j, n) in enumerate(names)))
end

# a source that lacks a column contributes an all-missing block
const EMPTY_COLUMN = Union{}[]

function _colpiece(f::File, nm::Symbol)
    j = get(f.lookup, nm, 0)
    return j == 0 ? EMPTY_COLUMN : f.table.columns[j]
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

function _mergeproblems(t::K.ParsedTable, headerlog::Union{Nothing, K.ProblemLog}, cap::Int)
    # clean parse: nothing to merge, sort, or cap — return the table as-is
    if isempty(t.problems) && t.droppedproblems == 0 &&
       (headerlog === nothing || (isempty(headerlog.items) && headerlog.dropped == 0))
        return t, nothing
    end
    log = K.ProblemLog(cap)
    if headerlog !== nothing
        if headerlog.first !== nothing
            first = headerlog.first
            (log.first === nothing || K.problemless(first, log.first)) && (log.first = first)
        end
        for pr in headerlog.items
            K.pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
        end
    end
    for pr in t.problems
        K.pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
    end
    log.dropped += t.droppedproblems +
                   (headerlog === nothing ? 0 : headerlog.dropped)
    K.sortproblems!(log)
    table = K.ParsedTable(t.names, t.columns, t.nrows, log.items, log.dropped)
    return table, log.first
end

# ---------------------------------------------------------------------------
# transpose=true — the compatibility path. Rows are columns: input row j is
# output column j; with header=true the first field of each row is that
# column's name. Types are inferred EXACTLY (every retained cell participates —
# these files are small by construction), or taken from `types`. Single-threaded,
# strings materialize as String. select/drop are not supported here.
# ---------------------------------------------------------------------------
function _cellstring(buf::Vector{UInt8}, ci, lr::Int, f::Int, opts)
    sp = K.fieldspan(ci, lr, f)
    sp === nothing && return ""
    cpos, clen, esc, st = K.cellcontent(buf, sp[1], sp[2], opts)
    st == K.CELL_VALUE || return ""
    if esc
        tmp = UInt8[]
        K._unescape_append!(tmp, buf, cpos, clen, opts.e, opts.cq)
        return String(tmp)
    end
    return String(buf[cpos:(cpos + clen - 1)])
end

function _transposedcolumn(buf::Vector{UInt8}, ci, lr::Int, startf::Int, n::Int,
                           T0, opts)
    nf = K.nfields(ci, lr)
    T = T0
    if T === nothing
        T = Missing
        for f in startf:min(nf, startf + n - 1)
            sp = K.fieldspan(ci, lr, f)
            sp === nothing && continue
            T = K.promote_kernel(T, K.detecttype(buf, sp[1], sp[2], opts))
        end
    end
    T === Missing && return fill(missing, n)
    if T === String
        out = Vector{Union{String, Missing}}(missing, n)
        sawmiss = nf - (startf - 1) < n
        for i in 1:min(n, nf - (startf - 1))
            f = startf + i - 1
            sp = K.fieldspan(ci, lr, f)
            if sp === nothing || sp[2] == 0
                sawmiss = true
                continue
            end
            cpos, clen, esc, st = K.cellcontent(buf, sp[1], sp[2], opts)
            if st != K.CELL_VALUE
                sawmiss = true
                continue
            end
            out[i] = esc ? (tmp = UInt8[];
                            K._unescape_append!(tmp, buf, cpos, clen, opts.e, opts.cq);
                            String(tmp)) :
                     String(buf[cpos:(cpos + clen - 1)])
        end
        return sawmiss ? out : convert(Vector{String}, out)
    end
    out = Vector{Union{T, Missing}}(missing, n)
    scratch = K._scratchfor(opts)
    sawmiss = nf - (startf - 1) < n
    for i in 1:min(n, nf - (startf - 1))
        f = startf + i - 1
        sp = K.fieldspan(ci, lr, f)
        if sp === nothing || sp[2] == 0
            sawmiss = true
            continue
        end
        cpos, clen, esc, st = K.cellcontent(buf, sp[1], sp[2], opts)
        if st != K.CELL_VALUE || esc || clen == 0
            st == K.CELL_VALUE && (esc || clen == 0) && T0 === nothing &&
                return _transposedcolumn(buf, ci, lr, startf, n, String, opts)
            sawmiss = true
            continue
        end
        ti, tj = K._trimblanks(buf, cpos, cpos + clen - 1)   # typed values tolerate blanks
        if ti > tj
            ti, tj = cpos, cpos + clen - 1
        end
        v, ok = K.parsevalue(T, buf, ti, tj, opts, scratch)
        if !ok
            # exact inference cannot conflict; a user-pinned type leaves the
            # cell missing (strict=false File semantics)
            T0 === nothing && return _transposedcolumn(buf, ci, lr, startf, n, String, opts)
            sawmiss = true
            continue
        end
        out[i] = v
    end
    return sawmiss ? out : convert(Vector{T}, out)
end

function _transposedfile(source; types=nothing, downcast::Bool=false,
                         stringtype::Type=K.CompactString,
                         header::Union{Bool, Integer, AbstractVector}=true,
                         skipto::Union{Nothing, Integer}=nothing,
                         missingstring=nothing, delim=',',
                         normalizenames::Bool=false, limit::Union{Nothing, Integer}=nothing,
                         validate::Bool=true,
                         buffer_in_memory::Bool=false, prefetch::Bool=true, kw...)
    allowed = (_DIALECTKW..., _VALUEKW...)
    _checkkwargs("File(transpose=true)", kw, allowed)
    # transposed geometry (0.10 semantics): header=N takes each row's Nth field
    # as that column's name; skipto=M starts data at field M (default: the field
    # after the header, or field 1 without one); header=[names] is explicit
    namefield = header === true ? 1 : header === false ? 0 :
                header isa Integer ? Int(header) : 0
    explicitnames = header isa AbstractVector && !(header isa AbstractVector{<:Integer}) ?
                    Symbol.(header) : nothing
    header isa AbstractVector{<:Integer} &&
        throw(ArgumentError("transpose=true takes a single header field index, not a range"))
    hasnames = namefield > 0
    startf = skipto === nothing ? namefield + 1 : Int(skipto)
    startf >= 1 || throw(ArgumentError("skipto must be ≥ 1 (got $skipto)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    hasnames && startf <= namefield &&
        throw(ArgumentError("skipto=$skipto must be past the header field $namefield"))
    buf = resolvesource(source; buffer_in_memory, prefetch)
    dialectkw = _pickkwargs(kw, _DIALECTKW)
    valuekw = _pickkwargs(kw, _VALUEKW)
    d = K.Dialect(; delim, dialectkw...)
    opts = K.makevalueopts(d; sentinels=_sentinels(missingstring), valuekw...)
    bi = K.index(buf, d; datastart=_datastart(buf), parallel=false)
    rows = Tuple{Any, Int}[]
    for ci in bi.chunks, lr in ci.firstdatarow:K.totalrows(ci)
        push!(rows, (ci, lr))
    end
    ncols = length(rows)
    n = ncols == 0 ? 0 :
        maximum(K.nfields(r[1], r[2]) - (startf - 1) for r in rows)
    n = max(n, 0)
    limit === nothing || (n = min(n, max(Int(limit), 0)))
    _tname(j, r) = (nm = hasnames ? _cellstring(buf, r[1], r[2], namefield, opts) : "";
                    isempty(nm) ? Symbol("Column", j) : Symbol(nm))
    names = explicitnames !== nothing ? copy(explicitnames) :
            Symbol[_tname(j, r) for (j, r) in enumerate(rows)]
    explicitnames !== nothing && length(names) != ncols &&
        throw(ArgumentError("header has $(length(names)) names for $ncols transposed rows"))
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = K.makeunique!(names)
    seed = K.resolvetypes(types, names, ncols; validate)
    cols = AbstractVector[_transposedcolumn(buf, r[1], r[2], startf, n, seed[j], opts)
                          for (j, r) in enumerate(rows)]
    t = K.ParsedTable(names, cols, n, K.Problem[], 0)
    downcast && (t = _downcast(t))
    nm = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
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
    cap = Int(pool[2])
    cap >= 0 || throw(ArgumentError("pool cap must be nonnegative (got $cap)"))
    return (Float64(ratio), cap)
end

# pool as a scalar policy, Dict(col => spec), or per-column vector → one spec
# per column of `names` (nothing = never pool)
function _resolvepool(pool, names::Vector{Symbol}, ncols::Int; validate::Bool=true)
    if pool isa AbstractDict
        specs = Vector{Union{Nothing, Tuple{Float64, Int}}}(nothing, ncols)
        for (j, sp) in K._resolvekeys(pool, names, ncols, "pool"; validate)
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
function _poolcolumn(c::K.CompactStringVector, ps::Tuple{Float64, Int}; parallel::Bool=true)
    n = length(c)
    n == 0 && return nothing
    ratiolevels = ps[1] == 1.0 ? n : floor(Int, ps[1] * n)
    maxlevels = min(ratiolevels, ps[2], Int(typemax(UInt32)))
    maxlevels <= 0 && return nothing
    nt = parallel ? clamp(n ÷ 65_536, 1, 4 * Threads.nthreads()) : 1
    bounds = [1 + (t - 1) * n ÷ nt for t in 1:nt]
    push!(bounds, n + 1)
    refs = zeros(UInt32, n)
    locals = Vector{Tuple{Vector{K.CompactStringPayload}, Vector{K.CompactString}}}(undef, nt)
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
        levels = K.CompactStringPayload[]
        globalof = Dict{K.CompactString, UInt32}()
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
    lv = K.CompactStringVector{K.CompactString}(levels, c.buf, c.extra)
    return Missing <: eltype(c) ? K.PooledColumn{Union{K.CompactString, Missing}}(refs, lv) :
                                  K.PooledColumn{K.CompactString}(refs, lv)
end

# intern rows lo..hi of `c` into a fresh local table; refs get LOCAL ids
function _internrange!(refs::Vector{UInt32}, c::K.CompactStringVector, lo::Int, hi::Int,
                       maxlevels::Int, aborted::Threads.Atomic{Bool})
    table = Dict{K.CompactString, UInt32}()
    levels = K.CompactStringPayload[]
    keys = K.CompactString[]
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
function _poolcolumns(t::K.ParsedTable, specs::AbstractVector; parallel::Bool=true)
    js = [j for (j, c) in enumerate(t.columns)
          if c isa K.CompactStringVector && j <= length(specs) && specs[j] !== nothing]
    isempty(js) && return t
    cols = AbstractVector[t.columns...]
    pooled = Vector{Any}(nothing, length(js))
    poolone = i -> (pooled[i] = _poolcolumn(cols[js[i]]::K.CompactStringVector, specs[js[i]]; parallel))
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
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# user-requested narrow types (Int8/16/32, UInt*, Float16/32): the kernel
# parsed the native type; convert here. A value outside the narrow range
# becomes missing with a recorded problem (0.10 semantics, strict=false).
function _narrowtypes(t::K.ParsedTable, req, sel)
    all(x -> x === nothing, req) && return t
    # The kernel emits selected columns in file order. `sel` retains the user's
    # request order and may contain duplicates, so normalize it before mapping
    # output position back to the file-column-indexed `req` vector.
    keep = sel === nothing ? collect(1:length(req)) : sort!(unique(sel))
    cols = AbstractVector[t.columns...]
    problems = copy(t.problems)
    for (o, j) in enumerate(keep)
        T = req[j]
        T === nothing && continue
        c = cols[o]
        Base.nonmissingtype(eltype(c)) in (Int64, Int128, Float64) || continue
        out = Vector{Union{T, Missing}}(undef, length(c))
        # The kernel widens a user-declared Union{Missing,T} before this door.
        # Preserve that declaration even when every value is present.
        anymissing = Missing <: eltype(c)
        @inbounds for i in eachindex(c)
            x = c[i]
            if x === missing
                out[i] = missing
                anymissing = true
            elseif T <: Integer && !(typemin(T) <= x <= typemax(T))
                out[i] = missing
                anymissing = true
                push!(problems, K.Problem(i, j, 0, :invalid_value, "value $x does not fit $T"))
            else
                out[i] = convert(T, x)
            end
        end
        cols[o] = anymissing ? out : convert(Vector{T}, out)
    end
    return K.ParsedTable(t.names, cols, t.nrows, problems, t.droppedproblems)
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
function _downcast(t::K.ParsedTable)
    cols = AbstractVector[_downcastcol(c) for c in t.columns]
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# PooledColumn -> PooledArrays.PooledArray, the ecosystem dictionary type.
# Levels materialize to String (at most the pool cap of them); refs are shared
# outright for missing-free columns and remapped once — missing joins the pool,
# CSV.jl's convention — otherwise. Measured 0.2-0.8 ms on 20 MiB shapes.
function _topooledarray(c::K.PooledColumn{ELT}, ::Type{S0}=String) where {ELT, S0}
    n = length(c.levels)
    lv = _levelvector(S0, c.levels, n)   # an abstract S0 (InlineString) resolves to a width here
    S = eltype(lv)
    if !(Missing <: ELT)
        invpool = Dict{S, UInt32}(lv[i] => UInt32(i) for i in 1:n)
        return PooledArray(PooledArrays.RefArray(K.poolrefs(c)), invpool, lv)
    end
    pool = Vector{Union{S, Missing}}(undef, n + 1)
    @inbounds for i in 1:n
        pool[i] = lv[i]
    end
    pool[n + 1] = missing
    invpool = Dict{Union{S, Missing}, UInt32}(pool[i] => UInt32(i) for i in 1:(n + 1))
    oldrefs = K.poolrefs(c)
    refs = similar(oldrefs)
    mref = UInt32(n + 1)
    @inbounds @simd for i in eachindex(refs)
        r = oldrefs[i]
        refs[i] = r == 0 ? mref : r
    end
    return PooledArray(PooledArrays.RefArray(refs), invpool, pool)
end

function _pooledarrays(t::K.ParsedTable, ::Type{S}=String) where {S}
    any(c -> c isa K.PooledColumn, t.columns) || return t
    cols = AbstractVector[c isa K.PooledColumn ? _topooledarray(c, S) : c
                          for c in t.columns]
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# --- the string-output hook -------------------------------------------------
# `stringtype` names the element type string columns come out as. The core
# knows CompactString (the default; zero-copy views) and String (bulk
# materialization). Extensions register more by adding methods to
# `_stringsink` (validation) and `_materializecolumn` / `_levelvector`
# (conversion): CSVInlineStringsExt registers InlineString (auto-width per
# column) and the fixed String1..String255.
_stringsink(::Type{K.CompactString}) = true
_stringsink(::Type{String}) = true
_stringsink(::Type) = false
_checkstringtype(T) =
    (T isa Type && _stringsink(T)) ||
        throw(ArgumentError("stringtype must be CSVKernel.CompactString, String, or a " *
                            "type provided by an extension (e.g. InlineString with " *
                            "InlineStrings loaded); got $T"))

# a CompactStringVector to Vector{S} / Vector{Union{S,Missing}}. String goes
# through K.materialize's bulk path — one shared scratch, word-store inline
# reconstruction, unsafe_string per cell; a per-cell String() broadcast ran the
# generic AbstractString path and was a measured 55–110 MiB/s cliff on
# string-heavy shapes.
_materializecolumn(::Type{String}, col::K.CompactStringVector) = K.materialize(col)
# pool levels (a CompactStringVector) to Vector{S}
_levelvector(::Type{String}, levels::K.CompactStringVector, n::Int) =
    String[String(levels[i]) for i in 1:n]

function _materializestrings(t::K.ParsedTable, ::Type{S}=String) where {S}
    cols = AbstractVector[col isa K.CompactStringVector ? _materializecolumn(S, col) : col
                          for col in t.columns]
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# --- Tables.jl + row access -------------------------------------------------

# NB: getproperty resolves COLUMNS first (f.score), so interface methods must
# reach internals via getfield — a column named `table` must not shadow them.
Tables.istable(::Type{File}) = true
Tables.columnaccess(::Type{File}) = true
Tables.rowaccess(::Type{File}) = true
Tables.columns(f::File) = getfield(f, :table)
Tables.rows(f::File) = f
Tables.columnnames(f::File) = K.names(getfield(f, :table))
Tables.getcolumn(f::File, i::Int) = K.columns(getfield(f, :table))[i]
Tables.getcolumn(f::File, nm::Symbol) = getfield(f, :table)[nm]
Tables.rowcount(f::File) = getfield(f, :table).nrows
Tables.schema(f::File) = Tables.schema(getfield(f, :table))

problems(f::File) = K.problems(getfield(f, :table))
problems(t::K.ParsedTable) = K.problems(t)

Base.names(f::File) = K.names(getfield(f, :table))
Base.propertynames(f::File) = K.names(getfield(f, :table))
function Base.getproperty(f::File, nm::Symbol)
    lk = getfield(f, :lookup)
    haskey(lk, nm) && return K.columns(getfield(f, :table))[lk[nm]]
    return getfield(f, nm)
end

struct FileRow <: Tables.AbstractRow
    f::File
    row::Int
end

Base.length(f::File) = getfield(f, :table).nrows
Base.eltype(::Type{File}) = FileRow
Base.iterate(f::File, i::Int=1) = i > length(f) ? nothing : (FileRow(f, i), i + 1)
Base.getindex(f::File, i::Int) = (1 <= i <= length(f) || throw(BoundsError(f, i)); FileRow(f, i))

Tables.columnnames(r::FileRow) = K.names(getfield(getfield(r, :f), :table))
Tables.getcolumn(r::FileRow, j::Int) =
    K.columns(getfield(getfield(r, :f), :table))[j][getfield(r, :row)]
Tables.getcolumn(r::FileRow, nm::Symbol) =
    Tables.getcolumn(r, getfield(getfield(r, :f), :lookup)[nm])
rownumber(r::FileRow) = getfield(r, :row)

function Base.show(io::IO, f::File)
    t = getfield(f, :table)
    println(io, "CSVApi.File($(repr(getfield(f, :name)))):")
    println(io, "Size: $(t.nrows) x $(length(K.names(t)))")
    show(io, Tables.schema(t))
    nproblems = length(t.problems) + t.droppedproblems
    nproblems > 0 &&
        print(io, "\n$nproblems problem(s) recorded — $(length(t.problems)) retained by problems(f)")
end

"""
    CSVApi.read(source, sink; kw...)

`CSV.read` analog: parse `source` (same keywords as [`File`](@ref)) straight
into a Tables.jl `sink` — `read(path, DataFrame)`, `read(path, columntable)`,
`read(path, Tables.matrix)`, or any function of a table. The sink is CALLED
(0.10 semantics): a type sink runs its constructor, a function sink runs. The
parsed columns are freshly allocated, so they are handed over as
`Tables.CopiedColumns` — sinks that honor it (DataFrame) take ownership
without copying.
"""
read(source, sink; kw...) = sink(Tables.CopiedColumns(File(source; kw...)))

# ---------------------------------------------------------------------------
# lazy / LazyFile — the structural index AS a table
# ---------------------------------------------------------------------------
"""
    CSVApi.lazy(source; types=nothing, stringtype=CompactString, kw...) -> LazyFile

The fastest possible first look at a file: build the structural index (the
parallel scan that finds every row and field — a fraction of a full parse) and
return a table whose cells materialize only when touched. Every column is a
lazy vector of `CompactString`s (zero-copy views into the input; `missing` for
empty cells) with O(1) random access; `nrows`/`size`, names, `lf[i, j]`,
`lf.col`, `lf[:col]`, and the Tables.jl columns interface all work, so
`DataFrame(lf)` or `Tables.columntable(lf)` materialize on demand. Pass
`types=Dict(:price => Float64)` (or a Type/Vector) to make chosen columns
parse on access instead. When you decide the file is worth a full parse,
`File(lf; kw...)` reuses the index — the scan is never repeated.

`stringtype=String` materializes each accessed cell as a `String`. Row-
positional (`header`, `skipto`, `footerskip`, `limit`), dialect, and value
keywords match [`File`](@ref); `select`/`drop` apply too (unselected columns
are simply not offered).
"""
function lazy(source; types=nothing, stringtype::Type=K.CompactString,
              select=nothing, drop=nothing, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW..., :validate)
    _checkkwargs("lazy", kw, allowed)
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    validate = get(kw, :validate, true)
    sel = _resolveselect(select, drop, p.names)
    seed = types === nothing ? nothing :
           K.resolvetypes(types, p.names, p.ncols; validate)
    chunks = p.bi.chunks
    rowbases = cumsum([0; Int[K.nrows(ci) for ci in chunks[1:max(length(chunks) - 1, 0)]]])
    total = sum(K.nrows, chunks; init=0)
    nr = p.limit === nothing ? total : min(total, p.limit)
    js = sel === nothing ? collect(1:p.ncols) : sel
    cols = AbstractVector[]
    for j in js
        T = seed === nothing ? nothing : seed[j]
        c = T === nothing || T === String ?
                LazyColumn{_lazyeltype(stringtype)}(p.buf, chunks, rowbases, j, p.opts, nr, stringtype) :
                LazyColumn{Union{T, Missing}}(p.buf, chunks, rowbases, j, p.opts, nr, T)
        push!(cols, c)
    end
    names = p.names[js]
    return LazyFile(_sourcename(source), p, names, cols, nr,
                    Dict(nm => i for (i, nm) in enumerate(names)))
end
_lazyeltype(::Type{K.CompactString}) = Union{K.CompactString, Missing}
_lazyeltype(::Type{S}) where {S} = Union{S, Missing}

"""
    LazyColumn{ELT}

One column of a [`LazyFile`](@ref): an `AbstractVector` whose `getindex`
locates the cell through the structural index (a chunk lookup and an O(1)
field-span read), then returns a `CompactString` view (or the requested
string type / parsed value). Nothing is stored per cell.
"""
struct LazyColumn{ELT, T} <: AbstractVector{ELT}   # T: CompactString | String | extension string type | a value type
    buf::Vector{UInt8}
    chunks::Vector{K.ChunkIndex}
    rowbases::Vector{Int}
    j::Int
    opts::K.ValueOpts
    nrows::Int
    hint::Base.RefValue{Int}   # last chunk touched: sequential/local access skips the search
end
LazyColumn{ELT}(buf, chunks, rowbases, j, opts, nrows, ::Type{T}) where {ELT, T} =
    LazyColumn{ELT, T}(buf, chunks, rowbases, j, opts, nrows, Ref(1))
_lazytarget(::LazyColumn{ELT, T}) where {ELT, T} = T
Base.size(c::LazyColumn) = (c.nrows,)
Base.IndexStyle(::Type{<:LazyColumn}) = IndexLinear()

# global row → (chunk, local row); rowbases is nondecreasing. The hint makes
# a scan of the column O(1) per cell (a stale hint from another task is only
# a hint: it is validated, and any task may overwrite it).
@inline function _lazylocate(c::LazyColumn, i::Int)
    k = c.hint[]
    @inbounds if !(1 <= k <= length(c.chunks) &&
                   c.rowbases[k] < i <= c.rowbases[k] + K.nrows(c.chunks[k]))
        k = searchsortedlast(c.rowbases, i - 1)
        c.hint[] = k
    end
    ci = @inbounds c.chunks[k]
    return ci, ci.firstdatarow + (i - @inbounds(c.rowbases[k])) - 1
end

function Base.getindex(c::LazyColumn, i::Int)
    @boundscheck checkbounds(c, i)
    ci, lr = _lazylocate(c, i)
    sp = K.fieldspan(ci, lr, c.j)
    sp === nothing && return missing                       # short row
    pos, len = sp
    len == 0 && return missing
    return _lazyvalue(c, pos, len)
end
@inline function _lazyvalue(c::LazyColumn{ELT, T}, pos::Int, len::Int) where {ELT, T}
    cpos, clen, esc, st = K.cellcontent(c.buf, pos, len, c.opts)
    st == K.CELL_MISSING && return missing
    if T === K.CompactString || T === String || !(T <: Number || T <: Dates.TimeType || T === Bool || T === Base.UUID)
        # a string cell: zero-copy view, unless quoting demands unescaping
        # (or the structural quote reading is malformed — keep the raw bytes)
        if st == K.CELL_BADQUOTE
            cpos, clen, esc = pos, len, false
        end
        s = if esc
            bytes = K._unescape_bytes(c.buf, Int64(cpos), Int32(clen), c.opts.e, c.opts.cq)
            n = length(bytes)
            n <= K.COMPACTSTRING_INLINE ? K.CompactString(K.inline_payload(bytes, 1, n), K.EMPTY_BYTES) :
                                          K.CompactString(K.view_payload(bytes, 1, n, 0, 0), bytes)
        elseif clen <= K.COMPACTSTRING_INLINE
            K.CompactString(K.inline_payload(c.buf, cpos, clen), K.EMPTY_BYTES)
        else
            K.CompactString(K.view_payload(c.buf, cpos, clen, 0, cpos - 1), c.buf)
        end
        return T === K.CompactString ? s : convert(T, String(s))
    end
    # a typed cell: the same kernels File uses, on demand
    (st == K.CELL_BADQUOTE || clen == 0 || esc) && return missing
    v, ok = K.parsevalue(T, c.buf, cpos, cpos + clen - 1, c.opts)
    return ok ? v : missing
end

# Sequential access (collect, sum, DataFrame(lf), display) walks chunk by
# chunk with no per-cell chunk lookup; only random access pays the search.
@inline function _lazycell(c::LazyColumn, ci::K.ChunkIndex, lr::Int)
    sp = K.fieldspan(ci, lr, c.j)
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
        if lr <= K.totalrows(ci)
            return _lazycell(c, ci, lr), (k, lr + 1, done + 1)
        end
        k += 1
        lr = 0
    end
    return nothing
end

"""
    CSVApi.LazyFile

The table [`lazy`](@ref) returns. Columns are [`LazyColumn`](@ref)s;
`File(lf; kw...)` parses it fully on the same index.
"""
struct LazyFile
    name::String
    prepared::Prepared
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

"""
    File(lf::LazyFile; kw...)

Full typed parse of a lazily indexed file, reusing its structural index —
the scan is not repeated. `kw` are `File`'s type/string/pool/engine keywords
(row-positional and dialect choices were fixed when `lf` was created).
"""
function File(lf::LazyFile; types=nothing, select=nothing, drop=nothing, pool=DEFAULT_POOL,
              downcast::Bool=false, stringtype::Type=K.CompactString, strict::Bool=false,
              on_error::Symbol=strict ? :error : :collect, maxproblems::Int=10_000,
              parallel::Bool=Threads.nthreads() > 1, validate::Bool=true)
    _checkstringtype(stringtype)
    return _filefromprepared(getfield(lf, :prepared), getfield(lf, :name); types, select, drop,
                             pool, downcast, stringtype, on_error, maxproblems, parallel, validate)
end

# ---------------------------------------------------------------------------
# Rows — streaming
# ---------------------------------------------------------------------------

"""
    CSVApi.Rows(source; types=nothing, kw...)

`CSV.Rows` analog: iterate lightweight row views; each cell materializes only
on access (`Union{String, Missing}` by default). `types` (Type | Vector |
Dict) makes indexing return typed values parsed on demand through the same
kernel value layer. Row-positional keywords (`header`, `skipto`, `limit`,
`footerskip`) and dialect/value keywords match [`File`](@ref).
"""
struct Rows
    inner::E.Rows
    types::Union{Nothing, Vector{Type}}
    limit::Union{Nothing, Int}
    stringtype::Type
end

"""
    CSVApi.Rows(source; types=nothing, stringtype=CompactString, kw...)

`CSV.Rows` analog: stream rows without materializing columns. Cells are lazy
views by default (`CompactString` for text, on-demand typed access when
`types` is given); `stringtype=String` (or an extension type such as
`InlineString`) materializes each string cell as it is accessed. `reusebuffer`
is accepted for 0.10 compatibility and inert: there is no per-row buffer.
"""
function Rows(source; types=nothing, reusebuffer::Bool=false,
              stringtype::Type=K.CompactString, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW...)
    _checkkwargs("Rows", kw, allowed)
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    seed = types === nothing ? nothing :
           Type[T === nothing ? String : T
                for T in K.resolvetypes(types, p.names, p.ncols; validate=get(kw, :validate, true))]
    inner = E.Rows(p.buf, p.bi.chunks, p.names,
                   Dict(nm => j for (j, nm) in enumerate(p.names)), p.opts, p.d)
    return Rows(inner, seed, p.limit, stringtype)
end

Tables.istable(::Type{Rows}) = true
Tables.rowaccess(::Type{Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = r.types === nothing ?
    Tables.Schema(r.inner.names, fill(Union{_rowstringtype(r.stringtype), Missing},
                                      length(r.inner.names))) :
    Tables.Schema(r.inner.names, Type[Union{T, Missing} for T in r.types])
_rowstringtype(T) = T === K.CompactString ? K.CompactString : T

struct Row <: Tables.AbstractRow
    view::E.RowView
    types::Union{Nothing, Vector{Type}}
    stringtype::Type
end

Base.eltype(::Type{Rows}) = Row
Base.IteratorSize(::Type{Rows}) = Base.SizeUnknown()

function Base.iterate(r::Rows, state=((1, nothing, 1)))
    r.limit !== nothing && state[3] > r.limit && return nothing
    it = iterate(r.inner, state)
    it === nothing && return nothing
    view, next = it
    return Row(view, r.types, r.stringtype), next
end

Tables.columnnames(row::Row) = Tables.columnnames(getfield(row, :view))
function Tables.getcolumn(row::Row, j::Int)
    ts = getfield(row, :types)
    v = getfield(row, :view)
    x = ts === nothing ? v[j] : E.typedvalue(ts[j], v, j)
    st = getfield(row, :stringtype)
    return st === K.CompactString || !(x isa K.CompactString) ? x : _rowstring(st, x)
end
# per-cell string materialization for Rows(stringtype=...); extensions may add
_rowstring(::Type{String}, x::K.CompactString) = String(x)
Tables.getcolumn(row::Row, nm::Symbol) =
    Tables.getcolumn(row, getfield(getfield(row, :view), :r).lookup[nm])
Base.getindex(row::Row, j::Int) = Tables.getcolumn(row, j)
Base.getindex(row::Row, nm::Symbol) = Tables.getcolumn(row, nm)
rownumber(row::Row) = getfield(getfield(row, :view), :rownumber)

# ---------------------------------------------------------------------------
# Chunks — batched
# ---------------------------------------------------------------------------

"""
    CSVApi.Chunks(source; ntasks=Threads.nthreads(), kw...)

`CSV.Chunks` analog: iterate the file as a sequence of `File`-shaped tables.
Unlike `CSV.Chunks`, every batch reports the SAME column types — a whole-window
schema prepass over the index makes the batch schema stable by construction.
`ntasks` sizes the batches (or pass `chunkbytes` directly). `stringtype` and
`pool` behave as in [`File`](@ref) (pooling is per batch; a single policy).
"""
struct Chunks
    inner::E.Batches
    headerlog::K.ProblemLog
    maxproblems::Int
    stringtype::Type
    poolspec::Union{Nothing, Tuple{Float64, Int}}
end

Base.length(c::Chunks) = length(getfield(c, :inner))
Base.eltype(::Type{Chunks}) = K.ParsedTable
Tables.partitions(c::Chunks) = c

function Base.iterate(c::Chunks, state::Int=1)
    it = iterate(getfield(c, :inner), state)
    it === nothing && return nothing
    t, next = it
    headerlog = state == 1 ? getfield(c, :headerlog) : nothing
    t, _ = _mergeproblems(t, headerlog, getfield(c, :maxproblems))
    # every batch leaves through the same door as File: PooledArray for
    # pooled columns (levels in the output string type), then the string
    # materialization the caller asked for
    st = getfield(c, :stringtype)
    poolS = st === K.CompactString ? String : st
    ps = getfield(c, :poolspec)
    ps === nothing || (t = _poolcolumns(t, fill(ps, length(t.columns))))
    t = _pooledarrays(t, poolS)
    st === K.CompactString || (t = _materializestrings(t, st))
    return t, next
end

function Chunks(source; types=nothing, ntasks::Union{Nothing, Int}=nothing,
                maxproblems::Int=10_000, stringtype::Type=K.CompactString,
                pool=DEFAULT_POOL, kw...)
    nt = something(ntasks, Threads.nthreads())
    nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkstringtype(stringtype)
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
    p = _prepare(source; maxproblems=capturecap, kw...)
    chunks = p.bi.chunks
    fullrows = sum(K.nrows, chunks; init=0)
    p.limit === nothing || _limitrows!(chunks, p.limit)
    filter!(ci -> K.nrows(ci) > 0, chunks)
    seed = K.resolvetypes(types, p.names, p.ncols; validate=get(kw, :validate, true))
    userprovided = [T !== nothing for T in seed]
    if any(isnothing, seed)
        total = sum(K.nrows, chunks; init=0)
        inferred = K.sampletypes(p.buf, chunks, p.ncols, p.opts; nsample=max(1, total))
        for j in 1:p.ncols
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    seedtypes = Type[T for T in seed]
    allowmissing = E.schemamissing(p.buf, chunks, seedtypes, p.opts)
    unclosedquote = p.bi.unclosedquote && (p.limit === nothing || p.limit >= fullrows)
    inner = E.Batches(p.buf, chunks, p.names, seedtypes, userprovided, allowmissing,
                      p.opts, p.d, capturecap, unclosedquote)
    return Chunks(inner, p.headerlog, maxproblems, stringtype, poolspec)
end

end # module CSVApi
