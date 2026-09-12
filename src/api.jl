# The public readers and the internal delimiter/shape detection.
#
# Every entry point uses the same pipeline: resolve source bytes → settle the
# dialect (sniffing if asked) → index once (rebuilt under the field-start
# quote rule when a bare quote is flagged) → settle names/row-window
# (header/skipto/footerskip/limit as *index arithmetic*, before any value
# work) → hand the parse driver or the streaming primitives the prepared
# index. There is no per-entrypoint parsing code and no mode flags inside the
# driver: File/Rows/Chunks differ only in what they do AFTER `_prepare`.
#
# Reader conventions:
#   • problems are retained data; eager readers also warn once by default
#     (`strict=true` maps to `on_error=:error`, `maxwarnings` to `maxproblems`)
#   • empty unquoted cells are ALWAYS missing; `missingstring` ADDS spellings
#   • `select`/`drop`/`types` take lists, names, regexes, or dictionaries
#     (Tables.Scan is the expression channel)
#   • `stringtype` defaults to DataString; `stringtype=String` materializes;
#     InlineStrings are an extension
#   • Bool defaults accept lower, title, and upper case; user lists replace them
#   • integer spellings that fit Int128 stay exact, including initially-wide
#     and grouped columns
#
using Tables, Unicode, Mmap, PooledArrays, CodecZlib, Downloads

# `sniff`/`Spec` are internal (behind `delim=nothing`); not exported.

# No pooling unless asked: dictionary encoding costs a pass over every text
# column, and most consumers do not need it. `pool=(0.2, 500)` enables the
# ratio-and-cap policy; `pool=true` pools every string column.
const DEFAULT_POOL = false
# Pooled references are UInt32, but a 32-bit Julia process cannot represent
# UInt32's full maximum as Int. Cap at the smaller index space without an
# overflowing conversion during precompile.
const _MAX_POOL_LEVELS = Int(min(UInt64(typemax(Int)), UInt64(typemax(UInt32))))

const _DIALECTKW = (:quotechar, :openquotechar, :closequotechar, :escapechar,
                    :quoted, :comment, :ignoreemptyrows, :ignorerepeated)
const _VALUEKW = (:dateformat, :decimal, :truestrings, :falsestrings,
                  :stripwhitespace, :groupmark)
const _INDEXKW = (:fastindex,)
const _DRIVERKW = (:maxproblems, :nsample, :typemap)

function _pickkwargs(kw, allowed)
    return NamedTuple(p for p in pairs(kw) if p.first in allowed)
end

const _REMOVED_KW = Dict{Symbol, String}(
    :silencewarnings => "use on_error=:collect to silence warnings; problems(f) returns retained problems",
    :inferdecimal => "CSV does not infer decimal types; request one with types, for example " *
                     "types=Dict(:amount => DataDecimals.Decimal64{2})",
    :debug => "removed in 1.0; parse problems and the structural index are inspectable directly",
    :lazystrings => "use stringtype=DataStrings.DataString (the default) or stringtype=String",
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

Base.@nospecializeinfer function _probedelim(@nospecialize(dialectkw))
    quotechar = get(dialectkw, :quotechar, '"')
    openquotechar = get(dialectkw, :openquotechar, nothing)
    oq = something(openquotechar, quotechar)
    for c in ('\x1f', '\x1e', '\x1d')
        c == oq || return c
    end
    return '\x1c'
end

# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
# An AbstractString is a FILE PATH (use IOBuffer(str) or codeunits for literal
# data). Everything becomes one byte buffer at this seam;
# large regular files use a read-only mapping while other sources use a copy.

# Files at or above the threshold memory-map instead of copying: the parser
# never writes to `buf`, and page faults amortize over the parallel chunk
# sweep. Eager columns own their bytes; Rows, lazy, and the Chunks iterator
# retain the source. Garbage collection unmaps an unreferenced buffer.
# Small files still read() — one small copy beats fault setup.
# `buffer_in_memory=true` forces the copy.
const MMAP_THRESHOLD = 1 << 19

# gzip is detected by magic bytes on every source kind: a
# compressed source decompresses to a fresh buffer before any parsing.
_isgzip(buf::AbstractVector{UInt8}) = length(buf) >= 2 && buf[1] == 0x1f && buf[2] == 0x8b
_maybegunzip(buf::Vector{UInt8}) = _isgzip(buf) ? transcode(GzipDecompressor, buf) : buf

resolvesource(buf::Vector{UInt8}; buffer_in_memory::Bool=false, prefetch::Bool=true) =
    _maybegunzip(buf)
# other byte containers (codeunits, views) copy into a Vector, the parser's buffer type
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
        @wkspawn _prefetchrange(m, lo, hi)
    end
    return
end

function resolvesource(s::AbstractString; buffer_in_memory::Bool=false, prefetch::Bool=true)
    # a URL: fetch to a temporary file with the Downloads stdlib, then
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
        # async readahead: faulting overlaps the parallel parse. madvise is a
        # Unix API.
        @static Sys.isunix() && Mmap.madvise!(m, Mmap.MADV_WILLNEED)
        # cold-file IO/parse overlap: WILLNEED alone loses to demand faults on a
        # cold file (the range planner reads the whole buffer once before
        # the index wave). Detached toucher tasks stride one
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
# names — normalizename
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
# The structural index IS the detector: for each candidate delimiter, index a
# bounded quote-aware sample and score how consistent the per-row field counts
# are. Candidates represented in the first surviving row win before data-only
# punctuation, which prevents Time values from making `:` look like a delimiter.
# `DELIM_CANDIDATES` order breaks score ties.

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
    d = Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    return _sample(buf, samplebytes, start, d)
end

function _sample(buf::Vector{UInt8}, samplebytes::Int, start::Int, d::Dialect)
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    start = clamp(start, 1, length(buf) + 1)
    length(buf) - start + 1 <= samplebytes && return start == 1 ? buf : buf[start:end]
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
                     dialect::Dialect, fastindex::Bool)
    dialect.quoted && UInt8(delim) == dialect.oq && return (0.0, 0, 0, 0)
    d = withdelim(dialect, UInt8(delim))
    bi = index(buf, d; datastart, parallel=false, fastindex)
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

function _detectdelim(sample::Vector{UInt8}, d::Dialect, fastindex::Bool)
    # Validate user syntax once. Candidate-only quote collisions are skipped in
    # `_scoredelim`; all other invalid options must reach the caller.
    datastart = _datastart(sample)
    # scoring reads at most 11 rows per candidate, but indexes whatever it is
    # given — trim to the first 12 rows once (row boundaries are quote-aware
    # and delimiter-independent) so candidates don't each index the full sample
    stop, rows = datastart, 0
    while stop <= length(sample) && rows < 12
        stop = nextrowstart(sample, stop, length(sample), d, false, true)
        rows += 1
    end
    scoresample = stop > length(sample) ? sample : sample[1:stop - 1]
    best, bestdelim = (false, false, 0.0, 0), first(DELIM_CANDIDATES)
    headercandidate = false
    for c in DELIM_CANDIDATES
        consistency, fields, firstfields, nrows = _scoredelim(scoresample, c, datastart,
                                                              d, fastindex)
        # a real delimiter splits the FIRST row and the data rows the same way:
        # a candidate that only appears in the header (a space in "Created
        # Date" over one-word data rows) is not represented in the data
        represented = firstfields > 1 && fields == firstfields
        # more fields only wins when the DATA rows established it: with a single
        # row (or an all-header sample) the field count is no evidence at all —
        # a one-line "\"a, b\", \"c\"" must not elect the space — so candidates
        # then tie on consistency and the candidate order decides
        evidence = nrows >= 2 && consistency > 0 && fields > 1
        # tier order: a candidate PRESENT IN THE HEADER outranks one that
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
    # for field-consistency evidence) follow the byte-count tiers below
    delim = (best[1] && best[2]) ? bestdelim :
            _detectdelim_bytecounts(scoresample, datastart, d, bestdelim,
                                    best[1] && headercandidate)
    # Space-ALIGNED files: a run of blanks between fields is one
    # separator. Score the (' ', ignorerepeated=true) reading last, and elect it
    # only when it cannot change a file that detected before: the plain space
    # won (so the file was going to be space-delimited anyway — with a column
    # per blank), and the repeated reading is at least as consistent with
    # fewer, ≥2 fields; or nothing structured the sample at all.
    if !d.ignorerepeated
        cons, fields, firstfields, nrows = _scoredelim(scoresample, ' ', datastart,
                                                       withdelim(d, d.delim::UInt8, true),
                                                       fastindex)
        aligned = nrows >= 2 && cons > 0 && fields > 1 && fields == firstfields
        if aligned && (delim == ' ' || !best[1])
            plaincons, plainfields, _, _ = delim == ' ' ?
                _scoredelim(scoresample, ' ', datastart, d, fastindex) : (0.0, typemax(Int), 0, 0)
            (cons >= plaincons && fields < plainfields) && return (' ', true)
        end
    end
    return (delim, false)
end

# The byte-count detector: count candidate bytes outside quotes over the
# header row and up to 10 data rows; tier 1 = present in header AND total count
# divisible by nlines; tier 2 = divisible by nlines; tier 3 = most frequent in
# the header, SPACE excluded; else ','. A one-row sample goes directly to tier
# 3 so a header phrase cannot elect space. `fallback` is the consistency
# scorer's data-only pick, used when it found real data evidence.
function _detectdelim_bytecounts(sample::Vector{UInt8}, datastart::Int, d::Dialect,
                                 fallback::Char, havedataevidence::Bool)
    oq, cq, eq = d.oq, d.cq, d.e
    len = length(sample)
    hcounts = zeros(Int, 256); counts = zeros(Int, 256)
    pos = datastart; nlines = 0; inheader = true; parsedany = false; lastnl = false
    while pos <= len && nlines < 11
        parsedany = true
        b = sample[pos]; pos += 1
        if d.quoted && b == oq
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
    # example `Created Date`); use the header-only tier, which
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
sample, candidates $(DELIM_CANDIDATES) in that order), whether a header
row is likely (row 1 all text while later rows type differently), and the
resulting names/types. `samplebytes` is the initial sample size; a sample too
small to hold even one complete row grows until it does. `kw` may pin dialect, value, and index pieces
(`quotechar`, `comment`, `decimal`, `fastindex`, ...) that sniffing should use.
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
    d = Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    sample = _sample(buf, samplebytes, 1, d)
    bestdelim, ir = _detectdelim(sample, d, get(indexkw, :fastindex, true))
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
                Base.nonmissingtype(eltype(cnh)) in (String, DataString)
        end
    t = headerlikely ? theader : tnoheader
    return Spec(bestdelim, get(dialectkw, :quoted, true), headerlikely,
                length(names(t)), copy(names(t)), Type[eltype(c) for c in columns(t)])
end

# delimiter-only sniff for File(delim=nothing) — no second parse
# -> (delim, ignorerepeated)
function _sniffdelim(buf::Vector{UInt8}, samplebytes::Int, start::Int,
                     d::Dialect, fastindex::Bool)
    sample = _sample(buf, samplebytes, start, d)
    return _detectdelim(sample, d, fastindex)
end

# ---------------------------------------------------------------------------
# the shared front end
# ---------------------------------------------------------------------------
# Everything row-positional is settled BEFORE any value work, and always in
# RAW structural rows — quote-aware, counting comment and empty lines (a
# comment line between header and skipto still counts). Numbered headers
# shift `datastart` so skipped prefix rows
# never even enter the index; `skipto` advances `firstdatarow` by byte
# offset, so hygiene-dropped rows cannot skew the count.

# Public row/field positions accept any Integer. Keep
# oversized UInt/BigInt values as an unreachable sentinel instead of narrowing
# them before the source geometry is known. The saturated successor is needed
# for `header + 1` and EOF positions at the machine-Int boundary.
_saturatedint(x::Integer) = x > typemax(Int) ? typemax(Int) :
                                    x < typemin(Int) ? typemin(Int) : Int(x)
_saturatedinc(x::Int) = x == typemax(Int) ? x : x + 1

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
    n <= 1 && return datastart
    datastart > length(buf) && return _saturatedinc(length(buf))
    _fastrowcount(d) && return _skiprowends(buf, d, datastart, n - 1)
    off = datastart
    for _ in 1:(n - 1)
        off > length(buf) && return _saturatedinc(length(buf))
        off = nextrowstart(buf, off, length(buf), d, false, true)
    end
    return off
end

# Row positioning (`skipto`, numbered headers, `footerskip`) walks raw rows
# from the anchor. Without comment rows, the walk counts row endings outside
# quoted fields 64 bytes at a time with the fast scanner's masks; the
# byte-at-a-time walk stays for comment rows and the lenient quote rule.
_fastrowcount(d::Dialect) = splittable(d) && !commentaware(d)

# Row-ending events outside quoted fields in the 64-byte block at `buf[pos]`
# (`p` points at that byte), as (ends, inq, skip, crlast). `inq` and `skip`
# are the quote state entering the block, as `quotewalk` defines them, and
# the returned pair is the state after it. `pairskip` drops a leading LF that
# pairs with the previous block's final CR, and `crlast` reports a CR row
# ending at the block's last byte.
@inline function _rowendblock(buf::Vector{UInt8}, p::Ptr{UInt8}, pos::Int, d::Dialect,
                              inq::Bool, skip::Bool, pairskip::Bool)
    if symmetricquotes(d)
        q64 = d.quoted ? byte_mask_vec(p, d.oq) : zero(UInt64)
        inmask = prefix_xor64(q64)
        inq && (inmask = ~inmask)
        inq ⊻= isodd(count_ones(q64))
    else
        oq, cq, e = d.oq, d.cq, d.e
        doubling = e == cq
        walkonly = !doubling && e == oq
        o64 = byte_mask_vec(p, oq)
        c64 = byte_mask_vec(p, cq)
        e64 = doubling ? c64 : walkonly ? o64 : byte_mask_vec(p, e)
        len = length(buf)
        nextiscq = doubling && pos + 64 <= len && @inbounds(buf[pos + 64]) == cq
        inmask, _, inq, skip = quoteblock(o64, c64, e64, doubling, walkonly, inq, skip, nextiscq)
    end
    cr64 = byte_mask_vec(p, CR)
    lf64 = byte_mask_vec(p, LF)
    ends = (cr64 | lf64) & ~inmask
    ends &= ~(lf64 & (cr64 << 1))          # the LF of a CR LF pair
    pairskip && (ends &= ~(lf64 & one(UInt64)))   # ... split across two blocks
    crlast = ((ends >> 63) & (cr64 >> 63)) != zero(UInt64)
    return ends, inq, skip, crlast
end

# The byte after the `k`-th row ending at or after `from` (`from` starts
# outside quotes), or `length(buf) + 1` when the input has fewer.
function _skiprowends(buf::Vector{UInt8}, d::Dialect, from::Int, k::Int)
    len = length(buf)
    pos = from
    inq = false
    skip = false
    pairskip = false
    remaining = k
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= len && remaining > 0
            ends, inq, skip, crlast = _rowendblock(buf, p + pos - 1, pos, d, inq, skip, pairskip)
            c = count_ones(ends)
            if c >= remaining
                for _ in 2:remaining          # keep the remaining-th set bit
                    ends &= ends - one(UInt64)
                end
                tz = trailing_zeros(ends)
                nxt = pos + tz + 1
                buf[pos + tz] == CR && nxt <= len && buf[nxt] == LF && (nxt += 1)
                return nxt
            end
            remaining -= c
            pairskip = crlast
            pos += 64
        end
    end
    pairskip && pos <= len && @inbounds(buf[pos]) == LF && (pos += 1)
    skip && (pos += 1)   # consumed content inside the open quoted field
    while remaining > 0 && pos <= len
        pos = nextrowstart(buf, pos, len, d, inq)
        inq = false
        remaining -= 1
    end
    return min(pos, len + 1)
end

# Raw rows from `from` to the end, as the byte-at-a-time walk counts them: a
# final row without a terminator counts, an input ending on one does not add
# an empty row.
function _countrows(buf::Vector{UInt8}, d::Dialect, from::Int)
    len = length(buf)
    from > len && return 0
    pos = from
    inq = false
    skip = false
    pairskip = false
    total = 0
    lastnext = 0        # the byte after the last row ending seen
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= len
            ends, inq, skip, crlast = _rowendblock(buf, p + pos - 1, pos, d, inq, skip, pairskip)
            c = count_ones(ends)
            if c > 0
                total += c
                hb = 63 - leading_zeros(ends)
                lastnext = pos + hb + 1
                buf[pos + hb] == CR && lastnext <= len && buf[lastnext] == LF && (lastnext += 1)
            end
            pairskip = crlast
            pos += 64
        end
    end
    pairskip && pos <= len && @inbounds(buf[pos]) == LF && (pos += 1)
    skip && (pos += 1)   # consumed content inside the open quoted field
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    doubling = e == cq
    @inbounds while pos <= len
        b = buf[pos]
        if inq
            if b == e && !doubling
                pos += 2
            elseif b == cq
                if doubling && pos < len && buf[pos + 1] == cq
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
        elseif b == LF || b == CR
            total += 1
            pos += 1 + (b == CR && pos < len && buf[pos + 1] == LF)
            lastnext = pos
        else
            pos += 1
        end
    end
    return total + ((total == 0 || lastnext <= len) ? 1 : 0)
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
# drops them; comment rows do not count.
function _footeroffset(buf::Vector{UInt8}, d::Dialect, rawstart::Int, footerskip::Int)
    footerskip == 0 && return _saturatedinc(length(buf))
    # Count first, then locate the first footer row. This is two structural
    # scans but constant memory; a ring of `footerskip` Ints lets a valid public
    # option allocate many GiB before discovering that the file has fewer rows.
    if _fastrowcount(d)
        nrows = _countrows(buf, d, rawstart)
        footerskip >= nrows && return rawstart
        return _skiprowends(buf, d, rawstart, nrows - footerskip)
    end
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

# Fixed settings cross from the API layer to the parse driver. Optional values remain fields,
# rather than changing the type of a keyword NamedTuple at every call site.
struct ReadSettings
    chunkbytes::Int
    parallel::Bool
    ntasks::Union{Nothing, Int}
    scanner::Symbol
    maxproblems::Int
    nsample::Union{Nothing, Int}
    typemap::Union{Nothing, Dict{Type, Type}}
    validate::Bool
    colopts::Union{Nothing, Vector{ValueOpts}}
end

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
    settings::ReadSettings
end

function _headerproblems(buf::Vector{UInt8}, refs::Vector{Tuple{ChunkIndex, Int}},
                         opts::ValueOpts, cap::Int)
    log = ProblemLog(cap)
    for (ci, hrow) in refs, j in 1:nfields(ci, hrow)
        pos, len = fieldspan(ci, hrow, j)::Tuple{Int, Int}
        len == 0 && continue
        cpos, clen, _, st = cellcontent(buf, pos, len, opts)
        st == CELL_BADQUOTE &&
            pushproblem!(log, 0, j, pos, :invalid_quoted_field,
                         "malformed quoting in header " * excerpt(buf, pos, len))
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
                  lenient::Bool=false,
                  kw...)
    return _prepare(source, header, normalizenames, skipto, footerskip, missingstring,
                    delim, limit, samplebytes, chunkbytes, parallel, ntasks,
                    buffer_in_memory, prefetch, validate, lenient, kw)
end

# Keep source handling, sniffing and row-window construction independent of the
# caller's keyword names and container types. Hot column loops specialize later.
Base.@nospecializeinfer function _prepare(@nospecialize(source), @nospecialize(header), normalizenames::Bool,
                  @nospecialize(skipto), @nospecialize(footerskip), @nospecialize(missingstring),
                  @nospecialize(delim), @nospecialize(limit), samplebytes::Int,
                  @nospecialize(chunkbytes::Union{Nothing, Int}), parallel::Bool,
                  @nospecialize(ntasks::Union{Nothing, Int}),
                  buffer_in_memory::Bool, prefetch::Bool, validate::Bool, lenient::Bool,
                  @nospecialize(kw))
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
    # The default header row 1 with skipto=1 means "no header, data starts at
    # row 1" (the header row and the first data row cannot coincide).
    if header isa Integer && header == 1 && skipto !== nothing && skipto == 1
        header = false
    end
    rawheaderrow = header === true ? 1 : header === false ? 0 :
                   header isa Integer ? header :
                   header isa AbstractVector{<:Integer} && !isempty(header) ? last(header) : 0
    buf = resolvesource(source; buffer_in_memory, prefetch)
    d0 = Dialect(; delim=delim === nothing ? _probedelim(kw) : delim,
                 quotechar=get(kw, :quotechar, '"'),
                 openquotechar=get(kw, :openquotechar, nothing),
                 closequotechar=get(kw, :closequotechar, nothing),
                 escapechar=get(kw, :escapechar, nothing), quoted=get(kw, :quoted, true),
                 comment=get(kw, :comment, nothing), ignoreemptyrows=get(kw, :ignoreemptyrows, true),
                 ignorerepeated=get(kw, :ignorerepeated, false), lenient)
    fastindex = get(kw, :fastindex, true)::Bool
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
    # `d` is assigned once: the `rowoff` closure below captures it, and a
    # captured local that is reassigned is boxed.
    d = if delim === nothing
        get(kw, :ignorerepeated, false) &&
            throw(ArgumentError("auto-delimiter detection is not supported with " *
                                "ignorerepeated=true; pass delim explicitly"))
        # Sniff from the first row that matters: skipped prefix rows are junk
        # and must not vote on the delimiter (a one-line "skip me" preamble
        # otherwise elects the space).
        sniffed, ir = _sniffdelim(buf, samplebytes, anchoroff, d0, fastindex)
        delim = sniffed
        withdelim(d0, UInt8(sniffed), ir || d0.ignorerepeated)
    else
        d0
    end
    # missingstring → sentinels ("" entries are inert: empty is always missing)
    sentinels = _sentinels(missingstring)
    dateformat = get(kw, :dateformat, nothing)
    dfdict = dateformat isa AbstractDict ? dateformat : nothing
    decimal = get(kw, :decimal, '.')::Char
    truestrings = get(kw, :truestrings, nothing)
    falsestrings = get(kw, :falsestrings, nothing)
    stripwhitespace = get(kw, :stripwhitespace, false)::Bool
    groupmark = get(kw, :groupmark, nothing)::Union{Nothing, Char}
    opts = makevalueopts(d, dfdict === nothing ? dateformat : nothing, decimal,
                         truestrings, falsestrings, stripwhitespace, groupmark, sentinels)
    # `ntasks` sets the chunk target (about four chunks per task) inside the
    # 64 KiB–1 MiB band that keeps a chunk cache-resident: the column loops
    # re-read a chunk once per column, so a chunk must not grow with the file.
    cb = chunkbytes === nothing ?
         _defaultchunkbytes(length(buf), something(ntasks, Threads.nthreads())) : chunkbytes

    # -- the row window, in RAW rows: header rows, skipto, footerskip ---------
    header isa Integer && !(header isa Bool) &&
        (header = header == 0 ? false : _saturatedint(header))
    headerrows = header isa AbstractVector{<:Integer} ? _saturatedint.(header) :
                 header isa Int ? [header] : Int[]
    headerrow = header === true ? 1 : isempty(headerrows) ? 0 : last(headerrows)
    # Skipped prefix rows never enter the index (a generated column count would
    # otherwise come from a junk first row instead of the first DATA row) —
    # the index starts at the anchor. Row `n` at/after the anchor is
    # `n - firstrow + 1` quote-aware structural rows from it.
    datastart = anchoroff
    rowoff(n::Int) = n < firstrow ? _physicallineoffset(buf, rawstart, n) :
                                    _rawrowoffset(buf, d, anchoroff, n - firstrow + 1)
    bi = index(buf, d; datastart, chunkbytes=cb, parallel, ntasks, fastindex)
    if bi.barequote && !lenient
        # A quote that did not start its field (`5' 11"`, `x"y`) made the
        # parallel toggle scan unsound: rows may have merged into one cell.
        # Prepare again under the lenient quote rule, from the same bytes.
        # Well-formed input never takes this path.
        return _prepare(buf, header, normalizenames, skipto, footerskip, missingstring,
                        delim, limit, samplebytes, chunkbytes, parallel, ntasks,
                        buffer_in_memory, prefetch, validate, true, kw)
    end
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
        # to ColumnN first. Each listed row is parsed
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
    # Footer rows are counted from the anchor: a stray quote in a skipped
    # prefix row must not swallow the file (the prefix is quote-blind).
    footer = footerskip > 0 && footerskip >= length(buf) ? anchoroff :
             _footeroffset(buf, d, anchoroff, Int(footerskip))
    keep = footerskip == 0 ? sum(nrows, chunks; init=0) : _rowsbefore(chunks, footer)
    lim = limit === nothing ? (footerskip > 0 ? keep : nothing) :
          limit >= keep ? keep : Int(limit)

    # engine + diagnostics kwargs the parse driver consumes directly
    colopts = nothing
    if dfdict !== nothing
        overrides = _resolvekeys(dfdict, names, length(names), "dateformat"; validate)
        colopts = ValueOpts[haskey(overrides, j) ?
                              makevalueopts(d, overrides[j], decimal, truestrings, falsestrings,
                                            stripwhitespace, groupmark, sentinels) : opts
                              for j in 1:length(names)]
    end
    nsample = get(kw, :nsample, nothing)::Union{Nothing, Int}
    nsample === nothing || nsample >= 1 ||
        throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    settings = ReadSettings(cb, parallel, ntasks, resolvescanner(d, fastindex),
                            get(kw, :maxproblems, 10_000), nsample,
                            _normalizetypemap(get(kw, :typemap, nothing)::Union{Nothing, AbstractDict}),
                            validate, colopts)
    return Prepared(buf, bi, names, length(names), lim, opts, d, headerlog, headerrefs, settings)
end

# Prepared already owns the dialect, value options and structural index.
# Reuse them rather than rebuilding them through parse's keyword front.
function _parseprepared(p::Prepared, plan::ColumnPlan;
                        parallel::Bool=p.settings.parallel,
                        ntasks::Union{Nothing, Int}=p.settings.ntasks,
                        validate::Bool=p.settings.validate,
                        maxproblems::Int=p.settings.maxproblems,
                        limit::Union{Nothing, Int}=p.limit,
                        rowmask::Union{Nothing, Vector{Bool}}=nothing,
                        reportstructural::Bool=true)
    tasklimit = parallel ? min(something(ntasks, Threads.nthreads()), Threads.nthreads()) : 1
    settings = p.settings
    return _parse(p.buf, p.d, p.opts, settings.scanner, settings.typemap,
                  settings.chunkbytes, parallel, tasklimit, maxproblems, :collect,
                  validate, reportstructural, settings.nsample, limit,
                  p.names, nothing, nothing, settings.colopts, plan, rowmask, p.bi)
end

# kwargs _prepare consumes itself (not forwarded to the parse driver)
const _PREPKW = (:header, :normalizenames, :skipto, :footerskip, :missingstring,
                 :delim, :limit, :samplebytes, :chunkbytes, :parallel,
                 :buffer_in_memory, :prefetch, :validate)

_preparedcolopts(p::Prepared) = p.settings.colopts

# Create the column plan from the names and value rules found during source
# preparation. Name selection also accepts the spelling used before
# `normalizenames=true` changed the header.
function settlecolumns(p::Prepared; select=nothing, drop=nothing, types=nothing,
                       available::Union{Nothing, Vector{Int}}=nothing,
                       validate::Bool=true)
    return settlecolumns(p.names, p.opts; select, drop, types, available,
                         colopts=_preparedcolopts(p), validate,
                         matchnormalized=true)
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
              stringtype::Type=DataString,
              strict::Bool=false, on_error::Symbol=strict ? :error : :warn,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              validate::Bool=true,
              kw...)
    return _file(source, types, select, drop, scan, pool, downcast, transpose, stringtype,
                 on_error, maxproblems, ntasks, parallel, validate, kw)
end

Base.@nospecializeinfer function _file(@nospecialize(source), @nospecialize(types),
               @nospecialize(select), @nospecialize(drop), @nospecialize(scan),
               @nospecialize(pool), downcast::Bool, transpose::Bool,
               @nospecialize(stringtype::Type), on_error::Symbol, maxproblems::Int,
               @nospecialize(ntasks::Union{Nothing, Int}), parallel::Bool,
               validate::Bool, @nospecialize(kw))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkstringtype(stringtype)
    _checkonerror(on_error)
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    if transpose
        (select !== nothing || drop !== nothing) &&
            throw(ArgumentError("select/drop are not supported with transpose=true"))
        scan === nothing || throw(ArgumentError("scan is not supported with transpose=true"))
        return _transposedfile(source; types, pool, downcast, stringtype, on_error,
                               maxproblems, validate, parallel, ntasks, kw...)
    end
    capturecap = max(maxproblems, 1)
    if scan !== nothing
        # -- Tables.Scan pushdown: the scan owns selection, types, and row
        # bounds; the classic keywords for those axes are refused rather than
        # merged, so a request means one thing --------------------------------
        scan isa Tables.Scan ||
            throw(ArgumentError("scan must be a Tables.Scan (got $(typeof(scan)))"))
        select === nothing && drop === nothing ||
            throw(ArgumentError("pass the column selection through the Scan, not select=/drop="))
        types === nothing ||
            throw(ArgumentError("pass column types through the Scan's select items (`:col => T`), not types="))
        haskey(kw, :limit) &&
            throw(ArgumentError("pass the row limit through the Scan, not limit="))
        p = _prepare(source; parallel, ntasks, maxproblems=capturecap, validate, kw...)
        nm = _sourcename(source)
        t, requests = _executescan(p, scan; maxproblems, on_error, source=nm)
        # pool keys name the scan's OUTPUT columns (the request already renamed
        # and reordered them)
        t = _poolcolumns(t, _resolvepool(pool, names(t), length(names(t)); validate); parallel)
        t = _finishstrings(t, stringtype, requests; parallel)
        downcast && (t = _downcast(t))
        return File(nm, t, Dict(n => j for (j, n) in enumerate(names(t))))
    end
    p = _prepare(source; parallel, ntasks, maxproblems=capturecap, validate, kw...)
    return _filefromprepared(p, _sourcename(source); types, select, drop, pool, downcast,
                             stringtype, on_error, maxproblems, parallel, ntasks, validate)
end

_sourcename(source) = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
_sourceprovenance(source, i::Int) =
    source isa AbstractString ? String(source) : "<source $i>"

function _setemptytypes!(plan::ColumnPlan)
    for j in plan.sources
        plan.columns[j].parsetype === nothing &&
            (plan.columns[j] = _columndecision(Missing))
    end
    return plan
end

function _filefromprepared(p::Prepared, nm::String; types=nothing, select=nothing, drop=nothing,
                           pool=DEFAULT_POOL, downcast::Bool=false, stringtype::Type=DataString,
                           on_error::Symbol=:warn, maxproblems::Int=10_000,
                           parallel::Bool=Threads.nthreads() > 1, validate::Bool=true,
                           ntasks::Union{Nothing, Int}=nothing,
                           available::Union{Nothing, Vector{Int}}=nothing)
    return _filefromprepared(p, nm, types, select, drop, pool, downcast, stringtype,
                             on_error, maxproblems, parallel, validate, ntasks, available)
end

Base.@nospecializeinfer function _filefromprepared(p::Prepared, nm::String, @nospecialize(types),
                           @nospecialize(select), @nospecialize(drop), @nospecialize(pool),
                           downcast::Bool, @nospecialize(stringtype::Type),
                           on_error::Symbol, maxproblems::Int, parallel::Bool, validate::Bool,
                           @nospecialize(ntasks::Union{Nothing, Int}),
                           @nospecialize(available::Union{Nothing, Vector{Int}}))
    viewnames = available === nothing ? p.names : p.names[available]
    plan = settlecolumns(p; select, drop, types, available, validate)
    p.limit == 0 && _setemptytypes!(plan)
    # File(::LazyFile) resolves pool keys against the columns exposed by the
    # LazyFile, not against columns that were already dropped.
    poolspecs = _resolvepool(pool, viewnames, length(viewnames); validate)
    # Prepared records the options used to build the index, but value parsing
    # belongs to this File call. Override every value-driver option that this
    # method exposes; in particular, a LazyFile prepared with defaults must not
    # silently cap a later larger maxproblems request at 10,000.
    t = _parseprepared(p, plan; parallel, ntasks, validate,
                       maxproblems=max(maxproblems, 1))
    headerlog = _headerproblems(p.buf, p.headerrefs, p.opts, max(maxproblems, 1))
    t, firstproblem = _mergeproblems(t, headerlog, maxproblems)
    t, firstproblem = _narrowtypes(t, plan, p.bi.chunks, maxproblems, firstproblem)
    _reportproblems(t, on_error, firstproblem, nm)
    t = _poolcolumns(t, poolspecs[plan.positions]; parallel)
    t = _finishstrings(t, stringtype, _requestedstrings(plan); parallel)
    downcast && (t = _downcast(t))
    return File(nm, t, Dict(n => j for (j, n) in enumerate(names(t))))
end

# One place applies the `on_error` policy once the problems of a read are
# final: `:error` throws the source-earliest problem as a `ParseError`,
# `:warn` prints one summary, `:collect` keeps them for `problems(f)`.
function _reportproblems(t::ParsedTable, on_error::Symbol,
                         firstproblem::Union{Nothing, Problem}, source::String,
                         warned::Union{Nothing, Base.RefValue{Bool}}=nothing,
                         note::String="")
    if on_error === :error
        firstproblem !== nothing &&
            _throwparseerror(firstproblem, length(t.problems) + t.droppedproblems, source)
    elseif on_error === :warn
        # `warned` makes a Chunks warn once: its first batch with problems
        warned !== nothing && warned[] && return
        emitted = _warnproblems(t.problems, t.droppedproblems, source, note)
        warned === nothing || (warned[] = emitted)
    end
    return
end

# Resolve output types before converting either pooled levels or text columns.
# An explicit string type wins over the default for inferred text. Pool levels
# are owned strings even when DataString was requested.
_hasstringrequest(d::ColumnDecision) = d.parsetype === String && d.resulttype !== nothing
_requestedstring(d::ColumnDecision) = d.parsetype === String ? d.resulttype : nothing
function _requestedstrings(plan::ColumnPlan, sources=plan.sources)
    any(j -> _hasstringrequest(plan.columns[j]), sources) || return nothing
    return Union{Nothing, Type}[_requestedstring(plan.columns[j]) for j in sources]
end

function _finishstrings(t::ParsedTable, stringtype::Type, requests; parallel::Bool=true)
    requests === nothing && stringtype === DataString && return _pooledarrays(t)
    any(c -> c isa PooledColumn || c isa DataStringVector, t.columns) || return t
    cols = AbstractVector[t.columns...]
    jobs = Int[]
    for j in eachindex(cols)
        S = requests === nothing ? stringtype : something(requests[j], stringtype)
        c = cols[j]
        (c isa PooledColumn || (c isa DataStringVector && S !== DataString)) && push!(jobs, j)
    end
    # columns convert independently, and a long column splits its rows
    convertone = j -> begin
        S = requests === nothing ? stringtype : something(requests[j], stringtype)
        c = cols[j]
        if c isa PooledColumn
            cols[j] = _topooledarray(c, S === DataString ? String : S)
        elseif c isa DataStringVector
            cols[j] = _materializecolumn(S, c, parallel)
        end
    end
    if parallel && length(jobs) > 1
        _spawnall(convertone, jobs)
    else
        foreach(convertone, jobs)
    end
    return ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# The optional scan implementation is included after this file. A Tables
# version without Scan never reaches this call because the type check above
# fails first with a clear error.
function _executescan(p::Prepared, scan; maxproblems::Int, on_error::Symbol,
                      source::String="")
    return _executescanplan(p, scan; headerlog=p.headerlog,
                            maxproblems, on_error, source)
end

function File(sources::AbstractVector; source=nothing, kw...)
    if eltype(sources) === UInt8
        # A byte buffer is a vector. Read it as one source.
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
    on_error = get(kw, :on_error, strict ? :error : :warn)
    maxwarnings = get(kw, :maxwarnings, nothing)
    maxproblems = haskey(kw, :maxproblems) ? get(kw, :maxproblems, 10_000) :
                  something(maxwarnings, 10_000)
    _checkonerror(on_error)
    capturecap = max(maxproblems, 1)
    # Parse children in collecting mode, then apply one diagnostic cap and one
    # strict decision to the logical concatenated input. Otherwise N sources
    # could retain N * maxproblems entries and a later source could throw before
    # the globally first problem was known.
    childkw = merge(NamedTuple(kw), (; strict=false, on_error=:collect,
                                     maxwarnings=nothing, maxproblems=capturecap))
    nt = get(kw, :ntasks, nothing)
    nt === nothing || nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    parallel = get(kw, :parallel, nt === nothing ? Threads.nthreads() > 1 : nt > 1)
    budget = parallel ? min(something(nt, Threads.nthreads()), Threads.nthreads()) : 1
    files = Vector{File}(undef, length(sources))
    if budget > 1 && length(sources) >= budget
        # Bound the outer workers and avoid nested parser task groups. Each
        # worker owns a source; collection and diagnostics stay in source order.
        singlekw = merge(childkw, (; ntasks=1, parallel=false))
        _taskforeach(eachindex(sources), budget) do i
            files[i] = File(sources[i]; singlekw...)
        end
    else
        # A few large sources can each use the full parser budget.
        for i in eachindex(sources)
            files[i] = File(sources[i]; childkw...)
        end
    end
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
    nm = "<$(length(sources)) sources>"
    _reportproblems(t, on_error, log.first, nm)
    return File(nm, t, Dict(n => j for (j, n) in enumerate(outnames)))
end

# a source that lacks a column contributes an all-missing block
const EMPTY_COLUMN = Union{}[]

function _colpiece(f::File, nm::Symbol)
    j = get(getfield(f, :lookup), nm, 0)
    return j == 0 ? EMPTY_COLUMN : getfield(f, :table).columns[j]
end

# Concatenate one column's per-source pieces (EMPTY_COLUMN ⇒ the source lacks
# the column: all-missing block). Element types promote across sources. Text
# that every source parsed as DataString stays a DataString column: the pieces'
# payloads join, re-pointed at their own buffers appended to one buffer list,
# with no string copied. Other string types concatenate as `String`.
function _chaincolumn(pieces::Vector{AbstractVector}, counts::Vector{Int}, total::Int)
    if any(c -> c isa DataStringVector, pieces) &&
       all(c -> c === EMPTY_COLUMN || c isa DataStringVector, pieces)
        return _chaindatastrings(pieces, counts, total)
    end
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
            S === Union{} || (T = _chaintype(T, S))
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
            _copypiece!(out, off, c, n)
        end
        off += n
    end
    return pooled ? PooledArray(out) : out
end

function _chaindatastrings(pieces::Vector{AbstractVector}, counts::Vector{Int}, total::Int)
    payloads = Vector{DataStringPayload}(undef, total)
    buffers = Vector{UInt8}[]
    anymissing = false
    off = 0
    for (c, n) in zip(pieces, counts)
        if c === EMPTY_COLUMN
            n > 0 && (anymissing = true)
            @inbounds for i in 1:n
                payloads[off + i] = PAYLOAD_MISSING
            end
        else
            col = c::DataStringVector
            anymissing |= Missing <: eltype(col)
            # this piece's buffer k becomes buffer base + k of the result
            base = length(buffers)
            append!(buffers, col.buffers)
            src = col.payloads
            @inbounds for i in 1:n
                p = src[i]
                payloads[off + i] = payloadlen(p) > INLINE_MAX ?
                    repoint_payload(p, payloadbufidx(p) + base, payloadoffset(p)) : p
            end
        end
        off += n
    end
    return _stringvector(anymissing ? Union{DataString, Missing} : DataString, payloads, buffers)
end

# Text keeps one shared owned string type when every source produced it (an
# explicit `types=String15` on each), and is `String` otherwise.
function _chaintype(T::Type, S::Type)
    if S <: AbstractString
        S === DataString && (S = String)
        T === Union{} && return S
        T === S && return S
        T <: AbstractString && return String
    end
    return promote_type(T, S)
end

# Function barrier: the piece's concrete type is static inside, so string
# materialization does not dispatch per element.
function _copypiece!(out::Vector, off::Int, c::AbstractVector, n::Int)
    S = Base.nonmissingtype(eltype(c))
    E = Base.nonmissingtype(eltype(out))
    if S !== Union{} && S <: AbstractString && S !== E
        R = E <: AbstractString ? E : String
        @inbounds for k in 1:n
            x = c[k]
            out[off + k] = x === missing ? missing : R(x)
        end
    else
        copyto!(out, off + 1, c, 1, n)
    end
    return out
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
                           T0, opts, log::ProblemLog, col::Int,
                           declaredmissing::Bool=false)
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
    T === String && return _transposedstrings(buf, ci, lr, startf, n, opts, declaredmissing)
    # `T` is a runtime value here; the typed loop specializes on it once per
    # column so its cells parse and store without a dispatch each
    return _transposedtyped(T, buf, ci, lr, startf, n, T0 !== nothing, opts, log, col,
                            declaredmissing)
end

function _transposedstrings(buf::Vector{UInt8}, ci, lr::Int, startf::Int, n::Int, opts,
                            declaredmissing::Bool)
    nf = nfields(ci, lr)
    scol = StringColumn(n, opts.e, opts.cq)
    payloads = scol.payloads
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
            inl === nothing ? _ownescaped!(scol, buf, cpos, clen, i) : (payloads[i] = inl)
        elseif clen <= INLINE_MAX
            payloads[i] = inline_payload(buf, cpos, clen)
        else
            _own!(scol, buf, cpos, clen, i)
        end
    end
    return finalizecolumn(String, scol, n, sawmiss || declaredmissing)
end

function _transposedtyped(::Type{T}, buf::Vector{UInt8}, ci, lr::Int, startf::Int, n::Int,
                          requested::Bool, opts, log::ProblemLog, col::Int,
                          declaredmissing::Bool) where {T}
    nf = nfields(ci, lr)
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
            # exact inference cannot conflict: a text-only cell under an
            # inferred type means the row is text
            st == CELL_VALUE && (esc || clen == 0) && !requested &&
                return _transposedstrings(buf, ci, lr, startf, n, opts, declaredmissing)
            if requested && st != CELL_MISSING
                kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
                pushproblem!(log, i, col, sp[1], kind,
                               "cannot parse transposed value as $T")
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
            # a requested type leaves the cell missing (strict=false File semantics)
            requested || return _transposedstrings(buf, ci, lr, startf, n, opts, declaredmissing)
            pushproblem!(log, i, col, sp[1], :invalid_value,
                           "cannot parse transposed value as $T")
            sawmiss = true
            continue
        end
        out[i] = v
    end
    return sawmiss || declaredmissing ? out : convert(Vector{T}, out)
end

function _transposedfile(source; types=nothing, pool=DEFAULT_POOL, downcast::Bool=false,
                         stringtype::Type=DataString,
                         on_error::Symbol=:warn, maxproblems::Int=10_000,
                         header::Union{Bool, Integer, AbstractVector}=true,
                         skipto::Union{Nothing, Integer}=nothing,
                         missingstring=nothing, delim=',',
                         normalizenames::Bool=false, limit::Union{Nothing, Integer}=nothing,
                         validate::Bool=true, parallel::Bool=Threads.nthreads() > 1,
                         ntasks::Union{Nothing, Integer}=nothing,
                         buffer_in_memory::Bool=false, prefetch::Bool=true, kw...)
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    tasklimit = parallel ? min(Int(something(ntasks, Threads.nthreads())), Threads.nthreads()) : 1
    _checkonerror(on_error)
    allowed = (_DIALECTKW..., _VALUEKW...)
    _checkkwargs("File(transpose=true)", kw, allowed)
    header isa Integer && header < 0 &&
        throw(ArgumentError("header must be ≥ 0 (got $header)"))
    header isa AbstractVector{<:Integer} &&
        throw(ArgumentError("transpose=true takes a single header field index, not a range"))
    skipto === nothing || skipto >= 1 ||
        throw(ArgumentError("skipto must be ≥ 1 (got $skipto)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    # transposed geometry: header=N takes each row's Nth field
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
    valuekw0 = _pickkwargs(kw, _VALUEKW)
    # single assignments: the per-column option comprehension captures `valuekw`
    dfdict = haskey(valuekw0, :dateformat) && valuekw0.dateformat isa AbstractDict ?
             valuekw0.dateformat : nothing
    valuekw = dfdict === nothing ? valuekw0 :
              NamedTuple(kv for kv in pairs(valuekw0) if kv.first != :dateformat)
    d0 = Dialect(; delim, dialectkw...)
    opts = makevalueopts(d0; sentinels=_sentinels(missingstring), valuekw...)
    bi0 = index(buf, d0; datastart=_datastart(buf), parallel=tasklimit > 1, ntasks=tasklimit)
    # a quote that did not start its field: use the lenient rule (single
    # assignments: the per-column option comprehension captures `d`)
    d = bi0.barequote ? withlenient(d0) : d0
    bi = bi0.barequote ?
         index(buf, d; datastart=_datastart(buf), parallel=tasklimit > 1, ntasks=tasklimit) : bi0
    rows = Tuple{ChunkIndex, Int}[]
    for ci in bi.chunks, lr in ci.firstdatarow:totalrows(ci)
        push!(rows, (ci, lr))
    end
    ncols = length(rows)
    # one assignment: the column comprehension below captures `n`
    longest = ncols == 0 ? 0 :
              max(0, maximum(nfields(r[1], r[2]) - (startf - 1) for r in rows))
    n = limit === nothing || limit >= longest ? longest : Int(limit)
    # `cell` is local to the closure; the source name below is a different variable
    _tname(j, r) = (cell = hasnames ? _cellstring(buf, r[1], r[2], namefield, opts) : "";
                    isempty(cell) ? Symbol("Column", j) : Symbol(cell))
    names = explicitnames !== nothing ? copy(explicitnames) :
            Symbol[_tname(j, r) for (j, r) in enumerate(rows)]
    explicitnames !== nothing && length(names) != ncols &&
        throw(ArgumentError("header has $(length(names)) names for $ncols transposed rows"))
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = makeunique!(names)
    colopts = if dfdict === nothing
        fill(opts, ncols)
    else
        overrides = _resolvekeys(dfdict, names, ncols, "dateformat"; validate)
        ValueOpts[haskey(overrides, j) ?
                    makevalueopts(d; sentinels=_sentinels(missingstring), valuekw...,
                                    dateformat=overrides[j]) : opts
                    for j in 1:ncols]
    end
    plan = settlecolumns(names, opts; types, colopts, validate)
    # narrow numeric requests parse natively here; a requested string type is
    # applied after the parse by `_finishstrings`
    seed = Union{Nothing, Type}[_hasstringrequest(d) ? String : accessparsetype(d)
                                for d in plan.columns]
    # Each input row becomes one output column, parsed independently. Problems
    # carry the cell index as their row, so no rebasing applies across columns.
    pending = PendingProblemLog(maxproblems)
    cols = Vector{AbstractVector}(undef, ncols)
    _taskforeach(1:ncols, tasklimit) do j
        r = rows[j]
        collog = ProblemLog(maxproblems)
        cols[j] = _transposedcolumn(buf, r[1], r[2], startf, n, seed[j], colopts[j],
                                    collog, j, plan.columns[j].declaredmissing)
        mergeproblems!(pending, collog, j)
    end
    log = finishproblems(pending, zeros(Int, ncols))
    sortproblems!(log)
    t = ParsedTable(names, cols, n, log.items, log.dropped)
    nm = _sourcename(source)
    _reportproblems(t, on_error, log.first, nm)
    t = _poolcolumns(t, _resolvepool(pool, names, ncols; validate); parallel=tasklimit > 1)
    t = _finishstrings(t, stringtype, _requestedstrings(plan); parallel=tasklimit > 1)
    downcast && (t = _downcast(t))
    return File(nm, t, Dict(nm2 => j for (j, nm2) in enumerate(names)))
end

# --- pooling: a finalize-time pass at the API layer ---------------------------
#
# The parser never pools. When asked, each DataString column is interned
# ONCE, allocation-free (DataString hashing/equality are content-based),
# into first-occurrence levels; the policy bound `min(floor(ratio·n), cap)`
# abandons a column the moment its distinct count exceeds it (a unique-valued
# column costs the walk up to that bound, nothing more). Columns pool in
# parallel.

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

# intern one DataString column; nothing when the policy bound is exceeded.
# Rows split into contiguous ranges interned in parallel (each range's local
# table is a Dict{DataString,UInt32}; the DataString hash walks the
# bytes, no allocation); the local level lists then merge in range order — so
# level ids are first-occurrence-in-file order exactly as a serial pass would
# assign them — and each range's refs remap through a small local→global
# vector. A range exceeding the bound locally proves the column exceeds it.
function _poolcolumn(c::DataStringVector, ps::Tuple{Float64, Int}; parallel::Bool=true)
    n = length(c)
    n == 0 && return nothing
    ratiolevels = ps[1] == 1.0 ? n : floor(Int, ps[1] * n)
    maxlevels = min(ratiolevels, ps[2], _MAX_POOL_LEVELS)
    maxlevels <= 0 && return nothing
    nt = parallel ? clamp(n ÷ 65_536, 1, 4 * Threads.nthreads()) : 1
    bounds = [1 + (t - 1) * n ÷ nt for t in 1:nt]
    push!(bounds, n + 1)
    refs = zeros(UInt32, n)
    locals = Vector{Tuple{Vector{DataStringPayload}, Vector{DataString}}}(undef, nt)
    aborted = Threads.Atomic{Bool}(false)
    # task bodies are named functions (a closure that assigned `levels`/`keys`
    # here would rebind the merge scope's variables — shared across tasks)
    if nt > 1
        @sync for t in 1:nt
            @wkspawn (locals[t] = _internrange!(refs, c, bounds[t], bounds[t + 1] - 1,
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
        levels = DataStringPayload[]
        globalof = Dict{DataString, UInt32}()
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
            @wkspawn _remaprange!(refs, remaps[t], bounds[t], bounds[t + 1] - 1)
        end
    end
    lv = DataStringVector{DataString}(levels, c.buffers, Val(:trusted))
    return Missing <: eltype(c) ? PooledColumn{Union{DataString, Missing}}(refs, lv) :
                                  PooledColumn{DataString}(refs, lv)
end

# intern rows lo..hi of `c` into a fresh local table; refs get LOCAL ids
function _internrange!(refs::Vector{UInt32}, c::DataStringVector, lo::Int, hi::Int,
                       maxlevels::Int, aborted::Threads.Atomic{Bool})
    table = Dict{DataString, UInt32}()
    levels = DataStringPayload[]
    keys = DataString[]
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

# pool the table's DataString columns per `specs` (one per output column),
# in parallel across columns
function _poolcolumns(t::ParsedTable, specs::AbstractVector; parallel::Bool=true)
    js = [j for (j, c) in enumerate(t.columns)
          if c isa DataStringVector && j <= length(specs) && specs[j] !== nothing]
    isempty(js) && return t
    cols = AbstractVector[t.columns...]
    pooled = Vector{Any}(nothing, length(js))
    poolone = i -> (pooled[i] = _poolcolumn(cols[js[i]]::DataStringVector, specs[js[i]]; parallel))
    if parallel && length(js) > 1
        @sync for i in eachindex(js)
            @wkspawn poolone(i)
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
# carry the same global row and source-byte position as parse diagnostics.
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

# The scalar parser uses a wider type for a narrow numeric request. Convert the
# result here. An integer outside the requested range becomes `missing` and
# adds a problem.
function _narrowtypes(t::ParsedTable, plan::ColumnPlan, chunks, maxproblems::Int,
                      firstproblem::Union{Nothing, Problem}=nothing;
                      problemrowbase::Int=0,
                      sourcerows::Union{Nothing, AbstractVector{Int}}=nothing)
    all(j -> plan.columns[j].resulttype === nothing, plan.sources) &&
        return t, firstproblem
    sourcerows === nothing || length(sourcerows) == t.nrows ||
        throw(ArgumentError("source-row map length $(length(sourcerows)) != $(t.nrows) rows"))
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
    for (o, j) in enumerate(plan.sources)
        T = plan.columns[j].resulttype
        T === nothing && continue
        c = cols[o]
        Base.nonmissingtype(eltype(c)) in (Int64, Int128, Float64) || continue
        cols[o] = _narrowcolumn(T, c, j, chunks, log, problemrowbase, sourcerows)
    end
    sortproblems!(log)
    return ParsedTable(t.names, cols, t.nrows, log.items, log.dropped), log.first
end

# One column's checked conversion. A function barrier: `T` and the concrete
# column type are static inside, so the loop is monomorphic.
function _narrowcolumn(::Type{T}, c::AbstractVector, j::Int, chunks, log::ProblemLog,
                       problemrowbase::Int, sourcerows) where {T}
    n = length(c)
    # The parser widens a user-declared Union{Missing,T} before this step.
    # Preserve that declaration even when every value is present.
    if !(Missing <: eltype(c))
        # every value present: convert straight into the Vector{T}; the first
        # out-of-range integer switches to the missing-capable loop below
        out = Vector{T}(undef, n)
        bad = _narrowfill!(out, c)
        bad == 0 && return out
        uout = Vector{Union{T, Missing}}(undef, n)
        copyto!(uout, 1, out, 1, bad - 1)
        return _narrowreport!(T, uout, c, bad, j, chunks, log, problemrowbase, sourcerows)
    end
    return _narrowreport!(T, Vector{Union{T, Missing}}(undef, n), c, 1, j, chunks, log,
                          problemrowbase, sourcerows)
end

# Convert every value of `c` into `out` until one falls outside `T`'s range;
# return that index, or 0 when the whole column converted.
function _narrowfill!(out::Vector{T}, c::AbstractVector) where {T}
    @inbounds for i in eachindex(c)
        x = c[i]
        T <: Integer && !(typemin(T) <= x <= typemax(T)) && return i
        out[i] = convert(T, x)
    end
    return 0
end

# Fill `out[from:end]` from `c`; an out-of-range integer becomes `missing` and
# reports a problem. The message is formatted only when the bounded log would
# retain it, so a column of overflowing values costs no string per cell.
function _narrowreport!(::Type{T}, out::Vector{Union{T, Missing}}, c::AbstractVector, from::Int,
                        j::Int, chunks, log::ProblemLog, problemrowbase::Int, sourcerows) where {T}
    chunkidx = 1
    indexedrowbase = 0
    @inbounds for i in from:length(c)
        x = c[i]
        if x === missing
            out[i] = missing
        elseif T <: Integer && !(typemin(T) <= x <= typemax(T))
            out[i] = missing
            sourcei = sourcerows === nothing ? i : sourcerows[i]
            problemrow, problempos, chunkidx, indexedrowbase =
                _narrowlocation(chunks, sourcei, j, problemrowbase,
                                chunkidx, indexedrowbase)
            if wantsproblem(log, problemrow, j, problempos)
                pushproblem!(log, problemrow, j, problempos, :invalid_value,
                             "value $x does not fit $T")
            else
                log.dropped += 1
            end
        else
            out[i] = convert(T, x)
        end
    end
    return out
end

# downcast=true: Int64 columns shrink to the smallest of Int8/Int16/Int32 that
# holds every value (one extrema scan + one convert per column)
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
# outright for missing-free columns and remapped once — missing joins the pool
# — otherwise.
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
# knows DataString (the default; the parsed column as it is) and String (bulk
# materialization). Extensions register more by adding methods to
# `_stringsink` (validation) and `_materializecolumn` / `_levelvector`
# (conversion): CSVInlineStringsExt registers InlineString (auto-width per
# column) and the fixed String1..String255.
_stringsink(::Type{DataString}) = true
_stringsink(::Type{String}) = true
_stringsink(::Type{Symbol}) = true   # `types=Symbol`: text converted once after parsing
_stringsink(::Type) = false
_checkstringtype(T) =
    (T isa Type && T !== Symbol && _stringsink(T)) ||
        throw(ArgumentError("stringtype must be DataStrings.DataString, String, or a " *
                            "type provided by an extension (e.g. InlineString with " *
                            "InlineStrings loaded); got $T"))

# a DataStringVector to Vector{S} / Vector{Union{S,Missing}}. String goes
# through materialize's bulk path — one shared scratch, word-store inline
# reconstruction, unsafe_string per cell (a per-cell String() broadcast would
# take the generic AbstractString path).
_materializecolumn(::Type{S}, col::DataStringVector, parallel::Bool) where {S} =
    _materializecolumn(S, col)
_materializecolumn(::Type{String}, col::DataStringVector) = materialize(col)
# String allocation scales across tasks: a string-heavy file otherwise spent
# several times its parse time materializing on one task.
function _materializecolumn(::Type{String}, col::DataStringVector, parallel::Bool)
    n = length(col)
    (parallel && n > _ROWS_PER_TASK) || return materialize(col)
    ELT = eltype(col)
    out = Vector{ELT === DataString ? String : Union{String, Missing}}(undef, n)
    _rowranges(n, parallel) do lo, hi
        part = materialize(_stringvector(ELT, col.payloads[lo:hi], col.buffers))
        copyto!(out, lo, part, 1, hi - lo + 1)
    end
    return out
end

function _materializecolumn(::Type{Symbol}, col::DataStringVector)
    n = length(col)
    Missing <: eltype(col) || return Symbol[Symbol(col[i]) for i in 1:n]
    out = Vector{Union{Symbol, Missing}}(undef, n)   # a declared Union stays, as for String
    @inbounds for i in 1:n
        x = col[i]
        out[i] = x === missing ? missing : Symbol(x)
    end
    return out
end
# pool levels (a DataStringVector) to Vector{S}
_levelvector(::Type{String}, levels::DataStringVector, n::Int) =
    String[String(levels[i]) for i in 1:n]
_levelvector(::Type{Symbol}, levels::DataStringVector, n::Int) =
    Symbol[Symbol(levels[i]) for i in 1:n]

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
    # `f.names` and `f[:names]` read the schema without a second copy of it.
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
Base.getindex(r::FileRow, nm::AbstractString) = Tables.getcolumn(r, Symbol(nm))
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
read(source; kw...) =
    throw(ArgumentError("CSV.read needs a Tables.jl sink as its second argument, " *
                        "for example `using DataFrames; CSV.read(source, DataFrame)`; " *
                        "use CSV.File(source) for CSV.jl's own table"))

# ---------------------------------------------------------------------------
# lazy / LazyFile — the structural index AS a table
# ---------------------------------------------------------------------------
function lazy(source; types=nothing, stringtype::Type=DataString,
              select=nothing, drop=nothing, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW..., :validate)
    _checkkwargs("lazy", kw, allowed)
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    validate = get(kw, :validate, true)
    plan = settlecolumns(p; select, drop, types, validate)
    chunks = p.bi.chunks
    rowbases = cumsum([0; Int[nrows(ci) for ci in chunks[1:max(length(chunks) - 1, 0)]]])
    total = sum(nrows, chunks; init=0)
    nr = p.limit === nothing ? total : min(total, p.limit)
    js = plan.sources
    cols = AbstractVector[]
    for j in js
        dec = plan.columns[j]
        T = accessparsetype(dec)
        opts = columnopts(plan, j)
        # an explicitly requested string type names the cell type; inferred
        # text follows `stringtype`
        S = dec.parsetype === String ? something(dec.resulttype, stringtype) : nothing
        c = T === nothing ?
                LazyColumn{_lazyeltype(stringtype)}(p.buf, chunks, rowbases, j, opts, nr, stringtype) :
            S !== nothing ?
                LazyColumn{_lazyeltype(S)}(p.buf, chunks, rowbases, j, opts, nr, S) :
                LazyColumn{Union{T, Missing}}(p.buf, chunks, rowbases, j, opts, nr, T)
        push!(cols, c)
    end
    names = p.names[js]
    return LazyFile(_sourcename(source), p, js, names, cols, nr,
                    Dict(nm => i for (i, nm) in enumerate(names)))
end

_lazyeltype(::Type{DataString}) = Union{DataString, Missing}
_lazyeltype(::Type{S}) where {S} = Union{S, Missing}

# Internal lazy-vector implementation. Public callers interact with it through
# `CSV.LazyFile` and the AbstractVector/Tables.jl interfaces.
struct LazyColumn{ELT, T} <: AbstractVector{ELT}   # T: DataString | String | extension string type | a value type
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
        return T === DataString ? s : _rowstring(T, s)
    end
    # `types=Missing` is an intentional sink: every present value recovers to
    # missing, as it does in the eager parser's default collecting mode.
    T === Missing && return missing
    # a typed cell: the same parsers File uses, on demand
    (st == CELL_BADQUOTE || clen == 0 || esc) && return missing
    v, ok = parsevalue(T, c.buf, cpos, cpos + clen - 1, c.opts)
    return ok ? v : missing
end

# DataString's view word has an Int32 offset. Lazy access normally retains
# the source buffer with no copy. For a long cell beyond that absolute offset,
# copy only the cell into its own small backing buffer. The returned value owns
# that buffer, so this fallback is lifetime- and concurrency-safe.
@inline function _lazycompact(buf::Vector{UInt8}, pos::Int, len::Int,
                              viewoffsetlimit::Int=Int(typemax(Int32)))
    len <= INLINE_MAX &&
        return DataString(inline_payload(buf, pos, len), EMPTY_BYTES)
    pos - 1 <= viewoffsetlimit &&
        return DataString(view_payload(buf, pos, len, 0, pos - 1), buf)
    bytes = Vector{UInt8}(undef, len)
    copyto!(bytes, 1, buf, pos, len)
    return DataString(view_payload(bytes, 1, len, 0, 0), bytes)
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
              downcast::Bool=false, stringtype::Type=DataString, strict::Bool=false,
              on_error::Symbol=strict ? :error : :warn,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              validate::Bool=true)
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkonerror(on_error)
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    _checkstringtype(stringtype)
    return _filefromprepared(getfield(lf, :prepared), getfield(lf, :name); types, select, drop,
                             pool, downcast, stringtype, on_error, maxproblems, parallel, ntasks,
                             validate, available=getfield(lf, :sourceindices))
end

# ---------------------------------------------------------------------------
# Rows — streaming
# ---------------------------------------------------------------------------
# `NT` is a NamedTuple type: its names are the row's columns and its field
# types are the cell access types (the requested type, `Missing`, or the
# string type inferred text converts to). Carrying the schema in the type makes
# `row.name`, and `row[j]` with a literal `j`, resolve statically like
# NamedTuple fields: a typed cell parses and returns without a dynamic dispatch
# or a boxed value. `E` is the `on_error` mode.
struct Rows{NT <: NamedTuple, E}
    name::String
    inner::_IndexedRows
    sourceindices::Vector{Int}
    limit::Union{Nothing, Int}
end

function Rows(source; types=nothing, reusebuffer::Bool=false, select=nothing, drop=nothing,
              stringtype::Type=DataString, strict::Bool=false,
              on_error::Symbol=strict ? :error : :collect, kw...)
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW...)
    _checkkwargs("Rows", kw, allowed)
    _checkonerror(on_error)
    on_error === :warn && throw(ArgumentError(
        "Rows does not retain diagnostics; use File with on_error=:warn, " *
        "or Rows with on_error=:error to check cells on access"))
    _checkstringtype(stringtype)
    p = _prepare(source; kw...)
    plan = settlecolumns(p; select, drop, types,
                         validate=get(kw, :validate, true))
    names = p.names[plan.sources]
    # the access type of each output column: a requested type, or the string
    # type that inferred text converts to
    access = Type[something(accessparsetype(plan.columns[j]), stringtype) for j in plan.sources]
    NT = NamedTuple{Tuple(names), Tuple{access...}}
    name = _sourcename(source)
    inner = _IndexedRows(p.buf, p.bi.chunks, p.names,
                         Dict(nm => j for (j, nm) in enumerate(p.names)),
                         plan, p.d, name)
    return Rows{NT, on_error}(name, inner, plan.sources, p.limit)
end

Base.names(::Rows{NT}) where {NT} = collect(Symbol, fieldnames(NT))
# the element type a column's cells have: `Missing`, or a union with `Missing`
# of the type the access converts to (an extension can widen a string request,
# for example an auto-width InlineString to `Union{InlineString, String}`)
_roweltype(::Type{Missing}) = Missing
_roweltype(::Type{T}) where {T} = Union{_rowstringtype(T), Missing}
# The structural index is complete before iteration starts, so the row count
# is known: consumers can preallocate.
Base.IteratorSize(::Type{Rows}) = Base.HasLength()
function Base.length(r::Rows)
    n = sum(nrows, getfield(getfield(r, :inner), :chunks); init=0)
    lim = getfield(r, :limit)
    return lim === nothing ? n : min(n, lim)
end

function Base.show(io::IO, r::Rows)
    n = length(r)
    sch = Tables.schema(r)
    print(io, "CSV.Rows(", repr(getfield(r, :name)), "): ", n, " row", n == 1 ? "" : "s",
          " × ", length(sch.names), " column", length(sch.names) == 1 ? "" : "s",
          " (cells parse on access)")
    for (nm, T) in zip(sch.names, sch.types)
        print(io, "\n  ", nm, "::", T)
    end
end

Tables.istable(::Type{<:Rows}) = true
Tables.rowaccess(::Type{<:Rows}) = true
Tables.rows(r::Rows) = r
Tables.columnnames(r::Rows) = names(r)
Tables.schema(::Rows{NT}) where {NT} =
    Tables.Schema(fieldnames(NT), Tuple{map(_roweltype, fieldtypes(NT))...})
_rowstringtype(T) = T === DataString ? DataString : T

struct Row{NT <: NamedTuple, E} <: Tables.AbstractRow
    view::_IndexedRow
    sourceindices::Vector{Int}
end

Base.eltype(::Type{Rows{NT, E}}) where {NT, E} = Row{NT, E}

function Base.iterate(r::Rows{NT, E}, state=((1, nothing, 1))) where {NT, E}
    lim = getfield(r, :limit)
    lim !== nothing && state[3] > lim && return nothing
    it = iterate(getfield(r, :inner), state)
    it === nothing && return nothing
    view, next = it
    return Row{NT, E}(view, getfield(r, :sourceindices)), next
end

Tables.columnnames(::Row{NT}) where {NT} = fieldnames(NT)
Base.propertynames(::Row{NT}) where {NT} = fieldnames(NT)

@noinline function _throwrowproblem(view::_IndexedRow, j::Int, pos::Int,
                                    kind::Symbol, message::String)
    throw(ParseError(Problem(getfield(view, :rownumber), j, pos, kind, message),
                     1, getfield(getfield(view, :r), :name)))
end

# Rows has no retained diagnostic table. In fail-fast mode, validate and parse
# the requested cell at the access boundary, where its lazy value is first
# observed. This keeps the default allocation-free row view while honoring
# the `strict` keyword and the `on_error` contract.
function _strictrowvalue(view::_IndexedRow, j::Int, ::Type{T}) where {T}
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
    _stringsink(T) && return _typedvalue(T, view, j)
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

# With a literal `j` (as `row.name` and `row[3]` produce) the access type
# and the cell type are constants, so the parse call and the returned value
# are typed; a runtime `j` dispatches once per cell.
@inline function Tables.getcolumn(row::Row{NT, E}, j::Int) where {NT, E}
    T = fieldtype(NT, j)
    v = getfield(row, :view)
    @inbounds sourcej = getfield(row, :sourceindices)[j]
    return _rowcell(T, v, sourcej, Val(E))::_roweltype(T)
end

_rowcell(::Type{T}, v::_IndexedRow, j::Int, ::Val{:error}) where {T} =
    _strictrowvalue(v, j, T)
_rowcell(::Type{Missing}, v::_IndexedRow, j::Int, ::Val{:error}) =
    _strictrowvalue(v, j, Missing)
_rowcell(::Type{Missing}, v::_IndexedRow, j::Int, ::Val) = missing
_rowcell(::Type{T}, v::_IndexedRow, j::Int, ::Val) where {T} = _typedvalue(T, v, j)
# per-cell string materialization for Rows(stringtype=...) and requested
# string types; extensions may add
_rowstring(::Type{String}, x::DataString) = String(x)
_rowstring(::Type{DataString}, x::DataString) = x
_rowstring(::Type{Symbol}, x::DataString) = Symbol(x)
function Tables.getcolumn(row::Row{NT}, nm::Symbol) where {NT}
    j = Base.fieldindex(NT, nm, false)
    j == 0 && throw(KeyError(nm))
    return Tables.getcolumn(row, j)
end

Tables.getcolumn(row::Row, ::Type{T}, j::Int, nm::Symbol) where {T} =
    Tables.getcolumn(row, j)
Base.getindex(row::Row, j::Int) = Tables.getcolumn(row, j)
Base.getindex(row::Row, nm::Symbol) = Tables.getcolumn(row, nm)
Base.getindex(row::Row, nm::AbstractString) = Tables.getcolumn(row, Symbol(nm))
rownumber(row::Row) = getfield(getfield(row, :view), :rownumber)

# ---------------------------------------------------------------------------
# Chunks — batched
# ---------------------------------------------------------------------------

struct Chunks
    name::String
    inner::Batches
    headerlog::ProblemLog
    maxproblems::Int
    plan::ColumnPlan
    on_error::Symbol
    stringtype::Type
    stringrequests::Union{Nothing, Vector{Union{Nothing, Type}}}  # settled per column
    poolspec::Union{Nothing, Tuple{Float64, Int}}
    warned::Base.RefValue{Bool}   # on_error=:warn reports the first batch with problems
end

Base.length(c::Chunks) = length(getfield(c, :inner))
Base.eltype(::Type{Chunks}) = File
Tables.partitions(c::Chunks) = c
Base.names(c::Chunks) = getfield(getfield(c, :inner), :names)
function Base.show(io::IO, c::Chunks)
    inner = getfield(c, :inner)
    n = length(inner)
    st = getfield(c, :stringtype)
    reqs = getfield(c, :stringrequests)
    print(io, "CSV.Chunks(", repr(getfield(c, :name)), "): ", n, " batch", n == 1 ? "" : "es",
          " × ", length(inner.names), " column", length(inner.names) == 1 ? "" : "s")
    for (q, (nm, T, allowmissing)) in enumerate(zip(inner.names, inner.seedtypes, inner.allowmissing))
        decision = inner.plan.columns[inner.plan.sources[q]]
        S = reqs === nothing ? something(decision.resulttype, st) : something(reqs[q], st)
        E = T === String ? _rowstringtype(S) : something(decision.resulttype, T)
        # A ratio/cap pool policy can materialize some DataString batches and
        # leave others as views. Display the set of possible output scalars.
        getfield(c, :poolspec) !== nothing && E === DataString && (E = Union{DataString, String})
        print(io, "\n  ", nm, "::", allowmissing ? Union{E, Missing} : E)
    end
end

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
    t, firstproblem = _narrowtypes(t, getfield(c, :plan),
                                   (ci,), cap, firstproblem; problemrowbase)
    _reportproblems(t, getfield(c, :on_error), firstproblem,
                    "batch $state of $(getfield(c, :name))", getfield(c, :warned),
                    " Later batches do not warn; inspect CSV.problems(batch).")
    # Apply the same final steps as File. Build each requested PooledArray, then
    # build the requested string type.
    st = getfield(c, :stringtype)
    ps = getfield(c, :poolspec)
    ps === nothing || (t = _poolcolumns(t, fill(ps, length(t.columns))))
    t = _finishstrings(t, st, getfield(c, :stringrequests))
    f = File(getfield(c, :name), t,
             Dict(nm => j for (j, nm) in enumerate(names(t))))
    return f, next
end

function Chunks(source; types=nothing, ntasks::Union{Nothing, Int}=nothing,
                maxproblems::Int=10_000, stringtype::Type=DataString,
                pool=DEFAULT_POOL, select=nothing, drop=nothing, strict::Bool=false,
                on_error::Symbol=strict ? :error : :warn, kw...)
    nt = something(ntasks, Threads.nthreads())
    nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    _checkonerror(on_error)
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
    plan = settlecolumns(p; select, drop, types,
                         validate=get(kw, :validate, true))
    seed = Union{Nothing, Type}[d.parsetype for d in plan.columns]
    # One stable schema for the whole row window: seed from the usual
    # stratified sample, then validate every cell of every selected column
    # with the monomorphic scalar parsers (promoting on the first conflict).
    if any(j -> seed[j] === nothing, plan.sources)
        selected = _selectedmask(plan, p.ncols)
        inferred = sampletypes(p.buf, chunks, p.ncols, p.opts; selected, colopts=plan.colopts)
        for j in plan.sources
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    seedtypes = Type[seed[j] for j in plan.sources]
    maxlens = zeros(Int, length(seedtypes))
    allowmissing = settlebatchschema!(seedtypes, p.buf, chunks, plan, maxlens;
                                      parallel=get(kw, :parallel, nt > 1), tasklimit=nt)
    stringrequests = _settlestringrequests(plan, seedtypes, maxlens, stringtype)
    unclosedquote = p.bi.unclosedquote && (p.limit === nothing || p.limit >= fullrows)
    inner = Batches(p.buf, chunks, p.names[plan.sources], plan, seedtypes,
                    allowmissing, p.d, capturecap, unclosedquote,
                    get(kw, :parallel, nt > 1) ? nt : 1)
    return Chunks(name, inner, p.headerlog, maxproblems, plan, on_error,
                  stringtype, stringrequests, poolspec, Ref(false))
end

# One output string type per text column for the whole row window: an
# auto-width request (InlineString) settles on the longest value the schema
# pass saw, so every batch has the same element type. Extensions add methods.
_settledstringtype(::Type{S}, maxlen::Int) where {S} = S
function _settlestringrequests(plan::ColumnPlan, seedtypes::Vector{Type},
                               maxlens::Vector{Int}, stringtype::Type)
    requests = _requestedstrings(plan)
    settled = requests === nothing ? Union{Nothing, Type}[nothing for _ in seedtypes] :
                                     copy(requests)
    changed = false
    for q in eachindex(seedtypes)
        seedtypes[q] === String || continue
        S = something(settled[q], stringtype)
        R = _settledstringtype(S, maxlens[q])
        R === S && continue
        settled[q] = R
        changed = true
    end
    return changed ? settled : requests
end
