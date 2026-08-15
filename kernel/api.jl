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
# Run the demo:  julia --project=kernel kernel/api.jl

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))
isdefined(Main, :KernelExamples) || include(joinpath(@__DIR__, "examples.jl"))

module CSVApi

using ..CSVKernel, ..KernelExamples
using Tables, Dates, Unicode, Mmap, PooledArrays, CodecZlib
const K = CSVKernel
const E = KernelExamples

export sniff, Spec

const DEFAULT_POOL = (0.2, 500)   # CSV.jl's default: pool strings ≤20% unique, ≤500 levels

"""
    CSVApi.read(source, scan::Tables.Scan; kw...)

The Tables.Scan front door: every axis of `scan` (select/rename, pinned
types, limit/offset, filter) pushes into the kernel — unselected columns are
never parsed, filtered-out rows never cost value work. Sources resolve
exactly like `File` (paths, IO, bytes, Cmd, gzip by magic, mmap); `kw` is
the usual dialect/value surface.
"""
# defined only when the Tables.Scan proposal is present (the dev'd Tables);
# released Tables loads this file without the Scan surface
if isdefined(Tables, :Scan)
    # Load the executor while this file is evaluated. Loading it lazily inside
    # `read` defines `KernelScan.read` in a newer world than the active call,
    # which fails on a fresh Julia 1.12 process.
    isdefined(Main, :KernelScan) || Base.include(Main, joinpath(@__DIR__, "scan.jl"))
    function read(source, scan::Tables.Scan; buffer_in_memory::Bool=false,
                  prefetch::Bool=true, missingstring=nothing, kw...)
        haskey(kw, :sentinels) &&
            throw(ArgumentError("pass missing spellings as missingstring, not sentinels"))
        buf = resolvesource(source; buffer_in_memory, prefetch)
        return Main.KernelScan.read(buf, scan; sentinels=_sentinels(missingstring), kw...)
    end
end

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
    :validate => "column references are always validated",
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
    isfile(s) || throw(ArgumentError("no file at $(repr(String(s))) — a String " *
                                     "source is a file path; wrap literal data in IOBuffer"))
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
function _sample(buf::Vector{UInt8}, samplebytes::Int; dialectkw...)
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    length(buf) <= samplebytes && return buf
    sample = buf[1:samplebytes]
    d = K.Dialect(; delim=_probedelim(dialectkw), dialectkw...)
    datastart = _datastart(sample)
    rowstart = datastart
    while rowstart <= length(sample)
        next = K.nextrowstart(sample, rowstart, length(sample), d, false)
        next > length(sample) && break
        rowstart = next
    end
    return rowstart <= 1 ? UInt8[] : sample[1:rowstart - 1]
end

function _scoredelim(buf::Vector{UInt8}, delim::Char, datastart::Int,
                     dialectkw::NamedTuple, indexkw::NamedTuple)
    quoted = haskey(dialectkw, :quoted) ? dialectkw.quoted : true
    quotechar = haskey(dialectkw, :quotechar) ? dialectkw.quotechar : '"'
    openquotechar = haskey(dialectkw, :openquotechar) ? dialectkw.openquotechar : nothing
    quoted && delim == something(openquotechar, quotechar) && return (0.0, 0, 0)
    d = K.Dialect(; delim, dialectkw...)
    bi = K.index(buf, d; datastart, parallel=false, indexkw...)
    counts = Int[]
    for ci in bi.chunks, lr in 1:K.totalrows(ci)
        push!(counts, K.nfields(ci, lr))
        length(counts) >= 11 && break
    end
    isempty(counts) && return (0.0, 0, 0)
    modal = argmax(c -> count(==(c), counts), unique(counts))
    return (count(==(modal), counts) / length(counts), modal, first(counts))
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
        stop = K.nextrowstart(sample, stop, length(sample), d0, false)
        rows += 1
    end
    scoresample = stop > length(sample) ? sample : sample[1:stop - 1]
    best, bestdelim = (false, 0.0, 1), first(DELIM_CANDIDATES)
    for c in DELIM_CANDIDATES
        consistency, fields, firstfields = _scoredelim(scoresample, c, datastart,
                                                       dialectkw, indexkw)
        represented = firstfields > 1
        score = represented ? (true, consistency, fields) : (false, 0.0, 1)
        if score > best
            best, bestdelim = score, c
        end
    end
    return bestdelim
end

"""
    CSVApi.sniff(source; samplebytes=65536, kw...) -> Spec

Detect the delimiter (quote-aware field-count consistency over a bounded
sample, candidates $(DELIM_CANDIDATES) in CSV.jl's order), whether a header
row is likely (row 1 all text while later rows type differently), and the
resulting names/types. `kw` may pin dialect, value, and index pieces
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
    bestdelim = _detectdelim(sample, dialectkw, indexkw)
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
function _sniffdelim(buf::Vector{UInt8}, samplebytes::Int,
                     dialectkw::NamedTuple, indexkw::NamedTuple)
    sample = _sample(buf, samplebytes; dialectkw...)
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

# byte offset of raw structural row `n` (1-based from `datastart`)
function _rawrowoffset(buf::Vector{UInt8}, d::K.Dialect, datastart::Int, n::Int)
    off = datastart
    for _ in 1:(n - 1)
        off > length(buf) && return length(buf) + 1
        off = K.nextrowstart(buf, off, length(buf), d, false)
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
        rowstart = K.nextrowstart(buf, rowstart, length(buf), d, false)
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
                  kw...)
    footerskip >= 0 || throw(ArgumentError("footerskip must be ≥ 0 (got $footerskip)"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    samplebytes >= 1 || throw(ArgumentError("samplebytes must be ≥ 1 (got $samplebytes)"))
    allowed = (_DIALECTKW..., _VALUEKW..., _INDEXKW..., _DRIVERKW...)
    _checkkwargs("File/Rows/Chunks", kw, allowed)
    buf = resolvesource(source; buffer_in_memory, prefetch)
    dialectonly = _pickkwargs(kw, _DIALECTKW)
    indexonly = _pickkwargs(kw, _INDEXKW)
    if delim === nothing
        get(kw, :ignorerepeated, false) &&
            throw(ArgumentError("auto-delimiter detection is not supported with " *
                                "ignorerepeated=true; pass delim explicitly"))
        delim = _sniffdelim(buf, samplebytes, dialectonly, indexonly)
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
    header isa Integer && (header = header == 0 ? false : Int(header))
    headerrows = header isa AbstractVector{<:Integer} ? Int.(header) :
                 header isa Int ? [header] : Int[]
    if !isempty(headerrows)
        (issorted(headerrows) && first(headerrows) >= 1 &&
         headerrows == first(headerrows):last(headerrows)) ||
            throw(ArgumentError("header rows must be consecutive and ≥ 1 (got $header)"))
    end
    headerrow = header === true ? 1 : isempty(headerrows) ? 0 : last(headerrows)
    rawstart = _datastart(buf)
    datastart = isempty(headerrows) || first(headerrows) == 1 ? rawstart :
                _rawrowoffset(buf, d, rawstart, first(headerrows))
    bi = K.index(buf, d; datastart, chunkbytes=cb, parallel, indexonly...)
    chunks = bi.chunks
    headerlog = K.ProblemLog(get(kw, :maxproblems, 10_000))

    names = if header isa AbstractVector && !(header isa AbstractVector{<:Integer})
        Symbol.(header)
    elseif header === false || isempty(chunks)
        k = _firstlive(chunks)
        n = k === nothing ? 0 : K.nfields(chunks[k], chunks[k].firstdatarow)
        [Symbol("Column", j) for j in 1:n]
    elseif header === true || length(headerrows) == 1
        k = _firstlive(chunks)
        k === nothing ? Symbol[] : K.parseheader!(buf, chunks[k], opts, d, headerlog)
    else
        # multi-row header: every part participates in the join (blank cells
        # resolve to ColumnN first) — pinned against CSV.jl
        parts = Vector{Vector{Symbol}}()
        firstrows = Int[ci.firstdatarow for ci in chunks]
        for _ in headerrows
            k = _firstlive(chunks)
            k === nothing && break
            push!(parts, K.parseheader!(buf, chunks[k], opts, d, headerlog))
        end
        for (ci, firstrow) in zip(chunks, firstrows)
            ci.firstdatarow = firstrow
        end
        _skiptobyte!(chunks, _rawrowoffset(buf, d, rawstart, headerrow + 1))
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
        _skiptobyte!(chunks, _rawrowoffset(buf, d, rawstart, Int(skipto)))
    end
    footer = _footeroffset(buf, d, rawstart, Int(footerskip))
    keep = footerskip == 0 ? sum(K.nrows, chunks; init=0) : _rowsbefore(chunks, footer)
    lim = limit === nothing ? (footerskip > 0 ? keep : nothing) : min(Int(limit), keep)

    # engine + diagnostics kwargs the kernel driver consumes directly
    colopts = nothing
    if dfdict !== nothing
        overrides = Dict{Int, Any}()
        for (key, fmt) in dfdict
            j = key isa Integer ? Int(key) : findfirst(==(Symbol(key)), names)
            (j === nothing || !(1 <= j <= length(names))) &&
                throw(ArgumentError("dateformat column $key not found"))
            overrides[j] = fmt
        end
        colopts = K.ValueOpts[haskey(overrides, j) ?
                              K.makevalueopts(d; sentinels, valuekw...,
                                              dateformat=overrides[j]) : opts
                              for j in 1:length(names)]
    end
    passthrough = _pickkwargs(kw, allowed)
    dfdict !== nothing &&
        (passthrough = NamedTuple(kv for kv in pairs(passthrough) if kv.first != :dateformat))
    parsekw = merge(passthrough, (; delim, sentinels, chunkbytes=cb, parallel, colopts))
    return Prepared(buf, bi, names, length(names), lim, opts, d, headerlog, parsekw)
end

# kwargs _prepare consumes itself (not forwarded to the kernel driver)
const _PREPKW = (:header, :normalizenames, :skipto, :footerskip, :missingstring,
                 :delim, :limit, :samplebytes, :chunkbytes, :parallel,
                 :buffer_in_memory, :prefetch)

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
            j = findfirst(==(Symbol(s)), names)
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

`CSV.File` analog. `source` is a file path, `IO`, `Vector{UInt8}`, or `Cmd`.
Keywords (CSV.jl names): `header` (row number | Bool | names | rows-to-merge),
`normalizenames`, `skipto`, `footerskip`, `limit`, `missingstring`, `delim`
(`nothing` ⇒ sniffed), `quotechar`/`openquotechar`/`closequotechar`/`escapechar`,
`quoted`, `comment`, `ignoreemptyrows`, `ignorerepeated`, `dateformat`,
`decimal`, `truestrings`/`falsestrings`, `stripwhitespace`, `groupmark`,
`types` (Type | Vector | Dict), `select`/`drop` (lists), `pool`
(default `(0.2, 500)` — CSV.jl's), `stringtype` (kernel string | `String`),
`strict`/`on_error`, `maxwarnings`/`maxproblems`, `ntasks`/`parallel`,
`nsample`, `buffer_in_memory`. Diagnostics are data: [`problems`](@ref)`(f)`.
"""
struct File
    name::String
    table::K.ParsedTable
    lookup::Dict{Symbol, Int}
end

function File(source;
              types=nothing, select=nothing, drop=nothing,
              pool=DEFAULT_POOL,
              downcast::Bool=false,
              transpose::Bool=false,
              stringtype::Type=K.CompactString,
              strict::Bool=false, on_error::Symbol=strict ? :error : :collect,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              kw...)
    if transpose
        (select !== nothing || drop !== nothing) &&
            throw(ArgumentError("select/drop are not supported with transpose=true"))
        return _transposedfile(source; types, downcast, stringtype, kw...)
    end
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    stringtype in (K.CompactString, String) ||
        throw(ArgumentError("stringtype must be CSVKernel.CompactString or String (got $stringtype)"))
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    capturecap = max(maxproblems, 1)
    p = _prepare(source; parallel, maxproblems=capturecap, kw...)
    sel = _resolveselect(select, drop, p.names)
    parsetypes = if p.limit == 0
        Type[T === nothing ? Missing : T for T in K.resolvetypes(types, p.names, p.ncols)]
    else
        types
    end
    poolarg, poolspecs = _resolvepool(pool, p.names, p.ncols)
    t = K.parse(p.buf; index=p.bi, header=p.names, types=parsetypes, select=sel, limit=p.limit,
                pool=poolarg, poolspecs, on_error=:collect, p.parsekw...)
    t, firstproblem = _mergeproblems(t, p.headerlog, maxproblems)
    if on_error === :error && firstproblem !== nothing
        pr = firstproblem
        nproblems = length(t.problems) + t.droppedproblems
        throw(ErrorException("CSVKernel: $(pr.kind) at data row $(pr.row), column $(pr.col): " *
                             pr.message * (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
    t = _pooledarrays(t)
    downcast && (t = _downcast(t))
    stringtype === String && (t = _materializestrings(t))
    nm = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
    return File(nm, t, Dict(n => j for (j, n) in enumerate(K.names(t))))
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

# Bulk materialization through K.materialize — one shared scratch, word-store
# inline reconstruction, unsafe_string per cell. The per-cell String() broadcast
# this replaces ran the generic AbstractString path and was the measured
# 55–110 MiB/s cliff on string-heavy shapes.
# ---------------------------------------------------------------------------
# transpose=true — the compatibility path. Rows are columns: input row j is
# output column j; with header=true the first field of each row is that
# column's name. Types are inferred EXACTLY (every cell participates — these
# files are small by construction), or taken from `types`. Single-threaded,
# strings materialize as String. select/drop/limit are not supported here.
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
        for f in startf:nf
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
        v, ok = K.parsevalue(T, buf, cpos, cpos + clen - 1, opts, scratch)
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
                         header::Union{Bool, Integer}=true,
                         missingstring=nothing, delim=',',
                         normalizenames::Bool=false,
                         buffer_in_memory::Bool=false, prefetch::Bool=true, kw...)
    allowed = (_DIALECTKW..., _VALUEKW...)
    _checkkwargs("File(transpose=true)", kw, allowed)
    hasnames = header === true || header == 1
    hasnames || header === false ||
        throw(ArgumentError("transpose=true supports header=true (names in field 1 of " *
                            "each row) or header=false"))
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
    startf = hasnames ? 2 : 1
    n = ncols == 0 ? 0 :
        maximum(K.nfields(r[1], r[2]) - (startf - 1) for r in rows)
    n = max(n, 0)
    names = Symbol[hasnames ? Symbol(_cellstring(buf, r[1], r[2], 1, opts)) :
                   Symbol("Column", j) for (j, r) in enumerate(rows)]
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = K.makeunique!(names)
    seed = K.resolvetypes(types, names, ncols)
    cols = AbstractVector[_transposedcolumn(buf, r[1], r[2], startf, n, seed[j], opts)
                          for (j, r) in enumerate(rows)]
    t = K.ParsedTable(names, cols, n, K.Problem[], 0)
    downcast && (t = _downcast(t))
    nm = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
    return File(nm, t, Dict(nm2 => j for (j, nm2) in enumerate(names)))
end

# pool as Dict(col => spec) or per-column vector: resolve to kernel poolspecs
# (entries nothing = never pool). Scalars pass through as the global policy.
function _resolvepool(pool, names::Vector{Symbol}, ncols::Int)
    if pool isa AbstractDict
        specs = Vector{Any}(nothing, ncols)
        for (key, sp) in pool
            j = key isa Integer ? Int(key) : findfirst(==(Symbol(key)), names)
            (j === nothing || !(1 <= j <= ncols)) &&
                throw(ArgumentError("pool column $key not found"))
            specs[j] = K._poolpolicy(sp)
        end
        return false, specs
    elseif pool isa AbstractVector
        length(pool) == ncols ||
            throw(ArgumentError("pool vector length $(length(pool)) != $ncols columns"))
        return false, Any[K._poolpolicy(sp) for sp in pool]
    end
    return pool, nothing
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
function _topooledarray(c::K.PooledColumn{ELT}) where {ELT}
    n = length(c.levels)
    if !(Missing <: ELT)
        pool = Vector{String}(undef, n)
        @inbounds for i in 1:n
            pool[i] = String(c.levels[i])
        end
        invpool = Dict{String, UInt32}(pool[i] => UInt32(i) for i in 1:n)
        return PooledArray(PooledArrays.RefArray(K.poolrefs(c)), invpool, pool)
    end
    pool = Vector{Union{String, Missing}}(undef, n + 1)
    @inbounds for i in 1:n
        pool[i] = String(c.levels[i])
    end
    pool[n + 1] = missing
    invpool = Dict{Union{String, Missing}, UInt32}(pool[i] => UInt32(i) for i in 1:(n + 1))
    oldrefs = K.poolrefs(c)
    refs = similar(oldrefs)
    mref = UInt32(n + 1)
    @inbounds @simd for i in eachindex(refs)
        r = oldrefs[i]
        refs[i] = r == 0 ? mref : r
    end
    return PooledArray(PooledArrays.RefArray(refs), invpool, pool)
end

function _pooledarrays(t::K.ParsedTable)
    any(c -> c isa K.PooledColumn, t.columns) || return t
    cols = AbstractVector[c isa K.PooledColumn ? _topooledarray(c) : c
                          for c in t.columns]
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

function _materializestrings(t::K.ParsedTable)
    cols = AbstractVector[col isa K.CompactStringVector ? K.materialize(col) : col
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
into a Tables.jl `sink` — `read(path, DataFrame)`, `read(path, columntable)`.
"""
read(source, sink; kw...) = Tables.materializer(sink)(File(source; kw...))

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
end

function Rows(source; types=nothing, reusebuffer::Bool=false, kw...)
    # reusebuffer is CSV.jl surface: rows here are lazy views over the index —
    # no per-row buffer exists to reuse, so the kwarg is accepted and inert
    allowed = (_PREPKW..., _DIALECTKW..., _VALUEKW..., _INDEXKW...)
    _checkkwargs("Rows", kw, allowed)
    p = _prepare(source; kw...)
    seed = types === nothing ? nothing :
           Type[T === nothing ? String : T
                for T in K.resolvetypes(types, p.names, p.ncols)]
    inner = E.Rows(p.buf, p.bi.chunks, p.names,
                   Dict(nm => j for (j, nm) in enumerate(p.names)), p.opts, p.d)
    return Rows(inner, seed, p.limit)
end

Tables.istable(::Type{Rows}) = true
Tables.rowaccess(::Type{Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) = r.types === nothing ?
    Tables.Schema(r.inner.names, fill(Union{String, Missing}, length(r.inner.names))) :
    Tables.Schema(r.inner.names, Type[Union{T, Missing} for T in r.types])

struct Row <: Tables.AbstractRow
    view::E.RowView
    types::Union{Nothing, Vector{Type}}
end

Base.eltype(::Type{Rows}) = Row
Base.IteratorSize(::Type{Rows}) = Base.SizeUnknown()

function Base.iterate(r::Rows, state=((1, nothing, 1)))
    r.limit !== nothing && state[3] > r.limit && return nothing
    it = iterate(r.inner, state)
    it === nothing && return nothing
    view, next = it
    return Row(view, r.types), next
end

Tables.columnnames(row::Row) = Tables.columnnames(getfield(row, :view))
function Tables.getcolumn(row::Row, j::Int)
    ts = getfield(row, :types)
    v = getfield(row, :view)
    return ts === nothing ? v[j] : E.typedvalue(ts[j], v, j)
end
Tables.getcolumn(row::Row, nm::Symbol) =
    Tables.getcolumn(row, getfield(getfield(row, :view), :r).lookup[nm])
Base.getindex(row::Row, j) = Tables.getcolumn(row, j)
rownumber(row::Row) = getfield(getfield(row, :view), :rownumber)

# ---------------------------------------------------------------------------
# Chunks — batched
# ---------------------------------------------------------------------------

"""
    CSVApi.Chunks(source; ntasks=Threads.nthreads(), kw...)

`CSV.Chunks` analog: iterate the file as a sequence of `File`-shaped tables.
Unlike `CSV.Chunks`, every batch reports the SAME column types — a whole-window
schema prepass over the index makes the batch schema stable by construction.
`ntasks` sizes the batches (or pass `chunkbytes` directly).
"""
struct Chunks
    inner::E.Batches
    headerlog::K.ProblemLog
    maxproblems::Int
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
    return t, next
end

function Chunks(source; types=nothing, ntasks::Union{Nothing, Int}=nothing,
                maxproblems::Int=10_000, kw...)
    nt = something(ntasks, Threads.nthreads())
    nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
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
    seed = K.resolvetypes(types, p.names, p.ncols)
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
    return Chunks(inner, p.headerlog, maxproblems)
end

# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

function demo()
    path, io = mktemp()
    write(io, "name,score,when\nalice,10,2024-01-02\nbob,11.5,2024-01-03\ncarol,NA,2024-01-04\n")
    close(io)
    println("== sniff ==")
    show(stdout, sniff(path)); println("\n")
    println("== File ==")
    f = File(path; missingstring="NA")
    show(stdout, f); println()
    println("score column: ", collect(f.score))
    println("row 2 name:   ", f[2].name)
    println("\n== Rows (typed) ==")
    for row in Rows(path; types=Dict(:score => Float64), missingstring="NA")
        println("  ", row.name, " → ", row.score)
    end
    println("\n== Chunks ==")
    for (k, batch) in enumerate(Chunks(path; chunkbytes=32, missingstring="NA"))
        println("  batch $k: $(batch.nrows) rows, score::$(eltype(batch[:score]))")
    end
    rm(path)
end

end # module CSVApi

abspath(PROGRAM_FILE) == (@__FILE__) && CSVApi.demo()
