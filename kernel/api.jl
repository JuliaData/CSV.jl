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
#   • `stringtype` defaults to the kernel string (KStr / CompactString-to-be);
#     `stringtype=String` materializes; InlineStrings become an extension
#   • Bool columns are strictly `true`/`false` unless truestrings/falsestrings
#
# Run the demo:  julia --project=kernel kernel/api.jl

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))
isdefined(Main, :KernelExamples) || include(joinpath(@__DIR__, "examples.jl"))

module CSVApi

using ..CSVKernel, ..KernelExamples
using Tables, Dates, Unicode
const K = CSVKernel
const E = KernelExamples

export sniff, Spec

const DEFAULT_POOL = (0.2, 500)   # CSV.jl's default: pool strings ≤20% unique, ≤500 levels

# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
# CSV.jl semantics: an AbstractString is a FILE PATH (use IOBuffer(str) or
# codeunits for literal data). Everything becomes one in-memory byte buffer —
# the prove-out's L0; mmap/gzip/multi-file live behind this seam as extensions.

resolvesource(buf::Vector{UInt8}) = buf
resolvesource(io::IO) = Base.read(io)
resolvesource(cmd::Base.AbstractCmd) = Base.read(cmd)
resolvesource(s::AbstractString) = isfile(s) ? Base.read(s) :
    throw(ArgumentError("no file at $(repr(String(s))) — a String source is a " *
                        "file path; wrap literal data in IOBuffer"))

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
# are. That subsumes CSV.jl's byte-count-divisibility heuristic (which cannot
# see quotes spanning rows) while agreeing with it on unambiguous files.
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

# quote-aware sample clip: index the prefix and keep whole rows only
function _sample(buf::Vector{UInt8}, samplebytes::Int)
    length(buf) <= samplebytes && return buf
    return buf[1:samplebytes]   # candidate scoring drops each sample's last (possibly cut) row
end

function _scoredelim(buf::Vector{UInt8}, delim::Char, datastart::Int, clipped::Bool; kw...)
    d = K.Dialect(; delim, kw...)
    bi = try
        K.index(buf, d; datastart, parallel=false)
    catch
        return (0.0, 0)
    end
    counts = Int[]
    for ci in bi.chunks, lr in 1:K.totalrows(ci)
        push!(counts, K.nfields(ci, lr))
        length(counts) >= 11 && break
    end
    clipped && length(counts) > 1 && pop!(counts)   # drop the possibly-cut row
    isempty(counts) && return (0.0, 0)
    modal = argmax(c -> count(==(c), counts), unique(counts))
    return (count(==(modal), counts) / length(counts), modal)
end

"""
    CSVApi.sniff(source; samplebytes=65536, kw...) -> Spec

Detect the delimiter (quote-aware field-count consistency over a bounded
sample, candidates $(DELIM_CANDIDATES) in CSV.jl's order), whether a header
row is likely (row 1 all text while later rows type differently), and the
resulting names/types. `kw` may pin dialect pieces (`quotechar`, `comment`,
...) that sniffing should take as given.
"""
function sniff(source; samplebytes::Int=1 << 16, kw...)
    buf = resolvesource(source)
    sample = _sample(buf, samplebytes)
    clipped = length(sample) < length(buf)
    datastart = _datastart(sample)
    best, bestdelim = (-1.0, 0), first(DELIM_CANDIDATES)
    for c in DELIM_CANDIDATES
        consistency, fields = _scoredelim(sample, c, datastart, clipped; kw...)
        score = (consistency * (fields > 1), fields)   # single-field parses rank last
        if score > best
            best, bestdelim = score, c
        end
    end
    # header detection: parse the sample twice — types with row 1 as data vs
    # header. A likely header = row 1 headerless-types degrade to String while
    # the with-header types do not (numbers under a text row 1).
    limit = 100
    theader = K.parse(sample; delim=bestdelim, header=true, limit, parallel=false, kw...)
    tnoheader = K.parse(sample; delim=bestdelim, header=false, limit, parallel=false, kw...)
    headerlikely = tnoheader.nrows > theader.nrows &&
        any(zip(K.columns(theader), K.columns(tnoheader))) do (ch, cnh)
            Base.nonmissingtype(eltype(ch)) !== String && eltype(ch) !== Missing &&
                Base.nonmissingtype(eltype(cnh)) in (String, K.KStr)
        end
    t = headerlikely ? theader : tnoheader
    return Spec(bestdelim, get(kw, :quoted, true), headerlikely,
                length(K.names(t)), copy(K.names(t)), Type[eltype(c) for c in K.columns(t)])
end

# delimiter-only sniff for File(delim=nothing) — no second parse
function _sniffdelim(buf::Vector{UInt8}, samplebytes::Int; kw...)
    sample = _sample(buf, samplebytes)
    clipped = length(sample) < length(buf)
    datastart = _datastart(sample)
    best, bestdelim = (-1.0, 0), first(DELIM_CANDIDATES)
    for c in DELIM_CANDIDATES
        consistency, fields = _scoredelim(sample, c, datastart, clipped; kw...)
        score = (consistency * (fields > 1), fields)
        if score > best
            best, bestdelim = score, c
        end
    end
    return bestdelim
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
        off > length(buf) &&
            throw(ArgumentError("row $n is past the end of the data"))
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

const _VALUEKW = (:dateformat, :decimal, :truestrings, :falsestrings, :sentinels,
                  :stripwhitespace, :groupmark)

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
                  kw...)
    footerskip >= 0 || throw(ArgumentError("footerskip must be ≥ 0 (got $footerskip)"))
    buf = resolvesource(source)
    dialectonly = NamedTuple(p for p in pairs(kw)
                             if p.first in (:quotechar, :openquotechar, :closequotechar,
                                            :escapechar, :quoted, :comment,
                                            :ignoreemptyrows, :ignorerepeated))
    if delim === nothing
        get(kw, :ignorerepeated, false) &&
            throw(ArgumentError("auto-delimiter detection is not supported with " *
                                "ignorerepeated=true; pass delim explicitly"))
        delim = _sniffdelim(buf, samplebytes; dialectonly...)
    end
    # missingstring → kernel sentinels ("" entries are inert: empty is always missing)
    sentinels = missingstring === nothing ? nothing :
                missingstring isa AbstractString ? (isempty(missingstring) ? nothing : [String(missingstring)]) :
                [String(s) for s in missingstring if !isempty(s)]
    sentinels !== nothing && isempty(sentinels) && (sentinels = nothing)
    valuekw = NamedTuple(p for p in pairs(kw) if p.first in _VALUEKW)
    d = K.Dialect(; delim, dialectonly...)
    opts = K.makevalueopts(d; sentinels, valuekw...)
    cb = chunkbytes === nothing ?
         clamp(cld(length(buf), 2 * Threads.nthreads()), 1 << 16, 1 << 20) : chunkbytes

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
    bi = K.index(buf, d; datastart, chunkbytes=cb, parallel,
                 (p for p in pairs(kw) if p.first in (:fastindex, :scanner))...)
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
        for _ in headerrows
            k = _firstlive(chunks)
            k === nothing && throw(ArgumentError("header rows $header past the end of the data"))
            push!(parts, K.parseheader!(buf, chunks[k], opts, d, headerlog))
        end
        n = maximum(length, parts)
        [Symbol(join((j <= length(p) ? String(p[j]) : "Column$j" for p in parts), "_"))
         for j in 1:n]
    end
    normalizenames && (names = [normalizename(String(nm)) for nm in names])
    names = K.makeunique!(names)

    if skipto !== nothing
        skipto > headerrow ||
            throw(ArgumentError("skipto=$skipto must be past the header (row $headerrow)"))
        _skiptobyte!(chunks, _rawrowoffset(buf, d, rawstart, Int(skipto)))
    end
    total = sum(K.nrows, chunks; init=0)
    keep = max(total - Int(footerskip), 0)
    lim = limit === nothing ? (footerskip > 0 ? keep : nothing) : min(Int(limit), keep)

    # engine + diagnostics kwargs the kernel driver consumes directly
    passthrough = NamedTuple(p for p in pairs(kw)
                             if p.first in (:quotechar, :openquotechar, :closequotechar,
                                            :escapechar, :quoted, :comment, :ignoreemptyrows,
                                            :ignorerepeated, :dateformat, :decimal,
                                            :truestrings, :falsestrings, :stripwhitespace,
                                            :groupmark, :fastindex, :scanner, :maxproblems,
                                            :nsample))
    parsekw = merge(passthrough, (; delim, sentinels, chunkbytes=cb, parallel))
    return Prepared(buf, bi, names, length(names), lim, opts, d, headerlog, parsekw)
end

# kwargs _prepare consumes itself (not forwarded to the kernel driver)
const _PREPKW = (:header, :normalizenames, :skipto, :footerskip, :missingstring,
                 :delim, :limit, :samplebytes, :chunkbytes, :parallel)

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
`nsample`. Diagnostics are data: [`problems`](@ref)`(f)`.
"""
struct File
    name::String
    table::K.ParsedTable
    lookup::Dict{Symbol, Int}
end

function File(source;
              types=nothing, select=nothing, drop=nothing,
              pool=DEFAULT_POOL,
              stringtype::Type=K.KStr,
              strict::Bool=false, on_error::Symbol=strict ? :error : :collect,
              maxwarnings::Union{Nothing, Int}=nothing,
              maxproblems::Int=something(maxwarnings, 10_000),
              ntasks::Union{Nothing, Int}=nothing,
              parallel::Bool=ntasks === nothing ? Threads.nthreads() > 1 : ntasks > 1,
              kw...)
    p = _prepare(source; parallel, maxproblems, kw...)
    sel = _resolveselect(select, drop, p.names)
    t = K.parse(p.buf; index=p.bi, header=p.names, types, select=sel, limit=p.limit,
                pool, maxproblems, on_error, p.parsekw...)
    t = _mergeheaderlog(t, p.headerlog, maxproblems)
    stringtype === String && (t = _materializestrings(t))
    nm = source isa AbstractString ? String(source) : "<$(nameof(typeof(source)))>"
    return File(nm, t, Dict(n => j for (j, n) in enumerate(K.names(t))))
end

function _mergeheaderlog(t::K.ParsedTable, headerlog::K.ProblemLog, cap::Int)
    isempty(headerlog.items) && return t
    log = K.ProblemLog(cap)
    for pr in headerlog.items
        K.pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
    end
    for pr in t.problems
        K.pushproblem!(log, pr.row, pr.col, pr.pos, pr.kind, pr.message)
    end
    K.sortproblems!(log)
    return K.ParsedTable(t.names, t.columns, t.nrows, log.items,
                         log.dropped + t.droppedproblems)
end

function _materializestrings(t::K.ParsedTable)
    cols = AbstractVector[
        if !(eltype(col) <: Union{K.KStr, Missing}) || eltype(col) === Missing ||
           col isa K.PooledColumn
            col
        elseif Missing <: eltype(col)
            Union{String, Missing}[ismissing(x) ? missing : String(x) for x in col]
        else
            String[String(x) for x in col]
        end
        for col in t.columns]
    return K.ParsedTable(t.names, cols, t.nrows, t.problems, t.droppedproblems)
end

# --- Tables.jl + row access -------------------------------------------------

# NB: getproperty resolves COLUMNS first (f.score), so interface methods must
# reach internals via getfield — a column named `table` must not shadow them.
Tables.istable(::Type{File}) = true
Tables.columnaccess(::Type{File}) = true
Tables.columns(f::File) = getfield(f, :table)
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

Tables.columnnames(r::FileRow) = K.names(getfield(r, :f).table)
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
    isempty(t.problems) ||
        print(io, "\n$(length(t.problems)) problem(s) recorded — see problems(f)")
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

function Rows(source; types=nothing, kw...)
    for k in keys(kw)
        k in _PREPKW || k in (:quotechar, :openquotechar, :closequotechar,
              :escapechar, :quoted, :comment, :ignoreemptyrows, :ignorerepeated,
              :dateformat, :decimal, :truestrings, :falsestrings, :stripwhitespace,
              :groupmark, :fastindex, :scanner, :maxproblems, :nsample) ||
            throw(ArgumentError("unsupported Rows keyword $k"))
    end
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
Unlike `CSV.Chunks`, every batch reports the SAME column types — a whole-file
schema prepass over the index makes the batch schema stable by construction.
`ntasks` sizes the batches (or pass `chunkbytes` directly).
"""
function Chunks(source; ntasks::Union{Nothing, Int}=nothing, kw...)
    nt = something(ntasks, Threads.nthreads())
    nt >= 1 || throw(ArgumentError("ntasks must be ≥ 1 (got $nt)"))
    if !haskey(kw, :chunkbytes)
        buf = resolvesource(source)
        kw = (; kw..., chunkbytes=clamp(cld(length(buf), nt), 1 << 10, 1 << 22))
        source = buf
    end
    types = get(kw, :types, nothing)
    p = _prepare(source; (pr for pr in pairs(kw) if pr.first !== :types)...)
    chunks = filter(ci -> K.nrows(ci) > 0, p.bi.chunks)
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
    return E.Batches(p.buf, chunks, p.names, seedtypes, userprovided, allowmissing,
                     p.opts, p.d, get(kw, :maxproblems, 10_000), p.bi.unclosedquote)
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
