# Tables.Scan pushdown for the CSV kernel — what `CSV.File(src; scan=...)`
# runs. The kernel consumes EVERY axis of a Scan exactly (no residual is ever
# handed to Tables.scan), and the differential tests assert the result equals
# `Tables.scan(K.parse(src), scan)` — the generic reference semantics.
#
#   select/rename : bound against the header; unselected columns are never
#                   sampled, parsed, or stitched (zero per-cell cost)
#   types         : convert retained output rows after filtering and bounds
#   limit/offset  : exact row bounds; trailing chunks are never value-parsed
#   filter        : the TWO-PHASE MASKED PARSE — parse only the predicate's
#                   columns first, evaluate the mask, then parse the remaining
#                   selected columns only where the mask is true. Excluded rows
#                   never cost value parsing, string materialization, pooling,
#                   or output-conversion problems, and type inference for phase-2 columns
#                   sees only qualifying rows.
#
# The structural index is built ONCE and shared by both phases.


module KernelScan

using Tables, Dates
using ..CSVKernel
const K = CSVKernel

"""
    KernelScan.scan(source, scan::Tables.Scan; kw...) -> K.ParsedTable

The kernel-level door (raw source, kernel kwargs): index, extract the header,
then `execute`. Equivalent to — but far cheaper than —
`Tables.scan(K.parse(source; kw...), scan)`, which is what the differential
tests assert. `CSV.File(src; scan=...)` reaches `execute` through its own
`_prepare` instead, so scans compose with every File keyword.
"""
function scan(source, sc::Tables.Scan; header::Union{Bool, AbstractVector}=true,
              chunkbytes::Union{Nothing, Int}=nothing, kw...)
    haskey(kw, :types) && throw(ArgumentError("pass column types through the Scan's select items, not types="))
    haskey(kw, :select) && throw(ArgumentError("pass the selection through the Scan, not select="))
    for key in (:limit, :rowmask, :index)
        haskey(kw, key) && throw(ArgumentError("$key is managed by the Scan executor"))
    end
    haskey(kw, :reportstructural) &&
        throw(ArgumentError("reportstructural is managed by the Scan executor"))
    buf = source isa Vector{UInt8} ? source :
          source isa AbstractString ? Vector{UInt8}(codeunits(source)) : Base.read(source)
    dialectkw = NamedTuple(k => v for (k, v) in kw if k in _DIALECTKW)
    parsekw = NamedTuple(k => v for (k, v) in kw)   # parse re-validates; dialect keys pass through
    maxproblems = haskey(parsekw, :maxproblems) ? parsekw.maxproblems : 10_000
    maxproblems >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $maxproblems)"))
    on_error = haskey(parsekw, :on_error) ? parsekw.on_error : :collect
    on_error in (:collect, :error) ||
        throw(ArgumentError("on_error must be :collect or :error"))
    phasecap = max(maxproblems, on_error === :error ? 1 : 0)

    # -- index once; extract the header --------------------------------------
    cb = chunkbytes === nothing ? K._defaultchunkbytes(length(buf)) : chunkbytes
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1
    d = K.Dialect(; dialectkw...)
    indexkw = NamedTuple(k => v for (k, v) in pairs(parsekw)
                         if k in (:parallel, :fastindex, :scanner))
    bi = K.index(buf, d; datastart, chunkbytes=cb, indexkw...)
    opts = K.makevalueopts(d; (k => v for (k, v) in pairs(parsekw)
                               if k in (:dateformat, :decimal, :truestrings, :falsestrings,
                                        :sentinels, :stripwhitespace, :groupmark))...)
    headerlog = K.ProblemLog(phasecap)
    names = if header === true && !isempty(bi.chunks)
        ci = first(bi.chunks)
        K.parseheader!(buf, ci, opts, d, headerlog)
    elseif header isa AbstractVector
        Symbol.(header)
    else
        isempty(bi.chunks) ? Symbol[] :
            [Symbol("Column", j) for j in 1:K.nfields(bi.chunks[1], bi.chunks[1].firstdatarow)]
    end
    names = K.makeunique!(names)
    return execute(buf, bi, names, sc; parsekw, headerlog, maxproblems, on_error)
end

const _DIALECTKW = (:delim, :quotechar, :openquotechar, :closequotechar, :escapechar,
                    :quoted, :comment, :ignoreemptyrows, :ignorerepeated)

"""
    KernelScan.execute(buf, bi, names, scan; parsekw, headerlog, maxproblems, on_error)

The pushdown core over an already-indexed buffer with known column names:
bind the scan, seed types from its select items, and run either the single-
phase (no filter) or the two-phase masked parse. `parsekw` are the kernel
options `K.parse` takes (dialect, value options, pool, engine knobs).
"""
function execute(buf::Vector{UInt8}, bi::K.BufferIndex, names::Vector{Symbol}, scan::Tables.Scan;
                 parsekw, headerlog::K.ProblemLog, maxproblems::Int, on_error::Symbol)
    phasecap = max(maxproblems, on_error === :error ? 1 : 0)
    phasekw = merge(NamedTuple(parsekw), (; maxproblems=phasecap, on_error=:collect))
    b = Tables.bind(scan, names)

    # -- translate the bound scan into kernel pushdown --------------------------
    # Requested output types: duplicate selections of one source column must
    # agree. Narrow types parse through their native kernel type and convert
    # only after filter/offset/limit, exactly like generic Tables.scan.
    seedtypes = Dict{Int, Type}()
    requested = Vector{Union{Nothing, Type}}(nothing, length(names))
    for c in b.columns
        c.type === nothing && continue
        newT = c.type === Missing ? Missing : Base.nonmissingtype(c.type)
        T = get(seedtypes, c.index, nothing)
        T === nothing || T === newT ||
            throw(ArgumentError("column $(names[c.index]) selected twice with conflicting types $T and $newT"))
        seedtypes[c.index] = newT
        haskey(K.NARROW_TYPES, newT) && (requested[c.index] = newT)
    end
    outidx = [c.index for c in b.columns]
    predidx = b.filtercols

    if b.filter === nothing
        # Generic Tables semantics apply offset/limit before requested type
        # conversion. A row mask preserves that order while keeping excluded
        # values out of the typed parse and its diagnostics.
        bounded = b.offset > 0 || b.limit !== nothing
        mask = bounded ? fill(true, sum(K.nrows, bi.chunks; init=0)) : nothing
        mask === nothing || _cliprows!(mask, b.offset, b.limit)
        t = K.parse(buf; index=bi, header=names, select=unique(outidx),
                    types=seedtypes, rowmask=mask, phasekw...)
        sourcerows = mask === nothing ? nothing : findall(mask)
        t = _narrowphase(t, requested, unique(outidx), bi, phasecap; sourcerows)
        t = _project(t, b, names)
        return _finishproblems(t, maxproblems, on_error, headerlog, t)
    end

    # -- two-phase masked parse --------------------------------------------------
    # phase 1: only the predicate's columns, every row
    predsel = sort(unique(predidx))
    t1 = K.parse(buf; index=bi, header=names, select=predsel, phasekw...)
    mask = Tables.filtermask(b.filter, t1)
    # bake offset/limit into the mask: phase 2 parses EXACTLY the output rows
    _cliprows!(mask, b.offset, b.limit)

    # phase 2: parse every requested output column only for qualifying rows.
    # Predicate columns are intentionally parsed again here: the predicate saw
    # native source values, while requested type conversion is an output step.
    outsel = sort(unique(outidx))
    t2 = K.parse(buf; index=bi, header=names, select=outsel,
                 types=seedtypes, rowmask=mask, reportstructural=false, phasekw...)
    kept = findall(mask)
    t2 = _narrowphase(t2, requested, outsel, bi, phasecap; sourcerows=kept)
    t = _project(t2, b, names)
    return _finishproblems(t, maxproblems, on_error, headerlog, t1, t2)
end

function _narrowphase(t::K.ParsedTable, requested, selected, bi::K.BufferIndex,
                      maxproblems::Int; sourcerows=nothing)
    A = Base.parentmodule(@__MODULE__).CSVApi
    narrowed, _ = A._narrowtypes(t, requested, selected, bi.chunks, maxproblems;
                                 sourcerows)
    return narrowed
end

# keep only the first `limit` trues after skipping `offset` trues
function _cliprows!(mask::Vector{Bool}, offset::Int, limit::Union{Nothing, Int})
    seen = 0
    @inbounds for i in eachindex(mask)
        mask[i] || continue
        seen += 1
        if seen <= offset || (limit !== nothing && seen > offset + limit)
            mask[i] = false
        end
    end
    return mask
end

# reorder/rename the driver's (file-ordered, deduplicated) selection into the
# scan's output shape
function _project(t::K.ParsedTable, b::Tables.BoundScan, names::Vector{Symbol})
    lookup = Dict(nm => i for (i, nm) in enumerate(K.names(t)))
    cols = AbstractVector[K.columns(t)[lookup[names[c.index]]] for c in b.columns]
    return K.ParsedTable([c.name for c in b.columns], cols, t.nrows,
                         K.problems(t), t.droppedproblems)
end

function _finishproblems(t::K.ParsedTable, maxproblems::Int, on_error::Symbol,
                         headerlog::K.ProblemLog, phases...)
    items = copy(headerlog.items)
    dropped = headerlog.dropped
    for phase in phases
        phase === nothing && continue
        append!(items, K.problems(phase))
        dropped += phase.droppedproblems
    end
    sort!(items; by=K.problemkey)
    firstproblem = isempty(items) ? nothing : first(items)
    nkeep = min(length(items), maxproblems)
    dropped += length(items) - nkeep
    resize!(items, nkeep)
    if on_error === :error && firstproblem !== nothing
        nproblems = length(items) + dropped
        p = firstproblem
        throw(ErrorException("CSVKernel: $(p.kind) at data row $(p.row), column $(p.col): $(p.message)" *
                             (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
    return K.ParsedTable(K.names(t), K.columns(t), t.nrows, items, dropped)
end

end # module KernelScan
