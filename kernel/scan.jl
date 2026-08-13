# Tables.Scan pushdown for the CSV kernel — the CSV.read(source, scan) analog.
#
# This is the integration the Tables.scan proposal promises: the kernel
# consumes EVERY axis of a Scan exactly, so the residual is always empty and
# Tables.finish is the identity.
#
#   select/rename : bound against the header; unselected columns are never
#                   sampled, parsed, or stitched (zero per-cell cost)
#   types         : seed the column type — inference skipped for that column
#   limit/offset  : exact row bounds; trailing chunks are never value-parsed
#   filter        : the TWO-PHASE MASKED PARSE — parse only the predicate's
#                   columns first, evaluate the mask, then parse the remaining
#                   selected columns only where the mask is true. Excluded rows
#                   never cost value parsing, string materialization, pooling,
#                   or problem reports, and type inference for phase-2 columns
#                   sees only qualifying rows.
#
# The structural index is built ONCE and shared by both phases.

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))
isdefined(Main, :KernelExamples) || include(joinpath(@__DIR__, "examples.jl"))

module KernelScan

using Tables, Dates
using ..CSVKernel
const K = CSVKernel

"""
    KernelScan.read(source, scan::Tables.Scan; kw...) -> K.ParsedTable

Parse `source` with every axis of `scan` pushed into the kernel. `kw` are the
usual kernel options (dialect, dateformat, pool, ...). Equivalent to — but far
cheaper than — `Tables.finish(K.parse(source; kw...), scan)`, which is exactly
what the differential tests assert.
"""
function read(source, scan::Tables.Scan; kw...)
    t, residual = apply(source, scan; kw...)
    return Tables.finish(t, residual)   # residual is empty: identity
end

const _DIALECTKW = (:delim, :quotechar, :openquotechar, :closequotechar, :escapechar,
                    :quoted, :comment, :ignoreemptyrows, :ignorerepeated)

function apply(source, scan::Tables.Scan; header::Union{Bool, AbstractVector}=true,
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
    phasekw = merge(parsekw, (; maxproblems=phasecap, on_error=:collect))

    # -- index once; bind the scan against the extracted header ----------------
    cb = chunkbytes === nothing ?
         clamp(cld(length(buf), 2 * Threads.nthreads()), 1 << 16, 1 << 20) : chunkbytes
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
    b = Tables.bind(scan, names)

    # -- translate the bound scan into kernel pushdown --------------------------
    # types: seed every column the scan pins (duplicate selections of one source
    # column must agree)
    seedtypes = Dict{Int, Type}()
    for c in b.columns
        c.type === nothing && continue
        newT = c.type === Missing ? Missing : Base.nonmissingtype(c.type)
        T = get(seedtypes, c.index, nothing)
        T === nothing || T === newT ||
            throw(ArgumentError("column $(names[c.index]) selected twice with conflicting types $T and $newT"))
        seedtypes[c.index] = newT
    end
    outidx = [c.index for c in b.columns]
    predidx = b.filtercols

    if b.filter === nothing
        # single phase: selection + exact row bounds straight into the driver
        lim = b.limit === nothing ? nothing : b.offset + b.limit
        t = K.parse(buf; index=bi, header=names, select=unique(outidx),
                    types=seedtypes, limit=lim, phasekw...)
        b.offset > 0 && (t = _droprows(t, b.offset))
        t = _project(t, b, names)
        return (_finishproblems(t, maxproblems, on_error, headerlog, t), Tables.EMPTYSCAN)
    end

    # -- two-phase masked parse --------------------------------------------------
    # phase 1: only the predicate's columns, every row
    predsel = sort(unique(predidx))
    t1 = K.parse(buf; index=bi, header=names, select=predsel,
                 types=Dict(i => T for (i, T) in seedtypes if i in predsel), phasekw...)
    mask = Tables.filtermask(b.filter, t1)
    # bake offset/limit into the mask: phase 2 parses EXACTLY the output rows
    _cliprows!(mask, b.offset, b.limit)

    # phase 2: the remaining selected columns, only where the mask is true
    rest = sort(unique(i for i in outidx if !(i in predsel)))
    t2 = isempty(rest) ? nothing :
         K.parse(buf; index=bi, header=names, select=rest,
                 types=Dict(i => T for (i, T) in seedtypes if i in rest),
                 rowmask=mask, reportstructural=false, phasekw...)

    # combine: phase-1 columns compact under the same mask; phase-2 columns are
    # already compact
    kept = findall(mask)
    cols = Vector{AbstractVector}(undef, length(b.columns))
    outnames = Vector{Symbol}(undef, length(b.columns))
    for (o, c) in enumerate(b.columns)
        outnames[o] = c.name
        if c.index in predsel
            src = Tables.getcolumn(Tables.columns(t1), names[c.index])
            col = src[kept]
            # a type pinned on a predicate column was already seeded in phase 1
            cols[o] = col
        else
            cols[o] = Tables.getcolumn(Tables.columns(t2), names[c.index])
        end
    end
    t = K.ParsedTable(outnames, cols, length(kept), K.Problem[], 0)
    return (_finishproblems(t, maxproblems, on_error, headerlog, t1, t2), Tables.EMPTYSCAN)
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

function _droprows(t::K.ParsedTable, offset::Int)
    n = max(t.nrows - offset, 0)
    cols = AbstractVector[c[(offset + 1):t.nrows] for c in K.columns(t)]
    return K.ParsedTable(K.names(t), cols, n, K.problems(t), t.droppedproblems)
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
