# Tables.Scan can select columns, change names and types, filter rows, and set
# row bounds. CSV applies the complete request. A filter read has two value
# passes. The first pass reads only the filter columns. The second pass reads
# result columns only for rows that passed the filter. Both passes use the same
# structural index.

using Tables

# A filter can use a column number from the source. The predicate pass reads
# only the columns used by the filter. This view keeps the full source names
# and maps a requested source position to the parsed predicate column.
struct PredicateColumns
    parsed::ParsedTable
    names::Vector{Symbol}
    sources::Vector{Int}
end

Tables.istable(::Type{PredicateColumns}) = true
Tables.columnaccess(::Type{PredicateColumns}) = true
Tables.columns(t::PredicateColumns) = t
Tables.columnnames(t::PredicateColumns) = t.names
Tables.rowcount(t::PredicateColumns) = t.parsed.nrows

function Tables.getcolumn(t::PredicateColumns, j::Int)
    q = searchsortedfirst(t.sources, j)
    q <= length(t.sources) && t.sources[q] == j ||
        throw(ArgumentError("filter column $j was not parsed"))
    return columns(t.parsed)[q]
end

function settlecolumns(names::Vector{Symbol}, opts::ValueOpts,
                       b::Tables.BoundScan;
                       colopts::Union{Nothing, Vector{ValueOpts}}=nothing)
    colopts === nothing || length(colopts) == length(names) ||
        throw(ArgumentError("colopts length $(length(colopts)) != " *
                            "$(length(names)) columns"))
    columns = [ColumnDecision() for _ in names]
    settled = Dict{Int, Type}()
    for c in b.columns
        c.type === nothing && continue
        requested = c.type === Missing ? Missing : Base.nonmissingtype(c.type)
        priorrequest = get(settled, c.index, nothing)
        priorrequest === nothing || priorrequest === requested ||
            throw(ArgumentError("column $(names[c.index]) selected twice with " *
                                "conflicting types $priorrequest and $requested"))
        settled[c.index] = requested
        decision = _columndecision(c.type)
        prior = columns[c.index]
        columns[c.index] = ColumnDecision(
            decision.parsetype,
            decision.resulttype,
            prior.declaredmissing || decision.declaredmissing,
        )
    end
    sources = sort!(unique(Int[c.index for c in b.columns]))
    predicate = sort!(unique(Int[b.filtercols...]))
    return ColumnPlan(columns, sources, Int[], predicate, opts, colopts)
end

function _executescanplan(p::Prepared, scan::Tables.Scan;
                          headerlog::ProblemLog, maxproblems::Int,
                          on_error::Symbol, source::String="")
    bi = p.bi
    inputnames = p.names
    phasecap = max(maxproblems, on_error === :error ? 1 : 0)
    b = Tables.resolve(scan, inputnames)
    plan = settlecolumns(inputnames, p.opts, b; colopts=_preparedcolopts(p))
    requests = _requestedstrings(plan, [c.index for c in b.columns])

    # The prepared row window (`footerskip`) ends before the scan's own offset
    # and limit apply. The kernel takes one mask, so the window is baked in.
    total = sum(nrows, bi.chunks; init=0)
    window = p.limit === nothing ? total : min(total, p.limit)
    if b.filter === nothing
        # Apply row bounds before a requested type conversion.
        bounded = b.offset > 0 || b.limit !== nothing || window < total
        mask = bounded ? fill(true, total) : nothing
        if mask !== nothing
            fill!(view(mask, (window + 1):total), false)
            _cliprows!(mask, b.offset, b.limit)
        end
        t = _parseprepared(p, plan; limit=nothing, rowmask=mask, maxproblems=phasecap)
        sourcerows = mask === nothing ? nothing : findall(mask)
        t = _narrowphase(t, plan, bi, phasecap; sourcerows)
        t = _project(t, b, inputnames)
        return _finishproblems(t, maxproblems, on_error, headerlog, source, t), requests
    end

    # First, read only the columns used by the filter, inside the window.
    predcolumns = [ColumnDecision() for _ in inputnames]
    predplan = ColumnPlan(predcolumns, plan.predicate, Int[], Int[],
                          plan.opts, plan.colopts)
    t1 = _parseprepared(p, predplan; limit=p.limit, maxproblems=phasecap)
    mask = Tables.filtermask(b, PredicateColumns(t1, inputnames, plan.predicate))
    length(mask) == window ||
        throw(ArgumentError("filter mask has $(length(mask)) entries for $window rows"))
    window < total && append!(mask, Iterators.repeated(false, total - window))
    _cliprows!(mask, b.offset, b.limit)

    # Then, read result columns only for rows that passed the filter. A result
    # column used by the filter is read again because its requested type applies
    # only to the result.
    t2 = _parseprepared(p, plan; limit=nothing, rowmask=mask,
                        reportstructural=false, maxproblems=phasecap)
    kept = findall(mask)
    t2 = _narrowphase(t2, plan, bi, phasecap; sourcerows=kept)
    t = _project(t2, b, inputnames)
    return _finishproblems(t, maxproblems, on_error, headerlog, source, t1, t2), requests
end

function _narrowphase(t::ParsedTable, plan::ColumnPlan, bi::BufferIndex,
                      maxproblems::Int; sourcerows=nothing)
    narrowed, _ = _narrowtypes(t, plan, bi.chunks, maxproblems;
                               sourcerows)
    return narrowed
end

# Skip the first `offset` true values. Keep no more than `limit` after them.
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

# Put columns in the requested order and apply requested names.
function _project(t::ParsedTable, b::Tables.BoundScan, inputnames::Vector{Symbol})
    lookup = Dict(nm => i for (i, nm) in enumerate(names(t)))
    cols = AbstractVector[columns(t)[lookup[inputnames[c.index]]] for c in b.columns]
    return ParsedTable([c.name for c in b.columns], cols, t.nrows,
                       problems(t), t.droppedproblems)
end

function _finishproblems(t::ParsedTable, maxproblems::Int, on_error::Symbol,
                         headerlog::ProblemLog, source::String, phases...)
    items = copy(headerlog.items)
    dropped = headerlog.dropped
    for phase in phases
        phase === nothing && continue
        append!(items, problems(phase))
        dropped += phase.droppedproblems
    end
    sort!(items; by=problemkey)
    firstproblem = isempty(items) ? nothing : first(items)
    nkeep = min(length(items), maxproblems)
    dropped += length(items) - nkeep
    resize!(items, nkeep)
    out = ParsedTable(names(t), columns(t), t.nrows, items, dropped)
    _reportproblems(out, on_error, firstproblem, source)
    return out
end
