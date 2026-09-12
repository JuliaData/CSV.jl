# Tables.Scan can select columns, change names and types, filter rows, and set
# row bounds. CSV applies the complete request. A filter read has two value
# passes. The first pass reads only the filter columns, one chunk at a time,
# and keeps only the row mask. The second pass reads result columns only for
# rows that passed the filter, and skips a column whose values the first pass
# already produced. Both passes use the same structural index.

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
    # and limit apply. The driver takes one mask, so the window is baked in.
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

    # The predicate pass streams one chunk at a time; see `_streampredicate`.
    # A chunk whose cell contradicts the sampled type of its column falls
    # back to one parse of the whole window, which promotes the column the
    # usual way.
    streamed = _streampredicate(p, plan, b, window, total, phasecap)
    if streamed === nothing
        predcolumns = [ColumnDecision() for _ in inputnames]
        predplan = ColumnPlan(predcolumns, plan.predicate, Int[], Int[],
                              plan.opts, plan.colopts)
        t1 = _parseprepared(p, predplan; limit=p.limit, maxproblems=phasecap)
        mask = Vector{Bool}(Tables.filtermask(b, PredicateColumns(t1, inputnames, plan.predicate)))
        length(mask) == window ||
            throw(ArgumentError("filter mask has $(length(mask)) entries for $window rows"))
        window < total && append!(mask, Iterators.repeated(false, total - window))
        predphase = t1
        slices = Dict{Int, Vector{AbstractVector}}()
        seeds = Dict{Int, Type}()
    else
        mask, predphase, slices, seeds = streamed
    end
    _cliprows!(mask, b.offset, b.limit)
    kept = findall(mask)
    nkept = length(kept)

    # A result column that the predicate pass parsed with the type the result
    # needs keeps those values; the others parse only the rows that passed.
    reused = _reusepredicate(p, plan, kept, slices, seeds, b.offset)
    sources = Int[j for j in plan.sources if !haskey(reused, j)]
    if isempty(sources)
        t2 = ParsedTable(Symbol[], AbstractVector[], nkept, Problem[], 0)
    else
        plan2 = ColumnPlan(plan.columns, sources, plan.positions, plan.predicate,
                           plan.opts, plan.colopts)
        t2 = _parseprepared(p, plan2; limit=nothing, rowmask=mask,
                            reportstructural=false, maxproblems=phasecap)
        t2 = _narrowphase(t2, plan2, bi, phasecap; sourcerows=kept)
    end
    lookup = Dict(nm => i for (i, nm) in enumerate(names(t2)))
    cols = AbstractVector[haskey(reused, j) ? reused[j] : columns(t2)[lookup[inputnames[j]]]
                          for j in plan.sources]
    tall = ParsedTable(inputnames[plan.sources], cols, nkept, problems(t2), t2.droppedproblems)
    t = _project(tall, b, inputnames)
    return _finishproblems(t, maxproblems, on_error, headerlog, source, predphase, t2), requests
end

# Stream the predicate pass. The predicate columns parse one chunk at a time
# with the types the window sample settled, the filter runs on each chunk, and
# only the Bool mask stays in memory, plus the kept values of any result column
# that can reuse this parse. Chunks parse in groups of `tasklimit`, in file
# order. Returns `nothing` when a cell contradicts its sampled type or when the
# filter uses no column; the caller then parses the window at once.
function _streampredicate(p::Prepared, plan::ColumnPlan, b::Tables.BoundScan,
                          window::Int, total::Int, cap::Int)
    bi = p.bi
    chunks = bi.chunks
    settings = p.settings
    inputnames = p.names
    ncols = p.ncols
    predicate = plan.predicate
    (isempty(predicate) || window == 0) && return nothing
    tasklimit = settings.parallel ?
                min(something(settings.ntasks, Threads.nthreads()), Threads.nthreads()) : 1
    tm = settings.typemap
    selected = fill(false, ncols)
    for j in predicate
        selected[j] = true
    end
    sawmissing = fill(false, ncols)
    ns = settings.nsample === nothing ? clamp(window >> 6, 8, 128) : settings.nsample
    probechunks = ChunkIndex[ci for ci in chunks if nrows(ci) > 0]
    inferred = sampletypes(p.buf, probechunks, ncols, plan.opts; nsample=max(ns, 1), selected,
                           sawmissing, colopts=plan.colopts, maxrows=window)
    seedtypes = Type[_maptype(tm, inferred[j]) for j in predicate]
    predcolumns = [ColumnDecision() for _ in inputnames]
    predplan = ColumnPlan(predcolumns, predicate, Int[], Int[], plan.opts, plan.colopts)
    batches = Batches(p.buf, chunks, inputnames[predicate], predplan, seedtypes,
                      fill(true, length(predicate)), p.d, cap,
                      bi.unclosedquote && window == total, 1)
    seeds = Dict{Int, Type}(j => seedtypes[q] for (q, j) in enumerate(predicate))
    # result columns that may reuse this parse: same source, same parse type
    slices = Dict{Int, Vector{AbstractVector}}()
    for c in b.columns
        j = c.index
        haskey(seeds, j) || continue
        d = plan.columns[j]
        (d.resulttype === nothing || _requestedstring(d) !== nothing) || continue
        (d.parsetype === nothing || d.parsetype === seeds[j]) || continue
        slices[j] = AbstractVector[]
    end
    mask = Vector{Bool}(undef, total)
    items = Problem[]
    dropped = 0
    pos = 0
    k = 1
    rowbases = cumsum([0; Int[nrows(ci) for ci in chunks]])
    nchunks = _limitchunks(chunks, rowbases, window)
    while k <= nchunks
        group = k:min(k + tasklimit - 1, nchunks)
        tables = Vector{Union{Nothing, ParsedTable}}(nothing, length(group))
        _taskforeach(eachindex(group), tasklimit) do g
            ck = group[g]
            n = min(nrows(chunks[ck]), window - rowbases[ck])
            tables[g] = tryparsebatch(batches, chunks[ck], n, rowbases[ck])
        end
        for g in eachindex(group)
            t = tables[g]
            t === nothing && return nothing
            n = t.nrows
            m = Vector{Bool}(Tables.filtermask(b, PredicateColumns(t, inputnames, predicate)))
            length(m) == n ||
                throw(ArgumentError("filter mask has $(length(m)) entries for $n rows"))
            copyto!(mask, pos + 1, m, 1, n)
            for (j, pieces) in slices
                push!(pieces, _keptslice(columns(t)[searchsortedfirst(predicate, j)], m))
            end
            append!(items, problems(t))
            dropped += t.droppedproblems
            pos += n
        end
        k = last(group) + 1
    end
    fill!(view(mask, (window + 1):total), false)
    return mask, ParsedTable(Symbol[], AbstractVector[], 0, items, dropped), slices, seeds
end

# The rows of one chunk's column that passed the filter.
_keptslice(col::AbstractVector, m::Vector{Bool}) = col[m]
_keptslice(col::DataStringVector, m::Vector{Bool}) =
    _stringvector(eltype(col), col.payloads[m], col.buffers)

# Decide which result columns keep the predicate pass's values. A requested
# type that equals the parse type always can. An inferred result type can
# only when the kept rows infer the same type the window sample gave; the
# kept rows alone decide an inferred result type.
function _reusepredicate(p::Prepared, plan::ColumnPlan, kept::Vector{Int},
                         slices::Dict{Int, Vector{AbstractVector}}, seeds::Dict{Int, Type},
                         offset::Int)
    reused = Dict{Int, AbstractVector}()
    (isempty(slices) || isempty(kept)) && return reused
    settings = p.settings
    inferredcands = Int[j for j in keys(slices) if plan.columns[j].parsetype === nothing]
    if !isempty(inferredcands)
        ncols = p.ncols
        selected = fill(false, ncols)
        for j in inferredcands
            selected[j] = true
        end
        chunks = p.bi.chunks
        rowbases0 = cumsum([0; Int[nrows(ci) for ci in chunks[1:max(length(chunks) - 1, 0)]]])
        ns = settings.nsample === nothing ? clamp(length(kept) >> 6, 8, 128) : settings.nsample
        sawmissing = Bool[plan.columns[j].declaredmissing for j in 1:ncols]
        inferred = sampletypesrows(p.buf, chunks, rowbases0, kept, ncols, plan.opts, selected;
                                   nsample=max(ns, 1), sawmissing, colopts=plan.colopts)
        filter!(j -> _maptype(settings.typemap, inferred[j]) === seeds[j], inferredcands)
    end
    nkept = length(kept)
    for (j, pieces) in slices
        (plan.columns[j].parsetype !== nothing || j in inferredcands) || continue
        reused[j] = _assemblereused(pieces, seeds[j], offset, nkept,
                                    plan.columns[j].declaredmissing)
    end
    return reused
end

# Join the kept slices, drop the rows that `offset` and `limit` removed, and
# narrow the element type when no kept value is missing.
function _assemblereused(pieces::Vector{AbstractVector}, ::Type{T}, offset::Int, nkept::Int,
                         declaredmissing::Bool) where {T}
    range = (offset + 1):(offset + nkept)
    T === Missing && return fill(missing, nkept)
    if T === String
        counts = Int[length(c) for c in pieces]
        chained = _chaindatastrings(pieces, counts, sum(counts))
        payloads = chained.payloads[range]
        allpresent = !declaredmissing && !any(pl -> payloadlen(pl) < 0, payloads)
        return _stringvector(allpresent ? DataString : Union{DataString, Missing},
                             payloads, chained.buffers)
    end
    v = reduce(vcat, pieces; init=Union{Missing, T}[])
    r = v[range]
    return declaredmissing || any(ismissing, r) ? r : Vector{T}(r)
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
    sort!(items; lt=problemless)
    firstproblem = isempty(items) ? nothing : first(items)
    nkeep = min(length(items), maxproblems)
    dropped += length(items) - nkeep
    resize!(items, nkeep)
    out = ParsedTable(names(t), columns(t), t.nrows, items, dropped)
    _reportproblems(out, on_error, firstproblem, source)
    return out
end
