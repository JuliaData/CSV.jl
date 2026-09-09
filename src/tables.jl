# Tables.jl adapters and the batch and row readers used by the public API.

using Tables

# ---------------------------------------------------------------------------
# 1. Eager reading — CSV.read
# ---------------------------------------------------------------------------
# Keep the Tables.jl methods here so the structural kernel remains independent
# of Tables.jl.

Tables.istable(::Type{ParsedTable}) = true
Tables.columnaccess(::Type{ParsedTable}) = true
Tables.columns(t::ParsedTable) = t
Tables.columnnames(t::ParsedTable) = names(t)
Tables.getcolumn(t::ParsedTable, i::Int) = columns(t)[i]
Tables.getcolumn(t::ParsedTable, nm::Symbol) = t[nm]
Tables.rowcount(t::ParsedTable) = t.nrows
Tables.schema(t::ParsedTable) =
    Tables.Schema(names(t), Type[eltype(c) for c in columns(t)])

# ---------------------------------------------------------------------------
# 2. Batched reading — CSV.Chunks
# ---------------------------------------------------------------------------
# Each item holds values from one nonempty data chunk. The structural index
# still covers the full file. Before iteration starts, a full-file pass finds
# each column type and whether the column can contain `missing`. This gives each
# batch the same schema. A sample cannot give this guarantee.

struct Batches
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    names::Vector{Symbol}
    plan::ColumnPlan
    seedtypes::Vector{Type}
    allowmissing::Vector{Bool}
    d::Dialect
    maxproblems::Int
    unclosedquote::Bool
end

# Settle the batch schema in place: `types[q]` is promoted until every cell of
# the window parses, and the returned vector says whether column q can hold
# `missing` (an empty or sentinel cell, a short row, or — for a requested
# type — an invalid cell that the parse will report and leave missing).
# Columns are independent, so they validate in parallel.
function settlebatchschema!(types::Vector{Type}, buf, chunks, plan::ColumnPlan,
                            maxlens::Union{Nothing, Vector{Int}}=nothing;
                            parallel::Bool=false, tasklimit::Int=1)
    allowmissing = Bool[plan.columns[j].declaredmissing for j in plan.sources]
    settle = q -> begin
        j = plan.sources[q]
        d = plan.columns[j]
        requested = d.parsetype !== nothing
        # a requested narrow type is checked at its own range; a requested
        # string type is checked as text
        checktype = requested && _requestedstring(d) === nothing ?
                    something(accessparsetype(d), types[q]) : types[q]
        T, sawmissing, maxlen = _settlecolumn(checktype, buf, chunks, j, columnopts(plan, j),
                                              requested, allowmissing[q])
        allowmissing[q] = sawmissing
        maxlens === nothing || (maxlens[q] = maxlen)
        # the batch parses with the native (wide) kernel; narrowing follows
        requested || (types[q] = T)
    end
    if parallel && tasklimit > 1 && length(types) > 1
        _taskforeach(settle, eachindex(types), tasklimit)
    else
        foreach(settle, eachindex(types))
    end
    return allowmissing
end

# Validate one column from its current type, re-entering with the promoted
# type from the conflicting row. Range widening from nanoseconds to
# microseconds can reject earlier fractions, so that transition starts over.
# Each entry is monomorphic in `T`. The longest value
# (in output bytes) settles an auto-width string request for the whole window.
function _settlecolumn(::Type{T0}, buf, chunks, j::Int, opts::ValueOpts,
                       requested::Bool, sawmissing::Bool) where {T0}
    T = T0
    k, lr, maxlen = 1, 0, 0
    while true
        T2, sawmissing, k, lr, maxlen = _settlecolumnfrom(T, buf, chunks, j, opts, requested,
                                                          sawmissing, k, lr, maxlen)
        T2 === T && return T, sawmissing, maxlen
        if T === _TS_NS && T2 === _TS_US
            k, lr = 1, 0
        end
        T = T2
    end
end

# The bytes a text cell has after parsing: malformed quoting keeps the raw
# field, escapes collapse, and other cells keep their content span.
@inline function _valuelength(buf::Vector{UInt8}, pos::Int, len::Int, cpos::Int, clen::Int,
                              esc::Bool, st::UInt8, opts::ValueOpts)
    st == CELL_BADQUOTE && return len
    esc || return clen
    return _unescapedlength(buf, cpos, clen, opts.e, opts.cq)
end

function _unescapedlength(buf::Vector{UInt8}, pos::Int, len::Int, e::UInt8, cq::UInt8)
    n = 0
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        i += (buf[i] == e && i < last && (e != cq || buf[i + 1] == cq)) ? 2 : 1
        n += 1
    end
    return n
end

function _settlecolumnfrom(::Type{T}, buf::Vector{UInt8}, chunks, j::Int, opts::ValueOpts,
                           requested::Bool, sawmissing::Bool, k::Int, lr::Int,
                           maxlen::Int) where {T}
    scratch = _scratchfor(opts)
    @inbounds while k <= length(chunks)
        ci = chunks[k]
        lr = max(lr, ci.firstdatarow)
        while lr <= totalrows(ci)
            sp = fieldspan(ci, lr, j)
            if sp === nothing || sp[2] == 0
                sawmissing = true
                lr += 1
                continue
            end
            pos, len = sp
            cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
            st == CELL_MISSING ||
                (maxlen = max(maxlen, _valuelength(buf, pos, len, cpos, clen, esc, st, opts)))
            if st == CELL_MISSING
                sawmissing = true
            elseif T === String
                # every present cell is a string; only the missing flag matters
            elseif T === Missing
                # a present value under an all-missing seed: promote by detection
                requested && (sawmissing = true)
                requested || return (promote_kernel(Missing, detecttype(buf, pos, len, opts)),
                                     sawmissing, k, lr, maxlen)
            else
                ok = false
                if st == CELL_VALUE && clen > 0 && !esc
                    ti, tj = _trimblanks(buf, cpos, cpos + clen - 1)
                    if ti > tj   # blanks only: parse the original (invalid) span
                        ti, tj = cpos, cpos + clen - 1
                    end
                    ok = parsevalue(T, buf, ti, tj, opts, scratch)[2]
                end
                if !ok
                    # an invalid cell under a requested type parses to missing
                    # (with a problem); under an inferred type it promotes
                    requested && (sawmissing = true)
                    if !requested
                        detected = promote_kernel(T, detecttype(buf, pos, len, opts))
                        return (detected === T ? String : detected, sawmissing, k, lr, maxlen)
                    end
                end
            end
            lr += 1
        end
        k += 1
        lr = 0
    end
    return T, sawmissing, k, lr, maxlen
end

Base.length(b::Batches) = length(b.chunks)
Base.eltype(::Type{Batches}) = ParsedTable
Tables.partitions(b::Batches) = b

function Base.iterate(b::Batches, i::Int=1)
    i > length(b.chunks) && return nothing
    return parsebatch(b, b.chunks[i]), i + 1
end

# Parse one indexed chunk with the types and options settled for this request.
function parsebatch(b::Batches, ci::ChunkIndex)
    n = nrows(ci)
    ncols = length(b.names)
    log = ProblemLog(b.maxproblems)
    rowbase = chunkrowbase(b.chunks, ci)
    nsourcecols = length(b.plan.columns)

    for lr in ci.firstdatarow:totalrows(ci)
        nf = nfields(ci, lr)
        if nf != nsourcecols
            grow = rowbase + (lr - ci.firstdatarow) + 1
            sp = fieldspan(ci, lr, nf < nsourcecols ? 1 : nsourcecols + 1)::Tuple{Int, Int}
            _emptyrow(b.buf, ci, nf, sp) || pushrowproblem!(log, grow, sp[1], nsourcecols, nf)
        end
    end
    b.unclosedquote && ci === last(b.chunks) &&
        pushproblem!(log, 0, 0, length(b.buf), :unclosed_quote,
                       "input ended inside a quoted field")

    cols = Vector{AbstractVector}(undef, ncols)
    for q in 1:ncols
        j = b.plan.sources[q]
        T = b.seedtypes[q]
        opts = columnopts(b.plan, j)
        userprovided = b.plan.columns[j].parsetype !== nothing
        col = allocatecolumn(T, n, b.buf, opts.e, b.d.cq)
        conflict = T === Missing ?
            parsecolchunk_missing(b.buf, ci, j, rowbase, opts, userprovided, log) :
            parsecolchunk!(col, b.buf, ci, j, 0, opts, userprovided, log, rowbase)
        conflict == 0 || error("internal error: batch schema prepass disagreed with value parsing")
        cols[q] = finalizecolumn(T, col, n, b.allowmissing[q])
    end
    sortproblems!(log)
    return ParsedTable(b.names, cols, n, log.items, log.dropped)
end

# ---------------------------------------------------------------------------
# 3. Row streaming — CSV.Rows
# ---------------------------------------------------------------------------
# Iteration returns small row views over the index. A cell is parsed only when
# the caller reads it.

struct _IndexedRows
    name::String
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    opts::ValueOpts
    colopts::Union{Nothing, Vector{ValueOpts}}
    d::Dialect
end

_IndexedRows(buf, chunks, names, lookup, plan::ColumnPlan, d, source::String="") =
    _IndexedRows(source, buf, chunks, names, lookup, plan.opts, plan.colopts, d)

Tables.istable(::Type{_IndexedRows}) = true
Tables.rowaccess(::Type{_IndexedRows}) = true
Tables.rows(r::_IndexedRows) = r
Tables.schema(r::_IndexedRows) =
    Tables.Schema(r.names, fill(Union{DataString, Missing}, length(r.names)))

@inline _rowopts(r::_IndexedRows, j::Int) =
    r.colopts === nothing ? r.opts : @inbounds(r.colopts[j])

# DataString's view word has an Int32 offset. Row access normally retains
# the source buffer with no copy. For a long cell beyond that absolute offset,
# copy only the cell into a private backing buffer. The returned value owns the
# buffer, so separate rows and concurrent consumers do not share mutable state.
@inline function _rowcompact(buf::Vector{UInt8}, pos::Int, len::Int,
                             viewoffsetlimit::Int=Int(typemax(Int32)))
    len <= COMPACTSTRING_INLINE &&
        return DataString(inline_payload(buf, pos, len), EMPTY_BYTES)
    pos - 1 <= viewoffsetlimit &&
        return DataString(view_payload(buf, pos, len, 0, pos - 1), buf)
    bytes = Vector{UInt8}(undef, len)
    copyto!(bytes, 1, buf, pos, len)
    return DataString(view_payload(bytes, 1, len, 0, 0), bytes)
end

struct _IndexedRow <: Tables.AbstractRow
    r::_IndexedRows
    ci::ChunkIndex
    localrow::Int
    rownumber::Int
end

Base.eltype(::Type{_IndexedRows}) = _IndexedRow
Base.IteratorSize(::Type{_IndexedRows}) = Base.SizeUnknown()

function Base.iterate(r::_IndexedRows, state=(1, nothing, 1))
    chunkidx, lr, rownum = state
    while chunkidx <= length(r.chunks)
        ci = r.chunks[chunkidx]
        localrow = lr === nothing ? ci.firstdatarow : lr
        if localrow <= totalrows(ci)
            return _IndexedRow(r, ci, localrow, rownum),
                   (chunkidx, localrow + 1, rownum + 1)
        end
        chunkidx += 1
        lr = nothing
    end
    return nothing
end

Base.length(row::_IndexedRow) = length(getfield(row, :r).names)
Base.propertynames(row::_IndexedRow) = getfield(row, :r).names
Tables.columnnames(row::_IndexedRow) = getfield(row, :r).names
Tables.getcolumn(row::_IndexedRow, j::Int) = row[j]
Tables.getcolumn(row::_IndexedRow, nm::Symbol) = row[nm]

# Untyped access: Union{DataString, Missing} — a lazy view. Short cells
# are inline payloads, long cells view the input buffer (zero-copy); an
# escaped cell unescapes into a small owned buffer that the DataString
# then views. No String allocation on the plain path.
function Base.getindex(row::_IndexedRow, j::Int)
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    buf = r.buf
    opts = _rowopts(r, j)
    cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
    st == CELL_VALUE || return missing
    if esc
        inl = _unescape_inline(buf, cpos, clen, opts.e, r.d.cq)
        inl === nothing || return DataString(inl, EMPTY_BYTES)
        own = UInt8[]
        n = _unescape_append!(own, buf, cpos, clen, opts.e, r.d.cq)
        return _rowcompact(own, 1, n)
    end
    return _rowcompact(buf, cpos, clen)
end
Base.getindex(row::_IndexedRow, nm::Symbol) = row[getfield(row, :r).lookup[nm]]
function Base.getproperty(row::_IndexedRow, nm::Symbol)
    r = getfield(row, :r)
    return haskey(r.lookup, nm) ? row[nm] : getfield(row, nm)
end

# Typed access on demand — the CSV.Rows `parse(T, row, i)` pattern.
function _typedvalue(::Type{String}, row::_IndexedRow, j::Int)
    x = row[j]
    return x === missing ? missing : String(x)
end
function _typedvalue(::Type{T}, row::_IndexedRow, j::Int) where {T}
    if _stringsink(T)   # a requested string type: the view, converted per cell
        x = row[j]
        return x === missing ? missing : _rowstring(T, x)
    end
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    opts = _rowopts(r, j)
    cpos, clen, esc, st = cellcontent(r.buf, pos, len, opts)
    (st == CELL_VALUE && clen > 0 && !esc) || return missing
    v, ok = parsevalue(T, r.buf, cpos, cpos + clen - 1, opts)
    return ok ? v : missing
end
_typedvalue(::Type{T}, row::_IndexedRow, nm::Symbol) where {T} =
    _typedvalue(T, row, getfield(row, :r).lookup[nm])
