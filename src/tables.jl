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

function schemamissing(buf, chunks, types, plan::ColumnPlan)
    missing = Bool[plan.columns[j].declaredmissing for j in plan.sources]
    for ci in chunks, lr in ci.firstdatarow:totalrows(ci), q in eachindex(types)
        missing[q] && continue
        j = plan.sources[q]
        sp = fieldspan(ci, lr, j)
        if sp === nothing || sp[2] == 0 || types[q] === Missing
            missing[q] = true
            continue
        end
        pos, len = sp
        opt = columnopts(plan, j)
        cpos, clen, esc, st = cellcontent(buf, pos, len, opt)
        if st != CELL_VALUE
            missing[q] = true
        elseif types[q] !== String
            checktype = something(accessparsetype(plan.columns[j]), types[q])
            missing[q] = clen == 0 || esc ||
                         !parsevalue(checktype, buf, cpos, cpos + clen - 1, opt)[2]
        end
    end
    return missing
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
        grow = rowbase + (lr - ci.firstdatarow) + 1
        if nf < nsourcecols
            sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
            pushproblem!(log, grow, 0, sp[1], :short_row,
                           "expected $nsourcecols fields, found $nf (remaining columns set to missing)")
        elseif nf > nsourcecols
            sp = fieldspan(ci, lr, nsourcecols + 1)::Tuple{Int, Int}
            pushproblem!(log, grow, 0, sp[1], :long_row,
                           "expected $nsourcecols fields, found $nf (extra fields ignored)")
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
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    opts::ValueOpts
    colopts::Union{Nothing, Vector{ValueOpts}}
    d::Dialect
end

_IndexedRows(buf, chunks, names, lookup, plan::ColumnPlan, d) =
    _IndexedRows(buf, chunks, names, lookup, plan.opts, plan.colopts, d)

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
