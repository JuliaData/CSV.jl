# Shared Tables.jl adapters and batch/row primitives over the parsing core. The
# public API adds source preparation and compatibility options in `api.jl`.

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

"""
    _readtable(source; kw...) -> ParsedTable

Internal table adapter used by the package tests. Any Tables.jl sink consumes the result:
`DataFrame(_readtable(src))`, `Tables.columntable(...)`, etc.
"""
_readtable(source; kw...) = parse(source; kw...)

# ---------------------------------------------------------------------------
# 2. Batched reading — CSV.Chunks
# ---------------------------------------------------------------------------
# Iterate the file as a sequence of ParsedTables, one per nonempty data chunk,
# holding only one batch's values in memory at a time (the index itself is whole-file
# here).
#
# One whole-file prepass computes both value types and missingness, so every
# batch reports the same column types. (Earlier CSV.Chunks
# re-infers per chunk, which is why its docs warn that chunk schemas may differ.)
# Sampling cannot prove this property: one unsampled value or one batch-local
# missing would otherwise change that batch's schema.

struct Batches
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    names::Vector{Symbol}
    sourceindices::Vector{Int}
    nsourcecols::Int
    seedtypes::Vector{Type}
    userprovided::Vector{Bool}
    allowmissing::Vector{Bool}
    opts::ValueOpts
    colopts::Union{Nothing, Vector{ValueOpts}}
    d::Dialect
    maxproblems::Int
    unclosedquote::Bool
end

"""Construct stable-schema batches. Row iteration calls `_batches` without a schema prepass."""
batches(source; kw...) = _batches(source, false; kw...)

function _batches(source, rowmode::Bool;
                 header::Union{Bool, AbstractVector}=true,
                 types=nothing,
                 chunkbytes::Int=1 << 20,
                 maxproblems::Int=10_000,
                 stripwhitespace::Bool=false,
                 dateformat=nothing, decimal::Char='.',
                 truestrings=nothing, falsestrings=nothing,
                 dialectkw...)
    buf = source isa Vector{UInt8} ? source :
          source isa AbstractString ? Vector{UInt8}(codeunits(source)) : Base.read(source)
    d = Dialect(; dialectkw...)
    opts = makevalueopts(d; dateformat, decimal, truestrings, falsestrings, stripwhitespace)
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1
    bi = index(buf, d; datastart, chunkbytes)
    chunks = bi.chunks
    # Header names use the same extraction and malformed-span preservation rules
    # as the eager driver. Batch-local problem collection begins during iteration.
    names = if header === true && !isempty(chunks)
        ci = chunks[1]
        hrow = ci.firstdatarow
        nms = [headername(buf, ci, hrow, j, opts, d.cq) for j in 1:nfields(ci, hrow)]
        ci.firstdatarow = hrow + 1
        nms
    elseif header isa AbstractVector
        Symbol.(header)
    else
        isempty(chunks) ? Symbol[] :
            [Symbol("Column", j) for j in 1:nfields(chunks[1], chunks[1].firstdatarow)]
    end
    names = makeunique!(names)
    filter!(ci -> nrows(ci) > 0, chunks)
    ncols = length(names)
    seed = resolvetypes(types, names, ncols)
    userprovided = [T !== nothing for T in seed]
    if any(isnothing, seed)
        # A stable schema requires every value to participate. The eager driver
        # can sample and promote later; a partition already yielded cannot.
        total = sum(nrows, chunks; init=0)
        inferred = sampletypes(buf, chunks, ncols, opts; nsample=max(1, total))
        for j in 1:ncols
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    seedtypes = Type[T for T in seed]
    allowmissing = rowmode ? fill(false, ncols) :
                             schemamissing(buf, chunks, seedtypes, opts)
    return Batches(buf, chunks, names, collect(1:ncols), ncols, seedtypes,
                   userprovided, allowmissing, opts, nothing, d, maxproblems,
                   bi.unclosedquote)
end

function schemamissing(buf, chunks, types, opts;
                       sourceindices=collect(eachindex(types)), colopts=nothing)
    missing = fill(false, length(types))
    for ci in chunks, lr in ci.firstdatarow:totalrows(ci), q in eachindex(types)
        missing[q] && continue
        j = sourceindices[q]
        sp = fieldspan(ci, lr, j)
        if sp === nothing || sp[2] == 0 || types[q] === Missing
            missing[q] = true
            continue
        end
        pos, len = sp
        opt = colopts === nothing ? opts : @inbounds(colopts[j])
        cpos, clen, esc, st = cellcontent(buf, pos, len, opt)
        if st != CELL_VALUE
            missing[q] = true
        elseif types[q] !== String
            missing[q] = clen == 0 || esc ||
                         !parsevalue(types[q], buf, cpos, cpos + clen - 1, opt)[2]
        end
    end
    return missing
end

function headername(buf, ci, hrow, j, opts, cq::UInt8)
    pos, len = fieldspan(ci, hrow, j)::Tuple{Int, Int}
    len == 0 && return Symbol("Column", j)
    cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
    (st != CELL_VALUE || clen == 0) && return st == CELL_BADQUOTE ?
        Symbol(String(buf[pos:pos + len - 1])) : Symbol("Column", j)
    return Symbol(esc ? _unescape(buf, Int64(cpos), Int32(clen), opts.e, cq) :
                  GC.@preserve(buf, unsafe_string(pointer(buf, cpos), clen)))
end

Base.length(b::Batches) = length(b.chunks)
Base.eltype(::Type{Batches}) = ParsedTable
Tables.partitions(b::Batches) = b

function Base.iterate(b::Batches, i::Int=1)
    i > length(b.chunks) && return nothing
    return parsebatch(b, b.chunks[i]), i + 1
end

# One batch = the kernel's column primitives applied to a single chunk. This is
# the "building blocks compose" claim made concrete: no new value parsing code
# and no special streaming mode.
function parsebatch(b::Batches, ci::ChunkIndex)
    n = nrows(ci)
    ncols = length(b.names)
    log = ProblemLog(b.maxproblems)
    rowbase = chunkrowbase(b.chunks, ci)

    for lr in ci.firstdatarow:totalrows(ci)
        nf = nfields(ci, lr)
        grow = rowbase + (lr - ci.firstdatarow) + 1
        if nf < b.nsourcecols
            sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
            pushproblem!(log, grow, 0, sp[1], :short_row,
                           "expected $(b.nsourcecols) fields, found $nf (remaining columns set to missing)")
        elseif nf > b.nsourcecols
            sp = fieldspan(ci, lr, b.nsourcecols + 1)::Tuple{Int, Int}
            pushproblem!(log, grow, 0, sp[1], :long_row,
                           "expected $(b.nsourcecols) fields, found $nf (extra fields ignored)")
        end
    end
    b.unclosedquote && ci === last(b.chunks) &&
        pushproblem!(log, 0, 0, length(b.buf), :unclosed_quote,
                       "input ended inside a quoted field")

    cols = Vector{AbstractVector}(undef, ncols)
    for q in 1:ncols
        j = b.sourceindices[q]
        T = b.seedtypes[q]
        opts = b.colopts === nothing ? b.opts : @inbounds(b.colopts[j])
        col = allocatecolumn(T, n, b.buf, opts.e, b.d.cq)
        conflict = T === Missing ?
            parsecolchunk_missing(b.buf, ci, j, rowbase, opts, b.userprovided[q], log) :
            parsecolchunk!(col, b.buf, ci, j, 0, opts, b.userprovided[q], log, rowbase)
        conflict == 0 || error("internal error: batch schema prepass disagreed with value parsing")
        cols[q] = finalizecolumn(T, col, n, b.allowmissing[q])
    end
    sortproblems!(log)
    return ParsedTable(b.names, cols, n, log.items, log.dropped)
end

# ---------------------------------------------------------------------------
# 3. Row streaming — CSV.Rows
# ---------------------------------------------------------------------------
# No column storage at all: iterating yields lightweight row views over the
# index; each cell materializes only when accessed. Typed access reuses the
# exact same span + kernel machinery as the columnar path —
# CSV.Rows' whole parallel implementation (its own Context mode, its own
# @unrollcolumns dispatch table) reduces to this.

struct _IndexedRows
    buf::Vector{UInt8}
    chunks::Vector{ChunkIndex}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    opts::ValueOpts
    colopts::Union{Nothing, Vector{ValueOpts}}
    d::Dialect
end

Tables.istable(::Type{_IndexedRows}) = true
Tables.rowaccess(::Type{_IndexedRows}) = true
Tables.rows(r::_IndexedRows) = r
Tables.schema(r::_IndexedRows) =
    Tables.Schema(r.names, fill(Union{CompactString, Missing}, length(r.names)))

function _indexedrows(source; header::Union{Bool, AbstractVector}=true,
              stripwhitespace::Bool=false,
              dateformat=nothing, decimal::Char='.',
              truestrings=nothing, falsestrings=nothing,
              chunkbytes::Int=1 << 22, dialectkw...)
    # types=String skips type inference; rowmode also skips the missingness
    # prepass. Row streaming needs the structural index and names, never a schema.
    b = _batches(source, true; header, chunkbytes, stripwhitespace, types=String,
                 dateformat, decimal, truestrings, falsestrings, dialectkw...)
    return _IndexedRows(b.buf, b.chunks, b.names,
                        Dict(nm => j for (j, nm) in enumerate(b.names)),
                        b.opts, nothing, b.d)
end

@inline _rowopts(r::_IndexedRows, j::Int) =
    r.colopts === nothing ? r.opts : @inbounds(r.colopts[j])

# CompactString's view word has an Int32 offset. Row access normally retains
# the source buffer with no copy. For a long cell beyond that absolute offset,
# copy only the cell into a private backing buffer. The returned value owns the
# buffer, so separate rows and concurrent consumers do not share mutable state.
@inline function _rowcompact(buf::Vector{UInt8}, pos::Int, len::Int,
                             viewoffsetlimit::Int=Int(typemax(Int32)))
    len <= COMPACTSTRING_INLINE &&
        return CompactString(inline_payload(buf, pos, len), EMPTY_BYTES)
    pos - 1 <= viewoffsetlimit &&
        return CompactString(view_payload(buf, pos, len, 0, pos - 1), buf)
    bytes = Vector{UInt8}(undef, len)
    copyto!(bytes, 1, buf, pos, len)
    return CompactString(view_payload(bytes, 1, len, 0, 0), bytes)
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

# Untyped access: Union{CompactString, Missing} — a lazy view. Short cells
# are inline payloads, long cells view the input buffer (zero-copy); an
# escaped cell unescapes into a small owned buffer that the CompactString
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
        inl === nothing || return CompactString(inl, EMPTY_BYTES)
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
