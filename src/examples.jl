# Shared Tables.jl adapters and batch/row primitives over CSVKernel. The public
# API adds source preparation and compatibility options in `api.jl`.

module KernelExamples

using ..CSVKernel
using Tables, Dates
const K = CSVKernel

# ---------------------------------------------------------------------------
# 1. Eager reading — CSV.read
# ---------------------------------------------------------------------------
# Keep the Tables.jl methods here so the structural kernel remains independent
# of Tables.jl.

Tables.istable(::Type{K.ParsedTable}) = true
Tables.columnaccess(::Type{K.ParsedTable}) = true
Tables.columns(t::K.ParsedTable) = t
Tables.columnnames(t::K.ParsedTable) = K.names(t)
Tables.getcolumn(t::K.ParsedTable, i::Int) = K.columns(t)[i]
Tables.getcolumn(t::K.ParsedTable, nm::Symbol) = t[nm]
Tables.rowcount(t::K.ParsedTable) = t.nrows
Tables.schema(t::K.ParsedTable) =
    Tables.Schema(K.names(t), Type[eltype(c) for c in K.columns(t)])

"""
    KernelExamples.read(source; kw...) -> ParsedTable (a Tables.jl table)

Internal table adapter used by the package tests. Any Tables.jl sink consumes the result:
`DataFrame(KernelExamples.read(src))`, `Tables.columntable(...)`, etc.
"""
read(source; kw...) = K.parse(source; kw...)

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
    chunks::Vector{K.ChunkIndex}
    names::Vector{Symbol}
    sourceindices::Vector{Int}
    nsourcecols::Int
    seedtypes::Vector{Type}
    userprovided::Vector{Bool}
    allowmissing::Vector{Bool}
    opts::K.ValueOpts
    colopts::Union{Nothing, Vector{K.ValueOpts}}
    d::K.Dialect
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
    d = K.Dialect(; dialectkw...)
    opts = K.makevalueopts(d; dateformat, decimal, truestrings, falsestrings, stripwhitespace)
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1
    bi = K.index(buf, d; datastart, chunkbytes)
    chunks = bi.chunks
    # Header names use the same extraction and malformed-span preservation rules
    # as the eager driver. Batch-local problem collection begins during iteration.
    names = if header === true && !isempty(chunks)
        ci = chunks[1]
        hrow = ci.firstdatarow
        nms = [headername(buf, ci, hrow, j, opts, d.cq) for j in 1:K.nfields(ci, hrow)]
        ci.firstdatarow = hrow + 1
        nms
    elseif header isa AbstractVector
        Symbol.(header)
    else
        isempty(chunks) ? Symbol[] :
            [Symbol("Column", j) for j in 1:K.nfields(chunks[1], chunks[1].firstdatarow)]
    end
    names = K.makeunique!(names)
    filter!(ci -> K.nrows(ci) > 0, chunks)
    ncols = length(names)
    seed = K.resolvetypes(types, names, ncols)
    userprovided = [T !== nothing for T in seed]
    if any(isnothing, seed)
        # A stable schema requires every value to participate. The eager driver
        # can sample and promote later; a partition already yielded cannot.
        total = sum(K.nrows, chunks; init=0)
        inferred = K.sampletypes(buf, chunks, ncols, opts; nsample=max(1, total))
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
    for ci in chunks, lr in ci.firstdatarow:K.totalrows(ci), q in eachindex(types)
        missing[q] && continue
        j = sourceindices[q]
        sp = K.fieldspan(ci, lr, j)
        if sp === nothing || sp[2] == 0 || types[q] === Missing
            missing[q] = true
            continue
        end
        pos, len = sp
        opt = colopts === nothing ? opts : @inbounds(colopts[j])
        cpos, clen, esc, st = K.cellcontent(buf, pos, len, opt)
        if st != K.CELL_VALUE
            missing[q] = true
        elseif types[q] !== String
            missing[q] = clen == 0 || esc ||
                         !K.parsevalue(types[q], buf, cpos, cpos + clen - 1, opt)[2]
        end
    end
    return missing
end

function headername(buf, ci, hrow, j, opts, cq::UInt8)
    pos, len = K.fieldspan(ci, hrow, j)::Tuple{Int, Int}
    len == 0 && return Symbol("Column", j)
    cpos, clen, esc, st = K.cellcontent(buf, pos, len, opts)
    (st != K.CELL_VALUE || clen == 0) && return st == K.CELL_BADQUOTE ?
        Symbol(String(buf[pos:pos + len - 1])) : Symbol("Column", j)
    return Symbol(esc ? K._unescape(buf, Int64(cpos), Int32(clen), opts.e, cq) :
                  GC.@preserve(buf, unsafe_string(pointer(buf, cpos), clen)))
end

Base.length(b::Batches) = length(b.chunks)
Base.eltype(::Type{Batches}) = K.ParsedTable
Tables.partitions(b::Batches) = b

function Base.iterate(b::Batches, i::Int=1)
    i > length(b.chunks) && return nothing
    return parsebatch(b, b.chunks[i]), i + 1
end

# One batch = the kernel's column primitives applied to a single chunk. This is
# the "building blocks compose" claim made concrete: no new value parsing code
# and no special streaming mode.
function parsebatch(b::Batches, ci::K.ChunkIndex)
    n = K.nrows(ci)
    ncols = length(b.names)
    log = K.ProblemLog(b.maxproblems)
    rowbase = K.chunkrowbase(b.chunks, ci)

    for lr in ci.firstdatarow:K.totalrows(ci)
        nf = K.nfields(ci, lr)
        grow = rowbase + (lr - ci.firstdatarow) + 1
        if nf < b.nsourcecols
            sp = K.fieldspan(ci, lr, 1)::Tuple{Int, Int}
            K.pushproblem!(log, grow, 0, sp[1], :short_row,
                           "expected $(b.nsourcecols) fields, found $nf (remaining columns set to missing)")
        elseif nf > b.nsourcecols
            sp = K.fieldspan(ci, lr, b.nsourcecols + 1)::Tuple{Int, Int}
            K.pushproblem!(log, grow, 0, sp[1], :long_row,
                           "expected $(b.nsourcecols) fields, found $nf (extra fields ignored)")
        end
    end
    b.unclosedquote && ci === last(b.chunks) &&
        K.pushproblem!(log, 0, 0, length(b.buf), :unclosed_quote,
                       "input ended inside a quoted field")

    cols = Vector{AbstractVector}(undef, ncols)
    for q in 1:ncols
        j = b.sourceindices[q]
        T = b.seedtypes[q]
        opts = b.colopts === nothing ? b.opts : @inbounds(b.colopts[j])
        col = K.allocatecolumn(T, n, b.buf, opts.e, b.d.cq)
        conflict = T === Missing ?
            K.parsecolchunk_missing(b.buf, ci, j, rowbase, opts, b.userprovided[q], log) :
            K.parsecolchunk!(col, b.buf, ci, j, 0, opts, b.userprovided[q], log, rowbase)
        conflict == 0 || error("internal error: batch schema prepass disagreed with value parsing")
        cols[q] = K.finalizecolumn(T, col, n, b.allowmissing[q])
    end
    K.sortproblems!(log)
    return K.ParsedTable(b.names, cols, n, log.items, log.dropped)
end

# ---------------------------------------------------------------------------
# 3. Row streaming — CSV.Rows
# ---------------------------------------------------------------------------
# No column storage at all: iterating yields lightweight row views over the
# index; each cell materializes only when accessed. Typed access reuses the
# exact same span + kernel machinery as the columnar path —
# CSV.Rows' whole parallel implementation (its own Context mode, its own
# @unrollcolumns dispatch table) reduces to this.

struct Rows
    buf::Vector{UInt8}
    chunks::Vector{K.ChunkIndex}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    opts::K.ValueOpts
    colopts::Union{Nothing, Vector{K.ValueOpts}}
    d::K.Dialect
end

Tables.istable(::Type{Rows}) = true
Tables.rowaccess(::Type{Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) =
    Tables.Schema(r.names, fill(Union{K.CompactString, Missing}, length(r.names)))

function rows(source; header::Union{Bool, AbstractVector}=true,
              stripwhitespace::Bool=false,
              dateformat=nothing, decimal::Char='.',
              truestrings=nothing, falsestrings=nothing,
              chunkbytes::Int=1 << 22, dialectkw...)
    # types=String skips type inference; rowmode also skips the missingness
    # prepass. Row streaming needs the structural index and names, never a schema.
    b = _batches(source, true; header, chunkbytes, stripwhitespace, types=String,
                 dateformat, decimal, truestrings, falsestrings, dialectkw...)
    return Rows(b.buf, b.chunks, b.names, Dict(nm => j for (j, nm) in enumerate(b.names)),
                b.opts, nothing, b.d)
end

@inline _rowopts(r::Rows, j::Int) = r.colopts === nothing ? r.opts : @inbounds(r.colopts[j])

# CompactString's view word has an Int32 offset. Row access normally retains
# the source buffer with no copy. For a long cell beyond that absolute offset,
# copy only the cell into a private backing buffer. The returned value owns the
# buffer, so separate rows and concurrent consumers do not share mutable state.
@inline function _rowcompact(buf::Vector{UInt8}, pos::Int, len::Int,
                             viewoffsetlimit::Int=Int(typemax(Int32)))
    len <= K.COMPACTSTRING_INLINE &&
        return K.CompactString(K.inline_payload(buf, pos, len), K.EMPTY_BYTES)
    pos - 1 <= viewoffsetlimit &&
        return K.CompactString(K.view_payload(buf, pos, len, 0, pos - 1), buf)
    bytes = Vector{UInt8}(undef, len)
    copyto!(bytes, 1, buf, pos, len)
    return K.CompactString(K.view_payload(bytes, 1, len, 0, 0), bytes)
end

struct RowView <: Tables.AbstractRow
    r::Rows
    ci::K.ChunkIndex
    localrow::Int
    rownumber::Int
end

Base.eltype(::Type{Rows}) = RowView
Base.IteratorSize(::Type{Rows}) = Base.SizeUnknown()

function Base.iterate(r::Rows, state=(1, nothing, 1))
    chunkidx, lr, rownum = state
    while chunkidx <= length(r.chunks)
        ci = r.chunks[chunkidx]
        localrow = lr === nothing ? ci.firstdatarow : lr
        if localrow <= K.totalrows(ci)
            return RowView(r, ci, localrow, rownum), (chunkidx, localrow + 1, rownum + 1)
        end
        chunkidx += 1
        lr = nothing
    end
    return nothing
end

Base.length(row::RowView) = length(getfield(row, :r).names)
Base.propertynames(row::RowView) = getfield(row, :r).names
Tables.columnnames(row::RowView) = getfield(row, :r).names
Tables.getcolumn(row::RowView, j::Int) = row[j]
Tables.getcolumn(row::RowView, nm::Symbol) = row[nm]

# Untyped access: Union{CompactString, Missing} — a lazy view. Short cells
# are inline payloads, long cells view the input buffer (zero-copy); an
# escaped cell unescapes into a small owned buffer that the CompactString
# then views. No String allocation on the plain path.
function Base.getindex(row::RowView, j::Int)
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = K.fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    buf = r.buf
    opts = _rowopts(r, j)
    cpos, clen, esc, st = K.cellcontent(buf, pos, len, opts)
    st == K.CELL_VALUE || return missing
    if esc
        inl = K._unescape_inline(buf, cpos, clen, opts.e, r.d.cq)
        inl === nothing || return K.CompactString(inl, K.EMPTY_BYTES)
        own = UInt8[]
        n = K._unescape_append!(own, buf, cpos, clen, opts.e, r.d.cq)
        return _rowcompact(own, 1, n)
    end
    return _rowcompact(buf, cpos, clen)
end
Base.getindex(row::RowView, nm::Symbol) = row[getfield(row, :r).lookup[nm]]
function Base.getproperty(row::RowView, nm::Symbol)
    r = getfield(row, :r)
    return haskey(r.lookup, nm) ? row[nm] : getfield(row, nm)
end

# Typed access on demand — the CSV.Rows `parse(T, row, i)` pattern.
function typedvalue(::Type{String}, row::RowView, j::Int)
    x = row[j]
    return x === missing ? missing : String(x)
end
function typedvalue(::Type{T}, row::RowView, j::Int) where {T}
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = K.fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    opts = _rowopts(r, j)
    cpos, clen, esc, st = K.cellcontent(r.buf, pos, len, opts)
    (st == K.CELL_VALUE && clen > 0 && !esc) || return missing
    v, ok = K.parsevalue(T, r.buf, cpos, cpos + clen - 1, opts)
    return ok ? v : missing
end
typedvalue(::Type{T}, row::RowView, nm::Symbol) where {T} =
    typedvalue(T, row, getfield(row, :r).lookup[nm])

end # module KernelExamples
