# How CSV.jl's user-facing surfaces become thin layers over the kernel.
#
# Three real (small, working) reimplementations:
#
#   KernelExamples.read     ≈ CSV.read   — eager, parallel, Tables.jl-compatible
#   KernelExamples.batches  ≈ CSV.Chunks — memory-bounded batch iterator with a
#                                          STABLE schema across batches (a schema
#                                          prepass visits every indexed field)
#   KernelExamples.rows     ≈ CSV.Rows   — zero-column-allocation row streaming
#                                          with lazy string views + on-demand
#                                          typed access
#
# The point being proven: all three share one index + one set of column
# primitives. There is no second dispatch table for row iteration (CSV.jl's
# `@unrollcolumns`), no per-chunk re-inference (CSV.Chunks), and no separate
# streaming Context mode (CSV.Rows) — those existed because the old design
# coupled "find structure", "pick types", and "materialize" into one pass.
#
# Run the demo:  julia --project=kernel kernel/examples.jl

isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))

module KernelExamples

using ..CSVKernel
using Tables, Dates
const K = CSVKernel

# ---------------------------------------------------------------------------
# 1. Eager reading — CSV.read
# ---------------------------------------------------------------------------
# The kernel driver already IS the eager reader; the API layer only adds
# Tables.jl integration (in the real package these definitions live with the
# type; they're here to keep core.jl dependency-free).

Tables.istable(::Type{K.ParsedTable}) = true
Tables.columnaccess(::Type{K.ParsedTable}) = true
Tables.columns(t::K.ParsedTable) = t
Tables.columnnames(t::K.ParsedTable) = K.names(t)
Tables.getcolumn(t::K.ParsedTable, i::Int) = K.columns(t)[i]
Tables.getcolumn(t::K.ParsedTable, nm::Symbol) = t[nm]
Tables.schema(t::K.ParsedTable) =
    Tables.Schema(K.names(t), Type[eltype(c) for c in K.columns(t)])

"""
    KernelExamples.read(source; kw...) -> ParsedTable (a Tables.jl table)

`CSV.read` analog. Any Tables.jl sink consumes the result:
`DataFrame(KernelExamples.read(src))`, `Tables.columntable(...)`, etc.
"""
read(source; kw...) = K.parse(source; kw...)

# ---------------------------------------------------------------------------
# 2. Batched reading — CSV.Chunks
# ---------------------------------------------------------------------------
# Iterate the file as a sequence of ParsedTables, one per nonempty data chunk,
# holding only one batch's values in memory at a time (the index itself is whole-file
# here; a production StreamSource would index chunk-by-chunk too — same code,
# different L0).
#
# The upgrade over CSV.Chunks: one whole-file prepass computes both value types
# and missingness, so every batch reports the same column types. (CSV.Chunks
# re-infers per chunk, which is why its docs warn that chunk schemas may differ.)
# Sampling cannot prove this property: one unsampled value or one batch-local
# missing would otherwise change that batch's schema.

struct Batches
    buf::Vector{UInt8}
    chunks::Vector{K.ChunkIndex}
    names::Vector{Symbol}
    seedtypes::Vector{Type}
    userprovided::Vector{Bool}
    allowmissing::Vector{Bool}
    opts::K.ValueOpts
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
    return Batches(buf, chunks, names, seedtypes, userprovided, allowmissing,
                   opts, d, maxproblems, bi.unclosedquote)
end

function schemamissing(buf, chunks, types, opts)
    missing = fill(false, length(types))
    for ci in chunks, lr in ci.firstdatarow:K.totalrows(ci), j in eachindex(types)
        missing[j] && continue
        sp = K.fieldspan(ci, lr, j)
        if sp === nothing || sp[2] == 0 || types[j] === Missing
            missing[j] = true
            continue
        end
        pos, len = sp
        cpos, clen, esc, st = K.cellcontent(buf, pos, len, opts)
        if st != K.CELL_VALUE
            missing[j] = true
        elseif types[j] !== String
            missing[j] = clen == 0 || esc ||
                         !K.parsevalue(types[j], buf, cpos, cpos + clen - 1, opts)[2]
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
        if nf < ncols
            sp = K.fieldspan(ci, lr, 1)::Tuple{Int, Int}
            K.pushproblem!(log, grow, 0, sp[1], :short_row,
                           "expected $ncols fields, found $nf (remaining columns set to missing)")
        elseif nf > ncols
            sp = K.fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
            K.pushproblem!(log, grow, 0, sp[1], :long_row,
                           "expected $ncols fields, found $nf (extra fields ignored)")
        end
    end
    b.unclosedquote && ci === last(b.chunks) &&
        K.pushproblem!(log, 0, 0, length(b.buf), :unclosed_quote,
                       "input ended inside a quoted field")

    cols = Vector{AbstractVector}(undef, ncols)
    for j in 1:ncols
        T = b.seedtypes[j]
        col = K.allocatecolumn(T, n, b.buf, b.opts.e, b.d.cq)
        conflict = T === Missing ?
            K.parsecolchunk_missing(b.buf, ci, j, rowbase, b.opts, b.userprovided[j], log) :
            K.parsecolchunk!(col, b.buf, ci, j, 0, b.opts, b.userprovided[j], log, rowbase)
        conflict == 0 || error("internal error: batch schema prepass disagreed with value parsing")
        cols[j] = K.finalizecolumn(T, col, n, b.allowmissing[j])
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
    d::K.Dialect
end

Tables.istable(::Type{Rows}) = true
Tables.rowaccess(::Type{Rows}) = true
Tables.rows(r::Rows) = r
Tables.schema(r::Rows) =
    Tables.Schema(r.names, fill(Union{String, Missing}, length(r.names)))

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
                b.opts, b.d)
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

# Untyped access: Union{String, Missing}, materialized (and unescaped) on demand.
function Base.getindex(row::RowView, j::Int)
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = K.fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    buf = r.buf
    cpos, clen, esc, st = K.cellcontent(buf, pos, len, r.opts)
    st == K.CELL_VALUE || return missing
    return esc ? K._unescape(buf, Int64(cpos), Int32(clen), r.opts.e, r.d.cq) :
        GC.@preserve(buf, unsafe_string(pointer(buf, cpos), clen))
end
Base.getindex(row::RowView, nm::Symbol) = row[getfield(row, :r).lookup[nm]]
function Base.getproperty(row::RowView, nm::Symbol)
    r = getfield(row, :r)
    return haskey(r.lookup, nm) ? row[nm] : getfield(row, nm)
end

# Typed access on demand — the CSV.Rows `parse(T, row, i)` pattern.
typedvalue(::Type{String}, row::RowView, j::Int) = row[j]
function typedvalue(::Type{T}, row::RowView, j::Int) where {T}
    r = getfield(row, :r)
    @boundscheck checkbounds(r.names, j)
    sp = K.fieldspan(getfield(row, :ci), getfield(row, :localrow), j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    cpos, clen, esc, st = K.cellcontent(r.buf, pos, len, r.opts)
    (st == K.CELL_VALUE && clen > 0 && !esc) || return missing
    v, ok = K.parsevalue(T, r.buf, cpos, cpos + clen - 1, r.opts)
    return ok ? v : missing
end
typedvalue(::Type{T}, row::RowView, nm::Symbol) where {T} =
    typedvalue(T, row, getfield(row, :r).lookup[nm])

# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

function demo()
    csv = "name,score,when,notes\n" *
          "alice,10,2024-01-02,\"likes\nmultiline, quoted text\"\n" *
          "bob,11.5,2024-01-03,\n" *
          "carol,12,2024-01-04,\"she said \"\"hi\"\"\"\n"
    println("== eager (CSV.read analog) ==")
    t = read(csv)
    show(stdout, t); println()
    println("as a NamedTuple of columns: ")
    show(stdout, Tables.columntable(t)); println("\n")

    println("== batches (CSV.Chunks analog, chunkbytes=32) ==")
    for (k, batch) in enumerate(batches(csv; chunkbytes=32))
        println("batch $k: $(batch.nrows) rows, score::$(eltype(batch[:score]))")
    end
    println()

    println("== row streaming (CSV.Rows analog) ==")
    for row in rows(csv)
        println("row $(row.rownumber): name=$(row.name) score=$(typedvalue(Float64, row, :score)) notes=$(repr(row[:notes]))")
    end
end

end # module KernelExamples

abspath(PROGRAM_FILE) == (@__FILE__) && KernelExamples.demo()
