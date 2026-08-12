# How CSV.jl's user-facing surfaces become thin layers over the kernel.
#
# Three real (small, working) reimplementations:
#
#   KernelExamples.read     ≈ CSV.read   — eager, parallel, Tables.jl-compatible
#   KernelExamples.batches  ≈ CSV.Chunks — memory-bounded batch iterator with a
#                                          STABLE schema across batches (inference
#                                          samples the whole file once, up front)
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
using Tables, Parsers, Dates
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
# Iterate the file as a sequence of ParsedTables, one per index chunk, holding
# only one batch's values in memory at a time (the index itself is whole-file
# here; a production StreamSource would index chunk-by-chunk too — same code,
# different L0).
#
# The upgrade over CSV.Chunks: the schema is inferred ONCE from a stratified
# whole-file sample, so every batch reports the same column types. (CSV.Chunks
# re-infers per chunk, which is why its docs warn that chunk schemas may
# differ.) A batch value that still defies the sampled type promotes that
# batch's column upward — rare by construction, and surfaced in the batch's
# problems rather than silently.

struct Batches
    buf::Vector{UInt8}
    chunks::Vector{K.ChunkIndex}
    names::Vector{Symbol}
    seedtypes::Vector{Type}
    opts::Parsers.Options
    d::K.Dialect
    maxproblems::Int
end

function batches(source;
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
    opts = K.makeoptions(d; dateformat, decimal, truestrings, falsestrings, stripwhitespace)
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1
    bi = K.index(buf, d; datastart, chunkbytes)
    chunks = bi.chunks
    # header + names, exactly as the driver does it
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
    ncols = length(names)
    seed = K.resolvetypes(types, names, ncols)
    if any(isnothing, seed)
        inferred = K.sampletypes(buf, chunks, ncols, opts)
        for j in 1:ncols
            seed[j] === nothing && (seed[j] = inferred[j])
        end
    end
    return Batches(buf, chunks, names, Type[T for T in seed], opts, d, maxproblems)
end

function headername(buf, ci, hrow, j, opts, cq::UInt8)
    pos, len = K.fieldspan(ci, hrow, j)::Tuple{Int, Int}
    len == 0 && return Symbol("Column", j)
    res = Parsers.xparse(String, buf, pos, pos + len - 1, opts)
    pl = res.val
    return Symbol(Parsers.escapedstring(res.code) ?
                  K._unescape(buf, Int64(pl.pos), Int32(pl.len), opts.e, cq) :
                  unsafe_string(pointer(buf, pl.pos), pl.len))
end

Base.length(b::Batches) = length(b.chunks)
Base.eltype(::Type{Batches}) = K.ParsedTable
Tables.partitions(b::Batches) = b

function Base.iterate(b::Batches, i::Int=1)
    i > length(b.chunks) && return nothing
    return parsebatch(b, b.chunks[i]), i + 1
end

# One batch = the kernel's column primitives applied to a single chunk. This is
# the "building blocks compose" claim made concrete: ~30 lines, no new parsing
# code, no special streaming mode.
function parsebatch(b::Batches, ci::K.ChunkIndex)
    n = K.nrows(ci)
    ncols = length(b.names)
    log = K.ProblemLog(b.maxproblems)
    cols = Vector{AbstractVector}(undef, ncols)
    for j in 1:ncols
        T = b.seedtypes[j]
        while true
            col = K.allocatecolumn(T, n, b.buf, b.opts.e, b.d.cq)
            conflict = T === Missing ?
                K.parsecolchunk_missing(b.buf, ci, j, b.opts) :
                K.parsecolchunk!(col, b.buf, ci, j, 0, b.opts, false, log)
            if conflict == 0
                cols[j] = K.finalizecolumn(T, col, n)
                break
            end
            sp = K.fieldspan(ci, conflict, j)::Tuple{Int, Int}
            newT = K.promote_kernel(T, K.detecttype(b.buf, sp[1], sp[2], b.opts))
            T = newT === T ? String : newT
        end
    end
    return K.ParsedTable(b.names, cols, n, log.items, log.dropped)
end

# ---------------------------------------------------------------------------
# 3. Row streaming — CSV.Rows
# ---------------------------------------------------------------------------
# No column storage at all: iterating yields lightweight row views over the
# index; each cell materializes only when accessed. `Parsers.parse`-style typed
# access reuses the exact same span + options machinery as the columnar path —
# CSV.Rows' whole parallel implementation (its own Context mode, its own
# @unrollcolumns dispatch table) reduces to this.

struct Rows
    buf::Vector{UInt8}
    chunks::Vector{K.ChunkIndex}
    names::Vector{Symbol}
    lookup::Dict{Symbol, Int}
    opts::Parsers.Options
    d::K.Dialect
end

function rows(source; header::Union{Bool, AbstractVector}=true,
              stripwhitespace::Bool=false,
              dateformat=nothing, decimal::Char='.',
              truestrings=nothing, falsestrings=nothing,
              chunkbytes::Int=1 << 22, dialectkw...)
    b = batches(source; header, chunkbytes, stripwhitespace,
                dateformat, decimal, truestrings, falsestrings, dialectkw...)
    return Rows(b.buf, b.chunks, b.names, Dict(nm => j for (j, nm) in enumerate(b.names)),
                b.opts, b.d)
end

struct RowView
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

Base.length(row::RowView) = length(row.r.names)
Base.propertynames(row::RowView) = row.r.names

# Untyped access: Union{String, Missing}, materialized (and unescaped) on demand.
function Base.getindex(row::RowView, j::Int)
    sp = K.fieldspan(row.ci, row.localrow, j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    buf = row.r.buf
    res = Parsers.xparse(String, buf, pos, pos + len - 1, row.r.opts)
    Parsers.sentinel(res.code) && return missing
    pl = res.val
    return Parsers.escapedstring(res.code) ?
        K._unescape(buf, Int64(pl.pos), Int32(pl.len), row.r.opts.e, row.r.d.cq) :
        unsafe_string(pointer(buf, pl.pos), pl.len)
end
Base.getindex(row::RowView, nm::Symbol) = row[row.r.lookup[nm]]
Base.getproperty(row::RowView, nm::Symbol) =
    nm in (:r, :ci, :localrow, :rownumber) ? getfield(row, nm) : row[nm]

# Typed access on demand — the CSV.Rows `Parsers.parse(T, row, i)` pattern.
function typedvalue(::Type{T}, row::RowView, j::Int) where {T}
    sp = K.fieldspan(row.ci, row.localrow, j)
    sp === nothing && return missing
    pos, len = sp
    len == 0 && return missing
    res = Parsers.xparse(T, row.r.buf, pos, pos + len - 1, row.r.opts)
    Parsers.ok(res.code) && res.tlen == len || return missing
    return Parsers.sentinel(res.code) ? missing : res.val
end
typedvalue(::Type{T}, row::RowView, nm::Symbol) where {T} = typedvalue(T, row, row.r.lookup[nm])

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
