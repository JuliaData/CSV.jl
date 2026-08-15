isdefined(Main, :CSVKernel) || include(joinpath(@__DIR__, "core.jl"))

# The kernel-era writer: CSV.write's surface with the 1.0 modernizations.
#
#   quotestyle   :minimal (default) — quote only when the value contains the
#                delimiter, the quote, CR/LF, or leading/trailing whitespace;
#                :all — every string cell quoted; :none — never quote (values
#                containing structural bytes are an ArgumentError: silent
#                corruption is not an option)
#   floatformat  a printf-style format string ("%.3f") applied to every
#                AbstractFloat cell (issue #492); default is Julia's shortest
#                round-trip (Ryu) printing
#   compress     :auto (by .gz extension) | :gzip | :none
#   partition    write a Vector of sinks in parallel, one table partition each
#
# The engine renders row-major from Tables.columns, in parallel: rows split
# into contiguous blocks, each block renders into its own buffer on its own
# task, blocks concatenate in order into one output write. Bytes out are
# identical at any thread count.

module KernelWrite

using Tables, Dates, Printf, CodecZlib
using ..CSVKernel
const K = CSVKernel

const WRITE_QUOTESTYLES = (:minimal, :all, :none)

struct WriteOpts
    delim::UInt8
    oq::UInt8
    cq::UInt8
    e::UInt8
    newline::Vector{UInt8}
    missingstring::Vector{UInt8}
    quotestyle::Symbol
    floatfmt::Union{Nothing, Printf.Format}
    dateformat::Union{Nothing, DateFormat}
    decimal::UInt8
    bom::Bool
end

function _writeopts(; delim::Union{Char, String}=',', quotechar::Char='"',
                    openquotechar::Union{Nothing, Char}=nothing,
                    closequotechar::Union{Nothing, Char}=nothing,
                    escapechar::Union{Nothing, Char}=nothing,
                    newline::Union{Char, String}='\n',
                    missingstring::AbstractString="",
                    quotestyle::Symbol=:minimal,
                    floatformat::Union{Nothing, AbstractString}=nothing,
                    dateformat=nothing,
                    decimal::Char='.',
                    bom::Bool=false)
    delim isa String && sizeof(delim) != 1 &&
        throw(ArgumentError("write delim must be a single byte (got $(repr(delim)))"))
    quotestyle in WRITE_QUOTESTYLES ||
        throw(ArgumentError("quotestyle must be one of $(WRITE_QUOTESTYLES) (got $quotestyle)"))
    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    e = something(escapechar, Char(cq)) % UInt8
    d = delim isa Char ? delim % UInt8 : codeunit(delim, 1)
    df = dateformat === nothing ? nothing :
         dateformat isa DateFormat ? dateformat : DateFormat(string(dateformat))
    ff = floatformat === nothing ? nothing : Printf.Format(String(floatformat))
    return WriteOpts(d, oq, cq, e, Vector{UInt8}(codeunits(string(newline))),
                     Vector{UInt8}(codeunits(String(missingstring))),
                     quotestyle, ff, df, decimal % UInt8, bom)
end

# --- cell rendering ---------------------------------------------------------

_needsquote(o::WriteOpts, b::UInt8) =
    b == o.delim || b == o.oq || b == o.cq || b == UInt8('\n') || b == UInt8('\r')

function _writestring(io::IO, s::AbstractString, o::WriteOpts)
    bytes = codeunits(s)
    if o.quotestyle === :none
        for b in bytes
            _needsquote(o, b) &&
                throw(ArgumentError("quotestyle=:none cannot write a value containing " *
                                    "a structural byte: $(repr(s))"))
        end
        return Base.write(io, bytes)
    end
    quote_it = o.quotestyle === :all
    if !quote_it
        for b in bytes
            if _needsquote(o, b)
                quote_it = true
                break
            end
        end
        # leading/trailing whitespace survives a round-trip only when quoted
        if !quote_it && !isempty(bytes)
            (bytes[1] == UInt8(' ') || bytes[end] == UInt8(' ')) && (quote_it = true)
        end
    end
    quote_it || return Base.write(io, bytes)
    n = Base.write(io, o.oq)
    for b in bytes
        (b == o.cq || (o.e != o.cq && b == o.e)) && (n += Base.write(io, o.e))
        n += Base.write(io, b)
    end
    return n + Base.write(io, o.cq)
end

function _writecell(io::IO, x, o::WriteOpts)
    if x === missing
        Base.write(io, o.missingstring)
    elseif x isa AbstractString
        _writestring(io, x, o)
    elseif x isa AbstractFloat
        if o.floatfmt !== nothing
            Printf.format(io, o.floatfmt, x)
        elseif o.decimal != UInt8('.')
            _writestring(io, replace(string(x), '.' => Char(o.decimal)), o)
        else
            print(io, x)
        end
    elseif x isa Dates.TimeType
        o.dateformat === nothing ? print(io, x) :
            _writestring(io, Dates.format(x, o.dateformat), o)
    elseif x isa Bool
        print(io, x)
    elseif x isa Integer || x isa Number
        print(io, x)
    else
        _writestring(io, string(x), o)
    end
    return
end

# --- row-block rendering (the parallel unit) --------------------------------

function _renderblock(cols, lo::Int, hi::Int, o::WriteOpts)
    io = IOBuffer()
    ncols = length(cols)
    @inbounds for r in lo:hi
        for (j, col) in enumerate(cols)
            _writecell(io, col[r], o)
            j < ncols && Base.write(io, o.delim)
        end
        Base.write(io, o.newline)
    end
    return take!(io)
end

function _renderheader(names, o::WriteOpts)
    io = IOBuffer()
    o.bom && Base.write(io, 0xef, 0xbb, 0xbf)
    for (j, nm) in enumerate(names)
        _writestring(io, String(nm), o)
        j < length(names) && Base.write(io, o.delim)
    end
    Base.write(io, o.newline)
    return take!(io)
end

# --- the front door ---------------------------------------------------------

"""
    KernelWrite.write(sink, table; kw...) -> sink

Write any Tables.jl table as CSV. `sink` is a file path, an `IO`, or (with
`partition=true`) a vector of paths/IOs receiving one table partition each,
written in parallel. Rendering is parallel and byte-deterministic.
"""
function write(sink, table; append::Bool=false, writeheader::Union{Nothing, Bool}=nothing,
               header::Union{Nothing, Vector}=nothing, compress::Symbol=:auto,
               partition::Bool=false,
               ntasks::Int=Threads.nthreads(), kw...)
    o = _writeopts(; kw...)
    if partition
        parts = Tables.partitions(table)
        sinks = sink isa AbstractVector ? sink :
                throw(ArgumentError("partition=true needs a Vector of sinks"))
        partsv = collect(parts)
        length(partsv) == length(sinks) ||
            throw(ArgumentError("partition count $(length(partsv)) != sink count $(length(sinks))"))
        @sync for (snk, part) in zip(sinks, partsv)
            Threads.@spawn write(snk, part; append, writeheader, header,
                                 compress, partition=false, ntasks=1, kw...)
        end
        return sink
    end
    cols0 = Tables.columns(table)
    names = header !== nothing ? Symbol.(header) :
            collect(Symbol, Tables.columnnames(cols0))
    cols = AbstractVector[Tables.getcolumn(cols0, nm) for nm in Tables.columnnames(cols0)]
    isempty(cols) && length(names) > 0 && (cols = AbstractVector[])
    nrows = isempty(cols) ? 0 : length(cols[1])
    wantheader = writeheader === nothing ? !append : writeheader

    blocks = Vector{Vector{UInt8}}()
    if wantheader && !isempty(names)
        push!(blocks, _renderheader(names, o))
    end
    if nrows > 0
        nb = max(1, min(ntasks, cld(nrows, 4096)))
        bounds = [1 + (b - 1) * nrows ÷ nb for b in 1:nb]
        push!(bounds, nrows + 1)
        rendered = Vector{Vector{UInt8}}(undef, nb)
        if nb > 1
            @sync for b in 1:nb
                Threads.@spawn rendered[b] = _renderblock(cols, bounds[b], bounds[b + 1] - 1, o)
            end
        else
            rendered[1] = _renderblock(cols, 1, nrows, o)
        end
        append!(blocks, rendered)
    end
    out = isempty(blocks) ? UInt8[] : reduce(vcat, blocks)

    gzip = compress === :gzip ||
           (compress === :auto && sink isa AbstractString && endswith(String(sink), ".gz"))
    compress in (:auto, :gzip, :none) ||
        throw(ArgumentError("compress must be :auto, :gzip, or :none (got $compress)"))
    payload = gzip ? transcode(GzipCompressor, out) : out
    if sink isa AbstractString
        open(io -> Base.write(io, payload), String(sink), append ? "a" : "w")
    else
        Base.write(sink, payload)
    end
    return sink
end

end # module KernelWrite
