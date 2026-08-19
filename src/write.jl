
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
# The engine renders from Tables.columns, in parallel: rows split into
# contiguous blocks, each block renders on its own task. Within a block every
# COLUMN renders first, through a loop specialized on that column's element
# type, into a staged (bytes, ends) buffer — one dynamic dispatch per column
# per block instead of one per cell (the per-cell `col[r]` on an
# `AbstractVector` was 4× slower than 0.10) — and a row-gather then
# interleaves the staged cells with delimiters and newlines. Ints emit their
# digits directly, floats through Ryu into the buffer at the write position
# (no `string(x)`), strings memcpy after one structural-byte scan. Blocks
# stream to the sink in order (nothing is concatenated). Bytes out are
# identical at any thread count and byte-identical to 0.10's writer.

module KernelWrite

using Tables, Dates, Printf, CodecZlib
using CodecZlib.TranscodingStreams
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
    delim isa Char && !isascii(delim) &&
        throw(ArgumentError("write delim must be a single byte (got $(repr(delim)))"))
    quotestyle in WRITE_QUOTESTYLES ||
        throw(ArgumentError("quotestyle must be one of $(WRITE_QUOTESTYLES) (got $quotestyle)"))
    oqc = something(openquotechar, quotechar)
    cqc = something(closequotechar, quotechar)
    ec = something(escapechar, cqc)
    for (nm, c) in (("quotechar", quotechar), ("openquotechar", oqc),
                    ("closequotechar", cqc), ("escapechar", ec),
                    ("decimal", decimal))
        isascii(c) || throw(ArgumentError("$nm must be ASCII (got $(repr(c)))"))
    end
    oq = oqc % UInt8
    cq = cqc % UInt8
    e = ec % UInt8
    d = delim isa Char ? delim % UInt8 : codeunit(delim, 1)
    d in (UInt8('\r'), UInt8('\n')) &&
        throw(ArgumentError("write delimiter may not be \\r or \\n"))
    d == oq && throw(ArgumentError("write delimiter may not equal the open quote character"))
    any(c -> c in ('\r', '\n'), (oqc, cqc, ec)) &&
        throw(ArgumentError("write quote/escape characters may not be \\r or \\n"))
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
_numericsyntax(b::UInt8) = b - UInt8('0') <= 0x09 || b in (UInt8('+'), UInt8('-'))

function _writebytes(io::IO, bytes::AbstractVector{UInt8}, o::WriteOpts;
                     stringcell::Bool)
    if o.quotestyle === :none
        stringcell && isempty(bytes) &&
            throw(ArgumentError("quotestyle=:none cannot distinguish an empty string from missing"))
        for b in bytes
            _needsquote(o, b) &&
                throw(ArgumentError("quotestyle=:none cannot write a value containing " *
                                    "a structural byte: $(repr(String(bytes)))"))
        end
        return Base.write(io, bytes)
    end
    # Empty quoted content is the parser's present-empty-string spelling.
    # Empty unquoted content is missing, matching the kernel's pinned 1.0
    # convention (and intentionally differing from CSV.write's ambiguity).
    quote_it = stringcell && (o.quotestyle === :all || isempty(bytes))
    if !quote_it
        for b in bytes
            if _needsquote(o, b)
                quote_it = true
                break
            end
        end
        # leading/trailing whitespace survives a round-trip only when quoted
        if stringcell && !quote_it && !isempty(bytes)
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

_writestring(io::IO, s::AbstractString, o::WriteOpts; stringcell::Bool=true) =
    _writebytes(io, codeunits(s), o; stringcell)

_writescalar(io::IO, s::AbstractString, o::WriteOpts) =
    _writestring(io, s, o; stringcell=false)
_writescalar(io::IO, x, o::WriteOpts) = _writescalar(io, string(x), o)

function _writecell(io::IO, x, o::WriteOpts)
    if x === missing
        _writebytes(io, o.missingstring, o; stringcell=false)
    elseif x isa AbstractString
        _writestring(io, x, o)
    elseif x isa AbstractFloat
        if o.floatfmt !== nothing
            s = Printf.format(o.floatfmt, x)
            o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
            _writescalar(io, s, o)
        else
            s = string(x)
            o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
            _writescalar(io, s, o)
        end
    elseif x isa Dates.TimeType
        _writescalar(io, o.dateformat === nothing ? string(x) : Dates.format(x, o.dateformat), o)
    elseif x isa Bool
        _writescalar(io, x, o)
    elseif x isa Integer
        # The ordinary CSV dialect cannot conflict with an integer spelling;
        # keep that hot path allocation-free. Exotic numeric delimiters or
        # quote bytes take the checked rendering path.
        any(_numericsyntax, (o.delim, o.oq, o.cq)) ? _writescalar(io, x, o) : print(io, x)
    elseif x isa Number
        _writescalar(io, x, o)
    else
        _writestring(io, string(x), o)
    end
    return
end

# --- staged column rendering ---------------------------------------------------
#
# A block's cells for ONE column, rendered into `bytes` back to back with
# `ends[k]` = end offset of the k-th cell (cell k = bytes[ends[k-1]+1 : ends[k]]).

struct ColStage
    bytes::Vector{UInt8}
    ends::Vector{Int}
end
ColStage() = ColStage(UInt8[], Int[])
@inline function _reset!(st::ColStage, ncells::Int)
    empty!(st.bytes)
    resize!(st.ends, ncells)
    return st
end
@inline _endcell!(st::ColStage, k::Int) = (@inbounds st.ends[k] = length(st.bytes); nothing)

# ensure `st.bytes` can take `n` more bytes when written through pointers
@inline function _room!(v::Vector{UInt8}, n::Int)
    need = length(v) + n
    need > length(v) && resize!(v, need)     # length grows; content is written by the caller
    return
end

# structural-byte scan for a byte range: any delimiter/quote/CR/LF?
@inline function _needsquotebytes(o::WriteOpts, p::Ptr{UInt8}, n::Int)
    @inbounds for k in 0:(n - 1)
        _needsquote(o, unsafe_load(p, k + 1)) && return true
    end
    return false
end

# The exact `_writebytes` policy, appending to a Vector{UInt8}. `stringcell`
# says whether the cell is a string (only strings get :all-quoting, the
# empty-means-present rule, and whitespace-preserving quoting).
function _appendbytes!(out::Vector{UInt8}, bytes::AbstractVector{UInt8}, o::WriteOpts,
                       stringcell::Bool)
    n = length(bytes)
    if o.quotestyle === :none
        stringcell && n == 0 &&
            throw(ArgumentError("quotestyle=:none cannot distinguish an empty string from missing"))
        for b in bytes
            _needsquote(o, b) &&
                throw(ArgumentError("quotestyle=:none cannot write a value containing " *
                                    "a structural byte: $(repr(String(collect(bytes))))"))
        end
        return append!(out, bytes)
    end
    quote_it = stringcell && (o.quotestyle === :all || n == 0)
    if !quote_it
        for b in bytes
            if _needsquote(o, b)
                quote_it = true
                break
            end
        end
        if stringcell && !quote_it && n > 0
            (bytes[1] == UInt8(' ') || bytes[end] == UInt8(' ')) && (quote_it = true)
        end
    end
    quote_it || return append!(out, bytes)
    push!(out, o.oq)
    for b in bytes
        (b == o.cq || (o.e != o.cq && b == o.e)) && push!(out, o.e)
        push!(out, b)
    end
    push!(out, o.cq)
    return out
end
_appendstring!(out::Vector{UInt8}, s::AbstractString, o::WriteOpts) =
    _appendbytes!(out, codeunits(s), o, true)
_appendscalar!(out::Vector{UInt8}, s::AbstractString, o::WriteOpts) =
    _appendbytes!(out, codeunits(s), o, false)

# fast path for String / SubString{String}: pointer scan, one memcpy when no
# quoting is needed (the overwhelmingly common case)
function _appendstring!(out::Vector{UInt8}, s::Union{String, SubString{String}}, o::WriteOpts)
    n = ncodeunits(s)
    if o.quotestyle === :minimal && n > 0
        GC.@preserve s begin
            p = pointer(s)
            if !_needsquotebytes(o, p, n) &&
               unsafe_load(p) != UInt8(' ') && unsafe_load(p, n) != UInt8(' ')
                len = length(out)
                _room!(out, n)
                GC.@preserve out unsafe_copyto!(pointer(out, len + 1), p, n)
                return out
            end
        end
    end
    return _appendbytes!(out, codeunits(s), o, true)
end

# --- integers: digits straight into the buffer ---------------------------------
@inline function _appendint!(out::Vector{UInt8}, x::Union{Int128, Int64, Int32, Int16, Int8})
    neg = x < 0
    u = neg ? reinterpret(unsigned(typeof(x)), -x) : unsigned(x)   # wraps typemin correctly
    return _appendudec!(out, u, neg)
end
@inline _appendint!(out::Vector{UInt8}, x::Union{UInt128, UInt64, UInt32, UInt16, UInt8}) =
    _appendudec!(out, x, false)
# other Integers (BigInt, ...) print via Base
_appendint!(out::Vector{UInt8}, x::Integer) = append!(out, codeunits(string(x)))
function _appendudec!(out::Vector{UInt8}, u::Unsigned, neg::Bool)
    nd = u == 0 ? 1 : ndigits(u; base=10)
    len = length(out)
    _room!(out, nd + neg)
    @inbounds begin
        neg && (out[len + 1] = UInt8('-'))
        pos = len + neg + nd
        while true
            q = u ÷ 0xa
            out[pos] = UInt8('0') + (u - q * 0xa) % UInt8
            u = q
            pos -= 1
            u == 0 && break
        end
    end
    return out
end
# 64-bit and narrower: two digits per step through Ryu's pair table (half the
# divisions of the generic loop above, which stays for UInt128)
@inline function _declen64(v::UInt64)   # Ryu.decimallength stops at 17 digits; ints need 20
    v < 10 && return 1
    v < 100 && return 2
    v < 1_000 && return 3
    v < 10_000 && return 4
    v < 100_000 && return 5
    v < 1_000_000 && return 6
    v < 10_000_000 && return 7
    v < 100_000_000 && return 8
    v < 1_000_000_000 && return 9
    v < 10_000_000_000 && return 10
    v < 100_000_000_000 && return 11
    v < 1_000_000_000_000 && return 12
    v < 10_000_000_000_000 && return 13
    v < 100_000_000_000_000 && return 14
    v < 1_000_000_000_000_000 && return 15
    v < 10_000_000_000_000_000 && return 16
    v < 100_000_000_000_000_000 && return 17
    v < 1_000_000_000_000_000_000 && return 18
    v < 10_000_000_000_000_000_000 && return 19
    return 20
end
function _appendudec!(out::Vector{UInt8}, u::Union{UInt64, UInt32, UInt16, UInt8}, neg::Bool)
    v = UInt64(u)
    nd = _declen64(v)
    len = length(out)
    _room!(out, nd + neg)
    @inbounds neg && (out[len + 1] = UInt8('-'))
    Base.Ryu.append_c_digits(nd, v, out, len + 1 + neg)
    return out
end

# --- floats: Ryu shortest, written at the buffer position ------------------------
# `string(x::Float64)` IS `Ryu.writeshortest(x)` with the default options. The
# generic writer spends ~60% of its time in option branches it cannot fold
# (plus/space/hash/precision/typed/compact/padexp), so this is that function
# with the defaults inlined: same digits (Ryu.reduce_shortest), same layout
# rules — fixed notation for -4 < pt <= 6 (Float16: 3) unless an integer-valued
# value would print more digits than its magnitude warrants, else `d.ddde±xx`;
# hash=true forces the trailing ".0". Byte equality with string(x) is pinned
# in the tests over random bits, specials, and every exponent form.
@inline function _appendfloat!(out::Vector{UInt8}, x::Union{Float64, Float32, Float16}, o::WriteOpts)
    len = length(out)
    _room!(out, Base.Ryu.neededdigits(typeof(x)))
    pos = _writeshortest_default(out, len + 1, x, o.decimal)
    resize!(out, pos - 1)
    return out
end

function _writeshortest_default(buf::Vector{UInt8}, pos::Int, x::T, decchar::UInt8) where {T <: Union{Float64, Float32, Float16}}
    @inbounds begin
        if x == 0
            signbit(x) && (buf[pos] = UInt8('-'); pos += 1)
            buf[pos] = UInt8('0'); buf[pos + 1] = decchar; buf[pos + 2] = UInt8('0')
            return pos + 3
        elseif isnan(x)
            buf[pos] = UInt8('N'); buf[pos + 1] = UInt8('a'); buf[pos + 2] = UInt8('N')
            return pos + 3
        elseif !isfinite(x)
            signbit(x) && (buf[pos] = UInt8('-'); pos += 1)
            buf[pos] = UInt8('I'); buf[pos + 1] = UInt8('n'); buf[pos + 2] = UInt8('f')
            return pos + 3
        end
        output, nexp = Base.Ryu.reduce_shortest(x, nothing)
        signbit(x) && (buf[pos] = UInt8('-'); pos += 1)
        olength = Base.Ryu.decimallength(output)
        pt = nexp + olength
        maxpt = T == Float16 ? 3 : 6
        expform = !(-4 < pt <= maxpt &&
                    !(pt >= olength && abs(mod(x + 0.05, 10^(pt - olength)) - 0.05) > 0.05))
        if !expform
            if pt <= 0
                buf[pos] = UInt8('0'); pos += 1
                buf[pos] = decchar; pos += 1
                for _ in 1:(-pt)
                    buf[pos] = UInt8('0'); pos += 1
                end
                Base.Ryu.append_c_digits(olength, output, buf, pos)
                return pos + olength
            elseif pt >= olength
                Base.Ryu.append_c_digits(olength, output, buf, pos)
                pos += olength
                for _ in 1:nexp
                    buf[pos] = UInt8('0'); pos += 1
                end
                buf[pos] = decchar; buf[pos + 1] = UInt8('0')
                return pos + 2
            else
                # digits with the point inside: write the two runs directly
                # (the generic writer writes then memmoves)
                Base.Ryu.append_c_digits(olength, output, buf, pos + 1)   # all digits, shifted right by one
                # move the integer digits back left by one to open the slot
                for k in 0:(pt - 1)
                    buf[pos + k] = buf[pos + k + 1]
                end
                buf[pos + pt] = decchar
                return pos + olength + 1
            end
        else
            # d.ddd e±xx
            Base.Ryu.append_c_digits(olength, output, buf, pos + 1)
            buf[pos] = buf[pos + 1]
            buf[pos + 1] = decchar
            pos += olength + 1
            if olength == 1                       # "1.0e10" (hash forces the zero)
                buf[pos] = UInt8('0'); pos += 1
            end
            buf[pos] = UInt8('e'); pos += 1
            exp2 = nexp + olength - 1
            if exp2 < 0
                buf[pos] = UInt8('-'); pos += 1
                exp2 = -exp2
            end
            if exp2 >= 100
                c = exp2 % 10
                d100 = Base.Ryu.DIGIT_TABLE16[(div(exp2, 10) % Int) + 1]
                buf[pos] = d100 % UInt8; buf[pos + 1] = (d100 >> 0x8) % UInt8
                buf[pos + 2] = UInt8('0') + (c % UInt8)
                return pos + 3
            elseif exp2 >= 10
                d100 = Base.Ryu.DIGIT_TABLE16[(exp2 % Int) + 1]
                buf[pos] = d100 % UInt8; buf[pos + 1] = (d100 >> 0x8) % UInt8
                return pos + 2
            else
                buf[pos] = UInt8('0') + (exp2 % UInt8)
                return pos + 1
            end
        end
    end
end

# --- dates: the ISO spellings `string(::Date/::DateTime)` produces, direct ----
# Date       yyyy-mm-dd            year ≥ 4 digits (more if needed), '-' if negative
# DateTime   yyyy-mm-ddTHH:MM:SS   plus ".sss" (three digits) only when the
#                                  milliseconds are nonzero (Dates' `.s` token)
# Byte equality with `string(x)` is pinned by the test suite over adversarial
# years (negative, 5-digit) and every millisecond value.
@inline function _append2!(out::Vector{UInt8}, v::Int)   # two zero-padded digits, 0 ≤ v < 100
    len = length(out)
    _room!(out, 2)
    @inbounds begin
        out[len + 1] = UInt8('0') + (v ÷ 10) % UInt8
        out[len + 2] = UInt8('0') + (v % 10) % UInt8
    end
    return out
end
@inline function _appendyear!(out::Vector{UInt8}, y::Int)
    y < 0 && (push!(out, UInt8('-')); y = -y)
    y < 1000 && push!(out, UInt8('0'))
    y < 100 && push!(out, UInt8('0'))
    y < 10 && push!(out, UInt8('0'))
    return _appendudec!(out, unsigned(y), false)
end
function _appenddate!(out::Vector{UInt8}, x::Date)
    y, m, d = Dates.yearmonthday(x)
    _appendyear!(out, y); push!(out, UInt8('-'))
    _append2!(out, m); push!(out, UInt8('-'))
    _append2!(out, d)
    return out
end
function _appenddatetime!(out::Vector{UInt8}, x::DateTime)
    y, m, d = Dates.yearmonthday(x)
    _appendyear!(out, y); push!(out, UInt8('-'))
    _append2!(out, m); push!(out, UInt8('-'))
    _append2!(out, d); push!(out, UInt8('T'))
    _append2!(out, Dates.hour(x)); push!(out, UInt8(':'))
    _append2!(out, Dates.minute(x)); push!(out, UInt8(':'))
    _append2!(out, Dates.second(x))
    ms = Dates.millisecond(x)
    if ms != 0                                   # ".sss" — three digits, omitted only when zero
        push!(out, UInt8('.'))
        d1, r = divrem(ms, 100); d2, d3 = divrem(r, 10)
        push!(out, UInt8('0') + d1 % UInt8)
        push!(out, UInt8('0') + d2 % UInt8)
        push!(out, UInt8('0') + d3 % UInt8)
    end
    return out
end

const _TRUE = codeunits("true"); const _FALSE = codeunits("false")
@inline _boolbyte(b::UInt8) = b in (UInt8('t'), UInt8('r'), UInt8('u'), UInt8('e'),
                                    UInt8('f'), UInt8('a'), UInt8('l'), UInt8('s'))

# --- per-column staged loops (specialized on the column type) ------------------
# Each renders cells lo..hi of `col`; the loop body is monomorphic, so the
# `x === missing` split is static for Union columns.

@inline _stagecell!(st::ColStage, x, o::WriteOpts) = _appendcell!(st.bytes, x, o)

# the semantic reference is `_writecell`; this mirrors it, appending to a byte vector
@inline function _appendcell!(out::Vector{UInt8}, x, o::WriteOpts)
    if x === missing
        _appendbytes!(out, o.missingstring, o, false)
    elseif x isa AbstractString
        _appendstring!(out, x, o)
    elseif x isa AbstractFloat
        if o.floatfmt !== nothing
            s = Printf.format(o.floatfmt, x)
            o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
            _appendscalar!(out, s, o)
        elseif x isa Union{Float64, Float32, Float16} && !any(_numericsyntax, (o.delim, o.oq, o.cq))
            _appendfloat!(out, x, o)
        else
            s = string(x)
            o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
            _appendscalar!(out, s, o)
        end
    elseif x isa Dates.TimeType
        if o.dateformat === nothing && !any(_numericsyntax, (o.delim, o.oq, o.cq)) &&
           o.delim != UInt8('T') && o.delim != UInt8(':') && o.delim != UInt8('.') &&
           x isa Union{Date, DateTime}
            x isa Date ? _appenddate!(out, x) : _appenddatetime!(out, x)
        else
            _appendscalar!(out, o.dateformat === nothing ? string(x) : Dates.format(x, o.dateformat), o)
        end
    elseif x isa Bool
        # the letters of true/false can only be structural under an exotic
        # dialect; the checked path handles that
        if o.quotestyle !== :none && !_boolbyte(o.delim) && !_boolbyte(o.oq) && !_boolbyte(o.cq)
            append!(out, x ? _TRUE : _FALSE)
        else
            _appendscalar!(out, x ? "true" : "false", o)
        end
    elseif x isa Integer
        any(_numericsyntax, (o.delim, o.oq, o.cq)) ? _appendscalar!(out, string(x), o) :
                                                    _appendint!(out, x)
    elseif x isa Number
        _appendscalar!(out, string(x), o)
    else
        _appendstring!(out, string(x), o)
    end
    return
end

# the monomorphic driver: `col` is concretely typed here, so `col[r]` and the
# `_stagecell!` branches resolve statically for the common element types
function _stagecolumn!(st::ColStage, col::AbstractVector, lo::Int, hi::Int, o::WriteOpts)
    _reset!(st, hi - lo + 1)
    k = 0
    @inbounds for r in lo:hi
        _stagecell!(st, col[r], o)
        _endcell!(st, k += 1)
    end
    return st
end

# --- row-block rendering (the parallel unit) --------------------------------

# Narrow tables (the common case): the columns travel as a Tuple, so the
# recursion below is unrolled by the compiler with every column's element type
# known statically — one specialized row renderer, direct row-major writes,
# no staging and no gather. Wide tables use the staged path (a Tuple of
# hundreds of vectors would cost compile time out of proportion).
const TUPLE_RENDER_MAXCOLS = 32

@inline function _writerow!(out::Vector{UInt8}, r::Int, cols::Tuple, o::WriteOpts)
    _writecells!(out, r, cols, o)
    for b in o.newline
        push!(out, b)
    end
    return
end
@inline _writecells!(out::Vector{UInt8}, r::Int, ::Tuple{}, o::WriteOpts) = nothing
@inline function _writecells!(out::Vector{UInt8}, r::Int, cols::Tuple, o::WriteOpts)
    @inbounds _appendcell!(out, first(cols)[r], o)
    rest = Base.tail(cols)
    isempty(rest) || push!(out, o.delim)
    return _writecells!(out, r, rest, o)
end

function _renderblock_tuple(cols::Tuple, lo::Int, hi::Int, o::WriteOpts)
    out = UInt8[]
    nrows = hi - lo + 1
    # size the block from a sample of its own rows: a fixed 16 B/cell guess
    # over-reserved ~2x on typical tables, and the fresh pages that reserves
    # cost first-touch faults across every task at once — measured as the
    # difference between 3.8x and ~6x speedup on eight threads
    probe = min(nrows, 32)
    @inbounds for r in lo:(lo + probe - 1)
        _writerow!(out, r, cols, o)
    end
    if probe < nrows
        est = (length(out) * nrows) ÷ probe
        sizehint!(out, est + (est >> 3) + 64 * length(cols))
        @inbounds for r in (lo + probe):hi
            _writerow!(out, r, cols, o)
        end
    end
    return out
end

function _renderblock(cols, lo::Int, hi::Int, o::WriteOpts)
    ncols = length(cols)
    ncols <= TUPLE_RENDER_MAXCOLS && return _renderblock_tuple(Tuple(cols), lo, hi, o)
    nrows = hi - lo + 1
    stages = [ColStage() for _ in 1:ncols]
    total = 0
    for j in 1:ncols
        _stagecolumn!(stages[j], cols[j], lo, hi, o)   # one dynamic dispatch per column
        total += length(stages[j].bytes)
    end
    out = Vector{UInt8}(undef, total + nrows * (max(ncols - 1, 0) + length(o.newline)))
    pos = 1
    nl = o.newline
    GC.@preserve out begin
        @inbounds for k in 1:nrows
            for j in 1:ncols
                st = stages[j]
                s = k == 1 ? 1 : st.ends[k - 1] + 1
                e = st.ends[k]
                n = e - s + 1
                n > 0 && (unsafe_copyto!(pointer(out, pos), pointer(st.bytes, s), n); pos += n)
                j < ncols && (out[pos] = o.delim; pos += 1)
            end
            for b in nl
                out[pos] = b; pos += 1
            end
        end
    end
    return out
end

function _renderheader(names, o::WriteOpts)
    io = IOBuffer()
    for (j, nm) in enumerate(names)
        _writestring(io, String(nm), o)
        j < length(names) && Base.write(io, o.delim)
    end
    Base.write(io, o.newline)
    return take!(io)
end

# --- RowWriter: the row-string iterator ---------------------------------------

"""
    KernelWrite.RowWriter(table; writeheader=true, header=nothing, kw...)

Iterate `table` as CSV-formatted `String`s: the header line first (unless
`writeheader=false`), then one line per row, each rendered by exactly the
code path `write` uses — so `join(RowWriter(t))` is byte-identical to
`write(io, t)`. `kw` is the writer's dialect surface (delim, quotestyle,
floatformat, dateformat, ...). Streams: rows render on demand from a
row-access view of the table (`Tables.rows`), no whole-table buffer.
"""
struct RowWriter{R, I}
    rows::R
    initial::I
    names::Vector{Symbol}
    o::WriteOpts
    writeheader::Bool
    prefetched::Bool
end

function RowWriter(table; writeheader::Bool=true, header::Union{Nothing, Vector}=nothing,
                   kw...)
    o = _writeopts(; kw...)
    rows = Tables.rows(table)
    sch = Tables.schema(rows)
    prefetched = sch === nothing
    initial = prefetched ? iterate(rows) : nothing
    source_names = sch === nothing ?
                   initial === nothing ? nothing :
                   collect(Symbol, Tables.columnnames(initial[1])) :
                   collect(Symbol, sch.names)
    names = header === nothing ? something(source_names, Symbol[]) : Symbol.(header)
    source_names !== nothing && length(names) != length(source_names) &&
        throw(ArgumentError("header has $(length(names)) names for $(length(source_names)) columns"))
    return RowWriter(rows, initial, names, o, writeheader, prefetched)
end

Base.IteratorSize(::Type{<:RowWriter}) = Base.SizeUnknown()
Base.eltype(::Type{<:RowWriter}) = String

function _renderrow(row, names, o::WriteOpts)
    io = IOBuffer()
    ncols = length(names)
    for (j, nm) in enumerate(names)
        _writecell(io, Tables.getcolumn(row, j), o)
        j < ncols && Base.write(io, o.delim)
    end
    Base.write(io, o.newline)
    return String(take!(io))
end

function Base.iterate(rw::RowWriter, state=nothing)
    if state === nothing
        it = rw.prefetched ? rw.initial : iterate(rw.rows)
        if rw.writeheader && !isempty(rw.names)
            line = String(_renderheader(rw.names, rw.o))
            rw.o.bom && (line = string('\ufeff', line))
            return line, (it,)
        elseif rw.o.bom
            it === nothing && return "\ufeff", (nothing,)
            row, rstate = it
            return string('\ufeff', _renderrow(row, rw.names, rw.o)),
                   (iterate(rw.rows, rstate),)
        end
        state = (it,)
    end
    it = state[1]
    it === nothing && return nothing
    row, rstate = it
    return _renderrow(row, rw.names, rw.o), (iterate(rw.rows, rstate),)
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
    ntasks >= 1 || throw(ArgumentError("ntasks must be >= 1 (got $ntasks)"))
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
    source_names = collect(Symbol, Tables.columnnames(cols0))
    names = header !== nothing ? Symbol.(header) : source_names
    length(names) == length(source_names) ||
        throw(ArgumentError("header has $(length(names)) names for $(length(source_names)) columns"))
    cols = AbstractVector[Tables.getcolumn(cols0, nm) for nm in source_names]
    nrows = isempty(cols) ? 0 : length(cols[1])
    all(col -> length(col) == nrows, cols) ||
        throw(ArgumentError("all table columns must have the same length"))
    wantheader = writeheader === nothing ? !append : writeheader

    blocks = Vector{Vector{UInt8}}()
    o.bom && !append && push!(blocks, UInt8[0xef, 0xbb, 0xbf])
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
    gzip = compress === :gzip ||
           (compress === :auto && sink isa AbstractString && endswith(String(sink), ".gz"))
    compress in (:auto, :gzip, :none) ||
        throw(ArgumentError("compress must be :auto, :gzip, or :none (got $compress)"))
    # blocks stream to the sink in order — the output is never concatenated
    # into a second whole-file buffer; gzip compresses through a stream
    emit = function (io)
        if gzip
            gz = GzipCompressorStream(io)
            for blk in blocks
                Base.write(gz, blk)
            end
            # finish the gzip member without closing the caller's IO
            Base.write(gz, TranscodingStreams.TOKEN_END)
            flush(gz)
        else
            # an in-memory sink grows by doubling under repeated writes — for
            # a 75 MiB output that is more copying than the rendering itself;
            # reserve the total once (the size is known: the blocks exist)
            io isa Base.GenericIOBuffer && Base.ensureroom(io, sum(length, blocks; init=0))
            for blk in blocks
                Base.write(io, blk)
            end
        end
        return
    end
    if sink isa AbstractString
        open(emit, String(sink), append ? "a" : "w")
    else
        seekable = sink isa IO && hasmethod(seek, Tuple{typeof(sink), Integer}) &&
                   hasmethod(seekend, Tuple{typeof(sink)})
        seekable && (append ? seekend(sink) : seekstart(sink))
        emit(sink)
        # `append=false` means replacement for seekable IOs as it does for a
        # path opened with "w". Remove any stale suffix when the new payload
        # is shorter than the old contents.
        !append && seekable && applicable(truncate, sink, position(sink)) &&
            truncate(sink, position(sink))
    end
    return sink
end

end # module KernelWrite
