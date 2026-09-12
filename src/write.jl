
# CSV.write implementation and formatting options.
#
#   quotestyle   :minimal (default) — quote only when the value contains the
#                delimiter, the quote, CR/LF, or leading/trailing whitespace;
#                :all — every string cell quoted; :none — never quote (values
#                containing structural bytes are an ArgumentError: silent
#                corruption is not an option)
#   floatformat  a printf-style format string ("%.3f") applied to every
#                AbstractFloat cell; default is Julia's shortest
#                round-trip (Ryu) printing
#   compress     :auto (by .gz extension) | :gzip | :none
#   partition    write a Vector of sinks in parallel, one table partition each
#
# The engine renders contiguous row blocks from `Tables.columns` in parallel.
# Every table shares one tagged row loop, so a new schema costs no
# compilation; uncommon column types stage once per block before gathering.
# Integers emit digits directly, floats use Ryu at the output position, and
# strings copy after one structural scan. Blocks stream to the sink in order. Output bytes do not depend on the
# thread count.

using Tables, Dates, Printf, CodecZlib

# The shared row loop keeps a logical byte cursor separate from storage size.
# Reserving storage once per block avoids resizing a Vector for every cell.
# Existing cell formatters use the same indexing and append operations here.
mutable struct _WriteBuffer <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
    len::Int
end

_WriteBuffer() = _WriteBuffer(Vector{UInt8}(undef, 1024), 0)
Base.size(b::_WriteBuffer) = (b.len,)
Base.length(b::_WriteBuffer) = b.len
Base.IndexStyle(::Type{_WriteBuffer}) = IndexLinear()
function Base.getindex(b::_WriteBuffer, i::Int)
    @boundscheck checkbounds(b, i)
    @inbounds return b.bytes[i]
end

function Base.setindex!(b::_WriteBuffer, x, i::Int)
    @boundscheck checkbounds(b, i)
    @inbounds b.bytes[i] = x
    return b
end
Base.pointer(b::_WriteBuffer, i::Integer=1) = pointer(b.bytes, i)
@noinline function _growwritebuffer!(b::_WriteBuffer, need::Int)
    resize!(b.bytes, max(need, 2length(b.bytes)))
    return b
end

function Base.resize!(b::_WriteBuffer, n::Integer)
    n > length(b.bytes) && _growwritebuffer!(b, Int(n))
    b.len = n
    return b
end

function Base.sizehint!(b::_WriteBuffer, n::Integer)
    n > length(b.bytes) && _growwritebuffer!(b, Int(n))
    return b
end

@inline function Base.push!(b::_WriteBuffer, x::UInt8)
    n = b.len + 1
    resize!(b, n)
    @inbounds b.bytes[n] = x
    return b
end

function Base.append!(b::_WriteBuffer, xs::AbstractVector{UInt8})
    n = length(xs)
    len = b.len
    resize!(b, len + n)
    copyto!(b.bytes, len + 1, xs, 1, n)
    return b
end

const _WriteOutput = Union{Vector{UInt8}, _WriteBuffer}

const WRITE_QUOTESTYLES = (:minimal, :all, :none)

# The custom float and date formats are type parameters: with the defaults
# (`Nothing`) their formatting branches vanish at compile time, which keeps
# the shared row loop free of Printf/Dates dispatch and keeps the default
# writer statically compilable.
struct WriteOpts{F <: Union{Nothing, Printf.Format}, D <: Union{Nothing, DateFormat}}
    delim::UInt8                # first delimiter byte: quoting and syntax-clash checks
    delimbytes::Vector{UInt8}   # the complete delimiter (multi-byte delimiters write as-is)
    oq::UInt8
    cq::UInt8
    e::UInt8
    newline::Vector{UInt8}
    missingstring::Vector{UInt8}
    quotestyle::Symbol
    floatfmt::F
    dateformat::D
    decimal::UInt8
    bom::Bool
    bufsize::Int
end

# One ASCII byte, spelled as a Char or a one-character string.
function _asciibyte(name::String, c)
    c isa AbstractString && length(c) == 1 && (c = first(c))
    c isa Char || throw(ArgumentError("$name must be a character (got $(repr(c)))"))
    isascii(c) || throw(ArgumentError("$name must be ASCII (got $(repr(c)))"))
    return c % UInt8
end

const _WRITEKW = (:delim, :quotechar, :openquotechar, :closequotechar, :escapechar,
                  :newline, :missingstring, :quotestyle, :quotestrings, :floatformat,
                  :dateformat, :decimal, :bom, :bufsize)

function _checkwritekwargs(kw)
    for k in keys(kw)
        k in _WRITEKW ||
            throw(ArgumentError("unsupported write keyword $k; see ?CSV.write"))
    end
    return
end

function _writeopts(; delim::Union{Char, AbstractString}=',',
                    quotechar='"',
                    openquotechar=nothing,
                    closequotechar=nothing,
                    escapechar=nothing,
                    newline::Union{Char, AbstractString}='\n',
                    missingstring::Union{Nothing, AbstractString}="",
                    quotestyle::Union{Symbol, AbstractString}=:minimal,
                    quotestrings::Bool=false,
                    floatformat::Union{Nothing, AbstractString}=nothing,
                    dateformat=nothing,
                    decimal='.',
                    bom::Bool=false,
                    bufsize::Integer=1 << 22)
    bufsize >= 1 || throw(ArgumentError("bufsize must be >= 1 (got $bufsize)"))
    quotestyle = Symbol(quotestyle)
    quotestrings && quotestyle === :none &&
        throw(ArgumentError("quotestrings=true conflicts with quotestyle=:none"))
    quotestrings && (quotestyle = :all)
    quotestyle in WRITE_QUOTESTYLES ||
        throw(ArgumentError("quotestyle must be :minimal, :all, or :none"))
    oq = _asciibyte("openquotechar", something(openquotechar, quotechar))
    cq = _asciibyte("closequotechar", something(closequotechar, quotechar))
    e = escapechar === nothing ? cq : _asciibyte("escapechar", escapechar)
    dec = _asciibyte("decimal", decimal)
    (UInt8('0') <= dec <= UInt8('9') ||
     dec in (UInt8('+'), UInt8('-'), UInt8('e'), UInt8('E'), oq, cq, e)) &&
        throw(ArgumentError("decimal $(repr(Char(dec))) conflicts with numeric or quote syntax"))
    delimbytes = Vector{UInt8}(codeunits(string(delim)))
    isempty(delimbytes) && throw(ArgumentError("write delimiter must be non-empty"))
    for b in delimbytes
        b in (UInt8('\r'), UInt8('\n')) &&
            throw(ArgumentError("write delimiter may not contain \\r or \\n"))
        b == oq && throw(ArgumentError("write delimiter may not contain the open quote character"))
    end
    any(b -> b in (UInt8('\r'), UInt8('\n')), (oq, cq, e)) &&
        throw(ArgumentError("write quote/escape characters may not be \\r or \\n"))
    df = dateformat === nothing ? nothing :
         dateformat isa DateFormat ? dateformat : DateFormat(string(dateformat))
    ff = floatformat === nothing ? nothing : Printf.Format(String(floatformat))
    intbufsize = bufsize > typemax(Int) ? typemax(Int) : Int(bufsize)
    return WriteOpts(first(delimbytes), delimbytes, oq, cq, e,
                     Vector{UInt8}(codeunits(string(newline))),
                     Vector{UInt8}(codeunits(something(missingstring, ""))),
                     quotestyle, ff, df, dec, bom, intbufsize)
end

@noinline _rowtoolarge(n::Int, cap::Int) =
    throw(ArgumentError("row size ($n) exceeds bufsize ($cap); pass a larger bufsize"))

# --- cell rendering ---------------------------------------------------------

_needsquote(o::WriteOpts, b::UInt8) =
    b == o.delim || b == o.oq || b == o.cq || b == UInt8('\n') || b == UInt8('\r')
_numericsyntax(b::UInt8) = b - UInt8('0') <= 0x09 || b in (UInt8('+'), UInt8('-'))
_datesyntax(b::UInt8) = _numericsyntax(b) || b in (UInt8('T'), UInt8(':'), UInt8('.'))

function _appenddelim!(out::_WriteOutput, o::WriteOpts)
    length(o.delimbytes) == 1 ? push!(out, o.delim) : append!(out, o.delimbytes)
    return out
end

@noinline _nothingerror() = throw(ArgumentError(
    "a `nothing` cell is not printable; use transform=(column, value) -> " *
    "something(value, missing) or replace it before writing"))

# --- staged column rendering ---------------------------------------------------
#
# A block's cells for ONE column, rendered into `bytes` back to back with
# `ends[k]` = end offset of the k-th cell (cell k = bytes[ends[k-1]+1 : ends[k]]).

struct ColStage
    bytes::Vector{UInt8}
    ends::Vector{Int}
end

ColStage() = ColStage(UInt8[], Int[])
function _reset!(st::ColStage, ncells::Int)
    empty!(st.bytes)
    resize!(st.ends, ncells)
    return st
end

_endcell!(st::ColStage, k::Int) = (@inbounds st.ends[k] = length(st.bytes); nothing)

# ensure `st.bytes` can take `n` more bytes when written through pointers
function _room!(v::_WriteOutput, n::Int)
    need = length(v) + n
    need > length(v) && resize!(v, need)     # length grows; content is written by the caller
    return
end

# structural-byte scan for a byte range: any delimiter/quote/CR/LF? Eight
# bytes per step (the marks can only be set when some byte matches, so the
# borrow-propagation false positives of `_eqmask8_c` cannot create a match).
@inline function _needsquotebytes(o::WriteOpts, p::Ptr{UInt8}, n::Int)
    d, oq, cq = o.delim, o.oq, o.cq
    k = 0
    while k + 8 <= n
        w = unsafe_load(Ptr{UInt64}(p + k))
        m = _eqmask8_c(w, d) | _eqmask8_c(w, oq) | _eqmask8_c(w, cq) |
            _eqmask8_c(w, UInt8('\n')) | _eqmask8_c(w, UInt8('\r'))
        m != zero(UInt64) && return true
        k += 8
    end
    while k < n
        b = unsafe_load(p, k + 1)
        (b == d || b == oq || b == cq || b == UInt8('\n') || b == UInt8('\r')) && return true
        k += 1
    end
    return false
end

# The cell quoting policy, appending to a Vector{UInt8}. `stringcell` says
# whether the cell is a string (only strings get :all-quoting, the
# empty-means-present rule, and whitespace-preserving quoting). Empty quoted
# content is the parser's present-empty-string spelling; empty unquoted content
# is missing, matching the parser's convention. A multi-byte
# delimiter quotes on its first byte: over-quoting is harmless.
function _appendbytes!(out::_WriteOutput, bytes::AbstractVector{UInt8}, o::WriteOpts,
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
    d, oq, cq, e = o.delim, o.oq, o.cq, o.e
    quote_it = stringcell && (o.quotestyle === :all || n == 0)
    if !quote_it
        for b in bytes
            if b == d || b == oq || b == cq || b == UInt8('\n') || b == UInt8('\r')
                quote_it = true
                break
            end
        end
        if stringcell && !quote_it && n > 0
            (bytes[1] == UInt8(' ') || bytes[end] == UInt8(' ')) && (quote_it = true)
        end
    end
    quote_it || return append!(out, bytes)
    # reserve the escaped upper bound once, then store bytes by index
    k = length(out)
    _room!(out, 2n + 2)
    @inbounds begin
        out[k += 1] = oq
        for b in bytes
            (b == cq || (e != cq && b == e)) && (out[k += 1] = e)
            out[k += 1] = b
        end
        out[k += 1] = cq
    end
    resize!(out, k)
    return out
end

_appendstring!(out::_WriteOutput, s::AbstractString, o::WriteOpts) =
    _appendbytes!(out, codeunits(s), o, true)
_appendscalar!(out::_WriteOutput, s::AbstractString, o::WriteOpts) =
    _appendbytes!(out, codeunits(s), o, false)

# fast path for String / SubString{String}: pointer scan, one memcpy when no
# quoting is needed (the overwhelmingly common case)
function _appendstring!(out::_WriteOutput, s::Union{String, SubString{String}}, o::WriteOpts)
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

# DataString cells: a view payload has its bytes in the column buffer; an
# inline payload (at most 12 bytes) is copied to a stack scratch first. Both
# then take the same pointer scan and single memcpy as String.
function _appendstring!(out::_WriteOutput, s::DataString, o::WriteOpts)
    n = ncodeunits(s)
    if o.quotestyle === :minimal && n > 0
        if n > INLINE_MAX
            GC.@preserve s begin
                _appendscanned!(out, pointer(s.data, payloadpos(s.p)), n, o) && return out
            end
        else
            scratch = Ref{NTuple{16, UInt8}}()
            p = Ptr{UInt8}(Base.unsafe_convert(Ptr{NTuple{16, UInt8}}, scratch))
            GC.@preserve scratch begin
                @inbounds for i in 1:n
                    unsafe_store!(p, codeunit(s, i), i)
                end
                _appendscanned!(out, p, n, o) && return out
            end
        end
    end
    return _appendbytes!(out, codeunits(s), o, true)
end
# the unquoted fast path shared by String and DataString: true when appended
@inline function _appendscanned!(out::_WriteOutput, p::Ptr{UInt8}, n::Int, o::WriteOpts)
    (!_needsquotebytes(o, p, n) && unsafe_load(p) != UInt8(' ') &&
     unsafe_load(p, n) != UInt8(' ')) || return false
    len = length(out)
    _room!(out, n)
    GC.@preserve out unsafe_copyto!(pointer(out, len + 1), p, n)
    return true
end

# --- integers: digits straight into the buffer ---------------------------------
function _appendint!(out::_WriteOutput, x::Union{Int128, Int64, Int32, Int16, Int8})
    neg = x < 0
    u = neg ? reinterpret(unsigned(typeof(x)), -x) : unsigned(x)   # wraps typemin correctly
    return _appendudec!(out, u, neg)
end

_appendint!(out::_WriteOutput, x::Union{UInt128, UInt64, UInt32, UInt16, UInt8}) =
    _appendudec!(out, x, false)
# other Integers (BigInt, ...) print via Base
_appendint!(out::_WriteOutput, x::Integer) = append!(out, codeunits(string(x)))
function _appendudec!(out::_WriteOutput, u::Unsigned, neg::Bool)
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

function _appendudec!(out::_WriteOutput, u::Union{UInt64, UInt32, UInt16, UInt8}, neg::Bool)
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
# hash=true forces the trailing ".0". The output is byte-identical to
# `string(x)` for every bit pattern, the specials, and every exponent form.
@inline function _appendfloat!(out::_WriteOutput, x::Union{Float64, Float32, Float16}, o::WriteOpts)
    len = length(out)
    _room!(out, Base.Ryu.neededdigits(typeof(x)))
    pos = _writeshortest_default(out, len + 1, x, o.decimal)
    resize!(out, pos - 1)
    return out
end

function _writeshortest_default(buf::_WriteOutput, pos::Int, x::T, decchar::UInt8) where {T <: Union{Float64, Float32, Float16}}
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
# The output is byte-identical to `string(x)`, including negative and
# five-digit years and every millisecond value.
@inline function _append2!(out::_WriteOutput, v::Integer)   # two zero-padded digits, 0 ≤ v < 100
    len = length(out)
    _room!(out, 2)
    @inbounds begin
        out[len + 1] = UInt8('0') + (v ÷ 10) % UInt8
        out[len + 2] = UInt8('0') + (v % 10) % UInt8
    end
    return out
end

@inline function _appendyear!(out::_WriteOutput, y::Integer)
    y < 0 && (push!(out, UInt8('-')); y = -y)
    y < 1000 && push!(out, UInt8('0'))
    y < 100 && push!(out, UInt8('0'))
    y < 10 && push!(out, UInt8('0'))
    return _appendudec!(out, unsigned(y), false)
end

function _appenddate!(out::_WriteOutput, x::Date)
    y, m, d = Dates.yearmonthday(x)
    _appendyear!(out, y); push!(out, UInt8('-'))
    _append2!(out, m); push!(out, UInt8('-'))
    _append2!(out, d)
    return out
end

function _appenddatetime!(out::_WriteOutput, x::DateTime)
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

# `Timestamp{P}` prints like `string(x)`: ISO date and time, then the
# fraction of a second with its trailing zeros removed (`.12`, `.000001`,
# `.123456789`), or no fraction at all.
function _appendtimestamp!(out::_WriteOutput, x::Timestamp)
    y, m, d = Dates.yearmonthday(x)
    _appendyear!(out, y); push!(out, UInt8('-'))
    _append2!(out, m); push!(out, UInt8('-'))
    _append2!(out, d); push!(out, UInt8('T'))
    ns = Dates.value(Time(x))                      # nanoseconds of the day
    h, rest = divrem(ns, 3_600_000_000_000)
    mi, rest = divrem(rest, 60_000_000_000)
    s, frac = divrem(rest, 1_000_000_000)
    _append2!(out, h); push!(out, UInt8(':'))
    _append2!(out, mi); push!(out, UInt8(':'))
    _append2!(out, s)
    frac == 0 && return out
    nd = 9
    while frac % 10 == 0
        frac ÷= 10
        nd -= 1
    end
    push!(out, UInt8('.'))
    len = length(out)
    _room!(out, nd)
    @inbounds for k in nd:-1:1
        frac, digit = divrem(frac, 10)
        out[len + k] = UInt8('0') + digit
    end
    return out
end

const _TRUE = codeunits("true"); const _FALSE = codeunits("false")
_boolbyte(b::UInt8) = b in (UInt8('t'), UInt8('r'), UInt8('u'), UInt8('e'),
                                    UInt8('f'), UInt8('a'), UInt8('l'), UInt8('s'))

# --- per-column staged loops (specialized on the column type) ------------------
# Each renders cells lo..hi of `col`; the loop body is monomorphic, so the
# `x === missing` split is static for Union columns.

_stagecell!(st::ColStage, x, o::WriteOpts) = _appendcell!(st.bytes, x, o)

# The one cell renderer: every writer path (blocks, RowWriter, headers) appends
# through it, so quoting and value formatting cannot drift between paths.
@inline function _appendcell!(out::_WriteOutput, x, o::WriteOpts)
    if x === missing
        _appendbytes!(out, o.missingstring, o, false)
    elseif x === nothing
        _nothingerror()
    elseif x isa AbstractString
        _appendstring!(out, x, o)
    elseif x isa AbstractFloat
        if o.floatfmt !== nothing
            _appendformatted!(out, o.floatfmt, x, o)
        elseif x isa Union{Float64, Float32, Float16} && !any(_numericsyntax, (o.delim, o.oq, o.cq))
            _appendfloat!(out, x, o)
        else
            s = string(x)
            o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
            _appendscalar!(out, s, o)
        end
    elseif x isa Dates.TimeType
        if o.dateformat === nothing && x isa Union{Date, DateTime, Timestamp}
            if !any(_datesyntax, (o.delim, o.oq, o.cq))
                x isa Date ? _appenddate!(out, x) :
                x isa DateTime ? _appenddatetime!(out, x) : _appendtimestamp!(out, x)
            else
                # a dialect whose delimiter or quote is date syntax: render
                # with the same digits, then quote through the scalar path
                # (never `string(x)`: Dates' printer is not trim-safe)
                tmp = UInt8[]
                x isa Date ? _appenddate!(tmp, x) :
                x isa DateTime ? _appenddatetime!(tmp, x) : _appendtimestamp!(tmp, x)
                _appendbytes!(out, tmp, o, false)
            end
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
        # DataDecimals and other numeric scalars print through Base; the
        # decimal separator applies to their radix point as it does to floats
        s = string(x)
        o.decimal == UInt8('.') || (s = replace(s, '.' => Char(o.decimal)))
        _appendscalar!(out, s, o)
    else
        _appendstring!(out, string(x), o)
    end
    return
end

# `floatformat`: Printf renders straight into the output (no String per cell),
# the decimal separator is patched in place, and the rare structural byte in a
# rendering (an exotic dialect) re-appends it through the quoting policy.
function _appendformatted!(out::_WriteOutput, fmt::Printf.Format, x::AbstractFloat, o::WriteOpts)
    start = length(out)
    need = Printf.computelen(fmt.substringranges, fmt.formats, (x,))
    resize!(out, start + need)
    buf = out isa _WriteBuffer ? out.bytes : out
    pos = Printf.format(buf, start + 1, fmt, x)
    n = pos - 1 - start
    resize!(out, start + n)
    if o.decimal != UInt8('.')
        @inbounds for k in (start + 1):(start + n)
            buf[k] == UInt8('.') && (buf[k] = o.decimal)
        end
    end
    GC.@preserve buf begin
        _needsquotebytes(o, pointer(buf, start + 1), n) || return
    end
    rendered = buf[(start + 1):(start + n)]
    resize!(out, start)
    _appendbytes!(out, rendered, o, false)
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

# Keep the renderer's temporary storage independent of the total output size.
# The row cap protects tiny rows from task overhead. The byte target protects
# large rows from multiplying `bufsize` by 4096 for every live task. Wide-table
# staging may use about twice the rendered-byte target (stages plus output).
const WRITE_BLOCK_ROWS = 4096
const WRITE_BLOCK_BYTES = 8 << 20

function _encodedbound(n::Int, cap::Int)
    n > (cap - 2) >> 1 && return cap
    return min(2n + 2, cap) # every source byte escaped, plus quote pair
end

# `@noinline`: the typed prelude calls this once per column of a new schema;
# inlining it there would re-generate its body per column.
@noinline function _columncellbound(col::AbstractVector, o::WriteOpts)
    E = eltype(col)
    bound = Missing <: E ? _encodedbound(length(o.missingstring), o.bufsize) : 0
    T = Base.nonmissingtype(E)
    T === Union{} && return bound
    valuebound = if T <: AbstractString
        n = 0
        @inbounds for x in col
            x === missing || (n = max(n, ncodeunits(x)))
        end
        _encodedbound(n, o.bufsize)
    elseif T <: Bool
        _encodedbound(5, o.bufsize)
    elseif T <: Integer && isbitstype(T)
        _encodedbound(3sizeof(T) + 3, o.bufsize)
    elseif T <: Union{Float16, Float32, Float64} && o.floatfmt === nothing
        _encodedbound(32, o.bufsize)
    elseif T <: Dates.TimeType && o.dateformat === nothing
        _encodedbound(64, o.bufsize)
    else
        # Any/custom values and custom format strings can produce up to the
        # enforced row cap. Use that cap rather than guessing from a sample.
        o.bufsize
    end
    return max(bound, valuebound)
end

# `rowbound` is the byte bound of one rendered row, saturated at `bufsize`.
function _writerblockrows(rowbound::Int, o::WriteOpts, transform)
    transform === _identity_transform ||
        return min(WRITE_BLOCK_ROWS, max(1, WRITE_BLOCK_BYTES ÷ o.bufsize))
    return min(WRITE_BLOCK_ROWS, max(1, WRITE_BLOCK_BYTES ÷ clamp(rowbound, 1, o.bufsize)))
end

_accumulatebound(rowbound::Int, cellbound::Int, cap::Int) =
    rowbound > cap - cellbound ? cap : rowbound + cellbound

# A fixed descriptor separates the column's type from the table's schema.
# Only the field selected by `tag` is read. The other fields share empty
# vectors. Cell branches remain static; each column dispatches once during
# preparation. Fallback columns also dispatch once per rendered block.
struct _WriteColumn
    tag::UInt8
    ints::Vector{Int64}
    floats::Vector{Float64}
    strings::Vector{String}
    missingints::Vector{Union{Missing, Int64}}
    bools::Vector{Bool}
    dates::Vector{Date}
    datetimes::Vector{DateTime}
    missingfloats::Vector{Union{Missing, Float64}}
    missingstrings::Vector{Union{Missing, String}}
    missingbools::Vector{Union{Missing, Bool}}
    missingdates::Vector{Union{Missing, Date}}
    missingdatetimes::Vector{Union{Missing, DateTime}}
    datatext::DataStringVector{DataString}
    missingdatatext::DataStringVector{Union{Missing, DataString}}
    timestamps::Vector{Timestamp{Dates.Nanosecond}}
    missingtimestamps::Vector{Union{Missing, Timestamp{Dates.Nanosecond}}}
    pooled::PooledVector{String, UInt32, Vector{UInt32}}                   # pool=true output
    missingpooled::PooledVector{Union{Missing, String}, UInt32, Vector{UInt32}}
    stage::ColStage
end

const _EMPTY_WRITECOLUMNS = (
    Vector{Int64}(),
    Vector{Float64}(),
    Vector{String}(),
    Vector{Union{Missing, Int64}}(),
    Vector{Bool}(),
    Vector{Date}(),
    Vector{DateTime}(),
    Vector{Union{Missing, Float64}}(),
    Vector{Union{Missing, String}}(),
    Vector{Union{Missing, Bool}}(),
    Vector{Union{Missing, Date}}(),
    Vector{Union{Missing, DateTime}}(),
    DataStringVector{DataString}(DataStringPayload[], Vector{UInt8}[]),
    DataStringVector{Union{Missing, DataString}}(DataStringPayload[], Vector{UInt8}[]),
    Vector{Timestamp{Dates.Nanosecond}}(),
    Vector{Union{Missing, Timestamp{Dates.Nanosecond}}}(),
    PooledArray(String[]),
    PooledArray(Union{Missing, String}[]),
)
const _EMPTY_WRITESTAGE = ColStage()
# These methods are generated once for a fixed set of column types, never
# for a table schema or a column count.
# `@noinline`: one compiled method per column type, called (not expanded)
# once per column when a new schema is prepared.
for (tag, emptycol) in enumerate(_EMPTY_WRITECOLUMNS)
    args = Any[:(_EMPTY_WRITECOLUMNS[$j]) for j in eachindex(_EMPTY_WRITECOLUMNS)]
    args[tag] = :col
    @eval @noinline _preparewritecolumn(col::$(typeof(emptycol))) =
        _WriteColumn($(UInt8(tag)), $(args...), _EMPTY_WRITESTAGE)
end

@noinline _preparewritecolumn(col::AbstractVector) =
    _WriteColumn(0x00, _EMPTY_WRITECOLUMNS..., _EMPTY_WRITESTAGE)
_isdirect(::Type{<:AbstractVector}) = false
for emptycol in _EMPTY_WRITECOLUMNS
    @eval _isdirect(::Type{$(typeof(emptycol))}) = true
end
# Narrow integer columns widen once (exactly) into the Int64 descriptors:
# one pass over the column instead of the per-block staging fallback, which
# renders each cell twice. UInt64 can exceed Int64 and keeps the fallback.
for S in (Int8, Int16, Int32, UInt8, UInt16, UInt32)
    @eval @noinline _preparewritecolumn(col::Vector{$S}) =
        _WriteColumn(0x01, convert(Vector{Int64}, col), _EMPTY_WRITECOLUMNS[2:end]...,
                     _EMPTY_WRITESTAGE)
    @eval @noinline _preparewritecolumn(col::Vector{Union{Missing, $S}}) =
        _WriteColumn(0x04, _EMPTY_WRITECOLUMNS[1:3]..., convert(Vector{Union{Missing, Int64}}, col),
                     _EMPTY_WRITECOLUMNS[5:end]..., _EMPTY_WRITESTAGE)
    @eval _isdirect(::Type{Vector{$S}}) = true
    @eval _isdirect(::Type{Vector{Union{Missing, $S}}}) = true
end

# A fallback column (pooled, InlineString, narrow integer, custom scalar, ...)
# stages once per block through a call that is typed on that column only.
struct _Stager{V <: AbstractVector}
    col::V
end
# `stagers` holds one typed stager per fallback column, in `fallback` order;
# it is the empty tuple for every table of common column types, so the
# descriptor type (and everything compiled for it) is shared across schemas.
struct _WriterColumns{P <: Tuple}
    original::Vector{AbstractVector}   # the transform path reads cells here
    direct::Vector{_WriteColumn}
    fallback::Vector{Int}
    stagers::P
end

_fallbackindices(direct::Vector{_WriteColumn}) = findall(col -> col.tag == 0x00, direct)
@noinline _lengthmismatch() =
    throw(ArgumentError("all table columns must have the same length"))

# One step per column, compiled once per column type: check the length,
# store the descriptor, and return the column's cell byte bound. `@noinline`
# keeps the descriptor stores (sixteen reference fields) out of the callers.
@noinline function _setdescriptor!(direct::Vector{_WriteColumn}, original::Vector{AbstractVector},
                                   i::Int, col::AbstractVector, nrows::Int, o::WriteOpts)
    length(col) == nrows || _lengthmismatch()
    @inbounds direct[i] = _preparewritecolumn(col)
    @inbounds original[i] = col
    return _columncellbound(col, o)
end

# Descriptors plus the row byte bound. The NamedTuple form is generated
# straight-line code with constant tuple indices: one static call per column,
# no tuple recursion or splatting, so a new schema costs little inference and
# a trimmed binary can resolve every call. Fallback stagers are selected from
# the column types when the code is generated.
@generated function _preparewritecolumns(cols::Tuple, nrows::Int, o::WriteOpts)
    n = length(cols.parameters)
    ex = quote
        direct = Vector{_WriteColumn}(undef, $n)
        original = Vector{AbstractVector}(undef, $n)
        rowbound = length(o.newline) + max($n - 1, 0)
    end
    for i in 1:n
        push!(ex.args, :(rowbound = _accumulatebound(
            rowbound, _setdescriptor!(direct, original, $i, cols[$i], nrows, o), o.bufsize)))
    end
    stagers = Expr(:tuple, (:(_Stager(cols[$i])) for i in 1:n
                            if !_isdirect(cols.parameters[i]))...)
    push!(ex.args, :(return _WriterColumns(original, direct, _fallbackindices(direct), $stagers),
                     rowbound))
    return ex
end

function _preparewritecolumns(cols::Vector{AbstractVector}, nrows::Int, o::WriteOpts)
    n = length(cols)
    direct = Vector{_WriteColumn}(undef, n)
    original = Vector{AbstractVector}(undef, n)
    rowbound = length(o.newline) + max(n - 1, 0)
    for i in 1:n
        rowbound = _accumulatebound(rowbound, _setdescriptor!(direct, original, i, cols[i], nrows, o),
                                    o.bufsize)
    end
    fallback = _fallbackindices(direct)
    stagers = Tuple(_Stager(cols[j]) for j in fallback)
    return _WriterColumns(original, direct, fallback, stagers), rowbound
end

@inline function _appendtaggedcell!(out::_WriteOutput, col::_WriteColumn, r::Int, k::Int, o::WriteOpts)
    if col.tag == 0x01
        @inbounds _appendcell!(out, col.ints[r], o)
    elseif col.tag == 0x02
        @inbounds _appendcell!(out, col.floats[r], o)
    elseif col.tag == 0x03
        @inbounds _appendcell!(out, col.strings[r], o)
    elseif col.tag == 0x04
        @inbounds _appendcell!(out, col.missingints[r], o)
    elseif col.tag == 0x05
        @inbounds _appendcell!(out, col.bools[r], o)
    elseif col.tag == 0x06
        @inbounds _appendcell!(out, col.dates[r], o)
    elseif col.tag == 0x07
        @inbounds _appendcell!(out, col.datetimes[r], o)
    elseif col.tag == 0x08
        @inbounds _appendcell!(out, col.missingfloats[r], o)
    elseif col.tag == 0x09
        @inbounds _appendcell!(out, col.missingstrings[r], o)
    elseif col.tag == 0x0a
        @inbounds _appendcell!(out, col.missingbools[r], o)
    elseif col.tag == 0x0b
        @inbounds _appendcell!(out, col.missingdates[r], o)
    elseif col.tag == 0x0c
        @inbounds _appendcell!(out, col.missingdatetimes[r], o)
    elseif col.tag == 0x0d
        @inbounds _appendcell!(out, col.datatext[r], o)
    elseif col.tag == 0x0e
        @inbounds _appendcell!(out, col.missingdatatext[r], o)
    elseif col.tag == 0x0f
        @inbounds _appendcell!(out, col.timestamps[r], o)
    elseif col.tag == 0x10
        @inbounds _appendcell!(out, col.missingtimestamps[r], o)
    elseif col.tag == 0x11
        @inbounds _appendcell!(out, col.pooled[r], o)
    elseif col.tag == 0x12
        @inbounds _appendcell!(out, col.missingpooled[r], o)
    else
        st = col.stage
        @inbounds s = k == 1 ? 1 : st.ends[k - 1] + 1
        @inbounds n = st.ends[k] - s + 1
        len = length(out)
        _room!(out, n)
        GC.@preserve out st unsafe_copyto!(pointer(out, len + 1), pointer(st.bytes, s), n)
    end
    return
end

_renderblock(cols::Vector{AbstractVector}, lo::Int, hi::Int, o::WriteOpts) =
    _renderblock(_preparewritecolumns(cols, length(cols[1]), o)[1], lo, hi, o)

# One sink write per rendered block, straight from the block buffer.
function _writeblock(io, b::_WriteBuffer)
    GC.@preserve b unsafe_write(io, pointer(b.bytes), UInt(b.len))
    return
end

# An IOBuffer sink grows in small steps on every block write (Base's
# overallocation policy copied about three output sizes on a 69 MiB write).
# Reserve the estimated total once, from the first rendered block. Other sinks
# manage their own buffering.
_reservesink!(io, blockbytes::Int, nblocks::Int) = nothing
function _reservesink!(io::Base.GenericIOBuffer, blockbytes::Int, nblocks::Int)
    io.writable || return nothing
    est, overflow = Base.mul_with_overflow(blockbytes, nblocks)
    (overflow || est <= 0) && return nothing
    Base.ensureroom(io, UInt(est))
    return nothing
end

@inline function _writerow_direct!(out::_WriteBuffer, r::Int, k::Int,
                                   cols::Vector{_WriteColumn}, o::WriteOpts)
    start = length(out)
    ncols = length(cols)
    @inbounds for j in 1:ncols
        @inline _appendtaggedcell!(out, cols[j], r, k, o)
        j < ncols && _appenddelim!(out, o)
    end
    for b in o.newline
        push!(out, b)
    end
    rowsize = length(out) - start
    rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
    return
end

# Render rows lo..hi into `out` (a buffer the caller reuses from block to
# block), after an optional prefix (the header, when a block must carry it).
function _renderblock(cols::_WriterColumns, lo::Int, hi::Int, o::WriteOpts,
                             out::_WriteBuffer=_WriteBuffer(),
                             prefix::Vector{UInt8}=EMPTY_BYTES)
    prepared = cols.direct
    if !isempty(cols.fallback)
        prepared = copy(cols.direct)
        stages = map(s -> _stagecolumn!(ColStage(), s.col, lo, hi, o), cols.stagers)
        for (i, j) in enumerate(cols.fallback)
            @inbounds prepared[j] = _WriteColumn(0x00, _EMPTY_WRITECOLUMNS..., stages[i])
        end
    end
    resize!(out, 0)
    isempty(prefix) || append!(out, prefix)
    nrows = hi - lo + 1
    probe = min(nrows, 32)
    @inbounds for r in lo:(lo + probe - 1)
        @inline _writerow_direct!(out, r, r - lo + 1, prepared, o)
    end
    if probe < nrows
        est = (length(out) * nrows) ÷ probe
        sizehint!(out, est + (est >> 3) + 64 * length(prepared))
        @inbounds for r in (lo + probe):hi
            @inline _writerow_direct!(out, r, r - lo + 1, prepared, o)
        end
    end
    return out
end

# Compatibility path for `transform`: callbacks are observable and may keep
# state, so preserve the row-major, sequential call order even for wide
# tables. Cells still read through the typed descriptors, so a type-stable
# transform renders without a dynamic dispatch per cell.
const _WRITECOLUMN_FIELDS = (:ints, :floats, :strings, :missingints, :bools, :dates,
                             :datetimes, :missingfloats, :missingstrings, :missingbools,
                             :missingdates, :missingdatetimes, :datatext, :missingdatatext,
                             :timestamps, :missingtimestamps, :pooled, :missingpooled)
let ex = :(_appendcell!(out, transform(j, orig[r]), o))
    # widened narrow-integer columns: the transform sees the original element
    # type through a typed branch rather than the dynamic fallback
    for S in (Int8, Int16, Int32, UInt8, UInt16, UInt32),
        V in (Vector{S}, Vector{Union{Missing, S}})
        ex = :(if orig isa $V
                   @inbounds _appendcell!(out, transform(j, orig[r]), o)
               else
                   $ex
               end)
    end
    for (tag, f) in Iterators.reverse(collect(enumerate(_WRITECOLUMN_FIELDS)))
        # a descriptor field aliases the table's own column; a widened
        # narrow-integer copy does not, and its transform must still see the
        # original element type
        ex = :(if col.tag == $(UInt8(tag)) && col.$f === orig
                   @inbounds _appendcell!(out, transform(j, col.$f[r]), o)
               else
                   $ex
               end)
    end
    # `orig` is only compared by identity (and indexed on the fallback path),
    # so it stays unspecialized: the call from the row loop is then static.
    # The descriptor is loaded here, by index: this function is too large to
    # inline, and a `_WriteColumn` (twenty references, stored inline in its
    # vector) passed by value to a non-inlined call would be boxed per cell.
    @eval function _appendtransformed!(out::_WriteOutput, direct::Vector{_WriteColumn}, j::Int,
                                       @nospecialize(orig), r::Int, o::WriteOpts, transform)
        @inbounds col = direct[j]
        $ex
        return
    end
end

# `transform::F`: a function argument that is only passed on (never called
# here) is otherwise compiled as an abstract `Function`, and every cell
# would dispatch dynamically.
function _renderblock_transformed(cols::_WriterColumns, lo::Int, hi::Int, o::WriteOpts,
                                  transform::F, out::_WriteBuffer=_WriteBuffer()) where {F}
    direct = cols.direct
    original = cols.original
    ncols = length(direct)
    resize!(out, 0)
    @inbounds for r in lo:hi
        start = length(out)
        for j in 1:ncols
            _appendtransformed!(out, direct, j, original[j], r, o, transform)
            j < ncols && _appenddelim!(out, o)
        end
        for b in o.newline
            push!(out, b)
        end
        rowsize = length(out) - start
        rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
    end
    return out
end

# A rendering task reports its exception as data. This lets the consumer wait
# for every already-started task before it rethrows the original exception
# type, instead of leaking a TaskFailedException or leaving background work
# running after `CSV.write` returns.
struct _RenderFailure
    exception
    backtrace
    block::Int
end

@inline function _capture_render(renderblock, block::Int, slot::Int)
    try
        return renderblock(block, slot)
    catch err
        return _RenderFailure(err, catch_backtrace(), block)
    end
end

@noinline function _throw_render_failure(failure::_RenderFailure)
    # Julia has no public API for attaching an arbitrary task backtrace while
    # rethrowing the original exception type. Keep that type and object for
    # compatibility, and retain the render backtrace in debug diagnostics.
    @debug "CSV writer block rendering failed" block=failure.block exception=(
        failure.exception, failure.backtrace)
    throw(failure.exception)
end

"""
    _ordered_parallel_blocks!(emitblock, renderblock, nblocks, ntasks)

Render numbered blocks in parallel and pass them to `emitblock` in increasing
order. The ring contains at most `min(nblocks, ntasks)` tasks, so completed
blocks waiting for an earlier block cannot grow with `nblocks`. `renderblock`
receives the block and its ring slot (1 to `min(nblocks, ntasks)`): a slot's
blocks render one after another, and each is emitted before the slot starts
another, so a renderer can reuse one buffer per slot.
"""
function _ordered_parallel_blocks!(emitblock, renderblock,
                                   nblocks::Int, ntasks::Int)
    nblocks == 0 && return
    window = min(nblocks, ntasks)
    tasks = Union{Nothing, Task}[nothing for _ in 1:window]
    nextblock = 1
    try
        for slot in 1:window
            block = nextblock
            tasks[slot] = @wkspawn _capture_render($renderblock, $block, $slot)
            nextblock += 1
        end
        for block in 1:nblocks
            slot = mod1(block, window)
            task = tasks[slot]::Task
            rendered = fetch(task)
            # Drop the task's reference to its result before emission. After
            # emission, drop the local reference before starting a replacement
            # task. Thus the high-water mark stays at `window` blocks.
            tasks[slot] = nothing
            task = nothing
            rendered isa _RenderFailure && _throw_render_failure(rendered)
            emitblock(rendered)
            rendered = nothing
            if nextblock <= nblocks
                queued = nextblock
                tasks[slot] = @wkspawn _capture_render($renderblock, $queued, $slot)
                nextblock += 1
            end
        end
    finally
        # Rendering catches ordinary exceptions, so these waits do not replace
        # a sink or ordered-render exception. They only ensure no work escapes
        # the lifetime of this call.
        for task in tasks
            task === nothing || wait(task)
        end
    end
    return
end

@inline function _capture_item(f, item, index::Int)
    try
        f(item, index)
        return nothing
    catch err
        return _RenderFailure(err, catch_backtrace(), index)
    end
end

"""
    _bounded_foreach!(f, iter, ntasks) -> count

Apply `f(item, index)` to a possibly one-shot, size-unknown iterator with at
most `min(ntasks, Threads.nthreads())` tasks live. Items are pulled only as a
task slot becomes available. On failure, wait for every started task and throw
the original exception object.
"""
function _bounded_foreach!(f, iter, ntasks::Int)
    workers = min(ntasks, Threads.nthreads())
    tasks = Union{Nothing, Task}[nothing for _ in 1:workers]
    state = iterate(iter)
    state === nothing && return 0
    nextindex = 1
    pending = 0
    try
        for slot in 1:workers
            state === nothing && break
            item, iterstate = state
            index = nextindex
            tasks[slot] = @wkspawn _capture_item($f, $item, $index)
            nextindex += 1
            pending += 1
            state = iterate(iter, iterstate)
        end
        completed = 0
        slot = 1
        while pending > 0
            while tasks[slot] === nothing
                slot = mod1(slot + 1, workers)
            end
            result = fetch(tasks[slot]::Task)
            tasks[slot] = nothing
            pending -= 1
            result isa _RenderFailure && _throw_render_failure(result)
            completed += 1
            if state !== nothing
                item, iterstate = state
                index = nextindex
                tasks[slot] = @wkspawn _capture_item($f, $item, $index)
                nextindex += 1
                pending += 1
                state = iterate(iter, iterstate)
            end
            slot = mod1(slot + 1, workers)
        end
        return completed
    finally
        for task in tasks
            task === nothing || wait(task)
        end
    end
end

@noinline function _renderwrite_identity!(io, cols, lo::Int, hi::Int, o::WriteOpts,
                                          out::_WriteBuffer)
    _writeblock(io, _renderblock(cols, lo, hi, o, out))
    return
end

@noinline function _renderwrite_transformed!(io, cols, lo::Int, hi::Int,
                                             o::WriteOpts, transform::F,
                                             out::_WriteBuffer) where {F}
    _writeblock(io, _renderblock_transformed(cols, lo, hi, o, transform, out))
    return
end

Base.@constprop :aggressive function _emitrowblocks!(io, cols::_WriterColumns, nrows::Int,
                         blockrows::Int, o::WriteOpts, transform::F, ntasks::Int,
                         ::Val{SERIAL}) where {F, SERIAL}
    nrows == 0 && return
    nblocks = cld(nrows, blockrows)
    workers = SERIAL ? 1 : min(ntasks, Threads.nthreads())
    bounds(block) = ((block - 1) * blockrows + 1,
                     min(block * blockrows, nrows))

    # Transform callbacks are observable and can retain state. Run their
    # fixed-size blocks sequentially to preserve global row-major call order.
    if transform !== _identity_transform
        out = _WriteBuffer()
        for block in 1:nblocks
            lo, hi = bounds(block)
            _renderwrite_transformed!(io, cols, lo, hi, o, transform, out)
            block == 1 && _reservesink!(io, out.len, nblocks)
        end
        return
    end

    # Avoid task overhead when the caller requested one task or the table fits
    # in one block. Fixed-size blocks still bound the single-task path.
    if SERIAL || workers == 1 || nblocks == 1
        out = _WriteBuffer()
        for block in 1:nblocks
            lo, hi = bounds(block)
            _renderwrite_identity!(io, cols, lo, hi, o, out)
            block == 1 && _reservesink!(io, out.len, nblocks)
        end
        return
    end

    # Each block renders into a fresh buffer: a buffer reused across ring
    # slots is written on one core, read by the emitting task, then written on
    # another core, and pays for its cache lines' ownership each time. The
    # ordered scheduler bounds the live buffers to the ring size.
    renderblock = function (block, slot)
        lo, hi = bounds(block)
        return _renderblock(cols, lo, hi, o)
    end
    reserved = Ref(false)
    emitblock = function (rendered)
        if !reserved[]
            reserved[] = true
            _reservesink!(io, rendered.len, nblocks)
        end
        _writeblock(io, rendered)
    end
    _ordered_parallel_blocks!(emitblock, renderblock, nblocks, workers)
    return
end

# Parallel gzip: each row block compresses inside its render task into a
# complete gzip member, and the members concatenate in block order. A gzip
# stream is a sequence of members (RFC 1952 §2.2): gunzip, zlib-based
# libraries, and CSV's own reader all read it back as one byte stream.
function _emitmembers!(io, cols::_WriterColumns, nrows::Int, blockrows::Int, o::WriteOpts,
                       ntasks::Int, prefix::Vector{UInt8})
    nblocks = max(cld(nrows, blockrows), 1)   # an empty table still emits its prefix
    workers = min(ntasks, Threads.nthreads(), nblocks)
    codecs = [GzipCompressor() for _ in 1:workers]   # one reusable compressor per ring slot
    foreach(CodecZlib.TranscodingStreams.initialize, codecs)
    try
        renderblock = function (block, slot)
            lo = (block - 1) * blockrows + 1
            hi = min(block * blockrows, nrows)
            out = _renderblock(cols, lo, hi, o, _WriteBuffer(), block == 1 ? prefix : EMPTY_BYTES)
            resize!(out.bytes, out.len)
            return transcode(codecs[slot], out.bytes)
        end
        emitblock = member -> Base.write(io, member)
        _ordered_parallel_blocks!(emitblock, renderblock, nblocks, workers)
    finally
        foreach(CodecZlib.TranscodingStreams.finalize, codecs)
    end
    return
end

# Some TranscodingStreams releases admitted by CodecZlib 0.7 close a
# compressor's wrapped stream even when `stop_on_end=true`, so protect
# caller-owned sinks rather than depend on a transitive version. Sink
# failures still pass through unchanged.
struct _NonClosingIO{T <: IO} <: IO
    io::T
end

Base.isopen(io::_NonClosingIO) = isopen(io.io)
Base.isreadable(io::_NonClosingIO) = isreadable(io.io)
Base.iswritable(io::_NonClosingIO) = iswritable(io.io)
Base.unsafe_read(io::_NonClosingIO, p::Ptr{UInt8}, n::UInt) =
    Base.unsafe_read(io.io, p, n)
Base.unsafe_write(io::_NonClosingIO, p::Ptr{UInt8}, n::UInt) =
    Base.unsafe_write(io.io, p, n)
Base.flush(io::_NonClosingIO) = flush(io.io)
Base.close(::_NonClosingIO) = nothing

function _emitgzip!(emitpayload, io)
    # Closing in `finally` finalizes a valid partial gzip member after a render
    # error. The proxy makes closing safe on every supported CodecZlib stack.
    gz = GzipCompressorStream(_NonClosingIO(io); stop_on_end=true)
    payload_complete = false
    try
        Base.write(gz) # initialize a valid member even for an empty payload
        emitpayload(gz)
        payload_complete = true
    finally
        try
            close(gz)
            payload_complete && flush(io)
        catch
            # A cleanup failure must not replace the render or sink exception.
            # When payload emission succeeded, cleanup is the primary failure.
            payload_complete && rethrow()
        end
    end
    return
end

function _renderheader(names, o::WriteOpts)
    out = UInt8[]
    for (j, nm) in enumerate(names)
        _appendstring!(out, String(nm), o)
        j < length(names) && _appenddelim!(out, o)
    end
    append!(out, o.newline)
    length(out) <= o.bufsize || _rowtoolarge(length(out), o.bufsize)
    return out
end

function _headeroptions(source_names, header, writeheader, defaultheader::Bool)
    if header isa Bool
        writeheader !== nothing && writeheader != header &&
            throw(ArgumentError("header=$header conflicts with writeheader=$writeheader"))
        names = something(source_names, Symbol[])
        return names, something(writeheader, header)
    elseif header === nothing
        return something(source_names, Symbol[]), something(writeheader, defaultheader)
    elseif header isa AbstractVector
        names = isempty(header) ? something(source_names, Symbol[]) : Symbol.(header)
        source_names !== nothing && length(names) != length(source_names) &&
            throw(ArgumentError("header has $(length(names)) names for " *
                                "$(length(source_names)) columns"))
        return names, something(writeheader, defaultheader)
    end
    throw(ArgumentError("header must be true, false, or a vector of column names"))
end

_identity_transform(::Int, value) = value

# --- RowWriter: the row-string iterator ---------------------------------------

struct RowWriter{R, I, O <: WriteOpts, F, P}
    rows::R
    initial::I
    names::Vector{Symbol}
    o::O
    writeheader::Bool
    transform::F
end

function _rowwriter(table, o::WriteOpts;
                    writeheader::Union{Nothing, Bool}=nothing,
                    header::Union{Nothing, Bool, AbstractVector}=nothing,
                    transform::Function=_identity_transform,
                    defaultheader::Bool=true)
    rows = Tables.rows(table)
    sch = Tables.schema(rows)
    prefetched = sch === nothing
    initial = prefetched ? iterate(rows) : nothing
    source_names = sch === nothing ?
                   initial === nothing ? nothing :
                   collect(Symbol, Tables.columnnames(initial[1])) :
                   collect(Symbol, sch.names)
    names, wantheader = _headeroptions(source_names, header, writeheader, defaultheader)
    return RowWriter{typeof(rows), typeof(initial), typeof(o), typeof(transform), prefetched}(
        rows, initial, names, o, wantheader, transform)
end

function RowWriter(table; writeheader::Union{Nothing, Bool}=nothing,
                   header::Union{Nothing, Bool, AbstractVector}=nothing,
                   transform::Function=_identity_transform,
                   bufsize::Integer=1 << 22, kw...)
    _checkwritekwargs(kw)
    return _rowwriter(table, _writeopts(; bufsize, kw...);
                      writeheader, header, transform)
end

_rowwritersize(::Base.HasLength) = Base.HasLength()
_rowwritersize(::Base.HasShape) = Base.HasLength()
_rowwritersize(::Base.IsInfinite) = Base.IsInfinite()
_rowwritersize(::Base.SizeUnknown) = Base.SizeUnknown()
Base.IteratorSize(::Type{<:RowWriter{R, I, O, F, true}}) where {R, I, O, F} =
    Base.SizeUnknown()
Base.IteratorSize(::Type{<:RowWriter{R, I, O, F, false}}) where {R, I, O, F} =
    _rowwritersize(Base.IteratorSize(R))
Base.eltype(::Type{<:RowWriter}) = String
function Base.length(rw::RowWriter{R, I, O, F, false}) where {R, I, O, F}
    nrows = length(rw.rows)
    hasheader = rw.writeheader && !isempty(rw.names)
    bomonly = rw.o.bom && nrows == 0 && !hasheader
    return nrows + hasheader + bomonly
end

Base.size(rw::RowWriter{R, I, O, F, false}) where {R, I, O, F} = (length(rw),)

# Append one Tables.jl row to `out` through the shared cell renderer.
function _appendrow!(out::Vector{UInt8}, row, ncols::Int, o::WriteOpts, transform::F) where {F}
    start = length(out)
    for j in 1:ncols
        _appendcell!(out, transform(j, Tables.getcolumn(row, j)), o)
        j < ncols && _appenddelim!(out, o)
    end
    append!(out, o.newline)
    rowsize = length(out) - start
    rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
    return out
end

# With a known schema each cell reads through a static
# `Tables.getcolumn(row, T, i, name)` call (Tables unrolls the columns), so a
# NamedTuple row renders without a dynamic dispatch per cell.
function _appendrow!(out::Vector{UInt8}, row, sch::Tables.Schema, ncols::Int, o::WriteOpts,
                     transform::F) where {F}
    start = length(out)
    Tables.eachcolumn(sch, row) do val, j, nm
        _appendcell!(out, transform(j, val), o)
        j < ncols && _appenddelim!(out, o)
    end
    append!(out, o.newline)
    rowsize = length(out) - start
    rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
    return out
end

_appendrow!(out::Vector{UInt8}, row, ::Nothing, ncols::Int, o::WriteOpts, transform::F) where {F} =
    _appendrow!(out, row, ncols, o, transform)

_renderrow(row, names, o::WriteOpts, transform) =
    String(_appendrow!(UInt8[], row, length(names), o, transform))

function Base.iterate(rw::RowWriter, state=nothing)
    if state === nothing
        it = rw isa RowWriter{<:Any, <:Any, <:Any, <:Any, true} ? rw.initial : iterate(rw.rows)
        if rw.writeheader && !isempty(rw.names)
            line = String(_renderheader(rw.names, rw.o))
            rw.o.bom && (line = string('\ufeff', line))
            return line, (it,)
        elseif rw.o.bom
            it === nothing && return "\ufeff", (nothing,)
            row, rstate = it
            return string('\ufeff', _renderrow(row, rw.names, rw.o, rw.transform)),
                   (iterate(rw.rows, rstate),)
        end
        state = (it,)
    end
    it = state[1]
    it === nothing && return nothing
    row, rstate = it
    return _renderrow(row, rw.names, rw.o, rw.transform), (iterate(rw.rows, rstate),)
end

# Row sources render into one reusable block buffer that flushes to the sink at
# the block byte target: one sink write per block, not per row.
function _emitrows!(io, rw::RowWriter; bom::Bool=false)
    bom && Base.write(io, UInt8[0xef, 0xbb, 0xbf])
    out = UInt8[]
    rw.writeheader && !isempty(rw.names) && append!(out, _renderheader(rw.names, rw.o))
    ncols = length(rw.names)
    it = rw isa RowWriter{<:Any, <:Any, <:Any, <:Any, true} ? rw.initial : iterate(rw.rows)
    # the loop specializes on the schema (a function barrier): known column
    # types make every cell read static, an unknown schema stays dynamic
    _emitrowsloop!(io, out, rw, it, Tables.schema(rw.rows), ncols)
    return
end

function _emitrowsloop!(io, out::Vector{UInt8}, rw::RowWriter, it, sch, ncols::Int)
    complete = length(out)
    try
        while it !== nothing
            row, state = it
            _appendrow!(out, row, sch, ncols, rw.o, rw.transform)
            complete = length(out)
            if complete >= WRITE_BLOCK_BYTES
                # A sink failure may have written part of the block. Do not retry it.
                complete = 0
                Base.write(io, out)
                empty!(out)
            end
            it = iterate(rw.rows, state)
        end
    catch
        # Preserve complete rows on both render and iterator failures. A partial
        # failing row is discarded; cleanup cannot replace the original error.
        resize!(out, complete)
        try
            isempty(out) || Base.write(io, out)
        catch
        end
        rethrow()
    end
    isempty(out) || Base.write(io, out)
    return
end

# `CSV.Chunks` is a sequence of stable-schema batches (each a `CSV.File`). Stream
# every batch under one header, rendering each batch's columns with the block
# renderer, so a chunked read can be re-written without collecting it.
function _emitchunks!(io, chunks::Chunks, o::WriteOpts, transform, ntasks::Int;
                      header, writeheader, append::Bool)
    o.bom && !append && Base.write(io, UInt8[0xef, 0xbb, 0xbf])
    source_names = names(chunks)
    headernames, wantheader = _headeroptions(source_names, header, writeheader, !append)
    wantheader && !isempty(headernames) && Base.write(io, _renderheader(headernames, o))
    for batch in chunks
        cols = _writecolumns(Tables.columns(batch), source_names)
        nrows = isempty(cols) ? 0 : length(cols[1])
        rendercols, rowbound = _preparewritecolumns(cols, nrows, o)
        _emitrowblocks!(io, rendercols, nrows, _writerblockrows(rowbound, o, transform), o,
                        transform, ntasks, Val(ntasks == 1))
    end
    return
end

# Sink and scheduler code see the descriptor container, whose type is shared
# by every table of common column types; only fallback stagers, transform,
# options, and sink behavior specialize above the renderer.
struct _ColumnEmitter{C <: _WriterColumns, O <: WriteOpts, F, S, G} <: Function
    cols::C
    nrows::Int
    blockrows::Int
    opts::O
    transform::F
    ntasks::Int
    header::Vector{UInt8}
    append::Bool
    serial::S
    gzip::G         # Val(true): compress each block in parallel as its own gzip
                    # member. A type parameter, like `serial`, so the one-task
                    # emitter never compiles the parallel scheduler (trim=safe).
end

function (emit::_ColumnEmitter)(io)
    o = emit.opts
    if emit.gzip isa Val{true}
        prefix = UInt8[]
        o.bom && !emit.append && append!(prefix, (0xef, 0xbb, 0xbf))
        append!(prefix, emit.header)
        _emitmembers!(io, emit.cols, emit.nrows, emit.blockrows, o, emit.ntasks, prefix)
        return nothing
    end
    o.bom && !emit.append && Base.write(io, UInt8[0xef, 0xbb, 0xbf])
    isempty(emit.header) || Base.write(io, emit.header)
    _emitrowblocks!(io, emit.cols, emit.nrows, emit.blockrows, o, emit.transform, emit.ntasks,
                    emit.serial)
    return nothing
end

# --- public write methods ---------------------------------------------------

# A NamedTuple keeps its column types until the descriptors below exist, so
# descriptor preparation and fallback staging resolve statically (a trimmed
# binary needs that). Other tables erase to one container. Either way the
# scheduler, the task closures, and the row loop see one descriptor type.
_writecolumns(cols::NamedTuple, names) = values(cols)
_writecolumns(cols, names) = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]

@inline function write(sink, table; append::Bool=false, writeheader::Union{Nothing, Bool}=nothing,
               header::Union{Nothing, Bool, AbstractVector}=nothing,
               compress::Union{Bool, Symbol}=:auto,
               partition::Bool=false,
               transform::F=_identity_transform,
               bufsize::Integer=1 << 22,
               ntasks::Int=Threads.nthreads(), kw...) where {F <: Function}
    compression = compress isa Bool ? (compress ? :gzip : :none) : compress
    compression in (:auto, :gzip, :none) ||
        throw(ArgumentError("compress must be true, false, :auto, :gzip, or :none " *
                            "(got $compress)"))
    _checkwritekwargs(kw)
    o = _writeopts(; bufsize, kw...)
    ntasks >= 1 || throw(ArgumentError("ntasks must be >= 1 (got $ntasks)"))
    if partition
        parts = Tables.partitions(table)
        pathbase = sink isa AbstractString
        sinks = pathbase ? nothing :
                sink isa AbstractVector ? sink :
                throw(ArgumentError("partition=true needs a path or a Vector of sinks"))
        partcompression = compression === :auto && pathbase && _gzpath(sink) ?
                          :gzip : compression
        writepart = function (part, i)
            if !pathbase && i > length(sinks)
                throw(ArgumentError("more partitions than sinks (sink count $(length(sinks)))"))
            end
            partsink = pathbase ? string(sink, "_", i) : sinks[i]
            write(partsink, part; append, writeheader, header,
                  compress=partcompression, partition=false, transform,
                  bufsize, ntasks=1, kw...)
            return nothing
        end
        nparts = _bounded_foreach!(writepart, parts, ntasks)
        !pathbase && nparts != length(sinks) &&
            throw(ArgumentError("partition count $nparts != sink count $(length(sinks))"))
        return pathbase ? [string(sink, "_", i) for i in 1:nparts] : sink
    end
    gzip = compression === :gzip ||
           (compression === :auto && sink isa AbstractString && _gzpath(sink))
    emitpayload = if table isa Chunks
        io -> _emitchunks!(io, table, o, transform, ntasks; header, writeheader, append)
    elseif Tables.columnaccess(typeof(table))
        cols0 = Tables.columns(table)
        source_names = collect(Symbol, Tables.columnnames(cols0))
        names, wantheader = _headeroptions(source_names, header, writeheader, !append)
        cols = _writecolumns(cols0, source_names)
        nrows = isempty(cols) ? 0 : length(cols[1])
        rendercols, rowbound = _preparewritecolumns(cols, nrows, o)
        headerblock = wantheader && !isempty(names) ? _renderheader(names, o) : EMPTY_BYTES
        blockrows = _writerblockrows(rowbound, o, transform)
        # Header and fixed-size row blocks stream directly to the sink. The
        # ordered renderer retains no more than `ntasks` blocks. With several
        # tasks, gzip compresses block by block in those tasks.
        if ntasks == 1
            _ColumnEmitter(rendercols, nrows, blockrows, o, transform, ntasks, headerblock,
                           append, Val(true), Val(false))
        elseif gzip && Threads.nthreads() > 1 && transform === _identity_transform
            _ColumnEmitter(rendercols, nrows, blockrows, o, transform, ntasks, headerblock,
                           append, Val(false), Val(true))
        else
            _ColumnEmitter(rendercols, nrows, blockrows, o, transform, ntasks, headerblock,
                           append, Val(false), Val(false))
        end
    else
        # A row source may be one-shot and may not know its schema until its
        # first row. Prefetch exactly that row and retain its iterator state.
        rw = _rowwriter(table, o; writeheader, header, transform,
                        defaultheader=!append)
        io -> _emitrows!(io, rw; bom=o.bom && !append)
    end
    emit = gzip && !(emitpayload isa _ColumnEmitter && emitpayload.gzip isa Val{true}) ?
           (io -> _emitgzip!(emitpayload, io)) : emitpayload
    if sink isa AbstractString
        open(emit, String(sink), append ? "a" : "w")
    else
        # An IO sink is written at its current position, exactly like
        # `Base.write`: the caller owns the stream, so earlier content (a log
        # already on stdout, a prefix written by the caller) is never rewound
        # over or truncated. `append` only decides whether a header is written.
        emit(sink)
    end
    return sink
end

_gzpath(sink) = endswith(lowercase(String(sink)), ".gz")

write(sink; kw...) = table -> write(sink, table; kw...)
