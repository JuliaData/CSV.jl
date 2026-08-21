
# CSV.write implementation and formatting options.
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
# The engine renders contiguous row blocks from `Tables.columns` in parallel.
# Narrow tables use a type-specialized tuple renderer. Wide tables stage each
# column once per block and then gather rows. Integers emit digits directly,
# floats use Ryu at the output position, and strings copy after one structural
# scan. Blocks stream to the sink in order. Output bytes do not depend on the
# thread count.

using Tables, Dates, Printf, CodecZlib

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
    bufsize::Int
end

function _writeopts(; delim::Union{Char, String}=',', quotechar::Char='"',
                    openquotechar::Union{Nothing, Char}=nothing,
                    closequotechar::Union{Nothing, Char}=nothing,
                    escapechar::Union{Nothing, Char}=nothing,
                    newline::Union{Char, String}='\n',
                    missingstring::AbstractString="",
                    quotestyle::Symbol=:minimal,
                    quotestrings::Bool=false,
                    floatformat::Union{Nothing, AbstractString}=nothing,
                    dateformat=nothing,
                    decimal::Char='.',
                    bom::Bool=false,
                    bufsize::Integer=1 << 22)
    bufsize >= 1 || throw(ArgumentError("bufsize must be >= 1 (got $bufsize)"))
    delim isa String && sizeof(delim) != 1 &&
        throw(ArgumentError("write delim must be a single byte (got $(repr(delim)))"))
    delim isa Char && !isascii(delim) &&
        throw(ArgumentError("write delim must be a single byte (got $(repr(delim)))"))
    quotestrings && quotestyle === :none &&
        throw(ArgumentError("quotestrings=true conflicts with quotestyle=:none"))
    quotestrings && (quotestyle = :all)
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
    intbufsize = bufsize > typemax(Int) ? typemax(Int) : Int(bufsize)
    return WriteOpts(d, oq, cq, e, Vector{UInt8}(codeunits(string(newline))),
                     Vector{UInt8}(codeunits(String(missingstring))),
                     quotestyle, ff, df, decimal % UInt8, bom, intbufsize)
end

@noinline _rowtoolarge(n::Int, cap::Int) =
    throw(ArgumentError("row size ($n) exceeds bufsize ($cap); pass a larger bufsize"))

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
    # Empty unquoted content is missing, matching the parser's pinned 1.0
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
    elseif x === nothing
        _nothingerror()
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
@inline function _append2!(out::Vector{UInt8}, v::Integer)   # two zero-padded digits, 0 ≤ v < 100
    len = length(out)
    _room!(out, 2)
    @inbounds begin
        out[len + 1] = UInt8('0') + (v ÷ 10) % UInt8
        out[len + 2] = UInt8('0') + (v % 10) % UInt8
    end
    return out
end
@inline function _appendyear!(out::Vector{UInt8}, y::Integer)
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
    elseif x === nothing
        _nothingerror()
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

# Keep the renderer's temporary storage independent of the total output size.
# The row cap protects tiny rows from task overhead. The byte target protects
# large rows from multiplying `bufsize` by 4096 for every live task. Wide-table
# staging may use about twice the rendered-byte target (stages plus output).
const WRITE_BLOCK_ROWS = 4096
const WRITE_BLOCK_BYTES = 8 << 20

@inline function _encodedbound(n::Int, cap::Int)
    n > (cap - 2) >> 1 && return cap
    return min(2n + 2, cap) # every source byte escaped, plus quote pair
end

function _columncellbound(col::AbstractVector, o::WriteOpts)
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

function _writerblockrows(cols, o::WriteOpts, transform)
    transform === _identity_transform ||
        return min(WRITE_BLOCK_ROWS, max(1, WRITE_BLOCK_BYTES ÷ o.bufsize))
    rowbound = length(o.newline) + max(length(cols) - 1, 0)
    for col in cols
        cellbound = _columncellbound(col, o)
        rowbound > o.bufsize - cellbound && (rowbound = o.bufsize; break)
        rowbound += cellbound
    end
    rowbound = clamp(rowbound, 1, o.bufsize)
    return min(WRITE_BLOCK_ROWS, max(1, WRITE_BLOCK_BYTES ÷ rowbound))
end

@inline function _writerow!(out::Vector{UInt8}, r::Int, cols::Tuple, o::WriteOpts)
    start = length(out)
    _writecells!(out, r, cols, o)
    for b in o.newline
        push!(out, b)
    end
    rowsize = length(out) - start
    rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
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
            rowstart = pos
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
            rowsize = pos - rowstart
            rowsize <= o.bufsize || _rowtoolarge(rowsize, o.bufsize)
        end
    end
    return out
end

# Compatibility path for `transform`: callbacks are observable and may keep
# state, so preserve CSV 0.10's row-major, sequential call order even for wide
# tables. This path is intentionally separate from the staged column renderer.
function _renderblock_transformed(cols, lo::Int, hi::Int, o::WriteOpts, transform)
    out = UInt8[]
    ncols = length(cols)
    @inbounds for r in lo:hi
        start = length(out)
        for j in 1:ncols
            _appendcell!(out, transform(j, cols[j][r]), o)
            j < ncols && push!(out, o.delim)
        end
        append!(out, o.newline)
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

@inline function _capture_render(renderblock, block::Int)
    try
        return renderblock(block)
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
blocks waiting for an earlier block cannot grow with `nblocks`.
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
            tasks[slot] = Threads.@spawn _capture_render($renderblock, $block)
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
                tasks[slot] = Threads.@spawn _capture_render($renderblock, $queued)
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
            tasks[slot] = Threads.@spawn _capture_item($f, $item, $index)
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
                tasks[slot] = Threads.@spawn _capture_item($f, $item, $index)
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

@noinline function _renderwrite_identity!(io, cols, lo::Int, hi::Int, o::WriteOpts)
    Base.write(io, _renderblock(cols, lo, hi, o))
    return
end

@noinline function _renderwrite_transformed!(io, cols, lo::Int, hi::Int,
                                             o::WriteOpts, transform)
    Base.write(io, _renderblock_transformed(cols, lo, hi, o, transform))
    return
end

function _emitrowblocks!(io, cols, nrows::Int, o::WriteOpts,
                         transform, ntasks::Int)
    nrows == 0 && return
    blockrows = _writerblockrows(cols, o, transform)
    nblocks = cld(nrows, blockrows)
    workers = min(ntasks, Threads.nthreads())
    bounds(block) = ((block - 1) * blockrows + 1,
                     min(block * blockrows, nrows))

    # Transform callbacks are observable and can retain state. Run their
    # fixed-size blocks sequentially to preserve global row-major call order.
    if transform !== _identity_transform
        for block in 1:nblocks
            lo, hi = bounds(block)
            _renderwrite_transformed!(io, cols, lo, hi, o, transform)
        end
        return
    end

    # Avoid task overhead when the caller requested one task or the table fits
    # in one block. Fixed-size blocks still bound the single-task path.
    if workers == 1 || nblocks == 1
        for block in 1:nblocks
            lo, hi = bounds(block)
            _renderwrite_identity!(io, cols, lo, hi, o)
        end
        return
    end

    renderblock = function (block)
        lo, hi = bounds(block)
        return _renderblock(cols, lo, hi, o)
    end
    emitblock = rendered -> Base.write(io, rendered)
    _ordered_parallel_blocks!(emitblock, renderblock, nblocks, workers)
    return
end

# TranscodingStreams 0.9 and 0.10 close a compressor's wrapped stream even
# when `stop_on_end=true`; 0.11 fixed that behavior. CodecZlib 0.7 permits all
# three releases, so protect caller-owned sinks rather than depend on a
# transitive version. Sink failures still pass through unchanged.
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
    io = IOBuffer()
    for (j, nm) in enumerate(names)
        _writestring(io, String(nm), o)
        j < length(names) && Base.write(io, o.delim)
    end
    Base.write(io, o.newline)
    out = take!(io)
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

@inline _identity_transform(::Int, value) = value

# --- RowWriter: the row-string iterator ---------------------------------------

struct RowWriter{R, I, F, P}
    rows::R
    initial::I
    names::Vector{Symbol}
    o::WriteOpts
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
    return RowWriter{typeof(rows), typeof(initial), typeof(transform), prefetched}(
        rows, initial, names, o, wantheader, transform)
end

function RowWriter(table; writeheader::Union{Nothing, Bool}=nothing,
                   header::Union{Nothing, Bool, AbstractVector}=nothing,
                   transform::Function=_identity_transform,
                   bufsize::Integer=1 << 22, kw...)
    return _rowwriter(table, _writeopts(; bufsize, kw...);
                      writeheader, header, transform)
end

_rowwritersize(::Base.HasLength) = Base.HasLength()
_rowwritersize(::Base.HasShape) = Base.HasLength()
_rowwritersize(::Base.IsInfinite) = Base.IsInfinite()
_rowwritersize(::Base.SizeUnknown) = Base.SizeUnknown()
Base.IteratorSize(::Type{<:RowWriter{R, I, F, true}}) where {R, I, F} =
    Base.SizeUnknown()
Base.IteratorSize(::Type{<:RowWriter{R, I, F, false}}) where {R, I, F} =
    _rowwritersize(Base.IteratorSize(R))
Base.eltype(::Type{<:RowWriter}) = String
function Base.length(rw::RowWriter{R, I, F, false}) where {R, I, F}
    nrows = length(rw.rows)
    hasheader = rw.writeheader && !isempty(rw.names)
    bomonly = rw.o.bom && nrows == 0 && !hasheader
    return nrows + hasheader + bomonly
end
Base.size(rw::RowWriter{R, I, F, false}) where {R, I, F} = (length(rw),)

function _renderrowbytes(row, names, o::WriteOpts, transform)
    io = IOBuffer()
    ncols = length(names)
    for (j, nm) in enumerate(names)
        _writecell(io, transform(j, Tables.getcolumn(row, j)), o)
        j < ncols && Base.write(io, o.delim)
    end
    Base.write(io, o.newline)
    out = take!(io)
    length(out) <= o.bufsize || _rowtoolarge(length(out), o.bufsize)
    return out
end


_renderrow(row, names, o::WriteOpts, transform) =
    String(_renderrowbytes(row, names, o, transform))

function Base.iterate(rw::RowWriter, state=nothing)
    if state === nothing
        it = rw isa RowWriter{<:Any, <:Any, <:Any, true} ? rw.initial : iterate(rw.rows)
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


function _emitrows!(io, rw::RowWriter; bom::Bool=false)
    bom && Base.write(io, UInt8[0xef, 0xbb, 0xbf])
    rw.writeheader && !isempty(rw.names) && Base.write(io, _renderheader(rw.names, rw.o))
    it = rw isa RowWriter{<:Any, <:Any, <:Any, true} ? rw.initial : iterate(rw.rows)
    while it !== nothing
        row, state = it
        Base.write(io, _renderrowbytes(row, rw.names, rw.o, rw.transform))
        it = iterate(rw.rows, state)
    end
    return
end

# --- the front door ---------------------------------------------------------

function write(sink, table; append::Bool=false, writeheader::Union{Nothing, Bool}=nothing,
               header::Union{Nothing, Bool, AbstractVector}=nothing,
               compress::Union{Bool, Symbol}=:auto,
               partition::Bool=false,
               transform::Function=_identity_transform,
               bufsize::Integer=1 << 22,
               ntasks::Int=Threads.nthreads(), kw...)
    compression = compress isa Bool ? (compress ? :gzip : :none) : compress
    compression in (:auto, :gzip, :none) ||
        throw(ArgumentError("compress must be true, false, :auto, :gzip, or :none " *
                            "(got $compress)"))
    o = _writeopts(; bufsize, kw...)
    ntasks >= 1 || throw(ArgumentError("ntasks must be >= 1 (got $ntasks)"))
    if partition
        parts = Tables.partitions(table)
        pathbase = sink isa AbstractString
        sinks = pathbase ? nothing :
                sink isa AbstractVector ? sink :
                throw(ArgumentError("partition=true needs a path or a Vector of sinks"))
        partcompression = compression === :auto && pathbase &&
                          endswith(String(sink), ".gz") ? :gzip : compression
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
           (compression === :auto && sink isa AbstractString && endswith(String(sink), ".gz"))
    emitpayload = if Tables.columnaccess(typeof(table))
        cols0 = Tables.columns(table)
        source_names = collect(Symbol, Tables.columnnames(cols0))
        names, wantheader = _headeroptions(source_names, header, writeheader, !append)
        cols = AbstractVector[Tables.getcolumn(cols0, nm) for nm in source_names]
        nrows = isempty(cols) ? 0 : length(cols[1])
        all(col -> length(col) == nrows, cols) ||
            throw(ArgumentError("all table columns must have the same length"))
        headerblock = wantheader && !isempty(names) ? _renderheader(names, o) : nothing
        # Header and fixed-size row blocks stream directly to the sink. The
        # ordered renderer retains no more than `ntasks` blocks.
        function (io)
            o.bom && !append && Base.write(io, UInt8[0xef, 0xbb, 0xbf])
            headerblock === nothing || Base.write(io, headerblock)
            _emitrowblocks!(io, cols, nrows, o, transform, ntasks)
            return
        end
    else
        # A row source may be one-shot and may not know its schema until its
        # first row. Prefetch exactly that row and retain its iterator state.
        rw = _rowwriter(table, o; writeheader, header, transform,
                        defaultheader=!append)
        io -> _emitrows!(io, rw; bom=o.bom && !append)
    end
    emit = io -> gzip ? _emitgzip!(emitpayload, io) : emitpayload(io)
    if sink isa AbstractString
        open(emit, String(sink), append ? "a" : "w")
    else
        seekable = sink isa IO && hasmethod(seek, Tuple{typeof(sink), Integer}) &&
                   hasmethod(seekend, Tuple{typeof(sink)})
        seekable && (append ? seekend(sink) : seekstart(sink))
        emission_complete = false
        try
            emit(sink)
            emission_complete = true
        finally
            # `append=false` means replacement for seekable IOs as it does for
            # a path opened with "w". Truncate even after a render failure, so
            # a partial new payload never exposes stale bytes from old content.
            if !append && seekable
                try
                    pos = position(sink)
                    applicable(truncate, sink, pos) && truncate(sink, pos)
                catch
                    # Preserve the render/sink exception when cleanup also
                    # fails. A truncation failure is primary after successful
                    # emission and must be reported.
                    emission_complete && rethrow()
                end
            end
        end
    end
    return sink
end


write(sink; kw...) = table -> write(sink, table; kw...)
