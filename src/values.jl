"""
    KernelValues

The new value layer — span-exact, monomorphic, self-contained type parsers.
This module is the initial shape of what becomes Parsers.jl 3.0's core and,
after that, the proposed Base implementation. Design constraints, in force from
day one:

  * **Total functions.** Every parser consumes an exact byte span `[i, j]` and
    returns `(value, rc)` where `rc` is OK / INVALID / OVERFLOW. There is no
    fallback to `Parsers.xparse`, `Base.parse`, or any C routine — exceptional
    cases (768-digit halfway floats, subnormals, huge exponents) are handled by
    self-contained slow paths *inside* the parsers.
  * **No parser dependencies in the core.** The fixed-width kernels do not use
    GMP, MPFR, libc parsing, or external packages. The float slow path is Tao's
    simple decimal conversion over a fixed-size digit buffer (correct rounding
    for Float64 never needs more than 768 significant digits). The
    Eisel–Lemire powers-of-five table is computed at precompile time by
    `_buildpow5` using a minimal little-endian limb array. The user-only
    BigInt/BigFloat kernels use their Base-owned GMP/MPFR arithmetic after this
    layer has parsed and rounded the input; no string parser receives the span.
  * **Bootstrap-clean fixed-width paths.** No `@generated`, no `eval`, no
    closures over mutable state, and no allocation on the fixed-width hot paths
    — plain loops and bit ops, compilable early enough for Base adoption and
    `--trim`. The arbitrary-precision result types allocate their own storage.
  * **Strictness: parse-set ≡ detect-set.** Kernels accept exactly the canonical
    spellings (plus explicit user lists for Bool). `Bool` does NOT accept "1";
    `DateTime` does NOT accept a bare date. This eliminates the CSV kernel's
    sample-independence guard by construction.
  * **Dates independence.** Date/time parsing produces a plain `CivilParts`
    record via pure integer arithmetic (format programs included); thin adapters
    at the bottom of this file convert to `Dates.Date`/`DateTime`/`Time`. When
    this moves to Base, the adapters move to the Dates stdlib.

Deliberate semantic choices (deltas from legacy `Parsers.xparse` are pinned in
test_values.jl): whitespace is never consumed (trimming is the caller's layer);
sentinels/quotes are the caller's layer (`findcontent`/`matchsentinel` below);
`-0` parses as `Int64(0)`; `Inf`/`Infinity`/`NaN` are case-insensitive; float
overflow yields `±Inf` with OK (matching `Base.parse`); integer overflow is
reported as OVERFLOW so the CSV type lattice can promote Int64 → Int128 → Float64.
"""
module KernelValues

export RC_OK, RC_INVALID, RC_OVERFLOW, degroup!, parsebigint, parsebigfloat, parseuuid,
       parseint64, parseint128, parsefloat64, parsebool, findcontent, matchsentinel,
       CivilParts, parsecivil, daysfromcivil, DatePattern, compilepattern,
       parsegroupedint64

const RC_OK       = 0x00
const RC_INVALID  = 0x01
const RC_OVERFLOW = 0x02

# =============================================================================
# integers
# =============================================================================

# All eight bytes of `w` are ASCII digits? (exact: borrow-free formulation)
@inline function _alldigits8(w::UInt64)
    return ((w & 0xf0f0f0f0f0f0f0f0) |
            (((w + 0x0606060606060606) & 0xf0f0f0f0f0f0f0f0) >> 4)) ==
           0x3333333333333333
end

# Convert eight ASCII digits (already validated) to their integer value — the
# classic two-multiply SWAR gather.
@inline function _digits8(w::UInt64)
    w -= 0x3030303030303030
    w = (w * 10) + (w >> 8)                     # pairs
    w = (((w & 0x000000ff000000ff) * 0x000f424000000064) +
         (((w >> 16) & 0x000000ff000000ff) * 0x0000271000000001)) >> 32
    return w
end

@inline _load8(buf::Vector{UInt8}, i::Int) =
    GC.@preserve buf ltoh(unsafe_load(Ptr{UInt64}(pointer(buf, i))))

"""
    parseint64(buf, i, j) -> (Int64, rc)

Parse `buf[i:j]` as a base-10 `Int64`: optional single `-`/`+` sign, then one or
more ASCII digits, nothing else. Leading zeros are accepted (the CSV layer's
inference policy for zero-padded identifiers lives above this). `rc` is
OVERFLOW when the digits are well-formed but exceed Int64, INVALID otherwise.
"""
function parseint64(buf::Vector{UInt8}, i::Int, j::Int)
    i > j && return (zero(Int64), RC_INVALID)
    @inbounds b = buf[i]
    neg = b == UInt8('-')
    (neg | (b == UInt8('+'))) && (i += 1)
    i > j && return (zero(Int64), RC_INVALID)
    # skip (but count) leading zeros so the digit-count overflow bound is exact
    z = i
    @inbounds while i <= j && buf[i] == UInt8('0')
        i += 1
    end
    i > j && return (zero(Int64), RC_OK)         # all zeros ("0", "-000")
    ndig = j - i + 1
    if ndig > 19
        return _digitsonly(buf, i, j) ? (zero(Int64), RC_OVERFLOW) :
                                        (zero(Int64), RC_INVALID)
    end
    v = zero(UInt64)
    @inbounds while i + 7 <= j
        w = _load8(buf, i)
        _alldigits8(w) || return (zero(Int64), RC_INVALID)
        v = v * 100_000_000 + _digits8(w)
        i += 8
    end
    @inbounds while i <= j
        d = buf[i] - UInt8('0')
        d > 0x09 && return (zero(Int64), RC_INVALID)
        v = v * 10 + d
        i += 1
    end
    # 19 digits can exceed typemax; check against the signed bound
    if ndig == 19
        lim = neg ? UInt64(9223372036854775808) : UInt64(9223372036854775807)
        v > lim && return (zero(Int64), RC_OVERFLOW)
    end
    return (neg ? -reinterpret(Int64, v) : reinterpret(Int64, v), RC_OK)
end

"""
    parseint128(buf, i, j) -> (Int128, rc)

Parse the strict integer grammar as `Int128`. This is the exact-width fallback
after `parseint64` reports overflow.
"""
function parseint128(buf::Vector{UInt8}, i::Int, j::Int)
    i > j && return (zero(Int128), RC_INVALID)
    @inbounds b = buf[i]
    neg = b == UInt8('-')
    (neg | (b == UInt8('+'))) && (i += 1)
    i > j && return (zero(Int128), RC_INVALID)
    @inbounds while i <= j && buf[i] == UInt8('0')
        i += 1
    end
    i > j && return (zero(Int128), RC_OK)
    ndig = j - i + 1
    if ndig > 39
        return _digitsonly(buf, i, j) ? (zero(Int128), RC_OVERFLOW) :
                                       (zero(Int128), RC_INVALID)
    end
    lim = UInt128(typemax(Int128)) + UInt128(neg)
    v = zero(UInt128)
    @inbounds while i <= j
        d = buf[i] - UInt8('0')
        d > 0x09 && return (zero(Int128), RC_INVALID)
        v > (lim - UInt128(d)) ÷ UInt128(10) &&
            return (zero(Int128), RC_OVERFLOW)
        v = v * UInt128(10) + UInt128(d)
        i += 1
    end
    if neg
        v == UInt128(typemax(Int128)) + 1 && return (typemin(Int128), RC_OK)
        return (-Int128(v), RC_OK)
    end
    return (Int128(v), RC_OK)
end

@inline function _digitsonly(buf::Vector{UInt8}, i::Int, j::Int)
    @inbounds for k in i:j
        (buf[k] - UInt8('0')) > 0x09 && return false
    end
    return true
end

# =============================================================================
# floats — three self-contained tiers:
#   1. exact small case (mantissa ≤ 15 digits, small exponent): one fma-free
#      multiply/divide by an exactly-representable power of ten
#   2. Eisel–Lemire: 128-bit product against the precomputed powers-of-five
#      table; bails (rarely) on rounding-boundary ambiguity
#   3. Tao's simple decimal conversion over a fixed 800-digit buffer — total,
#      exact, covers subnormals and every ambiguous case
# =============================================================================

# --- decimal decomposition ---------------------------------------------------

struct DecParts
    mant::UInt64      # up to 19 significant digits (truncated beyond)
    exp10::Int32      # power of ten applied to mant
    ndig::Int32       # significant digits seen (may exceed 19)
    truncated::Bool   # digits beyond 19 were dropped (a nonzero one ⇒ sticky)
    neg::Bool
    digstart::Int32   # buf offset of the first significant digit (tier 3 re-read)
end

# Split [i,j] into sign/digits/point/exponent. Returns (parts, rc) with
# rc=INVALID for structure errors; special spellings handled by caller.
function _decompose(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    neg = false
    @inbounds if i <= j
        b = buf[i]
        neg = b == UInt8('-')
        (neg | (b == UInt8('+'))) && (i += 1)
    end
    i > j && return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
    mant = zero(UInt64)
    ndig = 0
    exp10 = 0
    truncated = false
    sawdigit = false
    sawpoint = false
    digstart = 0
    @inbounds while i <= j
        b = buf[i]
        d = b - UInt8('0')
        if d <= 0x09
            sawdigit = true
            if !(ndig == 0 && d == 0x00)         # skip leading zeros entirely
                digstart == 0 && (digstart = i)
                if ndig < 19
                    mant = mant * 10 + d
                    ndig += 1
                else
                    truncated |= d != 0x00
                    ndig += 1
                    sawpoint || (exp10 += 1)     # dropped integer digit
                    i += 1
                    continue
                end
            end
            sawpoint && ndig <= 19 && (exp10 -= 1)
        elseif b == decimal && !sawpoint
            sawpoint = true
        elseif (b == UInt8('e')) | (b == UInt8('E'))
            sawdigit || return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
            i += 1
            eneg = false
            @inbounds if i <= j
                eb = buf[i]
                eneg = eb == UInt8('-')
                (eneg | (eb == UInt8('+'))) && (i += 1)
            end
            i > j && return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
            e = 0
            @inbounds while i <= j
                ed = buf[i] - UInt8('0')
                ed > 0x09 && return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
                e < 100_000 && (e = e * 10 + Int(ed))   # clamp: beyond ±99999 saturates
                i += 1
            end
            exp10 += eneg ? -e : e
            return (DecParts(mant, Int32(exp10), Int32(ndig), truncated, neg, Int32(digstart)), RC_OK)
        else
            return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
        end
        i += 1
    end
    sawdigit || return (DecParts(0, 0, 0, false, neg, 0), RC_INVALID)
    return (DecParts(mant, Int32(exp10), Int32(ndig), truncated, neg, Int32(digstart)), RC_OK)
end

# --- tier 2: Eisel–Lemire ------------------------------------------------------

# Powers of five, 128-bit truncated significands, q ∈ POW5MIN:POW5MAX.
const POW5MIN = -342
const POW5MAX = 308

# Minimal limb machinery — exists solely to build the table at precompile time.
# Little-endian Vector{UInt64} limbs; operations: multiply by 5, bit length,
# extract top-128, and shifted compare/subtract driving one restoring division.
function _mul5!(a::Vector{UInt64})
    carry = zero(UInt64)
    @inbounds for k in eachindex(a)
        hi, lo = _mul64(a[k], UInt64(5))
        s = lo + carry
        a[k] = s
        carry = hi + (s < lo)
    end
    carry != 0 && push!(a, carry)
    return a
end
@inline function _mul64(x::UInt64, y::UInt64)
    p = UInt128(x) * UInt128(y)
    return (UInt64(p >> 64), UInt64(p & typemax(UInt64)))
end
function _bitlen(a::Vector{UInt64})
    @inbounds for k in length(a):-1:1
        a[k] != 0 && return 64 * (k - 1) + (64 - leading_zeros(a[k]))
    end
    return 0
end
@inline function _getbit(a::Vector{UInt64}, bit::Int)  # 0-based
    limb = bit >> 6 + 1
    limb > length(a) && return false
    return (a[limb] >> (bit & 63)) & 1 == 1
end
# top 128 bits of `a` (normalized so bit (bitlen-1) is the msb of hi)
function _top128(a::Vector{UInt64})
    bl = _bitlen(a)
    hi = zero(UInt64); lo = zero(UInt64)
    for b in 0:127
        src = bl - 1 - b
        bit = src >= 0 ? _getbit(a, src) : false
        if b < 64
            hi |= UInt64(bit) << (63 - b)
        else
            lo |= UInt64(bit) << (127 - b)
        end
    end
    sticky = false
    for b in 0:(bl - 129)
        if _getbit(a, b)
            sticky = true
            break
        end
    end
    return hi, lo, sticky
end
# is (a << s) <= r ?
function _shiftedle(a::Vector{UInt64}, s::Int, r::Vector{UInt64})
    bla = _bitlen(a) + s
    blr = _bitlen(r)
    bla != blr && return bla < blr
    for b in (blr - 1):-1:0
        ab = b - s >= 0 ? _getbit(a, b - s) : false
        rb = _getbit(r, b)
        ab != rb && return rb        # first difference: a<r iff r has the 1
    end
    return true
end
# r -= a << s   (requires (a<<s) ≤ r)
function _subshifted!(r::Vector{UInt64}, a::Vector{UInt64}, s::Int)
    limbshift = s >> 6
    bitshift = s & 63
    borrow = zero(UInt64)
    @inbounds for k in 1:length(r)
        ak = k - limbshift
        av = zero(UInt64)
        if 1 <= ak <= length(a)
            av = a[ak] << bitshift
            bitshift != 0 && ak > 1 && (av |= a[ak - 1] >> (64 - bitshift))
        elseif bitshift != 0 && 1 <= ak - 1 <= length(a) && ak == length(a) + 1
            av = a[ak - 1] >> (64 - bitshift)
        end
        d = r[k] - av
        b2 = d > r[k]
        d2 = d - borrow
        borrow = UInt64(b2 | (d2 > d))
        r[k] = d2
    end
    return r
end

function _buildpow5()
    n = POW5MAX - POW5MIN + 1
    HI = Vector{UInt64}(undef, n)
    LO = Vector{UInt64}(undef, n)
    # positive q (and q = 0): truncated top-128 of 5^q
    p = UInt64[1]
    for q in 0:POW5MAX
        hi, lo, _ = _top128(p)
        HI[q - POW5MIN + 1] = hi
        LO[q - POW5MIN + 1] = lo
        _mul5!(p)
    end
    # negative q: floor(2^(bitlen(5^p)+127) / 5^p) + 1  (reference table rule)
    p = UInt64[1]
    for q in -1:-1:POW5MIN
        _mul5!(p)                                 # p = 5^(-q)
        k = _bitlen(p) + 127
        # restoring division: quotient of 2^k / p has exactly 128 bits
        r = zeros(UInt64, (k >> 6) + 2)
        r[(k >> 6) + 1] |= UInt64(1) << (k & 63)
        qhi = zero(UInt64); qlo = zero(UInt64)
        for bit in 127:-1:0
            if _shiftedle(p, bit, r)
                _subshifted!(r, p, bit)
                if bit >= 64
                    qhi |= UInt64(1) << (bit - 64)
                else
                    qlo |= UInt64(1) << bit
                end
            end
        end
        qlo += 1                                   # the +1 (never overflows: quotient is odd-truncated)
        qlo == 0 && (qhi += 1)
        HI[q - POW5MIN + 1] = qhi
        LO[q - POW5MIN + 1] = qlo
    end
    return HI, LO
end

const POW5HI, POW5LO = _buildpow5()

# Eisel–Lemire core: value = mant × 10^q (mant ≠ 0, not truncated unless
# `truncated`). Returns reinterpretable bits, or -1 ⇒ tier 3 decides.
function _eisel_lemire(mant::UInt64, q::Int)
    (q < POW5MIN || q > POW5MAX) && return Int64(-2)   # certain under/overflow, sign applied by caller
    lz = leading_zeros(mant)
    w = mant << lz
    idx = q - POW5MIN + 1
    @inbounds t = UInt128(w) * UInt128(POW5HI[idx])
    hi = UInt64(t >> 64)
    lo = UInt64(t & typemax(UInt64))
    if (hi & 0x1ff) == 0x1ff                            # need more precision
        @inbounds t2 = UInt128(w) * UInt128(POW5LO[idx])
        hi2 = UInt64(t2 >> 64)
        lo0 = lo
        lo += hi2
        lo < lo0 && (hi += 1)
        (hi & 0x1ff) == 0x1ff && lo + 1 == 0 && return Int64(-1)  # still ambiguous
    end
    upper = hi >> 63
    shift = Int(upper) + 9
    m = hi >> shift                                     # 54 bits: 53 + round bit
    e2 = ((217706 * q) >> 16) + 63 + Int(upper) - lz    # unbiased binary exponent of hi's msb
    e2 += 1023                                          # bias
    if e2 <= 0
        # Shift the guard-bit mantissa into the denormal range, then round it.
        # A carry can promote the result to the smallest normal. This is the
        # standard Eisel-Lemire subnormal step; rejecting here made every
        # shortest subnormal pay the exact-decimal tier's ~1,000 scaling loops.
        shift = -e2 + 1
        shift >= 64 && return Int64(0)                  # certain underflow
        m >>= shift
        m = (m + (m & 1)) >> 1
        e2 = m < (UInt64(1) << 52) ? 0 : 1
        return reinterpret(Int64, (UInt64(e2) << 52) | (m & 0x000fffffffffffff))
    end
    # Exact halfway values in the small-power range need the discarded bits.
    # Keep the fast_float condition exact; this tier delegates instead of
    # repairing the low bit because SDC already owns all ambiguous answers.
    if lo <= 1 && -4 <= q <= 23 && (m & 0b11) == 0b01 && (m << shift) == hi
        return Int64(-1)
    end
    m = (m + (m & 1)) >> 1                              # round to nearest, ties away resolved below
    if m == (UInt64(1) << 53)
        m >>= 1
        e2 += 1
    end
    e2 >= 2047 && return Int64(-3)                      # overflow ⇒ ±Inf
    return reinterpret(Int64, (UInt64(e2) << 52) | (m & 0x000fffffffffffff))
end

# --- tier 3: simple decimal conversion ----------------------------------------

const SDC_MAXDIG = 800   # 768-digit worst case + slack

# Fixed-size decimal 0.d₁d₂…dₙ × 10^dp with sticky truncation. All operations
# are exact except the documented truncate-at-800 (which sets `sticky` and is
# beyond the decision bound for Float64).
mutable struct HPD
    d::Vector{UInt8}
    n::Int
    dp::Int
    sticky::Bool
end

function _hpd(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    d = Vector{UInt8}(undef, SDC_MAXDIG)
    n = 0
    dp = 0
    sticky = false
    sawpoint = false
    sawdig = false
    k = i
    @inbounds if k <= j && (buf[k] == UInt8('-') || buf[k] == UInt8('+'))
        k += 1
    end
    @inbounds while k <= j
        b = buf[k]
        dig = b - UInt8('0')
        if dig <= 0x09
            sawdig = true
            if n == 0 && dig == 0
                sawpoint && (dp -= 1)
            else
                if n < SDC_MAXDIG
                    n += 1
                    d[n] = dig
                else
                    sticky |= dig != 0
                end
                sawpoint || (dp += 1)
            end
        elseif b == decimal
            sawpoint = true
        else # exponent (structure already validated by _decompose)
            k += 1
            eneg = false
            @inbounds if k <= j && (buf[k] == UInt8('-') || buf[k] == UInt8('+'))
                eneg = buf[k] == UInt8('-')
                k += 1
            end
            e = 0
            @inbounds while k <= j
                e < 100_000 && (e = e * 10 + Int(buf[k] - UInt8('0')))
                k += 1
            end
            dp += eneg ? -e : e
            break
        end
        k += 1
    end
    while n > 0 && d[n] == 0
        n -= 1
    end
    return HPD(d, n, dp, sticky)
end

# value ≥ 1?
_hpdge1(h::HPD) = h.n > 0 && h.dp > 0
# double in place: value *= 2  (value < 1 before call keeps digits bounded)
function _double!(h::HPD)
    carry = 0
    @inbounds for k in h.n:-1:1
        v = Int(h.d[k]) * 2 + carry
        h.d[k] = UInt8(v % 10)
        carry = v ÷ 10
    end
    if carry != 0
        # shift right one digit to prepend the carry
        n = min(h.n + 1, SDC_MAXDIG)
        h.sticky |= h.n + 1 > SDC_MAXDIG && h.d[SDC_MAXDIG] != 0
        @inbounds for k in n:-1:2
            h.d[k] = h.d[k - 1]
        end
        h.d[1] = UInt8(carry)
        h.n = n
        h.dp += 1
    end
    while h.n > 0 && h.d[h.n] == 0
        h.n -= 1
    end
    return h
end
# halve in place: value /= 2 == value*5, dp -= 1
function _halve!(h::HPD)
    carry = 0
    # multiply by 5 processing from the right
    @inbounds for k in h.n:-1:1
        v = Int(h.d[k]) * 5 + carry
        h.d[k] = UInt8(v % 10)
        carry = v ÷ 10
    end
    while carry != 0
        n = min(h.n + 1, SDC_MAXDIG)
        h.sticky |= h.n + 1 > SDC_MAXDIG && h.d[SDC_MAXDIG] != 0
        @inbounds for k in n:-1:2
            h.d[k] = h.d[k - 1]
        end
        h.d[1] = UInt8(carry % 10)
        carry ÷= 10
        h.n = n
        h.dp += 1
    end
    h.dp -= 1
    while h.n > 0 && h.d[h.n] == 0
        h.n -= 1
    end
    return h
end
# subtract 1 (requires 1 ≤ value < 2, i.e. dp == 1 and d1 ≥ 1... value<2 ⇒ d1 ∈ 1)
function _sub1!(h::HPD)
    # value = d1.d2d3… with dp == 1; subtracting 1 zeroes the integer digit
    @inbounds h.d[1] -= 1
    while h.n > 0 && h.d[h.n] == 0
        h.n -= 1
    end
    if h.n > 0 && h.d[1] == 0
        # renormalize: drop leading zeros
        lead = 0
        @inbounds while lead < h.n && h.d[lead + 1] == 0
            lead += 1
        end
        @inbounds for k in 1:(h.n - lead)
            h.d[k] = h.d[k + lead]
        end
        h.n -= lead
        h.dp -= lead
    elseif h.n == 0
        h.dp = 0
    end
    return h
end

# @noinline is load-bearing: this is the ~1-in-10^4 cold tier, and letting it
# inline bloats parsefloat64's hot path ~7x (measured 29ns -> 203ns per value).
@noinline function _sdc(buf::Vector{UInt8}, i::Int, j::Int, neg::Bool, decimal::UInt8)
    h = _hpd(buf, i, j, decimal)
    h.n == 0 && return _sign(0x0000000000000000, neg)
    # scale into [1, 2): binary exponent accumulates in e2
    e2 = 0
    while !_hpdge1(h)                       # value < 1: double
        _double!(h)
        e2 -= 1
        e2 < -1200 && return _sign(0x0000000000000000, neg)  # certain underflow to 0
    end
    while h.dp > 1 || (h.dp == 1 && h.d[1] >= 2)   # value ≥ 2: halve
        _halve!(h)
        e2 += 1
        e2 > 1100 && return _sign(0x7ff0000000000000, neg)   # certain overflow
    end
    # now 1 ≤ value < 2, msb bit is the leading 1
    e2biased = e2 + 1023
    nbits = 52
    subnormal = e2biased <= 0
    if subnormal
        # subnormals have NO implicit bit: the leading 1 is stored, followed by
        # nbits generated bits (nbits == -1 ⇒ even the leading 1 is below bit 0
        # and becomes the rounding bit for the min-subnormal decision)
        nbits = 52 + e2biased - 1
        nbits < -1 && return _sign(0x0000000000000000, neg)
        e2biased = 0
    end
    _sub1!(h)                                # consume the leading 1
    local mant::UInt64
    local roundbit::Bool
    if subnormal && nbits == -1
        mant = zero(UInt64)
        roundbit = true                      # the leading 1 itself
    else
        mant = subnormal ? one(UInt64) : zero(UInt64)
        for _ in 1:nbits
            _double!(h)
            bit = _hpdge1(h)
            mant = (mant << 1) | UInt64(bit)
            bit && _sub1!(h)
        end
        _double!(h)
        roundbit = _hpdge1(h)
        roundbit && _sub1!(h)
    end
    stickyrest = h.n > 0 || h.sticky
    if roundbit && (stickyrest || (mant & 1) == 1)
        mant += 1
        if e2biased == 0 && mant == (UInt64(1) << 52)
            e2biased = 1                     # subnormal rounded up to normal
            mant = 0
        elseif mant == (UInt64(1) << 52)
            mant = 0
            e2biased += 1
        end
    end
    if e2biased == 0 && nbits < 52
        # subnormal: mantissa currently has `nbits+?` bits — it is already in
        # low-bit position because we generated exactly nbits of them
        return _sign(mant, neg)
    end
    e2biased >= 2047 && return _sign(0x7ff0000000000000, neg)
    return _sign((UInt64(e2biased) << 52) | (mant & 0x000fffffffffffff), neg)
end

@inline _sign(bits::UInt64, neg::Bool) =
    reinterpret(Float64, bits | (UInt64(neg) << 63))

# --- special spellings ---------------------------------------------------------

@inline _lower(b::UInt8) = b | 0x20
function _matchspecial(buf::Vector{UInt8}, i::Int, j::Int)
    # returns (Float64, matched)
    neg = false
    @inbounds if i <= j
        b = buf[i]
        neg = b == UInt8('-')
        (neg | (b == UInt8('+'))) && (i += 1)
    end
    n = j - i + 1
    @inbounds if n == 3
        if _lower(buf[i]) == UInt8('n') && _lower(buf[i+1]) == UInt8('a') && _lower(buf[i+2]) == UInt8('n')
            return (neg ? -NaN : NaN, true)
        elseif _lower(buf[i]) == UInt8('i') && _lower(buf[i+1]) == UInt8('n') && _lower(buf[i+2]) == UInt8('f')
            return (neg ? -Inf : Inf, true)
        end
    elseif n == 8
        ok = true
        for (k, c) in enumerate((UInt8('i'), UInt8('n'), UInt8('f'), UInt8('i'), UInt8('n'), UInt8('i'), UInt8('t'), UInt8('y')))
            ok &= _lower(buf[i + k - 1]) == c
        end
        ok && return (neg ? -Inf : Inf, true)
    end
    return (0.0, false)
end

# byte-equality marks (0x80 at each matching byte) — SWAR zero-byte test
@inline function _eqmask8(w::UInt64, b::UInt8)
    x = w ⊻ (0x0101010101010101 * b)
    return (x - 0x0101010101010101) & ~x & 0x8080808080808080
end

# Validate-and-gather the low `len` bytes of `w` as digits: left-align so high
# garbage falls off, back-fill ASCII zeros, one _alldigits8 + one _digits8.
@inline function _rundigits(w::UInt64, len::Int)
    s = (8 - len) << 3
    w = (w << s) | (0x3030303030303030 >>> (64 - s))
    return (_digits8(w), _alldigits8(w))
end

const _P10U = (UInt64(1), UInt64(10), UInt64(100), UInt64(1000), UInt64(10_000),
               UInt64(100_000), UInt64(1_000_000), UInt64(10_000_000), UInt64(100_000_000))

# The dominant float shape — [sign] up-to-8 digits [decimal up-to-8 digits],
# no exponent — resolves from two word loads: the decimal locates via an eq
# mask, both digit runs extract from the loaded registers (never re-reading
# the buffer, so nothing reads past the guard), and the mantissa packs with
# the same SWAR gather integers use. Any other spelling — exponents, >8-digit
# runs, specials, spans within 16 bytes of the buffer's end — returns
# handled=false and the general state machine decides, so the accepted set is
# unchanged by construction. Undecided Eisel-Lemire edges also fall back.
@inline function _float_fast(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    neg = false
    @inbounds if i <= j
        b = buf[i]
        neg = b == UInt8('-')
        (neg | (b == UInt8('+'))) && (i += 1)
    end
    n = j - i + 1
    (1 <= n <= 15 && i + 15 <= length(buf)) || return (0.0, false)
    w1 = _load8(buf, i)
    w2 = _load8(buf, i + 8)
    mk1 = _eqmask8(w1, decimal)
    mk2 = _eqmask8(w2, decimal)
    n < 8 && (mk1 &= (UInt64(1) << (n << 3)) - UInt64(1))
    mk2 &= n <= 8 ? zero(UInt64) : (UInt64(1) << ((n - 8) << 3)) - UInt64(1)
    count_ones(mk1) + count_ones(mk2) <= 1 || return (0.0, false)
    p = mk1 != 0 ? (trailing_zeros(mk1) >> 3) :
        mk2 != 0 ? 8 + (trailing_zeros(mk2) >> 3) : n   # 0-based decimal position
    intlen = p
    fraclen = p == n ? 0 : n - p - 1
    (intlen <= 8 && fraclen <= 8 && intlen + fraclen >= 1) || return (0.0, false)
    iv, iok = _rundigits(w1, intlen)             # int run = low bytes of w1
    fv, fok = fraclen == 0 ? (zero(UInt64), true) : begin
        off = p + 1                              # 1 ≤ off ≤ 8 by the bounds above
        wf = (w1 >>> (off << 3)) | (w2 << ((8 - off) << 3))
        _rundigits(wf, fraclen)
    end
    (iok & fok) || return (0.0, false)
    mant = iv * (@inbounds _P10U[fraclen + 1]) + fv
    q = -fraclen
    mant == zero(UInt64) && return (neg ? -0.0 : 0.0, true)
    # n ≤ 15 leaves at most 14 digits when a point is present; without one,
    # intlen ≤ 8. The mantissa is therefore always below 2^53, and q is -8:0.
    f = Float64(mant)
    f = q == 0 ? f : f / _POW10[-q + 1]
    return (neg ? -f : f, true)
end

@inline function _parsefloat_core(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    v, handled = _float_fast(buf, i, j, decimal)
    handled && return (v, RC_OK, true)
    sp, matched = _matchspecial(buf, i, j)
    matched && return (sp, RC_OK, true)
    parts, rc = _decompose(buf, i, j, decimal)
    rc == RC_OK || return (0.0, rc, true)
    mant = parts.mant
    q = Int(parts.exp10)
    mant == 0 && return (parts.neg ? -0.0 : 0.0, RC_OK, true)
    untrunc = !parts.truncated && parts.ndig <= 19
    if untrunc && -22 <= q <= 22 && mant <= 9007199254740992   # 2^53
        # tier 1: both mant and 10^|q| exactly representable → one rounding
        f = Float64(mant)
        f = q >= 0 ? f * _POW10[q + 1] : f / _POW10[-q + 1]
        return (parts.neg ? -f : f, RC_OK, true)
    end
    bits = _eisel_lemire(mant, q)
    if !untrunc && bits >= 0
        # truncated mantissa: decided only if mant and mant+1 round identically
        # (the reference fast_float rule); otherwise the digits must speak (tier 3)
        bits2 = _eisel_lemire(mant + 1, q)
        bits2 == bits || return (parts.neg ? -1.0 : 1.0, RC_OK, false)
    end
    bits >= 0 && return (_sign(reinterpret(UInt64, bits), parts.neg), RC_OK, true)
    if bits == Int64(-2)
        return (q < 0 ? (parts.neg ? -0.0 : 0.0) : (parts.neg ? -Inf : Inf), RC_OK, true)
    elseif bits == Int64(-3)
        return (parts.neg ? -Inf : Inf, RC_OK, true)
    end
    return (parts.neg ? -1.0 : 1.0, RC_OK, false)   # tier 3 required; sign in value
end

"""
    parsefloat64(buf, i, j, decimal=UInt8('.')) -> (Float64, rc)

Parse `buf[i:j]` as a `Float64` with correct (round-half-even) rounding for
every input — no C, no BigFloat: Clinger's exact small case, then Eisel–Lemire,
then simple-decimal-conversion for the rare ambiguous/subnormal cases.
Accepts sign, digits, one `decimal` byte, optional e/E exponent, and the
case-insensitive spellings Inf/Infinity/NaN. Overflow → ±Inf with OK.

Structured as an @inline hot core plus a thin wrapper owning the cold tier-3
tail. (A once-suspected compiler pessimization here turned out to be real
tier-3 work: random-bit benchmarks include ~0.05% subnormals, which cost ~1ms
each until Eisel-Lemire grew the standard denormal shift. See
probe_float_anomaly.jl for the post-mortem.)
"""
function parsefloat64(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    v, rc, done = _parsefloat_core(buf, i, j, decimal)
    done && return (v, rc)
    return (_sdc(buf, i, j, v < 0, decimal), RC_OK)
end
parsefloat64(buf::Vector{UInt8}, i::Int, j::Int; decimal::UInt8=UInt8('.')) =
    parsefloat64(buf, i, j, decimal)

const _POW10 = Float64[10.0^k for k in 0:22]

# =============================================================================
# arbitrary precision & identifiers — BigInt / BigFloat / UUID
#
# The self-contained mandate covers the PARSING: span validation, digit
# decomposition, binary extraction, and rounding are all ours. BigInt/BigFloat
# are Base types whose arithmetic is GMP/MPFR by definition — we hand them a
# finished, correctly-rounded value (never a string), so mpz_set_str /
# mpfr_strtofr are never involved.
# =============================================================================

const _POW10_INT = Int64[Int64(10)^k for k in 0:17]
const _POW10_18 = Int64(10)^18

# Powers of five for the BigFloat scaling path, built at precompile time.
# Covers every exponent reachable from ~150 significant digits around the
# double range; rarer exponents compute fresh. Entries are READ-ONLY — the
# scaling code must never hand them to an in-place GMP op's output slot.
for f in (:set_si!, :mul!, :mul_si!, :add_ui!, :mul_2exp!, :tdiv_qr!,
          :fdiv_q_2exp!, :tstbit, :scan1, :sizeinbase, :pow_ui, :neg!)
    isdefined(Base.GMP.MPZ, f) ||
        error("KernelValues requires Base.GMP.MPZ.$f (Julia internals moved?)")
end

const _POW5BIG = [BigInt(5)^k for k in 0:512]
@inline _pow5big(k::Int) = k <= 512 ? @inbounds(_POW5BIG[k + 1]) : BigInt(5)^k

"""
    BigWork()

Reusable workspace for `parsebigfloat`: the two BigInt temporaries (mantissa
accumulator and division remainder) live here so a column loop allocates them
once instead of per value — GMP objects are finalizer-registered, and two
fewer registrations per value is most of the distance to mpfr_strtofr's
single-allocation profile. Never share one across concurrent tasks.
"""
struct BigWork
    M::BigInt
    R::BigInt
end
BigWork() = BigWork(BigInt(0), BigInt(0))

# One correctly-rounded store into a fresh BigFloat: our prec-bit integer
# mantissa is exact under mpfr_set_z, the 2^e scale is exact under mul_2si,
# and the sign flips in place — one MPFR allocation, no ldexp/unary-minus
# temporaries.
function _assemble(M::BigInt, e::Int, neg::Bool, prec::Int)
    v = BigFloat(; precision=prec)
    ccall((:mpfr_set_z, Base.MPFR.libmpfr), Int32,
          (Ref{BigFloat}, Ref{BigInt}, Int32), v, M, 0)
    ccall((:mpfr_mul_2si, Base.MPFR.libmpfr), Int32,
          (Ref{BigFloat}, Ref{BigFloat}, Clong, Int32), v, v, e, 0)
    neg && ccall((:mpfr_neg, Base.MPFR.libmpfr), Int32,
                 (Ref{BigFloat}, Ref{BigFloat}, Int32), v, v, 0)
    return v
end

# 18-digit chunks flush through in-place GMP ops — one BigInt allocated per
# value, zero per chunk (the allocating `big = big*10^18 + acc` form measured
# ~40% slower than mpz_set_str; this form beats it).
@inline function _flushchunk!(big::BigInt, started::Bool, acc::Int64, mult::Int64)
    if started
        Base.GMP.MPZ.mul_si!(big, mult)
        Base.GMP.MPZ.add_ui!(big, acc % UInt64)
    else
        Base.GMP.MPZ.set_si!(big, acc)
    end
    return true
end

"""
    parsebigint(buf, i, j) -> (BigInt, rc)

Exact-span BigInt: sign and decimal digits only (the strict integer grammar,
same as `parseint64` without the width limit). Digits accumulate through
18-digit Int64 chunks, so the big-number work is O(n/18) multiply-adds on the
result type rather than per-digit ops or a string round-trip.
"""
function parsebigint(buf::Vector{UInt8}, i::Int, j::Int)
    i > j && return (BigInt(0), RC_INVALID)
    neg = false
    @inbounds begin
        b = buf[i]
        if b == UInt8('-') || b == UInt8('+')
            neg = b == UInt8('-')
            i += 1
        end
    end
    i > j && return (BigInt(0), RC_INVALID)
    acc = Int64(0)
    nacc = 0
    big = BigInt(0)
    started = false
    @inbounds for k in i:j
        d = buf[k] - UInt8('0')
        d > 0x09 && return (BigInt(0), RC_INVALID)
        acc = acc * 10 + Int64(d)
        nacc += 1
        if nacc == 18
            started = _flushchunk!(big, started, acc, _POW10_18)
            acc = 0
            nacc = 0
        end
    end
    nacc > 0 && (started = _flushchunk!(big, started, acc, @inbounds(_POW10_INT[nacc + 1])))
    neg && Base.GMP.MPZ.neg!(big)
    return (big, RC_OK)
end

"""
    parsebigfloat(buf, i, j, decimal=UInt8('.'); prec=precision(BigFloat)) -> (BigFloat, rc)

Correctly rounded (round-half-even) BigFloat at `prec` bits, same grammar and
special spellings as `parsefloat64`. The high-precision decimal machinery from
tier 3 generalizes: scale the exact decimal into [1, 2), generate `prec`
binary digits, round once with the sticky bit. MPFR only STORES the result —
the value is assembled from an exactly-representable prec-bit integer and an
exact `ldexp`, so this layer performs the single rounding itself.

Prove-out range bound: decimal magnitudes beyond ~10^±65536 return
RC_OVERFLOW (binary scaling is bit-at-a-time here; the upstream Parsers form
gets power-of-ten jump tables the way Eisel-Lemire's POW5 works). No
subnormal handling is needed inside that range — BigFloat's exponent field
dwarfs it.
"""
parsebigfloat(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8=UInt8('.');
              prec::Int=precision(BigFloat)) =
    parsebigfloat(buf, i, j, decimal, BigWork(); prec)

function parsebigfloat(buf::Vector{UInt8}, i::Int, j::Int,
                       decimal::UInt8, ws::BigWork; prec::Int=precision(BigFloat))
    prec >= 2 || throw(ArgumentError("prec must be ≥ 2"))
    sp, matched = _matchspecial(buf, i, j)
    matched && return (BigFloat(sp; precision=prec), RC_OK)
    parts, rc = _decompose(buf, i, j, decimal)
    rc == RC_OK || return (BigFloat(0; precision=prec), rc)
    if parts.mant == 0
        z = BigFloat(0; precision=prec)
        return (parts.neg ? -z : z, RC_OK)
    end
    # every significant digit into a BigInt (the parsebigint accumulator, with
    # the decimal byte skipped), tracking the true power of ten. The range test
    # uses the full mantissa exponent, not DecParts.exp10 (which is relative to
    # its truncated 19-digit mantissa).
    M = ws.M
    Base.GMP.MPZ.set_si!(M, 0)
    q, inrange = _bigmantissa!(M, buf, i, Int(parts.digstart), j, decimal)
    inrange || return (BigFloat(0; precision=prec), RC_OVERFLOW)
    # value = M × 10^q = M × 5^q × 2^q — pure integer scaling, one rounding:
    #   q ≥ 0: N = M·5^q is exact and value = N × 2^q
    #   q < 0: N = ⌊M·2^s / 5^-q⌋ with s sized so N keeps ≥ prec+2 bits; the
    #          remainder is the sticky. value = N × 2^(q-s)
    # in-place GMP throughout: M becomes N; rounding decisions read bits
    # without materializing masks (tstbit/scan1); ~4 allocations per value
    MPZ = Base.GMP.MPZ
    sticky = false
    if q >= 0
        q > 0 && MPZ.mul!(M, _pow5big(q))
        e2 = q
    else
        k = -q
        d5 = _pow5big(k)
        s = max(0, prec + 3 + Int(MPZ.sizeinbase(d5, 2)) - Int(MPZ.sizeinbase(M, 2)))
        MPZ.mul_2exp!(M, s % Culong)
        R = ws.R
        MPZ.tdiv_qr!(M, R, M, d5)
        sticky = !iszero(R)
        e2 = q - s
    end
    nb = Int(MPZ.sizeinbase(M, 2))                  # exact for base 2
    if nb > prec
        drop = nb - prec
        rbit = MPZ.tstbit(M, (drop - 1) % Culong)
        # sticky below the round bit: lowest set bit sits under it
        sticky = sticky || (drop > 1 && Int(MPZ.scan1(M, 0)) < drop - 1)
        MPZ.fdiv_q_2exp!(M, drop % Culong)          # M >>= drop (M ≥ 0 here)
        if rbit && (sticky || MPZ.tstbit(M, Culong(0)))
            MPZ.add_ui!(M, 1)
            if Int(MPZ.sizeinbase(M, 2)) > prec     # carry out of the mantissa
                MPZ.fdiv_q_2exp!(M, Culong(1))
                drop += 1
            end
        end
        return (_assemble(M, e2 + drop, parts.neg, prec), RC_OK)
    end
    # exact case (only reachable when q ≥ 0, where sticky is impossible)
    return (_assemble(M, e2, parts.neg, prec), RC_OK)
end

# All significant digits from `digstart` (first significant digit, per
# _decompose) through the end of the digit run, skipping the decimal byte.
# Returns `(M, q, inrange)` with value `M × 10^q` when `inrange`; the
# boolean is false when `abs(q + ndig) > 65536`. Shape is already validated.
function _bigmantissa!(big::BigInt, buf::Vector{UInt8}, i::Int, digstart::Int, j::Int,
                       decimal::UInt8)
    started = false
    acc = Int64(0)
    nacc = 0
    # A decimal point BEFORE the first significant digit ("0.001") puts the
    # whole mantissa in the fraction, and the skipped zeros between the point
    # and digstart are fractional positions too.
    frac = 0
    infrac = false
    ndig = 0
    @inbounds for p in i:(digstart - 1)
        if buf[p] == decimal
            infrac = true
            frac = digstart - p - 1
            break
        end
    end
    k = digstart
    @inbounds while k <= j
        b = buf[k]
        d = b - UInt8('0')
        if d <= 0x09
            acc = acc * 10 + Int64(d)
            nacc += 1
            ndig += 1
            infrac && (frac += 1)
            if nacc == 18
                started = _flushchunk!(big, started, acc, _POW10_18)
                acc = 0
                nacc = 0
            end
        elseif b == decimal
            infrac = true
        else
            break                                    # e/E exponent
        end
        k += 1
    end
    nacc > 0 && (started = _flushchunk!(big, started, acc, @inbounds(_POW10_INT[nacc + 1])))
    expv = 0
    @inbounds if k <= j                              # exponent (validated shape)
        k += 1                                       # skip e/E
        eneg = buf[k] == UInt8('-')
        (eneg || buf[k] == UInt8('+')) && (k += 1)
        offset = ndig - frac
        if j - k + 1 <= 18
            # Every 18-digit exponent fits Int64. This branch covers normal
            # input with no bound arithmetic in the digit loop.
            while k <= j
                expv = expv * 10 + Int(buf[k] - UInt8('0'))
                k += 1
            end
        else
            # Only exponents within this bound can make |q + ndig| <= 65536.
            # Saturating against a fixed constant is unsafe: a long mantissa can
            # cancel that constant and let an enormous reconstructed q reach pow_ui.
            aoff = abs(offset)
            limit = aoff > typemax(Int) - 65536 ? typemax(Int) : aoff + 65536
            limit10, limitdigit = divrem(limit, 10)
            while k <= j
                d = Int(buf[k] - UInt8('0'))
                (expv > limit10 || (expv == limit10 && d > limitdigit)) &&
                    return (0, false)
                expv = expv * 10 + d
                k += 1
            end
        end
        signedexp = eneg ? -expv : expv
        abs(Int128(signedexp) + Int128(offset)) > 65536 && return (0, false)
        return (signedexp - frac, true)
    end
    abs(ndig - frac) > 65536 && return (0, false)
    return (-frac, true)
end

"""
    parseuuid(buf, i, j) -> (UInt128, rc)

The canonical 8-4-4-4-12 dashed hex form, case-insensitive — exactly the
spellings `Base.tryparse(UUID, s)` accepts. Returns the raw UInt128; thin
adapters construct `Base.UUID` (mirroring the CivilParts/Dates split).
"""
function parseuuid(buf::Vector{UInt8}, i::Int, j::Int)
    j - i + 1 == 36 || return (UInt128(0), RC_INVALID)
    @inbounds begin
        (buf[i + 8] == UInt8('-')) & (buf[i + 13] == UInt8('-')) &
        (buf[i + 18] == UInt8('-')) & (buf[i + 23] == UInt8('-')) ||
            return (UInt128(0), RC_INVALID)
    end
    # 8-4-4-4-12 → four 8-hex-char words. The 4-char groups pair up via their
    # low 32 bits (loads at 9|14 and 19|24); every load stays inside the span.
    w1 = _load8(buf, i)
    w2 = (_load8(buf, i + 9) & 0x00000000ffffffff) | (_load8(buf, i + 14) << 32)
    w3 = (_load8(buf, i + 19) & 0x00000000ffffffff) | (_load8(buf, i + 24) << 32)
    w4 = _load8(buf, i + 28)
    v1, ok1 = _hex8(w1)
    v2, ok2 = _hex8(w2)
    v3, ok3 = _hex8(w3)
    v4, ok4 = _hex8(w4)
    ok1 & ok2 & ok3 & ok4 || return (UInt128(0), RC_INVALID)
    return ((UInt128(v1) << 96) | (UInt128(v2) << 64) | (UInt128(v3) << 32) | UInt128(v4), RC_OK)
end

# Eight ASCII hex chars (either case) in one word → (UInt32 value, valid). Byte
# k of `w` is character k, so the first character is the most significant
# nibble of the result. Branch-free: lowercase, range-test digits and a-f
# lanes with the borrow-free trick, pick the nibble as (b & 0x0f) + 9·isalpha
# ('a'..'f' have low nibbles 1..6), then fold the eight nibbles together.
@inline function _hex8(w::UInt64)
    w |= 0x2020202020202020                       # 'A'..'F' → 'a'..'f'; digits unchanged
    # digit lanes: bytes in 0x30..0x39; alpha lanes: bytes in 0x61..0x66
    d = w ⊻ 0x3030303030303030                     # digit ⇒ 0x00..0x09
    a = w ⊻ 0x6060606060606060                     # 'a'..'f' ⇒ 0x01..0x06
    isdig = ((d + 0x7676767676767676) & 0x8080808080808080) ⊻ 0x8080808080808080  # d <= 9 ⇒ no carry into bit 7
    isalp = ((a + 0x7979797979797979) & 0x8080808080808080) ⊻ 0x8080808080808080  # a <= 6
    isalp &= ((a - 0x0101010101010101) & 0x8080808080808080) ⊻ 0x8080808080808080 # a >= 1
    # each lane must be exactly one of the two, and high bytes (>= 0x80) never
    # qualify: exclude them via the byte's own high bit
    hi = w & 0x8080808080808080
    ok = ((isdig | isalp) & ~hi) == 0x8080808080808080
    nib = (w & 0x0f0f0f0f0f0f0f0f) + ((isalp >> 7) * 0x09)   # + 9 on alpha lanes
    # fold 8 nibbles (byte lanes) into 32 bits, first char most significant
    t = ((nib & 0x000f000f000f000f) << 4) | ((nib & 0x0f000f000f000f00) >> 8)
    t = ((t & 0x000000ff000000ff) << 8) | ((t & 0x00ff000000ff0000) >> 16)
    t = ((t & 0x000000000000ffff) << 16) | ((t & 0x0000ffff00000000) >> 32)
    return (UInt32(t & 0xffffffff), ok)
end

# =============================================================================
# bool — exactly true/false (or the caller's explicit lists, matched above)
# =============================================================================

function parsebool(buf::Vector{UInt8}, i::Int, j::Int)
    n = j - i + 1
    @inbounds if n == 4 && buf[i] == UInt8('t') && buf[i+1] == UInt8('r') &&
                 buf[i+2] == UInt8('u') && buf[i+3] == UInt8('e')
        return (true, RC_OK)
    elseif n == 5 && buf[i] == UInt8('f') && buf[i+1] == UInt8('a') &&
           buf[i+2] == UInt8('l') && buf[i+3] == UInt8('s') && buf[i+4] == UInt8('e')
        return (false, RC_OK)
    end
    return (false, RC_INVALID)
end

# =============================================================================
# span utilities — the layer that replaces the quote/sentinel/whitespace
# machinery (structure itself is the tape's job, one level further down)
# =============================================================================

"""
    findcontent(buf, i, j, oq, cq, e) -> (cpos, clen, escaped, rc)

Given a raw field span, locate the content: unquoted spans pass through
untouched (no parsing at all); quoted spans strip the quotes and report whether
interior escape processing is needed. INVALID = malformed quoting (unterminated
open quote or trailing bytes after the close).
"""
function findcontent(buf::Vector{UInt8}, i::Int, j::Int, oq::UInt8, cq::UInt8, e::UInt8)
    @inbounds if i > j || buf[i] != oq
        return (i, j - i + 1, false, RC_OK)
    end
    # quoted: walk interior honoring escapes to find the true close. For the
    # RFC dialect (e == cq) the walk word-scans for the quote byte — runs of
    # ordinary content skip 8 bytes per iteration, which is most of every
    # quoted cell's bytes (this walk runs on EVERY quoted cell, escaped or
    # not). Distinct-escape dialects take the byte walk below.
    k = i + 1
    escaped = false
    if e == cq
        GC.@preserve buf begin
            p = pointer(buf)
            @inbounds while k <= j
                if k + 7 <= j
                    mk = _eqmask8(ltoh(unsafe_load(Ptr{UInt64}(p + k - 1))), cq)
                    if mk == 0
                        k += 8
                        continue
                    end
                    k += trailing_zeros(mk) >> 3
                else
                    while k <= j && buf[k] != cq
                        k += 1
                    end
                    k > j && break
                end
                # buf[k] == cq: pair ⇒ escaped content, else the close
                if k < j && buf[k + 1] == cq
                    escaped = true
                    k += 2
                else
                    return k == j ? (i + 1, j - i - 1, escaped, RC_OK) :
                                    (i + 1, j - i - 1, escaped, RC_INVALID)
                end
            end
        end
        return (i + 1, j - i, escaped, RC_INVALID)   # unterminated
    end
    @inbounds while k <= j
        b = buf[k]
        if b == e && e != cq
            escaped = true
            k += 2
        elseif b == cq
            # close found: valid only if it is the final byte
            return k == j ? (i + 1, j - i - 1, escaped, RC_OK) :
                            (i + 1, j - i - 1, escaped, RC_INVALID)
        else
            k += 1
        end
    end
    return (i + 1, j - i, escaped, RC_INVALID)   # unterminated
end

"""
    matchsentinel(buf, i, j, sentinels) -> Bool

Does the span exactly equal any sentinel string? (Empty spans are the caller's
missing fast path and never reach here.)
"""
function matchsentinel(buf::Vector{UInt8}, i::Int, j::Int, sentinels::Vector{Vector{UInt8}})
    n = j - i + 1
    @inbounds for s in sentinels
        length(s) == n || continue
        k = 1
        while k <= n && buf[i + k - 1] == s[k]
            k += 1
        end
        k > n && return true
    end
    return false
end

"""
    degroup!(scratch, buf, i, j, groupmark, decimal) -> n

Copy the numeric span `[i, j]` into `scratch` with digit-group separators
removed. A separator is valid only BETWEEN two digits in the integer part
(before the decimal point or exponent); group widths are deliberately not
enforced — "1,234,567" and Indian-style "12,34,567" both pass, matching the
lenient behavior CSV consumers expect. Returns the degrouped length, `-1` when
the span contains no separator at all (parse the original span — the common
case costs one scan), or `-2` when a separator is misplaced (leading, trailing,
adjacent to another separator, or in the fraction/exponent).
"""
function degroup!(scratch::Vector{UInt8}, buf::Vector{UInt8}, i::Int, j::Int,
                  gm::UInt8, decimal::UInt8)
    _hasbyte(buf, i, j, gm) || return -1
    n = j - i + 1
    length(scratch) < n && resize!(scratch, max(n, 64))
    m = 0
    intpart = true
    @inbounds for k in i:j
        b = buf[k]
        if b == gm
            intpart || return -2
            (k > i && (buf[k-1] - UInt8('0')) <= 0x09 &&
             k < j && (buf[k+1] - UInt8('0')) <= 0x09) || return -2
        else
            (b == decimal || b == UInt8('e') || b == UInt8('E')) && (intpart = false)
            m += 1
            scratch[m] = b
        end
    end
    return m
end

# Does `buf[i:j]` contain byte `b`? Word-at-a-time (eq-mask) while eight bytes
# remain inside the buffer, byte tail otherwise — the mark pre-scan every cell
# of a grouped column pays, so it must be nearly free when there are no marks.
@inline function _hasbyte(buf::Vector{UInt8}, i::Int, j::Int, b::UInt8)
    k = i
    lim = min(j, length(buf)) - 7
    @inbounds while k <= lim
        _eqmask8(_load8(buf, k), b) != 0 && return true
        k += 8
    end
    @inbounds while k <= j
        buf[k] == b && return true
        k += 1
    end
    return false
end

"""
    parsegroupedint64(buf, i, j, gm) -> (Int64, rc)

`parseint64` for spans that may carry digit-group marks `gm` (`1,234,567`):
exactly `degroup!` + `parseint64` (marks only BETWEEN digits, no leading/
trailing/adjacent marks; group widths lenient), without the scratch copy —
each digit run gathers straight out of the loaded word. Runs longer than
eight digits, or spans within eight bytes of the buffer's end, take the
reference path so nothing reads past the buffer.
"""
parsegroupedint64(buf::Vector{UInt8}, i::Int, j::Int, gm::UInt8) =
    parsegroupedint64(buf, i, j, gm, Vector{UInt8}(undef, 64))

function parsegroupedint64(buf::Vector{UInt8}, i::Int, j::Int, gm::UInt8, scratch::Vector{UInt8})
    i > j && return (zero(Int64), RC_INVALID)
    i0 = i                                       # the reference path re-reads the sign itself
    @inbounds b = buf[i]
    neg = b == UInt8('-')
    (neg | (b == UInt8('+'))) && (i += 1)
    i > j && return (zero(Int64), RC_INVALID)
    j + 8 > length(buf) && return _parsegroupedint64_slow(buf, i0, j, gm, scratch)
    v = zero(UInt64)
    ndig = 0            # significant digits (leading zeros of the whole number excluded)
    k = i
    @inbounds while true
        w = _load8(buf, k)
        # position of the first non-digit lane (exact for the lowest flagged
        # lane; a misclassified high byte only lengthens a run that
        # _rundigits then rejects)
        d = w ⊻ 0x3030303030303030
        nondig = ((d + 0x7676767676767676) & 0x8080808080808080)
        avail = j - k + 1
        firstbad = nondig == 0 ? 8 : (trailing_zeros(nondig) >> 3)
        r = min(firstbad, avail)
        r == 0 && return (zero(Int64), RC_INVALID)   # mark/garbage where a digit must be
        # a run longer than the word (ninth byte still a digit) → reference path
        r == 8 && k + 8 <= j && (buf[k + 8] - UInt8('0')) <= 0x09 &&
            return _parsegroupedint64_slow(buf, i0, j, gm, scratch)
        run, ok = _rundigits(w, r)
        ok || return (zero(Int64), RC_INVALID)
        # digit accounting with the parseint64 leading-zero rule
        if ndig == 0
            lz = 0
            while lz < r && buf[k + lz] == UInt8('0')
                lz += 1
            end
            ndig = r - lz
        else
            ndig += r
        end
        ndig > 19 && return _parsegroupedint64_slow(buf, i0, j, gm, scratch)
        v = v * _P10U[r + 1] + run
        k += r
        k > j && break
        # the byte after a run must be a mark, followed by another digit run
        (buf[k] == gm && k < j) || return (zero(Int64), RC_INVALID)
        k += 1
        (buf[k] - UInt8('0')) <= 0x09 || return (zero(Int64), RC_INVALID)
    end
    if ndig == 19
        lim = neg ? UInt64(9223372036854775808) : UInt64(9223372036854775807)
        v > lim && return (zero(Int64), RC_OVERFLOW)
    end
    return (neg ? -reinterpret(Int64, v) : reinterpret(Int64, v), RC_OK)
end

# reference semantics for the guarded cases: degroup the WHOLE span (sign
# included) into the caller's scratch (degroup! grows it if needed), then
# parseint64 — allocation-free on the column loop's per-chunk scratch
@noinline function _parsegroupedint64_slow(buf::Vector{UInt8}, i::Int, j::Int, gm::UInt8,
                                           scratch::Vector{UInt8})
    n = degroup!(scratch, buf, i, j, gm, 0xff)
    n == -2 && return (zero(Int64), RC_INVALID)
    return n == -1 ? parseint64(buf, i, j) : parseint64(scratch, 1, n)
end

# =============================================================================
# dates & times — CivilParts core (no Dates dependency) + format programs
# =============================================================================

"""
    CivilParts

A parsed civil timestamp: pure integers, no calendar library. `nanosecond`
carries full sub-second precision; adapters truncate per target type.
"""
struct CivilParts
    year::Int32
    month::Int8
    day::Int8
    hour::Int8
    minute::Int8
    second::Int8
    nanosecond::Int32
end
CivilParts() = CivilParts(1, 1, 1, 0, 0, 0, 0)

const _DAYSINMONTH = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
@inline _isleap(y::Integer) = (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0))
@inline function _validymd(y::Integer, m::Integer, d::Integer)
    1 <= m <= 12 || return false
    dim = @inbounds _DAYSINMONTH[m] + ((m == 2 && _isleap(y)) ? 1 : 0)
    return 1 <= d <= dim
end
@inline _validhms(h, mi, s) = (0 <= h <= 23) & (0 <= mi <= 59) & (0 <= s <= 59)

"""
    daysfromcivil(y, m, d) -> Int64

Days since 0000-12-31 (Rata Die), matching `Dates.value(Date(y,m,d))` — this
IS the Dates stdlib's `totaldays` formula (shift the year to start on March 1
so the leap day is the year's last day; then days + month offset + year days),
carried here without a Dates dependency so Dates can one day call it instead.
Equivalence is pinned exhaustively (every day of years -1000..3000 and the
extremes) in the test suite; the earlier Hinnant era/year-of-era form was
identical over ±9999 but ~35% slower.
"""
const _SHIFTEDMONTHDAYS = (306, 337, 0, 31, 61, 92, 122, 153, 184, 214, 245, 275)
function daysfromcivil(y::Integer, m::Integer, d::Integer)
    z = Int64(y) - (m < 3)
    return Int64(d) + @inbounds(_SHIFTEDMONTHDAYS[m]) + 365z + fld(z, 4) - fld(z, 100) +
           fld(z, 400) - 306
end

# --- format programs -----------------------------------------------------------
#
# A compiled pattern is a flat vector of ops. Numeric fields consume 1..width
# digits (fixed = exactly width); literals must match exactly; month-name ops
# consume letters and match against a supplied table. This is the engine that
# Dates-the-stdlib would drive with its locales; here we carry the English
# month names the CSV kernel needs.

struct PatternOp
    kind::UInt8     # 1=year 2=month 3=day 4=hour 5=minute 6=second 7=subsec
                    # 8=literal 9=monthname-abbrev 10=monthname-full
    width::UInt8    # numeric: max digits; fixed ⇒ exactly; literal: byte
    fixed::Bool
end

struct DatePattern
    ops::Vector{PatternOp}
    hasdate::Bool
    hastime::Bool
end

const ENGLISH_MONTHS_ABBR = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
const ENGLISH_MONTHS_FULL = ["January","February","March","April","May","June","July",
                             "August","September","October","November","December"]

"""
    compilepattern(fmt::AbstractString) -> DatePattern

Compile a Dates-style format string (tokens `y m d H M S s u U`, plus literal
separators; repeated letters set the width, `yyyy`-style runs are fixed-width).
Unsupported tokens throw at compile time — configuration errors surface when
the format is pinned, never per cell.
"""
function compilepattern(fmt::AbstractString)
    ops = PatternOp[]
    hasdate = false
    hastime = false
    i = firstindex(fmt)
    while i <= lastindex(fmt)
        c = fmt[i]
        n = 1
        while i + n <= lastindex(fmt) && fmt[i + n] == c
            n += 1
        end
        if c == 'y' || c == 'Y'
            n <= typemax(UInt8) ||
                throw(ArgumentError("year token run exceeds 255 bytes in \"$fmt\""))
            push!(ops, PatternOp(1, UInt8(max(n, 4)), n >= 4)); hasdate = true
        elseif c == 'm'
            push!(ops, PatternOp(2, UInt8(2), n >= 2)); hasdate = true
        elseif c == 'd'
            push!(ops, PatternOp(3, UInt8(2), n >= 2)); hasdate = true
        elseif c == 'H'
            push!(ops, PatternOp(4, UInt8(2), n >= 2)); hastime = true
        elseif c == 'M'
            push!(ops, PatternOp(5, UInt8(2), n >= 2)); hastime = true
        elseif c == 'S'
            push!(ops, PatternOp(6, UInt8(2), n >= 2)); hastime = true
        elseif c == 's'
            push!(ops, PatternOp(7, UInt8(9), false)); hastime = true
        elseif c == 'u'
            push!(ops, PatternOp(9, UInt8(0), false)); hasdate = true
        elseif c == 'U'
            push!(ops, PatternOp(10, UInt8(0), false)); hasdate = true
        elseif c in ('e', 'E', 'Q', 'q')
            # Dates tokens this engine does not support yet — fail at compile time
            throw(ArgumentError("unsupported date format token '$c' in \"$fmt\""))
        elseif isascii(c)
            # any other ASCII char is a literal (Dates' rule: only token letters
            # are special — 'T' in ISO datetime is a plain separator)
            for _ in 1:n
                push!(ops, PatternOp(8, UInt8(c), true))
            end
        else
            throw(ArgumentError("non-ASCII literal '$c' in date format \"$fmt\""))
        end
        i += n
    end
    return DatePattern(ops, hasdate, hastime)
end

# The default ISO patterns, precompiled.
const ISO_DATE     = compilepattern("yyyy-mm-dd")
const ISO_TIME     = compilepattern("HH:MM:SS.s")
const ISO_DATETIME = compilepattern("yyyy-mm-ddTHH:MM:SS.s")

@inline function _readnum(buf, i, j, maxw, fixed)
    v = 0
    k = i
    lim = min(j, i + Int(maxw) - 1)
    @inbounds while k <= lim
        d = buf[k] - UInt8('0')
        d > 0x09 && break
        v > (typemax(Int) - Int(d)) ÷ 10 && return (0, k, false)
        v = v * 10 + Int(d)
        k += 1
    end
    ndig = k - i
    ndig == 0 && return (0, i, false)
    fixed && ndig != Int(maxw) && return (v, k, false)
    return (v, k, true)
end

function _matchname(buf, i, j, table)
    # case-insensitive prefix match against table entries; returns (idx, next, ok)
    @inbounds for (mi, name) in enumerate(table)
        ncu = ncodeunits(name)
        i + ncu - 1 <= j || continue
        ok = true
        for k in 1:ncu
            _lower(buf[i + k - 1]) == _lower(UInt8(codeunit(name, k))) || (ok = false; break)
        end
        ok && return (mi, i + ncu, true)
    end
    return (0, i, false)
end

# --- fixed-width ISO fast paths ----------------------------------------------
# The ISO defaults dominate real data and have fixed shapes; the pattern
# interpreter costs ~18 ns/date walking its op list. These accelerators handle
# exactly the fixed-width spellings ("yyyy-mm-dd" in 10 bytes, the 19-byte
# datetime without subseconds, "HH:MM:SS" in 8) and REJECT to the interpreter
# on any guard failure — equivalence with parsecivil is by construction, and
# only invalid cells (already the problems path) pay both.

@inline _dig(b::UInt8) = b - UInt8('0')

@inline function _iso_ymd(buf::Vector{UInt8}, i::Int)
    @inbounds begin
        (buf[i + 4] == UInt8('-')) & (buf[i + 7] == UInt8('-')) || return (0, 0, 0, false)
        y0 = _dig(buf[i]); y1 = _dig(buf[i + 1]); y2 = _dig(buf[i + 2]); y3 = _dig(buf[i + 3])
        m0 = _dig(buf[i + 5]); m1 = _dig(buf[i + 6])
        d0 = _dig(buf[i + 8]); d1 = _dig(buf[i + 9])
        (y0 <= 0x09) & (y1 <= 0x09) & (y2 <= 0x09) & (y3 <= 0x09) &
        (m0 <= 0x09) & (m1 <= 0x09) & (d0 <= 0x09) & (d1 <= 0x09) ||
            return (0, 0, 0, false)
        return (Int(y0) * 1000 + Int(y1) * 100 + Int(y2) * 10 + Int(y3),
                Int(m0) * 10 + Int(m1), Int(d0) * 10 + Int(d1), true)
    end
end

@inline function _iso_hms(buf::Vector{UInt8}, i::Int)
    @inbounds begin
        (buf[i + 2] == UInt8(':')) & (buf[i + 5] == UInt8(':')) || return (0, 0, 0, false)
        h0 = _dig(buf[i]); h1 = _dig(buf[i + 1])
        m0 = _dig(buf[i + 3]); m1 = _dig(buf[i + 4])
        s0 = _dig(buf[i + 6]); s1 = _dig(buf[i + 7])
        (h0 <= 0x09) & (h1 <= 0x09) & (m0 <= 0x09) & (m1 <= 0x09) &
        (s0 <= 0x09) & (s1 <= 0x09) || return (0, 0, 0, false)
        return (Int(h0) * 10 + Int(h1), Int(m0) * 10 + Int(m1), Int(s0) * 10 + Int(s1), true)
    end
end

"""
    parseiso10(buf, i) -> (CivilParts, rc)

`yyyy-mm-dd` in exactly 10 bytes (caller checks the length). RC_INVALID means
"not this shape or not a real date" — the caller falls through to
[`parsecivil`](@ref), which agrees on every 10-byte input.
"""
@inline function parseiso10(buf::Vector{UInt8}, i::Int)
    y, m, d, ok = _iso_ymd(buf, i)
    (ok && _validymd(y, m, d)) || return (CivilParts(), RC_INVALID)
    return (CivilParts(Int32(y), Int8(m), Int8(d), Int8(0), Int8(0), Int8(0), Int32(0)), RC_OK)
end

"""
    parseiso19(buf, i) -> (CivilParts, rc)

`yyyy-mm-ddTHH:MM:SS` in exactly 19 bytes (no subseconds; those fall through).
"""
@inline function parseiso19(buf::Vector{UInt8}, i::Int)
    @inbounds buf[i + 10] == UInt8('T') || return (CivilParts(), RC_INVALID)
    y, mo, d, okd = _iso_ymd(buf, i)
    h, mi, s, okt = _iso_hms(buf, i + 11)
    (okd && okt && _validymd(y, mo, d) && _validhms(h, mi, s)) ||
        return (CivilParts(), RC_INVALID)
    return (CivilParts(Int32(y), Int8(mo), Int8(d), Int8(h), Int8(mi), Int8(s), Int32(0)), RC_OK)
end

"""
    parseiso8(buf, i) -> (CivilParts, rc)

`HH:MM:SS` in exactly 8 bytes.
"""
@inline function parseiso8(buf::Vector{UInt8}, i::Int)
    h, mi, s, ok = _iso_hms(buf, i)
    (ok && _validhms(h, mi, s)) || return (CivilParts(), RC_INVALID)
    return (CivilParts(Int32(1), Int8(1), Int8(1), Int8(h), Int8(mi), Int8(s), Int32(0)), RC_OK)
end

"""
    parsecivil(buf, i, j, pat::DatePattern) -> (CivilParts, rc)

Run a compiled pattern over the exact span. Trailing sub-second precision
beyond the pattern (`.s` matching 1–9 digits) is scaled to nanoseconds. The
whole span must be consumed. Calendar validity (month/day ranges, leap years)
is checked here — structurally valid but impossible dates are INVALID.
"""
function parsecivil(buf::Vector{UInt8}, i::Int, j::Int, pat::DatePattern)
    y = 1; mo = 1; dy = 1; h = 0; mi = 0; s = 0; ns = 0
    k = i
    ops = pat.ops
    oi = 1
    @inbounds while oi <= length(ops)
        op = ops[oi]
        if op.kind == 8
            (k <= j && buf[k] == op.width) || begin
                # a trailing optional subsecond group (".s" at pattern end) may be absent
                if oi + 1 <= length(ops) && ops[oi + 1].kind == 7 && oi + 1 == length(ops) && k > j
                    oi = length(ops) + 1
                    break
                end
                return (CivilParts(), RC_INVALID)
            end
            k += 1
        elseif op.kind == 9 || op.kind == 10
            idx, k2, ok = _matchname(buf, k, j, op.kind == 9 ? ENGLISH_MONTHS_ABBR : ENGLISH_MONTHS_FULL)
            ok || return (CivilParts(), RC_INVALID)
            mo = idx
            k = k2
        elseif op.kind == 7
            v, k2, ok = _readnum(buf, k, j, 9, false)
            ok || return (CivilParts(), RC_INVALID)
            nd = k2 - k
            ns = v * Int(10)^(9 - nd)
            k = k2
        else
            v, k2, ok = _readnum(buf, k, j, op.width, op.fixed)
            ok || return (CivilParts(), RC_INVALID)
            if op.kind == 1
                y = v
            elseif op.kind == 2
                mo = v
            elseif op.kind == 3
                dy = v
            elseif op.kind == 4
                h = v
            elseif op.kind == 5
                mi = v
            else
                s = v
            end
            k = k2
        end
        oi += 1
    end
    k <= j && return (CivilParts(), RC_INVALID)          # unconsumed bytes
    pat.hasdate && !_validymd(y, mo, dy) && return (CivilParts(), RC_INVALID)
    pat.hastime && !_validhms(h, mi, s) && return (CivilParts(), RC_INVALID)
    typemin(Int32) <= y <= typemax(Int32) || return (CivilParts(), RC_INVALID)
    return (CivilParts(Int32(y), Int8(mo), Int8(dy), Int8(h), Int8(mi), Int8(s), Int32(ns)), RC_OK)
end

end # module KernelValues

# =============================================================================
# Dates adapters — the ONLY section that touches the Dates stdlib. When this
# layer moves to Base, these functions move to Dates itself.
# =============================================================================

module KernelValuesDates

using ..KernelValues
using Dates

export todate, todatetime, totime

function todate(c::KernelValues.CivilParts)
    return Dates.Date(Dates.UTD(KernelValues.daysfromcivil(c.year, c.month, c.day)))
end

function todatetime(c::KernelValues.CivilParts)
    days = KernelValues.daysfromcivil(c.year, c.month, c.day)
    ms = ((Int64(c.hour) * 60 + c.minute) * 60 + c.second) * 1000 + c.nanosecond ÷ 1_000_000
    return Dates.DateTime(Dates.UTM((days - 1) * 86_400_000 + ms + 86_400_000))
end

function totime(c::KernelValues.CivilParts)
    return Dates.Time(Dates.Nanosecond(((Int64(c.hour) * 60 + c.minute) * 60 + c.second) * 1_000_000_000 + c.nanosecond))
end

end # module KernelValuesDates
