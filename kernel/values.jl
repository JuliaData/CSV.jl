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
  * **No dependencies in the core.** No GMP/BigInt, no MPFR/BigFloat, no libc.
    The float slow path is Tao's simple decimal conversion over a fixed-size
    digit buffer (correct rounding for Float64 never needs more than 768
    significant digits). The Eisel–Lemire powers-of-five table is computed at
    precompile time by `_buildpow5` using a minimal little-endian limb array
    with exactly the operations table generation needs (mul-by-5, shifted
    compare/subtract, one restoring division) — machinery that never escapes
    the builder.
  * **Bootstrap-clean.** No `@generated`, no `eval`, no closures over mutable
    state, no allocation on any hot path — plain loops and bit ops, compilable
    early enough for Base adoption and `--trim`.
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
reported as OVERFLOW so the CSV type lattice can promote Int64 → Float64.
"""
module KernelValues

export RC_OK, RC_INVALID, RC_OVERFLOW, degroup!,
       parseint64, parsefloat64, parsebool, findcontent, matchsentinel,
       CivilParts, parsecivil, daysfromcivil, DatePattern, compilepattern

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
OVERFLOW when the digits are well-formed but exceed Int64 (the caller's cue to
promote to Float64), INVALID otherwise.
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

"""
    parsefloat64(buf, i, j, decimal=UInt8('.')) -> (Float64, rc)

Parse `buf[i:j]` as a `Float64` with correct (round-half-even) rounding for
every input — no C, no BigFloat: Clinger's exact small case, then Eisel–Lemire,
then simple-decimal-conversion for the rare ambiguous/subnormal cases.
Accepts sign, digits, one `decimal` byte, optional e/E exponent, and the
case-insensitive spellings Inf/Infinity/NaN. Overflow → ±Inf with OK.

Structured as an @inline hot core plus a thin wrapper owning the cold tail:
with everything in one body, the presence of the tier-3 call site pessimized
the hot path ~7x (28ns → 203ns measured; neither @noinline on the callee nor
an inference barrier recovered it — a signature-identical dummy callee showed
no penalty). The split shape measures at the composed-pipeline floor. Worth a
minimized upstream Julia issue.
"""
@inline function _parsefloat_core(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
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

function parsefloat64(buf::Vector{UInt8}, i::Int, j::Int, decimal::UInt8)
    v, rc, done = _parsefloat_core(buf, i, j, decimal)
    done && return (v, rc)
    return (_sdc(buf, i, j, v < 0, decimal), RC_OK)
end
parsefloat64(buf::Vector{UInt8}, i::Int, j::Int; decimal::UInt8=UInt8('.')) =
    parsefloat64(buf, i, j, decimal)

const _POW10 = Float64[10.0^k for k in 0:22]

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
    # quoted: walk interior honoring escapes to find the true close
    k = i + 1
    escaped = false
    @inbounds while k <= j
        b = buf[k]
        if b == e && e != cq
            escaped = true
            k += 2
        elseif b == cq
            if e == cq && k < j && buf[k + 1] == cq
                escaped = true
                k += 2
            else
                # close found: valid only if it is the final byte
                return k == j ? (i + 1, j - i - 1, escaped, RC_OK) :
                                (i + 1, j - i - 1, escaped, RC_INVALID)
            end
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
    has = false
    @inbounds for k in i:j
        if buf[k] == gm
            has = true
            break
        end
    end
    has || return -1
    n = j - i + 1
    length(scratch) < n && resize!(scratch, max(n, 64))
    m = 0
    intpart = true
    @inbounds for k in i:j
        b = buf[k]
        if b == gm && intpart
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

Days since 0000-12-31 (Rata Die), matching `Dates.value(Date(y,m,d))`. Pure
integer arithmetic (Hinnant's civil-days algorithm, shifted to the Rata Die
epoch Dates uses).
"""
function daysfromcivil(y::Integer, m::Integer, d::Integer)
    yy = Int64(y) - (m <= 2)
    era = fld(yy, 400)
    yoe = yy - era * 400
    doy = fld(153 * (m + (m > 2 ? -3 : 9)) + 2, 5) + d - 1
    doe = yoe * 365 + fld(yoe, 4) - fld(yoe, 100) + doy
    return era * 146097 + doe - 305
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
