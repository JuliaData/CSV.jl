# CompactString — the inline-else-view string type (the "German strings" /
# Arrow StringView layout) and its user-facing vector.
#
# This file is deliberately independent of the CSV kernel: it needs only Base.
# Nothing here knows about quotes, escapes, chunks, or pooling — those live in
# core.jl (StringColumn staging, the unescape helpers, InlineTable/PoolSegment).
# It is the unit that could become a shared package (Arrow.jl's StringView
# columns want the same 16-byte payload; see the CompactString/Arrow note in
# the review) — keep it that way: no references to kernel types.

# --- inline-else-view strings (the "German strings" / Arrow StringView layout) --
#
# Every string cell is one 16-byte payload:
#   a: bits 0..31 = content length as Int32 (-1 ⇒ missing);
#      bits 32..63 = content bytes 1..4 (the full bytes when inline, the PREFIX
#      when a view — prefixes make equality's fast path branch-free)
#   b: len ≤ 12 ⇒ content bytes 5..12 (zero-padded);
#      len > 12 ⇒ bits 0..31 = Int32 BUFFER INDEX (0 = the input buffer, zero
#      copy; 1 = the column's `extra` buffer, where escaped values are
#      unescaped once at parse time), bits 32..63 = Int32 0-based byte OFFSET
#      of the content within that buffer
# Byte packing is by explicit shifts, so the layout is endianness-independent.
# This IS Arrow's StringView entry, byte for byte (12-byte inline, 4-byte
# prefix, int32 buffer index + int32 offset): a payload vector hands off to
# Arrow as a views buffer with no rewrite, and Arrow view arrays come back
# the same way. Arrow's int32 offsets are the reason buffers must stay under
# 2 GiB (the production plan's chunk-owned buffers); `view_payload` refuses
# larger positions.
struct CompactStringPayload
    a::UInt64
    b::UInt64
end
const PAYLOAD_MISSING = CompactStringPayload(UInt64(0xffffffff), zero(UInt64))
const COMPACTSTRING_INLINE = 12
const EMPTY_BYTES = UInt8[]

@inline cslen(p::CompactStringPayload) = reinterpret(Int32, p.a % UInt32)
# Long-entry word (Arrow StringView's second word): buffer index and 0-based
# byte offset within that buffer; `cspos` is the 1-based Julia position.
@inline csbufidx(p::CompactStringPayload) = reinterpret(Int32, p.b % UInt32)
@inline csoffset(p::CompactStringPayload) = reinterpret(Int32, (p.b >> 32) % UInt32)
@inline cspos(p::CompactStringPayload) = Int(csoffset(p)) + 1
@inline _viewword(bufidx::Integer, offset0::Integer) =
    UInt64(bufidx % UInt32) | (UInt64(offset0 % UInt32) << 32)

# Two overlapping little-endian loads gather up to 12 content bytes branch-free;
# the byte-loop fallback only runs within 11 bytes of the buffer's end (loads
# must not read past it). This sits on the hot path of every short string cell.
@inline function inline_payload(src::Vector{UInt8}, pos::Int, len::Int)
    if pos + 11 <= length(src)
        GC.@preserve src begin
            p = pointer(src, pos)
            lo = ltoh(unsafe_load(Ptr{UInt64}(p)))           # content bytes 1..8
            hi = ltoh(unsafe_load(Ptr{UInt64}(p + 4)))       # content bytes 5..12
        end
        m4 = len >= 4 ? 0x00000000ffffffff : (UInt64(1) << (8 * len)) - 1
        nb = max(len - 4, 0)
        m8 = nb >= 8 ? typemax(UInt64) : (UInt64(1) << (8 * nb)) - 1
        return CompactStringPayload(UInt64(len % UInt32) | ((lo & m4) << 32), hi & m8)
    end
    a = UInt64(len % UInt32)
    b = zero(UInt64)
    @inbounds for i in 1:min(len, 4)
        a |= UInt64(src[pos + i - 1]) << (32 + 8 * (i - 1))
    end
    @inbounds for i in 5:len
        b |= UInt64(src[pos + i - 1]) << (8 * (i - 5))
    end
    return CompactStringPayload(a, b)
end


# `bufidx`/`offset0` are the entry's Arrow words: which buffer (0 = input,
# 1 = extra) and the 0-based byte offset of the content within it. `srcpos`
# is the 1-based position of the same content in `src` (the buffer the
# prefix is read from). len > 12 guarantees the 4-byte prefix load is
# in-bounds. Offsets beyond Int32 cannot be represented — buffers must stay
# under 2 GiB.
@inline function view_payload(src::Vector{UInt8}, srcpos::Int, len::Int,
                              bufidx::Integer, offset0::Integer)
    (0 <= offset0 <= typemax(Int32) && 0 <= bufidx <= typemax(Int32)) ||
        throw(ArgumentError("CompactString view (buffer $bufidx, offset $offset0) " *
                            "does not fit Arrow's Int32 view words; buffers must stay under 2 GiB"))
    GC.@preserve src begin
        pre = ltoh(unsafe_load(Ptr{UInt32}(pointer(src, srcpos))))
    end
    a = UInt64(len % UInt32) | (UInt64(pre) << 32)
    return CompactStringPayload(a, _viewword(bufidx, offset0))
end

# The same entry re-pointed `base` bytes further into its buffer — what
# stitching per-chunk `extra` buffers into one column-owned buffer needs.
@inline function rebase_payload(p::CompactStringPayload, base::Integer)
    off = Int(csoffset(p)) + Int(base)
    0 <= off <= typemax(Int32) ||
        throw(ArgumentError("rebased CompactString view offset $off does not fit " *
                            "Arrow's Int32 view offset; buffers must stay under 2 GiB"))
    return CompactStringPayload(p.a, _viewword(csbufidx(p), off))
end

"""
    CompactString <: AbstractString

A kernel string value: 16-byte payload plus the byte vector long values view
into (a shared empty vector for inline values). Byte access, direct comparisons,
and iteration do not allocate; they use the inline bytes or retained buffer.
Hashing and ordering operate on the payload bytes without allocation and agree
with `String`. `String(s)` (or `materialize` on the column) copies out.
Lifetime: a view pins its buffer, exactly like today's `PosLenString` — the
production compaction story is `materialize`.
"""
struct CompactString <: AbstractString
    p::CompactStringPayload
    data::Vector{UInt8}    # dereferenced only when len > COMPACTSTRING_INLINE
end

Base.ncodeunits(s::CompactString) = Int(cslen(s.p))
Base.codeunit(::CompactString) = UInt8
Base.@propagate_inbounds function Base.codeunit(s::CompactString, i::Int)
    @boundscheck 1 <= i <= ncodeunits(s) || throw(BoundsError(s, i))
    len = cslen(s.p)
    if len <= COMPACTSTRING_INLINE
        return i <= 4 ? (s.p.a >> (32 + 8 * (i - 1))) % UInt8 :
                        (s.p.b >> (8 * (i - 5))) % UInt8
    else
        return @inbounds s.data[cspos(s.p) + i - 1]
    end
end

function Base.isvalid(s::CompactString, i::Int)
    1 <= i <= ncodeunits(s) || return false
    @inbounds b = codeunit(s, i)
    b & 0xc0 == 0x80 || return true
    i > 1 || return true
    @inbounds b = codeunit(s, i - 1)
    0xc0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 2 || return true
    @inbounds b = codeunit(s, i - 2)
    0xe0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 3 || return true
    @inbounds b = codeunit(s, i - 3)
    return !(0xf0 <= b <= 0xf7)
end

# UTF-8 iteration mirroring `String`'s tolerant behavior: Julia `Char`s ARE the
# UTF-8 bytes left-aligned in 32 bits, and a malformed sequence yields the bytes
# consumed so far as an (invalid) Char. Pinned against the String oracle by a
# randomized test.
function Base.iterate(s::CompactString, i::Int=1)
    i > ncodeunits(s) && return nothing
    @inbounds b1 = codeunit(s, i)
    b1 < 0x80 && return (reinterpret(Char, UInt32(b1) << 24), i + 1)
    l = b1 < 0xc0 ? 1 : b1 < 0xe0 ? 2 : b1 < 0xf0 ? 3 : b1 < 0xf8 ? 4 : 1
    n = ncodeunits(s)
    c = UInt32(b1) << 24
    j = 1
    @inbounds while j < l && i + j <= n
        nb = codeunit(s, i + j)
        (nb & 0xc0) == 0x80 || break
        c |= UInt32(nb) << (24 - 8 * j)
        j += 1
    end
    return (reinterpret(Char, c), i + j)
end

# Base's generic AbstractString length is isvalid-count-based, which undercounts
# malformed inputs (String yields each bare continuation byte as its own invalid
# Char). Count by iteration so length/collect agree with the String oracle.
function Base.length(s::CompactString)
    n = 0
    for _ in s
        n += 1
    end
    return n
end

function Base.:(==)(x::CompactString, y::CompactString)
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    if n <= COMPACTSTRING_INLINE
        return x.p.a == y.p.a && x.p.b == y.p.b   # payload holds the full content
    end
    x.p.a == y.p.a || return false                # length + 4-byte prefix reject
    GC.@preserve x y begin
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, cspos(x.p)),
                     pointer(y.data, cspos(y.p)), n) == 0
    end
end
# Direct byte comparison against String — Base's generic AbstractString ==
# decodes chars, which is an order of magnitude slower on this hot path
# (filtering/grouping compare CompactString columns against String literals constantly).
function Base.:(==)(x::CompactString, y::Union{String, SubString{String}})
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    GC.@preserve x y begin
        py = pointer(y)
        if n <= COMPACTSTRING_INLINE
            @inbounds for i in 1:n
                codeunit(x, i) == unsafe_load(py, i) || return false
            end
            return true
        end
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, cspos(x.p)), py, n) == 0
    end
end
Base.:(==)(y::Union{String, SubString{String}}, x::CompactString) = x == y

# Ordering: memcmp over the bytes, exactly like String's `cmp` (Base's generic
# AbstractString fallback iterates chars — measured 15-45x slower on sortperm).
# Inline×inline compares in registers; view×view goes straight to memcmp on
# the retained buffers; only the mixed case materializes a stack scratch.
# Raw payload words with content byte k at byte k (byte 1 = LSB of w1) —
# bit-defined, so endian-independent.
@inline _cs_words(s::CompactString) =
    ((s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32), s.p.b >> 32)
@inline _cs_scratch(s::CompactString) = map(htol, _cs_words(s))
function Base.cmp(x::CompactString, y::CompactString)
    nx, ny = ncodeunits(x), ncodeunits(y)
    if (nx <= COMPACTSTRING_INLINE) & (ny <= COMPACTSTRING_INLINE)
        # Register compare in memcmp order: payload words are zero-padded past
        # each length, so the first differing big-endian word decides by the
        # first differing byte; words all equal means the shared prefix
        # matches and any longer side is all-NUL past the shorter — exactly
        # memcmp(min bytes) then the length tiebreak. The non-short-circuit
        # `&` (one branch) and falling into the ORIGINAL unified tail below
        # measured strictly faster than a dedicated view×view branch — 2.7×
        # on inline-heavy sorts, 1.3× on view-heavy — adjudicated by
        # interleaved same-process A/B.
        w1x, w2x = _cs_words(x)
        w1y, w2y = _cs_words(y)
        a, b = bswap(w1x), bswap(w1y)
        a == b || return a < b ? -1 : 1
        a, b = bswap(w2x), bswap(w2y)
        a == b || return a < b ? -1 : 1
        return cmp(nx, ny)
    end
    n = min(nx, ny)
    rx = Ref(_cs_scratch(x)); ry = Ref(_cs_scratch(y))
    GC.@preserve x y rx ry begin
        px = nx <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, cspos(x.p))
        py = ny <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, ry)) :
             pointer(y.data, cspos(y.p))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, py, n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
function Base.cmp(x::CompactString, y::Union{String, SubString{String}})
    nx, ny = ncodeunits(x), ncodeunits(y)
    n = min(nx, ny)
    rx = Ref(_cs_scratch(x))
    GC.@preserve x y rx begin
        px = nx <= COMPACTSTRING_INLINE ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, cspos(x.p))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, pointer(y), n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
Base.cmp(y::Union{String, SubString{String}}, x::CompactString) = -cmp(x, y)
Base.isless(x::CompactString, y::CompactString) = cmp(x, y) < 0
Base.isless(x::CompactString, y::Union{String, SubString{String}}) = cmp(x, y) < 0
Base.isless(y::Union{String, SubString{String}}, x::CompactString) = cmp(y, x) < 0

# hash must agree with `String`'s so mixed Dict{String}/CompactString use is sound.
# is the correctness-first choice — the production version shares InlineStrings'
# memhash approach (which is exactly the private-API exposure CSV.jl #1164 is
# about, so the kernel does not copy it).
# hash contract: hash(cs) == hash(String(cs)) — CompactStrings are Dict keys
# next to Strings. Base hashes a String as memhash(bytes, len, seed) + seed;
# we run the same C hash over the bytes we already have: the retained buffer
# for views, a stack copy of the payload words for inline strings. No String
# allocation on either path.
# Base's String hash over a raw pointer, on both hashing generations:
#   ≤ 1.12  memhash(bytes, n, seed) + seed  with seed = h + memhash_seed
#   ≥ 1.13  hash_bytes(ptr, n, UInt64(h), HASH_SECRET) % UInt   (rapidhash)
# The gate is on the API that exists, not the version number.
@static if isdefined(Base, :hash_bytes) && isdefined(Base, :HASH_SECRET)
    @inline _stringhash(p::Ptr{UInt8}, n::Int, h::UInt) =
        Base.hash_bytes(p, n, UInt64(h), Base.HASH_SECRET) % UInt
else
    @inline function _stringhash(p::Ptr{UInt8}, n::Int, h::UInt)
        h += Base.memhash_seed
        return ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), p, n, h % UInt32) + h
    end
end

function Base.hash(s::CompactString, h::UInt)
    n = ncodeunits(s)
    if n > COMPACTSTRING_INLINE
        GC.@preserve s begin
            return _stringhash(pointer(s.data, cspos(s.p)), n, h)
        end
    end
    # inline: bytes 1-4 are the high 32 bits of `a`, bytes 5-12 are `b` —
    # pack them contiguously into two little-endian words: word 1 = bytes 1-8
    # = (a>>32) | (low 32 bits of b) << 32, word 2 = bytes 9-12 = b >> 32
    w1 = (s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32)
    w2 = s.p.b >> 32
    scratch = (htol(w1), htol(w2))
    r = Ref(scratch)
    GC.@preserve r begin
        p = Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, r))
        return _stringhash(p, n, h)
    end
end

function Base.String(s::CompactString)
    n = ncodeunits(s)
    if n > COMPACTSTRING_INLINE
        # view: one memcpy out of the retained buffer
        GC.@preserve s begin
            return unsafe_string(pointer(s.data, cspos(s.p)), n)
        end
    end
    out = Vector{UInt8}(undef, n)
    @inbounds for i in 1:n
        out[i] = codeunit(s, i)
    end
    return String(out)
end
Base.convert(::Type{String}, s::CompactString) = String(s)
Base.Symbol(s::CompactString) = Symbol(String(s))
Base.promote_rule(::Type{CompactString}, ::Type{String}) = String

function Base.write(io::IO, s::CompactString)
    n = 0
    @inbounds for i in 1:ncodeunits(s)
        n += write(io, codeunit(s, i))
    end
    return n
end
Base.print(io::IO, s::CompactString) = (write(io, s); nothing)

# The user-facing string column. getindex returns a `CompactString` (or `missing`) with
# NO allocation: inline values live in the payload, long values view into `buf`
# (input) or `extra` (unescaped-at-parse-time). `materialize` copies out to
# `Vector{String}`, detaching from both buffers.
struct CompactStringVector{ELT} <: AbstractVector{ELT}
    payloads::Vector{CompactStringPayload}
    buf::Vector{UInt8}
    extra::Vector{UInt8}
end
Base.size(v::CompactStringVector) = size(v.payloads)
Base.@propagate_inbounds @inline function Base.getindex(v::CompactStringVector{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = cslen(p)
    len < 0 && return missing
    len <= COMPACTSTRING_INLINE && return CompactString(p, EMPTY_BYTES)
    return CompactString(p, csbufidx(p) == 0 ? v.buf : v.extra)
end
# All-present columns skip the missing branch entirely — the concrete return
# type is what lets access compile down to zero allocations.
Base.@propagate_inbounds @inline function Base.getindex(v::CompactStringVector{CompactString}, i::Int)
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = cslen(p)
    len <= COMPACTSTRING_INLINE && return CompactString(p, EMPTY_BYTES)
    return CompactString(p, csbufidx(p) == 0 ? v.buf : v.extra)
end

function materialize(v::CompactStringVector{ELT}) where {ELT}
    out = Vector{ELT === CompactString ? String : Union{String, Missing}}(undef, length(v))
    scratch = Vector{UInt8}(undef, 16)   # inline payloads reconstruct via two word stores
    GC.@preserve scratch begin
        q = pointer(scratch)
        @inbounds for i in eachindex(v.payloads)
            p = v.payloads[i]
            len = cslen(p)
            if len < 0
                out[i] = missing
            elseif len <= COMPACTSTRING_INLINE
                unsafe_store!(Ptr{UInt64}(q), htol((p.a >> 32) | (p.b << 32)))
                unsafe_store!(Ptr{UInt64}(q + 8), htol(p.b >> 32))
                out[i] = unsafe_string(q, len)
            else
                src = csbufidx(p) == 0 ? v.buf : v.extra
                GC.@preserve src begin
                    out[i] = unsafe_string(pointer(src, cspos(p)), len)
                end
            end
        end
    end
    return out
end
