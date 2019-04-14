function skipto(tape, idx, cur, dest)
    cur >= dest && return idx
    for _ = 1:(dest-cur)
        while !newline(tape[idx])
            idx += 2
        end
    end
    return idx
end

getname(buf, tape, idx, i) = getlen(tape[idx]) == 0 ? Symbol(:Column, i) : Symbol(WeakRefString(pointer(buf, getoff(tape[idx])), getlen(tape[idx])))

function getcolumnnames(buf, tape, header::Integer, datarow)
    if header <= 0
        idx = skipto(tape, 1, 1, datarow)
        dataidx = idx
        idx = skipto(tape, idx, 0, 1)
        ncols = div(idx - dataidx, 2)
        columnnames = [Symbol(:Column, i) for i = 1:ncols]
    else
        idx = skipto(tape, 1, 1, header)
        startidx = idx
        while !newline(tape[idx])
            idx += 2
        end
        columnnames = [getname(buf, tape, idx, i) for (i, idx) in enumerate(startidx:2:idx)]
        if datarow != header + 1
            dataidx = skipto(tape, idx, header+1, datarow)
        else
            dataidx = idx + 2
        end
    end
    return dataidx, columnnames
end

function getcolumnnames(buf, tape, header::AbstractRange, datarow)
    idx = skipto(tape, 1, 1, first(header))
    startidx = idx
    while !newline(tape[idx])
        idx += 2
    end
    ncols = div(idx - startidx, 2)
    columnnames = [getname(buf, tape, idx, i) for (i, idx) in enumerate(startidx:2:idx)]
    for _ = (header - 1)
        for i = 1:ncols
            columnnames = Symbol(columnnames[i], :_, getname(buf, tape, idx, i))
            idx += 2
        end
    end
    if datarow != last(header) + 1
        idx = skipto(tape, idx, last(header) + 1, datarow)
    end
    return idx, columnnames
end

function getcolumnnames(buf, tape, header::Vector, datarow)
    idx = skipto(tape, 1, 1, datarow)
    return idx, [Symbol(x) for x in header]
end

@generated function compress(::Val{32}, bm::Vec{32, Bool})
    decl = "declare i32 @llvm.x86.avx2.pmovmskb(<32 x i8>)"
    ir = """
        ; convert byte vector to word vector
        %a.i16 = bitcast <32 x i8> %0 to <16 x i16>
        ; psllw
        %b.i16 = shl <16 x i16> %a.i16, <i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7, i16 7>
        %b.i8  = bitcast <16 x i16> %b.i16 to <32 x i8>
        ; pmovmskb
        %r.i32 = call i32 @llvm.x86.avx2.pmovmskb(<32 x i8> %b.i8)
        ret i32 %r.i32
    """
    quote
        Base.@_inline_meta
        Base.llvmcall($(decl, ir), UInt32, Tuple{NTuple{32, Base.VecElement{Bool}}}, bm.elts)
    end
end

_mm_clmulepi64_si128(a, b) = ccall("llvm.x86.pclmulqdq", llvmcall, NTuple{2, Base.VecElement{UInt64}}, (NTuple{2, Base.VecElement{UInt64}}, NTuple{2, Base.VecElement{UInt64}}, Int8), a.elts, b.elts, 0)

pmovmskb(bm) = ccall("llvm.x86.avx2.pmovmskb", llvmcall, UInt32, (NTuple{32, Base.VecElement{UInt8}},), bm.elts)
function eqmask(a::Vec{64, UInt8}, b::Vec{64, UInt8})
    c = reinterpret(Vec{64, UInt8}, a == b) << 7
    r1 = pmovmskb(shufflevector(c, Val{(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31)}))
    r2 = pmovmskb(shufflevector(c, Val{(32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63)}))
    return (UInt64(r2) << 32) | r1
end

const NEWLINES = Vec{64, UInt8}('\n' % UInt8)
const RETURNS  = Vec{64, UInt8}('\r' % UInt8)
const FF = Vec{2, UInt64}(0xffffffffffffffff)

roundup(a, n) = (a + (n - 1)) & ~(n - 1)

# buf = Mmap.mmap(file);
# d = UInt8(',')
# oq = cq = eq = UInt8('"')
# quotetype = CSV.SAME
# newlinetype = CSV.NEWLINE
# limit = nothing
function detectstructure(buf, d, oq, cq, eq, ::Val{quotetype}, ::Val{newlinetype}, ::Val{limit}) where {quotetype, newlinetype, limit}
    openquotes = Vec{64, UInt8}(oq)
    closequotes = Vec{64, UInt8}(cq)
    escapes = Vec{64, UInt8}(eq)
    delims = Vec{64, UInt8}(d)
    newlines = newlinetype == CSV.RETURN ? CSV.RETURNS : CSV.NEWLINES
    len = CSV.roundup(length(buf), 64)
    tape = Mmap.mmap(Vector{UInt64}, div(len, 2))
    niterations = div(len, 64)
    pos = UInt64(1)
    ptr = pointer(buf, pos)
    prev_len = UInt16(0)
    prev_iter_ends_odd_backslash = UInt64(0)
    prev_iter_inside_quote = UInt64(0)
    UN = UInt64(64)
    idx = 1
    rows = 0
    for j = 1:niterations
        bytes = vload(Vec{64, UInt8}, ptr)
        quotemask, prev_iter_ends_odd_backslash, prev_iter_inside_quote = CSV.getquotemask(Val(quotetype), bytes, openquotes, closequotes, escapes, prev_iter_ends_odd_backslash, prev_iter_inside_quote)
        commamask = CSV.eqmask(bytes, delims)
        newlinemask = CSV.eqmask(bytes, newlines) & ~quotemask
        if newlinetype == CRLF
            returnmask = CSV.eqmask(bytes, RETURNS) & ~quotemask
        end
        rows += count_ones(newlinemask)
        mask = (commamask & ~quotemask) | newlinemask
        off = UInt16(0)
        while mask !== UInt64(0)
            tz = unsafe_trunc(UInt16, Base.cttz_int(mask))
            len = (tz - off) + prev_len
            if newlinetype == CRLF
                len -= unsafe_trunc(UInt16, (returnmask >> tz) & UInt64(1))
            end
            @inbounds tape[idx] = (pos << 16) | (len | ((newlinemask << (16 - Core.bitcast(Int16, tz) - 1)) & 0x8000))
            idx += 2
            mask &= (mask - UInt64(1))
            pos += len + UInt64(1)
            prev_len = UInt16(0)
            off = tz + UInt16(1)
        end
        prev_len += UN - off
        ptr += UN
        if limit !== nothing
            rows > limit && break
        end
    end
    if prev_len > 0
        tape[idx] = (pos << 16) | (prev_len | 0x800)
        idx += 2
    end
    return tape, idx, rows
end

@inline function getquotemask(::Val{quotetype}, bytes, openquotes, closequotes, escapes, prev_iter_ends_odd_backslash, prev_iter_inside_quote) where {quotetype}
    evenbits = 0x5555555555555555
    oddbits = ~evenbits
    if quotetype == SAME
        bs_bits = eqmask(bytes, openquotes)
    else
        bs_bits = eqmask(bytes, escapes)
    end
    start_edges = bs_bits & ~(bs_bits << 1)
    even_start_mask = xor(evenbits, prev_iter_ends_odd_backslash)
    even_starts = start_edges & even_start_mask
    odd_starts = start_edges & ~even_start_mask
    even_carries = bs_bits + even_starts
    odd_carries, iter_ends_odd_backslash = Base.add_with_overflow(bs_bits, odd_starts)
    odd_carries |= prev_iter_ends_odd_backslash
    prev_iter_ends_odd_backslash = iter_ends_odd_backslash ? UInt64(0x1) : UInt64(0)
    even_carry_ends = even_carries & ~bs_bits
    odd_carry_ends = odd_carries & ~bs_bits
    even_start_odd_end = even_carry_ends & oddbits
    odd_start_even_end = odd_carry_ends & evenbits
    odd_ends = even_start_odd_end | odd_start_even_end
    if quotetype == SAME
        quote_bits = eqmask(bytes, openquotes)
        quote_bits = quote_bits & (odd_ends >> 1)
    elseif quotetype == ESCAPE
        quote_bits = eqmask(bytes, openquotes)
        quote_bits = quote_bits & ~odd_ends
    elseif quotetype == OPENCLOSE
        openquote_bits = eqmask(bytes, openquotes)
        closequote_bits = eqmask(bytes, closequotes)
        quote_bits = (openquote_bits | closequote_bits) & ~odd_ends
    end
    quote_mask = _mm_clmulepi64_si128(Vec{2, UInt64}((quote_bits, UInt64(0))), FF)[1].value
    quote_mask = xor(quote_mask, prev_iter_inside_quote)
    prev_iter_inside_quote = Core.bitcast(UInt64, Core.bitcast(Int64, quote_mask) >> 63)
    return quote_mask, prev_iter_ends_odd_backslash, prev_iter_inside_quote
end

struct ByteValueCounter
    counts::Vector{Int64}
    ByteValueCounter() = new(zeros(Int64, 256))
end

function incr!(c::ByteValueCounter, b::UInt8)
    @inbounds c.counts[b] += 1
    return
end

@enum NewlineType RETURN NEWLINE CRLF
@enum QuoteType SAME ESCAPE OPENCLOSE
quotetype(oq, cq, eq) = oq == cq == eq ? SAME : oq == cq ? ESCAPE : OPENCLOSE

consumecommentedline(bytes, pos, ::Nothing) = pos
function consumecommentedline(bytes, pos, comment::String)
    if pos < length(bytes) && ccall(:memcmp, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Cint), pointer(bytes, pos + 1), pointer(comment), sizeof(comment)) == 0
        while true
            pos += 1
            @inbounds b = bytes[pos]
            if b === UInt8('\n')
                break
            elseif b !== UInt8('\r')
                if pos < length(bytes) && bytes[pos + 1] === UInt8('\n')
                    pos += 1
                end
                break
            end
        end
    end
    return pos
end

function guessnrows(bytes::Vector{UInt8}, oq, cq, e::UInt8, source, delim, comment)
    len = length(bytes)
    pos = 0
    pos = consumecommentedline(bytes, pos, comment)
    bvc = ByteValueCounter()
    nbytes = 0
    nlines = 0
    newlinetype = NEWLINE
    while pos < len && nlines < 10
        pos += 1
        @inbounds b = bytes[pos]
        nbytes += 1
        if b === oq
            while pos < len
                pos += 1
                @inbounds b = bytes[pos]
                nbytes += 1
                if b === e
                    if pos == len
                        break
                    elseif e === cq && bytes[pos + 1] !== cq
                        break
                    end
                    pos += 1
                    @inbounds b = bytes[pos]
                    nbytes += 1
                elseif b === cq
                    break
                end
            end
        elseif b === UInt8('\n')
            pos = consumecommentedline(bytes, pos, comment)
            nlines += 1
        elseif b === UInt8('\r')
            if pos < len && bytes[pos + 1] === UInt8('\n')
                pos += 1
                newlinetype = CRLF
            else
                newlinetype = RETURN
            end
            pos = consumecommentedline(bytes, pos, comment)
            nlines += 1
        else
            incr!(bvc, b)
        end
    end
    nlines += pos == len

    if delim === nothing
        if isa(source, AbstractString) && endswith(source, ".tsv")
            d = '\t'
        elseif isa(source, AbstractString) && endswith(source, ".wsv")
            d = ' '
        elseif nlines > 1
            for attempted_delim in (',', '\t', ' ', '|', ';', ':')
                if bvc.counts[Int(attempted_delim)] % nlines == 0
                    d = attempted_delim
                    break
                end
            end
        else
            d = ','
        end
    else
        d = delim
    end

    guess = len / (nbytes / nlines) * 1.1
    rowsguess = isfinite(guess) ? ceil(Int, guess) : 0
    return rowsguess, newlinetype, d
end
