import DataDecimals

# Inspect spelling before numeric conversion. Parsing into a decimal may round;
# inference and explicit schemas must never mistake rounded success for exact fit.
function _decimalshape(buf, i::Int, j::Int, dec::UInt8)
    i, j = _trimblanks(buf, i, j)
    i > j && return nothing
    neg = buf[i] == UInt8('-')
    (neg || buf[i] == UInt8('+')) && (i += 1)
    digits = 0
    intdigits = 0
    fraction = 0
    trailing = 0
    nonzero = false
    point = false
    @inbounds while i <= j
        b = buf[i]
        if UInt8('0') <= b <= UInt8('9')
            digits += 1
            nonzero |= b != UInt8('0')
            trailing = b == UInt8('0') ? trailing + 1 : 0
            if point
                fraction += 1
            elseif nonzero
                intdigits += 1
            end
        elseif b == dec && !point
            point = true
        else
            break
        end
        i += 1
    end
    digits == 0 && return nothing
    exponent = 0
    scientific = i <= j
    if scientific
        buf[i] in (UInt8('e'), UInt8('E')) || return nothing
        i += 1
        eneg = i <= j && buf[i] == UInt8('-')
        i <= j && (eneg || buf[i] == UInt8('+')) && (i += 1)
        i > j && return nothing
        @inbounds while i <= j
            b = buf[i]
            UInt8('0') <= b <= UInt8('9') || return nothing
            exponent = min(1_000_000, 10 * exponent + Int(b - UInt8('0')))
            i += 1
        end
        eneg && (exponent = -exponent)
    end
    return (; intdigits, fraction, trailing, nonzero, exponent, scientific,
            negativezero=neg && !nonzero)
end

@inline function _parsedecimal(::Type{T}, buf, i, j, vo) where {T <: DataDecimals.AbstractDecimal}
    shape = _decimalshape(buf, i, j, vo.decimal)
    shape === nothing && return (nothing, false)
    if T <: DataDecimals.Decimal
        shape.nonzero && shape.fraction - shape.trailing - shape.exponent > DataDecimals.scale(T) &&
            return (nothing, false)
    end
    value = Parsers.tryparse(T, buf, i, j; decimal=Char(vo.decimal))
    return (value, value isa T)
end

function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                    vo::ValueOpts, scratch::Vector{UInt8}) where {T <: DataDecimals.AbstractDecimal}
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (nothing, false)
        n >= 0 && return _parsedecimal(T, scratch, 1, n, vo)
    end
    return _parsedecimal(T, buf, i, j, vo)
end

parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
           vo::ValueOpts) where {T <: DataDecimals.AbstractDecimal} =
    parsevalue(T, buf, i, j, vo, _scratchfor(vo))

# Full-column opt-in profile. Integers and missing values are neutral. Require
# two fractional values with identical written scales. A header name never
# changes numeric semantics. Scientific notation and negative zero remain floats.
function _decimalcandidate(buf, spans, vo)
    scale = -1
    integerdigits = 0
    fractionalvalues = 0
    scratch = _scratchfor(vo)
    for span in spans
        span === nothing && continue
        pos, len = span
        i, n, escaped, status = cellcontent(buf, pos, len, vo)
        status == CELL_MISSING && continue
        (status != CELL_VALUE || escaped || n == 0) && return nothing
        j = i + n - 1
        bytes = buf
        if vo.groupmark != 0x00
            count = _degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
            count == -2 && return nothing
            if count >= 0
                bytes, i, j = scratch, 1, count
            end
        end
        shape = _decimalshape(bytes, i, j, vo.decimal)
        shape === nothing && return nothing
        (shape.scientific || shape.negativezero) && return nothing
        if shape.fraction > 0
            scale >= 0 && scale != shape.fraction && return nothing
            scale = shape.fraction
            fractionalvalues += 1
        end
        integerdigits = max(integerdigits, shape.intdigits)
        integerdigits + max(scale, 0) > 76 && return nothing
    end
    fractionalvalues >= 2 || return nothing
    precision = max(1, integerdigits + scale)
    precision <= 18 && return DataDecimals.Decimal64{scale}
    precision <= 38 && return DataDecimals.Decimal128{scale}
    return DataDecimals.Decimal256{scale}
end

# Keep iteration state to three integers. Nested flatten/filter generators keep
# chunk closures in their state and allocate on every row on supported Julia.
struct DecimalSpans{M}
    chunks::Vector{ChunkIndex}
    column::Int
    limit::Int
    rowmask::M
end

@inline function Base.iterate(spans::DecimalSpans, state=(1, 0, 0))
    chunk, localrow, row = state
    while chunk <= length(spans.chunks)
        ci = spans.chunks[chunk]
        localrow = max(localrow, ci.firstdatarow)
        if localrow > totalrows(ci)
            chunk += 1
            localrow = 0
            continue
        end
        row += 1
        row > spans.limit && return nothing
        nextrow = localrow + 1
        if spans.rowmask === nothing || spans.rowmask[row]
            return (fieldspan(ci, localrow, spans.column), (chunk, nextrow, row))
        end
        localrow = nextrow
    end
    return nothing
end

function _inferdecimaltypes!(seed, buf, chunks, plan; limit=nothing, rowmask=nothing)
    for j in plan.sources
        seed[j] === nothing || continue
        spans = DecimalSpans(chunks, j, something(limit, typemax(Int)), rowmask)
        seed[j] = _decimalcandidate(buf, spans, columnopts(plan, j))
    end
    return seed
end
