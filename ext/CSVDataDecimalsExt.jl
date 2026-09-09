# Exact parsing of explicitly requested DataDecimals column types. CSV never
# infers a decimal type; request one with `types=Dict(:amount => Decimal64{2})`.
module CSVDataDecimalsExt

using CSV, DataDecimals
import Parsers
using CSV: ValueOpts, _trimblanks, _degroup!, _scratchfor

CSV._parseable(::Type{<:DataDecimals.AbstractDecimal}) = true

# Inspect spelling before numeric conversion. Parsing into a decimal may round;
# an explicit schema must never mistake rounded success for exact fit.
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

function CSV.parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                        vo::ValueOpts, scratch::Vector{UInt8}) where {T <: DataDecimals.AbstractDecimal}
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (nothing, false)
        n >= 0 && return _parsedecimal(T, scratch, 1, n, vo)
    end
    return _parsedecimal(T, buf, i, j, vo)
end

CSV.parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
               vo::ValueOpts) where {T <: DataDecimals.AbstractDecimal} =
    CSV.parsevalue(T, buf, i, j, vo, _scratchfor(vo))

end # module
