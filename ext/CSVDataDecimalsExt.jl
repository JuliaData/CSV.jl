# Exact parsing of explicitly requested DataDecimals column types. CSV never
# infers a decimal type; request one with `types=Dict(:amount => Decimal64{2})`.
module CSVDataDecimalsExt

using CSV, DataDecimals
import Parsers
using CSV: ValueOpts, _trimblanks, _degroup!, _scratchfor
using DataDecimals: Decimal, DecimalValue, unscaled

# A column schema needs a scalar representation and, for Decimal, a scale.
CSV._parseable(::Type{T}) where {T <: DataDecimals.AbstractDecimal} = isconcretetype(T)

# Spelling of a numeric field, for the wide path: the number of significant
# fractional places decides whether the value fits the target scale exactly.
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

# A requested `Decimal{P, S}` must hold the field's value exactly.
@static if isdefined(DataDecimals, :RoundExact)
    # DataDecimals 1.1 and later: the parser itself reports a value that the
    # target scale cannot hold exactly, so one parse decides.
    @inline function _parsedecimal(::Type{T}, buf, i, j, vo) where {T <: Decimal}
        value = Parsers.tryparse(T, buf, i, j; decimal=Char(vo.decimal),
                                 rounding=DataDecimals.RoundExact)
        value === nothing && return (zero(T), false)
        return (value, true)
    end
else
    # DataDecimals 1.0: the parser rounds, so the field is read once as a
    # `DecimalValue` with the storage type of the target, which keeps the digits
    # and the scale the field spelled. The value then moves to the target scale:
    # fewer places scale up with an overflow check, more places divide and must
    # leave no remainder. A field with more digits than the storage type holds,
    # or with an exponent outside the `DecimalValue` range, takes the wide path:
    # the spelling decides exactness before the target type parses the field.
    @inline function _exactdecimal(::Type{Decimal{P, S, T}}, v::DecimalValue{T}) where {P, S, T}
        u = unscaled(v)
        s = Int(DataDecimals.scale(v))
        if s < S
            u, overflow = DataDecimals._scaleup(u, S - s)
            overflow && return (zero(Decimal{P, S, T}), false)
        elseif s > S
            u, inexact = DataDecimals._scaledown(u, s - S, RoundToZero)
            inexact && return (zero(Decimal{P, S, T}), false)
        end
        limit = unscaled(typemax(Decimal{P, S, T}))
        -limit <= u <= limit || return (zero(Decimal{P, S, T}), false)
        return (reinterpret(Decimal{P, S, T}, u), true)
    end

    @inline function _parsedecimal(::Type{Decimal{P, S, T}}, buf, i, j, vo) where {P, S, T}
        v = Parsers.tryparse(DecimalValue{T}, buf, i, j; decimal=Char(vo.decimal))
        v === nothing || return _exactdecimal(Decimal{P, S, T}, v)
        return _parsedecimalwide(Decimal{P, S, T}, buf, i, j, vo)
    end

    @noinline function _parsedecimalwide(::Type{DT}, buf, i, j, vo) where {DT <: Decimal}
        shape = _decimalshape(buf, i, j, vo.decimal)
        shape === nothing && return (zero(DT), false)
        shape.nonzero && shape.fraction - shape.trailing - shape.exponent > DataDecimals.scale(DT) &&
            return (zero(DT), false)
        value = Parsers.tryparse(DT, buf, i, j; decimal=Char(vo.decimal))
        value === nothing && return (zero(DT), false)
        return (value, true)
    end
end

# A `DecimalValue` keeps the scale the field spelled, so it needs no exactness check.
@inline function _parsedecimal(::Type{T}, buf, i, j, vo) where {T <: DataDecimals.AbstractDecimal}
    value = Parsers.tryparse(T, buf, i, j; decimal=Char(vo.decimal))
    value === nothing && return (zero(T), false)
    return (value, true)
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
