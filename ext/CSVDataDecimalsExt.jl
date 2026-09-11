# Exact parsing of explicitly requested DataDecimals column types. CSV never
# infers a decimal type; request one with `types=Dict(:amount => Decimal64{2})`.
module CSVDataDecimalsExt

using CSV, DataDecimals
import Parsers
using CSV: ValueOpts, _degroup!, _scratchfor
using DataDecimals: Decimal, RoundExact

# A column schema needs a scalar representation and, for Decimal, a scale.
CSV._parseable(::Type{T}) where {T <: DataDecimals.AbstractDecimal} = isconcretetype(T)

# A requested `Decimal{P, S}` must hold the field's value exactly. Under
# `RoundExact` the parser reports a value that the target scale cannot hold
# exactly, so one parse decides.
@inline function _parsedecimal(::Type{T}, buf, i, j, vo) where {T <: Decimal}
    value = Parsers.tryparse(T, buf, i, j; decimal=Char(vo.decimal), rounding=RoundExact)
    value === nothing && return (zero(T), false)
    return (value, true)
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
        n == -2 && return (zero(T), false)
        n >= 0 && return _parsedecimal(T, scratch, 1, n, vo)
    end
    return _parsedecimal(T, buf, i, j, vo)
end

CSV.parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
               vo::ValueOpts) where {T <: DataDecimals.AbstractDecimal} =
    CSV.parsevalue(T, buf, i, j, vo, _scratchfor(vo))

end # module
