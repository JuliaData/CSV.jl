# stringtype=InlineString (or a fixed String1..String255) for CSV.File/read.
#
# InlineStrings are fixed-width isbits strings (1, 3, 7, 15, 31, 63, 127, 255
# byte payloads). `stringtype=InlineString` picks the smallest width that fits
# each column's longest value (InlineStrings.inlinestrings semantics — 0.10's
# default behavior); a specific `String15` etc. pins the width, erroring on
# an over-long value like `String15("...")` would.
#
# Conversion runs from the CompactString payloads directly: inline values
# rebuild from the payload words, views copy out of the retained buffer — one
# pass, no intermediate Vector{String}.
module CSVInlineStringsExt

using CSV, InlineStrings
const A = CSV.CSVApi
const K = CSV.CSVKernel

const _WIDTHS = (String1, String3, String7, String15, String31, String63, String127, String255)

# validation hook
A._stringsink(::Type{InlineString}) = true
A._stringsink(::Type{T}) where {T <: InlineString} = true

# smallest InlineString type holding `n` bytes
function _fitwidth(n::Int)
    for T in _WIDTHS
        n <= sizeof(T) - 1 && return T
    end
    throw(ArgumentError("value of $n bytes exceeds the InlineString maximum of 255"))
end

@inline function _inl(::Type{T}, s::K.CompactString) where {T <: InlineString}
    n = ncodeunits(s)
    n > sizeof(T) - 1 &&
        throw(ArgumentError("value of $n bytes does not fit $T"))
    if n > K.COMPACTSTRING_INLINE
        off = K.csoff(s.p)
        o = off < 0 ? -off : off
        GC.@preserve s begin
            return T(pointer(s.data, o), n)
        end
    end
    # inline payload: build through a stack scratch (≤12 bytes)
    buf = Ref{NTuple{16, UInt8}}()
    p = Ptr{UInt8}(Base.unsafe_convert(Ptr{NTuple{16, UInt8}}, buf))
    GC.@preserve buf begin
        @inbounds for i in 1:n
            unsafe_store!(p, codeunit(s, i), i)
        end
        return T(p, n)
    end
end

function _widthfor(col::K.CompactStringVector)
    m = 0
    @inbounds for i in eachindex(col)
        x = col[i]
        x === missing && continue
        m = max(m, ncodeunits(x))
    end
    return _fitwidth(m)
end

function A._materializecolumn(::Type{InlineString}, col::K.CompactStringVector)
    return A._materializecolumn(_widthfor(col), col)
end
function A._materializecolumn(::Type{T}, col::K.CompactStringVector) where {T <: InlineString}
    n = length(col)
    if Missing <: eltype(col)
        out = Vector{Union{T, Missing}}(undef, n)
        anymissing = false
        @inbounds for i in 1:n
            x = col[i]
            if x === missing
                out[i] = missing
                anymissing = true
            else
                out[i] = _inl(T, x)
            end
        end
        return anymissing ? out : convert(Vector{T}, out)
    end
    out = Vector{T}(undef, n)
    @inbounds for i in 1:n
        out[i] = _inl(T, col[i])
    end
    return out
end

# Rows(stringtype=InlineString): per-cell, smallest fitting width
A._rowstring(::Type{InlineString}, x::K.CompactString) = _inl(_fitwidth(ncodeunits(x)), x)
A._rowstring(::Type{T}, x::K.CompactString) where {T <: InlineString} = _inl(T, x)

A._levelvector(::Type{InlineString}, levels::K.CompactStringVector, n::Int) =
    A._levelvector(_widthfor(levels), levels, n)
A._levelvector(::Type{T}, levels::K.CompactStringVector, n::Int) where {T <: InlineString} =
    T[_inl(T, levels[i]) for i in 1:n]

end # module
