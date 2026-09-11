# stringtype=InlineString (or a fixed String1..String255) for CSV.File/read.
#
# InlineStrings are fixed-width isbits strings (1, 3, 7, 15, 31, 63, 127, 255
# byte payloads). `stringtype=InlineString` picks the smallest width that fits
# each column's longest value (InlineStrings.inlinestrings semantics); a
# specific `String15` etc. fixes the width, erroring on an over-long value like
# `String15("...")` would.
#
# Conversion runs from the DataString payloads directly: inline values
# rebuild from the payload words, views copy out of the retained buffer — one
# pass, no intermediate Vector{String}.
module CSVInlineStringsExt

using CSV, InlineStrings

const _WIDTHS = (String1, String3, String7, String15, String31, String63, String127, String255)

_capacity(::Type{T}) where {T <: InlineString} = sizeof(T) - 1

# validation hook
CSV._stringsink(::Type{InlineString}) = true
CSV._stringsink(::Type{T}) where {T <: InlineString} = true

# smallest InlineString type holding `n` bytes
function _fitwidth(n::Int)
    for T in _WIDTHS
        n <= _capacity(T) && return T
    end
    throw(ArgumentError("value of $n bytes exceeds the InlineString maximum of 255"))
end

@inline function _inl(::Type{T}, s::CSV.DataString) where {T <: InlineString}
    n = ncodeunits(s)
    n > _capacity(T) &&
        throw(ArgumentError("value of $n bytes does not fit $T"))
    if n > CSV.INLINE_MAX
        GC.@preserve s begin
            return T(pointer(s.data, CSV.payloadpos(s.p)), n)
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

# `stringtype=InlineString` stops at String31: wider inline strings copy
# 64–256 bytes per cell and lose to `String` on every operation.
# A column whose longest value exceeds it comes back as `String`, so a valid
# file never fails to read because of its text width.
const _AUTO_MAX_WIDTH = _capacity(String31)

# smallest auto width for a column, or `nothing` when the text is too wide
function _widthfor(col::CSV.DataStringVector)
    m = 0
    @inbounds for i in eachindex(col)
        x = col[i]
        x === missing && continue
        m = max(m, ncodeunits(x))
    end
    return m <= _AUTO_MAX_WIDTH ? _fitwidth(m) : nothing
end

# Chunks settles the auto width once for its whole row window from the
# longest value the schema pass saw, so every batch has one element type.
CSV._settledstringtype(::Type{InlineString}, maxlen::Int) =
    maxlen <= _AUTO_MAX_WIDTH ? _fitwidth(maxlen) : String

function CSV._materializecolumn(::Type{InlineString}, col::CSV.DataStringVector,
                                parallel::Bool=true)
    W = _widthfor(col)
    return W === nothing ? CSV._materializecolumn(String, col, parallel) :
                           CSV._materializecolumn(W, col, parallel)
end

function CSV._materializecolumn(::Type{T}, col::CSV.DataStringVector,
                                parallel::Bool=true) where {T <: InlineString}
    n = length(col)
    if Missing <: eltype(col)
        out = Vector{Union{T, Missing}}(undef, n)
        CSV._rowranges(n, parallel) do lo, hi
            @inbounds for i in lo:hi
                x = col[i]
                out[i] = x === missing ? missing : _inl(T, x)
            end
        end
        # The parser already settles missingness. Preserve a declared Union
        # even when this particular column or batch contains no missing cells.
        return out
    end
    out = Vector{T}(undef, n)
    CSV._rowranges(n, parallel) do lo, hi
        @inbounds for i in lo:hi
            out[i] = _inl(T, col[i])
        end
    end
    return out
end

# Auto width can return either an InlineString or an owned String. Tables
# consumers must allocate a column that can hold both, including >255 bytes.
CSV._rowstringtype(::Type{InlineString}) = Union{InlineString, String}
CSV._lazyeltype(::Type{InlineString}) = Union{Missing, InlineString, String}

# Rows(stringtype=InlineString): per-cell, smallest fitting width (String past
# the auto ceiling)
function CSV._rowstring(::Type{InlineString}, x::CSV.DataString)
    n = ncodeunits(x)
    return n <= _AUTO_MAX_WIDTH ? _inl(_fitwidth(n), x) : String(x)
end

CSV._rowstring(::Type{T}, x::CSV.DataString) where {T <: InlineString} = _inl(T, x)

function CSV._levelvector(::Type{InlineString}, levels::CSV.DataStringVector, n::Int)
    W = _widthfor(levels)
    return W === nothing ? CSV._levelvector(String, levels, n) : CSV._levelvector(W, levels, n)
end

CSV._levelvector(::Type{T}, levels::CSV.DataStringVector, n::Int) where {T <: InlineString} =
    T[_inl(T, levels[i]) for i in 1:n]

end # module
