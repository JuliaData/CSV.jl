# CSV builds payloads after validating field spans and quote structure.
# DataStrings owns scalar semantics, column access, and supported column edits.
import DataStrings
using DataStrings: DataString, StringVector, StringPayload, inline_payload,
                   view_payload, rebase_payload
const DataStringPayload = StringPayload
const DataStringVector = StringVector
const PAYLOAD_MISSING = DataStrings.PAYLOAD_MISSING
const COMPACTSTRING_INLINE = DataStrings.INLINE_MAX
const COMPACTSTRING_BUFFER_BYTES = Int(typemax(Int32))
const EMPTY_BYTES = UInt8[]
const cslen = DataStrings.payloadlength
const csbufidx = DataStrings.payloadbufidx
const csoffset = DataStrings.payloadoffset
const cspos = DataStrings.payloadpos
@inline _viewword(bufidx::Integer, offset0::Integer) =
    UInt64(bufidx % UInt32) | (UInt64(offset0 % UInt32) << 32)

function _stringvector(::Type{T}, payloads, buf, extra, overflow) where {T}
    buffers = Vector{UInt8}[buf, extra]
    append!(buffers, overflow)
    return StringVector{T}(payloads, buffers, Val(:trusted))
end

# Re-point an owned-buffer view while preserving its length and four-byte
# prefix. Stitching uses this when a chunk-local buffer is copied into a final
# column-owned buffer, which may have a different Arrow buffer index.
@inline function repoint_payload(p::DataStringPayload, bufidx::Integer,
                                 offset0::Integer)
    (0 <= offset0 <= typemax(Int32) && 0 <= bufidx <= typemax(Int32)) ||
        throw(ArgumentError("DataString view (buffer $bufidx, offset $offset0) " *
                            "does not fit Arrow's Int32 view words"))
    return DataStringPayload(p.a, _viewword(bufidx, offset0))
end


materialize(v::StringVector) = DataStrings.materialize(v)
