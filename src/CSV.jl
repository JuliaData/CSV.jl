module CSV

if !isdefined(Base, :contains)
    contains(haystack, needle) = occursin(needle, haystack)
end

# stdlib
using Mmap, Dates, Unicode
# Parsers.jl is used for core type parsing from byte buffers
# and all other parsing options (quoted fields, delimiters, dateformats etc.)
using Parsers
# Tables.jl allows integration with all other table/data file formats
using Tables
# PooledArrays.jl is used for materializing pooled columns
using PooledArrays
# SentinelArrays.jl allow efficient conversion from Vector{Union{T, Missing}} to Vector{T}
# it also provides the MissingVector and ChainedVector array types
using SentinelArrays
# WeakRefStrings provides the InlineString and PosLenString types for more gc-friendly string types
using WeakRefStrings
export PosLenString, InlineString
# CodecZlib is used for unzipping gzip files
using CodecZlib
# FilePathsBase allows more structured file path types
using FilePathsBase

struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

# constants
const DEFAULT_STRINGTYPE = InlineString
const DEFAULT_POOL = 0.25
const DEFAULT_ROWS_TO_CHECK = 30
const DEFAULT_MAX_WARNINGS = 100
const DEFAULT_MAX_INLINE_STRING_LENGTH = 32
const TRUE_STRINGS = ["true", "True", "TRUE", "T", "1"]
const FALSE_STRINGS = ["false", "False", "FALSE", "F", "0"]

include("keyworddocs.jl")
include("utils.jl")
include("detection.jl")
include("context.jl")
include("file.jl")
include("chunks.jl")
include("rows.jl")
include("write.jl")

"""
`CSV.read(source, sink::T; kwargs...)` => T

Read and parses a delimited file or files, materializing directly using the `sink` function. Allows avoiding excessive copies
of columns for certain sinks like `DataFrame`.

$KEYWORD_DOCS
"""
function read(source, sink=nothing; copycols::Bool=false, kwargs...)
    if sink === nothing
        throw(ArgumentError("provide a valid sink argument, like `using DataFrames; CSV.read(source, DataFrame)`"))
    end
    Tables.CopiedColumns(CSV.File(source; kwargs...)) |> sink
end

include("precompile.jl")
_precompile_()

end # module
