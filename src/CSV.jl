module CSV

if !isdefined(Base, :contains)
    contains(haystack, needle) = occursin(needle, haystack)
end

if applicable(Core.Compiler.typesubtract, Union{Int, Missing}, Missing)
    ts(T, S) = Core.Compiler.typesubtract(T, S)
else
    ts(T, S) = Core.Compiler.typesubtract(T, S, 16)
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
using WeakRefStrings
export PosLenString

struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

# constants
const SINGLE_THREADED_POOL_DEFAULT = 0.75
const MULTI_THREADED_POOL_DEFAULT = 0.25

include("utils.jl")
include("detection.jl")
include("context.jl")
include("file.jl")
include("chunks.jl")
include("rows.jl")
include("write.jl")

"""
`CSV.read(source, sink::T; kwargs...)` => T

Read and parses a delimited file, materializing directly using the `sink` function.

`CSV.read` supports all the same keyword arguments as [`CSV.File`](@ref).
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
