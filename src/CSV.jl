module CSV

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

using CategoricalArrays, DataFrames

struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

include("utils.jl")
include("detection.jl")
include("header.jl")
include("file.jl")
include("chunks.jl")
include("rows.jl")
include("write.jl")

"""
`CSV.read(source; copycols::Bool=false, kwargs...)` => `DataFrame`

Parses a delimited file into a `DataFrame`. `copycols` determines whether a copy of columns should be made when creating the DataFrame; by default, no copy is made.

`CSV.read` supports the same keyword arguments as [`CSV.File`](@ref).
"""
function read(source; copycols::Bool=false, kwargs...)
    @warn "`CSV.read(input; kw...)` is deprecated in favor of `DataFrame!(CSV.File(input; kw...))`"
    DataFrame(CSV.File(source; kwargs...), copycols=copycols)
end

DataFrames.DataFrame(f::CSV.File; copycols::Bool=true) = DataFrame(getcolumns(f), getnames(f); copycols=copycols)

include("precompile.jl")
_precompile_()

end # module
