module CSV

# stdlib
using Mmap, Dates, Unicode
using Parsers, Tables, PooledArrays, FilePathsBase
using CategoricalArrays, WeakRefStrings, DataFrames

struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

include("utils.jl")
include("detection.jl")
include("header.jl")
include("file.jl")
include("iteration.jl")
include("tables.jl")
include("rows.jl")
include("write.jl")

"""
`CSV.read(source; copycols::Bool=false, kwargs...)` => `DataFrame`

Parses a delimited file into a `DataFrame`. `copycols` determines whether a copy of columns should be made when creating the DataFrame; by default, no copy is made, and the DataFrame is built with immutable, read-only `CSV.Column` vectors. If mutable operations are needed on the DataFrame columns, set `copycols=true`.

`CSV.read` supports the same keyword arguments as [`CSV.File`](@ref).
"""
read(source; copycols::Bool=false, kwargs...) = DataFrame(CSV.File(source; kwargs...), copycols=copycols)

DataFrames.DataFrame(f::CSV.File; copycols::Bool=true) = DataFrame(getcolumns(f), getnames(f); copycols=copycols)

end # module
