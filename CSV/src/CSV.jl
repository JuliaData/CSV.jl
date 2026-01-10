"""
CSV provides fast, flexible reader & writer for delimited text files in various formats.

Reading:
- `CSV.File` reads delimited data and returns a `CSV.File` object, which allows dot-access to columns and iterating rows.
- `CSV.read` is similar to `CSV.File` but used when the input will be passed directly to a sink function such as a `DataFrame`.
- `CSV.Rows` reads delimited data and returns a `CSV.Rows` object, which allows "streaming" the data by iterating and thereby has a lower memory footprint than `CSV.File`.
- `CSV.Chunks` allows processing extremely large files in "batches" or "chunks".
Writing:
- `CSV.write` writes a [Tables.jl interface input](https://github.com/JuliaData/Tables.jl) such as a `DataFrame` to a csv file or an in-memory IOBuffer.
- `CSV.RowWriter` creates an iterator that produces csv-formatted strings for each row in the input table.
Here is an example of reading a csv file and passing the input to a `DataFrame`:
```julia
using CSV, DataFrames
ExampleInputDF = CSV.read("ExampleInputFile.csv", DataFrame)
```
Here is an example of writing out a `DataFrame` to a csv file:
```julia
using CSV, DataFrames
ExampleOutputDF = DataFrame(rand(10,10), :auto)
CSV.write("ExampleOutputFile.csv", ExampleOutputDF)
```
"""
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
# InlineStrings and WeakRefStrings provide the InlineString and PosLenString types for more gc-friendly string types
using InlineStrings, WeakRefStrings
export PosLenString, InlineString
export String1, String3, String7, String15, String31, String63, String127, String255
# CodecZlib is used for unzipping gzip files
using CodecZlib
# FilePathsBase allows more structured file path types
using FilePathsBase
# WorkerUtilities for lock/spawn utilities
using WorkerUtilities

struct Error <: Exception
    msg::String
end

Base.showerror(io::IO, e::Error) = println(io, e.msg)

# constants
const DEFAULT_STRINGTYPE = InlineString
const DEFAULT_POOL = (0.2, 500)
const DEFAULT_ROWS_TO_CHECK = 30
const DEFAULT_MAX_WARNINGS = 100
const DEFAULT_MAX_INLINE_STRING_LENGTH = 32
const TRUE_STRINGS = ["true", "True", "TRUE", "T", "1"]
const FALSE_STRINGS = ["false", "False", "FALSE", "F", "0"]
const StringCodeUnits = Base.CodeUnits{UInt8, String}
const ValidSources = Union{Vector{UInt8}, SubArray{UInt8, 1, Vector{UInt8}}, StringCodeUnits, IO, Cmd, AbstractString, AbstractPath}
const MAX_INPUT_SIZE = Int === Int64 ? 2^42 : typemax(Int32)
const EMPTY_INT_ARRAY = Int[]

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

# Example
```
julia> using CSV, DataFrames

julia> path = tempname();

julia> write(path, "a,b,c\\n1,2,3");

julia> CSV.read(path, DataFrame)
1×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> CSV.read(path, DataFrame; header=false)
2×3 DataFrame
 Row │ Column1  Column2  Column3
     │ String1  String1  String1
─────┼───────────────────────────
   1 │ a        b        c
   2 │ 1        2        3
```

$KEYWORD_DOCS
"""
function read(source, sink=nothing; copycols::Bool=false, kwargs...)
    if sink === nothing
        throw(ArgumentError("provide a valid sink argument, like `using DataFrames; CSV.read(source, DataFrame)`"))
    end
    Tables.CopiedColumns(CSV.File(source; kwargs...)) |> sink
end

include("precompile.jl")

function __init__()
end

end # module
