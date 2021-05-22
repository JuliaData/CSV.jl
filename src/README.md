This README serves as developer documentation for CSV.jl internals. It doesn't pretend to be comprehensive, but was created with the aim to explain both the overall strategies CSV.jl uses in parsing delimited files as well as the trickiest mechanics and some of the reasoning that went into the architecture.

# CSV.File

Let's go over details specific to `CSV.File`.

## The `pool` keyword argument

CSV.jl provides a native integration with the [PooledArrays.jl](https://github.com/JuliaData/PooledArrays.jl/) package, which provides an array storage optimization by having a (hopefully) small pool of "exspensive" (big or heap-allocated, or whatever) values, along with a memory-efficient integer array of "refs" where each ref maps to one of the values in the pool. This is sometimes referred to as a "dictionary encoding" in various data formats. As an example, if you have a column with 1,000,000 elements, but only 10 unique string values, you can have a `Vector{String}` pool to store the 10 unique strings and give each a unique `UInt32` value, and a `Vector{UInt32}` "ref array" for the million elements, where each element just indexes into the pool to get the actual string value.

By providing the `pool` keyword argument, users can control how this optimization will be applied to individual columns, or to all columns of the delimted text being read.

Valid inputs for `pool` include:
  * A `Bool`, `true` or `false`, which will apply to _all_ columns parsed; they either will _all_ be pooled, or _all_ not pooled
  * A `Real`, which will be converted to `Float64`, which should be a value between `0.0` and `1.0`, to indicate the % cardinality threshold _under which_ a column will be pooled. e.g. by passing `pool=0.1`, if a column has less than 10% unique values, it will end up as a `PooledArray`, otherwise a normal array. Like the `Bool` argument, this will apply the same % threshold to _all_ columns
  * An `AbstractVector`, where the # of elements should/needs to match the # of columns in the dataset. Each element of the `pool` argument should be a `Bool` or `Real` indicating the pooling behavior for each specific column.
  * An `AbstractDict`, with keys as `String`s, `Symbol`s, or `Int`s referring to column names or indices, and values in the `AbstractDict` being `Bool` or `Real` to again signal how specific columns should be pooled

