This README serves as developer documentation for CSV.jl internals. It doesn't pretend to be comprehensive, but was created with the aim to explain both the overall strategies CSV.jl uses in parsing delimited files as well as the trickiest mechanics and some of the reasoning that went into the architecture.

# CSV.File

Let's go over details specific to `CSV.File`.

## General parsing strategy/mechanics

In the context.jl file, we take care of processing/validating all the various options/keyword arguments from the first `CSV.File`/`CSV.read` call. The end result in a `CSV.Context` object, which represents a parsing "context" of a delimited file. It holds a reference to the input buffer as a `Vector{UInt8}`, detected or generated column names, byte position where parsing should start, whether parsing should use multiple threads, etc. One of the key fields of `CSV.Context` is `ctx.columns`, which is a `Vector{CSV.Column}`. A `CSV.Column` holds a bunch of information related to parsing a single column in a file, including it's currently detected/user-specified type, whether any `missing` values have been encountered, if the column will be dropped after parsing, whether it should be pooled, etc.

So the general strategy is to get the overall `CSV.Context` for a delimited file, then pass then choose one of two main paths for actual parsing based on whether parsing will use multiple threads or not. For non-threaded, we go directly into parsing the file, row by row, until finished. For multithreaded parsing, the `CSV.Context` will have determined the starting byte positions for each chunk and also sampled column types, so we spawn a threaded task per file "chunk", then need to synchronize the chunks after each has finished. This syncing is to ensure the parsed/detected types match and that pooled columns have matching refs. The final step for single or multithreaded parsing is to make the final columns: if no missing values were encountered, then we'll "unwrap" the SentinelArray to get a normal Vector for most standard types; for pooled columns, we'll create the actual PooledArray; for `stringtype=PosLenString`, we make a `PosLenStringVector`. For multithreaded parsing, we do the same steps, but also link the different threaded chunks into single long columns via `ChainedVector`.

## Non-standard types



## The `pool` keyword argument

CSV.jl provides a native integration with the [PooledArrays.jl](https://github.com/JuliaData/PooledArrays.jl/) package, which provides an array storage optimization by having a (hopefully) small pool of "expensive" (big or heap-allocated, or whatever) values, along with a memory-efficient integer array of "refs" where each ref maps to one of the values in the pool. This is sometimes referred to as a "dictionary encoding" in various data formats. As an example, if you have a column with 1,000,000 elements, but only 10 unique string values, you can have a `Vector{String}` pool to store the 10 unique strings and give each a unique `UInt32` value, and a `Vector{UInt32}` "ref array" for the million elements, where each element just indexes into the pool to get the actual string value.

By providing the `pool` keyword argument, users can control how this optimization will be applied to individual columns, or to all columns of the delimited text being read.

Valid inputs for `pool` include:
  * A `Bool`, `true` or `false`, which will apply to all string columns parsed; string columns either will _all_ be pooled, or _all_ not pooled
  * A `Real`, which will be converted to `Float64`, which should be a value between `0.0` and `1.0`, to indicate the % cardinality threshold _under which_ a column will be pooled. e.g. by passing `pool=0.1`, if a column has less than 10% unique values, it will end up as a `PooledArray`, otherwise a normal array. Like the `Bool` argument, this will apply the same % threshold to only/all string columns.
  * a `Tuple{Float64, Int}`, where the 1st argument is the same as the above percent threshold on cardinality, while the 2nd argument is an absolute upper limit on the # of unique values. This is useful for large datasets where 0.2 may grow to allow pooled columns with thousands of values; it's helpful performance-wise to put an upper limit like `pool=(0.2, 500)` to ensure no pooled column will have more than 500 unique values.
  * An `AbstractVector`, where the # of elements should/needs to match the # of columns in the dataset. Each element of the `pool` argument should be a `Bool`, `Real`, or `Tuple{Float64, Int}` indicating the pooling behavior for each specific column.
  * An `AbstractDict`, with keys as `String`s, `Symbol`s, or `Int`s referring to column names or indices, and values in the `AbstractDict` being `Bool`, `Real`, or `Tuple{Float64, Int}` to again signal how specific columns should be pooled
  * A function of the form `(i, nm) -> Union{Bool, Real, Tuple{Float64, Int}}` where it takes the column index and name as two arguments, and returns one of the first 3 possible pool values from the above list.

For the implementation of pooling:
  * We normalize however the keyword argument was provided to have a `pool` value per column while parsing
  * We also have a `pool` field on the `Context` structure in case columns are widened while parsing, they will take on this value
  * Once column parsing is done, the cardinality is checked against the individual column pool value and whether the column should be pooled or not is computed.