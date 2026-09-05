"""
    CSV

Fast, flexible reading and writing of delimited text.

Reading — `CSV.File`, `CSV.read`, `CSV.Rows`, `CSV.Chunks`.
Writing — `CSV.write`.
Diagnostics — `CSV.problems`.

All public names live under the `CSV` namespace. Reading supports eager,
lazy, row-wise, and chunked workflows. Parsing and writing are deterministic
for any supported thread count.
"""
module CSV

using Tables

# These files form one `CSV` module. The split keeps each implementation area
# small enough to read without adding private module boundaries.
include("core.jl")       # indexing, values, parsing, and columns
include("decimals.jl")   # exact decimal parsing and optional scale inference
include("tables.jl")     # Tables.jl support and row access
include("api.jl")        # File, read, Rows, Chunks, and option handling
include("write.jl")      # write and RowWriter
if isdefined(Tables, :Scan)
    include("scan.jl")   # optional Tables.Scan support
end

# Delimiter and header detection (`sniff` and `Spec`) is internal machinery
# behind `delim=nothing`; it is not part of the 1.0 public surface (0.10 had
# no such API). It can be promoted later if there is demand.

# These are namespace APIs, not exports. Their public docs stay here so the
# complete supported surface is easy to review.
@doc """
    CSV.File(source; keywords...) -> CSV.File

Read delimited data into an eager Tables.jl table. `source` can be a path or
HTTP(S) URL, an `IO`, a `Cmd`, bytes, or a vector of sources. CSV.jl detects
the delimiter and column types by default. Text uses `DataStrings.DataString`,
pooling is off, and recoverable parse problems are available through
[`CSV.problems`](@ref CSV.problems). Use `on_error=:error` for fail-fast
parsing. Reader keywords control the header and row window, dialect, missing
values, types, selected columns, strings, pooling, validation, and task count.
`ntasks=N` bounds parsing to at most `N` worker tasks.
Transpose mode is sequential. It accepts and validates `ntasks` and `parallel`
for compatibility.
""" File
@doc """
    CSV.lazy(source; keywords...) -> CSV.LazyFile

Build the quote-aware structural index and return a table whose cells parse
when accessed. Column, cell, and Tables.jl column access are supported.
[`CSV.File`](@ref CSV.File)`(lazyfile)` performs a full typed parse without
repeating the structural scan. This API retains the source bytes and index; it
does not stream an unbounded input. List selection uses stable file order and
removes duplicates. A later `CSV.File(lazyfile)` retains those visible columns
and can only project them further. Ordinary text cells are zero-copy views. A
long cell whose absolute source offset cannot fit the compact view word copies
only that cell into a bounded backing buffer.
""" lazy
@doc """
    CSV.LazyFile

The indexed table returned by [`CSV.lazy`](@ref CSV.lazy). Values materialize
on access. Convert it with [`CSV.File`](@ref CSV.File) to reuse its index for an
eager typed parse.
""" LazyFile
@doc """
    CSV.Rows(source; types=nothing, stringtype=DataStrings.DataString, keywords...)

Iterate lightweight Tables.jl row views without allocating eager columns.
Cells materialize on access. The source bytes and complete structural index
remain in memory. `reusebuffer` is accepted for 0.10 compatibility but is
inert because the row view has no per-row value buffer. Invalid or malformed
cells become `missing` by default; `strict=true` or `on_error=:error` throws
when the cell is accessed. Rows do not retain parse diagnostics, so use
[`CSV.File`](@ref CSV.File) when `CSV.problems` or a diagnostic cap is needed.
List `select` and `drop` forms project columns in stable file order.
""" Rows
@doc """
    CSV.Chunks(source; ntasks=Threads.nthreads(), keywords...)

Iterate a source as stable-schema [`CSV.File`](@ref CSV.File) batches and
provide the Tables.jl partitions interface. Pooling is evaluated per batch.
`ntasks` influences the target batch size; use `chunkbytes` for direct size
control. List `select` and `drop` forms project every batch in stable file
order.
""" Chunks
@doc """
    CSV.read(source, sink; keywords...)

Parse with the same options as [`CSV.File`](@ref CSV.File), then call the
Tables.jl `sink`. The new columns are passed as `Tables.CopiedColumns`, so a
sink that honors that marker can take ownership without another copy.
""" read
@doc """
    CSV.problems(file)

Return the retained parse problems for a `CSV.File`. The parser can retain at
most `maxproblems` entries; the file display
reports any additional dropped count. Use `strict=true` or `on_error=:error`
to stop at the first parse problem.
""" problems
@doc """
    CSV.write(sink, table; keywords...)

Write any Tables.jl table as delimited text to a path or `IO`. The writer
supports header control, append mode, gzip, partitioned sinks, quote styles,
number and date formats, cell transforms, and deterministic ordered output.
Column-access tables can render row blocks in parallel. Row-access sources
stream sequentially without being collected. A partitioned string base path
returns the generated path vector; other forms return the supplied sink.
""" write
@doc """
    CSV.RowWriter(table; keywords...)

Iterate complete CSV-formatted row strings. The header is first unless it is
disabled. Rows render on demand with the same dialect and value formatting as
[`CSV.write`](@ref CSV.write). With the same formatting options, joining the
iterator gives the same uncompressed bytes as `CSV.write`.
""" RowWriter
# Julia 1.11 added `public`. Build the expression at runtime so Julia 1.10 can
# still parse this file. The surface remains deliberately unexported: users
# call it through the `CSV` namespace.
@static if VERSION >= v"1.11"
    Core.eval(@__MODULE__, Expr(:public, :File, :lazy, :LazyFile, :Rows,
                                :Chunks, :read, :problems, :write,
                                :RowWriter))
end

# -- precompile workload -------------------------------------------------------
# The specialized per-column loops are exactly what makes first-File
# expensive to compile (~4 s cold on an M3). One small in-memory pass through
# each public reader and writer caches those specializations: File (type
# inference, type changes, pooling, missing values, each built-in value type,
# gzip, parallel parsing,
# stringtype=String materializer), Rows, Chunks, the sniffer, write, RowWriter.
using PrecompileTools: @setup_workload, @compile_workload
import Dates, CodecZlib
@setup_workload begin
    mixed = "int,float,date,datetime,bool,null,str,catg,int_float\n" *
            "1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n" *
            "2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"
    pooled = "s,t\na1,x1\na2,\na0,x3\na1,x4\na2,x5\n"
    @compile_workload begin
        f = File(IOBuffer(mixed))
        Tables.columntable(f)
        problems(f)
        File(IOBuffer(pooled); pool=(0.5, 100))
        File(IOBuffer(mixed); stringtype=String)
        File(IOBuffer(mixed); parallel=true, ntasks=2, chunkbytes=1 << 10)
        # `stop_on_end=true` did not preserve caller-owned IO on every
        # TranscodingStreams version admitted by CodecZlib 0.7. Use the
        # one-shot codec here so the lower-bound precompile workload is stable.
        compressed = transcode(CodecZlib.GzipCompressor,
                               Vector{UInt8}(codeunits(mixed)))
        File(compressed)
        foreach(identity, Rows(IOBuffer("a,b\n1,x\n2,y\n")))
        first(Chunks(IOBuffer(pooled); chunkbytes=1 << 16))
        sniff(IOBuffer(mixed))
        out = IOBuffer()
        write(out, (a=[1, 2], b=["x", "y,z"], c=[1.5, missing],
                    d=[Dates.Date(2024, 1, 2), Dates.Date(2024, 3, 4)]))
        join(RowWriter((a=[1], b=["x"])))
    end
end

end # module CSV
