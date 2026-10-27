"""
    CSV

Fast, flexible reading and writing of delimited text.

Reading — `CSV.File`, `CSV.read`, `CSV.lazy`, `CSV.Rows`, `CSV.Chunks`.
Writing — `CSV.write`, `CSV.RowWriter`.
Diagnostics — `CSV.problems`, `CSV.Problem`, `CSV.ParseError`.

All public names live under the `CSV` namespace. Results do not depend on the
number of threads.
"""
module CSV

using Tables

# These files form one `CSV` module. The split keeps each implementation area
# small enough to read without adding private module boundaries.
include("core.jl")       # indexing, values, parsing, and columns
include("tables.jl")     # Tables.jl support and row access
include("api.jl")        # File, read, Rows, Chunks, and option handling
include("write.jl")      # write and RowWriter
include("scan.jl")       # Tables.Scan support

# These are namespace APIs, not exports. Their public docs stay here so the
# complete supported surface is in one place.
@doc """
    CSV.File(source; keywords...) -> CSV.File

Read delimited data into an eager Tables.jl table. `source` can be a path or
HTTP(S) URL, an `IO`, a `Cmd`, bytes, or a vector of sources. CSV.jl detects
the delimiter and column types by default. Text uses `DataStrings.DataString`,
pooling is off, and recoverable parse problems are available through
[`CSV.problems`](@ref CSV.problems). The default `on_error=:warn` prints one
summary warning per read; `on_error=:collect` records problems silently;
`on_error=:error` (or `strict=true`) throws a
[`CSV.ParseError`](@ref CSV.ParseError) at the first problem.

# Keywords

Header and rows:
- `header=1`: the header row number, `false` for `Column1`, `Column2`, ...
  names, a vector of names, or a vector of row numbers whose cells are joined.
- `normalizenames=false`: turn names into valid Julia identifiers.
- `skipto`: the first data row. `footerskip=0`: rows to omit at the end.
  `limit`: the maximum number of data rows.

Dialect:
- `delim=nothing`: a `Char` or `String` delimiter. `nothing` detects one of
  comma, tab, space, pipe, semicolon, or colon.
- `quoted=true`, `quotechar='"'`, `openquotechar`, `closequotechar`, and
  `escapechar` (the closing quote by default).
- `comment`: skip rows that start with this string.
- `ignoreemptyrows=true`; `ignorerepeated=false` treats a run of delimiters
  as one.

Values:
- `missingstring`: a string or vector of strings that also read as `missing`.
  An unquoted empty field is always `missing`.
- `truestrings`, `falsestrings`: Bool spellings; a list replaces the defaults.
- `dateformat`: a format string or `Dates.DateFormat`, or a per-column
  dictionary of them.
- `decimal='.'`, `groupmark` (digit grouping), and `stripwhitespace=false`.

Types and columns:
- `types`: one type, a vector with one type per column, or a dictionary keyed
  by column index, name, or `Regex`.
- `typemap`: a dictionary that replaces inferred types, such as
  `Dict(Int64 => Float64)`.
- `downcast=false`: narrow `Int64` columns to the smallest integer type that
  holds their values.
- `select`, `drop`: column indices, names, or a Bool mask; one name or index;
  a `Regex`; or an `(i, name) -> Bool` function.
- `stringtype=DataStrings.DataString`: the type of inferred text.
- `pool=false`: `true`, a ratio, `(ratio, maximum_levels)`, or per-column
  policies.
- `transpose=false`: read input rows as output columns.
- `scan`: a `Tables.Scan` request for projection, filters, and row bounds.
- `source`: with a vector of sources, add a column naming each row's source.

Problems:
- `on_error=:warn`: `:warn`, `:collect`, or `:error`. `strict=true` means
  `:error`.
- `maxproblems=10_000` (or `maxwarnings`): the number of problems retained.
- `validate=true`: require `types`, `dateformat`, and `pool` dictionary keys
  to match a column.

Performance:
- `ntasks`: the maximum number of parsing tasks. `parallel=false` uses one.
- `chunkbytes`: the target size of a structural-index chunk.
- `nsample`: rows sampled for type inference. `samplebytes=65536`: bytes
  sampled for delimiter detection.
- `fastindex=true`: the vector scanner; `false` uses the byte-at-a-time
  scanner.
- `buffer_in_memory=false`: copy a local file into memory instead of mapping
  it. `prefetch=true`: touch a mapped file's pages on worker threads.

See the [Reading data](@ref) guide for details.
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

It takes the keywords of [`CSV.File`](@ref CSV.File) for one source, except
`pool`, `typemap`, `downcast`, `transpose`, `scan`, `on_error`, `strict`,
`maxproblems`, `ntasks`, and `nsample`.
""" lazy
@doc """
    CSV.LazyFile

The indexed table that [`CSV.lazy`](@ref CSV.lazy) returns. It holds the
source bytes and the index, and no parsed values. `names(lf)`, `size(lf)`, and
`Tables.columnnames` come from the index. `lf.name` and `lf[:name]` return a
column view whose cells parse when read; `lf.name[i]` parses one cell. A cell
is text (`stringtype`, `DataString` by default) or `missing` unless `types`
requested a type for its column. A view keeps the source alive, so a
`LazyFile` is a way to look before parsing, not a way to hold a large file
cheaply. [`CSV.File`](@ref CSV.File)`(lf)` runs the eager typed parse on the
existing index and keeps the columns `lf` selected.
""" LazyFile
@doc """
    CSV.Rows(source; types=nothing, stringtype=DataStrings.DataString, keywords...)

Iterate Tables.jl row views; each cell parses when it is accessed. The source
and an index of every row stay in memory, so `length(rows)` and `names(rows)`
are known before iteration. `reusebuffer` is accepted and ignored. Invalid or
malformed cells
become `missing` by default; `strict=true` or `on_error=:error` throws a
[`CSV.ParseError`](@ref CSV.ParseError) when the cell is accessed. Rows do not
retain parse diagnostics, so use [`CSV.File`](@ref CSV.File) when
`CSV.problems` or a diagnostic cap is needed. `select` and `drop` (a list, one
name, a `Regex`, or an `(i, name) -> Bool` function) project columns in stable
file order. Other keywords are those of [`CSV.File`](@ref CSV.File) for one
source, except `pool`, `typemap`, `downcast`, `transpose`, `scan`,
`maxproblems`, `ntasks`, and `nsample`.
""" Rows
@doc """
    CSV.Chunks(source; ntasks=Threads.nthreads(), chunkbytes=nothing, keywords...)

Iterate a source as [`CSV.File`](@ref CSV.File) batches that all have the
same column types, and serve them through `Tables.partitions`. Pooling is
decided per batch; `select` and `drop` apply to every batch. With the default
`on_error=:warn`, the first batch with parse problems prints one summary
warning; every batch keeps its own [`CSV.problems`](@ref CSV.problems).

One batch is one window of the source's data bytes. `chunkbytes` is the target
size of that window and defaults to 64 MiB, or the whole source when it is
smaller. A window ends at a row boundary, so a batch holds every row that starts
within its bytes and one complete row can push it past `chunkbytes`. `ntasks`
bounds the parallel work inside a window; it is not a batch count and does not
affect the batch size.

Each window is indexed when its batch is produced and released with it, so a
read holds one window's structural index rather than the whole source's. The
source bytes stay in memory for later batches, and a returned batch owns its
own. The constructor validates every row it will yield, before the first batch,
to settle one schema. Value validation excludes the rows after `limit`: the
window that holds the limit is indexed to its end, and the rows past the limit
are dropped before any value is read. The pass starts over when a quote that did
not start its field, or a timestamp that needs microseconds, invalidates what it
settled. Its final run walks the windows iteration walks, so it also counts the
batches and `length` is defined.

Other keywords are those of [`CSV.File`](@ref CSV.File) for one source, except
`typemap`, `downcast`, `transpose`, `scan`, `maxwarnings`, and `nsample`;
`pool` takes one policy for every column.
""" Chunks
@doc """
    CSV.read(source, sink; keywords...)

Parse with the same options as [`CSV.File`](@ref CSV.File), then call the
Tables.jl `sink`, as in `CSV.read("data.csv", DataFrame)`. The new columns are
passed as `Tables.CopiedColumns`, so a sink that honors that marker can take
ownership without another copy.
""" read
@doc """
    CSV.problems(file) -> Vector{CSV.Problem}

Return the parse problems a `CSV.File` retained, in source order: at most
`maxproblems` of them (10,000 by default). Showing the file reports how many
were recorded in total. Pass `on_error=:collect` to a reader to record problems
without the summary warning, or `on_error=:error` to throw a
[`CSV.ParseError`](@ref CSV.ParseError) instead.
""" problems
@doc """
    CSV.write(sink, table; keywords...) -> sink

Write any Tables.jl `table` as delimited text to a path or `IO` and return
`sink`. An `IO` is written at its current position. Column tables render in
parallel; row tables and [`CSV.Chunks`](@ref CSV.Chunks) stream without being
collected. The output is the same for every `ntasks` value.
`CSV.write(sink; keywords...)` returns a function of the table, for
`table |> CSV.write(path)`.

# Keywords

- `delim=','`: a character or a string such as `"::"`.
- `quotechar='"'`, `openquotechar`, `closequotechar`, and `escapechar` (the
  closing quote by default).
- `quotestyle=:minimal`: quote text only when it needs quotes; `:all` quotes
  all text, and `:none` throws for text that needs quotes. `quotestrings=true`
  means `:all`.
- `newline='\\n'`, `missingstring=""`, and `bom=false`.
- `writeheader=true` (`false` when appending) and `header`, a vector of
  replacement names.
- `append=false`: append to a path instead of replacing it.
- `compress=:auto`: gzip a path ending in `.gz`; `:gzip` or `:none` force it
  on or off.
- `floatformat`: a Printf format such as `"%.2f"`; `decimal='.'`.
- `dateformat`: a Dates format for `Date`, `DateTime`, `Timestamp`, and `Time`
  values.
- `transform`: a `(col, val) -> val` function applied to each cell in
  row-major order.
- `partition=false`: write each of `Tables.partitions(table)` to its own sink,
  from a vector of sinks or a base path that gets `_1`, `_2`, ... appended (the
  generated paths are returned).
- `ntasks=Threads.nthreads()`: the maximum number of tasks.
- `bufsize=4 MiB`: the largest allowed row, in bytes.

See the [Writing data](@ref) guide for details.
""" write
@doc """
    CSV.RowWriter(table; keywords...)

Iterate the rows of `table` as CSV-formatted strings, the header first unless
`writeheader=false`. It takes the dialect and formatting keywords of
[`CSV.write`](@ref CSV.write); joining its rows gives the text `CSV.write`
writes with the same keywords.
""" RowWriter
# Julia 1.11 added `public`. Build the expression at runtime so Julia 1.10 can
# still parse this file. The surface remains deliberately unexported: users
# call it through the `CSV` namespace.
@static if VERSION >= v"1.11"
    Core.eval(@__MODULE__, Expr(:public, :File, :lazy, :LazyFile, :Rows,
                                :Chunks, :read, :problems, :Problem,
                                :ParseError, :write, :RowWriter))
end

# -- precompile workload -------------------------------------------------------
# The specialized per-column loops are what makes the first `File` call
# expensive to compile. One small in-memory pass through
# each public reader and writer caches those specializations: File (type
# inference, type changes, pooling, missing values, each built-in value type,
# gzip, parallel parsing,
# stringtype=String materializer), Rows, Chunks, write, and RowWriter.
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
        # Exercise common option values and selected-column loops. The API
        # boundaries share code across keyword combinations; this covers the
        # few scalar conversions and execution paths absent from a default read.
        File(IOBuffer(mixed); comment="#", missingstring="NA",
             dateformat="yyyy-mm-dd", limit=1)
        File(IOBuffer(mixed); typemap=Dict(Int64 => Float64))
        File(IOBuffer(mixed); select=[:int, :str])
        File(IOBuffer(mixed); drop=[:null])
        File(IOBuffer(mixed); types=Dict(:int => Int64))
        File(IOBuffer(mixed); types=String)
        File(IOBuffer(mixed); dateformat=Dict(:date => Dates.DateFormat("yyyy-mm-dd")))
        # Use the one-shot codec here: the streaming form's handling of a
        # caller-owned IO differs across the TranscodingStreams versions that
        # CodecZlib 0.7 admits, and the precompile workload must be stable.
        compressed = transcode(CodecZlib.GzipCompressor,
                               Vector{UInt8}(codeunits(mixed)))
        File(compressed)
        foreach(identity, Rows(IOBuffer("a,b\n1,x\n2,y\n")))
        foreach(identity, Rows(IOBuffer("a;b\n1;x\n"); delim=';'))
        first(Chunks(IOBuffer(pooled); chunkbytes=1 << 16))
        first(Chunks(IOBuffer("a;b\n1;x\n"); delim=';'))
        lf = lazy(IOBuffer(mixed))
        lf[1, :int]
        collect(lf.str)
        File(lf)
        lazy(IOBuffer("a;b\n1;x\n"); delim=';')
        out = IOBuffer()
        write(out, (a=[1, 2], b=["x", "y,z"], c=[1.5, missing],
                    d=[Dates.Date(2024, 1, 2), Dates.Date(2024, 3, 4)]))
        # A parsed table has abstractly typed columns: that generic column
        # path, and the path sink, are the common read-then-write workflow.
        write(out, f)
        mktemp() do path, _
            write(path, f)
        end
        join(RowWriter((a=[1], b=["x"])))
    end
end

__init__() = _probecpu!()

end # module CSV
