# CSV.jl 1.3.0 release notes

Unreleased. Requires Julia 1.10 or later.

## Schema validation and trimmed readers

- `CSV.Rows(source; schema=Tables.Schema(names, types))` validates the header,
  row width, missing values, and values against the supplied schema. Iteration
  returns `NamedTuple` rows with exactly the schema's types.
- `CSV.File(source; schema)` performs the same checks before returning one
  `Vector{T}` per column, or a `DataStrings.StringVector{T}` for a
  `DataString` column, as `CSV.File` returns text without a schema. Text
  columns own their bytes. `CSV.read` passes these columns to a sink.
- The dialect and row-window options still apply. Per-column date formats
  name the schema's columns, with or without a header row.
- Schema reads compile with `juliac --trim=safe`. Sources are local paths,
  `IO`, or bytes; URLs are rejected. Custom date formats and generic
  user-defined `tryparse` fallbacks cannot yet be trimmed.
  Julia 1.12 trimmed programs need one thread or
  `parallel=false, prefetch=false`; Julia 1.13 supports multiple threads.

See [Reading with a known schema](@ref) for examples and constraints. Existing
reads keep their behavior when `schema` is omitted or `nothing`.

# CSV.jl 1.2.0 release notes

Released October 27, 2026. Requires Julia 1.10 or later.

## Changes

- `select` and `drop` accept an `(i, name) -> Bool` function again, as in 0.10.
  It receives each column's position and name before any value is read. 1.0
  and 1.1 rejected it. `types` and `pool` still do not accept functions.

## Fixes

- Delimiter detection only elects a delimiter that splits the first row. A
  single-column file of times or timestamps, such as `event time` over
  `10:30:00` rows, is read as one column again instead of being split on `:`,
  as in 0.10.
- A parallel read with a `typemap` whose target is outside the promotion order
  (such as `Int64 => Int32` or `Timestamp{Nanosecond} => DateTime`) could read a
  sparse column as text. It now gives the same type as a serial read.
- A header with many copies of one name is no longer quadratic to read: 10,000
  copies took 40 s and now take milliseconds.

## Performance

- Text columns reserve each chunk's string bytes once instead of growing their
  buffers cell by cell: text-heavy files read up to twice as fast with several
  tasks and use less memory.
- Wide files read 20-25% faster with several tasks: each chunk reads every
  column's type under one lock.
- Quoted fields are checked for inner quotes with one vector compare, so quoted
  files use about 10% less CPU.
- Loading CSV no longer invalidates compiled Base code: a redundant `eltype`
  method for `CSV.File` invalidated about 400 method instances.

## Documentation

- `?CSV.File` lists every reader keyword with its default, and `?CSV.write`
  lists the writer keywords.
- The migration guide's rows for date-times, empty fields, Booleans,
  `footerskip`, and `maxwarnings` now match what 0.10 did.

# CSV.jl 1.1.0 release notes

Released September 21, 2026. Requires Julia 1.10 or later.

## `CSV.Chunks` streams windows of the source

`CSV.Chunks` no longer builds a structural index of the whole source. One batch
is now one window of the source's data bytes: the window is indexed when its
batch is produced and released with it, so a chunked read holds one window's
index instead of the file's. The index of a narrow numeric file is several times
the size of the file, so this is the difference between a bounded read and one
that cannot start on a file larger than memory.

- `chunkbytes` is the target size of that window, in source bytes. It defaults
  to 64 MiB, or the whole source when it is smaller. A window ends at a row
  boundary, so one complete row can push a batch past `chunkbytes`.
- `ntasks` bounds the parallel work inside one window. It is not a batch count
  and no longer affects the batch size.
- The constructor still checks every row it will return before the first
  batch, so every batch has the same column types, as in 1.0.
  `length(chunks)` is the batch count.
- Only the index is bounded. The source bytes stay in memory for later
  batches, and each returned batch owns its own bytes.

Batch boundaries therefore differ from 1.0 for the same options. The rows,
column types, values, and diagnostics do not.

## Fixes

- Generic ARM64 package precompilation works on Julia 1.13.
- Typed `CSV.Rows` column access avoids per-row allocations.
- Floating-point output is quoted when its text conflicts with the delimiter
  or quote characters, including decimal separators, exponents, and `NaN`/`Inf`.
- A space-aligned file with a quote inside a field, such as
  `"a  b\n1  2\n3  x\"y\n"`, reads as two columns, as with an explicit
  `delim=' ', ignorerepeated=true`. 1.0 added an empty column between `a` and
  `b`.

# CSV.jl 1.0 release notes

CSV.jl 1.0 replaces the parsing and writing internals while keeping the main
CSV.jl entry points. It requires Julia 1.10 or later.

## Highlights

- A quote-aware structural index separates row and field discovery from typed
  value parsing. The same index drives eager files, row views, lazy access, and
  batches.
- `CSV.lazy` returns an indexed table that parses cells only when they are
  accessed. `CSV.File(lazyfile)` reuses the existing index.
- Inferred text uses `DataStrings.DataString` by default. Short text is stored in
  the value; longer text lives in column-owned buffers, so eager tables never
  refer to the source or to a mapped file.
- Typed values parse with the Parsers.jl 3 kernels.
- Parse recovery produces structured `CSV.problems(file)` data. `on_error`
  selects one summary warning (`:warn`, the default), silent collection
  (`:collect`), or fail-fast `CSV.ParseError` (`:error`).
- `CSV.Chunks` uses one stable schema for its complete row window.
- A `Tables.Scan` projection, filter, type request, offset, and limit go into
  the parser.
- The writer has deterministic parallel output, explicit quote styles,
  Printf-style floating-point formatting, bounded row-block memory, gzip suffix
  detection, parallel gzip compression (one member per row block), and parallel
  partition output.
- String conversion (`stringtype=String`, `types=String`, InlineStrings),
  `CSV.Chunks` batch parsing, and gzip output run in parallel.

## Compatibility changes

The most important default changes are:

- Julia 1.10 is the minimum runtime;
- `DataStrings.DataString` replaces InlineStrings.jl as the default text type;
- pooling is off unless requested;
- an unquoted empty field is always `missing`;
- `true`, `True`, `TRUE`, `false`, `False`, and `FALSE` are the default Boolean
  spellings;
- a date-time column is a `Timestamp{Nanosecond}` (Durations.jl) instead of a
  `Dates.DateTime`, ISO date-times accept `T` or a space, and fractions keep
  up to nine digits; and
- parse problems are retained as problem objects, with one summary warning per
  read instead of one warning per problem.

See [Migrating from 0.10 to 1.0](migration.md) for option mappings, writer
compatibility, source-memory behavior, and upgrade examples.
