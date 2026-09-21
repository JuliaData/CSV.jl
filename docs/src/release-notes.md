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
- The constructor still validates every row it will yield, before the first
  batch, to settle one schema: every batch has the same column types,
  missingness, and settled text width, as in 1.0. That pass stops where `limit`
  stops, and its final run walks the same windows iteration walks, so it also
  counts them. `length(chunks)` is the batch count and costs no extra pass.
- Only the structural index is bounded. The source bytes stay in memory for
  later batches, and each returned batch owns its own bytes.

Batch boundaries therefore differ from 1.0 for the same options. The rows,
column types, values, and diagnostics do not.

## Fixes

- Generic ARM64 package precompilation works on Julia 1.13. The quote scanner
  retains its accelerated PMULL path behind a runtime CPU check, with a
  shift/XOR fallback. CI checks generic package images and JIT compilation on
  ARM64 and x86-64 with Julia 1.10 and current Julia.
- Typed `CSV.Rows` column access avoids per-row allocations.
- Floating-point output is quoted when its text conflicts with the delimiter
  or quote characters, including decimal separators, exponents, and `NaN`/`Inf`.
- A quote that does not start its field makes a reader prepare the source again
  under the lenient quote rule. That retry now keeps the whole syntax the first
  pass resolved, including a sniffed `ignorerepeated`. 1.0 kept only the
  delimiter, so an aligned-column source such as `"a  b\n1  2\n3  x\"y\n"` gained
  an empty column between `a` and `b`; it now reads as the two columns an
  explicit `delim=' ', ignorerepeated=true` always gave.

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
- Typed value parsing uses the low-level parsers of Parsers, now even faster in
  version 3.
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
  `CSV.Chunks` batch parsing, and gzip output run in parallel. Files with
  `comment` rows, `skipto`, and `footerskip` use the vector scanner, and so do
  files with a separate `escapechar` or distinct `openquotechar` and
  `closequotechar`. `CSV.Rows`
  carries its schema in its type, so `row.name` on a typed column returns a
  typed value without allocation. Transposed reads parse each row as a typed
  column. Several sources concatenate text as `DataString` columns.

## Compatibility changes

The most important default changes are:

- Julia 1.10 is the minimum runtime;
- `DataStrings.DataString` replaces InlineStrings.jl as the default text type;
- pooling is off unless requested;
- an unquoted empty field is always `missing`;
- `true`, `True`, `TRUE`, `false`, `False`, and `FALSE` are the default Boolean
  spellings;
- a date-time column is a `Timestamp{Nanosecond}` (Durations.jl) instead of a
  `Dates.DateTime`, ISO date-times accept `T` or a space, and no fraction digit
  is truncated; and
- parse problems are retained as problem objects, with one summary warning per
  read instead of one warning per problem.

See [Migrating from 0.10 to 1.0](migration.md) for option mappings, writer
compatibility, source-memory behavior, and upgrade examples.
