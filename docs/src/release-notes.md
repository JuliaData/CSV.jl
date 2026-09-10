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
- A quote in the middle of a field (`5' 11"`) is ordinary content. The
  structural scan notices such a quote and rebuilds its index under the
  field-start rule, so the file still parses correctly; input without such
  quotes never pays for the check.
- Typed value parsing uses the low-level parsers of Parsers 3.
- Parse recovery produces structured `CSV.problems(file)` data. `on_error`
  selects one summary warning (`:warn`, the default), silent collection
  (`:collect`), or fail-fast `CSV.ParseError` (`:error`).
- `CSV.Chunks` uses one stable schema for its complete row window.
- Compatible Tables.jl releases can send a `Tables.Scan` projection, filter,
  type request, offset, and limit into the parser.
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
- CSV does not infer decimal types. With DataDecimals loaded, an explicitly
  requested decimal type parses exactly from the field bytes; a value that
  would need rounding is a problem.

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
