# CSV.jl 1.0 release notes

!!! warning "Draft"
    CSV.jl 1.0 is not registered yet. These notes describe the development
    branch and must be finalized against the tagged commit.

CSV.jl 1.0 replaces the parsing and writing internals while keeping the main
CSV.jl entry points. It requires Julia 1.10 or later.

## Highlights

- A quote-aware structural index separates row and field discovery from typed
  value parsing. The same index drives eager files, row views, lazy access, and
  batches.
- `CSV.lazy` returns an indexed table that parses cells only when they are
  accessed. `CSV.File(lazyfile)` reuses the existing index.
- Inferred text uses `DataStrings.DataString` by default. Short text is stored in
  the value. Long text can refer to the retained input buffer.
- Typed value parsing uses the reviewed low-level kernels from Parsers 3.
- Parse recovery produces structured `CSV.problems(file)` data. `on_error`
  selects collection (the default), one summary warning (`:warn`), or
  fail-fast `CSV.ParseError` (`:error`).
- `CSV.Chunks` uses one stable schema for its complete row window.
- Compatible Tables.jl releases can send a `Tables.Scan` projection, filter,
  type request, offset, and limit into the parser.
- The writer has deterministic parallel output, explicit quote styles,
  Printf-style floating-point formatting, bounded row-block memory, gzip suffix
  detection, and parallel partition output.

## Compatibility changes

The most important default changes are:

- Julia 1.10 is the minimum runtime;
- `DataStrings.DataString` replaces InlineStrings.jl as the default text type;
- pooling is off unless requested;
- an unquoted empty field is always `missing`;
- exact lowercase `true` and `false` are the default Boolean spellings; and
- parse warnings are retained as problem objects instead of printed.

See [Migrating from 0.10 to 1.0](migration.md) for option mappings, writer
compatibility, source-memory behavior, and upgrade examples.

## Dependency and release status

The rewrite uses released Parsers 3, InlineStrings 2, Tables 1.14, DataStrings
1, and DataDecimals 1, all registered in General. Verify a fresh registry-only
installation before tagging CSV 1.0.

Default string columns now use the shared DataStrings package. They support
column edits while preserving scalar values returned before an edit. CSV no
longer contains its own scalar string implementation.

Explicit DataDecimals types parse directly from byte spans with exact scale
checks. `inferdecimal=true` adds optional full-column consistent-scale detection.
Default numeric inference remains unchanged. See [Decimal columns](decimals.md).

All mandatory CI, downstream compatibility checks, and maintainer review remain
release gates. This PR does not tag or register CSV itself.
