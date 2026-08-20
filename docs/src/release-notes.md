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
- Inferred text uses `CSV.CompactString` by default. Short text is stored in
  the value. Long text can refer to the retained input buffer.
- Parse recovery produces structured `CSV.problems(file)` data. Applications
  can select collection or fail-fast behavior.
- `CSV.Chunks` uses one stable schema for its complete row window.
- Compatible Tables.jl releases can send a `Tables.Scan` projection, filter,
  type request, offset, and limit into the parser.
- The writer has deterministic parallel output, explicit quote styles,
  Printf-style floating-point formatting, bounded row-block memory, gzip suffix
  detection, and parallel partition output.

## Compatibility changes

The most important default changes are:

- Julia 1.10 is the minimum runtime;
- `CSV.CompactString` replaces InlineStrings.jl as the default text type;
- pooling is off unless requested;
- an unquoted empty field is always `missing`;
- exact lowercase `true` and `false` are the default Boolean spellings; and
- parse warnings are retained as problem objects instead of printed.

See [Migrating from 0.10 to 1.0](migration.md) for option mappings, writer
compatibility, source-memory behavior, and upgrade examples.

## Before final publication

The final release depends on a registered Tables.jl version that contains the
reviewed scan API. Maintainers must also resolve the Parsers 3.0 and shared
compact-string dependency choices, run the full release matrix, and hand-review
all generated contributions. The tagged release notes must replace this
section with the final dependency versions and verified test results.
