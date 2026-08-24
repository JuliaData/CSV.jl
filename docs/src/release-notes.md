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
- Typed value parsing uses the reviewed low-level kernels from Parsers 3.
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
reviewed scan API. The Parsers 3 integration decision is complete. CSV.jl
depends on Parsers 3 for its reviewed low-level kernels. It keeps
`CSV.CompactString` as the default text type. CI temporarily pins
[Parsers.jl PR #210](https://github.com/JuliaData/Parsers.jl/pull/210) at exact
`e4adc5ba720e5668b726f65a574e2037c866d6df` until registration. A registered
Parsers 3 release and removal of this pin are 1.0 tag gates. Registered
InlineStrings releases still require Parsers 2. CI pins
[InlineStrings.jl PR #93](https://github.com/JuliaStrings/InlineStrings.jl/pull/93)
at exact `ce4c3549691c4b3443cc14ffa90ebdd6636eff2f`. A compatible InlineStrings
release and removal of this pin are also 1.0 tag gates.

The final source layout has one runtime module: `CSV`. Implementation files are
includes, not public submodules. Maintainers must run the full release matrix
and hand-review all generated contributions. The tagged release notes must
replace this section with the final dependency versions and verified test
results.
