# The CSV.jl kernel prove-out

A stand-alone, working implementation of the proposed CSV.jl internals rewrite,
small enough to read as one unit, complete
enough to demonstrate that the architecture holds end-to-end:

- a **structural pass** (branchless SWAR fast path + a scalar reference state
  machine) that produces a field-offset index,
- **deterministic parallel chunking** — chunk entry quote-states are *computed*
  by parity composition, not guessed-and-retried,
- **typed parsing over the index**, column by column, in monomorphic loops,
  with per-column promotion (never a whole-chunk re-parse),
- **problems-as-data** instead of warning spam,
- and the three user-facing surfaces (`CSV.read` / `CSV.Chunks` / `CSV.Rows`
  analogs) rebuilt as thin layers over the same primitives.

## Files

| file | contents |
|---|---|
| `core.jl` | the kernel: `Dialect`, structural index (scalar + SWAR scanners), parallel indexing, type detection & promotion lattice, column builders, `CSVKernel.parse` driver, `Problem` log |
| `test.jl` | ~4100 assertions: pinned structural cases, a randomized round-trip property test run across every scanner × chunk-geometry combination, typed-layer tests, examples-layer tests |
| `examples.jl` | `KernelExamples.read` / `.batches` / `.rows` — the CSV.jl API surfaces as compositions of kernel blocks, plus Tables.jl integration |
| `bench.jl` | rough throughput probe (index vs full-parse split; optional CSV.jl comparison) |
| `Project.toml` | standalone environment (Parsers, Tables, Dates + test deps) |

```sh
julia --project=kernel -e 'using Pkg; Pkg.instantiate()'   # once
julia --project=kernel -t4 kernel/test.jl                  # full suite
julia --project=kernel kernel/examples.jl                  # demo
julia --project=kernel -t8 kernel/bench.jl                 # throughput probe
```

## The architecture, in five decisions

**1. Structure is separated from values.** One pass finds every field's
`(offset, len)` and every row boundary (quote-aware); `Parsers.xparse` is then
applied to *exact field spans*. Nothing downstream rediscovers boundaries:
comment/empty-row filtering uses the index, and header/type/value parsing only
examines assigned spans. This deletes the five hand-rolled, subtly-different
quote-skipping byte loops in today's `detection.jl`.

**2. Iteration order becomes ours.** Because the index decouples reading order
from file order, typed parsing runs **column-at-a-time within each chunk**: one
monomorphic loop per (column × chunk), dynamic dispatch once per loop instead of
once per cell. This is the direct fix for today's 24-branch `parserow` chain,
the duplicate `@unrollcolumns` table in `rows.jl`, and the `@generated`
custom-type kernels — all three collapse into `parsecolchunk!`.

**3. Promotion is column-local.** A type conflict aborts one column's loop,
promotes through a small lattice (`Missing → Int64 → Float64 → String`,
temporals/Bool → `String`), and re-runs *that column* over the retained index.
Today's `promotetostring!` re-parses every column of the chunk from its start.
Inference seeds from a **stratified sample** (evenly spaced rows across the
whole index — late-file surprises are as visible as early ones), which is what
makes mid-parse promotion rare in the first place.

**4. Parallelism is deterministic, not speculative.** Quote-toggle parity is
associative, so a parallel per-range popcount plus an exclusive XOR-scan gives
every range its true entry quote-state — the two-state specialization of
ParPaRaw's FSM-composition, strictly stronger than DuckDB's speculate-then-
validate. Row starts follow deterministically; results are identical for any
`chunkbytes` and any thread count (the test suite runs every structural case
at chunk sizes 3/7/16/64 to hold this property). The old `findrowstarts!`
"guess, sample, maybe fall back to single-threaded" — the root of the
multithreaded-corruption issue family (#1019, #1143, #1157) — has no analog
here because there is nothing to guess.

**5. Errors are data.** Short/long rows, invalid values under user-provided
types, malformed quoting, and unclosed-quote EOF all become `Problem` records
(row, column, byte offset, kind, excerpt) on the result, bounded by
`maxproblems`, with `on_error=:error` as a post-parse escalation. No `@warn`
spam, nothing lost to a terminal scrollback.

## Column storage

- isbits columns: `Vector{T}` + `Vector{Bool}` presence — no sentinel
  arithmetic (retires the SentinelArrays dependency for this purpose); columns
  with no missings hand back the raw `Vector{T}` zero-copy.
- string columns: content spans into the input buffer, materialized (and
  unescaped) lazily on `getindex`; `materialize(col)` copies out. The
  view-vs-copy choice moves to *after* parsing instead of CSV.jl's up-front
  `stringtype=` commitment. Escaped-length is `Int32`, deliberately not
  Parsers' `PosLen` (whose 20-bit length cap is the root of CSV.jl #935).
- exact allocation: the index knows the exact row count before any value is
  parsed — `rowsguess`, `reallocate!`, and growth heuristics have no analog.

## Measured breadth (kernel/bench.jl)

`bench.jl` runs a shape × size matrix (numeric, mixed, string-heavy,
quoted-with-embedded-newlines, 200-column wide, 2-column long, missing-heavy ×
10 KiB → 200 MiB) against the installed CSV.jl. Ratios from an M-series laptop,
8 threads (kernel-lazy / CSV.File; `kernel+str` additionally collects string
columns to `Vector{String}`):

- **10 KiB: kernel wins every shape, ~1.9–3.9×** (absolute: 56–110 µs vs
  CSV.File's 112–255 µs) — no Context ceremony, size-aware sampling.
- **1–200 MiB: a parity band, ~0.85–1.25×** — ahead on sparse/strings-lazy/
  longnarrow/numeric, behind ~10–15% on `mixed` (the price of the
  sample-independence guard on Bool/temporal columns) and on `wide`
  (column-at-a-time wants cache-resident chunks; the 1 MiB `chunkbytes` cap
  recovered most of it: 623 → 911 MiB/s at 200 MiB × 200 cols).
- **Single-threaded (20 MiB): kernel ahead on 5/7 shapes (1.1–1.5×)**, ~parity
  on the rest — the wins are not a threading artifact.
- **`kernel+str` on string-heavy shapes is ~0.35×**: collecting to
  `Vector{String}` heap-allocates every cell, vs CSV.jl's unboxed InlineStrings.
  This is precisely the gap the planned inline-else-view string layout closes;
  the lazy default is already at/above parity.

Two honest architecture taxes to track: the index is a second pass over the
bytes (visible on pure-numeric RAM-bound runs), and quote-heavy fields get their
quotes scanned twice (index + `xparse`) — the designed fixes are pipelined
chunk index→parse (ChunkedBase-style) and carrying the index's quoted/escaped
bits into value parsing.

## Pinned semantics (deliberate, tested)

- Structural quotes **always toggle** (the Sep/simdcsv rule). A bare
  quote mid-field opens a quoted region; `test.jl` pins the exact behavior.
  This is the price of composable parallelism; a "quotes only at field start"
  strict mode is possible as a scalar-path dialect if ever needed.
- Whitespace is content unless `stripwhitespace=true` (then a whitespace-only
  field is `missing`).
- Unquoted empty fields are `missing`; quoted `""` is the empty string.
- A `\r`-terminated row followed by an empty `\n`-terminated row is *bytewise
  ambiguous* CSV (indistinguishable from one CRLF); the property-test generator
  documents and avoids manufacturing it.

## What the real integration adds (designed seams, not rework)

| concern | where it plugs in |
|---|---|
| streaming/mmap/compressed/URL sources | an L0 `ByteSource` trait producing the same chunks; today's `getsource` tempfile hack dies |
| dialect & schema sniffing (`CSV.Spec`) | replaces the fixed `Dialect(...)` construction; everything downstream unchanged |
| InlineString widths, Int downcast, typemap | more entries in the column-builder dispatch + a wider promotion lattice |
| pooled columns | a pooling `ColumnBuilder` doing dictionary lookups inside `parsecolchunk!` (spans make interning allocation-free) |
| `select`/`drop` | skip entries in the (column × chunk) task grid — projection is free when parsing is columnar |
| multi-byte delimiters | already routed through the scalar scanner |
| `ignorerepeated` | delimiter-run suppression in the scalar scanner |
| transpose | a utility path over the same source and value primitives |
| SIMD.jl / CLMUL 64-byte scanner | drop-in replacement for `indexchunk_swar!` behind the same emission helpers |
| `Memory{T}` buffers, word-aligned validity bitmaps | swap inside `TypedColumn` on Julia ≥ 1.11 |
| writer | untouched by this kernel (separate workstream) |

## Old → new map

| CSV.jl today | kernel |
|---|---|
| `Context` (700-line constructor) | `Dialect` + `index()` + driver kwargs (separable, testable) |
| `detectdelimandguessrows`, `skiptorow`, `findrowstarts!`, `checkcommentandemptyline`, `ReversedBuf` footer logic | the structural index + row-level hygiene in `endrow!` |
| `parserow`'s 24-branch type switch | one dynamic dispatch per (column × chunk) into `parsecolchunk!` |
| `rows.jl` `@unrollcolumns` | gone — `KernelExamples.rows` iterates the index |
| `@generated parsecustom!` | gone — validated concrete `Parsers` targets use the same builder path |
| `promotetostring!` whole-chunk re-parse | per-column re-parse over the retained index |
| `SentinelVector` / `ChainedVector` columns | `Vector{T}` + presence, written into exact-size slices |
| `PosLenString` / `stringtype=` commitment | lazy string views + post-hoc `materialize` |
| `maxwarnings` @warn stream | `problems(t)::Vector{Problem}` + `on_error` |
