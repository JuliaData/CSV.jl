# The CSV.jl kernel prove-out

A stand-alone, working implementation of the proposed CSV.jl internals rewrite,
small enough to read as one unit, complete
enough to demonstrate that the architecture holds end-to-end:

- a **structural pass** (width-generic vector default + portable SWAR fallback +
  a scalar reference state machine) that produces a compact event tape,
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
| `core.jl` | the kernel: `Dialect`, tape index (vector + SWAR + scalar scanners), parallel indexing, type detection & promotion lattice, column builders, `CSVKernel.parse` driver, `Problem` log |
| `test.jl` | 11,890 assertions: pinned structural cases, randomized scanner and string properties, typed-layer tests, driver determinism stress, examples-layer tests |
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

**1. Structure is separated from values.** One scan emits every quote-aware
delimiter and row-end position to a flat tape. A compact assembly pass applies
row hygiene and records row starts; `fieldspan` then reconstructs `(offset, len)`
in O(1). `Parsers.xparse` is applied to those *exact field spans*. Nothing
downstream rediscovers boundaries:
comment/empty-row filtering is centralized in assembly, and header/type/value
parsing only examines assigned spans. This deletes the five hand-rolled,
subtly-different quote-skipping byte loops in today's `detection.jl`.

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
Inference seeds from a **stratified set of probe chunks** spanning the file,
which makes mid-parse promotion rare. Any unsampled conflict still joins the
shared promotion register, and stale segments re-parse under the frozen final
type, so the result does not depend on the sample.

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
- string columns: 16-byte `CompactString` payloads hold up to 12 bytes inline and view
  longer content in the retained input buffer. Long escaped values are
  unescaped once into a column-owned extra buffer. `getindex` returns a `CompactString`
  without allocating; `materialize(col)` copies to plain `String`s. Length is
  `Int32`, deliberately not Parsers' `PosLen` (whose 20-bit length cap is the
  root of CSV.jl #935).
- exact allocation: each indexed chunk knows its row count before segment
  parsing, and the stitch phase knows the exact global row count. `rowsguess`,
  `reallocate!`, and growth heuristics have no analog.

## Measured breadth (kernel/bench.jl)

`bench.jl` runs a shape × size matrix (numeric, mixed, string-heavy,
quoted-with-embedded-newlines, 200-column wide, 2-column long, missing-heavy ×
10 KiB → 200 MiB) against the installed CSV.jl. With the three deep pieces in
(width-generic 64-byte vector scanner, CompactString inline-else-view strings, fused index→parse
pipeline), on an M-series laptop at 8 threads (kernel ÷ CSV.File, kernel's
string columns being the zero-copy CompactString default):

- **10 KiB: kernel wins every shape, ~1.7–2.3×** (51–112 µs vs 100–255 µs).
- **20–200 MiB: wins or ties 5–6 of 7 shapes** — numeric 1.25–1.31×, sparse
  1.55–1.67×, strings 1.2×, longnarrow 1.1×, wide ~parity (was 0.54× before
  the fused pipeline); `mixed` 0.88–0.97× and `quoted` ~0.93× remain the
  honest gaps (the Bool/temporal sample-independence guard, and quotes being
  scanned by both the structural and value layers).
- **Single-threaded (20 MiB): wins 6 of 7 (1.03–1.58×)** — not a threading
  artifact.
- `kernel+str` (detaching to `Vector{String}`) is allocation-bound by
  definition; the CompactString columns are the intended interface (zero-alloc access,
  2× faster length/iteration than InlineString columns, direct ==(·,String)).

The fused driver indexes each chunk and parses all its columns while the bytes
are cache-hot (segments stitched into exact-size finals; single-chunk files
finalize the segment in place, zero copies), which is what removed the former
second-pass-over-RAM tax on numeric/wide at scale.

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
| `ignorerepeated` | assembly-time delimiter-run suppression shared by every scanner; `fieldspan` advances over each collapsed run in O(1) |
| transpose | a utility path over the same source and value primitives |
| additional scanner tuning | new `blockmasks` / `prefix_xor64` methods; tape emission and assembly stay unchanged |
| `Memory{T}` buffers, word-aligned validity bitmaps | swap inside `TypedColumn` on Julia ≥ 1.11 |
| writer | untouched by this kernel (separate workstream) |

## Old → new map

| CSV.jl today | kernel |
|---|---|
| `Context` (700-line constructor) | `Dialect` + `index()` + driver kwargs (separable, testable) |
| `detectdelimandguessrows`, `skiptorow`, `findrowstarts!`, `checkcommentandemptyline`, `ReversedBuf` footer logic | tape scan + row-level hygiene in `assemblerows!` |
| `parserow`'s 24-branch type switch | one dynamic dispatch per (column × chunk) into `parsecolchunk!` |
| `rows.jl` `@unrollcolumns` | gone — `KernelExamples.rows` iterates the index |
| `@generated parsecustom!` | gone — validated concrete `Parsers` targets use the same builder path |
| `promotetostring!` whole-chunk re-parse | per-column re-parse over the retained index |
| `SentinelVector` / `ChainedVector` columns | `Vector{T}` + presence, written into exact-size slices |
| `PosLenString` / `stringtype=` commitment | lazy string views + post-hoc `materialize` |
| `maxwarnings` @warn stream | `problems(t)::Vector{Problem}` + `on_error` |
