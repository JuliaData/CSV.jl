# Arrow zero-copy handoff — the plan

*Status: design only. No code. 2026-08-14.*

## The bet, restated

`CompactStringPayload` was designed against Arrow's **Utf8View** ("German
strings") layout on purpose: 16 bytes per string, a 4-byte length, a 4-byte
prefix, and either 12 inline bytes or a (buffer, offset) pointer pair. The
goal of this plan is to cash that bet: hand a parsed table to Arrow consumers
(Arrow.jl, DuckDB, pyarrow, polars) **without materializing a single string**.

## Layout mapping (what exists → what Arrow wants)

| kernel                      | Arrow Utf8View               | conversion |
|-----------------------------|------------------------------|------------|
| `a` bits 0..31 = length     | `length: int32`              | move |
| `a` bits 32..63 = bytes 1–4 | `prefix: 4 bytes`            | already identical by design |
| `b` = bytes 5–12 (inline)   | inline bytes 5–12            | move |
| `b` = Int64 offset, sign = which buffer | `buf_index: int32` + `offset: int32` | sign → index {0: input, 1: extra}; magnitude → offset |
| `PAYLOAD_MISSING` (len −1)  | validity bitmap bit = 0      | tag pass |

One flat O(n) pass over payloads rewrites 16B words into 16B words — **no
string bytes move**. The data buffers list is exactly two entries: the input
buffer (usually the mmap) and the column's `extra` (unescaped cells).

**The int32 constraint**: Arrow view offsets are int32, so each referenced
buffer must be < 2 GiB. The production plan already commits to chunk-owned
buffers under 2 GiB; until then, files ≥ 2 GiB fall back to per-chunk buffer
segmentation (the buffers list is variadic — one entry per chunk is legal
Arrow) or to offset-based `utf8` export with a copy. Design the converter
against a Vector of buffers from day one.

## Column-by-column

- **`Vector{T}` (bits)** — Arrow primitive arrays can wrap Julia vector
  memory as-is on the in-memory path; nothing to build.
- **`Vector{Union{T,Missing}}`** — Julia's layout is data + tag *bytes*;
  Arrow wants data + validity *bitmap*. An O(n/8) tag→bitmap pass builds the
  bitmap; the data region cannot be aliased out of a Julia union array via
  public API, so v1 copies data (memcpy-class). The endgame (phase 3) is a
  driver mode whose typed finals are (data, bitmap) pairs natively —
  union-direct finals already proved the write-direct pattern works.
- **`CompactStringVector`** — the headline: payload-word rewrite + two-entry
  buffer list, zero string-byte copies. Missing payloads become validity 0.
- **`PooledArray` (from File)** — Arrow DictEncoded: indices = refs − 1
  (one O(n) subtract pass; Arrow is 0-based and encodes missing in validity,
  not as ref 0), dictionary = the levels exported as Utf8View.
- **`Vector{Missing}`** — Arrow null array, length only.

## Ownership and lifetime

Zero-copy only matters for the **in-memory** handoff; Arrow IPC file/stream
writes serialize (copy) regardless. Two deliverables:

1. **Arrow.jl in-memory table**: an `Arrow.Table` whose columns wrap our
   buffers. The table must retain references to the input buffer (mmap) and
   each `extra` — plain Julia references suffice; GC does the rest. Requires
   Arrow.jl's Utf8View (`arrow.string_view`) support on the *read/wrap*
   side — verify its maturity first; it landed for format 1.4 but wrapping
   external buffers may need a small Arrow.jl PR.
2. **C Data Interface export** (`ArrowArray`/`ArrowSchema`): the release
   callback holds a `Ref` pinning {input buf, extras, payload words, bitmaps}
   until the consumer releases. This is what makes `pyarrow`/DuckDB/polars
   ingestion zero-copy. Alignment: Julia arrays are 16-byte aligned —
   satisfies Arrow's 8-byte minimum (64-byte is only a recommendation).

## Phases

- **P1 — converter core** (S, ~2–3 days): payload→view-word pass, validity
  bitmaps, dictionary export, buffers-list plumbing, exhaustive equality
  differential (every cell via Arrow read-back == every cell via our
  getindex, incl. escaped/extra-backed and >2 GiB-guard tests).
- **P2 — C Data Interface** (M, ~3–4 days): schema+array export with release
  callbacks, round-trip through pyarrow in CI (pycall-free: write a small C
  harness or use DuckDB's Julia client as consumer), lifetime fuzz (release
  after GC pressure).
- **P3 — validity-native typed finals** (M, optional): driver writes
  (data, bitmap) instead of Vector{Union}, erasing the union-column copy;
  gate on P1/P2 demand.
- **P4 — upstream** (ongoing): Arrow.jl PRs where wrapping externally-owned
  Utf8View buffers needs support; coordinate the CSV.File→Arrow.write fast
  path so `Arrow.write(file, CSV.File(...))` skips materialization.

## Risks / open questions

- Arrow.jl Utf8View maturity on the wrap-external-buffers path (audit first;
  the C interface path does not depend on it).
- Int32 offsets vs single >2 GiB inputs before chunk-owned buffers land.
- Union-column data aliasing is impossible via public API today (P3 exists
  because of this).
- `extra` rebase invariants: after the parallel extras rebase, offsets are
  final-column-relative — exactly what the buffers list needs; keep the
  invariant pinned by test so a future driver change can't silently break
  the export.
