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
| `b` = Int64 offset, sign = which buffer | `buf_index: int32` + `offset: int32` | sign → index {0: input, 1: extra}; `abs(b) - 1` → zero-based offset |
| `PAYLOAD_MISSING` (len −1)  | validity bitmap bit = 0      | set validity 0 and emit a zeroed empty view |

One flat O(n) pass over payloads rewrites 16B words into 16B words — **no
string bytes move**. Kernel offsets are signed and **one-based**: input views
store `cpos`, while extra views store `-(extra_position)`. Arrow offsets are
signed int32 and zero-based, so the subtraction above is required. Without it,
every long string starts one byte late. Before input-buffer segmentation, the
data buffers list has two logical entries: the input buffer (usually the mmap)
and the column's `extra` (unescaped cells). Empty buffers may be omitted if the
view indices are remapped consistently. A null slot must not leak the kernel's
negative length into the Arrow views buffer; the converter writes a canonical
zero-length inline view while clearing its validity bit.

**The int32 constraint**: Arrow view lengths, start offsets, and buffer indices
are signed int32. Each must be nonnegative and no larger than
`typemax(Int32)`, and the resulting half-open range must be inside its buffer.
A data buffer itself may be larger than 2 GiB, but a view cannot start beyond
the signed-int32 range. The production plan already commits to chunk-owned
buffers under 2 GiB; until then, files ≥ 2 GiB fall back to per-chunk buffer
segmentation (the buffers list is variadic — one entry per chunk is legal
Arrow) or to offset-based `utf8` export with a copy. Design the converter
against a Vector of buffers from day one.

## Column-by-column

- **`Vector{T}` (compatible fixed-width numeric primitives)** — Arrow arrays
  can wrap Julia vector memory as-is when the Arrow logical type has the same
  width and representation. This does **not** cover `Bool` (Arrow bit-packs
  it), `Int128` (no plain Arrow integer type), or Julia temporal values whose
  unit or epoch differs; those need conversion or a deliberate Arrow type.
- **`Vector{Union{T,Missing}}`** — Julia's layout is data + tag *bytes*;
  Arrow wants data + validity *bitmap*. An O(n/8) tag→bitmap pass builds the
  bitmap; the data region cannot be aliased out of a Julia union array via
  public API, so v1 copies data (memcpy-class). The endgame (phase 3) is a
  driver mode whose typed finals are (data, bitmap) pairs natively —
  union-direct finals already proved the write-direct pattern works.
- **`CompactStringVector`** — the headline: payload-word rewrite + two-entry
  buffer list, zero string-byte copies. Missing payloads become validity 0.
- **kernel `PooledColumn` (before `File` conversion)** — Arrow DictEncoded:
  indices = refs − 1 (one O(n) subtract pass; Arrow is 0-based and encodes
  missing in validity, not as ref 0), dictionary = the compact levels exported
  as Utf8View. The current `CSVApi.File` conversion materializes those levels
  to `String` inside `PooledArray`, so the zero-copy hook must run before
  `_pooledarrays` or retain the kernel pooled representation explicitly.
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
   callback owns a GC-rooted handle that pins {input buf, extras, converted
   view words, bitmaps} until the consumer releases. A bare `Ref` stored only
   as a C pointer is not a Julia GC root; use a rooted owner registry (or an
   equivalent runtime-supported stable handle), and let the release callback
   remove that root exactly once. This is what makes `pyarrow`/DuckDB/polars
   ingestion zero-copy. Utf8View also requires the C Data Interface's extra
   variadic-buffer-sizes buffer. Do not depend on undocumented Julia allocator
   alignment: verify each exported pointer and copy into a suitably aligned
   owner only where a consumer requires it (Arrow recommends 8- or 64-byte
   alignment; IPC serialization handles its own required alignment and
   padding).

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
- One-based signed kernel offsets vs zero-based Arrow view offsets; conversion
  must use `abs(offset) - 1`, validate each int32 field, and check the full
  `offset + length` range against its buffer.
- Null payloads use a negative kernel length. Arrow output must replace the
  view with a canonical zero-length value as well as clear validity.
- `Bool`, `Int128`, and temporal physical-layout conversions; the primitive
  zero-copy path must use an explicit compatibility table, not `isbits(T)`.
- `CSVApi.File` currently materializes pooled string levels before constructing
  `PooledArray`; export must intercept the kernel `PooledColumn` first.
- The C Data Interface requires the Utf8View variadic-buffer-sizes buffer in
  addition to validity, views, and variadic data buffers.
- A raw pointer to a Julia `Ref` does not pin its referents. The exported
  `private_data` owner needs an explicit GC root until the release callback.
- Union-column data aliasing is impossible via public API today (P3 exists
  because of this).
- `extra` rebase invariants: after the parallel extras rebase, offsets are
  final-column-relative — exactly what the buffers list needs; keep the
  invariant pinned by test so a future driver change can't silently break
  the export.
