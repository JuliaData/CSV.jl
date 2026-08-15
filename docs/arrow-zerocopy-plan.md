# Arrow zero-copy handoff — plan v2, targeting the Arrow.jl core rewrite

*Design only. No code. 2026-08-15. Supersedes v1, which was written against
released Arrow.jl 2.x. This version targets `~/.julia/dev/Arrow` branch
`core-rewrite`, `core/ArrowCore.jl` (the "runtime-tagged, C-data-shaped
core" from the redesign report §9) plus its `core/examples/cdata.jl` and
`ipc_write.jl` adapters. Names below are the rewrite's, verified against the
source at commit `75acaab`.*

## What the rewrite gives us (and why it fits)

The rewrite's core has exactly the shape a zero-copy CSV handoff wants:

| rewrite concept | what it is | why it matters here |
|---|---|---|
| `OwnerRegion(ptr, len; root)` | immutable `(ptr, len, alignment, root)`; validity is **GC reachability, nothing more** — no guards, no lifecycle, no revocation | our buffers (mmap, payload words, `extra`, refs) become regions by construction; holding a slice keeps them alive |
| `heapregion(v::Vector{T})` | borrows any isbits `Vector` zero-copy, the Vector is the `root` | one call per buffer we already own |
| `BufferSlice(region, offset, len)` | the only currency for buffer data; bounds-checked at construction | we hand slices, never raw pointers |
| `ArrayData(type, len, buffers; children, dictionary, nullcount=-1)` | runtime-tagged array: `type::ArrowType` value (not a Julia type parameter), `buffers::FrozenVector{BufferSlice}` by **role** from the layout registry, atomic on-demand `nullcount` | we construct these directly; no Julia-type-parameter explosion, no per-column codegen |
| `layoutspec(::ViewType) = ([VALIDITY, VIEWS], width 16, variadic)` | Utf8View: `buffers = [validity, views, data₁, data₂, …]` | our string columns map to this — see below |
| `layoutspec(t::IntType/FloatType/BoolType/DateType…)` | `[VALIDITY, DATA]` | our typed columns |
| `layoutspec(::DictionaryType)` | `[VALIDITY, DATA]` of the **index** type; values in `ArrayData.dictionary` (an object reference — no IPC id in core) | our pooled columns |
| `Field(name, type, nullable, metadata, children)` / `Schema` | schema data as **values** | one `Field` per column, no type-level names |
| `validate_structural / validate_semantic / validate_full` | staged; semantic results cached per `ArrayData` (atomic `semachecked`); Utf8View semantic check = long entries in-bounds **and prefix == first 4 data bytes** | our payloads satisfy the prefix rule by construction (v1's bet, still true) |
| `cdata.jl: to_c_data(f, d) → (ArrowSchema*, ArrowArray*)` | export with an `ExportedRoot` that keeps `roots` reachable until the consumer's release callback runs; exactly-once release; `reap!` | this is the pyarrow/DuckDB/polars door — ownership is the registry's job, not ours |
| `ipc_write.jl` writer | eager, copies buffer contents into message bodies; refuses nonzero element offsets | IPC is a copy path either way; zero-copy matters for in-memory + C-data |

The core does **not** carry an offsets-based `Utf8Type` requirement for us
— `ViewType(utf8=true)` is a first-class layout with accessors, validation,
IPC (format 1.4 `variadicBufferCounts`) and C-data (`vu` + trailing
int64 buffer-lengths buffer) all mapped. That is the whole point: our string
column layout **is** an Arrow layout the rewrite already speaks.

## The mapping, column by column

### CompactString columns → `ViewType(true)` (the headline, zero string-byte copies)

Our payload (16 B): `a = len:UInt32 | prefix4:UInt32` (low/high 32), `b =`
inline bytes 5–12 **or** an `Int64` offset whose **sign selects the buffer**
(`≥0` → input buffer at `off`, `<0` → the column's `extra` at `-off`).

Arrow view word (16 B): `len:Int32 | prefix4 | bufidx:Int32 | offset:Int32`
(inline: `len:Int32 | inline12`).

- Inline entries (`len ≤ 12`): **bit-identical already** (`len` then 12
  bytes). Nothing to do.
- Long entries: rewrite the `b` word only — `Int64 signed offset` →
  `(bufidx::Int32, offset::Int32)` with `bufidx = 0` for the input buffer,
  `1` for `extra`. One flat `@simd` pass over `payloads::Vector{CompactStringPayload}`;
  the payloads vector itself (reinterpreted as `UInt8` bytes) becomes the
  `VIEWS` slice via `heapregion`. **No string bytes move.**
- Buffers list: `[validity, views, input, extra]` — the two-entry variadic
  set. `input` is the mmap (already a Vector{UInt8} → `heapregion`; the
  rewrite's own `mmapregion` exists but we already hold the mapping, so we
  root it ourselves) and `extra` is a plain Vector → `heapregion`.
- Validity: our missing payload is the length field `-1` (`cslen(p) < 0`).
  When the column is null-free (`_allpresent`/no `PAYLOAD_MISSING`), pass
  the canonical empty `BufferSlice()` and `nullcount=0` — **no bitmap at
  all**. Otherwise build the bitmap in one O(n/8) pass from the payload
  length signs (a bit-packing loop over `payloads`), and — important —
  **rewrite null slots' words to zeros** because the rewrite's
  `validate_full` will grow a canonical-padding check ("unused inline view
  bytes … remain a validate_full-tier concern") and null entries with a
  garbage word are legal today but not canonical. Cheap to do in the same
  pass.
- The int32 constraint: view offsets are `Int32`, so any single referenced
  buffer must be < 2 GiB. Files ≥ 2 GiB: chunk-owned input regions (the
  production plan already commits to <2 GiB chunk buffers) — the buffers
  list is variadic, one entry per chunk region is legal Arrow, and our
  payload's `off` maps to `(chunk_index, chunk_relative_offset)` with one
  division per long entry. Design the converter against a `Vector` of
  regions from day one; the sub-2 GiB case is just length-1.
- Semantic validation cost: `validate_semantic` walks every long entry to
  check bounds + prefix. It is cached (`semachecked`), and it is exactly the
  contract our parse already guarantees; running it once at export is the
  honest price. For an internal, trusted handoff (CSV → Arrow.jl in-process
  table) it can be skipped by construction — the rewrite separates the
  stages so an adapter may.

### Typed columns → primitive `ArrayData`

- `Vector{T}` (missing-free): `heapregion(v)` as the `DATA` slice, empty
  validity, `nullcount=0`. **Zero copies, zero passes.** Types map to
  `IntType(bits, signed)`, `FloatType(bits)`, `BoolType` (see below),
  `DateType(DAY)`, `TimestampType(unit, nothing)`, `TimeType(unit, bits)`.
- `Vector{Union{T,Missing}}` (union-direct finals): Julia's layout is data
  words + a **tag byte per element**; Arrow wants data words + a **validity
  bitmap**. The data region of a Julia union array is not reachable through
  public API as a `Vector{T}`, so v2's honest answer for 1.0 is a copy of the
  data words plus a tag→bitmap pass — O(n), memcpy-class, and only for
  missing-bearing typed columns. The endgame is unchanged from v1 (**P3**):
  a driver mode whose missing-bearing typed finals are `(data::Vector{T},
  bitmap::Vector{UInt64})` pairs, written directly by the parse — the
  union-direct work already proved write-direct storage is free — which
  makes this column class zero-copy too and makes `Vector{Union}` a *view*
  we build only for Julia consumers.
- `Bool`: Arrow bools are bit-packed; Julia's `Vector{Bool}` is bytes. Copy
  (bit-pack) — unavoidable, tiny.
- `Vector{Missing}` (all-missing column): `NullType`, `len`, no buffers.

### Pooled columns → `DictionaryType(IntType(32,false), ViewType(true), false)`

- Indices: our `refs::Vector{UInt32}` with `0 = missing`; Arrow indices are
  0-based with nulls in validity. One O(n) pass: `idx = ref - 1`, and null
  rows → validity bit off (the pass can also produce the bitmap). If the
  column is missing-free (`npresent == ndata`), the pass is a plain
  subtract; if a driver flag someday keeps refs 0-based internally, it is
  zero-copy.
- Dictionary: the levels are a `CompactStringVector` → the Utf8View mapping
  above, as `ArrayData.dictionary`. The rewrite's IPC writer does
  replacement-on-change dictionary batches keyed by **pool object
  identity**; a CSV table has one immutable pool per column, so one
  dictionary batch each.

### The table → `Schema` + `RecordBatch`

`Field(name, type, nullable, nothing, ())` per column, `nullable = Missing <:
eltype`; `Schema(fields)`; `RecordBatch` of the `ArrayData`s. The rewrite's
`RecordBatchSource` pull interface then feeds either the C stream export
(`export_stream!`) or the IPC writer.

## Ownership and lifetime — the rewrite's model, restated for us

- In-process: nothing to do. Every `BufferSlice` we hand out has our
  Vector/mmap as its region `root`; the `ArrayData` keeps the slices; the
  table stays alive as long as anyone holds the arrays. This is the whole
  memory model ("validity is reachability").
- C-data export: `to_c_data(field, arraydata)` (or `export_stream!` for the
  batch stream) registers an `ExportedRoot` whose `roots` vector holds our
  `ArrayData`s (hence our regions, hence our Vectors and the mmap) until the
  consumer's release callback runs; release is exactly-once; `reap!` frees
  the mallocs. **We never write a release callback.** Constraint we inherit
  and must state in our docs: the export contract requires callbacks on
  Julia-attached threads and non-overlapping calls per stream (the
  rewrite's stated v1 execution contract); a marshaling worker is the
  rewrite's production item, not ours.
- The rewrite forbids **eager unmap** by design (no revocation). For us that
  means a CSV.File-derived Arrow table pins the input mmap until every
  consumer drops it — same as CompactString views today, and now the same
  rule across the ecosystem.
- Immutable-borrow rule: while a region is in use we must not resize or
  mutate the backing Vector — our columns are frozen after `parse`, so this
  holds by construction; document it for `extra`.

## Interfaces to build (in CSV.jl), and what they cost

1. **`CSV.arrowdata(f::CSV.File) -> (Schema, RecordBatch)`** — the
   converter: per-column `ArrayData` construction as above; the only
   non-trivial passes are (a) long-entry `b`-word rewrite + null-word zeroing
   for string columns, (b) tag→bitmap (+ data copy, until P3) for union
   columns, (c) refs−1 for pooled columns, (d) bit-pack for Bool. Every
   other buffer is `heapregion` of something we already have.
2. **`Arrow.write(io, CSV.File)` fast path** — the rewrite's writer takes
   `RecordBatch`; with (1) the write skips all materialization. Nonzero
   element offsets are refused by the writer — we never produce them.
3. **C-data / C-stream** — `to_c_data` / `export_stream!` on the batch:
   pyarrow, DuckDB, polars ingest zero-copy (`vu` format string; the
   trailing int64 variadic-buffer-lengths buffer is the adapter's job).
4. **Regions ≥ 2 GiB** — chunk-owned regions, `bufidx = chunk`.

## Phases

- **P1 — converter core** (S, 2–3 days once the rewrite's core is a
  package/loadable dep): (1)+(2), exhaustive equality differential (every
  cell via `getvalue(field, arraydata, i)` == every cell via our getindex,
  incl. escaped/extra-backed, missing-bearing, pooled), `validate_full`
  clean on every produced batch, an mmap-backed 1 GiB test and a synthetic
  ≥ 2 GiB chunked-regions test.
- **P2 — C-data round-trip** (M): export via `to_c_data`, import back via
  `from_c_data`, equality; then a foreign consumer (DuckDB's Julia client, or
  a small C harness) reading it; lifetime under GC pressure and release
  ordering.
- **P3 — validity-native typed finals in the driver** (M, optional, gated
  on demand): erases the last data copy for missing-bearing typed columns.
- **P4 — coordination** (ongoing): the rewrite's own P4 (production trim,
  incremental IO framer, parallel writer, marshaling worker) is theirs; our
  ask list to the rewrite is short — keep `ViewType` inline/long word layout
  and the `[validity, views, data…]` role order stable (it's the spec, so
  yes), keep `heapregion` accepting arbitrary isbits Vectors, and expose
  the null-word canonicalization rule once `validate_full` grows it so we
  match it exactly.

## What changed from v1

v1 assumed released Arrow.jl 2.x: typed `ArrowVector` wrappers, a possible
Arrow.jl PR to wrap external Utf8View buffers, hand-written release
callbacks pinning `Ref`s, and an int32/`utf8`-copy fallback. Against the
rewrite: (1) `ArrayData` + `heapregion` + `BufferSlice` **are** the
external-buffer wrapping API — no upstream PR needed for the in-memory
path; (2) ownership on export is the `ExportedRoot` registry's, not ours;
(3) `ViewType` is a registry layout with validation, IPC and C-data all
mapped, so the "does Arrow.jl support Utf8View on the wrap side" risk is
gone; (4) the remaining real work is the four O(n) passes listed above and
the ≥ 2 GiB chunk-region story — the same as v1's core, now with exact
targets.
