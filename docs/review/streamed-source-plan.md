# StreamedSource — bounded-memory streaming for Rows / Chunks / File-over-IO

Status: design (2026-08-19), not implemented. Written in response to the review
comment on RESPONSES.md #24 item 6: "sketch out the full plan for StreamedSource
and make sure it will work for the Rows/Chunks cases efficiently and
performantly." Borrows the outer loop from @drvi's ChunkedBase (see
`drvi-comparison.md` §3c) and keeps the kernel's inner loop unchanged.

## What today's kernel needs, and what it can't do

The kernel is index-then-columnar over a **whole buffer**: `chunkplan` computes
per-range quote parity over the entire file, `File` mmaps paths, and every
other source (`IO`, `Cmd`, gzip, URL) is *read fully into memory* first.
Consequences the audit flagged: a larger-than-RAM `.gz` via `Rows`/`Chunks` no
longer works (0.10 spooled to a temp file and mmapped), and `Rows` over an `IO`
holds the entire input even though it yields one row at a time.

Two facts make streaming cheap to add without touching the kernel's inner loop:

1. `chunkplan` + `indexone!` + the column loops already operate on an arbitrary
   byte range `[datastart, len]` of a buffer, and their only global input is the
   quote state at the range start — which streaming knows exactly (see below).
2. `KernelExamples.Batches` (what `CSV.Chunks` wraps) already parses **one chunk
   at a time** through `parsebatch` — the "batch = a ChunkIndex over a buffer" seam
   is the streaming unit.

## The model: a coordinator that keeps a rolling window of row-aligned buffers

```
                 ┌────────────────────────────────────────────────────────┐
  IO / gzip ───▶ │ Reader task: fill buffer k (bufsize, e.g. 8–64 MiB)      │
                 │   • carry the tail of buffer k-1 (bytes after its last   │
                 │     structural row end) to the FRONT of buffer k          │
                 │   • run chunkplan on buffer k with the KNOWN entry quote  │
                 │     state (false at a row start; the carried tail starts  │
                 │     at a row start by construction)                       │
                 │   • find the last structural row end in buffer k          │
                 │     (quote-aware scan backwards from the end — or forward │
                 │     from the last chunk boundary; ≤ one range of work)    │
                 │   • hand ⟨buffer k, chunks of k, at_eof⟩ to consumers     │
                 └──────────────┬─────────────────────────────────────────┘
                                │ Channel (depth = nbuffers, e.g. 2–3)
                 ┌──────────────▼─────────────────────────────────────────┐
                 │ Consumers: File-over-IO / Chunks / Rows                  │
                 │   parse buffer k's chunks with the ordinary kernel loops │
                 │   (index inside the task, columnar parse, promotion,     │
                 │   problems) — exactly `parsebatch` today                 │
                 └──────────────┬─────────────────────────────────────────┘
                                │ buffer k released back to the pool when its
                                │ consumers are done (TaskCounter, ChunkedBase style)
```

Key properties:

* **Bounded memory:** `nbuffers × bufsize` plus the consumer's own output. Rows
  and Chunks never hold more than that; File-over-IO accumulates its columns
  (unavoidable — it returns them) but not the input.
* **Row alignment by construction:** every buffer starts at a row start (the
  carried tail is the incomplete last row of the previous buffer) and ends at
  the last complete row; a row longer than `bufsize` grows the buffer for that
  one row (ChunkedBase errors here; we grow — the kernel already handles a
  chunk that is one giant row, up to the tape's 1 GiB single-row bound).
* **Quote state is exact:** each buffer begins at a row start ⇒ entry state
  `false`; `chunkplan`'s parity composition inside the buffer is unchanged.
  The one subtlety: the reader must find the last *structural* row end of a
  buffer, i.e. honor quotes — a backward scan is ambiguous, so scan **forward
  from the last chunk boundary chunkplan already computed** (that boundary's
  quote state is known); cost ≤ one range.
* **Same code paths:** consumers call `K.indexone!`/`fusedchunk!`/`directwave!`
  per buffer exactly as `parsebatch` does; problems carry global row bases
  (buffer offset + chunk row base); `unclosedquote` is reported only at EOF.

## Per-front-door behavior

**`CSV.Chunks(io)`** — the natural fit: one buffer = one or more batches. Schema
stability across batches (today's whole-index prepass) becomes a *first-buffer
prepass* plus a promotion protocol: seed types from the first buffer (or a
user `types=`); a later buffer that would promote a column emits the batch
under the widened type and records a `:schema_change` problem — or, with
`stable=true`, errors. (This is the trade every streaming reader makes;
ChunkedCSV sidesteps it by refusing inference.) Batches yield in order.

**`CSV.Rows(io)`** — Rows already parses lazily by row over an index (no
columns). It becomes an iterator over buffers: index buffer k, iterate its
rows (row views into buffer k), when exhausted release k and take k+1. Memory
= the window; per-row cost unchanged; typed access via the same `typedvalue`.
`reusebuffer` stays a documented no-op.

**`CSV.File(io)` / gzip / URL / Cmd** — read through the same coordinator
(parallel per-buffer parse), appending each buffer's parsed columns; the input
never lives whole in RAM. For gzip this is the 0.10 tempfile+mmap capability
without the tempfile. (Files on disk keep the mmap path — it is faster.)

## Threading and ordering

* Reader task = serial IO + tail-shift + `chunkplan` (a parity popcount, memory
  speed) + last-row-end scan. It is a small fraction of the parse work, so one
  reader feeds many consumers (ChunkedBase's design point holds).
* Consumers = a pool of `Threads.@spawn` tasks pulling buffers from the channel;
  inside a buffer the existing per-chunk parallelism applies (nested spawn is
  fine; or, simpler, one task per buffer with the buffer's chunks parsed by
  the ordinary parallel driver).
* Ordering: Chunks/Rows need in-order delivery — a `PayloadOrderer`-style
  reorder buffer keyed by buffer index (bounded by `nbuffers`).
* Backpressure: the reader blocks when `nbuffers` are outstanding; consumers
  release buffers explicitly (a counter per buffer of chunks parsed).

## Sizes and defaults

`bufsize` 16 MiB (≥ chunkbytes × 4 so a buffer still splits into parallel
chunks); `nbuffers` 3 (one filling, one parsing, one draining). Rows: `bufsize`
4 MiB is plenty. Both keyword-tunable.

## Error handling

EOF inside a quoted field: recorded as `:unclosed_quote` at EOF exactly as
today (the last buffer's `at_eof` flag lets `finishscan!` decide). Row longer
than a buffer: grow (log a `:long_row_buffer_growth` info problem if it
exceeds 4×). IO errors propagate through the channel with the buffer index.

## What it costs / buys

* Costs: the schema-stability protocol for Chunks (design decision above),
  ~300–400 lines (coordinator, buffer pool, orderer, three front-door
  adapters), and a differential test battery (stream vs whole-buffer parse
  must be byte-identical for every source, size, and buffer geometry).
* Buys: bounded memory for every non-file source; larger-than-RAM gzip and
  pipes; Rows/Chunks that actually stream; and it removes the "read the whole
  IO first" caveat from the docs. The kernel's inner loop, value layer, and
  column builders are untouched.

## Suggested order

1. Coordinator + buffer pool + orderer over a plain `IO`, with `Chunks` as the
   first consumer (it already parses per chunk); differential vs today.
2. `Rows` over the coordinator.
3. gzip (a `GzipDecompressorStream` is just an `IO`) and URL/Cmd through it.
4. `File(io)` accumulating columns; measure vs read-all-then-parse.
5. Chunks schema-stability protocol + `stable=` keyword.
