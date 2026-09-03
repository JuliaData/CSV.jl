# Measurement: owning string bytes vs viewing the input buffer

Question: what does it cost to stop `CompactString` columns from viewing the
input buffer (copy every >12-byte value into a column-owned arena at parse
time) — the change that removes the mmap-aliasing crash, the whole-file
retention, and the per-file 2 GiB offset cliff in one move?

Method: a throwaway copy of `src/core.jl` with a global switch in
`parsecolchunk!(::StringColumn)`; interleaved A/B in one process, best of 3×3,
8 threads, M-series laptop, other Julia jobs running (ratios are the signal,
not absolute ms). Files from the perf sweep's fixtures.

Three implementations were measured:

1. **naive staging** — every long cell through the escaped-cell staging tuple
   (`resize!` + three `push!`es per cell), flushed under the column lock.
2. **direct write** — bytes copied straight into the chunk's private `extra`
   (one `resize!` + `unsafe_copyto!`, payload emitted immediately), then the
   existing fold copies chunk extras into the final column's arena.
3. **direct write, no fold** — as (2) but the final column keeps the chunk
   extras as a *list* of buffers (Arrow `Utf8View`'s multi-buffer layout: the
   payload's buffer index selects the chunk). Timing probe only — values were
   not rebased, so only the parse time is meaningful.

| file | MiB | long-string bytes | view | (1) naive | (2) direct+fold | (3) multi-buffer |
|---|---:|---|---:|---:|---:|---:|
| s8_short (4 short string cols) | 19.6 | 0 % | 12.2 ms | 1.03× | 0.96× | 1.04× |
| q7_plain (mixed) | 42.9 | 0 % | 19.7 ms | 1.04× | 1.05× | 0.99× |
| m6_alllong (ragged, mostly numeric) | 51.9 | 0 % | 123 ms | 1.02× | 1.05× | 1.07× |
| sales_comment (7 cols, 1 % long) | 45.8 | 1 % | 25.4 ms | 1.12× | 1.01× | 1.01× |
| s8_unique (1 unique 13–20 B col) | 33.1 | 62 % | 14.6 ms | 2.21× | 1.32× | 1.12× |
| s8_mid (4 cols, all 13–14 B) | 56.3 | 100 % | 16.6 ms | 3.31× | 2.02× | 1.51× |
| big_100k_cells (100 KB cells) | 47.7 | 100 % | 5.5 ms | 3.18× | 2.51× | 2.10× |

Reading: on ordinary files (short strings inline, or numeric-dominated) the
cost is inside noise. The pathological all-long-string files pay 1.5× with
the multi-buffer arena; the remaining cost there is first-touch page faults
on fresh arena memory plus one memcpy — the mmap path pays neither because it
never allocates for string bytes. The 100 KB-cell file is the floor: pure
memcpy + faults of 48 MiB.

What the owned layout buys: no SIGBUS when the source file is truncated
while a table is alive; a table's memory is its columns, not the whole file
(`select=[:a]` on a 10 GiB file retains ~nothing); serialize/deepcopy/slices
carry string bytes, not the file; the 2 GiB limit becomes per-buffer, and the
multi-buffer form has no limit at all; `Vector{UInt8}` inputs stop aliasing
the table. It is also the layout CompactStrings.jl was scaffolded around
(buffer index + offset), and polars/DuckDB/Arrow all own their string bytes.
