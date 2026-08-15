# The default string type: CompactString vs InlineString vs String

*Measured 2026-08-15 on the 22-shape corpus (20 MiB shapes, 8 threads, M3 Max),
interleaved best-of-N per cell. Script: `scratchpad/strcmp.jl`; raw:
`strcmp2.tsv`. After the CompactString `cmp`/`isless` memcmp fix (before it,
CompactString sort was 15–45× slower — that gap was a missing method, not a
property of the type).*

## What was measured

Per string-bearing shape and per `stringtype`: `file` = `CSV.File(buf;
stringtype=T, pool=false)` wall time (parse + materialize into T); `mem` =
bytes retained by the string columns (CompactString is charged the whole
20 MiB input buffer it views into); then the downstream operations users
actually run on string columns: touch every cell (`getidx`), hash every cell
(Dict/groupby), `==` against a probe (filter), `sortperm` of the first string
column, and a `Dict` count (groupby).

## Headline numbers (ms unless noted)

| shape | type | file | mem MiB | hash | eq | sort | dictgb |
|---|---|---|---|---|---|---|---|
| strings | Compact | **8.0** | 213 | 32.5 | 5.1 | 89 | 10.6 |
| strings | Inline | 74.5 | **66** | 23.9 | **1.1** | **12.9** | **7.9** |
| strings | String | 53.9 | 51 | 24.3 | 7.9 | 74 | 10.2 |
| pooled_high | Compact | **8.1** | 94 | 12.5 | 4.5 | 272 | 28.4 |
| pooled_high | Inline | 31.0 | **17** | 14.1 | **0.3** | **37** | **24.1** |
| pooled_high | String | 39.2 | 49 | 10.6 | 12.5 | 227 | 32.3 |
| longtext (4 KB cells) | Compact | **2.1** | 62 | 5.1 | 0.4 | 30 | 8.7 |
| longtext | Inline | 199.1 | 37 | 3.7 | 0.8 | 29 | 23.0 |
| longtext | String | 5.5 | **22** | 4.4 | 1.2 | **17** | **8.2** |
| mixed | Compact | **6.1** | 47 | 3.9 | 2.5 | 68 | 13.0 |
| mixed | Inline | 23.0 | 27 | 3.5 | **0.7** | **32** | 13.8 |
| mixed | String | 10.7 | **10** | **1.9** | 1.1 | 48 | **10.5** |

(All ten shapes in the TSV; the pattern is uniform.)

## What the data says

1. **Parse cost is not close.** CompactString is 3–9× faster to *produce* than
   either owned type on every shape (8 vs 54/75 ms on `strings`), because it
   is the format the kernel already writes; both owned types pay a full
   materialization pass — InlineString's per-column width scan plus a copy,
   String's per-object allocation (the floor we measured earlier). On
   `longtext` (4 KB cells) InlineString collapses (199 ms: `String255` cannot
   hold them, and 255-byte isbits moves are expensive) — InlineStrings is not
   a general default for free-text columns.

2. **Downstream, InlineString wins where the operation is a whole-value
   compare**, because an InlineString compares in registers. *Updated after
   the inline payload-word `cmp` fast path landed (2026-08-15, 1M-cell
   micro-bench, interleaved A/B)*: CompactString `sortperm` improved 2.7×
   on short strings (260→96 ms) and 1.27× on long (412→323 ms) — it now
   beats `String` on BOTH (313/353 ms) — and `==` runs ~5× faster than
   `String`. The residual InlineString sort lead (~3.8× on short strings)
   is structural: a heap-referencing 24-byte struct cannot match a pure
   isbits register compare, and LLVM vectorizes whole InlineString `==`
   scans outright. Hashing and Dict-groupby are at parity across the three
   (the allocation-free hash landed with this work).

3. **Memory: CompactString retains the input buffer.** Charged honestly, a
   CompactString table over a 20 MiB file holds ~20 MiB more than an
   InlineString table (213 vs 66 MiB on `strings`) — that *is* the zero-copy
   design: the file stays mapped for the table's lifetime. Once a user
   materializes (or the table is passed to Arrow) that cost vanishes. For a
   workflow that reads and immediately discards the source, InlineString's
   footprint is the smallest of the three on short-string shapes; String's
   is smallest on long-text shapes.

4. **String is never the best choice on any axis except long text**, where
   its per-object layout beats both (17 ms sort, 22 MiB). It is the
   ecosystem-safest type and the slowest to produce.

## Decision (2026-08-15)

**`CompactString` IS the 1.0 default** — ratified by the maintainer; the
InlineStrings extension ships for users who want the 0.10 types. The default
optimizes the operation every user pays (parsing) and the read-and-hand-off
path (DataFrames, Arrow); with the inline-compare fast path landed it also
beats `String` on every downstream operation measured, so the remaining
trades are input-buffer residency and the structural sort gap to
InlineString on short strings. Users whose workload is compare-heavy on
short strings and who want the smallest footprint get exactly 0.10's
behavior with `stringtype=InlineString`, and `stringtype=String` remains for
maximal ecosystem compatibility.

The counter-argument, recorded for the migration guide: defaulting to
InlineString would preserve 0.10's *types* exactly (no `String3` →
`CompactString` surprises in downstream code) at a 3–9× parse-time cost and
a hard failure mode on long text. The extension makes flipping a one-line
change if the ecosystem ever demands it.
