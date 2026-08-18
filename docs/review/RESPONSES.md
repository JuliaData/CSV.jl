# CSV.jl 1.0 review — responses to the 27 items

Source: `/Users/jacob.quinn/.julia/dev/CSV_Review.md`. Worked 2026-08-18 on
branch `kernel-proveout` (13 commits, `891df6a..b11d5dd`) and Tables.jl
`jq/scan` (`ee9df1e`, local — PR #380's head is still `d1fbb6e`; push when
ready). Full test battery after all changes: **1,676,486 assertions green**
(kernel 54.5k, values 1.62M, api 691, write 293, scan 306, legacy 1.4k) on
Julia 1.12.6; the package also loads and hashes correctly on 1.13.0-rc1.

Companion documents in this directory:
`drvi-comparison.md` (#4), `parsers-downstream.md` (#10/#17),
`csv-issues-audit.md` (#24), `floatsweep-2026-08-18.txt` (#11), and the
walkthrough page for #5 at
https://claude.ai/code/artifact/c8d6a8f9-ca1b-4ce6-82ad-f5a708281cad.

Legend: ✅ done · 🔎 answered · 📐 designed/recommended, not built · ⏭ deferred by your ordering.

---

## 1. 🔎 What the quote-toggle semantics note means

**The claim:** the structural scanner treats *every* `"` byte as flipping the in-quote state, regardless of where it sits in a field. That is what makes quote state *composable*: a byte range's effect on the state is just its quote count mod 2, so a range's true entry state is the XOR of the parities of all ranges before it (the parallel-chunking trick — see #5's page). RFC `""` doubling is two toggles, parity-neutral, so it costs nothing.

**The consequence:** a bare quote in the middle of an *unquoted* field opens a quoted region for the structural layer. 0.10's value-level parser only honored a quote at *field start*, so on such malformed inputs the two designs can split rows differently. Concrete example — delimiter `,`, the row

```
1,ab"cd,e"f,g
```

- kernel: the `"` after `ab` opens a quoted region that closes at the `"` after `e`; the comma between `cd` and `e` is protected; the row has **3 fields**: `1`, `ab"cd,e"f`, `g`. The value layer then notices an unquoted span containing the delimiter and records a problem (`_delimclash`), keeping the bytes exactly.
- 0.10: the field does not start with `"`, so the quotes are literal; **4 fields**: `1`, `ab"cd`, `e"f`, `g`.

Where it matters most is a quoted **newline** after a bare quote: `x"y\nz` — the kernel keeps `y⏎z…` inside one field until the next `"`; 0.10 ends the row at `⏎`. On well-formed RFC 4180 data the two never differ. The trade is deliberate: composable state ⇒ deterministic parallel chunking with no speculation. (The issues audit's item 4 — bare quotes in *skipped prefix rows* still yielding 0 rows — is the same rule biting where it shouldn't; recommended fix: walk skipped prefix rows quote-blind and move `datastart` past them, exactly as numbered headers already do.)

## 2. 🔎 Regex delimiters/quotes

They were never a feature. In 0.10 `delim::Union{Nothing, UInt8, Char, String}`; in Parsers 2 `Regex` appears only inside `_match`, an internal option-*consistency* helper (checks that delim/quote/escape don't collide) — never as a parse-time matcher. What 0.10 *did* support is regex-**keyed** per-column dicts (`types=Dict(r"^x_" => Float64)`, likewise `pool`/`dateformat`); the rewrite honors those (verified: `types=Dict(r"^x_"=>Float64)` types both `x_a`,`x_b` and leaves `y` inferred). Nothing to build.

## 3. ✅ `cellcontent` return contract

Docstring on the function now states the `(cpos, clen, escaped, disposition)` contract with a table of worked examples (`42`, `"a,b"`, escaped, outer blanks, quoted-empty vs empty, sentinels inside quotes, unterminated, bytes-after-close, and the `stripwhitespace` variants). Commit `39b8998`.

## 4. 🔎 Comparison with @drvi's NewlineLexers / ChunkedBase / ChunkedCSV

Full report: `docs/review/drvi-comparison.md` (file:line-cited both sides). The short version for the conversation:

- **What they built.** NewlineLexers finds *only newlines* (no delimiters) with SIMD.jl `Vec{64}` + a `<64 x i8> icmp→i64` llvmcall; prefix-XOR is a shift ladder by default and PCLMULQDQ only behind `NEWLINELEXERS_NATIVE` (gated off because of julia#49653). Its `""`-doubling kernel uses simdjson-style even/odd run tricks and carries a `prev_escaped` ambiguity resolved on the *next* buffer's first byte. ChunkedBase is a coordinator: serial IO+lex, tail-shift refill, worker pool over a Channel, strict two-buffer pipeline with `TaskCounter` backpressure, out-of-order `consume!` (ordering opt-in), hard errors for row>buffer and EOF-in-quote. ChunkedCSV parses row-major with one `Parsers.xparse` per cell through an enum switch, `PosLen31` strings, per-row `RowStatus`, no type inference (schema or String); no published perf numbers.
- **Where ours differs and why.** No serial lexer at all (parity popcount + XOR scan replaces it), deterministic chunking, delimiters *are* emitted (4 B/field on the tape is nearly free inside the SIMD loop and is what unlocks column-at-a-time monomorphic parsing with O(1) `fieldspan`), column-local promotion, direct-to-final writes, zero-copy CompactString views, CRLF pre-paired structurally, problems-as-data.
- **What they can do that we can't (yet).** Bounded-memory streaming over any IO/gzip; parallel parse for *every* dialect (we run backslash-escape and asymmetric-quote dialects single-threaded and even single-chunk); SIMD for backslash escapes; per-row status; a deliberately simpler no-inference contract.
- **Borrow list.** (a) Their coordinator loop as the template for our `StreamSource`/`CSV.Chunks`-over-IO: row-align stream buffers via tail-shift, then run `chunkplan` + parallel index/parse unchanged per buffer, add an `at_eof` flag to `finishscan!`, keep their TaskCounter backpressure. (b) For non-parity-clean dialects, reuse our comment-dialect serial row-walk planner so index+parse still parallelize, then port their escaped-quote mask into `blockmasks`. (c) Verify/gate the unconditional PCLMULQDQ/PMULL llvmcalls under `JULIA_CPU_TARGET`/PackageCompiler (they gate; we don't). (d) A "skip to next structural byte" specials mask for the scalar/tail paths. (e) Add ChunkedCSV in schema-provided mode to `bench/bench.jl`.
- **Talking points.** Index-then-columnar exists because emitting delimiters is nearly free and buys monomorphic parsing; parity composition replaces the serial lexer using a fact their kernel already relies on (RFC doubling is parity-neutral); the right hybrid for streaming is their outer loop + our inner loop per buffer, with schema stability following their `types=` contract.

## 5. ✅ HTML walkthrough of parallel indexing

https://claude.ai/code/artifact/c8d6a8f9-ca1b-4ce6-82ad-f5a708281cad — a 38-byte example with 12-byte ranges: per-range parity table, the XOR-scan figure, the state-seeded row-start scan (with the wrong answers the naive scan gives, drawn), the row-longer-than-a-range collapse example, why parity composes and which dialects break it, and where `chunkplan` sits in the pipeline. Every number was computed by `quoteparity`/`nextrowstart`/`chunkplan` on the shown bytes.

## 6. 🔎 TypedColumn vs UnionColumn

Not the same thing: two storage layouts so the FINAL column is a plain Base vector with zero copies **either way**. `TypedColumn{T}` = values + presence bytes → finalizes to a raw `Vector{T}` when nothing is missing. `UnionColumn{T}` = a `Vector{Union{T,Missing}}` written in place (Base's own bitsunion layout: data store + tag store, the same two stores as values+present) → finalizes to that vector. Converting one layout to the other after the parse costs a full extra pass — measured at 120–150% of a whole 20 MiB parse, because bitsunion stores have no memcpy path — which is the entire reason for two layouts. Chosen per column by whether the sample showed missings. Cross-referenced in the code (`6c83158`).

## 7. ✅ CompactString-independent split

`src/compactstring.jl` now holds the payload/accessors, the `CompactString` `AbstractString` interface (`==`, `cmp`, `hash`, `String`, write/print), and `CompactStringVector` + `materialize` — **Base-only, no kernel references** — moved verbatim; the quote/escape unescape helpers, `StringColumn` staging, and the pooling tables stay in `core.jl` as the CSV layer over it. This is the seed unit for #22. Commit `388a7cf`; battery unchanged.

## 8. 🔎/✅ Pooling: is it worth it? where should it live?

**Measurements** (39 MiB, 1M rows, 5 cols: id, 10-level region, 400-level cat, price, high-cardinality string; 8 threads):
- parse with 0.10's default policy `(0.2, 500)`: **22.4 ms**; with `pool=false`: **13.6 ms** → **default pooling costs +65% parse time**.
- kernel-integrated pooling of the two poolable columns: ~8.8 ms; a naïve single-threaded API-layer pass (`Dict{CompactString,UInt32}` → `PooledArray`): 31 ms; parallel-per-column ≈ 15 ms. So the API layer is **2–3× slower for the pooling step only**, and only when pooling is requested.
- code: the kernel-integrated version is ~530 lines (~12% of core.jl) and the most intricate concurrency surface in the kernel — `InlineTable` open addressing, `ViewKey` hashing, `PoolSegment` with per-column atomic abort/degrade mid-chunk, the interning parse loop, hash-sampling pre-skip with a birthday bound, and the pooled merge. An API-layer pass is ~120 lines.
- what others do: polars, pyarrow.csv (`auto_dict_encode=False`), pandas, DuckDB, data.table all make dictionary/categorical encoding **opt-in**; pyarrow is the only one that does it inside the reader.

**Done:** `DEFAULT_POOL = false` (`70d1bc3`); `pool=(0.2, 500)` restores 0.10 verbatim, `pool=true` pools everything; docs updated; the value-differential vs 0.10 still holds.

**Recommendation:** with pooling opt-in, move it to a finalize-time API pass over the `CompactString` column (allocation-free hashing already exists; parallel per column) and delete the parse-time interning + pre-skip machinery. Cost when opted in: roughly +7–20 ms on this file; benefit: ~400 fewer lines in the kernel and no atomic-abort concurrency surface. I did not rip it out today because it touches many pinned pool tests and the CLI rounds invested in it — say the word and it's a contained change. If you'd rather keep the fast path, keep it as-is behind opt-in; the complexity is at least no longer paid by default.

## 9. 📐 Merge ParsedTable & File; drop CSVKernel.parse

Agreed, and it should happen *inside* #20's flatten: `File` becomes the struct that `ParsedTable` is now (names, columns, nrows, problems, dropped) plus the source name and lookup; the driver's `parse` becomes the internal `_parse!` that returns a `File`; `CSVApi.File(...)` post-processing (`_pooledarrays`, `_downcast`, `_materializestrings`) folds into the same function. Doing it before the flatten means threading types across three inner modules and then undoing that; doing it with the flatten is one pass. Everything else in the plan is compatible with it (KernelScan.execute already returns the table shape File will hold).

## 10. 🔎 Parsers.jl downstream users

Full map with per-package table: `docs/review/parsers-downstream.md`. Headline: **40 registry dependents; 12 stale/never-call; 28 real (7 yours, 21 third-party). 15 of 28 (54%) use only `parse`/`tryparse`** (String/SubString/byte vectors; 4 also use the positional byte-span form or `dateformat=`). Third-party packages the delim/quote/sentinel removal actually breaks: **ChunkedCSV** (full Options + PosLen31 + Result codes + `checkdelim!`/`getstring` + custom typeparser — will pin to 2), **ARFFFiles** (active, used by OpenML), **PowerFlowData** (used by PowerSystems.jl), **OSMToolset** (a two-line shim), **Schemata** (dead since 2021), plus **FixedPointDecimals** (hooks the deep `AbstractConf/typeparser/parsedigits/xparse2` seam; DuckDB depends on it). Yours: JSON (`xparse2` prefix parse), JSON3 + MySQL (7-arg `typeparser` + `OPTIONS`), WeakRefStrings (`PosLen` type only), InlineStrings ext (only for CSV), JSON2 (deprecated).

## 11. ✅/🔎 Float parsing sharp edges vs Base / Parsers 2 / fast_float

New `bench/floatsweep.jl` + `bench/ffbench.cpp` (fast_float, the industry reference, on identical corpus bytes with a checksum cross-check). 16 shapes; table in `docs/review/floatsweep-2026-08-18.txt`.

- **One real sharp edge, fixed:** odd 16-digit integers just above 2^53 (and any exact tie under exponents −4..23) cost **2,391 ns/value — 188× slower than fast_float, 70× slower than Base** — because Eisel–Lemire detected the tie and delegated to the 800-digit tier. For −4 ≤ q ≤ 23 the 128-bit product is exact (Mushtak & Lemire, *Fast Number Parsing Without Fallback*), so the tie is resolved in-line by clearing the low bit: **20 ns now**, 700k tie values bit-exact vs Base, pinned. (`179731d`)
- Parity or better vs fast_float: short `x.y` (1.08×), negative short (1.2×), specials (1.1×), 20-digit mantissa (1.15×), halfway (now 1.6×).
- Remaining gaps: **1.6–1.8×** on 17-digit / exponent / subnormal shapes (our `_decompose` is a per-digit byte loop past the 8+8 fast path; fast_float reads 8 digits at a time) and **2.7× / 5.3×** on 100/400-digit mantissas (fast_float falls back to a limb comparison, we walk an HPD). I tried an 8-digit block step inside the shared loop and reverted it — it slowed leading-zero and long-tail shapes; the right fix is a *phase-structured* decompose (integer run / fraction run / exponent as separate SWAR loops) — a contained follow-up worth doing before Parsers 3.0.
- Side finding: **Parsers 2 `xparse` throws `InexactError` on a 400-digit mantissa.**
- vs Base and Parsers 2 we are faster on every shape except 40/400-digit mantissas vs Base (0.7×).

## 12. ✅ parsefloat64 docstring placement

It had drifted onto `_eqmask8` after SWAR helpers were inserted between them; it now sits on the definition (and a `%%` typo is gone). `39b8998`.

## 13. ✅ parseuuid, 8 chars at a time

Yes: four 8-hex-char word conversions (the 8-4-4-4-12 groups gather via low-32-bit combines from loads at 0, 9|14, 19|24, 28 — every load inside the span), through a branch-free `_hex8` (lowercase, borrow-free digit/`a-f` lane tests, nibble = low nibble + 9·isalpha, three fold steps). **102 → 6.3 ns/value** (Base.tryparse: 117). Oracle: 200k random either-case UUIDs plus every single-byte corruption at every position. `0dacf81`.

## 14. ✅ degroup! word-at-a-time

Two changes: the mark pre-scan every cell of a grouped column pays is a word eq-mask (`_hasbyte`, ~1 ns — unmarked cells now cost `parseint64` + 1 ns), and Int64 grouped cells parse through `parsegroupedint64`, which gathers each digit run straight from the loaded word (no scratch, no compaction); the reference `degroup!`+`parseint64` stays for >8-digit runs / >19 digits / buffer-end guards and as the oracle (800k adversarial spellings identical, incl. typemin flush against a buffer end; allocation-free via the column loop's scratch). **15.9 → 15.2 ns** — honest: ~5 ns per group is the floor of this approach; getting to ~8 ns needs shape special-casing (fixed 3-digit groups). Not pursued.

## 15. ✅ daysfromcivil vs Dates

Exhaustively identical to `Dates.value(Date(y,m,d))` — every day of years −9999..9999 (7.3M days) plus extremes (±100k years, year 2^31-ish). Then I **adopted Dates' own `totaldays` formula verbatim** (shifted-month table; no Dates dependency) — same function, simpler, ~35% faster than the Hinnant form; the test now pins every day of −1000..3000 plus extremes. `6c83158`.

## 16. 🔎 `_readnum` / `_iso_ymd` byte-by-byte

The kernel's real ISO path was already fast: `parseiso10` = **2.5 ns/date**, `parsevalue(Date)` = 4.3 ns end-to-end (vs xparse 28, Base 49); `_iso_ymd` = 1.9 ns — branch-free byte gathers off one cache line, which SWAR can't beat. The **17.9 ns in the bench table was the format *interpreter*** (`parsecivil`, custom `dateformat`s), mislabeled as "date ISO"; the table now shows the real column path (3.0 / 33 / 55) with the interpreter as its own row. A SWAR `_readnum` was tried and reverted: the interpreter's cost is op-list dispatch, not the digit loop; if custom formats ever matter, the fix is compiling a `DatePattern` to a specialized function.

## 17. 📐 Parsers.jl 3.0 plan — what today adds

- Downstream research: done (#10). Migration difficulty is concentrated in six third-party packages; the extension seam question (`supportedtype`/`typeparser`) has exactly two live third-party customers (FixedPointDecimals, ChunkedCSV).
- What 3.0 should keep, ranked by dependents: (1) `parse`/`tryparse(T, s)` on `AbstractString`/`AbstractVector{UInt8}` incl. `SubString` and views, `nothing` on failure, whitespace-tolerant; (2) the public byte-span `parse/tryparse(T, buf, start, stop)` (you already want this); (3) a public delimiter-free **prefix parse** primitive returning `(value, next_pos, status)` — this is what JSON/JSON3/MySQL actually need and keeps number parsing centralized; (4) `dateformat` pass-through for Date/DateTime/Time; (5) `PosLen` (or move it to WeakRefStrings); (6) a small documented `supportedtype`+`typeparser` seam, or accept that FPD/ChunkedCSV pin to 2.
- Non-base-10 ints: Base supports `parse(Int, s; base=2..62)`; floats only base 10 (plus hex floats `0x1.8p3` via `parse(Float64, "0x1p3")`). Plan: `parseint(T, buf, i, j; base)` for 2..62 with the SWAR path only for base 10 (and 16 — the `_hex8` from #13 is exactly the base-16 gather), scalar for the rest; hex floats via a small dedicated path.
- Whitespace layer: `Base.parse` strips leading/trailing whitespace; a `strip` prefix pass in the Base-parity layer, exactly as you describe.
- Dates story: verified `daysfromcivil ≡ Dates.totaldays` (#15) — the civil core can literally *be* the Dates implementation. Remaining format-token audit: Dates supports `y m u U d H I M S s e E p` (+ `Q`? no) — our `compilepattern` handles y/m/d/H/M/S/s/u/U and literals; **`I` (12-hour), `p` (AM/PM), `e`/`E` (day names) are not implemented** — needed before "Dates drops its machinery".
- Split-by-file and the benchmark dashboard: `values.jl` is 1.6k lines and splits naturally into ints/floats/civil/big/uuid/span; the dashboard should be built from `bench/valuebench.jl` + `bench/floatsweep.jl` (both produce ns/value tables today). Not started.
- Perf debt to clear before "release candidate": the phase-structured `_decompose` (#11) and a limb-comparison long-mantissa fallback.

## 18. 🔎 examples.jl — remove?

Its name is a relic; its contents are live infrastructure: the Tables.jl glue for `ParsedTable` and the `Batches`/`Rows`/`RowView`/`typedvalue`/`schemamissing` primitives that `api.jl`'s `Chunks`/`Rows` wrap (kernel tests still exercise `E.batches`). The two `demo()`s are gone (`39b8998`); the rest folds into the single module in #20.

## 19. ✅ sniff/Spec exposure

Removed from the public surface (`6bc3b26`): no export, no `CSV.sniff`/`CSV.Spec` bindings. The *machinery* stays because it implements `delim=nothing` auto-detection (0.10 parity); it is reachable as `CSVApi.sniff` for tests and promotable later on demand. (While there: the sniffer had a real bug — it sampled from byte 1, so a one-line preamble before `header=2` elected `' '` as the delimiter for the whole file. Fixed: sampling starts at the first row that matters. `176b9d3`.)

## 20. ⏭ Flatten the module structure

Deferred by your ordering (after the Parsers upstream). Plan for when it lands: one `CSV` module; `File` absorbs `ParsedTable` (#9); `examples.jl`'s primitives merge into the Chunks/Rows code; `KernelWrite`'s namespace disappears (#26's engine is already self-contained); `KernelScan.execute` becomes a plain internal function; the only sub-namespace worth keeping is `CompactString`'s file (#7/#22). Note the tests reach in as `CSV.CSVKernel.*` / `CSVApi.*` — that's the biggest mechanical churn.

## 21. ✅ Misplaced comment block in api.jl

The "bulk materialization" perf note was an orphan (its code moved); it now sits on `_materializecolumn(String, …)` where that path lives, and the transpose section got its opening rule back. `39b8998`.

## 22. 🔎/📐 CompactString shared with Arrow

Today's split (#7) makes `src/compactstring.jl` a Base-only unit that is exactly Arrow's StringView layout (16-byte payload: length + 4-byte prefix/inline bytes; second word = inline bytes 5–12 or an offset into a buffer). Options, in order of preference:
1. **A tiny standalone package (`CompactStrings.jl`)** owning the type + vector + Tables/hash/cmp interface; CSV and Arrow both depend on it. Cleanest for users (one type, `isa` works across the two, DataFrames sees one string type) and for the compat story (a 0.x→1.0 of the string type doesn't couple to CSV/Arrow releases). ~360 lines today; zero deps.
2. DataAPI: it defines *interfaces*, not concrete types; a concrete string type there would be out of character. No.
3. Extension of one on the other: forces an ordering (Arrow depends on CSV or vice-versa) and duplicates the type when only one is loaded — the exact `isa` mismatch we want to avoid. No.

Compat notes for (1): Arrow's StringView buffers are `Int32`-offset into a *set* of data buffers (buffer index + offset), while ours is a signed `Int64` offset into two implicit buffers (input / extra); the shared type should carry `(buffer id, offset)` or CSV should adopt Arrow's addressing — worth deciding before the package exists. `hash`/`==`/`cmp` semantics (String parity) and the `Missing` sentinel encoding are already the same as what Arrow needs.

## 23. ✅ demo() methods

Removed from both `api.jl` and `examples.jl`. `39b8998`.

## 24. 🔎/✅ Open issues/PRs vs the rewrite

Full audit: `docs/review/csv-issues-audit.md` (114 issues, 9 PRs; every non-obvious one was *executed* against the rewrite, 60+ probes). Counts: **40 fixed-by-rewrite, 26 obsolete, 24 still-open features, 12 docs/won't-fix, 8 still-open bugs, 4 needs-verification**; PRs: 3 superseded, 3 still-relevant (docs content, memhash), 3 stale. Roughly **92 of 123 are closable at release**.

Its Tier-A list, and status after today:
1. Julia 1.13 `memhash_seed` (#1164/PR #1165) — ✅ fixed (`706c4d6`, hash parity verified on 1.13-rc1; add a 1.13 CI lane).
2. `CSV.read(source, sink)` silently ignoring function sinks (#1030, #941) — ✅ fixed (`b11d5dd`, 0.10's `sink(CopiedColumns(...))` semantics).
3. `header=false` + `skipto=N` taking the column count from row 1 (data loss) — ✅ fixed (`176b9d3`).
4. Bare quote in skipped prefix rows → 0 rows (#1012, #1079, #1160) — 📐 open; recommended fix in #1 above (quote-blind prefix walk + shift `datastart`), small.
5. Writer slower than 0.10 (#947, #800, #751, #1087) — ✅ fixed and then some (#26).
6. Decisions to make now (cheap now, impossible after 1.0): `missingstring=nothing` no longer yields `""` for empties (#1107/#1120's escape hatch — a one-bit gate to restore); gzip/IO sources fully in RAM (0.10 spooled to tempfile+mmap; larger-than-RAM `.gz` via Rows/Chunks no longer works — #997/#988; ties into the StreamSource plan in #4); rename `Chunks(ntasks=)` (#1049); #1000 exhausted IO now silently returns an empty File.
7. Cheap wins: #506 URL sources via `Downloads` (~10 lines), #853 sniff `(' ', ignorerepeated=true)`, #990 select against raw-or-normalized names, keep raw bytes for malformed-quote cells (#1118/#522).

## 25. ✅ Scan API reshaped

**Tables.jl (`jq/scan` `ee9df1e`, local):** `Tables.Scan` (the request, plain data, `Scan(s; kw...)` copy-with-changes for residuals) + `Tables.scan(table, scan)` (the generic executor over any Tables.jl table = the reference semantics). `apply`/`finish`/the composed `scan(source, s)` are gone. Sources accept `scan=` as a keyword, push down what they can while materializing, and either reject an axis with an `ArgumentError` or hand a residual to `Tables.scan`. Tables suite 119/119.

**CSV (`176b9d3`):** `CSV.File(source; scan=Tables.Scan(...))` — through `_prepare` like every other keyword (header rows, skipto, comment, missingstring, sniffed delimiters compose), then `KernelScan.execute` over the prepared index and names. `select`/`drop`/`types`/`limit` are refused next to `scan=` (one request means one thing); the positional `read(source, scan)` is retired; `CSV.read(src, sink; scan=)` works. Differential: `File(src; scan) ≡ Tables.scan(K.parse(src), scan)` across scan shapes × chunk geometry × parallelism, plus the pinned divergence (masked inference sees only qualifying rows).

**"Filter at typed-value parse time":** the engine already never *value-parses* an excluded row for non-predicate columns; what remains post-hoc is that predicate columns are parsed for the whole file, then the mask is evaluated, then the rest parses masked. The fused version — evaluate the predicate per *chunk* right after that chunk's predicate columns parse, then parse the chunk's other columns under the chunk-local mask, all inside one `fusedchunk!` task — is a contained driver change (`fusedchunk!` takes the bound filter; `Tables.filtermask` over the chunk's staged columns) with no API impact. I left it as the designed follow-up; the measured cost of the current two-phase path is small (mask evaluation is 0.6 ms on 1M rows after the CLI-session barrier fix), so it's a locality/memory win rather than a wall-clock one.

## 26. ✅ Writer

Baseline confirmed *worse* than the audit: 75.7 MiB, 8-column table — new **1660 ms** (1T) / 649 ms (8T) vs 0.10 **409 ms** (1T). Causes: per-cell dynamic dispatch on `col[r]` over `AbstractVector[]`, `string(x)` per float/date cell, per-byte IOBuffer writes for quoted content, `reduce(vcat)` holding the output twice. Rewrite (`ad8cf81`): narrow tables (≤32 columns) render through a Tuple-typed row recursion (every column's element type static, direct row-major writes, no staging); wide tables render each column through a loop specialized on its element type into a staged (bytes, ends) buffer and gather rows once — one dynamic dispatch per column per block either way. Ints emit digits directly (all fixed widths, typemin-safe; BigInt via `string`), floats through `Ryu.writeshortest` at the write position (`decchar` handles `decimal=`), Date/DateTime hand-rendered ISO (three-digit `.sss` only when nonzero — pinned against `string(x)` over every millisecond and adversarial years), strings memcpy after a pointer scan, Bool appends the word, blocks stream to the sink (gzip through a compressor stream). **169 ms (1T) / 53 ms (8T)** — 2.1× faster than 0.10 single-threaded, 6.7× with 8 threads; **polars `write_csv` on the same table: 130 / 19 ms** (we're within 1.3× / 2.8×; was 13× / 34× before). Byte-identical to 0.10 and across thread counts. `KernelWrite` as a *module* goes with #20; the engine is already self-contained.

## 27. ✅ `$b` in `@spawn`

Not a bug as stated: `for`-loop iteration variables get a **fresh binding per iteration** (the manual: "as if the loop body were surrounded by a `let`"), so `@sync for b in 1:nb; @spawn … b …` is correct without `$` — pinned by a test that spawns 64 tasks whose later iterations finish *first*. `$x` exists for the *other* hazard: a captured variable that is **reassigned later**, which Julia lowers to a shared `Core.Box` (that is the mechanism behind this project's earlier `ci = chunks[k]` race). So the audit was done the rigorous way — lower every method containing `@spawn` and look for `Core.Box`. Exactly one offender: the `parse` driver boxed `chunks`/`indexed`/`rowbases0`/`final` (if/else double-assignment plus the limit-clip rebind). No observable race (every capturing task joins under `@sync` before the rebinds), but each was an `Any`-typed load inside task bodies and closures. Restructured to single assignment (`allchunks`/`nchall` pre-clip, a `_limitchunks` helper, per-branch `finaldirect`/`finalstaged`) — **zero boxes remain**, and a kernel test now lowers all spawn-containing methods and asserts that invariant. `891df6a`.

---

## Housekeeping

- `Project.toml` carries an **uncommitted diff from a CLI session** (adds Chairmarks, InlineStrings, Parsers as package deps — looks like a bench-time `Pkg.add`); I left it untouched. Verify before committing.
- Tables.jl `jq/scan` has one local commit (`ee9df1e`) not yet on PR #380.
- Not done from the list: #9/#20 (your ordering: after Parsers), #17's dashboard and file split, the pooling API-layer move (#8, awaiting your call), #22's package decision, the fused per-chunk filter (#25), and the audit's Tier-A item 4 (bare quotes in skipped rows).
