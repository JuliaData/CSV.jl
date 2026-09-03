# CSV.jl 1.0 kernel — performance cliffs & footguns sweep

**Setup.** Worktree `kernel-proveout` (read-only), Julia 1.12.6, `-t8` on a 14-core M-series Mac with other agents' Julia jobs running concurrently. Absolute times are noisy; every cliff below is an **in-process ratio, best-of-3**, and each headline was confirmed in a second process (where two numbers are quoted they are the two runs). Baseline "0.10" = the frozen `LegacyCSV` oracle from the test env. Probe scripts and raw output: `$SP/pc/p01..p21*.jl` and matching `*.out` (`h.jl` = harness, `run.sh` = runner). Main fixture: `sales_1000000.csv` (45.6 MiB, 1M rows × 7 cols: Int, String, Float64, Int, Date, Bool, Union{Missing,String}).

Severity: **blocker** = wrong result / throws, or an advertised door unusable at the scale it exists for; **major** = ≥5× cliff users will hit in ordinary use, or a ≥2× regression vs 0.10; **minor** = ≤3×, or 0.10 parity, or an easy-to-avoid knob.

## 0. Summary

| # | cliff | measured | severity |
|---|---|---|---|
| C0 | any file > 2 GiB whose last row ends in a float column throws `InexactError: trunc(Int32, …)` | 2.05–2.2 GiB files: `File`, `select=numeric`, `stringtype=String`, `Chunks` all throw; 0.10 reads them | **blocker** |
| C1 | `CSV.Chunks` construction prepass: ~20 allocs/row, O(rows × chunks) | 45 MiB: 2.0 s vs 37 ms `File` (55×); 1 GiB: **59 s, 24.5 GiB allocated** (81×); 2 GiB: 163 s; `chunkbytes=1 KiB`: 57 s | **blocker** |
| C2 | `types=Dict(:c => Int32/Float32/UInt8/…)` (narrow types) | 12–14× slower than default, 3 allocs/cell; 6–10× slower than 0.10 | major |
| C3 | `stringtype=InlineString` picks `String255` for long strings | 200-B strings: 8.8 s vs 0.10 s default (86×), 11× slower than 0.10 | major |
| C4 | ragged/long rows: ~15 allocs per bad row, even with `maxproblems=0` | all-short 1M rows: 15 M allocs, 787 MiB (42× file), gc 52 % | major |
| C5 | `limit=N` indexes the whole file first | 1 GiB `limit=10`: 188 ms + **2.1 GiB allocated** (0.10: 0.27 ms) | major |
| C6 | `skipto` / `footerskip` re-walk the file serially after indexing | 1 GiB: 1.32 s / 1.69 s vs 0.73 s full parse | major |
| C7 | `File(::Vector{paths})` string chaining | 100 files: 670 ms vs 22 ms concat (30×), 6× slower than 0.10 | major |
| C8 | `RowWriter` per-row `IOBuffer` + `string(x)` | 61 allocs/row, 4.6× slower than 0.10 | major |
| C9 | writer Tuple-path compile per new ≤32-col schema | 0.3 s avg / up to 6.6 s CPU per schema; 32→33 cols discontinuity | major (TTFX) |
| C10 | `escapechar='\\'`, asymmetric quotes → scalar + single chunk; `comment=` → scalar | 7.4–7.7× vs default, **3× slower than 0.10**; comment 3.6× / 1.3× | major |
| C11 | `pool=true` on a unique column; per-column pooling overhead on wide tables | 17× unpooled (0.10 parity); 5000-col strings 5× | minor |
| C12 | ≥20 000 columns: throughput −23×, parallel slower than serial | 50k cols: 43 MiB/s; `parallel=false` faster (0.10 parity) | major footgun |
| C13 | `Rows(types=…)` dynamic per cell; `File` row iteration 6 allocs/row | 3.3× untyped Rows; row iteration = 0.10 parity | minor |
| C14 | writer `floatformat`/`dateformat`/`Time`/`Symbol` allocate per cell | 4–12 allocs/cell; `dateformat` 20× the default date path | minor |
| C15 | a single `CompactString` view pins the whole input buffer | `fm.a[1]` = 56 MiB retained; 1 GiB at scale | minor (docs) |
| C16 | `DataFrame(CSV.File(...))` copies to `Vector{CompactString}` (no `Base.copy` method) | 5× vs `Vector{String}`, column type lost | minor |
| C17 | downstream string ops fall to Base's char-iterating fallbacks | `startswith` 9.8×, `occursin(::String)` 9.9×, `length` 3.7×, `Vector{Union{Missing,String}}(col)` 43× | minor–major |
| C18 | `openquotechar`/`closequotechar` without `delim=` throws (bug) | `FieldError` in `_probedelim` | blocker (that dialect) |

---

## 1. Cliffs, ranked

### C0. Files over 2 GiB throw `InexactError: trunc(Int32, …)` — blocker
`p14`, `p19`, `p20` (each file generated once, parsed, deleted):

| file | result |
|---|---|
| 2.20 GiB, `id,name(20 B),val` | `File` **THREW** after 1.3 s; `stringtype=String`, `select=[:id,:val]`, `pool=true`, `Chunks` all throw `InexactError: trunc(Int32, 2149894650)` |
| 2.20 GiB, `id,nm(5 B),val,…` 7 cols (≤12 B strings) | `File` **THREW** `trunc(Int32, 2163731536)`; 0.10 reads it in 19.7 s |
| 2.05 GiB, `id,name(20 B),val` (block-written) | `File`, `select=[:id,:val]`, `parallel=false`, `K.parse(select=[1,3])` all **THREW**; `File(limit=10)` OK; `K.index` OK (57 M rows, 0.6 s) |
| 2.07 GiB, `a,b(Float),c,d` numeric only, float NOT last | `File` OK 2.3 s (81 M rows), `select`, `types=`, `parallel=false`, `Rows`, `lazy`, `Chunks` (163 s — see C1) all OK |

Stack (`p20.out`): `_decompose` (`src/values.jl:318`) → `Int32(digstart)` → `DecParts.digstart::Int32` (`values.jl:186`, "buf offset of the first significant digit (tier 3 re-read)") is an **absolute buffer position**, so any float cell that reaches `_decompose` past byte 2³¹ throws. Cells reach `_decompose` whenever the 15-byte SWAR fast path declines: `_float_fast` (`values.jl:772-781`) requires `i + 15 <= length(buf)`, so **the last cell of the file always declines when it is a float**, and the stratified sampler always visits the last row → deterministic failure for any >2 GiB file whose last column is a float (also any float with >15 chars, an exponent, or >8 integer/fraction digits anywhere past 2 GiB). Called from `detecttype` (`core.jl:1481`) during `sampletypes`, so it also fires for *unselected* columns' sampling? — no: it fires because `val` was selected/inferred; `select=[:id,:val]` still includes the float. Fix: make `digstart` an `Int64` (or store it as an `Int32` offset relative to `i`; `_bigmantissa!` at `values.jl:1013` is the only consumer). Separately, `Rows`/`lazy` on >2 GiB files with long strings throw the documented `view_payload` `ArgumentError` ("buffers must stay under 2 GiB") — expected under the current single-buffer design, but the error arrives *mid-iteration* after minutes of work; the front door should refuse (or switch to chunk-owned buffers) up front when `length(buf) > typemax(Int32)` and a string column is not inline-only.

### C1. `CSV.Chunks` construction is O(file) with ~20 allocations per row and O(rows × chunks) row location — blocker
| shape | `Chunks(src)` construct | `File(src)` | ratio |
|---|---|---|---|
| sales 45 MiB, 1M rows | 2.04–2.15 s, 20.0 M allocs, 1 066 MiB | 37–43 ms | 55× |
| sales 1 GiB, 23 M rows | **59.4 s, 460 M allocs, 24.5 GiB** | 0.73 s | 81× |
| numeric 2.07 GiB, 81 M rows | **163 s** (construct + first batch) | 2.3 s | 71× |
| ragged `m6_alllong` 52 MiB | 2.78 s, 20 M allocs | 147 ms | 19× |
| wide 20 000 × 250 (38 MiB) | 1.81 s | 413 ms | 4.4× |
| 45 MiB with `chunkbytes=1 KiB` (46 k chunks) | **57 s** construct+iterate | `File` 0.75 s | 76× |
| 0.10 `Chunks` construct | 0.37 ms | | |

Breakdown on the clean 45 MiB file (`p16` F5): `_prepare`/index 11 ms; `K.sampletypes(nsample=total)` 214 ms (0 allocs — but a full detect cascade on every cell, and `locate` (`core.jl:2207`) is a linear scan over chunks per sampled row, i.e. O(rows × chunks): with 46 k chunks this alone is ~50 s); **`KernelExamples.schemamissing` 2.01 s, 19.999 M allocs, 977 MiB** = 20 allocs per row. Passing `types=` for every column does not help (2.06 s): `schemamissing` runs regardless. Iteration additionally calls `K.chunkrowbase` (O(chunks)) per batch (17 µs per call at 11.7 k chunks — minor). `ntasks` makes no difference (serial prepass).

Code path: `src/api.jl:1980-1990` (`Chunks` → `K.sampletypes(...; nsample=max(1,total))` → `E.schemamissing`), `src/examples.jl:134-152`, `src/core.jl:2105-2126` + `:2207`. In `schemamissing` the loop is `for ci, lr, j` with `types[j]::Type` a *runtime* value: `K.parsevalue(types[j], …)[2]` is a dynamic dispatch whose `(value, ok)` tuple is boxed per cell; `K.cellcontent`'s 4-tuple crosses the same abstract boundary. Chunks exists for files too big to parse eagerly; a construction step that allocates 24× the file and takes 80× longer than `File` defeats it. Fix: (1) column-major function barrier `_colmissing(::Type{T}, buf, chunks, j, opts)` so the inner loop is monomorphic and allocation-free (the discipline `parsecolchunk!` already follows) and run it per column in parallel; (2) use `searchsortedlast` on cumulative row bases in `sampletypes` (as `sampletypesrows` already does) so `locate` is O(log chunks); (3) better, drop the value re-parse: presence (`fieldspan === nothing || len == 0`) needs no value parsing, and the "quoted-empty / sentinel / unparseable → missing" cases can be folded into the per-batch parse with a retroactive `Union{T,Missing}` widening (or simply report `Union{Missing,T}` for Chunks, which 0.10 users already tolerate).

### C2. `types=Dict(:col => Int32/Float32/UInt8/Int16/…)` (any *narrow* type) — 12–14× slower than default, 3 allocations per cell — major
| | time | allocs |
|---|---|---|
| `File(BIG)` default | 45 ms | 7 k |
| `File(BIG; types=Dict(:price=>Float32))` | **536 ms** | 5.0 M |
| `File(BIG; types=Dict(:qty=>Int32))` | **632 / 351 ms** | 3.0 M |
| `File(BIG; types=Dict(:qty=>Int8))` (overflow → problems) | 325 ms | 3.0 M |
| 0.10 same requests | 58–59 ms | 2.4 k |
| `convert(Vector{Int32}, qty)` typed, for scale | 0.54 ms | 3 |

Linear at 3.0 allocs/row (`p16` F1: 10 k → +8.7 ms, 100 k → +44 ms, 1 M → +327 ms). Code path: `src/api.jl:1428-1463` `_narrowtypes`: `T = req[j]` and `c = cols[o]` are abstractly typed (`Type`, `AbstractVector`), so `typemin(T) <= x`, `convert(T, x)` and `out[i] = …` inside the per-element loop are dynamic dispatches that box every value. Fix: one barrier per column `_narrowcol(::Type{T}, c::Vector{S}) where {T,S}` (and the `Union{S,Missing}` variant) with the problem push inside. A 6–10× regression vs 0.10 on a common kwarg.

### C3. `stringtype=InlineString` auto-width picks `String255` for long strings — 86× slower than the default, 11× slower than 0.10 — major footgun
`s8_long.csv` (745 MiB, 4 cols of ~200-byte strings, 1M rows):
| | time |
|---|---|
| default (CompactString) | 103 ms |
| `stringtype=String` | 539 ms |
| **`stringtype=InlineString`** | **8.83 s** |
| 0.10 default (auto-InlineString capped at 31 B, else `String`) | 482 ms |

Breakdown (`p17` G1): `_widthfor` 0.26 ms; `_materializecolumn(String255, col)` = **2.50 s per column**; `String255(ptr, 200)` costs 2.3 µs *per call* (InlineStrings' `addcodeunit` byte loop over a 256-bit integer, `InlineStrings.jl:227-245`) vs 35 ns for `String15(ptr, 15)`. At 13–20 B the same path is 35–40 ms/column (5–8× the parse; tolerable). A fixed too-narrow width (`String31` on 200-B data) *throws* rather than being slow. Code path: `ext/CSVInlineStringsExt.jl:25-30` (`_fitwidth` goes up to `String255`), `:62-64`. Fix: cap the *auto* width at `String31` and fall back to `String` (0.10 parity), or construct wide InlineStrings by loading the bytes as a wide integer + one shift (O(1)) instead of per-byte `addcodeunit`; also an upstream InlineStrings issue.

### C4. Ragged / long rows: ~15 allocations per bad row, paid even with `maxproblems=0` — major
`m6_allshort.csv` (18.7 MiB, every one of 1M rows short):
| | time | allocs | alloc bytes |
|---|---|---|---|
| `File` default | 139 ms | **15.0 M** | 787 MiB (42× file), gc 52 % |
| `File(maxproblems=0)` | 123 / 79 ms | **15.0 M** | 747 MiB |
| `File(maxproblems=typemax(Int))` | 336 ms | 15.0 M | 1.29 GiB |
| 0.10 | 121 ms | 639 | 60 MiB |
| every 10th row short (`m6_ragged10`) | 53 ms (0.10: 283 ms) | 1.5 M | 213 MiB |
| all rows long (`m6_alllong`, 52 MiB) | 147 ms (0.10: 268 ms) | 15.0 M | 878 MiB |

Code path: `src/core.jl:2989-2999` in `directchunk!` — `"expected $ncols fields, found $nf (…)"` is interpolated *before* `pushproblem!` (`:1930`) decides whether the log is full; same in `parsebatch` (`examples.jl:183-190`) and the `userprovided` branches of `parsecolchunk!` (`:1730-1737`, `message * excerpt(...)`). Each bad row costs two `string(Int)`s, a concat, the `Problem` box and the heap path. It also shows up under `ignorerepeated` (C16 below): 83 666 short-row problems ⇒ 1.26 M allocs. Fix: (a) early-out when `log.limit == 0`; when the log is full compare `(pos,row,col)` against the heap top *before* formatting; (b) store `(kind, ncols, nf)` and format lazily in `show`/`getproperty`.

### C5. `limit=N` still indexes the whole file: 1 GiB `limit=10` = 188 ms and **2.1 GiB allocated** (0.10: 0.27 ms) — major footgun
| file | `File(limit=10)` new | 0.10 | ratio |
|---|---|---|---|
| 45 MiB (bytes source) | 4.6–5.9 ms, 91 MiB alloc | 0.63 ms | 7–9× |
| 45 MiB, `parallel=false` | 26.5 ms | | 42× |
| 1 GiB (path) | **188 ms, 2 091 MiB alloc**, gc 16 % | 0.27 ms | ~700× |
| 2.05 GiB | 1.0–1.2 s | | |

It is all the structural index: `K.index` of the full 45 MiB buffer costs 5.0 ms/91 MiB at 8T (26 ms serial); the first 1 MiB chunk alone is 0.6 ms/3 MiB (`p17` G2). The same cost lands on `Rows` (first 10 rows: 5.1 ms), `lazy` (8.5 ms) and `Chunks`. Code path: `src/api.jl:700` `_prepare` → `K.index(buf, d; …)` unconditionally; `limit` is applied afterwards (`_limitrows!`, `_limitchunks`). Fix: with `limit` (or a small `Iterators.take` on `Rows`), index chunks lazily in file order — chunk 1 always starts outside quotes, so no parity pre-scan is needed for a prefix — and stop once ≥ `limit` rows are indexed.

### C6. `skipto` / `footerskip` re-walk the file serially even though the index exists — at 1 GiB each costs *more than a full parse* — major
| 1 GiB | time | vs full `File` (0.73 s) |
|---|---|---|
| `skipto=22_000_000` | **1.32 s** | 1.8× |
| `footerskip=1` | **1.69 s** | 2.3× |
| `footerskip=1, limit=10` | 1.28 s | 1.75× |
| `header=2` | 0.65 s | 0.9× (physical-line walk stops at line 2 — fine) |

At 45 MiB (`p17` G2): `skipto=900_000` 49 ms vs 22 ms default; `_rawrowoffset(buf,d,1,900_000)` alone 40 ms (0 allocs, 44 ns/row scalar `nextrowstart`); `footerskip=1` 72 ms vs 22 ms; `_footeroffset` alone 47 ms (walks the whole buffer forward for a 1-row footer); `skipto=500_000` with `comment='#'` 106 ms. Code path: `src/api.jl:528-536` `_rawrowoffset`, `:559-573` `_footeroffset`, both called from `_prepare` *after* `bi = K.index(...)` (`:700`) has computed every row start in parallel. 0.10: `skipto=900_000` 3.8 s (so still a 77× win); `footerskip=1` parity (85 ms). Fix: derive both offsets from the index (per-chunk `rowstartrel`, walking chunks from the end for the footer; for `skipto`, count rows across chunks — if hygiene-dropped raw rows must count, record the dropped-row count per chunk in `assemblerows!`), making both O(#chunks).

### C7. `File(::Vector{paths})` string chaining: 30× slower than the concatenated file, 6× slower than 0.10, ~8 allocs per string cell — major
100 files × 0.44 MiB (the same 1M rows as `sales`):
| | time | allocs |
|---|---|---|
| `File(paths)` | **670 ms** | 19.7 M |
| `[File(p) for p in paths]` (no chain) | 80 ms | 160 k |
| `File(concat_single_file)` | 22 ms | 7 k |
| 0.10 `File(paths)` | 110 ms | 139 k |
| `File(paths; select=[:id,:price,:qty])` numeric only | 95 ms | 137 k |
| `File(paths; select=[:region])` one inline string column | 276 ms | 8.1 M |
| `A._chaincolumn` on 1 inline string column (1M cells) | 223 ms, 7.95 M allocs | vs `reduce(vcat, K.materialize.(pieces))` 14 ms |
| 1 000 tiny files | 33 ms (0.10: 11 ms; serial per-file `File`) | 552 k |

Code path: `src/api.jl:1049-1084` `_chaincolumn`: `pieces::Vector{AbstractVector}` and `E`/`T` computed at runtime, so `for (k, x) in enumerate(c) … out[off+k] = String(x)` is a dynamic loop (boxing `x`, dynamic `String`, dynamic `setindex!`). Fix: per-piece barrier `_appendpiece!(out::Vector{E}, c::K.CompactStringVector, off)` using `K.materialize`-style bulk conversion; parallelize the per-file `File` calls (0.10 does).

### C8. `RowWriter` renders through an `IOBuffer` + `string(x)` per cell: 61 allocations per row, 4.6× slower than 0.10 — major
| 1M rows × 8 mixed cols | time | allocs |
|---|---|---|
| `collect(CSV.RowWriter(t))` | **2.66 s** | 61.1 M, 3.3 GiB |
| `for r in RowWriter; write(io, r)` | 2.45 s | 60.1 M |
| 0.10 `collect(RowWriter(t))` | 582 ms | 18.0 M |
| `CSV.write(IOBuffer(), t)` (bulk path) | 33 ms | 227 |

Code path: `src/write.jl:669-678` `_renderrow` (fresh `IOBuffer`; `_writecell(io, Tables.getcolumn(row, j), o)` where `Tables.getcolumn` on a `Tables.RowIterator` row is type-unstable and `_writecell` (`:142-171`) uses `string(x)` for floats/dates/times; `String(take!(io))` per row). Fix: render each row with the bulk engine's `_appendcell!`/`_writerow!` into a reusable `Vector{UInt8}` (the ≤32-col Tuple path already exists) and take the row schema once (`Tables.schema(rows)`).

### C9. Writer Tuple-path compile latency: ~0.3 s per *new* ≤32-column schema (1.06 s for 8 cols, 6.6 s CPU for 31–32 cols), 1.55× 0.10 — major TTFX footgun
| | first call (wall) | compile (CPU; sums parallel JIT) | steady |
|---|---|---|---|
| 50 random schemas of 4–20 cols, 1 000 rows | **15.0 s total** | 14.95 s | 27 ms total |
| 0.10, same 50 schemas | 9.6 s total | | |
| 8 cols × 200 k rows | 141 ms | 1.06 s | 8.0 ms |
| 31 cols | 847 ms | **6.66 s** | 14.6 ms |
| 32 cols (Tuple path) | 838 ms | 6.61 s | 10.6 ms |
| 33 cols (staged path) | 51 ms | 232 ms | 15.9 ms |
| 40 / 64 / 200 cols (staged) | 15–31 ms | 0 | 14–31 ms |
| 32 cols of 32 distinct eltypes | 1.27 s | 1.26 s | 1.4 ms |
| same table as 33 cols (staged) | 528 ms | 525 ms | 1.5 ms |

Steady-state the Tuple recursion buys ≤1.5× over the staged path (10.6 vs 15.9 ms at 50 k rows; nothing at 1 000 rows) but costs 0.2–0.8 s wall / up to 6.6 s CPU per new schema (`src/write.jl:549-585`, `_writecells!` recursion on `cols::Tuple`, keyed on *container* types — `Vector` vs `view` vs `PooledArray` each recompile ~75 ms; column names don't). Users writing many differently-shaped tables (per-group exports, tests, notebooks) pay it repeatedly, and 32→33 columns is a discontinuity (838 ms → 51 ms first call). Fix: lower `TUPLE_RENDER_MAXCOLS` (8–12) or gate the Tuple path on `nrows ≥ ~100 k`; precompile the common eltype set; specialize on eltypes only.

### C10. Dialects that lose the vector scanner and/or parallel chunking — 3–8× slower than the default dialect; two of them 3× slower than 0.10 — major
45 MiB sales file (`p04`, two runs):
| dialect | scanner / chunking | new | vs default (45 ms) | 0.10 | new/0.10 |
|---|---|---|---|---|---|
| `escapechar='\\'` | scalar, **single chunk (serial)** | 347–465 ms | **7.6×** | 103–154 ms | **3.0–3.4×** |
| `openquotechar='<', closequotechar='>'` | scalar, **single chunk** | 334 ms | **7.4×** | 109 ms | **3.1×** |
| `comment="#"` | scalar (chunked) | 162–184 ms | 3.6× | 117–159 ms | 1.2–1.4× |
| `scanner=:scalar` forced (for scale) | scalar, chunked | 107 ms | 2.4× | | |
| `delim="::"` (multi-byte) | scalar, chunked | 94 ms | 2.1× | 130 ms | 0.73× |
| `ignorerepeated=true` (`,,` file) | vec | 54–67 ms (1.26 M allocs — C4) | 1.5× | 525 ms | 0.13× |
| `quoted=false` | vec | 34 ms | 0.8× | 112 ms | 0.30× |
| `missingstring=["NA","null"]` | vec | 68 ms | 1.5× | 130 ms | 0.53× |
| `dateformat="yyyy-mm-dd"` | vec | 74 ms | 1.6× | 134 ms | 0.55× |
| `stripwhitespace`, `decimal`, `truestrings`, `groupmark`, `normalizenames`, `header=false` | vec | 0.9–1.1× | | | |

Code path: `src/core.jl:123-131` `parityclean`/`swareligible`, `:1353-1358` `resolvescanner`, `:1281` `nranges = parityclean(d) ? … : 1`. Backslash escapes and asymmetric quotes are common in real exports (MySQL `OUTFILE`, Hive, bracketed quoting); the fallback is serial *and* scalar. Fix options: (a) speculative chunk boundaries + validation for non-parity-clean dialects; (b) for `escapechar='\\'`, parity is recoverable — a quote preceded by an odd run of backslashes is not a toggle, a (quote, backslash-parity) 2-state FSM still composes; (c) for `comment=`, chunk bounds are already computed — the loss is the scalar scanner; a vector scanner that treats a comment prefix at a row start as "skip to newline" recovers most of the 3.6×.

### C11. `pool=true` on a high-cardinality column (17× the unpooled parse; 0.10 parity) and per-column pooling overhead on wide tables — minor
`s8_unique.csv` (1M unique 13–20 B values in col a): no pool 13.6 ms; `pool=true` **230 ms, 1.0 M allocs, 679 MiB** (16.9×); 0.10 `pool=true` 214 ms. With `pool=true` the bound is `n`, so the column is fully interned into a `Dict{CompactString,UInt32}` of 1M entries and *kept* as a 1M-level PooledArray; `pool=(0.2,500)` aborts early: 37 ms. Documented 0.10 semantics — a doc warning suffices. `_poolcolumns` (`api.jl:1405-1426`) spawns one task per string column with a fresh `Dict` + 3 vectors: on the 5000-column string-wide file `pool=true` costs 601 ms vs 117 ms (5×, 6.9 M allocs ≈ 1 400 per column). Fix: skip the parallel split for short columns and reuse a thread-local intern table.

### C12. Wide files: throughput drops 23× from 1 000 to 50 000 columns and the parallel path becomes *slower* than serial above ~20 000 columns — major footgun (0.10 parity)
| shape (all ≈38 MiB, Float64) | new | MiB/s | allocs | `parallel=false` | 0.10 | new/0.10 |
|---|---|---|---|---|---|---|
| 100 × 50 000 | 38 ms | 980 | 31 k | 197 ms | 40 ms | 0.95× |
| 1 000 × 5 000 | 38 ms | 1 004 | 290 k | 185 ms | 60 ms | 0.62× |
| 5 000 × 1 000 | 124 ms | 305 | 1.27 M | 182 ms | 140 ms | 0.88× |
| 20 000 × 250 | 413 ms | 92 | 3.98 M | **286 ms** | 407 ms | 1.02× |
| 50 000 × 100 | 875 ms | 43 | 9.98 M | **692 ms** | 1.34 s | 0.65× |

Allocations scale at ~200 per column (≈5 per column × chunk: 38 chunks × 50 k cols = 1.9 M `(col, chunk)` units), and each unit takes `lock(promolock)` twice, builds a closure and dispatches dynamically into `parsecolchunk!` (`src/core.jl:3006-3060` in `directchunk!`); serial wins because that fixed cost dominates and 8 tasks contend on the lock. Downstream: `Tables.schema` 3.2 ms at 50 k; `DataFrame(f)` 833 ms and `Tables.columntable` 846 ms at 50 k (DataFrames/Tables `NamedTuple` cost, not CSV); `select=[:c1,:c2]` 55 ms; `CSV.write` of 50 k cols 1.19 s (parity); `Rows` 25 ms. Fix: batch columns per chunk task for wide tables (keep `ncols × nchunks` bounded, pick larger chunks when `ncols` is large), read `promo[j]` lock-free when `userprovided[j]`.

### C13. `Rows` with `types=` — 3.3× slower than untyped `Rows`, 3 allocs/row; `File` row iteration 6 allocs/row (0.10 parity) — minor
`p10`/`p21`: `Rows` `row.price` 56–96 ms / 2.0 M allocs; `Rows(types=Dict(:price=>Float64))` **317 ms / 3.0 M allocs**; all 7 cols accessed 188 ms / 6.9 M allocs (≈1 alloc per cell); still 50–160× faster than 0.10 `Rows` (15.4 s on this file). `Tables.getcolumn(row::Row, j)` (`api.jl:1907-1917`) goes through `types::Union{Nothing,Vector{Type}}` and `E.typedvalue(ts[j], …)` with a runtime `Type`, so every typed cell is a dynamic dispatch + box; `Row` itself is one allocation per row. `for row in CSV.File(...); row.price` is 95 ms / 6 allocs per row — identical to 0.10 (94.6 ms / 6 allocs), `Tables.rowtable(f)` 355 vs 201 ms (1.8×), `namedtupleiterator` 436 vs 310 ms (1.4×). Fix: carry the schema in the `Rows`/`Row` type (a `Tuple` of column types) as 0.10 did.

### C14. Writer per-cell allocations for `floatformat` / `dateformat` / `Time` / `Symbol` — minor (parity or better vs 0.10)
1M rows: `floatformat="%.3f"` 33.5 ms / 4.0 M allocs (0.10: 28 ms); `dateformat="dd/mm/yyyy"` 103 ms / 12 M allocs vs 5.1 ms default (20×; 0.10: 1.69 s); `Time` column 47 ms / 7.8 M allocs (`string(x)` path); `Symbol` 1 alloc/cell; `BigFloat` 18 allocs/cell. Code: `src/write.jl:490-525` `_appendcell!`. Fix: `Printf.format(io, …)` / `Dates.format(io, …)` into the byte buffer; a direct `Time` renderer next to `_appenddatetime!`.

### C15. View retention footgun — a single 13-byte `CompactString` pins the whole input buffer — minor (by design; needs loud docs)
`p17` G3 on a 56 MiB file: `summarysize(fm.a[1])` = **56.3 MiB**; `fm.a[1:5]` = 56.3 MiB; `unique(fm.a)` (5 000 levels) = 56.4 MiB; `Dict` keys = 56.5 MiB; a column with escaped long cells pins the `extra` buffer (23.7 MiB) as well. `String(x)` releases it. Inline (≤12 B) values retain nothing (`f.notes[1:5]` = 0 MiB). At 1 GiB, `s = df.name[1]; df = nothing` keeps 1 GiB mapped. Fix: document; have `unique`/`Set`/`PooledArray`/`getindex(::AbstractVector)` on `CompactStringVector` return materialized or `CompactStringVector` results.

### C16. `DataFrame(CSV.File(...))` copies each `CompactStringVector` into a `Vector{CompactString}` — minor but ubiquitous
`p09` H9b: `DataFrame(f)` 6.3 ms / 916 k allocs vs 1.2 ms for `Vector{String}` columns; the result's `df.region` is a `Vector{CSV.CSVKernel.CompactString}` (24 B/elt, every element carrying a buffer pointer, `materialize`/the writer fast paths no longer apply). `CSV.read(src, DataFrame)` is unaffected (20 ms; it wraps in `Tables.CopiedColumns`, `api.jl:1626`). Cause: no `Base.copy`/`similar` method for `CompactStringVector` (`src/compactstring.jl`), so `copycols=true` falls to the generic `AbstractArray` copy. Fix: `Base.copy(v::CompactStringVector) = CompactStringVector{eltype(v)}(copy(v.payloads), v.buf, v.extra)` (+ `similar`, range `getindex`).

### C17. Downstream string ops on `CompactString` that hit Base's char-iterating `AbstractString` fallbacks — minor–major
1M rows, `p09` (CS = `CompactStringVector`, S = `Vector{String}`), ratios CS/S:
- **9.9×** `occursin.("99", col)` (121 vs 12 ms, 280 k allocs), **9.8×** `startswith.(col, "word-0000")` (56 vs 5.7 ms), **3.2×** `endswith.`, **3.7×** `length.` / `sum(length, col)` (no ASCII fast path in `Base.length(::CompactString)`), **3.2×** `join(col, ",")`, **2.3×** per-element `write(io, s)` (byte loop, `compactstring.jl:322`), **4.2×** `split.(col,'-')`, **2.6×** `SubString.`/`filter`, **2.3×** `strip.`, **43×** `Vector{Union{Missing,String}}(notes)` (25.7 ms, 2.7 allocs/elt) and `convert(Vector{String}, col)`/`Vector{String}(col)` 1 alloc/elt (11 ms vs `K.materialize` 11.5 ms — fine, but should route to `materialize`, which itself is 8.9× `copy(::Vector{String})` because it must allocate 1M `String`s), **3.2×** `coalesce.(notes, "")` (916 k allocs, `Union{CompactString,String}` result), **1.6×** `Symbol.`, **1.55×** `Dict` counting (3 allocs/elt), **2.9×** `innerjoin` on a CS key, **1.6×** `sort(df, :a)`, **1.3×** `sort`/`sortperm` on views, **1.5–2.5×** `CSV.write` of CS columns vs `String` columns (generic `codeunits` path in `_appendstring!`, `write.jl:243`), **1.25–1.3×** `Arrow.write` (round-trips as `String`; the StringView payload compatibility isn't exploited).
- Wins / parity: `sort(region)` 0.46×, `mid .== x` 0.33×, `region .== "north"` 1.0×, `unique`/`Set` 0.8–1.06×, `groupby` 0.42–0.9×, `filter(:region => ==("north"))` 0.94×, `ncodeunits.` 0.5×, `string.` 1.1×, `map(uppercase, ·)`/`lowercase.` 1.4–1.5× (both allocate 7/elt), `PooledArray(col)` 1.4×, `String15.(col)` 1.0×, `show(text/plain)` 1.3×, `describe` 0.9×, `vcat` 1.4×, `dropmissing` 1.4×.
Fix: byte-level methods for `startswith`/`endswith`/`occursin(::AbstractString, ::CompactString)`/`findnext`, an ASCII fast path in `length`, `write(io, s)` via `unsafe_write` of the payload words / buffer slice, `convert(Vector{...}, ::CompactStringVector)` → `materialize`, and a `String`-typed fast path in the writer for `CompactString`.

### C18. Correctness bug found in passing: `openquotechar`/`closequotechar` without an explicit `delim` throws — blocker for that dialect
`CSV.File(path; openquotechar='<', closequotechar='>')` → `FieldError: type Base.Pairs has no field openquotechar` from `_probedelim` (`src/api.jl:102`: `dialectkw.openquotechar` on a `Base.Pairs`; `haskey` works but property access does not). Works with `delim=','`. Fix: `get(dialectkw, :openquotechar, nothing)`.

---

## 2. Memory profile (H12)
- `CSV.read(45 MiB, DataFrame)`: Δ maxrss **214 MiB = 4.7× file**; `Base.summarysize(df)` 108 MiB (each `CompactStringVector` counts the shared 45 MiB mmap → the two string columns report 61 MiB each); `stringtype=String` df: 74 MiB owned.
- 1 GiB file: `File` **0.73 s (1 440 MiB/s), 3.64 GiB allocated (3.5× file), gc 25 %**, Δ maxrss 3.3 GiB; 0.10: 2.99 s, 2.06 GiB allocated. `CSV.read(…, DataFrame; stringtype=String)` 1.41 s, 5.3 GiB allocated, maxrss 8.7 GiB. `pool=true` 1.02 s. `stringtype=String` `File` 1.82 s / 44 M allocs. `CSV.write` of the 1 GiB table to a path 2.17 s / 5.0 GiB allocated. The transient index (UInt32 tape ≈ one word per structural byte, ~0.7 GiB here) plus final columns pushes allocation to 3.5×; peak RSS ≈ 3.1× is at the upper edge of the 2–3× target. `select=[:id]` on 1 GiB still allocates 2.3 GiB (index + columns) — projection is free for parsing, not for indexing. `Rows` over 1 GiB: 0.98 s, 23 M allocs (1/row), 2.8 GiB allocated (index).
- Retention: C15.

## 3. Thread scaling (H13) — same script at `-t1/2/4/8`, separate processes (`p13_t*.out`)
| op | t=1 | t=2 | t=4 | t=8 | speedup t8/t1 |
|---|---|---|---|---|---|
| `File(BIG)` | 109 ms | 85.5 ms | T4 | T8 | S1 |
| `File(BIG; stringtype=String)` | 130 ms | 125 ms | T4 | T8 | S2 |
| `File(BIG; pool=true)` | 139 ms | 75 ms | T4 | T8 | S3 |
| `File(s8_mid; pool=true)` | 123 ms | 68 ms | T4 | T8 | S4 |
| `CSV.write(IOBuffer, f)` | 195 ms | 101 ms | T4 | T8 | S5 |
| `Chunks` construct | 1.87 s | 1.92 s | T4 | T8 | S6 |
| `Rows` iterate col 1 | 57 ms | — | — | — | (serial by nature) |
| 0.10 `File(BIG)` | 286 ms | — | — | — | |
| `File(wide 5000×1000)` | 152 ms | — | — | — | |

Single-threaded the kernel is already 2.6× faster than 0.10 on this file. `parallel=false` and `ntasks=1` behave identically to `-t1` (107–112 ms at every thread count). SUMMARY13

## 4. 2 GiB boundary (H14)
See C0. Summary: `K.index` is fine past 2 GiB (chunk-relative UInt32 tape); the columnar path throws from the float decomposer's `Int32` absolute offset whenever a float cell falls off the 15-byte fast path (deterministic when the file's last column is a float, since the last row is always sampled); `Rows`/`lazy` throw the designed `view_payload` `ArgumentError` for long-string views past 2 GiB; numeric-only >2 GiB files read correctly (2.05 GiB in 2.3 s; 2.2 GiB `Chunks` on it 163 s per C1).

---

## 5. Hypotheses tested that were FINE (coverage)
- **Late type promotion (H5)** — all fine and 5–14× faster than 0.10: Int→String at row 999 999: 44 ms vs 45 ms clean (0.97×; 0.10: 310 ms = 5.1× its clean); mid-file 0.98×; Int→Float64 0.79×; Int64→Int128 1.01×; Float→String 0.77×; Bool→String 0.76×; 6 columns promoting at once 1.53× (0.10: 14.2×). `nsample` 8…100 000 makes no difference (37–60 ms, noise).
- **Missing-heavy (H6)**: 50 % missing cells 29 ms (781 MiB/s), 0.68× 0.10. Ragged every-10th-row 53 ms vs 0.10 283 ms (0.19×) despite C4. `strict=true` throws after 57 ms (0.10: 288 ms). `CSV.problems(f)` access is free.
- **Quoting (H7)**: every cell quoted 44 ms (0.83× plain, 0.61× 0.10); embedded newlines 0.53× plain (0.42× 0.10); escaped `""` in every cell (inline) 0.59× plain (0.36× 0.10); escaped *long* cells → `extra` buffer 109 ms = 2.05× plain, 1.11× 0.10, 2.0 M allocs (one per unescaped cell); 500 × 100 KB quoted cells 7.3 ms (0.04× 0.10); ONE 200 MB quoted cell 2.43 s / 78 MiB/s single-row chunk (0.10: 15.3 s, 6 GiB alloc); a 200 MB *unquoted* single line 2.79 s — no tape-limit error (`MAX_TAPE_RELPOS` = 1 GiB would need a >1 GiB row).
- **String shapes (H8)**: ≤12 B inline 31 ms (0.54× 0.10), 13–20 B views 20 ms (0.25×), 200 B 103 ms (0.21×); `stringtype=String` 92/126/539 ms (1.1×/1.5×/0.7× of 0.10's `stringtype=String`); `pool=true` on low-card 32–44 ms; `pool=(0.2,500)` 27–40 ms; `downcast=true` 50 vs 35 ms (typed path); `sales_pool` default 12 ms vs 0.10 16 ms.
- **Reader TTFX (H3, fresh process, `p03`)**: baseline first call 288 ms (0.10: 135 ms) then 12–40 µs; new *shapes* (6 cols with Date/Bool/missing) +0.06 ms; each new driver-kwarg combination re-specializes: `types=Dict(1=>Float32)` +262 ms, `select=[:a]` +450 ms, `dateformat=Dict(...)` +484 ms, `dateformat="dd/mm/yyyy"` +232 ms, `comment` +251 ms, `escapechar` +221 ms, `missingstring` +304 ms, `limit=1` +206 ms, `transpose` +281 ms, custom `tryparse` type +95 ms (0.10: +337 ms), `Int128`/`BigInt`/`UUID` +88–110 ms, `String15`/`InlineString`/`pool` +14–67 ms, `lazy` +107 ms, `Chunks` +18 ms, `Rows` +0; total across 47 new-CSV variants 5.8 s. Not a cliff, but `select`/`limit`/`missingstring`/`dateformat`/`comment` (200–480 ms each) deserve a precompile workload. `types=Dict(1=>Char)` errors clearly ("unsupported column type Char").
- **Sources (H11)**: path/mmap 41 ms, `buffer_in_memory` 42 ms, `Vector{UInt8}` 22 ms, `IOBuffer` 39 ms, `IOStream` 51 ms, `codeunits(String)` 60 ms, `Cmd` 80 ms; gzip 45 MiB 153 ms (0.10 242 ms; `transcode` alone 113 ms of it); `GzipDecompressorStream` as IO 185 ms. Per-call fixed overhead on a 3-line table: **8.2 µs / 244 allocs** with `delim=','`, 12 µs with sniffing (0.10: 140–162 µs); `_prepare` 0.8 µs, kernel `parse` 5 µs, `Dialect` 0.1 µs, `CSV.write` of a 2-row table 1.1 µs. `append=true` × 1 000 to a path: 22 ms (0.10 129 ms).
- **Writer (H15)** 1M×8 mixed: 33 ms vs 0.10 414 ms (0.08×); to a path 51 ms; `Union{Missing,Float64}` 4.3 ms; Float64 8.5 ms; `decimal=','` 6.7 ms; Date 5.1 ms (0.10 188 ms); DateTime 8.1 ms (0.10 409 ms); String 8.5 ms, `quotestyle=:all` 7.2 ms, `:none` 17.5 ms; `PooledArray` 4.0 ms; `CategoricalArray` 2.5 ms; `String15` 3.8 ms; `SubString` 6.3 ms; `CompactString` inline 10.1 ms / views 10.6 ms / `Union{Missing,CompactString}` 12.7 ms; `Vector{Any}` 15.9 ms (0.10 100 ms); `Char`/`UInt8`/`Int32`/`Float32` 4–11 ms; `Rational`/`Complex` 4–5 ms per 100 k; row-table input 64 ms (0.10 445 ms); `DataFrame` 34 ms (0.10 1.9 s); `SubDataFrame` 21–23 ms; matrix 26 ms; `bom`/`newline`/`delim`/`missingstring` ≤1.6×; `ntasks=1` 209 ms (6.3× scaling); `compress=:gzip` 3.02 s vs 0.10 2.90 s (zlib level; parity — a `level` knob would help); `partition=true` fine; wide writes 1 000 cols 159 ms (0.10 686 ms), 5 000 cols 409 ms, 50 k cols 1.19 s (parity). 32 vs 33 columns: steady-state parity (C9 is compile only).
- **Doors (H10)**: `Rows` untyped 56–96 ms (0.10 15.4 s); `Rows` construct 15 ms (= index); `Rows(stringtype=String)` 140 ms (3.75 allocs/row); `Tables.rowtable`/`columntable(Rows; limit=100k)` 47 / 32 ms; `DataFrame(Rows)` full 432 ms (0.10: 968 ms for 100 k rows); `Chunks` iteration after construction is cheap and O(chunks²) only through `chunkrowbase` (24 µs/chunk at 11.7 k chunks); `lazy` construct 7.6 ms (= index), `File(lf)` 16 ms, `DataFrame(lf)` 178 ms / 7.2 M allocs (LazyColumn `getindex` boxes the `Union` per cell — acceptable), `collect(lf.price)` 16 ms, `lf[i,j]` 61 ns sequential / 353 ns random, typed lazy `sum(skipmissing(lf.price))` 20 ms / 7 allocs; `lf.price[1:1000]` returns `Vector{Union{Missing,CompactString}}` (fine). `File` sinks: `DataFrame(f)` 6 ms (C16), `Tables.columntable(f)` 26 µs, `collect(f)` 3 ms, `Tables.matrix` 43 ms, `show(f)` 1.9 ms. `chunkbytes=16 KiB` 101 ms, `chunkbytes=64 MiB` (1 chunk) 148 ms.
- **Wide (H1)**: `Tables.schema` with 50 k types 3.2 ms; `CSV.write` of 1 000–5 000-col tables 4× faster than 0.10; `select` on wide files cheap; 1 000×5 000 float file 38 ms = 1 GiB/s (0.62× 0.10). The cliff is only the >20 k-column per-unit overhead (C12).
- **Memory (H12)**: no leak: dropping the `File` releases the mapping unless a view survives (C15); GC time 18–25 % on the 1 GiB parse.

## 6. Files
`$SP/pc/`: `h.jl` harness; `p01_wide`, `p02_writer_compile`, `p03_ttfx`, `p04_dialects`, `p05_promotion`, `p06_missing_ragged`, `p07_quoting`, `p08_strings`, `p09_downstream`, `p10_doors` (+`p10_doorsb`), `p11_sources`, `p12_memory`, `p13_threads` (`p13_t1/2/4/8.out`), `p14_2gib`, `p15_writer`, `p16_followups`, `p17_followups2`, `p18_irchk`, `p19_2gib_trace`, `p20_2gib_str`, `p21_rows_chunks`. Generated data under `$SP/data/` has been reduced to the small fixtures (`tiny.csv`, `tiny1000/`); the 1–2 GiB files were deleted by their probes.
