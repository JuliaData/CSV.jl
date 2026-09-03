# CSV.jl 1.0 — hardening review, 2026-09-02

Branch `kernel-proveout` reviewed at `eefe404`. Six parallel reviews, each
with a probe battery run against the branch (and the frozen 0.10 `LegacyCSV`
oracle where a comparison made sense), plus my own probes and measurements:

| report | slice | probes |
|---|---|---|
| [reader-ergonomics](hardening-2026-09-02/reader-ergonomics.md) | File/read/Rows/Chunks/lazy as a user sees them | ~700 |
| [api-consistency](hardening-2026-09-02/api-consistency.md) | keyword × door matrix, Tables interface matrix, writer | ~600 |
| [downstream-interop](hardening-2026-09-02/downstream-interop.md) | CompactString in DataFrames / Arrow / Base | ~400 |
| [usecase-gaps](hardening-2026-09-02/usecase-gaps.md) | pandas / polars / DuckDB / readr / fread / pyarrow gap analysis | ~330 |
| [perf-cliffs](hardening-2026-09-02/perf-cliffs.md) | 17 cliffs, 15 hypothesis families that were fine | shapes × sizes to 2 GiB |
| [kernel-warts](hardening-2026-09-02/kernel-warts.md) | core/values/compactstring: fragile invariants, unsafe code, dead code | 26 findings |
| [own-strings-measurement](hardening-2026-09-02/own-strings-measurement.md) | cost of owning string bytes instead of viewing the input | A/B, 7 shapes |

The six reports overlap heavily — the same handful of root causes surfaced in
every slice. This document is the consolidated view: what I changed on the
branch (section 1), the decisions that are yours (section 2), and the ranked
backlog that remains (sections 3–6). Settled 1.0 decisions (CompactString
default, `pool=false`, empties-always-missing, problems-as-data, no schema
widening, strict Bool, Int128, retired function selectors) were not
re-litigated; where one has a sharp edge it is reported as such.

## 0. TL;DR

The architecture holds. Every reviewer confirmed the headline properties —
determinism across thread counts and chunk geometry, sample-independent
inference, exact `limit`/`skipto` under threads, problems as data, single-
threaded speed 2.6× 0.10 and 8-thread 5–15× — and found no correctness bug
in the structural or value layers on well-formed input. The rough edges are
concentrated at the seams: the string column's memory model, the API
surface's spelling consistency, and a few O(file) prepasses.

**Root cause behind the four worst findings: string columns view the input
buffer.** That single fact produced (a) read-only string columns in every
`CSV.read(…, DataFrame)` result, (b) a process crash (SIGBUS) when a mapped
file is truncated while a table is alive, (c) whole-file memory retention by
any surviving string value or slice, and (d) the 2 GiB offset cliff. (a) is
fixed in this pass by making the column a real mutable vector with an
append-only arena; (b)–(d) need the design decision in §2.1, which I
recommend making before 1.0 and measured to cost ≤ 7 % on ordinary files.

Landed on the branch in this pass (details in §1): mutable, constructible
`CompactString` columns; the `scan=` door repaired against the current
Tables `jq/scan`; `Chunks` yields `File`; `CSV.ParseError`; `on_error=:warn`;
`f[:col]`/`f["col"]`/`size`; readable `show` for every public type; writer
keyword hygiene (0.10 spellings, multi-byte delimiters, no more truncating
`stdout`); `types=String` honored as an output type; six correctness bugs;
five performance cliffs removed (Chunks construction 55× → 6×, narrow types
30× → 1×, multi-file chaining 30× → 12×, ragged-row allocation, InlineString
auto-width). Full suite green (counts in §1.9).

## 1. Landed in this pass

Each item names the commit's theme; the detailed report ids (R = reader-
ergonomics, A/W/S/K/N/D = api-consistency, I = downstream-interop, G =
usecase-gaps, C = perf-cliffs, W = kernel-warts) say where it came from.

### 1.1 `CompactString` construction and a mutable `CompactStringVector` (R1, I1–I4, G1, N3, W16, W18)

`src/compactstring.jl`:

- `CompactString(s::AbstractString)` and `convert`: ≤ 12 bytes inline, longer
  content copied once into a private buffer the value views. `push!(df, row)`,
  `col[i] = "x"`, `CompactString["a", "b"]`, `Dict{CompactString}` sets,
  `PooledArray`/`CategoricalArray` pushes all work.
- `CompactStringVector` is a real mutable vector: `setindex!`, `push!`,
  `pushfirst!`, `insert!`, `pop!`, `popfirst!`, `deleteat!`, `keepat!`,
  `empty!`, `resize!` (grown slots are `missing`/`""`, never garbage views),
  `sizehint!`, `copy`, `IndexLinear`. Foreign bytes are appended to the
  column-owned `extra` arena (append-only, so live views stay valid); values
  already viewing the column's buffers keep their bytes. Every DataFrames
  mutation that failed in the reviews — `sort!` by any column, `filter!`,
  `deleteat!`, `dropmissing!`, `append!`, `push!`, `replace!`, `subset!`,
  `df[i, :s] = "x"` — passes.
- Regex: `match`, `findnext(::Regex)` (hence `split(s, r"…")`), `reverse`
  forward to `String`, as InlineStrings does.
- `Base.write(io, ::CompactString)` is one `unsafe_write` (views) or a 16-byte
  scratch store (inline) — the byte loop was 55× slower at 1 KB and sat under
  `print`, `join`, `string`, and DataFrame display.
- `convert(Vector{String}, col)` routes through `materialize`.

### 1.2 Scan door against the current Tables `jq/scan` (S1, G3)

Tables `0a8ae5e` renamed `bind → resolve`, `coleq/colne/… → colcmp(op, …)`,
`in_ → colin`, and made `select=All()` the default (`select=nothing` is
refused). `src/scan.jl` and `test/scan.jl` are migrated; 307 scan tests
green. Pin `Tables = "1.14"` for the Scan path once it is released (§2.7).

### 1.3 Surface and ergonomics (A1, R4, R8, R10, R11, K5–K8, N2, D1, R5)

- `File`: `f[:col]`, `f["col"]`, `size(f)` (`(nrows, ncols)`, matching
  `LazyFile`), `f[end]`, `f[1:2]`, `row["col"]` on both row types,
  `lf["col"]`, `names(::Rows)`, `names(::Chunks)`.
- `Chunks` yields `File` (not `CSVKernel.ParsedTable`), so `chunk.col`,
  `chunk[:col]`, `length`, `show`, `problems` all behave; `eltype` is `File`.
- `show` for `File`, `Rows`, `Chunks`, `LazyFile`, `Problem`, `Spec` prints
  public names (`CSV.File("path"): 1000 × 5` + schema) and never dumps the
  buffer (a `Rows` over a 1 GiB file printed a gigabyte). Type names in
  schemas print as `CSV.CompactString`, not `CSV.CSVKernel.CompactString`
  (string replacement — the flatten in §2.5 is the real fix). Source labels are
  `"<IOBuffer>"`, `"<bytes>"`, `"<Cmd>"`, `"<IOStream>"`, the path, or the
  `FilePathsBase` path.
- `CSV.ParseError <: Exception` (`problem`, `nproblems`, `source`) replaces
  the three bare `ErrorException("CSVKernel: …")` sites; `showerror` names
  kind, row, column, byte, and the `on_error=:collect` escape.
- `on_error=:warn` on `File`/`read`/`scan`: ONE summary `@warn` per parse
  (count, first problem, how to retrieve the rest). Default unchanged (§2.2).
- `CSV.read(source)` without a sink is a guiding `ArgumentError` (0.10
  behavior); `CSV.read(source, Matrix)` works (`Tables.matrix`).
- `File(::LazyFile)` validates its keywords, accepts `ntasks`/`maxwarnings`,
  and says "fixed when the LazyFile was created; pass it to CSV.lazy" for
  row-positional/dialect keywords instead of a `MethodError`.
- Bound at top level: `CSV.Problem`, `CSV.ParseError`, `CSV.rownumber`,
  `CSV.CompactStringVector`, `CSV.materialize`. The submodules' exports
  (`index`, `Dialect`, `ParsedTable`) no longer leak into `CSV`.
- Docstrings: `Problem` (kinds documented), `problems`, `RowWriter`
  (`?CSV.RowWriter` resolves), `CSV.write` (every keyword listed), one `Rows`
  docstring instead of two that disagreed, module docstring lists every door,
  legacy-keyword hints say `CSV.CompactString`.

### 1.4 Correctness bugs (C17, W5, W12, W13/K6, K2, W14)

- Custom `quotechar`/`openquotechar` without an explicit `delim` threw
  `FieldError` on any input past the 64 KiB sniff sample (`_probedelim` read
  keyword `Pairs` as a `NamedTuple`).
- `footerskip` walked quote-aware from byte 1, so a quote in a skipped prefix
  row swallowed the file (0 rows instead of 2) — now walks from the anchor,
  matching the physical-line prefix rule.
- `limit=0` dropped a declared `Union{Missing, T}`.
- `Rows(types=Dict(:a => Int))` silently turned every *other* column's cells
  from `CompactString` into `String`; they now stay what they are without
  `types=` (and follow `stringtype`).
- `dateformat=dateformat"…"` (a `Dates.DateFormat`) is accepted again, alone
  and inside a `Dict` (0.10 accepted it).
- `decimal='0'` / `'e'` / `'-'` are rejected (`decimal='0'` parsed `"105"` as
  105); `decimal == delim` stays legal (such values are only expressible
  quoted, like `groupmark == delim`).

### 1.5 `types=String` names the output type (R5, I7)

`types=String`, `types=Dict(:s => String)`, `types=[Int, Union{String,
Missing}]` return `Vector{String}` columns (0.10's read-everything-as-text
idiom); `String15`/`InlineString` (with InlineStrings loaded) and
`CSV.CompactString` are accepted in `types`. `stringtype` governs inferred
text only.

### 1.6 Writer (W1, W2, W3, W4, W6, W10, W13)

- IO sinks are written at their current position. The rewind-and-truncate
  applied to every seekable IO — `julia script.jl > log` lost every earlier
  log line, `>> log` lost the CSV itself, `open(p, "w") do io; write(io,
  hdr); CSV.write(io, t) end` lost `hdr`. Only `IOBuffer` keeps the
  replace/append semantics (the tests pin it).
- 0.10 spellings: `quotestrings=true` ⇒ `quotestyle=:all` (alias),
  `header=false` ⇒ `writeheader=false`, `compress=true/false` ⇒
  `:gzip/:none`; `transform`/`bufsize` and unknown keywords get the reader's
  "removed in 1.0: …" / "unsupported write keyword" errors instead of an
  internal `MethodError`; one-character strings are accepted where a `Char`
  is meant; `quotestyle="all"`, `missingstring=nothing`.
- Multi-byte delimiters (`delim="::"`) write, on both render paths, and
  round-trip through the reader.
- `CSV.write(sink, ::Chunks)` streams every batch under one header (it wrote
  a nonsense one-row table before); `partition=true` unchanged.
- `write(sink; kw...)` curried form (`table |> CSV.write(path)`) restored;
  `.GZ` counts as gzip; `write(::AbstractPath, t)` returns the path.

### 1.7 Performance (C1/W2, C2/W3, C4, C7, C3)

| cliff | before | after | how |
|---|---:|---:|---|
| `Chunks(src)` construction, 45 MiB | 2 040 ms (55× `File`) | 275 ms | per-column barrier in `schemamissing` (20 allocs/row → 0); O(log chunks) row location in `sampletypes` (was O(rows × chunks): 50 s at 46k chunks) |
| `types=Dict(:col => Float32)` | 536 ms (12× default) | 26 ms | `_narrowcol` barrier |
| all-short-rows file, 1M rows | 139 ms, 787 MiB | 49 ms, 335 MiB | `wantsproblem` — never format a message the cap will drop |
| `File(100 paths)` string chaining | 670 ms | 263 ms | `_chainstrings!` barrier |
| `stringtype=InlineString` on 200 B strings | 8.8 s (86× default) | = `stringtype=String` | auto width stops at `String31` (0.10's rule), longer text ⇒ `String` |

The remaining Chunks cost is `sampletypes(nsample=total)` — a full detect
cascade on every row (214 ms here); §3 says how to retire it.

### 1.8 Consolidation

`pushrowproblem!` is the single home for short/long-row reports (three
drivers had the text inlined); `ParseError` replaces three copies of the
message assembly.

### 1.9 Verification

Test files touched: `test/kernel.jl` (new `CompactString: construction,
mutation, regex, output` testset), `test/api.jl` (two new testsets, ~90
assertions), `test/write.jl` (one new testset), `test/scan.jl` (Tables API
migration), `test/legacy/harness.jl` (`ParseError` counts as a parse error).
Full `Pkg.test` after every change: **1,676,501 passed, 0 failed** (Julia
1.12.6, 8 threads, 4m50s; the legacy differential ledger is unchanged — the
same pinned `differ`/`new_errors`/`old_errors` sets as before the pass).

## 2. Decisions for the maintainer

Ordered by how much they change if left until after 1.0.

### 2.1 Own the string bytes (recommended before 1.0)

Today a `CompactString` cell > 12 bytes views the input buffer (an mmap for
files ≥ 512 KiB). Consequences reproduced by the reviews:

- **Crash.** `df = CSV.read(p, DataFrame); …; CSV.write(p, other); df.s[end]`
  → `Bus error: 10` when the rewrite shrank the file (reader-ergonomics R2,
  `b11_mmap.jl`). Overwriting with a same-size file silently changes values
  in the live table. Inline values (≤ 12 B) are unaffected, so it is
  intermittent — the worst kind.
- **Retention.** A single 13-byte value, a 10-row slice, `unique`, a `Dict`
  key, or `serialize`/`deepcopy` of any of them pins the whole file
  (`summarysize(df[1:10, :])` = 72.9 MiB; `serialize` ships the file per
  message under Distributed). `select=[:a]` on a 10 GiB file retains 10 GiB.
- **2 GiB cliff.** `view_payload`'s Int32 offset is absolute in the input, so
  any long string past byte 2³¹ throws mid-parse (kernel-warts W1), and
  `stringtype=String` — the obvious escape — fails the same way because
  materialization runs after the view parse.
- `Vector{UInt8}` inputs are adopted, not copied: mutating the vector
  mutates the table.

The alternative is what polars, DuckDB, and Arrow do: copy long bytes into a
column-owned arena at parse time (the escaped-cell path already does exactly
this). Measured cost ([own-strings-measurement](hardening-2026-09-02/own-strings-measurement.md)):
≤ 7 % on ordinary files, 1.1–1.5× on files that are *nothing but* 13–20-byte
strings, 2× on 100 KB cells (pure memcpy + page faults). What it buys: no
crash class, memory = columns not file, per-buffer (or with the multi-buffer
layout, no) 2 GiB limit, `Vector{UInt8}` inputs stop aliasing, and it is the
layout CompactStrings.jl was scaffolded around. The mutable-vector work in
§1.1 already uses the arena for assigned values, so the design is half in.

Recommendation: own by default; if the zero-copy parse speed matters for a
read-only pipeline, offer it as an explicit opt-in on `File` (a keyword such
as `zerocopy=true`) with the caveats documented — never as the default. If
you keep views as the default, at minimum: `buffer_in_memory=true` whenever
a `CompactString` column will be retained from a mapped file (turns the
crash into memory), a clear up-front `ArgumentError` for > 2 GiB inputs
instead of a `CompositeException` 1.3 s in, and custom
`serialize`/`deepcopy_internal` for `CompactString`.

### 2.2 `on_error` default: `:collect` vs `:warn`

`CSV.read(path, DataFrame)` on ragged rows, invalid values under `types=`, a
stray `"` mid-field (a 2 001-row file became 1 001 rows), or an unclosed
quote at EOF prints nothing (R3, G4). 0.10 warned (and was fatal on the
unclosed quote); polars errors; readr prints one summary line. The mechanism
now exists (`on_error=:warn`, one line, never per problem). My
recommendation is `:warn` as the default for `File` and `read` — "errors are
data" is right, but data nobody sees is data loss — with `:collect` for
silence and `:error` to throw. One constant flips it; the tests pin
`:collect` today. Also worth deciding: whether `:unclosed_quote` and "bare
quote engaged structural protection" (the two kinds that discard *rows*)
should escalate to `:error` by default, as 0.10 and polars do.

### 2.3 Bool spellings

Only lowercase `true`/`false` infer `Bool`. `TRUE/FALSE` (R, Excel),
`True/False` (pandas), `T/F` come back as `String` with no signal (G2, R17).
0.10's default was `["true","True","TRUE","T","1"]`. The strictness argument
holds for `1/0` and `T/F`; it does not for case variants. Recommendation:
default `truestrings=["true","True","TRUE"]` / `falsestrings=["false",
"False","FALSE"]`, accept per-column `Dict`s (the `_resolvekeys` plumbing
exists), and put the change on the migration guide's first screen either way.

### 2.4 Temporal edges

- `2024-01-01 00:00:00` (space, not `T`) is the most common non-ISO
  timestamp spelling (every DB dump); it infers as `String` (G5). Adding the
  space form to the default lattice is a change to `parseiso19` and a second
  default `DateTime` pattern (the `ValueOpts` field set), contained.
- Fractional seconds beyond milliseconds are **silently truncated** (`…
  .123456789` → `.123`, no problem recorded — R6, W10). 0.10 kept the
  string. Either record a `:precision_loss` problem or keep the column
  `String`; silent rounding is the one option nobody expects.
- A user `dateformat="yyyy-mm-dd"` rejects `2020-1-1` (W9): the kernel makes
  every ≥ 2-letter run fixed-width; Dates and 0.10 only when a token abuts
  another token. Adopt Dates' adjacency rule for user formats. The *default*
  ISO strictness (`2024-1-5`, `10:00` → String; R16) is a defensible 1.0
  delta to document.

### 2.5 Module flatten before 1.0 (RESPONSES #20, ordered after Parsers)

Every reviewer independently listed the leaks: `typeof(f) ==
CSV.CSVApi.File`, `CSV.CSVKernel.CompactString` in every schema and error,
`CSV.CSVApi.FileRow`/`Row`, `CSV.KernelWrite.RowWriter{…}`, docstring
signatures `CSVApi.File(...)`, `Base.parentmodule(@__MODULE__).KernelScan`,
`invoke(File, Tuple{Any}, …)`. `show` methods now print public names, but
`typeof`, `summary`, `Tables.schema`, stack traces, and DataFrames' column
headers (`CompactS…`) cannot be fixed without the flatten. It is mechanical
(the tests reach in as `CSV.CSVKernel.*`/`CSVApi.*`, which is the churn) and
it is the kind of thing that cannot change after 1.0. I would do it before
the Parsers 3.0 integration rather than after.

### 2.6 Rows/Chunks keyword parity and names

- `select`/`drop` are refused on `Rows` and `Chunks` (0.10 had them; the
  README calls projection "free") — R9, A R2, G8. `typemap`, `downcast`,
  `strict`/`on_error`, `maxwarnings` likewise. Rows/lazy typed-access
  failures become `missing` with no diagnostic channel (R7, K7).
- `Chunks(ntasks=)` still means "number of batches" (audit B3): rename to
  `nchunks`, add `chunkrows=`, keep `chunkbytes`.
- `CSV.lazy` (a verb) beside `File`/`Rows`/`Chunks` (constructors); `lf[1]`
  is a column where `f[1]` is a row (R18). A zero-cost `CSV.LazyFile(src;
  kw...)` constructor would make all four readers constructors.
- Two spellings per concept (`strict`/`on_error`, `maxwarnings`/`maxproblems`,
  `ntasks`/`parallel`) are accepted inconsistently across doors (K1). Pick
  the public one per concept and move the other to `_LEGACYKW`.
- `transpose=true` silently ignores `pool`, `stringtype`, `strict`,
  `on_error`, `ntasks`, `maxproblems` and never records a problem (R3, A R3).

### 2.7 Tables.jl Scan dependency

`scan=` compiles only against the unreleased `jq/scan`; the API moved under
CSV twice in three weeks. Pin the CSV-side calls to a released Tables
(`Tables = "1.14"` when it ships) and add a CI lane that installs the branch
until then, or the door will keep breaking silently (the conditional include
hides it).

### 2.8 `sniff` public?

The usecase-gaps reviewer disagrees with keeping `sniff`/`Spec` internal:
DuckDB's `sniff_csv` (which prints a ready-to-run `read_csv(...)` call) is
the best "why did my file parse wrong" tool in the field, and the machinery
exists. A `CSV.sniff(src) -> Spec` whose `show` prints the replayable
`CSV.File(src; delim=';', header=true, …)` call would cost a docstring.

### 2.9 Still-pending audit items

`missingstring=nothing` no longer yields `""` for empties (audit B1);
exhausted `IO` gives a silent empty table (A6); gzip/IO sources are read
fully into memory pending `StreamedSource` (B2).

## 3. Backlog: correctness and robustness

| id | sev | item | source |
|---|---|---|---|
| C0 | blocker | > 2 GiB files throw `InexactError: trunc(Int32, …)` from the float decomposer's absolute `digstart::Int32` (`values.jl:186`) whenever a float cell falls off the 15-byte fast path — deterministic when the last column is a float. Make it `Int64` or span-relative. | perf C0 |
| W1 | blocker | > 2 GiB with a long string past 2³¹ (see §2.1). Until then: up-front `ArgumentError` naming `stringtype=String`, and a materializing builder for `stringtype=String`. | kernel W1 |
| W4 | major | `strict=true` does not throw for narrow-type overflow (`types=Int8`, value 300); those problems bypass `maxproblems` and carry `pos=0`. Parse narrow ints natively in the kernel (`parsevalue(::Type{Int8}) = parseint64 + range check`), which also deletes `_narrowtypes`. | kernel W3/W4 |
| W6 | major | `Float32`/`Float16` double-round through `Float64` (`"16777217.000000001"`, `"3.4028235677973366e38"` → `Inf`; 0.10 shares it). Native Float32 tiers (Parsers 3 already has them). | kernel W6 |
| W8 | major | Non-parity-clean dialects (`escapechar='\\'`, asymmetric quotes) plan the whole file as ONE chunk: single-task, scalar, and the 1 GiB tape bound applies to the file. Reuse the comment-dialect row-walk planner (one condition at `chunkplan`). | kernel W8, perf C10 |
| R2 | major | mmap aliasing / SIGBUS (§2.1). | reader R2 |
| I5 | major | The input buffer stays alive after `df = nothing; GC.gc()` until the next parallel parse: spawned closures capture `buf` and Julia roots the last finished task per thread. Capture a clearable holder; `WeakRef` for the prefetch touchers; add a 2-thread `WeakRef` test. | interop I5 |
| I6 | major | `serialize`/`deepcopy`/`summarysize` of a slice carry the whole input; no public detach idiom beyond `String.(col)` (`CSV.materialize` is now bound). Custom `serialize`/`deepcopy_internal` for `CompactString`(`Vector`). Moot under §2.1. | interop I6 |
| W11 | minor | `errormonitor(Threads.@spawn …)` inside `@sync` (18 sites) double-reports every task failure. | kernel W11 |
| W15 | minor | Custom-type cells cost 4.5 µs each (`hasmethod` per cell + two allocations + try/catch). Decide the parse function once per column. | kernel W15 |
| W17 | minor | `EMPTY_BYTES` is a shared mutable global handed out as a "scratch" by `_scratchfor` when `groupmark == 0` — one guard away from corruption. | kernel W17 |
| W19 | minor | `Problem.pos` is undocumented as "offset into the bytes parsed" (post-gzip); no line number; `excerpt` can cut a multibyte character. Add a lazily computed `line`, clip at `thisind`. | kernel W19, reader R8 |
| W20 | minor | `write(delim="::")` now works; `Dialect` and the writer should agree on every dialect axis (`openquotechar` etc. do). | kernel W20 |
| W21 | done | submodule exports no longer leak (`CSV.index`). | kernel W21 |
| W22–26 | cosmetic | negative-shift comment, `LazyColumn.hint` unsynchronized write, `Tuple{Any,Int}` typing, gratuitous `Int32(clen)`. | kernel |
| K4 | minor | `header=[:a,:b]` for a 3-column file records one `long_row` per row and silently drops column 3; one header-level diagnostic (or error) would be clearer. `header=5` past EOF and an exhausted IO are silent `0 × 0`. | api K4, reader R15 |
| K14 | minor | `Vector{Problem}` is not `Tables.istable` (DataFrame(problems) works via the struct fallback); a summary `show` for the vector (counts by kind) and aggregation of identical structural problems (10 000 identical `short_row`s today). | api K14, reader R8 |
| dead | cleanup | `_cellhash`/`_splitmix64`, `materialize(::PooledColumn)`, `_widenmissing(::PooledColumn)`, `poollevels`, test-only doors `E.read/batches/rows/headername` (and `E.headername` has drifted from `parseheader!`), `KernelScan.scan`, stale comments (`PooledColumn` docstring, orphan `# the effective (ratio, cap)…`, `probe_float_anomaly.jl`, kernel-README `test.jl`). | kernel "Dead code" |
| C1–C18 | cleanup | eighteen duplicated-logic sites listed in kernel-warts (BOM test ×4, `Symbol("Column", j)` ×9, `_DIALECTKW` defined twice with different contents, `finalizecolumn` 3/4-arg pairs, two heap reservoirs, two escape-walk loops, `_writecell` vs `_appendcell!` quoting policies). | kernel "Consolidation" |

## 4. Backlog: performance

| id | item | measured | fix sketch |
|---|---|---|---|
| C5 | `limit=N` still indexes the whole file: 1 GiB `limit=10` = 188 ms and 2.1 GiB allocated (0.10: 0.3 ms). Same for `Rows` head, `lazy`, `Chunks`. | 700× at 1 GiB | index chunks lazily in file order when `limit` is set (chunk 1 starts outside quotes — no parity prescan needed for a prefix) and stop at `limit` rows |
| C6 | `skipto`/`footerskip` re-walk the file serially although the index has every row start: at 1 GiB each costs more than the full parse (1.3–1.7 s vs 0.73 s). | 1.8–2.3× a full parse | derive both offsets from `rowstartrel` per chunk (audit B10) |
| C1 rest | `Chunks` still runs `sampletypes(nsample=total)` — the full detect cascade on every row. | 214 ms at 45 MiB | run the fused driver once in a types-only mode (it resolves promotions in 31 ms), or cap at the stratified default and widen retroactively |
| C8 | `RowWriter` renders through an `IOBuffer` + `string(x)` per cell: 61 allocations per row, 4.6× slower than 0.10 (2.66 s vs 0.58 s per 1M rows) while the bulk writer does the same rows in 33 ms. | 4.6× vs 0.10 | render each row with `_appendcell!`/`_writerow!` into a reusable byte vector; take the schema once |
| C9 | Writer Tuple path compiles ~0.3 s per new ≤ 32-column schema (6.6 s CPU at 31–32 columns; 32 → 33 columns is a 838 → 51 ms discontinuity) for ≤ 1.5× steady-state gain. | TTFX | lower `TUPLE_RENDER_MAXCOLS` (8–12) or gate the Tuple path on `nrows ≥ ~100k`; specialize on eltypes only |
| C10 | `escapechar='\\'` / asymmetric quotes: 7.6× slower than the default dialect and 3× slower than 0.10 (serial + scalar). `comment=` 3.6× (scalar). | 3–8× | W8 planner change recovers the parallelism; a backslash-aware parity (quote preceded by an odd run of `\` is not a toggle) recovers the vector path; a comment-aware vector scanner for `comment=` |
| C12 | Wide files: throughput drops 23× from 1 000 to 50 000 columns and `parallel=false` beats parallel above ~20 000 columns (200 allocations per column, `promolock` twice per (column × chunk)). | parity with 0.10 | batch columns per chunk task; lock-free read of `promo[j]` when `userprovided[j]`; larger chunks when `ncols` is large |
| C11 | `pool=true` on a high-cardinality column interns and keeps 1M levels (17× the parse; parity with 0.10); `pool=true` on 5 000 string columns spawns a task + fresh `Dict` per column (5×). | footgun | docs; skip the parallel split under a few k rows per column; reuse a thread-local intern table |
| C13 | `Rows(types=…)` typed cells are a dynamic dispatch + box each (3.3× untyped; still 50× faster than 0.10). | minor | carry the schema in the `Rows`/`Row` type |
| C14 | Writer `floatformat`/`dateformat`/`Time`/`Symbol` cells allocate a `String` each. | 20× default on `dateformat` | `Printf.format(io, …)` / `Dates.format(io, …)` into the byte buffer; a direct `Time` renderer |
| I11/I12 | View-string per-op costs: `length` 3.6×, `startswith` 3.6×, `split` 7× vs `String`; `stringtype=InlineString` read 5.6× slower than `String` (InlineStrings' wide pointer constructor + `Ref` scratch). | minor | pointer-based `startswith`/`endswith`, ASCII fast path for `length`, `memchr` `findnext`; build `StringN` from the payload words |
| TTFX | Each new driver-kwarg combination re-specializes 200–480 ms (`select`, `limit`, `missingstring`, `dateformat`, `comment`); 47 variants = 5.8 s total (0.10: 130–570 ms each). | TTFX | extend the precompile workload to those keywords |
| G5 | Arena/own-strings cost table in §2.1 if adopted. | | |

## 5. Backlog: use-case gaps vs pandas / polars / DuckDB / readr / fread

Verified against the branch and the competitors' primary docs (citations in
[usecase-gaps](hardening-2026-09-02/usecase-gaps.md)). Ordered by adoption impact.

1. **Bad-line policy.** `on_error` lacks `:skip` (pandas `on_bad_lines="skip"`,
   Spark DROPMALFORMED, DuckDB `ignore_errors`) — the index knows every row
   boundary, so it is a row mask. `Problem` lacks the raw row text (a
   `CompactString` view is free), the column *name*, the physical line, and
   the source file (DuckDB's `reject_errors` has all four).
2. **Encodings and compression.** No `encoding=`; Latin-1 silently yields
   invalid UTF-8 cells; a UTF-16 BOM file (Excel "Unicode Text") dies with
   `Symbol name may not contain \0`; zstd gives the same error; bz2/zip parse
   as garbage. Built-in `:latin1`/`:utf16`, magic-byte detection with an
   actionable error, and a decoder extension seam (CodecZstd/Bzip2/Xz/
   ZipArchives) the way CodecZlib does gzip today; `compress=:zstd` on write.
3. **Streaming.** gzip/IO sources are whole-buffer; `Rows`/`Chunks` over a
   20 GB `.csv.gz` on a 16 GB box regress from "works in 0.10" to OOM.
   `docs/review/streamed-source-plan.md` is the design; ship it or caveat it
   on the first docs page.
4. **Per-column `missingstring`** (pandas `na_values` dict, polars
   `null_values` dict); a `CSV.NA_STRINGS` constant (pandas' 19-entry list)
   so `missingstring=CSV.NA_STRINGS` is the one-liner without widening the
   default (`NA`, `NULL`, `N/A`, `null`, `-` all come back as `String`).
5. **Selection ergonomics.** `select=r"^x"` and `select=[:a => :new]` raise
   bare `MethodError`s (Scan handles both); no `drop=[:end]`/negative index,
   `offset=`, `skipto="pattern"`/`skipto=r"…"`, `rownumber=:idx` (10 lines —
   the tape knows physical lines), glob/directory input, `source=true`,
   `header=:auto` (the sniffer already computes it).
6. **Writer.** A string cell spelled like `missingstring` cannot round-trip
   (the reader treats a quoted sentinel as missing too; 0.10 identical) —
   document. No `quotestyle=:nonnumeric` (Python `QUOTE_NONNUMERIC`, polars
   `non_numeric`), no `datetimeformat`/`timeformat` (one `dateformat` for all
   three, and `"HH:MM"` on a `Date` column is a `MethodError` — W5), no
   `float_precision` shorthand, no `select=` on write, `nothing` written as
   the text `nothing`, `BigFloat` round-trips as `1.0e+40` vs `1.0e40`.
7. **Excel-Europe files.** `decimal=:auto` when `;` is sniffed (fread's
   `dec="auto"`, polars `decimal_comma`); `CSV.EXCEL_ERRORS` constant.
8. **fread's `text=`.** `CSV.File(["a,b", "1,2"])` — a vector of lines when
   no element is an existing file — is a common notebook idiom; the current
   "wrap literal data in IOBuffer" error is clear, so this is a coin flip.

Where the rewrite is ahead of every competitor surveyed (worth naming in the
docs): deterministic parallel chunking, sample-independent inference,
problems as data with byte offsets, exact threaded `limit`/`skipto`, the
Int128 lattice, zero-allocation `CompactString` columns with `String`-parity
hashing, `CSV.lazy`, Scan pushdown with the two-phase masked filter,
stable-schema `Chunks`, by-name multi-file union with `source=`, `tryparse`
custom types inside the monomorphic loop, the writer's speed and byte
determinism, quoted-empty vs missing round trip, and ~1 ms cold start.

## 6. Docs, CI, repository hygiene

- `docs/src/reading.md` and `index.md` are the 0.10 manual; the
  api-consistency report lists the ten statements that are now false
  (`f[:col]` now works again, but `PosLenString`, `buffer_in_memory`
  tempfile semantics, `silencewarnings`, `rows_to_check`, function-typed
  `select`, `CSV.detect`, `Row2`, `reusebuffer`, `Chunks` re-inference are all
  gone). `docs/src/writing.md` is two `@docs` lines. The docs wave is gated
  on your call; the migration-guide skeleton in usecase-gaps §8 (12 items,
  0.10 defaults verified from `legacy/src/`) is a start.
- `.github/workflows/ci.yml` is 0.10's: tests Julia 1.6 (compat says 1.10),
  no LegacyCSV instantiate step, and `test/Manifest.toml` is not tracked
  while the test env `dev`s `~/.julia/dev/Tables` by absolute path — CI cannot
  resolve it. `README.md` says "tested against Julia 1.0".
- `legacy/` (0.10 source, 656 KB) ships inside the package directory; it is
  only needed by the test oracle (`test/LegacyCSV`). Move or exclude before
  registration. `test/legacy/testfiles` is 18 MB of tracked corpus (a
  deliberate decision, recorded).
- No `NEWS.md`/`CHANGELOG`.

## 7. Method notes

- Every finding above was reproduced by a script; the six reports quote the
  output. Probe scripts lived in the session scratchpad and were not
  retained; the reports are self-contained.
- The reviewers ran concurrently on one machine, so absolute timings in the
  perf report are noisy; every cliff is an in-process ratio confirmed in a
  second run. The arena A/B is interleaved best-of-3×3 in one process.
- Two things this pass did **not** do, deliberately: change any default
  (§2.2, §2.3), and touch the value layer's inference lattice (§2.4).
