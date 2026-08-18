# CSV.jl open-issue audit against the 1.0 rewrite (branch `kernel-proveout`)

Date: 2026-08-18. Scope: all **114 open issues** and **9 open PRs** on JuliaData/CSV.jl (fetched with `gh`, raw JSON in `scratchpad/csv-issues/{issues,prs}.json`).
Rewrite examined: `~/.julia/dev/CSV-kernel-proveout` @ `891df6a` (src/api.jl, core.jl, values.jl, write.jl, scan.jl; docs/kernel-README.md; test/legacy/AUDIT.md; the local ISSUE-*.md triage notes under ~/.julia/dev/CSV).

**Method.** Every issue was read (body + last comments) and mapped to a mechanism in the rewrite. Wherever the answer was not obvious from code, I *ran the rewrite* (Julia 1.12.6, 8 threads, `--project=.` and `--project=test` for side-by-side against the 0.10 oracle `LegacyCSV`) — 60+ probes in `scratchpad/csv-issues/probe*.jl`, outputs in `probe*.out`. "verified" in the tables below means the reproduction was executed today. I also tried to load the package under **Julia 1.13.0-rc1** (`+1.13`).

## Headline findings (things the rewrite does NOT yet handle — found while auditing)

1. **Julia 1.13 blocker (#1164, PR #1165).** The rewrite fails to precompile on 1.13.0-rc1: `UndefVarError: memhash_seed not defined in Base` at `src/core.jl:1877` in `hash(::CompactString)` (hit by the precompile workload's `Dict{CompactString,UInt32}`). `Base.memhash`/`memhash_seed` are gone in 1.13; `hash(::String)` there is `hash_bytes(pointer(s), sizeof(s), UInt64(h), Base.HASH_SECRET) % UInt`. Needs a `@static if isdefined(Base, :hash_bytes)` branch (and a 1.13/nightly CI lane).
2. **`CSV.read(source, sink)` regression.** 0.10 did `Tables.CopiedColumns(File(...)) |> sink`; the rewrite does `Tables.materializer(sink)(File(...))`, and `Tables.materializer(::Function)` falls back to `columntable`. Verified: `CSV.read(io, Tables.matrix)`, `CSV.read(io, Tables.rowtable)`, `CSV.read(io, x->42)` all silently return a NamedTuple (0.10 returns a Matrix / Vector{NamedTuple} / 42). Affects #1030, #941 and any function/type sink without a materializer.
3. **`header=false` + `skipto=N` takes the column count from row 1, not the first data row.** Verified: `"foo\nbar\n1\t2\n3\t4"` with `header=false, skipto=3, delim='\t'` → 0.10 gives 2 columns; rewrite gives 1 column + `long_row` Problems (silent data loss). `_prepare` derives `ColumnN` names before `_skiptobyte!` runs.
4. **Bare quotes in skipped prefix rows still kill the parse (#1012, #1079; also the spirit of #1160).** Verified: `header=2, skipto=3` with a `1'2"` first row → 0 columns; `header=false, skipto=3` with `11.0"` in row 2 → 0 rows. Prefix rows are walked quote-aware and (for skipto) are still inside the index. Fix: walk prefix rows quote-blind (physical lines, = polars `skip_lines`) and shift `datastart` past them, as numbered headers already do.
5. **Writer is slower than 0.10 despite parallel rendering.** Measured (rewrite 8 threads vs 0.10 1 thread): 1e6×4 mixed 0.44 s vs 0.30 s (1.5 GB vs 0.48 GB allocated); 2000×1000 Float64 0.351 s vs 0.065 s (5.4×); 2e6×2 Int 0.205 s vs 0.139 s. Causes visible in write.jl: `string(x)` per Float/Date cell, `AbstractVector[]` container ⇒ two dynamic dispatches per cell, `reduce(vcat, blocks)` (whole output resident twice → #1087). Touches #947, #800, #751, #1087, #1138.
6. **Two capability removals that 0.10 users rely on** (pinned 1.0 deltas — decisions, not bugs): (a) `missingstring=nothing` no longer turns empty cells into `""` (verified vs 0.10) — #1107/#1120's only escape hatch; (b) gzip files and IO sources are decompressed/read fully into memory (verified 104 MB gz → 723 MB allocated; Rows over IO holds the entire buffer) — 0.10 spooled to a tempfile + mmap, so larger-than-RAM `.gz`/zip inputs via `Rows`/`Chunks` worked (#997, #988).

Everything else in the open backlog is either fixed/obsoleted by the rewrite (66 of 114) or a feature request that can be deferred or declined with a rationale.

## Issue table

| # | title | age | class | reason |
|---|---|---|---|---|
| [#380](https://github.com/JuliaData/CSV.jl/issues/380) | Add a types header | 7.6y | QUESTION/SUPPORT | won't-fix: Julia-specific types row is not CSV's job; `header=1, skipto=3, types=` already reads such files. Close. |
| [#492](https://github.com/JuliaData/CSV.jl/issues/492) | CSV.write - floats | 7.0y | FIXED-BY-REWRITE | writer `floatformat="%.8f"` (printf-style) applied to every AbstractFloat cell. |
| [#506](https://github.com/JuliaData/CSV.jl/issues/506) | Read CSV directly from URL | 6.9y | STILL-OPEN-FEATURE | `resolvesource(::AbstractString)` errors `no file at "https://…"`; a Downloads-stdlib branch (`startswith(s, r"https?://")` → download to bytes) is ~10 lines, zero deps. |
| [#522](https://github.com/JuliaData/CSV.jl/issues/522) | Allow user-provided row and field error callback functions for custom error handling | 6.8y | FIXED-BY-REWRITE | problems-as-data: every bad cell/row becomes a `Problem(row, col, byteoffset, kind, excerpt)`; imports never abort under `on_error=:collect`. (Callbacks per se are not offered; the goal is met.) |
| [#601](https://github.com/JuliaData/CSV.jl/issues/601) | Add option for FWF reader and writer | 6.3y | QUESTION/SUPPORT | won't-fix: fixed-width reader/writer is a separate package's scope. Close. |
| [#701](https://github.com/JuliaData/CSV.jl/issues/701) | Using select with explicit header names requires all the column names to be specified | 6.1y | STILL-OPEN-FEATURE | unchanged (verified: `header=["b","c"], select=[2,3]` → `select/drop index out of range`, at least a clean error now). Tables.Scan `select=[2=>:b, 3=>:c]` covers it; binding `header` names post-select is a small API-layer change. |
| [#739](https://github.com/JuliaData/CSV.jl/issues/739) | strict=true | 5.9y | FIXED-BY-REWRITE | `strict=true` ⇒ `on_error=:error`, which escalates EVERY problem incl. short/long rows (verified: strict=true errors on `long_row`). |
| [#751](https://github.com/JuliaData/CSV.jl/issues/751) | Adapt multithreaded CSV write from R data.table | 5.9y | FIXED-BY-REWRITE | writer renders row blocks in parallel (byte-identical at any thread count) — but measured net SLOWER than 0.10 single-thread; see writer-perf item in 'Worth doing'. |
| [#793](https://github.com/JuliaData/CSV.jl/issues/793) | Don't warn on dropped last column with trailing delimiter | 5.7y | STILL-OPEN-FEATURE | warnings are gone, but a trailing delimiter with N user names still records one `long_row` Problem per row (verified; strict=true errors). A 'single trailing empty field is not a long row' tolerance would be small; workaround header with N+1 names + drop. |
| [#800](https://github.com/JuliaData/CSV.jl/issues/800) | Improve performance of writing Time | 5.6y | STILL-OPEN-BUG | writer still does `string(x)` per Dates cell (and per Float cell): verified ~42 bytes allocated per output byte; part of the writer-perf item. |
| [#840](https://github.com/JuliaData/CSV.jl/issues/840) | Feature request: Additional header parsing control | 5.2y | STILL-OPEN-FEATURE | `# name1,name2` header still needs `header=false`+names (comment lines are dropped before header parsing). Low demand; won't-fix for 1.0. |
| [#841](https://github.com/JuliaData/CSV.jl/issues/841) | Yet another feature request: Chunks but for groups | 5.2y | STILL-OPEN-FEATURE | group-wise Chunks not implemented; would fit (index knows row starts; one column scan finds boundaries). 1.x. |
| [#844](https://github.com/JuliaData/CSV.jl/issues/844) | optional argument specifying initializers for missing values | 5.2y | STILL-OPEN-FEATURE | per-column fill values for missing not implemented; post-read `coalesce`/Scan does it. Won't-fix or 1.x. |
| [#853](https://github.com/JuliaData/CSV.jl/issues/853) | Feature request: Auto-detect for repeated space delimited file | 5.1y | STILL-OPEN-FEATURE | auto-detect + `ignorerepeated` is still refused (`_prepare` throws). Adding a `(' ', ignorerepeated=true)` candidate to `_detectdelim`'s consistency scorer is contained; aligned-column scientific dumps are common. |
| [#857](https://github.com/JuliaData/CSV.jl/issues/857) | Relax type restrictions on delim | 5.1y | OBSOLETE | multi-BYTE String delims are supported (scalar scanner); a SET/regex of delimiters is not (see #956). Custom AbstractChar delims won't-fix. |
| [#890](https://github.com/JuliaData/CSV.jl/issues/890) | How can I delete a file after reading its content with CSV.Rows ?  | 4.9y | NEEDS-VERIFICATION | reporter confirms fixed in 0.10.15. Rewrite mmaps files ≥512 KiB and Rows cells are lazy views into the map, so the mapping lives until GC: on Windows `rm` right after a Rows loop needs `GC.gc()` or `buffer_in_memory=true` — document. |
| [#935](https://github.com/JuliaData/CSV.jl/issues/935) | Unable to parse really long CSV cell (breaks Parsers.jl) | 4.8y | FIXED-BY-REWRITE | Int32 field lengths, no Parsers `PosLen` (20-bit cap); verified 150k-char and 2 MiB quoted cells. |
| [#936](https://github.com/JuliaData/CSV.jl/issues/936) | Need a feature to handle an extra unit row | 4.8y | QUESTION/SUPPORT | `header=1, skipto=3` reads it; units row = a second 1-row read. Close. |
| [#941](https://github.com/JuliaData/CSV.jl/issues/941) | CSV.read not working properly with StructArray | 4.8y | QUESTION/SUPPORT | upstream (StructArrays needs a Tables materializer/constructor). NOTE the rewrite's `read(source, sink)` currently makes this worse (see sink-semantics regression in 'Worth doing'). |
| [#947](https://github.com/JuliaData/CSV.jl/issues/947) | Slow CSV writing | 4.7y | STILL-OPEN-BUG | PERF REGRESSION measured: 2000×1000 Float64 write 0.351 s (rewrite, 8 threads) vs 0.065 s (0.10, 1 thread); 1e6×4 mixed 0.44 vs 0.30 s with 3× the allocation. Writer-perf item. |
| [#954](https://github.com/JuliaData/CSV.jl/issues/954) | Reading multiple CSV files with CSV.read fails when one file is pooled | 4.7y | FIXED-BY-REWRITE | new `File(sources::Vector)` + `_chaincolumn` (by-name matching, promotion, owned String concat, PooledArray kept when any piece pooled). |
| [#956](https://github.com/JuliaData/CSV.jl/issues/956) | Feature request: Handle multiple, different delimiters in a file | 4.6y | STILL-OPEN-FEATURE | multiple distinct delimiters not supported; `delim=' ', ignorerepeated=true` covers the readdlm whitespace case. A delimiter-set scalar dialect is possible later; won't-fix for 1.0. |
| [#959](https://github.com/JuliaData/CSV.jl/issues/959) | Issue when reading file by chunks | 4.6y | FIXED-BY-REWRITE | skipto/limit are index arithmetic: verified 104 MB/3M rows, `skipto=2.9M, limit=1000` 0.107 s (0.10: tens of seconds, GBs allocated). Note skipto is a serial ~1 GB/s byte walk — could reuse the index's row starts (minor). |
| [#967](https://github.com/JuliaData/CSV.jl/issues/967) | Request: Mode in which quoted fields always remain as Strings | 4.6y | STILL-OPEN-FEATURE | verified quoted `"1"` still infers Int64. An opt-in flag is contained (detecttype: quoted⇒String; typed loops: quoted cell ⇒ conflict via cellcontent's quoted status). 1.x. |
| [#969](https://github.com/JuliaData/CSV.jl/issues/969) | Reading CSV hangs when Soss is loaded | 4.6y | OBSOLETE | Julia 1.7-era compile hang; entirely new code, no @generated kernels. Close. |
| [#970](https://github.com/JuliaData/CSV.jl/issues/970) | Allow lazy materialization of iterated CSV.Chunks | 4.6y | STILL-OPEN-FEATURE | Chunks still yields materialized tables; the design makes a lazy `Chunk` handle (buf + chunk-index range + schema) + `materialize` cheap. 1.x. |
| [#974](https://github.com/JuliaData/CSV.jl/issues/974) | First call of CSV.File very slow | 4.5y | FIXED-BY-REWRITE | PrecompileTools workload: first `File` 4.0 s → ~1 ms, fresh-process total 0.19 s (vs 0.10.16 1.04 s). |
| [#982](https://github.com/JuliaData/CSV.jl/issues/982) | PooledVectors for isbitstypes of small size? | 4.5y | OBSOLETE | different economics now: CompactString is 16 B, a pooled ref 4 B, parse-time pooling ~free; the (0.2, 500) policy is retained. Close (PR #983 too). |
| [#984](https://github.com/JuliaData/CSV.jl/issues/984) | Kernel dies sometimes when benchmark CSV.File() with 4 threads on Windows | 4.5y | NEEDS-VERIFICATION | old chunk-speculation threading code is gone (Julia 1.6.5-era report), but the rewrite has not run on Windows yet: one Windows multi-thread CI job, then close. |
| [#987](https://github.com/JuliaData/CSV.jl/issues/987) | Failed to precompile CSV | 4.5y | OBSOLETE | Julia 1.7.1 precompile crash; nothing in common. Close. |
| [#988](https://github.com/JuliaData/CSV.jl/issues/988) | option to specify alternative location for `mktemp` | 4.4y | OBSOLETE | no tempfile at all any more (gzip/IO decompress into memory) — the flip side is that big gz/IO sources are now fully in RAM (see #997 item). |
| [#990](https://github.com/JuliaData/CSV.jl/issues/990) | Select behaviour with normalizenames | 4.4y | STILL-OPEN-FEATURE | select/drop/types keys still resolve against NORMALIZED names; matching raw-or-normalized in `_resolveselect`/`_resolvekeys` is small. Do if cheap. |
| [#993](https://github.com/JuliaData/CSV.jl/issues/993) | Prettier (or custom) `normalizenames` behaviour? | 4.4y | STILL-OPEN-FEATURE | `normalizename` kept verbatim (tests pin it). 1.0 is the only free moment to change it; recommend won't-fix (post-rename). |
| [#995](https://github.com/JuliaData/CSV.jl/issues/995) | Installing on Mac M1 Pro - hanging | 4.4y | OBSOLETE | Julia 1.7 under Rosetta. Close. |
| [#997](https://github.com/JuliaData/CSV.jl/issues/997) | Memory Consumption of CSV.Rows with ZipFile | 4.4y | STILL-OPEN-BUG | design limitation: `resolvesource(io) = read(io)` — Rows over an IO holds the whole decompressed input (verified 2.78 MB source → 2.78 MB buffer; gzip file 104 MB → 723 MB allocated). 0.10 spooled to tempfile+mmap. Decide: restore tempfile spool for big non-file sources or document. |
| [#998](https://github.com/JuliaData/CSV.jl/issues/998) | Keyword argument consistency: missingstring -> missingstrings | 4.4y | OBSOLETE | `missingstring` kept (String or Vector); `missingstrings=` is now a hard error with a hint. Close. |
| [#1000](https://github.com/JuliaData/CSV.jl/issues/1000) | Improve error message when IO source exhausted | 4.3y | STILL-OPEN-BUG | minor: verified an exhausted IOBuffer now yields a silent empty 0-column File (was BoundsError). Throw ArgumentError when `read(io)` is empty and `position(io) > 0`. |
| [#1002](https://github.com/JuliaData/CSV.jl/issues/1002) | Undefined result when parsing tsv containing double quotes inside a field | 4.3y | FIXED-BY-REWRITE | no `#undef` possible; verified `"Cool" movie` cell → `missing` + `:invalid_quoted_field` Problem, `The "Real" Story` parses. Smoke the real IMDB tsv (see Verify). |
| [#1008](https://github.com/JuliaData/CSV.jl/issues/1008) | Numbers not detected with stripwhitespace | 4.2y | FIXED-BY-REWRITE | verified: `stripwhitespace=true` column c → `Union{Missing,Int64}` (typed detect trims blanks; whitespace-only ⇒ missing). |
| [#1011](https://github.com/JuliaData/CSV.jl/issues/1011) | Consider removing deprecated kwargs | 4.1y | FIXED-BY-REWRITE | all legacy kwargs are hard errors with hints; no `Context`. |
| [#1012](https://github.com/JuliaData/CSV.jl/issues/1012) | Can't skip rows with quote | 4.1y | STILL-OPEN-BUG | VERIFIED STILL BROKEN: `header=2, skipto=3` with a `1'2"` prefix row → 0 columns/0 rows. Prefix rows before header/skipto are walked quote-aware (`_rawrowoffset`/`nextrowstart`), so a bare quote swallows the file. Same fix as #1079. |
| [#1013](https://github.com/JuliaData/CSV.jl/issues/1013) | Ability to pass `UInt16` or `compress=true` for `PooledArray`s | 4.1y | STILL-OPEN-FEATURE | refs are always UInt32 (verified `PooledVector{String,UInt32}`); `PooledArrays.compress` post-hoc. 1.x. |
| [#1019](https://github.com/JuliaData/CSV.jl/issues/1019) | Bug when parsing complex CSV with multi-threading enabled | 4.0y | FIXED-BY-REWRITE | parity-composed deterministic chunking — no boundary speculation (verified 2000 quoted-multiline rows, 8 tasks, 512-byte chunks). |
| [#1027](https://github.com/JuliaData/CSV.jl/issues/1027) | Allow `Any` type in column | 3.9y | STILL-OPEN-FEATURE | verified `types=Any`/`Number` → `unsupported column type`. `Number` = a numeric-only lattice ceiling (contained, 1.x); `Any` doesn't fit a columnar parser (won't-fix). |
| [#1029](https://github.com/JuliaData/CSV.jl/issues/1029) | CSV.read `multithreadpostparse`: erroring  | 3.9y | FIXED-BY-REWRITE | `multithreadpostparse` is gone; verified `escapechar='\\', quotechar='\'', types=Union{String,Missing}, ntasks=2`. |
| [#1030](https://github.com/JuliaData/CSV.jl/issues/1030) | Please provide a way to sink to a standard Matrix without Tables or Dataframes | 3.9y | STILL-OPEN-FEATURE | verified `CSV.read(io, Matrix)` returns a NamedTuple (materializer fallback) — worse than erroring. Falls out of the sink-semantics fix + a `read(src, ::Type{<:AbstractMatrix}) = Tables.matrix(...)` line. |
| [#1031](https://github.com/JuliaData/CSV.jl/issues/1031) | Find a way to provide a way specify a row iterator via regex w/ named fields | 3.9y | QUESTION/SUPPORT | won't-fix (regex row parsing is out of scope). Close. |
| [#1032](https://github.com/JuliaData/CSV.jl/issues/1032) | Confusing error message when failing to parse date | 3.9y | FIXED-BY-REWRITE | message now `cannot parse Date from "04-10-2022"` (verified) instead of `INVALID: DELIMITED`; consider appending `(dateformat=…)` hint. |
| [#1040](https://github.com/JuliaData/CSV.jl/issues/1040) | Dates not automatically parsed if they can be integers | 3.8y | QUESTION/SUPPORT | docs: numeric detection still precedes temporal by design (verified Int64 with dateformat=yyyymmdd); port PR #1042's sentence into the new docs. |
| [#1044](https://github.com/JuliaData/CSV.jl/issues/1044) | CSV precompile error | 3.8y | OBSOLETE | CSV 0.5.23 era. Close. |
| [#1048](https://github.com/JuliaData/CSV.jl/issues/1048) | CSV.Chunks possible memory leak | 3.8y | FIXED-BY-REWRITE | no per-thread caches; verified maxrss flat (1215→1238 MB) iterating 20 chunks of a 104 MB file. |
| [#1049](https://github.com/JuliaData/CSV.jl/issues/1049) | CSV.Chunks API | 3.8y | STILL-OPEN-FEATURE | API naming: rewrite's `Chunks(ntasks=)` still means 'number of batches' (plus `chunkbytes`). 1.0 is the moment to rename (`nchunks`). |
| [#1054](https://github.com/JuliaData/CSV.jl/issues/1054) | Error parsing compressed file multi-thread | 3.7y | OBSOLETE | not reproducible; threading code entirely different. Close. |
| [#1055](https://github.com/JuliaData/CSV.jl/issues/1055) | TaskFailedException on joins  | 3.7y | OBSOLETE | by design + now diagnosable: bare quotes engage structural protection and produce Problems (not fatal TaskFailedException); `quoted=false` remains the answer for such files. Sniff-level quote detection = 1.x idea. |
| [#1059](https://github.com/JuliaData/CSV.jl/issues/1059) | Large csv file crashes Julia with a status access violation | 3.7y | NEEDS-VERIFICATION | old multithreaded code is gone, but no Windows run of the rewrite yet: Windows CI multi-thread job on a ≥1 GB file (mmap path), then close. |
| [#1061](https://github.com/JuliaData/CSV.jl/issues/1061) | Invalidations when loading DataFrames | 3.7y | OBSOLETE | DataStructures/SentinelArrays/WeakRefStrings/Parsers deps are gone (deps: Tables, Dates, Unicode, Mmap, PooledArrays, CodecZlib, Printf, PrecompileTools). |
| [#1062](https://github.com/JuliaData/CSV.jl/issues/1062) | Emit warning on nonsensical argument combinations to `write` | 3.7y | FIXED-BY-REWRITE | structural scalar quoting: verified `decimal=',', delim=','` writes `"1,23","4,56"` (valid CSV); delim==quote is an ArgumentError. |
| [#1063](https://github.com/JuliaData/CSV.jl/issues/1063) | Multiprocessing crash | 3.7y | OBSOLETE | no Parsers types inside File; verified serialize/deserialize round-trip of a File. Close. |
| [#1064](https://github.com/JuliaData/CSV.jl/issues/1064) | CSV.read error with limit on multiple threads | 3.7y | FIXED-BY-REWRITE | exact parallel `limit` (verified limit=1000, ntasks=4, pooled column). |
| [#1067](https://github.com/JuliaData/CSV.jl/issues/1067) | UndefVarError: writeshortest not defined | 3.6y | OBSOLETE | Parsers version mismatch; no Parsers dependency. Close. |
| [#1068](https://github.com/JuliaData/CSV.jl/issues/1068) | Parsing based on first row when select, header and skipto are provided | 3.6y | FIXED-BY-REWRITE | long rows never widen the schema: verified 3 columns `[1,1],[2,2],[3,3]` (+2 `long_row` Problems recorded). |
| [#1070](https://github.com/JuliaData/CSV.jl/issues/1070) | `CSV.io` is not defined | 3.6y | OBSOLETE | writer rewritten. Close. |
| [#1071](https://github.com/JuliaData/CSV.jl/issues/1071) | CSV.File breaks with multiple input CSVs | 3.6y | FIXED-BY-REWRITE | #954 family (new multi-source chaining). |
| [#1074](https://github.com/JuliaData/CSV.jl/issues/1074) | Reading large CSV files is slow/crashes | 3.5y | FIXED-BY-REWRITE | the crash (0.10 widened columns per row → OOM) can't happen: verified `# preamble` without `comment=` → 1 column + Problems. `comment="#"` takes the scalar scanner: 0.169 s vs 0.048 s per 104 MB (~600 MB/s, 3.5× slower than the vector path); select composes. Smoke a real 1 GB Stan file (Verify). |
| [#1075](https://github.com/JuliaData/CSV.jl/issues/1075) | Performance regression since v0.8.0 | 3.5y | NEEDS-VERIFICATION | rewrite; the File matrix beats 0.10 on every shape, but the reporter's `read(rows)` snippet is truncated (bisected to a Rows-era commit) — re-bench `CSV.Rows` over 1e6 rows vs 0.10 once, then close. |
| [#1076](https://github.com/JuliaData/CSV.jl/issues/1076) | `stripwhitespace=true` not removing trailing white space? | 3.4y | FIXED-BY-REWRITE | verified tab-delimited `stripwhitespace=true`: header `c  `→`c`, cell `chr1 `→`chr1`. |
| [#1079](https://github.com/JuliaData/CSV.jl/issues/1079) | skipto breaks if there is a quote in the skipped rows | 3.3y | STILL-OPEN-BUG | VERIFIED STILL BROKEN (0.10 too): `header=false, skipto=3` with a bare `"` in row 2 → 0 rows + 1 Problem. Fix: walk prefix rows quote-blind (physical lines) and shift `datastart` past them (extend the numbered-header rule to skipto). |
| [#1082](https://github.com/JuliaData/CSV.jl/issues/1082) | big integers are parsed as Float64 | 3.3y | FIXED-BY-REWRITE | Int64→Int128→Float64 lattice; verified both objectid columns come back `Int128`. |
| [#1085](https://github.com/JuliaData/CSV.jl/issues/1085) | writeheader=true ineffective in combination with header= | 3.3y | FIXED-BY-REWRITE | verified `header=[...], append=true, writeheader=true` writes `x,y\n1,2`. |
| [#1086](https://github.com/JuliaData/CSV.jl/issues/1086) | Do not convert quoted cells | 3.3y | STILL-OPEN-FEATURE | duplicate of #967 (verified quoted numbers still infer Int). |
| [#1087](https://github.com/JuliaData/CSV.jl/issues/1087) | CSV.write should conditionally convert type unstable iterators | 3.3y | STILL-OPEN-BUG | row-iteration type instability is gone (writer uses Tables.columns), BUT the writer renders the whole output in memory (`reduce(vcat, blocks)`) — a 5 GB output needs ≥10 GB transient. Stream blocks to the sink (writer-perf item). |
| [#1090](https://github.com/JuliaData/CSV.jl/issues/1090) | pool kwarg documentation | 3.3y | QUESTION/SUPPORT | docs: `pool` docstring in the rewrite already says 'columns detected as String' incl. stringtype variants; fold into docs wave. |
| [#1091](https://github.com/JuliaData/CSV.jl/issues/1091) | There is no clear method reading non-UTF8 gzipped file in example | 3.2y | QUESTION/SUPPORT | docs: example for non-UTF-8 gzip (`transcode(GzipDecompressor, read(f))` → StringEncodings → IOBuffer). Docs wave. |
| [#1092](https://github.com/JuliaData/CSV.jl/issues/1092) | burntsushi's issue | 3.2y | OBSOLETE | the design answer: quote-parity is computed, not guessed (README §4) — burntsushi's objection to jump-and-recover does not apply. Close with pointer. |
| [#1096](https://github.com/JuliaData/CSV.jl/issues/1096) | Error reading CSV - missing lines | 3.2y | OBSOLETE | by design + diagnosable: verified a lone bare quote merges following rows into one field and records `bare quote engaged structural protection` + `unclosed_quote` Problems (0.10: silent). `quoted=false` gives all rows. Docs + maybe sniff-level quote scoring (1.x). |
| [#1102](https://github.com/JuliaData/CSV.jl/issues/1102) | Configurable max inline string length | 3.1y | OBSOLETE | CompactString: ≤12 B inline, longer = zero-copy view — no inline-length cap; InlineStrings widths via extension. Close. |
| [#1104](https://github.com/JuliaData/CSV.jl/issues/1104) | Formatting issues in examples | 3.1y | OBSOLETE | docs are being rewritten for 1.0. Close. |
| [#1105](https://github.com/JuliaData/CSV.jl/issues/1105) | Cannot compile this package on Julia 1.9.1 in Ubuntu 22.04 container | 3.1y | OBSOLETE | Julia 1.9.1 illegal instruction in a container; unrelated to current code. Close. |
| [#1107](https://github.com/JuliaData/CSV.jl/issues/1107) | "Missing" Values | 3.0y | STILL-OPEN-FEATURE | DECISION: 1.0 pins 'empty ⇒ missing always'; VERIFIED this removes 0.10's escape hatch (`missingstring=nothing` → `""` in 0.10, still `missing` in the rewrite). Either restore that meaning (small ValueOpts gate) or document loudly. |
| [#1111](https://github.com/JuliaData/CSV.jl/issues/1111) | CSV.write somehow cannot write file with name `con.csv` in Windows?! | 3.0y | QUESTION/SUPPORT | won't-fix: Windows reserved device name. Close. |
| [#1113](https://github.com/JuliaData/CSV.jl/issues/1113) | Segfault on Julia 1.9 on Intel Sapphire Rapids during precompilation | 2.9y | OBSOLETE | the Float16 line is gone; Julia ≥ 1.10 floor. Close. |
| [#1114](https://github.com/JuliaData/CSV.jl/issues/1114) | `bufsize` of `write` is defined to be length of row but actually cells | 2.7y | OBSOLETE | no `bufsize` kwarg in the new writer. Close. |
| [#1116](https://github.com/JuliaData/CSV.jl/issues/1116) | Formatting broken on Examples page in documentation | 2.7y | OBSOLETE | docs rewritten. Close. |
| [#1118](https://github.com/JuliaData/CSV.jl/issues/1118) | Error on CSV.read attempt | 2.7y | FIXED-BY-REWRITE | verified: no MethodError (0.10 still throws); the malformed cell becomes `missing` + `:invalid_quoted_field` Problem. Consider keeping raw bytes for String columns instead of missing. |
| [#1120](https://github.com/JuliaData/CSV.jl/issues/1120) | `emptyvalue` keyword option | 2.7y | STILL-OPEN-FEATURE | same decision as #1107 (`emptyvalue`); if `missingstring=nothing` is restored this is covered. |
| [#1122](https://github.com/JuliaData/CSV.jl/issues/1122) | CSV.Chunks splits file into uneven chunks | 2.5y | OBSOLETE | batches are byte-even and row-aligned (verified 10 batches: 11666/10278…/9487 rows) — document, close. |
| [#1129](https://github.com/JuliaData/CSV.jl/issues/1129) | CSV is failing PkgEval | 2.4y | OBSOLETE | old PkgEval failure; the 1.0 branch needs its own CI matrix anyway. Close. |
| [#1130](https://github.com/JuliaData/CSV.jl/issues/1130) | Error when combining single row with multiple row CSV file into a DataFrame  with pooling on.   | 2.4y | FIXED-BY-REWRITE | #954 family (verified one-row + multi-row sources with pooling). |
| [#1131](https://github.com/JuliaData/CSV.jl/issues/1131) | `Date` types should not be inferred from column | 2.4y | QUESTION/SUPPORT | won't-fix: `typemap=Dict(Date=>String)` or `types=` opts out. Close. |
| [#1135](https://github.com/JuliaData/CSV.jl/issues/1135) | 1.12.0-DEV.317 ERROR: LoadError: TypeError: in typeassert, expected Tuple{Vector{UInt8}, Int64, Int64, Union{Nothing, String}}, got a value of type Tuple{Memory{UInt8}, Int64, Int64, Nothing} | 2.4y | OBSOLETE | fixed in 0.10.x; `getsource` no longer exists. Close. |
| [#1136](https://github.com/JuliaData/CSV.jl/issues/1136) | Error when passing as `source` a vector with fewer unique elements than files. | 2.3y | FIXED-BY-REWRITE | verified `source="patients" => ["Alice","Alice"]` (one label per source, duplicates fine). |
| [#1138](https://github.com/JuliaData/CSV.jl/issues/1138) | CSV.write() with append=true allocating a lot of memory | 2.1y | FIXED-BY-REWRITE | no fixed 4 MiB buffer per call any more (tiny appends allocate tiny buffers) — but see writer-perf: per-cell allocations are now the cost. |
| [#1140](https://github.com/JuliaData/CSV.jl/issues/1140) | Cannot round-trip a file (read, write, read) in some circumstances | 2.0y | FIXED-BY-REWRITE | #1019 family + #1153 (`\r` quoted on write). Public 300 MB DCMS file is a good round-trip smoke (Verify). |
| [#1141](https://github.com/JuliaData/CSV.jl/issues/1141) | Segfault when reading into dataframe with a transpose | 2.0y | FIXED-BY-REWRITE | transpose is an API compat path with exact whole-column inference (verified promotion case); also fixed on main by #1191. |
| [#1142](https://github.com/JuliaData/CSV.jl/issues/1142) | Use PrettyTables.jl in CSV.File for a friendlier experience | 1.9y | STILL-OPEN-FEATURE | PrettyTables show not implemented; could be an extension. 1.x/won't-fix. |
| [#1143](https://github.com/JuliaData/CSV.jl/issues/1143) | CSV.jl fails to parse a file that DuckDB is fine with | 1.9y | FIXED-BY-REWRITE | chunk-boundary bug family (README §4; also #1194 on main). Smoke the public pirate_attacks.csv (Verify). |
| [#1145](https://github.com/JuliaData/CSV.jl/issues/1145) | ERROR: UndefVarError: `A` not defined in local scope | 1.8y | FIXED-BY-REWRITE | #954 family. |
| [#1146](https://github.com/JuliaData/CSV.jl/issues/1146) | source column-name missing when the input vector contains only one csv-file | 1.7y | FIXED-BY-REWRITE | verified `File([p]; source=:src)` adds the column (documented improvement over 0.10). |
| [#1147](https://github.com/JuliaData/CSV.jl/issues/1147) | Wishlist : import percent or currency formatted data | 1.7y | STILL-OPEN-FEATURE | won't-fix for 1.0: a `tryparse(::Type{Money}, ::String)` overload already routes through the custom-type door; Scan/post-processing otherwise. |
| [#1150](https://github.com/JuliaData/CSV.jl/issues/1150) | Non-US number formats: `delim = ','` is not handled well  | 1.7y | QUESTION/SUPPORT | docs: `delim=';', decimal=',', groupmark='.'` (reporter confirmed groupmark solves it). Add the European-format example. |
| [#1153](https://github.com/JuliaData/CSV.jl/issues/1153) | Not quoting strings automatically when required | 1.7y | FIXED-BY-REWRITE | verified `"a\r"` is quoted on write and round-trips. |
| [#1155](https://github.com/JuliaData/CSV.jl/issues/1155) | [feature request] add option to add a rownumber column | 1.5y | STILL-OPEN-FEATURE | no rownumber column; `rownumber(row)` on Rows is the data-row index (verified). Physical line numbers = the tape knows them; 1.x. |
| [#1156](https://github.com/JuliaData/CSV.jl/issues/1156) | columns with Int starting with padded zero detected as Int | 1.5y | STILL-OPEN-FEATURE | verified `000100` → Int 100 (pandas/polars/duckdb do the same). Opt-in flag = same mechanism as #967; `types=`/`typemap` workaround. 1.x. |
| [#1157](https://github.com/JuliaData/CSV.jl/issues/1157) | read fallback to ntasks=1 not always working | 1.4y | FIXED-BY-REWRITE | there is no speculation and no fallback: parallel results are identical to serial by construction. |
| [#1159](https://github.com/JuliaData/CSV.jl/issues/1159) | Determine delimiter detection by number of occurrences first | 1.4y | FIXED-BY-REWRITE | consistency scorer first: verified the `;`-with-embedded-commas example → `;`, 6 columns. |
| [#1160](https://github.com/JuliaData/CSV.jl/issues/1160) | Linecount for `limit` and double-quotes | 1.2y | OBSOLETE | by design: skipto/limit count structural (quote-aware) rows, as 0.10 and polars `skip_rows`. VERIFIED improvement: with `comment="%"` the quoted span inside comment lines no longer counts (0.10 miscounted). A quote-blind prefix walk (the #1012/#1079 fix) would give polars-`skip_lines` semantics for free. |
| [#1161](https://github.com/JuliaData/CSV.jl/issues/1161) | Bug: spurious integer overflow | 1.2y | FIXED-BY-REWRITE | no Parsers; Int16 parses via Int64 and narrows at the door: verified −32760/−32767 exact, −32769 → missing + `does not fit Int16` Problem. |
| [#1162](https://github.com/JuliaData/CSV.jl/issues/1162) | Keywords to ignore leading and trailing delimiters | 1.1y | STILL-OPEN-FEATURE | no leading/trailing-delimiter keywords or `drop=[end]`; drop by index works once ncols known. Small (negative index or `:end` in select/drop) — 1.x. |
| [#1163](https://github.com/JuliaData/CSV.jl/issues/1163) | Map cells | 1.1y | QUESTION/SUPPORT | won't-fix: custom types via `tryparse` overload; Tables.Scan for expressions. Close. |
| [#1164](https://github.com/JuliaData/CSV.jl/issues/1164) | `memhash` is to be deleted in 1.13 | 1.0y | STILL-OPEN-BUG | BLOCKER — VERIFIED: the rewrite FAILS TO PRECOMPILE on Julia 1.13.0-rc1 (`UndefVarError: memhash_seed not defined in Base` at src/core.jl:1877, `hash(::CompactString)`, hit by the precompile workload's `Dict{CompactString,UInt32}`). Fix: `@static if isdefined(Base, :hash_bytes)` → `Base.hash_bytes(ptr, n, UInt64(h), Base.HASH_SECRET) % UInt` (exactly what 1.13's `hash(::String)` does), else the memhash path. |
| [#1168](https://github.com/JuliaData/CSV.jl/issues/1168) | Unexpected behavior writing to existing `IOStream` | 11mo | FIXED-BY-REWRITE | verified: `append=false` seeks+truncates (`a,b\n1,2\n10,20`), `append=true` seeks to end and omits the header — both exactly as the reporter expected. |
| [#1169](https://github.com/JuliaData/CSV.jl/issues/1169) | Inconsistent escaping | 11mo | FIXED-BY-REWRITE | no WeakRefStrings; kernel unescape: verified Rows (default and String) and File all give `{"a": 2, "b": ""}`. |
| [#1171](https://github.com/JuliaData/CSV.jl/issues/1171) | CSV.jl "corrupts" data when a field is very large | 9mo | FIXED-BY-REWRITE | duplicate of #935 (verified 2 MiB cell). |
| [#1180](https://github.com/JuliaData/CSV.jl/issues/1180) | Having Artifacts to store test files has some side effects | 5mo | FIXED-BY-REWRITE | the artifact is gone; the 24-file corpus (~5 MB compressed) is committed under test/legacy/testfiles (ships in the package tarball, not as an artifact). Close. |

## PR table

| PR | title | age | author | class | reason |
|---|---|---|---|---|---|
| [#656](https://github.com/JuliaData/CSV.jl/pull/656) | Start work on supporting filtering while parsing | 6.1y | quinnj | SUPERSEDED | Tables.Scan filter pushdown (two-phase masked parse; 1.5–1.9× vs full parse) replaces filter-while-parsing. Close. |
| [#983](https://github.com/JuliaData/CSV.jl/pull/983) | Don't pool small inline string columns by default | 4.5y | quinnj | SUPERSEDED | see #982 — pooling economics differ under CompactString; policy retained. Close. |
| [#1042](https://github.com/JuliaData/CSV.jl/pull/1042) | clarify dateformat docs w.r.t. primitive types | 3.8y | DylanModesitt | STILL-RELEVANT | docs content only: port the 'dateformat applies after numeric detection fails' sentence into the new reading docs, then close. |
| [#1165](https://github.com/JuliaData/CSV.jl/pull/1165) | Fix for newer Julia - Update utils.jl | 1.0y | PallHaraldsson | STILL-RELEVANT | the SAME bug exists in the rewrite (`hash(::CompactString)` uses `Base.memhash`/`memhash_seed`, verified precompile failure on 1.13-rc1). Land a version-guarded `hash_bytes` path in core.jl (and on main if any 0.10.x release is still planned), then close this draft. |
| [#1166](https://github.com/JuliaData/CSV.jl/pull/1166) | Improve zip file example | 12mo | nhz2 | STILL-RELEVANT | docs content only: port the improved ZipArchives example (no mmap, proper closing) into the new examples page, then close. |
| [#1187](https://github.com/JuliaData/CSV.jl/pull/1187) | Bump codecov/codecov-action from 6 to 7 | 1mo | app/dependabot | STALE | dependabot; the 1.0 branch has no CI workflow yet — use codecov-action@v7 when writing it (or merge on main if 0.10.x CI stays alive). |
| [#1188](https://github.com/JuliaData/CSV.jl/pull/1188) | Bump actions/cache from 5 to 6 | 1mo | app/dependabot | STALE | dependabot; use actions/cache@v6 in the new CI workflow (or merge on main). |
| [#1189](https://github.com/JuliaData/CSV.jl/pull/1189) | Bump actions/checkout from 6 to 7 | 1mo | app/dependabot | STALE | dependabot; use actions/checkout@v7 in the new CI workflow (or merge on main). |
| [#1192](https://github.com/JuliaData/CSV.jl/pull/1192) | Keep source column when sources vector has one file | 0mo | JohnCobbler | SUPERSEDED | rewrite's `File(sources; source=)` handles one-element vectors (verified) and errors on name collisions. Close with thanks (or merge on main for 0.10.x). |

## Counts (issues)

| class | n |
|---|---|
| FIXED-BY-REWRITE | 40 |
| OBSOLETE | 26 |
| STILL-OPEN-FEATURE | 24 |
| QUESTION/SUPPORT | 12 |
| STILL-OPEN-BUG | 8 |
| NEEDS-VERIFICATION | 4 |
| **total** | **114** |

## Counts (PRs)

| class | n |
|---|---|
| SUPERSEDED | 3 |
| STILL-RELEVANT | 3 |
| STALE | 3 |
| **total** | **9** |

## Issues by class

- **FIXED-BY-REWRITE** (40): #492, #522, #739, #751, #935, #954, #959, #974, #1002, #1008, #1011, #1019, #1029, #1032, #1048, #1062, #1064, #1068, #1071, #1074, #1076, #1082, #1085, #1118, #1130, #1136, #1138, #1140, #1141, #1143, #1145, #1146, #1153, #1157, #1159, #1161, #1168, #1169, #1171, #1180
- **OBSOLETE** (26): #857, #969, #982, #987, #988, #995, #998, #1044, #1054, #1055, #1061, #1063, #1067, #1070, #1092, #1096, #1102, #1104, #1105, #1113, #1114, #1116, #1122, #1129, #1135, #1160
- **STILL-OPEN-FEATURE** (24): #506, #701, #793, #840, #841, #844, #853, #956, #967, #970, #990, #993, #1013, #1027, #1030, #1049, #1086, #1107, #1120, #1142, #1147, #1155, #1156, #1162
- **QUESTION/SUPPORT** (12): #380, #601, #936, #941, #1031, #1040, #1090, #1091, #1111, #1131, #1150, #1163
- **STILL-OPEN-BUG** (8): #800, #947, #997, #1000, #1012, #1079, #1087, #1164
- **NEEDS-VERIFICATION** (4): #890, #984, #1059, #1075

## Worth doing before 1.0

Ranked by (user impact × fit with the new design). "verified" = reproduced today against `891df6a`.

### Tier A — must-fix (regressions or blockers in the rewrite itself)

| rank | items | what | recommendation | size |
|---|---|---|---|---|
| A1 | #1164, PR #1165 | **Fails to precompile on Julia 1.13-rc1** (`Base.memhash_seed` in `hash(::CompactString)`, core.jl:1877). | **Do it.** `@static if isdefined(Base, :hash_bytes)` → `Base.hash_bytes(p, n, UInt64(h), Base.HASH_SECRET) % UInt` (== 1.13's `hash(::String)`), else current memhash path; keep the `hash(cs) == hash(String(cs))` parity test running on 1.10/1.12/1.13; add a 1.13/nightly CI lane. Also fix the stale comment above it that says the kernel "does not copy" the memhash approach. | tiny |
| A2 | #1030, #941 (+ any function sink) | **`CSV.read(source, sink)` ignores function sinks** (verified: `Tables.matrix`, `Tables.rowtable`, lambdas → NamedTuple). | **Do it.** `read(source, sink; kw...) = sink(File(source; kw...))` (0.10 semantics; wrap in `Tables.CopiedColumns` as 0.10 did if you want DataFrame to adopt columns without copying — decide, since CompactStringVector columns would then land in DataFrames as-is). Add `read(source, ::Type{<:AbstractMatrix}; kw...) = Tables.matrix(File(...))` for #1030 and a test for `Tables.rowtable`/`Tables.matrix`/closure sinks. | tiny |
| A3 | (new) | **`header=false` + `skipto=N` uses row 1's field count** (verified: 0.10 → 2 cols; rewrite → 1 col + long_row Problems). | **Do it.** In `_prepare`, apply the skipto byte offset before computing the `ColumnN` count (or compute the count from the first data row after skipto). Add the differential test vs LegacyCSV (`"foo\nbar\n1\t2\n3\t4"`, `header=false, skipto=3`). | tiny |
| A4 | #1012, #1079 (and #1160) | **Bare quote in prefix rows → 0 rows** (verified still broken, as in 0.10). | **Do it.** Treat rows before `header`/`skipto` as *physical lines*: walk them quote-blind (`Dialect(quoted=false)` copy in `_rawrowoffset`) and, for `header=false`+`skipto`, shift `datastart` to that offset so prefix bytes never enter the index (the numbered-header rule already does this). Document "rows before the header/skipto are skipped as physical lines" — that is polars' `skip_lines` and answers #1160 too. Trade-off: a *quoted* multi-line title in the prefix would then miscount — far rarer than junk preambles (JPL Horizons, instrument dumps, MATPOWER). | small |
| A5 | #947, #800, #751, #1087, #1138 | **Writer perf/memory regression** (verified 1.5–5.4× slower than 0.10 single-thread; ~42 B allocated per output byte; whole output resident twice). | **Do a writer pass** (the writer was correctness-fuzzed but never benchmarked): (1) per-block reusable byte buffer, floats via `Base.Ryu.writeshortest(buf, pos, x)`, ints via digit loop, temporals via `Dates.format(io, x, df)` — no `string(x)`; (2) one dispatch per (column × block) instead of per cell: either a tuple-unrolled schema path for ≤~32 columns + dynamic fallback (0.10's shape) or the reader's trick transposed — render each column of a block into (bytes, offsets) monomorphically, then interleave rows; (3) stream blocks to the sink in order as they finish instead of `reduce(vcat, blocks)` (bounds memory to a few blocks → fixes #1087). Target: ≥ 0.10 per-thread throughput. | medium (1–2 days) |
| A6 | #1000 | Exhausted IO → silent empty File (verified). | **Do it.** In `resolvesource(io::IO)`: if `read(io)` is empty and `position(io) > 0`, throw `ArgumentError("IO is at end (position N); seekstart(io) first")`. | tiny |

### Tier B — decide now (1.0 is the breaking release; these are cheap or become impossible later)

| rank | items | what | recommendation | size |
|---|---|---|---|---|
| B1 | #1107, #1120 | 1.0 pins "empty unquoted cell ⇒ missing, always"; verified this removes 0.10's `missingstring=nothing` ⇒ `""` behaviour. | Prefer restoring: `missingstring=nothing` (or `emptyvalue=:string`) ⇒ no missing spellings at all → empties are `""` in String columns, a Problem in typed columns. It is a one-bit gate where `cellcontent` returns `CELL_MISSING`; the sentinel plumbing already exists. If you keep the pin, put it in the migration guide's first screen. | small / doc |
| B2 | #997, #988 | gzip/IO sources fully in RAM (verified); Rows/Chunks over `.gz`/zip are no longer larger-than-RAM capable. | Restore a tempfile-spool + mmap path in `resolvesource` for gzip files and IO sources above a threshold (0.10's `buffer_in_memory=false` semantics; ~30 lines; keeps `buffer_in_memory=true` for the in-memory case). Otherwise document the limitation and point to the Phase-3 streaming Source. | small |
| B3 | #1049 | `Chunks(ntasks=)` still means "number of batches". | Rename to `nchunks` (keep `chunkbytes`); accept `ntasks` with a deprecation error naming the new kwarg. Only free at 1.0. | tiny |
| B4 | #1118, #1002, #522 | Malformed-quote cells in String columns become `missing` + Problem (verified). | Keep the raw bytes for String columns instead of `missing` (data-preservation principle #522 asked for; the Problem still says why). Small change where `CELL_BADQUOTE` is stored. | small |
| B5 | #506 | URL sources (13 👍-class ask, 4 comments, 7 years old). | ~10 lines with the `Downloads` stdlib in `resolvesource(::AbstractString)` (`r"^https?://"` → `Downloads.download(url, IOBuffer())`); no new dependency. Do it, or close as won't-fix explicitly. | tiny |
| B6 | #853 | Aligned/space-padded files: auto-detect refuses `ignorerepeated`. | Add `(' ', ignorerepeated=true)` as an extra candidate in `_detectdelim`'s field-count consistency scorer (Spec gains `ignorerepeated`). Common for scientific dumps; contained. | small |
| B7 | #990 | select/drop/types keys resolve only against normalized names. | Match raw-or-normalized (keep raw names in `Prepared`). | tiny |
| B8 | #1032 | Date problem message. | Append `(dateformat=\"…\")` / "pass dateformat=" hint to the `cannot parse Date` message. | tiny |
| B9 | #1096, #1055 (#1140-class malformed files) | Lone bare quotes merge rows; now diagnosable but users still lose rows unless they read `problems(f)`. | Docs: a "bare quotes / use `quoted=false`" section keyed on the `bare quote engaged structural protection` Problem kind. 1.x: let `sniff` score `quoted=false` vs `true` by field-count consistency (DuckDB does this). | doc now, 1.x |
| B10 | #959 (perf wart) | `skipto` is a serial ~1 GB/s byte walk on top of the parallel index (verified 0.107 s vs 0.048 s full parse). | Reuse the index's row starts (`_rowsbefore`-style arithmetic) instead of `nextrowstart` walking; only matters for skipto ≫ 0. | small, 1.x ok |

### Tier C — 1.x (fit the design, not blocking; close-or-defer at release with a note)

- #967 / #1086 / #1156 — opt-in `quotedstrings=true` (+ leading-zero-as-string) — detecttype quoted⇒String plus quoted-status conflict in typed loops; contained.
- #1027 — `types=Number`/`Real` as a numeric-only promotion ceiling; `Any` won't-fix.
- #970 — lazy `CSV.Chunk` handle + `materialize` (E.Batches already holds everything).
- #841 — group-wise chunks over a sorted column.
- #701 — bind `header=[names]` post-`select`, or point to Tables.Scan rename.
- #793 / #1162 — trailing-empty-field tolerance; `drop=[end]`/negative index.
- #1013 — pooled ref width; #1142 — PrettyTables extension; #1155 — rownumber column; #844 — fill values; #993 — `normalizenames::Function` or trailing-`_` strip (if ever, now); #840 — commented header row.

### Won't-fix (close at release with rationale)

#380 (types header), #601 (FWF), #857/#956 (delimiter sets/regex — `delim=' ', ignorerepeated=true` covers whitespace), #1031/#1163 (regex rows / cell mapexpr — use `tryparse` custom types or Tables.Scan), #1147 (currency — `tryparse(::Type{Money}, ::String)` overload works today), #936 (units row — `header=1, skipto=3`), #1131 (`typemap=Dict(Date=>String)`), #1111 (Windows `con.csv`), #998 (`missingstring` name kept), #1160 (structural row counting is by design; A4 gives physical-line skipping for prefixes).

### Docs wave (fold into the 1.0 docs/migration guide, then close)

#1040 + PR #1042 (dateformat only after numeric detection fails), #1090 (pool applies to every string type), #1091 (non-UTF-8 gzip: `transcode(GzipDecompressor, read(f))` → StringEncodings → IOBuffer), #1150 (European numbers: `delim=';', decimal=',', groupmark='.'`), PR #1166 (ZipArchives example), #1122 (Chunks are byte-even, row-aligned), #890 (mmap lifetime on Windows: `GC.gc()`/`buffer_in_memory=true` before `rm`), #1096/#1055 (bare quotes → `quoted=false`), #1104/#1116/#1114 (obsolete formatting/bufsize docs).

## Verify then close

Real-world files that motivated FIXED items; the mechanism is proven and synthetic repros pass, but a smoke on the reporter's data is cheap insurance before closing. Run with the rewrite (`julia --project=. -t auto`).

| item | exact reproduction | expected |
|---|---|---|
| #1143 (also #1157 class) | `f = download("https://raw.githubusercontent.com/newzealandpaul/Maritime-Pirate-Attacks/refs/heads/main/data/csv/pirate_attacks.csv"); t1 = CSV.File(f); t0 = CSV.File(f; ntasks=1); Tables.columntable(t1) == Tables.columntable(t0)` | no error at 8 threads; identical to `ntasks=1`; row count matches DuckDB/QuackIO |
| #1140, #1153 | DCMS grants export (~300 MB): `t = CSV.File("grants.csv"); CSV.write("rt.csv", t); t2 = CSV.File("rt.csv"); Tables.columntable(t) == Tables.columntable(t2)` | round-trips; `\r`-bearing fields quoted; no `invalid_quoted_field` fatal |
| #1002, #1055 | IMDB `title.basics.tsv.gz` / `title.akas.tsv.gz`: `t = CSV.File(f; delim='\t'); length(CSV.problems(t)); n_q = length(t); n_nq = length(CSV.File(f; delim='\t', quoted=false))` | no MethodError/`#undef`; if `n_q < n_nq`, Problems of kind `bare quote engaged structural protection` explain the merged rows (→ B9 docs) |
| #1074 | a real Stan CSV (~1 GB, thousands of columns): `@time CSV.read(f, DataFrame; comment="#")` and `@time CSV.read(f, DataFrame; comment="#", select=cols)`; watch RSS | ≥ ~0.5 GB/s (comment dialect = scalar scanner, measured 3.5× slower than the vector path on 104 MB), no memory blow-up |
| #935 / #1171 | `segment_mini.csv` attached to #935 (~150k-char geospatial cells) and MYRIAD-HES `test.csv` from #1171 | cells intact (synthetic 150k and 2 MiB cells verified) |
| #1096 | `fi.csv` attached to #1096: `length(CSV.File("fi.csv"))` vs `quoted=false` | 33704 vs 34034 rows, and `problems(f)` names the lone quote at line 5741 col 288 |
| #1141 | `table.csv` attached: `CSV.File("table.csv"; transpose=true)` | no segfault (synthetic promotion case verified) |
| #984, #1059, #890 (Windows) | Windows CI job: multi-thread File on a ≥1 GB file (mmap path); `for r in CSV.Rows("big.csv") end; rm("big.csv")` | no access violation; document that `rm` needs `GC.gc()` or `buffer_in_memory=true` while views are alive |
| #1063 | `julia -p 2 -e 'using CSV; f = CSV.File("bla.csv"); @spawnat 2 println(f.col1[1])'` | works (local `serialize`/`deserialize` of a File verified) |
| #1075 | reporter's `read(rows)` benchmark is truncated; if it was `CSV.Rows`, `@benchmark for r in CSV.Rows(io) end` on 1e6 rows vs 0.10 | rewrite ≥ 0.10 |
| #1087 | 6-column, 5 GB write on a 16 GB machine after A5 | completes; RSS bounded by a few render blocks |
| #1029 | Wikipedia dump per the repo in the issue (`escapechar='\\', quotechar='\'', types=Union{String,Missing}`, many threads) | no typeassert (synthetic verified) |

Already verified today and safe to close on release without further checks: #492, #522, #739, #935, #954, #959, #974, #1008, #1011, #1019, #1029, #1048, #1062, #1064, #1068, #1071, #1076, #1082, #1085, #1118, #1130, #1136, #1141, #1145, #1146, #1153, #1157, #1159, #1161, #1168, #1169, #1171, #1180.

## Counts

**Issues (114):**

| class | n |
|---|---|
| FIXED-BY-REWRITE | 40 |
| OBSOLETE | 26 |
| STILL-OPEN-FEATURE | 24 |
| QUESTION/SUPPORT (incl. docs, won't-fix) | 12 |
| STILL-OPEN-BUG | 8 |
| NEEDS-VERIFICATION | 4 |

STILL-OPEN-BUG (8): #800, #947, #997, #1000, #1012, #1079, #1087, #1164 — plus three regressions with no issue number yet (sink semantics, `header=false`+`skipto` column count, `missingstring=nothing`).
NEEDS-VERIFICATION (4): #890, #984, #1059, #1075 (Windows / unknown reporter snippet).

**PRs (9):** SUPERSEDED 3 (#656, #983, #1192) · STILL-RELEVANT 3 (#1042 docs, #1165 memhash — same bug in the rewrite, #1166 docs) · STALE 3 (dependabot #1187/#1188/#1189 — use those action versions in the new CI workflow).

Closable at 1.0 release (FIXED + OBSOLETE + QUESTION/SUPPORT + won't-fix features + superseded/stale PRs): **~92 of 123**; the remainder are the Tier A/B items above and 1.x feature requests worth keeping open (or converting to a single 1.x tracking issue).
