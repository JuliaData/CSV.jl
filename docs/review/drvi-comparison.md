# CSV.jl kernel vs. drvi's NewlineLexers / ChunkedBase / ChunkedCSV

Prepared 2026-08-18 for a conversation with Tomáš Drvoštěp (@drvi). All claims are from reading
the sources locally: drvi's packages at `scratchpad/drvi/{NewlineLexers.jl (v0.1.5, 2023-12),
ChunkedBase.jl (v0.3.1, 2023-11), ChunkedCSV.jl (v0.2.1, last commit 2026-01)}` and the kernel at
`~/.julia/dev/CSV-kernel-proveout/src/{core.jl,values.jl}`, `docs/kernel-README.md`, `bench/bench.jl`.
Citations are `file:line`. Paths abbreviated: `NL` = NewlineLexers.jl/src/NewlineLexers.jl,
`CB` = ChunkedBase.jl/src, `CC` = ChunkedCSV.jl/src, `K` = CSV-kernel-proveout/src/core.jl.

---

## 1. What drvi's packages do, precisely

### 1.1 NewlineLexers.jl — quote-aware newline finder (the "lexer")

**Job.** Given a byte buffer and a stateful `Lexer{E,OQ,CQ,NL,IO_t}` (escape, open-quote,
close-quote, newline byte, IO type — `NL:158-166`), push the positions (`Int32`, buffer-relative)
of every newline byte that is *not inside a quoted field* to an output vector (`find_newlines!`,
`NL:534-553`). It finds **only newlines**, never delimiters; delimiter discovery is left to
`Parsers.xparse` per cell downstream.

**SIMD approach.** SIMD.jl `Vec{64,UInt8}` loads (`vload`, `NL:538`) and a hand-written
`@generated` `llvmcall` that does `icmp eq <64 x i8>` → `bitcast <64 x i1> to i64` (`NL:25-38`) —
one 64-bit mask per compare, three compares per block (newline / quote / escape,
`NL:218-220`). Positions are extracted with the standard `trailing_zeros` + `x & (x-1)` loop
(`NL:540-543`). Tails `< 64` bytes go to a scalar fallback (`NL:548-551`).

**Prefix-XOR.** Two implementations chosen at load time (`NL:57-77`): a 6-step shift-XOR
ladder (`NL:58-66`) or `llvm.x86.pclmulqdq` carry-less multiply by all-ones (`NL:69-76`).
**Important detail:** the CLMUL path is only used when the CPU reports `clmul` *and*
`NEWLINELEXERS_NATIVE` is set — the default is the portable ladder
(`_AVOID_PLATFORM_SPECIFIC_LLVM_CODE = get(ENV, "NEWLINELEXERS_NATIVE","false") == "false"`, `NL:21`),
to dodge JuliaLang/julia#49653 (multiversioning/PackageCompiler codegen failure with
target-specific intrinsics). See §3 — this is a live risk for our kernel.

**Quote/escape state within a block.** Two kernels:
- `Lexer{Q,Q,Q}` (escape == quote, RFC `""` doubling; `NL:249-310`): an adaptation of simdjson's
  odd/even backslash-run trick to quote runs. It marks *sequence starts* (`escape_chars & ~(escape_chars<<1)`,
  `NL:255`), splits them by even/odd bit position, adds them to the run mask with `Base.add_with_overflow`
  to find run *ends* and detect a run spilling past bit 63 (`NL:273-274`), and derives the
  **string-boundary quotes only** (`quotes`, `NL:283`, escaped pairs removed). Then
  `in_string = prefix_xor(quotes) ⊻ prev_in_string` and `newlines = compress_newlines & ~in_string`
  (`NL:284-285`).
- `Lexer{E,Q,Q}` (distinct escape, e.g. backslash; `NL:331-365`): simdjson §3.1.1 verbatim —
  odd-length backslash-run detection produces an `escaped` mask, `quotes = quotemask & ~escaped`,
  then prefix-XOR. **This is a SIMD path for backslash dialects; the kernel has none (§2c, §3).**
- Asymmetric quotes (`OQ != CQ`) have no SIMD path: `find_newlines!(::Lexer{E,OQ,CQ})` is the
  scalar generic routine (`NL:521-525`).

**State across 64-byte blocks and across chunks (buffers).** Two mutable words on the Lexer:
`prev_in_string ∈ {0, typemax}` and `prev_escaped ∈ {0,1}` (`NL:163-164`); the truth tables at
`NL:107-128` define their meaning. `prev_in_string` is the top bit of `in_string` (`NL:307`);
`prev_escaped` is the add-with-overflow carry (`NL:308`) — "the block ended on a quote that may be
the first half of an escape pair". Because chunks (IO buffers) can end *anywhere*, this ambiguity
must be resolved when the next chunk arrives: `_find_newlines_generic!` (`NL:378-393`) peeks at
the first byte of the new chunk (`if buf[curr_pos] == E` → it was an escape, skip it; else it was
a closing quote, flip `quoted`). "In quoted field at end of file" is reported through
`possibly_not_in_string(l)` (`NL:224-225`; for `Q,Q,Q` it is `(prev_in_string & 1) == prev_escaped`),
which ChunkedBase turns into `UnmatchedQuoteError` at EOF (`CB/read_and_lex.jl:49`).

**Scalar fallback** (`_find_newlines_generic!`, `NL:368-465`) is not byte-at-a-time: it uses
`ScanByte.memchr` over a **byte set** `{E,OQ,CQ,NL}` (`NL:213`, `NL:401`) to jump to the next
structural byte, then runs the state machine only there. Also used for the `<64` tail and for
asymmetric quotes. Quote-unaware `Lexer{Nothing,...}` has its own SIMD + memchr path
(`NL:468-508`).

**CR handling.** None in the lexer: it matches exactly one `newline` byte. `\r\n` files are lexed on
`\n` and the trailing `\r` is left inside the row bytes for `Parsers.xparse` (which treats `\r`
as a newline). Empty rows are detected as `prev+1 == next || (prev+2 == next && bytes[prev+1] == '\r')`
(`CB/read_and_lex_utils.jl:27-29`). Lone-`\r` files: `_detect_newline` picks `\r` only if the first
buffer has no `\n` (`CB/read_and_lex_utils.jl:3-17`).

### 1.2 ChunkedBase.jl — streaming coordinator/worker chunking

**Model.** One *coordinator* task does IO + lexing; `nworkers` long-lived worker tasks pull
`SubtaskMetadata` (`(task_start, task_end, row_num, task_num, use_current_context)`,
`CB/parser_parallel.jl:7`) from an unbounded `Channel` (`:154`), call the user's
`populate_result_buffer!` (parse) then `consume!` (`:82-84`) on the same task, then `task_done!`
(`:85`). Sentinel `(0,0,...)` messages stop workers (`:175-178`).

**Double buffering.** Two `ChunkingContext`s (`CB/ChunkingContext.jl:48-60`; the second is
constructed lazily only if the input exceeds one buffer, `parser_parallel.jl:155-160`), each with
its own `bytes` buffer, `newline_positions` (`BufferedVector{Int32}` seeded with a fake `0`,
`read_and_lex.jl:63-64`) and a `TaskCounter`. Coordinator loop (`parser_parallel.jl:37-53`): copy
the tail (bytes after the last newline) of the current buffer into the *next* buffer (`:43`),
`read_and_lex!` next (`:44`), submit next's segments (`:46`), **then** `sync_tasks(current)`
(`:49`), swap (`:52`). So at most two chunks are in flight; the coordinator can never get more
than one buffer ahead → **backpressure by construction**.

**Row spanning a buffer boundary / "last newline" handoff.** `prepare_buffer!`
(`read_and_lex.jl:14-34`): `unsafe_copyto!` the bytes after `last_newline_at` to the front of the
buffer, refill the rest; lexing then starts at `start_pos = buffersize - last_newline_at + 1`
(`:73`) with the *carried lexer state*, so the tail bytes are lexed once (in the previous buffer)
but present twice (for contiguous row parsing). A row longer than `buffersize` is a hard error
(`NoValidRowsInBufferError`, `CB/exceptions.jl:6-21`, checked at `read_and_lex.jl:36-44`).
`handle_file_end!` (`:46-55`) synthesizes a final newline at `end_pos+1` if the file does not
end in one, and throws `UnmatchedQuoteError` on EOF-inside-quote.

**Task sizing.** `estimate_task_size` (`ChunkingContext.jl:107-119`): aim for
`MIN_TASK_SIZE_IN_BYTES = 16 KiB` per task (`:5`), prorated by how full the buffer is; segments
overlap by one newline (`task_start = task_end`, `parser_parallel.jl:22`) so each segment carries
its own start. `should_use_parallel` (`:95-100`) drops to `parse_file_serial` for
`nthreads==1 || nworkers==1 || last_newline < 16 KiB`.

**Synchronization/ordering.** `TaskCounter` = `Int + Threads.Condition + exception` (`CB/TaskCounters.jl`);
`setup_tasks!` sets it to `ntasks` per buffer, `task_done!` decrements, `sync_tasks` waits for 0
(`CB/ConsumeContexts.jl:61-95`). Overridable so a consumer can add extra "units of work" (e.g. hand
payloads to other tasks and `task_done!` from there — the `PayloadOrderer` pattern,
`CB/payload.jl:88-132`, which re-sequences out-of-order `ParsedPayload`s by `row_num`).
`consume!` runs **in parallel and out of order**; there is no ordering guarantee unless you use
`PayloadOrderer`. Result buffers: exactly `2 * nworkers` (`ChunkingContext.jl:93`), indexed
`task_num + (use_current ? 0 : nworkers)` (`parser_parallel.jl:77`) so a non-blocking `consume!`
on chunk N cannot race the parse of chunk N+1.

**Errors.** A worker exception closes the channel *and both counters* with the
`CapturedException` (`parser_parallel.jl:88-95`) so a coordinator blocked in `sync_tasks` wakes;
`cleanup(consume_ctx, e)` hook (`:171`).

**Sources.** `_input_to_io` (`read_and_lex_utils.jl:37-48`): path → `IOStream`, gzip by magic →
`CodecZlibNG.GzipDecompressorStream`, optional `MmapStream` (`:147-184`) which mmaps but still
**copies** each chunk into the buffer (`unsafe_copyto!`, `:158`) — no zero-copy parse from the map.
Header/comment/`skipto` handling reads and lexes the first buffer up front (`initial_read!`,
`initial_lex!`, `skip_rows_init!`, `read_and_lex.jl:86-143`, `read_and_lex_utils.jl:89-141`),
refilling if needed. `limit` is implemented by truncating `newline_positions` (`limit_eols!`, `:55-64`).

### 1.3 ChunkedCSV.jl — per-row / per-cell parsing on top of ChunkedBase

**Setup** (`CC/parse_file.jl:82-160` `setup_parser`): defaults `nworkers = max(1, nthreads-1)`,
`buffersize = nworkers * 1 MiB` (`:108-111`), optional `use_mmap`, `no_quoted_newlines` (selects
the quote-unaware lexer, `CC/init_parsing.jl:19-21`), newline auto-detect (`:14-17`), delimiter
detection over the first 11 lines (`CC/detect_delim.jl:9-106`, quote-aware byte loop with a
"points" heuristic — count multiple of nlines / of last-line count / of header count).
Header + schema reconciliation (`CC/init_parsing.jl:40-216`) — **no type inference**: missing
schema entries default to `String` (README:164, `init_parsing_utils.jl:97`).

**Parsing** (`populate_result_buffer!`, `CC/populate_result_buffer.jl:63-189`). Row-major:
for each row (`:88`), skip empty/comment rows (`:95-99`), `ignorerepeated` pre-skip (`:105-107`),
then for each column an **enum switch** over `enum_schema` (`Enums.CSV_TYPE::UInt8`, `CC/Enums.jl:11-22`)
with one `Parsers.xparse(T, row_bytes, pos, len, options)::Result{T}` per cell (`:135-166`) —
"unrolling on enums is easier for the compiler than unrolling on types" (`CC/ChunkedCSV.jl:20-22`),
and the explicit design goal "without dynamic dispatch overhead and … without specializing on
input schema" (README:8-9). Custom types (FixedDecimal, GuessDateTime, …) go through a
`@generated parsecustom!` if-chain over a `Tuple{customtypes...}` (`:35-61`, "adapted from
CSV.jl"). Strings are stored as `Parsers.PosLen31` rebased to the buffer (`:164-166`; 31-bit
pos + 31-bit len, `CC/result_buffer.jl:171-173`) — their fix for PosLen's 20-bit length cap.

**Result buffers** (`TaskResultBuffer`, `CC/result_buffer.jl:163-169`): `cols::Vector{BufferedVector}`
(abstractly typed; concrete `BufferedVector{T}` per column), a `RowStatus` byte per row (bitflags
Ok/MissingValues/TooFewColumns/TooManyColumns/ValueParsingError/SkippedRow, `:23-37`), and two
`BitSetMatrix`es (`:72-114`) that hold a missing/errored bitset **only for rows that have any**
(compact). `ColumnIterator{T}` (`:245-285`) walks a column yielding `(value, isinvalidrow,
iserroredvalue, ismissingvalue)`. Consumers get one `ParsedPayload` per segment; `DebugContext`
prints per-chunk status counts (`CC/consume_contexts.jl:38-133`), `TestContext` collects/sorts by
`row_num` (`:138-191`). Values live only until `task_done!` — the buffer is recycled.

**No benchmark numbers** are published in any of the three repos (only `BenchmarkTools` used in a
`GuessDateTime` test). The README's positioning vs CSV.jl (README:161-168): "We pre-lex the
chunks of data so that parallel parsing can be safe. CSV.jl does the often faster and riskier
thing, where parser tasks jump directly in the file at various offsets … and try to recover."

---

## 2. Point-by-point comparison with the new kernel

### (a) Structure finding: their lexer vs our scanners + tape

| | NewlineLexers | Kernel |
|---|---|---|
| Output | newline positions only (`Int32`/row) | full event tape `(relpos<<2)|kind` for **every delimiter and row end** (`K:596-612`, `K:614-625`), ~1 UInt32 per field |
| SIMD | SIMD.jl `Vec{64}` + one llvmcall compare/mask (`NL:25-38`), 3 compares/block | dependency-free llvmcall with width-generic `<64 x i8>` IR: one call ORs delim/CR/LF compares (`SPECIALS_MASK_VEC_IR`, `K:1023-1040`), one for the quote byte (`K:1042-1051`); portable SWAR `eqmarks/movemask` fallback (`K:960-965`, `K:1067-1087`) |
| Prefix-XOR | shift ladder by default; PCLMULQDQ opt-in (`NL:57-77`) | PCLMULQDQ on x86_64, PMULL64 on Apple aarch64, ladder elsewhere (`K:977-1008`) — **unconditional** |
| Quote state in block | boundary-quote extraction (`NL:249-310`) then prefix-XOR | raw prefix-XOR over **all** quote bytes (`K:1105`), parity carried as one bit `inq ⊻= isodd(count_ones(q64))` (`K:1126`) |
| CRLF | not modelled (`\r` left in row bytes) | pre-paired at scan time as kind 3 (`K:1117-1119`, `K:913-919`), incl. CR at bit 63 / LF in next block via `pairskip` (`K:1108`, `K:1119`) |
| Tail | `ScanByte.memchr` byte-set jump (`NL:401`) | scalar byte loop (`K:1132-1162`) |
| Backslash escapes | SIMD (`NL:331-365`) | **scalar only** (`parityclean`, `K:123`; `swareligible`, `K:131`) |
| Asymmetric quotes | scalar (`NL:521-525`) | scalar (`K:861-926`) |
| Comments | not lexer's concern; skipped by row prefix in `populate_result_buffer!` (`CC:99`) | scalar scanner + assembly (`K:872-887`, `K:720-731`) |
| Post-processing | none | `assemblerows!` (`K:689-744`) applies empty/comment-row hygiene over the tape and builds `rowfirst`/`rowstartrel`; `fieldspan` is O(1) (`K:637-656`) |

The key technical observation: for RFC `""` doubling, drvi's boundary-quote extraction is
**unnecessary** — an escaped quote is two toggles, so `prefix_xor(all quotes)` equals
`prefix_xor(boundary quotes)` at every non-quote position (and only non-quote positions are ever
consulted). This is exactly the property (`K:118-123`, "parity-neutral") that lets the kernel (i)
carry one bit between blocks with no `prev_escaped` ambiguity and (ii) compose parity across
arbitrary byte ranges in parallel (§b). drvi's `Q,Q,Q` kernel inherits simdjson's structure
(built for backslashes, where masking *is* required) and consequently needs the
`prev_escaped` truth table (`NL:107-116`) and the next-chunk disambiguation (`NL:378-393`).

Both designs still scan quotes twice (lexer + value layer). Ours re-touches quote/escape bytes in
`cellcontent`/`findcontent` (`K:345-377`, `values.jl:1122`); theirs in `Parsers.xparse`. Ours
also finds delimiters once (tape) whereas theirs finds them per cell inside `xparse`.

### (b) Parallelism model

**Theirs:** serial IO+lex on a coordinator; parallel *parse+consume* by a fixed worker pool over
row segments of the current buffer; strict two-buffer pipeline; consumers see payloads in
arbitrary order (`PayloadOrderer` to fix). The lexer is the serial bottleneck: whole-file
structural lex at one core's speed (SIMD, so multi-GB/s, but serial), plus the coordinator's
`unsafe_copyto!` tail shuffles.

**Ours:** whole buffer addressable (read/mmap/gunzip-to-memory, `api.jl:152-210`); `chunkplan`
(`K:1256-1332`) splits into fixed byte ranges, computes each range's quote parity **in parallel**
(`quoteparity`, `K:1179-1199`, SWAR popcount at memory speed), exclusive XOR-scan (`K:1299-1303`)
gives every range its true entry state, then a per-range `nextrowstart` (`K:1205-1250`) walks at
most one row to the first true row start. Row-aligned chunks are then indexed in parallel
(`index`, `K:1382-1391`) and parsed **column-at-a-time per chunk** in parallel (`directwave!`
/`directchunk!`, `K:3591-3834`) writing straight into exact-size final columns at known row bases
(`K:3157-3158`). Default geometry `chunkbytes = clamp(cld(n, 4*nthreads), 64 KiB, 1 MiB)`
(`K:3016-3017`) vs their 16 KiB task floor / 1 MiB-per-worker buffers. Determinism: identical
output for any `chunkbytes`/thread count (kernel-README:63-69). No serial pass over the bytes
except the parallel-friendly popcount; nothing speculative.

Where the kernel *does* degrade to a serial walk: comment dialects use `_rowstartatorafter`
(`K:1263-1280`, `K:1336-1342`) — a sequential quote-aware row walk that is, in effect, drvi's
serial lexer at chunk granularity; and **non-parity-clean dialects (backslash escape, `oq!=cq`)
collapse to `nranges = 1`** (`K:1281`) — one chunk, i.e. single-threaded index *and* value
parse. That is strictly worse than ChunkedBase for those dialects (§3, §4).

### (c) Quote handling across boundaries

Theirs: boundaries fall anywhere (IO buffer size), so the lexer *must* carry `(prev_in_string,
prev_escaped)` and disambiguate on the next buffer's first byte. Ours: chunk boundaries are true
row starts by construction (parity + `nextrowstart`), and non-final chunks are asserted to exit
outside quotes (`K:1393-1399`, `K:3321-3324`). Within a chunk the fast scanner carries the parity
bit and `pairskip` between 64-byte blocks. EOF-inside-quote is a `Problem` (`:unclosed_quote`,
`K:3406-3412`), not an exception.

The moment the kernel gets a streaming `ByteSource` (buffer boundaries anywhere), it will face
exactly drvi's problem; the answer is either (i) row-align stream buffers ChunkedBase-style
(tail shift) so each buffer starts at a row start and the *parity trick* is applied within a
buffer, or (ii) carry `(inq, pairskip)` across buffers as NewlineLexers carries its two words.
See §3.

### (d) Value parsing

Theirs: row-major, one `Parsers.xparse` per cell through a per-cell enum switch
(`CC/populate_result_buffer.jl:115-169`) plus a `@generated` chain for custom types. Cell
delimiting, quoting, sentinels, whitespace, and type parsing all happen inside `xparse` with a
runtime `Parsers.Options`. Schema-independent compilation is a stated goal.

Ours: index first, then per-(column × chunk) **monomorphic** loops (`parsecolchunk!`,
`K:2169-2268`): one dynamic dispatch per (column, chunk), the loop body knows `T`; `cellcontent`
(`K:345-377`) turns the exact span into content + disposition; `KernelValues` kernels
(`values.jl`) are total, span-exact, no Parsers.jl (`values.jl:1-41`; e.g. SWAR 8-digit int gather
`values.jl:57-71`, Eisel–Lemire + fallback float `values.jl:167-175`). Also schema-independent
(a fixed set of `parsecolchunk!{T}` instantiations), *and* no per-cell branch. Type inference by
stratified sample + per-column promotion + stale-segment re-parse (`K:3170-3199`, `K:3681-3720`),
which they explicitly do not attempt (README:164; ChunkedBase `TODO(#11)`,
`CB/ConsumeContexts.jl:81`).

Error model: theirs is a per-row `RowStatus` byte + sparse `BitSetMatrix` (unbounded, streaming-friendly,
consumers can filter rows); ours is a bounded reservoir of `Problem` records with excerpts
(`K:2467-2626`) and `on_error=:error` escalation. Ours loses per-row status once `maxproblems`
is hit; theirs cannot give an excerpt without the (soon-recycled) buffer.

### (e) Memory model

Theirs: `2 × buffersize` bytes + `2 × nworkers` result buffers, all reused; O(buffer) regardless of
file size; works on `IO`, gzip streams, S3 prefetch streams; strings valid only during `consume!`.
Ours: whole input resident (mmap or read; gzip → full `transcode`, `api.jl:150,191`) + full
tape (~4 B/field) + exact-size columns; strings are zero-copy `CompactString` views into the buffer
(`K:1532-1551`); `Batches`/`Chunks` (`examples.jl:54-66`, `api.jl:1437-1461`) bound *value*
memory per batch but still hold the whole-file index and buffer.

### (f) What each can do that the other cannot

Theirs, not ours (today): unbounded/non-seekable input with bounded memory (gzip, sockets,
`PrefetchedDownloadStream`), true streaming `consume!` callbacks, per-row status for every row,
SIMD lexing of backslash-escaped files, parallel *parse* for every dialect (lexer is serial but
dialect-agnostic).

Ours, not theirs: deterministic parallel chunking with an exact parallel limit (no serial lex),
`O(1)` field spans and delimiter finding once, column-at-a-time cache behaviour (chunk stays
L2-resident across columns — measured 623 → 911 MiB/s on 200 MiB × 200 cols going 8 MiB → 1 MiB
chunks, `K:3081-3084`), monomorphic value loops, type inference with sample-independent results,
zero-copy string columns and direct-to-final writes (no result-buffer copies, no stitch), CRLF
modelled structurally, problems-as-data instead of hard `UnmatchedQuoteError`/`NoValidRowsInBufferError`
(a >1 MiB row is a fatal error for them; ours only refuses a >1 GiB row, `K:668`, `K:675-679`).

---

## 3. Concrete things worth borrowing

1. **Guard the CLMUL/PMULL `llvmcall`s (or verify they are safe under multiversioning).**
   `K:977-1005` call `llvm.x86.pclmulqdq` / `llvm.aarch64.neon.pmull64` unconditionally. drvi
   ships the same intrinsic *off by default* (`NL:21`, `NL:57`) specifically because of
   JuliaLang/julia#49653 (PackageCompiler / `JULIA_CPU_TARGET` multi-target codegen). CSV.jl is
   precompiled into pkgimages that follow the sysimage's multi-target list; a `generic` clone
   compiling `prefix_xor64` may fail. Action: test precompile with
   `JULIA_CPU_TARGET="generic;native"` and under PackageCompiler; if it breaks, gate on
   `Sys.CPU_NAME`/`LLVMGetHostCPUFeatures` at `__init__` (their detection code `NL:39-49`) or
   move the intrinsic behind a `Preferences` flag. The generic `<64 x i8>` IR (`K:1023-1051`) is
   fine — no target-specific intrinsic there.

2. **Do not collapse non-parity-clean dialects to one chunk.** `K:1281` sets `nranges = 1` for
   backslash-escape / asymmetric-quote dialects → fully serial. Cheap fix: route them through the
   existing comment-dialect planner branch (`K:1263-1280`, `_rowstartatorafter`) — a serial
   quote-aware row walk at chunk granularity (this *is* ChunkedBase's model: serial lex, parallel
   parse) — so index + value work parallelize. Cost: one scalar pass; today those files pay a
   scalar pass *and* serial parse.

3. **Add the simdjson escaped-quote mask to `blockmasks` for backslash dialects** — port
   `_find_newlines_kernel!(::Lexer{E,Q,Q})` (`NL:331-365`) as a third `blockmasks` variant that
   also carries `prev_escaped` (add-with-overflow carry, `NL:337`, `NL:363`) between blocks. Within
   a row-aligned chunk this is sequential anyway, so no composability issue. (Composable *planning*
   for backslash dialects is also possible with a 2-bit range state — parity under "first byte
   escaped / not escaped" + "ends in odd backslash run" — but that is a bigger change; item 2
   already gets the parallel parse.)

4. **Skip-to-next-structural-byte in the scalar scanner.** `indexchunk_scalar!` (`K:861-926`) is
   byte-at-a-time. NewlineLexers's generic path jumps with `ScanByte.memchr` over the byte set
   `{E,OQ,CQ,NL}` (`NL:213`, `NL:401`). We already have `specials_mask_vec`/`byte_mask_vec`; a
   64-byte "candidates" mask (delim|CR|LF|oq|cq|e) and a `trailing_zeros` walk that runs the state
   machine only at candidate positions would make the exotic-dialect and comment paths several×
   faster with ~30 lines. Also usable for the `<64` tail (`K:1132-1162`).

5. **ChunkedBase's coordinator loop as the template for `StreamSource` / `CSV.Chunks` over IO**
   (the kernel's acknowledged gap: `api.jl:156-157` reads the whole IO; `examples.jl:57-59` "a
   production StreamSource would index chunk-by-chunk"). Borrow specifically:
   - the **tail-shift refill** (`prepare_buffer!`, `CB/read_and_lex.jl:14-34`) so every stream
     buffer begins at a row start — then `chunkplan` + parallel index/parse runs *unchanged inside
     each buffer* (parity trick per buffer), no cross-buffer quote carry needed; only
     `finishscan!` (`K:834-852`) needs an `at_eof` flag to *not* synthesize a row end for the
     buffer's trailing partial row and to report where the tail starts;
   - the **two-buffer pipeline with `TaskCounter` backpressure** (`CB/parser_parallel.jl:37-53`,
     `CB/TaskCounters.jl`): read+plan buffer N+1 while buffer N is being parsed; never more than
     two in flight;
   - **exception plumbing that wakes a blocked coordinator** (`CB/parser_parallel.jl:88-95`
     closes the channel and both counters with the `CapturedException`); our `@sync`/`errormonitor`
     structure is fine for eager parse but not for a long-lived pipeline;
   - the **`2 × nworkers` result-buffer rule** (`CB/parser_parallel.jl:73-77`) if batches are
     handed to a user callback that may not block; for our `Chunks` iterator (pull-based) one
     batch table per buffer suffices;
   - the `NoValidRowsInBufferError` case (row > buffer): grow the buffer instead of erroring —
     we can, they chose not to;
   - `MmapStream` (`CB/read_and_lex_utils.jl:147-184`) is *not* worth borrowing (it copies);
     the whole-file mmap path (`api.jl:196-207`) is better; a stream source over mmap should hand
     out views, not copies.
   Schema in a stream: they require it (README:164). For us: seed from the first buffer's
   stratified sample, promote forward only (widening); document that batch schemas can widen but
   never narrow — or offer `types=` for full stability, exactly their contract.

6. **CRLF at a stream-buffer boundary.** If a `StreamSource` ever lexes non-row-aligned buffers,
   `K:1117` (`pos < stop && buf[pos+1] == LF`) will emit a lone CR at buffer end and an LF-only
   empty row at the next buffer start (bytewise ambiguous per kernel-README:127-129). Carry
   `pairskip` across buffers like NewlineLexers carries `prev_escaped`; or, again, row-align the
   buffers and the problem disappears. drvi sidesteps it entirely by lexing only `\n` and letting
   the value parser eat `\r` (`CB/read_and_lex_utils.jl:27-29`).

7. **Per-row status for streaming consumers.** Their `RowStatus` byte + sparse `BitSetMatrix`
   (`CC/result_buffer.jl:23-114`) is a good shape for a batch table's "problems" when the buffer
   will be recycled: cheap, unbounded, filterable. Consider `ParsedTable` exposing an optional
   `rowstatus::Vector{UInt8}` in stream mode alongside the bounded `Problem` log.

8. **`GuessDateTime`** (`CC/type_parsers/datetime_parser.jl:1-48`): a multi-format ISO8601 +
   timezone parser (`_tryparse_timezone`, `:270`) with clamping semantics for out-of-range years.
   Our `parsecivil`/`DatePattern` (`values.jl:1284-1540`) is pattern-exact by design; a
   "lenient ISO-8601 with zone → UTC" *user type* on top of `KernelValues` would cover the
   Snowflake-style exports their tests target (`CC/test/snowflake_generated_csvs.jl`).

9. **Delimiter sniffing heuristic** (`CC/detect_delim.jl:9-106`): candidate set
   `, ; | : \t space`, quote-aware count over ≤11 lines, "points" for consistency with line count /
   header count / last-line count, header-unique shortcut. Comparable to what `CSV.sniff` needs;
   worth checking against ours for the header-vs-data disagreement cases.

10. **Bench them.** No public numbers exist; add ChunkedCSV as a third engine in
    `bench/bench.jl` (shapes at `bench.jl:37-108`) in *schema-provided* mode with a counting
    `consume!` (a `SkipContext`-like context that just sums lengths, `CB/ConsumeContexts.jl:106-107`)
    and separately with a column-collecting context, so both "throughput" and "materialized table"
    are compared fairly. Expect them to be competitive on the `quoted` shape (their lexer is
    SIMD, and our README already flags `quoted` ~0.93× vs CSV 0.10 as the honest gap,
    kernel-README:103-105).

---

## 4. Honest weaknesses

**Ours relative to theirs**
- No streaming: `parse(io) = parse(read(io))` (`K:3434`, `api.jl:156-157`); gzip is fully
  inflated to memory (`api.jl:150`, `:191`). ChunkedCSV runs on an arbitrarily large gzip stream
  in memory bounded by ~2×buffersize plus its `2×nworkers` result buffers.
- Serial for backslash/asymmetric dialects (`K:1281`); no SIMD for backslash escapes.
- Unconditional target-specific intrinsics (`K:977-1005`) — the exact thing they gate off by default.
- Structural rule "every quote toggles" (`K:44-49`, kernel-README:120-123): a bare mid-field
  quote opens a quoted region and can merge rows; ChunkedCSV lexes the same way (their lexer is
  also "every unescaped quote toggles"), but `Parsers.xparse` only honors quotes at field start,
  so their *value* layer is more forgiving of stray quotes within a row that the lexer already
  split correctly. We surface this as `:invalid_value "bare quote engaged structural protection"`
  (`K:2241-2245`) but the row split can differ from CSV 0.10 on malformed input.
- Type inference is a real complexity cost (sampling, promotion register, stale re-parse,
  union-direct columns) that their "schema or String" contract simply avoids; every one of our
  correctness invariants around sample-independence is a maintenance surface they don't carry.
- Problems are bounded (`maxproblems`); no per-row status.

**Theirs relative to ours**
- Serial lexer is the ceiling: parallel speedup is bounded by one core's SIMD lex + coordinator
  copies; no way to use N cores on structure.
- Row-major per-cell `xparse` with runtime `Options`: every cell pays quote/sentinel/whitespace/
  delimiter logic and a code-path switch; no column-local cache behaviour; strings must be
  materialized or become invalid when the buffer recycles.
- No inference, no table object, no CRLF modelling in the lexer, `\r`-only files only detected
  from the first buffer, header/comment skipping is a special-cased first-buffer dance
  (`CB/read_and_lex_utils.jl:89-141`) rather than a uniform tape pass.
- Hard errors on a row larger than the buffer and on EOF-in-quote (`CB/exceptions.jl`).
- `Lexer{Q,Q,Q}` carries complexity (even/odd runs, `prev_escaped` truth table, next-chunk
  disambiguation) that pure parity makes unnecessary for `""` doubling.
- Consumption order is undefined without `PayloadOrderer`; deterministic-for-any-thread-count is
  not a property they claim.

---

## 5. Talking points

- **Why index-then-columnar rather than lex-then-workers.** Their design keeps the parse loop
  row-major (so `xparse` per cell, enum switch per cell, result buffers per worker) because the
  lexer only produces row boundaries. Emitting delimiters too (4 B/field) is cheap in the SIMD loop
  and buys O(1) `fieldspan`, which is what makes column-at-a-time possible: one monomorphic loop
  per (column × chunk), chunk cache-resident across columns, direct writes to final columns, and
  per-column promotion instead of whole-chunk re-parse. Same "no schema specialization" goal they
  state, achieved without the per-cell branch.

- **Why parity composition instead of a serial lexer.** With RFC doubling, quote parity is
  associative, so a memory-speed popcount per range + XOR-scan gives every range its entry state —
  the serial lex disappears and results are identical for any chunk size/thread count. Their own
  kernel already relies on the underlying fact (escaped pairs are parity-neutral) — we just take it
  to the range level. Credit them: their `Q,Q,Q` state tables were the clearest statement of the
  boundary problem, and their `E,Q,Q` SIMD kernel is what we should port for backslash files.

- **Where a hybrid is right.** Streaming/unbounded input (gzip, sockets, cloud prefetch): use
  *their* outer loop (tail-shifted double buffer, TaskCounter backpressure, exception plumbing) and
  *our* inner loop per buffer (parity plan → parallel index → columnar parse → one batch table).
  The serial lexer is replaced by the parity scan; the parse stays columnar; buffer memory stays
  bounded. That is the `StreamSource`/`CSV.Chunks`-over-IO design, and it is mostly assembling
  pieces that both codebases already have. Schema stability in that mode follows their contract
  (`types=` given → stable; inferred → widen-only across batches).

- **Things to concede up front:** we are single-threaded on backslash dialects today (fixable —
  §3.2), we do not stream, and their PackageCompiler caution about CLMUL is one we should adopt or
  disprove.

- **Ask him:** whether RelationalAI ever measured the serial-lexer ceiling at high thread counts;
  whether `PosLen31` + `BufferedVector` columns were ever a bottleneck vs materialization; any
  war stories from the `prev_escaped` disambiguation across buffers (it is the trickiest code in the
  three packages); and whether he'd review the `StreamSource` design once drafted.
