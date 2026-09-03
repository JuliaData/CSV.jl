# Kernel hardening review — implementation warts, fragile invariants, unsafe code, dead code, type design

Scope: `src/core.jl`, `src/values.jl`, `src/compactstring.jl`, `src/examples.jl`, `src/scan.jl`, and how `src/api.jl` consumes them. Branch `kernel-proveout` @ `eefe404`. Every file in scope was read end to end; every claimed bug below was reproduced with the scripts in `$SP/probe_*.jl` (Julia 1.12.6, `-t8`, test env with the `LegacyCSV` 0.10 oracle). The module flatten and `ParsedTable`→`File` merge are known-deferred and not re-argued; concrete pain they cause today is listed under W24/C15/C16.

Severity key: **blocker** = must change before 1.0 (data loss, hard error on realistic input, or an API that cannot be fixed compatibly later) · **major** = correctness or ≥10× perf defect a user will hit · **minor** · **cosmetic** · **perf**.

## Ranked findings

| id | sev | where | one line |
|---|---|---|---|
| W1 | blocker | compactstring.jl:79 `view_payload`; core.jl:1690–1735 (StringColumn), 1774 `_flushstaging!`; api.jl:1741; examples.jl:302 | Any input > 2 GiB with a >12-byte string cell past byte 2³¹ throws `ArgumentError` from inside a chunk task → `CompositeException`; `stringtype=String`, `lazy`, `Rows` all fail; no user workaround. Reproduced on a 2,147,488,199-byte buffer. |
| W2 | blocker/perf | examples.jl:134 `schemamissing`; api.jl:1995 + examples.jl:122 `sampletypes(nsample=total)` | `CSV.Chunks` construction is **2127 ms vs 31 ms** for `CSV.File` on the same 50 MiB file (68×): a per-cell dynamic-dispatch prepass over the whole file (1927 ms) plus the full detect cascade on every row (198 ms). |
| W3 | major/perf | api.jl:1428 `_narrowtypes` | `types=Float32`/`Int32`/… converts through `c[i]` on an `AbstractVector` → **926 ms vs 0.5 ms** monomorphic on 1.16 M rows; `types=Float32` costs 30× the entire parse. |
| W4 | major | api.jl:944–976 `_filefromprepared`, 1453 | `strict=true`/`on_error=:error` does **not** throw for out-of-range narrow types; those problems also bypass `maxproblems`, are unsorted, and carry `pos=0`. Reproduced (`types=Int8`, value 300). |
| W5 | major | api.jl:559 `_footeroffset` | `footerskip` walks quote-aware from `rawstart`, but skipped prefix rows are defined quote-blind → a stray `"` in a prefix row yields **0 rows instead of 2**. Reproduced. |
| W6 | major | core.jl:2262 `NARROW_TYPES`, api.jl:1456 | `Float32`/`Float16` are parsed as `Float64` then converted → double rounding: `"16777217.000000001"` → `1.6777216e7` (direct: `1.6777218e7`); `"3.4028235677973366e38"` → `Inf` (direct: `3.4028235e38`, finite). 0.10 has the same defect (verified with LegacyCSV). |
| W7 | major (API) | core.jl:2672, api.jl:961, scan.jl:202 | `on_error=:error` throws a bare `ErrorException("CSVKernel: …")` built in three places; not structurally catchable, leaks the internal module name; `Problem` has no `show`. Needs a `CSV.ParseError` before 1.0. |
| W8 | major | core.jl:1281 `chunkplan`, 675 `checktaperange` | Non-parity-clean dialects (`escapechar='\\'`, asymmetric quotes, comment+escape) are planned as **one chunk for the whole file**, so `checktaperange`'s 1 GiB bound applies to the *file*; a ≥ 2³⁰-byte such file fails with "a single row is 1 GiB or larger". Reproduced (40 KB file, 1 KiB chunks → 1 chunk vs 40). |
| W9 | major | values.jl:1518–1522 `compilepattern` | A user `dateformat="yyyy-mm-dd"` rejects `"2020-1-1"`; Dates and 0.10 accept it (kernel makes every ≥2-letter run fixed-width; Dates only when a token abuts another token). Reproduced against both. |
| W10 | minor (data) | values.jl:1774 `todatetime` | `DateTime` sub-millisecond digits are **silently truncated** (`…00.1239` → `…00.123`, no problem recorded); Dates throws, 0.10 leaves the column as strings. |
| W11 | minor | core.jl:1288–3302, 18 sites | `errormonitor(Threads.@spawn …)` inside `@sync`: every task failure is printed to stderr by `errormonitor` **and** rethrown as `CompositeException` (double report, verified); other tasks keep running. |
| W12 | minor | api.jl:949 | `limit=0` with a declared `Union{Missing,T}` returns `Vector{T}` (limit=1 returns the union). Reproduced. |
| W13 | minor | api.jl:1873; examples.jl:313 | `Rows(src; types=Dict(:a=>Int))` turns every *untyped* column's cells from `CompactString` into `String` (and changes `Tables.schema`); without `types=` they are `CompactString`. Reproduced. |
| W14 | minor | core.jl:223 `makevalueopts` | `decimal` is only checked for ASCII: `decimal='0'` parses `"105"` as `Int64 105`; `'e'`/`'-'` are also accepted. `groupmark` gets the full conflict check; `decimal` should too. |
| W15 | minor/perf | core.jl:581 `parsevalue(::Type{T})` | Custom-type cells cost **4.5 µs each** (`hasmethod` per cell + `String(buf[i:j])` + try/catch) — 1 M cells ≈ 4.5 s. |
| W16 | minor/perf | compactstring.jl:330 `Base.write(io, ::CompactString)` | Byte-by-byte loop: 4.4 µs for a 1000-byte view vs 80 ns `unsafe_write` (55×); break-even ≈ 28 bytes. `CSV.write` bypasses it, `print`/`join`/`string` do not. |
| W17 | minor | compactstring.jl:35, 116; core.jl:518 `_scratchfor` | `CompactString` is 24 bytes and not `isbits` (a `data` pointer; every inline string points at the shared mutable global `EMPTY_BYTES`); `_scratchfor` hands that same global out as a "scratch" that `degroup!` would `resize!` — currently unreachable, one guard away from corrupting a global. |
| W18 | minor | compactstring.jl (interface) | Gaps: `match(::Regex, cs)` unsupported (Base limitation; `occursin` works), `reverse(cs)` MethodError, no `CompactString(::String)` constructor. Everything else probed works (see "checked and sound"). |
| W19 | minor | core.jl:1874 `Problem` | `pos` is an offset into the bytes CSV *parsed* (decompressed gzip, downloaded URL, `read(io)`), undocumented; no line number; `excerpt` can cut a multibyte character at 32 bytes. |
| W20 | minor | core.jl:72 `Dialect`, write.jl:61 | `File(delim="::")` reads, `write(delim="::")` throws — a file read with a multi-byte delimiter cannot be written back. |
| W21 | minor | core.jl:63 `export`, CSV.jl:27 | `using .CSVKernel` binds `CSV.index`, `CSV.Dialect`, `CSV.ParsedTable`, `CSV.Problem` in the top module (unexported but resolvable). `index` is a generic name to occupy. |
| W22 | cosmetic | values.jl:795 | Comment "1 ≤ off ≤ 8" is false: `off == 9` is reachable (`"12345678.5"`) and works only because Julia defines `w2 << -8` as `w2 >>> 8`. Result verified correct; the reliance should be explicit. |
| W23 | cosmetic | api.jl:1695 `LazyColumn.hint` | Cross-task plain write to a `Ref{Int}`; benign (word-sized, validated on read) but formally a data race — `@atomic` or per-task. |
| W24 | cosmetic | api.jl:979, 989, 985 | Flatten pain today: `Base.parentmodule(@__MODULE__).KernelScan` runtime lookup; `invoke(File, Tuple{Any}, …)` byte-vector door; `Vector{Any}` of `UInt8` → `MethodError` from `resolvesource(::UInt8)`. |
| W25 | cosmetic | api.jl:1232 | `rows = Tuple{Any, Int}[]` — the first element is always a `ChunkIndex`. |
| W26 | cosmetic | core.jl:1658, 2062; api.jl:1736; examples.jl:163 | `Int32(clen)` in the `_unescape_bytes` signature and call sites: `InexactError` for a ≥ 2 GiB cell (moot under W1, but the Int32 is gratuitous). |

## Details

### W1 — the 2 GiB zero-copy cliff (blocker)

`view_payload` (compactstring.jl:79–91) refuses any 0-based offset > `typemax(Int32)`. It is called for every string cell with content > 12 bytes from `parsecolchunk!(::StringColumn)` (core.jl:1703 malformed-quote raw bytes, 1735 the normal view), `_flushstaging!` (1774, `extra`-relative — safe until `extra` itself exceeds 2 GiB), `RowView.getindex` (examples.jl:302) and `_lazyvalue` (api.jl:1741). Nothing above it checks `length(buf)` first.

Repro (`probe_2gib.jl`, buffer of 2,141,065 rows of `"a"^1000 * ",b"`):

```
buffer bytes = 2147488199 (2^31 = 2147483648)
UNHANDLED TASK ERROR: ArgumentError: CompactString view (buffer 0, offset 2147484187) does not fit Arrow's Int32 view words; buffers must stay under 2 GiB
exception type: CompositeException
  nested task error: ArgumentError: … buffers must stay under 2 GiB
--- same with stringtype=String: also fails: CompositeException
--- lazy: first cell ok; last cell: ArgumentError … offset 2147487196
```

Why it matters: 0.10 read multi-GiB files (its `PosLen` had a 44-bit position). Here the failure is (a) 1.3 s into the parse, (b) shaped as a `CompositeException` wrapping `TaskFailedException`, (c) accompanied by stderr noise, (d) with a message that tells the user to do something they cannot ("buffers must stay under 2 GiB"), and (e) `stringtype=String` — the obvious escape hatch — still fails because materialization runs *after* the CompactString parse. `Rows`/`lazy` fail lazily at the first access past the mark.

Fix, in order of preference: (1) the windowed-buffer plan already recorded for CompactStrings.jl (buffer index = ⌊offset / 2 GiB⌋ window; `CompactStringVector` holds the window list) — this is the only fix that keeps zero-copy for big files; (2) until it lands, a single up-front guard in `CSVKernel.parse`/`_prepare`/`lazy`/`Rows`: `length(buf) > typemax(Int32)` ⇒ either throw one clear `ArgumentError("input is N bytes; zero-copy string columns are limited to 2 GiB — pass stringtype=String")` *before any work*, and make `stringtype=String` route string columns through a materializing builder (unescape/copy per cell into `Vector{String}`) instead of the CompactString staging. (3) Move the `Int32` range check out of the per-cell hot path once (2) exists (it is a branch on every long cell today).

### W2 — `Chunks` construction is a 68× whole-file prepass (blocker-class perf)

`probe_perf.jl`, 52 MB / 1,158,841 rows, 6 columns, 8 threads:

```
File total: 31.1 ms
Chunks constructor: 2126.6 ms      Chunks iterate all batches: 86.6 ms
  _prepare (resolve+index+header): 7.9 ms
  sampletypes(nsample=total) over 1158841 rows: 197.6 ms
  schemamissing prepass: 1927.5 ms
```

`schemamissing` (examples.jl:134–153) calls `K.parsevalue(types[j], buf, …)` with `types[j]::Type` from a `Vector{Type}` — a dynamic dispatch per cell for every cell in the file, plus `cellcontent` per cell — and `sampletypes(nsample=total)` (api.jl:1995, examples.jl:122) runs the full detect cascade on every row. Both exist for the stated goal (stable batch schema), but they cost 60× the parse they precede; a user who reaches for `Chunks` to bound memory pays more wall-clock than `File`.

Fix: the cheapest correct version is a function barrier — `schemamissing` loops per column calling `_schemamissingcol(::Type{T}, buf, chunks, j, opts)` so the inner loop is monomorphic (this alone should bring 1.9 s to roughly the parse cost, ~30 ms), and cap `sampletypes` at the stratified default. The better version is the schema-stability protocol from `docs/review/streamed-source-plan.md`: run the fused driver once in a "types-only" mode (it already resolves promotions across all chunks in 31 ms), then parse batches under the frozen types — no second machinery at all. Same applies to `E._batches` (test-only today).

### W3 — `_narrowtypes` per-cell dynamic dispatch (major perf)

api.jl:1428–1459: `c = cols[o]` is an `AbstractVector`; `x = c[i]` in the loop is a dynamic call per cell, and `convert(T, x)` another.

```
Float64/Int64: 29.3 ms   Float32/Int32: 842.5 ms   nrows=1158841
_narrowtypes alone on 2 columns: 926.1 ms
monomorphic convert of the Float64 column: 0.52 ms
```

Fix: `_narrowcol(c::Vector{S}, ::Type{T}, j, problems) where {S,T}` behind a barrier; or better, parse narrow ints natively in the kernel (`parsevalue(::Type{Int8}) = parseint64 + range check`) so the problem is recorded at the right `pos`/row through the normal log — which also fixes W4 and deletes `_narrowtypes`.

### W4 — `strict=true` ignores narrow-type overflow (major)

`_filefromprepared` (api.jl:944–976) runs `_mergeproblems` and the `on_error === :error` throw at 957–964, then calls `_narrowtypes` at 966, which pushes onto a *copy* of the already-sorted, already-capped problem vector.

```
types=Int8 strict=true DID NOT THROW; col=Union{Missing, Int8}[missing]
  problems=[Problem(1, 1, 0, :invalid_value, "value 300 does not fit Int8")]
maxproblems=1 with 2 overflow cells: n=2          # cap bypassed
```

Fix: narrow before escalation, through a `ProblemLog` (cap + order), with the cell's real `pos` (available if narrowing happens in the kernel per W3). Add the strict test.

### W5 — `footerskip` desync on a quote in a skipped prefix row (major)

`_prepare` deliberately treats rows before the anchor as physical lines (api.jl:508–525, the #1012/#1079/#1160 fix), but `_footeroffset` (559–572) walks the whole file from `rawstart` with the quote-aware `nextrowstart`. A `"` in the prefix opens a quoted region that swallows the rest of the file → `nrows == 1` → `footerskip >= nrows` → returns `rawstart` → `keep = 0`.

```
input: junk "quote here\nignored\nx,y\n1,2\n3,4\nfooter line\n
header=3 footerskip=1 rows=0 expected 2 -> Missing[]
same without the quote rows=2
```

Fix: start the footer walk at `anchoroff` (everything before it is already excluded) — one argument change; the count of non-comment rows from the anchor is what the caller needs anyway.

### W6 — Float32/Float16 double rounding (major; 0.10 parity)

`resolvetypes` maps `Float32 => Float64` (core.jl:2262) and `_narrowtypes` does `convert(T, x)`. Decimal → binary64 → binary32 is not the same as decimal → binary32 whenever the binary64 rounding lands exactly on a binary32 midpoint:

```
"16777217.000000001"      direct=1.6777218e7  via64=1.6777216e7  kernel=1.6777216e7  MISMATCH
"1.00000005960464477550"  direct=1.0000001    via64=1.0          kernel=1.0          MISMATCH
"3.4028235677973366e38"   direct=3.4028235e38 via64=Inf          kernel=Inf          MISMATCH   (finite → Inf)
LegacyCSV 0.10 types=Float32 on 16777217.000000001 => 1.6777216e7   (same defect)
```

The random search (300 k `N.ddddddddd` samples) found none — the window is ~2⁻²⁹ per value — but a value column with 9–17 significant digits will contain them. Fix: `parsefloat32` = the same three tiers with 24-bit precision (Eisel–Lemire's Float32 variant uses the identical POW5 table with a different shift; tier 3 with `nbits = 23`); `Float16` via Float32 is exact-enough only if done the same way. Do it before Parsers 3.0 freezes the value layer.

### W7 — exception type for `on_error=:error` (major, API)

Three copies of `throw(ErrorException("CSVKernel: $(p.kind) at data row … (+N more)"))` (core.jl:2672, api.jl:961, scan.jl:202). Users of `CSV.File(strict=true)` see "CSVKernel" and get nothing structured to catch:

```
ErrorException: CSVKernel: short_row at data row 2, column 0: expected 2 fields, found 1 (remaining columns set to missing)
```

Recommend for 1.0: `struct ParseError <: Exception; source::String; problem::Problem; nproblems::Int; end` with `showerror` (kind, data row, column, byte offset, excerpt, "+N more; problems(f) after on_error=:collect"), thrown from one helper. Give `Problem` a `show` (today: `CSV.CSVKernel.Problem(1, 0, 5, :short_row, "…")`) and keep the row/col `0` sentinels documented or move to `Union{Nothing,Int}`. The `error("internal error: …")` sites (8) are fine as `ErrorException` but should say "please report".

### W8 — non-parity-clean dialects are one chunk; 1 GiB cliff and no parallelism (major)

core.jl:1281 `nranges = parityclean(d) ? … : 1`. For `escapechar≠quotechar` or `openquotechar≠closequotechar` the file is one `ChunkIndex`, so: (a) `checktaperange` (675) throws for any such file ≥ 2³⁰ bytes with a message about "a single row"; (b) the index and every column parse are single-task; (c) the column-at-a-time cache-residency argument (the reason for the 1 MiB cap) is lost. Verified: 40 KB / 1 KiB chunks → 1 chunk with `escapechar='\\'`, 40 without; comment+escape → 1.

Fix: reuse the comment-dialect planner (`_rowstartatorafter` walks true row starts with the full dialect state machine) for every non-parity-clean dialect — a one-line condition change at 1264 — giving bounded, parallel-parsable chunks at the cost of a sequential row-start walk. Fix the message either way.

### W9 — user `dateformat` fixed-width rule differs from Dates (major)

`compilepattern` (values.jl:1518–1522): `y` run ≥ 4 and `m`/`d`/`H`/`M`/`S` runs ≥ 2 are `fixed`. Dates' rule: a part is fixed-width only when it is immediately followed by another part (`yyyymmdd`), never when a literal follows.

```
kernel dateformat="yyyy-mm-dd" on 2020-1-1:     CompactString "2020-1-1"
Dates  Date("2020-1-1", dateformat"yyyy-mm-dd"): 2020-01-01
0.10   dateformat="yyyy-mm-dd" on 2020-1-1:      Date 2020-01-01
kernel dateformat="yyyy-mm-dd" on 02020-01-01:  CompactString; Dates: 2020-01-01
```

For the *default* ISO detection the strictness is a defensible 1.0 change (it is what makes parse-set ≡ detect-set), but it also changes behavior on files 0.10 read as dates: `"2020-1-1"`, `"10000-01-01"`, `"2020-01-01T00:00"` were `Date`/`DateTime` in 0.10 and are strings now (verified). For a *user-supplied* `dateformat` the divergence from Dates is a bug: adopt Dates' adjacency rule in `compilepattern` (fixed ⇔ next op is a numeric part), and document the default-ISO delta in the 1.0 notes.

### W10 — DateTime sub-ms truncation is silent (minor, data)

`todatetime` (values.jl:1774) does `nanosecond ÷ 1_000_000`; `parsecivil` accepts 1–9 subsecond digits for the `s` op. So `2020-01-01T00:00:00.1239` → `2020-01-01T00:00:00.123` with no problem recorded, while Dates throws "Invalid DateTime string" and 0.10 leaves the column `String`. `Time` keeps full ns (good). Either reject > 3 digits for `DateTime` (detect as String, matching Dates), or round and log an `:invalid_value`-class problem — silent truncation is the one option that loses data without a trace.

### W11 — `errormonitor` inside `@sync` (minor)

Verified with `probe_exc.jl`: the failure is printed as `UNHANDLED TASK ERROR: …` to stderr *and* rethrown as `CompositeException(TaskFailedException…)`. Under W1 every failing chunk prints. `@sync` already rethrows; drop `errormonitor` inside `@sync` blocks (18 sites) or replace with a small `_spawn` helper. There is no cancellation: sibling chunks run to completion after the first failure (acceptable, but worth a one-line comment).

### W12 — `limit=0` drops the declared union (minor)

api.jl:949–952 replaces `types` with a `Vector{Type}` of non-missing types when `p.limit == 0`, so `declaredmissing` sees no `Union{Missing,T}`: `types=Union{Missing,Int}, limit=0` → `Vector{Int64}`; `limit=1` → `Vector{Union{Missing,Int64}}`. Pass `wantmissing` through, or don't rewrite `types`.

### W13 — `Rows(types=…)` changes untyped cells to `String` (minor)

api.jl:1873 seeds `nothing ⇒ String`, and `E.typedvalue(::Type{String})` (examples.jl:313) materializes. Verified: without `types=` cells are `CompactString`, with `types=Dict(:a=>Int)` column `b` becomes `String` and `Tables.schema` reports `Union{Missing,String}`. Seed `nothing ⇒ CompactString` (or route through `_rowstring(stringtype, x)`).

### W14 — `decimal` validation (minor)

`makevalueopts` (core.jl:223) checks `isascii(decimal)` only; `groupmark` (226–230) gets the full conflict check. `decimal='0'` on `"105"` → `Int64 105`; `'e'`, `'-'`, `','`(= delim) accepted silently. Apply the same rule set (not a digit, not `+`/`-`/`e`/`E`, not a quote/escape byte).

### W15 — custom-type cell cost (minor perf)

core.jl:581–594: `hasmethod(Base.tryparse, …)` is evaluated per cell (it is already computed once in `_customparseable`, 2267), `String(buf[i:j])` allocates twice, and the try/catch is per cell: **4.5 µs/cell** measured. Cache the parse function per column (a `Val{:tryparse}`/`Val{:parse}` decision at column setup) and pass `unsafe_string(pointer(buf,i), n)` (one allocation).

### W16 — `Base.write(io, ::CompactString)` (minor perf)

```
n=28:   byte-loop 183 ns; unsafe_write span 77 ns
n=300:  byte-loop 1382 ns; unsafe_write span 67 ns
n=1000: byte-loop 4433 ns; unsafe_write span 80 ns
```

Views: `unsafe_write(io, pointer(data, cspos), n)` under `GC.@preserve`; inline: two word stores into a 16-byte stack scratch (the `materialize` trick) and one `unsafe_write`. `Base.print` and `string` inherit the fix.

### W17 — `CompactString`'s pointer and the `EMPTY_BYTES` sentinel (minor)

`sizeof(CompactString) == 24`, `isbitstype == false` (verified); every inline string's `data` is the global `EMPTY_BYTES`. Consequences: `Vector{CompactString}` (from `collect`, `Dict` keys in `_internrange!`, `_chaincolumn`) carries GC references and write barriers; the payload vector in `CompactStringVector` is fine. Separately, `_scratchfor(vo)` (core.jl:518) returns that same `EMPTY_BYTES` as the "scratch" when `groupmark == 0`, and `degroup!` (values.jl:1305) `resize!`s its scratch — only the `groupmark != 0` guard in each `parsevalue` keeps a global from being mutated. The CompactStrings.jl payload+validity design removes the pointer; until then make the no-groupmark scratch `nothing` (or a `Union` field) rather than a shared mutable vector, and keep `EMPTY_BYTES` private to `compactstring.jl`.

### W18 — CompactString interface gaps (minor)

Probed (`probe_cs2.jl`): works — `s[1:3]`/`SubString`, `split`, `strip`, `startswith`, `occursin` (String and Regex), `replace`, `uppercase`/`lowercase`/`titlecase`, `chop`/`chomp`, `^`, `first`/`last`, `findfirst`, `parse`/`tryparse`, `Symbol`, `join`, `*`, `string`, `repr`, `escape_string`, `convert(String, s)`, `Dict{String}` lookup by CompactString and vice versa, `sort`, `promote_type(CS, SubString{String}) == AbstractString`, `[cs, "str"]` → `Vector{String}`. Fails — `match(::Regex, cs)` (Base restricts to `String`; consider `Base.match(r, s::CompactString) = match(r, String(s))` or document), `reverse(cs)` (no generic method in Base; InlineStrings defines its own), `CompactString("abc")` (no constructor — acceptable, but `Base.convert(::Type{CompactString}, ::String)` would make `push!(col, "x")`-style code work on a `Vector{CompactString}`).

### W19 — `Problem` semantics (minor)

`pos` is an offset into the buffer CSV parsed: for a gzip source it points into decompressed bytes (verified: 30-byte `.gz`, `pos=9`), for a URL into the downloaded temp file, for `IO` into `read(io)`. Undocumented. There is no `line`; `excerpt` slices 32 raw bytes (`String(buf[pos:pos+n-1])`) and can end mid-character. Suggest: document `pos`, add a lazily computed `line` accessor (`count(==(0x0a), buf[1:pos])` on demand from `File`), and clip the excerpt at a character boundary (`thisind`).

### W20–W26 — smaller items

- W20: `Dialect` (core.jl:72) accepts multi-byte delimiters and the scalar scanner handles them (25 chunks for `"::"` verified), but `write` (write.jl:61) refuses `delim="::"` — round-tripping a file fails. Either write the bytes (trivial) or reject on read.
- W21: `export Dialect, index, ParsedTable, Problem` (core.jl:63) + `using .CSVKernel` (CSV.jl:27) ⇒ `CSV.index`, `CSV.Dialect`, `CSV.ParsedTable`, `CSV.Problem` resolve (verified with `isdefined`). `CSV.Problem` may be *wanted* public; `CSV.index` is not. Drop the export or import selectively.
- W22: values.jl:795 comment vs reality — `off = 9` is reached for `"12345678.5"` (verified: fast path returns `1.23456785e7` correctly) via `w1 >>> 72 == 0` and `w2 << -8 == w2 >>> 8`. Write it as `off <= 8 ? … : w2 >>> 8` so the negative shift is not load-bearing.
- W23: `LazyColumn.hint::Ref{Int}` written from any task without synchronization (api.jl:1706–1714). Benign on every supported platform (aligned word store, validated on read) but a formal data race; `@atomic :monotonic` or a task-local hint costs nothing.
- W24: flatten pain today — `Base.parentmodule(@__MODULE__).KernelScan` (api.jl:979) resolves a sibling module by name at run time; `invoke(File, Tuple{Any}, sources; kw...)` (989) is the only way to reach the byte-vector method from the vector-of-sources method; `eltype(sources) === UInt8` (985) sends `Any[0x61, 0x0a]` to `resolvesource(::UInt8)` → `MethodError` (verified). `KernelScan._DIALECTKW` and `CSVApi._DIALECTKW` differ by `:delim` (C4).
- W25: api.jl:1232 `Tuple{Any,Int}[]` → `Tuple{K.ChunkIndex,Int}[]`.
- W26: `_unescape_bytes(buf, pos::Int64, len::Int32, …)` (core.jl:1658) forces `Int32(clen)` at four call sites; nothing needs the narrow type.

### Design notes (no bug, worth a decision)

- **`ProblemLog` retention policy.** `pushproblem!` (core.jl:1930) keeps the `limit` *source-earliest* problems (max-heap displacement, O(log n)) — verified reading; the count is exact. This is the right policy for a deterministic parallel parser (first-N-encountered would vary with thread count). The cost is that `problemless` compares `message` strings as the last key and that `problemkey` (1898, allocating, used by `scan.jl:195` `sort!(by=problemkey)`) and `problemless` (1902) are two definitions of the same order.
- **`ValueOpts` per column (`colopts`).** A general per-column options vector used only for `dateformat::Dict`; `_copts` is `@inbounds` and only `parse` validates the length (core.jl:2540) — `sampletypesrows` and the examples paths trust it. Fine as a mechanism (per-column `decimal`/`sentinels` could ride it); the validation should live in `_copts`'s constructor path, not in one caller.
- **`TypedColumn`/`UnionColumn`/`StringColumn` + `finalizecolumn`.** The rationale is sound (RESPONSES #6). The 3-arg/4-arg `force_missing` pairs (core.jl:3253–3280, 7 methods) can be 4 with a keyword default. `PooledColumn{ELT}` (3094) is now a transient handed straight to `_topooledarray`; its `levels` field is hard-typed to `CompactStringVector{CompactString}` — the `ELT` parameter only ever encodes `Missing`.
- **`Dialect.delim::Union{UInt8, Vector{UInt8}}`.** The union field is tested per byte in the scalar scanner's hot loop (core.jl:894). Acceptable because the scalar path is the slow path, but a `Val`-specialized `indexchunk_scalar!` would make multi-byte and single-byte scalar dialects both cheaper.
- **Return-code conventions.** `RC_*` (values.jl) vs `(v, ok::Bool)` (`parsevalue`) vs `(v, matched)` (`_matchspecial`) vs `(v, handled)` (`_float_fast`) vs 3-tuple `(v, rc, done)` (`_parsefloat_core`) vs `Int` sentinels `-1/-2` (`degroup!`) and `-1/-2/-3` (`_eisel_lemire`). All internal and each locally documented; for the Parsers-3.0 surface, pick one (`(value, rc)` with `rc::UInt8`) and keep the sentinel ints private.
- **Sniffing single-column files** (out of slice, observed while probing): a one-column file whose data contains `:` (`"a\n00:00:00.123\n"`) elects `:` as the delimiter — identically in 0.10 (LegacyCSV warned "parsed expected 1 columns, but didn't reach end of line"). Parity, but a time/datetime column in a single-column file is common enough to special-case (a candidate that splits the *header* into one field and data rows into >1 should lose to `,`).

## Consolidation candidates (duplicated logic)

| id | what | where |
|---|---|---|
| C1 | BOM/`datastart` test, 4 copies (plus `_sample` calling `_datastart`) | api.jl:201, core.jl:2443, examples.jl:96, scan.jl:57 |
| C2 | Header-name extraction, 2 implementations that have **drifted**: `parseheader!` records malformed-quote and `_delimclash` problems and keeps raw bytes; `E.headername` records nothing and skips the delim-clash check | core.jl:2039–2067 vs examples.jl:155–164 |
| C3 | `Symbol("Column", j)` generated names, 9 sites | core.jl:2047, 2056, 2496; api.jl:710, 1242; examples.jl:111, 157, 160; scan.jl:73 |
| C4 | `_DIALECTKW` defined twice with different contents (`:delim` present only in scan.jl); `_checkkwargs` allowed tuples rebuilt in `lazy`, `Rows`, `Chunks`, `_prepare`, `sniff`, `_transposedfile` | api.jl:49, scan.jl:79; api.jl:1656, 1868, 1978, 626, 452, 1207 |
| C5 | `ErrorException("CSVKernel: …")` message assembly, 3 copies | core.jl:2672, api.jl:961, scan.jl:202 |
| C6 | short_row/long_row reporting loop, 3 copies | core.jl:2704–2719 (`fusedchunk!`), 2987–3002 (`directchunk!`), examples.jl:183–194 |
| C7 | Escape-walk loops, 4 copies (`_unescape_inline`, the non-RFC branch of `_unescape_append!`, `_unescape_bytes`, dead `_cellhash`) | core.jl:1580, 1605, 1658, 2150 |
| C8 | `_eqmask8` ≡ `_eqmask8_c` (identical SWAR helpers in two modules); `eqmarks` is the exact variant | values.jl:748, core.jl:1573, core.jl:947 |
| C9 | Comment-prefix compare: inline loops in `assemblerows!` and `assemblecollapsed!`, `_matchbytes`, api's `_iscommentrow` | core.jl:722–730, 806–814, 968; api.jl:547 |
| C10 | `assemblerows!` / `assemblecollapsed!` share ~60 lines of row-end hygiene | core.jl:689–757, 758–840 |
| C11 | Two bounded max-heap reservoirs: `pushproblem!(::ProblemLog)` and the inner loop of `mergeproblems!(::PendingProblemLog)`; `problemkey` vs `problemless` | core.jl:1930–1950, 1984–2018, 1898, 1902 |
| C12 | `sampletypes` vs `sampletypesrows` | core.jl:2105, 2186 |
| C13 | `finalizecolumn` 3-arg/4-arg pairs | core.jl:3253–3280 |
| C14 | Three switches over the same `types` spec: `resolvetypes`, `declaredmissing`, `requestedtypes` | core.jl:2300, 2331, 2349 |
| C15 | `E._batches` re-implements `parse`/`_prepare`'s source→dialect→index→header→seed pipeline (and is test-only) | examples.jl:83–131 |
| C16 | `KernelScan.scan` re-implements the door (test-only); `execute` is what `File` uses | scan.jl:35–77 |
| C17 | `parsevalue` 5-arg/6-arg ladder threading `scratch` (10 methods) | core.jl:446–535 |
| C18 | write.jl: `_writecell`/`_writestring` vs `_appendcell!` — two quoting policies ("the semantic reference is `_writecell`; this mirrors it") that can drift | write.jl:135–170, 485–530 |

## Dead code / leftovers

- `_cellhash` (core.jl:2150–2174) and `_splitmix64` (2176–2181): zero callers — remnant of the deleted pooling pre-skip.
- `materialize(::PooledColumn)` (core.jl:3126), `_widenmissing(::PooledColumn)` (3105), `poollevels` (3121): zero callers.
- Stale comments: `PooledColumn` docstring (core.jl:3088–3093, "Each chunk interns strings during parsing…"); orphan `# the effective (ratio, cap) for column j…` (3123) with no function after it; orphan `# All-missing column.` (1680); compactstring.jl:6 "InlineTable/PoolSegment"; values.jl:856 "See probe_float_anomaly.jl" (no such file in the repo); `docs/kernel-README.md` "`test.jl` pins the exact behavior" (file is `test/kernel.jl`).
- Test-only doors: `E.read` (examples.jl:51), `E.batches`/`E._batches` (81/83), `E.rows` (238), `E.headername` (155) — only `test/kernel.jl:1418–1485` call them; `CSVApi.Chunks`/`Rows` construct `E.Batches`/`E.Rows` directly. `KernelScan.scan` (scan.jl:35) — only `test/scan.jl`. The file-level docstring of examples.jl still describes them as the three surfaces.
- `BufferIndex.nrows` — read only by `test/kernel.jl:40`; goes stale after `_limitrows!` (api.jl:583) resizes `rowfirst` in place.
- `parsegroupedint64(buf, i, j, gm)` 4-arg allocating convenience (values.jl:1354) — no `src` caller.
- No `TODO`/`FIXME`/`@show`/debug `println` leftovers (grepped).

## Checked and sound

- **Adversarial tiny inputs** (`probe_tiny.jl`, `--check-bounds=yes`): 71 inputs (empty, lone CR/LF/CRLF, lone quote, BOM-only, cells ending exactly at EOF at 10/12/13/16 bytes, `.5`/`5.`/`-.5`, `1e400`, 39-digit ints, `\r`-only, comments, `ignorerepeated`, unterminated quotes) × scanners `:vec/:swar/:scalar` × `chunkbytes ∈ {1,2,3,7,16,64,1 MiB}` × parallel on/off, plus `File`/`Rows`/`Chunks`/`lazy` doors: **0 failures, byte-identical results across all scanner/geometry combinations**.
- **Every `unsafe_load`/pointer read is span- or buffer-bounded**: `inline_payload` (`pos+11 ≤ length`), `view_payload` (`len > 12` ⇒ 4-byte prefix in bounds at every call site), `_load8` in `parseint64` (`i+7 ≤ j`), `_decompose` (`lastw = j-7`), `_float_fast` (`i+15 ≤ length(buf)`), `_hasbyte` (`min(j,length)-7`), `parsegroupedint64` (`j+8 > length ⇒ slow path`), `parseuuid` (loads end at `i+35 = j`), `_iso_ymd/_iso_hms` (callers check exact lengths 10/19/8), SWAR/vector block loop (`pos+63 ≤ stop`; in-block `buf[pos+tz+1]` guarded by `pos+tz < stop`), `quoteparity`, `_delimclash`, `_nextpair`, `findcontent` word loops (`k+7 ≤ last`), `memcmp` in `==`/`cmp` (equal / min lengths), `materialize`'s 16-byte scratch (`len ≤ 12`), `hash`'s `Ref` scratch under `GC.@preserve`.
- **Tape bounds**: `checktaperange` guarantees `(stop+1-start) << 2` fits `UInt32` including the synthesized EOF event; `rowfirst::Int32` cannot overflow inside a < 2³⁰-byte chunk; CRLF split across 64-byte blocks (`pairskip`) is only set after the LF was verified, so the cleared bit is always the LF.
- **Comment rows cannot begin inside quotes**: `atrowstart` is only set after a row-end event, which only fires outside quotes; `assemblerows!`'s comment compare cannot leak past the row (terminators are excluded from comment bytes by `Dialect`).
- **`_float_fast` `off == 9` path** verified correct against Base for `"12345678.5"`, `"12345678.123456"`, `"12345678."`; **HPD tier** with 790–1500-digit mantissas (incl. 300 leading zeros, `e-300`, subnormal-ish): 0/3000 mismatches vs a 4000-bit BigFloat oracle.
- **Integer/float edge spellings** behave as documented: `"+"`, `"-"` INVALID; `"-0"`, `"00"`, `"007"` OK; `"1_000"` INVALID; 19-digit overflow → `RC_OVERFLOW` with leading zeros counted correctly; `Inf/Infinity/NaN` case-insensitive with sign; `1e400` → `Inf`, `1e-400` → `0.0` with OK (documented, same as `Base.parse` and 0.10); `"."`, `"e5"`, `"1e"`, `"0x10"` INVALID. `parsebool` is strictly lowercase `true`/`false` (0.10 parity). Int128 → Float64 at 2¹²⁷; `UInt64` max; grouped ints (`1,234,567`, quoted, Indian grouping) OK; `1,,234` → String. `BigFloat` beyond 10^±65536 → problem, not crash. `typemap` cycles terminate at `String`.
- **CompactString `hash`/`==`/`cmp`/`length`/`collect`/`String` parity with `String`** over 3000 random byte strings of lengths 0–40 (invalid UTF-8 included) and embedded NULs: 0 mismatches.
- **Thread safety**: promotion register (`promo`/`finals`) read and written under `promolock`; `segments[k]`/`segtypes[k]`/`indexed[k]`/`finals[j]` (alloc phase) are disjoint-slot pointer/byte stores; `UnionColumn` data+tag stores and `present::Vector{Bool}` are byte-disjoint per chunk; `PendingProblemLog` is locked and task-local `ProblemLog`s are folded once; the boxed-capture test covers every `@spawn`-containing method; `for`-loop variables get fresh bindings; `Threads.nthreads()` appears only in call-time-evaluated defaults and bodies. `_prefetch!`'s detached tasks (api.jl:148) intentionally outlive the call and keep the mapping alive.
- **Promotion monotonicity**: every value that parses as `T` parses as any lattice successor of `T` (Int64⊂Int128⊂Float64⊂String, temporals/Bool⊂String, grouped forms included), and `_promotemapped` collapses any typemap that would move downward or cycle to `String`, so the `error("internal error: re-parse under the joined type conflicted")` sites in `redirect!`/`restale!` are unreachable by user input; "promotion did not converge" needs > 8 steps on a 4-deep lattice — unreachable.
- **`EMPTY_BYTES` mutation** is currently unreachable (every `degroup!` call is behind `vo.groupmark != 0`, and `_scratchfor` allocates a real vector in that case) — see W17 for why it is one guard away.
- **`fieldspan`** is O(1) under `ignorerepeated` via `ext`; trailing delimiter yields a final empty field; `\r`-only endings are row ends; empty `\r\r\n` sequences drop as two empty rows; whitespace-only rows are single-field rows (short_row problems), as in 0.10.
- **`makeunique!`** (`[:a,:a,:a_1,:a]` → `[:a,:a_2,:a_1,:a_3]`) matches its docstring; `select` duplicates collapse; `select` output is file-ordered (0.10 parity — document).
