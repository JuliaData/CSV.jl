# CSV.jl 1.0 review — API consistency and writer

Branch `kernel-proveout` @ `eefe404`, Julia 1.12.6, `-t8`. Every claim below was reproduced by the probe scripts under `$SP/api/` (`kwmatrix.jl`, `naming.jl`, `tables.jl`, `writer.jl`, `writer_legacy.jl`, `extra.jl`, `docs.jl`, `stdout_new.jl`/`stdout_legacy.jl`); raw logs are in `$SP/api/out/`. Settled decisions (CompactString default, pool=false, empties-always-missing, problems-as-data, retired function-typed kwargs, internal sniff/Spec, deferred module flatten) are not re-argued; consistency of their application is.

## 1. Ranked findings

| id | sev | one line |
|---|---|---|
| W1 | **blocker** | `CSV.write(io, t)` on any seekable IO without `append=true` does `seekstart`+`truncate`: `julia s.jl > log` loses every earlier log line; `>> log` loses the CSV itself; `open(p,"w") do io; write(io,"hdr\n"); CSV.write(io,t) end` loses `hdr`. 0.10 rewound but never truncated. |
| A1 | **blocker** | `f[:col]` / `f["col"]` on `CSV.File` are gone (MethodError). Documented 0.10 idiom (index.md, File docstring); also `size(f)`, `f[1:2]`, `f[end]`, `lastindex`, `filter(pred, f)` gone (File is no longer `AbstractVector{Row}`). |
| S1 | **blocker** | `File(...; scan=Tables.Scan(...))` throws `MethodError: no method matching bind(::Tables.Scan, ::Vector{Symbol})` — the dev'd Tables (`jq/scan`) renamed `Tables.bind` → `Tables.resolve`; `src/scan.jl:94` still calls `bind`. Every scan-door call and `test/scan.jl` are broken against the checked-out Tables. |
| W2 | major | Writer has no legacy-keyword table: `quotestrings`, `transform`, `bufsize`, any typo → `MethodError: no method matching _writeopts(; quotestrings::Bool)`; `compress=true`, `header=false`, `quotestyle="all"`, `decimal=","`, `quotechar="\""`, `missingstring=nothing` → raw `TypeError`s. Reader doors get `ArgumentError("x was removed in 1.0: …")`. |
| W3 | major | `header=false`/`header=true` on `write`/`RowWriter` → `TypeError`. 0.10 *deprecated `writeheader` in favour of `header::Bool`*; 1.0 flips back to `writeheader` and rejects the 0.10-recommended spelling. |
| W4 | major | `delim` asymmetry: readers accept `delim="::"` and `'→'`; writer refuses (`write delim must be a single byte`). 0.10 writer accepted `"::"`. A file read with `delim="::"` cannot be written back. |
| W5 | major | `dateformat=` on write is applied to every `TimeType`: a `Date` column with `"HH:MM"` → `MethodError: no method matching hour(::Date)`; a `Time` column with `"yyyy/mm/dd"` → `MethodError: days(::Time)` (0.10 same bug). `dateformat=Dict(...)` is accepted and stringified into the cell: `"Dict(:a => ""2020"")"`. |
| W6 | major | `CSV.write(sink, CSV.Chunks(src))` (no `partition`) silently writes garbage: `a,b\n[1],"CSV.CSVKernel.CompactString[""x""]"\n` (the Chunks iterator is treated as a row table whose rows are whole chunks). |
| R1 | major | `show(io, ::Rows)`, `show(io, ::Chunks)`, `show(io, ::RowWriter)` are default struct dumps that print the **entire input buffer** (`CSV.CSVApi.Rows(CSV.KernelExamples.Rows(UInt8[0x61, 0x2c, …`). REPL-displaying `CSV.Rows("1GB.csv")` prints a gigabyte. 0.10 printed `CSV.Rows("name"): Size: N …`. |
| R2 | major | Rows/Chunks refuse `select`/`drop`/`typemap`/`downcast`/`strict`/`maxwarnings` (all accepted by 0.10 Rows/Chunks; `pool` Dict/vector on Chunks too). `select`/`drop` while streaming is a real 0.10 feature loss; `lazy` already has it. |
| R3 | major | `transpose=true` silently accepts and ignores `pool="x"`, `stringtype=Int`, `on_error=:bogus`, `strict=true`, `ntasks=0`, `maxproblems=-1` (the transposed door returns before any validation and never uses `stringtype`: strings are always `String`). |
| R4 | major | `File(::LazyFile; kw...)` has no keyword validation: `on_error=:bogus` accepted silently; `maxproblems=-1` accepted unless a problem occurs; `validate=false` is overridden by the `validate=true` baked into `lf.prepared.parsekw` (so `File(lf; validate=false, types=Dict(:zzz=>Int))` still errors); `ntasks`/`maxwarnings`/`header` → raw `MethodError: no method matching File(::LazyFile; ntasks)`. Docstring says "kw are File's type/string/pool/engine keywords". |
| N1 | major | Submodule names leak everywhere users look: `typeof(f) == CSV.CSVApi.File`, `show(f)` → `CSVApi.File("…")`, every schema line → `CSV.CSVKernel.CompactString`, chunks → `CSVKernel.ParsedTable`, rows → `CSV.CSVApi.FileRow` / `CSV.CSVApi.Row`, `CSV.KernelWrite.RowWriter{…}`, error text `"CSVKernel: invalid_value at data row 1 …"`, `"stringtype must be CSVKernel.CompactString, …"`, `"lazystrings … use stringtype=CSVKernel.CompactString"`, `?CSV.File` signature `CSVApi.File(source; kw...)`. Flatten is deferred (#20) — this is the full list for that pass. |
| N2 | major | `Chunks` yields `CSVKernel.ParsedTable`: `chunk.a` → `FieldError … available fields: names, columns, nrows, problems, droppedproblems`; no `length`; `Tables.columns(::File)` also returns a `ParsedTable`. 0.10 yielded `CSV.File`. (#9 merge is the planned fix — it must land before 1.0 or Chunks should wrap in `File`.) |
| N3 | major | CompactString ergonomics: `push!(df, (a=9,b="q",…))` on `CSV.read(p, DataFrame)` → `MethodError: no method matching CompactString(::String)` (no constructor from a string); `df.b[1] = "q"` → `CanonicalIndexError: setindex! not defined for CompactStringVector`; `match(r"x", cs)` → `ArgumentError: regex matching is only available for String`; `CSV.materialize`/`CSV.CompactStringVector` not public — the only documented detach is `String.(col)`. |
| D1 | major | Docstrings: `?CSV.RowWriter` and `?CSV.problems` → "No documentation found"; `CSV.write` docstring documents **zero** keywords (0.10 documented 18; `docs/src/writing.md` is just `@docs CSV.write CSV.RowWriter`); two conflicting `Rows` docstrings (`Union{String,Missing}` default vs CompactString); `[`LazyColumn`](@ref)`, `[`sniff`](@ref)` unresolvable from `CSV`; module docstring omits `lazy`/`LazyFile`/`RowWriter`; `File` docstring omits `downcast`, `transpose`, `typemap`, `chunkbytes`, `samplebytes`, `prefetch`, `fastindex`, `scanner`; no `# Examples` anywhere. |
| R5 | major | `CSV.read(path)` (no sink) → bare `MethodError: no method matching read(::String)`; 0.10: `ArgumentError: provide a valid sink argument, like using DataFrames; CSV.read(source, DataFrame)`. `df |> CSV.write("f.csv")` (0.10 curried `write(file; kw...)`) → `MethodError: no method matching write(::String)`. |
| K1 | minor | Two spellings, inconsistently accepted: `strict`/`on_error`, `maxwarnings`/`maxproblems`, `ntasks`/`parallel`. `maxwarnings` is refused on Chunks (`unsupported Chunks keyword maxwarnings`) but `maxproblems` accepted; Rows refuses both; `File(lf)` accepts `maxproblems`, MethodErrors on `maxwarnings`; `lazy`/Rows accept `parallel` but refuse `ntasks`; `Chunks(ntasks=)` still means batches (B3 pending). |
| K2 | minor | `dateformat=dateformat"yyyy-mm-dd"` (a `DateFormat`) refused by every reader (`dateformat must be a format String (got DateFormat{…})`) but accepted by the writer; 0.10 accepted both. |
| K3 | minor | `transpose=true` defaults `delim=','` (no sniffing) and `delim=nothing` → `TypeError`; other doors default `delim=nothing` (sniff). `header=-1` there errors with `skipto must be ≥ 1 (got nothing)`. |
| K4 | minor | `header=[:a,:b]` for a 3-column file: File/Chunks record one `long_row` Problem **per row** (4 problems) and silently drop column 3; `header=[:a,:b,:c,:d]` → one `short_row` per row plus an all-`Missing` column `d`; Rows/lazy truncate silently. One header-level diagnostic (or an error) would be clearer. |
| K5 | minor | `Tables.columnnames(::Rows)` → `(:inner, :types, :limit, :stringtype)`, `Tables.columnnames(::Chunks)` → struct fields; `Tables.rowcount(::Rows)` → `MethodError: length(::CSV.KernelExamples.Rows)`. |
| K6 | minor | `Rows(types=Dict(:a=>Int))` silently changes the *other* columns' cells from `CompactString` to `String` (untyped columns are seeded `String`, ignoring `stringtype`); schema says `Union{Missing,String}`. Without `types`, cells are `CompactString`. |
| K7 | minor | Rows/lazy typed-cell failures become `missing` with **no** diagnostic and no `strict`/`on_error`; `CSV.problems(::Rows)`/`(::LazyFile)`/`(::Chunks)` → `MethodError`. File records a Problem for the same cell. Document or add a clean error/`problems` for these doors. |
| K8 | minor | `on_error=:error` throws a bare `ErrorException("CSVKernel: invalid_value at data row 1, column 1: …")`; no exception type to catch, no access to the `Problem`. |
| W7 | minor | A string cell equal to `missingstring` (e.g. `"NA"` with `missingstring="NA"`) is written unquoted → re-reads as `missing`. `""` already gets the quoted-empty treatment; extend it to the missingstring spelling. |
| W8 | minor | `nothing` cells write the literal `nothing` (`Union{Nothing,Int}` column → `1`, `nothing`). 0.10 errored with a `transform=` hint; pandas writes empty. |
| W9 | minor | `BigFloat` cells write `1.0e+40` (Base spelling), `Float64` writes `1.0e40`; re-read (Float64) then re-write differs byte-wise. Everything else in the round-trip table is byte-identical. |
| W10 | minor | `write(::FilePathsBase.AbstractPath, t)` returns a `String`, not the path (docstring `-> sink`). |
| W11 | minor | `CSV.write([io1, io2], t)` without `partition=true` → `MethodError: no method matching write(::Vector{IOBuffer}, ::Vector{UInt8})` (internal); `partition=true` with a single `String` (0.10 wrote `path_1`, `path_2`) → `partition=true needs a Vector of sinks` with no hint. |
| W12 | minor | `RowWriter`: `length`/`size` gone (0.10 `HasLength`; now `SizeUnknown`); `header=String[]` (0.10's default value) → `header has 0 names for 2 columns`; `bom=true, writeheader=false` on an empty table yields `["﻿"]`. |
| W13 | minor | `.GZ`/`.gzip` paths are not auto-gzipped (`endswith(".gz")` is case-sensitive); reader is magic-byte based so it still reads them. |
| K9 | minor | `validate=false` does not cover `select`/`drop` names (0.10: "Invalid values in select are ignored"); docstring correctly scopes it to types/dateformat/pool — decide and document. |
| K10 | minor | `lazy([p1, p2])` → `MethodError: no method matching resolvesource(::Vector{String}; …)`; `File(IOBuffer at EOF)` → silent empty table (audit A6, still pending). |
| K11 | minor | `File == File` on identical data is `false` (identity `==`/`hash`); 0.10 (AbstractVector) compared elementwise. `isempty(::LazyFile)` / `isempty(::ParsedTable)` → `MethodError` (no `iterate`). |
| K12 | cosmetic | `_checkkwargs` context string: `File(src; bogus=1)` says `unsupported File/Rows/Chunks keyword bogus`; `lazy` reports `unsupported lazy keyword` for its own list but `File/Rows/Chunks` for `_prepare`'s. |
| K13 | cosmetic | All multi-line `show` methods are on `show(io, x)`, not `show(io, ::MIME"text/plain", x)`: `[f1, f2]` and `(f,)` print multi-line blobs inside the container. |
| K14 | cosmetic | `Problem` shows as `CSV.CSVKernel.Problem(1, 1, 3, :invalid_value, "…")`; `Vector{Problem}` is not `Tables.istable` (DataFrame(problems) still works via struct iteration). |

## 2. Details

### W1 — writer rewinds and truncates seekable IO sinks (blocker)

`src/write.jl:788-796`:
```julia
seekable = sink isa IO && hasmethod(seek, Tuple{typeof(sink), Integer}) && hasmethod(seekend, …)
seekable && (append ? seekend(sink) : seekstart(sink))
emit(sink)
!append && seekable && applicable(truncate, sink, position(sink)) && truncate(sink, position(sink))
```
`IOStream` is seekable, and a redirected `stdout` **is an `IOStream`**.

Repro (`$SP/api/stdout_new.jl`):
```julia
println("LINE1 before"); println("LINE2 before")
CSV.write(stdout, (a=[1,2], b=["x","y"]))
println("LINE3 after")
```
* `julia s.jl > log` → log = `a,b\n1,x\n2,y\nLINE3 after` (LINE1/LINE2 destroyed).
* `echo PRE > log; julia s.jl >> log` → log = `PRE-EXISTINGLINE3 after` (the CSV output vanished: O_APPEND writes went to the end, then `truncate(position)` cut the file at the pre-write position).
* `julia s.jl | cat` → correct (pipes aren't seekable).
* 0.10 same script `> log` → `a,b\n1,x\n2,y\nLINE3 after\n<garbage>LINE2 before` (rewound, overwrote, no truncate).
* `open(p,"w") do io; write(io,"PRIOR-LONGER-CONTENT-LINE\n"); CSV.write(io, tbl) end` → file is `a,b\n1,x\n2,y\n` (prior content gone). `open(p,"r+") do io; seekend(io); CSV.write(io,t) end` → same loss.
* Also: `CSV.write(gz::GzipCompressorStream, t)` then `close(gz)` (default `stop_on_end=false`) closes the caller's IOBuffer → `take!` yields a truncated member (`ZlibError`). Works with `stop_on_end=true` or `TOKEN_END`+`flush`. 0.10 documented "a compression stream can always be provided as the first file argument"; document the `stop_on_end=true` requirement or detect `TranscodingStream` and finish the member.

Expected: an IO sink is written at its current position, like `Base.write`; `append` only chooses whether the header is emitted (and file mode for paths). Recommendation: delete the seek/truncate block; if "replace an IOBuffer" is wanted, limit it to `Base.GenericIOBuffer` with an explicit opt-in (and never `truncate` a stream the caller owns). Add a test that writes to an `IOStream` holding prior content.

### A1 — `File` column indexing and vector surface (blocker)

0.10: `struct File <: AbstractVector{Row}` with `getindex(::File, ::Symbol)`, `getindex(::File, ::String)`, `size`, `length`, `f[end]`, `f[1:2]`, `f.names`. 1.0 `File` is a plain struct with `getindex(::File, ::Int)` only (`api.jl:1596`).

| call | 0.10 | 1.0 |
|---|---|---|
| `f[:a]` | column | `MethodError: no method matching getindex(::CSV.CSVApi.File, ::Symbol)` |
| `f["a"]` | column | `MethodError … ::String` |
| `f.names` | names | `FieldError: type CSV.CSVApi.File has no field names` |
| `size(f)` | `(nrows,)` | `MethodError` |
| `f[end]`, `f[1:2]`, `f[[1,2]]`, `last(f)` | rows | `MethodError` (no `lastindex`) |
| `filter(r->…, f)`, `findall` | works | `MethodError` |
| `length(f)`, `f[1]`, `f.a`, `map`, `first`, iteration | works | works |

Recommendation: at minimum `getindex(::File, ::Symbol)`, `getindex(::File, ::AbstractString)`, `Tables.getcolumn(::File, ::AbstractString)`, `lastindex`/`firstindex`, `getindex(::File, ::AbstractVector{Int})`/`UnitRange` → `Tables.subset`. Decide explicitly whether `File <: AbstractVector{Row}` is kept (it is what made `filter`/`f[end]`/`size` work) and say so in the migration guide. Same `getindex(::Symbol/::String)` gap on `LazyFile` (`lf["a"]` fails; `lf[:a]` works), `FileRow["a"]`, `Row["a"]`.

### S1 — scan door broken against dev'd Tables (blocker; environment)

```
julia> CSV.File(IOBuffer("a,b\n1,2\n"); scan=Tables.Scan(select=[:a]))
ERROR: MethodError: no method matching bind(::Tables.Scan, ::Vector{Symbol})
```
`~/.julia/dev/Tables` is on `jq/scan` (head `0a8ae5e`), whose `scan.jl` defines `Tables.resolve(scan, names) -> BoundScan` (line 470); no `bind` exists. `src/scan.jl:94` calls `Tables.bind`. `Tables.scan(f, Scan)` (generic) works, so only the pushdown door is affected. Fix: `b = Tables.resolve(scan, names)`; run `test/scan.jl`.

### W2/W3 — writer keyword hygiene (major)

0.10 → 1.0 mapping and what a 0.10 caller sees today (`_writeopts` has no `_LEGACYKW`; every kw signature is strictly typed):

| 0.10 keyword | 1.0 | today's error |
|---|---|---|
| `quotestrings=true` | `quotestyle=:all` | `MethodError: no method matching _writeopts(; quotestrings::Bool)` |
| `transform=(col,val)->…` | dropped | `MethodError … _writeopts(; transform::…)` |
| `bufsize=…` | dropped | `MethodError … _writeopts(; bufsize::Int64)` |
| `compress=true/false` | `compress=:gzip/:none/:auto` | `TypeError: in keyword argument compress, expected Symbol, got a value of type Bool` |
| `header=false` (0.10-recommended; `writeheader` was `depwarn`ed) | `writeheader=false` | `TypeError: in keyword argument header, expected Union{Nothing, Vector}, got a value of type Bool` |
| `delim="::"` | refused | `ArgumentError: write delim must be a single byte (got "::")` |
| `missingstring=nothing` (reader accepts) | — | `TypeError … expected AbstractString, got Nothing` |
| `quotestyle="all"`, `decimal=","`, `quotechar="\""`, `escapechar="\\"` | — | `TypeError` (String where Char/Symbol expected) |
| `bogus=1` | — | `MethodError … _writeopts(; bogus::Int64)` |
| `RowWriter(t; append/compress/partition/ntasks)` | — | `MethodError … _writeopts(; append::Bool)` |

0.10 also silently ignored unknown writer kwargs (`LegacyCSV.write(io, t; bogus=1)` → wrote normally), so 0.10 code with typos now breaks with an internal-function MethodError.

Recommendation: a `_checkwritekwargs` mirroring the reader's: `quotestrings` → "use quotestyle=:all", `transform` → "removed; map the table first (e.g. `Tables.transform`/`map`)", `bufsize` → "removed: the writer streams blocks", `compress::Bool` → accept and map (`true`⇒`:gzip`, `false`⇒`:none`) or error with the Symbol hint, `header::Bool` → map to `writeheader` (keep 0.10's spelling working; pick ONE of `header::Bool`/`writeheader` as documented), unknown → `ArgumentError("unsupported write keyword x")`. Loosen `quotestyle::Union{Symbol,AbstractString}`, `decimal/quotechar::Union{Char,AbstractString}` (1 byte), `missingstring::Union{Nothing,AbstractString}`; every rejection an `ArgumentError` naming the keyword.

### W4 — multi-byte delimiter asymmetry

`CSV.File(IOBuffer("a::b\n1::2\n"); delim="::")` → `[:a, :b]`; `CSV.File(...; delim='→')` works. `CSV.write(io, t; delim="::")` → `ArgumentError: write delim must be a single byte (got "::")`; 0.10: `a::b\n1::x\n2::y\n`. Recommend a byte-sequence delimiter in the writer (quote when the sequence occurs; the `:none` structural scan generalises) — the Dialect struct already models `delim::Union{UInt8,Vector{UInt8}}`.

### W5 — `dateformat` on write

| format | Date col | DateTime col | Time col |
|---|---|---|---|
| `"yyyy/mm/dd"` | `2020/01/02` | `2020/01/02` | `MethodError: no method matching days(::Time)` |
| `"HH:MM"` | `MethodError: hour(::Date)` | `03:04` | `03:04` |
| `"yyyy-mm-ddTHH:MM:SS"` | `MethodError: hour(::Date)` | ok | `MethodError: days(::Time)` |

Any table with both a `Date` and a `Time` column cannot use `dateformat` at all. `dateformat=Dict(:a=>"yyyy")` is accepted and produces the cell `"Dict(:a => ""2020"")"`. Recommend: apply the format only where its tokens are representable (Date-only formats to Date/DateTime, time-only to Time/DateTime), else fall back to `string(x)`; accept a `Dict{Type,…}`/per-column Dict like the reader; reject other types with an ArgumentError.

### W6 — `CSV.write(sink, ::Chunks)`

```
julia> String(take!(CSV.write(IOBuffer(), CSV.Chunks(IOBuffer("a,b\n1,x\n")))))
"a,b\n[1],\"CSV.CSVKernel.CompactString[\"\"x\"\"]\"\n"
```
`Tables.istable(Chunks)` is false; `Tables.columns(ch)` falls back to iterating the chunks as rows. Recommend `write(sink, c::Chunks; kw...)` = write each partition (header once, then `append=true`), or an `ArgumentError` pointing at `partition=true`. (`partition=true` with a matching sink vector works; `CSV.write([io], CSV.Chunks(src; ntasks=1); partition=true)` → correct.)

### R1 — struct-dump `show` for Rows/Chunks/RowWriter

`sprint(show, CSV.Rows(src))` = `CSV.CSVApi.Rows(CSV.KernelExamples.Rows(UInt8[0x61, 0x2c, 0x62, …<every byte of the file>…], CSV.CSVKernel.ChunkIndex[…], [:a,:b,:c], Dict(…), CSV.CSVKernel.ValueOpts(…DatePattern…)), nothing, nothing, CSV.CSVKernel.CompactString)`. Same for `Chunks` (`Batches(UInt8[…`) and `RowWriter` (whole `WriteOpts`). Displaying a `CSV.Rows` over a large file in the REPL prints the file. 0.10: `CSV.Rows("<IOBuffer>"):\nSize: 2\nTables.Schema: …`. Add `show` methods (name, ncols, schema for Rows; name, nbatches, schema for Chunks; names/opts summary for RowWriter).

### R2 — keyword parity for Rows/Chunks (see matrix §5)

Refused today (0.10 accepted): Rows — `select`, `drop`, `typemap`, `downcast`, `strict`, `maxwarnings`, `pool` (0.10: inert), `transpose`, `ntasks`; Chunks — `select`, `drop`, `typemap`, `downcast`, `strict`/`on_error`, `maxwarnings`, `pool=Dict/Vector`, `transpose`, `source`. Messages are `unsupported Rows keyword select`. Recommend: `select`/`drop` on Rows/Chunks (lazy already has the list machinery); `typemap`+`nsample` on Chunks; `maxproblems`/`on_error` on Chunks (it already carries a `ProblemLog`); for the rest either support or add them to a per-door "not supported by Rows" message that says why.

### R3 — `transpose=true` ignores validation

`CSV.File(src; transpose=true, ntasks=0, pool="x", on_error=:bogus, stringtype=Int, strict=true, maxproblems=-1)` → returns a File. `File(...)` returns to `_transposedfile` (`api.jl:899`) before `ntasks`/`stringtype`/`on_error` checks; `_transposedfile` receives `stringtype` and never uses it (strings are always `String`; `stringtype=InlineString` also yields `String`). Also `footerskip`, `chunkbytes`, `fastindex`, `scanner`, `typemap`, `nsample` → "unsupported File(transpose=true) keyword". Recommend: validate first, refuse `pool`/`stringtype≠String`/`on_error`/`ntasks` explicitly with "not supported with transpose=true", and document the door's reduced surface.

### R4 — `File(::LazyFile)` validation

`CSV.File(CSV.lazy(src); on_error=:bogus)` → File (no check). `CSV.File(CSV.lazy(IOBuffer("a\nx\n")); types=Int, on_error=:bogus)` → File (should have errored or collected). `CSV.File(CSV.lazy(src); maxproblems=-1)` → File; only with a problem present → `ArgumentError: maxproblems must be ≥ 0`. `CSV.File(CSV.lazy(src); validate=false, types=Dict(:zzz=>Int))` → `ArgumentError: types key zzz does not match any column` because `_filefromprepared` splats `p.parsekw` (which carries `validate=true` from `lazy`) after its own `validate`. `ntasks`, `maxwarnings`, `strict`… `strict` works, `ntasks`/`maxwarnings`/`header` → `MethodError: no method matching File(::LazyFile; ntasks::Int64)`. Recommend routing through the same validation block as `File(source)` and a `_checkkwargs("File(::LazyFile)", …)` with a "fixed when lazy() was called" message for positional/dialect keys.

### N1 — name leaks (list for the flatten pass)

* `typeof`: `CSV.CSVApi.File`, `CSV.CSVApi.LazyFile`, `CSV.CSVApi.Rows`, `CSV.CSVApi.Chunks`, `CSV.CSVApi.FileRow`, `CSV.CSVApi.Row`, `CSV.CSVApi.LazyColumn`, `CSV.CSVApi.Spec`, `CSV.CSVKernel.ParsedTable`, `CSV.CSVKernel.Problem`, `CSV.CSVKernel.CompactString`, `CSV.CSVKernel.CompactStringVector`, `CSV.KernelWrite.RowWriter`, `CSV.KernelExamples.Rows/Batches/RowView`.
* `show`: `CSVApi.File("…"):`, `CSVKernel.ParsedTable: 3 × 3`, `CSVApi.Spec(delim=…)`, `CSV.CSVApi.FileRow:` / `CSV.CSVApi.Row:` (Tables' AbstractRow show uses `typeof`), every `Tables.Schema` line `CSV.CSVKernel.CompactString`, `Vector{CSV.CSVKernel.Problem}`, `3-element CSV.CSVKernel.CompactStringVector{CSV.CSVKernel.CompactString}`, `CSV.CSVApi.LazyColumn{Union{Missing, CSV.CSVKernel.CompactString}, CSV.CSVKernel.CompactString}`; `summary(f)` = `"CSV.CSVApi.File"`. (`CSV.LazyFile(…)` show already uses the public name — the one exception.)
* Error text: `CSVKernel: invalid_value at data row 1, column 1: …`; `stringtype must be CSVKernel.CompactString, String, or …`; `lazystrings was removed in 1.0: use stringtype=CSVKernel.CompactString (the default) …`; `FieldError: type CSV.CSVKernel.ParsedTable has no field a, available fields: names, columns, …`; `no method matching CSV.CSVApi.File(::CSV.CSVApi.LazyFile; ntasks)`; `no method matching length(::CSV.KernelExamples.Rows)`; `no method matching _writeopts(; …)`; `no method matching resolvesource(::Vector{String}; …)`.
* Docstrings: signature lines `CSVApi.File(source; kw...)`, `CSVApi.read(source, sink; kw...)`, `CSVApi.lazy(…)`, `CSVApi.LazyFile`, `CSVApi.Rows(…)` (×2), `CSVApi.Chunks(…)`, `CSVApi.Spec`, `CSVApi.sniff`, `KernelWrite.write(sink, table; kw...) -> sink`, `KernelWrite.RowWriter(…)`; module docstring names `CSV.CSVKernel` and `CSV.CSVApi`; File docstring says columns come back as `CSV.CSVKernel.CompactStringVector`.
* `methods(CSV.write)` → `from CSV.KernelWrite`; `parentmodule(CSV.File)` → `CSV.CSVApi`.

### N2 — `Chunks` element type / `Tables.columns(::File)`

`eltype(CSV.Chunks) == CSV.CSVKernel.ParsedTable`; `chunk.a` → `FieldError` listing internal fields; `chunk.names`/`chunk.nrows` are the struct fields (so a column named `names` is shadowed the *other* way round from `File`, where columns win); `chunk[:a]` works, `chunk[1]` doesn't; no `length`; `show` → `CSVKernel.ParsedTable: 3 × 3 … (1 problem(s) recorded)`. `Tables.columns(f::File)` returns the `ParsedTable`, so generic code that does `Tables.columns(f).col` fails while `f.col` works. The #9 plan (File absorbs ParsedTable) resolves both; until then `Chunks` should yield `File(name, t, lookup)` (one constructor call per batch).

### N3 — CompactString consequences (given the settled default)

* `df = CSV.read(p, DataFrame)`: `typeof(df.b) == CompactStringVector{CompactString}`; `push!(df, (a=9, b="q", c=Date(2020)))` → `MethodError: no method matching CSV.CSVKernel.CompactString(::String)` (DataFrames logs "Error adding value to column :b"); `df.b[1] = "q"`, `df[1,:b] = "q"` → `CanonicalIndexError: setindex! not defined`; `df.b .= "q"`, `sort!`, `filter!`, `deleteat!`, `append!`, `vcat` all work (they replace the column). `DataFrame(CSV.File(p))` copies to `Vector{CompactString}` — mutable, but `push!` still fails for lack of a `CompactString(::AbstractString)` constructor.
* `match(r"x", cs)` → `ArgumentError: regex matching is only available for the String and AnnotatedString types`. `startswith`, `occursin`, `split`, `replace`, `uppercase`, `*`, `parse(Int, cs)`, `Date(cs)`, `Symbol(cs)`, Dict lookup by `String`, `==`/`hash`/`isequal` with `String`, `cs in ["x"]`, `f.b .== "x"`, `sort`, `unique`, `view` all work.
* Detaching: `String.(col)`, `collect(col)`, `copy(col)`, `convert(Vector{String}, col)`, `Vector{String}(col)` all give `Vector{String}`; `CSV.CSVKernel.materialize` works but is internal; the CompactString docstring says "`materialize` on the column" and "exactly like today's `PosLenString`".
Recommendations: (1) define `CompactString(::AbstractString)` (inline ≤12 bytes, else own a copy) — unblocks `push!`, `convert`, `similar`-based algorithms; (2) export `CSV.materialize(col)` (or document `String.(col)` as the idiom) and mention it in `read`'s docstring next to `Tables.CopiedColumns`; (3) `Base.match`/`eachmatch` on `CompactString` via `String(s)` fallback; (4) migration-guide entry: "for a mutable DataFrame pass `stringtype=String`".

### D1 — docstrings

* `?CSV.RowWriter` → `No documentation found for private binding CSV.RowWriter` (docs are attached to the UnionAll in `KernelWrite`; the alias resolves for `DataType`s and `Function`s only). `?CSV.problems` → `No documentation found` (no docstring exists).
* `CSV.write` docstring (278 chars) documents none of `delim, quotechar, openquotechar, closequotechar, escapechar, newline, missingstring, quotestyle, floatformat, dateformat, decimal, bom, append, writeheader, header, compress, ntasks`; `docs/src/writing.md` is only `@docs CSV.write CSV.RowWriter`, so the 1.0 writing page would be empty of options.
* Two `Rows` docstrings: struct doc says cells are `Union{String, Missing}` by default; constructor doc says `CompactString` (actual: `CompactString`). `Chunks` doc: "sequence of `File`-shaped tables" (actual `ParsedTable`).
* `@ref`s that don't resolve from `CSV`: `[`LazyColumn`](@ref)` (LazyFile doc; `CSV.LazyColumn` undefined), `[`sniff`](@ref)` (Spec doc). Documenter needs `CSV.File`-style targets; signature lines say `CSVApi.File`.
* Module docstring: lists File/read/Rows/Chunks/write/problems only; no `lazy`, `LazyFile`, `RowWriter`, `CompactString`.
* `File` docstring keyword list omits `downcast`, `transpose`, `typemap`, `chunkbytes`, `samplebytes`, `prefetch`, `fastindex`, `scanner`; no defaults are given for most keywords; no `# Examples` section in any public docstring (0.10 had examples on File/read/write).
* `read` docstring: `read(path, columntable)` should be `Tables.columntable`; `Tables.CopiedColumns` consequence (immutable string columns) unmentioned.
* `File(lf::LazyFile)` docstring claims "File's type/string/pool/engine keywords" — `ntasks`/`maxwarnings` MethodError (R4).
* `CompactString` docstring references `materialize` (internal) and `PosLenString` (0.10 type).

### R5 — `CSV.read(path)` / curried `CSV.write`

`CSV.read(tmp)` → `MethodError: no method matching read(::String)` (0.10: `ArgumentError: provide a valid sink argument, like using DataFrames; CSV.read(source, DataFrame)`). Recommend restoring the guiding error (returning a `File` would blur `File`/`read`; pandas-habit users get a one-line pointer instead). `tbl |> CSV.write(path)` → `MethodError: no method matching write(::String)`; 0.10 defined `write(file; kw...) = x -> write(file, x; kw...)` and its docstring shows `table |> CSV.write(file)`. Restore the one-liner or list it in the migration guide. Also `CSV.read(path, Matrix)` → `MethodError: (Matrix)(::Tables.CopiedColumns{…})` (audit A2's `read(source, ::Type{<:AbstractMatrix})` not done); `CSV.read(path, Tables.matrix)`/`rowtable`/`columntable`/`dictrowtable`/`NamedTuple`/closures work.

### K1 — one spelling per concept

Recommendation for 1.0: public knobs `on_error`, `maxproblems`, `ntasks`; move `strict`, `maxwarnings` to `_LEGACYKW` ("use on_error=:error" / "use maxproblems"), and make `parallel` internal (derive from `ntasks`; today `lazy`/`Rows` accept `parallel` but refuse `ntasks`, `File` accepts both with `ntasks` only checked `≥ 1` and otherwise only deciding `parallel`). If both stay: accept both on every door that accepts one. Precedence today when both given: `maxproblems` wins over `maxwarnings` (7 retained with `maxwarnings=3, maxproblems=7`); `on_error` wins over `strict` (`strict=true, on_error=:collect` collects). `Chunks(ntasks=)` = number of batches (B3 rename to `nchunks` pending, not done).

### K2/K3/K4/K5/K6/K7/K8 (reader minors)

* K2: `dateformat=dateformat"yyyy-mm-dd"` → `ArgumentError: dateformat must be a format String (got DateFormat{Symbol("yyyy-mm-dd"), …})` on File/read/Vec/tr/lazy/Rows/Chunks/sniff; the writer accepts it. The format string is recoverable as `string(typeof(df).parameters[1])`.
* K3: `File(src; transpose=true, delim=nothing)` → `TypeError: in keyword argument delim, expected Union{Char, String}, got Nothing`; `header=-1` → `skipto must be ≥ 1 (got nothing)`.
* K4: `File(src; header=[:a,:b])` (3 data columns) → names `[:a,:b]`, problems = 4 × `long_row "expected 2 fields, found 3 (extra fields ignored)"`; `header=[:a,:b,:c,:d]` → 4 × `short_row`, `eltype(f.d) == Missing`. Rows/lazy: silent.
* K5: `Tables.columnnames(CSV.Rows(src))` → `(:inner, :types, :limit, :stringtype)`; `Tables.rowcount(rs)` → `MethodError: length(::CSV.KernelExamples.Rows)`.
* K6: `first(CSV.Rows(src; types=Dict(:a=>Int)))` → `(a=1::Int64, b::String)`; without `types` → `b::CompactString`. `api.jl:1873` seeds `String` for unspecified columns; seed `stringtype` instead.
* K7: `first(CSV.Rows(IOBuffer("a\nx\n"); types=Int)).a === missing`, `collect(CSV.lazy(IOBuffer("a\nx\n1\n"); types=Int).a) == [missing, 1]`, no Problem, `strict`/`on_error` refused; `CSV.problems(rs)` → `MethodError`. File: `[missing, 1]` + 1 Problem.
* K8: `try CSV.File(…; on_error=:error) catch e; typeof(e) end == ErrorException`; message `CSVKernel: invalid_value at data row 1, column 1: cannot parse Int64 from "x" (+1 more)`. Suggest `CSV.ParseError <: Exception` carrying the first `Problem` (pandas `ParserError`, polars `ComputeError`).

### W7–W13 (writer minors)

* W7: `CSV.write(io, (a=["NA", missing, "x"],); missingstring="NA")` → `a\nNA\nNA\nx\n`; re-read with `missingstring="NA"` → `[missing, missing, "x"]`. Quote the literal (as `""` is quoted) for a lossless round trip.
* W8: `(x=[1, nothing],)` → `x\n1\nnothing\n`; `Any[1,"x",2.5,missing,nothing,:s]` → `1,x,2.5,,nothing,s`. Pick: treat `nothing` as missing, or error as 0.10 did.
* W9: round-trip table (`$SP/api/writer.jl` D4): all 13 columns re-read with empty `problems`, correct eltypes, equal values (`NaN`, `-0.0`, `1e21`, `1e-7`, ms/µs `DateTime`/`Time`, quotes/newlines/leading-space strings, `""` vs `missing`, `Int128`), and `write(File(write(t)))` is byte-identical **except** `BigFloat` (`1.0e+40` vs `1.0e40`; the column re-reads as `Float64`).
* W10: `typeof(CSV.write(Path("x.csv"), t)) == String`.
* W11: messages quoted in the table.
* W12: `Base.IteratorSize(CSV.RowWriter) == SizeUnknown()`; `length(rw)` → `MethodError`; 0.10 `HasLength()`, `length == nrows + 1`. `RowWriter` streams (a side-effecting generator advanced 1 row at construction for schema-less sources, then lazily); `first(rw)` restarts; `collect` twice works.
* W13: `.GZ` → plain text (`compress=:auto` matches `endswith(".gz")` only).

### Minor consistency notes not in the table

* `missingstring=""` / `[""]` / `nothing` on readers are all no-ops (documented pin B1 pending); `missingstring=1` → `missingstring entries must be strings (got Int64)` (good).
* `pool=fn`, `types=fn`, `select=fn`: `select` gets a purpose-built message (`function-typed select/drop is retired; pass a list (or use Tables.Scan for expressions)`), `types=fn` gets `unsupported types specification: var"#39#40"`, `pool=fn` gets `pool spec must be Bool, Real, (Real, Integer), or nothing (got var"#41#42")`. Give all three the retired-function message.
* `File(Vector{UInt8}[])`/`IOBuffer("")` → empty File (`Symbol[]`, 0 rows); `Tables.matrix` of a 0-column table → `reducing over an empty collection` (Tables.jl).
* `source=` forms all work as documented; `File(tmp; source=:src)` (non-vector) → `unsupported File/Rows/Chunks keyword source` (fine, but the message could say "source= only applies to a vector of sources").
* `types=Dict(:b=>String7)` → `unsupported column type String7`, `types=Dict(:b=>CompactString)` → `unsupported column type`; only `String` is the string spelling in `types` while `stringtype` takes the width — document.
* `write` `quotestyle=:none` with an empty **header** name → `quotestyle=:none cannot distinguish an empty string from missing` (message is about cells).
* Names: `CSV.lazy` (verb) vs `File`/`Rows`/`Chunks` (constructors). `CSV.LazyFile` is already exported; recommend `CSV.LazyFile(source; kw...)` as the door (0-cost alias of `lazy`) so all four readers are constructors, keep `lazy` only if a verb is wanted. `FileRow` vs `Row` (0.10 `Row`/`Row2`): recommend `CSV.Row` for the File row (documented 0.10 name) and a parametric `Row{S}` (or `CSV.StreamRow`) for `Rows`; neither `CSV.Row` nor `CSV.FileRow` is bound today (`isdefined(CSV, :Row) == false`). `CSV.Problem` is bound (via `using .CSVKernel`) but prints as `CSV.CSVKernel.Problem`. `on_error=:collect|:error` is fine against pandas `on_bad_lines=error|warn|skip`, polars `ignore_errors`, readr `problems()`; a future `:skip` slots in.

## 3. Keyword × door matrix (reader)

Legend: ok = accepted; LEG = `ArgumentError("x was removed in 1.0: …")`; UNS = `ArgumentError("unsupported <door> keyword x")`; REF = other `ArgumentError`; ME = `MethodError` (raw); TE = `TypeError` (raw). Value used in each probe is in `$SP/api/kwmatrix.jl`. `File(lf)` = `File(::LazyFile)`; `File(tr)` = `transpose=true`.

| keyword | File | read | File(Vec) | File(tr) | lazy | File(lf) | Rows | Chunks | sniff |
|---|---|---|---|---|---|---|---|---|---|
| `header=1/true/false` | ok | ok | ok | ok | ok | ME | ok | ok | UNS |
| `header=[:a,:b,:c]` / `["a","b","c"]` | ok | ok | ok | REF (count vs transposed rows) | ok | ME | ok | ok | UNS |
| `header=[1]` / `1:1` | ok | ok | ok | REF (single field index only) | ok | ME | ok | ok | UNS |
| `normalizenames` | ok | ok | ok | ok | ok | ME | ok | ok | UNS |
| `datarow` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `skipto` | ok | ok | ok | ok | ok | ME | ok | ok | UNS |
| `footerskip` | ok | ok | ok | UNS | ok | ME | ok | ok | UNS |
| `transpose=false` | ok | ok | ok | ok | UNS | ME | UNS | UNS | UNS |
| `comment`, `ignoreemptyrows`, `ignorerepeated`, `quoted`, `quotechar`, `open/closequotechar`, `escapechar` | ok | ok | ok | ok | ok | ME | ok | ok | ok |
| `ignoreemptylines` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `select` / `drop` (list) | ok | ok | ok | REF | ok | ok | UNS | UNS | UNS |
| `select=fn` | REF (retired) | REF | REF | REF | REF | REF | UNS | UNS | UNS |
| `limit` | ok | ok | ok | ok | ok | ME | ok | ok | UNS |
| `buffer_in_memory`, `prefetch` | ok | ok | ok | ok | ok | ME | ok | ok | ok |
| `threaded`, `tasks`, `rows_to_check`, `lines_to_check` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `ntasks` | ok | ok | ok | ok (ignored) | UNS | ME | UNS | ok (=batches) | UNS |
| `parallel` | ok | ok | ok | ok (ignored) | ok | ok | ok | ok | UNS |
| `missingstrings` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `missingstring` (str / vec / nothing) | ok | ok | ok | ok | ok | ME | ok | ok | ok |
| `delim=Char` / `String` | ok | ok | ok | ok | ok | ME | ok | ok | UNS |
| `delim=nothing` (sniff) | ok | ok | ok | **TE** | ok | ME | ok | ok | UNS |
| `dateformat=String` | ok | ok | ok | ok | ok | ME | ok | ok | ok |
| `dateformat=DateFormat` | **REF** | REF | REF | REF | REF | ME | REF | REF | REF |
| `dateformat=Dict` | ok | ok | ok | REF | ok | ME | ok | ok | REF |
| `dateformats` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `decimal`, `groupmark`, `truestrings`, `falsestrings`, `stripwhitespace` | ok | ok | ok | ok | ok | ME | ok | ok | ok |
| `type` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `types=Type/Dict` | ok | ok | ok | ok | ok | ok | ok | ok | UNS |
| `types=Vector` | ok | ok | ok | REF (length = transposed cols) | ok | ok | ok | ok | UNS |
| `types=fn` | REF | REF | REF | REF | REF | REF | REF | REF | UNS |
| `typemap` | ok | ok | ok | UNS | UNS | ME | UNS | UNS | ok |
| `pool=true/ratio/(r,cap)` | ok | ok | ok | ok (ignored) | UNS | ok | UNS | ok | UNS |
| `pool=Dict/Vector` | ok | ok | ok | ok (ignored) | UNS | ok | UNS | REF (single policy) | UNS |
| `pool=fn` | REF | REF | REF | ok (ignored) | UNS | REF | UNS | REF | UNS |
| `downcast` | ok | ok | ok | ok | UNS | ok | UNS | UNS | UNS |
| `lazystrings` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `stringtype=String/CompactString` | ok | ok | ok | ok (ignored) | ok | ok | ok | ok | UNS |
| `strict` | ok | ok | ok | ok (ignored) | UNS | ok | UNS | UNS | UNS |
| `on_error` | ok | ok | ok | ok (ignored) | UNS | ok (unvalidated) | UNS | UNS | UNS |
| `silencewarnings`, `debug`, `parsingdebug` | LEG | LEG | LEG | LEG | LEG | ME | LEG | LEG | LEG |
| `maxwarnings` | ok | ok | ok | ok (ignored) | UNS | ME | UNS | UNS | UNS |
| `maxproblems` | ok | ok | ok | ok (ignored) | UNS | ok (unvalidated) | UNS | ok | ok |
| `validate` | ok | ok | ok | ok | ok | ok (overridden) | ok | ok | UNS |
| `reusebuffer` | UNS | UNS | UNS | UNS | UNS | ME | ok (inert) | UNS | UNS |
| `source` | UNS | UNS | ok | UNS | UNS | ME | UNS | UNS | UNS |
| `nsample` | ok | ok | ok | UNS | UNS | ME | UNS | UNS | ok |
| `chunkbytes`, `fastindex`, `scanner` | ok | ok | ok | UNS | ok | ME | ok | ok | UNS/ok |
| `samplebytes` | ok | ok | ok | UNS | ok | ME | ok | ok | ok |
| `scan` | **ME (S1)** | ME | ME | REF | UNS | ME | UNS | UNS | UNS |
| `bogus` | UNS | UNS | UNS | UNS | UNS | ME | UNS | UNS | UNS |

Defaults per door: `header` 1 (`_prepare`) / `true` (transpose, `KernelExamples`, `KernelScan`) — equivalent externally; `pool=false` everywhere (Chunks: scalar only); `stringtype=CompactString` everywhere except transpose (always `String`); `parallel = nthreads()>1` (File/lazy/Rows/File(lf)), `ntasks=nothing` (File), `ntasks=nthreads()` = batches (Chunks), `ntasks` default in `write` = `nthreads()`; `maxproblems=10_000` (File, Chunks, File(lf), scan); `on_error=:collect`; `limit=nothing`; `delim=nothing` (sniff) except transpose `','`; `samplebytes=65536`; `missingstring=nothing`; `validate=true`; `buffer_in_memory=false`; `prefetch=true`.

Validation coverage: `limit ≥ 0` — all doors ✓ (transpose too). `maxproblems ≥ 0` — File/read/Vec/Chunks/sniff ✓; transpose ✗ (ignored); File(lf) only when a problem occurs. `pool` spec — File/read/Vec/Chunks/File(lf) ✓; transpose ✗. `stringtype` — all ✓ except transpose. `ntasks ≥ 1` — File/read/Vec/Chunks/write ✓; transpose ✗; File(lf) MethodError. `on_error ∈ (:collect,:error)` — File/read/Vec ✓; File(lf)/transpose ✗. `validate=false` honoured for types/pool/dateformat on File/read/Vec/Rows/Chunks/lazy/transpose; not on File(lf); never for `select`/`drop`.

0.10 keywords with no 1.0 handling: reader — none (every 0.10 File/Rows/Chunks keyword is accepted, mapped, or in `_LEGACYKW`; `reusebuffer` accepted on Rows only). Writer — `bufsize`, `quotestrings`, `transform` (MethodError), `compress::Bool`, `header::Bool` (TypeError); `RowWriter` additionally has no handling for `append`/`compress`/`partition`/`ntasks` (0.10 also didn't accept them; the error is now an internal MethodError).

## 4. Keyword × door matrix (writer)

| keyword | write | RowWriter | note |
|---|---|---|---|
| `bufsize`, `quotestrings`, `transform`, `bogus` | ME `_writeopts` | ME | W2 |
| `delim=Char` / 1-byte String | ok | ok | |
| `delim="::"` / `'→'` | REF | REF | W4 |
| `quotechar`, `openquotechar`/`closequotechar`, `escapechar` (Char) | ok | ok | String forms → TE |
| `missingstring::AbstractString` | ok | ok | `nothing`/Symbol → TE |
| `dateformat=String` / `DateFormat` | ok | ok | Dict accepted, garbage (W5) |
| `append` | ok | ME | |
| `compress=:gzip/:none/:auto` | ok | ME | `true/false/"gzip"` → TE |
| `writeheader` | ok | ok | |
| `header=Vector` | ok | ok | `Bool`/`Int`/Tuple → TE (W3) |
| `newline` (Char/String, incl. `""`, `","`, `"ab"`) | ok | ok | no validation at all |
| `quotestyle=:minimal/:all/:none` | ok | ok | `"all"` → TE; `:bogus` → REF |
| `decimal=Char` | ok | ok | affects floats only (ints untouched; `1//2` untouched); with `floatformat` the replacement applies to the formatted string |
| `bom` | ok | ok | suppressed with `append=true` on `write`; RowWriter prefixes first line |
| `partition` | ok (Vector sinks only) | ME | |
| `floatformat` (printf string) | ok | ok | `"abc"` → `ArgumentError: Number of format specifiers … differ` (Printf's message); `%d` truncates; padding not quoted |
| `ntasks` | ok (≥1 checked) | ME | |

Writer cell rendering (all verified, `$SP/api/writer.jl` D1b): Char → `a` (and `","` quoted); Symbol → `a`; Nothing → `nothing` (W8); NaN/Inf/-Inf/-0.0 → `NaN`,`Inf`,`-Inf`,`-0.0`; Float64/Float32/Float16 byte-equal to `string(x)`; BigFloat → `string` (`1.0e+40`, 80-digit `1e-30`); BigInt/Int128/UInt8/typemin → digits; Rational → `1//3`; Complex → `1.0 + 2.0im`; UUID → canonical; Time → `01:02:03`, `.004`, `.004005006`; DateTime → `.006`, `.060`, `.600`, none when 0; Date → `-0001-01-01`, `12345-01-01`; Bool → `true`/`false`; Vector/Tuple → `string` (quoted when containing `,`); custom struct → `string(x)`; String with delim/quote/newline/leading or trailing space → quoted; `""` → `""` (quoted) vs missing → empty; CR-only inside a string → quoted; `\t`, `\0`, backslash unquoted; SubString, InlineString, CompactString (incl. missing), CategoricalArray (incl. missing), PooledArray, `Vector{Any}`, all-missing column all fine. `quotestyle=:all` quotes strings/Symbols/Chars/`nothing`/Periods/Regex/Types, not numbers/Bools/missing. `quotestyle=:none` → `ArgumentError` on any structural byte or empty string (documented). Header names with space/quote/delim/newline/empty/unicode → quoted per the same rule (`a b,"q""q","c,d","n\nl","",ü`). Shapes: 0 rows → header only; 0 columns → `""`; `Tables.table(matrix)` → `Column1,…`; row tables (Vector{NamedTuple}, heterogeneous, `Vector{Any}`, Dict rows, `dictrowtable` union of keys, generators incl. empty, `Iterators.map`) all fine; bare Matrix/Vector/Int/String → Tables' own ArgumentError; ragged columns → `all table columns must have the same length`.

Sinks: path (returns path, `===` input), IOBuffer (returns io), devnull, Pipe, `GzipCompressorStream` (needs `stop_on_end=true` or `TOKEN_END`), `.gz`/`.csv.gz` auto-gzip, `.gz` + `append=true` (second gzip member, readable), `compress=:gzip` to IOBuffer, `partition=true` with mixed IO/paths, `.gz` partitions, `header=` override per partition, DataFrame (1 partition), nonexistent dir → `SystemError` (fine). `append=true` suppresses header and BOM; `writeheader=true` overrides.

## 5. Tables.jl interface matrix

`✗` = MethodError/ArgumentError. `PT` = `CSVKernel.ParsedTable` (a chunk).

| probe | File | LazyFile | Rows | Chunks | PT | FileRow | Row |
|---|---|---|---|---|---|---|---|
| `istable` | true | true | true | false | true | false | false |
| `rowaccess` / `columnaccess` | true / true | false / true | true / false | false / false | false / true | – | – |
| `rows(x)` | itself | `RowIterator{LazyFile}` | itself | `IteratorWrapper` | `RowIterator{PT}` | – | – |
| `columns(x)` | **PT** | itself | `CopiedColumns{NamedTuple}` | `CopiedColumns` (chunks as rows!) | itself | ✗ | – |
| `schema` | ✓ | ✓ (`Union{Missing,CompactString}` / typed) | ✓ | `nothing` | ✓ | `nothing` | `nothing` |
| `columnnames` | `[:a,:b,:c]` | ✓ | **struct fields** | **struct fields** | ✓ | ✓ | ✓ |
| `getcolumn(Int)` / `(Symbol)` | ✓ / ✓ | ✓ / ✓ | field / ✗ | field / ✗ | ✓ / ✓ | ✓ / ✓ | ✓ / ✓ |
| `getcolumn(String)` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| `rowcount` | 3 | 3 | ✗ (`length(::KernelExamples.Rows)`) | 1 (=length) | 3 | – | – |
| `partitions` | `(f,)` | `(lf,)` | `(rs,)` | itself | `(t,)` | – | – |
| `materializer` | `columntable` (default) | default | default | default | default | – | – |
| `subset(1:2)` / `subset(1)` | `DictColumnTable` / `ColumnsRow{PT}` | same | ✗ | ✗ | same | – | – |
| `dictrowtable` / `rowtable` / `columntable` / `matrix` | ✓ (`Matrix{Any}`) | ✓ (`LazyColumn`s / `Matrix{Union{Missing,CompactString}}`) | ✓ | garbage (W6) | ✓ | ✗ | – |
| `DataAPI.nrow`/`ncol` | ✗ | ✗ | ✗ | ✗ | ✗ | – | – |
| `length` / `size` | 3 / ✗ | 3 / `(3,3)` | ✗ / ✗ | 1 / ✗ | ✗ / ✗ | 3 | 3 |
| `eltype` / `IteratorSize` | `FileRow` / HasLength | `Any` / – (no iterate) | `Row` / SizeUnknown | `PT` / HasLength | – | – | – |
| `getindex(1)` / `(:a)` / `("a")` / `(1,1)` | row / ✗ / ✗ / ✗ | column / column / ✗ / cell | ✗ | ✗ | ✗ / column / ✗ | cell / cell / ✗ | cell / cell / ✗ |
| `propertynames` / `getproperty(:a)` | names / column | names / column | fields / ✗ | fields / ✗ | fields / ✗ | names / cell | names / cell |
| `haskey(:a)` / `keys` / `values` | ✗ / ✗ / itself | ✗ / ✗ / itself | ✗ | ✗ | ✗ | ✓ / names / cells | ✓ / names / cells |
| `isempty` / `summary` | ✓ / `"CSV.CSVApi.File"` | ✗ / leak | ✓ | ✓ | ✗ | ✓ | ✓ |
| `show` lines / has `text/plain` method | 6 / no | 4 / no | 1 (struct dump) | 1 (struct dump) | 4 / no | 4 (Tables) | 4 (Tables) |
| `==`/`isequal`/`hash` vs equal data | identity | identity | identity | identity | identity | identity | identity |
| `NamedTuple(x)` | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✓ |
| `Base.names` | ✓ | ✓ | ✗ | ✗ | ✓ | ✗ | ✗ |

Column-name shadowing: a file whose columns are `table,name,lookup,names,nrows,columns,problems,prepared,inner,types,limit,stringtype,view,f,row` reads, shows, iterates, `problems`, `Tables.schema`, `columntable`, `write`, `RowWriter` all correctly for `File`, `LazyFile`, `Rows`; `fs.table`/`fs.name`/`lfs.prepared`/`fs[1].row` return the columns (internals reached via `getfield`). Only `ParsedTable` differs: `chunk.names`/`chunk.nrows` are the struct fields (use `chunk[:names]`).

## 6. Checked, fine

* Every 0.10 reader keyword is either accepted, mapped, or rejected with a `was removed in 1.0: …` message; unknown reader keywords → `unsupported <door> keyword x`.
* `limit`, `footerskip`, `skipto` (incl. `skipto=1 ⇒ header=false`), `header=0`, `header=[]`, multi-row header validation (`header rows must be increasing and ≥ 1`), `chunkbytes`/`samplebytes`/`nsample`/`scanner` validation messages, `groupmark`/`decimal`/`quotechar` ASCII checks, `comment=""`, `delim=""`, `truestrings="T"` all give clear `ArgumentError`s.
* `select`/`drop` names match raw or normalized, `String` or `Symbol`; `select` order is honoured by `lazy` (`[:c,:a]`) but File returns file order (`[:a,:c]`) — document.
* `types`/`pool`/`dateformat` Dicts accept Int, Symbol, String and Regex keys; `validate=false` skips absent keys on File/read/Vec/Rows/Chunks/lazy/transpose.
* `typemap` works on File (Int64⇒Float64) and sniff.
* `source=` on multi-file: Symbol/String/Pair; collision error; `"<source i>"` labels for non-path sources; 1-element vector works; `read([p1,p2], sink)` works; string columns come back `String` (documented).
* Sources: path, IOBuffer, `Vector{UInt8}`, `codeunits`, `SubArray`, `Cmd`, FilePathsBase path (File/lazy/Rows/write), `http(s)://`; literal string source gives the helpful "wrap literal data in IOBuffer" error; empty sources (`UInt8[]`, `IOBuffer("")`, header-only) give empty tables.
* `strict=true ⇔ on_error=:error`; `maxproblems=0` keeps counting (`2 problem(s) recorded — 0 retained by problems(f)`), and `on_error=:error` still fires with `maxproblems=0`.
* InlineStrings extension: `stringtype=InlineString` (auto width), fixed `String7` (errors on overflow with a clear message) on File/Rows/Chunks/lazy; pool levels take the InlineString width; multi-file and transpose fall back to `String`.
* `Rows`: iterates repeatedly, `limit`/`footerskip`/`header=false` honoured, short rows → `missing`, long rows ignore extras, typed access via `types`, `stringtype=String`; `Tables.rowtable`/`columntable`/`DataFrame(rs)` work; `reusebuffer` inert as documented.
* `Chunks`: `Tables.partitions(ch) === ch`, stable schema, `pool`/`stringtype` per batch, header problems merged into the first batch, `CSV.write(sinks, ch; partition=true)`.
* `lazy`: `lf[i,j]`, `lf.col`, `lf[:col]`, `size`, `names`, `types=` per-column parse-on-access (bad cell ⇒ `missing`), `stringtype=String`, `select`, `File(lf; types/select/pool)` reuse; `DataFrame(lf)`, `Tables.matrix(lf)`, `Tables.rows(lf)`, `Tables.scan(lf, …)`.
* `CSV.read`: `DataFrame`, `Tables.columntable/rowtable/matrix/dictrowtable`, `NamedTuple`, closures; `CopiedColumns` honoured by DataFrames (no copy; `stringtype=String` gives `Vector{String}`, `pool=true` gives `PooledVector{String}`).
* `CompactString` semantics: `==`/`hash`/`isequal`/`cmp` agree with `String`, Dict lookup by `String`, `Symbol`, `parse`, `Date`, `startswith`/`occursin`/`split`/`replace`/`uppercase`/`*`, `print`/`show` as a string.
* Writer round trip (13 columns, ints incl. `typemax`, `1e-7`/`1e21`/`-0.0`/`NaN`/`±Inf`, dates 0001–9999, ms `DateTime`, ms `Time`, bools, quoted/newline/comma/leading-space/unicode strings, `""` vs `missing`, `Int128`) re-reads with empty `problems`, same values, and byte-identical output on re-write (BigFloat excepted, W9). `""`/`missing` round-trips under `:minimal`, `:all`, and `missingstring="NA"`.
* `write` byte parity with 0.10 on: floats, `decimal=','`, `quotestyle=:all` ≡ `quotestrings=true`, 0-row/0-col tables, quoted header names, generators/row tables, `writeheader=false`, `header=[…]`, `missingstring="NA"`, leading/trailing-space strings; `join(RowWriter(t)) == write(io, t)`.
* `write` return values: path (same object), IO, gz stream, sink vector under `partition=true`.

## 7. `docs/src/reading.md` / `index.md` — top 10 statements now false

1. index.md: "individual columns can be accessed like `file.col1`, `file[:col1]`, or `file["col"]` … `file.names`" — `f[:col]`, `f["col"]`, `f.names` all error (A1).
2. index.md: "`CSV.Rows` … each iteration produces a `CSV.Row2` … every column type will be essentially `Union{Missing, String}` … `reusebuffer=true`" — type is `CSV.CSVApi.Row`, cells are `CompactString`, `reusebuffer` is inert.
3. index.md / reading.md `ntasks`: "`CSV.Chunks` … each iteration will return a `CSV.File`" and the 0.10 Chunks docstring "each iteration does independent type inference" — returns `ParsedTable`, schema is stable.
4. index.md: "`CSV.write` … writes to an internal buffer … buffer size is configurable … partitioned inputs as separate output files, one file per input partition" — `bufsize` gone; `partition=true` requires a Vector of sinks (no `_i` suffix); `table |> CSV.write(file)` gone.
5. reading.md `input`: mmap of every path, gzip decompressed "to a temporary file first, then mmapped", `IO`/`Cmd` "consumed into a temporary file", `buffer_in_memory` semantics — now: files < 512 KiB are read, larger mmapped, gzip/IO/Cmd fully in memory (audit B2 pending); URLs are accepted directly.
6. reading.md `select`/`drop`: "can also be given as boolean functions `(i, name) -> Bool` … Invalid values in `select` are ignored" — functions retired; invalid names are an `ArgumentError`.
7. reading.md `limit`: "may or may not return exactly `limit` … pass `ntasks=1`" — exact under any thread count now.
8. reading.md `missingstring`: "default `missingstring=""` … `missingstring=nothing` ⇒ no missing values will be checked" — empties are always missing; `missingstring` only *adds* sentinels; `nothing` is a no-op (B1 pending).
9. reading.md `stringtype`: "Supported values are `InlineString` (the default), `PosLenString`, or `String`" and `pool` default `(0.2, 500)` with a function form — default is `CompactString`, `PosLenString` is gone, `InlineString` needs the extension, `pool=false`, no function form.
10. reading.md `types`/`strict`/`silencewarnings`/`maxwarnings`/`debug`/`rows_to_check`/`typemap` sections: function-typed `types`, "a warning will be emitted", `silencewarnings`, `debug`, `rows_to_check`, and "API Reference … `CSV.detect`" — warnings are `problems(f)`, `strict`→`on_error`, `maxwarnings`→`maxproblems`, `debug`/`rows_to_check`/`CSV.detect` removed.
