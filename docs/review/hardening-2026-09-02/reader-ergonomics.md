# CSV.jl 1.0 kernel — reader ergonomics review

Scope: `CSV.File` / `CSV.read` / `CSV.Rows` / `CSV.Chunks` / `CSV.lazy` as an ordinary user sees them — keyword UX, error messages, display, `problems`, edge-case inputs, and surprises for people coming from 0.10 / pandas / polars / readr.
Method: ~700 scripted probes (Julia 1.12.6, 8 threads) against the `kernel-proveout` worktree, every one run side by side with the frozen 0.10 oracle (`LegacyCSV`) where a comparison made sense, plus DataFrames/Arrow interop in a stacked environment. Probe scripts and raw outputs are under `$SP/re/` (`b1`…`b11`, `*.out`). Every claim below was reproduced; printed output is quoted verbatim.

Settled decisions (CompactString default, `pool=false`, empty ⇒ missing, no schema widening, problems-as-data, strict Bool, Int128, retired function selectors, deferred module flatten) are not re-litigated; their concrete UX consequences are reported where they bite.

---

## 1. Ranked findings

| id | sev | one line |
|---|---|---|
| R1 | **blocker** | `CSV.read(path, DataFrame)` string columns are read-only, non-resizable `CompactStringVector`s: `sort!(df, :s)`, `filter!`, `deleteat!`, `append!`, `push!`, `replace!`, `df.s[i] = "x"` all throw `CanonicalIndexError`/`MethodError`; there is no `CompactString(::String)`/`convert`, so even `Vector{CompactString}` refuses String assignment. 0.10 handed back plain mutable Vectors. |
| R2 | **blocker** | mmap aliasing: for files ≥ 512 KiB, long (>12 B) string cells are views into the mapping. Overwriting the file changes values in an already-parsed table; truncating it and touching a cell kills Julia with `Bus error: 10`. `df = CSV.read(p, DataFrame); …; CSV.write(p, other); df.s[end]` is the crash path. Inline (≤12 B) values are unaffected, so it is intermittent. |
| R3 | major | Silent data loss with zero signal: `CSV.read` prints nothing for ragged rows, invalid values under `types=`, stray quotes or an unclosed quote, and `CSV.problems(::DataFrame)` is a MethodError. A single stray `"` at row 1001 of a 2001-row file yields a 1001-row DataFrame with no output (0.10: 2001 rows; unclosed quote was a fatal error in 0.10, an error in polars, a printed summary in readr). No `on_error=:warn`. |
| R4 | major | `CSV.Chunks` yields `CSVKernel.ParsedTable`, not `File`: `chunk.col` is a `FieldError`, `chunk["col"]`/`chunk[1]`/`length`/`size`/`iterate` are MethodErrors, `show` prints `CSVKernel.ParsedTable: 3 × 3`, fields `names/columns/problems/droppedproblems` leak. Docstring promises "`File`-shaped tables". 0.10 yielded `File`. |
| R5 | major | `types=String` (or `Dict(:s=>String)`) silently returns `CompactString` columns, while `types=CompactString`, `types=InlineString`, `types=String15` are refused as "unsupported column type". 0.10: `types=String` ⇒ `Vector{String}`, `String15` accepted. |
| R6 | major | Fractional seconds beyond milliseconds are silently truncated on inference and under `types=DateTime`: `2024-01-05T10:00:00.123456` → `DateTime("…10:00:00.123")`, no problem recorded (0.10 kept the String / reported INEXACT). |
| R7 | major | `Rows` typed access swallows parse failures as `missing` with no diagnostic channel (`CSV.problems(::Rows)` is a MethodError, `strict`/`on_error`/`maxproblems` refused on Rows). Schema advertises `Union{Missing,Int64}` so the missing looks legitimate. |
| R8 | major | `Problem` UX: printed as `CSV.CSVKernel.Problem(2, 0, 13, :short_row, "…")` (positional, module-prefixed), no docstring for `Problem` or `CSV.problems`, no summary `show` for the vector, `kind`s undocumented, `pos` is a byte offset the user cannot map to a line (buffer not retained on `File`), narrowing problems carry `pos=0`, identical structural problems flood the log (20 000 short rows ⇒ 10 000 retained + 10 000 dropped, no aggregation), error text says `column 0` for row-level problems. |
| R9 | major | Keyword × door inconsistencies (matrix in §4): `select`/`drop` refused on Rows and Chunks (0.10 had them); `typemap`, `downcast`, `strict`/`on_error`, `maxwarnings` refused on Rows/Chunks; Chunks takes `maxproblems` but not `nsample`; `File(lf)` takes `parallel`/`maxproblems` but refuses `ntasks`/`maxwarnings` (as MethodErrors); `transpose=true` silently ignores `pool`, `strict`, `on_error`, `ntasks`, `maxproblems`, `stringtype` (and never records a problem) while refusing `footerskip`. |
| R10 | major | REPL display of `CSV.Rows(...)`/`CSV.Chunks(...)` is the default struct dump: the whole byte buffer plus every `ChunkIndex` array (`sprint(show, rows)` = 4.08 MB for a 540 KB file; 8 KB even under `:limit`). `Tables.columnnames(rows)` returns the struct fields `(:inner, :types, :limit, :stringtype)`. |
| R11 | minor | Missing 0.10 accessors on `File`: `f[:a]`, `f["a"]`, `size(f)`, `f[end]`/`last(f)`, `f[1:2]`, `f[1, :a]`, `haskey`, `keys`; `names(::Rows)` missing; `CSV.rownumber` not reachable (only `CSV.CSVApi.rownumber`). |
| R12 | minor | Error-message quality for common mistakes: `types=(i,n)->Int` → `unsupported types specification: var"#53#54"`; `select=:a` → `MethodError: length(::Symbol)`; `select="a"` → a `map(f, s::AbstractString)` error; `select=(:a,)` → "got Tuple{Int64}" (misreports the user's type); `CSV.read(src)` without sink → MethodError pointing at `Base.read`; `CSV.read(DataFrame, src)` / `CSV.File(:sym)` / `CSV.Rows([p1,p2])` → `no method matching resolvesource(...)` (internal name leak); `quotechar=nothing` → `No value arguments present`; typo kwargs get no did-you-mean. |
| R13 | minor | Scalar-kwarg type acceptance is inconsistent: `delim=","` (String) OK but `quotechar="'"`, `escapechar="\\"`, `decimal=","` are TypeErrors; `comment='#'` (Char) is a TypeError (String required); `quotechar=0x22` refused (0.10 accepted UInt8); `dateformat=dateformat"yyyy-mm-dd"` refused (0.10 accepted `DateFormat`); `header=nothing` TypeError while every other kwarg accepts `nothing`. |
| R14 | minor | Silently accepted conflicting/odd dialects: `decimal=','` with `delim=','` (garbage + one problem instead of an error); `openquotechar='['` without `closequotechar` (3 problems, `:b Missing`); `typemap=Dict(String=>Symbol)` ignored; `truestrings=["T"]` without `falsestrings` silently disables Bool inference. |
| R15 | minor | `header=["a"]` with a 2-column file silently drops the second column (one `long_row` problem per row); 0.10 widened to `[:a, :Column2]`. Same family: `header=5` past EOF and an `IOBuffer` already at EOF both return a `0 × 0` table with no diagnostic (0.10 same; #1000 still unaddressed); `.zip`/`.xlsx` paths parse as a `0 × 1` table named `Symbol("PK\x03\x04junk")`. |
| R16 | minor | Temporal leniency regressions vs 0.10: `2024-1-5`, `2024-01-5` → String (0.10 `Date`; `Dates.Date("2024-1-5")` parses); `10:00`, `1:00` → String (0.10 `Time`; HH:MM is valid ISO 8601); `types=Time` on `10:00` and `types=DateTime` on `2024-01-05` → `missing` + problem (0.10 parsed). `2024-01-05 10:00:00` (space) → String in both. |
| R17 | minor | `types=Dict(:d=>Date)` on non-ISO text: `cannot parse Date from "01/05/2024"` — no hint that `dateformat=` exists; `types=Bool` on `TRUE`/`1`: `cannot parse Bool from "TRUE"` — no hint at `truestrings`. Decision consequences worth a message: pandas/polars/R-style `TRUE`/`True`/`T` columns now come back as strings with no signal. |
| R18 | minor | `CSV.lazy`: lowercase verb beside `File`/`Rows`/`Chunks` constructors; `lf[1]` returns a column where `f[1]` returns a row; no `lf["a"]`, no `iterate`; `lf.prepared` leaks a `CSVApi.Prepared`; `File(lf; header=false)` is a MethodError rather than the "fixed when lf was created" ArgumentError. |
| R19 | minor | Name/namespace leaks (full list §5): `show(File)` → `CSVApi.File("<GenericIOBuffer>")`, `show(LazyFile)` → `CSV.LazyFile(...)`, `show(ParsedTable)` → `CSVKernel.ParsedTable`, sources print as `<GenericIOBuffer>`/`<Array>`/`<CodeUnits>`/`<PosixPath>`/`<IOStream>`/`<Cmd>`; schema/`typeof` show `CSV.CSVKernel.CompactString`, `CSV.CSVApi.FileRow`, `CSV.CSVApi.Row`, `CSV.CSVApi.LazyColumn{…}`; DataFrame header truncates to `CompactS…`; two `Rows` docstrings disagree; `File` docstring omits five accepted engine knobs; module docstring omits `lazy`. |
| R20 | minor | Memory retention: any `CompactString` column pins the entire input buffer, even when every value is inline or `select` picked one narrow column (1 167 B retained for a 1 000 B file with `select=[:a]`); `Base.summarysize(df.s)` for a 10 001-row 2-column file is 200 168 B (the whole file). |
| R21 | cosmetic | `stripwhitespace=true` now strips inside quoted cells (`" x "` → `"x"`; 0.10 kept ` x `); `footerskip=1` with a trailing blank line keeps the `total` row (0.10 same); single-column single-data-row files with `:` values elect `:` as delimiter (0.10 same, but 1.0 truncates where 0.10 widened); `f.name`/`f.table`/`f.lookup` reachable by getproperty when no such column exists. |

---

## 2. Details and repros

### R1 — `CSV.read(path, DataFrame)` returns immutable string columns (blocker)

`read` hands `Tables.CopiedColumns(File(...))` to the sink, so DataFrame takes the `CompactStringVector` by reference (correct for the design), but that vector implements neither `setindex!` nor resizing, and `CompactString` has no constructor from `AbstractString`:

```julia
df = CSV.read(IOBuffer("a,b\ny,1\nx,2\n"), DataFrame)
typeof(df.a)      # CSV.CSVKernel.CompactStringVector{CSV.CSVKernel.CompactString}
sort!(df, :a)     # CanonicalIndexError: setindex! not defined for CSV.CSVKernel.CompactStringVector{CSV.CSVKernel.CompactString}
df.a[1] = "z"     # CanonicalIndexError (same)
replace!(df.a, "x"=>"z")   # CanonicalIndexError
deleteat!(df, 1)  # MethodError: no method matching deleteat!(::CompactStringVector, ::Vector{Int64})
filter!(:b => >(1), df)    # MethodError (deleteat!)
append!(df, df)   # MethodError: no method matching resize!(::CompactStringVector, ::Int64)
push!(df, ("q", 3))        # MethodError: no method matching CSV.CSVKernel.CompactString(::String)
CSV.CompactString("x")     # MethodError
convert(CSV.CompactString, "x")   # MethodError
df2 = DataFrame(CSV.File(...))    # copycols ⇒ Vector{CompactString}
df2.a[1] = "z"    # MethodError: CompactString(::String)  — copying does not help
```

Works: `df[!, :a] .= "z"` (replaces the column with `Vector{String}`), `allowmissing!`, `vcat`, `transform!`, `unique`, `groupby`, `innerjoin`, `sort` (non-bang), `describe`, `Arrow.write`, `CSV.write`, `==`/`hash`/`Set`/`Dict` interop with `String`. Union columns (`Union{Missing,CompactString}`) fail the same way for `df.a[2] = missing`. `pool=true` and `stringtype=String` columns mutate fine.

Compared with 0.10, where `CSV.read(p, DataFrame)` gave `Vector{String}`/`PooledVector`, this breaks the most common post-read idioms (`sort!`, `filter!`, in-place cleaning). Recommendation (both parts are needed): (a) `CompactString(::AbstractString)` + `Base.convert` — ≤12 bytes inline, otherwise own the bytes in the existing `data::Vector{UInt8}` slot; (b) either make `CompactStringVector` a proper mutable vector (`setindex!`, `resize!`, `push!`, `deleteat!`, `insert!`, `similar`, `copyto!`), or have the `read` door materialise to `Vector{CompactString}` (16 B/element copy) once (a) exists. Also worth a line in the `stringtype` docs.

### R2 — mmap views alias the file; truncation is a process crash (blocker)

`$SP/re/b11_mmap.jl`, 1.9 MB file, 60 000 rows of 25-byte strings (above `MMAP_THRESHOLD = 524288`):

```
before: xxxxxxxxxxxx-000000060000
after overwrite same size: yyyyyyyyyyyy-000000060000   ← values of an already-parsed File changed
after truncation, reading f.b[1]: short                 ← reads the NEW file's bytes
after truncation, reading f.b[end]:
[31331] signal 10 (1): Bus error: 10
_platform_memmove at /usr/lib/system/libsystem_platform.dylib
```

Values ≤12 bytes are inline copies and are unaffected, which is why the earlier `x100000` probe in `b8` looked safe — the hazard depends on string length, so users will hit it inconsistently. The read-clean-write-back-to-the-same-path workflow (`df = CSV.read(p, DataFrame); …; CSV.write(p, df2)`; keep using `df`) is common. 0.10 mapped too but always copied strings out. Recommendation: for mapped sources, copy the bytes referenced by view payloads into a heap buffer at finalize (one memcpy of the string bytes; short values are already inline), or default `buffer_in_memory=true` whenever any `CompactString` column will be retained; at minimum document it loudly under `buffer_in_memory` and make `CSV.write` refuse a path that a live `File` still maps.

### R3 — silent data loss on `read`; no problems channel (major, design finding)

What the user sees for each malformed input (`b1.out`), `CSV.read(IOBuffer(s), DataFrame)`, no other output in every case:

| input | 1.0 `read → DataFrame` | 0.10 | polars / pandas / readr |
|---|---|---|---|
| short row `4,5` in a 3-col file | 3×3, `c` = `missing` at row 2, **no output** | same data + `@warn "only found 2 / 3 columns around data row: 2"` | polars: error; pandas: fills NaN silently (`on_bad_lines` applies to long rows); readr: one-line `Warning: 1 parsing issue` + `problems()` |
| long row `4,5,6,99` | 3×3, `99` dropped, no output | widened to 4 cols + warn | polars: error; pandas: error by default; readr: warning summary |
| `types=Dict(:b=>Int)` on `x`,`y` | `b` all `missing`, no output | missing + two `@warn` | readr warns; pandas raises |
| stray `"` mid-field (`1,ab"c`) | **1 row instead of 2**, `b = "ab\"c\n2,d\n"`, no output | 2 rows (`ab"c`, `d`), no warning | polars errors on the unclosed quote |
| unclosed `"` at EOF | 1 row containing the rest of the file, no output | **fatal error** | polars: error |

Scale version (`b10`): a 2 001-row file with `2,5"11` at row 1001 → `nrow(CSV.read(...)) == 1001`, no output; 0.10 returned 2 001 rows. `File` at least prints `2 problem(s) recorded — 2 retained by problems(f)`, but `read` users never see a `File`, and:

```julia
CSV.problems(CSV.read(src, DataFrame))          # MethodError: no method matching problems(::DataFrame)
CSV.problems(CSV.read(src, Tables.columntable)) # MethodError
CSV.File(src; on_error=:warn)   # ArgumentError: on_error must be :collect or :error
CSV.File(src; on_error=:skip)   # ArgumentError (no drop-bad-rows mode)
```

Evidence-based recommendation: keep problems as data, but (1) have `read` (and arguably `File` when displayed non-interactively) emit ONE summary `@warn` when the log is non-empty — `"CSV: 2 parse problems (first: short_row at data row 2 — expected 3 fields, found 2). Retrieve them with CSV.problems(CSV.File(src)); pass on_error=:collect… / silence with …"` — readr's model; (2) treat `:unclosed_quote` and a quote that engaged structural protection as `:error` by default (0.10 and polars precedent) — these are the two kinds that discard rows rather than cells; (3) add `on_error=:warn` and consider `:skip` (drop offending rows) as pandas' `on_bad_lines="skip"`; (4) give `read` a way to return problems (`problems=Ref`/callback kwarg, or `CSV.read(src, sink; problems=true)` returning `(table, problems)`).

`strict=true`/`on_error=:error` messages are good: `CSVKernel: short_row at data row 2, column 0: expected 3 fields, found 2 (remaining columns set to missing)` and `CSVKernel: invalid_value at data row 1, column 2: cannot parse Int64 from "x" (+1 more)` — except the `CSVKernel:` prefix and `column 0`.

### R4 — Chunks elements are `ParsedTable` (major)

```julia
c = first(CSV.Chunks(IOBuffer(S); ntasks=2))
typeof(c)   # CSV.CSVKernel.ParsedTable
c.a         # FieldError: type CSV.CSVKernel.ParsedTable has no field `a`, available fields: `names`, `columns`, `nrows`, `problems`, `droppedproblems`
c["a"]; c[1]; length(c); size(c); collect(c)   # MethodError each
show(c)     # CSVKernel.ParsedTable: 3 × 3\n  a::Int64 …
```

`c[:a]`, `names(c)`, `CSV.problems(c)`, `Tables.*`, `DataFrame(c)` work. Wrapping each batch in `File(name, t, lookup)` (what `File` itself does) fixes all of it and makes the docstring true. Also: `Chunks` `ntasks` means number of batches (rename pending), `pool` accepts scalars only (Dict/vector refused with a clear message), and header-row problems are merged into the first batch only.

### R5 — `types=String` vs `types=CompactString` (major)

```julia
Tables.schema(CSV.File(IOBuffer("a\n1\n"); types=String))          # :a  CSV.CSVKernel.CompactString
CSV.File(IOBuffer("a\n1\n"); types=CSV.CompactString)                # ArgumentError: unsupported column type CSV.CSVKernel.CompactString
CSV.File(IOBuffer("a\n1\n"); types=Dict(:a=>String15))               # ArgumentError: unsupported column type String15   (0.10: String15 column)
CSV.File(IOBuffer("a\n1\n"); types=InlineString)                     # ArgumentError (0.10 also refused: non-concrete)
typeof(L.File(IOBuffer("a\n1\n"); types=Dict(:a=>String)).a)         # Vector{String}
```

Recommendation: `types=String` should mean "this column comes out as `String`" (per-column materialise, same code path as `stringtype=String`), and the accepted string types in `types` should equal the accepted `stringtype`s (`CompactString`, `String`, extension types). Same applies to `Rows`/`lazy` (`Rows(...; types=String)` already yields `String` cells — inconsistent with `File`).

### R6 — sub-millisecond timestamps silently truncated (major)

```julia
CSV.File(IOBuffer("a\n2024-01-05T10:00:00.123456\n"); delim=',').a       # [DateTime("2024-01-05T10:00:00.123")], problems = []
CSV.File(IOBuffer("a\n2024-01-05T10:00:00.123456789\n"); delim=',').a    # [DateTime("2024-01-05T10:00:00.123")]
CSV.File(IOBuffer("a\n2024-01-05T10:00:00.123456\n"); types=DateTime).a  # same, no problem
L.File(...).a                          # String31 (inference) / missing + "INEXACT" warning (types=DateTime)
DateTime("2024-01-05T10:00:00.123456") # ArgumentError: Invalid DateTime string
```

Microsecond timestamps are the norm in exported logs/DB dumps. Either keep them as strings (0.10, Base Dates) or record an `:inexact_value` problem; silent rounding is the one option nobody expects. (Rounding vs truncation is also unspecified.)

### R7 — `Rows` typed access failures are invisible (major)

```julia
rt = CSV.Rows(IOBuffer("a\n1\nx\n"); types=Int)
[row.a for row in rt]        # Union{Missing, Int64}[1, missing]
CSV.problems(rt)             # MethodError
CSV.Rows(src; strict=true)   # ArgumentError: unsupported Rows keyword strict   (0.10 accepted strict on Rows)
```

Same for `lazy(...; types=Int)` (`[missing, missing, missing]` for a text column, `CSV.problems(lf)` MethodError). 0.10's Rows also returned missing, but 1.0's whole pitch is "errors are data" and the streaming door has no data. Options: a per-row `CSV.problems(row)`/`row` flag, an `on_error=:error` on Rows, or at least a documented statement.

### R8 — `Problem` display and discoverability (major)

```
julia> CSV.problems(f)
5-element Vector{CSV.CSVKernel.Problem}:
 CSV.CSVKernel.Problem(2, 0, 13, :short_row, "expected 3 fields, found 2 (remaining columns set to missing)")
 CSV.CSVKernel.Problem(3, 0, 23, :long_row, "expected 3 fields, found 4 (extra fields ignored)")
 CSV.CSVKernel.Problem(4, 1, 26, :invalid_quoted_field, "malformed quoting in \"\\\"x,1,2\\n\"")
 CSV.CSVKernel.Problem(0, 0, 32, :unclosed_quote, "input ended inside a quoted field")
```

- Positional fields: a reader has to know `(row, col, pos, kind, message)`; `row=0` means file-level, `col=0` row-level — undocumented. `Base.Docs.doc(CSV.CSVKernel.Problem)` → "No documentation found"; `doc(CSV.problems)` → "No documentation found for private binding `CSV.CSVApi.problems`".
- `pos` is an absolute byte offset; `File` does not retain the buffer, so there is no `CSV.line(f, p)`/excerpt-of-row helper; users cannot get the offending raw line without re-reading the file themselves.
- `_narrowtypes` problems have `pos=0` (`Problem(2, 1, 0, :invalid_value, "value 300 does not fit Int8")`).
- Kinds seen: `:short_row :long_row :invalid_value :invalid_quoted_field :unclosed_quote` — only in a source comment.
- Flooding: `"a,b\n" * "1\n"^20000` → `20000 problem(s) recorded — 10000 retained`, all identical `short_row`; a `(kind, ncols, nfields)` aggregate with a count and first/last row would be far more useful than 10 000 rows of the same text.
- Good: header problems merge and sort before data problems (`Problem(0, 2, 3, :invalid_quoted_field, "malformed quoting in header \"\\\"b\\\"x\"")`), retention is source-ordered and deterministic across chunk geometry, excerpts are truncated at 32 bytes with `…`, `maxproblems=0` still reports the count in `show(File)`.

Recommendation: a `show` for `Problem` (`short_row @ data row 2 (byte 13): expected 3 fields, found 2 …`), a compact summary `show` for the vector (counts by kind + first N), docstrings for `Problem`, `problems`, and the kind vocabulary, keep `pos` but add a `line`/`excerpt` helper that reads the source on demand, and aggregate repeated structural problems.

### R9 — keyword × door inconsistencies (major) — see §4

Concrete silent-ignore cases on `transpose=true` (`b3`, `b9`):

```julia
CSV.File(IOBuffer("a,1,x\nb,3,4\n"); transpose=true, types=Int, strict=true)     # no error; a = [1, missing]; problems = []
CSV.File(IOBuffer("a,1,x\nb,3,4\n"); transpose=true, types=Int, on_error=:error) # no error
CSV.File(src; transpose=true, pool=true / ntasks=2 / maxproblems=1 / stringtype=InlineString)  # accepted, no effect
CSV.File(src; transpose=true, footerskip=1)   # ArgumentError: unsupported File(transpose=true) keyword footerskip
CSV.File(src; transpose=true, delim=nothing)  # TypeError: in keyword argument delim, expected Union{Char, String}, got Nothing
```

The `File` method consumes `pool/strict/on_error/ntasks/maxproblems/stringtype` before dispatching to `_transposedfile`, which never records problems. Refuse them explicitly or implement them.

### R10 — `show(Rows)`, `show(Chunks)`, `Tables.columnnames(rows)`

```julia
length(sprint(show, CSV.Rows(IOBuffer(big))))                      # 4078040  (540 KB input)
length(sprint(show, r; context=:limit=>true))                      # 8294
first(sprint(show, r; context=:limit=>true), 120)
# "CSV.CSVApi.Rows(CSV.KernelExamples.Rows(UInt8[0x61, 0x2c, 0x62, 0x0a, 0x31, 0x2c, 0x78, 0x78, 0x78, 0x78  …  0x78, …], CSV.CSVKernel.ChunkIndex[CSV.CSVKernel.ChunkIndex(1, 65560, UInt32[0x00000004, …"
Tables.columnnames(r)          # (:inner, :types, :limit, :stringtype)
Tables.columnnames(chunks)     # (:inner, :headerlog, :maxproblems, :stringtype, :poolspec)
```

0.10 printed `CSV.Rows("<IOBuffer: …>"):\nSize: 3\nTables.Schema: …`. Any `@info rows`, string interpolation, or `print` of a Rows/Chunks over a large file dumps megabytes.

### R11 — access battery gaps (minor)

`File`: `f[:a]`, `f["a"]`, `f[1, :a]`, `f[1, 1]`, `f[1:2]`, `f[[1,3]]`, `f[end]`, `last(f)`, `size(f)`, `keys(f)`, `haskey(f, :a)`, `copy(f)`, `filter(pred, f)` — all MethodErrors (0.10 had `f[:a]`, `f["a"]`, `size(f)`; `nrow`/`ncol` are DataFrames-only in both). `f[1]["a"]` fails in both. Works: `f.a`, `f[1]`, `f[1].a/[1]/[:a]`, `length`, `names`, `propertynames`, `first(f, n)`, `collect`, `map`, `NamedTuple(row)`, `pairs(row)`, all `Tables.*`.
`Rows`: `names(rows)` MethodError, `row["a"]` MethodError, `row[5]` → `BoundsError: attempt to access 3-element Vector{Symbol}` (leaks the names vector). Works: restartable iteration, `parse(Int, row.a)`, `String(row.s)`, `NamedTuple(row)`, `DataFrame(rows)`, `stringtype=String/InlineString/String15`, `limit`, `skipto`, `footerskip`, `missingstring`, `.gz` paths.
`lazy`: `lf["a"]` MethodError, `lf[1:2]` MethodError, `iterate(lf)` MethodError (`Tables.rows(lf)` works), `push!(lf.a, "x")` → `CompactString(::String)` MethodError.
`CSV.rownumber` → `UndefVarError` (only `CSV.CSVApi.rownumber`); 0.10 did not export it either, but the API layer defines it so it should be reachable.

### R12 / R13 / R14 — error messages, verbatim

```
CSV.File("a,b\n1,2")            ArgumentError: no file at "a,b\n1,2" — a String source is a file path (or an http(s):// URL); wrap literal data in IOBuffer   ✔ good
CSV.File("/tmp")                ArgumentError: no file at "/tmp" — …                                    (could say "is a directory")
CSV.File(src; delimiter=',')    ArgumentError: unsupported File/Rows/Chunks keyword delimiter           (no "did you mean delim"; context string wrong from `read`)
CSV.File(src; type=Int)         ArgumentError: type was removed in 1.0: use types (a single Type applies to every column)  ✔
CSV.File(src; types=(i,n)->Int) ArgumentError: unsupported types specification: var"#53#54"             ✘ (select/drop has the proper "retired" message)
CSV.File(src; select=:a)        MethodError: no method matching length(::Symbol)
CSV.File(src; select="a")       ArgumentError: map(f, s::AbstractString) requires f to return AbstractChar; …
CSV.File(src; select=(:a,))     ArgumentError: select must be a vector of Bool/Int/Symbol/String (got Tuple{Int64})   (user passed Tuple{Symbol})
CSV.read(src)                   MethodError: no method matching read(::IOBuffer)  You may have intended to import Base.read   (0.10: "provide a valid sink argument, like `using DataFrames; CSV.read(source, DataFrame)`")
CSV.read(DataFrame, src)        MethodError: no method matching resolvesource(::typeof(columntable); buffer_in_memory::Bool, prefetch::Bool)
CSV.File(:a) / CSV.File(nothing) / CSV.File((a=[1],))   MethodError: no method matching resolvesource(::Symbol; …)
CSV.Rows([p, p]) / CSV.Chunks([p,p]) / CSV.lazy([p,p])   MethodError: no method matching resolvesource(::Vector{String}; …)   (File/read accept vectors)
CSV.File(src; quotechar=nothing)   ArgumentError: No value arguments present        (`something()` leak)
CSV.File(src; header=nothing)      TypeError: in keyword argument header, expected Union{Integer, AbstractVector}, got a value of type Nothing
CSV.File(src; quotechar="'")       TypeError: … expected Char, got a value of type String      (delim="," is accepted)
CSV.File(src; comment='#')         TypeError: … expected Union{Nothing, String}, got a value of type Char
CSV.File(src; quotechar=0x22)      TypeError (0.10 accepted UInt8)
CSV.File(src; dateformat=dateformat"yyyy-mm-dd")   ArgumentError: dateformat must be a format String (got DateFormat{…})   (0.10 accepted)
CSV.File(src; decimal=',', delim=',')   accepted: Size 1 x 2, 1 problem   (0.10 also accepted; should be a conflict error)
CSV.File(src; openquotechar='[')        accepted: :a CompactString, :b Missing, 3 problems
CSV.File(src; source=:s)                ArgumentError: unsupported File/Rows/Chunks keyword source   (only meaningful for a vector; say so)
CSV.File(src; header=(1,2)) / header=("a","b")   TypeError (Tuple not AbstractVector; 0.10 same)
CSV.File(src; stringtype=Symbol)  ArgumentError: stringtype must be CSVKernel.CompactString, String, or a type provided by an extension (e.g. InlineString with InlineStrings loaded); got Symbol   (should read CSV.CompactString)
```

Good ones worth keeping: every `_LEGACYKW` entry produces `X was removed in 1.0: …` on File/Rows/Chunks/lazy (13/13 verified); `pool=2.0`, `pool="true"`, `pool=(500, 0.5)`, `pool=[true]`, `types=[Int,Int]` (count), `types=Dict(3=>Int)` (range), `types=Dict(r"z"=>Int)`, `select=[5]`, `select=[true]`, `select+drop`, `skipto=1, header=2`, `limit=-1`, `footerskip=-1`, `ntasks=0`, `chunkbytes=0`, `nsample=0`, `samplebytes=0`, `maxproblems=-1`, `scanner=:foo`, `comment=""`, `comment="\n"`, `delim='\n'`, `delim='"'`, `escapechar='§'`, `decimal='€'`, `missingstring="\""`, `truestrings="T"`, `missingstring=1`, `String1` overflow, empty sources vector, `source` collision — all clear ArgumentErrors.

### R15 — silent-empty / silent-narrow results

```julia
Tables.columntable(CSV.File(IOBuffer("1,2\n3,4\n"); header=["a"]))   # (a = [1, 3],)   + 2 long_row problems     (0.10: (a=[1,3], Column2=[2,4]))
CSV.File(IOBuffer("a,b\n1,2\n"); header=5)                            # Size: 0 x 0, no problem (0.10 same)
io = IOBuffer(S); read(io); CSV.File(io)                              # Size: 0 x 0 (0.10 same; issue #1000)
CSV.File("x.zip") / CSV.File("x.xlsx")                                # Size: 0 x 1, Symbol("PK\x03\x04junk")  (0.10 same)
CSV.File(IOBuffer(""); select=[:a])                                   # ArgumentError: select/drop name a does not match any column (0.10: empty table)
```

Cheap wins: an `ArgumentError`/problem when an explicit `header` vector is shorter than the data width (mirrors "long rows do not widen"), a problem or error for "IO already at EOF / empty source", PK-magic detection ("looks like a zip archive").

### R16 / R17 — type inference deltas vs 0.10 (explicit `delim=','`, full table in `b5.out`, `b5b.out`)

| input | 1.0 | 0.10 | note |
|---|---|---|---|
| `2024-1-5`, `2024-01-5` | String | Date | Base `Date("2024-1-5")` parses |
| `10:00`, `1:00` | String | Time | HH:MM is ISO 8601 |
| `types=Time` on `10:00` | missing + `cannot parse Time from "10:00"` | Time(10) | |
| `types=DateTime` on `2024-01-05` | missing + problem | DateTime midnight | |
| `2024-01-05T10:00:00.123456` | DateTime(…123) silently | String | R6 |
| `TRUE/FALSE`, `True/False`, `T/F` | String | Bool | decision; no signal to the user |
| `types=Bool` on `TRUE` / `1` | missing + `cannot parse Bool from "TRUE"` | Bool | message should mention `truestrings` |
| `""` (quoted empty) | `""` | missing | pinned |
| `9223372036854775808` | Int128 | Float64 | decision; DataFrames and Arrow handle Int128 fine (verified) |
| `types=UInt8` on `-1` | missing + `value -1 does not fit UInt8` | `0xff` (0.10 bug) | 1.0 better |
| `Date` then `1` | String | `Date("0001-01-01")` (0.10 bug) | 1.0 better |
| `true` then `1` | String | `Bool[1,1]` (0.10 bug) | 1.0 better |
| `types=Char` | `unsupported column type Char` | Char | parity gap |
| `Rational{Int}` | `Union{Missing,Rational}` all missing + problem | error | custom types accepted but unparseable — should refuse up front like `Char` |
| `dateformat="yyyy-mm-dd"` explicit | DateTime columns become String | same | shared wart |

Unchanged and correct in both: leading zeros → Int, `1e5`/`1E5`, `NaN/Inf/-inf/nan/Infinity/+Inf`, `1.`/`.5`/`+1`/`-0`/`-.5`/`5.`/`1e+5`, hex/`1_000`/`12%`/`$1,000` → String, `1` and `1.0` → Float64, Int→Float→Int → Float64, Int then String at row 2001 → String, all-missing → `Missing`, `Union{Int,Missing}` honoured, `typemap` (`Int⇒Float64`, `Int⇒Int32`, `Float64⇒Float32`, `Int128⇒Float64`), `downcast`, `groupmark`, `decimal`, quoted numbers with blanks, `BigInt`/`BigFloat`/`UUID`, `1e400`→Inf, `1e-400`→0.0, 40-digit floats, `0.30000000000000004`, `Bool` + missing, Int with 100 leading missings, trailing tab/space.

### R18 — `lazy` door

`CSV.lazy(src)` works well as a first look (`show` is the best of the three: `CSV.LazyFile("<GenericIOBuffer>"): 3 rows × 3 columns (indexed, cells lazy)` + schema; `lf.a[1:10]`, `lf[1, :a]`, `size`, `names`, `DataFrame(lf)`, `File(lf; types=, select=)` all work; `types=Dict(:a=>Int)` parses on access). Rough edges: `lf[1]` is column 1 while `f[1]` is row 1; no `lf["a"]`; no `iterate`; `eltype(lf) == Any`; `lf.prepared` exposes `CSVApi.Prepared`; `File(lf; header=false)` → `MethodError … got unsupported keyword argument "header"`; `CSV.LazyColumn` is not reachable at top level while `LazyFile` is; the lowercase-verb name sits oddly beside `File`/`Rows`/`Chunks` (either promote to a type-constructor `CSV.LazyFile(src)` or accept the verb and document it as such); the module docstring does not list it.

### R20 — buffer retention (design consequence)

```julia
df = CSV.read(IOBuffer("a,b\nx,1\n" * "y,2\n"^10000), DataFrame)
Base.summarysize(df.a)                       # 200168  — the whole file, for 10 001 one-byte values
f = CSV.File(IOBuffer("a,b\nx," * "z"^1000 * "\n"); select=[:a]); Base.summarysize(f.a)   # 1167 — buffer retained though the only value is inline
```

Consider dropping the buffer reference when a column has no view payloads.

### Sources (`b8.out`) — all fine except as noted

Path, `SubString` path, `IOBuffer` (incl. partially read: parses from the current position), `Vector{UInt8}`, `codeunits`, `SubArray`, `Cmd`, `IOStream` (left open afterwards, as 0.10), `Pipe`, `Base.Filesystem.File`, `GzipDecompressorStream`, `.gz` path on File/Rows/Chunks, gzip bytes in an IOBuffer, `.gz`-named plain file, FilePathsBase paths (File/Rows/vector), vectors of paths / IOBuffers / mixed with `source=:s`, `source=:s=>labels`, one-element vector with `source=`, missing columns across sources → missing-filled, problems re-based across sources, `>512 KiB` mmap path, URL 404 / unresolvable host → `Downloads.RequestError`. Not supported: `~` expansion (0.10 same; message could mention `expanduser`), `file://`, vector of sources on Rows/Chunks/lazy (bad message, R12). Failing `Cmd` → `ProcessFailedException` (fine).

### Threading / knobs (`b9.out`) — determinism holds

`ntasks=1`, `parallel=false`, `chunkbytes=64`, `ntasks=3` vs `8`, `scanner=:vec/:swar/:scalar`, `fastindex=false`, `samplebytes=1`, `prefetch=false`, `buffer_in_memory=true`, `validate=false` all produce identical columns AND identical `problems` on a 2 000-row quoted-multiline file and a 500-row ragged file. `ntasks=2, parallel=false` and `ntasks=1, parallel=true` are both accepted without complaint (`parallel` wins? undocumented). `fastindex`, `scanner`, `samplebytes`, `prefetch`, `chunkbytes` are accepted by every door but absent from the `File` docstring (`nsample` is documented) — either document as engine knobs or mark internal.

### Name normalization (`b9.out`) — identical to 0.10

21-name battery (`a b`, `1x`, `end`, `α β`, ``, `dup,dup`, `a_b`, `_a`, `a__b`, `Ab/ab`, `x-y`, `x.y`, `x/y`, `$x`, `x%`, `über`, `日本語`, `  spaced  `, `a\tb`) normalizes exactly as 0.10 does; `makeunique` (`a,a,a_1` → `a,a_2,a_1`) matches. `select` accepts raw or normalized spellings (#990) with and without `normalizenames`; but `types=Dict("a b"=>…)`, `pool=Dict("a b"=>…)`, `dateformat=Dict("a b"=>…)` do NOT accept the raw spelling after `normalizenames=true` (`types key a b does not match any column`) — inconsistent with `select`. `Rows(normalizenames=true)` works but cannot be observed via `names(rows)` (R11).

---

## 3. Docstrings / show / internal-name leaks (skim of `src/api.jl`, `src/CSV.jl`)

- Docstring headers use internal paths: `CSVApi.Spec`, `CSVApi.sniff`, `CSVApi.File(source; kw...)`, `CSVApi.read`, `CSVApi.lazy`, `CSVApi.LazyFile`, `CSVApi.Rows` (×2), `CSVApi.Chunks`; `KernelWrite.RowWriter`, `KernelWrite.write` in write.jl. Users type `CSV.File`.
- `Rows` has two docstrings (struct at api.jl:1842 and constructor at :1858) that disagree: the first says cells are `Union{String, Missing}` by default, the second says `CompactString`; only the second mentions `stringtype`/`reusebuffer`.
- `Chunks` docstring: "a sequence of `File`-shaped tables" — they are `ParsedTable`s (R4).
- `File` docstring does not list `chunkbytes`, `samplebytes`, `prefetch`, `fastindex`, `scanner` (all accepted); says `stringtype` `CompactString ⇒ CSV.CSVKernel.CompactStringVector`.
- Module docstring (`src/CSV.jl`) lists File/read/Rows/Chunks/write/problems but not `lazy`/`LazyFile`, and points readers at `CSV.CSVKernel`/`CSV.CSVApi`.
- `_LEGACYKW[:lazystrings]`: "use stringtype=CSVKernel.CompactString" (should be `CSV.CompactString`); `_LEGACYKW[:debug]`: "removed in 1.0: removed in 1.0; …" (doubled phrase). `_checkstringtype` error: "stringtype must be CSVKernel.CompactString, …".
- `show(File)` → `CSVApi.File("<GenericIOBuffer>"):`; `show(LazyFile)` → `CSV.LazyFile("<GenericIOBuffer>")`; `show(ParsedTable)` → `CSVKernel.ParsedTable: 3 × 3`; `show(Spec)` → `CSVApi.Spec(...)`. Three different prefixes; 0.10 printed `CSV.File("<IOBuffer: hash>")`.
- `_sourcename`: `<GenericIOBuffer>`, `<Array>` (Vector{UInt8}), `<CodeUnits>`, `<IOStream>`, `<Cmd>`, `<PosixPath>`, `<2 sources>`.
- `typeof`/schema strings users see constantly: `CSV.CSVKernel.CompactString`, `CSV.CSVKernel.CompactStringVector{CSV.CSVKernel.CompactString}`, `CSV.CSVApi.FileRow`, `CSV.CSVApi.Row`, `CSV.CSVApi.LazyColumn{Union{Missing, CSV.CSVKernel.CompactString}, CSV.CSVKernel.CompactString}`, `CSV.CSVApi.Prepared`, `CSV.CSVKernel.Problem`, `CSV.CSVKernel.ParsedTable`; `Tables.schema` shows them in full; DataFrames' header truncates to `CompactS…` (was `String7`/`String`).
- Error text prefixes: `CSVKernel: short_row at data row 2, column 0: …`.
- `Base.Docs.doc(CSV.problems)` → "No documentation found for private binding `CSV.CSVApi.problems`"; `Problem` has no docstring; `CSV.Problem` is not bound at top level (`isdefined(CSV, :Problem)` is true only via the `using .CSVKernel` re-export path — the type prints with the module).
- `KernelExamples.read` docstring calls itself the "`CSV.read` analog" (internal, but discoverable via `?`).
- `Spec` docstring: "splat it back — `File(src; spec.delim, spec.header)`" — fine, internal.

---

## 4. Keyword × door matrix

Legend: ✓ accepted & effective · ✗ refused (`ArgumentError: unsupported <door> keyword …` unless noted) · **ign** accepted but silently ignored · `ME` refused as a MethodError · `—` n/a. Derived from the `allowed` tuples in `src/api.jl` and confirmed by `b3.out`/`b6.out`/`b7.out`/`b9.out`.

| keyword | File / read | lazy | Rows | Chunks | File(lf) | transpose=true | 0.10 |
|---|---|---|---|---|---|---|---|
| header, normalizenames, skipto, limit, missingstring, delim | ✓ | ✓ | ✓ | ✓ | `ME` (fixed at lazy) | ✓ (`delim=nothing` → TypeError) | all doors |
| footerskip | ✓ | ✓ | ✓ | ✓ | `ME` | ✗ | all doors |
| quotechar, open/closequotechar, escapechar, quoted, comment, ignoreemptyrows, ignorerepeated | ✓ | ✓ | ✓ | ✓ | `ME` | ✓ | all doors |
| dateformat, decimal, truestrings, falsestrings, stripwhitespace, groupmark | ✓ | ✓ | ✓ | ✓ | `ME` | ✓ | all doors |
| types | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | all doors |
| select, drop | ✓ | ✓ | ✗ | ✗ | ✓ | ✗ (explicit msg) | File, Rows, Chunks |
| pool | ✓ (all forms) | ✗ | ✗ | ✓ scalar only | ✓ | **ign** | File, Chunks (Rows inert) |
| stringtype | ✓ | ✓ | ✓ | ✓ | ✓ | **ign** (always `String`) | all doors |
| typemap | ✓ | ✗ | ✗ | ✗ | ✗ `ME` | ✗ | all doors |
| downcast | ✓ | ✗ | ✗ | ✗ | ✓ | ✓ | all doors |
| strict / on_error | ✓ | ✗ | ✗ | ✗ | ✓ | **ign** (no problems ever) | all doors (`strict`) |
| maxwarnings (alias) | ✓ | ✗ | ✗ | ✗ | `ME` | **ign** | all doors |
| maxproblems | ✓ | ✗ | ✗ | ✓ | ✓ | **ign** | — |
| nsample | ✓ | ✗ | ✗ | ✗ | `ME` | ✗ | (`rows_to_check` all doors) |
| ntasks | ✓ | ✗ | ✗ | ✓ (= #batches) | `ME` | **ign** | File, Chunks |
| parallel | ✓ | ✓ | ✓ | ✓ | ✓ | **ign** | — |
| chunkbytes, samplebytes | ✓ | ✓ | ✓ | ✓ | `ME` | ✗ | — |
| buffer_in_memory, prefetch | ✓ | ✓ | ✓ | ✓ | `ME` | ✓ | `buffer_in_memory` all doors |
| validate | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | all doors |
| fastindex, scanner | ✓ (undocumented) | ✓ | ✓ | ✓ | `ME` | ✗ | — |
| scan | ✓ | ✗ | ✗ | ✗ | `ME` | ✗ (explicit msg) | — |
| transpose | ✓ | ✗ | ✗ | ✗ | `ME` | — | all doors |
| reusebuffer | ✗ | ✗ | ✓ inert | ✗ | `ME` | ✗ | Rows |
| source | vector form only | ✗ | ✗ | ✗ | `ME` | ✗ | File(vector) |
| 13 `_LEGACYKW` names | "removed in 1.0" | same | same | same | `ME` | same | — |

Flagged inconsistencies: `select/drop` on Rows/Chunks (regression from 0.10; projection is "free" per the README); `typemap`/`downcast`/`strict` on Rows/Chunks (regression); `maxproblems` yes / `nsample` no on Chunks; `File(lf)` exposing `parallel` but not `ntasks`, `maxproblems` but not `maxwarnings`, and refusing with MethodErrors; transpose's six silent ignores vs its one refusal.

---

## 5. Checked, fine

- Structure: empty file, header-only (with/without newline), only newlines (± `ignoreemptyrows`), BOM (± header), CRLF, CR-only, mixed endings, no trailing newline, single column, 1×1, trailing delimiters, leading blank line, comments first/mid-file, quoted newlines/CRLF/tabs, escaped quotes, `"a,b"` header, only delimiters, NUL bytes, 100 KB line, 1 000-column header, multi-row header (incl. blank cell → `ColumnN`), `header=2` with a quote in the junk line (1.0 correct where 0.10 returned nothing), space-aligned files (#853, 1.0 correct where 0.10 produced 4 columns), tab/`;`/`|` sniffing, `skipto`, `footerskip`, `limit` (incl. > rows, `limit=0` schema `Missing` like 0.10), `skipto+limit`, `footerskip+limit`, header=2/skipto=4 with a comment between.
- Missing handling: unquoted empty ⇒ missing in every door; `missingstring` string/vector adds spellings; `""` entries inert; quoted `"NA"` honoured; `NULL/NA/null` are NOT missing by default (same as 0.10).
- Values: see R16 table "unchanged" list; `Int64` overflow exact as `Int128` end-to-end into DataFrames and Arrow; narrowing (`Int8/Int32/UInt8/Float32/Float16`) with out-of-range ⇒ missing + problem; `types=[Int,nothing,Int]`; regex `types` keys; `validate=false`; `typemap` narrow targets; `downcast` with missings; `Complex{Float64}` custom type; `BigInt`, `BigFloat`, `UUID`.
- Quoting: bare quote / unclosed quote produce problems and keep raw bytes (`"ab"c` kept verbatim + problem; 0.10 silently gave `ab`); `quoted=false` recovers stray-quote files (2001 rows).
- Determinism: identical values and problems across `ntasks`, `parallel`, `chunkbytes`, `scanner`, `fastindex`, `samplebytes`, `prefetch`, `buffer_in_memory`.
- Legacy kwargs: all 13 `_LEGACYKW` entries error with a mapped message on every door; `maxwarnings` aliases `maxproblems`; `strict=true` ⇒ `on_error=:error`; `maxproblems=0` keeps the count.
- `CompactString` read-only interop: `==`/`isequal`/`hash` with `String` both ways, `Set`/`Dict` keys, `*`, `string`, `parse`, `occursin`, `replace`, `split`, `Symbol`, `convert(String, x)`, `String.(col)`, `Vector{String}(col)`, `copy`/`collect` → `Vector{CompactString}`; DataFrames `sort`, `groupby`, `innerjoin`, `filter`, `describe`, `vcat`, `unique`, `coalesce`, `df.a .* "!"`; `Arrow.write` round-trips as `String`; `CSV.write` of a CompactString column.
- Pooling: `pool=true/ratio/(ratio,cap)/Dict/vector` on File ⇒ `PooledVector{String}`; pooled columns mutate fine; `pool` only affects string columns (0.10 pooled Int columns under `Dict`).
- `Rows`: lazy cells are `CompactString`, restartable, `types` per column (Dict/Type), `stringtype=String/InlineString/String15`, `dateformat` with typed access, `limit`, `skipto`, `footerskip`, `header=false`, `missingstring`, escaped cells, `.gz`, `reusebuffer` inert as documented, `Tables.schema` reflects `types`/`stringtype`, `DataFrame(rows)`.
- `Chunks`: stable schema across batches under tiny `chunkbytes`, `limit` respected, names on every batch, per-batch problems, `Tables.partitions`, `DataFrame(chunk)`, `vcat`, `pool=true` ⇒ `PooledVector` per batch, `stringtype=String`, empty/header-only sources, iterate twice, `ntasks=100` on a tiny file.
- `lazy`: index reuse via `File(lf)` (with `types`/`select`/`pool`/`stringtype`), typed lazy columns, `stringtype=String/InlineString`, `select`, ragged rows ⇒ `missing`, escaped/bad-quote cells, empty/header-only, `Tables.schema`, `DataFrame(lf)`.
- Transpose: values, `header=false`, `header=N`, `header=[names]` (+ count check), `skipto`, `limit`, `missingstring`, `types=Dict`, `downcast`, quoted newlines, empty input, `read(..., DataFrame; transpose=true)`; `select`/`scan` refused with clear messages.
- Sources: see §2 "Sources".
- Name normalization: byte-for-byte parity with 0.10 on the 21-name battery; `makeunique`; empty header cell ⇒ `ColumnN`; numeric headers; BOM + quoted header; `select` by raw or normalized spelling.
