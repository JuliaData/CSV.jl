# Downstream / ecosystem interop review: `CompactString` and `CompactStringVector`

Branch `kernel-proveout`, Julia 1.12.6, `-t8`, env at `$SP/env` (DataFrames, Arrow, InlineStrings,
PooledArrays, CategoricalArrays, CodecZlib, Chairmarks). Every claim below was reproduced by a script
under `$SP/t/` (`t0`..`t13`; outputs in the matching `*.out`). Repo was not modified.

Doors compared throughout:
* **read door**: `CSV.read(src, DataFrame)` → string columns are `CSV.CSVKernel.CompactStringVector{…}` (immutable, adopted via `Tables.CopiedColumns`)
* **File door**: `DataFrame(CSV.File(src))` → `Vector{CompactString}` (mutable copy, 24 B/elem)
* `stringtype=String` → `Vector{String}`; `stringtype=InlineString` → `Vector{String31}` etc. (0.10 default)
* `pool=true` → `PooledVector{String}`

Settled decisions (CompactString default, pool=false, problems-as-data) are not re-argued; only consequences are reported.

---

## Ranked findings

| # | Sev | Finding | Doors affected |
|---|-----|---------|----------------|
| 1 | **blocker** | `CSV.read(path, DataFrame)` returns a DataFrame that cannot be mutated in place: `sort!` (by *any* column), `push!`, `append!`, `filter!`, `deleteat!`, `dropmissing!`, `unique!`, `subset!`, `keepat!`, `df[i,:s]=`, `df[:,:s]=`, `replace!`, `reverse!`, `permute!`, `shuffle!`, `map!`, `fill!`, `empty!` all throw | read door |
| 2 | **major** | No `CompactString(::AbstractString)` / `convert`: every String→CompactString assignment fails (`push!(df, row)`, `df[1,:s]="x"`, `df[:,:s] .= "x"`, `append!` with a String table, `replace!`, `fill!`, `Dict{CompactString}` set, `PooledArray`/`CategoricalArray` `push!`/`levels!`) | both doors |
| 3 | **major** | Regex gaps: `match`, `findfirst/findnext(::Regex)`, `split(s, ::Regex)` throw `ArgumentError`; `reverse(s)` MethodError. InlineString (0.10 default) supports all of these | all CompactString values |
| 4 | **major** | Output path 6–10× slower than `String`: `Base.write(io, ::CompactString)` is a byte loop; hits `print/println/join/*/string(s,1)/show(df)/CSV.write`. A 10-line `unsafe_write` method recovers 10× | all |
| 5 | **major** | The whole input buffer (mmap or heap) stays alive after `df = nothing; GC.gc()` until the *next* parallel parse — for `stringtype=String` too. Cause: spawned prefetch/index/parse closures capture `buf`; Julia roots the last finished task per thread. `ntasks=1`/`parallel=false`/`-t1` do not leak | every door, every stringtype, on ≥2 threads |
| 6 | **major** | Retention footguns beyond the documented "views pin the file": `serialize`/`deepcopy`/`Base.summarysize` of a 3-row slice carry the entire input (6 MB / 73 MiB); `copy(df)`, `collect`, `DataFrame(df)` do **not** detach; no public `materialize`; `Vector{UInt8}` input is adopted without copy (mutating it mutates the table) | read + File doors |
| 7 | **major** | Explicit `types=Dict(:s=>String)` / `types=String` is ignored: column still comes back `CompactStringVector`; `types=Dict(:s=>CSV.CompactString)` and `types=Dict(:s=>String3)` → `unsupported column type` | read + File |
| 8 | minor | `coalesce.(df.s, "")`, `ifelse.(…, "NA", df.s)`, comprehensions mixing literals, `string.(df.s)` with missing, `vcat`/`outerjoin` compact×InlineString → `Vector{AbstractString}` (InlineString in 0.10 had the same) | any non-String eltype |
| 9 | minor | Method-signature interop: `f(::Vector{String})`, `f(::AbstractVector{String})`, `f(::String)` MethodError (inherent); `f(::Vector{<:AbstractString})` fails on the read door only (CompactStringVector isn't a `Vector`) | read door worse |
| 10 | minor | Rows/lazy/Chunks: `Rows(stringtype=InlineString)` → `Vector{Union{Missing,InlineString}}` (abstract eltype); `Rows(types=Dict(:n=>Int))` flips the *other* columns to `String`; `Rows` docstring says `Union{String,Missing}`; `DataFrame(CSV.Chunks(…))` silently builds a nonsense table; all chunks share one buffer | Rows/Chunks/lazy |
| 11 | minor | Per-op perf cliffs on view strings: `length` 3.6×, `startswith` 3.6×, `split` 7× (9 allocs vs 3), `uppercase` 1.5×, `occursin(::Regex)` allocates a String per call. Hash/==/cmp/sort/Set/groupby/join are at parity or faster | all |
| 12 | minor | `stringtype=InlineString` read is 5.6× slower than `stringtype=String` (444 vs 79 ms on 73 MiB; default 28 ms): bound by InlineStrings' wide pointer constructor (165 ns for `String63`) plus 2 allocs/cell in `_inl` | InlineString door |
| 13 | cosmetic | Types print as `CSV.CSVKernel.CompactString` / `CSV.CSVKernel.CompactStringVector{Union{Missing, CSV.CSVKernel.CompactString}}` in `typeof`, `summary`, `show(CSV.File)`, `Tables.schema`, `LazyFile`; DataFrames header abbreviates to `CompactS…`; `CSV.CompactString` is not `public`; `CompactStringVector`/`materialize` unreachable from `CSV` | display |

Everything else exercised (grouping, joins, hashing across buffers, sorting, Sets/Dicts, categorical/pooled,
Base string API, display of values, Arrow, CSV.write round-trip, Serialization round-trip, missing handling,
Threads) worked; see "What worked" at the end.

---

## 1. `CSV.read(path, DataFrame)` hands DataFrames immutable columns — blocker

`read(source, sink) = sink(Tables.CopiedColumns(File(source)))` (api.jl:1626). DataFrames honors
`CopiedColumns` and adopts the `CompactStringVector` as-is. It defines only `size`/`getindex`, so every
DataFrames operation that mutates a column in place fails, **including operations that have nothing to
do with the string column** (sorting by an Int column permutes all columns):

```julia
df = CSV.read(IOBuffer("s,n\nabc,1\nthis-is-a-longer-string-than-12,2\ndef,3\nxyz,4\n"), DataFrame)
sort!(df, :n, rev=true)   # CanonicalIndexError: setindex! not defined for CSV.CSVKernel.CompactStringVector{CSV.CSVKernel.CompactString}
filter!(:n => >(1), df)   # MethodError: no method matching deleteat!(::CSV.CSVKernel.CompactStringVector{…}, ::Vector{Int64})
push!(df, (s="new", n=9)) # MethodError: no method matching CSV.CSVKernel.CompactString(::String)  (+ DataFrames logs "Maybe you forgot passing `promote=true`?")
append!(df, copy(df))     # MethodError: no method matching resize!(::CSV.CSVKernel.CompactStringVector{…}, ::Int64)
```

Observed on the read door (t1.out, t9.out). Fails: `push!(df,row)`, `append!`, `df[1,:s]=`, `df.s[1]=`,
`df[:,:s] .=`, `df[:,:s] = v`, `filter!`, `deleteat!`, `sort!` (any key; only a no-op sort on already
sorted data passes), `unique!` (when duplicates exist), `dropmissing!` (even with no missings — DataFrames
calls `deleteat!` with an all-false mask), `subset!`, `keepat!`, `replace!`, `reverse!`, `permute!`,
`invpermute!`, `shuffle!`, `map!`, `fill!`, `empty!`, `resize!`, `pop!`, `push!(df.s, x)`,
`df[2,:s] = df[1,:s]` (even with a CompactString value). Passes: everything non-mutating or
column-replacing (`sort`, `unique`, `filter`, `df[!, :s] .= x`, `df[!, :s] = v`, `transform!`,
`select!`, `insertcols!`, `rename!`, `allowmissing!`, `disallowmissing!`, `vcat`, `hcat`, `repeat`,
`stack`, `unstack`, `flatten`, `copy(df)`, `df[rows, :]`, `view`).

The File door (`DataFrame(CSV.File(path))`) copies into `Vector{CompactString}` and all of the above
pass except those that assign a `String` (finding 2). `stringtype=String` and `stringtype=InlineString`
pass all of them.

The error messages are not actionable: nothing says "columns from `CSV.read` are read-only; use
`copy(df)`/`String.(col)`/`stringtype=String`". `push!(df, row)` produces the same confusing
`CompactString(::String)` MethodError on both doors.

Cost of the alternatives (73 MiB / 1M-row file, t11.out): `DataFrame(Tables.CopiedColumns(f))` 0 B,
0.0 ms; `DataFrame(f)` 61 MiB, 1.4 ms — `sizeof(CompactString) == 24` and it is **not isbits** (holds
the `data` pointer), so a `Vector{CompactString}` is 24 B/elem vs 16 B for the payload vector and is a
pointer-bearing array the GC must scan. Sort/groupby perf is the same on both (30 vs 27 ms; 8 vs 11 ms
for String).

**Recommendation** (pick one; (a) preferred):
- (a) Make `CompactStringVector` a real mutable vector: forward `deleteat!`, `resize!`, `pop!`, `push!`,
  `empty!`, `sizehint!`, `permute!`, `insert!`, `splice!` to `payloads`; implement `setindex!(v, x, i)` as: store
  the payload verbatim when `x` is inline or `x.data === v.buf`/`v.extra`; otherwise copy the bytes into
  the column-owned `extra` arena (append-only, already a `Vector{UInt8}`) and store a bufidx=1 view.
  Also declare `Base.IndexStyle(::Type{<:CompactStringVector}) = IndexLinear()` (currently reports
  `IndexCartesian()`). With this, the read door keeps zero-copy adoption and users get a normal column.
- (b) Have `read` return `collect`ed `Vector{CompactString}` (what the File door already does), accepting
  the 24 B/elem + GC-scan cost. Simpler, but a `Vector{CompactString}` still cannot accept `String`s
  without finding 2.
- (c) At minimum, throw a descriptive error from `setindex!`/`deleteat!`/`resize!` on
  `CompactStringVector` pointing at `copy(df)`, `String.(col)`, and `stringtype=String`.

## 2. No `CompactString(::AbstractString)` constructor — major

```julia
CSV.CompactString("abc")            # MethodError: no method matching CSV.CSVKernel.CompactString(::String)
convert(CSV.CompactString, "abc")   # same
push!(CSV.CompactString[], "x")     # same;  push!(String31[], "x") works
df = DataFrame(CSV.File(io)); df[1, :s] = "new"      # same (File door)
d = Dict{CSV.CompactString,Int}(); d["k"] = 1        # same (lookups with String keys DO work)
c = categorical(df.s); push!(c, "new"); levels!(c, ["a","b"])   # same
p = PooledArray(df.s); push!(p, "zzz")               # same
```

This single missing method is the root cause of ~40% of the failures in t1/t7/t9/t10 and is what makes
the File door half-mutable. It also blocks the obvious workaround for finding 8
(`coalesce.(df.s, CSV.CompactString(""))`).

**Recommendation:** define `CompactString(s::AbstractString)` (and `convert`): `n ≤ 12` → inline
payload built from the bytes; `n > 12` → copy the bytes into a fresh owned `Vector{UInt8}` and store a
view at offset 0 (bufidx is irrelevant outside a column). `promote_rule(CompactString, String) = String`
already exists, so mixed containers still promote to `String`. Also consider `CompactString(::SubString)`,
`CompactString(::InlineString)` fall through the same path.

## 3. Regex / Base-string gaps — major

Base's regex machinery is `String`/`SubString{String}`-only; for other `AbstractString`s some entry
points convert and some throw. Observed (t3.out, t9.out) for both inline and view values:

```julia
s = CSV.read(IOBuffer("s\nthis-is-a-longer-string-than-12\n"), DataFrame).s[1]
match(r"(\d+)", s)        # ArgumentError: regex matching is only available for the String and AnnotatedString types; use String(s) to convert
findfirst(r"[a-z]", s)    # ArgumentError: regex search is only available for the String type; use String(s) to convert
split(s, r"-")            # same
reverse(s)                # MethodError: no method matching reverse(::CSV.CSVKernel.CompactString)
transcode(UInt16, s)      # MethodError
# work (Base converts internally, 1 String allocation each):
occursin(r"long", s); eachmatch(r"[a-z]+", s); replace(s, r"a" => "X"); startswith(s, r"^t")
```

`match(r"(\d+)", row.s)[1]` is one of the most common things done to a CSV string cell. InlineStrings
defines `match`, `findnext(::Regex)`, `reverse` (verified: `match(r"[a-z]+", String31("abc"))` → `SubString{String}`),
so this is a regression from the 0.10 default.

**Recommendation:** mirror InlineStrings: `Base.match(r::Regex, s::CompactString, i::Integer=1, add_opts=0) = match(r, String(s), i, add_opts)`,
`Base.findnext(r::Regex, s::CompactString, i)`, `Base.split(s::CompactString, r::Regex; kw...)`,
`Base.reverse(s::CompactString) = reverse(String(s))`. (For view strings a pointer-based
`Base.PCRE.exec` avoiding the copy is possible later.)

## 4. `write`/`print` path is 6–10× slower than `String` — major (10-line fix)

`Base.write(io, s::CompactString)` (compactstring.jl:329) writes one byte per `write(io, codeunit(s,i))`
call, and `Base.print` goes through it. write.jl's `_writestring`/`_appendstring!` take
`codeunits(s)` (a generic `CodeUnits{UInt8,CompactString}`), which also degrades to per-byte access.
1e6 rows × 29-byte view strings (t3.out, t9.out):

| op (1e6 rows) | CompactString | String | InlineString |
|---|---|---|---|
| `for x in col; write(io, x)` | **169 ms** | 18 ms | 31 ms |
| `println(io, x)` loop | 184 ms | 36 ms | 68 ms |
| `join(col, "\n")` | 181 ms | 29 ms | 28 ms |
| `col .* "x"` | 235 ms (7 allocs/elem) | 25 ms (1) | 210 ms |
| `string(s, 1)` (single) | 174 ns | 80 ns | — |
| `show(df)` (text/plain, 1e6 rows) | 3.4 s | 2.3 s | 2.1 s |
| `CSV.write(io, df)` | 26.8 ms | 13.6 ms | 51.7 ms |

Prototype (t9.jl, scratch-only override): views → `unsafe_write(io, pointer(data, pos), n)`; inline →
two `htol` word stores into a stack `Ref` then `unsafe_write`. Verified byte-exact for 3-, 12- and
31-byte values. Result: write loop 109 → 10.7 ms (**10.2×**), join 104 → 17 ms (6×), CSV.write 26.8 →
15.6 ms, println loop 184 → 45 ms.

**Recommendation:** replace `Base.write(io::IO, ::CompactString)` with the pointer version; add a
`CompactString` fast path in write.jl's `_writebytes`/`_appendbytes!` (pointer+len for views, 16-byte
scratch for inline) so `CSV.write` reaches String parity.

## 5. Input buffer survives `df = nothing` until the next parallel parse — major

Not a CompactString issue: it happens with `stringtype=String` and with numeric-only `select`. Bisected
in t12/t13 (each stage run in a callee returning only a `WeakRef`, then 3× `GC.gc(true)`):

| stage | buffer alive after GC? |
|---|---|
| control: 73 MiB `Vector` built in callee | no |
| control: same, touched once by a detached `Threads.@spawn` | **yes** |
| `resolvesource(path)` (mmap + prefetch touchers) | **yes**; `prefetch=false` → no |
| `_prepare(bytes)` (sniff + parallel index) | **yes** |
| `File(bytes)` default / `stringtype=String` / `select=[:id,:val]` / `lazy` / `Rows` / `Chunks` | **yes** |
| `File(bytes; parallel=false)` or `ntasks=1` or `julia -t1` | no |
| three sequential `File(bytes)` | only the **last** is alive |
| after spawning no-op tasks on every thread + GC | freed |

Mechanism: the spawned closures (prefetch touchers in `_prefetch!`, api.jl:148; `@sync`/`@spawn` index
and column waves in core.jl/api.jl) capture `buf`; Julia keeps the most recently finished task on each
thread reachable, so the closure — and the 73 MiB mapping/heap buffer — stays rooted until another task
runs there. For users: "I did `df = nothing; GC.gc()` and RSS did not drop"; on a 10 GiB file this is
10 GiB of address space (and resident pages) pinned by nothing visible. `buffer_in_memory=true` does not
help; `ntasks=1` does (at the cost of parallelism).

**Recommendation:** never capture the buffer directly in a spawned closure. Pass a small mutable holder
(e.g. `Ref{Union{Nothing,Vector{UInt8}}}` or the per-chunk work struct) and clear it after the `@sync`
block; make the prefetch touchers hold a `WeakRef` (they only touch pages). Add a 2-thread test:
`wr = (f = CSV.File(path); WeakRef(f.col.buf)); GC.gc(); @test wr.value === nothing`.

## 6. Retention-by-design footguns: slices, serialize, deepcopy, summarysize, adopted input — major

The documented design is that views pin the file (docs/stringtype-comparison.md §3). The concrete
consequences a DataFrames user hits (t5.out, t6.out, t11.out; 73 MiB file unless noted):

```julia
df = CSV.read("big.csv", DataFrame)          # 73 MiB, 1M rows
small = df[1:10, :]; df = nothing; GC.gc()   # mapping still alive (10 long views)  ✓ expected
Base.summarysize(small)                      # 72.9 MiB for 10 rows
serialize(io, small)                         # writes 72.9 MiB  (Distributed/pmap ships the file per message)
deepcopy(small)                              # allocates 88 MiB
serialize(io, df.s[1])                       # a single long CompactString → 6.17 MB (200k-row in-memory case)
copy(df).s[1].data === buf                   # true: copy/collect/DataFrame(df) do NOT detach
Base.summarysize(df.name)                    # 88 MiB even for an all-inline column (buf is a field)
```

Other observations:
- Inline-only slices (`df.name[1:10]`, all ≤12 bytes) do not pin the buffer (`data === EMPTY_BYTES`). Correct.
- `String.(col)`, `convert(Vector{String}, col)`, `Vector{String}(col)`, `K.materialize(col)` all detach
  (24–28 ms per 1M rows); `CSV.materialize` is **not defined** (`isdefined(CSV, :materialize) == false`),
  and there is no table-level idiom besides `mapcols(c -> eltype(c) <: Union{Missing,AbstractString} ? String.(c) : c, df)`.
- `rm(path)` while views are alive works on macOS and values still read correctly; `lsof` lists the file
  while the mapping lives. On Windows an open mapping blocks delete/overwrite of the CSV (not tested here) —
  worth a docs note since the default now keeps mappings alive for the table's lifetime.
- `Vector{UInt8}` input is adopted, not copied: `getfield(df.s, :buf) === bytes`; mutating `bytes` after the
  read changes table values (`DATA[7] = 'Z'` → `"Zhis-is-a-long…"`), and `empty!(bytes)` leaves views
  reading past the vector's length under `@inbounds`. `IOBuffer`, `IO`, `Cmd`, `codeunits` inputs are
  copied (`Base.read(io)` / `Vector{UInt8}(buf)`), so only the `Vector{UInt8}` path aliases. 0.10 also
  adopted the vector but copied strings out, so this is newly observable.
- Every column of a table (and every `CSV.Chunks` batch) shares the one buffer, so a chunked read cannot
  bound memory for string columns unless `stringtype=String`.

**Recommendation:** (1) custom `Serialization.serialize(::AbstractSerializer, ::CompactString)` writing the
bytes (deserialize as an owned string), and for `CompactStringVector` serialize as materialized or as
payloads + only the referenced byte ranges; (2) `Base.deepcopy_internal` for `CompactString` /
`CompactStringVector` that materializes instead of copying `buf`; (3) export `CSV.materialize(col)` and a
table-level `CSV.materialize(df)`/`materialize(table)` and document it next to `stringtype=String` as the
"detach from the file" idiom; (4) document (or copy) the `Vector{UInt8}` adoption.

## 7. Explicit `types=…=>String` is not honored — major

```julia
CSV.read(io, DataFrame; types=Dict(:s => String)).s   # CSV.CSVKernel.CompactStringVector{…}   (expected Vector{String})
CSV.read(io, DataFrame; types=String)                 # every column CompactStringVector
CSV.read(io, DataFrame; types=Dict(:s => CSV.CompactString))  # ArgumentError: unsupported column type CSV.CSVKernel.CompactString
CSV.read(io, DataFrame; types=Dict(:s => String3))    # ArgumentError: unsupported column type String3
CSV.read(io, DataFrame; types=String, stringtype=String).s     # Vector{String}
```

`types=String` ("read everything as text") is a very common 0.10 idiom and returned `Vector{String}`.
Here `types` decides *that* a column is text and `stringtype` alone decides the representation, so an
explicit `String` is silently downgraded, and there is no way to name the type you will actually get.

**Recommendation:** treat an explicit `String`/`InlineString`/`StringN`/`CompactString` in `types` as a
per-column `stringtype` override (materialize that column), keeping the `stringtype` keyword as the default
for inferred text columns. Accept `CSV.CompactString` in `types`.

## 8. `Vector{AbstractString}` results from mixing literals — minor

```julia
dm = CSV.read(IOBuffer("s,n\nabc,1\n,2\n"), DataFrame)
coalesce.(dm.s, "")                       # Vector{AbstractString}   (String: Vector{String}; String31: Vector{AbstractString})
ifelse.(ismissing.(dm.s), "NA", dm.s)     # Vector{AbstractString}
[ismissing(x) ? "NA" : x for x in dm.s]   # Vector{AbstractString}
string.(dm.s)                             # Vector{AbstractString}  (string(::AbstractString) is identity)
vcat(df_compact, df_inline).s             # Vector{AbstractString};  [cs, String31("x")] likewise
outerjoin(df_inline, df_compact, on=:long).long  # Vector{AbstractString}
```

`promote_type(CompactString, String) == String` is defined and works for `[cs, "x"]` → `Vector{String}`,
`vcat(df.s, ["x"])` → `Vector{String}`; broadcast uses `promote_typejoin`, which nothing can fix. Same
behaviour as InlineString under 0.10, so not a regression, but it is the price of a non-`String` default
and should be in the docs with the idiom `String.(coalesce.(df.s, ""))` (or `coalesce.(df.s, CSV.CompactString(""))`
once finding 2 is fixed).

## 9. Method-table interop — minor (inherent), read door slightly worse

`df.s isa AbstractVector{<:AbstractString}` ✓, `f(::AbstractVector{<:AbstractString})` ✓, `f(::AbstractString)` ✓.
`f(::Vector{String})`, `f(::AbstractVector{String})`, `f(::AbstractVector{Union{String,Missing}})`,
`f(::String)` → MethodError (same as InlineString). `f(::Vector{<:AbstractString})` fails on the read door
only (CompactStringVector is not a `Vector`) and passes on the File door. Conversions all work:
`convert(Vector{String}, df.s)`, `Vector{String}(df.s)`, `String[x for x in df.s]`, `String.(df.s)`,
`String[s]`, `copyto!(Vector{String}(undef,n), df.s)`, `append!(String[], df.s)`,
`Vector{Union{String,Missing}}(dm.s)`.

## 10. Rows / Chunks / lazy doors — minor

- `DataFrame(CSV.Rows(io))` → every column `Vector{Union{Missing,CompactString}}` (no inference; documented,
  but the first `Rows` docstring at api.jl:1846 still says `Union{String, Missing}`). Values view the
  `Rows` buffer (retained).
- `DataFrame(CSV.Rows(io; types=Dict(:n=>Int)))` → the untyped columns become `Vector{Union{Missing,String}}`
  (`T === nothing ? String : T`, api.jl:1872) while without `types` they are CompactString. Inconsistent.
- `DataFrame(CSV.Rows(io; stringtype=InlineString))` → `Vector{Union{Missing, InlineString}}` — an
  abstract eltype, because `_rowstring(InlineString, x)` picks the smallest width per *cell*
  (`unique(typeof.(d.s)) == [String3, String31]`). Recommend a fixed width per column for Rows or documenting.
- `DataFrame(CSV.lazy(io))` → all columns `Vector{Union{Missing,CompactString}}` even with no missings (fine
  for a "first look", worth a docs line). `DataFrame(CSV.File(lf))` works and reuses the index.
- Chunks: `DataFrame(chunk)`, `Tables.columntable(chunk)`, `reduce(vcat, DataFrame.(CSV.Chunks(io)))`
  (equal to a full read), stable schema across batches, `Arrow.write(io, CSV.Chunks(io))` (partitions →
  record batches), `CSV.write(io, chunks)`, `stringtype=String`, `pool=true` all work. But
  `DataFrame(CSV.Chunks(io))` and `Tables.columntable(CSV.Chunks(io))` silently produce a 10-row table
  whose cells are column vectors (Tables treats the chunk iterator as rows). Recommend a clear error or
  a `Tables.columns(::Chunks)` that vcats. `chunk.s` is a `FieldError` (`chunk[:s]` works).
- File: `f.s`, `f[1]`, `f[1].s`, `f[1][:s]`, `length(f)` work; `f[:s]`, `f[1, :s]`, `size(f)` are
  MethodErrors.
- `pool=[:s]` and `pool=(col,name)->…` (0.10 forms) are rejected; `pool=Dict(:s=>true)`, per-column
  vector, `Bool`, ratio, `(ratio, cap)` work. `pool` with `lazy`/`Rows` gives a clear unsupported-keyword error.

## 11. Per-operation perf on view strings — minor

Single 31-byte view string vs `String` (t9.out): `hash` 8.1 vs 8.4 ns, `isless` 3–5 ns, `cmp` 5 ns,
`==` against a literal 4.9 ns (String==String 1.1 ns), all zero-alloc. `length` 73 vs 20 ns, `startswith`
27 vs 7 ns, `uppercase` 200 vs 130 ns, `split(s, "-")` 1018 vs 140 ns (9 vs 3 allocs), `s * "x"` 138 vs
19 ns (7 vs 1 allocs — fixed by finding 4), `occursin(::Regex)` allocates 48 B per call (String copy).
Column-level (1e6 rows): `sort` 113 vs 112 ms, `Set` 42 vs 87 ms (faster), `.== literal` faster,
`groupby`/`innerjoin`/`unique` at parity, `length.` 93 vs 31 ms, `startswith.` 25 vs 6 ms,
`split.` 1.57 s vs 0.34 s (InlineString 1.40 s), `uppercase.` 434 vs 294 ms, `occursin.(regex)` same
time but 1e6 allocs. Cheap wins: pointer-based `startswith`/`endswith`, an ASCII fast path for `length`,
and `findnext(::String needle, ::CompactString, i)` via `memchr`/`memmem` (would fix `split`/`replace`).

## 12. `stringtype=InlineString` read speed — minor

73 MiB file: default 28 ms, `String` 79 ms, `InlineString` **444 ms**. `_materializecolumn(String63, col)`
193 ms/1M ≈ `String63.(Vector{String})` 217 ms, i.e. the extension is bound by InlineStrings' wide
constructor (`String63(ptr, n)` 165 ns; `String7` 2 ns) plus `_inl`'s heap-allocated `Ref` scratch
(2 allocs, 134 ns per inline cell). 0.10 built InlineStrings incrementally while parsing. Recommend
building `String15..String255` directly from the 16-byte payload words (inline) / 8-byte loads (views)
instead of the pointer constructor.

## 13. Naming and display — cosmetic

- `typeof(df.s)` → `CSV.CSVKernel.CompactStringVector{Union{Missing, CSV.CSVKernel.CompactString}}`;
  `summary(s)` → `"3-codeunit CSV.CSVKernel.CompactString"`; `show(CSV.File(io))`, `Tables.schema`,
  `show(LazyFile)`, `Rows` schema all print the `CSV.CSVKernel.` path. `describe(df).eltype` prints
  `CompactString`. DataFrames' header abbreviates to `CompactS…` / `CompactS…?` (its 8-char rule; `String31?`
  fits). `CSV.CompactString` is a `const` alias, not `public`; `CSV.CompactStringVector` and `CSV.materialize`
  don't exist. Because the struct is defined in `CSVKernel`, exporting the alias does not change how it prints.
  Recommend defining `CompactString`/`CompactStringVector` in module `CSV` itself (compactstring.jl is already
  Base-only) and `public`-marking them, so everything prints `CSV.CompactString`.
- `show(CSV.File)` header reads `CSVApi.File("<GenericIOBuffer>")` while `LazyFile` prints `CSV.LazyFile(...)`;
  chunks print `CSVKernel.ParsedTable`. `FileRow` show has one extra space (`:s   "abc"`) vs `Row`.
- Values display correctly everywhere: `"abc"` for `show`/`repr`/`display`, `abc` for `print`, identical to
  `String`; `missing` shown as `missing`; `show(df)` text/html/latex/csv all fine; `describe(df)` works
  (min/max/nunique on string columns).

---

## What worked (coverage)

- **Grouping/joins/ops**: `groupby` (incl. `sort=true`, multi-key, with missing), `combine(nrow / sum / first / join)`,
  `innerjoin`/`leftjoin`/`outerjoin`/`antijoin` CompactString×String and CompactString×CompactString from
  different buffers (hash/== agree across buffers: `Set(df.s) == Set(df2.s)`, `df.s == df2.s`), `unique`, `Set`,
  `Dict` counting (both `Dict{CompactString}` and `Dict{String}` — CompactString keys convert into `Dict{String}`),
  `df.s .== "abc"`, `in.(df.s, Ref(Set(...)))`, `sort`/`sortperm`/`issorted`/`searchsorted` identical to String,
  `levels`, `CategoricalArray`/`categorical`, `PooledArray`, `recode`, `maximum`/`minimum`/`extrema`, `findall`,
  `count(startswith("a"), …)`, `filter`/`subset` with `contains`/regex closures, `nonunique`, `allunique`,
  `describe`, `Threads.@threads` over a column.
- **Base string API** (inline and view values): `occursin` (String and Regex), `eachmatch`, `replace` (String
  and Regex patterns), `split(s, "-")`/`split(s, '-')`, `strip`, `lowercase`/`uppercase`/`titlecase`,
  `startswith`/`endswith` (String and Regex), `*`, `^`, `string(s, 1)`, `"$s"` interpolation, `join`, `Symbol`,
  `length`, `sizeof`, `ncodeunits`, `codeunits`, `s[1:3]` → `SubString{CompactString}`, `s[1]`, `first/last/chop/chomp`,
  `collect`, `isvalid`, `isascii`, `nextind/prevind/thisind`, `findfirst/findlast("a"/'a')`, `count`, `lpad/rpad`,
  `parse`/`tryparse` (Int/Float64), unicode iteration and `StringIndexError` parity, `escape_string`, `ascii`,
  `filter(isletter, s)`, `Base.Unicode.normalize`, `isempty`, `Regex(s)`, using `s` as a needle in
  `occursin`/`replace`/`split`; `==`/`isequal`/`hash`/`cmp`/`isless` agree with `String`, `SubString{String}`
  (incl. offset substrings) and `InlineString`, with a seeded hash too; 13+-byte equality with same-prefix
  differing-tail literals correct; `Dict{String}` get/haskey/getindex/setindex with CompactString keys;
  `Set{String}`/`Set{CompactString}` membership both ways; `println`/`repr`/`show`/`sprint(print)`;
  `convert(String, s)`, `String[s]`, `Vector{String}(col)`; `coalesce`/`something`/`passmissing`/`ismissing`/
  `isequal(missing, s)`; `@allocated == 0` for `getindex`, `==`, `hash`, `cmp`, `isless`, `length`, `startswith`;
  `@inferred getindex`.
- **Missing handling**: `dropmissing`, `skipmissing`, `ismissing.`, `passmissing(uppercase).`, `replace(col, missing=>"")`,
  `disallowmissing` (errors only with real missings, same as String), `df.s .== "abc"` → `Union{Missing,Bool}`,
  `isequal.`, `subset(...; skipmissing=true)`, `sort` with missing last, `levels` drops missing, Arrow round-trip.
- **Sinks**: Arrow.write/Arrow.Table round-trip exact for CompactString, with missing, and pooled
  (`DictEncoded`); bytes identical to the String version; ~equal time (30.5 vs 25.8 ms / 200k rows) but 663k vs
  306 allocations (Arrow.jl converts each cell; no StringView zero-copy path exists in Arrow.jl today, and
  `ArrowTypes.ArrowType(CompactString)` is not specialized). `Arrow.write(io, CSV.File(...))`, `(io, CSV.Rows(...))`,
  `(io, CSV.lazy(...))`, `(io, CSV.Chunks(...))` all work. `CSV.write` output byte-identical to String columns,
  quotes/escapes round-trip, `RowWriter` fine. `Serialization` round-trips (`deserialize` returns a working
  `CompactStringVector`). `copy`/`deepcopy` work (see finding 6 for size).
- **Pooled path** (`pool=true`, `(0.5,100)`, `0.5`, per-column vector/Dict; with `stringtype=String`/`InlineString`):
  `PooledVector{String}` / `{Union{Missing,String}}`, fully mutable (`push!`, `setindex!`, `sort!`), `levels`,
  `groupby`, `dropmissing`, Arrow dict-encoding.
- **Rows/Chunks/lazy**: `for row in Rows` (`row.s::CompactString`, `row[:n]`, `NamedTuple(row)`, `propertynames`),
  `collect(Rows)` rows survive (no `reusebuffer` aliasing), `Tables.dictrowtable/dictcolumntable/rowtable`,
  `Iterators.take`, `limit`, `CSV.write(io, Rows)`; typed `Rows(types=...)` returns typed cells; chunk
  DataFrames concatenate to the same table as a full read; `lf.col`, `lf[i, :col]`, `size(lf)`, `File(lf)`.
- **Input kinds**: path (mmap ≥ 512 KiB, heap otherwise), `IOBuffer`, `IOStream`, `Vector{UInt8}`, `codeunits`,
  gzip magic-byte detection; `close(io)` after read is safe (buffer copied); `read(io)`/`take!` inputs fine.
