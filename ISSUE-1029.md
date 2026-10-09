# Issue #1029 Triage Report

## Summary
`CSV.read` on a multithreaded parse dies with
`TypeError: in typeassert, expected SentinelVector{String15,...}, got SentinelVector{Int64,...}`.
`ntasks=1` avoids it. Reported 2022-10-02 against CSV v0.10.4.

## Issue Type
Bug — **still present in v0.10.17, gone in v1.0.0.**

## Reproduction
- Reproduced: **Yes** (exact error string, on both v0.10.4 and v0.10.17)
- Julia versions tested: 1.8.5 (for v0.10.4) and 1.12.6 (for v0.10.17 and v1.0.0)
- Package versions tested: CSV v0.10.4, v0.10.17, v1.0.0 (tag `v1.0.0`, commit 092eb2a)
- Threads: 10

### What actually triggers it
Two things have to happen together:

1. **Some data rows carry more fields than the header.** In 0.10.x a parse task
   that sees an extra field pushes its own task-local `Column` object straight
   into the shared top-level `columns` vector
   (`src/file.jl`, `multithreadparse`), so the widened column is aliased between
   one task and the parent.
2. **That widened column parses as `Int64` in some byte chunks and as a short
   string in others.** `multithreadpostparse` then has to promote every chunk to
   the final string type; for a pool-eligible column, `checkpooled!` asserts
   `SVec{String15}` and gets the un-promoted `SVec{Int64}`.

Ingredient 1 is exactly what the reporter's script produces. Its cleanup step
`split(line, "NULL),")` in `WikipediaDiGraph.jl/test/runtests.jl` splits the
MySQL `INSERT` line on the literal text `NULL),`, so any record whose
`page_lang` is **not** NULL is merged with the next record and the resulting CSV
row has extra fields. `page_links_updated` supplies ingredient 2: it is a quoted
14-digit number (`Int64`) in most chunks and the literal `NULL` (`String15`,
alongside the 14-char timestamps) in others.

Neither ingredient alone reproduces it — I ran 60 well-formed files with
cross-chunk type flips through v0.10.4 and got 0 failures.

### Observed error (CSV v0.10.17, Julia 1.12.6, 10 threads)
```
nested task error: TypeError: in typeassert,
  expected SentinelArrays.SentinelVector{String15, String15, Missing, Vector{String15}},
  got a value of type SentinelArrays.SentinelVector{Int64, Int64, Missing, Vector{Int64}}
Stacktrace:
 [1] checkpooled!(::Type{Union{Missing, String15}}, pertaskcolumns::Vector{Vector{CSV.Column}}, ...)
```
Character-for-character the message in the issue.

### Generator (`fuzzgen.jl`)
```julia
using Random
qesc(s) = "'" * replace(s, "\\" => "\\\\", "'" => "\\'") * "'"
const SHORT = ["wikitext","css","js","NULL","abc","Dog's","Olwechsel"]
const LONG  = ["a_title_longer_than_fifteen_bytes","another_quite_long_value_here_ok"]

function patternfn(kind, rng, n)          # per-column value generator
    cut1 = rand(rng, 1:n); cut2 = rand(rng, 1:n); lo, hi = minmax(cut1, cut2)
    if     kind == :int                 ; i -> string(rand(rng, -10^9:10^9))
    elseif kind == :int_sentinel        ; i -> string(rand(rng, (typemin(Int64)+1, typemin(Int64), -1, 0, 7)))
    elseif kind == :int_then_float      ; i -> i <= lo ? string(rand(rng, 0:10^6)) : string(rand(rng))
    elseif kind == :int_then_str        ; i -> i <= lo ? string(rand(rng, 0:10^6)) : qesc(rand(rng, SHORT))
    elseif kind == :str_then_int        ; i -> i <= lo ? qesc(rand(rng, SHORT)) : string(rand(rng, 0:10^6))
    elseif kind == :int_str_int         ; i -> (i <= lo || i > hi) ? string(rand(rng, 0:10^6)) : qesc(rand(rng, SHORT))
    elseif kind == :missing_then_int    ; i -> i <= lo ? "" : string(rand(rng, 0:10^6))
    elseif kind == :missing_then_str    ; i -> i <= lo ? "" : qesc(rand(rng, SHORT))
    elseif kind == :int_then_missing_then_str; i -> i <= lo ? string(rand(rng,0:10^6)) : (i <= hi ? "" : qesc(rand(rng, SHORT)))
    elseif kind == :narrow_then_wide_str; i -> i <= lo ? qesc("abc") : qesc(rand(rng, LONG))
    elseif kind == :int_then_narrow_then_wide; i -> i <= lo ? string(rand(rng,0:99)) : (i <= hi ? qesc("abc") : qesc(rand(rng, LONG)))
    elseif kind == :bool_then_str       ; i -> i <= lo ? string(rand(rng, ("true","false"))) : qesc(rand(rng, SHORT))
    elseif kind == :timestamplike       ; i -> i <= lo ? qesc("2026090112" * string(1000+rand(rng,0:8999))) : "NULL"
    elseif kind == :pooled_then_int     ; i -> i <= lo ? qesc(rand(rng, ("aa","bb","cc"))) : string(rand(rng,0:99))
    elseif kind == :float_then_str      ; i -> i <= lo ? string(rand(rng)) : qesc(rand(rng, SHORT))
    else error("bad kind $kind") end
end

const KINDS = [:int,:int_sentinel,:int_then_float,:int_then_str,:str_then_int,:int_str_int,
               :missing_then_int,:missing_then_str,:int_then_missing_then_str,
               :narrow_then_wide_str,:int_then_narrow_then_wide,:bool_then_str,
               :timestamplike,:pooled_then_int,:float_then_str]

# ragged: every `every`-th row carries `nextra` EXTRA fields past the header
function genragged(path, seed)
    rng = MersenneTwister(seed)
    ncols = rand(rng, 4:10); nrows = rand(rng, 60_000:150_000)
    nextra = rand(rng, 1:3); every = rand(rng, (3, 17, 101, 1009))
    kinds  = [rand(rng, KINDS) for _ = 1:ncols];  fns   = [patternfn(k, rng, nrows) for k in kinds]
    exkinds= [rand(rng, KINDS) for _ = 1:nextra]; exfns = [patternfn(k, rng, nrows) for k in exkinds]
    open(path, "w") do io
        println(io, join(["c$i" for i = 1:ncols], ","))
        for i = 1:nrows
            vals = [f(i) for f in fns]
            i % every == 0 && append!(vals, [f(i) for f in exfns])
            println(io, join(vals, ","))
        end
    end
    return (; seed, ncols, nrows, kinds=vcat(kinds, exkinds), every, nextra)
end
```
Read each generated file with `escapechar='\\', quotechar='\''` (plus
`silencewarnings=true` on 0.10 / `on_error=:collect` on 1.0), once in parallel
and once with `ntasks=1`, and compare.

## Results

| corpus | CSV v0.10.4 (Julia 1.8.5) | CSV v0.10.17 (Julia 1.12.6) | CSV v1.0.0 (Julia 1.12.6) |
|---|---|---|---|
| ragged, 110 seeds  | 17 / 40 failed | **14 / 110 failed** | **0 / 110 failed** |
| well-formed, 60 seeds | 0 / 60 | — | 0 / 60 |
| real `simplewiki` page dump (947,771 rows, 102 MB) | ok | ok | ok |

Failure = the parallel read threw, or its result differed from the serial read
in size, eltype or any value.

Failing v0.10.17 seeds: 16, 36, 39, 47, 50, 52, 66, 71, 72, 78, 87, 89, 100, 110.
Twelve throw the `checkpooled!` typeassert from the issue; seed 36 throws
`UndefVarError: T not defined in static parameter matching` from
`SentinelArrays.ChainedVector` — same root cause (un-promoted chunk column),
different landing spot.

## Why v1.0.0 cannot hit it
The 1.0 rewrite (#1196) deletes the machinery involved. `multithreadpostparse`,
`promotetostring!` and the per-task `Vector{Vector{Column}}` promotion do not
exist in `src/`. Type resolution is now one shared promotion state with a
convergence loop, and the column set is fixed by the header. Per `docs/src/migration.md`:

> a long row does not widen the schema; extra fields add a problem

So ingredient 1 is structurally impossible. The extra fields are reported
instead of silently widening the table:

```
CSV.Problem(long_row at data row 1009, column 0, byte 39156:
            "expected 4 fields, found 7 (extra fields ignored)")
```

This is a deliberate, documented behavior change, not a silent drop: on the
ragged corpus v0.10.x returns the widened column set, v1.0.0 returns the header
columns plus a `:long_row` problem per offending row (`CSV.problems(f)`).

## Data equivalence check
Ran the reporter's actual pipeline (`clean_page_line` → `page_lines.csv`) over a
real MediaWiki dump (`simplewiki-latest-page.sql`, 103 MB, 947,771 rows, 12 cols,
genuinely ragged: 38,914 over-long rows). SHA-256 per column:
all 12 columns identical across v0.10.17 serial, v0.10.17 parallel, v1.0.0 serial
and v1.0.0 parallel.

On the fuzz corpus I also compared v0.10.17-serial against v1.0.0-parallel column
by column. Every shared column matched except columns of kind `:int_sentinel`,
where **v0.10.17 is the one that is wrong**: it reads `-9223372036854775807` as
`+9223372036854775807` (26,006 / 26,006 occurrences in seed 36, sign dropped).
v1.0.0 reads it correctly. Almost certainly the Parsers 2.x sign-after-prefix
bug fixed in Parsers 3.

## Verdict
**Fixed in v1.0.0.** The issue can be closed as fixed by #1196. It is worth
noting in the close comment that 1.0 no longer widens the schema on long rows,
so the reporter's `page_lines.csv` now yields the header's columns plus
`:long_row` problems rather than extra columns — their `clean_page_line` split is
the real defect and should be fixed independently.
