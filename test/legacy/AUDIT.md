# The 0.10 test-suite audit

*What the old suite was, what survived, what was dropped, and why. 2026-08-15.*

## The old suite in numbers

| file | lines | @test | shape |
|---|---:|---:|---|
| `basics.jl` | 933 | 362 | issue graveyard: `f = CSV.File(IOBuffer("…")); @test …` × ~200, keyed by issue number |
| `testfiles.jl` | 728 | 6 (+110 table rows) | a data table `(file, kwargs, size, schema, expected)` over the artifact corpus |
| `runtests.jl` | 530 | 203 | CSV.Rows / Chunks / select-drop / vector-of-files / chaincolumns! / detect |
| `iteration.jl` | 203 | 184 | row iteration on 2 corpus files (`row.A`, tuples of accessors) |
| `write.jl` | 404 | 43 | CSV.write / RowWriter / writebom / partition |
| `perf_write.jl` | 37 | 0 | a benchmark script, not a test |
| **corpus artifact** | 119 files, 28 MB | | 108 referenced; 82 of those ≤4 KB (12 KB total); 19 real-world dumps >64 KB (27 MB) |

## Verdicts

### 1. `testfiles.jl` — KEPT as data, driven differently (highest value, ~zero rewrite)

It was already a table. It stays a table (`corpus_table.jl`, verbatim) and is
driven by `harness.jl`'s `corpuscase`, which asserts (a) **agreement with the
0.10 implementation** on names/size/values — the oracle catches everything —
and (b) the table's **own literal expectations** (schema class + values) as an
oracle-independent pin. 110 entries → 190 agreeing replays.

Old string-type schemas (`InlineString15`, `String3`, `PosLenString`) are
aliased to `String` for the type-class comparison only.

### 2. `basics.jl` — REPLAYED, not transcribed

Every `CSV.File(...)` call was extracted by the parser (`JuliaSyntax`), 237 calls
total. Self-contained ones (literal/corpus input, no legacy-only names) became
`agree(...)` replays in `cases_file.jl` — the *input* is the test and the *oracle*
supplies the expectation. That is cheaper to maintain than 362 hand-written
value assertions and strictly stronger (every column, every row, both
implementations). Free-variable calls, `@test_logs`, and retired forms are
listed as `# MANUAL` in the same file for hand triage. This
round moved the self-contained custom-type cases and both `IdDict` typemap cases
out of that queue and into the replay.

The harness now compares row counts and normalized schema types as well as names
and values. Two thrown calls agree only when their semantic error categories
agree. Every delta pin states its exact expected outcome (`differ`, `new_errors`,
or `old_errors`), so a reversed error direction also fails. The current ledger
is 212 `agree`, 17 `both_error`, 4 `differ`, 2 `new_errors`, and 8 `old_errors`,
with no `unportable` outcome.

### 3. What the replay FOUND (the audit's real yield)

Real defects, all fixed in this pass:

- **quotes inside comment lines poisoned quote parity** — a `"` in a
  `# comment` line opened a quote for the rest of the file (scanner tape is
  comment-blind by design). Fix: comment dialects take the scalar scanner with
  row-start comment skipping; `nextrowstart` (header/skipto arithmetic) skips
  comment rows; the chunk planner walks true row starts for comment files.
- **delimiter sniffing**: header-only delimiter false positives (`Created
  Date` → split on space) and header-vs-data disagreements (`A;B;C` over
  `1,1,10`). Fix: field-consistency scorer first, then 0.10's exact byte-count
  tiers as fallback. A deterministic 256-sample differential fuzz covers
  quotes, spaces, all six candidate delimiters, CRLF, and multiple delimiter
  bytes inside quoted cells. Header-only, empty, BOM, and tie cases are pinned.
- **typed values with surrounding blanks** (`1, 2, 3`) parsed as strings.
  Fix: typed parse + detection trim blanks (0.10/Parsers semantics); String
  columns keep their spaces unless `stripwhitespace`.
- **`header=Symbol[]`** returned no columns (0.10: generate `ColumnN`).
- **transpose**: `header=N`, `skipto`, `header=[names]`, empty names →
  `ColumnN`, blank tolerance — all missing; implemented (0.10 parity incl. the
  #1172 promotion case).
- **narrow numeric user types** (`Int8/16/32`, `UInt*`, `Float16/32`) rejected;
  now parse via the native type and narrow at the door (out-of-range → missing
  + problem).
- **Regex keys** in `types`/`dateformat`/`pool` Dicts; **`codeunits`/`SubArray`
  byte sources**; **`skipto=1` ⇒ no header** (0.10 rule); **non-consecutive
  header rows** (`header=[1,3,5]`).
- **eltype precision**: a user-declared `Union{Missing,T}` is now the column
  type even when no missing appears (0.10 honors the declaration); and the type
  sample no longer looks past `limit`/footer rows (a skipped footer's missings
  were seeding missing-capable finals).
- **custom scalar types**: concrete user types with `tryparse(T, String)` or
  `parse(T, String)` now use the typed column path. Unsupported custom types
  still fail at configuration time, as in 0.10.

The comment scanner count reduction is intentional and exact. Forty-eight
randomized comment cases each lost 14 unsafe fast-scanner variants (672 checks).
Ten explicit comment cases each lost 18 variants (180 checks). Total: 852.
Scalar sequential and parallel geometry remains covered at chunk sizes 3, 7,
16, and 64. Focused row-start and quote-poison checks increased the current
kernel total beyond the original 54,488.
- Two Julia traps hit twice each in this pass and now documented in code:
  `cond && (a, b = c, d)` parses as a tuple (silent no-op), and `begin…end`
  inside a typed comprehension is a parse error.

### 4. Pinned 1.0 deltas surfaced by the replay (the migration guide's spine)

The harness asserts these DISAGREE with 0.10 (a stale pin fails):

- empty unquoted cell is always `missing`; `missingstring` only adds spellings
- Bool columns are strictly `true`/`false` unless `truestrings`/`falsestrings`
- long rows do not widen the schema (extra fields → problem)
- unclosed quote is a reported problem, not a fatal error
- NUL is an accepted delimiter byte
- retired: `PosLenString`, function-typed `types`/`select`/`drop`/`pool`,
  `debug`, `silencewarnings`, …
- writer: quoted `""` is a present empty string; unquoted empty is missing

### 5. `runtests.jl`, `iteration.jl`, `write.jl` — PARTIALLY superseded, rest queued

- CSV.Rows/Chunks/select/drop/vector-of-files: the new `test/api.jl` covers
  Rows/Chunks/select/drop differentially. **Vector-of-files input (+ `source=`
  column) is a real feature gap** found here — queued, not silently dropped.
- `iteration.jl`: row-accessor iteration is covered by `api.jl`'s Rows tests;
  the two corpus files it used are in the table. Dropped as redundant.
- `write.jl`: `test/write.jl` (77 tests, parser-oracle fuzz) supersedes it;
  RowWriter/writebom/`writerow` surface — queued for parity review.
- `perf_write.jl`: a benchmark, not a test → `bench/`.

### 6. The corpus artifact — keep, but shrink the dependency

- 82 referenced files are ≤4 KB (12 KB total): candidates to inline as string
  literals next to their assertions (self-describing tests, no artifact needed
  for the common path). Not done in this pass — the table drives them by path
  and works; inlining is mechanical follow-up.
- 19 real-world dumps (27 MB): each keeps only if it pins something synthetic
  input cannot — `pandas_zeros` (10 MB of a degenerate shape), `Fielding`,
  `FL_insurance` (wide real data), `randoms.csv.gz` (gzip + promotions),
  `escape_row_starts`, the multithreaded-row-start-detection files. Candidates
  to DROP: `precompile.csv` (295 KB, 0 refs), the three utf16 files (0 refs —
  utf16 is out of scope), 8 other 0-ref files.
- A lighter equivalent for most of the corpus: the table's kwargs × a
  generator. Most rows test one dialect knob on a tiny file; a parametrized
  generator (`bench/bench_matrix.jl` already has 22 shape generators) plus the
  oracle covers the same space with no artifact. Proposed, not built.

## Codex round 22 (the port's review) — CLEAN, with hardening

Four commits on top of the port (`c204119` `42abd70` `3773b5e` `6915e2a`):
comment skipping proven at row starts only; the kernel's 852-assertion drop
accounted for exactly (672 randomized + 180 explicit fast-scanner variants
that comment dialects no longer take — scalar sequential and parallel
geometry still covered at 3/7/16/64 bytes); delimiter fallbacks fixed for
single-row headers, CRLF evidence, BOM, ties, header precedence, and a
256-sample fuzz vs the 0.10 detector; sentinels with blanks; `limit` scoping
of inference; narrow-type/select mapping; declared-Union widening through
every container; **custom scalar types with `parse`/`tryparse` now work**
(ported from the manual queue). The harness now compares names, row counts,
schema types, values, AND semantic error categories (both-error must agree on
category), and pins assert their exact direction. Final ledger: 212 agree,
17 both-error, 8 + 4 + 2 pinned; battery 1,175/1,175.

## Layout

```
test/legacy/
  harness.jl        agree() / corpuscase() / outcome ledger; LegacyCSV = oracle
  corpus_table.jl   testfiles.jl's table, verbatim
  cases_file.jl     GENERATED replays of basics/runtests/write/iteration File calls
  runtests.jl       runs both, prints the outcome ledger
  AUDIT.md          this file
legacy/{src,test}   the frozen 0.10 sources + original tests (loadable as LegacyCSV)
```

Regenerate `cases_file.jl` with the extractor scripts if `legacy/test` ever
changes (they should not; the legacy tree is frozen).
