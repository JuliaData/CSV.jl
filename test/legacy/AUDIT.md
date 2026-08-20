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
oracle-independent pin. Its 110 entries each replay once, with additional
literal schema and value assertions.

Old string-type schemas (`InlineString15`, `String3`, `PosLenString`) are
aliased to `String` for the type-class comparison only.

### 2. `basics.jl` — REPLAYED, not transcribed

Every `CSV.File(...)` call was extracted by the parser (`JuliaSyntax`), 237 calls
total. Self-contained ones (literal/corpus input, no legacy-only names) became
`agree(...)` replays in `cases_file.jl` — the *input* is the test and the *oracle*
supplies the expectation. That is cheaper to maintain than 362 hand-written
value assertions and strictly stronger (every column, every row, both
implementations). Free-variable calls, `@test_logs`, and retired forms were
listed as `# MANUAL` in the same file for hand triage. **The manual queue is
now empty**: every one of the 71 remaining entries is either a replay (with
its missing context — seeded data, loop variables, thunk-built sources —
inlined) or a pinned delta with its direction asserted. `agree()` gained a
thunk input form for sources `seekstart` cannot reset (an IO whose position
matters, vectors of one-shot IOs).

The harness now compares row counts and normalized schema types as well as names
and values. Two thrown calls agree only when their semantic error categories
agree. Every delta pin states its exact expected outcome (`differ`, `new_errors`,
or `old_errors`), so a reversed error direction also fails. The current ledger
is 351 `agree`, 19 `both_error`, 14 `differ` (all pinned), 9 `new_errors` (all
pinned), and 8 `old_errors` (all pinned), with no `unportable` outcome and no
unqueued entry: 401 unique outcomes and 1,447/1,447 passing battery assertions.
Duplicate or empty outcome labels are fatal, so one case cannot silently replace
another case in the ledger.

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

The manual-queue burndown found three more, all fixed in that pass:

- **the delimiter sniff sample could come up empty** when a single row is
  wider than `samplebytes` (64 KB default; a 60,000-column space-delimited
  file) — detection then collapsed to one column. The sample now grows until
  it holds at least one complete row.
- **`transpose=true` did not accept `limit`** (0.10 does: first N transposed
  rows).
- **`validate=false` was wrongly retired** (and a `Regex` key matching no
  column was silently ignored even with the default `validate=true`). It is
  now a real keyword on `File`/`Rows`/`Chunks`: keys of `types`/`dateformat`/
  `pool` naming absent columns error by default and are ignored under
  `validate=false`, exactly as 0.10.

The comment scanner count reduction is intentional and exact. Forty-eight
randomized comment cases each lost 14 unsafe fast-scanner variants (672 checks).
Ten explicit comment cases each lost 18 variants (180 checks). Total: 852.
Scalar sequential and parallel geometry remains covered at chunk sizes 3, 7,
16, and 64. Focused row-start and quote-poison checks increased the current
kernel total beyond the original 54,488.

### 4. Pinned 1.0 deltas surfaced by the replay (the migration guide's spine)

The harness asserts these DISAGREE with 0.10 (a stale pin fails):

- empty unquoted cell is always `missing`; `missingstring` only adds spellings
- Bool columns are strictly `true`/`false` unless `truestrings`/`falsestrings`
- long rows do not widen the schema (extra fields → problem) — including the
  #1021 family, where 0.10 grew columns that only user `types` mentioned; a
  `types` VECTOR must now match the header's column count
- unclosed quote is a reported problem, not a fatal error
- NUL is an accepted delimiter byte
- retired: `PosLenString`, function-typed `types`/`select`/`drop`/`pool`,
  `debug`, `silencewarnings`, the 0.10-deprecated `type=` singular, …
- writer: quoted `""` is a present empty string; unquoted empty is missing
- multithreaded parsing of quoted multiline fields keeps exact column types
  (0.10's chunk-boundary speculation degraded such columns to `String`)
- multi-source `File`: the schema reports the true concatenated column types
  (0.10 returned the first source's pre-promotion types, disagreeing with its
  own columns), and `source=` labels for non-path sources are deterministic
  `"<source i>"` strings (0.10 embedded the IO object hash)

### 5. `runtests.jl`, `iteration.jl`, `write.jl` — superseded or replayed

- CSV.Rows/Chunks/select/drop/vector-of-files: the new `test/api.jl` covers
  Rows/Chunks/select/drop differentially. **Vector-of-files input (+ `source=`
  column) — the feature gap found here — is now BUILT** (first source's
  column set, by-name matching with missing-fill, promotion across sources,
  pooled provenance column) and its nine queue entries replay.
- `iteration.jl`: row-accessor iteration is covered by `api.jl`'s Rows tests;
  its two corpus files replay as `iteration:3-*`.
- `write.jl`: `test/write.jl` (including parser-oracle fuzz) supersedes it;
  the three File-side reads (tab dialect, control-char delims, FilePathsBase
  path — now an extension) replay as `write:*` cases.
- `perf_write.jl`: a benchmark, not a test → `bench/`.

### 6. The corpus artifact — retained lazily for large fixtures

- 82 referenced files at or below 4 KiB are exact byte literals in
  `corpus_inline.jl`. The harness writes them to one scratch directory so path
  behavior remains covered.
- The 24 retained real-world files (18 MB unpacked) come from the lazy
  `testfiles` artifact. Its exact file list and content hashes are pinned in
  `test/artifacts/testfiles.sha256`.
  They are byte-identical to the former in-repository
  `test/legacy/testfiles/` subset.
- `generated.jl` reuses the 21 benchmark shape generators at three sizes.
  It runs 63 differential cases and 63 writer round-trips. The sizes cross
  chunk boundaries without adding generated fixture files to the repository.
- `test/Artifacts.toml` keeps the large corpus test-only, so package users do
  not download it. `test/README.md` documents the fixture update process.

## Layout

```
test/legacy/
  harness.jl        agree() / corpuscase() / outcome ledger; LegacyCSV = oracle
  corpus_table.jl   testfiles.jl's table, verbatim
  cases_file.jl     GENERATED replays of basics/runtests/write/iteration File calls
  runtests.jl       runs both, prints the outcome ledger
  AUDIT.md          this file
test/Artifacts.toml lazy 24-file real-world corpus
test/README.md      fixture layout and artifact-update notes
test/LegacyCSV/src  direct-include shim that loads the frozen oracle as LegacyCSV
legacy/{src,test}   the frozen 0.10 sources + original tests
```

Regenerate `cases_file.jl` with the extractor scripts if `legacy/test` ever
changes (they should not; the legacy tree is frozen).
