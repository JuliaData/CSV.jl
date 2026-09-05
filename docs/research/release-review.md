# CSV 1.0 rewrite review — September 2026

This pass updates PR #1196 for shared data types and the released parser APIs.
CSV remains at 1.0.0-DEV. The PR remains a draft for maintainer review.

## Scope and baseline

The review started at `353ad5f05ab6728a21bdca30e8e8125aa3d515c9`.
Main was incorporated through `0d5699a74c67809a41d5a27b21876b8052251cd9`.
The original development checkouts were preserved. This report describes the
cleanup delta; the migration guide describes the complete rewrite from 0.10.

## Findings and changes

- **Shared string ownership.** Removed CSV's 410-line scalar/vector string
  implementation. DataStrings now owns values, buffer retention, and column
  editing. CSV retains only payload construction and rebasing glue. Regression
  tests hold old scalar values across column edits and exercise escaped,
  pooled, inline, long, missing, and overflow-buffer strings. Multi-source
  concatenation retains its existing owned-String behavior.
- **Released dependency APIs.** Require Parsers 3, InlineStrings 2, and Tables
  1.14. Remove their historical source pins and the optional Tables.Scan lane;
  every ordinary test job now runs the released Scan integration.
- **Opaque date formats.** Released Parsers DatePattern no longer exposes the
  draft `hasdate`/`hastime` fields. CSV records date/time classification from
  the format spelling when compiling it. Parsing still uses Parsers' public
  handle. Existing date, time, mixed-format, and custom-Bool tests cover this.
- **Resolved scan predicates.** Use Tables.resolve and evaluate the resolved
  BoundScan predicate. This preserves the selected column positions and names.
- **Exact decimal schemas.** Parse DataDecimals directly from field bytes.
  Reject excess nonzero scale and overflow instead of accepting rounded parse
  success. Extra trailing zeros are exact and remain valid. Support grouped,
  quoted, localized, scientific, missing, and runtime-scale DecimalValue input.
- **Optional decimal inference.** Add `inferdecimal=true` to materializing File
  and Chunks reads, including File(lazyfile), transpose, and filtered Scan.
  Profile the full selected row window before numeric conversion. Require at
  least two fractional values with equal written scales. Integers and missing
  values are neutral. Scientific notation, negative zero, nonnumeric data,
  inconsistent scale, and more than 76 required digits reject the candidate.
  Default float inference and explicit type precedence stay intact.
- **Bounded source scheduling.** Port main's multi-source scheduling intent
  into the rewritten reader. Share the task budget between outer source workers
  and each source's parser. Preserve source order and global diagnostics.
- **Writer compilation.** Keep small NamedTuple schemas typed through rendering
  and use a typed callable across the sink boundary. Separate the known serial
  path from task dispatch. The callback is a Function for Julia 1.10 `open`
  compatibility. Preserve the dynamic path for other table shapes.
- **Decimal output.** Honor the writer's decimal punctuation for DataDecimals
  values, including quoting when decimal and delimiter characters coincide.
- **Maintenance.** Refresh migration/reference docs, release notes, examples,
  package guidance, dependency setup, platform CI, and a real trim-compile job.

## Decisions and assumptions

Decimal inference is opt-in. Consistent scale is a formatting signal; it cannot
prove that a column contains money. Names and currency symbols do not drive
inference. Full-window validation avoids a late-row rounding surprise. Explicit
schemas remain the best choice for repeated ingestion with a known contract.
These choices were made for this review and remain reviewable API decisions.

CSV exports no new names. Users spell the shared types through DataStrings and
DataDecimals. The draft-only CSV.CompactString name was removed. Julia 1.10
remains the supported floor; the experimental trim job uses current Julia.

See [the inference research](decimal-inference.md) for primary sources. DuckDB
allows an opt-in DECIMAL CSV candidate with a supplied precision and scale.
Spark's `prefersDecimal` option is for JSON. Neither is a semantic money detector.

## Performance

Run `julia --project=test -t4 bench/release_review.jl new`. The script generates
30,000-row inputs, warms each case three times, and reports twelve measurements
with a GC before each. These are local microbenchmarks on arm64 macOS, Julia
1.12.6, four threads; they are not cross-platform throughput promises.

The baseline uses the original PR head and its exact review dependencies:
Parsers `e4adc5ba720e5668b726f65a574e2037c866d6df`, InlineStrings
`ce4c3549691c4b3443cc14ffa90ebdd6636eff2f`, Tables
`6be83de366499e7a428ecea1f6adab1c4f8eeff0`. Run the same script with `old` in that
separate environment. Data inputs and benchmark cases match in both runs.

| Case | Baseline median ms | Updated median ms | Baseline bytes | Updated bytes |
| --- | ---: | ---: | ---: | ---: |
| Numeric File | 0.516 | 0.534 | 1,929,600 | 1,930,032 |
| String File | 0.632 | 0.593 | 3,112,048 | 3,113,024 |
| Escaped strings | 1.102 | 1.074 | 12,373,984 | 12,374,688 |
| Pooled strings | 1.271 | 1.302 | 3,744,288 | 3,745,616 |
| Typed Rows | 5.803 | 4.801 | 9,008,912 | 9,008,912 |
| Serial NamedTuple write | 0.746 | 0.740 | 1,562,112 | 1,560,480 |

No material regression appeared in these default-path cases. Small differences
are sensitive to scheduling and noise. The Rows improvement also includes the
change from draft to released parser dependencies; it is not attributed solely
to CSV changes.

On the same numeric input, an explicit Decimal64 schema took **1.161 ms /
3,851,616 bytes**. Opt-in inference took **1.899 ms / 3,850,208 bytes**. For the
string input, enabling inference took **0.971 ms / 3,113,200 bytes**. Decimal
parsing costs more than Float64 here. The profile adds CPU work but no per-row
allocation. An initial nested iterator caused per-row allocations; replacing
its state with three integers reduced the inference case from 9.15 ms / 25.9 MB.
These measurements cover ingestion, not downstream decimal arithmetic.

## Validation and release gates

Validation covers the full checked-bounds suites on Julia 1.10 and current
Julia, deterministic fuzzing, required Tables.Scan tests, InlineStrings 2,
Aqua, strict Documenter/doctests, and DataFrames ownership and round trips.
The PR body records final suite counts and live CI status.

The trim suite compiles and runs the public serial CSV.write path for typed
NamedTuples containing integers, DataString values, decimals, and missing values.
It does not establish trim support for dynamically inferred CSV.File schemas.
Aqua's isolated persistent-task check remains disabled until the new packages
can resolve from General; the other Aqua checks run.

Before a CSV 1.0 release:

1. Finish DataStrings and DataDecimals registration, remove the two exact source
   pins in test/dependencies.jl, and verify a fresh registry-only resolution.
2. Release JSON with Parsers 3 compatibility and remove the docs-only JSON PR
   #480 pin. CSV has no runtime dependency on JSON.
3. Run reverse-dependency/PkgEval review beyond the DataFrames integration smoke
   tests. Coordinate required downstream compatibility updates.
4. Complete maintainer review, verify final CI and coverage policy, then change
   the version and register only the approved release commit.

The pending review revisions are DataStrings
`c5169652ef1942341c33fbb90a8d9dad3803ce29`, DataDecimals
`71da9bc9508d498ecdd1a8eed06b9888e5248c8b`, and docs-only JSON
`bcb8e334682e8135c08913781bf8200832cf752e`.
