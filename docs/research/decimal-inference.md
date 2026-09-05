# Decimal inference for the CSV.jl rewrite

Research checked against primary sources on 2026-09-05. This document separates verified product behavior from proposed CSV.jl behavior. The proposed options below are design sketches, not claims that those options already exist.

## Recommendation

Keep ordinary floating-point inference as the default for 1.0. Support explicit DataDecimals column types well. Explore an **opt-in fixed-scale inference policy** that reads the original text, chooses one scale for the entire column, and checks every value before returning a decimal column.

Consistent decimal places are a useful signal that an exporter intended fixed-scale values. They cannot establish that a column contains money. Prices, measurements rounded to two places, and percentage values can have identical text. Call the feature fixed-scale inference, not money detection. Let column names help a user choose columns; do not let English names silently change parsing semantics.

There is strong precedent for opt-in decimal preference and explicit schemas. I did not find a documented general-purpose CSV reader among the systems below that identifies money by a constant number of fractional digits. This is a bounded research finding, not proof that no such system exists.

## Verified industry behavior

| System | Verified behavior | Useful precedent |
| --- | --- | --- |
| DuckDB CSV | `DECIMAL` is an optional `auto_type_candidates` entry. It is absent from the default candidates. The sniffer tests whether values fit the supplied candidate types. | Let callers choose whether decimal participates in inference. |
| Spark CSV | `inferSchema` is off by default and requires an extra pass when enabled. The source attempts integer, long, decimal, then double. Its decimal path sends values with positive scale to double; decimal inference primarily covers large integral values. | Do not describe Spark CSV as having a fractional-decimal preference switch. |
| Spark JSON | `prefersDecimal=false` by default. Enabling it prefers decimals for fractional numbers and falls back to double if values do not fit. | A direct precedent for an explicit decimal-preference policy, in JSON. |
| Apache Arrow CSV | The documentation assigns `total_bill` and `tip` explicit `decimal128(10,2)` column types. The incremental reader freezes inferred types after its first block. | Known financial fields should use a schema; streaming requires a stable type contract. |
| Polars CSV | Schema inference examines `infer_schema_length` rows. Callers can increase that bound, scan all data, or use `schema_overrides`. `decimal_comma` controls numeric punctuation. | Separate sampling, schema overrides, and number syntax. A decimal separator option is not decimal-type preference. |
| pandas CSV | `dtype` and `converters` control conversion. `decimal` and `thousands` describe syntax. The documented Arrow-backed inference example still yields `double[pyarrow]` for fractional values. | Selecting an Arrow storage backend does not by itself select exact decimals. |
| Power Query | “Decimal number” means binary Float64; “Fixed decimal number”/`Currency.Type` is a distinct fixed-scale type. Unstructured-source detection normally samples the first 200 rows. | Terminology matters: a product saying “decimal” may mean binary floating point. |

Sources: [DuckDB auto detection](https://duckdb.org/docs/current/data/csv/auto_detection), [DuckDB CSV options](https://duckdb.org/docs/current/data/csv/overview), [DuckDB sniffer source](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/csv_scanner/sniffer/type_detection.cpp), [Spark CSV options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html), [Spark CSV inference source](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala), [Spark JSON options](https://spark.apache.org/docs/latest/sql-data-sources-json.html), [Arrow CSV guide](https://arrow.apache.org/docs/python/csv.html), [Polars CSV reference](https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html), [pandas CSV reference](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html), [pandas Arrow guide](https://pandas.pydata.org/docs/user_guide/pyarrow.html), [Power Query types](https://learn.microsoft.com/en-us/power-query/data-types).

Important DuckDB distinction: a bare `DECIMAL` means a concrete `DECIMAL(18,3)`, not an unconstrained decimal whose precision and scale are discovered from the file. The sniffer source reads width and scale from the candidate. This supports candidate testing, but it is not evidence of a scale-consistency heuristic. DuckDB also documents a cost increase for wide decimal arithmetic. These are design precedents, not performance measurements for Julia. [DuckDB numeric types](https://duckdb.org/docs/current/sql/data_types/numeric)

Schema metadata offers a stronger answer than heuristics. Frictionless Table Schema distinguishes numeric syntax using `decimalChar`, `groupChar`, and `bareNumber`. The last option explicitly permits stripping currency or percentage text. It does not infer a currency or define its arithmetic. For CSV.jl, currency symbols and percent scaling should likewise require an explicit policy. [Frictionless Table Schema](https://specs.frictionlessdata.io/table-schema/)

## Three distinct user needs

1. **Known schema:** “This amount is decimal with two places.” Parse directly to `DataDecimals.Decimal64{2}`. Precision and scale are supplied by the application or database schema.
2. **Fixed-scale discovery:** “Find numeric columns that look consistently formatted.” Inspect text and suggest or select a fixed-scale decimal type. This addresses the proposed money-like-column experience.
3. **Exact numeric ingestion:** “Preserve all finite base-10 values when they fit.” Select the maximum necessary scale even when formatting varies. This is a broader opt-in policy and should not be conflated with the fixed-scale heuristic.

A schema profiler can serve all three: report a proposed type, the evidence, and any counterexample. The caller can save the selected schema for future files. DuckDB's separate `sniff_csv` operation and its reusable read command provide a useful model. [DuckDB auto detection](https://duckdb.org/docs/current/data/csv/auto_detection)

## What a useful profiler should collect

Process decoded CSV field bytes before Float64 conversion. Respect the existing delimiter, quoting, whitespace, null, decimal-point, and grouping rules. Do not maintain a subtly different numeric grammar beside Parsers.

For each otherwise numeric column, collect bounded scalar statistics:

- Non-null count; integer-token count; fractional-token count.
- Minimum and maximum written fractional-digit counts, including trailing zeros.
- Maximum significant integral-digit count.
- Whether exponent notation, non-finite values, negative zero, currency text, or invalid syntax occurred.
- Whether more than one distinct value was seen, without retaining every value.
- First row/byte position that disproves each candidate, for useful diagnostics.

For plain positional tokens, choose `S = maximum fractional digits`. If `I` is the maximum significant integral-digit count, with zero for values whose magnitude is below one, a sufficient precision is `max(1, S, I + S)`. Check the final coefficients and the selected storage tier as well. Do not count a sign or integral leading zeros as precision.

Examples: `0.001` needs scale 3 and precision at least 3. `999` mixed with `0.01` needs precision at least 5 at scale 2. Merely taking the largest digit count of any individual input token is wrong for the second example.

If exponent notation is supported in the broad exact policy, incorporate the exponent when deriving scale and integral range. `1.20e-3` expands to `0.00120`: scale 5 retains the coefficient's trailing zero, while scale 4 suffices for its numeric value. `1e20` requires 21 integral digits. Bound exponent parsing and scale arithmetic before any power-of-ten operation. A token such as `1e999999999999999999` must not trigger enormous integer allocation.

## Candidate API designs

These spellings are proposals. Existing `decimal` syntax options should keep their meaning.

### A. Explicit types plus an inspection helper

Keep explicit `types` support for `DataDecimals.Decimal64{2}`. Add a profiling helper only after its result format is settled. A report could show:

```text
amount: proposed Decimal64{2}
12,482 non-null values; every fractional token has 2 places
no exponents; maximum integral digits 7; entire column checked
```

This is the smallest commitment for 1.0. The helper can initially live in a benchmark or experiment script. Do not promise a stable public profiling API before deciding how it handles streams and multiple sources.

### B. Opt-in consistent-scale inference

Possible spelling: `decimal_inference=:consistent`.

Only consider otherwise untyped numeric columns. Explicit `types` always wins. Require positional notation, a positive scale, consistent fractional-token scale, and a complete exact-fit check. Leave integer-only columns on the existing integer path.

For the first prototype, require the same positive number of places on **every non-null token**. A later relaxed rule could admit plain integers alongside consistently formatted fractional tokens. Keep these rules separate so users can understand why a column changed type.

Start with Decimal64 as the normal storage target, with scale at most 18 and enough remaining precision for the integral part. A scale cap of 4 can be explored as a stricter heuristic, but it is a product choice, not a universal money rule. Wider Decimal128/256 values are better handled by the explicit schema or broad exact policy.

No arbitrary confidence percentage should permit dropping a disagreeing value. Confidence controls whether to offer a suggestion. Exact representability controls whether conversion is allowed.

### C. Opt-in exact decimal preference

Possible spelling: `decimal_inference=:exact`.

Select a shared maximum scale, even for `1.2, 1.23, 4`. Permit wider storage up to the package limit of precision 76. Exponent support is a separate implementation choice. Treat NaN and infinity as disqualifying because fixed-point decimals cannot represent them.

Define the failure policy explicitly. A mode named “exact” should error or preserve strings when no exact bounded representation exists. Silently falling back to Float64 contradicts that name. If float fallback is desired, name the policy “prefer decimal” and document that it can lose decimal exactness.

Recommendation for this PR: ship dependency integration and explicit-type correctness; retain the current default; include a measured experiment for B before committing B or C as public 1.0 API.

## Sampling, late rows, and parallel reads

Sampling can propose a schema. It cannot prove the entire column fits. DuckDB samples 20,480 rows by default, with full-file sampling available; seekable inputs can sample different positions. Polars lets callers select the inference scan length. BigQuery uses up to the first 500 rows from a chosen file. Different sampling policies can produce different types from the same distribution. [DuckDB](https://duckdb.org/docs/current/data/csv/auto_detection), [Polars](https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html), [BigQuery](https://docs.cloud.google.com/bigquery/docs/schema-detect)

For a materialized `CSV.File`, the clean prototype is a complete profile followed by direct parsing into the chosen type. This avoids first building floats and reparsing them. An integrated single traversal can also work if it retains raw tokens or exact coefficients until the final scale is known. The extra complexity needs a measured benefit.

For chunks and row streams, freeze a schema before emitting typed values. A later `1.235` after a scale-2 sample must cause a documented failure or a caller-approved fallback. It cannot revise values that have already escaped. Non-seekable streams require buffering/spooling for full-file discovery; do not hide that memory or disk cost.

Combine worker statistics with order-independent operations such as maxima, minima, flags, and counts. Workers must not decide independent scales and silently concatenate different schemas. Test consistency across one task, many tasks, and changed chunk boundaries. Across multiple files, discover a common schema or apply one supplied schema.

## Exactness is separate from parse success

The local DataDecimals API intentionally rounds text parsing to the target scale. Its README explicitly shows `parse(Decimal64{2}, "1234.567") == 1234.57`. Therefore, **successful decimal parsing is not an exact-fit test**. An inference path must inspect excess fractional digits or use an exact coefficient check before calling the regular parser. `strict=true` alone cannot detect a value that parsed successfully after rounding. [Local DataDecimals README](/Users/jacob.quinn/Documents/Codex/2026-09-04/i-have-an-open-pr-for/releases/DataDecimals.jl/README.md:72)

Once a scale is chosen, extra fractional zeros are exactly representable numerically. `1.2300` fits scale 2 without numeric loss but does not preserve four-place formatting. Keep the written-scale consistency rule separate from the numeric exactness rule. Likewise, normalizing every token before profiling would erase the very trailing-zero signal the heuristic needs.

Do not infer from `Float64` values using multiplication, approximate-integer tests, or printed output. These operations discard the original trailing zeros and can hide prior rounding. A fixed-scale integer representation also cannot retain signed zero; negative-zero tokens should disqualify the conservative policy unless users explicitly accept that change.

## Adversarial examples and required outcomes

| Input values | Conservative consistent-scale result | Broader exact policy |
| --- | --- | --- |
| `12.30, 0.05, -7.10` | Decimal64 scale 2 candidate | Decimal scale 2 |
| `12, 0, -7` | Existing integer inference | Existing integer inference unless explicitly requested otherwise |
| `12, 0.05, -7.10` | Reject under strict formatting; optional relaxed rule can accept | Decimal scale 2 |
| `1.2, 1.23, 1.234` | Keep existing inference | Decimal scale 3 |
| `1.20, 1.2000` | Different written scales; reject | Scale 4 if written scale is retained |
| `1.20, 2.30, 1.235` late in file | Reject full-column candidate; never round the last value during discovery | Decimal scale 3 |
| `1e-3, 2e-3` | Keep existing inference | Decimal scale 3 if exponent support is enabled |
| `NaN, 1.20` or `Inf, 1.20` | Existing float inference | Explicit error/string policy |
| `-0.00, 1.20` | Keep float to preserve signed zero | Document normalization or reject |
| Nulls only, or one non-null value | Insufficient evidence for a heuristic suggestion | Follow existing empty-column/schema policy |
| Quoted `"1,23"`, delimiter `;` | Decimal candidate only with configured comma decimal point | Same grammar requirement |
| `"1,234"` without a known locale | Do not guess grouping versus decimal punctuation | Require syntax policy |
| `$12.30`, `12.30%`, `(12.30)` | Do not silently strip or reinterpret | Explicit conversion policy |
| 77 required significant digits | No bounded-decimal candidate | Explicit error/string policy |
| Very long zeros or huge exponent | Bounded, linear scan; no huge temporary integer | Same resource bound |

Quoted numeric fields need the same numeric decision as unquoted fields once CSV decoding is complete. Missing values must use the reader's configured null rules, including quoted-null handling. Duplicate headers, custom whitespace rules, and escaped numeric text must not create a separate inference grammar.

## Performance work to do before enabling a public policy

The lexical profiler can use bounded counters and flags. It does not require a BigInt per field. Fixed-width decimal columns can then store coefficients directly. This is an implementation hypothesis, not a measured speed claim.

Measure end-to-end time, bytes allocated, peak memory, compilation time, and downstream operations separately. Use narrow money columns, wide mixed tables, null-heavy columns, late counterexamples, Decimal128/256 bounds, compressed inputs, and shuffled records. Compare explicit decimal schema, ordinary float inference, full profiling plus decimal parse, and integrated profiling. Include both warm parsing and first-use compilation.

At least test summation, multiplication, division, mean, and joins/grouping downstream. Changing inferred type changes arithmetic behavior, not just display: this package's scale-2 division yields a scale-2 result, so `1.00 / 3.00` produces `0.33`. A column of rounded measurements may therefore be better left floating point even though its source text is exactly representable. [Local DataDecimals README](/Users/jacob.quinn/Documents/Codex/2026-09-04/i-have-an-open-pr-for/releases/DataDecimals.jl/README.md:47)

## Validation and limits of this research

Verified official documentation and relevant source code for the named systems. Read the local DataDecimals README to check precision tiers and parsing semantics. No external engine runtime benchmarks were run. No empirical false-positive rate for the proposed heuristic has been established. Recommendations about the CSV.jl API and profiler are design judgments; no package source code was changed for this research.
