# Decimal columns

Use a decimal schema when the data contract specifies exact decimal values.
CSV parses DataDecimals values directly from field bytes. It honors `decimal`,
`groupmark`, missing-value spellings, quotes, and surrounding numeric whitespace.

```@example decimals
using CSV, DataDecimals

Money = DataDecimals.Decimal64{2}
file = CSV.File(IOBuffer("amount\n12.30\n4.56\n"); types=Dict(:amount => Money))
(eltype(file.amount), sum(file.amount))
```

An explicit fixed-scale schema must fit exactly. `1.2300` fits scale 2;
`1.235` does not. CSV records an invalid-value problem and returns `missing`
for a value that needs rounding or exceeds precision. `strict=true` throws.
This is stricter than DataDecimals' standalone rounded parsing. Read the text
and round explicitly when that is the intended contract. Explicit schemas also
work with `CSV.Rows`, `CSV.lazy`, `CSV.Chunks`, and transposed input.

## Optional inference

Normal inference still chooses Float64 for fractional numbers. Set
`inferdecimal=true` on `CSV.File` or `CSV.Chunks` to look for consistent-scale
columns before value conversion:

```@example decimals
text = "amount,units\n12.30,1\n4.56,2\n7,3\n"
file = CSV.File(IOBuffer(text); inferdecimal=true)
(eltype(file.amount), eltype(file.units))
```

The rule is deliberately narrow:

- At least two present values must have fractional digits.
- Every fractional value must have the same number of written fractional digits.
  Trailing zeros count: `1.20` and `2.30` agree; `1.2` and `2.30` do not.
- Integers and missing values are neutral. Quoted numeric fields are supported.
- Scientific notation, negative zero, NaN, infinity, and nonnumeric content
  reject the decimal candidate. A rejected candidate follows ordinary inference.
- Precision must fit DataDecimals' maximum of 76 digits. The parser selects
  Decimal64, Decimal128, or Decimal256 storage. Ordinary money values generally
  use Decimal64. Leading integer zeros do not increase required precision.
- Explicit `types` take precedence. `typemap` can map an inferred decimal type.

CSV examines the complete selected row window, regardless of `nsample`.
A late value cannot be rounded to a scale chosen from an earlier sample.
`limit`, `skipto`, and `footerskip` constrain that window. With Tables.Scan,
the output profile examines only rows that pass the filter and row bounds.
Chunks uses one profile across its complete window so every batch has the same
schema. Transposed File reads profile each transposed column.

Rows and lazy reads retain their normal no-inference contract. Supply decimal
types explicitly there. `CSV.File(lazyfile; inferdecimal=true)` can reuse the
index and infer the materialized columns.

The extra pass has a cost. Use an explicit schema for repeated ingestion when
the scale is known. Decimal arithmetic can also cost more than Float64. This
option selects a representation; it does not promise faster arithmetic.

## What the rule means

Consistent scale is a formatting signal. It cannot distinguish money from a
temperature rounded to two decimal places. Names such as `amount`, `price`, or
`cost` are not part of the rule. Currency symbols and accounting parentheses
need explicit preprocessing; CSV does not infer a currency or exchange rate.
Specify locale punctuation with `decimal` and `groupmark` rather than guessing it.

There is precedent for opt-in decimal preference. DuckDB permits DECIMAL in its
[CSV inference candidates](https://duckdb.org/docs/stable/data/csv/auto_detection.html),
although it tests a supplied precision and scale. Spark provides
[`prefersDecimal` for JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html).
Neither establishes that fixed decimal places mean money. The
[research note](https://github.com/JuliaData/CSV.jl/blob/codex/kernel-proveout-review/docs/research/decimal-inference.md)
compares these approaches and records design alternatives.
