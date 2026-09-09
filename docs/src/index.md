# CSV.jl

CSV.jl reads and writes comma-separated and other delimited text data. Its
readers and writer implement the [Tables.jl](https://github.com/JuliaData/Tables.jl)
interfaces, so CSV data can move between Julia table packages without a
CSV-specific adapter.

## Installation

Install the registered release in the Julia REPL:

```julia
] add CSV
```

CSV.jl 1.0 requires Julia 1.10 or later. Read [Migrating to 1.0](migration.md)
before you update an application from CSV.jl 0.10.

## First read and write

```@example home
using CSV, DataStrings

input = IOBuffer("name,score\nAda,9.5\nGrace,10.0\n")
file = CSV.File(input)

(names(file), String.(file.name), collect(file.score))
```

`CSV.File` is a Tables.jl table. A table sink can consume it directly. The
writer accepts any Tables.jl table:

```@example home
output = IOBuffer()
CSV.write(output, (name=["Ada", "Grace"], score=[9.5, 10.0]))
String(take!(output))
```

## Select a reader

| API | Use it when | Materialization model |
|:----|:------------|:----------------------|
| `CSV.File` | You need a normal in-memory table | Index once, then parse columns |
| `CSV.read` | You want to hand the parsed columns to a sink | Same parse as `CSV.File`; marks columns as safe to take |
| `CSV.lazy` | You need a fast first look or sparse cell access | Index now; parse each accessed cell later |
| `CSV.Rows` | You process rows once and do not need columns | Indexes the source; materializes cells on access |
| `CSV.Chunks` | You process a large table in bounded batches | Indexes once; parses one stable-schema batch at a time |

`CSV.lazy`, `CSV.Rows`, and `CSV.Chunks` do not stream an unbounded input.
They retain the source bytes and a structural index. See [Input and memory
behavior](reading.md#Input-and-memory-behavior) for the exact source rules.

## 1.0 data model

CSV.jl builds one quote-aware structural index. It then parses each selected
column with a type-specialized loop. Parallel execution does not change row
order, `limit` results, or output bytes.

Date-time columns prefer `Timestamp{Nanosecond}` from Durations.jl, which
uses `Dates.Timestamp` when the standard library provides it. Wider dates use microseconds if all values
fit exactly; otherwise the column stays text. Request `Dates.DateTime` with
`types` when a consumer needs it.

Text columns use `DataStrings.DataString` by default. Short values are stored in the
value; longer values live in buffers the column owns, so an eager table never
refers to its source. Convert with `String(value)` when a standalone `String` is
required, or pass `stringtype=String` to a reader.

Parse problems are structured data in 1.0. A read prints one summary warning
and keeps the problems; call `CSV.problems(file)` to inspect them, set
`on_error=:collect` to skip the warning, or `on_error=:error` when a parse
problem must stop the read with a `CSV.ParseError`.

```@contents
Pages = ["reading.md", "writing.md", "examples.md", "reference.md", "release-notes.md", "migration.md"]
Depth = 2
```
