# CSV.jl

CSV.jl reads and writes comma-separated and other delimited text data. Its
readers and writer implement the [Tables.jl](https://github.com/JuliaData/Tables.jl)
interfaces, so they work with any Julia table package.

## Installation

Install the registered release in the Julia REPL:

```julia
] add CSV
```

CSV.jl 1.0 requires Julia 1.10 or later. Read [Migrating to 1.0](migration.md)
before you update an application from CSV.jl 0.10.

## First read and write

```@example home
using CSV

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

| API | Use it when | What it does |
|:----|:------------|:-------------|
| `CSV.File` | You need a normal in-memory table | Parses every selected column |
| `CSV.read` | You want the columns in another table type | Parses like `CSV.File`, then lets the sink take the columns without copying |
| `CSV.lazy` | You need a fast first look or sparse cell access | Indexes the input; parses each cell when it is accessed |
| `CSV.Rows` | You process rows once and do not need columns | Indexes the input; parses cells on access |
| `CSV.Chunks` | You process a large table in batches | Parses one batch at a time; every batch has the same column types |

`CSV.lazy`, `CSV.Rows`, and `CSV.Chunks` keep the whole input in memory (or
memory-mapped) while you use them, so none of them streams an unbounded input.
`CSV.lazy` and `CSV.Rows` also keep an index of every row; `CSV.Chunks` keeps
one batch's. See [Input and memory
behavior](reading.md#Input-and-memory-behavior) for details. Results do not
depend on the number of threads.

## Data model

Date-time columns are `Timestamp{Nanosecond}` from Durations.jl (the
`Dates.Timestamp` type where the standard library provides it). Wider dates
use microseconds if all values fit exactly; otherwise the column stays text.
Request `Dates.DateTime` with `types` when a consumer needs it.

Text columns use `DataStrings.DataString` by default. Short values are stored in the
value; longer values live in buffers the column owns, so an eager table never
refers to its source. Convert with `String(value)` when a standalone `String` is
required, or pass `stringtype=String` to a reader.

Parse problems are structured data. A read prints one summary warning
and keeps the problems; call `CSV.problems(file)` to inspect them, set
`on_error=:collect` to skip the warning, or `on_error=:error` when a parse
problem must stop the read with a `CSV.ParseError`.

```@contents
Pages = ["reading.md", "writing.md", "examples.md", "reference.md", "release-notes.md", "migration.md"]
Depth = 2
```
