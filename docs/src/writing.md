# Writing data

`CSV.write(sink, table; keywords...)` writes any Tables.jl table to a file path
or `IO`. It normally returns the sink. A partitioned string base path returns
the generated string path vector. A FilePathsBase path returns the original
path object.

```@example writing
using CSV

table = (name=["Ada", "Grace"], score=[9.5, missing])
output = IOBuffer()
CSV.write(output, table)
String(take!(output))
```

Column-access tables render rows in parallel when the table is large enough.
Output is ordered and byte-identical for every `ntasks` value. Row-access
sources stream sequentially without being collected. A custom `transform`
also runs sequentially to preserve its row-major callback order. The writer
emits bounded row blocks, so temporary rendering memory does not grow with the
total output size.

## Headers, appending, and sinks

The default writes the Tables.jl column names as the first row. Use
`writeheader=false` to omit them, or pass `header=[...]` to replace them.
The replacement header must contain one name for each source column.
`header=true` and `header=false` remain compatibility forms for controlling
header output.

`append=true` opens a path for append instead of replacement. It omits the
header unless `writeheader=true` is explicit. For a seekable `IO`,
`append=false` replaces its existing contents and truncates a stale suffix.

`bom=true` writes a UTF-8 byte-order mark before new output. It is not written
again during append.

```@example writing-header
using CSV

output = IOBuffer()
CSV.write(output, (a=[1, 2], b=[3, 4]); header=["left", "right"])
String(take!(output))
```

## Delimiters, quotes, and newlines

The writer dialect options are:

| Keyword | Default | Purpose |
|:--------|:--------|:--------|
| `delim` | `','` | One-byte output delimiter |
| `quotechar` | `'"'` | One opening and closing quote byte |
| `openquotechar`, `closequotechar` | unset | Distinct opening and closing quote bytes |
| `escapechar` | closing quote | Escape a quote or escape byte in a field |
| `newline` | `'\n'` | Output row terminator |
| `quotestyle` | `:minimal` | Use `:minimal`, `:all`, or `:none` |

`:minimal` quotes text that contains a delimiter, quote, carriage return,
newline, or leading or trailing space. It also quotes an empty string.
`:all` quotes every text field. `:none` rejects text that contains a structural
byte, and it rejects an empty string because that spelling would be ambiguous
with `missing`. It never writes data that the configured dialect would parse
incorrectly.

The delimiter and quote-related characters must be single ASCII bytes. A
newline can be a character or string such as `"\r\n"`.

```@example writing-quotes
using CSV

output = IOBuffer()
CSV.write(output, (value=["plain", "with,comma", ""]); quotestyle=:minimal)
String(take!(output))
```

## Missing and empty text

`missingstring` controls the text written for `missing` and defaults to an
unquoted empty field. A present empty string is always quoted when quoting is
enabled. This preserves the 1.0 read rule:

- an unquoted empty field is `missing`; and
- a quoted empty field is a present empty string.

Choose a non-empty `missingstring` if another system does not keep that
distinction.

## Numbers and dates

`floatformat` accepts a Printf-style format such as `"%.3f"`. Its default uses
Julia's shortest round-trip floating-point representation. `decimal` replaces
the decimal point in floating-point output.

`dateformat` accepts a Dates.jl format for all `Date`, `DateTime`, and `Time`
values. Other values use their ordinary text representation.

```@example writing-values
using CSV, Dates

output = IOBuffer()
CSV.write(output, (x=[1.23456], day=[Date(2026, 8, 19)]);
          floatformat="%.2f", dateformat="dd/mm/yyyy", delim=';')
String(take!(output))
```

## Gzip output

`compress=:auto` writes gzip when a path ends in `.gz`. Use `compress=:gzip`
to force gzip for any sink, or `compress=:none` to disable it. The compatibility
forms `compress=true` and `compress=false` mean `:gzip` and `:none`.
Compression is streamed to the sink.

```julia
CSV.write("output.csv.gz", table)                  # detected from the suffix
CSV.write(io, table; compress=:gzip)               # explicit for an IO
```

## Partitioned output

`partition=true` writes each value from `Tables.partitions(table)` to a
separate sink. A vector of sinks must contain one sink per partition. A base
path generates paths by appending `_1`, `_2`, and so on. Partitions write
concurrently. The partition iterator is consumed once and is not collected.
At most `ntasks` partition writes are active at one time.

```julia
sinks = ["part-1.csv", "part-2.csv"]
CSV.write(sinks, partitioned_table; partition=true)
generated = CSV.write("part.csv", partitioned_table; partition=true)
```

The string-base form returns the generated path vector. The explicit-vector
form returns the supplied vector.

## Compatibility controls

`quotestrings=true` remains an alias for `quotestyle=:all`. New code should use
`quotestyle` because it also expresses `:minimal` and `:none`.

`transform=(column, value) -> new_value` transforms each cell before writing.
The callback uses a 1-based column index and runs in row-major order. A custom
transform disables parallel rendering so callback order stays stable.

`bufsize` is the largest allowed rendered row size in bytes and defaults to
4 MiB. Increase it when one output row is larger. It is a safety bound, not a
whole-file staging buffer.

Writing is streaming. If a later row fails, the sink can contain the valid
prefix written before the error. A replacement write to a seekable sink still
removes any stale suffix from its old contents.

## RowWriter

`CSV.RowWriter(table; keywords...)` is an iterator of complete CSV-formatted
row strings. It yields the header first unless `writeheader=false`. It accepts
the same dialect, value-format, and BOM options as `CSV.write`.

```@example row-writer
using CSV

rows = collect(CSV.RowWriter((id=[1, 2], label=["a", "b"])))
join(rows)
```

Rows render on demand. `join(CSV.RowWriter(table))` is byte-identical to
`CSV.write(io, table)` with the same row-writer options.
