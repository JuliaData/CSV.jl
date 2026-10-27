# Writing data

`CSV.write(sink, table; keywords...)` writes any Tables.jl table to a file path
or `IO` and returns the sink.

```@example writing
using CSV

table = (name=["Ada", "Grace"], score=[9.5, missing])
output = IOBuffer()
CSV.write(output, table)
String(take!(output))
```

Column tables render in parallel on up to `ntasks` tasks (`Threads.nthreads()`
by default); the output is the same for every `ntasks` value. Row tables are
written sequentially without being collected.
The writer works in bounded blocks of rows, so its memory use does not grow
with the output size.

## Headers, appending, and sinks

The default writes the Tables.jl column names as the first row. Use
`writeheader=false` to omit them, or pass `header=[...]` with one name per
column to replace them. `header=true` and `header=false` also turn the header
on and off.

`append=true` opens a path for append instead of replacement. It omits the
header unless `writeheader=true` is explicit. An `IO` sink is written at its
current position, like `Base.write`; for an `IO`, `append` only decides
whether the header is written.

`CSV.write(sink, CSV.Chunks(source))` streams every batch of a chunked read
under one header without collecting the batches.

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
| `delim` | `','` | Output delimiter: one character or a string such as `"::"` |
| `quotechar` | `'"'` | One opening and closing quote byte |
| `openquotechar`, `closequotechar` | unset | Distinct opening and closing quote bytes |
| `escapechar` | closing quote | Escape a quote or escape byte in a field |
| `newline` | `'\n'` | Output row terminator |
| `quotestyle` | `:minimal` | Use `:minimal`, `:all`, or `:none` |

`:minimal` quotes text that contains a delimiter, quote, carriage return,
newline, or leading or trailing space. It also quotes an empty string.
`:all` quotes every text field. `:none` throws an `ArgumentError` for text that
contains a structural byte and for an empty string, which would read back as
`missing`.

Quote-related characters and `decimal` must be single ASCII characters, given
as a `Char` or a one-character string. A multi-byte delimiter is written
as-is, and a value containing its first byte is quoted. A newline can be a
character or string such as `"\r\n"`. An unknown keyword is an `ArgumentError`.

```@example writing-quotes
using CSV

output = IOBuffer()
CSV.write(output, (value=["plain", "with,comma", ""]); quotestyle=:minimal)
String(take!(output))
```

## Missing and empty text

`missingstring` controls the text written for `missing` and defaults to an
unquoted empty field. A present empty string is always quoted when quoting is
enabled. This matches how CSV.jl reads empty fields:

- an unquoted empty field is `missing`; and
- a quoted empty field is a present empty string.

Choose a non-empty `missingstring` if another system does not keep that
distinction.

## Numbers and dates

`floatformat` accepts a Printf-style format such as `"%.3f"`. Its default uses
Julia's shortest round-trip floating-point representation. `decimal` replaces
the decimal point in floating-point output.

Numbers are quoted when their text contains a delimiter, quote character, or
line break; `quotestyle=:none` rejects such values. When reading decimal-comma
output, configure the reader to use `decimal=','` as well.

`dateformat` accepts a Dates.jl format for all `Date`, `DateTime`, `Timestamp`, and `Time`
values. Other values use their ordinary text representation.

DataDecimals values keep their decimal spelling, and `decimal` applies to them
too: `decimal=','` writes `1.20` as `1,20`, quoted when the delimiter is also a
comma.

```@example writing-values
using CSV, Dates

output = IOBuffer()
CSV.write(output, (x=[1.23456], day=[Date(2026, 8, 19)]);
          floatformat="%.2f", dateformat="dd/mm/yyyy", delim=';')
String(take!(output))
```

## Gzip output

`compress=:auto` writes gzip when a path ends in `.gz` (in any letter case).
Use `compress=:gzip` (or `true`) to force gzip for any sink, or
`compress=:none` (or `false`) to disable it. Compression is streamed to the
sink. With more than one task, a column table's row blocks
compress in parallel, one gzip member each, so the file is a multi-member gzip
stream (RFC 1952). Readers such as `gunzip` and `CSV.File` concatenate the
members. A consumer that reads only one gzip member needs output written with
`ntasks=1`.

```julia
CSV.write("output.csv.gz", table)                  # detected from the suffix
CSV.write(io, table; compress=:gzip)               # explicit for an IO
```

## Partitioned output

`partition=true` writes each value from `Tables.partitions(table)` to a
separate sink, at most `ntasks` at a time. A vector of sinks must contain one
sink per partition. A base path generates paths by appending `_1`, `_2`, and
so on, and `CSV.write` returns them.

```julia
CSV.write(["part-1.csv", "part-2.csv"], partitioned_table; partition=true)
CSV.write("part.csv", partitioned_table; partition=true) # ["part.csv_1", "part.csv_2"]
```

## Other options

`quotestrings=true` is an alias for `quotestyle=:all`.

`transform=(column, value) -> new_value` transforms each cell before writing.
The callback uses a 1-based column index and runs in row-major order. A custom
transform disables parallel rendering so callback order stays stable.

`bufsize` is the largest allowed rendered row size in bytes and defaults to
4 MiB. Increase it when one output row is larger. It is a safety bound, not a
whole-file staging buffer.

Writing is streaming. If a later row fails, the sink contains the rows written
before the error and nothing of the failing row.

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
`CSV.write(io, table)` with the same options.
