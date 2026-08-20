# Examples

These examples use in-memory inputs so they also serve as executable checks in
the documentation build.

## Read literal CSV text

A `String` source means a path or URL. Use `IOBuffer` for literal data.

```@example examples-literal
using CSV

text = "city,temp\nDenver,31.5\nBoston,25.0\n"
file = CSV.File(IOBuffer(text); stringtype=String)

collect(zip(file.city, file.temp))
```

## Supply or normalize column names

```@example examples-header
using CSV

without_header = CSV.File(IOBuffer("1,2\n3,4\n"); header=false)
manual_header = CSV.File(IOBuffer("1,2\n3,4\n"); header=[:left, :right])
normalized = CSV.File(IOBuffer("first value,2nd value\n1,2\n");
                      normalizenames=true)

(names(without_header), names(manual_header), names(normalized))
```

## Select columns and set types

```@example examples-select
using CSV

text = "id,amount,note\n1,10.5,first\n2,20.0,second\n"
file = CSV.File(IOBuffer(text);
                select=[:id, :amount],
                types=Dict(:id => Int32, :amount => Float64))

(names(file), eltype(file.id), collect(file.amount))
```

`select` and `drop` use list forms. A Boolean mask is also accepted:

```@example examples-select
file = CSV.File(IOBuffer(text); drop=[false, false, true])
names(file)
```

## Parse a custom dialect

```@example examples-dialect
using CSV

text = "name;amount\nAda;1,25\nGrace;2,50\n"
file = CSV.File(IOBuffer(text); delim=';', decimal=',', stringtype=String)

(collect(file.name), collect(file.amount))
```

## Treat repeated delimiters as one

```@example examples-repeated
using CSV

text = "left   right\n1      2\n3      4\n"
file = CSV.File(IOBuffer(text); delim=' ', ignorerepeated=true)

(collect(file.left), collect(file.right))
```

## Configure Boolean spellings

```@example examples-bool
using CSV

text = "id,active\n1,Y\n2,N\n"
file = CSV.File(IOBuffer(text);
                types=Dict(:active => Bool),
                truestrings=["Y"],
                falsestrings=["N"])

collect(file.active)
```

## Inspect bad values

```@example examples-problems
using CSV

text = "id,amount\n1,10\n2,not-a-number\n"
file = CSV.File(IOBuffer(text); types=Dict(:amount => Int))

[(problem.row, problem.col, problem.kind, problem.message)
 for problem in CSV.problems(file)]
```

Use `on_error=:error` when recovery is not acceptable:

```julia
CSV.File(IOBuffer(text); types=Dict(:amount => Int), on_error=:error)
```

## Keep empty text distinct from missing

```@example examples-empty
using CSV

table = (value=Union{Missing, String}[missing, "", "text"],)
output = IOBuffer()
CSV.write(output, table)
bytes = String(take!(output))
roundtrip = CSV.File(IOBuffer(bytes); stringtype=String)

(bytes, collect(roundtrip.value))
```

## Write without a temporary file

```@example examples-write
using CSV

table = (id=[1, 2], note=["plain", "comma, inside"])
output = IOBuffer()
CSV.write(output, table; newline="\r\n")
payload = take!(output)

copy(payload)
```

The returned byte vector can be sent to a network client or another Julia
parser.

## Read into another table package

`CSV.read` calls any Tables.jl sink. For example, with DataFrames.jl installed:

```julia
using CSV, DataFrames

df = CSV.read("input.csv", DataFrame)
CSV.write("output.csv", df)
```

## Process rows or batches

```@example examples-rows
using CSV

rows = CSV.Rows(IOBuffer("id,value\n1,10\n2,20\n"); types=[Int, Int])
total = sum(row[:value] for row in rows)
```

Use `CSV.Chunks` when a downstream operation accepts table partitions:

```@example examples-chunks
using CSV

chunks = CSV.Chunks(IOBuffer("id,value\n1,10\n2,20\n3,30\n"); ntasks=2)
length(collect(chunks))
```

## Index first and parse later

```@example examples-lazy
using CSV

lazyfile = CSV.lazy(IOBuffer("id,value\n1,10.5\n2,20.0\n"))
first_id = String(lazyfile.id[1])
eager = CSV.File(lazyfile; types=Dict(:value => Float64))

(first_id, collect(eager.value))
```

## Read several sources with provenance

```@example examples-sources
using CSV

inputs = [IOBuffer("id,value\n1,10\n"), IOBuffer("id,value\n2,20\n")]
file = CSV.File(inputs; source=:file => ["north", "south"])

(collect(file.id), collect(file.file))
```
