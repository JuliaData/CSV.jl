# API reference

CSV.jl keeps its public namespace small. These names are not exported; use the
`CSV.` prefix.

```@docs
CSV
```

## Eager reading

```@docs
CSV.File
CSV.read
CSV.problems
```

## Indexed and incremental access

```@docs
CSV.lazy
CSV.LazyFile
CSV.Rows
CSV.Chunks
```

## Writing

```@docs
CSV.write
CSV.RowWriter
```

## Text values

Text values and mutable string columns come from [DataStrings.jl](https://github.com/JuliaData/DataStrings.jl). Use `DataStrings.DataString` and `DataStrings.StringVector` for their APIs.
