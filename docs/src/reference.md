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

```@docs
CSV.CompactString
```
