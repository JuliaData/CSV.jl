module CSV

using DataStreams, Parsers, Missings, CategoricalArrays, DataFrames, WeakRefStrings
using Mmap, Dates

const RETURN  = UInt8('\r')
const NEWLINE = UInt8('\n')
const COMMA   = UInt8(',')
const QUOTE   = UInt8('"')
const ESCAPE  = UInt8('\\')
const PERIOD  = UInt8('.')
const SPACE   = UInt8(' ')
const TAB     = UInt8('\t')
const MINUS   = UInt8('-')
const PLUS    = UInt8('+')
const NEG_ONE = UInt8('0')-UInt8(1)
const ZERO    = UInt8('0')
const TEN     = UInt8('9')+UInt8(1)
Base.isascii(c::UInt8) = c < 0x80

substitute(::Type{Union{T, Missing}}, ::Type{T1}) where {T, T1} = Union{T1, Missing}
substitute(::Type{T}, ::Type{T1}) where {T, T1} = T1
substitute(::Type{Missing}, ::Type{T1}) where {T1} = Missing

"""
Represents the various configuration settings for delimited text file parsing.

Keyword Arguments:

 * `delim::Union{Char,UInt8}`: how fields in the file are delimited; default `','`
 * `quotechar::Union{Char,UInt8}`: the character that indicates a quoted field that may contain the `delim` or newlines; default `'"'`
 * `escapechar::Union{Char,UInt8}`: the character that escapes a `quotechar` in a quoted field; default `'\\'`
 * `missingstring::String`: indicates how missing values are represented in the dataset; default `""`
 * `dateformat::Union{AbstractString,Dates.DateFormat}`: how dates/datetimes are represented in the dataset; default `Base.Dates.ISODateTimeFormat`
 * `decimal::Union{Char,UInt8}`: character to recognize as the decimal point in a float number, e.g. `3.14` or `3,14`; default `'.'`
 * `truestring`: string to represent `true::Bool` values in a csv file; default `"true"`. Note that `truestring` and `falsestring` cannot start with the same character.
 * `falsestring`: string to represent `false::Bool` values in a csv file; default `"false"`
 * `internstrings`: whether strings should be interned, rather than creating a new object for each string field; default `true`
"""
struct Options{D}
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    missingstring::Vector{UInt8}
    null::Union{Vector{UInt8},Nothing} # deprecated
    missingcheck::Bool
    dateformat::D
    decimal::UInt8
    truestring::Vector{UInt8}
    falsestring::Vector{UInt8}
    internstrings::Bool
    # non-public for now
    datarow::Int
    rows::Int
    header::Union{Integer,UnitRange{Int},Vector}
    types
end

Options(;delim=COMMA, quotechar=QUOTE, escapechar=ESCAPE, missingstring="", null=nothing, dateformat=nothing, decimal=PERIOD, truestring="true", falsestring="false", internstrings=true, datarow=-1, rows=0, header=1, types=Type[]) =
    Options(delim%UInt8, quotechar%UInt8, escapechar%UInt8,
            map(UInt8, collect(ascii(String(missingstring)))), null === nothing ? nothing : map(UInt8, collect(ascii(String(null)))), missingstring != "" || (null != "" && null != nothing),
            isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat,
            decimal%UInt8, map(UInt8, collect(truestring)), map(UInt8, collect(falsestring)), internstrings, datarow, rows, header, types)
function Base.show(io::IO,op::Options)
    println(io, "    CSV.Options:")
    println(io, "        delim: '", Char(op.delim), "'")
    println(io, "        quotechar: '", Char(op.quotechar), "'")
    print(io, "        escapechar: '"); escape_string(io, string(Char(op.escapechar)), "\\"); println(io, "'")
    print(io, "        missingstring: \""); escape_string(io, isempty(op.missingstring) ? "" : String(collect(op.missingstring)), "\\"); println(io, "\"")
    println(io, "        dateformat: ", op.dateformat == nothing ? "" : op.dateformat)
    println(io, "        decimal: '", Char(op.decimal), "'")
    println(io, "        truestring: '$(String(op.truestring))'")
    println(io, "        falsestring: '$(String(op.falsestring))'")
    print(io, "        internstrings: $(op.internstrings)")
end

"""
A type that satisfies the `Data.Source` interface in the `DataStreams.jl` package.

A `CSV.Source` can be manually constructed in order to be re-used multiple times.

`CSV.Source(file_or_io; kwargs...) => CSV.Source`

Note that a filename string can be provided or any `IO` type. For the full list of supported
keyword arguments, see the docs for [`CSV.read`](@ref) or type `?CSV.read` at the repl

An example of re-using a `CSV.Source` is:
```julia
# manually construct a `CSV.Source` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.Source(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
mutable struct Source{P, I, DF} <: Data.Source
    schema::Data.Schema
    parsinglayers::P
    io::I
    bools::Parsers.Trie
    dateformat::DF
    decimal::UInt8
    fullpath::String
    datapos::Int # the position in the IOBuffer where the rows of data begins
end

function Base.show(io::IO, f::Source)
    println(io, "CSV.Source: ", f.fullpath)
    println(io, f.io)
    show(io, f.schema)
end

"""
`CSV.TransposedSource(file_or_io; kwargs...) => CSV.TransposedSource`

Type that in all respects is identical to a `CSV.Source`, except when reading a csv file,
the data will be parsed "transposed", i.e. rows will become columns / columns will become rows.
This can be a huge convenience if the csv data happens to be transposed, as well as lead to huge
performance gains as the resulting data set can be more consistently typed.

Typical usage involves calling `CSV.read(file; transpose=true)`.
"""
mutable struct TransposedSource{I, D} <: Data.Source
    schema::Data.Schema
    options::Options{D}
    io::I
    fullpath::String
    datapos::Int # the position in the IOBuffer where the rows of data begins
    columnpositions::Vector{Int}
end

"""
A type that satisfies the `Data.Sink` interface in the `DataStreams.jl` package.

A `CSV.Sink` can be manually constructed in order to be re-used multiple times.

`CSV.Sink(file_or_io; kwargs...) => CSV.Sink`

Note that a filename string can be provided or any `IO` type. For the full list of supported
keyword arguments, see the docs for [`CSV.write`](@ref) or type `?CSV.write` at the repl

An example of re-using a `CSV.Sink` is:
```julia
# manually construct a `CSV.Source` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.Source(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
mutable struct Sink{D, B} <: Data.Sink
    options::Options{D}
    io::IOBuffer
    fullpath::Union{String, IO}
    datapos::Int # the position in the IOBuffer where the rows of data begins
    header::Bool
    colnames::Vector{String}
    cols::Int
    append::Bool
    quotefields::B
end

# include("parsefields.jl")
# include("float.jl")
include("io.jl")
# include("TransposedSource.jl")
include("Source.jl")
include("Sink.jl")
include("validate.jl")

end # module
