__precompile__(true)
module CSV

using Compat, DataStreams, WeakRefStrings, Nulls

export Data, DataFrame

include("buffer.jl")

struct CSVError <: Exception
    msg::String
end

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

"""
Represents the various configuration settings for delimited text file parsing.

Keyword Arguments:

 * `delim::Union{Char,UInt8}`; how fields in the file are delimited
 * `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines
 * `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field
 * `null::String`; indicates how NULL values are represented in the dataset
 * `dateformat::Union{AbstractString,Dates.DateFormat}`; how dates/datetimes are represented in the dataset
"""
mutable struct Options{D}
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    null::Vector{UInt8}
    nullcheck::Bool
    dateformat::D
    # non-public for now
    datarow::Int
    rows::Int
    header::Union{Integer,UnitRange{Int},Vector}
    types::Union{Dict{Int,DataType},Dict{String,DataType},Vector{DataType}}
end

Options(;delim=COMMA, quotechar=QUOTE, escapechar=ESCAPE, null="", dateformat=Dates.ISODateTimeFormat, datarow=-1, rows=0, header=1, types=DataType[]) =
    Options(delim%UInt8, quotechar%UInt8, escapechar%UInt8,
            map(UInt8, collect(ascii(null))), null != "", isa(dateformat,Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat), datarow, rows, header, types)
function Base.show(io::IO,op::Options)
    println(io, "    CSV.Options:")
    println(io, "        delim: '", Char(op.delim), "'")
    println(io, "        quotechar: '", Char(op.quotechar), "'")
    print(io, "        escapechar: '"); escape_string(io, string(Char(op.escapechar)), "\\"); println(io, "'")
    print(io, "        null: \""); escape_string(io, isempty(op.null) ? "" : String(collect(op.null)), "\\"); println(io, "\"")
    print(io, "        dateformat: ", op.dateformat)
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
mutable struct Source{D, I, P1, P2} <: Data.Source
    schema::Data.Schema
    options::Options{D}
    io::Buffer{I, P1, P2}
    ptr::Int # pointer to underlying data buffer
    fullpath::String
    datapos::Int # the position in the IOBuffer where the rows of data begins
end

function Base.show(io::IO, f::Source)
    println(io, "CSV.Source: ", f.fullpath)
    println(io, f.options)
    show(io, f.schema)
end

mutable struct TransposedSource{D} <: Data.Source
    schema::Data.Schema
    options::Options{D}
    io::IOBuffer
    ptr::Int # pointer to underlying data buffer
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
mutable struct Sink <: Data.Sink
    options::Options
    io::IOBuffer
    fullpath::Union{String, IO}
    datapos::Int # the position in the IOBuffer where the rows of data begins
    header::Bool
    colnames::Vector{String}
    append::Bool
end

include("parsefields.jl")
include("float.jl")
include("io.jl")
include("Source.jl")
include("TransposedSource.jl")
include("Sink.jl")

end # module
