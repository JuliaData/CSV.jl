# # independent constructor
# function TransposedSource(fullpath::Union{AbstractString,IO};
#
#               delim=COMMA,
#               quotechar=QUOTE,
#               escapechar=ESCAPE,
#               null::AbstractString="",
#
#               header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
#               datarow::Int=-1, # by default, data starts immediately after header or start of file
#               types::Union{Dict{Int,Type},Dict{String,Type},Vector{Type}}=Type[],
#               nullable::Bool=true,
#               weakrefstrings::Bool=true,
#               dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
#
#               footerskip::Int=0,
#               rows_for_type_detect::Int=20,
#               rows::Int=0,
#               use_mmap::Bool=true)
#     # make sure character args are UInt8
#     isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
#     isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
#     isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))
#     dateformat = isa(dateformat, Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat)
#     return CSV.TransposedSource(fullpath=fullpath,
#                         options=CSV.Options(delim=typeof(delim) <: String ? UInt8(first(delim)) : (delim % UInt8),
#                                             quotechar=typeof(quotechar) <: String ? UInt8(first(quotechar)) : (quotechar % UInt8),
#                                             escapechar=typeof(escapechar) <: String ? UInt8(first(escapechar)) : (escapechar % UInt8),
#                                             null=null, dateformat=dateformat),
#                         header=header, datarow=datarow, types=types, nullable=nullable, weakrefstrings=weakrefstrings, footerskip=footerskip,
#                         rows_for_type_detect=rows_for_type_detect, rows=rows, use_mmap=use_mmap)
# end
#
# function TransposedSource(;fullpath::Union{AbstractString,IO}="",
#                 options::CSV.Options=CSV.Options(),
#
#                 header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
#                 datarow::Int=-1, # by default, data starts immediately after header or start of file
#                 types::Union{Dict{Int,Type},Dict{String,Type},Vector{Type}}=Type[],
#                 nullable::Bool=true,
#                 weakrefstrings::Bool=true,
#
#                 footerskip::Int=0,
#                 rows_for_type_detect::Int=20,
#                 rows::Int=0,
#                 use_mmap::Bool=true)
#     # argument checks
#     isa(fullpath, AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
#     isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
#
#     isa(fullpath, IOStream) && (fullpath = chop(replace(fullpath.name, "<file ", "")))
#
#     # open the file for property detection
#     if isa(fullpath, IOBuffer)
#         source = fullpath
#         fullpath = "<IOBuffer>"
#         fs = nb_available(source)
#     elseif isa(fullpath, IO)
#         source = IOBuffer(Base.read(fullpath))
#         fs = nb_available(source)
#         fullpath = isdefined(fullpath, :name) ? fullpath.name : "__IO__"
#     else
#         source = open(fullpath, "r") do f
#             IOBuffer(use_mmap ? Mmap.mmap(f) : Base.read(f))
#         end
#         fs = filesize(fullpath)
#     end
#     options.datarow != -1 && (datarow = options.datarow)
#     options.rows != 0 && (rows = options.rows)
#     options.header != 1 && (header = options.header)
#     !isempty(options.types) && (types = options.types)
#     rows = rows == 0 ? CSV.countlines(source, options.quotechar, options.escapechar) : rows
#     seekstart(source)
#     # BOM character detection
#     if fs > 0 && unsafe_peek(source) == 0xef
#         unsafe_read(source, UInt8)
#         unsafe_read(source, UInt8) == 0xbb || seekstart(source)
#         unsafe_read(source, UInt8) == 0xbf || seekstart(source)
#     end
#     datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
#     rows = fs == 0 ? 0 : max(-1, rows - datarow + 1 - footerskip) # rows now equals the actual number of rows in the dataset
#
#     # figure out # of columns and header, either an Integer, Range, or Vector{String}
#     # also ensure that `f` is positioned at the start of data
#     row_vals = Vector{RawField}()
#     if isa(header, Integer)
#         # default header = 1
#         if header <= 0
#             CSV.skipto!(source,1,datarow,options.quotechar,options.escapechar)
#             datapos = position(source)
#             CSV.readsplitline!(row_vals, source,options.delim,options.quotechar,options.escapechar)
#             seek(source, datapos)
#             columnnames = ["Column$i" for i = eachindex(row_vals)]
#         else
#             CSV.skipto!(source,1,header,options.quotechar,options.escapechar)
#             columnnames = [x.value for x in CSV.readsplitline!(row_vals, source,options.delim,options.quotechar,options.escapechar)]
#             datarow != header+1 && CSV.skipto!(source,header+1,datarow,options.quotechar,options.escapechar)
#             datapos = position(source)
#         end
#     elseif isa(header,Range)
#         CSV.skipto!(source,1,first(header),options.quotechar,options.escapechar)
#         columnnames = [x.value for x in readsplitline!(row_vals,source,options.delim,options.quotechar,options.escapechar)]
#         for row = first(header):(last(header)-1)
#             for (i,c) in enumerate([x.value for x in readsplitline!(row_vals,source,options.delim,options.quotechar,options.escapechar)])
#                 columnnames[i] *= "_" * c
#             end
#         end
#         datarow != last(header)+1 && CSV.skipto!(source,last(header)+1,datarow,options.quotechar,options.escapechar)
#         datapos = position(source)
#     elseif fs == 0
#         datapos = position(source)
#         columnnames = header
#         cols = length(columnnames)
#     else
#         CSV.skipto!(source,1,datarow,options.quotechar,options.escapechar)
#         datapos = position(source)
#         readsplitline!(row_vals,source,options.delim,options.quotechar,options.escapechar)
#         seek(source,datapos)
#         if isempty(header)
#             columnnames = ["Column$i" for i in eachindex(row_vals)]
#         else
#             length(header) == length(row_vals) || throw(ArgumentError("The length of provided header ($(length(header))) doesn't match the number of columns at row $datarow ($(length(row_vals)))"))
#             columnnames = header
#         end
#     end
#
#     # Detect column types
#     cols = length(columnnames)
#     if isa(types,Vector) && length(types) == cols
#         columntypes = types
#     elseif isa(types,Dict) || isempty(types)
#         columntypes = Vector{Type}(cols)
#         fill!(columntypes, Any)
#         lineschecked = 0
#         while !eof(source) && lineschecked < min(rows < 0 ? rows_for_type_detect : rows, rows_for_type_detect)
#             lineschecked += 1
#             # println("type detecting on row = $lineschecked...")
#             for i = 1:cols
#                 # print("\tdetecting col = $i...")
#                 typ = CSV.detecttype(source, options)
#                 # print(typ)
#                 columntypes[i] = promote_type2(columntypes[i], typ)
#                 # println("...promoting to: ", columntypes[i])
#             end
#         end
#     else
#         throw(ArgumentError("$cols number of columns detected; `types` argument has $(length(types)) entries"))
#     end
#     if isa(types, Dict{Int, Type})
#         for (col, typ) in types
#             columntypes[col] = typ
#         end
#     elseif isa(types, Dict{String, Type})
#         for (col,typ) in types
#             c = findfirst(columnnames, col)
#             columntypes[c] = typ
#         end
#     end
#     if nullable
#         for i = 1:cols
#             T = columntypes[i]
#             columntypes[i] = ifelse(T >: Null, T, ?T)
#         end
#     end
#     seek(source, datapos)
#     sch = Data.Schema(columnnames, columntypes, rows)
#     return TransposedSource(sch, options, source, Int(pointer(source.data)), fullpath, datapos)
# end
function TransposedSource(file::String; delim=COMMA, header=[], types=[])
    data = Mmap.mmap(file)
    columnpositions = Int[1]
    for i = 1:length(data)
        data[i] == CSV.NEWLINE && push!(columnpositions, i+1)
    end
    pop!(columnpositions)
    datapos = 1
    while data[datapos] != delim
        datapos += 1
    end
    columnpositions[1] = datapos
    for i = 2:length(columnpositions)
        c = columnpositions[i]
        while c < length(data) && data[c] != delim
            c += 1
        end
        columnpositions[i] = c
    end
    rows = 0
    i = datapos
    while data[i] != NEWLINE
        data[i] == delim && (rows += 1)
        i += 1
    end
    sch = Data.Schema(header, types, rows)
    return TransposedSource(sch, CSV.Options(delim=delim), IOBuffer(data), Int(pointer(data)), file, datapos, columnpositions)
end


# construct a new TransposedSource from a Sink
TransposedSource(s::CSV.Sink) = CSV.TransposedSource(fullpath=s.fullpath, options=s.options)

"reset a `CSV.TransposedSource` to its beginning to be ready to parse data from again"
reset!(s::CSV.TransposedSource) = (seek(s.io, s.datapos); return nothing)

# Data.Source interface
Data.schema(source::CSV.TransposedSource) = source.schema
Data.isdone(io::CSV.TransposedSource, row, col) = eof(io.io) || (!isnull(io.schema.rows) && row > io.schema.rows)
Data.streamtype{T<:CSV.TransposedSource}(::Type{T}, ::Type{Data.Field}) = true
@inline function Data.streamfrom{T}(source::CSV.TransposedSource, ::Type{Data.Field}, ::Type{T}, row, col)
    seek(source.io, source.columnpositions[col])
    v = CSV.parsefield(source.io, T, source.options, row, col)
    source.columnpositions[col] = position(source.io)
    return v
end
# @inline Data.streamfrom{T}(source::CSV.TransposedSource, ::Type{Data.Field}, ::Type{Union{T, Null}}, row, col) = CSV.parsefield(source.io, T, source.options, row, col)
# Data.streamfrom{T}(source::CSV.TransposedSource, ::Type{Data.Field}, ::Type{Nullable{T}}, row, col) = CSV.parsefield(source.io, T, source.options, row, col)
@inline function Data.streamfrom(source::CSV.TransposedSource, ::Type{Data.Field}, ::Type{WeakRefString{UInt8}}, row, col)
    seek(source.io, source.columnpositions[col])
    v = CSV.parsefield(source.io, WeakRefString{UInt8}, source.options, row, col, STATE, source.ptr)
    source.columnpositions[col] = position(source.io)
    return v
end
Data.reference(source::CSV.TransposedSource) = source.io.data

"""
`CSV.read(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...)` => `typeof(sink)`

`CSV.read(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...)` => `Data.Sink`


parses a delimited file into a Julia structure (a DataFrame by default, but any valid `Data.Sink` may be requested).

Positional arguments:

* `fullpath`; can be a file name (string) or other `IO` instance
* `sink::Type{T}`; `DataFrame` by default, but may also be other `Data.Sink` types that support streaming via `Data.Field` interface; note that the method argument can be the *type* of `Data.Sink`, plus any required arguments the sink may need (`args...`).
                    or an already constructed `sink` may be passed (2nd method above)

Keyword Arguments:

* `delim::Union{Char,UInt8}`; a single character or ascii-compatible byte that indicates how fields in the file are delimited; default is `UInt8(',')`
* `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines; default is `UInt8('"')`
* `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field; default is `UInt8('\\')`
* `null::String`; an ASCII string that indicates how NULL values are represented in the dataset; default is the empty string, `""`
* `header`; column names can be provided manually as a complete Vector{String}, or as an Int/Range which indicates the row/rows that contain the column names
* `datarow::Int`; specifies the row on which the actual data starts in the file; by default, the data is expected on the next row after the header row(s); for a file without column names (header), specify `datarow=1`
* `types`; column types can be provided manually as a complete Vector{Type}, or in a Dict to reference individual columns by name or number
* `nullable::Bool`; indicates whether values can be nullable or not; `true` by default. If set to `false` and missing values are encountered, a `NullException` will be thrown
* `weakrefstrings::Bool=true`: indicates whether string-type columns should use the `WeakRefString` (for efficiency) or a regular `String` type
* `dateformat::Union{AbstractString,Dates.DateFormat}`; how all dates/datetimes in the dataset are formatted
* `footerskip::Int`; indicates the number of rows to skip at the end of the file
* `rows_for_type_detect::Int=100`; indicates how many rows should be read to infer the types of columns
* `rows::Int`; indicates the total number of rows to read from the file; by default the file is pre-parsed to count the # of rows; `-1` can be passed to skip a full-file scan, but the `Data.Sink` must be setup account for a potentially unknown # of rows
* `use_mmap::Bool=true`; whether the underlying file will be mmapped or not while parsing
* `append::Bool=false`; if the `sink` argument provided is an existing table, `append=true` will append the source's data to the existing data instead of doing a full replace
* `transforms::Dict{Union{String,Int},Function}`; a Dict of transforms to apply to values as they are parsed. Note that a column can be specified by either number or column name.

Note by default, "string" or text columns will be parsed as the [`WeakRefString`](https://github.com/quinnj/WeakRefStrings.jl) type. This is a custom type that only stores a pointer to the actual byte data + the number of bytes.
To convert a `String` to a standard Julia string type, just call `string(::WeakRefString)`, this also works on an entire column.
Oftentimes, however, it can be convenient to work with `WeakRefStrings` depending on the ultimate use, such as transfering the data directly to another system and avoiding all the intermediate copying.

Example usage:
```
julia> dt = CSV.read("bids.csv")
7656334×9 DataFrames.DataFrame
│ Row     │ bid_id  │ bidder_id                               │ auction │ merchandise      │ device      │
├─────────┼─────────┼─────────────────────────────────────────┼─────────┼──────────────────┼─────────────┤
│ 1       │ 0       │ "8dac2b259fd1c6d1120e519fb1ac14fbqvax8" │ "ewmzr" │ "jewelry"        │ "phone0"    │
│ 2       │ 1       │ "668d393e858e8126275433046bbd35c6tywop" │ "aeqok" │ "furniture"      │ "phone1"    │
│ 3       │ 2       │ "aa5f360084278b35d746fa6af3a7a1a5ra3xe" │ "wa00e" │ "home goods"     │ "phone2"    │
...
```

Other example invocations may include:
```julia
# read in a tab-delimited file
CSV.read(file; delim='\t')

# read in a comma-delimited file with null values represented as '\N', such as a MySQL export
CSV.read(file; null="\\N")

# manually provided column names; must match # of columns of data in file
# this assumes there is no header row in the file itself, so data parsing will start at the very beginning of the file
CSV.read(file; header=["col1", "col2", "col3"])

# manually provided column names, even though the file itself has column names on the first row
# `datarow` is specified to ensure data parsing occurs at correct location
CSV.read(file; header=["col1", "col2", "col3"], datarow=2)

# types provided manually; as a vector, must match length of columns in actual data
CSV.read(file; types=[Int, Int, Float64])

# types provided manually; as a Dict, can specify columns by # or column name
CSV.read(file; types=Dict(3=>Float64, 6=>String))
CSV.read(file; types=Dict("col3"=>Float64, "col6"=>String))

# manually provided # of rows; if known beforehand, this will improve parsing speed
# this is also a way to limit the # of rows to be read in a file if only a sample is needed
CSV.read(file; rows=10000)

# for data files, `file` and `file2`, with the same structure, read both into a single DataFrame
# note that `df` is used as a 2nd argument in the 2nd call to `CSV.read` and the keyword argument
# `append=true` is passed
df = CSV.read(file)
df = CSV.read(file2, df; append=true)

# manually construct a `CSV.TransposedSource` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.TransposedSource(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
db = SQLite.DB()
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
function read end

read(source::CSV.TransposedSource, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms, args...); return Data.close!(sink))
read{T}(source::CSV.TransposedSource, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); return Data.close!(sink))
