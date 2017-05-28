function Sink(fullpath::AbstractString;
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              colnames::Vector{String}=String[],
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = IOBuffer()
    options = CSV.Options(delim=delim, quotechar=quotechar, escapechar=escapechar, null=null, dateformat=dateformat)
    !append && header && !isempty(colnames) && writeheaders(io, colnames, options)
    return Sink(options, io, fullpath, position(io), !append && header && !isempty(colnames), colnames, append)
end

function writeheaders(io::IOBuffer, h::Vector{String}, options)
    cols = length(h)
    q = Char(options.quotechar); e = Char(options.escapechar)
    for col = 1:cols
        print(io, q, replace("$(h[col])", q, "$e$q"), q)
        print(io, ifelse(col == cols, Char(NEWLINE), Char(options.delim)))
    end
    return nothing
end

# DataStreams interface
Data.streamtypes{T<:CSV.Sink}(::Type{T}) = [Data.Field]

# Constructors
function Sink{T}(sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, file::AbstractString; kwargs...)
    sink = Sink(file; append=append, colnames=Data.header(sch), kwargs...)
    return sink
end

function Sink{T}(sink, sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8})
    sink.append = append
    !sink.header && !append && writeheaders(sink.io, Data.header(sch), sink.options)
    return sink
end

Data.streamto!(sink::Sink, ::Type{Data.Field}, val, row, col, sch) = (col == size(sch, 2) ? println(sink.io, val) : print(sink.io, val, Char(sink.options.delim)); return nothing)
function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::AbstractString, row, col, sch)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    print(sink.io, q, replace(string(val), q, string(e,q)), q)
    print(sink.io, ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::Dates.TimeType, row, col, sch)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    val = sink.options.datecheck ? string(val) : Dates.format(val, sink.options.dateformat)
    print(sink.io, val)
    print(sink.io, ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function Data.streamto!{T}(sink::Sink, ::Type{Data.Field}, val::Nullable{T}, row, col, sch)
    if isnull(val)
        # print null string unquoted and unescaped
        # FIXME if the null strings should be escaped, add options.out_null = preescaped version of null
        print(sink.io, sink.options.null,
                       ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    else
        Data.streamto!(sink, Data.Field, unsafe_get(val), row, col, sch)
    end
    return nothing
end

if isdefined(:NAtype)
function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::NAtype, row, col, sch)
    # print null string unquoted and unescaped
    # FIXME if the null strings should be escaped, add options.out_null = preescaped version of null
    print(sink.io, sink.options.null,
                   ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end
end

function Data.close!(sink::CSV.Sink)
    io = isa(sink.fullpath, AbstractString) ? open(sink.fullpath, sink.append ? "a" : "w") : sink.fullpath
    Base.write(io, take!(sink.io))
    applicable(close, io) && close(io)
    return nothing
end

"""
`CSV.write(file_or_io::Union{AbstractString,IO}, source::Type{T}, args...; kwargs...)` => `CSV.Sink`

`CSV.write(file_or_io::Union{AbstractString,IO}, source::Data.Source; kwargs...)` => `CSV.Sink`


write a `Data.Source` out to a `file_or_io`.

Positional Arguments:

* `file_or_io`; can be a file name (string) or other `IO` instance
* `source` can be the *type* of `Data.Source`, plus any required `args...`, or an already constructed `Data.Source` can be passsed in directly (2nd method)

Keyword Arguments:

* `delim::Union{Char,UInt8}`; how fields in the file will be delimited; default is `UInt8(',')`
* `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines; default is `UInt8('"')`
* `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field; default is `UInt8('\\')`
* `null::String`; the ascii string that indicates how NULL values will be represented in the dataset; default is the emtpy string `""`
* `dateformat`; how dates/datetimes will be represented in the dataset; default is ISO-8601 `yyyy-mm-ddTHH:MM:SS.s`
* `header::Bool`; whether to write out the column names from `source`
* `colnames::Vector{String}`; a vector of string column names to be used when writing the header row
* `append::Bool`; start writing data at the end of `io`; by default, `io` will be reset to the beginning or overwritten before writing
* `transforms::Dict{Union{String,Int},Function}`; a Dict of transforms to apply to values as they are parsed. Note that a column can be specified by either number or column name.

A few example invocations include:
```julia
# write out a DataFrame `df` to a file name "out.csv" with all defaults, including comma as delimiter
CSV.write("out.csv", df)

# write out a DataFrame, this time as a tab-delimited file
CSV.write("out.csv", df; delim='\t')

# write out a DataFrame, with null values represented by the string "NA"
CSV.write("out.csv", df; null="NA")

# write out a "header-less" file, with actual data starting on row 1
CSV.write("out.csv", df; header=false)

# write out a DataFrame `df` twice to a file, the resulting file with have twice the # of rows as the DataFrame
# note the usage of the keyword argument `append=true` in the 2nd call
CSV.write("out.csv", df)
CSV.write("out.csv", df; append=true)

# write a DataFrame out to an IOBuffer instead of a file
io = IOBuffer
CSV.write(io, df)

# write the result of an SQLite query out to a comma-delimited file
db = SQLite.DB()
sqlite_source = SQLite.Source(db, "select * from sqlite_table")
CSV.write("sqlite_table.csv", sqlite_source)
```
"""
function write end

function write{T}(file::AbstractString, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(T(args...), CSV.Sink, append, transforms, file; kwargs...)
    Data.close!(sink)
    return sink
end
function write(file::AbstractString, source; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(source, CSV.Sink, append, transforms, file; kwargs...)
    Data.close!(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(T(args...), sink, append, transforms); Data.close!(sink); return sink)
write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); Data.close!(sink); return sink)
