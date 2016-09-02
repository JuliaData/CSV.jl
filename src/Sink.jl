# construct a Sink from an existing CSV Source
function Sink(s::CSV.Source; append::Bool=false)
    data = open(s.fullpath, append ? "a" : "w")
    seek(data, append ? filesize(s.fullpath) : s.datapos)
    return Sink(s.schema, s.options, data, s.datapos, !append)
end

function Sink(io::Union{AbstractString,IO},
              schema::Data.Schema=Data.EMPTYSCHEMA;
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io, AbstractString) ? open(io, append ? "a" : "w") : io
    return Sink(schema, CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat, -1, 0, 1, DataType[]), io, 0, header)
end

Base.close(s::Sink) = (applicable(close, s.data) && close(s.data); return nothing)
Base.flush(s::Sink) = (applicable(flush, s.data) && flush(s.data); return nothing)
Base.position(s::Sink) = applicable(position, s.data) ? position(s.data) : 0

function writeheaders(source, sink::Sink)
    rows, cols = size(Data.schema(source))
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    h = Data.header(source)
    for col = 1:cols
        print(sink.data, q, replace("$(h[col])", q, "$e$q"), q)
        print(sink.data, ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    end
    sink.datapos = position(sink)
    return nothing
end

# DataStreams interface
Data.streamtypes{T<:CSV.Sink}(::Type{T}) = [Data.Field]

# Constructors
# CSV.read(file, CSV.Sink, file_or_io; append=true)
function Sink{T}(source, ::Type{T}, append::Bool, file::AbstractString)
    sch = Data.schema(source)
    io = open(file, append ? "a" : "w")
    return Sink(sch, CSV.Options(), io, 0, !append)
end
function Sink{T}(source, ::Type{T}, append::Bool, io::IO)
    sch = Data.schema(source)
    append ? io : seekstart(io)
    return Sink(sch, CSV.Options(), io, 0, !append)
end
# TT(sink, source, typ, append)
function Sink{T}(sink, source, ::Type{T}, append::Bool)
    !append && seekstart(sink.data)
    sink.schema = Data.schema(source)
    return sink
end

writefield(io::Sink, val, col, N) = (col == N ? println(io.data,val) : print(io.data,val,Char(io.options.delim)); return nothing)
function writefield(sink::Sink, val::AbstractString, col, cols)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    print(sink.data, q, replace(string(val), q, "$e$q"), q)
    print(sink.data, ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function writefield(sink::Sink, val::Dates.TimeType, col, cols)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    val = sink.options.datecheck ? string(val) : Dates.format(val, sink.options.dateformat)
    print(sink.data, val)
    print(sink.data, ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function writefield{T}(sink::Sink, val::Nullable{T}, col, cols)
    writefield(sink, isnull(val) ? sink.options.null : get(val), col, cols)
    return nothing
end
if isdefined(:NAtype)
function writefield(sink::Sink, val::NAtype, col, cols)
    writefield(sink, sink.options.null, col, cols)
    return nothing
end
end

function writefield!{T}(sink, source, ::Type{T}, row, col, cols)
    val = Data.getfield(source, T, row, col)
    writefield(sink, val, col, cols)
    return nothing
end

# stream data from `source` to `sink`; `header::Bool` = whether to write the column names or not
function Data.stream!(source, ::Type{Data.Field}, sink::CSV.Sink, append::Bool)
    Data.schema(sink) == Data.EMPTYSCHEMA && (sink.schema = Data.schema(source))
    !append && sink.header && writeheaders(source, sink)
    rows, cols = size(source)
    Data.isdone(source, 1, 1) && return sink
    types = Data.types(source)
    row = 1
    while !Data.isdone(source, row, cols+1)
        for col = 1:cols
            writefield!(sink, source, types[col], row, col, cols)
        end
        row += 1
    end
    Data.setrows!(source, rows)
    flush(sink)
    return sink
end

"""
`CSV.write(fullpath::Union{AbstractString,IO}, source::Type{T}, args...; kwargs...)` => `CSV.Sink`
`CSV.write(fullpath::Union{AbstractString,IO}, source::Data.Source; kwargs...)` => `CSV.Sink`

write a `Data.Source` out to a `CSV.Sink`.

Positional Arguments:

* `fullpath`; can be a file name (string) or other `IO` instance
* `source` can be the *type* of `Data.Source`, plus any required `args...`, or an already constructed `Data.Source` can be passsed in directly (2nd method)

Keyword Arguments:

* `delim::Union{Char,UInt8}`; how fields in the file will be delimited; default is `UInt8(',')`
* `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines; default is `UInt8('"')`
* `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field; default is `UInt8('\\')`
* `null::String`; the ascii string that indicates how NULL values will be represented in the dataset; default is the emtpy string `""`
* `dateformat`; how dates/datetimes will be represented in the dataset; default is ISO-8601 `yyyy-mm-ddTHH:MM:SS.s`
* `header::Bool`; whether to write out the column names from `source`
* `append::Bool`; start writing data at the end of `io`; by default, `io` will be reset to its beginning before writing
"""
function write{T}(io::Union{AbstractString,IO}, ::Type{T}, args...;
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io,AbstractString) ? open(io, append ? "a" : "w") : io
    source = T(args...)
    schema = Data.schema(source)
    sink = Sink(schema, CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat, -1, 0, 1, DataType[]), io, 0, header)
    Data.stream!(source, sink, append)
    close(sink)
    return sink
end
function write(io::Union{AbstractString,IO}, source;
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat,AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io,AbstractString) ? open(io, append ? "a" : "w") : io
    schema = Data.schema(source)
    sink = Sink(schema, CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat, -1, 0, 1, DataType[]), io, 0, header)
    Data.stream!(source, sink, append)
    close(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false) = Data.stream!(T(args...), sink, append)
write(sink::Sink, source; append::Bool=false) = Data.stream!(source, sink, append)
