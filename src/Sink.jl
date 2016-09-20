# construct a Sink from an existing CSV Source
function Sink(s::CSV.Source; append::Bool=false)
    data = open(s.fullpath, append ? "a" : "w")
    seek(data, append ? filesize(s.fullpath) : s.datapos)
    return Sink(s.schema, s.options, s.fullpath, data, s.datapos, !append)
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
    name, io = isa(io, AbstractString) ? (io, open(io, append ? "a" : "w")) : ("__IO__", io)
    return Sink(schema, CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat, -1, 0, 1, DataType[]), name, io, 0, header && !append)
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
function Sink{T}(source, ::Type{T}, append::Bool, file::AbstractString)
    sch = Data.schema(source)
    io = open(file, append ? "a" : "w")
    return Sink(io, sch; append=append)
end

function Sink{T}(source, ::Type{T}, append::Bool, io::IO)
    sch = Data.schema(source)
    append ? io : seekstart(io)
    return Sink(io, sch; append=append)
end

function Sink{T}(sink, source, ::Type{T}, append::Bool)
    if !isopen(sink.data)
        # try to re-open if we have filename
        sink.path == "__IO__" && throw(ArgumentError("unable to stream to a closed sink"))
        sink.data = open(sink.path, append ? "a" : "w")
    else
        !append && seekstart(sink.data)
    end
    append && (sink.header = false)
    sink.schema = Data.schema(source)
    return sink
end

writefield(io::Sink, val, col, N) = (col == N ? println(io.data, val) : print(io.data, val, Char(io.options.delim)); return nothing)
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

function Data.streamfield!{T}(sink::CSV.Sink, source, ::Type{T}, row, col, cols)
    val = Data.getfield(source, T, row, col)
    writefield(sink, val, col, cols)
    return nothing
end

Data.open!(sink::CSV.Sink, source) = (sink.header && writeheaders(source, sink); return nothing)
Data.flush!(sink::CSV.Sink) = flush(sink)
Data.close!(sink::CSV.Sink) = close(sink)

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
* `append::Bool`; start writing data at the end of `io`; by default, `io` will be reset to the beginning before writing
"""
function write{T}(io::Union{AbstractString,IO}, ::Type{T}, args...;
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              append::Bool=false)
    sink = Sink(io; delim=delim, quotechar=quotechar, escapechar=escapechar, null=null,
                                         dateformat=dateformat, header=header, append=append)
    Data.stream!(T(args...), sink, append)
    Data.close!(sink)
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
    sink = Sink(io, Data.schema(source); delim=delim, quotechar=quotechar, escapechar=escapechar, null=null,
                                         dateformat=dateformat, header=header, append=append)
    Data.stream!(source, sink, append)
    Data.close!(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false) = (sink = Data.stream!(T(args...), sink, append); Data.close!(sink); return sink)
write(sink::Sink, source; append::Bool=false) = (sink = Data.stream!(source, sink, append); Data.close!(sink); return sink)
