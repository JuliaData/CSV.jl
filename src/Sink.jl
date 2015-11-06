
function Data.reset!(io::CSV.Sink;append::Bool=false)
    throw(ArgumentError("not currently able to reset a `CSV.Sink`"))
end
Data.isdone(io::CSV.Sink) = !isopen(io.data)

"""
creates a Sink wrapping an existing "true data source" (`s.fullpath` in this case)
can replace or append to this new Sink
"""
function Sink(s::CSV.Source;quotefields::Bool=true,append::Bool=false)
    data = open(s.fullpath,append ? "a" : "w")
    seek(data, s.datapos)
    return Sink(s.schema,s.options,data,s.datapos,quotefields)
end
# these two constructors create new Sinks, either from an open IOStream, or a filename
# most fields are populated through the CSV.Source
"create a new `Sink` patterned after the `CSV.Source`; data will be streamed to the `io::IOStream` argument"
Sink(s::CSV.Source,io::IOStream;quotefields::Bool=true) = Sink(s.schema,s.options,io,position(io),quotefields)
"create a new `Sink` patterned after the `CSV.Source`; data will be streamed to the `file` argument after opening"
function Sink(s::CSV.Source,file::AbstractString;quotefields::Bool=true,append::Bool=false)
    io = open(file,append ? "a" : "w")
    return Sink(s.schema,s.options,io,0,quotefields)
end
"""
create a new `Sink` from `io` (either a filename or `IOStream`), with various options:

* `delim`::Union{Char,UInt8} = how fields in the file will be delimited
* `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
* `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
* `null`::ASCIIString = the ascii string that indicates how NULL values will be represented in the dataset
* `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes will be represented in the dataset
* `quotefields`::Bool = whether all fields should be quoted or not
"""
function Sink(io::Union{AbstractString,IO};
              schema::Data.Schema=Data.EMPTYSCHEMA,
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              quotefields::Bool=true)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat,AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io,AbstractString) ? open(io,"w") : io
    return Sink(schema,CSV.Options(delim,quotechar,escapechar,COMMA,PERIOD,
            ascii(null),null=="",dateformat,dateformat == Dates.ISODateFormat),io,0,quotefields)
end

Base.close(s::Sink) = (applicable(close,s.data) && close(s.data); return nothing)

function writeheaders(source::Data.Source,sink::Sink)
    Data.isdone(sink) && throw(CSVError("$sink is already closed; can't write to it"))
    rows, cols = size(source)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    h = Data.header(source)
    for col = 1:cols
        print(sink.data,q,sink.quotefields ? replace("$(h[col])",q,"$e$q") : h[col],q)
        print(sink.data,ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    end
    sink.datapos = position(sink.data)
    return nothing
end

writefield(io::Sink, val, col, N) = (Data.isdone(io) && throw(CSVError("$io is already closed; can't write to it"));
                                        col == N ? println(io.data,val) : print(io.data,val,Char(io.options.delim)); return nothing)
function writefield(sink::Sink, val::AbstractString, col, cols)
    Data.isdone(sink) && throw(CSVError("$sink is already closed; can't write to it"))
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    if sink.quotefields
        print(sink.data,q,replace(string(val),q,"$e$q"),q)
    else
        print(sink.data,string(val)) # should we detect delim, newline here and quote automatically?
    end
    print(sink.data,ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end
function writefield(sink::Sink, val::Dates.TimeType, col, cols)
    Data.isdone(sink) && throw(CSVError("$sink is already closed; can't print to it"))
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    val = sink.options.datecheck ? val : Dates.format(val,sink.options.dateformat)
    if sink.quotefields
        print(sink.data,q,replace(val,q,"$e$q"),q)
    else
        print(sink.data,val)
    end
    print(sink.data,ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end
"stream data from `source` to `sink`; `header::Bool` = whether to write the column names or not"
function Data.stream!(source::CSV.Source,sink::CSV.Sink;header::Bool=true)
    Data.schema(sink) == Data.EMPTYSCHEMA && (sink.schema = source.schema)
    header && writeheaders(source,sink)
    rows, cols = size(source)
    types = Data.types(source)
    io = source.data; opts = source.options
    for row = 1:rows, col = 1:cols
        val, null = CSV.getfield(io, types[col], opts, row, col)
        writefield(sink, ifelse(null, sink.options.null, val), col, cols)
    end
    close(sink)
    return sink
end
