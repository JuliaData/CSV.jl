
function Sink(s::CSV.Source; header::Bool=true, quotefields::Bool=true, append::Bool=false)
    data = open(s.fullpath, append ? "a" : "w")
    seek(data, s.datapos)
    return Sink(s.schema, s.options, data, s.datapos, header, quotefields)
end

# these two constructors create new Sinks, either from an open IOStream, or a filename
# most fields are populated through the CSV.Source

# create a new `Sink` patterned after the `CSV.Source`; data will be streamed to the `io::IOStream` argument
Sink(s::CSV.Source,io::IOStream; header::Bool=true, quotefields::Bool=true) = Sink(s.schema, s.options, io, position(io), header, quotefields)

# create a new `Sink` patterned after the `CSV.Source`; data will be streamed to the `file` argument after opening
function Sink(s::CSV.Source, file::AbstractString; header::Bool=true, quotefields::Bool=true, append::Bool=false)
    io = open(file,append ? "a" : "w")
    return Sink(s.schema, s.options, io, 0,header, quotefields)
end

function Sink(io::Union{AbstractString,IO};
              schema::Data.Schema=Data.EMPTYSCHEMA,
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              header::Bool=true,
              quotefields::Bool=true,
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io, AbstractString) ? open(io, append ? "a" : "w") : io
    return Sink(schema, CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat,-1,0,1,DataType[]), io, 0, header, quotefields)
end

Base.close(s::Sink) = (applicable(close, s.data) && close(s.data); return nothing)

function writeheaders(source::Data.Source, sink::Sink)
    rows, cols = size(source)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    h = Data.header(source)
    for col = 1:cols
        print(sink.data, q, sink.quotefields ? replace("$(h[col])", q, "$e$q") : h[col], q)
        print(sink.data, ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    end
    sink.datapos = position(sink.data)
    return nothing
end

writefield(io::Sink, val, col, N) = (col == N ? println(io.data,val) : print(io.data,val,Char(io.options.delim)); return nothing)
function writefield(sink::Sink, val::AbstractString, col, cols)
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

# DataStreams interface
Data.streamtypes{T<:CSV.Sink}(::Type{T}) = [Data.Field]

# stream data from `source` to `sink`; `header::Bool` = whether to write the column names or not
function Data.stream!(source, ::Type{Data.Field}, sink::CSV.Sink)
    Data.schema(sink) == Data.EMPTYSCHEMA && (sink.schema = source.schema)
    sink.header && writeheaders(source,sink)
    rows, cols = size(source)
    types = Data.types(source)
    row = 0
    while !Data.isdone(source, row, cols)
        row += 1
        for col = 1:cols
            val = Data.getfield(source, types[col], row, col)
            # below assumes that the result of getfield is Nullable
            # not necessarily true for DataFrame(zeros(10, 10))
            writefield(sink, isnull(val) ? sink.options.null : get(val), col, cols)
        end
    end
    Data.setrows!(source, rows)
    close(sink)
    return sink
end

"""
write a `source::Data.Source` out to a `CSV.Sink`

* `io::Union{String,IO}`; a filename (String) or `IO` type to write the `source` to
* `source`; a `Data.Source` type
* `delim::Union{Char,UInt8}`; how fields in the file will be delimited
* `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines
* `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field
* `null::String`; the ascii string that indicates how NULL values will be represented in the dataset
* `dateformat`; how dates/datetimes will be represented in the dataset
* `quotefields::Bool`; whether all fields should be quoted or not
* `header::Bool`; whether to write out the column names from `source`
* `append::Bool`; start writing data at the end of `io`; by default, `io` will be reset to its beginning before writing
"""
function write(io::Union{AbstractString,IO}, source;
              schema::Data.Schema=Data.EMPTYSCHEMA,
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              quotefields::Bool=true,
              header::Bool=true,
              append::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat,AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io,AbstractString) ? open(io, append ? "a" : "w") : io
    sink = Sink(schema,CSV.Options(delim,quotechar,escapechar,COMMA,PERIOD,
            ascii(null),null=="",dateformat,dateformat == Dates.ISODateFormat),io,0,header,quotefields)
    return Data.stream!(source, sink)
end
