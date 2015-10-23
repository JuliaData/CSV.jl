type Sink{I<:IO} <: Data.Sink # <: IO
    schema::Data.Schema
    options::Options
    data::I
    datapos::Int # the byte position in `io` where the data rows start
    isclosed::Bool
    quotefields::Bool # whether to always quote string fields or not
end

Base.position(io::Sink) = position(io.data)
isclosed(io::Sink) = io.isclosed

# construct a new Source from a Sink that has been streamed to (i.e. DONE)
function Source(s::CSV.Sink{IOStream})
    isclosed(s) || throw(ArgumentError("::Sink has not been closed to streaming yet; call `close(::Sink)` first"))
    data = IOBuffer(Mmap.mmap(chop(replace(s.data.name,"<file ",""))))
    seek(data,s.datapos)
    return Source(s.schema,s.options,data,s.datapos,utf8(chop(replace(s.data.name,"<file ",""))))
end
function Source(s::CSV.Sink)
    isclosed(s) || throw(ArgumentError("::Sink has not been closed to streaming yet; call `close(::Sink)` first"))
    data = IOBuffer(readbytes(s.data))
    seek(data,s.datapos)
    return Source(s.schema,s.options,data,s.datapos,utf8(""))
end

# creates a Sink wrapping an existing "ultimate data source" (`s.fullpath` in this case)
# can replace or append to this new Sink
function Sink(s::CSV.Source;quotefields::Bool=true,append::Bool=false)
    data = open(s.fullpath,append ? "a" : "w")
    seek(data, s.datapos)
    return Sink(s.schema,s.options,data,s.datapos,false,quotefields)
end
# these two constructors create new Sinks, either from an open IOStream, or a filename
# most fields are populated through the CSV.Source
Sink(s::CSV.Source,io::IOStream;quotefields::Bool=true) = Sink(s.schema,s.options,io,position(io),false,quotefields)

function Sink(s::CSV.Source,file::AbstractString;quotefields::Bool=true,append::Bool=false)
    io = open(file,append ? "a" : "w")
    return Sink(s.schema,s.options,io,0,false,quotefields)
end

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
            ascii(null),null=="",dateformat,dateformat == Dates.ISODateFormat),io,0,false,quotefields)
end

Base.close(s::Sink) = (applicable(close,s.data) && close(s.data); s.isclosed = true; return nothing)

function writeheaders(source::Data.Source,sink::Sink)
    isclosed(sink) && throw(CSVError("$sink is already closed; can't write to it"))
    rows, cols = size(source)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    h = Data.header(source)
    for col = 1:cols
        print(sink.data,q,sink.quotefields ? replace("$(h[col])",q,"$e$q") : h[col],q)
        print(sink.data,ifelse(col == cols, Char(NEWLINE), Char(sink.options.delim)))
    end
    sink.datapos = position(sink)
    return nothing
end

writefield(io::Sink, val, col, N) = (isclosed(io) && throw(CSVError("$io is already closed; can't write to it"));
                                        col == N ? println(io.data,val) : print(io.data,val,Char(io.options.delim)); return nothing)
function writefield(sink::Sink, val::AbstractString, col, cols)
    isclosed(sink) && throw(CSVError("$sink is already closed; can't write to it"))
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    if sink.quotefields
        print(sink.data,q,replace(val,q,"$e$q"),q)
    else
        print(sink.data,val) # should we detect delim, newline here and quote automatically?
    end
    print(sink.data,ifelse(col == cols, NEWLINE, Char(sink.options.delim)))
    return nothing
end
function writefield(sink::Sink, val::Dates.TimeType, col, cols)
    isclosed(sink) && throw(CSVError("$sink is already closed; can't print to it"))
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    val = sink.options.datecheck ? val : Dates.format(val,sink.options.dateformat)
    if sink.quotefields
        print(sink.data,q,replace(val,q,"$e$q"),q)
    else
        print(sink.data,val)
    end
    print(sink.data,ifelse(col == cols, NEWLINE, Char(sink.options.delim)))
    return nothing
end

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
