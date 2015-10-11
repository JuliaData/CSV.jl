type Sink{I<:IO} <: IOSink # <: IO
    schema::Schema

    io::I
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    null::UTF8String # how null is represented in the dataset
    dateformat::Dates.DateFormat
    defaultdate::Bool # whether `dateformat` is the default Dates.ISODateFormat
    quotefields::Bool # whether to always quote string fields or not
    datapos::Int # the byte position in `io` where the data rows start
end

# construct a new Source from an existing Sink
Source(s::CSV.Sink) = Source(s.schema,utf8(""),s.delim,s.quotechar,s.escapechar,CSV.COMMA,CSV.PERIOD,s.null,s.null == "",s.dateformat,readbyes(s.io),s.datapos,s.datapos)

# creates a Sink around an existing CSV file (Source)
# can replace or append to this new Sink
function Sink(s::CSV.Source;quotefields::Bool=false,replace::Bool=true)
    io = open(s.fullpath,"r")
    !replace && seekend(io)
    return Sink(s.schema,io,s.delim,s.quotechar,s.escapechar,s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields)
end
# these two constructors create new Sinks, either from an open IOStream, or a filename
# most fields are populated through the CSV.Source
Sink(s::CSV.Source,io::IOStream;quotefields::Bool=false) = Sink(s.schema,io,s.delim,s.quotechar,s.escapechar,
                                                            s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields)
function Sink(s::CSV.Source,file::AbstractString;quotefields::Bool=false,replace::Bool=true)
    io = open(file,"r")
    !replace && seekend(io)
    return Sink(s.schema,io,s.delim,s.quotechar,s.escapechar,s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields)
end

function Sink(io::IO;
              schema::Schema=DataStreams.EMPTYSCHEMA,
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              quotefields::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat,AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    return Sink(schema,io,delim,quotechar,escapechar,utf8(null),dateformat,dateformat == Dates.ISODateFormat,quotefields)
end

Base.close(s::Sink) = applicable(close,s.io) && close(s.io)

function writeheaders(source::Source,sink::Sink)
    h = header(source)
    for col in h
        print(sink.io,sink.quotefields ? replace("$(sink.quotechar)$col$(sink.quotechar)",sink.quotechar,"$(io.escapechar)$(io.quotechar)") : col,sink.delim)
    end
    println(sink.io)
    return nothing
end

writefield(io::Sink, val, col, N) = (col == N ? println(io.io,val) : print(io.io,val,sink.delim); return nothing)
function writefield(io::Sink, val::AbstractString, col, N)
    if io.quotefields
        print(io.io,io.quotechar,replace(val,io.quotechar,"$(io.escapechar)$(io.quotechar)"),io.quotechar)
    else
        print(io.io,val) # should we detect delim, newline here and quote automatically?
    end
    col == N ? println(io.io) : print(io,io.delim)
    return nothing
end
function writefield(io::Sink, val::Dates.TimeType, col, N)
    if io.quotefields
        print(io.io,io.quotechar,io.defaultdate ? val : Dates.format(val,io.dateformat),io.quotechar)
    else
        print(io.io,io.defaultdate ? val : Dates.format(val,io.dateformat))
    end
    col == N ? println(io.io) : print(io,io.delim)
    return nothing
end

function DataStreams.stream!(source::CSV.Source,sink::CSV.Sink;header::Bool=true)
    header && writeheaders(source,sink)
    M, N = size(source)
    for row = 1:rows, col = 1:cols
        val, null = CSV.readfield(source, source.schema.types[col], row, col)
        writefield(sink, null ? sink.null : val, col, N)
    end
    close(sink)
    return sink
end
