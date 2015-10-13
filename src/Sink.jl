type Sink{I<:IO} <: IOSink # <: IO
    schema::Schema

    io::I
    isclosed::Bool
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    null::ASCIIString # how null is represented in the dataset
    dateformat::Dates.DateFormat
    defaultdate::Bool # whether `dateformat` is the default Dates.ISODateFormat
    quotefields::Bool # whether to always quote string fields or not
    datapos::Int # the byte position in `io` where the data rows start
end

Base.position(io::Sink) = position(io.io)

# construct a new Source from an existing Sink
Source(s::CSV.Sink{IOStream}) = Source(s.schema,utf8(chop(replace(s.io.name,"<file ",""))),s.delim,s.quotechar,s.escapechar,
                                    CSV.COMMA,CSV.PERIOD,s.null,s.null == "",s.dateformat,
                                    Mmap.mmap(chop(replace(s.io.name,"<file ",""))),s.datapos,s.datapos)
Source(s::CSV.Sink) = Source(s.schema,utf8(""),s.delim,s.quotechar,s.escapechar,CSV.COMMA,CSV.PERIOD,s.null,s.null == "",s.dateformat,readbytes(s.io),s.datapos,s.datapos)

# creates a Sink around an existing CSV file (Source)
# can replace or append to this new Sink
function Sink(s::CSV.Source;quotefields::Bool=false,replace::Bool=true)
    io = open(s.fullpath,replace ? "w" : "a")
    return Sink(s.schema,io,false,s.delim,s.quotechar,s.escapechar,s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields,0)
end
# these two constructors create new Sinks, either from an open IOStream, or a filename
# most fields are populated through the CSV.Source
Sink(s::CSV.Source,io::IOStream;quotefields::Bool=false) = Sink(s.schema,io,false,s.delim,s.quotechar,s.escapechar,
                                                            s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields,0)
function Sink(s::CSV.Source,file::AbstractString;quotefields::Bool=false,replace::Bool=true)
    io = open(file,replace ? "w" : "a")
    return Sink(s.schema,io,false,s.delim,s.quotechar,s.escapechar,s.null,s.dateformat,s.dateformat == Dates.ISODateFormat,quotefields,0)
end

function Sink(io::Union{AbstractString,IO};
              schema::Schema=DataStreams.EMPTYSCHEMA,
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              null::AbstractString="",
              dateformat::Union{AbstractString,Dates.DateFormat}=Dates.ISODateFormat,
              quotefields::Bool=false)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat,AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = isa(io,AbstractString) && open(io,"r") : io
    return Sink(schema,io,false,delim,quotechar,escapechar,utf8(null),dateformat,dateformat == Dates.ISODateFormat,quotefields,0)
end

Base.close(s::Sink) = (applicable(close,s.io) && close(s.io); s.isclosed = true; return nothing)

function writeheaders(source::Source,sink::Sink)
    sink.isclosed && throw(CSVError("$sink is already closed; can't write to it"))
    cols = source.schema.cols
    q = sink.quotechar; e = sink.escapechar
    h = DataStreams.header(source)
    for i = 1:cols
        write(sink.io,sink.quotefields ? replace("$q$(h[i])$q",q,"$e$q") : h[i])
        write(sink.io,ifelse(i == cols, NEWLINE, sink.delim))
    end
    return nothing
end

writefield(io::Sink, val, col, N) = (io.isclosed && throw(CSVError("$io is already closed; can't write to it"));
                                        col == N ? println(io.io,val) : print(io.io,val,Char(io.delim)); return nothing)
function writefield(io::Sink, val::AbstractString, col, N)
    io.isclosed && throw(CSVError("$io is already closed; can't write to it"))
    if io.quotefields
        write(io.io,io.quotechar,replace(val,io.quotechar,"$(io.escapechar)$(io.quotechar)"),io.quotechar)
    else
        write(io.io,val) # should we detect delim, newline here and quote automatically?
    end
    col == N ? println(io.io) : write(io,io.delim)
    return nothing
end
function writefield(io::Sink, val::Dates.TimeType, col, N)
    io.isclosed && throw(CSVError("$io is already closed; can't write to it"))
    if io.quotefields
        print(io.io,io.quotechar,io.defaultdate ? val : Dates.format(val,io.dateformat),io.quotechar)
    else
        print(io.io,io.defaultdate ? val : Dates.format(val,io.dateformat))
    end
    col == N ? println(io.io) : print(io,io.delim)
    return nothing
end

function DataStreams.stream!(source::CSV.Source,sink::CSV.Sink;header::Bool=true)
    sink.schema == DataStreams.EMPTYSCHEMA && (sink.schema = source.schema)
    header && writeheaders(source,sink)
    sink.datapos = position(sink)+1
    rows, cols = size(source)
    for row = 1:rows, col = 1:cols
        val, null = CSV.readfield(source, source.schema.types[col], row, col)
        writefield(sink, null ? sink.null : val, col, cols)
    end
    close(sink)
    return sink
end
