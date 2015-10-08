
type Sink{I<:IO} <: IOSink # <: IO
    io::I
    replace::Bool # replace or append
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    null::UTF8String # how null is represented in the dataset
    dateformat::Dates.DateFormat
    defaultdate::Bool # whether `dateformat` is the default Dates.ISODateFormat
    quotefields::Bool # whether to always quote string fields or not
end

function writeheaders(source::Source,sink::Sink)
    h = header(source)
    for col in h
        print(sink.io,io.quotefields ? replace("$(sink.quotechar)$col$(sink.quotechar)",sink.quotechar,"$(io.escapechar)$(io.quotechar)") : col,sink.delim)
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

function DataStreams.stream!(source::CSV.Source,sink::CSV.Sink;replace::Bool=true,header::Bool=true)
    replace ? seekstart(sink.io) : seekend(sink.io) # write a newline for append?
    header && writeheaders(source,sink)
    M, N = size(source)
    for row = 1:rows, col = 1:cols
        val, null = CSV.readfield(source, source.schema.types[col], row, col)
        writefield(sink, null ? sink.null : val, col, N)
    end
    return sink
end
