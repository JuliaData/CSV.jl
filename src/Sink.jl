function Sink(io::Union{AbstractString,IO};
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
    return Sink(CSV.Options(delim, quotechar, escapechar, COMMA, PERIOD,
            ascii(null), null=="", dateformat, dateformat == Dates.ISODateFormat, -1, 0, 1, DataType[]), name, io, 0, header && !append)
end

Base.close(s::Sink) = (applicable(close, s.data) && close(s.data); return nothing)
Base.flush(s::Sink) = (applicable(flush, s.data) && flush(s.data); return nothing)
Base.position(s::Sink) = applicable(position, s.data) ? position(s.data) : 0

function writeheaders(schema, sink::Sink)
    rows, cols = size(schema)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    h = Data.header(schema)
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
function Sink{T}(sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, file::AbstractString; kwargs...)
    io = open(file, append ? "a" : "w")
    sink = Sink(io; append=append, kwargs...)
    !append && writeheaders(sch, sink)
    return sink
end

function Sink{T}(sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, io::IO; kwargs...)
    !append && seekstart(io)
    sink = Sink(io; append=append, kwargs...)
    !append && writeheaders(sch, sink)
    return sink
end

function Sink{T}(sink, sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8})
    if !isopen(sink.data)
        # try to re-open if we have filename
        sink.path == "__IO__" && throw(ArgumentError("unable to stream to a closed sink"))
        sink.data = open(sink.path, append ? "a" : "w")
    else
        !append && seekstart(sink.data)
    end
    !append && writeheaders(sch, sink)
    return sink
end

Data.streamto!(sink::Sink, ::Type{Data.Field}, val, row, col, sch) = (col == size(sch, 2) ? println(sink.data, val) : print(sink.data, val, Char(sink.options.delim)); return nothing)
function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::AbstractString, row, col, sch)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    print(sink.data, q, replace(string(val), q, "$e$q"), q)
    print(sink.data, ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::Dates.TimeType, row, col, sch)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar)
    val = sink.options.datecheck ? string(val) : Dates.format(val, sink.options.dateformat)
    print(sink.data, val)
    print(sink.data, ifelse(col == size(sch, 2), Char(NEWLINE), Char(sink.options.delim)))
    return nothing
end

function Data.streamto!{T}(sink::Sink, ::Type{Data.Field}, val::Nullable{T}, row, col, sch)
    Data.streamto!(sink, Data.Field, isnull(val) ? sink.options.null : get(val), row, col, sch)
    return nothing
end

if isdefined(:NAtype)
function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::NAtype, row, col, sch)
    Data.streamto!(sink, Data.Field, sink.options.null, row, col, sch)
    return nothing
end
end

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
function write{T}(io::Union{AbstractString,IO}, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(T(args...), CSV.Sink, append, transforms, io; kwargs...)
    Data.close!(sink)
    return sink
end
function write(io::Union{AbstractString,IO}, source; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(source, CSV.Sink, append, transforms, io; kwargs...)
    Data.close!(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(T(args...), sink, append, transforms); Data.close!(sink); return sink)
write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); Data.close!(sink); return sink)
