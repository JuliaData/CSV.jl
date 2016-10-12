const EMPTY_DATEFORMAT = Dates.DateFormat("")

# Constructors
# independent constructor
function Source(fullpath::Union{AbstractString,IO};

              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              null::AbstractString="",

              header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types::Union{Dict{Int,DataType},Dict{String,DataType},Vector{DataType}}=DataType[],
              nullable::Bool=true,
              dateformat::Union{AbstractString,Dates.DateFormat}=EMPTY_DATEFORMAT,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              use_mmap::Bool=true)
    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))
    dateformat = isa(dateformat, Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat)
    return CSV.Source(fullpath=fullpath,
                        options=CSV.Options(delim=delim % UInt8,quotechar=quotechar % UInt8,escapechar=escapechar % UInt8,
                                    null=null,dateformat=dateformat),
                        header=header, datarow=datarow, types=types, nullable=nullable, footerskip=footerskip,
                        rows_for_type_detect=rows_for_type_detect, rows=rows, use_mmap=use_mmap)
end

function Source(;fullpath::Union{AbstractString,IO}="",
                options::CSV.Options=CSV.Options(),

                header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
                datarow::Int=-1, # by default, data starts immediately after header or start of file
                types::Union{Dict{Int,DataType},Dict{String,DataType},Vector{DataType}}=DataType[],
                nullable::Bool=true,

                footerskip::Int=0,
                rows_for_type_detect::Int=100,
                rows::Int=0,
                use_mmap::Bool=true)
    # delim=CSV.COMMA;quotechar=CSV.QUOTE;escapechar=CSV.ESCAPE;separator=CSV.COMMA;decimal=CSV.PERIOD;null="";header=1;datarow=2;types=DataType[];formats=UTF8String[];skipblankrows=true;footerskip=0;rows_for_type_detect=250;countrows=true
    # argument checks
    isa(fullpath, AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

    isa(fullpath, IOStream) && (fullpath = chop(replace(fullpath.name, "<file ", "")))

    # open the file for property detection
    if isa(fullpath, IOBuffer)
        source = fullpath
        fullpath = "<IOBuffer>"
        fs = nb_available(source)
    elseif isa(fullpath, IO)
        source = IOBuffer(read(fullpath))
        fs = nb_available(fullpath)
        fullpath = isdefined(fullpath, :name) ? fullpath.name : "__IO__"
    else
        source = IOBuffer(use_mmap ? Mmap.mmap(fullpath) : open(read, fullpath))
        fs = filesize(fullpath)
    end
    options.datarow != -1 && (datarow = options.datarow)
    options.rows != 0 && (rows = options.rows)
    options.header != 1 && (header = options.header)
    options.types != DataType[] && (types = options.types)
    rows = rows == 0 ? CSV.countlines(source, options.quotechar, options.escapechar) : rows
    seekstart(source)
    # BOM character detection
    if fs > 0 && unsafe_peek(source) == 0xef
        unsafe_read(source, UInt8)
        unsafe_read(source, UInt8) == 0xbb || seekstart(source)
        unsafe_read(source, UInt8) == 0xbf || seekstart(source)
    end
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    rows = rows - datarow + 1 - footerskip # rows now equals the actual number of rows in the dataset

    # figure out # of columns and header, either an Integer, Range, or Vector{String}
    # also ensure that `f` is positioned at the start of data
    cols = 0
    if isa(header, Integer)
        # default header = 1
        if header <= 0
            CSV.skipto!(source,1,datarow,options.quotechar,options.escapechar)
            datapos = position(source)
            cols = length(CSV.readsplitline(source,options.delim,options.quotechar,options.escapechar))
            seek(source, datapos)
            columnnames = ["Column$i" for i = 1:cols]
        else
            CSV.skipto!(source,1,header,options.quotechar,options.escapechar)
            columnnames = [x for x in CSV.readsplitline(source,options.delim,options.quotechar,options.escapechar)]
            cols = length(columnnames)
            datarow != header+1 && CSV.skipto!(source,header+1,datarow,options.quotechar,options.escapechar)
            datapos = position(source)
        end
    elseif isa(header,Range)
        CSV.skipto!(source,1,first(header),options.quotechar,options.escapechar)
        columnnames = [x for x in readsplitline(source,options.delim,options.quotechar,options.escapechar)]
        cols = length(columnnames)
        for row = first(header):(last(header)-1)
            for (i,c) in enumerate([x for x in readsplitline(source,options.delim,options.quotechar,options.escapechar)])
                columnnames[i] *= "_" * c
            end
        end
        datarow != last(header)+1 && CSV.skipto!(source,last(header)+1,datarow,options.quotechar,options.escapechar)
        datapos = position(source)
    else
        CSV.skipto!(source,1,datarow,options.quotechar,options.escapechar)
        datapos = position(source)
        cols = length(readsplitline(source,options.delim,options.quotechar,options.escapechar))
        seek(source,datapos)
        if isempty(header)
            columnnames = ["Column$i" for i = 1:cols]
        else
            length(header) == cols || throw(ArgumentError("length of provided header doesn't match number of columns of data at row $datarow"))
            columnnames = header
        end
    end

    # Detect column types
    columntypes = Array(DataType,cols)
    if isa(types,Vector) && length(types) == cols
        columntypes = types
    elseif isa(types,Dict) || isempty(types)
        poss_types = Array(DataType, min(rows < 0 ? rows_for_type_detect : rows, rows_for_type_detect), cols)
        fill!(poss_types, NullField)
        lineschecked = 0
        while !eof(source) && lineschecked < min(rows < 0 ? rows_for_type_detect : rows, rows_for_type_detect)
            vals = CSV.readsplitline(source, options.delim, options.quotechar, options.escapechar)
            lineschecked += 1
            for i = 1:cols
                i > length(vals) && continue
                poss_types[lineschecked, i] = CSV.detecttype(vals[i], options.dateformat, options.null)
            end
        end
        # detect most common/general type of each column of types
        d = Set{DataType}()
        for i = 1:cols
            for n = 1:lineschecked
                t = poss_types[n, i]
                t == NullField && continue
                push!(d, t)
            end
            columntypes[i] = (isempty(d) || WeakRefString{UInt8} in d ) ? WeakRefString{UInt8} :
                                (Date     in d) ? Date :
                                (DateTime in d) ? DateTime :
                                (Float64  in d) ? Float64 :
                                (Int      in d) ? Int : WeakRefString{UInt8}
            empty!(d)
        end
    else
        throw(ArgumentError("$cols number of columns detected; `types` argument has $(length(types)) entries"))
    end
    if isa(types, Dict{Int, DataType})
        for (col, typ) in types
            columntypes[col] = typ
        end
    elseif isa(types, Dict{String, DataType})
        for (col,typ) in types
            c = findfirst(columnnames, col)
            columntypes[c] = typ
        end
    end
    (any(columntypes .== DateTime) && options.dateformat == EMPTY_DATEFORMAT) && (options.dateformat = Dates.ISODateTimeFormat)
    (any(columntypes .== Date) && options.dateformat == EMPTY_DATEFORMAT) && (options.dateformat = Dates.ISODateFormat)
    if nullable
        columntypes = DataType[T <: Nullable ? T : Nullable{T} for T in columntypes]
    else
        # WeakRefStrings won't be safe if they don't end up in a NullableArray
        columntypes = DataType[T <: WeakRefString ? String : T for T in columntypes]
    end
    seek(source, datapos)
    return Source(Data.Schema(columnnames, columntypes, rows),
                  options, source, Int(pointer(source.data)), fullpath, datapos)
end

# construct a new Source from a Sink that has been streamed to (i.e. DONE)
Source(s::CSV.Sink) = CSV.Source(fullpath=s.fullpath, options=s.options)

# Data.Source interface
Data.schema(source::CSV.Source, ::Type{Data.Field}) = source.schema
Data.isdone(io::CSV.Source, row, col) = eof(io.io) || (row > io.schema.rows && io.schema.rows > -1)
Data.streamtype{T<:CSV.Source}(::Type{T}, ::Type{Data.Field}) = true
Data.streamfrom{T}(source::CSV.Source, ::Type{Data.Field}, ::Type{T}, row, col) = get(CSV.parsefield(source.io, T, source.options, row, col))
Data.streamfrom{T}(source::CSV.Source, ::Type{Data.Field}, ::Type{Nullable{T}}, row, col) = CSV.parsefield(source.io, T, source.options, row, col)
Data.streamfrom(source::CSV.Source, ::Type{Data.Field}, ::Type{Nullable{WeakRefString{UInt8}}}, row, col) = CSV.parsefield(source.io, WeakRefString{UInt8}, source.options, row, col, STATE, source.ptr)
Data.reference(source::CSV.Source) = source.io.data

"""
`CSV.read(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...)` => `typeof(sink)`
`CSV.read(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...)` => `Data.Sink`

parses a delimited file into a Julia structure (a DataFrame by default, but any `Data.Sink` may be given).

Positional arguments:

* `fullpath`; can be a file name (string) or other `IO` instance
* `sink::Type{T}`; `DataFrame` by default, but may also be other `Data.Sink` types that support streaming via `Data.Field` interface; note that the method argument can be the *type* of `Data.Sink`, plus any required arguments the sink may need (`args...`).
                    or an already constructed `sink` may be passed (2nd method above)

Keyword Arguments:

* `delim::Union{Char,UInt8}`; a single character or ascii-compatible byte that indicates how fields in the file are delimited; default is `UInt8(',')`
* `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines; default is `UInt8('"')`
* `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field; default is `UInt8('\\')`
* `null::String`; an ascii string that indicates how NULL values are represented in the dataset; default is the empty string, `""`
* `header`; column names can be provided manually as a complete Vector{String}, or as an Int/Range which indicates the row/rows that contain the column names
* `datarow::Int`; specifies the row on which the actual data starts in the file; by default, the data is expected on the next row after the header row(s); for a file without column names (header), specify `datarow=1`
* `types`; column types can be provided manually as a complete Vector{DataType}, or in a Dict to reference individual columns by name or number
* `nullable::Bool`; indicates whether values can be nullable or not; `true` by default. If set to `false` and missing values are encountered, a `NullException` will be thrown
* `dateformat::Union{AbstractString,Dates.DateFormat}`; how all dates/datetimes in the dataset are formatted
* `footerskip::Int`; indicates the number of rows to skip at the end of the file
* `rows_for_type_detect::Int=100`; indicates how many rows should be read to infer the types of columns
* `rows::Int`; indicates the total number of rows to read from the file; by default the file is pre-parsed to count the # of rows; `-1` can be passed to skip a full-file scan, but the `Data.Sink` must be setup account for a potentially unknown # of rows
* `use_mmap::Bool=true`; whether the underlying file will be mmapped or not while parsing
* `append::Bool=false`; if the `sink` argument provided is an existing table, `append=true` will append the source's data to the existing data instead of doing a full replace

Note by default, "string" or text columns will be parsed as the [`WeakRefString`](https://github.com/quinnj/WeakRefStrings.jl) type. This is a custom type that only stores a pointer to the actual byte data + the number of bytes.
To convert a `String` to a standard Julia string type, just call `string(::WeakRefString)`, this also works on an entire column.
Oftentimes, however, it can be convenient to work with `WeakRefStrings` depending on the ultimate use, such as transfering the data directly to another system and avoiding all the intermediate copying.

Example usage:
```
julia> dt = CSV.read("bids.csv")
7656334×9 DataFrames.DataFrame
│ Row     │ bid_id  │ bidder_id                               │ auction │ merchandise      │ device      │
├─────────┼─────────┼─────────────────────────────────────────┼─────────┼──────────────────┼─────────────┤
│ 1       │ 0       │ "8dac2b259fd1c6d1120e519fb1ac14fbqvax8" │ "ewmzr" │ "jewelry"        │ "phone0"    │
│ 2       │ 1       │ "668d393e858e8126275433046bbd35c6tywop" │ "aeqok" │ "furniture"      │ "phone1"    │
│ 3       │ 2       │ "aa5f360084278b35d746fa6af3a7a1a5ra3xe" │ "wa00e" │ "home goods"     │ "phone2"    │
...
```
"""
function read(fullpath::Union{AbstractString,IO}, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    source = Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink, append, transforms, args...)
    Data.close!(sink)
    return sink
end

function read{T}(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    source = Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink, append, transforms)
    Data.close!(sink)
    return sink
end

read(source::CSV.Source, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms, args...); Data.close!(sink); return sink)
read{T}(source::CSV.Source, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); Data.close!(sink); return sink)
