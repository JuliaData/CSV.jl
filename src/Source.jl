const EMPTY_DATEFORMAT = Dates.DateFormat("")

# Data.Source interface
Data.reset!(io::CSV.Source) = seek(io.data,io.datapos)
Data.isdone(io::CSV.Source) = eof(io.data)
# Data.getrow(io::CSV.Source) = readsplitline(io,io.options.delim,io.options.quotechar,io.options.escapechar)
Base.readline(io::CSV.Source) = readline(io,io.options.quotechar,io.options.escapechar)

# Constructors
# independent constructor
"""
constructs a `CSV.Source` file ready to start parsing data from

* `fullpath` can be a file name (string) or other `IO` instance
* `delim`::Union{Char,UInt8} = how fields in the file are delimited
* `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
* `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
* `null`::String = the ascii string that indicates how NULL values are represented in the dataset
* `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset
* `footerskip`::Int indicates the number of rows to skip at the end of the file
* `rows_for_type_detect`::Int indicates how many rows should be read to infer the types of columns
* `rows`::Int indicates the total number of rows to read from the file
* `use_mmap`::Bool=true; whether the underlying file will be mmapped or not while parsing

Note by default, "string" or text columns will be parsed as the `PointerString` type. This is a custom type that only stores a pointer to the actual byte data + the number of bytes.
To convert a `PointerString` to a standard Julia string type, just call `string(::PointerString)`, this also works on an entire column `string(::NullableVector{PointerString})`.
Oftentimes, however, it can be convenient to work with `PointerStrings` depending on the ultimate use, such as transfering the data directly to another system and avoiding all the intermediate byte copying.

Example usage:
```
julia> csv_source = CSV.Source("bids.csv")
CSV.Source: bids.csv
    CSV.Options:
        delim: ','
        quotechar: '"'
        escapechar: '\\'
        null: ""
        dateformat: Base.Dates.DateFormat(Base.Dates.Slot[],"","english")
7656334x9 Data.Schema:
 bid_id,     bidder_id,       auction,   merchandise,        device,  time,       country,            ip,           url
  Int64, PointerString, PointerString, PointerString, PointerString, Int64, PointerString, PointerString, PointerString
```
"""
function Source(fullpath::Union{AbstractString,IO};

              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              null::AbstractString="",

              header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types::Union{Dict{Int,DataType},Vector{DataType}}=DataType[],
              dateformat::Union{AbstractString,Dates.DateFormat}=EMPTY_DATEFORMAT,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              use_mmap::Bool=true)
    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))
    # isascii(decimal) || throw(ArgumentError("non-ASCII characters not supported for decimal argument: $decimal"))
    # isascii(separator) || throw(ArgumentError("non-ASCII characters not supported for separator argument: $separator"))
    delim = delim % UInt8
    quotechar = quotechar % UInt8
    escapechar = escapechar % UInt8
    decimal = CSV.PERIOD; decimal % UInt8
    separator = CSV.COMMA; separator % UInt8
    dateformat = isa(dateformat,Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat)
    return CSV.Source(fullpath=fullpath,
                        options=CSV.Options(delim,quotechar,escapechar,separator,
                                    decimal,ascii(null),null!="",dateformat,dateformat==Dates.ISODateFormat),
                        header=header, datarow=datarow, types=types, footerskip=footerskip,
                        rows_for_type_detect=rows_for_type_detect, rows=rows, use_mmap=use_mmap)
end

function Source(;fullpath::Union{AbstractString,IO}="",
                options::CSV.Options=CSV.Options(),

                header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
                datarow::Int=-1, # by default, data starts immediately after header or start of file
                types::Union{Dict{Int,DataType},Vector{DataType}}=DataType[],

                footerskip::Int=0,
                rows_for_type_detect::Int=100,
                rows::Int=0,
                use_mmap::Bool=true)
    # delim=CSV.COMMA;quotechar=CSV.QUOTE;escapechar=CSV.ESCAPE;separator=CSV.COMMA;decimal=CSV.PERIOD;null="";header=1;datarow=2;types=DataType[];formats=UTF8String[];skipblankrows=true;footerskip=0;rows_for_type_detect=250;countrows=true
    # argument checks
    isa(fullpath,AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
    isa(header,Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

    isa(fullpath,IOStream) && (fullpath = chop(replace(fullpath.name,"<file ","")))

    # open the file for property detection
    if isa(fullpath,IOBuffer)
        source = UnsafeBuffer(fullpath.data,fullpath.ptr,fullpath.size)
        fullpath = "<IOBuffer>"
    elseif isa(fullpath,IO)
        source = UnsafeBuffer(readbytes(fullpath))
        fullpath = fullpath.name
    else
        source = UnsafeBuffer(use_mmap ? Mmap.mmap(fullpath) : open(readbytes,fullpath))
    end
    rows = rows == 0 ? CSV.countlines(source,options.quotechar,options.escapechar) : rows
    rows == 0 && throw(ArgumentError("No rows of data detected in $fullpath"))
    seekstart(source)

    datarow = datarow == -1 ? (isa(header,Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    rows = rows - datarow + 1 - footerskip # rows now equals the actual number of rows in the dataset

    # figure out # of columns and header, either an Integer, Range, or Vector{String}
    # also ensure that `f` is positioned at the start of data
    cols = 0
    if isa(header,Integer)
        # default header = 1
        if header <= 0
            CSV.skipto!(source,1,datarow,options.quotechar,options.escapechar)
            datapos = position(source)
            cols = length(CSV.readsplitline(source,options.delim,options.quotechar,options.escapechar))
            seek(source, datapos)
            columnnames = UTF8String["Column$i" for i = 1:cols]
        else
            CSV.skipto!(source,1,header,options.quotechar,options.escapechar)
            columnnames = UTF8String[utf8(x) for x in CSV.readsplitline(source,options.delim,options.quotechar,options.escapechar)]
            cols = length(columnnames)
            datarow != header+1 && CSV.skipto!(source,header+1,datarow,options.quotechar,options.escapechar)
            datapos = position(source)
        end
    elseif isa(header,Range)
        CSV.skipto!(source,1,first(header),options.quotechar,options.escapechar)
        columnnames = UTF8String[utf8(x) for x in readsplitline(source,options.delim,options.quotechar,options.escapechar)]
        cols = length(columnnames)
        for row = first(header):(last(header)-1)
            for (i,c) in enumerate(UTF8String[utf8(x) for x in readsplitline(source,options.delim,options.quotechar,options.escapechar)])
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
            columnnames = UTF8String["Column$i" for i = 1:cols]
        else
            length(header) == cols || throw(ArgumentError("length of provided header doesn't match number of columns of data at row $datarow"))
            columnnames = UTF8String[utf8(x) for x in header]
        end
    end

    # Detect column types
    columntypes = Array(DataType,cols)
    if isa(types,Vector) && length(types) == cols
        columntypes = types
    elseif isa(types,Dict) || isempty(types)
        poss_types = Array(DataType,min(rows,rows_for_type_detect),cols)
        lineschecked = 0
        while !eof(source) && lineschecked < min(rows,rows_for_type_detect)
            vals = readsplitline(source,options.delim,options.quotechar,options.escapechar)
            lineschecked += 1
            for i = 1:cols
               poss_types[lineschecked,i] = CSV.detecttype(vals[i],options.dateformat,options.null)
            end
        end
        # detect most common/general type of each column of types
        d = Set{DataType}()
        for i = 1:cols
            for n = 1:lineschecked
                t = poss_types[n,i]
                t == NullField && continue
                push!(d,t)
            end
            columntypes[i] = (isempty(d) || PointerString in d ) ? PointerString :
                                (Date     in d) ? Date :
                                (DateTime in d) ? DateTime :
                                (Float64  in d) ? Float64 :
                                (Int      in d) ? Int : PointerString
            empty!(d)
        end
    else
        throw(ArgumentError("$cols number of columns detected; `types` argument has $(length(types)) entries"))
    end
    if isa(types,Dict)
        for (col,typ) in types
            columntypes[col] = typ
        end
    end
    (any(columntypes .== DateTime) || any(columntypes .== Date)) &&
        options.dateformat == EMPTY_DATEFORMAT && (options.dateformat = Dates.ISODateFormat)
    seek(source,datapos)
    return Source(Data.Schema(columnnames,columntypes,rows),options,source,datapos,utf8(fullpath))
end

"construct a new Source from a Sink that has been streamed to (i.e. DONE)"
function Source{I}(s::CSV.Sink{I})
    Data.isdone(s) || throw(ArgumentError("::Sink has not been closed to streaming yet; call `close(::Sink)` first"))
    if is(I,IOStream)
        nm = utf8(chop(replace(s.data.name,"<file ","")))
        data = IOBuffer(Mmap.mmap(nm))
    else
        seek(s.data, s.datapos)
        data = IOBuffer(readbytes(s.data))
        nm = utf8("")
    end
    seek(data,s.datapos)
    return Source(s.schema,s.options,data,s.datapos,nm)
end

# DataStreams interface
function parsefield!{T}(io::Union{IOBuffer,UnsafeBuffer}, dest::NullableVector{T}, ::Type{T}, opts, row, col)
    @inbounds val, null = CSV.parsefield(io, T, opts, row, col)
    @inbounds dest.values[row], dest.isnull[row] = val, null
    return
end

"parse data from `source` into a `Data.Table`"
function Data.stream!(source::CSV.Source,sink::Data.Table)
    Data.schema(source) == Data.schema(sink) || throw(ArgumentError("schema mismatch: \n$(Data.schema(source))\nvs.\n$(Data.schema(sink))"))
    rows, cols = size(source)
    types = Data.types(source)
    io = source.data
    opts = source.options
    for row = 1:rows, col = 1:cols
        @inbounds T = types[col]
        CSV.parsefield!(io, Data.unsafe_column(sink, col, T), T, opts, row, col)
    end
    sink.other = source.data # keep a reference to our mmapped array for PointerStrings
    return sink
end

"""
parses a delimited file into strongly typed NullableVectors.

* `fullpath` can be a file name (string) or other `IO` instance
* `delim`::Union{Char,UInt8} = how fields in the file are delimited
* `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
* `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
* `null`::String = the ascii string that indicates how NULL values are represented in the dataset
* `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset
* `footerskip`::Int indicates the number of rows to skip at the end of the file
* `rows_for_type_detect`::Int indicates how many rows should be read to infer the types of columns
* `rows`::Int indicates the total number of rows to read from the file
* `use_mmap`::Bool=true; whether the underlying file will be mmapped or not while parsing

Example usage:
```
julia> dt = CSV.csv("bids.csv")
DataStreams.Data.Table{Array{NullableArrays.NullableArray{T,1},1}}(7656334x9 Data.Schema:
     bid_id, Int64
  bidder_id, PointerString
    auction, PointerString
merchandise, PointerString
     device, PointerString
       time, Int64
    country, PointerString
         ip, PointerString
        url, PointerString
...
```
"""
function csv(fullpath::Union{AbstractString,IO};
              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              null::AbstractString="",
              header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types::Union{Dict{Int,DataType},Vector{DataType}}=DataType[],
              dateformat::Union{AbstractString,Dates.DateFormat}=EMPTY_DATEFORMAT,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              use_mmap::Bool=true)
    source = Source(fullpath::Union{AbstractString,IO};
                  delim=delim,quotechar=quotechar,escapechar=escapechar,null=null,
                  header=header,datarow=datarow,types=types,dateformat=dateformat,
                  footerskip=footerskip,rows_for_type_detect=rows_for_type_detect,rows=rows,use_mmap=use_mmap)
    return Data.stream!(source,Data.Table)
end
