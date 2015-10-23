const ZIP_FILE_EXTS = Set{UTF8String}([".zip",".gz","-gz",".z","-z","_z",".Z"#=,".bz2", ".bz", ".tbz2", ".tbz"=#])
const EMPTY_DATEFORMAT = Dates.DateFormat("")

Base.close(::Libz.BufferedStreams.BufferedInputStream{Libz.Source{:inflate,Libz.BufferedStreams.BufferedInputStream{IOStream}}}) = return nothing

"""
Represents the various configuration settings for csv file parsing.
 * `delim`::Union{Char,UInt8} = how fields in the file are delimited
 * `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
 * `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
 * `null`::ASCIIString = the ascii string that indicates how NULL values are represented in the dataset
 * `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset
"""
type Options
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    separator::UInt8
    decimal::UInt8
    null::ASCIIString # how null is represented in the dataset
    nullcheck::Bool   # do we have a custom null value to check for
    dateformat::Dates.DateFormat
    datecheck::Bool   # do we have a custom dateformat to check for
end
Options() = Options(COMMA,QUOTE,ESCAPE,COMMA,PERIOD,"",false,EMPTY_DATEFORMAT,false)
Options(;delim=COMMA,quotechar=QUOTE,escapechar=ESCAPE,null::ASCIIString="",dateformat=Dates.ISODateFormat) =
    Options(delim%UInt8,quotechar%UInt8,escapechar%UInt8,COMMA,PERIOD,
            null,null != "",isa(dateformat,Dates.DateFormat) ? dateformat : Dates.DateFormat(dateformat),dateformat == Dates.ISODateFormat)
function Base.show(io::IO,op::Options)
    println("    CSV.Options:")
    println(io,"        delim: '",@compat(Char(op.delim)),"'")
    println(io,"        quotechar: '",@compat(Char(op.quotechar)),"'")
    print(io,"        escapechar: '"); print_escaped(io,string(@compat(Char(op.escapechar))),"\\"); println(io,"'")
    print(io,"        null: \""); print_escaped(io,op.null,"\\"); println(io,"\"")
    print(io,"        dateformat: ",op.dateformat)
end

"`CSV.Source` satisfies the `DataStreams` interface for data processing for delimited `IO`."
type Source{I} <: Data.Source # <: IO
    schema::Data.Schema
    options::Options
    data::I # IOStream/IOBuffer/Any IO that implements read(io, UInt8) (which they all should)
    datapos::Int # the position in the IO where the rows of data begins
    fullpath::UTF8String
end

function Base.show(io::IO,f::Source)
    println(io,"CSV.Source: ",f.fullpath)
    println(io,f.options)
    showcompact(io, f.schema)
end

# IO interface
@inline Base.read(io::CSV.Source, ::Type{UInt8}) = read(io.data, UInt8)

# Data.Source interface
Base.eof(io::CSV.Source) = eof(io.data)
Data.reset!(io::CSV.Source) = seek(io.data,io.datapos)

Data.getrow(io::CSV.Source) = readsplitline(io,io.options.delim,io.options.quotechar,io.options.escapechar)

Base.readline(io::CSV.Source) = readline(io,io.options.quotechar,io.options.escapechar)

@inline function Base.read(from::Base.AbstractIOBuffer, ::Type{UInt8})
    @inbounds byte = from.data[from.ptr]
    from.ptr += 1
    return byte
end

# Constructors
# independent constructor
function Source(fullpath::Union{AbstractString,IO};
              compression="",

              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              null::AbstractString="",

              header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types::Union{Dict{Int,DataType},Vector{DataType}}=DataType[],
              dateformat::Union{AbstractString,Dates.DateFormat}=EMPTY_DATEFORMAT,

              footerskip::Int=0,
              rows_for_type_detect::Int=250,
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
    return CSV.Source(fullpath=fullpath, compression=compression,
                        options=CSV.Options(delim,quotechar,escapechar,separator,
                                    decimal,ascii(null),null!="",dateformat,dateformat==Dates.ISODateFormat),
                        header=header, datarow=datarow, types=types, footerskip=footerskip,
                        rows_for_type_detect=rows_for_type_detect, rows=rows, use_mmap=use_mmap)
end

function Source(;fullpath::Union{AbstractString,IO}="",
                compression="",
                options::CSV.Options=CSV.Options(),

                header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
                datarow::Int=-1, # by default, data starts immediately after header or start of file
                types::Union{Dict{Int,DataType},Vector{DataType}}=DataType[],

                footerskip::Int=0,
                rows_for_type_detect::Int=250,
                rows::Int=0,
                use_mmap::Bool=true)
    # compression="";delim=CSV.COMMA;quotechar=CSV.QUOTE;escapechar=CSV.ESCAPE;separator=CSV.COMMA;decimal=CSV.PERIOD;null="";header=1;datarow=2;types=DataType[];formats=UTF8String[];skipblankrows=true;footerskip=0;rows_for_type_detect=250;countrows=true
    # argument checks
    isa(fullpath,AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
    isa(header,Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

    isa(fullpath,IOStream) && (fullpath = chop(replace(fullpath.name,"<file ","")))

    # open the file for property detection; handle possible compression types
    if isa(fullpath,IOBuffer)
        source = fullpath
        fullpath = "<IOBuffer>"
    elseif isa(fullpath,IO)
        source = IOBuffer(readbytes(fullpath))
        fullpath = fullpath.name
    elseif compression in ZIP_FILE_EXTS || splitext(fullpath)[end] in ZIP_FILE_EXTS
        #TODO: detect zip formats?
        source = IOBuffer(readbytes(Libz.ZlibInflateInputStream(open(fullpath)))) # stored for reading data
    elseif compression == ""
        source = IOBuffer(use_mmap ? Mmap.mmap(fullpath) : open(readbytes,fullpath))
    else
        throw(ArgumentError("unsupported compression type: $compression"))
    end
    rows = rows == 0 ? CSV.countlines(source,options.quotechar,options.escapechar) : rows
    rows == 0 && throw(ArgumentError("No rows of data detected in $fullpath"))
    seekstart(source)

    datarow = datarow == -1 ? last(header) + 1 : datarow # by default, data starts on line after header
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
