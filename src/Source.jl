const ZIP_FILE_EXTS = Set{UTF8String}([".zip",".gz","-gz",".z","-z","_z",".Z"#=,".bz2", ".bz", ".tbz2", ".tbz"=#])
const EMPTY_DATEFORMAT = Dates.DateFormat("")

Base.close(::Libz.BufferedStreams.BufferedInputStream{Libz.Source{:inflate,Libz.BufferedStreams.BufferedInputStream{IOStream}}}) = return nothing

#TODO: make .data field a type parameter? ultimate source might be HTTP Stream or other IO object
# would need to refactor readfields to somehow operate on readline or something
type Source <: IOSource # <: IO
    fullpath::UTF8String

    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    separator::UInt8
    decimal::UInt8
    null::ASCIIString # how null is represented in the dataset
    nullcheck::Bool   # do we have a custom null value to check for
    schema::Schema
    dateformat::Dates.DateFormat

    data::Vector{UInt8} # mmapped array, or entire IOStream/gzipped file read into Vector{UInt8}
    ptr::Int # represents the current index position in `data`; i.e. 1-based
end

function Base.show(io::IO,f::Source)
    println(io,"fullpath: \"",f.fullpath,"\"")

    println(io,"delim: '",@compat(Char(f.delim)),"'")
    println(io,"quotechar: '",@compat(Char(f.quotechar)),"'")
    println(io,"escapechar: '\\",@compat(Char(f.escapechar)),"'")
    # println(io,"separator: '",@compat(Char(f.separator)),"'")
    # println(io,"decimal: '",@compat(Char(f.decimal)),"'")
    println(io,"null: \"",f.null,"\"")
    println(io,"schema: ",f.schema)
    println(io,"dateformat: ",f.dateformat)
end

# IO interface
@inline function Base.read(io::CSV.Source, ::Type{UInt8}=UInt8)
    @inbounds b = io.data[io.ptr]
    io.ptr += 1
    return b
end
# These are a little unsafe at the moment, but that's for performance and they're not really meant for public consumption, so traveller's beware
Base.peek(io::CSV.Source) = (@inbounds b = io.data[io.ptr]; return b)
Base.eof(io::CSV.Source) = io.ptr > length(io.data)
Base.position(io::CSV.Source) = io.ptr - 1
Base.seek(io::CSV.Source,i::Int) = (io.ptr = i)
Base.size(io::CSV.Source) = size(io.schema)
Base.readline(io::CSV.Source) = readline(io,io.quotechar,io.escapechar)
readsplitline(io::CSV.Source) = readsplitline(io,io.delim,io.quotechar,io.escapechar)
Base.countlines(io::CSV.Source) = countlines(io,io.quotechar,io.escapechar)

# Constructor
function Source(fullpath::AbstractString;
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
              rows::Int=0)
    # compression="";delim=CSV.COMMA;quotechar=CSV.QUOTE;escapechar=CSV.ESCAPE;separator=CSV.COMMA;decimal=CSV.PERIOD;null="";header=1;datarow=2;types=DataType[];formats=UTF8String[];skipblankrows=true;footerskip=0;rows_for_type_detect=250;countrows=true
    # argument checks
    isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file"))
    isa(header,Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

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

    # open the file for property detection; handle possible compression types
    if compression in ZIP_FILE_EXTS || splitext(fullpath)[end] in ZIP_FILE_EXTS
        #TODO: detect zip formats?
        temp = Libz.ZlibInflateInputStream(open(fullpath)) # used for countlines
        temp2 = Libz.ZlibInflateInputStream(open(fullpath)) # used for type detection
        source = readbytes(Libz.ZlibInflateInputStream(open(fullpath))) # stored for reading data
    elseif compression == ""
        temp = open(fullpath)
        temp2 = open(fullpath)
        source = Mmap.mmap(fullpath)
    else
        throw(ArgumentError("unsupported compression type: $compression"))
    end
    rows = rows == 0 ? countlines(temp,quotechar,escapechar) : rows
    rows == 0 && throw(ArgumentError("No rows of data detected in $fullpath"))
    close(temp)
    f = IOBuffer()
    bufferedrows = 0
    for i = 1:(rows < 0 ? rows_for_type_detect : min(rows,rows_for_type_detect))
        write(f,readline(temp2,quotechar,escapechar))
        bufferedrows += 1
        eof(temp2) && break
    end
    close(temp2)
    seekstart(f)

    datarow = datarow == -1 ? last(header) + 1 : datarow # by default, data starts on line after header
    rows = rows - datarow + 1 - footerskip # rows now equals the actual number of rows in the dataset

    # figure out # of columns and header, either an Integer, Range, or Vector{String}
    # also ensure that `f` is positioned at the start of data
    cols = 0
    if isa(header,Integer)
        # default header = 1
        CSV.skipto!(f,1,header,quotechar,escapechar)
        potential_header = UTF8String[utf8(x) for x in readsplitline(f,delim,quotechar,escapechar)]
        cols = length(potential_header)
        columnnames = header <= 0 ? UTF8String["Column$i" for i = 1:cols] : potential_header
        datarow != header+1 && CSV.skipto!(f,header+1,datarow,quotechar,escapechar)
        datapos = position(f)+1
    elseif isa(header,Range)
        CSV.skipto!(f,1,first(header),quotechar,escapechar)
        columnnames = UTF8String[utf8(x) for x in readsplitline(f,delim,quotechar,escapechar)]
        cols = length(columnnames)
        for row = first(header):(last(header)-1)
            for (i,c) in enumerate(UTF8String[utf8(x) for x in readsplitline(f,delim,quotechar,escapechar)])
                columnnames[i] *= "_" * c
            end
        end
        datarow != last(header)+1 && CSV.skipto!(f,last(header)+1,datarow,quotechar,escapechar)
        datapos = position(f)+1
    else
        CSV.skipto!(f,1,datarow,quotechar,escapechar)
        datapos = position(f)+1
        cols = length(readsplitline(f,delim,quotechar,escapechar))
        seek(f,datapos-1)
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
        while !eof(f) && lineschecked < min(rows,rows_for_type_detect)
            vals = readsplitline(f,delim,quotechar,escapechar)
            lineschecked += 1
            for i = 1:cols
               poss_types[lineschecked,i] = CSV.detecttype(vals[i],dateformat,null)
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
            columntypes[i] = (isempty(d) || AbstractString in d ) ? AbstractString :
                                (Date     in d) ? Date :
                                (DateTime in d) ? DateTime :
                                (Float64  in d) ? Float64 :
                                (Int      in d) ? Int : AbstractString
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
    (any(columntypes .== DateTime) || any(columntypes .== Date)) && dateformat == EMPTY_DATEFORMAT && (dateformat = Dates.ISODateFormat)
    return Source(utf8(fullpath),delim,quotechar,escapechar,separator,decimal,ascii(null),null != "",
                Schema(columnnames,columntypes,rows,cols),dateformat,source,datapos)
end

# used only during the type detection process
immutable NullField end

function detecttype(val::AbstractString,format,null)
    (val == "" || val == null) && return NullField
    val2 = replace(val, @compat(Char(COMMA)), "") # remove potential comma separators from integers
    t = tryparse(Int,val2)
    !isnull(t) && return Int
    # our strtod only works on period decimal points (e.g. "1.0")
    t = tryparse(Float64,val2)
    !isnull(t) && return Float64
    if format != EMPTY_DATEFORMAT
        try # it might be nice to throw an error when a format is specifically given but doesn't parse
            Date(val,format)
            return Date
        end
        try
            DateTime(val,format)
            return DateTime
        end
    else
        try
            Date(val)
            return Date
        end
        try
            DateTime(val)
            return DateTime
        end
    end
    return AbstractString
end
