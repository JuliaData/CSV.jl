module CSV

using Compat, NullableArrays

immutable CSVError <: Exception
    msg::ASCIIString
end

# used only during the type detection process
immutable NullField
end

const ZIP_FILE_EXTS = Set{UTF8String}([".zip",".gz","-gz",".z","-z","_z",".Z",".bz2", ".bz", ".tbz2", ".tbz"])

function unzip(ext,file)
    if ext in (".Z",".gz",".zip")
        return `gzip -dc $file`
    elseif ext in (".bz2", ".bz", ".tbz2", ".tbz")
        return `bzip2 -dck $file`
    else
        throw(ArgumentError("ERROR: unsupported compression file format: $ext"))
    end
    return cmd
end

const RETURN  = @compat UInt8('\r')
const NEWLINE = @compat UInt8('\n')
const COMMA   = @compat UInt8(',')
const QUOTE   = @compat UInt8('"')
const ESCAPE  = @compat UInt8('\\')
const PERIOD  = @compat UInt8('.')
const SPACE   = @compat UInt8(' ')
const TAB     = @compat UInt8('\t')
const MINUS   = @compat UInt8('-')
const PLUS    = @compat UInt8('+')
const NEG_ONE = @compat UInt8('0')-UInt8(1)
const ZERO    = @compat UInt8('0')
const TEN     = @compat UInt8('9')+UInt8(1)
Base.isascii(c::UInt8) = c < 0x80

# accept URLs?
# ignore columns
type File
    fullpath::UTF8String
    compression::UTF8String

    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    separator::UInt8
    decimal::UInt8
    null::UTF8String

    cols::Int
    header::Vector{UTF8String}
    types::Vector{DataType}
    formats::Vector{UTF8String}

    datapos::Int
    datarow::Int
    skipblankrows::Bool
    footerskip::Int
end

function Base.show(io::IO,f::File)
    println(io,"fullpath: \"",f.fullpath,"\"")
    println(io,"compression: ",f.compression)

    println(io,"delim: '",@compat(Char(f.delim)),"'")
    println(io,"quotechar: '",@compat(Char(f.quotechar)),"'")
    println(io,"escapechar: '\\",@compat(Char(f.escapechar)),"'")
    println(io,"separator: '",@compat(Char(f.separator)),"'")
    println(io,"decimal: '",@compat(Char(f.decimal)),"'")
    println(io,"null: \"",f.null,"\"")

    println(io,"cols: ",f.cols)
    println(io,"header: ",f.header)
    println(io,"types: ",f.types)
    println(io,"formats: ",f.formats)

    println(io,"datapos: ",f.datapos)
    println(io,"skipblankrows: ",f.skipblankrows)
    println(io,"footerskip: ",f.footerskip)
end

function File{T<:AbstractString}(fullpath::AbstractString;
              compression="",

              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              separator=COMMA,
              decimal=PERIOD,
              null::AbstractString="",

              header::Union(Integer,UnitRange{Int},Vector{T})=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=2, # by default, data starts immediately after header or start of file
              types::Union(Dict{Int,DataType},Vector{DataType})=DataType[],
              formats::Union(Dict{Int,T},Vector{T})=UTF8String[],

              skipblankrows::Bool=true,
              footerskip::Int=0,
              rows_for_type_detect::Int=250)
    # argument checks
    isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file"))
    isa(header,Integer) && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

    # open the file for property detection; handle possible compression types
    if compression in ZIP_FILE_EXTS || splitext(fullpath)[end] in ZIP_FILE_EXTS
        temp = tempname()
        splitext(fullpath)[end] in ZIP_FILE_EXTS && (compression = splitext(fullpath)[end])
        unzipcmd = unzip(splitext(fullpath)[end],fullpath)
        run(pipe(pipe(unzipcmd,`head -$(rows_for_type_detect+datarow)`);stdout=temp))
        f = open(temp)
        rm(temp)
    elseif compression == ""
        f = open(fullpath)
    else
        throw(ArgumentError("unsupported compression type: $compression"))
    end

    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))
    isascii(decimal) || throw(ArgumentError("non-ASCII characters not supported for decimal argument: $decimal"))
    isascii(separator) || throw(ArgumentError("non-ASCII characters not supported for separator argument: $separator"))
    delim = delim % UInt8
    quotechar = quotechar % UInt8
    escapechar = escapechar % UInt8
    decimal = decimal % UInt8
    separator = separator % UInt8

    # figure out # of columns and header, either an Integer, Range, or Vector{String}
    # also ensure that `f` is positioned at the start of data
    cols = 0
    if isa(header,Integer)
        CSV.skipto!(f,1,header,quotechar,escapechar)
        headerpos = position(f)
        cols = length(split(readline(f,quotechar),@compat(Char(delim))))
        seek(f,headerpos)
        columnnames = map(utf8,split(readline(f,quotechar),@compat(Char(delim))))
        datarow != 0 && CSV.skipto!(f,header+1,datarow,quotechar,escapechar)
        datapos = position(f)
    elseif isa(header,Range)
        CSV.skipto!(f,1,first(header),quotechar,escapechar)
        headerpos = position(f)
        cols = length(split(readline(f,quotechar),@compat(Char(delim))))
        seek(f,headerpos)
        columnames = fill(utf8(""),cols)
        for row in header
            for (i,c) in map(utf8,split(readline(f,quotechar),@compat(Char(delim))))
                columnames[i] *= "_" * c
            end
        end
        datarow != 0 && CSV.skipto!(f,last(header)+1,datarow,quotechar,escapechar)
        datapos = position(f)
    else
        datarow = datarow == 0 ? 1 : datarow
        CSV.skipto!(f,1,datarow,quotechar,escapechar)
        datapos = position(f)
        cols = length(split(readline(f,quotechar),@compat(Char(delim))))
        seek(f,datapos)
        if isempty(header)
            columnnames = ["Column$i" for i = 1:cols]
        else
            length(header) == cols || throw(ArgumentError("length of provided header doesn't match number of columns of data at row $datarow"))
            columnnames = header
        end
    end

    # Detect column types
    columnformats = fill(utf8(""),cols)
    if isa(formats,Vector) && length(formats) == cols
        columnformats = formats
    elseif isa(formats,Dict) || isempty(formats)
        for (k,v) in formats
            columnformats[k] = v
        end
    else
        throw(ArgumentError("$cols number of columns detected; `formats` argument has $(length(formats)) entries"))
    end
    columntypes = Array(DataType,cols)
    if isa(types,Vector) && length(types) == cols
        columntypes = types
    elseif isa(types,Dict) || isempty(types)
        poss_types = Array(DataType,rows_for_type_detect,cols)
        lineschecked = 0
        while !eof(f) && lineschecked < rows_for_type_detect
            vals = split(readline(f,quotechar),@compat(Char(delim)))
            lineschecked += 1
            for i = 1:cols
               poss_types[lineschecked,i], columnformats[i] = CSV.detecttype(vals[i],columnformats[i],@compat(Char(separator)),null)
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
            columptypes[col] = typ
        end
    end
    return File(utf8(fullpath),compression,
                delim,quotechar,escapechar,separator,decimal,utf8(null),
                cols,columnnames,columntypes,columnformats,
                datapos,datarow,skipblankrows,footerskip)
end

# for potentially embedded newlines in quoted fields
function readline(f::IOStream,q::UInt8)
    buf = IOBuffer()
    while !eof(f)
        b = read(f, UInt8)
        write(buf, b)
        if b == q
            while !eof(f)
                b = read(f, UInt8)
                write(buf, b)
                b == q && break
            end
        elseif b == NEWLINE
            break
        elseif b == RETURN
            mark(f)
            read(f, UInt8) != NEWLINE && reset(f)
            break
        end
    end
    return takebuf_string(buf)
end

# for skipping from `cur` line number to `to`
function skipto!(f,cur,to,q,e)
    while !eof(f)
        cur == to && break
        b = read(f, UInt8)
        if b == q
            while !eof(f)
                b = read(f, UInt8)
                if b == e
                    b = read(f, UInt8)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            cur += 1
        elseif b == RETURN
            mark(f)
            read(f, UInt8) != NEWLINE && reset(f)
            cur += 1
        end
    end
    return cur
end

function detecttype(val,f::AbstractString,sep::Char,null::AbstractString)
    (val == "" || val == null) && return NullField, ""
    val2 = replace(val, sep, "")
    t = tryparse(Int,val2)
    !isnull(t) && return Int, ""
    t = tryparse(Float64,val2)
    !isnull(t) && return Float64, ""
    if f != ""
        try
            Date(val,f)
            return Date, f
        end
        try
            DateTime(val,f)
            return DateTime, f
        end
    end
    return Str, ""
end

# we need to keep a reference to our mmapped arrays for Strs
const GLOBALREF = []

type Stream <: IO
    file::CSV.File
    data::Vector{UInt8} # Mmap.Array
    pos::Int
end
function Base.open(file::CSV.File)
    m = Mmap.mmap(file.fullpath)
    push!(GLOBALREF,m)
    Stream(file,m,1)
end
# CSV.open(file::AbstractString) = Stream(CSV.File(file),mmap(file),1)

@inline function Base.read(io::CSV.Stream, ::Type{UInt8}=UInt8)
    @inbounds b = io.data[io.pos]
    io.pos += 1
    return b
end
peek(io::CSV.Stream) = (@inbounds b = io.data[io.pos]; return b)
Base.eof(io::CSV.Stream) = io.pos > length(io.data)
Base.position(io::CSV.Stream) = io.pos
Base.seek(io::CSV.Stream,i::Int) = (io.pos = i)
function skipn!(f::CSV.Stream,n,q,e)
    cur = 0
    while !eof(f)
        cur == n && break
        b = read(f)
        if b == q
            while !eof(f)
                b = read(f)
                if b == e
                    b = read(f)
                elseif b == q
                    break
                end
            end
        elseif b == NEWLINE
            cur += 1
        elseif b == RETURN
            peek(f) == NEWLINE && (f.pos+=1)
            cur += 1
        end
    end
    return cur
end

immutable Str <: AbstractString
    ptr::Ptr{UInt8}
    len::Int
end
const NULLSTRING = Str(C_NULL,0)
Base.print(io::IO, x::Str) = show(io,x)
Base.show(io::IO, x::Str) = print(io,x == NULLSTRING ? "" : "\"$(bytestring(x.ptr,x.len))\"")
Base.endof(x::Str) = x.len
Base.string(x::Str) = x == NULLSTRING ? "" : bytestring(x.ptr,x.len)

@inline function readfield{T<:Integer}(io::CSV.Stream, ::Type{T}, row, col)
    pos = io.pos
    @inbounds begin
    b = read(io)
    while !eof(io) && (b == SPACE || b == TAB || b == io.file.quotechar)
        b = read(io)
    end
    if b == io.file.delim || b == NEWLINE
        return zero(T), true
    elseif b == RETURN
        peek(io) == NEWLINE && (io.pos+=1)
        return zero(T), true
    end
    negative = false
    if b == MINUS
        negative = true
        b = read(io)
    elseif b == PLUS
        b = read(io)
    end
    v = zero(T)
    while NEG_ONE < b < TEN
        # process digits
        v *= 10
        v += b - ZERO
        eof(io) && break
        b = read(io)
    end
    end # @inbounds
    if b == io.file.delim || b == NEWLINE || eof(io)
        return ifelse(negative,-v,v), false
    elseif b == RETURN
        peek(io) == NEWLINE && (io.pos+=1)
        return ifelse(negative,-v,v), false
    else
        throw(CSV.CSVError("error parsing $T on column $col, row $row; parsed $v before encountering $(@compat(Char(b))) character"))
    end
end

const REF = Array(Ptr{UInt8},1)

@inline function readfield(io::CSV.Stream, ::Type{Float64}, row, col)
    b = read(io)
    while !eof(io) && (b == CSV.SPACE || b == CSV.TAB || b == io.file.quotechar)
        b = read(io)
    end
    if b == io.file.delim || b == NEWLINE
        return NaN, true
    elseif b == RETURN
        peek(io) == NEWLINE && (io.pos+=1)
        return NaN, true
    end
    ptr = pointer(io.data) + UInt(io.pos-2)
    v = ccall(:strtod, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF)
    io.pos += REF[1] - ptr
    b = io.data[io.pos-1]
    if b == io.file.delim || b == NEWLINE || eof(io)
        return v, false
    elseif b == RETURN
        peek(io) == NEWLINE && (io.pos+=1)
        return v, false
    else
        throw(CSV.CSVError("error parsing Float64 on row $row, column $col; parsed '$v' before encountering $(@compat(Char(b))) character"))
    end
end

const UINT8NULL = convert(Ptr{UInt8},C_NULL)

@inline function readfield{T<:AbstractString}(io::CSV.Stream, ::Type{T}, row, col)
    pos = position(io)
    @inbounds while !eof(io)
        b = read(io)
        if b == io.file.quotechar
            while !eof(io)
                b = read(io)
                if b == io.file.escapechar
                    b = read(io)
                elseif b == io.file.quotechar
                    break
                end
            end
        elseif b == io.file.delim || b == NEWLINE
            break
        elseif b == RETURN
            peek(io) == NEWLINE && (io.pos+=1)
            break
        end
    end
    if pos == position(io)-1
        return NULLSTRING, true
    else
        return Str(pointer(io.data)+Uint(pos-1), position(io)-pos-1), false
    end
end

itr(io,n,val) = (for i = 1:n; val += 10; val += read(io) - ZERO; end; return val)

@inline function readfield(io::CSV.Stream, ::Type{Date}, row, col)
    year = itr(io,4,0)
    read(io)
    month = itr(io,2,0)
    read(io)
    day = itr(io,2,0)
    read(io)
    return Date(year,month,day), false
end

gettype(x) = x
gettype{T<:AbstractString}(::Type{T}) = Str

# add skip, limit, ignore columns
function Base.read(file::CSV.File)
    io = CSV.open(file)
    seek(io,file.datapos+1)
    rows = countlines(file.fullpath)
    N = file.datarow
    cols = file.cols
    result = Any[]
    for i = 1:cols
        push!(result,NullableArray(CSV.gettype(file.types[i]), rows - N + 1))
    end
    ind = 1
    while !eof(io)
        for i = 1:cols
            @inbounds result[i][ind] = CSV.readfield(io, file.types[i], N, i)
        end
        N += 1; ind += 1
        b = peek(io)
        empty = b == NEWLINE || b == RETURN
        if empty
            N == rows && break
            file.skipblankrows && skipn!(io,1,file.quotechar,file.escapechar)
        end
    end
    return result
end

function Base.setindex!{T}(a::NullableVector{T},x::Tuple{T,Bool},i::Int)
    a.values[i], a.isnull[i] = x
    return a
end
# validatetype{T<:Str}(value::Str,::Type{T},f,sep,null,r,c) = return
# function validatetype{T<:Real}(value::Str,::Type{T},f,sep,null,row,col)
#     (value == "" || value == null) && return
#     t = tryparse(T,replace(value,sep,""))
#     isnull(t) && throw(CSVError("'$value' is not a valid $T value on row: $row, column: $col"))
#     return
# end

# const FORMAT_CHARS = Set{Char}(['y','m','u','U','d','H','M','S','s','E','e'])

# function validatetype{T<:TimeType}(value::Str,::Type{T},f,sep,null,row,col)
#     (value == "" || value == null) && return
#     vlen = length(value)+1
#     flen = length(f)+1
#     vpos = fpos = 1
#     v = value[chr2ind(value,vpos)]
#     c = f[chr2ind(f,fpos)]
#     while true
#         if c in FORMAT_CHARS
#             while isdigit(v)
#                 vpos += 1
#                 vpos < vlen || break
#                 v = value[chr2ind(value,vpos)]
#             end
#             while c in FORMAT_CHARS
#                 fpos += 1
#                 fpos < flen || break
#                 c = f[chr2ind(f,fpos)]
#             end
#             (vpos == vlen || fpos == vlen) && break
#         else
#             c == v || throw(CSVError("'$value' is not a valid $T on row: $row, column: $col according to format: $f"))
#             vpos += 1
#             fpos += 1
#             (vpos < vlen && fpos < flen) || break
#             v = value[chr2ind(value,vpos)]
#             c = f[chr2ind(f,fpos)]
#         end
#     end
#     return
# end

# function validateline!(file,f,c,i,buf,::Type{Val{false}})
#     fieldsfound = 1
#     while !eof(f)
#         CSV.read!(f,c)
#         if c[] == file.quotechar
#             CSV.handlequoted(f,c,file.quotechar,file.escapechar)
#         elseif c[] == file.delim
#             fieldsfound += 1
#         elseif c[] == file.newline
#             break
#         end
#     end
#     fieldsfound == file.cols || throw(CSVError("error parsing line $i: expected $(file.cols) fields, only detected $fieldsfound"))
# end

# function validateline!(file,f,c,i,buf,::Type{Val{true}})
#     ts = file.types
#     fs = file.formats
#     fieldsfound = 1
#     while !eof(f)
#         read!(f,c)
#         if c[] == file.quotechar # if we run into a quote character
#             while !eof(f) # keep reading until we reach the closing q
#                 read!(f,c)
#                 # if we run into an escape character
#                 if c[] == file.escapechar
#                     write(buf,c[])
#                     read!(f,c)
#                     write(buf,c[]) # auto read the next Char
#                 elseif c[] == file.quotechar
#                     break
#                 end
#                 write(buf,c[])
#             end
#         elseif c[] == file.delim # if we've found a delimiter
#             validatetype(takebuf_string(buf),ts[fieldsfound],fs[fieldsfound],file.separator,file.null,i,fieldsfound) #TODO
#             fieldsfound += 1
#         elseif c[] == file.newline
#             break
#         else
#             write(buf,c[])
#         end
#     end
#     validatetype(takebuf_string(buf),ts[fieldsfound],fs[fieldsfound],file.separator,file.null,i,fieldsfound)
#     fieldsfound == file.cols || throw(CSVError("error parsing line $i: expected $(file.cols) fields, only detected $fieldsfound"))
# end

# @inline function handlequoted(f,c,q,e)
#     while !eof(f)
#         CSV.read!(f,c)
#         if c[] == e
#             CSV.read!(f,c)
#         elseif c[] == q
#             break
#         end
#     end
#     return
# end

# function skiplinesto!(f,c,i,n,q,e,newline)
#     while !eof(f)
#         i == n && break
#         CSV.read!(f,c)
#         if c[] == q
#             CSV.handlequoted(f,c,q,e)
#         elseif c[] == newline
#             i += 1
#         end
#     end
#     return i
# end

# # if size of buf is fixed, it needs to be at least as big as the largest expected field (value for a single column) in file
# validate(f::Str;verbose::Bool=true,checktypes::Bool=true) = validate(File(f);verbose=verbose,checktypes=checktypes)
# function validate(file::File,buf::IOBuffer=IOBuffer(1024);verbose::Bool=true,checktypes::Bool=true)
#     f = open(file.fullpath)
#     c = Ref{Char}()
#     n = file.newline
#     t = checktypes ? Val{true} : Val{false}
#     i = 1
#     if file.headerrow != 0
#         i = skiplinesto!(f,c,i,file.headerrow,file.quotechar,file.escapechar,file.newline)
#         CSV.validateline!(file,f,c,file.headerrow,buf,Val{false})
#         i += 1
#     end
#     i = skiplinesto!(f,c,i,file.datarow,file.quotechar,file.escapechar,file.newline)
#     while !eof(f)
#         CSV.validateline!(file,f,c,i,buf,t)
#         verbose && i % 100000 == 0 && println("Validated $i rows...")
#         i += 1
#     end
#     return nothing
# end

end # module
