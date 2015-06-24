# Validation Rules
 # Each row must contain the same number of columns
 # Each field must be formatted correctly and be of the right type and format, must be one of:
  # Empty, missing value
  # DATE
  # DATETIME
  # INTEGER
  # DOUBLE
  # STRING
  # quoted by f.quotechar, with f.escapechar allowed within f.quotechar to specify literal f.quotechar, f.delim, or f.newline

module CSV

using Compat
reload("Mmap")
import Mmap

typealias Str AbstractString

immutable CSVError <: Exception
    msg::ASCIIString
end

immutable NullField
end

immutable File
    fullpath::UTF8String
    delim::UInt8
    newline::UInt8
    quotechar::UInt8
    escapechar::UInt8
    headerpos::Int
    datapos::Int
    cols::Int
    header::Vector{UTF8String}
    types::Vector{DataType}
    formats::Vector{UTF8String}
    separator::UInt8
    null::UTF8String
end

function Base.show(io::IO,f::File)
    println(io,"fullpath: \"",f.fullpath,"\"")
    println(io,"delim: '",@compat(Char(f.delim)),"'")
    print_escaped(io,string("newline: '",@compat(Char(f.newline)),"'"),"")
    println(io,"\nquotechar: '",@compat(Char(f.quotechar)),"'")
    println(io,"escapechar: '\\",@compat(Char(f.escapechar)),"'")
    println(io,"headerpos: ",f.headerpos)
    println(io,"datapos: ",f.datapos)
    println(io,"cols: ",f.cols)
    println(io,"header: ",f.header)
    println(io,"types: ",f.types)
    println(io,"formats: ",f.formats)
    println(io,"separator: '",@compat(Char(f.separator)),"'")
    println(io,"null: \"",f.null,"\"")
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

function skipto!(f,cur,to,q,e,n)
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
        elseif b == n
            cur += 1
        end
    end
    return cur
end

const NEWLINE = @compat UInt8('\n')
const COMMA = @compat UInt8(',')
const QUOTE = @compat UInt8('"')
const ESCAPE = @compat UInt8('\\')

function readline(f::IOStream,q::UInt8,n::UInt8=NEWLINE)
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
        elseif b == n
            break
        end
    end
    return takebuf_string(buf)
end

function File{T<:Str}(fullpath::Str,
              delim=COMMA;
              newline=NEWLINE,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              numcols::Int=0,
              header::Vector{T}=UTF8String[],
              types::Vector{DataType}=DataType[],
              coltypes::Dict{Int,DataType}=Dict{Int,DataType}(),
              formats::Vector{T}=UTF8String[],
              colformats::Dict{Int,T}=Dict{Int,UTF8String}(),
              headerrow::Int=1,
              datarow::Int=headerrow+1,
              rows_for_type_detect::Int=250,
              separator=COMMA,
              null::Str="")
    # argument checks
    isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file"))
    datarow > headerrow || throw(ArgumentError("data row ($datarow) must come after header row ($headerrow)"))

    # make sure args are UInt8
    delim = delim % UInt8; newline = newline % UInt8;
    quotechar = quotechar % UInt8; escapechar = escapechar % UInt8;
    separator = separator % UInt8;

    if splitext(fullpath)[end] in ZIP_FILE_EXTS
        temp = tempname()
        unzipcmd = unzip(splitext(fullpath)[end],fullpath)
        run(pipe(pipe(unzipcmd,`head -$(rows_for_type_detect+headerrow)`);stdout=temp))
        f = open(temp)
    else
        f = open(fullpath)
    end

    CSV.skipto!(f,1,headerrow,quotechar,escapechar,newline)
    headerpos = position(f)
    cols = numcols == 0 ? length(split(readline(f,quotechar,newline),@compat(Char(delim)))) : numcols
    seek(f,headerpos)

    # if headerrow == 0, there is no header in the file itself
    # a header can be provided in the `header` argument, otherwise column names are auto-generated
    header = headerrow != 0 ? map(utf8,split(chomp(readline(f,quotechar,newline)),@compat(Char(delim)))) : 
                isempty(header) ? UTF8String["Column$i" for i = 1:cols] : map(utf8,header)
    
    seek(f,headerpos)
    CSV.skipto!(f,headerrow,datarow,quotechar,escapechar,newline)
    datapos = position(f)+1

    # Detect column types
    if isempty(formats)
        formats = Array(UTF8String,cols)
        fill!(formats,"")
    else
        length(formats) == cols || throw(ArgumentError("$cols number of columns detected; formats argument only has $(length(formats)) entries"))
    end
    if isempty(types)
        ts = Array(DataType,cols)
        poss_types = Array(DataType,rows_for_type_detect,cols)
        lineschecked = 0
        while !eof(f) && lineschecked < rows_for_type_detect
            vals = split(chomp(readline(f,quotechar,newline)),@compat(Char(delim)))
            lineschecked += 1
            for i = 1:cols
               poss_types[lineschecked,i], formats[i] = CSV.detecttype(vals[i],formats[i],@compat(Char(separator)),null)
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
            ts[i] = (isempty(d) || Str in d ) ? Str : (Date in d) ? Date : (DateTime in d) ? DateTime : (Float64 in d) ? Float64 : (Int in d) ? Int : Str
            empty!(d)
        end
    end

    types = isempty(types) ? ts : types
    if !isempty(coltypes)
        for (col,typ) in coltypes
            types[col] = typ
        end
    end
    if !isempty(colformats)
        for (col,format) in colformats
            formats[col] = format
        end
    end
    for i = 1:cols
        formats[i] = types[i] <: Dates.TimeType ? formats[i] : ""
    end
    return File(utf8(fullpath),
                delim,
                newline,
                quotechar,
                escapechar,
                headerpos,
                datapos,
                cols,
                header,
                types,
                formats,
                separator,
                utf8(null))
end

function detecttype(val,f::Str,sep::Char,null::Str)
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

# we need to keep a reference to our mmapped arrays for CStrings
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
    b = io.data[io.pos]
    io.pos += 1
    return b
end
Base.eof(io::CSV.Stream) = io.pos > length(io.data)
Base.position(io::CSV.Stream) = io.pos
Base.seek(io::CSV.Stream,i::Int) = (io.pos = i)

const SPACE     = @compat UInt8(' ')
const TAB       = @compat UInt8('\t')
const MINUS     = @compat UInt8('-')
const PLUS      = @compat UInt8('+')
const NEG_ONE   = @compat UInt8('0')-UInt8(1)
const ZERO      = @compat UInt8('0')
const TEN       = @compat UInt8('9')+UInt8(1)

@inline function readfield{T<:Integer}(io::CSV.Stream, ::Type{T}, row, col)
    pos = io.pos
    @inbounds begin
    b = read(io)
    while !eof(io) && (b == SPACE || b == TAB || b == io.file.quotechar)
        b = read(io)
    end
    if b == io.file.delim || b == io.file.newline
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
    if b == io.file.delim || b == io.file.newline || eof(io)
        return negative ? -v : v, false
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
    if b == io.file.delim || b == io.file.newline
        return NaN, true
    end
    ptr = pointer(io.data) + UInt(io.pos-2)
    v = ccall(:strtod, Float64, (Ptr{UInt8},Ptr{Ptr{UInt8}}), ptr, REF)
    io.pos += REF[1] - ptr
    b = unsafe_load(REF[1])
    if b == io.file.delim || b == io.file.newline || eof(io)
        return v, false
    else
        throw(CSV.CSVError("error parsing Float64 on row $row, column $col; parsed '$v' before encountering $(@compat(Char(b))) character"))
    end
end

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
        elseif b == io.file.delim || b == io.file.newline
            break
        end
    end
    if pos == position(io)-1
        return C_NULL, 0, true
    else
        return pointer(io.data)+Uint(pos-1), position(io)-pos-1, false
    end
end

@inline function readfield(io::CSV.Stream, ::Type{Date}, row, col)
    year = 0
    for i = 1:4
        year *= 10
        year += read(io) - ZERO
    end
    read(io)
    month = 0
    for i = 1:2
        month *= 10
        month += read(io) - ZERO 
    end
    read(io)
    day = 0
    for i = 1:2
        day *= 10
        day += read(io) - ZERO 
    end
    read(io)
    return Date(year,month,day), false
end

immutable CString <: AbstractString
    ptr::Ptr{UInt8}
    len::Int
end
const NULLSTRING = CString(C_NULL,0)
Base.print(io::IO, x::CString) = show(io,x)
Base.show(io::IO, x::CString) = print(io,x == NULLSTRING ? "NULL" : "\"$(bytestring(x.ptr,x.len))\"")
Base.endof(x::CString) = x.len
Base.string(x::CString) = x == NULLSTRING ? "" : bytestring(x.ptr,x.len)

function getfield{T<:Integer}(io, ::Type{T}, row, col, int, float)
    val, isnull = readfield(io, T, row, col)
    return ifelse(isnull,int,val)
end
function getfield(io, ::Type{Float64}, row, col, int, float)
    val, isnull = CSV.readfield(io, Float64, row, col)
    return ifelse(isnull,float,val)
end
function getfield{T<:AbstractString}(io, ::Type{T}, row, col, int, float)
    ptr, len, isnull = CSV.readfield(io,T,row,col)
    return ifelse(isnull,NULLSTRING,CString(ptr,len))
end
function getfield(io, ::Type{Date}, row, col, int, float)
    val, isnull = CSV.readfield(io, Date, row, col)
    return val
end

gettype(x) = x
gettype{T<:AbstractString}(::Type{T}) = CString

function Base.read(file::CSV.File;int::Int=typemin(Int),float::Float64=NaN)
    io = CSV.open(file)
    seek(io,file.datapos+1)
    rows = countlines(file.fullpath)-1
    cols = file.cols
    # pre-allocate
    result = Any[]
    for i = 1:cols
        push!(result,Array(CSV.gettype(file.types[i]), rows))
    end

    N = 1
    while !eof(io)
        for i = 1:cols
            result[i][N] = CSV.getfield(io, file.types[i], N, i, int, float)
            # println(CSV.getfield(io, file.types[i], N, i, int, float))
        end
        N += 1
    end
    return result
end

# function Base.countlines(file::CSV.File)
#     eol = file.newline
#     m = Mmap.mmap(file.fullpath)
#     lines = 0
#     for byte in m
#         lines += ifelse(byte == eol,1,0)
#     end
#     return lines+1
# end

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
