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
    println(io,"delim: '",Char(f.delim),"'")
    print_escaped(io,string("newline: '",Char(f.newline),"'"),"")
    println(io,"\nquotechar: '",Char(f.quotechar),"'")
    println(io,"escapechar: '\\",Char(f.escapechar),"'")
    println(io,"headerrow: ",f.headerpos)
    println(io,"datarow: ",f.datapos)
    println(io,"cols: ",f.cols)
    println(io,"header: ",f.header)
    println(io,"types: ",f.types)
    println(io,"formats: ",f.formats)
    println(io,"separator: '",Char(f.separator),"'")
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

function skipto!(f::Mmap.Stream,cur,to,q,e,n)
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

function File{T<:Str}(fullpath::Str,
              delim::UInt8=UInt8(',');
              newline::UInt8=UInt8('\n'),
              quotechar::UInt8=UInt8('"'),
              escapechar::UInt8=UInt8('\\'),
              numcols::Int=0,
              header::Vector{T}=UTF8String[],
              types::Vector{DataType}=DataType[],
              coltypes::Dict{Int,DataType}=Dict{Int,DataType}(),
              formats::Vector{T}=UTF8String[],
              colformats::Dict{Int,T}=Dict{Int,UTF8String}(),
              headerrow::Int=1,
              datarow::Int=headerrow+1,
              rows_for_type_detect::Int=250,
              separator::UInt8=UInt8(','),
              null::Str="")
    # argument checks
    isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file"))
    datarow > headerrow || throw(ArgumentError("data row ($datarow) must come after header row ($headerrow)"))

    if splitext(fullpath)[end] in ZIP_FILE_EXTS
        temp = tempname()
        unzipcmd = unzip(splitext(fullpath)[end],fullpath)
        run(pipe(pipe(unzipcmd,`head -$(rows_for_type_detect+headerrow)`);stdout=temp))
        f = Mmap.Stream(temp)
    else
        f = Mmap.Stream(fullpath)
    end

    CSV.skipto!(f,1,headerrow,quotechar,escapechar,newline)
    headerpos = position(f)
    cols = numcols == 0 ? length(split(readline(f,quotechar),Char(delim))) : numcols
    seek(f,headerpos)

    # if headerrow == 0, there is no header in the file itself
    # a header can be provided in the `header` argument, otherwise column names are auto-generated
    header = headerrow != 0 ? map(utf8,split(chomp(readline(f,quotechar)),Char(delim))) : 
                isempty(header) ? UTF8String["Column$i" for i = 1:cols] : map(utf8,header)
    
    seek(f,headerpos)
    CSV.skipto!(f,headerrow,datarow,quotechar,escapechar,newline)
    datapos = position(f)

    # Detect column types
    if isempty(formats)
        formats = Array(UTF8String,cols)
        fill!(formats,"")
    else
        length(formats) == cols || throw(ArgumentError("$cols number of columns detected; formats argument only has $(length(formats)) entries"))
    end
    if isempty(types)
        # we know delim, newline, quotechar, escapechar
        ts = Array(DataType,cols)
        poss_types = Array(DataType,rows_for_type_detect,cols)
        lineschecked = 0
        while !eof(f) && lineschecked < rows_for_type_detect
            vals = split(chomp(readline(f,quotechar)),Char(delim))
            lineschecked += 1
            for i = 1:cols
               poss_types[lineschecked,i], formats[i] = CSV.detecttype(vals[i],formats[i],Char(separator),null)
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

function detecttype(val::Str,f::Str,sep::Char,null::Str)
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