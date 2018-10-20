using DataStreams, DataFrames

import Parsers: readbyte, peekbyte

substitute(::Type{Union{T, Missing}}, ::Type{T1}) where {T, T1} = Union{T1, Missing}
substitute(::Type{T}, ::Type{T1}) where {T, T1} = T1
substitute(::Type{Missing}, ::Type{T1}) where {T1} = Missing

"""
Represents the various configuration settings for delimited text file parsing.

Keyword Arguments:

 * `delim::Union{Char,UInt8}`: how fields in the file are delimited; default `','`
 * `quotechar::Union{Char,UInt8}`: the character that indicates a quoted field that may contain the `delim` or newlines; default `'"'`
 * `escapechar::Union{Char,UInt8}`: the character that escapes a `quotechar` in a quoted field; default `'\\'`
 * `missingstring::String`: indicates how missing values are represented in the dataset; default `""`
 * `dateformat::Union{AbstractString,Dates.DateFormat}`: how dates/datetimes are represented in the dataset; default `Base.Dates.ISODateTimeFormat`
 * `decimal::Union{Char,UInt8}`: character to recognize as the decimal point in a float number, e.g. `3.14` or `3,14`; default `'.'`
 * `truestring`: string to represent `true::Bool` values in a csv file; default `"true"`. Note that `truestring` and `falsestring` cannot start with the same character.
 * `falsestring`: string to represent `false::Bool` values in a csv file; default `"false"`
 * `internstrings`: whether strings should be interned, rather than creating a new object for each string field; default `true`
"""
struct Options{D}
    delim::UInt8
    quotechar::UInt8
    escapechar::UInt8
    missingstring::Vector{UInt8}
    null::Union{Vector{UInt8},Nothing} # deprecated
    missingcheck::Bool
    dateformat::D
    decimal::UInt8
    truestring::Vector{UInt8}
    falsestring::Vector{UInt8}
    internstrings::Bool
    # non-public for now
    datarow::Int
    rows::Int
    header::Union{Integer,UnitRange{Int},Vector}
    types
end

function Options(;delim=UInt8(','), quotechar=UInt8('"'), escapechar=UInt8('\\'), missingstring="", null=nothing, dateformat=nothing, decimal=UInt8('.'), truestring="true", falsestring="false", internstrings=true, datarow=-1, rows=0, header=1, types=Type[])
    Base.depwarn("CSV.Options is deprecated and will be removed in a future release; please use CSV.File and CSV.write directly", nothing)
    return Options(delim%UInt8, quotechar%UInt8, escapechar%UInt8,
            map(UInt8, collect(ascii(String(missingstring)))), null === nothing ? nothing : map(UInt8, collect(ascii(String(null)))), missingstring != "" || (null != "" && null != nothing),
            isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat,
            decimal%UInt8, map(UInt8, collect(truestring)), map(UInt8, collect(falsestring)), internstrings, datarow, rows, header, types)
end
function Base.show(io::IO,op::Options)
    println(io, "    CSV.Options:")
    println(io, "        delim: '", Char(op.delim), "'")
    println(io, "        quotechar: '", Char(op.quotechar), "'")
    print(io, "        escapechar: '"); escape_string(io, string(Char(op.escapechar)), "\\"); println(io, "'")
    print(io, "        missingstring: \""); escape_string(io, isempty(op.missingstring) ? "" : String(collect(op.missingstring)), "\\"); println(io, "\"")
    println(io, "        dateformat: ", something(op.dateformat, ""))
    println(io, "        decimal: '", Char(op.decimal), "'")
    println(io, "        truestring: '$(String(op.truestring))'")
    println(io, "        falsestring: '$(String(op.falsestring))'")
    print(io, "        internstrings: $(op.internstrings)")
end

"""
A type that satisfies the `Data.Source` interface in the `DataStreams.jl` package.

A `CSV.Source` can be manually constructed in order to be re-used multiple times.

`CSV.Source(file_or_io; kwargs...) => CSV.Source`

Note that a filename string can be provided or any `IO` type. For the full list of supported
keyword arguments, see the docs for [`CSV.read`](@ref) or type `?CSV.read` at the repl

An example of re-using a `CSV.Source` is:
```julia
# manually construct a `CSV.Source` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.Source(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
mutable struct Source{P, I, DF, D} <: Data.Source
    schema::Data.Schema
    parsinglayers::P
    io::I
    bools::Parsers.Trie
    dateformat::DF
    decimal::UInt8
    fullpath::String
    datapos::D # the position in the IOBuffer where the rows of data begins or columnpositions for transposed files
    pools::Vector{CategoricalPool{String, UInt32, CategoricalString{UInt32}}}
end

function Base.show(io::IO, f::Source)
    println(io, "CSV.Source: ", f.fullpath)
    println(io, f.io)
    show(io, f.schema)
end

"""
A type that satisfies the `Data.Sink` interface in the `DataStreams.jl` package.

A `CSV.Sink` can be manually constructed in order to be re-used multiple times.

`CSV.Sink(file_or_io; kwargs...) => CSV.Sink`

Note that a filename string can be provided or any `IO` type. For the full list of supported
keyword arguments, see the docs for [`CSV.write`](@ref) or type `?CSV.write` at the repl

An example of re-using a `CSV.Sink` is:
```julia
# manually construct a `CSV.Source` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.Source(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
mutable struct Sink{D, B} <: Data.Sink
    options::Options{D}
    io::IOBuffer
    fullpath::Union{String, IO}
    datapos::Int # the position in the IOBuffer where the rows of data begins
    header::Bool
    colnames::Vector{String}
    cols::Int
    append::Bool
    quotefields::B
end

makedf(df::DateFormat) = df
makedf(df::String) = DateFormat(df)
makedf(df::Nothing) = df
makestr(d::UInt8) = string(Char(d))
makestr(d::Char) = string(d)
makestr(d::String) = d

readline!(io::IO; delim=",") = readline!(Parsers.Delimited(Parsers.Quoted(), delim; newline=true), io)
readline!(s::Source) = readline!(s.parsinglayers, s.io)
function countlines(io::IO, q::UInt8, e::UInt8)
    nl = 1
    b = 0x00
    while !eof(io)
        b = readbyte(io)
        if b === q
            while !eof(io)
                b = readbyte(io)
                if b === e
                    if eof(io)
                        break
                    elseif e === q && peekbyte(io) != q
                        break
                    end
                    b = readbyte(io)
                elseif b === q
                    break
                end
            end
        elseif b === UInt8('\n')
            nl += 1
        elseif b === UInt8('\r')
            nl += 1
            !eof(io) && peekbyte(io) === UInt8('\n') && readbyte(io)
        end
    end
    return nl - (b === UInt8('\n') || b === UInt8('\r'))
end
countlines(io::IO, q='"', e='\\') = countlines(io, UInt8(q), UInt8(e))

promote_type1(T::Type{<:Any}, ::Type{Any}) = T
promote_type1(::Type{Any}, T::Type{<:Any}) = T
# same types
promote_type1(::Type{T}, ::Type{T}) where {T} = T
# if we come across a Missing field, turn that column type into a Union{T, Missing}
promote_type1(T::Type{<:Any}, ::Type{Missing}) = Union{T, Missing}
promote_type1(::Type{Missing}, T::Type{<:Any}) = Union{T, Missing}
# these definitions allow Union{Int64, Missing} to promote to Union{Float64, Missing}
promote_type1(::Type{Union{T, Missing}}, ::Type{S}) where {T, S} = Union{promote_type1(T, S), Missing}
promote_type1(::Type{S}, ::Type{Union{T, Missing}}) where {T, S} = Union{promote_type1(T, S), Missing}
promote_type1(::Type{Union{T, Missing}}, ::Type{Union{S, Missing}}) where {T, S} = Union{promote_type1(T, S), Missing}
promote_type1(::Type{Union{String, Missing}}, ::Type{String}) = Union{String, Missing}
promote_type1(::Type{String}, ::Type{Union{String, Missing}}) = Union{String, Missing}
# basic promote type definitions from Base
promote_type1(::Type{Int64}, ::Type{Float64}) = Float64
promote_type1(::Type{Float64}, ::Type{Int64}) = Float64
promote_type1(::Type{Date}, ::Type{DateTime}) = DateTime
promote_type1(::Type{DateTime}, ::Type{Date}) = DateTime
# for cases when our current type can't widen, just promote to WeakRefString
promote_type1(::Type{<:Real}, ::Type{<:Dates.TimeType}) = String
promote_type1(::Type{<:Dates.TimeType}, ::Type{<:Real}) = String
promote_type1(::Type{T}, ::Type{String}) where T = String
promote_type1(::Type{Union{T, Missing}}, ::Type{String}) where T = Union{String, Missing}
promote_type1(::Type{String}, ::Type{T}) where T = String
promote_type1(::Type{String}, ::Type{Union{T, Missing}}) where T = Union{String, Missing}
# avoid ambiguity
promote_type1(::Type{Any}, ::Type{String}) = String
promote_type1(::Type{String}, ::Type{Any}) = String
promote_type1(::Type{String}, ::Type{String}) = String
promote_type1(::Type{String}, ::Type{Missing}) = Union{String, Missing}
promote_type1(::Type{Missing}, ::Type{String}) = Union{String, Missing}
promote_type1(::Type{Any}, ::Type{Missing}) = Missing
promote_type1(::Type{Missing}, ::Type{Missing}) = Missing

function detecttype(layers::Parsers.Delimited, io::IO, prevT, levels, row, col, bools, dateformat, dec, old)
    pos = position(io)
    result = Parsers.parse(layers, io, Tuple{Ptr{UInt8}, Int})
    Parsers.ok(result.code) || throw(Error(Parsers.Error(io, result), row, col))
    # @debug "res = $result"
    result.result === missing && return Missing
    # update levels
    incr!(levels, result.result::Tuple{Ptr{UInt8}, Int})

    if Int64 <: prevT || prevT == Missing
        seek(io, pos)
        res_int = Parsers.parse(layers, io, Int64)
        # @debug "res_int = $res_int"
        Parsers.ok(res_int.code) && return Int64
    end
    if Float64 <: prevT || Int64 <: prevT || prevT == Missing
        seek(io, pos)
        res_float = Parsers.parse(layers, io, Float64; decimal=dec)
        # @debug "res_float = $res_float"
        Parsers.ok(res_float.code) && return Float64
    end
    if Date <: prevT || DateTime <: prevT || prevT == Missing
        if dateformat === nothing
            # try to auto-detect TimeType
            seek(io, pos)
            res_date = Parsers.parse(layers, io, Date)
            # @debug "res_date = $res_date"
            Parsers.ok(res_date.code) && return Date
            seek(io, pos)
            res_datetime = Parsers.parse(layers, io, DateTime)
            # @debug "res_datetime = $res_datetime"
            Parsers.ok(res_datetime.code) && return DateTime
        else
            # use user-provided dateformat
            T = timetype(dateformat)
            # @debug "T = $T"
            seek(io, pos)
            res_dt = Parsers.parse(layers, io, T; dateformat=dateformat)
            # @debug "res_dt = $res_dt"
            Parsers.ok(res_dt.code) && return T
        end
    end
    if Bool <: prevT || prevT == Missing
        seek(io, pos)
        res_bool = Parsers.parse(layers, io, Bool; bools=bools)
        # @debug "res_bool = $res_bool"
        Parsers.ok(res_bool.code) && return Bool
    end
    return String
end

function datalayout(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, rows, startpos, fs)
    rows = rows == 0 ? CSV.countlines(source, quotechar, escapechar) : rows
    seek(source, startpos)
    rows = fs == 0 ? -1 : max(-1, rows - datarow + 1 - footerskip) # rows now equals the actual number of rows in the dataset

    # figure out # of columns and header, either an Integer, AbstractRange, or Vector{String}
    # also ensure that `source` is positioned at the start of data
    if isa(header, Integer)
        # default header = 1
        if header <= 0
            # no header row in dataset; skip to data to figure out # of columns
            CSV.skipto!(parsinglayers, source, 1, datarow)
            datapos = position(source)
            row_vals = readsplitline(parsinglayers, source)
            seek(source, datapos)
            columnnames = ["Column$i" for i = eachindex(row_vals)]
        else
            CSV.skipto!(parsinglayers, source, 1, header)
            columnnames = [ismissing(x) ? "" : strip(x) for x in readsplitline(parsinglayers, source)]
            datarow != header+1 && CSV.skipto!(parsinglayers, source, header+1, datarow)
            datapos = position(source)
        end
    elseif isa(header, AbstractRange)
        CSV.skipto!(parsinglayers, source, 1, first(header))
        columnnames = [x for x in readsplitline(parsinglayers, source)]
        for row = first(header):(last(header)-1)
            for (i,c) in enumerate([x for x in readsplitline(parsinglayers, source)])
                columnnames[i] *= "_" * c
            end
        end
        datarow != last(header)+1 && CSV.skipto!(parsinglayers, source, last(header)+1, datarow)
        datapos = position(source)
    elseif fs == 0
        datapos = position(source)
        columnnames = header
        cols = length(columnnames)
    else
        CSV.skipto!(parsinglayers, source, 1, datarow)
        datapos = position(source)
        row_vals = readsplitline(parsinglayers, source)
        seek(source, datapos)
        if isempty(header)
            columnnames = ["Column$i" for i in eachindex(row_vals)]
        else
            length(header) == length(row_vals) || throw(ArgumentError("The length of provided header ($(length(header))) doesn't match the number of columns at row $datarow ($(length(row_vals)))"))
            columnnames = header
        end
    end
    return rows, columnnames, datapos
end

function datalayout_transpose(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, fs)
    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row = skiptoheader!(parsinglayers, source, 1, header)
        # source now at start of 1st header cell
        columnnames = [strip(Parsers.parse(parsinglayers, source, String).result::String)]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = countfields(source, parsinglayers)
        
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
            # skip to header column to read column names
            row = skiptoheader!(parsinglayers, source, 1, header)
            cols += 1
            push!(columnnames, strip(Parsers.parse(parsinglayers, source, String).result::String))
            push!(columnpositions, position(source))
            readline!(parsinglayers, source)
        end
        seek(source, datapos)
    elseif isa(header, AbstractRange)
        # column names span several columns
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif fs == 0
        # emtpy file, use column names if provided
        datapos = position(source)
        columnpositions = Int[]
        columnnames = header
    else
        # column names provided explicitly or should be generated, they don't exist in data
        # skip to datarow
        row = skiptoheader!(parsinglayers, source, 1, datarow)
        # source now at start of 1st data cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : header[1]]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = countfields(source, parsinglayers)
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
            # skip to datarow column
            row = skiptoheader!(parsinglayers, source, 1, datarow)
            cols += 1
            push!(columnnames, isa(header, Integer) || isempty(header) ? "Column$cols" : header[cols])
            push!(columnpositions, position(source))
            readline!(parsinglayers, source)
        end
        seek(source, datapos)
    end
    rows = rows - footerskip # rows now equals the actual number of rows in the dataset
    return rows, columnnames, columnpositions
end

isascii(c) = Base.isascii(c)
isascii(c::UInt8) = c < 0x80

function Source(fullpath::Union{AbstractString,IO};
              delim=",",
              quotechar='"',
              escapechar='\\',
              missingstring=nothing,

              header::Union{Integer, UnitRange{Int}, Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types=Type[],
              typemap::Dict{Type, Type}=Dict{Type, Type}(),
              allowmissing::Symbol=:all,
              dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
              decimal='.',
              truestring=nothing,
              falsestring=nothing,
              categorical::Bool=true,
              strings::Union{Nothing, Symbol}=nothing,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              limit::Union{Nothing, Int}=nothing,
              use_mmap::Bool=true,
              transpose::Bool=false,
              warn::Bool=true)
    # make sure character args are UInt8
    warn && Base.depwarn("CSV.Source is deprecated and will be removed in a future release; please use CSV.File instead (supports most of the same options)", nothing)
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))

    # deprecations for CSV.File
    if strings === nothing
        strings = :intern # no need to depwarn
    elseif strings === :intern
        Base.depwarn("strings=:intern is deprecated, String values are interned by default now", nothing)
    elseif strings === :weakref
        Base.depwarn("strings=:weakref is deprecated, use typemap=Dict(String=>WeakRefString{UInt8}) instead", nothing)
        merge!(typemap, Dict(String=>WeakRefString{UInt8}))
    end
    if rows_for_type_detect != 100
        Base.depwarn("rows_for_type_detect is deprecated; CSV.File will randomly sample throughout an entire file to better infer column types", nothing)
    end
    if rows != 0
        Base.depwarn("the rows keyword argument is deprecated; to limit the number of rows read from a csv file, pass `limit=X` instead", nothing)
        limit = rows
    end
    if limit === nothing
        limit = 0
    end
    if truestring !== nothing
        Base.depwarn("truestring is deprecated in CSV.File in favor of allowing the passing of multiple strings via `truestrings=[...]`", nothing)
    else
        truestring = "true"
    end
    if falsestring !== nothing
        Base.depwarn("falsestring is deprecated in CSV.File in favor of allowing the passing of multiple strings via `falsestrings=[...]`", nothing)
    else
        falsestring = "false"
    end
    if missingstring !== nothing
        Base.depwarn("missingstring is deprecated in CSV.File in favor of allowing the passing of multiple strings via `missingstrings=[...]`", nothing)
    else
        missingstring = ""
    end

    # argument checks
    isa(fullpath, AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
    header = (isa(header, Integer) && header == 1 && datarow == 1) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))

    # open the file for property detection
    if isa(fullpath, IOBuffer)
        source = fullpath
        fs = bytesavailable(fullpath)
        fullpath = "<IOBuffer>"
    elseif isa(fullpath, IO)
        source = IOBuffer(Base.read(fullpath))
        fs = bytesavailable(source)
        fullpath = isdefined(fullpath, :name) ? fullpath.name : "__IO__"
    else
        source = open(fullpath, "r") do f
            IOBuffer(use_mmap ? Mmap.mmap(f) : Base.read(f))
        end
        fs = filesize(fullpath)
    end
    
    startpos = position(source)
    
    # BOM character detection
    if fs > 0 && Parsers.peekbyte(source) == 0xef
        Parsers.readbyte(source)
        Parsers.readbyte(source) == 0xbb || seek(source, startpos)
        Parsers.readbyte(source) == 0xbf || seek(source, startpos)
        startpos = position(source)
    end

    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header

    # build Parsers parsinglayers
    bools = (truestring != "true" || falsestring != "false") ? Parsers.Trie([truestring=>true, falsestring=>false]) : Parsers.BOOLS
    df = makedf(dateformat)
    dec = decimal % UInt8
    d = makestr(delim)
    parsinglayers = Parsers.Delimited(Parsers.Quoted(Parsers.Strip(Parsers.Sentinel(missingstring), d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t'), quotechar, escapechar), d; newline=true)

    # data layout detection: figure out rows, columnnames, and datapos
    if transpose
        rows, columnnames, datapos = datalayout_transpose(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, fs)
        originalcolumnpositions = [x for x in datapos]
    else
        rows, columnnames, datapos = datalayout(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, limit, startpos, fs)
    end
    cols = length(columnnames)
    @debug "columnnames=$columnnames, cols=$cols, rows=$rows, datapos=$datapos"

    # Detect column types
    pools = CategoricalPool{String, UInt32, CatStr}[]
    if isa(types, Vector) && length(types) == cols
        # types might be a Vector{DataType}, which will be a problem if Unions are needed
        columntypes = convert(Vector{Type}, types)
    elseif isa(types, Dict) || isempty(types)
        @debug "detecting types"
        columntypes = Type[Any for x = 1:cols]
        levels = [Dict{String, Int}() for _ = 1:cols]
        lineschecked = 0
        while !eof(source) && lineschecked < min(rows < 0 ? rows_for_type_detect : rows, rows_for_type_detect)
            lineschecked += 1
            @debug "type detecting on row = $lineschecked..."
            for i = 1:cols
                transpose && seek(source, datapos[i])
                @debug "\tdetecting col = $i..."
                typ = CSV.detecttype(parsinglayers, source, columntypes[i], levels[i], lineschecked, i, bools, df, dec, true)::Type
                transpose && setindex!(datapos, position(source), i)
                @debug "$typ"
                columntypes[i] = CSV.promote_type1(columntypes[i], typ)
                @debug "...promoting to: $(columntypes[i])"
            end
        end
        if df === nothing && any(x->Base.nonmissingtype(x) <: Dates.TimeType, columntypes)
            df = any(x->Base.nonmissingtype(x) <: DateTime, columntypes) ? Dates.default_format(DateTime) : Dates.default_format(Date)
        end
        if categorical
            pools = Vector{CategoricalPool{String, UInt32, CatStr}}(undef, cols)
            for i = 1:cols
                T = columntypes[i]
                if length(levels[i]) / sum(values(levels[i])) < .67 && T !== Missing && Base.nonmissingtype(T) <: String
                    columntypes[i] = substitute(T, CategoricalArrays.catvaluetype(Base.nonmissingtype(T), UInt32))
                    pools[i] = CategoricalPool{String, UInt32}(collect(keys(levels[i])))
                end
            end
        end
    else
        throw(ArgumentError("$cols number of columns detected; `types` argument has $(length(types)) entries"))
    end
    if isa(types, Dict)
        if isa(types, Dict{String})
            colinds = indexin(keys(types), columnnames)
        else
            colinds = keys(types)
        end
        for (col, typ) in zip(colinds, values(types))
            columntypes[col] = typ
        end
        autocols = setdiff(1:cols, colinds)
    elseif isempty(types)
        autocols = collect(1:cols)
    else
        autocols = Int[]
    end
    if !isempty(typemap)
        for col = 1:cols
            columntypes[col] = get(typemap, columntypes[col]) do
                if haskey(typemap, Base.nonmissingtype(columntypes[col]))
                    Union{Missing, typemap[Base.nonmissingtype(columntypes[col])]}
                else
                    columntypes[col]
                end
            end
        end
    end
    if allowmissing != :auto
        if allowmissing == :all # allow missing values in all automatically detected columns
            for i = autocols
                T = columntypes[i]
                columntypes[i] = Union{Base.nonmissingtype(T), Missing}
            end
        elseif allowmissing == :none # disallow missing values in all automatically detected columns
            for i = autocols
                T = columntypes[i]
                columntypes[i] = Base.nonmissingtype(T)
            end
        else
            throw(ArgumentError("allowmissing must be either :all, :none or :auto"))
        end
    end
    sch = Data.Schema(columntypes, columnnames, ifelse(rows < 0, missing, rows))
    if transpose
        datapos = originalcolumnpositions
        seek(source, startpos)
    else
        seek(source, datapos)
    end
    return Source(sch, parsinglayers, source, bools, df, dec, String(fullpath), datapos, pools)
end

# Data.Source interface
"reset a `CSV.Source` to its beginning to be ready to parse data from again"
Data.reset!(s::CSV.Source) = (seek(s.io, s.datapos); return nothing)
Data.schema(source::CSV.Source) = source.schema
Data.accesspattern(::Type{<:CSV.Source}) = Data.Sequential
@inline Data.isdone(io::CSV.Source, row, col, rows, cols) = eof(io.io) || (!ismissing(rows) && row > rows)
@inline Data.isdone(io::Source, row, col) = Data.isdone(io, row, col, size(io.schema)...)
Data.streamtype(::Type{<:CSV.Source}, ::Type{Data.Field}) = true
Data.reference(source::CSV.Source) = source.io.data

@inline function parsefield(source::CSV.Source{P, I, DF, D}, ::Type{T}, row, col; kwargs...) where {P, I, DF, D, T}
    D === Vector{Int} && Parsers.fastseek!(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, T; kwargs...)
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if Parsers.ok(r.code)
        return r.result
    else
        throw(Error(Parsers.Error(source.io, r), row, col))
    end
end

@inline Data.streamfrom(source::Source, ::Type{Data.Field}, T, row, col::Int) = parsefield(source, Base.nonmissingtype(T), row, col)
@inline Data.streamfrom(source::Source, ::Type{Data.Field}, ::Union{Type{Bool}, Union{Bool, Missing}}, row, col::Int) = parsefield(source, Bool, row, col; bools=source.bools)
@inline Data.streamfrom(source::Source, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T <: Union{AbstractFloat, Missing}} = parsefield(source, Base.nonmissingtype(T), row, col; decimal=source.decimal)
@inline Data.streamfrom(source::Source, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T <: Union{Dates.TimeType, Missing}} = parsefield(source, Base.nonmissingtype(T), row, col; dateformat=source.dateformat)
@inline Data.streamfrom(source::Source, ::Type{Data.Field}, ::Type{Missing}, row, col::Int) = parsefield(source, Missing, row, col)

@inline function Data.streamfrom(source::Source, ::Type{Data.Field}, ::Union{Type{Union{CatStr, Missing}}, Type{CatStr}}, row, col::Int)
    str = parsefield(source, Tuple{Ptr{UInt8}, Int}, row, col)
    if str isa Missing
        return missing
    else
        @inbounds pool = source.pools[col]
        str::Tuple{Ptr{UInt8}, Int}
        i = get(pool, str, nothing)
        if i === nothing
            i = get!(pool, unsafe_string(str[1], str[2]))
            issorted(levels(pool)) || levels!(pool, sort(levels(pool)))
        end
        return CatStr(i, pool)
    end
end

function read(source::CSV.Source, sink::Union{Type, Nothing}=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}())
    if append
        Base.depwarn("`CSV.read(source; append=true)` is deprecated in favor of sink-specific options; e.g. DataFrames supports `CSV.File(filename) |> x->append!(existing_df, x)` to append the rows of a csv file to an existing DataFrame", nothing)
    end
    if !isempty(transforms)
        Base.depwarn("`CSV.read(source; transforms=Dict(...)` is deprecated in favor of `CSV.File(filename) |> transform((a=x->x+1, b=x->string(\"custom_prefix\", x))) |> DataFrame`", nothing)
    end
    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms)
    return Data.close!(sink)
end
function read(source::CSV.Source, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    Base.depwarn("`CSV.read(source::CSV.Source, sink::$T)` is deprecated; use `CSV.read(source) |> sink` instead", nothing)
    if append
        Base.depwarn("`CSV.read(source; append=true)` is deprecated in favor of sink-specific options; e.g. DataFrames supports `CSV.File(filename) |> x->append!(existing_df, x)` to append the rows of a csv file to an existing DataFrame", nothing)
    end
    if !isempty(transforms)
        Base.depwarn("`CSV.read(source; transforms=Dict(...)` is deprecated in favor of `CSV.File(filename) |> transform((a=x->x+1, b=x->string(\"custom_prefix\", x))) |> DataFrame`", nothing)
    end
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    return Data.close!(sink)
end

function Sink(fullpath::Union{AbstractString, IO};
              delim::Char=',',
              quotechar::Char='"',
              escapechar::Char='\\',
              missingstring::AbstractString="",
              dateformat=nothing,
              header::Bool=true,
              colnames::Vector{String}=String[],
              append::Bool=false,
              quotefields::Bool=false)
    Base.depwarn("CSV.Sink is deprecated and will be removed in a future release; use `source |> CSV.write(file; kwargs...)` or `CSV.write(source, file; kwargs...)` directly.", nothing)
    delim = delim % UInt8; quotechar = quotechar % UInt8; escapechar = escapechar % UInt8
    dateformat = isa(dateformat, AbstractString) ? Dates.DateFormat(dateformat) : dateformat
    io = IOBuffer()
    options = CSV.Options(delim=delim, quotechar=quotechar, escapechar=escapechar, missingstring=missingstring, dateformat=dateformat)
    !append && header && !isempty(colnames) && writeheaders(io, colnames, options, Val{quotefields})
    return Sink(options, io, fullpath, position(io), !append && header && !isempty(colnames), colnames, length(colnames), append, Val{quotefields})
end

quoted(::Type{Val{true}},  val, q, e, d) =  string(q, replace(val, q=>string(e, q)), q)
quoted(::Type{Val{false}}, val, q, e, d) = (q in val || d in val) ? string(q, replace(val, q=>string(e, q)), q) : val

function writeheaders(io::IOBuffer, h::Vector{String}, options, quotefields)
    cols = length(h)
    q = Char(options.quotechar); e = Char(options.escapechar); d = Char(options.delim)
    for col = 1:cols
        Base.write(io, quoted(quotefields, h[col], q, e, d), ifelse(col == cols, UInt8('\n'), options.delim))
    end
    return nothing
end

# DataStreams interface
Data.streamtypes(::Type{CSV.Sink}) = [Data.Field]
Data.weakrefstrings(::Type{CSV.Sink}) = true

# Constructors
function Sink(sch::Data.Schema, T, append, file::Union{AbstractString, IO}; reference::Vector{UInt8}=UInt8[], kwargs...)
    sink = Sink(file; append=append, colnames=Data.header(sch), kwargs...)
    return sink
end

function Sink(sink, sch::Data.Schema, T, append; reference::Vector{UInt8}=UInt8[])
    sink.append = append
    sink.cols = size(sch, 2)
    !sink.header && !append && writeheaders(sink.io, Data.header(sch), sink.options, sink.quotefields)
    return sink
end

function Data.streamto!(sink::Sink, ::Type{Data.Field}, val, row, col::Int)
    q = Char(sink.options.quotechar); e = Char(sink.options.escapechar); d = Char(sink.options.delim)
    Base.write(sink.io, quoted(sink.quotefields, string(val), q, e, d), ifelse(col == sink.cols, UInt8('\n'), d))
    return nothing
end

function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::Dates.TimeType, row, col::Int)
    v = Dates.format(val, sink.options.dateformat === nothing ? Dates.default_format(typeof(val)) : sink.options.dateformat)
    Base.write(sink.io, v, ifelse(col == sink.cols, UInt8('\n'), sink.options.delim))
    return nothing
end

const EMPTY_UINT8_ARRAY = UInt8[]
function Data.streamto!(sink::Sink, ::Type{Data.Field}, val::Missing, row, col::Int)
    Base.write(sink.io, sink.options.missingcheck ? sink.options.missingstring : EMPTY_UINT8_ARRAY, ifelse(col == sink.cols, UInt8('\n'), sink.options.delim))
    return nothing
end

function Data.close!(sink::CSV.Sink)
    io = isa(sink.fullpath, AbstractString) ? open(sink.fullpath, sink.append ? "a" : "w") : sink.fullpath
    Base.write(io, take!(sink.io))
    isa(sink.fullpath, AbstractString) && close(io)
    return sink
end

function write(sink::Sink, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    Base.depwarn("CSV.write(file, source) is deprecated in favor of CSV.write(source, file); it will also now support piping like: `dataframe |> CSV.write(filename)`", nothing)
    sink = Data.stream!(T(args...), sink; append=append, transforms=transforms)
    return Data.close!(sink)
end

function write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}())
    Base.depwarn("CSV.write(file, source) is deprecated in favor of CSV.write(source, file); it will also now support piping like: `dataframe |> CSV.write(filename)`", nothing)
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    return Data.close!(sink)
end

function validate(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where {T}
    Base.depwarn("CSV.validate(filename, $T; kwargs...) is deprecated in favor of CSV.validate(filename; kwargs...)", nothing)
    validate(Source(fullpath; kwargs...))
    return
end
