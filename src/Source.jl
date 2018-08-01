makedf(df::DateFormat) = df
makedf(df::String) = DateFormat(df)
makedf(df::Nothing) = df
makestr(d::UInt8) = string(Char(d))
makestr(d::Char) = string(d)
makestr(d::String) = d

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
            row_vals = readsplitline(parsinglayers, source, delim)
            seek(source, datapos)
            columnnames = ["Column$i" for i = eachindex(row_vals)]
        else
            CSV.skipto!(parsinglayers, source, 1, header)
            columnnames = [ismissing(x) ? "" : strip(x) for x in readsplitline(parsinglayers, source, delim)]
            datarow != header+1 && CSV.skipto!(parsinglayers, source, header+1, datarow)
            datapos = position(source)
        end
    elseif isa(header, AbstractRange)
        CSV.skipto!(parsinglayers, source, 1, first(header))
        columnnames = [x for x in readsplitline(parsinglayers, source, delim)]
        for row = first(header):(last(header)-1)
            for (i,c) in enumerate([x for x in readsplitline(parsinglayers, source, delim)])
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
        row_vals = readsplitline(parsinglayers, source, delim)
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

function skiptoheader!(source, row, header, delim)
    while row < header
        while !eof(source)
            b = readbyte(source)
            b == delim && break
        end
        row += 1
    end
    return row
end

function datalayout_transpose(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, fs)
    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row = skiptoheader!(source, 1, header, delim)
        # source now at start of 1st header cell
        columnnames = [strip(Parsers.parse(parsinglayers, source, String).result::String)]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = 0
        result = Parsers.Result(Tuple{Ptr{UInt8}, Int})
        while !eof(source)
            Parsers.parse!(parsinglayers, source, result)
            result.code === Parsers.OK || throw(Error(result, rows+1, 1))
            rows += 1
            result.b === delim && continue
            (result.b === NEWLINE || result.b === RETURN || eof(source)) && break
        end
        
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
            # skip to header column to read column names
            row = skiptoheader!(source, 1, header, delim)
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
        row = skiptoheader!(source, 1, datarow, delim)
        # source now at start of 1st data cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : header[1]]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = 0
        result = Parsers.Result(Tuple{Ptr{UInt8}, Int})
        while !eof(source)
            Parsers.parse!(parsinglayers, source, result)
            result.code === Parsers.OK || throw(Error(result, rows+1, 1))
            rows += 1
            result.b === delim && continue
            (result.b === NEWLINE || result.b === RETURN || eof(source)) && break
        end
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
            # skip to datarow column
            row = skiptoheader!(source, 1, datarow, delim)
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

function Source(fullpath::Union{AbstractString,IO};

              delim=",",
              quotechar=QUOTE,
              escapechar=ESCAPE,
              missingstring::AbstractString="",

              header::Union{Integer, UnitRange{Int}, Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types=Type[],
              allowmissing::Symbol=:all,
              dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
              decimal=PERIOD,
              truestring="true",
              falsestring="false",
              categorical::Bool=true,
              strings::Symbol=:intern,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              use_mmap::Bool=true,
              transpose::Bool=false)
    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))

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
    parsinglayers = Parsers.Delimited(Parsers.Quoted(Parsers.Strip(Parsers.Sentinel(missingstring), d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t'), quotechar, escapechar), d, "\n", "\r", "\r\n")

    # data layout detection: figure out rows, columnnames, and datapos
    if transpose
        rows, columnnames, datapos = datalayout_transpose(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, fs)
        originalcolumnpositions = [x for x in datapos]
    else
        rows, columnnames, datapos = datalayout(header, source, parsinglayers, delim, quotechar, escapechar, datarow, footerskip, rows, startpos, fs)
    end
    cols = length(columnnames)
    @debug "columnnames=$columnnames, cols=$cols, rows=$rows, datapos=$datapos"

    # Detect column types
    pools = CategoricalPool{String, UInt32, CategoricalString{UInt32}}[]
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
                typ = CSV.detecttype(parsinglayers, source, columntypes[i], levels[i], lineschecked, i, bools, df, dec)::Type
                transpose && setindex!(datapos, position(source), i)
                @debug "$typ"
                columntypes[i] = CSV.promote_type2(columntypes[i], typ)
                @debug "...promoting to: $(columntypes[i])"
            end
        end
        if df === nothing && any(x->Base.nonmissingtype(x) <: Dates.TimeType, columntypes)
            df = any(x->Base.nonmissingtype(x) <: DateTime, columntypes) ? Dates.default_format(DateTime) : Dates.default_format(Date)
        end
        if categorical
            pools = Vector{CategoricalPool{String, UInt32, CategoricalString{UInt32}}}(undef, cols)
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
    if strings !== :weakref
        # columntypes = Type[(T !== Missing && Base.nonmissingtype(T) <: WeakRefString) ? substitute(T, String) : T for T in columntypes]
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

# construct a new Source from a Sink
Source(s::CSV.Sink) = CSV.Source(fullpath=s.fullpath, options=s.options)

# Data.Source interface
"reset a `CSV.Source` to its beginning to be ready to parse data from again"
Data.reset!(s::CSV.Source) = (seek(s.io, s.datapos); return nothing)
Data.schema(source::CSV.Source) = source.schema
Data.accesspattern(::Type{<:CSV.Source}) = Data.Sequential
@inline Data.isdone(io::CSV.Source, row, col, rows, cols) = eof(io.io) || (!ismissing(rows) && row > rows)
@inline Data.isdone(io::Source, row, col) = Data.isdone(io, row, col, size(io.schema)...)
Data.streamtype(::Type{<:CSV.Source}, ::Type{Data.Field}) = true
# @inline Data.streamfrom(source::CSV.Source, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T} = CSV.parsefield(source.io, T, source.options, row, col)
Data.reference(source::CSV.Source) = source.io.data

struct Error <: Exception
    result::Parsers.Result
    row::Int
    col::Int
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T, P, I, DF, D}
    D === Vector{Int} && Parsers.fastseek!(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Base.nonmissingtype(T))
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        return r.result
    else
        throw(Error(r, row, col))
    end
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Union{Type{Bool}, Union{Bool, Missing}}, row, col::Int) where {T, P, I, DF, D}
    D === Vector{Int} && seek(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Bool; bools=source.bools)
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        return r.result
    else
        throw(Error(r, row, col))
    end
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T <: Union{AbstractFloat, Missing}, P, I, DF, D}
    D === Vector{Int} && seek(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Base.nonmissingtype(T); decimal=source.decimal)
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        return r.result
    else
        throw(Error(r, row, col))
    end
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T <: Union{Dates.TimeType, Missing}, P, I, DF, D}
    D === Vector{Int} && seek(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Base.nonmissingtype(T); dateformat=source.dateformat)
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        return r.result
    else
        throw(Error(r, row, col))
    end
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Type{Missing}, row, col::Int) where {P, I, DF, D}
    D === Vector{Int} && seek(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Missing)
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        return r.result
    else
        throw(Error(r, row, col))
    end
end

function get🐱(pool::CategoricalPool, val::Tuple{Ptr{UInt8}, Int})
    index = Base.ht_keyindex2!(pool.invindex, val)
    if index > 0
        @inbounds v = pool.invindex.vals[index]
        return CategoricalString{UInt32}(v, pool)
    else
        v = CategoricalArrays.push_level!(pool, val)
        return CategoricalString{UInt32}(v, pool)
    end
end

function Data.streamfrom(source::CSV.Source{P, I, DF, D}, ::Type{Data.Field}, ::Union{Type{Union{CategoricalString{UInt32}, Missing}}, Type{CategoricalString{UInt32}}}, row, col::Int) where {T, P, I, DF, D}
    D === Vector{Int} && Parsers.fastseek!(source.io, source.datapos[col])
    r = Parsers.parse(source.parsinglayers, source.io, Tuple{Ptr{UInt8}, Int})
    D === Vector{Int} && setindex!(source.datapos, position(source.io), col)
    if r.code === Parsers.OK
        if r.result isa Missing
            return missing
        else
            @inbounds pool = source.pools[col]
            return get🐱(pool, r.result::Tuple{Ptr{UInt8}, Int})
        end
    else
        throw(Error(r, row, col))
    end
end

"""
`CSV.read(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...)` => `typeof(sink)`

`CSV.read(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...)` => `Data.Sink`


parses a delimited file into a Julia structure (a DataFrame by default, but any valid `Data.Sink` may be requested).

Minimal error-reporting happens w/ `CSV.read` for performance reasons; for problematic csv files, try [`CSV.validate`](@ref) which takes exact same arguments as `CSV.read` and provides much more information for why reading the file failed.

Positional arguments:

* `fullpath`; can be a file name (string) or other `IO` instance
* `sink::Type{T}`; `DataFrame` by default, but may also be other `Data.Sink` types that support streaming via `Data.Field` interface; note that the method argument can be the *type* of `Data.Sink`, plus any required arguments the sink may need (`args...`).
                    or an already constructed `sink` may be passed (2nd method above)

Keyword Arguments:

* `delim::Union{Char,UInt8}`: how fields in the file are delimited; default `','`
* `quotechar::Union{Char,UInt8}`: the character that indicates a quoted field that may contain the `delim` or newlines; default `'"'`
* `escapechar::Union{Char,UInt8}`: the character that escapes a `quotechar` in a quoted field; default `'\\'`
* `missingstring::String`: indicates how missing values are represented in the dataset; default `""`
* `dateformat::Union{AbstractString,Dates.DateFormat}`: how dates/datetimes are represented in the dataset; default `Dates.ISODateTimeFormat`
* `decimal::Union{Char,UInt8}`: character to recognize as the decimal point in a float number, e.g. `3.14` or `3,14`; default `'.'`
* `truestring`: string to represent `true::Bool` values in a csv file; default `"true"`. Note that `truestring` and `falsestring` cannot start with the same character.
* `falsestring`: string to represent `false::Bool` values in a csv file; default `"false"`
* `header`: column names can be provided manually as a complete Vector{String}, or as an Int/AbstractRange which indicates the row/rows that contain the column names
* `datarow::Int`: specifies the row on which the actual data starts in the file; by default, the data is expected on the next row after the header row(s); for a file without column names (header), specify `datarow=1`
* `types`: column types can be provided manually as a complete Vector{Type}, or in a Dict to reference individual columns by name or number
* `allowmissing::Symbol=:all`: indicates whether columns should allow for missing values or not, that is whether their element type should be `Union{T,Missing}`; by default, all columns are allowed to contain missing values. If set to `:none`, no column can contain missing values, and if set to `:auto`, only colums which contain missing values in the first `rows_for_type_detect` rows are allowed to contain missing values. Column types specified via `types` are not affected by this argument.
* `footerskip::Int`: indicates the number of rows to skip at the end of the file
* `rows_for_type_detect::Int=100`: indicates how many rows should be read to infer the types of columns
* `rows::Int`: indicates the total number of rows to read from the file; by default the file is pre-parsed to count the # of rows; `-1` can be passed to skip a full-file scan, but the `Data.Sink` must be set up to account for a potentially unknown # of rows
* `use_mmap::Bool=true`: whether the underlying file will be mmapped or not while parsing; note that on Windows machines, the underlying file will not be "deletable" until Julia GC has run (can be run manually via `GC.gc()`) due to the use of a finalizer when reading the file.
* `append::Bool=false`: if the `sink` argument provided is an existing table, `append=true` will append the source's data to the existing data instead of doing a full replace
* `transforms::Dict{Union{String,Int},Function}`: a Dict of transforms to apply to values as they are parsed. Note that a column can be specified by either number or column name.
* `transpose::Bool=false`: when reading the underlying csv data, rows should be treated as columns and columns as rows, thus the resulting dataset will be the "transpose" of the actual csv data.
* `categorical::Bool=true`: read string column as a `CategoricalArray` ([ref](https://github.com/JuliaData/CategoricalArrays.jl)), as long as the % of unique values seen during type detection is less than 67%. This will dramatically reduce memory use in cases where the number of unique values is small.
* `strings::Symbol`=:intern: indicates how to treat strings. By default strings are interned, i.e. a global pool of already encountered strings is used to avoid allocating a new `String` object for each field. If `strings=:raw`, string interning is disabled, which can be faster when most strings are unique. If `strings=:weakref`, [`WeakRefStrings`](https://github.com/quinnj/WeakRefStrings.jl) package is used used to speed up file parsing by avoiding copies; can only be used for the `Sink` objects that support `WeakRefStringArray` columns (note that `WeakRefStringArray` still returns regular `String` elements).

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

Other example invocations may include:
```julia
# read in a tab-delimited file
CSV.read(file; delim='\t')

# read in a comma-delimited file with missing values represented as '\\N', such as a MySQL export
CSV.read(file; missingstring="\\N")

# read a csv file that happens to have column names in the first column, and grouped data in rows instead of columns
CSV.read(file; transpose=true)

# manually provided column names; must match # of columns of data in file
# this assumes there is no header row in the file itself, so data parsing will start at the very beginning of the file
CSV.read(file; header=["col1", "col2", "col3"])

# manually provided column names, even though the file itself has column names on the first row
# `datarow` is specified to ensure data parsing occurs at correct location
CSV.read(file; header=["col1", "col2", "col3"], datarow=2)

# types provided manually; as a vector, must match length of columns in actual data
CSV.read(file; types=[Int, Int, Float64])

# types provided manually; as a Dict, can specify columns by # or column name
CSV.read(file; types=Dict(3=>Float64, 6=>String))
CSV.read(file; types=Dict("col3"=>Float64, "col6"=>String))

# manually provided # of rows; if known beforehand, this will improve parsing speed
# this is also a way to limit the # of rows to be read in a file if only a sample is needed
CSV.read(file; rows=10000)

# for data files, `file` and `file2`, with the same structure, read both into a single DataFrame
# note that `df` is used as a 2nd argument in the 2nd call to `CSV.read` and the keyword argument
# `append=true` is passed
df = CSV.read(file)
df = CSV.read(file2, df; append=true)

# manually construct a `CSV.Source` once, then stream its data to both a DataFrame
# and SQLite table `sqlite_table` in the SQLite database `db`
# note the use of `CSV.reset!` to ensure the `source` can be streamed from again
source = CSV.Source(file)
df1 = CSV.read(source, DataFrame)
CSV.reset!(source)
db = SQLite.DB()
sq1 = CSV.read(source, SQLite.Sink, db, "sqlite_table")
```
"""
function read end

function read(fullpath::Union{AbstractString,IO}, sink::Type=DataFrame, args...; append::Bool=false, transforms::AbstractDict=Dict{Int,Function}(), kwargs...)
    source = Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms)
    return Data.close!(sink)
end

function read(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::AbstractDict=Dict{Int,Function}(), kwargs...) where {T}
    source = Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    return Data.close!(sink)
end

read(source::CSV.Source, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, args...; append=append, transforms=transforms); return Data.close!(sink))
read(source::CSV.Source, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T} = (sink = Data.stream!(source, sink; append=append, transforms=transforms); return Data.close!(sink))
