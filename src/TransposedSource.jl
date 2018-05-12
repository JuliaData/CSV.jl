# independent constructor
function TransposedSource(fullpath::Union{AbstractString,IO};

              delim=COMMA,
              quotechar=QUOTE,
              escapechar=ESCAPE,
              missingstring::AbstractString="",
              null::Union{AbstractString,Nothing}=nothing,

              header::Union{Integer, UnitRange{Int}, Vector}=1, # header can be a row number, range of rows, or actual string vector
              datarow::Int=-1, # by default, data starts immediately after header or start of file
              types=Type[],
              allowmissing::Symbol=:all,
              nullable::Union{Bool,Missing,Nothing}=nothing,
              dateformat=missing,
              decimal=PERIOD,
              truestring="true",
              falsestring="false",
              categorical::Bool=true,
              weakrefstrings::Union{Bool,Nothing}=nothing,
              strings::Symbol=:intern,

              footerskip::Int=0,
              rows_for_type_detect::Int=100,
              rows::Int=0,
              use_mmap::Bool=true)
    # make sure character args are UInt8
    isascii(delim) || throw(ArgumentError("non-ASCII characters not supported for delim argument: $delim"))
    isascii(quotechar) || throw(ArgumentError("non-ASCII characters not supported for quotechar argument: $quotechar"))
    isascii(escapechar) || throw(ArgumentError("non-ASCII characters not supported for escapechar argument: $escapechar"))
    return CSV.TransposedSource(fullpath=fullpath,
                        options=CSV.Options(delim=typeof(delim) <: String ? UInt8(first(delim)) : (delim % UInt8),
                                            quotechar=typeof(quotechar) <: String ? UInt8(first(quotechar)) : (quotechar % UInt8),
                                            escapechar=typeof(escapechar) <: String ? UInt8(first(escapechar)) : (escapechar % UInt8),
                                            missingstring=missingstring, null=null, dateformat=dateformat, decimal=decimal,
                                            truestring=truestring, falsestring=falsestring, internstrings=(strings === :intern)),
                        header=header, datarow=datarow, types=types, allowmissing=allowmissing, nullable=nullable, categorical=categorical, footerskip=footerskip,
                        rows_for_type_detect=rows_for_type_detect, rows=rows, use_mmap=use_mmap)
end

function TransposedSource(;fullpath::Union{AbstractString,IO}="",
                options::CSV.Options{D}=CSV.Options(),

                header::Union{Integer,UnitRange{Int},Vector}=1, # header can be a row number, range of rows, or actual string vector
                datarow::Int=-1, # by default, data starts immediately after header or start of file
                types=Type[],
                allowmissing::Symbol=:all,
                nullable::Union{Bool,Missing,Nothing}=nothing,
                categorical::Bool=true,
                weakrefstrings::Union{Bool,Nothing}=nothing,
                strings::Symbol=:intern,

                footerskip::Int=0,
                rows_for_type_detect::Int=100,
                rows::Int=0,
                use_mmap::Bool=true) where {D}
    # argument checks
    isa(fullpath, AbstractString) && (isfile(fullpath) || throw(ArgumentError("\"$fullpath\" is not a valid file")))
    header = (isa(header, Integer) && header == 1 && datarow == 1) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    if options.null !== nothing
        resize!(options.missingstring, length(options.null))
        copyto!(options.missingstring, options.null)
        Base.depwarn("null option is deprecated, use missingstring instead", :TransposedSource)
    end
    if nullable !== nothing
        allowmissing = ismissing(nullable) ? :auto :
                       nullable            ? :all  : :none
        Base.depwarn("nullable=$nullable argument is deprecated, use allowmissing=$(repr(allowmissing)) instead", :TransposedSource)
    end
    if weakrefstrings !== nothing
        strings = weakrefstrings ? :weakref : :raw
        Base.depwarn("weakrefstrings argument is deprecated, use strings=$(repr(strings)) instead", :Source)
    end

    # open the file for property detection
    if isa(fullpath, IOBuffer)
        source = fullpath
        fs = nb_available(fullpath)
        fullpath = "<IOBuffer>"
    elseif isa(fullpath, IO)
        source = IOBuffer(Base.read(fullpath))
        fs = nb_available(fullpath)
        fullpath = isdefined(fullpath, :name) ? fullpath.name : "__IO__"
    else
        source = open(fullpath, "r") do f
            IOBuffer(use_mmap ? Mmap.mmap(f) : Base.read(f))
        end
        fs = filesize(fullpath)
    end
    options.datarow != -1 && (datarow = options.datarow)
    options.rows != 0 && (rows = options.rows)
    options.header != 1 && (header = options.header)
    !isempty(options.types) && (types = options.types)
    startpos = position(source)
    # BOM character detection
    if fs > 0 && peekbyte(source) == 0xef
        readbyte(source)
        readbyte(source) == 0xbb || seek(source, startpos)
        readbyte(source) == 0xbf || seek(source, startpos)
    end
    datarow = datarow == -1 ? (isa(header, Vector) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header

    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row = 1
        while row < header
            while !eof(source)
                b = readbyte(source)
                b == options.delim && break
            end
            row += 1
        end
        # source now at start of 1st header cell
        columnnames = [strip(parsefield(source, String, options, 1, row))]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = 0
        b = eof(source) ? 0x00 : peekbyte(source)
        while !eof(source) && b != NEWLINE && b != RETURN
            b = readbyte(source)
            rows += ifelse(b == options.delim, 1, 0)
            rows += ifelse(b == NEWLINE, 1, 0)
            rows += ifelse(b == RETURN, 1, 0)
            rows += ifelse(eof(source), 1, 0)
        end
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
           # skip to header column to read column names
            row = 1
            while row < header
                while !eof(source)
                    b = readbyte(source)
                    b == options.delim && break
                end
                row += 1
            end
            cols += 1
            push!(columnnames, strip(parsefield(source, String, options, cols, row)))
            push!(columnpositions, position(source))
            b = eof(source) ? 0x00 : peekbyte(source)
            while !eof(source) && b != NEWLINE && b != RETURN
                b = readbyte(source)
            end
        end
        seek(source, datapos)
    elseif isa(header, AbstractRange)
        # column names span several columns
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif fs == 0
        # emtpy file, use column names if provided
        datapos = position(source)
        columnnames = header
        cols = length(columnnames)
    else
        # column names provided explicitly or should be generated, they don't exist in data
        # skip to header column to read column names
        row = 1
        while row < datarow
            while !eof(source)
                b = readbyte(source)
                b == options.delim && break
            end
            row += 1
        end
        # source now at start of 1st header cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : header[1]]
        columnpositions = [position(source)]
        datapos = position(source)
        rows = 0
        b = peekbyte(source)
        while !eof(source) && b != NEWLINE && b != RETURN
            b = readbyte(source)
            rows += ifelse(b == options.delim, 1, 0)
            rows += ifelse(b == NEWLINE, 1, 0)
            rows += ifelse(b == RETURN, 1, 0)
            rows += ifelse(eof(source), 1, 0)
        end
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(source)
           # skip to datarow column
            row = 1
            while row < datarow
                while !eof(source)
                    b = readbyte(source)
                    b == options.delim && break
                end
                row += 1
            end
            cols += 1
            push!(columnnames, isa(header, Integer) || isempty(header) ? "Column$cols" : header[cols])
            push!(columnpositions, position(source))
            b = peekbyte(source)
            while !eof(source) && b != NEWLINE && b != RETURN
                b = readbyte(source)
            end
        end
        seek(source, datapos)
    end
    rows = rows - footerskip # rows now equals the actual number of rows in the dataset
    startingcolumnpositions = deepcopy(columnpositions)
    # Detect column types
    cols = length(columnnames)
    if isa(types, Vector) && length(types) == cols
        columntypes = types
    elseif isa(types, Dict) || isempty(types)
        columntypes = fill!(Vector{Type}(uninitialized, cols), Any)
        levels = [Dict{WeakRefString{UInt8}, Int}() for _ = 1:cols]
        lineschecked = 0
        while !eof(source) && lineschecked < min(rows < 0 ? rows_for_type_detect : rows, rows_for_type_detect)
            lineschecked += 1
            # println("type detecting on row = $lineschecked...")
            for i = 1:cols
                # print("\tdetecting col = $i...")
                seek(source, columnpositions[i])
                typ = CSV.detecttype(source, options, columntypes[i], levels[i])::Type
                columnpositions[i] = position(source)
                # print(typ)
                columntypes[i] = CSV.promote_type2(columntypes[i], typ)
                # println("...promoting to: ", columntypes[i])
            end
        end
        if options.dateformat === missing && any(x->x <: Dates.TimeType, columntypes)
            # auto-detected TimeType
            options = Options(delim=options.delim, quotechar=options.quotechar, escapechar=options.escapechar,
                              missingstring=options.missingstring, dateformat=Dates.ISODateTimeFormat, decimal=options.decimal,
                              datarow=options.datarow, rows=options.rows, header=options.header, types=options.types)
        end
        if categorical
            for i = 1:cols
                T = columntypes[i]
                if length(levels[i]) / sum(values(levels[i])) < .67 && T !== Missing && Missings.T(T) <: WeakRefString
                    columntypes[i] = substitute(T, CategoricalArrays.catvaluetype(Missings.T(T), UInt32))
                end
            end
        end
    else
        throw(ArgumentError("$cols number of columns detected; `types` argument has $(length(types)) entries"))
    end

    if isa(types, Dict)
        if isa(types, Dict{String})
            @static if VERSION >= v"0.7.0-DEV.3627"
                colinds = indexin(keys(types), columnnames)
            else
                colinds = indexin(collect(keys(types)), columnnames)
            end
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
        columntypes = Type[(T !== Missing && Missings.T(T) <: WeakRefString) ? substitute(T, String) : T for T in columntypes]
    end
    if allowmissing != :auto
        if allowmissing == :all # allow missing values in all automatically detected columns
            for i = autocols
                T = columntypes[i]
                columntypes[i] = Union{Missings.T(T), Missing}
            end
        elseif allowmissing == :none # disallow missing values in all automatically detected columns
            for i = autocols
                T = columntypes[i]
                columntypes[i] = Missings.T(T)
            end
        else
            throw(ArgumentError("allowmissing must be either :all, :none or :auto"))
        end
    end
    seek(source, datapos)
    sch = Data.Schema(columntypes, columnnames, ifelse(rows < 0, missing, rows))
    return TransposedSource(sch, options, source, String(fullpath), datapos, startingcolumnpositions)
end

# construct a new TransposedSource from a Sink
TransposedSource(s::CSV.Sink) = CSV.TransposedSource(fullpath=s.fullpath, options=s.options)

# Data.Source interface
"reset a `CSV.Source` to its beginning to be ready to parse data from again"
Data.reset!(s::CSV.TransposedSource) = (seek(s.io, s.datapos); return nothing)
Data.schema(source::CSV.TransposedSource) = source.schema
Data.accesspattern(::Type{<:CSV.TransposedSource}) = Data.Sequential
@inline Data.isdone(io::CSV.TransposedSource, row, col, rows, cols) = eof(io.io) || (!ismissing(rows) && row > rows)
@inline Data.isdone(io::TransposedSource, row, col) = Data.isdone(io, row, col, size(io.schema)...)
Data.streamtype(::Type{<:CSV.TransposedSource}, ::Type{Data.Field}) = true
@inline function Data.streamfrom(source::CSV.TransposedSource, ::Type{Data.Field}, ::Type{T}, row, col::Int) where {T}
    seek(source.io, source.columnpositions[col])
    v = CSV.parsefield(source.io, T, source.options, row, col)
    source.columnpositions[col] = position(source.io)
    return v
end
Data.reference(source::CSV.TransposedSource) = source.io.data
