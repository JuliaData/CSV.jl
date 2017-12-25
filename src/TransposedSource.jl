TransposedSource(src::Union{AbstractString,IO} = ""; kwargs...) =
    TransposedSource(src, Options(;kwargs...))

function TransposedSource(src::Union{AbstractString,IO}, options::Options)
    source, fullpath, fsize = open_source(src, options.use_mmap)
    startpos = position(source)
    skip_bom(source, fsize)

    const header = options.header
    const datarow = options.datarow
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
    elseif isa(header, Range)
        # column names span several columns
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif fsize == 0
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
    rows = rows - options.footerskip # rows now equals the actual number of data rows in the dataset

    sch = detect_dataschema(source, options, rows, columnnames, columnpositions)
    seek(source, datapos)
    return TransposedSource(sch, options, source, String(fullpath), datapos, columnpositions)
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
