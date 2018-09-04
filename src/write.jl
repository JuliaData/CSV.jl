"""
    CSV.write(table, file::Union{String, IO}; kwargs...) => file
    table |> CSV.write(file::Union{String, IO}; kwargs...) => file

Write a [table](https://github.com/JuliaData/Tables.jl) to a csv file, given as an `IO` argument or String representing the file name to write to.

Keyword arguments include:
* `delim::Union{Char, String}=','`: a character or string to print out as the file's delimiter
* `quotechar::Char='"'`: character to use for quoting text fields that may contain delimiters or newlines
* `openquotechar::Char`: instead of `quotechar`, use `openquotechar` and `closequotechar` to support different starting and ending quote characters
* `escapechar::Char='\\'`: character used to escape quote characters in a text field
* `missingstring::String=""`: string to print 
* `dateformat=Dates.default_format(T)`: the date format string to use for printing out Date & DateTime columns
* `append=false`: whether to append writing to an existing file/IO, if `true`, it will not write column names by default
* `writeheader=!append`: whether to write an initial row of delimited column names, not written by default if appending
* `header`: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table
"""
function write end

function with(f::Function, io::IO, append)
    !append && seekstart(io)
    f(io)
end

function with(f::Function, file::String, append)
    open(file, append ? "a" : "w") do io
        f(io)
    end
end

printcsv(io, val, delim, oq, cq, e, df) = print(io, val)

function printcsv(io, val::String, delim::Char, oq, cq, e, df)
    needtoescape = false
    bytes = codeunits(val)
    oq8, cq8 = UInt8(oq), UInt8(cq)
    @inbounds needtoquote = isempty(bytes) ? false : bytes[1] === oq8
    d = delim % UInt8
    @simd for i = 1:sizeof(val)
        @inbounds b = bytes[i]
        needtoquote |= (b === d) | (b === UInt8('\n')) | (b === UInt8('\r'))
        needtoescape |= b === cq8
    end
    printcsv(io, val, delim, oq, cq, e, df, needtoquote, needtoescape)
end

function printcsv(io, val::String, delim::String, oq, cq, e, df)
    needtoescape = false
    bytes = codeunits(val)
    oq8, cq8 = UInt8(oq), UInt8(cq)
    @inbounds needtoquote = bytes[1] === oq8
    @simd for i = 1:sizeof(val)
        @inbounds b = bytes[i]
        needtoquote |= (b === UInt8('\n')) | (b === UInt8('\r'))
        needtoescape |= b === cq8
    end
    needtoquote |= occursin(delim, val)
    printcsv(io, val, delim, oq, cq, e, df, needtoquote, needtoescape)
end

function printcsv(io, val::String, delim, oq, cq, e, df, needtoquote, needtoescape)
    if needtoquote
        v = needtoescape ? (oq === cq ? replace(val, cq=>string(e, cq)) : replace(replace(val, oq=>string(e, oq)), cq=>string(e, cq))) : val
        print(io, oq, v, cq)
    else
        print(io, val)
    end
    return
end

function printcsv(io, val::T, delim, oq, cq, e, df) where {T <: Dates.TimeType}
    v = Dates.format(val, df === nothing ? Dates.default_format(T) : df)
    printcsv(io, v, delim, oq, cq, e, df)
end

write(file::Union{String, IO}; kwargs...) = x->write(x, file; kwargs...)
function write(itr, file::Union{String, IO}; kwargs...)
    rows = Tables.rows(itr)
    sch = Tables.schema(rows)
    return write(sch, rows, file; kwargs...)
end

function printheader(io, header, delim, oq, cq, e, df)
    cols = length(header)
    for (col, nm) in enumerate(header)
        printcsv(io, string(nm), delim, oq, cq, d, df)
        Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
    end
    return
end

function write(sch::Tables.Schema{schema_names}, rows, file::Union{String, IO};
    delim::Union{Char, String}=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='\\',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    ) where {schema_names}
    oq, cq = openquotechar !== nothing ? (openquotechar, closequotechar) : (quotechar, quotechar)
    names = isempty(header) ? schema_names : header
    cols = length(names)
    with(file, append) do io
        writeheader && printheader(io, names, delim, oq, cq, escapechar, dateformat)
        for row in rows
            Tables.eachcolumn(sch, row) do val, col, nm
                printcsv(io, coalesce(val, missingstring), delim, oq, cq, escapechar, dateformat)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
    end
    return file
end

# handle unknown schema case
function write(::Nothing, rows, file::Union{String, IO};
    delim::Union{Char, String}=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='\\',
    missingstring::AbstractString="",
    dateformat=nothing,
    append::Bool=false,
    writeheader::Bool=!append,
    header::Vector=String[],
    )
    oq, cq = openquotechar !== nothing ? (openquotechar, closequotechar) : (quotechar, quotechar)
    state = iterate(rows)
    if state === nothing
        if writeheader && !isempty(header)
            with(file, append) do io
                printheader(io, header, delim, oq, cq, escapechar, dateformat)
            end
        end
        return file
    end
    row, st = state
    names = isempty(header) ? propertynames(row) : header
    cols = length(names)
    with(file, append) do io
        writeheader && printheader(io, names, delim, oq, cq, escapechar, dateformat)
        while true
            Tables.eachcolumn(names, row) do val, col, nm
                printcsv(io, coalesce(val, missingstring), delim, oq, cq, escapechar, dateformat)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
            state = iterate(rows, st)
            state === nothing && break
            row, st = state
        end
    end
    return file
end
