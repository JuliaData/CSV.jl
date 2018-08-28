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
    @inbounds needtoquote = bytes[1] === oq8
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
function write(itr, file::Union{String, IO};
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
    sch = Tables.schema(itr)
    cols = length(Tables.names(sch))
    with(file, append) do io
        if writeheader
            for (col, nm) in enumerate(isempty(header) ? Tables.names(sch) : header)
                printcsv(io, string(nm), delim, oq, cq, escapechar, dateformat)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
        for row in Tables.rows(itr)
            Tables.unroll(sch, row) do val, col, nm
                printcsv(io, coalesce(val, missingstring), delim, oq, cq, escapechar, dateformat)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
    end
    return file
end
