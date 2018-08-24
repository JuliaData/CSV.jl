with(f::Function, file::IO, append) = f(file)
function with(f::Function, file::String, append)
    open(file, append ? "a" : "w") do io
        f(io)
    end
end

printcsv(io, val, delim, oq, cq, e, m, df, n=false) = print(io, val)
printcsv(io, ::Missing, delim, oq, cq, e, m, df, n=false) = print(io, m)

function printcsv(io, val::String, delim, oq, cq, e, m, df)
    needtoescape = false
    bytes = codeunits(val)
    #TODO: support string delimiters
    d = delim % UInt8
    @simd for i = 1:sizeof(val)
        @inbounds b = bytes[i]
        needtoescape |= (b === d) | (b === UInt8('\n')) | (b === UInt8('\r'))
    end
    printcsv(io, val, delim, oq, cq, e, m, df, needtoescape)
end

function printcsv(io, val::String, delim, oq, cq, e, m, df, needtoescape)
    if needtoescape
        v = replace(replace(replace(val, delim=>string(e, delim)), "\n"=>string(e, "\n")), "\r"=>string(e, "\r"))
        print(io, oq, v, cq)
    else
        print(io, val)
    end
    return
end

function printcsv(io, val::T, delim, oq, cq, e, m, df, needtoescape) where {T <: Dates.TimeType}
    v = Dates.format(val, df === nothing ? Dates.default_format(T) : df)
    printcsv(io, v, delim, oq, cq, e, m, df)
end

write(file::Union{String, IO}; kwargs...) = x->write(x, file; kwargs...)
function write(itr, file::Union{String, IO};
    delim::Char=',',
    quotechar::Char='"',
    openquotechar::Union{Char, Nothing}=nothing,
    closequotechar::Union{Char, Nothing}=nothing,
    escapechar::Char='\\',
    missingstring::AbstractString="",
    dateformat=nothing,
    writeheader::Bool=true,
    header::Vector{String}=String[],
    append::Bool=true,
    )
    oq, cq = openquotechar !== nothing ? (openquotechar, closequotechar) : (quotechar, quotechar)
    sch = Tables.schema(itr)
    cols = length(Tables.names(sch))
    with(file, append) do io
        if writeheader
            for (col, nm) in enumerate(Tables.names(sch))
                printcsv(io, string(nm), delim, oq, cq, escapechar, missingstring, false)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
        for row in Tables.rows(itr)
            Tables.unroll(sch, row) do val, col, nm
                printcsv(io, val, delim, oq, cq, escapechar, missingstring, dateformat)
                Base.write(io, ifelse(col == cols, UInt8('\n'), delim))
            end
        end
    end
    return file
end
