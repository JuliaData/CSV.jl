# ensure each cell is valid type of column detected type
# ensure each row has exactly as many values as expected from detection

# text(state::P) = state[] == Delimiter ? "delimiter" : state[] == Newline ? "newline" : "end-of-file (EOF)"

struct ExpectedMoreColumnsError <: Exception
    msg::String
end

struct TooManyColumnsError <: Exception
    msg::String
end

"""
`CSV.validate(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...)` => `typeof(sink)`

`CSV.validate(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...)` => `Data.Sink`

Takes the same positional & keyword arguments as [`CSV.read`](@ref), but provides detailed information as to why reading a csv file failed. Useful for cases where reading fails and it's not clear whether it's due to a row havign too many columns, or wrong types, or what have you.

"""
function validate end

function validate(s::CSV.Source{P, I, DF, D}) where {P, I, DF, D}
    sch = Data.schema(s) # size, header, types
    rows, cols = size(sch)
    types = Data.types(sch)
    for row = 1:rows
        rowstr = ""
        for col = 1:cols
            D === Vector{Int} && Parsers.fastseek!(s.io, s.datapos[col])
            r = Parsers.parse(s.parsinglayers, s.io, Base.nonmissingtype(types[col]))
            D === Vector{Int} && setindex!(s.datapos, position(s.io), col)
            rowstr *= "$(col == 1 ? "" : " ")$(r.result)"
            if col < cols
                if eof(s.io)
                    throw(ExpectedMoreColumnsError("row=$row, col=$col: expected $cols columns, parsed $col, but parsing encountered unexpected end-of-file; parsed row: '$rowstr'"))
                elseif r.b === NEWLINE || r.b === RETURN
                    throw(ExpectedMoreColumnsError("row=$row, col=$col: expected $cols columns, parsed $col, but parsing encountered unexpected newline character; parsed row: '$rowstr'"))
                end
            else
                if !eof(s.io) && !(r.b === NEWLINE || r.b === RETURN)
                    throw(TooManyColumnsError("row=$row, col=$col: expected $cols columns then a newline or EOF; parsed row: '$rowstr'"))
                end
            end
        end
    end
end

function validate(fullpath::Union{AbstractString,IO}, sink::Type=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    validate(Source(fullpath; kwargs...))
    return
end

function validate(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where {T}
    validate(Source(fullpath; kwargs...))
    return
end