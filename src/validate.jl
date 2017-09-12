# ensure each cell is valid type of column detected type
# ensure each row has exactly as many values as expected from detection

text(state::P) = state[] == Delimiter ? "delimiter" : state[] == Newline ? "newline" : "end-of-file (EOF)"

struct ExpectedMoreColumnsError <: Exception
    msg::String
end

struct TooManyColumnsError <: Exception
    msg::String
end

function validate(s::CSV.Source)
    sch = Data.schema(s) # size, header, types
    rows, cols = size(sch)
    types = Data.types(sch)
    state = P()
    for row = 1:rows
        rowstr = ""
        for col = 1:cols
            v = CSV.parsefield(s.io, types[col], s.options, row, col, state)
            rowstr *= "$(col == 1 ? "" : Char(s.options.delim))$v"
            if col < cols
                if state[] != Delimiter
                    throw(ExpectedMoreColumnsError("row=$row, col=$col: expected $cols columns, parsed $col, but parsing encountered unexpected $(text(state)); parsed row: '$rowstr'"))
                end
            else
                if state[] != Newline && state[] != EOF
                    throw(TooManyColumnsError("row=$row, col=$col: expected $cols columns then a newline or EOF, but parsing encountered another $(text(state)): '$(Char(s.options.delim))'; parsed row: '$rowstr'"))
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