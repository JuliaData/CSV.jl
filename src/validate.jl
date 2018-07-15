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