module CSV

using Mmap, Parsers, Tables

struct Error <: Exception
    result::Parsers.Result
    row::Int
    col::Int
end

struct Row{F}
    file::F
    row::Int
end

struct File{NT, I, P}
    io::I
    parsinglayers::P
    rowpositions::Vector{Int}
    lastparsedcol::Base.RefValue{Int}
end

Base.eltype(f::F) where {F <: File} = Row{F}
Tables.schema(f::File{NT, I, P}) where {NT, I, P} = NT
Base.length(f::File) = length(f.rowpositions)

@inline function Base.iterate(f::File{NT, I, P}, st=1) where {NT, I, P}
    st > length(f) && return nothing
    return Row(f, st), st + 1
end

@inline function Base.getproperty(row::Row{File{NT, I, P}}, name::Symbol) where {NT, I, P}
    f = getfield(row, 1)
    col, T = Tables.columnindextype(NT, name)
    lastparsed = f.lastparsedcol[]
    if col === lastparsed + 1
    elseif col === 1
        @inbounds Parsers.fastseek!(f.io, f.rowpositions[getfield(row, 2)])
    elseif col > lastparsed + 1
        # skipping cells
        foreach(x->Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int}), 1:(col - lastparsed+1))
    elseif col !== lastparsed + 1
        # randomly seeking within row
        @inbounds Parsers.fastseek!(f.io, f.rowpositions[getfield(row, 2)])
        foreach(x->Parsers.parse(f.parsinglayers, f.io, Tuple{Ptr{UInt8}, Int}), 1:(col - 1))
    end
    r = Parsers.parse(f.parsinglayers, f.io, Base.nonmissingtype(T))
    f.lastparsedcol[] = col
    if Parsers.ok(r.code)
        return r.result
    else
        #TODO: could pass column name to error as well
        throw(Error(r, getfield(row, 2), col))
    end
end

getio(str::String, use_mmap) = IOBuffer(use_mmap ? Mmap.mmap(str) : Base.read(str))
getio(io::IO, use_mmap) = io

function datalayout(header::Integer, parsinglayers, io, datarow)
    # default header = 1
    if header <= 0
        # no header row in dataset; skip to data to figure out # of columns
        skipto!(parsinglayers, io, 1, datarow)
        datapos = position(io)
        row_vals = readsplitline(parsinglayers, io)
        seek(io, datapos)
        columnnames = Tuple(Symbol("Column$i") for i = eachindex(row_vals))
    else
        skipto!(parsinglayers, io, 1, header)
        columnnames = Tuple(ismissing(x) ? Symbol("") : Symbol(strip(x)) for x in readsplitline(parsinglayers, io))
        datarow != header+1 && skipto!(parsinglayers, io, header+1, datarow)
        datapos = position(io)
    end
    return columnnames, datapos
end

function datalayout(header::AbstractRange, parsinglayers, io, datarow)
    skipto!(parsinglayers, io, 1, first(header))
    columnnames = [x for x in readsplitline(parsinglayers, io)]
    for row = first(header):(last(header)-1)
        for (i,c) in enumerate([x for x in readsplitline(parsinglayers, io)])
            columnnames[i] *= "_" * c
        end
    end
    datarow != last(header)+1 && skipto!(parsinglayers, io, last(header)+1, datarow)
    datapos = position(io)
    return Tuple(Symbol(nm) for nm in columnnames), datapos
end

function datalayout(header, parsinglayers, io, datarow)
    skipto!(parsinglayers, io, 1, datarow)
    datapos = position(io)
    row_vals = readsplitline(parsinglayers, io)
    seek(io, datapos)
    if isempty(header)
        columnnames = Tuple(Symbol("Column$i") for i in eachindex(row_vals))
    else
        length(header) == length(row_vals) || throw(ArgumentError("The length of provided header ($(length(header))) doesn't match the number of columns at row $datarow ($(length(row_vals)))"))
        columnnames = Tuple(Symbol(nm) for nm in header)
    end
    return columnnames, datapos
end

function File(source::Union{String, IO};
    use_mmap::Bool=true,
    header::Union{Integer, UnitRange{Int}, Vector}=1, # header can be a row number, range of rows, or actual string vector
    datarow::Int=-1, # by default, data starts immediately after header or start of file
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Char, String}=",",
    quotechar::Union{UInt8, Char}='"',
    escapechar::Union{UInt8, Char}='\\',
    )

    io = getio(source, use_mmap)
    fs = bytesavailable(io)
    
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        Parsers.readbyte(io) == 0xbb || seek(io, startpos)
        Parsers.readbyte(io) == 0xbf || seek(io, startpos)
        startpos = position(io)
    end

    missingstrings = isempty(missingstrings) ? [missingstring] : missingstrings
    d = string(delim)
    parsinglayers = Parsers.Sentinel(missingstrings) |>
                    x->Parsers.Strip(x, d == " " ? 0x00 : ' ', d == "\t" ? 0x00 : '\t') |>
                    x->Parsers.Quoted(x, quotechar, escapechar) |>
                    x->Parsers.Delimited(x, d, "\n", "\r", "\r\n")
    names, datapos = datalayout(header, parsinglayers, io, datarow)
    types = Tuple{(Union{Missing, Int} for nm in names)...}
    rows = rowpositions(io, quotechar % UInt8, escapechar % UInt8)
    seek(io, rows[1])
    return File{NamedTuple{names, types}, typeof(io), typeof(parsinglayers)}(io, parsinglayers, rows, Ref{Int}(0))
end

const READLINE_RESULT = Parsers.Result(Tuple{Ptr{UInt8}, Int})

# readline! is used for implementation of skipto!
function readline!(layers, io::IO)
    eof(io) && return
    while true
        READLINE_RESULT.code = Parsers.SUCCESS
        res = Parsers.parse!(layers, io, READLINE_RESULT)
        Parsers.ok(res.code) || throw(Parsers.Error(res))
        ((res.code & Parsers.NEWLINE) > 0 || eof(io)) && break
    end
    return
end

function skipto!(layers, io::IO, cur, dest)
    cur >= dest && return
    for _ = 1:(dest-cur)
        readline!(layers, io)
    end
    return
end

#TODO: read Symbols directly
const READSPLITLINE_RESULT = Parsers.Result(String)
const DELIM_NEWLINE = Parsers.DELIMITED | Parsers.NEWLINE

readsplitline(io::IO) = readsplitline(Parsers.Delimited(Parsers.Quoted(), COMMA_NEWLINES), io)
function readsplitline(layers::Parsers.Delimited, io::IO)
    vals = Union{String, Missing}[]
    eof(io) && return vals
    col = 1
    result = READSPLITLINE_RESULT
    while true
        result.code = Parsers.SUCCESS
        Parsers.parse!(layers, io, result)
        # @debug "readsplitline!: result=$result"
        Parsers.ok(result.code) || throw(Error(result, 1, col))
        # @show result
        push!(vals, result.result)
        col += 1
        xor(result.code & DELIM_NEWLINE, Parsers.DELIMITED) == 0 && continue
        ((result.code & Parsers.NEWLINE) > 0 || eof(io)) && break
    end
    return vals
end

function rowpositions(io::IO, q::UInt8, e::UInt8)
    nl = Int64[]
    b = 0x00
    while !eof(io)
        b = Parsers.readbyte(io)
        if b === q
            while !eof(io)
                b = Parsers.readbyte(io)
                if b === e
                    if eof(io)
                        break
                    elseif e === q && Parsers.peekbyte(io) !== q
                        break
                    end
                    b = Parsers.readbyte(io)
                elseif b === q
                    break
                end
            end
        elseif b === UInt8('\n')
            push!(nl, position(io))
        elseif b === UInt8('\r')
            push!(nl, position(io))
            !eof(io) && Parsers.peekbyte(io) === UInt8('\n') && Parsers.readbyte(io)
        end
    end
    return nl
end

end # module
