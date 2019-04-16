function skiptoheader!(parsinglayers, io, row, header)
    while row < header
        while !eof(io)
            r = Parsers.parse(parsinglayers, io, Tuple{Ptr{UInt8}, Int})
            (r.code & Parsers.DELIMITED) > 0 && break
        end
        row += 1
    end
    return row
end

function countfields(io, parsinglayers)
    rows = 0
    result = Parsers.Result(Tuple{Ptr{UInt8}, Int})
    while !eof(io)
        result.code = Parsers.SUCCESS
        Parsers.parse!(parsinglayers, io, result)
        Parsers.ok(result.code) || throw(Error(result, rows+1, 1))
        rows += 1
        (result.code & Parsers.DELIMITED) > 0 && continue
        (newline(result.code) || eof(io)) && break
    end
    return rows
end

function datalayout_transpose(header, parsinglayers, io, datarow, normalizenames)
    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row = skiptoheader!(parsinglayers, io, 1, header)
        # io now at start of 1st header cell
        columnnames = [Parsers.parse(parsinglayers, io, String).result::String]
        columnpositions = [position(io)]
        datapos = position(io)
        rows = countfields(io, parsinglayers)
        
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(io)
            # skip to header column to read column names
            row = skiptoheader!(parsinglayers, io, 1, header)
            cols += 1
            push!(columnnames, Parsers.parse(parsinglayers, io, String).result::String)
            push!(columnpositions, position(io))
            readline!(parsinglayers, io)
        end
        Parsers.fastseek!(io, datapos)
    elseif isa(header, AbstractRange)
        # column names span several columns
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif eof(io)
        # emtpy file, use column names if provided
        datapos = position(io)
        columnpositions = Int[]
        columnnames = header
    else
        # column names provided explicitly or should be generated, they don't exist in data
        # skip to datarow
        row = skiptoheader!(parsinglayers, io, 1, datarow)
        # io now at start of 1st data cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : header[1]]
        columnpositions = [position(io)]
        datapos = position(io)
        rows = countfields(io, parsinglayers)
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while !eof(io)
            # skip to datarow column
            row = skiptoheader!(parsinglayers, io, 1, datarow)
            cols += 1
            push!(columnnames, isa(header, Integer) || isempty(header) ? "Column$cols" : header[cols])
            push!(columnpositions, position(io))
            readline!(parsinglayers, io)
        end
        Parsers.fastseek!(io, datapos)
    end
    return rows, makeunique(map(x->normalizenames ? normalizename(x) : Symbol(x), columnnames)), columnpositions
end

function datalayout(header::Integer, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
    # default header = 1
    if header <= 0
        # no header row in dataset; skip to data to figure out # of columns
        skipto!(parsinglayers, io, 1, datarow)
        datapos = position(io)
        row_vals = readsplitline(parsinglayers, io, cmt, ignorerepeated)
        Parsers.fastseek!(io, datapos)
        columnnames = [Symbol("Column$i") for i = eachindex(row_vals)]
    else
        skipto!(parsinglayers, io, 1, header)
        columnnames = makeunique([ismissing(x) ? Symbol("Column$i") : (normalizenames ? normalizename(x) : Symbol(x)) for (i, x) in enumerate(readsplitline(parsinglayers, io, cmt, ignorerepeated))])
        datarow != header+1 && skipto!(parsinglayers, io, header+1, datarow)
        datapos = position(io)
    end
    return columnnames, datapos
end

function datalayout(header::AbstractRange, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
    skipto!(parsinglayers, io, 1, first(header))
    columnnames = [x for x in readsplitline(parsinglayers, io, cmt, ignorerepeated)]
    for row = first(header):(last(header)-1)
        for (i,c) in enumerate([x for x in readsplitline(parsinglayers, io, cmt, ignorerepeated)])
            columnnames[i] *= "_" * c
        end
    end
    datarow != last(header)+1 && skipto!(parsinglayers, io, last(header)+1, datarow)
    datapos = position(io)
    return makeunique([normalizenames ? normalizename(nm) : Symbol(nm) for nm in columnnames]), datapos
end

function datalayout(header::Vector, parsinglayers, io, datarow, normalizenames, cmt, ignorerepeated)
    skipto!(parsinglayers, io, 1, datarow)
    datapos = position(io)
    if eof(io)
        columnnames = makeunique([normalizenames ? normalizename(nm) : Symbol(nm) for nm in header])
    else
        row_vals = readsplitline(parsinglayers, io, cmt, ignorerepeated)
        Parsers.fastseek!(io, datapos)
        if isempty(header)
            columnnames = [Symbol("Column$i") for i in eachindex(row_vals)]
        else
            length(header) == length(row_vals) || throw(ArgumentError("The length of provided header ($(length(header))) doesn't match the number of columns at row $datarow ($(length(row_vals)))"))
            columnnames = makeunique([normalizenames ? normalizename(nm) : Symbol(nm) for nm in header])
        end
    end
    return columnnames, datapos
end

const READLINE_RESULT = [Parsers.Result(Tuple{Ptr{UInt8}, Int})]
# readline! is used for implementation of skipto!
function readline!(layers, io::IO)
    eof(io) && return
    result = READLINE_RESULT[Threads.threadid()]
    while true
        result.code = Parsers.SUCCESS
        res = Parsers.parse!(layers, io, result)
        Parsers.ok(res.code) || throw(Parsers.Error(res))
        (newline(res.code) || eof(io)) && break
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

const READSPLITLINE_RESULT = [Parsers.Result(String)]
const DELIM_NEWLINE = Parsers.DELIMITED | Parsers.NEWLINE

readsplitline(io::IO; delim=",", cmt=nothing, ignorerepeated=false) = readsplitline(Parsers.Delimited(Parsers.Quoted(), delim; newline=true), io, cmt, ignorerepeated)
function readsplitline(layers::Parsers.Delimited, io::IO, cmt=nothing, ignorerepeated=false)
    vals = Union{String, Missing}[]
    eof(io) && return vals
    col = 1
    result = READSPLITLINE_RESULT[Threads.threadid()]
    while true
        consumecommentedline!(layers, io, cmt)
        ignorerepeated && Parsers.checkdelim!(layers, io)
        result.code = Parsers.SUCCESS
        Parsers.parse!(layers, io, result)
        # @debug "readsplitline!: result=$result"
        Parsers.ok(result.code) || throw(Error(Parsers.Error(io, result), 1, col))
        # @show result
        push!(vals, result.result)
        col += 1
        (result.code & Parsers.DELIMITED) > 0 && continue
        (newline(result.code) || eof(io)) && break
    end

    return vals
end

consumecommentedline!(layers, io, ::Nothing) = nothing
function consumecommentedline!(layers, io, comment::Parsers.Trie)
    result = READLINE_RESULT[Threads.threadid()]
    while Parsers.match!(comment, io, result, false)
        readline!(layers, io)
    end
end

consumecommentedline!(io, ::Nothing) = nothing
function consumecommentedline!(io, comment)
    result = READLINE_RESULT[Threads.threadid()]
    while Parsers.match!(comment, io, result, false) && !eof(io)
        b = Parsers.readbyte(io)
        while !eof(io) && b !== UInt8('\n') && b !== UInt8('\r')
            b = Parsers.readbyte(io)
        end
        b === UInt8('\r') && !eof(io) && Parsers.peekbyte(io) === UInt8('\n') && Parsers.readbyte(io)
    end
end

struct ByteValueCounter
    counts::Vector{Int64}
    ByteValueCounter() = new(zeros(Int64, 256))
end

function incr!(c::ByteValueCounter, b::UInt8)
    @inbounds c.counts[b] += 1
    return
end

function guessnrows(io::IO, oq::UInt8, cq::UInt8, eq::UInt8, source, delim, comment, debug)
    fs = bytesavailable(io)
    pos = position(io)
    nbytes = 0
    lastbytenewline = false
    nlines = 0
    bvc = ByteValueCounter()
    b = 0x00
    consumecommentedline!(io, comment)
    while !eof(io) && nlines < 10
        b = Parsers.readbyte(io)
        nbytes += 1
        if b === oq
            while !eof(io)
                b = Parsers.readbyte(io)
                nbytes += 1
                if b === eq
                    if eof(io)
                        break
                    elseif eq === cq && Parsers.peekbyte(io) !== cq
                        break
                    end
                    b = Parsers.readbyte(io)
                    nbytes += 1
                elseif b === cq
                    break
                end
            end
        elseif b === UInt8('\n')
            consumecommentedline!(io, comment)
            nlines += 1
            lastbytenewline = true
        elseif b === UInt8('\r')
            !eof(io) && Parsers.peekbyte(io) === UInt8('\n') && Parsers.readbyte(io)
            consumecommentedline!(io, comment)
            nlines += 1
            lastbytenewline = true
        else
            lastbytenewline = false
            incr!(bvc, b)
        end
    end
    nlines += !lastbytenewline

     if delim === nothing
        if isa(source, AbstractString) && endswith(source, ".tsv")
            d = "\t"
        elseif isa(source, AbstractString) && endswith(source, ".wsv")
            d = " "
        elseif nlines > 1
            d = nothing
            for attempted_delim in (',', '\t', ' ', '|', ';', ':')
                debug && @show attempted_delim
                debug && @show bvc.counts[Int(attempted_delim)]
                debug && @show nlines
                cnt = bvc.counts[Int(attempted_delim)]
                if cnt > 0 && cnt % nlines == 0
                    d = string(attempted_delim)
                    break
                end
            end
            d = something(d, ",")
        else
            d = ","
        end
    else
        d = string(delim)
    end
    Parsers.fastseek!(io, pos)
    guess = fs / (nbytes / nlines) * 1.1
    rowsguess = isfinite(guess) ? ceil(Int, guess) : 0
    return rowsguess, d
end
