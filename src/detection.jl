function detectheaderdatapos(buf, pos, len, oq, eq, cq, cmt, ignoreemptylines, header, datarow)
    headerpos = 0
    datapos = 1
    if header isa Integer
        if header <= 0
            # no header row in dataset; skip to data
            datapos = skiptorow(buf, pos, len, oq, eq, cq, 1, datarow)
        else
            headerpos = skiptorow(buf, pos, len, oq, eq, cq, 1, header)
            headerpos = checkcommentandemptyline(buf, headerpos, len, cmt, ignoreemptylines)
            datapos = skiptorow(buf, headerpos, len, oq, eq, cq, header, datarow)
        end
    elseif header isa AbstractVector{<:Integer}
        headerpos = skiptorow(buf, pos, len, oq, eq, cq, 1, header[1])
        headerpos = checkcommentandemptyline(buf, headerpos, len, cmt, ignoreemptylines)
        datapos = skiptorow(buf, headerpos, len, oq, eq, cq, header[1], datarow)
    elseif header isa Union{AbstractVector{Symbol}, AbstractVector{String}}
        datapos = skiptorow(buf, 1, len, oq, eq, cq, 1, datarow)
    else
        throw(ArgumentError("unsupported header argument: $header"))
    end
    datapos = max(1, checkcommentandemptyline(buf, datapos, len, cmt, ignoreemptylines))
    return headerpos, datapos
end

function detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, delim, cmt, ignoreemptylines)
    nbytes = 0
    lastbytenewline = false
    parsedanylines = false
    nlines = 0
    headerbvc = ByteValueCounter()
    bvc = ByteValueCounter()
    b = 0x00
    # don't parse header row if there isn't one: #508
    if headerpos > 0
        pos = headerpos
        # parsing our header row is useful for delimiter
        # detection, but we don't track nbytes here
        # because the header row size doesn't necessarily
        # correlate w/ data row size
        while pos <= len
            parsedanylines = true
            @inbounds b = buf[pos]
            pos += 1
            if b == oq
                while pos <= len
                    @inbounds b = buf[pos]
                    pos += 1
                    if b == eq
                        if pos > len
                            break
                        elseif eq == cq && buf[pos] != cq
                            break
                        end
                        @inbounds b = buf[pos]
                        pos += 1
                    elseif b == cq
                        break
                    end
                end
            elseif b == UInt8('\n')
                nlines += 1
                lastbytenewline = true
                break
            elseif b == UInt8('\r')
                pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
                nlines += 1
                lastbytenewline = true
                break
            else
                lastbytenewline = false
                incr!(headerbvc, b)
                incr!(bvc, b)
            end
        end
    end
    pos = max(1, checkcommentandemptyline(buf, datapos, len, cmt, ignoreemptylines))
    while pos <= len && nlines < 10
        parsedanylines = true
        @inbounds b = buf[pos]
        pos += 1
        nbytes += 1
        if b == oq
            while pos <= len
                @inbounds b = buf[pos]
                pos += 1
                nbytes += 1
                if b == eq
                    if pos > len
                        break
                    elseif eq == cq && buf[pos] != cq
                        break
                    end
                    @inbounds b = buf[pos]
                    pos += 1
                    nbytes += 1
                elseif b == cq
                    break
                end
            end
        elseif b == UInt8('\n')
            pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptylines)
            nlines += 1
            lastbytenewline = true
        elseif b == UInt8('\r')
            pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
            pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptylines)
            nlines += 1
            lastbytenewline = true
        else
            lastbytenewline = false
            incr!(bvc, b)
        end
    end
    nlines += parsedanylines && !lastbytenewline
    if delim == UInt8('\n')
        if nlines > 0
            d = UInt8('\n')
            for attempted_delim in (UInt8(','), UInt8('\t'), UInt8(' '), UInt8('|'), UInt8(';'), UInt8(':'))
                cnt = bvc.counts[Int(attempted_delim)]
                # @show Char(attempted_delim), cnt, nlines
                if cnt > 0 && cnt % nlines == 0
                    d = attempted_delim
                    break
                end
            end
            if d == UInt8('\n')
                maxcnt = 0
                for attempted_delim in (UInt8(','), UInt8('\t'), UInt8(' '), UInt8('|'), UInt8(';'), UInt8(':'))
                    cnt = headerbvc.counts[Int(attempted_delim)]
                    if cnt > maxcnt
                        d = attempted_delim
                        maxcnt = cnt
                    end
                end
            end
            if d == UInt8('\n')
                d = UInt8(',')
            end
        else
            d = UInt8(',')
        end
    else # delim explicitly provided
        d = delim
    end
    guess = ((len - datapos) / (nbytes / nlines))
    rowsguess = isfinite(guess) ? ceil(Int, guess) : 0
    return d, rowsguess
end

function detectcolumnnames(buf, headerpos, datapos, len, options, header, normalizenames)
    if header isa Union{AbstractVector{Symbol}, AbstractVector{String}}
        fields, pos = readsplitline(buf, datapos, len, options)
        if isempty(header)
            return [Symbol(:Column, i) for i = 1:length(fields)]
        elseif headerpos > 0
            length(header) == length(fields) || throw(ArgumentError("The length of provided header ($(length(header))) doesn't match the number of columns in data ($(length(fields)))"))
        end
        names = header
    elseif headerpos == 0
        fields, pos = readsplitline(buf, datapos, len, options)
        # generate column names
        return [Symbol(:Column, i) for i = 1:length(fields)]
    elseif header isa Integer
        names, pos = readsplitline(buf, headerpos, len, options)
    elseif header isa AbstractVector{<:Integer}
        names, pos = readsplitline(buf, headerpos, len, options)
        for row = 2:length(header)
            pos = skiptorow(buf, pos, len, options.oq, options.e, options.cq, 1, header[row] - header[row - 1])
            fields, pos = readsplitline(buf, pos, len, options)
            for (i, x) in enumerate(fields)
                names[i] *= "_" * x
            end
        end
    end
    return makeunique([normalizenames ? normalizename(x) : Symbol(x) for x in names])
end

struct ByteValueCounter
    counts::Vector{Int64}
    ByteValueCounter() = new(zeros(Int64, 256))
end

function incr!(c::ByteValueCounter, b::UInt8)
    @inbounds c.counts[b] += 1
    return
end

function skiptorow(buf, pos, len, oq, eq, cq, cur, dest)
    cur >= dest && return pos
    for _ = 1:(dest - cur)
        while pos <= len
            @inbounds b = buf[pos]
            pos += 1
            if b == oq
                while pos <= len
                    @inbounds b = buf[pos]
                    pos += 1
                    if b == eq
                        if pos > len
                            break
                        elseif eq == cq && buf[pos] != cq
                            break
                        end
                        @inbounds b = buf[pos]
                        pos += 1
                    elseif b == cq
                        break
                    end
                end
            elseif b == UInt8('\n')
                break
            elseif b == UInt8('\r')
                pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
                break
            end
        end
    end
    return pos
end

function readsplitline(buf, pos, len, options::Parsers.Options{ignorerepeated}) where {ignorerepeated}
    vals = String[]
    (pos > len || pos == 0) && return vals, pos
    col = 1
    while true
        if ignorerepeated
            pos = Parsers.checkdelim!(buf, pos, len, options)
        end
        _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
        push!(vals, columnname(buf, vpos, vlen, code, options, col))
        pos += tlen
        col += 1
        Parsers.delimited(code) && continue
        (Parsers.newline(code) || pos > len) && break
    end
    return vals, pos
end

function columnname(buf, vpos, vlen, code, options, i)
    if Parsers.sentinel(code)
        return "Column$i"
    elseif Parsers.escapedstring(code)
        return unescape(PointerString(pointer(buf, vpos), vlen), options.e)
    else
        return unsafe_string(pointer(buf, vpos), vlen)
    end
end

@inline function skipemptyrow(buf, pos, len)
    @inbounds b = buf[pos]
    if b == UInt8('\n')
        return pos + 1
    elseif b == UInt8('\r')
        if pos + 1 < len && buf[pos + 1] == UInt8('\n')
            return pos + 2
        else
            return pos + 1
        end
    end
    return pos
end

function checkcommentandemptyline(buf, pos, len, cmt, ignoreemptylines)
    cmtptr, cmtlen = cmt === nothing ? (C_NULL, 0) : cmt
    ptr = pointer(buf, pos)
    while pos <= len
        skipped = matched = false
        if ignoreemptylines
            newpos = skipemptyrow(buf, pos, len)
            if newpos > pos
                pos = newpos
                skipped = true
            end
        end
        if cmtlen > 0 && (pos + cmtlen - 1) <= len
            matched = Parsers.memcmp(ptr, cmtptr, cmtlen)
            if matched
                pos += cmtlen
                pos > len && break
                @inbounds b = buf[pos]
                while b != UInt8('\n') && b != UInt8('\r')
                    pos += 1
                    pos > len && break
                    @inbounds b = buf[pos]
                end
                b == UInt8('\r') && pos <= len && buf[pos + 1] == UInt8('\n') && (pos += 1)
                pos += 1
            end
        end
        (skipped | matched) || break
        ptr = pointer(buf, pos)
    end
    return pos
end

function findrowstarts!(buf, len, options::Parsers.Options{ignorerepeated}, cmt, ignoreemptylines, ranges, ncols) where {ignorerepeated}
    for i = 2:(length(ranges) - 1)
        pos = ranges[i]
        while pos <= len
            startpos = pos
            code = Parsers.ReturnCode(0)
            # assume not in quoted field; start parsing, count ncols + newline and if things match, return
            while pos <= len
                _, code, _, _, tlen = Parsers.xparse(String, buf, pos, len, options)
                pos += tlen
                if Parsers.newline(code)
                    pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptylines)
                    if ignorerepeated
                        pos = Parsers.checkdelim!(buf, pos, len, options)
                    end
                    # assume we found the correct start of the next row
                    ranges[i] = pos
                    break
                end
            end
            # now we read the next row and see if we get the right # of columns
            for _ = 1:ncols
                _, code, _, _, tlen = Parsers.xparse(String, buf, pos, len, options)
                pos += tlen
                pos > len && break
            end
            if Parsers.newline(code)
                # boom, we read a whole row and got correct # of columns
                break
            end
            # else, assume we were inside a quoted field:
            pos = startpos
            # if first byte is quotechar, need to check previous char for escapechar and if so, skip forward
            if buf[pos] == options.cq && buf[pos - 1] == options.e
                pos += 1
            end
            # start parsing until we find quotechar (ignoring escaped quote chars)
            cq, eq = options.cq, options.e
            while pos <= len
                b = buf[pos]
                pos += 1
                if b == eq
                    if pos > len
                        break
                    elseif eq == cq && buf[pos] != cq
                        break
                    end
                    b = buf[pos]
                    pos += 1
                elseif b == cq
                    break
                end
            end
            while pos <= len
                _, code, _, _, tlen = Parsers.xparse(String, buf, pos, len, options)
                pos += tlen
                if Parsers.newline(code)
                    pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptylines)
                    if ignorerepeated
                        pos = Parsers.checkdelim!(buf, pos, len, options)
                    end
                    # assume we found the correct start of the next row
                    ranges[i] = pos
                    break
                end
            end
            # in the worse case, we read to the end of the file; this shouldn't happen
            # because we're only identifying the starting byte positions of rows
            # in the middle of the file; if we hit this, it's most likely a corrupt file
            # with unquoted delimiters in string cells, or misquoted cells.
            # but if there's an actual bug here somehow, let's ask the user to tell us about it for now
            if pos > len
                @warn "$i; something went wrong trying to determine row positions for multithreading; it'd be very helpful if you could open an issue at https://github.com/JuliaData/CSV.jl/issues so package authors can investigate"
            end
        end
    end
    return
end

function detecttranspose(buf, pos, len, options, header, datarow, normalizenames)
    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row, pos = skiptofield!(buf, pos, len, options, 1, header)
        # io now at start of 1st header cell
        _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
        columnnames = [columnname(buf, vpos, vlen, code, options, 1)]
        pos += tlen
        row, pos = skiptofield!(buf, pos, len, options, header+1, datarow)
        columnpositions = Int64[pos]
        datapos = pos
        rows, pos = countfields(buf, pos, len, options)
        
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while pos <= len
            # skip to header column to read column names
            row, pos = skiptofield!(buf, pos, len, options, 1, header)
            cols += 1
            _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
            push!(columnnames, columnname(buf, vpos, vlen, code, options, cols))
            pos += tlen
            row, pos = skiptofield!(buf, pos, len, options, header+1, datarow)
            push!(columnpositions, pos)
            _, pos = countfields(buf, pos, len, options)
        end
    elseif isa(header, AbstractRange)
        # column names span several columns
        columnpositions = Int64[]
        columnnames = String[]
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif pos > len
        # emtpy file, use column names if provided
        datapos = pos
        columnpositions = Int64[]
        columnnames = header isa Vector && !isempty(header) ? String[string(x) for x in header] : []
        rows = 0
    else
        # column names provided explicitly or should be generated, they don't exist in data
        # skip to datarow
        row, pos = skiptofield!(buf, pos, len, options, 1, datarow)
        # io now at start of 1st data cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : string(header[1])]
        columnpositions = Int64[pos]
        datapos = pos
        rows, pos = countfields(buf, pos, len, options)
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while pos <= len
            # skip to datarow column
            row, pos = skiptofield!(buf, pos, len, options, 1, datarow)
            cols += 1
            push!(columnnames, isa(header, Integer) || isempty(header) ? "Column$cols" : string(header[cols]))
            push!(columnpositions, pos)
            _, pos = countfields(buf, pos, len, options)
        end
    end
    return rows, makeunique(map(x->normalizenames ? normalizename(x) : Symbol(x), columnnames))::Vector{Symbol}, columnpositions
end

function skiptofield!(buf, pos, len, options, row, header)
    while row < header
        while pos <= len
            _, code, _, _, tlen = Parsers.xparse(String, buf, pos, len, options)
            pos += tlen
            Parsers.delimited(code) && break
        end
        row += 1
    end
    return row, pos
end

function countfields(buf, pos, len, options)
    rows = 0
    while pos <= len
        _, code, _, _, tlen = Parsers.xparse(String, buf, pos, len, options)
        pos += tlen
        rows += 1
        Parsers.delimited(code) && continue
        (Parsers.newline(code) || pos > len) && break
    end
    return rows, pos
end
