# figure out at what byte position the header row(s) start and at what byte position the data starts
function detectheaderdatapos(buf, pos, len, oq, eq, cq, @nospecialize(cmt), ignoreemptyrows, @nospecialize(header), skipto)
    headerpos = 0
    datapos = 1
    if header isa Integer
        if header <= 0
            # no header row in dataset; skip to data
            datapos = skiptorow(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, 1, skipto)
        else
            headerpos = skiptorow(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, 1, header)
            datapos = skiptorow(buf, headerpos, len, oq, eq, cq, cmt, ignoreemptyrows, header, skipto)
        end
    elseif header isa AbstractVector{<:Integer}
        headerpos = skiptorow(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, 1, header[1])
        datapos = skiptorow(buf, headerpos, len, oq, eq, cq, cmt, ignoreemptyrows, header[1], skipto)
    elseif header isa Union{AbstractVector{Symbol}, AbstractVector{String}}
        datapos = skiptorow(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, 1, skipto)
    else
        throw(ArgumentError("unsupported header argument: $header"))
    end
    return headerpos, datapos
end

# this function scans a few rows and tracks the # of bytes and characters encountered
# it tries to guess a file's delimiter by which character showed up w/ the same frequency
# over all rows scanned; we use the average # of bytes per row w/ total length of the file
# to guess the total # of rows in the file
function detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, @nospecialize(cmt), ignoreemptyrows, delim=0x00)
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
    pos = max(1, checkcommentandemptyline(buf, datapos, len, cmt, ignoreemptyrows))
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
            pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptyrows)
            nlines += 1
            lastbytenewline = true
        elseif b == UInt8('\r')
            pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
            pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptyrows)
            nlines += 1
            lastbytenewline = true
        else
            lastbytenewline = false
            incr!(bvc, b)
        end
    end
    nlines += parsedanylines && !lastbytenewline
    d = delim
    if delim == UInt8('\n')
        if nlines > 0
            d = UInt8('\n')
            for attempted_delim in (UInt8(','), UInt8('\t'), UInt8(' '), UInt8('|'), UInt8(';'), UInt8(':'))
                cnt = bvc.counts[Int(attempted_delim) + 1]
                # @show Char(attempted_delim), cnt, nlines
                if cnt > 0 && cnt % nlines == 0
                    d = attempted_delim
                    break
                end
            end
            if d == UInt8('\n')
                maxcnt = 0
                for attempted_delim in (UInt8(','), UInt8('\t'), UInt8('|'), UInt8(';'), UInt8(':'))
                    cnt = headerbvc.counts[Int(attempted_delim) + 1]
                    # @show Char(attempted_delim), cnt, maxcnt
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
    end
    guess = ((len - datapos) / (nbytes / nlines))
    rowsguess = isfinite(guess) ? ceil(Int, guess) : 0
    return d, max(1, rowsguess)
end

struct ByteValueCounter
    counts::Vector{Int}
    ByteValueCounter() = new(zeros(Int, 256))
end

function incr!(c::ByteValueCounter, b::UInt8)
    @inbounds c.counts[b + 1] += 1
    return
end

# given the various header and normalization options, figure out column names for a file
function detectcolumnnames(buf, headerpos, datapos, len, options, @nospecialize(header), normalizenames, oq, eq, cq, cmt, ignoreemptyrows)::Vector{Symbol}
    if header isa Union{AbstractVector{Symbol}, AbstractVector{String}}
        fields, pos = readsplitline(buf, datapos, len, options)
        isempty(header) && return [Symbol(:Column, i) for i = 1:length(fields)]
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
            pos = skiptorow(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, 1, header[row] - header[row - 1])
            fields, pos = readsplitline(buf, pos, len, options)
            for (i, x) in enumerate(fields)
                names[i] *= "_" * x
            end
        end
    end
    return makeunique([normalizenames ? normalizename(x) : Symbol(x) for x in names])
end

# efficiently skip from `cur` to `dest` row
function skiptorow(buf, pos, len, oq, eq, cq, @nospecialize(cmt), ignoreemptyrows, cur, dest)
    nlines = Ref{Int}(0)
    pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptyrows, nlines)
    cur += nlines[]
    nlines[] = 0
    while cur < dest && pos < len
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
                typeof(buf) == ReversedBuf && pos <= len && buf[pos] == UInt8('\r') && (pos += 1)
                cur += 1
                break
            elseif b == UInt8('\r')
                pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
                cur += 1
                break
            end
        end
        pos = checkcommentandemptyline(buf, pos, len, cmt, ignoreemptyrows, nlines)
        cur += nlines[]
        nlines[] = 0
    end
    return pos
end

# read a single row, splitting cells on delimiters; used for parsing column names from header row(s)
function readsplitline(buf, pos, len, options)
    vals = String[]
    (pos > len || pos == 0) && return vals, pos
    col = 1
    while true
        res = Parsers.xparse(String, buf, pos, len, options)
        code = res.code
        push!(vals, columnname(buf, res.val, code, options, col))
        pos += res.tlen
        col += 1
        options.ignorerepeated && Parsers.newline(code) && break
        Parsers.delimited(code) && continue
        (Parsers.newline(code) || pos > len) && break
    end
    return vals, pos
end

function columnname(buf, poslen, code, options, i)
    if Parsers.sentinel(code) || poslen.len == 0
        return "Column$i"
    elseif Parsers.escapedstring(code)
        return Parsers.getstring(buf, poslen, options.e)
    else
        return unsafe_string(pointer(buf, poslen.pos), poslen.len)
    end
end

@inline function skipemptyrow(buf, pos, len)
    @inbounds b = buf[pos]
    if b == UInt8('\n')
        return pos + 1 + (typeof(buf) == ReversedBuf && (pos + 1) <= len && buf[pos + 1] == UInt8('\r'))
    elseif b == UInt8('\r')
        if pos + 1 < len && buf[pos + 1] == UInt8('\n')
            return pos + 2
        else
            return pos + 1
        end
    end
    return pos
end

const NLINES = Ref{Int}(0)

function checkcommentandemptyline(buf, pos, len, @nospecialize(cmt), ignoreemptyrows, nlines=NLINES)
    cmtptr, cmtlen = cmt === nothing ? (C_NULL, 0) : cmt
    ptr = pointer(buf, pos)
    while pos <= len
        skipped = matched = false
        if ignoreemptyrows
            newpos = skipemptyrow(buf, pos, len)
            if newpos > pos
                pos = newpos
                skipped = true
                nlines[] += 1
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
                nlines[] += 1
            end
        end
        (skipped | matched) || break
        ptr = pointer(buf, pos)
    end
    return pos
end

struct ColumnProperties
    typecode::UInt8
    maxstringsize::UInt8
end
ColumnProperties(T) = ColumnProperties(T, 0x00)

@inline function (cp::ColumnProperties)(_, _, _, S::UInt8)
    T = cp.typecode
    if T === S
        return cp
    elseif T === NEEDSTYPEDETECTION
        return ColumnProperties(S, cp.maxstringsize)
    elseif S === NEEDSTYPEDETECTION
        return cp
    elseif T === MISSING
        return ColumnProperties(S, cp.maxstringsize)
    elseif S === MISSING
        return cp
    elseif isinttypecode(T) && isinttypecode(S)
        return ColumnProperties(promote_typecode(T, S), cp.maxstringsize)
    elseif isinttypecode(T) && S === FLOAT64
        return ColumnProperties(S, cp.maxstringsize)
    elseif T === FLOAT64 && isinttypecode(S)
        return cp
    else
        return ColumnProperties(STRING, cp.maxstringsize)
    end
end

function findnextnewline(pos, stop, buf, opts)
    while pos < stop
        res = Parsers.xparse(String, buf, pos, stop, opts)
        pos += res.tlen
        Parsers.newline(res.code) && return pos
    end
    return stop
end

function findchunkrowstart(ranges, i, buf, opts, typemap, downcast, ncols, rows_to_check, columns, origcoltypes, columnlock, @nospecialize(stringtype), totalbytes, totalrows, succeeded)
    pos = ranges[i]
    len = ranges[i + 1] - 1
    addtrailingcolumn = false # set if the file ends with an empty column with no trailing newline
    if i == length(ranges)-1
        len += 1 # correctly handle the absence of a trailing newline
        addtrailingcolumn = Parsers.delimited(Parsers.xparse(String, buf, len, len, opts).code)
    end
    nextrowpos = 0
    startpos = pos
    code = Parsers.ReturnCode(0)
    attempted_quoted = false
    while true
        # now we read the next `rows_to_check` rows and see if we get the roughly the right # of columns
        rowstartpos = pos
        parsedncols = rowsparsed = 0
        columnprops = Vector{ColumnProperties}(undef, ncols)
        for i = 1:ncols
            if origcoltypes[i] === NeedsTypeDetection
                columnprops[i] = ColumnProperties(NEEDSTYPEDETECTION)
            else
                # if column type is already set, that means user provided manually
                # so we don't care about sampling that column's values for type
                columnprops[i] = ColumnProperties(0x00)
            end
        end
        for _ = 1:rows_to_check
            n = 1
            numcolsthisrow = 0
            while pos <= len
                res = Parsers.xparse(String, buf, pos, len, opts)
                poslen, code, tlen = res.val, res.code, res.tlen
                vpos, vlen = poslen.pos, poslen.len
                if n <= ncols && !Parsers.sentinel(code)
                    cp = columnprops[n]
                    if cp.typecode > 0x00
                        plen = unsafe_trunc(UInt8, poslen.len)
                        if plen > cp.maxstringsize
                            columnprops[n] = cp = ColumnProperties(cp.typecode, plen)
                        end
                        cp2 = detect(cp, buf, vpos, vpos + vlen - 1, opts, true, downcast)
                        if cp != cp2
                            columnprops[n] = cp2
                        end
                    end
                end
                pos += tlen
                numcolsthisrow += 1
                Parsers.newline(code) && break
                n += 1
            end
            rowsparsed += ((pos < len) | (numcolsthisrow != 0)) # trailing newline does not count
            parsedncols += numcolsthisrow
        end
        parsedncols += addtrailingcolumn
        lock(columnlock) do
            for i = 1:ncols
                cp = columnprops[i]
                col = columns[i]
                if cp.typecode > 0x00
                    type = something(promote_types(col.type, something(TYPES[cp.typecode], stringtype)), stringtype)
                    if type === stringtype
                        type = pickstringtype(stringtype, cp.maxstringsize)
                    end
                    col.type = get(typemap, type, type)
                end
            end
        end
        f40 = ncols * 0.025
        if (ncols - f40) <= (parsedncols / rowsparsed) <= (ncols + f40)
            # ok, seems like we figured out the right start for parsing on this chunk
            Threads.atomic_add!(totalbytes, Int(pos - rowstartpos))
            Threads.atomic_add!(totalrows, rowsparsed)
            break
        end
        if attempted_quoted
            # wah, wah, waaaah. we tried starting outside a quoted field
            # we started assuming we were _inside_, but can't seem to parse
            # anything that matches roughly what we're expecting, bail on
            # multithreaded parsing
            succeeded[] = false
            break
        end
        # else, assume we were inside a quoted field:
        pos = startpos
        # if first byte is quotechar, need to check previous char for escapechar and if so, skip forward
        if buf[pos] == opts.cq && buf[pos - 1] == opts.e
            pos += 1
        end
        # start parsing until we find quotechar (ignoring escaped quote chars)
        cq, eq = opts.cq, opts.e
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
        # ok, we made it out of a quoted field
        attempted_quoted = true
        pos = nextrowpos = findnextnewline(pos, len, buf, opts)
    end
    return ifelse(nextrowpos==0, startpos, nextrowpos)
end

# here we try to "chunk" up a file; given the equally spaced out byte positions in `ranges`, we start at each
# byte position and start parsing until we find the start of the next row; if the next rows all verify w/ the
# right # of expected columns then we move on to the next file chunk byte position. If we fail, we start over
# at the byte position, assuming we were in a quoted field (and encountered a newline inside the quoted
# field the first time through)
function findrowstarts!(buf, opts, ranges, ncols, columns, @nospecialize(stringtype), typemap, downcast, rows_to_check=5)
    totalbytes = Threads.Atomic{Int}(0)
    totalrows = Threads.Atomic{Int}(0)
    succeeded = Threads.Atomic{Bool}(true)
    lock = ReentrantLock()
    origcoltypes = Type[col.type for col in columns]
    stop = last(ranges)
    @sync for i in 2:(length(ranges) - 1)
        # preprocessing of ranges: ensure that each range starts and ends at a newline
        Threads.@spawn begin
            ranges[i] = findnextnewline(ranges[i], stop, buf, opts)
        end
    end
    # remove ranges starting after the end, if any
    new_last_idx = length(ranges)-1
    while ranges[new_last_idx] > stop
        new_last_idx -= 1
    end
    resize!(ranges, new_last_idx+1)
    ranges[end] = stop
    unique!(ranges) # in case multiple tasks start on the same row
    newranges = similar(ranges)
    N = length(ranges) - 1
    @sync for i in 2:N
        Threads.@spawn begin
            newranges[i] = findchunkrowstart(ranges, i, buf, opts, typemap, downcast, ncols, rows_to_check, columns, origcoltypes, lock, stringtype, totalbytes, totalrows, succeeded)
        end
    end
    @inbounds for i in 2:N # this update occurs after the parallel loop to avoid a race condition
        ranges[i] = newranges[i]
    end
    return totalbytes[] / totalrows[], succeeded[]
end

function detecttranspose(buf, pos, len, options, @nospecialize(header), skipto, normalizenames)
    if isa(header, Integer) && header > 0
        # skip to header column to read column names
        row, pos = skiptofield!(buf, pos, len, options, 1, header)
        # io now at start of 1st header cell
        res = Parsers.xparse(String, buf, pos, len, options)
        columnnames = [columnname(buf, res.val, res.code, options, 1)]
        pos += res.tlen
        row, pos = skiptofield!(buf, pos, len, options, header+1, skipto)
        columnpositions = Int[pos]
        datapos = pos
        rows, pos = countfields(buf, pos, len, options)
        endpositions = Int[pos]
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while pos <= len
            # skip to header column to read column names
            row, pos = skiptofield!(buf, pos, len, options, 1, header)
            cols += 1
            res = Parsers.xparse(String, buf, pos, len, options)
            push!(columnnames, columnname(buf, res.val, res.code, options, cols))
            pos += res.tlen
            row, pos = skiptofield!(buf, pos, len, options, header+1, skipto)
            push!(columnpositions, pos)
            _, pos = countfields(buf, pos, len, options)
            push!(endpositions, pos)
        end
    elseif isa(header, AbstractRange)
        # column names span several columns
        throw(ArgumentError("not implemented for transposed csv files"))
    elseif pos > len
        # empty file, use column names if provided
        datapos = pos
        columnpositions = Int[]
        endpositions = Int[]
        columnnames = header isa Vector && !isempty(header) ? String[string(x) for x in header] : []
        rows = 0
    else
        # column names provided explicitly or should be generated, they don't exist in data
        # skip to skipto
        row, pos = skiptofield!(buf, pos, len, options, 1, skipto)
        # io now at start of 1st data cell
        columnnames = [isa(header, Integer) || isempty(header) ? "Column1" : string(header[1])]
        columnpositions = Int[pos]
        datapos = pos
        rows, pos = countfields(buf, pos, len, options)
        endpositions = Int[pos]
        # we're now done w/ column 1, if EOF we're done, otherwise, parse column 2's column name
        cols = 1
        while pos <= len
            # skip to skipto column
            row, pos = skiptofield!(buf, pos, len, options, 1, skipto)
            cols += 1
            push!(columnnames, isa(header, Integer) || isempty(header) ? "Column$cols" : string(header[cols]))
            push!(columnpositions, pos)
            _, pos = countfields(buf, pos, len, options)
            push!(endpositions, pos)
        end
    end
    return rows, makeunique(map(x -> normalizenames ? normalizename(x) : Symbol(x), columnnames))::Vector{Symbol}, columnpositions, endpositions
end

function skiptofield!(buf, pos, len, options, row, header)
    while row < header
        while pos <= len
            res = Parsers.xparse(String, buf, pos, len, options)
            pos += res.tlen
            Parsers.delimited(res.code) && break
        end
        row += 1
    end
    return row, pos
end

function countfields(buf, pos, len, options)
    rows = 0
    while pos <= len
        res = Parsers.xparse(String, buf, pos, len, options)
        pos += res.tlen
        rows += 1
        Parsers.delimited(res.code) && continue
        (Parsers.newline(res.code) || pos > len) && break
    end
    return rows, pos
end
