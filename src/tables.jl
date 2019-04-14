# a mapping of TypeCode to its corresponding values array in Column struct
# enables doing things like getfield(A::CSV.Column, TYPEINDEX[col.typecode])
const TYPEINDEX = Dict(
    EMPTY    => 0,
    MISSING  => 6,
    INT      => 7,
    FLOAT    => 8,
    DATE     => 9,
    DATETIME => 10,
    BOOL     => 11,
    (INT | MISSING)      => 12,
    (FLOAT | MISSING)    => 13,
    (DATE | MISSING)     => 14,
    (DATETIME | MISSING) => 15,
    (BOOL | MISSING)     => 16,
)

mutable struct Column
    typecode::TypeCode
    escapestrings::Bool
    lastref::UInt32

    offsets::Vector{UInt64}
    lengths::Vector{UInt32}

    missings::Vector{Missing}
    ints::Vector{Int64}
    floats::Vector{Float64}
    dates::Vector{Date}
    datetimes::Vector{DateTime}
    bools::Vector{Bool}
    
    intsm::Vector{Union{Int64, Missing}}
    floatsm::Vector{Union{Float64, Missing}}
    datesm::Vector{Union{Date, Missing}}
    datetimesm::Vector{Union{DateTime, Missing}}
    boolsm::Vector{Union{Bool, Missing}}

    refs::Vector{UInt32}
    pool::Dict{String, UInt32}
    poolm::Dict{Union{String, Missing}, UInt32}

    Column(len) = new(EMPTY, false, 0)
end

function setinitialtypes!(columns::Vector{Column}, typecodes, rows)
    for (i, (T, col)) in enumerate(zip(typecodes, columns))
        if T !== EMPTY
            col.typecode = T
            T &= ~USER
            if T === STRING
                col.offsets = Vector{UInt64}(undef, rows)
                col.lengths = Vector{UInt32}(undef, rows)
            elseif pooled(T)
                col.refs = Vector{UInt32}(undef, rows)
                if missingtype(T)
                    col.poolm = Dict{Union{String, Missing}, UInt32}()
                else
                    col.pool = Dict{String, UInt32}()
                end
            elseif T !== STRING
                setfield!(col, TYPEINDEX[T], Vector{TYPECODES[T]}(undef, rows))
            end
        else
            col.offsets = Vector{UInt64}(undef, rows)
            col.lengths = Vector{UInt32}(undef, rows)
        end
    end
    return
end

Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = nothing

function Tables.columns(f::File)
    ncols = length(f.names)
    rows = f.rows
    columns = [Column(rows) for i = 1:ncols]
    setinitialtypes!(columns, f.typecodes, rows)
    buf = f.tape.buf
    tape = f.tape.tape
    idx = f.dataidx
    for row = 1:rows
        for col = 1:ncols
            @inbounds col = columns[col]
            type = typebits(col.typecode)
            @inbounds pos = tape[idx]
            @inbounds len = getlen(tape[idx + 1])
            idx += 2
            if type === EMPTY
                parseempty!(col, row, rows, buf, pos, len, NamedTuple())
            elseif type === MISSINGTYPE
                parsevalue!(Missing, col, row, rows)
            elseif type === INT
                parseint!(col, row, buf, pos, len, NamedTuple(), false, true)
            elseif type === FLOAT
                parsevalue!(Float64, col, row, rows)
            elseif type === DATE
                parsevalue!(Date, col, row, rows)
            elseif type === DATETIME
                parsevalue!(DateTime, col, row, rows)
            elseif type === BOOL
                parsevalue!(Bool, col, row, rows)
            elseif type === PSTRING || type === CSTRING
                # parsevalue!(CatStr, col, row, rows)
            else # STRING
                parsestring!(col, row, pos, len)
            end
        end
    end
    return columns
end

function parseempty!(col, row, rows, buf, pos, len, kwargs)
    x = detecttype(buf, pos, len, kwargs)
    T = typecode(x)
    # T = get(typemap, T, T)
    col.typecode = T
    if T === STRING
        # if pool[] > 0.0
        #     col.typecode = POOL
        #     col.refs = Vector{UInt32}(undef, rows)
        #     col.pool = Dict{String, UInt32}()
        #     adjust!(CatStr, col, col.refs, row, rows, p, i, k, pool, r, typemap)
        # else
            parsestring!(col, row, pos, len)
        # end
    else
        values = Vector{typeof(x)}(undef, rows)
        setfield!(col, TYPEINDEX[T], values)
        values[1] = x
    end
end

function parsestring!(col, row, pos, len)
    col.escapestrings = false # TODO
    col.typecode |= len == 0 ? MISSING : EMPTY
    @inbounds col.offsets[row] = len == 0 ? WeakRefStrings.MISSING_OFFSET : UInt64(pos)
    @inbounds col.lengths[row] = len
    return
end

@inline function parseint!(col, row, buf, pos, len, kwargs, strict, silencewarnings)
    x, ok = strtoi64(buf)
    if ok
        if x !== missing
            if !missingtype(col.typecode)
                @inbounds col.ints[row] = x
            else
                @inbounds col.intsm[row] = x
            end
        else
            # parsed a missing value
            if !missingtype(col.typecode)
                col.typecode |= MISSING
                col.intsm = convert(Vector{Union{Missing, Int64}}, col.ints)
            end
        end
    else
        if user(col.typecode)
            if !strict
                if !missingtype(col.typecode)
                    col.typecode |= MISSING
                    col.intsm = convert(Vector{Union{Missing, Int64}}, col.ints)
                end
                silencewarnings || println("warning int")
            else
                error("bad int")
            end
        else
            y, ok = parsefloat(buf, pos, len, kwargs)
            if ok
                if missingtype(col.typecode)
                    col.typecode = FLOAT | MISSING
                    col.floatsm = convert(Vector{Union{Float64, Missing}}, col.intsm)
                    col.floatsm[row] = y
                else
                    col.typecode = FLOAT
                    col.floats = convert(Vector{Float64}, col.ints)
                    col.floats[row] = y
                end
            else
                col.typecode = STRING | (missingtype(col.typecode) ? MISSING : EMPTY)
                parsestring!(col, row, pos, len)
            end
        end
    end
    if !user(col.typecode)
        @inbounds col.offsets[row] = len == 0 ? WeakRefStrings.MISSING_OFFSET : UInt64(pos)
        @inbounds col.lengths[row] = len
    end
    return
end

function parseint(buf, pos, len)
    len == 0 && return missing, true
    @inbounds b = buf[pos]
    neg = b === UInt8('-')
    pos += neg || (b === UInt8('+'))
    len -= neg
    x = 0
    @inbounds b = buf[pos] - UInt8('0')
    for _ = 1:len
        x = 10 * x + b
        pos += 1
        @inbounds b = buf[pos] - UInt8('0')
    end
    return neg ? -x : x, true
end

function strtoi64(buf)
    i = 1
    @inbounds b = buf[i]
    if b == UInt8('0')
        return 0, true
    end
    neg = b == UInt8('-')
    i += neg || b == UInt8('+')
    start = i
    while b == UInt8('0')
        i += 1
    end
    acc = 0
    sf = i
    @inbounds digit = buf[sf] - UInt8('0')
    while digit < 10
        acc = 10 * acc + digit
        sf += 1
        @inbounds digit = buf[sf] - UInt8('0')
    end
    if (sf > 0 || sf > start) && sf <= 19 && acc <= typemax(Int64)
        return neg ? -acc : acc, true
    else
        return 0, true
    end
end

function parsefloat(buf, pos, len, kwargs)

end

function parsedate(buf, pos, len, kwargs)

end

function parsedatetime(buf, pos, len, kwargs)

end

function parsebool(buf, pos, len, kwargs)

end

function detecttype(buf, pos, len, kwargs)
    len == 0 && return missing
    int, ok = parseint(buf, pos, len)
    ok && return int
    float, ok = parsefloat(buf, pos, len, kwargs)
    ok && return float
    if !haskey(kwargs, :dateformat)
        date, ok = parsedate(buf, pos, len)
        ok && return date
        datetime, ok = parsedatetime(buf, pos, len)
        ok && return datetime
    else
        T = timetype(kwargs.dateformat)
        dt = T == Date ? parsedate(buf, pos, len, kwargs) :
            parsedatetime(buf, pos, len, kwargs)
        ok && return dt
    end
    bool, ok = parsebool(buf, pos, len, kwargs)
    ok && return bool
    return WeakRefString(pointer(buf, pos), len)
end