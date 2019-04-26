Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(f.names, f.types)

# fieldindex maps a given column type to its corresponding typed array field in Column
fieldindex(::Type{Missing}) = 3
fieldindex(::Type{Int64}) = 4
fieldindex(::Type{Float64}) = 5
fieldindex(::Type{Date}) = 6
fieldindex(::Type{DateTime}) = 7
fieldindex(::Type{Bool}) = 8
fieldindex(::Type{<:AbstractString}) = 0
fieldindex(::Type{Union{T, Missing}}) where {T} = fieldindex(T) + 5

# Column is a special container that only initializes an array of one type
# in Tables.columns, it allows us to have a type-stable Vector{Column} for all columns
# and then based on a column's typecode, we can access the internal typed array as needed
mutable struct Column
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

    Column(rows::Integer) = new(Vector{UInt64}(undef, rows), Vector{UInt32}(undef, rows))

    function Column(A::AbstractVector{T}) where {T}
        c = new()
        setfield!(c, fieldindex(T), A)
        return c
    end
end

function makecolumn(T, rows)
    if T === EMPTY
        A = Column(Missing[])
    elseif T === MISSINGTYPE
        A = Column(fill(missing, rows))
    elseif T === STRING || T === (STRING | MISSING) ||
           T === POOL || T === (POOL | MISSING)
        A = Column(rows)
    else
        A = Column(Vector{TYPECODES[T]}(undef, rows))
    end
    return A
end

function finalcolumn(col, T, buf, QS, escapedstrings, refs, i, pool, categorical, categoricalpools)
    if T === STRING
        return StringArray{escapedstrings ? QS : String, 1}(buf, col.offsets, col.lengths)
    elseif T === (STRING | MISSING)
        return StringArray{Union{escapedstrings ? QS : String, Missing}, 1}(buf, col.offsets, col.lengths)
    elseif pool > 0.0 && (T === POOL || (T === (POOL | MISSING)))
        refs = refs[i]
        if !categorical
            if missingtype(T)
                missingref = maximum(col.lengths) + UInt32(1)
                replace!(col.lengths, UInt32(0) => missingref)
                refs[missing] = missingref
            else
                refs = convert(Dict{String, UInt32}, refs)
            end
            return PooledArray(PooledArrays.RefArray(col.lengths), refs)
        else
            pool = categoricalpools[i]
            if missingtype(T)
                return CategoricalArray{Union{Missing, String}, 1}(col.lengths, pool)
            else
                return CategoricalArray{String, 1}(col.lengths, pool)
            end
        end
    else
        return getfield(col, fieldindex(TYPECODES[T]))
    end
end

function setchunk!(A, row, chunkrows, tape, tapeidx, ncol, f)
    for rowoff = 0:chunkrows
        @inbounds A[row + rowoff] = f(tape[tapeidx + (rowoff * ncol * 2) + 1])
    end
    return
end

function setchunk!(A::Vector{Float64}, row, chunkrows, tape, tapeidx, ncol, f)
    for rowoff = 0:chunkrows
        @inbounds offlen = tape[tapeidx + (rowoff * ncol * 2)]
        @inbounds x = tape[tapeidx + (rowoff * ncol * 2) + 1]
        @inbounds A[row + rowoff] = intvalue(offlen) ? Float64(int64(x)) : float64(x)
    end
    return
end

function setchunkm!(A, row, chunkrows, tape, tapeidx, ncol, f)
    for rowoff = 0:chunkrows
        @inbounds offlen = tape[tapeidx + (rowoff * ncol * 2)]
        @inbounds A[row + rowoff] = missingvalue(offlen) ? missing : f(tape[tapeidx + (rowoff * ncol * 2) + 1])
    end
    return
end

function setchunkm!(A::Vector{Union{Missing, Float64}}, row, chunkrows, tape, tapeidx, ncol, f)
    for rowoff = 0:chunkrows
        @inbounds offlen = tape[tapeidx + (rowoff * ncol * 2)]
        if !missingvalue(offlen)
            @inbounds x = tape[tapeidx + (rowoff * ncol * 2) + 1]
            @inbounds A[row + rowoff] = intvalue(offlen) ? Float64(int64(x)) : float64(x)
        end
    end
    return
end

function Tables.columns(f::File)
    ncol = f.cols
    ncol == 0 && return DataFrame()
    f.rows == 0 && return DataFrame(AbstractVector[Missing[] for i = 1:ncol], f.names)
    typecodes = f.typecodes
    columns = Column[makecolumn(typecodes[i], f.rows) for i = 1:ncol]
    escapedstrings = fill(false, ncol)
    tape = f.tape
    # the idea of chunking here is to make sure we access as close to a "page" of memory from tape
    # at a time while storing the values in the output columns; this hasn't been rigourously tested for a performance increase
    chunk = max(1, div(256, ncol))
    totalrows = f.rows
    rowidx = 1
    for row = 1:chunk:f.rows
        chunkrows = min(chunk - 1, totalrows - row)
        for j = 1:ncol
            @inbounds col = columns[j]
            @inbounds T = typecodes[j]
            off = (j - 1) * 2
            if T === INT
                setchunk!(col.ints, row, chunkrows, tape, rowidx + off, ncol, int64)
            elseif T === (INT | MISSING)
                setchunkm!(col.intsm, row, chunkrows, tape, rowidx + off, ncol, int64)
            elseif T === FLOAT
                setchunk!(col.floats, row, chunkrows, tape, rowidx + off, ncol, float64)
            elseif T === (FLOAT | MISSING)
                setchunkm!(col.floatsm, row, chunkrows, tape, rowidx + off, ncol, float64)
            elseif T === DATE
                setchunk!(col.dates, row, chunkrows, tape, rowidx + off, ncol, date)
            elseif T === (DATE | MISSING)
                setchunkm!(col.datesm, row, chunkrows, tape, rowidx + off, ncol, date)
            elseif T === DATETIME
                setchunk!(col.datetimes, row, chunkrows, tape, rowidx + off, ncol, datetime)
            elseif T === (DATETIME | MISSING)
                setchunkm!(col.datetimesm, row, chunkrows, tape, rowidx + off, ncol, datetime)
            elseif T === BOOL
                setchunk!(col.bools, row, chunkrows, tape, rowidx + off, ncol, bool)
            elseif T === (BOOL | MISSING)
                setchunkm!(col.boolsm, row, chunkrows, tape, rowidx + off, ncol, bool)
            elseif T === MISSINGTYPE
                # don't need to do anything
            elseif T === POOL || T === (POOL | MISSING)
                setchunk!(col.lengths, row, chunkrows, tape, rowidx + off, ncol, ref)
            else
                for rowoff = 0:chunkrows
                    @inbounds offlen = tape[rowidx + off + (rowoff * ncol * 2)]
                    if missingvalue(offlen)
                        @inbounds col.offsets[row + rowoff] = WeakRefStrings.MISSING_OFFSET
                    else
                        # minus 1 because the tape stores the byte *position* and StringArray wants the *offset*
                        @inbounds col.offsets[row + rowoff] = getpos(offlen) - 1
                        @inbounds col.lengths[row + rowoff] = unsafe_trunc(UInt32, getlen(offlen))
                        if escapedvalue(offlen)
                            @inbounds escapedstrings[j] = true
                        end
                    end
                end
            end
        end
        rowidx += ncol * 2 * chunk
    end
    finalcolumns = columns
    finaltypecodes = typecodes
    return DataFrame([finalcolumn(finalcolumns[i], finaltypecodes[i], f.buf, f.escapedstringtype, escapedstrings[i], f.refs, i, f.pool, f.categorical, f.categoricalpools) for i = 1:ncol], f.names; copycols=false)
end
