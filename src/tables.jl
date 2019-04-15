Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = nothing

fieldindex(::Type{Missing}) = 3
fieldindex(::Type{Int64}) = 4
fieldindex(::Type{Float64}) = 5
fieldindex(::Type{Date}) = 6
fieldindex(::Type{DateTime}) = 7
fieldindex(::Type{Bool}) = 8
fieldindex(::Type{<:AbstractString}) = 0
fieldindex(::Type{Union{T, Missing}}) where {T} = fieldindex(T) + 5

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

    Column(rows::Int) = new(Vector{UInt64}(undef, rows), Vector{UInt32}(undef, rows))

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
        A = Column(missings(rows))
    elseif T === STRING || T === (STRING | MISSING) ||
           T === POOL || T === (POOL | MISSING)
        A = Column(rows)
    else
        A = Column(Vector{TYPECODES[T]}(undef, rows))
    end
    return A
end

function finalcolumn(col, T, buf, E, escapestrings, refs, i, pool, categorical)
    if T === STRING
        return StringArray{escapestrings ? E : String, 1}(buf, col.offsets, col.lengths)
    elseif T === (STRING | MISSING)
        return StringArray{Union{escapestrings ? E : String, Missing}, 1}(buf, col.offsets, col.lengths)
    elseif pool > 0.0 && (T === POOL || (T === (POOL | MISSING)))
        refs = refs[i]
        if !categorical
            return PooledArray(PooledArrays.RefArray(col.lengths), missingtype(T) ? refs : convert(Dict{String, UInt32}, refs))
        else
            if missingtype(T)
                delete!(refs, missing)
                pool = CategoricalPool(convert(Dict{String, UInt32}, refs))
                levels!(pool, sort(levels(pool)))
                return CategoricalArray{Union{Missing, String}, 1}(col.lengths, pool)
            else
                pool = CategoricalPool(convert(Dict{String, UInt32}, refs))
                levels!(pool, sort(levels(pool)))
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

function setchunkm!(A, row, chunkrows, tape, tapeidx, ncol, f)
    for rowoff = 0:chunkrows
        @inbounds offlen = tape[tapeidx + (rowoff * ncol * 2)]
        @inbounds A[row + rowoff] = missingvalue(offlen) ? missing : f(tape[tapeidx + (rowoff * ncol * 2) + 1])
    end
    return
end

function Tables.columns(f::File)
    t = time()
    ncol = length(f.names)
    ncol == 0 && return DataFrame()
    f.rows == 0 && return DataFrame(AbstractVector[Missing[] for i = 1:ncol], f.names)
    typecodes = f.typecodes
    columns = Column[makecolumn(typecodes[i], f.rows) for i = 1:ncol]
    tape = f.tape
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
                    @inbounds col.offsets[row + rowoff] = missingvalue(offlen) ? 0xfffffffffffffffe : offlen >> 16
                    @inbounds col.lengths[row + rowoff] = unsafe_trunc(UInt32, offlen & 0x000000000000ffff)
                end
            end
        end
        rowidx += ncol * 2 * chunk
    end
    finalcolumns = columns
    finaltypecodes = typecodes
    println(time() - t)
    return DataFrame([finalcolumn(finalcolumns[i], finaltypecodes[i], f.io.data, f.escapestring, f.escapestrings[i], f.refs, i, f.pool, f.categorical) for i = 1:ncol], f.names)
end
