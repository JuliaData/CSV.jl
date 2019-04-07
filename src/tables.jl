Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = nothing

# a mapping of TypeCode to its corresponding values array in Array struct
# enables doing things like getfield(A::CSV.Array, TYPEINDEX[col.typecode])
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

mutable struct Array
    offsets::Vector{UInt64}
    lengths::Vector{UInt32}
    typecode::TypeCode
    escapestrings::Bool
    lastref::UInt32

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

    Array(len) = new(Vector{UInt64}(undef, len), Vector{UInt32}(undef, len), EMPTY, false, 0)
end

# "materialize" a CSV.Array as a real Array/CategoricalArray/PooledArray/StringArray
# and resize the resulting array according to the # of rows we actually parsed
function vec(buf, x::Array, rows, E, pool)
    if pooled(x.typecode)
        if pool !== false
            A = PooledArray(PooledArrays.RefArray(x.refs), missingtype(x.typecode) ? x.poolm : x.pool)
        else
            if missingtype(x.typecode)
                delete!(x.poolm, missing)
                pool = CategoricalPool(convert(Dict{String, UInt32}, x.poolm))
                levels!(pool, sort(levels(pool)))
                A = CategoricalArray{Union{Missing, String}, 1}(x.refs, pool)
            else
                pool = CategoricalPool(x.pool)
                levels!(pool, sort(levels(pool)))
                A = CategoricalArray{String, 1}(x.refs, pool)
            end
        end
    elseif x.typecode === STRING
        A = StringArray{x.escapestrings ? E : String, 1}(buf, x.offsets, x.lengths)
    elseif x.typecode === (STRING | MISSING)
        A = StringArray{Union{x.escapestrings ? E : String, Missing}, 1}(buf, x.offsets, x.lengths)
    elseif x.typecode === EMPTY
        A = Missing[]
    else
        A = getfield(x, TYPEINDEX[x.typecode])
    end
    return resize!(A, rows)
end

function setinitialtypes!(columns::Vector{Array}, typecodes, rowsguess)
    for (i, T) in enumerate(typecodes)
        if T !== EMPTY
            @inbounds col = columns[i]
            col.typecode = T
            if T === STRING
            elseif pooled(T)
                col.refs = Vector{UInt32}(undef, rowsguess)
                if missingtype(T)
                    col.poolm = Dict{Union{String, Missing}, UInt32}()
                else
                    col.pool = Dict{String, UInt32}()
                end
            else
                setfield!(col, TYPEINDEX[T], Vector{TYPECODES[T]}(undef, rowsguess))
            end
        end
    end
    return
end

function Tables.columns(f::File{transpose}) where {transpose}
    ncols = length(f.names)
    io = f.io
    parsinglayers = f.parsinglayers
    kwargs = f.kwargs
    rowsguess = f.rowsguess
    typemap = f.typemap
    if transpose
        positions = copy(f.positions)
    else
        Parsers.fastseek!(io, f.datapos)
    end
    lastcode = Ref{Parsers.ReturnCode}()
    columns = [Array(rowsguess) for i = 1:ncols]
    setinitialtypes!(columns, f.typecodes, f.rowsguess)
    pool = Ref{Float64}(f.pool === true || f.categorical === true ? 1.0 :
                        f.pool isa Float64 ? f.pool :
                        f.categorical isa Float64 ? f.categorical : 0.0)
    row = 0
    if !eof(io)
        while row < f.limit
            row += 1
            consumecommentedline!(parsinglayers, io, f.cmt)
            f.ignorerepeated && Parsers.checkdelim!(parsinglayers, io)
            for i = 1:ncols
                if transpose
                    @inbounds Parsers.fastseek!(io, positions[i])
                end
                @inbounds col = columns[i]
                type = col.typecode
                if type === EMPTY
                    parsevalue!(Union{}, col, col.lengths, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === MISSING
                    parsevalue!(Missing, col, col.missings, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === INT
                    parsevalue!(Int64, col, col.ints, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === (INT | MISSING)
                    parsevalue!(Union{Int64, Missing}, col, col.intsm, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === FLOAT
                    parsevalue!(Float64, col, col.floats, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === (FLOAT | MISSING)
                    parsevalue!(Union{Float64, Missing}, col, col.floatsm, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === DATE
                    parsevalue!(Date, col, col.dates, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === (DATE | MISSING)
                    parsevalue!(Union{Date, Missing}, col, col.datesm, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === DATETIME
                    parsevalue!(DateTime, col, col.datetimes, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === (DATETIME | MISSING)
                    parsevalue!(Union{DateTime, Missing}, col, col.datetimesm, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === BOOL
                    parsevalue!(Bool, col, col.bools, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif type === (BOOL | MISSING)
                    parsevalue!(Union{Bool, Missing}, col, col.boolsm, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                elseif pooled(type)
                    parsevalue!(CatStr, col, col.refs, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                else # STRING
                    parsevalue!(String, col, col.lengths, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode)
                end
                if transpose
                    @inbounds positions[i] = position(io)
                else
                    if i < ncols
                        if newline(lastcode[])
                            f.silencewarnings || notenoughcolumns(i, ncols, row)
                            for j = (i + 1):ncols
                                @inbounds adjustmissing!(columns[j], row)
                            end
                            break
                        end
                    else
                        if !eof(io) && !newline(lastcode[])
                            f.silencewarnings || toomanycolumns(ncols, row)
                            readline!(parsinglayers, io)
                        end
                    end
                end
            end
            eof(io) && break
            if row > rowsguess
                println("WARNING: didn't pre-allocate enough while parsing: preallocated=$(rowsguess)")
                break
            end
        end
    end
    finalrow = row
    return DataFrame(AbstractVector[vec(io.data, x, finalrow, f.escapestring, f.pool) for x in columns], f.names)
end

@noinline notenoughcolumns(cols, ncols, row) = println("warning: only found $cols / $ncols columns on row: $row. Filling remaining columns with `missing`...")
@noinline toomanycolumns(cols, row) = println("warning: parsed expected $cols columns, but didn't reach end of line on row: $row. Ignoring rest of row...")

function parsevalue!(::Type{T}, col, values, row, rowsguess, parsinglayers, io, kwargs, pool, typemap, lastcode) where {T}
    r = parsevalue(T, parsinglayers, io, kwargs)
    adjust!(T, col, values, row, rowsguess, parsinglayers, io, kwargs, pool, r, typemap)
    @inbounds lastcode[] = r.code
    @inbounds col.offsets[row] = sentinel(r.code) ? WeakRefStrings.MISSING_OFFSET : UInt64(r.pos)
    @inbounds col.lengths[row] = UInt32(r.len)
    return
end

@inline parsevalue(::Union{Type{String}, Type{CatStr}}, parsinglayers, io, kwargs) = Parsers.parse(parsinglayers, io, Tuple{Ptr{UInt8}, Int}; kwargs...)
@inline parsevalue(::Union{Type{Missing}, Type{Union{}}}, parsinglayers, io, kwargs) = detecttype(io, parsinglayers, kwargs)
@inline parsevalue(::Type{T}, parsinglayers, io, kwargs) where {T} = Parsers.parse(parsinglayers, io, Base.nonmissingtype(T); kwargs...)

@inline function adjust!(::Type{String}, col, values, row, rowsguess, p, i, k, pool, r, typemap)
    if escapestring(r.code)
        col.escapestrings = true
    end
    if sentinel(r.code)
        col.typecode |= MISSING
    end
    return
end

@inline function getref!(x::Dict, key::Tuple{Ptr{UInt8}, Int}, new::UInt32, col)
    index = Base.ht_keyindex2!(x, key)
    if index > 0
        @inbounds found_key = x.vals[index]
        return found_key::UInt32
    else
        @inbounds Base._setindex!(x, new, unsafe_string(key[1], key[2]), -index)
        col.lastref = new
        return new
    end
end

@inline function adjust!(::Type{CatStr}, col, values, row, rowsguess, p, i, k, pool, r, typemap)
    if sentinel(r.code)
        if !missingtype(col.typecode)
            col.typecode |= MISSING
            col.poolm = convert(Dict{Union{Missing, String}, UInt32}, col.pool)
            col.poolm[missing] = UInt32(0)
        end
        ref = UInt32(0)
        curpool = col.poolm
    elseif missingtype(col.typecode)
        curpool = col.poolm
        ref = getref!(col.poolm, r.result, col.lastref + UInt32(1), col)
    else
        curpool = col.pool
        ref = getref!(col.pool, r.result, col.lastref + UInt32(1), col)
    end
    if (length(curpool) / rowsguess) > pool[]
        col.typecode = STRING | (missingtype(col.typecode) ? MISSING : EMPTY)
        adjust!(String, col, col.lengths, row, rowsguess, p, i, k, pool, r, typemap)
    else
        @inbounds values[row] = ref
    end
    return
end

@inline function adjust!(::Type{Union{}}, col, values, row, rowsguess, p, i, k, pool, r, typemap)
    T = typecode(r.result)
    T = get(typemap, T, T)
    col.typecode = T
    if T === STRING
        if pool[] > 0.0
            col.typecode = POOL
            col.refs = Vector{UInt32}(undef, rowsguess)
            col.pool = Dict{String, UInt32}()
            adjust!(CatStr, col, col.refs, row, rowsguess, p, i, k, pool, r, typemap)
        else
            adjust!(String, col, col.lengths, row, rowsguess, p, i, k, pool, r, typemap)
        end
    else
        values = Vector{typeof(r.result)}(undef, rowsguess)
        setfield!(col, TYPEINDEX[T], values)
        values[1] = r.result
    end
    return
end

@inline function adjust!(::Type{Missing}, col, values::Vector{Missing}, row, rowsguess, p, i, k, pool, r, typemap)
    T = typecode(r.result)
    T = get(typemap, T, T)
    if T === STRING
        if pool[] > 0.0
            col.typecode = POOL | MISSING
            col.refs = Vector{UInt32}(undef, rowsguess)
            col.poolm = Dict{Union{String, Missing}, UInt32}()
            adjust!(CatStr, col, col.refs, row, rowsguess, p, i, k, pool, r, typemap)
        else
            col.typecode = STRING | MISSING
            adjust!(String, col, col.lengths, row, rowsguess, p, i, k, pool, r, typemap)
        end
    elseif T !== MISSING
        col.typecode |= T
        values = Vector{Union{Missing, typeof(r.result)}}(undef, rowsguess)
        values[row] = r.result
        # don't need to copyto! or convert since all previous values were `missing` and default value of Vector{Union{T, Missing}} is `missing`
        setfield!(col, TYPEINDEX[col.typecode], values)
    end
    return
end

@inline function adjust!(::Type{T}, col, values::Vector{T}, row, rowsguess, p, i, k, pool, r, typemap) where {T}
    if !Parsers.ok(r.code)
        col.typecode = STRING
        adjust!(String, col, col.lengths, row, rowsguess, p, i, k, pool, r, typemap)
    elseif sentinel(r.code)
        newvalues = convert(Vector{Union{Missing, T}}, values)
        col.typecode |= MISSING
        @inbounds newvalues[row] = missing
        setfield!(col, TYPEINDEX[col.typecode], newvalues)
    else
        @inbounds values[row] = r.result
    end
    return
end

@inline function adjust!(::Type{Union{T, Missing}}, col, values::Vector{T}, row, rowsguess, p, i, k, pool, r, typemap) where {T}
    if !Parsers.ok(r.code)
        col.typecode = STRING | MISSING
        adjust!(String, col, col.lengths, row, rowsguess, p, i, k, pool, r, typemap)
    else
        @inbounds values[row] = r.result
    end
    return
end

@inline function adjust!(::Type{Int64}, col, values::Vector{Int64}, row, rowsguess, parsinglayers, io, kwargs, pool, r, typemap)
    if !Parsers.ok(r.code)
        Parsers.fastseek!(io, r.pos - (quotedstring(r.code) ? 1 : 0))
        r = Parsers.parse(parsinglayers, io, Float64; kwargs...)
        if !Parsers.ok(r.code)
            col.typecode = STRING
            adjust!(String, col, col.lengths, row, rowsguess, parsinglayers, io, kwargs, pool, r, typemap)
        else
            floats = convert(Vector{Float64}, values)
            col.typecode = FLOAT
            @inbounds floats[row] = r.result
            col.floats = floats
        end
    elseif sentinel(r.code)
        valuesm = convert(Vector{Union{Missing, Int64}}, values)
        col.typecode |= MISSING
        @inbounds valuesm[row] = missing
        col.intsm = valuesm
    else
        @inbounds values[row] = r.result
    end
    return
end

@inline function adjust!(::Type{Union{Int64, Missing}}, col, values::Vector{Union{Int64, Missing}}, row, rowsguess, parsinglayers, io, kwargs, pool, r, typemap)
    if !Parsers.ok(r.code)
        Parsers.fastseek!(io, r.pos - (quotedstring(r.code) ? 1 : 0))
        r = Parsers.parse(parsinglayers, io, Float64; kwargs...)
        if !Parsers.ok(r.code)
            col.typecode = STRING | MISSING
            adjust!(String, col, col.lengths, row, rowsguess, parsinglayers, io, kwargs, pool, r, typemap)
        else
            floatsm = convert(Vector{Union{Float64, Missing}}, values)
            col.typecode = FLOAT | MISSING
            @inbounds floatsm[row] = r.result
            col.floatsm = floatsm
        end
    else
        @inbounds values[row] = r.result
    end
    return
end

function adjustmissing!(col, row)
    type = col.typecode
    if type === EMPTY
        col.typecode = MISSING
        col.missings = [missing]
    elseif type === MISSING
    elseif type === STRING && !missingtype(type)
        col.typecode |= MISSING
    elseif pooled(type)
        if !missingtype(type)
            col.typecode |= MISSING
            col.poolm = convert(Dict{Union{Missing, String}, UInt32}, col.pool)
            col.poolm[missing] = UInt32(0)
        end
        @inbounds col.refs[row] = UInt32(0)
    elseif missingtype(type)
        setindex!(getfield(col, TYPEINDEX[type]), missing, row)
    else
        values = getfield(col, TYPEINDEX[type])
        valuesm = convert(Vector{Union{Missing, eltype(values)}}, values)
        col.typecode |= MISSING
        @inbounds valuesm[row] = missing
        setfield!(col, TYPEINDEX[col.typecode], valuesm)
    end
    @inbounds col.offsets[row] = WeakRefStrings.MISSING_OFFSET
    return
end

@inline function trytype(io, pos, layers, T, kwargs)
    Parsers.fastseek!(io, pos)
    res = Parsers.parse(layers, io, T; kwargs...)
    return res
end

function detecttype(io, layers, kwargs)
    pos = position(io)
    int = trytype(io, pos, layers, Int64, kwargs)
    Parsers.ok(int.code) && return int
    float = trytype(io, pos, layers, Float64, kwargs)
    Parsers.ok(float.code) && return float
    if !haskey(kwargs, :dateformat)
        try
            date = trytype(io, pos, layers, Date, kwargs)
            Parsers.ok(date.code) && return date
        catch e
        end
        try
            datetime = trytype(io, pos, layers, DateTime, kwargs)
            Parsers.ok(datetime.code) && return datetime
        catch e
        end
    else
        # use user-provided dateformat
        T = timetype(kwargs.dateformat)
        dt = trytype(io, pos, layers, T, kwargs)
        Parsers.ok(dt.code) && return dt
    end
    bool = trytype(io, pos, layers, Bool, kwargs)
    Parsers.ok(bool.code) && return bool
    return trytype(io, pos, layers, Tuple{Ptr{UInt8}, Int}, kwargs)
end
