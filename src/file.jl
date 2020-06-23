# a Row "view" type for iterating `CSV.File`
struct Row <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol, AbstractVector}
    row::Int64
end

getnames(r::Row) = getfield(r, :names)
getcolumn(r::Row, col::Int) = getfield(r, :columns)[col]
getcolumn(r::Row, col::Symbol) = getfield(r, :lookup)[col]
getrow(r::Row) = getfield(r, :row)

Tables.columnnames(r::Row) = getnames(r)

@inline function Tables.getcolumn(row::Row, ::Type{T}, i::Int, nm::Symbol) where {T}
    column = getcolumn(row, i)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Tables.getcolumn(row::Row, col::Symbol)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

@inline function Tables.getcolumn(row::Row, col::Int)
    column = getcolumn(row, col)
    @inbounds x = column[getrow(row)]
    return x
end

# main structure when parsing an entire file and inferring column types
struct File{threaded} <: AbstractVector{Row}
    name::String
    names::Vector{Symbol}
    types::Vector{Type}
    rows::Int64
    cols::Int64
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol, AbstractVector}
end

getname(f::File) = getfield(f, :name)
getnames(f::File) = getfield(f, :names)
gettypes(f::File) = getfield(f, :types)
getrows(f::File) = getfield(f, :rows)
getcols(f::File) = getfield(f, :cols)
getcolumns(f::File) = getfield(f, :columns)
getlookup(f::File) = getfield(f, :lookup)
getcolumn(f::File, col::Int) = getfield(f, :columns)[col]
getcolumn(f::File, col::Symbol) = getfield(f, :lookup)[col]

function Base.show(io::IO, f::File)
    println(io, "CSV.File(\"$(getname(f))\"):")
    println(io, "Size: $(getrows(f)) x $(getcols(f))")
    show(io, Tables.schema(f))
end

Base.IndexStyle(::Type{File}) = Base.IndexLinear()
Base.eltype(f::File) = Row
Base.size(f::File) = (getrows(f),)

Tables.isrowtable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), gettypes(f))
Tables.columns(f::File) = f
Tables.columnnames(f::File) = getnames(f)
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return get(lookup, col) do
        getfield(f, col)
    end
end

Tables.getcolumn(f::File, nm::Symbol) = getcolumn(f, nm)
Tables.getcolumn(f::File, i::Int) = getcolumn(f, i)

Base.@propagate_inbounds function Base.getindex(f::File, row::Int)
    @boundscheck checkbounds(f, row)
    return Row(getnames(f), getcolumns(f), getlookup(f), row)
end

@inline function Base.iterate(f::File, st::Int=1)
    st > length(f) && return nothing
    return Row(getnames(f), getcolumns(f), getlookup(f), st), st + 1
end

"""
    CSV.File(source; kwargs...) => CSV.File

Read a UTF-8 CSV input (a filename given as a String or FilePaths.jl type, or any other IO source), returning a `CSV.File` object.

Opens the file and uses passed arguments to detect the number of columns and column types, unless column types are provided
manually via the `types` keyword argument. Note that passing column types manually can increase performance and reduce the
memory use for each column type provided (column types can be given as a `Vector` for all columns, or specified per column via
name or index in a `Dict`). For text encodings other than UTF-8, see the [StringEncodings.jl](https://github.com/JuliaStrings/StringEncodings.jl)
package for re-encoding a file or IO stream.
The returned `CSV.File` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values. `CSV.File`
also supports entire column access like a `DataFrame` via direct property access on the file object, like `f = CSV.File(file); f.col1`.
Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name `a` will become `a_1`).
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.File(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

By supporting the Tables.jl interface, a `CSV.File` can also be a table input to any other table sink function. Like:

```julia
# materialize a csv file as a DataFrame, without copying columns from CSV.File; these columns are read-only
df = CSV.File(file) |> DataFrame!

# load a csv file directly into an sqlite database table
db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

Supported keyword arguments include:
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.)
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data
  * `skipto::Int`: similar to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file; note for large files when multiple threads are used for parsing, the `limit` argument may not be exact
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing
  * `use_mmap::Bool=!Sys.iswindows()`: whether the file should be mmapped for reading, which in some cases can be faster
  * `ignoreemptylines::Bool=false`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)
  * `threaded::Bool`: whether parsing should utilize multiple threads; by default threads are used on large enough files, but isn't allowed when `transpose=true` or when `limit` is used; only available in Julia 1.3+
  * `select`: an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "selector" function of the form `(i, name) -> keep::Bool`; only columns in the collection or for which the selector function returns `true` will be parsed and accessible in the resulting `CSV.File`. Invalid values in `select` are ignored.
  * `drop`: inverse of `select`; an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "drop" function of the form `(i, name) -> drop::Bool`; columns in the collection or for which the drop function returns `true` will ignored in the resulting `CSV.File`. Invalid values in `drop` are ignored.
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted for the entire file
  * `decimal='.'`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented; by default only `true` and `false` are treated as `Bool`
* Column Type Options:
  * `type`: a single type to use for parsing an entire file; i.e. all columns will be treated as the same type; useful for matrix-like data files
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64; if a `Vector` if provided, it must match the # of columns provided or detected in `header`
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `String`
  * `pool::Union{Bool, Float64}=0.1`: if `true`, *all* columns detected as `String` will be internally pooled; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `lazystrings::Bool=false`: avoid allocating full strings in string columns; returns a custom `LazyStringVector` array type that *does not* support mutable operations (e.g. `push!`, `append!`, or even `setindex!`). Calling `copy(x)` will materialize a full `Vector{String}`. Also note that each `LazyStringVector` holds a reference to the full input file buffer, so it won't be closed after parsing and trying to delete or modify the file probably has undefined behavior. Despite all the caveats, this setting can help avoid lots string allocations in large files and lead to faster parsing times.
  * `categorical::Bool=false`: whether pooled columns should be copied as CategoricalArray instead of PooledArray; note that in `CSV.read`, by default, columns are not copied, so pooled columns will have type `CSV.Column{String, PooledString}`; to get `CategoricalArray` columns, also pass `copycols=true`
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
"""
function File(source;
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Integer=-1,
    skipto::Union{Nothing, Integer}=nothing,
    footerskip::Integer=0,
    limit::Integer=typemax(Int64),
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    use_mmap::Bool=!Sys.iswindows(),
    ignoreemptylines::Bool=false,
    threaded::Union{Bool, Nothing}=nothing,
    select=nothing,
    drop=nothing,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Nothing, Char, String}=nothing,
    ignorerepeated::Bool=false,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='"',
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    dateformats::Union{AbstractDict, Nothing}=nothing,
    decimal::Union{UInt8, Char}=UInt8('.'),
    truestrings::Union{Vector{String}, Nothing}=["true", "True", "TRUE"],
    falsestrings::Union{Vector{String}, Nothing}=["false", "False", "FALSE"],
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    categorical::Union{Bool, Real}=false,
    pool::Union{Bool, Real}=0.1,
    lazystrings::Bool=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, limit, transpose, comment, use_mmap, ignoreemptylines, threaded, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, categorical, pool, lazystrings, strict, silencewarnings, debug, parsingdebug, false)
    rowsguess, ncols, buf, len, datapos, options, coloptions, positions, types, flags, pool, categorical = h.rowsguess, h.cols, h.buf, h.len, h.datapos, h.options, h.coloptions, h.positions, h.types, h.flags, h.pool, h.categorical
    # determine if we can use threads while parsing
    if threaded === nothing && VERSION >= v"1.3-DEV" && Threads.nthreads() > 1 && !transpose && (limit < rowsguess ? limit : rowsguess) > Threads.nthreads() && ((limit < rowsguess ? limit : rowsguess) * ncols) >= 5_000
        threaded = true
    elseif threaded === true
        if VERSION < v"1.3-DEV"
            @warn "incompatible julia version for `threaded=true`: $VERSION, requires >= v\"1.3\", setting `threaded=false`"
            threaded = false
        elseif transpose
            @warn "`threaded=true` not supported on transposed files"
            threaded = false
        end
    end

    # we now do our parsing pass over the file, starting at datapos
    # `tapes` are Vector{UInt64} for each column, with length == # of estimated rows
    # all types are treated as UInt64 for type stability while looping over columns and for making "recoding" more convenient
    # (e.g. when we "promote" a column type from Int => Float64)
    # we have a sentinel value for each value to signal a `missing` value; this sentinel value is tracked internally and adjusted
    # automatically if an actual value of the sentinel value is parsed
    # for strings, we have the following encoding of a single UInt64:
        # leftmost bit indicates a sentinel value (`missing`) was detected while parsing
        # 2nd leftmost bit indicates if a field was quoted and included escape chararacters (will have to be unescaped later)
        # 42 bits for position (allows for maximum file size of ~4TB)
        # 20 bits for field length (allows for maximum field size of ~1M)
    # the `poslens` are also Vector{UInt64} allocated for each column where the type must be detected; it stores the "string value" UInt64
    # of the cell, which allows promoting any column to a string later if needed
    # if a column type if promoted to string, the values are stored in the corresponding `tape` instead of `poslen`
    refs = Vector{RefPool}(undef, ncols)
    if threaded === true
        # multithread
        finalrows, tapes = multithreadparse(types, flags, buf, datapos, len, options, coloptions, rowsguess, pool, refs, ncols, typemap, h.categorical, limit, debug)
    else
        if limit < rowsguess
            rowsguess = limit
        end
        tapes = allocate(rowsguess, ncols, types, flags)
        t = Base.time()
        finalrows, pos = parsetape!(Val(transpose), ncols, typemap, tapes, buf, datapos, len, limit, positions, pool, refs, rowsguess, types, flags, debug, options, coloptions)
        debug && println("time for initial parsing to tape: $(Base.time() - t)")
        for i = 1:ncols
            tape = tapes[i]
            if tape isa Vector{UInt32}
                makeandsetpooled!(tapes, i, tape, refs, flags, h.categorical)
            elseif tape isa Vector{PosLen} || tape isa ChainedVector{PosLen, Vector{PosLen}}
                if anymissing(flags[i])
                    tapes[i] = LazyStringVector{Union{String, Missing}}(buf, options.e, tape)
                else
                    tapes[i] = LazyStringVector{String}(buf, options.e, tape)
                end
            elseif tape isa Vector{String} || tape isa Vector{Union{String, Missing}}
            else
                if !anymissing(flags[i])
                    if tape isa Vector{Union{Missing, Bool}}
                        tapes[i] = convert(Vector{Bool}, tape)
                    else
                        tapes[i] = parent(tape)
                    end
                end
            end
            # if zero rows
            if types[i] === Union{}
                types[i] = Missing
            end
        end
    end
    debug && println("types after parsing: $types, pool = $pool")
    columns = tapes
    deleteat!(h.names, h.todrop)
    deleteat!(types, h.todrop)
    ncols -= length(h.todrop)
    deleteat!(columns, h.todrop)
    lookup = Dict(k => v for (k, v) in zip(h.names, columns))
    return File{something(threaded, false)}(h.name, h.names, types, finalrows, ncols, columns, lookup)
end

const EMPTY_INT_ARRAY = Int64[]
const EMPTY_REFRECODE = UInt32[]

function syncrefs!(refs, tl_refs, col, tl_rows, tape)
    if !isassigned(refs, col)
        @inbounds refs[col] = tl_refs[col]
    else
        @inbounds colrefs = refs[col]
        @inbounds tlcolrefs = tl_refs[col]
        refrecodes = EMPTY_REFRECODE
        recode = false
        for (k, v) in tlcolrefs.refs
            refvalue = get(colrefs.refs, k, UInt32(0))
            if refvalue != v
                recode = true
                if isempty(refrecodes)
                    refrecodes = [UInt32(i) for i = 1:tlcolrefs.lastref]
                end
                if refvalue == 0
                    refvalue = (colrefs.lastref += UInt32(1))
                end
                @inbounds colrefs.refs[k] = refvalue
                @inbounds refrecodes[v] = refvalue
            end
        end
        if recode
            for j = 1:tl_rows
                @inbounds tape[j] = refrecodes[tape[j]]
            end
        end
    end
    return
end

vectype(::Type{A}) where {A <: SentinelVector{T}} where {T} = Vector{T}
vectype(T) = T

function makechain(::Type{T}, tape::T, N, col, perthreadtapes, limit) where {T}
    chain = Vector{vectype(T)}(undef, N)
    @inbounds chain[1] = parent(tape)
    for i = 2:N
        @inbounds tp = perthreadtapes[i][col]
        if tp isa T
            @inbounds chain[i] = parent(tp)
        end
    end
    x = ChainedVector(chain)
    if limit < length(x)
        resize!(x, limit)
    end
    return x
end

function makeandsetpooled!(tapes, i, tape, refs, flags, categorical)
    if categorical
        colrefs = isassigned(refs, i) ? refs[i].refs : Dict{String, UInt32}()
        if anymissing(flags[i])
            missingref = colrefs[missing]
            delete!(colrefs, missing)
            colrefs = convert(Dict{String, UInt32}, colrefs)
            for j = 1:length(tape)
                @inbounds tape[j] = ifelse(tape[j] === missingref, UInt32(0), tape[j])
            end
        else
            colrefs = convert(Dict{String, UInt32}, colrefs)
        end
        pool = CategoricalPool(colrefs)
        A = CategoricalArray{anymissing(flags[i]) ? Union{String, Missing} : String, 1}(tape, pool)
        levels!(A, sort(levels(A)))
        tapes[i] = A
    else
        r = isassigned(refs, i) ? (anymissing(flags[i]) ? refs[i].refs : convert(Dict{String, UInt32}, refs[i].refs)) : Dict{String, UInt32}()
        tapes[i] = PooledArray(PooledArrays.RefArray(tape), r)
    end
    return
end

function promotetostring!(tapes, poslens, flags, col, rows, fullrows, options, buf)
    if lazystrings(flags[col])
        tapes[col] = poslens[col]
    else
        tape = allocate(String, fullrows)
        colposlens = poslens[col]
        e = options.e::UInt8
        for row = 1:rows
            @inbounds tape[row] = str(buf, e, colposlens[row])
        end
        tapes[col] = tape
    end
    unset!(poslens, col, 0, 0)
    return
end

function multithreadparse(types, flags, buf, datapos, len, options, coloptions, rowsguess, pool, refs, ncols, typemap, categorical, limit, debug)
    N = Threads.nthreads()
    if limit < rowsguess
        newlen = [0, ceil(Int64, (limit / (rowsguess * 0.8)) * len), 0]
        findrowstarts!(buf, len, options, newlen, ncols)
        len = newlen[2]
        debug && println("limiting, adjusting len to $len")
    end
    chunksize = div(len - datapos, N)
    ranges = [i == 0 ? datapos : (datapos + chunksize * i) for i = 0:N]
    ranges[end] = len
    debug && println("initial byte positions before adjusting for start of rows: $ranges")
    findrowstarts!(buf, len, options, ranges, ncols)
    rowchunkguess = cld(rowsguess, N)
    debug && println("parsing using $N threads: $rowchunkguess rows chunked at positions: $ranges")
    perthreadtapes = Vector{Vector{AbstractVector}}(undef, N)
    rows = zeros(Int64, N)
    locks = [ReentrantLock() for i = 1:ncols]
    Threads.@threads for i = 1:N
        tt = Base.time()
        tl_flags = copy(flags)
        tl_refs = Vector{RefPool}(undef, ncols)
        tl_len = ranges[i + 1] - (i != N)
        tl_pos = ranges[i]
        tl_types = copy(types)
        tl_tapes = allocate(rowchunkguess, ncols, tl_types, tl_flags)
        perthreadtapes[i] = tl_tapes
        tl_rows, tl_pos = parsetape!(Val(false), ncols, typemap, tl_tapes, buf, tl_pos, tl_len, typemax(Int64), EMPTY_INT_ARRAY, pool, tl_refs, rowchunkguess, tl_types, tl_flags, debug, options, coloptions)
        rows[i] = tl_rows
        # promote column types across threads
        for col = 1:ncols
            lock(locks[col]) do
                @inbounds T = types[col]
                @inbounds types[col] = promote_types(T, tl_types[col])
                # if T !== types[col]
                #     debug && println("promoting col = $col from $T to $(types[col]), thread chunk was type = $(tl_types[col])")
                # end
                @inbounds flags[col] |= tl_flags[col]
                # synchronize refs if needed
                @inbounds tape = tl_tapes[col]
                if tape isa Vector{UInt32}
                    syncrefs!(refs, tl_refs, col, tl_rows, tape)
                end
            end
        end
        debug && println("finished parsing $tl_rows rows on thread = $(Threads.threadid()): time for parsing: $(Base.time() - tt)")
    end
    finaltapes = Vector{AbstractVector}(undef, ncols)
    Threads.@threads for col = 1:ncols
        for i = 1:N
            tl_tapes = perthreadtapes[i]
            tl_rows = rows[i]
            # check if we need to promote a thread-local column based on what other threads parsed
            @inbounds T = types[col]
            if (T === String || T === Union{String, Missing}) && !(tl_tapes[col] isa Vector{PosLen}) && !(tl_tapes[col] isa StringVec)
                # promoting non-string to string column
                promotetostring!(col, Val(false), ncols, typemap, tl_tapes, buf, ranges[i], ranges[i + 1] - (i != N), tl_rows, EMPTY_INT_ARRAY, pool, refs, tl_rows, types, flags, debug, options, coloptions)
            elseif (T === Float64 || T === Union{Float64, Missing}) && tl_tapes[col] isa SVec{Int64}
                tl_tapes[col] = convert(SentinelVector{Float64}, tl_tapes[col])
            elseif T !== Union{} && T !== Missing && tl_tapes[col] isa MissingVector
                tl_tapes[col] = allocate(T, tl_rows)
            end
        end
        @inbounds tape = perthreadtapes[1][col]
        if tape isa SVec{Int64}
            sent = tape.sentinel
            chain = makechain(SVec{Int64}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain, sent) : chain
        elseif tape isa SVec{Float64}
            chain = makechain(SVec{Float64}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain) : chain
        elseif tape isa StringVec
            chain = makechain(StringVec, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain) : chain
        elseif tape isa SVec{Date}
            chain = makechain(SVec{Date}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain) : chain
        elseif tape isa SVec{DateTime}
            chain = makechain(SVec{DateTime}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain) : chain
        elseif tape isa SVec{Time}
            chain = makechain(SVec{Time}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = anymissing(flags[col]) ? SentinelArray(chain) : chain
        elseif tape isa Vector{Union{Missing, Bool}}
            chain = makechain(Vector{anymissing(flags[col]) ? Bool : Union{Missing, Bool}}, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = chain
        elseif tape isa Vector{UInt32}
            chain = makechain(Vector{UInt32}, tape, N, col, perthreadtapes, limit)
            makeandsetpooled!(finaltapes, col, chain, refs, flags, categorical)
        elseif tape isa Vector{PosLen}
            chain = makechain(Vector{PosLen}, tape, N, col, perthreadtapes, limit)
            if anymissing(flags[col])
                @inbounds finaltapes[col] = LazyStringVector{Union{String, Missing}}(buf, options.e, chain)
            else
                @inbounds finaltapes[col] = LazyStringVector{String}(buf, options.e, chain)
            end
        elseif tape isa MissingVector
            chain = makechain(MissingVector, tape, N, col, perthreadtapes, limit)
            @inbounds finaltapes[col] = chain
            types[col] = Missing
        else
            error("unhandled column type: $(typeof(tape))")
        end
    end
    finalrows = sum(rows)
    return limit < finalrows ? limit : finalrows, finaltapes
end

function parsetape!(TR::Val{transpose}, ncols, typemap, tapes, buf, pos, len, limit, positions, pool, refs, rowsguess, types, flags, debug, options::Parsers.Options{ignorerepeated}, coloptions) where {transpose, ignorerepeated}
    row = 0
    startpos = pos
    if pos <= len && len > 0
        while row < limit
            row += 1
            pos = parserow(row, TR, ncols, typemap, tapes, startpos, buf, pos, len, positions, pool, refs, rowsguess, types, flags, debug, options, coloptions)
            (pos > len || row == limit) && break
            # if our initial row estimate was too few, we need to reallocate our tapes/poslens to read the rest of the file
            if row + 1 > rowsguess
                # (bytes left in file) / (avg bytes per row) == estimated rows left in file (+ 10 for kicks)
                estimated_rows_left = ceil(Int64, (len - pos) / ((pos - startpos) / row) + 10.0)
                newrowsguess = rowsguess + estimated_rows_left
                debug && reallocatetape(row, rowsguess, newrowsguess)
                for i = 1:ncols
                    reallocate!(tapes[i], newrowsguess)
                end
                rowsguess = newrowsguess
            end
        end
    end
    # done parsing (at least this chunk), so resize tapes to final row count
    for i = 1:ncols
        # TODO: specialize?
        resize!(tapes[i], row)
    end
    return row, pos
end

@noinline function promotetostring!(col, TR::Val{transpose}, ncols, typemap, tapes, buf, pos, len, limit, positions, pool, refs, rowsguess, types, origflags, debug, options::Parsers.Options{ignorerepeated}, coloptions) where {transpose, ignorerepeated}
    flags = copy(origflags)
    for i = 1:ncols
        if i == col
            flags[i] |= TYPEDETECTED
            tapes[i] = allocate(lazystrings(flags[i]) ? PosLen : String, rowsguess)
            types[i] = Union{String, anymissing(flags[i]) ? Missing : Union{}}
        else
            flags[i] |= WILLDROP
        end
    end
    row = 0
    startpos = pos
    if pos <= len && len > 0
        while row < limit
            row += 1
            pos = parserow(row, TR, ncols, typemap, tapes, startpos, buf, pos, len, positions, pool, refs, rowsguess, types, flags, debug, options, coloptions)
            pos > len && break
        end
    end
    return
end

@noinline reallocatetape(row, old, new) = println("thread = $(Threads.threadid()) warning: didn't pre-allocate enough tape while parsing on row $row, re-allocating from $old to $new...")
@noinline notenoughcolumns(cols, ncols, row) = println("thread = $(Threads.threadid()) warning: only found $cols / $ncols columns on data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = println("thread = $(Threads.threadid()) warning: parsed expected $cols columns, but didn't reach end of line on data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = println("thread = $(Threads.threadid()) warning: error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) fatal error, encountered an invalidly quoted field while parsing on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))

@inline function parserow(row, TR::Val{transpose}, ncols, typemap, tapes, startpos, buf, pos, len, positions, pool, refs, rowsguess, types, flags, debug, options::Parsers.Options{ignorerepeated}, coloptions) where {transpose, ignorerepeated}
    for col = 1:ncols
        if transpose
            @inbounds pos = positions[col]
        end
        @inbounds flag = flags[col]
        @inbounds tape = tapes[col]
        @inbounds opts = coloptions === nothing ? options : coloptions[col]
        # @show typeof(tape)
        if willdrop(flag) || (user(flag) && tape isa MissingVector)
            pos, code = parsemissing!(buf, pos, len, opts, row, col)
        elseif !typedetected(flag)
            pos, code = detect(tapes, buf, pos, len, opts, row, col, typemap, pool, refs, debug, types, flags, rowsguess)
        elseif tape isa SVec{Int64}
            pos, code = parseint!(flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa SVec{Float64}
            pos, code = parsevalue!(Float64, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa StringVec
            pos, code = parsestring2!(flag, tape, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa SVec{Date}
            pos, code = parsevalue!(Date, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa SVec{DateTime}
            pos, code = parsevalue!(DateTime, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa SVec{Time}
            pos, code = parsevalue!(Time, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa Vector{Union{Missing, Bool}}
            pos, code = parsevalue!(Bool, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags)
        elseif tape isa Vector{UInt32}
            pos, code = parsepooled!(flag, tape, tapes, buf, pos, len, opts, row, col, rowsguess, pool, refs, types, flags)
        elseif tape isa Vector{PosLen}
            pos, code = parsestring!(flag, tape, buf, pos, len, opts, row, col, types, flags)
        else
            error("bad array type: $(typeof(tape))")
        # TODO: support all other integer types, float16, float32
        # in an else clause, we'll parse a string and call
        # Parsers.parse(T, str)
        end
        if promote_to_string(code)
            debug && println("promoting col = $col to string")
            promotetostring!(col, TR, ncols, typemap, tapes, buf, startpos, len, row, positions, pool, refs, rowsguess, types, flags, debug, options, coloptions)
        end
        if transpose
            @inbounds positions[col] = pos
        else
            if col < ncols
                if Parsers.newline(code) || pos > len
                    options.silencewarnings || notenoughcolumns(col, ncols, row)
                    for j = (col + 1):ncols
                        @inbounds flags[j] |= ANYMISSING
                        @inbounds types[j] = Union{Missing, types[j]}
                    end
                    break # from for col = 1:ncols
                end
            else
                if pos <= len && !Parsers.newline(code)
                    options.silencewarnings || toomanycolumns(ncols, row)
                    # ignore the rest of the line
                    pos = skiptorow(buf, pos, len, options.oq, options.e, options.cq, 1, 2)
                end
            end
        end
    end
    return pos
end

@inline function poslen(code, pos, len)
    pos = Core.bitcast(UInt64, pos) << 20
    pos |= ifelse(Parsers.sentinel(code), MISSING_BIT, UInt64(0))
    pos |= ifelse(Parsers.escapedstring(code), ESCAPE_BIT, UInt64(0))
    return pos | Core.bitcast(UInt64, len)
end

@inline function setposlen!(tape, row, code, pos, len)
    @inbounds tape[row] = poslen(code, pos, len)
    return
end

function detect(tapes, buf, pos, len, options, row, col, typemap, pool, refs, debug, types, flags, rowsguess)
    # debug && println("detecting on thread $(Threads.threadid())")
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        flags[col] |= ANYMISSING
        # return; parsing will continue to detect until a non-missing value is parsed
        @goto finaldone
    end
    if Parsers.ok(code) && !haskey(typemap, Int64)
        # debug && println("detecting Int64 for column = $col on thread = $(Threads.threadid())")
        tape = allocate(Int64, rowsguess)
        tape[row] = int
        newT = Int64
        @goto done
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if Parsers.ok(code) && !haskey(typemap, Float64)
        # debug && println("detecting Float64 for column = $col on thread = $(Threads.threadid())")
        tape = allocate(Float64, rowsguess)
        tape[row] = float
        newT = Float64
        @goto done
    end
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, Date)
                tape = allocate(Date, rowsguess)
                tape[row] = date
                newT = Date
                @goto done
            end
        catch
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, DateTime)
                tape = allocate(DateTime, rowsguess)
                tape[row] = datetime
                newT = DateTime
                @goto done
            end
        catch
        end
        try
            time, code, vpos, vlen, tlen = Parsers.xparse(Time, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, Time)
                tape = allocate(Time, rowsguess)
                tape[row] = time
                newT = Time
                @goto done
            end
        catch
        end
    else
        try
            # use user-provided dateformat
            DT = timetype(options.dateformat)
            dt, code, vpos, vlen, tlen = Parsers.xparse(DT, buf, pos, len, options)
            if Parsers.ok(code)
                tape = allocate(DT, rowsguess)
                tape[row] = dt
                newT = DT
                @goto done
            end
        catch
        end
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, options)
    if Parsers.ok(code) && !haskey(typemap, Bool)
        tape = allocate(Bool, rowsguess)
        tape[row] = bool
        newT = Bool
        @goto done
    end
    _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if pool > 0.0
        r = RefPool()
        @inbounds refs[col] = r
        tape = allocate(PooledString, rowsguess)
        if anymissing(flags[col])
            ref = getref!(r, missing, code, options)
            fill!(tape, ref)
        end
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), code, options)
        @inbounds tape[row] = ref
        newT = PooledString
    else
        # debug && println("detecting String for column = $col on thread = $(Threads.threadid())")
        if lazystrings(flags[col])
            tape = allocate(PosLen, rowsguess)
            @inbounds setposlen!(tape, row, code, vpos, vlen)
        else
            tape = allocate(String, rowsguess)
            @inbounds tape[row] = str(buf, options.e, poslen(code, vpos, vlen))
        end
        newT = String
    end
@label done
    # if we're here, that means we found a non-missing value, so we need to update tapes
    tapes[col] = tape
    flags[col] |= TYPEDETECTED
    types[col] = Union{newT, anymissing(flags[col]) ? Missing : Union{}}
@label finaldone
    return pos + tlen, code
end

function parseint!(flag, tape, tapes, buf, pos, len, options, row, col, types, flags)
    x, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if code > 0
        if !Parsers.sentinel(code)
            @inbounds tape[row] = x
        else
            if !anymissing(flag)
                @inbounds flags[col] = flag | ANYMISSING
                @inbounds types[col] = Union{Int64, Missing}
            end
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, row, col)
        end
        if user(flag)
            if !options.strict
                options.silencewarnings || warning(Int64, buf, pos, tlen, code, row, col)
                flags[col] = flag | ANYMISSING
                types[col] = Union{Int64, Missing}
            else
                stricterror(Int64, buf, pos, tlen, code, row, col)
            end
        else
            # println("encountered an error parsing an Intger in auto-detect mode for column = $col, so let's try Float64")
            y, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
            if code > 0
                @inbounds tapes[col] = convert(SentinelVector{Float64}, tape)
                @inbounds tapes[col][row] = y
                @inbounds types[col] = Union{Float64, anymissing(flag) ? Missing : Union{}}
            else
                # println("promoting to String instead")
                code |= PROMOTE_TO_STRING
            end
        end
    end
    return pos + tlen, code
end

function parsevalue!(::Type{type}, flag, tape, tapes, buf, pos, len, options, row, col, types, flags) where {type}
    x, code, vpos, vlen, tlen = Parsers.xparse(type, buf, pos, len, options)
    if code > 0
        if !Parsers.sentinel(code)
            @inbounds tape[row] = x
        else
            if !anymissing(flag)
                @inbounds flags[col] = flag | ANYMISSING
                @inbounds types[col] = Union{type, Missing}
            end
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, row, col)
        end
        if user(flag)
            if !options.strict
                # code |= Parsers.SENTINEL # what was this for??
                options.silencewarnings || warning(type, buf, pos, tlen, code, row, col)
                flags[col] = flag | ANYMISSING
                types[col] = Union{type, Missing}
            else
                stricterror(type, buf, pos, tlen, code, row, col)
            end
        else
            code |= PROMOTE_TO_STRING
        end
    end
    return pos + tlen, code
end

function parsestring!(flag, tape, buf, pos, len, options, row, col, types, flags)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(tape, row, code, vpos, vlen)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code)
        if !anymissing(flag)
            @inbounds flags[col] |= ANYMISSING
            @inbounds types[col] = Union{String, Missing}
        end
    end
    return pos + tlen, code
end

function parsestring2!(flag, tape, buf, pos, len, options, row, col, types, flags)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code)
        if !anymissing(flag)
            @inbounds flags[col] |= ANYMISSING
            @inbounds types[col] = Union{String, Missing}
        end
    else
        @inbounds tape[row] = str(buf, options.e, poslen(code, vpos, vlen))
    end
    return pos + tlen, code
end

function parsemissing!(buf, pos, len, options, row, col)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    return pos + tlen, code
end

@inline function getref!(refpool, key::Missing, code, options)
    get!(refpool.refs, key) do
        refpool.lastref += UInt32(1)
    end
end

@inline function getref!(refpool, key::PointerString, code, options)
    # lock(refpool.lock)
    x = refpool.refs
    if Parsers.escapedstring(code)
        key2 = unescape(key, options.e)
        index = Base.ht_keyindex2!(x, key2)
    else
        index = Base.ht_keyindex2!(x, key)
    end
    if index > 0
        @inbounds found_key = x.vals[index]
        ret = found_key::UInt32
    else
        @inbounds new = refpool.lastref += UInt32(1)
        @inbounds Base._setindex!(x, new, Parsers.escapedstring(code) ? key2 : String(key), -index)
        ret = new
    end
    # unlock(refpool.lock)
    return ret
end

function parsepooled!(flag, tape, tapes, buf, pos, len, options, row, col, rowsguess, pool, refs, types, flags)
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if !isassigned(refs, col)
        r = RefPool()
        @inbounds refs[col] = r
    else
        @inbounds r = refs[col]
    end
    if Parsers.sentinel(code)
        @inbounds flags[col] = flag | ANYMISSING
        @inbounds types[col] = Union{PooledString, Missing}
        ref = getref!(r, missing, code, options)
    else
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), code, options)
    end
    if !user(flag) && ((length(r.refs) - anymissing(flags[col])) / rowsguess) > pool
        code |= PROMOTE_TO_STRING
    else
        @inbounds tape[row] = ref
    end
    return pos + tlen, code
end
