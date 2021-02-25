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

Base.IndexStyle(::Type{<:File}) = Base.IndexLinear()
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

Read a UTF-8 CSV input and return a `CSV.File` object.

The `source` argument can be one of:
  * filename given as a string or FilePaths.jl type
  * an `AbstractVector{UInt8}` like a byte buffer or `codeunits(string)`
  * an `IOBuffer`

To read a csv file from a url, use the HTTP.jl package, where the `HTTP.Response` body can be passed like:
```julia
f = CSV.File(HTTP.get(url).body)
```

For other `IO` or `Cmd` inputs, you can pass them like: `f = CSV.File(read(obj))`.

Opens the file and uses passed arguments to detect the number of columns and column types, unless column types are provided
manually via the `types` keyword argument. Note that passing column types manually can slightly increase performance
for each column type provided (column types can be given as a `Vector` for all columns, or specified per column via
name or index in a `Dict`).

For text encodings other than UTF-8, load the [StringEncodings.jl](https://github.com/JuliaStrings/StringEncodings.jl)
package and call e.g. `CSV.File(open(read, source, enc"ISO-8859-1"))`.

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
# materialize a csv file as a DataFrame, without copying columns from CSV.File
df = CSV.File(file) |> DataFrame

# load a csv file directly into an sqlite database table
db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

Supported keyword arguments include:
* File layout options:
  * `header=1`: the `header` argument can be an `Int`, indicating the row to parse for column names; or a `Range`, indicating a span of rows to be concatenated together as column names; or an entire `Vector{Symbol}` or `Vector{String}` to use as column names; if a file doesn't have column names, either provide them as a `Vector`, or set `header=0` or `header=false` and column names will be auto-generated (`Column1`, `Column2`, etc.). Note that if a row number header and `comment` or `ignoreemtpylines` are provided, the header row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header row will actually be the next non-commented row.
  * `normalizenames=false`: whether column names should be "normalized" into valid Julia identifier symbols; useful when iterating rows and accessing column values of a row via `getproperty` (e.g. `row.col1`)
  * `datarow`: an `Int` argument to specify the row where the data starts in the csv file; by default, the next row after the `header` row is used. If `header=0`, then the 1st row is assumed to be the start of data; providing a `datarow` or `skipto` argument does _not_ affect the `header` argument. Note that if a row number `datarow` and `comment` or `ignoreemtpylines` are provided, the data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the data row will actually be the next non-commented row.
  * `skipto::Int`: identical to `datarow`, specifies the number of rows to skip before starting to read data
  * `footerskip::Int`: number of rows at the end of a file to skip parsing.  Do note that commented rows (see the `comment` keyword argument) *do not* count towards the row number provided for `footerskip`, they are completely ignored by the parser
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file; note for large files when multiple threads are used for parsing, the `limit` argument may not result in exact an exact # of rows parsed; use `threaded=false` to ensure an exact limit if necessary
  * `transpose::Bool`: read a csv file "transposed", i.e. each column is parsed as a row
  * `comment`: rows that begin with this `String` will be skipped while parsing. Note that if a row number header or `datarow` and `comment` are provided, the header/data row will be the first non-commented/non-empty row _after_ the row number, meaning if the provided row number is a commented row, the header/data row will actually be the next non-commented row.
  * `ignoreemptylines::Bool=true`: whether empty rows/lines in a file should be ignored (if `false`, each column will be assigned `missing` for that empty row)
  * `threaded::Bool`: whether parsing should utilize multiple threads; by default threads are used on large enough files, but isn't allowed when `transpose=true`; only available in Julia 1.3+
  * `tasks::Integer=Threads.nthreads()`: for multithreaded parsing, this controls the number of tasks spawned to read a file in chunks concurrently; defaults to the # of threads Julia was started with (i.e. `JULIA_NUM_THREADS` environment variable)
  * `lines_to_check::Integer=5`: for multithreaded parsing, a file is split up into `tasks` # of equal chunks, then `lines_to_check` # of lines are checked to ensure parsing correctly found valid rows; for certain files with very large quoted text fields, `lines_to_check` may need to be higher (10, 30, etc.) to ensure parsing correctly finds these rows
  * `select`: an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "selector" function of the form `(i, name) -> keep::Bool`; only columns in the collection or for which the selector function returns `true` will be parsed and accessible in the resulting `CSV.File`. Invalid values in `select` are ignored.
  * `drop`: inverse of `select`; an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "drop" function of the form `(i, name) -> drop::Bool`; columns in the collection or for which the drop function returns `true` will ignored in the resulting `CSV.File`. Invalid values in `drop` are ignored.
* Parsing options:
  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted for the entire file
  * `dateformats::Union{AbstractDict, Nothing}`: a Dict of date format strings to indicate how the Date/DateTime columns corresponding to the keys are formatted. The Dict can map column index `Int`, or name `Symbol` or `String` to the format string for that column.
  * `decimal='.'`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented; by default only `true` and `false` are treated as `Bool`
* Column Type Options:
  * `type`: a single type to use for parsing an entire file; i.e. all columns will be treated as the same type; useful for matrix-like data files
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64; if a `Vector` if provided, it must match the # of columns provided or detected in `header`
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `String`; only "standard" types are allowed to be mapped to another type, i.e. `Int64`, `Float64`, `Date`, `DateTime`, `Time`, and `Bool`. If a column of one of those types is "detected", it will be mapped to the specified type.
  * `pool::Union{Bool, Float64}=0.1`: if `true`, *all* columns detected as `String` will be internally pooled; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `lazystrings::Bool=false`: avoid allocating full strings in string columns; returns a custom `LazyStringVector` array type that *does not* support mutable operations (e.g. `push!`, `append!`, or even `setindex!`). Calling `copy(x)` will materialize a full `Vector{String}`. Also note that each `LazyStringVector` holds a reference to the full input file buffer, so it won't be closed after parsing and trying to delete or modify the file may result in errors (particularly on windows) and generally has undefined behavior. Given these caveats, this setting can help avoid lots of string allocations in large files and lead to faster parsing times.
  * `strict::Bool=false`: whether invalid values should throw a parsing error or be replaced with `missing`
  * `silencewarnings::Bool=false`: if `strict=false`, whether invalid value warnings should be silenced
  * `maxwarnings::Int=100`: if more than `maxwarnings` number of warnings are printed while parsing, further warnings will be silenced by default; for multithreaded parsing, each parsing task will print up to `maxwarnings`
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
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    use_mmap=nothing,
    ignoreemptylines::Bool=true,
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
    truestrings::Union{Vector{String}, Nothing}=["true", "True", "TRUE", "1"],
    falsestrings::Union{Vector{String}, Nothing}=["false", "False", "FALSE", "0"],
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    pool::Union{Bool, Real}=0.1,
    lazystrings::Bool=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,
    kw...)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, use_mmap, ignoreemptylines, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, pool, lazystrings, strict, silencewarnings, debug, parsingdebug, false)
    return File(h; debug=debug, typemap=typemap, kw...)
end

function File(h::Header;
    finalizebuffer=true,
    startingbyteposition=nothing,
    endingbyteposition=nothing,
    limit::Union{Integer, Nothing}=nothing,
    threaded::Union{Bool, Nothing}=nothing,
    typemap::Dict=Dict{Type, Type}(),
    tasks::Integer=Threads.nthreads(),
    lines_to_check::Integer=5,
    maxwarnings::Int=100,
    debug::Bool=false,
    )
    rowsguess, ncols, buf, len, datapos, datarow, options, coloptions, positions, types, flags, pool, customtypes = h.rowsguess, h.cols, h.buf, h.len, h.datapos, h.datarow, h.options, h.coloptions, h.positions, h.types, h.flags, h.pool, h.customtypes
    if startingbyteposition !== nothing
        datapos = startingbyteposition
    end
    if endingbyteposition !== nothing
        len = endingbyteposition
    end
    transpose = gettranspose(h)
    # determine if we can use threads while parsing
    minrows = min(something(limit, typemax(Int64)), rowsguess)
    if threaded === nothing && VERSION >= v"1.3-DEV" && tasks > 1 && !transpose && minrows > (tasks * 5) && (minrows * ncols) >= 5_000
        threaded = true
    elseif threaded === true
        if VERSION < v"1.3-DEV"
            @warn "incompatible julia version for `threaded=true`: $VERSION, requires >= v\"1.3\", setting `threaded=false`"
            threaded = false
        elseif transpose
            @warn "`threaded=true` not supported on transposed files"
            threaded = false
        elseif tasks == 1
            @warn "`threaded=true` but `tasks=1`; to support threaded parsing, pass `tasks=N` where `N > 1`; `tasks` defaults to `Threads.nthreads()`, so you may consider starting Julia with multiple threads"
            threaded = false
        elseif minrows < (tasks * 5)
            @warn "`threaded=true` but there were not enough estimated rows ($minrows) to justify multithreaded parsing"
            threaded = false
        end
    end
    # attempt to chunk up a file for multithreaded parsing; there's chance we can't figure out how to accurately chunk
    # due to quoted fields, so threaded might get set to false
    if threaded === true
        # when limiting w/ multithreaded parsing, we try to guess about where in the file the limit row # will be
        # then adjust our final file len to the end of that row
        # we add some cushion so we hopefully get the limit row correctly w/o shooting past too far and needing to resize! down
        # but we also don't guarantee limit will be exact w/ multithreaded parsing
        origrowsguess = rowsguess
        if limit !== nothing
            limitposguess = ceil(Int64, (limit / (origrowsguess * 0.8)) * len)
            newlen = [0, limitposguess, min(limitposguess * 2, len)]
            findrowstarts!(buf, len, options, newlen, ncols, types, flags, lines_to_check)
            len = newlen[2] - 1
            origrowsguess = limit
            debug && println("limiting, adjusting len to $len")
        end
        chunksize = div(len - datapos, tasks)
        ranges = [i == 0 ? datapos : i == tasks ? len : (datapos + chunksize * i) for i = 0:tasks]
        debug && println("initial byte positions before adjusting for start of rows: $ranges")
        avgbytesperrow, successfullychunked = findrowstarts!(buf, len, options, ranges, ncols, types, flags, lines_to_check)
        if successfullychunked
            origbytesperrow = ((len - datapos) / origrowsguess)
            weightedavgbytesperrow = ceil(Int64, avgbytesperrow * ((tasks - 1) / tasks) + origbytesperrow * (1 / tasks))
            rowsguess = ceil(Int64, (len - datapos) / weightedavgbytesperrow)
            debug && println("single-threaded estimated rows = $origrowsguess, multi-threaded estimated rows = $rowsguess")
            debug && println("multi-threaded column types sampled as: $types")
        else
            debug && println("something went wrong chunking up a file for multithreaded parsing, falling back to single-threaded parsing")
            threaded = false
        end
    end

    # we now do our parsing pass over the file, starting at datapos
    refs = Vector{RefPool}(undef, ncols)
    if threaded === true
        # multithreaded
        finalrows, columns = multithreadparse(types, flags, buf, options, coloptions, rowsguess, datarow - 1, pool, refs, ncols, typemap, customtypes, limit, tasks, ranges, maxwarnings, debug)
    else
        if limit !== nothing
            rowsguess = limit
        end
        columns = allocate(rowsguess, ncols, types, flags, refs)
        t = Base.time()
        finalrows, pos = parsefilechunk!(Val(transpose), ncols, typemap, columns, buf, datapos, len, something(limit, typemax(Int64)), positions, pool, refs, rowsguess, datarow - 1, types, flags, debug, options, coloptions, customtypes, maxwarnings)
        debug && println("time for initial parsing: $(Base.time() - t)")
        # cleanup our columns if needed
        for i = 1:ncols
            column = columns[i]
            if column isa Vector{UInt32}
                # make a PooledArray given a columns' values (Vector{UInt32}) and refs (RefPool)
                makeandsetpooled!(columns, i, column, refs, flags)
            elseif column isa Vector{PosLen} || column isa ChainedVector{PosLen, Vector{PosLen}}
                # string column parsed lazily; return a LazyStringVector
                if anymissing(flags[i])
                    columns[i] = LazyStringVector{Union{String, Missing}}(buf, options.e, column)
                else
                    columns[i] = LazyStringVector{String}(buf, options.e, column)
                end
            else
                # if no missing values were parsed for a column, we want to "unwrap" it to a plain Vector{T}
                if !anymissing(flags[i])
                    if column isa Vector{Union{Missing, Bool}}
                        columns[i] = convert(Vector{Bool}, column)
                    elseif types[i] !== Union{} && types[i] <: SmallIntegers
                        columns[i] = convert(Vector{types[i]}, column)
                    else
                        columns[i] = parent(column)
                    end
                end
            end
            # if zero rows
            if types[i] === Union{}
                types[i] = Missing
                columns[i] = MissingVector(finalrows)
            elseif schematype(types[i]) !== types[i]
                types[i] = schematype(types[i])
            end
        end
    end
    debug && println("types after parsing: $types, pool = $pool")
    columns = columns
    # delete any dropped columns from names, types, columns
    deleteat!(h.names, h.todrop)
    deleteat!(types, h.todrop)
    ncols -= length(h.todrop)
    deleteat!(columns, h.todrop)
    lookup = Dict(k => v for (k, v) in zip(h.names, columns))
    # for windows, it's particularly finicky about throwing errors when you try to modify an mmapped file
    # so we just want to make sure we finalize the input buffer so users don't run into surprises
    if finalizebuffer && Sys.iswindows() && ncols > 0 && !lazystrings(flags[1])
        finalize(buf)
    end
    return File{something(threaded, false)}(h.name, h.names, types, finalrows, ncols, columns, lookup)
end

const EMPTY_INT_ARRAY = Int64[]
const EMPTY_REFRECODE = UInt32[]

# after multithreaded parsing, we need to synchronize pooled refs from different chunks of the file
# we pick one chunk as "source of truth", then adjust other chunks as needed
function syncrefs!(refs, task_refs, col, task_rows, column)
    if !isassigned(refs, col)
        @inbounds refs[col] = task_refs[col]
    else
        @inbounds colrefs = refs[col]
        @inbounds tlcolrefs = task_refs[col]
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
                    # "source of truth" chunk didn't know about this ref, so we add it to source of truth
                    refvalue = (colrefs.lastref += UInt32(1))
                end
                @inbounds colrefs.refs[k] = refvalue
                @inbounds refrecodes[v] = refvalue
            end
        end
        if recode
            for j = 1:task_rows
                @inbounds column[j] = refrecodes[column[j]]
            end
        end
    end
    return
end

vectype(::Type{A}) where {A <: SentinelVector{T}} where {T} = Vector{T}
vectype(T) = T

# while each chunk parses values into a SentinelVector, we invert after parsing is done
# so the final array type is SentinelVector wrapping a ChainedVector of the raw chunk vectors
function makechain(::Type{T}, column, N, col, pertaskcolumns, limit, anymissing, sentref=nothing) where {T}
    if T === SVec{Int64}
        # synchronize int sentinels if necessary
        SentinelArrays.newsentinel!((pertaskcolumns[i][col]::SVec{Int64} for i = 1:N)...; force=false)
        sentref[] = column.sentinel
    end
    if anymissing
        x = ChainedVector([pertaskcolumns[i][col] for i = 1:N])
    else
        x = ChainedVector([convert(vectype(T), parent(pertaskcolumns[i][col])) for i = 1:N])
    end
    if limit !== nothing && limit < length(x)
        resize!(x, limit)
    end
    return x
end

function makeandsetpooled!(columns, i, column, refs, flags)
    r = isassigned(refs, i) ? (anymissing(flags[i]) ? refs[i].refs : convert(Dict{String, UInt32}, refs[i].refs)) : Dict{String, UInt32}()
    columns[i] = PooledArray(PooledArrays.RefArray(column), r)
    return
end

function multithreadparse(types, flags, buf, options, coloptions, rowsguess, datarow, pool, refs, ncols, typemap, customtypes, limit, N, ranges, maxwarnings, debug)
    rowchunkguess = cld(rowsguess, N)
    debug && println("parsing using $N tasks: $rowchunkguess rows chunked at positions: $ranges")
    pertaskcolumns = Vector{Vector{AbstractVector}}(undef, N)
    rows = zeros(Int64, N)
    locks = [ReentrantLock() for i = 1:ncols]
    @sync for i = 1:N
@static if VERSION >= v"1.3-DEV"
        Threads.@spawn begin
            tt = Base.time()
            task_flags = copy(flags)
            task_refs = Vector{RefPool}(undef, ncols)
            task_len = ranges[i + 1] - (i != N)
            task_pos = ranges[i]
            task_types = copy(types)
            task_columns = allocate(rowchunkguess, ncols, task_types, task_flags, task_refs)
            pertaskcolumns[i] = task_columns
            task_rows, task_pos = parsefilechunk!(Val(false), ncols, typemap, task_columns, buf, task_pos, task_len, typemax(Int64), EMPTY_INT_ARRAY, pool, task_refs, rowchunkguess, datarow + (rowchunkguess * (i - 1)), task_types, task_flags, debug, options, coloptions, customtypes, maxwarnings)
            rows[i] = task_rows
            # promote column types/flags across task chunks
            for col = 1:ncols
                lock(locks[col])
                try
                    @inbounds T = types[col]
                    @inbounds types[col] = promote_types(T, task_types[col])
                    if T !== types[col]
                        debug && println("promoting col = $col from $T to $(types[col]), task chunk ($i) was type = $(task_types[col])")
                    end
                    @inbounds flags[col] |= task_flags[col]
                    # synchronize refs if needed
                    @inbounds column = task_columns[col]
                    if column isa Vector{UInt32}
                        syncrefs!(refs, task_refs, col, task_rows, column)
                    end
                finally
                    unlock(locks[col])
                end
            end
            debug && println("finished parsing $task_rows rows on task = $i: time for parsing: $(Base.time() - tt)")
        end
end # @static if VERSION >= v"1.3-DEV"
    end
    finalcolumns = Vector{AbstractVector}(undef, ncols)
    @sync for col = 1:ncols
@static if VERSION >= v"1.3-DEV"
        Threads.@spawn begin
            for i = 1:N
                task_columns = pertaskcolumns[i]
                task_rows = rows[i]
                # check if we need to promote a task-local column based on what other threads parsed
                @inbounds T = types[col]
                if (T === String || T === Union{String, Missing}) && !(task_columns[col] isa Vector{PosLen}) && !(task_columns[col] isa SVec2{String})
                    # promoting non-string to string column
                    debug && println("multithreaded promoting column $col to string from $(typeof(task_columns[col]))")
                    promotetostring!(col, Val(false), ncols, typemap, task_columns, buf, ranges[i], ranges[i + 1] - (i != N), task_rows, EMPTY_INT_ARRAY, pool, refs, task_rows, 0, types, flags, debug, options, coloptions, customtypes)
                elseif (T === Float64 || T === Union{Float64, Missing}) && task_columns[col] isa SVec{Int64}
                    # one chunk parsed as Int, another as Float64, promote to Float64
                    debug && println("multithreaded promoting column $col to float")
                    task_columns[col] = convert(SentinelVector{Float64}, task_columns[col])
                elseif T !== Union{} && T !== Missing && task_columns[col] isa MissingVector
                    # one chunk parsed all missing values, but another chunk had a typed value, promote to that
                    debug && println("multithreaded promoting column $col from missing on task $i")
                    task_columns[col] = allocate(T, task_rows)
                    if T == Union{PooledString, Missing}
                        colrefs = refs[col]
                        ref = getref!(colrefs, missing, Int16(0), options)
                        column = task_columns[col]
                        for j = 1:task_rows
                            @inbounds column[j] = ref
                        end
                    end
                end
            end
            @inbounds column = pertaskcolumns[1][col]
            if column isa SVec{Int64}
                sentref = Ref(column.sentinel)
                @inbounds finalcolumns[col] = makechain(SVec{Int64}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]), sentref)
            elseif column isa SVec{Float64}
                @inbounds finalcolumns[col] = makechain(SVec{Float64}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa SVec2{String}
                @inbounds finalcolumns[col] = makechain(SVec2{String}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa SVec{Date}
                @inbounds finalcolumns[col] = makechain(SVec{Date}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa SVec{DateTime}
                @inbounds finalcolumns[col] = makechain(SVec{DateTime}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa SVec{Time}
                @inbounds finalcolumns[col] = makechain(SVec{Time}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa Vector{Union{Missing, Bool}}
                @inbounds finalcolumns[col] = makechain(Vector{anymissing(flags[col]) ? Union{Missing, Bool} : Bool}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            elseif column isa Vector{UInt32}
                chain = makechain(Vector{UInt32}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
                makeandsetpooled!(finalcolumns, col, chain, refs, flags)
            elseif column isa Vector{PosLen}
                chain = makechain(Vector{PosLen}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
                if anymissing(flags[col])
                    @inbounds finalcolumns[col] = LazyStringVector{Union{String, Missing}}(buf, options.e, chain)
                else
                    @inbounds finalcolumns[col] = LazyStringVector{String}(buf, options.e, chain)
                end
            elseif column isa MissingVector
                @inbounds finalcolumns[col] = makechain(MissingVector, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
                types[col] = Missing
            elseif Base.nonmissingtype(types[col]) <: SmallIntegers
                T = types[col]
                @inbounds finalcolumns[col] = makechain(Vector{T}, column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
            else
                @inbounds finalcolumns[col] = makechain(typeof(column), column, N, col, pertaskcolumns, limit, anymissing(flags[col]))
                # error("unhandled column type: $(typeof(column))")
            end
        end
end # @static if VERSION >= v"1.3-DEV"
    end
    finalrows = sum(rows)
    return limit !== nothing && limit < finalrows ? limit : finalrows, finalcolumns
end

function parsefilechunk!(TR::Val{transpose}, ncols, typemap, columns, buf, pos, len, limit, positions, pool, refs, rowsguess, rowoffset, types, flags, debug, options::Parsers.Options{ignorerepeated}, coloptions, ::Type{customtypes}, maxwarnings) where {transpose, ignorerepeated, customtypes}
    row = 0
    startpos = pos
    if pos <= len && len > 0
        numwarnings = Ref(0)
        while row < limit
            row += 1
            pos = parserow(row, TR, ncols, typemap, columns, startpos, buf, pos, len, positions, pool, refs, rowsguess, rowoffset, types, flags, debug, options, coloptions, customtypes, numwarnings, maxwarnings)
            (pos > len || row == limit) && break
            # if our initial row estimate was too few, we need to reallocate our columsn to read the rest of the file
            if row + 1 > rowsguess
                # (bytes left in file) / (avg bytes per row) == estimated rows left in file (+ 10 for kicks)
                estimated_rows_left = ceil(Int64, (len - pos) / ((pos - startpos) / row) + 10.0)
                newrowsguess = rowsguess + estimated_rows_left
                debug && reallocatetape(rowoffset + row, rowsguess, newrowsguess)
                for i = 1:ncols
                    reallocate!(columns[i], newrowsguess)
                end
                rowsguess = newrowsguess
            end
        end
    end
    # done parsing (at least this chunk), so resize columns to final row count
    for i = 1:ncols
        resize!(columns[i], row)
    end
    return row, pos
end

@noinline function promotetostring!(col, TR::Val{transpose}, ncols, typemap, columns, buf, pos, len, limit, positions, pool, refs, rowsguess, rowoffset, types, origflags, debug, options::Parsers.Options{ignorerepeated}, coloptions, ::Type{customtypes}) where {transpose, ignorerepeated, customtypes}
    if columns[col] isa Vector{UInt32} && !lazystrings(origflags[col])
        # optimize conversion from PooledString => String w/o reparsing
        types[col] = Union{String, anymissing(origflags[col]) ? Missing : Union{}}
        pooledvalues = columns[col]
        columns[col] = column = allocate(lazystrings(origflags[col]) ? PosLen : String, rowsguess)
        colrefs = refs[col].refs
        refvalues = anymissing(origflags[col]) ? Vector{Union{String, Missing}}(undef, length(colrefs)) : Vector{String}(undef, length(colrefs))
        for (k, v) in colrefs
            @inbounds refvalues[v] = k
        end
        for row = 1:limit
            @inbounds column[row] = refvalues[pooledvalues[row]]
        end
        return
    end
    flags = copy(origflags)
    for i = 1:ncols
        if i == col
            flags[i] |= TYPEDETECTED
            columns[i] = allocate(lazystrings(flags[i]) ? PosLen : String, rowsguess)
            types[i] = Union{String, anymissing(flags[i]) ? Missing : Union{}}
        else
            flags[i] |= WILLDROP
        end
    end
    row = 0
    startpos = pos
    if pos <= len && len > 0
        numwarnings = Ref(1)
        while row < limit
            row += 1
            pos = parserow(row, TR, ncols, typemap, columns, startpos, buf, pos, len, positions, pool, refs, rowsguess, rowoffset, types, flags, debug, options, coloptions, customtypes, numwarnings, 0)
            pos > len && break
        end
    end
    return
end

@noinline reallocatetape(row, old, new) = @warn("thread = $(Threads.threadid()) warning: didn't pre-allocate enough column while parsing around row $row, re-allocating from $old to $new...")
@noinline notenoughcolumns(cols, ncols, row) = @warn("thread = $(Threads.threadid()) warning: only found $cols / $ncols columns around data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = @warn("thread = $(Threads.threadid()) warning: parsed expected $cols columns, but didn't reach end of line around data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = @warn("thread = $(Threads.threadid()) warning: error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) fatal error, encountered an invalidly quoted field while parsing around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))
@noinline toomanywwarnings() = @warn("thread = $(Threads.threadid()): too many warnings, silencing any further warnings")

@inline function parsecustom!(::Type{T}, flag, columns, buf, pos, len, opts, row, rowoffset, col, types, flags) where {T}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(typeof(column))")
        end)
        for i = 1:fieldcount(T)
            vec = fieldtype(T, i)
            pushfirst!(block.args, quote
                if column isa $(fieldtype(vec, 1))
                    return parsevalue!($(fieldtype(vec, 2)), flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
                end
            end)
        end
        pushfirst!(block.args, quote
            @inbounds column = columns[col]
        end)
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        @inbounds column = columns[col]
        return parsevalue!(eltype(parent(column)), flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
    end
end

@inline function parserow(row, TR::Val{transpose}, ncols, typemap, columns, startpos, buf, pos::A, len, positions, pool, refs, rowsguess, rowoffset, types, flags, debug, options::B, coloptions::C, ::Type{customtypes}, numwarnings, maxwarnings) where {transpose, A, B, C, customtypes}
    for col = 1:ncols
        if transpose
            @inbounds pos = positions[col]
        end
        @inbounds flag = flags[col]
        @inbounds column = columns[col]
        @inbounds opts = coloptions === nothing ? options : coloptions[col]
        # @show typeof(column)
        if willdrop(flag) || (user(flag) && column isa MissingVector)
            pos, code = parsemissing!(buf, pos, len, opts, row, rowoffset, col)
        elseif !typedetected(flag)
            pos, code = detect(columns, buf, pos, len, opts, row, rowoffset, col, typemap, pool, refs, debug, types, flags, rowsguess)
        elseif column isa SVec{Int64}
            pos, code = parseint!(flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa SVec{Float64}
            pos, code = parsevalue!(Float64, flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa SVec2{String}
            pos, code = parsestring2!(flag, column, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa SVec{Date}
            pos, code = parsevalue!(Date, flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa SVec{DateTime}
            pos, code = parsevalue!(DateTime, flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa SVec{Time}
            pos, code = parsevalue!(Time, flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa Vector{Union{Missing, Bool}}
            pos, code = parsevalue!(Bool, flag, column, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif column isa Vector{UInt32}
            pos, code = parsepooled!(flag, column, columns, buf, pos, len, opts, row, rowoffset, col, rowsguess, pool, refs, types, flags)
        elseif column isa Vector{PosLen}
            pos, code = parsestring!(flag, column, buf, pos, len, opts, row, rowoffset, col, types, flags)
        elseif customtypes !== Tuple{}
            pos, code = parsecustom!(customtypes, flag, columns, buf, pos, len, opts, row, rowoffset, col, types, flags)
        else
            error("bad array type: $(typeof(column))")
        end
        if promote_to_string(code)
            debug && println("promoting col = $col to string from $(typeof(column)) on chunk = $(Threads.threadid())")
            promotetostring!(col, TR, ncols, typemap, columns, buf, startpos, len, row, positions, pool, refs, rowsguess, rowoffset, types, flags, debug, options, coloptions, customtypes)
        end
        if transpose
            @inbounds positions[col] = pos
        else
            if col < ncols
                if Parsers.newline(code) || pos > len
                    options.silencewarnings || numwarnings[] > maxwarnings || notenoughcolumns(col, ncols, rowoffset + row)
                    !options.silencewarnings && numwarnings[] == maxwarnings && toomanywwarnings()
                    numwarnings[] += 1
                    for j = (col + 1):ncols
                        @inbounds flags[j] |= ANYMISSING
                        @inbounds types[j] = Union{Missing, types[j]}
                        if columns[j] isa Vector{UInt32}
                            ref = getref!(refs[j], missing, code, options)
                            columns[j][row] = ref
                            @inbounds flag = flags[j]
                            if !user(flag) && maybepooled(flag) && row == POOLSAMPLESIZE
                                resize!(columns[j], rowsguess)
                            end
                        end
                    end
                    break # from for col = 1:ncols
                end
            else
                if pos <= len && !Parsers.newline(code)
                    options.silencewarnings || numwarnings[] > maxwarnings || toomanycolumns(ncols, rowoffset + row)
                    numwarnings[] += 1
                    # ignore the rest of the line
                    pos = skiptorow(buf, pos, len, options.oq, options.e, options.cq, options.cmt, ignoreemptylines(options), 1, 2)
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

@inline function setposlen!(column, row, code, pos, len)
    @inbounds column[row] = poslen(code, pos, len)
    return
end

function detect(columns, buf, pos, len, options, row, rowoffset, col, typemap, pool, refs, debug, types, flags, rowsguess)
    # debug && println("detecting on task $(Threads.threadid())")
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, rowoffset + row, col)
    end
    if Parsers.sentinel(code) && code > 0
        flags[col] |= ANYMISSING
        # return; parsing will continue to detect until a non-missing value is parsed
        @goto finaldone
    end
    if Parsers.ok(code)
        intT = get(typemap, Int64, Int64)
        if intT <: Integer
            # debug && println("detecting Int64 for column = $col on task = $(Threads.threadid())")
            column = allocate(intT, rowsguess)
            column[row] = int
            newT = intT
            @goto done
        elseif intT === String
            @goto stringdetect
        end
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if Parsers.ok(code)
        floatT = get(typemap, Float64, Float64)
        if floatT <: AbstractFloat
            # debug && println("detecting Float64 for column = $col on task = $(Threads.threadid())")
            column = allocate(floatT, rowsguess)
            column[row] = float
            newT = floatT
            @goto done
        elseif floatT === String
            @goto stringdetect
        end
    end
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if Parsers.ok(code)
                dtT = get(typemap, Date, Date)
                if dtT === Date
                    column = allocate(Date, rowsguess)
                    column[row] = date
                    newT = Date
                    @goto done
                elseif dtT === String
                    @goto stringdetect
                end
            end
        catch
        end
        try
            datetime, code, vpos, vlen, tlen = Parsers.xparse(DateTime, buf, pos, len, options)
            if Parsers.ok(code)
                dtmT = get(typemap, DateTime, DateTime)
                if dtmT === DateTime
                    column = allocate(DateTime, rowsguess)
                    column[row] = datetime
                    newT = DateTime
                    @goto done
                elseif dtmT === String
                    @goto stringdetect
                end
            end
        catch
        end
        try
            time, code, vpos, vlen, tlen = Parsers.xparse(Time, buf, pos, len, options)
            if Parsers.ok(code)
                tT = get(typemap, Time, Time)
                if tT === Time
                    column = allocate(Time, rowsguess)
                    column[row] = time
                    newT = Time
                    @goto done
                elseif tT === String
                    @goto stringdetect
                end
            end
        catch
        end
    else
        try
            # use user-provided dateformat
            DT = timetype(options.dateformat)
            dt, code, vpos, vlen, tlen = Parsers.xparse(DT, buf, pos, len, options)
            if Parsers.ok(code)
                column = allocate(DT, rowsguess)
                column[row] = dt
                newT = DT
                @goto done
            end
        catch
        end
    end
    bool, code, vpos, vlen, tlen = Parsers.xparse(Bool, buf, pos, len, options)
    if Parsers.ok(code) && get(typemap, Bool, Bool) === Bool
        column = allocate(Bool, rowsguess)
        column[row] = bool
        newT = Bool
        @goto done
    end
@label stringdetect
    _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if pool > 0.0
        # debug && println("detecting PooledString for column = $col on task = $(Threads.threadid()); pool = $pool, row = $row, POOLSAMPLESIZE = $POOLSAMPLESIZE")
        r = RefPool()
        @inbounds refs[col] = r
        column = allocate(PooledString, pool < 1.0 && row < POOLSAMPLESIZE ? min(rowsguess, POOLSAMPLESIZE) : rowsguess)
        if pool < 1.0 && row < POOLSAMPLESIZE
            flags[col] |= MAYBEPOOLED
        end
        if anymissing(flags[col])
            ref = getref!(r, missing, code, options)
            for i = 1:(row - 1)
                @inbounds column[i] = ref
            end
        end
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), code, options)
        @inbounds column[row] = ref
        newT = PooledString
    else
        # debug && println("detecting String for column = $col on task = $(Threads.threadid()); pool = $pool, row = $row, POOLSAMPLESIZE = $POOLSAMPLESIZE")
        if lazystrings(flags[col])
            column = allocate(PosLen, rowsguess)
            @inbounds setposlen!(column, row, code, vpos, vlen)
        else
            column = allocate(String, rowsguess)
            @inbounds column[row] = str(buf, options.e, poslen(code, vpos, vlen))
        end
        newT = String
    end
@label done
    # if we're here, that means we found a non-missing value, so we need to update columns
    columns[col] = column
    flags[col] |= TYPEDETECTED
    types[col] = Union{newT, anymissing(flags[col]) ? Missing : Union{}}
@label finaldone
    return pos + tlen, code
end

function parseint!(flag, column, columns, buf, pos, len, options, row, rowoffset, col, types, flags)::Tuple{Int64, Int16}
    x, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if code > 0
        if !Parsers.sentinel(code)
            @inbounds column[row] = x
        else
            if !anymissing(flag)
                @inbounds flags[col] = flag | ANYMISSING
                @inbounds types[col] = Union{Int64, Missing}
            end
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, rowoffset + row, col)
        end
        if user(flag)
            if !options.strict
                options.silencewarnings || warning(Int64, buf, pos, tlen, code, rowoffset + row, col)
                flags[col] = flag | ANYMISSING
                types[col] = Union{Int64, Missing}
            else
                stricterror(Int64, buf, pos, tlen, code, rowoffset + row, col)
            end
        else
            # println("encountered an error parsing an Intger in auto-detect mode for column = $col, so let's try Float64")
            y, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
            if code > 0
                @inbounds columns[col] = convert(SentinelVector{Float64}, column)
                @inbounds columns[col][row] = y
                @inbounds types[col] = Union{Float64, anymissing(flag) ? Missing : Union{}}
            else
                # println("promoting to String instead")
                code |= PROMOTE_TO_STRING
            end
        end
    end
    return pos + tlen, code
end

function parsevalue!(::Type{type}, flag, column, columns, buf, pos, len, options, row, rowoffset, col, types, flags)::Tuple{Int64, Int16} where {type}
    x, code, vpos, vlen, tlen = Parsers.xparse(type, buf, pos, len, options)
    if code > 0
        if !Parsers.sentinel(code)
            @inbounds column[row] = x
        else
            if !anymissing(flag)
                @inbounds flags[col] = flag | ANYMISSING
                @inbounds types[col] = Union{type, Missing}
            end
        end
    else
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, rowoffset + row, col)
        end
        if user(flag)
            if !options.strict
                options.silencewarnings || warning(type, buf, pos, tlen, code, rowoffset + row, col)
                flags[col] = flag | ANYMISSING
                types[col] = Union{type, Missing}
            else
                stricterror(type, buf, pos, tlen, code, rowoffset + row, col)
            end
        else
            code |= PROMOTE_TO_STRING
        end
    end
    return pos + tlen, code
end

function parsestring!(flag, column, buf, pos, len, options, row, rowoffset, col, types, flags)::Tuple{Int64, Int16}
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    setposlen!(column, row, code, vpos, vlen)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, rowoffset + row, col)
    end
    if Parsers.sentinel(code)
        if !anymissing(flag)
            @inbounds flags[col] |= ANYMISSING
            @inbounds types[col] = Union{String, Missing}
        end
    end
    return pos + tlen, code
end

function parsestring2!(flag, column, buf, pos, len, options, row, rowoffset, col, types, flags)::Tuple{Int64, Int16}
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, rowoffset + row, col)
    end
    if Parsers.sentinel(code)
        if !anymissing(flag)
            @inbounds flags[col] |= ANYMISSING
            @inbounds types[col] = Union{String, Missing}
        end
    else
        @inbounds column[row] = str(buf, options.e, poslen(code, vpos, vlen))
    end
    return pos + tlen, code
end

function parsemissing!(buf, pos, len, options, row, rowoffset, col)::Tuple{Int64, Int16}
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, rowoffset + row, col)
    end
    return pos + tlen, code
end

@inline function getref!(refpool, key::Missing, code, options)
    get!(refpool.refs, key) do
        refpool.lastref += UInt32(1)
    end
end

@inline function getref!(refpool, key::PointerString, code, options)
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
    return ret
end

function parsepooled!(flag, column, columns, buf, pos, len, options, row, rowoffset, col, rowsguess, pool, refs, types, flags)::Tuple{Int64, Int16}
    x, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        # this usually means parsing is borked because of an invalidly quoted field, hard error
        fatalerror(buf, pos, tlen, code, rowoffset + row, col)
    end
    @inbounds colrefs = refs[col]
    if Parsers.sentinel(code)
        @inbounds flags[col] = flag | ANYMISSING
        @inbounds types[col] = Union{PooledString, Missing}
        ref = getref!(colrefs, missing, code, options)
    else
        ref = getref!(colrefs, PointerString(pointer(buf, vpos), vlen), code, options)
    end
    @inbounds column[row] = ref
    if !user(flag) && maybepooled(flag)
        if rowsguess <= POOLSAMPLESIZE && ((length(colrefs.refs) - anymissing(flags[col])) / rowsguess) > pool
            # println("promoting col = $col to string from pooled on task = $(Threads.threadid())")
            code |= PROMOTE_TO_STRING
        elseif row == POOLSAMPLESIZE
            if ((length(colrefs.refs) - anymissing(flags[col])) / min(rowsguess, POOLSAMPLESIZE)) > pool
                numrefs = length(colrefs.refs) - anymissing(flags[col])
                # println("promoting col = $col to string from pooled on task = $(Threads.threadid()); numrefs = $numrefs, POOLSAMPLESIZE = $POOLSAMPLESIZE")
                code |= PROMOTE_TO_STRING
            else
                numrefs = length(colrefs.refs) - anymissing(flags[col])
                # println("confirming col = $col is pooled on task = $(Threads.threadid()); numrefs = $numrefs")
                resize!(column, rowsguess)
            end
            flags[col] &= ~MAYBEPOOLED
        end
    end
    return pos + tlen, code
end
