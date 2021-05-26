# a Row "view" type for iterating `CSV.File`
struct Row <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{Column}
    lookup::Dict{Symbol, Column}
    row::Int64
end

getnames(r::Row) = getfield(r, :names)
getcolumn(r::Row, col::Int) = getfield(r, :columns)[col].column
getcolumn(r::Row, col::Symbol) = getfield(r, :lookup)[col].column
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
    columns::Vector{Column}
    lookup::Dict{Symbol, Column}
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
Base.eltype(::File) = Row
Base.size(f::File) = (getrows(f),)

Tables.isrowtable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), gettypes(f))
Tables.columns(f::File) = f
Tables.columnnames(f::File) = getnames(f)
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return haskey(lookup, col) ? lookup[col].column : getfield(f, col)
end

Tables.getcolumn(f::File, nm::Symbol) = getcolumn(f, nm).column
Tables.getcolumn(f::File, i::Int) = getcolumn(f, i).column

Base.@propagate_inbounds function Base.getindex(f::File, row::Int)
    @boundscheck checkbounds(f, row)
    return Row(getnames(f), getcolumns(f), getlookup(f), row)
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

### File layout options:

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
  * `lines_to_check::Integer=30`: for multithreaded parsing, a file is split up into `tasks` # of equal chunks, then `lines_to_check` # of lines are checked to ensure parsing correctly found valid rows; for certain files with very large quoted text fields, `lines_to_check` may need to be higher (10, 30, etc.) to ensure parsing correctly finds these rows
  * `select`: an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "selector" function of the form `(i, name) -> keep::Bool`; only columns in the collection or for which the selector function returns `true` will be parsed and accessible in the resulting `CSV.File`. Invalid values in `select` are ignored.
  * `drop`: inverse of `select`; an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a "drop" function of the form `(i, name) -> drop::Bool`; columns in the collection or for which the drop function returns `true` will ignored in the resulting `CSV.File`. Invalid values in `drop` are ignored.

### Parsing options:

  * `missingstrings`, `missingstring`: either a `String`, or `Vector{String}` to use as sentinel values that will be parsed as `missing`; by default, only an empty field (two consecutive delimiters) is considered `missing`
  * `delim=','`: a `Char` or `String` that indicates how columns are delimited in a file; if no argument is provided, parsing will try to detect the most consistent delimiter on the first 10 rows of the file
  * `ignorerepeated::Bool=false`: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells
  * `quotechar='"'`, `openquotechar`, `closequotechar`: a `Char` (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters
  * `escapechar='"'`: the `Char` used to escape quote characters in a quoted field
  * `dateformat::Union{String, Dates.DateFormat, Nothing}`: a date format string to indicate how Date/DateTime columns are formatted for the entire file
  * `dateformats::Union{AbstractDict, Nothing}`: a Dict of date format strings to indicate how the Date/DateTime columns corresponding to the keys are formatted. The Dict can map column index `Int`, or name `Symbol` or `String` to the format string for that column.
  * `decimal='.'`: a `Char` indicating how decimals are separated in floats, i.e. `3.14` used '.', or `3,14` uses a comma ','
  * `truestrings`, `falsestrings`: `Vectors of Strings` that indicate how `true` or `false` values are represented; by default only `true` and `false` are treated as `Bool`

### Column Type Options:

  * `type`: a single type to use for parsing an entire file; i.e. all columns will be treated as the same type; useful for matrix-like data files
  * `types`: a Vector or Dict of types to be used for column types; a Dict can map column index `Int`, or name `Symbol` or `String` to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict("column1"=>Float64) will set the column1 to Float64; if a `Vector` if provided, it must match the # of columns provided or detected in `header`
  * `typemap::Dict{Type, Type}`: a mapping of a type that should be replaced in every instance with another type, i.e. `Dict(Float64=>String)` would change every detected `Float64` column to be parsed as `String`; only "standard" types are allowed to be mapped to another type, i.e. `Int64`, `Float64`, `Date`, `DateTime`, `Time`, and `Bool`. If a column of one of those types is "detected", it will be mapped to the specified type.
  * `pool::Union{Bool, Float64}=0.1`: if `true`, *all* columns detected as `String` will be internally pooled; alternatively, the proportion of unique values below which `String` columns should be pooled (by default 0.1, meaning that if the # of unique strings in a column is under 10%, it will be pooled)
  * `lazystrings::Bool=false`: avoid allocating full strings in string columns; returns a custom `PosLenStringVector` array type that *does not* support mutable operations (e.g. `push!`, `append!`, or even `setindex!`). Calling `copy(x)` will materialize a full `Vector{String}`. Also note that each `PosLenStringVector` holds a reference to the full input file buffer, so it won't be closed after parsing and trying to delete or modify the file may result in errors (particularly on windows) and generally has undefined behavior. Given these caveats, this setting can help avoid lots of string allocations in large files and lead to faster parsing times.
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
    ignoreemptylines::Bool=true,
    select=nothing,
    drop=nothing,
    limit::Union{Integer, Nothing}=nothing,
    threaded::Union{Bool, Nothing}=nothing,
    tasks::Integer=Threads.nthreads(),
    lines_to_check::Integer=DEFAULT_LINES_TO_CHECK,
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
    truestrings::Union{Vector{String}, Nothing}=TRUE_STRINGS,
    falsestrings::Union{Vector{String}, Nothing}=FALSE_STRINGS,
    # type options
    type=nothing,
    types=nothing,
    typemap::Dict=Dict{Type, Type}(),
    pool::Union{Bool, Real, AbstractVector, AbstractDict}=NaN,
    lazystrings::Bool=false,
    stringtype::StringTypes=String,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=DEFAULT_MAX_WARNINGS,
    debug::Bool=false,
    parsingdebug::Bool=false
    )
    # header=1;normalizenames=false;datarow=-1;skipto=nothing;footerskip=0;transpose=false;comment=nothing;ignoreemptylines=true;
    # select=nothing;drop=nothing;limit=nothing;threaded=nothing;tasks=8;lines_to_check=30;missingstrings=String[];missingstring="";
    # delim=nothing;ignorerepeated=false;quotechar='"';openquotechar=nothing;closequotechar=nothing;escapechar='"';dateformat=nothing;
    # dateformats=nothing;decimal=UInt8('.');truestrings=nothing;falsestrings=nothing;type=nothing;types=nothing;typemap=Dict{Type,Type}();
    # pool=NaN;lazystrings=false;stringtype=String;strict=false;silencewarnings=false;maxwarnings=100;debug=true;parsingdebug=false;
    ctx = Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptylines, select, drop, limit, threaded, tasks, lines_to_check, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, pool, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, false)
    return File(ctx)
end

function File(ctx::Context, chunking::Bool=false)
    @inbounds begin
    # we now do our parsing pass over the file, starting at datapos
    if ctx.threaded
        # multithreaded parsing
        rowsguess, ntasks, columns, limit = ctx.rowsguess, ctx.ntasks, ctx.columns, ctx.limit
        # calculate our guess for how many rows will be parsed by each concurrent parsing task
        rowchunkguess = cld(rowsguess, ntasks)
        wholecolumnslock = ReentrantLock() # in case columns are widened during parsing
        pertaskcolumns = Vector{Vector{Column}}(undef, ntasks)
        # initialize each top-level column's lock; used after a task is done parsing its chunk of rows
        # and it "checks in" the types it parsed for each column
        foreach(col -> col.lock = ReentrantLock(), columns)
        rows = zeros(Int64, ntasks) # how many rows each parsing task ended up actually parsing
        @sync for i = 1:ntasks
            Threads.@spawn begin
                tt = Base.time()
                task_columns = [Column(col) for col in columns] # task-local columns derived from top-level columns
                allocate!(task_columns, rowchunkguess)
                pertaskcolumns[i] = task_columns
                task_pos = ctx.chunkpositions[i]
                task_len = ctx.chunkpositions[i + 1] - (i != ntasks)
                # for error-reporting purposes, we want to try and give the best guess of where a row emits a warning/error, so compute that
                rowchunkoffset = (ctx.datarow - 1) + (rowchunkguess * (i - 1))
                task_rows, task_pos = parsefilechunk!(ctx, ctx.buf, task_pos, task_len, rowchunkguess, rowchunkoffset, task_columns, ctx.transpose, ctx.options, ctx.coloptions, ctx.customtypes)
                rows[i] = task_rows
                # promote column types/flags this task detected while parsing
                lock(wholecolumnslock) do
                    # check if this task widened columns while parsing
                    if length(task_columns) > length(columns)
                        for j = (length(columns) + 1):length(task_columns)
                            col = task_columns[j] # I'm pretty sure it's ok to just use the per-task column directly here as new top-level column?
                            # initialize lock since it hasn't been initialized yet
                            col.lock = ReentrantLock()
                            push!(columns, col)
                        end
                    end
                end
                # now we know that columns is at least as long as task_columns
                for j = 1:length(task_columns)
                    col = columns[j]
                    # note col.lock is shared amongst all tasks (i.e. belongs to parent columns[i].lock)
                    lock(col.lock) do
                        task_col = task_columns[j]
                        T = col.type
                        col.type = something(promote_types(T, task_col.type), ctx.stringtype)
                        if T !== col.type
                            ctx.debug && println("promoting col = $j from $T to $(col.type), task chunk ($i) was type = $(task_col.type)")
                        end
                        col.anymissing |= task_col.anymissing
                    end
                end
                ctx.debug && println("finished parsing $task_rows rows on task = $i: time for parsing: $(Base.time() - tt)")
            end
        end
        finalrows = sum(rows)
        if ctx.limit < finalrows
            finalrows = limit
        end
        # ok, all the parsing tasks have finished and we've promoted their types w/ the top-level columns
        # so now we just need to finish processing each column by making ChainedVectors of the individual columns
        # from each task
        # quick check that each set of task columns has the right # of columns
        for i = 1:ntasks
            task_columns = pertaskcolumns[i]
            if length(task_columns) < length(columns)
                # some other task widened columns that this task didn't likewise detect
                for _ = (length(task_columns) + 1):length(columns)
                    push!(task_columns, Column(Missing))
                end
            end
        end
        @sync for (j, col) in enumerate(columns)
            Threads.@spawn begin
                for i = 1:ntasks
                    task_columns = pertaskcolumns[i]
                    task_col = task_columns[j]
                    task_rows = rows[i]
                    # check if we need to promote a task-local column based on what other threads parsed
                    T = col.type # final promoted type from amongst all separate parsing tasks
                    T2 = task_col.type
                    if T isa StringTypes && !(T2 isa StringTypes)
                        # promoting non-string to string column
                        ctx.debug && println("multithreaded promoting column $j to string from $T2")
                        task_len = ctx.chunkpositions[i + 1] - (i != ntasks)
                        task_pos = ctx.chunkpositions[i]
                        promotetostring!(ctx, ctx.buf, task_pos, task_len, task_rows, (ctx.datarow - 1) + (rowchunkguess * (i - 1)), task_columns, ctx.transpose, ctx.options, ctx.coloptions, ctx.customtypes, j, Ref(0), task_rows)
                    elseif T === Float64 && T2 <: Integer
                        # one chunk parsed as Int, another as Float64, promote to Float64
                        ctx.debug && println("multithreaded promoting column $j to float")
                        if pooled(col)
                            task_col.refpool.refs = convert(Refs{Float64}, task_col.refpool.refs)
                        else
                            task_col.column = convert(SentinelVector{Float64}, task_col.column)
                        end
                    elseif T !== T2
                        # one chunk parsed all missing values, but another chunk had a typed value, promote to that
                        # while keeping all values `missing` (allocate by default ensures columns have all missing values)
                        ctx.debug && println("multithreaded promoting column $j from missing on task $i")
                        task_col.column = allocate(pooled(col) ? Pooled : T, task_rows)
                    end
                    # synchronize refs if needed
                    if pooled(col)
                        if !isdefined(col, :refpool) && isdefined(task_col, :refpool)
                            # this case only occurs if user explicitly passed pool=true
                            # but we only parsed `missing` values during sampling
                            col.refpool = task_col.refpool
                        elseif isdefined(col, :refpool) && pooltype(col) !== T && isdefined(task_col, :refpool)
                            # we pooled/detected one type while sampling, but parsing promoted to another type
                            # if the task_col has a refpool, then we know it's the promoted type
                            col.refpool = task_col.refpool
                        elseif isdefined(task_col, :refpool)
                            # @show j, i, col.type, task_col.type, typeof(col.refpool.refs), typeof(task_col.refpool.refs)
                            syncrefs!(col.type, col, task_col, task_rows)
                        end
                    end
                end
                if pooled(col)
                    makechain!(pertaskcolumns, col, j, ntasks)
                    # pooled columns are the one case where we invert the order of arrays;
                    # i.e. we return PooledArray{T, ChainedVector{T}} instead of ChainedVector{T, PooledArray{T}}
                    makepooled!(col)
                elseif col.type === Int64
                    # we need to special-case Int64 here because while parsing, a default Int64 sentinel value is chosen to
                    # represent missing; if any chunk bumped into that sentinel value while parsing, then it cycled to a 
                    # new sentinel value; this step ensure that each chunk has the same encoded sentinel value
                    # passing force=false means it will first check if all chunks already have the same sentinel and return
                    # immediately if so, which will be the case most often
                    SentinelArrays.newsentinel!((pertaskcolumns[i][j].column::SVec{Int64} for i = 1:ntasks)...; force=false)
                    makechain!(pertaskcolumns, col, j, ntasks)
                elseif col.type === PosLenString
                    col.column = ChainedVector(PosLenStringVector{coltype(col)}[makeposlen!(pertaskcolumns[i][j], coltype(col), ctx) for i = 1:ntasks])
                elseif col.type === NeedsTypeDetection || col.type === HardMissing
                    col.type = Missing
                    col.column = MissingVector(finalrows)
                else
                    makechain!(pertaskcolumns, col, j, ntasks)
                end
                if finalrows < length(col.column)
                    # we only ever resize! down here, so no need to use reallocate!
                    resize!(col.column, finalrows)
                end
            end
        end
    else
        # single-threaded parsing
        columns = ctx.columns
        allocate!(columns, ctx.rowsguess)
        t = Base.time()
        finalrows, pos = parsefilechunk!(ctx, ctx.buf, ctx.datapos, ctx.len, ctx.rowsguess, 0, columns, ctx.transpose, ctx.options, ctx.coloptions, ctx.customtypes)
        ctx.debug && println("time for initial parsing: $(Base.time() - t)")
        # cleanup our columns if needed
        for col in columns
@label processcolumn
            if !isdefined(col, :refpool) && pooled(col)
                col.type = Missing
                col.column = allocate(Pooled, finalrows)
                col.refpool = RefPool(Missing)
            elseif col.type === NeedsTypeDetection
                # fill in uninitialized column fields
                col.type = Missing
                col.column = MissingVector(finalrows)
                col.pool = 0.0
            end
            if pooled(col)
                makepooled!(col)
            elseif col.column isa Vector{UInt32}
                # check if final column should be PooledArray or not
                if ((length(col.refpool.refs) - 1) / finalrows) <= ifelse(isnan(col.pool), SINGLE_THREADED_POOL_DEFAULT, col.pool)
                    makepooled!(col)
                else
                    # cardinality too high, so unpool
                    unpool!(col)
                    @goto processcolumn
                end
            elseif col.type === PosLenString
                # string col parsed lazily; return a PosLenStringVector
                makeposlen!(col, coltype(col), ctx)
            elseif !col.anymissing
                # if no missing values were parsed for a col, we want to "unwrap" it to a plain Vector{T}
                if col.type === Bool
                    col.column = convert(Vector{Bool}, col.column)
                elseif col.type !== Union{} && col.type <: SmallIntegers
                    col.column = convert(Vector{col.type}, col.column)
                else
                    col.column = parent(col.column)
                end
            end
        end
    end
    # delete any dropped columns from names, columns
    names = ctx.names
    if length(columns) > length(names)
        # columns were widened during parsing, auto-generate trailing column names
        names = makeunique(append!(names, [Symbol(:Column, i) for i = (length(names) + 1):length(columns)]))
    end
    for i = length(columns):-1:1
        col = columns[i]
        if col.willdrop
            deleteat!(names, i)
            deleteat!(columns, i)
        end
    end
    types = Type[coltype(col) for col in columns]
    lookup = Dict(k => v for (k, v) in zip(names, columns))
    ctx.debug && println("types after parsing: $types, pool = $(ctx.pool)")
    # for windows, it's particularly finicky about throwing errors when you try to modify an mmapped file
    # so we just want to make sure we finalize the input buffer so users don't run into surprises
    if !chunking && Sys.iswindows() && ctx.stringtype !== PosLenString
        finalize(ctx.buf)
    end
    end # @inbounds begin
    return File{ctx.threaded}(ctx.name, names, types, finalrows, length(columns), columns, lookup)
end

const EMPTY_INT_ARRAY = Int64[]
const EMPTY_REFRECODE = UInt32[]

# after multithreaded parsing, we need to synchronize pooled refs from different chunks of the file
# we pick one chunk as "source of truth", then adjust other chunks as needed
function syncrefs!(::Type{T}, col, task_col, task_rows) where {T}
    @assert task_col.column isa Vector{UInt32}
    # @inbounds begin
    colrefpool = col.refpool
    colrefs = colrefpool.refs::Refs{T}
    taskrefpool = task_col.refpool
    taskrefs = taskrefpool.refs::Refs{T}
    refrecodes = EMPTY_REFRECODE
    recode = false
    for (k, v) in taskrefs
        # `k` is our task-specific parsed value
        # `v` is our task-specific UInt32 ref code
        # for each task-specific ref, check if it matches parent column ref
        refvalue = get(colrefs, k, UInt32(0))
        if refvalue != v
            # task-specific ref didn't match parent column ref, need to recode
            recode = true
            if isempty(refrecodes)
                # refrecodes is indexed by task-specific ref code
                # each *element*, however, is the parent column ref code
                refrecodes = [UInt32(i) for i = 1:taskrefpool.lastref]
            end
            if refvalue == 0
                # parent column didn't know about this ref, so we add it
                refvalue = (colrefpool.lastref += UInt32(1))
                colrefs[k] = refvalue
            end
            refrecodes[v] = refvalue
        end
    end
    if recode
        column = task_col.column::Vector{UInt32}
        for j = 1:task_rows
            # here we recode by replacing task column ref code w/ parent column ref code
            column[j] = refrecodes[column[j]]
        end
    end
    # end # @inbounds begin
    return
end

function makechain!(pertaskcolumns, col, j, ntasks)
    if col.anymissing
        col.column = ChainedVector([pertaskcolumns[i][j].column for i = 1:ntasks])
    else
        if col.type === Bool
            col.column = ChainedVector([convert(Vector{Bool}, pertaskcolumns[i][j].column) for i = 1:ntasks])
        elseif col.type !== Union{} && col.type <: SmallIntegers
            col.column = ChainedVector([convert(Vector{col.type}, pertaskcolumns[i][j].column) for i = 1:ntasks])
        else
            col.column = ChainedVector([parent(pertaskcolumns[i][j].column) for i = 1:ntasks])
        end
    end
    return
end

default(::Type{PosLenString}) = PosLenString(UInt8[], WeakRefStrings.MISSING_BIT, 0x00)
default(::Type{String}) = ""
default(::Type{T}) where {T} = zero(T)
default(::Type{Union{}}) = nothing

function makepooled!(col)
    T = col.type
    r = isdefined(col, :refpool) ? col.refpool.refs : Refs{T === NeedsTypeDetection ? Missing : T}()
    column = isdefined(col, :column) ? col.column : UInt32[]
    col.column = PooledArray{coltype(col)}(PooledArrays.RefArray(column), r)
    return
end

function unpool!(col)
    r = col.refpool.refs
    pool = Vector{keytype(r)}(undef, length(r))
    for (k, v) in r
        @inbounds pool[v] = k
    end
    if col.type === PosLenString
        # unwrap the PosLenStrings, so they get handled by makeposlen!
        col.column = [pool[ref].poslen for ref in col.column]
    else
        col.column = [pool[ref] for ref in col.column]
    end
    col.pool = 0.0
    return
end

function makeposlen!(col, T, ctx)
    col.column = PosLenStringVector{T}(ctx.buf, col.column, ctx.options.e)
    return col.column
end

function parsefilechunk!(ctx::Context, buf, pos, len, rowsguess, rowoffset, columns, TR::Val{transpose}, opts, coloptions, ::Type{customtypes}) where {transpose, customtypes}
    limit = ctx.limit
    row = 0
    startpos = pos
    if pos <= len && len > 0 && row < limit
        numwarnings = Ref(0)
        while true
            row += 1
            # @show columns
            @inbounds pos = parserow(startpos, row, numwarnings, ctx, buf, pos, len, rowsguess, rowoffset, columns, TR, opts, coloptions, customtypes)
            # @show columns
            row == limit && break
            (transpose ? all(c -> c.position >= c.endposition, columns) : pos > len) && break
            # if our initial row estimate was too few, we need to reallocate our columns to read the rest of the file/chunk
            if !transpose && row + 1 > rowsguess
                # (bytes left in file/chunk) / (avg bytes per row) == estimated rows left in file (+ 5% to try and avoid reallocating)
                estimated_rows_left = ceil(Int64, ((len - pos) / ((pos - startpos) / row)) * 1.05)
                newrowsguess = rowsguess + estimated_rows_left
                newrowsguess = max(rowsguess + 1, newrowsguess)
                ctx.debug && reallocatecolumns(rowoffset + row, rowsguess, newrowsguess)
                for col in columns
                    isdefined(col, :column) && reallocate!(col.column, newrowsguess)
                end
                rowsguess = newrowsguess
            end
        end
    end
    # done parsing (at least this chunk), so resize columns to final row count
    for col in columns
        # we only ever resize! down here, so no need to use reallocate!
        isdefined(col, :column) && resize!(col.column, row)
    end
    return row, pos
end

@noinline reallocatecolumns(row, old, new) = @warn("thread = $(Threads.threadid()) warning: didn't pre-allocate enough column while parsing around row $row, re-allocating from $old to $new...")
@noinline notenoughcolumns(cols, ncols, row) = @warn("thread = $(Threads.threadid()) warning: only found $cols / $ncols columns around data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = @warn("thread = $(Threads.threadid()) warning: parsed expected $cols columns, but didn't reach end of line around data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = @warn("thread = $(Threads.threadid()) warning: error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) fatal error, encountered an invalidly quoted field while parsing around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))
@noinline toomanywwarnings() = @warn("thread = $(Threads.threadid()): too many warnings, silencing any further warnings")

Base.@propagate_inbounds function parserow(startpos, row, numwarnings, ctx::Context, buf, pos, len, rowsguess, rowoffset, columns, TR::Val{transpose}, options, coloptions, ::Type{customtypes}) where {transpose, customtypes}
    # @show columns
    ncols = length(columns)
    for i = 1:ncols
        col = columns[i]
        if transpose
            pos = col.position
        end
        type = col.type
        opts = coloptions === nothing ? options : coloptions[i]
        if type === HardMissing
            pos, code = parsevalue!(Missing, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === NeedsTypeDetection
            pos, code = detect(buf, pos, len, opts, row, rowoffset, i, col, ctx, rowsguess)
        elseif type === Int64
            pos, code = parsevalue!(Int64, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === Float64
            pos, code = parsevalue!(Float64, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === String
            pos, code = parsevalue!(String, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === PosLenString
            pos, code = parsevalue!(PosLenString, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === Date
            pos, code = parsevalue!(Date, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === DateTime
            pos, code = parsevalue!(DateTime, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === Time
            pos, code = parsevalue!(Time, buf, pos, len, opts, row, rowoffset, i, col)
        elseif type === Bool
            pos, code = parsevalue!(Bool, buf, pos, len, opts, row, rowoffset, i, col)
        else
            if customtypes !== Tuple{}
                pos, code = parsecustom!(customtypes, buf, pos, len, opts, row, rowoffset, i, col)
            else
                error("bad column type: $(type))")
            end
        end
        if promote_to_string(code)
            ctx.debug && println("promoting column i = $i to string from $(type) on chunk = $(Threads.threadid())")
            promotetostring!(ctx, buf, startpos, len, rowsguess, rowoffset, columns, TR, options, coloptions, customtypes, i, numwarnings, row)
        end
        if transpose
            col.position = pos
        else
            if i < ncols
                if Parsers.newline(code) || pos > len
                    opts.silencewarnings || numwarnings[] > ctx.maxwarnings || notenoughcolumns(i, ncols, rowoffset + row)
                    !opts.silencewarnings && numwarnings[] == ctx.maxwarnings && toomanywwarnings()
                    numwarnings[] += 1
                    for j = (i + 1):ncols
                        columns[j].anymissing = true
                    end
                    break # from for i = 1:ncols
                end
            elseif pos <= len && !Parsers.newline(code)
                # extra columns on this row, let's widen
                j = i + 1
                T = ctx.streaming ? Union{ctx.stringtype, Missing} : NeedsTypeDetection
                while pos <= len && !Parsers.newline(code)
                    col = Column(T)
                    col.anymissing = true # assume all previous rows were missing
                    col.pool = ctx.pool
                    if T === NeedsTypeDetection
                        pos, code = detect(buf, pos, len, opts, row, rowoffset, j, col, ctx, rowsguess)
                    else
                        # need to allocate
                        col.column = allocate(ctx.stringtype, ctx.rowsguess)
                        pos, code = parsevalue!(ctx.stringtype, buf, pos, len, opts, row, rowoffset, j, col)
                    end
                    j += 1
                    push!(columns, col)
                    if coloptions !== nothing
                        push!(coloptions, ctx.options)
                    end
                end
            end
        end
    end
    return pos
end

@noinline function _parseany(T, buf, pos, len, opts)::Tuple{Any, Int16, Int64, Int64, Int64}
    return Parsers.xparse(T, buf, pos, len, opts)
end

function detect(buf, pos, len, opts, row, rowoffset, i, col, ctx, rowsguess)::Tuple{Int64, Int16}
    # debug && println("detecting on task $(Threads.threadid())")
    x, code, tlen = detect(buf, pos, len, opts, false, rowoffset + row, i)
    if x === missing
        col.anymissing = true
        @goto finaldone
    end
    if x !== nothing
        # we found a non-missing value
        newT = get(ctx.typemap, typeof(x), typeof(x))
        if !(newT isa StringTypes)
            if newT !== typeof(x)
                # type-mapping typeof(x) => newT
                # this ultimate call to Parsers.xparse has no hope in inference (because of the typeof(x) => newT mapping)
                # so we "outline" the call and assert the types of everything but `y` to make sure `code` and `tlen` stay type stable
                y, code, vpos, vlen, tlen = _parseany(newT, buf, pos, len, opts)
                if Parsers.ok(code)
                    val = y
                    @goto done
                end
            else
                val = x
                @goto done
            end
        end
    end
@label stringdetect
    _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, opts)
    if ctx.stringtype === PosLenString
        newT = PosLenString
        val = PosLen(vpos, vlen, Parsers.sentinel(code), Parsers.escapedstring(code))
    elseif ctx.stringtype === String
        newT = String
        poslen = PosLen(vpos, vlen, Parsers.sentinel(code), Parsers.escapedstring(code))
        val = String(buf, poslen, opts.e)
    end
@label done
    # if we're here, that means we found a non-missing value, so we need to update column
    if pooled(col) || maybepooled(col) || (isnan(col.pool) && newT isa StringTypes)
        column = allocate(Pooled, rowsguess)
        col.refpool = RefPool(newT)
        val = getref!(col.refpool, newT, val isa PosLen ? PosLenString(buf, val, opts.e) : val)
    else
        column = allocate(newT, rowsguess)
    end
    column[row] = val
    col.column = column
    col.type = newT
@label finaldone
    return pos + tlen, code
end

function parsevalue!(::Type{type}, buf, pos, len, opts, row, rowoffset, i, col)::Tuple{Int64, Int16} where {type}
    x, code, vpos, vlen, tlen = Parsers.xparse(type === Missing ? String : type, buf, pos, len, opts)
    if code > 0
        if type !== Missing
            if Parsers.sentinel(code)
                col.anymissing = true
            else
                column = col.column
                if column isa Vector{UInt32}
                    if type === String || type === PosLenString
                        if Parsers.escapedstring(code)
                            poslen = PosLen(vpos, vlen, Parsers.sentinel(code), Parsers.escapedstring(code))
                            ref = getref!(col.refpool, type, String(buf, poslen, opts.e))
                        else
                            ref = getref!(col.refpool, type, PointerString(pointer(buf, vpos), vlen))
                        end
                    else
                        ref = getref!(col.refpool, type, x)
                    end
                    @inbounds column[row] = ref
                elseif type === String
                    poslen = PosLen(vpos, vlen, Parsers.sentinel(code), Parsers.escapedstring(code))
                    @inbounds (column::SVec2{String})[row] = String(buf, poslen, opts.e)
                elseif type === PosLenString
                    poslen = PosLen(vpos, vlen, Parsers.sentinel(code), Parsers.escapedstring(code))
                    @inbounds (column::Vector{PosLen})[row] = poslen
                else
                    @inbounds (column::vectype(type))[row] = x
                end
            end
        end
    else
        # something went wrong parsing
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, tlen, code, rowoffset + row, i)
        end
        if type !== Missing && !(type isa StringTypes)
            if col.userprovidedtype
                if !opts.strict
                    opts.silencewarnings || warning(type, buf, pos, tlen, code, rowoffset + row, i)
                    col.anymissing = true
                else
                    stricterror(type, buf, pos, tlen, code, rowoffset + row, i)
                end
            else
                if type === Int64
                    y, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, opts)
                    if code > 0
                        col.type = Float64
                        column = col.column
                        if column isa Vector{UInt32}
                            col.refpool.refs = convert(Refs{Float64}, col.refpool.refs)
                            ref = getref!(col.refpool, Float64, y)
                            @inbounds column[row] = ref
                        elseif column isa SVec{Int64}
                            col.column = convert(SentinelVector{Float64}, column)
                            @inbounds col.column[row] = y
                        end
                    else
                        code |= PROMOTE_TO_STRING
                    end
                else
                    code |= PROMOTE_TO_STRING
                end
            end
        end
    end
    return pos + tlen, code
end

@inline function getref!(refpool, ::Type{T}, key) where {T}
    x = refpool.refs::Refs{T}
    get!(x, key) do
        refpool.lastref += UInt32(1)
    end
end

@inline function getref!(refpool, ::Type{T}, key::PointerString) where {T}
    x = refpool.refs::Refs{T}
    index = Base.ht_keyindex2!(x, key)
    if index > 0
        @inbounds found_key = x.vals[index]
        ret = found_key::UInt32
    else
        @inbounds new = refpool.lastref += UInt32(1)
        @inbounds Base._setindex!(x, new, T(key), -index)
        ret = new
    end
    return ret
end

@inline function parsecustom!(::Type{customtypes}, buf, pos, len, opts, row, rowoffset, i, col) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(type)")
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    return parsevalue!($T, buf, pos, len, opts, row, rowoffset, i, col)
                end
            end)
        end
        pushfirst!(block.args, :(type = col.type))
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        return parsevalue!(col.type, buf, pos, len, opts, row, rowoffset, i, col)
    end
end

@noinline function promotetostring!(ctx::Context, buf, pos, len, rowsguess, rowoffset, columns, TR::Val{transpose}, opts, coloptions, ::Type{customtypes}, column_to_promote, numwarnings, limit) where {transpose, customtypes}
    cols = [i == column_to_promote ? columns[i] : Column(Missing) for i = 1:length(columns)]
    col = cols[column_to_promote]
    if pooled(col) || maybepooled(col) || isnan(col.pool)
        col.refpool = RefPool(ctx.stringtype)
        col.column = allocate(Pooled, rowsguess)
    else
        col.column = allocate(ctx.stringtype, rowsguess)
    end
    col.type = ctx.stringtype
    row = 0
    startpos = pos
    if pos <= len && len > 0
        while row < limit
            row += 1
            @inbounds pos = parserow(startpos, row, numwarnings, ctx, buf, pos, len, rowsguess, rowoffset, cols, TR, opts, coloptions, customtypes)
            pos > len && break
        end
    end
    return
end
