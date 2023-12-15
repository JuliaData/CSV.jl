# a Row "view" type for iterating `CSV.File`
struct Row <: Tables.AbstractRow
    names::Vector{Symbol}
    columns::Vector{Column}
    lookup::Dict{Symbol, Column}
    row::Int
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

"""
    CSV.File(input; kwargs...) => CSV.File

Read a UTF-8 CSV input and return a `CSV.File` object, which is like a lightweight table/dataframe, allowing dot-access to columns
and iterating rows. Satisfies the Tables.jl interface, so can be passed to any valid sink, yet to avoid unnecessary copies of data,
use `CSV.read(input, sink; kwargs...)` instead if the `CSV.File` intermediate object isn't needed.

The [`input`](@ref input) argument can be one of:
  * filename given as a string or FilePaths.jl type
  * a `Vector{UInt8}` or `SubArray{UInt8, 1, Vector{UInt8}}` byte buffer
  * a `CodeUnits` object, which wraps a `String`, like `codeunits(str)`
  * a csv-formatted string can also be passed like `IOBuffer(str)`
  * a `Cmd` or other `IO`
  * a gzipped file (or gzipped data in any of the above), which will automatically be decompressed for parsing
  * a `Vector` of any of the above, which will parse and vertically concatenate each source, returning a single, "long" `CSV.File`

To read a csv file from a url, use the Downloads.jl stdlib or HTTP.jl package, where the resulting downloaded tempfile or `HTTP.Response` body can be passed like:
```julia
using Downloads, CSV
f = CSV.File(Downloads.download(url))

# or

using HTTP, CSV
f = CSV.File(HTTP.get(url).body)
```

Opens the file or files and uses passed arguments to detect the number of columns and column types, unless column types are provided
manually via the `types` keyword argument. Note that passing column types manually can slightly increase performance
for each column type provided (column types can be given as a `Vector` for all columns, or specified per column via
name or index in a `Dict`).

When a `Vector` of inputs is provided, the column names and types of each separate file/input must match to be vertically concatenated. Separate threads will
be used to parse each input, which will each parse their input using just the single thread. The results of all threads are then vertically concatenated using
`ChainedVector`s to lazily concatenate each thread's columns.

For text encodings other than UTF-8, load the [StringEncodings.jl](https://github.com/JuliaStrings/StringEncodings.jl)
package and call e.g. `CSV.File(open(read, input, enc"ISO-8859-1"))`.

The returned `CSV.File` object supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface
and can iterate `CSV.Row`s. `CSV.Row` supports `propertynames` and `getproperty` to access individual row values. `CSV.File`
also supports entire column access like a `DataFrame` via direct property access on the file object, like `f = CSV.File(file); f.col1`.
Or by getindex access with column names, like `f[:col1]` or `f["col1"]`. The returned columns are `AbstractArray` subtypes, including:
`SentinelVector` (for integers), regular `Vector`, `PooledVector` for pooled columns, `MissingVector` for columns of all `missing` values,
`PosLenStringVector` when `stringtype=PosLenString` is passed, and `ChainedVector` will chain one of the previous array types together for
data inputs that use multiple threads to parse (each thread parses a single "chain" of the input).
Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name `a` will become `a_1`).
For example, one could iterate over a csv file with column names `a`, `b`, and `c` by doing:

```julia
for row in CSV.File(file)
    println("a=\$(row.a), b=\$(row.b), c=\$(row.c)")
end
```

By supporting the Tables.jl interface, a `CSV.File` can also be a table input to any other table sink function. Like:

```julia
# materialize a csv file as a DataFrame, copying columns from CSV.File
df = CSV.File(file) |> DataFrame

# to avoid making a copy of parsed columns, use CSV.read
df = CSV.read(file, DataFrame)

# load a csv file directly into an sqlite database table
db = SQLite.DB()
tbl = CSV.File(file) |> SQLite.load!(db, "sqlite_table")
```

$KEYWORD_DOCS
"""
struct File <: AbstractVector{Row}
    name::String
    names::Vector{Symbol}
    types::Vector{Type}
    rows::Int
    cols::Int
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

Base.IndexStyle(::Type{File}) = Base.IndexLinear()
Base.eltype(::File) = Row
Base.size(f::File) = (getrows(f),)

Tables.isrowtable(::Type{File}) = true
Tables.columnaccess(::Type{File}) = true
Tables.schema(f::File)  = Tables.Schema(getnames(f), gettypes(f))
Tables.columns(f::File) = f
Tables.columnnames(f::File) = getnames(f)
Base.propertynames(f::File) = getnames(f)

function Base.getproperty(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return haskey(lookup, col) ? lookup[col].column : getfield(f, col)
end

function Base.getindex(f::File, col::Symbol)
    lookup = getfield(f, :lookup)
    return haskey(lookup, col) ? lookup[col].column : getfield(f, col)
end

Base.getindex(f::File, col::String) = getindex(f, Symbol(col))

Tables.getcolumn(f::File, nm::Symbol) = getcolumn(f, nm).column
Tables.getcolumn(f::File, i::Int) = getcolumn(f, i).column

Base.@propagate_inbounds function Base.getindex(f::File, row::Int)
    @boundscheck checkbounds(f, row)
    return Row(getnames(f), getcolumns(f), getlookup(f), row)
end

function File(source::ValidSources;
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}}=1,
    normalizenames::Bool=false,
    # by default, data starts immediately after header or start of file
    datarow::Integer=-1,
    skipto::Integer=-1,
    footerskip::Integer=0,
    transpose::Bool=false,
    comment::Union{String, Nothing}=nothing,
    ignoreemptyrows::Bool=true,
    ignoreemptylines=nothing,
    select=nothing,
    drop=nothing,
    limit::Union{Integer, Nothing}=nothing,
    buffer_in_memory::Bool=false,
    threaded::Union{Bool, Nothing}=nothing,
    ntasks::Union{Nothing, Integer}=nothing,
    tasks::Union{Nothing, Integer}=nothing,
    rows_to_check::Integer=DEFAULT_ROWS_TO_CHECK,
    lines_to_check=nothing,
    # parsing options
    missingstrings=String[],
    missingstring="",
    delim::Union{Nothing, Char, String}=nothing,
    ignorerepeated::Bool=false,
    quoted::Bool=true,
    quotechar::Union{UInt8, Char}='"',
    openquotechar::Union{UInt8, Char, Nothing}=nothing,
    closequotechar::Union{UInt8, Char, Nothing}=nothing,
    escapechar::Union{UInt8, Char}='"',
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
    dateformats=nothing,
    decimal::Union{UInt8, Char}=UInt8('.'),
    groupmark::Union{Char, Nothing}=nothing,
    truestrings::Union{Vector{String}, Nothing}=TRUE_STRINGS,
    falsestrings::Union{Vector{String}, Nothing}=FALSE_STRINGS,
    stripwhitespace::Bool=false,
    # type options
    type=nothing,
    types=nothing,
    typemap::AbstractDict=IdDict{Type, Type}(),
    pool=DEFAULT_POOL,
    downcast::Bool=false,
    lazystrings::Bool=false,
    stringtype::StringTypes=DEFAULT_STRINGTYPE,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=DEFAULT_MAX_WARNINGS,
    debug::Bool=false,
    parsingdebug::Bool=false,
    validate::Bool=true,
    )
    # header=1;normalizenames=false;datarow=-1;skipto=-1;footerskip=0;transpose=false;comment=nothing;ignoreemptyrows=true;ignoreemptylines=nothing;
    # select=nothing;drop=nothing;limit=nothing;threaded=nothing;ntasks=Threads.nthreads();tasks=nothing;rows_to_check=30;lines_to_check=nothing;missingstrings=String[];missingstring="";
    # delim=nothing;ignorerepeated=false;quoted=true;quotechar='"';openquotechar=nothing;closequotechar=nothing;escapechar='"';dateformat=nothing;
    # dateformats=nothing;decimal=UInt8('.');groupmark=nothing;truestrings=nothing;falsestrings=nothing;type=nothing;types=nothing;typemap=IdDict{Type,Type}();
    # pool=CSV.DEFAULT_POOL;downcast=false;lazystrings=false;stringtype=String;strict=false;silencewarnings=false;maxwarnings=100;debug=false;parsingdebug=false;buffer_in_memory=false
    # @descend CSV.Context(CSV.Arg(source), CSV.Arg(header), CSV.Arg(normalizenames), CSV.Arg(datarow), CSV.Arg(skipto), CSV.Arg(footerskip), CSV.Arg(transpose), CSV.Arg(comment), CSV.Arg(ignoreemptyrows), CSV.Arg(ignoreemptylines), CSV.Arg(select), CSV.Arg(drop), CSV.Arg(limit), CSV.Arg(buffer_in_memory), CSV.Arg(threaded), CSV.Arg(ntasks), CSV.Arg(tasks), CSV.Arg(rows_to_check), CSV.Arg(lines_to_check), CSV.Arg(missingstrings), CSV.Arg(missingstring), CSV.Arg(delim), CSV.Arg(ignorerepeated), CSV.Arg(quoted), CSV.Arg(quotechar), CSV.Arg(openquotechar), CSV.Arg(closequotechar), CSV.Arg(escapechar), CSV.Arg(dateformat), CSV.Arg(dateformats), CSV.Arg(decimal), CSV.Arg(truestrings), CSV.Arg(falsestrings), CSV.Arg(type), CSV.Arg(types), CSV.Arg(typemap), CSV.Arg(pool), CSV.Arg(downcast), CSV.Arg(lazystrings), CSV.Arg(stringtype), CSV.Arg(strict), CSV.Arg(silencewarnings), CSV.Arg(maxwarnings), CSV.Arg(debug), CSV.Arg(parsingdebug), CSV.Arg(false))
    ctx = @refargs Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptyrows, ignoreemptylines, select, drop, limit, buffer_in_memory, threaded, ntasks, tasks, rows_to_check, lines_to_check, missingstrings, missingstring, delim, ignorerepeated, quoted, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, groupmark, truestrings, falsestrings, stripwhitespace, type, types, typemap, pool, downcast, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, validate, false)
    return File(ctx)
end

function File(ctx::Context, @nospecialize(chunking::Bool=false))
    @inbounds begin
    # we now do our parsing pass over the file, starting at datapos
    if ctx.threaded
        # multithreaded parsing
        rowsguess, ntasks, columns = ctx.rowsguess, ctx.ntasks, ctx.columns
        # calculate our guess for how many rows will be parsed by each concurrent parsing task
        rowchunkguess = cld(rowsguess, ntasks)
        wholecolumnslock = ReentrantLock() # in case columns are widened during parsing
        pertaskcolumns = Vector{Vector{Column}}(undef, ntasks)
        # initialize each top-level column's lock; used after a task is done parsing its chunk of rows
        # and it "checks in" the types it parsed for each column
        foreach(col -> col.lock = ReentrantLock(), columns)
        rows = zeros(Int, ntasks) # how many rows each parsing task ended up actually parsing
        @sync for i = 1:ntasks
            @wkspawn multithreadparse($ctx, $pertaskcolumns, $rowchunkguess, $i, $rows, $wholecolumnslock)
            # CSV.multithreadparse(ctx, pertaskcolumns, rowchunkguess, i, rows, wholecolumnslock)
        end
        finalrows = sum(rows)
        if ctx.limit < finalrows
            finalrows = ctx.limit
            # adjust columns according to limit
            acc = 0
            for i = 1:ntasks
                if acc + rows[i] > finalrows
                    # need to resize this tasks columns down
                    if finalrows - acc > 0
                        for col in pertaskcolumns[i]
                            if isdefined(col, :column)
                                resize!(col.column, finalrows - acc)
                            end
                        end
                    else
                        for col in pertaskcolumns[i]
                            if isdefined(col, :column)
                                empty!(col.column)
                            end
                        end
                    end
                end
                acc += rows[i]
            end
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
                    push!(task_columns, Column(Missing, ctx.options))
                end
            end
        end
        @sync for (j, col) in enumerate(columns)
            @wkspawn multithreadpostparse($ctx, $ntasks, $pertaskcolumns, $rows, $finalrows, $j, $col)
        end
    else
        # single-threaded parsing
        columns = ctx.columns
        allocate!(columns, ctx.rowsguess)
        t = Base.time()
        finalrows, pos = parsefilechunk!(ctx, ctx.datapos, ctx.len, ctx.rowsguess, 0, columns, ctx.customtypes)::Tuple{Int, Int}
        ctx.debug && println("time for initial parsing: $(Base.time() - t)")
        # cleanup our columns if needed
        for col in columns
@label processcolumn
            if col.type === NeedsTypeDetection
                # fill in uninitialized column fields
                col.type = Missing
                col.column = MissingVector(finalrows)
                col.pool = 0.0
            end
            T = col.anymissing ? Union{col.type, Missing} : col.type
            if maybepooled(col) &&
                (col.type isa StringTypes || col.columnspecificpool) &&
                checkpooled!(T, nothing, col, 0, 1, finalrows, ctx)
                # col.column is a PooledArray
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
    # check if a temp file was generated for parsing
    if !chunking && ctx.tempfile !== nothing && ctx.stringtype !== PosLenString
        rm(ctx.tempfile; force=true)
    end
    end # @inbounds begin
    return File(ctx.name, names, types, finalrows, length(columns), columns, lookup)
end

function multithreadparse(ctx, pertaskcolumns, rowchunkguess, i, rows, wholecolumnslock)
    columns = ctx.columns
    tt = Base.time()
    task_columns = [Column(col) for col in columns] # task-local columns derived from top-level columns
    allocate!(task_columns, rowchunkguess)
    pertaskcolumns[i] = task_columns
    task_pos = ctx.chunkpositions[i]
    task_len = ctx.chunkpositions[i + 1] - (i != ctx.ntasks)
    # for error-reporting purposes, we want to try and give the best guess of where a row emits a warning/error, so compute that
    rowchunkoffset = (ctx.datarow - 1) + (rowchunkguess * (i - 1))
    task_rows, task_pos = parsefilechunk!(ctx, task_pos, task_len, rowchunkguess, rowchunkoffset, task_columns, ctx.customtypes)::Tuple{Int, Int}
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
    return
end

function multithreadpostparse(ctx, ntasks, pertaskcolumns, rows, finalrows, j, col)
    # first check if need to re-parse any chunks
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
            promotetostring!(ctx, ctx.buf, task_pos, task_len, task_rows, sum(rows[1:i-1]), task_columns, ctx.customtypes, j, Ref(0), task_rows, T)
            col.type = something(promote_types(T, task_columns[j].type), ctx.stringtype)
            T = col.type
        end
    end
    for i = 1:ntasks
        task_columns = pertaskcolumns[i]
        task_col = task_columns[j]
        task_rows = rows[i]
        # check if we need to promote a task-local column based on what other threads parsed
        T = col.type # final promoted type from amongst all separate parsing tasks
        T2 = task_col.type
        if T === Float64 && T2 <: Integer
            # one chunk parsed as Int, another as Float64, promote to Float64
            ctx.debug && println("multithreaded promoting column $j to float")
            task_col.column = convert(SentinelVector{Float64}, task_col.column)
        elseif T !== T2 && (T <: InlineString || (T === String && T2 <: InlineString))
            # promote to widest InlineString type
            task_col.column = convert(SentinelVector{T}, task_col.column)
        elseif T !== T2
            # one chunk parsed all missing values, but another chunk had a typed value, promote to that
            # while keeping all values `missing` (allocate by default ensures columns have all missing values)
            ctx.debug && println("multithreaded promoting column $j from missing on task $i")
            task_col.column = allocate(T, task_rows)
        end
    end
    T = col.anymissing ? Union{col.type, Missing} : col.type
    if maybepooled(col) &&
        (col.type isa StringTypes || col.columnspecificpool) &&
        checkpooled!(T, pertaskcolumns, col, j, ntasks, finalrows, ctx)
        # col.column is a PooledArray
    elseif col.type === Int64
        # we need to special-case Int here because while parsing, a default Int64 sentinel value is chosen to
        # represent missing; if any chunk bumped into that sentinel value while parsing, then it cycled to a
        # new sentinel value; this step ensures that each chunk has the same encoded sentinel value
        # passing force=false means it will first check if all chunks already have the same sentinel and return
        # immediately if so, which will be the case most often
        SentinelArrays.newsentinel!((pertaskcolumns[i][j].column::SVec{Int64} for i = 1:ntasks)...; force=false)
        makechain!(col.type, pertaskcolumns, col, j, ntasks)
    elseif col.type === PosLenString
        col.column = ChainedVector(PosLenStringVector{coltype(col)}[makeposlen!(pertaskcolumns[i][j], coltype(col), ctx) for i = 1:ntasks])
    elseif col.type === NeedsTypeDetection || col.type === HardMissing
        col.type = Missing
        col.column = MissingVector(finalrows)
    else
        makechain!(col.type, pertaskcolumns, col, j, ntasks)
    end
    if finalrows < length(col.column)
        # we only ever resize! down here, so no need to use reallocate!
        resize!(col.column, finalrows)
    end
    return
end

function makechain!(::Type{T}, pertaskcolumns, col, j, ntasks) where {T}
    if col.anymissing
        col.column = ChainedVector([pertaskcolumns[i][j].column for i = 1:ntasks])
    else
        if col.type === Bool
            col.column = ChainedVector([convert(Vector{Bool}, pertaskcolumns[i][j].column::vectype(T)) for i = 1:ntasks])
        elseif col.type !== Union{} && col.type <: SmallIntegers
            col.column = ChainedVector([convert(Vector{col.type}, pertaskcolumns[i][j].column::vectype(T)) for i = 1:ntasks])
        else
            col.column = ChainedVector([parent(pertaskcolumns[i][j].column) for i = 1:ntasks])
        end
    end
    return
end

# T is Union{T, Missing} or T depending on col.anymissing
function checkpooled!(::Type{T}, pertaskcolumns, col, j, ntasks, nrows, ctx) where {T}
    S = Base.nonmissingtype(T)
    pool = Dict{T, UInt32}()
    lastref = Ref{UInt32}(0)
    refs = Vector{UInt32}(undef, nrows)
    k = 1
    limit = col.pool isa Tuple ? col.pool[2] : typemax(Int)
    for i = 1:ntasks
        column = (pertaskcolumns === nothing ? col.column : pertaskcolumns[i][j].column)::columntype(S)
        for x in column
            if x isa PosLen
                if x.missingvalue
                    refs[k] = get!(pool, missing) do
                        lastref[] += UInt32(1)
                    end
                elseif x.escapedvalue
                    val = S === PosLenString ? S(ctx.buf, x, ctx.options.e) : Parsers.getstring(ctx.buf, x, ctx.options.e)
                    refs[k] = get!(pool, val) do
                        lastref[] += UInt32(1)
                    end
                else
                    val = PointerString(pointer(ctx.buf, x.pos), x.len)
                    index = Base.ht_keyindex2!(pool, val)
                    if index > 0
                        found_key = pool.vals[index]
                        ref = found_key::UInt32
                    else
                        new = lastref[] += UInt32(1)
                        if S === PosLenString
                            Base._setindex!(pool, new, S(ctx.buf, x, ctx.options.e), -index)
                        else
                            Base._setindex!(pool, new, S(val), -index)
                        end
                        ref = new
                    end
                    refs[k] = ref
                end
            else
                refs[k] = get!(pool, x) do
                    lastref[] += UInt32(1)
                end
            end
            k += 1
            if length(pool) > limit
                return false
            end
        end
    end
    cpool = col.pool
    percent = cpool isa Tuple ? cpool[1] : cpool
    if ((length(pool) - 1) / nrows) <= percent
        col.column = PooledArray(PooledArrays.RefArray(refs), pool)
        return true
    else
        return false
    end
end

function makeposlen!(col, T, ctx)
    col.column = PosLenStringVector{T}(ctx.buf, col.column::Vector{PosLen}, ctx.options.e)
    return col.column
end

function parsefilechunk!(ctx::Context, pos, len, rowsguess, rowoffset, columns, ::Type{customtypes})::Tuple{Int, Int} where {customtypes}
    buf = ctx.buf
    transpose = ctx.transpose
    limit = ctx.limit
    row = 0
    startpos = pos
    if pos <= len && len > 0 && row < limit
        numwarnings = Ref(0)
        while true
            row += 1
            # @show columns
            @inbounds pos = parserow(startpos, row, numwarnings, ctx, buf, pos, len, rowsguess, rowoffset, columns, customtypes)::Int
            # @show columns
            row == limit && break
            (transpose ? all(c -> c.position >= c.endposition, columns) : pos > len) && break
            # if our initial row estimate was too few, we need to reallocate our columns to read the rest of the file/chunk
            if !transpose && row + 1 > rowsguess
                # (bytes left in file/chunk) / (avg bytes per row) == estimated rows left in file (+ 5% to try and avoid reallocating)
                estimated_rows_left = ceil(Int, ((len - pos) / ((pos - startpos) / row)) * 1.05)
                newrowsguess = rowsguess + estimated_rows_left
                newrowsguess = max(rowsguess + 1, newrowsguess)
                ctx.debug && reallocatecolumns(rowoffset + row, rowsguess, newrowsguess)
                for col in columns
                    isdefined(col, :column) && reallocate!(col.column, newrowsguess)
                end
                rowsguess = newrowsguess
            end
        end
        if !ctx.threaded && ctx.ntasks > 1 && !ctx.silencewarnings
            # !ctx.threaded && ctx.ntasks > 1 indicate that multithreaded parsing failed.
            # These messages echo the corresponding debug statement in the definition of ctx
            if numwarnings[] > 0
                @warn "Multithreaded parsing failed and fell back to single-threaded parsing, check previous warnings for possible reasons."
            else
                @error "Multithreaded parsing failed and fell back to single-threaded parsing. This can happen if the input contains multi-line fields; otherwise, please report this issue."
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
@noinline toomanycolumns(cols, row) = @warn("thread = $(Threads.threadid()) warning: parsed expected $cols columns, but didn't reach end of line around data row: $row. Parsing extra columns and widening final columnset")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = @warn("thread = $(Threads.threadid()) warning: error parsing $T around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) fatal error, encountered an invalidly quoted field while parsing around row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))
@noinline toomanywwarnings() = @warn("thread = $(Threads.threadid()): too many warnings, silencing any further warnings")

Base.@propagate_inbounds function parserow(startpos, row, numwarnings, ctx::Context, buf, pos, len, rowsguess, rowoffset, columns, ::Type{customtypes})::Int where {customtypes}
    # @show columns
    ncols = length(columns)
    for i = 1:ncols
        col = columns[i]
        if ctx.transpose
            pos = col.position
        end
        type = col.type
        cellstartpos = pos
        if type === HardMissing
            pos, code = parsevalue!(Missing, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === NeedsTypeDetection
            pos, code = detectcell(buf, pos, len, row, rowoffset, i, col, ctx, rowsguess)
        elseif type === Int8
            pos, code = parsevalue!(Int8, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Int16
            pos, code = parsevalue!(Int16, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Int32
            pos, code = parsevalue!(Int32, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Int64
            pos, code = parsevalue!(Int64, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Int128
            pos, code = parsevalue!(Int128, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Float16
            pos, code = parsevalue!(Float16, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Float32
            pos, code = parsevalue!(Float32, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Float64
            pos, code = parsevalue!(Float64, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString1
            pos, code = parsevalue!(InlineString1, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString3
            pos, code = parsevalue!(InlineString3, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString7
            pos, code = parsevalue!(InlineString7, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString15
            pos, code = parsevalue!(InlineString15, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString31
            pos, code = parsevalue!(InlineString31, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString63
            pos, code = parsevalue!(InlineString63, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString127
            pos, code = parsevalue!(InlineString127, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === InlineString255
            pos, code = parsevalue!(InlineString255, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === String
            pos, code = parsevalue!(String, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === PosLenString
            pos, code = parsevalue!(PosLenString, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Date
            pos, code = parsevalue!(Date, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === DateTime
            pos, code = parsevalue!(DateTime, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Time
            pos, code = parsevalue!(Time, buf, pos, len, row, rowoffset, i, col, ctx)
        elseif type === Bool
            pos, code = parsevalue!(Bool, buf, pos, len, row, rowoffset, i, col, ctx)
        else
            if customtypes !== Tuple{}
                pos, code = parsecustom!(customtypes, buf, pos, len, row, rowoffset, i, col, ctx)
            else
                error("Column $i bad column type: `$(type)`")
            end
        end
        if promote_to_string(code)
            ctx.debug && println("promoting column i = $i to string from $(type) on chunk = $(Threads.threadid())")
            if type <: InlineString
                newT = String
            elseif ctx.stringtype === InlineString
                str = Parsers.xparse(String, buf, cellstartpos, len, col.options)
                newT = pickstringtype(InlineString, str.val.len)
            else
                newT = ctx.stringtype
            end
            promotetostring!(ctx, buf, startpos, len, rowsguess, rowoffset, columns, customtypes, i, numwarnings, row, newT)
        end
        if ctx.transpose
            col.position = pos
        else
            if i < ncols
                if Parsers.newline(code) || pos > len
                    # in https://github.com/JuliaData/CSV.jl/issues/948,
                    # it was noticed that if we reached the EOF right before parsing
                    # the last expected column, then the warning is a bit spurious.
                    # The final value is `missing` and the csv writer chose to just
                    # "close" the file w/o including a final newline
                    # we can treat this special-case as "valid" and not emit a warning
                    if !(pos > len && i == (ncols - 1))
                        ctx.silencewarnings || numwarnings[] > ctx.maxwarnings || notenoughcolumns(i, ncols, rowoffset + row)
                        !ctx.silencewarnings && numwarnings[] == ctx.maxwarnings && toomanywwarnings()
                        numwarnings[] += 1
                    end
                    for j = (i + 1):ncols
                        columns[j].anymissing = true
                    end
                    break # from for i = 1:ncols
                end
            elseif pos <= len && !Parsers.newline(code)
                # extra columns on this row, let's widen
                ctx.silencewarnings || toomanycolumns(ncols, rowoffset + row)
                j = i + 1
                while pos <= len && !Parsers.newline(code)
                    col = initialize_column(j, ctx)
                    col.anymissing = ctx.streaming || rowoffset == 0 && row > 1 # assume all previous rows were missing
                    col.pool = ctx.pool
                    T = col.type
                    # TODO: Support edge case where a custom type was provided for the new column?
                    # Right now if `T` is a `nonstandardtype` not already in `customtypes`, then
                    # we won't have a specialised parse method for it, so parsing is expected to fail.
                    # Only log the error, rather than throw, in case parsing somehow works.
                    nonstandardtype(T) === Union{} || T in ctx.customtypes.parameters || @error "Parsing extra column with unknown type `$T`. Parsing may fail!"
                    if T === NeedsTypeDetection
                        pos, code = detectcell(buf, pos, len, row, rowoffset, j, col, ctx, rowsguess)
                    else
                        # need to allocate
                        col.column = allocate(T, ctx.rowsguess)
                        pos, code = parsevalue!(T, buf, pos, len, row, rowoffset, j, col, ctx)
                    end
                    j += 1
                    push!(columns, col)
                end
            end
        end
    end
    return pos
end

function detectcell(buf, pos, len, row, rowoffset, i, col, ctx, rowsguess)::Tuple{Int, Int16}
    # debug && println("detecting on task $(Threads.threadid())")
    opts = col.options
    code, tlen, x, xT = detect(pass, buf, pos, len, opts, false, ctx.downcast, rowoffset + row, i)
    if x === missing
        col.anymissing = true
        @goto finaldone
    end
    newT = ctx.stringtype
    if x !== nothing
        # we found a non-missing value
        newT = get(ctx.typemap, typeof(x), typeof(x))
        if !(newT isa StringTypes)
            if newT !== typeof(x)
                # type-mapping typeof(x) => newT
                # this ultimate call to Parsers.xparse has no hope in inference (because of the typeof(x) => newT mapping)
                # so we "outline" the call and assert the types of everything but `y` to make sure `code` and `tlen` stay type stable
                res = _parseany(newT, buf, pos, len, opts)
                code, tlen = res.code, res.tlen
                if Parsers.ok(code)
                    val = res.val
                    @goto done
                end
            else
                val = x
                @goto done
            end
        end
    end
    # if we "fall through" to here, that means we either detected a string value
    # or we're type-mapping from another detected type to string
    str = Parsers.xparse(String, buf, pos, len, opts)
    poslen = str.val
    if newT === InlineString && poslen.len < DEFAULT_MAX_INLINE_STRING_LENGTH
        newT = InlineStringType(poslen.len)
        val = newT(PosLenString(buf, poslen, opts.e))
    elseif newT === PosLenString
        newT = PosLenString
        val = poslen
    else
        newT = String
        val = Parsers.getstring(buf, poslen, opts.e)
    end
@label done
    # if we're here, that means we found a non-missing value, so we need to update column
    column = allocate(newT, rowsguess)
    column[row] = val
    col.column = column
    col.type = newT
@label finaldone
    return pos + tlen, code
end

function parsevalue!(::Type{type}, buf, pos, len, row, rowoffset, i, col, ctx)::Tuple{Int, Int16} where {type}
    opts = col.options
    res = Parsers.xparse(type === Missing ? String : type, buf, pos, len, opts)
    code = res.code
    if !Parsers.invalid(code)
        if type !== Missing
            if Parsers.sentinel(code)
                col.anymissing = true
            else
                column = col.column
                val = res.val
                if column isa Vector{PosLen} && val isa PosLen
                    @inbounds (column::Vector{PosLen})[row] = val
                elseif type === String
                    @inbounds (column::SVec2{String})[row] = Parsers.getstring(buf, val, opts.e)
                else
                    @inbounds (column::vectype(type))[row] = val
                end
            end
        end
    else
        # something went wrong parsing
        if Parsers.invalidquotedfield(code)
            # this usually means parsing is borked because of an invalidly quoted field, hard error
            fatalerror(buf, pos, res.tlen, code, rowoffset + row, i)
        end
        if type !== Missing && type !== PosLenString && type !== String
            if col.userprovidedtype
                if !ctx.strict
                    ctx.silencewarnings || warning(type, buf, pos, res.tlen, code, rowoffset + row, i)
                    col.anymissing = true
                else
                    stricterror(type, buf, pos, res.tlen, code, rowoffset + row, i)
                end
            else
                if type === Int8 || type === Int16 || type === Int32 || type === Int64 || type === Int128
                    newT = _widen(type)
                    while newT !== nothing && !Parsers.ok(code)
                        newT = get(ctx.typemap, newT, newT)
                        if newT isa StringTypes
                            code |= PROMOTE_TO_STRING
                            break
                        end
                        code = trytopromote!(type, newT, buf, pos, len, col, row)
                        newT = _widen(newT)
                    end
                elseif type === InlineString1 || type === InlineString3 || type === InlineString7 || type === InlineString15
                    newT = widen(type)
                    while newT !== InlineString63
                        ret = _parseany(newT, buf, pos, len, opts)
                        if !Parsers.invalid(ret.code)
                            col.type = newT
                            column = col.column
                            col.column = convert(SentinelVector{newT}, col.column::vectype(type))
                            @inbounds col.column[row] = ret.val
                            return pos + ret.tlen, ret.code
                        end
                        newT = widen(newT)
                    end
                    #TODO: should we just convert(SentinelVector{String}) here?
                    code |= PROMOTE_TO_STRING
                else
                    code |= PROMOTE_TO_STRING
                end
            end
        end
    end
    return pos + res.tlen, code
end

@noinline function trytopromote!(::Type{from}, ::Type{to}, buf, pos, len, col, row)::Int16 where {from, to}
    res = Parsers.xparse(to, buf, pos, len, col.options)
    code = res.code
    if !Parsers.invalid(code)
        col.type = to
        column = col.column
        if column isa vectype(from)
            col.column = convert(promotevectype(to), column)
            @inbounds col.column[row] = res.val
        end
    else
        code |= PROMOTE_TO_STRING
    end
    return code
end

@inline function parsecustom!(::Type{customtypes}, buf, pos, len, row, rowoffset, i, col, ctx) where {customtypes}
    if @generated
        block = Expr(:block)
        push!(block.args, quote
            error("CSV.jl code-generation error, unexpected column type: $(type)")
        end)
        for i = 1:fieldcount(customtypes)
            T = fieldtype(customtypes, i)
            pushfirst!(block.args, quote
                if type === $T
                    return parsevalue!($T, buf, pos, len, row, rowoffset, i, col, ctx)
                end
            end)
        end
        pushfirst!(block.args, :(type = col.type))
        pushfirst!(block.args, Expr(:meta, :inline))
        # @show block
        return block
    else
        # println("generated function failed")
        return parsevalue!(col.type, buf, pos, len, row, rowoffset, i, col, ctx)
    end
end

@noinline function promotetostring!(ctx::Context, buf, pos, len, rowsguess, rowoffset, columns, ::Type{customtypes}, column_to_promote, numwarnings, limit, stringtype) where {customtypes}
    cols = [i == column_to_promote ? columns[i] : Column(Missing, columns[i].options) for i = 1:length(columns)]
    col = cols[column_to_promote]
    col.column = allocate(stringtype, rowsguess)
    col.type = stringtype
    row = 0
    startpos = pos
    if pos <= len && len > 0
        while row < limit
            row += 1
            @inbounds pos = parserow(startpos, row, numwarnings, ctx, buf, pos, len, rowsguess, rowoffset, cols, customtypes)
            pos > len && break
        end
    end
    return
end

function File(sources::Vector;
    source::Union{Nothing, Symbol, AbstractString,
        Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing,
    kw...)
    isempty(sources) && throw(ArgumentError("unable to read delimited data from empty sources array"))
    if source isa Pair
        length(source.second) == length(sources) || throw(ArgumentError("source pair keyword argument list ($(length(source.second))) must match length of input vector ($(length(sources)))"))
    end
    length(sources) == 1 && return File(sources[1]; kw...)
    all(x -> x isa ValidSources, sources) || throw(ArgumentError("all provided sources must be one of: `$ValidSources`"))
    kws = merge(values(kw), (ntasks=1,))
    f = File(sources[1]; kws...)
    rows = getrows(f)
    for col in getcolumns(f)
        col.column = ChainedVector([col.column])
    end
    files = Vector{File}(undef, length(sources) - 1)
    @sync for i = 2:length(sources)
        Threads.@spawn begin
            files[i - 1] = File(sources[i]; kws...)
        end
    end
    lookup = getlookup(f)
    for i = 2:length(sources)
        f2 = files[i - 1]
        rows += getrows(f2)
        fl2 = getlookup(f2)
        for (nm, col) in lookup
            if haskey(fl2, nm)
                col.column = chaincolumns!(col.column, fl2[nm].column)
            else
                col.column = chaincolumns!(col.column, MissingVector(getrows(f2)))
            end
        end
    end
    if source !== nothing
        # add file name of each "partition" as 1st column
        pushfirst!(files, f)
        vals = source isa Pair ? source.second : [getname(f) for f in files]
        pool = Dict(x => UInt32(i) for (i, x) in enumerate(vals))
        arr = PooledArray(PooledArrays.RefArray(ChainedVector([fill(UInt32(i), getrows(f)) for (i, f) in enumerate(files)])), pool)
        col = Column(eltype(arr))
        col.column = arr
        push!(getcolumns(f), col)
        colnm = Symbol(source isa Pair ? source.first : source)
        push!(getnames(f), colnm)
        push!(gettypes(f), eltype(arr))
        getlookup(f)[colnm] = col
    end
    return File(getname(f), getnames(f), gettypes(f), rows, getcols(f), getcolumns(f), getlookup(f))
end
