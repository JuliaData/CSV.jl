struct Chunks
    ctx::Context
    Chunks(ctx::Context) = new(ctx)
end

"""
    CSV.Chunks(source; ntasks::Integer=Threads.nthreads(), kwargs...) => CSV.Chunks

Returns a file "chunk" iterator. Accepts all the same inputs and keyword arguments as [`CSV.File`](@ref),
see those docs for explanations of each keyword argument.

The `ntasks` keyword argument specifies how many chunks a file should be split up into, defaulting to
the # of threads available to Julia (i.e. `JULIA_NUM_THREADS` environment variable) or 8 if Julia is
run single-threaded.

Each iteration of `CSV.Chunks` produces the next chunk of a file as a `CSV.File`. While initial file
metadata detection is done only once (to determine # of columns, column names, etc), each iteration
does independent type inference on columns. This is significant as different chunks may end up with
different column types than previous chunks as new values are encountered in the file. Note that, as
with `CSV.File`, types may be passed manually via the `type` or `types` keyword arguments.

This functionality is new and thus considered experimental; please
[open an issue](https://github.com/JuliaData/CSV.jl/issues/new) if you run into any problems/bugs.

$KEYWORD_DOCS
"""
function Chunks(source::ValidSources;
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
    ntasks::Union{Integer, Nothing}=Threads.nthreads() == 1 ? 8 : Threads.nthreads(),
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
    pool::Union{Bool, Real, AbstractVector, AbstractDict, Base.Callable, Tuple}=DEFAULT_POOL,
    downcast::Bool=false,
    lazystrings::Bool=false,
    stringtype::StringTypes=DEFAULT_STRINGTYPE,
    strict::Bool=false,
    silencewarnings::Bool=false,
    maxwarnings::Int=DEFAULT_MAX_WARNINGS,
    debug::Bool=false,
    parsingdebug::Bool=false,
    validate=true,
    )

    ctx = @refargs Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptyrows, ignoreemptylines, select, drop, limit, buffer_in_memory, nothing, ntasks, tasks, rows_to_check, lines_to_check, missingstrings, missingstring, delim, ignorerepeated, quoted, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, groupmark, truestrings, falsestrings, stripwhitespace, type, types, typemap, pool, downcast, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, validate, false)
    !ctx.threaded && throw(ArgumentError("unable to iterate chunks from input file source"))
    foreach(col -> col.lock = ReentrantLock(), ctx.columns)
    return Chunks(ctx)
end

Base.IteratorSize(::Type{Chunks}) = Base.HasLength()
Base.length(x::Chunks) = x.ctx.ntasks
Base.eltype(x::Chunks) = File

function Base.iterate(x::Chunks, i=1)
    i > x.ctx.ntasks && return nothing
    names = copy(x.ctx.names)
    columns = [Column(col) for col in x.ctx.columns]
    datapos = x.ctx.chunkpositions[i]
    len = x.ctx.chunkpositions[i + 1] - 1
    rowsguess = cld(x.ctx.rowsguess, x.ctx.ntasks)
    threaded = false
    ntasks = 1
    limit = typemax(Int)
    ctx = Context(x.ctx.transpose, x.ctx.name, names, rowsguess, x.ctx.cols, x.ctx.buf, datapos, len, 1, x.ctx.options, columns, x.ctx.pool, x.ctx.downcast, x.ctx.customtypes, x.ctx.typemap, x.ctx.stringtype, limit, threaded, ntasks, x.ctx.chunkpositions, x.ctx.strict, x.ctx.silencewarnings, x.ctx.maxwarnings, x.ctx.debug, x.ctx.tempfile, x.ctx.streaming, x.ctx.types)
    f = File(ctx, true)
    return f, i + 1
end

Tables.partitions(x::Chunks) = x
