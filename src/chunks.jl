struct Chunks{H}
    h::H # Header
    threaded::Union{Bool, Nothing}
    typemap::Dict{Type, Type}
    tasks::Int
    debug::Bool
    ranges::Vector{Int64}
end

"""
    CSV.Chunks(source; tasks::Integer=Threads.nthreads(), kwargs...) => CSV.Chunks

Returns a file "chunk" iterator. Accepts all the same inputs and keyword arguments as [`CSV.File`](@ref),
see those docs for explanations of each keyword argument.

The `tasks` keyword argument specifies how many chunks a file should be split up into, defaulting to 
the # of threads available to Julia (i.e. `JULIA_NUM_THREADS` environment variable) or 8 if Julia is
run single-threaded.

Each iteration of `CSV.Chunks` produces the next chunk of a file as a `CSV.File`. While initial file
metadata detection is done only once (to determine # of columns, column names, etc), each iteration
does independent type inference on columns. This is significant as different chunks may end up with
different column types than previous chunks as new values are encountered in the file. Note that, as
with `CSV.File`, types may be passed manually via the `type` or `types` keyword arguments.

This functionality is new and thus considered experimental; please
[open an issue](https://github.com/JuliaData/CSV.jl/issues/new) if you run into any problems/bugs.
"""
function Chunks(source;
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
    threaded::Union{Bool, Nothing}=nothing,
    tasks::Integer=Threads.nthreads(),
    lines_to_check::Integer=5,
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
    pool::Union{Bool, Real}=0.1,
    lazystrings::Bool=false,
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, use_mmap, ignoreemptylines, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, pool, lazystrings, strict, silencewarnings, debug, parsingdebug, false)
    rowsguess, ncols, buf, len, datapos, options = h.rowsguess, h.cols, h.buf, h.len, h.datapos, h.options
    N = tasks > rowsguess || rowsguess < 100 ? 1 : tasks == 1 ? 8 : tasks
    chunksize = div(len - datapos, N)
    ranges = Int64[i == 0 ? datapos : i == N ? len : (datapos + chunksize * i) for i = 0:N]
    findrowstarts!(buf, len, options, ranges, ncols, h.types, h.flags, lines_to_check)
    return Chunks(h, threaded, typemap, tasks, debug, ranges)
end

function Base.iterate(x::Chunks, i=1)
    i >= length(x.ranges) && return nothing
    f = File(x.h; finalizebuffer=false, startingbyteposition=x.ranges[i], endingbyteposition=(x.ranges[i + 1] - (i != length(x.ranges))), threaded=x.threaded, typemap=x.typemap, tasks=x.tasks, debug=x.debug)
    return f, i + 1
end

Tables.partitions(x::Chunks) = x
