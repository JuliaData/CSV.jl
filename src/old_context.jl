"""
Internal structure used to track information for a single column in a delimited file.

Fields:
  * `type`: always a single, concrete type; no Union{T, Missing}; missingness is tracked in anymissing field; this field is mutable; it may start as one type and get "promoted" to another while parsing; two special types exist: `NeedsTypeDetection`, which specifies that we need to try and detect what type this column's values are and `HardMissing` which means the column type is definitely `Missing` and we don't need to detect anything; to get the "final type" of a column after parsing, call `CSV.coltype(col)`, which takes into account `anymissing`
  * `anymissing`: whether any missing values have been encountered while parsing; if a user provided a type like `Union{Int, Missing}`, we'll set this to `true`, or when `missing` values are encountered while parsing
  * `userprovidedtype`: whether the column type was provided by the user or not; this affects whether we'll promote a column's type while parsing, or emit a warning/error depending on `strict` keyword arg
  * `willdrop`: whether we'll drop this column from the final columnset; computed from select/drop keyword arguments; this will result in a column type of `HardMissing` while parsing, where an efficient parser is used to "skip" a field w/o allocating any parsed value
  * `pool`: computed from `pool` keyword argument; `true` is `1.0`, `false` is `0.0`, everything else is `Float64(pool)`; once computed, this field isn't mutated at all while parsing; it's used in type detection to determine whether a column will be pooled or not once a type is detected;
  * `columnspecificpool`: if `pool` was provided via Vector or Dict by user, then `true`, other `false`; if `false`, then only string column types will attempt pooling
  * `column`: the actual column vector to hold parsed values; field is typed as `AbstractVector` and while parsing, we do switches on `col.type` to assert the column type to make code concretely typed
  * `lock`: in multithreaded parsing, we have a top-level set of `Vector{Column}`, then each threaded parsing task makes its own copy to parse its own chunk; when synchronizing column types/pooled refs, the task-local `Column` will `lock(col.lock)` to make changes to the parent `Column`; each task-local `Column` shares the same `lock` of the top-level `Column`
  * `position`: for transposed reading, the current column position
  * `endposition`: for transposed reading, the expected ending position for this column
"""
mutable struct Column
    # fields that are copied per task when parsing
    type::Type
    anymissing::Bool
    userprovidedtype::Bool
    willdrop::Bool
    pool::Union{Float64, Tuple{Float64, Int}}
    columnspecificpool::Bool
    # lazily/manually initialized fields
    column::AbstractVector
    # per top-level column fields (don't need to copy per task when parsing)
    lock::ReentrantLock
    position::Int
    endposition::Int
    options::Parsers.Options

    Column(type::Type, anymissing::Bool, userprovidedtype::Bool, willdrop::Bool, pool::Union{Float64, Tuple{Float64, Int}}, columnspecificpool::Bool) =
        new(type, anymissing, userprovidedtype, willdrop, pool, columnspecificpool)
end

function Column(type::Type, options::Union{Parsers.Options, Nothing}=nothing)
    T = nonmissingtypeunlessmissingtype(type)
    col = Column(type === Missing ? HardMissing : T,
        type >: Missing,
        type !== NeedsTypeDetection,
        false, NaN, false)
    if options !== nothing
        col.options = options
    end
    return col
end

# creating a per-task column from top-level column
function Column(x::Column)
    @assert isdefined(x, :lock)
    y = Column(x.type, x.anymissing, x.userprovidedtype, x.willdrop, x.pool, x.columnspecificpool)
    y.lock = x.lock # parent and child columns _share_ the same lock
    if isdefined(x, :options)
        y.options = x.options
    end
    # specifically _don't_ copy/re-use x.column; that needs to be allocated fresh per parsing task
    return y
end

"""
    isvaliddelim(delim)

Whether a character or string is valid for use as a delimiter.
"""
isvaliddelim(delim) = false
isvaliddelim(delim::Char) = delim != '\r' && delim != '\n' && delim != '\0'
isvaliddelim(delim::AbstractString) = all(isvaliddelim, delim)

"""
    checkvaliddelim(delim)

Checks whether a character or string is valid for use as a delimiter.  If
`delim` is `nothing`, it is assumed that the delimiter will be auto-selected.
Throws an error if `delim` is invalid.
"""
function checkvaliddelim(delim)
    delim !== nothing && !isvaliddelim(delim) &&
        throw(ArgumentError("invalid delim argument = '$(escape_string(string(delim)))', "*
                            "the following delimiters are invalid: '\\r', '\\n', '\\0'"))
end

function checkinvalidcolumns(dict::AbstractDict, argname, ncols, names)
    for (k, _) in dict
        if k isa Integer
            (0 < k <= ncols) || throw(ArgumentError("invalid column number provided in `$argname` keyword argument: $k. Column number must be 0 < i <= $ncols as detected in the data. To ignore invalid columns numbers in `$argname`, pass `validate=false`"))
        else
            isvalid = (k isa Regex && any(nm -> contains(string(nm), k), names)) || Symbol(k) in names
            isvalid || throw(ArgumentError("invalid column name provided in `$argname` keyword argument: $k. Valid column names detected in the data are: $names. To ignore invalid columns names in `$argname`, pass `validate=false`"))
        end
    end
    return nothing
end
function checkinvalidcolumns(vec::AbstractVector, argname, ncols, names)
    # we generally expect `length(types) == ncols` but still want to support the case where
    # an additional column is found later in the file and e.g. has its type given in `types`
    length(vec) >= ncols || throw(ArgumentError("provided `$argname::AbstractVector` keyword argument doesn't match detected # of columns: `$(length(vec)) < $ncols`"))
    return nothing
end
# if the argument isn't given as an AbstractDict or an AbstractVector then
# we have no way to check it again the number of cols or the names
checkinvalidcolumns(arg::Any, argname, ncols, names) = nothing

@noinline nonconcretetypes(types) = throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))

# Create all the `Column`s and keep track of any non-standard eltypes for which we will
# later need generate specialized parsing methods.
# - `ncols` is the number of columns to create
# - `types` is the user-given input
function initialize_columns(ncols::Int, types, names, args...; validate)
    columns = Vector{Column}(undef, ncols)
    customtypes = Tuple{}
    validate && checkinvalidcolumns(types, "types", ncols, names)
    for i = 1:ncols
        col = initialize_column(i, types, names, args...)
        columns[i] = col
        if nonstandardtype(col.type) !== Union{}
            customtypes = tupcat(customtypes, nonstandardtype(col.type))
        end
    end
    return columns, customtypes
end

# Create a `Column` with their eltype set using any user-provided types,
# but without yet allocating a vector to hold the parsed results (see `allocate`)
# - `i` is the column number e.g. i=1 for the 1st column.
# - `types` is the user-given input
function initialize_column(i, types, names, stringtype, streaming::Bool, options)
    T = initial_column_type(i, types, names, stringtype, streaming::Bool)
    return Column(T, options)
end

function initial_column_type(i, types::AbstractVector, _, _, ::Bool)
    # we generally expected `length(types) == ncols` but we still want to support the case
    # where an additional column is found later in the file and wasn't in `types`
    i <= length(types) ? types[i] : NeedsTypeDetection
end

function initial_column_type(i, types::AbstractDict, names, stringtype, streaming::Bool)
    defaultT = streaming ? Union{stringtype, Missing} : NeedsTypeDetection
    # if an additional column is found while parsing, it will not have a name yet
    nm = i <= length(names) ? names[i] : ""
    getordefault(types, nm, i, defaultT)
end

function initial_column_type(i, types::Function, names, stringtype, streaming::Bool)
    defaultT = streaming ? Union{stringtype, Missing} : NeedsTypeDetection
    # if an additional column is found while parsing, it will not have a name yet
    nm = i <= length(names) ? names[i] : ""
    something(types(i, nm), defaultT)
end

function initial_column_type(_, ::Nothing, _, stringtype, streaming::Bool)
    streaming ? Union{stringtype, Missing} : NeedsTypeDetection
end

function initial_column_type(_, types::Type, _, _, ::Bool)
    types
end

function reinitialize_column_type!(columns, types, names, stringtype, streaming)
    for (i, col) in pairs(columns)
        col.type = initial_column_type(i, types, names, stringtype, streaming)
    end
end

mutable struct Context
    transpose::Bool
    name::String
    names::Vector{Symbol}
    rowsguess::Int
    cols::Int
    buf::Vector{UInt8}
    datapos::Int
    len::Int
    datarow::Int
    options::Parsers.Options
    columns::Vector{Column}
    pool::Union{Float64, Tuple{Float64, Int}}
    downcast::Bool
    customtypes::Type
    typemap::IdDict{Type, Type}
    stringtype::StringTypes
    limit::Int
    threaded::Bool
    ntasks::Int
    chunkpositions::Vector{Int}
    strict::Bool
    silencewarnings::Bool
    maxwarnings::Int
    debug::Bool
    tempfile::Union{String, Nothing}
    streaming::Bool
    types::Union{Nothing, Type, AbstractVector, AbstractDict, Function}
end

function initialize_column(i, ctx::Context)
    return initialize_column(i, ctx.types, ctx.names, ctx.stringtype, ctx.streaming, ctx.options)
end

# user-facing function if just the context is desired
function Context(source::ValidSources;
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
    return @refargs Context(source, header, normalizenames, datarow, skipto, footerskip, transpose, comment, ignoreemptyrows, ignoreemptylines, select, drop, limit, buffer_in_memory, threaded, ntasks, tasks, rows_to_check, lines_to_check, missingstrings, missingstring, delim, ignorerepeated, quoted, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, groupmark, truestrings, falsestrings, stripwhitespace, type, types, typemap, pool, downcast, lazystrings, stringtype, strict, silencewarnings, maxwarnings, debug, parsingdebug, validate, false)
end

@refargs function Context(source::ValidSources,
    # file options
    # header can be a row number, range of rows, or actual string vector
    header::Union{Integer, Vector{Symbol}, Vector{String}, AbstractVector{<:Integer}},
    normalizenames::Bool,
    datarow::Integer,
    skipto::Integer,
    footerskip::Integer,
    transpose::Bool,
    comment::Union{String, Nothing},
    ignoreemptyrows::Bool,
    ignoreemptylines::Union{Nothing, Bool},
    select,
    drop,
    limit::Union{Integer, Nothing},
    buffer_in_memory::Bool,
    threaded::Union{Nothing, Bool},
    ntasks::Union{Nothing, Integer},
    tasks::Union{Nothing, Integer},
    rows_to_check::Integer,
    lines_to_check::Union{Nothing, Integer},
    # parsing options
    missingstrings::Union{Nothing, String, Vector{String}},
    missingstring::Union{Nothing, String, Vector{String}},
    delim::Union{Nothing, UInt8, Char, String},
    ignorerepeated::Bool,
    quoted::Bool,
    quotechar::Union{UInt8, Char},
    openquotechar::Union{Nothing, UInt8, Char},
    closequotechar::Union{Nothing, UInt8, Char},
    escapechar::Union{UInt8, Char},
    dateformat::Union{Nothing, String, Dates.DateFormat, Parsers.Format, AbstractVector, AbstractDict},
    dateformats::Union{Nothing, String, Dates.DateFormat, Parsers.Format, AbstractVector, AbstractDict},
    decimal::Union{UInt8, Char},
    groupmark::Union{Char, Nothing},
    truestrings::Union{Nothing, Vector{String}},
    falsestrings::Union{Nothing, Vector{String}},
    stripwhitespace::Bool,
    # type options
    type::Union{Nothing, Type},
    types::Union{Nothing, Type, AbstractVector, AbstractDict, Function},
    typemap::AbstractDict,
    pool::Union{Bool, Real, AbstractVector, AbstractDict, Base.Callable, Tuple},
    downcast::Bool,
    lazystrings::Bool,
    stringtype::StringTypes,
    strict::Bool,
    silencewarnings::Bool,
    maxwarnings::Integer,
    debug::Bool,
    parsingdebug::Bool,
    validate::Bool,
    streaming::Bool)

    # initial argument validation and adjustment
    @inbounds begin
    ((source isa AbstractString || source isa AbstractPath) && !isfile(source)::Bool) && throw(ArgumentError("\"$source\" is not a valid file or doesn't exist"))
    if types !== nothing
        if types isa AbstractVector
            any(x->!concrete_or_concreteunion(x), types) && nonconcretetypes(types)
        elseif types isa AbstractDict
            typs = values(types)
            any(x->!concrete_or_concreteunion(x), typs) && nonconcretetypes(typs)
        elseif types isa Type
            concrete_or_concreteunion(types) || nonconcretetypes(types)
        end
    end
    checkvaliddelim(delim)
    ignorerepeated && delim === nothing && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter like `delim=','`"))
    if lazystrings && !streaming
        @warn "`lazystrings` keyword argument is deprecated; use `stringtype=PosLenString` instead"
        stringtype = PosLenString
    end
    if tasks !== nothing
        @warn "`tasks` keyword argument is deprecated; use `ntasks` instead"
        ntasks = tasks
    end
    if ignoreemptylines !== nothing
        @warn "`ignoreemptylines` keyword argument is deprecated; use `ignoreemptyrows` instead"
        ignoreemptyrows = ignoreemptylines
    end
    if lines_to_check !== nothing
        @warn "`lines_to_check` keyword argument is deprecated; use `rows_to_check` instead"
        rows_to_check = lines_to_check
    end
    if !isempty(missingstrings)
        @warn "`missingstrings` keyword argument is deprecated; pass a `Vector{String}` to `missingstring` instead"
        missingstring = missingstrings
    end
    if dateformats !== nothing
        @warn "`dateformats` keyword argument is deprecated; pass column date formats to `dateformat` keyword argument instead"
        dateformat = dateformats
    end
    if datarow != -1
        @warn "`datarow` keyword argument is deprecated; use `skipto` instead"
        skipto = datarow
    end
    if type !== nothing
        @warn "`type` keyword argument is deprecated; a single type can be passed to `types` instead"
        types = type
    end
    if threaded !== nothing
        @warn "`threaded` keyword argument is deprecated; to avoid multithreaded parsing, pass `ntasks=1`"
        ntasks = threaded ? Threads.nthreads() : 1
    end
    if header isa Integer
        if header == 1 && skipto == 1
            header = -1
        elseif skipto != -1 && skipto < header
            throw(ArgumentError("skipto row ($skipto) must come after header row ($header)"))
        end
    end
    if skipto == -1
        if isa(header, Vector{Symbol}) || isa(header, Vector{String})
            skipto = 0
        elseif header isa Integer
            # by default, data starts on line after header
            skipto = header + 1
        elseif header isa AbstractVector{<:Integer}
            skipto = last(header) + 1
        end
    end
    debug && println("header is: $header, skipto computed as: $skipto")
    # getsource will turn any input into a `AbstractVector{UInt8}`
    buf, pos, len, tempfile = getsource(source, buffer_in_memory)
    if len > MAX_INPUT_SIZE
        throw(ArgumentError("delimited source to parse too large; must be < $MAX_INPUT_SIZE bytes"))
    end
    # skip over initial BOM character, if present
    pos = consumeBOM(buf, pos)

    oq = something(openquotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    trues = truestrings === nothing ? nothing : truestrings
    falses = falsestrings === nothing ? nothing : falsestrings
    sentinel = missingstring === nothing ? missingstring : (isempty(missingstring) || (missingstring isa Vector && length(missingstring) == 1 && missingstring[1] == "")) ? missing : missingstring isa String ? [missingstring] : missingstring

    if delim === nothing
        if source isa AbstractString || source isa AbstractPath
            filename = string(source)
            del = endswith(filename, ".tsv") ? UInt8('\t') : endswith(filename, ".wsv") ? UInt8(' ') : UInt8('\n')
        else
            del = UInt8('\n')
        end
    else
        del = (delim isa Char && isascii(delim)) ? delim % UInt8 :
            (sizeof(delim) == 1 && isascii(delim)) ? delim[1] % UInt8 : delim
    end
    cmt = comment === nothing ? nothing : (pointer(comment), sizeof(comment))

    if footerskip > 0 && len > 0
        lastbyte = buf[end]
        endpos = (lastbyte == UInt8('\r') || lastbyte == UInt8('\n')) +
            (lastbyte == UInt8('\n') && buf[end - 1] == UInt8('\r'))
        revlen = skiptorow(ReversedBuf(buf), 1 + endpos, len, oq, eq, cq, cmt, ignoreemptyrows, 0, footerskip) - 2
        len -= revlen
        debug && println("adjusted for footerskip, len = $(len + revlen - 1) => $len")
    end

    df = dateformat isa AbstractVector || dateformat isa AbstractDict ? nothing : dateformat
    wh1 = UInt8(' ')
    wh2 = UInt8('\t')
    if sentinel isa Vector
        for sent in sentinel
            if contains(sent, " ")
                wh1 = 0x00
            end
            if contains(sent, "\t")
                wh2 = 0x00
            end
        end
    end
    headerpos = datapos = pos
    if !transpose
        # step 1: detect the byte position where the column names start (headerpos)
        # and where the first data row starts (datapos)
        headerpos, datapos = detectheaderdatapos(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, header, skipto)
        debug && println("headerpos = $headerpos, datapos = $datapos")
    end
    # step 2: detect delimiter (or use given) and detect number of (estimated) rows and columns
    # step 3: build Parsers.Options w/ parsing arguments
    if del isa UInt8
        d, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, cmt, ignoreemptyrows, del)
        wh1 = d == UInt(' ') ? 0x00 : wh1
        wh2 = d == UInt8('\t') ? 0x00 : wh2
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, quoted, parsingdebug, stripwhitespace, false, groupmark)
    elseif del isa Char
        _, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, cmt, ignoreemptyrows)
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, del, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, quoted, parsingdebug, stripwhitespace, false, groupmark)
        d = del
    elseif del isa String
        _, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, cmt, ignoreemptyrows)
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, del, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, quoted, parsingdebug, stripwhitespace, false, groupmark)
        d = del
    else
        error("invalid delim type")
    end
    debug && println("estimated rows: $rowsguess")
    debug && println("detected delimiter: \"$(escape_string(d isa UInt8 ? string(Char(d)) : d))\"")

    if !transpose
        # step 4a: if we're ignoring repeated delimiters, then we ignore any
        # that start a row, so we need to check if we need to adjust our headerpos/datapos
        if ignorerepeated
            if headerpos > 0
                headerpos = Parsers.checkdelim!(buf, headerpos, len, options)
            end
            datapos = Parsers.checkdelim!(buf, datapos, len, options)
        end

        # step 4b: generate or parse column names
        names = detectcolumnnames(buf, headerpos, datapos, len, options, header, normalizenames, oq, eq, cq, cmt, ignoreemptyrows)
        ncols = length(names)
    else
        # transpose
        rowsguess, names, positions, endpositions = detecttranspose(buf, pos, len, options, header, skipto, normalizenames)
        ncols = length(names)
        datapos = isempty(positions) ? 0 : positions[1]
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    # generate initial columns
    columns, customtypes = initialize_columns(ncols, types, names, stringtype, streaming, options; validate=validate)
    if transpose
        # set column positions
        for i = 1:ncols
            col = columns[i]
            col.position = positions[i]
            col.endposition = endpositions[i]
        end
    end
    # check for nonstandard types in typemap
    typemap = convert(IdDict{Type, Type}, typemap)::IdDict{Type, Type}
    for T in values(typemap)
        if nonstandardtype(T) !== Union{}
            customtypes = tupcat(customtypes, nonstandardtype(T))
        end
    end

    # generate column options if applicable
    if dateformat isa AbstractDict
        for i = 1:ncols
            df = getordefault(dateformat, names[i], i, nothing)
            # devdoc: if we want to add any other column-specific parsing options, this is where we'd at the logic
            # e.g. per-column sentinel, decimal, trues, falses, openquotechar, closequotechar, escapechar, etc.
            if df !== nothing
                columns[i].options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, true, parsingdebug, stripwhitespace, false, groupmark)
            end
        end
        validate && checkinvalidcolumns(dateformat, "dateformat", ncols, names)
    end

    # pool keyword
    finalpool = 0.0
    if !streaming
        if pool isa AbstractVector
            length(pool) == ncols || throw(ArgumentError("provided `pool::AbstractVector` keyword argument doesn't match detected # of columns: `$(length(pool)) != $ncols`"))
            for i = 1:ncols
                col = columns[i]
                col.pool = getpool(pool[i])
                col.columnspecificpool = true
            end
        elseif pool isa AbstractDict
            for i = 1:ncols
                col = columns[i]
                p = getordefault(pool, names[i], i, NaN)
                if !isnan(p)
                    col.pool = getpool(p)
                    col.columnspecificpool = true
                end
            end
            validate && checkinvalidcolumns(pool, "pool", ncols, names)
        elseif pool isa Base.Callable
            for i = 1:ncols
                col = columns[i]
                p = pool(i, names[i])
                if p !== nothing
                    col.pool = getpool(p)
                    col.columnspecificpool = true
                end
            end
        else
            finalpool = getpool(pool)
            for col in columns
                col.pool = finalpool
            end
        end
    end

    # figure out if we'll drop any columns while parsing
    if select !== nothing && drop !== nothing
        throw(ArgumentError("`select` and `drop` keywords were both provided; only one or the other is allowed"))
    elseif select !== nothing
        if select isa AbstractVector{Bool}
            for i = 1:ncols
                select[i] || willdrop!(columns, i)
            end
        elseif select isa AbstractVector{<:Integer}
            for i = 1:ncols
                i in select || willdrop!(columns, i)
            end
        elseif select isa AbstractVector{Symbol} || select isa AbstractVector{<:AbstractString}
            select = map(Symbol, select)
            for i = 1:ncols
                names[i] in select || willdrop!(columns, i)
            end
        elseif select isa Base.Callable
            for i = 1:ncols
                select(i, names[i])::Bool || willdrop!(columns, i)
            end
        else
            throw(ArgumentError("`select` keyword argument must be an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a selector function of the form `(i, name) -> keep::Bool`"))
        end
    elseif drop !== nothing
        if drop isa AbstractVector{Bool}
            for i = 1:ncols
                drop[i] && willdrop!(columns, i)
            end
        elseif drop isa AbstractVector{<:Integer}
            for i = 1:ncols
                i in drop && willdrop!(columns, i)
            end
        elseif drop isa AbstractVector{Symbol} || drop isa AbstractVector{<:AbstractString}
            drop = map(Symbol, drop)
            for i = 1:ncols
                names[i] in drop && willdrop!(columns, i)
            end
        elseif drop isa Base.Callable
            for i = 1:ncols
                drop(i, names[i])::Bool && willdrop!(columns, i)
            end
        else
            throw(ArgumentError("`drop` keyword argument must be an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a selector function of the form `(i, name) -> keep::Bool`"))
        end
    end
    debug && println("computed types are: $types")

    # determine if we can use threads while parsing
    limit = something(limit, typemax(Int))
    minrows = min(limit, rowsguess)
    nthreads = Int(something(ntasks, Threads.nthreads()))
    if ntasks === nothing && !streaming && nthreads > 1 && !transpose && minrows > (nthreads * 5) && (minrows * ncols) >= 5_000
        threaded = true
        ntasks = nthreads
    elseif ntasks !== nothing && ntasks > 1
        threaded = true
        if transpose
            @warn "`ntasks > 1` not supported on transposed files"
            threaded = false
            ntasks = 1
        elseif minrows < (nthreads * 5)
            @warn "`ntasks > 1` but there were not enough estimated rows ($minrows) to justify multithreaded parsing"
            threaded = false
            ntasks = 1
        end
    else
        threaded = false
        ntasks = 1
    end
    # attempt to chunk up a file for multithreaded parsing; there's chance we can't figure out how to accurately chunk
    # due to quoted fields, so threaded might get set to false
    if threaded
        # when limiting w/ multithreaded parsing, we try to guess about where in the file the limit row # will be
        # then adjust our final file len to the end of that row
        # we add some cushion so we hopefully get the limit row correctly w/o shooting past too far and needing to resize! down
        # but we also don't guarantee limit will be exact w/ multithreaded parsing
        origrowsguess = rowsguess
        if limit !== typemax(Int)
            limit = Int(limit)
            limitposguess = ceil(Int, (limit / (origrowsguess * 0.8)) * len)
            if limitposguess < len
                newlen = [0, limitposguess, min(limitposguess * 2, len)]
                findchunkrowstart(newlen, 2, buf, options, typemap, downcast, ncols, 5, columns, Type[col.type for col in columns], ReentrantLock(), stringtype, Threads.Atomic{Int}(0), Threads.Atomic{Int}(0), Threads.Atomic{Bool}(true))
                len = newlen[2] - 1
                reinitialize_column_type!(columns, types, names, stringtype, streaming)
                origrowsguess = limit
            end
            debug && println("limiting, adjusting len to $len")
        end
        chunksize = div(len - datapos, ntasks)
        chunkpositions = [datapos + chunksize * i for i in 0:ntasks]
        chunkpositions[end] = len
        debug && println("initial byte positions before adjusting for start of rows: $chunkpositions")
        avgbytesperrow, successfullychunked = findrowstarts!(buf, options, chunkpositions, ncols, columns, stringtype, typemap, downcast, rows_to_check)
        ntasks = length(chunkpositions) - 1
        if successfullychunked
            origbytesperrow = ((len - datapos) / origrowsguess)
            weightedavgbytesperrow = ceil(Int, avgbytesperrow * ((ntasks - 1) / ntasks) + origbytesperrow * (1 / ntasks))
            rowsguess = ceil(Int, ((len - datapos) / weightedavgbytesperrow) * 1.01)
            debug && println("single-threaded estimated rows = $origrowsguess, multi-threaded estimated rows = $rowsguess")
            debug && println("multi-threaded column types sampled as: $columns")
        else
            # The following debug statement is doubled by a loud @warning or @error in parsefilechunk!
            debug && println("multi-threaded parsing failed! Falling back to single thread, reinitializing column types.")
            reinitialize_column_type!(columns, types, names, stringtype, streaming)
            threaded = false # the failing is signaled by having !ctx.threaded && ctx.ntasks > 1
        end
    end
    if !threaded
        chunkpositions = EMPTY_INT_ARRAY
        if limit < rowsguess
            rowsguess = limit
        end
    end

    end # @inbounds begin
    return Context(
        transpose,
        getname(source),
        names,
        rowsguess,
        ncols,
        buf,
        datapos,
        len,
        skipto,
        options,
        columns,
        finalpool,
        downcast,
        customtypes,
        typemap,
        stringtype,
        limit,
        threaded,
        ntasks,
        chunkpositions,
        strict,
        silencewarnings,
        maxwarnings,
        debug,
        tempfile,
        streaming,
        types,
    )
end
