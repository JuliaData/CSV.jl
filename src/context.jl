# a RefPool holds our refs as a Dict, along with a lastref field which is incremented when a new ref is found while parsing pooled columns
mutable struct RefPool
    # what? why ::Any here? well, we want flexibility in what kind of refs we stick in here
    # it might be Dict{Union{String, Missing}, UInt32}, but it might be some other string type
    # or it might not allow `missing`; in short, there are too many options to try and type
    # the field concretely; luckily, working with the `refs` field here is limited to
    # a very few specific methods, and we'll always have the column type, so we just need
    # to make sure we assert the concrete type before using refs
    refs::Any
    lastref::UInt32
end

# start lastref at 1, since it's reserved for `missing`, so first ref value will be 2
const Refs{T} = Dict{Union{T, Missing}, UInt32}
RefPool(::Type{T}=String) where {T} = RefPool(Refs{T}(), 1)

"""
Internal structure used to track information for a single column in a delimited file.

Fields:
  * `type`: always a single, concrete type; no Union{T, Missing}; missingness is tracked in anymissing field; this field is mutable; it may start as one type and get "promoted" to another while parsing; two special types exist: `NeedsTypeDetection`, which specifies that we need to try and detect what type this column's values are and `HardMissing` which means the column type is definitely `Missing` and we don't need to detect anything; to get the "final type" of a column after parsing, call `CSV.coltype(col)`, which takes into account `anymissing`
  * `anymissing`: whether any missing values have been encountered while parsing; if a user provided a type like `Union{Int, Missing}`, we'll set this to `true`, or when `missing` values are encountered while parsing
  * `userprovidedtype`: whether the column type was provided by the user or not; this affects whether we'll promote a column's type while parsing, or emit a warning/error depending on `strict` keyword arg
  * `willdrop`: whether we'll drop this column from the final columnset; computed from select/drop keyword arguments; this will result in a column type of `HardMissing` while parsing, where an efficient parser is used to "skip" a field w/o allocating any parsed value
  * `pool`: computed from `pool` keyword argument; `true` is `1.0`, `false` is `0.0`, everything else is `Float64(pool)`; once computed, this field isn't mutated at all while parsing; it's used in type detection to determine whether a column will be pooled or not once a type is detected; 
  * `column`: the actual column vector to hold parsed values; field is typed as `AbstractVector` and while parsing, we do switches on `col.type` to assert the column type to make code concretely typed
  * `refpool`: if the column is pooled (or might be pooled in single-threaded case), this is the column-specific `RefPool` used to track unique parsed values and their `UInt32` ref codes
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
    pool::Float64
    # lazily/manually initialized fields
    column::AbstractVector
    refpool::RefPool
    # per top-level column fields (don't need to copy per task when parsing)
    lock::ReentrantLock
    position::Int
    endposition::Int
    options::Parsers.Options

    Column(type::Type, anymissing::Bool, userprovidedtype::Bool, willdrop::Bool, pool::Float64) =
        new(type, anymissing, userprovidedtype, willdrop, pool)
end

function Column(type::Type, options::Parsers.Options=nothing)
    T = nonmissingtypeunlessmissingtype(type)
    col = Column(type === Missing ? HardMissing : T,
        type >: Missing,
        type !== NeedsTypeDetection,
        false, NaN)
    if options !== nothing
        col.options = options
    end
    return col
end

# creating a per-task column from top-level column
function Column(x::Column)
    @assert isdefined(x, :lock)
    y = Column(x.type, x.anymissing, x.userprovidedtype, x.willdrop, x.pool)
    y.lock = x.lock # parent and child columns _share_ the same lock
    if isdefined(x, :options)
        y.options = x.options
    end
    if isdefined(x, :refpool)
        # if parent has refpool from sampling, make a copy
        y.refpool = RefPool(copy(x.refpool.refs), x.refpool.lastref)
    end
    # specifically _don't_ copy/re-use x.column; that needs to be allocated fresh per parsing task
    return y
end

struct Context
    transpose::Val
    name::String
    names::Vector{Symbol}
    rowsguess::Int64
    cols::Int
    buf::AbstractVector{UInt8}
    datapos::Int64
    len::Int
    datarow::Int
    options::Parsers.Options
    columns::Vector{Column}
    pool::Float64
    downcast::Bool
    customtypes::Type
    typemap::Dict{Type, Type}
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

function Context(source,
    # file options
    # header can be a row number, range of rows, or actual string vector
    header,
    normalizenames,
    datarow,
    skipto,
    footerskip,
    transpose,
    comment,
    ignoreemptyrows,
    ignoreemptylines,
    select,
    drop,
    limit,
    threaded,
    ntasks,
    tasks,
    rows_to_check,
    lines_to_check,
    # parsing options
    missingstrings,
    missingstring,
    delim,
    ignorerepeated,
    quoted,
    quotechar,
    openquotechar,
    closequotechar,
    escapechar,
    dateformat,
    dateformats,
    decimal,
    truestrings,
    falsestrings,
    # type options
    type,
    types,
    typemap,
    pool,
    downcast,
    lazystrings,
    stringtype,
    strict,
    silencewarnings,
    maxwarnings,
    debug,
    parsingdebug,
    streaming)

    # initial argument validation and adjustment
    @inbounds begin
    !isa(source, IO) && !isa(source, AbstractVector{UInt8}) && !isa(source, Cmd) && !isfile(source) &&
        throw(ArgumentError("\"$source\" is not a valid file or doesn't exist"))
    if types !== nothing
        if types isa AbstractVector || types isa AbstractDict
            any(x->!concrete_or_concreteunion(x), types isa AbstractDict ? values(types) : types) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
        else
            concrete_or_concreteunion(types) || throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
        end
    end
    checkvaliddelim(delim)
    ignorerepeated && delim === nothing && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter like `delim=','`"))
    if lazystrings && !streaming
        Base.depwarn("`lazystrings` keyword argument is deprecated; use `stringtype=PosLenString` instead", :Context)
        stringtype = PosLenString
    end
    if tasks !== nothing
        Base.depwarn("`tasks` keyword argument is deprecated; use `ntasks` instead", :Context)
        ntasks = tasks
    end
    if ignoreemptylines !== nothing
        Base.depwarn("`ignoreemptylines` keyword argument is deprecated; use `ignoreemptyrows` instead", :Context)
        ignoreemptyrows = ignoreemptylines
    end
    if lines_to_check !== nothing
        Base.depwarn("`lines_to_check` keyword argument is deprecated; use `rows_to_check` instead", :Context)
        rows_to_check = lines_to_check
    end
    if !isempty(missingstrings)
        Base.depwarn("`missingstrings` keyword argument is deprecated; pass a `Vector{String}` to `missingstring` instead", :Context)
        missingstring = missingstrings
    end
    if dateformats !== nothing
        Base.depwarn("`dateformats` keyword argument is deprecated; pass column date formats to `dateformat` keyword argument instead", :Context)
        dateformat = dateformats
    end
    if datarow != -1
        Base.depwarn("`datarow` keyword argument is deprecated; use `skipto` instead", :Context)
        skipto = datarow
    end
    if type !== nothing
        Base.depwarn("`type` keyword argument is deprecated; a single type can be passed to `types` instead", :Context)
        types = type
    end
    if threaded !== nothing
        Base.depwarn("`threaded` keyword argument is deprecated; to avoid multithreaded parsing, pass `ntasks=1`", :Context)
        ntasks = threaded ? Threads.nthreads() : 1
    end
    header = (isa(header, Integer) && header == 1 && skipto == 1) ? -1 : header
    isa(header, Integer) && skipto != -1 && (skipto > header || throw(ArgumentError("data row ($skipto) must come after header row ($header)")))
    skipto = skipto == -1 ? (isa(header, Vector{Symbol}) || isa(header, Vector{String}) ? 0 : last(header)) + 1 : skipto # by default, data starts on line after header
    debug && println("header is: $header, skipto computed as: $skipto")
    # getsource will turn any input into a `AbstractVector{UInt8}`
    buf, pos, len, tempfile = getsource(source)
    if len > Int64(2)^42
        throw(ArgumentError("delimited source to parse too large; must be < $(2^42) bytes"))
    end
    # skip over initial BOM character, if present
    pos = consumeBOM(buf, pos)

    oq = something(openquotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    trues = truestrings === nothing ? nothing : truestrings
    falses = falsestrings === nothing ? nothing : falsestrings
    sentinel = (isempty(missingstring) || (missingstring isa Vector && length(missingstring) == 1 && missingstring[1] == "")) ? missing : missingstring isa String ? [missingstring] : missingstring

    if delim === nothing
        del = isa(source, AbstractString) && endswith(source, ".tsv") ? UInt8('\t') :
            isa(source, AbstractString) && endswith(source, ".wsv") ? UInt8(' ') :
            UInt8('\n')
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
    if !transpose
        # step 1: detect the byte position where the column names start (headerpos)
        # and where the first data row starts (datapos)
        headerpos, datapos = detectheaderdatapos(buf, pos, len, oq, eq, cq, cmt, ignoreemptyrows, header, skipto)
        debug && println("headerpos = $headerpos, datapos = $datapos")

        # step 2: detect delimiter (or use given) and detect number of (estimated) rows and columns
        d, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, del, cmt, ignoreemptyrows)
        debug && println("estimated rows: $rowsguess")
        debug && println("detected delimiter: \"$(escape_string(d isa UInt8 ? string(Char(d)) : d))\"")

        # step 3: build Parsers.Options w/ parsing arguments
        wh1 = d == UInt(' ') ? 0x00 : UInt8(' ')
        wh2 = d == UInt8('\t') ? 0x00 : UInt8('\t')
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
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, quoted, parsingdebug)

        # step 4a: if we're ignoring repeated delimiters, then we ignore any
        # that start a row, so we need to check if we need to adjust our headerpos/datapos
        if ignorerepeated
            if headerpos > 0
                headerpos = Parsers.checkdelim!(buf, headerpos, len, options)
            end
            datapos = Parsers.checkdelim!(buf, datapos, len, options)
        end

        # step 4b: generate or parse column names
        names = detectcolumnnames(buf, headerpos, datapos, len, options, header, normalizenames)
        ncols = length(names)
    else
        # transpose
        d, rowsguess = detectdelimandguessrows(buf, pos, pos, len, oq, eq, cq, del, cmt, ignoreemptyrows)
        wh1 = d == UInt(' ') ? 0x00 : UInt8(' ')
        wh2 = d == UInt8('\t') ? 0x00 : UInt8('\t')
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, quoted, parsingdebug)
        rowsguess, names, positions, endpositions = detecttranspose(buf, pos, len, options, header, skipto, normalizenames)
        ncols = length(names)
        datapos = isempty(positions) ? 0 : positions[1]
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    # generate initial columns
    # deduce initial column types/flags for parsing based on whether any user-provided types were provided or not
    customtypes = Tuple{}
    if types isa AbstractVector
        length(types) == ncols || throw(ArgumentError("provided `types::AbstractVector` keyword argument doesn't match detected # of columns: `$(length(types)) != $ncols`"))
        columns = Vector{Column}(undef, ncols)
        for i = 1:ncols
            col = Column(types[i], options)
            columns[i] = col
            if nonstandardtype(col.type) !== Union{}
                customtypes = tupcat(customtypes, nonstandardtype(col.type))
            end
        end
    elseif types isa AbstractDict
        T = streaming ? Union{stringtype, Missing} : NeedsTypeDetection
        columns = Vector{Column}(undef, ncols)
        for i = 1:ncols
            S = getordefault(types, names[i], i, T)
            col = Column(S, options)
            columns[i] = col
            if nonstandardtype(col.type) !== Union{}
                customtypes = tupcat(customtypes, nonstandardtype(col.type))
            end
        end
    else
        T = types === nothing ? (streaming ? Union{stringtype, Missing} : NeedsTypeDetection) : types
        columns = Vector{Column}(undef, ncols)
        foreach(1:ncols) do i
            col = Column(T, options)
            columns[i] = col
        end
    end
    if transpose
        # set column positions
        for i = 1:ncols
            col = columns[i]
            col.position = positions[i]
            col.endposition = endpositions[i]
        end
    end
    # check for nonstandard types in typemap
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
                columns[i].options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptyrows, comment, true, parsingdebug)
            end
        end
    end

    # pool keyword
    finalpool = 0.0
    if !streaming
        if pool isa AbstractVector
            length(pool) == ncols || throw(ArgumentError("provided `pool::AbstractVector` keyword argument doesn't match detected # of columns: `$(length(pool)) != $ncols`"))
            for i = 1:ncols
                columns[i].pool = getpool(pool[i])
            end
        elseif pool isa AbstractDict
            for i = 1:ncols
                columns[i].pool = getpool(getordefault(pool, names[i], i, 0.0))
            end
        else
            finalpool = getpool(pool)
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
                select(i, names[i]) || willdrop!(columns, i)
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
                drop(i, names[i]) && willdrop!(columns, i)
            end
        else
            throw(ArgumentError("`drop` keyword argument must be an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a selector function of the form `(i, name) -> keep::Bool`"))
        end
    end
    debug && println("computed types are: $types")

    # determine if we can use threads while parsing
    limit = something(limit, typemax(Int64))
    minrows = min(limit, rowsguess)
    nthreads = something(ntasks, Threads.nthreads())
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
        if limit !== typemax(Int64)
            limitposguess = ceil(Int64, (limit / (origrowsguess * 0.8)) * len)
            newlen = [0, limitposguess, min(limitposguess * 2, len)]
            findrowstarts!(buf, options, newlen, ncols, columns, stringtype, downcast, 5)
            len = newlen[2] - 1
            origrowsguess = limit
            debug && println("limiting, adjusting len to $len")
        end
        chunksize = div(len - datapos, ntasks)
        chunkpositions = [i == 0 ? datapos : i == ntasks ? len : (datapos + chunksize * i) for i = 0:ntasks]
        debug && println("initial byte positions before adjusting for start of rows: $chunkpositions")
        avgbytesperrow, successfullychunked = findrowstarts!(buf, options, chunkpositions, ncols, columns, stringtype, downcast, rows_to_check)
        if successfullychunked
            origbytesperrow = ((len - datapos) / origrowsguess)
            weightedavgbytesperrow = ceil(Int64, avgbytesperrow * ((ntasks - 1) / ntasks) + origbytesperrow * (1 / ntasks))
            rowsguess = ceil(Int64, ((len - datapos) / weightedavgbytesperrow) * 1.01)
            debug && println("single-threaded estimated rows = $origrowsguess, multi-threaded estimated rows = $rowsguess")
            debug && println("multi-threaded column types sampled as: $columns")
            # check if we need to adjust column pooling
            if finalpool == 0.0 || finalpool == 1.0
                foreach(col -> col.pool = finalpool, columns)
            end
        else
            debug && println("something went wrong chunking up a file for multithreaded parsing, falling back to single-threaded parsing")
            threaded = false
        end
    else
        chunkpositions = EMPTY_INT_ARRAY
    end
    if !threaded && limit < rowsguess
        rowsguess = limit
    end

    end # @inbounds begin
    return Context(
        Val(transpose),
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
        streaming
    )
end
