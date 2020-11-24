struct Header{transpose, O, IO}
    name::String
    names::Vector{Symbol}
    rowsguess::Int64
    cols::Int
    e::UInt8
    buf::IO
    len::Int
    datapos::Int64
    datarow::Int
    options::O # Parsers.Options
    coloptions::Union{Nothing, Vector{Parsers.Options}}
    positions::Vector{Int64}
    types::Vector{Type}
    flags::Vector{UInt8}
    todrop::Vector{Int}
    pool::Float64
    customtypes::Type
end

gettranspose(h::Header{transpose}) where {transpose} = transpose

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

getdf(x::AbstractDict{String}, nm, i) = haskey(x, string(nm)) ? x[string(nm)] : nothing
getdf(x::AbstractDict{Symbol}, nm, i) = haskey(x, nm) ? x[nm] : nothing
getdf(x::AbstractDict{Int}, nm, i) = haskey(x, i) ? x[i] : nothing

@inline function Header(source,
    # file options
    # header can be a row number, range of rows, or actual string vector
    header,
    normalizenames,
    datarow,
    skipto,
    footerskip,
    transpose,
    comment,
    use_mmap,
    ignoreemptylines,
    select,
    drop,
    # parsing options
    missingstrings,
    missingstring,
    delim,
    ignorerepeated,
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
    lazystrings,
    strict,
    silencewarnings,
    debug,
    parsingdebug,
    streaming)

    # initial argument validation and adjustment
    !isa(source, IO) && !isa(source, AbstractVector{UInt8}) && !isa(source, Cmd) && !isfile(source) &&
        throw(ArgumentError("\"$source\" is not a valid file"))
    (types !== nothing && any(x->!isconcretetype(x) && !(x isa Union), types isa AbstractDict ? values(types) : types)) && throw(ArgumentError("Non-concrete types passed in `types` keyword argument, please provide concrete types for columns: $types"))
    checkvaliddelim(delim)
    ignorerepeated && delim === nothing && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter like `delim=','`"))
    if use_mmap !== nothing
        Base.depwarn("`use_mmap` keyword argument is deprecated and will be removed in the next release", :Header)
    end
    if skipto !== nothing
        if datarow != -1
            @warn "both `skipto` and `datarow` arguments provided, using `skipto`"
        end
        datarow = skipto
    end
    header = (isa(header, Integer) && header == 1 && datarow == 1) ? -1 : header
    isa(header, Integer) && datarow != -1 && (datarow > header || throw(ArgumentError("data row ($datarow) must come after header row ($header)")))
    datarow = datarow == -1 ? (isa(header, Vector{Symbol}) || isa(header, Vector{String}) ? 0 : last(header)) + 1 : datarow # by default, data starts on line after header
    debug && println("header is: $header, datarow computed as: $datarow")
    # getsource will turn any input into a `AbstractVector{UInt8}`
    buf, pos, len = getsource(source)
    # skip over initial BOM character, if present
    pos = consumeBOM(buf, pos)

    oq = something(openquotechar, quotechar) % UInt8
    eq = escapechar % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    trues = truestrings === nothing ? nothing : truestrings
    falses = falsestrings === nothing ? nothing : falsestrings
    sentinel = ((isempty(missingstrings) && missingstring == "") || (length(missingstrings) == 1 && missingstrings[1] == "")) ? missing : isempty(missingstrings) ? [missingstring] : missingstrings

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
        revlen = skiptorow(ReversedBuf(buf), 1 + endpos, len, oq, eq, cq, cmt, ignoreemptylines, 0, footerskip) - 2
        len -= revlen
        debug && println("adjusted for footerskip, len = $(len + revlen - 1) => $len")
    end

    if !transpose
        # step 1: detect the byte position where the column names start (headerpos)
        # and where the first data row starts (datapos)
        headerpos, datapos = detectheaderdatapos(buf, pos, len, oq, eq, cq, cmt, ignoreemptylines, header, datarow)
        debug && println("headerpos = $headerpos, datapos = $datapos")

        # step 2: detect delimiter (or use given) and detect number of (estimated) rows and columns
        d, rowsguess = detectdelimandguessrows(buf, headerpos, datapos, len, oq, eq, cq, del, cmt, ignoreemptylines)
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
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, dateformat, ignorerepeated, ignoreemptylines, comment, true, parsingdebug, strict, silencewarnings)

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
        positions = Int64[]
    else
        # transpose
        d, rowsguess = detectdelimandguessrows(buf, pos, pos, len, oq, eq, cq, del, cmt, ignoreemptylines)
        wh1 = d == UInt(' ') ? 0x00 : UInt8(' ')
        wh2 = d == UInt8('\t') ? 0x00 : UInt8('\t')
        options = Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, dateformat, ignorerepeated, ignoreemptylines, comment, true, parsingdebug, strict, silencewarnings)
        rowsguess, names, positions = detecttranspose(buf, pos, len, options, header, datarow, normalizenames)
        ncols = length(names)
        datapos = isempty(positions) ? 0 : positions[1]
    end
    debug && println("column names detected: $names")
    debug && println("byte position of data computed at: $datapos")

    # generate column options if applicable
    if dateformats isa AbstractDict
        coloptions = Vector{Parsers.Options}(undef, ncols)
        for i = 1:ncols
            df = getdf(dateformats, names[i], i)
            coloptions[i] = df === nothing ? options : Parsers.Options(sentinel, wh1, wh2, oq, cq, eq, d, decimal, trues, falses, df, ignorerepeated, ignoreemptylines, comment, true, parsingdebug, strict, silencewarnings)
        end
    else
        coloptions = nothing
    end
    debug && println("column options generated as: $(something(coloptions, ""))")

    # deduce initial column types for parsing based on whether any user-provided types were provided or not
    T = type === nothing ? (streaming ? Union{String, Missing} : Union{}) : type
    F = (type === nothing ? (streaming ? (USER | TYPEDETECTED) : 0x00) : (USER | TYPEDETECTED)) | (lazystrings ? LAZYSTRINGS : 0x00)
    if types isa Vector
        types = Type[T for T in types]
        flags = [(USER | TYPEDETECTED | (lazystrings ? LAZYSTRINGS : 0x00)) for _ = 1:ncols]
    elseif types isa AbstractDict
        flags = initialflags(F, types, names, lazystrings)
        types = initialtypes(T, types, names)
    else
        types = Type[T for _ = 1:ncols]
        flags = [F for _ = 1:ncols]
    end
    if streaming
        for i = 1:ncols
            T = types[i]
            if T === PooledString || T === Union{PooledString, Missing}
                @warn "pooled column types not allowed in `CSV.Rows` (column number = $i)"
                types[i] = T >: Missing ? Union{String, Missing} : String
            end
        end
    end
    # generate a customtypes Tuple{...} we'll need to generate code for during parsing
    customtypes = Tuple{(nonstandardtype(T) for T in vcat(types, values(typemap)...) if nonstandardtype(T) !== Union{})...}
    # figure out if we'll drop any columns while parsing
    todrop = Int[]
    if select !== nothing && drop !== nothing
        error("`select` and `drop` keywords were both provided; only one or the other is allowed")
    elseif select !== nothing
        if select isa AbstractVector{Int}
            for i = 1:ncols
                i in select || push!(todrop, i)
            end
        elseif select isa AbstractVector{Symbol} || select isa AbstractVector{<:AbstractString}
            select = map(Symbol, select)
            for i = 1:ncols
                names[i] in select || push!(todrop, i)
            end
        elseif select isa AbstractVector{Bool}
            for i = 1:ncols
                select[i] || push!(todrop, i)
            end
        elseif select isa Base.Callable
            for i = 1:ncols
                select(i, names[i]) || push!(todrop, i)
            end
        else
            error("`select` keyword argument must be an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a selector function of the form `(i, name) -> keep::Bool`")
        end
    elseif drop !== nothing
        if drop isa AbstractVector{Int}
            for i = 1:ncols
                i in drop && push!(todrop, i)
            end
        elseif drop isa AbstractVector{Symbol} || drop isa AbstractVector{<:AbstractString}
            drop = map(Symbol, drop)
            for i = 1:ncols
                names[i] in drop && push!(todrop, i)
            end
        elseif drop isa AbstractVector{Bool}
            for i = 1:ncols
                drop[i] && push!(todrop, i)
            end
        elseif drop isa Base.Callable
            for i = 1:ncols
                drop(i, names[i]) && push!(todrop, i)
            end
        else
            error("`drop` keyword argument must be an `AbstractVector` of `Int`, `Symbol`, `String`, or `Bool`, or a selector function of the form `(i, name) -> keep::Bool`")
        end
    end
    for i in todrop
        flags[i] |= WILLDROP | ANYMISSING
        types[i] = Missing
    end
    debug && println("computed types are: $types")
    pool = pool === true ? 1.0 : pool isa Float64 ? pool : 0.0
    return Header{transpose, typeof(options), typeof(buf)}(
        getname(source),
        names,
        rowsguess,
        ncols,
        eq,
        buf,
        len,
        datapos,
        datarow,
        options,
        coloptions,
        positions,
        types,
        flags,
        todrop,
        pool,
        customtypes
    )
end
