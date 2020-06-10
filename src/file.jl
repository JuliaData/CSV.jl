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
  * `limit`: an `Int` to indicate a limited number of rows to parse in a csv file; use in combination with `skipto` to read a specific, contiguous chunk within a file
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
    strict::Bool=false,
    silencewarnings::Bool=false,
    debug::Bool=false,
    parsingdebug::Bool=false,)

    h = Header(source, header, normalizenames, datarow, skipto, footerskip, limit, transpose, comment, use_mmap, ignoreemptylines, threaded, select, drop, missingstrings, missingstring, delim, ignorerepeated, quotechar, openquotechar, closequotechar, escapechar, dateformat, dateformats, decimal, truestrings, falsestrings, type, types, typemap, categorical, pool, strict, silencewarnings, debug, parsingdebug, false)
    rowsguess, ncols, buf, len, datapos, options, coloptions, positions, types, flags, pool, categorical = h.rowsguess, h.cols, h.buf, h.len, h.datapos, h.options, h.coloptions, h.positions, h.types, h.flags, h.pool, h.categorical
    # determine if we can use threads while parsing
    if threaded === nothing && VERSION >= v"1.3-DEV" && Threads.nthreads() > 1 && !transpose && limit == typemax(Int64) && rowsguess > Threads.nthreads() && (rowsguess * ncols) >= 5_000
        threaded = true
    elseif threaded === true
        if VERSION < v"1.3-DEV"
            @warn "incompatible julia version for `threaded=true`: $VERSION, requires >= v\"1.3\", setting `threaded=false`"
            threaded = false
        elseif transpose
            @warn "`threaded=true` not supported on transposed files"
            threaded = false
        elseif limit != typemax(Int64)
            @warn "`threaded=true` not supported when limiting # of rows"
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
        finalrows, tapes = multithreadparse(types, flags, buf, datapos, len, options, coloptions, rowsguess, pool, refs, ncols, typemap, limit, debug)
    else
        tapes, poslens = allocate(rowsguess, ncols, types, flags)
        t = Base.time()
        finalrows = parsetape!(Val(transpose), ncols, typemap, tapes, poslens, buf, datapos, len, limit, positions, pool, refs, rowsguess, rowsguess, types, flags, debug, options, coloptions)
        debug && println("time for initial parsing to tape: $(Base.time() - t)")
    end
    debug && println("types after parsing: $types, pool = $pool")
    for i = 1:ncols
        tape = tapes[i]
        if tape isa Vector{UInt32}
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
                r =  isassigned(refs, i) ? (anymissing(flags[i]) ? refs[i].refs : convert(Dict{String, UInt32}, refs[i].refs)) : Dict{String, UInt32}()
                tapes[i] = PooledArray(PooledArrays.RefArray(tape), r)
            end
        elseif tape isa Vector{PosLen}
            tapes[i] = [str(buf, options.e, x) for x in tape]
        else
            if !anymissing(flags[i])
                if tapes[i] isa SentinelArray{Union{Missing, Bool},1,Missing,Missing,Array{Union{Missing, Bool},1}}
                    tapes[i] = convert(Vector{Bool}, parent(tapes[i]))
                else
                    tapes[i] = parent(tapes[i])
                end
            end
        end
        # if zero rows
        if types[i] === Union{}
            types[i] = Missing
        end
    end
    columns = tapes
    deleteat!(h.names, h.todrop)
    deleteat!(types, h.todrop)
    ncols -= length(h.todrop)
    deleteat!(columns, h.todrop)
    lookup = Dict(k => v for (k, v) in zip(h.names, columns))
    return File{something(threaded, false)}(h.name, h.names, types, finalrows, ncols, columns, lookup)
end

function multithreadparse(types, flags, buf, datapos, len, options, coloptions, rowsguess, pool, refs, ncols, typemap, limit, debug)
    N = Threads.nthreads()
    chunksize = div(len - datapos, N)
    ranges = [i == 0 ? datapos : (datapos + chunksize * i) for i = 0:N]
    ranges[end] = len
    debug && println("initial byte positions before adjusting for start of rows: $ranges")
    findrowstarts!(buf, len, options, ranges, ncols)
    rowchunkguess = cld(rowsguess, N)
    debug && println("parsing using $N threads: $rowchunkguess rows chunked at positions: $ranges")
    rows = Threads.Atomic{Int64}(0)
    perthreadtapes = Vector{Vector{AbstractVector}}(undef, N)
    perthreadposlens = Vector{Vector{Vector{UInt64}}}(undef, N)
    perthreadrefs = Vector{Vector{RefPool}}(undef, N)
    perthreadtypes = [copy(types) for i = 1:N]
    perthreadflags = [copy(flags) for i = 1:N]
    @sync for i = 1:N
@static if VERSION >= v"1.3-DEV"
        Threads.@spawn begin
            tt = Base.time()
            tl_types = perthreadtypes[i]
            tl_flags = perthreadflags[i]
            tl_tapes, tl_poslens = allocate(rowchunkguess, ncols, tl_types, tl_flags)
            tl_refs = Vector{RefPool}(undef, ncols)
            tl_len = ranges[i + 1] - (i != N)
            tl_rows = parsetape!(Val(false), ncols, typemap, tl_tapes, tl_poslens, buf, ranges[i], tl_len, limit, Int64[], pool, tl_refs, rowchunkguess, rowsguess, tl_types, tl_flags, debug, options, coloptions)
            debug && println("thread = $(Threads.threadid()): time for parsing: $(Base.time() - tt)")
            Threads.atomic_add!(rows, tl_rows)
            perthreadtapes[i] = tl_tapes
            perthreadposlens[i] = tl_poslens
            perthreadrefs[i] = tl_refs
        end
end # @static if VERSION >= v"1.3-DEV"
    end
    for i = 1:N
        tl_types = perthreadtypes[i]
        tl_flags = perthreadflags[i]
        for col = 1:ncols
            @inbounds T = types[col]
            @inbounds types[col] = promote_types(T, tl_types[col])
            if debug && T !== types[col]
                println("promoting col = $col from $T to $(types[col]), thread chunk was type = $(tl_types[col])")
            end
            @inbounds flags[col] |= tl_flags[col]
        end
    end
    # take care of any column promoting that needs to happen between threads
    for i = 1:N
        tltapes = perthreadtapes[i]
        tlposlens = perthreadposlens[i]
        tlrefs = perthreadrefs[i]
        tlrows = length(tltapes[1])::Int64
        for col = 1:ncols
            @inbounds T = types[col]
            if (T === String || T === Union{String, Missing}) && !(tltapes[col] isa Vector{PosLen})
                debug && println("promoting $(eltype(tltapes[col])) for column = $col from thread = $i to String")
                # promoting non-string to string column
                tltapes[col] = tlposlens[col]
            elseif (T === PooledString || T === Union{PooledString, Missing}) && tltapes[col] isa MissingVector
                tltapes[col] = allocate(PooledString, tlrows)
            elseif tltapes[col] isa MissingVector
                tltapes[col] = allocate(T, tlrows)
            elseif (T === Float64 || T === Union{Float64, Missing}) && tltapes[col] isa SVec{Int64}
                tltapes[col] = convert(SentinelVector{Float64}, tltapes[col])
            elseif tltapes[col] isa Vector{UInt32}
                if !isassigned(refs, col)
                    refs[col] = tlrefs[col]
                else
                    colrefs = refs[col]
                    tlcolrefs = tlrefs[col]
                    refrecodes = collect(UInt32(1):tlcolrefs.lastref)
                    recode = false
                    for (k, v) in tlcolrefs.refs
                        refvalue = get(colrefs.refs, k, UInt32(0))
                        if refvalue != v
                            recode = true
                            if refvalue == 0
                                refvalue = (colrefs.lastref += UInt32(1))
                            end
                            colrefs.refs[k] = refvalue
                            refrecodes[v] = refvalue
                        end
                    end
                    if recode
                        tape = tltapes[col]
                        if tape isa Vector{UInt32}
                            for j = 1:tlrows
                                tape[j] = refrecodes[tape[j]]
                            end
                        end
                    end
                end
            end
        end
    end
    for i = 1:ncols
        _vcat!(rows[], perthreadtapes, i)
    end
    return rows[], perthreadtapes[1]
end

function parsetape!(TR::Val{transpose}, ncols, typemap, tapes, poslens, buf, pos, len, limit, positions, pool, refs, rowsguess, fullrowsguess, types, flags, debug, options::Parsers.Options{ignorerepeated}, coloptions) where {transpose, ignorerepeated}
    row = 0
    startpos = pos
    if pos <= len && len > 0
        while row < limit
            row += 1
            pos = parserow(row, TR, ncols, typemap, tapes, poslens, buf, pos, len, positions, pool, refs, rowsguess, fullrowsguess, types, flags, debug, options, coloptions)
            pos > len && break
            # if our initial row estimate was too few, we need to reallocate our tapes/poslens to read the rest of the file
            if row + 1 > rowsguess
                # (bytes left in file) / (avg bytes per row) == estimated rows left in file (+ 10 for kicks)
                estimated_rows_left = ceil(Int64, (len - pos) / ((pos - startpos) / row) + 10.0)
                newrowsguess = rowsguess + estimated_rows_left
                debug && reallocatetape(row, rowsguess, newrowsguess)
                for i = 1:ncols
                    # TODO: specialize
                    reallocate!(tapes[i], newrowsguess)
                    if isassigned(poslens, i)
                        reallocate!(poslens[i], newrowsguess)
                    end
                end
                rowsguess = newrowsguess
            end
        end
    end
    # done parsing (at least this chunk), so resize tapes to final row count
    for i = 1:ncols
        # TODO: specialize
        resize!(tapes[i], row)
        if isassigned(poslens, i)
            resize!(poslens[i], row)
        end
    end
    return row
end

@noinline reallocatetape(row, old, new) = println("thread = $(Threads.threadid()) warning: didn't pre-allocate enough tape while parsing on row $row, re-allocating from $old to $new...")
@noinline notenoughcolumns(cols, ncols, row) = println("thread = $(Threads.threadid()) warning: only found $cols / $ncols columns on data row: $row. Filling remaining columns with `missing`")
@noinline toomanycolumns(cols, row) = println("thread = $(Threads.threadid()) warning: parsed expected $cols columns, but didn't reach end of line on data row: $row. Ignoring any extra columns on this row")
@noinline stricterror(T, buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))"))
@noinline warning(T, buf, pos, len, code, row, col) = println("thread = $(Threads.threadid()) warning: error parsing $T on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code))")
@noinline fatalerror(buf, pos, len, code, row, col) = throw(Error("thread = $(Threads.threadid()) fatal error, encountered an invalidly quoted field while parsing on row = $row, col = $col: \"$(String(buf[pos:pos+len-1]))\", error=$(Parsers.codes(code)), check your `quotechar` arguments or manually fix the field in the file itself"))

@inline function parserow(row, ::Val{transpose}, ncols, typemap, tapes, poslens, buf, pos, len, positions, pool, refs, rowsguess, fullrowsguess, types, flags, debug, options::Parsers.Options{ignorerepeated}, coloptions) where {transpose, ignorerepeated}
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
            pos, code = detect(tapes, buf, pos, len, opts, row, col, typemap, pool, refs, debug, types, flags, poslens, rowsguess)
        elseif tape isa SVec{Int64}
            pos, code = parseint!(flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa SVec{Float64}
            pos, code = parsevalue!(Float64, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa SVec{Date}
            pos, code = parsevalue!(Date, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa SVec{DateTime}
            pos, code = parsevalue!(DateTime, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa SVec{Time}
            pos, code = parsevalue!(Time, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa SentinelArray{Union{Missing, Bool},1,Missing,Missing,Array{Union{Missing, Bool},1}}
            pos, code = parsevalue!(Bool, flag, tape, tapes, buf, pos, len, opts, row, col, types, flags, poslens)
        elseif tape isa Vector{UInt32}
            pos, code = parsepooled!(flag, tape, tapes, buf, pos, len, opts, row, col, fullrowsguess, pool, refs, types, flags, poslens)
        elseif tape isa Vector{PosLen}
            pos, code = parsestring!(flag, tape, buf, pos, len, opts, row, col, types, flags)
        else
            @show typeof(tape)
            error("bad array type")
        # TODO: support all other integer types, float16, float32
        # in an else clause, we'll parse a string and call
        # Parsers.parse(T, str)
        end
        if transpose
            @inbounds positions[col] = pos
        else
            if col < ncols
                if Parsers.newline(code) || pos > len
                    options.silencewarnings || notenoughcolumns(col, ncols, row)
                    for j = (col + 1):ncols
                        if isassigned(poslens, j)
                            setposlen!(poslens[j], row, Parsers.SENTINEL, pos, UInt64(0))
                        end
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

@inline function setposlen!(tape, row, code, pos, len)
    pos = Core.bitcast(UInt64, pos) << 20
    pos |= ifelse(Parsers.sentinel(code), MISSING_BIT, UInt64(0))
    pos |= ifelse(Parsers.escapedstring(code), ESCAPE_BIT, UInt64(0))
    @inbounds tape[row] = Core.bitcast(PosLen, pos | Core.bitcast(UInt64, len))
    return
end

function detect(tapes, buf, pos, len, options, row, col, typemap, pool, refs, debug, types, flags, poslens, rowsguess)
    int, code, vpos, vlen, tlen = Parsers.xparse(Int64, buf, pos, len, options)
    if Parsers.invalidquotedfield(code)
        fatalerror(buf, pos, tlen, code, row, col)
    end
    if Parsers.sentinel(code) && code > 0
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        flags[col] |= ANYMISSING
        # return; parsing will continue to detect until a non-missing value is parsed
        @goto finaldone
    end
    if Parsers.ok(code) && !haskey(typemap, Int64)
        debug && println("detecting Int64 for column = $col on thread = $(Threads.threadid())")
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        tape = allocate(Int64, rowsguess)
        tape[row] = int
        newT = Int64
        @goto done
    end
    float, code, vpos, vlen, tlen = Parsers.xparse(Float64, buf, pos, len, options)
    if Parsers.ok(code) && !haskey(typemap, Float64)
        debug && println("detecting Float64 for column = $col on thread = $(Threads.threadid())")
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        tape = allocate(Float64, rowsguess)
        tape[row] = float
        newT = Float64
        @goto done
    end
    if options.dateformat === nothing
        try
            date, code, vpos, vlen, tlen = Parsers.xparse(Date, buf, pos, len, options)
            if Parsers.ok(code) && !haskey(typemap, Date)
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        tape = allocate(Bool, rowsguess)
        tape[row] = bool
        newT = Bool
        @goto done
    end
    _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
    if pool > 0.0
        r = RefPool()
        @inbounds refs[col] = r
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        tape = allocate(PooledString, rowsguess)
        if anymissing(flags[col])
            ref = getref!(r, missing, code, options)
            fill!(tape, ref)
        end
        ref = getref!(r, PointerString(pointer(buf, vpos), vlen), code, options)
        @inbounds tape[row] = ref
        newT = PooledString
    else
        debug && println("detecting String for column = $col on thread = $(Threads.threadid())")
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        tape = poslens[col]
        unset!(poslens, col, row, 1)
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

function parseint!(flag, tape, tapes, buf, pos, len, options, row, col, types, flags, poslens)
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
        if !user(flag)
            @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
                @inbounds types[col] = Union{Float64, anymissing(flag) ? Missing : Union{}}
            else
                # println("promoting to String instead")
                _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
                @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
                @inbounds tapes[col] = poslens[col]
                @inbounds types[col] = Union{String, anymissing(flag) ? Missing : Union{}}
                unset!(poslens, col, row, 2)
            end
        end
    end
    return pos + tlen, code
end

function parsevalue!(::Type{type}, flag, tape, tapes, buf, pos, len, options, row, col, types, flags, poslens) where {type}
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
        if !user(flag)
            @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
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
            _, code, vpos, vlen, tlen = Parsers.xparse(String, buf, pos, len, options)
            @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
            @inbounds tapes[col] = poslens[col]
            @inbounds types[col] = Union{String, anymissing(flag) ? Missing : Union{}}
            unset!(poslens, col, row, 2)
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

function parsepooled!(flag, tape, tapes, buf, pos, len, options, row, col, rowsguess, pool, refs, types, flags, poslens)
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
        # promote to string
        @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        @inbounds tapes[col] = poslens[col]
        @inbounds types[col] = Union{String, anymissing(flags[col]) ? Missing : Union{}}
        unset!(poslens, col, row, 2)
    else
        if !user(flag)
            @inbounds setposlen!(poslens[col], row, code, vpos, vlen)
        end
        @inbounds tape[row] = ref
    end
    return pos + tlen, code
end
