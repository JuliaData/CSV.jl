#=
    CSV parser core

The internal CSV parsing engine. It first finds all rows and fields. It then
parses field values and builds columns.

The pipeline (and the file's layout) is:

    L0  bytes         : The input is one `Vector{UInt8}`. Other code reads,
                        maps, or decompresses the source before this step.
    L1  rows and      : A quote-aware scan finds delimiters and row endings. It
        fields          stores their byte positions in one `ChunkIndex` for each
                        chunk. A scalar scanner supports all CSV options. A
                        vector scanner processes 64 bytes at a time.
    L1' chunks        : Under every quote rule but the lenient one, the parser
                        first divides the input into fixed byte ranges and
                        settles the quote state at each range start: a quote
                        count under the standard rule, a three-state table
                        under a distinct escape or quote byte. It then moves
                        each range start to the next complete row boundary
                        and indexes the resulting chunks at the same time.
    L2  types         : The parser reads rows from across the input. It uses
                        these rows to choose an initial type for each column.
    L3  values        : The parser reads each column from the stored field
                        positions. If a later value needs a different type, it
                        changes that column type. It reads only the affected
                        parts of that column again.
    L4  columns       : Each non-string column stores its values and a separate
                        present-or-missing flag. String columns store short
                        values inline and copy longer values into buffers the
                        column owns, with escapes removed during the parse.
                        Known row counts let the parser allocate each column once.
    L5  result        : `CSV.parse` runs the steps above and returns a
                        typed table. It also returns details about invalid data.

The API layer adds source handling, delimiter detection, row windows, pooling,
transposed input, multiple sources, and the public Tables.jl interfaces.

The index scan changes between inside and outside a quoted field at each quote
byte. Two quote bytes leave it in the same state. This rule supports standard
CSV fields and doubled quotes. A quote in the middle of an unquoted field starts
a quoted region during this scan. The value parser only starts a quoted value at
the start of a field, after allowed leading blanks. Invalid input with a bare
quote can therefore produce different row boundaries in these two steps. This
choice lets the parser find safe range starts without reading all earlier bytes
again.
=#

using Dates
using Durations: Timestamp
import Parsers

# A finished `Threads.@spawn` task keeps its closure (and everything the
# closure captured, such as a mapped input) alive until its thread runs
# another task (Julia 1.10 to 1.13; JuliaLang/julia master collects it). Every
# task CSV spawns clears its own closure on exit, so finished workers do not
# prevent collection of a mapped file. Same trick as ConcurrentUtilities.@wkspawn.
function _cleartask()
    t = current_task()
    t.storage = nothing
    t.code = nothing
    return
end

macro wkspawn(expr)
    return esc(:(Threads.@spawn begin
        try
            $expr
        finally
            $_cleartask()
        end
    end))
end

# Parsers owns scalar value conversion. CSV owns rows, fields, quotes, missing
# values, and column assembly.
const _ISO_DATE_PATTERN = Parsers.compilepattern("yyyy-mm-dd")
const _ISO_DATETIME_PATTERN = Parsers.compilepattern("yyyy-mm-ddTHH:MM:SS.s")
const _ISO_DATETIME_SPACE_PATTERN = Parsers.compilepattern("yyyy-mm-dd HH:MM:SS.s")
const _ISO_TIME_PATTERN = Parsers.compilepattern("HH:MM:SS.s")

# ---------------------------------------------------------------------------
# Dialect: the structural options. Value-level options (sentinels, dateformats,
# true/false spellings, decimal char) live in `ValueOpts`, built once in
# `makevalueopts` and applied to exact field spans by the Parsers functions.
# ---------------------------------------------------------------------------

struct Dialect
    delim::Union{UInt8, Vector{UInt8}}  # single byte fast path; multi-byte handled by the scalar scanner
    oq::UInt8                           # open quote
    cq::UInt8                           # close quote
    e::UInt8                            # escape char (== cq for RFC ""-doubling)
    quoted::Bool                        # false = no quote handling at all
    comment::Union{Nothing, Vector{UInt8}}  # rows beginning with these bytes are dropped
    ignoreemptyrows::Bool
    ignorerepeated::Bool                # adjacent delimiters collapse into one boundary
    # A quote opens a field only at the field start and closes only before the
    # delimiter or row end. This is the serial repair path the readers take
    # when the structural scan found a quote that did not start its field
    # (`5' 11"`, `x"y`); it is never the first pass.
    lenient::Bool
    # Comment rows are dropped by their first bytes, so their quotes have no
    # meaning. The parallel planner and the fast scanner assume comment rows
    # contain no quote byte; when one does, the index rebuilds serially with
    # the scalar scanner under this flag. Well-formed input never sets it.
    commentquotes::Bool
end

const LF = UInt8('\n')
const CR = UInt8('\r')

function Dialect(; delim::Union{Char, String}=',',
                   quotechar::Char='"',
                   openquotechar::Union{Char, Nothing}=nothing,
                   closequotechar::Union{Char, Nothing}=nothing,
                   escapechar::Union{Char, Nothing}=nothing,
                   quoted::Bool=true,
                   comment::Union{String, Nothing}=nothing,
                   ignoreemptyrows::Bool=true,
                   ignorerepeated::Bool=false,
                   lenient::Bool=false,
                   commentquotes::Bool=false)
    isempty(delim) && throw(ArgumentError("delimiter must be non-empty"))
    d = delim isa Char ? (isascii(delim) ? delim % UInt8 : Vector{UInt8}(string(delim))) :
        sizeof(delim) == 1 ? codeunit(delim, 1) : Vector{UInt8}(delim)
    for (nm, c) in (("quotechar", quotechar), ("openquotechar", openquotechar),
                    ("closequotechar", closequotechar), ("escapechar", escapechar))
        c === nothing || isascii(c) || throw(ArgumentError("$nm must be ASCII (got $(repr(c)))"))
    end
    oq = something(openquotechar, quotechar) % UInt8
    cq = something(closequotechar, quotechar) % UInt8
    e  = something(escapechar, Char(cq)) % UInt8
    for b in (d isa UInt8 ? (d,) : d)
        (b == LF || b == CR) && throw(ArgumentError("delimiter may not contain \\r or \\n"))
        quoted && b == oq && throw(ArgumentError("delimiter may not equal the quote character"))
    end
    quoted && (oq in (LF, CR) || cq in (LF, CR) || e in (LF, CR)) &&
        throw(ArgumentError("quote/escape characters may not be \\r or \\n"))
    cmt = comment === nothing ? nothing :
          isempty(comment) ? throw(ArgumentError("comment must be non-empty")) : Vector{UInt8}(comment)
    cmt !== nothing && (LF in cmt || CR in cmt) &&
        throw(ArgumentError("comment may not contain \\r or \\n"))
    return Dialect(d, oq, cq, e, quoted, cmt, ignoreemptyrows, ignorerepeated, lenient,
                   commentquotes)
end

# Delimiter candidates share the already validated quote/comment options.
function withdelim(d::Dialect, delim::UInt8, ignorerepeated::Bool=d.ignorerepeated)
    d.quoted && delim == d.oq &&
        throw(ArgumentError("delimiter may not equal the quote character"))
    return Dialect(delim, d.oq, d.cq, d.e, d.quoted, d.comment,
                   d.ignoreemptyrows, ignorerepeated, d.lenient, d.commentquotes)
end

withlenient(d::Dialect) = Dialect(d.delim, d.oq, d.cq, d.e, d.quoted, d.comment,
                                  d.ignoreemptyrows, d.ignorerepeated, true, d.commentquotes)
withcommentquotes(d::Dialect) = Dialect(d.delim, d.oq, d.cq, d.e, d.quoted, d.comment,
                                        d.ignoreemptyrows, d.ignorerepeated, d.lenient, true)

# The range planner needs the quote state at the start of each byte range.
# Under the standard quote rule the same byte opens and closes a field and a
# doubled quote is an escape, so every quote byte flips the state and the
# parity of a range's quote count gives its exit state. A separate escape byte
# or distinct open and close bytes need the state machine itself: the planner
# runs it over each range from every possible entry state and composes the
# results in file order. The lenient rule cannot be planned from a range start,
# because a quote there has meaning only at a field start.
symmetricquotes(d::Dialect) = !d.quoted || (d.oq == d.cq && d.e == d.cq)
splittable(d::Dialect) = !d.lenient
# Quote bytes in a comment row do not change the CSV quote state. A byte range
# that starts in the middle of a row cannot know whether that row is a comment.
# The parallel planner and the fast scanner assume comment rows hold no quote
# byte; assembly checks every dropped comment row, and a quote found there
# rebuilds the index serially under `Dialect.commentquotes`.
commentaware(d::Dialect) = d.comment !== nothing
commentserial(d::Dialect) = commentaware(d) && d.commentquotes

# The fast scanner additionally needs a single-byte delimiter.
fasteligible(d::Dialect) = splittable(d) && d.delim isa UInt8 && !commentserial(d)

# These options control how CSV reads one field. Date and time parsing uses a
# compiled pattern. The default patterns accept ISO date, date-time, and time
# text. A user `dateformat` replaces these patterns. Empty true and false lists
# select the standard `true` and `false` text. A sentinel is text that means
# missing. `cellcontent` checks sentinels before it detects or parses a type.
struct ValueOpts
    oq::UInt8
    cq::UInt8
    e::UInt8
    quoted::Bool
    delim::Vector{UInt8}
    decimal::UInt8
    stripws::Bool
    sentinels::Vector{Vector{UInt8}}
    sentfirst::NTuple{4, UInt64}  # first-byte map: skip comparisons for most cells
    hassentinels::Bool            # false: no cell can be a sentinel (one branch per cell)
    trues::Vector{Vector{UInt8}}
    falses::Vector{Vector{UInt8}}
    datepat::Parsers.DatePattern
    datetimepat::Parsers.DatePattern
    datetimespacepat::Parsers.DatePattern   # `yyyy-mm-dd HH:MM:SS`; the T pattern when custom
    timepat::Parsers.DatePattern
    customfmt::Bool
    customkind::UInt8 # 1=date, 2=time, 3=date and time; independent of parser storage
    inferbool::Bool   # false when another type also accepts a user Bool spelling
    groupmark::UInt8  # digit-group separator for numeric cells; 0x00 = off
end

# Parsers returns signed zero or signed infinity with a range code. CSV accepts
# these rounded values.
_fixedfloatusable(rc) =
    rc == Parsers.RC_OK || rc == Parsers.RC_OVERFLOW || rc == Parsers.RC_UNDERFLOW

# Return true when `buf[i:j]` contains `byte`. Check eight bytes at a time when
# the range is long enough. This check avoids copying cells that have no group
# mark.
@inline function _containsbyte(buf::Vector{UInt8}, i::Int, j::Int, byte::UInt8)
    k = i
    if k + 7 <= j
        GC.@preserve buf begin
            p = pointer(buf)
            @inbounds while k + 7 <= j
                _eqmask8_c(ltoh(unsafe_load(Ptr{UInt64}(p + k - 1))), byte) != 0 &&
                    return true
                k += 8
            end
        end
    end
    @inbounds while k <= j
        buf[k] == byte && return true
        k += 1
    end
    return false
end

# Copy one numeric field into `scratch` and remove valid group marks. A group
# mark must be between two digits in the integer part. Return `-1` when the
# field has no group mark. Return `-2` when a group mark is invalid.
function _degroup!(scratch::Vector{UInt8}, buf::Vector{UInt8}, i::Int, j::Int,
                   groupmark::UInt8, decimal::UInt8)
    _containsbyte(buf, i, j, groupmark) || return -1
    n = j - i + 1
    length(scratch) < n && resize!(scratch, max(n, 64))
    copied = 0
    integerpart = true
    @inbounds for k in i:j
        byte = buf[k]
        if byte == groupmark
            integerpart || return -2
            (k > i && (buf[k - 1] - UInt8('0')) <= 0x09 &&
             k < j && (buf[k + 1] - UInt8('0')) <= 0x09) || return -2
        else
            (byte == decimal || byte == UInt8('e') || byte == UInt8('E')) &&
                (integerpart = false)
            copied += 1
            scratch[copied] = byte
        end
    end
    return copied
end

function _bytelist(x, name::Symbol)
    x === nothing && return Vector{UInt8}[]
    x isa AbstractString &&
        throw(ArgumentError("$name must be a collection of strings, not one string"))
    out = Vector{Vector{UInt8}}()
    for s in x
        s isa AbstractString ||
            throw(ArgumentError("$name entries must be strings (got $(typeof(s)))"))
        isempty(s) && throw(ArgumentError("$name cannot contain an empty spelling"))
        push!(out, Vector{UInt8}(codeunits(s)))
    end
    return out
end

function _earlierbooltype(s::Vector{UInt8}, decimal::UInt8,
                          dp::Parsers.DatePattern, dtp::Parsers.DatePattern,
                          tp::Parsers.DatePattern,
                          customfmt::Bool, kind::UInt8, gm::UInt8)
    i, j = 1, length(s)
    if gm != 0x00
        scratch = Vector{UInt8}(undef, 64)
        n = _degroup!(scratch, s, i, j, gm, decimal)
        if n >= 0
            Parsers.parseint(Int64, scratch, 1, n)[2] == Parsers.RC_OK && return Int64
            _fixedfloatusable(Parsers.parsefloat(Float64, scratch, 1, n, decimal)[2]) &&
                return Float64
        end
    end
    Parsers.parseint(Int64, s, i, j)[2] == Parsers.RC_OK && return Int64
    _fixedfloatusable(Parsers.parsefloat(Float64, s, i, j, decimal)[2]) && return Float64
    if customfmt
        c, rc = Parsers.parsecivil(s, i, j, dp)
        if rc == Parsers.RC_OK
            if kind == 0x03
                T = _timestamptype(c)
                return T === String ? nothing : T
            end
            return kind == 0x01 ? Date : Time
        end
    else
        Parsers.parsecivil(s, i, j, dp)[2] == Parsers.RC_OK && return Date
        pat = _spacedatetime(s, i, j) ? _ISO_DATETIME_SPACE_PATTERN : dtp
        c, rc = Parsers.parsecivil(s, i, j, pat)
        if rc == Parsers.RC_OK
            T = _timestamptype(c)
            T === String || return T
        end
        Parsers.parsecivil(s, i, j, tp)[2] == Parsers.RC_OK && return Time
    end
    return nothing
end

# Another type can also accept a user Bool spelling. For example, Int64 accepts
# `"1"`. In this case, CSV does not infer Bool for the column. A user can still
# set the column type to Bool and use the custom spelling. This rule makes the
# result independent of the sampled rows.
function _validatebools(trues, falses, decimal, dp, dtp, tp, customfmt, kind, gm)
    for t in trues, f in falses
        t == f && throw(ArgumentError("Bool spelling $(repr(String(t))) is both true and false"))
    end
    for s in Iterators.flatten((trues, falses))
        _earlierbooltype(s, decimal, dp, dtp, tp, customfmt, kind, gm) === nothing || return false
    end
    return true
end

# Keep type inference metadata from the format spelling. DatePattern is an
# opaque Parsers handle. Backslash runs escape the following character, as in
# Dates and Parsers; escaped token letters are literals.
function _dateformatkind(fmt::AbstractString)
    kind = UInt8(0)
    i = firstindex(fmt)
    while i <= lastindex(fmt)
        c = fmt[i]
        if c == '\\'
            while i <= lastindex(fmt) && fmt[i] == '\\'
                i = nextind(fmt, i)
            end
        else
            c in ('y', 'Y', 'm', 'd', 'u', 'U') && (kind |= 0x01)
            c in ('H', 'M', 'S', 's', 'I', 'p') && (kind |= 0x02)
        end
        i <= lastindex(fmt) && (i = nextind(fmt, i))
    end
    return kind
end

# A `Dates.DateFormat` carries its format string as its first type parameter.
_dateformatstring(fmt::AbstractString) = String(fmt)
_dateformatstring(fmt::DateFormat) = String(typeof(fmt).parameters[1])
_dateformatstring(fmt) =
    throw(ArgumentError("dateformat must be a format String or DateFormat (got $(typeof(fmt)))"))

function makevalueopts(d::Dialect; dateformat=nothing, decimal::Char='.',
                       truestrings=nothing, falsestrings=nothing,
                       stripwhitespace::Bool=false,
                       groupmark::Union{Nothing, Char}=nothing,
                       sentinels=nothing)
    return makevalueopts(d, dateformat, decimal, truestrings, falsestrings,
                         stripwhitespace, groupmark, sentinels)
end

Base.@nospecializeinfer function makevalueopts(d::Dialect, @nospecialize(dateformat), decimal::Char,
                       @nospecialize(truestrings), @nospecialize(falsestrings),
                       stripwhitespace::Bool, groupmark::Union{Nothing, Char},
                       @nospecialize(sentinels))
    isascii(decimal) || throw(ArgumentError("decimal must be ASCII (got $(repr(decimal)))"))
    # A digit, sign, or exponent letter as the decimal separator would make
    # ordinary integers parse as fractions (`decimal='0'` read 105 as 1.5).
    # `decimal == delim` stays legal: such values are only expressible quoted.
    (isdigit(decimal) || decimal in ('+', '-', 'e', 'E') ||
     (d.quoted && decimal % UInt8 in (d.oq, d.cq, d.e))) &&
        throw(ArgumentError("decimal $(repr(decimal)) conflicts with numeric or quote syntax"))
    gm = 0x00
    if groupmark !== nothing
        isascii(groupmark) || throw(ArgumentError("groupmark must be ASCII (got $(repr(groupmark)))"))
        gm = groupmark % UInt8
        (gm == 0x00 || gm - UInt8('0') <= 0x09 || gm == decimal % UInt8 ||
         gm in (UInt8('e'), UInt8('E'), UInt8('+'), UInt8('-'), d.oq, d.cq, d.e)) &&
            throw(ArgumentError("groupmark $(repr(groupmark)) conflicts with numeric or quote syntax"))
        # groupmark == delim is allowed: such fields are only expressible quoted,
        # which the indexer already handles (the mark is content, not structure)
    end
    kind = UInt8(0)
    if dateformat === nothing
        dp, dtp, dtsp, tp, custom = _ISO_DATE_PATTERN, _ISO_DATETIME_PATTERN,
                                    _ISO_DATETIME_SPACE_PATTERN, _ISO_TIME_PATTERN, false
    else
        dateformat = _dateformatstring(dateformat)
        p = Parsers.compilepattern(dateformat)
        kind = _dateformatkind(dateformat)
        kind != 0x00 ||
            throw(ArgumentError("dateformat must contain a date or time token"))
        dp = dtp = dtsp = tp = p
        custom = true
    end
    delimbytes = d.delim isa UInt8 ? [d.delim] : copy(d.delim)
    trues = _bytelist(truestrings, :truestrings)
    falses = _bytelist(falsestrings, :falsestrings)
    sentinelbytes = _bytelist(sentinels, :sentinels)
    if d.quoted
        for s in sentinelbytes, b in s
            b in (d.oq, d.cq, d.e) &&
                throw(ArgumentError("sentinels cannot contain quote or escape characters"))
        end
    end
    inferbool = _validatebools(trues, falses, decimal % UInt8, dp, dtp, tp, custom, kind, gm)
    sf = (zero(UInt64), zero(UInt64), zero(UInt64), zero(UInt64))
    for s in sentinelbytes
        b = s[1]
        sf = Base.setindex(sf, sf[(b >> 6) + 1] | (UInt64(1) << (b & 0x3f)), (b >> 6) + 1)
    end
    return ValueOpts(d.oq, d.cq, d.e, d.quoted, delimbytes, decimal % UInt8, stripwhitespace,
                     sentinelbytes, sf, !isempty(sentinelbytes), trues, falses,
                     dp, dtp, dtsp, tp, custom, kind, inferbool, gm)
end

# ISO date-times separate the date and the time with `T` or a space. The byte
# after the date selects the pattern, so a cell parses once either way.
_spacedatetime(buf::Vector{UInt8}, i::Int, j::Int) =
    j - i >= 10 && @inbounds(buf[i + 10]) == UInt8(' ')
function _datetimepattern(vo::ValueOpts, buf::Vector{UInt8}, i::Int, j::Int)
    vo.customfmt && return vo.datetimepat
    return _spacedatetime(buf, i, j) ? vo.datetimespacepat : vo.datetimepat
end

# `Dates.DateTime` holds milliseconds. A finer fraction has no exact DateTime,
# so an explicit `types=DateTime` column reports such a cell. Inference never
# meets this rule: it infers `Timestamp{Nanosecond}`, which keeps the fraction.
_wholemilliseconds(c::Parsers.CivilParts) = c.nanosecond % 1_000_000 == 0

# --- the cell layer -----------------------------------------------------------
#
# One function turns a raw field span into a *content* span + disposition:
#     CELL_VALUE    content [cpos, cpos+clen) is a present value (maybe escaped)
#     CELL_MISSING  empty / whitespace-stripped-to-empty / sentinel ⇒ missing
#     CELL_BADQUOTE malformed quoting (unterminated, or bytes after the close)
# Rules: outer space/tab around a QUOTED field is structural, never content;
# unquoted whitespace is significant
# unless `stripwhitespace`; a quoted empty field is a present empty string,
# never missing; sentinels match the (possibly unquoted) content exactly.
const CELL_VALUE    = 0x00
const CELL_MISSING  = 0x01
const CELL_BADQUOTE = 0x02

_isot(b::UInt8) = (b == UInt8(' ')) | (b == UInt8('\t'))

# a cell can only be a sentinel if its first byte starts one — one bit test
# replaces the per-cell spelling comparisons (empty sentinel list ⇒ zero map)
_maybesentinel(vo::ValueOpts, b::UInt8) =
    (vo.sentfirst[(b >> 6) + 1] >> (b & 0x3f)) & UInt64(1) != 0

# Typed values and sentinel matching accept surrounding blanks. String columns
# keep these bytes. This helper changes only the span used for value parsing and
# sentinel checks.
@inline function _trimblanks(buf::Vector{UInt8}, i::Int, j::Int)
    @inbounds while i <= j && _isot(buf[i]); i += 1; end
    @inbounds while j >= i && _isot(buf[j]); j -= 1; end
    return i, j
end

# Typed cells ignore surrounding blanks. A cell made only of blanks remains
# present and reaches the scalar parser (for example, `types=Char` accepts " ").
@inline function _typedspan(buf::Vector{UInt8}, i::Int, j::Int)
    ti, tj = _trimblanks(buf, i, j)
    return ti <= tj ? (ti, tj) : (i, j)
end

# A typed value uses decoded content too: a custom quote/escape byte may be
# part of a number, date, or user-defined scalar. Ordinary cells keep their
# original span; only escaped content needs a temporary byte buffer.
@inline function _parsecontent(::Type{T}, buf::Vector{UInt8}, pos::Int, len::Int,
                               escaped::Bool, opts::ValueOpts,
                               scratch::Vector{UInt8}=_scratchfor(opts)) where {T}
    if escaped
        decoded = _unescape_bytes(buf, Int64(pos), Int32(len), opts.e, opts.cq)
        i, j = _typedspan(decoded, 1, length(decoded))
        return parsevalue(T, decoded, i, j, opts, scratch)
    end
    i, j = _typedspan(buf, pos, pos + len - 1)
    return parsevalue(T, buf, i, j, opts, scratch)
end

@inline function _spanmatches(buf::Vector{UInt8}, i::Int, j::Int,
                              choices::Vector{Vector{UInt8}})
    n = j - i + 1
    @inbounds for choice in choices
        length(choice) == n || continue
        k = 1
        while k <= n && buf[i + k - 1] == choice[k]
            k += 1
        end
        k > n && return true
    end
    return false
end

@inline function _matchsentinel(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.hassentinels || return false
    i <= j && _maybesentinel(vo, @inbounds(buf[i])) &&
        _spanmatches(buf, i, j, vo.sentinels) && return true
    ti, tj = _trimblanks(buf, i, j)
    return ti <= tj && (ti != i || tj != j) &&
           _maybesentinel(vo, @inbounds(buf[ti])) &&
           _spanmatches(buf, ti, tj, vo.sentinels)
end

"""
    findcontent(buf, i, j, openquote, closequote, escape)

Find the content bytes in one field. An unquoted field keeps its full span. A
quoted field drops its outer quotes. The returned Boolean is true when the
content contains an escape sequence. The return code is `Parsers.RC_INVALID`
when the closing quote is absent or extra bytes follow it.
"""
function findcontent(buf::Vector{UInt8}, i::Int, j::Int,
                     openquote::UInt8, closequote::UInt8, escape::UInt8)
    @inbounds if i > j || buf[i] != openquote
        return (i, j - i + 1, false, Parsers.RC_OK)
    end

    k = i + 1
    escaped = false
    if escape == closequote
        GC.@preserve buf begin
            p = pointer(buf)
            @inbounds while k <= j
                if k + 7 <= j
                    marks = _eqmask8_c(ltoh(unsafe_load(Ptr{UInt64}(p + k - 1))), closequote)
                    if marks == 0
                        k += 8
                        continue
                    end
                    k += trailing_zeros(marks) >> 3
                else
                    while k <= j && buf[k] != closequote
                        k += 1
                    end
                    k > j && break
                end
                if k < j && buf[k + 1] == closequote
                    escaped = true
                    k += 2
                else
                    return k == j ? (i + 1, j - i - 1, escaped, Parsers.RC_OK) :
                                    (i + 1, j - i - 1, escaped, Parsers.RC_INVALID)
                end
            end
        end
        return (i + 1, j - i, escaped, Parsers.RC_INVALID)
    end

    @inbounds while k <= j
        b = buf[k]
        if b == escape
            escaped = true
            k += 2
        elseif b == closequote
            return k == j ? (i + 1, j - i - 1, escaped, Parsers.RC_OK) :
                            (i + 1, j - i - 1, escaped, Parsers.RC_INVALID)
        else
            k += 1
        end
    end
    return (i + 1, j - i, escaped, Parsers.RC_INVALID)
end

"""
    cellcontent(buf, pos, len, vo) -> (cpos, clen, escaped, disposition)

Turn one raw field span `buf[pos : pos+len-1]` (exactly what the structural
index delimited — quotes and any surrounding blanks included) into the
*content* the value layer should look at:

  * `cpos`, `clen`     the content span `buf[cpos : cpos+clen-1]`: quotes and
                       structural blanks stripped; `clen == 0` for a quoted
                       empty field `""` (a PRESENT empty string, not missing);
  * `escaped`          `true` when the content still contains escape sequences
                       (`""` doubling or backslash-escapes) that must be unescaped before
                       the bytes are the value — typed parsers reject such
                       cells, string cells unescape once at parse time;
  * `disposition`      `CELL_VALUE`    → parse `[cpos, cpos+clen)` as a value
                       `CELL_MISSING`  → empty / stripped-to-empty / sentinel
                                          (`clen` is 0; the caller stores missing)
                       `CELL_BADQUOTE` → malformed quoting (unterminated open
                                          quote, or bytes after the close quote);
                                          `cpos`/`clen` still point at the best-
                                          effort content so diagnostics can
                                          excerpt it.

Examples (default dialect, `stripwhitespace=false`, sentinels `["NA"]`):

    field bytes        → cpos..len  escaped  disposition
    `42`               → `42`       false    CELL_VALUE
    `"a,b"`            → `a,b`      false    CELL_VALUE       (quotes stripped)
    `"say ""hi"" now"` → `say ""hi"" now` true  CELL_VALUE   (needs unescape)
    `  "x"  `          → `x`        false    CELL_VALUE       (outer blanks structural)
    `""`               → ``  (0)    false    CELL_VALUE       (present empty string)
    ``                 → (0)        false    CELL_MISSING
    `NA`               → (0)        false    CELL_MISSING     (sentinel)
    `"NA"`             → (0)        false    CELL_MISSING     (sentinel inside quotes)
    `"unterminated`    → …          false    CELL_BADQUOTE
    `"x"y`             → `x`        false    CELL_BADQUOTE    (bytes after close)

With `stripwhitespace=true`, unquoted blanks are stripped too (`  7  ` → `7`)
and blanks inside quotes are stripped as content (`"  x  "` → `x`).
"""
@inline function cellcontent(buf::Vector{UInt8}, pos::Int, len::Int, vo::ValueOpts)
    i, j = pos, pos + len - 1
    @inbounds begin
        if vo.stripws
            while i <= j && _isot(buf[i]); i += 1; end
            while j >= i && _isot(buf[j]); j -= 1; end
        end
        i > j && return (i, 0, false, CELL_MISSING)
        if vo.quoted
            ii, jj = i, j
            while ii <= jj && _isot(buf[ii]); ii += 1; end
            if ii <= jj && buf[ii] == vo.oq
                while jj > ii && _isot(buf[jj]); jj -= 1; end
                cpos, clen, esc, rc = findcontent(buf, ii, jj, vo.oq, vo.cq, vo.e)
                rc == Parsers.RC_OK || return (cpos, clen, esc, CELL_BADQUOTE)
                if vo.stripws
                    cj = cpos + clen - 1
                    while cpos <= cj && _isot(buf[cpos]); cpos += 1; end
                    while cj >= cpos && _isot(buf[cj]); cj -= 1; end
                    clen = cj - cpos + 1
                end
                if clen > 0
                    sentinel = if esc && vo.hassentinels
                        decoded = _unescape_bytes(buf, Int64(cpos), Int32(clen), vo.e, vo.cq)
                        _matchsentinel(decoded, 1, length(decoded), vo)
                    else
                        !esc && _matchsentinel(buf, cpos, cpos + clen - 1, vo)
                    end
                    sentinel && return (cpos, 0, false, CELL_MISSING)
                end
                return (cpos, clen, esc, CELL_VALUE)
            end
        end
        _matchsentinel(buf, i, j, vo) &&
            return (i, 0, false, CELL_MISSING)
        return (i, j - i + 1, false, CELL_VALUE)
    end
end

# --- typed value dispatch ------------------------------------------------------
#
# `parsevalue(T, buf, i, j, vo) -> (value, ok)` reads one content span.
# It accepts the same forms that `detecttype` accepts. Boolean values use
# `true`, `false`, or a user list. Date and time values must use the selected
# format and consume the full span. The inferred type does not depend on which
# rows are in the sample.
const _DATE0 = Date(1)
const _DATETIME0 = DateTime(1)
const _TIME0 = Time(0)
# Stay within Dates' advertised calendar range. Its constructors accept wider
# years, but their unchecked instant arithmetic can wrap into a different date.
const _DATEYEARS = (year(typemin(Date)), year(typemax(Date)))
const _DATETIMEYEARS = (year(typemin(DateTime)), year(typemax(DateTime)))
_timestamp0(::Type{Timestamp{P}}) where {P} = Timestamp{P}(Dates.UTInstant(P(0)))

# Parsers returns calendar fields without choosing a Dates representation. CSV
# owns this conversion because it chooses the final column type.
# Parsers validated the calendar fields, so the instants build from rata days
# directly (`Date(y, m, d)` would re-run `validargs` on every cell).
todate(c::Parsers.CivilParts) =
    Date(Dates.UTD(Dates.totaldays(Int64(c.year), Int64(c.month), Int64(c.day))))

@inline function todatetime(c::Parsers.CivilParts)
    milliseconds = Int64(c.nanosecond) ÷ 1_000_000
    days = Dates.totaldays(Int64(c.year), Int64(c.month), Int64(c.day))
    ms = ((days * 24 + Int64(c.hour)) * 60 + Int64(c.minute)) * 60_000 +
         Int64(c.second) * 1_000 + milliseconds
    return DateTime(Dates.UTM(ms))
end

@inline totime(c::Parsers.CivilParts) =
    Time(Dates.Nanosecond(((Int64(c.hour) * 60 + c.minute) * 60 + c.second) *
                          1_000_000_000 + c.nanosecond))

# A `Timestamp{P}` is an Int64 count of `P` since the Unix epoch. Parsers has
# already validated the calendar fields, so the instant is built from rata
# days and nanoseconds of the day with overflow-checked Int64 arithmetic: a
# fraction that is not a whole number of `P`, or an instant outside the Int64
# tick range, is not a `Timestamp{P}`. This avoids `Dates.validargs` (which
# recomputes `year(typemin(...))` on every call) and Int128 arithmetic.
_tickscale(::Type{Dates.Nanosecond}) = Int64(1)
_tickscale(::Type{Dates.Microsecond}) = Int64(1_000)
_tickscale(::Type{Dates.Millisecond}) = Int64(1_000_000)
_tickscale(::Type{Dates.Second}) = Int64(1_000_000_000)
const _UNIXEPOCHDAYS = Int64(Dates.UNIXEPOCH ÷ 86_400_000)   # rata days of 1970-01-01
# Seconds have the widest supported range. Cache this coarse guard so even
# Int64 calendar years cannot overflow totaldays before the checked tick math.
const _TIMESTAMPYEARS = (year(typemin(Timestamp{Dates.Second})),
                         year(typemax(Timestamp{Dates.Second})))
@inline function totimestamp(::Type{Timestamp{P}}, c::Parsers.CivilParts) where {P}
    _TIMESTAMPYEARS[1] <= c.year <= _TIMESTAMPYEARS[2] ||
        return (_timestamp0(Timestamp{P}), false)
    scale = _tickscale(P)
    nsofday = ((Int64(c.hour) * 60 + Int64(c.minute)) * 60 + Int64(c.second)) * 1_000_000_000 +
              Int64(c.nanosecond)
    tickofday, rem = divrem(nsofday, scale)
    rem == 0 || return (_timestamp0(Timestamp{P}), false)
    days = Dates.totaldays(Int64(c.year), Int64(c.month), Int64(c.day)) - _UNIXEPOCHDAYS
    ticksperday = 86_400_000_000_000 ÷ scale
    ticks, overflow = Base.mul_with_overflow(days, ticksperday)
    overflow || ((ticks, overflow) = Base.add_with_overflow(ticks, tickofday))
    if overflow
        # the first or last day of the range: the day product alone overflows
        wide = Int128(days) * ticksperday + tickofday
        typemin(Int64) <= wide <= typemax(Int64) || return (_timestamp0(Timestamp{P}), false)
        ticks = Int64(wide)
    end
    return (Timestamp{P}(Dates.UTInstant(P(ticks))), true)
end
# Inference prefers nanoseconds. Every instant of a year strictly inside the
# nanosecond range fits, so the type follows from the year alone; only the two
# boundary years (1677 and 2262) need the exact instant. An instant outside
# the range (`9999-12-31` sentinels) widens to microseconds, the way an Int64
# overflow widens to Int128.
const _NANOSECONDYEARS = (year(typemin(Timestamp{Dates.Nanosecond})),
                          year(typemax(Timestamp{Dates.Nanosecond})))
function _timestamptype(c::Parsers.CivilParts)
    _NANOSECONDYEARS[1] < c.year < _NANOSECONDYEARS[2] && return Timestamp{Dates.Nanosecond}
    (c.year == _NANOSECONDYEARS[1] || c.year == _NANOSECONDYEARS[2]) &&
        totimestamp(Timestamp{Dates.Nanosecond}, c)[2] && return Timestamp{Dates.Nanosecond}
    totimestamp(Timestamp{Dates.Microsecond}, c)[2] && return Timestamp{Dates.Microsecond}
    return String
end

# Numeric parsers take a scratch buffer so grouped digits (groupmark) degroup
# without per-cell allocation; the hot loops pass a per-(column × chunk)
# scratch, and the 5-arg convenience forms below allocate one lazily. With
# groupmark off, the extra argument is dead and the parsers run untouched.
@inline function parsevalue(::Type{Int64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, 0xff)
        n == -2 && return (Int64(0), false)
        if n >= 0
            v, rc = Parsers.parseint(Int64, scratch, 1, n)
            return (v, rc == Parsers.RC_OK)
        end
    end
    v, rc = Parsers.parseint(Int64, buf, i, j)
    return (v, rc == Parsers.RC_OK)
end

@inline function parsevalue(::Type{Int128}, buf::Vector{UInt8}, i::Int, j::Int,
                            vo::ValueOpts, scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, 0xff)
        n == -2 && return (Int128(0), false)
        if n >= 0
            v, rc = Parsers.parseint(Int128, scratch, 1, n)
            return (v, rc == Parsers.RC_OK)
        end
    end
    v, rc = Parsers.parseint(Int128, buf, i, j)
    return (v, rc == Parsers.RC_OK)
end

@inline function parsevalue(::Type{Float64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (0.0, false)
        if n >= 0
            v, rc = Parsers.parsefloat(Float64, scratch, 1, n, vo.decimal)
            return (v, _fixedfloatusable(rc))
        end
    end
    v, rc = Parsers.parsefloat(Float64, buf, i, j, vo.decimal)
    return (v, _fixedfloatusable(rc))
end
parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                   scratch::Vector{UInt8}) where {T} = parsevalue(T, buf, i, j, vo)

# Narrow numeric requests use the native integer/float parsers, then convert at
# the API boundary. Keep that rule available to lazy and row readers too:
# calling `tryparse(Int8, String(...))` here would lose decimal/groupmark
# handling and would allocate one String per cell.
const NarrowParseType = Union{Int8, Int16, Int32,
                              UInt8, UInt16, UInt32, UInt64,
                              Float16, Float32}
_narrowbase(::Type{<:Union{Int8, Int16, Int32,
                                   UInt8, UInt16, UInt32}}) = Int64
_narrowbase(::Type{UInt64}) = Int128
_narrowbase(::Type{<:Union{Float16, Float32}}) = Float64
function _narrowvalue(::Type{T}, value, ok::Bool) where {T <: NarrowParseType}
    ok || return (zero(T), false)
    T <: Integer && !(typemin(T) <= value <= typemax(T)) && return (zero(T), false)
    return (convert(T, value), true)
end

function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                            vo::ValueOpts, scratch::Vector{UInt8}) where {T <: NarrowParseType}
    value, ok = parsevalue(_narrowbase(T), buf, i, j, vo, scratch)
    return _narrowvalue(T, value, ok)
end
# CSV uses these types only when the user requests them. Type inference does not
# select them.
function _parsebigint_direct(buf::Vector{UInt8}, i::Int, j::Int)
    v, rc = Parsers.parsebigint(buf, i, j)
    return (v, rc == Parsers.RC_OK)
end

@inline function parsevalue(::Type{BigInt}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, 0xff)
        n == -2 && return (BigInt(0), false)
        n >= 0 && return _parsebigint_direct(scratch, 1, n)
    end
    return _parsebigint_direct(buf, i, j)
end

function _parsebigfloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    value = Parsers.tryparse(BigFloat, buf, i, j; decimal=Char(vo.decimal))
    return value === nothing ? (BigFloat(0), false) : (value, true)
end

@inline function parsevalue(::Type{BigFloat}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                            scratch::Vector{UInt8})
    if vo.groupmark != 0x00
        n = _degroup!(scratch, buf, i, j, vo.groupmark, vo.decimal)
        n == -2 && return (BigFloat(0), false)
        n >= 0 && return _parsebigfloat_direct(scratch, 1, n, vo)
    end
    return _parsebigfloat_direct(buf, i, j, vo)
end

function parsevalue(::Type{Base.UUID}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    u, rc = Parsers.parseuuid(buf, i, j)
    return (Base.UUID(u), rc == Parsers.RC_OK)
end

_scratchfor(vo::ValueOpts) = vo.groupmark == 0x00 ? EMPTY_BYTES : Vector{UInt8}(undef, 64)
parsevalue(::Type{Int64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int64, buf, i, j, vo, _scratchfor(vo))
parsevalue(::Type{Int128}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int128, buf, i, j, vo, _scratchfor(vo))
@inline parsevalue(::Type{BigInt}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsebigint_direct(buf, i, j) :
                           parsevalue(BigInt, buf, i, j, vo, Vector{UInt8}(undef, 64))
function _parsefloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    v, rc = Parsers.parsefloat(Float64, buf, i, j, vo.decimal)
    return (v, _fixedfloatusable(rc))
end

@inline parsevalue(::Type{Float64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsefloat_direct(buf, i, j, vo) :
                           parsevalue(Float64, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline parsevalue(::Type{BigFloat}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsebigfloat_direct(buf, i, j, vo) :
                           parsevalue(BigFloat, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                   vo::ValueOpts) where {T <: NarrowParseType} =
    parsevalue(T, buf, i, j, vo, _scratchfor(vo))
# Default Boolean spellings: `true`, `True`, `TRUE` and `false`, `False`,
# `FALSE`. One unaligned word compare per spelling: no table, no case fold.
_load32(buf::Vector{UInt8}, i::Int) =
    GC.@preserve buf unsafe_load(Ptr{UInt32}(pointer(buf, i)))
_word32(s::String) = _load32(Vector{UInt8}(codeunits(s)), 1)
const _TRUE_WORDS = (_word32("true"), _word32("True"), _word32("TRUE"))
const _FALSE_WORDS = (_word32("fals"), _word32("Fals"), _word32("FALS"))
@inline function _parsebool(buf::Vector{UInt8}, i::Int, j::Int)
    n = j - i + 1
    if n == 4
        w = _load32(buf, i)
        (w == _TRUE_WORDS[1] || w == _TRUE_WORDS[2] || w == _TRUE_WORDS[3]) &&
            return (true, true)
    elseif n == 5
        w = _load32(buf, i)
        last = @inbounds buf[j]
        (((w == _FALSE_WORDS[1] || w == _FALSE_WORDS[2]) && last == UInt8('e')) ||
         (w == _FALSE_WORDS[3] && last == UInt8('E'))) && return (false, true)
    end
    return (false, false)
end

@inline function parsevalue(::Type{Bool}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    isempty(vo.trues) && isempty(vo.falses) && return _parsebool(buf, i, j)
    _spanmatches(buf, i, j, vo.trues) && return (true, true)
    _spanmatches(buf, i, j, vo.falses) && return (false, true)
    return (false, false)
end
# A Char cell is exactly one Unicode scalar.
@inline function parsevalue(::Type{Char}, buf::Vector{UInt8}, i::Int, j::Int, ::ValueOpts)
    n = j - i + 1
    1 <= n <= 4 || return ('\0', false)
    b1 = @inbounds buf[i]
    len = b1 < 0x80 ? 1 : b1 < 0xc2 ? 0 : b1 < 0xe0 ? 2 : b1 < 0xf0 ? 3 : b1 < 0xf5 ? 4 : 0
    len == n || return ('\0', false)
    u = UInt32(b1) << 24
    @inbounds for k in 1:(n - 1)
        b = buf[i + k]
        (b & 0xc0) == 0x80 || return ('\0', false)
        u |= UInt32(b) << (24 - 8k)
    end
    c = reinterpret(Char, u)
    return (c, isvalid(c))
end

@inline function parsevalue(::Type{Date}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && vo.customkind != 0x01 && return (_DATE0, false)
    c, rc = Parsers.parsecivil(buf, i, j, vo.datepat)
    rc == Parsers.RC_OK && _DATEYEARS[1] <= c.year <= _DATEYEARS[2] ||
        return (_DATE0, false)
    return (todate(c), true)
end

@inline function parsevalue(::Type{DateTime}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && vo.customkind != 0x03 && return (_DATETIME0, false)
    c, rc = Parsers.parsecivil(buf, i, j, _datetimepattern(vo, buf, i, j))
    rc == Parsers.RC_OK && _wholemilliseconds(c) &&
        _DATETIMEYEARS[1] <= c.year <= _DATETIMEYEARS[2] || return (_DATETIME0, false)
    return (todatetime(c), true)
end

@inline function parsevalue(::Type{Timestamp{P}}, buf::Vector{UInt8}, i::Int, j::Int,
                            vo::ValueOpts) where {P}
    vo.customfmt && vo.customkind != 0x03 && return (_timestamp0(Timestamp{P}), false)
    c, rc = Parsers.parsecivil(buf, i, j, _datetimepattern(vo, buf, i, j))
    rc == Parsers.RC_OK || return (_timestamp0(Timestamp{P}), false)
    return totimestamp(Timestamp{P}, c)
end

@inline function parsevalue(::Type{Time}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && vo.customkind != 0x02 && return (_TIME0, false)
    c, rc = Parsers.parsecivil(buf, i, j, vo.timepat)
    rc == Parsers.RC_OK || return (_TIME0, false)
    return (totime(c), true)
end

# User-defined scalar types. A type parses through `Parsers.tryparse` on the
# field bytes when it defines that method (no copy), and otherwise through
# `Base.tryparse` on a `String` made from the field bytes. A type with neither
# method is rejected when the column plan is settled, so a column can never
# come back all missing because no parser existed. A parser that throws
# aborts the read: a `tryparse` method must return `nothing` for text it
# cannot parse. The method lookup runs once per type: readers take the
# published dictionary without a lock, and a miss publishes a new one.
const _SPANPARSERS = Ref(Base.ImmutableDict{Type, Bool}())
const _SPANPARSERS_LOCK = ReentrantLock()

function _usesspanparser(::Type{T}) where {T}
    r = get(_SPANPARSERS[], T, nothing)
    r === nothing || return r
    return _registerspanparser(T)
end

@noinline function _registerspanparser(::Type{T}) where {T}
    lock(_SPANPARSERS_LOCK)
    try
        d = _SPANPARSERS[]
        cached = get(d, T, nothing)
        cached === nothing || return cached
        r = _hasspanparser(T)
        _SPANPARSERS[] = Base.ImmutableDict(d, T => r)
        return r
    finally
        unlock(_SPANPARSERS_LOCK)
    end
end

function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, ::ValueOpts) where {T}
    return _usesspanparser(T) ? _parsecustom(T, buf, i, j, Val(:parsers)) :
                                _parsecustom(T, buf, i, j, Val(:base))
end

function _parsecustom(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, ::Val{:parsers}) where {T}
    v = Parsers.tryparse(T, buf, i, j)
    return (v, v isa T)
end

function _parsecustom(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, ::Val{:base}) where {T}
    s = GC.@preserve buf unsafe_string(pointer(buf, i), j - i + 1)
    v = Base.tryparse(T, s)
    return (v, v isa T)
end

# ---------------------------------------------------------------------------
# L1: the row and field index.
#
# The scanners store one UInt32 for each delimiter or row ending:
#     (relpos << 2) | kind
#     kind: 0 = delimiter, 1 = CR, 2 = LF, 3 = CRLF at the CR
# The stored position is relative to the chunk start. `assemblerows!` then reads
# this list once. It finds row boundaries and removes comment rows and empty
# rows. The byte scanner does not do this row work.
#
# After assembly, tape kinds become: 0 = delimiter (next field starts
# `delimskip` bytes later), 1 = row end (+1 byte), 2 = row end (+2 bytes, CRLF).
# Every event closes exactly one field, so a row's field count is its event count.
# A stored relative position must be less than 2^30. A very large row can exceed
# this limit. The parser rejects that row before it builds the index.
# ---------------------------------------------------------------------------

mutable struct ChunkIndex
    start::Int                  # absolute (1-based) byte offset of the chunk in buf
    stop::Int                   # absolute offset of the chunk's last byte
    tape::Vector{UInt32}        # (relpos << 2) | kind, one per field-closing event
    ext::Vector{UInt32}         # ignorerepeated only (else empty): extra delimiters
                                # each kept delimiter event swallowed (its run - 1)
    rowfirst::Vector{Int32}     # rowfirst[r]..rowfirst[r+1]-1 index `tape` for row r
    rowstartrel::Vector{UInt32} # chunk-relative byte offset of each surviving row's start
    delimskip::Int              # bytes a delimiter event consumes (multi-byte delims)
    firstdatarow::Int           # local row where data begins (2 when this chunk holds the header row)
    unclosedquote::Bool         # buffer ended while inside a quoted field (malformed input)
    barequote::Bool             # an opening quote was not the first non-blank byte of its field
    rawrows::Int                # row-end events the scanner emitted (assembly sizes its
                                # row vectors from it instead of growing them)
    commentquote::Bool          # a dropped comment row held a quote byte (the fast
                                # scan and the parallel plan may then be wrong)
end

ChunkIndex(start::Int, stop::Int) =
    ChunkIndex(start, stop, UInt32[], UInt32[], Int32[1], UInt32[], 1, 1, false, false, 0,
               false)

nrows(ci::ChunkIndex) = length(ci.rowfirst) - 1 - (ci.firstdatarow - 1)
totalrows(ci::ChunkIndex) = length(ci.rowfirst) - 1
nfields(ci::ChunkIndex, localrow::Int) = Int(ci.rowfirst[localrow + 1] - ci.rowfirst[localrow])

# Absolute (pos, len) of field `col` in local row `localrow`, or `nothing` when the
# row is too short (ragged input). Field col is closed by the row's col-th event;
# it starts at the row start (col == 1) or just past the previous event.
@inline function fieldspan(ci::ChunkIndex, localrow::Int, col::Int)
    @boundscheck 1 <= localrow <= totalrows(ci) || throw(BoundsError(ci, localrow))
    @boundscheck col >= 1 || throw(BoundsError(ci, (localrow, col)))
    @inbounds first = Int(ci.rowfirst[localrow])
    @inbounds nextr = Int(ci.rowfirst[localrow + 1])
    return _span(ci.start, ci.tape, ci.rowstartrel, ci.ext, ci.delimskip, first, nextr,
                 localrow, col)
end

# The same span from hoisted index fields and the row's event bounds
# `tape[first : nextr - 1]`. The column loops carry `nextr` into the next
# row's `first`, so each cell reads one row-bound word and its two events
# instead of re-reading the chunk's fields.
@inline function _span(start::Int, tape::Vector{UInt32}, rowstartrel::Vector{UInt32},
                       ext::Vector{UInt32}, delimskip::Int,
                       first::Int, nextr::Int, lr::Int, col::Int)
    col <= nextr - first || return nothing
    fi = first + col - 1
    @inbounds stop = start + Int(tape[fi] >> 2) - 1
    if col == 1
        @inbounds s = start + Int(rowstartrel[lr])
    else
        @inbounds e = tape[fi - 1]
        k = e & 0x03
        skip = delimskip
        # ignorerepeated: the previous event closed a run of 1 + ext delimiters
        k == 0x00 && !isempty(ext) && (skip += skip * Int(@inbounds ext[fi - 1]))
        s = start + Int(e >> 2) + (k == 0x00 ? skip : Int(k))
    end
    return (s, stop - s + 1)
end

struct BufferIndex
    chunks::Vector{ChunkIndex}
    nrows::Int                  # total rows across chunks (header still included at this layer)
    unclosedquote::Bool         # input ended inside a quoted field (captured before empty-chunk filtering)
    # A quote that did not start its field (`5' 11"`, `x"y`) toggled the
    # structural scan, so this index may have merged rows: the reader must
    # rebuild it under the lenient quote rule before trusting it.
    barequote::Bool
end

# --- tape plumbing -----------------------------------------------------------

const MAX_TAPE_HINT = 1 << 20   # initial-capacity cap: a giant single row spans
                                # many bytes but holds few events
const MAX_TAPE_RELPOS = Int(typemax(UInt32) >> 2)

function tape_room!(tape::Vector{UInt32}, n::Int, extra::Int)
    length(tape) < n + extra && resize!(tape, max(2 * length(tape), n + extra + 256))
    return tape
end

function checktaperange(ci::ChunkIndex)
    ci.stop - ci.start < MAX_TAPE_RELPOS ||
        throw(ArgumentError("a single row is 1 GiB or larger and is not supported"))
    return ci
end

# raw event kinds during scanning
rawkind(b::UInt8) = UInt32((b == CR) + 2 * (b == LF))   # 0 delim, 1 CR, 2 LF, 3 CRLF (pre-paired)

# --- build rows from stored events ------------------------------------------
#
# Read the stored events in place. Remove comment rows and, when requested,
# empty rows. Store the start of each remaining row. Read input bytes only when
# a row can start with the comment prefix.
function assemblerows!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int)
    d.ignorerepeated && return assemblecollapsed!(ci, buf, d, n)
    tape = ci.tape
    ci.delimskip = d.delim isa UInt8 ? 1 : length(d.delim::Vector{UInt8})
    rowfirst = ci.rowfirst
    rowstartrel = ci.rowstartrel
    # exact capacity from the scanner's row-end count: no growth, no copies
    resize!(rowfirst, ci.rawrows + 1); @inbounds rowfirst[1] = Int32(1)
    resize!(rowstartrel, ci.rawrows)
    r = 0                  # rows kept so far
    cmt = d.comment
    w = 0
    roweventw = 1          # tape index where the current row's events begin
    rowstart = ci.start    # absolute byte where the current row begins
    i = 1
    @inbounds while i <= n
        e = tape[i]
        k = e & 0x03
        if k == 0x00                       # delimiter: field boundary, row continues
            w += 1
            tape[w] = e
            i += 1
        else                               # row end: scanners pre-pair CRLF (kind 3)
            pos = ci.start + Int(e >> 2)
            wide = k == 0x03
            w += 1
            tape[w] = (e & ~UInt32(0x03)) | (wide ? UInt32(2) : UInt32(1))
            i += 1
            nextrow = pos + (wide ? 2 : 1)
            # Decide whether to keep this row. Do not scan its bytes again.
            drop = false
            if d.ignoreemptyrows && w == roweventw && pos == rowstart
                drop = true                # a row that is one empty field
            elseif cmt !== nothing && rowstart + length(cmt) - 1 <= length(buf)
                # a terminator byte can never match a comment byte (validated in
                # Dialect), so this compare cannot leak past the row
                match = true
                for c in eachindex(cmt)
                    if buf[rowstart + c - 1] != cmt[c]
                        match = false
                        break
                    end
                end
                drop = match
                match && d.quoted && pos > rowstart &&
                    _containsbyte(buf, rowstart, pos - 1, d.oq) && (ci.commentquote = true)
            end
            if drop
                w = roweventw - 1
            else
                r += 1
                rowstartrel[r] = UInt32(rowstart - ci.start)
                rowfirst[r + 1] = Int32(w + 1)
                roweventw = w + 1
            end
            rowstart = nextrow
        end
    end
    resize!(tape, w)
    resize!(rowfirst, r + 1)
    resize!(rowstartrel, r)
    return ci
end

# `assemblerows!` under ignorerepeated: adjacent delimiter events collapse into
# one field boundary. The kept event is the run's FIRST delimiter (so the field
# before it stops cleanly) and `ext[w]` records how many extra delimiters the
# run swallowed (so `fieldspan` starts the next field past the whole run). A
# run at the row start is pure padding — it advances the row's field start and
# emits nothing. A run touching the row end collapses into it: the kept run
# event is dropped and the row-end event takes the run's first-delimiter
# position, excluding the padding from the last field (its kind bits are
# unread past assembly — only its relpos is, as that field's stop).
# Use the original row start for these checks. A row that contains only
# delimiters has one empty field. It is not an empty row. A comment prefix must
# start at the first byte of the row.
function assemblecollapsed!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int)
    tape = ci.tape
    skip = ci.delimskip = d.delim isa UInt8 ? 1 : length(d.delim::Vector{UInt8})
    ext = ci.ext
    length(ext) < n && resize!(ext, n)
    rowfirst = ci.rowfirst
    rowstartrel = ci.rowstartrel
    resize!(rowfirst, ci.rawrows + 1); @inbounds rowfirst[1] = Int32(1)
    resize!(rowstartrel, ci.rawrows)
    r = 0
    cmt = d.comment
    w = 0
    roweventw = 1          # tape index where the current row's events begin
    rowstart = ci.start    # original row start for comment and empty-row checks
    fieldstart = ci.start  # row start advanced past leading delimiter padding
    runend = 0             # absolute byte just past the last kept event's run
    i = 1
    @inbounds while i <= n
        e = tape[i]
        k = e & 0x03
        if k == 0x00                       # delimiter
            pos = ci.start + Int(e >> 2)
            if w < roweventw && pos == fieldstart
                fieldstart = pos + skip    # leading padding: no boundary yet
            elseif w >= roweventw && (tape[w] & 0x03) == 0x00 && pos == runend
                ext[w] += UInt32(1)        # extends the previous run
            else
                w += 1
                tape[w] = e
                ext[w] = UInt32(0)
            end
            runend = pos + skip
            i += 1
        else                               # row end: scanners pre-pair CRLF (kind 3)
            pos = ci.start + Int(e >> 2)
            wide = k == 0x03
            endrel = e & ~UInt32(0x03)
            if w >= roweventw && (tape[w] & 0x03) == 0x00 && pos == runend
                endrel = tape[w] & ~UInt32(0x03)   # trailing padding: run folds
                w -= 1                             # into the row end
            end
            w += 1
            tape[w] = endrel | (wide ? UInt32(2) : UInt32(1))
            ext[w] = UInt32(0)
            i += 1
            nextrow = pos + (wide ? 2 : 1)
            drop = false
            if d.ignoreemptyrows && w == roweventw && pos == rowstart
                drop = true                # a row that is zero bytes
            elseif cmt !== nothing && rowstart + length(cmt) - 1 <= length(buf)
                match = true
                for c in eachindex(cmt)
                    if buf[rowstart + c - 1] != cmt[c]
                        match = false
                        break
                    end
                end
                drop = match
                match && d.quoted && pos > rowstart &&
                    _containsbyte(buf, rowstart, pos - 1, d.oq) && (ci.commentquote = true)
            end
            if drop
                w = roweventw - 1
            else
                r += 1
                rowstartrel[r] = UInt32(fieldstart - ci.start)
                rowfirst[r + 1] = Int32(w + 1)
                roweventw = w + 1
            end
            rowstart = fieldstart = nextrow
        end
    end
    resize!(tape, w)
    resize!(ext, w)
    resize!(rowfirst, r + 1)
    resize!(rowstartrel, r)
    return ci
end

# End-of-chunk: synthesize a row end when the chunk does not finish on one — a
# trailing unterminated row ("a,b"), a trailing empty field ("a,b,"), or an
# unclosed quote running to EOF.
# Without the scanner's count, derive the row-end events from the tape.
function finishscan!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int, inquote::Bool)
    rows = 0
    tape = ci.tape
    @inbounds for i in 1:n
        rows += (tape[i] & 0x03) != 0x00
    end
    return finishscan!(ci, buf, d, n, inquote, rows)
end

function finishscan!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int, inquote::Bool,
                     rows::Int)
    start, stop = ci.start, ci.stop
    needsend = if n == 0
        stop >= start
    else
        e = @inbounds ci.tape[n]
        # a pre-paired CRLF event sits at the CR; its row end is the LF byte
        (e & 0x03) == 0x00 ||
            ci.start + Int(e >> 2) + ((e & 0x03) == 0x03 ? 1 : 0) < stop
    end
    if needsend
        tape_room!(ci.tape, n, 1)
        n += 1
        @inbounds ci.tape[n] = (UInt32(stop + 1 - start) << 2) | UInt32(2)  # LF-kind at EOF
        rows += 1
    end
    ci.unclosedquote = inquote
    ci.rawrows = rows
    assemblerows!(ci, buf, d, n)
    return ci
end

# --- scalar scanner ---------------------------------------------------------
#
# Read one byte at a time. This scanner supports every option, including a
# multi-byte delimiter and a comment row that holds a quote byte, which the
# fast scanner does not take, and it is the reference the fast scanner must
# agree with. Each chunk starts at a complete row, so this scan always starts
# outside a quoted field.

function indexchunk_scalar!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    start, stop = ci.start, ci.stop
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    delim = d.delim
    tape = ci.tape
    n = 0
    pos = start
    inquote = false
    fieldstart = true      # only blanks seen since the row start or the last delimiter
    bare = false
    cmt = d.comment
    atrowstart = true      # comment rows are skipped whole: their bytes are not structural
    rows = 0
    @inbounds while pos <= stop
        if atrowstart && cmt !== nothing && !inquote &&
           pos + length(cmt) - 1 <= stop && _matchbytes(buf, pos, cmt)
            # consume through the row terminator, emitting the row-end event
            # only (assembly drops the comment row by its start bytes)
            while pos <= stop && buf[pos] != LF && buf[pos] != CR
                pos += 1
            end
            pos > stop && break
            b = buf[pos]
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            rows += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            continue
        end
        atrowstart = false
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2                       # escape consumes the next byte
            elseif b == cq
                if e == cq && pos < stop && buf[pos + 1] == cq
                    pos += 2                   # "" = escaped quote, still inside
                else
                    inquote = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif quoted && b == oq
            # structural rule: any quote toggles. A quote that is not the first
            # non-blank byte of its field (and not the second of a pair) means
            # the toggle reading is unsound for this input: flag it so the
            # reader rebuilds the index under the lenient rule.
            bare |= !fieldstart && !(pos > start && buf[pos - 1] == oq)
            inquote = true
            fieldstart = false
            pos += 1
        elseif delim isa UInt8 ? b == delim :
               (b == delim[1] && pos + length(delim) - 1 <= stop && _matchbytes(buf, pos, delim))
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = UInt32(pos - start) << 2         # kind 0
            pos += delim isa UInt8 ? 1 : length(delim)
            fieldstart = true
        elseif b == LF || b == CR
            # CR immediately followed by LF emits ONE pre-paired event (kind 3):
            # half the row-end tape traffic, and assembly needs no pairing pass
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            rows += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            fieldstart = true
            atrowstart = true
        else
            fieldstart &= _isblank(b)
            pos += 1
        end
    end
    ci.barequote = bare
    return finishscan!(ci, buf, d, n, inquote, rows)
end

@inline function _matchbytes(buf::Vector{UInt8}, pos::Int, bytes::Vector{UInt8})
    @inbounds for k in eachindex(bytes)
        buf[pos + k - 1] == bytes[k] || return false
    end
    return true
end

# --- lenient scanner --------------------------------------------------------
#
# The field-start quote rule: a quote opens a field only as the field's first byte
# (after optional blanks) and closes only when it is not doubled; everything
# after the closing quote up to the delimiter or row end belongs to the field
# (the value layer reports it as malformed quoting). A quote anywhere else is
# content. Range starts cannot be derived from quote counts under this rule,
# so the planner walks rows serially and each chunk is scanned here.

_isblank(b::UInt8) = b == UInt8(' ') || b == UInt8('\t')

function indexchunk_lenient!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    start, stop = ci.start, ci.stop
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    delim = d.delim
    tape = ci.tape
    n = 0
    pos = start
    inquote = false
    fieldstart = true      # only blanks seen since the row start or the last delimiter
    cmt = d.comment
    atrowstart = true
    rows = 0
    @inbounds while pos <= stop
        if atrowstart && cmt !== nothing &&
           pos + length(cmt) - 1 <= stop && _matchbytes(buf, pos, cmt)
            while pos <= stop && buf[pos] != LF && buf[pos] != CR
                pos += 1
            end
            pos > stop && break
            b = buf[pos]
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            rows += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            continue
        end
        atrowstart = false
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2
            elseif b == cq
                if e == cq && pos < stop && buf[pos + 1] == cq
                    pos += 2
                else
                    inquote = false               # the rest of the field is content
                    pos += 1
                end
            else
                pos += 1
            end
        elseif delim isa UInt8 ? b == delim :
               (b == delim[1] && pos + length(delim) - 1 <= stop && _matchbytes(buf, pos, delim))
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = UInt32(pos - start) << 2
            pos += delim isa UInt8 ? 1 : length(delim)
            fieldstart = true
        elseif b == LF || b == CR
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            rows += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            fieldstart = true
            atrowstart = true
        elseif fieldstart && quoted && b == oq
            inquote = true
            fieldstart = false
            pos += 1
        else
            fieldstart &= _isblank(b)
            pos += 1
        end
    end
    return finishscan!(ci, buf, d, n, inquote, rows)
end

# `nextrowstart` under the lenient rule; `from` is a row start.
function _nextrowstart_lenient(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect)::Int
    pos = from
    cq, oq, e = d.cq, d.oq, d.e
    delim = d.delim
    inquote = false
    fieldstart = true
    @inbounds while pos <= to
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2
            elseif b == cq
                if e == cq && pos < to && buf[pos + 1] == cq
                    pos += 2
                else
                    inquote = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif b == LF
            return pos + 1
        elseif b == CR
            return pos + 1 + (pos < to && buf[pos + 1] == LF)
        elseif delim isa UInt8 ? b == delim :
               (b == delim[1] && pos + length(delim) - 1 <= to && _matchbytes(buf, pos, delim))
            pos += delim isa UInt8 ? 1 : length(delim)
            fieldstart = true
        elseif fieldstart && d.quoted && b == oq
            inquote = true
            fieldstart = false
            pos += 1
        else
            fieldstart &= _isblank(b)
            pos += 1
        end
    end
    return to + 1
end

# --- fast scanner ------------------------------------------------------------
#
# The fast scanner reads 64 bytes at a time. Each 64-bit mask uses one bit for
# each input byte. One mask marks quotes. A second mask marks delimiters,
# carriage returns, and line feeds. The quote marks show which bytes are inside
# quoted fields. Only delimiters and line endings outside quoted fields become
# index events. The masks come from LLVM vector code, which selects vector
# instructions for the current CPU.
#
# `prefix_xor64` converts the quote marks into a running inside-or-outside mask.
# Each quote changes the value for all later bytes in the block. Supported x86-64
# and Apple ARM CPUs can calculate this mask with one instruction. Other CPUs
# use six shift-and-XOR steps.

const ONES8   = 0x0101010101010101
const LOWS7   = 0x7f7f7f7f7f7f7f7f
const MOVEMASK_MAGIC = 0x0102040810204080

# Set the high bit of each byte in `w` that equals `b`. This form does not mark
# a byte that differs from `b`, so callers can safely combine several results.
function eqmarks(w::UInt64, b::UInt8)::UInt64
    x = w ⊻ (ONES8 * b)
    return ~(((x & LOWS7) + LOWS7) | x | LOWS7)
end

movemask(marks::UInt64)::UInt64 = ((marks >> 7) * MOVEMASK_MAGIC) >> 56

@inline function prefix_xor64_shift(m::UInt64)::UInt64
    m ⊻= m << 1
    m ⊻= m << 2
    m ⊻= m << 4
    m ⊻= m << 8
    m ⊻= m << 16
    m ⊻= m << 32
    return m
end

@static if Sys.ARCH === :x86_64
    @inline function prefix_xor64_pclmul(m::UInt64)::UInt64
        # Inline assembly permits generic CPU compilation. Use the VEX form so
        # the vector scanner does not mix AVX with a legacy SSE instruction.
        # Execution requires both PCLMUL and AVX, checked at package load.
        v = Base.llvmcall((raw"""
            define i64 @entry(i64 %m) #0 {
                %a0 = insertelement <2 x i64> zeroinitializer, i64 %m, i32 0
                %b0 = insertelement <2 x i64> zeroinitializer, i64 -1, i32 0
                %r = call <2 x i64> asm "vpclmulqdq $$0, $2, $1, $0", "=x,x,x"(<2 x i64> %a0, <2 x i64> %b0)
                %lo = extractelement <2 x i64> %r, i32 0
                ret i64 %lo
            }
            attributes #0 = { alwaysinline }""", "entry"), UInt64, Tuple{UInt64}, m)
        return v
    end
    const HAS_PCLMUL = Ref(false)
    @inline prefix_xor64(m::UInt64) = HAS_PCLMUL[] ? prefix_xor64_pclmul(m) : prefix_xor64_shift(m)
elseif Sys.ARCH === :aarch64
    # `pmull` computes the running quote mask in one instruction. Every Apple
    # silicon CPU has it; another aarch64 host has it when the CPU has the AES
    # extension, which `__init__` probes through Base. The instruction lives
    # in a helper that carries its own target features, so a package image
    # built for a generic aarch64 target still compiles it, and the probe keeps
    # it from running on a CPU that would trap. The helper is a real call.
    @inline function prefix_xor64_pmull(m::UInt64)::UInt64
        v = Base.llvmcall(("""
            declare <16 x i8> @llvm.aarch64.neon.pmull64(i64, i64)
            define internal i64 @pmull_impl(i64 %m) #1 {
                %r = call <16 x i8> @llvm.aarch64.neon.pmull64(i64 %m, i64 -1)
                %v = bitcast <16 x i8> %r to <2 x i64>
                %lo = extractelement <2 x i64> %v, i32 0
                ret i64 %lo
            }
            define i64 @entry(i64 %m) #0 {
                %r = call i64 @pmull_impl(i64 %m)
                ret i64 %r
            }
            attributes #0 = { alwaysinline }
            attributes #1 = { noinline "target-features"="+neon,+aes" }""", "entry"),
            UInt64, Tuple{UInt64}, m)
        return v
    end
    const HAS_PMULL = Ref(Sys.isapple())
    @inline prefix_xor64(m::UInt64) = HAS_PMULL[] ? prefix_xor64_pmull(m) : prefix_xor64_shift(m)
else
    @inline prefix_xor64(m::UInt64) = prefix_xor64_shift(m)
end

# Runtime CPU probe for the paths above. Base has probed the host since
# Julia 1.7; on Julia 1.14 the probe comes from the cpufeatures library.
function _probecpu!()
    @static if Sys.ARCH === :x86_64
        C = Base.BinaryPlatforms.CPUID
        HAS_PCLMUL[] = C.test_cpu_feature(C.JL_X86_pclmul) &&
                       C.test_cpu_feature(C.JL_X86_avx)
    elseif Sys.ARCH === :aarch64 && !Sys.isapple()
        C = Base.BinaryPlatforms.CPUID
        HAS_PMULL[] = C.test_cpu_feature(C.JL_AArch64_aes)
    end
    return nothing
end

# LLVM code that creates the 64-byte masks. A load can start at any address. The
# first input byte maps to bit 0 on the supported little-endian systems. Julia
# 1.10 requires a typed pointer. Later Julia versions require an opaque pointer.
@static if VERSION < v"1.11"
    const LLVM_BYTE_PTR = "i8*"
    const LLVM_LOAD64 = """
            %vp = bitcast i8* %p to <64 x i8>*
            %x = load <64 x i8>, <64 x i8>* %vp, align 1"""
else
    const LLVM_BYTE_PTR = "ptr"
    const LLVM_LOAD64 = "%x = load <64 x i8>, ptr %p, align 1"
end

const SPECIALS_MASK_VEC_IR = """
        define i64 @entry($LLVM_BYTE_PTR %p, i8 %d, i8 %cr, i8 %lf) #0 {
$LLVM_LOAD64
            %d0 = insertelement <64 x i8> undef, i8 %d, i32 0
            %dv = shufflevector <64 x i8> %d0, <64 x i8> undef, <64 x i32> zeroinitializer
            %c0 = insertelement <64 x i8> undef, i8 %cr, i32 0
            %cv = shufflevector <64 x i8> %c0, <64 x i8> undef, <64 x i32> zeroinitializer
            %l0 = insertelement <64 x i8> undef, i8 %lf, i32 0
            %lv = shufflevector <64 x i8> %l0, <64 x i8> undef, <64 x i32> zeroinitializer
            %e1 = icmp eq <64 x i8> %x, %dv
            %e2 = icmp eq <64 x i8> %x, %cv
            %e3 = icmp eq <64 x i8> %x, %lv
            %o1 = or <64 x i1> %e1, %e2
            %o2 = or <64 x i1> %o1, %e3
            %m = bitcast <64 x i1> %o2 to i64
            ret i64 %m
        }
        attributes #0 = { alwaysinline }"""

const BYTE_MASK_VEC_IR = """
        define i64 @entry($LLVM_BYTE_PTR %p, i8 %b) #0 {
$LLVM_LOAD64
            %b0 = insertelement <64 x i8> undef, i8 %b, i32 0
            %bv = shufflevector <64 x i8> %b0, <64 x i8> undef, <64 x i32> zeroinitializer
            %c = icmp eq <64 x i8> %x, %bv
            %m = bitcast <64 x i1> %c to i64
            ret i64 %m
        }
        attributes #0 = { alwaysinline }"""

# Bits of the 64 bytes at `p` that equal `a`, `b`, or `c`.
@inline function threebyte_mask_vec(p::Ptr{UInt8}, a::UInt8, b::UInt8, c::UInt8)::UInt64
    Base.llvmcall((SPECIALS_MASK_VEC_IR, "entry"),
        UInt64, Tuple{Ptr{UInt8}, UInt8, UInt8, UInt8}, p, a, b, c)
end

specials_mask_vec(p::Ptr{UInt8}, d::UInt8)::UInt64 = threebyte_mask_vec(p, d, CR, LF)

@inline function byte_mask_vec(p::Ptr{UInt8}, b::UInt8)::UInt64
    Base.llvmcall((BYTE_MASK_VEC_IR, "entry"), UInt64, Tuple{Ptr{UInt8}, UInt8}, p, b)
end

function blockmasks(p::Ptr{UInt8}, quoted::Bool, oq::UInt8, delim::UInt8)
    q64 = quoted ? byte_mask_vec(p, oq) : zero(UInt64)
    return q64, specials_mask_vec(p, delim)
end

# space and tab marks: the blanks a field may start with before its quote
blankmask(p::Ptr{UInt8}) = byte_mask_vec(p, UInt8(' ')) | byte_mask_vec(p, UInt8('\t'))

# Quote state across one 64-byte block under a rule that is not the standard
# doubled-quote rule. `o64`, `c64`, and `e64` mark the open, close, and escape
# bytes; under the doubling rule (`e == cq`) `e64` equals `c64`. `inq` and
# `skip` give the state at the block start: `skip` means the first byte is
# content that the previous block consumed (the byte after an escape byte, or
# the second byte of a doubled close quote). `nextiscq` tells whether the byte
# after the block is a close quote, for a doubled close quote at the last byte.
# Returns the inside mask (set for every byte inside a quoted field, including
# the opening quote and excluding the closing quote), the mask of opening
# quotes, and the state after the block. Each loop step handles one quote
# event, so a block without quote bytes costs one test.
@inline function quotewalk(o64::UInt64, c64::UInt64, e64::UInt64, doubling::Bool,
                           inq::Bool, skip::Bool, nextiscq::Bool)
    inmask = zero(UInt64)
    opens = zero(UInt64)
    i = 0
    if skip
        inmask = one(UInt64)
        skip = false
        i = 1
    end
    t64 = c64 | e64
    while i < 64
        above = ~zero(UInt64) << i
        if !inq
            rest = o64 & above
            rest == zero(UInt64) && break
            t = trailing_zeros(rest)
            bit = one(UInt64) << t
            opens |= bit
            inmask |= bit
            inq = true
            i = t + 1
        else
            rest = t64 & above
            if rest == zero(UInt64)
                inmask |= above
                break
            end
            t = trailing_zeros(rest)
            inmask |= above & (~zero(UInt64) >> (63 - t))
            consumes = doubling ? (t < 63 ? ((c64 >> (t + 1)) & one(UInt64)) != zero(UInt64) : nextiscq) :
                                  ((e64 >> t) & one(UInt64)) != zero(UInt64)
            if consumes
                t == 63 ? (skip = true) : (inmask |= one(UInt64) << (t + 1))
                i = t + 2
            else
                inmask &= ~(one(UInt64) << t)
                inq = false
                i = t + 1
            end
        end
    end
    return inmask, opens, inq, skip
end

const EVENBITS = 0x5555555555555555
const ODDBITS = 0xaaaaaaaaaaaaaaaa

# The byte after each odd-length run of escape bytes in `b64` is escaped. A
# run that starts at an even bit and reaches bit 63 has an even length; a run
# that starts at an odd bit and reaches bit 63 escapes the next block's first
# byte, which the returned carry reports.
@inline function escapedafter(b64::UInt64)
    starts = b64 & ~(b64 << 1)
    evenruns, _ = Base.add_with_overflow(b64, starts & EVENBITS)
    oddruns, carry = Base.add_with_overflow(b64, starts & ODDBITS)
    escaped = ((evenruns & ~b64) & ODDBITS) | ((oddruns & ~b64) & EVENBITS)
    return escaped, carry
end

# Quote state across one 64-byte block under a rule that is not the standard
# doubled-quote rule, with the same contract as `quotewalk`. The prefix XOR of
# the toggle bytes gives the inside mask when every quote byte acts as its
# position allows: an open byte opens only outside a field, a close byte closes
# only inside one or doubles a close byte before it, and an escaped quote byte
# sits inside a field. A block that breaks one of these rules takes the exact
# walk instead. `walkonly` selects the walk for a dialect whose escape byte is
# also its open byte, where the run rule cannot tell an escape from an open.
@inline function quoteblock(o64::UInt64, c64::UInt64, e64::UInt64, doubling::Bool,
                            walkonly::Bool, inq::Bool, skip::Bool, nextiscq::Bool)
    if skip
        # The first byte is consumed content inside a quoted field.
        o64 &= ~one(UInt64)
        c64 &= ~one(UInt64)
        e64 &= ~one(UInt64)
    end
    walkonly && return quotewalk(o64, c64, e64, doubling, inq, false, nextiscq)
    if doubling
        t = o64 | c64
        inmask = prefix_xor64(t)
        inq && (inmask = ~inmask)
        closing = c64 & ~inmask
        bad = (o64 & ~inmask) | (c64 & inmask & ~(closing << 1))
        bad == zero(UInt64) || return quotewalk(o64, c64, e64, true, inq, false, nextiscq)
        nextinq = inq ⊻ isodd(count_ones(t))
        # A close byte at the last bit doubles a close byte after the block.
        skipnext = (closing >> 63) != zero(UInt64) && nextiscq
        skipnext && (nextinq = true)
        return inmask, o64 & inmask, nextinq, skipnext
    end
    escaped, carry = escapedafter(e64)
    q64 = o64 | c64
    t = q64 & ~escaped
    inmask = prefix_xor64(t)
    inq && (inmask = ~inmask)
    bad = (escaped & q64 & ~inmask) | (o64 & ~c64 & ~escaped & ~inmask) |
          (c64 & ~o64 & ~escaped & inmask)
    bad == zero(UInt64) || return quotewalk(o64, c64, e64, false, inq, false, false)
    nextinq = inq ⊻ isodd(count_ones(t))
    # An odd escape run at the end of a quoted field's block consumes the next byte.
    skipnext = carry && (inmask >> 63) != zero(UInt64)
    return inmask, o64 & t & inmask, nextinq, skipnext
end

# Bare-quote detection for one 64-byte block. `o64` marks the open-quote bytes
# and `opens` the ones that opened a field; `inmask` is the inside-quote state
# after each byte. An opening quote must be the first non-blank byte of its
# field (the byte after a delimiter or row ending, or after the blanks that
# follow one) or follow another open-quote byte. `fscarry`/`prevquote` carry
# the field-start and previous-byte-is-quote facts across blocks. The blank-run
# rule is one addition: a carry from each structural byte propagates through
# the run of blank bits after it and lands on the first non-blank byte.
@inline function barequotes(o64::UInt64, opens::UInt64, s64::UInt64, blank64::UInt64,
                            inmask::UInt64, fscarry::Bool, prevquote::Bool)
    # A whitespace delimiter ends a blank run. Otherwise overlapping carry
    # seeds add twice in that run and can erase its field-start bit.
    s64 &= ~inmask
    blank64 &= ~s64
    run = blank64 + ((s64 << 1) | (fscarry ? one(UInt64) : zero(UInt64)))
    fieldstart = run & ~blank64
    prevq = (o64 << 1) | (prevquote ? one(UInt64) : zero(UInt64))
    bare = (opens & ~fieldstart & ~prevq) != zero(UInt64)
    carryout = (s64 >> 63) != zero(UInt64) || run < blank64   # overflow: a blank run reached the end
    return bare, carryout, (o64 >> 63) != zero(UInt64)
end

# Initial tape capacity from the event density of the chunk's first 4 KiB
# (delimiters and line endings, quote-blind): dense numeric data gets the
# words it needs without a growth copy, and long-text data does not reserve
# a word per few bytes for events it never emits. The tape still grows on
# demand when the probe underestimates.
function _tapecapacity(buf::Vector{UInt8}, start::Int, stop::Int, delim::UInt8)
    len = stop - start + 1
    probe = min(len, 4096)
    n = 0
    pos = start
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= start + probe - 1
            n += count_ones(specials_mask_vec(p + pos - 1, delim))
            pos += 64
        end
    end
    probed = pos - start
    probed == 0 && return min(len + 1, 256)
    est = (n * len) ÷ probed
    return clamp(est + (est >> 3) + 256, 256, min(len + 1, MAX_TAPE_HINT))
end

function indexchunk_fast!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    @assert fasteligible(d)
    start, stop = ci.start, ci.stop
    delim = d.delim::UInt8
    oq, cq, e = d.oq, d.cq, d.e
    quoted = d.quoted
    symmetric = symmetricquotes(d)
    doubling = e == cq
    walkonly = !doubling && e == oq
    tape = ci.tape
    length(tape) < 256 && resize!(tape, _tapecapacity(buf, start, stop, delim))
    n = 0
    rows = 0           # row-end events emitted (assembly sizes its vectors from it)
    inq = false        # whether this block starts inside a quoted field
    skip = false       # the previous block consumed this block's first byte
    pairskip = false   # The last CR in a block already consumed the next LF.
    fscarry = true     # the next byte is a field start (chunks begin at a row start)
    prevquote = false  # the previous byte was a quote
    bare = false
    pos = start
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= stop
            q64, s64 = blockmasks(p + pos - 1, quoted, oq, delim)
            if symmetric
                inmask = prefix_xor64(q64)
                inq && (inmask = ~inmask)
                opens = q64 & inmask
                nextinq = inq ⊻ isodd(count_ones(q64))
            else
                c64 = byte_mask_vec(p + pos - 1, cq)
                e64 = doubling ? c64 : walkonly ? q64 : byte_mask_vec(p + pos - 1, e)
                nextiscq = doubling && pos + 64 <= stop && buf[pos + 64] == cq
                inmask, opens, nextinq, skip =
                    quoteblock(q64, c64, e64, doubling, walkonly, inq, skip, nextiscq)
            end
            if quoted
                b64 = blankmask(p + pos - 1)
                isbare, fscarry, prevquote = barequotes(q64, opens, s64, b64, inmask, fscarry, prevquote)
                bare |= isbare
            end
            specials = s64 & ~inmask
            pairskip && (specials &= ~one(UInt64))   # LF of a CRLF split across blocks
            pairskip = false
            if specials != zero(UInt64)
                tape = tape_room!(tape, n, 64)
                base = UInt32(pos - start)
                while specials != zero(UInt64)
                    tz = trailing_zeros(specials)
                    b = buf[pos + tz]
                    n += 1
                    rows += b != delim
                    if b == CR && pos + tz < stop && buf[pos + tz + 1] == LF
                        tape[n] = ((base + UInt32(tz)) << 2) | UInt32(3)
                        tz < 63 ? (specials &= ~(UInt64(1) << (tz + 1))) : (pairskip = true)
                    else
                        tape[n] = ((base + UInt32(tz)) << 2) | rawkind(b)
                    end
                    specials &= specials - one(UInt64)
                end
            end
            inq = nextinq
            pos += 64
        end
    end
    ci.tape = tape
    # Read the final bytes one at a time. Keep the state from the last full block.
    fieldstart = fscarry
    @inbounds while pos <= stop
        b = buf[pos]
        if inq
            if skip
                skip = false
                pos += 1
            elseif b == e && !doubling
                pos += 2
            elseif b == cq
                if doubling && pos < stop && buf[pos + 1] == cq
                    pos += 2
                else
                    inq = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif quoted && b == oq
            bare |= !fieldstart && !(pos > start && buf[pos - 1] == oq)
            inq = true
            fieldstart = false
            pos += 1
        elseif b == delim || b == LF || b == CR
            if pairskip
                pairskip = false
                pos += 1                             # the LF a block-final CR consumed
            else
                crlf = b == CR && pos < stop && buf[pos + 1] == LF
                tape_room!(tape, n, 1)
                n += 1
                rows += b != delim
                tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
                pos += crlf ? 2 : 1
            end
            fieldstart = true
        else
            fieldstart &= _isblank(b)
            pos += 1
        end
    end
    ci.barequote = bare
    return finishscan!(ci, buf, d, n, inq, rows)
end

# --- find safe chunk boundaries ---------------------------------------------
#
# A fixed byte range can start in the middle of a row or a quoted field. The
# parser must know whether each range starts inside a quoted field before it can
# use a line ending as a row boundary.
#
# The parser does these steps:
#
#   1. Divide the input into fixed byte ranges.
#   2. Find the quote state change of each range. Different tasks can work on
#      different ranges at the same time. Under the standard quote rule this is
#      the parity of the range's quote count. Under a distinct escape byte or
#      distinct open and close bytes it is the exit state for each of the three
#      possible entry states.
#   3. Read the results in file order. The input starts outside a quoted field.
#      Under the standard rule an odd count means that the next range starts on
#      the other side of a quote. Otherwise the exit state for the known entry
#      state is the next range's entry state.
#   4. Scan forward from each range start. Ignore line endings inside quoted
#      fields. The first line ending outside a quoted field gives a safe start
#      for the next chunk.
#   5. Remove empty chunks. An empty chunk can occur when one row crosses one or
#      more complete byte ranges.
#   6. Index the remaining chunks. This work can run at the same time.
#
# This process does not guess a boundary. Task order does not change the result.

# Return true when this range contains an odd number of quote bytes.
function quoteparity(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect)::Bool
    d.quoted || return false
    q = d.oq
    n = 0
    i = from
    # Check eight bytes at a time. Count matching quote bytes without creating
    # a separate value for each byte.
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while i + 7 <= to
            w = ltoh(unsafe_load(Ptr{UInt64}(p + i - 1)))
            n += count_ones(movemask(eqmarks(w, q)))
            i += 8
        end
    end
    @inbounds while i <= to
        n += buf[i] == q
        i += 1
    end
    return isodd(n)
end

# Quote states for the range planner under a distinct escape byte or distinct
# open and close bytes.
const QUOTE_OUTSIDE = 0x00
const QUOTE_INSIDE = 0x01
const QUOTE_CONSUMED = 0x02   # inside, and the next byte is consumed content:
                              # it follows an escape byte, or it is the byte
                              # after a close quote that may double it

# One step of the quote state machine for a byte with the given roles.
@inline function _quotestep(s::UInt8, isoq::Bool, iscq::Bool, ise::Bool, doubling::Bool)
    if s == QUOTE_OUTSIDE
        return isoq ? QUOTE_INSIDE : QUOTE_OUTSIDE
    elseif s == QUOTE_INSIDE
        doubling && return iscq ? QUOTE_CONSUMED : QUOTE_INSIDE
        return ise ? QUOTE_CONSUMED : iscq ? QUOTE_OUTSIDE : QUOTE_INSIDE
    else
        # After an escape byte the byte is content. After a close quote a
        # second close quote is content; any other byte is read outside.
        doubling || return QUOTE_INSIDE
        return (iscq || isoq) ? QUOTE_INSIDE : QUOTE_OUTSIDE
    end
end

# Transition tables for `quotetransitions`. The three entry states are packed
# as `s0 + 3 s1 + 9 s2`. A byte's class marks it as an open byte (1), a close
# byte (2), or an escape byte (4). Index 1 is the escape rule, index 2 the
# doubling rule.
const QUOTE_TABLES = let
    tables = (Vector{UInt8}(undef, 27 * 8), Vector{UInt8}(undef, 27 * 8))
    for (k, doubling) in enumerate((false, true)), packed in 0:26, cls in 0:7
        isoq, iscq, ise = (cls & 1) != 0, (cls & 2) != 0, (cls & 4) != 0
        states = (UInt8(packed % 3), UInt8((packed ÷ 3) % 3), UInt8(packed ÷ 9))
        next = map(state -> _quotestep(state, isoq, iscq, ise, doubling), states)
        tables[k][8 * packed + cls + 1] = next[1] + 0x03 * next[2] + 0x09 * next[3]
    end
    tables
end

# Return the exit state of `buf[from:to]` for each entry state, as a tuple
# indexed by `state + 1`. One table step moves all three machines. Only quote
# and escape bytes step them: a run of other bytes steps them once.
function quotetransitions(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect)::NTuple{3, UInt8}
    oq, cq, e = d.oq, d.cq, d.e
    doubling = e == cq
    tab = QUOTE_TABLES[doubling ? 2 : 1]
    packed = 21               # each entry state maps to itself
    last = from - 1           # the last quote or escape byte handled
    i = from
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while i + 63 <= to
            m = threebyte_mask_vec(p + i - 1, oq, cq, e)
            while m != zero(UInt64)
                t = i + trailing_zeros(m)
                t > last + 1 && (packed = Int(tab[8 * packed + 1]))
                b = buf[t]
                cls = Int(b == oq) | (Int(b == cq) << 1) | (Int(!doubling && b == e) << 2)
                packed = Int(tab[8 * packed + cls + 1])
                last = t
                m &= m - one(UInt64)
            end
            i += 64
        end
        @inbounds while i + 7 <= to
            w = ltoh(unsafe_load(Ptr{UInt64}(p + i - 1)))
            m = movemask(eqmarks(w, oq) | eqmarks(w, cq) | eqmarks(w, e))
            while m != zero(UInt64)
                t = i + trailing_zeros(m)
                t > last + 1 && (packed = Int(tab[8 * packed + 1]))
                b = buf[t]
                cls = Int(b == oq) | (Int(b == cq) << 1) | (Int(!doubling && b == e) << 2)
                packed = Int(tab[8 * packed + cls + 1])
                last = t
                m &= m - one(UInt64)
            end
            i += 8
        end
    end
    @inbounds while i <= to
        b = buf[i]
        if b == oq || b == cq || b == e
            i > last + 1 && (packed = Int(tab[8 * packed + 1]))
            cls = Int(b == oq) | (Int(b == cq) << 1) | (Int(!doubling && b == e) << 2)
            packed = Int(tab[8 * packed + cls + 1])
            last = i
        end
        i += 1
    end
    to > last && (packed = Int(tab[8 * packed + 1]))
    return (UInt8(packed % 3), UInt8((packed ÷ 3) % 3), UInt8(packed ÷ 9))
end

# The first row start at or after `from` when `state` is the quote state at
# `from`. A consumed byte is content, so the scan continues after it inside the
# quoted field. A close quote that the byte does not double has taken effect,
# so the scan reads the byte outside.
function _rangerowstart(buf::Vector{UInt8}, from::Int, len::Int, d::Dialect, state::UInt8)
    state == QUOTE_OUTSIDE && return nextrowstart(buf, from, len, d, false)
    state == QUOTE_INSIDE && return nextrowstart(buf, from, len, d, true)
    if d.e == d.cq && !(from <= len && @inbounds(buf[from]) == d.cq)
        return nextrowstart(buf, from, len, d, false)
    end
    return nextrowstart(buf, from + 1, len, d, true)
end

# Scan from `from` to the first row ending outside a quoted field. `inquote`
# tells whether `from` is inside a quoted field. Return the byte after the row
# ending. Return `to + 1` if this range has no complete row ending.
#
# Set `atrowstart` only when `from` is a known row start. This lets the function
# identify a comment row. Quote bytes in a comment row have no CSV meaning, so
# the function scans directly to that row's end.
function nextrowstart(buf::Vector{UInt8}, from::Int, to::Int, d::Dialect, inquote::Bool,
                      atrowstart::Bool=false)::Int
    pos = from
    cq, oq, e = d.cq, d.oq, d.e
    cmt = d.comment
    if atrowstart && !inquote && cmt !== nothing &&
       from + length(cmt) - 1 <= to && _matchbytes(buf, from, cmt)
        @inbounds while pos <= to
            b = buf[pos]
            b == LF && return pos + 1
            b == CR && return pos + 1 + (pos < to && buf[pos + 1] == LF)
            pos += 1
        end
        return to + 1
    end
    d.lenient && return _nextrowstart_lenient(buf, from, to, d)
    @inbounds while pos <= to
        b = buf[pos]
        if inquote
            if b == e && e != cq
                pos += 2
            elseif b == cq
                if e == cq && pos < to && buf[pos + 1] == cq
                    pos += 2
                else
                    inquote = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif d.quoted && b == oq
            inquote = true
            pos += 1
        elseif b == LF
            return pos + 1
        elseif b == CR
            return pos + 1 + (pos < to && buf[pos + 1] == LF)
        else
            pos += 1
        end
    end
    return to + 1
end

# Choose the start and end of each chunk. This function does not build the field
# index. `index` and `parse` both use this plan. They build the indexes for the
# planned chunks before they parse field values.
function chunkplan(buf::Vector{UInt8}, d::Dialect, datastart::Int, chunkbytes::Int,
                   parallel::Bool, tasklimit::Int; _taskobserver=nothing)
    len = length(buf)
    # Split compatible input into bounded chunks even when `parallel` is false.
    # Bounded chunks keep each parsing pass on a smaller part of the input.
    # `parallel` only controls whether this work uses tasks or a plain loop.
    if (d.lenient || (commentserial(d) && splittable(d))) && len - datastart + 1 > chunkbytes
        # Raw quote counts are not valid under the lenient quote rule, or when
        # a comment row is known to hold a quote. Start at a known row boundary
        # and find later row boundaries in file order. The later index work can
        # still use multiple tasks.
        chunks = ChunkIndex[]
        b0 = datastart
        while b0 <= len
            target = min(b0 + chunkbytes - 1, len)
            # Start at the known row boundary `b0`. Move through complete rows
            # until the scan passes `target`. This keeps quoted line endings and
            # comment rows intact.
            b1 = target >= len ? len + 1 : _rowstartatorafter(buf, b0, target, len, d)
            push!(chunks, ChunkIndex(b0, b1 - 1))
            b0 = b1
        end
        foreach(checktaperange, chunks)
        return chunks
    end
    nranges = splittable(d) ? max(1, cld(len - datastart + 1, chunkbytes)) : 1
    starts = [datastart + (i - 1) * chunkbytes for i in 1:nranges]
    entry = fill(QUOTE_OUTSIDE, nranges)
    if nranges > 1 && symmetricquotes(d)
        par = Vector{Bool}(undef, nranges)
        if parallel && tasklimit > 1
            _taskforeach(1:nranges, tasklimit, _taskobserver) do i
                to = i == nranges ? len : starts[i + 1] - 1
                par[i] = quoteparity(buf, starts[i], to, d)
            end
        else
            for i in 1:nranges
                to = i == nranges ? len : starts[i + 1] - 1
                par[i] = quoteparity(buf, starts[i], to, d)
            end
        end
        acc = false
        for i in 2:nranges
            acc ⊻= par[i - 1]
            entry[i] = acc ? QUOTE_INSIDE : QUOTE_OUTSIDE
        end
    elseif nranges > 1
        exits = Vector{NTuple{3, UInt8}}(undef, nranges)
        if parallel && tasklimit > 1
            _taskforeach(1:nranges, tasklimit, _taskobserver) do i
                to = i == nranges ? len : starts[i + 1] - 1
                exits[i] = quotetransitions(buf, starts[i], to, d)
            end
        else
            for i in 1:nranges
                to = i == nranges ? len : starts[i + 1] - 1
                exits[i] = quotetransitions(buf, starts[i], to, d)
            end
        end
        acc = QUOTE_OUTSIDE
        for i in 2:nranges
            acc = exits[i - 1][acc + 1]
            entry[i] = acc
        end
    end
    bounds = Vector{Int}(undef, nranges)
    bounds[1] = datastart
    if nranges > 1
        if parallel && tasklimit > 1
            _taskforeach(2:nranges, tasklimit, _taskobserver) do i
                bounds[i] = _rangerowstart(buf, starts[i], len, d, entry[i])
            end
        else
            for i in 2:nranges
                bounds[i] = _rangerowstart(buf, starts[i], len, d, entry[i])
            end
        end
    end
    push!(bounds, len + 1)
    # Each chunk starts at a row boundary. Drop an empty chunk. This can occur
    # when one row crosses one or more complete byte ranges.
    chunks = ChunkIndex[]
    for i in 1:nranges
        b0, b1 = bounds[i], bounds[i + 1]
        b0 < b1 && push!(chunks, ChunkIndex(b0, b1 - 1))
    end
    # Each event stores a 30-bit offset from the chunk start. A very large row
    # can make a chunk larger than `chunkbytes`. Reject the chunk if an event
    # offset cannot fit.
    foreach(checktaperange, chunks)
    return chunks
end

# Start at the known row boundary `from`. Return the first row start after
# `target`. Return `len + 1` when `target` is in the final row.
function _rowstartatorafter(buf::Vector{UInt8}, from::Int, target::Int, len::Int, d::Dialect)
    pos = from
    while pos <= target
        pos = nextrowstart(buf, pos, len, d, false, true)
    end
    return min(pos, len + 1)
end

function indexone!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, scanner::Symbol)
    scanner === :lenient ? indexchunk_lenient!(ci, buf, d) :
    scanner === :scalar  ? indexchunk_scalar!(ci, buf, d) :
                           indexchunk_fast!(ci, buf, d)
end

# Choose the scanner. A multi-byte delimiter, the lenient quote rule, and a
# comment row that holds a quote byte require the scalar scanner.
# `fastindex=false` selects it for every input, as the reference.
function resolvescanner(d::Dialect, fastindex::Bool)
    d.lenient && return :lenient
    return fastindex && fasteligible(d) ? :vec : :scalar
end

"""
    index(buf, d::Dialect; datastart=1, chunkbytes=2^23, parallel=true,
          ntasks=nothing, fastindex=true)

Build an index of the rows and fields in `buf[datastart:end]`. Each chunk starts
and ends at a complete row boundary. Each stored field has its exact byte
position and length. The same input gives the same index for every valid
`chunkbytes` value and thread count.
"""
function index(buf::Vector{UInt8}, d::Dialect;
               datastart::Int=1,
               chunkbytes::Int=1 << 23,
               parallel::Bool=Threads.nthreads() > 1,
               ntasks::Union{Nothing, Int}=nothing,
               fastindex::Bool=true,
               _taskobserver=nothing)
    len = length(buf)
    # No lower bound beyond 1: a tiny chunkbytes forces row boundaries
    # everywhere, which exercises every chunk geometry. The standalone index
    # default is 8 MiB; `parse` passes its size-aware 64 KiB–1 MiB default.
    chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    datastart >= 1 || throw(ArgumentError("datastart must be ≥ 1 (got $datastart)"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    tasklimit = _readtasklimit(parallel, ntasks)
    sc = resolvescanner(d, fastindex)
    datastart > len && return BufferIndex(ChunkIndex[], 0, false, false)

    chunks = chunkplan(buf, d, datastart, chunkbytes, parallel, tasklimit;
                       _taskobserver)
    if length(chunks) == 1 || tasklimit <= 1
        for ci in chunks
            indexone!(ci, buf, d, sc)
        end
    else
        _taskforeach(chunks, tasklimit, _taskobserver) do ci
            indexone!(ci, buf, d, sc)
        end
    end

    # A comment row held a quote byte: the parallel plan (and a fast scan)
    # assumed none, so rebuild in file order with the scalar scanner. This
    # comes before the boundary check below, which that assumption can break.
    if commentaware(d) && !d.commentquotes && !d.lenient &&
       any(ci -> ci.commentquote, chunks)
        return index(buf, withcommentquotes(d); datastart, chunkbytes, parallel, ntasks,
                     fastindex, _taskobserver)
    end
    # Every non-final chunk ends after a complete row. It must therefore end
    # outside a quoted field. A failure here means that chunk planning is wrong.
    for (k, ci) in enumerate(chunks)
        k < length(chunks) && ci.unclosedquote &&
            error("internal error: chunk $(k) ended inside a quoted field")
    end
    # Capture malformed-EOF before filtering: an unclosed quote inside a dropped
    # (e.g. all-comment) chunk must still surface as a Problem.
    unclosed = !isempty(chunks) && last(chunks).unclosedquote
    bare = any(ci -> ci.barequote, chunks)
    filter!(ci -> totalrows(ci) > 0, chunks)
    return BufferIndex(chunks, sum(totalrows, chunks; init=0), unclosed, bare)
end

index(buf::Vector{UInt8}; kw...) = index(buf, Dialect(); kw...)

# ---------------------------------------------------------------------------
# L2/L3: typed parsing over the index.
# ---------------------------------------------------------------------------

# CSV changes a column type in this order:
#   Missing → Int64 → Int128 → Float64 → String
#   Missing → (Date | Timestamp{Nanosecond} → Timestamp{Microsecond} | Time | Bool) → String
# Other type combinations change to String. The API layer handles smaller
# integer and string types. `typemap` changes an inferred type. It does not
# change a type that the user set. Missing does not use `typemap`.
function _normalizetypemap(typemap)
    typemap === nothing && return nothing
    tm = Dict{Type, Type}()
    inttarget = nothing
    for (a, b) in typemap
        a isa Type && b isa Type ||
            throw(ArgumentError("typemap entries must be Type => Type (got $a => $b)"))
        key = Base.nonmissingtype(a)
        # Inference uses Int64 on every architecture. Keep the common `Int`
        # spelling portable by treating it as the inferred integer type on
        # 32-bit Julia too; otherwise the same typemap silently stops applying.
        if key === Int && Int !== Int64
            inttarget = Base.nonmissingtype(b)
        else
            tm[key] = Base.nonmissingtype(b)
        end
    end
    # An explicit Int64 entry is more specific than the portable Int alias.
    inttarget === nothing || haskey(tm, Int64) || (tm[Int64] = inttarget)
    return isempty(tm) ? nothing : tm
end

_maptype(tm, T) = tm === nothing || T === Missing ? T : get(tm, T, T)
@inline function _promotemapped(tm, current::Type, detected::Type)
    joined = promote_kernel(current, detected)
    # A mapped result is already the selected parse type. Do not map it again
    # when a later result keeps the same type.
    joined === current && return current
    mapped = _maptype(tm, joined)
    # A map can return to the type that rejected the field. Use String in this
    # case because String accepts both field forms.
    return mapped === current ? String : mapped
end

_copts(colopts, opts, j::Int) = colopts === nothing ? opts : @inbounds colopts[j]

const _TS_NS = Timestamp{Dates.Nanosecond}
const _TS_US = Timestamp{Dates.Microsecond}
promote_kernel(a::Type, b::Type) =
    a === b          ? a :
    a === Missing    ? b :
    b === Missing    ? a :
    a === Int64 && b === Int128 ? Int128 :
    a === Int128 && b === Int64 ? Int128 :
    a in (Int64, Int128) && b === Float64 ? Float64 :
    a === Float64 && b in (Int64, Int128) ? Float64 :
    a === _TS_NS && b === _TS_US ? _TS_US :
    a === _TS_US && b === _TS_NS ? _TS_US :
    String

# Detect the type of one field. Detection and value parsing use the same Parsers
# functions on the same content bytes. A type conflict therefore always changes
# the column to a type that can accept both field forms.
function detecttype(buf::Vector{UInt8}, pos::Int, len::Int, opts::ValueOpts)
    len == 0 && return Missing
    cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
    st == CELL_MISSING && return Missing
    st == CELL_BADQUOTE && return String    # malformed quoting reports at parse time
    clen == 0 && return String             # quoted-empty is a present string
    if esc
        decoded = _unescape_bytes(buf, Int64(cpos), Int32(clen), opts.e, opts.cq)
        return _detectcontent(decoded, 1, length(decoded), opts)
    end
    return _detectcontent(buf, cpos, clen, opts)
end

function _detectcontent(buf::Vector{UInt8}, cpos::Int, clen::Int, opts::ValueOpts)
    cpos, cj = _trimblanks(buf, cpos, cpos + clen - 1)
    cpos > cj && return String              # blanks only: a present string
    if opts.groupmark != 0x00
        # sampling is cold: a fresh scratch per call keeps the signature small
        scratch = Vector{UInt8}(undef, 64)
        parsevalue(Int64, buf, cpos, cj, opts, scratch)[2] && return Int64
        parsevalue(Int128, buf, cpos, cj, opts, scratch)[2] && return Int128
        parsevalue(Float64, buf, cpos, cj, opts, scratch)[2] && return Float64
    else
        rc = Parsers.parseint(Int64, buf, cpos, cj)[2]
        rc == Parsers.RC_OK && return Int64
        rc == Parsers.RC_OVERFLOW &&
            Parsers.parseint(Int128, buf, cpos, cj)[2] == Parsers.RC_OK && return Int128
        _fixedfloatusable(Parsers.parsefloat(Float64, buf, cpos, cj, opts.decimal)[2]) &&
            return Float64
    end
    if opts.customfmt
        # one probe: the user format's own components say which type it detects
        c, rc = Parsers.parsecivil(buf, cpos, cj, opts.datepat)
        if rc == Parsers.RC_OK
            if opts.customkind != 0x03
                opts.customkind == 0x01 || return Time
                return _DATEYEARS[1] <= c.year <= _DATEYEARS[2] ? Date : String
            end
            T = _timestamptype(c)
            T === String || return T
        end
    else
        parsevalue(Date, buf, cpos, cj, opts)[2] && return Date
        c, rc = Parsers.parsecivil(buf, cpos, cj, _datetimepattern(opts, buf, cpos, cj))
        if rc == Parsers.RC_OK
            T = _timestamptype(c)
            T === String || return T
        end
        Parsers.parsecivil(buf, cpos, cj, opts.timepat)[2] == Parsers.RC_OK && return Time
    end
    opts.inferbool && parsevalue(Bool, buf, cpos, cj, opts)[2] && return Bool
    return String
end

# --- column storage ----------------------------------------------------------

# Two typed storage layouts, chosen per column by what the SAMPLE showed, so
# that the FINAL column is a plain Base vector with zero copies either way:
#   TypedColumn{T}  values + presence  → `Vector{T}` when nothing is missing
#   UnionColumn{T}  Vector{Union{T,Missing}} written in place → that vector
# (converting one layout to the other after the parse is a full extra pass,
# which is why two layouts exist rather than one plus a conversion.)
#
# Fixed-size isbits values + presence bytes. `Vector{Bool}` (not BitVector): chunk
# tasks write disjoint row ranges concurrently and BitVector packs 64 rows per word
# (a data race).
struct TypedColumn{T}
    values::Vector{T}
    present::Vector{Bool}
end

TypedColumn{T}(n::Int) where {T} = TypedColumn{T}(Vector{T}(undef, n), fill(false, n))

# Direct-to-final storage for typed columns the SAMPLE showed missings in: the
# parse writes `Vector{Union{T,Missing}}` cells straight into the final — for a
# bits `T` that is a data store plus a tag-byte store, the same two stores as
# values+present — so finalize hands the Base vector back with zero copies.
# Missing-free columns keep TypedColumn and return the raw `Vector{T}`; a
# column whose (sparse) missings the sample missed converts once at finalize.
# That conversion costs about as much as the parse itself (bitsunion stores
# have no memcpy path), which is why the write-direct mode exists.
struct UnionColumn{T}
    uvalues::Vector{Union{T, Missing}}
    UnionColumn{T}(uvalues::Vector{Union{T, Missing}}) where {T} = new{T}(uvalues)
end

UnionColumn{T}(n::Int) where {T} = UnionColumn{T}(Vector{Union{T, Missing}}(undef, n))

function _storevalue!(col::TypedColumn{T}, i::Int, v::T) where {T}
    @inbounds col.values[i] = v
    @inbounds col.present[i] = true
    return
end

function _storevalue!(col::UnionColumn{T}, i::Int, v::T) where {T}
    @inbounds col.uvalues[i] = v
    return
end

# --- strings ------------------------------------------------------------------
# The DataString type family (payload, accessors, AbstractString interface,
# DataStringVector, materialize) lives in DataStrings;
# the quote/escape-aware helpers and the StringColumn staging below are the
# CSV-specific layer over it.
include("strings.jl")

# Next `""` pair at or after i (RFC doubling; the span passed findcontent, so
# quotes only occur doubled) — word-scan for the quote byte, verify adjacency
@inline function _nextpair(buf::Vector{UInt8}, i::Int, last::Int, cq::UInt8)
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while i + 7 <= last
            mk = _eqmask8_c(ltoh(unsafe_load(Ptr{UInt64}(p + i - 1))), cq)
            if mk == 0
                i += 8
                continue
            end
            # Borrow propagation can mark bytes after the first match. The
            # first mark is exact; restart after a lone quote before searching
            # for another candidate.
            k = i + (trailing_zeros(mk) >> 3)
            k < last && buf[k + 1] == cq && return k
            i = k + 1
        end
    end
    @inbounds while i < last
        buf[i] == cq && buf[i + 1] == cq && return i
        i += 1
    end
    return 0
end

function _eqmask8_c(w::UInt64, b::UInt8)
    x = w ⊻ (0x0101010101010101 * b)
    return (x - 0x0101010101010101) & ~x & 0x8080808080808080
end

# Unescape ≤12 result bytes straight into a payload — no allocation; returns
# `nothing` when the unescaped content exceeds the inline capacity.
@inline function _unescape_inline(buf::Vector{UInt8}, pos::Int, len::Int, e::UInt8, cq::UInt8)
    a = zero(UInt64)
    b = zero(UInt64)
    n = 0
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        c = buf[i]
        if c == e && i < last && (e != cq || buf[i + 1] == cq)
            c = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        n += 1
        n > INLINE_MAX && return nothing
        if n <= 4
            a |= UInt64(c) << (32 + 8 * (n - 1))
        else
            b |= UInt64(c) << (8 * (n - 5))
        end
    end
    return DataStringPayload(a | UInt64(n % UInt32), b)
end

@inline function _unescape_append!(dst::Vector{UInt8}, buf::Vector{UInt8}, pos::Int, len::Int,
                                   e::UInt8, cq::UInt8)
    n0 = length(dst)
    if e == cq
        # run-copy: reserve the upper bound once, bulk-copy the bytes between
        # "" pairs, trim to the actual size — no per-byte push!/branch
        resize!(dst, n0 + len)
        w = n0
        i = pos
        last = pos + len - 1
        @inbounds while i <= last
            k = _nextpair(buf, i, last, cq)
            run = (k == 0 ? last + 1 : k + 1) - i    # keep one quote of the pair
            copyto!(dst, w + 1, buf, i, run)
            w += run
            k == 0 && break
            i = k + 2
        end
        resize!(dst, w)
        return w - n0
    end
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        c = buf[i]
        if c == e && i < last && (e != cq || buf[i + 1] == cq)
            c = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        push!(dst, c)
    end
    return length(dst) - n0
end

# The column builder: payloads plus the bytes this column OWNS. Every cell
# longer than the inline payload is copied out of the input at parse time, so
# a finished column never references the source buffer: a mapped file can be
# unmapped or rewritten as soon as parsing ends (no SIGBUS), a slice retains
# column buffers instead of the source, and a `Vector{UInt8}` input is never
# aliased. Payload buffer index 1 is `extra` (this column's own appends);
# indices 2.. are chunk-segment buffers adopted without a copy.
mutable struct StringColumn
    payloads::Vector{DataStringPayload}
    extra::Vector{UInt8}             # bytes appended by this column's own parse
    adopted::Vector{Vector{UInt8}}   # chunk-segment buffers folded in by reference
    lock::ReentrantLock              # guards `adopted` under parallel re-parses
    e::UInt8                         # escape char
    cq::UInt8                        # close-quote char (e == cq for RFC ""-doubling)
    slot::Int                        # payload buffer index of `extra`: 1 for a private
                                     # column, 1 + k for chunk k's segment of a direct final
end
StringColumn(payloads::Vector{DataStringPayload}, e::UInt8, cq::UInt8, slot::Int=1) =
    StringColumn(payloads, UInt8[], Vector{Vector{UInt8}}(), ReentrantLock(), e, cq, slot)
StringColumn(n::Int, e::UInt8, cq::UInt8) = StringColumn(fill(PAYLOAD_MISSING, n), e, cq)

# Buffer 0 is the unreferenced source slot (DataStrings appends its own edit
# arena after the last buffer).
_buffers(col::StringColumn) = Vector{UInt8}[EMPTY_BYTES, col.extra, col.adopted...]

@noinline _extratoolarge() =
    throw(ArgumentError("a single chunk holds more than 2 GiB of text; use a smaller chunkbytes"))

# Copy one long cell into the column's bytes and store its view payload.
@inline function _own!(col::StringColumn, buf::Vector{UInt8}, cpos::Int, clen::Int, out::Int)
    extra = col.extra
    spos = length(extra) + 1
    spos - 1 <= typemax(Int32) - clen || _extratoolarge()
    resize!(extra, spos + clen - 1)
    GC.@preserve extra buf unsafe_copyto!(pointer(extra, spos), pointer(buf, cpos), clen)
    @inbounds col.payloads[out] = view_payload(extra, spos, clen, col.slot, spos - 1)
    return
end

# Unescape one long cell straight into the column's bytes.
@inline function _ownescaped!(col::StringColumn, buf::Vector{UInt8}, cpos::Int, clen::Int,
                              out::Int)
    extra = col.extra
    spos = length(extra) + 1
    spos - 1 <= typemax(Int32) - clen || _extratoolarge()
    n = _unescape_append!(extra, buf, cpos, clen, col.e, col.cq)
    @inbounds col.payloads[out] = n <= INLINE_MAX ?
                                  inline_payload(extra, spos, n) :
                                  view_payload(extra, spos, n, col.slot, spos - 1)
    return
end

# Adopt a chunk segment's bytes as one more buffer of `col`, by reference.
# Returns the new payload buffer index. Locked: the stale re-parse wave adopts
# from several tasks at once.
function _adoptbuffer!(col::StringColumn, bytes::Vector{UInt8})
    lock(col.lock)
    try
        push!(col.adopted, bytes)
        return 1 + length(col.adopted)
    finally
        unlock(col.lock)
    end
end

# Fold `seg`'s owned bytes into `col` and re-point the payloads in `rows` (of
# `col.payloads`, which the segment wrote into) from the segment's buffer 1.
function _adopt!(col::StringColumn, seg::StringColumn, rows::AbstractUnitRange{Int})
    isempty(seg.extra) && return
    newidx = _adoptbuffer!(col, seg.extra)
    payloads = col.payloads
    @inbounds for r in rows
        pl = payloads[r]
        if payloadlen(pl) > INLINE_MAX && payloadbufidx(pl) == 1
            payloads[r] = repoint_payload(pl, newidx, payloadoffset(pl))
        end
    end
    return
end

# Unescape: `""` collapses to `"` when e == cq; `\X` drops the backslash when
# e != cq. Spans are Int64/Int32 end to end, so a single field may be
# arbitrarily wide.
function _unescape_bytes(buf::Vector{UInt8}, pos::Int64, len::Int32, e::UInt8, cq::UInt8)
    out = Vector{UInt8}(undef, len)
    n = 0
    i = Int(pos)
    last = i + Int(len) - 1
    @inbounds while i <= last
        b = buf[i]
        if b == e && i < last && (e != cq || buf[i + 1] == cq)
            n += 1
            out[n] = e == cq ? cq : buf[i + 1]
            i += 2
        else
            n += 1
            out[n] = b
            i += 1
        end
    end
    return resize!(out, n)
end

_unescape(buf::Vector{UInt8}, pos::Int64, len::Int32, e::UInt8, cq::UInt8) =
    String(_unescape_bytes(buf, pos, len, e, cq))

# --- per-(column × chunk) parse loops ---------------------------------------
#
# Each call below parses one column type in one chunk. Julia selects the method
# once for that work. It does not select a method for each field. If a field
# needs a different type, the function returns that row. The driver then changes
# and reads only this column again.

# Returns 0 on success, or the local row of the first conflicting value.
function parsecolchunk!(col::Union{TypedColumn{T}, UnionColumn{T}}, buf::Vector{UInt8},
                        ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int)) where {T}
    scratch = _scratchfor(opts)
    tape, rowfirst, rowstartrel, ext = ci.tape, ci.rowfirst, ci.rowstartrel, ci.ext
    start, skip, fdr, total = ci.start, ci.delimskip, ci.firstdatarow, totalrows(ci)
    @inbounds first = fdr <= total ? Int(rowfirst[fdr]) : 0
    @inbounds for lr in fdr:total
        rowlo = first
        nextr = Int(rowfirst[lr + 1])
        first = nextr
        localrow = lr - fdr + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = _span(start, tape, rowstartrel, ext, skip, rowlo, nextr, lr, j)
        sp === nothing && continue                      # short row ⇒ missing (reported once per row by the driver)
        pos, len = sp
        len == 0 && continue                            # empty ⇒ missing
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        st == CELL_MISSING && continue                  # sentinel / stripped-to-empty
        if st == CELL_VALUE && clen > 0
            v, ok = _parsecontent(T, buf, cpos, clen, esc, opts, scratch)
            if ok
                _storevalue!(col, out, v)
                continue
            end
        end
        # invalid for T (also: malformed quoting or quoted-empty)
        if userprovided
            problemrow = problemrowbase + localrow
            if st == CELL_BADQUOTE
                pushcellproblem!(problems, problemrow, j, pos, len, :invalid_quoted_field,
                                 "malformed quoting in ", buf)
            else
                pushcellproblem!(problems, problemrow, j, pos, len, :invalid_value,
                                 "cannot parse $(T) from ", buf)
            end
            # value stays missing under strict=false semantics
        else
            return lr                                   # inference conflict ⇒ promote & re-parse column
        end
    end
    return 0
end

# The string loop writes into a column that is PRIVATE to this call (a chunk
# segment, a batch, or a re-parse column), so owning bytes needs no lock.
function parsecolchunk!(col::StringColumn, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int))
    payloads = col.payloads
    tape, rowfirst, rowstartrel, ext = ci.tape, ci.rowfirst, ci.rowstartrel, ci.ext
    start, skip, fdr, total = ci.start, ci.delimskip, ci.firstdatarow, totalrows(ci)
    @inbounds first = fdr <= total ? Int(rowfirst[fdr]) : 0
    @inbounds for lr in fdr:total
        rowlo = first
        nextr = Int(rowfirst[lr + 1])
        first = nextr
        localrow = lr - fdr + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = _span(start, tape, rowstartrel, ext, skip, rowlo, nextr, lr, j)
        sp === nothing && continue
        pos, len = sp
        len == 0 && continue                            # unquoted empty ⇒ missing; quoted "" survives below
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            # Report invalid quoting and keep the original field bytes as the
            # value. Keep the quotes and do not remove escape bytes. This lets
            # the caller inspect the invalid input.
            problemrow = problemrowbase + localrow
            pushcellproblem!(problems, problemrow, j, pos, len, :invalid_quoted_field,
                             "malformed quoting in ", buf)
            len <= INLINE_MAX ? (payloads[out] = inline_payload(buf, pos, len)) :
                                          _own!(col, buf, pos, len, out)
            continue
        end
        if st == CELL_MISSING
            continue
        end
        if esc
            # escaped values are unescaped ONCE, at parse time (DataString needs
            # O(1) codeunit access): short results build inline payloads
            # allocation-free; long ones unescape into the column's bytes
            inl = _unescape_inline(buf, cpos, clen, col.e, col.cq)
            inl === nothing ? _ownescaped!(col, buf, cpos, clen, out) : (payloads[out] = inl)
        elseif clen <= INLINE_MAX
            payloads[out] = inline_payload(buf, cpos, clen)
        else
            _own!(col, buf, cpos, clen, out)
        end
    end
    return 0
end

# A column believed all-missing: inferred columns report the first conflict so
# the driver can promote; explicit Missing columns report every present value.
function parsecolchunk_missing(buf::Vector{UInt8}, ci::ChunkIndex, j::Int,
                               rowbase::Int, opts::ValueOpts,
                               userprovided::Bool, problems,
                               mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                               reportlimit::Int=typemax(Int))
    tape, rowfirst, rowstartrel, ext = ci.tape, ci.rowfirst, ci.rowstartrel, ci.ext
    start, skip, fdr, total = ci.start, ci.delimskip, ci.firstdatarow, totalrows(ci)
    @inbounds first = fdr <= total ? Int(rowfirst[fdr]) : 0
    @inbounds for lr in fdr:total
        rowlo = first
        nextr = Int(rowfirst[lr + 1])
        first = nextr
        localrow = lr - fdr + 1
        mask !== nothing && !mask[maskbase + localrow] && continue
        localrow > reportlimit && continue
        sp = _span(start, tape, rowstartrel, ext, skip, rowlo, nextr, lr, j)
        sp === nothing && continue
        _, len = sp
        len == 0 && continue
        st = cellcontent(buf, sp[1], len, opts)[4]
        if st != CELL_MISSING
            userprovided || return lr
            out = rowbase + localrow
            if st == CELL_BADQUOTE
                pushcellproblem!(problems, out, j, sp[1], len, :invalid_quoted_field,
                                 "malformed quoting in ", buf)
            else
                pushcellproblem!(problems, out, j, sp[1], len, :invalid_value,
                                 "column typed Missing contains ", buf)
            end
        end
    end
    return 0
end

# ---------------------------------------------------------------------------
# Problems: errors as data. Bounded (maxproblems) so a pathological file cannot
# exhaust memory. Retention and final order use source order, not task arrival
# order; the count of omitted reports is itself recorded.
# ---------------------------------------------------------------------------

"""
    CSV.Problem

One recoverable parse problem, as returned by [`CSV.problems`](@ref CSV.problems).
Fields: `row` (1-based data row; 0 for a file- or header-level problem), `col`
(1-based column; 0 for a whole-row problem), `pos` (byte offset into the parsed
bytes), `kind`, and `message`. The kinds are `:short_row`, `:long_row`,
`:invalid_value`, `:invalid_quoted_field`, and `:unclosed_quote`.
"""
struct Problem
    row::Int          # 1-based data row (0 = file-level problem)
    col::Int          # 1-based column (0 = row-level problem)
    pos::Int          # absolute byte offset into the source buffer
    kind::Symbol      # :short_row | :long_row | :invalid_value | :invalid_quoted_field | :unclosed_quote
    message::String
end

function Base.show(io::IO, p::Problem)
    print(io, "CSV.Problem(", p.kind, " at data row ", p.row, ", column ", p.col,
          ", byte ", p.pos, ": ", repr(p.message), ")")
end

"""
    CSV.ParseError <: Exception

Thrown by the readers under `on_error=:error` (or `strict=true`). `problem` is
the source-earliest [`CSV.Problem`](@ref CSV.Problem) in a File or Chunks batch,
`nproblems` counts every problem found, and `source` labels the input.
Rows reports the accessed cell's problem with `nproblems=1`.
"""
struct ParseError <: Exception
    problem::Problem
    nproblems::Int
    source::String
end
ParseError(problem::Problem, nproblems::Int=1) = ParseError(problem, nproblems, "")

function Base.showerror(io::IO, e::ParseError)
    p = e.problem
    print(io, "CSV.ParseError: ", p.kind, " at data row ", p.row, ", column ", p.col,
          " (byte ", p.pos, ")")
    isempty(e.source) || print(io, " in ", e.source)
    print(io, ": ", p.message)
    e.nproblems > 1 && print(io, " (+", e.nproblems - 1, " more)")
    print(io, "\nUse on_error=:collect to keep parsing and inspect ",
          "CSV.problems(file), or on_error=:warn for one summary warning (the eager default).")
end

@noinline _throwparseerror(p::Problem, nproblems::Int, source::String="") =
    throw(ParseError(p, nproblems, source))

# `on_error=:warn`: one summary warning per read, never one line per problem.
function _warnproblems(problems::Vector{Problem}, dropped::Int, source::String,
                       note::String="")
    n = length(problems) + dropped
    n == 0 && return false
    detail = isempty(problems) ? "" :
        (p = first(problems);
         "; first: $(p.kind) at data row $(p.row), column $(p.col): $(p.message)")
    @warn "CSV: $n parse problem$(n == 1 ? "" : "s") in $source$detail. " *
          "Inspect CSV.problems(file); pass on_error=:collect to silence this " *
          "warning or on_error=:error to throw.$note"
    return true
end

const ON_ERROR_MODES = (:collect, :warn, :error)
_checkonerror(on_error::Symbol) =
    on_error in ON_ERROR_MODES ||
        throw(ArgumentError("on_error must be :collect, :warn, or :error (got $(repr(on_error)))"))

mutable struct ProblemLog
    items::Vector{Problem}
    limit::Int
    dropped::Int
    first::Union{Nothing, Problem}
    heaped::Bool                  # items are a max-heap by source order (full logs)
end

function ProblemLog(limit::Int)
    limit >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $limit)"))
    return ProblemLog(Problem[], limit, 0, nothing, false)
end

# Source order: position, then row, column, kind, and message. Symbol comparison
# uses the lexical order of String(Symbol) without materializing either string.
@inline function problemless(a::Problem, b::Problem)
    a.pos != b.pos && return a.pos < b.pos
    a.row != b.row && return a.row < b.row
    a.col != b.col && return a.col < b.col
    a.kind != b.kind && return isless(a.kind, b.kind)
    return a.message < b.message
end

# Bounded retention keeps the `limit` SOURCE-EARLIEST problems. A full log
# maintains its items as a max-heap so displacing the worst retained entry is
# O(log limit); a linear scan per overflow would be quadratic in the cap on
# problem-dense files.
function _siftdown!(items::Vector, lt::F, i::Int) where {F}
    n = length(items)
    @inbounds while true
        l = 2i
        m = i
        l <= n && lt(items[m], items[l]) && (m = l)
        l + 1 <= n && lt(items[m], items[l + 1]) && (m = l + 1)
        m == i && return
        items[i], items[m] = items[m], items[i]
        i = m
    end
end

function _heapify!(items::Vector, lt::F) where {F}
    for i in (length(items) >> 1):-1:1
        _siftdown!(items, lt, i)
    end
end

function pushproblem!(log::ProblemLog, row::Int, col::Int, pos::Int, kind::Symbol, msg::String)
    p = Problem(row, col, pos, kind, msg)
    (log.first === nothing || problemless(p, log.first)) && (log.first = p)
    if length(log.items) < log.limit
        push!(log.items, p)
    else
        log.dropped += 1
        if log.limit > 0
            if !log.heaped
                _heapify!(log.items, problemless)
                log.heaped = true
            end
            @inbounds if problemless(p, log.items[1])
                log.items[1] = p
                _siftdown!(log.items, problemless, 1)
            end
        end
    end
    return
end

function sortproblems!(log::ProblemLog)
    log.heaped = false
    sort!(log.items; lt=problemless)
    return log.items
end

# Whether a problem keyed (row, col, pos) would be retained (or become the
# source-earliest `first`). Callers use it to skip formatting a message that
# the cap would drop: a 5%-ragged 1M-row file otherwise formats a million
# strings to keep ten thousand. Ties on the key say yes, so `pushproblem!`
# keeps the final decision.
function wantsproblem(log::ProblemLog, row::Int, col::Int, pos::Int)
    length(log.items) < log.limit && return true
    if log.limit == 0
        f = log.first
        return f === nothing || _keyless(row, col, pos, f)
    end
    if !log.heaped
        _heapify!(log.items, problemless)
        log.heaped = true
    end
    return _keyless(row, col, pos, @inbounds(log.items[1]))
end

_keyless(row::Int, col::Int, pos::Int, p::Problem) =
    pos != p.pos ? pos < p.pos : row != p.row ? row < p.row : col != p.col ? col < p.col : true

# A zero-byte row: one empty field that starts at the row start and stops at a
# row ending or the end of the input. Under ignorerepeated the stored row start
# sits past the leading delimiter padding, so a row of only delimiters also has
# one empty field at a row ending; the byte before the field tells the two
# apart (a row terminator, or the chunk start, means no padding).
@inline function _emptyrow(buf::Vector{UInt8}, ci::ChunkIndex, nf::Int, sp::Tuple{Int, Int})
    nf == 1 && sp[2] == 0 || return false
    pos = sp[1]
    (pos > length(buf) || @inbounds(buf[pos]) == LF || @inbounds(buf[pos]) == CR) || return false
    pos == ci.start && return true
    prev = @inbounds buf[pos - 1]
    return prev == LF || prev == CR
end

# A ragged-row report: the message is formatted only when it can be retained.
function pushrowproblem!(log::ProblemLog, row::Int, pos::Int, expected::Int, found::Int)
    if wantsproblem(log, row, 0, pos)
        found < expected ?
            pushproblem!(log, row, 0, pos, :short_row,
                         "expected $expected fields, found $found (remaining columns set to missing)") :
            pushproblem!(log, row, 0, pos, :long_row,
                         "expected $expected fields, found $found (extra fields ignored)")
    else
        log.dropped += 1
    end
    return
end

# A cell report whose message excerpts the cell: formatted only when retained.
function pushcellproblem!(log::ProblemLog, row::Int, col::Int, pos::Int, len::Int,
                          kind::Symbol, prefix::String, buf::Vector{UInt8})
    if wantsproblem(log, row, col, pos)
        pushproblem!(log, row, col, pos, kind, prefix * excerpt(buf, pos, len))
    else
        log.dropped += 1
    end
    return
end

struct LocatedProblem
    problem::Problem
    chunk::Int
end

mutable struct PendingProblemLog
    items::Vector{LocatedProblem}
    limit::Int
    dropped::Int
    first::Union{Nothing, LocatedProblem}
    lock::ReentrantLock
    heaped::Bool
end

function PendingProblemLog(limit::Int)
    limit >= 0 || throw(ArgumentError("maxproblems must be ≥ 0 (got $limit)"))
    return PendingProblemLog(LocatedProblem[], limit, 0, nothing, ReentrantLock(), false)
end

locatedless(a::LocatedProblem, b::LocatedProblem) = problemless(a.problem, b.problem)

# Fold one task-local log into the globally bounded reservoir, then release the
# local retained entries. Row ids stay chunk-local until every chunk is indexed.
# Absolute positions are the first problem-key field and chunks do not overlap,
# so later row rebasing cannot change which problems belong under the cap.
# The reservoir keeps the same max-heap-when-full discipline as ProblemLog —
# this loop runs under the lock, so a linear scan per overflow would serialize
# every chunk behind quadratic-by-cap work.
function mergeproblems!(out::PendingProblemLog, log::ProblemLog, chunk::Int,
                        less::F=locatedless) where {F}
    log.first === nothing && return
    lock(out.lock) do
        out.dropped += log.dropped
        if log.first !== nothing
            first = LocatedProblem(log.first, chunk)
            (out.first === nothing || less(first, out.first)) &&
                (out.first = first)
        end
        for p in log.items
            lp = LocatedProblem(p, chunk)
            if length(out.items) < out.limit
                push!(out.items, lp)
            else
                out.dropped += 1
                if out.limit > 0
                    if !out.heaped
                        _heapify!(out.items, less)
                        out.heaped = true
                    end
                    @inbounds if less(lp, out.items[1])
                        out.items[1] = lp
                        _siftdown!(out.items, less, 1)
                    end
                end
            end
        end
    end
    log.items = Problem[]
    log.dropped = 0
    log.first = nothing
    log.heaped = false
    return
end

function rebaseproblem(lp::LocatedProblem, rowbases)
    p = lp.problem
    p.row == 0 && return p
    return Problem(p.row + rowbases[lp.chunk], p.col, p.pos, p.kind, p.message)
end

function finishproblems(log::PendingProblemLog, rowbases)
    out = ProblemLog(log.limit)
    out.items = Problem[rebaseproblem(lp, rowbases) for lp in log.items]
    out.dropped = log.dropped
    out.first = log.first === nothing ? nothing : rebaseproblem(log.first, rowbases)
    return out
end

function excerpt(buf::Vector{UInt8}, pos::Int, len::Int; maxbytes::Int=32)
    n = min(len, maxbytes)
    s = String(buf[pos:pos + n - 1])
    return repr(len > maxbytes ? s * "…" : s)
end

function parseheader!(buf::Vector{UInt8}, ci::ChunkIndex, opts::ValueOpts,
                      d::Dialect, log::ProblemLog)
    hrow = ci.firstdatarow
    nh = nfields(ci, hrow)
    names = Vector{Symbol}(undef, nh)
    for j in 1:nh
        pos, len = fieldspan(ci, hrow, j)::Tuple{Int, Int}
        if len == 0
            names[j] = Symbol("Column", j)
            continue
        end
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            names[j] = Symbol(String(buf[pos:pos + len - 1]))
            pushproblem!(log, 0, j, pos, :invalid_quoted_field,
                         "malformed quoting in header " * excerpt(buf, pos, len))
        elseif st == CELL_MISSING || clen == 0
            names[j] = Symbol("Column", j)
        else
            names[j] = Symbol(esc ?
                              _unescape(buf, Int64(cpos), Int32(clen), opts.e, d.cq) :
                              GC.@preserve(buf, unsafe_string(pointer(buf, cpos), clen)))
        end
    end
    ci.firstdatarow = hrow + 1
    return names
end

# ---------------------------------------------------------------------------
# L5: the driver.
# ---------------------------------------------------------------------------

struct ParsedTable
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
    problems::Vector{Problem}
    droppedproblems::Int
end

Base.names(t::ParsedTable) = t.names
columns(t::ParsedTable) = t.columns
problems(t::ParsedTable) = t.problems
function Base.getindex(t::ParsedTable, nm::Symbol)
    j = findfirst(==(nm), t.names)
    j === nothing && throw(KeyError(nm))
    return t.columns[j]
end

function Base.show(io::IO, t::ParsedTable)
    print(io, "CSV.ParsedTable: $(t.nrows) × $(length(t.names))")
    for (nm, col) in zip(t.names, t.columns)
        print(io, "\n  ", nm, "::", eltype(col))
    end
    isempty(t.problems) || print(io, "\n  ($(length(t.problems)) problem(s) recorded)")
end

# Read up to `nsample` rows at even positions across the full input. This checks
# both early and late rows before value parsing starts.
function sampletypes(buf::Vector{UInt8}, chunks::Vector{ChunkIndex}, ncols::Int,
                     opts::ValueOpts; nsample::Int=128,
                     selected::Union{Nothing, Vector{Bool}}=nothing,
                     sawmissing::Union{Nothing, Vector{Bool}}=nothing,
                     colopts::Union{Nothing, Vector{ValueOpts}}=nothing,
                     maxrows::Union{Nothing, Int}=nothing)
    nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    total = sum(nrows, chunks; init=0)
    # rows past `limit` are never output: they must not seed union finals
    maxrows === nothing || (total = min(total, maxrows))
    total == 0 && return fill(Missing, ncols)
    types = fill(Missing, ncols)
    count = min(total, nsample)
    for k in 1:count
        # Exact integer interpolation includes both ends without duplicates.
        gr = count == 1 ? 1 :
             1 + Int(widemul(k - 1, total - 1) ÷ (count - 1))
        ci, lr = locate(chunks, gr)
        sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing, colopts)
    end
    return types
end

@inline function sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing=nothing,
                               colopts=nothing)
    for j in 1:ncols
        selected !== nothing && !selected[j] && continue
        sp = fieldspan(ci, lr, j)
        if sp === nothing
            sawmissing === nothing || (sawmissing[j] = true)
            continue
        end
        dt = detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j))
        sawmissing !== nothing && dt === Missing && (sawmissing[j] = true)
        types[j] = promote_kernel(types[j], dt)
    end
    return
end

# Read sample rows only from the rows that pass the filter. Type detection must
# not use rows that the result excludes.
function sampletypesrows(buf::Vector{UInt8}, chunks::Vector{ChunkIndex}, rowbases0,
                         qrows::Vector{Int}, ncols::Int, opts::ValueOpts,
                         selected::Union{Nothing, Vector{Bool}}; nsample::Int=128,
                         sawmissing::Union{Nothing, Vector{Bool}}=nothing,
                         colopts::Union{Nothing, Vector{ValueOpts}}=nothing)
    types = fill(Missing, ncols)
    total = length(qrows)
    total == 0 && return types
    count = min(total, nsample)
    for k in 1:count
        gr = qrows[count == 1 ? 1 : 1 + Int(widemul(k - 1, total - 1) ÷ (count - 1))]
        # locate via the precomputed bases (all chunks are indexed on this path)
        ki = searchsortedlast(rowbases0, gr - 1)
        ci = chunks[ki]
        lr = ci.firstdatarow + (gr - rowbases0[ki]) - 1
        sampledetect!(types, buf, ci, lr, ncols, opts, selected, sawmissing, colopts)
    end
    return types
end

# Map a global data-row id to (chunk, local row).
function locate(chunks::Vector{ChunkIndex}, grow::Int)
    for ci in chunks
        n = nrows(ci)
        grow <= n && return (ci, ci.firstdatarow + grow - 1)
        grow -= n
    end
    throw(BoundsError(chunks, grow))
end

allocatecolumn(::Type{Missing}, n::Int, buf, e, cq) = nothing
allocatecolumn(::Type{String}, n::Int, buf, e, cq) = StringColumn(n, e, cq)
allocatecolumn(::Type{T}, n::Int, buf, e, cq) where {T} = TypedColumn{T}(n)

# Number of leading chunks needed to cover `limit` data rows (all of them when
# the file is shorter than the limit).
function _limitchunks(chunks::Vector{ChunkIndex}, rowbases::Vector{Int}, limit::Int)
    lastk = 0
    for k in eachindex(chunks)
        lastk = k
        rowbases[k] + nrows(chunks[k]) >= limit && break
    end
    return lastk
end

# A column request records the user's type intent for one source column.
# `parsetype` is the type used by the scalar parser. `resulttype` is set only
# for an explicit string output type or a checked numeric conversion.
struct ColumnDecision
    parsetype::Union{Nothing, Type}
    resulttype::Union{Nothing, Type}
    declaredmissing::Bool
end

ColumnDecision() = ColumnDecision(nothing, nothing, false)

# One plan holds the final column rules for one read. `columns` has one item for
# each input column. `sources` lists the input columns to read, in file order.
# `positions` gives their positions in the columns available to this read. A
# scan can also list the input columns needed by its filter.
struct ColumnPlan
    columns::Vector{ColumnDecision}
    sources::Vector{Int}
    positions::Vector{Int}
    predicate::Vector{Int}
    opts::ValueOpts
    colopts::Union{Nothing, Vector{ValueOpts}}
end

columnopts(p::ColumnPlan, j::Int) =
    p.colopts === nothing ? p.opts : @inbounds(p.colopts[j])

function accessparsetype(d::ColumnDecision)
    return d.resulttype === nothing ? d.parsetype : d.resulttype
end

function _selectedmask(p::ColumnPlan, ncols::Int)
    length(p.sources) == ncols &&
        all(j -> @inbounds(p.sources[j]) == j, 1:ncols) && return nothing
    selected = fill(false, ncols)
    selected[p.sources] .= true
    return selected
end

# Narrow numeric requests use a wider scalar parser, followed by a checked
# conversion. This keeps the scalar parsers small and preserves range errors.
const NARROW_TYPES = Dict{Type, Type}(
    Int8 => Int64, Int16 => Int64, Int32 => Int64,
    UInt8 => Int64, UInt16 => Int64, UInt32 => Int64, UInt64 => Int128,
    Float16 => Float64, Float32 => Float64)
_nativetype(T::Type) = get(NARROW_TYPES, T, T)
# Extensions register exact parsers for their scalar types (DataDecimals).
_parseable(::Type) = false

# A user-defined scalar type parses through `Parsers.tryparse` on the field
# bytes when the type defines that method, and through `Base.tryparse` on a
# `String` otherwise. Parsers has a fallback span method for every type, so
# the check asks which method applies rather than whether one exists.
const _PARSERS_SPAN_FALLBACK = which(Parsers.tryparse, Tuple{Type{Any}, Vector{UInt8}, Int, Int})
_hasspanparser(T::Type) = hasmethod(Parsers.tryparse, Tuple{Type{T}, Vector{UInt8}, Int, Int}) &&
    which(Parsers.tryparse, Tuple{Type{T}, Vector{UInt8}, Int, Int}) !== _PARSERS_SPAN_FALLBACK
_customparseable(T::Type) = isconcretetype(T) &&
    (_hasspanparser(T) || hasmethod(Base.tryparse, Tuple{Type{T}, String}))

# Dict keys can be an integer position, a name, or a regular expression. An
# exact key takes precedence over a regular expression.
function _resolvekeys(dict::AbstractDict, names::Vector{Symbol}, ncols::Int, what::String;
                      validate::Bool=true)
    out = Dict{Int, Any}()
    for (k, v) in dict
        k isa Regex && continue
        if k isa Integer && !(1 <= k <= ncols)
            validate || continue
            throw(ArgumentError("$what key $k out of range"))
        end
        j = k isa Integer ? Int(k) : findfirst(==(Symbol(k)), names)
        if j === nothing || !(1 <= j <= ncols)
            validate || continue
            j === nothing && throw(ArgumentError("$what key $k does not match any column"))
            throw(ArgumentError("$what key $k out of range"))
        end
        out[j] = v
    end
    for (k, v) in dict
        k isa Regex || continue
        matched = false
        for (j, nm) in enumerate(names)
            occursin(k, String(nm)) || continue
            matched = true
            haskey(out, j) || (out[j] = v)
        end
        matched || !validate ||
            throw(ArgumentError("$what key $k does not match any column"))
    end
    return out
end

function _columndecision(T)
    T === nothing && return ColumnDecision()
    T isa Type ||
        throw(ArgumentError("column type must be a Type or nothing (got $(repr(T)))"))
    declaredmissing = T !== Missing && Missing <: T
    requested = T === Missing ? Missing : Base.nonmissingtype(T)
    # A requested string type names the OUTPUT type: `types=String` returns
    # `Vector{String}`, `DataString` keeps the parsed column as it is, and an
    # extension type (InlineString) converts once after parsing. Text is
    # always parsed as a DataString column first.
    if requested !== Missing && _stringsink(requested)
        return ColumnDecision(String, requested, declaredmissing)
    end
    parsetype = _nativetype(requested)
    parseable = parsetype === Missing ||
                parsetype in (Int64, Int128, Float64, Bool, Char, Date, DateTime, Time,
                              String, BigInt, BigFloat, Base.UUID) ||
                parsetype <: Timestamp ||
                _parseable(parsetype) ||
                _customparseable(parsetype)
    parseable || throw(ArgumentError(
        "unsupported column type $parsetype: define " *
        "Parsers.tryparse(::Type{$parsetype}, buf::AbstractVector{UInt8}, i::Int, j::Int) " *
        "or Base.tryparse(::Type{$parsetype}, ::String)"))
    resulttype = haskey(NARROW_TYPES, requested) ? requested : nothing
    return ColumnDecision(parsetype, resulttype, declaredmissing)
end

function _selectpositions(select, drop, names::Vector{Symbol};
                          matchnormalized::Bool=false)
    select !== nothing && drop !== nothing &&
        throw(ArgumentError("select and drop are mutually exclusive"))
    spec = select === nothing ? drop : select
    spec === nothing && return collect(eachindex(names))
    spec isa Base.Callable &&
        throw(ArgumentError("function-typed select/drop is retired; pass a list, " *
                            "a Regex, or use Tables.Scan for expressions"))
    if spec isa Regex
        re = spec   # the comprehension captures a single-assignment name
        matched = [nm for nm in names if occursin(re, String(nm))]
        select !== nothing && isempty(matched) &&
            throw(ArgumentError("select regex $spec does not match any column"))
        spec = matched
    end
    # one name or position is the one-element list
    (spec isa AbstractString || spec isa Symbol || spec isa Integer) && (spec = [spec])
    idx = Int[]
    if spec isa AbstractVector{Bool}
        length(spec) == length(names) ||
            throw(ArgumentError("Bool select/drop length $(length(spec)) != " *
                                "$(length(names)) columns"))
        append!(idx, findall(spec))
    elseif spec isa AbstractVector{<:Integer}
        append!(idx, Int.(spec))
    else
        spec isa AbstractVector || spec isa Tuple ||
            throw(ArgumentError("select/drop must be a list of names, positions, or a " *
                                "Bool mask (got $(typeof(spec)))"))
        for s in spec
            j = findfirst(==(Symbol(s)), names)
            if j === nothing && matchnormalized
                j = findfirst(==(Symbol(normalizename(String(s)))), names)
            end
            j === nothing &&
                throw(ArgumentError("select/drop name $s does not match any column"))
            push!(idx, j)
        end
    end
    all(j -> 1 <= j <= length(names), idx) ||
        throw(ArgumentError("select/drop index out of range"))
    return drop === nothing ? sort!(unique(idx)) : setdiff(1:length(names), idx)
end

function _applytypes!(columns::Vector{ColumnDecision}, types, names::Vector{Symbol},
                      available::Vector{Int}; validate::Bool=true)
    types === nothing && return columns
    if types isa Type
        decision = _columndecision(types)
        for j in available
            columns[j] = decision
        end
    elseif types isa AbstractVector
        length(types) == length(available) ||
            throw(ArgumentError("types vector length $(length(types)) != " *
                                "$(length(available)) columns"))
        for (k, j) in enumerate(available)
            columns[j] = _columndecision(types[k])
        end
    elseif types isa AbstractDict
        visible = names[available]
        for (k, T) in _resolvekeys(types, visible, length(visible), "types"; validate)
            columns[available[k]] = _columndecision(T)
        end
    else
        throw(ArgumentError("unsupported types specification: $(typeof(types))"))
    end
    return columns
end

"""
    settlecolumns(names, opts; keywords...) -> ColumnPlan

Resolve selection, types, missing values, input positions, and field parsing
options once for one read. The result has one item for each input column.
Selected input columns stay unique and in file order.
"""
function settlecolumns(names::Vector{Symbol}, opts::ValueOpts;
                       select=nothing, drop=nothing, types=nothing,
                       available::Union{Nothing, Vector{Int}}=nothing,
                       colopts::Union{Nothing, Vector{ValueOpts}}=nothing,
                       validate::Bool=true, matchnormalized::Bool=false)
    return settlecolumns(names, opts, select, drop, types, available, colopts,
                         validate, matchnormalized)
end

Base.@nospecializeinfer function settlecolumns(names::Vector{Symbol}, opts::ValueOpts,
                       @nospecialize(select), @nospecialize(drop), @nospecialize(types),
                       @nospecialize(available::Union{Nothing, Vector{Int}}),
                       @nospecialize(colopts::Union{Nothing, Vector{ValueOpts}}),
                       validate::Bool, matchnormalized::Bool)
    ncols = length(names)
    allavailable = available === nothing
    visible = allavailable ? collect(1:ncols) : copy(available)
    (issorted(visible) && allunique(visible) && all(j -> 1 <= j <= ncols, visible)) ||
        throw(ArgumentError("available columns must be unique, in file order, and in range"))
    colopts === nothing || length(colopts) == ncols ||
        throw(ArgumentError("colopts length $(length(colopts)) != $ncols columns"))
    if select === nothing && drop === nothing
        sources = visible
        positions = allavailable ? sources : collect(eachindex(visible))
    else
        positions = _selectpositions(select, drop, names[visible]; matchnormalized)
        sources = visible[positions]
    end
    columns = [ColumnDecision() for _ in 1:ncols]
    _applytypes!(columns, types, names, visible; validate)
    return ColumnPlan(columns, sources, positions, Int[], opts, colopts)
end

_defaultchunkbytes(nbytes::Int, nthreads::Int=Threads.nthreads()) =
    clamp(cld(nbytes, 4 * nthreads), 1 << 16, 1 << 20)

# Split rows 1:n into contiguous ranges of at least `minrows` and run
# `f(lo, hi)` on each in its own task (serially when the budget is one or
# the column is short). Post-parse passes that touch every cell of a long
# column (string materialization, string-type conversion) scale this way.
const _ROWS_PER_TASK = 1 << 16
function _rowranges(f, n::Int, tasklimit::Int=Threads.nthreads(), minrows::Int=_ROWS_PER_TASK)
    nt = clamp(n ÷ minrows, 1, tasklimit)
    if nt <= 1
        n > 0 && f(1, n)
        return
    end
    _conversionforeach(1:nt, nt) do t
        lo = 1 + (t - 1) * n ÷ nt
        hi = t * n ÷ nt
        f(lo, hi)
    end
    return
end

# Bounded conversion jobs, where a failure surfaces as the task's own exception (an
# over-long InlineString value is an ArgumentError to the caller, not a
# TaskFailedException). For post-parse conversions, whose failures are user
# errors, not internal ones.
function _conversionforeach(f, items, tasklimit::Int)
    try
        _taskforeach(f, items, tasklimit)
    catch e
        rethrow(_unwrapfailure(e))
    end
    return
end

_readtasklimit(parallel::Bool, ntasks::Union{Nothing, Int}) =
    parallel ? min(something(ntasks, Threads.nthreads()), Threads.nthreads()) : 1

_unwrapfailure(e) = e
_unwrapfailure(e::CompositeException) =
    isempty(e.exceptions) ? e : _unwrapfailure(first(e.exceptions))
_unwrapfailure(e::TaskFailedException) = _unwrapfailure(e.task.result)

# Run work with no more than `tasklimit` Julia tasks. Do not start one task for
# each chunk. A stored index can have more chunks than a later `ntasks=N`
# request allows.
function _taskforeach(f, items, tasklimit::Int, taskobserver=nothing)
    n = length(items)
    n == 0 && return nothing
    workers = min(tasklimit, n)
    if workers <= 1
        foreach(f, items)
        return nothing
    end
    next = Threads.Atomic{Int}(1)
    @sync for _ in 1:workers
        @wkspawn begin
            started = false
            try
                if taskobserver !== nothing
                    taskobserver(true)
                    started = true
                end
                while true
                    i = Threads.atomic_add!(next, 1)
                    i > n && break
                    f(@inbounds items[i])
                end
            finally
                started && taskobserver(false)
            end
        end
    end
    return nothing
end

"""
    CSV.parse(buf::Vector{UInt8}; kwargs...) -> ParsedTable
    CSV.parse(str::AbstractString; kwargs...)
    CSV.parse(io::IO; kwargs...)

Read delimited data and return a `ParsedTable`. The parser first sets chunk
boundaries at complete row endings. It builds the row and field index for all
chunks. It then reads rows from across the input to select an initial type for
each column. If a later value needs a different type, the parser changes that
column type and reads only the affected parts again. `parallel` selects tasks
or plain loops. It does not change the chunk layout. The default `chunkbytes` is
`clamp(cld(length(buf), 4 * Threads.nthreads()), 64 KiB, 1 MiB)`; the default
`nsample` is `clamp(probe_rows >> 6, 8, 128)`. Explicit values override both
defaults.
By default, the result records invalid data in its problem list.
`on_error=:error` throws the first problem in source order after parsing.

Keywords: `delim`, `quotechar`, `openquotechar`/`closequotechar`, `escapechar`,
`quoted`, `comment`, `ignoreemptyrows`, `ignorerepeated`, `header` (true | false | Vector), `types`
(Type | Vector | Dict), `dateformat`, `decimal`, `truestrings`/`falsestrings`,
`sentinels` (spellings that parse as missing), `stripwhitespace`, `groupmark`,
`chunkbytes`, `parallel`, `ntasks`, `fastindex`, `maxproblems`,
`on_error` (:collect | :error), `validate`, `nsample`.
"""
function parse(buf::Vector{UInt8};
               header::Union{Bool, AbstractVector}=true,
               types=nothing,
               dateformat=nothing,
               decimal::Char='.',
               truestrings=nothing,
               falsestrings=nothing,
               sentinels=nothing,
               stripwhitespace::Bool=false,
               groupmark::Union{Nothing, Char}=nothing,
               typemap::Union{Nothing, AbstractDict}=nothing,
               colopts::Union{Nothing, Vector{ValueOpts}}=nothing,
               chunkbytes::Union{Nothing, Int}=nothing,
               parallel::Bool=Threads.nthreads() > 1,
               ntasks::Union{Nothing, Int}=nothing,
               fastindex::Bool=true,
               maxproblems::Int=10_000,
               on_error::Symbol=:collect,
               validate::Bool=true,
               nsample::Union{Nothing, Int}=nothing,
               select=nothing,
               columnplan::Union{Nothing, ColumnPlan}=nothing,
               limit::Union{Nothing, Int}=nothing,
               rowmask::Union{Nothing, Vector{Bool}}=nothing,
               index::Union{Nothing, BufferIndex}=nothing,
               reportstructural::Bool=true,
               dialectkw...)
    on_error in (:collect, :error) || throw(ArgumentError("on_error must be :collect or :error"))
    limit === nothing || limit >= 0 || throw(ArgumentError("limit must be ≥ 0 (got $limit)"))
    limit !== nothing && rowmask !== nothing &&
        throw(ArgumentError("limit and rowmask cannot be combined; bake the limit into the mask"))
    tm = _normalizetypemap(typemap)
    nsample === nothing || nsample >= 1 || throw(ArgumentError("nsample must be ≥ 1 (got $nsample)"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    tasklimit = _readtasklimit(parallel, ntasks)
    # The default chunk size aims for four chunks per thread. It stays between
    # 64 KiB and 1 MiB. The lower limit avoids too much setup work for small
    # chunks. The upper limit keeps each column pass on a small part of the input.
    #
    # The default type sample grows with the row count. It stays between 8 and
    # 128 rows. This limits repeated work on small files and checks more of a
    # large file before value parsing starts.
    if chunkbytes === nothing
        chunkbytes = _defaultchunkbytes(length(buf))
    else
        chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    end
    d = Dialect(; dialectkw...)
    baseopts = makevalueopts(d; dateformat, decimal, truestrings, falsestrings, sentinels,
                             stripwhitespace, groupmark)
    sc = resolvescanner(d, fastindex)
    return _parse(buf, d, baseopts, sc, tm, chunkbytes, parallel, tasklimit, maxproblems,
                  on_error, validate, reportstructural, nsample, limit,
                  header, types, select, colopts, columnplan, rowmask, index)
end

# The driver body takes positional, concretely typed arguments so it compiles
# once per index/mask shape rather than once per keyword combination: every
# distinct keyword set (`delim`, `comment`, `dateformat`, `missingstring`, ...)
# would otherwise specialize this whole function again on first use.
Base.@nospecializeinfer function _parse(buf::Vector{UInt8}, d::Dialect, baseopts::ValueOpts, sc::Symbol,
                @nospecialize(tm::Union{Nothing, Dict{Type, Type}}), chunkbytes::Int, parallel::Bool,
                tasklimit::Int, maxproblems::Int, on_error::Symbol, validate::Bool,
                reportstructural::Bool,
                @nospecialize(nsample::Union{Nothing, Int}),
                @nospecialize(limit::Union{Nothing, Int}), @nospecialize(header), @nospecialize(types),
                @nospecialize(select), colopts::Union{Nothing, Vector{ValueOpts}},
                columnplan::Union{Nothing, ColumnPlan}, rowmask::Union{Nothing, Vector{Bool}},
                index::Union{Nothing, BufferIndex})
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1  # BOM
    # A caller can supply an index that it built earlier. The Scan integration
    # does this when it applies a filter in two steps. The chunk boundaries and
    # CSV options must match the options used to build that index.
    #
    # Assign each captured local value only once. Julia can put a captured value
    # in a `Core.Box` when the code assigns it more than once. Tasks would then
    # share a mutable value, and the compiler could not know its exact type.
    allchunks::Vector{ChunkIndex} = index === nothing ?
        chunkplan(buf, d, datastart, chunkbytes, parallel, tasklimit) : index.chunks
    indexed = fill(index !== nothing, length(allchunks))
    indexunclosed = index !== nothing && index.unclosedquote
    nchall = length(allchunks)
    headerlog = ProblemLog(maxproblems)

    # -- build all chunk indexes ---------------------------------------------
    # Index every chunk before parsing field values. This gives the exact row
    # count and output position for each chunk. The parser can then allocate the
    # final columns once and write values directly into them. It does not need
    # temporary columns for each chunk or a later copy step.
    toindex = [k for k in 1:nchall if !indexed[k]]
    if tasklimit > 1 && length(toindex) > 1
        _taskforeach(toindex, tasklimit) do k
            indexone!(allchunks[k], buf, d, sc)
            indexed[k] = true
        end
    else
        for k in toindex
            indexone!(allchunks[k], buf, d, sc)
            indexed[k] = true
        end
    end
    # A comment row held a quote byte: the parallel plan and the fast scan
    # assumed none. Rebuild in file order with the scalar scanner.
    if index === nothing && commentaware(d) && !d.commentquotes && !d.lenient &&
       any(ci -> ci.commentquote, allchunks)
        return _parse(buf, withcommentquotes(d), baseopts, :scalar, tm, chunkbytes, parallel,
                      tasklimit, maxproblems, on_error, validate,
                      reportstructural, nsample, limit, header, types, select, colopts,
                      columnplan, rowmask, nothing)
    end
    # A quote that did not start its field makes the toggle scan (and the
    # parity planner behind it) unsound: rows may have merged into one cell.
    # Rebuild everything under the lenient quote rule; well-formed input never
    # takes this path.
    if !d.lenient && any(ci -> ci.barequote, allchunks)
        return _parse(buf, withlenient(d), baseopts, :lenient, tm, chunkbytes, parallel,
                      tasklimit, maxproblems, on_error, validate,
                      reportstructural, nsample, limit, header, types, select, colopts,
                      columnplan, rowmask, nothing)
    end
    # The header is in the first chunk that remains after empty and comment rows
    # are removed.
    headerchunk = something(findfirst(k -> totalrows(allchunks[k]) > 0, 1:nchall), 0)

    # -- header & column names ------------------------------------------------
    local names::Vector{Symbol}
    if header === true && headerchunk > 0
        ci = allchunks[headerchunk]
        names = parseheader!(buf, ci, baseopts, d, headerlog)
    elseif header isa AbstractVector
        names = Symbol.(header)
    else
        ncg = headerchunk == 0 ? 0 :
              nfields(allchunks[headerchunk], allchunks[headerchunk].firstdatarow)
        names = [Symbol("Column", j) for j in 1:ncg]
    end
    names = makeunique!(names)
    ncols = length(names)
    fullrows = sum(nrows, allchunks; init=0)

    # -- column requests and row geometry ---------------------------------------
    if columnplan !== nothing
        types === nothing ||
            throw(ArgumentError("types cannot be combined with a settled column plan"))
        select === nothing ||
            throw(ArgumentError("select cannot be combined with a settled column plan"))
        length(columnplan.columns) == ncols ||
            throw(ArgumentError("column plan has $(length(columnplan.columns)) columns; " *
                                "input has $ncols"))
        colopts === nothing || colopts === columnplan.colopts ||
            throw(ArgumentError("colopts do not match the settled column plan"))
    end
    plan = columnplan === nothing ?
           settlecolumns(names, baseopts; select, types, colopts, validate) : columnplan
    opts = plan.opts
    columnopts = plan.colopts
    selected = _selectedmask(plan, ncols)
    # every chunk is indexed: global row bases are simply known
    rowbasesall = cumsum([0; Int[nrows(ci) for ci in allchunks[1:max(nchall - 1, 0)]]])
    if rowmask !== nothing
        length(rowmask) == fullrows ||
            throw(ArgumentError("rowmask length $(length(rowmask)) != $fullrows data rows"))
    end
    # Keep whole chunks up to the limit boundary. `sampletypes(maxrows=limit)`
    # restricts inference to the retained prefix of the boundary chunk. The
    # working `chunks`/`rowbases0`/`nch` bind exactly once, here.
    nch = limit === nothing ? nchall : _limitchunks(allchunks, rowbasesall, limit)
    chunks = nch == nchall ? allchunks : allchunks[1:nch]
    rowbases0 = nch == nchall ? rowbasesall : rowbasesall[1:nch]

    # -- select initial column types ------------------------------------------
    seed = Union{Nothing, Type}[d.parsetype for d in plan.columns]
    userprovided = [d.parsetype !== nothing for d in plan.columns]
    wantmissing = [d.declaredmissing for d in plan.columns]
    # typed columns whose sample shows a missing cell get union-direct finals
    # (the parse writes Vector{Union{T,Missing}} in place; conversion is never
    # paid). Sample-missed sparse missings fall back to a finalize conversion.
    sawmissing = copy(wantmissing)   # declared Union{Missing,T} ⇒ union finals
    if any(j -> seed[j] === nothing && (selected === nothing || selected[j]), 1:ncols)
        if rowmask === nothing
            probechunks = ChunkIndex[ci for ci in chunks if nrows(ci) > 0]
            probetotal = sum(nrows, probechunks; init=0)
            ns = nsample === nothing ? clamp(probetotal >> 6, 8, 128) : nsample
            inferred = sampletypes(buf, probechunks, ncols, opts; nsample=max(ns, 1), selected,
                                   sawmissing, colopts=columnopts, maxrows=limit)
        else
            # inference reflects the rows that will actually be output: a
            # masked-out malformed value must not promote a qualifying column
            qrows = findall(rowmask)
            ns = nsample === nothing ? clamp(length(qrows) >> 6, 8, 128) : nsample
            inferred = sampletypesrows(buf, chunks, rowbases0, qrows, ncols, opts, selected;
                                       nsample=max(ns, 1), sawmissing, colopts=columnopts)
        end
        for j in 1:ncols
            seed[j] === nothing && (seed[j] = _maptype(tm, inferred[j]))
        end
    end
    if selected !== nothing
        # unselected columns are never parsed; give unseeded ones a placeholder
        for j in 1:ncols
            !selected[j] && seed[j] === nothing && (seed[j] = Missing)
        end
    end

    # -- value wave ------------------------------------------------------------
    # Chunks are already indexed. Each chunk task reports its ragged rows with
    # chunk-local ids into a task-local log (folded once into the bounded
    # reservoir), parses every selected column, and promotes through the shared
    # `promo` register with an immediate hot re-parse on conflict. The unmasked
    # driver writes final columns directly; the masked driver stages and
    # stitches compactly.
    promo = Type[T for T in seed]
    promolock = ReentrantLock()
    segments = Vector{Vector{Any}}(undef, nch)
    segtypes = Vector{Vector{Type}}(undef, nch)
    pendingproblems = PendingProblemLog(maxproblems)
    mergeproblems!(pendingproblems, headerlog, 0)
    chunkrows = Int[nrows(ci) for ci in chunks]
    if limit !== nothing && nch > 0
        # only the retained prefix of the boundary chunk is written/reported
        chunkrows[end] = min(chunkrows[end], limit - rowbases0[end])
    end
    rowbases = cumsum([0; chunkrows[1:max(nch - 1, 0)]])
    ndata = rowmask === nothing ? sum(chunkrows; init=0) : count(rowmask)
    cols = Vector{AbstractVector}(undef, ncols)
    stitchjs = plan.sources
    mb = k -> rowmask === nothing ? 0 : rowbases0[k]
    # A concrete Bool/Int capture keeps worker callbacks the same type for
    # bounded and unbounded reads, including a new limit + typemap combination.
    limitenabled = limit !== nothing
    limitend = something(limit, 0)
    rl = k -> limitenabled ? clamp(limitend - rowbases0[k], 0, nrows(chunks[k])) : typemax(Int)

    if rowmask === nothing
        # -- write directly into the final columns ----------------------------
        # All chunk indexes are complete, so each chunk knows its output rows.
        # It writes values into the final columns. It does not need temporary
        # columns or a later copy step. The API layer can encode repeated strings
        # after this step; the parser does not encode them.
        directwave!(cols, chunks, buf, d, opts, ncols, userprovided, promo,
                    promolock, pendingproblems, segments, segtypes, selected,
                    rowbases, ndata, rl, reportstructural, parallel,
                    tasklimit, sawmissing, tm, columnopts)
        for k in 1:(nch - 1)
            chunks[k].unclosedquote &&
                error("internal error: chunk $(k) ended inside a quoted field")
        end
    else
        # -- masked wave: chunk-local staging + compacting stitch --------------
        # (the two-phase filter path; excluded rows never parse, output
        # positions gather compactly)
        if tasklimit > 1 && nch > 1
            _taskforeach(1:nch, tasklimit) do k
                fusedchunk!(chunks[k], buf, d, ncols, opts, userprovided, promo,
                            promolock, pendingproblems, segments, segtypes, k,
                            selected, rowmask, mb(k), rl(k), reportstructural,
                            tm, columnopts)
            end
        else
            for k in 1:nch
                fusedchunk!(chunks[k], buf, d, ncols, opts, userprovided,
                            promo, promolock, pendingproblems, segments, segtypes, k,
                            selected, rowmask, mb(k), rl(k), reportstructural,
                            tm, columnopts)
            end
        end
        for k in 1:(nch - 1)
            chunks[k].unclosedquote &&
                error("internal error: chunk $(k) ended inside a quoted field")
        end
        _settletimestampwidening!(promo, segtypes, chunks, buf, opts, stitchjs, rl,
                                  columnopts, rowmask, rowbases0)
        # unify: re-parse the (rare) segments parsed under a stale type.
        # `promo` is frozen now; a Missing segment upgrades without work.
        finalstaged = Type[promo[j] for j in 1:ncols]
        stale = Tuple{Int, Int}[]
        for k in 1:nch, j in 1:ncols
            T = segtypes[k][j]
            T !== finalstaged[j] && T !== Missing && push!(stale, (k, j))
        end
        if !isempty(stale)
            if tasklimit > 1 && length(stale) > 1
                _taskforeach(stale, tasklimit) do x
                    k, j = x
                    restale!(chunks, finalstaged, segments, segtypes, pendingproblems,
                             buf, opts, d, userprovided, k, j, rowmask, mb(k), rl(k),
                             columnopts)
                end
            else
                for (k, j) in stale
                    restale!(chunks, finalstaged, segments, segtypes, pendingproblems, buf,
                             opts, d, userprovided, k, j, rowmask, mb(k), rl(k),
                             columnopts)
                end
            end
        end
        parallelstitch = tasklimit > 1 && length(stitchjs) > 1 && ndata > 0 && length(chunks) > 1
        columnbudget = parallelstitch ? 1 : tasklimit
        stitchcol = j -> (cols[j] = stitchcolumn(finalstaged[j], segments, segtypes, j, chunkrows,
                                                 rowbases, ndata, buf, opts.e, d.cq,
                                                 rowmask, rowbases0; tasklimit=columnbudget))
        # single-chunk stitches are zero-copy finalizes — never worth a task spawn
        if parallelstitch
            _taskforeach(stitchcol, stitchjs, tasklimit)
        else
            foreach(stitchcol, stitchjs)
        end
    end

    # -- problems: rebase chunk-local rows, merge, deterministic cap -----------
    # problem rows always reference INPUT data-row numbers (diagnostics point
    # at the file, not at the filtered output)
    log = finishproblems(pendingproblems, rowmask === nothing ? rowbases : rowbases0)
    hasunclosed = indexunclosed || (nch > 0 && last(chunks).unclosedquote)
    unclosedincluded = rowmask === nothing || fullrows == 0 || rowmask[end]
    if reportstructural && hasunclosed && unclosedincluded &&
       (limit === nothing || limit >= fullrows)
        pushproblem!(log, 0, 0, length(buf), :unclosed_quote,
                     "input ended inside a quoted field")
    end

    # -- finalize --------------------------------------------------------------
    sortproblems!(log)
    on_error === :error && log.first !== nothing &&
        _throwparseerror(log.first, length(log.items) + log.dropped)
    # a user-declared Union{Missing,T} is the column type even without missings
    for j in stitchjs
        wantmissing[j] || continue
        c = cols[j]
        Missing <: eltype(c) && continue
        cols[j] = _widenmissing(c)
    end
    selected === nothing && return ParsedTable(names, cols, ndata, log.items, log.dropped)
    return ParsedTable(names[stitchjs], cols[stitchjs], ndata, log.items, log.dropped)
end

parse(str::AbstractString; kw...) = parse(Vector{UInt8}(codeunits(str)); kw...)
parse(io::IO; kw...) = parse(Base.read(io); kw...)

chunkrowbase(chunks::Vector{ChunkIndex}, target::ChunkIndex) =
    sum(nrows(c) for c in chunks if c.start < target.start; init=0)

# One masked-driver task: report ragged rows with chunk-local row ids and parse
# every selected column into chunk-local segment storage. All chunks are indexed
# by the unconditional index wave before this function can run.
function fusedchunk!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, ncols::Int,
                     opts::ValueOpts,
                     userprovided, promo, promolock, pendingproblems::PendingProblemLog,
                     segments, segtypes, k::Int,
                     selected::Union{Nothing, Vector{Bool}}=nothing,
                     mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                     reportlimit::Int=typemax(Int), reportstructural::Bool=true,
                     tm=nothing, colopts=nothing)
    n = nrows(ci)
    log = ProblemLog(pendingproblems.limit)
    if reportstructural
        for lr in ci.firstdatarow:totalrows(ci)
            localrow = lr - ci.firstdatarow + 1
            mask !== nothing && !mask[maskbase + localrow] && continue
            localrow > reportlimit && continue
            nf = nfields(ci, lr)
            if nf != ncols
                sp = fieldspan(ci, lr, nf < ncols ? 1 : ncols + 1)::Tuple{Int, Int}
                # a kept empty row (ignoreemptyrows=false) is all-missing by
                # request, not a short row to report
                _emptyrow(buf, ci, nf, sp) || pushrowproblem!(log, localrow, sp[1], ncols, nf)
            end
        end
    end
    segs = Vector{Any}(undef, ncols)
    st = Vector{Type}(undef, ncols)
    for j in 1:ncols
        if selected !== nothing && !selected[j]
            # unselected columns simply don't exist to the value layer
            segs[j] = nothing
            st[j] = Missing
            continue
        end
        T = lock(() -> promo[j], promolock)
        attempts = 0
        while true
            (attempts += 1) > 8 && error("internal error: promotion did not converge")
            stg = allocatecolumn(T, n, buf, opts.e, d.cq)
            conflict = T === Missing ?
                parsecolchunk_missing(buf, ci, j, 0, _copts(colopts, opts, j),
                                      userprovided[j], log, mask,
                                      maskbase, reportlimit) :
                parsecolchunk!(stg, buf, ci, j, 0, _copts(colopts, opts, j),
                               userprovided[j], log, 0, mask,
                               maskbase, reportlimit)
            if conflict == 0
                segs[j] = stg
                st[j] = T
                break
            end
            sp = fieldspan(ci, conflict, j)::Tuple{Int, Int}
            detected = promote_kernel(T, detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j)))
            # single assignment: the lock closure captures it, and a captured
            # local that is reassigned is boxed (and shared across tasks)
            promoT = detected === T ? String : detected   # a conflicting value must move the type
            T = lock(promolock) do
                promo[j] = _promotemapped(tm, promo[j], promoT)
            end
        end
    end
    segments[k] = segs
    segtypes[k] = st
    mergeproblems!(pendingproblems, log, k)
    return
end

# Re-parse one (chunk, column) segment under the final joined type. A top-level
# function on purpose: a closure that assigns a local of the enclosing function
# rebinds a boxed variable shared across every concurrent task. Task bodies are
# named functions.
function restale!(chunks, final, segments, segtypes,
                  pendingproblems::PendingProblemLog, buf::Vector{UInt8},
                  opts::ValueOpts, d::Dialect, userprovided, k::Int, j::Int,
                  mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                  reportlimit::Int=typemax(Int), colopts=nothing)
    ci = chunks[k]
    stg = allocatecolumn(final[j], nrows(ci), buf, opts.e, d.cq)
    log = ProblemLog(pendingproblems.limit)
    conflict = final[j] === Missing ? 0 :
        parsecolchunk!(stg, buf, ci, j, 0, _copts(colopts, opts, j), userprovided[j],
                       log, 0, mask, maskbase, reportlimit)
    conflict == 0 || error("internal error: re-parse under the joined type conflicted")
    segments[k][j] = stg
    segtypes[k][j] = final[j]
    mergeproblems!(pendingproblems, log, k)
    return
end

# --- the direct wave ---------------------------------------------------------
#
# The unmasked driver: every chunk writes its parsed values straight into
# exact-size final columns at its global row base (the parse loops take an
# offset `rowbase`). There is no per-(column × chunk) staging and no copy pass;
# on the rare promotion, completed chunks re-parse the column into the new
# final. Promotions are what stratified sampling exists to make rare.
# (Dictionary encoding is the API layer's job: it pools a finished DataString
# column in one pass when asked.)

# Direct finals allocate UNDEF: each chunk task fills its own slice right
# before parsing it (one page touch, in the task that writes it, parallel at
# chunk granularity instead of column granularity). The rewave fills the
# slices of promoted finals — including fill-only for chunks whose Missing
# parse upgrades for free.
function _allocdirect(::Type{T}, ndata::Int, buf::Vector{UInt8}, opts::ValueOpts,
                      d::Dialect, j::Int, wantunion::Bool=false) where {T}
    T === Missing && return nothing
    T === String && return StringColumn(Vector{DataStringPayload}(undef, ndata), opts.e, d.cq)
    wantunion && return UnionColumn{T}(ndata)
    return TypedColumn{T}(Vector{T}(undef, ndata), Vector{Bool}(undef, ndata))
end

# indexed @simd loops, not fill!(view(...)): the SubArray fill does not lower
# to a memset-class loop, and the missing-dense shapes (most rows per byte)
# do the most fill work per input byte
function _fillslice!(col::StringColumn, lo::Int, hi::Int)
    payloads = col.payloads
    @inbounds @simd for r in lo:hi
        payloads[r] = PAYLOAD_MISSING
    end
    return nothing
end

function _fillslice!(col::TypedColumn, lo::Int, hi::Int)
    present = col.present
    @inbounds @simd for r in lo:hi
        present[r] = false
    end
    return nothing
end

function _fillslice!(col::UnionColumn, lo::Int, hi::Int)
    uvalues = col.uvalues
    @inbounds for r in lo:hi
        uvalues[r] = missing
    end
    return nothing
end

# Microseconds extend the date range but cannot hold every nanosecond value.
# Before freezing the joined type, validate only the chunks that succeeded as
# nanoseconds and now need microseconds. Other promotions accept prior values.
# This cold pass reads the source spans again because the chunk's earlier
# destination has been replaced. Excluded rows must not affect the final type.
function _settletimestampwidening!(types, segtypes, chunks, buf, opts, js, rl,
                                   colopts, mask=nothing, rowbases=nothing)
    for j in js
        types[j] === _TS_US || continue
        vo = _copts(colopts, opts, j)
        for k in eachindex(chunks)
            segtypes[k][j] === _TS_NS || continue
            ci = chunks[k]
            exact = true
            @inbounds for lr in ci.firstdatarow:totalrows(ci)
                localrow = lr - ci.firstdatarow + 1
                localrow > rl(k) && break
                mask !== nothing && !mask[rowbases[k] + localrow] && continue
                sp = fieldspan(ci, lr, j)
                sp === nothing && continue
                pos, len = sp
                len == 0 && continue
                cpos, clen, esc, st = cellcontent(buf, pos, len, vo)
                st == CELL_MISSING && continue
                if !_parsecontent(_TS_US, buf, cpos, clen, esc, vo)[2]
                    exact = false
                    break
                end
            end
            if !exact
                types[j] = String
                break
            end
        end
    end
    return nothing
end

function directwave!(cols, chunks, buf::Vector{UInt8}, d::Dialect, opts::ValueOpts,
                     ncols::Int, userprovided, promo, promolock,
                     pendingproblems::PendingProblemLog, segments, segtypes,
                     selected::Union{Nothing, Vector{Bool}},
                     rowbases::Vector{Int}, ndata::Int, rl,
                     reportstructural::Bool, parallel::Bool,
                     tasklimit::Int,
                     unioncols::Vector{Bool}=fill(false, ncols), tm=nothing, colopts=nothing)
    nch = length(chunks)
    finals = Vector{Any}(nothing, ncols)
    allocjs = [j for j in 1:ncols if selected === nothing || selected[j]]
    # allocate per column in parallel: a Vector{Union{T,Missing}} final zero-
    # initializes its selector bytes at allocation, which is a serial memset
    # per union column if done on one task
    if tasklimit > 1 && length(allocjs) > 1 && ndata > (1 << 16)
        _taskforeach(allocjs, tasklimit) do j
            finals[j] = _allocdirect(promo[j], ndata, buf, opts, d, j,
                                     unioncols[j])
        end
    else
        for j in allocjs
            finals[j] = _allocdirect(promo[j], ndata, buf, opts, d, j, unioncols[j])
        end
    end
    if tasklimit > 1 && nch > 1
        _taskforeach(1:nch, tasklimit) do k
            directchunk!(chunks[k], buf, d, opts, ncols, userprovided, promo,
                         promolock, finals, pendingproblems, segments, segtypes, k,
                         selected, rowbases[k], rl(k), ndata, reportstructural,
                         unioncols, tm, colopts)
        end
    else
        for k in 1:nch
            directchunk!(chunks[k], buf, d, opts, ncols, userprovided, promo, promolock,
                         finals, pendingproblems, segments, segtypes, k, selected,
                         rowbases[k], rl(k), ndata, reportstructural, unioncols,
                         tm, colopts)
        end
    end

    _settletimestampwidening!(promo, segtypes, chunks, buf, opts, allocjs, rl, colopts)
    for j in allocjs
        if promo[j] === String && !(finals[j] isa StringColumn)
            finals[j] = _allocdirect(String, ndata, buf, opts, d, j, unioncols[j])
        end
    end

    # adopt the chunks' private string bytes into each final column: chunk k's
    # segment wrote its long cells with payload buffer index 1 + k, so the
    # fold stores each segment's bytes at that slot (no copy, no repoint pass;
    # a chunk without long cells leaves an empty placeholder)
    final = Type[promo[j] for j in 1:ncols]
    for j in allocjs
        final[j] === String || continue
        scol = finals[j]
        scol isa StringColumn || continue
        adopted = scol.adopted
        resize!(adopted, nch)
        fill!(adopted, EMPTY_BYTES)
        for k in 1:nch
            seg = segments[k][j]
            seg isa StringColumn || continue
            adopted[k] = seg.extra
            segments[k][j] = nothing
        end
    end

    # promo is frozen: chunks that wrote under a stale type re-parse against the
    # final column. A Missing-parsed chunk upgrades for free (its rows are
    # already absent in the final); a stale chunk under a pooled final restales
    # into pooled staging for the merge.
    stale = Tuple{Int, Int}[]
    for k in 1:nch, j in allocjs
        T = segtypes[k][j]
        T === final[j] && continue
        if T === Missing
            # the free Missing upgrade still needs the promoted final's UNDEF
            # slice filled with the missing pattern
            final[j] === Missing && continue
        end
        push!(stale, (k, j))
    end
    if !isempty(stale)
        redo = (k, j) -> begin
            if segtypes[k][j] === Missing
                lo = rowbases[k] + 1
                hi = rowbases[k] + min(nrows(chunks[k]), rl(k))
                hi >= lo && _fillslice!(finals[j], lo, hi)
            else
                redirect!(chunks, final, finals, segtypes, pendingproblems, buf,
                          opts, userprovided, k, j, rowbases[k], rl(k), colopts)
            end
        end
        if tasklimit > 1 && length(stale) > 1
            _taskforeach(stale, tasklimit) do x
                redo(x[1], x[2])
            end
        else
            for (k, j) in stale
                redo(k, j)
            end
        end
    end

    # finalize the direct columns in place; the presence scans are per-column
    # independent — spread them
    finjs = allocjs
    parallelfinalize = tasklimit > 1 && length(finjs) > 1 && ndata > (1 << 18)
    columnbudget = parallelfinalize ? 1 : tasklimit
    finalizeone = j -> begin
        T = final[j]
        cols[j] = T === Missing ? fill(missing, ndata) :
                  T === String ? finalizecolumn(String, finals[j]::StringColumn, ndata) :
                  finalizecolumn(T, finals[j]::Union{TypedColumn{T}, UnionColumn{T}}, ndata;
                                 tasklimit=columnbudget)
    end
    if parallelfinalize
        _taskforeach(finalizeone, finjs, tasklimit)
    else
        foreach(finalizeone, finjs)
    end
    return final
end

function directchunk!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, opts::ValueOpts,
                      ncols::Int, userprovided, promo, promolock, finals,
                      pendingproblems::PendingProblemLog, segments, segtypes, k::Int,
                      selected::Union{Nothing, Vector{Bool}}, rowbase::Int,
                      reportlimit::Int, ndata::Int, reportstructural::Bool,
                      unioncols::Vector{Bool}=fill(false, ncols), tm=nothing, colopts=nothing)
    n = nrows(ci)
    log = ProblemLog(pendingproblems.limit)
    if reportstructural
        for lr in ci.firstdatarow:totalrows(ci)
            localrow = lr - ci.firstdatarow + 1
            localrow > reportlimit && continue
            nf = nfields(ci, lr)
            if nf != ncols
                sp = fieldspan(ci, lr, nf < ncols ? 1 : ncols + 1)::Tuple{Int, Int}
                # a kept empty row (ignoreemptyrows=false) is all-missing by
                # request, not a short row to report
                _emptyrow(buf, ci, nf, sp) || pushrowproblem!(log, localrow, sp[1], ncols, nf)
            end
        end
    end
    segs = Vector{Any}(undef, ncols)
    st = Vector{Type}(undef, ncols)
    for j in 1:ncols
        if selected !== nothing && !selected[j]
            segs[j] = nothing
            st[j] = Missing
            continue
        end
        T, dest = lock(() -> (promo[j], finals[j]), promolock)
        attempts = 0
        lo = rowbase + 1
        hi = rowbase + min(n, reportlimit)
        while true
            (attempts += 1) > 8 && error("internal error: promotion did not converge")
            local conflict::Int
            if T === Missing
                segs[j] = nothing
                conflict = parsecolchunk_missing(buf, ci, j, 0,
                                                 _copts(colopts, opts, j),
                                                 userprovided[j], log, nothing, 0,
                                                 reportlimit)
            elseif T === String
                # shared payloads, PRIVATE bytes: each chunk copies its long
                # cells into its own buffer, and the driver adopts those buffers
                # in chunk order after the wave
                scol = dest::StringColumn
                hi >= lo && _fillslice!(scol, lo, hi)
                chunkcol = StringColumn(scol.payloads, scol.e, scol.cq, 1 + k)
                conflict = parsecolchunk!(chunkcol, buf, ci, j, rowbase,
                                          _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
                segs[j] = isempty(chunkcol.extra) ? nothing : chunkcol
            else
                segs[j] = nothing
                hi >= lo && _fillslice!(dest, lo, hi)
                conflict = parsecolchunk!(dest, buf, ci, j, rowbase, _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
            end
            if conflict == 0
                st[j] = T
                break
            end
            sp = fieldspan(ci, conflict, j)::Tuple{Int, Int}
            detected = promote_kernel(T, detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j)))
            # single assignment: promoT is captured by the lock closure below,
            # and a captured-and-reassigned local boxes
            promoT = detected === T ? String : detected
            T, dest = lock(promolock) do
                joined = _promotemapped(tm, promo[j], promoT)
                if joined !== promo[j]
                    promo[j] = joined
                    finals[j] = _allocdirect(joined, ndata, buf, opts, d, j,
                                             unioncols[j])
                end
                (promo[j], finals[j])
            end
        end
    end
    segments[k] = segs
    segtypes[k] = st
    mergeproblems!(pendingproblems, log, k)
    return
end

# re-parse one stale (chunk, column) straight into the final column
function redirect!(chunks, final, finals, segtypes,
                   pendingproblems::PendingProblemLog, buf::Vector{UInt8},
                   opts::ValueOpts, userprovided, k::Int, j::Int,
                   rowbase::Int, reportlimit::Int, colopts=nothing)
    ci = chunks[k]
    log = ProblemLog(pendingproblems.limit)
    hi = rowbase + min(nrows(ci), reportlimit)
    dest = finals[j]
    hi > rowbase && _fillslice!(dest, rowbase + 1, hi)
    if dest isa StringColumn
        # several stale chunks re-parse at once: each owns its bytes privately
        # under its chunk slot, sized by the direct wave's fold
        chunkcol = StringColumn(dest.payloads, dest.e, dest.cq, 1 + k)
        conflict = parsecolchunk!(chunkcol, buf, ci, j, rowbase, _copts(colopts, opts, j),
                                  userprovided[j], log, 0, nothing, 0, reportlimit)
        lock(dest.lock) do
            length(dest.adopted) >= k ||
                error("internal error: string final has no buffer slot for chunk $k")
            dest.adopted[k] = chunkcol.extra
        end
    else
        conflict = parsecolchunk!(dest, buf, ci, j, rowbase, _copts(colopts, opts, j),
                                  userprovided[j], log, 0, nothing, 0, reportlimit)
    end
    conflict == 0 || error("internal error: re-parse under the joined type conflicted")
    segtypes[k][j] = final[j]
    mergeproblems!(pendingproblems, log, k)
    return
end

# --- pooled (dictionary-encoded) string columns --------------------------------
#
# Pooling follows parsing. The API layer interns string ranges and merges levels
# in source order. This container stores the merged references and levels until
# the API layer converts them to PooledArrays.
struct PooledColumn{ELT} <: AbstractVector{ELT}
    refs::Vector{UInt32}          # 0 = missing (ELT includes Missing then)
    levels::DataStringVector{DataString}
end

Base.size(c::PooledColumn) = size(c.refs)

# widen a missing-free column to its Union{Missing,T} counterpart, zero-copy
# where the container supports it (DataString views / pooled refs), else a
# converted Base vector
_widenmissing(c::DataStringVector{DataString}) =
    DataStringVector{Union{DataString, Missing}}(c.payloads, c.buffers, Val(:trusted))
_widenmissing(c::PooledColumn{DataString}) =
    PooledColumn{Union{DataString, Missing}}(c.refs, c.levels)
_widenmissing(c::Vector{T}) where {T} = convert(Vector{Union{T, Missing}}, c)
_widenmissing(c::AbstractVector) = c

Base.@propagate_inbounds function Base.getindex(c::PooledColumn{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(c.refs, i)
    @inbounds r = c.refs[i]
    r == 0 && return missing
    return c.levels[Int(r)]
end

Base.@propagate_inbounds function Base.getindex(c::PooledColumn{DataString}, i::Int)
    @boundscheck checkbounds(c.refs, i)
    @inbounds return c.levels[Int(c.refs[i])]
end

poolrefs(c::PooledColumn) = c.refs

function materialize(c::PooledColumn{ELT}) where {ELT}
    lv = materialize(c.levels)
    out = Vector{ELT === DataString ? String : Union{String, Missing}}(undef, length(c.refs))
    @inbounds for i in eachindex(c.refs)
        r = c.refs[i]
        out[i] = r == 0 ? missing : lv[Int(r)]
    end
    return out
end

# Assemble one final exact-size column from its per-chunk segments. Segment
# copies are plain value memmoves (cheap relative to re-reading text from RAM);
# a Missing segment under a wider final type contributes all-absent rows with no
# re-parse. String segments adopt their owned buffers and update payload buffer
# indices without copying the string bytes.
function stitchcolumn(::Type{T}, segments, segtypes, j::Int, chunkrows, rowbases,
                      ndata::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                      mask::Union{Nothing, Vector{Bool}}=nothing, inbases=nothing;
                      tasklimit::Int=1) where {T}
    T === Missing && return fill(missing, ndata)
    mask === nothing || return _stitchmasked(T, segments, j, chunkrows, ndata, buf, e, cq,
                                             mask, inbases; tasklimit)
    # Single-chunk files (every input below chunkbytes): the lone segment IS the
    # final column — finalize it directly, zero copies, the same cost as
    # writing final columns in place.
    if length(chunkrows) == 1
        seg = segments[1][j]
        seg === nothing && return fill(missing, ndata)
        # a limit-clipped boundary segment is larger than the output; only the
        # untouched case may alias the staging directly
        if (seg isa StringColumn ? length(seg.payloads) : length((seg::TypedColumn{T}).values)) == ndata
            return T === String ? finalizecolumn(String, seg::StringColumn, ndata) :
                                  finalizecolumn(T, seg::TypedColumn{T}, ndata; tasklimit)
        end
    end
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        outcol = StringColumn(payloads, e, cq)
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            seg === nothing && continue          # all-missing segment
            scol = seg::StringColumn
            rb = rowbases[k]
            copyto!(payloads, rb + 1, scol.payloads, 1, chunkrows[k])
            _adopt!(outcol, scol, (rb + 1):(rb + chunkrows[k]))
        end
        return finalizecolumn(String, outcol, ndata)
    end
    values = Vector{T}(undef, ndata)
    present = fill(false, ndata)
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        seg === nothing && continue              # all-missing segment: stays absent
        tcol = seg::TypedColumn{T}
        rb = rowbases[k]
        copyto!(values, rb + 1, tcol.values, 1, chunkrows[k])
        copyto!(present, rb + 1, tcol.present, 1, chunkrows[k])
    end
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata; tasklimit)
end

# Row-filtered stitch: gather only mask-qualifying rows into compact output
# positions (chunk order, so output order is input order). Cells for excluded
# rows were never parsed; their staging slots are simply skipped here.
function _stitchmasked(::Type{T}, segments, j::Int, chunkrows, ndata::Int,
                       buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                       mask::Vector{Bool}, inbases; tasklimit::Int=1) where {T}
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        outcol = StringColumn(payloads, e, cq)
        dest = 0
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            if seg === nothing
                @inbounds for i in 1:chunkrows[k]
                    mask[inbases[k] + i] && (dest += 1)
                end
                continue
            end
            scol = seg::StringColumn
            newidx = isempty(scol.extra) ? 0 : _adoptbuffer!(outcol, scol.extra)
            @inbounds for i in 1:chunkrows[k]
                mask[inbases[k] + i] || continue
                dest += 1
                p = scol.payloads[i]
                if newidx != 0 && payloadlen(p) > INLINE_MAX && payloadbufidx(p) == 1
                    p = repoint_payload(p, newidx, payloadoffset(p))
                end
                payloads[dest] = p
            end
        end
        return finalizecolumn(String, outcol, ndata)
    end
    values = Vector{T}(undef, ndata)
    present = fill(false, ndata)
    dest = 0
    for k in eachindex(chunkrows)
        seg = segments[k][j]
        if seg === nothing
            @inbounds for i in 1:chunkrows[k]
                mask[inbases[k] + i] && (dest += 1)
            end
            continue
        end
        tcol = seg::TypedColumn{T}
        @inbounds for i in 1:chunkrows[k]
            mask[inbases[k] + i] || continue
            dest += 1
            hasvalue = tcol.present[i]
            hasvalue && (values[dest] = tcol.values[i])
            present[dest] = hasvalue
        end
    end
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata; tasklimit)
end

function finalizecolumn(::Type{Missing}, ::Nothing, n::Int, force_missing::Bool=false;
                         tasklimit::Int=1)
    return fill(missing, n)
end

function finalizecolumn(::Type{String}, col::StringColumn, n::Int, force_missing::Bool=false;
                         tasklimit::Int=1)
    anymissing = force_missing || any(p -> payloadlen(p) < 0, col.payloads)
    return anymissing ? _stringvector(Union{DataString, Missing}, col.payloads, _buffers(col)) :
                        _stringvector(DataString, col.payloads, _buffers(col))
end
# `all(::Vector{Bool})` short-circuits, so it compiles to a branchy scalar
# loop; `count` vectorizes, and missing-free columns (the common case)
# full-scan either way.
_allpresent(present::Vector{Bool}) = count(present) == length(present)
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int, force_missing::Bool=false;
                         tasklimit::Int=1) where {T}
    # no missings ⇒ hand back the raw Vector{T}, zero copies
    return !force_missing && _allpresent(col.present) ? col.values : _tounion(col, tasklimit)
end
# union-direct finals ARE the output — zero copies either way
finalizecolumn(::Type{T}, col::UnionColumn{T}, n::Int, force_missing::Bool=false;
               tasklimit::Int=1) where {T} = col.uvalues

# The sample-missed fallback: sparse missings the type sample did not see.
# Bitsunion stores have no memcpy path and cost about as much as the parse, so
# slice the conversion across tasks. A named helper, not a closure, so no
# captured local is reassigned.
function _tounionrange!(out, values, present, lo::Int, hi::Int)
    @inbounds for i in lo:hi
        out[i] = present[i] ? values[i] : missing
    end
    return
end

function _tounion(col::TypedColumn{T}, tasklimit::Int=1) where {T}
    values, present = col.values, col.present
    n = length(values)
    out = Vector{Union{T, Missing}}(undef, n)
    nt = tasklimit
    if n > (1 << 17) && nt > 1
        parts = min(nt, 8)
        @sync for c in 1:parts
            lo = 1 + (c - 1) * n ÷ parts
            hi = c * n ÷ parts
            @wkspawn _tounionrange!(out, values, present, lo, hi)
        end
    else
        _tounionrange!(out, values, present, 1, n)
    end
    return out
end

"""
    materialize(col) -> Vector

Convert a parsed column into an ordinary `Vector` (`Vector{T}` or
`Vector{Union{T,Missing}}`). Text columns allocate one `String` per value. A
column that is already a `Vector` is returned as is.
"""
materialize(v::AbstractVector) = collect(v)
materialize(v::Vector) = v

# A duplicate takes the smallest `name_k` not used by ANY
# name — original or already assigned — so `a,a,a_1` becomes `a,a_2,a_1`
# (renames never collide with an original that appears later).
function makeunique!(names::Vector{Symbol})
    taken = Set(names)
    seen = Set{Symbol}()
    for i in eachindex(names)
        nm = names[i]
        if nm in seen
            k = 1
            newnm = Symbol(nm, :_, k)
            while newnm in taken
                k += 1
                newnm = Symbol(nm, :_, k)
            end
            push!(taken, newnm)
            names[i] = newnm
            push!(seen, newnm)
        else
            push!(seen, nm)
        end
    end
    return names
end
