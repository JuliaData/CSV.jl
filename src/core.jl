#=
    CSV parser core

The internal CSV parsing engine. It first finds all rows and fields. It then
parses field values and builds columns.

The pipeline (and the file's layout) is:

    L0  bytes         : The input is one `Vector{UInt8}`. Other code reads,
                        maps, or decompresses the source before this step.
    L1  rows and      : A quote-aware scan finds delimiters and row endings. It
        fields          stores their byte positions in one `ChunkIndex` for each
                        chunk. A scalar scanner supports all CSV options. Two
                        fast scanners process 64 bytes at a time.
    L1' chunks        : For standard CSV quote rules, the parser first divides
                        the input into fixed byte ranges. It counts the quote
                        bytes in each range. These counts show whether each
                        range starts inside or outside a quoted field. The
                        parser then moves each range start to the next complete
                        row boundary. It can index the resulting chunks at the
                        same time.
    L2  types         : The parser reads rows from across the input. It uses
                        these rows to choose an initial type for each column.
    L3  values        : The parser reads each column from the stored field
                        positions. If a later value needs a different type, it
                        changes that column type. It reads only the affected
                        parts of that column again.
    L4  columns       : Each non-string column stores its values and a separate
                        present-or-missing flag. String values refer to the
                        input bytes and remove escapes when a caller reads them.
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
again. Tests preserve this behavior.
=#

using Dates
import Parsers

# Parsers owns scalar value conversion. CSV owns rows, fields, quotes, missing
# values, and column assembly.
const _ISO_DATE_PATTERN = Parsers.compilepattern("yyyy-mm-dd")
const _ISO_DATETIME_PATTERN = Parsers.compilepattern("yyyy-mm-ddTHH:MM:SS.s")
const _ISO_TIME_PATTERN = Parsers.compilepattern("HH:MM:SS.s")

# ---------------------------------------------------------------------------
# Dialect: the structural options. Value-level options (sentinels, dateformats,
# true/false spellings, decimal char) live in `ValueOpts`, built once in
# `makevalueopts` and applied to exact field spans by the Parsers kernels.
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
                   ignorerepeated::Bool=false)
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
    return Dialect(d, oq, cq, e, quoted, cmt, ignoreemptyrows, ignorerepeated)
end

# The range planner can use quote counts with standard CSV quote rules. The same
# byte must open and close a quoted field. An escaped quote must use two quote
# bytes. Each quote changes the state between inside and outside a quoted field.
# Two quotes change the state twice, so the final state is the same. If quote
# handling is off, every range starts outside a quoted field.
#
# A different escape byte or different open and close bytes need more context.
# The parser uses one scalar scan for those options.
parityclean(d::Dialect) = !d.quoted || (d.oq == d.cq && d.e == d.cq)
# Quote bytes in a comment row do not change the CSV quote state. A byte range
# that starts in the middle of a row cannot know whether that row is a comment.
# The planner therefore finds row starts in order for files with comment rows.
# It can still index the completed chunks at the same time.
commentaware(d::Dialect) = d.comment !== nothing

# The fast scanners additionally need a single-byte delimiter.
swareligible(d::Dialect) = parityclean(d) && d.delim isa UInt8 && !commentaware(d)

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
    trues::Vector{Vector{UInt8}}
    falses::Vector{Vector{UInt8}}
    datepat::Parsers.DatePattern
    datetimepat::Parsers.DatePattern
    timepat::Parsers.DatePattern
    customfmt::Bool
    inferbool::Bool   # false when another type also accepts a user Bool spelling
    groupmark::UInt8  # digit-group separator for numeric cells; 0x00 = off
end

# Parsers returns signed zero or signed infinity with a range code. CSV accepts
# these rounded values.
@inline _fixedfloatusable(rc) =
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
                          customfmt::Bool, gm::UInt8)
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
        if Parsers.parsecivil(s, i, j, dp)[2] == Parsers.RC_OK
            return dp.hasdate ? (dp.hastime ? DateTime : Date) : Time
        end
    else
        Parsers.parsecivil(s, i, j, dp)[2] == Parsers.RC_OK && return Date
        Parsers.parsecivil(s, i, j, dtp)[2] == Parsers.RC_OK && return DateTime
        Parsers.parsecivil(s, i, j, tp)[2] == Parsers.RC_OK && return Time
    end
    return nothing
end

# Another type can also accept a user Bool spelling. For example, Int64 accepts
# `"1"`. In this case, CSV does not infer Bool for the column. A user can still
# set the column type to Bool and use the custom spelling. This rule makes the
# result independent of the sampled rows.
function _validatebools(trues, falses, decimal, dp, dtp, tp, customfmt, gm)
    for t in trues, f in falses
        t == f && throw(ArgumentError("Bool spelling $(repr(String(t))) is both true and false"))
    end
    for s in Iterators.flatten((trues, falses))
        _earlierbooltype(s, decimal, dp, dtp, tp, customfmt, gm) === nothing || return false
    end
    return true
end

function makevalueopts(d::Dialect; dateformat=nothing, decimal::Char='.',
                       truestrings=nothing, falsestrings=nothing,
                       stripwhitespace::Bool=false,
                       groupmark::Union{Nothing, Char}=nothing,
                       sentinels=nothing)
    isascii(decimal) || throw(ArgumentError("decimal must be ASCII (got $(repr(decimal)))"))
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
    if dateformat === nothing
        dp, dtp, tp, custom =
            _ISO_DATE_PATTERN, _ISO_DATETIME_PATTERN, _ISO_TIME_PATTERN, false
    else
        dateformat isa AbstractString ||
            throw(ArgumentError("dateformat must be a format String (got $(typeof(dateformat)))"))
        p = Parsers.compilepattern(dateformat)
        (p.hasdate || p.hastime) ||
            throw(ArgumentError("dateformat must contain a date or time token"))
        dp = dtp = tp = p
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
    inferbool = _validatebools(trues, falses, decimal % UInt8, dp, dtp, tp, custom, gm)
    sf = (zero(UInt64), zero(UInt64), zero(UInt64), zero(UInt64))
    for s in sentinelbytes
        b = s[1]
        sf = Base.setindex(sf, sf[(b >> 6) + 1] | (UInt64(1) << (b & 0x3f)), (b >> 6) + 1)
    end
    return ValueOpts(d.oq, d.cq, d.e, d.quoted, delimbytes, decimal % UInt8, stripwhitespace,
                     sentinelbytes, sf, trues, falses,
                     dp, dtp, tp, custom, inferbool, gm)
end

# --- the cell layer -----------------------------------------------------------
#
# One function turns a raw field span into a *content* span + disposition:
#     CELL_VALUE    content [cpos, cpos+clen) is a present value (maybe escaped)
#     CELL_MISSING  empty / whitespace-stripped-to-empty / sentinel ⇒ missing
#     CELL_BADQUOTE malformed quoting (unterminated, or bytes after the close)
# Rules: outer space/tab around a QUOTED field is structural, never content
# (matching every CSV reader surveyed); unquoted whitespace is significant
# unless `stripwhitespace`; a quoted empty field is a present empty string,
# never missing; sentinels match the (possibly unquoted) content exactly.
const CELL_VALUE    = 0x00
const CELL_MISSING  = 0x01
const CELL_BADQUOTE = 0x02

@inline _isot(b::UInt8) = (b == UInt8(' ')) | (b == UInt8('\t'))

# a cell can only be a sentinel if its first byte starts one — one bit test
# replaces the per-cell spelling comparisons (empty sentinel list ⇒ zero map)
@inline _maybesentinel(vo::ValueOpts, b::UInt8) =
    (vo.sentfirst[(b >> 6) + 1] >> (b & 0x3f)) & UInt64(1) != 0

# Typed values and sentinel matching accept surrounding blanks. String columns
# keep these bytes. This helper changes only the span used for value parsing and
# sentinel checks.
@inline function _trimblanks(buf::Vector{UInt8}, i::Int, j::Int)
    @inbounds while i <= j && _isot(buf[i]); i += 1; end
    @inbounds while j >= i && _isot(buf[j]); j -= 1; end
    return i, j
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
                       the bytes are the value — typed kernels reject such
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
                if clen > 0 && !esc
                    _matchsentinel(buf, cpos, cpos + clen - 1, vo) &&
                        return (cpos, 0, false, CELL_MISSING)
                end
                return (cpos, clen, esc, CELL_VALUE)
            end
        end
        _matchsentinel(buf, i, j, vo) &&
            return (i, 0, false, CELL_MISSING)
        return (i, j - i + 1, false, CELL_VALUE)
    end
end

# An UNQUOTED span can only contain the delimiter when a bare mid-field quote
# engaged the indexer's structural protection — the value-level reading of the
# bytes disagrees with the structural one. String cells and headers surface
# that as a problem (typed kernels reject such spans naturally); the bytes are
# still preserved exactly where the caller keeps them.
function _delimclash(buf::Vector{UInt8}, cpos::Int, clen::Int, delim::Vector{UInt8})
    n = length(delim)
    clen < n && return false
    # this scan runs on EVERY unquoted string cell (protection detection, not
    # the exception path) — single-byte delimiters take the SWAR word walk
    if n == 1
        d = @inbounds delim[1]
        k = cpos
        last = cpos + clen - 1
        GC.@preserve buf begin
            p = pointer(buf)
            @inbounds while k + 7 <= last
                w = ltoh(unsafe_load(Ptr{UInt64}(p + k - 1)))
                movemask(eqmarks(w, d)) != 0 && return true
                k += 8
            end
        end
        @inbounds while k <= last
            buf[k] == d && return true
            k += 1
        end
        return false
    end
    @inbounds for k in cpos:(cpos + clen - n)
        if buf[k] == delim[1]
            m = 2
            while m <= n && buf[k + m - 1] == delim[m]
                m += 1
            end
            m > n && return true
        end
    end
    return false
end

# Was this raw span a quoted field? (Re-derives cellcontent's entry condition;
# only called on cold/string paths.)
@inline function _wasquoted(buf::Vector{UInt8}, pos::Int, len::Int, vo::ValueOpts)
    vo.quoted || return false
    i, j = pos, pos + len - 1
    @inbounds while i <= j && _isot(buf[i])
        i += 1
    end
    return i <= j && buf[i] == vo.oq
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

# Parsers returns calendar fields without choosing a Dates representation. CSV
# owns this conversion because it chooses the final column type.
@inline todate(c::Parsers.CivilParts) = Date(c.year, c.month, c.day)

@inline function todatetime(c::Parsers.CivilParts)
    milliseconds = Int64(c.nanosecond) ÷ 1_000_000
    return DateTime(c.year, c.month, c.day, c.hour, c.minute, c.second, milliseconds)
end

@inline totime(c::Parsers.CivilParts) =
    Time(Dates.Nanosecond(((Int64(c.hour) * 60 + c.minute) * 60 + c.second) *
                          1_000_000_000 + c.nanosecond))

# Numeric kernels take a scratch buffer so grouped digits (groupmark) degroup
# without per-cell allocation; the hot loops pass a per-(column × chunk)
# scratch, and the 5-arg convenience forms below allocate one lazily. With
# groupmark off, the extra argument is dead and the kernels run untouched.
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
@inline parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts,
                   scratch::Vector{UInt8}) where {T} = parsevalue(T, buf, i, j, vo)

# Narrow numeric requests use the native integer/float kernels, then convert at
# the API boundary. Keep that rule available to lazy and row readers too:
# calling `tryparse(Int8, String(...))` here would lose decimal/groupmark
# handling and would allocate one String per cell.
const NarrowParseType = Union{Int8, Int16, Int32,
                              UInt8, UInt16, UInt32, UInt64,
                              Float16, Float32}
@inline _narrowbase(::Type{<:Union{Int8, Int16, Int32,
                                   UInt8, UInt16, UInt32}}) = Int64
@inline _narrowbase(::Type{UInt64}) = Int128
@inline _narrowbase(::Type{<:Union{Float16, Float32}}) = Float64
@inline function _narrowvalue(::Type{T}, value, ok::Bool) where {T <: NarrowParseType}
    ok || return (zero(T), false)
    T <: Integer && !(typemin(T) <= value <= typemax(T)) && return (zero(T), false)
    return (convert(T, value), true)
end
@inline function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                            vo::ValueOpts, scratch::Vector{UInt8}) where {T <: NarrowParseType}
    value, ok = parsevalue(_narrowbase(T), buf, i, j, vo, scratch)
    return _narrowvalue(T, value, ok)
end
# CSV uses these types only when the user requests them. Type inference does not
# select them.
@inline function _parsebigint_direct(buf::Vector{UInt8}, i::Int, j::Int)
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
@inline function _parsebigfloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
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
@inline function parsevalue(::Type{Base.UUID}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    u, rc = Parsers.parseuuid(buf, i, j)
    return (Base.UUID(u), rc == Parsers.RC_OK)
end
_scratchfor(vo::ValueOpts) = vo.groupmark == 0x00 ? EMPTY_BYTES : Vector{UInt8}(undef, 64)
@inline parsevalue(::Type{Int64}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int64, buf, i, j, vo, _scratchfor(vo))
@inline parsevalue(::Type{Int128}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    parsevalue(Int128, buf, i, j, vo, _scratchfor(vo))
@inline parsevalue(::Type{BigInt}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts) =
    vo.groupmark == 0x00 ? _parsebigint_direct(buf, i, j) :
                           parsevalue(BigInt, buf, i, j, vo, Vector{UInt8}(undef, 64))
@inline function _parsefloat_direct(buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
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
@inline function parsevalue(::Type{Bool}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    if isempty(vo.trues) && isempty(vo.falses)
        v, rc = Parsers.parsebool(buf, i, j)
        return (v, rc == Parsers.RC_OK)
    end
    _spanmatches(buf, i, j, vo.trues) && return (true, true)
    _spanmatches(buf, i, j, vo.falses) && return (false, true)
    return (false, false)
end
@inline function parsevalue(::Type{Date}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (!vo.datepat.hasdate || vo.datepat.hastime) && return (_DATE0, false)
    c, rc = Parsers.parsecivil(buf, i, j, vo.datepat)
    rc == Parsers.RC_OK || return (_DATE0, false)
    return (todate(c), true)
end
@inline function parsevalue(::Type{DateTime}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (!vo.datetimepat.hasdate || !vo.datetimepat.hastime) && return (_DATETIME0, false)
    c, rc = Parsers.parsecivil(buf, i, j, vo.datetimepat)
    rc == Parsers.RC_OK || return (_DATETIME0, false)
    return (todatetime(c), true)
end
@inline function parsevalue(::Type{Time}, buf::Vector{UInt8}, i::Int, j::Int, vo::ValueOpts)
    vo.customfmt && (vo.timepat.hasdate || !vo.timepat.hastime) && return (_TIME0, false)
    c, rc = Parsers.parsecivil(buf, i, j, vo.timepat)
    rc == Parsers.RC_OK || return (_TIME0, false)
    return (totime(c), true)
end

# User-defined scalar types are not a common path. A concrete type is accepted
# when it defines `tryparse(T, ::String)` or
# `parse(T, ::String)`. Keep failures as ordinary invalid cells; a custom
# parser must not escape the parse loop and abort the whole file.
@noinline function parsevalue(::Type{T}, buf::Vector{UInt8}, i::Int, j::Int,
                              ::ValueOpts) where {T}
    s = String(buf[i:j])
    try
        if hasmethod(Base.tryparse, Tuple{Type{T}, String})
            v = tryparse(T, s)
            return (v, v isa T)
        end
        v = parse(T, s)
        return (v, v isa T)
    catch
        return (nothing, false)
    end
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
end

ChunkIndex(start::Int, stop::Int) =
    ChunkIndex(start, stop, UInt32[], UInt32[], Int32[1], UInt32[], 1, 1, false)

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
    col <= nextr - first || return nothing
    fi = first + col - 1
    @inbounds stop = ci.start + Int(ci.tape[fi] >> 2) - 1
    if col == 1
        @inbounds s = ci.start + Int(ci.rowstartrel[localrow])
    else
        @inbounds e = ci.tape[fi - 1]
        k = e & 0x03
        skip = Int(ci.delimskip)
        # ignorerepeated: the previous event closed a run of 1 + ext delimiters
        k == 0x00 && !isempty(ci.ext) && (skip += skip * Int(@inbounds ci.ext[fi - 1]))
        s = ci.start + Int(e >> 2) + (k == 0x00 ? skip : Int(k))
    end
    return (s, stop - s + 1)
end

struct BufferIndex
    chunks::Vector{ChunkIndex}
    nrows::Int                  # total rows across chunks (header still included at this layer)
    unclosedquote::Bool         # input ended inside a quoted field (captured before empty-chunk filtering)
end

# --- tape plumbing -----------------------------------------------------------

const MAX_TAPE_HINT = 1 << 20   # initial-capacity cap: a giant single row spans
                                # many bytes but holds few events
const MAX_TAPE_RELPOS = Int(typemax(UInt32) >> 2)

@inline function tape_room!(tape::Vector{UInt32}, n::Int, extra::Int)
    length(tape) < n + extra && resize!(tape, max(2 * length(tape), n + extra + 256))
    return tape
end

@inline function checktaperange(ci::ChunkIndex)
    ci.stop - ci.start < MAX_TAPE_RELPOS ||
        throw(ArgumentError("a single row is 1 GiB or larger and is not supported"))
    return ci
end

# raw event kinds during scanning
@inline rawkind(b::UInt8) = UInt32((b == CR) + 2 * (b == LF))   # 0 delim, 1 CR, 2 LF, 3 CRLF (pre-paired)

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
    resize!(rowfirst, 1); @inbounds rowfirst[1] = Int32(1)
    empty!(rowstartrel)
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
            end
            if drop
                w = roweventw - 1
            else
                push!(rowstartrel, UInt32(rowstart - ci.start))
                push!(rowfirst, Int32(w + 1))
                roweventw = w + 1
            end
            rowstart = nextrow
        end
    end
    resize!(tape, w)
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
# start at the first byte of the row. Tests check both rules.
function assemblecollapsed!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int)
    tape = ci.tape
    skip = ci.delimskip = d.delim isa UInt8 ? 1 : length(d.delim::Vector{UInt8})
    ext = ci.ext
    length(ext) < n && resize!(ext, n)
    rowfirst = ci.rowfirst
    rowstartrel = ci.rowstartrel
    resize!(rowfirst, 1); @inbounds rowfirst[1] = Int32(1)
    empty!(rowstartrel)
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
            end
            if drop
                w = roweventw - 1
            else
                push!(rowstartrel, UInt32(fieldstart - ci.start))
                push!(rowfirst, Int32(w + 1))
                roweventw = w + 1
            end
            rowstart = fieldstart = nextrow
        end
    end
    resize!(tape, w)
    resize!(ext, w)
    return ci
end

# End-of-chunk: synthesize a row end when the chunk does not finish on one — a
# trailing unterminated row ("a,b"), a trailing empty field ("a,b,"), or an
# unclosed quote running to EOF.
function finishscan!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, n::Int, inquote::Bool)
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
    end
    ci.unclosedquote = inquote
    assemblerows!(ci, buf, d, n)
    return ci
end

# --- scalar scanner ---------------------------------------------------------
#
# Read one byte at a time. This scanner supports multi-byte delimiters, a
# separate escape byte, and different open and close quote bytes. Tests compare
# the fast scanners with this scanner. Each chunk starts at a complete row, so
# this scan always starts outside a quoted field.

function indexchunk_scalar!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect)
    start, stop = ci.start, ci.stop
    oq, cq, e, quoted = d.oq, d.cq, d.e, d.quoted
    delim = d.delim
    tape = ci.tape
    n = 0
    pos = start
    inquote = false
    cmt = d.comment
    atrowstart = true      # comment rows are skipped whole: their bytes are not structural
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
            inquote = true                     # structural rule: any quote toggles
            pos += 1
        elseif delim isa UInt8 ? b == delim :
               (b == delim[1] && pos + length(delim) - 1 <= stop && _matchbytes(buf, pos, delim))
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = UInt32(pos - start) << 2         # kind 0
            pos += delim isa UInt8 ? 1 : length(delim)
        elseif b == LF || b == CR
            # CR immediately followed by LF emits ONE pre-paired event (kind 3):
            # half the row-end tape traffic, and assembly needs no pairing pass
            crlf = b == CR && pos < stop && buf[pos + 1] == LF
            tape_room!(tape, n, 1)
            n += 1
            tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
            pos += crlf ? 2 : 1
            atrowstart = true
        else
            pos += 1
        end
    end
    return finishscan!(ci, buf, d, n, inquote)
end

@inline function _matchbytes(buf::Vector{UInt8}, pos::Int, bytes::Vector{UInt8})
    @inbounds for k in eachindex(bytes)
        buf[pos + k - 1] == bytes[k] || return false
    end
    return true
end

# --- fast scanners -----------------------------------------------------------
#
# Both fast scanners read 64 bytes at a time. Each 64-bit mask uses one bit for
# each input byte. One mask marks quotes. A second mask marks delimiters,
# carriage returns, and line feeds. The quote marks show which bytes are inside
# quoted fields. Only delimiters and line endings outside quoted fields become
# index events.
#
# `:swar` processes the block as eight 64-bit words. It does not require vector
# instructions. `:vec` lets LLVM select vector instructions for the current CPU.
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
@inline function eqmarks(w::UInt64, b::UInt8)::UInt64
    x = w ⊻ (ONES8 * b)
    return ~(((x & LOWS7) + LOWS7) | x | LOWS7)
end

@inline movemask(marks::UInt64)::UInt64 = ((marks >> 7) * MOVEMASK_MAGIC) >> 56

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
    @inline function prefix_xor64(m::UInt64)::UInt64
        # This CPU instruction calculates the running quote mask in one step.
        v = Base.llvmcall(("""
            declare <2 x i64> @llvm.x86.pclmulqdq(<2 x i64>, <2 x i64>, i8)
            define i64 @entry(i64 %m) #0 {
                %a0 = insertelement <2 x i64> zeroinitializer, i64 %m, i32 0
                %b0 = insertelement <2 x i64> zeroinitializer, i64 -1, i32 0
                %r = call <2 x i64> @llvm.x86.pclmulqdq(<2 x i64> %a0, <2 x i64> %b0, i8 0)
                %lo = extractelement <2 x i64> %r, i32 0
                ret i64 %lo
            }
            attributes #0 = { alwaysinline }""", "entry"), UInt64, Tuple{UInt64}, m)
        return v
    end
elseif Sys.ARCH === :aarch64 && Sys.isapple()
    @inline function prefix_xor64(m::UInt64)::UInt64
        # This Apple ARM instruction calculates the running quote mask in one step.
        v = Base.llvmcall(("""
            declare <16 x i8> @llvm.aarch64.neon.pmull64(i64, i64)
            define i64 @entry(i64 %m) #0 {
                %r = call <16 x i8> @llvm.aarch64.neon.pmull64(i64 %m, i64 -1)
                %v = bitcast <16 x i8> %r to <2 x i64>
                %lo = extractelement <2 x i64> %v, i32 0
                ret i64 %lo
            }
            attributes #0 = { alwaysinline }""", "entry"), UInt64, Tuple{UInt64}, m)
        return v
    end
else
    @inline prefix_xor64(m::UInt64) = prefix_xor64_shift(m)
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

@inline function specials_mask_vec(p::Ptr{UInt8}, d::UInt8)::UInt64
    Base.llvmcall((SPECIALS_MASK_VEC_IR, "entry"),
        UInt64, Tuple{Ptr{UInt8}, UInt8, UInt8, UInt8}, p, d, CR, LF)
end

@inline function byte_mask_vec(p::Ptr{UInt8}, b::UInt8)::UInt64
    Base.llvmcall((BYTE_MASK_VEC_IR, "entry"), UInt64, Tuple{Ptr{UInt8}, UInt8}, p, b)
end

@inline function blockmasks(::Val{:vec}, p::Ptr{UInt8}, quoted::Bool, oq::UInt8, delim::UInt8)
    q64 = quoted ? byte_mask_vec(p, oq) : zero(UInt64)
    return q64, specials_mask_vec(p, delim)
end

@inline function blockmasks(::Val{:swar}, p::Ptr{UInt8}, quoted::Bool, oq::UInt8, delim::UInt8)
    q64 = zero(UInt64)
    s64 = zero(UInt64)
    if quoted
        for k in 0:7   # This loop always runs eight times. The compiler can unroll it.
            # Use little-endian byte order so the mask bits follow input order.
            w = ltoh(unsafe_load(Ptr{UInt64}(p + 8k)))
            q64 |= movemask(eqmarks(w, oq)) << (8k)
            sm = eqmarks(w, delim) | eqmarks(w, LF) | eqmarks(w, CR)
            s64 |= movemask(sm) << (8k)
        end
    else
        for k in 0:7
            w = ltoh(unsafe_load(Ptr{UInt64}(p + 8k)))
            sm = eqmarks(w, delim) | eqmarks(w, LF) | eqmarks(w, CR)
            s64 |= movemask(sm) << (8k)
        end
    end
    return q64, s64
end

function indexchunk_fast!(ci::ChunkIndex, buf::Vector{UInt8}, d::Dialect, ::Val{S}) where {S}
    @assert swareligible(d)
    start, stop = ci.start, ci.stop
    delim = d.delim::UInt8
    oq = d.oq
    quoted = d.quoted
    tape = ci.tape
    length(tape) < 256 && resize!(tape, min(max((stop - start + 1) >> 3, 256), MAX_TAPE_HINT))
    n = 0
    inq = false        # whether this block starts inside a quoted field
    pairskip = false   # The last CR in a block already consumed the next LF.
    pos = start
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds while pos + 63 <= stop
            q64, s64 = blockmasks(Val(S), p + pos - 1, quoted, oq, delim)
            inmask = prefix_xor64(q64)
            inq && (inmask = ~inmask)
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
                    if b == CR && pos + tz < stop && buf[pos + tz + 1] == LF
                        tape[n] = ((base + UInt32(tz)) << 2) | UInt32(3)
                        tz < 63 ? (specials &= ~(UInt64(1) << (tz + 1))) : (pairskip = true)
                    else
                        tape[n] = ((base + UInt32(tz)) << 2) | rawkind(b)
                    end
                    specials &= specials - one(UInt64)
                end
            end
            inq ⊻= isodd(count_ones(q64))
            pos += 64
        end
    end
    ci.tape = tape
    # Read the final bytes one at a time. Keep the state from the last full block.
    @inbounds while pos <= stop
        b = buf[pos]
        if inq
            if b == d.cq
                if pos < stop && buf[pos + 1] == d.cq
                    pos += 2
                else
                    inq = false
                    pos += 1
                end
            else
                pos += 1
            end
        elseif quoted && b == oq
            inq = true
            pos += 1
        elseif b == delim || b == LF || b == CR
            if pairskip
                pairskip = false
                pos += 1                             # the LF a block-final CR consumed
            else
                crlf = b == CR && pos < stop && buf[pos + 1] == LF
                tape_room!(tape, n, 1)
                n += 1
                tape[n] = (UInt32(pos - start) << 2) | (crlf ? UInt32(3) : rawkind(b))
                pos += crlf ? 2 : 1
            end
        else
            pos += 1
        end
    end
    return finishscan!(ci, buf, d, n, inq)
end

# --- find safe chunk boundaries ---------------------------------------------
#
# A fixed byte range can start in the middle of a row or a quoted field. The
# parser must know whether each range starts inside a quoted field before it can
# use a line ending as a row boundary.
#
# For standard CSV quote rules, the parser does these steps:
#
#   1. Divide the input into fixed byte ranges.
#   2. Count the quote bytes in each range. Different tasks can count different
#      ranges at the same time.
#   3. Read the counts in file order. The input starts outside a quoted field.
#      An odd count means that the next range starts on the other side of a
#      quote. An even count means that the next range starts on the same side.
#      This tells the parser whether each range starts inside a quoted field.
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
    if commentaware(d) && parityclean(d) && len - datastart + 1 > chunkbytes
        # Raw quote counts are not valid for comment rows. Start at a known row
        # boundary and find later row boundaries in file order. The later index
        # work can still use multiple tasks.
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
    nranges = parityclean(d) ? max(1, cld(len - datastart + 1, chunkbytes)) : 1
    starts = [datastart + (i - 1) * chunkbytes for i in 1:nranges]
    entry = falses(nranges)
    if nranges > 1
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
            entry[i] = acc
        end
    end
    bounds = Vector{Int}(undef, nranges)
    bounds[1] = datastart
    if nranges > 1
        if parallel && tasklimit > 1
            _taskforeach(2:nranges, tasklimit, _taskobserver) do i
                bounds[i] = nextrowstart(buf, starts[i], len, d, entry[i])
            end
        else
            for i in 2:nranges
                bounds[i] = nextrowstart(buf, starts[i], len, d, entry[i])
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
    scanner === :scalar ? indexchunk_scalar!(ci, buf, d) :
    scanner === :swar   ? indexchunk_fast!(ci, buf, d, Val(:swar)) :
                          indexchunk_fast!(ci, buf, d, Val(:vec))
end

# Choose the scanner. Complex quote, escape, or delimiter options require the
# scalar scanner. Use the vector scanner by default for supported input. The
# `:swar` scanner does not require vector instructions. Tests can select it.
function resolvescanner(d::Dialect, fastindex::Bool, scanner::Symbol)
    scanner in (:auto, :vec, :swar, :scalar) ||
        throw(ArgumentError("scanner must be :auto, :vec, :swar, or :scalar (got $(repr(scanner)))"))
    return !(fastindex && swareligible(d)) ? :scalar :
           scanner === :auto ? :vec : scanner
end

"""
    index(buf, d::Dialect; datastart=1, chunkbytes=2^23, parallel=true,
          ntasks=nothing, fastindex=true, scanner=:auto)

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
               scanner::Symbol=:auto,
               _taskobserver=nothing)
    len = length(buf)
    # No lower bound beyond 1: tests deliberately use tiny chunkbytes to force row
    # boundaries everywhere. The standalone index default is 8 MiB; `parse`
    # passes its size-aware 64 KiB–1 MiB default.
    chunkbytes >= 1 || throw(ArgumentError("chunkbytes must be ≥ 1 (got $chunkbytes)"))
    datastart >= 1 || throw(ArgumentError("datastart must be ≥ 1 (got $datastart)"))
    ntasks === nothing || ntasks >= 1 ||
        throw(ArgumentError("ntasks must be ≥ 1 (got $ntasks)"))
    tasklimit = parallel ? min(something(ntasks, Threads.nthreads()), Threads.nthreads()) : 1
    sc = resolvescanner(d, fastindex, scanner)
    datastart > len && return BufferIndex(ChunkIndex[], 0, false)

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

    # Every non-final chunk ends after a complete row. It must therefore end
    # outside a quoted field. A failure here means that chunk planning is wrong.
    for (k, ci) in enumerate(chunks)
        k < length(chunks) && ci.unclosedquote &&
            error("internal error: chunk $(k) ended inside a quoted field")
    end
    # Capture malformed-EOF before filtering: an unclosed quote inside a dropped
    # (e.g. all-comment) chunk must still surface as a Problem.
    unclosed = !isempty(chunks) && last(chunks).unclosedquote
    filter!(ci -> totalrows(ci) > 0, chunks)
    return BufferIndex(chunks, sum(totalrows, chunks; init=0), unclosed)
end

index(buf::Vector{UInt8}; kw...) = index(buf, Dialect(); kw...)

# ---------------------------------------------------------------------------
# L2/L3: typed parsing over the index.
# ---------------------------------------------------------------------------

# CSV changes a column type in this order:
#   Missing → Int64 → Int128 → Float64 → String
#   Missing → (Date | DateTime | Time | Bool) → String
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
@inline _maptype(tm, T) = tm === nothing || T === Missing ? T : get(tm, T, T)
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
@inline _copts(colopts, opts, j::Int) = colopts === nothing ? opts : @inbounds colopts[j]

promote_kernel(a::Type, b::Type) =
    a === b          ? a :
    a === Missing    ? b :
    b === Missing    ? a :
    a === Int64 && b === Int128 ? Int128 :
    a === Int128 && b === Int64 ? Int128 :
    a in (Int64, Int128) && b === Float64 ? Float64 :
    a === Float64 && b in (Int64, Int128) ? Float64 :
    String

# Detect the type of one field. Detection and value parsing use the same Parsers
# functions on the same content bytes. A type conflict therefore always changes
# the column to a type that can accept both field forms.
function detecttype(buf::Vector{UInt8}, pos::Int, len::Int, opts::ValueOpts)
    len == 0 && return Missing
    cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
    st == CELL_MISSING && return Missing
    st == CELL_BADQUOTE && return String    # malformed quoting reports at parse time
    (clen == 0 || esc) && return String     # quoted-empty / escape content is stringy
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
        if Parsers.parsecivil(buf, cpos, cj, opts.datepat)[2] == Parsers.RC_OK
            p = opts.datepat
            return p.hasdate ? (p.hastime ? DateTime : Date) : Time
        end
    else
        Parsers.parsecivil(buf, cpos, cj, opts.datepat)[2] == Parsers.RC_OK && return Date
        Parsers.parsecivil(buf, cpos, cj, opts.datetimepat)[2] == Parsers.RC_OK && return DateTime
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
# (converting one layout to the other after the parse is a full extra pass —
# see UnionColumn below for the measured cost — which is the whole reason two
# layouts exist rather than one plus a conversion.)
#
# Fixed-size isbits values + presence bytes. `Vector{Bool}` (not BitVector): chunk
# tasks write disjoint row ranges concurrently and BitVector packs 64 rows per word
# (a data race); the production version uses a word-aligned bitmap per chunk slice.
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
# Post-parse conversion measures 120-150% of a whole 20 MiB parse (bitsunion
# stores have no memcpy path), which is why the write-direct mode exists.
struct UnionColumn{T}
    uvalues::Vector{Union{T, Missing}}
    UnionColumn{T}(uvalues::Vector{Union{T, Missing}}) where {T} = new{T}(uvalues)
end
UnionColumn{T}(n::Int) where {T} = UnionColumn{T}(Vector{Union{T, Missing}}(undef, n))

@inline function _storevalue!(col::TypedColumn{T}, i::Int, v::T) where {T}
    @inbounds col.values[i] = v
    @inbounds col.present[i] = true
    return
end
@inline function _storevalue!(col::UnionColumn{T}, i::Int, v::T) where {T}
    @inbounds col.uvalues[i] = v
    return
end

# --- strings ------------------------------------------------------------------
# The CompactString type family (payload, accessors, AbstractString interface,
# CompactStringVector, materialize) lives in its own kernel-independent file;
# the quote/escape-aware helpers and the StringColumn staging below are the
# CSV-specific layer over it.
include("compactstring.jl")

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

@inline function _eqmask8_c(w::UInt64, b::UInt8)
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
        n > COMPACTSTRING_INLINE && return nothing
        if n <= 4
            a |= UInt64(c) << (32 + 8 * (n - 1))
        else
            b |= UInt64(c) << (8 * (n - 5))
        end
    end
    return CompactStringPayload(a | UInt64(n % UInt32), b)
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


# The column builder: payloads plus the input and bounded owned buffers that
# long views resolve into.
mutable struct StringColumn
    payloads::Vector{CompactStringPayload}
    buf::Vector{UInt8}
    extra::Vector{UInt8}          # first owned buffer; guarded by extralock
    overflow::Vector{Vector{UInt8}} # further bounded owned buffers
    extralock::ReentrantLock
    e::UInt8                      # escape char
    cq::UInt8                     # close-quote char (e == cq for RFC ""-doubling)
end
StringColumn(payloads::Vector{CompactStringPayload}, buf::Vector{UInt8},
             extra::Vector{UInt8}, extralock::ReentrantLock, e::UInt8, cq::UInt8) =
    StringColumn(payloads, buf, extra, Vector{Vector{UInt8}}(), extralock, e, cq)
StringColumn(n::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8) =
    StringColumn(fill(PAYLOAD_MISSING, n), buf, UInt8[], ReentrantLock(), e, cq)

@inline _ownedcount(col::StringColumn) = 1 + length(col.overflow)
@inline function _ownedbuffer(col::StringColumn, idx::Integer)
    idx == 1 && return col.extra
    return @inbounds col.overflow[Int(idx) - 1]
end
@inline _hasowned(col::StringColumn) = !isempty(col.extra) || !isempty(col.overflow)

# Append one parse-chunk staging buffer to a bounded owned buffer. A staging
# buffer cannot exceed Int32 because field lengths in the structural tape are
# Int32. Packing whole staging buffers keeps every cell contiguous.
function _appendowned_unlocked!(col::StringColumn, bytes::Vector{UInt8},
                                maxbytes::Int=COMPACTSTRING_BUFFER_BYTES)
    0 <= maxbytes <= COMPACTSTRING_BUFFER_BYTES ||
        throw(ArgumentError("invalid CompactString owned-buffer limit $maxbytes"))
    n = length(bytes)
    n <= maxbytes ||
        throw(ArgumentError("a single CSV string staging buffer exceeds the Int32 field limit"))
    idx = _ownedcount(col)
    dst = _ownedbuffer(col, idx)
    if length(dst) > maxbytes - n
        idx < typemax(Int32) ||
            throw(ArgumentError("too many CompactString owned buffers"))
        push!(col.overflow, UInt8[])
        idx += 1
        dst = col.overflow[end]
    end
    base = length(dst)
    append!(dst, bytes)
    return Int32(idx), base
end

function _copyownedbuffers!(dst::StringColumn, src::StringColumn,
                            maxbytes::Int=COMPACTSTRING_BUFFER_BYTES)
    maps = Vector{Tuple{Int32, Int}}(undef, _ownedcount(src))
    @inbounds for idx in eachindex(maps)
        maps[idx] = _appendowned_unlocked!(dst, _ownedbuffer(src, idx), maxbytes)
    end
    return maps
end

@inline function _repointowned(p::CompactStringPayload,
                               maps::Vector{Tuple{Int32, Int}})
    oldidx = Int(csbufidx(p))
    newidx, base = @inbounds maps[oldidx]
    return repoint_payload(p, newidx, base + Int(csoffset(p)))
end

# The kernel's own unescape: `""` collapses to `"` when e == cq; `\X` drops the
# backslash when e != cq. Spans are Int64/Int32 end to end, so a single field
# may be arbitrarily wide (the root cause of CSV.jl issue #935 was a 20-bit
# length cap in an intermediate representation — there is no intermediate here).
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

# All-missing column.

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
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue                      # short row ⇒ missing (reported once per row by the driver)
        pos, len = sp
        len == 0 && continue                            # empty ⇒ missing
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        st == CELL_MISSING && continue                  # sentinel / stripped-to-empty
        if st == CELL_VALUE && clen > 0 && !esc
            ti, tj = _trimblanks(buf, cpos, cpos + clen - 1)
            if ti > tj                              # blanks only: parse the original (invalid) span
                ti, tj = cpos, cpos + clen - 1
            end
            v, ok = parsevalue(T, buf, ti, tj, opts, scratch)
            if ok
                _storevalue!(col, out, v)
                continue
            end
        end
        # invalid for T (also: malformed quoting, quoted-empty, escaped content)
        if userprovided
            problemrow = problemrowbase + localrow
            kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
            message = st == CELL_BADQUOTE ? "malformed quoting in " :
                      "cannot parse $(T) from "
            pushproblem!(problems, problemrow, j, pos, kind, message * excerpt(buf, pos, len))
            # value stays missing under strict=false semantics
        else
            return lr                                   # inference conflict ⇒ promote & re-parse column
        end
    end
    return 0
end

function parsecolchunk!(col::StringColumn, buf::Vector{UInt8}, ci::ChunkIndex,
                        j::Int, rowbase::Int, opts::ValueOpts,
                        userprovided::Bool, problems,
                        problemrowbase::Int=rowbase,
                        mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                        reportlimit::Int=typemax(Int), fromrow::Int=0;
                        viewoffsetlimit::Int=Int(typemax(Int32)))
    payloads = col.payloads
    staging::Union{Nothing, NTuple{4, Vector}} = nothing  # (bytes, rows, offs, lens) for escaped-long cells
    @inbounds for lr in max(fromrow, ci.firstdatarow):totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        out = rowbase + localrow
        mask !== nothing && !mask[maskbase + out] && continue   # excluded row: cell never parsed
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        pos, len = sp
        len == 0 && continue                            # unquoted empty ⇒ missing; quoted "" survives below
        cpos, clen, esc, st = cellcontent(buf, pos, len, opts)
        if st == CELL_BADQUOTE
            # Report invalid quoting and keep the original field bytes as the
            # value. Keep the quotes and do not remove escape bytes. This lets
            # the caller inspect the invalid input.
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_quoted_field,
                         "malformed quoting in " * excerpt(buf, pos, len))
            if len <= COMPACTSTRING_INLINE
                payloads[out] = inline_payload(buf, pos, len)
            elseif pos - 1 <= viewoffsetlimit
                payloads[out] = view_payload(buf, pos, len, 0, pos - 1)
            else
                staging === nothing && (staging = (UInt8[], Int[], Int[], Int[]))
                _stageraw!(staging, buf, pos, len, out)
            end
            continue
        end
        if st == CELL_MISSING
            continue
        end
        if !_wasquoted(buf, pos, len, opts) && _delimclash(buf, cpos, clen, opts.delim)
            problemrow = problemrowbase + localrow
            pushproblem!(problems, problemrow, j, pos, :invalid_value,
                         "bare quote engaged structural protection in " * excerpt(buf, pos, len))
        end
        if esc
            # escaped values are unescaped ONCE, at parse time (CompactString needs O(1)
            # codeunit access): short results build inline payloads allocation-
            # free; long ones stage locally and flush to the shared extra buffer
            # under a single lock per (column × chunk), not per cell
            inl = _unescape_inline(buf, cpos, clen, col.e, col.cq)
            if inl !== nothing
                payloads[out] = inl
            else
                if staging === nothing
                    staging = (UInt8[], Int[], Int[], Int[])
                end
                _stageescaped!(staging, buf, cpos, clen, out, col.e, col.cq)
            end
        elseif clen <= COMPACTSTRING_INLINE
            payloads[out] = inline_payload(buf, cpos, clen)
        elseif cpos - 1 > viewoffsetlimit
            # Arrow StringView stores a signed 32-bit buffer-relative offset.
            # Preserve large-file support by copying only this value into a
            # bounded owned buffer; ordinary in-range values remain zero-copy.
            staging === nothing && (staging = (UInt8[], Int[], Int[], Int[]))
            _stageraw!(staging, buf, cpos, clen, out)
        else
            payloads[out] = view_payload(buf, cpos, clen, 0, cpos - 1)
        end
    end
    staging === nothing || _flushstaging!(col, payloads, staging)
    return 0
end

@inline function _stageraw!(staging::NTuple{4, Vector}, buf::Vector{UInt8},
                            cpos::Int, clen::Int, out::Int)
    sbytes = staging[1]::Vector{UInt8}
    spos = length(sbytes) + 1
    resize!(sbytes, length(sbytes) + clen)
    copyto!(sbytes, spos, buf, cpos, clen)
    push!(staging[2]::Vector{Int}, out)
    push!(staging[3]::Vector{Int}, spos)
    push!(staging[4]::Vector{Int}, clen)
    return
end

# Named top-level helpers, NOT closures: the previous do-block flush captured
# locals that were also reassigned in the parse loop, so Julia boxed them —
# every staged cell then paid allocating Any arithmetic (~2M boxed Ints on a
# 200 MiB mixed file). Same bug class as the task-body war story; same rule.
@inline function _stageescaped!(staging::NTuple{4, Vector}, buf::Vector{UInt8},
                                cpos::Int, clen::Int, out::Int, e::UInt8, cq::UInt8)
    sbytes = staging[1]::Vector{UInt8}
    spos = length(sbytes) + 1
    n = _unescape_append!(sbytes, buf, cpos, clen, e, cq)
    push!(staging[2]::Vector{Int}, out)
    push!(staging[3]::Vector{Int}, spos)
    push!(staging[4]::Vector{Int}, n)
    return
end

function _flushstaging!(col::StringColumn, payloads::Vector{CompactStringPayload},
                        staging::NTuple{4, Vector})
    sbytes = staging[1]::Vector{UInt8}
    srows = staging[2]::Vector{Int}
    soffs = staging[3]::Vector{Int}
    slens = staging[4]::Vector{Int}
    lock(col.extralock)
    try
        bufidx, base = _appendowned_unlocked!(col, sbytes)
        @inbounds for k in eachindex(srows)
            payloads[srows[k]] = view_payload(sbytes, soffs[k], slens[k],
                                              bufidx, base + soffs[k] - 1)
        end
    finally
        unlock(col.extralock)
    end
    return
end


# A column believed all-missing: inferred columns report the first conflict so
# the driver can promote; explicit Missing columns report every present value.
function parsecolchunk_missing(buf::Vector{UInt8}, ci::ChunkIndex, j::Int,
                               rowbase::Int, opts::ValueOpts,
                               userprovided::Bool, problems,
                               mask::Union{Nothing, Vector{Bool}}=nothing, maskbase::Int=0,
                               reportlimit::Int=typemax(Int))
    @inbounds for lr in ci.firstdatarow:totalrows(ci)
        localrow = lr - ci.firstdatarow + 1
        mask !== nothing && !mask[maskbase + localrow] && continue
        localrow > reportlimit && continue
        sp = fieldspan(ci, lr, j)
        sp === nothing && continue
        _, len = sp
        len == 0 && continue
        st = cellcontent(buf, sp[1], len, opts)[4]
        if st != CELL_MISSING
            userprovided || return lr
            out = rowbase + localrow
            kind = st == CELL_BADQUOTE ? :invalid_quoted_field : :invalid_value
            message = st == CELL_BADQUOTE ? "malformed quoting in " :
                      "column typed Missing contains "
            pushproblem!(problems, out, j, sp[1], kind,
                         message * excerpt(buf, sp[1], len))
        end
    end
    return 0
end

# ---------------------------------------------------------------------------
# Problems: errors as data. Bounded (maxproblems) so a pathological file cannot
# exhaust memory. Retention and final order use source order, not task arrival
# order; the count of omitted reports is itself recorded.
# ---------------------------------------------------------------------------

struct Problem
    row::Int          # 1-based data row (0 = file-level problem)
    col::Int          # 1-based column (0 = row-level problem)
    pos::Int          # absolute byte offset into the source buffer
    kind::Symbol      # :short_row | :long_row | :invalid_value | :invalid_quoted_field | :unclosed_quote
    message::String
end

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

problemkey(p::Problem) = (p.pos, p.row, p.col, String(p.kind), p.message)

# problemkey's order without its allocations: Symbol comparison uses the same
# lexical strcmp order as String(Symbol) without materializing either string.
@inline function problemless(a::Problem, b::Problem)
    a.pos != b.pos && return a.pos < b.pos
    a.row != b.row && return a.row < b.row
    a.col != b.col && return a.col < b.col
    a.kind != b.kind && return isless(a.kind, b.kind)
    return a.message < b.message
end

# Bounded retention keeps the `limit` SOURCE-EARLIEST problems. A full log
# maintains its items as a max-heap so displacing the worst retained entry is
# O(log limit) — the previous per-overflow findmax scan was O(limit) each,
# quadratic-by-cap on problem-dense files (measured: a 5%-ragged 20 MiB file
# spent seconds scanning a 10k reservoir per dropped report).
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

@inline locatedless(a::LocatedProblem, b::LocatedProblem) = problemless(a.problem, b.problem)

# Fold one task-local log into the globally bounded reservoir, then release the
# local retained entries. Row ids stay chunk-local until every chunk is indexed.
# Absolute positions are the first problem-key field and chunks do not overlap,
# so later row rebasing cannot change which problems belong under the cap.
# The reservoir keeps the same max-heap-when-full discipline as ProblemLog —
# this loop runs under the lock, so a linear scan per overflow would serialize
# every chunk behind quadratic-by-cap work.
function mergeproblems!(out::PendingProblemLog, log::ProblemLog, chunk::Int)
    log.first === nothing && return
    lock(out.lock) do
        out.dropped += log.dropped
        if log.first !== nothing
            first = LocatedProblem(log.first, chunk)
            (out.first === nothing || locatedless(first, out.first)) &&
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
                        _heapify!(out.items, locatedless)
                        out.heaped = true
                    end
                    @inbounds if locatedless(lp, out.items[1])
                        out.items[1] = lp
                        _siftdown!(out.items, locatedless, 1)
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
        elseif !_wasquoted(buf, pos, len, opts) && _delimclash(buf, cpos, clen, opts.delim)
            names[j] = Symbol(String(buf[pos:pos + len - 1]))
            pushproblem!(log, 0, j, pos, :invalid_value,
                         "bare quote engaged structural protection in header " *
                         excerpt(buf, pos, len))
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

# Content fingerprint of a sampled parsed string (FNV-1a over at most 64
# canonical bytes, then mixed with the canonical length). Quoting, outer
# whitespace, sentinels, and escapes must agree with `StringColumn`: equal
# parsed strings MUST have equal fingerprints or the distinct-count proof below
# would be unsound. A collision in the other direction merely manufactures a
# repeat, which lets pooling be attempted; the parse-time bound still decides.
@inline function _cellhash(buf::Vector{UInt8}, pos::Int, len::Int,
                           e::UInt8, cq::UInt8, escaped::Bool)
    h = 0xcbf29ce484222325
    if !escaped
        @inbounds for i in pos:(pos + min(len, 64) - 1)
            h = (h ⊻ buf[i]) * 0x00000100000001b3
        end
        return _splitmix64(h ⊻ UInt64(len))
    end
    n = 0
    i = pos
    last = pos + len - 1
    @inbounds while i <= last
        b = buf[i]
        if b == e && i < last && (e != cq || buf[i + 1] == cq)
            b = e == cq ? cq : buf[i + 1]
            i += 2
        else
            i += 1
        end
        n += 1
        n <= 64 && (h = (h ⊻ b) * 0x00000100000001b3)
    end
    return _splitmix64(h ⊻ UInt64(n))
end

@inline function _splitmix64(x::UInt64)
    x += 0x9e3779b97f4a7c15
    x = (x ⊻ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ⊻ (x >> 27)) * 0x94d049bb133111eb
    return x ⊻ (x >> 31)
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
allocatecolumn(::Type{String}, n::Int, buf, e, cq) = StringColumn(n, buf, e, cq)
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
# when the requested type needs a checked conversion after parsing.
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

@inline columnopts(p::ColumnPlan, j::Int) =
    p.colopts === nothing ? p.opts : @inbounds(p.colopts[j])

@inline function accessparsetype(d::ColumnDecision)
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
# conversion. This keeps the scalar kernels small and preserves range errors.
const NARROW_TYPES = Dict{Type, Type}(
    Int8 => Int64, Int16 => Int64, Int32 => Int64,
    UInt8 => Int64, UInt16 => Int64, UInt32 => Int64, UInt64 => Int128,
    Float16 => Float64, Float32 => Float64)
_nativetype(T::Type) = get(NARROW_TYPES, T, T)
_customparseable(T::Type) = isconcretetype(T) &&
    (hasmethod(Base.tryparse, Tuple{Type{T}, String}) ||
     hasmethod(Base.parse, Tuple{Type{T}, String}))

# Dict keys can be an integer position, a name, or a regular expression. An
# exact key takes precedence over a regular expression.
function _resolvekeys(dict::AbstractDict, names::Vector{Symbol}, ncols::Int, what::String;
                      validate::Bool=true)
    out = Dict{Int, Any}()
    for (k, v) in dict
        k isa Regex && continue
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
    parsetype = _nativetype(requested)
    parseable = parsetype === Missing ||
                parsetype in (Int64, Int128, Float64, Bool, Date, DateTime, Time,
                              String, BigInt, BigFloat, Base.UUID) ||
                _customparseable(parsetype)
    parseable || throw(ArgumentError("unsupported column type $parsetype"))
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
        throw(ArgumentError("function-typed select/drop is retired; pass a list " *
                            "(or use Tables.Scan for expressions)"))
    idx = Int[]
    if spec isa AbstractVector{Bool}
        length(spec) == length(names) ||
            throw(ArgumentError("Bool select/drop length $(length(spec)) != " *
                                "$(length(names)) columns"))
        append!(idx, findall(spec))
    elseif spec isa AbstractVector{<:Integer}
        append!(idx, Int.(spec))
    else
        (spec isa AbstractString || spec isa Symbol || spec isa Integer) &&
            throw(ArgumentError("select/drop must be a list (got $(typeof(spec)))"))
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

@inline _defaultchunkbytes(nbytes::Int, nthreads::Int=Threads.nthreads()) =
    clamp(cld(nbytes, 4 * nthreads), 1 << 16, 1 << 20)

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
        errormonitor(Threads.@spawn begin
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
        end)
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
`chunkbytes`, `parallel`, `ntasks`, `fastindex`, `scanner`
(:auto | :vec | :swar | :scalar), `maxproblems`,
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
               scanner::Symbol=:auto,
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
    tasklimit = parallel ? min(something(ntasks, Threads.nthreads()), Threads.nthreads()) : 1
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
    datastart = length(buf) >= 3 && buf[1] == 0xef && buf[2] == 0xbb && buf[3] == 0xbf ? 4 : 1  # BOM
    sc = resolvescanner(d, fastindex, scanner)
    # A caller can supply an index that it built earlier. The Scan integration
    # does this when it applies a filter in two steps. The chunk boundaries and
    # CSV options must match the options used to build that index.
    #
    # Assign each captured local value only once. Julia can put a captured value
    # in a `Core.Box` when the code assigns it more than once. Tasks would then
    # share a mutable value, and the compiler could not know its exact type. A
    # test checks that this method does not contain a `Core.Box`.
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
    rl = k -> limit === nothing ? typemax(Int) :
              clamp(limit - rowbases0[k], 0, nrows(chunks[k]))

    if rowmask === nothing
        # -- write directly into the final columns ----------------------------
        # All chunk indexes are complete, so each chunk knows its output rows.
        # It writes values into the final columns. It does not need temporary
        # columns or a later copy step. The API layer can encode repeated strings
        # after this step. The kernel does not encode them.
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
        stitchcol = j -> (cols[j] = stitchcolumn(finalstaged[j], segments, segtypes, j, chunkrows,
                                                 rowbases, ndata, buf, opts.e, d.cq,
                                                 rowmask, rowbases0))
        # single-chunk stitches are zero-copy finalizes — never worth a task spawn
        if tasklimit > 1 && length(stitchjs) > 1 && ndata > 0 && length(chunks) > 1
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
    if on_error === :error && log.first !== nothing
        p = log.first
        nproblems = length(log.items) + log.dropped
        throw(ErrorException("CSV: $(p.kind) at data row $(p.row), column $(p.col): $(p.message)" *
                             (nproblems > 1 ? " (+$(nproblems - 1) more)" : "")))
    end
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
            if nf < ncols
                sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :short_row,
                             "expected $ncols fields, found $nf (remaining columns set to missing)")
            elseif nf > ncols
                sp = fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :long_row,
                             "expected $ncols fields, found $nf (extra fields ignored)")
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
            newT = promote_kernel(T, detecttype(buf, sp[1], sp[2], _copts(colopts, opts, j)))
            newT = newT === T ? String : newT  # a conflicting value must move the type
            T = lock(promolock) do
                promo[j] = _promotemapped(tm, promo[j], newT)
            end
        end
    end
    segments[k] = segs
    segtypes[k] = st
    mergeproblems!(pendingproblems, log, k)
    return
end

# Re-parse one (chunk, column) segment under the final joined type. A top-level
# function on purpose: an earlier version was a closure inside `parse` whose
# `ci = chunks[k]` assignment REBOUND the enclosing function's boxed `ci`
# variable, silently shared across every concurrent task — the textbook Julia
# closure-capture race. Kernel rule: task bodies are named functions.
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
# exact-size final columns at its global row base (the parse loops always
# supported an offset `rowbase`; the staged driver simply passed 0). What this
# removes: per-(column × chunk) staging allocation (~2× the file size of
# transient churn per parse), the stitch's copy pass, and the GC pressure both
# fed. What it costs: on the rare promotion, completed chunks re-parse the
# column instead of stitch-time converting — promotions are what stratified
# sampling exists to make rare. (Dictionary encoding is not a kernel concern:
# the API layer pools a finished CompactString column in one pass when asked.)

# Direct finals allocate UNDEF: each chunk task fills its own slice right
# before parsing it (one page touch, in the task that writes it, parallel at
# chunk granularity instead of column granularity). The rewave fills the
# slices of promoted finals — including fill-only for chunks whose Missing
# parse upgrades for free.
function _allocdirect(::Type{T}, ndata::Int, buf::Vector{UInt8}, opts::ValueOpts,
                      d::Dialect, j::Int, wantunion::Bool=false) where {T}
    T === Missing && return nothing
    T === String && return StringColumn(Vector{CompactStringPayload}(undef, ndata), buf,
                                        UInt8[], ReentrantLock(), opts.e, d.cq)
    wantunion && return UnionColumn{T}(ndata)
    return TypedColumn{T}(Vector{T}(undef, ndata), Vector{Bool}(undef, ndata))
end

# indexed @simd loops, not fill!(view(...)): the SubArray fill does not lower
# to a memset-class loop, and the missing-dense shapes (most rows per byte,
# most fill work per input byte) measurably paid for it
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
    # per union column if done on one task — measured +17-21% at 8T on
    # missing-heavy shapes before this went parallel
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

    # fold the chunks' private escaped-string extras into each final column, in
    # chunk order (before the rewave, so stale re-parses append consistently)
    final = Type[promo[j] for j in 1:ncols]
    for j in allocjs
        final[j] === String || continue
        scol = finals[j]
        scol isa StringColumn || continue
        payloads = scol.payloads
        ks = [k for k in 1:nch if segments[k][j] isa StringColumn &&
                                  _hasowned(segments[k][j]::StringColumn)]
        isempty(ks) && continue
        # Copy whole chunk-owned buffers in source order and update their view
        # words. Owned buffers are rare; this serial fold also makes rollover
        # into a new bounded buffer deterministic.
        for k in ks
            seg = segments[k][j]::StringColumn
            maps = _copyownedbuffers!(scol, seg)
            rhi = k < nch ? rowbases[k + 1] : ndata
            @inbounds for r in (rowbases[k] + 1):rhi
                pl = payloads[r]
                if cslen(pl) > COMPACTSTRING_INLINE && csbufidx(pl) > 0
                    payloads[r] = _repointowned(pl, maps)
                end
            end
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
    finalizeone = j -> begin
        T = final[j]
        cols[j] = T === Missing ? fill(missing, ndata) :
                  T === String ? finalizecolumn(String, finals[j]::StringColumn, ndata) :
                  finalizecolumn(T, finals[j]::Union{TypedColumn{T}, UnionColumn{T}}, ndata)
    end
    finjs = allocjs
    if tasklimit > 1 && length(finjs) > 1 && ndata > (1 << 18)
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
            if nf < ncols
                sp = fieldspan(ci, lr, 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :short_row,
                             "expected $ncols fields, found $nf (remaining columns set to missing)")
            elseif nf > ncols
                sp = fieldspan(ci, lr, ncols + 1)::Tuple{Int, Int}
                pushproblem!(log, localrow, 0, sp[1], :long_row,
                             "expected $ncols fields, found $nf (extra fields ignored)")
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
                # shared payloads, PRIVATE extra: escaped-cell flushes stay
                # uncontended, and the driver concatenates + rebases the (rare)
                # chunk extras in chunk order after the wave
                scol = dest::StringColumn
                hi >= lo && _fillslice!(scol, lo, hi)
                chunkcol = StringColumn(scol.payloads, buf, UInt8[], ReentrantLock(),
                                        scol.e, scol.cq)
                conflict = parsecolchunk!(chunkcol, buf, ci, j, rowbase,
                                          _copts(colopts, opts, j),
                                          userprovided[j], log, 0, nothing, 0, reportlimit)
                segs[j] = _hasowned(chunkcol) ? chunkcol : nothing
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
            # and a captured-and-reassigned local boxes (the staging war story)
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
    hi > rowbase && _fillslice!(finals[j], rowbase + 1, hi)
    conflict = parsecolchunk!(finals[j], buf, ci, j, rowbase, _copts(colopts, opts, j),
                              userprovided[j], log,
                              0, nothing, 0, reportlimit)
    conflict == 0 || error("internal error: re-parse under the joined type conflicted")
    segtypes[k][j] = final[j]
    mergeproblems!(pendingproblems, log, k)
    return
end

# --- pooled (dictionary-encoded) string columns --------------------------------
#
# Each chunk interns strings during parsing. The stitch merges those local level
# tables in chunk order, which preserves first-occurrence order. If the merged
# level count exceeds the policy, the caller degrades the staging and performs a
# flat string stitch without reparsing.
struct PooledColumn{ELT} <: AbstractVector{ELT}
    refs::Vector{UInt32}          # 0 = missing (ELT includes Missing then)
    levels::CompactStringVector{CompactString}
end
Base.size(c::PooledColumn) = size(c.refs)

# widen a missing-free column to its Union{Missing,T} counterpart, zero-copy
# where the container supports it (CompactString views / pooled refs), else a
# converted Base vector
_widenmissing(c::CompactStringVector{CompactString}) =
    CompactStringVector{Union{CompactString, Missing}}(c.payloads, c.buf, c.extra, c.overflow)
_widenmissing(c::PooledColumn{CompactString}) =
    PooledColumn{Union{CompactString, Missing}}(c.refs, c.levels)
_widenmissing(c::Vector{T}) where {T} = convert(Vector{Union{T, Missing}}, c)
_widenmissing(c::AbstractVector) = c

Base.@propagate_inbounds function Base.getindex(c::PooledColumn{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(c.refs, i)
    @inbounds r = c.refs[i]
    r == 0 && return missing
    return c.levels[Int(r)]
end
Base.@propagate_inbounds function Base.getindex(c::PooledColumn{CompactString}, i::Int)
    @boundscheck checkbounds(c.refs, i)
    @inbounds return c.levels[Int(c.refs[i])]
end
poolrefs(c::PooledColumn) = c.refs
poollevels(c::PooledColumn) = c.levels

# the effective (ratio, cap) for column j: per-column override, else global


function materialize(c::PooledColumn{ELT}) where {ELT}
    lv = materialize(c.levels)
    out = Vector{ELT === CompactString ? String : Union{String, Missing}}(undef, length(c.refs))
    @inbounds for i in eachindex(c.refs)
        r = c.refs[i]
        out[i] = r == 0 ? missing : lv[Int(r)]
    end
    return out
end


# Assemble one final exact-size column from its per-chunk segments. Segment
# copies are plain value memmoves (cheap relative to re-reading text from RAM);
# a Missing segment under a wider final type contributes all-absent rows with no
# re-parse. String segments concatenate their extra buffers, rebasing the
# negative (extra-relative) offsets as they copy.
function stitchcolumn(::Type{T}, segments, segtypes, j::Int, chunkrows, rowbases,
                      ndata::Int, buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                      mask::Union{Nothing, Vector{Bool}}=nothing, inbases=nothing) where {T}
    T === Missing && return fill(missing, ndata)
    mask === nothing || return _stitchmasked(T, segments, j, chunkrows, ndata, buf, e, cq,
                                             mask, inbases)
    # Single-chunk files (every input below chunkbytes): the lone segment IS the
    # final column — finalize it directly, zero copies. This keeps the fused
    # driver's small-file cost identical to writing final columns in place.
    if length(chunkrows) == 1
        seg = segments[1][j]
        seg === nothing && return fill(missing, ndata)
        # a limit-clipped boundary segment is larger than the output; only the
        # untouched case may alias the staging directly
        if (seg isa StringColumn ? length(seg.payloads) : length((seg::TypedColumn{T}).values)) == ndata
            return T === String ? finalizecolumn(String, seg::StringColumn, ndata) :
                                  finalizecolumn(T, seg::TypedColumn{T}, ndata)
        end
    end
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        outcol = StringColumn(payloads, buf, UInt8[], ReentrantLock(), e, cq)
        for k in eachindex(chunkrows)
            seg = segments[k][j]
            seg === nothing && continue          # all-missing segment
            scol = seg::StringColumn
            rb = rowbases[k]
            if !_hasowned(scol)
                copyto!(payloads, rb + 1, scol.payloads, 1, chunkrows[k])
            else
                maps = _copyownedbuffers!(outcol, scol)
                @inbounds for i in 1:chunkrows[k]
                    p = scol.payloads[i]
                    if cslen(p) > COMPACTSTRING_INLINE && csbufidx(p) > 0
                        p = _repointowned(p, maps)
                    end
                    payloads[rb + i] = p
                end
            end
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
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata)
end

# Row-filtered stitch: gather only mask-qualifying rows into compact output
# positions (chunk order, so output order is input order). Cells for excluded
# rows were never parsed; their staging slots are simply skipped here.
function _stitchmasked(::Type{T}, segments, j::Int, chunkrows, ndata::Int,
                       buf::Vector{UInt8}, e::UInt8, cq::UInt8,
                       mask::Vector{Bool}, inbases) where {T}
    if T === String
        payloads = fill(PAYLOAD_MISSING, ndata)
        outcol = StringColumn(payloads, buf, UInt8[], ReentrantLock(), e, cq)
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
            maps = _hasowned(scol) ? _copyownedbuffers!(outcol, scol) :
                                     Tuple{Int32, Int}[]
            @inbounds for i in 1:chunkrows[k]
                mask[inbases[k] + i] || continue
                dest += 1
                p = scol.payloads[i]
                if cslen(p) > COMPACTSTRING_INLINE && csbufidx(p) > 0
                    p = _repointowned(p, maps)
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
            values[dest] = tcol.values[i]
            present[dest] = tcol.present[i]
        end
    end
    return finalizecolumn(T, TypedColumn{T}(values, present), ndata)
end

function finalizecolumn(::Type{Missing}, ::Nothing, n::Int)
    return fill(missing, n)
end
finalizecolumn(::Type{Missing}, ::Nothing, n::Int, ::Bool) = fill(missing, n)
function finalizecolumn(::Type{String}, col::StringColumn, n::Int)
    anymissing = any(p -> cslen(p) < 0, col.payloads)
    return anymissing ? CompactStringVector{Union{CompactString, Missing}}(col.payloads, col.buf, col.extra, col.overflow) :
                        CompactStringVector{CompactString}(col.payloads, col.buf, col.extra, col.overflow)
end
function finalizecolumn(::Type{String}, col::StringColumn, n::Int, force_missing::Bool)
    anymissing = force_missing || any(p -> cslen(p) < 0, col.payloads)
    return anymissing ? CompactStringVector{Union{CompactString, Missing}}(col.payloads, col.buf, col.extra, col.overflow) :
                        CompactStringVector{CompactString}(col.payloads, col.buf, col.extra, col.overflow)
end
# `all(::Vector{Bool})` short-circuits, so it compiles to a branchy scalar
# loop — 1.2 ms per 4M-row column. `count` vectorizes; missing-free columns
# (the common case) full-scan either way, 8× faster here.
_allpresent(present::Vector{Bool}) = count(present) == length(present)
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int) where {T}
    # no missings ⇒ hand back the raw Vector{T}, zero copies
    return _allpresent(col.present) ? col.values : _tounion(col)
end
function finalizecolumn(::Type{T}, col::TypedColumn{T}, n::Int, force_missing::Bool) where {T}
    return !force_missing && _allpresent(col.present) ? col.values : _tounion(col)
end
# union-direct finals ARE the output — zero copies either way
finalizecolumn(::Type{T}, col::UnionColumn{T}, n::Int) where {T} = col.uvalues
finalizecolumn(::Type{T}, col::UnionColumn{T}, n::Int, ::Bool) where {T} = col.uvalues

# The sample-missed fallback: sparse missings the 128-row probe didn't see.
# Bitsunion stores have no memcpy path (measured 120-150% of a whole 20 MiB
# parse serially), so slice it across tasks. Named helper, not a closure: the
# captured-and-reassigned boxing war story.
function _tounionrange!(out, values, present, lo::Int, hi::Int)
    @inbounds for i in lo:hi
        out[i] = present[i] ? values[i] : missing
    end
    return
end
function _tounion(col::TypedColumn{T}) where {T}
    values, present = col.values, col.present
    n = length(values)
    out = Vector{Union{T, Missing}}(undef, n)
    nt = Threads.nthreads()
    if n > (1 << 17) && nt > 1
        parts = min(nt, 8)
        @sync for c in 1:parts
            lo = 1 + (c - 1) * n ÷ parts
            hi = c * n ÷ parts
            errormonitor(Threads.@spawn _tounionrange!(out, values, present, lo, hi))
        end
    else
        _tounionrange!(out, values, present, 1, n)
    end
    return out
end

"""
    materialize(col) -> Vector

Convert a kernel column into an ordinary `Vector` (`Vector{T}` or
`Vector{Union{T,Missing}}`), detaching it from the input buffer. String views
allocate real `String`s here — the choice between views and copies is the caller's,
made after parsing instead of before it (this replaces CSV.jl's up-front
`stringtype=` commitment). A column that is already a `Vector` is returned as-is.
"""
materialize(v::AbstractVector) = collect(v)
materialize(v::Vector) = v

# CSV.jl-compatible: a duplicate takes the smallest `name_k` not used by ANY
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
