using CSV, DataStrings, DataDecimals, Tables
using Dates
using Durations: Timestamp

# Two schemas with different column counts: an ID column and numeric columns.
# They are constants, so the compiled program knows every column type.
const NARROW = Tables.Schema((:id, :x, :y), (String, Float64, Union{Missing, Int8}))
const WIDE = Tables.Schema((:id, :a, :b, :c, :d),
                           (Int, Float64, Float64, Float64, Union{Missing, Float64}))
# Wider than the tuple lengths Julia unrolls and Tables' 100-column
# specialization: read by row, with a runtime column index.
const MANY = Tables.Schema((:id, ntuple(i -> Symbol(:x, i), 120)...),
                           (Int, ntuple(_ -> Float64, 120)...))
const TEXT = Tables.Schema((:text,), (Union{Missing, DataString},))

function writer()::Cint
    io = IOBuffer()
    CSV.write(io, (id=[1,2], value=["short", "a longer value"]); ntasks=1)
    String(take!(io)) == "id,value\n1,short\n2,a longer value\n" || return 1
    D = DataDecimals.Decimal64{2}
    data = (amount=Union{D,Missing}[D("1.20"), missing],
            label=[DataString("short"), DataString("a long shared value")])
    out = IOBuffer()
    CSV.write(out, data; ntasks=1)
    String(take!(out)) == "amount,label\n1.20,short\n,a long shared value\n" || return 2
    # Cover both inferred resolutions, nullable direct descriptors, and the
    # quoting fallback without reaching Dates' string/Printf printer.
    timestamps = (ns=Union{Missing, Timestamp{Nanosecond}}[
                      Timestamp{Nanosecond}(2020, 1, 2, 3, 4, 5, 0, 0, 123456789), missing],
                  us=[Timestamp{Microsecond}(9999, 12, 31, 0, 0, 0, 0, 1),
                      Timestamp{Microsecond}(1970)])
    CSV.write(out, timestamps; ntasks=1)
    String(take!(out)) == "ns,us\n2020-01-02T03:04:05.123456789,9999-12-31T00:00:00.000001\n,1970-01-01T00:00:00\n" || return 3
    CSV.write(out, timestamps; ntasks=1, delim=':')
    String(take!(out)) == "ns:us\n\"2020-01-02T03:04:05.123456789\":\"9999-12-31T00:00:00.000001\"\n:\"1970-01-01T00:00:00\"\n" || return 4
    # Exercise direct nullable Time output, exact binary fractions, and both
    # inline and buffer-backed strings that require escaping.
    cells = (clock=Union{Time,Missing}[Time(3, 4, 5, 123, 456, 789), missing],
             value=[1.5, -0.125],
             text=[DataString("a,\"b"), DataString("long text without syntax long text without syntax long text without syntax, comma")])
    CSV.write(out, cells; ntasks=1)
    String(take!(out)) == "clock,value,text\n03:04:05.123456789,1.5,\"a,\"\"b\"\n,-0.125,\"long text without syntax long text without syntax long text without syntax, comma\"\n" || return 5
    # A dialect where a float rendering can carry a structural byte takes the
    # quoting fallback; keep that path in the trimmed image too.
    CSV.write(out, (value=[1.5, -0.125],); ntasks=1, delim='.')
    String(take!(out)) == "value\n\"1.5\"\n\"-0.125\"\n" || return 6
    bytes = Vector{UInt8}(codeunits("text longer than inline storage\n\"\"\n\n"))
    file = CSV.File(bytes; schema=TEXT, header=false, ignoreemptyrows=false)
    texts = file.text; texts isa Vector{Union{Missing, DataString}} || return 7
    bytes[1] = UInt8('X')
    texts[1] == "text longer than inline storage" && texts[2] == "" &&
        texts[3] === missing || return 8
    Core.println("trim workload passed")
    return 0
end

_cell(x::Missing) = "missing"
_cell(x) = string(x)

# Print every row, then check the schema and the column types. Exit code 4
# reports a type or schema property that does not hold.
function narrow(rows)::Cint
    Tables.schema(rows) === NARROW || return 4
    eltype(rows) === NamedTuple{(:id, :x, :y), Tuple{String, Float64, Union{Missing, Int8}}} ||
        return 4
    for (i, row) in enumerate(rows)
        Core.println(string(i, "|", repr(row.id), "|", row.x, "|", _cell(row.y)))
    end
    cols = Tables.columntable(rows)
    cols.id isa Vector{String} && cols.x isa Vector{Float64} &&
        cols.y isa Vector{Union{Missing, Int8}} || return 4
    length(cols.id) == length(rows) || return 4
    Core.println(string("columns ", length(cols.id)))
    return 0
end

function wide(rows)::Cint
    Tables.schema(rows) === WIDE || return 4
    eltype(rows) === NamedTuple{(:id, :a, :b, :c, :d),
                                Tuple{Int, Float64, Float64, Float64, Union{Missing, Float64}}} ||
        return 4
    for row in rows
        Core.println(string(row.id, "|", row.a, "|", row.b, "|", row.c, "|", _cell(row.d)))
    end
    cols = Tables.columntable(rows)
    cols.id isa Vector{Int} && cols.d isa Vector{Union{Missing, Float64}} || return 4
    Core.println(string("columns ", length(cols.id)))
    return 0
end

# A large file: summarize instead of printing, and check that the row and
# column readings agree.
function summary(rows)::Cint
    n = 0
    xsum = 0.0
    ymissing = 0
    ysum = 0
    for row in rows
        n += 1
        xsum += row.x
        y = row.y
        y === missing ? (ymissing += 1) : (ysum += y)
    end
    cols = Tables.columntable(rows)
    length(cols.x) == n && sum(cols.x) == xsum && count(ismissing, cols.y) == ymissing ||
        return 4
    Core.println(string("rows=", n, " xsum=", xsum, " ymissing=", ymissing, " ysum=", ysum))
    return 0
end

function many(rows)::Cint
    Tables.schema(rows) === MANY || return 4
    for row in rows
        total = 0.0
        for j in 2:121
            total += row[j]::Float64
        end
        Core.println(string(row.id, "|", total, "|", row.x120))
    end
    return 0
end

widerows(source) = CSV.Rows(source; schema=WIDE, delim=';', decimal=',',
                            missingstring="NA", comment="#")
widefile(source) = CSV.File(source; schema=WIDE, delim=';', decimal=',',
                            missingstring="NA", comment="#")

# `CSV.File` is not parametric, so a trimmed program checks each column's
# type, which the schema fixes, before it uses the column. Print the same
# lines as the row readers.
function narrow(f::CSV.File)::Cint
    ids = f.id; ids isa Vector{String} || return 4
    xs = f.x; xs isa Vector{Float64} || return 4
    ys = f.y; ys isa Vector{Union{Missing, Int8}} || return 4
    for i in eachindex(ids)
        Core.println(string(i, "|", repr(ids[i]), "|", xs[i], "|", _cell(ys[i])))
    end
    Core.println(string("columns ", length(ids)))
    return 0
end

function wide(f::CSV.File)::Cint
    ids = f.id; ids isa Vector{Int} || return 4
    as = f.a; as isa Vector{Float64} || return 4
    bs = f.b; bs isa Vector{Float64} || return 4
    cs = f.c; cs isa Vector{Float64} || return 4
    ds = f.d; ds isa Vector{Union{Missing, Float64}} || return 4
    for i in eachindex(ids)
        Core.println(string(ids[i], "|", as[i], "|", bs[i], "|", cs[i], "|", _cell(ds[i])))
    end
    Core.println(string("columns ", length(ids)))
    return 0
end

function many(f::CSV.File)::Cint
    ids = f.id; ids isa Vector{Int} || return 4
    for i in eachindex(ids)
        total = 0.0
        for j in 2:121
            column = Tables.getcolumn(f, j)
            column isa Vector{Float64} || return 4
            total += column[i]
        end
        x120 = f.x120; x120 isa Vector{Float64} || return 4
        Core.println(string(ids[i], "|", total, "|", x120[i]))
    end
    return 0
end

function summary(f::CSV.File)::Cint
    xs = f.x; xs isa Vector{Float64} || return 4
    ys = f.y; ys isa Vector{Union{Missing, Int8}} || return 4
    ysum = 0
    for y in ys
        y === missing || (ysum += y)
    end
    Core.println(string("rows=", length(xs), " xsum=", sum(xs), " ymissing=",
                        count(ismissing, ys), " ysum=", ysum))
    return 0
end

function reader(mode::String, arg::String)::Cint
    mode == "narrow" && return narrow(CSV.Rows(arg; schema=NARROW))
    mode == "narrow-text" && return narrow(CSV.Rows(IOBuffer(arg); schema=NARROW))
    mode == "narrow-summary" && return summary(CSV.Rows(arg; schema=NARROW))
    mode == "wide" && return wide(widerows(arg))
    mode == "wide-text" && return wide(widerows(IOBuffer(arg)))
    mode == "many" && return many(CSV.Rows(arg; schema=MANY))
    mode == "narrow-file" && return narrow(CSV.File(arg; schema=NARROW))
    mode == "narrow-file-text" && return narrow(CSV.File(IOBuffer(arg); schema=NARROW))
    mode == "narrow-file-summary" && return summary(CSV.File(arg; schema=NARROW))
    mode == "wide-file" && return wide(widefile(arg))
    mode == "wide-file-text" && return wide(widefile(IOBuffer(arg)))
    mode == "many-file" && return many(CSV.File(arg; schema=MANY))
    Core.println(string("unknown mode ", mode))
    return 1
end

function (@main)(args::Vector{String})::Cint
    isempty(args) && return writer()
    length(args) == 2 || return 1
    try
        return reader(args[1], args[2])
    catch err
        if err isa CSV.ParseError
            p = err.problem
            Core.println(string("ParseError ", p.kind, " row=", p.row, " col=", p.col,
                                ": ", p.message))
            return 2
        elseif err isa ArgumentError
            # `msg` is an AbstractString field; a String prints without dispatch
            msg = err.msg
            Core.println(msg isa String ? "ArgumentError: " * msg : "ArgumentError")
            return 3
        end
        rethrow()
    end
end
