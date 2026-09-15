using CSV, DataStrings, DataDecimals
using Dates
using Durations: Timestamp

function (@main)(args::Vector{String})::Cint
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
    Core.println("trim workload passed")
    return 0
end
