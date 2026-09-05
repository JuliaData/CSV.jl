using CSV, DataStrings, DataDecimals

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
    Core.println("trim workload passed")
    return 0
end
