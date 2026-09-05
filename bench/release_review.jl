# Compare with the original PR head in a separate environment, using `old` there
# and `new` here. Run with the same Julia version and thread count. See
# docs/research/release-review.md for exact baseline dependency revisions.
# julia --project=test -t4 bench/release_review.jl new
using CSV
println("Julia=", VERSION, " threads=", Threads.nthreads(), " machine=", Sys.MACHINE)
function measure(label, f)
    for _ in 1:3
        f()
    end
    times = Float64[]
    bytes = Int[]
    for _ in 1:12
        GC.gc()
        result = @timed f()
        push!(times, result.time * 1000)
        push!(bytes, result.bytes)
    end
    sort!(times)
    println(label, " median_ms=", round((times[6]+times[7])/2; digits=3),
            " min_ms=", round(first(times);digits=3), " bytes=", minimum(bytes))
end
n = 30_000
numeric = Vector{UInt8}("id,amount\n" * join(("$i,$(i%1000).25" for i in 1:n), '\n') * "\n")
strings = Vector{UInt8}("id,short,long\n" * join(("$i,value$(i%23),a longer value number $(i%1000)" for i in 1:n), '\n') * "\n")
quoted = Vector{UInt8}("id,text\n" * join(("$i,\"a long \"\"quoted\"\" value $i\"" for i in 1:n), '\n') * "\n")
measure("numeric", () -> CSV.File(numeric; delim=','))
measure("strings", () -> CSV.File(strings; delim=','))
measure("escaped", () -> CSV.File(quoted; delim=','))
measure("pooled", () -> CSV.File(strings; delim=',', pool=true))
function rowsum(buf)
    total = 0.0
    for row in CSV.Rows(buf; delim=',', types=Dict(:id=>Int64, :amount=>Float64))
        total += row.amount
    end
    return total
end
measure("rows", () -> rowsum(numeric))
table = (id=collect(1:n), value=["value$(i%23)" for i in 1:n])
measure("write_namedtuple", () -> CSV.write(IOBuffer(), table; ntasks=1))
if only(ARGS) == "new"
    @eval using DataDecimals
    D = DataDecimals.Decimal64{2}
    measure("decimal_explicit", () -> CSV.File(numeric; delim=',', types=Dict(:amount=>D)))
    measure("decimal_inference", () -> CSV.File(numeric; delim=',', inferdecimal=true))
    measure("strings_inference", () -> CSV.File(strings; delim=',', inferdecimal=true))
end
