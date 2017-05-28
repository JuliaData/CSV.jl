using PkgBenchmark, CSV, WeakRefStrings
if !is_windows()
    using DecFP
end

prep(io::IO, ::Type{Int64}) = write(io, "10")
prep(io::IO, ::Type{Float64}) = write(io, "10.0")
prep(io::IO, ::Type{WeakRefString{UInt8}}) = write(io, "hey there sailor")
prep(io::IO, ::Type{String}) = write(io, "hey there sailor")
prep(io::IO, ::Type{Date}) = write(io, "2016-09-28")
prep(io::IO, ::Type{DateTime}) = write(io, "2016-09-28T03:21:00")
if !is_windows()
    prep(io::IO, ::Type{Dec64}) = write(io, "10.0")
end

function prep{T}(::Type{IOBuffer}, ::Type{T})
    io = IOBuffer()
    prep(io, T)
    seekstart(io)
    return io, ()->return
end
function prep{T}(::Type{IOStream}, ::Type{T})
    t = tempname()
    io = open(t, "w")
    prep(io, T)
    close(io)
    io = open(t, "r")
    return io, ()->rm(t)
end

TYPES = !is_windows() ? (Int, Float64, WeakRefString{UInt8}, String, Date, DateTime, Dec64) : (Int, Float64, WeakRefString{UInt8}, String, Date, DateTime)

@benchgroup "CSV" begin
    @benchgroup "CSV.parsefield" begin
        opts = CSV.Options()
        row = col = 1
        state = Ref{CSV.ParsingState}(CSV.None)
        for I in (IOBuffer, IOStream)
            for T in TYPES
                io, f = prep(I, T)
                @bench "$I - $T" CSV.parsefield($io, $T, opts, row, col, state)
                f()
            end
        end
    end
    FILE = joinpath(dirname(@__FILE__), "randoms_small.csv")

    @benchgroup "CSV.read" begin
        @bench "CSV.read" CSV.read(FILE)
    end

    @benchgroup "CSV.write" begin
        df = CSV.read(FILE)
        t = tempname()
        @bench "CSV.write" CSV.write(t, df)
    end

end




# generate single column files w/ 1M rows for each type
using WeakRefStrings

val = "hey"
for i in (1001, 100.1, WeakRefString{UInt8}(pointer(val), 3, 0), Date(2008, 1, 3), DateTime(2008, 3, 4))
    open("/Users/jacobquinn/Downloads/randoms_$(typeof(i)).csv", "w") do f
        for j = 1:1_000_000
            write(f, string(i))
            write(f, "\n")
        end
    end
end

using CSV, TextParse
for T in (Int, Float64, WeakRefString{UInt8}, Date, DateTime)
    println("comparing for T = $T...")
    @time CSV.read("/Users/jacobquinn/Downloads/randoms_$(T).csv"; nullable=false)
    @time TextParse.csvread("/Users/jacobquinn/Downloads/randoms_$(T).csv")
end
# julia> for T in (Int, Float64, WeakRefString{UInt8}, Date, DateTime)
#        @time CSV.read("/Users/jacobquinn/Downloads/randoms_$(T).csv"; nullable=false)
#        # Int = 0
#        @time TextParse.csvread("/Users/jacobquinn/Downloads/randoms_$(T).csv")
#    end
#     pre-allocating DataFrame w/ rows = 999999
#       0.033606 seconds (2.40 k allocations: 7.758 MiB, 23.17% gc time)
#       0.050906 seconds (457 allocations: 15.574 MiB)
#     pre-allocating DataFrame w/ rows = 999999
#       0.130082 seconds (5.00 M allocations: 84.128 MiB, 17.92% gc time)
#       0.090331 seconds (457 allocations: 16.528 MiB, 4.83% gc time)
#     pre-allocating DataFrame w/ rows = 999999
#       0.151771 seconds (5.01 M allocations: 160.729 MiB, 13.58% gc time)
#       0.070806 seconds (595 allocations: 5.188 MiB)
#     pre-allocating DataFrame w/ rows = 999999
#       0.344441 seconds (6.01 M allocations: 114.772 MiB, 54.32% gc time)
#       0.156282 seconds (1.00 M allocations: 51.846 MiB, 5.05% gc time)
#     pre-allocating DataFrame w/ rows = 999999
#       0.236832 seconds (6.01 M allocations: 114.899 MiB, 11.88% gc time)
#       0.186432 seconds (1.00 M allocations: 60.516 MiB, 4.47% gc time)

T = Int64
source = CSV.Source("/Users/jacobquinn/Downloads/randoms_$(T).csv"; nullable=false)
@time CSV.read(source)
sink = Si = DataFrames.DataFrame
transforms = Dict{Int,Function}()
append = false
args = kwargs = ()
@code_warntype DataStreams.Data.stream!(source, sink, append, transforms, args...)
source_schema = DataStreams.Data.schema(source)
@code_warntype DataStreams.Data.transform(source_schema, transforms)
sink_schema, transforms2 = DataStreams.Data.transform(source_schema, transforms)
sinkstreamtype = DataStreams.Data.Field
sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...)
columns = []
filter = x->true
@code_warntype DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)
@time DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)

@code_warntype CSV.parsefield(IOBuffer(), Int, CSV.Options(), 0, 0, CSV.STATE)

# having CSV.parsefield(io, T) where T !>: Null decreases allocations by 1.00M
# inlining CSV.parsefield also dropped allocations
# making CSV.Options not have a type parameter also sped things up
#

using BenchmarkTools

g(x) = x < 5 ? x : -1
A = [i for i = 1:10]
function get_then_set(A)
    @simd for i = 1:10
        @inbounds A[i] = g(i)
    end
    return A
end
@code_warntype g(1)
@code_warntype get_then_set(A)
@benchmark get_then_set(A) # 20ns

g2(x) = x < 5 ? x : nothing

A = Union{Int, Void}[i for i = 1:10]
function get_then_set2(A)
    @simd for i = 1:10
        @inbounds A[i] = g2(i)
    end
    return A
end
@code_warntype g2(1)
@code_warntype get_then_set2(A)
@code_llvm get_then_set2(A)
@benchmark get_then_set2(A) # 155ns


g4(x::Int) = 1
g4(x::Void) = 0

A = [i for i = 1:10]
function get_sum(A)
    s = 0
    for a in A
        s += g4(a)
    end
    return s
end
@code_warntype get_sum(A)
@code_llvm get_sum(A)
@benchmark get_sum(A) # 24ns

A = Union{Int, Void}[i for i = 1:10]
A[[3, 5, 7]] = nothing
function get_sum2(A)
    s = 0
    for a in A
        s += g4(a)
    end
    return s
end
@code_warntype get_sum2(A)
@code_llvm get_sum(A)
@benchmark get_sum2(A) # 100ns