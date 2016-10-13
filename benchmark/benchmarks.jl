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
