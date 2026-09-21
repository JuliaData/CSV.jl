# Run after precompiling with JULIA_CPU_TARGET=generic, and separately with
# --cpu-target=generic --compiled-modules=no, to cover package images and JIT code.
using Test, CSV, Random

@testset "CPU feature dispatch" begin
    masks = [UInt64(0), typemax(UInt64), [UInt64(1) << k for k in 0:63]...,
             rand(MersenneTwister(42), UInt64, 1000)...]
    expected = CSV.prefix_xor64_shift.(masks)
    @test CSV.prefix_xor64.(masks) == expected
    feature = isdefined(CSV, :HAS_PCLMUL) ? CSV.HAS_PCLMUL :
              isdefined(CSV, :HAS_PMULL) ? CSV.HAS_PMULL : nothing
    if feature !== nothing
        enabled = feature[]
        try
            for usefeature in (enabled, false)
                feature[] = usefeature
                @test CSV.prefix_xor64.(masks) == expected
                input = "a,b\n" * "1,\"x,y\"\n"^100
                f = CSV.File(IOBuffer(input); chunkbytes=65)
                @test f.a == fill(1, 100)
                @test f.b == fill("x,y", 100)
            end
        finally
            feature[] = enabled
        end
    end
end
