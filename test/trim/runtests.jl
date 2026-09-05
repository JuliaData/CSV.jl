using Test, JuliaC
@testset "trim compilation and execution" begin
    mktempdir() do dir
        exe = joinpath(dir, "workload")
        workload = joinpath(@__DIR__, "workload.jl")
        project = @__DIR__
        entry = "using JuliaC; if isdefined(JuliaC, :main); JuliaC.main(ARGS); else JuliaC._main_cli(ARGS); end"
        cmd = `$(Base.julia_cmd()) --startup-file=no --project=$project -e $entry -- --output-exe workload --project=$project --experimental --trim=safe $workload`
        log = joinpath(dir, "compile.log")
        result = open(log, "w") do io
            return run(pipeline(ignorestatus(Cmd(cmd; dir=dir)), stdout=io, stderr=io))
        end
        output = read(log, String)
        success(result) || print(output)
        @test success(result)
        @test !occursin(r"[1-9][0-9]* errors,", output)
        @test !occursin(r"[1-9][0-9]* warnings", output)
        @test isfile(exe)
        if isfile(exe)
            @test occursin("trim workload passed", read(`$exe`, String))
        end
    end
end
