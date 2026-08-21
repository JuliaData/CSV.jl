using Test
using Aqua
using CSV

@testset "package quality" begin
    # Aqua starts a new environment for this check. That environment cannot
    # install Parsers 3 until Parsers 3 is registered.
    # TODO: Turn this check on after the Parsers 3 release.
    Aqua.test_all(CSV; persistent_tasks=false)
end
