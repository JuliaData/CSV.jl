using Test, Aqua, CSV

@testset "package quality" begin
    # The isolated fresh-process check cannot resolve the two packages until
    # their initial General registrations merge. All other Aqua checks run.
    Aqua.test_all(CSV; persistent_tasks=false)
end
