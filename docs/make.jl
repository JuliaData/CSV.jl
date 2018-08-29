using Documenter, CSV

makedocs(
    modules = [CSV],
    format = :html,
    sitename = "CSV.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    repo = "github.com/JuliaData/CSV.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
    julia = "0.7",
    osname = "linux"
)
