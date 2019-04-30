using Documenter, CSV

makedocs(
    modules = [CSV],
    sitename = "CSV.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    repo = "github.com/JuliaData/CSV.jl.git",
    target = "build"
)
