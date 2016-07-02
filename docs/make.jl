using Documenter, CSV

makedocs(
    modules = [CSV],
)

deploydocs(
    deps = Deps.pip("mkdocs", "python-markdown-math"),
    repo = "github.com/JuliaData/CSV.jl.git"
)
