using Documenter, CSV

makedocs(;
    modules=[CSV],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
        "Reading" => "reading.md",
        "Writing" => "writing.md",
        "Examples" => "examples.md",
    ],
    repo="https://github.com/JuliaData/CSV.jl/blob/{commit}{path}#L{line}",
    sitename="CSV.jl",
    authors="Jacob Quinn",
    assets=String[],
)

deploydocs(;
    repo="github.com/JuliaData/CSV.jl",
    devbranch = "main"
)