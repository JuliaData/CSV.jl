using Documenter, CSV

makedocs(;
    modules=[CSV],
    format=Documenter.HTML(prettyurls=false),
    pages=[
        "Home" => "index.md",
        "Reading" => "reading.md",
        "Writing" => "writing.md",
        "Examples" => "examples.md",
    ],
    repo="https://github.com/JuliaData/CSV.jl/blob/{commit}{path}#{line}",
    sitename="CSV.jl",
    authors="Jacob Quinn",
    warnonly = Documenter.except(),
)

deploydocs(;
    repo="github.com/JuliaData/CSV.jl",
    devbranch = "main"
)
