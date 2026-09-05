using CSV
using Documenter

DocMeta.setdocmeta!(CSV, :DocTestSetup, :(using CSV); recursive=true)

makedocs(;
    root=@__DIR__,
    sitename="CSV.jl",
    authors="CSV.jl contributors",
    modules=[CSV],
    pagesonly=true,
    checkdocs=:public,
    doctest=true,
    format=Documenter.HTML(;
        prettyurls=true,
        canonical="https://JuliaData.github.io/CSV.jl/stable",
        repolink="https://github.com/JuliaData/CSV.jl",
        edit_link="main",
        collapselevel=2,
    ),
    pages=[
        "Home" => "index.md",
        "Reading" => "reading.md",
        "Decimal columns" => "decimals.md",
        "Writing" => "writing.md",
        "Examples" => "examples.md",
        "API reference" => "reference.md",
        "1.0 release notes" => "release-notes.md",
        "Migrating to 1.0" => "migration.md",
    ],
)

if get(ENV, "GITHUB_ACTIONS", "false") == "true"
    deploydocs(;
        repo="github.com/JuliaData/CSV.jl.git",
        devbranch="main",
    )
end
