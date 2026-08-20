using CSV
using Documenter

DocMeta.setdocmeta!(CSV, :DocTestSetup, :(using CSV); recursive=true)

internal_modules = Module[CSV.CSVKernel, CSV.CSVApi, CSV.KernelExamples, CSV.KernelWrite]
isdefined(CSV, :KernelScan) && push!(internal_modules, CSV.KernelScan)

makedocs(;
    root=@__DIR__,
    sitename="CSV.jl",
    authors="CSV.jl contributors",
    modules=[CSV],
    pagesonly=true,
    checkdocs=:public,
    checkdocs_ignored_modules=internal_modules,
    doctest=true,
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://JuliaData.github.io/CSV.jl/stable",
        repolink="https://github.com/JuliaData/CSV.jl",
        edit_link="main",
        collapselevel=2,
    ),
    pages=[
        "Home" => "index.md",
        "Reading" => "reading.md",
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
