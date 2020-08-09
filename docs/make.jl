using Documenter, CSV

makedocs(modules = [CSV], sitename = "CSV.jl", format = HTML(canonical = "https://juliadata.github.io/CSV.jl/stable/"))

deploydocs(repo = "github.com/JuliaData/CSV.jl.git")
