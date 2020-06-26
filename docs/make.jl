using Documenter, CSV

makedocs(modules = [CSV], sitename = "CSV.jl")

deploydocs(repo = "github.com/JuliaData/CSV.jl.git")
