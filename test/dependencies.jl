# Temporary review sources. Remove these two pins after their initial General
# registrations merge. The docs-only JSON pin is explained below; all other
# dependencies resolve from General.
using Pkg
root = dirname(@__DIR__)
specs = [
    PackageSpec(url="https://github.com/JuliaData/DataStrings.jl.git",
                rev="c5169652ef1942341c33fbb90a8d9dad3803ce29"),
    PackageSpec(url="https://github.com/JuliaData/DataDecimals.jl.git",
                rev="71da9bc9508d498ecdd1a8eed06b9888e5248c8b"),
]
# Documenter needs JSON; registered JSON still excludes Parsers 3. The user's
# existing JSON PR supplies that compatibility until its release.
if basename(dirname(Base.active_project())) == "docs"
    push!(specs, PackageSpec(url="https://github.com/JuliaIO/JSON.jl.git",
                            rev="bcb8e334682e8135c08913781bf8200832cf752e"))
end
isroot = dirname(Base.active_project()) == root
isroot || push!(specs, PackageSpec(path=root))
Pkg.add(specs)
isroot || Pkg.develop(PackageSpec(path=root))
Pkg.instantiate()
