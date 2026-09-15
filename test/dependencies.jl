# Runtime and documentation dependencies resolve from General.
using Pkg
root = dirname(@__DIR__)
isroot = dirname(Base.active_project()) == root
isroot || Pkg.develop(PackageSpec(path=root))
Pkg.instantiate()
