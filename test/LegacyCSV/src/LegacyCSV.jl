# The 0.10-era CSV.jl, loaded under a different name so the new package's
# tests can use it as a behavioral ORACLE. The sources are the frozen copy in
# legacy/src/; only the module wrapper is rewritten at include time.
module LegacyCSV
const CSV = LegacyCSV   # the legacy sources reference themselves as `CSV.`
const _LEGACY_SRC = normpath(joinpath(@__DIR__, "..", "..", "..", "legacy", "src"))
let src = read(joinpath(_LEGACY_SRC, "CSV.jl"), String)
    lo = findfirst(r"^module CSV\s*$"m, src)
    hi = let ms = collect(eachmatch(r"^end # module\s*$"m, src))
        isempty(ms) ? nothing : (ms[end].offset:(ms[end].offset + length(ms[end].match) - 1))
    end
    lo === nothing && error("legacy CSV.jl: module header not found")
    hi === nothing && error("legacy CSV.jl: module footer not found")
    body = src[last(lo)+1:first(hi)-1]
    body = replace(body, "dirname(pathof(CSV))" => "_LEGACY_SRC")
    body = replace(body, r"include\(\"([^\"]+)\"\)" => s"include(joinpath(_LEGACY_SRC, \"\1\"))")
    include_string(@__MODULE__, body, "LegacyCSV(CSV.jl)")
end
end
