# The 0.10-era CSV.jl, loaded under a different name so the new package's
# tests can use it as a behavioral ORACLE. The sources are the frozen copy in
# legacy/src/; only the module wrapper is rewritten at include time.
module LegacyCSV
# Do not define `VERSION` here. The frozen sources use the imported
# `Base.VERSION` for Julia-runtime compatibility branches.
const CSV = LegacyCSV   # the legacy sources reference themselves as `CSV.`
const _LEGACY_SRC = normpath(joinpath(@__DIR__, "..", "..", "..", "legacy", "src"))

# Rewrite every source file, not only CSV.jl. This keeps nested includes inside
# the frozen tree and makes `dirname(pathof(CSV))` sound in optional workload
# files even though `CSV` is an alias for this shim module.
function _rewritesource(src::String)
    src = replace(src, "dirname(pathof(CSV))" => "_LEGACY_SRC")
    return replace(src, r"include\(\"([^\"]+)\"\)" => s"_includelegacy(\"\1\")")
end
function _includelegacy(file::String)
    path = normpath(joinpath(_LEGACY_SRC, file))
    startswith(path, _LEGACY_SRC * Base.Filesystem.path_separator) ||
        error("legacy include escapes source tree: $file")
    return include_string(@__MODULE__, _rewritesource(Base.read(path, String)),
                          "LegacyCSV(" * file * ")")
end

let src = Base.read(joinpath(_LEGACY_SRC, "CSV.jl"), String)
    lo = findfirst(r"^module CSV\s*$"m, src)
    hi = let ms = collect(eachmatch(r"^end # module\s*$"m, src))
        isempty(ms) ? nothing : (ms[end].offset:(ms[end].offset + length(ms[end].match) - 1))
    end
    lo === nothing && error("legacy CSV.jl: module header not found")
    hi === nothing && error("legacy CSV.jl: module footer not found")
    body = src[last(lo)+1:first(hi)-1]
    include_string(@__MODULE__, _rewritesource(body), "LegacyCSV(CSV.jl)")
end
end
