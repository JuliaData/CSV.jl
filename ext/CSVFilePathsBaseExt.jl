# FilePathsBase paths as sources and sinks.
#
# `CSV.File(p"data.csv")` and `CSV.write(tmp / "out.csv", t)` work when
# FilePathsBase is loaded. A path resolves through its string form, so every
# reader (File/read/Rows/Chunks/lazy — gzip by magic bytes, mmap, prefetch) and the
# writer (compress=:auto by extension, append) behave exactly as with a
# String path.
module CSVFilePathsBaseExt

using CSV, FilePathsBase

CSV.resolvesource(p::FilePathsBase.AbstractPath; kw...) =
    CSV.resolvesource(string(p); kw...)
CSV._sourcename(p::FilePathsBase.AbstractPath) = string(p)
CSV._sourceprovenance(p::FilePathsBase.AbstractPath, ::Int) = string(p)

function CSV.write(sink::FilePathsBase.AbstractPath, table; kw...)
    CSV.write(string(sink), table; kw...)
    return sink
end

end # module
