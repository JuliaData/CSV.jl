# FilePathsBase paths as sources and sinks.
#
# 0.10 carried FilePathsBase as a hard dependency so `CSV.File(p"data.csv")`
# and `CSV.write(tmp / "out.csv", t)` worked; 1.0 moves that to an extension.
# A path resolves through its string form, so every reader front door
# (File/read/Rows/Chunks/sniff — gzip by extension, mmap, prefetch) and the
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
