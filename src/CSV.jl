"""
    CSV

Fast, flexible reading and writing of delimited text.

Reading — `CSV.File`, `CSV.read`, `CSV.Rows`, `CSV.Chunks`, `CSV.sniff`.
Writing — `CSV.write`.
Diagnostics — `CSV.problems`.

The implementation is an index-then-columnar kernel (`CSV.CSVKernel`): a
quote-aware structural index is built once per file, then every column
parses in a monomorphic loop over the index — in parallel, deterministically
for any chunk geometry. The user-facing surface lives in `CSV.CSVApi`.
"""
module CSV

using Tables   # for the Scan-proposal guard below; submodules import their own

include("core.jl")       # CSVKernel: index, values, driver, columns
include("examples.jl")   # KernelExamples: Tables.jl glue + streaming primitives
include("api.jl")        # CSVApi: File / read / Rows / Chunks / sniff / Spec
include("write.jl")      # KernelWrite: write
if isdefined(Tables, :Scan)
    include("scan.jl")   # KernelScan: Tables.Scan pushdown (Tables proposal)
end

using .CSVKernel, .CSVApi, .KernelWrite

# -- public surface -----------------------------------------------------------
const File = CSVApi.File
const Rows = CSVApi.Rows
const Chunks = CSVApi.Chunks
const read = CSVApi.read
const sniff = CSVApi.sniff
const Spec = CSVApi.Spec
const problems = CSVApi.problems
const write = KernelWrite.write
const RowWriter = KernelWrite.RowWriter
const CompactString = CSVKernel.CompactString

export sniff, Spec

end # module CSV
