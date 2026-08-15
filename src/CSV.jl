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

# -- precompile workload -------------------------------------------------------
# The kernel's monomorphic per-column loops are exactly what makes first-File
# expensive to compile (~4 s cold on an M3). One small in-memory pass through
# every front door caches those specializations: File (inference, promotion,
# pooling, missing, all eight lattice types, gzip, parallel driver,
# stringtype=String materializer), Rows, Chunks, sniff, write, RowWriter.
using PrecompileTools: @setup_workload, @compile_workload
import Dates, CodecZlib
@setup_workload begin
    mixed = "int,float,date,datetime,bool,null,str,catg,int_float\n" *
            "1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n" *
            "2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"
    pooled = "s,t\na1,x1\na2,\na0,x3\na1,x4\na2,x5\n"
    @compile_workload begin
        f = File(IOBuffer(mixed))
        Tables.columntable(f)
        problems(f)
        File(IOBuffer(pooled); pool=(0.5, 100))
        File(IOBuffer(mixed); stringtype=String)
        File(IOBuffer(mixed); parallel=true, ntasks=2, chunkbytes=1 << 10)
        gz = IOBuffer()
        gzs = CodecZlib.GzipCompressorStream(gz)
        Base.write(gzs, mixed)
        close(gzs)
        File(take!(gz))
        foreach(identity, Rows(IOBuffer("a,b\n1,x\n2,y\n")))
        first(Chunks(IOBuffer(pooled); chunkbytes=1 << 16))
        sniff(IOBuffer(mixed))
        out = IOBuffer()
        write(out, (a=[1, 2], b=["x", "y,z"], c=[1.5, missing],
                    d=[Dates.Date(2024, 1, 2), Dates.Date(2024, 3, 4)]))
        join(RowWriter((a=[1], b=["x"])))
    end
end

end # module CSV
