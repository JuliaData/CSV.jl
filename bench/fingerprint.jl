# Differential fingerprint: run under two envs, diff the outputs.
using CSV, Tables, Dates, SHA, InlineStrings
include(joinpath(@__DIR__, "bench_matrix.jl"))
function fingerprint(t)
    io = IOBuffer()
    cols = Tables.columns(t)
    for nm in Tables.columnnames(cols)
        c = Tables.getcolumn(cols, nm)
        print(io, nm, "::", eltype(c), "|")
        for x in c
            print(io, x === missing ? "\x00" : x, "\x01")
        end
    end
    if t isa CSV.File
        for p in CSV.problems(t); print(io, p.row, ",", p.col, ",", p.pos, ",", p.kind, ";"); end
        print(io, "dropped=", getfield(t, :table).droppedproblems)
    end
    return bytes2hex(sha1(take!(io)))
end
const MB = 1
for shape in ALLSHAPES
    buf = makedata(shape, MB * 2^20)
    kw = shapeoptions(shape)
    println(shape, " default      ", fingerprint(CSV.File(buf; on_error=:collect, kw...)))
    println(shape, " ntasks1      ", fingerprint(CSV.File(buf; on_error=:collect, ntasks=1, kw...)))
    println(shape, " chunk64k     ", fingerprint(CSV.File(buf; on_error=:collect, chunkbytes=1<<16, kw...)))
    println(shape, " scalar       ", fingerprint(CSV.File(buf; on_error=:collect, scanner=:scalar, kw...)))
    println(shape, " swar         ", fingerprint(CSV.File(buf; on_error=:collect, scanner=:swar, kw...)))
    println(shape, " string       ", fingerprint(CSV.File(buf; on_error=:collect, stringtype=String, kw...)))
    println(shape, " pool         ", fingerprint(CSV.File(buf; on_error=:collect, pool=true, kw...)))
    println(shape, " limit        ", fingerprint(CSV.File(buf; on_error=:collect, limit=1000, kw...)))
    println(shape, " skipto       ", fingerprint(CSV.File(buf; on_error=:collect, skipto=500, kw...)))
    println(shape, " footerskip   ", fingerprint(CSV.File(buf; on_error=:collect, footerskip=7, kw...)))
    println(shape, " chunks       ", join([fingerprint(f) for f in CSV.Chunks(buf; on_error=:collect, kw...)], ","))
    println(shape, " rows         ", fingerprint(Tables.columntable(CSV.Rows(buf; kw...))))
    println(shape, " lazy         ", fingerprint(Tables.columntable(CSV.lazy(buf; kw...))))
    println(shape, " inline       ", fingerprint(CSV.File(buf; on_error=:collect, stringtype=InlineString, kw...)))
    println(shape, " typesstring  ", fingerprint(CSV.File(buf; on_error=:collect, types=String, kw...)))
    println(shape, " narrow       ", fingerprint(CSV.File(buf; on_error=:collect, typemap=Dict(Int64=>Int32), kw...)))
    if isdefined(Tables, :Scan)
        nms = Tables.columnnames(Tables.columns(CSV.File(buf; on_error=:collect, kw...)))
        println(shape, " scan         ", fingerprint(CSV.File(buf; on_error=:collect, scan=Tables.Scan(select=(nms[1], nms[end]), offset=3, limit=5000), kw...)))
    end
    io = IOBuffer(); CSV.write(io, CSV.File(buf; on_error=:collect, kw...)); println(shape, " write        ", bytes2hex(sha1(take!(io))))
end
# a few dialect-specific cases
mixed = makedata(:mixed, MB * 2^20)
println("mixed comment      ", fingerprint(CSV.File(Vector{UInt8}(replace(String(copy(mixed)), "\n1000," => "\n# c\n1000,")); comment="#", on_error=:collect)))
println("mixed types narrow ", fingerprint(CSV.File(mixed; types=Dict(:id=>Int32, :value=>Int16, :ratio=>Float32), on_error=:collect, maxproblems=50)))
println("mixed types narrow0", fingerprint(CSV.File(mixed; types=Dict(:id=>Int32, :value=>Int16), on_error=:collect, maxproblems=0)))
println("mixed multi        ", fingerprint(CSV.File([mixed, mixed]; on_error=:collect)))
println("mixed transpose    ", fingerprint(CSV.File(makedata(:wide, 200_000); transpose=true, on_error=:collect)))
println("mixed select       ", fingerprint(CSV.File(mixed; select=r"^(id|ratio)$", on_error=:collect)))
esc = makedata(:escaped, MB * 2^20)
println("escaped backslash  ", fingerprint(CSV.File(Vector{UInt8}(replace(String(copy(esc)), "\"\"" => "\\\"")); escapechar='\\', on_error=:collect)))
println("dates              ", fingerprint(CSV.File(IOBuffer("d,dt\n2020-02-29,2020-02-29T23:59:59.999\n0001-01-01,9999-12-31T00:00:00\n2021-02-29,2020-13-01T00:00:00\n"); on_error=:collect, types=Dict(:d=>Date, :dt=>DateTime))))
println("dates inferred     ", fingerprint(CSV.File(IOBuffer("d,dt\n2020-02-29,2020-02-29T23:59:59.999\n0001-01-01,9999-12-31T00:00:00\n"); on_error=:collect)))
# ---- writer fingerprints (decompressed bytes for gzip: member layout may differ) ----
using CodecZlib
wsha(f) = (io = IOBuffer(); f(io); bytes2hex(sha1(take!(io))))
gunzipsha(f) = (io = IOBuffer(); f(io); bytes2hex(sha1(transcode(GzipDecompressor, take!(io)))))
for shape in (:numeric, :mixed, :strings, :quoted, :temporal, :pooled_low, :sentinel, :wide)
    f = CSV.File(makedata(shape, MB * 2^20); on_error=:collect, shapeoptions(shape)...)
    ct = Tables.columntable(f)
    println(shape, " w/file        ", wsha(io -> CSV.write(io, f)))
    println(shape, " w/file nt1    ", wsha(io -> CSV.write(io, f; ntasks=1)))
    println(shape, " w/ct          ", wsha(io -> CSV.write(io, ct)))
    println(shape, " w/quoteall    ", wsha(io -> CSV.write(io, ct; quotestyle=:all)))
    println(shape, " w/floatfmt    ", wsha(io -> CSV.write(io, ct; floatformat="%.3f")))
    println(shape, " w/floatfmt,   ", wsha(io -> CSV.write(io, ct; floatformat="%.2e", decimal=',', delim=';')))
    println(shape, " w/transform   ", wsha(io -> CSV.write(io, ct; transform=(c, v) -> v)))
    println(shape, " w/transform2  ", wsha(io -> CSV.write(io, ct; transform=(c, v) -> v isa Number ? v * 2 : v)))
    println(shape, " w/gzip        ", gunzipsha(io -> CSV.write(io, ct; compress=true)))
    println(shape, " w/gzip nt1    ", gunzipsha(io -> CSV.write(io, ct; compress=true, ntasks=1)))
    println(shape, " w/gzip bom    ", gunzipsha(io -> CSV.write(io, ct; compress=true, bom=true)))
    println(shape, " w/gzip nohdr  ", gunzipsha(io -> CSV.write(io, ct; compress=true, header=false)))
    println(shape, " w/rows        ", wsha(io -> CSV.write(io, Tables.rowtable(Tables.subset(ct, 1:200)))))
    println(shape, " w/rowwriter   ", bytes2hex(sha1(join(CSV.RowWriter(Tables.subset(ct, 1:200))))))
end
narrow = (a=Int32[1, -2, typemax(Int32)], b=Union{Missing,Int16}[1, missing, typemin(Int16)], c=UInt8[0, 255, 7],
          d=UInt64[typemax(UInt64), 1, 2], e=Float32[1.5f0, 0.1f0, -2f0], f=["x", "y,z", " w"])
println("narrow write       ", wsha(io -> CSV.write(io, narrow)))
println("narrow transform   ", wsha(io -> CSV.write(io, narrow; transform=(c, v) -> string(typeof(v), ":", v))))
empty = (a=Int[], b=String[])
println("empty gzip         ", gunzipsha(io -> CSV.write(io, empty; compress=true)))
println("empty gzip bom     ", gunzipsha(io -> CSV.write(io, empty; compress=true, bom=true)))
println("gz roundtrip read  ", fingerprint(CSV.File(let io = IOBuffer(); CSV.write(io, Tables.columntable(CSV.File(makedata(:mixed, 2^20); on_error=:collect)); compress=true); take!(io) end)))
