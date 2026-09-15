# Breadth benchmark over the public option surface: File keyword axes, source
# kinds, Rows, Chunks, lazy, Scan, read, and the writer's option axes.
#
#   julia --project=<env> -t8 bench_surface.jl LABEL [--quick] [--only=regex]
#
# Appends TSV rows (label, case, bytes, ms, allocbytes, MiB/s) to
# surface-LABEL.tsv next to this file. Inputs never copy inside the timed
# region. Compare two labels with compare.py.
using CSV, Tables, Dates, Random, Printf, CodecZlib, PooledArrays, InlineStrings, DataStrings
include(joinpath(@__DIR__, "bench_matrix.jl"))          # makedata, shapeoptions, besttime
include(joinpath(@__DIR__, "writeshapes.jl"))         # shape(kind, n, rng)

const LABEL = isempty(ARGS) ? "run" : ARGS[1]
const QUICK = "--quick" in ARGS
const ONLY = let o = filter(a -> startswith(a, "--only="), ARGS); isempty(o) ? "" : o[1][8:end] end
const MB = QUICK ? 4 : 20
const NROWS_W = QUICK ? 200_000 : 1_000_000
const OUT = open(joinpath(@__DIR__, "surface-$LABEL.tsv"), "a")
const FAILURES = String[]
println(OUT, "# threads=$(Threads.nthreads()) julia=$(VERSION) at=$(now()) MB=$MB")

function runcase(name::String, f, bytes::Int; reps=5)
    (isempty(ONLY) || occursin(Regex(ONLY), name)) || return
    t, al = try
        besttime(f; reps, mintime=0.05)
    catch err
        println(rpad(name, 44), "ERROR ", first(sprint(showerror, err), 120))
        push!(FAILURES, name)
        return
    end
    println(OUT, join((LABEL, name, bytes, round(t * 1e3, digits=3), al,
                       round(bytes / 2^20 / t, digits=1)), '\t'))
    flush(OUT)
    @printf("%-44s %9.2f ms %9.1f MiB/s %8.1f MiB alloc\n", name, t * 1e3, bytes / 2^20 / t, al / 2^20)
    flush(stdout)
end

# ---------------------------------------------------------------------------
# inputs (generated once, outside timing)
# ---------------------------------------------------------------------------
gen(shape) = makedata(shape, MB * 2^20)
bufnum, bufmix, bufstr, bufpool, buftemp = gen(:numeric), gen(:mixed), gen(:strings), gen(:pooled_low), gen(:temporal)
bufdirty, bufwide, bufcrlf, bufsent, bufgm, bufir = gen(:dirty), gen(:wide), gen(:crlf), gen(:sentinel), gen(:groupmark), gen(:irspace)
bufesc = gen(:escaped)
nmix = Tables.rowcount(CSV.File(bufmix))
# derived dialect variants
bufdec = let s = String(copy(gen(:floatonly))); Vector{UInt8}(replace(replace(s, ',' => ';'), '.' => ',')) end
bufdd = Vector{UInt8}(replace(String(copy(bufnum)), "," => "::"))
bufesc2 = Vector{UInt8}(replace(String(copy(bufesc)), "\"\"" => "\\\""))
bufbracket = Vector{UInt8}(replace(String(copy(bufstr)), r"\"([^\"]*)\"" => s"[\1]"))
bufnohdr = let s = String(copy(bufnum)); Vector{UInt8}(s[findfirst('\n', s)+1:end]) end
bufcmt = let lines = split(String(copy(bufmix)), '\n'; keepempty=false)
    io = IOBuffer()
    for (i, l) in enumerate(lines)
        i % 50 == 0 && println(io, "# comment line ", i)
        println(io, l)
    end
    take!(io)
end
bufdate2 = let io = IOBuffer()
    println(io, "d,n")
    i = 0
    while position(io) < MB * 2^20
        i += 1
        println(io, Dates.format(Date(2020, 1, 1) + Day(i % 3000), "mm/dd/yyyy"), ",", i)
    end
    take!(io)
end
const TMP = mktempdir()
pathmix = joinpath(TMP, "mixed.csv"); write(pathmix, bufmix)
pathnum = joinpath(TMP, "numeric.csv"); write(pathnum, bufnum)
gzpath = joinpath(TMP, "mixed.csv.gz"); write(gzpath, transcode(GzipCompressor, bufmix))
gzbytes = read(gzpath)
paths4 = [joinpath(TMP, "part$i.csv") for i in 1:4]; foreach(p -> write(p, bufmix), paths4)
smallwide = makedata(:wide, 1 << 20)

# ---------------------------------------------------------------------------
# File: keyword axes
# ---------------------------------------------------------------------------
F(buf; kw...) = CSV.File(buf; on_error=:collect, kw...)
B = length
runcase("file/numeric/default",            () -> F(bufnum), B(bufnum))
runcase("file/mixed/default",              () -> F(bufmix), B(bufmix))
runcase("file/strings/default",            () -> F(bufstr), B(bufstr))
runcase("file/temporal/default",           () -> F(buftemp), B(buftemp))
runcase("file/wide/default",               () -> F(bufwide), B(bufwide))
runcase("file/mixed/delim_explicit",       () -> F(bufmix; delim=','), B(bufmix))
runcase("file/mixed/select2",              () -> F(bufmix; select=[:id, :ratio]), B(bufmix))
runcase("file/mixed/select_regex",         () -> F(bufmix; select=r"^(id|ratio)$"), B(bufmix))
runcase("file/mixed/drop2",                () -> F(bufmix; drop=[:label, :when]), B(bufmix))
runcase("file/mixed/types_dict",           () -> F(bufmix; types=Dict(:id=>Int64, :value=>Int64, :ratio=>Float64)), B(bufmix))
runcase("file/mixed/types_vector",         () -> F(bufmix; types=[Int64, Int64, Float64, String, Date, Bool]), B(bufmix))
runcase("file/mixed/types_string",         () -> F(bufmix; types=String), B(bufmix))
runcase("file/mixed/types_narrow",         () -> F(bufmix; types=Dict(:id=>Int32, :value=>Int32, :ratio=>Float32)), B(bufmix))
runcase("file/mixed/types_narrow_overflow",() -> F(bufmix; types=Dict(:id=>Int32, :value=>Int16, :ratio=>Float32)), B(bufmix))
runcase("file/mixed/types_union_missing",  () -> F(bufmix; types=Dict(:id=>Union{Missing,Int64})), B(bufmix))
runcase("file/mixed/typemap_int_float",    () -> F(bufmix; typemap=Dict(Int64=>Float64)), B(bufmix))
runcase("file/mixed/downcast",             () -> F(bufmix; downcast=true), B(bufmix))
runcase("file/strings/stringtype_String",  () -> F(bufstr; stringtype=String), B(bufstr))
runcase("file/strings/stringtype_Inline",  () -> F(bufstr; stringtype=InlineString), B(bufstr))
runcase("file/strings/types_String31",     () -> F(bufstr; types=String31), B(bufstr))
runcase("file/strings/types_Symbol",       () -> F(bufstr; types=Symbol), B(bufstr))
runcase("file/pooled/pool_true",           () -> F(bufpool; pool=true), B(bufpool))
runcase("file/pooled/pool_ratio",          () -> F(bufpool; pool=0.2), B(bufpool))
runcase("file/pooled/pool_tuple",          () -> F(bufpool; pool=(0.2, 500)), B(bufpool))
runcase("file/pooled/pool_dict",           () -> F(bufpool; pool=Dict(:region=>true)), B(bufpool))
runcase("file/pooled/pool_true_String",    () -> F(bufpool; pool=true, stringtype=String), B(bufpool))
runcase("file/strings/pool_true_overcap",  () -> F(bufstr; pool=(0.2, 500)), B(bufstr))
runcase("file/sentinel/missingstring_NA",  () -> F(bufsent; missingstring="NA"), B(bufsent))
runcase("file/sentinel/missingstring_list",() -> F(bufsent; missingstring=["NA", "N/A", "null"]), B(bufsent))
runcase("file/date/dateformat_custom",     () -> F(bufdate2; dateformat="mm/dd/yyyy"), B(bufdate2))
runcase("file/date/dateformat_dict",       () -> F(bufdate2; dateformat=Dict(:d=>"mm/dd/yyyy")), B(bufdate2))
runcase("file/date/types_Date_iso",        () -> F(buftemp; types=Dict(:d=>Date, :dt=>DateTime, :t=>Time)), B(buftemp))
runcase("file/float/decimal_comma",        () -> F(bufdec; delim=';', decimal=','), B(bufdec))
runcase("file/groupmark",                  () -> F(bufgm; delim=';', groupmark=','), B(bufgm))
runcase("file/mixed/limit_half",           () -> F(bufmix; limit=nmix ÷ 2), B(bufmix) ÷ 2)
runcase("file/mixed/skipto_half",          () -> F(bufmix; skipto=nmix ÷ 2), B(bufmix) ÷ 2)
runcase("file/mixed/footerskip10",         () -> F(bufmix; footerskip=10), B(bufmix))
runcase("file/numeric/header_false",       () -> F(bufnohdr; header=false), B(bufnohdr))
runcase("file/numeric/header_names",       () -> F(bufnohdr; header=[:a, :b, :c, :d, :e, :f]), B(bufnohdr))
runcase("file/mixed/normalizenames",       () -> F(bufmix; normalizenames=true), B(bufmix))
runcase("file/irspace/ignorerepeated",     () -> F(bufir; delim=' ', ignorerepeated=true), B(bufir))
runcase("file/mixed/comment",              () -> F(bufcmt; comment="#"), B(bufcmt))
runcase("file/mixed/stripwhitespace",      () -> F(bufmix; stripwhitespace=true), B(bufmix))
runcase("file/numeric/quoted_false",       () -> F(bufnum; quoted=false), B(bufnum))
runcase("file/escaped/default",            () -> F(bufesc), B(bufesc))
runcase("file/escaped/escapechar_backslash",() -> F(bufesc2; escapechar='\\'), B(bufesc2))
runcase("file/strings/open_close_quote",   () -> F(bufbracket; openquotechar='[', closequotechar=']'), B(bufbracket))
runcase("file/numeric/delim_multibyte",    () -> F(bufdd; delim="::"), B(bufdd))
runcase("file/crlf/default",               () -> F(bufcrlf), B(bufcrlf))
runcase("file/dirty/collect",              () -> F(bufdirty), B(bufdirty))
runcase("file/dirty/maxproblems0",         () -> F(bufdirty; maxproblems=0), B(bufdirty))
runcase("file/dirty/maxproblems_1e6",      () -> F(bufdirty; maxproblems=1_000_000), B(bufdirty))
runcase("file/mixed/ntasks1",              () -> F(bufmix; ntasks=1), B(bufmix))
runcase("file/mixed/ntasks2",              () -> F(bufmix; ntasks=2), B(bufmix))
runcase("file/mixed/ntasks4",              () -> F(bufmix; ntasks=4), B(bufmix))
runcase("file/mixed/parallel_false",       () -> F(bufmix; parallel=false), B(bufmix))
runcase("file/mixed/chunkbytes_64k",       () -> F(bufmix; chunkbytes=1 << 16), B(bufmix))
runcase("file/mixed/chunkbytes_8m",        () -> F(bufmix; chunkbytes=1 << 23), B(bufmix))
runcase("file/mixed/fastindex_false",      () -> F(bufmix; fastindex=false), B(bufmix))
runcase("file/mixed/nsample_1024",         () -> F(bufmix; nsample=1024), B(bufmix))
# source kinds
runcase("file/source/path_mmap",           () -> F(pathmix), B(bufmix))
runcase("file/source/path_inmemory",       () -> F(pathmix; buffer_in_memory=true), B(bufmix))
runcase("file/source/iobuffer",            () -> F(IOBuffer(bufmix)), B(bufmix))
runcase("file/source/iostream",            () -> open(io -> F(io), pathmix), B(bufmix))
runcase("file/source/gzip_path",           () -> F(gzpath), B(bufmix))
runcase("file/source/gzip_bytes",          () -> F(gzbytes), B(bufmix))
runcase("file/source/cmd_cat",             () -> F(`cat $pathmix`), B(bufmix))
runcase("file/source/multi4",              () -> F(paths4), 4B(bufmix))
runcase("file/source/multi4_source",       () -> F(paths4; source=:src), 4B(bufmix))
runcase("file/wide/transpose_1mib",        () -> F(smallwide; transpose=true), B(smallwide))
# scan pushdown
if isdefined(Tables, :Scan)
    T = Tables
    runcase("file/scan/select2",           () -> F(bufmix; scan=T.Scan(select=(:id, :ratio))), B(bufmix))
    runcase("file/scan/filter_half",       () -> F(bufmix; scan=T.Scan(filter=T.col(:id) > nmix ÷ 2)), B(bufmix))
    runcase("file/scan/filter_select",     () -> F(bufmix; scan=T.Scan(select=(:id, :label), filter=T.colcmp(==, T.col(:flag), true))), B(bufmix))
    runcase("file/scan/limit_offset",      () -> F(bufmix; scan=T.Scan(offset=1000, limit=nmix ÷ 2)), B(bufmix) ÷ 2)
end
runcase("read/mixed/columntable",          () -> CSV.read(bufmix, Tables.columntable; on_error=:collect), B(bufmix))
# lazy
lf = CSV.lazy(bufmix; types=Dict(:ratio=>Float64))
rng = MersenneTwister(1); idx = rand(rng, 1:nmix, 200_000)
runcase("lazy/build",                      () -> CSV.lazy(bufmix), B(bufmix))
runcase("lazy/collect_float_col",          () -> collect(lf.ratio), B(bufmix))
runcase("lazy/collect_string_col",         () -> collect(lf.label), B(bufmix))
runcase("lazy/random_200k",                () -> (s = 0.0; for i in idx; v = lf[i, :ratio]; v === missing || (s += v); end; s), 200_000 * 16)
runcase("lazy/to_file",                    () -> CSV.File(lf; on_error=:collect), B(bufmix))
# rows
function rowsum(itr)
    s = 0.0; n = 0
    for row in itr
        v = row[3]; v === missing || (s += v isa Number ? v : ncodeunits(v))
        l = row[4]; l === missing || (n += ncodeunits(l))
    end
    return s + n
end
runcase("rows/mixed/untyped",              () -> rowsum(CSV.Rows(bufmix)), B(bufmix))
runcase("rows/mixed/typed",                () -> rowsum(CSV.Rows(bufmix; types=Dict(:id=>Int64, :value=>Int64, :ratio=>Float64))), B(bufmix))
runcase("rows/mixed/typed_all",            () -> rowsum(CSV.Rows(bufmix; types=[Int64, Int64, Float64, String, Date, Bool])), B(bufmix))
runcase("rows/mixed/select",               () -> rowsum(CSV.Rows(bufmix; select=[:id, :value, :ratio, :label])), B(bufmix))
runcase("rows/mixed/stringtype_String",    () -> rowsum(CSV.Rows(bufmix; stringtype=String)), B(bufmix))
runcase("rows/mixed/reusebuffer",          () -> rowsum(CSV.Rows(bufmix; reusebuffer=true)), B(bufmix))
runcase("rows/mixed/strict",               () -> rowsum(CSV.Rows(bufmix; on_error=:error)), B(bufmix))
runcase("rows/mixed/construct_only",       () -> CSV.Rows(bufmix), B(bufmix))
runcase("rows/numeric/parse_getproperty",  () -> (s = 0.0; for r in CSV.Rows(bufnum; types=Dict(:c=>Float64)); s += r.c; end; s), B(bufnum))
# chunks
chunkcount(c) = (n = 0; for f in c; n += Tables.rowcount(f); end; n)
runcase("chunks/mixed/default",            () -> chunkcount(CSV.Chunks(bufmix; on_error=:collect)), B(bufmix))
runcase("chunks/mixed/construct_only",     () -> CSV.Chunks(bufmix; on_error=:collect), B(bufmix))
runcase("chunks/mixed/ntasks1",            () -> chunkcount(CSV.Chunks(bufmix; ntasks=1, on_error=:collect)), B(bufmix))
runcase("chunks/pooled/pool_true",         () -> chunkcount(CSV.Chunks(bufpool; pool=true, on_error=:collect)), B(bufpool))
runcase("chunks/strings/stringtype_String",() -> chunkcount(CSV.Chunks(bufstr; stringtype=String, on_error=:collect)), B(bufstr))
runcase("chunks/strings/stringtype_Inline",() -> chunkcount(CSV.Chunks(bufstr; stringtype=InlineString, on_error=:collect)), B(bufstr))
runcase("chunks/mixed/select_types",       () -> chunkcount(CSV.Chunks(bufmix; select=[:id, :ratio], types=Dict(:id=>Int32), on_error=:collect)), B(bufmix))

# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------
wnum = shape(:numeric, NROWS_W, MersenneTwister(7))
wmix = shape(:mixed, NROWS_W, MersenneTwister(7))
wstr = shape(:strings, NROWS_W, MersenneTwister(7))
wquo = shape(:quoted, NROWS_W, MersenneTwister(7))
wdt  = shape(:datetime, NROWS_W, MersenneTwister(7))
wwide = shape(:wide, NROWS_W ÷ 10, MersenneTwister(7))
wbytes(t) = (io = IOBuffer(); CSV.write(io, t); io.size)
bnum, bmix, bstr, bquo, bdt, bwide = wbytes.((wnum, wmix, wstr, wquo, wdt, wwide))
fmix = CSV.File(bufmix)                                     # abstract columns incl. DataString
fpool = CSV.File(bufpool; pool=true)
wds = (a=fmix.id, label=fmix.label, ratio=fmix.ratio)      # DataStringVector column
wpooled = (region=fpool.region, status=fpool.status, qty=fpool.qty)
winl = (s1=String15.(wstr.s1), s2=String15.(wstr.s2), n=wnum.e)
wi32 = (a=Int32.(wnum.c), b=Int16.(wnum.e .% 1000), c=UInt8.(wnum.e .% 200))
wmiss = (a=[i % 2 == 0 ? missing : Float64(i) for i in 1:NROWS_W], b=[i % 3 == 0 ? missing : i for i in 1:NROWS_W],
         c=[i % 5 == 0 ? missing : "v$i" for i in 1:NROWS_W])
wrows = Tables.rowtable(shape(:mixed, NROWS_W ÷ 10, MersenneTwister(7)))
bds, bpooled, binl, bi32, bmiss, brows = wbytes.((wds, wpooled, winl, wi32, wmiss, wrows))
wpath = joinpath(TMP, "out.csv")
W(tbl; kw...) = CSV.write(IOBuffer(), tbl; kw...)
runcase("write/numeric/iobuffer",          () -> W(wnum), bnum)
runcase("write/numeric/ntasks1",           () -> W(wnum; ntasks=1), bnum)
runcase("write/numeric/ntasks4",           () -> W(wnum; ntasks=4), bnum)
runcase("write/numeric/path",              () -> CSV.write(wpath, wnum), bnum)
runcase("write/numeric/devnull",           () -> CSV.write(devnull, wnum), bnum)
runcase("write/mixed/iobuffer",            () -> W(wmix), bmix)
runcase("write/mixed/ntasks1",             () -> W(wmix; ntasks=1), bmix)
runcase("write/strings/iobuffer",          () -> W(wstr), bstr)
runcase("write/quoted/iobuffer",           () -> W(wquo), bquo)
runcase("write/datetime/iobuffer",         () -> W(wdt), bdt)
runcase("write/wide/iobuffer",             () -> W(wwide), bwide)
runcase("write/file_table",                () -> W(fmix), B(bufmix))
runcase("write/datastring_cols",           () -> W(wds), bds)
runcase("write/pooled_cols",               () -> W(wpooled), bpooled)
runcase("write/inlinestring_cols",         () -> W(winl), binl)
runcase("write/narrow_int_cols",           () -> W(wi32), bi32)
runcase("write/missing_heavy",             () -> W(wmiss), bmiss)
runcase("write/mixed/quotestyle_all",      () -> W(wmix; quotestyle=:all), bmix)
runcase("write/mixed/quotestyle_none_safe",() -> W(wnum; quotestyle=:none), bnum)
runcase("write/numeric/floatformat",       () -> W(wnum; floatformat="%.3f"), bnum)
runcase("write/datetime/dateformat",       () -> W(wdt; dateformat="yyyy/mm/dd"), bdt)
runcase("write/mixed/transform_identity",  () -> W(wmix; transform=(c, v) -> v), bmix)
runcase("write/mixed/compress_gzip",       () -> W(wmix; compress=true), bmix)
runcase("write/numeric/delim_semicolon",   () -> W(wnum; delim=';'), bnum)
runcase("write/numeric/delim_multibyte",   () -> W(wnum; delim="::"), bnum)
runcase("write/numeric/decimal_comma",     () -> W(wnum; delim=';', decimal=','), bnum)
runcase("write/mixed/header_false",        () -> W(wmix; header=false), bmix)
runcase("write/mixed/append",              () -> W(wmix; append=true), bmix)
runcase("write/mixed/missingstring_NA",    () -> W(wmiss; missingstring="NA"), bmiss)
runcase("write/mixed/newline_crlf",        () -> W(wmix; newline="\r\n"), bmix)
runcase("write/rows_source_100k",          () -> W(wrows), brows)
runcase("write/rowwriter_join_100k",       () -> join(CSV.RowWriter(wrows)), brows)
runcase("write/chunks_stream",             () -> CSV.write(IOBuffer(), CSV.Chunks(bufmix; on_error=:collect)), B(bufmix))
parts = [shape(:numeric, NROWS_W ÷ 4, MersenneTwister(i)) for i in 1:4]
runcase("write/partition4",                () -> CSV.write([IOBuffer() for _ in 1:4], Tables.partitioner(parts); partition=true), bnum)
close(OUT)
rm(TMP; recursive=true, force=true)
isempty(FAILURES) || error("Benchmark cases failed: ", join(FAILURES, ", "))
