# Regression tests for CSV readers.
#
# Run:  julia --startup-file=no --project=test -t4 test/api.jl
#
# Strategy: behavior is pinned with explicit values and with source-mode
# equivalence between IOBuffer and byte-vector inputs. String containers and
# pooling wrappers are normalized only where their representation is not the
# contract. Intentional 1.0 behavior is asserted directly:
#   • empty unquoted cells are ALWAYS missing (custom missingstring ADDS)
#   • long rows do not widen the schema (extra fields ⇒ problem, not Column4)
#   • diagnostics are retained data, with one summary warning by default
#   • function-typed select/drop retired
#   • wide integers that fit Int128 remain exact

using Test, Dates, Tables, PooledArrays, CodecZlib, InlineStrings, FilePathsBase, Random, Mmap, Parsers
using Durations: Timestamp
using CSV
const A = CSV
const K = CSV

# Minimal ordered AbstractDict for precedence tests. Base.Dict iteration order
# is not an API contract, while CSV's rule is explicitly first matching Regex.
struct OrderedTestDict <: AbstractDict{Any, Any}
    entries::Vector{Pair{Any, Any}}
end
Base.length(d::OrderedTestDict) = length(d.entries)
Base.iterate(d::OrderedTestDict, state::Int=1) =
    state > length(d.entries) ? nothing : (d.entries[state], state + 1)

struct APICustomScalar
    value::Int
end
Base.tryparse(::Type{APICustomScalar}, s::String) =
    (x = tryparse(Int, s); x === nothing ? nothing : APICustomScalar(x))

struct APITaskScalar
    value::Int
end
const API_TASK_LOCK = ReentrantLock()
const API_PARSE_TASKS = Set{UInt}()
function Base.tryparse(::Type{APITaskScalar}, s::String)
    lock(API_TASK_LOCK) do
        push!(API_PARSE_TASKS, objectid(current_task()))
    end
    x = tryparse(Int, s)
    return x === nothing ? nothing : APITaskScalar(x)
end

# A type that parses its field bytes in place through Parsers.
struct APISpanScalar
    value::Int
end
Parsers.tryparse(::Type{APISpanScalar}, buf::AbstractVector{UInt8}, i::Integer, j::Integer) =
    (x = Parsers.tryparse(Int, buf, i, j); x === nothing ? nothing : APISpanScalar(x))

# A type with only `Base.parse`: rejected before parsing starts.
struct APIParseOnly
    value::Int
end
Base.parse(::Type{APIParseOnly}, s::String) = APIParseOnly(parse(Int, s))

# A `tryparse` that throws aborts the read instead of hiding the cell.
struct APIThrowingScalar
    value::Int
end
Base.tryparse(::Type{APIThrowingScalar}, s::String) = error("custom parser failure")

# Keep the allocation probe in compiled function scope. Julia 1.10 can box the
# UInt result when `@allocated` appears directly in a testset.
function hashall(c)
    h = UInt(0)
    @inbounds for i in eachindex(c)
        h ⊻= hash(c[i])
    end
    return h
end
allochashall(c) = @allocated hashall(c)

# Mmap attached its finalizer directly to Array through Julia 1.10. Julia
# 1.11's GenericMemory-backed Array attaches it to the memory owner instead.
# Finalize that exact owner so Windows can remove test files deterministically.
function finalizemapping!(mapped::Vector{UInt8})
    if hasfield(typeof(mapped), :ref)
        ref = getfield(mapped, :ref)
        finalize(getfield(ref, :mem))
    else
        finalize(mapped)
    end
    return nothing
end

@testset "Scan reader uses the Tables API" begin
    let
        @test Base.names(A.File(IOBuffer("a,b\n1,2\n"); scan=Tables.Scan(select=(:b,)))) == [:b]
        narrowsrc = "a,b\n1,x\n128,y\n2,z\n"
        excluded = Tables.Scan(select=(:a => Int8,), filter=Tables.col(:a) < 100)
        pushed = A.File(IOBuffer(narrowsrc); scan=excluded, pool=false, on_error=:collect)
        generic = Tables.scan(A.File(IOBuffer(narrowsrc); pool=false), excluded)
        @test pushed.a == generic.a == Int8[1, 2]
        @test isempty(A.problems(pushed))

        retained = Tables.Scan(select=(:a => Int8,), filter=Tables.col(:a) > 100)
        pushed = A.File(IOBuffer(narrowsrc); scan=retained, pool=false, on_error=:collect)
        @test isequal(pushed.a, Union{Missing, Int8}[missing])
        @test only(A.problems(pushed)).row == 2
        @test only(A.problems(pushed)).pos == first(findfirst("128", narrowsrc))
        @test_throws CSV.ParseError A.File(IOBuffer(narrowsrc); scan=retained,
                                           strict=true, maxproblems=0, pool=false)
        floatscan = Tables.Scan(select=(:a => Float32,))
        floatfile = A.File(IOBuffer("a\n1.5\n"); scan=floatscan, pool=false)
        @test floatfile.a == Float32[1.5] && eltype(floatfile.a) == Float32
    end
end

_norm(x) = x isa AbstractString ? String(x) : x

function colvalues(f)
    names = collect(Symbol, Tables.columnnames(Tables.columns(f)))
    return names, [Any[_norm(x) for x in Tables.getcolumn(Tables.columns(f), nm)] for nm in names]
end

# Exercise both in-memory source-resolution paths with the same public options.
function sourceparity(input; kw=NamedTuple())
    fromio = A.File(IOBuffer(input); on_error=:collect, kw...)
    frombytes = A.File(Vector{UInt8}(codeunits(input)); on_error=:collect, kw...)
    nio, vio = colvalues(fromio)
    nbytes, vbytes = colvalues(frombytes)
    @test nio == nbytes
    @test isequal(vio, vbytes)
    return fromio
end

@testset "CSV readers" begin

@testset "values, inference, and source modes" begin
    ints = sourceparity("a,b,c\n1,2,3\n4,5,6\n")
    @test (ints.a, ints.b, ints.c) == ([1, 4], [2, 5], [3, 6])
    floats = sourceparity("a,b\n1.5,2\n-3.25e2,4\n")
    @test floats.a == [1.5, -325.0] && floats.b == [2, 4]
    bools = sourceparity("x\ntrue\nfalse\n")
    @test bools.x == [true, false]
    temporal = sourceparity("d,t,dt\n2024-01-02,01:02:03,2024-01-02T01:02:03\n")
    @test temporal.d == [Date(2024, 1, 2)]
    @test temporal.t == [Time(1, 2, 3)]
    @test temporal.dt == [DateTime(2024, 1, 2, 1, 2, 3)]
    @test eltype(temporal.dt) === Timestamp{Nanosecond}
    sourceparity("s\nhello\nworld\n")
    sourceparity("m,x\n,1\n,2\n")                       # all-missing column
    sourceparity("p\n1\n2.5\n")                         # int → float promotion
    sourceparity("p\n1\nx\n")                           # int → string promotion
    big = sourceparity("p\n1\n99999999999999999999999999\n"; kw=(; pool=false))
    @test eltype(big.p) === Int128
    @test collect(big.p) == Int128[1, 99999999999999999999999999]
    wideinput = "p\n99999999999999999999999999\n"
    wide = A.File(IOBuffer(wideinput); pool=false)
    @test eltype(wide.p) === Int128
    @test wide.p[1] == Int128(99999999999999999999999999)
    sourceparity("p\n9999999999999999999999999999999999999999\n"; kw=(; pool=false))
    sourceparity("q\n\"a,b\"\n\"c\nd\"\n\"e\"\"f\"\n")  # quoted delim/newline/escape
    sourceparity("u\nα\n∀\n")                           # unicode passthrough
    sourceparity("neg\n-1\n+2\n")
    sourceparity("sci\n1e3\n-2.5E-2\n")
end

@testset "dialects agree" begin
    sourceparity("a;b\n1;2\n"; kw=(; delim=';'))
    sourceparity("a\tb\n1\t2\n"; kw=(; delim='\t'))
    sourceparity("a|b\n1|2\n"; kw=(; delim='|'))
    sourceparity("a,b\n'x,y',2\n"; kw=(; quotechar='\''))
    sourceparity("a,b\n\"x\\\"y\",2\n"; kw=(; escapechar='\\'))
    sourceparity("a,b\n[x,y],2\n"; kw=(; openquotechar='[', closequotechar=']'))
    sourceparity("a,b\n#c\n1,2\n#d\n3,4\n"; kw=(; comment="#"))
    sourceparity("a,b\n\n1,2\n\n\n3,4\n")                              # empty rows dropped
    sourceparity("a,b\n\n1,2\n"; kw=(; ignoreemptyrows=false))
    sourceparity("a b\n1  2\n 3 4 \n"; kw=(; delim=' ', ignorerepeated=true))
    sourceparity("a::b\n1::2\n"; kw=(; delim="::"))
    # multi-byte delim + ignorerepeated
    sourceparity("a::b::::c\n1::2::3\n"; kw=(; delim="::", ignorerepeated=true))
end

@testset "automatic delimiter detection" begin
    detected(source; kw...) = A._prepare(source; kw...).d.delim
    for (d, s) in ((',', "a,b\n1,2\n3,4\n"), (';', "a;b\n1;2\n3;4\n"),
                   ('\t', "a\tb\n1\t2\n3\t4\n"), ('|', "a|b\n1|2\n3|4\n"))
        sourceparity(s)
        @test detected(IOBuffer(s)) == UInt8(d)
        @test Base.names(A.File(IOBuffer(s))) == [:a, :b]
    end
    # Delimiters in quoted cells cannot influence the detector.
    @test detected(IOBuffer("a;b\n\"1;2;3;4;5\";6\n\"7;8\";9\n")) == UInt8(';')
    sourceparity("t\n12:34:56\n13:45:00\n")
    @test detected(IOBuffer("t\n12:34:56\n13:45:00\n")) == UInt8(',')
    f = A.File(IOBuffer("a;b\n1,5;2\n3,5;4\n"); decimal=',', fastindex=false)
    @test f.a == [1.5, 3.5] && f.b == [2, 4]
    f = A.File(IOBuffer("a;b\n\"x\ny\";1\nz;2\n"); samplebytes=12)
    @test f.a == ["x\ny", "z"] && f.b == [1, 2]
    # A bounded sample retains complete rows. It grows when its first row does
    # not fit, including when that row ends at EOF.
    normal = Vector{UInt8}("a,b\n1,2\n")
    @test A._sample(normal, 6, 1, A.Dialect()) == normal[1:4]
    @test A._sample(normal, 1, 1, A.Dialect()) == normal[1:4]
    single = Vector{UInt8}("a;b;c")
    @test A._sample(single, 1, 1, A.Dialect()) == single
    bom = vcat(UInt8[0xef, 0xbb, 0xbf], Vector{UInt8}("a;b\n1;2\n"))
    @test detected(bom; samplebytes=1) == UInt8(';')
    @test detected(IOBuffer("Created Date\n")) == UInt8(',')
    @test detected(IOBuffer("a;b;c\n")) == UInt8(';')
    @test detected(IOBuffer("")) == UInt8(',')
    @test detected(bom) == UInt8(';')
    # Two rows establish consistency. Equal scores retain candidate order.
    @test detected(IOBuffer("x y:a:p,q:p,q:p,q\r\n\"p:q\":b:c:d:x y")) == UInt8(':')
    @test detected(IOBuffer("a,b;c\n1,2;3\n")) == UInt8(',')
    # Header punctuation takes precedence over delimiters found only in data.
    @test detected(IOBuffer("header, text\n1:2\n3:4\n")) == UInt8(',')
    @test detected(IOBuffer("header text\n1:2\n3:4\n")) == UInt8(':')
    @test_throws ArgumentError A.File(IOBuffer("a b\n1  2\n"); ignorerepeated=true)
end

@testset "headers and source modes" begin
    sourceparity("1,2\n3,4\n"; kw=(; header=false))
    sourceparity("junk\na,b\n1,2\n"; kw=(; header=2))
    sourceparity("h1,h2\nx,y\n1,2\n"; kw=(; header=[1, 2]))
    sourceparity("h1,\nx,y\n1,2\n"; kw=(; header=[1, 2]))          # blank part → ColumnN_y
    # A comment between merged name parts is skipped while the raw row after
    # `last(header)` starts the data.
    sourceparity("a,b\n#middle\nx,y\n1,2\n"; kw=(; header=[1, 2], comment="#"))
    sourceparity("a,b\n"; kw=(; header=[1, 2]))                    # partial header at EOF
    sourceparity("1,2\n3,4\n"; kw=(; header=["l", "r"]))
    sourceparity("1,2\n3,4\n"; kw=(; header=[:l, :r]))
    sourceparity("my col,2x,for,,my col\n1,2,3,4,5\n"; kw=(; normalizenames=true))
    sourceparity("a,a,a_1\n1,2,3\n")                               # makeunique
    sourceparity("a,b\n")                                          # only a header
    # header row consumed even when it is the only content in early chunks
    f = A.File(IOBuffer("a,b\n1,2\n"); chunkbytes=4)
    @test collect(f.a) == [1]
    # Non-consecutive header rows join rows 1 and 3 while skipping row 2.
    sourceparity("a,b\nx,y\n1,2\n3,4\n"; kw=(; header=[1, 3]))
    @test_throws ArgumentError A.File(IOBuffer("a,b\nx,y\n1,2\n"); header=[3, 1])
end

@testset "row windowing and source modes (raw-row semantics)" begin
    sourceparity("a,b\n1,2\n3,4\n5,6\n"; kw=(; limit=2))
    sourceparity("a,b\n1,2\n3,4\n5,6\n"; kw=(; footerskip=2))
    sourceparity("a\n1\n\n2\n\n3\n"; kw=(; footerskip=2))       # empty rows COUNT
    sourceparity("a\n1\n\n2\n\n3\n"; kw=(; footerskip=2, ignoreemptyrows=false))
    sourceparity("a\n1\n#x\n2\n#y\n3\n"; kw=(; comment="#", footerskip=2))
    sourceparity("a,b\n\"x\ny\",1\nz,2\n"; kw=(; footerskip=1))
    sourceparity("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3))
    sourceparity("a,b\n1,2\n3,4\n5,6\n7,8\n"; kw=(; skipto=3, limit=1))
    sourceparity("a,b\n1,2\n3,4\n5,6\n"; kw=(; skipto=3, footerskip=1))
    sourceparity("a,b\n#skip\n1,2\n3,4\n"; kw=(; comment="#", skipto=3))   # comments COUNT
    sourceparity("junk\nmore junk\na,b\n1,2\n"; kw=(; header=3))
    sourceparity("a,b\n1,2\n"; kw=(; limit=0))
    sourceparity("a\n1\n2\n"; kw=(; limit=0, footerskip=1))
    fa0 = A.File(IOBuffer("a\n1\n2\n"); limit=0, footerskip=1)
    @test Tables.schema(fa0).types == (Missing,)
    sourceparity("﻿junk\n#ignore\na,b\n1,2\n3,4\n";
            kw=(; header=3, comment="#", skipto=5))
    sourceparity("a,b\n1,2\n"; kw=(; skipto=100))
    sourceparity("a,b\n1,2\n"; kw=(; header=100))
    # Integer row options are source positions, not machine-size allocations.
    # A BigInt beyond typemax(Int) models UInt32 row options on 32-bit Julia.
    huge = big(typemax(Int)) + 1
    bounded = "a,b\n1,2\n3,4\n"
    unlimited = A.File(IOBuffer(bounded); limit=huge)
    @test unlimited.a == [1, 3] && unlimited.b == [2, 4]
    @test isempty(A.File(IOBuffer(bounded); skipto=huge))
    @test isempty(A.File(IOBuffer(bounded); header=huge))
    @test isempty(A.File(IOBuffer(bounded); header=[huge, huge + 1]))
    @test isempty(A.File(IOBuffer(bounded); footerskip=huge))
    # Every non-transposed reader uses the same bounded preparation.
    @test isempty(collect(A.Rows(IOBuffer(bounded); skipto=huge)))
    @test isempty(collect(A.Chunks(IOBuffer(bounded); footerskip=huge)))
    @test length(A.lazy(IOBuffer(bounded); header=huge)) == 0
    # Default header 1 + skipto=1 means "no header, data at row 1".
    sourceparity("a,b\n1,2\n"; kw=(; skipto=1))
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); header=2, skipto=1)
    @test_throws ArgumentError A.File(IOBuffer("a,b\n1,2\n"); limit=-1)
    @test A.File(IOBuffer("a,b\n1,2\n"); footerskip=5).table.nrows == 0
    # Comment rows count for header/skipto positions but not footerskip. Quoted
    # physical lines that start with '#' remain one data row.
    commentheavy = "#lead\r\nh1,h2\r\n#between \" poison\r\n1,2\r\n\"top\r\n# content\r\nbottom\",3\r\n#tail"
    f = A.File(IOBuffer(commentheavy); comment="#", header=2, skipto=4,
               footerskip=1, delim=',')
    @test Base.names(f) == [:h1, :h2]
    @test f.h1 == [1]

    limitedtype = A.File(IOBuffer("a\n1\nx\n"); limit=1)
    @test limitedtype.a isa Vector{Int64}
    @test limitedtype.a == [1]

    # A blank or comment row may sit between explicitly listed header rows.
    sourceparity("a,b\n\nA,B\n1,2\n"; kw=(; header=[1, 2]))
    sourceparity("a,b\n# gap\nA,B\n1,2\n"; kw=(; header=[1, 2], comment="#"))
end

@testset "missingstring behavior and source modes" begin
    f = sourceparity("a,b\nNA,1\n2,NA\n"; kw=(; missingstring="NA"))
    @test isequal(f.a, [missing, 2]) && isequal(f.b, [1, missing])
    f = sourceparity("a,b\nNA,N/A\nx,2\n";
                     kw=(; missingstring=["NA", "N/A"]))
    @test isequal(_norm.(f.a), [missing, "x"]) && isequal(f.b, [missing, 2])
    f = sourceparity("a\n999\n1\n"; kw=(; missingstring="999"))
    @test isequal(f.a, [missing, 1])
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); missingstring="N\"A")
    # Empty unquoted cells remain missing even with a custom missing sentinel.
    fa = A.File(IOBuffer("a\n\nx\n"); missingstring="NA", ignoreemptyrows=false)
    @test isequal(collect(fa.a), [missing, "x"])
end

@testset "one column plan per read" begin
    opts = K.makevalueopts(K.Dialect())
    colopts = fill(opts, 3)
    plan = K.settlecolumns([:a_b, :b, :c], opts;
                           select=[:c, "a b", :c],
                           types=Dict(r"^[ab]" => Float32,
                                      :a_b => Union{Missing, Int8}),
                           colopts, matchnormalized=true)
    @test plan.sources == [1, 3]
    @test plan.positions == [1, 3]
    @test plan.columns[1].parsetype === Int64
    @test plan.columns[1].resulttype === Int8
    @test plan.columns[1].declaredmissing
    @test plan.columns[2].parsetype === Float64
    @test plan.columns[2].resulttype === Float32
    @test plan.columns[3].parsetype === nothing
    @test K.columnopts(plan, 2) === colopts[2]

    visible = K.settlecolumns([:a, :b, :c], opts;
                              available=[1, 3], select=[:c],
                              types=[Int8, Float32])
    @test visible.sources == [3]
    @test visible.positions == [2]
    @test visible.columns[1].resulttype === Int8
    @test visible.columns[2].parsetype === nothing
    @test visible.columns[3].resulttype === Float32

    @test_throws ArgumentError K.settlecolumns([:a], opts;
                                               select=[:a], drop=[:a])
    @test_throws ArgumentError K.settlecolumns([:a], opts; select=[:missing])
    @test K.settlecolumns([:a], opts; types=Dict(:missing => Int64),
                          validate=false).columns[1].parsetype === nothing
end

@testset "types and source modes" begin
    sourceparity("a,b\n1,2\n"; kw=(; types=Dict(:a => Float64)))
    sourceparity("a,b\n1,2\n"; kw=(; types=Dict(1 => Float64)))
    sourceparity("a,b\n1,2\n"; kw=(; types=[Float64, String]))
    sourceparity("a,b\n1,2\n"; kw=(; types=String))
    sourceparity("a,b\n1,2\n,3\n"; kw=(; types=Dict(:a => Union{Int64, Missing})))
    sourceparity("a\n1\nbad\n2\n"; kw=(; types=Int64))              # invalid → missing + diagnostic
    f = A.File(IOBuffer("a\n1\nbad\n"); types=Int64, on_error=:collect)
    @test any(p -> p.kind == :invalid_value, A.problems(f))
    @test_throws Exception A.File(IOBuffer("a\n1\nbad\n"); types=Int64, strict=true)

    # Narrow conversion is an API step. Requests remain indexed by
    # file column while selected output columns remain in file order.
    selected = A.File(IOBuffer("a,b,c\n1,2,300\n3,4,500\n");
                      types=Dict(:a => Int8, :c => Int16), select=[:c, :a])
    @test Base.names(selected) == [:a, :c]
    @test selected.a == Int8[1, 3]
    @test selected.c == Int16[300, 500]
    duplicate = A.File(IOBuffer("a,b,c\n1,2,300\n");
                       types=Dict(:a => Int8, :c => Int16), select=[3, 1, 3])
    @test Base.names(duplicate) == [:a, :c]

    overflow = A.File(IOBuffer("a\n127\n128\n"); types=Int8, on_error=:collect)
    @test isequal(collect(overflow.a), [Int8(127), missing])
    @test [(p.row, p.col, p.kind) for p in A.problems(overflow)] ==
          [(2, 1, :invalid_value)]
    manyoverflows = A.File(IOBuffer("a\n" * "128\n"^20);
                           types=Int8, maxproblems=1, on_error=:collect)
    @test length(A.problems(manyoverflows)) == 1
    @test getfield(manyoverflows, :table).droppedproblems == 19
    @test_throws CSV.ParseError A.File(IOBuffer("a\n128\n");
                                       types=Int8, strict=true, maxproblems=0)
    # Parse failures and post-parse narrow failures share source ordering. The
    # retained/strict problem must be whichever field occurs first in the bytes,
    # regardless of which phase reported it.
    mixedproblems = [
        ("a\nBAD\n128\n", "BAD", "cannot parse Int64"),
        ("a\n128\nBAD\n", "128", "does not fit Int8"),
    ]
    for (mixed, firstfield, firstmessage) in mixedproblems
        f = A.File(IOBuffer(mixed); types=Int8, maxproblems=1, on_error=:collect)
        pr = only(A.problems(f))
        @test (pr.row, pr.col, pr.pos) ==
              (1, 1, first(findfirst(firstfield, mixed)))
        @test occursin(firstmessage, pr.message)
        @test getfield(f, :table).droppedproblems == 1
        err = try
            A.File(IOBuffer(mixed); types=Int8, strict=true, maxproblems=0)
            nothing
        catch ex
            ex
        end
        @test err isa CSV.ParseError
        @test occursin(firstmessage, sprint(showerror, err))
    end
    unsigned = A.File(IOBuffer("u\n$(typemax(UInt64))\n"); types=UInt64)
    @test unsigned.u == UInt64[typemax(UInt64)]
    for T in (Float16, Float32)
        floatsrc = "x\n1e100\n-1e100\n"
        narrowed = A.File(IOBuffer(floatsrc); types=T)
        @test collect(narrowed.x) == T[Inf, -Inf]
        @test isempty(A.problems(narrowed))
    end
    declarednarrow = A.File(IOBuffer("a\n1\n2\n"); types=Union{Missing, Int8})
    @test eltype(declarednarrow.a) == Union{Missing, Int8}
    declaredtranspose = A.File(IOBuffer("a,1,2\n"); transpose=true,
                               types=Union{Missing, Int8})
    @test eltype(declaredtranspose.a) == Union{Missing, Int8}
    declaredchunk = first(A.Chunks(IOBuffer("a\n1\n2\n");
                                   types=Union{Missing, Int8}, pool=false))
    @test eltype(declaredchunk.a) == Union{Missing, Int8}
    # a requested String is the output type; DataString keeps the view column
    declaredstring = A.File(IOBuffer("a\nx\ny\n"); types=Union{Missing, String},
                            pool=false)
    @test eltype(declaredstring.a) == Union{Missing, String}
    declaredview = A.File(IOBuffer("a\nx\ny\n"); types=Union{Missing, K.DataString},
                          pool=false)
    @test eltype(declaredview.a) == Union{Missing, K.DataString}
    declaredpoolinput = "a\n" * join(fill("x", 40), '\n') * "\n"
    declaredpool = A.File(IOBuffer(declaredpoolinput);
                          types=Union{Missing, String}, pool=true)
    @test declaredpool.a isa PooledArrays.PooledArray
    @test eltype(declaredpool.a) == Union{Missing, String}

    # Exact keys beat Regex keys. Among Regex keys, the first matching entry in
    # the AbstractDict wins for types, dateformat, and pool.
    typemap = OrderedTestDict(Any[r"_col$" => Int16, r"^a" => Int32,
                                  :a_col => Int8])
    regexfile = A.File(IOBuffer("a_col,b_col,c\n1,2,3\n"); types=typemap)
    @test eltype(regexfile.a_col) == Int8
    @test eltype(regexfile.b_col) == Int16
    firstregex = OrderedTestDict(Any[r"^a" => Int16, r"_col$" => Int32])
    @test eltype(A.File(IOBuffer("a_col\n1\n"); types=firstregex).a_col) == Int16

    dateformats = OrderedTestDict(Any[r"^date" => "dd/mm/yyyy",
                                      r"1$" => "mm/dd/yyyy"])
    dates = A.File(IOBuffer("date1,date2\n03/04/2020,05/06/2021\n");
                   dateformat=dateformats)
    @test dates.date1 == [Date(2020, 4, 3)]
    pools = OrderedTestDict(Any[r"^a" => true, r"_col$" => false])
    pooledinput = "a_col,b_col\n" * join(fill("x,y", 40), '\n') * "\n"
    pooled = A.File(IOBuffer(pooledinput); pool=pools)
    @test pooled.a_col isa PooledArrays.PooledArray
    @test !(pooled.b_col isa PooledArrays.PooledArray)
end

@testset "select and drop agree" begin
    input = "a,b,c\n1,2,3\n4,5,6\n"
    sourceparity(input; kw=(; select=[:a, :c]))
    sourceparity(input; kw=(; select=[1, 3]))
    sourceparity(input; kw=(; select=[1, 1, 3]))                  # duplicates collapse
    sourceparity(input; kw=(; select=[true, false, true]))
    sourceparity(input; kw=(; drop=[:b]))
    sourceparity(input; kw=(; drop=[2]))
    sourceparity(input; kw=(; drop=[false, true, false]))
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:a], drop=[:b])
    @test_throws ArgumentError A.File(IOBuffer(input); select=(nm, i) -> i == 1)
    @test_throws ArgumentError A.File(IOBuffer(input); select=[:nope])
    f = sourceparity("my col,b\n1,2\n"; kw=(; normalizenames=true, select=[:my_col]))
    @test Base.names(f) == [:my_col]
end

@testset "lazy / LazyFile: the index as a table" begin
    csv = "id,name,price,when\n1,alice,3.5,2024-01-02\n2,\"bob, jr\",4.25,2024-01-03\n3,,5.0,\n4,\"say \"\"hi\"\"\",6.0,2024-01-05\n"
    lf = A.lazy(IOBuffer(csv))
    @test lf isa A.LazyFile && size(lf) == (4, 4) && Base.names(lf) == [:id, :name, :price, :when]
    @test lf.name[2] == "bob, jr" && lf[4, :name] == "say \"hi\"" && ismissing(lf[3, :name])   # quotes/escapes/empties
    @test lf[1, 3] == "3.5" && lf.price isa A.LazyColumn && eltype(lf.price) == Union{K.DataString, Missing}
    @test isequal(collect(lf.when), ["2024-01-02", "2024-01-03", missing, "2024-01-05"])
    @test isequal(collect(lf.name), [lf.name[i] for i in 1:4])       # sequential and random access agree
    # Tables.jl columns; materialization is on demand
    ct = Tables.columntable(lf)
    @test keys(ct) == (:id, :name, :price, :when) && ct.id == ["1", "2", "3", "4"]
    @test Tables.rowcount(lf) == 4 && Tables.schema(lf).names == (:id, :name, :price, :when)
    # typed lazy columns parse on access through the same kernels
    lt = A.lazy(IOBuffer(csv); types=Dict(:price => Float64, :when => Date, :id => Int64))
    @test collect(lt.price) == [3.5, 4.25, 5.0, 6.0] && eltype(lt.id) == Union{Int64, Missing}
    @test isequal(collect(lt.when), [Date(2024, 1, 2), Date(2024, 1, 3), missing, Date(2024, 1, 5)])
    @test ismissing(A.lazy(IOBuffer("a\nx\n"); types=Int64).a[1])          # unparsable ⇒ missing
    narrowlazy = A.lazy(IOBuffer("x\n1\n128\n"); types=Int8)
    @test eltype(narrowlazy.x) == Union{Int8, Missing}
    @test isequal(collect(narrowlazy.x), Union{Int8, Missing}[1, missing])
    customlazy = A.lazy(IOBuffer("x\n1\nbad\n"); types=APICustomScalar)
    @test isequal(collect(customlazy.x),
                  Union{APICustomScalar, Missing}[APICustomScalar(1), missing])
    # a Parsers span method parses the field bytes in place; a Base.tryparse
    # method sees a String; neither method is an error before parsing
    spanfile = A.File(IOBuffer("x\n1\nbad\n2\n"); types=APISpanScalar, on_error=:collect)
    @test isequal(collect(spanfile.x),
                  Union{APISpanScalar, Missing}[APISpanScalar(1), missing, APISpanScalar(2)])
    @test length(A.problems(spanfile)) == 1
    @test A._usesspanparser(APISpanScalar) && !A._usesspanparser(APICustomScalar)
    @test_throws ArgumentError A.File(IOBuffer("x\n1\n"); types=APIParseOnly)
    @test_throws ErrorException A.File(IOBuffer("x\n1\n"); types=APIThrowingScalar)
    missinglazy = A.lazy(IOBuffer("x\nvalue\n\n"); types=Missing,
                         ignoreemptyrows=false)
    @test all(ismissing, missinglazy.x)
    dateinput = "d1,d2\n15/01/2023,2023.01.16\n"
    datetypes = Dict(:d1 => Date, :d2 => Date)
    dateformats = Dict(:d1 => "dd/mm/yyyy", :d2 => "yyyy.mm.dd")
    datelazy = A.lazy(IOBuffer(dateinput); types=datetypes, dateformat=dateformats)
    @test datelazy.d1[1] == Date(2023, 1, 15)
    @test datelazy.d2[1] == Date(2023, 1, 16)
    # File(lf) reuses the index and equals a fresh parse
    f = A.File(lf); g = A.File(IOBuffer(csv))
    @test all(isequal(collect(Tables.getcolumn(f, nm)), collect(Tables.getcolumn(g, nm))) for nm in Base.names(g))
    ft = A.File(lf; types=Dict(:price => Float32))
    @test Tables.getcolumn(ft, :price) isa Vector{Float32}
    cleanlf = A.lazy(IOBuffer("a\n1\n"))
    @test_throws ArgumentError A.File(cleanlf; maxproblems=-1)
    @test_throws ArgumentError A.File(cleanlf; maxwarnings=-1)
    @test_throws ArgumentError A.File(cleanlf; on_error=:invalid)
    @test_throws ArgumentError A.File(cleanlf; ntasks=0)
    @test A.File(cleanlf; ntasks=1).a == [1]
    badlf = A.lazy(IOBuffer("a\nbad\n"))
    warningcapped = A.File(badlf; types=Int64, maxwarnings=0, on_error=:collect)
    @test isempty(A.problems(warningcapped))
    @test getfield(warningcapped, :table).droppedproblems == 1
    explicitcap = A.File(badlf; types=Int64, maxwarnings=0, maxproblems=1, on_error=:collect)
    @test length(A.problems(explicitcap)) == 1
    @test A.File(cleanlf; validate=false,
                 types=Dict(:absent => Int64)).a == [1]
    loosevalidation = A.lazy(IOBuffer("a\n1\n"); validate=false)
    @test_throws ArgumentError A.File(loosevalidation; validate=true,
                                      types=Dict(:absent => Int64))

    # Lazy projection is stable file order with duplicate entries removed.
    # A later eager parse can narrow that visible set, but cannot restore a
    # column that the LazyFile already dropped.
    projected = A.lazy(IOBuffer("a,b,c\n1,2,3\n"); select=[:c, :a, :c])
    @test Base.names(projected) == [:a, :c]
    projectedfile = A.File(projected; types=Dict(:a => Int8), pool=false)
    @test Base.names(projectedfile) == [:a, :c]
    @test projectedfile.a == Int8[1] && projectedfile.c == [3]
    @test Base.names(A.File(projected; drop=[:a], pool=false)) == [:c]
    @test_throws ArgumentError A.File(projected; select=[:b])

    # Prepared must not freeze the default 10k cap. Value diagnostics are
    # regenerated under the File call's cap. Header diagnostics are replayed
    # from compact structural row references instead of retained without a cap.
    manyproblemcount = 10_005
    manybadlf = A.lazy(IOBuffer("a\n" * "bad\n"^manyproblemcount))
    manybadfile = A.File(manybadlf; types=Int64, on_error=:collect,
                         maxproblems=manyproblemcount, ntasks=1)
    @test length(A.problems(manybadfile)) == manyproblemcount
    @test getfield(manybadfile, :table).droppedproblems == 0
    wideheader = join(fill("\"a\"x", manyproblemcount), ',') * "\n"
    wideheaderlazy = A.lazy(IOBuffer(wideheader))
    @test length(getfield(getfield(getfield(wideheaderlazy, :prepared), :p), :headerlog).items) == 10_000
    @test length(getfield(getfield(getfield(wideheaderlazy, :prepared), :p), :headerrefs)) == 1
    wideheaderfile = A.File(wideheaderlazy; on_error=:collect,
                            maxproblems=manyproblemcount, ntasks=1)
    @test length(A.problems(wideheaderfile)) == manyproblemcount
    @test getfield(wideheaderfile, :table).droppedproblems == 0
    # stringtype, select, limit, header, dialect
    ls = A.lazy(IOBuffer(csv); stringtype=String, select=[:name])
    @test Base.names(ls) == [:name] && eltype(ls.name) == Union{String, Missing} && ls.name[1] == "alice"
    ll = A.lazy(IOBuffer(csv); limit=2)
    @test size(ll, 1) == 2 && collect(ll.id) == ["1", "2"]
    lh = A.lazy(IOBuffer("x;y\n1;2\n"); delim=';', header=false)
    @test Base.names(lh) == [:Column1, :Column2] && lh[1, 1] == "x"
    # a chunked file: cells across chunk boundaries, random access, iteration
    big = "a,b\n" * join(("$(i),v$(i)" for i in 1:5000), "\n") * "\n"
    lb = A.lazy(IOBuffer(big); chunkbytes=512)
    @test lb.b[4321] == "v4321" && lb.b[1] == "v1" && lb.b[5000] == "v5000"
    @test collect(lb.a) == string.(1:5000)
    @test [x for x in lb.a] == string.(1:5000)                        # iterate == getindex
    @test getfield(lb.b, :hint) isa Threads.Atomic{Int}

    # DataString's view offset is Int32. Exercise the bounded-copy branch
    # everywhere with an injected small limit, then its real sparse >2 GiB
    # source position on platforms that can map it.
    lazybytes = Vector{UInt8}(codeunits("pad" * "lazy value beyond inline storage"))
    lazyview = A._compactview(lazybytes, 4, length(lazybytes) - 3)
    lazyowned = A._compactview(lazybytes, 4, length(lazybytes) - 3, -1)
    @test String(lazyowned) == String(lazyview) == "lazy value beyond inline storage"
    @test getfield(lazyview, :data) === lazybytes
    @test getfield(lazyowned, :data) !== lazybytes
    if Sys.WORD_SIZE == 64 && Sys.isunix()
        mktemp() do _, io
            offset0 = Int(typemax(Int32)) + 4096
            value = "lazy value at a large source offset"
            seek(io, offset0)
            write(io, value, '\n')
            flush(io)
            seekstart(io)
            mapped = Mmap.mmap(io, Vector{UInt8}, filesize(io))
            ci = K.ChunkIndex(offset0 + 1, filesize(io))
            dialect = K.Dialect()
            K.indexone!(ci, mapped, dialect, :scalar)
            column = A.LazyColumn{Union{K.DataString, Missing}}(
                mapped, [ci], [0], 1, K.makevalueopts(dialect), 1, K.DataString)
            cell = column[1]
            @test String(cell) == value
            @test getfield(cell, :data) !== mapped

            # Exercise indexed row access. The prepared index avoids a scan of
            # the sparse 2 GiB hole. Public Rows tests cover source preparation.
            opts = K.makevalueopts(dialect)
            plan = A.settlecolumns([:a], opts)
            inner = A._IndexedRows(mapped, [ci], [:a], Dict(:a => 1), plan,
                                   dialect)
            rowcell = first(inner)[1]
            @test String(rowcell) == value
            @test getfield(rowcell, :data) !== mapped
        end
    end

    # A LazyColumn can be shared by Tables.jl consumers. Exercise concurrent
    # random access across many chunks; each task races to replace the hint,
    # while the cell lookup must remain correct.
    order = randperm(MersenneTwister(0x6c617a79), 5000)
    observed = Vector{String}(undef, length(order))
    Threads.@threads for slot in eachindex(order)
        observed[slot] = String(lb.b[order[slot]])
    end
    @test observed == ["v$i" for i in order]
    @test occursin("5000 rows × 2 columns", sprint(show, lb))
    @test_throws BoundsError lb.a[5001]
end

@testset "cheap wins from the issue audit" begin
    # #853: space-ALIGNED files elect (' ', ignorerepeated=true); plain space
    # and comma files detect exactly as before
    aligned = "id   name    value\n1    alice   3.5\n22   bob     4.25\n333  carol   5.0\n"
    f = A.File(IOBuffer(aligned))
    @test Base.names(f) == [:id, :name, :value] && Tables.getcolumn(f, :id) == [1, 22, 333]
    @test Base.names(A.File(IOBuffer("a b c\n1 2 3\n4 5 6\n"))) == [:a, :b, :c]
    @test Base.names(A.File(IOBuffer("a,b\n1,2\n"))) == [:a, :b]
    @test_throws ArgumentError A.File(IOBuffer(aligned); ignorerepeated=true)   # still needs an explicit delim
    # #990: select/drop names match as spelled in the file OR normalized
    f = A.File(IOBuffer("my col,b\n1,2\n"); normalizenames=true, select=["my col"])
    @test Base.names(f) == [:my_col]
    f = A.File(IOBuffer("my col,b\n1,2\n"); normalizenames=true, drop=[:my_col])
    @test Base.names(f) == [:b]
    # #1118/#522: a malformed-quote cell keeps its raw bytes AND reports
    f = A.File(IOBuffer("a,b\n\"x\"y,1\nok,2\n"); types=String, on_error=:collect)
    @test collect(Tables.getcolumn(f, :a)) == ["\"x\"y", "ok"]
    @test any(p -> p.kind == :invalid_quoted_field && p.row == 1, CSV.problems(f))
    # #506: http(s) URLs are sources (Downloads stdlib); a bad URL is a clear error
    @test_throws Exception A.File("http://127.0.0.1:1/nope.csv")
end

@testset "skipped prefix rows are physical lines: quotes in them are inert" begin
    # #1012 / #1079 / #1160 — a stray quote in a junk preamble used to swallow the file
    f = A.File(IOBuffer("1'2\"junk\na,b\n1,2\n3,4\n"); header=2)
    @test Base.names(f) == [:a, :b] && Tables.getcolumn(f, :a) == [1, 3]
    f = A.File(IOBuffer("junk\n11.0\"\na,b\n1,2\n"); header=false, skipto=3)
    @test Base.names(f) == [:Column1, :Column2] && length(Tables.getcolumn(f, 1)) == 2
    f = A.File(IOBuffer("x\"y\nnames here\na,b\n1,2\n"); header=3)
    @test Base.names(f) == [:a, :b] && Tables.getcolumn(f, :b) == [2]
    f = A.File(IOBuffer("odd \" quote\r\nk,v\r\n7,8\r\n"); header=[2])   # CRLF prefix
    @test Base.names(f) == [:k, :v] && Tables.getcolumn(f, :k) == [7]
    # rows BETWEEN the header and skipto are real rows: a quoted newline there
    # is one row, exactly as before
    f = A.File(IOBuffer("a,b\n\"x\ny\",1\n2,3\n"); skipto=3)
    @test Tables.getcolumn(f, :a) == [2]
end

@testset "read(source, sink) calls the sink (functions, lambdas, types)" begin
    csv = IOBuffer("a,b\n1,2\n3,4\n")
    @test A.read(csv, Tables.matrix) == [1 2; 3 4]                 # function sink runs
    seekstart(csv)
    @test A.read(csv, Tables.rowtable) == [(a=1, b=2), (a=3, b=4)]
    seekstart(csv)
    @test A.read(csv, t -> sum(Tables.getcolumn(t, :a))) == 4     # lambda sink runs
    seekstart(csv)
    nt = A.read(csv, Tables.columntable)
    @test nt.a == [1, 3] && nt.b == [2, 4]
    @test A.read(Vector{UInt8}(codeunits("a,b\n1,2\n3,4\n")), Tables.matrix) ==
          [1 2; 3 4]
end

@testset "delimiter sniff ignores rows before a numbered header / skipto" begin
    # a one-line preamble ("skip me") used to elect the space as delimiter for
    # the whole file even though header=2 declares that row junk
    body = "region,price,qty\n" * join(("east,$(i).5,$(i)" for i in 1:300), "\n") * "\n"
    f = A.File(IOBuffer("skip me\n" * body); header=2)
    @test Base.names(f) == [:region, :price, :qty]
    f = A.File(IOBuffer("skip me\nand me too\n" * body); header=false, skipto=3)
    @test length(Base.names(f)) == 3            # column count from the first data row
    @test Tables.getcolumn(f, 1)[1] == "region" && length(Tables.getcolumn(f, 1)) == 301
    f = A.File(IOBuffer("skip me\nx\n" * body); header=[:r, :p, :q], skipto=3)
    @test Base.names(f) == [:r, :p, :q] && length(Tables.getcolumn(f, :q)) == 301
    f = A.File(IOBuffer("skip me\n" * body); header=[2])
    @test Base.names(f) == [:region, :price, :qty]
end

@testset "pooling is a finalize-time API pass over DataString columns" begin
    # values never change; container/policy/levels are the observable contract
    rng = Random.MersenneTwister(77)
    cats = ["aa", "bb", "a much longer categorical value", "q\"\"z"]
    rows = String[]
    for i in 1:500
        c = rand(rng, cats)
        cell = c == "q\"\"z" ? "\"q\"\"z\"" : c
        push!(rows, (rand(rng, 1:10) == 1 ? "" : cell) * "," * string(i))
    end
    csv = "cat,val\n" * join(rows, "\n") * "\n"
    plain = A.File(IOBuffer(csv))
    for cb in (64, 256, 1 << 20), par in (false, true)
        f = A.File(IOBuffer(csv); pool=true, chunkbytes=cb, parallel=par)
        c = Tables.getcolumn(f, :cat)
        @test c isa PooledArrays.PooledArray
        @test isequal(collect(c), collect(Tables.getcolumn(plain, :cat)))
        # deterministic level ids: first occurrence in row order (missing last)
        lv = [x for x in c.pool if !ismissing(x)]
        @test lv == unique([String(x) for x in collect(Tables.getcolumn(plain, :cat)) if !ismissing(x)])
    end
    # ratio policy: 4 levels / 500 rows ⇒ pooled at 0.05, plain at 0.005; cap abandons
    @test Tables.getcolumn(A.File(IOBuffer(csv); pool=0.05), :cat) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(A.File(IOBuffer(csv); pool=0.005), :cat) isa PooledArrays.PooledArray)
    tall = A.File(IOBuffer("a\n" * join(1:200, "\n") * "\n"); types=String, pool=(1.0, 8))
    @test !(Tables.getcolumn(tall, :a) isa PooledArrays.PooledArray)
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); pool=1.5)
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); pool=(-0.1, 10))
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); pool=(1.0, -1))
    # all-present ⇒ concrete eltype; missing ⇒ Union eltype with missing as a level
    tp = Tables.getcolumn(A.File(IOBuffer("a,b\nx,1\nx,2\n"); pool=true), :a)
    @test tp isa PooledArrays.PooledArray && eltype(tp) == String
    tm = Tables.getcolumn(A.File(IOBuffer("a,b\nx,1\n,2\ny,3\nx,4\n"); pool=true), :a)
    @test eltype(tm) == Union{String, Missing} && isequal(collect(tm), ["x", missing, "y", "x"])
    # escaped levels unescape exactly once
    esc = Tables.getcolumn(A.File(IOBuffer("a\n\"q\"\"z\"\nplain\n\"q\"\"z\"\n"); pool=true), :a)
    @test collect(esc) == ["q\"z", "plain", "q\"z"] && length(esc.pool) == 2
    # per-column specs: Dict by name / by regex, and a vector; select maps source specs
    f = A.File(IOBuffer("k,v,w\nx,y,z\nx,y,z\n"); pool=Dict(:k => true))
    @test Tables.getcolumn(f, :k) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :v) isa PooledArrays.PooledArray)
    f = A.File(IOBuffer("k,v,w\nx,y,z\nx,y,z\n"); pool=[false, true, false], select=[:v, :w])
    @test Tables.getcolumn(f, :v) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :w) isa PooledArrays.PooledArray)
    # empty typed String column: nothing to pool, no error
    @test length(Tables.getcolumn(A.File(IOBuffer("a\n"); types=String, pool=true), :a)) == 0
    # the primitive itself
    col = Tables.getcolumn(A.File(IOBuffer("a\nx\ny\nx\n")), :a)
    pc = A._poolcolumn(col, (1.0, 10))
    @test pc isa K.PooledColumn && K.poolrefs(pc) == UInt32[1, 2, 1]
    @test A._poolcolumn(col, (0.5, 10)) === nothing         # 2 levels > floor(0.5·3)=1
    @test A._poolpolicy((1.0, typemax(UInt32)))[2] ==
          Int(min(UInt128(typemax(Int)), UInt128(typemax(UInt32))))
    @test A._poolpolicy((1.0, big(typemax(Int)) + 1))[2] == typemax(Int)
    @test UInt64(A._MAX_POOL_LEVELS) ==
          min(UInt64(typemax(Int)), UInt64(typemax(UInt32)))
end

@testset "pooling agrees on values" begin
    vals = rand(["alpha", "beta", "gamma"], 400)
    input = "k\n" * join(vals, "\n") * "\n"
    # The default is no pooling. Explicit policies change only the container.
    f = sourceparity(input)
    @test !(Tables.getcolumn(Tables.columns(f), :k) isa PooledArrays.PooledArray)
    f = sourceparity(input; kw=(; pool=(0.2, 500)))
    @test Tables.getcolumn(Tables.columns(f), :k) isa PooledArrays.PooledArray
    f = sourceparity(input; kw=(; pool=false))
    @test !(Tables.getcolumn(Tables.columns(f), :k) isa PooledArrays.PooledArray)
    sourceparity(input; kw=(; pool=true))
    sourceparity(input; kw=(; pool=0.9))

    # Missing is an ordinary final pool level. Conversion remaps kernel ref 0
    # without changing the kernel-owned refs, while an all-present conversion
    # can transfer its exclusively owned refs during File conversion.
    kernelmissing = A._poolcolumn(K.parse("k\nx\n\ny\nx\n"; ignoreemptyrows=false)[:k], (1.0, 10))
    oldrefs = copy(K.poolrefs(kernelmissing))
    missingpool = A._topooledarray(kernelmissing)
    @test K.poolrefs(kernelmissing) == oldrefs == UInt32[1, 0, 2, 1]
    @test all(!iszero, missingpool.refs)
    @test missingpool.pool[end] === missing
    @test missingpool.invpool[missing] == missingpool.refs[2]
    @test isequal(collect(missingpool), ["x", missing, "y", "x"])

    kernelpresent = A._poolcolumn(K.parse(input)[:k], (1.0, typemax(Int)))
    presentpool = A._topooledarray(kernelpresent)
    @test presentpool.refs === K.poolrefs(kernelpresent)
    refsnapshot = copy(presentpool.refs)
    table = K.ParsedTable([:k], AbstractVector[presentpool], length(presentpool), K.Problem[], 0)
    @test A._downcast(A._finishstrings(table, String, nothing)).columns[1] === presentpool
    @test presentpool.refs == refsnapshot

    stringpool = A.File(IOBuffer(input); pool=true, stringtype=String).k
    @test stringpool isa PooledArrays.PooledArray{String}
end

@testset "value options agree" begin
    sourceparity("d\n15/01/2023\n16/01/2023\n"; kw=(; dateformat="dd/mm/yyyy"))
    sourceparity("x;y\n1,5;2\n"; kw=(; delim=';', decimal=','))
    sourceparity("b\nYES\nNO\n"; kw=(; truestrings=["YES"], falsestrings=["NO"]))
    sourceparity("n;m\n1,234;5\n"; kw=(; delim=';', groupmark=','))
    groupedinput = "n;m\n99,999,999,999,999,999,999,999,999;5\n"
    grouped = A.File(IOBuffer(groupedinput); delim=';', groupmark=',')
    @test eltype(grouped.n) === Int128
    @test grouped.n[1] == Int128(99999999999999999999999999)
    sourceparity("s,t\n  x  ,1\n"; kw=(; delim=',', stripwhitespace=true))
    # Without an explicit delimiter, field-count consistency keeps this as one column.
end

@testset "stringtype=String materializes" begin
    f = A.File(IOBuffer("s,m\nx,\ny,z\n"); stringtype=String)
    @test Tables.getcolumn(Tables.columns(f), :s) isa Vector{String}
    @test eltype(Tables.getcolumn(Tables.columns(f), :m)) == Union{String, Missing}
    @test isequal(collect(f.m), [missing, "z"])

    # The bulk path must reconstruct every inline length byte for byte. It must
    # also preserve embedded NULs, malformed UTF-8, and view-size boundaries.
    payloads = [fill(UInt8('a') + UInt8(n), n) for n in 0:12]
    append!(payloads, [fill(UInt8('p'), n) for n in (15, 16, 17)])
    push!(payloads, UInt8[0x61, 0x00, 0x62])
    push!(payloads, UInt8[0x61, 0xff, 0x62])
    input = UInt8[]
    append!(input, codeunits("s,m\n"))
    for bytes in payloads
        isempty(bytes) ? append!(input, codeunits("\"\"")) : append!(input, bytes)
        append!(input, codeunits(",ok\n"))
    end
    escaped = "a long \"escaped\" value"
    append!(input, codeunits("\"a long \"\"escaped\"\" value\",\n"))
    f = A.File(IOBuffer(input); types=String, pool=false, stringtype=String)
    expected = String[String(copy(bytes)) for bytes in payloads]
    push!(expected, escaped)
    @test [collect(codeunits(x)) for x in f.s] == [collect(codeunits(x)) for x in expected]
    @test isequal(collect(f.m), [fill("ok", length(payloads)); missing])

    # Source-mode coverage uses the String materialization route with an escaped
    # long cell and missing values in one parse.
    materialized = "s,m\n\"a long \"\"escaped\"\" value\",NA\nplain,\n"
    sourceparity(materialized;
                 kw=(; stringtype=String, pool=false, missingstring="NA"))
end

@testset "structural edge cases agree" begin
    sourceparity("a,b\r\n1,2\r\n3,4\r\n")                          # CRLF
    sourceparity("a,b\r1,2\r3,4\r")                              # CR-only
    sourceparity("﻿a,b\n1,2\n")                               # BOM
    sourceparity("a,b\n1,2")                                       # no trailing newline
    sourceparity("a,b\n\"x\ny\",2\n")                              # quoted newline
    f = A.File(IOBuffer(""))
    @test length(f) == 0 && isempty(Base.names(f))
    # tiny chunks: same values as one-chunk parse (kernel-side determinism)
    input = "a,b\n" * join(("$(i),v$(i)" for i in 1:50), "\n") * "\n"
    ref = colvalues(A.File(IOBuffer(input)))
    for cb in (16, 64, 256)
        @test isequal(colvalues(A.File(IOBuffer(input); chunkbytes=cb)), ref)
    end
end

@testset "long rows do not widen the schema" begin
    fa = A.File(IOBuffer("a,b\n1,2,3\n4,5\n"); on_error=:collect)
    @test Base.names(fa) == [:a, :b]                          # extra field ⇒ problem
    @test any(p -> p.kind == :long_row, A.problems(fa))
    @test collect(fa.a) == [1, 4] && collect(fa.b) == [2, 5]
end

@testset "File surface: rows, properties, show, problems" begin
    f = A.File(IOBuffer("name,score\nalice,1\nbob,2\n"))
    @test length(f) == 2
    @test f[1].name == "alice" && f[2].score == 2
    @test [r.name for r in f] == ["alice", "bob"]
    @test propertynames(f) == [:name, :score]
    @test collect(f.score) == [1, 2]
    @test A.rownumber(f[2]) == 2
    @test f[1][1] == "alice" && f[1][:name] == "alice"
    @test length(f[1]) == 2 && propertynames(f[1]) == [:name, :score]
    @test_throws BoundsError f[3]
    @test occursin("2 x 2", sprint(show, f))
    @test A.problems(f) isa Vector{K.Problem}
    @test Tables.schema(f).names == (:name, :score)
    @test Tables.rowaccess(A.File) && Tables.rows(f) === f
    fbad = A.File(IOBuffer("a\n\"unterminated"); on_error=:collect)
    @test any(p -> p.kind == :unclosed_quote, A.problems(fbad))
    @test occursin("problem", sprint(show, fbad))
    # columns named like internals cannot shadow the interface
    fsh = A.File(IOBuffer("table,name,lookup\n1,2,3\n"))
    @test collect(fsh.table) == [1] && collect(fsh.lookup) == [3]
    @test Tables.rowcount(fsh) == 1 && length(fsh) == 1
    @test fsh[1].name == 2
    @test Tables.columnnames(fsh[1]) == [:table, :name, :lookup]
    @test fsh isa AbstractVector{A.FileRow}
    @test size(fsh) == (1,) && axes(fsh) == (Base.OneTo(1),)
    @test fsh[:name] === fsh.name && fsh["name"] === fsh.name
    @test Base.names(fsh) == [:table, :name, :lookup]
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); ntasks=0)
    tasksrc = "a\n" * join(1:2000, '\n') * "\n"
    prepared = A._prepareindexed(IOBuffer(tasksrc); ntasks=2)
    @test length(getfield(prepared, :bi).chunks) <= 2
    @test 1 <= count(_ -> true, A.Chunks(IOBuffer(tasksrc); ntasks=2, pool=false)) <= 2
    empty!(API_PARSE_TASKS)
    A.File(IOBuffer(tasksrc); types=APITaskScalar, ntasks=2,
           parallel=true, chunkbytes=64, pool=false)
    @test 1 <= length(API_PARSE_TASKS) <= 2
    tasklazy = A.lazy(IOBuffer(tasksrc); chunkbytes=64)
    @test length(getfield(getfield(tasklazy, :prepared), :bi).chunks) > 2
    empty!(API_PARSE_TASKS)
    A.File(tasklazy; types=APITaskScalar, ntasks=2, parallel=true, pool=false)
    @test 1 <= length(API_PARSE_TASKS) <= 2
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); stringtype=SubString{String})
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); silencewarnings=true)
end

@testset "header diagnostics merge before strict/capping" begin
    clean = K.ParsedTable(Symbol[], AbstractVector[], 0, K.Problem[], 0)
    same, firstproblem = A._mergeproblems(clean, nothing, 0)
    @test same === clean && firstproblem === nothing
    droppedheader = K.ProblemLog(0)
    K.pushproblem!(droppedheader, 0, 1, 1, :invalid_value, "header")
    merged, firstproblem = A._mergeproblems(clean, droppedheader, 0)
    @test isempty(merged.problems) && merged.droppedproblems == 1
    @test firstproblem !== nothing && firstproblem.kind == :invalid_value

    input = "\"bad\"x,a\nBAD,2\n"
    f = A.File(IOBuffer(input); types=Dict(1 => Int64), maxproblems=1, on_error=:collect)
    @test length(A.problems(f)) == 1
    @test first(A.problems(f)).kind == :invalid_quoted_field
    @test getfield(f, :table).droppedproblems == 1
    f0 = A.File(IOBuffer(input); types=Dict(1 => Int64), maxproblems=0, on_error=:collect)
    @test isempty(A.problems(f0)) && getfield(f0, :table).droppedproblems == 2
    @test occursin("2 problem(s) recorded — 0 retained", sprint(show, f0))
    err = try
        A.File(IOBuffer(input); types=Dict(1 => Int64), strict=true, maxproblems=0)
        nothing
    catch e
        e
    end
    @test err isa CSV.ParseError
    @test occursin("invalid_quoted_field", sprint(showerror, err))
end

@testset "read into sinks" begin
    input = "a,b\n1,x\n2,y\n"
    ct = A.read(IOBuffer(input), Tables.columntable)
    @test ct.a == [1, 2] && String.(ct.b) == ["x", "y"]
    rt = Tables.rowtable(A.File(IOBuffer(input)))
    @test length(rt) == 2 && rt[1].a == 1
end

@testset "sources: path, IO, bytes, Cmd" begin
    path, io = mktemp()
    write(io, "a,b\n1,2\n"); close(io)
    @test collect(A.File(path).a) == [1]
    @test collect(A.File(Vector{UInt8}(codeunits("a,b\n1,2\n"))).a) == [1]
    catcmd = `$(Base.julia_cmd()) --startup-file=no --eval "write(stdout, open(ARGS[1]))"`
    @test collect(A.File(`$(catcmd) $path`).a) == [1]
    @test_throws ArgumentError A.File("definitely-not-a-file.csv")
    @test collect(A.File(codeunits("a,b\n1,2\n")).a) == [1]
    parent = Vector{UInt8}(codeunits("prefixa,b\n1,stable value\nsuffix"))
    lo = ncodeunits("prefix") + 1
    hi = lo + ncodeunits("a,b\n1,stable value\n") - 1
    viewfile = A.File(@view(parent[lo:hi]); types=Dict(:b => String), pool=false)
    fill!(@view(parent[lo:hi]), UInt8('x'))
    @test viewfile.a == [1]
    @test String(viewfile.b[1]) == "stable value"
    rm(path)
    # Zero-byte files use the read path. Directories and FIFOs fail before a
    # read or mmap. A file exactly at the threshold uses the mmap branch.
    zeropath, zeroio = mktemp()
    close(zeroio)
    @test isempty(A.resolvesource(zeropath))
    rm(zeropath)
    dirpath = mktempdir()
    @test_throws ArgumentError A.resolvesource(dirpath)
    rm(dirpath)
    @static if Sys.isunix()
        fifodir = mktempdir()
        fifopath = joinpath(fifodir, "source.fifo")
        run(`mkfifo $fifopath`)
        @test_throws ArgumentError A.resolvesource(fifopath)
        rm(fifodir; recursive=true)
    end
    edgepath, edgeio = mktemp()
    write(edgeio, fill(UInt8('x'), A.MMAP_THRESHOLD))
    close(edgeio)
    edgemapped = A.resolvesource(edgepath; prefetch=false)
    @test length(edgemapped) == A.MMAP_THRESHOLD
    # Windows does not allow an open memory mapping to be unlinked. Retain and
    # finalize the exact mapped backing instead of relying on GC reachability.
    finalizemapping!(edgemapped)
    rm(edgepath)

    # A file across the mmap threshold parses identically when mapped or
    # buffered. Every lazy public surface must keep the mapping alive.
    bigpath, bigio = mktemp()
    write(bigio, "s,n\n" *
                 join(("word$(i % 977)_abcdefghijklmnop,$(i)" for i in 1:60_000), "\n") *
                 "\n")
    close(bigio)
    @test filesize(bigpath) >= A.MMAP_THRESHOLD
    # Resolve one non-prefetched mapping. Pass the retained backing through all
    # lazy public surfaces so its lifetime is explicit and Windows cleanup is
    # deterministic. Buffered calls below never hold an OS mapping.
    mapped = A.resolvesource(bigpath; prefetch=false)
    let
        mappedcol = let f = A.File(mapped; pool=false)
            f.s
        end
        pooledcol = let f = A.File(mapped; pool=true)
            f.s
        end
        mappedrow = first(A.Rows(mapped))
        mappedbatch = first(A.Chunks(mapped; ntasks=2))
        fb = A.File(bigpath; buffer_in_memory=true)
        fnoprefetch = A.File(mapped; prefetch=false, pool=false)
        @test first(A.Rows(bigpath; buffer_in_memory=true)).s == mappedrow.s
        @test first(A.Rows(mapped; prefetch=false)).s == mappedrow.s
        bufferedbatch = first(A.Chunks(bigpath; ntasks=2, buffer_in_memory=true))
        noprefetchbatch = first(A.Chunks(mapped; ntasks=2, prefetch=false))
        @test colvalues(bufferedbatch) == colvalues(mappedbatch)
        @test colvalues(noprefetchbatch) == colvalues(mappedbatch)
        @test collect(String, mappedcol) == collect(String, Tables.getcolumn(fb, :s))
        @test collect(String, mappedcol) == collect(String, Tables.getcolumn(fnoprefetch, :s))
        @test pooledcol isa PooledArrays.PooledArray
        @test collect(Tables.getcolumn(mappedbatch, :n)) == collect(fb.n)[1:length(mappedbatch)]
        # This also runs K.materialize while its source is a read-only mapping.
        fm = A.File(mapped; pool=false, stringtype=String)
        @test fm.s == collect(String, mappedcol)
        GC.gc()
        @test String(mappedcol[1]) == "word1_abcdefghijklmnop"
        @test String(pooledcol[1]) == "word1_abcdefghijklmnop"
        @test mappedrow.s == "word1_abcdefghijklmnop"
        @test String(Tables.getcolumn(mappedbatch, :s)[1]) == "word1_abcdefghijklmnop"
    end
    finalizemapping!(mapped)
    rm(bigpath)
end

@testset "Rows behavior and source modes" begin
    input = "a,b\n1,x\n2,\n3,z\n"
    ra = collect(A.Rows(IOBuffer(input)))
    rb = collect(A.Rows(Vector{UInt8}(codeunits(input))))
    @test length(ra) == length(rb) == 3
    @test [_norm(r.a) for r in ra] == [_norm(r.a) for r in rb] == ["1", "2", "3"]
    @test isequal([_norm(r.b) for r in ra], [_norm(r.b) for r in rb])
    @test isequal([r.b for r in A.Rows(IOBuffer(input))], ["x", missing, "z"])
    # typed access parses on demand through the kernel value layer
    typed = A.Rows(IOBuffer(input); types=Dict(:a => Int64))
    @test [r.a for r in typed] == [1, 2, 3]
    @test Tables.schema(typed).types[1] == Union{Int64, Missing}
    partial = first(A.Rows(IOBuffer("a,b\n1,text\n"); types=Dict(:a => Int64)))
    @test partial.a == 1 && partial.b isa K.DataString
    partialinline = first(A.Rows(IOBuffer("a,b\n1,text\n");
                                 types=Dict(:a => Int64), stringtype=InlineString))
    @test partialinline.b isa String7
    narrowrows = A.Rows(IOBuffer("a\n1\n128\n"); types=Int8)
    @test Tables.schema(narrowrows).types[1] == Union{Int8, Missing}
    @test isequal([r.a for r in narrowrows], Union{Int8, Missing}[1, missing])
    typedbad = A.Rows(IOBuffer("a\n1\nbad\n"); types=Union{Int64, Missing})
    @test isequal([r.a for r in typedbad], Union{Int64, Missing}[1, missing])
    strictrow = first(A.Rows(IOBuffer("a\nbad\n"); types=Int64, strict=true))
    @test_throws CSV.ParseError strictrow.a
    errorrow = first(A.Rows(IOBuffer("a\n128\n"); types=Int8,
                            on_error=:error))
    @test_throws CSV.ParseError errorrow.a
    collectrow = first(A.Rows(IOBuffer("a\n128\n"); types=Int8,
                              strict=true, on_error=:collect))
    @test ismissing(collectrow.a)
    @test ismissing(first(A.Rows(IOBuffer("a\nvalue\n"); types=Missing)).a)
    @test_throws CSV.ParseError first(A.Rows(IOBuffer("a\nvalue\n");
                                             types=Missing, on_error=:error)).a
    malformedrow = first(A.Rows(IOBuffer("a\n\"x\"y\n"); on_error=:error))
    @test_throws CSV.ParseError malformedrow.a
    @test_throws ArgumentError A.Rows(IOBuffer(input); on_error=:invalid)
    # windowing composes
    @test length(collect(A.Rows(IOBuffer(input); limit=2))) == 2
    @test [r.a for r in A.Rows(IOBuffer(input); skipto=3)] == ["2", "3"]
    windowed = collect(A.Rows(IOBuffer(input); skipto=3, limit=1))
    @test [r.a for r in windowed] == ["2"] && A.rownumber(only(windowed)) == 1
    @test isequal([r.a for r in A.Rows(IOBuffer("a\nNA\n1\n"); missingstring="NA")],
                  [missing, "1"])
    quoted = "a,b\n\"x\ny\",1\nz,2\n"
    @test [_norm(r.a) for r in A.Rows(IOBuffer(quoted))] == ["x\ny", "z"]
    @test length(collect(A.Rows(IOBuffer(input); footerskip=2))) == 1
    footer = "a\n1\n\n2\n\n3\n"
    @test [_norm(r.a) for r in A.Rows(IOBuffer(footer); footerskip=2)] == ["1", "2"]
    @test_throws ArgumentError A.Rows(IOBuffer(input); pool=true)
    @test_throws ArgumentError A.Rows(IOBuffer(input); nsample=2)
    @test_throws ArgumentError A.Rows(IOBuffer(input); maxproblems=1)
    @test_throws ArgumentError A.Rows(IOBuffer(input); maxwarnings=1)
    selectedrows = A.Rows(IOBuffer("a,b,c\n1,2,3\n");
                          select=[:c, :a, :c], types=Dict(:a => Int8))
    @test Tables.schema(selectedrows).names == (:a, :c)
    selectedrow = first(selectedrows)
    @test selectedrow.a == Int8(1) && selectedrow.c == "3"
    @test collect(Tables.columnnames(selectedrow)) == [:a, :c]
    @test Tables.schema(A.Rows(IOBuffer("a,b,c\n1,2,3\n"); drop=[:b])).names == (:a, :c)
    dateinput = "d1,d2\n15/01/2023,2023.01.16\n"
    daterow = first(A.Rows(IOBuffer(dateinput);
                           types=Dict(:d1 => Date, :d2 => Date),
                           dateformat=Dict(:d1 => "dd/mm/yyyy", :d2 => "yyyy.mm.dd")))
    @test (daterow.d1, daterow.d2) == (Date(2023, 1, 15), Date(2023, 1, 16))
end

@testset "typed cells accept blanks in every reader" begin
    for (T, token, value) in ((Int64, "1", 1), (Float64, "1.5", 1.5),
                              (Date, "2024-01-02", Date(2024, 1, 2)),
                              (DateTime, "2024-01-02T03:04:05", DateTime(2024, 1, 2, 3, 4, 5)),
                              (Time, "03:04:05", Time(3, 4, 5)),
                              (Bool, "true", true), (Char, "x", 'x'))
        for field in (" \t$token \t", "\" \t$token \t\"")
            input = "a\n$field\n"
            kw = (; delim=',', types=T)
            @test only(A.File(IOBuffer(input); kw...).a) == value
            @test only(A.lazy(IOBuffer(input); kw...).a) == value
            @test only(A.Rows(IOBuffer(input); kw...)).a == value
            @test only(A.Rows(IOBuffer(input); kw..., on_error=:error)).a == value
            @test only(only(A.Chunks(IOBuffer(input); kw...)).a) == value
        end
    end
    for reader in (A.File, A.lazy, A.Rows)
        f = reader(IOBuffer("a\n\" \"\n"); delim=',', types=Char)
        @test only(Tables.getcolumn(Tables.columntable(f), :a)) == ' '
        f = reader(IOBuffer("a\n 1 \n"); delim=',', types=String)
        @test only(Tables.getcolumn(Tables.columntable(f), :a)) == " 1 "
    end
end

@testset "escaped content reaches typed parsers" begin
    cases = ((Int64, "121", 121, '1'), (Float64, "1.25", 1.25, '1'),
             (Bool, "true", true, 't'), (Char, "\"", '"', '"'),
             (Date, "2021-01-02", Date(2021, 1, 2), '2'),
             (DateTime, "2021-01-02T03:04:05", DateTime(2021, 1, 2, 3, 4, 5), 'T'),
             (Time, "03:04:05", Time(3, 4, 5), ':'),
             (APICustomScalar, "121", APICustomScalar(121), '1'),
             (APISpanScalar, "121", APISpanScalar(121), '1'))
    for (T, token, expected, q) in cases
        io = IOBuffer()
        A.write(io, (a=[token],); quotechar=q)
        input = String(take!(io))
        kw = (; delim=',', quotechar=q, types=T)
        for cb in (1, 64)
            @test only(A.File(IOBuffer(input); kw..., chunkbytes=cb, on_error=:error).a) == expected
            @test only(A.lazy(IOBuffer(input); kw..., chunkbytes=cb).a) == expected
            @test only(A.Rows(IOBuffer(input); kw..., chunkbytes=cb)).a == expected
            @test only(A.Rows(IOBuffer(input); kw..., chunkbytes=cb, on_error=:error)).a == expected
            @test only(only(A.Chunks(IOBuffer(input); kw..., chunkbytes=cb)).a) == expected
        end
        field = split(input, '\n')[2]
        @test only(A.File(IOBuffer("a,$field\n"); kw..., transpose=true, on_error=:error).a) == expected
    end
    # Escape bytes may quote a digit even with the usual double-quote dialect.
    input = "a\n\"\\1\\2\"\n"
    @test only(A.File(IOBuffer(input); delim=',', escapechar='\\').a) == 12
    @test only(A.File(IOBuffer(input); delim=',', escapechar='\\', types=Int).a) == 12
    # Sentinel matching uses the same decoded content as value parsing.
    input = "a\n\"\\N\\A\"\n"
    for reader in (A.File, A.lazy, A.Rows)
        f = reader(IOBuffer(input); delim=',', escapechar='\\', missingstring="NA")
        @test ismissing(only(Tables.getcolumn(Tables.columntable(f), :a)))
    end
end

@testset "Rows cell access allocations do not grow with row count" begin
    mkrows(n) = A.Rows(IOBuffer("a,b,c\n" *
        join(("$i,s$i,$i.5" for i in 1:n), '\n') * "\n"); types=[Int64, String, Float64])
    small, big = mkrows(500), mkrows(5000)
    function sumcells(rows, getcell::F) where {F}
        total = 0.0
        for row in rows
            total += getcell(row)
        end
        return total
    end
    # Warm each access form before measuring. Fixed call overhead may differ
    # across Julia versions, but allocations must not grow with the row count.
    for getcell in (r -> r.c, r -> r[3], r -> r[:c],
                    r -> Tables.getcolumn(r, 3), r -> Tables.getcolumn(r, :c),
                    r -> Tables.getcolumn(r, Float64, 3, :c))
        @test sumcells(small, getcell) == sum(1:500) + 0.5 * 500
        @test sumcells(big, getcell) == sum(1:5000) + 0.5 * 5000
        @test @allocated(sumcells(big, getcell)) == @allocated(sumcells(small, getcell))
    end
    # Read the index through a mutable reference on each row so it cannot fold
    # to a constant. Projection must still map it to the correct source column.
    projected = A.Rows(IOBuffer("a,b,c\n1,text,1.5\n2,more,2.5\n");
                       types=[Int64, String, Float64], select=[:c])
    index = Ref(1)
    @test sumcells(projected, r -> r[index[]]) == 4.0
    @test first(small)["c"] == 1.5
    @test_throws KeyError first(small)[:nope]
end

@testset "source worker lifetime" begin
    env = dirname(Base.active_project())
    script = joinpath(@__DIR__, "prefetch.jl")
    @test success(`$(Base.julia_cmd()) --startup-file=no --threads=2 --project=$env $script`)
end

@testset "Chunks: stable schema, values concat to File" begin
    input = "a,b\n" * join(("$(i)," * (i == 40 ? "" : "v$(i)") for i in 1:60), "\n") * "\n"
    ref = colvalues(A.File(IOBuffer(input); pool=false))
    batches = collect(A.Chunks(IOBuffer(input); chunkbytes=64))
    @test length(batches) > 1
    @test all(b -> b isa A.File && b isa AbstractVector{A.FileRow}, batches)
    @test all(b -> eltype(b[:a]) == eltype(batches[1][:a]), batches)      # stable
    @test all(b -> eltype(b[:b]) == eltype(batches[1][:b]), batches)      # even w/ late missing
    catted = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in batches))
              for j in (:a, :b)]
    @test isequal(catted, ref[2])
    # windowing composes with batching
    b2 = collect(A.Chunks(IOBuffer("a,b\n1,2\n3,4\n5,6\n"); chunkbytes=8, skipto=3))
    @test sum(length, b2) == 2
    @test isempty(collect(A.Chunks(IOBuffer(input); chunkbytes=32, limit=0)))
    @test isempty(collect(A.Chunks(IOBuffer(input); chunkbytes=32, footerskip=60)))
    footerparts = collect(A.Chunks(IOBuffer("a\n1\n\n2\n\n3\n");
                                   chunkbytes=2, footerskip=2))
    @test reduce(vcat, (collect(b[:a]) for b in footerparts); init=Int[]) == [1, 2]
    # limit and footer windows trim the prepared chunks before the schema pass.
    for kw in ((; limit=17), (; footerskip=17), (; skipto=10, limit=13))
        file = colvalues(A.File(IOBuffer(input); pool=false, kw...))
        parts = collect(A.Chunks(IOBuffer(input); chunkbytes=32, kw...))
        got = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in parts); init=Any[])
               for j in (:a, :b)]
        @test isequal(got, file[2])
    end
    routed = "a;b\n1,5;NA\n2,5;3\n"
    file = colvalues(A.File(IOBuffer(routed); delim=';', decimal=',',
                            missingstring="NA", pool=false, fastindex=false))
    parts = collect(A.Chunks(IOBuffer(routed); delim=';', decimal=',',
                             missingstring="NA", chunkbytes=8, fastindex=false))
    got = [reduce(vcat, (Any[_norm(x) for x in b[j]] for b in parts); init=Any[])
           for j in (:a, :b)]
    @test isequal(got, file[2])
    bad = first(A.Chunks(IOBuffer("a\nBAD\nNOPE\n"); types=Int64, on_error=:collect,
                         chunkbytes=64, maxproblems=0))
    @test isempty(A.problems(bad)) && getfield(bad, :table).droppedproblems == 2
    narrowbatch = first(A.Chunks(IOBuffer("a\n1\n128\n"); types=Int8, on_error=:collect,
                                 chunkbytes=64, maxproblems=1))
    @test isequal(collect(narrowbatch.a), Union{Int8, Missing}[1, missing])
    @test length(A.problems(narrowbatch)) == 1
    @test_throws CSV.ParseError first(A.Chunks(IOBuffer("a\n128\n"); types=Int8,
                                               chunkbytes=64, strict=true,
                                               maxproblems=0))
    laterbatchsrc = "a\n1\n2\n3\n4\n128\n"
    laterbatches = collect(A.Chunks(IOBuffer(laterbatchsrc); types=Int8, on_error=:collect,
                                    chunkbytes=4, maxproblems=1))
    problem_batches = filter(b -> !isempty(A.problems(b)), laterbatches)
    @test length(laterbatches) > 1
    @test all(eltype(b.a) == Union{Int8, Missing} for b in laterbatches)
    @test length(problem_batches) == 1
    laterproblem = only(A.problems(only(problem_batches)))
    @test (laterproblem.row, laterproblem.col, laterproblem.pos) ==
          (5, 1, first(findfirst("128", laterbatchsrc)))
    # pool is now supported per batch (each batch is an independent table);
    # only a single policy — Dict/vector per-column forms stay File-only
    @test Tables.getcolumn(first(A.Chunks(IOBuffer("s\n" * "x\ny\n"^50); pool=true, chunkbytes=64)), :s) isa PooledArrays.PooledArray
    @test_throws ArgumentError A.Chunks(IOBuffer(input); pool=Dict(:a => true))
    selected = collect(A.Chunks(IOBuffer("a,b,c\n1,bad,128\n2,no,3\n"); on_error=:collect,
                                select=[:c, :a, :c], types=Dict(:c => Int8),
                                chunkbytes=8, maxproblems=1))
    @test all(Base.names(b) == [:a, :c] for b in selected)
    @test reduce(vcat, (collect(b.a) for b in selected)) == [1, 2]
    @test isequal(reduce(vcat, (collect(b.c) for b in selected)),
                  Union{Missing, Int8}[missing, 3])
    @test sum(length ∘ A.problems, selected) == 1
    @test Base.names(first(A.Chunks(IOBuffer("a,b,c\n1,2,3\n");
                                          drop=[:b], chunkbytes=64))) == [:a, :c]
    dateinput = "d1,d2\n15/01/2023,2023.01.16\n"
    datechunk = first(A.Chunks(IOBuffer(dateinput);
                               types=Dict(:d1 => Date, :d2 => Date),
                               dateformat=Dict(:d1 => "dd/mm/yyyy", :d2 => "yyyy.mm.dd"),
                               chunkbytes=64))
    @test (datechunk.d1[1], datechunk.d2[1]) ==
          (Date(2023, 1, 15), Date(2023, 1, 16))
end

end # @testset CSV readers

@testset "transposed diagnostics" begin
    for parallel in (false, true), cap in (0, 1, 10), T in (nothing, String, Int64, Missing)
        input = "a,1,\"bad\"tail,3\n"
        f = A.File(IOBuffer(input); transpose=true, parallel, types=T,
                   maxproblems=cap, on_error=:collect)
        expected = T === Int64 || T === Missing ? missing : "\"bad\"tail"
        @test isequal(f.a[2], expected)
        nproblems = T === Missing ? 3 : 1
        @test length(A.problems(f)) == min(cap, nproblems)
        @test getfield(f, :table).droppedproblems == max(0, nproblems - cap)
        @test_throws A.ParseError A.File(IOBuffer(input); transpose=true, parallel,
                         types=T, maxproblems=cap, on_error=:error)
    end
    for input in ("a,\"unterminated", "a,1,\"unterminated")
        f = A.File(IOBuffer(input); transpose=true, on_error=:collect)
        @test any(p -> p.kind === :invalid_quoted_field, A.problems(f))
        @test any(p -> p.kind === :unclosed_quote, A.problems(f))
    end
    f = A.File(IOBuffer("a,1,\"unterminated"); transpose=true, limit=1)
    @test f.a == [1] && isempty(A.problems(f))
    f = A.File(IOBuffer("\"bad\"tail,1,2\n"); transpose=true, on_error=:collect)
    @test Tables.columnnames(f) == [Symbol("\"bad\"tail")]
    @test only(A.problems(f)).row == 0 && only(A.problems(f)).col == 1
    @test_throws A.ParseError A.File(IOBuffer("\"bad\"tail,1\n"); transpose=true,
                                    maxproblems=0, on_error=:error)
end

@testset "gzip, typemap, dateformat/pool Dicts, downcast, transpose, deprecations" begin
    # auto-gzip: every source kind decompresses by magic bytes
    plain = "a,b\n1,x\n2,y\n"
    gz = transcode(CodecZlib.GzipCompressor, Vector{UInt8}(codeunits(plain)))
    for src in (gz, IOBuffer(gz))
        f = A.File(src)
        @test Tables.getcolumn(f, :a) == [1, 2]
    end
    # This is also the in-memory stream pattern used by the precompile workload.
    # The wrapper must not close its IOBuffer before take! retrieves the bytes.
    gzbuf = IOBuffer()
    gzstream = CodecZlib.GzipCompressorStream(gzbuf; stop_on_end=true)
    write(gzstream, plain)
    close(gzstream)
    streamedgz = take!(gzbuf)
    @test streamedgz[1:2] == UInt8[0x1f, 0x8b]
    @test Tables.getcolumn(A.File(streamedgz), :a) == [1, 2]
    gzpath = joinpath(mktempdir(), "t.csv.gz")
    write(gzpath, gz)
    @test Tables.getcolumn(A.File(gzpath), :a) == [1, 2]

    # typemap: detected types remap; user-pinned ones don't
    input = "a,b\n1,1.5\n2,2.5\n"
    f = A.File(IOBuffer(input); typemap=Dict(Int64 => Float64))
    @test f.a == [1.0, 2.0] && eltype(f.a) == Float64
    f = A.File(IOBuffer(input); typemap=Dict(Int64 => String), types=Dict(:b => Float64))
    @test eltype(Tables.getcolumn(f, :b)) == Float64
    @test Tables.getcolumn(f, :a) isa AbstractVector{<:AbstractString}
    # `Int` remains the portable spelling for the inferred integer type even
    # though 1.0 makes that inferred type Int64 on 32-bit Julia too.
    machineintmap = A.File(IOBuffer("a\n1\n2\n"); typemap=IdDict(Int => String), pool=false)
    @test machineintmap.a == ["1", "2"]
    # A mapped parse type is a fixed point. Downward and cyclic maps must widen
    # instead of retrying the same rejecting type until the driver guard fires.
    for (mappedinput, tm) in (("a\n1.5\n2.5\n", IdDict(Float64 => Int64)),
                              ("a\nx\ny\n", IdDict(String => Int64)),
                              ("a\n1\n2.5\n", IdDict(Int64 => Float64,
                                                       Float64 => Int64)))
        mapped = A.File(IOBuffer(mappedinput); typemap=tm, pool=false)
        @test String.(mapped.a) == split(chomp(mappedinput), '\n')[2:end]
    end
    chained = A.File(IOBuffer("a\n1\n2\n");
                     typemap=IdDict(Int64 => Float64, Float64 => String), pool=false)
    @test chained.a isa Vector{Float64}
    pinned = A.File(IOBuffer("a\n1\n2\n"); types=Int64,
                    typemap=IdDict(Int64 => String), pool=false)
    @test pinned.a isa Vector{Int64}
    mappedmissing = A.File(IOBuffer("a\n1\n\n2\n"); ignoreemptyrows=false,
                           typemap=IdDict(Int64 => Float64), pool=false)
    @test eltype(mappedmissing.a) == Union{Float64, Missing}
    mappedpool = A.File(IOBuffer("a\n" * join((string(i % 3) for i in 1:300), '\n') * "\n");
                        typemap=IdDict(Int64 => String), pool=true)
    @test mappedpool.a isa PooledArrays.PooledArray{String}

    # per-column dateformat
    input = "d1,d2\n03/04/2020,2020-01-02\n05/06/2021,2021-07-08\n"
    f = A.File(IOBuffer(input); dateformat=Dict(:d1 => "dd/mm/yyyy"))
    @test Tables.getcolumn(f, :d1) == [Date(2020, 4, 3), Date(2021, 6, 5)]
    @test Tables.getcolumn(f, :d2) == [Date(2020, 1, 2), Date(2021, 7, 8)]

    # per-column pool: Dict pools only the listed column
    input = "a,b\n" * join(("p$(i % 5),q$(i % 5)" for i in 1:5000), '\n') * "\n"
    f = A.File(IOBuffer(input); pool=Dict(:a => (1.0, 500)))
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    f = A.File(IOBuffer(input); pool=[(1.0, 500), false])
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    f = A.File(IOBuffer(input); pool=[(1.0, 500), nothing])
    @test Tables.getcolumn(f, :a) isa PooledArrays.PooledArray
    @test !(Tables.getcolumn(f, :b) isa PooledArrays.PooledArray)
    @test_throws ArgumentError A.File(IOBuffer(input); pool=Dict(:nope => true))
    @test_throws ArgumentError A.File(IOBuffer(input); pool=[true])
    @test_throws ArgumentError A.File(IOBuffer(input); pool=Dict(:a => "invalid"))

    # The pre-skip proof and parse-time degrade both bind the policy by column.
    proofinput = "a,b\n" * join(("unique$i,cat$(i % 3)" for i in 1:1000), '\n') * "\n"
    proof = A.File(IOBuffer(proofinput);
                   pool=Dict(:a => (1.0, 10), :b => (1.0, 500)))
    @test !(proof.a isa PooledArrays.PooledArray)
    @test proof.b isa PooledArrays.PooledArray
    degradeinput = "a,b\nx,u\ny,v\n"
    degraded = A.File(IOBuffer(degradeinput);
                      pool=Dict(:a => (1.0, 1), :b => (1.0, 2)))
    @test !(degraded.a isa PooledArrays.PooledArray)
    @test degraded.b isa PooledArrays.PooledArray

    # downcast chooses the narrowest signed integer type per column
    input = "a,b,c\n1,300,70000\n2,-40,100000\n"
    f = A.File(IOBuffer(input); downcast=true)
    @test f.a == Int8[1, 2] && eltype(f.a) == Int8
    @test f.b == Int16[300, -40] && eltype(f.b) == Int16
    @test f.c == Int32[70000, 100000] && eltype(f.c) == Int32
    # downcast with missings keeps Union eltype
    f = A.File(IOBuffer("a\n1\n\n2\n"); downcast=true, ignoreemptyrows=false)
    @test eltype(Tables.getcolumn(f, :a)) == Union{Int8, Missing}
    for (T, lo, hi) in ((Int8, typemin(Int8), typemax(Int8)),
                        (Int16, typemin(Int16), typemax(Int16)),
                        (Int32, typemin(Int32), typemax(Int32)),
                        (Int64, typemin(Int64), typemax(Int64)))
        c = A.File(IOBuffer("a\n$lo\n$hi\n"); downcast=true).a
        @test eltype(c) == T
        @test c == T[lo, hi]
    end
    @test A.File(IOBuffer("a\n\n"); downcast=true, ignoreemptyrows=false).a isa Vector{Missing}

    # transpose: names in field 1 and ragged rows are padded
    input = "name,1,2,3\nscore,1.5,2.5,3.5\nnote,x,y\n"
    f = A.File(IOBuffer(input); transpose=true)
    @test Tables.columnnames(Tables.columns(f)) == [:name, :score, :note]
    @test Tables.getcolumn(f, :name) == [1, 2, 3]
    @test Tables.getcolumn(f, :score) == [1.5, 2.5, 3.5]
    @test isequal(Tables.getcolumn(f, :note), ["x", "y", missing])
    @test Base.nonmissingtype(eltype(f.note)) == K.DataString
    f = A.File(IOBuffer("1,2\n3,4\n"); transpose=true, header=false)
    @test Tables.getcolumn(f, :Column1) == [1, 2] && Tables.getcolumn(f, :Column2) == [3, 4]
    @test_throws ArgumentError A.File(IOBuffer(input); transpose=true, select=[:name])
    @test_throws ArgumentError A.File(IOBuffer(input); transpose=true, ntasks=0)
    @test A.File(IOBuffer(input); transpose=true, ntasks=2, parallel=true).name == [1, 2, 3]
    paddedtranspose = A.File(IOBuffer("a, 1 , \" 2 \" \nb, 3.5 , 4.5 \n");
                               transpose=true)
    @test paddedtranspose.a == [1, 2]
    @test paddedtranspose.b == [3.5, 4.5]
    narrowtranspose = A.File(IOBuffer("a,1,128\n"); transpose=true,
                             types=Int8, maxproblems=1, on_error=:collect)
    @test isequal(collect(narrowtranspose.a), Union{Int8, Missing}[1, missing])
    @test length(A.problems(narrowtranspose)) == 1
    @test_throws CSV.ParseError A.File(IOBuffer("a,x\n"); transpose=true,
                                       types=Int8, strict=true, maxproblems=0)
    transposedstrings = "text,alpha,beta\nnums,1,2\n"
    @test eltype(A.File(IOBuffer(transposedstrings); transpose=true,
                        stringtype=String, pool=false).text) == String
    @test eltype(A.File(IOBuffer(transposedstrings); transpose=true,
                        stringtype=InlineString, pool=false).text) == String7
    pooledtranspose = A.File(IOBuffer("text,x,x,x\nnums,1,2,3\n");
                             transpose=true, pool=true)
    @test pooledtranspose.text isa PooledArrays.PooledArray
    @test collect(pooledtranspose.text) == ["x", "x", "x"]
    transposeddate = A.File(IOBuffer("d1,15/01/2023\nd2,2023.01.16\n");
                            transpose=true, types=Dict(:d1 => Date, :d2 => Date),
                            dateformat=Dict(:d1 => "dd/mm/yyyy",
                                            :d2 => "yyyy.mm.dd"))
    @test (transposeddate.d1[1], transposeddate.d2[1]) ==
          (Date(2023, 1, 15), Date(2023, 1, 16))
    # Transposed reads index and parse in parallel; columns, types, and
    # problems match the sequential read.
    let io = IOBuffer()
        for r in 1:64
            print(io, "col", r)
            for c in 1:300
                print(io, ",", r == 7 && c == 150 ? "x" : string(r * c % 97))
            end
            println(io)
        end
        wide = take!(io)
        problemsof(f) = [(p.row, p.col, p.kind) for p in A.problems(f)]
        seq = A.File(copy(wide); transpose=true, parallel=false, on_error=:collect)
        par = A.File(copy(wide); transpose=true, parallel=true, ntasks=4, on_error=:collect)
        @test Tables.columnnames(seq) == Tables.columnnames(par)
        @test all(j -> eltype(Tables.getcolumn(seq, j)) == eltype(Tables.getcolumn(par, j)), 1:64)
        @test all(j -> isequal(collect(Tables.getcolumn(seq, j)), collect(Tables.getcolumn(par, j))), 1:64)
        @test eltype(seq.col7) == A.DataString && eltype(seq.col8) == Int64
        seqt = A.File(copy(wide); transpose=true, parallel=false, types=Int64, on_error=:collect)
        part = A.File(copy(wide); transpose=true, parallel=true, ntasks=3, types=Int64, on_error=:collect)
        @test problemsof(seqt) == problemsof(part) == [(150, 7, :invalid_value)]
        @test isequal(collect(part.col7), collect(seqt.col7))
        @test ismissing(part.col7[150]) && part.col7[149] == 7 * 149 % 97
        @test_throws ArgumentError A.File(copy(wide); transpose=true, ntasks=0)
    end
    # Quoted newlines, escapes, empty rows, unicode, ragged tails, and pinned
    # types retain exact names and values across in-memory source modes.
    transposedcases = [
        ("name,\"a\nb\",c\nnum,1,2\n", (;), [:name, :num], Any[Any["a\nb", "c"], Any[1, 2]]),
        ("name,\"a\"\"b\",c\nnum,1,2\n", (;), [:name, :num], Any[Any["a\"b", "c"], Any[1, 2]]),
        ("a,1,2\n\nb,3,4\n", (;), [:a, :b], Any[Any[1, 2], Any[3, 4]]),
        ("α,β,γ\nδ,日,月\n", (;), [:α, :δ], Any[Any["β", "γ"], Any["日", "月"]]),
        ("a,1,x,3\nb,2020-01-01,2020-01-02,\n",
         (; types=Dict(:a => Int64, :b => Date)), [:a, :b],
         Any[Any[1, missing, 3], Any[Date(2020, 1, 1), Date(2020, 1, 2), missing]]),
    ]
    for (transposedinput, transposedkw, expectednames, expectedvalues) in transposedcases
        tf = sourceparity(transposedinput; kw=(; transpose=true, transposedkw...))
        actualnames, actualvalues = colvalues(tf)
        @test actualnames == expectednames
        @test isequal(actualvalues, expectedvalues)
    end
    # limit scopes inference as well as output. Values after the retained
    # prefix cannot promote an Int column to Float64 or String.
    limited = A.File(IOBuffer("a,1,x\n"); transpose=true, limit=1)
    @test limited.a == [1] && eltype(limited.a) == Int64
    limited = A.File(IOBuffer("a,1,2.5\n"); transpose=true, limit=1, downcast=true)
    @test limited.a == Int8[1] && eltype(limited.a) == Int8
    zero = A.File(IOBuffer("a,1,2.5\nb,3,4\n"); transpose=true, limit=0)
    @test length(zero) == 0 && all(eltype(Tables.getcolumn(zero, nm)) == Missing
                                  for nm in Tables.columnnames(zero))
    numbered = A.File(IOBuffer("skip,a,1,2,x\nskip,b,3,4,5\n");
                        transpose=true, header=2, skipto=3, limit=2)
    @test numbered.a == [1, 2] && numbered.b == [3, 4]
    explicit = A.File(IOBuffer("1,2,x\n3,4,5\n");
                        transpose=true, header=[:a, :b], limit=2)
    @test explicit.a == [1, 2] && explicit.b == [3, 4]
    ragged = A.File(IOBuffer("a,1,2,3\nb,4\n"); transpose=true, limit=2)
    @test ragged.a == [1, 2] && isequal(ragged.b, [4, missing])
    @test A.File(IOBuffer("a,1,2\n"); transpose=true, limit=99).a == [1, 2]
    huge = big(typemax(Int)) + 1
    unlimited = A.File(IOBuffer("a,1,2\nb,3,4\n"); transpose=true, limit=huge)
    @test unlimited.a == [1, 2] && unlimited.b == [3, 4]
    @test isempty(A.File(IOBuffer("a,1,2\nb,3,4\n"); transpose=true,
                         skipto=huge))
    @test isempty(A.File(IOBuffer("a,1,2\nb,3,4\n"); transpose=true,
                         header=huge))
    @test isempty(A.File(IOBuffer("a,1,2\nb,3,4\n"); transpose=true,
                         header=huge, skipto=huge + 1))
    @test_throws ArgumentError A.File(IOBuffer("a,1\n"); transpose=true,
                                      header=huge, skipto=huge)
    @test_throws ArgumentError A.File(IOBuffer("a,1\n"); transpose=true, limit=-1)

    # Removed kwargs error with migration text.
    for (kwname, kwval) in ((:silencewarnings, true), (:debug, true), (:lazystrings, true),
                            (:tasks, 2), (:threaded, true), (:rows_to_check, 5),
                            (:lines_to_check, 5), (:ignoreemptylines, true),
                            (:datarow, 2), (:type, Int64), (:missingstrings, ["NA"]),
                            (:dateformats, Dict(:a => "yyyy-mm-dd")),
                            (:parsingdebug, true))
        err = try
            A.File(IOBuffer("a\n1\n"); kwname => kwval)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError && occursin("removed in 1.0", err.msg)
    end

    # reusebuffer: accepted and inert
    r = A.Rows(IOBuffer("a\n1\n2\n"); reusebuffer=true)
    @test length(collect(r)) == 2

    # validate=false ignores types/dateformat/pool keys that name absent
    # columns. The default validates them, including Regex misses.
    novalidate = A.File(IOBuffer("a,b,c\n1,2,3\n"); types=Dict(4 => Float64, r"_x$" => Int8),
                        dateformat=Dict(:e => "dd/mm/yyyy"), pool=Dict("f" => true),
                        validate=false)
    @test length(novalidate) == 1 && novalidate.a == [1]
    @test_throws ArgumentError A.File(IOBuffer("a,b,c\n1,2,3\n"); types=Dict(r"_x$" => Int8))
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); types=Dict(r"z" => Int), limit=0)
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); dateformat=Dict(r"z" => "yyyy"))
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); pool=Dict(r"z" => true))
    @test_throws ArgumentError A.Rows(IOBuffer("a\n1\n"); types=Dict(:zz => Int))
    @test first(A.Rows(IOBuffer("a\n1\n"); types=Dict(:zz => Int), validate=false)).a isa AbstractString
    @test first(A.Chunks(IOBuffer("a\n1\n"); types=Dict(:zz => Int), validate=false, chunkbytes=1 << 20))[:a] == [1]
    @test_throws ArgumentError A.Rows(IOBuffer("a\n1\n"); dateformat=Dict(r"z" => "yyyy"))
    @test_throws ArgumentError A.Chunks(IOBuffer("a\n1\n"); types=Dict(r"z" => Int))
    @test_throws ArgumentError A.File(IOBuffer("a,1\n"); transpose=true,
                                      types=Dict(r"z" => Int))
    @test_throws ArgumentError K.parse(Vector{UInt8}("a\n1\n"); types=Dict(r"z" => Int))
    @test length(A.File(IOBuffer("a,1\n"); transpose=true,
                        types=Dict(:z => Int, 2 => Int, r"q" => Int), validate=false)) == 1
    @test K.parse(Vector{UInt8}("a\n1\n");
                  types=Dict(:z => Int, 2 => Int, r"q" => Int), validate=false).nrows == 1
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); select=[:z], validate=false)
    @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); drop=[:z], validate=false)
    # An absent integer key can exceed machine Int. validate=false still
    # ignores it; normal validation reports the same range error as a small key.
    for option in (:types, :dateformat, :pool)
        spec = Dict(big(typemax(Int)) + 1 => (option === :types ? Int :
                                             option === :dateformat ? "yyyy" : true))
        @test A.File(IOBuffer("a\n1\n"); option => spec, validate=false).a == [1]
        @test_throws ArgumentError A.File(IOBuffer("a\n1\n"); option => spec)
    end
end

@testset "DataString hash + stringtype extension hook" begin
    # hash contract: DataString hashes like its String, allocation-free
    col = K.parse(Vector{UInt8}(codeunits("a\n" * join(("v$(i)_" * "x"^(i % 30) for i in 1:500), '\n') * "\n"))).columns[1]
    @test all(hash(col[i]) == hash(String(col[i])) for i in eachindex(col))
    @test all(hash(col[i], UInt(7)) == hash(String(col[i]), UInt(7)) for i in eachindex(col))
    @test allochashall(col) == 0
    for n in 0:14   # every inline length + the first view lengths, incl. escaped
        s = "y"^n
        c = K.parse(Vector{UInt8}(codeunits("a\n\"" * s * "\"\n\"" * s * "\"\"z\"\n"))).columns[1]
        @test hash(c[1]) == hash(s) && hash(c[2]) == hash(s * "\"z")
    end
    d = Dict{AbstractString, Int}(String(col[3]) => 3)
    @test d[col[3]] == 3                    # DataString finds the String key
    # stringtype validation and String path unchanged
    @test_throws ArgumentError A.File(IOBuffer("a\nx\n"); stringtype=Int)
    @test eltype(Tables.getcolumn(A.File(IOBuffer("a\nx\n"); stringtype=String), :a)) == String
end

@testset "InlineStrings extension" begin
    IE = Base.get_extension(CSV, :CSVInlineStringsExt)
    csv = "s,t,n\n" * join(("a$(i),$(i % 3 == 0 ? "" : "longer value number $(i)"),$(i)" for i in 1:300), '\n') * "\n"
    auto = A.File(IOBuffer(csv); stringtype=InlineString)
    @test eltype(Tables.getcolumn(auto, :s)) == String7           # auto width per column
    @test eltype(Tables.getcolumn(auto, :t)) == Union{Missing, String31}
    @test String(Tables.getcolumn(auto, :t)[1]) == "longer value number 1"
    @test Tables.getcolumn(auto, :t)[3] === missing
    f = A.File(IOBuffer(csv); stringtype=String31)
    @test eltype(Tables.getcolumn(f, :s)) == String31           # pinned width
    @test_throws ArgumentError A.File(IOBuffer(csv); stringtype=String7)   # too narrow
    widths = (String1, String3, String7, String15, String31, String63, String127, String255)
    for T in widths
        n = IE._capacity(T)
        value = "x"^n
        @test String(only(A.File(IOBuffer("s\n$value\n"); stringtype=T, pool=false).s)) == value
        @test_throws ArgumentError A.File(IOBuffer("s\n" * "x"^(n + 1) * "\n");
                                          stringtype=T, pool=false)
    end
    # the auto width stops at String31 (0.10's rule): wider text is String, so a
    # valid file never fails to read because of its text width
    @test eltype(A.File(IOBuffer("s\n" * "x"^256 * "\n");
                        stringtype=InlineString, pool=false).s) == String
    @test eltype(A.File(IOBuffer("s\n" * "x"^32 * "\n");
                        stringtype=InlineString, pool=false).s) == String
    @test eltype(A.File(IOBuffer("s\n" * "x"^31 * "\n");
                        stringtype=InlineString, pool=false).s) == String31
    # an explicit `types=String` names the output type even under an inline
    # stringtype; an inferred empty or all-missing text column takes String1
    empty = A.File(IOBuffer("s\n"); types=String, stringtype=InlineString, pool=false)
    @test isempty(empty.s) && eltype(empty.s) == String
    allmissing = A.File(IOBuffer("id,s\n1,\n2,\n"); types=Dict(:s => String),
                        stringtype=InlineString, pool=false)
    @test eltype(allmissing.s) == Union{Missing, String}
    @test all(ismissing, allmissing.s)
    inferredmissing = A.File(IOBuffer("id,s\n1,\n2,\n"); types=Dict(:s => A.DataString),
                             stringtype=InlineString, pool=false)
    @test eltype(inferredmissing.s) == Union{Missing, A.DataString}
    # pooled levels take the inline type; missing joins the pool
    f = A.File(IOBuffer(csv); stringtype=InlineString, pool=Dict(:s => false, :t => (1.0, 5000)))
    pooled = Tables.getcolumn(f, :t)
    @test pooled isa PooledArrays.PooledArray
    @test eltype(pooled) == Union{Missing, String31}
    @test all(getfield(pooled, :invpool)[getfield(pooled, :pool)[i]] == UInt32(i)
              for i in eachindex(getfield(pooled, :pool)))
    # escaped (extra-backed) values and unicode
    escaped = A.File(IOBuffer("a\n\"q\"\"x\"\n\"a long \"\"escaped\"\" value\"\nαβγδεζηθ\n");
                     stringtype=InlineString)
    @test collect(String.(Tables.getcolumn(escaped, :a))) ==
          ["q\"x", "a long \"escaped\" value", "αβγδεζηθ"]
    @test eltype(Tables.getcolumn(escaped, :a)) == String31
    @test all(i -> String(auto.s[i]) == "a$i", eachindex(auto.s))
    @test all(i -> i % 3 == 0 ? ismissing(auto.t[i]) :
                   String(auto.t[i]) == "longer value number $i", eachindex(auto.t))
end

@testset "RowWriter" begin
    t = (a=[1, 2, 3], b=["x", "y,z", missing], c=[1.5, 2.0, 3.25],
         d=[Date(2024, 1, 2), Date(2024, 3, 4), Date(2024, 5, 6)])
    lines = collect(CSV.RowWriter(t))
    @test length(lines) == 4
    @test lines[1] == "a,b,c,d\n"
    @test lines[3] == "2,\"y,z\",2.0,2024-03-04\n"
    io = IOBuffer(); CSV.write(io, t)
    @test join(lines) == String(take!(io))                    # byte-identical to write
    @test collect(CSV.RowWriter(t; writeheader=false))[1] == "1,x,1.5,2024-01-02\n"
    @test collect(CSV.RowWriter(t; header=["p", "q", "r", "s"]))[1] == "p,q,r,s\n"
    @test collect(CSV.RowWriter(t; delim=';', quotestyle=:all, floatformat="%.1f"))[2] ==
          "1;\"x\";1.5;2024-01-02\n"
    @test_throws ArgumentError collect(CSV.RowWriter(t; header=["only", "three", "names"]))
    # streams over a row-access table (a File) without materializing columns
    f = A.File(IOBuffer("a,b\n1,x\n2,y\n"))
    @test collect(CSV.RowWriter(f)) == ["a,b\n", "1,x\n", "2,y\n"]
    @test join(collect(CSV.RowWriter((x=[1, 2], y=["ab", "c,d"])))) ==
          "x,y\n1,ab\n2,\"c,d\"\n"
end

@testset "stringtype × pool matrix: File / Rows / Chunks agree" begin
    csv = "a,b,n\n" * join(("p$(i % 4),v$(i),$(i)" for i in 1:2000), '\n') * "\n"
    # File: (stringtype, pool) -> (a is pooled?, eltype of a, eltype of b)
    expect = Dict(
        (K.DataString, true)  => (true,  String,           K.DataString),
        (K.DataString, false) => (false, K.DataString,  K.DataString),
        (String, true)           => (true,  String,           String),
        (String, false)          => (false, String,           String),
        (InlineString, true)     => (true,  String3,          String7),
        (InlineString, false)    => (false, String3,          String7),
    )
    for ((st, pl), (pooled, ea, eb)) in expect
        pool = pl ? (0.2, 500) : false
        f = A.File(IOBuffer(csv); stringtype=st, pool)
        ca, cb = Tables.getcolumn(f, :a), Tables.getcolumn(f, :b)
        @test (ca isa PooledArrays.PooledArray) == pooled
        @test eltype(ca) == ea && eltype(cb) == eb
        @test String(ca[5]) == "p1" && String(cb[5]) == "v5"
        # Chunks batches use the same final steps.
        c = first(A.Chunks(IOBuffer(csv); ntasks=2, stringtype=st, pool))
        @test (Tables.getcolumn(c, :a) isa PooledArrays.PooledArray) == pooled
        @test eltype(Tables.getcolumn(c, :a)) == ea && eltype(Tables.getcolumn(c, :b)) == eb
    end
    # An auto width settles once for the whole Chunks window: batches whose
    # own longest value is shorter still use the window's width, so the
    # schema is stable and equals the File schema.
    widths = "s\n" * join((i % 50 == 0 ? "a much longer value $i" : "v$i" for i in 1:400), '\n') * "\n"
    chunks = A.Chunks(IOBuffer(widths); stringtype=InlineString, chunkbytes=64)
    @test count(_ -> true, chunks) > 3
    @test unique(eltype(b.s) for b in chunks) == [String31]
    @test eltype(A.File(IOBuffer(widths); stringtype=InlineString).s) == String31
    @test occursin("s::String31", sprint(show, chunks))
    escapedwidths = "s\n\"a\"\"b\"\nxy\n\"a\"\"b\"\"c\"\"d\"\n"
    @test unique(eltype(b.s) for b in A.Chunks(IOBuffer(escapedwidths);
                                                   stringtype=InlineString, chunkbytes=4)) == [String7]
    toowide = "s\n" * join((i == 300 ? "x"^40 : "v$i" for i in 1:400), '\n') * "\n"
    @test unique(eltype(b.s) for b in A.Chunks(IOBuffer(toowide);
                                                   stringtype=InlineString, chunkbytes=64)) == [String]
    # a promoted column (numbers, then text) settles on the widest of both
    promoted = "s\n" * join((i < 380 ? string(10i) : "t$i" for i in 1:400), '\n') * "\n"
    @test unique(eltype(b.s) for b in A.Chunks(IOBuffer(promoted);
                                                   stringtype=InlineString, chunkbytes=64)) == [String7]
    # a fixed width request is unchanged and errors on an over-long value
    @test unique(eltype(b.s) for b in A.Chunks(IOBuffer(widths); stringtype=String31, chunkbytes=64)) == [String31]
    @test_throws ArgumentError collect(A.Chunks(IOBuffer(widths); stringtype=String7, chunkbytes=64))
    # Rows: lazy views by default; stringtype materializes per cell
    r = first(A.Rows(IOBuffer(csv)))
    @test r[:a] isa K.DataString && r.b isa K.DataString
    @test first(A.Rows(IOBuffer(csv); stringtype=String))[:a] isa String
    @test first(A.Rows(IOBuffer(csv); stringtype=InlineString))[:b] isa String3   # per-cell smallest fit ("v1")
    @test first(A.Rows(IOBuffer(csv); stringtype=String15))[:a] isa String15
    @test Tables.schema(A.Rows(IOBuffer(csv); stringtype=String)).types[1] == Union{String, Missing}
    @test_throws ArgumentError A.Rows(IOBuffer(csv); stringtype=Int)
    # Auto-width is per column for File and per accessed cell for Rows.
    asymmetry = "s\na\nabcdef\n"
    @test eltype(A.File(IOBuffer(asymmetry); stringtype=InlineString, pool=false).s) == String7
    rowvalues = [row.s for row in A.Rows(IOBuffer(asymmetry); stringtype=InlineString)]
    @test rowvalues[1] isa String1 && rowvalues[2] isa String7
    @test_throws ArgumentError first(A.Rows(IOBuffer("s\nab\n"); stringtype=String1)).s
    # Plain views retain the input buffer. Long escaped cells own the unescaped buffer.
    plainrow = first(A.Rows(IOBuffer("s\nabcdefghijklmnop\n")))
    plainbuf = getfield(getfield(getfield(plainrow, :view), :r), :buf)
    plainvalue = plainrow.s
    @test getfield(plainvalue, :data) === plainbuf
    escapedrow = first(A.Rows(IOBuffer("s\n\"abcdefghij\"\"klmnop\"\n")))
    inputbuf = getfield(getfield(getfield(escapedrow, :view), :r), :buf)
    escapedvalue = escapedrow.s
    @test String(escapedvalue) == "abcdefghij\"klmnop"
    @test getfield(escapedvalue, :data) !== inputbuf
    owned = WeakRef(getfield(escapedvalue, :data))
    GC.gc(true)
    @test owned.value !== nothing && String(escapedvalue) == "abcdefghij\"klmnop"
    @test first(A.Rows(IOBuffer("s\nabcdefghijklmnop\n"); types=String)).s isa String
end

@testset "Chunks: schema stable across batches" begin
    # a promotion that only appears late must not change the batch schema
    data = "a,b\n" * join(("$(i),value$(i)" for i in 1:40), '\n') * "X"
    chunks = collect(A.Chunks(IOBuffer(data); ntasks=2, pool=false))
    nrows(t) = length(t)
    @test sum(nrows, chunks) == 40
    @test String(last(Tables.getcolumn(last(chunks), :b))) == "value40X"
    # ints then a string in the last batch: every batch reports the widened type
    late = "x\n" * join(string.(1:6000), '\n') * "\nfinal\n"
    cs = collect(A.Chunks(IOBuffer(late); ntasks=4, chunkbytes=8_000, pool=false))
    @test length(cs) >= 2
    types = unique(eltype(Tables.getcolumn(c, :x)) for c in cs)
    @test length(types) == 1 && Base.nonmissingtype(types[1]) <: AbstractString
    # Pooling must preserve the whole-file missing-capable schema in batches
    # that do not themselves contain a missing value.
    pooledinput = "id,s\n" *
                  join(("$(i)," * (i == 60 ? "" : "x") for i in 1:120), '\n') * "\n"
    pooledchunks = collect(A.Chunks(IOBuffer(pooledinput); chunkbytes=64, pool=true))
    @test length(pooledchunks) > 1
    @test all(Tables.getcolumn(c, :s) isa PooledArrays.PooledArray for c in pooledchunks)
    @test all(eltype(Tables.getcolumn(c, :s)) == Union{Missing, String} for c in pooledchunks)
    @test any(any(ismissing, Tables.getcolumn(c, :s)) for c in pooledchunks)
    @test any(!any(ismissing, Tables.getcolumn(c, :s)) for c in pooledchunks)
    # Ratio and cap apply to each batch's own rows and levels.
    twolvl = "s\n" * join((isodd(i) ? "x" : "y" for i in 1:20), '\n') * "\n"
    threelvl = "s\n" * join((string(Char(Int('x') + (i % 3))) for i in 1:21), '\n') * "\n"
    @test first(A.Chunks(IOBuffer(twolvl); chunkbytes=1 << 20,
                         pool=(1.0, 2)))[:s] isa PooledArrays.PooledArray
    @test !(first(A.Chunks(IOBuffer(threelvl); chunkbytes=1 << 20,
                           pool=(1.0, 2)))[:s] isa PooledArrays.PooledArray)
    # Generated gzip data covers magic-byte decompression without a test artifact.
    generatedrows = 70_000
    gziptext = "id,value\n" *
               join(("$i,v$(i % 17)" for i in 1:generatedrows), '\n') * "\n"
    gzipdata = transcode(CodecZlib.GzipCompressor,
                         Vector{UInt8}(codeunits(gziptext)))
    compressedchunks = collect(A.Chunks(gzipdata; ntasks=2, chunkbytes=64 * 1024))
    @test length(compressedchunks) > 1
    @test sum(nrows, compressedchunks) == generatedrows
    ids = reduce(vcat, (collect(c.id) for c in compressedchunks))
    values = reduce(vcat, (String.(c.value) for c in compressedchunks))
    @test ids == Int64.(1:generatedrows)
    @test values == ["v$(i % 17)" for i in 1:generatedrows]
end

@testset "wide batches merge diagnostics in source order" begin
    ncols = 64
    header = join(("c$j" for j in 1:ncols), ',') * "\n"
    for nrows in (20, 1025), parallel in (false, true), cap in (0, 1, 7, 100)
        input = header * (join(fill("bad", ncols), ',') * "\n")^nrows
        f = first(A.Chunks(IOBuffer(input); types=Int64, chunkbytes=1 << 20,
                            ntasks=4, parallel, maxproblems=cap, on_error=:collect))
        expected = [(1 + (i - 1) ÷ ncols, 1 + (i - 1) % ncols) for i in 1:cap]
        @test [(p.row, p.col) for p in A.problems(f)] == expected
        @test getfield(f, :table).droppedproblems == nrows * ncols - cap
        @test all(column -> all(ismissing, column), Tables.Columns(f))
    end
end

@testset "vector of sources + source= column" begin
    data = ["a,b,c\n1,2,3\n4,5,6\n", "a,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]
    f = A.File(map(IOBuffer, data))
    @test length(f) == 6 && f.a == [1, 4, 7, 10, 13, 16]
    # element types promote across sources
    f = A.File(map(IOBuffer, ["a\n1\n", "a\n2.5\n"]))
    @test eltype(Tables.getcolumn(f, :a)) == Float64 && f.a == [1.0, 2.5]
    mixedinputs = [Vector{UInt8}("a\n1\n"), Vector{UInt8}("a\nlong-value\n")]
    f = A.File(mixedinputs; pool=false)
    @test eltype(f.a) == Any && isequal(f.a, Any[1, "long-value"])
    @test f.a[2] isa String
    fill!(mixedinputs[2], UInt8('z'))
    @test f.a[2] == "long-value"
    # a source missing a column missing-fills it; its extra columns are ignored
    shifted = ["a,b,c\n1,2,3\n4,5,6\n", "a2,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]
    f = A.File(map(IOBuffer, shifted))
    @test Tables.columnnames(Tables.columns(f)) == [:a, :b, :c]
    @test isequal(collect(Tables.getcolumn(f, :a)), [1, 4, missing, missing, 13, 16])
    # string columns concatenate as String (concatenation owns its memory)
    f = A.File(map(IOBuffer, ["a,b\nx,1\n", "a,b\n,2\n"]))
    @test eltype(Tables.getcolumn(f, :a)) == Union{Missing, String}
    @test isequal(collect(Tables.getcolumn(f, :a)), ["x", missing])
    pooled = A.File(map(IOBuffer, ["a\nx\nx\n", "a\ny\ny\n"]); pool=true)
    @test pooled.a isa PooledArrays.PooledArray
    @test eltype(pooled.a) == String && collect(pooled.a) == ["x", "x", "y", "y"]
    # The concatenated strings own their bytes, including pooled levels.
    ownedinputs = [Vector{UInt8}("a\nlong-value-one\n"),
                   Vector{UInt8}("a\nlong-value-two\n")]
    owned = A.File(ownedinputs; pool=true)
    fill!(ownedinputs[1], UInt8('z'))
    fill!(ownedinputs[2], UInt8('z'))
    @test collect(owned.a) == ["long-value-one", "long-value-two"]
    # source= appends a pooled provenance column; labels are deterministic
    f = A.File(map(IOBuffer, data); source=:origin)
    col = Tables.getcolumn(f, :origin)
    @test col isa PooledArrays.PooledArray && eltype(col) == String
    @test collect(col) == ["<source 1>", "<source 1>", "<source 2>", "<source 2>",
                           "<source 3>", "<source 3>"]
    f = A.File(map(IOBuffer, data); source="origin" => [10, 20, 30])
    @test collect(Tables.getcolumn(f, :origin)) == [10, 10, 20, 20, 30, 30]
    labels = Union{Missing, Int}[1, missing, 3]
    f = A.File(map(IOBuffer, data); source=:origin => labels)
    @test eltype(f.origin) == Union{Missing, Int}
    @test isequal(collect(f.origin), [1, 1, missing, missing, 3, 3])
    # path sources label with the path; single-element vectors keep the column
    mktempdir() do tmp
        p1, p2 = joinpath(tmp, "x.csv"), joinpath(tmp, "y.csv")
        write(p1, "a\n1\n"); write(p2, "a\n2\n")
        f = A.File([p1, p2]; source=:src)
        @test collect(Tables.getcolumn(f, :src)) == [p1, p2]
        f = A.File([p1]; source=:src)
        @test collect(Tables.getcolumn(f, :src)) == [p1]
    end
    # per-file problems merge with row offsets
    f = A.File(map(IOBuffer, ["a,b\n1,2\n", "a,b\n3,4,5\n"]); on_error=:collect)
    @test length(A.problems(f)) == 1 && A.problems(f)[1].row == 2
    f = A.File(map(IOBuffer, ["a\n1\n", "a\n\"x\n"]); pool=false, on_error=:collect)
    @test any(p -> p.kind == :invalid_quoted_field && p.row == 2, A.problems(f))
    @test any(p -> p.kind == :unclosed_quote && p.row == 0, A.problems(f))
    # Column names may shadow File's implementation fields. Concatenation must
    # use getfield for its own state and keep column-first public access.
    for nm in ("table", "lookup")
        f = A.File([IOBuffer("$nm\n1\n"), IOBuffer("$nm\n2\n")])
        @test collect(f[Symbol(nm)]) == [1, 2]
    end
    invalidsources = [IOBuffer("a\nBAD\nNOPE\n"), IOBuffer("a\nWRONG\nFAIL\n")]
    capped = A.File(invalidsources; types=Int64, maxwarnings=2, maxproblems=1, on_error=:collect)
    @test length(A.problems(capped)) == 1
    @test A.problems(capped)[1].row == 1
    @test getfield(capped, :table).droppedproblems == 3
    @test_throws CSV.ParseError A.File(
        [IOBuffer("a\nBAD\n"), IOBuffer("a\nWRONG\n")];
        types=Int64, strict=true, maxproblems=0)
    # kwargs apply per source
    f = A.File(map(IOBuffer, data); select=["a"], types=Dict(:a => Float64))
    @test Tables.columnnames(Tables.columns(f)) == [:a] && f.a == [1.0, 4.0, 7.0, 10.0, 13.0, 16.0]
    @test A.File(map(IOBuffer, data); limit=1).a == [1, 7, 13]
    readback = CSV.read(map(IOBuffer, data), Tables.columntable; limit=1)
    @test readback.a == [1, 7, 13]
    # Byte buffers are still read as one source.
    @test A.File(Vector{UInt8}("a\n1\n")).a == [1]
    @test A.File(codeunits("a\n1\n")).a == [1]
    bytes = Vector{UInt8}("xa\n1\ny")
    byteview = @view bytes[2:end-1]
    @test A.File(byteview).a == [1]
    # errors: empty vector, label-length mismatch, name collision, bad source form
    @test_throws ArgumentError A.File(IOBuffer[])
    @test_throws ArgumentError A.File(map(IOBuffer, data); source="s" => [1, 2])
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=:a)
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=1)
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=:s => (1, 2, 3))
    @test_throws ArgumentError A.File(map(IOBuffer, data); source=1 => [1, 2, 3])
    @test_throws ArgumentError A.File(Vector{UInt8}("a\n1\n"); source=:src)
    @test_throws ArgumentError A.File(codeunits("a\n1\n"); source=:src)
    @test_throws ArgumentError A.File(byteview; source=:src)
    # all-missing everywhere stays Missing eltype
    f = A.File(map(IOBuffer, ["a,b\n1,\n", "a,b\n2,\n"]))
    @test eltype(Tables.getcolumn(f, :b)) == Missing
    # Empty and Missing-eltype pieces preserve their schema contribution. The
    # copy into the promoted Union final converts values and fills absent blocks.
    f = A.File(map(IOBuffer, ["a\n", "a\n1\n"]); pool=false)
    @test eltype(f.a) == Union{Missing, Int64} && f.a == [1]
    f = A.File(map(IOBuffer, ["a\n1\n\n", "a\n2.5\n"]);
               pool=false, ignoreemptyrows=false)
    @test eltype(f.a) == Union{Missing, Float64}
    @test isequal(f.a, [1.0, missing, 2.5])
    # A distinct empty Union{} piece is not the absent-column sentinel. Guard
    # the vacuous `Union{} <: AbstractString` relation in both chain passes.
    unionempty = Union{}[]
    chained = A._chaincolumn(AbstractVector[unionempty, Int64[1]], [0, 1], 1)
    @test chained == [1] && eltype(chained) == Int64
    f = A.File(map(IOBuffer, ["a,b\n1,2\n", "a\n3\n"]); pool=false)
    @test isequal(f.b, [2, missing])
    f = A.File(map(IOBuffer, ["", "a\n1\n"]); pool=false)
    @test isempty(Tables.columnnames(f)) && length(f) == 1
end

@testset "FilePathsBase extension" begin
    mktempdir() do tmp
        write(joinpath(tmp, "in.csv"), "x,y\n1,2\n3,4\n")
        p = joinpath(FilePathsBase.Path(tmp), "in.csv")
        f = A.File(p)
        @test f.x == [1, 3] && f.y == [2, 4]
        @test getfield(f, :name) == string(p)
        @test getfield(A.lazy(p), :name) == string(p)
        @test collect(A.File([p, p]; source=:src).src) == [string(p), string(p),
                                                           string(p), string(p)]
        @test first(A.Rows(p)).x isa AbstractString
        @test first(A.Chunks(p; chunkbytes=1 << 20))[:x] == [1, 3]
        @test CSV.read(p, Tables.columntable).x == [1, 3]
        # writer sink + compress=:auto by extension
        out = joinpath(FilePathsBase.Path(tmp), "out.csv")
        CSV.write(out, (a=[1],))
        CSV.write(out, (a=[2],); append=true)
        @test read(joinpath(tmp, "out.csv"), String) == "a\n1\n2\n"
        gz = joinpath(FilePathsBase.Path(tmp), "out.csv.gz")
        CSV.write(gz, (a=[1],))
        CSV.write(gz, (a=[2],); append=true)
        @test A.File(gz).a == [1, 2]
        @test first(A.Rows(gz)).a == "1"
        @test first(A.Chunks(gz; chunkbytes=1 << 20))[:a] == [1, 2]
        # A large AbstractPath takes the mmap branch and accepts both source
        # controls through the extension method.
        big = joinpath(FilePathsBase.Path(tmp), "big.csv")
        open(string(big), "w") do io
            write(io, "x,y\n")
            for i in 1:140_000
                print(io, i, ',', i + 1, '\n')
            end
        end
        @test filesize(string(big)) >= A.MMAP_THRESHOLD
        let mapped = A.File(big; prefetch=false, limit=2)
            @test mapped.x == [1, 2]
        end
        @test A.File(big; buffer_in_memory=true, limit=2).y == [2, 3]
        GC.gc(true)
    end
end

@testset "Multiple sources release completed diagnostic logs" begin
    inputs = ["a\n1\nbad-first\n", "a\nbad-second\n", "a\n3\nbad-third\n"]
    for cap in (0, 1, 3), parallel in (false, true)
        f = A.File(map(IOBuffer, inputs); types=Int, maxproblems=cap, parallel, on_error=:collect)
        @test getfield.(A.problems(f), :row) == [2, 3, 5][1:cap]
        @test getfield(f, :table).droppedproblems == 3 - cap
        err = try
            A.File(map(IOBuffer, inputs); types=Int, maxproblems=cap, parallel, on_error=:error)
            nothing
        catch e
            e
        end
        @test err isa A.ParseError
        @test err.problem.row == 2 && occursin("bad-first", err.problem.message)
        @test err.nproblems == 3
    end
    # Complete sources in reverse order. At each completion, the source no
    # longer retains its log and the shared reservoir stays within its cap.
    pending = A.PendingProblemLog(1)
    for i in reverse(eachindex(inputs))
        f = A.File(IOBuffer(inputs[i]); types=Int, on_error=:collect)
        clean = A._takefileproblems(f, pending, i)
        @test isempty(A.problems(clean))
        @test getfield(clean, :table).droppedproblems == 0
        @test isequal(clean.a, f.a)
        @test length(pending.items) <= 1
    end
    @test pending.dropped == 2
    @test only(pending.items).chunk == 1
    @test occursin("bad-first", only(pending.items).problem.message)
end

@testset "Multiple sources share a bounded task budget" begin
    inputs = [Vector{UInt8}("a\n" * join(1:50, '\n') * "\n") for _ in 1:8]
    for ntasks in (1,2), parallel in (false,true)
        empty!(API_PARSE_TASKS)
        result = A.File(inputs; types=APITaskScalar, ntasks, parallel, delim=',')
        @test getfield.(result.a, :value) == repeat(collect(1:50), 8)
        budget = parallel ? min(ntasks, Threads.nthreads()) : 1
        @test 1 <= length(API_PARSE_TASKS) <= budget
    end
end

@testset "ParseError, on_error=:warn, and problem display" begin
    bad = "a\n1\nx\n"
    err = try
        A.File(IOBuffer(bad); types=Int, on_error=:error)
        nothing
    catch e
        e
    end
    @test err isa CSV.ParseError
    @test err.problem.kind == :invalid_value && err.problem.row == 2 && err.nproblems == 1
    @test err.source == "<GenericIOBuffer>"
    msg = sprint(showerror, err)
    @test occursin("CSV.ParseError: invalid_value at data row 2, column 1", msg)
    @test occursin("on_error=:collect", msg)
    @test_throws CSV.ParseError A.File(IOBuffer(bad); types=Int, strict=true)
    @test_throws CSV.ParseError A.File(IOBuffer(bad); types=Int, strict=true, transpose=false)
    @test_throws CSV.ParseError first(A.Chunks(IOBuffer(bad); types=Int, on_error=:error))
    @test_throws CSV.ParseError A.File([IOBuffer(bad), IOBuffer(bad)]; types=Int, on_error=:error)
    @test_throws CSV.ParseError collect(A.Rows(IOBuffer(bad); types=Int, on_error=:error))[2].a
    @test_throws ArgumentError A.File(IOBuffer(bad); on_error=:ignore)
    @test_throws ArgumentError A.Rows(IOBuffer(bad); types=Int, on_error=:warn)
    rowerr = try
        collect(A.Rows(IOBuffer(bad); types=Int, on_error=:error))[2].a
    catch e
        e
    end
    @test rowerr isa CSV.ParseError && rowerr.source == "<GenericIOBuffer>"
    @test rowerr.nproblems == 1 && rowerr.problem.row == 2
    # :warn prints exactly one summary and returns the collected table
    f = @test_logs (:warn, r"CSV: 2 parse problems in <GenericIOBuffer>; first: invalid_value at data row 2") begin
        A.File(IOBuffer("a\n1\nx\ny\n"); types=Int, on_error=:warn)
    end
    @test length(A.problems(f)) == 2
    @test_logs A.File(IOBuffer("a\n1\n2\n"); types=Int, on_error=:warn)
    @test_logs (:warn, r"CSV: 1 parse problem") first(A.Chunks(IOBuffer(bad); types=Int, on_error=:warn))
    @test_logs (:warn, r"CSV: 1 parse problem") A.File(IOBuffer(bad); on_error=:warn, transpose=true, header=false, types=Dict(1 => Int))
    p = first(A.problems(f))
    @test sprint(show, p) == "CSV.Problem(invalid_value at data row 2, column 1, byte 5: \"cannot parse Int64 from \\\"x\\\"\")"
    # Rows/Chunks display never dumps the buffer or index
    rows = A.Rows(IOBuffer("a,b\n1,x\n2,y\n"); types=Dict(:a => Int64))
    shown = sprint(show, rows)
    @test startswith(shown, "CSV.Rows(\"<GenericIOBuffer>\"): 2 rows × 2 columns")
    @test occursin("a::Union{Missing, Int64}", shown) && !occursin("UInt8[", shown)
    chunks = A.Chunks(IOBuffer("a,b\n1,x\n2,\n"); chunkbytes=8)
    shown = sprint(show, chunks)
    @test startswith(shown, "CSV.Chunks(\"<GenericIOBuffer>\"): ")
    @test occursin("a::Int64", shown) && occursin("b::$(Union{Missing, A.DataString})", shown)
    typedshown = sprint(show, A.Chunks(IOBuffer("a,b\n1,x\n"); types=[Int32, String]))
    @test occursin("a::Int32", typedshown) && occursin("b::String", typedshown)
    @test !occursin("ChunkIndex", shown)
    @test names(rows) == [:a, :b] && names(chunks) == [:a, :b]
    @test Base.IteratorSize(typeof(rows)) isa Base.HasLength && length(rows) == 2
    @test length(A.Rows(IOBuffer("a\n1\n2\n3\n"); limit=2)) == 2
    @test length(collect(A.Rows(IOBuffer("a\n1\n2\n3\n"); limit=2))) == 2
    # string indexing on both row types, and a guiding error without a sink
    @test first(rows)["a"] == 1 && first(rows)["b"] == "x"
    ff = A.File(IOBuffer("a,b\n1,x\n"))
    @test ff[1]["a"] == 1 && ff[1]["b"] == "x"
    @test_throws ArgumentError A.read(IOBuffer("a\n1\n"))
end

@testset "reader option fixes: footerskip anchor, decimal, DateFormat, selection" begin
    # a quote inside a skipped prefix row cannot swallow the footer count
    f = A.File(IOBuffer("junk \" line\na,b\n1,2\n3,4\nfooter\n"); header=2, footerskip=1)
    @test names(f) == [:a, :b] && collect(f.a) == [1, 3]
    f = A.File(IOBuffer("skip \"me\nskip\na,b\n1,2\nfooter\n"); header=3, footerskip=1)
    @test collect(f.b) == [2]
    # decimal cannot be numeric syntax; decimal == delim stays legal (quoted values)
    @test_throws ArgumentError A.File(IOBuffer("x\n105\n"); decimal='0')
    @test_throws ArgumentError A.File(IOBuffer("x\n1e5\n"); decimal='e')
    @test_throws ArgumentError A.File(IOBuffer("x\n1-5\n"); decimal='-')
    @test_throws ArgumentError A.File(IOBuffer("x\n1\n"); decimal='"')
    @test collect(A.File(IOBuffer("x\n\"1,5\"\n"); decimal=',').x) == [1.5]
    # Dates.DateFormat objects are accepted alone and per column
    fmt = dateformat"yyyy/mm/dd"
    @test collect(A.File(IOBuffer("d\n2020/01/02\n"); dateformat=fmt).d) == [Date(2020, 1, 2)]
    @test collect(A.File(IOBuffer("d,e\n2020/01/02,2020-01-03\n");
                         dateformat=Dict(:d => fmt)).e) == [Date(2020, 1, 3)]
    @test collect(A.File(IOBuffer("d\n2020/01/02 03:04\n");
                         dateformat=dateformat"yyyy/mm/dd HH:MM").d) ==
          [DateTime(2020, 1, 2, 3, 4)]
    @test_throws ArgumentError A.File(IOBuffer("d\n1\n"); dateformat=1)
    # regex and single-name selection
    src = "ax,ay,b\n1,2,3\n"
    @test names(A.File(IOBuffer(src); select=r"^a")) == [:ax, :ay]
    @test names(A.File(IOBuffer(src); drop=r"^a")) == [:b]
    @test names(A.File(IOBuffer(src); select=:b)) == [:b]
    @test names(A.File(IOBuffer(src); select="ay")) == [:ay]
    @test names(A.File(IOBuffer(src); drop=1)) == [:ay, :b]
    @test names(A.lazy(IOBuffer(src); select=r"y$")) == [:ay]
    @test Tables.columnnames(A.Rows(IOBuffer(src); select=r"^a")) == [:ax, :ay]
    @test names(first(A.Chunks(IOBuffer(src); drop=r"^a"))) == [:b]
    @test_throws ArgumentError A.File(IOBuffer(src); select=r"^z")
    @test names(A.File(IOBuffer(src); drop=r"^z")) == [:ax, :ay, :b]
    @test_throws ArgumentError A.File(IOBuffer(src); select=(i, nm) -> true)
end

@testset "types=String names the output type" begin
    src = "s,t,u\nabc,1,x\n,2,y\n"
    f = A.File(IOBuffer(src); types=Dict(:s => String))
    @test eltype(f.s) == Union{Missing, String} && isequal(collect(f.s), ["abc", missing])
    @test eltype(f.u) == A.DataString
    f = A.File(IOBuffer(src); types=String)
    @test eltype(f.s) == Union{Missing, String} && eltype(f.t) == String && eltype(f.u) == String
    f = A.File(IOBuffer(src); types=[Union{Missing, String}, Int64, A.DataString])
    @test eltype(f.s) == Union{Missing, String} && eltype(f.t) == Int64 && eltype(f.u) == A.DataString
    f = A.File(IOBuffer(src); types=Dict(:u => String15))
    @test eltype(f.u) == String15 && collect(f.u) == [String15("x"), String15("y")]
    f = A.File(IOBuffer(src); types=Dict(:u => InlineString))
    @test eltype(f.u) == String1
    for (T, W) in ((String15, String15), (InlineString, String1))
        kw = (; types=Dict(:u => Union{Missing, T}))
        @test eltype(A.File(IOBuffer(src); kw...).u) == Union{Missing, W}
        @test all(b -> eltype(b.u) == Union{Missing, W},
                  A.Chunks(IOBuffer(src); kw..., chunkbytes=4))
    end
    for T in (String15, InlineString)
        W = T === InlineString ? String3 : T
        batches = collect(A.Chunks(IOBuffer("s\nabc\n\nxyz\n"); stringtype=T,
                                   ignoreemptyrows=false, chunkbytes=4))
        @test all(b -> eltype(b.s) == Union{Missing, W}, batches)
        @test isequal(vcat((b.s for b in batches)...), [W("abc"), missing, W("xyz")])
    end
    # pooling keeps String levels; stringtype governs inferred text only
    f = A.File(IOBuffer(src); types=Dict(:u => String), pool=true, stringtype=A.DataString)
    @test f.u isa PooledVector && eltype(f.u) == String
    # every reader honors the request
    @test eltype(A.lazy(IOBuffer(src); types=Dict(:u => String)).u) == Union{Missing, String}
    @test A.lazy(IOBuffer(src); types=Dict(:u => String15)).u[1] === String15("x")
    @test first(A.Rows(IOBuffer(src); types=Dict(:u => String))).u isa String
    @test first(A.Rows(IOBuffer(src); types=Dict(:u => String15))).u === String15("x")
    @test first(A.Rows(IOBuffer(src); types=Dict(:u => String), on_error=:error)).u isa String
    @test eltype(first(A.Chunks(IOBuffer(src); types=Dict(:u => String))).u) == String
    @test eltype(A.File(IOBuffer("k,1,2\n"); transpose=true, types=Dict(:k => String)).k) == String
    @test eltype(A.File(IOBuffer(src); scan=Tables.Scan(select=(:u => String,))).u) == String
    # InlineString auto width stops at String31; wider text stays String
    wide = "s\n" * repeat("x", 40) * "\ny\n"
    @test eltype(A.File(IOBuffer(wide); stringtype=InlineString).s) == String
    @test eltype(A.File(IOBuffer(wide); stringtype=InlineString, pool=true).s) == String
    @test eltype(A.File(IOBuffer("s\n" * repeat("x", 300) * "\n"); stringtype=InlineString).s) == String
    @test eltype(A.File(IOBuffer("s\nabc\n"); stringtype=InlineString).s) == String3
    @test first(A.Rows(IOBuffer(wide); stringtype=InlineString)).s isa String
    @test first(A.Rows(IOBuffer("s\nabc\n"); stringtype=InlineString)).s isa String3
    @test_throws ArgumentError A.File(IOBuffer(wide); stringtype=String15)
end

@testset "requested text output across readers and pools" begin
    for S in (String, A.DataString, String15), default in (String, A.DataString, String7), pool in (false, true)
        kw = (; types=Dict(:s => S), stringtype=default, pool)
        input = "s,t\nabc,xyz\n,uvw\n"
        readers = (A.File(IOBuffer(input); kw...),
                   first(A.Chunks(IOBuffer(input); chunkbytes=1024, kw...)),
                   A.File(IOBuffer("s,abc,\nt,xyz,uvw\n"); transpose=true, kw...),
                   A.File(IOBuffer(input); scan=Tables.Scan(select=(:s => S, :t)), stringtype=default, pool))
        expected = pool && S === A.DataString ? String : S
        for f in readers
            @test Base.nonmissingtype(eltype(f.s)) === expected
            @test isequal(collect(f.s), ["abc", missing])
            @test (f.s isa PooledVector) == pool
        end
        for f in (A.Rows(IOBuffer(input); types=Dict(:s => S), stringtype=default),
                  A.lazy(IOBuffer(input); types=Dict(:s => S), stringtype=default))
            col = Tables.columntable(f).s
            @test Base.nonmissingtype(eltype(col)) === S
            @test isequal(collect(col), ["abc", missing])
        end
    end
    for reader in (A.Rows, A.lazy), explicit in (false, true), n in (40, 300)
        input = "s\n" * "x"^n * "\ny\n"
        f = explicit ? reader(IOBuffer(input); types=InlineString) :
                       reader(IOBuffer(input); stringtype=InlineString)
        col = Tables.columntable(f).s
        @test col[1] isa String
        @test col[1] == "x"^n
        @test col[2] isa String1
        @test typeof(col[1]) <: eltype(col)
    end
end

@testset "prepared option combinations" begin
    # The quoted=false fallback must count delimiter bytes after a bare quote.
    @test names(A.File(IOBuffer("a\";b\n"); quoted=false)) == [Symbol("a\""), :b]
    input = "skip this\nid;value;day\n1;[1,234.5];2024/01/02\n2;NA;2024/01/03\n9;0;2024/01/04\n"
    kw = (; header=2, footerskip=1, missingstring="NA", delim=';',
            openquotechar='[', closequotechar=']', groupmark=',',
            dateformat=Dict(:day => dateformat"yyyy/mm/dd"),
            types=Dict(:id => Int, :value => Float64, :day => Date))
    for f in (A.File(IOBuffer(input); kw...),
              first(A.Chunks(IOBuffer(input); chunkbytes=1024, kw...)))
        @test collect(f.id) == [1, 2]
        @test isequal(collect(f.value), [1234.5, missing])
        @test collect(f.day) == [Date(2024, 1, 2), Date(2024, 1, 3)]
    end
    for f in (A.Rows(IOBuffer(input); kw...), A.lazy(IOBuffer(input); kw...))
        ct = Tables.columntable(f)
        @test collect(ct.id) == [1, 2]
        @test isequal(collect(ct.value), [1234.5, missing])
        @test collect(ct.day) == [Date(2024, 1, 2), Date(2024, 1, 3)]
    end
end

@testset "bare quotes: the index repairs itself under the lenient rule" begin
    # 0.10 parity: a quote that does not start its field is content
    bare = "a,b\n1,x\"y\n2,z\n3,w\n"
    f = A.File(IOBuffer(bare))
    @test length(f) == 3 && String.(f.b) == ["x\"y", "z", "w"] && isempty(A.problems(f))
    @test getfield(getfield(getfield(A.lazy(IOBuffer(bare)), :prepared), :p), :d).lenient
    @test !getfield(getfield(getfield(A.lazy(IOBuffer("a,b\n1,\"x\"\n")), :prepared), :p), :d).lenient
    inch = "size,desc\n10,Pipe 3\" long\n12,Rod 5' 11\"\n14,plain\n"
    f = A.File(IOBuffer(inch))
    @test f.size == [10, 12, 14] && String.(f.desc) == ["Pipe 3\" long", "Rod 5' 11\"", "plain"]
    mixed = "a,b\n1,\"ok, quoted\"\n2,x\"y\n3,\"esc \"\"q\"\" here\"\n4,z\n"
    @test String.(A.File(IOBuffer(mixed)).b) == ["ok, quoted", "x\"y", "esc \"q\" here", "z"]
    # every reader, every chunk geometry
    many = "a,b\n" * join(("$i,v$(i)\"" for i in 1:2000), "\n") * "\n"
    for chunkbytes in (7, 64, 1 << 20)
        f = A.File(IOBuffer(many); chunkbytes)
        @test length(f) == 2000 && f.a == 1:2000 && String(f.b[2000]) == "v2000\""
        @test length(collect(A.Rows(IOBuffer(many); chunkbytes))) == 2000
        @test String(A.lazy(IOBuffer(many); chunkbytes).b[1]) == "v1\""
        @test sum(length, A.Chunks(IOBuffer(many); chunkbytes)) == 2000
    end
    @test A.File(IOBuffer(bare); types=Dict(:a => Int)).a == [1, 2, 3]
    @test names(A.File(IOBuffer("si\"ze,desc\n10,x\n11,y\n"))) == [Symbol("si\"ze"), :desc]
    @test String.(A.File(IOBuffer("a,b\r\n1,x\"y\r\n2,z\r\n")).b) == ["x\"y", "z"]
    @test String.(A.File(IOBuffer("a,b\n#c\"omment\n1,x\"y\n2,z\n"); comment="#").b) == ["x\"y", "z"]
    @test String.(A.File(IOBuffer("a,b\n1,x\"y\n2,z\nfooter\n"); footerskip=1).b) == ["x\"y", "z"]
    @test String.(A.File(IOBuffer("a,b\n1,\"x\\\"y\"\n2,z\"q\n3,w\n"); escapechar='\\').b) ==
          ["x\"y", "z\"q", "w"]
    @test String.(A.File(IOBuffer("a,b\n1, \"x, y\" \n2,z\"q\n3,w\n")).b) == ["x, y", "z\"q", "w"]
    @test String.(A.File(IOBuffer("a::b\nx\"y::z\n"); delim="::").a) == ["x\"y"]
    t = A.File(IOBuffer("k,x\"y,z\n"); transpose=true, header=false)
    @test String.(t.Column1) == ["k", "x\"y", "z"]
    # scans, including a filter that never parses the affected column
    sf = A.File(IOBuffer(bare); scan=Tables.Scan(select=(:a, :b), filter=Tables.col(:a) > 1))
    @test sf.a == [2, 3] && String.(sf.b) == ["z", "w"]
    balanced = "a,b\n1,x\"y\n2,z\n3,w\"v\n4,q\n"
    @test A.File(IOBuffer(balanced); select=:a).a == [1, 2, 3, 4]
    # an unclosed quote at a real field start is still malformed input
    f = A.File(IOBuffer("a,b\n1,\"x\n2,y\n"); on_error=:collect)
    @test any(p -> p.kind == :unclosed_quote, A.problems(f))
end

@testset "string columns own their bytes" begin
    csv = "s,n\n" * join(("value number $i is here,$i" for i in 1:60_000), "\n") * "\n"
    mktempdir() do dir
        p = joinpath(dir, "big.csv")
        write(p, csv)
        @test filesize(p) >= A.MMAP_THRESHOLD
        f = A.File(p)
        v = f.s[end]
        @test f.s.buffers[1] === A.EMPTY_BYTES      # nothing views the map
        # Rewrite the file smaller while values are live: the table holds no
        # reference to the mapping and every finished task drops its own, so
        # one collection releases the map (Windows keeps a mapped file
        # locked). Retry briefly if an unused mapping has not been finalized.
        for attempt in 1:20
            GC.gc(true)
            try
                CSV.write(p, (s=["tiny"], n=[1]))
                break
            catch e
                (e isa SystemError && attempt < 20) || rethrow()
                sleep(0.05)
            end
        end
        @test A.File(p).s == ["tiny"]
        @test String(v) == "value number 60000 is here"
        @test String(f.s[100]) == "value number 100 is here"
    end
    # a byte-vector input is never aliased
    b = Vector{UInt8}("s\nabcdefghijklmnopqrstuvwxyz\n")
    f = A.File(b)
    b[3] = UInt8('Z')
    @test String(f.s[1]) == "abcdefghijklmnopqrstuvwxyz"
    # a value retains the chunk-private buffer its text lives in, not the input
    f = A.File(IOBuffer(csv))
    @test Base.summarysize(f.s[1]) < Base.summarysize(f.s) ÷ 2
    @test Base.summarysize(f.s[1]) <= 2 * maximum(length, f.s.buffers) + 4096
    # values survive chunk adoption, escapes, and every chunk geometry
    esc = "s\n" * join(("\"say \"\"hi\"\" number $i\"" for i in 1:5000), "\n") * "\n"
    for chunkbytes in (16, 1024, 1 << 20)
        f = A.File(IOBuffer(esc); chunkbytes)
        @test String(f.s[1]) == "say \"hi\" number 1"
        @test String(f.s[end]) == "say \"hi\" number 5000"
        @test f.s.buffers[1] === A.EMPTY_BYTES
        @test A.File(IOBuffer(esc); chunkbytes, types=String).s[end] == "say \"hi\" number 5000"
    end
    # Int → String promotion across chunks re-parses earlier chunks
    promo = "s\n" * join((i < 4000 ? string(i) : "text value $i" for i in 1:5000), "\n") * "\n"
    f = A.File(IOBuffer(promo); chunkbytes=64)
    @test String(f.s[1]) == "1" && String(f.s[5000]) == "text value 5000"
    @test f.s.buffers[1] === A.EMPTY_BYTES
    esc2 = "s,n\n" * join(("\"say \"\"hi\"\" number $i\",$i" for i in 1:5000), "\n") * "\n"
    sf = A.File(IOBuffer(esc2); chunkbytes=64,
                scan=Tables.Scan(select=(:s,), filter=Tables.col(:n) > 4990))
    @test length(sf.s) == 10 && String(sf.s[end]) == "say \"hi\" number 5000"
    @test String(first(A.Chunks(IOBuffer(esc); chunkbytes=64)).s[1]) == "say \"hi\" number 1"
    @test String(A.File(IOBuffer("k,\"a long quoted value\",x\n"); transpose=true).k[1]) ==
          "a long quoted value"
    pooled = A.File(IOBuffer(csv); pool=true, limit=100)
    @test String(pooled.s[1]) == "value number 1 is here"
    # Rows and lazy still view the retained source
    r = first(A.Rows(IOBuffer(csv)))
    @test String(r.s) == "value number 1 is here"
    lf = A.lazy(IOBuffer(csv))
    @test String(lf.s[60_000]) == "value number 60000 is here"
end

@testset "on_error=:warn is the eager default; Char and Symbol types; scan windows" begin
    bad = "a,b\n1,x\ny,z\n"
    # every eager reader warns once, with the silencing option in the message
    for reader in (src -> A.File(src; types=Dict(:a => Int)),
                   src -> A.read(src, Tables.columntable; types=Dict(:a => Int)),
                   src -> A.File(A.lazy(src); types=Dict(:a => Int)),
                   src -> A.File([src, IOBuffer("a,b\n3,w\n")]; types=Dict(:a => Int)),
                   src -> A.File(src; scan=Tables.Scan(select=(:a => Int, :b))),
                   src -> A.File(src; scan=Tables.Scan(select=(:a => Int,), filter=Tables.colcmp(==, Tables.col(:b), "z"))))
        f = @test_logs (:warn, r"CSV: \d+ parse problems? in .*pass on_error=:collect") reader(IOBuffer(bad))
        @test f !== nothing
        @test_logs reader(IOBuffer("a,b\n1,x\n2,z\n"))
    end
    @test_logs (:warn, r"CSV: 1 parse problem") A.File(IOBuffer("a,1,y\nb,x,z\n"); transpose=true,
                                                       types=Dict(:a => Int))
    @test_logs A.File(IOBuffer("a,1,2\nb,x,z\n"); transpose=true, types=Dict(:a => Int))
    @test_logs A.File(IOBuffer(bad); types=Dict(:a => Int), on_error=:collect)
    # Retention and warning policy are independent: a zero cap still warns.
    @test_logs (:warn, r"CSV: 1 parse problem") A.File(IOBuffer(bad); types=Dict(:a => Int), maxproblems=0)
    @test_logs A.File(IOBuffer(bad); types=Dict(:a => Int), maxproblems=0, on_error=:collect)
    @test_throws A.ParseError A.File(IOBuffer(bad); types=Dict(:a => Int), on_error=:error)
    @test_throws A.ParseError A.File(IOBuffer(bad); types=Dict(:a => Int), strict=true)
    # Chunks warns for the first batch with problems only; every batch keeps its problems
    chunky = "a\n1\nx\n2\ny\n3\nz\n"
    batches = @test_logs (:warn, r"batch \d+ of <GenericIOBuffer>.*Later batches do not warn") begin
        collect(A.Chunks(IOBuffer(chunky); types=Int, chunkbytes=4))
    end
    @test length(batches) > 1
    @test sum(length(A.problems(b)) for b in batches) == 3
    @test_logs collect(A.Chunks(IOBuffer(chunky); types=Int, chunkbytes=4, on_error=:collect))
    @test_logs collect(A.Chunks(IOBuffer("a\n1\n2\n3\n"); types=Int, chunkbytes=4))
    # Rows keeps collecting (it has no diagnostics to summarize)
    @test_logs collect(A.Rows(IOBuffer(bad); types=Dict(:a => Int)))
    @test_throws ArgumentError A.Rows(IOBuffer(bad); on_error=:warn)

    # types=Char: exactly one Unicode scalar; anything else is a problem
    f = A.File(IOBuffer("c,s\na,x\nβ,long value here\n漢,\"q,z\"\n\" \",\"\"\n");
               types=Dict(:c => Char, :s => Symbol), on_error=:collect)
    @test eltype(f.c) == Char && collect(f.c) == ['a', 'β', '漢', ' ']
    @test eltype(f.s) == Symbol && collect(f.s) == [:x, Symbol("long value here"), Symbol("q,z"), Symbol("")]
    @test isempty(A.problems(f))
    f = A.File(IOBuffer("c\nab\na\n\n\xff\n\xed\xa0\x80\n"); types=Char, on_error=:collect,
               ignoreemptyrows=false)
    @test isequal(collect(f.c), [missing, 'a', missing, missing, missing])
    @test [(p.row, p.kind) for p in A.problems(f)] == [(1, :invalid_value), (4, :invalid_value), (5, :invalid_value)]
    @test_throws A.ParseError A.File(IOBuffer("c\nab\n"); types=Char, on_error=:error)
    @test isequal([r.c for r in A.Rows(IOBuffer("c\nq\nqq\n"); types=Char)], ['q', missing])
    @test isequal(collect(A.lazy(IOBuffer("c\nq\nqq\n"); types=Char).c), ['q', missing])
    @test eltype(first(A.Chunks(IOBuffer("c\nq\nr\n"); types=Char)).c) == Char
    # types=Symbol: parsed as text, converted once; pooled levels are Symbols
    f = A.File(IOBuffer("s\nx\n\ny\n"); types=Symbol, pool=true, ignoreemptyrows=false)
    @test f.s isa PooledArrays.PooledArray && isequal(collect(f.s), [:x, missing, :y])
    @test eltype(f.s) == Union{Missing, Symbol}
    @test [r.s for r in A.Rows(IOBuffer("s\nx\ny\n"); types=Symbol)] == [:x, :y]
    @test collect(A.lazy(IOBuffer("s\nx\ny\n"); types=Symbol).s) == [:x, :y]
    @test eltype(first(A.Chunks(IOBuffer("s\nx\ny\n"); types=Symbol)).s) == Symbol
    @test Tables.schema(A.Rows(IOBuffer("s\nx\n"); types=Symbol)).types == (Union{Missing, Symbol},)
    @test eltype(A.File(IOBuffer("s\nx\ny\n"); types=Dict(:s => Union{Missing, Symbol})).s) ==
          Union{Missing, Symbol}
    @test_throws ArgumentError A.File(IOBuffer("s\nx\n"); stringtype=Symbol)
    @test_throws ArgumentError A.Rows(IOBuffer("s\nx\n"); stringtype=Symbol)

    # a scan applies the prepared row window (footerskip) before its own bounds
    footer = "a,b\n1,x\n2,y\n3,z\nfooter line\n"
    f = A.File(IOBuffer(footer); footerskip=1, scan=Tables.Scan(select=(:a,)))
    @test collect(f.a) == [1, 2, 3] && isempty(A.problems(f))
    f = A.File(IOBuffer(footer); footerskip=1,
               scan=Tables.Scan(select=(:a, :b), filter=Tables.col(:a) > 1))
    @test collect(f.a) == [2, 3] && String.(f.b) == ["y", "z"] && isempty(A.problems(f))
    f = A.File(IOBuffer(footer); footerskip=1, scan=Tables.Scan(select=(:a,), offset=1, limit=1))
    @test collect(f.a) == [2]
    f = A.File(IOBuffer(footer); footerskip=1,
               scan=Tables.Scan(select=(:a,), filter=Tables.col(:a) > 0, offset=2))
    @test collect(f.a) == [3]
    f = A.File(IOBuffer(footer); footerskip=4, scan=Tables.Scan(select=(:a,)))
    @test isempty(f.a)

    # several sources keep a shared explicit string type; mixtures are String
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]; types=String15).a isa Vector{String15}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]; types=String).a isa Vector{String}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]).a isa A.DataStringVector{A.DataString}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]).a == ["x", "y"]
    mixedwidth = A.File([IOBuffer("a\nx\n"), IOBuffer("b\ny\n")]; types=String15)
    @test isequal(collect(mixedwidth.a), [String15("x"), missing]) &&
          mixedwidth.a isa Vector{Union{Missing, String15}}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]; types=Dict(:a => String15)).a isa Vector{String15}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]; stringtype=String15).a isa Vector{String15}
    @test A.File([IOBuffer("a\nx\n"), IOBuffer("a\ny\n")]; stringtype=InlineString).a isa Vector{String1}
end

@testset "short chunk samples (main #1199/#1200)" begin
    # Chunk starts come from the structural index, so every chunk geometry
    # yields the same rows: CRLF, no trailing newline, an empty last field,
    # and a chunk never starts inside a multiline quoted field.
    for newline in ("\n", "\r\n"), trailingnewline in (false, true),
            emptyfield in (false, true), chunkbytes in (16, 80, 1 << 20)
        rows = ["$i,$(2i)" for i in 1:100]
        emptyfield && (rows[end] = "100,")
        data = Vector{UInt8}("a,b" * newline * join(rows, newline) *
            (trailingnewline ? newline : ""))
        chunks = collect(A.Chunks(data; ntasks=10, chunkbytes))
        @test chunkbytes < length(data) ? length(chunks) > 1 : length(chunks) == 1
        @test reduce(vcat, (chunk.a for chunk in chunks)) == 1:100
        expected = Union{Missing, Int}[2i for i in 1:100]
        emptyfield && (expected[end] = missing)
        @test isequal(reduce(vcat, (chunk.b for chunk in chunks)), expected)
        @test A.File(data; ntasks=10, chunkbytes).a == 1:100
    end
    data = Vector{UInt8}("id,text\n" * join(("$i,\"123\nabc\"" for i in 1:4000), "\n"))
    chunks = collect(A.Chunks(data; ntasks=2, chunkbytes=4096))
    @test length(chunks) > 1
    @test reduce(vcat, (chunk.id for chunk in chunks)) == 1:4000
    @test all(chunk -> all(==("123\nabc"), chunk.text), chunks)
end

@testset "date-times infer as Timestamp{Nanosecond}" begin
    TS = Timestamp{Nanosecond}
    src = "t,n\n2020-01-02T03:04:05,1\n2020-01-02 03:04:05.123456789,2\n2020-01-02T03:04:05.5,3\n"
    f = A.File(IOBuffer(src))
    @test eltype(f.t) === TS
    @test collect(f.t) == TS.(["2020-01-02T03:04:05", "2020-01-02T03:04:05.123456789", "2020-01-02T03:04:05.5"])
    @test f.t[1] == DateTime(2020, 1, 2, 3, 4, 5)           # Durations compares across types
    # every reader agrees
    @test [r.t for r in A.Rows(IOBuffer(src); types=Dict(:t => TS))] == collect(f.t)
    @test collect(A.lazy(IOBuffer(src); types=Dict(:t => TS)).t) == collect(f.t)
    @test eltype(first(A.Chunks(IOBuffer(src))).t) === TS
    @test eltype(A.File(IOBuffer(src); scan=Tables.Scan(select=(:t => Timestamp{Millisecond},)),
                        on_error=:collect).t) === Union{Missing, Timestamp{Millisecond}}
    # an instant outside the nanosecond range widens the column to microseconds
    wide = A.File(IOBuffer("t\n2020-01-02T03:04:05\n9999-12-31T23:59:59\n"))
    @test eltype(wide.t) === Timestamp{Microsecond}
    @test collect(wide.t) == Timestamp{Microsecond}.(["2020-01-02T03:04:05", "9999-12-31T23:59:59"])
    # the old default is one option away
    @test eltype(A.File(IOBuffer(src); types=Dict(:t => DateTime), on_error=:collect).t) ==
          Union{Missing, DateTime}
    # a typemap replaces the inferred type; a value the mapped type rejects
    # promotes the column (to text), as any inferred conflict does
    @test eltype(A.File(IOBuffer(src); typemap=Dict(TS => DateTime)).t) === A.DataString
    whole = "t,n\n2020-01-02T03:04:05.120,1\n2020-01-03T03:04:05,2\n"
    @test collect(A.File(IOBuffer(whole); typemap=Dict(TS => DateTime)).t) ==
          [DateTime(2020, 1, 2, 3, 4, 5, 120), DateTime(2020, 1, 3, 3, 4, 5)]
    @test collect(A.File(IOBuffer(whole); types=Dict(:t => DateTime)).t) ==
          [DateTime(2020, 1, 2, 3, 4, 5, 120), DateTime(2020, 1, 3, 3, 4, 5)]
    # explicit resolutions check the fraction
    msfile = A.File(IOBuffer(src); types=Dict(:t => Timestamp{Millisecond}), on_error=:collect)
    @test isequal(collect(msfile.t), [Timestamp{Millisecond}("2020-01-02T03:04:05"), missing,
                                      Timestamp{Millisecond}("2020-01-02T03:04:05.5")])
    @test_throws A.ParseError A.File(IOBuffer(src); types=Dict(:t => Timestamp{Second}), on_error=:error)
    # custom formats with time tokens infer a Timestamp too
    custom = A.File(IOBuffer("t\n2020/01/02 03:04\n"); dateformat="yyyy/mm/dd HH:MM")
    @test collect(custom.t) == [TS(2020, 1, 2, 3, 4)]
    # writing prints like `string`, and the bytes read back to the same values
    tbl = (t=[TS(2020, 1, 2, 3, 4, 5, 0, 0, 123456789), TS(2020, 1, 2, 3, 4, 5, 120), TS(2020, 1, 2)],
           m=Union{Missing, TS}[missing, TS(2021, 6, 7, 8, 9, 10), TS(2021, 6, 7, 8, 9, 10, 0, 0, 1000)],
           u=[Timestamp{Microsecond}(9999, 12, 31, 23, 59, 59, 0, 123)])
    io = IOBuffer()
    CSV.write(io, (t=tbl.t, m=tbl.m))
    out = String(take!(io))
    @test out == "t,m\n2020-01-02T03:04:05.123456789,\n2020-01-02T03:04:05.12,2021-06-07T08:09:10\n" *
                 "2020-01-02T00:00:00,2021-06-07T08:09:10.000001\n"
    back = A.File(IOBuffer(out))
    @test collect(back.t) == tbl.t && isequal(collect(back.m), tbl.m)
    io = IOBuffer()
    CSV.write(io, (u=tbl.u,))
    @test String(take!(io)) == "u\n9999-12-31T23:59:59.000123\n"
    io = IOBuffer()
    CSV.write(io, (t=[TS(2020, 1, 2, 3, 4, 5, 120)],); delim=':')
    @test String(take!(io)) == "t\n\"2020-01-02T03:04:05.12\"\n"
end

@testset "timestamp range widening preserves earlier precision" begin
    fine = "2020-01-02T03:04:05.123456789"
    ordinary = "2020-01-02T03:04:05.123456"
    wide = "9999-12-31T23:59:59"
    for parallel in (false, true), chunkbytes in (16, 4096), reverseorder in (false, true)
        values = reverseorder ? [wide, fine] : [fine, wide]
        src = "t,n\n" * join(("$v,$i" for (i, v) in enumerate(values)), '\n') * "\n"
        for nsample in (1, 100)
            f = A.File(IOBuffer(src); parallel, chunkbytes, nsample)
            @test eltype(f.t) === A.DataString
            @test f.t == values
            @test isempty(A.problems(f))
            # A filter uses the staged driver. Both retained rows must take
            # part in the exactness check, irrespective of chunk order.
            f = A.File(IOBuffer(src); parallel, chunkbytes, nsample,
                       scan=Tables.Scan(filter=Tables.col(:n) > 0))
            @test f.t == values
            @test isempty(A.problems(f))
        end
        chunks = collect(A.Chunks(IOBuffer(src); parallel, chunkbytes))
        @test all(c -> eltype(c.t) === A.DataString, chunks)
        @test reduce(vcat, (c.t for c in chunks)) == values
    end
    # The conflicting rows miss Chunks' stratified sample. Its schema pass
    # must recheck row 2 when row 3 widens the range.
    values = fill(ordinary, 1000)
    values[2], values[3] = fine, wide
    src = "t,n\n" * join(("$v,$i" for (i, v) in enumerate(values)), '\n') * "\n"
    for parallel in (false, true), chunkbytes in (16, 4096)
        chunks = collect(A.Chunks(IOBuffer(src); parallel, chunkbytes))
        @test all(c -> eltype(c.t) === A.DataString, chunks)
        @test reduce(vcat, (c.t for c in chunks)) == values
        # Excluded fine fractions permit microseconds; excluded wide dates
        # permit nanoseconds. A row limit excludes the wide sentinel too.
        for excluded in (2, 3)
            f = A.File(IOBuffer(src); parallel, chunkbytes, nsample=1,
                       scan=Tables.Scan(filter=Tables.colcmp(!=, Tables.col(:n), excluded)))
            P = excluded == 2 ? Microsecond : Nanosecond
            @test eltype(f.t) === Timestamp{P}
            @test f.t == Timestamp{P}.(values[setdiff(eachindex(values), [excluded])])
        end
        @test eltype(A.File(IOBuffer(src); parallel, chunkbytes, nsample=1, limit=2).t) === Timestamp{Nanosecond}
    end
end

@testset "typed timestamp resolutions across readers" begin
    for (P, fraction) in ((Second, ""), (Millisecond, ".123"),
                          (Microsecond, ".123456"), (Nanosecond, ".123456789"))
        T = Timestamp{P}
        text = "2020-01-02T03:04:05$fraction"
        src = "t\n$text\ninvalid\n\n"
        expected = [T(text), missing]
        @test isequal(A.File(IOBuffer(src); types=T, on_error=:collect).t, expected)
        @test isequal([r.t for r in A.Rows(IOBuffer(src); types=T, on_error=:collect)], expected)
        @test isequal(collect(A.lazy(IOBuffer(src); types=T).t), expected)
        @test isequal(reduce(vcat, (c.t for c in A.Chunks(IOBuffer(src); types=T,
                                                       chunkbytes=16, on_error=:collect))), expected)
        @test isequal(A.File(IOBuffer(src); scan=Tables.Scan(select=(:t => T,)),
                             on_error=:collect).t, expected)
    end
end

@testset "Date and DateTime reject out-of-range years" begin
    for T in (Date, DateTime)
        years = (year(typemin(T)), year(typemax(T)))
        for y in (typemin(Int64), years[1] - 1, years[2] + 1, typemax(Int64))
            token = string(y, "-01-01", T === DateTime ? "T00:00:00" : "")
            input = "a\n$token\n"
            kw = (; delim=',', types=T)
            f = A.File(IOBuffer(input); kw..., on_error=:collect)
            @test ismissing(only(f.a))
            @test only(A.problems(f)).kind === :invalid_value
            @test ismissing(only(A.lazy(IOBuffer(input); kw...).a))
            @test ismissing(only(A.Rows(IOBuffer(input); kw...)).a)
            @test_throws A.ParseError A.File(IOBuffer(input); kw..., on_error=:error)
            if T === Date
                @test only(A.File(IOBuffer(input); delim=',').a) == token
                @test only(A.File(IOBuffer(input); delim=',', dateformat="yyyy-mm-dd").a) == token
            end
        end
        for x in (typemin(T), typemax(T), T(-1, 1, 1), T(0, 1, 1), T(2024, 2, 29))
            f = A.File(IOBuffer("a\n$x\n"); delim=',', types=T, on_error=:error)
            @test only(f.a) == x
        end
    end
end

@testset "timestamp conversion rejects overflowing civil years" begin
    # These valid calendar years wrap the unchecked Int64 rata-day formula
    # back near 1970. They must never become apparently valid timestamps.
    tokens = ["50505469855535079-01-01T00:00:00",
              "-50505469855531139-01-01T00:00:00",
              "$(typemin(Int64))-01-01T00:00:00",
              "$(typemax(Int64))-01-01T00:00:00"]
    for format in (nothing, "yyyy-mm-ddTHH:MM:SS")
        src = "t\n" * join(tokens, '\n') * "\n"
        inferred = A.File(IOBuffer(src); delim=',', dateformat=format)
        @test inferred.t == tokens
        for token in tokens
            @test only(A.File(IOBuffer("t\n$token\n"); delim=',', dateformat=format).t) == token
        end
        for P in (Second, Millisecond, Microsecond, Nanosecond)
            T = Timestamp{P}
            opts = (; delim=',', dateformat=format, types=T)
            f = A.File(IOBuffer(src); opts..., on_error=:collect)
            @test all(ismissing, f.t)
            @test length(A.problems(f)) == length(tokens)
            @test all(ismissing, [r.t for r in A.Rows(IOBuffer(src); opts..., on_error=:collect)])
            @test all(ismissing, A.lazy(IOBuffer(src); opts...).t)
            @test all(c -> all(ismissing, c.t), A.Chunks(IOBuffer(src); opts..., on_error=:collect))
        end
    end
end

@testset "timestamp calendar validation and checked tick boundaries" begin
    # Parsers rejects hour 24, even midnight, before CSV constructs an instant.
    invalid = ["2023-02-29T00:00:00", "2024-04-31T00:00:00",
               "2024-00-01T00:00:00", "2024-01-00T00:00:00",
               "2024-01-01T24:00:00", "2024-01-01T24:00:01",
               "2024-01-01T23:60:00", "2024-01-01T23:59:60"]
    for format in (nothing, "yyyy-mm-ddTHH:MM:SS"), P in (Second, Millisecond, Microsecond, Nanosecond)
        f = A.File(IOBuffer("t\n" * join(invalid, '\n') * "\n"); delim=',',
                   dateformat=format, types=Timestamp{P}, on_error=:collect)
        @test all(ismissing, f.t)
        @test length(A.problems(f)) == length(invalid)
    end
    rng = MersenneTwister(0x6c10)
    for P in (Second, Millisecond, Microsecond, Nanosecond)
        T = Timestamp{P}
        scale = Int128(Dates.value(convert(Nanosecond, P(1))))
        perday = 86_400_000_000_000 ÷ scale
        ticks = [Int128(typemin(Int64)) + d for d in (-perday, -1, 0, 1, perday)]
        append!(ticks, [Int128(typemax(Int64)) + d for d in (-perday, -1, 0, 1, perday)])
        append!(ticks, Int128.(rand(rng, Int64, 256)))
        for tick in ticks
            days, subday = fldmod(tick, perday)
            date = Date(Dates.UTD(Int64(days + Dates.totaldays(1970, 1, 1))))
            time = Time(Nanosecond(Int64(subday * scale)))
            ns = millisecond(time) * 1_000_000 + microsecond(time) * 1_000 + nanosecond(time)
            c = Parsers.CivilParts(year(date), month(date), day(date), hour(time), minute(time), second(time), ns)
            value, ok = A.totimestamp(T, c)
            @test ok == (typemin(Int64) <= tick <= typemax(Int64))
            if ok
                @test Dates.value(value) == tick
                @test value == T(year(date), month(date), day(date), hour(time), minute(time), second(time), 0, 0, ns)
            end
        end
        if P !== Nanosecond
            c = Parsers.CivilParts(2024, 1, 1, 0, 0, 0, 1)
            @test !A.totimestamp(T, c)[2]
        end
    end
    # A negative year is a valid proleptic Gregorian year at wider resolutions.
    f = A.File(IOBuffer("t\n-0001-01-01T00:00:00\n0000-02-29T00:00:00\n"); delim=',')
    @test f.t == [Timestamp{Microsecond}(-1), Timestamp{Microsecond}(0, 2, 29)]
end

@testset "a kept empty row is all-missing, not a short-row problem" begin
    src = "a,b,c\n1,2,3\n\n4,5,6\n\n"
    for chunkbytes in (4, 1 << 20), parallel in (false, true)
        f = A.File(IOBuffer(src); ignoreemptyrows=false, chunkbytes, parallel, on_error=:collect)
        @test length(f) == 4 && isequal(collect(f.b), [2, missing, 5, missing])
        @test isempty(A.problems(f))
        @test isempty(A.problems(A.File(IOBuffer(src); ignoreemptyrows=false, chunkbytes, parallel,
                                          on_error=:collect, types=Int)))
        batches = collect(A.Chunks(IOBuffer(src); ignoreemptyrows=false, chunkbytes, on_error=:collect))
        @test all(b -> isempty(A.problems(b)), batches)
        @test sum(length, batches) == 4
    end
    @test_logs A.File(IOBuffer(src); ignoreemptyrows=false)
    # a genuinely short row is still reported
    f = A.File(IOBuffer("a,b,c\n1,2,3\n4\n"); ignoreemptyrows=false, on_error=:collect)
    @test [p.kind for p in A.problems(f)] == [:short_row]
    # an empty row in a one-column file is a missing value with no problem
    f = A.File(IOBuffer("a\n1\n\n2\n"); ignoreemptyrows=false, on_error=:collect)
    @test isequal(collect(f.a), [1, missing, 2]) && isempty(A.problems(f))
    # every row-ending style marks a zero-byte row
    for src in ("a,b,c\r\n1,2,3\r\n\r\n4,5,6\r\n", "a,b,c\r1,2,3\r\r4,5,6\r")
        f = A.File(IOBuffer(src); ignoreemptyrows=false, on_error=:collect)
        @test length(f) == 3 && isequal(collect(f.a), [1, missing, 4]) && isempty(A.problems(f))
    end
    # a row of only delimiters under ignorerepeated is one empty field: a short
    # row, not an empty row (its stored start sits past the padding)
    f = A.File(IOBuffer("a b\n   \n1 2\n"); delim=' ', ignorerepeated=true, on_error=:collect)
    @test length(f) == 2 && [(p.row, p.kind) for p in A.problems(f)] == [(1, :short_row)]
    f = A.File(IOBuffer("a b\n\n1 2\n"); delim=' ', ignorerepeated=true, ignoreemptyrows=false,
               on_error=:collect)
    @test length(f) == 2 && isempty(A.problems(f))
end
