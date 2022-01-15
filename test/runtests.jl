using Test, CSV, Mmap, Dates, Tables, PooledArrays, CodecZlib, FilePathsBase, SentinelArrays, Parsers, WeakRefStrings, InlineStrings

const dir = joinpath(dirname(pathof(CSV)), "..", "test", "testfiles")

@eval macro $(:try)(ex)
    quote
        try $(esc(ex))
        catch
        end
    end
end

@testset "CSV" begin

@testset "CSV.File" begin

include("basics.jl")
include("testfiles.jl")
include("iteration.jl")

end # @testset "CSV.File"

include("write.jl")

@testset "PooledArrays" begin

    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), pool=true)
    @test typeof(f.X) == PooledArrays.PooledArray{InlineString1,UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X == ["b", "c", "a", "c"]
    @test f.X.refs[2] == f.X.refs[4]

    f = CSV.File(IOBuffer("X\nb\nc\na\nc"), pool=0.75)
    @test typeof(f.X) == PooledArrays.PooledArray{InlineString1,UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X == ["b", "c", "a", "c"]
    @test f.X.refs[2] == f.X.refs[4]

    f = CSV.File(IOBuffer("X\nb\nc\n\nc"), pool=true, ignoreemptyrows=false)
    @test typeof(f.X) == PooledArray{Union{Missing, InlineString1},UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (4, 1)
    @test f.X[3] === missing

    f = CSV.File(IOBuffer("X\nc\nc\n\nc\nc\nc\nc\nc\nc"), pool=0.25, ignoreemptyrows=false)
    @test typeof(f.X) == PooledArray{Union{Missing, InlineString1},UInt32,1,Array{UInt32,1}}
    @test (length(f), length(f.names)) == (9, 1)
    @test isequal(f.X, ["c", "c", missing, "c", "c", "c", "c", "c", "c"])

end

@testset "CSV.Rows" begin

    rows = CSV.Rows(IOBuffer("X\nb\nc\na\nc"))
    show(rows)
    row = first(rows)
    @test row.X == "b"
    @test row[1] == "b"

    # limit
    rows = collect(CSV.Rows(IOBuffer("X\nb\nc\na\nc"), limit=1))
    @test length(rows) == 1
    @test rows[1][1] == "b"
    rows = collect(CSV.Rows(IOBuffer("X\nb\nc\na\nc"), limit=0))
    @test length(rows) == 0

    # transpose
    rows = collect(CSV.Rows(IOBuffer("x,1\nx2,2\n"), transpose=true))
    @test length(rows) == 1
    @test rows[1].x == "1"
    @test rows[1].x2 == "2"

    # ignorerepeated
    rows = collect(CSV.Rows(IOBuffer("x   y   z\n   1   2   3"), ignorerepeated=true, delim=' '))
    @test length(rows) == 1
    @test length(rows[1]) == 3
    @test all(rows[1] .== ["1", "2", "3"])
    row = rows[1]
    @test CSV.Parsers.parse(Int, row, 1) == 1
    @test CSV.Parsers.parse(Int, row, :x) == 1
    @test CSV.detect(row, 2) == 2
    @test CSV.detect(row, :y) == 2

    # 448
    @test_throws ArgumentError CSV.Rows(IOBuffer("x\n1\n2\n3\n#4"), ignorerepeated=true)

    # 447
    rows = collect(CSV.Rows(IOBuffer("a,b,c\n1,2,3\n\n")))
    @test length(rows) == 1
    @test all(rows[1] .== ["1", "2", "3"])

    # not enough columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1\n2,3\n4,5,6")))
    @test length(rows) == 3
    @test all(isequal.(rows[1], ["1", missing, missing]))
    @test all(isequal.(rows[2], ["2", "3", missing]))
    @test all(rows[3] .== ["4", "5", "6"])

    # too many columns
    rows = collect(CSV.Rows(IOBuffer("x,y,z\n1,2,3\n4,5,6,7,8,")))
    @test length(rows) == 2
    @test isequal(values(rows[1]), ["1", "2", "3", missing, missing])
    @test isequal(values(rows[2]), ["4", "5", "6", "7", "8"])

    # fatal
    @test_throws CSV.Error collect(CSV.Rows(IOBuffer("x\n\"invalid quoted field")))

    # reusebuffer
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3")))
    @test length(rows) == 3
    @test rows[1][1] == "1"
    @test rows[2][1] == "2"
    @test rows[3][1] == "3"
    rows = collect(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
    @test length(rows) == 3
    @test rows[1][1] == "3"
    @test rows[2][1] == "3"
    @test rows[3][1] == "3"

    for (i, row) in enumerate(CSV.Rows(IOBuffer("x\n1\n2\n3"), reusebuffer=true))
        @test all(row .== [string(i)])
    end

    # 903
    rows = collect(CSV.Rows(IOBuffer("int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n,,,,,,,,\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"); types=[Int, Float64, Date, DateTime, Bool, Missing, String, String, Float64]))
    @test isequal(collect(rows[1]), [1, 3.14, Date(2019, 1, 1), DateTime(2019, 1, 1, 1, 2, 3), true, missing, "hey", "abc", 2.0])
    foreach(x -> @test(x === missing), rows[2])
    @test isequal(collect(rows[3]), [2, NaN, Date(2019, 1, 2), DateTime(2019, 1, 3, 1, 2, 3), false, missing, "there", "abc", 3.14])
end

@testset "CSV.detect" begin

@test CSV.detect("") == ""
@test CSV.detect("1") == 1
@test CSV.detect("1.1") == 1.1
@test CSV.detect("2015-01-01") == Date(2015)
@test CSV.detect("2015-01-01T03:04:05") == DateTime(2015, 1, 1, 3, 4, 5)
@test CSV.detect("03:04:05") == Time(3, 4, 5)
@test CSV.detect("true") === true
@test CSV.detect("false") === false
@test CSV.detect("abc") === "abc"

end

@testset "CSV.promote_types" begin

@test CSV.promote_types(Int64, Float64) == Float64
@test CSV.promote_types(Int64, Int64) == Int64
@test CSV.promote_types(CSV.NeedsTypeDetection, Float64) == Float64
@test CSV.promote_types(Float64, CSV.NeedsTypeDetection) == Float64
@test CSV.promote_types(Float64, Missing) == Float64
@test CSV.promote_types(Float64, String) === String

end

@testset "CSV.File with select/drop" begin

csv = """
a,b,c,d,e
1,2,3,4,5
6,7,8,9,10
"""

f = CSV.File(IOBuffer(csv), select=[1, 3, 5])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=[:a, :c, :e])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=["a", "c", "e"])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=[true, false, true, false, true])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=(i, nm) -> i in (1, 3, 5))
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), select=Int[])
@test length(f) == 2
@test length(f[1]) == 0

f = CSV.File(IOBuffer(csv), select=[1, 2, 3, 4, 5])
@test length(f) == 2
@test length(f[1]) == 5

f = CSV.File(IOBuffer(csv), drop=[2, 4])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=[:b, :d])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=["b", "d"])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=[false, true, false, true, false])
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=(i, nm) -> i in (2, 4))
@test f.a == [1, 6]
@test all(f[1] .== [1, 3, 5])
@test length(f[1]) == 3

f = CSV.File(IOBuffer(csv), drop=Int[])
@test length(f) == 2
@test length(f[1]) == 5

f = CSV.File(IOBuffer(csv), drop=[1, 2, 3, 4, 5])
@test length(f) == 2
@test length(f[1]) == 0

end

@testset "CSV.Chunks" begin

chunks = CSV.Chunks(transcode(GzipDecompressor, Mmap.mmap(joinpath(dir, "randoms.csv.gz"))); ntasks=2)
@test length(chunks) == 2
state = iterate(chunks)
f, st = state
@test length(f) == 35086
state = iterate(chunks, st)
f, st = state
@test length(f) == 34914

end

function strs(x::Vector, e=nothing)
    len = sum(x -> sizeof(coalesce(x, "")), x)
    data = Vector{UInt8}(undef, len)
    poslens = Vector{PosLen}(undef, length(x))
    pos = 1
    anymissing = false
    for (i, s) in enumerate(x)
        if s !== missing
            slen = sizeof(s)
            bytes = codeunits(s)
            copyto!(data, pos, bytes, 1, slen)
            poslens[i] = PosLen(pos, slen, false, e !== nothing && e in bytes)
            pos += slen
        else
            anymissing = true
            poslens[i] = PosLen(pos, 0, true, false)
        end
    end
    return PosLenStringVector{anymissing ? Union{Missing, PosLenString} : PosLenString}(data, poslens, something(e, 0x00))
end

arrtype(x::ChainedVector{T, AT}) where {T, AT} = AT

@testset "CSV.chaincolumns!" begin

mv = MissingVector(1)
vec = [big(1)]
svec = convert(SentinelArray{BigInt}, [big(1), missing])
# MissingVector -> Vector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), vec)) == typeof(svec)
# Vector{CustomType} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), mv)) == typeof(svec)
# MissingVector -> SentinelVector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), svec)) == typeof(svec)
# SentinelVector{CustomType} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), mv)) == typeof(svec)
# Vector{CustomType} -> SentinelVector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), svec)) == typeof(svec)
# SentinelVector{CustomType} -> Vector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), vec)) == typeof(svec)
# Vector{CustomType} -> Vector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), vec)) == Vector{BigInt}
# SentinelVector{CustomType} -> SentinelVector{CustomType}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), svec)) == typeof(svec)

vec = strs(["hey", "there", "sailor"])
svec = strs(["hey", "ho", missing])
# MissingVector -> PosLenStringVector{PosLenString}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), vec)) <: PosLenStringVector{Union{PosLenString, Missing}}
# PosLenStringVector{PosLenString} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), mv)) <: PosLenStringVector{Union{PosLenString, Missing}}
# MissingVector -> PosLenStringVector{Union{PosLenString, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), svec)) <: PosLenStringVector{Union{PosLenString, Missing}}
# PosLenStringVector{Union{PosLenString, Missing}} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), mv)) <: PosLenStringVector{Union{PosLenString, Missing}}
# PosLenStringVector{PosLenString} -> PosLenStringVector{Union{PosLenString, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), svec)) <: PosLenStringVector{Union{PosLenString, Missing}}
# PosLenStringVector{Union{PosLenString, Missing}} -> PosLenStringVector{PosLenString}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), vec)) <: PosLenStringVector{Union{PosLenString, Missing}}

vec = [true, false, true]
svec = [true, false, missing]
# MissingVector -> Vector{Bool}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), vec)) == Vector{Union{Bool, Missing}}
# Vector{Bool} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), mv)) == Vector{Union{Bool, Missing}}
# MissingVector -> Vector{Union{Bool, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), svec)) == Vector{Union{Bool, Missing}}
# Vector{Union{Bool, Missing}} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), mv)) == Vector{Union{Bool, Missing}}
# Vector{Bool} -> Vector{Union{Bool, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), svec)) == Vector{Union{Bool, Missing}}
# Vector{Union{Bool, Missing}} -> Vector{Bool}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), vec)) == Vector{Union{Bool, Missing}}

vec = Int32[1, 2, 3]
svec = Union{Int32, Missing}[1, 2, missing]
# MissingVector -> Vector{Int32}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), vec)) == Vector{Union{Int32, Missing}}
# Vector{Int32} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), mv)) == Vector{Union{Int32, Missing}}
# MissingVector -> Vector{Union{Int32, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([mv]), svec)) == Vector{Union{Int32, Missing}}
# Vector{Union{Int32, Missing}} -> MissingVector
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), mv)) == Vector{Union{Int32, Missing}}
# Vector{Int32} -> Vector{Union{Int32, Missing}}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec]), svec)) == Vector{Union{Int32, Missing}}
# Vector{Union{Int32, Missing}} -> Vector{Int32}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec]), vec)) == Vector{Union{Int32, Missing}}

vec1 = Int32[1, 2, 3]
vec2 = Int64[1, 2, 3]
svec1 = convert(SentinelArray{Int32}, [Int32(1), Int32(2), missing])
svec2 = convert(SentinelArray{Int64}, [Int64(1), Int64(2), missing])
# Vector{Int32} -> Vector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), vec2)) == Vector{Int64}
# Vector{Int64} -> Vector{Int32}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec2]), vec1)) == Vector{Int64}
# Vector{Int32} -> SentinelVector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), svec2)) == typeof(svec2)
# SentinelVector{Int32} -> Vector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), vec2)) == typeof(svec2)
# SentinelVector{Int32} -> SentinelVector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), svec2)) == typeof(svec2)
# SentinelVector{Int64} -> SentinelVector{Int32}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec2]), svec1)) == typeof(svec2)

vec1 = String3["hey", "ho"]
vec2 = String7["hey", "ho"]
svec1 = convert(SentinelArray{String3}, [String3("hey"), missing])
svec2 = convert(SentinelArray{String7}, [String7("hey"), missing])
# Vector{InlineString3} -> Vector{InlineString7}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), vec2)) == Vector{String7}
# Vector{InlineString7} -> Vector{InlineString3}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec2]), vec1)) == Vector{String7}
# Vector{InlineString3} -> SentinelVector{InlineString7}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), svec2)) == typeof(svec2)
# SentinelVector{InlineString3} -> Vector{InlineString7}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), vec2)) == typeof(svec2)
# SentinelVector{InlineString3} -> SentinelVector{InlineString7}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), svec2)) == typeof(svec2)
# SentinelVector{InlineString7} -> SentinelVector{InlineString3}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec2]), svec1)) == typeof(svec2)

vec1 = Int64[1, 2]
vec2 = [3.14, 2.28]
svec1 = convert(SentinelArray{Int64}, [Int64(1), missing])
svec2 = convert(SentinelArray{Float64}, [3.14, missing])
# Vector{Int64} -> Vector{Float64}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), vec2)) == Vector{Float64}
# Vector{Float64} -> Vector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec2]), vec1)) == Vector{Float64}
# Vector{Int64} -> SentinelVector{Float64}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), svec2)) == typeof(svec2)
# SentinelVector{Int64} -> Vector{Float64}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), vec2)) == typeof(svec2)
# SentinelVector{Int64} -> SentinelVector{Float64}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), svec2)) == typeof(svec2)
# SentinelVector{Float64} -> SentinelVector{Int64}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec2]), svec1)) == typeof(svec2)

vec1 = String3["hey", "ho"]
vec2 = ["hey", "ho"]
svec1 = convert(SentinelArray{String3}, [String3("hey"), missing])
svec2 = convert(SentinelArray{String}, ["ho", missing])
# Vector{InlineString3} -> Vector{String}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), vec2)) == Vector{String}
# Vector{String} -> Vector{InlineString3}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec2]), vec1)) == Vector{String}
# Vector{InlineString3} -> SentinelVector{String}
@test arrtype(CSV.chaincolumns!(ChainedVector([vec1]), svec2)) == typeof(svec2)
# SentinelVector{InlineString3} -> Vector{String}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), vec2)) == typeof(svec2)
# SentinelVector{InlineString3} -> SentinelVector{String}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec1]), svec2)) == typeof(svec2)
# SentinelVector{String} -> SentinelVector{InlineString3}
@test arrtype(CSV.chaincolumns!(ChainedVector([svec2]), svec1)) == typeof(svec2)

vec1 = String3["hey", "ho"]
vec2 = ["hey", "ho"]
svec1 = convert(SentinelArray{String3}, [String3("hey"), missing])
svec2 = convert(SentinelArray{String}, ["ho", missing])
function pvecs(vec1, vec2, svec1, svec2)
    return PooledArray(vec1), PooledArray(vec2), PooledArray(svec1), PooledArray(svec2)
end
# MissingVector -> PooledVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(mv, pvec1)) == typeof(psvec1)
# PooledVector{String} -> MissingVector
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec2, mv)) == typeof(psvec2)
# MissingVector -> PooledVector{Union{String, Missing}}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(mv, psvec2)) == typeof(psvec2)
# PooledVector{Union{String7, Missing}} -> MissingVector
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(psvec1, mv)) == typeof(psvec1)
# PooledVector{String} -> Vector{String}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec2, vec2)) == typeof(pvec2)
# PooledVector{String} -> SentinelVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec2, svec1)) == typeof(psvec2)
# PooledVector{Union{String, Missing}} -> Vector{String}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(psvec2, vec2)) == typeof(psvec2)
# PooledVector{Union{String7, Missing}} -> SentinelVector{String}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(psvec1, svec2)) == typeof(psvec2)
# PooledVector{String} -> PooledVector{Union{String, Missing}}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec2, psvec2)) == typeof(psvec2)
# PooledVector{String} -> PooledVector{Union{String7, Missing}}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec2, psvec1)) == typeof(psvec2)
# Vector{String} -> PooledVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(vec2, pvec1)) == typeof(pvec2)
# SentinelVector{String} -> PooledVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(svec2, pvec1)) == typeof(psvec2)
# Vector{String7} -> PooledVector{Union{String, Missing}}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(vec1, psvec2)) == typeof(psvec2)
# SentinelVector{String7} -> PooledVector{Union{String, Missing}}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(svec1, psvec2)) == typeof(psvec2)
# PooledVector{Union{String7, Missing}} -> PooledVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(psvec1, pvec1)) == typeof(psvec1)
# PooledVector{Union{String, Missing}} -> PooledVector{String7}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(psvec2, pvec1)) == typeof(psvec2)

# issue 925; ambiguity in `CSV._promote(::Type{PooledVector{T,R,RA}}, x::PooledVector{T,R,RA})`
# PooledVector{String3} -> PooledVector{String}
pvec1, pvec2, psvec1, psvec2 = pvecs(vec1, vec2, svec1, svec2)
@test typeof(CSV.chaincolumns!(pvec1, pvec2)) == typeof(pvec2)

end

@testset "CSV.File vector inputs" begin

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a,b,c\n7,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

f = CSV.File(map(IOBuffer, data))
@test length(f) == 6
@test f.names == [:a, :b, :c]
@test f.a == [1, 4, 7, 10, 13, 16]

data = [
    "a,b\nbill,x\njane,y\n",
    "a,b\ntomm,z\ntimm,\n",
    "a,b\njoee,z\njerr,g\n",
]

f = CSV.File(map(IOBuffer, data))
@test length(f) == 6
@test isequal(f.b, ["x", "y", "z", missing, "z", "g"])

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a,b,c\n7.14,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

f = CSV.File(map(IOBuffer, data))
@test eltype(f.a) == Float64

data = [
    "a,b,c\n1,2,3\n4,5,6\n",
    "a2,b,c\n7,8,9\n10,11,12\n",
    "a,b,c\n13,14,15\n16,17,18",
]

f = CSV.File(map(IOBuffer, data))
@test isequal(f.a, [1, 4, missing, missing, 13, 16])

f = CSV.File(map(IOBuffer, data); source=:source)
@test eltype(f.source) == String
@test f.source isa PooledArray

f = CSV.File(map(IOBuffer, data); source="source")
@test eltype(f.source) == String
@test f.source isa PooledArray

f = CSV.File(map(IOBuffer, data); source="source"=>[1,2,3])
@test eltype(f.source) <: Integer
@test f.source isa PooledArray

f = CSV.File(map(IOBuffer, data); source=:source=>["1", "2", "3"])
@test eltype(f.source) == String
@test f.source isa PooledArray

end

end
