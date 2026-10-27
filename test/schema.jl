# Schema reads: `CSV.Rows(source; schema)` checks a source against a
# `Tables.Schema` the caller knows before reading and iterates its NamedTuple
# rows. `test/trim` compiles and runs these reads with `juliac --trim=safe`.
#
# Run:  julia --startup-file=no --project=test test/schema.jl

using Test, Tables, Dates, CodecZlib
using Durations: Timestamp
using DataStrings: DataString
using CSV

const SCHEMA3 = Tables.Schema((:id, :x, :y), (String, Float64, Union{Missing, Int8}))
const SCHEMA5 = Tables.Schema((:id, :a, :b, :c, :d),
                              (Int, Float64, Float64, Float64, Union{Missing, Float64}))
const ROW3 = NamedTuple{(:id, :x, :y), Tuple{String, Float64, Union{Missing, Int8}}}

schemarows(input; schema=SCHEMA3, kw...) = collect(CSV.Rows(IOBuffer(input); schema, kw...))

# The `CSV.ParseError` a read throws, as (kind, data row, column); `nothing`
# when the read succeeds.
function schemaproblem(input; kw...)
    try
        schemarows(input; kw...)
    catch err
        err isa CSV.ParseError || rethrow()
        return (err.problem.kind, err.problem.row, err.problem.col)
    end
    return nothing
end

function schemaerror(input; kw...)
    try
        schemarows(input; kw...)
    catch err
        return err
    end
    return nothing
end

sumx(rows) = (s = 0.0; for row in rows; s += row.x; end; s)

@testset "schema reads" begin

@testset "typed rows and columns" begin
    input = "id,x,y\n\"a,\"\"q\"\"\",1.5,3\nb,-2e3,\nc,7,-128\n"
    rows = @inferred CSV.Rows(IOBuffer(input); schema=SCHEMA3)
    @test rows isa CSV.Rows{ROW3}
    @test eltype(rows) === ROW3
    @test Tables.schema(rows) === SCHEMA3
    @test length(rows) == 3
    @test names(rows) == [:id, :x, :y]
    @test @inferred(first(rows)) === ROW3(("a,\"q\"", 1.5, Int8(3)))
    expected = ROW3[("a,\"q\"", 1.5, Int8(3)), ("b", -2000.0, missing), ("c", 7.0, Int8(-128))]
    @test isequal(collect(rows), expected)
    @test isequal(collect(rows), expected)   # the index is retained: iterate again
    cols = Tables.columntable(rows)
    @test cols isa NamedTuple{(:id, :x, :y),
                              Tuple{Vector{String}, Vector{Float64}, Vector{Union{Missing, Int8}}}}
    @test cols.id == ["a,\"q\"", "b", "c"]
    @test isequal(cols.y, Union{Missing, Int8}[3, missing, -128])
    @test occursin("rows parse and validate when iterated", sprint(show, rows))
    # Without a schema, Rows keeps its lazy row views.
    @test eltype(CSV.Rows(IOBuffer(input))) <: CSV.Row
end

@testset "schemas with different column counts" begin
    narrow = "id,x,y\na,1,2\n"
    wide = "id,a,b,c,d\n1,1.5,2,3,\n2,0,0,0,4\n"
    @test isequal(schemarows(wide; schema=SCHEMA5),
                  [(id=1, a=1.5, b=2.0, c=3.0, d=missing), (id=2, a=0.0, b=0.0, c=0.0, d=4.0)])
    @test schemarows(narrow) == [(id="a", x=1.0, y=Int8(2))]
    @test schemaproblem(wide) == (:header_mismatch, 0, 2)
    @test schemaproblem(narrow; schema=SCHEMA5) == (:header_mismatch, 0, 2)
end

@testset "the header must name the schema's columns in order" begin
    for (input, col, message) in [
            ("id,z,y\na,1,2\n", 2, "the header does not match the schema; missing columns: x; extra columns: z"),
            ("id,y,x\na,1,2\n", 2, "the header names column 2 y; the schema expects x"),
            ("id,x,x\na,1,2\n", 3, "the header repeats the name x (columns 2 and 3)"),
            ("id,x\na,1\n", 3, "the header does not match the schema; missing columns: y"),
            ("id,x,y,w\na,1,2,3\n", 4, "the header does not match the schema; extra columns: w"),
            ("id,w\na,1\n", 2, "missing columns: x, y; extra columns: w"),
            ("", 1, "missing columns: id, x, y"),
        ]
        err = schemaerror(input)
        @test err isa CSV.ParseError
        @test (err.problem.kind, err.problem.row, err.problem.col) == (:header_mismatch, 0, col)
        @test occursin(message, err.problem.message)
    end
    # The problem points at the header field that differs.
    @test schemaerror("id,z,y\na,1,2\n").problem.pos == 4
    # A repeated name is a mismatch even when the schema uses the unique name
    # that CSV.File gives the repeat.
    repeated = "id,x,x\na,1,2\n"
    @test propertynames(CSV.File(IOBuffer(repeated))) == [:id, :x, :x_1]
    @test schemaproblem(repeated; schema=Tables.Schema((:id, :x, :x_1), (String, Int, Int))) ==
          (:header_mismatch, 0, 3)
    # Malformed quoting in the header fails the read.
    @test schemaproblem("id,\"x\"z,y\na,1,2\n") == (:invalid_quoted_field, 0, 2)
    # Names are compared after normalizenames.
    @test schemarows("first name,x\nab,1\n"; schema=Tables.Schema((:first_name, :x), (String, Int)),
                     normalizenames=true) == [(first_name="ab", x=1)]
    # A later header row, a two-row header, and no header row at all.
    @test schemarows("junk\nid,x,y\na,1,2\n"; header=2) == [(id="a", x=1.0, y=Int8(2))]
    @test schemarows("id,x\nA,B\nq,1\n"; schema=Tables.Schema((:id_A, :x_B), (String, Int)),
                     header=[1, 2]) == [(id_A="q", x_B=1)]
    @test isequal(schemarows("a,1,2\nb,3,\n"; header=false),
                  ROW3[("a", 1.0, Int8(2)), ("b", 3.0, missing)])
    @test schemarows("a,1,2\n"; skipto=1) == [(id="a", x=1.0, y=Int8(2))]
    @test schemaproblem("a,1\n"; header=false) == (:short_row, 1, 0)
end

@testset "every cell parses as its column type" begin
    @test schemaproblem("id,x,y\na,1,2\nb,abc,3\n") == (:invalid_value, 2, 2)
    input = "id,x,y\na,1,2\nb,abc,3\n"
    err = schemaerror(input)
    @test err.problem.pos == first(findfirst("abc", input))
    @test err.problem.message == "cannot parse Float64 from \"abc\""
    @test err.source == "<GenericIOBuffer>"
    @test schemaproblem("id,x,y\na,1,128\n") == (:invalid_value, 1, 3)    # Int8 range
    @test schemaproblem("id,x,y\na,1,1.5\n") == (:invalid_value, 1, 3)    # no truncation
    @test schemaproblem("id,x,y\na,\"\",1\n") == (:invalid_value, 1, 2)   # quoted empty is a value
    @test schemaproblem("id,x,y\n\"a\"b,1,2\n") == (:invalid_quoted_field, 1, 1)
    @test schemaproblem("id,x,y\na,1,2\n\"b,1,2\n") == (:unclosed_quote, 0, 0)
    @test schemaproblem("id,x,y\na,1,2\nb,1\n") == (:short_row, 2, 0)
    @test schemaproblem("id,x,y\na,1,2,3\n") == (:long_row, 1, 0)
    # Integers overflow; floats round to the nearest Float64, as Base.parse does.
    s = Tables.Schema((:n, :f), (Int64, Float64))
    @test schemaproblem("n,f\n9223372036854775808,1\n"; schema=s) == (:invalid_value, 1, 1)
    @test schemarows("n,f\n9223372036854775807,1e400\n"; schema=s) ==
          [(n=typemax(Int64), f=Inf)]
    @test schemarows("n,f\n-9223372036854775808,-1e-400\n"; schema=s)[1].f === -0.0
    # An integer spelling is a Float64; Bool takes its spellings only.
    @test schemarows("id,x,y\na,7,1\n")[1].x === 7.0
    sb = Tables.Schema((:b,), (Bool,))
    @test schemaproblem("b\n1\n"; schema=sb) == (:invalid_value, 1, 1)
    @test schemarows("b\nTRUE\nfalse\n"; schema=sb) == [(b=true,), (b=false,)]
    @test schemarows("b\nY\n"; schema=sb, truestrings=["Y"], falsestrings=["N"]) == [(b=true,)]
    # Rows past `limit` are never read.
    @test length(schemarows("id,x,y\na,1,2\nb,bad,3\n"; limit=1)) == 1
    @test length(schemarows("id,x,y\na,1,2\n\"b,1,2\n"; limit=1)) == 1
    # Iteration stops at the first invalid row.
    rows = CSV.Rows(IOBuffer("id,x,y\na,1,2\nb,bad,3\n"); schema=SCHEMA3)
    row, state = iterate(rows)
    @test row == (id="a", x=1.0, y=Int8(2))
    @test_throws CSV.ParseError iterate(rows, state)
end

@testset "missing values need a Union with Missing" begin
    @test schemaproblem("id,x,y\n,1,2\n") == (:missing_value, 1, 1)
    @test schemaproblem("id,x,y\na,,2\n") == (:missing_value, 1, 2)
    @test schemaerror("id,x,y\na,,2\n").problem.message == "missing value in a Float64 column"
    @test schemaproblem("id,x,y\na,NA,2\n"; missingstring="NA") == (:missing_value, 1, 2)
    @test ismissing(schemarows("id,x,y\na,1,NA\n"; missingstring="NA")[1].y)
    # A quoted empty field is a present, empty string.
    @test schemarows("id,x,y\n\"\",1,2\n")[1].id == ""
    # A kept empty row is one empty field.
    @test schemaproblem("id,x,y\na,1,2\n\nb,1,2\n"; ignoreemptyrows=false) == (:short_row, 2, 0)
    s1 = Tables.Schema((:y,), (Union{Missing, Int},))
    @test isequal(schemarows("y\n1\n\n2\n"; schema=s1, ignoreemptyrows=false),
                  [(y=1,), (y=missing,), (y=2,)])
end

@testset "schema reads use the dialect and value options" begin
    row(input; kw...) = only(schemarows(input; kw...))
    # a sniffed tab delimiter, a BOM, CRLF row endings, a quoted row ending
    @test isequal(schemarows("﻿id\tx\ty\r\n\"two\r\nlines\"\t0.25\t\r\n\"\"\"q\"\"\"\t-0.0\t127\r\n"),
                  ROW3[("two\r\nlines", 0.25, missing), ("\"q\"", -0.0, Int8(127))])
    @test row("id;x;y\na;1,5;2\n"; delim=';', decimal=',') == (id="a", x=1.5, y=Int8(2))
    @test row("id,x,y\n\"1.234,5\",\"2.000,5\",1\n"; decimal=',', groupmark='.') ==
          (id="1.234,5", x=2000.5, y=Int8(1))
    @test row("id,x,y\n'a\\'b',1,2\n"; quotechar='\'', escapechar='\\').id == "a'b"
    @test row("# note\nid,x,y\n# another\na,1,2\n"; comment="#").id == "a"
    @test row("id,x,y\n a , 1 ,2\n").id == " a "
    @test row("id,x,y\n a , 1 ,2\n"; stripwhitespace=true) == (id="a", x=1.0, y=Int8(2))
    @test row("id||x|y\na||1|2\n"; delim='|', ignorerepeated=true).x == 1.0
    @test row("id::x::y\na::1::2\n"; delim="::").y == 2
    @test schemarows("id,x,y\nskip,0,0\na,1,2\nb,3,4\ntotal\n"; skipto=3, footerskip=1) ==
          [(id="a", x=1.0, y=Int8(2)), (id="b", x=3.0, y=Int8(4))]
    sd = Tables.Schema((:d, :t, :ts), (Date, DateTime, Timestamp{Nanosecond}))
    @test row("d,t,ts\n2024/01/02,2024-01-02T03:04:05,2024-01-02T03:04:05.123456789\n"; schema=sd,
              dateformat=Dict(:d => "yyyy/mm/dd")) ==
          (d=Date(2024, 1, 2), t=DateTime(2024, 1, 2, 3, 4, 5),
           ts=Timestamp{Nanosecond}(2024, 1, 2, 3, 4, 5, 123, 456, 789))
    @test schemaproblem("d,t,ts\n2024-01-02,2024-01-02T03:04:05,x\n"; schema=sd,
                        dateformat=Dict(:d => "yyyy/mm/dd")) == (:invalid_value, 1, 1)
    # A bare quote makes the reader index again under the lenient quote rule.
    @test row("id,x,y\n5' 11\",1,2\n").id == "5' 11\""
end

@testset "text column types" begin
    s = Tables.Schema((:s, :d, :y), (String, DataString, Symbol))
    row = only(schemarows("s,d,y\nab,\"c,\"\"d\",sym\n"; schema=s))
    @test row isa NamedTuple{(:s, :d, :y), Tuple{String, DataString, Symbol}}
    @test row == (s="ab", d="c,\"d", y=:sym)
    long = "x"^40
    @test only(schemarows("s,d,y\n$long,$long,$long\n"; schema=s)).d == long
end

@testset "schema reads take paths, IO, and bytes, but not URLs" begin
    input = "id,x,y\na,1,2\n"
    expected = [(id="a", x=1.0, y=Int8(2))]
    mktemp() do path, io
        write(io, input)
        close(io)
        @test collect(CSV.Rows(path; schema=SCHEMA3)) == expected
    end
    @test collect(CSV.Rows(Vector{UInt8}(codeunits(input)); schema=SCHEMA3)) == expected
    @test collect(CSV.Rows(codeunits(input); schema=SCHEMA3)) == expected
    gz = transcode(GzipCompressor, Vector{UInt8}(codeunits(input)))
    @test collect(CSV.Rows(gz; schema=SCHEMA3)) == expected
    # A mapped file read with every thread agrees with the eager reader.
    mktemp() do path, io
        println(io, "id,x,y")
        for i in 1:60_000
            println(io, "k", i, ",", i / 4, ",", i % 5 == 0 ? "" : string(i % 100))
        end
        close(io)
        @test filesize(path) >= CSV.MMAP_THRESHOLD
        rows = CSV.Rows(path; schema=SCHEMA3)
        f = CSV.File(path; types=Dict(:id => String, :x => Float64, :y => Int8))
        @test length(rows) == length(f)
        @test isequal(Tables.columntable(rows), (id=f.id, x=f.x, y=f.y))
    end
    @test_throws ArgumentError CSV.Rows("https://example.invalid/data.csv"; schema=SCHEMA3)
    @test_throws ArgumentError CSV.Rows(joinpath(tempdir(), "csv-no-such-file.csv"); schema=SCHEMA3)
end

@testset "schema arguments" begin
    input = "id,x,y\na,1,2\n"
    for kw in ((; types=Dict(:x => Int)), (; stringtype=String), (; select=[:id]),
               (; drop=[:y]), (; on_error=:collect), (; strict=false),
               (; header=["id", "x", "y"]), (; ntasks=2), (; maxproblems=1))
        @test_throws ArgumentError CSV.Rows(IOBuffer(input); schema=SCHEMA3, kw...)
    end
    @test schemarows(input; strict=true) == schemarows(input; on_error=:error) ==
          [(id="a", x=1.0, y=Int8(2))]
    for bad in (Tables.Schema((:a,), (Vector{Int},)), Tables.Schema((:a,), (Real,)),
                Tables.Schema((:a,), (Missing,)), Tables.Schema((:a,), (Union{Int, Float64},)),
                Tables.Schema((:a,), (Any,)), Tables.Schema((:a, :a), (Int, Int)),
                Tables.Schema([:a], [Int]; stored=true), Tables.Schema((:a,), nothing))
        @test_throws ArgumentError CSV.Rows(IOBuffer("a\n1\n"); schema=bad)
    end
    @test_throws ArgumentError CSV.Rows(IOBuffer("a\n1\n"); schema=(a=Int,))
end

@testset "numeric schema rows iterate without allocating per row" begin
    s = Tables.Schema((:x, :y), (Float64, Union{Missing, Int}))
    source(n) = Vector{UInt8}(codeunits("x,y\n" * join(("$i.5,$(isodd(i) ? i : "")" for i in 1:n), '\n')))
    small = CSV.Rows(source(500); schema=s)
    large = CSV.Rows(source(5000); schema=s)
    sumx(small); sumx(large)
    @test sumx(large) == sum(i + 0.5 for i in 1:5000)
    @test @allocated(sumx(large)) == @allocated(sumx(small))
end

end # @testset schema reads
