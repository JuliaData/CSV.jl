using Test, JuliaC

# Run the trimmed program. A signal leaves `exitcode` at 0, so return it too.
function runexe(cmd::Cmd)
    out = IOBuffer()
    err = IOBuffer()
    p = run(pipeline(ignorestatus(cmd); stdout=out, stderr=err))
    return (out=String(take!(out)), code=p.exitcode, signal=p.termsignal,
            err=String(take!(err)))
end

# The lines the program prints for rows of the two schemas.
narrowline(i, id, x, y) = string(i, "|", repr(id), "|", x, "|", y === missing ? "missing" : y)
wideline(id, a, b, c, d) = string(id, "|", a, "|", b, "|", c, "|", d === missing ? "missing" : d)
lines(xs...) = join(xs, '\n') * '\n'
# A file for the 121-column schema: an id and `ncols` numbers per row.
manytext(rows; ncols=120) =
    lines(join(["id"; ["x$j" for j in 1:ncols]], ','),
          (join([string(id); [string(f(j)) for j in 1:ncols]], ',') for (id, f) in rows)...)

# Readers that must succeed: (mode, source text, expected output). Path modes
# read the text from a file written here; text modes pass it as an argument.
const ACCEPTED = [
    ("many", manytext([(1, j -> j / 4), (2, j -> -j)]), lines("1|1815.0|30.0", "2|-7260.0|-120.0")),
    ("narrow", "id,x,y\n\"a,\"\"q\"\"\",1.5,3\nb,-2e3,\nc,7,-128\n",
     lines(narrowline(1, "a,\"q\"", 1.5, 3), narrowline(2, "b", -2000.0, missing),
           narrowline(3, "c", 7.0, -128), "columns 3")),
    # sniffed tab, BOM, CRLF, a quoted row ending, and an escaped quote
    ("narrow", "﻿id\tx\ty\r\n\"two\r\nlines\"\t0.25\t\r\n\"\"\"q\"\"\"\t-0.0\t127\r\n",
     lines(narrowline(1, "two\r\nlines", 0.25, missing), narrowline(2, "\"q\"", -0.0, 127),
           "columns 2")),
    ("narrow-text", "id,x,y\nz,1e-3,0\n", lines(narrowline(1, "z", 0.001, 0), "columns 1")),
    ("narrow", "id,x,y\n", lines("columns 0")),
    ("wide", "# generated\nid;a;b;c;d\n1;1,5;2;-0,25;NA\n2;1e3;0;3;4,5\n",
     lines(wideline(1, 1.5, 2.0, -0.25, missing), wideline(2, 1000.0, 0.0, 3.0, 4.5),
           "columns 2")),
    ("wide-text", "id;a;b;c;d\n3;\"3,5\";1;1;\n",
     lines(wideline(3, 3.5, 1.0, 1.0, missing), "columns 1")),
]

# Readers that must fail: (mode, source text or path, exit code, output prefix).
# A schema mismatch or an invalid value is a CSV.ParseError (exit 2) at its
# data row and column; row 0 is the header and column 0 a whole row.
const REJECTED = [
    ("narrow", "id,z,y\na,1,2\n", 2, "ParseError header_mismatch row=0 col=2:"),
    ("narrow", "id,y,x\na,1,2\n", 2, "ParseError header_mismatch row=0 col=2:"),
    ("narrow", "id,x,x\na,1,2\n", 2, "ParseError header_mismatch row=0 col=3:"),
    ("narrow", "id,x\na,1\n", 2, "ParseError header_mismatch row=0 col=3:"),
    ("narrow", "id,x,y,w\na,1,2,3\n", 2, "ParseError header_mismatch row=0 col=4:"),
    ("narrow", "", 2, "ParseError header_mismatch row=0 col=1:"),
    # the wide file under the narrow schema, and the reverse
    ("narrow", "id;a;b;c;d\n1;1;1;1;1\n", 2, "ParseError header_mismatch row=0 col=2:"),
    ("wide", "id,x,y\na,1,2\n", 2, "ParseError header_mismatch row=0 col=1:"),
    ("narrow", "id,x,y\na,1,2\nb,abc,3\n", 2, "ParseError invalid_value row=2 col=2:"),
    ("narrow", "id,x,y\na,1,128\n", 2, "ParseError invalid_value row=1 col=3:"),
    ("narrow", "id,x,y\na,1,1.5\n", 2, "ParseError invalid_value row=1 col=3:"),
    ("narrow-text", "id,x,y\na,\"\",1\n", 2, "ParseError invalid_value row=1 col=2:"),
    ("narrow", "id,x,y\n,1,2\n", 2, "ParseError missing_value row=1 col=1:"),
    ("narrow", "id,x,y\na,,2\n", 2, "ParseError missing_value row=1 col=2:"),
    ("narrow", "id,x,y\na,1,2\nb,1\n", 2, "ParseError short_row row=2 col=0:"),
    ("narrow", "id,x,y\na,1,2,3\n", 2, "ParseError long_row row=1 col=0:"),
    ("narrow", "id,x,y\n\"a\"b,1,2\n", 2, "ParseError invalid_quoted_field row=1 col=1:"),
    ("narrow", "id,x,y\na,1,2\n\"b,1,2\n", 2, "ParseError unclosed_quote row=0 col=0:"),
    ("wide", "id;a;b;c;d\n1;1;NA;1;1\n", 2, "ParseError missing_value row=1 col=3:"),
    ("wide", "id;a;b;c;d\n9223372036854775808;1;1;1;1\n", 2,
     "ParseError invalid_value row=1 col=1:"),
    ("wide-text", "id;a;b;c;d\n1;1.5;1;1;1\n", 2, "ParseError invalid_value row=1 col=2:"),
    ("many", manytext([(1, j -> j)]; ncols=119), 2, "ParseError header_mismatch row=0 col=121:"),
    ("many", manytext([(1, j -> j)])[1:end-1] * ",5\n", 2, "ParseError long_row row=1 col=0:"),
    ("many", manytext([(1, j -> j), (2, j -> j == 77 ? "x" : j)]), 2,
     "ParseError invalid_value row=2 col=78:"),
]

@testset "trim compilation and execution" begin
    mktempdir() do dir
        exe = joinpath(dir, "workload")
        workload = joinpath(@__DIR__, "workload.jl")
        project = @__DIR__
        entry = "using JuliaC; if isdefined(JuliaC, :main); JuliaC.main(ARGS); else JuliaC._main_cli(ARGS); end"
        cmd = `$(Base.julia_cmd()) --startup-file=no --project=$project -e $entry -- --output-exe workload --project=$project --experimental --trim=safe $workload`
        log = joinpath(dir, "compile.log")
        result = open(log, "w") do io
            return run(pipeline(ignorestatus(Cmd(cmd; dir=dir)), stdout=io, stderr=io))
        end
        output = read(log, String)
        success(result) || print(output)
        @test success(result)
        @test !occursin(r"[1-9][0-9]* errors,", output)
        @test !occursin(r"[1-9][0-9]* warnings", output)
        @test isfile(exe)
        isfile(exe) || return
        @test occursin("trim workload passed", read(`$exe`, String))

        @testset "schema reads accept matching input: $mode $(repr(text))" for (mode, text, expected) in ACCEPTED
            arg = endswith(mode, "-text") ? text : (path = joinpath(dir, "input.csv"); write(path, text); path)
            r = runexe(`$exe $mode $arg`)
            @test (r.code, r.signal) == (0, 0)
            @test r.out == expected
            r.code == 0 || print(r.out, r.err)
        end

        # Rows before the invalid one were already printed: the error is the
        # last line.
        @testset "schema reads reject mismatched input: $mode $(repr(text))" for (mode, text, code, prefix) in REJECTED
            arg = endswith(mode, "-text") ? text : (path = joinpath(dir, "input.csv"); write(path, text); path)
            r = runexe(`$exe $mode $arg`)
            @test (r.code, r.signal) == (code, 0)
            reported = last(split(chomp(r.out), '\n'))
            @test startswith(reported, prefix)
            startswith(reported, prefix) || print(r.out, r.err)
        end

        @testset "schema reads reject sources they cannot read" begin
            r = runexe(`$exe narrow https://example.invalid/data.csv`)
            @test (r.code, r.signal) == (3, 0)
            @test startswith(r.out, "ArgumentError: a schema read takes a local path")
            r = runexe(`$exe narrow $(joinpath(dir, "absent.csv"))`)
            @test (r.code, r.signal) == (3, 0)
            @test startswith(r.out, "ArgumentError: no file at")
        end

        # A file large enough to map, prefetch, and index in several chunks,
        # read with one and with several threads. Every `x` is a multiple of
        # 1/4, so the sums are exact in any order.
        @testset "large file with $threads threads" for threads in (1, 4)
            n = 150_000
            path = joinpath(dir, "large.csv")
            open(path, "w") do io
                println(io, "id,x,y")
                for i in 1:n
                    println(io, "id", i, ",", i / 4, ",", i % 7 == 0 ? "" : string(i % 200 - 100))
                end
            end
            @test filesize(path) > 1 << 20
            ys = [i % 200 - 100 for i in 1:n if i % 7 != 0]
            expected = string("rows=", n, " xsum=", sum(i / 4 for i in 1:n), " ymissing=",
                              n - length(ys), " ysum=", sum(ys), "\n")
            r = runexe(addenv(`$exe narrow-summary $path`, "JULIA_NUM_THREADS" => string(threads)))
            @test (r.code, r.signal) == (0, 0)
            @test r.out == expected
        end
    end
end
