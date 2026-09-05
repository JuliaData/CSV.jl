using Test, CSV, DataDecimals, DataStrings, Tables

@testset "Exact decimal schemas and inference" begin
    D = DataDecimals.Decimal64{2}
    source = "amount,label\n1.20,one\n-23.40,two\n5,three\n,empty\n"
    @test eltype(CSV.File(IOBuffer(source)).amount) == Union{Missing,Float64}
    for parallel in (false, true), chunkbytes in (8, 1024)
        f = CSV.File(IOBuffer(source); inferdecimal=true, parallel, chunkbytes, nsample=1)
        @test eltype(f.amount) == Union{Missing,D}
        @test isequal(f.amount, [D("1.20"), D("-23.40"), D("5.00"), missing])
        @test f.label isa DataStrings.StringVector
        @test isempty(CSV.problems(f))
    end
    for reader in (CSV.File, CSV.lazy, CSV.Rows)
        f = reader(IOBuffer("x\n1.20\n1.235\n1.2300\n1.2e1\n"); types=Dict(:x=>D))
        vals = collect(Tables.getcolumn(Tables.columntable(f), :x))
        @test isequal(vals, [D("1.20"), missing, D("1.23"), D("12.00")])
    end
    @test_throws ErrorException CSV.File(IOBuffer("x\n1.235\n"); types=D, strict=true)
    grouped = CSV.File(IOBuffer("x;y\n1.234,50;a\n2.000,00;b\n");
                       delim=';', decimal=',', groupmark='.', inferdecimal=true)
    @test grouped.x == D[D("1234.50"), D("2000.00")]
    @test CSV.File(IOBuffer("x\n1.234,50\n"); delim=';', decimal=',', groupmark='.', types=D).x == [D("1234.50")]

    for values in (["1.20", "2.345"], ["1.20", "2.30", "3.456"],
                   ["1.20", "2.30e0"], ["1.20", "-0.00"],
                   ["1.20", "NaN"], ["1.20", "Inf"], ["1.20", "2"])
        f = CSV.File(IOBuffer("x\n" * join(values, '\n') * "\n"); inferdecimal=true, nsample=1)
        @test eltype(f.x) === Float64
    end
    limited = CSV.File(IOBuffer("x\n1.20\n2.30\n3.456\n"); inferdecimal=true, limit=2)
    @test eltype(limited.x) === D
    @test eltype(CSV.File(IOBuffer("x\n1\n2\n"); inferdecimal=true).x) === Int64
    @test eltype(CSV.File(IOBuffer("x\n1.20\n2.30\n"); inferdecimal=true, types=Float64).x) === Float64
    @test eltype(CSV.File(IOBuffer("x\n1.20\n2.30\n"); inferdecimal=true,
                         typemap=Dict(D=>Float64)).x) === Float64
    @test eltype(CSV.File(IOBuffer("x\n0001.20\n0002.30\n"); inferdecimal=true).x) === D
    @test eltype(CSV.File(IOBuffer("x\n.12\n.34\n"); inferdecimal=true).x) === D
    for (digits, T) in [(18, DataDecimals.Decimal128{2}), (38, DataDecimals.Decimal256{2})]
        v = repeat("9", digits) * ".12"
        f = CSV.File(IOBuffer("x\n$v\n$v\n"); inferdecimal=true)
        @test eltype(f.x) === T
        @test f.x == [T(v), T(v)]
    end
    too_wide = repeat("9", 75) * ".12"
    @test eltype(CSV.File(IOBuffer("x\n$too_wide\n$too_wide\n"); inferdecimal=true).x) === Float64
    chunks = collect(CSV.Chunks(IOBuffer(source); inferdecimal=true, chunkbytes=8))
    @test all(f -> Base.nonmissingtype(eltype(f.amount)) === D, chunks)
    @test isequal(vcat([collect(f.amount) for f in chunks]...),
                  [D("1.20"), D("-23.40"), D("5.00"), missing])
    transposed = CSV.File(IOBuffer("amount,1.20,2.30\n"); transpose=true, inferdecimal=true)
    @test transposed.amount == D[D("1.20"), D("2.30")]
    f = CSV.File(IOBuffer(source); inferdecimal=true)
    io = IOBuffer()
    CSV.write(io, f)
    roundtrip = CSV.File(IOBuffer(take!(io)); inferdecimal=true)
    @test isequal(roundtrip.amount, f.amount)
    filtered = CSV.File(IOBuffer("amount,keep\n1.20,1\n2.30,1\n3.456,0\n");
                        inferdecimal=true,
                        scan=Tables.Scan(select=(:amount,), filter=Tables.colcmp(==, Tables.col(:keep), 1)))
    @test eltype(filtered.amount) === D
    @test filtered.amount == [D("1.20"), D("2.30")]
end

@testset "DataStrings column ownership" begin
    f = CSV.File(IOBuffer("text\na long original string\nshort\n"); delim=',')
    held = f.text[1]
    f.text[1] = "a long replacement string"
    push!(f.text, "appended")
    @test held == "a long original string"
    @test collect(f.text) == ["a long replacement string", "short", "appended"]
    @test f.text isa DataStrings.StringVector{DataString}
end

@testset "Decimal spelling boundaries and row windows" begin
    D = DataDecimals.Decimal64{2}
    for (token, expected) in [("+1.20", D("1.20")), ("1.2000e1", D("12.00")),
                              ("120e-2", D("1.20")), ("-0.00", D("0")),
                              ("1.201e-1", missing), ("1e-10000000", missing),
                              ("1e10000000", missing), ("1e+", missing),
                              ("+", missing), (".", missing), ("1.2.3", missing),
                              ("1e-2x", missing)]
        f = CSV.File(IOBuffer("x\n$token\n"); delim=',', types=D)
        @test isequal(only(f.x), expected)
    end
    @test isempty(CSV.File(IOBuffer("x\n1.20\n2.30\n"); inferdecimal=true, limit=0).x)
    @test eltype(CSV.File(IOBuffer("x\n1.235\n1.20\n2.30\n9.456\n");
                         inferdecimal=true, skipto=3, footerskip=1).x) === D
    @test eltype(CSV.File(IOBuffer("x\n1.20\nNA\n2.30\n"); inferdecimal=true,
                         missingstring="NA").x) === Union{Missing,D}
    @test eltype(CSV.File(IOBuffer("x\n\"1.20\"\n\"2.30\"\n"); inferdecimal=true).x) === D
    @test eltype(CSV.File(CSV.lazy(IOBuffer("x\n1.20\n2.30\n")); inferdecimal=true).x) === D
    @test eltype(CSV.File(IOBuffer("x\n0.00\n0.00\n"); inferdecimal=true).x) === D
    @test eltype(CSV.File(IOBuffer("x\n1.20\n2.30\ntext\n"); inferdecimal=true).x) === DataString
    decimalvalue = CSV.File(IOBuffer("x\n1.234\n2.5\n"); types=DataDecimals.DecimalValue{Int64})
    @test DataDecimals.scale.(decimalvalue.x) == [3,1]
end

@testset "Decimal writer dialect" begin
    D = DataDecimals.Decimal64{2}
    table = (amount=D[D("1.20"), D("-2.30")],)
    for ntasks in (1, 2), delim in (',', ';')
        io = IOBuffer()
        CSV.write(io, table; decimal=',', delim, ntasks)
        text = String(take!(io))
        expected = delim == ',' ? "amount\n\"1,20\"\n\"-2,30\"\n" : "amount\n1,20\n-2,30\n"
        @test text == expected
        @test CSV.File(IOBuffer(text); decimal=',', delim, types=D).amount == table.amount
    end
    @test collect(CSV.RowWriter(table; decimal=',')) == ["amount\n", "\"1,20\"\n", "\"-2,30\"\n"]
end
