using Test, CSV, DataDecimals, DataStrings, Tables

@testset "Decimal schemas require concrete scalar types" begin
    for T in (DataDecimals.AbstractDecimal, DataDecimals.Decimal,
              DataDecimals.Decimal64, DataDecimals.DecimalValue),
        reader in (CSV.File, CSV.Rows, CSV.lazy, CSV.Chunks)
        @test_throws ArgumentError reader(IOBuffer("x\n1.20\n"); types=T)
    end
end

# CSV never infers a decimal type. With DataDecimals loaded, an explicitly
# requested decimal type parses exactly from the field bytes.
@testset "Explicit decimal schemas" begin
    D = DataDecimals.Decimal64{2}
    source = "amount,label\n1.20,one\n-23.40,two\n5,three\n,empty\n"
    @test eltype(CSV.File(IOBuffer(source)).amount) == Union{Missing,Float64}
    @test_throws ArgumentError CSV.File(IOBuffer(source); inferdecimal=true)
    for parallel in (false, true), chunkbytes in (8, 1024)
        f = CSV.File(IOBuffer(source); types=Dict(:amount => D), parallel, chunkbytes, nsample=1)
        @test eltype(f.amount) == Union{Missing,D}
        @test isequal(f.amount, [D("1.20"), D("-23.40"), D("5.00"), missing])
        @test f.label isa DataStrings.StringVector
        @test isempty(CSV.problems(f))
    end
    for reader in (CSV.File, CSV.lazy, CSV.Rows)
        quiet = reader === CSV.File ? (; on_error=:collect) : (;)
        f = reader(IOBuffer("x\n1.20\n1.235\n1.2300\n1.2e1\n"); types=Dict(:x=>D), quiet...)
        vals = collect(Tables.getcolumn(Tables.columntable(f), :x))
        @test isequal(vals, [D("1.20"), missing, D("1.23"), D("12.00")])
    end
    @test_throws CSV.ParseError CSV.File(IOBuffer("x\n1.235\n"); types=D, strict=true)
    @test CSV.File(IOBuffer("x\n1.234,50\n"); delim=';', decimal=',', groupmark='.', types=D).x == [D("1234.50")]
    chunks = collect(CSV.Chunks(IOBuffer(source); types=Dict(:amount => D), chunkbytes=8))
    @test all(f -> Base.nonmissingtype(eltype(f.amount)) === D, chunks)
    @test isequal(vcat([collect(f.amount) for f in chunks]...),
                  [D("1.20"), D("-23.40"), D("5.00"), missing])
    transposed = CSV.File(IOBuffer("amount,1.20,2.30\n"); transpose=true, types=D)
    @test transposed.amount == D[D("1.20"), D("2.30")]
    f = CSV.File(IOBuffer(source); types=Dict(:amount => D))
    io = IOBuffer()
    CSV.write(io, f)
    roundtrip = CSV.File(IOBuffer(take!(io)); types=Dict(:amount => D))
    @test isequal(roundtrip.amount, f.amount)
    filtered = CSV.File(IOBuffer("amount,keep\n1.20,1\n2.30,1\n3.456,0\n");
                        scan=Tables.Scan(select=(:amount => D,), filter=Tables.colcmp(==, Tables.col(:keep), 1)))
    @test eltype(filtered.amount) === D
    @test filtered.amount == [D("1.20"), D("2.30")]
    for (digits, T) in [(18, DataDecimals.Decimal128{2}), (38, DataDecimals.Decimal256{2})]
        v = repeat("9", digits) * ".12"
        f = CSV.File(IOBuffer("x\n$v\n$v\n"); types=T)
        @test f.x == [T(v), T(v)]
    end
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

@testset "Decimal spelling boundaries" begin
    D = DataDecimals.Decimal64{2}
    for (token, expected) in [("+1.20", D("1.20")), ("1.2000e1", D("12.00")),
                              ("120e-2", D("1.20")), ("-0.00", D("0")),
                              ("1.201e-1", missing), ("1e-10000000", missing),
                              ("1e10000000", missing), ("1e+", missing),
                              ("+", missing), (".", missing), ("1.2.3", missing),
                              ("1e-2x", missing)]
        f = CSV.File(IOBuffer("x\n$token\n"); delim=',', types=D, on_error=:collect)
        @test isequal(only(f.x), expected)
    end
    @test eltype(CSV.File(IOBuffer("x\n1.20\nNA\n2.30\n"); types=D, missingstring="NA").x) === Union{Missing,D}
    @test eltype(CSV.File(IOBuffer("x\n\"1.20\"\n\"2.30\"\n"); types=D).x) === D
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
