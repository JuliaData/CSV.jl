# Differential test suite for the new value layer.
#
# Run:  julia --project=kernel kernel/test_values.jl
#
# Base.parse / Dates are the ORACLES here (tests only — the runtime layer never
# calls them). Every parser must agree bit-for-bit with the oracle on the
# accept-set, and the accept-set deltas (strictness: no whitespace, no "1" Bool,
# etc.) are pinned explicitly.

using Test, Random, Dates

isdefined(Main, :KernelValues) || include(joinpath(@__DIR__, "values.jl"))
using .KernelValues
using .KernelValuesDates
const V = KernelValues

b(s) = Vector{UInt8}(codeunits(s))
pint(s) = V.parseint64(b(s), 1, ncodeunits(s))
pflt(s) = V.parsefloat64(b(s), 1, ncodeunits(s))
pbool(s) = V.parsebool(b(s), 1, ncodeunits(s))

@testset "KernelValues" begin

@testset "parseint64: oracle differential" begin
    for s in ("0", "-0", "+0", "1", "-1", "42", "123456789", "-123456789",
              "9223372036854775807", "-9223372036854775808", "00042", "-007",
              "1234567890123456789")
        v, rc = pint(s)
        @test rc == V.RC_OK
        @test v == parse(Int64, s)
    end
    # overflow: well-formed digits, out of range → RC_OVERFLOW (lattice cue)
    for s in ("9223372036854775808", "-9223372036854775809",
              "99999999999999999999999999", "18446744073709551616")
        v, rc = pint(s)
        @test rc == V.RC_OVERFLOW
        @test_throws OverflowError parse(Int64, s)
    end
    # invalid
    for s in ("", "-", "+", "abc", "1a", "1.5", " 1", "1 ", "--1", "1-", "0x10")
        @test pint(s)[2] == V.RC_INVALID
    end
    rng = MersenneTwister(42)
    for _ in 1:200_000
        x = rand(rng, Int64)
        s = string(x)
        v, rc = pint(s)
        @test rc == V.RC_OK && v == x
    end
    # random digit strings of every length 1..25
    for len in 1:25, _ in 1:2_000
        s = (rand(rng, Bool) ? "-" : "") * String(rand(rng, '0':'9', len))
        v, rc = pint(s)
        or = tryparse(Int64, s)
        if or === nothing
            @test rc == V.RC_OVERFLOW
        else
            @test rc == V.RC_OK && v == or
        end
    end
end

@testset "parsefloat64: pinned adversaries" begin
    cases = [
        "0.0", "-0.0", "1.0", "0.1", "2.5", "1e0", "1e1", "1e-1", "1E5",
        "1.7976931348623157e308",      # maxfloat
        "1.7976931348623159e308",      # rounds to Inf
        "4.9406564584124654e-324",     # min subnormal
        "5e-324", "4.9e-324", "2.4703282292062327e-324",  # subnormal boundary (rounds to 0)
        "2.2250738585072014e-308",     # min normal
        "2.2250738585072011e-308",     # the notorious PHP hang value
        "9007199254740993",            # 2^53+1 (halfway)
        "9007199254740993.0",
        "1.00000000000000011102230246251565404236316680908203125",  # exactly representable long
        "0.500000000000000166533453693773481063544750213623046875",
        "1e308", "1e309", "1e-308", "1e-309", "1e-400", "1e400",
        "123456789012345678901234567890.123456789e-25",
        "3.141592653589793", "2.718281828459045",
        "0.000001", "1000000.0", "1e23", "8.98846567431158e307",
        "1" * "0"^100, "0." * "0"^100 * "1e103",
        # 768-digit halfway stress: forces the SDC tier
        "0." * "5"^400, "1." * "0"^300 * "1",
        "7.2057594037927933e16",
        "Inf", "-Inf", "+inf", "INFINITY", "-infinity", "NaN", "-nan", "+NAN",
    ]
    for s in cases
        v, rc = pflt(s)
        @test rc == V.RC_OK
        o = tryparse(Float64, s)
        if o === nothing
            # pinned delta: Base rejects ERANGE both directions; this layer
            # returns ±Inf (overflow) / ±0.0 (underflow) with OK
            @test isinf(v) || v == 0.0
        else
            @test (isnan(v) && isnan(o)) || reinterpret(UInt64, v) == reinterpret(UInt64, o)
        end
    end
    # Compact subnormals are an Eisel-Lemire case, not an exact-decimal case.
    # This route pin prevents a few real tier-3 calls from dominating a corpus
    # benchmark and being mistaken for wrapper/compiler overhead.
    for s in ("5e-324", "4.9e-324", "2.4703282292062327e-324",
              "1e-320", "2.2250738585072011e-308")
        _, rc, done = V._parsefloat_core(b(s), 1, ncodeunits(s), UInt8('.'))
        @test rc == V.RC_OK
        @test done
    end
    # Exact midpoint between zero and the minimum subnormal is 2^-1075.
    # Put the deciding ±1 decimal digit beyond HPD's 800 stored significant
    # digits. This pins both the 768-digit decision bound and sticky-tail use.
    decfrac(n, scale) = (d = string(n); "0." * "0"^(scale - length(d)) * d)
    midpoint = big(5)^1075 * big(10)^49
    for (n, bits) in ((midpoint - 1, UInt64(0)),
                      (midpoint, UInt64(0)),
                      (midpoint + 1, UInt64(1)))
        s = decfrac(n, 1124)
        buf = b(s)
        @test !V._parsefloat_core(buf, 1, length(buf), UInt8('.'))[3]
        v, rc = V.parsefloat64(buf, 1, length(buf))
        @test rc == V.RC_OK
        @test reinterpret(UInt64, v) == bits
    end
    for s in ("", ".", "-", "e5", "1e", "1e+", "1..2", "1.2.3", "1f5", " 1.0", "1.0 ", "nanx", "infs")
        @test pflt(s)[2] == V.RC_INVALID
    end
end

@testset "parsefloat64: round-trip (shortest repr) differential" begin
    rng = MersenneTwister(7)
    n = 0
    while n < 300_000
        bits = rand(rng, UInt64)
        x = reinterpret(Float64, bits)
        (isnan(x) || isinf(x)) && continue
        n += 1
        s = string(x)                       # Ryu shortest — must round-trip exactly
        v, rc = pflt(s)
        @test rc == V.RC_OK
        @test reinterpret(UInt64, v) == reinterpret(UInt64, x)
    end
end

@testset "parsefloat64: random decimal-string differential vs Base" begin
    rng = MersenneTwister(11)
    for _ in 1:150_000
        mant = String(rand(rng, '0':'9', rand(rng, 1:24)))
        frac = rand(rng, Bool) ? "." * String(rand(rng, '0':'9', rand(rng, 1:24))) : ""
        ex = rand(rng, Bool) ? "e" * string(rand(rng, -330:330)) : ""
        s = (rand(rng, Bool) ? "-" : "") * mant * frac * ex
        v, rc = pflt(s)
        o = tryparse(Float64, s)
        @test rc == V.RC_OK
        if o === nothing
            # pinned delta: Base rejects ERANGE both directions (overflow AND
            # underflow-to-zero); this layer returns ±Inf / ±0.0 with OK
            @test isinf(v) || v == 0.0
        else
            @test (isnan(v) && isnan(o)) || reinterpret(UInt64, v) == reinterpret(UInt64, o)
        end
    end
    # long-mantissa SDC pressure
    for _ in 1:2_000
        # Cross the 800-digit storage cap in the general differential corpus.
        s = "0." * String(rand(rng, '0':'9', rand(rng, 100:1_200))) * "e" * string(rand(rng, -300:300))
        v, rc = pflt(s)
        o = tryparse(Float64, s)
        @test rc == V.RC_OK
        if o === nothing
            # pinned delta: Base rejects ERANGE both directions (overflow AND
            # underflow-to-zero); this layer returns ±Inf / ±0.0 with OK
            @test isinf(v) || v == 0.0
        else
            @test (isnan(v) && isnan(o)) || reinterpret(UInt64, v) == reinterpret(UInt64, o)
        end
    end
end

@testset "parsebool + strictness pins" begin
    @test pbool("true") == (true, V.RC_OK)
    @test pbool("false") == (false, V.RC_OK)
    # strictness: parse-set ≡ detect-set — these are all INVALID by design
    for s in ("True", "TRUE", "1", "0", "t", "f", "yes", "no", "")
        @test pbool(s)[2] == V.RC_INVALID
    end
end

@testset "findcontent + matchsentinel" begin
    q = UInt8('"')
    fc(s) = V.findcontent(b(s), 1, ncodeunits(s), q, q, q)
    @test fc("plain") == (1, 5, false, V.RC_OK)
    @test fc("\"quoted\"") == (2, 6, false, V.RC_OK)
    @test fc("\"a\"\"b\"") == (2, 4, true, V.RC_OK)
    @test fc("\"\"") == (2, 0, false, V.RC_OK)
    @test fc("\"unterminated")[4] == V.RC_INVALID
    @test fc("\"x\"y")[4] == V.RC_INVALID
    bs = UInt8('\\')
    fce(s) = V.findcontent(b(s), 1, ncodeunits(s), q, q, bs)
    @test fce("\"a\\\"b\"") == (2, 4, true, V.RC_OK)
    @test fce("\"a\\\"")[4] == V.RC_INVALID       # escaped close, no true close
    @test fce("\"a\"tail")[4] == V.RC_INVALID     # bytes after true close
    sents = [b("NA"), b("NULL")]
    @test V.matchsentinel(b("NA"), 1, 2, sents)
    @test V.matchsentinel(b("NULL"), 1, 4, sents)
    @test !V.matchsentinel(b("NAN"), 1, 3, sents)
    @test !V.matchsentinel(b("na"), 1, 2, sents)
end

@testset "civil: daysfromcivil vs Dates oracle" begin
    for y in (-4000, -1900, -400, -100, -4, -1, 0,
              1, 100, 1583, 1600, 1900, 1970, 2000, 2020, 2024, 2100, 2400, 9999)
        for m in 1:12, d in (1, 15, 28)
            @test V.daysfromcivil(y, m, d) == Dates.value(Date(y, m, d))
        end
    end
    # every day across two centuries
    dt = Date(1900, 1, 1)
    while dt <= Date(2100, 12, 31)
        @test V.daysfromcivil(year(dt), month(dt), day(dt)) == Dates.value(dt)
        dt += Day(1)
    end
end

@testset "civil: ISO patterns" begin
    c, rc = V.parsecivil(b("2024-02-29"), 1, 10, V.ISO_DATE)
    @test rc == V.RC_OK && todate(c) == Date(2024, 2, 29)
    @test V.parsecivil(b("2023-02-29"), 1, 10, V.ISO_DATE)[2] == V.RC_INVALID  # not a leap year
    @test V.parsecivil(b("2024-13-01"), 1, 10, V.ISO_DATE)[2] == V.RC_INVALID
    @test V.parsecivil(b("2024-00-01"), 1, 10, V.ISO_DATE)[2] == V.RC_INVALID
    @test V.parsecivil(b("24-01-01"), 1, 8, V.ISO_DATE)[2] == V.RC_INVALID    # fixed-width year
    s = "2024-01-02T03:04:05"
    c, rc = V.parsecivil(b(s), 1, ncodeunits(s), V.ISO_DATETIME)
    @test rc == V.RC_OK && todatetime(c) == DateTime(2024, 1, 2, 3, 4, 5)
    s = "2024-01-02T03:04:05.125"
    c, rc = V.parsecivil(b(s), 1, ncodeunits(s), V.ISO_DATETIME)
    @test rc == V.RC_OK && todatetime(c) == DateTime(2024, 1, 2, 3, 4, 5, 125)
    s = "10:30:00"
    c, rc = V.parsecivil(b(s), 1, 8, V.ISO_TIME)
    @test rc == V.RC_OK && totime(c) == Time(10, 30)
    s = "10:30:00.000000001"
    c, rc = V.parsecivil(b(s), 1, ncodeunits(s), V.ISO_TIME)
    @test rc == V.RC_OK && totime(c) == Time(10, 30, 0) + Nanosecond(1)
    @test V.parsecivil(b("25:00:00"), 1, 8, V.ISO_TIME)[2] == V.RC_INVALID
    @test V.parsecivil(b("23:60:00"), 1, 8, V.ISO_TIME)[2] == V.RC_INVALID
    @test V.parsecivil(b("23:59:60"), 1, 8, V.ISO_TIME)[2] == V.RC_INVALID
    @test V.parsecivil(b("23:59:59.1234567890"), 1, 19, V.ISO_TIME)[2] == V.RC_INVALID
    # whole-span rule
    @test V.parsecivil(b("2024-01-02x"), 1, 11, V.ISO_DATE)[2] == V.RC_INVALID
end

@testset "civil: custom patterns (the kernel's test formats)" begin
    p = V.compilepattern("yyyymmdd")
    c, rc = V.parsecivil(b("20240102"), 1, 8, p)
    @test rc == V.RC_OK && todate(c) == Date(2024, 1, 2)
    p = V.compilepattern("dd/mm/yyyy")
    c, rc = V.parsecivil(b("15/01/2023"), 1, 10, p)
    @test rc == V.RC_OK && todate(c) == Date(2023, 1, 15)
    p = V.compilepattern("u dd yyyy")
    c, rc = V.parsecivil(b("Jan 02 2024"), 1, 11, p)
    @test rc == V.RC_OK && todate(c) == Date(2024, 1, 2)
    c, rc = V.parsecivil(b("jul 04 1776"), 1, 11, p)
    @test rc == V.RC_OK && todate(c) == Date(1776, 7, 4)
    @test V.parsecivil(b("Foo 02 2024"), 1, 11, p)[2] == V.RC_INVALID
    @test_throws ArgumentError V.compilepattern("yyyy-Qq")
    @test_throws ArgumentError V.compilepattern("y"^256)
    # Large year fields are invalid data, not conversion exceptions.
    pwide = V.compilepattern("yyyyyyyyyy")
    @test V.parsecivil(b("9999999999"), 1, 10, pwide)[2] == V.RC_INVALID
    phuge = V.compilepattern("y"^19)
    @test V.parsecivil(b("9999999999999999999"), 1, 19, phuge)[2] == V.RC_INVALID
    # differential against Dates for a spread of dates and formats
    for (fmt, dfmt) in (("yyyy-mm-dd", dateformat"yyyy-mm-dd"),
                        ("dd/mm/yyyy", dateformat"dd/mm/yyyy"),
                        ("yyyymmdd", dateformat"yyyymmdd"))
        p = V.compilepattern(fmt)
        dt = Date(1980, 1, 1)
        while dt < Date(2040, 1, 1)
            s = Dates.format(dt, dfmt)
            c, rc = V.parsecivil(b(s), 1, ncodeunits(s), p)
            @test rc == V.RC_OK && todate(c) == dt
            dt += Day(97)
        end
    end
end

@testset "parsebigint: oracle differential" begin
    rng = MersenneTwister(23)
    for len in (1, 5, 17, 18, 19, 20, 37, 100, 300), _ in 1:500
        s = (rand(rng, Bool) ? "-" : "") * String(rand(rng, '0':'9', len))
        v, rc = V.parsebigint(b(s), 1, ncodeunits(s))
        @test rc == V.RC_OK
        @test v == parse(BigInt, s)
    end
    for s in ("0", "-0", "+7", "00042", "9" ^ 1000)
        v, rc = V.parsebigint(b(s), 1, ncodeunits(s))
        @test rc == V.RC_OK && v == parse(BigInt, s)
    end
    for s in ("", "-", "+", "1.5", "1e5", " 1", "1 ", "0x10", "--1")
        @test V.parsebigint(b(s), 1, ncodeunits(s))[2] == V.RC_INVALID
    end
end

@testset "parsebigfloat: oracle differential (mpfr_strtofr)" begin
    rng = MersenneTwister(29)
    check(s) = begin
        v, rc = V.parsebigfloat(b(s), 1, ncodeunits(s))
        @test rc == V.RC_OK
        o = parse(BigFloat, s)
        @test (isnan(v) && isnan(o)) || (v == o && signbit(v) == signbit(o))
    end
    for prec in (2, 24, 53, 65, 113, 256, 1000)
        setprecision(BigFloat, prec) do
            # pinned adversaries (halfway cases matter at every precision)
            for s in ("0.1", "-0.1", "1.5", "1.75", "1.7500000000000000000001",
                      "2.5", "1e0", "9007199254740993",
                      "0." * "5"^400, "1." * "0"^300 * "1",
                      "3.14159265358979323846264338327950288419716939937510582097",
                      "1e300", "1e-300", "123456789.123456789e-45",
                      "2.2250738585072011e-308", "2.2250738585072014e-308",
                      "4.9406564584124654e-324", "1.7976931348623157e308",
                      "Inf", "-inf", "NaN", "0.0", "-0.0")
                check(s)
            end
            # random decimal strings
            for _ in 1:4_000
                mant = String(rand(rng, '0':'9', rand(rng, 1:60)))
                frac = rand(rng, Bool) ? "." * String(rand(rng, '0':'9', rand(rng, 1:60))) : ""
                ex = rand(rng, Bool) ? "e" * string(rand(rng, -320:320)) : ""
                check((rand(rng, Bool) ? "-" : "") * mant * frac * ex)
            end
            # round-trips of random values at this precision
            for _ in 1:1_000
                x = ldexp(BigFloat(rand(rng, UInt64)) + rand(rng), rand(rng, -200:200))
                rand(rng, Bool) && (x = -x)
                check(string(x))
            end
        end
    end
    # Float64 consistency: at 53 bits the two independent pipelines must agree
    # bit-for-bit on normal-range values (BigFloat has no subnormals)
    setprecision(BigFloat, 53) do
        for _ in 1:20_000
            bits = rand(rng, UInt64)
            x = reinterpret(Float64, bits)
            (isnan(x) || isinf(x) || issubnormal(x) || x == 0) && continue
            s = string(x)
            vb, _ = V.parsebigfloat(b(s), 1, ncodeunits(s))
            vf, _ = V.parsefloat64(b(s), 1, ncodeunits(s))
            @test Float64(vb) === vf
        end
    end
    # prove-out range bound is explicit, not silent
    for s in ("1e65535", "1e-65537")
        v, rc = V.parsebigfloat(b(s), 1, ncodeunits(s); prec=65)
        o = setprecision(BigFloat, 65) do
            parse(BigFloat, s)
        end
        @test rc == V.RC_OK && v == o
    end
    @test V.parsebigfloat(b("1e65536"), 1, 7)[2] == V.RC_OVERFLOW
    @test V.parsebigfloat(b("1e-65538"), 1, 8)[2] == V.RC_OVERFLOW
    @test V.parsebigfloat(b("1e100000"), 1, 8)[2] == V.RC_OVERFLOW
    @test V.parsebigfloat(b("1e-100000"), 1, 9)[2] == V.RC_OVERFLOW
    # The gate is based on the full M * 10^q representation. DecParts.exp10
    # only describes its first 19 digits and used to reject this in-range value.
    longfraction = "0." * "1"^65_556
    setprecision(BigFloat, 24) do
        v, rc = V.parsebigfloat(b(longfraction), 1, ncodeunits(longfraction))
        @test rc == V.RC_OK && v == parse(BigFloat, longfraction)
    end
    # A fixed exponent clamp could be cancelled by a long mantissa at the gate,
    # after which _bigmantissa reconstructed an enormous q for pow_ui.
    clamptrap = "1"^50_000 * "e-100000000000000000000"
    @test V.parsebigfloat(b(clamptrap), 1, ncodeunits(clamptrap); prec=24)[2] == V.RC_OVERFLOW
    # Zero bypasses scaling and preserves its sign even with a huge exponent.
    negzero = "-0e100000000000000000000"
    nz, rc = V.parsebigfloat(b(negzero), 1, ncodeunits(negzero); prec=65)
    @test rc == V.RC_OK && iszero(nz) && signbit(nz)
    for s in ("", ".", "1..2", "1e", "x")
        @test V.parsebigfloat(b(s), 1, ncodeunits(s))[2] == V.RC_INVALID
    end
end

@testset "parseuuid: oracle differential" begin
    rng = MersenneTwister(31)
    for _ in 1:20_000
        u = rand(rng, UInt128)
        s = string(Base.UUID(u))
        s = rand(rng, Bool) ? uppercase(s) : s
        v, rc = V.parseuuid(b(s), 1, 36)
        @test rc == V.RC_OK
        @test Base.UUID(v) == Base.tryparse(Base.UUID, s)
    end
    for s in ("123e4567-e89b-12d3-a456-42661417400",    # 35 chars
              "123e4567-e89b-12d3-a456-4266141740000",  # 37 chars
              "123e4567xe89b-12d3-a456-426614174000",   # bad dash
              "123e4567-e89b-12d3-a456-42661417400g",   # bad hex
              "{123e4567-e89b-12d3-a456-426614174000}", # braces
              "")
        @test V.parseuuid(b(s), 1, ncodeunits(s))[2] == V.RC_INVALID
        @test Base.tryparse(Base.UUID, s) === nothing
    end
end

end # testset
