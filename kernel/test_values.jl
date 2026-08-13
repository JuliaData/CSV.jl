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
            # pinned delta: Base.parse throws on overflow-to-Inf (strtod ERANGE);
            # this layer returns ±Inf with OK (the CSV/strtod-value semantic)
            @test isinf(v)
        else
            @test (isnan(v) && isnan(o)) || reinterpret(UInt64, v) == reinterpret(UInt64, o)
        end
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
        s = "0." * String(rand(rng, '0':'9', rand(rng, 100:790))) * "e" * string(rand(rng, -300:300))
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
    sents = [b("NA"), b("NULL")]
    @test V.matchsentinel(b("NA"), 1, 2, sents)
    @test V.matchsentinel(b("NULL"), 1, 4, sents)
    @test !V.matchsentinel(b("NAN"), 1, 3, sents)
    @test !V.matchsentinel(b("na"), 1, 2, sents)
end

@testset "civil: daysfromcivil vs Dates oracle" begin
    for y in (1, 100, 1583, 1600, 1900, 1970, 2000, 2020, 2024, 2100, 2400, 9999)
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

end # testset
