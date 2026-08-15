using Random

function legacydetectdelim(s::String)
    buf = Vector{UInt8}(codeunits(s))
    len = length(buf)
    pos = len >= 3 && buf[1:3] == UInt8[0xef, 0xbb, 0xbf] ? 4 : 1
    hp, dp = LegacyCSV.detectheaderdatapos(buf, pos, len, UInt8('"'), UInt8('"'),
                                           UInt8('"'), nothing, true, 1, 2)
    d, _ = LegacyCSV.detectdelimandguessrows(buf, hp, dp, len, UInt8('"'),
                                             UInt8('"'), UInt8('"'), nothing,
                                             true, UInt8('\n'))
    return Char(d)
end

@testset "delimiter detector differential fuzz" begin
    rng = MersenneTwister(0xd311_0f10)
    candidates = (',', '\t', ' ', '|', ';', ':')
    for trial in 1:256
        delim = rand(rng, candidates)
        ncols = rand(rng, 2:5)
        nrows = rand(rng, 1:5)
        lines = [join(("h$(j)" for j in 1:ncols), delim)]
        for r in 1:nrows
            cells = String[]
            for c in 1:ncols
                if rand(rng, Bool)
                    # All candidate bytes, spaces, and a doubled quote occur
                    # inside a valid quoted field. None may affect detection.
                    noise = join(rand(rng, candidates, rand(rng, 1:6))) * "\"q"
                    push!(cells, "\"" * replace(noise, "\"" => "\"\"") * "\"")
                else
                    push!(cells, "v$(trial)_$(r)_$(c)")
                end
            end
            push!(lines, join(cells, delim))
        end
        eol = rand(rng, Bool) ? "\n" : "\r\n"
        sample = join(lines, eol) * (rand(rng, Bool) ? eol : "")
        new = CSV.CSVApi.sniff(IOBuffer(sample)).delim
        old = legacydetectdelim(sample)
        @test new == old == delim
    end

    # Candidate order and header evidence are stable tie breakers.
    @test legacydetectdelim("a,b;c\n1,2;3\n") ==
          CSV.CSVApi.sniff(IOBuffer("a,b;c\n1,2;3\n")).delim == ','
    @test legacydetectdelim("A;B;C\n1,1,10\n2,0,16") ==
          CSV.CSVApi.sniff(IOBuffer("A;B;C\n1,1,10\n2,0,16")).delim == ';'
    @test CSV.CSVApi.sniff(IOBuffer("Created Date\n")).delim == ','
    @test CSV.CSVApi.sniff(IOBuffer("a;b;c\n")).delim == ';'
    @test CSV.CSVApi.sniff(IOBuffer("")).delim == ','
    bom = String(UInt8[0xef, 0xbb, 0xbf]) * "a|b\n1|2\n"
    @test legacydetectdelim(bom) == CSV.CSVApi.sniff(IOBuffer(bom)).delim == '|'
end
