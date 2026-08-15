# GENERATED from the 0.10 test suite (legacy/test) by the audit's extractor;
# each entry replays one legacy CSV.File call through both implementations.
# Hand-triaged entries carry a comment; see AUDIT.md for the ledger.

@testset "legacy corpus: CSV.File replay" begin
    # basics.jl:23 — "" is a (nonexistent) PATH, not content; agree() would wrap
    # it in an IOBuffer, so pin the error pair directly: both reject the path.
    @test_throws ArgumentError NEW.File("")
    @test_throws ArgumentError OLD.File("")
    _recordoutcome!("basics:23", :both_error)
    @case "basics:26" agree(corpusfile("test_no_header.csv"); skipto=1, header=2, label="basics:26")
    @case "basics:29" agree(corpusfile("test_float_in_int_column.csv"); types=[Int, Int, Int], strict=true, label="basics:29")
    @case "basics:32" agree(corpusfile("int64_overflow.csv"); types=[Int8], strict=true, label="basics:32")
    @case "basics:35" agree(corpusfile("test_newline_line_endings.csv"), types=Dict(1=>Integer); label="basics:35")
    @case "basics:38" agree(IOBuffer(" \"a, b\", \"c\" "), skipto=1; label="basics:38")
    @case "basics:44" agree(IOBuffer(" \"2018-01-01\", \"1\" ,1,2,3"), skipto=1; label="basics:44")
    @case "basics:54" agree(corpusfile("test_types.csv"), types=Dict(:string=>Union{Missing,DateTime}); label="basics:54")
    @case "basics:58" agree(corpusfile("test_types.csv"); label="basics:58")
    # PINNED 1.0 DELTA: NUL is an accepted delimiter byte (0.10 rejected it)
    @case "basics:60" agree(IOBuffer("a\0b\n1\02\n"); delim='\0', label="basics:60", expect_delta=(outcome=:old_errors, reason="NUL is an accepted delimiter byte (0.10 rejected it)"))
    # PINNED 1.0 DELTA: NUL is an accepted delimiter byte (0.10 rejected it)
    @case "basics:61" agree(IOBuffer("a\0b\n1\02\n"); delim="\0", label="basics:61", expect_delta=(outcome=:old_errors, reason="NUL is an accepted delimiter byte (0.10 rejected it)"))
    @case "basics:63" agree(IOBuffer("a,b\n1,2\n"); header=3, label="basics:63")
    @case "basics:67" agree(IOBuffer("a,b\n1,2\n"); skipto=3, label="basics:67")
    @case "basics:71" agree(IOBuffer("a,b\n1,2\n"); skipto=3, label="basics:71")
    @case "basics:75" agree(IOBuffer("a,b\n1,2\n"); limit=0, label="basics:75")
    @case "basics:80" agree(IOBuffer("a,b\n1,1\n1,2\n1,3\n"); label="basics:80")
    @case "basics:96" agree(corpusfile("time.csv"); dateformat="H:M:S", label="basics:96")
    @case "basics:101" agree(corpusfile("GSM2230757_human1_umifm_counts.csv"); ntasks=1, label="basics:101")
    @case "basics:106" agree(IOBuffer("fullVisitorId,PredictedLogRevenue\n18966949534117,0\n39738481224681,0\n"), limit=3; label="basics:106")
    @case "basics:112" agree(IOBuffer("col1,col2\n1.0,hi"), limit=3; label="basics:112")
    @case "basics:116" agree(IOBuffer("x\n\",\"\n\",\""); label="basics:116")
    @case "basics:120" agree(IOBuffer("x\n1\n2\n"); label="basics:120")
    @case "basics:123" agree(IOBuffer("x,y\n1,\n2,\n"); label="basics:123")
    @case "basics:126" agree(IOBuffer("x y\n1 \n2 \n"); label="basics:126")
    @case "basics:129" agree(IOBuffer("x\ty\n1\t\n2\t\n"); label="basics:129")
    @case "basics:132" agree(IOBuffer("x|y\n1|\n2|\n"); label="basics:132")
    @case "basics:135" agree(IOBuffer("a;b;c\n1,1;2,2;3,3\n4,4;5,5;6,6\n"); decimal=',', label="basics:135")
    @case "basics:143" agree(IOBuffer("x\n1\n3.14"); label="basics:143")
    @case "basics:149" agree(IOBuffer("x\n1\n\n"), ignoreemptyrows=false; label="basics:149")
    @case "basics:155" agree(IOBuffer("x\n\n1\n"), ignoreemptyrows=false; label="basics:155")
    @case "basics:161" agree(IOBuffer("x\n\n1\n3.14\n"), ignoreemptyrows=false; label="basics:161")
    @case "basics:168" agree(IOBuffer("x\n1\n\n3.14\n"), ignoreemptyrows=false; label="basics:168")
    @case "basics:175" agree(IOBuffer("x\n1\n3.14\n\n"), ignoreemptyrows=false; label="basics:175")
    @case "basics:182" agree(IOBuffer("x\n1\nabc"); label="basics:182")
    @case "basics:188" agree(IOBuffer("x\n3.14\nabc"); label="basics:188")
    @case "basics:194" agree(IOBuffer("x\n1\n3.14\nabc"); label="basics:194")
    @case "basics:201" agree(IOBuffer("x\n\n1\n3.14\nabc"), ignoreemptyrows=false; label="basics:201")
    @case "basics:209" agree(IOBuffer("x\n1"), downcast=true, ignoreemptyrows=true; label="basics:209")
    @case "basics:212" agree(IOBuffer("x\n1\n$(typemax(Int16))"), downcast=true, ignoreemptyrows=true; label="basics:212")
    @case "basics:215" agree(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))"), downcast=true, ignoreemptyrows=true; label="basics:215")
    @case "basics:218" agree(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))"), downcast=true, ignoreemptyrows=true; label="basics:218")
    @case "basics:221" agree(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))"), downcast=true, ignoreemptyrows=true; label="basics:221")
    @case "basics:224" agree(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14"), downcast=true, ignoreemptyrows=true; label="basics:224")
    @case "basics:227" agree(IOBuffer("x\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14\nabc"), downcast=true, ignoreemptyrows=true; label="basics:227")
    @case "basics:230" agree(IOBuffer("x\n\n1"), downcast=true, ignoreemptyrows=false; label="basics:230")
    @case "basics:233" agree(IOBuffer("x\n\n1\n$(typemax(Int16))"), downcast=true, ignoreemptyrows=false; label="basics:233")
    @case "basics:236" agree(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))"), downcast=true, ignoreemptyrows=false; label="basics:236")
    @case "basics:239" agree(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))"), downcast=true, ignoreemptyrows=false; label="basics:239")
    @case "basics:242" agree(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))"), downcast=true, ignoreemptyrows=false; label="basics:242")
    @case "basics:245" agree(IOBuffer("x\n\n1\n$(typemax(Int16))\n$(typemax(Int32))\n$(typemax(Int64))\n$(typemax(Int128))\n3.14"), downcast=true, ignoreemptyrows=false; label="basics:245")
    @case "basics:249" agree(IOBuffer("x\n\na\n"), pool=true, ignoreemptyrows=false; label="basics:249")
    @case "basics:255" agree(IOBuffer("x\na\n\n"), pool=true, ignoreemptyrows=false; label="basics:255")
    @case "basics:261" agree(IOBuffer("x\na\nb\na\nb\na\nb\na\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nn\nm\no\np\nq\nr\n"), pool=0.5; label="basics:261")
    @case "basics:265" agree(IOBuffer("\"column name with \"\" escape character inside\"\n1\n"); label="basics:265")
    @case "basics:268" agree(IOBuffer("\"column name with \"\" escape character inside\",1\n,2"), transpose=true; label="basics:268")
    @case "basics:272" agree(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true; label="basics:272")
    @case "basics:276" agree(IOBuffer("x\na\nb\n\"quoted field with \"\" escape character inside\"\n"), pool=true; label="basics:276")
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:281" agree(IOBuffer("x\n\"quoted field that never ends"); label="basics:281", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:282" agree(IOBuffer("x\nhey\n\"quoted field that never ends"); label="basics:282", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:283" agree(IOBuffer("x\n\n\"quoted field that never ends"); label="basics:283", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:284" agree(IOBuffer("x\n1\n\"quoted field that never ends"); label="basics:284", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:285" agree(IOBuffer("x\n1.0\n\"quoted field that never ends"); label="basics:285", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    # PINNED 1.0 DELTA: unclosed quote is a reported problem, not a fatal error (warnings are data)
    @case "basics:286" agree(IOBuffer("x\na\n\"quoted field that never ends"), pool=true; label="basics:286", expect_delta=(outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"))
    @case "basics:289" agree(IOBuffer("x\nabc\n"), types=Int; label="basics:289")
    @case "basics:293" agree(IOBuffer("x\nabc\n"), types=Int, strict=true; label="basics:293")
    @case "basics:296" agree(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=2; label="basics:296")
    @case "basics:302" agree(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=1, skipto=3; label="basics:302")
    @case "basics:308" agree(IOBuffer("x,y,1\nx2,y2,2\n"), transpose=true, header=false, skipto=3; label="basics:308")
    @case "basics:314" agree(IOBuffer(""), transpose=true, header=false; label="basics:314")
    @case "basics:317" agree(IOBuffer(""), transpose=true, header=Symbol[]; label="basics:317")
    # basics.jl:327 — #1172 transpose promotion, bare and quoted variants
    @case "basics:327-1" agree(IOBuffer("a,,1,x,3\nb,,4,5,6"); transpose=true, label="basics:327-1")
    @case "basics:327-2" agree(IOBuffer("a,,1,\"x\",3\nb,,4,5,6"); transpose=true, label="basics:327-2")
    @case "basics:342" agree(IOBuffer("skip,a,,1,x,3\nskip,b,,4,5,6"); transpose=true, header=2, label="basics:342")
    @case "basics:346" agree(IOBuffer(",1,x,3\n,4,5,6"); transpose=true, header=[:a, :b], label="basics:346")
    @case "basics:350" agree(IOBuffer("a,,1,\"x\",3\nb,,4,5,6"); transpose=true, limit=3, label="basics:350")
    @case "basics:354" agree(IOBuffer("a,,1,x,3\nb,4"); transpose=true, label="basics:354")
    @case "basics:360" agree(IOBuffer("x\nabc\n"), header=Symbol[]; label="basics:360")
    @case "basics:364" agree(IOBuffer("x\ntrue\n\n"), ignoreemptyrows=false; label="basics:364")
    @case "basics:370" agree(IOBuffer("x\n2019-01-01\n\n"), ignoreemptyrows=false; label="basics:370")
    @case "basics:376" agree(IOBuffer("x\n2019-01-01\n\n"), types=Dict("x"=>Date), ignoreemptyrows=false; label="basics:376")
    @case "basics:382" agree(IOBuffer("int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"); pool=0.3, label="basics:382")
    @case "basics:405" agree(IOBuffer("x\n1\n2\n3\n#4"); ignorerepeated=true, label="basics:405")
    @case "basics:408" agree(IOBuffer("x\n"), pool=true; label="basics:408")
    @case "basics:411" agree(IOBuffer("x\n1\n2\n3\n#4"), comment="#"; label="basics:411")
    @case "basics:416" agree(IOBuffer("x\n1\n2\n3\n#4"), types=[CSV_Foo]; label="basics:416")
    @case "basics:417" agree(IOBuffer("x\n1\n2\n3\n#4"), types=Dict(:x=>CSV_Foo); label="basics:417")
    @case "basics:420" agree(IOBuffer("a,b,c\n1,2,3\n\n"); label="basics:420")
    @case "basics:423" agree(IOBuffer("zip\n11111-1111\n"), dateformat = "y-m-dTH:M:S.s"; label="basics:423")
    # basics.jl:428 — a Cmd source (`cat` spelled portably via julia itself)
    let catcmd = `$(Base.julia_cmd()) --startup-file=no --eval "write(stdout, open(ARGS[1]))"`
        @case "basics:428" agree(`$(catcmd) $(corpusfile("test_basic.csv"))`; label="basics:428")
    end
    @case "basics:429" agree(corpusfile("test_basic.csv"); label="basics:429")
    @case "basics:432" agree(corpusfile("randoms.csv.gz"); buffer_in_memory=true, label="basics:432")
    @case "basics:435" agree(IOBuffer("thistime\n10:00:00.0\n12:00:00.0"); label="basics:435")
    @case "basics:440" agree(IOBuffer(",column2\nNA,2\n2,3"), missingstring=["NA"]; label="basics:440")
    @case "basics:444" agree(IOBuffer("x\n01:02:03\n\n04:05:06\n"), delim=',', ignoreemptyrows=false; label="basics:444")
    @case "basics:448" agree(IOBuffer("x\r\n1\r\n2\r\n3\r\n4\r\n5\r\n"), footerskip=3; label="basics:448")
    # basics.jl:455 — comment rows do not count towards footerskip, per terminator
    for (i, newline) in enumerate(("\n", "\r\n", "\r"))
        csv = join(("a,b", "1,2", "3,4", "# trailing comment 1", "# trailing comment 2"), newline) * newline
        @case "basics:455-$i" agree(IOBuffer(csv); comment="#", footerskip=1, label="basics:455-$i")
    end
    @case "basics:462" agree(IOBuffer("h1234567890123456\t"^2262 * "lasthdr\r\n" * "dummy dummy dummy\r\n" * ("1.23\t"^2262 * "2.46\r\n")^10), skipto=3, ntasks=1; label="basics:462")
    @case "basics:467" agree(IOBuffer("date\n2020-05-05\n2020-05-32"); label="basics:467")
    @case "basics:470" agree(IOBuffer("time,date,datetime\n10:00:00.0,04/16/2020,2020-04-16 23:14:00\n"), dateformat=Dict(2=>"mm/dd/yyyy", 3=>"yyyy-mm-dd HH:MM:SS"); label="basics:470")
    @case "basics:477" agree(
    IOBuffer("int8,uint32,bigint,bigfloat,dec64,csvstring\n1,2,170141183460469231731687303715884105727,3.14,1.02,hey there sailor\n2,,,,,\n");
    types=[Int8, UInt32, BigInt, BigFloat, Dec64, CSVString]
, label="basics:477")
    @case "basics:494" agree(corpusfile("randoms.csv.gz"); types=[Int, CSVString, String, Float64, Dec64, Date, DateTime], label="basics:494")
    # PINNED 1.0 DELTA: stringtype=PosLenString retired: CompactString (default) or String
    @case "basics:499" agree(corpusfile("promotions.csv"); stringtype=PosLenString, label="basics:499", expect_delta=(outcome=:new_errors, reason="stringtype=PosLenString retired: CompactString (default) or String"))
    @case "basics:502" agree(corpusfile("promotions.csv"); limit=7500, ntasks=2, label="basics:502")
    @case "basics:505" agree(IOBuffer("1,2\r\n3,4\r\n\r\n5,6\r\n"); header=["col1", "col2"], ignoreemptyrows=true, label="basics:505")
    @case "basics:508" agree(corpusfile("escape_row_starts.csv"); ntasks=2, label="basics:508")
    # basics.jl:522 — #1139: a chunk boundary landing inside a quoted multiline body
    mktempdir() do tmp
        path = joinpath(tmp, "issue1139.csv")
        open(path, "w") do io
            Base.write(io, "id,text\n")
            for i in 1:4000
                Base.write(io, string(i), ",\"123\nabc\"\n")
            end
        end
        # PINNED 1.0 DELTA: the kernel's deterministic chunking parses id as
        # Int64; 0.10's chunk-boundary speculation landing inside the quoted
        # multiline bodies degraded id to String (its own test asserted values
        # via parse(Int, string(x)))
        @case "basics:522" agree(path; ntasks=2, label="basics:522", expect_delta=(outcome=:differ, reason="multithreaded quoted-multiline parsing keeps exact column types; 0.10 degraded the id column to String at chunk boundaries"))
    end
    # PINNED 1.0 DELTA: stringtype=PosLenString retired: CompactString (default) or String
    @case "basics:529" agree(IOBuffer("col1\nhey\nthere\nsailor"); stringtype=PosLenString, label="basics:529", expect_delta=(outcome=:new_errors, reason="stringtype=PosLenString retired: CompactString (default) or String"))
    # PINNED 1.0 DELTA: stringtype=PosLenString retired: CompactString (default) or String
    @case "basics:548" agree(corpusfile("big_types.csv"); stringtype=PosLenString, pool=false, label="basics:548", expect_delta=(outcome=:new_errors, reason="stringtype=PosLenString retired: CompactString (default) or String"))
    @case "basics:561" agree(IOBuffer("col1\n1"); label="basics:561")
    # basics.jl:584 — #668: parse from the IO's CURRENT position (a consumed line
    # stays consumed); thunk form because seekstart would resurrect it
    @case "basics:584" agree(() -> (b = IOBuffer("garbage\na,b\n1,2\n"); readline(b); b); header=["A", "B"], label="basics:584")
    # basics.jl:593 — #680: typemap accepts Dict and IdDict
    @case "basics:593-1" agree(IOBuffer("a\n1\n2\n3"); typemap=Dict(Int => Int32), label="basics:593-1")
    @case "basics:593-2" agree(IOBuffer("a\n1\n2\n3"); typemap=IdDict(Int => Int32), label="basics:593-2")
    @case "basics:596" agree(IOBuffer("a\n1\n2\n3"); typemap=IdDict(Int=>String), label="basics:596")
    @case "basics:600" agree(IOBuffer("""x,y
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b
                                       a,b

                                       """), ignoreemptyrows=false; label="basics:600")
    @case "basics:616" agree(IOBuffer("a,b,c\n1,2,3\n4,5,6\n"); select=["a"], types=Dict(2=>Int8), label="basics:616")
    @case "basics:619" agree(transcode(GzipDecompressor, Mmap.mmap(corpusfile("randoms.csv.gz"))); types=Dict(:id=>Int32), select=["first"], label="basics:619")
    @case "basics:624" agree(IOBuffer("col1,col2,col3\n1.0,2.0,3.0\n1.0,2.0,3.0\n1.0,2.0,3.0\n1.0,2.0,3.0\n"); ntasks=2, label="basics:624")
    @case "basics:629" agree(IOBuffer("col1,col2,col3,col4,col5\na,b,c,d,e\n" * "a,b,c,d\n"^101); label="basics:629")
    @case "basics:633" agree(IOBuffer("col1\n\n \n  \n1\n2\n3"), missingstring=["", " ", "  "], ignoreemptyrows=false; label="basics:633")
    @case "basics:637" agree(IOBuffer("a\n1"); label="basics:637")
    @case "basics:642" agree(IOBuffer("""
# 1'2
name
junk
1
"""), comment="#", header=2, skipto=4; label="basics:642")
    @case "basics:651" agree(IOBuffer("""
# 1'2"
name
junk
1
"""), comment="#", header=2, skipto=4; label="basics:651")
    @case "basics:661" agree(IOBuffer("Created Date\nToday\n"); label="basics:661")
    # basics.jl:667-681 — #796: footerskip over a trailing blank row, per terminator
    for (lbl1, lbl2, newline) in (("basics:667", "basics:669", "\n"),
                                  ("basics:673", "basics:675", "\r\n"),
                                  ("basics:679", "basics:681", "\r"))
        csv = "1, a$(newline)2, b$(newline)3, c$(newline)4, d$(newline)$(newline)"
        @case lbl1 agree(IOBuffer(csv); skipto=1, footerskip=1, label=lbl1)
        @case lbl2 agree(IOBuffer(csv); skipto=1, footerskip=2, label=lbl2)
    end
    @case "basics:684" agree(IOBuffer(join(rand(Xoshiro(684), ["a", "b", "c"], 500), "\n")); header=false, ntasks=2, label="basics:684")
    @case "basics:689" agree(IOBuffer("a, 0.1, 0.2, 0.3\nb, 0.4"); transpose=true, label="basics:689")
    @case "basics:695" agree(IOBuffer("x\n\0\n"); label="basics:695")
    @case "basics:700" agree(IOBuffer("x\n\"abc\"\n"); quoted=false, label="basics:700")
    # PINNED 1.0 DELTA: empty unquoted cell is ALWAYS missing (missingstring only ADDS spellings); 0.10's missingstring=nothing made empties present ""
    @case "basics:704" agree(IOBuffer("a,b,c\n1,2,3\n,null,4\n"), missingstring=nothing; label="basics:704", expect_delta=(outcome=:differ, reason="empty unquoted cell is ALWAYS missing (missingstring only ADDS spellings); 0.10's missingstring=nothing made empties present \"\""))
    @case "basics:714" agree(IOBuffer("a,b,c\n1,2,3"); types=Dict(4 => Float64), label="basics:714")
    @case "basics:715" agree(IOBuffer("a,b,c\n1,2,3"); types=Dict(:d => Float64), label="basics:715")
    @case "basics:716" agree(IOBuffer("a,b,c\n1,2,3"); types=Dict("d" => Float64), label="basics:716")
    @case "basics:718" agree(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict(4 => "dd/mm/yyyy"), label="basics:718")
    @case "basics:719" agree(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict(:d => "dd/mm/yyyy"), label="basics:719")
    @case "basics:720" agree(IOBuffer("a,b,c\n1,2,3"); dateformat=Dict("d" => "dd/mm/yyyy"), label="basics:720")
    @case "basics:722" agree(IOBuffer("a,b,c\n1,2,3"); pool=Dict(4 => true), label="basics:722")
    @case "basics:723" agree(IOBuffer("a,b,c\n1,2,3"); pool=Dict(:d => true), label="basics:723")
    @case "basics:724" agree(IOBuffer("a,b,c\n1,2,3"); pool=Dict("d" => true), label="basics:724")
    @case "basics:727" agree(IOBuffer("a,b,c\n1,2,3"); types=Dict(4 => Float64), dateformat=Dict(:e => "dd/mm/yyyy"), pool=Dict("f" => true), validate=false, label="basics:727")
    @case "basics:737" agree(IOBuffer("a,b,c\n1,2,3\n3.14,5,6\n"); typemap=IdDict(Float64 => String), label="basics:737")
    # basics.jl:741-745 — SubString (via strip) and SubArray{UInt8} sources
    @case "basics:741" agree(IOBuffer(strip("\"column_name\",\"data_type\",\"is_nullable\"\nfoobar,string,YES\nbazbat,timestamptz,YES  ")); label="basics:741")
    let data = Vector{UInt8}("\"column_name\",\"data_type\",\"is_nullable\"\nfoobar,string,YES\nbazbat,timestamptz,YES")
        @case "basics:745" agree(@view(data[:]); label="basics:745")
    end
    # basics.jl:759-778 — preprocessing honors SubArray and IO source bounds
    let data = Vector{UInt8}("a,b\n1,2\n3,4\n"),
        compressed = transcode(GzipCompressor, data),
        prefix = Vector{UInt8}("ignored prefix"),
        suffix = Vector{UInt8}("ignored suffix")
        parent = vcat(prefix, compressed, suffix)
        firstbyte = length(prefix) + 1
        lastbyte = firstbyte + length(compressed) - 1
        @case "basics:759-1" agree(@view(parent[firstbyte:lastbyte]); buffer_in_memory=false, label="basics:759-1")
        @case "basics:759-2" agree(@view(parent[firstbyte:lastbyte]); buffer_in_memory=true, label="basics:759-2")
        # an IO parses from its current position (here: past the prefix)
        @case "basics:765" agree(() -> (io = IOBuffer(vcat(prefix, compressed)); seek(io, length(prefix)); io); label="basics:765")
        parent2 = vcat(compressed, data)
        @case "basics:770" agree(@view(parent2[(length(compressed) + 1):end]); label="basics:770")
        parent3 = vcat(prefix, data, suffix)
        @case "basics:776" agree(@view(parent3[firstbyte:(length(prefix) + length(data))]); footerskip=1, label="basics:776")
    end
    @case "basics:778" agree(UInt8[0xef, 0xbb, 0xbf]; footerskip=1, label="basics:778")
    @case "basics:781" agree(IOBuffer("a,b,c\n1.2,3.4,5.6\n"); types=Float32, label="basics:781")
    @case "basics:785" agree(IOBuffer("a,b,c\n1.2,3.4,5.6\n"); types=BigFloat, label="basics:785")
    @case "basics:790" agree(codeunits("a,b,c\n1.2,3.4,5.6\n"); label="basics:790")
    # PINNED 1.0 DELTA: function-typed types=/pool= retired: Dict / vector / Type forms (Tables.Scan is the expression channel)
    @case "basics:795" agree(codeunits("a,b,c,d\n1,2,3.14,hey\n4,2,6.5,hey\n");
                types=(i, nm) -> i == 1 ? Int8 : i == 2 ? BigInt : i == 3 ? Float64 : String,
                pool=(i, nm) -> i == 2 ? true : nothing, label="basics:795", expect_delta=(outcome=:new_errors, reason="function-typed types=/pool= retired: Dict / vector / Type forms (Tables.Scan is the expression channel)"))
    @case "basics:821" agree(corpusfile("multithreadedpromote.csv"); label="basics:821")
    # PINNED 1.0 DELTA: `type=` was already deprecated in 0.10; removed
    @case "basics:831" agree(IOBuffer("name, age\nJack, 12\nTom, 10\n"); select=[2], type=Int32, label="basics:831", expect_delta=(outcome=:new_errors, reason="0.10-deprecated type= removed: pass a single type to types="))
    # basics.jl:838-843 — #939: 60k columns × 271 rows, space-delimited; the wide
    # path through user types + typemap/downcast (first column pinned String)
    let rng = Xoshiro(838),
        row = join((i == 1 ? string(i + 10000000000) :
                    i == 60_000 ? "0\n" : rand(rng, ("-1", "0", "1")) for i = 1:60_000), " "),
        data = repeat(row, 271)
        @case "basics:838" agree(IOBuffer(data); header=false, types=Dict(1 => String), typemap=IdDict(Int => Int8), label="basics:838")
        @case "basics:840" agree(IOBuffer(data); header=false, types=Dict(1 => String), downcast=true, label="basics:840")
        @case "basics:843" agree(IOBuffer(data); header=false, types=Dict(1 => String), typemap=Dict(Int => Int8), ntasks=16, label="basics:843")
    end
    @case "basics:847" agree(IOBuffer("a,b\n1,2\n3,"); label="basics:847")
    # basics.jl:853 — #1190: short final row, with and without trailing newline
    @case "basics:853-1" agree(IOBuffer("a,b,c\n1,2,3\n4,5"); label="basics:853-1")
    @case "basics:853-2" agree(IOBuffer("a,b,c\n1,2,3\n4,5\n"); label="basics:853-2")
    @case "basics:858" agree(IOBuffer("a,a,a\n"); label="basics:858")
    @case "basics:861" agree(IOBuffer("a,a_1,a\n"); label="basics:861")
    @case "basics:864" agree(IOBuffer("a,a,a_1\n"); label="basics:864")
    # basics.jl:873-875 — #951: stripwhitespace on a pipe table
    let data = "| Name       |  Zip |\n| Joe        |  123 |\n| Mary Anne  | 1234 |\n"
        @case "basics:873" agree(IOBuffer(data); delim='|', normalizenames=true, stripwhitespace=false, label="basics:873")
        @case "basics:875" agree(IOBuffer(data); delim='|', stripwhitespace=true, label="basics:875")
    end
    # basics.jl:879 — #963: limit stops a million-row parse at 10k
    let rng = Xoshiro(879)
        @case "basics:879" agree(IOBuffer(join((rand(rng, ("a,$(rand(rng))", "b,$(rand(rng))")) for _ = 1:10^6), "\n")); header=false, limit=10000, label="basics:879")
    end
    @case "basics:883" agree(IOBuffer("a\nfalse\n"); label="basics:883")
    # basics.jl:889-899 — #1014: Regex keys in types (fresh buffers; the original
    # reused one IOBuffer, which agree()'s per-side replay cannot)
    let data = "a_col,b_col,c,d\n1,2,3.14,hey\n4,2,6.5,hey\n"
        @case "basics:889" agree(IOBuffer(data); types=Dict(r"_col$" => Int16), label="basics:889")
        # a Regex matching no column is an argument error on both sides
        @case "basics:892" agree(IOBuffer(data); types=Dict(r"_column$" => Int16), label="basics:892")
        @case "basics:894" agree(IOBuffer(data); types=Dict(r"_col$" => Int16, "c" => Float16), label="basics:894")
        # Regex has lower precedence than an exact name match
        @case "basics:899" agree(IOBuffer(data); types=Dict(r"_col$" => Int16, :a_col => Int8), label="basics:899")
    end
    @case "basics:903" agree(IOBuffer("time,date1,date2\n10:00:00.0,04/16/2020,04/17/2022\n"); dateformat=Dict(r"^date"=>"mm/dd/yyyy"), label="basics:903")
    # basics.jl:916-925 — #1021: user types for columns only found in LATER rows.
    # 0.10 widened the schema to those columns; the kernel does not widen (long
    # rows are reported problems) — the whole family pins that delta.
    let str = "1 2 3\n1 2\n1 2 3 4\n1\n1 2 3 4 5\n"
        @case "basics:916" agree(IOBuffer(str); delim=" ", header=false, types=String, label="basics:916", expect_delta=(outcome=:differ, reason="long rows do not widen the schema: 0.10 grew Column4/Column5 from ragged rows"))
        @case "basics:919" agree(IOBuffer(str); delim=" ", header=false, types=[Int8, Int16, Int32, Int64, Int128], label="basics:919", expect_delta=(outcome=:new_errors, reason="a types vector must match the header's column count; 0.10 let it widen the schema past ragged rows"))
        @case "basics:922" agree(IOBuffer(str); delim=" ", header=false, types=(i, nm) -> (i == 5 ? Int8 : String), label="basics:922", expect_delta=(outcome=:new_errors, reason="function-typed types retired: Dict / vector / Type forms (Tables.Scan is the expression channel)"))
        @case "basics:925" agree(IOBuffer(str); delim=" ", header=false, types=Dict(r".*" => Float16), label="basics:925", expect_delta=(outcome=:differ, reason="long rows do not widen the schema: 0.10 grew Column4/Column5 from ragged rows"))
    end
    # basics.jl:930 — #1080: vector of sources; a column name shadowing a File field
    @case "basics:930" agree(() -> map(IOBuffer, ["name\n2\n", "name\n11\n"]); label="basics:930")
    # iteration.jl:3 — the two files its row-accessor loops parsed
    @case "iteration:3-1" agree(corpusfile("test_not_enough_columns.csv"); label="iteration:3-1")
    @case "iteration:3-2" agree(corpusfile("test_correct_trailing_missings.csv"); label="iteration:3-2")
    @case "runtests:28" agree(IOBuffer("X\nb\nc\na\nc"), pool=true; label="runtests:28")
    @case "runtests:34" agree(IOBuffer("X\nb\nc\na\nc"), pool=0.75; label="runtests:34")
    @case "runtests:40" agree(IOBuffer("X\nb\nc\n\nc"), pool=true, ignoreemptyrows=false; label="runtests:40")
    @case "runtests:45" agree(IOBuffer("X\nc\nc\n\nc\nc\nc\nc\nc\nc"), pool=0.25, ignoreemptyrows=false; label="runtests:45")
    # runtests.jl:164-226 — every select/drop list form (function forms retired)
    let csv = "a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n"
        @case "runtests:164" agree(IOBuffer(csv); select=[1, 3, 5], label="runtests:164")
        @case "runtests:169" agree(IOBuffer(csv); select=[:a, :c, :e], label="runtests:169")
        @case "runtests:174" agree(IOBuffer(csv); select=["a", "c", "e"], label="runtests:174")
        @case "runtests:179" agree(IOBuffer(csv); select=[true, false, true, false, true], label="runtests:179")
        @case "runtests:184" agree(IOBuffer(csv); select=(i, nm) -> i in (1, 3, 5), label="runtests:184", expect_delta=(outcome=:new_errors, reason="function-typed select/drop retired: pass a list (Tables.Scan is the expression channel)"))
        @case "runtests:189" agree(IOBuffer(csv); select=Int[], label="runtests:189")
        @case "runtests:193" agree(IOBuffer(csv); select=[1, 2, 3, 4, 5], label="runtests:193")
        @case "runtests:197" agree(IOBuffer(csv); drop=[2, 4], label="runtests:197")
        @case "runtests:202" agree(IOBuffer(csv); drop=[:b, :d], label="runtests:202")
        @case "runtests:207" agree(IOBuffer(csv); drop=["b", "d"], label="runtests:207")
        @case "runtests:212" agree(IOBuffer(csv); drop=[false, true, false, true, false], label="runtests:212")
        @case "runtests:217" agree(IOBuffer(csv); drop=(i, nm) -> i in (2, 4), label="runtests:217", expect_delta=(outcome=:new_errors, reason="function-typed select/drop retired: pass a list (Tables.Scan is the expression channel)"))
        @case "runtests:222" agree(IOBuffer(csv); drop=Int[], label="runtests:222")
        @case "runtests:226" agree(IOBuffer(csv); drop=[1, 2, 3, 4, 5], label="runtests:226")
    end
    # runtests.jl:479-524 — vector-of-sources (thunk form: one-shot IOs, each
    # side gets fresh ones). PINNED 1.0 DELTA on every case whose columns
    # promote or missing-fill across sources: our schema reports the TRUE
    # concatenated types; 0.10 returned the first source's pre-promotion
    # types, disagreeing with its own columns (schema said Int64 while the
    # column held 7.14). Values always agree.
    @case "runtests:479" agree(() -> map(IOBuffer, ["a,b,c\n1,2,3\n4,5,6\n", "a,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]); label="runtests:479")
    @case "runtests:490" agree(() -> map(IOBuffer, ["a,b\nbill,x\njane,y\n", "a,b\ntomm,z\ntimm,\n", "a,b\njoee,z\njerr,g\n"]); label="runtests:490", expect_delta=(outcome=:differ, reason="multi-source schema reports true concatenated types; 0.10's schema kept the first source's pre-promotion types"))
    @case "runtests:500" agree(() -> map(IOBuffer, ["a,b,c\n1,2,3\n4,5,6\n", "a,b,c\n7.14,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]); label="runtests:500", expect_delta=(outcome=:differ, reason="multi-source schema reports true concatenated types; 0.10's schema kept the first source's pre-promotion types"))
    let data = ["a,b,c\n1,2,3\n4,5,6\n", "a2,b,c\n7,8,9\n10,11,12\n", "a,b,c\n13,14,15\n16,17,18"]
        @case "runtests:509" agree(() -> map(IOBuffer, data); label="runtests:509", expect_delta=(outcome=:differ, reason="multi-source schema reports true concatenated types; 0.10's schema kept the first source's pre-promotion types"))
        # ...and source labels for non-path sources are deterministic
        # "<source i>" strings; 0.10 embedded the IO object's hash, which two
        # independent calls cannot even reproduce
        @case "runtests:512" agree(() -> map(IOBuffer, data); source=:source, label="runtests:512", expect_delta=(outcome=:differ, reason="deterministic \"<source i>\" labels (0.10 embedded the IO object hash) + true concatenated schema types"))
        @case "runtests:516" agree(() -> map(IOBuffer, data); source="source", label="runtests:516", expect_delta=(outcome=:differ, reason="deterministic \"<source i>\" labels (0.10 embedded the IO object hash) + true concatenated schema types"))
        @case "runtests:520" agree(() -> map(IOBuffer, data); source="source" => [1, 2, 3], label="runtests:520", expect_delta=(outcome=:differ, reason="multi-source schema reports true concatenated types; 0.10's schema kept the first source's pre-promotion types"))
        @case "runtests:524" agree(() -> map(IOBuffer, data); source=:source => ["1", "2", "3"], label="runtests:524", expect_delta=(outcome=:differ, reason="multi-source schema reports true concatenated types; 0.10's schema kept the first source's pre-promotion types"))
    end
    # write.jl:312/338 — File reads back writer output: tab and control-char
    # delims (the byte strings ARE the written form of the original tables)
    @case "write:312" agree(IOBuffer("a\tdt\tdttm\n11\t2017-12-07\t2017-12-07T00:00:00\n22\t2017-12-14\t2017-12-14T00:00:00\n"); delim='\t', label="write:312")
    for byte in UInt8[1, 2, 3, 4]
        char = Char(byte)
        bytes = "A$(char)B\n1$(char)a\n2$(char)b\n3$(char)c\n"
        @case "write:338-$byte" agree(IOBuffer(bytes); delim=char, label="write:338-$byte")
    end
    # write.jl:346 — a FilePathsBase path as the source (extension on our side)
    mktempdir() do tmp
        Base.write(joinpath(tmp, "test.txt"), "A,B\n1,a\n2,b\n3,c\n")
        @case "write:346" agree(joinpath(FilePathsBase.Path(tmp), "test.txt"); label="write:346")
    end
    # testfiles.jl:9 was the corpus table's generic driver — corpus_table.jl +
    # corpuscase() replay the whole table; nothing separate to port.
end
