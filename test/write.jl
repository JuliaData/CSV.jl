using CSV, Dates, WeakRefStrings, Tables, CodecZlib
using FilePathsBase
using FilePathsBase: /

const default_table = (col1=[1,2,3], col2=[4,5,6], col3=[7,8,9])

const weakrefs = StringVector{WeakRefString{UInt8}}(["hey", "hey", "hey"])

const table_types = (
    col1=[true, false, true],
    col2=[4.1,5.2,4e10],
    col3=[NaN, Inf, -Inf],
    col4=[Date(2017, 1, 1), Date(2018, 1, 1), Date(2019, 1, 1)],
    col5=[DateTime(2017, 1, 1, 4, 5, 6, 7), DateTime(2018, 1, 1, 4, 5, 6, 7), DateTime(2019, 1, 1, 4, 5, 6, 7)],
    col6=["hey", "there", "sailor"],
    col7=[weakrefs[1], weakrefs[2], weakrefs[3]],
    col8=weakrefs,
)

struct StructType
    adate::Date
    astring::Union{String, Nothing}
    aumber::Union{Real, Nothing}
end

struct AF <: AbstractFloat
    f::Float64
end
Base.string(x::AF) = string(x.f)

@testset "CSV.write" begin

    testcases = [
        (
            default_table,
            NamedTuple(),
            "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
        ),
        (
            default_table,
            (delim='\t',),
            "col1\tcol2\tcol3\n1\t4\t7\n2\t5\t8\n3\t6\t9\n"
        ),
        (
            (col1=[1,2,3], col2=["hey", "the::re", "::sailor"], col3=[7,8,9]),
            (delim="::",),
            "col1::col2::col3\n1::hey::7\n2::\"the::re\"::8\n3::\"::sailor\"::9\n"
        ),
        (
            default_table,
            (header=[:Col1, :Col2, :Col3],),
            "Col1,Col2,Col3\n1,4,7\n2,5,8\n3,6,9\n"
        ),
        (
            default_table,
            (header=["Col1", "Col2", "Col3"],),
            "Col1,Col2,Col3\n1,4,7\n2,5,8\n3,6,9\n"
        ),
        (
            default_table,
            (bom=true,),
            "\xEF\xBB\xBFcol1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
        ),
        (
            table_types,
            NamedTuple(),
            "col1,col2,col3,col4,col5,col6,col7,col8\ntrue,4.1,NaN,2017-01-01,2017-01-01T04:05:06.007,hey,hey,hey\nfalse,5.2,Inf,2018-01-01,2018-01-01T04:05:06.007,there,hey,hey\ntrue,4.0e10,-Inf,2019-01-01,2019-01-01T04:05:06.007,sailor,hey,hey\n"
        ),
        (
            default_table,
            (writeheader=false,),
            "1,4,7\n2,5,8\n3,6,9\n"
        ),
        (
            default_table,
            (writeheader=false,newline=""),
            "1,4,72,5,83,6,9"
        ),
        (
            default_table,
            (writeheader=false,newline="",delim="::"),
            "1::4::72::5::83::6::9"
        ),
        (
            (col4=[Date(2017, 1, 1), Date(2018, 1, 1), Date(2019, 1, 1)],
                col5=[DateTime(2017, 1, 1, 4, 5, 6, 7), DateTime(2018, 1, 1, 4, 5, 6, 7), DateTime(2019, 1, 1, 4, 5, 6, 7)],
            ),
            (dateformat="mm/dd/yyyy",),
            "col4,col5\n01/01/2017,01/01/2017\n01/01/2018,01/01/2018\n01/01/2019,01/01/2019\n"
        ),
        (
            (col1=[1,missing,3], col2=[missing, missing, missing], col3=[7,8,9]),
            NamedTuple(),
            "col1,col2,col3\n1,,7\n,,8\n3,,9\n"
        ),
        (
            (col1=[1,missing,3], col2=[missing, missing, missing], col3=[7,8,9]),
            (missingstring="NA",),
            "col1,col2,col3\n1,NA,7\nNA,NA,8\n3,NA,9\n"
        ),
        (
            (col1=[1,nothing,3], col2=[nothing, missing, missing], col3=[7,8,9]),
            (transform=(col, val) -> something(val, missing), missingstring="NA"),
            "col1,col2,col3\n1,NA,7\nNA,NA,8\n3,NA,9\n"
        ),
        (
            (col1=["hey, there, sailor", "this, also, has, commas", "this\n has\n newlines\n", "no quoting", "just a random \" quote character", ],),
            (escapechar='\\',),
            "col1\n\"hey, there, sailor\"\n\"this, also, has, commas\"\n\"this\n has\n newlines\n\"\nno quoting\n\"just a random \\\" quote character\"\n"
        ),
        (
            (col1=["\"hey there sailor\""],),
            (escapechar='\\',),
            "col1\n\"\\\"hey there sailor\\\"\"\n"
        ),
        (
            (col1=["{\"key\": \"value\"}", "{\"key\": null}"],),
            (openquotechar='{', closequotechar='}', escapechar='\\'),
            "col1\n{\\{\"key\": \"value\"\\}}\n{\\{\"key\": null\\}}\n"
        ),
        (
            default_table,
            (newline='\r',),
            "col1,col2,col3\r1,4,7\r2,5,8\r3,6,9\r"
        ),
        (
            default_table,
            (newline="\r\n",),
            "col1,col2,col3\r\n1,4,7\r\n2,5,8\r\n3,6,9\r\n"
        ),
        (
            default_table,
            (delim="::", newline="\r\n"),
            "col1::col2::col3\r\n1::4::7\r\n2::5::8\r\n3::6::9\r\n"
        ),
        (
            default_table,
            (delim="::", newline="\r"),
            "col1::col2::col3\r1::4::7\r2::5::8\r3::6::9\r"
        ),
        # quotedstrings: #362
        (
            (col1=[1,2,3], col2=["hey", "the::re", "::sailor"], col3=[7,8,9]),
            (delim="::", quotestrings=true),
            "\"col1\"::\"col2\"::\"col3\"\n1::\"hey\"::7\n2::\"the::re\"::8\n3::\"::sailor\"::9\n"
        ),
        (
            (col1=[1,2,3], col2=[4,5,6], col3=["hey \r\n there","sailor","ho"]),
            (delim="::", newline="\r\n"),
            "col1::col2::col3\r\n1::4::\"hey \r\n there\"\r\n2::5::sailor\r\n3::6::ho\r\n"
        ),
        (
            (col1=[1,2,3], col2=[4,5,6], col3=["hey \r\n there","sailor","ho"]),
            (delim="::", newline="\r\n", quotestrings=true),
            "\"col1\"::\"col2\"::\"col3\"\r\n1::4::\"hey \r\n there\"\r\n2::5::\"sailor\"\r\n3::6::\"ho\"\r\n"
        ),
        # custom float decimal: #385
        (
            (col1=[1.1,2.2,3.3], col2=[4,5,6], col3=[7,8,9]),
            (delim='\t', decimal=','),
            "col1\tcol2\tcol3\n1,1\t4\t7\n2,2\t5\t8\n3,3\t6\t9\n"
        ),
        # custom abstract float decimal: #1108
        (
            (col1=AF.([1.1,2.2,3.3]), col2=[4,5,6], col3=[7,8,9]),
            (delim='\t', decimal=','),
            "col1\tcol2\tcol3\n1,1\t4\t7\n2,2\t5\t8\n3,3\t6\t9\n"
        ),
        # issue 515
        (
            (col1=[""],),
            NamedTuple(),
            "col1\n\n"
        ),
        # issue 540
        (
            NamedTuple{(Symbol("col1,col2"),), Tuple{Vector{String}}}((["hey"],)),
            NamedTuple(),
            "\"col1,col2\"\nhey\n"
        ),
        # 568
        (
            [(a=big(1),)],
            NamedTuple(),
            "a\n1\n"
        ),
        # 756
        (
            Any[(a=1,)],
            NamedTuple(),
            "a\n1\n"
        ),
        # jcunwin
        (
            [StructType(Date("2021-12-01"), "string 1", 123.45), StructType(Date("2021-12-02"), "string 2", 456.78)],
            (header=["Date Column", "String Column", "Number Column"],),
            "Date Column,String Column,Number Column\n2021-12-01,string 1,123.45\n2021-12-02,string 2,456.78\n"
        )
    ]

    io = IOBuffer()
    for case in testcases
        global x = case
        if :writeheader in keys(case[2])
            @test_deprecated case[1] |> CSV.write(io; case[2]...)
        else
            case[1] |> CSV.write(io; case[2]...)
        end
        @test String(take!(io)) == case[3]
        @test join(collect(CSV.RowWriter(case[1]; case[2]...))) == case[3]
    end

    @test_throws ErrorException (col1=[1,nothing,3], col2=[nothing, missing, missing], col3=[7,8,9]) |> CSV.write(io)

    default_table |> CSV.write(io)
    default_table |> CSV.write(io; append=false) # this is the default
    @test String(take!(io)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"

    default_table |> CSV.write(io)
    default_table |> CSV.write(io; append=true)
    @test String(take!(io)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n1,4,7\n2,5,8\n3,6,9\n"

    file = "test.csv"
    default_table |> CSV.write(file)
    @test String(read(file)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
    rm(file)

    filepath = Path(file)
    default_table |> CSV.write(filepath)
    @test String(read(filepath)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
    rm(filepath)

    open(file, "w") do io
        default_table |> CSV.write(io)
    end
    @test String(read(file)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
    rm(file)

    # #247
    open(file, "w") do io
        write(io, "or5a2ztZo\n")
        @test_deprecated (A=1:3, B=[17, 17, 19], C=["Wg5", "SJ4", "w48"]) |> CSV.write(io; append=true, writeheader=true)
    end
    @test String(read(file)) == "or5a2ztZo\nA,B,C\n1,17,Wg5\n2,17,SJ4\n3,19,w48\n"
    rm(file)

    # unknown schema case
    opts = CSV.Options(UInt8(','), UInt8('"'), UInt8('"'), UInt8('"'), UInt8('\n'), UInt8('.'), nothing, false, (), (col,val)->val, true)
    rt = [(a=1, b=4.0, c=7), (a=2.0, b=missing, c="8"), (a=3, b=6.0, c="9")]
    CSV.write(nothing, rt, io, opts)
    @test String(take!(io)) == "\xEF\xBB\xBFa,b,c\n1,4.0,7\n2.0,,8\n3,6.0,9\n"

    opts = CSV.Options(UInt8(','), UInt8('"'), UInt8('"'), UInt8('"'), UInt8('\n'), UInt8('.'), nothing, false, (), (col,val)->val, false)
    io = IOBuffer()
    CSV.write(nothing, Tables.rows(default_table), io, opts)
    @test String(take!(io)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"

    rt = [(a=1, b=4.0, c=7), (a=2.0, b=missing, c="8"), (a=3, b=6.0, c="9")]
    CSV.write(nothing, rt, io, opts)
    @test String(take!(io)) == "a,b,c\n1,4.0,7\n2.0,,8\n3,6.0,9\n"

    CSV.write(nothing, Tables.rows((col1=Int[], col2=Float64[])), io, opts)
    @test String(take!(io)) == ""

    CSV.write(nothing, Tables.rows((col1=Int[], col2=Float64[])), io, opts; header=["col1", "col2"])
    @test String(take!(io)) == "col1,col2\n"

    # 280
    io = IOBuffer()
    CSV.write(io, (x=[',','\n', ','],))
    @test String(take!(io)) == "x\n\",\"\n\"\n\"\n\",\"\n"

    CSV.write(io, (x=['-'],y=['-']), delim='-')
    @test String(take!(io)) == "x-y\n\"-\"-\"-\"\n"

    CSV.write(io, (x= [[1 2; 3 4]],y=[[5 6; 7 8]]), delim=';')
    @test String(take!(io)) == "x;y\n\"[1 2; 3 4]\";\"[5 6; 7 8]\"\n"

    if !Sys.iswindows()
        try
            io = open("$file.gz", "w")
            open(`gzip`, "w", io) do f
                CSV.write(f, default_table)
            end
            run(`gunzip $file.gz`)
            @test String(read("$file")) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
            rm(file)
        catch e
            @error "error running test" exception=(e, stacktrace(catch_backtrace()))
        end
    end

    # 357
    x1 = (ISBN=[9500286327, 671727680, 385333757], Book_Title=["Tres Mosqueteros, Los: Adaptacic\"n", "Romeo and Juliet", "Losing Julia"])
    CSV.write(  "x1.csv",  x1; delim=';' ,quotechar='"' ,escapechar='\\' )
    @test read("x1.csv", String) == "ISBN;Book_Title\n9500286327;\"Tres Mosqueteros, Los: Adaptacic\\\"n\"\n671727680;Romeo and Juliet\n385333757;Losing Julia\n"
    rm("x1.csv")

    # #137
    tbl = (a=[11,22], dt=[Date(2017,12,7), Date(2017,12,14)], dttm=[DateTime(2017,12,7), DateTime(2017,12,14)])
    io = IOBuffer()
    tbl |> CSV.write(io; delim='\t')
    seekstart(io)
    f = CSV.File(io; delim='\t')
    @test (f |> columntable) == tbl

    # validate char args: #369
    @test_throws ArgumentError default_table |> CSV.write(io; escapechar='☃')

    # write to stdout: #465
    old_stdout = stdout
    (rd, wr) = redirect_stdout()
    try
        default_table |> CSV.write(stdout)
    finally
        redirect_stdout(old_stdout)
        try
            close(wr)
        catch
        end
    end
    @test read(rd, String) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"

    df = (A=[1,2,3], B=["a", "b", "c"])
    # test control character delimiters
    for char ∈ Char.(UInt8[1,2,3,4])
        io = IOBuffer()
        CSV.write(io, df, delim=char)
        seekstart(io)
        @test columntable(CSV.File(io, delim=char)) == df
    end
    # don't allow writing with delimiters we refuse to read
    @test_throws ArgumentError CSV.write(io, df, delim='\r')

    # test with FilePath
    mktmpdir() do tmp
        CSV.write(tmp / "test.txt", df)
        @test columntable(CSV.File(tmp / "test.txt")) == df
    end

    io = Base.BufferStream()
    CSV.write(io, (a=[1,2,3], b=[4.1, 5.2, 6.3]))
    close(io)
    @test read(io, String) == "a,b\n1,4.1\n2,5.2\n3,6.3\n"

    # https://github.com/JuliaData/CSV.jl/issues/643
    s = join(1:1000000);
    @test_throws ArgumentError CSV.write("out.test.csv", [(a=s,)])

    # https://github.com/JuliaData/CSV.jl/issues/691
    io = IOBuffer()
    CSV.write(io, Tuple[(1,), (2,)], header=false)
    @test String(take!(io)) == "1\n2\n"

    # partition writing
    io = IOBuffer()
    io2 = IOBuffer()
    CSV.write([io, io2], Tables.partitioner((default_table, default_table)); partition=true)
    @test String(take!(io)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"
    @test String(take!(io2)) == "col1,col2,col3\n1,4,7\n2,5,8\n3,6,9\n"

    # compressed writing
    io = IOBuffer()
    CSV.write(io, default_table; compress=true)
    ct = CSV.read(io, Tables.columntable)
    @test ct == default_table

    # CSV.writerow
    row = (a=1, b=2.3, c="hey", d=Date(2022, 5, 4))
    str = CSV.writerow(row)
    @test str == "1,2.3,hey,2022-05-04\n"
    io = IOBuffer()
    CSV.writerow(io, row)
    @test String(take!(io)) == "1,2.3,hey,2022-05-04\n"
    str = CSV.writerow(row; delim='\t')
    @test str == "1\t2.3\they\t2022-05-04\n"

end # @testset "CSV.write"
