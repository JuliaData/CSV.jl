using PrecompileTools

const PRECOMPILE_DATA = """"int,float,date,datetime,bool,null,str,catg,int_float
1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2
2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14
"""

@compile_workload begin
    CSV.Context(IOBuffer(PRECOMPILE_DATA))
    collect(CSV.Rows(IOBuffer(PRECOMPILE_DATA)))

    for basename in ["precompile_small.csv", "promotions.csv"]
        collect(CSV.File(joinpath(dirname(pathof(CSV)), "..", "test", "testfiles", basename)))
    end

    CSV.read(
        joinpath(dirname(pathof(CSV)), "..", "test", "testfiles", "precompile_small.csv"),
        Tables.dictcolumntable
    )

    table = Tables.dictcolumntable(Dict(
        :a => ["foo", "bar"],
        :b => [1, 2],
        :c => [1.0, NaN],
        :d => [true, false],
        :e => [Dates.today(), Dates.today()],
        :f => [Dates.now(), Dates.now()],
    ))
    CSV.write(IOBuffer(), table)
end
