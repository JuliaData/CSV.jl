using PrecompileTools

const PRECOMPILE_DATA = """"int,float,date,datetime,bool,null,str,catg,int_float
1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2
2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14
"""

@compile_workload begin
    CSV.Context(IOBuffer(PRECOMPILE_DATA))
    collect(CSV.Rows(IOBuffer(PRECOMPILE_DATA)))

    # Use promotions.csv from src/ directory for precompilation
    collect(CSV.File(joinpath(dirname(pathof(CSV)), "promotions.csv")))

    CSV.read(IOBuffer(PRECOMPILE_DATA), Tables.dictcolumntable)

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
