const PRECOMPILE_DATA = "int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"
function _precompile_()
    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    while false; end
    # CSV.Context(IOBuffer(CSV.PRECOMPILE_DATA))
    # foreach(row -> row, CSV.Rows(IOBuffer(PRECOMPILE_DATA)))
    CSV.Context(joinpath(dirname(pathof(CSV)), "promotions.csv"))
end
