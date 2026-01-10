using PrecompileTools

@setup_workload begin
    # Putting some things in `setup` can reduce the size of the
    # precompile file and potentially make loading faster.
    PRECOMPILE_DATA = "int,float,date,datetime,bool,null,str,catg,int_float\n1,3.14,2019-01-01,2019-01-01T01:02:03,true,,hey,abc,2\n2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,abc,3.14\n"
    @compile_workload begin
        # all calls in this block will be precompiled, regardless of whether
        # they belong to your package or not (on Julia 1.8 and higher)
        CSV.File(IOBuffer(PRECOMPILE_DATA))
        CSV.File(Vector{UInt8}(PRECOMPILE_DATA))
    end
end

precompile(CSV.File, (String,))
