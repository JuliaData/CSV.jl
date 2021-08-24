function _precompile_()
    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    @assert Base.precompile(Tuple{typeof(parsefilechunk!),Context,Vector{UInt8},Int64,Int64,Int64,Int64,Vector{Column},Val{false},Type{Tuple{}}})   # time: 1.0312017
    @assert Base.precompile(Tuple{typeof(parsevalue!),Type{BigFloat},Vector{UInt8},Int64,Int64,Int64,Int64,Int64,Column,Context})   # time: 0.5886147
    @assert Base.precompile(Tuple{typeof(detect),String})   # time: 0.14655608
    @assert Base.precompile(Tuple{typeof(write),Base.BufferStream,NamedTuple{(:a, :b), Tuple{Vector{Int64}, Vector{Float64}}}})   # time: 0.06289695
    @assert Base.precompile(Tuple{typeof(parsevalue!),Type{BigInt},Vector{UInt8},Int64,Int64,Int64,Int64,Int64,Column,Context})   # time: 0.0527356
    @assert Base.precompile(Tuple{typeof(getname),Cmd})   # time: 0.050515886
    @assert Base.precompile(Tuple{typeof(write),IOBuffer,NamedTuple{(:x,), Tuple{Vector{Char}}}})   # time: 0.035573076
    @assert Base.precompile(Tuple{typeof(parsevalue!),Type{UInt32},Vector{UInt8},Int64,Int64,Int64,Int64,Int64,Column,Context})   # time: 0.035170615
    @assert Base.precompile(Tuple{Type{File},Vector{IOBuffer}})   # time: 0.033758428
    @assert Base.precompile(Tuple{Type{File},Context,Bool})   # time: 0.033758428
    @assert Base.precompile(Tuple{Type{Context},Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg,Arg})
    @assert Base.precompile(Tuple{typeof(write),Base.Process,NamedTuple{(:col1, :col2, :col3), Tuple{Vector{Int64}, Vector{Int64}, Vector{Int64}}}})   # time: 0.03311246
    @assert Base.precompile(Tuple{typeof(detectcolumnnames),Vector{UInt8},Int64,Int64,Int64,Parsers.Options,Any,Bool})   # time: 0.027399136
    @assert Base.precompile(Tuple{typeof(findchunkrowstart),Vector{Int64},Int64,Vector{UInt8},Parsers.Options,Bool,Int64,Int64,Vector{Column},ReentrantLock,Any,Base.Threads.Atomic{Int64},Base.Threads.Atomic{Int64},Base.Threads.Atomic{Bool}})   # time: 0.026987862
    @assert Base.precompile(Tuple{typeof(write),String,Vector{NamedTuple{(:a,), Tuple{String}}}})   # time: 0.022341058
    @assert Base.precompile(Tuple{typeof(makepooled!),Column})   # time: 0.020273618
    @assert Base.precompile(Tuple{typeof(unpool!),Column,Type,RefPool})   # time: 0.019640453
    @assert Base.precompile(Tuple{typeof(Tables.getcolumn),File,Symbol})   # time: 0.018873245
    @assert Base.precompile(Tuple{typeof(getsource),Vector{UInt8},Bool})   # time: 0.013699824
    @assert Base.precompile(Tuple{typeof(detectheaderdatapos),Vector{UInt8},Int64,Int64,UInt8,UInt8,UInt8,Tuple{Ptr{UInt8}, Int64},Bool,Any,Int64})   # time: 0.012563237
    @assert Base.precompile(Tuple{typeof(detectheaderdatapos),Vector{UInt8},Int64,Int64,UInt8,UInt8,UInt8,Nothing,Bool,Any,Int64})   # time: 0.011714202
    @assert Base.precompile(Tuple{typeof(Tables.schema),File})   # time: 0.011355454
    @assert Base.precompile(Tuple{typeof(detectdelimandguessrows),Vector{UInt8},Int64,Int64,Int64,UInt8,UInt8,UInt8,UInt8,Tuple{Ptr{UInt8}, Int64},Bool})   # time: 0.010538074
    @assert Base.precompile(Tuple{Type{Context},Val{false},String,Vector{Symbol},Int64,Int64,Vector{UInt8},Int64,Int64,Int64,Parsers.Options,Vector{Column},Float64,Bool,Type,Dict{DataType, DataType},Type,Int64,Bool,Int64,Vector{Int64},Bool,Bool,Int64,Bool,Nothing,Bool})   # time: 0.010367362
    @assert Base.precompile(Tuple{typeof(detectdelimandguessrows),Vector{UInt8},Int64,Int64,Int64,UInt8,UInt8,UInt8,UInt8,Nothing,Bool})   # time: 0.010337504
    @assert Base.precompile(Tuple{typeof(warning),Type,Vector{UInt8},Int64,Int64,Int16,Int64,Int64})   # time: 0.008919449
    @assert Base.precompile(Tuple{Type{Rows},String})   # time: 0.008758181
    @assert Base.precompile(Tuple{Type{Rows},IOBuffer})   # time: 0.008604496
end
