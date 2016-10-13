
# DataFrames
FILE = joinpath(DSTESTDIR, "randoms_small.csv")
DF = CSV.read(FILE)
DF2 = CSV.read(FILE)
dfsource = Tester("DataFrame", x->x, false, DataFrame, (:DF,), scalartransforms, vectortransforms, x->x, x->nothing)
dfsink = Tester("DataFrame", x->x, false, DataFrame, (:DF2,), scalartransforms, vectortransforms, x->x, x->nothing)
function DataFrames.DataFrame(sym::Symbol; append::Bool=false)
    return @eval $sym
end
function DataFrames.DataFrame(sch::Data.Schema, ::Type{Data.Field}, append::Bool, ref::Vector{UInt8}, sym::Symbol)
    return DataFrame(DataFrame(sym), sch, Data.Field, append, ref)
end
function DataFrame(sink, sch::Data.Schema, ::Type{Data.Field}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    newsize = max(0, rows + (append ? size(sink, 1) : 0))
    # need to make sure we don't break a NullableVector{WeakRefString{UInt8}} when appending
    if append
        for (i, T) in enumerate(Data.types(sch))
            if T <: Nullable{WeakRefString{UInt8}}
                sink.columns[i] = NullableArray(String[string(get(x, "")) for x in sink.columns[i]])
                sch.types[i] = Nullable{String}
            end
        end
    else
        for (i, T) in enumerate(Data.types(sch))
            if T != eltype(sink.columns[i])
                sink.columns[i] = NullableArray(eltype(T), newsize)
            end
        end
    end
    foreach(x->resize!(x, newsize), sink.columns)
    if !append
        for (i, T) in enumerate(eltypes(sink))
            if T <: Nullable{WeakRefString{UInt8}}
                sink.columns[i] = NullableArray{WeakRefString{UInt8}, 1}(Array{WeakRefString{UInt8}}(newsize), fill(true, newsize), isempty(ref) ? UInt8[] : ref)
            end
        end
    end
    sch.rows = newsize
    return sink
end

# CSV
FILE2 = joinpath(DSTESTDIR, "randoms2_small.csv")
csvsource = Tester("CSV.Source", CSV.read, true, CSV.Source, (FILE,), scalartransforms, vectortransforms, x->x, x->nothing)
csvsink = Tester("CSV.Sink", CSV.write, true, CSV.Sink, (FILE2,), scalartransforms, vectortransforms, x->CSV.read(FILE2; use_mmap=false), x->rm(FILE2))

DataStreamsIntegrationTests.teststream([dfsource, csvsource], [dfsink, csvsink]; rows=99)
