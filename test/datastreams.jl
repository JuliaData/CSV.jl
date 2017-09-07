reload("Nulls"); reload("WeakRefStrings"); reload("DataStreams"); reload("CSV"); reload("DataStreamsIntegrationTests")
# NamedTuples
FILE = joinpath(DataStreamsIntegrationTests.DSTESTDIR, "randoms_small.csv")
DF = CSV.read(FILE)
DF2 = CSV.read(FILE)
dfsource = DataStreamsIntegrationTests.Tester("NamedTuple", x->x, false, NamedTuple, (DF,), DataStreamsIntegrationTests.scalartransforms, DataStreamsIntegrationTests.vectortransforms, x->x, x->nothing)
dfsink = DataStreamsIntegrationTests.Tester("NamedTuple", x->x, false, NamedTuple, (DF2,), DataStreamsIntegrationTests.scalartransforms, DataStreamsIntegrationTests.vectortransforms, x->x, x->nothing)

# CSV
FILE2 = joinpath(DataStreamsIntegrationTests.DSTESTDIR, "randoms2_small.csv")
csvsource = DataStreamsIntegrationTests.Tester("CSV.Source", CSV.read, true, CSV.Source, (FILE,), DataStreamsIntegrationTests.scalartransforms, DataStreamsIntegrationTests.vectortransforms, x->x, x->nothing)
csvsink = DataStreamsIntegrationTests.Tester("CSV.Sink", CSV.write, true, CSV.Sink, (FILE2,), DataStreamsIntegrationTests.scalartransforms, DataStreamsIntegrationTests.vectortransforms, x->CSV.read(FILE2; use_mmap=false), x->rm(FILE2))

DataStreamsIntegrationTests.teststream([dfsource, csvsource], [dfsink]; rows=99)
