# CSV [BETA]

[![CSV](http://pkg.julialang.org/badges/Example_0.4.svg)](http://pkg.julialang.org/?pkg=CSV&ver=0.4)

Linux: [![Build Status](https://travis-ci.org/JuliaDB/CSV.jl.svg?branch=master)](https://travis-ci.org/JuliaDB/CSV.jl)

Windows: [![Build Status](https://ci.appveyor.com/api/projects/status/github/JuliaDB/CSV.jl?branch=master&svg=true)](https://ci.appveyor.com/project/tkelman/example-jl/branch/master)

[![Coverage Status](https://coveralls.io/repos/JuliaDB/CSV.jl/badge.svg?branch=master)](https://coveralls.io/r/JuliaDB/CSV.jl?branch=master)

A package for working with CSV and other delimited files.

Types/functions:

* `CSV.Source`/`CSV.Sink`: `Data.Source` and `Data.Sink` types for the [DataStreams.jl](https://github.com/JuliaDB/DataStreams.jl) interface
* `CSV.Options`: a type that collects various parsing configurations that can be passed to `CSV.Source` or `CSV.read`
* `Data.stream!(::CSV.Source,::CSV.Sink)`: method for streaming data from a CSV source to a CSV sink (csv file to csv file)
* `Data.stream!(::CSV.Source,::Data.Table)`: method for streaming data from a CSV source to a `Data.Table` (Julia structure)
* `Data.stream!(::Data.Table,::CSV.Sink)`: method for streaming data from a `Data.Table` to a CSV sink
* `CSV.getfield{T}(io::IOBuffer, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0)`: various custom parsing methods for types `T`
* `readline(f::IO,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())`: custom `readline` implementation that accounts for potentially quoted newlines
* `readsplitline(f::IO,d::UInt8,q::UInt8,e::UInt8,buf::IOBuffer=IOBuffer())`: similar to `readline`, but also splits each field into a separate element in a `Vector{UTF8String}`
* `countlines(f::IO,q::UInt8,e::UInt8)`: custom `countlines` implementation that accounts for potential quoted newlines

See the help documentation for any of the above for additional details (e.g. `?CSV.Options`, `?CSV.read`, etc.)

The package is currently in "beta", which means it's been tested, used by various and sundry whipper-snappers, but most likely has some corner cases yet to be fleshed out. Please let us know about your experience, any bugs, or feature requests [here](https://github.com/JuliaDB/CSV.jl/issues/new)!
