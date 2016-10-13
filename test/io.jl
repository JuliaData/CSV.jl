# `CSV.readline(io::IO, q='"', e='\\', buf::IOBuffer=IOBuffer())` => `String`
str = "field1,field2,\"quoted \\\"field with \n embedded newline\",field3"
io = IOBuffer(str)
@test CSV.readline(io) == str
io = IOBuffer(str * "\n" * str * "\r\n" * str)
@test CSV.readline(io) == str * "\n"
@test CSV.readline(io) == str * "\r\n"
@test CSV.readline(io) == str

# `CSV.readline(source::CSV.Source)` => `String`
source = CSV.Source(IOBuffer(str); header=["col1","col2","col3","col4"])
@test CSV.readline(source) == str

# `CSV.readsplitline(io, d=',', q='"', e='\\', buf::IOBuffer=IOBuffer())` => `Vector{String}`
spl = ["field1","field2","quoted \\\"field with \n embedded newline", "field3"]
io = IOBuffer(str)
@test CSV.readsplitline(io) == spl
io = IOBuffer(str * "\n" * str * "\r\n" * str)
@test CSV.readsplitline(io) == spl
@test CSV.readsplitline(io) == spl
@test CSV.readsplitline(io) == spl

# `CSV.readsplitline(source::CSV.Source)` => `Vector{String}`
source = CSV.Source(IOBuffer(str); header=["col1","col2","col3","col4"])
@test CSV.readsplitline(source) == spl

# `CSV.countlines(io::IO, quotechar, escapechar)` => `Int`
@test CSV.countlines(IOBuffer(str)) == 1
@test CSV.countlines(IOBuffer(str * "\n" * str)) == 2

# `CSV.countlines(source::CSV.Source)` => `Int`
source = CSV.Source(IOBuffer(str); header=["col1","col2","col3","col4"])
@test CSV.countlines(source) == 1
