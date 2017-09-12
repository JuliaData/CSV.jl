io = IOBuffer("""A,B,C
1,1,10
6,1""")

@test_throws CSV.ExpectedMoreColumnsError CSV.validate(io)

io = IOBuffer("""A;B;C
1,1,10
2,0,16""")
@test_throws CSV.TooManyColumnsError CSV.validate(io)

io = IOBuffer("""A;B;C
1,1,10
2,0,16""")
@test_throws CSV.ExpectedMoreColumnsError CSV.validate(io; delim=';')