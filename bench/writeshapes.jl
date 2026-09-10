# Writer benchmark tables: one NamedTuple per shape (numeric, mixed, strings,
# quoted, wide, datetime). Shared by writebench.jl and bench_surface.jl.
using Random, Dates

function shape(kind::Symbol, n::Int, rng)
    kind === :numeric   && return (a = rand(rng, Int64, n), b = rand(rng, n), c = rand(rng, Int32, n),
                                   d = rand(rng, n) .* 1e6, e = rand(rng, 1:1000, n))
    kind === :mixed     && return (id = collect(1:n), region = [rand(rng, ("north","south","east","west")) for _ in 1:n],
                                   price = rand(rng, n) .* 1000, qty = rand(rng, 1:1000, n),
                                   note = [rand(rng, Bool) ? "plain text" : "needs, quoting \"here\"" for _ in 1:n],
                                   flag = rand(rng, Bool, n), day = [Date(2020,1,1) + Day(i % 1000) for i in 1:n],
                                   maybe = [rand(rng) < 0.1 ? missing : rand(rng, Int32) for _ in 1:n])
    kind === :strings   && return (s1 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n],
                                   s2 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n],
                                   s3 = [String(rand(rng, 'a':'z', rand(rng, 3:14))) for _ in 1:n])
    kind === :quoted    && return (s1 = ["v,$(i)" for i in 1:n], s2 = ["say \"hi\" $(i)" for i in 1:n],
                                   s3 = [String(rand(rng, 'a':'z', 8)) for _ in 1:n])
    kind === :wide      && return NamedTuple{Tuple(Symbol("c$i") for i in 1:60)}(Tuple(rand(rng, n) for _ in 1:60))
    kind === :datetime  && return (t = [DateTime(2020,1,1) + Second(i) for i in 1:n], d = [Date(2020,1,1) + Day(i % 3000) for i in 1:n],
                                   x = rand(rng, Int64, n))
    error("unknown shape")
end
