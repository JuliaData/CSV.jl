# Walk the legacy test files' ASTs; collect every CSV.File / CSV.read call
# expression as source text with its file:line, and classify the input.
using JuliaSyntax
files = ["basics.jl", "iteration.jl", "runtests.jl", "write.jl", "testfiles.jl"]
root = joinpath(@__DIR__, "..", "..", "legacy", "test")
calls = []
function walk(node, src, file, out)
    kids = JuliaSyntax.children(node)
    kids === nothing && return
    if JuliaSyntax.kind(node) == JuliaSyntax.K"call"
        cs = kids
        if !isempty(cs)
            head = JuliaSyntax.sourcetext(cs[1])
            if head in ("CSV.File", "CSV.read", "CSV.Rows", "CSV.Chunks")
                push!(out, (file=file, line=JuliaSyntax.source_line(node), head=head,
                            text=JuliaSyntax.sourcetext(node)))
            end
        end
    end
    for c in kids
        walk(c, src, file, out)
    end
end
for f in files
    src = read(joinpath(root, f), String)
    tree = JuliaSyntax.parseall(JuliaSyntax.SyntaxNode, src; filename=f, ignore_errors=true)
    walk(tree, src, f, calls)
end
println(length(calls), " calls")
using Printf
counts = Dict{String,Int}()
for c in calls; counts[c.head] = get(counts, c.head, 0) + 1; end
println(counts)
# input classification
cls = Dict{String,Int}()
for c in calls
    t = c.text
    k = occursin("IOBuffer(", t) ? "IOBuffer literal" :
        occursin("joinpath(dir", t) ? "corpus file" :
        occursin("codeunits(", t) || occursin("@view", t) ? "bytes" :
        occursin("`", t) ? "Cmd" : "other/variable"
    cls[k] = get(cls, k, 0) + 1
end
println(cls)
open(joinpath(@__DIR__, "legacy_calls.txt"), "w") do io
    for c in calls
        println(io, "### ", c.file, ":", c.line, " [", c.head, "]")
        println(io, c.text)
    end
end
