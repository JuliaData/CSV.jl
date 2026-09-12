# Static hygiene: Core.Box captures anywhere in CSV (closure-capture race class),
# plus hot-signature dynamic-dispatch / Any scan.
using CSV, Dates, Durations
using Durations: Timestamp
K = CSV
boxes = String[]
for nm in names(K; all=true)
    f = try getfield(K, nm) catch; continue end
    (f isa Function || f isa Type) || continue
    ms = try methods(f) catch; continue end
    for m in ms
        m.module === K || continue
        ci = try Base.uncompressed_ast(m) catch; continue end
        for (i, st) in enumerate(ci.code)
            s = string(st)
            if occursin("Core.Box", s)
                push!(boxes, "$(m.name) @ $(basename(string(m.file))):$(m.line)  stmt $i: $(first(s, 100))")
            end
        end
    end
end
println("Core.Box sites: ", length(boxes))
foreach(println, unique(boxes))

# --- dynamic dispatch / Any scan over hot signatures --------------------------
function dynamic_calls(f, argtypes)
    cts = code_typed(f, argtypes; optimize=true)
    isempty(cts) && return ["<no method>"]
    ci, rt = cts[1]
    out = String[]
    for (i, st) in enumerate(ci.code)
        st isa Expr || continue
        if st.head === :call
            g = st.args[1]
            callee = g isa GlobalRef ? (isdefined(g.mod, g.name) ? getfield(g.mod, g.name) : g) :
                     g isa Core.SSAValue ? "ssa" : g
            if !(callee isa Core.Builtin || callee isa Core.IntrinsicFunction)
                push!(out, "dyn call: $(sprint(show, st)) ")
            end
        end
    end
    for (i, T) in enumerate(ci.ssavaluetypes)
        T === Any && push!(out, "Any ssa $i: $(sprint(show, ci.code[i]))")
    end
    return out
end
V8 = Vector{UInt8}
sigs = [
    (K.parsecolchunk!, (K.TypedColumn{Int64}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.TypedColumn{Float64}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.UnionColumn{Int64}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.TypedColumn{Date}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.TypedColumn{Timestamp{Nanosecond}}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.TypedColumn{Bool}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk!, (K.StringColumn, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Int, Nothing, Int, Int)),
    (K.parsecolchunk_missing, (V8, K.ChunkIndex, Int, Int, K.ValueOpts, Bool, K.ProblemLog, Nothing, Int, Int)),
    (K.indexchunk_fast!, (K.ChunkIndex, V8, K.Dialect, Val{:vec})),
    (K.indexchunk_fast!, (K.ChunkIndex, V8, K.Dialect, Val{:swar})),
    (K.indexchunk_scalar!, (K.ChunkIndex, V8, K.Dialect)),
    (K.indexchunk_lenient!, (K.ChunkIndex, V8, K.Dialect)),
    (K.assemblerows!, (K.ChunkIndex, V8, K.Dialect, Int)),
    (K.assemblecollapsed!, (K.ChunkIndex, V8, K.Dialect, Int)),
    (K.quoteparity, (V8, Int, Int, K.Dialect)),
    (K.quotetransitions, (V8, Int, Int, K.Dialect)),
    (K.nextrowstart, (V8, Int, Int, K.Dialect, Bool, Bool)),
    (K.detecttype, (V8, Int, Int, K.ValueOpts)),
    (K.cellcontent, (V8, Int, Int, K.ValueOpts)),
    (K.directchunk!, (K.ChunkIndex, V8, K.Dialect, K.ValueOpts, Int, Vector{Bool}, Vector{Type}, ReentrantLock, Vector{Any}, K.PendingProblemLog, Vector{Vector{Any}}, Vector{Vector{Type}}, Int, Nothing, Int, Int, Int, Bool, Vector{Bool}, Nothing, Nothing)),
    (K._internrange!, (Vector{UInt32}, K.DataStringVector{K.DataString}, Int, Int, Int, Threads.Atomic{Bool})),
    (K._internrange!, (Vector{UInt32}, K.DataStringVector{Union{Missing,K.DataString}}, Int, Int, Int, Threads.Atomic{Bool})),
    (K.materialize, (K.DataStringVector{K.DataString},)),
    (K._settlecolumnfrom, (Type{Int64}, V8, Vector{K.ChunkIndex}, Int, K.ValueOpts, Bool, Bool, Int, Int, Int)),
    (K._settlecolumnfrom, (Type{String}, V8, Vector{K.ChunkIndex}, Int, K.ValueOpts, Bool, Bool, Int, Int, Int)),
    (K._writerow_direct!, (K._WriteBuffer, Int, Int, Vector{K._WriteColumn}, K.WriteOpts{Nothing,Nothing})),
    (K._renderblock, (K._WriterColumns{Tuple{}}, Int, Int, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, Float64, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, Int64, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, String, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, K.DataString, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, Date, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, DateTime, K.WriteOpts{Nothing,Nothing})),
    (K._appendcell!, (K._WriteBuffer, Union{Missing,Int64}, K.WriteOpts{Nothing,Nothing})),
    (K._stagecolumn!, (K.ColStage, Vector{Int32}, Int, Int, K.WriteOpts{Nothing,Nothing})),
    (Base.getindex, (K._IndexedRow, Int)),
    (K._typedvalue, (Type{Int64}, K._IndexedRow, Int)),
    (K._typedvalue, (Type{Float64}, K._IndexedRow, Int)),
    (Base.getindex, (K.LazyColumn{Union{Missing,K.DataString}, K.DataString}, Int)),
    (Base.getindex, (K.LazyColumn{Union{Missing,Int64}, Int64}, Int)),
    (K.sampledetect!, (Vector{Type}, V8, K.ChunkIndex, Int, Int, K.ValueOpts, Nothing, Vector{Bool}, Nothing)),
    (K._narrowcolumn, (Type{Int32}, Vector{Int64}, Int, Vector{K.ChunkIndex}, K.ProblemLog, Int, Nothing)),
    (K._tounionrange!, (Vector{Union{Missing,Int64}}, Vector{Int64}, Vector{Bool}, Int, Int)),
    (K._fillslice!, (K.TypedColumn{Int64}, Int, Int)),
]
println("\n--- dynamic dispatch / Any scan ---")
for (f, at) in sigs
    issues = try dynamic_calls(f, at) catch e; ["<error: $(sprint(showerror, e))>"] end
    println(rpad(string(nameof(f)), 22), " ", join(string.(at), ", ")[1:min(end, 90)], "  → ",
            isempty(issues) ? "clean" : "$(length(issues)) issue(s)")
    for s in issues[1:min(end, 12)]
        println("    ", first(s, 220))
    end
end
