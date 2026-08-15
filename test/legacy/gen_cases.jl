# Generate test/legacy/cases_file.jl: one agree(...) per extracted CSV.File
# call whose input is a self-contained literal or a corpus path. Calls with
# free variables are emitted commented-out for hand triage.
lines = readlines(joinpath(@__DIR__, "legacy_calls.txt"))
entries = []
let i = 1
while i <= length(lines)
    if startswith(lines[i], "### ")
        hdr = lines[i][5:end]
        j = i + 1
        buf = String[]
        while j <= length(lines) && !startswith(lines[j], "### ")
            push!(buf, lines[j]); j += 1
        end
        push!(entries, (hdr, join(buf, "\n")))
        i = j
    else
        i += 1
    end
end
end
# Pinned 1.0 deltas: the two implementations MUST disagree here (a stale pin
# fails). Reason strings are the migration-guide entries.
DELTAS = Dict(
    "basics:704" => (outcome=:differ, reason="empty unquoted cell is ALWAYS missing (missingstring only ADDS spellings); 0.10's missingstring=nothing made empties present \"\""),
    "basics:529" => (outcome=:new_errors, reason="stringtype=PosLenString retired: CompactString (default) or String"),
    "basics:795" => (outcome=:new_errors, reason="function-typed types=/pool= retired: Dict / vector / Type forms (Tables.Scan is the expression channel)"),
    "basics:281" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:282" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:283" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:284" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:285" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:286" => (outcome=:old_errors, reason="unclosed quote is a reported problem, not a fatal error (warnings are data)"),
    "basics:60"  => (outcome=:old_errors, reason="NUL is an accepted delimiter byte (0.10 rejected it)"),
    "basics:61"  => (outcome=:old_errors, reason="NUL is an accepted delimiter byte (0.10 rejected it)"),
)
out = IOBuffer()
println(out, "# GENERATED from the 0.10 test suite (legacy/test) by the audit's extractor;")
println(out, "# each entry replays one legacy CSV.File call through both implementations.")
println(out, "# Hand-triaged entries carry a comment; see AUDIT.md for the ledger.\n")
println(out, "@testset \"legacy corpus: CSV.File replay\" begin")
nauto = nmanual = 0
for (hdr, text) in entries
    occursin("[CSV.File]", hdr) || continue
    # strip the head "CSV.File(" ... ")" -> args
    inner = strip(text)
    startswith(inner, "CSV.File(") || continue
    args = inner[length("CSV.File(")+1:end-1]
    selfcontained = occursin("IOBuffer(", args) || occursin("joinpath(dir", args) ||
                    occursin("codeunits(", args)
    # any bare identifier used as the whole input or inside IOBuffer(...) is a
    # free legacy variable — hand triage (the generator has no scope)
    # a call is self-contained when its INPUT is a literal/corpus path and its
    # kwargs reference no legacy-only names (retired types, custom types, helper
    # variables); everything else is hand triage
    inputlit = occursin(r"^\s*IOBuffer\(\"", args) || occursin(r"^\s*corpusfile\(\"", args) ||
               occursin(r"^\s*joinpath\(dir,\s*\"", args) ||
               occursin(r"^\s*codeunits\(\"", args)
    # strip the corpusfile("...") input before scanning kwargs for free names
    kwargs_only = replace(args, r"corpusfile\(\"[^\"]*\"\)" => "CORPUS")
    legacyname = occursin(r"\b(PosLenString|InlineString\d*|String\d+|test_logs|CustomTypes|catcmd)\b", kwargs_only) ||
                 occursin(r"\b(data|buf|io|str|csv|tm|parent|source|path|firstbyte|lastbyte)\b\s*(?:[,;)\]]|$)", kwargs_only) ||
                 occursin(r"\(\s*i\s*,\s*nm\s*\)\s*->", kwargs_only)   # function-typed forms are retired
    lbl = replace(split(hdr, " [")[1], ".jl" => "")
    pinned = haskey(DELTAS, lbl)
    freevar = !pinned && (!inputlit || legacyname || occursin("test_logs", text) ||
                          hdr in ("basics.jl:727 [CSV.File]",))
    args = replace(args, "joinpath(dir, " => "corpusfile(")
    # legacy-only kwargs the new side rejects outright: strip for the replay
    args = replace(args, r",?\s*silencewarnings\s*=\s*true" => "")
    if selfcontained && !freevar
        sep = occursin(";", args) ? ", " : "; "
        if pinned
            delta = DELTAS[lbl]
            println(out, "    # PINNED 1.0 DELTA: ", delta.reason)
            println(out, "    @case \"", lbl, "\" agree(", args, sep, "label=\"", lbl,
                    "\", expect_delta=(outcome=:", delta.outcome, ", reason=\"",
                    replace(delta.reason, "\"" => "\\\""), "\"))")
        else
            println(out, "    @case \"", lbl, "\" agree(", args, sep, "label=\"", lbl, "\")")
        end
        global nauto += 1
    else
        println(out, "    # MANUAL ", hdr, ": agree(", replace(args, "\n" => " "), ")")
        global nmanual += 1
    end
end
println(out, "end")
write(joinpath(@__DIR__, "cases_file.jl"), String(take!(out)))
println("auto=", nauto, " manual=", nmanual)
