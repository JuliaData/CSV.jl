# Empirical pin of CSV.jl ignorerepeated semantics — run with --project=. (dev CSV)
using CSV, Tables

function show_file(label, s; kw...)
    println("== ", label, " | ", repr(s), " | ", kw)
    try
        f = CSV.File(IOBuffer(s); ignorerepeated=true, kw...)
        println("   names: ", Tables.columnnames(f))
        for (i, row) in enumerate(f)
            println("   row $i: ", [Tables.getcolumn(row, j) for j in 1:length(Tables.columnnames(f))])
        end
    catch err
        println("   ERROR: ", sprint(showerror, err))
    end
end

show_file("q1 basic", "a b\n1 2\n"; delim=' ')
show_file("q1b runs", "a  b\n1   2\n"; delim=' ')
show_file("q2 leading", " a b\n 1 2\n"; delim=' ')
show_file("q2b leading runs", "  a b\n   1 2\n"; delim=' ')
show_file("q3 trailing", "a b \n1 2  \n"; delim=' ')
show_file("q4 all-delim row", "a b\n   \n1 2\n"; delim=' ')
show_file("q4b all-delim + ignoreemptyrows=false", "a b\n   \n1 2\n"; delim=' ', ignoreemptyrows=false)
show_file("q5 quoted empty", "a b\n\"\" 2\n"; delim=' ')
show_file("q5b quoted mid", "a b c\n1 \"x y\" 3\n"; delim=' ')
show_file("q6 comment after padding", "a b\n  #x\n1 2\n"; delim=' ', comment="#")
show_file("q7 multibyte delim", "a::::b\n1::2\n"; delim="::")
show_file("q7b multibyte trailing", "a::b::\n1::2::::\n"; delim="::")
show_file("q8 header leading", "  a b\n1 2\n"; delim=' ')
show_file("q9 short row", "a b c\n1 2\n3 4 5\n"; delim=' ')
show_file("q10 long row", "a b\n1 2 3\n"; delim=' ')
show_file("q11 quoted keeps spaces", "a b\n\"x  y\" 2\n"; delim=' ')
show_file("q12 eof no newline trailing run", "a b\n1 2   "; delim=' ')
show_file("q13 tab runs", "a\tb\n1\t\t2\n"; delim='\t')
