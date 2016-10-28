var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Home",
    "title": "Home",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#CSV.jl-Documentation-1",
    "page": "Home",
    "title": "CSV.jl Documentation",
    "category": "section",
    "text": "CSV.jl is built to be a fast and flexible pure-Julia library for handling delimited text files."
},

{
    "location": "index.html#CSV.read",
    "page": "Home",
    "title": "CSV.read",
    "category": "Function",
    "text": "CSV.read(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...) => typeof(sink)\n\nCSV.read(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...) => Data.Sink\n\nparses a delimited file into a Julia structure (a DataFrame by default, but any valid Data.Sink may be requested).\n\nPositional arguments:\n\nfullpath; can be a file name (string) or other IO instance\nsink::Type{T}; DataFrame by default, but may also be other Data.Sink types that support streaming via Data.Field interface; note that the method argument can be the type of Data.Sink, plus any required arguments the sink may need (args...).                   or an already constructed sink may be passed (2nd method above)\n\nKeyword Arguments:\n\ndelim::Union{Char,UInt8}; a single character or ascii-compatible byte that indicates how fields in the file are delimited; default is UInt8(',')\nquotechar::Union{Char,UInt8}; the character that indicates a quoted field that may contain the delim or newlines; default is UInt8('\"')\nescapechar::Union{Char,UInt8}; the character that escapes a quotechar in a quoted field; default is UInt8('\\')\nnull::String; an ascii string that indicates how NULL values are represented in the dataset; default is the empty string, \"\"\nheader; column names can be provided manually as a complete Vector{String}, or as an Int/Range which indicates the row/rows that contain the column names\ndatarow::Int; specifies the row on which the actual data starts in the file; by default, the data is expected on the next row after the header row(s); for a file without column names (header), specify datarow=1\ntypes; column types can be provided manually as a complete Vector{DataType}, or in a Dict to reference individual columns by name or number\nnullable::Bool; indicates whether values can be nullable or not; true by default. If set to false and missing values are encountered, a NullException will be thrown\nweakrefstrings::Bool=true: indicates whether string-type columns should use the WeakRefString (for efficiency) or a regular String type\ndateformat::Union{AbstractString,Dates.DateFormat}; how all dates/datetimes in the dataset are formatted\nfooterskip::Int; indicates the number of rows to skip at the end of the file\nrows_for_type_detect::Int=100; indicates how many rows should be read to infer the types of columns\nrows::Int; indicates the total number of rows to read from the file; by default the file is pre-parsed to count the # of rows; -1 can be passed to skip a full-file scan, but the Data.Sink must be setup account for a potentially unknown # of rows\nuse_mmap::Bool=true; whether the underlying file will be mmapped or not while parsing\nappend::Bool=false; if the sink argument provided is an existing table, append=true will append the source's data to the existing data instead of doing a full replace\ntransforms::Dict{Union{String,Int},Function}; a Dict of transforms to apply to values as they are parsed. Note that a column can be specified by either number or column name.\n\nNote by default, \"string\" or text columns will be parsed as the WeakRefString type. This is a custom type that only stores a pointer to the actual byte data + the number of bytes. To convert a String to a standard Julia string type, just call string(::WeakRefString), this also works on an entire column. Oftentimes, however, it can be convenient to work with WeakRefStrings depending on the ultimate use, such as transfering the data directly to another system and avoiding all the intermediate copying.\n\nExample usage:\n\njulia> dt = CSV.read(\"bids.csv\")\n7656334×9 DataFrames.DataFrame\n│ Row     │ bid_id  │ bidder_id                               │ auction │ merchandise      │ device      │\n├─────────┼─────────┼─────────────────────────────────────────┼─────────┼──────────────────┼─────────────┤\n│ 1       │ 0       │ \"8dac2b259fd1c6d1120e519fb1ac14fbqvax8\" │ \"ewmzr\" │ \"jewelry\"        │ \"phone0\"    │\n│ 2       │ 1       │ \"668d393e858e8126275433046bbd35c6tywop\" │ \"aeqok\" │ \"furniture\"      │ \"phone1\"    │\n│ 3       │ 2       │ \"aa5f360084278b35d746fa6af3a7a1a5ra3xe\" │ \"wa00e\" │ \"home goods\"     │ \"phone2\"    │\n...\n\nOther example invocations may include:\n\n# read in a tab-delimited file\nCSV.read(file; delim='	')\n\n# read in a comma-delimited file with null values represented as 'N', such as a MySQL export\nCSV.read(file; null=\"\\N\")\n\n# manually provided column names; must match # of columns of data in file\n# this assumes there is no header row in the file itself, so data parsing will start at the very beginning of the file\nCSV.read(file; header=[\"col1\", \"col2\", \"col3\"])\n\n# manually provided column names, even though the file itself has column names on the first row\n# `datarow` is specified to ensure data parsing occurs at correct location\nCSV.read(file; header=[\"col1\", \"col2\", \"col3\"], datarow=2)\n\n# types provided manually; as a vector, must match length of columns in actual data\nCSV.read(file; types=[Int, Int, Float64])\n\n# types provided manually; as a Dict, can specify columns by # or column name\nCSV.read(file; types=Dict(3=>Float64, 6=>String))\nCSV.read(file; types=Dict(\"col3\"=>Float64, \"col6\"=>String))\n\n# manually provided # of rows; if known beforehand, this will improve parsing speed\n# this is also a way to limit the # of rows to be read in a file if only a sample is needed\nCSV.read(file; rows=10000)\n\n# for data files, `file` and `file2`, with the same structure, read both into a single DataFrame\n# note that `df` is used as a 2nd argument in the 2nd call to `CSV.read` and the keyword argument\n# `append=true` is passed\ndf = CSV.read(file)\ndf = CSV.read(file2, df; append=true)\n\n# manually construct a `CSV.Source` once, then stream its data to both a DataFrame\n# and SQLite table `sqlite_table` in the SQLite database `db`\n# note the use of `CSV.reset!` to ensure the `source` can be streamed from again\nsource = CSV.Source(file)\ndf1 = CSV.read(source, DataFrame)\nCSV.reset!(source)\ndb = SQLite.DB()\nsq1 = CSV.read(source, SQLite.Sink, db, \"sqlite_table\")\n\n\n\n"
},

{
    "location": "index.html#CSV.write",
    "page": "Home",
    "title": "CSV.write",
    "category": "Function",
    "text": "CSV.write(file_or_io::Union{AbstractString,IO}, source::Type{T}, args...; kwargs...) => CSV.Sink\n\nCSV.write(file_or_io::Union{AbstractString,IO}, source::Data.Source; kwargs...) => CSV.Sink\n\nwrite a Data.Source out to a file_or_io.\n\nPositional Arguments:\n\nfile_or_io; can be a file name (string) or other IO instance\nsource can be the type of Data.Source, plus any required args..., or an already constructed Data.Source can be passsed in directly (2nd method)\n\nKeyword Arguments:\n\ndelim::Union{Char,UInt8}; how fields in the file will be delimited; default is UInt8(',')\nquotechar::Union{Char,UInt8}; the character that indicates a quoted field that may contain the delim or newlines; default is UInt8('\"')\nescapechar::Union{Char,UInt8}; the character that escapes a quotechar in a quoted field; default is UInt8('\\')\nnull::String; the ascii string that indicates how NULL values will be represented in the dataset; default is the emtpy string \"\"\ndateformat; how dates/datetimes will be represented in the dataset; default is ISO-8601 yyyy-mm-ddTHH:MM:SS.s\nheader::Bool; whether to write out the column names from source\ncolnames::Vector{String}; a vector of string column names to be used when writing the header row\nappend::Bool; start writing data at the end of io; by default, io will be reset to the beginning or overwritten before writing\ntransforms::Dict{Union{String,Int},Function}; a Dict of transforms to apply to values as they are parsed. Note that a column can be specified by either number or column name.\n\nA few example invocations include:\n\n# write out a DataFrame `df` to a file name \"out.csv\" with all defaults, including comma as delimiter\nCSV.write(\"out.csv\", df)\n\n# write out a DataFrame, this time as a tab-delimited file\nCSV.write(\"out.csv\", df; delim='	')\n\n# write out a DataFrame, with null values represented by the string \"NA\"\nCSV.write(\"out.csv\", df; null=\"NA\")\n\n# write out a \"header-less\" file, with actual data starting on row 1\nCSV.write(\"out.csv\", df; header=false)\n\n# write out a DataFrame `df` twice to a file, the resulting file with have twice the # of rows as the DataFrame\n# note the usage of the keyword argument `append=true` in the 2nd call\nCSV.write(\"out.csv\", df)\nCSV.write(\"out.csv\", df; append=true)\n\n# write a DataFrame out to an IOBuffer instead of a file\nio = IOBuffer\nCSV.write(io, df)\n\n# write the result of an SQLite query out to a comma-delimited file\ndb = SQLite.DB()\nsqlite_source = SQLite.Source(db, \"select * from sqlite_table\")\nCSV.write(\"sqlite_table.csv\", sqlite_source)\n\n\n\n"
},

{
    "location": "index.html#High-level-interface-1",
    "page": "Home",
    "title": "High-level interface",
    "category": "section",
    "text": "CSV.read\nCSV.write"
},

{
    "location": "index.html#CSV.Source",
    "page": "Home",
    "title": "CSV.Source",
    "category": "Type",
    "text": "A type that satisfies the Data.Source interface in the DataStreams.jl package.\n\nA CSV.Source can be manually constructed in order to be re-used multiple times.\n\nCSV.Source(file_or_io; kwargs...) => CSV.Source\n\nNote that a filename string can be provided or any IO type. For the full list of supported keyword arguments, see the docs for CSV.read or type ?CSV.read at the repl\n\nAn example of re-using a CSV.Source is:\n\n# manually construct a `CSV.Source` once, then stream its data to both a DataFrame\n# and SQLite table `sqlite_table` in the SQLite database `db`\n# note the use of `CSV.reset!` to ensure the `source` can be streamed from again\nsource = CSV.Source(file)\ndf1 = CSV.read(source, DataFrame)\nCSV.reset!(source)\nsq1 = CSV.read(source, SQLite.Sink, db, \"sqlite_table\")\n\n\n\n"
},

{
    "location": "index.html#CSV.Sink",
    "page": "Home",
    "title": "CSV.Sink",
    "category": "Type",
    "text": "A type that satisfies the Data.Sink interface in the DataStreams.jl package.\n\nA CSV.Sink can be manually constructed in order to be re-used multiple times.\n\nCSV.Sink(file_or_io; kwargs...) => CSV.Sink\n\nNote that a filename string can be provided or any IO type. For the full list of supported keyword arguments, see the docs for CSV.write or type ?CSV.write at the repl\n\nAn example of re-using a CSV.Sink is:\n\n# manually construct a `CSV.Source` once, then stream its data to both a DataFrame\n# and SQLite table `sqlite_table` in the SQLite database `db`\n# note the use of `CSV.reset!` to ensure the `source` can be streamed from again\nsource = CSV.Source(file)\ndf1 = CSV.read(source, DataFrame)\nCSV.reset!(source)\nsq1 = CSV.read(source, SQLite.Sink, db, \"sqlite_table\")\n\n\n\n"
},

{
    "location": "index.html#CSV.Options",
    "page": "Home",
    "title": "CSV.Options",
    "category": "Type",
    "text": "Represents the various configuration settings for delimited text file parsing.\n\nKeyword Arguments:\n\ndelim::Union{Char,UInt8}; how fields in the file are delimited\nquotechar::Union{Char,UInt8}; the character that indicates a quoted field that may contain the delim or newlines\nescapechar::Union{Char,UInt8}; the character that escapes a quotechar in a quoted field\nnull::String; indicates how NULL values are represented in the dataset\ndateformat::Union{AbstractString,Dates.DateFormat}; how dates/datetimes are represented in the dataset\n\n\n\n"
},

{
    "location": "index.html#CSV.parsefield",
    "page": "Home",
    "title": "CSV.parsefield",
    "category": "Function",
    "text": "CSV.parsefield{T}(io::IO, ::Type{T}, opt::CSV.Options=CSV.Options(), row=0, col=0) => Nullable{T} CSV.parsefield{T}(s::CSV.Source, ::Type{T}, row=0, col=0) => Nullable{T}`\n\nio is an IO type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell) leading whitespace is ignored for Integer and Float types. returns a Nullable{T} saying whether the field contains a null value or not (empty field, missing value) field is null if the next delimiter or newline is encountered before any other characters. Specialized methods exist for Integer, Float, String, Date, and DateTime. For other types T, a generic fallback requires parse(T, str::String) to be defined. the field value may also be wrapped in opt.quotechar; two consecutive opt.quotechar results in a null field opt.null is also checked if there is a custom value provided (i.e. \"NA\", \"\\N\", etc.) For numeric fields, if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown\n\nThe second method of CSV.parsefield operates on a CSV.Source directly allowing for easy usage when writing custom parsing routines. Do note, however, that the row and col arguments are for error-reporting purposes only. A CSV.Source maintains internal state with regards to the underlying data buffer and can only parse fields sequentially. This means that CSV.parsefield needs to be called somewhat like:\n\nsource = CSV.Source(file)\n\ntypes = Data.types(source)\n\nfor col = 1:length(types)\n    println(get(CSV.parsefield(source, types[col]), \"\"\"\"))\nend\n\n\n\n"
},

{
    "location": "index.html#CSV.readline",
    "page": "Home",
    "title": "CSV.readline",
    "category": "Function",
    "text": "CSV.readline(io::IO, q='\"', e='\\', buf::IOBuffer=IOBuffer()) => String CSV.readline(source::CSV.Source) => String\n\nread a single line from io (any IO type) or a CSV.Source as a string, accounting for potentially embedded newlines in quoted fields (e.g. value1, value2, \"value3 with   embedded newlines\"). Can optionally provide a buf::IOBuffer type for buffer reuse\n\nThis function basically mirrors Base.readline except it can account for quoted newlines to not as the true end of a line.\n\n\n\n"
},

{
    "location": "index.html#CSV.readsplitline",
    "page": "Home",
    "title": "CSV.readsplitline",
    "category": "Function",
    "text": "CSV.readsplitline(io, d=',', q='\"', e='\\', buf::IOBuffer=IOBuffer()) => Vector{String} CSV.readsplitline(source::CSV.Source) => Vector{String}\n\nread a single, delimited line from io (any IO type) or a CSV.Source as a Vector{String} delimited fields are separated by an ascii character d). Can optionally provide a buf::IOBuffer type for buffer reuse\n\n\n\n"
},

{
    "location": "index.html#CSV.countlines",
    "page": "Home",
    "title": "CSV.countlines",
    "category": "Function",
    "text": "CSV.countlines(io::IO, quotechar, escapechar) => Int CSV.countlines(source::CSV.Source) => Int\n\ncount the number of lines in a file, accounting for potentially embedded newlines in quoted fields\n\n\n\n"
},

{
    "location": "index.html#Lower-level-utilities-1",
    "page": "Home",
    "title": "Lower-level utilities",
    "category": "section",
    "text": "CSV.Source\nCSV.Sink\nCSV.Options\nCSV.parsefield\nCSV.readline\nCSV.readsplitline\nCSV.countlines"
},

]}
