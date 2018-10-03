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
    "location": "index.html#CSV.File",
    "page": "Home",
    "title": "CSV.File",
    "category": "type",
    "text": "CSV.File(source::Union{String, IO}; kwargs...) => CSV.File\n\nRead a csv input (a filename given as a String, or any other IO source), returning a CSV.File object. Opens the file and uses passed arguments to detect the number of columns and column types. The returned CSV.File object supports the Tables.jl interface and can iterate CSV.Rows. CSV.Row supports propertynames and getproperty to access individual row values. Note that duplicate column names will be detected and adjusted to ensure uniqueness (duplicate column name a will become a_1). For example, one could iterate over a csv file with column names a, b, and c by doing:\n\nfor row in CSV.File(file)\n    println(\"a=$(row.a), b=$(row.b), c=$(row.c)\")\nend\n\nBy supporting the Tables.jl interface, a CSV.File can also be a table input to any other table sink function. Like:\n\n# materialize a csv file as a DataFrame\ndf = CSV.File(file) |> DataFrame\n\n# load a csv file directly into an sqlite database table\ndb = SQLite.DB()\ntbl = CSV.File(file) |> SQLite.load!(db, \"sqlite_table\")\n\nSupported keyword arguments include:\n\nFile layout options:\nheader=1: the header argument can be an Int, indicating the row to parse for column names; or a Range, indicating a span of rows to be combined together as column names; or an entire Vector of Symbols or Strings to use as column names\nnormalizenames=false: whether column names should be \"normalized\" into valid Julia identifier symbols\ndatarow: an Int argument to specify the row where the data starts in the csv file; by default, the next row after the header row is used\nskipto::Int: similar to datarow, specifies the number of rows to skip before starting to read data\nfooterskip::Int: number of rows at the end of a file to skip parsing\nlimit: an Int to indicate a limited number of rows to parse in a csv file\ntranspose::Bool: read a csv file \"transposed\", i.e. each column is parsed as a row\ncomment: a String that occurs at the beginning of a line to signal parsing that row should be skipped\nuse_mmap::Bool=!Sys.iswindows(): whether the file should be mmapped for reading, which in some cases can be faster\nParsing options:\nmissingstrings, missingstring: either a String, or Vector{String} to use as sentinel values that will be parsed as missing; by default, only an empty field (two consecutive delimiters) is considered missing\ndelim=\',\': a Char or String that indicates how columns are delimited in a file\nignorerepeated::Bool=false: whether repeated (consecutive) delimiters should be ignored while parsing; useful for fixed-width files with delimiter padding between cells\nquotechar=\'\"\', openquotechar, closequotechar: a Char (or different start and end characters) that indicate a quoted field which may contain textual delimiters or newline characters\nescapechar=\'\\\': the Char used to escape quote characters in a text field\ndateformat::Union{String, Dates.DateFormat, Nothing}: a date format string to indicate how Date/DateTime columns are formatted in a delimited file\ndecimal: a Char indicating how decimals are separated in floats, i.e. 3.14 used \'.\', or 3,14 uses a comma \',\'\ntruestrings, falsestrings: Vectors of Strings that indicate how true or false values are represented\nColumn Type Options:\ntypes: a Vector or Dict of types to be used for column types; a Dict can map column index Int, or name Symbol or String to type for a column, i.e. Dict(1=>Float64) will set the first column as a Float64, Dict(:column1=>Float64) will set the column named column1 to Float64 and, Dict(\"column1\"=>Float64) will set the column1 to Float64\ntypemap::Dict{Type, Type}: a mapping of a type that should be replaced in every instance with another type, i.e. Dict(Float64=>String) would change every detected Float64 column to be parsed as Strings\nallowmissing=:all: indicate how missing values are allowed in columns; possible values are :all - all columns may contain missings, :auto - auto-detect columns that contain missings or, :none - no columns may contain missings\ncategorical::Union{Bool, Real}=false: if true, columns detected as String are returned as a CategoricalArray; alternatively, the proportion of unique values below which String columns should be treated as categorical (for example 0.1 for 10%)\nstrict::Bool=false: whether invalid values should throw a parsing error or be replaced with missing values\n\n\n\n\n\n"
},

{
    "location": "index.html#CSV.validate",
    "page": "Home",
    "title": "CSV.validate",
    "category": "function",
    "text": "CSV.validate(fullpath::Union{AbstractString,IO}, sink::Type{T}=DataFrame, args...; kwargs...) => typeof(sink)\n\nCSV.validate(fullpath::Union{AbstractString,IO}, sink::Data.Sink; kwargs...) => Data.Sink\n\nTakes the same positional & keyword arguments as CSV.read, but provides detailed information as to why reading a csv file failed. Useful for cases where reading fails and it\'s not clear whether it\'s due to a row having too many columns, or wrong types, or what have you.\n\n\n\n\n\n"
},

{
    "location": "index.html#CSV.write",
    "page": "Home",
    "title": "CSV.write",
    "category": "function",
    "text": "CSV.write(file::Union{String, IO}, file; kwargs...) => file\ntable |> CSV.write(file::Union{String, IO}; kwargs...) => file\n\nWrite a Tables.jl interface input to a csv file, given as an IO argument or String representing the file name to write to.\n\nKeyword arguments include:\n\ndelim::Union{Char, String}=\',\': a character or string to print out as the file\'s delimiter\nquotechar::Char=\'\"\': character to use for quoting text fields that may contain delimiters or newlines\nopenquotechar::Char: instead of quotechar, use openquotechar and closequotechar to support different starting and ending quote characters\nescapechar::Char=\'\\\': character used to escape quote characters in a text field\nmissingstring::String=\"\": string to print \ndateformat=Dates.default_format(T): the date format string to use for printing out Date & DateTime columns\nappend=false: whether to append writing to an existing file/IO, if true, it will not write column names by default\nwriteheader=!append: whether to write an initial row of delimited column names, not written by default if appending\nheader: pass a list of column names (Symbols or Strings) to use instead of the column names of the input table\n\n\n\n\n\n"
},

{
    "location": "index.html#High-level-interface-1",
    "page": "Home",
    "title": "High-level interface",
    "category": "section",
    "text": "CSV.File\nCSV.validate\nCSV.write"
},

]}
