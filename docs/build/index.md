
<a id='CSV.jl-Documentation-1'></a>

# CSV.jl Documentation


<a id='High-level-interface-1'></a>

## High-level interface

<a id='CSV.read' href='#CSV.read'>#</a>
**`CSV.read`** &mdash; *Function*.



parses a delimited file into a Julia structure (a DataFrame by default, but any `Data.Sink` may be given).

  * `fullpath`; can be a file name (string) or other `IO` instance
  * `sink`; a `DataFrame` by default, but may also be other `Data.Sink` types that support the `AbstractTable` interface
  * `delim::Union{Char,UInt8}`; how fields in the file are delimited
  * `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines
  * `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field
  * `null::String`; an ascii string that indicates how NULL values are represented in the dataset
  * `header`; column names can be provided manually as a complete Vector{String}, or as an Int/Range which indicates the row/rows that contain the column names
  * `datarow::Int`; specifies the row on which the actual data starts in the file; by default, the data is expected on the next row after the header row(s)
  * `types`; column types can be provided manually as a complete Vector{DataType}, or in a Dict to reference a column by name or number
  * `dateformat::Union{AbstractString,Dates.DateFormat}`; how all dates/datetimes are represented in the dataset
  * `footerskip::Int`; indicates the number of rows to skip at the end of the file
  * `rows_for_type_detect::Int=100`; indicates how many rows should be read to infer the types of columns
  * `rows::Int`; indicates the total number of rows to read from the file; by default the file is pre-parsed to count the # of rows
  * `use_mmap::Bool=true`; whether the underlying file will be mmapped or not while parsing

Note by default, "string" or text columns will be parsed as the `WeakRefString` type. This is a custom type that only stores a pointer to the actual byte data + the number of bytes. To convert a `String` to a standard Julia string type, just call `string(::String)`, this also works on an entire column `string(::NullableVector{String})`. Oftentimes, however, it can be convenient to work with `WeakRefStrings` depending on the ultimate use, such as transfering the data directly to another system and avoiding all the intermediate byte copying.

Example usage:

```
julia> dt = CSV.read("bids.csv")
7656334×9 DataFrames.DataFrame
│ Row     │ bid_id  │ bidder_id                               │ auction │ merchandise      │ device      │
├─────────┼─────────┼─────────────────────────────────────────┼─────────┼──────────────────┼─────────────┤
│ 1       │ 0       │ "8dac2b259fd1c6d1120e519fb1ac14fbqvax8" │ "ewmzr" │ "jewelry"        │ "phone0"    │
│ 2       │ 1       │ "668d393e858e8126275433046bbd35c6tywop" │ "aeqok" │ "furniture"      │ "phone1"    │
│ 3       │ 2       │ "aa5f360084278b35d746fa6af3a7a1a5ra3xe" │ "wa00e" │ "home goods"     │ "phone2"    │
...
```


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L218-251' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">read</span><span class="p">(</span><span class="n">fullpath</span><span class="p">::</span><span class="n">Union{AbstractString,IO}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L266">src/Source.jl:266</a>
</li>
<li>
    <pre><span class="nf">read</span><span class="p">(</span>
    <span class="n">fullpath</span><span class="p">::</span><span class="n">Union{AbstractString,IO}</span><span class="p">,
</span>    <span class="n">sink</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L266">src/Source.jl:266</a>
</li>
</ul>

<a id='CSV.write' href='#CSV.write'>#</a>
**`CSV.write`** &mdash; *Function*.



write a `source::Data.Source` out to a `CSV.Sink`

  * `io::Union{String,IO}`; a filename (String) or `IO` type to write the `source` to
  * `source`; a `Data.Source` type
  * `delim::Union{Char,UInt8}`; how fields in the file will be delimited
  * `quotechar::Union{Char,UInt8}`; the character that indicates a quoted field that may contain the `delim` or newlines
  * `escapechar::Union{Char,UInt8}`; the character that escapes a `quotechar` in a quoted field
  * `null::String`; the ascii string that indicates how NULL values will be represented in the dataset
  * `dateformat`; how dates/datetimes will be represented in the dataset
  * `quotefields::Bool`; whether all fields should be quoted or not
  * `header::Bool`; whether to write out the column names from `source`
  * `append::Bool`; start writing data at the end of `io`; by default, `io` will be reset to its beginning before writing


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L98-111' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre><span class="nf">write</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Union{AbstractString,IO}</span><span class="p">,
</span>    <span class="n">source</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L122">src/Sink.jl:122</a>
</li>
</ul>

<a id='CSV.Options' href='#CSV.Options'>#</a>
**`CSV.Options`** &mdash; *Type*.



Represents the various configuration settings for csv file parsing.

  * `delim`::Union{Char,UInt8} = how fields in the file are delimited
  * `quotechar`::Union{Char,UInt8} = the character that indicates a quoted field that may contain the `delim` or newlines
  * `escapechar`::Union{Char,UInt8} = the character that escapes a `quotechar` in a quoted field
  * `null`::String = indicates how NULL values are represented in the dataset
  * `dateformat`::Union{AbstractString,Dates.DateFormat} = how dates/datetimes are represented in the dataset


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L51-59' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">Options</span><span class="p">(</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L72">src/CSV.jl:72</a>
</li>
<li>
    <pre><span class="nf">Options</span><span class="p">(</span>
    <span class="n">delim</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">quotechar</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">escapechar</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">separator</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">decimal</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">null</span><span class="p">::</span><span class="n">String</span><span class="p">,
</span>    <span class="n">nullcheck</span><span class="p">::</span><span class="n">Bool</span><span class="p">,
</span>    <span class="n">dateformat</span><span class="p">::</span><span class="n">Base.Dates.DateFormat</span><span class="p">,
</span>    <span class="n">datecheck</span><span class="p">::</span><span class="n">Bool</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L61">src/CSV.jl:61</a>
</li>
<li>
    <pre><span class="nf">Options</span><span class="p">(</span>
    <span class="n">delim</span><span class="p">,
</span>    <span class="n">quotechar</span><span class="p">,
</span>    <span class="n">escapechar</span><span class="p">,
</span>    <span class="n">separator</span><span class="p">,
</span>    <span class="n">decimal</span><span class="p">,
</span>    <span class="n">null</span><span class="p">,
</span>    <span class="n">nullcheck</span><span class="p">,
</span>    <span class="n">dateformat</span><span class="p">,
</span>    <span class="n">datecheck</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L61">src/CSV.jl:61</a>
</li>
</ul>

_Hiding 1 method defined outside of this package._


<a id='Lower-level-utilities-1'></a>

## Lower-level utilities

<a id='CSV.Source' href='#CSV.Source'>#</a>
**`CSV.Source`** &mdash; *Type*.



constructs a `CSV.Source` file ready to start parsing data from

implements the `Data.Source` interface for providing convenient `Data.stream!` methods for various `Data.Sink` types


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L84-88' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">Source</span><span class="p">(</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L59">src/Source.jl:59</a>
</li>
<li>
    <pre><span class="nf">Source</span><span class="p">{</span><span class="n">I<:IO</span><span class="p">}</span><span class="p">(</span>
    <span class="n">schema</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">,
</span>    <span class="n">options</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">data</span><span class="p">::</span><span class="n">I</span><span class="p">,
</span>    <span class="n">datapos</span><span class="p">::</span><span class="n">Int64</span><span class="p">,
</span>    <span class="n">fullpath</span><span class="p">::</span><span class="n">String</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L90">src/CSV.jl:90</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Source</span><span class="p">(</span><span class="n">fullpath</span><span class="p">::</span><span class="n">Union{AbstractString,IO}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L28">src/Source.jl:28</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Source</span><span class="p">{</span><span class="n">I</span><span class="p">}</span><span class="p">(</span><span class="n">s</span><span class="p">::</span><span class="n">CSV.Sink{I}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Source.jl#L176">src/Source.jl:176</a>
</li>
</ul>

_Hiding 1 method defined outside of this package._

<a id='CSV.Sink' href='#CSV.Sink'>#</a>
**`CSV.Sink`** &mdash; *Type*.



constructs a `CSV.Sink` file ready to start writing data to

implements the `Data.Sink` interface for providing convenient `Data.stream!` methods for various `Data.Source` types


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L103-107' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre><span class="nf">Sink</span><span class="p">{</span><span class="n">I<:IO</span><span class="p">}</span><span class="p">(</span>
    <span class="n">schema</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">,
</span>    <span class="n">options</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">data</span><span class="p">::</span><span class="n">I</span><span class="p">,
</span>    <span class="n">datapos</span><span class="p">::</span><span class="n">Int64</span><span class="p">,
</span>    <span class="n">quotefields</span><span class="p">::</span><span class="n">Bool</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/CSV.jl#L109">src/CSV.jl:109</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Sink</span><span class="p">(</span><span class="n">s</span><span class="p">::</span><span class="n">CSV.Source</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L8">src/Sink.jl:8</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Sink</span><span class="p">(</span><span class="n">s</span><span class="p">::</span><span class="n">CSV.Source</span><span class="p">, </span><span class="n">io</span><span class="p">::</span><span class="n">IOStream</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L17">src/Sink.jl:17</a>
</li>
<li>
    <pre><span class="nf">Sink</span><span class="p">(</span>
    <span class="n">s</span><span class="p">::</span><span class="n">CSV.Source</span><span class="p">,
</span>    <span class="n">file</span><span class="p">::</span><span class="n">AbstractString</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L21">src/Sink.jl:21</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Sink</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">Union{AbstractString,IO}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/Sink.jl#L34">src/Sink.jl:34</a>
</li>
</ul>

_Hiding 1 method defined outside of this package._

<a id='CSV.parsefield' href='#CSV.parsefield'>#</a>
**`CSV.parsefield`** &mdash; *Function*.



`io` is an `IO` type that is positioned at the first byte/character of an delimited-file field (i.e. a single cell) leading whitespace is ignored for Integer and Float types. returns a `Tuple{T,Bool}` with a value & bool saying whether the field contains a null value or not Specialized methods exist for Integer, Float, String, Date, and DateTime. For other types `T`, a generic fallback requires `zero(T)` and `parse(T, str::String)` to be defined. field is null if the next delimiter or newline is encountered before any other characters. the field value may also be wrapped in `opt.quotechar`; two consecutive `opt.quotechar` results in a null field `opt.null` is also checked if there is a custom value provided (i.e. "NA", "\N", etc.) For numeric fields, if field is non-null and non-digit characters are encountered at any point before a delimiter or newline, an error is thrown


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L55-65' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L99">src/parsefields.jl:99</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L99">src/parsefields.jl:99</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L99">src/parsefields.jl:99</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L99">src/parsefields.jl:99</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L149">src/parsefields.jl:149</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L149">src/parsefields.jl:149</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L149">src/parsefields.jl:149</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L149">src/parsefields.jl:149</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:Integer</span><span class="p">}</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L69">src/parsefields.jl:69</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:Integer</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L69">src/parsefields.jl:69</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:Integer</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L69">src/parsefields.jl:69</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:Integer</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L69">src/parsefields.jl:69</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L131">src/parsefields.jl:131</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L131">src/parsefields.jl:131</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L131">src/parsefields.jl:131</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractFloat</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L131">src/parsefields.jl:131</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L183">src/parsefields.jl:183</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L183">src/parsefields.jl:183</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L183">src/parsefields.jl:183</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T<:AbstractString</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L183">src/parsefields.jl:183</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{Date}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L221">src/parsefields.jl:221</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{Date}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L221">src/parsefields.jl:221</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{Date}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L221">src/parsefields.jl:221</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{Date}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L221">src/parsefields.jl:221</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{DateTime}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L269">src/parsefields.jl:269</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{DateTime}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L269">src/parsefields.jl:269</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{DateTime}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L269">src/parsefields.jl:269</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{DateTime}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L269">src/parsefields.jl:269</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">parsefield</span><span class="p">{</span><span class="n">T</span><span class="p">}</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L338">src/parsefields.jl:338</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L338">src/parsefields.jl:338</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L338">src/parsefields.jl:338</a>
</li>
<li>
    <pre><span class="nf">parsefield</span><span class="p">{</span><span class="n">T</span><span class="p">}</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span><span class="p">,
</span>    <span class="n">opt</span><span class="p">::</span><span class="n">CSV.Options</span><span class="p">,
</span>    <span class="n">row</span><span class="p">,
</span>    <span class="n">col</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/parsefields.jl#L338">src/parsefields.jl:338</a>
</li>
</ul>

<a id='CSV.readline-Tuple{CSV.Source}' href='#CSV.readline-Tuple{CSV.Source}'>#</a>
**`CSV.readline`** &mdash; *Method*.



read a single line from `io` (any `IO` type) as a string, accounting for potentially embedded newlines in quoted fields (e.g. value1, value2, "value3 with   embedded newlines"). Can optionally provide a `buf::IOBuffer` type for buffer resuse


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L1-3' class='documenter-source'>source</a><br>

<a id='CSV.readsplitline' href='#CSV.readsplitline'>#</a>
**`CSV.readsplitline`** &mdash; *Function*.



read a single line from `io` (any `IO` type) as a `Vector{String}` with elements being delimited fields. Can optionally provide a `buf::IOBuffer` type for buffer resuse


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L29' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">readsplitline</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L31">src/io.jl:31</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">readsplitline</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">, </span><span class="n">d</span><span class="p">::</span><span class="n">UInt8</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L31">src/io.jl:31</a>
</li>
<li>
    <pre><span class="nf">readsplitline</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n">d</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">q</span><span class="p">::</span><span class="n">UInt8</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L31">src/io.jl:31</a>
</li>
<li>
    <pre><span class="nf">readsplitline</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n">d</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">q</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">e</span><span class="p">::</span><span class="n">UInt8</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L31">src/io.jl:31</a>
</li>
<li>
    <pre><span class="nf">readsplitline</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">IO</span><span class="p">,
</span>    <span class="n">d</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">q</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">e</span><span class="p">::</span><span class="n">UInt8</span><span class="p">,
</span>    <span class="n">buf</span><span class="p">::</span><span class="n">Base.AbstractIOBuffer{Array{UInt8,1}}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L31">src/io.jl:31</a>
</li>
</ul>

<a id='CSV.countlines-Tuple{CSV.Source}' href='#CSV.countlines-Tuple{CSV.Source}'>#</a>
**`CSV.countlines`** &mdash; *Method*.



count the number of lines in a file, accounting for potentially embedded newlines in quoted fields


<a target='_blank' href='https://github.com/JuliaDB/CSV.jl/tree/14562f6574423ca8e283c3bfdd5178a7734f338a/src/io.jl#L60' class='documenter-source'>source</a><br>

