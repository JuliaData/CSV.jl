# Parsers.jl downstream usage map (for the 3.0 breaking release)

Date: 2026-08-18. Source of truth: local General registry (`~/.julia/registries/General`, Parsers UUID `69de0a69-1ddd-5017-9359-2bf0b02dc9f0`) plus a `git clone --depth 1` of every dependent's default branch into `scratchpad/parsers-deps/<name>` and a grep of `src/` (and `ext/` where present). RhsJTool's GitHub repo is deleted; its 0.0.3 tarball was pulled from pkg.julialang.org instead. Parsers 2.8.7 is the latest release; local checkout at `~/.julia/dev/Parsers` (2026-06-02) was used to check which signatures dependents are actually hitting.

## 0. Headline numbers

- **40** packages in General list Parsers in `Deps.toml` (only InlineStrings also lists it as a weak dep; no other weak/extension deps).
- **6** of those no longer depend on Parsers in their latest release (registry range is historical only): FHIRClient, JutulDarcy, Qwind, RankCompV3, Tapestree, ReinforcementLearningCore. Nothing to migrate.
- **6** declare the dep in their current release but never call it (dead `using Parsers` or Project-only): BraketAHS, Diagonalizations, Firebird, NanoDates (a WIP `internal.jl` that is never `include`d), PowerDynData, RhsJTool. Only a compat bump needed (or drop the dep).
- **28** packages actually call into Parsers. Of these:
  - **15 (54%)** use only scalar `Parsers.parse`/`Parsers.tryparse` (11 string-only, 4 also using the byte-span/positional form or `dateformat`): Batsrus, Vlasiator, NISTStatisticalReferenceData, MortalityTables, Bonsai, KiteUtils, NuclearToolkit, GraphQLParser, XmlStructLoader, CSVReader, FWFTables, GeoEnergyIO, PSID, DLMReader, DeIdentification.
  - **7 (25%)** use the delimiter/quote/sentinel field machinery (`xparse` + `Options(delim/quoted/sentinel/...)` + result-code predicates + `PosLen`/`getstring`): CSV, InlineStrings (ext), ChunkedCSV, ARFFFiles, PowerFlowData, OSMToolset, Schemata.
  - **4 (14%)** reach into the typeparser/extension internals without the delim machinery (`typeparser`, `xparse2`, `supportedtype`, `AbstractConf`, `conf/returntype/result`, `scale/noscale/parsedigits`, `Parsers.OPTIONS`): FixedPointDecimals, JSON3, MySQL, JSON. (ChunkedCSV, counted above, does both.)
  - **2 (7%)** other: WeakRefStrings (imports the `PosLen` type only), JSON2 (`Parsers.parse(T, io::IO)` + `readbyte/peekbyte`).
- Jacob-maintained among the 28: CSV, WeakRefStrings, InlineStrings, JSON, JSON2, JSON3, MySQL (7). Third-party: 21.
- **Third-party packages the 3.0 removal of field-parsing machinery genuinely breaks (5, +1 dead):** ChunkedCSV, ARFFFiles, PowerFlowData, OSMToolset, FixedPointDecimals (extension internals, not delim machinery), and Schemata (unmaintained since 2021). Everything else third-party is trivial or a one-line shim.

## 1. Full table

Owner: "Jacob" = JuliaData/quinnj/JuliaIO-JSON/JuliaDatabases-MySQL/JuliaStrings-InlineStrings (packages Jacob authors or maintains); "3rd" = third party. "Last commit" = default-branch HEAD date of the shallow clone. Difficulty: trivial = `parse`/`tryparse` only; easy = a small shim/API rename; hard = relies on delim/quote/sentinel field machinery or typeparser internals; none = no code change (compat bump only).

| Package | Owner | Last commit | Usage classes | Specific API surface used | Difficulty | Notes |
|---|---|---|---|---|---|---|
| ARFFFiles | 3rd (cjdoris) | 2026-08-16 | b, c, d, f | `Parsers.Options(sentinel=["?"], openquotechar, closequotechar, escapechar='\\', delim=',', quoted=true, comment="%", ignoreemptylines=true, dateformat=df)`; `Parsers.xparse(T, data, pos, len, opts)` for T in {Float64, String, DateTime}; `res.code/.tlen/.val.pos/.val.len`; `Parsers.invalid/quoted/escapedstring/newline/eof/delimited/sentinel`; `Parsers.getstring(data, Parsers.PosLen(pos,len), 0x00)`; reads `opts.oq` | **hard** | A full ARFF tokenizer built on Parsers' field machinery (tries single- then double-quote Options per datum). Dependent: OpenML.jl. Actively maintained. |
| Batsrus | 3rd | 2026-07-28 | a | `Parsers.parse(Int32/Int64/Float32/Float64, ::SubString)`, broadcast `Parsers.parse.(Float32, split(...))` | trivial | Note Int32/Float32 targets and SubString inputs. |
| Bonsai | 3rd | 2024-01-09 | a | `Parsers.parse(Float64, x)` | trivial | |
| BraketAHS | 3rd | 2024-09-20 | – | none in src (Project.toml only) | none | Drop dep. |
| CSV | Jacob | 2026-08-05 | b, c, d, f, g | `Parsers.Options(...)` 19-arg positional internal ctor; `xparse` (24 sites); `ok/newline/delimited/sentinel/invalid/escapedstring/invalidquotedfield/codes`; `getstring`; `PosLen`, `Parsers.MISSING_BIT`, `Parsers.ReturnCode`, `Parsers.Result`; `Parsers.Format` (dateformat type + `timetype`); `Parsers.checkdelim!`; `Parsers.parse(T, str)`; `Parsers.OPTIONS`; `Parsers.memcmp` | hard (owned) | The primary consumer; CSV 3.0 kernel rewrite is separately planned. 759 direct dependents. |
| CSVReader | 3rd (tk3369) | 2021-04-14 | a | `using Parsers: tryparse`; `tryparse(Int/Float64, s)` -> `nothing` on failure | trivial | Unmaintained. |
| ChunkedCSV | 3rd (RelationalAI) | 2026-01-05 | b, c, d, e, f, g | `Parsers.Options(sentinel, wh1, wh2, openquotechar, closequotechar, escapechar, delim, quoted, stripwhitespace, trues, falses, groupmark, dateformat, ignorerepeated, decimal, ignoreemptylines, rounding)`; `xparse(T, bytes, pos, len, options[, Parsers.PosLen31])`; `Parsers.Result{T}` (constructed & asserted); `Parsers.PosLen`, `PosLen31`; code consts `INVALID/EOF/OK/OVERFLOW/INEXACT`; `eof/ok/sentinel/newline/delimited/invalid`; `checkdelim!`; `getstring(bytes, pl, escapechar)`; extension protocol for `GuessDateTime`: `Parsers.supportedtype`, `Parsers.returntype`, `Parsers.default_format`, `Parsers.typeparser(::Parsers.AbstractConf{GuessDateTime}, src, pos, len, b, code, pl, opts)`; `Parsers.tryparsenext(Dates.DatePart{'z'/'Z'}(…), buf, pos, len, b, code)` for timezones | **hard** | Essentially a second CSV reader on top of Parsers 2 internals + FixedPointDecimals. 0 registered dependents (used internally at RAI). Would have to pin `Parsers = "2"` or vendor. |
| DLMReader | 3rd (sl-solution) | 2025-10-15 | b (byte-span only) | Windows-only branch: `Parsers.xparse(T<:Real, bytes::Vector{UInt8}, lo, hi)` and tests `val.code == 33` (OK|EOF); otherwise uses `jl_try_substrtod` ccall | easy | Replace with `tryparse(T, bytes, lo, hi)`; the code==33 check just means "whole span consumed and valid". |
| DeIdentification | 3rd (bcbi) | 2024-12-02 | a (+ dateformat) | `Parsers.tryparse(DateTime/Date, str, Parsers.Options(dateformat=df))` | easy | Only needs a `dateformat` pass-through on `tryparse` (kwarg or positional DateFormat). |
| Diagonalizations | 3rd | 2026-03-12 | – | none in src | none | Drop dep. |
| FHIRClient | 3rd | 2024-05-11 | – | stale: Parsers only in 0.3–0.x; latest 2.3.0 has no dep | none | |
| FWFTables | 3rd | 2024-11-07 | a | `Parsers.tryparse(Int64/Float64, value)` -> `something(…, NaN)` | trivial | |
| Firebird | 3rd | 2026-07-09 | – | `using Parsers` only; zero call sites | none | Drop dep. |
| FixedPointDecimals | 3rd (JuliaMath / RAI maintainers) | 2026-03-17 | e, g, c, f | Deep extension: `struct FixedDecimalConf{T} <: Parsers.AbstractConf{T}`; overloads `Parsers.conf`, `Parsers.returntype`, `Parsers.result`, `Parsers.supportedtype`, `Parsers.scale`, `Parsers.noscale`, `Parsers.typeparser(::FixedDecimalConf, src, pos, len, b, code, pl, opts)`; calls `Parsers.parsedigits(conf, …)`, `Parsers.incr!`, `Parsers.eof(src,pos,len)`, `Parsers.peekbyte`, `Parsers.PosLen`, `Parsers.FLOAT64`/`FloatType`; code consts `INEXACT/INVALID/OVERFLOW/EOF`; `Parsers.Options(rounding=RoundNearest/RoundToZero/nothing)`, reads `options.decimal`, `options.rounding`; `Parsers.xparse2(FD, bytes, 1, len, opts)`; predicates `eof/ok/inexact/overflow/invalid` | **hard** (internals, not delim) | Implements `Base.parse/tryparse(FD)` by hooking Parsers' float digit accumulator with a custom scale step. Dependents: DuckDB, Currencies, ChunkedCSV, … (6). The single serious customer of an "extension seam". |
| GeoEnergyIO | 3rd (sintefmath) | 2026-08-06 | a, f | `Parsers.parse(Int/T, s)`, `Parsers.tryparse(T/Float64, s)`; `opts = Parsers.Options()`; positional byte-span `Parsers.parse(Int, el, opts, 1, stop)` and `Parsers.tryparse(T, el, opts, pos)` | easy | Only needs the byte-span form; `Options()` is a placeholder. Dependent: JutulDarcy. |
| GraphQLParser | 3rd | 2022-10-24 | a | `Parsers.parse(Int64/Float64, @view codeunits(str)[a:b])` (a `SubArray{UInt8}` of `CodeUnits`) | trivial | Wants `parse(T, ::AbstractVector{UInt8})` incl. views; would be happier with `parse(T, bytes, start, stop)`. |
| InlineStrings | Jacob | 2025-09-29 | d, c, g (in `ext/ParsersExt.jl`) | Overloads `Parsers.xparse(::Type{<:InlineString}, src, pos, len, options, S)`: calls `Parsers.xparse(String, …, PosLen)`, `Parsers.valueok/sentinel/escapedstring`, `Parsers.fastseek!`, `Parsers.peekbyte`, `Parsers.incr!`, reads `options.e`, ORs `Parsers.OVERFLOW`, builds `Parsers.Result{S}(code, tlen, x)`; tests use `xparse`, `Options()`, `overflow`, `ok/delimited/newline` | hard (owned) | Exists so CSV can parse straight into InlineStrings; disappears if CSV 3.0 owns string materialization. |
| JSON | Jacob | 2026-08-11 | e/g, c | `Parsers.xparse2(Float64/BigFloat, buf, startpos, len)` (prefix parse, len = end of buffer) using `res.val/.code/.tlen`; `Parsers.specialvalue(code)`, `Parsers.invalid(code)` | easy (owned) | Needs a delimiter-free "parse a number prefix, tell me how many bytes" primitive plus overflow/special detection. 849 direct dependents: the compat bump matters. |
| JSON2 | Jacob (deprecated) | 2025-10-03 | g, a | `Parsers.parse(T<:Integer/AbstractFloat, io::IO)`; `import Parsers: readbyte, peekbyte`; `Parsers.parse(T, ::String)` | easy/ignore | Deprecated package; pin to 2. |
| JSON3 | Jacob | 2025-10-09 | e | Legacy 7-arg `Parsers.typeparser(Float64/Int64/numbertype(T), buf, pos, len, b, Int16(0), Parsers.OPTIONS)` -> `(x, code, pos)`, checks `code > 0`; `Parsers.neededdigits(T)` | easy (owned) | Same prefix-parse need as JSON. 395 direct dependents. |
| JutulDarcy | 3rd | 2026-08-09 | – | stale: dep only in 0.2.8–0.2.16; latest 0.3.9 uses GeoEnergyIO instead | none | |
| KiteUtils | 3rd | 2026-08-14 | a | `Parsers.parse.(Float32, split(...))` | trivial | |
| MortalityTables | 3rd (JuliaActuary) | 2026-05-14 | a | `Parsers.parse(Int/Float64/t, ::AbstractString)` (`parsemaybe(t,x)` generic) | trivial | |
| MySQL | Jacob | 2026-08-01 | e, f | Legacy 7-arg `Parsers.typeparser(T, buf, 1, len, buf[1], Int16(0), Parsers.OPTIONS)` on an `unsafe_wrap`ped buffer, checks `code > 0`; `Parsers.Options(dateformat=dateformat"yyyy-mm-dd HH:MM:SS.s")` and `typeparser(DateTime, …, DATETIME_OPTIONS)`; `typeparser(Int, buf, i+1, len, …)` | easy (owned) | Needs `tryparse(T, bytes, pos, len)` and a `dateformat` option. |
| NISTStatisticalReferenceData | 3rd (joshday) | 2022-01-16 | a | `Parsers.parse(Int/Float64, ::SubString)` | trivial | |
| NanoDates | 3rd (JuliaTime) | 2026-08-18 | – (aborted e) | `using Parsers` in module; `src/internal.jl` (never included) sketches a `typeparser(::Type{NanoDate}, source, pos, len, b, code, options)` overload using `getstring/PosLen/INVALID_TOKEN/EOF/peekbyte/eof/Delim/invalidtoken` | none (drop dep) | Compat already says `Parsers = "2 - 3"`. Evidence someone wanted a custom-date extension seam and gave up. |
| NuclearToolkit | 3rd | 2026-05-03 | a | `Parsers.parse(Float64, tl)` | trivial | |
| OSMToolset | 3rd (pszufe) | 2024-07-24 | b, c | `Parsers.xparse(Int, line; pos=ix+1, openquotechar='"', closequotechar='"').val`; `Parsers.xparse(String, …).val` -> `PosLen`; `Parsers.getstring(line, poslen, UInt8(0))` | easy | Uses quote-aware xparse to read `id="123"` XML attributes. Replace with find-closing-quote + `parse(Int, line, a, b)`. |
| PSID | 3rd | 2025-12-17 | a (byte-span) | `const PARSER_OPTS = Parsers.OPTIONS`; `Parsers.parse(Float64, line, PARSER_OPTS, first(r), last(r))` (fixed-width columns) | easy | Exactly the `parse(T, str, start, stop)` form Jacob wants public. |
| PowerDynData | 3rd | 2026-01-04 | – | `using Parsers: Parsers`; zero call sites | none | Drop dep. |
| PowerFlowData | 3rd (nickrobinson251) | 2025-11-18 | b, c, f, g | `Parsers.Options(sentinel=missing, quoted=true, openquotechar='\'', closequotechar='\'', stripquoted=true, delim=','/' ', ignorerepeated=true, wh1=0x00)`; `using Parsers: xparse, checkdelim!, codes, eof, invalid, invaliddelimiter, newline, valueok, peekbyte`; `xparse(T, bytes, pos, len, options)` with `res.val/.tlen/.code`; `newline(code)` drives record layout (T2 vs T3 rows) | **hard** | A PSS/E `.raw` reader written entirely against Parsers' field machinery. Dependents: PowerSystems.jl (NREL Sienna), PowerFlowFileParser. |
| Qwind | 3rd | 2023-02-14 | – | stale (dep only in 0.x; latest 3.1.1) | none | |
| RankCompV3 | 3rd | 2024-08-31 | – | stale (dep only in 0.1.1; latest 0.1.8) | none | |
| ReinforcementLearningCore | 3rd | 2025-01-13 | – | stale (dep in 0.9–0.14; latest 0.15.5) | none | |
| RhsJTool | 3rd | (repo deleted) | – | `using Parsers` but calls `Base.parse(Float64, …)` | none | Unfetchable repo; dead. |
| Schemata | 3rd (JockLawrie) | 2021-09-15 | b, c, f | `Parsers.Options()` / `Parsers.Options(kwargs...)` from user-supplied schema kwargs; `Parsers.xparse(returntype, codeunits(val) or io, 1, len, opts)`; `Parsers.ok(res.code) ? res.val : missing` | hard-ish, but dead | Generic "parse column value with user Options". Unmaintained since 2021; dependent SpineBasedRecordLinkage. |
| Tapestree | 3rd | 2026-07-01 | – | stale (dep in 0.3–0.4.2; latest 0.4.5) | none | |
| Vlasiator | 3rd | 2026-07-01 | a | `Parsers.parse(Int, ::String)` (XML attribute values) | trivial | |
| WeakRefStrings | Jacob | 2026-04-23 | g | `using Parsers: PosLen` (PosLenString/PosLenStringVector are built on it) | easy (owned) | `PosLen` must survive somewhere (Parsers 3, or move the definition into WeakRefStrings). |
| XmlStructLoader | 3rd | 2026-08-05 | a | `Parsers.parse(T, content_string)` for `T<:Number` (arbitrary Number subtypes; relies on Parsers' Base.tryparse fallback for unsupported T) | trivial | Keep the "unsupported T falls back to `Base.parse`" behavior or document it. |

Usage-class legend: (a) plain `parse`/`tryparse`; (b) `xparse` byte-span with `Options`; (c) result-code inspection / `PosLen` / `getstring`; (d) `Format`/dateformat; (e) extension of Parsers via `typeparser`/`supportedtype`/etc.; (f) `Options()` construction; (g) other.

## 2. Which `T`s people parse

Across the plain-parse group: `Int` (dominant), `Int32`, `Int64`, `Float32`, `Float64`, `Date`, `DateTime` (with a custom `dateformat`), and generic `T<:Number` (XmlStructLoader, MortalityTables `parsemaybe`, GeoEnergyIO `T`). No third-party scalar use of `Bool`, `Time`, `UUID`, `Char`, `Symbol` was found (CSV/ChunkedCSV do Bool/Time via xparse). Inputs are `String`, `SubString` (very common: results of `split`/`eachsplit`), `Vector{UInt8}`, and a `SubArray` view of `CodeUnits` (GraphQLParser). Whitespace-stripping is relied on implicitly (values come from `split` so usually clean; PSID fixed-width columns contain padding spaces and rely on `Parsers.parse` skipping them).

## 3. The delimiter/quote/sentinel machinery: exactly who breaks and what they'd need

Third-party, alive:

1. **ChunkedCSV** (RelationalAI, 2026-01). Whole product is built on Parsers 2's `xparse` + full `Options` (every field incl. `wh1/wh2`, `groupmark`, `rounding`, `ignoreemptylines`), `PosLen31`, `Result{T}`, code constants, `checkdelim!`, `getstring`, plus the `AbstractConf`/`typeparser`/`supportedtype`/`returntype`/`default_format`/`tryparsenext` extension protocol for its `GuessDateTime`. Migration = rewrite or pin to `Parsers = "2"`. Realistically: they pin. Note it also depends on FixedPointDecimals' Parsers hooks.
2. **ARFFFiles** (cjdoris, active, used by OpenML.jl). Needs: `Options(sentinel, openquotechar, closequotechar, escapechar, delim, quoted, comment, ignoreemptylines, dateformat)`, `xparse` returning `(code, tlen, val::PosLen)`, and predicates `invalid/quoted/escapedstring/newline/eof/delimited/sentinel`, `getstring`. Migration = hand-write a small ARFF tokenizer (~100 lines) and call `Parsers.parse(T, bytes, start, stop)` on the token spans, or pin to 2.
3. **PowerFlowData** (nickrobinson251, used by PowerSystems.jl). Needs `Options(sentinel, quoted, openquotechar, closequotechar, stripquoted, delim, ignorerepeated, wh1)`, `xparse`, `checkdelim!`, `newline/eof/invalid/invaliddelimiter/valueok/codes`, `peekbyte`. Same story: tokenizer + span parse, or pin. The `newline(code)`-driven "is this a T2 or T3 record" logic is the fiddly bit.
4. **OSMToolset** (pszufe). Two-line shim: it uses `xparse(...; openquotechar='"', closequotechar='"')` only to read a quoted XML attribute; `findnext('"', …)` + `parse(Int, line, a, b)` replaces it.
5. **Schemata** (dead since 2021). Generic `xparse(T, bytes, 1, len, Options(kwargs...))` + `ok`. Would break, but nobody will fix it; its one dependent (SpineBasedRecordLinkage) is equally dormant.

Third-party via extension internals (not delim machinery, but equally broken by a rewrite):

6. **FixedPointDecimals** (JuliaMath; DuckDB.jl depends on it). Overloads `conf/returntype/result/supportedtype/scale/noscale/typeparser(::AbstractConf)`, calls `parsedigits`, `incr!/eof/peekbyte`, `PosLen`, `xparse2`, and uses `Options(rounding=…)`/`options.decimal`. This is the one place a documented extension seam has a real, non-Jacob customer. Options for 3.0: (i) provide a public "decimal digit accumulator" hook (`parsedigits`-style: give me digits + exponent + sign, I produce the value) so FPD can keep a ~150-line integration; (ii) drop it and FPD vendors Parsers 2's float digit loop (they already vendored the BigInt buffer code from Parsers PR #195, so precedent exists); (iii) FPD pins to 2. Note that ChunkedCSV depends on FPD's Parsers hooks too.

Jacob-owned that break: CSV (by design), InlineStrings ext (only exists for CSV), WeakRefStrings (`PosLen` type import only), JSON (`xparse2` prefix float parse + `specialvalue/invalid` + `tlen`), JSON3 (legacy 7-arg `typeparser` + `OPTIONS` + `neededdigits`), MySQL (legacy 7-arg `typeparser` + `Options(dateformat=)`), JSON2 (deprecated; pin).

Everything else third-party (Batsrus, Vlasiator, NIST, MortalityTables, Bonsai, KiteUtils, NuclearToolkit, GraphQLParser, XmlStructLoader, CSVReader, FWFTables, GeoEnergyIO, PSID, DLMReader, DeIdentification) is a compat bump plus, in four cases, switching to the public byte-span/dateformat forms.

## 4. What 3.0 should keep (ranked by how many depend on it)

1. **`Parsers.parse(T, s)` / `Parsers.tryparse(T, s)`** with `s::AbstractString` (incl. `SubString`) and `s::AbstractVector{UInt8}` (incl. `SubArray` views); `tryparse` returns `nothing`; whole input must be consumed (after whitespace strip) or it is invalid; unsupported `T` falls back to `Base.tryparse`. Used by 15+ third-party packages, T in {Int, Int32, Int64, Float32, Float64, Date, DateTime, generic Number}. This is the entire API for over half of real users; keep the names exactly.
2. **Byte-span form `parse/tryparse(T, buf, start, stop)`** (PSID, GeoEnergyIO, DLMReader, GraphQLParser, MySQL, JSON, JSON3 all want this). Existing callers use the 2.x positional `(T, buf, opts, pos, len)` where `len` is an absolute stop index, so `(T, buf, start, stop)` semantics match what they already pass; a `Parsers.OPTIONS`/`Options()` placeholder positional arg is what they will have to delete.
3. **A public delimiter-free "prefix parse" primitive** returning `(value, next_pos, status)` (what `xparse2`/legacy 7-arg `typeparser` do today): JSON, JSON3, MySQL, JSON2(io) and the InlineStrings ext all need "parse a number starting at pos, stop at the first non-numeric byte, tell me how far you got and whether it overflowed / was special (NaN/Inf) / was invalid". This is the seam Jacob's own JSON stack lives on; keeping it public (e.g. `Parsers.parsenext(T, buf, pos, len)`) avoids JSON/JSON3 growing private number parsers. Status needs at least: ok, invalid, overflow, inexact, specialvalue, eof.
4. **`dateformat` pass-through** for Date/DateTime/Time: `tryparse(DateTime, s; dateformat=df)` or a positional `DateFormat` (DeIdentification, MySQL, ARFFFiles, ChunkedCSV, CSV). Whether the type is `Parsers.Format` or plain `Dates.DateFormat` matters little to third parties (only CSV names `Parsers.Format`).
5. **`PosLen`** as a small public type, or move it into WeakRefStrings (WeakRefStrings imports only this; CSV/InlineStrings/ARFFFiles/ChunkedCSV/OSMToolset use it via `xparse(String)`). `getstring(buf, poslen, escapechar)` goes with it if any quoted/escaped string materialization survives.
6. **Extension seam** (`supportedtype` + `typeparser`-style hook): third-party customers are FixedPointDecimals (deep, hooks the float digit accumulator) and ChunkedCSV (`GuessDateTime`, custom typeparser + `tryparsenext` for timezones); NanoDates tried and abandoned. If 3.0 offers a seam, make it a simple documented `Parsers.typeparser(::Type{T}, buf, pos, len, opts) -> (val, pos′, status)` plus `supportedtype(T)`; FPD would additionally need a public digits/exponent accumulator (or vendor it). Only 2 live third-party users, so dropping the seam is defensible if FPD/ChunkedCSV are warned and pin to 2.
7. **`Options(rounding=…)`** and `decimal=` are used only by FixedPointDecimals/ChunkedCSV/CSV; `groupmark`, `trues/falses`, `stripwhitespace`, `wh1/wh2`, `comment`, `ignoreemptylines`, `ignorerepeated`, `stripquoted`, `sentinel`, `quoted`, `openquotechar/closequotechar/escapechar`, `delim` are used only by the CSV-like readers (CSV, ChunkedCSV, ARFFFiles, PowerFlowData, OSMToolset, Schemata). None of the plain-parse users touch any `Options` field except `dateformat`.

## 5. Suggested migration notes per third-party breakage

- ChunkedCSV, ARFFFiles, PowerFlowData, Schemata: pin `Parsers = "2"` (they all already have upper bound 2, so nothing breaks until they opt in). Offer, in the 3.0 release notes, a short recipe: "tokenize fields yourself (delims/quotes), then `Parsers.parse(T, bytes, start, stop)` each span"; a ~30-line reference `splitfields` example would cover ARFFFiles/PowerFlowData/OSMToolset.
- FixedPointDecimals: reach out before release; either expose a digits accumulator or point them at the vendoring option (they've done it before for the BigInt buffers).
- OSMToolset, DLMReader, PSID, GeoEnergyIO, DeIdentification: one-line changes to the public byte-span / dateformat forms; worth opening PRs when 3.0 ships (they are all `Parsers = "2"`-capped, so no immediate breakage).
- The 6 stale and 6 dead-import packages need nothing beyond (optionally) a compat/drop-dep PR.
- Jacob-owned: JSON, JSON3, MySQL need the prefix-parse primitive; WeakRefStrings needs `PosLen`; InlineStrings drops its ext once CSV 3.0 materializes strings itself; JSON2 pins.

## Appendix: raw registry list (name | repo)

ARFFFiles | https://github.com/cjdoris/ARFFFiles.jl.git
Batsrus | https://github.com/henry2004y/Batsrus.jl.git
Bonsai | https://github.com/onetonfoot/Bonsai.jl.git
BraketAHS | https://github.com/amazon-braket/BraketAHS.jl.git
CSV | https://github.com/JuliaData/CSV.jl.git
CSVReader | https://github.com/tk3369/CSVReader.jl.git
ChunkedCSV | https://github.com/RelationalAI/ChunkedCSV.jl.git
DLMReader | https://github.com/sl-solution/DLMReader.jl.git
DeIdentification | https://github.com/bcbi/DeIdentification.jl.git
Diagonalizations | https://github.com/Marco-Congedo/Diagonalizations.jl.git
FHIRClient | https://github.com/JuliaHealth/FHIRClient.jl.git
FWFTables | https://github.com/HenricoWitvliet/FWFTables.jl.git
Firebird | https://github.com/nakagami/Firebird.jl.git
FixedPointDecimals | https://github.com/JuliaMath/FixedPointDecimals.jl.git
GeoEnergyIO | https://github.com/sintefmath/GeoEnergyIO.jl.git
GraphQLParser | https://github.com/mmiller-max/GraphQLParser.jl.git
InlineStrings | https://github.com/JuliaStrings/InlineStrings.jl.git
JSON | https://github.com/JuliaIO/JSON.jl.git
JSON2 | https://github.com/quinnj/JSON2.jl.git
JSON3 | https://github.com/quinnj/JSON3.jl.git
JutulDarcy | https://github.com/sintefmath/JutulDarcy.jl.git
KiteUtils | https://github.com/OpenSourceAWE/KiteUtils.jl.git
MortalityTables | https://github.com/JuliaActuary/MortalityTables.jl.git
MySQL | https://github.com/JuliaDatabases/MySQL.jl.git
NISTStatisticalReferenceData | https://github.com/joshday/NISTStatisticalReferenceData.jl.git
NanoDates | https://github.com/JuliaTime/NanoDates.jl.git
NuclearToolkit | https://github.com/SotaYoshida/NuclearToolkit.jl.git
OSMToolset | https://github.com/pszufe/OSMToolset.jl.git
PSID | https://github.com/aaowens/PSID.jl.git
PowerDynData | https://github.com/cuihantao/PowerDynData.jl.git
PowerFlowData | https://github.com/nickrobinson251/PowerFlowData.jl.git
Qwind | https://github.com/arnauqb/Qwind.jl.git
RankCompV3 | https://github.com/yanjer/RankCompV3.jl.git
ReinforcementLearningCore | https://github.com/JuliaReinforcementLearning/ReinforcementLearning.jl.git (monorepo subdir)
RhsJTool | https://github.com/skahanium/RhsJTool.jl.git (deleted; tarball via pkg server)
Schemata | https://github.com/JockLawrie/Schemata.jl.git
Tapestree | https://github.com/ignacioq/Tapestree.jl.git
Vlasiator | https://github.com/henry2004y/Vlasiator.jl.git
WeakRefStrings | https://github.com/JuliaData/WeakRefStrings.jl.git
XmlStructLoader | https://github.com/Tom-Lemmens/XmlStructTools.jl.git (monorepo subdir)

Clones live at `docs/review/parsers-deps/<name>` for follow-up grepping.
