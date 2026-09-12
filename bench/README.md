# Benchmarks and performance tooling

All scripts run from the repository root with the test environment
(`julia --project=test`). None are part of the test suite. Results that a
script appends (`*.tsv`) are machine-local and not committed.

| Script | What it measures |
| --- | --- |
| `bench_matrix.jl LABEL [sizes...] [--core]` | `CSV.File` throughput over 22 data shapes (numeric, mixed, strings, quoted, escaped, long text, wide, sparse, pooled, temporal, dirty, CRLF, sentinels, ...) at the given sizes in MiB; `--core` limits to seven shapes. Appends `kernel-bench-LABEL.tsv`. |
| `bench_surface.jl LABEL [--quick] [--only=substr]` | The public option surface: every `File` keyword axis, source kinds (path, IO, gzip, command, several files), `Rows`, `Chunks`, `lazy`, `Tables.Scan`, and the writer's option axes. Appends `surface-LABEL.tsv`. |
| `writebench.jl [rows...]` | `CSV.write` throughput per shape, with Polars `write_csv` when available. |
| `shootout.jl <dir> [MiB] [reps]` | Reader shootout against polars, duckdb, and pyarrow (`shootout.py`) on identical files. |
| `profile_shapes.jl [shapes...]` | Flat self-time profiles of `CSV.File` per shape (run with `-t1` for clean attribution). |
| `profile_write.jl` | Writer GC share, sink comparison, and flat profiles. |
| `fingerprint.jl` | Deterministic fingerprints (values, types, problems, written bytes) over every shape and option axis; run it under two environments and `diff` the outputs before and after a change. |
| `hygiene.jl` | Static checks: `Core.Box` captures anywhere in the package, and dynamic-dispatch / `Any` scans over the hot signatures. |
| `compare.py A.tsv B.tsv` | Per-case min-of-runs comparison of two `bench_surface.jl` result files. |

## Method

Timings on one machine drift by up to 30% between sessions, so a change is
only measured by interleaving the two versions: `git archive` the base commit
into a scratch directory, develop it into a copy of the test environment, and
alternate `--project` runs of the same script (two or more rounds). Compare
minima. Never run a benchmark while the test suite or another benchmark is
running. Force a collection between repetitions (the scripts do) so a garbage
collection landing inside one measurement is not read as a regression.

A fair cross-engine comparison reads the same bytes into memory in every
engine, pins the thread count in every engine, and reports the schema each
engine inferred; `shootout.py` sets those for polars, duckdb, and pyarrow.

## Architecture coverage

The vector scanner is width-generic LLVM IR: LLVM lowers the 64-byte masks to
AVX-512 mask registers, AVX2 pairs, or NEON reductions for the host. The
prefix-xor step uses `pclmulqdq` on x86-64 CPUs with PCLMUL and `pmull64` on
aarch64 CPUs with the AES extension (always on Apple silicon; probed at load elsewhere);
other targets use the six-step shift fallback. `fastindex=false` selects the
byte-at-a-time scalar scanner, the reference the vector scanner must match.
Measure at least one AVX-512 x86-64 host, one AVX2-only host, and one
non-Apple aarch64 host; `bench_matrix.jl LABEL 20` and
`bench_surface.jl LABEL` are the two runs to repeat there.
