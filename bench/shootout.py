#!/usr/bin/env python3
# Cross-engine reader shootout: polars, duckdb, pyarrow on the files that
# bench/shootout.jl generates (same bytes as bench_matrix.jl shapes).
#   python3 bench/shootout.py <datadir> <threads> [reps]
# Appends TSV rows (engine, shape, threads, ms, MiB/s) to <datadir>/shootout.tsv.
import os, sys, time, glob
datadir, threads = sys.argv[1], int(sys.argv[2])
reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
os.environ["POLARS_MAX_THREADS"] = str(threads)
import polars as pl, duckdb, pyarrow as pa, pyarrow.csv as pacsv
pa.set_cpu_count(threads); pa.set_io_thread_count(threads)
con = duckdb.connect(); con.execute(f"SET threads={threads}")
def best(f):
    f()
    return min(timeit(f) for _ in range(reps))
def timeit(f):
    t = time.perf_counter(); f(); return time.perf_counter() - t
out = open(os.path.join(datadir, "shootout.tsv"), "a")
for path in sorted(glob.glob(os.path.join(datadir, "*.csv"))):
    shape = os.path.basename(path)[:-4]
    nbytes = os.path.getsize(path)
    engines = {
        "polars":  lambda: pl.read_csv(path, try_parse_dates=True, infer_schema_length=10000),
        "duckdb":  lambda: con.execute("SELECT * FROM read_csv_auto(?)", [path]).fetch_arrow_table(),
        "pyarrow": lambda: pacsv.read_csv(path),
    }
    for name, f in engines.items():
        try:
            table = f()
            print(f"{name:8s} {shape:12s} schema={table.schema}", flush=True)
            t = best(f)
            print(f"{name:8s} {shape:12s} {threads}T {t*1e3:9.1f} ms {nbytes/2**20/t:8.0f} MiB/s", flush=True)
            out.write(f"{name}\t{shape}\t{threads}\t{t*1e3:.2f}\t{nbytes/2**20/t:.1f}\n"); out.flush()
        except Exception as e:
            print(f"{name:8s} {shape:12s} ERROR {e}", flush=True)
