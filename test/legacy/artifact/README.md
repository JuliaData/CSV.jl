# The recut test corpus artifact

The 0.10 corpus (119 files, 28 MB) is now: 82 small files inlined as byte
literals in `test/legacy/corpus_inline.jl` (12 KB), the generated shape
battery in `test/legacy/generated.jl` (zero bytes), and THIS artifact — the
24 large real-world files whose value is being real (messy provenance a
synthetic cannot reproduce): 18 MB unpacked, 5.1 MB gzipped.

Dropped: `pandas_zeros.csv` (10 MB synthetic zeros grid used only for a
`normalizenames` check the inline corpus covers) and the 12 files no test
referenced (`precompile*.csv`, `test_utf16*.csv`, `test_basic.csv.gz`,
`test_floats.csv`, `test_header_on_row_4.csv`, `test_int_sentinel.csv`,
`test_missing_last_column.csv`, `test_mixed_date_formats.csv`,
`test_one_row_of_data.csv`).

## Publishing (release action — maintainer)

    git-tree-sha1 = d37a9eaf615396a9c00d9f4280cb832111193b57
    sha256        = eafb7963676f45ee4e07f9d2e6216cb9e62f6b032ef5ddc059c81589907d2533
    tarball       = testfiles-artifact.tar.gz (in this directory, gitignored)

1. Create GitHub release `testdata-full-2` on JuliaData/CSV.jl and upload
   `testfiles-artifact.tar.gz`.
2. `Artifacts.toml` already points at that URL with the hashes above.

Until the release exists, the OLD artifact (`testdata-full-1`) still resolves
every kept file (the new set is a strict subset), so tests keep passing:
`corpusfile()` falls back to it when the new tree is unavailable.
