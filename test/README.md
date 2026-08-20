# Test fixtures

The small legacy fixtures are exact byte literals in `legacy/corpus_inline.jl`.
The 24 large real-world fixtures come from the lazy `testfiles` artifact in
`Artifacts.toml`. Running `Pkg.test()` downloads the artifact once and caches
it in the Julia depot. Package users do not download these test-only files.

`artifacts/testfiles.sha256` is the source-of-truth file list and checksum
manifest. To replace the corpus, first put the original fixture files in one
source directory. Build and verify the archive twice with the checked-in tool:

```sh
julia --project=test test/artifacts/build_testfiles.jl \
    /path/to/original/testfiles /tmp/testfiles-artifact.tar.gz
```

The tool rejects missing files, checksum changes, symbolic links, nested paths,
and non-reproducible output. It also extracts the result, verifies its exact file
list and contents, and prints the `git-tree-sha1` and `sha256` values needed by
`Artifacts.toml`.

Publish the archive under a new immutable release tag and asset URL. Never
replace an existing asset because old checkouts must remain reproducible. Update
both hashes and the URL in `Artifacts.toml`, then verify a clean download:

```sh
julia --startup-file=no --project=test -e '
    using Pkg.Artifacts
    ensure_artifact_installed("testfiles", "test/Artifacts.toml")
    @assert artifact_hash("testfiles", "test/Artifacts.toml") ==
        Base.SHA1("EXPECTED_GIT_TREE_SHA1")
'
```

Run the legacy test set and the full package test suite after the download
check.

## Frozen 0.10 oracle

`runtests.jl` includes `LegacyCSV/src/LegacyCSV.jl` directly. It is test source,
not a registry package or a nested Julia environment. `Project.toml` therefore
lists the frozen source's direct dependencies. Its four oracle-only dependencies
are pinned to exact versions, so a dependency update cannot silently change the
comparison behavior. CSV itself does not depend on Parsers.

The benchmark scripts use the same loader. Prepare the test environment once
before running them from a fresh checkout:

```sh
julia --project=test -e 'using Pkg; Pkg.develop(path=pwd()); Pkg.instantiate()'
julia --project=test -t4 bench/bench_matrix.jl local 0.01 --core
```
