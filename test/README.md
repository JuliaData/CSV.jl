# Test fixtures via Artifacts

CSV.jl’s test fixtures live in an artifact (`artifact"testfiles"`) to avoid shipping ~30 MB of data to package users. The artifact is published as a GitHub release asset: `testdata-full-1/testfiles-artifact.tar.gz`. Running `Pkg.test` (or `test/runtests.jl`) will download it once and cache it in `~/.julia/artifacts`.

## Updating the testfiles artifact
1. Download and extract the current asset (preserves layout expected by tests):  
   `curl -L -o testfiles-artifact.tar.gz https://github.com/JuliaData/CSV.jl/releases/download/testdata-full-1/testfiles-artifact.tar.gz`  
   `mkdir testfiles && tar -xzf testfiles-artifact.tar.gz -C testfiles`
2. Edit files in `testfiles/` as needed (add/remove/update).
3. Repack and compute hashes with Julia (uses Tar/Artifacts only):  
   ```julia
   using Pkg.Artifacts, SHA, Tar
   artifact_hash = create_artifact() do artdir
       run(`cp -R testfiles/* $artdir`)
   end
   tarball = "testfiles-artifact.tar.gz"
   archive_artifact(artifact_hash, tarball)
   println((; artifact_hash, sha256=bytes2hex(open(sha256, tarball))))
   ```
4. Upload the new tarball to a release (same repo/tag is fine), update `Artifacts.toml` with the new `git-tree-sha1` and `sha256`, and keep the asset available so tests can download it.

Tests assume the artifact root contains the contents of the old `test/testfiles` directory (no extra nesting).
