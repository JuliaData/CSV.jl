#!/usr/bin/env julia

using CodecZlib: GzipCompressorStream, GzipDecompressor
using SHA: sha256
using Tar

const CHECKSUMS = joinpath(@__DIR__, "testfiles.sha256")

function usage()
    println(stderr, "usage: julia --project=test test/artifacts/build_testfiles.jl SOURCE_DIR OUTPUT.tar.gz")
    return 2
end

function load_manifest(path::AbstractString)
    entries = Pair{String,String}[]
    for (line_number, line) in enumerate(eachline(path))
        isempty(line) && continue
        m = match(r"^([0-9a-f]{64})  (.+)$", line)
        m === nothing && error("invalid checksum entry at $(path):$(line_number)")
        hash, name = m.captures
        (basename(name) == name && name != "." && name != "..") ||
            error("fixture names must be plain file names: $(repr(name))")
        push!(entries, name => hash)
    end
    isempty(entries) && error("fixture checksum manifest is empty")
    names = first.(entries)
    names == sort(names) || error("fixture checksum manifest must be sorted by file name")
    allunique(names) || error("fixture checksum manifest contains duplicate file names")
    return entries
end

function stage_fixtures(source::AbstractString, stage::AbstractString, entries)
    isdir(source) || error("fixture source directory does not exist: $(source)")
    for (name, expected) in entries
        input = joinpath(source, name)
        ispath(input) || error("missing fixture: $(input)")
        islink(input) && error("fixture must not be a symbolic link: $(input)")
        isfile(input) || error("fixture must be a regular file: $(input)")
        actual = bytes2hex(sha256(read(input)))
        actual == expected || error("checksum mismatch for $(name): expected $(expected), got $(actual)")
        output = joinpath(stage, name)
        cp(input, output)
        chmod(output, 0o644)
    end
    return nothing
end

function create_archive(stage::AbstractString, output::AbstractString)
    open(output, "w") do raw
        gzip = GzipCompressorStream(raw; level=6)
        try
            Tar.create(stage, gzip; portable=true)
        finally
            close(gzip)
        end
    end
    return nothing
end

function verify_archive(bytes::Vector{UInt8}, entries)
    tarbytes = transcode(GzipDecompressor, bytes)
    tree = string(Tar.tree_hash(IOBuffer(tarbytes)))
    mktempdir() do extracted
        Tar.extract(IOBuffer(tarbytes), extracted)
        names = sort(readdir(extracted))
        names == first.(entries) || error("archive file list does not match the checksum manifest")
        for (name, expected) in entries
            path = joinpath(extracted, name)
            islink(path) && error("archive contains a symbolic link: $(name)")
            isfile(path) || error("archive entry is not a regular file: $(name)")
            actual = bytes2hex(sha256(read(path)))
            actual == expected || error("archive checksum mismatch for $(name)")
        end
    end
    return tree
end

function main(args)
    length(args) == 2 || return usage()
    source = abspath(args[1])
    output = abspath(args[2])
    entries = load_manifest(CHECKSUMS)
    mkpath(dirname(output))

    mktempdir() do work
        stage = joinpath(work, "stage")
        mkdir(stage)
        stage_fixtures(source, stage, entries)

        first_archive = joinpath(work, "testfiles-a.tar.gz")
        second_archive = joinpath(work, "testfiles-b.tar.gz")
        create_archive(stage, first_archive)
        create_archive(stage, second_archive)
        first_bytes = read(first_archive)
        second_bytes = read(second_archive)
        first_bytes == second_bytes || error("archive creation is not byte-reproducible")

        tree = verify_archive(first_bytes, entries)
        archive_hash = bytes2hex(sha256(first_bytes))
        cp(first_archive, output; force=true)
        println("wrote $(length(entries)) fixtures to $(output)")
        println("git-tree-sha1 = \"$(tree)\"")
        println("sha256 = \"$(archive_hash)\"")
    end
    return 0
end

exit(main(ARGS))
