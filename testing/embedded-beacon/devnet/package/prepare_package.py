#!/usr/bin/env python3
"""Prepare an authenticated, image-pinned package copy under ignored vendor/."""

import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tarfile
import tempfile
import tokenize
import urllib.request

HERE = Path(__file__).resolve().parent
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_MEMBER_BYTES = 8 * 1024 * 1024
MAX_EXTRACTED_BYTES = 128 * 1024 * 1024


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pin_run_sh(source, helper_image):
    """Insert an immutable image only when the direct run_sh call omits it."""
    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    line_offsets = [0]
    for line in source.splitlines(keepends=True):
        line_offsets.append(line_offsets[-1] + len(line))
    edits = []
    significant = [t for t in tokens if t.type not in {
        tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE,
        tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER,
    }]
    for index in range(len(significant) - 3):
        if [t.string for t in significant[index:index + 4]] != ["plan", ".", "run_sh", "("]:
            continue
        opening = significant[index + 3]
        depth = 1
        has_image = False
        for candidate in range(index + 4, len(significant)):
            token = significant[candidate]
            if depth == 1 and token.string == "image" and significant[candidate + 1].string == "=":
                has_image = True
            if token.string in {"(", "[", "{"}:
                depth += 1
            elif token.string in {")", "]", "}"}:
                depth -= 1
                if depth == 0:
                    break
        if depth != 0:
            raise ValueError("Unterminated plan.run_sh call")
        if not has_image:
            offset = line_offsets[opening.end[0] - 1] + opening.end[1]
            edits.append((offset, 'image="' + helper_image + '",'))
    for offset, insertion in reversed(edits):
        source = source[:offset] + insertion + source[offset:]
    return source, len(edits)



def prepare(archive, pins):
    expected = pins["ethereum_package"]["archive_sha256"]
    if not archive.is_file() or archive.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("Package archive exceeds file bound")
    if sha256_file(archive) != expected:
        raise ValueError("Package archive SHA-256 differs from pins.json")
    vendor = HERE / "vendor"
    vendor.mkdir(exist_ok=True)
    extracted_bytes = 0
    records = []
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle:
            parts = PurePosixPath(member.name).parts
            if len(parts) < 2:
                continue
            relative = PurePosixPath(*parts[1:])
            if relative.as_posix() == "run.gif":
                continue
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Unsafe package archive path")
            # The upstream agents skill is an irrelevant symlink; do not copy links.
            if member.issym() or member.islnk():
                continue
            destination = vendor.joinpath(*relative.parts)
            if destination.is_symlink() or not destination.resolve().is_relative_to(vendor.resolve()):
                raise ValueError("Unsafe existing vendor path")
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ValueError("Unexpected package archive member")
            extracted_bytes += member.size
            if member.size > MAX_MEMBER_BYTES or extracted_bytes > MAX_EXTRACTED_BYTES:
                raise ValueError("Package archive exceeds extraction bounds")
            destination.parent.mkdir(parents=True, exist_ok=True)
            content = bundle.extractfile(member).read(MAX_MEMBER_BYTES + 1)
            if len(content) != member.size:
                raise ValueError("Truncated package archive member")
            destination.write_bytes(content)
            records.append(relative.as_posix())
    key_generator = vendor / "src/prelaunch_data_generator/validator_keystores/validator_keystore_generator.star"
    constants = vendor / "src/package_io/constants.star"
    static_files = vendor / "src/static_files/static_files.star"
    replacements = [
        (key_generator, 'ETH_VAL_TOOLS_IMAGE = "protolambda/eth2-val-tools:latest"',
         'ETH_VAL_TOOLS_IMAGE = "' + pins["images"]["validator_key_generator"]["image"] + '"'),
        (constants, 'DEFAULT_YQ_IMAGE = "linuxserver/yq"',
         'DEFAULT_YQ_IMAGE = "' + pins["images"]["yq"]["image"] + '"'),
        (static_files, 'STATIC_FILES_DIRPATH = "/static_files"',
         'STATIC_FILES_DIRPATH = "/vendor/static_files"'),

    ]
    for path, old, new in replacements:
        source = path.read_text(encoding="utf-8")
        if source.count(old) != 1:
            raise ValueError("Pinned upstream source has changed: " + str(path))
        path.write_text(source.replace(old, new), encoding="utf-8", newline="\n")

    patched = []
    helper_calls = 0
    for relative in sorted(records):
        path = vendor / relative
        if path.suffix == ".star":
            source = path.read_text(encoding="utf-8")
            result, count = pin_run_sh(source, pins["images"]["genesis_generator"]["image"])
            if count:
                path.write_text(result, encoding="utf-8", newline="\n")
                helper_calls += count
                patched.append(relative)
    provenance = {
        "ethereum_package": pins["ethereum_package"],
        "extracted_file_count": len(records),
        "helper_image": pins["images"]["genesis_generator"]["image"],
        "default_helper_calls_pinned": helper_calls,
        "patched_helper_files": patched,
        "patches": [str(path.relative_to(vendor)) for path, _, _ in replacements],

    }
    (vendor / "embedded-lock.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(provenance, indent=2))


def download_verified_archive(work, pins):
    """Publish the cache only after a complete bounded download passes its pin."""
    work.mkdir(exist_ok=True)
    target = work / "ethereum-package.tar.gz"
    expected = pins["ethereum_package"]["archive_sha256"]
    if target.is_file() and target.stat().st_size <= MAX_ARCHIVE_BYTES and sha256_file(target) == expected:
        return target
    # Reuse a separately verified local research cache without touching an active download.
    reference = work / "reference/ethereum-package.tar.gz"
    if reference.is_file() and reference.stat().st_size <= MAX_ARCHIVE_BYTES and sha256_file(reference) == expected:
        return reference
    request = urllib.request.Request(
        pins["ethereum_package"]["archive_url"],
        headers={"User-Agent": "embedded-beacon-devnet"},
    )
    partial = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix="ethereum-package.", suffix=".partial", dir=work, delete=False,
        ) as output:
            partial = Path(output.name)
            digest = hashlib.sha256()
            total = 0
            with urllib.request.urlopen(request, timeout=90) as response:
                for chunk in iter(lambda: response.read(65536), b""):
                    total += len(chunk)
                    if total > MAX_ARCHIVE_BYTES:
                        raise ValueError("Package archive exceeds download bound")
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != expected:
                raise ValueError("Package archive SHA-256 differs from pins.json")
        # Replace is atomic on the same filesystem; readers only see verified archives.
        partial.replace(target)
        return target
    finally:
        if partial is not None and partial.exists():
            partial.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="Use an existing archive after SHA-256 verification")
    args = parser.parse_args()
    pins = json.loads((HERE / "pins.json").read_text(encoding="utf-8"))
    archive = args.archive or download_verified_archive(HERE.parent / "work", pins)
    prepare(archive, pins)


if __name__ == "__main__":
    main()
