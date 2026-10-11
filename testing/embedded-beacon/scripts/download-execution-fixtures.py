#!/usr/bin/env python3
"""Download current execution blockchain fixtures and record exact release provenance."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import time
import urllib.error
import urllib.request
import uuid

API = "https://api.github.com/repos/ethereum/execution-specs/releases"
MAX_ARCHIVE = 2 * 1024**3
MAX_FILE = 1024**3
MAX_TOTAL = 24 * 1024**3
MAX_FILES = 100_000


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def api_json(url):
    headers = {"User-Agent": "embedded-beacon-spec-fixtures", "Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
        return json.load(response)


def resolve_release(tag):
    if tag != "latest":
        from urllib.parse import quote
        release = api_json(API + "/tags/" + quote(tag, safe=""))
        if release.get("draft") or not release["tag_name"].startswith("tests@"):
            raise ValueError("release is not a published execution fixture release")
        return release
    candidates = []
    # Release ordering also contains independent development channels. Select only tests@.
    for page in range(1, 4):
        releases = api_json(API + f"?per_page=100&page={page}")
        candidates.extend(r for r in releases if not r.get("draft") and r.get("published_at") and r["tag_name"].startswith("tests@"))
        if len(releases) < 100:
            break
    if not candidates:
        raise ValueError("no published execution fixture release found")
    return max(candidates, key=lambda r: r["published_at"])


def download_asset(asset, cache):
    sha = asset.get("digest", "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", sha):
        raise ValueError("release asset has no published SHA-256 digest")
    size = asset["size"]
    if not 0 < size <= MAX_ARCHIVE:
        raise ValueError("archive exceeds download limit")
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / (sha[7:] + ".tar.gz")
    if path.exists():
        if path.stat().st_size != size or digest(path) != sha[7:]:
            raise ValueError("cached execution fixture archive fails authentication")
        return path
    part = cache / (sha[7:] + "." + uuid.uuid4().hex + ".part")
    for attempt in range(3):
        try:
            request = urllib.request.Request(asset["browser_download_url"] + "?download=" + uuid.uuid4().hex, headers={"User-Agent": "embedded-beacon-spec-fixtures"})
            received = 0
            hasher = hashlib.sha256()
            with urllib.request.urlopen(request, timeout=60) as response, part.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    received += len(chunk)
                    if received > size:
                        raise ValueError("archive is larger than published size")
                    hasher.update(chunk)
                    output.write(chunk)
            if received != size or hasher.hexdigest() != sha[7:]:
                raise ValueError("execution fixture archive fails published size or SHA-256 check")
            os.replace(part, path)
            return path
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == 2:
                raise
            time.sleep(attempt + 1)
    raise AssertionError("unreachable download state")


def extract_blockchain(archive, destination):
    if destination.exists():
        raise ValueError("fixture destination already exists; use verification or a fresh destination")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / (destination.name + ".staging-" + uuid.uuid4().hex)
    staging.mkdir()
    files = []
    seen = set()
    total = 0
    with tarfile.open(archive, "r|gz") as stream:
        for member in stream:
            parts = PurePosixPath(member.name).parts
            if "blockchain_tests" not in parts:
                continue
            if member.isdir():
                continue
            if not member.isfile() or member.issym() or member.islnk():
                raise ValueError("blockchain fixture archive contains a non-regular file")
            if PurePosixPath(member.name).suffix != ".json":
                continue
            if member.name.startswith("/") or ".." in parts or any("\\" in p or ":" in p for p in parts):
                raise ValueError("unsafe execution fixture archive path")
            relative = PurePosixPath(*parts[parts.index("blockchain_tests"):])
            if str(relative) in seen:
                raise ValueError("duplicate execution fixture archive path")
            seen.add(str(relative))
            total += member.size
            if member.size > MAX_FILE or total > MAX_TOTAL or len(files) >= MAX_FILES:
                raise ValueError("execution fixture extraction limit exceeded")
            target = staging.joinpath(*relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            hasher = hashlib.sha256()
            length = 0
            source = stream.extractfile(member)
            if source is None:
                raise ValueError("unreadable fixture archive member")
            with source, target.open("xb") as output:
                while chunk := source.read(1024 * 1024):
                    length += len(chunk)
                    if length > member.size:
                        raise ValueError("fixture member exceeds declared length")
                    hasher.update(chunk)
                    output.write(chunk)
            if length != member.size:
                raise ValueError("truncated execution fixture archive member")
            files.append({"path": str(relative), "bytes": length, "sha256": hasher.hexdigest()})
    if not files:
        raise ValueError("release archive contains no execution blockchain fixtures")
    staging.rename(destination)
    return sorted(files, key=lambda f: f["path"])


def verify(fixtures, provenance):
    records = provenance["files"]
    expected = {r["path"] for r in records}
    if not records or len(expected) != len(records):
        raise ValueError("empty or duplicate execution fixture inventory")
    actual = {p.relative_to(fixtures).as_posix() for p in fixtures.rglob("*") if p.is_file()}
    if actual != expected:
        raise ValueError("execution fixture inventory differs from downloaded release")
    for record in records:
        relative = PurePosixPath(record["path"])
        if relative.is_absolute() or ".." in relative.parts or any("\\" in p or ":" in p for p in relative.parts):
            raise ValueError("unsafe execution fixture inventory path")
        path = fixtures.joinpath(*relative.parts)
        if path.is_symlink() or path.stat().st_size != record["bytes"] or digest(path) != record["sha256"]:
            raise ValueError("execution fixture bytes differ from authenticated archive: " + record["path"])
    return len(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", default="latest", help="latest includes the newest tests@ prerelease")
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        record = json.loads(args.provenance.read_text())
        count = verify(args.fixtures, record)
        print(json.dumps({"release": record["release"], "verified_files": count}))
        return
    if args.work is None:
        parser.error("--work is required when downloading")
    release = resolve_release(args.release)
    assets = [a for a in release["assets"] if a["name"] == "fixtures.tar.gz"]
    if len(assets) != 1:
        raise ValueError("execution release does not contain exactly one fixtures.tar.gz asset")
    asset = assets[0]
    archive = download_asset(asset, args.work)
    files = extract_blockchain(archive, args.fixtures)
    record = {"schema_version": 1, "release": release["tag_name"], "published_at": release["published_at"], "prerelease": release["prerelease"], "asset_url": asset["browser_download_url"], "archive_bytes": asset["size"], "archive_sha256": asset["digest"][7:], "fixture_format": "blockchain_test", "files": files, "complete_execution_conformance": False, "excluded_formats": ["state_test", "blockchain_test_engine", "blockchain_test_engine_x", "transaction_test"]}
    args.provenance.parent.mkdir(parents=True, exist_ok=True)
    args.provenance.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    count = verify(args.fixtures, record)
    print(json.dumps({"release": record["release"], "verified_files": count, "archive_sha256": record["archive_sha256"], "provenance_sha256": digest(args.provenance)}))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, tarfile.TarError) as error:
        print("execution fixture preparation failed: " + str(error), file=sys.stderr)
        sys.exit(1)
