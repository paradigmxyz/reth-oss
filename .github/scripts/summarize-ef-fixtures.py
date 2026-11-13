"""Summarize runner verdicts and reject incomplete fixture coverage."""

from collections import Counter
import json
import os
from pathlib import Path
import sys


def main():
    fixtures, results_path, status, release, mode = sys.argv[1:]
    expected = Counter()
    for path in sorted(Path(fixtures).rglob("*.json")):
        with path.open() as source:
            expected.update(json.load(source).keys())

    try:
        with Path(results_path).open() as source:
            results = json.load(source)
        if not isinstance(results, list) or any(
            not isinstance(result, dict)
            or not isinstance(result.get("name"), str)
            or type(result.get("pass")) is not bool
            or not isinstance(result.get("rejections"), list)
            for result in results
        ):
            raise ValueError("expected an array of fixture results")
    except (OSError, ValueError) as error:
        print(f"Cannot read runner results: {error}; runner exit code: {status}", file=sys.stderr)
        return 1

    reported = Counter(result["name"] for result in results)
    missing = expected - reported
    unexpected = reported - expected
    failed = [result for result in results if not result["pass"]]
    lines = [
        f"## {mode}: {release}",
        "",
        f"Runner exit code: {status}.",
        f"Fixtures: {len(results)} reported / {expected.total()} expected; "
        f"{len(results) - len(failed)} passed, {len(failed)} failed.",
        f"Missing results: {missing.total()}; unexpected results: {unexpected.total()}.",
    ]
    for label, names in (("Missing", missing), ("Unexpected", unexpected)):
        lines.extend(f"{label}: {name}" for name in list(names)[:20])
    lines.extend(
        f"Failed: {result['name']}: {result.get('error', '')}" for result in failed[:20]
    )
    summary = "\n\n".join(lines) + "\n"
    print(summary)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a") as output:
            output.write(summary)

    return int(not expected or bool(missing) or bool(unexpected) or (status == "0" and bool(failed)))


if __name__ == "__main__":
    sys.exit(main())
