#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

def reports(root: Path):
    for path in root.rglob("*.json"):
        if path.name == "hive.json":
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(report, dict) and isinstance(report.get("testCases"), dict):
            yield report


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize Hive test-case results.")
    parser.add_argument("root", type=Path, help="Hive log directory")
    parser.add_argument("--exclusion", type=Path)
    parser.add_argument("--ignored", type=Path)
    parser.add_argument(
        "--policy",
        action="store_true",
        help="emit unexpected-failure/unexpected-pass rows using Reth policy files",
    )
    args = parser.parse_args()

    exclusions = {}
    ignored = {}
    if args.policy:
        import yaml

        if args.exclusion is None or args.ignored is None:
            parser.error("--policy requires --exclusion and --ignored")
        exclusions = yaml.safe_load(args.exclusion.read_text(encoding="utf-8")) or {}
        ignored = yaml.safe_load(args.ignored.read_text(encoding="utf-8")) or {}

    rows = []
    for report in reports(args.root):
        suite = report.get("name", "<unnamed-suite>")
        suite_exclusions = set(exclusions.get(suite, []))
        suite_ignored = set(ignored.get(suite, []))
        for case in report["testCases"].values():
            name = case.get("name", "<unnamed-test>")
            result = case.get("summaryResult") or {}
            passed = result.get("pass")
            if args.policy:
                if name in suite_ignored:
                    continue
                if passed is False and name not in suite_exclusions:
                    rows.append(("unexpected-failure", suite, name))
                elif passed is True and name in suite_exclusions:
                    rows.append(("unexpected-pass", suite, name))
            else:
                rows.append((suite, name, str(passed).lower()))

    for row in sorted(rows):
        print("\t".join(row))


if __name__ == "__main__":
    main()
