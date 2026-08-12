#!/usr/bin/env python3

import argparse
import base64
import json
import re
import sys
from pathlib import Path


ACTIVATION_MARKER = "Returning save input"
PARTIAL_TRIE_RE = re.compile(r"\bnew_partial_state_trie=(?:Some\()?([0-9]+)")
DB_TIP_RE = re.compile(r"\bnew_db_tip=(?:Some\()?([0-9]+)")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
VOLATILE_REPLACEMENTS = (
    (re.compile(r"\b[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+Z?\b"), "<timestamp>"),
    (re.compile(r"\b0x[0-9a-fA-F]{8,}\b"), "<hex>"),
    (re.compile(r"\b[0-9a-fA-F]{12,}\b"), "<hex>"),
    (re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}\b"), "<id>"),
    (re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]+\b"), "<address>"),
    (re.compile(r"\b(port|pid|block|height|number|nonce)=?[ :]+[0-9]+\b", re.I), r"\1=<n>"),
)
SERIOUS_PATTERNS = (
    ("panic", re.compile(r"\bpanicked at\b|\bpanic(?:ked)?\b", re.I)),
    ("fatal", re.compile(r"\bfatal(?: error)?\b", re.I)),
    ("startup-timeout", re.compile(r"(?:startup|start(?:ing)?)\b.*\btim(?:e|ed)[ -]?out\b|\btimed out\b.*\bstart", re.I)),
    ("startup-failure", re.compile(r"\bfailed to (?:start|launch|initialize)\b", re.I)),
    ("database-error", re.compile(r"\b(?:database|mdbx|static file)\b.*\b(?:error|corrupt|failed)\b", re.I)),
    ("state-root-mismatch", re.compile(r"\bstate[ -]?root\b.*\b(?:mismatch|incorrect|invalid)\b|\b(?:mismatch|incorrect)\b.*\bstate[ -]?root\b", re.I)),
    ("missing-trie-node", re.compile(r"\bmissing trie node\b|\btrie node\b.*\b(?:missing|not found)\b", re.I)),
    ("overlay-error", re.compile(r"\boverlay\b.*\b(?:error|failed|missing|mismatch)\b", re.I)),
    ("rpc-mismatch", re.compile(r"\brpc\b.*\b(?:response )?mismatch\b|\bresponse mismatch\b", re.I)),
)


def text_files(root: Path):
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix.lower() in {".log", ".txt"}:
            yield path


def load_results(path: Path):
    results = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        parts = line.split("\t")
        if len(parts) != 3:
            raise ValueError(f"{path}:{line_number}: expected three tab-separated columns")
        suite, test, passed = parts
        results.setdefault((suite, test), []).append(passed)
    return {key: tuple(sorted(value)) for key, value in results.items()}


def hive_regex(value: str) -> str:
    # Hive splits suite/test regexes on unescaped slashes. A slash inside a
    # character class is not treated as a separator and remains valid in Go RE2.
    special = set(r"\.^$|?*+()[]{}")
    escaped = []
    for char in value:
        if char == "/":
            escaped.append("[/]")
        elif char in special:
            escaped.append("\\" + char)
        elif char in {"\n", "\r", "\t"}:
            raise ValueError("Hive suite and test names must not contain control characters")
        else:
            escaped.append(char)
    return "^" + "".join(escaped) + "$"


def encode(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def differences(args) -> int:
    baseline = load_results(args.baseline)
    partial = load_results(args.partial)
    changed = sorted(key for key in baseline.keys() | partial.keys() if baseline.get(key) != partial.get(key))
    for index, (suite, test) in enumerate(changed, 1):
        test_filter = f"{hive_regex(suite)}/{hive_regex(test)}"
        baseline_result = ",".join(baseline.get((suite, test), ("missing",)))
        partial_result = ",".join(partial.get((suite, test), ("missing",)))
        print(
            "\t".join(
                (
                    str(index),
                    encode(suite),
                    encode(test),
                    encode(test_filter),
                    baseline_result,
                    partial_result,
                )
            )
        )
    return 1 if changed else 0


def lookup(args) -> int:
    values = load_results(args.results).get((args.suite, args.test))
    if values is None:
        print("missing")
        return 1
    print(",".join(values))
    return 0


def activation(args) -> int:
    found_behind = False
    found_event = False
    print("file\tline\tnew_partial_state_trie\tnew_db_tip\tbehind_database_tip\tevent")
    for path in text_files(args.root):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line_number, line in enumerate(lines, 1):
            if ACTIVATION_MARKER not in line:
                continue
            found_event = True
            partial_match = PARTIAL_TRIE_RE.search(line)
            db_match = DB_TIP_RE.search(line)
            event = line.replace("\t", " ").strip()
            if partial_match is None or db_match is None:
                print(f"{path}\t{line_number}\tunparsed\tunparsed\tfalse\t{event}")
                continue
            partial_trie = int(partial_match.group(1))
            db_tip = int(db_match.group(1))
            behind = partial_trie < db_tip
            found_behind = found_behind or behind
            print(
                f"{path}\t{line_number}\t{partial_trie}\t{db_tip}\t"
                f"{str(behind).lower()}\t{event}"
            )
    if not found_event:
        print("# no Returning save input event found")
    elif not found_behind:
        print("# persistence events found, but none had the partial trie behind the database tip")
    return 0 if found_behind else 1


def rpc_ci_filter(args) -> int:
    import yaml

    workflow = yaml.safe_load(args.workflow.read_text(encoding="utf-8"))
    filters = set()

    def visit(value):
        if isinstance(value, dict):
            if value.get("sim") == "ethereum/rpc-compat":
                includes = value.get("include") or []
                if not isinstance(includes, list) or not all(isinstance(item, str) for item in includes):
                    raise ValueError("RPC compatibility include list is not a string list")
                filters.add("/(" + "|".join(includes) + ")")
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(workflow)
    if not filters:
        raise ValueError("no ethereum/rpc-compat include list found in the Reth Hive workflow")
    for value in sorted(filters):
        print(value)
    if filters != {args.expected}:
        print(f"expected runner filter: {args.expected}", file=sys.stderr)
        return 1
    return 0


def normalize_message(line: str) -> str:
    line = ANSI_RE.sub("", line).strip()
    for pattern, replacement in VOLATILE_REPLACEMENTS:
        line = pattern.sub(replacement, line)
    line = re.sub(r"\b[0-9]+\b", "<n>", line)
    return re.sub(r"\s+", " ", line)


def report_timeouts(root: Path):
    for path in sorted(root.rglob("*.json")):
        if path.name == "hive.json":
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(report, dict) or not isinstance(report.get("testCases"), dict):
            continue
        suite = report.get("name", "<unnamed-suite>")
        for case in report["testCases"].values():
            result = case.get("summaryResult") or {}
            if result.get("timeout") is True:
                yield "test-timeout", f"{suite}: {case.get('name', '<unnamed-test>')}"


def errors(args) -> int:
    signatures = set(report_timeouts(args.root))
    raw_rows = []
    for path in text_files(args.root):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line_number, line in enumerate(lines, 1):
            for category, pattern in SERIOUS_PATTERNS:
                if pattern.search(line):
                    signature = (category, normalize_message(line))
                    signatures.add(signature)
                    raw_rows.append((category, str(path), str(line_number), line.strip()))
                    break
    if args.raw_output is not None:
        args.raw_output.parent.mkdir(parents=True, exist_ok=True)
        with args.raw_output.open("w", encoding="utf-8") as output:
            for row in raw_rows:
                output.write("\t".join(row) + "\n")
    for category, message in sorted(signatures):
        print(f"{category}\t{message}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze the Hive partial-persistence comparison.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    activation_parser = subparsers.add_parser("activation")
    activation_parser.add_argument("root", type=Path)
    activation_parser.set_defaults(func=activation)

    filter_parser = subparsers.add_parser("rpc-ci-filter")
    filter_parser.add_argument("workflow", type=Path)
    filter_parser.add_argument("expected")
    filter_parser.set_defaults(func=rpc_ci_filter)

    differences_parser = subparsers.add_parser("differences")
    differences_parser.add_argument("baseline", type=Path)
    differences_parser.add_argument("partial", type=Path)
    differences_parser.set_defaults(func=differences)

    lookup_parser = subparsers.add_parser("lookup")
    lookup_parser.add_argument("results", type=Path)
    lookup_parser.add_argument("suite")
    lookup_parser.add_argument("test")
    lookup_parser.set_defaults(func=lookup)

    errors_parser = subparsers.add_parser("errors")
    errors_parser.add_argument("root", type=Path)
    errors_parser.add_argument("--raw-output", type=Path)
    errors_parser.set_defaults(func=errors)

    args = parser.parse_args()
    try:
        return args.func(args)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    sys.exit(main())
