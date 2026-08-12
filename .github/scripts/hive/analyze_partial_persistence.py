#!/usr/bin/env python3

import argparse
import base64
import hashlib
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
    (
        "startup-failure",
        re.compile(
            r"\bfailed to (?:start|launch|initialize)\b|"
            r"\bcould not start client\b|\bterminated unexpectedly\b",
            re.I,
        ),
    ),
    ("database-error", re.compile(r"\b(?:database|mdbx|static file)\b.*\b(?:error|corrupt|failed)\b", re.I)),
    ("state-root-mismatch", re.compile(r"\bstate[ -]?root\b.*\b(?:mismatch|incorrect|invalid)\b|\b(?:mismatch|incorrect)\b.*\bstate[ -]?root\b", re.I)),
    ("missing-trie-node", re.compile(r"\bmissing trie node\b|\btrie node\b.*\b(?:missing|not found)\b", re.I)),
    ("overlay-error", re.compile(r"\boverlay\b.*\b(?:error|failed|missing|mismatch)\b", re.I)),
    ("rpc-mismatch", re.compile(r"\brpc\b.*\b(?:response )?mismatch\b|\bresponse mismatch\b", re.I)),
)
DIAGNOSTIC_REPLACEMENTS = (
    (re.compile(r"\x1b\[[0-9;]*[A-Za-z]"), ""),
    (re.compile(r"\b[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+Z?\b"), "<timestamp>"),
    (re.compile(r"\b(?:container|client|node)([= :]+)[0-9a-fA-F]{8,64}\b", re.I), r"\1<id>"),
    (re.compile(r"client-[0-9a-fA-F]{8,64}\.log"), "client-<id>.log"),
    (re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]+\b"), "<address>"),
    (re.compile(r"\b[0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s|min)\b"), "<duration>"),
    (re.compile(r"/(?:home/runner/work|tmp)/[^\s:]+"), "<path>"),
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


def hive_reports(root: Path):
    for path in sorted(root.rglob("*.json")):
        if path.name == "hive.json":
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(report, dict) and isinstance(report.get("testCases"), dict):
            yield path, report


def resolve_log_path(root: Path, report_path: Path, relative: str) -> Path | None:
    if not relative:
        return None
    candidate = root / Path(relative)
    if candidate.is_file():
        return candidate
    candidate = report_path.parent / Path(relative)
    if candidate.is_file():
        return candidate
    return None


def read_log_slice(path: Path | None, offsets: dict | None) -> str:
    if path is None:
        return ""
    try:
        data = path.read_bytes()
    except OSError:
        return ""
    if isinstance(offsets, dict):
        begin = offsets.get("begin", 0)
        end = offsets.get("end", len(data))
        if isinstance(begin, int) and isinstance(end, int) and 0 <= begin <= end:
            data = data[begin:end]
    return data.decode("utf-8", errors="replace")


def normalize_diagnostic(text: str) -> str:
    for pattern, replacement in DIAGNOSTIC_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return re.sub(r"\s+", " ", text).strip()


def safe_filename(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.")[:70]
    return name or "unnamed"


def diagnostic_preview(text: str, limit: int = 6000) -> str:
    text = text.strip()
    if not text:
        return "<no diagnostic text recorded>"
    if len(text) <= limit:
        return text
    return f"<truncated; full text is in the extracted evidence file>\n…{text[-limit:]}"


def failure_details(args) -> int:
    args.output.mkdir(parents=True, exist_ok=True)
    signatures = []
    report_lines = ["# Hive failure diagnostics", ""]
    failure_index = 0

    for report_path, report in hive_reports(args.root):
        suite = report.get("name", "<unnamed-suite>")
        details_path = resolve_log_path(args.root, report_path, report.get("testDetailsLog", ""))
        for case in report["testCases"].values():
            result = case.get("summaryResult") or {}
            if result.get("pass") is not False:
                continue
            failure_index += 1
            test = case.get("name", "<unnamed-test>")
            timeout = result.get("timeout") is True
            hive_details = result.get("details") or read_log_slice(details_path, result.get("log"))
            digest = hashlib.sha256(f"{suite}\0{test}".encode()).hexdigest()[:12]
            case_dir = args.output / f"{failure_index:04d}-{safe_filename(test)}-{digest}"
            case_dir.mkdir(parents=True, exist_ok=True)
            (case_dir / "hive-details.txt").write_text(hive_details, encoding="utf-8")

            clients = []
            client_previews = []
            for client_index, client in enumerate((case.get("clientInfo") or {}).values(), 1):
                client_log_path = resolve_log_path(args.root, report_path, client.get("logFile", ""))
                client_excerpt = read_log_slice(client_log_path, client.get("logOffsets"))
                client_name = client.get("name", "client")
                filename = f"client-{client_index:02d}-{safe_filename(client_name)}.log"
                (case_dir / filename).write_text(client_excerpt, encoding="utf-8")
                clients.append(
                    {
                        "name": client_name,
                        "sourceLog": client.get("logFile", ""),
                        "excerpt": filename,
                        "offsets": client.get("logOffsets"),
                    }
                )
                client_previews.append((client_name, filename, diagnostic_preview(client_excerpt)))

            metadata = {
                "suite": suite,
                "test": test,
                "timeout": timeout,
                "report": str(report_path.relative_to(args.root)),
                "testDetailsLog": report.get("testDetailsLog", ""),
                "hiveDetails": "hive-details.txt",
                "clients": clients,
            }
            (case_dir / "metadata.json").write_text(
                json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            normalized_reason = normalize_diagnostic(hive_details)
            signatures.append((suite, test, str(timeout).lower(), normalized_reason))

            relative_case_dir = case_dir.relative_to(args.output)
            report_lines.extend(
                [
                    f"## {suite} / {test}",
                    "",
                    f"- Timeout: `{str(timeout).lower()}`",
                    f"- Extracted evidence: `{relative_case_dir}`",
                    f"- Participating clients: `{len(clients)}`",
                    "",
                    "### Hive assertion or failure reason",
                    "",
                    "```text",
                    diagnostic_preview(hive_details),
                    "```",
                    "",
                ]
            )
            for client_name, filename, preview in client_previews:
                report_lines.extend(
                    [
                        f"### Reth client log: {client_name}",
                        "",
                        f"Full test-scoped excerpt: `{relative_case_dir / filename}`",
                        "",
                        "```text",
                        preview,
                        "```",
                        "",
                    ]
                )

    with (args.output / "failure-signatures.tsv").open("w", encoding="utf-8") as output:
        for row in sorted(signatures):
            output.write("\t".join(row) + "\n")
    (args.output / "failure-report.md").write_text("\n".join(report_lines), encoding="utf-8")
    (args.output / "failure-count.txt").write_text(f"{failure_index}\n", encoding="utf-8")
    return 0


def normalized_case_details(root: Path):
    rows = []
    for report_path, report in hive_reports(root):
        suite = report.get("name", "<unnamed-suite>")
        details_path = resolve_log_path(root, report_path, report.get("testDetailsLog", ""))
        for case in report["testCases"].values():
            test = case.get("name", "<unnamed-test>")
            result = case.get("summaryResult") or {}
            details = result.get("details") or read_log_slice(details_path, result.get("log"))
            rows.append(
                (
                    suite,
                    test,
                    str(result.get("pass")).lower(),
                    str(result.get("timeout") is True).lower(),
                    normalize_diagnostic(details),
                )
            )
    return sorted(rows)


def case_details(args) -> int:
    for row in normalized_case_details(args.root):
        print("\t".join(row))
    return 0


def case_signatures(args) -> int:
    for suite, test, passed, timeout, details in normalized_case_details(args.root):
        signature = hashlib.sha256(
            f"{passed}\0{timeout}\0{details}".encode("utf-8")
        ).hexdigest()
        print("\t".join((suite, test, signature)))
    return 0


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
    if args.startup_only:
        changed = [key for key in changed if key[1].startswith("client launch (")]
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


def verify_rpc_inventory(args) -> int:
    inventory = {
        line.strip()
        for line in args.inventory.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    expected = {f"{name} ({args.client})" for name in inventory}
    expected.add(f"client launch ({args.client})")
    actual_rows = load_results(args.results)
    actual = {test for (suite, test) in actual_rows if suite == "rpc-compat"}
    missing = sorted(expected - actual)
    additional = sorted(actual - expected)

    print(f"fixture_cases\t{len(inventory)}")
    print(f"expected_cases_with_launch\t{len(expected)}")
    print(f"actual_rpc_compat_cases\t{len(actual)}")
    print(f"missing_cases\t{len(missing)}")
    print(f"additional_cases\t{len(additional)}")
    for name in missing:
        print(f"missing\t{name}")
    for name in additional:
        print(f"additional\t{name}")
    return 1 if missing or additional else 0


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
    differences_parser.add_argument(
        "--startup-only",
        action="store_true",
        help="emit only differing mandatory Hive client-launch cases",
    )
    differences_parser.set_defaults(func=differences)

    lookup_parser = subparsers.add_parser("lookup")
    lookup_parser.add_argument("results", type=Path)
    lookup_parser.add_argument("suite")
    lookup_parser.add_argument("test")
    lookup_parser.set_defaults(func=lookup)

    inventory_parser = subparsers.add_parser("verify-rpc-inventory")
    inventory_parser.add_argument("inventory", type=Path)
    inventory_parser.add_argument("results", type=Path)
    inventory_parser.add_argument("--client", default="reth")
    inventory_parser.set_defaults(func=verify_rpc_inventory)

    errors_parser = subparsers.add_parser("errors")
    errors_parser.add_argument("root", type=Path)
    errors_parser.add_argument("--raw-output", type=Path)
    errors_parser.set_defaults(func=errors)

    failure_parser = subparsers.add_parser("failure-details")
    failure_parser.add_argument("root", type=Path)
    failure_parser.add_argument("output", type=Path)
    failure_parser.set_defaults(func=failure_details)

    case_details_parser = subparsers.add_parser("case-details")
    case_details_parser.add_argument("root", type=Path)
    case_details_parser.set_defaults(func=case_details)

    case_signatures_parser = subparsers.add_parser("case-signatures")
    case_signatures_parser.add_argument("root", type=Path)
    case_signatures_parser.set_defaults(func=case_signatures)

    args = parser.parse_args()
    try:
        return args.func(args)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    sys.exit(main())
