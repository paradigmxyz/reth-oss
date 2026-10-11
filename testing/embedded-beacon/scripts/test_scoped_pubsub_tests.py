"""Offline preparation and complete-run gate contracts; these never run Cargo."""

import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import tomllib
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("scoped_pubsub", Path(__file__).with_name("scoped-pubsub-tests.py"))
pubsub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pubsub)
SNAPSHOT_SOURCE = Path(__file__).resolve().parents[3] / pubsub.SNAPSHOT


def inventory_log(names):
    return "".join(f"{name}: test\n" for name in names) + f"\n{len(names)} tests, 0 benchmarks\n"


def run_log(names):
    return (f"running {len(names)} tests\n" + "".join(f"test {name} ... ok\n" for name in names) +
            f"test result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n")


class CopyContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pubsub-contract-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        (self.repository / "Cargo.toml").write_bytes(b"production manifest bytes\n")
        (self.repository / "Cargo.lock").write_bytes(b"production lock bytes\n")
        self.source = self.repository / pubsub.SNAPSHOT
        shutil.copytree(SNAPSHOT_SOURCE, self.source)
        self.temp_root = self.root / "temporary-copies"
        self.artifacts = self.root / "evidence"

    def prepared(self):
        return pubsub.prepare(self.repository, self.temp_root, self.artifacts)

    def test_copy_is_unique_complete_and_normalization_is_only_in_private_manifest(self):
        self.assertEqual(pubsub.SNAPSHOT, Path("crates/embedded-beacon/gossipsub"))
        before = pubsub.files(self.source)
        production = pubsub.production_hashes(self.repository)
        record = self.prepared()
        package, owner, artifacts = pubsub.verify_preparation(record)
        manifest = tomllib.loads((package / "Cargo.toml").read_text())
        self.assertEqual(manifest["dev-dependencies"]["quickcheck"]["version"], "=1.0.3")
        self.assertEqual(manifest["workspace"], {})
        self.assertEqual(pubsub.files(self.source), before)
        self.assertEqual(pubsub.production_hashes(self.repository), production)
        self.assertEqual((package / "Cargo.toml").read_bytes(),
                         (self.source / "Cargo.toml").read_bytes() + pubsub.NORMALIZATION)
        self.assertEqual(pubsub.read_json(artifacts / "preparation.json"), record)
        self.assertTrue((artifacts / "LICENSE-MIT").exists())
        second = pubsub.prepare(self.repository, self.temp_root, self.root / "second-evidence")
        self.assertNotEqual(second["owner"], str(owner))

    def test_private_lock_may_change_without_production_lock_change(self):
        record = self.prepared()
        (Path(record["package"]) / "Cargo.lock").write_bytes(b"verification-only resolution\n")
        pubsub.verify_preparation(record)
        self.assertEqual((self.repository / "Cargo.lock").read_bytes(), b"production lock bytes\n")

    def test_source_changes_fail_both_hash_and_copy_checks(self):
        record = self.prepared()
        (self.source / "src/lib.rs").write_bytes(b"changed source")
        with self.assertRaisesRegex(ValueError, "file/hash mismatch"):
            pubsub.verify_preparation(record)

    def test_copied_source_changes_fail(self):
        record = self.prepared()
        (Path(record["package"]) / "src/lib.rs").write_bytes(b"changed copied source")
        with self.assertRaisesRegex(ValueError, "temporary package source/manifest changed"):
            pubsub.verify_preparation(record)

    def test_production_manifest_or_lock_changes_fail(self):
        record = self.prepared()
        for name in ("Cargo.toml", "Cargo.lock"):
            with self.subTest(name=name):
                path = self.repository / name
                original = path.read_bytes()
                path.write_bytes(b"changed production bytes")
                with self.assertRaisesRegex(ValueError, "production manifest/lock changed"):
                    pubsub.verify_preparation(record)
                path.write_bytes(original)

    def test_unrecorded_file_fails_instead_of_copying_unknown_sources(self):
        (self.source / "unknown.rs").write_bytes(b"unrecorded")
        with self.assertRaisesRegex(ValueError, "file/hash mismatch"):
            self.prepared()
        self.assertFalse(self.temp_root.exists())

    def test_provenance_path_traversal_is_rejected(self):
        path = self.source / "SOURCE-PROVENANCE.json"
        provenance = pubsub.read_json(path)
        provenance["original_files"][0]["path"] = "../Cargo.toml"
        pubsub.write_json(path, provenance)
        with self.assertRaisesRegex(ValueError, "unsafe file name"):
            self.prepared()

    def test_snapshot_links_are_rejected(self):
        link = self.source / "linked.rs"
        try:
            link.symlink_to(self.repository / "Cargo.toml")
        except OSError as error:
            self.skipTest(f"platform cannot create an unprivileged test symlink: {error}")
        with self.assertRaisesRegex(ValueError, "linked/reparse path"):
            self.prepared()

    def test_temp_or_artifact_directories_inside_snapshot_are_rejected(self):
        for temp, artifacts in ((self.source / "tmp", self.artifacts),
                                (self.temp_root, self.source / "evidence"),
                                (self.temp_root, self.repository)):
            with self.subTest(temp=temp, artifacts=artifacts):
                with self.assertRaises(ValueError):
                    pubsub.prepare(self.repository, temp, artifacts)

    def test_existing_evidence_is_never_overwritten(self):
        self.artifacts.mkdir()
        sentinel = self.artifacts / "keep.txt"
        sentinel.write_text("keep existing evidence")
        with self.assertRaisesRegex(ValueError, "fresh evidence directory"):
            self.prepared()
        self.assertEqual(sentinel.read_text(), "keep existing evidence")

    def test_ownership_record_mismatch_prevents_execution(self):
        record = self.prepared()
        altered = dict(record, test_selection="filtered-library")
        with self.assertRaisesRegex(ValueError, "ownership record mismatch"):
            pubsub.verify_preparation(altered)

    def mock_runner(self, record, failure=False, source_change=False):
        """Synthetic subprocess output establishes gate policy, never Rust conformance."""
        names = ["protocol::first", "peer_score::second", "rpc::third"]
        calls = []

        def command(command, directory, log, environment):
            calls.append(command)
            if log.name == "lock-resolution.log":
                (directory / "Cargo.lock").write_text("isolated generated lock")
                log.write_text("synthetic resolution\n")
            elif log.name == "library-build.log":
                executable = Path(environment["CARGO_TARGET_DIR"]) / "debug/deps/library-tests"
                executable.parent.mkdir(parents=True)
                executable.write_bytes(b"synthetic executable, not run")
                log.write_text(json.dumps({"reason": "compiler-artifact", "features": ["metrics"],
                    "profile": {"test": True}, "target": {"name": "libp2p_gossipsub", "kind": ["lib"],
                    "src_path": str(directory / "src/lib.rs")}, "executable": str(executable)}) + "\n")
            elif log.name == "library-inventory.log":
                log.write_text(inventory_log(names))
            else:
                log.write_text("synthetic process failure\n" if failure else run_log(names))
                if source_change:
                    (self.repository / "Cargo.lock").write_text("changed production lock")
                if failure:
                    return 1
            return 0

        return command, calls

    def test_mocked_execution_contract_uses_one_binary_full_inventory_and_no_cargo_filter(self):
        record = self.prepared()
        runner, calls = self.mock_runner(record)
        with patch.object(pubsub, "command_run", side_effect=runner):
            gate = pubsub.run(record)
        self.assertTrue(gate["accepted"])
        self.assertEqual(gate["tests"], 3)
        self.assertEqual(calls[1][-5:], ["--features", "metrics", "--lib", "--no-run", "--message-format=json"])
        self.assertEqual(calls[2][0], calls[3][0])
        self.assertEqual(calls[2][1:], ["--list"])
        self.assertEqual(calls[3][1:], ["--test-threads=1"])
        self.assertTrue(gate["production_files_unchanged"])
        self.assertEqual(len(gate["test_executable_sha256"]), 64)
        self.assertEqual((self.artifacts / "verification-Cargo.lock").read_text(), "isolated generated lock")

    def test_mocked_failure_preserves_log_status_provenance_and_failure_gate(self):
        record = self.prepared()
        runner, _ = self.mock_runner(record, failure=True)
        with patch.object(pubsub, "command_run", side_effect=runner):
            gate = pubsub.run(record)
        self.assertFalse(gate["accepted"])
        self.assertEqual(gate["commands"][-1]["exit_code"], 1)
        self.assertTrue(gate["production_files_unchanged"])
        self.assertTrue((self.artifacts / "SOURCE-PROVENANCE.json").exists())
        self.assertIn("synthetic process failure", (self.artifacts / "library-tests.log").read_text())
        self.assertEqual(pubsub.read_json(self.artifacts / "ci-gate.json"), gate)

    def test_source_mutation_rejects_even_complete_mocked_success(self):
        record = self.prepared()
        runner, _ = self.mock_runner(record, source_change=True)
        with patch.object(pubsub, "command_run", side_effect=runner):
            gate = pubsub.run(record)
        self.assertFalse(gate["accepted"])
        self.assertIn("production manifest/lock changed", gate["source_verification_error"])


class CompleteGateContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="pubsub-gate-contract-")
        self.addCleanup(self.temporary.cleanup)
        self.log = Path(self.temporary.name) / "synthetic.log"
        self.names = ["first::case", "second::case", "third::case"]

    def test_inventory_requires_complete_unique_nonempty_library_and_no_benchmarks(self):
        self.log.write_text(inventory_log(self.names))
        self.assertEqual(pubsub.check_inventory(self.log), self.names)
        for text in (inventory_log([]), inventory_log(["first", "first"]),
                     inventory_log(self.names).replace("3 tests", "4 tests"),
                     inventory_log(self.names).replace("0 benchmarks", "1 benchmark")):
            with self.subTest(text=text):
                self.log.write_text(text)
                with self.assertRaises(ValueError):
                    pubsub.check_inventory(self.log)

    def test_gate_requires_all_names_and_passes_should_panic_success(self):
        self.log.write_text(run_log(self.names).replace("test second::case ...", "test second::case - should panic ..."))
        gate = pubsub.check_run(self.log, self.names, 0)
        self.assertEqual(gate["passed"], 3)
        self.assertEqual(gate["ignored"], 0)
        self.assertEqual(len(gate["test_names_sha256"]), 64)

    def test_subsets_duplicates_failed_ignored_filtered_or_missing_outcomes_never_pass(self):
        good = run_log(self.names)
        rejected = [good.replace("test third::case ... ok\n", ""),
                    good.replace("test third::case", "test first::case"),
                    good.replace("test third::case", "test unknown::case"),
                    good.replace("3 passed; 0 failed", "2 passed; 1 failed"),
                    good.replace("3 passed; 0 failed; 0 ignored", "2 passed; 0 failed; 1 ignored"),
                    good.replace("0 filtered out", "1 filtered out"),
                    good.replace("0 measured", "1 measured"),
                    good.replace("running 3 tests\n", ""), good + good,
                    good.replace("test result: ok.", "test result: FAILED.")]
        for text in rejected:
            with self.subTest(text=text):
                self.log.write_text(text)
                with self.assertRaises(ValueError):
                    pubsub.check_run(self.log, self.names, 0)
        self.log.write_text(good)
        with self.assertRaises(ValueError):
            pubsub.check_run(self.log, self.names, 1)


if __name__ == "__main__":
    unittest.main()
