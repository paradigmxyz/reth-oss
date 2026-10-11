"""Strict complete report policy regressions using small synthetic source identities."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("complete_consensus_gate", Path(__file__).with_name("check-complete-consensus-fixture-results.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CompleteConsensusGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.paths = {role: self.root / (role + ".json") for role in ("primary", "compliance")}
        self.identities = ["general/phase0/ssz_generic/boolean/invalid/primary",
                           "minimal/gloas/fork_choice_compliance/on_block/pyspec_tests/compliance"]
        self.manifests = {}
        self.provenances = {}
        self.pins = {"format_version": 1, "spec_release": "v1.7.0-beta.3",
                     "corpus_role_order": ["primary", "compliance"], "required_case_count": 2,
                     "required_file_checks": 4, "corpora": {}}
        for index, role in enumerate(("primary", "compliance")):
            identity = self.identities[index]
            files = {identity + "/steps.yaml": str(index + 1) * 64,
                     "minimal/config.yaml": str(index + 3) * 64}
            self.manifests[role] = {"spec_release": self.pins["spec_release"],
                                   "required_suites": ["/".join(identity.split("/")[:5])],
                                   "files": files}
            archive = role + ".tar.gz"
            self.pins["corpora"][role] = {"file_count": 2, "case_count": 1,
                                          "required_suite_count": 1, "archives": {archive: "a" * 64}}
            self.provenances[role] = {"spec_release": self.pins["spec_release"], "profile": "full",
                                      "corpus_role": role, "case_count": 1, "published_case_count": 1,
                                      "extracted_files": 2,
                                      "archives": [{"name": archive, "bytes": 1, "sha256": "a" * 64,
                                                    "digest_source": "published_release_metadata"}]}
            self.write_manifest(role)
        file_hasher = hashlib.sha256(module.FILE_DOMAIN)
        for role in ("primary", "compliance"):
            for relative, digest in sorted(self.manifests[role]["files"].items()):
                module.sized(file_hasher, role.encode())
                module.sized(file_hasher, relative.encode())
                file_hasher.update(bytes.fromhex(digest))
        self.pins["canonical_input_files_sha256"] = file_hasher.hexdigest()
        self.pins["canonical_case_ids_sha256"] = module.case_ids_digest(self.identities)
        self.report = {"spec_release": self.pins["spec_release"], "complete_matrix": True, "errors": [],
                       "full_corpus": {"case_ids_sha256": "0x" + self.pins["canonical_case_ids_sha256"],
                                       "input_files_sha256": "0x" + self.pins["canonical_input_files_sha256"],
                                       "corpora": [{"role": role, "files": 2, "cases": 1}
                                                   for role in ("primary", "compliance")]},
                       "cases": [{"path": identity, "outcome": "passed", "message": "", "seconds": 0.1,
                                  **{marker: False for marker in module.MARKERS}}
                                 for identity in self.identities]}

    def write_manifest(self, role):
        raw = (json.dumps(self.manifests[role], indent=2, sort_keys=True) + "\n").encode()
        self.paths[role].write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        self.pins["corpora"][role]["manifest_sha256"] = digest
        self.provenances[role]["manifest_sha256"] = digest

    def gate(self, status=0):
        return module.gate(self.pins, self.report, self.provenances["primary"], self.provenances["compliance"],
                           self.paths["primary"], self.paths["compliance"], status)

    def test_exact_fresh_source_policy_accepts_and_keeps_runtime_unverified(self):
        result = self.gate()
        self.assertEqual(result["cases"], 2)
        self.assertTrue(result["complete_conformance"])
        self.assertFalse(result["runtime_synchronization_verified"])
        self.assertFalse(result["fixture_assumptions_are_runtime_proof"])
        self.assertEqual(sum(family["cases"] for family in result["handler_coverage"]), 2)
        self.assertEqual({family["fork"] for family in result["handler_coverage"]}, {"phase0", "gloas"})
        for case in self.report["cases"]:
            for marker in module.MARKERS:
                case[marker] = True
        result = self.gate()
        self.assertTrue(all(result[marker] == 2 for marker in module.MARKERS))

    def test_focused_historical_and_boolean_only_complete_claims_are_rejected(self):
        for change in ({"complete_matrix": False}, {"full_corpus": None},
                       {"full_corpus": {}, "complete_conformance": True}, {"spec_release": "master"}):
            original = copy.deepcopy(self.report)
            self.report.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.gate()
            self.report = original

    def test_failed_unsupported_and_setup_outcomes_cannot_be_expected(self):
        for outcome in ("failed", "unsupported"):
            self.report["cases"][0]["outcome"] = outcome
            self.report["expected_failures"] = [self.identities[0]]
            with self.subTest(outcome=outcome), self.assertRaisesRegex(ValueError, "did not pass"):
                self.gate()
        self.report["cases"][0]["outcome"] = "passed"
        self.report["errors"] = ["checksum error"]
        with self.assertRaisesRegex(ValueError, "setup or integrity"):
            self.gate()

    def test_incomplete_duplicate_and_substituted_results_are_rejected(self):
        original = copy.deepcopy(self.report["cases"])
        for cases in (original[:1], [original[0], original[0]],
                      [dict(original[0], path="minimal/gloas/other/handler/suite/case"), original[1]]):
            self.report["cases"] = cases
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                self.gate()
        self.report["cases"] = original

    def test_original_manifest_bytes_and_role_qualified_config_values_are_bound(self):
        self.paths["compliance"].write_bytes(self.paths["compliance"].read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "original bytes"):
            self.gate()
        self.write_manifest("compliance")
        self.manifests["compliance"]["files"]["minimal/config.yaml"] = "f" * 64
        self.write_manifest("compliance")
        with self.assertRaisesRegex(ValueError, "input/case identity"):
            self.gate()

    def test_missing_original_file_or_noncanonical_required_suite_is_rejected(self):
        del self.manifests["compliance"]["files"]["minimal/config.yaml"]
        self.write_manifest("compliance")
        with self.assertRaisesRegex(ValueError, "file count"):
            self.gate()
        self.manifests["compliance"]["files"]["minimal/config.yaml"] = "4" * 64
        self.manifests["compliance"]["required_suites"] = ["minimal/gloas/fork_choice_compliance/on_block"]
        self.write_manifest("compliance")
        with self.assertRaisesRegex(ValueError, "absent or noncanonical"):
            self.gate()

    def test_duplicate_source_case_is_rejected_even_with_two_valid_source_manifests(self):
        old = self.identities[1] + "/steps.yaml"
        self.manifests["compliance"]["files"][self.identities[0] + "/steps.yaml"] = self.manifests["compliance"]["files"].pop(old)
        self.manifests["compliance"]["required_suites"] = self.manifests["primary"]["required_suites"]
        self.write_manifest("compliance")
        with self.assertRaisesRegex(ValueError, "duplicate canonical case across source"):
            self.gate()

    def test_unauthenticated_archive_or_wrong_role_provenance_is_rejected(self):
        for field, value in (("corpus_role", "primary"), ("profile", "implemented"), ("extracted_files", 1)):
            original = copy.deepcopy(self.provenances["compliance"])
            self.provenances["compliance"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.gate()
            self.provenances["compliance"] = original
        self.provenances["primary"]["archives"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "archive digest"):
            self.gate()

    def test_reported_full_source_identity_and_order_are_mandatory(self):
        for field in ("case_ids_sha256", "input_files_sha256"):
            original = self.report["full_corpus"][field]
            self.report["full_corpus"][field] = "0" * 64
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.gate()
            self.report["full_corpus"][field] = original
        self.report["full_corpus"]["corpora"].reverse()
        with self.assertRaisesRegex(ValueError, "authentication evidence"):
            self.gate()

    def test_every_failure_exit_and_missing_assumption_marker_is_fatal(self):
        for status in (1, 2, 101, 137):
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, "did not exit successfully"):
                self.gate(status)
        del self.report["cases"][0][module.MARKERS[0]]
        with self.assertRaisesRegex(ValueError, "assumption marker"):
            self.gate()

    def test_lengths_and_domains_match_the_independent_rust_policy_golden(self):
        self.assertEqual(module.case_ids_digest(["ab", "c"]),
                         "37cf853c87720cc9b925621fa2e9d8fe372ee03fe681c4971817e9848dcc3cc6")
        self.assertNotEqual(module.case_ids_digest(["ab", "c"]), module.case_ids_digest(["a", "bc"]))
        digest = hashlib.sha256(module.FILE_DOMAIN)
        for role, value in ((b"primary", bytes(32)), (b"compliance", bytes([17]) * 32)):
            module.sized(digest, role)
            module.sized(digest, b"config.yaml")
            digest.update(value)
        self.assertEqual(digest.hexdigest(), "7f74ed15e61a32fd45d1b9becc91cad7166585765017569cb333150c84d04813")

    def test_checked_source_pins_match_the_frozen_complete_runner_identities(self):
        repository = Path(__file__).resolve().parents[3]
        self.assertEqual(module.DEFAULT_PINS, repository / "testing/embedded-beacon/spec-tests/full-corpus-pins.json")
        self.assertTrue((repository / "Cargo.toml").is_file())
        pins = module.read_json(module.DEFAULT_PINS)
        source = (module.DEFAULT_PINS.parent / "src/full_corpus.rs").read_text()
        for value in [pins["canonical_case_ids_sha256"], pins["canonical_input_files_sha256"],
                      *(pins["corpora"][role]["manifest_sha256"] for role in ("primary", "compliance"))]:
            self.assertIn('"0x' + value + '"', source)
        self.assertEqual(pins["required_case_count"], 73_992)
        self.assertEqual(pins["required_file_checks"], 1_296_962)


class CompleteJsonIntegrityContracts(unittest.TestCase):
    def test_duplicate_json_fields_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text('{"complete_matrix":false,"complete_matrix":true}')
            with self.assertRaisesRegex(ValueError, "duplicate JSON field"):
                module.read_json(path)


if __name__ == "__main__":
    unittest.main()
