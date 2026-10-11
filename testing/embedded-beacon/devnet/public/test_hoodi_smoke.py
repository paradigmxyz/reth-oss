"""Cheap source contracts only; synthetic API replies do not prove BLS, EL or live sync."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
from pathlib import Path
import signal
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import hoodi_metadata
import hoodi_smoke as smoke
from contracts import CheckError, NotReady


def root(value):
    return "0x" + value.to_bytes(32, "big").hex()


class FixtureApis:
    def __init__(self, count=7):
        self.headers, self.blocks, self.execution = {}, {}, {}
        for slot in range(1, count + 1):
            message = {"slot": str(slot), "proposer_index": "0", "parent_root": root(slot - 1), "state_root": root(slot + 100)}
            self.headers[root(slot)] = {"execution_optimistic": False, "data": {
                "canonical": True, "root": root(slot), "header": {"message": message}}}
            payload = {"block_number": str(slot), "block_hash": root(slot + 200), "parent_hash": root(slot + 199),
                       "state_root": root(slot + 300), "receipts_root": root(slot + 400), "gas_limit": "60000000",
                       "gas_used": "21000", "timestamp": str(slot + 1000), "base_fee_per_gas": "7",
                       "blob_gas_used": "0", "excess_blob_gas": "0"}
            self.blocks[root(slot)] = {"version": "fulu", "execution_optimistic": False, "data": {
                "signature": "0x" + "22" * 96, "message": {**message, "body": {"execution_payload": payload}}}}
            self.execution[slot] = {"number": hex(slot), "hash": payload["block_hash"], "parentHash": payload["parent_hash"],
                                    "stateRoot": payload["state_root"], "receiptsRoot": payload["receipts_root"],
                                    "gasLimit": hex(60000000), "gasUsed": hex(21000), "timestamp": hex(slot + 1000),
                                    "baseFeePerGas": "0x7", "blobGasUsed": "0x0", "excessBlobGas": "0x0", "transactions": []}

    def beacon(self, path):
        if "/headers/" in path:
            identity = path.rsplit("/", 1)[1]
            return deepcopy(self.headers[identity])
        if "/blocks/" in path:
            return deepcopy(self.blocks[path.rsplit("/", 1)[1]])
        return {"data": {"root": path.split("/")[-2]}}

    def rpc(self, method, params):
        return deepcopy(self.execution[int(params[0], 16)])


class SmokeContracts(unittest.TestCase):
    def test_source_pins_and_testing_artifacts_follow_the_split_layout(self):
        repository = Path(__file__).resolve().parents[4]
        self.assertEqual(smoke.ROOT, repository)
        self.assertEqual(smoke.TESTING_ROOT, repository / "testing/embedded-beacon")
        self.assertEqual(smoke.parser().get_default("artifact_root"), smoke.TESTING_ROOT / "artifacts/hoodi-smoke")
        self.assertTrue((smoke.TESTING_ROOT / "spec-tests/full-corpus-pins.json").is_file())
        self.assertTrue((smoke.TESTING_ROOT / "scripts/check-complete-consensus-fixture-results.py").is_file())

    def test_trusted_checkpoint_requires_explicit_nonzero_epoch_and_root(self):
        self.assertEqual(smoke.checkpoint("12:" + root(99))["epoch"], 12)
        for value in ("head", "12", "0:" + root(1), "12:" + root(0), "012:" + root(1), str(2**64) + ":" + root(1)):
            with self.subTest(value=value), self.assertRaises(CheckError):
                smoke.checkpoint(value)

    def test_argv_uses_actual_checkpoint_flags_and_minimal_without_genesis_or_private_network(self):
        args = SimpleNamespace(checkpoint_url="https://example.invalid/checked-artifacts", trusted_checkpoint="12:" + root(99),
                               advertised_ip="8.8.8.8", peer_port=19000, quic_port=19001, beacon_port=15052,
                               rpc_port=18545, execution_port=19303)
        command = smoke.argv(args, Path("/verified/binary"), Path("/fresh/owned"), Path("/frozen/seeds"))
        for flag in ("--minimal", "--embedded-beacon", "--beacon.trusted-checkpoint", "--beacon.checkpoint-url"):
            self.assertEqual(command.count(flag), 1)
        self.assertEqual(command[command.index("--chain") + 1], "hoodi")
        self.assertEqual(command[command.index("--beacon.sync-mode") + 1], "checkpoint")
        self.assertNotIn("--beacon.genesis", command)
        self.assertNotIn("--beacon.devnet-config", command)
        self.assertEqual(command[command.index("--beacon.bind-ip") + 1], "0.0.0.0")
        self.assertEqual(command[command.index("--port") + 1], "19303")
        self.assertEqual(command[command.index("--discovery.port") + 1], "19303")
        self.assertEqual(command[command.index("--discovery.v5.port") + 1], "19304")
        self.assertIn("--disable-auth-server", command)
        self.assertIn("--ipcdisable", command)
        args.advertised_ip = "2606:4700:4700::1111"
        ipv6 = smoke.argv(args, Path("/verified/binary"), Path("/fresh/owned"), Path("/frozen/seeds"))
        self.assertEqual(ipv6[ipv6.index("--beacon.bind-ip") + 1], "::")
        self.assertEqual(ipv6[ipv6.index("--beacon.listen-ip") + 1], args.advertised_ip)
        self.assertEqual(ipv6[ipv6.index("--beacon.http-addr") + 1], "127.0.0.1")

    def test_exact_canonical_payload_includes_actual_hash_and_gas_limit_crosscheck(self):
        apis = FixtureApis()
        current = smoke.header(apis, root(2))
        record = smoke.checked_block(apis, apis, current)
        self.assertEqual((record["execution_height"], record["execution_hash"], record["gas_limit"]), (2, root(202), 60000000))
        for field, value in (("gasLimit", "0x1"), ("hash", root(800)), ("parentHash", root(900))):
            saved = deepcopy(apis.execution[2])
            apis.execution[2][field] = value
            with self.subTest(field=field), self.assertRaises(CheckError):
                smoke.checked_block(apis, apis, current)
            apis.execution[2] = saved

    def test_five_actual_original_additions_are_walked_not_slot_difference_credit(self):
        apis = FixtureApis()
        previous = smoke.checked_block(apis, apis, smoke.header(apis, root(1)))
        records = smoke.additions(apis, apis, previous, smoke.header(apis, root(6)))
        self.assertEqual(len(records), smoke.BLOCK_LIMIT)
        self.assertEqual([record["beacon_root"] for record in records], [root(n) for n in range(2, 7)])
        self.assertEqual([record["execution_height"] for record in records], list(range(2, 7)))

    def test_optimistic_noncanonical_or_foreign_retained_root_never_counts(self):
        apis = FixtureApis()
        apis.headers[root(2)]["execution_optimistic"] = True
        with self.assertRaises(NotReady):
            smoke.header(apis, root(2))
        apis.headers[root(2)]["execution_optimistic"] = False
        apis.headers[root(2)]["data"]["canonical"] = False
        with self.assertRaises(NotReady):
            smoke.header(apis, root(2))
        apis.headers[root(2)]["data"]["canonical"] = True
        apis.headers[root(2)]["data"]["root"] = root(3)
        with self.assertRaises(CheckError):
            smoke.header(apis, root(2))

    def test_real_ancestry_checks_reject_reorg_cycles_and_execution_height_jumps(self):
        apis = FixtureApis()
        previous = smoke.checked_block(apis, apis, smoke.header(apis, root(1)))
        for parent in (root(3), root(0)):
            apis.headers[root(3)]["data"]["header"]["message"]["parent_root"] = parent
            apis.blocks[root(3)]["data"]["message"]["parent_root"] = parent
            with self.subTest(parent=parent), self.assertRaises((CheckError, KeyError)):
                smoke.additions(apis, apis, previous, smoke.header(apis, root(3)))

    def test_execution_gap_cannot_be_hidden_by_matching_payload_and_execution_apis(self):
        apis = FixtureApis()
        previous = smoke.checked_block(apis, apis, smoke.header(apis, root(1)))
        apis.blocks[root(3)]["data"]["message"]["body"]["execution_payload"] = deepcopy(
            apis.blocks[root(4)]["data"]["message"]["body"]["execution_payload"])
        with self.assertRaises(CheckError):
            smoke.additions(apis, apis, previous, smoke.header(apis, root(3)))

    def test_full_gate_rejection_precedes_binary_execution_and_node_process(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(artifact_root=Path(directory))
            with patch.object(smoke, "parser") as parser, patch.object(smoke, "checked_inputs", return_value=({}, {})), \
                    patch.object(smoke, "full_conformance", side_effect=CheckError("original corpus incomplete")), \
                    patch.object(smoke, "compiled_profile") as profile, patch.object(smoke.subprocess, "Popen") as launch, \
                    redirect_stdout(io.StringIO()):
                parser.return_value.parse_args.return_value = args
                self.assertEqual(smoke.main(), 1)
            profile.assert_not_called()
            launch.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_shutdown_only_signals_the_owned_handle_and_records_actual_graceful_exit(self):
        child = SimpleNamespace(returncode=None, poll=lambda: None, sent=[])
        child.send_signal = child.sent.append
        child.wait = lambda timeout: 0
        self.assertEqual(smoke.stop_owned(child), {"graceful": True, "signal": "SIGINT", "exit_code": 0})
        self.assertEqual(child.sent, [signal.SIGINT])


class MetadataContracts(unittest.TestCase):
    def test_original_signed_record_text_is_preserved_without_claiming_crypto_authority(self):
        raw = b"# official inventory\n- enr:abcdef_-012 # actual YAML inline comment\n- enr:another-record\n"
        self.assertEqual(hoodi_metadata.extract_seeds(raw), b"enr:abcdef_-012\nenr:another-record\n")
        for raw in (b"", b"- enr:abc\n- enr:abc\n", b"- http://unrelated.invalid\n", b"- enr:bad+encoding\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                hoodi_metadata.extract_seeds(raw)

    def test_consensus_metadata_cannot_turn_minimal_resource_profile_into_minimal_preset(self):
        raw = (Path(__file__).parent / "testdata/hoodi-config.yaml").read_bytes()
        self.assertEqual(hoodi_metadata.config_scalars(raw)["PRESET_BASE"], "mainnet")
        for changed in (raw.replace(b"mainnet", b"minimal"), raw.replace(b"560048", b"1"), raw + b"CONFIG_NAME: other\n"):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                hoodi_metadata.config_scalars(changed)


if __name__ == "__main__":
    unittest.main()
