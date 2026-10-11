"""Public observation contract tests; these are not live public synchronization evidence."""

from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from contracts import CheckError, NotReady
from observe_public import caught_up, load_restart, main, observe, verify_restart
from test_verify import HASH, ROOT, STATE, PARENT, SIGNATURE, block_response, config_routes, endpoint, execution_block, independent_fixture, payload, receipt, rpc_route, server
from execution import block_at, compare_execution
from verify import READINESS_FIELDS


class LiveNode:
    def __init__(self):
        self.responses = {
            "/eth/v1/node/syncing": {"data": {"is_syncing": False, "is_optimistic": False, "el_offline": False, "head_slot": "10", "sync_distance": "0"}},
            "/eth/v1/beacon/headers/head": {"execution_optimistic": False, "data": {"canonical": True, "root": ROOT, "header": {"message": {"slot": "10"}}}},
            "/eth/v1/node/peer_count": {"data": {"connected": "1"}},
        }
        self.block = {"number": "0x9", "hash": HASH, "timestamp": "0xa0"}

    def beacon(self, path):
        return deepcopy(self.responses[path])

    def rpc(self, method, params):
        return deepcopy(self.block)


class PublicObservationContracts(unittest.TestCase):
    def setUp(self):
        self.node = LiveNode()
        self.config = {"slot_duration_ms": 6000, "slots_per_epoch": 32, "genesis": {"genesis_time": 100}, "forks": [{"name": "gloas", "epoch": 0}]}

    def test_peer_connections_alone_cannot_hide_stale_beacon_or_execution(self):
        result = caught_up(self.node, self.node, self.config, 4, 160)
        self.assertEqual((result["beacon_slot"], result["execution_height"], result["fork"]), (10, 9, "gloas"))
        for now, timestamp in ((190, "0xa0"), (160, "0x64")):
            self.node.block["timestamp"] = timestamp
            with self.subTest(now=now), self.assertRaises(NotReady):
                caught_up(self.node, self.node, self.config, 4, now)

    def test_missing_sync_flags_or_optimistic_head_cannot_receive_acceptance(self):
        for key in ("is_syncing", "is_optimistic", "el_offline"):
            original = deepcopy(self.node.responses)
            self.node.responses["/eth/v1/node/syncing"]["data"][key] = True
            with self.subTest(key=key), self.assertRaises(NotReady):
                caught_up(self.node, self.node, self.config, 4, 160)
            del self.node.responses["/eth/v1/node/syncing"]["data"][key]
            with self.assertRaises(CheckError):
                caught_up(self.node, self.node, self.config, 4, 160)
            self.node.responses = original
        self.node.responses["/eth/v1/beacon/headers/head"]["execution_optimistic"] = True
        with self.assertRaises(NotReady):
            caught_up(self.node, self.node, self.config, 4, 160)

    def test_restart_artifact_requires_exact_completed_original_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "previous.json"
            path.write_text(json.dumps({"schema_version": 1, "status": "passed", "public_network_sync_acceptance": True}))
            checksum = "0x" + hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(load_restart(path, checksum)["status"], "passed")
            for digest in (None, "0x" + "00" * 32):
                with self.subTest(digest=digest), self.assertRaises(CheckError):
                    load_restart(path, digest)
            path.write_bytes(path.read_bytes() + b" ")
            with self.assertRaises(CheckError):
                load_restart(path, checksum)


    def test_public_http_flow_requires_all_original_links_and_real_advancement_conditions(self):
        heads = {"sut-beacon": 0, "reference-beacon": 0}
        routes = config_routes([0] * 8)
        routes["/eth/v1/config/spec"]["data"]["SLOT_DURATION_MS"] = "6000"
        routes["/eth/v1/beacon/genesis"]["data"]["genesis_time"] = "100"

        def number_hash(value):
            return "0x" + value.to_bytes(32, "big").hex()

        def block(height):
            value = execution_block(height)
            value["hash"] = number_hash(height + 20)
            value["transactions"] = [] if height == 0 else [number_hash(height)]
            value["timestamp"] = "0xa0"
            return value

        def route(role, path, request):
            if request is not None:
                method, params = request["method"], request["params"]
                if method == "eth_chainId":
                    result = hex(560048)
                elif method == "eth_getBlockByNumber":
                    current = 9 + int(min(heads.values()) >= 1)
                    result = block(current if params[0] in ("finalized", "latest") else int(params[0], 16))
                elif method == "eth_getRawTransactionByHash":
                    result = "0x01"
                else:
                    height = int(params[0], 16)
                    result = receipt(height)
                    result["transactionHash"] = number_hash(height)
                    result["blockHash"] = number_hash(height + 20)
                return 200, {"jsonrpc": "2.0", "id": request["id"], "result": result}
            if path in routes:
                return 200, routes[path]
            current_head = 10 + int(min(heads.values()) >= 1)
            if path == "/eth/v1/node/syncing":
                return 200, {"data": {"is_syncing": False, "is_optimistic": False, "el_offline": False, "head_slot": str(current_head), "sync_distance": "0"}}
            if path == "/eth/v1/node/peer_count":
                return 200, {"data": {"connected": "1"}}
            if "/headers/" in path:
                requested = path.rsplit("/", 1)[1]
                if requested == "finalized":
                    heads[role] += 1
                    slot = min(heads[role], 2)
                else:
                    slot = current_head if requested == "head" else int(requested)
                return 200, {"execution_optimistic": False, "data": {"canonical": True, "root": number_hash(slot + 100), "header": {"message": {"slot": str(slot), "state_root": STATE}}}}
            if "/blocks/" in path:
                slot = int(path.rsplit("/", 1)[1])
                value = block_response("gloas", slot)
                value["data"]["message"]["body"]["execution_payload_bid"]["block_hash"] = number_hash(slot + 26)
                return 200, value
            if "/states/" in path:
                return 200, {"data": {"root": STATE}, "execution_optimistic": False, "finalized": True}
            if "/execution_payload_envelopes/" in path:
                slot = int(path.rsplit("/", 1)[1], 16) - 100
                value = payload("gloas", slot + 6)
                value.update({"block_hash": number_hash(slot + 26), "slot_number": str(slot), "timestamp": "160"})
                return 200, {"version": "gloas", "execution_optimistic": False, "finalized": True, "data": {"signature": SIGNATURE, "message": {"beacon_block_root": number_hash(slot + 100), "execution_requests": {}, "payload": value}}}
            return 404, {}

        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            urls = {}
            for key, role in (("sut_execution", "sut-execution"), ("reference_execution", "reference-execution"), ("sut_beacon", "sut-beacon"), ("reference_beacon", "reference-beacon")):
                url, _ = stack.enter_context(server(lambda path, request, role=role: route(role, path, request)))
                urls[key] = url
            expected, _, inspect = independent_fixture(urls)
            path = Path(directory) / "deployment.json"
            path.write_text(json.dumps(expected))
            args = SimpleNamespace(independent_runtime_provenance=str(path), sut_beacon_url=urls["sut_beacon"], reference_beacon_url=urls["reference_beacon"], sut_el_url=urls["sut_execution"], reference_el_url=urls["reference_execution"], resume_report=None, resume_report_sha256=None, timeout_seconds=2, poll_interval=0.001, network="hoodi", expected_execution_genesis_hash=number_hash(20), expected_genesis_validators_root=ROOT, max_head_lag_slots=4, expected_live_fork="gloas", min_finalized_blocks=1, output_dir=directory)
            report = {"public_network_sync_acceptance": False}
            with patch("provenance.docker_output", side_effect=inspect), patch("observe_public.time.time", return_value=160):
                observe(args, report)
            self.assertTrue(report["public_network_sync_acceptance"])
            self.assertFalse(report["restart_resume_acceptance"])
            self.assertEqual(report["finalized_anchor"]["beacon_slot"], 2)
            self.assertEqual(report["finalized_execution"]["gas_used"], 21000)
            self.assertGreater(report["live_head"]["beacon_slot"], report["initial_observation"]["live_beacon_slot"])
            self.assertGreater(report["live_head"]["execution_height"], report["initial_observation"]["live_execution_height"])

    def test_saved_restart_anchors_must_match_live_original_network(self):
        previous = {"execution_network": {"chain_id": 1}, "network": self.config}
        with self.assertRaisesRegex(CheckError, "^restarted node changed its original public network identity$"):
            verify_restart(previous, {"chain_id": 560048}, self.config, None, None, None, None)

    def test_false_readiness_blocks_before_http_or_deployment_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            readiness = root / "readiness.json"
            readiness.write_text(json.dumps({"target_fork": "gloas", **{key: False for key in READINESS_FIELDS}}))
            with patch("observe_public.deployment_provenance", side_effect=AssertionError("must not inspect")), patch("observe_public.HttpTransport", side_effect=AssertionError("must not connect")):
                status = main(["--network", "hoodi", "--sut-beacon-url", "http://127.0.0.1:1", "--reference-beacon-url", "http://127.0.0.1:2", "--sut-el-url", "http://127.0.0.1:3", "--reference-el-url", "http://127.0.0.1:4", "--expected-execution-genesis-hash", HASH, "--expected-genesis-validators-root", ROOT, "--readiness-file", str(readiness), "--independent-runtime-provenance", str(root / "absent"), "--output-dir", str(root / "out")])
            self.assertEqual(status, 2)
            result = json.loads((root / "out/verification.json").read_text())
            self.assertFalse(result["public_network_sync_acceptance"])


class ExecutionDataContracts(unittest.TestCase):
    def test_gas_and_raw_transaction_data_are_checked_against_original_payload(self):
        with server(rpc_route) as (url, calls):
            result = compare_execution(endpoint(url + "/sut"), endpoint(url + "/reference"), 7, payload())
        self.assertEqual((result["gas_limit"], result["gas_used"], result["blob_gas_used"]), (30000000, 21000, 0))
        self.assertEqual(result["transaction_data_sha256"], [hashlib.sha256(bytes([1])).hexdigest()])
        self.assertEqual(sum(call[1]["method"] == "eth_getRawTransactionByHash" for call in calls), 2)
        self.assertFalse(result["independent_transaction_hash_computation"])

    def test_equal_hashes_do_not_hide_gas_or_blob_field_disagreements(self):
        for field, value in (("gasLimit", "0x1c9c381"), ("gasUsed", "0x5209"), ("blobGasUsed", "0x20000")):
            def route(path, request):
                status, response = rpc_route(path, request)
                if path == "/sut" and request["method"] == "eth_getBlockByNumber":
                    response["result"][field] = value
                return status, response
            with self.subTest(field=field), server(route) as (url, _), self.assertRaises(CheckError):
                compare_execution(endpoint(url + "/sut"), endpoint(url + "/reference"), 7, payload())

    def test_equal_endpoints_cannot_hide_wrong_beacon_data_or_cumulative_gas(self):
        for method, field, value in (("eth_getRawTransactionByHash", None, "0x02"), ("eth_getTransactionReceipt", "cumulativeGasUsed", "0x5209")):
            def route(path, request):
                status, response = rpc_route(path, request)
                if request["method"] == method:
                    if field is None:
                        response["result"] = value
                    else:
                        response["result"][field] = value
                return status, response
            with self.subTest(method=method), server(route) as (url, _), self.assertRaises(CheckError):
                compare_execution(endpoint(url), endpoint(url), 7, payload())

    def test_pre_london_genesis_does_not_require_a_base_fee(self):
        def route(path, request):
            block = execution_block(0)
            del block["baseFeePerGas"]
            return 200, {"jsonrpc": "2.0", "id": request["id"], "result": block}
        with server(route) as (url, _):
            self.assertEqual(block_at(endpoint(url), 0)["number"], "0x0")


if __name__ == "__main__":
    unittest.main()
