"""Contracts for provider-selected checkpoint provenance, not live sync evidence."""

from copy import deepcopy
import json
from pathlib import Path
import unittest

import fetch_network_inputs as inputs
import hoodi_metadata


CATALOG = json.loads((Path(__file__).parent / "network-sources.json").read_bytes())
HOODI = CATALOG["networks"]["hoodi"]
BLOCK_ROOT = "0x" + "11" * 32
STATE_ROOT = "0x" + "22" * 32


def checkpoint_inputs():
    genesis = {"data": {key: str(HOODI[key]) for key in
                       ("genesis_time", "genesis_validators_root", "genesis_fork_version")}}
    finality = {"data": {"finalized": {"epoch": "12", "root": BLOCK_ROOT}}}
    block = bytearray(184)
    block[:4] = (100).to_bytes(4, "little")
    block[100:108] = (384).to_bytes(8, "little")
    block[148:180] = bytes.fromhex(STATE_ROOT[2:])
    return genesis, finality, block


class NetworkInputsContracts(unittest.TestCase):
    def test_original_prefix_records_exact_selected_epoch_root_and_state(self):
        genesis, finality, block = checkpoint_inputs()
        result = inputs.checked_checkpoint(HOODI, genesis, finality, block, "fulu")
        self.assertEqual(result["trusted_checkpoint"], "12:" + BLOCK_ROOT)
        self.assertEqual((result["block_slot"], result["boundary_slot"]), (384, 384))
        self.assertEqual(result["state_root"], STATE_ROOT)
        self.assertEqual(result["ssz_authentication"], "required_by_node")

    def test_a_checkpoint_block_before_the_epoch_boundary_keeps_both_slots(self):
        genesis, finality, block = checkpoint_inputs()
        block[100:108] = (383).to_bytes(8, "little")
        result = inputs.checked_checkpoint(HOODI, genesis, finality, block, "fulu")
        self.assertEqual((result["block_slot"], result["boundary_slot"]), (383, 384))

    def test_provider_genesis_cannot_select_a_different_network(self):
        genesis, finality, block = checkpoint_inputs()
        for key, value in (("genesis_time", "1"), ("genesis_validators_root", STATE_ROOT),
                           ("genesis_fork_version", "0x00000000")):
            changed = deepcopy(genesis)
            changed["data"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "^provider genesis differs"):
                inputs.checked_checkpoint(HOODI, changed, finality, block, "fulu")

    def test_checkpoint_epoch_rejects_zero_nondecimal_and_overflow(self):
        for value in ("0", "012", "-1", "12.0", str(2**64), 12, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                inputs.positive_decimal(value)

    def test_checkpoint_and_state_roots_reject_zero_or_malformed_values(self):
        for value in ("0x" + "00" * 32, "0x12", "head", 12, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                inputs.root(value)
        genesis, finality, block = checkpoint_inputs()
        block[148:180] = bytes(32)
        with self.assertRaises(ValueError):
            inputs.checked_checkpoint(HOODI, genesis, finality, block, "fulu")

    def test_incomplete_invalid_prefix_or_later_block_cannot_be_used(self):
        genesis, finality, block = checkpoint_inputs()
        wrong_offset = bytearray(block)
        wrong_offset[:4] = (101).to_bytes(4, "little")
        future = bytearray(block)
        future[100:108] = (385).to_bytes(8, "little")
        for changed in (block[:183], wrong_offset, future):
            with self.subTest(bytes=len(changed)), self.assertRaises(ValueError):
                inputs.checked_checkpoint(HOODI, genesis, finality, changed, "fulu")

    def test_missing_unknown_or_wrong_case_version_is_rejected(self):
        genesis, finality, block = checkpoint_inputs()
        for version in (None, "future", "Fulu"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                inputs.checked_checkpoint(HOODI, genesis, finality, block, version)

    def test_official_hoodi_configuration_is_checksum_pinned_with_announced_activation(self):
        reviewed = HOODI["reviewed_config"]
        raw = (Path(__file__).parent / reviewed["fixture"]).read_bytes()
        self.assertEqual(inputs.hashlib.sha256(raw).hexdigest(), reviewed["sha256"])
        config = hoodi_metadata.config_scalars(raw)
        self.assertEqual(config["GLOAS_FORK_VERSION"], "0x80000910")
        self.assertEqual(config["GLOAS_FORK_EPOCH"], "132352")
        self.assertEqual(hoodi_metadata.GENESIS_TIME + int(config["GLOAS_FORK_EPOCH"]) * 32 * 12,
                         reviewed["gloas_activation_timestamp"])
        self.assertEqual(reviewed["gloas_activation_timestamp"], 1793036568)
        self.assertEqual(hoodi_metadata.config_schedules(raw)["GAS_LIMIT_SCHEDULE"],
                         [{"EPOCH": 132352, "GAS_LIMIT": 200000000}])
        self.assertIs(reviewed["gas_limit_schedule_advisory"], True)

    def test_stale_missing_or_conflicting_announced_gloas_fields_are_rejected(self):
        raw = (Path(__file__).parent / HOODI["reviewed_config"]["fixture"]).read_bytes()
        for changed in (
                raw.replace(b"GLOAS_FORK_EPOCH: 132352", b"GLOAS_FORK_EPOCH: 18446744073709551615"),
                raw.replace(b"GLOAS_FORK_VERSION: 0x80000910", b"GLOAS_FORK_VERSION: 0x70000910"),
                raw.replace(b"GLOAS_FORK_EPOCH: 132352\n", b"")):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                hoodi_metadata.config_scalars(changed)

    def test_changed_duplicate_or_malformed_network_schedule_is_rejected(self):
        raw = (Path(__file__).parent / HOODI["reviewed_config"]["fixture"]).read_bytes()
        for changed in (
                raw.replace(b"MAX_BLOBS_PER_BLOCK: 21", b"MAX_BLOBS_PER_BLOCK: 20"),
                raw.replace(b"GAS_LIMIT: 200000000", b"GAS_LIMIT: 199999999"),
                raw + b"GAS_LIMIT_SCHEDULE:\n  - EPOCH: 132352\n    GAS_LIMIT: 200000000\n",
                raw.replace(b"    GAS_LIMIT: 200000000", b"    UNUSED: 200000000")):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                hoodi_metadata.config_scalars(changed)

    def test_catalog_preserves_mainnet_provider_access_requirement(self):
        self.assertEqual(set(CATALOG["networks"]), {"hoodi", "mainnet", "sepolia"})
        self.assertEqual(CATALOG["networks"]["mainnet"]["checkpoint_access"], "provider_api_key_required")
        self.assertEqual(CATALOG["networks"]["hoodi"]["checkpoint_access"], "public")
        self.assertEqual(CATALOG["networks"]["hoodi"]["registry"]["repository"], "eth-clients/hoodi")


if __name__ == "__main__":
    unittest.main()
