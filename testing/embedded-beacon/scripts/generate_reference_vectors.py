"""Generate structural SSZ fixtures using the pinned independent reference package.

Run with Python >=3.12 and eth-ssz-specs==0.1.0. These fixtures are generated,
rather than published consensus transition vectors. Schema declarations match
consensus-specs v1.7.0-beta.3. No full state transition is performed.
"""

import json
from importlib.metadata import version
from pathlib import Path

from ssz import (
    BitVector, Boolean, Byte, ByteList, ByteVector, Container, List,
    ProgressiveContainer, ProgressiveList, Uint64, Uint256, active_fields,
)

assert version("eth-ssz-specs") == "0.1.0"


class Bytes20(ByteVector):
    LENGTH = 20


class Bytes32(ByteVector):
    LENGTH = 32


class Bytes48(ByteVector):
    LENGTH = 48


class Bytes96(ByteVector):
    LENGTH = 96


class LogsBloom(ByteVector):
    LENGTH = 256


class ExtraData(ByteList):
    LIMIT = 32


class PTCBits(BitVector):
    LENGTH = 512


class BlobCommitments(ProgressiveList[Bytes48]):
    pass


class ExecutionPayloadBid(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=12)
    parent_block_hash: Bytes32
    parent_block_root: Bytes32
    block_hash: Bytes32
    prev_randao: Bytes32
    fee_recipient: Bytes20
    gas_limit: Uint64
    builder_index: Uint64
    slot: Uint64
    value: Uint64
    execution_payment: Uint64
    blob_kzg_commitments: BlobCommitments
    execution_requests_root: Bytes32


class SignedExecutionPayloadBid(Container):
    message: ExecutionPayloadBid
    signature: Bytes96


class PayloadAttestationData(Container):
    beacon_block_root: Bytes32
    slot: Uint64
    payload_present: Boolean
    blob_data_available: Boolean


class PayloadAttestation(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=3)
    aggregation_bits: PTCBits
    data: PayloadAttestationData
    signature: Bytes96


class PTCBitsMinimal(BitVector):
    LENGTH = 16


class PayloadAttestationMinimal(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=3)
    aggregation_bits: PTCBitsMinimal
    data: PayloadAttestationData
    signature: Bytes96


class PTCIndices(List[Uint64]):
    LIMIT = 512


class PTCIndicesMinimal(List[Uint64]):
    LIMIT = 16


class IndexedPayloadAttestation(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=3)
    attesting_indices: PTCIndices
    data: PayloadAttestationData
    signature: Bytes96


class IndexedPayloadAttestationMinimal(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=3)
    attesting_indices: PTCIndicesMinimal
    data: PayloadAttestationData
    signature: Bytes96


class Transaction(ProgressiveList[Byte]):
    pass


class Transactions(ProgressiveList[Transaction]):
    pass


class Withdrawal(Container):
    index: Uint64
    validator_index: Uint64
    address: Bytes20
    amount: Uint64


class Withdrawals(ProgressiveList[Withdrawal]):
    pass


class BlockAccessList(ProgressiveList[Byte]):
    pass


class ExecutionPayload(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=19)
    parent_hash: Bytes32
    fee_recipient: Bytes20
    state_root: Bytes32
    receipts_root: Bytes32
    logs_bloom: LogsBloom
    prev_randao: Bytes32
    block_number: Uint64
    gas_limit: Uint64
    gas_used: Uint64
    timestamp: Uint64
    extra_data: ExtraData
    base_fee_per_gas: Uint256
    block_hash: Bytes32
    transactions: Transactions
    withdrawals: Withdrawals
    blob_gas_used: Uint64
    excess_blob_gas: Uint64
    block_access_list: BlockAccessList
    slot_number: Uint64


class DepositRequest(Container):
    pubkey: Bytes48
    withdrawal_credentials: Bytes32
    amount: Uint64
    signature: Bytes96
    index: Uint64


class WithdrawalRequest(Container):
    source_address: Bytes20
    validator_pubkey: Bytes48
    amount: Uint64


class ConsolidationRequest(Container):
    source_address: Bytes20
    source_pubkey: Bytes48
    target_pubkey: Bytes48


class BuilderDepositRequest(Container):
    pubkey: Bytes48
    withdrawal_credentials: Bytes32
    amount: Uint64
    signature: Bytes96


class BuilderExitRequest(Container):
    source_address: Bytes20
    pubkey: Bytes48


class DepositRequests(ProgressiveList[DepositRequest]):
    pass


class WithdrawalRequests(ProgressiveList[WithdrawalRequest]):
    pass


class ConsolidationRequests(ProgressiveList[ConsolidationRequest]):
    pass


class BuilderDepositRequests(ProgressiveList[BuilderDepositRequest]):
    pass


class BuilderExitRequests(ProgressiveList[BuilderExitRequest]):
    pass


class ExecutionRequests(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=5)
    deposits: DepositRequests
    withdrawals: WithdrawalRequests
    consolidations: ConsolidationRequests
    builder_deposits: BuilderDepositRequests
    builder_exits: BuilderExitRequests


class ExecutionPayloadEnvelope(ProgressiveContainer):
    ACTIVE_FIELDS = active_fields(width=5)
    payload: ExecutionPayload
    execution_requests: ExecutionRequests
    builder_index: Uint64
    beacon_block_root: Bytes32
    parent_beacon_block_root: Bytes32


class SignedExecutionPayloadEnvelope(Container):
    message: ExecutionPayloadEnvelope
    signature: Bytes96


def main():
    output = Path(__file__).resolve().parents[3] / "crates/embedded-beacon/primitives/testdata/generated-gloas"
    output.mkdir(parents=True, exist_ok=True)
    bid = ExecutionPayloadBid(
        parent_block_hash=bytes([1]) * 32, parent_block_root=bytes([2]) * 32,
        block_hash=bytes([3]) * 32, prev_randao=bytes([4]) * 32,
        fee_recipient=bytes([5]) * 20, gas_limit=60_000_000, builder_index=7, slot=42,
        value=8, execution_payment=9, blob_kzg_commitments=BlobCommitments(data=[bytes([6]) * 48]),
        execution_requests_root=bytes([10]) * 32,
    )
    vote_data = PayloadAttestationData(
        beacon_block_root=bytes([1]) * 32, slot=9, payload_present=True, blob_data_available=False,
    )
    payload = ExecutionPayload(
        parent_hash=bytes([1]) * 32, fee_recipient=bytes([2]) * 20,
        state_root=bytes([3]) * 32, receipts_root=bytes([4]) * 32, prev_randao=bytes([5]) * 32,
        block_number=7, gas_limit=60_000_000, gas_used=21000, timestamp=1742213412,
        extra_data=ExtraData(data=[1, 2, 3]), base_fee_per_gas=42, block_hash=bytes([6]) * 32,
        transactions=Transactions(data=[Transaction(data=[2, 1]), Transaction(data=[4, 2, 3])]),
        withdrawals=Withdrawals(data=[Withdrawal()]), block_access_list=BlockAccessList(data=[4, 5]), slot_number=1,
    )
    requests = ExecutionRequests(
        deposits=DepositRequests.decode_bytes(bytes([1]) * 192),
        withdrawals=WithdrawalRequests.decode_bytes(bytes([2]) * 76),
        consolidations=ConsolidationRequests.decode_bytes(bytes([3]) * 116),
        builder_deposits=BuilderDepositRequests.decode_bytes(bytes([4]) * 184),
        builder_exits=BuilderExitRequests.decode_bytes(bytes([5]) * 68),
    )
    envelope = ExecutionPayloadEnvelope(
        payload=payload, execution_requests=requests, builder_index=9,
        beacon_block_root=bytes([7]) * 32, parent_beacon_block_root=bytes([8]) * 32,
    )
    cases = {
        "bid": bid,
        "signed-bid": SignedExecutionPayloadBid(message=bid, signature=bytes([11]) * 96),
        "ptc-data": vote_data,
        "ptc-vote": PayloadAttestation(
            aggregation_bits=PTCBits.decode_bytes(bytes([1]) * 64),
            data=vote_data, signature=bytes([2]) * 96,
        ),
        "ptc-vote-minimal": PayloadAttestationMinimal(
            aggregation_bits=PTCBitsMinimal.decode_bytes(bytes([1]) * 2),
            data=vote_data, signature=bytes([2]) * 96,
        ),
        "indexed-ptc": IndexedPayloadAttestation(
            attesting_indices=PTCIndices(data=[0, 5]), data=vote_data, signature=bytes([2]) * 96,
        ),
        "indexed-ptc-minimal": IndexedPayloadAttestationMinimal(
            attesting_indices=PTCIndicesMinimal(data=[0, 5]), data=vote_data, signature=bytes([2]) * 96,
        ),
        "payload": payload,
        "requests": requests,
        "envelope": envelope,
        "signed-envelope": SignedExecutionPayloadEnvelope(message=envelope, signature=bytes([9]) * 96),
    }
    metadata = {
        "kind": "generated structural SSZ fixtures",
        "consensus_spec_pin": "v1.7.0-beta.3",
        "reference_ssz_package": "eth-ssz-specs==0.1.0",
        "preset": "mainnet",
        "cases": {},
    }
    for name, value in cases.items():
        encoded = value.encode_bytes()
        root = bytes(value.hash_tree_root())
        (output / f"{name}.ssz").write_bytes(encoded)
        (output / f"{name}.root").write_bytes(root)
        metadata["cases"][name] = {"root": "0x" + root.hex(), "ssz_length": len(encoded),
                                   "preset": "minimal" if name.endswith("-minimal") else "mainnet"}
    (output / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
