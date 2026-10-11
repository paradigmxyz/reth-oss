"""Same-height finalized execution block and receipt comparisons."""

import hashlib
import json
from beacon import variable_bytes
from contracts import CheckError, NotReady, RpcFailure, array, decimal, hex_bytes, normalized, obj, quantity


def finalized_el(endpoint):
    try:
        block = endpoint.rpc("eth_getBlockByNumber", ["finalized", False])
    except RpcFailure as exc:
        # A historical pre-execution network may not have an EL finalized tag yet.
        if exc.code in (-32000, -32001):
            raise NotReady(f"{endpoint.role} has no finalized execution block yet") from None
        raise
    if block is None:
        raise NotReady(f"{endpoint.role} has no finalized execution block yet")
    return quantity(obj(block, "finalized execution block").get("number"), "finalized execution height")


def block_at(endpoint, height):
    block = endpoint.rpc("eth_getBlockByNumber", [hex(height), False])
    if block is None:
        raise NotReady(f"{endpoint.role} missing requested finalized execution block")
    block = obj(block, "execution block")
    if quantity(block.get("number"), "execution block height") != height:
        raise CheckError("execution endpoint returned a different requested block height")
    for field in ("hash", "parentHash", "stateRoot", "receiptsRoot"):
        hex_bytes(block.get(field), 32, f"execution block {field}")
    for field in ("gasLimit", "gasUsed", "timestamp"):
        quantity(block.get(field), f"execution block {field}")
    if quantity(block["gasUsed"], "execution gas used") > quantity(block["gasLimit"], "execution gas limit"):
        raise CheckError("execution block gas exceeds its limit")
    for field in ("baseFeePerGas", "blobGasUsed", "excessBlobGas"):
        if field in block:
            quantity(block[field], f"execution block {field}")
    if ("blobGasUsed" in block) != ("excessBlobGas" in block):
        raise CheckError("execution block has incomplete blob gas fields")
    transactions = array(block.get("transactions"), "execution block transactions")
    for tx in transactions:
        hex_bytes(tx, 32, "transaction hash")
    if len(set(normalized(transactions))) != len(transactions):
        raise CheckError("execution block repeats a transaction hash")
    return block


def receipt_at(endpoint, tx, block, index):
    receipt = endpoint.rpc("eth_getTransactionReceipt", [tx])
    if receipt is None:
        raise NotReady(f"{endpoint.role} missing a finalized transaction receipt")
    receipt = obj(receipt, "transaction receipt")
    if hex_bytes(receipt.get("transactionHash"), 32, "receipt transaction hash") != tx.lower():
        raise CheckError("receipt identifies a different transaction")
    if hex_bytes(receipt.get("blockHash"), 32, "receipt block hash") != block["hash"].lower():
        raise CheckError("receipt belongs to a different finalized block hash")
    if quantity(receipt.get("blockNumber"), "receipt height") != quantity(block["number"], "block height"):
        raise CheckError("receipt belongs to a different finalized block height")
    if quantity(receipt.get("transactionIndex"), "receipt transaction index") != index:
        raise CheckError("receipt transaction order differs")
    for field in ("cumulativeGasUsed", "gasUsed", "status", "type", "effectiveGasPrice"):
        quantity(receipt.get(field), f"receipt {field}")
    if int(receipt["status"], 16) not in (0, 1):
        raise CheckError("receipt has invalid execution status")
    hex_bytes(receipt.get("from"), 20, "receipt sender")
    for field in ("to", "contractAddress"):
        if field not in receipt:
            raise CheckError(f"receipt {field} is absent")
        if receipt[field] is not None:
            hex_bytes(receipt[field], 20, f"receipt {field}")
    hex_bytes(receipt.get("logsBloom"), 256, "receipt logs bloom")
    for field in ("blobGasUsed", "blobGasPrice"):
        if field in receipt:
            quantity(receipt[field], f"receipt {field}")
    for log in array(receipt.get("logs"), "receipt logs"):
        log = obj(log, "receipt log")
        if hex_bytes(log.get("blockHash"), 32, "log block hash") != block["hash"].lower() or quantity(log.get("blockNumber"), "log block number") != quantity(block["number"], "block number"):
            raise CheckError("receipt log belongs to a different block")
        if hex_bytes(log.get("transactionHash"), 32, "log transaction hash") != tx.lower() or quantity(log.get("transactionIndex"), "log transaction index") != index:
            raise CheckError("receipt log identifies a different transaction")
        hex_bytes(log.get("address"), 20, "log address")
        for topic in array(log.get("topics"), "log topics"):
            hex_bytes(topic, 32, "log topic")
        quantity(log.get("logIndex"), "log index")
        variable_bytes(log.get("data"), "log data")
        if log.get("removed") is not False:
            raise CheckError("finalized receipt contains a removed or unqualified log")
    return receipt


def compare_execution(sut, reference, height, payload=None):
    """Anchor both roles to one immutable finalized height, not their moving heads."""
    if height > min(finalized_el(sut), finalized_el(reference)):
        raise NotReady("requested execution block has not finalized on both endpoints")
    left, right = block_at(sut, height), block_at(reference, height)
    for field in ("hash", "parentHash", "stateRoot", "receiptsRoot", "transactions", "gasLimit", "gasUsed", "timestamp"):
        if normalized(left[field]) != normalized(right[field]):
            raise CheckError(f"finalized blocks differ in {field} at height {height}")
    for field in ("baseFeePerGas", "blobGasUsed", "excessBlobGas", "withdrawals"):
        if (field in left) != (field in right) or normalized(left.get(field)) != normalized(right.get(field)):
            raise CheckError(f"finalized blocks differ in {field} at height {height}")
    if payload is not None:
        for beacon_key, execution_key in (("block_hash", "hash"), ("parent_hash", "parentHash"), ("state_root", "stateRoot"), ("receipts_root", "receiptsRoot")):
            if normalized(payload[beacon_key]) != normalized(left[execution_key]):
                raise CheckError(f"beacon payload {beacon_key} differs from finalized execution block")
        if decimal(payload["block_number"], "beacon execution height") != height:
            raise CheckError("beacon execution height differs from comparison height")
        for beacon_key, execution_key in (("gas_limit", "gasLimit"), ("gas_used", "gasUsed"), ("timestamp", "timestamp"), ("base_fee_per_gas", "baseFeePerGas"), ("blob_gas_used", "blobGasUsed"), ("excess_blob_gas", "excessBlobGas")):
            if (beacon_key in payload) != (execution_key in left):
                raise CheckError(f"beacon payload {beacon_key} availability differs from finalized execution block")
            if beacon_key in payload and decimal(payload[beacon_key], f"beacon {beacon_key}", (1 << 256) - 1) != quantity(left[execution_key], f"execution {execution_key}"):
                raise CheckError(f"beacon payload {beacon_key} differs from finalized execution block")
        if ("withdrawals" in payload) != ("withdrawals" in left):
            raise CheckError("beacon withdrawals availability differs from finalized execution block")
        if "withdrawals" in payload and withdrawals(payload["withdrawals"], False) != withdrawals(left["withdrawals"], True):
            raise CheckError("beacon withdrawals differ from finalized execution block")
        if len(array(payload.get("transactions"), "beacon transaction data")) != len(left["transactions"]):
            raise CheckError("beacon transaction data count differs from finalized execution block")
    receipts = []
    cumulative_gas, transaction_data = 0, []
    mandatory = ("transactionHash", "transactionIndex", "blockHash", "blockNumber", "from", "to", "cumulativeGasUsed", "gasUsed", "contractAddress", "logs", "logsBloom", "status", "type", "effectiveGasPrice")
    optional = ("blobGasUsed", "blobGasPrice")
    for index, tx in enumerate(left["transactions"]):
        a, b = receipt_at(sut, tx, left, index), receipt_at(reference, tx, right, index)
        for field in mandatory:
            if field not in a or field not in b or normalized(a[field]) != normalized(b[field]):
                raise CheckError(f"finalized receipts differ in {field} at height {height}")
        for field in optional:
            if (field in a) != (field in b) or normalized(a.get(field)) != normalized(b.get(field)):
                raise CheckError(f"finalized receipts differ in {field} at height {height}")
        cumulative_gas += quantity(a["gasUsed"], "receipt gas used")
        if quantity(a["cumulativeGasUsed"], "receipt cumulative gas") != cumulative_gas:
            raise CheckError("receipt cumulative gas differs from ordered gas usage")
        if payload is not None:
            encoded = payload["transactions"][index]
            variable_bytes(encoded, "beacon encoded transaction")
            if encoded == "0x":
                raise CheckError("beacon contains empty transaction data")
            for node in (sut, reference):
                raw = node.rpc("eth_getRawTransactionByHash", [tx])
                if raw is None:
                    raise NotReady(f"{node.role} missing finalized transaction data")
                variable_bytes(raw, "raw transaction data")
                if raw.lower() != encoded.lower():
                    raise CheckError("beacon transaction data differs from finalized execution transaction")
            transaction_data.append(hashlib.sha256(bytes.fromhex(encoded[2:])).hexdigest())
        canonical = normalized({key: a[key] for key in mandatory + optional if key in a})
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        receipts.append({"transaction_hash": tx.lower(), "transaction_index": index, "receipt_digest": digest})
    if cumulative_gas != quantity(left["gasUsed"], "execution block gas used"):
        raise CheckError("finalized receipt gas usage differs from block gas usage")
    if block_at(sut, height)["hash"].lower() != left["hash"].lower() or block_at(reference, height)["hash"].lower() != right["hash"].lower():
        raise CheckError("a finalized height changed during receipt collection")
    return {"height": height, "hash": left["hash"].lower(), "parent_hash": left["parentHash"].lower(), "state_root": left["stateRoot"].lower(), "receipts_root": left["receiptsRoot"].lower(), "transaction_count": len(receipts), "receipts": receipts, "gas_limit": quantity(left["gasLimit"], "gas limit"), "gas_used": cumulative_gas, "timestamp": quantity(left["timestamp"], "execution timestamp"), "base_fee_per_gas": quantity(left["baseFeePerGas"], "base fee") if "baseFeePerGas" in left else None, "blob_gas_used": quantity(left["blobGasUsed"], "blob gas used") if "blobGasUsed" in left else None, "excess_blob_gas": quantity(left["excessBlobGas"], "excess blob gas") if "excessBlobGas" in left else None, "transaction_data_sha256": transaction_data, "transaction_data_checked": payload is not None, "independent_transaction_hash_computation": False}


def execution_identity(sut, reference):
    chain_a = quantity(sut.rpc("eth_chainId", []), "SUT chain id")
    chain_b = quantity(reference.rpc("eth_chainId", []), "reference chain id")
    genesis_a, genesis_b = block_at(sut, 0), block_at(reference, 0)
    if chain_a != chain_b or genesis_a["hash"].lower() != genesis_b["hash"].lower():
        raise CheckError("execution endpoints have different chain ids or genesis hashes")
    return {"chain_id": chain_a, "genesis_execution_hash": genesis_a["hash"].lower()}


def withdrawals(value, execution):
    """Compare the complete ordered withdrawal fields across decimal and RPC encodings."""
    result = []
    for entry in array(value, "withdrawals"):
        entry = obj(entry, "withdrawal")
        parse_number = quantity if execution else decimal
        result.append({"index": parse_number(entry.get("index"), "withdrawal index"),
                       "validator_index": parse_number(entry.get("validatorIndex" if execution else "validator_index"), "withdrawal validator index"),
                       "address": hex_bytes(entry.get("address"), 20, "withdrawal address"),
                       "amount": parse_number(entry.get("amount"), "withdrawal amount")})
    return result
