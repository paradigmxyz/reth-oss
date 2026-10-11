"""Finalized full beacon block and fork-specific execution payload probes."""

import re
from contracts import CheckError, HttpFailure, NotReady, Header, FORKS, array, decimal, hex_bytes, obj


def finalized_header(endpoint):
    response = endpoint.beacon("/eth/v1/beacon/headers/finalized", missing_ok=True)
    if response is None:
        raise NotReady(f"{endpoint.role} has no finalized beacon header yet")
    response = obj(response, "finalized beacon header response")
    if response.get("execution_optimistic") is True:
        raise NotReady(f"{endpoint.role} finalized checkpoint is optimistic")
    if response.get("execution_optimistic") is not False:
        raise CheckError("finalized checkpoint lacks explicit execution verification")
    data = obj(response.get("data"), "finalized header data")
    if data.get("canonical") is not True:
        raise CheckError("finalized header is not canonical")
    message = obj(obj(data.get("header"), "signed header").get("message"), "beacon header")
    return Header(decimal(message.get("slot"), "finalized beacon slot"), hex_bytes(data.get("root"), 32, "finalized beacon root"))


def variable_bytes(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value):
        raise CheckError(f"{name} must be encoded bytes")


def execution_payload(endpoint, response, header):
    response = obj(response, "full beacon block response")
    version = response.get("version")
    if version not in FORKS:
        raise CheckError("full beacon block has unknown consensus version")
    if response.get("execution_optimistic") is not False or response.get("finalized") is not True:
        raise CheckError("fork-boundary beacon block must be finalized and non-optimistic")
    signed = obj(response.get("data"), "signed beacon block")
    hex_bytes(signed.get("signature"), 96, "beacon block signature")
    message = obj(signed.get("message"), "beacon block")
    slot = decimal(message.get("slot"), "beacon block slot")
    if slot != header.slot:
        raise CheckError("beacon response has a different requested slot")
    parent_root = hex_bytes(message.get("parent_root"), 32, "beacon parent root")
    body = obj(message.get("body"), "beacon block body")
    if version in ("phase0", "altair"):
        if "execution_payload" in body or "execution_payload_header" in body:
            raise CheckError("pre-merge block unexpectedly contains an execution payload")
        if version == "altair":
            obj(body.get("sync_aggregate"), "Altair sync aggregate")
        return None
    obj(body.get("sync_aggregate"), "sync aggregate")
    if version == "gloas":
        if "execution_payload" in body or "execution_payload_header" in body:
            raise CheckError("Gloas block must carry a bid instead of an inline payload")
        if "signed_execution_payload_bid" in body and "execution_payload_bid" in body:
            raise CheckError("Gloas block ambiguously contains two payload bid forms")
        if "signed_execution_payload_bid" in body:
            signed_bid = obj(body["signed_execution_payload_bid"], "Gloas signed bid")
            hex_bytes(signed_bid.get("signature"), 96, "payload bid signature")
            bid = obj(signed_bid.get("message"), "Gloas payload bid")
        else:
            bid = obj(body.get("execution_payload_bid"), "Gloas payload bid")
        if decimal(bid.get("slot"), "bid slot") != slot:
            raise CheckError("Gloas bid slot differs from its block")
        if hex_bytes(bid.get("parent_block_root"), 32, "bid parent beacon root") != parent_root:
            raise CheckError("Gloas bid parent root differs from its block")
        obj(body.get("parent_execution_requests"), "Gloas parent execution requests")
        array(body.get("payload_attestations"), "Gloas payload attestations")
        envelope = endpoint.beacon(f"/eth/v1/beacon/execution_payload_envelopes/{header.root}", missing_ok=True)
        if envelope is None:
            raise NotReady("Gloas FULL execution envelope not observed at this boundary block")
        envelope = obj(envelope, "Gloas envelope response")
        if envelope.get("version") != "gloas" or envelope.get("execution_optimistic") is not False or envelope.get("finalized") is not True:
            raise CheckError("Gloas envelope must be finalized and non-optimistic")
        signed_envelope = obj(envelope.get("data"), "signed execution envelope")
        hex_bytes(signed_envelope.get("signature"), 96, "envelope signature")
        content = obj(signed_envelope.get("message"), "execution envelope")
        if hex_bytes(content.get("beacon_block_root"), 32, "envelope beacon root") != header.root:
            raise CheckError("Gloas envelope links to a different beacon block")
        if "parent_beacon_block_root" in content and hex_bytes(content["parent_beacon_block_root"], 32, "envelope parent beacon root") != parent_root:
            raise CheckError("Gloas envelope parent beacon root differs")
        obj(content.get("execution_requests"), "Gloas envelope execution requests")
        payload = obj(content.get("payload"), "Gloas execution payload")
        if hex_bytes(payload.get("block_hash"), 32, "execution hash") != hex_bytes(bid.get("block_hash"), 32, "bid execution hash"):
            raise CheckError("Gloas bid and envelope execution hashes differ")
        if decimal(payload.get("slot_number"), "execution slot") != slot:
            raise CheckError("Gloas payload slot differs")
        variable_bytes(payload.get("block_access_list"), "Gloas block access list")
    else:
        payload = obj(body.get("execution_payload"), "full execution payload")
        if version in ("electra", "fulu"):
            obj(body.get("execution_requests"), "execution requests")
        if version in ("deneb", "electra", "fulu"):
            array(body.get("blob_kzg_commitments"), "blob commitments")
    for key in ("parent_hash", "state_root", "receipts_root", "block_hash", "prev_randao"):
        hex_bytes(payload.get(key), 32, f"execution {key}")
    hex_bytes(payload.get("fee_recipient"), 20, "execution fee recipient")
    hex_bytes(payload.get("logs_bloom"), 256, "execution logs bloom")
    variable_bytes(payload.get("extra_data"), "execution extra data")
    decimal(payload.get("base_fee_per_gas"), "execution base fee", (1 << 256) - 1)
    for key in ("block_number", "gas_limit", "gas_used", "timestamp"):
        decimal(payload.get(key), f"execution {key}")
    if int(payload["gas_used"]) > int(payload["gas_limit"]):
        raise CheckError("execution payload gas exceeds its limit")
    for tx in array(payload.get("transactions"), "execution transactions"):
        variable_bytes(tx, "encoded transaction")
    if version in ("capella", "deneb", "electra", "fulu", "gloas"):
        for withdrawal in array(payload.get("withdrawals"), "execution withdrawals"):
            withdrawal = obj(withdrawal, "execution withdrawal")
            for key in ("index", "validator_index", "amount"):
                decimal(withdrawal.get(key), f"withdrawal {key}")
            hex_bytes(withdrawal.get("address"), 20, "withdrawal address")
    elif "withdrawals" in payload:
        raise CheckError("Bellatrix payload unexpectedly has withdrawals")
    if version in ("deneb", "electra", "fulu", "gloas"):
        decimal(payload.get("blob_gas_used"), "blob gas used")
        decimal(payload.get("excess_blob_gas"), "excess blob gas")
    elif "blob_gas_used" in payload or "excess_blob_gas" in payload:
        raise CheckError("pre-Deneb payload unexpectedly has blob gas fields")
    if payload["block_hash"].lower() == "0x" + "00" * 32:
        raise NotReady("post-merge execution payload is still empty")
    return payload


def fixed_beacon(endpoint, slot):
    response = endpoint.beacon(f"/eth/v2/beacon/blocks/{slot}", missing_ok=True)
    if response is None:
        return None
    response = obj(response, "beacon block response")
    header_response = obj(endpoint.beacon(f"/eth/v1/beacon/headers/{slot}"), "beacon header response")
    data = obj(header_response.get("data"), "beacon header data")
    if data.get("canonical") is not True:
        raise CheckError("fork-boundary header is not canonical")
    message = obj(obj(data.get("header"), "signed header").get("message"), "beacon header")
    if decimal(message.get("slot"), "requested header slot") != slot:
        raise CheckError("beacon header has a different requested slot")
    header = Header(slot, hex_bytes(data.get("root"), 32, "beacon header root"))
    block_message = obj(obj(response.get("data"), "signed block").get("message"), "block message")
    expected_state_root = hex_bytes(message.get("state_root"), 32, "header state root")
    if expected_state_root != hex_bytes(block_message.get("state_root"), 32, "block state root"):
        raise CheckError("beacon block/header/state API roots differ")
    lookup_kind = "slot"
    try:
        state_response = endpoint.beacon(f"/eth/v1/beacon/states/{slot}/root")
    except HttpFailure as exc:
        if exc.code != 404:
            raise
        # Historic slot indexes may be unavailable while the exact state remains known.
        # The pinned API checks its hot/cold state index for a root-addressed query.
        lookup_kind = "root"
        state_response = endpoint.beacon(f"/eth/v1/beacon/states/{expected_state_root}/root", missing_ok=True)
        if state_response is None:
            raise NotReady(f"{endpoint.role} has no known finalized state for slot {slot}") from None
    state_response = obj(state_response, "beacon state root response")
    if state_response.get("execution_optimistic") is not False or state_response.get("finalized") is not True:
        raise CheckError("fork-boundary consensus state must be finalized and non-optimistic")
    state_root = hex_bytes(obj(state_response.get("data"), "state root data").get("root"), 32, "beacon state root")
    if state_root != expected_state_root:
        raise CheckError("beacon block/header/state API roots differ")
    return header, response, execution_payload(endpoint, response, header), state_root, lookup_kind
