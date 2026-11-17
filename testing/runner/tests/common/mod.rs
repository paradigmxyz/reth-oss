//! Coherent invalid fixtures derived from the checked-in valid blocks.

use alloy_consensus::{Block, TxEnvelope};
use alloy_eips::eip7928::{compute_block_access_list_hash, BlockAccessList};
use alloy_primitives::{Bytes, B256};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Loads a valid fixture, or derives an expected-invalid variant with a matching block hash.
pub(super) fn load_fixture(name: &str) -> Value {
    let invalid_state = name.ends_with("bad_state_root.json");
    let invalid_bal = name == "enginetest_amsterdam_bad_access_list.json";
    let base = if invalid_state {
        if name.starts_with("engine") {
            "enginetest_empty_block.json"
        } else {
            "blocktest_empty_block.json"
        }
    } else if invalid_bal {
        "enginetest_amsterdam_empty_block.json"
    } else {
        name
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut data: Value = serde_json::from_slice(&fs::read(root.join(base)).unwrap()).unwrap();
    if !invalid_state && !invalid_bal {
        return data
    }
    let test = data.as_object_mut().unwrap().values_mut().next().unwrap();
    let block_fixture = if invalid_bal {
        "blocktest_amsterdam_empty_block.json"
    } else {
        "blocktest_empty_block.json"
    };
    let source: Value =
        serde_json::from_slice(&fs::read(root.join(block_fixture)).unwrap()).unwrap();
    let block = &source.as_object().unwrap().values().next().unwrap()["blocks"][0];
    let encoded: Bytes = serde_json::from_value(block["rlp"].clone()).unwrap();
    let mut block: Block<TxEnvelope> = alloy_rlp::decode_exact(&encoded).unwrap();
    if invalid_state {
        block.header.state_root = B256::repeat_byte(1);
    } else {
        let encoded: Bytes = serde_json::from_value(
            test["engineNewPayloads"][0]["params"][0]["blockAccessList"].clone(),
        )
        .unwrap();
        let mut bal: BlockAccessList = alloy_rlp::decode_exact(&encoded).unwrap();
        assert!(bal.pop().is_some());
        block.header.block_access_list_hash = Some(compute_block_access_list_hash(&bal));
        test["engineNewPayloads"][0]["params"][0]["blockAccessList"] =
            json!(Bytes::from(alloy_rlp::encode(&bal)));
    }
    if name.starts_with("engine") {
        let payload = &mut test["engineNewPayloads"][0];
        payload["params"][0]["stateRoot"] = json!(block.header.state_root);
        payload["params"][0]["blockHash"] = json!(block.header.hash_slow());
        payload["validationError"] = if invalid_state {
            json!("BlockException.INVALID_STATE_ROOT")
        } else {
            json!("BlockException.INVALID_BLOCK_ACCESS_LIST")
        };
    } else {
        test["blocks"][0]["rlp"] = json!(Bytes::from(alloy_rlp::encode(&block)));
        test["blocks"][0]["expectException"] = json!("BlockException.INVALID_STATE_ROOT");
    }
    test["lastblockhash"] = test["genesisBlockHeader"]["hash"].clone();
    let original = if invalid_bal { "amsterdam_empty_block" } else { "empty_block" };
    let test = data.as_object_mut().unwrap().remove(original).unwrap();
    if invalid_bal {
        json!({"amsterdam_bad_access_list": test})
    } else {
        json!({"bad_state_root": test})
    }
}

/// Writes a fixture into the test's temporary directory, keeping its requested filename.
pub(super) fn write_fixture(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, load_fixture(name).to_string()).unwrap();
    path
}
