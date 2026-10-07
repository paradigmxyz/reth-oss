//! Types for the draft `eth_getLogsV2` proposal.
//!
//! See <https://github.com/ethereum/execution-apis/pull/900>.

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, U64};
use alloy_rpc_types_eth::{Filter, ValueOrArray};
use serde::{de::Error, Deserialize, Deserializer, Serialize};

/// Hash-anchored log filter with an inclusive upper bound.
///
/// The anchor is inclusive in the current proposal. Resuming from a returned cursor therefore
/// repeats that block's logs; consumers must deduplicate the boundary block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsV2Filter {
    /// Canonical hash of the first block to scan, inclusive.
    pub from_block_hash: B256,
    /// Inclusive end of the scan. Omission defaults to the captured canonical head.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_to_block"
    )]
    pub to_block: Option<BlockNumberOrTag>,
    /// Address or addresses to match. Omission or `null` matches every address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<ValueOrArray<Address>>,
    /// Up to four topic positions, with the same matching semantics as `eth_getLogs`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_topics"
    )]
    pub topics: Option<LogsV2Topics>,
}

impl LogsV2Filter {
    /// Converts the matching fields to a regular filter for a resolved block range.
    pub fn into_filter(self, from: u64, to: u64) -> Filter {
        let mut filter = Filter::new().from_block(from).to_block(to);
        filter.address = self.address.map(Into::into).unwrap_or_default();
        for (position, topic) in self.topics.unwrap_or_default().into_iter().enumerate() {
            // Deserialization and the handler reject more than four positions.
            if let Some(slot) = filter.topics.get_mut(position) {
                *slot = topic.into();
            }
        }
        filter
    }
}

/// Identity of a scanned block or the canonical head captured for a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsV2BlockRef {
    /// Block number encoded as a hexadecimal quantity.
    pub number: U64,
    /// Block hash.
    pub hash: B256,
    /// Parent block hash.
    pub parent_hash: B256,
}

/// One page of whole-block logs, including progress through blocks without matching logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsV2Result<L> {
    /// Matching logs in block, transaction and log order.
    pub logs: Vec<L>,
    /// Last block fully scanned, inclusive.
    pub cursor_block: LogsV2BlockRef,
    /// Canonical head captured before the scan.
    pub head_block: LogsV2BlockRef,
}

/// Positional topic selectors with per-position alternatives and null wildcards.
pub type LogsV2Topics = Vec<ValueOrArray<Option<B256>>>;

fn deserialize_to_block<'de, D>(deserializer: D) -> Result<Option<BlockNumberOrTag>, D::Error>
where
    D: Deserializer<'de>,
{
    let block = BlockNumberOrTag::deserialize(deserializer)?;
    if block.is_pending() {
        return Err(D::Error::custom("toBlock cannot be pending"));
    }
    Ok(Some(block))
}

fn deserialize_topics<'de, D>(deserializer: D) -> Result<Option<LogsV2Topics>, D::Error>
where
    D: Deserializer<'de>,
{
    let topics = Option::<LogsV2Topics>::deserialize(deserializer)?;
    if topics.as_ref().is_some_and(|topics| topics.len() > 4) {
        return Err(D::Error::custom("topics must contain at most four positions"));
    }
    Ok(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn logs_v2_filter_accepts_get_logs_matching_fields() {
        let address = Address::with_last_byte(1);
        let topic = B256::with_last_byte(2);
        let request: LogsV2Filter = serde_json::from_value(json!({
            "fromBlockHash": B256::ZERO,
            "address": address,
            "topics": [null, [topic, null], topic]
        }))
        .unwrap();
        let filter = request.into_filter(3, 9);
        assert_eq!(filter.address, address.into());
        assert!(filter.topics[0].is_empty());
        assert!(filter.topics[1].is_empty());
        assert_eq!(filter.topics[2], topic.into());
        assert_eq!(filter.topics[3], Default::default());

        let request: LogsV2Filter = serde_json::from_value(json!({
            "fromBlockHash": B256::ZERO, "address": null, "topics": null
        }))
        .unwrap();
        assert_eq!(request.into_filter(0, 0), Filter::new().from_block(0u64).to_block(0u64));
    }

    #[test]
    fn logs_v2_filter_rejects_invalid_schema() {
        for value in [
            json!({}),
            json!({"fromBlockHash": "0x00"}),
            json!({"fromBlockHash": B256::ZERO, "fromBlock": "0x1"}),
            json!({"fromBlockHash": B256::ZERO, "blockHash": B256::ZERO}),
            json!({"fromBlockHash": B256::ZERO, "toBlock": "pending"}),
            json!({"fromBlockHash": B256::ZERO, "toBlock": null}),
            json!({"fromBlockHash": B256::ZERO, "topics": [null, null, null, null, null]}),
        ] {
            assert!(serde_json::from_value::<LogsV2Filter>(value.clone()).is_err(), "{value}");
        }
    }

    #[test]
    fn logs_v2_result_uses_quantity_and_camel_case_fields() {
        let block =
            LogsV2BlockRef { number: U64::from(16), hash: B256::ZERO, parent_hash: B256::ZERO };
        let result = LogsV2Result::<()> { logs: vec![], cursor_block: block, head_block: block };
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(
            value,
            json!({
                "logs": [],
                "cursorBlock": {"number": "0x10", "hash": B256::ZERO, "parentHash": B256::ZERO},
                "headBlock": {"number": "0x10", "hash": B256::ZERO, "parentHash": B256::ZERO}
            })
        );
        assert_eq!(serde_json::from_value::<LogsV2Result<()>>(value).unwrap(), result);
    }
}
