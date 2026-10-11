use super::{Header, LightClientError, LightClientParameters};
use crate::ssz::{auxiliary, capella, deneb, List, Preset, Vector};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Fork, Root};

pub(super) const fn zero_beacon() -> BeaconBlockHeader {
    BeaconBlockHeader {
        slot: 0,
        proposer_index: 0,
        parent_root: [0; 32],
        state_root: [0; 32],
        body_root: [0; 32],
    }
}
pub(super) fn empty_capella() -> capella::ExecutionPayloadHeader {
    capella::ExecutionPayloadHeader {
        parent_hash: [0; 32],
        fee_recipient: [0; 20],
        state_root: [0; 32],
        receipts_root: [0; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: [0; 32],
        block_number: 0,
        gas_limit: 0,
        gas_used: 0,
        timestamp: 0,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [0; 32],
        transactions_root: [0; 32],
        withdrawals_root: [0; 32],
    }
}
pub(super) fn to_deneb(pre: &capella::ExecutionPayloadHeader) -> deneb::ExecutionPayloadHeader {
    deneb::ExecutionPayloadHeader {
        parent_hash: pre.parent_hash,
        fee_recipient: pre.fee_recipient,
        state_root: pre.state_root,
        receipts_root: pre.receipts_root,
        logs_bloom: pre.logs_bloom.clone(),
        prev_randao: pre.prev_randao,
        block_number: pre.block_number,
        gas_limit: pre.gas_limit,
        gas_used: pre.gas_used,
        timestamp: pre.timestamp,
        extra_data: pre.extra_data.clone(),
        base_fee_per_gas: pre.base_fee_per_gas,
        block_hash: pre.block_hash,
        transactions_root: pre.transactions_root,
        withdrawals_root: pre.withdrawals_root,
        blob_gas_used: 0,
        excess_blob_gas: 0,
    }
}
pub(super) fn to_capella(pre: &deneb::ExecutionPayloadHeader) -> capella::ExecutionPayloadHeader {
    capella::ExecutionPayloadHeader {
        parent_hash: pre.parent_hash,
        fee_recipient: pre.fee_recipient,
        state_root: pre.state_root,
        receipts_root: pre.receipts_root,
        logs_bloom: pre.logs_bloom.clone(),
        prev_randao: pre.prev_randao,
        block_number: pre.block_number,
        gas_limit: pre.gas_limit,
        gas_used: pre.gas_used,
        timestamp: pre.timestamp,
        extra_data: pre.extra_data.clone(),
        base_fee_per_gas: pre.base_fee_per_gas,
        block_hash: pre.block_hash,
        transactions_root: pre.transactions_root,
        withdrawals_root: pre.withdrawals_root,
    }
}
pub(super) fn empty_header(schema: Fork, _preset: Preset) -> Result<Header, LightClientError> {
    match schema {
        Fork::Altair | Fork::Bellatrix => {
            Ok(Header::Altair(auxiliary::altair::LightClientHeader { beacon: zero_beacon() }))
        }
        Fork::Capella => Ok(Header::Capella(Box::new(auxiliary::capella::LightClientHeader {
            beacon: zero_beacon(),
            execution: empty_capella(),
            execution_branch: Vector::from_vec(vec![[0; 32]; 4]),
        }))),
        Fork::Deneb | Fork::Electra | Fork::Fulu => {
            Ok(Header::Deneb(Box::new(auxiliary::deneb::LightClientHeader {
                beacon: zero_beacon(),
                execution: to_deneb(&empty_capella()),
                execution_branch: Vector::from_vec(vec![[0; 32]; 4]),
            })))
        }
        Fork::Gloas => Ok(Header::Gloas(Box::new(auxiliary::gloas::LightClientHeader {
            beacon: zero_beacon(),
            execution_block_hash: [0; 32],
            execution_branch: Vector::from_vec(vec![[0; 32]; 11]),
        }))),
        Fork::Phase0 => Err(LightClientError::Unsupported(schema)),
    }
}
fn enabled_at(params: &LightClientParameters, fork: Fork, epoch: u64) -> bool {
    params.epoch(fork) != u64::MAX && epoch >= params.epoch(fork)
}
/// Execution root under the header's actual slot fork, including pre-upgrade projections.
pub fn get_lc_execution_root(
    header: &Header,
    params: &LightClientParameters,
) -> Result<Root, LightClientError> {
    params.validate()?;
    header.hash_tree_root(params.preset())?;
    let epoch = header.beacon().slot / params.slots_per_epoch();
    match header {
        Header::Altair(_) => Ok([0; 32]),
        Header::Capella(value) => {
            if enabled_at(params, Fork::Capella, epoch) {
                Ok(value.execution.hash_tree_root(params.preset())?)
            } else {
                Ok([0; 32])
            }
        }
        Header::Deneb(value) => {
            if enabled_at(params, Fork::Deneb, epoch) {
                Ok(value.execution.hash_tree_root(params.preset())?)
            } else if enabled_at(params, Fork::Capella, epoch) {
                Ok(to_capella(&value.execution).hash_tree_root(params.preset())?)
            } else {
                Ok([0; 32])
            }
        }
        Header::Gloas(value) => {
            if enabled_at(params, Fork::Gloas, epoch) {
                return Ok(value.execution_block_hash)
            }
            let (depth, gindex) = if enabled_at(params, Fork::Deneb, epoch) {
                if value.beacon.slot == 0 {
                    return Ok(to_deneb(&empty_capella()).hash_tree_root(params.preset())?)
                }
                (5, 44u64)
            } else if enabled_at(params, Fork::Capella, epoch) {
                if value.beacon.slot == 0 {
                    return Ok(empty_capella().hash_tree_root(params.preset())?)
                }
                (4, 28)
            } else {
                return Ok([0; 32])
            };
            let inner_len = value.execution_branch.values.len() - 4;
            let mut root = value.execution_block_hash;
            for (level, sibling) in
                value.execution_branch.values[inner_len - depth..inner_len].iter().enumerate()
            {
                root = if (gindex >> level) & 1 == 1 {
                    reth_embedded_beacon_primitives::hash_pair(*sibling, root)
                } else {
                    reth_embedded_beacon_primitives::hash_pair(root, *sibling)
                };
            }
            Ok(root)
        }
    }
}
/// Validate all execution fields, fork-dependent zero rules and exact Merkle inclusion.
pub fn is_valid_light_client_header(
    header: &Header,
    params: &LightClientParameters,
) -> Result<bool, LightClientError> {
    params.validate()?;
    header.hash_tree_root(params.preset())?;
    let epoch = header.beacon().slot / params.slots_per_epoch();
    match header {
        Header::Altair(_) => Ok(true),
        Header::Capella(value) => {
            if !enabled_at(params, Fork::Capella, epoch) {
                return Ok(value.execution == empty_capella() &&
                    value.execution_branch.values.iter().all(|root| *root == [0; 32]))
            }
            Ok(super::is_valid_normalized_merkle_branch(
                get_lc_execution_root(header, params)?,
                &value.execution_branch.values,
                25,
                value.beacon.body_root,
            ))
        }
        Header::Deneb(value) => {
            if !enabled_at(params, Fork::Deneb, epoch) &&
                (value.execution.blob_gas_used != 0 || value.execution.excess_blob_gas != 0)
            {
                return Ok(false)
            }
            if !enabled_at(params, Fork::Capella, epoch) {
                return Ok(value.execution == to_deneb(&empty_capella()) &&
                    value.execution_branch.values.iter().all(|root| *root == [0; 32]))
            }
            Ok(super::is_valid_normalized_merkle_branch(
                get_lc_execution_root(header, params)?,
                &value.execution_branch.values,
                25,
                value.beacon.body_root,
            ))
        }
        Header::Gloas(value) => {
            let gindex = if enabled_at(params, Fork::Gloas, epoch) {
                2856
            } else if enabled_at(params, Fork::Deneb, epoch) {
                812
            } else if enabled_at(params, Fork::Capella, epoch) {
                412
            } else {
                return Ok(value.execution_block_hash == [0; 32] &&
                    value.execution_branch.values.iter().all(|root| *root == [0; 32]))
            };
            Ok(super::is_valid_normalized_merkle_branch(
                value.execution_block_hash,
                &value.execution_branch.values,
                gindex,
                value.beacon.body_root,
            ))
        }
    }
}
