//! Typed beacon SSZ to portable execution data conversion.
//! Schemas and Gloas envelope comparisons follow consensus-specs v1.7.0-beta.3.
//! Structural bounds and object linkage do not establish signature, transition, DA or execution
//! validity. Returned data is ordinary engine input and carries no validated token.

use alloy_eips::{eip4895::Withdrawal, eip7685::Requests};
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
    ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3, ExecutionPayloadV4,
    PraguePayloadFields,
};
use reth_embedded_beacon_primitives::{sha256, Root, SszError};
use reth_embedded_beacon_state_transition::ssz::{
    bellatrix, capella, deneb, electra, fulu, gloas, BeaconBlock, Preset, Ssz,
};

/// Conservative allocation budget for copied execution data and collection metadata.
pub const MAX_CONVERSION_BYTES: usize = 10 * 1024 * 1024;

/// Conversion failure without payload bytes, keys or untrusted error text.
#[derive(Debug, thiserror::Error)]
pub enum PayloadConversionError {
    /// Complete structural SSZ collection or encoding bounds were violated.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// This fork does not carry an execution payload.
    #[error("beacon block has no execution payload")]
    NoExecutionPayload,
    /// Gloas carries its payload separately and requires explicit envelope linkage.
    #[error("Gloas payload conversion requires its linked envelope")]
    EnvelopeRequired,
    /// Typed data exceeds the converter's bounded allocation budget.
    #[error("execution data exceeds conversion allocation budget")]
    Oversized,
    /// Ordered versioned hashes or canonical request-group encodings are malformed.
    #[error("invalid execution context encoding")]
    ContextEncoding,
    /// A named envelope/bid/block/state-input linkage is inconsistent.
    #[error("Gloas execution payload linkage failed: {0}")]
    Linkage(&'static str),
}

/// State-derived inputs required by the pinned Gloas envelope helper.
/// These ordinary values must come from the caller's already checked state; this type proves none
/// of their validity and does not authorize fork choice, block import or engine acceptance.
#[derive(Clone, Copy, Debug)]
pub struct GloasPayloadContext {
    /// Parent execution hash from `state.latest_block_hash`.
    pub parent_execution_hash: Root,
    /// Expected timestamp computed from network genesis time and the block slot.
    pub expected_timestamp: u64,
    /// SSZ root of `state.payload_expected_withdrawals` under the same preset.
    pub expected_withdrawals_root: Root,
}

fn common_budget(
    extra: &[u8],
    transactions: impl Iterator<Item = usize>,
    bal: usize,
) -> Result<(), PayloadConversionError> {
    let mut total = extra
        .len()
        .checked_add(bal)
        .and_then(|size| size.checked_add(1024))
        .ok_or(PayloadConversionError::Oversized)?;
    for length in transactions {
        total = total
            .checked_add(length)
            .and_then(|size| size.checked_add(core::mem::size_of::<Bytes>()))
            .ok_or(PayloadConversionError::Oversized)?;
        if total > MAX_CONVERSION_BYTES {
            return Err(PayloadConversionError::Oversized);
        }
    }
    if total > MAX_CONVERSION_BYTES {
        return Err(PayloadConversionError::Oversized);
    }
    Ok(())
}

fn collection_bytes(length: usize, width: usize) -> Result<usize, PayloadConversionError> {
    length.checked_mul(width).ok_or(PayloadConversionError::Oversized)
}

fn request_allocation<T: Ssz>(
    values: &[T],
    preset: Preset,
) -> Result<usize, PayloadConversionError> {
    if values.is_empty() {
        return Ok(0);
    }
    collection_bytes(values.len(), T::fixed_size(preset).ok_or(PayloadConversionError::Oversized)?)?
        .checked_add(1 + core::mem::size_of::<Bytes>())
        .ok_or(PayloadConversionError::Oversized)
}

macro_rules! requests_allocation {
    ($value:expr, $preset:expr, $($field:ident),+ $(,)?) => {{
        let value = $value;
        let mut total = 0usize;
        $(total = total.checked_add(request_allocation(&value.$field.values, $preset)?)
            .ok_or(PayloadConversionError::Oversized)?;)+
        total
    }};
}

macro_rules! payload_budget {
    ($payload:expr, $withdrawals:expr, $commitments:expr, $requests:expr, $bal:expr) => {{
        let payload = $payload;
        let requests = $requests;
        let copied = collection_bytes($withdrawals, core::mem::size_of::<Withdrawal>())?
            .checked_add(collection_bytes($commitments, core::mem::size_of::<B256>())?)
            .and_then(|size| size.checked_add(requests))
            .and_then(|size| size.checked_add($bal))
            .ok_or(PayloadConversionError::Oversized)?;
        common_budget(
            &payload.extra_data.values,
            payload.transactions.values.iter().map(|tx| tx.values.len()),
            copied,
        )?;
    }};
}

macro_rules! payload_v1 {
    ($name:ident, $module:ident) => {
        fn $name(
            payload: &$module::ExecutionPayload,
            preset: Preset,
        ) -> Result<ExecutionPayloadV1, PayloadConversionError> {
            common_budget(
                &payload.extra_data.values,
                payload.transactions.values.iter().map(|tx| tx.values.len()),
                0,
            )?;
            payload.hash_tree_root(preset)?;
            let bloom: [u8; 256] =
                payload.logs_bloom.values.as_slice().try_into().map_err(|_| {
                    SszError::InvalidLength {
                        expected: 256,
                        actual: payload.logs_bloom.values.len(),
                    }
                })?;
            Ok(ExecutionPayloadV1 {
                parent_hash: B256::from(payload.parent_hash),
                fee_recipient: Address::from(payload.fee_recipient),
                state_root: B256::from(payload.state_root),
                receipts_root: B256::from(payload.receipts_root),
                logs_bloom: Bloom::from(bloom),
                prev_randao: B256::from(payload.prev_randao),
                block_number: payload.block_number,
                gas_limit: payload.gas_limit,
                gas_used: payload.gas_used,
                timestamp: payload.timestamp,
                extra_data: Bytes::copy_from_slice(&payload.extra_data.values),
                base_fee_per_gas: U256::from_le_bytes(payload.base_fee_per_gas),
                block_hash: B256::from(payload.block_hash),
                transactions: payload
                    .transactions
                    .values
                    .iter()
                    .map(|tx| Bytes::copy_from_slice(&tx.values))
                    .collect(),
            })
        }
    };
}
payload_v1!(bellatrix_v1, bellatrix);
payload_v1!(capella_v1, capella);
payload_v1!(deneb_v1, deneb);
payload_v1!(electra_v1, electra);
payload_v1!(fulu_v1, fulu);
payload_v1!(gloas_v1, gloas);

macro_rules! payload_v3 {
    ($name:ident, $module:ident, $common:ident) => {
        fn $name(
            payload: &$module::ExecutionPayload,
            preset: Preset,
        ) -> Result<ExecutionPayloadV3, PayloadConversionError> {
            Ok(ExecutionPayloadV3 {
                payload_inner: ExecutionPayloadV2 {
                    payload_inner: $common(payload, preset)?,
                    withdrawals: payload
                        .withdrawals
                        .values
                        .iter()
                        .map(|value| Withdrawal {
                            index: value.index,
                            validator_index: value.validator_index,
                            address: Address::from(value.address),
                            amount: value.amount,
                        })
                        .collect(),
                },
                blob_gas_used: payload.blob_gas_used,
                excess_blob_gas: payload.excess_blob_gas,
            })
        }
    };
}
payload_v3!(deneb_v3, deneb, deneb_v1);
payload_v3!(electra_v3, electra, electra_v1);
payload_v3!(fulu_v3, fulu, fulu_v1);
payload_v3!(gloas_v3, gloas, gloas_v1);

fn versioned_hashes(commitments: &[[u8; 48]]) -> Vec<B256> {
    commitments
        .iter()
        .map(|commitment| {
            let mut hash = sha256(commitment);
            hash[0] = 1;
            B256::from(hash)
        })
        .collect()
}

fn cancun(parent: Root, commitments: &[[u8; 48]]) -> CancunPayloadFields {
    CancunPayloadFields {
        parent_beacon_block_root: B256::from(parent),
        versioned_hashes: versioned_hashes(commitments),
    }
}

fn request_group<T: Ssz>(
    requests: &mut Requests,
    kind: u8,
    values: &[T],
    preset: Preset,
) -> Result<(), PayloadConversionError> {
    if values.is_empty() {
        return Ok(());
    }
    let width = T::fixed_size(preset).ok_or(PayloadConversionError::Oversized)?;
    let length = values
        .len()
        .checked_mul(width)
        .and_then(|value| value.checked_add(1))
        .ok_or(PayloadConversionError::Oversized)?;
    if length > MAX_CONVERSION_BYTES {
        return Err(PayloadConversionError::Oversized);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.push(kind);
    for value in values {
        value.write_ssz(preset, &mut bytes)?;
    }
    requests.push_request(bytes.into());
    Ok(())
}

macro_rules! requests {
    ($name:ident, $module:ident) => {
        fn $name(
            value: &$module::ExecutionRequests,
            preset: Preset,
        ) -> Result<Requests, PayloadConversionError> {
            value.hash_tree_root(preset)?;
            let mut requests = Requests::with_capacity(3);
            request_group(&mut requests, 0, &value.deposits.values, preset)?;
            request_group(&mut requests, 1, &value.withdrawals.values, preset)?;
            request_group(&mut requests, 2, &value.consolidations.values, preset)?;
            Ok(requests)
        }
    };
}
requests!(electra_requests, electra);
requests!(fulu_requests, fulu);

fn gloas_requests(
    value: &gloas::ExecutionRequests,
    preset: Preset,
) -> Result<Requests, PayloadConversionError> {
    value.hash_tree_root(preset)?;
    let mut requests = Requests::with_capacity(5);
    request_group(&mut requests, 0, &value.deposits.values, preset)?;
    request_group(&mut requests, 1, &value.withdrawals.values, preset)?;
    request_group(&mut requests, 2, &value.consolidations.values, preset)?;
    request_group(&mut requests, 3, &value.builder_deposits.values, preset)?;
    request_group(&mut requests, 4, &value.builder_exits.values, preset)?;
    Ok(requests)
}

/// Convert the exact Bellatrix payload submitted by the mandatory transition callback.
/// Structural checks and bounded copies provide ordinary engine input, not execution validity.
pub fn from_bellatrix_payload(
    payload: &bellatrix::ExecutionPayload,
    preset: Preset,
) -> Result<ExecutionData, PayloadConversionError> {
    Ok(ExecutionData::new(bellatrix_v1(payload, preset)?.into(), ExecutionPayloadSidecar::none()))
}
/// Convert a complete typed Bellatrix through Fulu block into
/// ordinary engine data. Bytes are copied from the borrowed beacon object; transaction decoding and
/// engine verdicts remain the engine's responsibility. Current fork timing and consensus validity
/// remain the caller's job.
pub fn from_beacon_block(block: &BeaconBlock) -> Result<ExecutionData, PayloadConversionError> {
    // Check aggregate copied allocations before hashing or cloning caller supplied collections.
    match block {
        BeaconBlock::Bellatrix(block, _) => {
            payload_budget!(&block.body.execution_payload, 0, 0, 0, 0)
        }
        BeaconBlock::Capella(block, _) => payload_budget!(
            &block.body.execution_payload,
            block.body.execution_payload.withdrawals.values.len(),
            0,
            0,
            0
        ),
        BeaconBlock::Deneb(block, _) => payload_budget!(
            &block.body.execution_payload,
            block.body.execution_payload.withdrawals.values.len(),
            block.body.blob_kzg_commitments.values.len(),
            0,
            0
        ),
        BeaconBlock::Electra(block, preset) => payload_budget!(
            &block.body.execution_payload,
            block.body.execution_payload.withdrawals.values.len(),
            block.body.blob_kzg_commitments.values.len(),
            requests_allocation!(
                &block.body.execution_requests,
                *preset,
                deposits,
                withdrawals,
                consolidations
            ),
            0
        ),
        BeaconBlock::Fulu(block, preset) => payload_budget!(
            &block.body.execution_payload,
            block.body.execution_payload.withdrawals.values.len(),
            block.body.blob_kzg_commitments.values.len(),
            requests_allocation!(
                &block.body.execution_requests,
                *preset,
                deposits,
                withdrawals,
                consolidations
            ),
            0
        ),
        BeaconBlock::Phase0(_, _) | BeaconBlock::Altair(_, _) | BeaconBlock::Gloas(_, _) => {}
    }
    block.hash_tree_root()?;
    Ok(match block {
        BeaconBlock::Phase0(_, _) | BeaconBlock::Altair(_, _) => {
            return Err(PayloadConversionError::NoExecutionPayload)
        }
        BeaconBlock::Bellatrix(block, preset) => ExecutionData::new(
            bellatrix_v1(&block.body.execution_payload, *preset)?.into(),
            ExecutionPayloadSidecar::none(),
        ),
        BeaconBlock::Capella(block, preset) => {
            let payload = &block.body.execution_payload;
            ExecutionData::new(
                ExecutionPayloadV2 {
                    payload_inner: capella_v1(payload, *preset)?,
                    withdrawals: payload
                        .withdrawals
                        .values
                        .iter()
                        .map(|value| Withdrawal {
                            index: value.index,
                            validator_index: value.validator_index,
                            address: Address::from(value.address),
                            amount: value.amount,
                        })
                        .collect(),
                }
                .into(),
                ExecutionPayloadSidecar::none(),
            )
        }
        BeaconBlock::Deneb(block, preset) => ExecutionData::new(
            deneb_v3(&block.body.execution_payload, *preset)?.into(),
            ExecutionPayloadSidecar::v3(cancun(
                block.parent_root,
                &block.body.blob_kzg_commitments.values,
            )),
        ),
        BeaconBlock::Electra(block, preset) => ExecutionData::new(
            electra_v3(&block.body.execution_payload, *preset)?.into(),
            ExecutionPayloadSidecar::v4(
                cancun(block.parent_root, &block.body.blob_kzg_commitments.values),
                PraguePayloadFields::new(electra_requests(
                    &block.body.execution_requests,
                    *preset,
                )?),
            ),
        ),
        BeaconBlock::Fulu(block, preset) => ExecutionData::new(
            fulu_v3(&block.body.execution_payload, *preset)?.into(),
            ExecutionPayloadSidecar::v4(
                cancun(block.parent_root, &block.body.blob_kzg_commitments.values),
                PraguePayloadFields::new(fulu_requests(&block.body.execution_requests, *preset)?),
            ),
        ),
        BeaconBlock::Gloas(_, _) => return Err(PayloadConversionError::EnvelopeRequired),
    })
}

/// Convert a Gloas payload only after checking block, committed bid and supplied state-input
/// linkage. This implements the structural comparisons of the pinned envelope helper; it does not
/// verify either signature, state transition, execution block hash, withdrawals derivation or blob
/// custody.
pub fn from_gloas_envelope(
    block: &gloas::BeaconBlock,
    signed_envelope: &gloas::SignedExecutionPayloadEnvelope,
    preset: Preset,
    context: &GloasPayloadContext,
) -> Result<ExecutionData, PayloadConversionError> {
    let envelope = &signed_envelope.message;
    let payload = &envelope.payload;
    let bid = &block.body.signed_execution_payload_bid.message;
    payload_budget!(
        payload,
        payload.withdrawals.values.len(),
        bid.blob_kzg_commitments.values.len(),
        requests_allocation!(
            &envelope.execution_requests,
            preset,
            deposits,
            withdrawals,
            consolidations,
            builder_deposits,
            builder_exits
        ),
        payload.block_access_list.values.len()
    );
    let block_root = block.hash_tree_root(preset)?;
    signed_envelope.hash_tree_root(preset)?;
    for (matches, field) in [
        (envelope.beacon_block_root == block_root, "beacon block root"),
        (envelope.parent_beacon_block_root == block.parent_root, "parent beacon root"),
        (bid.parent_block_root == block.parent_root, "bid parent beacon root"),
        (envelope.builder_index == bid.builder_index, "builder index"),
        (payload.prev_randao == bid.prev_randao, "randao"),
        (payload.gas_limit == bid.gas_limit, "gas limit"),
        (payload.block_hash == bid.block_hash, "execution hash"),
        (
            envelope.execution_requests.hash_tree_root(preset)? == bid.execution_requests_root,
            "execution requests root",
        ),
        (payload.slot_number == block.slot, "slot"),
        (bid.slot == block.slot, "slot"),
        (
            payload.parent_hash == context.parent_execution_hash &&
                bid.parent_block_hash == context.parent_execution_hash,
            "parent execution hash",
        ),
        (payload.timestamp == context.expected_timestamp, "timestamp"),
        (
            payload.withdrawals.hash_tree_root(preset)? == context.expected_withdrawals_root,
            "expected withdrawals",
        ),
    ] {
        if !matches {
            return Err(PayloadConversionError::Linkage(field));
        }
    }
    common_budget(
        &payload.extra_data.values,
        payload.transactions.values.iter().map(|tx| tx.values.len()),
        payload.block_access_list.values.len(),
    )?;
    let engine = ExecutionPayloadV4 {
        payload_inner: gloas_v3(payload, preset)?,
        block_access_list: Bytes::copy_from_slice(&payload.block_access_list.values),
        slot_number: payload.slot_number,
    };
    Ok(ExecutionData::new(
        ExecutionPayload::V4(engine),
        ExecutionPayloadSidecar::v4(
            cancun(envelope.parent_beacon_block_root, &bid.blob_kzg_commitments.values),
            PraguePayloadFields::new(gloas_requests(&envelope.execution_requests, preset)?),
        ),
    ))
}

pub mod candidate;

#[cfg(test)]
mod tests;
