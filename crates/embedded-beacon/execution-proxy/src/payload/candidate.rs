//! Portable conversion of the exact candidate supplied by mandatory engine callbacks.
//! Inputs remain ordinary data; caller-authenticated consensus context and actual engine
//! acceptance are independently required before authoritative import.

use super::{
    capella_v1, collection_bytes, common_budget, deneb_v3, electra_v3, fulu_v3, gloas_v3,
    PayloadConversionError, MAX_CONVERSION_BYTES,
};
use alloy_eips::{eip4895::Withdrawal, eip7685::Requests};
use alloy_primitives::{Address, Bytes, B256};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayloadSidecar, ExecutionPayloadV2,
    ExecutionPayloadV4, PraguePayloadFields,
};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::{
    capella, deneb, electra, fulu, gloas, Preset, Ssz,
};

/// Exact ordered execution inputs supplied by the consensus transition.
#[derive(Clone, Copy, Debug)]
pub struct EngineSidecarInputs<'a> {
    /// Parent beacon root committed by the current block or payload envelope.
    pub parent_beacon_block_root: Root,
    /// Versioned hashes in the same order as the block's validated commitments.
    pub versioned_hashes: &'a [Root],
    /// Canonical nonempty type-prefixed request groups, in ascending type order.
    pub execution_requests_list: &'a [Vec<u8>],
}

/// Convert the exact Capella candidate into Engine V2 input, preserving ordered withdrawals.
/// Structural conversion grants no terminal, execution, consensus or availability authority.
pub fn from_capella_payload(
    payload: &capella::ExecutionPayload,
    preset: Preset,
) -> Result<ExecutionData, PayloadConversionError> {
    payload_budget!(payload, payload.withdrawals.values.len(), 0, 0, 0);
    Ok(ExecutionData::new(
        ExecutionPayloadV2 {
            payload_inner: capella_v1(payload, preset)?,
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
    ))
}

/// Convert the exact Deneb candidate into Engine V3 input with its consensus callback context.
/// Hash order and parent beacon root remain exact; the caller must authenticate their provenance.
pub fn from_deneb_payload(
    payload: &deneb::ExecutionPayload,
    preset: Preset,
    parent_beacon_block_root: Root,
    versioned_hashes: &[Root],
) -> Result<ExecutionData, PayloadConversionError> {
    if versioned_hashes.len() > 4096 || versioned_hashes.iter().any(|hash| hash[0] != 1) {
        return Err(PayloadConversionError::ContextEncoding)
    }
    payload_budget!(payload, payload.withdrawals.values.len(), versioned_hashes.len(), 0, 0);
    Ok(ExecutionData::new(
        deneb_v3(payload, preset)?.into(),
        ExecutionPayloadSidecar::v3(CancunPayloadFields {
            parent_beacon_block_root: B256::from(parent_beacon_block_root),
            versioned_hashes: versioned_hashes.iter().copied().map(B256::from).collect(),
        }),
    ))
}
/// Convert the exact Electra candidate into Engine V4 input without rebuilding a beacon block.
pub fn from_electra_payload(
    payload: &electra::ExecutionPayload,
    preset: Preset,
    inputs: EngineSidecarInputs<'_>,
) -> Result<ExecutionData, PayloadConversionError> {
    let copied = sidecar_budget(inputs)?;
    payload_budget!(
        payload,
        payload.withdrawals.values.len(),
        inputs.versioned_hashes.len(),
        copied,
        0
    );
    Ok(ExecutionData::new(electra_v3(payload, preset)?.into(), sidecar(inputs, preset, false)?))
}

/// Convert the exact Fulu candidate into Engine V4 input without rebuilding a beacon block.
pub fn from_fulu_payload(
    payload: &fulu::ExecutionPayload,
    preset: Preset,
    inputs: EngineSidecarInputs<'_>,
) -> Result<ExecutionData, PayloadConversionError> {
    let copied = sidecar_budget(inputs)?;
    payload_budget!(
        payload,
        payload.withdrawals.values.len(),
        inputs.versioned_hashes.len(),
        copied,
        0
    );
    Ok(ExecutionData::new(fulu_v3(payload, preset)?.into(), sidecar(inputs, preset, false)?))
}

/// Convert the exact Gloas candidate into Engine V5 input, retaining its access list and slot.
/// The transition must authenticate the complete bid and envelope before invoking this callback.
pub fn from_gloas_payload(
    payload: &gloas::ExecutionPayload,
    preset: Preset,
    inputs: EngineSidecarInputs<'_>,
) -> Result<ExecutionData, PayloadConversionError> {
    let copied = sidecar_budget(inputs)?;
    payload_budget!(
        payload,
        payload.withdrawals.values.len(),
        inputs.versioned_hashes.len(),
        copied,
        payload.block_access_list.values.len()
    );
    let converted = ExecutionPayloadV4 {
        payload_inner: gloas_v3(payload, preset)?,
        block_access_list: Bytes::copy_from_slice(&payload.block_access_list.values),
        slot_number: payload.slot_number,
    };
    Ok(ExecutionData::new(converted.into(), sidecar(inputs, preset, true)?))
}

fn sidecar_budget(inputs: EngineSidecarInputs<'_>) -> Result<usize, PayloadConversionError> {
    let mut bytes = 0usize;
    for request in inputs.execution_requests_list {
        bytes = bytes
            .checked_add(request.len())
            .and_then(|size| size.checked_add(core::mem::size_of::<Bytes>()))
            .ok_or(PayloadConversionError::Oversized)?;
    }
    if bytes > MAX_CONVERSION_BYTES {
        return Err(PayloadConversionError::Oversized);
    }
    Ok(bytes)
}

fn sidecar(
    inputs: EngineSidecarInputs<'_>,
    preset: Preset,
    is_gloas: bool,
) -> Result<ExecutionPayloadSidecar, PayloadConversionError> {
    if inputs.versioned_hashes.iter().any(|hash| hash[0] != 1) ||
        inputs.execution_requests_list.len() > if is_gloas { 5 } else { 3 }
    {
        return Err(PayloadConversionError::ContextEncoding);
    }
    let mut previous = None;
    let mut requests = Requests::with_capacity(inputs.execution_requests_list.len());
    for group in inputs.execution_requests_list {
        let (&kind, body) = group.split_first().ok_or(PayloadConversionError::ContextEncoding)?;
        if body.is_empty() || previous.is_some_and(|previous| kind <= previous) {
            return Err(PayloadConversionError::ContextEncoding);
        }
        let (width, limit) = match kind {
            0 => (
                gloas::DepositRequest::fixed_size(preset),
                if is_gloas { None } else { Some(8192) },
            ),
            1 => (
                gloas::WithdrawalRequest::fixed_size(preset),
                if is_gloas { None } else { Some(16) },
            ),
            2 => (
                gloas::ConsolidationRequest::fixed_size(preset),
                if is_gloas { None } else { Some(2) },
            ),
            3 if is_gloas => (gloas::BuilderDepositRequest::fixed_size(preset), None),
            4 if is_gloas => (gloas::BuilderExitRequest::fixed_size(preset), None),
            _ => return Err(PayloadConversionError::ContextEncoding),
        };
        let width = width.ok_or(PayloadConversionError::ContextEncoding)?;
        if !body.len().is_multiple_of(width) ||
            limit.is_some_and(|limit| body.len() / width > limit)
        {
            return Err(PayloadConversionError::ContextEncoding);
        }
        previous = Some(kind);
        requests.push_request(Bytes::copy_from_slice(group));
    }
    Ok(ExecutionPayloadSidecar::v4(
        CancunPayloadFields {
            parent_beacon_block_root: B256::from(inputs.parent_beacon_block_root),
            versioned_hashes: inputs.versioned_hashes.iter().copied().map(B256::from).collect(),
        },
        PraguePayloadFields::new(requests),
    ))
}
