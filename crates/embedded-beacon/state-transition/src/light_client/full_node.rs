//! Derive light-client data from complete post states and their committed signed blocks.
//!
//! Callers supply blocks and states admitted by the full consensus/availability import path.
//! These helpers check their exact commitments and sync signatures; derivation itself does not
//! grant execution validity, data availability, or fork-choice authority.

use super::{
    headers::{empty_header, to_deneb},
    merkle,
    processing::gindices,
    types::{branch_depths, empty_committee},
    Bootstrap, FinalityUpdate, Header, LightClientError, LightClientParameters, OptimisticUpdate,
    Update,
};
use crate::ssz::{
    altair::{SyncAggregate, SyncCommittee},
    auxiliary, capella, BeaconState, Preset, SignedBeaconBlock, Ssz, Vector,
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root};

macro_rules! payload_header {
    ($payload:expr,$preset:expr) => {{
        let payload = $payload;
        capella::ExecutionPayloadHeader {
            parent_hash: payload.parent_hash,
            fee_recipient: payload.fee_recipient,
            state_root: payload.state_root,
            receipts_root: payload.receipts_root,
            logs_bloom: payload.logs_bloom.clone(),
            prev_randao: payload.prev_randao,
            block_number: payload.block_number,
            gas_limit: payload.gas_limit,
            gas_used: payload.gas_used,
            timestamp: payload.timestamp,
            extra_data: payload.extra_data.clone(),
            base_fee_per_gas: payload.base_fee_per_gas,
            block_hash: payload.block_hash,
            transactions_root: payload.transactions.hash_tree_root($preset)?,
            withdrawals_root: payload.withdrawals.hash_tree_root($preset)?,
        }
    }};
}
macro_rules! deneb_header {
    ($value:expr,$preset:expr,$beacon:expr) => {{
        let value = $value;
        let payload = &value.message.body.execution_payload;
        let mut execution = to_deneb(&payload_header!(payload, *$preset));
        execution.blob_gas_used = payload.blob_gas_used;
        execution.excess_blob_gas = payload.excess_blob_gas;
        Header::Deneb(Box::new(auxiliary::deneb::LightClientHeader {
            beacon: $beacon,
            execution,
            execution_branch: Vector::from_vec(merkle::compute_merkle_proof(
                &value.message.body,
                25,
                *$preset,
            )?),
        }))
    }};
}
/// Derive a fork-specific header and proof from the actual complete signed block body.
pub fn block_to_light_client_header(
    block: &SignedBeaconBlock,
    params: &LightClientParameters,
) -> Result<Header, LightClientError> {
    params.validate()?;
    let beacon = block.header()?;
    block.hash_tree_root()?;
    let header = match block {
        SignedBeaconBlock::Phase0(_, _) => return Err(LightClientError::Unsupported(Fork::Phase0)),
        SignedBeaconBlock::Altair(_, _) | SignedBeaconBlock::Bellatrix(_, _) => {
            Header::Altair(auxiliary::altair::LightClientHeader { beacon })
        }
        SignedBeaconBlock::Capella(value, preset) => {
            Header::Capella(Box::new(auxiliary::capella::LightClientHeader {
                beacon,
                execution: payload_header!(&value.message.body.execution_payload, *preset),
                execution_branch: Vector::from_vec(merkle::compute_merkle_proof(
                    &value.message.body,
                    25,
                    *preset,
                )?),
            }))
        }
        SignedBeaconBlock::Deneb(value, preset) => deneb_header!(value, preset, beacon),
        SignedBeaconBlock::Electra(value, preset) => deneb_header!(value, preset, beacon),
        SignedBeaconBlock::Fulu(value, preset) => deneb_header!(value, preset, beacon),
        SignedBeaconBlock::Gloas(value, preset) => {
            Header::Gloas(Box::new(auxiliary::gloas::LightClientHeader {
                beacon,
                execution_block_hash: value
                    .message
                    .body
                    .signed_execution_payload_bid
                    .message
                    .parent_block_hash,
                execution_branch: Vector::from_vec(merkle::compute_merkle_proof(
                    &value.message.body,
                    2856,
                    *preset,
                )?),
            }))
        }
    };
    let preset = block_preset(block);
    if preset != params.preset() {
        return Err(LightClientError::Invalid("block preset differs from trusted parameters"))
    }
    if !super::is_valid_light_client_header(&header, params)? {
        return Err(LightClientError::Invalid("block execution proof or slot fork mismatch"))
    }
    Ok(header)
}
const fn block_preset(block: &SignedBeaconBlock) -> Preset {
    match block {
        SignedBeaconBlock::Phase0(_, preset) |
        SignedBeaconBlock::Altair(_, preset) |
        SignedBeaconBlock::Bellatrix(_, preset) |
        SignedBeaconBlock::Capella(_, preset) |
        SignedBeaconBlock::Deneb(_, preset) |
        SignedBeaconBlock::Electra(_, preset) |
        SignedBeaconBlock::Fulu(_, preset) |
        SignedBeaconBlock::Gloas(_, preset) => *preset,
    }
}
const fn state_preset(state: &BeaconState) -> Preset {
    match state {
        BeaconState::Phase0(_, preset) |
        BeaconState::Altair(_, preset) |
        BeaconState::Bellatrix(_, preset) |
        BeaconState::Capella(_, preset) |
        BeaconState::Deneb(_, preset) |
        BeaconState::Electra(_, preset) |
        BeaconState::Fulu(_, preset) |
        BeaconState::Gloas(_, preset) => *preset,
    }
}
/// Validate exact same-slot state commitment and latest-header linkage before derivation.
pub fn validate_state_block_link(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    params.validate()?;
    if state_preset(state) != params.preset() ||
        block_preset(block) != params.preset() ||
        state.schema_fork() != block.schema_fork() ||
        state.genesis_validators_root() != params.genesis_validators_root
    {
        return Err(LightClientError::Invalid("state/block network, schema or preset mismatch"))
    }
    let mut latest = state.latest_block_header();
    let block_header = block.header()?;
    let state_root = state.hash_tree_root()?;
    if state.slot() != latest.slot ||
        state.slot() != block_header.slot ||
        block_header.state_root != state_root
    {
        return Err(LightClientError::Invalid("post state is not committed by the same-slot block"))
    }
    latest.state_root = state_root;
    if latest.hash_tree_root() != block.message_root()? {
        return Err(LightClientError::Invalid("latest header does not identify the committed block"))
    }
    Ok(())
}
macro_rules! read_committee {
    ($state:expr,$field:ident) => {{
        let value = $state;
        SyncCommittee {
            pubkeys: value.$field.pubkeys.clone(),
            aggregate_pubkey: value.$field.aggregate_pubkey,
        }
    }};
}
pub(super) fn committee(
    state: &BeaconState,
    next: bool,
) -> Result<SyncCommittee, LightClientError> {
    macro_rules! read {
        ($value:expr) => {
            if next {
                read_committee!($value, next_sync_committee)
            } else {
                read_committee!($value, current_sync_committee)
            }
        };
    }
    Ok(match state {
        BeaconState::Phase0(_, _) => return Err(LightClientError::Unsupported(Fork::Phase0)),
        BeaconState::Altair(value, _) => read!(value),
        BeaconState::Bellatrix(value, _) => read!(value),
        BeaconState::Capella(value, _) => read!(value),
        BeaconState::Deneb(value, _) => read!(value),
        BeaconState::Electra(value, _) => read!(value),
        BeaconState::Fulu(value, _) => read!(value),
        BeaconState::Gloas(value, _) => read!(value),
    })
}
/// Exact finalized checkpoint from the actual fork-specific state.
pub const fn finalized_checkpoint(state: &BeaconState) -> Checkpoint {
    match state {
        BeaconState::Phase0(value, _) => value.finalized_checkpoint,
        BeaconState::Altair(value, _) => value.finalized_checkpoint,
        BeaconState::Bellatrix(value, _) => value.finalized_checkpoint,
        BeaconState::Capella(value, _) => value.finalized_checkpoint,
        BeaconState::Deneb(value, _) => value.finalized_checkpoint,
        BeaconState::Electra(value, _) => value.finalized_checkpoint,
        BeaconState::Fulu(value, _) => value.finalized_checkpoint,
        BeaconState::Gloas(value, _) => value.finalized_checkpoint,
    }
}
fn aggregate(block: &SignedBeaconBlock) -> Result<SyncAggregate, LightClientError> {
    macro_rules! read {
        ($value:expr) => {
            SyncAggregate {
                sync_committee_bits: $value.message.body.sync_aggregate.sync_committee_bits.clone(),
                sync_committee_signature: $value
                    .message
                    .body
                    .sync_aggregate
                    .sync_committee_signature,
            }
        };
    }
    Ok(match block {
        SignedBeaconBlock::Phase0(_, _) => return Err(LightClientError::Unsupported(Fork::Phase0)),
        SignedBeaconBlock::Altair(value, _) => read!(value),
        SignedBeaconBlock::Bellatrix(value, _) => read!(value),
        SignedBeaconBlock::Capella(value, _) => read!(value),
        SignedBeaconBlock::Deneb(value, _) => read!(value),
        SignedBeaconBlock::Electra(value, _) => read!(value),
        SignedBeaconBlock::Fulu(value, _) => read!(value),
        SignedBeaconBlock::Gloas(value, _) => read!(value),
    })
}
fn normalize(proof: Vec<Root>, depth: usize) -> Result<Vec<Root>, LightClientError> {
    if proof.len() > depth {
        return Err(LightClientError::Invalid("proof cannot fit target data schema"))
    }
    let mut normalized = vec![[0; 32]; depth - proof.len()];
    normalized.extend(proof);
    Ok(normalized)
}
/// Produce a bootstrap only from a complete post state committed by its exact block.
pub fn create_light_client_bootstrap(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    params: &LightClientParameters,
) -> Result<Bootstrap, LightClientError> {
    validate_state_block_link(state, block, params)?;
    if state.schema_fork() == Fork::Phase0 {
        return Err(LightClientError::Unsupported(Fork::Phase0))
    }
    let (depth, _) = branch_depths(block.schema_fork())?;
    let header = block_to_light_client_header(block, params)?;
    Ok(Bootstrap {
        schema: block.schema_fork(),
        header,
        current_sync_committee: committee(state, false)?,
        current_sync_committee_branch: normalize(
            merkle::compute_state_merkle_proof(state, gindices(state.slot(), params).0)?,
            depth,
        )?,
    })
}
/// Derive a complete update with exact parent/finalized links, proofs and real aggregate BLS.
pub fn create_light_client_update(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    attested_state: &BeaconState,
    attested_block: &SignedBeaconBlock,
    finalized_block: Option<&SignedBeaconBlock>,
    params: &LightClientParameters,
) -> Result<Update, LightClientError> {
    create_update_with_schema(
        state,
        block,
        attested_state,
        attested_block,
        finalized_block,
        params,
        block.schema_fork(),
    )
}

/// Serving data retains the attested block's declared schema across a signing fork boundary.
pub(super) fn create_serving_update(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    attested_state: &BeaconState,
    attested_block: &SignedBeaconBlock,
    finalized_block: Option<&SignedBeaconBlock>,
    params: &LightClientParameters,
) -> Result<Update, LightClientError> {
    create_update_with_schema(
        state,
        block,
        attested_state,
        attested_block,
        finalized_block,
        params,
        attested_block.schema_fork(),
    )
}

fn create_update_with_schema(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    attested_state: &BeaconState,
    attested_block: &SignedBeaconBlock,
    finalized_block: Option<&SignedBeaconBlock>,
    params: &LightClientParameters,
    schema: Fork,
) -> Result<Update, LightClientError> {
    validate_state_block_link(state, block, params)?;
    validate_state_block_link(attested_state, attested_block, params)?;
    let (committee_depth, finality_depth) = branch_depths(schema)?;
    if block.header()?.parent_root != attested_block.message_root()? ||
        block.header()?.slot <= attested_state.slot()
    {
        return Err(LightClientError::Invalid(
            "attested block is not the signing block's earlier parent",
        ))
    }
    let attested_period =
        super::compute_sync_committee_period_at_slot(attested_state.slot(), params)?;
    let signature_slot = block.header()?.slot;
    let signature_period = super::compute_sync_committee_period_at_slot(signature_slot, params)?;
    let sync_aggregate = aggregate(block)?;
    sync_aggregate.hash_tree_root(params.preset())?;
    // A full node has the actual signing block's committed state, including the
    // committee after any intervening period changes. Do not project a distant
    // parent's two cached committees onto a later signing period.
    let current_committee = committee(state, false)?;
    let participant_keys: Vec<_> = current_committee
        .pubkeys
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, key)| {
            (sync_aggregate.sync_committee_bits.bits.get(index) == Some(true)).then_some(*key)
        })
        .collect();
    let signing_root = compute_signing_root(
        attested_block.message_root()?,
        compute_domain(
            [7, 0, 0, 0],
            params.version_at_slot(signature_slot.saturating_sub(1))?,
            params.genesis_validators_root,
        ),
    );
    fast_aggregate_verify(
        &participant_keys,
        &signing_root,
        &sync_aggregate.sync_committee_signature,
    )?;
    let mut update = Update {
        schema,
        attested_header: super::upgrade_header(
            &block_to_light_client_header(attested_block, params)?,
            schema,
            params,
        )?,
        next_sync_committee: empty_committee(params.preset()),
        next_sync_committee_branch: vec![[0; 32]; committee_depth],
        finalized_header: empty_header(schema, params.preset())?,
        finality_branch: vec![[0; 32]; finality_depth],
        sync_aggregate,
        signature_slot,
    };
    if attested_period == signature_period {
        update.next_sync_committee = committee(attested_state, true)?;
        update.next_sync_committee_branch = normalize(
            merkle::compute_state_merkle_proof(
                attested_state,
                gindices(attested_state.slot(), params).1,
            )?,
            committee_depth,
        )?;
    }
    if let Some(finalized) = finalized_block {
        if finalized.header()?.slot != 0 {
            if finalized.message_root()? != finalized_checkpoint(attested_state).root {
                return Err(LightClientError::Invalid(
                    "finalized block root differs from parent state's checkpoint",
                ))
            }
            update.finalized_header = super::upgrade_header(
                &block_to_light_client_header(finalized, params)?,
                schema,
                params,
            )?;
        } else if finalized_checkpoint(attested_state).root != [0; 32] {
            return Err(LightClientError::Invalid(
                "genesis finalized checkpoint must have zero root",
            ))
        }
        update.finality_branch = normalize(
            merkle::compute_state_merkle_proof(
                attested_state,
                gindices(attested_state.slot(), params).2,
            )?,
            finality_depth,
        )?;
    }
    Ok(update)
}
/// Derive compact finality data from the already generated complete update.
pub fn create_light_client_finality_update(update: &Update) -> FinalityUpdate {
    FinalityUpdate {
        schema: update.schema,
        attested_header: update.attested_header.clone(),
        finalized_header: update.finalized_header.clone(),
        finality_branch: update.finality_branch.clone(),
        sync_aggregate: update.sync_aggregate.clone(),
        signature_slot: update.signature_slot,
    }
}
/// Derive compact optimistic data from the already generated complete update.
pub fn create_light_client_optimistic_update(update: &Update) -> OptimisticUpdate {
    OptimisticUpdate {
        schema: update.schema,
        attested_header: update.attested_header.clone(),
        sync_aggregate: update.sync_aggregate.clone(),
        signature_slot: update.signature_slot,
    }
}

/// Whether the exact block aggregate has any participant; bounds are rechecked first.
pub fn has_sync_committee_participants(
    block: &SignedBeaconBlock,
) -> Result<bool, LightClientError> {
    let aggregate = aggregate(block)?;
    aggregate.hash_tree_root(block_preset(block))?;
    Ok(aggregate.sync_committee_bits.bits.as_ssz().iter().any(|byte| *byte != 0))
}
