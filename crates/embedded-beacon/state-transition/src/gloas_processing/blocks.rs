//! Exact synchronous Gloas beacon block sequencing; execution envelopes validate separately.

use super::{
    builder, checked_constants, common, operations, payload, transact, with_altair, withdrawals,
    GloasParameters,
};
use crate::{
    altair_processing,
    phase0_processing::{position, Constants, ProcessingError},
    ssz::gloas::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock, SyncAggregate},
};
use reth_embedded_beacon_crypto::{compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{sha256, uint64_root, BeaconBlockHeader};

/// Select the Gloas balance-weighted proposer from the authenticated state.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_beacon_proposer_index(state, parameters, &constants)
}

pub(crate) fn verify_block_signature_kernel(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let index = common::index(block.message.proposer_index, state.validators.values.len())?;
    let root = compute_signing_root(
        block.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [0; 4], block.message.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&state.validators.values[index].pubkey, &root, &block.signature)?)
}
/// Verify only the outer proposer signature; beacon import also verifies slots and the complete
/// post-state root.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.preset())?;
    verify_block_signature_kernel(state, block, parameters)
}
pub(crate) fn header_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if block.slot != state.slot || block.slot <= state.latest_block_header.slot {
        return Err(ProcessingError::InvalidState("block slot does not match a newer state slot"));
    }
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    if block.proposer_index != proposer {
        return Err(ProcessingError::InvalidState("block proposer is not selected Gloas proposer"));
    }
    if block.parent_root != state.latest_block_header.hash_tree_root() {
        return Err(ProcessingError::InvalidState("block parent differs from latest beacon header"));
    }
    if state.validators.values[common::index(proposer, state.validators.values.len())?].slashed {
        return Err(ProcessingError::InvalidState("block proposer is slashed"));
    }
    state.latest_block_header = BeaconBlockHeader {
        slot: block.slot,
        proposer_index: proposer,
        parent_root: block.parent_root,
        state_root: [0; 32],
        body_root: block.body.hash_tree_root(parameters.preset())?,
    };
    Ok(())
}
/// Atomically process only the full Gloas body header.
pub fn process_block_header(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        header_kernel(candidate, block, parameters, constants)
    })
}

pub(crate) fn randao_kernel(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    let key =
        state.validators.values[common::index(proposer, state.validators.values.len())?].pubkey;
    verify_signature(
        &key,
        &compute_signing_root(uint64_root(epoch), common::domain(state, [2, 0, 0, 0], epoch)),
        reveal,
    )?;
    let mix = state
        .randao_mixes
        .values
        .get_mut(position(epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    for (byte, hashed) in mix.iter_mut().zip(sha256(reveal)) {
        *byte ^= hashed;
    }
    Ok(())
}
/// Verify the Gloas selected proposer's RANDAO signature before atomically changing the mix.
pub fn process_randao(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        randao_kernel(candidate, &body.randao_reveal, parameters, constants)
    })
}
/// Apply only the unchanged Eth1 majority-vote operation using exact inherited constants.
pub fn process_eth1_data(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| {
            altair_processing::block::eth1_vote_kernel(view, &body.eth1_data, constants)
        })
    })
}
/// Verify the sync aggregate using Gloas proposer selection and exact rewards.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    aggregate.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        common::sync_aggregate_kernel(candidate, aggregate, parameters, constants)
    })
}

/// Raw complete block sequence; callers own candidate isolation, slots, outer BLS and post-root.
pub(crate) fn process_block(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let parent_slot = state.latest_block_header.slot;
    payload::parent_execution_kernel(state, block, parameters, constants)?;
    header_kernel(state, block, parameters, constants)?;
    withdrawals::withdrawals_kernel(state, parameters)?;
    builder::bid_kernel(state, &block.body.signed_execution_payload_bid, parameters, constants)?;
    randao_kernel(state, &block.body.randao_reveal, parameters, constants)?;
    with_altair(state, |view| {
        altair_processing::block::eth1_vote_kernel(view, &block.body.eth1_data, constants)
    })?;
    operations::operations_kernel(state, &block.body, parameters, constants, parent_slot)?;
    common::sync_aggregate_kernel(state, &block.body.sync_aggregate, parameters, constants)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ssz::{gloas, Preset, PresetBits, ProgressiveList, Ssz};
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;

    pub(crate) fn state_and_keys() -> (BeaconState, Vec<SecretKey>, GloasParameters) {
        let (pre, keys, fulu) = crate::fulu_processing::tests::state_and_keys();
        let mut parameters = GloasParameters::for_preset(Preset::Minimal);
        parameters.fulu = fulu;
        parameters.fork_epoch = 0;
        (super::super::upgrade_to_gloas(&pre, &parameters).unwrap(), keys, parameters)
    }
    pub(crate) fn empty_requests() -> gloas::ExecutionRequests {
        gloas::ExecutionRequests {
            deposits: ProgressiveList::default(),
            withdrawals: ProgressiveList::default(),
            consolidations: ProgressiveList::default(),
            builder_deposits: ProgressiveList::default(),
            builder_exits: ProgressiveList::default(),
        }
    }
    pub(crate) fn body(state: &BeaconState, parameters: &GloasParameters) -> BeaconBlockBody {
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        BeaconBlockBody {
            randao_reveal: [0; 96],
            eth1_data: state.eth1_data.clone(),
            graffiti: [0; 32],
            proposer_slashings: ProgressiveList::default(),
            attester_slashings: ProgressiveList::default(),
            attestations: ProgressiveList::default(),
            deposits: ProgressiveList::default(),
            voluntary_exits: ProgressiveList::default(),
            sync_aggregate: SyncAggregate {
                sync_committee_bits: PresetBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
                sync_committee_signature: infinity,
            },
            bls_to_execution_changes: ProgressiveList::default(),
            signed_execution_payload_bid: gloas::SignedExecutionPayloadBid {
                message: gloas::ExecutionPayloadBid {
                    parent_block_hash: state.latest_block_hash,
                    parent_block_root: state.block_roots.values[((state.slot - 1) % 64) as usize],
                    block_hash: [10; 32],
                    prev_randao: state.randao_mixes.values[(state.slot / 8 % 64) as usize],
                    fee_recipient: [8; 20],
                    gas_limit: 30_000_000,
                    builder_index: u64::MAX,
                    slot: state.slot,
                    value: 0,
                    execution_payment: 0,
                    blob_kzg_commitments: ProgressiveList::default(),
                    execution_requests_root: empty_requests()
                        .hash_tree_root(parameters.preset())
                        .unwrap(),
                },
                signature: infinity,
            },
            payload_attestations: ProgressiveList::default(),
            parent_execution_requests: empty_requests(),
        }
    }
    pub(crate) fn candidate(
        state: &BeaconState,
        keys: &[SecretKey],
        parameters: &GloasParameters,
    ) -> (SignedBeaconBlock, BeaconState) {
        let mut expected = state.clone();
        super::super::process_slots(&mut expected, state.slot + 1, parameters).unwrap();
        let proposer = get_beacon_proposer_index(&expected, parameters).unwrap();
        let key = &keys[proposer as usize];
        let mut body = body(&expected, parameters);
        body.randao_reveal = key
            .sign(
                &compute_signing_root(
                    uint64_root(expected.slot / 8),
                    common::domain(&expected, [2, 0, 0, 0], expected.slot / 8),
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        let mut message = BeaconBlock {
            slot: expected.slot,
            proposer_index: proposer,
            parent_root: expected.latest_block_header.hash_tree_root(),
            state_root: [0; 32],
            body,
        };
        let constants = checked_constants(&expected, parameters).unwrap();
        process_block(&mut expected, &message, parameters, &constants).unwrap();
        message.state_root = expected.hash_tree_root(parameters.preset()).unwrap();
        let signature = key
            .sign(
                &compute_signing_root(
                    message.hash_tree_root(parameters.preset()).unwrap(),
                    common::domain(&expected, [0; 4], message.slot / 8),
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        (SignedBeaconBlock { message, signature }, expected)
    }
    #[test]
    fn strict_beacon_import_uses_full_progressive_postroot_and_mandatory_signatures() {
        let (mut state, keys, parameters) = state_and_keys();
        let before = state.clone();
        let (block, expected) = candidate(&state, &keys, &parameters);
        let mut invalid = block.clone();
        invalid.signature = [0; 96];
        assert!(super::super::state_transition(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        invalid = block.clone();
        invalid.message.state_root[0] ^= 1;
        invalid.signature = keys[invalid.message.proposer_index as usize]
            .sign(
                &compute_signing_root(
                    invalid.message.hash_tree_root(parameters.preset()).unwrap(),
                    common::domain(&state, [0; 4], invalid.message.slot / 8),
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        assert!(matches!(
            super::super::state_transition(&mut state, &invalid, &parameters),
            Err(ProcessingError::InvalidState("block post-state root mismatch"))
        ));
        assert_eq!(state, before);
        super::super::state_transition(&mut state, &block, &parameters).unwrap();
        assert_eq!(state, expected);
        assert_eq!(state.latest_execution_payload_bid.builder_index, u64::MAX);
        assert_eq!(state.latest_block_hash, [9; 32]);
    }
    #[test]
    fn progressive_body_limits_are_enforced_before_acceptance_and_failure_is_atomic() {
        let (mut state, keys, parameters) = state_and_keys();
        let (block, _) = candidate(&state, &keys, &parameters);
        super::super::process_slots(&mut state, block.message.slot, &parameters).unwrap();
        let mut body = block.message.body;
        let operation = gloas::PayloadAttestation {
            aggregation_bits: PresetBits::from_ssz(&[0; 2], Preset::Minimal).unwrap(),
            data: gloas::PayloadAttestationData {
                beacon_block_root: [0; 32],
                slot: 0,
                payload_present: false,
                blob_data_available: false,
            },
            signature: [0; 96],
        };
        body.payload_attestations = ProgressiveList::from_vec(vec![operation; 5]);
        // Progressive SSZ itself permits this; consensus operation limits still reject it.
        body.hash_tree_root(parameters.preset()).unwrap();
        let before = state.clone();
        assert!(matches!(
            operations::process_operations(&mut state, &body, 0, &parameters),
            Err(ProcessingError::InvalidState(
                "Gloas body operation count exceeds consensus limits"
            ))
        ));
        assert_eq!(state, before);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "blocks_artificial.rs"]
pub(crate) mod artificial;
