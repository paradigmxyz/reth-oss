use super::{
    headers::{empty_capella, to_deneb, zero_beacon},
    types::{committee_size, empty_committee},
    *,
};
use crate::{
    fork_processing::{ForkActivation, TransitionParameters},
    gloas_processing::GloasParameters,
    ssz::{
        altair::{SyncAggregate, SyncCommittee, SyncCommitteeBits},
        auxiliary, Preset, Ssz, Vector,
    },
};
use blst::min_pk::{AggregateSignature, SecretKey};
use reth_embedded_beacon_crypto::{
    aggregate_pubkeys, compute_domain, compute_signing_root, ETHEREUM_BLS_DST,
};
use reth_embedded_beacon_primitives::{
    hash_pair, merkleize, uint64_root, BeaconBlockHeader, Fork, Root,
};

fn params() -> LightClientParameters {
    let mut inherited = GloasParameters::for_preset(Preset::Minimal);
    inherited.fulu.blob_schedule.clear();
    let forks = [
        Fork::Phase0,
        Fork::Altair,
        Fork::Bellatrix,
        Fork::Capella,
        Fork::Deneb,
        Fork::Electra,
        Fork::Fulu,
        Fork::Gloas,
    ];
    let schedule = forks
        .into_iter()
        .enumerate()
        .map(|(index, fork)| ForkActivation {
            fork,
            epoch: if index < 2 { 0 } else { u64::MAX },
            version: [index as u8, 0, 0, 1],
        })
        .collect();
    LightClientParameters {
        transitions: TransitionParameters::new(inherited, schedule).unwrap(),
        genesis_validators_root: [42; 32],
    }
}
fn keys(seed: u8) -> Vec<SecretKey> {
    (0..32)
        .map(|index| {
            let mut material = [seed; 32];
            material[0] = index;
            SecretKey::key_gen(&material, &[]).unwrap()
        })
        .collect()
}
fn committee(keys: &[SecretKey]) -> SyncCommittee {
    let pubkeys: Vec<_> = keys.iter().map(|key| key.sk_to_pk().to_bytes()).collect();
    SyncCommittee {
        aggregate_pubkey: aggregate_pubkeys(&pubkeys).unwrap(),
        pubkeys: Vector::from_vec(pubkeys),
    }
}
fn proof_root(leaf: Root, branch: &[Root], gindex: u64) -> Root {
    let mut root = leaf;
    for (level, sibling) in branch.iter().enumerate() {
        root = if (gindex >> level) & 1 == 1 {
            hash_pair(*sibling, root)
        } else {
            hash_pair(root, *sibling)
        };
    }
    root
}
fn merkle_proof(leaves: &[Root], mut index: usize) -> Vec<Root> {
    let mut level = leaves.to_vec();
    level.resize(level.len().next_power_of_two(), [0; 32]);
    let mut proof = Vec::new();
    while level.len() > 1 {
        proof.push(level[index ^ 1]);
        level = level.chunks_exact(2).map(|pair| hash_pair(pair[0], pair[1])).collect();
        index /= 2;
    }
    proof
}
fn bootstrap(committee: SyncCommittee, params: &LightClientParameters) -> Bootstrap {
    let branch = vec![[17; 32]; 5];
    let state_root = proof_root(committee.hash_tree_root(params.preset()).unwrap(), &branch, 54);
    Bootstrap {
        schema: Fork::Altair,
        header: Header::Altair(auxiliary::altair::LightClientHeader {
            beacon: BeaconBlockHeader { slot: 1, state_root, ..zero_beacon() },
        }),
        current_sync_committee: committee,
        current_sync_committee_branch: branch,
    }
}
fn sign(update: &mut Update, keys: &[SecretKey], count: usize, params: &LightClientParameters) {
    let mut bytes = vec![0u8; committee_size(params.preset()) / 8];
    for index in 0..count {
        bytes[index / 8] |= 1 << (index % 8)
    }
    let domain = compute_domain(
        [7, 0, 0, 0],
        params.version_at_slot(update.signature_slot - 1).unwrap(),
        params.genesis_validators_root,
    );
    let root = compute_signing_root((*update.attested_header.beacon()).hash_tree_root(), domain);
    let signatures: Vec<_> =
        keys[..count].iter().map(|key| key.sign(&root, ETHEREUM_BLS_DST, &[])).collect();
    let signatures: Vec<_> = signatures.iter().collect();
    update.sync_aggregate = SyncAggregate {
        sync_committee_bits: SyncCommitteeBits::from_ssz(&bytes, params.preset()).unwrap(),
        sync_committee_signature: AggregateSignature::aggregate(&signatures, true)
            .unwrap()
            .to_signature()
            .to_bytes(),
    };
}
fn optimistic(
    slot: u64,
    keys: &[SecretKey],
    count: usize,
    params: &LightClientParameters,
) -> Update {
    let mut update = Update {
        schema: Fork::Altair,
        attested_header: Header::Altair(auxiliary::altair::LightClientHeader {
            beacon: BeaconBlockHeader { slot, ..zero_beacon() },
        }),
        next_sync_committee: empty_committee(params.preset()),
        next_sync_committee_branch: vec![[0; 32]; 5],
        finalized_header: Header::empty(Fork::Altair, params.preset()).unwrap(),
        finality_branch: vec![[0; 32]; 6],
        sync_aggregate: SyncAggregate {
            sync_committee_bits: SyncCommitteeBits::from_ssz(&[0; 4], params.preset()).unwrap(),
            sync_committee_signature: [0; 96],
        },
        signature_slot: slot + 1,
    };
    sign(&mut update, keys, count, params);
    update
}
fn finalizing(
    attested: u64,
    finalized: u64,
    next: SyncCommittee,
    keys: &[SecretKey],
    params: &LightClientParameters,
) -> Update {
    let mut update = optimistic(attested, keys, 32, params);
    update.finalized_header = Header::Altair(auxiliary::altair::LightClientHeader {
        beacon: BeaconBlockHeader { slot: finalized, ..zero_beacon() },
    });
    let mut leaves = vec![[0; 32]; 32];
    let finalized_root = (*update.finalized_header.beacon()).hash_tree_root();
    let finalized_epoch = uint64_root(finalized / params.slots_per_epoch());
    leaves[20] = hash_pair(finalized_epoch, finalized_root);
    leaves[23] = next.hash_tree_root(params.preset()).unwrap();
    update.next_sync_committee = next;
    update.next_sync_committee_branch = merkle_proof(&leaves, 23);
    update.finality_branch = vec![finalized_epoch];
    update.finality_branch.extend(merkle_proof(&leaves, 20));
    update.attested_header = Header::Altair(auxiliary::altair::LightClientHeader {
        beacon: BeaconBlockHeader {
            slot: attested,
            state_root: merkleize(&leaves, 32).unwrap(),
            ..zero_beacon()
        },
    });
    sign(&mut update, keys, 32, params);
    update
}
#[test]
fn bootstrap_authenticates_root_and_committee_proof_before_store_creation() {
    let params = params();
    let bootstrap = bootstrap(committee(&keys(11)), &params);
    let trusted = (*bootstrap.header.beacon()).hash_tree_root();
    let store = initialize_light_client_store(trusted, &bootstrap, &params).unwrap();
    assert_eq!(store.current_sync_committee(), &bootstrap.current_sync_committee);
    assert!(!is_next_sync_committee_known(&store));
    assert!(initialize_light_client_store([99; 32], &bootstrap, &params).is_err());
    let mut forged = bootstrap;
    forged.current_sync_committee_branch[0][0] ^= 1;
    assert!(initialize_light_client_store(trusted, &forged, &params).is_err());
}
#[test]
fn real_signature_optimistic_progress_and_invalid_update_are_atomic() {
    let params = params();
    let keys = keys(12);
    let bootstrap = bootstrap(committee(&keys), &params);
    let mut store = initialize_light_client_store(
        (*bootstrap.header.beacon()).hash_tree_root(),
        &bootstrap,
        &params,
    )
    .unwrap();
    let mut update = optimistic(3, &keys, 32, &params);
    process_light_client_update(&mut store, &update, 4, &params).unwrap();
    assert_eq!(store.optimistic_header().beacon().slot, 3);
    assert_eq!(store.finalized_header().beacon().slot, 1);
    assert_eq!(store.current_max_active_participants(), 32);
    let before = store.clone();
    update.sync_aggregate.sync_committee_signature = [0; 96];
    assert!(matches!(
        process_light_client_update(&mut store, &update, 4, &params),
        Err(LightClientError::Crypto(_))
    ));
    assert_eq!(store, before);
    let mut changed = params;
    changed.genesis_validators_root = [99; 32];
    assert!(process_light_client_store_force_update(&mut store, 100, &changed).is_err());
    assert_eq!(store, before);
}
#[test]
fn finality_learns_and_rotates_authentic_committees_across_periods() {
    let params = params();
    let keys = keys(13);
    let next_keys = super::tests::keys(14);
    let bootstrap = bootstrap(committee(&keys), &params);
    let mut store = initialize_light_client_store(
        (*bootstrap.header.beacon()).hash_tree_root(),
        &bootstrap,
        &params,
    )
    .unwrap();
    let update = finalizing(4, 2, committee(&next_keys), &keys, &params);
    process_light_client_update(&mut store, &update, 5, &params).unwrap();
    assert_eq!(store.finalized_header().beacon().slot, 2);
    assert_eq!(store.next_sync_committee(), &committee(&next_keys));
    let update = finalizing(65, 64, committee(&keys), &next_keys, &params);
    process_light_client_update(&mut store, &update, 66, &params).unwrap();
    assert_eq!(store.finalized_header().beacon().slot, 64);
    assert_eq!(store.current_sync_committee(), &committee(&next_keys));
    assert_eq!(store.next_sync_committee(), &committee(&keys));
    assert_eq!(store.previous_max_active_participants(), 32);
    assert_eq!(store.current_max_active_participants(), 0);
}
#[test]
fn explicit_timeout_has_strict_boundary_and_never_promotes_invalid_data() {
    let params = params();
    let keys = keys(15);
    let bootstrap = bootstrap(committee(&keys), &params);
    let mut store = initialize_light_client_store(
        (*bootstrap.header.beacon()).hash_tree_root(),
        &bootstrap,
        &params,
    )
    .unwrap();
    let update = optimistic(3, &keys, 3, &params);
    process_light_client_update(&mut store, &update, 4, &params).unwrap();
    process_light_client_store_force_update(&mut store, 65, &params).unwrap();
    assert_eq!(store.finalized_header().beacon().slot, 1);
    process_light_client_store_force_update(&mut store, 66, &params).unwrap();
    assert_eq!(store.finalized_header().beacon().slot, 3);
    assert!(store.best_valid_update().is_none());
}
#[test]
fn normalization_rejects_nonzero_padding_and_short_branches() {
    let leaf = [1; 32];
    let proof = vec![[2; 32]; 5];
    let root = proof_root(leaf, &proof, 54);
    let mut normalized = vec![[0; 32]; 6];
    normalized.extend_from_slice(&proof);
    assert!(is_valid_normalized_merkle_branch(leaf, &normalized, 54, root));
    normalized[0] = [8; 32];
    assert!(!is_valid_normalized_merkle_branch(leaf, &normalized, 54, root));
    assert!(!is_valid_normalized_merkle_branch(leaf, &proof[..4], 54, root));
    assert!(!is_valid_normalized_merkle_branch(leaf, &[], 0, leaf));
}
#[test]
fn gloas_upgrade_constructs_exact_old_execution_hash_proofs() {
    let mut params = params();
    let epochs = [0, 0, 1, 2, 3, 4, 5, 6];
    for (entry, epoch) in params.transitions.schedule.iter_mut().zip(epochs) {
        entry.epoch = epoch
    }
    params.transitions = TransitionParameters::new(
        params.transitions.gloas.clone(),
        params.transitions.schedule.clone(),
    )
    .unwrap();
    let mut execution = empty_capella();
    execution.block_hash = [18; 32];
    execution.extra_data.values = vec![0, 1, 255];
    let branch = vec![[19; 32]; 4];
    let beacon = BeaconBlockHeader {
        slot: 16,
        body_root: proof_root(execution.hash_tree_root(params.preset()).unwrap(), &branch, 25),
        ..zero_beacon()
    };
    let header = Header::Capella(Box::new(auxiliary::capella::LightClientHeader {
        beacon,
        execution: execution.clone(),
        execution_branch: Vector::from_vec(branch.clone()),
    }));
    let gloas = upgrade_header(&header, Fork::Gloas, &params).unwrap();
    assert!(is_valid_light_client_header(&gloas, &params).unwrap());
    assert_eq!(
        get_lc_execution_root(&gloas, &params).unwrap(),
        execution.hash_tree_root(params.preset()).unwrap()
    );
    let mut execution = to_deneb(&execution);
    execution.blob_gas_used = 262144;
    let beacon = BeaconBlockHeader {
        slot: 24,
        body_root: proof_root(execution.hash_tree_root(params.preset()).unwrap(), &branch, 25),
        ..zero_beacon()
    };
    let header = Header::Deneb(Box::new(auxiliary::deneb::LightClientHeader {
        beacon,
        execution: execution.clone(),
        execution_branch: Vector::from_vec(branch),
    }));
    let gloas = upgrade_header(&header, Fork::Gloas, &params).unwrap();
    assert!(is_valid_light_client_header(&gloas, &params).unwrap());
    assert_eq!(
        get_lc_execution_root(&gloas, &params).unwrap(),
        execution.hash_tree_root(params.preset()).unwrap()
    );
    if let Header::Gloas(mut forged) = gloas {
        forged.execution_branch.values[0] = [5; 32];
        assert!(!is_valid_light_client_header(&Header::Gloas(forged), &params).unwrap())
    }
}

#[test]
fn signature_at_fork_boundary_uses_previous_slot_version() {
    let mut params = params();
    let epochs = [0, 0, 1, 4, 4, u64::MAX, u64::MAX, u64::MAX];
    for (entry, epoch) in params.transitions.schedule.iter_mut().zip(epochs) {
        entry.epoch = epoch
    }
    params.transitions = TransitionParameters::new(
        params.transitions.gloas.clone(),
        params.transitions.schedule.clone(),
    )
    .unwrap();
    let keys = keys(16);
    let bootstrap = bootstrap(committee(&keys), &params);
    let mut store = initialize_light_client_store(
        (*bootstrap.header.beacon()).hash_tree_root(),
        &bootstrap,
        &params,
    )
    .unwrap();
    upgrade_store(&mut store, Fork::Deneb, &params).unwrap();
    let update = optimistic(31, &keys, 32, &params);
    let update = upgrade_update(&update, Fork::Deneb, &params).unwrap();
    process_light_client_update(&mut store, &update, 32, &params).unwrap();
    assert_eq!(store.optimistic_header().beacon().slot, 31);
    let before = store.clone();
    let mut bad = update;
    let domain = compute_domain(
        [7, 0, 0, 0],
        params.transitions.schedule[4].version,
        params.genesis_validators_root,
    );
    let root = compute_signing_root((*bad.attested_header.beacon()).hash_tree_root(), domain);
    let signatures: Vec<_> =
        keys.iter().map(|key| key.sign(&root, ETHEREUM_BLS_DST, &[])).collect();
    let refs: Vec<_> = signatures.iter().collect();
    bad.sync_aggregate.sync_committee_signature =
        AggregateSignature::aggregate(&refs, true).unwrap().to_signature().to_bytes();
    assert!(matches!(
        process_light_client_update(&mut store, &bad, 32, &params),
        Err(LightClientError::Crypto(_))
    ));
    assert_eq!(store, before);
}
#[test]
fn store_upgrade_preserves_authenticated_beacon_data_and_pads_exact_branches() {
    let params = params();
    let keys = keys(17);
    let bootstrap = bootstrap(committee(&keys), &params);
    let mut store = initialize_light_client_store(
        (*bootstrap.header.beacon()).hash_tree_root(),
        &bootstrap,
        &params,
    )
    .unwrap();
    let update = optimistic(3, &keys, 12, &params);
    process_light_client_update(&mut store, &update, 4, &params).unwrap();
    let before = store.clone();
    upgrade_store(&mut store, Fork::Gloas, &params).unwrap();
    assert_eq!(store.finalized_header().beacon(), before.finalized_header().beacon());
    assert_eq!(store.optimistic_header().beacon(), before.optimistic_header().beacon());
    assert_eq!(store.current_sync_committee(), before.current_sync_committee());
    assert_eq!(store.best_valid_update().unwrap().next_sync_committee_branch.len(), 11);
    assert_eq!(store.best_valid_update().unwrap().finality_branch.len(), 9);
    let upgraded = upgrade_update(&update, Fork::Gloas, &params).unwrap();
    validate_light_client_update(&store, &upgraded, 4, &params).unwrap();
    let before = store.clone();
    assert!(upgrade_store(&mut store, Fork::Electra, &params).is_err());
    assert_eq!(store, before);
}

#[test]
fn actual_progressive_state_and_bid_paths_match_complete_object_roots() {
    let (fulu, _, inherited) = crate::fulu_processing::tests::state_and_keys();
    let fulu_root = fulu.hash_tree_root(Preset::Minimal).unwrap();
    let fulu_proof = merkle::compute_merkle_proof(&fulu, 86, Preset::Minimal).unwrap();
    assert_eq!(
        merkle::tree_node(&fulu, 86, Preset::Minimal).unwrap(),
        fulu.current_sync_committee.hash_tree_root(Preset::Minimal).unwrap()
    );
    assert!(is_valid_normalized_merkle_branch(
        fulu.current_sync_committee.hash_tree_root(Preset::Minimal).unwrap(),
        &fulu_proof,
        86,
        fulu_root
    ));
    let mut params = GloasParameters::for_preset(Preset::Minimal);
    params.fulu = inherited;
    params.fork_epoch = 0;
    let state = crate::gloas_processing::upgrade_to_gloas(&fulu, &params).unwrap();
    let root = state.hash_tree_root(Preset::Minimal).unwrap();
    assert_eq!(merkle::tree_node(&state, 1, Preset::Minimal).unwrap(), root);
    for (gindex, leaf) in [
        (2945, state.current_sync_committee.hash_tree_root(Preset::Minimal).unwrap()),
        (2946, state.next_sync_committee.hash_tree_root(Preset::Minimal).unwrap()),
        (735, state.finalized_checkpoint.root),
    ] {
        assert_eq!(merkle::tree_node(&state, gindex, Preset::Minimal).unwrap(), leaf);
        let proof = merkle::compute_merkle_proof(&state, gindex, Preset::Minimal).unwrap();
        assert!(is_valid_normalized_merkle_branch(leaf, &proof, gindex, root));
    }
    use crate::ssz::{gloas, ProgressiveList};
    let mut bid = state.latest_execution_payload_bid.clone();
    bid.parent_block_hash = [81; 32];
    let body = gloas::BeaconBlockBody {
        randao_reveal: [0; 96],
        eth1_data: state.eth1_data,
        graffiti: [0; 32],
        proposer_slashings: ProgressiveList::default(),
        attester_slashings: ProgressiveList::default(),
        attestations: ProgressiveList::default(),
        deposits: ProgressiveList::default(),
        voluntary_exits: ProgressiveList::default(),
        sync_aggregate: gloas::SyncAggregate {
            sync_committee_bits: SyncCommitteeBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
            sync_committee_signature: [0; 96],
        },
        bls_to_execution_changes: ProgressiveList::default(),
        signed_execution_payload_bid: gloas::SignedExecutionPayloadBid {
            message: bid,
            signature: [0; 96],
        },
        payload_attestations: ProgressiveList::default(),
        parent_execution_requests: gloas::ExecutionRequests {
            deposits: ProgressiveList::default(),
            withdrawals: ProgressiveList::default(),
            consolidations: ProgressiveList::default(),
            builder_deposits: ProgressiveList::default(),
            builder_exits: ProgressiveList::default(),
        },
    };
    let proof = merkle::compute_merkle_proof(&body, 2856, Preset::Minimal).unwrap();
    assert_eq!(merkle::tree_node(&body, 2856, Preset::Minimal).unwrap(), [81; 32]);
    assert!(is_valid_normalized_merkle_branch(
        [81; 32],
        &proof,
        2856,
        body.hash_tree_root(Preset::Minimal).unwrap()
    ));
    assert!(matches!(
        merkle::tree_node(&body, 0, Preset::Minimal),
        Err(reth_embedded_beacon_primitives::SszError::UnsupportedMerklePath(0))
    ));
}

fn committed_altair_pair(
    slot: u64,
    parent_root: Root,
    mut state: crate::ssz::altair::BeaconState,
    sync: SyncAggregate,
) -> (crate::ssz::BeaconState, crate::ssz::SignedBeaconBlock) {
    use crate::ssz::{altair, BeaconState, List, SignedBeaconBlock};
    let body = altair::BeaconBlockBody {
        randao_reveal: [0; 96],
        eth1_data: state.eth1_data.clone(),
        graffiti: [0; 32],
        proposer_slashings: List::default(),
        attester_slashings: List::default(),
        attestations: List::default(),
        deposits: List::default(),
        voluntary_exits: List::default(),
        sync_aggregate: sync,
    };
    let mut block = altair::SignedBeaconBlock {
        message: altair::BeaconBlock {
            slot,
            proposer_index: 0,
            parent_root,
            state_root: [0; 32],
            body,
        },
        signature: [0; 96],
    };
    state.slot = slot;
    state.latest_block_header = block.message.header(Preset::Minimal).unwrap();
    block.message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    (BeaconState::Altair(state, Preset::Minimal), SignedBeaconBlock::Altair(block, Preset::Minimal))
}
#[test]
fn full_node_derivation_authenticates_complete_commitments_and_real_sync_signature() {
    use crate::{
        common::AltairFields,
        ssz::{BeaconState, SignedBeaconBlock},
    };
    let (mut base, keys) = crate::bellatrix_processing::tests::state_and_keys();
    // This independently constructed Altair unit state is never substituted for a later-fork root.
    let mut state = base.take_common();
    let mut params = params();
    params.genesis_validators_root = state.genesis_validators_root;
    state.fork.current_version = params.transitions.schedule[1].version;
    state.fork.previous_version = params.transitions.schedule[0].version;
    state.fork.epoch = 0;
    let aggregate = SyncAggregate {
        sync_committee_bits: SyncCommitteeBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
        sync_committee_signature: [0; 96],
    };
    let (parent_state, parent) = committed_altair_pair(1, [0; 32], state, aggregate);
    let bootstrap =
        full_node::create_light_client_bootstrap(&parent_state, &parent, &params).unwrap();
    initialize_light_client_store(parent.message_root().unwrap(), &bootstrap, &params).unwrap();
    let parent_root = parent.message_root().unwrap();
    let actual = match &parent_state {
        BeaconState::Altair(state, _) => state,
        _ => unreachable!(),
    };
    let domain = compute_domain(
        [7, 0, 0, 0],
        params.version_at_slot(1).unwrap(),
        params.genesis_validators_root,
    );
    let root = compute_signing_root(parent_root, domain);
    let signatures: Vec<_> = actual
        .current_sync_committee
        .pubkeys
        .values
        .iter()
        .map(|pubkey| {
            keys.iter().find(|key| key.sk_to_pk().to_bytes() == *pubkey).unwrap().sign(
                &root,
                ETHEREUM_BLS_DST,
                &[],
            )
        })
        .collect();
    let refs: Vec<_> = signatures.iter().collect();
    let sync = SyncAggregate {
        sync_committee_bits: SyncCommitteeBits::from_ssz(&[255; 4], Preset::Minimal).unwrap(),
        sync_committee_signature: AggregateSignature::aggregate(&refs, true)
            .unwrap()
            .to_signature()
            .to_bytes(),
    };
    let (state, block) = committed_altair_pair(2, parent_root, actual.clone(), sync);
    let update = full_node::create_light_client_update(
        &state,
        &block,
        &parent_state,
        &parent,
        None,
        &params,
    )
    .unwrap();
    assert_eq!(update.signature_slot, 2);
    assert_eq!((*update.attested_header.beacon()).hash_tree_root(), parent_root);
    let mut store = initialize_light_client_store(parent_root, &bootstrap, &params).unwrap();
    process_light_client_update(&mut store, &update, 2, &params).unwrap();
    let mut serving = data_collection::LightClientDataStore::new(
        &parent_state,
        &parent,
        &params,
        data_collection::LightClientDataLimits::default(),
    )
    .unwrap();
    serving.on_block(&state, &block, &parent_state, &parent, None).unwrap();
    serving.on_head(block.message_root().unwrap()).unwrap();
    assert_eq!(serving.latest_optimistic_update().unwrap().signature_slot, 2);
    let empty_sync = SyncAggregate {
        sync_committee_bits: SyncCommitteeBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
        sync_committee_signature: [0; 96],
    };
    let (sibling_state, sibling) =
        committed_altair_pair(3, parent_root, actual.clone(), empty_sync);
    serving.on_block(&sibling_state, &sibling, &parent_state, &parent, None).unwrap();
    serving.on_head(sibling.message_root().unwrap()).unwrap();
    assert!(serving.latest_optimistic_update().is_none());
    assert!(serving.best_update(0).is_none());
    assert!(serving.on_head([99; 32]).is_err());
    assert_eq!(serving.head(), sibling.message_root().unwrap());
    let mut capped = data_collection::LightClientDataStore::new(
        &parent_state,
        &parent,
        &params,
        data_collection::LightClientDataLimits {
            max_blocks: 1,
            max_encoded_bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap();
    assert!(capped.on_block(&state, &block, &parent_state, &parent, None).is_err());
    assert_eq!(capped.retained_blocks(), 1);
    let mut forged_state = state.clone();
    if let BeaconState::Altair(value, _) = &mut forged_state {
        value.balances.values[0] += 1
    }
    assert!(full_node::create_light_client_bootstrap(&forged_state, &block, &params).is_err());
    let mut forged_block = block;
    if let SignedBeaconBlock::Altair(value, _) = &mut forged_block {
        value.message.body.sync_aggregate.sync_committee_signature = [0; 96]
    }
    assert!(full_node::create_light_client_update(
        &state,
        &forged_block,
        &parent_state,
        &parent,
        None,
        &params
    )
    .is_err());
}
#[test]
fn normalized_complete_data_encodings_roundtrip_all_later_schemas() {
    let params = params();
    let keys = keys(18);
    let bootstrap = bootstrap(committee(&keys), &params);
    let update = optimistic(3, &keys, 32, &params);
    for schema in [
        Fork::Altair,
        Fork::Bellatrix,
        Fork::Capella,
        Fork::Deneb,
        Fork::Electra,
        Fork::Fulu,
        Fork::Gloas,
    ] {
        let bootstrap = upgrade_bootstrap(&bootstrap, schema, &params).unwrap();
        let bytes = bootstrap.to_ssz(params.preset()).unwrap();
        let decoded = Bootstrap::from_ssz(&bytes, schema, params.preset()).unwrap();
        assert_eq!(bootstrap, decoded);
        assert_eq!(
            bootstrap.hash_tree_root(params.preset()).unwrap(),
            decoded.hash_tree_root(params.preset()).unwrap()
        );
        let update = upgrade_update(&update, schema, &params).unwrap();
        let bytes = update.to_ssz(params.preset()).unwrap();
        assert_eq!(update, Update::from_ssz(&bytes, schema, params.preset()).unwrap());
        let finality = full_node::create_light_client_finality_update(&update);
        assert_eq!(
            finality,
            FinalityUpdate::from_ssz(
                &finality.to_ssz(params.preset()).unwrap(),
                schema,
                params.preset()
            )
            .unwrap()
        );
        let optimistic = full_node::create_light_client_optimistic_update(&update);
        assert_eq!(
            optimistic,
            OptimisticUpdate::from_ssz(
                &optimistic.to_ssz(params.preset()).unwrap(),
                schema,
                params.preset()
            )
            .unwrap()
        );
    }
}

#[test]
fn structural_defaults_preserve_all_declared_body_offsets_and_bounds() {
    use crate::ssz::Ssz;
    macro_rules! check {
        ($module:ident) => {{
            let value =
                <crate::ssz::$module::BeaconBlockBody as Ssz>::default_value(Preset::Minimal)
                    .unwrap();
            let bytes = value.to_ssz(Preset::Minimal).unwrap();
            let parsed =
                crate::ssz::$module::BeaconBlockBody::from_ssz(&bytes, Preset::Minimal).unwrap();
            assert_eq!(value, parsed);
            assert_eq!(
                value.hash_tree_root(Preset::Minimal).unwrap(),
                parsed.hash_tree_root(Preset::Minimal).unwrap()
            );
        }};
    }
    check!(phase0);
    check!(altair);
    check!(bellatrix);
    check!(capella);
    check!(deneb);
    check!(electra);
    check!(fulu);
    check!(gloas);
}
