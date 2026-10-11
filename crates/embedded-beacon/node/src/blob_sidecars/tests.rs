//! Real proposer signatures, KZG proofs and bounded atomic duplicate tracking.

use super::*;
use crate::gossip_tests::store_and_keys;
use blst::min_pk::SecretKey;
use c_kzg::{ethereum_kzg_settings, Blob, Bytes48, BYTES_PER_BLOB};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::Store;
use reth_embedded_beacon_state_transition::{
    light_client::merkle::compute_merkle_proof,
    ssz::{deneb as deneb_schema, electra as electra_schema, Fixed, List, Preset, Ssz, Vector},
};

#[test]
fn actual_ssz_list_and_body_proof_binds_commitment_index_length_and_body() {
    let preset = Preset::Minimal;
    let commitments = List::<[u8; 48], Fixed<4096>>::from_vec(vec![[7; 48], [8; 48]]);
    let mut body_fields = Vector::<Root, Fixed<16>>::from_vec(vec![[9; 32]; 16]);
    body_fields.values[11] = commitments.hash_tree_root(preset).unwrap();
    let mut branch = compute_merkle_proof(&commitments, 8193, preset).unwrap();
    branch.extend(compute_merkle_proof(&body_fields, 27, preset).unwrap());
    assert_eq!(branch.len(), 17);
    let blob = deneb::Blob::from_vec(Vec::new());
    let mut header = SignedBeaconBlockHeader::default_value(preset).unwrap();
    header.message.body_root = body_fields.hash_tree_root(preset).unwrap();
    let mut parts = BlobParts {
        index: 1,
        blob: &blob,
        commitment: &commitments.values[1],
        proof: &[0; 48],
        signed_header: &header,
        inclusion: &branch,
    };
    verify_inclusion(&parts).unwrap();
    parts.index = 0;
    assert!(matches!(verify_inclusion(&parts), Err(BlobGossipError::Reject(_))));
    parts.index = 1;
    parts.commitment = &commitments.values[0];
    assert!(matches!(verify_inclusion(&parts), Err(BlobGossipError::Reject(_))));
    parts.commitment = &commitments.values[1];
    let mut changed_length = branch.clone();
    changed_length[12][0] = 3;
    parts.inclusion = &changed_length;
    assert!(matches!(verify_inclusion(&parts), Err(BlobGossipError::Reject(_))));
    parts.inclusion = &branch[..16];
    assert!(matches!(verify_inclusion(&parts), Err(BlobGossipError::Reject(_))));
    parts.inclusion = &branch;
    let mut wrong_body = header.clone();
    wrong_body.message.body_root[0] ^= 1;
    parts.signed_header = &wrong_body;
    assert!(matches!(verify_inclusion(&parts), Err(BlobGossipError::Reject(_))));
    parts.signed_header = &header;
    verify_inclusion(&parts).unwrap();
}

#[test]
fn real_deneb_and_electra_sidecars_accept_without_mutating_the_consensus_graph() {
    for fork in [Fork::Deneb, Fork::Electra] {
        let (store, keys, parameters) = store_and_keys(fork);
        let head = store.get_head().unwrap();
        let original = store.block_state(head.root).unwrap().to_ssz().unwrap();
        let sidecars = signed_sidecars(&store, &keys, &parameters, 1, 2);
        let time = store.genesis_time_ms() + store.slot_duration_ms();
        let mut seen = SeenBlobSidecars::new(8, 8).unwrap();
        validate(fork, &mut seen, &store, &sidecars[0], time).unwrap();
        validate(fork, &mut seen, &store, &sidecars[1], time).unwrap();
        assert_eq!(seen.seen.len(), 2);
        assert_eq!(seen.proposers.len(), 1);
        assert!(matches!(
            validate(fork, &mut seen, &store, &sidecars[0], time),
            Err(BlobGossipError::Ignore(
                "already seen blob sidecar from this proposer for this slot and index"
            ))
        ));
        assert_eq!(store.get_head().unwrap(), head);
        assert_eq!(store.block_state(head.root).unwrap().to_ssz().unwrap(), original);
        assert!(store.block(sidecars[0].signed_block_header.message.hash_tree_root()).is_none());
    }
}

#[test]
fn invalid_signature_inclusion_and_kzg_proofs_leave_all_caches_unbound() {
    let (store, keys, parameters) = store_and_keys(Fork::Deneb);
    let sidecar = signed_sidecars(&store, &keys, &parameters, 1, 1).remove(0);
    let time = store.genesis_time_ms() + store.slot_duration_ms();
    let mut seen = SeenBlobSidecars::new(4, 8).unwrap();
    let mut invalid = sidecar.clone();
    invalid.signed_block_header.signature = [0; 96];
    assert!(matches!(
        validate(Fork::Deneb, &mut seen, &store, &invalid, time),
        Err(BlobGossipError::Reject("invalid proposer signature on blob sidecar block header"))
    ));
    assert_unbound(&seen);
    invalid = sidecar.clone();
    invalid.kzg_commitment_inclusion_proof.values[0][0] ^= 1;
    assert!(matches!(
        validate(Fork::Deneb, &mut seen, &store, &invalid, time),
        Err(BlobGossipError::Reject("invalid blob sidecar inclusion proof"))
    ));
    assert_unbound(&seen);
    invalid = sidecar.clone();
    invalid.blob.values[31] = 2;
    assert!(matches!(
        validate(Fork::Deneb, &mut seen, &store, &invalid, time),
        Err(BlobGossipError::Reject("invalid blob kzg proof"))
    ));
    assert_unbound(&seen);
    validate(Fork::Deneb, &mut seen, &store, &sidecar, time).unwrap();
    assert_eq!(seen.seen.len(), 1);
}

#[test]
fn a_real_signature_from_the_wrong_selected_proposer_is_rejected_after_kzg_validation() {
    let (store, keys, parameters) = store_and_keys(Fork::Electra);
    let mut sidecar = signed_sidecars(&store, &keys, &parameters, 1, 1).remove(0);
    sidecar.signed_block_header.message.proposer_index =
        (sidecar.signed_block_header.message.proposer_index + 1) % keys.len() as u64;
    resign_header(&store, &keys, &mut sidecar.signed_block_header);
    let time = store.genesis_time_ms() + store.slot_duration_ms();
    let mut seen = SeenBlobSidecars::new(4, 8).unwrap();
    assert!(matches!(
        validate(Fork::Electra, &mut seen, &store, &sidecar, time),
        Err(BlobGossipError::Reject(
            "blob sidecar proposer_index does not match expected proposer"
        ))
    ));
    assert_unbound(&seen);
}

#[test]
fn capacity_failure_is_atomic_and_pruning_keeps_the_trusted_context_binding() {
    let (deneb_store, keys, parameters) = store_and_keys(Fork::Deneb);
    let sidecars = signed_sidecars(&deneb_store, &keys, &parameters, 1, 2);
    let time = deneb_store.genesis_time_ms() + deneb_store.slot_duration_ms();
    let mut seen = SeenBlobSidecars::new(1, 8).unwrap();
    validate(Fork::Deneb, &mut seen, &deneb_store, &sidecars[0], time).unwrap();
    assert!(matches!(
        validate(Fork::Deneb, &mut seen, &deneb_store, &sidecars[1], time),
        Err(BlobGossipError::ResourceLimit)
    ));
    assert_eq!(seen.seen.len(), 1);
    assert_eq!(seen.proposers.len(), 1);
    seen.retain_from_slot(2);
    assert!(seen.seen.is_empty());
    assert!(seen.proposers.is_empty());
    assert!(seen.context.is_some());
    let (electra_store, keys, parameters) = store_and_keys(Fork::Electra);
    let electra = signed_sidecars(&electra_store, &keys, &parameters, 1, 1).remove(0);
    assert!(matches!(
        validate(Fork::Electra, &mut seen, &electra_store, &electra, time),
        Err(BlobGossipError::Context("blob cache belongs to another chain or policy"))
    ));
    assert!(seen.seen.is_empty());
    assert!(seen.proposers.is_empty());
    assert!(SeenBlobSidecars::new(0, 8).is_err());
    assert!(SeenBlobSidecars::new(1_048_577, 8).is_err());
}

#[test]
fn future_tolerance_and_local_replay_limit_never_mark_valid_sidecars_seen_early() {
    let (store, keys, parameters) = store_and_keys(Fork::Deneb);
    let sidecar = signed_sidecars(&store, &keys, &parameters, 1, 1).remove(0);
    let time = store.genesis_time_ms() + store.slot_duration_ms();
    let mut seen = SeenBlobSidecars::new(4, 8).unwrap();
    assert!(matches!(
        validate(Fork::Deneb, &mut seen, &store, &sidecar, time - 501),
        Err(BlobGossipError::Ignore("blob sidecar is from a future slot"))
    ));
    assert_unbound(&seen);
    validate(Fork::Deneb, &mut seen, &store, &sidecar, time - 500).unwrap();
    let later = signed_sidecars(&store, &keys, &parameters, 2, 1).remove(0);
    let mut bounded = SeenBlobSidecars::new(4, 1).unwrap();
    assert!(matches!(
        validate(Fork::Deneb, &mut bounded, &store, &later, time * 2),
        Err(BlobGossipError::Ignore("proposer shuffling unavailable"))
    ));
    assert_unbound(&bounded);
    validate(Fork::Deneb, &mut bounded, &store, &sidecar, time).unwrap();
}

fn assert_unbound(seen: &SeenBlobSidecars) {
    assert!(seen.seen.is_empty());
    assert!(seen.proposers.is_empty());
    assert!(seen.context.is_none());
}

fn validate(
    fork: Fork,
    seen: &mut SeenBlobSidecars,
    store: &Store,
    sidecar: &deneb::BlobSidecar,
    time: u64,
) -> Result<(), BlobGossipError> {
    if fork == Fork::Deneb {
        seen.validate_deneb(store, sidecar, time, sidecar.index % 6, ethereum_kzg_settings(0))
    } else {
        let sidecar = electra::BlobSidecar {
            index: sidecar.index,
            blob: sidecar.blob.clone(),
            kzg_commitment: sidecar.kzg_commitment,
            kzg_proof: sidecar.kzg_proof,
            signed_block_header: sidecar.signed_block_header.clone(),
            kzg_commitment_inclusion_proof: sidecar.kzg_commitment_inclusion_proof.clone(),
        };
        seen.validate_electra(store, &sidecar, time, sidecar.index % 9, ethereum_kzg_settings(0))
    }
}

fn signed_sidecars(
    store: &Store,
    keys: &[SecretKey],
    parameters: &fork_processing::TransitionParameters,
    slot: u64,
    count: usize,
) -> Vec<deneb::BlobSidecar> {
    let preset = Preset::Minimal;
    let settings = ethereum_kzg_settings(0);
    let head = store.get_head().unwrap();
    let parent = store.block_state(head.root).unwrap();
    let replay = fork_processing::replay_slots(parent, slot, parameters, 8).unwrap();
    let proposer = match &replay {
        BeaconState::Deneb(state, _) => {
            deneb_processing::get_beacon_proposer_index(state, &parameters.gloas.fulu.electra.deneb)
                .unwrap()
        }
        BeaconState::Electra(state, _) => {
            electra_processing::get_beacon_proposer_index(state, &parameters.gloas.fulu.electra)
                .unwrap()
        }
        _ => panic!("real blob test requires Deneb or Electra"),
    };
    let native = (0..count)
        .map(|index| {
            let mut bytes = [0; BYTES_PER_BLOB];
            bytes[31] = (index + 1) as u8;
            Blob::new(bytes)
        })
        .collect::<Vec<_>>();
    let commitments = native
        .iter()
        .map(|blob| settings.blob_to_kzg_commitment(blob).unwrap().to_bytes().into_inner())
        .collect::<Vec<_>>();
    let proofs = native
        .iter()
        .zip(&commitments)
        .map(|(blob, commitment)| {
            settings
                .compute_blob_kzg_proof(blob, &Bytes48::new(*commitment))
                .unwrap()
                .to_bytes()
                .into_inner()
        })
        .collect::<Vec<_>>();
    let (body_root, inclusions) = if replay.schema_fork() == Fork::Deneb {
        let mut body = deneb_schema::BeaconBlockBody::default_value(preset).unwrap();
        body.blob_kzg_commitments = List::from_vec(commitments.clone());
        (
            body.hash_tree_root(preset).unwrap(),
            (0..count)
                .map(|index| {
                    compute_merkle_proof(&body, (27 << 13) | index as u64, preset).unwrap()
                })
                .collect::<Vec<_>>(),
        )
    } else {
        let mut body = electra_schema::BeaconBlockBody::default_value(preset).unwrap();
        body.blob_kzg_commitments = List::from_vec(commitments.clone());
        (
            body.hash_tree_root(preset).unwrap(),
            (0..count)
                .map(|index| {
                    compute_merkle_proof(&body, (27 << 13) | index as u64, preset).unwrap()
                })
                .collect::<Vec<_>>(),
        )
    };
    let mut header = SignedBeaconBlockHeader::default_value(preset).unwrap();
    header.message.slot = slot;
    header.message.proposer_index = proposer;
    header.message.parent_root = head.root;
    header.message.state_root = replay.hash_tree_root().unwrap();
    header.message.body_root = body_root;
    resign_header(store, keys, &mut header);
    native
        .iter()
        .enumerate()
        .map(|(index, blob)| deneb::BlobSidecar {
            index: index as u64,
            blob: deneb::Blob::from_vec(blob.as_ref().to_vec()),
            kzg_commitment: commitments[index],
            kzg_proof: proofs[index],
            signed_block_header: header.clone(),
            kzg_commitment_inclusion_proof: Vector::from_vec(inclusions[index].clone()),
        })
        .collect()
}

fn resign_header(store: &Store, keys: &[SecretKey], header: &mut SignedBeaconBlockHeader) {
    let head = store.get_head().unwrap();
    let state = store.block_state(head.root).unwrap();
    let epoch = header.message.slot / store.slots_per_epoch();
    let fork = state.fork_info();
    let version = if epoch < fork.epoch { fork.previous_version } else { fork.current_version };
    let root = compute_signing_root(
        header.message.hash_tree_root(),
        compute_domain([0; 4], version, state.genesis_validators_root()),
    );
    header.signature =
        keys[header.message.proposer_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
}
