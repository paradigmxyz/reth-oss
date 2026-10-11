use super::*;
use crate::gossip_tests::{
    fulu_gloas_boundary, gloas_store_and_keys, signing_root, store_and_keys,
};
use blst::min_pk::{AggregateSignature, SecretKey};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::{gloas::GloasStore, Store};
use reth_embedded_beacon_state_transition::ssz::Ssz;
use std::{
    future::Future,
    io,
    task::{Context, Poll, Waker},
};

#[cfg(feature = "spec-tests")]
use reth_embedded_beacon_fork_choice::gloas::spec_only::SpecStore;

fn signed_legacy(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> ssz::phase0::Attestation {
    let head = store.get_head().unwrap();
    let state = store.block_state(head.root).unwrap();
    let BeaconState::Phase0(state, preset) = state else { panic!("expected Phase0 factory") };
    let committee = phase0_processing::get_beacon_committee(
        state,
        0,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    let mut bytes = vec![0; committee.len() / 8 + 1];
    bytes[0] = 1;
    bytes[committee.len() / 8] |= 1 << (committee.len() % 8);
    let data = AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: head.root,
        source: store.checkpoints().0,
        target: Checkpoint { epoch: 0, root: head.root },
    };
    let root = signing_root(
        data.hash_tree_root(*preset).unwrap(),
        [1, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    ssz::phase0::Attestation {
        aggregation_bits: ssz::phase0::AggregationBits::from_ssz(&bytes, *preset).unwrap(),
        data,
        signature: keys[committee[0] as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    }
}

#[test]
fn real_legacy_signature_rejection_is_atomic_and_binding_precedes_duplicates_and_pruning() {
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let message = signed_legacy(&store, &keys, &parameters);
    let mut seen = SeenAttestations::new(8, 1024).unwrap();
    let original = seen.clone();
    let mut bad = message.clone();
    bad.signature[3] ^= 1;
    assert!(matches!(
        seen.validate_attestation(&store, LegacyAttestation::Phase0(&bad), 0, 0),
        Err(AttestationGossipError::Reject("invalid attestation signature"))
    ));
    assert_eq!(seen, original);
    seen.validate_attestation(&store, LegacyAttestation::Phase0(&message), 0, 0).unwrap();
    assert_eq!(seen.entries(), 1);
    assert!(matches!(
        seen.validate_attestation(&store, LegacyAttestation::Phase0(&message), 0, 0),
        Err(AttestationGossipError::Ignore(
            "already seen attestation for this epoch and validator"
        ))
    ));
    let head = store.get_head().unwrap();
    let mut limits = store.store_parameters();
    limits.max_blocks += 1;
    let mut different = Store::get_store(
        store.block_state(head.root).unwrap().clone(),
        store.block(head.root).unwrap().clone(),
        limits,
    )
    .unwrap();
    different.set_transition_parameters(parameters).unwrap();
    let accepted = seen.clone();
    assert!(matches!(
        seen.validate_attestation(&different, LegacyAttestation::Phase0(&message), 0, 0),
        Err(AttestationGossipError::Context(_))
    ));
    assert_eq!(seen, accepted);
    seen.retain_from_epoch(1);
    assert_eq!(seen.entries(), 0);
    assert!(matches!(
        seen.validate_attestation(&different, LegacyAttestation::Phase0(&message), 0, 0),
        Err(AttestationGossipError::Context(_))
    ));
}

#[test]
fn genuine_aggregate_checks_all_signatures_superset_cache_and_atomic_capacity() {
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let mut attestation = signed_legacy(&store, &keys, &parameters);
    let view = ClassicView(&store);
    let head = view.head().unwrap();
    let committee = head.committee(0, 0).unwrap();
    let fields = head.fields();
    let preset = head.preset();
    let mut bits = vec![0; committee.len() / 8 + 1];
    for index in 0..committee.len() {
        bits[index / 8] |= 1 << (index % 8);
    }
    bits[committee.len() / 8] |= 1 << (committee.len() % 8);
    attestation.aggregation_bits = ssz::phase0::AggregationBits::from_ssz(&bits, preset).unwrap();
    let root = compute_signing_root(
        attestation.data.hash_tree_root(preset).unwrap(),
        fields.domain([1, 0, 0, 0], 0),
    );
    let signatures = committee
        .iter()
        .map(|&index| keys[index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let references = signatures.iter().collect::<Vec<_>>();
    attestation.signature =
        AggregateSignature::aggregate(&references, true).unwrap().to_signature().to_bytes();
    let proof_root = compute_signing_root([0; 32], fields.domain([5, 0, 0, 0], 0));
    let proof = auxiliary::phase0::AggregateAndProof {
        aggregator_index: committee[0],
        aggregate: attestation,
        selection_proof: keys[committee[0] as usize]
            .sign(&proof_root, ETHEREUM_BLS_DST, &[])
            .to_bytes(),
    };
    let root =
        compute_signing_root(proof.hash_tree_root(preset).unwrap(), fields.domain([6, 0, 0, 0], 0));
    let message = auxiliary::phase0::SignedAggregateAndProof {
        message: proof,
        signature: keys[committee[0] as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    };
    let mut seen = SeenAttestations::new(8, 1024).unwrap();
    let original = seen.clone();
    let mut bad = message.clone();
    bad.signature[4] ^= 1;
    assert!(matches!(
        seen.validate_aggregate(&store, SignedAggregate::Phase0(&bad), 0),
        Err(AttestationGossipError::Reject("invalid aggregator signature"))
    ));
    assert_eq!(seen, original);
    let mut limited = SeenAttestations::new(1, 1024).unwrap();
    let empty = limited.clone();
    assert!(matches!(
        limited.validate_aggregate(&store, SignedAggregate::Phase0(&message), 0),
        Err(AttestationGossipError::Capacity)
    ));
    assert_eq!(limited, empty);
    seen.validate_aggregate(&store, SignedAggregate::Phase0(&message), 0).unwrap();
    assert_eq!(seen.entries(), 2);
    let mut subset = message.clone();
    let mut bits = vec![0; committee.len() / 8 + 1];
    bits[0] = 1;
    bits[committee.len() / 8] |= 1 << (committee.len() % 8);
    subset.message.aggregate.aggregation_bits =
        ssz::phase0::AggregationBits::from_ssz(&bits, preset).unwrap();
    assert!(matches!(
        seen.validate_aggregate(&store, SignedAggregate::Phase0(&subset), 0),
        Err(AttestationGossipError::Ignore("already seen aggregate for this data"))
    ));
    assert_eq!(seen.entries(), 2);
}

#[test]
fn electra_single_requires_real_signature_and_exact_assigned_member() {
    let (store, keys, parameters) = store_and_keys(Fork::Electra);
    let view = ClassicView(&store);
    let head = view.head().unwrap();
    let committee = head.committee(0, 0).unwrap();
    let fields = head.fields();
    let root = store.get_head().unwrap().root;
    let data = AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: root,
        source: store.checkpoints().0,
        target: Checkpoint { epoch: 0, root },
    };
    let signing = compute_signing_root(
        data.hash_tree_root(parameters.preset()).unwrap(),
        fields.domain([1, 0, 0, 0], 0),
    );
    let message = auxiliary::electra::SingleAttestation {
        committee_index: 0,
        attester_index: committee[0],
        data,
        signature: keys[committee[0] as usize].sign(&signing, ETHEREUM_BLS_DST, &[]).to_bytes(),
    };
    let mut seen = SeenAttestations::new(8, 1024).unwrap();
    let original = seen.clone();
    let mut bad = message.clone();
    bad.signature[3] ^= 1;
    assert!(matches!(
        seen.validate_single(&store, SingleAttestation::Electra(&bad), 0, 0),
        Err(AttestationGossipError::Reject("invalid attestation signature"))
    ));
    assert_eq!(seen, original);
    let mut outside = message.clone();
    outside.attester_index = 32;
    assert!(matches!(
        seen.validate_single(&store, SingleAttestation::Electra(&outside), 0, 0),
        Err(AttestationGossipError::Reject("attester is not a member of the committee"))
    ));
    assert_eq!(seen, original);
    seen.validate_single(&store, SingleAttestation::Electra(&message), 0, 0).unwrap();
    assert_eq!(seen.entries(), 1);
}

#[test]
fn clock_tolerances_are_inclusive_and_large_slots_do_not_overflow() {
    let (store, _, _) = store_and_keys(Fork::Phase0);
    let binding = ClassicView(&store).binding().unwrap();
    let end = 33 * binding.slot_ms + 500;
    checked_time(&binding, 0, end, Fork::Phase0, false).unwrap();
    assert!(matches!(
        checked_time(&binding, 0, end + 1, Fork::Phase0, false),
        Err(AttestationGossipError::Ignore("attestation slot not within propagation range"))
    ));
    checked_time(&binding, 1, binding.slot_ms - 500, Fork::Deneb, false).unwrap();
    assert!(matches!(
        checked_time(&binding, 1, binding.slot_ms - 501, Fork::Deneb, false),
        Err(AttestationGossipError::Ignore("attestation slot is from a future slot"))
    ));
    let end = 2 * binding.slots_per_epoch * binding.slot_ms + 500;
    checked_time(&binding, 0, end, Fork::Deneb, false).unwrap();
    assert!(matches!(
        checked_time(&binding, 0, end + 1, Fork::Deneb, true),
        Err(AttestationGossipError::Ignore("aggregate epoch is not current or previous epoch"))
    ));
    assert!(matches!(
        checked_time(&binding, u64::MAX, u64::MAX, Fork::Phase0, false),
        Err(AttestationGossipError::Ignore(_))
    ));
}

#[test]
fn gloas_single_never_creates_payload_authority_and_failed_vote_cannot_poison_seen() {
    let (store, keys, parameters) = gloas_store_and_keys();
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let committee = gloas_processing::get_beacon_committee(state, 0, 0, &parameters.gloas).unwrap();
    let fields = Fields {
        validators: &state.validators.values,
        fork: state.fork.clone(),
        network: state.genesis_validators_root,
    };
    let mut message = auxiliary::gloas::SingleAttestation {
        committee_index: 0,
        attester_index: committee[0],
        data: AttestationData {
            slot: 0,
            index: 1,
            beacon_block_root: root,
            source: store.checkpoints().0,
            target: Checkpoint { epoch: 0, root },
        },
        signature: [0; 96],
    };
    let sign = |message: &mut auxiliary::gloas::SingleAttestation| {
        let signing = compute_signing_root(
            message.data.hash_tree_root(parameters.preset()).unwrap(),
            fields.domain([1, 0, 0, 0], message.data.target.epoch),
        );
        message.signature =
            keys[message.attester_index as usize].sign(&signing, ETHEREUM_BLS_DST, &[]).to_bytes();
    };
    sign(&mut message);
    let mut seen = SeenAttestations::new(8, 1024).unwrap();
    let empty = seen.clone();
    assert!(matches!(
        seen.validate_gloas_single(&store, &message, 0, 0),
        Err(AttestationGossipError::Reject("same-slot attestation must attest with index 0"))
    ));
    assert_eq!(seen, empty);
    message.data.slot = 1;
    message.attester_index =
        gloas_processing::get_beacon_committee(state, 1, 0, &parameters.gloas).unwrap()[0];
    sign(&mut message);
    assert!(matches!(
        seen.validate_gloas_single(&store, &message, store.slot_duration_ms(), 1),
        Err(AttestationGossipError::Ignore("execution payload envelope has not been seen"))
    ));
    assert_eq!(seen, empty);
    assert!(!store.is_payload_verified(root));
    message.data.index = 0;
    sign(&mut message);
    seen.validate_gloas_single(&store, &message, store.slot_duration_ms(), 1).unwrap();
    assert_eq!(seen.entries(), 1);
    assert!(!store.is_payload_verified(root));
}

#[cfg(feature = "spec-tests")]
#[test]
fn published_gloas_invalid_payload_rejects_after_signed_envelope_without_mutating_cache_or_graph() {
    // These unchanged published members retain their SHA-256 provenance in testdata. Signed setup
    // uses strict transitions; the asserted payload/status observation remains inside SpecStore.
    let decode = |compressed: &[u8], expected: &str| {
        assert_eq!(
            sha256(compressed),
            reth_embedded_beacon_primitives::root_from_hex(expected).unwrap()
        );
        snap::raw::Decoder::new().decompress_vec(compressed).unwrap()
    };
    let preset = Preset::Minimal;
    let state = ssz::gloas::BeaconState::from_ssz(
        &decode(
            include_bytes!("../../testdata/gloas-invalid-payload-attestation/state.ssz_snappy"),
            "fb185ff77bfcaf4430b88f021c29804940cb889a36060d7dcf5ecb0cee9232a7",
        ),
        preset,
    )
    .unwrap();
    let anchor = ssz::gloas::SignedBeaconBlock::from_ssz(
        &decode(
            include_bytes!("../../testdata/gloas-invalid-payload-attestation/block_0x9f87a12d020fe5f111a8e388f428594715e83c39314fecf4b523dd7da27848b5.ssz_snappy"),
            "09a16c11417a3ee1046100b432a475d256e1722aa17dee75ee1492346c49bbbf",
        ),
        preset,
    )
    .unwrap();
    let block = ssz::gloas::SignedBeaconBlock::from_ssz(
        &decode(
            include_bytes!("../../testdata/gloas-invalid-payload-attestation/block_0x2bdd11129831a9f7045c6cba57db119b16c42192f8a0abd3f248bea2f21d15b2.ssz_snappy"),
            "b80dc7bb2ab1f4b6238767c4711dd9c80e14dc11365da79522d7db1cd69ca825",
        ),
        preset,
    )
    .unwrap();
    let envelope = ssz::gloas::SignedExecutionPayloadEnvelope::from_ssz(
        &decode(
            include_bytes!("../../testdata/gloas-invalid-payload-attestation/execution_payload_envelope_0x131ab93ae0a85b4dae8dd199874c24c136c7c8645f3a7b21bb0135cff179e47a.ssz_snappy"),
            "32ef51ae5a44440696c57bff07e234eac85ca3ebe29c7b0fbbdeccbed135b42d",
        ),
        preset,
    )
    .unwrap();
    let message = auxiliary::gloas::SingleAttestation::from_ssz(
        &decode(
            include_bytes!("../../testdata/gloas-invalid-payload-attestation/single_attestation_0x1d652b262f932b0ec56fb699239c3824b4ab04597e2abfab4e840b55687bae22.ssz_snappy"),
            "b8d3909fdba31423e2bb754061bad4ad98ac07d85aee0c49b3b888118e0d043b",
        ),
        preset,
    )
    .unwrap();
    assert_eq!(state.fork.epoch, 0);
    assert_eq!(anchor.message.state_root, state.hash_tree_root(preset).unwrap());
    let defaults = TransitionParameters::for_preset(preset);
    let schedule = defaults
        .schedule
        .into_iter()
        .map(|mut activation| {
            activation.epoch = 0;
            activation
        })
        .collect();
    let parameters = TransitionParameters::new(defaults.gloas, schedule).unwrap();
    let mut store = SpecStore::get_store(
        state,
        anchor.message,
        StoreParameters::for_preset(preset),
        parameters.gloas,
    )
    .unwrap();
    let block_root = block.message.hash_tree_root(preset).unwrap();
    store.on_tick(store.genesis_time_ms() + block.message.slot * store.slot_duration_ms()).unwrap();
    store.on_block_and_contents(&block).unwrap();
    assert_eq!(message.data.beacon_block_root, block_root);
    assert_eq!(envelope.message.beacon_block_root, block_root);
    assert_eq!(message.data.index, 1);
    let head = store.get_head().unwrap();
    let retained =
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &store, block_root,
        )
        .unwrap()
        .to_ssz(preset)
        .unwrap();
    let checkpoints = store.finalized_checkpoint();
    let mut seen = SeenAttestations::new(8, 1024).unwrap();
    let empty = seen.clone();
    store.fixture_set_payload_status(block_root, ExecutionStatus::Invalid).unwrap();
    assert!(matches!(
        seen.validate_gloas_single(&store, &message, 12_500, 4),
        Err(AttestationGossipError::Ignore("execution payload envelope has not been seen"))
    ));
    assert_eq!(seen, empty);

    // The original envelope must pass mandatory signature and exact block/state linkage checks.
    // This explicit fixture observation supplies no Engine, availability or runtime FULL token.
    store.fixture_assert_verified_payload_observation(&envelope).unwrap();
    let mut valid_control = store.clone();
    valid_control.fixture_set_payload_status(block_root, ExecutionStatus::Valid).unwrap();
    let mut control_seen = seen.clone();
    control_seen.validate_gloas_single(&valid_control, &message, 12_500, 4).unwrap();
    assert_eq!(control_seen.entries(), 1);
    assert!(matches!(
        seen.validate_gloas_single(&store, &message, 12_500, 4),
        Err(AttestationGossipError::Reject("attested payload is invalid"))
    ));
    assert_eq!(seen, empty);
    assert_eq!(store.get_head().unwrap(), head);
    assert_eq!(store.finalized_checkpoint(), checkpoints);
    assert_eq!(
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &store, block_root
        )
        .unwrap()
        .to_ssz(preset)
        .unwrap(),
        retained
    );
    assert_eq!(valid_control.get_head().unwrap(), head);
    assert_eq!(
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &valid_control,
            block_root
        )
        .unwrap()
        .to_ssz(preset)
        .unwrap(),
        retained
    );
}

struct MigrationWithoutExecution;
macro_rules! unavailable_migration_engine {
    ($module:ident, $schema:ident) => {
        impl $module::ExecutionEngine for MigrationWithoutExecution {
            type Error = io::Error;
            async fn verify_terminal_block(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(io::Error::other("gossip control supplies no terminal proof"))
            }
            async fn verify_payload(
                &self,
                _: &ssz::$schema::ExecutionPayload,
                _: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(io::Error::other("gossip control supplies no execution verdict"))
            }
        }
    };
}
unavailable_migration_engine!(bellatrix_processing, bellatrix);
unavailable_migration_engine!(capella_processing, capella);
unavailable_migration_engine!(deneb_processing, deneb);
unavailable_migration_engine!(electra_processing, electra);
unavailable_migration_engine!(fulu_processing, fulu);
impl gloas_processing::ExecutionEngine for MigrationWithoutExecution {
    type Error = io::Error;
    async fn verify_payload(
        &self,
        _: &ssz::gloas::ExecutionPayload,
        _: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(io::Error::other("gossip control supplies no envelope verdict"))
    }
}
fn ready_migration<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("signed beacon-only migration must not invent external validation"),
    }
}

#[test]
fn original_previous_fulu_wire_keeps_schema_domain_cache_and_unbranched_ancestry() {
    let (mut previous, keys, parameters, signed) = fulu_gloas_boundary();
    let root = previous.get_head().unwrap().root;
    let BeaconState::Fulu(state, preset) = previous.block_state(root).unwrap() else {
        unreachable!()
    };
    let committee =
        fulu_processing::get_beacon_committee(state, 0, 0, &parameters.gloas.fulu).unwrap();
    let participant = committee[0];
    let data = AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: root,
        source: previous.checkpoints().0,
        target: Checkpoint { epoch: 0, root },
    };
    let signature = keys[participant as usize]
        .sign(
            &signing_root(
                data.hash_tree_root(*preset).unwrap(),
                [1, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let message = auxiliary::fulu::SingleAttestation {
        committee_index: 0,
        attester_index: participant,
        data,
        signature,
    };
    let original = message.to_ssz(*preset).unwrap();
    let message = auxiliary::fulu::SingleAttestation::from_ssz(&original, *preset).unwrap();
    let mut seen = SeenAttestations::new(16, 4096).unwrap();
    seen.validate_single(&previous, SingleAttestation::Fulu(&message), 0, 0).unwrap();
    let prior_cache = seen.clone();
    let now = parameters.slots_per_epoch() * previous.slot_duration_ms();
    previous.on_tick(now).unwrap();
    let migrated = ready_migration(GloasStore::migrate_from_classic(
        &previous,
        &signed,
        &MigrationWithoutExecution,
    ))
    .unwrap();
    assert_eq!(migrated.node_for_root(root).unwrap().payload_status, None);
    assert_eq!(migrated.historical_block(root).unwrap().schema_fork(), Fork::Fulu);
    assert_eq!(
        migrated.read_historical_block_state(root).unwrap().unwrap().schema_fork(),
        Fork::Fulu
    );
    assert_eq!(
        migrated.read_historical_block_state(root).unwrap().unwrap().to_ssz().unwrap(),
        previous.block_state(root).unwrap().to_ssz().unwrap()
    );
    assert!(matches!(
        seen.validate_historical_single(&migrated, SingleAttestation::Fulu(&message), now, 0),
        Err(AttestationGossipError::Ignore(
            "already seen attestation for this epoch and validator"
        ))
    ));
    assert_eq!(seen, prior_cache);
    let mut fresh = SeenAttestations::new(16, 4096).unwrap();
    let mut forged = message.clone();
    forged.signature[9] ^= 1;
    assert!(matches!(
        fresh.validate_historical_single(&migrated, SingleAttestation::Fulu(&forged), now, 0),
        Err(AttestationGossipError::Reject("invalid attestation signature"))
    ));
    assert_eq!(fresh.entries(), 0);
    fresh.validate_historical_single(&migrated, SingleAttestation::Fulu(&message), now, 0).unwrap();
    assert_eq!(fresh.entries(), 1);
    assert_eq!(message.to_ssz(Preset::Minimal).unwrap(), original);
}
