//! Genuine signed CL boundary/EMPTY/FULL controls using controlled typed Engine replies.
//! The callbacks assert exact local contracts; passing is not live execution or `PoW` proof.

use super::*;
use crate::{
    compact_persistence::{
        state_source::RepositoryStateSource, AdmittedStateRepository, PersistenceNetwork,
    },
    execution_bootstrap::{
        tests::{adapter, anchor, genuine_gloas, limits, BackendHandle, Reply},
        ExecutionBootstrap, ReconciledCheckpoint,
    },
    execution_checkpoint_boundary::{ExecutionBoundaryError, ExecutionCheckpointBoundary},
    gossip_tests::{signing_root, store_and_keys},
    import_coordinator::ImportAnchor,
    runtime_engine::RuntimeEngineAdapter,
    runtime_parameters::RuntimeParameters,
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::{
    compact::{CompactGloasStore, CompactGraphLimits, StateSourceError},
    gloas::{gossip_view::GloasGossipView, DataAvailability, GloasStore},
    ForkChoiceNode, PayloadStatus, StoreError,
};
use reth_embedded_beacon_primitives::{uint64_root, Fork, Root};
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind, Owner, SharedDataStore};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    gloas_processing,
    ssz::{gloas, BeaconBlock, BeaconState, List, Preset, ProgressiveList, Ssz, Vector},
};
use reth_embedded_beacon_storage::CompactStateLimits;
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::TempDir;

struct Setup {
    config: ChainConfig,
    anchor: ImportAnchor,
    now: u64,
    store: GloasStore,
    parameters: TransitionParameters,
    keys: Vec<SecretKey>,
}
async fn authenticated_setup() -> Setup {
    tokio::task::spawn_blocking(|| {
        let (state, signed, parameters) =
            tokio::runtime::Handle::current().block_on(genuine_gloas());
        let (config, anchor, now) = anchor(&state, &signed, &parameters);
        let runtime = RuntimeParameters::from_config(&config).unwrap();
        let BeaconState::Gloas(state, _) = state else { panic!("genuine Gloas post-state") };
        let BeaconBlock::Gloas(block, _) = signed.message() else { panic!("genuine Gloas block") };
        let store =
            GloasStore::get_store(state, block, runtime.fork_choice, runtime.transitions.gloas)
                .unwrap();
        let (_, keys, _) = store_and_keys(Fork::Fulu);
        Setup { config, anchor, now, store, parameters, keys }
    })
    .await
    .unwrap()
}
async fn receipt(
    setup: &Setup,
    engine: &RuntimeEngineAdapter<BackendHandle>,
) -> ReconciledCheckpoint {
    let config = setup.config.clone();
    let anchor = setup.anchor.clone();
    let now = setup.now;
    let (mut owner, _) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(config, &anchor, now, limits()).unwrap()
    })
    .await
    .unwrap();
    owner.reconcile(engine).await.unwrap()
}
async fn ready() -> (Setup, ExecutionCheckpointBoundary, Arc<RuntimeEngineAdapter<BackendHandle>>) {
    let setup = authenticated_setup().await;
    let (engine, _) = adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
    let engine = Arc::new(engine);
    let receipt = receipt(&setup, &engine).await;
    let captured = engine.clone();
    let (setup, boundary) = tokio::task::spawn_blocking(move || {
        let boundary = ExecutionCheckpointBoundary::authenticate(
            receipt,
            &setup.config,
            &setup.anchor,
            setup.now,
            &setup.store,
            &captured,
        )
        .unwrap();
        (setup, boundary)
    })
    .await
    .unwrap();
    (setup, boundary, engine)
}
fn node(root: Root, status: PayloadStatus) -> ForkChoiceNode {
    ForkChoiceNode { root, payload_status: Some(status) }
}
fn requests() -> gloas::ExecutionRequests {
    gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    }
}
fn child(setup: &Setup, include_parent: bool, hash: u8) -> gloas::SignedBeaconBlock {
    let head = setup.store.get_head().unwrap();
    let target = setup.store.block_state(head.root).unwrap().slot + 1;
    child_at_slot(setup, include_parent, hash, target)
}
fn child_at_slot(
    setup: &Setup,
    include_parent: bool,
    hash: u8,
    target: u64,
) -> gloas::SignedBeaconBlock {
    let head = setup.store.get_head().unwrap();
    let mut state = setup.store.block_state(head.root).unwrap().clone();
    let parameters = &setup.parameters.gloas;
    gloas_processing::process_slots(&mut state, target, parameters).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, parameters).unwrap();
    let key = &setup.keys[proposer as usize];
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: if include_parent {
            state.latest_execution_payload_bid.block_hash
        } else {
            state.latest_block_hash
        },
        parent_block_root: head.root,
        block_hash: [hash; 32],
        prev_randao: state.randao_mixes.values[(target / 8 % 64) as usize],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot: target,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests().hash_tree_root(Preset::Minimal).unwrap(),
    };
    body.randao_reveal = key
        .sign(
            &signing_root(
                uint64_root(target / 8),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = gloas::BeaconBlock {
        slot: target,
        proposer_index: proposer,
        parent_root: head.root,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    gloas_processing::process_parent_execution_payload(&mut state, &message, parameters).unwrap();
    gloas_processing::process_block_header(&mut state, &message, parameters).unwrap();
    gloas_processing::process_withdrawals(&mut state, parameters).unwrap();
    gloas_processing::process_execution_payload_bid(
        &mut state,
        &message.body.signed_execution_payload_bid,
        parameters,
    )
    .unwrap();
    gloas_processing::process_randao(&mut state, &message.body, parameters).unwrap();
    gloas_processing::process_eth1_data(&mut state, &message.body, parameters).unwrap();
    gloas_processing::process_operations(&mut state, &message.body, parent_slot, parameters)
        .unwrap();
    gloas_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, parameters)
        .unwrap();
    message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    gloas::SignedBeaconBlock { message, signature }
}
fn import(setup: &mut Setup, include_parent: bool, hash: u8) -> Root {
    let signed = child(setup, include_parent, hash);
    setup
        .store
        .on_tick(
            setup.store.genesis_time_ms() + signed.message.slot * setup.store.slot_duration_ms(),
        )
        .unwrap();
    setup.store.on_block(&signed).unwrap();
    if signed.message.slot < 24 {
        assert_eq!(setup.store.proposer_boost_root(), [0; 32]);
    }
    signed.message.hash_tree_root(Preset::Minimal).unwrap()
}
fn envelope(setup: &Setup, root: Root) -> gloas::SignedExecutionPayloadEnvelope {
    let state = setup.store.block_state(root).unwrap();
    let bid = &state.latest_execution_payload_bid;
    let message = gloas::ExecutionPayloadEnvelope {
        payload: gloas::ExecutionPayload {
            parent_hash: state.latest_block_hash,
            fee_recipient: bid.fee_recipient,
            state_root: [1; 32],
            receipts_root: [2; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: bid.prev_randao,
            block_number: 2,
            gas_limit: bid.gas_limit,
            gas_used: 0,
            timestamp: state.genesis_time + state.slot * 6,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: bid.block_hash,
            transactions: ProgressiveList::default(),
            withdrawals: state.payload_expected_withdrawals.clone(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
            block_access_list: ProgressiveList::default(),
            slot_number: state.slot,
        },
        execution_requests: requests(),
        builder_index: bid.builder_index,
        beacon_block_root: root,
        parent_beacon_block_root: state.latest_block_header.parent_root,
    };
    let signature = setup.keys[state.latest_block_header.proposer_index as usize]
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [11, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    gloas::SignedExecutionPayloadEnvelope { message, signature }
}
struct EmptyCustody {
    root: Root,
    calls: AtomicUsize,
}
impl DataAvailability for EmptyCustody {
    type Error = io::Error;
    async fn verify(&self, root: Root, block: &gloas::BeaconBlock) -> Result<(), io::Error> {
        assert_eq!(root, self.root);
        assert_eq!(root, block.hash_tree_root(Preset::Minimal).unwrap());
        assert!(block
            .body
            .signed_execution_payload_bid
            .message
            .blob_kzg_commitments
            .values
            .is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct ExactEnvelope {
    payload: gloas::ExecutionPayload,
    parent_root: Root,
    calls: AtomicUsize,
    accept: bool,
}
impl gloas_processing::ExecutionEngine for ExactEnvelope {
    type Error = io::Error;
    async fn verify_payload(
        &self,
        payload: &gloas::ExecutionPayload,
        context: gloas_processing::ExecutionContext,
    ) -> Result<(), io::Error> {
        assert_eq!(payload, &self.payload);
        assert_eq!(context.slot, payload.slot_number);
        assert_eq!(context.parent_beacon_block_root, self.parent_root);
        assert_eq!(context.previous_execution_payload_hash, payload.parent_hash);
        assert!(context.expected_blob_versioned_hashes.is_empty());
        assert!(context.execution_requests_list.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.accept {
            Ok(())
        } else {
            Err(io::Error::other("exact controlled envelope rejection"))
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_valid_base_authenticates_only_anchor_empty_safe_and_finalized_parent() {
    let (setup, boundary, engine) = ready().await;
    tokio::task::spawn_blocking(move || {
        let root = setup.anchor.verified.checkpoint().root;
        assert!(matches!(
            setup.store.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
            Err(StoreError::UnknownRoot)
        ));
        boundary.check_adapter(&setup.config, &engine).unwrap();
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
        assert_eq!(boundary.finalized_execution_block_hash(&setup.store).unwrap(), Some([41; 32]));
        assert_eq!(
            GloasGossipView::execution_status(&setup.store, root),
            reth_embedded_beacon_fork_choice::ExecutionStatus::Optimistic
        );
        assert!(GloasGossipView::payload(&setup.store, root).is_none());
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_missing_full_unknown_root_and_bad_status_never_get_boundary_credit() {
    let (setup, boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        let root = setup.anchor.verified.checkpoint().root;
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Pending))
                .unwrap(),
            None
        );
        assert!(matches!(
            boundary.verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Full)),
            Err(ExecutionBoundaryError::Store(StoreError::ExecutionImport(_)))
        ));
        assert!(matches!(
            boundary
                .verified_execution_block_hash(&setup.store, node([99; 32], PayloadStatus::Empty)),
            Err(ExecutionBoundaryError::Store(StoreError::UnknownRoot))
        ));
        assert!(matches!(
            boundary.verified_execution_block_hash(
                &setup.store,
                ForkChoiceNode { root, payload_status: None }
            ),
            Err(ExecutionBoundaryError::Store(StoreError::InvalidAttestationTarget))
        ));
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_signed_empty_gaps_carry_only_the_exact_reconciled_base() {
    let (mut setup, boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        for hash in [45, 46, 47] {
            let root = import(&mut setup, false, hash);
            assert_eq!(
                boundary
                    .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty))
                    .unwrap(),
                Some([41; 32])
            );
            assert!(boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Full))
                .is_err());
        }
        assert_eq!(boundary.finalized_execution_block_hash(&setup.store).unwrap(), Some([41; 32]));
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_adapter_original_bytes_and_closed_proxy_reject_sealed_receipt_transfer() {
    let setup = authenticated_setup().await;
    let (engine, _) = adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
    let proof = receipt(&setup, &engine).await;
    tokio::task::spawn_blocking(move || {
        let (different, _) = adapter(setup.config.clone(), vec![], Duration::from_secs(1));
        assert!(ExecutionCheckpointBoundary::authenticate(
            proof,
            &setup.config,
            &setup.anchor,
            setup.now,
            &setup.store,
            &different
        )
        .is_err());
    })
    .await
    .unwrap();
    let (setup, boundary, engine) = ready().await;
    tokio::task::spawn_blocking(move || {
        let mut other = setup.config.clone();
        other.chain_id += 1;
        assert!(matches!(
            boundary.check_adapter(&other, &engine),
            Err(ExecutionBoundaryError::Binding)
        ));
        engine.close_bootstrap_admission();
        assert!(matches!(
            boundary.check_adapter(&setup.config, &engine),
            Err(ExecutionBoundaryError::ReconciliationRequired)
        ));
    })
    .await
    .unwrap();
    let setup = authenticated_setup().await;
    let (engine, _) = adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
    let proof = receipt(&setup, &engine).await;
    tokio::task::spawn_blocking(move || {
        let mut altered = setup.anchor.clone();
        let mut bytes = altered.block_ssz.to_vec();
        bytes[0] ^= 1;
        altered.block_ssz = bytes.into();
        assert!(ExecutionCheckpointBoundary::authenticate(
            proof,
            &setup.config,
            &altered,
            setup.now,
            &setup.store,
            &engine
        )
        .is_err());
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_anchor_metadata_and_carried_hash_mismatches_leave_graph_unchanged() {
    let (setup, mut boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        let root = setup.anchor.verified.checkpoint().root;
        let before = setup.store.snapshot().unwrap();
        boundary.base_hash = Some([88; 32]);
        assert!(matches!(
            boundary.verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty)),
            Err(ExecutionBoundaryError::Binding)
        ));
        boundary.base_hash = Some([41; 32]);
        boundary.pending_payload_hash = [77; 32];
        assert!(matches!(
            boundary.verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty)),
            Err(ExecutionBoundaryError::Binding)
        ));
        assert_eq!(setup.store.snapshot().unwrap(), before);
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_signed_full_envelope_requires_engine_and_empty_custody_then_remains_core_authority() {
    let (mut setup, boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        let root = setup.anchor.verified.checkpoint().root;
        let signed = envelope(&setup, root);
        let before = setup.store.snapshot().unwrap();
        let contract = ExactEnvelope {
            payload: signed.message.payload.clone(),
            parent_root: signed.message.parent_beacon_block_root,
            calls: AtomicUsize::new(0),
            accept: false,
        };
        let da = EmptyCustody { root, calls: AtomicUsize::new(0) };
        assert!(tokio::runtime::Handle::current()
            .block_on(setup.store.on_execution_payload_envelope(&signed, &contract, &da))
            .is_err());
        assert!(boundary
            .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Full))
            .is_err());
        assert_eq!(setup.store.snapshot().unwrap(), before);
        assert_eq!(contract.calls.load(Ordering::SeqCst), 1);
        assert_eq!(da.calls.load(Ordering::SeqCst), 1);
        let contract = ExactEnvelope { accept: true, ..contract };
        tokio::runtime::Handle::current()
            .block_on(setup.store.on_execution_payload_envelope(&signed, &contract, &da))
            .unwrap();
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Full))
                .unwrap(),
            Some([44; 32])
        );
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
        let child = import(&mut setup, true, 45);
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(child, PayloadStatus::Empty))
                .unwrap(),
            Some([44; 32])
        );
        // Rejected and accepted envelopes each verify this exact payload and empty custody.
        assert_eq!(contract.calls.load(Ordering::SeqCst), 2);
        assert_eq!(da.calls.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normative_unknown_parent_and_nonincreasing_cycle_links_cannot_enter_private_graph() {
    let (mut setup, boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        let before = setup.store.snapshot().unwrap();
        let mut signed = child(&setup, false, 45);
        signed.message.parent_root = [99; 32];
        assert!(matches!(setup.store.on_block(&signed), Err(StoreError::UnknownRoot)));
        let mut replay = child(&setup, false, 45);
        replay.message.slot = 16;
        assert!(setup.store.on_block(&replay).is_err());
        assert_eq!(setup.store.snapshot().unwrap(), before);
        assert_eq!(
            boundary
                .verified_execution_block_hash(
                    &setup.store,
                    node(setup.anchor.verified.checkpoint().root, PayloadStatus::Empty)
                )
                .unwrap(),
            Some([41; 32])
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_known_retained_dependency_can_boost_after_unknown_checkpoint_history() {
    let (mut setup, boundary, _) = ready().await;
    tokio::task::spawn_blocking(move || {
        let early = import(&mut setup, false, 80);
        assert_eq!(setup.store.proposer_boost_root(), [0; 32]);
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(early, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
        // Empty slots do not invent missed votes. Once the requested shuffling dependency is
        // after the actual retained anchor, both real selected-parent paths can be compared.
        let target = setup.anchor.verified.slot() + 2 * setup.config.slots_per_epoch;
        setup.store.on_tick(setup.config.clock().unwrap().start_of_slot(target).unwrap()).unwrap();
        let signed = child_at_slot(&setup, false, 81, target);
        let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
        setup.store.on_block(&signed).unwrap();
        assert_eq!(setup.store.proposer_boost_root(), root);
        assert_eq!(
            boundary
                .verified_execution_block_hash(&setup.store, node(root, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_compact_checkpoint_cold_restart_preserves_base_and_reports_missing_durable_data() {
    let (setup, boundary, engine) = ready().await;
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let network = PersistenceNetwork::from_config(setup.config.clone()).unwrap();
        let identity = network.identity();
        let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
        let repository = Arc::new(
            AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
                .unwrap(),
        );
        let verifier = SszCheckpointVerifier::new(
            setup.config.clone(),
            setup.anchor.decode_limits,
            setup.anchor.block_format,
        )
        .unwrap();
        repository
            .persist_checkpoint(
                &verifier,
                &setup.anchor.verified,
                &setup.anchor.state_ssz,
                &setup.anchor.block_ssz,
                setup.now,
                setup.anchor.max_age_epochs,
            )
            .unwrap();
        let root = setup.anchor.verified.checkpoint().root;
        let source = RepositoryStateSource::open(repository.clone()).unwrap();
        let policy = CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() };
        let compact = CompactGloasStore::from_store(setup.store, source.clone(), policy).unwrap();
        assert_eq!(compact.stats().unwrap().hot_states, 0);
        assert!(compact.stats().unwrap().cold_entries > 0);
        boundary.check_adapter(&setup.config, &engine).unwrap();
        assert_eq!(
            boundary
                .verified_execution_block_hash(&compact, node(root, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
        assert_eq!(boundary.finalized_execution_block_hash(&compact).unwrap(), Some([41; 32]));
        assert!(repository.current_snapshot().unwrap().is_none());
        let held = compact.read_block_state(root).unwrap().unwrap();
        let original =
            ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, &setup.anchor.block_ssz);
        shared.release(Owner::Checkpoint(root), original).unwrap();
        assert!(matches!(
            boundary.verified_execution_block_hash(&compact, node(root, PayloadStatus::Empty)),
            Err(ExecutionBoundaryError::Store(StoreError::CompactState(
                StateSourceError::BindingMismatch
            )))
        ));
        assert_eq!(held.latest_block_hash, [41; 32]);
        shared.pin(Owner::Checkpoint(root), original).unwrap();
        drop(held);
        drop(compact);
        drop(source);
        drop(repository);
        drop(shared);
        let network = PersistenceNetwork::from_config(setup.config.clone()).unwrap();
        let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
        let repository = Arc::new(
            AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap(),
        );
        let recovered = repository
            .recover_checkpoint(&verifier, setup.now, setup.anchor.max_age_epochs)
            .unwrap()
            .unwrap();
        let BeaconState::Gloas(state, preset) = recovered.state.state() else {
            panic!("original Gloas schema")
        };
        let BeaconBlock::Gloas(block, _) =
            SignedBeaconBlock::from_ssz(&recovered.block_ssz, Fork::Gloas, *preset)
                .unwrap()
                .message()
        else {
            panic!("original signed Gloas block")
        };
        let runtime = RuntimeParameters::from_config(&setup.config).unwrap();
        let graph = GloasStore::get_store(
            state.clone(),
            block,
            runtime.fork_choice,
            runtime.transitions.gloas,
        )
        .unwrap();
        drop(recovered);
        let source = RepositoryStateSource::open(repository).unwrap();
        let compact = CompactGloasStore::from_store(graph, source, policy).unwrap();
        assert_eq!(compact.stats().unwrap().hot_states, 0);
        assert_eq!(
            boundary
                .verified_execution_block_hash(&compact, node(root, PayloadStatus::Empty))
                .unwrap(),
            Some([41; 32])
        );
        assert!(matches!(
            compact.verified_execution_block_hash(node(root, PayloadStatus::Full)),
            Err(StoreError::ExecutionImport(_))
        ));
    })
    .await
    .unwrap();
}
