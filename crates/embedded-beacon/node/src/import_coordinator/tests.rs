//! Strict import and publication boundary regressions with genuine BLS inputs.
//! Controlled Engine replies establish policy only, never actual EL validation or live sync.

use super::*;
use crate::{
    compact_persistence::PersistenceNetwork,
    gossip_tests::{
        fulu_gloas_boundary, phase0_gloas_boundary, phase0_signed_child, signing_root,
        store_and_keys,
    },
};
use alloy_rpc_types_engine::{ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_config::{BlobParameters, ForkActivation, ForkSchedule};
use reth_embedded_beacon_crypto::{CryptoError, ETHEREUM_BLS_DST};
use reth_embedded_beacon_execution_proxy::{ExecutionProxy, PayloadVersion};
use reth_embedded_beacon_primitives::{uint64_root, Checkpoint};
use reth_embedded_beacon_shared_data::{journal::inject_post_rename_failure, SharedDataStore};
use reth_embedded_beacon_state_transition::{
    bellatrix_processing, deneb_processing,
    fork_processing::{self, TransitionParameters},
    gloas_processing,
    ssz::{bellatrix, deneb, fulu, List, ProgressiveList, Ssz, Vector},
};
use reth_embedded_beacon_storage::CompactStateLimits;
use reth_fs_util as fs;
use std::{
    io,
    sync::{atomic::AtomicU64, Mutex},
};
use tempfile::TempDir;

#[derive(Debug)]
struct Backend {
    payload_status: Mutex<PayloadStatusEnum>,
    forkchoice_status: Mutex<PayloadStatusEnum>,
    payload_calls: Mutex<Vec<PayloadVersion>>,
    forkchoice_calls: Mutex<Vec<(ForkchoiceVersion, ForkchoiceState)>>,
    expected_payload_hash: Mutex<Root>,
    callback_clock: Mutex<Option<(Arc<AtomicU64>, u64)>>,
}
#[derive(Clone, Debug)]
struct BackendHandle(Arc<Backend>);
impl ExecutionEngine for BackendHandle {
    type Payload = ExecutionData;
    type Forkchoice = ForkchoiceState;
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = io::Error;
    async fn new_payload(
        &self,
        version: PayloadVersion,
        _payload: ExecutionData,
    ) -> Result<PayloadReply, io::Error> {
        self.0.payload_calls.lock().unwrap().push(version);
        let status = self.0.payload_status.lock().unwrap().clone();
        let hash = matches!(status, PayloadStatusEnum::Valid)
            .then_some(B256::from(*self.0.expected_payload_hash.lock().unwrap()));
        if let Some((clock, now)) = self.0.callback_clock.lock().unwrap().take() {
            clock.store(now, Ordering::Release);
        }
        Ok(PayloadReply::V1(PayloadStatus::new(status, hash)))
    }
    async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, io::Error> {
        assert_eq!(custody, None);
        self.0.forkchoice_calls.lock().unwrap().push((version, state));
        let status = self.0.forkchoice_status.lock().unwrap().clone();
        let hash = matches!(status, PayloadStatusEnum::Valid).then_some(state.head_block_hash);
        Ok(ForkchoiceReply::V1(ForkchoiceUpdated::new(PayloadStatus::new(status, hash))))
    }
}
#[derive(Debug)]
struct MissingAvailability;
impl ClassicAvailability for MissingAvailability {
    type Error = io::Error;
    async fn verify(&self, _root: Root, _block: &BeaconBlock) -> Result<(), io::Error> {
        Err(io::Error::other("actual retrieval unavailable in controlled test"))
    }
}
impl GloasAvailability for MissingAvailability {
    type Error = io::Error;
    async fn verify(&self, _root: Root, _block: &gloas::BeaconBlock) -> Result<(), io::Error> {
        Err(io::Error::other("actual custody retrieval unavailable in controlled test"))
    }
}
fn config(state: &BeaconState, parameters: &TransitionParameters) -> ChainConfig {
    ChainConfig {
        name: "strict-import-test".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 12345,
        genesis_time: state.genesis_time(),
        genesis_validators_root: state.genesis_validators_root(),
        seconds_per_slot: parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
        slots_per_epoch: parameters.slots_per_epoch(),
        forks: ForkSchedule::new(
            parameters
                .schedule
                .iter()
                .map(|entry| ForkActivation {
                    fork: entry.fork,
                    epoch: entry.epoch,
                    version: entry.version,
                })
                .collect(),
        )
        .unwrap(),
        blob_schedule: parameters
            .gloas
            .fulu
            .blob_schedule
            .iter()
            .map(|entry| BlobParameters {
                epoch: entry.epoch,
                max_blobs_per_block: entry.max_blobs_per_block,
            })
            .collect(),
        max_payload_size: 10 * 1024 * 1024,
    }
}
fn repository(directory: &TempDir, config: ChainConfig) -> AdmittedStateRepository {
    let network = PersistenceNetwork::from_config(config).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap()
}
fn setup(
    directory: &TempDir,
    store: &Store,
    parameters: &TransitionParameters,
) -> (WorkerSetup<BackendHandle, MissingAvailability, MissingAvailability>, Arc<Backend>) {
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let block = store.block(root).unwrap();
    let config = config(state, parameters);
    let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
    let block_ssz = Arc::<[u8]>::from(block.to_ssz().unwrap());
    let decode_limits = BootstrapDecodeLimits::default();
    let verifier =
        SszCheckpointVerifier::new(config.clone(), decode_limits, CheckpointBlockFormat::Unsigned)
            .unwrap();
    let verified = verifier
        .verify_checkpoint(
            &state_ssz,
            &block_ssz,
            Checkpoint { epoch: state.slot() / parameters.slots_per_epoch(), root },
            (state.genesis_time() + state.slot() * config.seconds_per_slot) * 1000,
            1,
        )
        .unwrap();
    let backend = Arc::new(Backend {
        payload_status: Mutex::new(PayloadStatusEnum::Valid),
        forkchoice_status: Mutex::new(PayloadStatusEnum::Valid),
        payload_calls: Mutex::new(Vec::new()),
        forkchoice_calls: Mutex::new(Vec::new()),
        expected_payload_hash: Mutex::new([2; 32]),
        callback_clock: Mutex::new(None),
    });
    let proxy =
        ExecutionProxy::new(BackendHandle(backend.clone()), Duration::from_millis(50)).unwrap();
    let engine = RuntimeEngineAdapter::new(proxy, config.clone()).unwrap();
    (
        WorkerSetup {
            repository: repository(directory, config.clone()),
            config,
            anchor: ImportAnchor {
                verified,
                state_ssz,
                block_ssz,
                block_format: CheckpointBlockFormat::Unsigned,
                max_age_epochs: 1,
                decode_limits,
                replay: None,
            },
            engine,
            classic_availability: MissingAvailability,
            gloas_availability: MissingAvailability,
            limits: ImportLimits::default(),
            runtime: Handle::current(),
            clock: OwnerClock::Controlled(Arc::new(AtomicU64::new(
                parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot * 1000,
            ))),
        },
        backend,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_bls_rejection_never_publishes_and_real_child_advances_the_actual_graph() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    // The owner implementation is synchronous; even unit execution keeps its SSZ/BLS/storage
    // and Handle::block_on calls on the blocking pool, never on an async IO worker.
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let original = worker.graph.snapshot().unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        let wire = WireBlock::from_ssz(invalid.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        // A new Phase0 block runs the mandatory signed transition. Its original BLS error
        // remains a consensus rejection; there is no execution payload to defer for catch-up.
        let rejected = worker.import(Input::Block(wire), 0);
        assert!(
            matches!(
                &rejected,
                Err(ImportError::Consensus(StoreError::ExecutionImport(message)))
                    if message == &CryptoError::InvalidSignature.to_string()
            ),
            "{rejected:?}"
        );
        assert_eq!(worker.graph.snapshot().unwrap().head, original.head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), original.head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let root = wire.root();
        let receipt = worker.import(Input::Block(wire.clone()), 0).unwrap();
        assert_eq!(receipt.admitted_root, root);
        assert_eq!(receipt.snapshot.head.root, root);
        assert_eq!(receipt.snapshot.head_slot, 1);
        assert!(!receipt.execution_forkchoice_updated);
        assert_eq!(
            worker.repository.original_block(root).unwrap().unwrap().1.as_ref(),
            wire.ssz().as_ref()
        );
        assert_eq!(
            worker.repository.current_snapshot().unwrap().unwrap().head(),
            receipt.snapshot.head
        );
        let mut invalid_duplicate = signed.clone();
        invalid_duplicate.signature = [0; 96];
        let invalid_duplicate =
            WireBlock::from_ssz(invalid_duplicate.to_ssz(Preset::Minimal).unwrap(), &config)
                .unwrap();
        assert!(matches!(
            worker.import(Input::Block(invalid_duplicate), 0),
            Err(ImportError::Wire)
        ));
        let duplicate = worker.import(Input::Block(wire), 0).unwrap();
        assert_eq!(duplicate.snapshot.head.root, root);
        assert_eq!(worker.import_metrics.attempts_total, 4);
        assert_eq!(worker.import_metrics.blocks_total, 4);
        assert_eq!(worker.import_metrics.envelopes_total, 0);
        assert_eq!(worker.import_metrics.accepted_total, 2);
        assert_eq!(worker.import_metrics.newly_admitted_blocks_total, 1);
        assert_eq!(worker.import_metrics.duplicate_blocks_total, 1);
        assert_eq!(worker.import_metrics.measured_native_imports_total, 1);
        assert_eq!(worker.import_metrics.rejected_total, 2);
        assert_eq!(worker.import_metrics.execution_deferred_total, 0);
        assert!(
            worker.import_metrics.last_duration_micros <= worker.import_metrics.max_duration_micros
        );
        assert!(
            worker.import_metrics.max_duration_micros <=
                worker.import_metrics.duration_micros_total
        );
        assert!(!duplicate.execution_forkchoice_updated);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_original_wire_context_is_rejected_without_graph_or_data_publication() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (setup, _) = setup(&directory, &store, &parameters);
    let mut foreign = setup.config.clone();
    foreign.genesis_validators_root = [9; 32];
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &foreign).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(worker.config.seconds_per_slot * 1000).unwrap();
        let head = worker.graph.snapshot().unwrap().head;
        assert!(matches!(worker.import(Input::Block(wire), 0), Err(ImportError::Wire)));
        assert_eq!(worker.graph.snapshot().unwrap().head, head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_final_pointer_halts_without_publishing_candidate_in_memory() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (setup, _) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        let head = worker.graph.snapshot().unwrap().head;
        // Stage the genuine signed candidate through the same native Engine/DA admission. Inject
        // only the next canonical publication, after original bytes, clock and prepare are durable.
        let mut candidate = worker.graph.clone();
        let signed = decode_block(&wire, &worker.config).unwrap();
        let Graph::Classic { store, .. } = &mut candidate else { panic!("classic owner") };
        worker
            .runtime
            .block_on(store.on_block_with_engine(
                &signed,
                &worker.engine,
                &worker.classic_availability,
            ))
            .unwrap();
        assert!(candidate.admit_body_observations(wire.root(), &signed).unwrap().is_empty());
        worker
            .record_import_event(&Input::Block(wire.clone()), 0, worker.graph.time_ms(), &candidate)
            .unwrap();
        worker.source.prepare_snapshot(candidate.snapshot().unwrap()).unwrap();
        let journal = directory.path().join("shared-data-v1/beacon-admitted-states-v1/journal");
        let generation = fs::read_dir(&journal)
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("snapshot-"))
                    .and_then(|name| name.strip_suffix(".commit"))
                    .and_then(|number| number.parse::<u64>().ok())
                    .unwrap_or(0)
            })
            .max()
            .unwrap();
        inject_post_rename_failure(journal.join(format!("snapshot-{:020}.commit", generation + 1)));
        let error = worker.publish(&mut candidate).unwrap_err();
        assert!(matches!(error, ImportError::Persistence(_)));
        assert!(error.fatal());
        assert_eq!(worker.graph.snapshot().unwrap().head, head);
        assert!(worker.repository.current_snapshot().is_err());
        // The candidate graph shares the state source and its exclusive durable writer lock.
        drop(candidate);
        drop(worker);
        let recovered = repository(&directory, config);
        assert_eq!(recovered.current_snapshot().unwrap().unwrap().head().root, wire.root());
        // A recovered hint is deliberately not a graph constructor or a proof of live recovery.
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_engine_network_binding_is_checked_before_bootstrap_data_is_written() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let (mut setup, backend) = setup(&directory, &store, &parameters);
    let mut foreign = setup.config.clone();
    foreign.name = "another-checked-profile".into();
    setup.engine = RuntimeEngineAdapter::new(
        ExecutionProxy::new(BackendHandle(backend), Duration::from_millis(50)).unwrap(),
        foreign,
    )
    .unwrap();
    tokio::task::spawn_blocking(move || {
        assert!(matches!(Worker::new(setup, Ok(0)), Err(ImportError::Configuration)));
    })
    .await
    .unwrap();
}

#[test]
fn queue_bounds_and_fatal_outcomes_preserve_their_distinct_meanings() {
    let limits = ImportLimits::default();
    assert_eq!(
        limits.reserve_bytes(limits.max_input_bytes).unwrap(),
        limits.max_input_bytes as u32
    );
    assert!(matches!(limits.reserve_bytes(0), Err(ImportError::Bounds)));
    assert!(matches!(limits.reserve_bytes(limits.max_input_bytes + 1), Err(ImportError::Bounds)));
    for limits in [
        ImportLimits { command_capacity: 0, ..limits },
        ImportLimits { max_queued_bytes: 1, ..limits },
        ImportLimits { max_tick_slots: 8193, ..limits },
    ] {
        assert!(matches!(limits.validate(), Err(ImportError::Bounds)));
    }
    assert!(ImportError::Forkchoice("transport outcome unknown".into()).fatal());
    assert!(ImportError::EngineUncertain.fatal());
    for error in [
        StoreError::UnknownRoot,
        StoreError::InvalidAttestationTime,
        StoreError::InvalidAttestationTarget,
        StoreError::InvalidSlashing,
    ] {
        retained_observation_result([0; 32], 0, "attestation", Err(error)).unwrap();
    }
    for error in [
        StoreError::ResourceLimit,
        StoreError::InvalidParameters,
        StoreError::Kernel(ForkChoiceError::Overflow),
    ] {
        let failure =
            retained_observation_result([0; 32], 0, "attestation", Err(error)).unwrap_err();
        assert!(matches!(failure, ImportError::ObservationInfrastructure(_)));
        assert!(failure.fatal());
    }
    assert!(!ImportError::Wire.fatal());
    assert!(!ImportError::Consensus(StoreError::InvalidBlockTime).fatal());
    assert_eq!(forkchoice_version(Fork::Bellatrix), Some(ForkchoiceVersion::V1));
    assert_eq!(forkchoice_version(Fork::Capella), Some(ForkchoiceVersion::V2));
    assert_eq!(forkchoice_version(Fork::Fulu), Some(ForkchoiceVersion::V3));
    assert_eq!(forkchoice_version(Fork::Gloas), Some(ForkchoiceVersion::V4));
    assert_eq!(authenticated_safe_hash(Some([7; 32]), Some([7; 32])).unwrap(), [7; 32]);
    assert_eq!(authenticated_safe_hash(Some([0; 32]), None).unwrap(), [0; 32]);
    assert_eq!(authenticated_safe_hash(None, None).unwrap(), [0; 32]);
    assert!(matches!(
        authenticated_safe_hash(Some([7; 32]), None),
        Err(ImportError::Consensus(StoreError::InvalidParameters))
    ));
    assert!(matches!(
        authenticated_safe_hash(Some([7; 32]), Some([8; 32])),
        Err(ImportError::Consensus(StoreError::InvalidParameters))
    ));
}

#[tokio::test]
async fn queued_original_bytes_keep_the_reservation_until_processing_finishes() {
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let root = store.get_head().unwrap().root;
    let config = config(store.block_state(root).unwrap(), &parameters);
    let block = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    let size = block.ssz().len();
    let limits = ImportLimits {
        command_capacity: 1,
        max_input_bytes: size,
        max_queued_bytes: size,
        ..ImportLimits::default()
    };
    let (sender, mut receiver) = mpsc::channel(1);
    let handle = ImportCoordinator {
        queue_high_water: Default::default(),
        config_digest: [0; 32],
        sender,
        commands: Arc::new(Semaphore::new(1)),
        bytes: Arc::new(Semaphore::new(size)),
        halted: Arc::new(AtomicBool::new(false)),
        limits,
    };
    let queued = tokio::spawn(handle.import_block(block.clone(), 0));
    let command = receiver.recv().await.unwrap();
    assert_eq!(handle.bytes.available_permits(), 0);
    assert_eq!(handle.commands.available_permits(), 0);
    let occupied = handle.clone().queue_observation();
    assert_eq!((occupied.reserved_jobs, occupied.reserved_bytes), (1, size));
    assert_eq!((occupied.jobs_high_water, occupied.bytes_high_water), (1, size));
    assert_eq!((occupied.job_capacity, occupied.byte_capacity), (1, size));
    assert_eq!(occupied.imports_enqueued_total, 1);
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    // A cancelled reply and a dequeued active command still hold both counted reservations.
    assert_eq!(handle.bytes.available_permits(), 0);
    assert_eq!(handle.commands.available_permits(), 0);
    let original_bytes = block.ssz().clone();
    let counted = Arc::strong_count(&original_bytes);
    let rejected = handle.import_block(block.clone(), 0);
    // Even an unpolled rejected reply future must not retain another large original input.
    assert_eq!(Arc::strong_count(&original_bytes), counted);
    assert!(matches!(rejected.await, Err(ImportError::Backpressure)));
    assert_eq!(Arc::strong_count(&original_bytes), counted);
    assert!(matches!(handle.snapshot().await, Err(ImportError::Backpressure)));
    crate::live_sync::tests::assert_backpressure_preserves_live_input(
        handle.clone(),
        config,
        block.clone(),
    )
    .await;
    let cancelled = handle.queue_observation();
    assert_eq!((cancelled.reserved_jobs, cancelled.reserved_bytes), (1, size));
    assert_eq!((cancelled.imports_enqueued_total, cancelled.import_backpressure_total), (1, 1));
    drop(command);
    assert_eq!(handle.bytes.available_permits(), size);
    assert_eq!(handle.commands.available_permits(), 1);
    let released = handle.clone().queue_observation();
    assert_eq!((released.reserved_jobs, released.reserved_bytes), (0, 0));
    assert_eq!((released.jobs_high_water, released.bytes_high_water), (1, size));
    let shutdown = Shutdown {
        bytes: handle.bytes.clone(),
        commands: handle.commands.clone(),
        halted: handle.halted.clone(),
    };
    drop(shutdown);
    assert!(handle.is_halted());
    assert!(matches!(handle.snapshot().await, Err(ImportError::Halted)));
}

// This private policy anchor explicitly trusts a controlled post-Merge execution association.
// It grants no terminal ancestry or real EL proof. Its descendants still use actual BLS,
// full transition commitments and the mandatory typed Engine boundary independently.
fn post_merge_anchor() -> (Store, Vec<SecretKey>, TransitionParameters) {
    let (original, keys, parameters) = store_and_keys(Fork::Bellatrix);
    let root = original.get_head().unwrap().root;
    let BeaconState::Bellatrix(mut state, preset) = original.block_state(root).unwrap().clone()
    else {
        unreachable!()
    };
    let BeaconBlock::Bellatrix(mut block, _) = original.block(root).unwrap().clone() else {
        unreachable!()
    };
    block.body.execution_payload.block_hash = [1; 32];
    block.body.execution_payload.gas_limit = 30_000_000;
    block.body.execution_payload.prev_randao = state.randao_mixes.values[0];
    state.latest_execution_payload_header =
        bellatrix_processing::validate_execution_payload_consensus(
            &state,
            &block.body.execution_payload,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
        )
        .unwrap();
    state.latest_block_header.body_root = block.body.hash_tree_root(preset).unwrap();
    block.state_root = state.hash_tree_root(preset).unwrap();
    let mut store = Store::get_store(
        BeaconState::Bellatrix(state, preset),
        BeaconBlock::Bellatrix(block, preset),
        parameters_for(&parameters),
    )
    .unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    (store, keys, parameters)
}
fn parameters_for(
    parameters: &TransitionParameters,
) -> reth_embedded_beacon_fork_choice::StoreParameters {
    let mut result = reth_embedded_beacon_fork_choice::StoreParameters::for_preset(Preset::Minimal);
    result.slot_duration_ms =
        parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot * 1000;
    result
}
fn bellatrix_signed_child(
    store: &impl ReadGossipView,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> bellatrix::SignedBeaconBlock {
    let root = store.get_head().unwrap().root;
    let owned = ReadGossipView::read_block_state(store, root).unwrap().unwrap();
    let BeaconState::Bellatrix(mut state, preset) = owned.as_ref().clone() else { unreachable!() };
    let processing = &parameters.gloas.fulu.electra.deneb.capella.bellatrix;
    let slot = state.slot + 1;
    bellatrix_processing::process_slots(&mut state, slot, processing).unwrap();
    let proposer = bellatrix_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[usize::try_from(proposer).unwrap()];
    let epoch = slot / processing.slots_per_epoch();
    let version = state.fork.current_version;
    let mut message = bellatrix::BeaconBlock::default_value(preset).unwrap();
    message.slot = slot;
    message.proposer_index = proposer;
    message.parent_root = root;
    message.body.eth1_data = state.eth1_data.clone();
    message.body.randao_reveal = key
        .sign(
            &signing_root(uint64_root(epoch), [2, 0, 0, 0], version, state.genesis_validators_root),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    message.body.execution_payload.parent_hash = state.latest_execution_payload_header.block_hash;
    message.body.execution_payload.block_number =
        state.latest_execution_payload_header.block_number + 1;
    message.body.execution_payload.block_hash =
        [u8::try_from(message.body.execution_payload.block_number + 1).unwrap(); 32];
    message.body.execution_payload.gas_limit = 30_000_000;
    message.body.execution_payload.timestamp =
        state.genesis_time + slot * processing.seconds_per_slot;
    message.body.execution_payload.prev_randao = state.randao_mixes.values[(epoch % 64) as usize];
    message.body.sync_aggregate.sync_committee_signature[0] = 0xc0;
    bellatrix_processing::process_block_header(&mut state, &message, processing).unwrap();
    state.latest_execution_payload_header =
        bellatrix_processing::validate_execution_payload_consensus(
            &state,
            &message.body.execution_payload,
            processing,
        )
        .unwrap();
    bellatrix_processing::process_randao(&mut state, &message.body, processing).unwrap();
    bellatrix_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    bellatrix_processing::process_operations(&mut state, &message.body, processing).unwrap();
    bellatrix_processing::process_sync_aggregate(
        &mut state,
        &message.body.sync_aggregate,
        processing,
    )
    .unwrap();
    message.state_root = state.hash_tree_root(preset).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(preset).unwrap(),
                [0; 4],
                version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    bellatrix::SignedBeaconBlock { message, signature }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syncing_payload_never_mutates_graph_data_or_canonical_execution() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &setup.config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(worker.config.seconds_per_slot * 1000).unwrap();
        let head = worker.graph.snapshot().unwrap().head;
        *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        let root = wire.root();
        assert!(matches!(
            worker.import(Input::Block(wire), 0),
            Err(ImportError::ExecutionPending(actual)) if actual == root
        ));
        assert_eq!(worker.graph.snapshot().unwrap().head, head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        assert_eq!(worker.import_metrics.attempts_total, 1);
        assert_eq!(worker.import_metrics.execution_deferred_total, 1);
        assert_eq!(worker.import_metrics.accepted_total, 0);
        assert_eq!(worker.import_metrics.rejected_total, 0);
        assert_eq!(*backend.payload_calls.lock().unwrap(), vec![PayloadVersion::V1]);
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 1);
        assert!(!worker.engine.is_closed());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attempted_forkchoice_error_is_fatal_even_when_proxy_admission_is_open() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &setup.config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(worker.config.seconds_per_slot * 1000).unwrap();
        let head = worker.graph.snapshot().unwrap().head;
        *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        let error = worker.import(Input::Block(wire.clone()), 0).unwrap_err();
        assert!(matches!(error, ImportError::Forkchoice(_)));
        assert!(error.fatal());
        assert!(!worker.engine.is_closed());
        assert_eq!(worker.graph.snapshot().unwrap().head, head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
        assert!(worker.repository.original_block(wire.root()).unwrap().is_some());
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 2);
        // Data is durable, but the remote FCU outcome cannot be described as rolled back.
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_actual_da_rejects_before_any_payload_or_data_publication() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Deneb);
    let parent = store.get_head().unwrap().root;
    let BeaconState::Deneb(state, preset) =
        fork_processing::replay_slots(store.block_state(parent).unwrap(), 1, &parameters, 1)
            .unwrap()
    else {
        unreachable!()
    };
    let processing = &parameters.gloas.fulu.electra.deneb;
    let proposer = deneb_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let mut message = deneb::BeaconBlock::default_value(preset).unwrap();
    message.slot = 1;
    message.parent_root = parent;
    message.proposer_index = proposer;
    let signature = keys[usize::try_from(proposer).unwrap()]
        .sign(
            &signing_root(
                message.hash_tree_root(preset).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let signed = deneb::SignedBeaconBlock { message, signature };
    let (setup, backend) = setup(&directory, &store, &parameters);
    let wire = WireBlock::from_ssz(signed.to_ssz(preset).unwrap(), &setup.config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(worker.config.seconds_per_slot * 1000).unwrap();
        let head = worker.graph.snapshot().unwrap().head;
        assert!(matches!(
            worker.import(Input::Block(wire), 0),
            Err(ImportError::Consensus(StoreError::DataAvailability(_)))
        ));
        assert_eq!(worker.graph.snapshot().unwrap().head, head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        // This negative prerequisite control has a genuine outer signature; missing DA
        // blocks the mandatory complete transition rather than granting its validity.
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_saved_later_head_cannot_reseed_or_overwrite_the_authenticated_anchor_graph() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (initial, _) = setup(&directory, &store, &parameters);
    let config = initial.config.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        worker.import(Input::Block(wire.clone()), 0).unwrap();
        drop(worker);
        let (reopened, backend) = setup(&directory, &store, &parameters);
        assert!(matches!(Worker::new(reopened, Ok(0)), Err(ImportError::RecoveryRequired)));
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        assert_eq!(
            repository(&directory, config).current_snapshot().unwrap().unwrap().head().root,
            wire.root()
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_clock_catchup_rejects_before_graph_or_persistence_mutation() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let (mut setup, backend) = setup(&directory, &store, &parameters);
    setup.limits.max_tick_slots = 1;
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let initial = worker.graph.snapshot().unwrap();
        assert!(matches!(
            worker.advance(worker.config.seconds_per_slot * 2000),
            Err(ImportError::Bounds)
        ));
        assert_eq!(worker.graph.current_slot(), 0);
        assert_eq!(worker.graph.time_ms(), 0);
        assert_eq!(worker.graph.snapshot().unwrap().head, initial.head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), initial.head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossed_slot_callback_uses_finalized_safety_and_resumes_genuine_import() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (mut setup, backend) = setup(&directory, &store, &parameters);
    setup.limits.fast_confirmation = true;
    let config = setup.config.clone();
    let OwnerClock::Controlled(clock) = &setup.clock else { unreachable!() };
    let clock = clock.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let slot_ms = config.seconds_per_slot * 1000;
        worker.advance(slot_ms).unwrap();
        // Only the private controlled callback's clock advances. Signed votes, post-state and
        // block metadata remain the genuine input; this is timing policy, not a real EL proof.
        *backend.callback_clock.lock().unwrap() = Some((clock.clone(), slot_ms * 2 + slot_ms / 2));
        let first = worker.import(Input::Block(wire.clone()), 0).unwrap();
        assert!(first.execution_forkchoice_updated);
        assert_eq!(worker.graph.current_slot(), 2);
        assert_eq!(worker.graph.time_ms(), slot_ms * 2 + slot_ms / 2);
        let Graph::Classic { store, confirmation } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        assert_eq!(
            store.read_block_state(wire.root()).unwrap().unwrap().hash_tree_root().unwrap(),
            signed.message.state_root
        );
        let second = bellatrix_signed_child(store, &keys, &parameters);
        let second_wire =
            WireBlock::from_ssz(second.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        *backend.expected_payload_hash.lock().unwrap() = [3; 32];
        let second = worker.import(Input::Block(second_wire.clone()), 0).unwrap();
        assert_eq!(second.snapshot.head.root, second_wire.root());
        assert_eq!(second.snapshot.head_slot, 2);
        assert_eq!(
            worker.repository.current_snapshot().unwrap().unwrap().head(),
            second.snapshot.head
        );
        assert_eq!(backend.payload_calls.lock().unwrap().len(), 2);
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 3);
        let calls = backend.forkchoice_calls.lock().unwrap();
        for (_, state) in calls.iter() {
            assert_eq!(state.safe_block_hash, B256::from([1; 32]));
            assert_eq!(state.finalized_block_hash, B256::from([1; 32]));
        }
        drop(calls);
        assert!(!worker.engine.is_closed());
        clock.store(slot_ms * 3, Ordering::Release);
        worker.advance(slot_ms * 3).unwrap();
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert_eq!(
            confirmation.state.as_ref().unwrap().snapshot().current_slot_head,
            second_wire.root()
        );
        assert_eq!(worker.graph.current_slot(), 3);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_gloas_migration_preserves_real_history_and_requires_signed_recovery() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters, signed) = fulu_gloas_boundary();
    let (initial_setup, backend) = setup(&directory, &store, &parameters);
    let config = initial_setup.config.clone();
    let OwnerClock::Controlled(clock) = &initial_setup.clock else { unreachable!() };
    let clock = clock.clone();
    let initial_root = store.get_head().unwrap().root;
    let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
    let BeaconBlock::Fulu(anchor, _) = store.block(initial_root).unwrap() else { unreachable!() };
    let anchor_signature = keys[anchor.proposer_index as usize]
        .sign(
            &signing_root(
                initial_root,
                [0; 4],
                parameters.gloas.fulu.fork_version,
                config.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let historical_wire = WireBlock::from_ssz(
        fulu::SignedBeaconBlock { message: anchor.clone(), signature: anchor_signature }
            .to_ssz(Preset::Minimal)
            .unwrap(),
        &config,
    )
    .unwrap();
    tokio::task::spawn_blocking(move || {
        let original_store = store;
        let mut worker = Worker::new(initial_setup, Ok(0)).unwrap();
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        let shared_original = store.read_block_state(initial_root).unwrap().unwrap();
        let original = shared_original.to_ssz().unwrap();
        let now = config.seconds_per_slot * 1000 * config.slots_per_epoch;
        clock.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        let mut invalid = signed.clone();
        let SignedBeaconBlock::Gloas(block, _) = &mut invalid else { unreachable!() };
        block.signature[5] ^= 1;
        let invalid_wire = WireBlock::from_ssz(invalid.to_ssz().unwrap(), &config).unwrap();
        assert!(matches!(
            worker.import(Input::Block(invalid_wire), 0),
            Err(ImportError::Consensus(_))
        ));
        assert!(matches!(worker.graph, Graph::Classic { .. }));
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        let admitted = worker.import(Input::Block(wire.clone()), 0).unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let retained = store.read_historical_block_state(initial_root).unwrap().unwrap();
        // Evicting a hot original may legitimately reconstruct a different allocation. The
        // owning cold read preserves the exact original commitment/schema and remains stable
        // while another real guard leases that same retained allocation.
        let repeated = store.read_historical_block_state(initial_root).unwrap().unwrap();
        assert!(std::ptr::eq(retained.as_ref(), repeated.as_ref()));
        assert_eq!(retained.hash_tree_root().unwrap(), shared_original.hash_tree_root().unwrap());
        assert_eq!(retained.schema_fork(), Fork::Fulu);
        assert_eq!(retained.to_ssz().unwrap(), original);
        assert_eq!(store.node_for_root(initial_root).unwrap().payload_status, None);
        assert_eq!(
            store.read_block_state(wire.root()).unwrap().unwrap().hash_tree_root().unwrap(),
            signed.header().unwrap().state_root
        );
        assert_eq!(admitted.snapshot.head.root, wire.root());
        assert!(!admitted.execution_forkchoice_updated);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        let duplicate = worker.import(Input::Block(historical_wire), 0).unwrap();
        assert_eq!(duplicate.snapshot.head, admitted.snapshot.head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
        let persisted_head = worker.repository.current_snapshot().unwrap().unwrap().head();
        drop(worker);
        let (reopen, reopened_backend) = setup(&directory, &original_store, &parameters);
        assert!(matches!(Worker::new(reopen, Ok(0)), Err(ImportError::RecoveryRequired)));
        assert!(reopened_backend.forkchoice_calls.lock().unwrap().is_empty());
        assert_eq!(persisted_head, admitted.snapshot.head);
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct ExactEmptyGloasData(Root);
impl GloasAvailability for ExactEmptyGloasData {
    type Error = io::Error;
    async fn verify(&self, root: Root, block: &gloas::BeaconBlock) -> Result<(), io::Error> {
        if root != self.0 ||
            !block
                .body
                .signed_execution_payload_bid
                .message
                .blob_kzg_commitments
                .values
                .is_empty()
        {
            return Err(io::Error::other("controlled candidate is not the exact empty-data block"))
        }
        Ok(())
    }
}
fn original_gloas_envelope(
    store: &impl ReadGloasGossipView,
    root: Root,
    keys: &[SecretKey],
) -> gloas::SignedExecutionPayloadEnvelope {
    let state = ReadGloasGossipView::read_block_state(store, root).unwrap().unwrap();
    let bid = &state.latest_execution_payload_bid;
    let requests = gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    };
    let message = gloas::ExecutionPayloadEnvelope {
        payload: gloas::ExecutionPayload {
            parent_hash: state.latest_block_hash,
            fee_recipient: bid.fee_recipient,
            state_root: [1; 32],
            receipts_root: [2; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: bid.prev_randao,
            block_number: state.slot + 1,
            gas_limit: bid.gas_limit,
            gas_used: 0,
            timestamp: state.genesis_time + state.slot * store.slot_duration_ms() / 1000,
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
        execution_requests: requests,
        builder_index: bid.builder_index,
        beacon_block_root: root,
        parent_beacon_block_root: state.latest_block_header.parent_root,
    };
    let signature = keys[state.latest_block_header.proposer_index as usize]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_pre_execution_history_has_zero_safe_only_after_verified_signed_gloas_envelope() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters, signed) = phase0_gloas_boundary();
    let (initial_setup, backend) = setup(&directory, &store, &parameters);
    let config = initial_setup.config.clone();
    let root = signed.message_root().unwrap();
    let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
    let WorkerSetup {
        config,
        anchor,
        repository,
        engine,
        classic_availability,
        gloas_availability: _,
        limits,
        runtime,
        clock,
    } = initial_setup;
    let controlled = match &clock {
        OwnerClock::Controlled(value) => value.clone(),
        _ => unreachable!(),
    };
    let initial_setup = WorkerSetup {
        config: config.clone(),
        anchor,
        repository,
        engine,
        classic_availability,
        gloas_availability: ExactEmptyGloasData(root),
        limits,
        runtime,
        clock,
    };
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial_setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000 * config.slots_per_epoch;
        controlled.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        let admitted = worker.import(Input::Block(wire.clone()), 0).unwrap();
        assert!(!admitted.execution_forkchoice_updated);
        let Graph::Gloas { store, confirmation } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        let confirmed = store.snapshot().unwrap().finalized_checkpoint.root;
        assert_eq!(store.historical_block(confirmed).unwrap().schema_fork(), Fork::Phase0);
        assert_eq!(store.node_for_root(confirmed).unwrap().payload_status, None);
        assert_eq!(
            store.verified_execution_block_hash(store.node_for_root(confirmed).unwrap()).unwrap(),
            None
        );
        let signed_envelope = original_gloas_envelope(store, root, &keys);
        let envelope = WireEnvelope::from_ssz(
            signed_envelope.to_ssz(Preset::Minimal).unwrap(),
            &wire,
            &config,
        )
        .unwrap();
        *backend.expected_payload_hash.lock().unwrap() = [9; 32];
        let verified = worker.import(Input::Envelope(envelope), 0).unwrap();
        assert!(verified.execution_forkchoice_updated);
        assert_eq!(
            verified.snapshot.head.payload_status,
            Some(reth_embedded_beacon_fork_choice::PayloadStatus::Full)
        );
        let Observation::Object(Some(full)) =
            worker.observe(ObservationRequest::Envelope(ObservationId::Root(root))).unwrap()
        else {
            panic!("actual canonical FULL signed envelope")
        };
        assert_eq!(full.block_root, root);
        assert!(full.canonical && !full.execution_optimistic);
        let calls = backend.forkchoice_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, ForkchoiceVersion::V4);
        assert_eq!(calls[0].1.head_block_hash, B256::from([9; 32]));
        assert_eq!(calls[0].1.safe_block_hash, B256::ZERO);
        assert_eq!(calls[0].1.finalized_block_hash, B256::ZERO);
        assert_eq!(
            worker.repository.current_snapshot().unwrap().unwrap().head(),
            verified.snapshot.head
        );
        drop(calls);
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let child = genuine_empty_gloas_child(store, root, &keys, &parameters);
        let child_wire =
            WireBlock::from_ssz(child.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let child_now = config.clock().unwrap().start_of_slot(child.message.slot).unwrap();
        controlled.store(child_now, Ordering::Release);
        worker.advance(child_now).unwrap();
        worker.import(Input::Block(child_wire), 0).unwrap();
        assert!(worker.repository.original_envelope(root).unwrap().is_some());
        assert!(matches!(
            worker.observe(ObservationRequest::Envelope(ObservationId::Root(root))).unwrap(),
            Observation::Object(None)
        ));
        let Observation::Object(Some(beacon)) = worker
            .observe(ObservationRequest::Block {
                id: ObservationId::Root(root),
                header_only: false,
            })
            .unwrap()
        else {
            panic!("canonical beacon ancestry retains original signed block")
        };
        assert!(beacon.canonical);
        assert!(matches!(
            worker.observe(ObservationRequest::StateRoot(ObservationId::Root([253; 32]))).unwrap(),
            Observation::Object(None)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_replayed_checkpoint_preserves_original_head_and_separate_boundary() {
    for upgrade in [false, true] {
        let fixture = crate::bootstrap::boundary_replay::tests::fixture(upgrade);
        let directory = TempDir::new().unwrap();
        let decode_limits = BootstrapDecodeLimits::default();
        let verifier = SszCheckpointVerifier::new(
            fixture.config.clone(),
            decode_limits,
            CheckpointBlockFormat::Signed,
        )
        .unwrap();
        let state_ssz = Arc::<[u8]>::from(fixture.boundary.to_ssz().unwrap());
        let post_block_state_ssz = Arc::<[u8]>::from(fixture.post.to_ssz().unwrap());
        let block_ssz = Arc::<[u8]>::from(fixture.block);
        let proof = verifier
            .verify_checkpoint_replay(
                &state_ssz,
                &post_block_state_ssz,
                &block_ssz,
                fixture.trusted,
                fixture.now,
                4,
                7,
            )
            .unwrap();
        let backend = Arc::new(Backend {
            payload_status: Mutex::new(PayloadStatusEnum::Valid),
            forkchoice_status: Mutex::new(PayloadStatusEnum::Valid),
            payload_calls: Mutex::new(Vec::new()),
            forkchoice_calls: Mutex::new(Vec::new()),
            expected_payload_hash: Mutex::new([0; 32]),
            callback_clock: Mutex::new(None),
        });
        let engine = RuntimeEngineAdapter::new(
            ExecutionProxy::new(BackendHandle(backend.clone()), Duration::from_millis(50)).unwrap(),
            fixture.config.clone(),
        )
        .unwrap();
        let setup = WorkerSetup {
            repository: repository(&directory, fixture.config.clone()),
            config: fixture.config,
            anchor: ImportAnchor {
                verified: proof.verified().clone(),
                state_ssz,
                block_ssz,
                block_format: CheckpointBlockFormat::Signed,
                max_age_epochs: 4,
                decode_limits,
                replay: Some(ReplayAnchor { proof, post_block_state_ssz }),
            },
            engine,
            classic_availability: MissingAvailability,
            gloas_availability: MissingAvailability,
            limits: ImportLimits::default(),
            runtime: Handle::current(),
            clock: OwnerClock::Controlled(Arc::new(AtomicU64::new(fixture.now))),
        };
        tokio::task::spawn_blocking(move || {
            let worker = Worker::new(setup, Ok(fixture.now)).unwrap();
            let actual = worker.graph.snapshot().unwrap();
            assert_eq!(actual.head.root, fixture.trusted.root);
            assert_eq!(actual.head_slot, 1);
            assert_eq!(actual.finalized_checkpoint, fixture.trusted);
            let status = worker
                .graph
                .authenticated_status(&worker.config, worker.earliest_available_slot)
                .unwrap();
            assert_eq!(status.snapshot(), actual);
            assert_eq!(status.current_slot(), 16);
            let origin = worker.repository.block_state(fixture.trusted.root).unwrap().unwrap();
            assert_eq!(origin.state().slot(), 1);
            assert_eq!(origin.state().schema_fork(), Fork::Phase0);
            let recovered =
                worker.repository.recover_checkpoint(&verifier, fixture.now, 4).unwrap().unwrap();
            assert_eq!(recovered.state.state().slot(), 8);
            assert_eq!(
                recovered.state.state().schema_fork(),
                if upgrade { Fork::Altair } else { Fork::Phase0 }
            );
            let anchor = recovered.into_import_anchor(4, decode_limits).unwrap();
            assert!(anchor.replay.as_ref().unwrap().proof.matches_inputs(
                &anchor.state_ssz,
                &anchor.replay.as_ref().unwrap().post_block_state_ssz,
                &anchor.block_ssz,
                anchor.block_format,
            ));
            assert!(backend.payload_calls.lock().unwrap().is_empty());
            assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_recovery_replays_real_signed_input_before_one_canonical_commit() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (initial, _) = setup(&directory, &store, &parameters);
    let anchor = initial.anchor.verified.clone();
    let config = initial.config.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    let root = wire.root();
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        let unsigned = worker
            .observe(ObservationRequest::Block { id: ObservationId::Head, header_only: false })
            .unwrap();
        assert!(matches!(unsigned, Observation::Object(None)));
        let Observation::Object(Some(seed_state)) =
            worker.observe(ObservationRequest::StateRoot(ObservationId::Head)).unwrap()
        else {
            panic!("actual unsigned seed state commitment")
        };
        assert_eq!(seed_state.header.state_root, anchor.state_root());
        assert!(seed_state.canonical && seed_state.finalized);

        worker.advance(config.seconds_per_slot * 1000).unwrap();
        worker.import(Input::Block(wire), 0).unwrap();
        let Observation::Object(Some(original)) = worker
            .observe(ObservationRequest::Block { id: ObservationId::Head, header_only: false })
            .unwrap()
        else {
            panic!("actual signed child")
        };
        let json: serde_json::Value = serde_json::from_slice(&original.json).unwrap();
        let signature = format!(
            "0x{}",
            signed.signature.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        );
        assert_eq!(json["signature"], signature);
        assert_eq!(original.block_root, root);
        assert!(original.canonical);
        let Observation::Object(Some(header)) = worker
            .observe(ObservationRequest::Block { id: ObservationId::Head, header_only: true })
            .unwrap()
        else {
            panic!("actual signed header")
        };
        let header_json: serde_json::Value = serde_json::from_slice(&header.json).unwrap();
        assert_eq!(header_json["signature"], signature);
        assert_eq!(header.header, original.header);
        for id in [ObservationId::Slot(2), ObservationId::Root([252; 32])] {
            assert!(matches!(
                worker.observe(ObservationRequest::Block { id, header_only: false }).unwrap(),
                Observation::Object(None)
            ));
        }

        drop(worker);
        let (mut restored, backend) = setup(&directory, &store, &parameters);
        restored.runtime = runtime;
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &config,
            anchor.checkpoint().root,
            RecoveryLimits::default(),
        )
        .unwrap();
        assert_eq!(plan.blocks.len(), 1);
        let mut worker =
            Worker::initialize(restored, Ok(config.seconds_per_slot * 1000), true, None).unwrap();
        assert_ne!(worker.graph.snapshot().unwrap().head.root, root);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head().root, root);
        let replay = plan.load(&worker.repository, &worker.config, &plan.blocks[0]).unwrap();
        let transcript = plan.transcript.as_ref().expect("original ordered owner observations");
        for sequence in &transcript.sequences {
            worker.replay_original_event(*sequence).unwrap();
        }
        assert_eq!(worker.graph.time_ms(), transcript.time_ms);
        assert_eq!(recovery::state_root(&worker.graph, root).unwrap(), replay.state_root);
        plan.check(&worker.graph).unwrap();
        worker.replaying = false;
        let mut graph = worker.graph.clone();
        assert!(!worker.publish(&mut graph).unwrap());
        assert_eq!(worker.graph.snapshot().unwrap().head.root, root);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

pub(crate) fn genuine_empty_gloas_child(
    store: &CompactGloasStore,
    parent: Root,
    keys: &[SecretKey],
    transitions: &TransitionParameters,
) -> gloas::SignedBeaconBlock {
    let parameters = &transitions.gloas;
    let mut state = (*store.read_block_state(parent).unwrap().unwrap()).clone();
    let slot = state.slot + 1;
    gloas_processing::process_slots(&mut state, slot, parameters).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, parameters).unwrap();
    let key = &keys[proposer as usize];
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: state.latest_block_hash,
        parent_block_root: parent,
        block_hash: [29; 32],
        prev_randao: state.randao_mixes.values[(slot / 8 % 64) as usize],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: gloas::ExecutionRequests {
            deposits: ProgressiveList::default(),
            withdrawals: ProgressiveList::default(),
            consolidations: ProgressiveList::default(),
            builder_deposits: ProgressiveList::default(),
            builder_exits: ProgressiveList::default(),
        }
        .hash_tree_root(Preset::Minimal)
        .unwrap(),
    };
    body.randao_reveal = key
        .sign(
            &signing_root(
                uint64_root(slot / 8),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut block = gloas::BeaconBlock {
        slot,
        proposer_index: proposer,
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    gloas_processing::process_parent_execution_payload(&mut state, &block, parameters).unwrap();
    gloas_processing::process_block_header(&mut state, &block, parameters).unwrap();
    gloas_processing::process_withdrawals(&mut state, parameters).unwrap();
    gloas_processing::process_execution_payload_bid(
        &mut state,
        &block.body.signed_execution_payload_bid,
        parameters,
    )
    .unwrap();
    gloas_processing::process_randao(&mut state, &block.body, parameters).unwrap();
    gloas_processing::process_eth1_data(&mut state, &block.body, parameters).unwrap();
    gloas_processing::process_operations(&mut state, &block.body, parent_slot, parameters).unwrap();
    gloas_processing::process_sync_aggregate(&mut state, &block.body.sync_aggregate, parameters)
        .unwrap();
    block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = key
        .sign(
            &signing_root(
                block.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    gloas::SignedBeaconBlock { message: block, signature }
}

mod gloas_gossip;
mod gossip;
mod serving;

mod publication;

mod slashings;

mod columns;

mod slashing_cut;

mod custody_history;
mod custody_pruned;

mod custody_positive;

mod optimistic;

mod recovery_clock;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_fcr_keeps_verified_finalized_safety_and_strict_imports() {
    assert!(!ImportLimits::default().fast_confirmation);
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (mut setup, backend) = setup(&directory, &store, &parameters);
    setup.limits.fast_confirmation = false;
    let config = setup.config.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        let admitted = worker.import(Input::Block(wire.clone()), 0).unwrap();
        assert_eq!(admitted.snapshot.head.root, wire.root());
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        assert_eq!(backend.payload_calls.lock().unwrap().len(), 1);
        for (_, state) in backend.forkchoice_calls.lock().unwrap().iter() {
            assert_eq!(state.safe_block_hash, B256::from([1; 32]));
            assert_eq!(state.finalized_block_hash, B256::from([1; 32]));
        }
        assert!(!worker.engine.is_closed());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fcr_suspends_when_the_consensus_head_falls_behind_punctual_observations() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = post_merge_anchor();
    let (mut setup, _) = setup(&directory, &store, &parameters);
    setup.limits.fast_confirmation = true;
    let slot_ms = setup.config.seconds_per_slot * 1000;
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(slot_ms).unwrap();
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_some());
        // A punctual timer cannot substitute for a recent authenticated consensus head.
        worker.advance(slot_ms * 2).unwrap();
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        assert!(!worker.engine.is_closed());
    })
    .await
    .unwrap();
}

mod fast_confirmation;
