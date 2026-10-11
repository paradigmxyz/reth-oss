//! Concrete checkpoint authentication and genuine signed CL transition controls.
//! Engine/terminal callbacks are exact test doubles; these tests are not live EL/DA proof.

use super::*;
use crate::gossip_tests::{signing_root, store_and_keys};
use alloy_rpc_types_engine::{ForkchoiceUpdated, ForkchoiceUpdatedResponseV2, PayloadStatusEnum};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_config::{BlobParameters, ForkActivation, ForkSchedule};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_execution_proxy::{ExecutionProxy, PayloadVersion};
use reth_embedded_beacon_primitives::{uint64_root, Checkpoint};
use reth_embedded_beacon_state_transition::{
    fork_processing::{self, TransitionParameters},
    fulu_processing, gloas_processing,
    ssz::{gloas, List, ProgressiveList, Vector},
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
    sync::{atomic::AtomicUsize, Mutex},
    task::Poll,
};

type Call = (ForkchoiceVersion, ForkchoiceState, Option<[u8; 16]>);

#[derive(Debug)]
pub(crate) enum Reply {
    Valid,
    Syncing,
    SyncingHash,
    WrongHash,
    MissingHash,
    PayloadJob,
    Extended,
    Rejected,
    Error,
    Pending,
}
#[derive(Debug)]
pub(crate) struct Backend {
    replies: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<Call>>,
    entered: watch::Sender<usize>,
}
impl Backend {
    pub(crate) fn forkchoice_call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BackendHandle(Arc<Backend>);
impl ExecutionEngine for BackendHandle {
    type Payload = ExecutionData;
    type Forkchoice = ForkchoiceState;
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = io::Error;
    async fn new_payload(
        &self,
        _version: PayloadVersion,
        _data: ExecutionData,
    ) -> Result<PayloadReply, io::Error> {
        panic!("checkpoint download must never submit a fabricated payload")
    }
    async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, io::Error> {
        let calls = {
            let mut calls = self.0.calls.lock().unwrap();
            calls.push((version, state, custody));
            calls.len()
        };
        self.0.entered.send_replace(calls);
        let reply = self.0.replies.lock().unwrap().pop_front().unwrap_or(Reply::Syncing);
        match reply {
            Reply::Valid => Ok(ForkchoiceReply::V1(
                ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash(state.head_block_hash),
            )),
            Reply::Syncing => {
                Ok(ForkchoiceReply::V1(ForkchoiceUpdated::from_status(PayloadStatusEnum::Syncing)))
            }
            Reply::SyncingHash => Ok(ForkchoiceReply::V1(
                ForkchoiceUpdated::from_status(PayloadStatusEnum::Syncing)
                    .with_latest_valid_hash(state.head_block_hash),
            )),
            Reply::WrongHash => Ok(ForkchoiceReply::V1(
                ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash([0xee; 32].into()),
            )),
            Reply::MissingHash => {
                Ok(ForkchoiceReply::V1(ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid)))
            }
            Reply::PayloadJob => Ok(ForkchoiceReply::V1(
                ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash(state.head_block_hash)
                    .with_payload_id(Default::default()),
            )),
            Reply::Extended => Ok(ForkchoiceReply::V2(
                ForkchoiceUpdatedResponseV2::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash(state.head_block_hash),
            )),
            Reply::Rejected => Ok(ForkchoiceReply::V1(ForkchoiceUpdated::new(PayloadStatus::new(
                PayloadStatusEnum::Invalid {
                    validation_error: "exact controlled rejection".into(),
                },
                Some(B256::ZERO),
            )))),
            Reply::Error => Err(io::Error::other("exact controlled transport failure")),
            Reply::Pending => std::future::pending().await,
        }
    }
}
pub(crate) fn adapter(
    config: ChainConfig,
    replies: Vec<Reply>,
    timeout: Duration,
) -> (RuntimeEngineAdapter<BackendHandle>, Arc<Backend>) {
    let (entered, _) = watch::channel(0);
    let backend = Arc::new(Backend {
        replies: Mutex::new(replies.into()),
        calls: Mutex::new(Vec::new()),
        entered,
    });
    let proxy = ExecutionProxy::new(BackendHandle(backend.clone()), timeout).unwrap();
    (RuntimeEngineAdapter::new(proxy, config).unwrap(), backend)
}
fn config(state: &BeaconState, parameters: &TransitionParameters) -> ChainConfig {
    ChainConfig {
        name: "execution-checkpoint-control".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 321,
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
pub(crate) fn limits() -> BootstrapLimits {
    BootstrapLimits {
        max_attempts: 3,
        total_timeout: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    }
}
pub(crate) fn anchor(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &TransitionParameters,
) -> (ChainConfig, ImportAnchor, u64) {
    let config = config(state, parameters);
    let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
    let block_ssz = Arc::<[u8]>::from(block.to_ssz().unwrap());
    let root = block.message_root().unwrap();
    let now = config.clock().unwrap().start_of_slot(state.slot()).unwrap();
    let decode_limits = crate::BootstrapDecodeLimits::default();
    let verified =
        SszCheckpointVerifier::new(config.clone(), decode_limits, CheckpointBlockFormat::Signed)
            .unwrap()
            .verify_checkpoint(
                &state_ssz,
                &block_ssz,
                Checkpoint { epoch: state.slot() / config.slots_per_epoch, root },
                now,
                2,
            )
            .unwrap();
    (
        config,
        ImportAnchor {
            verified,
            replay: None,
            state_ssz,
            block_ssz,
            block_format: CheckpointBlockFormat::Signed,
            max_age_epochs: 2,
            decode_limits,
        },
        now,
    )
}

/// Exact controlled first-Fulu callback contract, without pretending to calculate an EL block.
struct FuluContract {
    payload: fulu::ExecutionPayload,
    parent_root: Root,
    slot: u64,
    terminal_calls: AtomicUsize,
    payload_calls: AtomicUsize,
}
impl FuluContract {
    fn check(&self, payload: &fulu::ExecutionPayload, context: &fulu_processing::ExecutionContext) {
        assert_eq!(payload, &self.payload);
        assert_eq!(context.parent_beacon_block_root, self.parent_root);
        assert_eq!(context.slot, self.slot);
        assert_eq!(context.previous_execution_payload_hash, None);
        assert!(context.expected_blob_versioned_hashes.is_empty());
        assert!(context.execution_requests_list.is_empty());
    }
}
impl fulu_processing::ExecutionEngine for FuluContract {
    type Error = io::Error;
    async fn verify_terminal_block(
        &self,
        payload: &fulu::ExecutionPayload,
        context: fulu_processing::ExecutionContext,
    ) -> Result<(), io::Error> {
        self.check(payload, &context);
        assert_eq!(self.payload_calls.load(Ordering::SeqCst), 0);
        self.terminal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn verify_payload(
        &self,
        payload: &fulu::ExecutionPayload,
        context: fulu_processing::ExecutionContext,
    ) -> Result<(), io::Error> {
        self.check(payload, &context);
        assert_eq!(self.terminal_calls.load(Ordering::SeqCst), 1);
        self.payload_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) async fn genuine_fulu(
) -> (BeaconState, SignedBeaconBlock, Vec<SecretKey>, TransitionParameters) {
    let (store, keys, parameters) = store_and_keys(Fork::Fulu);
    let parent = store.get_head().unwrap().root;
    let pre = store.block_state(parent).unwrap();
    let target = 8;
    let BeaconState::Fulu(mut state, _) =
        fork_processing::replay_slots(pre, target, &parameters, target).unwrap()
    else {
        panic!("real Fulu checkpoint slot");
    };
    let processing = &parameters.gloas.fulu;
    let proposer = fulu_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let mut body = fulu::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    body.sync_aggregate.sync_committee_signature = infinity;
    body.execution_payload = fulu::ExecutionPayload {
        parent_hash: state.latest_execution_payload_header.block_hash,
        fee_recipient: [8; 20],
        state_root: [1; 32],
        receipts_root: [2; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: state.randao_mixes.values[(target / 8 % 64) as usize],
        block_number: 1,
        gas_limit: 30_000_000,
        gas_used: 0,
        timestamp: state.genesis_time + target * 6,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [41; 32],
        transactions: List::default(),
        withdrawals: List::default(),
        blob_gas_used: 0,
        excess_blob_gas: 0,
    };
    let key = &keys[proposer as usize];
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
    let mut message = fulu::BeaconBlock {
        slot: target,
        proposer_index: proposer,
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    fulu_processing::process_block_header(&mut state, &message, processing).unwrap();
    fulu_processing::process_withdrawals(&mut state, &message.body.execution_payload, processing)
        .unwrap();
    state.latest_execution_payload_header =
        fulu_processing::validate_execution_payload_consensus(&state, &message.body, processing)
            .unwrap();
    fulu_processing::process_randao(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_operations(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, processing)
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
    let signed = fulu::SignedBeaconBlock { message, signature };
    let contract = FuluContract {
        payload: signed.message.body.execution_payload.clone(),
        parent_root: parent,
        slot: target,
        terminal_calls: AtomicUsize::new(0),
        payload_calls: AtomicUsize::new(0),
    };
    let BeaconState::Fulu(mut actual, _) = pre.clone() else { unreachable!() };
    // Mandatory outer BLS, full signed body, real scheduled epoch work and state-root check.
    fulu_processing::state_transition(&mut actual, &signed, processing, &contract).await.unwrap();
    assert_eq!(actual, state);
    assert_eq!(contract.terminal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(contract.payload_calls.load(Ordering::SeqCst), 1);
    (
        BeaconState::Fulu(actual, Preset::Minimal),
        SignedBeaconBlock::Fulu(signed, Preset::Minimal),
        keys,
        parameters,
    )
}

async fn prepared() -> (ChainConfig, ImportAnchor, ExecutionBootstrap, BootstrapCancellation) {
    let (config, anchor, now) = tokio::task::spawn_blocking(|| {
        let (state, signed, _, parameters) =
            tokio::runtime::Handle::current().block_on(genuine_fulu());
        anchor(&state, &signed, &parameters)
    })
    .await
    .unwrap();
    let owned_config = config.clone();
    let owned_anchor = anchor.clone();
    let (owner, cancel) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(owned_config, &owned_anchor, now, limits())
    })
    .await
    .unwrap()
    .unwrap();
    (config, anchor, owner, cancel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syncing_then_exact_valid_reconciles_only_original_execution_target() {
    let (config, anchor, mut owner, _) = prepared().await;
    let (engine, backend) =
        adapter(config.clone(), vec![Reply::Syncing, Reply::Valid], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    assert_eq!(proof.checkpoint().checkpoint(), anchor.verified.checkpoint());
    assert_eq!(proof.config_digest(), checked_config_digest(&config).unwrap());
    assert_eq!(proof.execution_hash(), Some([41; 32]));
    assert_eq!(proof.attempts(), 2);
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for &(version, state, custody) in calls.iter() {
        assert_eq!(version, ForkchoiceVersion::V3);
        assert_eq!(state.head_block_hash, B256::from([41; 32]));
        assert_eq!(state.safe_block_hash, B256::ZERO);
        assert_eq!(state.finalized_block_hash, B256::ZERO);
        assert_eq!(custody, None);
    }
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syncing_never_creates_a_success_receipt_and_attempts_are_bounded() {
    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, Vec::new(), Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Attempts)));
    assert_eq!(backend.calls.lock().unwrap().len(), 3);
    assert!(!owner.completed);
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::AlreadyFinished)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_hash_job_and_version_close_real_shared_admission() {
    for reply in [
        Reply::WrongHash,
        Reply::MissingHash,
        Reply::SyncingHash,
        Reply::PayloadJob,
        Reply::Extended,
    ] {
        let (config, _, mut owner, _) = prepared().await;
        let (engine, backend) = adapter(config, vec![reply], Duration::from_secs(1));
        assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Malformed)));
        assert!(owner.requires_reconciliation());
        assert!(engine.is_closed());
        assert!(!owner.completed);
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
        assert!(matches!(
            owner.reconcile(&engine).await,
            Err(BootstrapRunError::ReconciliationRequired)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_transport_error_and_deterministic_rejection_remain_distinct() {
    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Error], Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await,
        Err(BootstrapRunError::Engine(EngineVerificationError::Proxy(ProxyError::Engine(error))))
        if error.to_string()=="exact controlled transport failure"));
    assert!(owner.requires_reconciliation());
    assert!(!engine.is_closed());
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Rejected], Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await,
        Err(BootstrapRunError::Rejected(PayloadStatus {
            status: PayloadStatusEnum::Invalid { validation_error }, latest_valid_hash: Some(hash)
        })) if validation_error=="exact controlled rejection" && hash==B256::ZERO));
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_cancelled_work_never_dispatches_but_active_cancellation_is_uncertain() {
    let (config, _, mut owner, cancel) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(10));
    cancel.cancel();
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Cancelled)));
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());

    let (config, _, mut owner, cancel) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(10));
    let mut entered = backend.entered.subscribe();
    let task = tokio::spawn(async move {
        let result = owner.reconcile(&engine).await;
        (result, owner.requires_reconciliation(), engine.is_closed())
    });
    tokio::time::timeout(Duration::from_secs(1), entered.wait_for(|count| *count > 0))
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
    let (result, uncertain, closed) = task.await.unwrap();
    assert!(matches!(result, Err(BootstrapRunError::Cancelled)));
    assert!(uncertain);
    assert!(closed);
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_active_future_and_actual_proxy_timeout_cannot_reopen() {
    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(10));
    let mut entered = backend.entered.subscribe();
    let engine = Arc::new(engine);
    let active_engine = engine.clone();
    let task = tokio::spawn(async move { owner.reconcile(&active_engine).await });
    tokio::time::timeout(Duration::from_secs(1), entered.wait_for(|count| *count > 0))
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(engine.is_closed());

    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_millis(10));
    assert!(matches!(
        owner.reconcile(&engine).await,
        Err(BootstrapRunError::Engine(EngineVerificationError::Proxy(ProxyError::Timeout)))
    ));
    assert!(owner.requires_reconciliation());
    assert!(engine.is_closed());
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn total_deadline_stops_syncing_without_inventing_authority_or_closing_known_idle_proxy() {
    let (config, _, mut owner, _) = prepared().await;
    owner.limits.total_timeout = Duration::from_millis(10);
    owner.started = Instant::now();
    owner.limits.initial_backoff = Duration::from_secs(1);
    let (engine, backend) = adapter(config, Vec::new(), Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Deadline)));
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
    assert!(!owner.completed);
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concrete_binding_original_bytes_and_adapter_configuration_cannot_be_tampered() {
    let (config, anchor, _, _) = prepared().await;
    let mut other = config.clone();
    other.chain_id += 1;
    let auth_config = config.clone();
    let auth_other = other.clone();
    tokio::task::spawn_blocking(move || {
        assert!(matches!(
            ExecutionBootstrap::authenticate(auth_other, &anchor, 48_000, limits()),
            Err(BootstrapPrepareError::Binding)
        ));
        let mut tampered = anchor.clone();
        let mut bytes = tampered.state_ssz.to_vec();
        bytes[0] ^= 1;
        tampered.state_ssz = bytes.into();
        assert!(matches!(
            ExecutionBootstrap::authenticate(auth_config, &tampered, 48_000, limits()),
            Err(BootstrapPrepareError::Bootstrap(_))
        ));
    })
    .await
    .unwrap();
    let (_, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(other, vec![Reply::Valid], Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Binding)));
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn weak_subjectivity_expiration_during_download_cannot_issue_a_receipt() {
    let (config, _, mut owner, _) = prepared().await;
    owner.expires_unix_ms = owner.now_unix_ms;
    let (engine, backend) = adapter(config, vec![Reply::Valid], Duration::from_secs(1));
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Deadline)));
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!owner.completed);
}

#[test]
fn fork_methods_and_retry_bounds_are_exact() {
    for (fork, version) in [
        (Fork::Phase0, ForkchoiceVersion::V1),
        (Fork::Altair, ForkchoiceVersion::V1),
        (Fork::Bellatrix, ForkchoiceVersion::V1),
        (Fork::Capella, ForkchoiceVersion::V2),
        (Fork::Deneb, ForkchoiceVersion::V3),
        (Fork::Electra, ForkchoiceVersion::V3),
        (Fork::Fulu, ForkchoiceVersion::V3),
        (Fork::Gloas, ForkchoiceVersion::V4),
    ] {
        assert_eq!(method(fork), version);
    }
    for bad in [
        BootstrapLimits { max_attempts: 0, ..limits() },
        BootstrapLimits { max_attempts: 257, ..limits() },
        BootstrapLimits { total_timeout: Duration::ZERO, ..limits() },
        BootstrapLimits { total_timeout: Duration::from_secs(3601), ..limits() },
        BootstrapLimits { initial_backoff: Duration::ZERO, ..limits() },
        BootstrapLimits { max_backoff: Duration::from_secs(61), ..limits() },
    ] {
        assert!(matches!(bad.validate(), Err(BootstrapPrepareError::Bounds)));
    }
}

pub(crate) async fn genuine_gloas() -> (BeaconState, SignedBeaconBlock, TransitionParameters) {
    let (fulu_state, fulu_signed, keys, original) = genuine_fulu().await;
    let mut schedule = original.schedule.clone();
    schedule.last_mut().unwrap().epoch = 2;
    let parameters = TransitionParameters::new(original.gloas, schedule).unwrap();
    let target = 16;
    let BeaconState::Gloas(mut state, _) =
        fork_processing::replay_slots(&fulu_state, target, &parameters, 8).unwrap()
    else {
        panic!("genuine scheduled Fulu to Gloas checkpoint");
    };
    let pre = state.clone();
    let processing = &parameters.gloas;
    let proposer = gloas_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[proposer as usize];
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    let requests = gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    };
    let parent = fulu_signed.message_root().unwrap();
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: state.latest_block_hash,
        parent_block_root: parent,
        block_hash: [44; 32],
        prev_randao: state.randao_mixes.values[(target / 8 % 64) as usize],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot: target,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests.hash_tree_root(Preset::Minimal).unwrap(),
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
        parent_root: parent,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    gloas_processing::process_parent_execution_payload(&mut state, &message, processing).unwrap();
    gloas_processing::process_block_header(&mut state, &message, processing).unwrap();
    gloas_processing::process_withdrawals(&mut state, processing).unwrap();
    gloas_processing::process_execution_payload_bid(
        &mut state,
        &message.body.signed_execution_payload_bid,
        processing,
    )
    .unwrap();
    gloas_processing::process_randao(&mut state, &message.body, processing).unwrap();
    gloas_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    gloas_processing::process_operations(&mut state, &message.body, parent_slot, processing)
        .unwrap();
    gloas_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, processing)
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
    let signed = gloas::SignedBeaconBlock { message, signature };
    let mut actual = pre;
    gloas_processing::state_transition(&mut actual, &signed, processing).unwrap();
    assert_eq!(actual, state);
    assert_eq!(state.latest_block_hash, [41; 32]);
    assert_eq!(state.latest_execution_payload_bid.block_hash, [44; 32]);
    (
        BeaconState::Gloas(actual, Preset::Minimal),
        SignedBeaconBlock::Gloas(signed, Preset::Minimal),
        parameters,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_gloas_downloads_only_the_finalized_parent_association_not_pending_bid() {
    let (config, anchor, now) = tokio::task::spawn_blocking(|| {
        let (state, signed, parameters) =
            tokio::runtime::Handle::current().block_on(genuine_gloas());
        anchor(&state, &signed, &parameters)
    })
    .await
    .unwrap();
    let auth_config = config.clone();
    let (mut owner, _) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(auth_config, &anchor, now, limits())
    })
    .await
    .unwrap()
    .unwrap();
    let (engine, backend) =
        adapter(config, vec![Reply::Syncing, Reply::Valid], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    assert_eq!(proof.execution_hash(), Some([41; 32]));
    assert_eq!(proof.attempts(), 2);
    let calls = backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for &(version, state, custody) in calls.iter() {
        assert_eq!(version, ForkchoiceVersion::V4);
        assert_eq!(state.head_block_hash, B256::from([41; 32]));
        assert_ne!(state.head_block_hash, B256::from([44; 32]));
        assert_eq!(state.safe_block_hash, B256::ZERO);
        assert_eq!(state.finalized_block_hash, B256::ZERO);
        assert_eq!(custody, None);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_pre_execution_checkpoint_needs_no_engine_download_or_job() {
    let (config, anchor, now) = tokio::task::spawn_blocking(|| {
        let (store, _, parameters) = store_and_keys(Fork::Phase0);
        let root = store.get_head().unwrap().root;
        let state = store.block_state(root).unwrap();
        let block = store.block(root).unwrap();
        let config = config(state, &parameters);
        let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
        let block_ssz = Arc::<[u8]>::from(block.to_ssz().unwrap());
        let decode_limits = crate::BootstrapDecodeLimits::default();
        let verified = SszCheckpointVerifier::new(
            config.clone(),
            decode_limits,
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap()
        .verify_checkpoint(&state_ssz, &block_ssz, Checkpoint { epoch: 0, root }, 0, 2)
        .unwrap();
        (
            config,
            ImportAnchor {
                verified,
                replay: None,
                state_ssz,
                block_ssz,
                block_format: CheckpointBlockFormat::Unsigned,
                max_age_epochs: 2,
                decode_limits,
            },
            0,
        )
    })
    .await
    .unwrap();
    let auth_config = config.clone();
    let (mut owner, _) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(auth_config, &anchor, now, limits())
    })
    .await
    .unwrap()
    .unwrap();
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    assert_eq!(proof.execution_hash(), None);
    assert_eq!(proof.attempts(), 0);
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_existing_proxy_queue_returns_original_saturation_without_new_unbounded_waiters() {
    let (config, _, mut owner, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(30));
    let state = ForkchoiceState {
        head_block_hash: B256::from([41; 32]),
        safe_block_hash: B256::ZERO,
        finalized_block_hash: B256::ZERO,
    };
    let mut pending = Vec::new();
    // Poll exactly the proxy's 128 admitted queue entries; only the serialized first call
    // reaches the backend. The bootstrap owner adds no waiting task after saturation.
    for _ in 0..128 {
        let mut future =
            Box::pin(engine.bootstrap_forkchoice_updated(ForkchoiceVersion::V3, state));
        std::future::poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        pending.push(future);
    }
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
    assert!(matches!(
        owner.reconcile(&engine).await,
        Err(BootstrapRunError::Engine(EngineVerificationError::Proxy(ProxyError::QueueFull)))
    ));
    assert_eq!(owner.attempts, 1);
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
    drop(pending);
    assert!(engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_total_deadline_marks_unknown_outcome_and_closes_the_captured_proxy() {
    let (config, _, mut owner, _) = prepared().await;
    owner.limits.total_timeout = Duration::from_millis(10);
    owner.started = Instant::now();
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(30));
    assert!(matches!(owner.reconcile(&engine).await, Err(BootstrapRunError::Deadline)));
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
    assert!(owner.requires_reconciliation());
    assert!(engine.is_closed());
    assert!(!owner.completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_receipt_binds_exact_original_bytes_and_the_same_captured_adapter() {
    let (config, anchor, mut owner, _) = prepared().await;
    let (engine, _) = adapter(config.clone(), vec![Reply::Valid], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    let (other, _) = adapter(config.clone(), vec![Reply::Valid], Duration::from_secs(1));
    tokio::task::spawn_blocking(move || {
        proof.check_binding(&config, &anchor, &engine).unwrap();
        assert!(matches!(
            proof.check_binding(&config, &anchor, &other),
            Err(BootstrapProofError::Binding)
        ));
        let mut tampered = anchor.clone();
        let mut bytes = tampered.block_ssz.to_vec();
        // A different signature leaves the unsigned root unchanged, but this receipt binds
        // the exact original checkpoint container bytes instead of only its message root.
        bytes[4] ^= 1;
        tampered.block_ssz = bytes.into();
        assert!(matches!(
            proof.check_binding(&config, &tampered, &engine),
            Err(BootstrapProofError::Binding)
        ));
        engine.close_bootstrap_admission();
        assert!(matches!(
            proof.check_binding(&config, &anchor, &engine),
            Err(BootstrapProofError::ReconciliationRequired)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_download_hook_rejects_zero_finality_and_unaudited_method_context_before_dispatch() {
    let (config, _, _, _) = prepared().await;
    let (engine, backend) = adapter(config, vec![Reply::Pending], Duration::from_secs(1));
    let state = ForkchoiceState {
        head_block_hash: B256::from([41; 32]),
        safe_block_hash: B256::ZERO,
        finalized_block_hash: B256::ZERO,
    };
    for (version, state) in [
        (ForkchoiceVersion::V5, state),
        (ForkchoiceVersion::V3, ForkchoiceState { head_block_hash: B256::ZERO, ..state }),
        (ForkchoiceVersion::V3, ForkchoiceState { safe_block_hash: B256::from([41; 32]), ..state }),
        (
            ForkchoiceVersion::V4,
            ForkchoiceState { finalized_block_hash: B256::from([41; 32]), ..state },
        ),
    ] {
        assert!(matches!(
            engine.bootstrap_forkchoice_updated(version, state).await,
            Err(EngineVerificationError::Configuration)
        ));
    }
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_gloas_boundary_keeps_original_fulu_target_schema_and_current_v4_method() {
    let (config, anchor, now) = tokio::task::spawn_blocking(|| {
        let (post, signed, _, original) =
            tokio::runtime::Handle::current().block_on(genuine_fulu());
        let mut schedule = original.schedule.clone();
        schedule.last_mut().unwrap().epoch = 2;
        let parameters = TransitionParameters::new(original.gloas, schedule).unwrap();
        let boundary = fork_processing::replay_slots(&post, 16, &parameters, 8).unwrap();
        assert_eq!(post.schema_fork(), Fork::Fulu);
        assert_eq!(boundary.schema_fork(), Fork::Gloas);
        let config = config(&boundary, &parameters);
        let state_ssz = Arc::<[u8]>::from(boundary.to_ssz().unwrap());
        let post_block_state_ssz = Arc::<[u8]>::from(post.to_ssz().unwrap());
        let block_ssz = Arc::<[u8]>::from(signed.to_ssz().unwrap());
        let decode_limits = crate::BootstrapDecodeLimits::default();
        let now = 96_000;
        let proof = SszCheckpointVerifier::new(
            config.clone(),
            decode_limits,
            CheckpointBlockFormat::Signed,
        )
        .unwrap()
        .verify_checkpoint_replay(
            &state_ssz,
            &post_block_state_ssz,
            &block_ssz,
            Checkpoint { epoch: 2, root: signed.message_root().unwrap() },
            now,
            2,
            8,
        )
        .unwrap();
        let verified = proof.verified().clone();
        (
            config,
            ImportAnchor {
                verified,
                replay: Some(crate::import_coordinator::ReplayAnchor {
                    proof,
                    post_block_state_ssz,
                }),
                state_ssz,
                block_ssz,
                block_format: CheckpointBlockFormat::Signed,
                max_age_epochs: 2,
                decode_limits,
            },
            now,
        )
    })
    .await
    .unwrap();
    let auth_config = config.clone();
    let auth_anchor = anchor.clone();
    let (mut owner, _) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(auth_config, &auth_anchor, now, limits())
    })
    .await
    .unwrap()
    .unwrap();
    let (engine, backend) =
        adapter(config.clone(), vec![Reply::Syncing, Reply::Valid], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    assert_eq!(proof.execution_hash(), Some([41; 32]));
    assert_eq!(proof.checkpoint().slot(), 16);
    assert_eq!(backend.calls.lock().unwrap().len(), 2);
    for &(version, state, custody) in backend.calls.lock().unwrap().iter() {
        assert_eq!(version, ForkchoiceVersion::V4);
        assert_eq!(state.head_block_hash, B256::from([41; 32]));
        assert_eq!(state.safe_block_hash, B256::ZERO);
        assert_eq!(state.finalized_block_hash, B256::ZERO);
        assert_eq!(custody, None);
    }
    tokio::task::spawn_blocking(move || {
        proof.check_binding(&config, &anchor, &engine).unwrap();
        let mut tampered = anchor.clone();
        let replay = tampered.replay.as_mut().unwrap();
        let mut bytes = replay.post_block_state_ssz.to_vec();
        bytes[0] ^= 1;
        replay.post_block_state_ssz = bytes.into();
        assert!(matches!(
            ExecutionBootstrap::authenticate(config.clone(), &tampered, now, limits()),
            Err(BootstrapPrepareError::Binding)
        ));
        assert!(matches!(
            proof.check_binding(&config, &tampered, &engine),
            Err(BootstrapProofError::Binding)
        ));
        tampered = anchor.clone();
        tampered.replay = None;
        assert!(matches!(
            ExecutionBootstrap::authenticate(config, &tampered, now, limits()),
            Err(BootstrapPrepareError::Bootstrap(BootstrapError::UnverifiedBoundaryState))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_unused_cancellation_handle_does_not_cancel_bounded_download() {
    let (config, _, mut owner, cancellation) = prepared().await;
    drop(cancellation);
    let (engine, backend) =
        adapter(config, vec![Reply::Syncing, Reply::Valid], Duration::from_secs(1));
    let proof = owner.reconcile(&engine).await.unwrap();
    assert_eq!(proof.execution_hash(), Some([41; 32]));
    assert_eq!(proof.attempts(), 2);
    assert_eq!(backend.calls.lock().unwrap().len(), 2);
    assert!(!owner.requires_reconciliation());
    assert!(!engine.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_syncing_exhaustion_requires_fresh_authentication_before_same_adapter_valid() {
    let (config, anchor, mut first, _) = prepared().await;
    first.limits.max_attempts = 2;
    let (engine, backend) = adapter(
        config.clone(),
        vec![Reply::Syncing, Reply::Syncing, Reply::Valid],
        Duration::from_secs(1),
    );
    assert!(matches!(first.reconcile(&engine).await, Err(BootstrapRunError::Attempts)));
    assert_eq!(backend.calls.lock().unwrap().len(), 2);
    assert!(!first.requires_reconciliation());
    assert!(!engine.is_closed());
    let fresh_config = config.clone();
    let fresh_anchor = anchor.clone();
    let now = config.clock().unwrap().start_of_slot(anchor.verified.slot()).unwrap();
    let (mut fresh, _) = tokio::task::spawn_blocking(move || {
        ExecutionBootstrap::authenticate(fresh_config, &fresh_anchor, now, limits()).unwrap()
    })
    .await
    .unwrap();
    let receipt = fresh.reconcile(&engine).await.unwrap();
    assert_eq!(receipt.execution_hash(), Some([41; 32]));
    assert_eq!(backend.calls.lock().unwrap().len(), 3);
    assert!(!engine.is_closed());
    tokio::task::spawn_blocking(move || {
        receipt.check_binding(&config, &anchor, &engine).unwrap();
        let expired_slot =
            anchor.verified.slot() + (anchor.max_age_epochs + 1) * config.slots_per_epoch;
        let expired = config.clock().unwrap().start_of_slot(expired_slot).unwrap();
        assert!(matches!(
            ExecutionBootstrap::authenticate(config, &anchor, expired, limits()),
            Err(BootstrapPrepareError::Bootstrap(BootstrapError::StaleCheckpoint))
        ));
        assert_eq!(backend.calls.lock().unwrap().len(), 3);
    })
    .await
    .unwrap();
}
