//! Real published signed-input regressions for execution optimism isolation.

use super::*;
use reth_embedded_beacon_state_transition::{
    fork_processing::ForkActivation, gloas_processing::GloasParameters, ssz::Preset,
};
use std::{
    io,
    task::{Context, Poll, Waker},
};

#[derive(Clone, Copy)]
enum Mode {
    Immediate,
    Fail,
    Pending,
}
struct TestEngine {
    response: Mutex<PayloadResponse>,
    terminal: Mutex<TerminalResponse>,
    mode: Mutex<Mode>,
    calls: Mutex<Vec<(Root, PayloadContext)>>,
}
impl TestEngine {
    fn new() -> Self {
        Self {
            response: Mutex::new(PayloadResponse {
                status: PayloadStatus::Syncing,
                latest_valid_hash: None,
                validation_error: None,
            }),
            terminal: Mutex::new(TerminalResponse::Verified),
            mode: Mutex::new(Mode::Immediate),
            calls: Mutex::default(),
        }
    }
    fn set_valid(&self, hash: Root) {
        *self.response.lock().unwrap() = PayloadResponse {
            status: PayloadStatus::Valid,
            latest_valid_hash: Some(hash),
            validation_error: None,
        };
    }
}
impl ExecutionEngine for TestEngine {
    type Error = io::Error;
    async fn verify_terminal_block(
        &self,
        _: Payload<'_>,
        _: PayloadContext,
    ) -> Result<TerminalResponse, Self::Error> {
        Ok(*self.terminal.lock().unwrap())
    }
    async fn notify_new_payload(
        &self,
        payload: Payload<'_>,
        context: PayloadContext,
    ) -> Result<PayloadResponse, Self::Error> {
        self.calls.lock().unwrap().push((payload.block_hash(), context));
        let mode = *self.mode.lock().unwrap();
        match mode {
            Mode::Immediate => Ok(self.response.lock().unwrap().clone()),
            Mode::Fail => Err(io::Error::other("unavailable execution transport")),
            Mode::Pending => std::future::pending().await,
        }
    }
}
struct NoBlobRequests;
impl DataAvailability for NoBlobRequests {
    type Error = io::Error;
    async fn verify(&self, _: Root, _: &BeaconBlock) -> Result<(), Self::Error> {
        Err(io::Error::other("Bellatrix must not request blob availability"))
    }
}
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("synchronous fixture callback unexpectedly pending"),
    }
}
fn published() -> (BeaconState, BeaconBlock, SignedBeaconBlock) {
    decode_published(
        include_bytes!("../../../testdata/official-optimistic-bellatrix/anchor_state.ssz"),
        include_bytes!("../../../testdata/official-optimistic-bellatrix/anchor_block.ssz"),
        include_bytes!("../../../testdata/official-optimistic-bellatrix/first_block.ssz"),
    )
}
fn decode_published(
    state: &[u8],
    block: &[u8],
    signed: &[u8],
) -> (BeaconState, BeaconBlock, SignedBeaconBlock) {
    (
        BeaconState::from_ssz(state, Fork::Bellatrix, Preset::Minimal).unwrap(),
        BeaconBlock::from_ssz(block, Fork::Bellatrix, Preset::Minimal).unwrap(),
        SignedBeaconBlock::from_ssz(signed, Fork::Bellatrix, Preset::Minimal).unwrap(),
    )
}
fn parameters() -> TransitionParameters {
    let mut gloas = GloasParameters::for_preset(Preset::Minimal);
    gloas.fulu.blob_schedule.clear();
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
            epoch: if fork <= Fork::Bellatrix { 0 } else { u64::MAX },
            version: [index as u8, 0, 0, 1],
        })
        .collect();
    TransitionParameters::new(gloas, schedule).unwrap()
}
fn follower(engine: Arc<TestEngine>) -> (OptimisticStore<TestEngine>, SignedBeaconBlock) {
    let (state, block, signed) = published();
    let mut store = OptimisticStore::get_store(
        state,
        block,
        StoreParameters::for_preset(Preset::Minimal),
        parameters(),
        OptimisticParameters::default(),
        engine,
    )
    .unwrap();
    store.on_tick(7728 * 1000).unwrap();
    (store, signed)
}
fn first_merge_follower(
    engine: Arc<TestEngine>,
) -> (OptimisticStore<TestEngine>, SignedBeaconBlock) {
    let (state, block, signed) = decode_published(
        include_bytes!("../../../testdata/official-first-merge-bellatrix/anchor_state.ssz"),
        include_bytes!("../../../testdata/official-first-merge-bellatrix/anchor_block.ssz"),
        include_bytes!("../../../testdata/official-first-merge-bellatrix/first_merge_block.ssz"),
    );
    let BeaconState::Bellatrix(pre, _) = &state else {
        panic!("actual published Bellatrix merge anchor");
    };
    assert!(!bellatrix_processing::is_merge_transition_complete(pre));
    let SignedBeaconBlock::Bellatrix(first, _) = &signed else {
        panic!("actual published signed Bellatrix merge child");
    };
    assert_ne!(
        first.message.body.execution_payload,
        bellatrix::ExecutionPayload::default_value(Preset::Minimal).unwrap()
    );
    assert_eq!(first.message.slot, 1);
    let optimistic = OptimisticParameters::default();
    let mut store = OptimisticStore::get_store(
        state,
        block,
        StoreParameters::for_preset(Preset::Minimal),
        parameters(),
        optimistic,
        engine,
    )
    .unwrap();
    // The unchanged first-merge block becomes eligible by actual age. No retained state,
    // signed candidate or consensus check is altered to manufacture terminal deferral.
    let eligible_slot = signed
        .header()
        .unwrap()
        .slot
        .checked_add(optimistic.safe_slots_to_import_optimistically)
        .unwrap();
    let time = eligible_slot
        .checked_mul(store.consensus.slot_duration_ms())
        .unwrap()
        .checked_add(store.consensus.genesis_time_ms())
        .unwrap();
    store.on_tick(time).unwrap();
    (store, signed)
}
fn execution_hash(signed: &SignedBeaconBlock) -> Root {
    let SignedBeaconBlock::Bellatrix(value, _) = signed else { panic!("actual Bellatrix input") };
    value.message.body.execution_payload.block_hash
}
#[test]
fn genuine_signed_payload_can_defer_but_cannot_publish_execution_authority() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, signed) = follower(engine.clone());
    let root = signed.message_root().unwrap();
    assert_eq!(
        ready(store.on_block(&signed, &NoBlobRequests)).unwrap(),
        ImportOutcome::Accepted { execution_optimistic: true }
    );
    assert_eq!(store.head().unwrap().root, root);
    assert!(store.head().unwrap().execution_optimistic);
    assert!(matches!(store.execution_verified_head(), Err(OptimisticError::ExecutionDeferred)));
    let calls = engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, execution_hash(&signed));
    let PayloadContext::Bellatrix(context) = &calls[0].1 else {
        panic!("real version-one context")
    };
    assert_eq!(context.slot, 9);
    assert_eq!(context.parent_beacon_block_root, signed.header().unwrap().parent_root);
    drop(calls);
    engine.set_valid(execution_hash(&signed));
    ready(store.refresh_execution_status(root)).unwrap();
    assert!(!store.head().unwrap().execution_optimistic);
    assert_eq!(store.execution_verified_head().unwrap().root, root);
}

#[test]
fn deferred_consensus_import_reports_actual_phases_without_execution_authority() {
    for status in [PayloadStatus::Syncing, PayloadStatus::Accepted] {
        let engine = Arc::new(TestEngine::new());
        engine.response.lock().unwrap().status = status;
        let (mut store, signed) = follower(engine.clone());
        let root = signed.message_root().unwrap();
        let (outcome, timings) =
            ready(store.on_block_consensus_only_timed(&signed, &NoBlobRequests)).unwrap();
        assert_eq!(outcome, ImportOutcome::Accepted { execution_optimistic: true });
        assert_eq!(timings.availability_micros, 0);
        assert!(timings.state_transition_micros > 0);
        assert!(timings.execution_wait_micros.is_some());
        assert_eq!(timings.persistence_micros, 0);
        assert_eq!(engine.calls.lock().unwrap().len(), 1);
        assert_eq!(store.head().unwrap().root, root);
        assert!(store.head().unwrap().execution_optimistic);
        assert!(matches!(store.execution_verified_head(), Err(OptimisticError::ExecutionDeferred)));
        // An already admitted root does not produce another payload measurement or transition.
        let (_, duplicate) =
            ready(store.on_block_consensus_only_timed(&signed, &NoBlobRequests)).unwrap();
        assert_eq!(duplicate, BlockImportTimings::default());
        assert_eq!(engine.calls.lock().unwrap().len(), 1);
    }
}

#[test]
fn timed_consensus_rejection_preserves_graph_and_does_not_submit_payload() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, mut signed) = follower(engine.clone());
    let before = store.head().unwrap();
    let SignedBeaconBlock::Bellatrix(value, _) = &mut signed else {
        panic!("actual signed Bellatrix fixture")
    };
    value.signature[0] ^= 1;
    assert!(ready(store.on_block_consensus_only_timed(&signed, &NoBlobRequests)).is_err());
    assert_eq!(store.head().unwrap(), before);
    assert_eq!(store.statuses.len(), 1);
    assert!(engine.calls.lock().unwrap().is_empty());
}

#[test]
fn syncing_engine_cannot_admit_invalid_signature_or_post_state() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, signed) = follower(engine);
    let before = store.head().unwrap();
    for tamper_state in [false, true] {
        let mut altered = signed.clone();
        let SignedBeaconBlock::Bellatrix(value, _) = &mut altered else {
            panic!("real signed Bellatrix")
        };
        if tamper_state {
            value.message.state_root[0] ^= 1;
        } else {
            value.signature[0] ^= 1;
        }
        assert!(ready(store.on_block(&altered, &NoBlobRequests)).is_err());
        assert_eq!(store.head().unwrap(), before);
        assert_eq!(store.statuses.len(), 1);
    }
}
#[test]
fn unavailable_and_cancelled_callbacks_do_not_modify_any_graph_or_status() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, signed) = follower(engine.clone());
    let before = store.head().unwrap();
    *engine.mode.lock().unwrap() = Mode::Fail;
    assert!(ready(store.on_block(&signed, &NoBlobRequests)).is_err());
    assert_eq!(store.head().unwrap(), before);
    *engine.mode.lock().unwrap() = Mode::Pending;
    {
        let mut future = std::pin::pin!(store.on_block(&signed, &NoBlobRequests));
        let mut context = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(store.head().unwrap(), before);
    assert_eq!(store.statuses.len(), 1);
    assert_eq!(store.consensus.blocks.len(), 1);
}
#[test]
fn contradictory_valid_feedback_halts_instead_of_publishing_an_invalid_head() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, signed) = follower(engine.clone());
    let root = signed.message_root().unwrap();
    engine.set_valid(execution_hash(&signed));
    ready(store.on_block(&signed, &NoBlobRequests)).unwrap();
    *engine.response.lock().unwrap() = PayloadResponse {
        status: PayloadStatus::Invalid,
        latest_valid_hash: None,
        validation_error: Some("contradictory".into()),
    };
    assert!(matches!(
        ready(store.refresh_execution_status(root)),
        Err(OptimisticError::EngineContradiction)
    ));
    assert!(store.is_halted());
    assert!(matches!(store.head(), Err(OptimisticError::Halted)));
    assert!(matches!(store.execution_verified_head(), Err(OptimisticError::Halted)));
}
#[test]
fn exact_hash_and_status_resource_bounds_reject_atomically() {
    let engine = Arc::new(TestEngine::new());
    let (mut store, signed) = follower(engine.clone());
    let before = store.head().unwrap();
    engine.set_valid([0xfe; 32]);
    assert!(ready(store.on_block(&signed, &NoBlobRequests)).is_err());
    assert_eq!(store.head().unwrap(), before);
    engine.set_valid(execution_hash(&signed));
    store.parameters.max_status_bytes = store.statuses.len() * size_of::<(Root, StatusRecord)>();
    assert!(matches!(
        ready(store.on_block(&signed, &NoBlobRequests)),
        Err(OptimisticError::ResourceLimit)
    ));
    assert_eq!(store.head().unwrap(), before);
}
#[test]
fn unknown_terminal_ancestry_cannot_promote_a_merge_block_to_valid() {
    let engine = Arc::new(TestEngine::new());
    *engine.terminal.lock().unwrap() = TerminalResponse::UnknownPowAncestry;
    let (mut store, signed) = first_merge_follower(engine.clone());
    let root = signed.message_root().unwrap();
    ready(store.on_block(&signed, &NoBlobRequests)).unwrap();
    assert!(store.statuses[&root].terminal_unverified);
    engine.set_valid(execution_hash(&signed));
    assert!(matches!(
        ready(store.refresh_execution_status(root)),
        Err(OptimisticError::TerminalEvidenceUnavailable)
    ));
    assert!(store.head().unwrap().execution_optimistic);
    assert!(matches!(store.execution_verified_head(), Err(OptimisticError::ExecutionDeferred)));
    *engine.terminal.lock().unwrap() = TerminalResponse::Verified;
    ready(store.refresh_execution_status(root)).unwrap();
    assert_eq!(store.execution_verified_head().unwrap().root, root);
}
