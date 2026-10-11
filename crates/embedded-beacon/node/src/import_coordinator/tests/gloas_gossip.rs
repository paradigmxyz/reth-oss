//! Genuine Gloas gossip dispatch, strict prerequisite and payload authority regressions.
//! Controlled Engine replies establish policy only, never actual EL validation or live sync.

use super::*;
use crate::gossip_tests::gloas_store_and_keys;
use blst::min_pk::AggregateSignature;
use reth_embedded_beacon_fork_choice::{ForkChoiceNode, PayloadStatus as GloasPayloadStatus};
use reth_embedded_beacon_network::{
    gossip::{GossipKind, GossipTopic},
    transport::GossipVerdict,
};
use reth_embedded_beacon_primitives::{bytes_root, merkleize};
use reth_embedded_beacon_state_transition::ssz::{auxiliary, phase0};
use std::collections::BTreeSet;

pub(super) fn genuine_gloas_setup(
    directory: &TempDir,
) -> (
    WorkerSetup<BackendHandle, MissingAvailability, MissingAvailability>,
    Arc<Backend>,
    Vec<SecretKey>,
) {
    let (store, keys, parameters) = gloas_store_and_keys();
    let root = store.get_head().unwrap().root;
    let state = BeaconState::Gloas(store.block_state(root).unwrap().clone(), Preset::Minimal);
    let block =
        BeaconBlock::Gloas(store.block_arc(root).unwrap().as_ref().clone(), Preset::Minimal);
    let config = config(&state, &parameters);
    let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
    let block_ssz = Arc::<[u8]>::from(block.to_ssz().unwrap());
    let decode_limits = BootstrapDecodeLimits::default();
    let verifier =
        SszCheckpointVerifier::new(config.clone(), decode_limits, CheckpointBlockFormat::Unsigned)
            .unwrap();
    let verified = verifier
        .verify_checkpoint(&state_ssz, &block_ssz, Checkpoint { epoch: 0, root }, 0, 1)
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
            clock: OwnerClock::Controlled(Arc::new(AtomicU64::new(0))),
        },
        backend,
        keys,
    )
}

fn topic(config: &ChainConfig, kind: GossipKind, slot: u64) -> GossipTopic {
    GossipTopic {
        fork: Fork::Gloas,
        digest: config.fork_digest(slot / config.slots_per_epoch).unwrap(),
        kind,
    }
}

fn preferences(
    worker: &Worker<BackendHandle, MissingAvailability, MissingAvailability>,
    keys: &[SecretKey],
) -> auxiliary::gloas::SignedProposerPreferences {
    let Graph::Gloas { store, .. } = &worker.graph else { panic!("genuine Gloas owner") };
    let root = store.get_head().unwrap().root;
    let state = store.read_block_state(root).unwrap().unwrap();
    let validator_index = state.proposer_lookahead.values[1];
    let message = auxiliary::gloas::ProposerPreferences {
        dependent_root: root,
        proposal_slot: 1,
        validator_index,
        fee_recipient: [8; 20],
        target_gas_limit: 30_000_000,
    };
    let root = signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        [13, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let signature = keys[validator_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    auxiliary::gloas::SignedProposerPreferences { message, signature }
}

fn payload_message(
    worker: &Worker<BackendHandle, MissingAvailability, MissingAvailability>,
    keys: &[SecretKey],
) -> (auxiliary::gloas::PayloadAttestationMessage, Vec<u64>) {
    let Graph::Gloas { store, .. } = &worker.graph else { panic!("genuine Gloas owner") };
    let root = store.get_head().unwrap().root;
    let state = store.read_block_state(root).unwrap().unwrap();
    let committee = gloas_processing::get_ptc(&state, state.slot, store.processing_parameters())
        .unwrap()
        .values
        .clone();
    let validator_index = committee[0];
    let data = gloas::PayloadAttestationData {
        beacon_block_root: root,
        slot: state.slot,
        payload_present: true,
        blob_data_available: true,
    };
    let root = signing_root(
        data.hash_tree_root(Preset::Minimal).unwrap(),
        [12, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let signature = keys[validator_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    (auxiliary::gloas::PayloadAttestationMessage { validator_index, data, signature }, committee)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_preferences_signature_and_topic_precede_cache_commit() {
    let directory = TempDir::new().unwrap();
    let (setup, backend, keys) = genuine_gloas_setup(&directory);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let before = worker.graph.snapshot().unwrap();
        let signed = preferences(&worker, &keys);
        let topic = topic(&config, GossipKind::Preferences, 1);
        let mut wrong_topic = topic;
        wrong_topic.digest[0] ^= 1;
        assert_eq!(
            worker
                .dispatch_gossip(wrong_topic, &signed.to_ssz(Preset::Minimal).unwrap(), 0)
                .unwrap(),
            GossipVerdict::Reject
        );
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert_eq!(
            worker.dispatch_gossip(topic, &invalid.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Reject
        );
        assert_eq!(worker.graph.snapshot().unwrap(), before);
        assert_eq!(
            worker.dispatch_gossip(topic, &signed.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Accept
        );
        assert_eq!(
            worker.dispatch_gossip(topic, &signed.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Ignore
        );
        assert_eq!(worker.graph.snapshot().unwrap(), before);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), before.head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_ptc_signature_publishes_every_assigned_position_without_verifying_a_payload() {
    let directory = TempDir::new().unwrap();
    let (setup, backend, keys) = genuine_gloas_setup(&directory);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let (signed, committee) = payload_message(&worker, &keys);
        let root = signed.data.beacon_block_root;
        let before = worker.graph.snapshot().unwrap();
        let topic = topic(&config, GossipKind::PayloadAttestation, signed.data.slot);
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert_eq!(
            worker.dispatch_gossip(topic, &invalid.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Reject
        );
        {
            let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
            assert!(store.payload_timeliness_votes(root).unwrap().iter().all(Option::is_none));
            assert!(store
                .payload_data_availability_votes(root)
                .unwrap()
                .iter()
                .all(Option::is_none));
        }
        assert_eq!(
            worker.dispatch_gossip(topic, &signed.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Accept
        );
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let expected = committee
            .iter()
            .map(|&index| (index == signed.validator_index).then_some(true))
            .collect::<Vec<_>>();
        assert_eq!(store.payload_timeliness_votes(root).unwrap(), expected);
        assert_eq!(store.payload_data_availability_votes(root).unwrap(), expected);
        assert!(!store.is_payload_verified(root));
        assert!(store.payload(root).is_none());
        assert!(worker.repository.original_envelope(root).unwrap().is_none());
        assert_eq!(worker.graph.snapshot().unwrap().head, before.head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), before.head);
        assert_eq!(
            worker.dispatch_gossip(topic, &signed.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Ignore
        );
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bids_without_a_verified_parent_payload_do_not_borrow_ambient_engine_authority() {
    let directory = TempDir::new().unwrap();
    let (setup, backend, keys) = genuine_gloas_setup(&directory);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let before = worker.graph.snapshot().unwrap();
        let preferences = preferences(&worker, &keys);
        assert_eq!(
            worker
                .dispatch_gossip(
                    topic(&config, GossipKind::Preferences, 1),
                    &preferences.to_ssz(Preset::Minimal).unwrap(),
                    0
                )
                .unwrap(),
            GossipVerdict::Accept
        );
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let state = store.read_block_state(before.head.root).unwrap().unwrap();
        let mut message = state.latest_execution_payload_bid.clone();
        message.slot = 1;
        message.parent_block_root = before.head.root;
        message.parent_block_hash = state.latest_execution_payload_bid.parent_block_hash;
        message.block_hash = [55; 32];
        message.fee_recipient = preferences.message.fee_recipient;
        message.builder_index = 0;
        message.value = 1;
        let signature = keys[0]
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
        drop(state);
        let signed = gloas::SignedExecutionPayloadBid { message, signature };
        let bytes = signed.to_ssz(Preset::Minimal).unwrap();
        let topic = topic(&config, GossipKind::Bid, 1);
        assert_eq!(worker.dispatch_gossip(topic, &bytes, 0).unwrap(), GossipVerdict::Ignore);
        assert_eq!(worker.dispatch_gossip(topic, &bytes, 0).unwrap(), GossipVerdict::Ignore);
        assert_eq!(worker.graph.snapshot().unwrap(), before);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        // No external-builder acceptance claim: this source intentionally lacks its finalized
        // registry entry and verified signed parent envelope. Engine callbacks cannot replace them.
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_envelope_signature_and_failed_strict_availability_never_poison_a_retry() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters, signed) = phase0_gloas_boundary();
    let (setup, backend) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    let OwnerClock::Controlled(clock) = &setup.clock else { unreachable!() };
    let clock = clock.clone();
    let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000 * config.slots_per_epoch;
        clock.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        worker.import(Input::Block(wire.clone()), now).unwrap();
        let before = worker.graph.snapshot().unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let signed = original_gloas_envelope(store, wire.root(), &keys);
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        let invalid =
            WireEnvelope::from_ssz(invalid.to_ssz(Preset::Minimal).unwrap(), &wire, &config)
                .unwrap();
        assert!(matches!(
            worker.import(Input::GossipEnvelope(invalid), now),
            Err(ImportError::GossipReject("invalid envelope signature"))
        ));
        let valid = WireEnvelope::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &wire, &config)
            .unwrap();
        worker.validate_envelope_gossip(&valid, now).unwrap();
        for _ in 0..2 {
            assert!(matches!(
                worker.import(Input::GossipEnvelope(valid.clone()), now),
                Err(ImportError::Envelope(_))
            ));
            assert_eq!(worker.graph.snapshot().unwrap(), before);
            let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
            assert!(!store.is_payload_verified(wire.root()));
            assert!(store.payload(wire.root()).is_none());
            assert!(worker.repository.original_envelope(wire.root()).unwrap().is_none());
        }
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

/// Every candidate is a complete real signed block with an exactly empty blob commitment list.
/// Registering its root cannot substitute for any nonempty data availability proof.
#[derive(Debug, Clone)]
struct RegisteredEmptyBlobData(Arc<Mutex<BTreeSet<Root>>>);
impl GloasAvailability for RegisteredEmptyBlobData {
    type Error = io::Error;
    async fn verify(&self, root: Root, block: &gloas::BeaconBlock) -> Result<(), Self::Error> {
        if !self.0.lock().unwrap().contains(&root) ||
            block.hash_tree_root(Preset::Minimal).unwrap() != root ||
            !block
                .body
                .signed_execution_payload_bid
                .message
                .blob_kzg_commitments
                .values
                .is_empty()
        {
            return Err(io::Error::other("unregistered or nonempty blob candidate"))
        }
        Ok(())
    }
}

fn empty_requests() -> gloas::ExecutionRequests {
    gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    }
}

/// Construct a real original self-build, including the actual assigned committee's FFG vote.
/// The FULL parent choice commits its exact requests; registry/finality fields are never seeded.
fn finalizing_child(
    store: &CompactGloasStore,
    parent: Root,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    parent_requests: gloas::ExecutionRequests,
    requests: &gloas::ExecutionRequests,
    full_parent: bool,
) -> gloas::SignedBeaconBlock {
    let mut state = (*store.read_block_state(parent).unwrap().unwrap()).clone();
    let processing = &parameters.gloas;
    let slot = state.slot + 1;
    let slots_per_epoch = processing.slots_per_epoch();
    let historical_slots = state.block_roots.values.len() as u64;
    gloas_processing::process_slots(&mut state, slot, processing).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[proposer as usize];
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    body.sync_aggregate.sync_committee_signature = infinity;
    body.parent_execution_requests = parent_requests;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: if full_parent {
            state.latest_execution_payload_bid.block_hash
        } else {
            state.latest_block_hash
        },
        parent_block_root: parent,
        block_hash: [100 + slot as u8; 32],
        prev_randao: state.randao_mixes.values
            [(slot / slots_per_epoch) as usize % state.randao_mixes.values.len()],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests.hash_tree_root(Preset::Minimal).unwrap(),
    };
    let attested_slot = slot - 1;
    let epoch = attested_slot / slots_per_epoch;
    let committee =
        gloas_processing::get_beacon_committee(&state, attested_slot, 0, processing).unwrap();
    let data = phase0::AttestationData {
        slot: attested_slot,
        index: 0,
        beacon_block_root: state.block_roots.values[(attested_slot % historical_slots) as usize],
        source: if epoch == slot / slots_per_epoch {
            state.current_justified_checkpoint
        } else {
            state.previous_justified_checkpoint
        },
        target: Checkpoint {
            epoch,
            root: state.block_roots.values[(epoch * slots_per_epoch % historical_slots) as usize],
        },
    };
    let version = if epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    let vote_root = signing_root(
        data.hash_tree_root(Preset::Minimal).unwrap(),
        [1, 0, 0, 0],
        version,
        state.genesis_validators_root,
    );
    let signatures = committee
        .iter()
        .map(|&index| keys[index as usize].sign(&vote_root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let signature = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
        .unwrap()
        .to_signature()
        .to_bytes();
    let mut bits = vec![0; (committee.len() + 1).div_ceil(8)];
    for index in 0..=committee.len() {
        bits[index / 8] |= 1 << (index % 8);
    }
    body.attestations.values.push(gloas::Attestation {
        data,
        signature,
        aggregation_bits: gloas::AggregationBits::from_ssz(&bits, Preset::Minimal).unwrap(),
        committee_bits: gloas::CommitteeBits::from_ssz(&[1], Preset::Minimal).unwrap(),
    });
    body.randao_reveal = key
        .sign(
            &signing_root(
                uint64_root(slot / slots_per_epoch),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let mut message = gloas::BeaconBlock {
        slot,
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
    gloas::SignedBeaconBlock { message, signature }
}

fn sign_external_bid(
    message: gloas::ExecutionPayloadBid,
    state: &gloas::BeaconState,
    key: &SecretKey,
) -> gloas::SignedExecutionPayloadBid {
    let root = signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        [11, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let signature = key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    gloas::SignedExecutionPayloadBid { message, signature }
}

fn next_preferences(
    store: &CompactGloasStore,
    parent: Root,
    slot: u64,
    keys: &[SecretKey],
) -> auxiliary::gloas::SignedProposerPreferences {
    let processing = store.processing_parameters();
    let start = (slot / store.slots_per_epoch()).saturating_sub(1) * store.slots_per_epoch();
    let dependent_root = store
        .get_ancestor(
            ForkChoiceNode { root: parent, payload_status: Some(GloasPayloadStatus::Pending) },
            start.saturating_sub(1),
        )
        .unwrap()
        .root;
    let mut state = (*store.read_block_state(dependent_root).unwrap().unwrap()).clone();
    let target = start.max(state.slot);
    gloas_processing::process_slots(&mut state, target, processing).unwrap();
    let validator_index = state.proposer_lookahead.values[(slot - start) as usize];
    let message = auxiliary::gloas::ProposerPreferences {
        dependent_root,
        proposal_slot: slot,
        validator_index,
        fee_recipient: [8; 20],
        target_gas_limit: 30_000_000,
    };
    let signature = keys[validator_index as usize]
        .sign(
            &signing_root(
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [13, 0, 0, 0],
                processing.fork_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    auxiliary::gloas::SignedProposerPreferences { message, signature }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finalized_external_builder_bid_requires_its_real_signed_parent_payload() {
    let directory = TempDir::new().unwrap();
    let (classic, keys, parameters, first_gloas) = phase0_gloas_boundary();
    let (initial, backend) = setup(&directory, &classic, &parameters);
    let first_gloas = WireBlock::from_ssz(first_gloas.to_ssz().unwrap(), &initial.config).unwrap();
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
    } = initial;
    let OwnerClock::Controlled(controlled) = &clock else { unreachable!() };
    let controlled = controlled.clone();
    let availability = RegisteredEmptyBlobData(Arc::new(Mutex::new(BTreeSet::new())));
    let setup = WorkerSetup {
        config: config.clone(),
        anchor,
        repository,
        engine,
        classic_availability,
        gloas_availability: availability.clone(),
        limits,
        runtime,
        clock,
    };
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let parameters = RuntimeParameters::from_config(&config).unwrap().transitions;
        let first_slot = first_gloas.slot() + 1;
        let last_slot = 6 * config.slots_per_epoch;
        let proposal_slot = last_slot + 1;
        let now = config.clock().unwrap().start_of_slot(first_gloas.slot()).unwrap();
        controlled.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        worker.import(Input::Block(first_gloas), now).unwrap();
        let builder = SecretKey::key_gen(&[222; 32], &[]).unwrap();
        let mut credentials = [0; 32];
        credentials[0] = 0xb0;
        credentials[12..].copy_from_slice(&[7; 20]);
        let amount = 2_000_000_000;
        let pubkey = builder.sk_to_pk().to_bytes();
        let deposit_root = merkleize(
            &[bytes_root(&pubkey, 48, false).unwrap(), credentials, uint64_root(amount)],
            3,
        )
        .unwrap();
        let signature = builder
            .sign(
                &signing_root(
                    deposit_root,
                    [14, 0, 0, 0],
                    parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
                    [0; 32],
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        let deposit = gloas::BuilderDepositRequest {
            pubkey,
            withdrawal_credentials: credentials,
            amount,
            signature,
        };
        assert!(gloas_processing::is_valid_builder_deposit_signature(&deposit, &parameters.gloas)
            .unwrap());
        let mut forged_deposit = deposit.clone();
        forged_deposit.signature = [0; 96];
        assert!(!gloas_processing::is_valid_builder_deposit_signature(&forged_deposit, &parameters.gloas)
            .unwrap());
        let mut deposits = empty_requests();
        // The forged first request must not allocate a registry position or suppress the valid one.
        deposits.builder_deposits.values.extend([forged_deposit, deposit]);
        let mut parent = worker.graph.snapshot().unwrap().head.root;
        let mut last_wire = None;
        for slot in first_slot..=last_slot {
            let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
            let signed = finalizing_child(
                store,
                parent,
                &keys,
                &parameters,
                if slot == first_slot + 1 { deposits.clone() } else { empty_requests() },
                &if slot == first_slot { deposits.clone() } else { empty_requests() },
                slot == first_slot + 1,
            );
            let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
            availability.0.lock().unwrap().insert(root);
            let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
            let now = config.clock().unwrap().start_of_slot(slot).unwrap();
            controlled.store(now, Ordering::Release);
            worker.advance(now).unwrap();
            let receipt = worker.import(Input::Block(wire.clone()), now).unwrap();
            assert_eq!(receipt.admitted_root, root);
            assert_eq!(receipt.snapshot.head.root, root);
            parent = root;
            if slot == first_slot {
                let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
                let state = store.read_block_state(root).unwrap().unwrap();
                assert!(state.builders.values.is_empty());
                drop(state);
                let mut envelope = original_gloas_envelope(store, root, &keys);
                envelope.message.execution_requests = deposits.clone();
                let state = store.read_block_state(root).unwrap().unwrap();
                envelope.signature = keys[state.latest_block_header.proposer_index as usize]
                    .sign(
                        &signing_root(
                            envelope.message.hash_tree_root(Preset::Minimal).unwrap(),
                            [11, 0, 0, 0],
                            state.fork.current_version,
                            state.genesis_validators_root,
                        ),
                        ETHEREUM_BLS_DST,
                        &[],
                    )
                    .to_bytes();
                drop(state);
                *backend.expected_payload_hash.lock().unwrap() = envelope.message.payload.block_hash;
                let envelope = WireEnvelope::from_ssz(
                    envelope.to_ssz(Preset::Minimal).unwrap(), &wire, &config,
                ).unwrap();
                worker.import(Input::GossipEnvelope(envelope), now).unwrap();
            }
            if slot == first_slot + 1 {
                let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
                let state = store.read_block_state(root).unwrap().unwrap();
                assert_eq!(state.builders.values.len(), 1);
                assert_eq!(state.builders.values[0].pubkey, pubkey);
                assert_eq!(state.builders.values[0].balance, amount);
                assert_eq!(state.builders.values[0].deposit_epoch, slot / config.slots_per_epoch);
                assert!(!gloas_processing::is_active_builder(&state, 0, &parameters.gloas).unwrap());
            }
            last_wire = Some(wire);
        }
        let wire = last_wire.unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let state = store.read_block_state(parent).unwrap().unwrap();
        assert!(state.finalized_checkpoint.epoch > state.builders.values[0].deposit_epoch);
        assert!(gloas_processing::is_active_builder(&state, 0, &parameters.gloas).unwrap());
        let mut message = state.latest_execution_payload_bid.clone();
        message.slot = proposal_slot;
        message.parent_block_root = parent;
        message.parent_block_hash = state.latest_execution_payload_bid.block_hash;
        message.block_hash = [201; 32];
        message.builder_index = 0;
        message.value = 1;
        message.prev_randao = state.randao_mixes.values
            [(state.slot / config.slots_per_epoch) as usize % state.randao_mixes.values.len()];
        let valid = sign_external_bid(message, &state, &builder);
        drop(state);
        let now = config.clock().unwrap().start_of_slot(proposal_slot).unwrap();
        controlled.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let preferences = next_preferences(store, parent, proposal_slot, &keys);
        assert_eq!(worker.dispatch_gossip(
            topic(&config, GossipKind::Preferences, proposal_slot),
            &preferences.to_ssz(Preset::Minimal).unwrap(), now,
        ).unwrap(), GossipVerdict::Accept);
        let bid_topic = topic(&config, GossipKind::Bid, proposal_slot);
        let before = worker.graph.snapshot().unwrap();
        let payload_calls = backend.payload_calls.lock().unwrap().len();
        let forkchoice_calls = backend.forkchoice_calls.lock().unwrap().len();
        // The finalized registry and an idle VALID Engine still cannot authenticate a payload.
        assert_eq!(worker.dispatch_gossip(bid_topic, &valid.to_ssz(Preset::Minimal).unwrap(), now)
            .unwrap(), GossipVerdict::Ignore);
        assert_eq!(worker.graph.snapshot().unwrap(), before);
        assert_eq!(backend.payload_calls.lock().unwrap().len(), payload_calls);
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), forkchoice_calls);
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        assert!(!store.is_payload_verified(parent));
        let envelope = original_gloas_envelope(store, parent, &keys);
        *backend.expected_payload_hash.lock().unwrap() = envelope.message.payload.block_hash;
        let envelope = WireEnvelope::from_ssz(
            envelope.to_ssz(Preset::Minimal).unwrap(), &wire, &config,
        ).unwrap();
        worker.import(Input::GossipEnvelope(envelope), now).unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        assert!(store.is_payload_verified(parent));
        assert!(store.payload(parent).is_some());
        assert!(worker.repository.original_envelope(parent).unwrap().is_some());
        let state = store.read_block_state(parent).unwrap().unwrap();
        let mut wrong_hash = valid.message.clone();
        wrong_hash.parent_block_hash = [202; 32];
        let wrong_hash = sign_external_bid(wrong_hash, &state, &builder);
        let mut wrong_randao = valid.message.clone();
        wrong_randao.prev_randao[0] ^= 1;
        let wrong_randao = sign_external_bid(wrong_randao, &state, &builder);
        let mut no_funds = valid.message.clone();
        no_funds.value = amount;
        let no_funds = sign_external_bid(no_funds, &state, &builder);
        drop(state);
        let mut forged = valid.clone();
        forged.signature = [0; 96];
        let before = worker.graph.snapshot().unwrap();
        let retained = worker.repository.stats().unwrap().retained_states;
        let payload_calls = backend.payload_calls.lock().unwrap().len();
        let forkchoice_calls = backend.forkchoice_calls.lock().unwrap().len();
        for (signed, expected) in [
            (wrong_hash, GossipVerdict::Ignore),
            (wrong_randao, GossipVerdict::Reject),
            (no_funds, GossipVerdict::Ignore),
            (forged, GossipVerdict::Reject),
            (valid.clone(), GossipVerdict::Accept),
            (valid, GossipVerdict::Ignore),
        ] {
            assert_eq!(worker.dispatch_gossip(bid_topic,
                &signed.to_ssz(Preset::Minimal).unwrap(), now).unwrap(), expected);
            assert_eq!(worker.graph.snapshot().unwrap(), before);
            assert_eq!(worker.repository.stats().unwrap().retained_states, retained);
            assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), before.head);
            assert_eq!(backend.payload_calls.lock().unwrap().len(), payload_calls);
            assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), forkchoice_calls);
        }
        // Accepted bid propagation does not import an external payload, block, or execution head.
        assert_eq!(worker.graph.snapshot().unwrap().head.root, parent);
    }).await.unwrap();
}
