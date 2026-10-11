//! Genuine signed Casper finalization, cold graph cuts and same-adapter canonical controls.
//! Controlled typed replies prove local Engine contracts; they are not live execution proof.

use super::*;
use crate::{
    bootstrap::genesis::tests::genuine_phase0_gloas,
    compact_persistence::{state_source::RepositoryStateSource, PersistenceNetwork},
    execution_bootstrap::{
        tests::{adapter, anchor, genuine_gloas, limits, BackendHandle, Reply},
        ExecutionBootstrap,
    },
    gossip_tests::{classic_to_gloas_child, signing_root, store_and_keys},
    BootstrapDecodeLimits, VerifiedGenesis,
};
use blst::min_pk::{AggregateSignature, SecretKey};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::{
    compact::{CompactGraphLimits, CompactStore},
    gloas::GloasStore,
    Store,
};
use reth_embedded_beacon_primitives::uint64_root;
use reth_embedded_beacon_shared_data::{journal::inject_post_rename_failure, SharedDataStore};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    gloas_processing,
    ssz::{gloas, phase0, ProgressiveList, SignedBeaconBlock, Ssz},
};
use reth_embedded_beacon_storage::CompactStateLimits;
use reth_fs_util as fs;
use std::time::Duration;
use tempfile::TempDir;

struct Modern {
    config: ChainConfig,
    anchor: Option<ImportAnchor>,
    store: CompactGloasStore,
    repository: Arc<AdmittedStateRepository>,
    source: Arc<RepositoryStateSource>,
    parameters: TransitionParameters,
    keys: Vec<SecretKey>,
    last: Root,
    _directory: TempDir,
}
async fn modern() -> (Modern, ExecutionCheckpointBoundary, RuntimeEngineAdapter<BackendHandle>) {
    let (setup, engine, receipt) = {
        let (config, anchor, parameters, original, block, keys, now) =
            tokio::task::spawn_blocking(|| {
                let (state, signed, parameters) =
                    tokio::runtime::Handle::current().block_on(genuine_gloas());
                let (config, anchor, now) = anchor(&state, &signed, &parameters);
                let BeaconState::Gloas(state, _) = state else {
                    panic!("original Gloas post-state")
                };
                let BeaconBlock::Gloas(block, _) = signed.message() else {
                    panic!("original signed Gloas block")
                };
                let (_, keys, _) = store_and_keys(Fork::Fulu);
                (config, anchor, parameters, state, block, keys, now)
            })
            .await
            .unwrap();
        let (engine, _) =
            adapter(config.clone(), vec![Reply::Valid, Reply::Valid], Duration::from_secs(1));
        let auth_config = config.clone();
        let auth_anchor = anchor.clone();
        let (mut bootstrap, _) = tokio::task::spawn_blocking(move || {
            ExecutionBootstrap::authenticate(auth_config, &auth_anchor, now, limits()).unwrap()
        })
        .await
        .unwrap();
        let receipt = bootstrap.reconcile(&engine).await.unwrap();
        let setup = tokio::task::spawn_blocking(move || {
            let directory = TempDir::new().unwrap();
            let network = PersistenceNetwork::from_config(config.clone()).unwrap();
            let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
            let repository = Arc::new(
                AdmittedStateRepository::open(shared, network, CompactStateLimits::default())
                    .unwrap(),
            );
            let verifier = SszCheckpointVerifier::new(
                config.clone(),
                anchor.decode_limits,
                anchor.block_format,
            )
            .unwrap();
            repository
                .persist_checkpoint(
                    &verifier,
                    &anchor.verified,
                    &anchor.state_ssz,
                    &anchor.block_ssz,
                    now,
                    anchor.max_age_epochs,
                )
                .unwrap();
            let runtime = RuntimeParameters::from_config(&config).unwrap();
            let graph = GloasStore::get_store(
                original,
                block,
                runtime.fork_choice,
                runtime.transitions.gloas,
            )
            .unwrap();
            let source = RepositoryStateSource::open(repository.clone()).unwrap();
            let last = anchor.verified.checkpoint().root;
            let store = CompactGloasStore::from_store(
                graph,
                source.clone(),
                CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() },
            )
            .unwrap();
            Modern {
                config,
                anchor: Some(anchor),
                store,
                repository,
                source,
                parameters,
                keys,
                last,
                _directory: directory,
            }
        })
        .await
        .unwrap();
        (setup, engine, receipt)
    };
    tokio::task::spawn_blocking(move || {
        let boundary = ExecutionCheckpointBoundary::authenticate(
            receipt,
            &setup.config,
            setup.anchor.as_ref().unwrap(),
            setup
                .config
                .clock()
                .unwrap()
                .start_of_slot(setup.anchor.as_ref().unwrap().verified.slot())
                .unwrap(),
            &setup.store,
            &engine,
        )
        .unwrap();
        (setup, boundary, engine)
    })
    .await
    .unwrap()
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
fn child_at_slot(setup: &Modern, parent: Root, hash: u8, target: u64) -> gloas::SignedBeaconBlock {
    let mut state = (*setup.store.read_block_state(parent).unwrap().unwrap()).clone();
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
        parent_block_hash: state.latest_block_hash,
        parent_block_root: parent,
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
    let attested_slot = target - 1;
    let epoch = attested_slot / setup.config.slots_per_epoch;
    let committee =
        gloas_processing::get_beacon_committee(&state, attested_slot, 0, parameters).unwrap();
    let data = phase0::AttestationData {
        slot: attested_slot,
        index: 0,
        beacon_block_root: state.block_roots.values[(attested_slot % 64) as usize],
        source: if epoch == target / 8 {
            state.current_justified_checkpoint
        } else {
            state.previous_justified_checkpoint
        },
        target: Checkpoint { epoch, root: state.block_roots.values[(epoch * 8 % 64) as usize] },
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
        .map(|&index| setup.keys[index as usize].sign(&vote_root, ETHEREUM_BLS_DST, &[]))
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
fn advance(setup: &mut Modern, target: u64) {
    let current = setup.store.block(setup.last).unwrap().slot;
    for slot in current + 1..=target {
        let signed = child_at_slot(setup, setup.last, 100 + slot as u8, slot);
        let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
        setup.store.on_tick(setup.config.clock().unwrap().start_of_slot(slot).unwrap()).unwrap();
        // The entire original signed transition and each real contained BLS vote are mandatory.
        setup.store.on_block_and_contents(&signed).unwrap();
        setup.last = root;
    }
}
fn execution(setup: &Modern, boundary: &ExecutionCheckpointBoundary) -> ForkchoiceState {
    let head = boundary
        .verified_execution_block_hash(&setup.store, setup.store.get_head().unwrap())
        .unwrap()
        .unwrap();
    let finalized = boundary.finalized_execution_block_hash(&setup.store).unwrap().unwrap();
    ForkchoiceState {
        head_block_hash: B256::from(head),
        safe_block_hash: B256::from(finalized),
        finalized_block_hash: B256::from(finalized),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_nonzero_checkpoint_cuts_preserve_private_base_and_require_both_publications() {
    let (mut setup, active, engine) = modern().await;
    let original = setup.anchor.as_ref().unwrap().verified.checkpoint().root;
    let (mut setup, engine, prepared, early_commit, expected, policy) =
        tokio::task::spawn_blocking(move || {
            advance(&mut setup, 64);
            assert!(setup.store.snapshot().unwrap().finalized_checkpoint.epoch > 2);
            let expected = execution(&setup, &active);
            let policy = FinalizedRetentionPolicy::new(2).unwrap();
            let prepared = active
                .prepare_finalized_roll_forward(
                    &setup.config,
                    &setup.store,
                    policy,
                    Some(expected),
                    &engine,
                )
                .unwrap()
                .unwrap();
            let early_engine = active
                .prepare_finalized_roll_forward(
                    &setup.config,
                    &setup.store,
                    policy,
                    Some(expected),
                    &engine,
                )
                .unwrap()
                .unwrap();
            let early_commit = active
                .prepare_finalized_roll_forward(
                    &setup.config,
                    &setup.store,
                    policy,
                    Some(expected),
                    &engine,
                )
                .unwrap()
                .unwrap();
            let before = setup.store.snapshot().unwrap();
            assert!(matches!(
                setup.store.prune_finalized_history(policy),
                Err(StoreError::UnknownRoot)
            ));
            assert_eq!(setup.store.snapshot().unwrap(), before);
            let cut = setup.store.prune_finalized_consensus_history(policy).unwrap();
            assert_eq!(cut.boundary, Some(prepared.checkpoint()));
            assert!(cut.removed_blocks > 0);
            assert!(setup.store.read_block_state(original).unwrap().is_none());
            assert!(matches!(
                setup.store.verified_execution_block_hash(setup.store.get_head().unwrap()),
                Err(StoreError::UnknownRoot)
            ));
            assert_eq!(
                prepared
                    .candidate()
                    .verified_execution_block_hash(&setup.store, setup.store.get_head().unwrap())
                    .unwrap(),
                Some([41; 32])
            );
            assert!(matches!(
                early_engine.commit(&setup.config, &engine, None, &setup.repository),
                Err(BoundaryCommitError::Binding)
            ));
            (setup, engine, prepared, early_commit, expected, policy)
        })
        .await
        .unwrap();
    let receipt = engine
        .forkchoice_updated_receipt(
            reth_embedded_beacon_execution_proxy::ForkchoiceVersion::V4,
            expected,
            None,
        )
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        assert!(matches!(
            early_commit.commit(&setup.config, &engine, Some(&receipt), &setup.repository),
            Err(BoundaryCommitError::Binding)
        ));
        setup.source.commit_snapshot(setup.store.snapshot().unwrap()).unwrap();
        let first_cut = prepared.checkpoint().root;
        let mut active =
            prepared.commit(&setup.config, &engine, Some(&receipt), &setup.repository).unwrap();
        setup.source.finish_retention(setup.store.snapshot().unwrap()).unwrap();
        reauthenticate_active_checkpoint(&setup);
        assert_eq!(active.finalized_execution_block_hash(&setup.store).unwrap(), Some([41; 32]));
        assert!(matches!(
            setup.store.verified_execution_block_hash(setup.store.get_head().unwrap()),
            Err(StoreError::UnknownRoot)
        ));
        let prior = active.root;
        advance(&mut setup, 96);
        let expected2 = execution(&setup, &active);
        assert_eq!(expected2, expected);
        let prepared = active
            .prepare_finalized_roll_forward(
                &setup.config,
                &setup.store,
                policy,
                Some(expected2),
                &engine,
            )
            .unwrap()
            .unwrap();
        let cut = setup.store.prune_finalized_consensus_history(policy).unwrap();
        assert!(cut.removed_blocks > 0);
        assert_eq!(cut.boundary, Some(prepared.checkpoint()));
        setup.source.commit_snapshot(setup.store.snapshot().unwrap()).unwrap();
        active =
            prepared.commit(&setup.config, &engine, Some(&receipt), &setup.repository).unwrap();
        setup.source.finish_retention(setup.store.snapshot().unwrap()).unwrap();
        reauthenticate_active_checkpoint(&setup);
        assert!(setup.repository.original_block(original).unwrap().is_some());
        // The first local generation remains readable after the second canonical cut.
        assert_eq!(prior, first_cut);
        assert!(setup.repository.original_block(first_cut).unwrap().is_some());
        assert_ne!(active.root, prior);
        assert!(setup.store.read_block_state(prior).unwrap().is_none());
        assert_eq!(
            active
                .verified_execution_block_hash(&setup.store, setup.store.get_head().unwrap())
                .unwrap(),
            Some([41; 32])
        );
        assert_eq!(
            active
                .verified_execution_block_hash(
                    &setup.store,
                    ForkChoiceNode {
                        root: active.root,
                        payload_status: Some(PayloadStatus::Pending)
                    }
                )
                .unwrap(),
            None
        );
        assert!(matches!(
            active.verified_execution_block_hash(
                &setup.store,
                ForkChoiceNode { root: active.root, payload_status: Some(PayloadStatus::Full) }
            ),
            Err(ExecutionBoundaryError::Store(StoreError::ExecutionImport(_)))
        ));
        active.check_adapter(&setup.config, &engine).unwrap();
        let expected_snapshot = setup.store.snapshot().unwrap();
        let current_root = active.root;
        let (fresh_engine, calls) =
            adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
        let (setup, prepared) = reopen_modern(setup, &fresh_engine);
        assert_eq!(
            calls.forkchoice_call_count(),
            0,
            "signed replay must not rewind execution to its cut seed"
        );
        assert!(!receipt.matches(&fresh_engine, expected2));
        assert_eq!(setup.store.snapshot().unwrap(), expected_snapshot);
        assert!(matches!(
            setup.store.verified_execution_block_hash(setup.store.get_head().unwrap()),
            Err(StoreError::UnknownRoot)
        ));
        let fresh_receipt = tokio::runtime::Handle::current()
            .block_on(fresh_engine.forkchoice_updated_receipt(
                reth_embedded_beacon_execution_proxy::ForkchoiceVersion::V4,
                expected2,
                None,
            ))
            .unwrap();
        assert_eq!(calls.forkchoice_call_count(), 1);
        let active = prepared
            .commit(
                &setup.config,
                &setup.store,
                expected_snapshot,
                Some(expected2),
                Some(&fresh_receipt),
                &setup.repository,
                &fresh_engine,
            )
            .unwrap();
        assert_eq!(active.root, current_root);
        assert_eq!(
            active
                .verified_execution_block_hash(&setup.store, setup.store.get_head().unwrap())
                .unwrap(),
            Some([41; 32])
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_adapter_and_invented_execution_tuple_cannot_prepare_a_cut() {
    let (mut setup, active, engine) = modern().await;
    tokio::task::spawn_blocking(move || {
        advance(&mut setup, 64);
        let expected = execution(&setup, &active);
        let policy = FinalizedRetentionPolicy::new(2).unwrap();
        let before = setup.store.snapshot().unwrap();
        let (foreign, _) =
            adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
        assert!(matches!(
            active.prepare_finalized_roll_forward(
                &setup.config,
                &setup.store,
                policy,
                Some(expected),
                &foreign
            ),
            Err(ExecutionBoundaryError::Binding)
        ));
        let wrong = ForkchoiceState { head_block_hash: B256::from([99; 32]), ..expected };
        assert!(matches!(
            active.prepare_finalized_roll_forward(
                &setup.config,
                &setup.store,
                policy,
                Some(wrong),
                &engine
            ),
            Err(ExecutionBoundaryError::Binding)
        ));
        assert!(matches!(
            active.prepare_finalized_roll_forward(
                &setup.config,
                &setup.store,
                policy,
                None,
                &engine
            ),
            Err(ExecutionBoundaryError::Binding)
        ));
        assert_eq!(setup.store.snapshot().unwrap(), before);
        assert!(setup.repository.current_snapshot().unwrap().is_none());
    })
    .await
    .unwrap();
}

fn reauthenticate_active_checkpoint(setup: &Modern) {
    let original = setup.anchor.as_ref().unwrap();
    let verifier = SszCheckpointVerifier::new(
        setup.config.clone(),
        original.decode_limits,
        original.block_format,
    )
    .unwrap();
    let now = setup.store.time_ms();
    let recovered = setup.repository.recover_checkpoint(&verifier, now, 2048).unwrap().unwrap();
    let anchor = recovered.into_import_anchor(2048, original.decode_limits).unwrap();
    let verifier =
        SszCheckpointVerifier::new(setup.config.clone(), anchor.decode_limits, anchor.block_format)
            .unwrap();
    if let Some(replay) = &anchor.replay {
        setup
            .repository
            .persist_replayed_checkpoint(
                &verifier,
                &replay.proof,
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                now,
                2048,
            )
            .unwrap();
    } else {
        setup
            .repository
            .persist_checkpoint(
                &verifier,
                &anchor.verified,
                &anchor.state_ssz,
                &anchor.block_ssz,
                now,
                2048,
            )
            .unwrap();
    }
    assert_ne!(anchor.verified.checkpoint().root, original.verified.checkpoint().root);
    assert_eq!(
        setup
            .repository
            .original_block(original.verified.checkpoint().root)
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        original.block_ssz.as_ref()
    );
}

fn reopen_modern(
    setup: Modern,
    engine: &RuntimeEngineAdapter<BackendHandle>,
) -> (Modern, crate::execution_checkpoint_boundary::recovery::PreparedRecoveryExecutionBoundary) {
    let expected = setup.store.snapshot().unwrap();
    let now = setup.store.time_ms();
    reopen_modern_at(setup, engine, expected, now)
}

fn reopen_modern_at(
    setup: Modern,
    engine: &RuntimeEngineAdapter<BackendHandle>,
    expected: StoreSnapshot,
    now: u64,
) -> (Modern, crate::execution_checkpoint_boundary::recovery::PreparedRecoveryExecutionBoundary) {
    let Modern { config, anchor, store, repository, source, parameters, keys, last, _directory } =
        setup;
    drop(store);
    drop(source);
    drop(repository);
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(_directory.path(), network.identity(), 0).unwrap();
    let repository = Arc::new(
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap(),
    );
    let local = repository.local_recovery_inputs().unwrap().unwrap();
    let checkpoint = local.checkpoint;
    let BeaconState::Gloas(post, _) = local.post.state() else { panic!("native Gloas post-state") };
    let BeaconBlock::Gloas(block, _) = local.block else { panic!("native Gloas cut block") };
    let runtime = RuntimeParameters::from_config(&config).unwrap();
    let graph = GloasStore::get_store_at_checkpoint(
        post.clone(),
        block,
        checkpoint,
        &local.boundary.state().to_ssz().unwrap(),
        runtime.fork_choice,
        runtime.transitions.gloas,
    )
    .unwrap();
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    let mut store = CompactGloasStore::from_store(
        graph,
        source.clone(),
        CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() },
    )
    .unwrap();
    let prepared =
        crate::execution_checkpoint_boundary::recovery::PreparedRecoveryExecutionBoundary::prepare(
            &config,
            &store,
            &repository,
            engine,
        )
        .unwrap();
    for (slot, root) in repository.replay_inventory(4096).unwrap() {
        if root == checkpoint.root {
            continue
        }
        let (format, bytes) = repository.original_block(root).unwrap().unwrap();
        assert_eq!(format, CheckpointBlockFormat::Signed);
        let SignedBeaconBlock::Gloas(signed, _) =
            SignedBeaconBlock::from_ssz(&bytes, Fork::Gloas, Preset::Minimal).unwrap()
        else {
            panic!("native signed replay")
        };
        store.on_tick(config.clock().unwrap().start_of_slot(slot).unwrap()).unwrap();
        store.on_block_and_contents(&signed).unwrap();
    }
    store.on_tick(now).unwrap();
    assert_eq!(store.snapshot().unwrap(), expected);
    source.commit_snapshot(expected).unwrap();
    (
        Modern { config, anchor, store, repository, source, parameters, keys, last, _directory },
        prepared,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_genesis_origin_survives_two_native_finalized_generations_and_reopen() {
    let (config, state, keys, parameters) = genuine_phase0_gloas();
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    let origin = genesis.block_root();
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let network = PersistenceNetwork::from_config(config.clone()).unwrap();
        let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
        let repository = Arc::new(
            AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
                .unwrap(),
        );
        repository.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
        let (post, block) = genesis.clone().into_import_inputs(&config).unwrap();
        let runtime = RuntimeParameters::from_config(&config).unwrap();
        let mut graph =
            Store::get_store(post.as_ref().clone(), block.as_ref().clone(), runtime.fork_choice)
                .unwrap();
        graph.set_transition_parameters(parameters.clone()).unwrap();
        let signed = classic_to_gloas_child(&graph, &keys, &parameters);
        let last = signed.message().hash_tree_root().unwrap();
        let source = RepositoryStateSource::open(repository.clone()).unwrap();
        let mut classic = CompactStore::from_store(
            graph,
            source.clone(),
            CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() },
        )
        .unwrap();
        classic
            .on_tick(config.clock().unwrap().start_of_slot(parameters.slots_per_epoch()).unwrap())
            .unwrap();
        let (engine, calls) = adapter(config.clone(), Vec::new(), Duration::from_secs(1));
        let store = tokio::runtime::Handle::current()
            .block_on(CompactGloasStore::migrate_from_classic(&classic, &signed, &engine))
            .unwrap();
        assert_eq!(calls.forkchoice_call_count(), 0);
        // Migration shares its durable state source; this superseded owner is not a cold reader.
        drop(classic);
        let mut setup = Modern {
            config,
            anchor: None,
            store,
            repository,
            source,
            parameters,
            keys,
            last,
            _directory: directory,
        };
        let policy = FinalizedRetentionPolicy::new(2).unwrap();
        advance(&mut setup, 64);
        let obsolete = setup
            .repository
            .replay_inventory(4096)
            .unwrap()
            .into_iter()
            .find(|(slot, _)| *slot == 9)
            .unwrap()
            .1;
        let held = setup.repository.block_state(obsolete).unwrap().unwrap();
        let held_root = held.state().hash_tree_root().unwrap();
        let original_bytes = setup.repository.original_block(obsolete).unwrap().unwrap().1;
        let independent = reth_embedded_beacon_shared_data::Owner::Pool([91; 32]);
        let id = shared
            .put(
                reth_embedded_beacon_shared_data::ObjectKind::BeaconBlock,
                reth_embedded_beacon_shared_data::Encoding::Ssz,
                &original_bytes,
                independent,
            )
            .unwrap();

        let first_cut = setup.store.prune_finalized_history(policy).unwrap();
        assert!(first_cut.removed_blocks > 0);
        let first_root = first_cut.boundary.unwrap().root;
        setup.source.commit_snapshot(setup.store.snapshot().unwrap()).unwrap();
        setup.source.finish_retention(setup.store.snapshot().unwrap()).unwrap();
        assert!(setup.repository.original_block(obsolete).unwrap().is_none());
        assert!(shared
            .owner_objects(reth_embedded_beacon_shared_data::Owner::Consensus(obsolete))
            .is_empty());
        assert_eq!(shared.owner_objects(independent), vec![id]);
        assert_eq!(shared.get_bounded(id, original_bytes.len()).unwrap().unwrap(), original_bytes);
        assert_eq!(held.state().hash_tree_root().unwrap(), held_root);
        // This control owns only its isolated TempDir. Exercise the existing explicit collector
        // after a genuinely signed finalized cut: released immutable files disappear while an
        // independently held original and an owning state lease remain valid.
        let physical_before = shared.stats().unwrap().physical_bytes;
        let collected = setup.repository.collect_retired_objects().unwrap();
        let physical_after = shared.stats().unwrap().physical_bytes;
        assert!(collected > 0);
        assert!(physical_after < physical_before);
        assert_eq!(shared.owner_objects(independent), vec![id]);
        assert_eq!(shared.get_bounded(id, original_bytes.len()).unwrap().unwrap(), original_bytes);
        assert_eq!(held.state().hash_tree_root().unwrap(), held_root);

        assert_eq!(
            setup
                .repository
                .recover_genesis(BootstrapDecodeLimits::default())
                .unwrap()
                .unwrap()
                .block_root(),
            origin
        );
        advance(&mut setup, 96);
        // Durable unpublished originals are intentionally absent from the committed replay
        // inventory. The actual live native graph supplies this admitted competing-branch parent.
        let parent = setup.store.block(setup.last).unwrap().parent_root;
        assert_eq!(setup.store.block(parent).unwrap().slot, 95);
        assert!(setup.repository.original_block(parent).unwrap().is_some());
        let competing = child_at_slot(&setup, parent, 249, 96);
        let competing_root = competing.message.hash_tree_root(Preset::Minimal).unwrap();
        setup.store.on_block_and_contents(&competing).unwrap();
        setup.store.on_tick(setup.config.clock().unwrap().start_of_slot(97).unwrap()).unwrap();

        let second_cut = setup.store.prune_finalized_history(policy).unwrap();
        assert!(second_cut.removed_blocks > 0);
        assert_ne!(second_cut.boundary.unwrap().root, first_root);
        let expected = setup.store.snapshot().unwrap();
        let now = setup.store.time_ms();
        setup.source.commit_snapshot(expected).unwrap();
        // Crash at the admission deletion-plan publication, after the real canonical pointer
        // is durable. No compact or original owner may be released on this uncertain return.
        let journal = shared.namespace_root().join("beacon-admitted-states-v1/journal");
        let generation = fs::read_dir(&journal)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                entry
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
        assert!(setup.source.finish_retention(expected).is_err());
        assert!(setup.repository.current_snapshot().is_err());
        assert!(setup.repository.original_block(first_root).is_err());
        assert_eq!(shared.owner_objects(independent), vec![id]);
        assert_eq!(held.state().hash_tree_root().unwrap(), held_root);
        // Owning leases preserve their data and the live durable writer. The checks above
        // prove their survival through retirement/crash; close all writers before cold reopen.
        drop(held);
        let (engine, _) = adapter(setup.config.clone(), Vec::new(), Duration::from_secs(1));
        drop(shared);
        let (setup, prepared) = reopen_modern_at(setup, &engine, expected, now);

        assert!(setup.store.read_block_state(competing_root).unwrap().is_some());
        assert!(setup.repository.original_block(competing_root).unwrap().is_some());
        assert!(setup.store.read_block_state(first_root).unwrap().is_none());
        assert!(setup.repository.original_block(first_root).unwrap().is_some());
        assert_eq!(
            setup
                .repository
                .recover_genesis(BootstrapDecodeLimits::default())
                .unwrap()
                .unwrap()
                .original_state_ssz(),
            genesis.original_state_ssz()
        );
        assert_eq!(
            setup
                .repository
                .recover_genesis(BootstrapDecodeLimits::default())
                .unwrap()
                .unwrap()
                .block_root(),
            origin
        );
        let active = prepared
            .commit(&setup.config, &setup.store, expected, None, None, &setup.repository, &engine)
            .unwrap();
        assert_eq!(
            active
                .verified_execution_block_hash(&setup.store, setup.store.get_head().unwrap())
                .unwrap(),
            None
        );
        assert!(setup.repository.original_block(first_root).unwrap().is_some());
        assert!(setup.store.read_block_state(competing_root).unwrap().is_some());
        // Close every manual graph/source writer, then exercise the real genesis initializer.
        // Its graph starts at the later signed cut, while custody retains the exact unsigned
        // origin directly from fresh complete genesis authentication.
        let Modern { config, store, repository, source, _directory, .. } = setup;
        drop(store);
        drop(source);
        drop(repository);
        crate::import_coordinator::assert_genesis_custody_after_cold_reopen(
            config,
            genesis,
            _directory.path(),
            now,
        );
    })
    .await
    .unwrap();
}

#[path = "tests/old_ptc.rs"]
mod old_ptc;
