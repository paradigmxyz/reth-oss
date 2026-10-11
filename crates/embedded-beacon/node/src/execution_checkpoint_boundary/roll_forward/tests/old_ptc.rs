//! Original signed PTC observations cannot cross a skipped checkpoint's constructor clock.

use super::*;
use crate::compact_persistence::events::{DurableEvent, EventAction};
use reth_embedded_beacon_fork_choice::compact::CompactStateSource;
use reth_embedded_beacon_network::gossip::{GossipKind, GossipTopic};
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind};
use reth_embedded_beacon_state_transition::ssz::auxiliary;

fn record(setup: &Modern, begin_ms: u64, action: EventAction, original: Option<&[u8]>) {
    let completed_ms = setup.store.time_ms();
    setup
        .source
        .record_event(
            setup.store.snapshot().unwrap(),
            DurableEvent {
                sequence: 0,
                begin_ms,
                completed_ms,
                // The native tick/import begins before its completed state is staged. An
                // observation cannot claim receipt after processing began; ordinary block and
                // gossip rows here complete at the same controlled clock as their actual input.
                received_ms: begin_ms,
                historical: false,
                action,
            },
            original,
            false,
        )
        .unwrap_or_else(|error| {
            panic!("actual native event beginning {begin_ms}, completed {completed_ms}: {error:?}")
        });
}

fn advance_skipping_boundary(setup: &mut Modern, target: u64) {
    let current = setup.store.block(setup.last).unwrap().slot;
    for slot in current + 1..=target {
        if slot == 32 {
            continue
        }
        let signed = child_at_slot(setup, setup.last, 100 + slot as u8, slot);
        let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
        let begin = setup.store.time_ms();
        setup.store.on_tick(setup.config.clock().unwrap().start_of_slot(slot).unwrap()).unwrap();
        record(setup, begin, EventAction::Tick, None);
        setup.store.on_block_and_contents(&signed).unwrap();
        record(setup, setup.store.time_ms(), EventAction::Block(root), None);
        setup.last = root;
    }
}

type PtcRows = Vec<(Root, Option<Vec<Option<bool>>>, Option<Vec<Option<bool>>>)>;

fn ptc_rows(setup: &Modern) -> PtcRows {
    setup
        .repository
        .replay_inventory(4096)
        .unwrap()
        .into_iter()
        .map(|(_, root)| {
            (
                root,
                setup.store.payload_timeliness_votes(root).map(|votes| votes.to_vec()),
                setup.store.payload_data_availability_votes(root).map(|votes| votes.to_vec()),
            )
        })
        .collect()
}

fn commit_cut(
    setup: &mut Modern,
    active: &ExecutionCheckpointBoundary,
    engine: &RuntimeEngineAdapter<BackendHandle>,
    ordinal: u64,
    checkpoint: Checkpoint,
) -> ExecutionCheckpointBoundary {
    let expected = execution(setup, active);
    let policy = FinalizedRetentionPolicy::new(2).unwrap();
    let before = setup.store.snapshot().unwrap();
    let floor = setup.source.retention_epoch_limit().unwrap();
    assert_eq!(
        floor,
        Some(checkpoint.epoch),
        "cut {ordinal}: native finality {:?}, policy {policy:?}",
        before.finalized_checkpoint
    );
    assert_eq!(
        setup.store.finalized_retention_boundary(policy).unwrap(),
        Some(checkpoint),
        "cut {ordinal}: native finality {:?}, durable floor {floor:?}",
        before.finalized_checkpoint
    );
    let mut prepared = active
        .prepare_finalized_roll_forward(&setup.config, &setup.store, policy, Some(expected), engine)
        .unwrap()
        .unwrap();
    assert_eq!(prepared.checkpoint(), checkpoint, "cut {ordinal}: prepared boundary");
    let cut = setup.store.prune_finalized_consensus_history(policy).unwrap();
    assert!(
        cut.removed_blocks > 0,
        "cut {ordinal}: boundary {:?}, native finality {:?}, durable floor {floor:?}",
        cut.boundary,
        before.finalized_checkpoint
    );
    assert_eq!(cut.boundary, Some(prepared.checkpoint()), "cut {ordinal}: actual boundary");
    let wrong = ForkchoiceState { safe_block_hash: B256::from([200; 32]), ..expected };
    assert!(matches!(
        prepared.rebind_finalized_confirmation(&setup.config, &setup.store, engine, Some(wrong)),
        Err(ExecutionBoundaryError::Binding)
    ));
    prepared
        .rebind_finalized_confirmation(&setup.config, &setup.store, engine, Some(expected))
        .unwrap();
    let receipt = tokio::runtime::Handle::current()
        .block_on(engine.forkchoice_updated_receipt(
            reth_embedded_beacon_execution_proxy::ForkchoiceVersion::V4,
            expected,
            None,
        ))
        .unwrap();
    setup.source.commit_snapshot(setup.store.snapshot().unwrap()).unwrap();
    let active = prepared.commit(&setup.config, engine, Some(&receipt), &setup.repository).unwrap();
    setup.source.finish_retention(setup.store.snapshot().unwrap()).unwrap();
    active
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_old_ptc_skipped_boundary_reopens_exactly_and_advances_another_native_cut() {
    let (mut setup, active, engine) = modern().await;
    tokio::task::spawn_blocking(move || {
        setup.source.enable_event_transcript().unwrap();
        advance_skipping_boundary(&mut setup, 31);
        let root31 = setup.last;
        let state = setup.store.read_block_state(root31).unwrap().unwrap();
        let committee = gloas_processing::get_ptc(&state, 31, &setup.parameters.gloas).unwrap();
        let validator_index = committee.values[0];
        let data = gloas::PayloadAttestationData {
            beacon_block_root: root31,
            slot: 31,
            payload_present: true,
            blob_data_available: true,
        };
        let signing = signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            [12, 0, 0, 0],
            state.fork.current_version,
            state.genesis_validators_root,
        );
        let signature =
            setup.keys[validator_index as usize].sign(&signing, ETHEREUM_BLS_DST, &[]).to_bytes();
        let expected_positions = committee
            .values
            .iter()
            .map(|index| (*index == validator_index).then_some(true))
            .collect::<Vec<_>>();
        drop(state);
        let signed =
            auxiliary::gloas::PayloadAttestationMessage { validator_index, data, signature };
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert!(setup.store.on_payload_attestation_message(&invalid).is_err());
        setup.store.on_payload_attestation_message(&signed).unwrap();
        assert_eq!(setup.store.payload_timeliness_votes(root31).unwrap(), expected_positions);
        assert_eq!(
            setup.store.payload_data_availability_votes(root31).unwrap(),
            expected_positions
        );
        assert!(!setup.store.is_payload_verified(root31));
        assert!(setup.store.payload(root31).is_none());
        let bytes = signed.to_ssz(Preset::Minimal).unwrap();
        let action = EventAction::Gossip {
            topic: GossipTopic {
                fork: Fork::Gloas,
                digest: setup.config.fork_digest(31 / setup.config.slots_per_epoch).unwrap(),
                kind: GossipKind::PayloadAttestation,
            }
            .name(),
            root: root31,
            original: ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, &bytes),
        };
        record(&setup, setup.store.time_ms(), action.clone(), Some(&bytes));
        setup.source.commit_snapshot(setup.store.snapshot().unwrap()).unwrap();
        let original = setup
            .repository
            .replay_transcript()
            .unwrap()
            .unwrap()
            .sequences
            .iter()
            .map(|sequence| setup.repository.original_event(*sequence).unwrap())
            .find(|event| event.action == action)
            .unwrap();
        assert_eq!(setup.repository.original_observation(&original).unwrap().as_ref(), bytes);
        // Current and previous canonical snapshots constrain retention independently. Publish
        // genuinely later signed states so the slot-31 snapshot cannot pin the cut at epoch 2.
        advance_skipping_boundary(&mut setup, 48);
        let root48 = setup.last;
        let intermediate = setup.store.snapshot().unwrap();
        assert_eq!(intermediate.finalized_checkpoint.epoch, 4);
        setup.source.commit_snapshot(intermediate).unwrap();
        advance_skipping_boundary(&mut setup, 64);
        let intermediate = setup.store.snapshot().unwrap();
        assert_eq!(intermediate.finalized_checkpoint.epoch, 6);
        setup.source.commit_snapshot(intermediate).unwrap();
        let active =
            commit_cut(&mut setup, &active, &engine, 1, Checkpoint { epoch: 4, root: root31 });
        assert_eq!(active.root, root31);
        let local = setup.repository.local_recovery_inputs().unwrap().unwrap();
        assert_eq!(local.checkpoint, Checkpoint { epoch: 4, root: root31 });
        assert_eq!(local.post.state().slot(), 31);
        assert_eq!(local.boundary.state().slot(), 32);
        assert_ne!(
            local.post.state().hash_tree_root().unwrap(),
            local.boundary.state().hash_tree_root().unwrap()
        );
        drop(local);
        assert!(setup.store.payload_timeliness_votes(root31).unwrap().iter().all(Option::is_none));
        assert!(setup
            .store
            .payload_data_availability_votes(root31)
            .unwrap()
            .iter()
            .all(Option::is_none));
        assert!(!setup
            .repository
            .replay_transcript()
            .unwrap()
            .unwrap()
            .sequences
            .contains(&original.sequence));
        assert!(setup.repository.original_envelope(root31).unwrap().is_none());
        let expected = setup.store.snapshot().unwrap();
        let state_root = setup
            .store
            .read_block_state(expected.head.root)
            .unwrap()
            .unwrap()
            .hash_tree_root()
            .unwrap();
        let expected_execution = execution(&setup, &active);
        let rows = ptc_rows(&setup);
        let (fresh, calls) =
            adapter(setup.config.clone(), vec![Reply::Valid, Reply::Valid], Duration::from_secs(1));
        let (mut setup, prepared) = reopen_modern(setup, &fresh);
        assert_eq!(calls.forkchoice_call_count(), 0);
        assert_eq!(setup.store.snapshot().unwrap(), expected);
        assert_eq!(
            setup
                .store
                .read_block_state(expected.head.root)
                .unwrap()
                .unwrap()
                .hash_tree_root()
                .unwrap(),
            state_root
        );
        assert_eq!(ptc_rows(&setup), rows);
        let receipt = tokio::runtime::Handle::current()
            .block_on(fresh.forkchoice_updated_receipt(
                reth_embedded_beacon_execution_proxy::ForkchoiceVersion::V4,
                expected_execution,
                None,
            ))
            .unwrap();
        let active = prepared
            .commit(
                &setup.config,
                &setup.store,
                expected,
                Some(expected_execution),
                Some(&receipt),
                &setup.repository,
                &fresh,
            )
            .unwrap();
        assert_eq!(execution(&setup, &active), expected_execution);
        assert_eq!(calls.forkchoice_call_count(), 1);

        let first_finalized = expected.finalized_checkpoint;
        // Rotate the recovery floor with another actual signed canonical snapshot before
        // asking for a second cut; merely advancing the private graph cannot move this floor.
        advance(&mut setup, 80);
        let intermediate = setup.store.snapshot().unwrap();
        assert_eq!(intermediate.finalized_checkpoint.epoch, 8);
        assert!(intermediate.finalized_checkpoint.epoch > first_finalized.epoch);
        assert_eq!(execution(&setup, &active), expected_execution);
        setup.source.commit_snapshot(intermediate).unwrap();
        advance(&mut setup, 96);
        assert_eq!(setup.store.snapshot().unwrap().finalized_checkpoint.epoch, 10);
        let active =
            commit_cut(&mut setup, &active, &fresh, 2, Checkpoint { epoch: 6, root: root48 });
        assert_ne!(active.root, root31);
        assert_eq!(active.root, root48);
        assert_eq!(active.slot, 48);
        assert!(active.slot > 31);
        assert!(setup.store.snapshot().unwrap().finalized_checkpoint.epoch > first_finalized.epoch);
        assert_eq!(calls.forkchoice_call_count(), 2);
        let expected = setup.store.snapshot().unwrap();
        let state_root = setup
            .store
            .read_block_state(expected.head.root)
            .unwrap()
            .unwrap()
            .hash_tree_root()
            .unwrap();
        let expected_execution = execution(&setup, &active);
        let rows = ptc_rows(&setup);
        let (final_engine, calls) =
            adapter(setup.config.clone(), vec![Reply::Valid], Duration::from_secs(1));
        let (setup, prepared) = reopen_modern(setup, &final_engine);
        assert_eq!(calls.forkchoice_call_count(), 0);
        assert_eq!(setup.store.snapshot().unwrap(), expected);
        assert_eq!(
            setup
                .store
                .read_block_state(expected.head.root)
                .unwrap()
                .unwrap()
                .hash_tree_root()
                .unwrap(),
            state_root
        );
        assert_eq!(ptc_rows(&setup), rows);
        let receipt = tokio::runtime::Handle::current()
            .block_on(final_engine.forkchoice_updated_receipt(
                reth_embedded_beacon_execution_proxy::ForkchoiceVersion::V4,
                expected_execution,
                None,
            ))
            .unwrap();
        let restored = prepared
            .commit(
                &setup.config,
                &setup.store,
                expected,
                Some(expected_execution),
                Some(&receipt),
                &setup.repository,
                &final_engine,
            )
            .unwrap();
        assert_eq!(restored.root, active.root);
        assert_eq!(execution(&setup, &restored), expected_execution);
        assert_eq!(calls.forkchoice_call_count(), 1);
    })
    .await
    .unwrap();
}
