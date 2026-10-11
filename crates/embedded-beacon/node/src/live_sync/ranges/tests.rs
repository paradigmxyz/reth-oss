//! Range batching preserves exact original ancestry, partial windows and native provenance.

use super::*;
use crate::live_sync::tests::{connect, transport};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_network::{
    availability_rpc::{ColumnIdentifier, SidecarRequest},
    transport::TransportError,
};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_state_transition::ssz::{phase0, SignedBeaconBlock, Ssz};

fn config() -> ChainConfig {
    ChainConfig {
        name: "bounded-range-test".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 12345,
        genesis_time: 0,
        genesis_validators_root: [2; 32],
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        forks: ForkSchedule::new(
            [
                Fork::Phase0,
                Fork::Altair,
                Fork::Bellatrix,
                Fork::Capella,
                Fork::Deneb,
                Fork::Electra,
                Fork::Fulu,
                Fork::Gloas,
            ]
            .into_iter()
            .enumerate()
            .map(|(index, fork)| ForkActivation {
                fork,
                epoch: if fork == Fork::Phase0 { 0 } else { u64::MAX },
                version: [index as u8, 0, 0, 0],
            })
            .collect(),
        )
        .unwrap(),
        blob_schedule: Vec::new(),
        max_payload_size: 10 * 1024 * 1024,
    }
}

fn wire(config: &ChainConfig, slot: u64, parent_root: Root) -> WireBlock {
    let mut message = phase0::BeaconBlock::default_value(Preset::Minimal).unwrap();
    message.slot = slot;
    message.parent_root = parent_root;
    WireBlock::from_ssz(
        SignedBeaconBlock::Phase0(
            phase0::SignedBeaconBlock { message, signature: [0; 96] },
            Preset::Minimal,
        )
        .to_ssz()
        .unwrap(),
        config,
    )
    .unwrap()
}

#[test]
fn catch_up_is_bounded_by_protocol_and_steady_head_stays_single_slot() {
    assert_eq!(
        catch_up_request(10, 100).unwrap(),
        BlockRequest::Range { start_slot: 10, count: 16 }
    );
    assert_eq!(catch_up_request(10, 14).unwrap(), BlockRequest::Range { start_slot: 10, count: 5 });
    assert_eq!(catch_up_request(10, 10).unwrap(), BlockRequest::Range { start_slot: 10, count: 1 });
    assert!(matches!(catch_up_request(11, 10), Err(LiveSyncError::Bounds)));
    assert!(matches!(catch_up_request(u64::MAX, u64::MAX), Err(LiveSyncError::Bounds)));
}

#[test]
fn partial_range_keeps_all_originals_and_missing_slots_without_claiming_ancestor_progress() {
    let config = config();
    let prior = wire(&config, 1, [1; 32]);
    let first = wire(&config, 3, prior.root());
    let second = wire(&config, 6, first.root());
    let peer = PeerId::random();
    let branch = BranchRecovery::from_range(peer, &[first.clone(), second.clone()]).unwrap();
    assert_eq!(branch.peer, peer);
    assert_eq!(branch.target_root, second.root());
    assert_eq!(branch.next_root, Some(prior.root()));
    assert_eq!(branch.blocks.len(), 2);
    assert_eq!(branch.bytes, first.ssz().len() + second.ssz().len());
    assert_eq!(branch.blocks[0].ssz().as_ref(), first.ssz().as_ref());
    assert_eq!(branch.blocks[1].ssz().as_ref(), second.ssz().as_ref());
    // These values are used only after the exact native import callback commits.
    assert_eq!(branch.range_next_slot_for(&first).unwrap(), Some(4));
    assert_eq!(branch.range_next_slot_for(&second).unwrap(), Some(7));
    assert_eq!(branch.range_next_slot_for(&prior).unwrap(), None);
    assert_eq!(branch.range_next_slot_for(&wire(&config, 7, second.root())).unwrap(), None);
    assert_eq!(branch.range_next_slot, Some(7));
}

#[test]
fn range_queue_rejects_reordered_unlinked_duplicate_and_oversized_windows() {
    let config = config();
    let first = wire(&config, 3, [1; 32]);
    let second = wire(&config, 6, first.root());
    let peer = PeerId::random();
    assert!(matches!(BranchRecovery::from_range(peer, &[]), Err(LiveSyncError::Bounds)));
    assert!(BranchRecovery::from_range(peer, &[second.clone(), first.clone()]).is_err());
    assert!(matches!(
        BranchRecovery::from_range(peer, &[first.clone(), first.clone()]),
        Err(LiveSyncError::Response)
    ));
    assert!(matches!(
        BranchRecovery::from_range(peer, &[first.clone(), wire(&config, 6, [8; 32])]),
        Err(LiveSyncError::Response)
    ));
    assert!(matches!(
        BranchRecovery::from_range(peer, &[first.clone(), wire(&config, 19, first.root())]),
        Err(LiveSyncError::Bounds)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tcp_partial_multi_chunk_range_keeps_exact_inputs_and_empty_window_is_untrusted() {
    let config = config();
    let first = wire(&config, 3, [1; 32]);
    let second = wire(&config, 6, first.root());
    let checkpoint = Checkpoint { epoch: 0, root: [1; 32] };
    let mut server = transport(&config, checkpoint, second.root(), 16);
    let mut follower = transport(&config, checkpoint, checkpoint.root, 0);
    tokio::time::timeout(Duration::from_secs(10), connect(&mut server, &mut follower))
        .await
        .unwrap();
    for originals in [vec![first.clone(), second.clone()], Vec::new()] {
        let request = catch_up_request(3, 16).unwrap();
        let expected = follower.request_blocks(server.peer_id(), request.clone()).unwrap();
        let batch = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = server.next_event() => if let TransportEvent::BlockRequest { id, request: observed, .. } = event {
                        assert_eq!(observed, request); server.respond_blocks(id, originals.clone()).unwrap();
                    },
                    event = follower.next_event() => if let TransportEvent::Blocks { peer, id, batch } = event {
                        assert_eq!(peer, server.peer_id()); assert_eq!(id, expected);
                        request.matches(batch.blocks()).unwrap(); break batch;
                    },
                }
            }
        }).await.unwrap();
        if originals.is_empty() {
            assert!(batch.blocks().is_empty());
            // No returned original can mint a range import or authenticated frontier.
            assert!(BranchRecovery::from_range(server.peer_id(), batch.blocks()).is_err());
        } else {
            let branch = BranchRecovery::from_range(server.peer_id(), batch.blocks()).unwrap();
            drop(batch);
            assert_eq!(branch.blocks.iter().cloned().collect::<Vec<_>>(), originals);
            assert_eq!(branch.next_root, Some(first.parent_root()));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn withheld_tcp_sidecar_owner_is_retired_before_late_response_can_be_admitted() {
    let config = config();
    let checkpoint = Checkpoint { epoch: 0, root: [1; 32] };
    let mut server = transport(&config, checkpoint, checkpoint.root, 16);
    let mut follower = transport(&config, checkpoint, checkpoint.root, 0);
    tokio::time::timeout(Duration::from_secs(10), connect(&mut server, &mut follower))
        .await
        .unwrap();
    let peer = server.peer_id();
    let request = SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
        block_root: checkpoint.root,
        columns: vec![0],
    }]);
    let original = follower.request_sidecars(peer, request.clone()).unwrap();
    let inbound = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = server.next_event() => if let TransportEvent::SidecarRequest { id, request: received, .. } = event {
                    assert_eq!(received, request); break id;
                },
                event = follower.next_event() => assert!(!matches!(event, TransportEvent::Sidecars { .. })),
            }
        }
    }).await.unwrap();
    // Leave the exact server stream unanswered, then invoke the same transport retirement
    // used by the live owner's original-job deadline. No replacement request is issued.
    follower.retire_peer(peer);
    assert!(matches!(follower.request_sidecars(peer, request), Err(TransportError::Configuration)));
    let late = server.respond_sidecars(inbound, Vec::new());
    assert!(late.is_ok() || matches!(late, Err(TransportError::Configuration)));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                _ = server.next_event() => {},
                event = follower.next_event() => match event {
                    TransportEvent::Closed(closed) if closed == peer => break,
                    TransportEvent::Sidecars { id, .. } => panic!("retired original {original:?} admitted late response {id:?}"),
                    _ => {},
                },
            }
        }
    }).await.unwrap();
    assert!(matches!(
        server.respond_sidecars(inbound, Vec::new()),
        Err(TransportError::Configuration)
    ));
}
