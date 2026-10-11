//! Native Gloas column proof, duplicate identity and publication-backpressure controls.
//!
//! Real KZG cells and an actual authenticated native anchor provide mathematical/contextual
//! evidence. Controlled Engine replies here confer no live execution or public-network credit.

use super::{gloas_gossip::genuine_gloas_setup, *};
use c_kzg::{ethereum_kzg_settings, Blob, BYTES_PER_BLOB};
use reth_embedded_beacon_network::{
    availability_rpc::WireSidecar,
    gossip::{GossipKind, GossipTopic},
    transport::GossipVerdict,
    Protocol,
};
use reth_embedded_beacon_state_transition::ssz::{auxiliary, ProgressiveList};

#[derive(Debug)]
struct RecordingColumns {
    digest: Root,
    busy: AtomicBool,
    original: Mutex<Vec<Vec<u8>>>,
}
impl ColumnRetention for RecordingColumns {
    fn config_digest(&self) -> Root {
        self.digest
    }
    fn retain(&self, receipt: &ValidatedColumn) -> Result<(), ImportError> {
        assert_eq!(receipt.config_digest(), self.digest);
        if self.busy.swap(false, Ordering::AcqRel) {
            return Err(ImportError::Backpressure)
        }
        self.original.lock().unwrap().push(receipt.original().ssz().to_vec());
        Ok(())
    }
}

pub(super) fn setup_columns(
    directory: &TempDir,
) -> (
    WorkerSetup<BackendHandle, MissingAvailability, MissingAvailability>,
    Arc<Backend>,
    auxiliary::gloas::DataColumnSidecar,
) {
    let (mut setup, backend, _keys) = genuine_gloas_setup(directory);
    let config = setup.config.clone();
    let BeaconState::Gloas(mut state, _) =
        BeaconState::from_ssz(&setup.anchor.state_ssz, Fork::Gloas, Preset::Minimal).unwrap()
    else {
        panic!("genuine upgraded native Gloas state")
    };
    let BeaconBlock::Gloas(mut block, _) =
        BeaconBlock::from_ssz(&setup.anchor.block_ssz, Fork::Gloas, Preset::Minimal).unwrap()
    else {
        panic!("complete native Gloas block")
    };
    let settings = ethereum_kzg_settings(0);
    let blob = Blob::new([0; BYTES_PER_BLOB]);
    let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes().into_inner();
    let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
    block.body.signed_execution_payload_bid.message.blob_kzg_commitments =
        ProgressiveList::from_vec(vec![commitment]);
    state.latest_execution_payload_bid.blob_kzg_commitments =
        ProgressiveList::from_vec(vec![commitment]);
    state.latest_block_header.body_root = block.body.hash_tree_root(Preset::Minimal).unwrap();
    block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let root = block.hash_tree_root(Preset::Minimal).unwrap();
    let state_ssz = Arc::<[u8]>::from(state.to_ssz(Preset::Minimal).unwrap());
    let block_ssz = Arc::<[u8]>::from(block.to_ssz(Preset::Minimal).unwrap());
    let verified = SszCheckpointVerifier::new(
        config,
        setup.anchor.decode_limits,
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap()
    .verify_checkpoint(&state_ssz, &block_ssz, Checkpoint { epoch: 0, root }, 0, 1)
    .unwrap();
    setup.anchor.verified = verified;
    setup.anchor.state_ssz = state_ssz;
    setup.anchor.block_ssz = block_ssz;
    let sidecar = auxiliary::gloas::DataColumnSidecar {
        index: 3,
        column: ProgressiveList::from_vec(vec![Vector::from_vec(cells[3].to_bytes().to_vec())]),
        kzg_proofs: ProgressiveList::from_vec(vec![proofs[3].to_bytes().into_inner()]),
        slot: block.slot,
        beacon_block_root: root,
    };
    (setup, backend, sidecar)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_columns_require_real_math_and_retry_local_publication_without_seen_poisoning() {
    let directory = TempDir::new().unwrap();
    let (setup, backend, sidecar) = setup_columns(&directory);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let snapshot = worker.graph.snapshot().unwrap();
        let publisher = Arc::new(RecordingColumns {
            digest: checked_config_digest(&config).unwrap(),
            busy: AtomicBool::new(true),
            original: Mutex::new(Vec::new()),
        });
        worker.column_retention = Some(publisher.clone());
        let topic = GossipTopic {
            fork: Fork::Gloas,
            digest: config.fork_digest(0).unwrap(),
            kind: GossipKind::Column(3),
        };
        let mut bad = sidecar.clone();
        bad.kzg_proofs.values[0][0] ^= 1;
        assert_eq!(
            worker.dispatch_gossip(topic, &bad.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Reject
        );
        let bytes = sidecar.to_ssz(Preset::Minimal).unwrap();
        assert!(matches!(worker.dispatch_gossip(topic, &bytes, 0), Err(ImportError::Backpressure)));
        assert!(publisher.original.lock().unwrap().is_empty());
        assert_eq!(worker.dispatch_gossip(topic, &bytes, 0).unwrap(), GossipVerdict::Accept);
        assert_eq!(publisher.original.lock().unwrap().as_slice(), &[bytes]);
        let mut changed = sidecar.clone();
        changed.slot = 1;
        assert_eq!(
            worker.dispatch_gossip(topic, &changed.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Ignore
        );
        let wrong_subnet = GossipTopic { kind: GossipKind::Column(4), ..topic };
        assert_eq!(
            worker
                .dispatch_gossip(wrong_subnet, &sidecar.to_ssz(Preset::Minimal).unwrap(), 0)
                .unwrap(),
            GossipVerdict::Ignore
        );
        assert_eq!(worker.graph.snapshot().unwrap(), snapshot);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_unsigned_anchor_columns_revalidate_without_minting_graph_or_seen_authority() {
    let directory = TempDir::new().unwrap();
    let (setup, backend, sidecar) = setup_columns(&directory);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let worker = Worker::new(setup, Ok(0)).unwrap();
        let snapshot = worker.graph.snapshot().unwrap();
        let publisher = Arc::new(RecordingColumns {
            digest: checked_config_digest(&config).unwrap(),
            busy: AtomicBool::new(false),
            original: Mutex::new(Vec::new()),
        });
        let mut worker = worker;
        worker.column_retention = Some(publisher.clone());
        let wire = WireSidecar::from_ssz(
            sidecar.to_ssz(Preset::Minimal).unwrap(),
            config.fork_digest(0).unwrap(),
            Protocol::DataColumnsByRootV1,
            &config,
        )
        .unwrap();
        worker.retain_custody_columns(sidecar.beacon_block_root, vec![wire.clone()]).unwrap();
        assert_eq!(publisher.original.lock().unwrap().as_slice(), &[wire.ssz().to_vec()]);
        let topic = GossipTopic {
            fork: Fork::Gloas,
            digest: config.fork_digest(0).unwrap(),
            kind: GossipKind::Column(sidecar.index),
        };
        assert_eq!(
            worker.dispatch_gossip(topic, &sidecar.to_ssz(Preset::Minimal).unwrap(), 0).unwrap(),
            GossipVerdict::Accept
        );
        assert_eq!(worker.graph.snapshot().unwrap(), snapshot);
        assert!(worker
            .repository
            .original_block(snapshot.head.root)
            .unwrap()
            .is_some_and(|(format, _)| format == CheckpointBlockFormat::Unsigned));
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}
