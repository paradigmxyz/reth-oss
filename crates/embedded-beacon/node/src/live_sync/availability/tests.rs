//! Real TCP/Noise sidecar retrieval and real KZG checks; no Engine or live-chain credit.

use super::*;
use blst::min_pk::SecretKey;
use c_kzg::{Blob, Bytes48, BYTES_PER_BLOB};
use libp2p::{identity::Keypair, Multiaddr};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_network::{
    availability_rpc::WireSidecar,
    transport::{StatusTransport, TransportEvent},
    Protocol, Status, StatusCompatibility, StatusV2,
};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_state_transition::{
    light_client::merkle::compute_merkle_proof,
    ssz::{deneb as deneb_schema, List, SignedBeaconBlock, Ssz, Vector},
};

pub(super) fn config() -> ChainConfig {
    ChainConfig {
        name: "actual-retrieval-test".into(),
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
                epoch: if fork <= Fork::Deneb { 0 } else { u64::MAX },
                version: [index as u8, 0, 0, 0],
            })
            .collect(),
        )
        .unwrap(),
        blob_schedule: Vec::new(),
        max_payload_size: 10 * 1024 * 1024,
    }
}
pub(super) fn candidate(config: &ChainConfig) -> (WireBlock, BeaconBlock, deneb::BlobSidecar) {
    let settings = ethereum_kzg_settings(0);
    let blob = Blob::new([0; BYTES_PER_BLOB]);
    let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes().into_inner();
    let proof = settings
        .compute_blob_kzg_proof(&blob, &Bytes48::new(commitment))
        .unwrap()
        .to_bytes()
        .into_inner();
    let mut message = deneb_schema::BeaconBlock::default_value(Preset::Minimal).unwrap();
    message.slot = 1;
    message.parent_root = [1; 32];
    message.state_root = [7; 32];
    message.body.blob_kzg_commitments = List::from_vec(vec![commitment]);
    let header = message.header(Preset::Minimal).unwrap();
    let key = SecretKey::key_gen(&[4; 32], &[]).unwrap();
    let signature = key
        .sign(
            &compute_signing_root(
                header.hash_tree_root(),
                compute_domain(
                    [0; 4],
                    config.fork_at_slot(1).unwrap().version,
                    config.genesis_validators_root,
                ),
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let inclusion = compute_merkle_proof(&message.body, 27 << 13, Preset::Minimal).unwrap();
    let sidecar = deneb::BlobSidecar {
        index: 0,
        blob: deneb::Blob::from_vec(blob.as_ref().to_vec()),
        kzg_commitment: commitment,
        kzg_proof: proof,
        signed_block_header: SignedBeaconBlockHeader { message: header, signature },
        kzg_commitment_inclusion_proof: Vector::from_vec(inclusion),
    };
    let signed = SignedBeaconBlock::Deneb(
        deneb_schema::SignedBeaconBlock { message: message.clone(), signature },
        Preset::Minimal,
    );
    let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), config).unwrap();
    (wire, BeaconBlock::Deneb(message, Preset::Minimal), sidecar)
}
pub(super) fn transport(config: &ChainConfig) -> StatusTransport {
    let checkpoint = Checkpoint { epoch: 0, root: [1; 32] };
    StatusTransport::new(
        Keypair::generate_secp256k1(),
        config.clone(),
        1,
        checkpoint,
        vec![(0, checkpoint.root)],
        StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(0).unwrap(),
                finalized_root: checkpoint.root,
                finalized_epoch: 0,
                head_root: [3; 32],
                head_slot: 1,
            },
            earliest_available_slot: 0,
        },
    )
    .unwrap()
}
pub(super) async fn connect(listener: &mut StatusTransport, dialer: &mut StatusTransport) {
    listener.listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap()).unwrap();
    let TransportEvent::Listening(address) = listener.next_event().await else {
        panic!("actual TCP listener missing")
    };
    dialer.dial(listener.peer_id(), address).unwrap();
    let mut left = false;
    let mut right = false;
    while !left || !right {
        tokio::select! {
            event = listener.next_event() => if let TransportEvent::PeerStatus { compatibility, .. } = event {
                assert_eq!(compatibility, StatusCompatibility::KnownFinality); left = true;
            },
            event = dialer.next_event() => if let TransportEvent::PeerStatus { compatibility, .. } = event {
                assert_eq!(compatibility, StatusCompatibility::KnownFinality); right = true;
            },
        }
    }
}

async fn actual_sidecar_roundtrip(
    mut mutate: impl FnMut(&mut deneb::BlobSidecar),
) -> Result<(), RetrievalError> {
    let config = config();
    let (wire, block, mut sidecar) = candidate(&config);
    mutate(&mut sidecar);
    let original = WireSidecar::from_ssz(
        sidecar.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::BlobSidecarsByRootV1,
        &config,
    )
    .unwrap();
    let store_identity = reth_embedded_beacon_shared_data::StoreIdentity {
        genesis_root: config.genesis_validators_root,
        config_digest: [219; 32],
    };
    let (_directory, shared) = tokio::task::spawn_blocking(move || {
        let directory = tempfile::TempDir::new().unwrap();
        let shared = reth_embedded_beacon_shared_data::SharedDataStore::open(
            directory.path(),
            store_identity,
            0,
        )
        .unwrap();
        (directory, shared)
    })
    .await
    .unwrap();
    let (availability, mut broker) =
        RetrievalAvailability::with_shared_store(config.clone(), shared).unwrap();
    // This isolated blob proof control does not advertise or claim discovery custody.
    availability.bind_peer_identity([0; 32]).unwrap();
    let _candidate_guard = availability.bind_candidate(&wire).unwrap();
    let mut listener = transport(&config);
    let mut dialer = transport(&config);
    tokio::time::timeout(Duration::from_secs(10), connect(&mut listener, &mut dialer))
        .await
        .unwrap();
    let proof = ClassicAvailability::verify(&availability, wire.root(), &block);
    tokio::pin!(proof);
    let mut pending = None;
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            tokio::select! {
                result = &mut proof => return result,
                job = broker.receiver.recv(), if pending.is_none() => {
                    let job = job.unwrap();
                    let id = dialer.request_sidecars(listener.peer_id(), job.request.clone()).unwrap();
                    pending = Some((id, job));
                },
                event = listener.next_event() => if let TransportEvent::SidecarRequest { id, request, .. } = event {
                    assert_eq!(request, SidecarRequest::BlobRoots(vec![BlobIdentifier { block_root: wire.root(), index: 0 }]));
                    listener.respond_sidecars(id, vec![original.clone()]).unwrap();
                },
                event = dialer.next_event() => if let TransportEvent::Sidecars { id, batch, .. } = event {
                    let (expected, job) = pending.take().unwrap(); assert_eq!(id, expected);
                    job.request.matches(&config, &batch).unwrap();
                    job.reply.send(Ok(batch)).unwrap();
                },
            }
        }
    }).await.unwrap();
    let archive = availability.archive.clone().unwrap();
    let root = wire.root();
    let slot = wire.slot();
    let archived = tokio::task::spawn_blocking(move || archive.read(root, slot, 0, false))
        .await
        .unwrap()
        .unwrap();
    // This genuine KZG input still lacks strict signed state/Engine admission: no durable
    // candidate pin or archive record is authorized by mathematical proof alone.
    assert!(archived.is_none());
    if result.is_ok() {
        assert_eq!(availability.staged_bytes(), original.ssz().len());
    } else {
        assert_eq!(availability.staged_bytes(), 0);
    }
    drop(_candidate_guard);
    assert_eq!(availability.staged_bytes(), 0);
    assert!(!availability.is_halted());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_tcp_blob_retrieval_requires_original_signature_inclusion_and_real_kzg() {
    assert_eq!(actual_sidecar_roundtrip(|_| {}).await, Ok(()));
    assert_eq!(
        actual_sidecar_roundtrip(|sidecar| sidecar.signed_block_header.signature[0] ^= 1).await,
        Err(RetrievalError::Association)
    );
    assert_eq!(
        actual_sidecar_roundtrip(|sidecar| sidecar.kzg_proof[0] ^= 1).await,
        Err(RetrievalError::Proof)
    );
    assert_eq!(actual_sidecar_roundtrip(|sidecar| sidecar.kzg_commitment_inclusion_proof.values[0][0] ^= 1).await,
        Err(RetrievalError::Proof));
}

#[tokio::test]
async fn cancelled_reply_keeps_actual_request_admission_until_job_is_released() {
    let config = config();
    let (wire, _, _) = candidate(&config);
    let (availability, mut broker) = RetrievalAvailability::new(config).unwrap();
    let _candidate_guard = availability.bind_candidate(&wire).unwrap();
    let request =
        SidecarRequest::BlobRoots(vec![BlobIdentifier { block_root: wire.root(), index: 0 }]);
    let reply = availability.request(request.clone());
    assert_eq!(availability.requests.available_permits(), 0);
    assert_eq!(availability.clone().observation().retrieval_jobs, 1);
    drop(reply);
    assert_eq!(availability.requests.available_permits(), 0);
    assert_eq!(availability.observation().retrieval_jobs, 1);
    let second = availability.request(request);
    assert!(matches!(second.await, Err(RetrievalError::Capacity)));
    let job = broker.receiver.recv().await.unwrap();
    drop(job);
    assert_eq!(availability.requests.available_permits(), 1);
    assert_eq!(availability.observation().retrieval_jobs, 0);
}

#[tokio::test]
async fn empty_commitments_prove_only_exact_no_data_requirement_and_foreign_root_rejects() {
    let config = config();
    let (wire, mut block, _) = candidate(&config);
    let (availability, _) = RetrievalAvailability::new(config).unwrap();
    let _candidate_guard = availability.bind_candidate(&wire).unwrap();
    assert_eq!(
        ClassicAvailability::verify(&availability, [9; 32], &block).await,
        Err(RetrievalError::Association)
    );
    if let BeaconBlock::Deneb(value, _) = &mut block {
        value.body.blob_kzg_commitments.values.clear();
    }
    assert_eq!(
        ClassicAvailability::verify(&availability, wire.root(), &block).await,
        Err(RetrievalError::Association)
    );
}
