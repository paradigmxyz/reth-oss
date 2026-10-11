//! Real column proofs and TCP partial responses exercise complete sampling without lease stacking.

use super::{
    super::tests::{config as base_config, connect, transport},
    *,
};
use c_kzg::{Blob, BYTES_PER_BLOB};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_network::{transport::TransportEvent, Protocol};
use reth_embedded_beacon_state_transition::{
    light_client::merkle::compute_merkle_proof,
    ssz::{
        fulu as fulu_schema, gloas as gloas_schema, List, ProgressiveList, SignedBeaconBlock, Ssz,
        Vector,
    },
};

fn fixture(fork: Fork) -> (ChainConfig, WireBlock, BeaconBlock, Vec<WireSidecar>) {
    let mut config = base_config();
    config.forks = ForkSchedule::new(
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
        .map(|(index, current)| ForkActivation {
            fork: current,
            epoch: if current <= fork { 0 } else { u64::MAX },
            version: [index as u8, 0, 0, 0],
        })
        .collect(),
    )
    .unwrap();
    let selection = sampling::SamplingSelection::default();
    selection.bind([0; 32]).unwrap();
    let columns = selection.columns().unwrap();
    let settings = ethereum_kzg_settings(0);
    let blob = Blob::new([0; BYTES_PER_BLOB]);
    let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes().into_inner();
    let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
    let signature = [35; 96];
    let signed = match fork {
        Fork::Fulu => {
            let mut block = fulu_schema::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.slot = 1;
            block.parent_root = [1; 32];
            block.state_root = [7; 32];
            block.body.blob_kzg_commitments = List::from_vec(vec![commitment]);
            SignedBeaconBlock::Fulu(
                fulu_schema::SignedBeaconBlock { message: block, signature },
                Preset::Minimal,
            )
        }
        Fork::Gloas => {
            let mut block = gloas_schema::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.slot = 1;
            block.parent_root = [1; 32];
            block.state_root = [7; 32];
            block.body.signed_execution_payload_bid.message.blob_kzg_commitments =
                ProgressiveList::from_vec(vec![commitment]);
            SignedBeaconBlock::Gloas(
                gloas_schema::SignedBeaconBlock { message: block, signature },
                Preset::Minimal,
            )
        }
        _ => panic!("column fixture requires Fulu or Gloas"),
    };
    let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
    let block = signed.message();
    let originals = columns
        .into_iter()
        .map(|index| {
            let bytes = match &block {
                BeaconBlock::Fulu(block, _) => fulu::DataColumnSidecar {
                    index,
                    column: List::from_vec(vec![Vector::from_vec(
                        cells[index as usize].to_bytes().to_vec(),
                    )]),
                    kzg_commitments: block.body.blob_kzg_commitments.clone(),
                    kzg_proofs: List::from_vec(vec![proofs[index as usize]
                        .to_bytes()
                        .into_inner()]),
                    signed_block_header: SignedBeaconBlockHeader {
                        message: block.header(Preset::Minimal).unwrap(),
                        signature,
                    },
                    kzg_commitments_inclusion_proof: Vector::from_vec(
                        compute_merkle_proof(&block.body, 27, Preset::Minimal).unwrap(),
                    ),
                }
                .to_ssz(Preset::Minimal)
                .unwrap(),
                BeaconBlock::Gloas(block, _) => gloas::DataColumnSidecar {
                    index,
                    column: ProgressiveList::from_vec(vec![Vector::from_vec(
                        cells[index as usize].to_bytes().to_vec(),
                    )]),
                    kzg_proofs: ProgressiveList::from_vec(vec![proofs[index as usize]
                        .to_bytes()
                        .into_inner()]),
                    slot: block.slot,
                    beacon_block_root: wire.root(),
                }
                .to_ssz(Preset::Minimal)
                .unwrap(),
                _ => unreachable!(),
            };
            WireSidecar::from_ssz(bytes, wire.context(), Protocol::DataColumnsByRootV1, &config)
                .unwrap()
        })
        .collect();
    (config, wire, block, originals)
}

fn bounded_fixture(fork: Fork) -> (ChainConfig, WireBlock, BeaconBlock, Vec<WireSidecar>) {
    // KZG's complete cell generation has large inline debug temporaries.
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || fixture(fork))
        .unwrap()
        .join()
        .unwrap()
}

fn changed_original(
    config: &ChainConfig,
    original: &WireSidecar,
    mutate: impl FnOnce(&mut fulu::DataColumnSidecar),
) -> WireSidecar {
    let mut sidecar = fulu::DataColumnSidecar::from_ssz(original.ssz(), Preset::Minimal).unwrap();
    mutate(&mut sidecar);
    WireSidecar::from_ssz(
        sidecar.to_ssz(Preset::Minimal).unwrap(),
        original.context(),
        Protocol::DataColumnsByRootV1,
        config,
    )
    .unwrap()
}

#[test]
fn partial_fulu_proofs_require_original_signature_inclusion_and_every_selected_sample() {
    let (config, wire, block, originals) = bounded_fixture(Fork::Fulu);
    let verifier = ColumnVerifier {
        config: Arc::new(config.clone()),
        candidate: Candidate { root: wire.root(), slot: wire.slot(), signature: Some([35; 96]) },
        block,
    };
    let required = originals.iter().map(WireSidecar::index).collect::<Vec<_>>();
    let mut progress = ColumnProgress::new(required.clone()).unwrap();
    let checked = verifier.verify(&originals[..1], &required).unwrap();
    progress.record(&checked).unwrap();
    assert_eq!(progress.remaining(), required[1..]);
    let expired = Instant::now() - ATTEMPT_TIMEOUT - Duration::from_secs(1);
    assert_eq!(progress.check_completion(wire.root(), expired), Err(RetrievalError::Missing));
    assert_eq!(verifier.verify(&[], &progress.remaining()), Ok(Vec::new()));
    assert_eq!(
        verifier.verify(&originals[..1], &progress.remaining()),
        Err(RetrievalError::Association)
    );
    assert_eq!(
        verifier.verify(&[originals[1].clone(), originals[1].clone()], &progress.remaining()),
        Err(RetrievalError::Association)
    );
    let bad_signature = changed_original(&config, &originals[1], |sidecar| {
        sidecar.signed_block_header.signature[0] ^= 1
    });
    assert_eq!(
        verifier.verify(&[bad_signature], &progress.remaining()),
        Err(RetrievalError::Association)
    );
    let bad_inclusion = changed_original(&config, &originals[1], |sidecar| {
        sidecar.kzg_commitments_inclusion_proof.values[0][0] ^= 1
    });
    assert_eq!(
        verifier.verify(&[bad_inclusion], &progress.remaining()),
        Err(RetrievalError::Proof)
    );
    let bad_kzg =
        changed_original(&config, &originals[1], |sidecar| sidecar.kzg_proofs.values[0][0] ^= 1);
    assert_eq!(verifier.verify(&[bad_kzg], &progress.remaining()), Err(RetrievalError::Proof));
    assert_eq!(progress.remaining(), required[1..]);
    for original in &originals[1..] {
        let checked =
            verifier.verify(std::slice::from_ref(original), &progress.remaining()).unwrap();
        progress.record(&checked).unwrap();
    }
    assert_eq!(progress.remaining(), Vec::<u64>::new());
    // Genuine completion still succeeds when the last proof crossed the attempt deadline.
    assert_eq!(progress.check_completion(wire.root(), expired), Ok(true));
    assert_eq!(ColumnProgress::new(required.clone()).unwrap().remaining(), required);
}

#[test]
fn partial_gloas_proofs_bind_columns_to_actual_candidate_bid_and_reject_corrupt_kzg() {
    let (config, wire, block, originals) = bounded_fixture(Fork::Gloas);
    let verifier = ColumnVerifier {
        config: Arc::new(config.clone()),
        candidate: Candidate { root: wire.root(), slot: wire.slot(), signature: Some([35; 96]) },
        block,
    };
    let required = originals.iter().map(WireSidecar::index).collect::<Vec<_>>();
    assert_eq!(verifier.verify(&originals[..1], &required), Ok(vec![required[0]]));
    let mut sidecar =
        gloas::DataColumnSidecar::from_ssz(originals[0].ssz(), Preset::Minimal).unwrap();
    sidecar.kzg_proofs.values[0][0] ^= 1;
    let corrupt = WireSidecar::from_ssz(
        sidecar.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::DataColumnsByRootV1,
        &config,
    )
    .unwrap();
    assert_eq!(verifier.verify(&[corrupt], &required), Err(RetrievalError::Proof));
    sidecar.beacon_block_root = [8; 32];
    let foreign = WireSidecar::from_ssz(
        sidecar.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::DataColumnsByRootV1,
        &config,
    )
    .unwrap();
    assert_eq!(verifier.verify(&[foreign], &required), Err(RetrievalError::Association));
    let mut changed = verifier;
    if let BeaconBlock::Gloas(block, _) = &mut changed.block {
        block.body.signed_execution_payload_bid.message.blob_kzg_commitments.values[0][0] ^= 1;
    }
    assert_eq!(changed.verify(&originals[..1], &required), Err(RetrievalError::Proof));
}

async fn partial_roundtrip(fork: Fork, empty: bool) -> (Result<(), RetrievalError>, usize) {
    let (config, wire, block, originals) = bounded_fixture(fork);
    let (availability, mut broker) = RetrievalAvailability::new(config.clone()).unwrap();
    availability.bind_peer_identity([0; 32]).unwrap();
    let _candidate = availability.bind_candidate(&wire).unwrap();
    let required = availability.sampling.columns().unwrap();
    let mut listener = transport(&config);
    let mut dialer = transport(&config);
    tokio::time::timeout(Duration::from_secs(10), connect(&mut listener, &mut dialer))
        .await
        .unwrap();
    let proof = async {
        match &block {
            BeaconBlock::Fulu(_, _) => {
                ClassicAvailability::verify(&availability, wire.root(), &block).await
            }
            BeaconBlock::Gloas(block, _) => {
                GloasAvailability::verify(&availability, wire.root(), block).await
            }
            _ => unreachable!(),
        }
    };
    tokio::pin!(proof);
    let mut pending = None;
    let mut responses = 0;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                result = &mut proof => return result,
                job = broker.receiver.recv(), if pending.is_none() => {
                    let job = job.unwrap();
                    assert_eq!(availability.requests.available_permits(), 0);
                    let id = dialer.request_sidecars(listener.peer_id(), job.request.clone()).unwrap();
                    pending = Some((id, job));
                },
                event = listener.next_event() => if let TransportEvent::SidecarRequest { id, request, .. } = event {
                    let remaining = if empty { &required[..] } else { &required[responses..] };
                    assert_eq!(request, SidecarRequest::ColumnRoots(vec![ColumnIdentifier { block_root: wire.root(), columns: remaining.to_vec() }]));
                    let sidecars = if empty { Vec::new() } else { vec![originals[responses].clone()] };
                    listener.respond_sidecars(id, sidecars).unwrap();
                    responses += 1;
                },
                event = dialer.next_event() => if let TransportEvent::Sidecars { id, batch, .. } = event {
                    let (expected, job) = pending.take().unwrap();
                    assert_eq!(id, expected);
                    job.request.matches(&config, &batch).unwrap();
                    job.complete(Ok(batch));
                },
            }
        }
    }).await.unwrap();
    assert_eq!(availability.requests.available_permits(), 1);
    assert_eq!(availability.proofs.available_permits(), 1);
    assert!(!availability.is_halted());
    (result, responses)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_partial_tcp_responses_complete_fulu_and_gloas_without_retaining_transport_leases() {
    assert_eq!(partial_roundtrip(Fork::Fulu, false).await, (Ok(()), MAX_SAMPLES));
    assert_eq!(partial_roundtrip(Fork::Gloas, false).await, (Ok(()), MAX_SAMPLES));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_partial_tcp_responses_end_boundedly_without_asserting_availability() {
    assert_eq!(
        partial_roundtrip(Fork::Fulu, true).await,
        (Err(RetrievalError::Missing), MAX_NO_PROGRESS)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_originals_survive_retry_but_every_attempt_repeats_native_proofs() {
    let (config, wire, block, originals) = bounded_fixture(Fork::Fulu);
    let (availability, _) = RetrievalAvailability::new(config.clone()).unwrap();
    availability.bind_peer_identity([0; 32]).unwrap();
    let guard = availability.bind_candidate(&wire).unwrap();
    let candidate = availability.candidate(wire.root(), wire.slot()).unwrap();
    let verifier = Arc::new(ColumnVerifier { config: Arc::new(config.clone()), candidate, block });
    let required = availability.sampling.columns().unwrap();
    verifier.verify(&originals[..4], &required).unwrap();
    availability.stage_verified_originals(&originals[..4]).unwrap();
    assert_eq!(
        availability.staged_bytes(),
        originals[..4].iter().map(|wire| wire.ssz().len()).sum::<usize>()
    );
    let mut next_attempt = ColumnProgress::new(required.clone()).unwrap();
    let reproved =
        availability.prove_retained_columns(verifier.clone(), required.clone()).await.unwrap();
    assert_eq!(reproved, required[..4]);
    next_attempt.record(&reproved).unwrap();
    assert_eq!(next_attempt.remaining(), required[4..]);

    // Retained bytes are not trusted validity. A changed actual candidate body is rejected
    // by native root/commitment verification instead of replaying a prior checked-index bit.
    let mut changed_block = verifier.block.clone();
    if let BeaconBlock::Fulu(block, _) = &mut changed_block {
        block.body.blob_kzg_commitments.values[0][0] ^= 1;
    }
    let changed = Arc::new(ColumnVerifier {
        config: verifier.config.clone(),
        candidate,
        block: changed_block,
    });
    assert_eq!(
        availability.prove_retained_columns(changed, required.clone()).await,
        Err(RetrievalError::Proof)
    );
    assert_eq!(availability.proofs.available_permits(), 1);
    assert_eq!(next_attempt.remaining(), required[4..]);
    let foreign = Candidate { root: [8; 32], ..candidate };
    assert!(matches!(
        availability.staged_columns(foreign, &required),
        Err(RetrievalError::Association)
    ));

    let newly_checked = verifier.verify(&originals[4..], &next_attempt.remaining()).unwrap();
    availability.stage_verified_originals(&originals[4..]).unwrap();
    next_attempt.record(&newly_checked).unwrap();
    let expired = Instant::now() - ATTEMPT_TIMEOUT - Duration::from_secs(1);
    assert_eq!(next_attempt.check_completion(wire.root(), expired), Ok(true));
    assert_eq!(
        availability.prove_retained_columns(verifier, required.clone()).await.unwrap(),
        required
    );
    drop(guard);
    assert_eq!(availability.staged_bytes(), 0);
    let _next_guard = availability.bind_candidate(&wire).unwrap();
    assert!(availability.staged_columns(candidate, &required).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_sampling_batch_reproves_a_corrupt_last_original_without_reusing_valid_prefix() {
    let (config, wire, block, originals) = bounded_fixture(Fork::Fulu);
    let (availability, _) = RetrievalAvailability::new(config.clone()).unwrap();
    availability.bind_peer_identity([0; 32]).unwrap();
    let _guard = availability.bind_candidate(&wire).unwrap();
    let candidate = availability.candidate(wire.root(), wire.slot()).unwrap();
    let verifier = Arc::new(ColumnVerifier { config: Arc::new(config.clone()), candidate, block });
    let required = availability.sampling.columns().unwrap();
    verifier.verify(&originals, &required).unwrap();
    availability.stage_verified_originals(&originals).unwrap();
    assert_eq!(
        availability.prove_retained_columns(verifier.clone(), required.clone()).await.unwrap(),
        required
    );
    let last = originals.last().unwrap();
    let corrupt = changed_original(&config, last, |sidecar| sidecar.kzg_proofs.values[0][0] ^= 1);
    // Inject changed persisted inputs without changing the candidate or the valid prefix.
    // An earlier successful attempt cannot confer authority on this new exact byte batch.
    let mut replaced = originals.clone();
    *replaced.last_mut().unwrap() = corrupt;
    availability.staging.lock().unwrap().clear();
    availability.stage_verified_originals(&replaced).unwrap();
    assert_eq!(
        availability.prove_retained_columns(verifier, required).await,
        Err(RetrievalError::Proof)
    );
    assert_eq!(availability.proofs.available_permits(), 1);
    assert_eq!(availability.requests.available_permits(), 1);
}
