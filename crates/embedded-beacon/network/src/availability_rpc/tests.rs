//! Structural wire checks and shared resource admission; no signature or KZG authority.
use super::*;
use crate::{
    block_rpc::WireBlock,
    envelope_rpc::{EnvelopeCodec, EnvelopeReply},
};
use futures::{io::Cursor, FutureExt};
use reth_embedded_beacon_config::{BlobParameters, ForkActivation, ForkSchedule};
use reth_embedded_beacon_state_transition::ssz::{fulu as fulu_block, Ssz};
use std::{
    pin::Pin,
    sync::mpsc,
    task::{Context, Poll},
};
use tokio::time::{timeout, Duration};

pub(crate) fn config() -> ChainConfig {
    let mut config = ChainConfig::hoodi();
    config.preset = ConfigPreset::Minimal;
    config.slots_per_epoch = 8;
    config.genesis_validators_root = [42; 32];
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
        .map(|(index, fork)| ForkActivation {
            fork,
            epoch: index as u64,
            version: [index as u8, 0, 0, 42],
        })
        .collect(),
    )
    .unwrap();
    config.blob_schedule.clear();
    config.validate().unwrap();
    config
}

pub(crate) const fn slot(fork: Fork) -> u64 {
    match fork {
        Fork::Deneb => 33,
        Fork::Electra => 41,
        Fork::Fulu => 49,
        Fork::Gloas => 57,
        _ => panic!("sidecar schema required"),
    }
}

pub(crate) const fn protocol(fork: Fork, roots: bool) -> Protocol {
    match (fork, roots) {
        (Fork::Deneb | Fork::Electra, false) => Protocol::BlobSidecarsByRangeV1,
        (Fork::Deneb | Fork::Electra, true) => Protocol::BlobSidecarsByRootV1,
        (Fork::Fulu | Fork::Gloas, false) => Protocol::DataColumnsByRangeV1,
        (Fork::Fulu | Fork::Gloas, true) => Protocol::DataColumnsByRootV1,
        _ => panic!("sidecar schema required"),
    }
}

/// Complete structural objects deliberately carry no verified BLS/inclusion/KZG receipts.
pub(crate) fn sidecar_bytes(fork: Fork, index: u64, marker: u8) -> Vec<u8> {
    match fork {
        Fork::Deneb => {
            let mut sidecar = deneb::BlobSidecar::default_value(Preset::Minimal).unwrap();
            sidecar.index = index;
            sidecar.signed_block_header.message.slot = slot(fork);
            sidecar.signed_block_header.message.body_root = [marker; 32];
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Electra => {
            let mut sidecar = electra::BlobSidecar::default_value(Preset::Minimal).unwrap();
            sidecar.index = index;
            sidecar.signed_block_header.message.slot = slot(fork);
            sidecar.signed_block_header.message.body_root = [marker; 32];
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Fulu => {
            let mut sidecar = fulu::DataColumnSidecar::default_value(Preset::Minimal).unwrap();
            sidecar.index = index;
            sidecar.signed_block_header.message.slot = slot(fork);
            sidecar.signed_block_header.message.body_root = [marker; 32];
            sidecar.column.values.push(fulu::Cell::default_value(Preset::Minimal).unwrap());
            sidecar.kzg_commitments.values.push([0; 48]);
            sidecar.kzg_proofs.values.push([0; 48]);
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Gloas => {
            let mut sidecar = gloas::DataColumnSidecar::default_value(Preset::Minimal).unwrap();
            sidecar.index = index;
            sidecar.slot = slot(fork);
            sidecar.beacon_block_root = [marker; 32];
            sidecar.column.values.push(gloas::Cell::default_value(Preset::Minimal).unwrap());
            sidecar.kzg_proofs.values.push([0; 48]);
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        _ => panic!("sidecar schema required"),
    }
}

pub(crate) fn wire_sidecar(fork: Fork, index: u64, marker: u8) -> WireSidecar {
    let config = config();
    WireSidecar::from_ssz(
        sidecar_bytes(fork, index, marker),
        config.fork_digest(slot(fork) / config.slots_per_epoch).unwrap(),
        protocol(fork, false),
        &config,
    )
    .unwrap()
}

fn codec() -> (BlockCodec, SidecarCodec) {
    let config = Arc::new(config());
    let blocks = BlockCodec::new(config.clone());
    let sidecars = SidecarCodec::new(config, &blocks);
    (blocks, sidecars)
}

async fn response_wire(codec: &mut SidecarCodec, sidecars: Vec<WireSidecar>) -> Vec<u8> {
    let protocol = sidecars
        .first()
        .map_or(Protocol::DataColumnsByRootV1, |sidecar| protocol(sidecar.fork(), true));
    let mut out = Cursor::new(Vec::new());
    codec
        .write_response(
            &StreamProtocol::new(protocol.id()),
            &mut out,
            SidecarReply::Sidecars(SidecarBatch::serving(sidecars)),
        )
        .await
        .unwrap();
    out.into_inner()
}

#[test]
fn exact_requests_round_trip_independent_pinned_ssz_containers() {
    let blob = BlobIdentifier { block_root: [7; 32], index: 5 };
    let mut independent = deneb::BlobIdentifier::default_value(Preset::Minimal).unwrap();
    independent.block_root = blob.block_root;
    independent.index = blob.index;
    let request = SidecarRequest::BlobRoots(vec![blob]);
    assert_eq!(request.encode().unwrap(), independent.to_ssz(Preset::Minimal).unwrap());
    let range = SidecarRequest::ColumnRange { start_slot: 49, count: 2, columns: vec![0, 127] };
    let mut expected = 49u64.to_le_bytes().to_vec();
    expected.extend_from_slice(&2u64.to_le_bytes());
    expected.extend_from_slice(&20u32.to_le_bytes());
    expected.extend_from_slice(&0u64.to_le_bytes());
    expected.extend_from_slice(&127u64.to_le_bytes());
    assert_eq!(range.encode().unwrap(), expected);
    let identifiers = vec![
        ColumnIdentifier { block_root: [8; 32], columns: vec![1] },
        ColumnIdentifier { block_root: [9; 32], columns: vec![0, 127] },
    ];
    let mut expected = 8u32.to_le_bytes().to_vec();
    expected.extend_from_slice(&52u32.to_le_bytes());
    for identifier in &identifiers {
        let mut independent =
            fulu::DataColumnsByRootIdentifier::default_value(Preset::Minimal).unwrap();
        independent.block_root = identifier.block_root;
        independent.columns.values.clone_from(&identifier.columns);
        expected.extend_from_slice(&independent.to_ssz(Preset::Minimal).unwrap());
    }
    let roots = SidecarRequest::ColumnRoots(identifiers);
    assert_eq!(roots.encode().unwrap(), expected);
    for request in [request, range, roots, SidecarRequest::BlobRange { start_slot: 33, count: 2 }] {
        let bytes = request.encode().unwrap();
        assert_eq!(SidecarRequest::decode(request.protocol(), &bytes).unwrap(), request);
        assert_eq!(request.validate_outbound(), Ok(()));
    }
}

#[test]
fn request_offsets_truncation_and_negotiated_family_are_strict() {
    let range = SidecarRequest::ColumnRange { start_slot: 49, count: 1, columns: vec![1] };
    let roots = SidecarRequest::ColumnRoots(vec![
        ColumnIdentifier { block_root: [1; 32], columns: vec![0] },
        ColumnIdentifier { block_root: [2; 32], columns: vec![1] },
    ]);
    for request in [range, roots] {
        let original = request.encode().unwrap();
        for truncated in [original[..3].to_vec(), original[..original.len() - 1].to_vec()] {
            assert!(SidecarRequest::decode(request.protocol(), &truncated).is_err());
        }
        let mut shifted = original.clone();
        let offset = if matches!(request, SidecarRequest::ColumnRange { .. }) { 16 } else { 0 };
        shifted[offset..offset + 4].copy_from_slice(&24u32.to_le_bytes());
        assert!(SidecarRequest::decode(request.protocol(), &shifted).is_err());
        let mut trailing = original;
        trailing.push(0);
        assert!(SidecarRequest::decode(request.protocol(), &trailing).is_err());
    }
    let mut roots = SidecarRequest::ColumnRoots(vec![
        ColumnIdentifier { block_root: [1; 32], columns: vec![0] },
        ColumnIdentifier { block_root: [2; 32], columns: vec![1] },
    ])
    .encode()
    .unwrap();
    roots[4..8].copy_from_slice(&4u32.to_le_bytes());
    assert!(SidecarRequest::decode(Protocol::DataColumnsByRootV1, &roots).is_err());
    roots[4..8].copy_from_slice(&52u32.to_le_bytes());
    roots[40..44].copy_from_slice(&40u32.to_le_bytes());
    assert!(SidecarRequest::decode(Protocol::DataColumnsByRootV1, &roots).is_err());
    for bytes in [vec![0; 39], vec![0; 41]] {
        assert!(SidecarRequest::decode(Protocol::BlobSidecarsByRootV1, &bytes).is_err());
    }
    assert!(SidecarRequest::decode(Protocol::StatusV2, &[0; 16]).is_err());
    assert!(SidecarRequest::BlobRange { start_slot: u64::MAX, count: 2 }.encode().is_err());
}

#[test]
fn outbound_budgets_count_actual_requested_indices_and_duplicates() {
    assert_eq!(SidecarRequest::BlobRange { start_slot: 33, count: 14 }.validate_outbound(), Ok(()));
    assert_eq!(
        SidecarRequest::BlobRange { start_slot: 33, count: 15 }.validate_outbound(),
        Err(WireError::Oversized)
    );
    let ids = (0..128).map(|index| BlobIdentifier { block_root: [1; 32], index }).collect();
    assert_eq!(SidecarRequest::BlobRoots(ids).validate_outbound(), Ok(()));
    for request in [
        SidecarRequest::BlobRoots(vec![BlobIdentifier { block_root: [1; 32], index: 0 }; 2]),
        SidecarRequest::ColumnRange { start_slot: 49, count: 1, columns: vec![0, 0] },
        SidecarRequest::ColumnRoots(vec![
            ColumnIdentifier { block_root: [1; 32], columns: vec![0] },
            ColumnIdentifier { block_root: [1; 32], columns: vec![0] },
        ]),
        SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
            block_root: [1; 32],
            columns: vec![128],
        }]),
    ] {
        assert_eq!(request.validate_outbound(), Err(WireError::Ssz));
    }
    assert_eq!(
        SidecarRequest::ColumnRange { start_slot: 49, count: 1, columns: (0..128).collect() }
            .validate_outbound(),
        Ok(())
    );
    assert_eq!(
        SidecarRequest::ColumnRange { start_slot: 49, count: 2, columns: (0..128).collect() }
            .validate_outbound(),
        Err(WireError::Oversized)
    );
    for request in [
        SidecarRequest::BlobRoots(Vec::new()),
        SidecarRequest::ColumnRoots(Vec::new()),
        SidecarRequest::ColumnRange { start_slot: 49, count: 1, columns: Vec::new() },
    ] {
        assert_eq!(request.validate_outbound(), Err(WireError::Oversized));
    }
}

#[test]
fn complete_original_sidecars_require_historical_context_schema_and_index_limits() {
    let config = config();
    for fork in [Fork::Deneb, Fork::Electra, Fork::Fulu, Fork::Gloas] {
        let bytes = sidecar_bytes(fork, 0, 3);
        let context = config.fork_digest(slot(fork) / config.slots_per_epoch).unwrap();
        let sidecar =
            WireSidecar::from_ssz(bytes.clone(), context, protocol(fork, false), &config).unwrap();
        assert_eq!(sidecar.ssz().as_ref(), bytes);
        assert_eq!(sidecar.slot(), slot(fork));
        assert_eq!(sidecar.fork(), fork);
        assert_eq!(sidecar.index(), 0);
        assert_eq!(sidecar.context(), context);
        assert_eq!(
            sidecar.kind(),
            if fork < Fork::Fulu { SidecarKind::Blob } else { SidecarKind::Column }
        );
        assert!(WireSidecar::from_ssz(
            bytes[..bytes.len() - 1].to_vec(),
            context,
            protocol(fork, false),
            &config
        )
        .is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(WireSidecar::from_ssz(trailing, context, protocol(fork, false), &config).is_err());
        assert_eq!(
            WireSidecar::from_ssz(bytes.clone(), [0; 4], protocol(fork, false), &config),
            Err(WireError::ForkContext)
        );
        let mut foreign = config.clone();
        foreign.genesis_validators_root = [99; 32];
        assert!(
            WireSidecar::from_ssz(bytes.clone(), context, protocol(fork, false), &foreign).is_err()
        );
        let mut bounded = config.clone();
        bounded.max_payload_size = bytes.len() - 1;
        assert_eq!(
            WireSidecar::from_ssz(bytes, context, protocol(fork, false), &bounded),
            Err(WireError::Oversized)
        );
        let index = if fork < Fork::Fulu {
            config.blob_parameters(slot(fork) / config.slots_per_epoch).unwrap().max_blobs_per_block
        } else {
            128
        };
        assert_eq!(
            WireSidecar::from_ssz(
                sidecar_bytes(fork, index, 3),
                context,
                protocol(fork, false),
                &config
            ),
            Err(WireError::Ssz)
        );
    }
}

#[test]
fn variable_sidecar_offsets_parallel_shapes_and_active_blob_upgrades_are_checked() {
    let config = config();
    for fork in [Fork::Fulu, Fork::Gloas] {
        let mut bytes = sidecar_bytes(fork, 1, 3);
        bytes[8] ^= 1;
        assert!(WireSidecar::from_ssz(
            bytes,
            config.fork_digest(slot(fork) / 8).unwrap(),
            protocol(fork, false),
            &config
        )
        .is_err());
    }
    let mut fulu =
        fulu::DataColumnSidecar::from_ssz(&sidecar_bytes(Fork::Fulu, 0, 3), Preset::Minimal)
            .unwrap();
    fulu.kzg_commitments.values.clear();
    assert_eq!(
        WireSidecar::from_ssz(
            fulu.to_ssz(Preset::Minimal).unwrap(),
            config.fork_digest(6).unwrap(),
            Protocol::DataColumnsByRootV1,
            &config
        ),
        Err(WireError::Ssz)
    );
    let mut gloas =
        gloas::DataColumnSidecar::from_ssz(&sidecar_bytes(Fork::Gloas, 0, 3), Preset::Minimal)
            .unwrap();
    gloas.kzg_proofs.values.clear();
    assert_eq!(
        WireSidecar::from_ssz(
            gloas.to_ssz(Preset::Minimal).unwrap(),
            config.fork_digest(7).unwrap(),
            Protocol::DataColumnsByRootV1,
            &config
        ),
        Err(WireError::Ssz)
    );
    let mut upgraded = config.clone();
    upgraded.blob_schedule = vec![BlobParameters { epoch: 7, max_blobs_per_block: 1 }];
    let bytes = sidecar_bytes(Fork::Gloas, 0, 3);
    assert_ne!(config.fork_digest(7), upgraded.fork_digest(7));
    assert!(WireSidecar::from_ssz(
        bytes.clone(),
        config.fork_digest(7).unwrap(),
        Protocol::DataColumnsByRootV1,
        &upgraded
    )
    .is_err());
    assert!(WireSidecar::from_ssz(
        bytes,
        upgraded.fork_digest(7).unwrap(),
        Protocol::DataColumnsByRootV1,
        &upgraded
    )
    .is_ok());
    let mut two =
        gloas::DataColumnSidecar::from_ssz(&sidecar_bytes(Fork::Gloas, 0, 3), Preset::Minimal)
            .unwrap();
    two.column.values.push(gloas::Cell::default_value(Preset::Minimal).unwrap());
    two.kzg_proofs.values.push([0; 48]);
    assert_eq!(
        WireSidecar::from_ssz(
            two.to_ssz(Preset::Minimal).unwrap(),
            upgraded.fork_digest(7).unwrap(),
            Protocol::DataColumnsByRootV1,
            &upgraded
        ),
        Err(WireError::Ssz)
    );
}

#[test]
fn exact_root_index_membership_range_order_and_partial_responses_are_enforced() {
    for fork in [Fork::Deneb, Fork::Electra, Fork::Fulu, Fork::Gloas] {
        let a = wire_sidecar(fork, 0, 3);
        let b = wire_sidecar(fork, 1, 3);
        let wrong_root = wire_sidecar(fork, 1, 4);
        let range = if fork < Fork::Fulu {
            SidecarRequest::BlobRange { start_slot: slot(fork), count: 1 }
        } else {
            SidecarRequest::ColumnRange { start_slot: slot(fork), count: 1, columns: vec![0, 1] }
        };
        let roots = if fork < Fork::Fulu {
            SidecarRequest::BlobRoots(vec![
                BlobIdentifier { block_root: a.block_root(), index: 0 },
                BlobIdentifier { block_root: a.block_root(), index: 1 },
            ])
        } else {
            SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
                block_root: a.block_root(),
                columns: vec![0, 1],
            }])
        };
        assert_eq!(
            range.matches(&config(), &SidecarBatch::serving(vec![a.clone(), b.clone()])),
            Ok(())
        );
        assert_eq!(
            roots.matches(&config(), &SidecarBatch::serving(vec![b.clone(), a.clone()])),
            Ok(())
        );
        assert_eq!(
            range.matches(&config(), &SidecarBatch::serving(vec![b.clone(), a.clone()])),
            Err(WireError::Range)
        );
        assert_eq!(
            roots.matches(&config(), &SidecarBatch::serving(vec![wrong_root.clone()])),
            Err(WireError::Range)
        );
        assert_eq!(
            range.matches(&config(), &SidecarBatch::serving(vec![a.clone(), wrong_root])),
            Err(WireError::Range)
        );
        assert_eq!(
            roots.matches(&config(), &SidecarBatch::serving(vec![a.clone(), a.clone()])),
            Err(WireError::Ssz)
        );
        assert_eq!(
            roots.matches(&config(), &SidecarBatch::serving(vec![wire_sidecar(fork, 2, 3)])),
            Err(WireError::Range)
        );
        assert_eq!(roots.matches(&config(), &SidecarBatch::serving(vec![a])), Ok(()));
        assert_eq!(roots.matches(&config(), &SidecarBatch::serving(Vec::new())), Ok(()));
        let outside = if fork < Fork::Fulu {
            SidecarRequest::BlobRange { start_slot: slot(fork) + 1, count: 1 }
        } else {
            SidecarRequest::ColumnRange { start_slot: slot(fork) + 1, count: 1, columns: vec![1] }
        };
        assert_eq!(
            outside.matches(&config(), &SidecarBatch::serving(vec![b])),
            Err(WireError::Range)
        );
    }
}

#[tokio::test]
async fn every_schema_and_request_family_round_trips_original_framed_bytes() {
    let (_, mut codec) = codec();
    for fork in [Fork::Deneb, Fork::Electra, Fork::Fulu, Fork::Gloas] {
        for roots in [false, true] {
            let sidecar = wire_sidecar(fork, 1, 3);
            let request = match (fork < Fork::Fulu, roots) {
                (true, false) => SidecarRequest::BlobRange { start_slot: slot(fork), count: 1 },
                (true, true) => SidecarRequest::BlobRoots(vec![BlobIdentifier {
                    block_root: sidecar.block_root(),
                    index: 1,
                }]),
                (false, false) => SidecarRequest::ColumnRange {
                    start_slot: slot(fork),
                    count: 1,
                    columns: vec![1],
                },
                (false, true) => SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
                    block_root: sidecar.block_root(),
                    columns: vec![1],
                }]),
            };
            let protocol = StreamProtocol::new(protocol(fork, roots).id());
            let mut request_out = Cursor::new(Vec::new());
            codec.write_request(&protocol, &mut request_out, request.clone()).await.unwrap();
            assert_eq!(
                codec
                    .read_request(&protocol, &mut Cursor::new(request_out.into_inner()))
                    .await
                    .unwrap(),
                request
            );
            let wire = response_wire(&mut codec, vec![sidecar.clone()]).await;
            let SidecarReply::Sidecars(batch) =
                codec.read_response(&protocol, &mut Cursor::new(wire)).await.unwrap()
            else {
                panic!("expected sidecars")
            };
            assert_eq!(batch.sidecars(), [sidecar]);
            assert_eq!(request.matches(&config(), &batch), Ok(()));
        }
    }
}

#[tokio::test]
async fn remote_wire_maximum_can_receive_bounded_partial_data() {
    let (_, mut codec) = codec();
    let sidecar = wire_sidecar(Fork::Deneb, 0, 3);
    for request in [
        SidecarRequest::BlobRange { start_slot: 33, count: 128 },
        SidecarRequest::BlobRoots(
            (0..129)
                .map(|index| BlobIdentifier { block_root: sidecar.block_root(), index })
                .collect(),
        ),
        SidecarRequest::ColumnRange { start_slot: 49, count: 128, columns: (0..128).collect() },
    ] {
        assert_eq!(request.validate_outbound(), Err(WireError::Oversized));
        let protocol = StreamProtocol::new(request.protocol().id());
        let wire = encode_request(&request.encode().unwrap(), REQUEST_LIMITS).unwrap();
        assert_eq!(codec.read_request(&protocol, &mut Cursor::new(wire)).await.unwrap(), request);
        if matches!(request, SidecarRequest::BlobRange { .. } | SidecarRequest::BlobRoots(_)) {
            assert_eq!(
                request.matches(&config(), &SidecarBatch::serving(vec![sidecar.clone()])),
                Ok(())
            );
        }
    }
    let mut out = Cursor::new(Vec::new());
    assert!(codec
        .write_request(
            &StreamProtocol::new(Protocol::DataColumnsByRootV1.id()),
            &mut out,
            SidecarRequest::BlobRoots(vec![BlobIdentifier {
                block_root: sidecar.block_root(),
                index: 0
            }]),
        )
        .await
        .is_err());
    assert!(out.into_inner().is_empty());
}

#[tokio::test]
async fn response_truncation_errors_context_and_count_fail_without_leaking_budget() {
    let (blocks, mut codec) = codec();
    let sidecar = wire_sidecar(Fork::Gloas, 0, 3);
    let protocol = StreamProtocol::new(Protocol::DataColumnsByRootV1.id());
    let wire = response_wire(&mut codec, vec![sidecar]).await;
    let mut wrong_context = wire.clone();
    wrong_context[1..5].fill(0);
    let mut mixed = wire.clone();
    mixed.push(3);
    mixed.extend_from_slice(&encode_request(b"unavailable", REQUEST_LIMITS).unwrap());
    for bad in
        [wire[..wire.len() - 1].to_vec(), wrong_context, mixed, wire.repeat(MAX_SIDECAR_BATCH + 1)]
    {
        assert!(codec.read_response(&protocol, &mut Cursor::new(bad)).await.is_err());
        assert_eq!(blocks.budgets().0.available_permits(), 2);
    }
    let mut out = Cursor::new(Vec::new());
    assert!(codec.write_response(&protocol, &mut out, SidecarReply::Error(4)).await.is_err());
    assert_eq!(blocks.budgets().1.available_permits(), 2);
    assert!(out.into_inner().is_empty());
}

#[tokio::test]
async fn batch_byte_and_item_caps_apply_before_any_write_or_request_match() {
    let (_, mut codec) = codec();
    let sidecar = wire_sidecar(Fork::Gloas, 0, 3);
    let request = SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
        block_root: sidecar.block_root(),
        columns: vec![0],
    }]);
    for batch in [SidecarBatch::serving(vec![sidecar.clone(); MAX_SIDECAR_BATCH + 1]), {
        let mut too_large = sidecar;
        too_large.ssz = vec![0; MAX_BATCH_BYTES + 1].into();
        SidecarBatch::serving(vec![too_large])
    }] {
        assert_eq!(request.matches(&config(), &batch), Err(WireError::Oversized));
        let mut out = Cursor::new(Vec::new());
        assert!(codec
            .write_response(
                &StreamProtocol::new(Protocol::DataColumnsByRootV1.id()),
                &mut out,
                SidecarReply::Sidecars(batch)
            )
            .await
            .is_err());
        assert!(out.into_inner().is_empty());
    }
}

#[tokio::test]
async fn retained_batch_clones_share_block_and_envelope_decode_reservations() {
    let (mut blocks, mut sidecars) = codec();
    let mut envelopes = EnvelopeCodec::new(Arc::new(config()), &blocks);
    let sidecar = wire_sidecar(Fork::Fulu, 0, 3);
    let protocol = StreamProtocol::new(Protocol::DataColumnsByRootV1.id());
    let wire = response_wire(&mut sidecars, vec![sidecar]).await;
    let mut received = Vec::new();
    for _ in 0..2 {
        let SidecarReply::Sidecars(batch) =
            sidecars.read_response(&protocol, &mut Cursor::new(wire.clone())).await.unwrap()
        else {
            panic!("expected sidecars")
        };
        received.push(batch);
    }
    assert_eq!(blocks.budgets().0.available_permits(), 0);
    assert!(sidecars
        .read_response(&protocol, &mut Cursor::new(wire.clone()))
        .now_or_never()
        .is_none());
    assert!(blocks
        .read_response(
            &StreamProtocol::new(Protocol::BlocksByRootV2.id()),
            &mut Cursor::new(Vec::new())
        )
        .now_or_never()
        .is_none());
    assert!(envelopes
        .read_response(
            &StreamProtocol::new(Protocol::PayloadEnvelopesByRootV1.id()),
            &mut Cursor::new(Vec::new())
        )
        .now_or_never()
        .is_none());
    let retained = received[0].clone();
    drop(received);
    assert_eq!(blocks.budgets().0.available_permits(), 1);
    drop(retained);
    assert_eq!(blocks.budgets().0.available_permits(), 2);
    assert!(timeout(
        Duration::from_secs(2),
        sidecars.read_response(&protocol, &mut Cursor::new(wire))
    )
    .await
    .unwrap()
    .is_ok());
}

#[tokio::test]
async fn shared_write_budget_limits_sidecar_error_and_actual_compression_workers() {
    let (blocks, mut sidecars) = codec();
    let mut envelopes = EnvelopeCodec::new(Arc::new(config()), &blocks);
    let budget = blocks.budgets().1;
    let first = budget.clone().acquire_owned().await.unwrap();
    let second = budget.clone().acquire_owned().await.unwrap();
    let protocol = StreamProtocol::new(Protocol::DataColumnsByRootV1.id());
    assert!(sidecars
        .write_response(&protocol, &mut Cursor::new(Vec::new()), SidecarReply::Error(3))
        .now_or_never()
        .is_none());
    assert!(envelopes
        .write_response(
            &StreamProtocol::new(Protocol::PayloadEnvelopesByRootV1.id()),
            &mut Cursor::new(Vec::new()),
            EnvelopeReply::Error(3)
        )
        .now_or_never()
        .is_none());
    drop(first);
    let wire = response_wire(&mut sidecars, vec![wire_sidecar(Fork::Gloas, 0, 3)]).await;
    assert!(!wire.is_empty());
    assert_eq!(budget.available_permits(), 1);
    drop(second);
    assert_eq!(budget.available_permits(), 2);
}

struct PendingIo;
impl AsyncRead for PendingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn canceled_incomplete_sidecar_read_releases_its_reservation() {
    let (blocks, mut sidecars) = codec();
    let protocol = StreamProtocol::new(Protocol::DataColumnsByRootV1.id());
    let mut io = PendingIo;
    let mut read = Box::pin(sidecars.read_response(&protocol, &mut io));
    assert!(read.as_mut().now_or_never().is_none());
    assert_eq!(blocks.budgets().0.available_permits(), 1);
    drop(read);
    assert_eq!(blocks.budgets().0.available_permits(), 2);
}

#[tokio::test]
async fn canceled_ssz_and_compression_work_keep_budget_until_worker_finishes() {
    let (blocks, _) = codec();
    let (read, write) = blocks.budgets();
    for (budget, compress) in [(read, false), (write, true)] {
        let other = budget.clone().acquire_owned().await.unwrap();
        let reservation = Arc::new(budget.clone().acquire_owned().await.unwrap());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let sidecar = wire_sidecar(Fork::Gloas, 0, 3);
        let mut work = Box::pin(reserved_blocking(reservation, move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            if compress {
                encode_request(
                    &sidecar.ssz,
                    Limits { max_payload: 10 * 1024 * 1024, max_compressed: 12 * 1024 * 1024 },
                )
            } else {
                WireSidecar::from_ssz(
                    sidecar.ssz.to_vec(),
                    sidecar.context(),
                    Protocol::DataColumnsByRootV1,
                    &config(),
                )
                .map(|decoded| decoded.ssz.to_vec())
            }
        }));
        assert!(work.as_mut().now_or_never().is_none());
        timeout(Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
        drop(work);
        assert_eq!(budget.available_permits(), 0);
        release_tx.send(()).unwrap();
        let completed =
            timeout(Duration::from_secs(2), budget.clone().acquire_owned()).await.unwrap().unwrap();
        drop(completed);
        drop(other);
        assert_eq!(budget.available_permits(), 2);
    }
}

#[test]
fn post_fulu_legacy_blob_requests_can_be_empty_without_an_availability_claim() {
    assert_eq!(
        SidecarRequest::BlobRange { start_slot: slot(Fork::Gloas), count: 1 }
            .matches(&config(), &SidecarBatch::serving(Vec::new())),
        Ok(())
    );
    let mut block = fulu_block::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    block.message.slot = slot(Fork::Fulu);
    let wire = WireBlock::from_ssz(block.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap();
    assert_eq!(
        SidecarRequest::BlobRoots(vec![BlobIdentifier { block_root: wire.root(), index: 0 }])
            .matches(&config(), &SidecarBatch::serving(Vec::new())),
        Ok(())
    );
}
