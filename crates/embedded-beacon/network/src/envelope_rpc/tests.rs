//! Envelope framing, exact beacon context, shared response budgets and cancellation regressions.

use super::*;
use crate::{
    block_rpc::{BlockBatch, BlockReply},
    test_envelopes::{config, signed_envelope, wire_block, wire_envelope},
};
use futures::{io::Cursor, FutureExt};
use reth_embedded_beacon_config::BlobParameters;
use reth_embedded_beacon_state_transition::ssz::Ssz;
use std::{
    pin::Pin,
    sync::mpsc,
    task::{Context, Poll},
};
use tokio::time::{timeout, Duration};

fn codec() -> (BlockCodec, EnvelopeCodec) {
    let config = Arc::new(config());
    let blocks = BlockCodec::new(config.clone());
    let envelopes = EnvelopeCodec::new(config, &blocks);
    (blocks, envelopes)
}

fn references(blocks: &[WireBlock]) -> Vec<EnvelopeBlockReference> {
    blocks.iter().map(EnvelopeBlockReference::from).collect()
}

async fn response_wire(codec: &mut EnvelopeCodec, envelopes: Vec<WireEnvelope>) -> Vec<u8> {
    let mut out = Cursor::new(Vec::new());
    codec
        .write_response(
            &StreamProtocol::new(Protocol::PayloadEnvelopesByRangeV1.id()),
            &mut out,
            EnvelopeReply::Envelopes(EnvelopeBatch::serving(envelopes)),
        )
        .await
        .unwrap();
    out.into_inner()
}

#[test]
fn requests_require_exact_bounds_roots_slots_and_original_parent_chain() {
    let a = wire_block(9, [1; 32]);
    let b = wire_block(12, a.root());
    let refs = references(&[a.clone(), b.clone()]);
    let range = EnvelopeRequest::Range { start_slot: 9, count: 4 };
    let batch = EnvelopeBatch::serving(vec![wire_envelope(&a), wire_envelope(&b)]);
    assert_eq!(range.validate_expected(&config(), &refs), Ok(()));
    assert_eq!(
        range.matches(&config(), &batch, |root| refs.iter().find(|block| block.root() == root)),
        Ok(())
    );
    let roots = EnvelopeRequest::Roots(vec![b.root(), a.root()]);
    assert_eq!(
        roots.matches(&config(), &batch, |root| refs.iter().find(|block| block.root() == root)),
        Ok(())
    );
    assert_eq!(roots.matches(&config(), &batch, |_| None), Err(WireError::Ssz));
    assert_eq!(roots.validate_expected(&config(), &refs[..1]), Err(WireError::Ssz));
    assert_eq!(
        EnvelopeRequest::Roots(vec![[9; 32]]).validate_expected(&config(), &refs),
        Err(WireError::Ssz)
    );
    assert_eq!(
        range.matches(
            &config(),
            &EnvelopeBatch::serving(vec![wire_envelope(&b), wire_envelope(&a)]),
            |root| refs.iter().find(|block| block.root() == root)
        ),
        Err(WireError::Range)
    );
    assert_eq!(
        range.matches(
            &config(),
            &EnvelopeBatch::serving(vec![wire_envelope(&a), wire_envelope(&a)]),
            |root| refs.iter().find(|block| block.root() == root)
        ),
        Err(WireError::Oversized)
    );
    assert_eq!(
        EnvelopeRequest::Range { start_slot: 10, count: 3 }.validate_expected(&config(), &refs),
        Err(WireError::Range)
    );
    let forked = wire_block(12, [4; 32]);
    let fork_refs = references(&[a.clone(), forked.clone()]);
    assert_eq!(
        range.matches(
            &config(),
            &EnvelopeBatch::serving(vec![wire_envelope(&a), wire_envelope(&forked)]),
            |root| fork_refs.iter().find(|block| block.root() == root)
        ),
        Err(WireError::Range)
    );
    for request in [
        EnvelopeRequest::Range { start_slot: u64::MAX, count: 2 },
        EnvelopeRequest::Range { start_slot: 0, count: 0 },
        EnvelopeRequest::Range { start_slot: 0, count: 17 },
        EnvelopeRequest::Roots(Vec::new()),
        EnvelopeRequest::Roots(vec![a.root(); 2]),
        EnvelopeRequest::Roots(vec![[0; 32]; 17]),
    ] {
        assert_eq!(request.validate_outbound(), Err(WireError::Range));
    }
}

#[test]
fn complete_signed_schema_and_exact_parent_root_and_payload_slot_are_required() {
    let block = wire_block(9, [1; 32]);
    let bytes = signed_envelope(&block).to_ssz(Preset::Minimal).unwrap();
    let envelope = WireEnvelope::from_ssz(bytes.clone(), &block, &config()).unwrap();
    assert_eq!(envelope.beacon_block_root(), block.root());
    assert_eq!(envelope.parent_beacon_block_root(), block.parent_root());
    let mut malformed = bytes.clone();
    malformed[0] ^= 1;
    assert_eq!(WireEnvelope::from_ssz(malformed, &block, &config()), Err(WireError::Ssz));
    assert_eq!(
        WireEnvelope::from_ssz(bytes[..100].to_vec(), &block, &config()),
        Err(WireError::Ssz)
    );
    let mut signed = signed_envelope(&block);
    signed.message.beacon_block_root = [9; 32];
    assert_eq!(
        WireEnvelope::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &block, &config()),
        Err(WireError::Ssz)
    );
    signed = signed_envelope(&block);
    signed.message.parent_beacon_block_root = [9; 32];
    assert_eq!(
        WireEnvelope::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &block, &config()),
        Err(WireError::Ssz)
    );
    signed = signed_envelope(&block);
    signed.message.payload.slot_number = block.slot() + 1;
    assert_eq!(
        WireEnvelope::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &block, &config()),
        Err(WireError::Ssz)
    );
    let classic = crate::test_blocks::wire_block(9, [1; 32]);
    assert_eq!(
        WireEnvelope::from_ssz(bytes, &classic, &crate::test_blocks::config()),
        Err(WireError::ForkContext)
    );
}

#[test]
fn scheduled_context_uses_original_beacon_slot_and_never_payload_timestamp() {
    let mut config = config();
    config.blob_schedule.push(BlobParameters { epoch: 2, max_blobs_per_block: 30 });
    config.validate().unwrap();
    let block = WireBlock::from_ssz(wire_block(9, [1; 32]).ssz().to_vec(), &config).unwrap();
    let reference = EnvelopeBlockReference::from(&block);
    let signed = signed_envelope(&block);
    assert_eq!(signed.message.payload.timestamp, u64::MAX);
    let envelope =
        WireEnvelope::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &block, &config).unwrap();
    let request = EnvelopeRequest::Roots(vec![block.root()]);
    assert_eq!(
        request.matches(&config, &EnvelopeBatch::serving(vec![envelope.clone()]), |_| Some(
            &reference
        )),
        Ok(())
    );
    let mut wrong = envelope;
    // Both digests are known Gloas digests, but only the referenced slot selects the correct one.
    wrong.context = config.fork_digest(2).unwrap();
    assert_ne!(wrong.context, block.context());
    assert_eq!(
        request.matches(&config, &EnvelopeBatch::serving(vec![wrong]), |_| Some(&reference)),
        Err(WireError::ForkContext)
    );
}

#[tokio::test]
async fn range_and_roots_requests_roundtrip_only_after_exact_eof() {
    let (_, mut codec) = codec();
    let block = wire_block(9, [1; 32]);
    for request in [
        EnvelopeRequest::Range { start_slot: 9, count: 4 },
        EnvelopeRequest::Roots(vec![block.root()]),
    ] {
        let protocol = StreamProtocol::new(request.protocol().id());
        let mut out = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut out, request.clone()).await.unwrap();
        let wire = out.into_inner();
        assert_eq!(
            codec.read_request(&protocol, &mut Cursor::new(wire.clone())).await.unwrap(),
            request
        );
        let mut trailing = wire.clone();
        trailing.push(0);
        assert_eq!(
            codec.read_request(&protocol, &mut Cursor::new(trailing)).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            codec
                .read_request(&protocol, &mut Cursor::new(wire[..wire.len() - 1].to_vec()))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let wrong = StreamProtocol::new(Protocol::BlocksByRootV2.id());
        assert_eq!(
            codec
                .write_request(&wrong, &mut Cursor::new(Vec::new()), request)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[tokio::test]
async fn multi_chunk_empty_and_unavailable_responses_preserve_exact_wire_data() {
    let (_, mut codec) = codec();
    let a = wire_block(9, [1; 32]);
    let b = wire_block(12, a.root());
    let envelopes = vec![wire_envelope(&a), wire_envelope(&b)];
    let wire = response_wire(&mut codec, envelopes.clone()).await;
    let protocol = StreamProtocol::new(Protocol::PayloadEnvelopesByRangeV1.id());
    let EnvelopeReply::Envelopes(batch) =
        codec.read_response(&protocol, &mut Cursor::new(wire)).await.unwrap()
    else {
        panic!("expected envelopes")
    };
    assert_eq!(batch.envelopes(), envelopes);
    drop(batch);
    let EnvelopeReply::Envelopes(empty) =
        codec.read_response(&protocol, &mut Cursor::new(Vec::new())).await.unwrap()
    else {
        panic!("expected empty envelopes")
    };
    assert!(empty.envelopes().is_empty());
    drop(empty);
    let mut out = Cursor::new(Vec::new());
    codec.write_response(&protocol, &mut out, EnvelopeReply::Error(3)).await.unwrap();
    assert!(matches!(
        codec.read_response(&protocol, &mut Cursor::new(out.into_inner())).await.unwrap(),
        EnvelopeReply::Error(3)
    ));
}

#[tokio::test]
async fn malformed_digest_truncated_frames_invalid_codes_and_trailing_chunks_reject() {
    let (_, mut codec) = codec();
    let protocol = StreamProtocol::new(Protocol::PayloadEnvelopesByRangeV1.id());
    let wire = response_wire(&mut codec, vec![wire_envelope(&wire_block(9, [1; 32]))]).await;
    let mut wrong = wire.clone();
    wrong[1] ^= 1;
    assert_eq!(
        codec.read_response(&protocol, &mut Cursor::new(wrong)).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    for end in [1, 4, 8, wire.len() - 1] {
        assert_eq!(
            codec
                .read_response(&protocol, &mut Cursor::new(wire[..end].to_vec()))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
    let mut invalid_code = wire.clone();
    invalid_code[0] = 4;
    assert_eq!(
        codec.read_response(&protocol, &mut Cursor::new(invalid_code)).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut malformed = wire_envelope(&wire_block(9, [1; 32]));
    let mut ssz = malformed.ssz.to_vec();
    ssz[0] ^= 1;
    malformed.ssz = ssz.into();
    let malformed_wire = response_wire(&mut codec, vec![malformed]).await;
    assert_eq!(
        codec.read_response(&protocol, &mut Cursor::new(malformed_wire)).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut envelopes = Vec::new();
    let mut root = [1; 32];
    for slot in 0..MAX_ENVELOPE_BATCH as u64 {
        let block = wire_block(slot, root);
        root = block.root();
        envelopes.push(wire_envelope(&block));
    }
    let mut full_wire = response_wire(&mut codec, envelopes).await;
    full_wire.push(0);
    assert_eq!(
        codec.read_response(&protocol, &mut Cursor::new(full_wire)).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn oversized_batch_is_rejected_before_writing_or_matching() {
    let (_, mut codec) = codec();
    let block = wire_block(9, [1; 32]);
    let mut envelope = wire_envelope(&block);
    envelope.ssz = vec![0; MAX_BATCH_BYTES + 1].into();
    let batch = EnvelopeBatch::serving(vec![envelope]);
    let request = EnvelopeRequest::Roots(vec![block.root()]);
    let reference = EnvelopeBlockReference::from(&block);
    assert_eq!(request.matches(&config(), &batch, |_| Some(&reference)), Err(WireError::Oversized));
    let mut out = Cursor::new(Vec::new());
    assert_eq!(
        codec
            .write_response(
                &StreamProtocol::new(request.protocol().id()),
                &mut out,
                EnvelopeReply::Envelopes(batch)
            )
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(out.into_inner().is_empty());
    assert_eq!(codec.write_budget.available_permits(), 2);
}

#[tokio::test]
async fn two_retained_block_batches_can_release_all_permits_before_envelope_completion() {
    let (mut blocks, mut envelopes) = codec();
    let a = wire_block(9, [1; 32]);
    let b = wire_block(12, a.root());
    let block_protocol = StreamProtocol::new(Protocol::BlocksByRangeV2.id());
    let mut received = Vec::new();
    let mut refs = Vec::new();
    for block in [a.clone(), b.clone()] {
        let mut out = Cursor::new(Vec::new());
        blocks
            .write_response(
                &block_protocol,
                &mut out,
                BlockReply::Blocks(BlockBatch::serving(vec![block])),
            )
            .await
            .unwrap();
        let BlockReply::Blocks(batch) = blocks
            .read_response(&block_protocol, &mut Cursor::new(out.into_inner()))
            .await
            .unwrap()
        else {
            panic!("expected block")
        };
        refs.extend(batch.blocks().iter().map(EnvelopeBlockReference::from));
        received.push(batch);
    }
    assert!(std::mem::size_of::<EnvelopeBlockReference>() <= 128);
    assert_eq!(envelopes.read_budget.available_permits(), 0);
    let protocol = StreamProtocol::new(Protocol::PayloadEnvelopesByRangeV1.id());
    let wire = response_wire(&mut envelopes, vec![wire_envelope(&a), wire_envelope(&b)]).await;
    assert!(envelopes
        .read_response(&protocol, &mut Cursor::new(wire.clone()))
        .now_or_never()
        .is_none());
    let retained = received[0].clone();
    drop(received);
    assert_eq!(envelopes.read_budget.available_permits(), 1);
    drop(retained);
    assert_eq!(envelopes.read_budget.available_permits(), 2);
    let EnvelopeReply::Envelopes(batch) =
        timeout(Duration::from_secs(2), envelopes.read_response(&protocol, &mut Cursor::new(wire)))
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("expected envelope")
    };
    assert_eq!(
        EnvelopeRequest::Range { start_slot: 9, count: 4 }
            .matches(&config(), &batch, |root| refs.iter().find(|block| block.root() == root)),
        Ok(())
    );
    let retained = batch.clone();
    drop(batch);
    assert_eq!(blocks.budgets().0.available_permits(), 1);
    drop(retained);
    assert_eq!(blocks.budgets().0.available_permits(), 2);
}

#[tokio::test]
async fn shared_write_budget_admits_no_hidden_envelope_workers() {
    let (blocks, mut envelopes) = codec();
    let (_, write) = blocks.budgets();
    let a = write.clone().acquire_owned().await.unwrap();
    let b = write.clone().acquire_owned().await.unwrap();
    let protocol = StreamProtocol::new(Protocol::PayloadEnvelopesByRootV1.id());
    assert!(envelopes
        .write_response(&protocol, &mut Cursor::new(Vec::new()), EnvelopeReply::Error(3))
        .now_or_never()
        .is_none());
    assert_eq!(write.available_permits(), 0);
    drop(a);
    let mut out = Cursor::new(Vec::new());
    timeout(
        Duration::from_secs(2),
        envelopes.write_response(&protocol, &mut out, EnvelopeReply::Error(3)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!out.into_inner().is_empty());
    assert_eq!(write.available_permits(), 1);
    drop(b);
    assert_eq!(write.available_permits(), 2);
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
async fn canceled_incomplete_read_returns_its_shared_reservation() {
    let (blocks, mut envelopes) = codec();
    let protocol = StreamProtocol::new(Protocol::PayloadEnvelopesByRootV1.id());
    let mut io = PendingIo;
    let mut read = Box::pin(envelopes.read_response(&protocol, &mut io));
    assert!(read.as_mut().now_or_never().is_none());
    assert_eq!(blocks.budgets().0.available_permits(), 1);
    drop(read);
    assert_eq!(blocks.budgets().0.available_permits(), 2);
}

#[tokio::test]
async fn canceled_ssz_or_compression_worker_keeps_reservation_until_actual_completion() {
    let (blocks, _) = codec();
    let (read, write) = blocks.budgets();
    for (budget, compress) in [(read, false), (write, true)] {
        let other = budget.clone().acquire_owned().await.unwrap();
        let reservation = Arc::new(budget.clone().acquire_owned().await.unwrap());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let block = wire_block(9, [1; 32]);
        let ssz = wire_envelope(&block).ssz().clone();
        let config = config();
        let context = block.context();
        let mut work = Box::pin(reserved_blocking(reservation, move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            if compress {
                encode_request(
                    &ssz,
                    Limits { max_payload: 10 * 1024 * 1024, max_compressed: 12 * 1024 * 1024 },
                )
            } else {
                WireEnvelope::decode(ssz.to_vec(), context, &config)
                    .map(|envelope| envelope.ssz.to_vec())
            }
        }));
        assert!(work.as_mut().now_or_never().is_none());
        timeout(Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
        drop(work);
        assert_eq!(budget.available_permits(), 0);
        release_tx.send(()).unwrap();
        let completed =
            timeout(Duration::from_secs(2), budget.clone().acquire_owned()).await.unwrap().unwrap();
        assert_eq!(budget.available_permits(), 0);
        drop(completed);
        drop(other);
        assert_eq!(budget.available_permits(), 2);
    }
}

#[test]
fn genuine_gloas_empty_payload_gap_walks_original_block_metadata() {
    // These complete signed SSZ blocks carry distinct bids. The successor builds on A's
    // payload rather than the middle block's bid, making the middle payload EMPTY on this
    // branch. This wire test grants no signature, fork-choice or execution authority.
    let mut a_signed = gloas::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    a_signed.message.slot = 9;
    a_signed.message.parent_root = [1; 32];
    a_signed.message.body.signed_execution_payload_bid.message.slot = 9;
    a_signed.message.body.signed_execution_payload_bid.message.block_hash = [11; 32];
    let a = WireBlock::from_ssz(a_signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap();
    let mut empty_signed = gloas::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    empty_signed.message.slot = 10;
    empty_signed.message.parent_root = a.root();
    empty_signed.message.body.signed_execution_payload_bid.message.slot = 10;
    empty_signed.message.body.signed_execution_payload_bid.message.parent_block_root = a.root();
    empty_signed.message.body.signed_execution_payload_bid.message.parent_block_hash = [11; 32];
    empty_signed.message.body.signed_execution_payload_bid.message.block_hash = [12; 32];
    let empty =
        WireBlock::from_ssz(empty_signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap();
    let mut successor_signed = gloas::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    successor_signed.message.slot = 12;
    successor_signed.message.parent_root = empty.root();
    successor_signed.message.body.signed_execution_payload_bid.message.slot = 12;
    successor_signed.message.body.signed_execution_payload_bid.message.parent_block_root =
        empty.root();
    successor_signed.message.body.signed_execution_payload_bid.message.parent_block_hash = [11; 32];
    successor_signed.message.body.signed_execution_payload_bid.message.block_hash = [13; 32];
    assert_ne!(
        successor_signed.message.body.signed_execution_payload_bid.message.parent_block_hash,
        empty_signed.message.body.signed_execution_payload_bid.message.block_hash
    );
    let successor =
        WireBlock::from_ssz(successor_signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap();
    let mut a_payload = signed_envelope(&a);
    a_payload.message.payload.block_hash = [11; 32];
    let mut successor_payload = signed_envelope(&successor);
    successor_payload.message.payload.block_hash = [13; 32];
    let batch = EnvelopeBatch::serving(vec![
        WireEnvelope::from_ssz(a_payload.to_ssz(Preset::Minimal).unwrap(), &a, &config()).unwrap(),
        WireEnvelope::from_ssz(
            successor_payload.to_ssz(Preset::Minimal).unwrap(),
            &successor,
            &config(),
        )
        .unwrap(),
    ]);
    let request = EnvelopeRequest::Range { start_slot: 9, count: 4 };
    let original = references(&[a.clone(), empty, successor.clone()]);
    assert_eq!(request.validate_expected(&config(), &original), Ok(()));
    assert_eq!(
        request
            .matches(&config(), &batch, |root| original.iter().find(|block| block.root() == root)),
        Ok(())
    );
    let missing_intermediate = references(&[a, successor]);
    assert_eq!(
        request.matches(&config(), &batch, |root| missing_intermediate
            .iter()
            .find(|block| block.root() == root)),
        Err(WireError::Range)
    );
}

#[test]
fn root_serving_may_omit_unavailable_payloads_without_unrelated_references() {
    let a = wire_block(9, [1; 32]);
    let b = wire_block(12, a.root());
    let request = EnvelopeRequest::Roots(vec![a.root(), b.root()]);
    let refs = references(std::slice::from_ref(&a));
    assert_eq!(request.validate_expected(&config(), &refs), Err(WireError::Ssz));
    assert_eq!(request.validate_serving(&config(), &refs), Ok(()));
    assert_eq!(
        request.matches(&config(), &EnvelopeBatch::serving(vec![wire_envelope(&a)]), |root| refs
            .iter()
            .find(|block| block.root() == root)),
        Ok(())
    );
}

#[tokio::test]
async fn valid_remote_wire_maximum_can_receive_bounded_partial_response() {
    let (_, mut codec) = codec();
    let a = wire_block(9, [1; 32]);
    let reference = EnvelopeBlockReference::from(&a);
    let range = EnvelopeRequest::Range { start_slot: 9, count: 128 };
    let roots = EnvelopeRequest::Roots(
        std::iter::once(a.root()).chain((1..128).map(|index| [index; 32])).collect(),
    );
    for request in [range, roots] {
        assert_eq!(request.validate_outbound(), Err(WireError::Range));
        let protocol = StreamProtocol::new(request.protocol().id());
        let wire = encode_request(&request.encode().unwrap(), REQUEST_LIMITS).unwrap();
        let actual = codec.read_request(&protocol, &mut Cursor::new(wire)).await.unwrap();
        assert_eq!(actual, request);
        assert_eq!(request.validate_serving(&config(), &[reference]), Ok(()));
        assert_eq!(
            request.matches(&config(), &EnvelopeBatch::serving(vec![wire_envelope(&a)]), |root| {
                (root == a.root()).then_some(&reference)
            }),
            Ok(())
        );
    }
    assert_eq!(
        EnvelopeRequest::Range { start_slot: 9, count: 129 }.encode(),
        Err(WireError::Range)
    );
    assert_eq!(EnvelopeRequest::Roots(vec![[0; 32]; 129]).encode(), Err(WireError::Oversized));
}
