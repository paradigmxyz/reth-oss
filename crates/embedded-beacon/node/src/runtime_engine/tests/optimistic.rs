//! Optimistic dispatch policy over the same exact typed backend used by strict verification.

use super::*;
use reth_embedded_beacon_fork_choice::store::optimistic::{
    ExecutionEngine as OptimisticEngine, Payload, PayloadContext, PayloadStatus as OptimisticStatus,
};

fn inputs(candidate: &Candidate) -> (Payload<'_>, PayloadContext) {
    match candidate {
        Candidate::Bellatrix(payload, context) => {
            (Payload::Bellatrix(payload), PayloadContext::Bellatrix(context.clone()))
        }
        Candidate::Capella(payload, context) => {
            (Payload::Capella(payload), PayloadContext::Capella(context.clone()))
        }
        Candidate::Deneb(payload, context) => {
            (Payload::Deneb(payload), PayloadContext::Deneb(context.clone()))
        }
        Candidate::Electra(payload, context) => {
            (Payload::Electra(payload), PayloadContext::Electra(context.clone()))
        }
        Candidate::Fulu(payload, context) => {
            (Payload::Fulu(payload), PayloadContext::Fulu(context.clone()))
        }
        Candidate::Gloas(..) => unreachable!("Gloas uses the separate envelope graph"),
    }
}

#[tokio::test]
async fn exact_original_statuses_and_all_fork_sidecars_survive_optimistic_dispatch() {
    for fork in [Fork::Bellatrix, Fork::Capella, Fork::Deneb, Fork::Electra, Fork::Fulu] {
        let candidate = Candidate::new(fork);
        let (strict, strict_backend) =
            adapter(fork, Reply::Status(PayloadStatusEnum::Valid, Some([6; 32].into())));
        candidate.verify(&strict).await.unwrap();
        for (status, hash, expected, diagnostic) in [
            (PayloadStatusEnum::Valid, Some([6; 32].into()), OptimisticStatus::Valid, None),
            (PayloadStatusEnum::Syncing, None, OptimisticStatus::Syncing, None),
            (PayloadStatusEnum::Accepted, None, OptimisticStatus::Accepted, None),
            (
                PayloadStatusEnum::Invalid { validation_error: "original rejection".into() },
                Some([1; 32].into()),
                OptimisticStatus::Invalid,
                Some("original rejection"),
            ),
        ] {
            let (optimistic, backend) = adapter(fork, Reply::Status(status, hash));
            let (payload, context) = inputs(&candidate);
            let reply =
                OptimisticEngine::notify_new_payload(&optimistic, payload, context).await.unwrap();
            assert_eq!(reply.status, expected);
            assert_eq!(reply.latest_valid_hash, hash.map(Into::into));
            assert_eq!(reply.validation_error.as_deref(), diagnostic);
            assert!(!optimistic.is_closed());
            let actual = backend.observed.lock().unwrap();
            let original = strict_backend.observed.lock().unwrap();
            assert_eq!(actual.len(), 1);
            assert_eq!(actual[0].0, original[0].0);
            assert_eq!(actual[0].1.payload, original[0].1.payload);
            assert_same_sidecar(&actual[0].1.sidecar, &original[0].1.sidecar);
        }
    }
}

#[tokio::test]
async fn malformed_or_uncertain_payload_replies_never_become_optimism() {
    let candidate = Candidate::new(Fork::Fulu);
    for (index, reply) in [
        Reply::Status(PayloadStatusEnum::Valid, None),
        Reply::Status(PayloadStatusEnum::Valid, Some([9; 32].into())),
        Reply::Status(PayloadStatusEnum::Syncing, Some([6; 32].into())),
        Reply::Status(PayloadStatusEnum::Accepted, Some([1; 32].into())),
        Reply::Status(
            PayloadStatusEnum::Invalid { validation_error: "contradictory".into() },
            Some([6; 32].into()),
        ),
        Reply::Status(PayloadStatusEnum::Invalid { validation_error: "x".repeat(1025) }, None),
        Reply::Wrong,
        Reply::Pending,
    ]
    .into_iter()
    .enumerate()
    {
        let (engine, backend) = adapter(Fork::Fulu, reply);
        let shared = engine.clone();
        assert!(Arc::ptr_eq(&engine.config, &shared.config));
        assert!(Arc::ptr_eq(&engine.bellatrix, &shared.bellatrix));
        assert!(Arc::ptr_eq(&engine.late, &shared.late));
        let (payload, context) = inputs(&candidate);
        let error =
            OptimisticEngine::notify_new_payload(&engine, payload, context).await.unwrap_err();
        match index {
            0..=4 => assert!(matches!(error, EngineVerificationError::LatestValidHashMismatch)),
            5..=6 => assert!(matches!(error, EngineVerificationError::UnexpectedResponse)),
            7 => assert!(matches!(error, EngineVerificationError::Proxy(ProxyError::Timeout))),
            _ => unreachable!(),
        }
        assert!(engine.is_closed());
        assert!(shared.is_closed());
        let (payload, context) = inputs(&candidate);
        assert!(matches!(
            OptimisticEngine::notify_new_payload(&shared, payload, context).await,
            Err(EngineVerificationError::Proxy(ProxyError::Closed))
        ));
        assert_eq!(backend.observed.lock().unwrap().len(), 1);
    }
    let (engine, _) = adapter(Fork::Fulu, Reply::Error);
    let (payload, context) = inputs(&candidate);
    assert!(matches!(
        OptimisticEngine::notify_new_payload(&engine, payload, context).await,
        Err(EngineVerificationError::Proxy(ProxyError::Engine(TestError)))
    ));
    assert!(!engine.is_closed());
}

#[tokio::test]
async fn context_fork_clock_parent_and_blob_encoding_stay_mandatory() {
    let Candidate::Fulu(payload, context) = Candidate::new(Fork::Fulu) else { unreachable!() };
    let (engine, backend) = adapter(Fork::Fulu, Reply::Pending);
    let mut changed = payload.clone();
    changed.timestamp += 1;
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &engine,
            Payload::Fulu(&changed),
            PayloadContext::Fulu(context.clone())
        )
        .await,
        Err(EngineVerificationError::TimestampMismatch)
    ));
    let mut changed = context.clone();
    changed.previous_execution_payload_hash = Some([9; 32]);
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &engine,
            Payload::Fulu(&payload),
            PayloadContext::Fulu(changed)
        )
        .await,
        Err(EngineVerificationError::ParentMismatch)
    ));
    let mut changed = context.clone();
    changed.expected_blob_versioned_hashes[0][0] = 2;
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &engine,
            Payload::Fulu(&payload),
            PayloadContext::Fulu(changed)
        )
        .await,
        Err(EngineVerificationError::Conversion(_))
    ));
    let Candidate::Electra(_, other_context) = Candidate::new(Fork::Electra) else {
        unreachable!()
    };
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &engine,
            Payload::Fulu(&payload),
            PayloadContext::Electra(other_context)
        )
        .await,
        Err(EngineVerificationError::ForkMismatch)
    ));
    let mut changed = context.clone();
    changed.previous_execution_payload_hash = None;
    assert!(matches!(
        OptimisticEngine::verify_terminal_block(
            &engine,
            Payload::Fulu(&payload),
            PayloadContext::Fulu(changed.clone())
        )
        .await,
        Err(EngineVerificationError::TerminalVerificationUnavailable)
    ));
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &engine,
            Payload::Fulu(&payload),
            PayloadContext::Fulu(changed)
        )
        .await,
        Err(EngineVerificationError::TerminalVerificationUnavailable)
    ));
    assert!(backend.observed.lock().unwrap().is_empty());
    assert!(!engine.is_closed());
    let (other, backend) = adapter(Fork::Electra, Reply::Pending);
    assert!(matches!(
        OptimisticEngine::notify_new_payload(
            &other,
            Payload::Fulu(&payload),
            PayloadContext::Fulu(context)
        )
        .await,
        Err(EngineVerificationError::ForkMismatch)
    ));
    assert!(backend.observed.lock().unwrap().is_empty());
}
