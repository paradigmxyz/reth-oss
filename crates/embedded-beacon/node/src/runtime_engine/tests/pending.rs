//! Actual typed status feedback permits retry only and remains tied to one owned input attempt.
use super::*;

#[tokio::test]
async fn actual_typed_pending_never_validates_and_cannot_cross_original_attempts() {
    for fork in
        [Fork::Bellatrix, Fork::Capella, Fork::Deneb, Fork::Electra, Fork::Fulu, Fork::Gloas]
    {
        for status in [PayloadStatusEnum::Syncing, PayloadStatusEnum::Accepted] {
            let candidate = Candidate::new(fork);
            let (verifier, backend) = adapter(fork, Reply::Status(status, None));
            verifier.begin_import_attempt([41; 32]).unwrap();
            assert!(matches!(
                candidate.verify(&verifier).await,
                Err(EngineVerificationError::NotValidated(_))
            ));
            assert!(verifier.take_pending_import([42; 32]).unwrap().is_none());
            let pending = verifier.take_pending_import([41; 32]).unwrap().unwrap();
            assert_eq!(pending.hash, [6; 32]);
            assert!(!verifier.is_closed());
            assert!(verifier.take_pending_import([41; 32]).unwrap().is_none());
            verifier.begin_import_attempt([42; 32]).unwrap();
            assert!(verifier.take_pending_import([41; 32]).unwrap().is_none());
            *backend.reply.lock().unwrap() =
                Some(Reply::Status(PayloadStatusEnum::Valid, Some([6; 32].into())));
            candidate.verify(&verifier).await.unwrap();
            assert!(verifier.take_pending_import([42; 32]).unwrap().is_none());
            assert_eq!(backend.observed.lock().unwrap().len(), 2);
        }
        let candidate = Candidate::new(fork);
        let (verifier, _) = adapter(
            fork,
            Reply::Status(
                PayloadStatusEnum::Invalid {
                    validation_error: "genuine controlled invalid".into(),
                },
                None,
            ),
        );
        verifier.begin_import_attempt([43; 32]).unwrap();
        assert!(matches!(
            candidate.verify(&verifier).await,
            Err(EngineVerificationError::NotValidated(_))
        ));
        assert!(verifier.take_pending_import([43; 32]).unwrap().is_none());
    }
}
