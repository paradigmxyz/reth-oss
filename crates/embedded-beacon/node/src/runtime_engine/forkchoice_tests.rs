//! Controlled backend policy tests do not establish live execution validity.

use super::*;
use alloy_rpc_types_engine::{ForkchoiceUpdated, ForkchoiceUpdatedResponseV2, PayloadStatus};
use reth_embedded_beacon_execution_proxy::ProxyError;
use std::{sync::Mutex, time::Duration};

#[derive(Debug, thiserror::Error)]
#[error("private forkchoice policy backend failed")]
struct TestError;

enum Reply {
    Status(PayloadStatusEnum, Option<B256>),
    PayloadJob,
    Extended,
    Error,
    Pending,
}

type ForkchoiceCall = (ForkchoiceVersion, ForkchoiceState, Option<[u8; 16]>);

struct Backend {
    reply: Mutex<Option<Reply>>,
    calls: Mutex<Vec<ForkchoiceCall>>,
}

#[derive(Clone)]
struct Handle(Arc<Backend>);

impl TypedExecutionEngine for Handle {
    type Payload = ExecutionData;
    type Forkchoice = ForkchoiceState;
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = TestError;

    async fn new_payload(
        &self,
        _version: PayloadVersion,
        _data: ExecutionData,
    ) -> Result<PayloadReply, TestError> {
        unreachable!("forkchoice policy checks cannot submit a payload")
    }

    async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, TestError> {
        self.0.calls.lock().unwrap().push((version, state, custody));
        let reply = self.0.reply.lock().unwrap().take().unwrap();
        match reply {
            Reply::Status(status, hash) => {
                Ok(ForkchoiceReply::V1(ForkchoiceUpdated::new(PayloadStatus::new(status, hash))))
            }
            Reply::PayloadJob => Ok(ForkchoiceReply::V1(
                ForkchoiceUpdated::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash(state.head_block_hash)
                    .with_payload_id(Default::default()),
            )),
            Reply::Extended => Ok(ForkchoiceReply::V2(
                ForkchoiceUpdatedResponseV2::from_status(PayloadStatusEnum::Valid)
                    .with_latest_valid_hash(state.head_block_hash),
            )),
            Reply::Error => Err(TestError),
            Reply::Pending => std::future::pending().await,
        }
    }
}

fn adapter(reply: Reply) -> (RuntimeEngineAdapter<Handle>, Arc<Backend>) {
    let backend =
        Arc::new(Backend { reply: Mutex::new(Some(reply)), calls: Mutex::new(Vec::new()) });
    let proxy = ExecutionProxy::new(Handle(backend.clone()), Duration::from_millis(20)).unwrap();
    (RuntimeEngineAdapter::new(proxy, ChainConfig::hoodi()).unwrap(), backend)
}

fn state() -> ForkchoiceState {
    ForkchoiceState {
        head_block_hash: [3; 32].into(),
        safe_block_hash: [2; 32].into(),
        finalized_block_hash: [1; 32].into(),
    }
}

#[tokio::test]
async fn explicit_method_hashes_and_custody_reach_the_same_typed_boundary() {
    for version in
        [ForkchoiceVersion::V1, ForkchoiceVersion::V2, ForkchoiceVersion::V3, ForkchoiceVersion::V4]
    {
        let expected = state();
        let custody = (version == ForkchoiceVersion::V4).then_some([11; 16]);
        let (verifier, backend) =
            adapter(Reply::Status(PayloadStatusEnum::Valid, Some(expected.head_block_hash)));
        verifier.forkchoice_updated(version, expected, custody).await.unwrap();
        let calls = backend.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], (version, expected, custody));
        assert!(!verifier.is_closed());
    }
}

#[tokio::test]
async fn historical_custody_is_rejected_before_dispatch_without_closing_admission() {
    for version in [ForkchoiceVersion::V1, ForkchoiceVersion::V2, ForkchoiceVersion::V3] {
        let (verifier, backend) = adapter(Reply::Pending);
        assert!(matches!(
            verifier.forkchoice_updated(version, state(), Some([1; 16])).await,
            Err(EngineVerificationError::Proxy(ProxyError::CustodyVersion))
        ));
        assert!(backend.calls.lock().unwrap().is_empty());
        assert!(!verifier.is_closed());
    }
}

#[tokio::test]
async fn unvalidated_statuses_never_publish_execution_validity() {
    for status in [
        PayloadStatusEnum::Syncing,
        PayloadStatusEnum::Accepted,
        PayloadStatusEnum::Invalid { validation_error: "private policy fixture".into() },
    ] {
        let label = status.as_str();
        let (verifier, backend) = adapter(Reply::Status(status, Some(state().head_block_hash)));
        let result = verifier.forkchoice_updated(ForkchoiceVersion::V3, state(), None).await;
        assert!(
            matches!(result, Err(EngineVerificationError::NotValidated(actual)) if actual == label)
        );
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn ambiguous_hash_jobs_and_response_containers_close_actual_admission() {
    for (index, reply) in [
        Reply::Status(PayloadStatusEnum::Valid, None),
        Reply::Status(PayloadStatusEnum::Valid, Some([9; 32].into())),
        Reply::PayloadJob,
        Reply::Extended,
    ]
    .into_iter()
    .enumerate()
    {
        let (verifier, backend) = adapter(reply);
        let result =
            verifier.forkchoice_updated(ForkchoiceVersion::V4, state(), Some([1; 16])).await;
        if index < 2 {
            assert!(matches!(result, Err(EngineVerificationError::LatestValidHashMismatch)));
        } else {
            assert!(matches!(result, Err(EngineVerificationError::UnexpectedResponse)));
        }
        assert!(verifier.is_closed());
        assert!(matches!(
            verifier.forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
            Err(EngineVerificationError::Proxy(ProxyError::Closed))
        ));
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn admitted_deadline_expiry_closes_future_execution_admission() {
    let (verifier, backend) = adapter(Reply::Pending);
    assert!(matches!(
        verifier.forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
        Err(EngineVerificationError::Proxy(ProxyError::Timeout))
    ));
    assert!(verifier.is_closed());
    assert!(matches!(
        verifier.forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
        Err(EngineVerificationError::Proxy(ProxyError::Closed))
    ));
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn original_engine_errors_are_preserved_for_coordinator_reconciliation() {
    let (verifier, backend) = adapter(Reply::Error);
    assert!(matches!(
        verifier.forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
        Err(EngineVerificationError::Proxy(ProxyError::Engine(TestError)))
    ));
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn zero_head_and_unaudited_extended_method_do_not_start_an_engine_call() {
    let expected = state();
    let (verifier, backend) =
        adapter(Reply::Status(PayloadStatusEnum::Valid, Some(expected.head_block_hash)));
    assert!(matches!(
        verifier.forkchoice_updated(ForkchoiceVersion::V5, expected, None).await,
        Err(EngineVerificationError::Configuration)
    ));
    let empty = ForkchoiceState { head_block_hash: B256::ZERO, ..expected };
    assert!(matches!(
        verifier.forkchoice_updated(ForkchoiceVersion::V3, empty, None).await,
        Err(EngineVerificationError::Configuration)
    ));
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(!verifier.is_closed());
    verifier.forkchoice_updated(ForkchoiceVersion::V3, expected, None).await.unwrap();
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[cfg(feature = "gossip-validation")]
#[tokio::test]
async fn optimistic_forkchoice_retains_syncing_without_a_receipt_and_seals_exact_valid_once() {
    let expected = state();
    let (engine, backend) = adapter(Reply::Status(PayloadStatusEnum::Syncing, None));
    assert!(matches!(
        engine.optimistic_forkchoice_updated(ForkchoiceVersion::V3, expected, None).await.unwrap(),
        OptimisticForkchoice::Syncing
    ));
    assert!(!engine.is_closed());
    assert_eq!(*backend.calls.lock().unwrap(), vec![(ForkchoiceVersion::V3, expected, None)]);
    let (engine, backend) =
        adapter(Reply::Status(PayloadStatusEnum::Valid, Some(expected.head_block_hash)));
    let OptimisticForkchoice::Valid(receipt) = engine
        .optimistic_forkchoice_updated(ForkchoiceVersion::V4, expected, Some([0x5a; 16]))
        .await
        .unwrap()
    else {
        panic!("exact VALID must have a receipt")
    };
    assert!(receipt.matches(&engine, expected));
    assert!(receipt.matches(&engine.clone(), expected));
    let mut changed = expected;
    changed.safe_block_hash = B256::ZERO;
    assert!(!receipt.matches(&engine, changed));
    let (other, _) = adapter(Reply::Pending);
    assert!(!receipt.matches(&other, expected));
    assert_eq!(
        *backend.calls.lock().unwrap(),
        vec![(ForkchoiceVersion::V4, expected, Some([0x5a; 16]))]
    );
}

#[cfg(feature = "gossip-validation")]
#[tokio::test]
async fn optimistic_forkchoice_rejects_jobs_accepted_malformed_and_uncertain_responses() {
    for (index, reply) in [
        Reply::Status(PayloadStatusEnum::Accepted, None),
        Reply::Status(PayloadStatusEnum::Syncing, Some(state().head_block_hash)),
        Reply::Status(PayloadStatusEnum::Valid, None),
        Reply::PayloadJob,
        Reply::Extended,
        Reply::Pending,
    ]
    .into_iter()
    .enumerate()
    {
        let (engine, backend) = adapter(reply);
        let result =
            engine.optimistic_forkchoice_updated(ForkchoiceVersion::V3, state(), None).await;
        match index {
            0 | 3 | 4 => {
                assert!(matches!(result, Err(EngineVerificationError::UnexpectedResponse)))
            }
            1 | 2 => {
                assert!(matches!(result, Err(EngineVerificationError::LatestValidHashMismatch)))
            }
            5 => {
                assert!(matches!(result, Err(EngineVerificationError::Proxy(ProxyError::Timeout))))
            }
            _ => unreachable!(),
        }
        assert!(engine.is_closed());
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
    }
    let (engine, _) = adapter(Reply::Status(
        PayloadStatusEnum::Invalid { validation_error: "actual invalid branch".into() },
        Some(B256::ZERO),
    ));
    assert!(matches!(
        engine.optimistic_forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
        Err(EngineVerificationError::NotValidated("INVALID"))
    ));
    let (engine, _) = adapter(Reply::Error);
    assert!(matches!(
        engine.optimistic_forkchoice_updated(ForkchoiceVersion::V3, state(), None).await,
        Err(EngineVerificationError::Proxy(ProxyError::Engine(TestError)))
    ));
}
