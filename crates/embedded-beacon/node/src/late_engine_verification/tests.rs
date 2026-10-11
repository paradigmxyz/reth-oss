//! Policy regressions use a private typed backend; they are not real execution validation.

use super::*;
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusV2};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_execution_proxy::{ForkchoiceVersion, ProxyError};
use std::{sync::Mutex, time::Duration};

#[derive(Debug, thiserror::Error)]
#[error("private fixture backend unavailable")]
struct TestError;

enum Reply {
    Status(PayloadStatusEnum, Option<B256>),
    Wrong,
    Pending,
}

struct Backend {
    reply: Mutex<Option<Reply>>,
    observed: Mutex<Vec<(PayloadVersion, ExecutionData)>>,
}

#[derive(Clone)]
struct Handle(Arc<Backend>);

impl TypedExecutionEngine for Handle {
    type Payload = ExecutionData;
    type Forkchoice = ();
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ();
    type Error = TestError;

    async fn new_payload(
        &self,
        version: PayloadVersion,
        data: ExecutionData,
    ) -> Result<PayloadReply, TestError> {
        self.0.observed.lock().unwrap().push((version, data));
        let reply = self.0.reply.lock().unwrap().take().unwrap();
        match reply {
            Reply::Status(status, hash) => Ok(PayloadReply::V1(PayloadStatus::new(status, hash))),
            Reply::Wrong => Ok(PayloadReply::V2(PayloadStatusV2::new(
                PayloadStatus::new(PayloadStatusEnum::Valid, Some([6; 32].into())),
                None,
            ))),
            Reply::Pending => std::future::pending().await,
        }
    }

    async fn forkchoice_updated(
        &self,
        _version: ForkchoiceVersion,
        _state: (),
        _custody: Option<[u8; 16]>,
    ) -> Result<(), TestError> {
        unreachable!("payload verification never sets fork choice")
    }
}

fn adapter(fork: Fork, reply: Reply) -> (LateForkEngineAdapter<Handle>, Arc<Backend>) {
    let forks = [
        Fork::Phase0,
        Fork::Altair,
        Fork::Bellatrix,
        Fork::Capella,
        Fork::Deneb,
        Fork::Electra,
        Fork::Fulu,
        Fork::Gloas,
    ];
    let config = ChainConfig {
        name: "late-engine-fixture".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 12345,
        genesis_time: 100,
        genesis_validators_root: [7; 32],
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        forks: ForkSchedule::new(
            forks
                .into_iter()
                .take_while(|item| *item <= fork)
                .enumerate()
                .map(|(index, fork)| ForkActivation {
                    fork,
                    epoch: 0,
                    version: [index as u8, 0, 0, 1],
                })
                .collect(),
        )
        .unwrap(),
        blob_schedule: vec![],
        max_payload_size: 10 * 1024 * 1024,
    };
    let backend =
        Arc::new(Backend { reply: Mutex::new(Some(reply)), observed: Mutex::new(Vec::new()) });
    let proxy = ExecutionProxy::new(Handle(backend.clone()), Duration::from_millis(20)).unwrap();
    (LateForkEngineAdapter::new(proxy, config).unwrap(), backend)
}

macro_rules! input {
    ($name:ident, $schema:ident, $processing:ident, $parent:expr, $($extra:ident:$value:expr),*) => {
        fn $name() -> ($schema::ExecutionPayload, $processing::ExecutionContext) {
            let payload = $schema::ExecutionPayload {
                parent_hash: [1; 32], fee_recipient: [2; 20], state_root: [3; 32],
                receipts_root: [4; 32], logs_bloom: $schema::LogsBloom::from_vec(vec![0; 256]),
                prev_randao: [5; 32], block_number: 2, gas_limit: 30_000_000,
                gas_used: 21_000, timestamp: 112, extra_data: Default::default(),
                base_fee_per_gas: [0; 32], block_hash: [6; 32],
                transactions: $schema::Transactions::from_vec(vec![
                    $schema::Transaction::from_vec(vec![0x02, 0xc0])]),
                withdrawals: Default::default(), blob_gas_used: 0, excess_blob_gas: 0,
                $($extra:$value,)*
            };
            let context = $processing::ExecutionContext {
                slot: 2, parent_beacon_block_root: [8; 32],
                previous_execution_payload_hash: $parent,
                expected_blob_versioned_hashes: vec![[1; 32]],
                execution_requests_list: vec![],
            };
            (payload, context)
        }
    };
}
input!(electra_input, electra, electra_processing, Some([1; 32]),);
input!(fulu_input, fulu, fulu_processing, Some([1; 32]),);
input!(gloas_input, gloas, gloas_processing, [1; 32],
    block_access_list: gloas::BlockAccessList::from_vec(vec![0xff, 0x42]), slot_number:2);

#[tokio::test]
async fn later_forks_dispatch_exact_engine_methods_and_sidecars() {
    let (payload, context) = electra_input();
    let (verifier, backend) = adapter(
        Fork::Electra,
        Reply::Status(PayloadStatusEnum::Valid, Some(payload.block_hash.into())),
    );
    electra_processing::ExecutionEngine::verify_payload(&verifier, &payload, context)
        .await
        .unwrap();
    {
        let observed = backend.observed.lock().unwrap();
        assert_eq!(observed[0].0, PayloadVersion::V4);
        assert_eq!(observed[0].1.payload.as_v1().transactions[0].as_ref(), &[0x02, 0xc0]);
        assert_eq!(observed[0].1.sidecar.parent_beacon_block_root(), Some([8; 32].into()));
        assert_eq!(observed[0].1.sidecar.versioned_hashes().unwrap(), &[B256::repeat_byte(1)]);
    }

    let (payload, context) = fulu_input();
    let (verifier, backend) = adapter(
        Fork::Fulu,
        Reply::Status(PayloadStatusEnum::Valid, Some(payload.block_hash.into())),
    );
    fulu_processing::ExecutionEngine::verify_payload(&verifier, &payload, context).await.unwrap();
    assert_eq!(backend.observed.lock().unwrap()[0].0, PayloadVersion::V4);

    let (payload, context) = gloas_input();
    let (verifier, backend) = adapter(
        Fork::Gloas,
        Reply::Status(PayloadStatusEnum::Valid, Some(payload.block_hash.into())),
    );
    gloas_processing::ExecutionEngine::verify_payload(&verifier, &payload, context).await.unwrap();
    let observed = backend.observed.lock().unwrap();
    assert_eq!(observed[0].0, PayloadVersion::V5);
    let converted = observed[0].1.payload.as_v4().unwrap();
    assert_eq!(converted.slot_number, 2);
    assert_eq!(converted.block_access_list.as_ref(), &[0xff, 0x42]);
}

#[tokio::test]
async fn later_forks_reject_ambiguous_verdicts_and_close_unknown_outcomes() {
    let (payload, context) = gloas_input();
    for status in [
        PayloadStatusEnum::Accepted,
        PayloadStatusEnum::Syncing,
        PayloadStatusEnum::Invalid { validation_error: "invalid".into() },
    ] {
        let (verifier, _) =
            adapter(Fork::Gloas, Reply::Status(status, Some(payload.block_hash.into())));
        assert!(matches!(
            gloas_processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                .await,
            Err(EngineVerificationError::NotValidated(_))
        ));
    }
    for (index, reply) in [
        Reply::Status(PayloadStatusEnum::Valid, None),
        Reply::Status(PayloadStatusEnum::Valid, Some(B256::ZERO)),
        Reply::Wrong,
        Reply::Pending,
    ]
    .into_iter()
    .enumerate()
    {
        let (verifier, _) = adapter(Fork::Gloas, reply);
        let result =
            gloas_processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                .await;
        match index {
            0 | 1 => {
                assert!(matches!(result, Err(EngineVerificationError::LatestValidHashMismatch)))
            }
            2 => assert!(matches!(result, Err(EngineVerificationError::UnexpectedResponse))),
            3 => {
                assert!(matches!(result, Err(EngineVerificationError::Proxy(ProxyError::Timeout))))
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            gloas_processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                .await,
            Err(EngineVerificationError::Proxy(ProxyError::Closed))
        ));
    }
}

#[tokio::test]
async fn later_forks_reject_wrong_context_and_unproven_terminal_blocks_before_dispatch() {
    let (mut payload, mut context) = electra_input();
    let (verifier, backend) = adapter(Fork::Electra, Reply::Wrong);
    payload.timestamp += 1;
    assert!(matches!(
        electra_processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
            .await,
        Err(EngineVerificationError::TimestampMismatch)
    ));
    payload.timestamp -= 1;
    context.previous_execution_payload_hash = Some([99; 32]);
    assert!(matches!(
        electra_processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
            .await,
        Err(EngineVerificationError::ParentMismatch)
    ));
    context.previous_execution_payload_hash = None;
    assert!(matches!(
        electra_processing::ExecutionEngine::verify_terminal_block(
            &verifier,
            &payload,
            context.clone()
        )
        .await,
        Err(EngineVerificationError::TerminalVerificationUnavailable)
    ));
    assert!(matches!(
        electra_processing::ExecutionEngine::verify_payload(&verifier, &payload, context).await,
        Err(EngineVerificationError::TerminalVerificationUnavailable)
    ));
    assert!(backend.observed.lock().unwrap().is_empty());
    let (payload, context) = gloas_input();
    let (verifier, backend) = adapter(Fork::Fulu, Reply::Wrong);
    assert!(matches!(
        gloas_processing::ExecutionEngine::verify_payload(&verifier, &payload, context).await,
        Err(EngineVerificationError::ForkMismatch)
    ));
    assert!(backend.observed.lock().unwrap().is_empty());
}
