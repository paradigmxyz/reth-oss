//! Typed backend policy regressions. Controlled replies prove dispatch/validation policy only;
//! these tests confer no actual execution-validity or consensus import authority.

use super::*;
use alloy_rpc_types_engine::{ExecutionPayloadSidecar, PayloadStatus, PayloadStatusV2};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_execution_proxy::{ForkchoiceVersion, ProxyError};
use reth_embedded_beacon_state_transition::{fork_processing, ssz::Ssz};
use std::{sync::Mutex, time::Duration};

#[derive(Debug, thiserror::Error)]
#[error("private typed policy backend unavailable")]
struct TestError;

enum Reply {
    Status(PayloadStatusEnum, Option<B256>),
    Wrong,
    Error,
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
            Reply::Error => Err(TestError),
            Reply::Pending => std::future::pending().await,
        }
    }

    async fn forkchoice_updated(
        &self,
        _version: ForkchoiceVersion,
        _state: (),
        _custody: Option<[u8; 16]>,
    ) -> Result<(), TestError> {
        unreachable!("candidate verification never selects an execution head")
    }
}

fn adapter(fork: Fork, reply: Reply) -> (RuntimeEngineAdapter<Handle>, Arc<Backend>) {
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
        name: "composed-engine-policy-fixture".into(),
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
    (RuntimeEngineAdapter::new(proxy, config).unwrap(), backend)
}

#[derive(Clone)]
enum Candidate {
    Bellatrix(bellatrix::ExecutionPayload, bellatrix_processing::ExecutionContext),
    Capella(capella::ExecutionPayload, capella_processing::ExecutionContext),
    Deneb(deneb::ExecutionPayload, deneb_processing::ExecutionContext),
    Electra(electra::ExecutionPayload, electra_processing::ExecutionContext),
    Fulu(fulu::ExecutionPayload, fulu_processing::ExecutionContext),
    Gloas(gloas::ExecutionPayload, gloas_processing::ExecutionContext),
}

fn hashes() -> Vec<Root> {
    [[13; 48], [14; 48]]
        .iter()
        .map(|commitment| {
            let mut hash = reth_embedded_beacon_primitives::sha256(commitment);
            hash[0] = 1;
            hash
        })
        .collect()
}

impl Candidate {
    fn new(fork: Fork) -> Self {
        macro_rules! input {
            ($variant:ident, $schema:ident, $context:expr) => {{
                let mut payload =
                    $schema::ExecutionPayload::default_value(Preset::Minimal).unwrap();
                payload.parent_hash = [1; 32];
                payload.block_hash = [6; 32];
                payload.timestamp = 112;
                payload.block_number = 2;
                payload.gas_limit = 30_000_000;
                payload.gas_used = 21_000;
                payload.transactions = $schema::Transactions::from_vec(vec![
                    $schema::Transaction::from_vec(vec![0x02, 0xc0]),
                ]);
                Self::$variant(payload, $context)
            }};
        }
        let classic = bellatrix_processing::ExecutionContext {
            slot: 2,
            parent_beacon_block_root: [8; 32],
            previous_execution_payload_hash: Some([1; 32]),
        };
        macro_rules! context {
            ($processing:ident, $parent:expr) => {
                $processing::ExecutionContext {
                    slot: 2,
                    parent_beacon_block_root: [8; 32],
                    previous_execution_payload_hash: $parent,
                    expected_blob_versioned_hashes: hashes(),
                    execution_requests_list: vec![],
                }
            };
        }
        let mut candidate = match fork {
            Fork::Bellatrix => input!(Bellatrix, bellatrix, classic),
            Fork::Capella => input!(Capella, capella, classic),
            Fork::Deneb => input!(
                Deneb,
                deneb,
                deneb_processing::ExecutionContext {
                    slot: 2,
                    parent_beacon_block_root: [8; 32],
                    previous_execution_payload_hash: Some([1; 32]),
                    expected_blob_versioned_hashes: hashes(),
                }
            ),
            Fork::Electra => input!(Electra, electra, context!(electra_processing, Some([1; 32]))),
            Fork::Fulu => input!(Fulu, fulu, context!(fulu_processing, Some([1; 32]))),
            Fork::Gloas => input!(Gloas, gloas, context!(gloas_processing, [1; 32])),
            _ => panic!("execution-bearing fork required"),
        };
        if let Self::Gloas(payload, _) = &mut candidate {
            payload.slot_number = 2;
            payload.block_access_list = gloas::BlockAccessList::from_vec(vec![0xff, 0x42]);
        }
        if let Self::Capella(payload, _) = &mut candidate {
            payload.withdrawals = capella::Withdrawals::from_vec(vec![capella::Withdrawal {
                index: 7,
                validator_index: 9,
                address: [4; 20],
                amount: 32,
            }]);
        }
        candidate
    }

    async fn verify(
        &self,
        adapter: &RuntimeEngineAdapter<Handle>,
    ) -> Result<(), EngineVerificationError<TestError>> {
        macro_rules! verify {
            ($processing:ident, $payload:expr, $context:expr) => {
                $processing::ExecutionEngine::verify_payload(adapter, $payload, $context.clone())
                    .await
            };
        }
        match self {
            Self::Bellatrix(payload, context) => verify!(bellatrix_processing, payload, context),
            Self::Capella(payload, context) => verify!(capella_processing, payload, context),
            Self::Deneb(payload, context) => verify!(deneb_processing, payload, context),
            Self::Electra(payload, context) => verify!(electra_processing, payload, context),
            Self::Fulu(payload, context) => verify!(fulu_processing, payload, context),
            Self::Gloas(payload, context) => verify!(gloas_processing, payload, context),
        }
    }
}

#[tokio::test]
async fn composed_adapter_implements_all_six_traits_and_preserves_method_and_sidecar_inputs() {
    fn complete<E: fork_processing::ExecutionEngine>(_: &E) {}
    for (fork, version) in [
        (Fork::Bellatrix, PayloadVersion::V1),
        (Fork::Capella, PayloadVersion::V2),
        (Fork::Deneb, PayloadVersion::V3),
        (Fork::Electra, PayloadVersion::V4),
        (Fork::Fulu, PayloadVersion::V4),
        (Fork::Gloas, PayloadVersion::V5),
    ] {
        let candidate = Candidate::new(fork);
        let (verifier, backend) =
            adapter(fork, Reply::Status(PayloadStatusEnum::Valid, Some([6; 32].into())));
        complete(&verifier);
        candidate.verify(&verifier).await.unwrap();
        let observed = backend.observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0, version);
        let data = &observed[0].1;
        assert_eq!(data.payload.as_v1().transactions[0].as_ref(), &[0x02, 0xc0]);
        assert_eq!(data.payload.as_v1().parent_hash, B256::repeat_byte(1));
        assert_eq!(data.payload.as_v1().timestamp, 112);
        if fork >= Fork::Deneb {
            assert_eq!(data.sidecar.parent_beacon_block_root(), Some(B256::repeat_byte(8)));
            assert_eq!(
                data.sidecar.versioned_hashes().unwrap(),
                &hashes().into_iter().map(B256::from).collect::<Vec<_>>()
            );
            if fork == Fork::Deneb {
                assert_eq!(data.sidecar.requests(), None);
            }
        } else {
            assert_eq!(data.sidecar.parent_beacon_block_root(), None);
            assert_eq!(data.sidecar.versioned_hashes(), None);
        }
        if fork == Fork::Gloas {
            let payload = data.payload.as_v4().unwrap();
            assert_eq!(payload.slot_number, 2);
            assert_eq!(payload.block_access_list.as_ref(), &[0xff, 0x42]);
        }
        if let Candidate::Capella(payload, _) = candidate {
            let mut block = capella::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.body.execution_payload = payload;
            let expected = reth_embedded_beacon_execution_proxy::payload::from_beacon_block(
                &reth_embedded_beacon_state_transition::ssz::BeaconBlock::Capella(
                    block,
                    Preset::Minimal,
                ),
            )
            .unwrap();
            assert_eq!(data.payload, expected.payload);
            assert_same_sidecar(&data.sidecar, &expected.sidecar);
        } else if let Candidate::Deneb(payload, _) = candidate {
            let mut block = deneb::BeaconBlock::default_value(Preset::Minimal).unwrap();
            block.parent_root = [8; 32];
            block.body.execution_payload = payload;
            block.body.blob_kzg_commitments =
                deneb::BlobKZGCommitments::from_vec(vec![[13; 48], [14; 48]]);
            let expected = reth_embedded_beacon_execution_proxy::payload::from_beacon_block(
                &reth_embedded_beacon_state_transition::ssz::BeaconBlock::Deneb(
                    block,
                    Preset::Minimal,
                ),
            )
            .unwrap();
            assert_eq!(data.payload, expected.payload);
            assert_same_sidecar(&data.sidecar, &expected.sidecar);
        }
    }
}

#[tokio::test]
async fn capella_and_deneb_reject_all_unvalidated_statuses_without_claiming_import() {
    for fork in [Fork::Capella, Fork::Deneb] {
        for status in [
            PayloadStatusEnum::Accepted,
            PayloadStatusEnum::Syncing,
            PayloadStatusEnum::Invalid { validation_error: "invalid".into() },
        ] {
            let candidate = Candidate::new(fork);
            let (verifier, backend) = adapter(fork, Reply::Status(status, Some([6; 32].into())));
            assert!(matches!(
                candidate.verify(&verifier).await,
                Err(EngineVerificationError::NotValidated(_))
            ));
            assert_eq!(backend.observed.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn ambiguous_capella_deneb_replies_and_timeouts_close_the_shared_composed_proxy() {
    for fork in [Fork::Capella, Fork::Deneb] {
        for (index, reply) in [
            Reply::Status(PayloadStatusEnum::Valid, None),
            Reply::Status(PayloadStatusEnum::Valid, Some(B256::ZERO)),
            Reply::Wrong,
            Reply::Pending,
        ]
        .into_iter()
        .enumerate()
        {
            let candidate = Candidate::new(fork);
            let (verifier, backend) = adapter(fork, reply);
            let result = candidate.verify(&verifier).await;
            match index {
                0 | 1 => {
                    assert!(matches!(result, Err(EngineVerificationError::LatestValidHashMismatch)))
                }
                2 => assert!(matches!(result, Err(EngineVerificationError::UnexpectedResponse))),
                3 => assert!(matches!(
                    result,
                    Err(EngineVerificationError::Proxy(ProxyError::Timeout))
                )),
                _ => unreachable!(),
            }
            assert!(matches!(
                candidate.verify(&verifier).await,
                Err(EngineVerificationError::Proxy(ProxyError::Closed))
            ));
            assert_eq!(backend.observed.lock().unwrap().len(), 1);
        }
        let candidate = Candidate::new(fork);
        let (verifier, backend) = adapter(fork, Reply::Error);
        assert!(matches!(
            candidate.verify(&verifier).await,
            Err(EngineVerificationError::Proxy(ProxyError::Engine(TestError)))
        ));
        assert_eq!(backend.observed.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn capella_deneb_reject_wrong_context_and_unproven_terminal_candidates_before_dispatch() {
    macro_rules! check {
        ($fork:ident, $variant:ident, $processing:ident) => {{
            let Candidate::$variant(mut payload, mut context) = Candidate::new(Fork::$fork) else {
                unreachable!();
            };
            let (verifier, backend) = adapter(Fork::$fork, Reply::Wrong);
            payload.timestamp += 1;
            assert!(matches!(
                $processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                    .await,
                Err(EngineVerificationError::TimestampMismatch)
            ));
            payload.timestamp -= 1;
            context.previous_execution_payload_hash = Some([99; 32]);
            assert!(matches!(
                $processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                    .await,
                Err(EngineVerificationError::ParentMismatch)
            ));
            context.previous_execution_payload_hash = None;
            assert!(matches!(
                $processing::ExecutionEngine::verify_terminal_block(
                    &verifier,
                    &payload,
                    context.clone(),
                )
                .await,
                Err(EngineVerificationError::TerminalVerificationUnavailable)
            ));
            assert!(matches!(
                $processing::ExecutionEngine::verify_payload(&verifier, &payload, context.clone())
                    .await,
                Err(EngineVerificationError::TerminalVerificationUnavailable)
            ));
            context.previous_execution_payload_hash = Some([1; 32]);
            let (other_fork, other_backend) = adapter(Fork::Bellatrix, Reply::Wrong);
            assert!(matches!(
                $processing::ExecutionEngine::verify_payload(&other_fork, &payload, context).await,
                Err(EngineVerificationError::ForkMismatch)
            ));
            assert!(backend.observed.lock().unwrap().is_empty());
            assert!(other_backend.observed.lock().unwrap().is_empty());
        }};
    }
    check!(Capella, Capella, capella_processing);
    check!(Deneb, Deneb, deneb_processing);
}

#[tokio::test]
async fn malformed_deneb_context_is_rejected_before_typed_dispatch() {
    let Candidate::Deneb(payload, mut context) = Candidate::new(Fork::Deneb) else {
        unreachable!();
    };
    let (verifier, backend) = adapter(Fork::Deneb, Reply::Wrong);
    context.expected_blob_versioned_hashes[0][0] = 2;
    assert!(matches!(
        deneb_processing::ExecutionEngine::verify_payload(&verifier, &payload, context).await,
        Err(EngineVerificationError::Conversion(_))
    ));
    assert!(backend.observed.lock().unwrap().is_empty());
}

fn assert_same_sidecar(actual: &ExecutionPayloadSidecar, expected: &ExecutionPayloadSidecar) {
    assert_eq!(actual.cancun().is_some(), expected.cancun().is_some());
    assert_eq!(actual.parent_beacon_block_root(), expected.parent_beacon_block_root());
    assert_eq!(actual.versioned_hashes(), expected.versioned_hashes());
    assert_eq!(actual.prague().is_some(), expected.prague().is_some());
    assert_eq!(actual.requests(), expected.requests());
    assert_eq!(actual.requests_hash(), expected.requests_hash());
    assert_eq!(actual.bogota().is_some(), expected.bogota().is_some());
    assert_eq!(actual.inclusion_list_transactions(), expected.inclusion_list_transactions());
}

mod pending;

#[cfg(feature = "gossip-validation")]
mod optimistic;

#[tokio::test]
async fn cloned_runtime_keeps_the_same_exact_pending_attempt_feedback() {
    let (engine, backend) = adapter(Fork::Fulu, Reply::Status(PayloadStatusEnum::Syncing, None));
    let shared = engine.clone();
    let original_root = [0x5a; 32];
    engine.begin_import_attempt(original_root).unwrap();
    assert!(matches!(
        Candidate::new(Fork::Fulu).verify(&shared).await,
        Err(EngineVerificationError::NotValidated("SYNCING"))
    ));
    let pending = engine.take_pending_import(original_root).unwrap().unwrap();
    assert_eq!(pending.hash, [6; 32]);
    assert_eq!(pending.status, super::pending::PendingPayloadStatus::Syncing);
    assert!(shared.take_pending_import(original_root).unwrap().is_none());
    assert_eq!(backend.observed.lock().unwrap().len(), 1);
}
