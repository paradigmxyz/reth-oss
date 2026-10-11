//! Mandatory consensus-to-execution verification using the ordered typed proxy.
//!
//! The proxy must be captured from the execution node built with the same checked network
//! configuration. This adapter validates post-merge Bellatrix candidates. Terminal eligibility
//! remains unavailable until real execution ancestry/total-difficulty evidence is integrated.

use alloy_primitives::B256;
use alloy_rpc_types_engine::{ExecutionData, PayloadStatusEnum};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_execution_proxy::{
    payload::{from_bellatrix_payload, PayloadConversionError},
    ExecutionEngine as TypedExecutionEngine, ExecutionProxy, PayloadReply, PayloadVersion,
    ProxyError,
};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::{
    bellatrix_processing::{ExecutionContext, ExecutionEngine},
    ssz::{bellatrix::ExecutionPayload, Preset},
};
use std::sync::Arc;

/// Invokes real versioned execution validation inside the consensus transition.
#[derive(Debug)]
pub struct BellatrixEngineAdapter<E> {
    proxy: ExecutionProxy<E>,
    config: Arc<ChainConfig>,
    pending: Option<crate::runtime_engine::pending::PendingFeedback>,
}

impl<E> BellatrixEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply>,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Uses the typed API captured from this network's execution node.
    pub fn new(
        proxy: ExecutionProxy<E>,
        config: ChainConfig,
    ) -> Result<Self, EngineVerificationError<E::Error>> {
        config.validate().map_err(|_| EngineVerificationError::Configuration)?;
        config.clock().map_err(|_| EngineVerificationError::Configuration)?;
        Ok(Self { proxy, config: Arc::new(config), pending: None })
    }

    pub(crate) fn with_pending_feedback(
        mut self,
        pending: crate::runtime_engine::pending::PendingFeedback,
    ) -> Self {
        self.pending = Some(pending);
        self
    }
    fn validate_context(
        &self,
        payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<Preset, EngineVerificationError<E::Error>> {
        let activation = self
            .config
            .fork_at_slot(context.slot)
            .map_err(|_| EngineVerificationError::Configuration)?;
        if activation.fork != Fork::Bellatrix {
            return Err(EngineVerificationError::ForkMismatch)
        }
        let expected_time = self
            .config
            .clock()
            .map_err(|_| EngineVerificationError::Configuration)?
            .start_of_slot(context.slot)
            .map_err(|_| EngineVerificationError::Configuration)? /
            1000;
        if payload.timestamp != expected_time {
            return Err(EngineVerificationError::TimestampMismatch)
        }
        if context
            .previous_execution_payload_hash
            .is_some_and(|parent| parent != payload.parent_hash)
        {
            return Err(EngineVerificationError::ParentMismatch)
        }
        Ok(match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        })
    }
}

impl<E> ExecutionEngine for BellatrixEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply>,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_terminal_block<'a>(
        &'a self,
        _payload: &'a ExecutionPayload,
        _context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(EngineVerificationError::TerminalVerificationUnavailable)
    }

    async fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        let preset = self.validate_context(payload, context)?;
        let expected_hash = B256::from(payload.block_hash);
        let data = from_bellatrix_payload(payload, preset)?;
        let timer = self.pending.as_ref().and_then(|pending| pending.measure_execution());
        let response = self.proxy.new_payload(PayloadVersion::V1, data).await?;
        drop(timer);
        let PayloadReply::V1(status) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        };
        if let Some(pending) = &self.pending {
            pending.observe(&status.status, expected_hash.0);
        }
        if !matches!(status.status, PayloadStatusEnum::Valid) {
            return Err(EngineVerificationError::NotValidated(status.status.as_str()))
        }
        if status.latest_valid_hash != Some(expected_hash) {
            self.proxy.close();
            return Err(EngineVerificationError::LatestValidHashMismatch)
        }
        Ok(())
    }
}

/// A failed or ambiguous execution response cannot commit consensus state.
#[derive(Debug, thiserror::Error)]
pub enum EngineVerificationError<E: std::error::Error + 'static> {
    /// Invalid checked configuration or overflowing slot clock.
    #[error("invalid execution-verification configuration")]
    Configuration,
    /// The adapter never silently selects another fork's method.
    #[error("execution verifier received a different active fork")]
    ForkMismatch,
    /// Consensus time differs from the checked network slot clock.
    #[error("execution timestamp does not match beacon slot")]
    TimestampMismatch,
    /// Candidate does not extend the committed execution parent.
    #[error("execution payload parent does not match committed state")]
    ParentMismatch,
    /// Gloas payload slot differs from its authenticated envelope context.
    #[error("execution slot does not match beacon slot")]
    SlotMismatch,
    /// Terminal-block ancestry/total-difficulty evidence has not been implemented.
    #[error("terminal execution block verification is unavailable")]
    TerminalVerificationUnavailable,
    /// Concrete local terminal-provider evidence failed; retain its original source.
    #[error(transparent)]
    TerminalProof(#[from] crate::terminal_verification::TerminalVerificationError),
    /// Checked structural conversion or allocation limit failed.
    #[error(transparent)]
    Conversion(#[from] PayloadConversionError),
    /// Bounded typed transport failure with unchanged engine error.
    #[error(transparent)]
    Proxy(#[from] ProxyError<E>),
    /// ACCEPTED and SYNCING do not establish execution validity.
    #[error("execution payload has status {0}, not VALID")]
    NotValidated(&'static str),
    /// Versioned request received another response container.
    #[error("execution payload response does not match requested method")]
    UnexpectedResponse,
    /// VALID response must identify this exact candidate.
    #[error("execution latest-valid hash does not match candidate")]
    LatestValidHashMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusV2};
    use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
    use reth_embedded_beacon_execution_proxy::ForkchoiceVersion;
    use reth_embedded_beacon_state_transition::ssz::bellatrix;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        time::Duration,
    };

    #[derive(Debug, thiserror::Error)]
    #[error("execution unavailable")]
    struct TestError;

    enum Reply {
        V1(PayloadStatus),
        V2(PayloadStatusV2),
        Error,
        Pending,
    }

    struct Backend {
        reply: Mutex<Option<Reply>>,
        calls: AtomicUsize,
        observed: Mutex<Option<ExecutionData>>,
    }

    #[derive(Clone)]
    struct BackendHandle(Arc<Backend>);

    impl TypedExecutionEngine for BackendHandle {
        type Payload = ExecutionData;
        type Forkchoice = ();
        type PayloadReply = PayloadReply;
        type ForkchoiceReply = ();
        type Error = TestError;
        async fn new_payload(
            &self,
            version: PayloadVersion,
            payload: ExecutionData,
        ) -> Result<PayloadReply, TestError> {
            assert_eq!(version, PayloadVersion::V1);
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            *self.0.observed.lock().unwrap() = Some(payload);
            let reply = self.0.reply.lock().unwrap().take().unwrap();
            match reply {
                Reply::V1(status) => Ok(PayloadReply::V1(status)),
                Reply::V2(status) => Ok(PayloadReply::V2(status)),
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
            unreachable!("payload verification never updates head")
        }
    }

    fn input() -> (ChainConfig, ExecutionPayload, ExecutionContext) {
        let config = ChainConfig {
            name: "engine-verification-fixture".into(),
            preset: ConfigPreset::Minimal,
            chain_id: 12345,
            genesis_time: 100,
            genesis_validators_root: [7; 32],
            seconds_per_slot: 6,
            slots_per_epoch: 8,
            forks: ForkSchedule::new(vec![
                ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0; 4] },
                ForkActivation { fork: Fork::Altair, epoch: 0, version: [1; 4] },
                ForkActivation { fork: Fork::Bellatrix, epoch: 0, version: [2; 4] },
            ])
            .unwrap(),
            blob_schedule: vec![],
            max_payload_size: 10 * 1024 * 1024,
        };
        let payload = ExecutionPayload {
            parent_hash: [1; 32],
            fee_recipient: [2; 20],
            state_root: [3; 32],
            receipts_root: [4; 32],
            logs_bloom: bellatrix::LogsBloom::from_vec(vec![0; 256]),
            prev_randao: [5; 32],
            block_number: 2,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            timestamp: 112,
            extra_data: Default::default(),
            base_fee_per_gas: [0; 32],
            block_hash: [6; 32],
            transactions: bellatrix::Transactions::from_vec(vec![
                bellatrix::Transaction::from_vec(vec![0x02, 0xc0]),
            ]),
        };
        let context = ExecutionContext {
            slot: 2,
            parent_beacon_block_root: [8; 32],
            previous_execution_payload_hash: Some(payload.parent_hash),
        };
        (config, payload, context)
    }

    fn adapter(
        config: ChainConfig,
        reply: Reply,
    ) -> (BellatrixEngineAdapter<BackendHandle>, Arc<Backend>) {
        let backend = Arc::new(Backend {
            reply: Mutex::new(Some(reply)),
            calls: AtomicUsize::new(0),
            observed: Mutex::new(None),
        });
        let proxy =
            ExecutionProxy::new(BackendHandle(backend.clone()), Duration::from_millis(30)).unwrap();
        (BellatrixEngineAdapter::new(proxy, config).unwrap(), backend)
    }

    #[tokio::test]
    async fn exact_valid_response_verifies_exact_payload_once() {
        let (config, payload, context) = input();
        let (verifier, backend) = adapter(
            config,
            Reply::V1(PayloadStatus::new(
                PayloadStatusEnum::Valid,
                Some(payload.block_hash.into()),
            )),
        );
        verifier.verify_payload(&payload, context).await.unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let data = backend.observed.lock().unwrap();
        let data = data.as_ref().unwrap();
        assert_eq!(data.payload.block_hash(), B256::from(payload.block_hash));
        assert!(data.sidecar.cancun().is_none());
        let alloy_rpc_types_engine::ExecutionPayload::V1(converted) = &data.payload else {
            panic!("wrong payload version")
        };
        assert_eq!(converted.transactions[0].as_ref(), &[0x02, 0xc0]);
    }

    #[tokio::test]
    async fn optimistic_invalid_and_malformed_responses_never_verify() {
        let (config, payload, context) = input();
        for status in [
            PayloadStatusEnum::Accepted,
            PayloadStatusEnum::Syncing,
            PayloadStatusEnum::Invalid { validation_error: "rejected".into() },
        ] {
            let (verifier, _) = adapter(
                config.clone(),
                Reply::V1(PayloadStatus::new(status, Some(payload.block_hash.into()))),
            );
            assert!(matches!(
                verifier.verify_payload(&payload, context).await,
                Err(EngineVerificationError::NotValidated(_))
            ));
        }
        for hash in [None, Some(B256::ZERO)] {
            let (verifier, _) = adapter(
                config.clone(),
                Reply::V1(PayloadStatus::new(PayloadStatusEnum::Valid, hash)),
            );
            assert!(matches!(
                verifier.verify_payload(&payload, context).await,
                Err(EngineVerificationError::LatestValidHashMismatch)
            ));
            assert!(matches!(
                verifier.verify_payload(&payload, context).await,
                Err(EngineVerificationError::Proxy(ProxyError::Closed))
            ));
        }
        let extended = PayloadStatusV2::new(
            PayloadStatus::new(PayloadStatusEnum::Valid, Some(payload.block_hash.into())),
            None,
        );
        let (verifier, _) = adapter(config, Reply::V2(extended));
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::UnexpectedResponse)
        ));
    }

    #[tokio::test]
    async fn checked_context_and_terminal_evidence_are_mandatory() {
        let (config, mut payload, mut context) = input();
        let (verifier, backend) = adapter(config.clone(), Reply::Error);
        payload.timestamp += 1;
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::TimestampMismatch)
        ));
        payload.timestamp -= 1;
        context.previous_execution_payload_hash = Some([99; 32]);
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::ParentMismatch)
        ));
        context.previous_execution_payload_hash = None;
        assert!(matches!(
            verifier.verify_terminal_block(&payload, context).await,
            Err(EngineVerificationError::TerminalVerificationUnavailable)
        ));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        let mut earlier = config;
        earlier.forks = ForkSchedule::new(vec![ForkActivation {
            fork: Fork::Phase0,
            epoch: 0,
            version: [0; 4],
        }])
        .unwrap();
        let (verifier, _) = adapter(earlier, Reply::Error);
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::ForkMismatch)
        ));
    }

    #[tokio::test]
    async fn execution_error_and_timeout_preserve_failure() {
        let (config, payload, context) = input();
        let (verifier, _) = adapter(config.clone(), Reply::Error);
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::Proxy(ProxyError::Engine(TestError)))
        ));
        let (verifier, _) = adapter(config, Reply::Pending);
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::Proxy(ProxyError::Timeout))
        ));
        assert!(matches!(
            verifier.verify_payload(&payload, context).await,
            Err(EngineVerificationError::Proxy(ProxyError::Closed))
        ));
    }
}
