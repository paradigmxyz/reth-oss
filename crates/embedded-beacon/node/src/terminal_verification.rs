//! Bounded local-provider terminal ancestry verification for the first execution payload.
//!
//! The provider must be captured from the same validated execution node as the typed Engine API.
//! Complete header hashes, the actual chain specification and original total-difficulty evidence
//! are checked independently. This module neither validates `PoW` itself nor treats newPayload
//! VALID as terminal proof. Synchronous provider reads belong on the consensus owner/blocking
//! thread.

use crate::bootstrap::checked_config_digest;
use alloy_consensus::Header;
use alloy_primitives::{B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecProvider, EthChainSpec};
use reth_embedded_beacon_config::{ChainConfig, DevnetConfig};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_ethereum_forks::{EthereumHardfork, ForkCondition, Hardforks};
use reth_storage_api::{errors::ProviderError, HeaderProvider};
use std::{
    error::Error,
    sync::{Arc, Mutex, TryLockError},
};
use thiserror::Error;

/// An exclusively admitted local-provider verifier, without a queued waiter list.
#[derive(Debug)]
pub struct TerminalVerifier<P> {
    work: Mutex<Work<P>>,
    config: ChainConfig,
    config_digest: Root,
    spec: Arc<ChainSpec>,
    rule: TerminalRule,
    limits: TerminalLimits,
}
impl<P: TerminalExecutionProvider> TerminalVerifier<P> {
    /// Capture the actual execution specification and validate its consensus/profile identity.
    ///
    /// The optional private profile selects only its actual execution genesis, at the checked
    /// activation epoch. There is no constructor taking an asserted hash, threshold or boolean.
    pub fn capture(
        provider: P,
        config: ChainConfig,
        private: Option<&DevnetConfig>,
        limits: TerminalLimits,
    ) -> Result<Self, TerminalVerificationError> {
        limits.validate()?;
        let config_digest =
            checked_config_digest(&config).map_err(|_| TerminalVerificationError::Configuration)?;
        let spec = provider.chain_spec();
        let genesis = spec.genesis_header();
        check_header(genesis)?;
        if spec.chain_id() != config.chain_id ||
            genesis.number != 0 ||
            genesis.parent_hash != B256::ZERO ||
            genesis.hash_slow() != spec.genesis_hash() ||
            spec.genesis().config.chain_id != spec.chain_id()
        {
            return Err(TerminalVerificationError::Identity)
        }
        let ForkCondition::TTD { total_difficulty, .. } = spec.fork(EthereumHardfork::Paris) else {
            return Err(TerminalVerificationError::Configuration)
        };
        if spec
            .genesis()
            .config
            .terminal_total_difficulty
            .is_some_and(|threshold| threshold != total_difficulty)
        {
            return Err(TerminalVerificationError::Configuration)
        }
        let rule = if let Some(private) = private {
            private
                .validate_execution_identity(spec.chain_id(), spec.genesis_hash().into())
                .map_err(|_| TerminalVerificationError::Identity)?;
            if private.consensus != config {
                return Err(TerminalVerificationError::Identity)
            }
            TerminalRule::GenesisOverride {
                hash: spec.genesis_hash(),
                activation_epoch: private.terminal_block_hash_activation_epoch,
            }
        } else {
            TerminalRule::TotalDifficulty(total_difficulty)
        };
        Ok(Self { work: Mutex::new(Work { provider }), config, config_digest, spec, rule, limits })
    }

    /// Verify the first payload's exact execution parent using at most five provider reads.
    ///
    /// The slot is taken from the normative consensus transition. Missing header/TD evidence
    /// may become available later, but cannot supply a terminal proof now. A concurrent caller
    /// receives Busy immediately rather than joining an unbounded blocking queue.
    pub fn verify(
        &self,
        parent_hash: Root,
        slot: u64,
        candidate_number: u64,
    ) -> Result<VerifiedTerminalParent, TerminalVerificationError> {
        let work = match self.work.try_lock() {
            Ok(work) => work,
            Err(TryLockError::WouldBlock) => return Err(TerminalVerificationError::Busy),
            Err(TryLockError::Poisoned(_)) => {
                return Err(TerminalVerificationError::ProviderPoisoned)
            }
        };
        if !Arc::ptr_eq(&self.spec, &work.provider.chain_spec()) {
            return Err(TerminalVerificationError::Identity)
        }
        let active = self
            .config
            .fork_at_slot(slot)
            .map_err(|_| TerminalVerificationError::Configuration)?
            .fork;
        if matches!(active, Fork::Phase0 | Fork::Altair) || parent_hash == [0; 32] {
            return Err(TerminalVerificationError::Context)
        }
        self.config
            .clock()
            .map_err(|_| TerminalVerificationError::Configuration)?
            .start_of_slot(slot)
            .map_err(|_| TerminalVerificationError::Configuration)?;
        let mut budget = ReadBudget { remaining: self.limits.max_provider_reads };
        let genesis = read_header(&work.provider, self.spec.genesis_hash(), &mut budget)?;
        if genesis != *self.spec.genesis_header() ||
            genesis.number != 0 ||
            genesis.parent_hash != B256::ZERO
        {
            return Err(TerminalVerificationError::Identity)
        }
        let hash = B256::from(parent_hash);
        let (terminal, total_difficulty) = match self.rule {
            TerminalRule::GenesisOverride { hash: selected, activation_epoch } => {
                if slot / self.config.slots_per_epoch < activation_epoch {
                    return Err(TerminalVerificationError::Activation)
                }
                if hash != selected {
                    return Err(TerminalVerificationError::WrongTerminalHash)
                }
                // The explicit override is authenticated by the complete local genesis header.
                // The pinned rule does not require an invented TTD crossing or PoW parent.
                (genesis, None)
            }
            TerminalRule::TotalDifficulty(threshold) => {
                let terminal = read_header(&work.provider, hash, &mut budget)?;
                if terminal.number == 0 ||
                    terminal.parent_hash == B256::ZERO ||
                    terminal.difficulty == U256::ZERO
                {
                    return Err(TerminalVerificationError::Ancestry)
                }
                let parent = read_header(&work.provider, terminal.parent_hash, &mut budget)?;
                if parent.number.checked_add(1) != Some(terminal.number) ||
                    terminal.timestamp <= parent.timestamp
                {
                    return Err(TerminalVerificationError::Ancestry)
                }
                let terminal_td = read_td(&work.provider, hash, &mut budget)?;
                let parent_td = read_td(&work.provider, terminal.parent_hash, &mut budget)?;
                if parent_td.checked_add(terminal.difficulty) != Some(terminal_td) {
                    return Err(TerminalVerificationError::DifficultyArithmetic)
                }
                if parent.number == 0 &&
                    (parent.hash_slow() != self.spec.genesis_hash() ||
                        parent_td != parent.difficulty)
                {
                    return Err(TerminalVerificationError::Identity)
                }
                if terminal_td < threshold || parent_td >= threshold {
                    return Err(TerminalVerificationError::Threshold)
                }
                (terminal, Some(terminal_td))
            }
        };
        let candidate_time = self
            .config
            .clock()
            .map_err(|_| TerminalVerificationError::Configuration)?
            .start_of_slot(slot)
            .map_err(|_| TerminalVerificationError::Configuration)? /
            1000;
        if terminal.number.checked_add(1) != Some(candidate_number) ||
            terminal.timestamp >= candidate_time
        {
            return Err(TerminalVerificationError::Ancestry)
        }
        Ok(VerifiedTerminalParent {
            parent_hash,
            slot,
            candidate_number,
            config_digest: self.config_digest,
            execution_genesis: self.spec.genesis_hash().into(),
            terminal_number: terminal.number,
            total_difficulty,
        })
    }
}

/// Private sealed bridge consumed only by the composed runtime adapter.
/// Provider construction is public; caller-defined validity claims cannot implement this bridge.
pub(crate) trait TerminalAuthority:
    authority_sealed::Sealed + Send + Sync + std::fmt::Debug
{
    /// Check immutable configuration before capturing this authority in the runtime adapter.
    fn configuration_matches(&self, config: &ChainConfig) -> bool;
    /// Verify actual provider evidence against this adapter's independently checked configuration.
    fn verify_context(
        &self,
        config: &ChainConfig,
        parent_hash: Root,
        slot: u64,
        candidate_number: u64,
    ) -> Result<(), TerminalVerificationError>;
}
impl<P: TerminalExecutionProvider> authority_sealed::Sealed for TerminalVerifier<P> {}
impl<P: TerminalExecutionProvider> TerminalAuthority for TerminalVerifier<P> {
    fn configuration_matches(&self, config: &ChainConfig) -> bool {
        config == &self.config
    }
    fn verify_context(
        &self,
        config: &ChainConfig,
        parent_hash: Root,
        slot: u64,
        candidate_number: u64,
    ) -> Result<(), TerminalVerificationError> {
        if config != &self.config {
            return Err(TerminalVerificationError::Identity)
        }
        let proof = self.verify(parent_hash, slot, candidate_number)?;
        if !proof.matches(
            config,
            parent_hash,
            slot,
            candidate_number,
            self.spec.genesis_hash().into(),
        ) {
            return Err(TerminalVerificationError::Identity)
        }
        Ok(())
    }
}
mod authority_sealed {
    pub(crate) trait Sealed {}
}

/// Actual Reth local header provider bridge captured directly from the built execution node.
/// Modern `HeaderProvider` exposes no historical TD query. Only this exact genesis has a known
/// total difficulty from its authenticated complete header; every other missing TD stays absent.
#[derive(Debug)]
pub struct RethTerminalProvider<P> {
    provider: P,
}
impl<P> TerminalExecutionProvider for RethTerminalProvider<P>
where
    P: HeaderProvider<Header = Header> + ChainSpecProvider<ChainSpec = ChainSpec> + Sync,
{
    type Error = ProviderError;
    fn chain_spec(&self) -> Arc<ChainSpec> {
        self.provider.chain_spec()
    }
    fn header(&self, hash: B256) -> Result<Option<Header>, Self::Error> {
        self.provider.header(hash)
    }
    fn total_difficulty(&self, hash: B256) -> Result<Option<U256>, Self::Error> {
        let spec = self.provider.chain_spec();
        if hash == spec.genesis_hash() {
            Ok(Some(spec.genesis_header().difficulty))
        } else {
            Ok(None)
        }
    }
}

/// Capture real Reth header/spec access without a JSON-RPC server or caller validity declaration.
/// The launcher calls this with the provider from the same built node that supplies its typed API.
pub fn capture_checked_provider<P>(
    provider: P,
    config: ChainConfig,
    private: Option<&DevnetConfig>,
    limits: TerminalLimits,
) -> Result<TerminalVerifier<RethTerminalProvider<P>>, TerminalVerificationError>
where
    P: HeaderProvider<Header = Header> + ChainSpecProvider<ChainSpec = ChainSpec> + Sync,
{
    TerminalVerifier::capture(RethTerminalProvider { provider }, config, private, limits)
}

/// Original synchronous evidence from the captured, independently validating execution node.
/// Implementations must return that node's immutable specification and complete local headers.
/// Missing TD must remain None; a VALID payload response cannot replace historical TD evidence.
pub trait TerminalExecutionProvider: Send + Sync + std::fmt::Debug {
    /// Preserve the original provider error type inside the returned error source.
    type Error: Error + Send + Sync + 'static;
    /// The exact Arc captured from this node, stable throughout the verifier's lifetime.
    fn chain_spec(&self) -> Arc<ChainSpec>;
    /// The complete locally validated execution header under this exact hash.
    fn header(&self, hash: B256) -> Result<Option<Header>, Self::Error>;
    /// Original total difficulty for the exact complete header, if retained/authenticated.
    fn total_difficulty(&self, hash: B256) -> Result<Option<U256>, Self::Error>;
}

/// Fixed local work bound; excess or concurrent work never enters an unbounded queue.
#[derive(Clone, Copy, Debug)]
pub struct TerminalLimits {
    /// Maximum header and TD reads for one proof, bounded to one through sixteen.
    pub max_provider_reads: u8,
}
impl Default for TerminalLimits {
    fn default() -> Self {
        Self { max_provider_reads: 5 }
    }
}
impl TerminalLimits {
    const fn validate(self) -> Result<(), TerminalVerificationError> {
        if self.max_provider_reads == 0 || self.max_provider_reads > 16 {
            return Err(TerminalVerificationError::Bounds)
        }
        Ok(())
    }
}

/// Sealed terminal ancestry evidence, independently bound to the checked network and slot.
#[derive(Debug)]
pub struct VerifiedTerminalParent {
    parent_hash: Root,
    slot: u64,
    candidate_number: u64,
    config_digest: Root,
    execution_genesis: Root,
    terminal_number: u64,
    total_difficulty: Option<U256>,
}
impl VerifiedTerminalParent {
    /// Actual complete terminal execution header hash.
    pub const fn parent_hash(&self) -> Root {
        self.parent_hash
    }
    /// Genuine terminal header number; an explicit checked genesis override has number zero.
    pub const fn terminal_number(&self) -> u64 {
        self.terminal_number
    }
    /// Actual TTD evidence, absent only for the spec-defined authenticated genesis override.
    pub const fn total_difficulty(&self) -> Option<U256> {
        self.total_difficulty
    }
    /// Bind this sealed result to the exact normative transition context, never another network.
    pub(crate) fn matches(
        &self,
        config: &ChainConfig,
        parent_hash: Root,
        slot: u64,
        candidate_number: u64,
        execution_genesis: Root,
    ) -> bool {
        checked_config_digest(config).ok() == Some(self.config_digest) &&
            parent_hash == self.parent_hash &&
            slot == self.slot &&
            candidate_number == self.candidate_number &&
            execution_genesis == self.execution_genesis
    }
}

/// Explicit failures do not manufacture terminal eligibility or flatten provider errors.
#[derive(Debug, Error)]
pub enum TerminalVerificationError {
    /// Invalid actual execution fork configuration or consensus clock.
    #[error("invalid terminal execution configuration")]
    Configuration,
    /// Captured execution chain, genesis, specification allocation or private profile differs.
    #[error("terminal execution provider identity mismatch")]
    Identity,
    /// Provider reads must be explicitly bounded.
    #[error("invalid terminal verification work bounds")]
    Bounds,
    /// The exclusively admitted local provider is already verifying another candidate.
    #[error("terminal execution verifier is busy")]
    Busy,
    /// A previous panic leaves the exclusive provider owner unusable.
    #[error("terminal execution provider owner is poisoned")]
    ProviderPoisoned,
    /// Complete locally validated headers are currently unavailable.
    #[error("terminal execution header is unavailable")]
    MissingHeader,
    /// Original authenticated TD is currently unavailable.
    #[error("terminal execution total difficulty is unavailable")]
    MissingDifficulty,
    /// Recomputed complete header differs from its requested hash.
    #[error("terminal execution header hash mismatch")]
    HeaderHash,
    /// Variable header data exceeded the canonical fixed bound before hashing.
    #[error("terminal execution header extra data exceeds 32 bytes")]
    HeaderBounds,
    /// Exact parent hash, number or time relation is invalid.
    #[error("terminal execution ancestry mismatch")]
    Ancestry,
    /// Original TD does not equal the checked parent TD plus this header's difficulty.
    #[error("terminal execution difficulty arithmetic mismatch or overflow")]
    DifficultyArithmetic,
    /// This exact block does not cross the actual configured terminal threshold.
    #[error("terminal execution total difficulty threshold not crossed")]
    Threshold,
    /// No payload can select the override before its authenticated activation epoch.
    #[error("terminal execution override activation not reached")]
    Activation,
    /// The first execution payload must extend the exact authenticated override hash.
    #[error("terminal execution override hash mismatch")]
    WrongTerminalHash,
    /// A zero parent or pre-execution consensus slot cannot establish a Merge parent.
    #[error("invalid terminal execution transition context")]
    Context,
    /// Work was stopped before another local storage read.
    #[error("terminal execution verification read budget exhausted")]
    ReadBudget,
    /// Preserve the original local provider error and its source/downcast information.
    #[error("terminal execution provider failed: {0}")]
    Provider(#[source] Box<dyn Error + Send + Sync>),
}

#[derive(Debug)]
struct Work<P> {
    provider: P,
}
#[derive(Clone, Copy, Debug)]
enum TerminalRule {
    TotalDifficulty(U256),
    GenesisOverride { hash: B256, activation_epoch: u64 },
}
struct ReadBudget {
    remaining: u8,
}
impl ReadBudget {
    fn take(&mut self) -> Result<(), TerminalVerificationError> {
        self.remaining =
            self.remaining.checked_sub(1).ok_or(TerminalVerificationError::ReadBudget)?;
        Ok(())
    }
}
fn check_header(header: &Header) -> Result<(), TerminalVerificationError> {
    if header.extra_data.len() > 32 {
        return Err(TerminalVerificationError::HeaderBounds)
    }
    Ok(())
}
fn read_header<P: TerminalExecutionProvider>(
    provider: &P,
    hash: B256,
    budget: &mut ReadBudget,
) -> Result<Header, TerminalVerificationError> {
    budget.take()?;
    let header = provider
        .header(hash)
        .map_err(|error| TerminalVerificationError::Provider(Box::new(error)))?
        .ok_or(TerminalVerificationError::MissingHeader)?;
    check_header(&header)?;
    if header.hash_slow() != hash {
        return Err(TerminalVerificationError::HeaderHash)
    }
    Ok(header)
}
fn read_td<P: TerminalExecutionProvider>(
    provider: &P,
    hash: B256,
    budget: &mut ReadBudget,
) -> Result<U256, TerminalVerificationError> {
    budget.take()?;
    provider
        .total_difficulty(hash)
        .map_err(|error| TerminalVerificationError::Provider(Box::new(error)))?
        .ok_or(TerminalVerificationError::MissingDifficulty)
}

#[cfg(test)]
mod tests;
