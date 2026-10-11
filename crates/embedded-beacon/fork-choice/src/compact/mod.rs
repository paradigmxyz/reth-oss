//! Owned reads for admitted hot/cold states. Data providers never create graph authority.
//!
//! The graph constructs every binding from an admitted block or an actual checkpoint replay.
//! Cold reconstruction is checked again against that private binding before consensus reads it.
//! Ordinary stores keep their existing borrowed hot-state API; compact wrappers expose guards.

use crate::StoreError;
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Fork, Root, SszError};
use reth_embedded_beacon_state_transition::ssz::{self, gloas, BeaconState, Preset, Ssz};
use std::{
    collections::BTreeSet,
    fmt::Debug,
    ops::Deref,
    sync::{Arc, Mutex, Weak},
};
use thiserror::Error;

/// Exact checked repository identity. Neither digest grants consensus authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateSourceIdentity {
    /// Full network and storage-profile digest.
    pub profile_digest: Root,
    /// Exact checked consensus configuration digest.
    pub consensus_config_digest: Root,
}

/// Binding constructed exclusively from private, actually admitted graph data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateBinding {
    origin_block_root: Root,
    origin_state_root: Root,
    state_root: Root,
    fork: Fork,
    preset: Preset,
    slot: u64,
    genesis_validators_root: Root,
    genesis_time: u64,
    identity: StateSourceIdentity,
    checkpoint_epoch: Option<u64>,
}
impl StateBinding {
    /// Actual admitted origin beacon block.
    pub const fn origin_block_root(&self) -> Root {
        self.origin_block_root
    }
    /// State commitment in that original beacon block.
    pub const fn origin_state_root(&self) -> Root {
        self.origin_state_root
    }
    /// Actual complete state root, distinct for a replayed checkpoint when applicable.
    pub const fn state_root(&self) -> Root {
        self.state_root
    }
    /// Actual schema of the complete state.
    pub const fn fork(&self) -> Fork {
        self.fork
    }
    /// Checked consensus preset; resource profiles never change it.
    pub const fn preset(&self) -> Preset {
        self.preset
    }
    /// Actual complete state slot.
    pub const fn slot(&self) -> u64 {
        self.slot
    }
    /// Genesis registry commitment from admitted complete state data.
    pub const fn genesis_validators_root(&self) -> Root {
        self.genesis_validators_root
    }
    /// Actual beacon genesis time in seconds.
    pub const fn genesis_time(&self) -> u64 {
        self.genesis_time
    }
    /// Checked repository/configuration namespace.
    pub const fn identity(&self) -> StateSourceIdentity {
        self.identity
    }
    /// Production-replayed epoch boundary; absence denotes the block's committed post-state.
    pub const fn checkpoint_epoch(&self) -> Option<u64> {
        self.checkpoint_epoch
    }

    pub(crate) fn admitted(
        block_root: Root,
        committed_root: Root,
        state: StateData<'_>,
        identity: StateSourceIdentity,
    ) -> Result<Self, StoreError> {
        let binding = Self::from_data(block_root, committed_root, state, identity, None)?;
        if binding.state_root != committed_root {
            return Err(StateSourceError::BindingMismatch.into())
        }
        Ok(binding)
    }

    pub(crate) fn admitted_header(
        root: Root,
        header: &BeaconBlockHeader,
        fork: Fork,
        preset: Preset,
        genesis: (Root, u64),
        identity: StateSourceIdentity,
    ) -> Result<Self, StoreError> {
        if header.hash_tree_root(preset)? != root {
            return Err(StateSourceError::BindingMismatch.into())
        }
        Ok(Self {
            origin_block_root: root,
            origin_state_root: header.state_root,
            state_root: header.state_root,
            fork,
            preset,
            slot: header.slot,
            genesis_validators_root: genesis.0,
            genesis_time: genesis.1,
            identity,
            checkpoint_epoch: None,
        })
    }

    pub(crate) fn checkpoint(
        origin: &Self,
        epoch: u64,
        state: StateData<'_>,
    ) -> Result<Self, StoreError> {
        if state.genesis_time() != origin.genesis_time ||
            state.genesis_validators_root() != origin.genesis_validators_root ||
            state.preset() != origin.preset ||
            state.slot() < origin.slot
        {
            return Err(StateSourceError::BindingMismatch.into())
        }
        Self::from_data(
            origin.origin_block_root,
            origin.origin_state_root,
            state,
            origin.identity,
            Some(epoch),
        )
    }

    fn from_data(
        origin_block_root: Root,
        origin_state_root: Root,
        state: StateData<'_>,
        identity: StateSourceIdentity,
        checkpoint_epoch: Option<u64>,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            origin_block_root,
            origin_state_root,
            state_root: state.hash_tree_root()?,
            fork: state.fork(),
            preset: state.preset(),
            slot: state.slot(),
            genesis_validators_root: state.genesis_validators_root(),
            genesis_time: state.genesis_time(),
            identity,
            checkpoint_epoch,
        })
    }

    pub(crate) fn check(&self, state: StateData<'_>) -> Result<(), StoreError> {
        if state.hash_tree_root()? != self.state_root ||
            state.fork() != self.fork ||
            state.preset() != self.preset ||
            state.slot() != self.slot ||
            state.genesis_time() != self.genesis_time ||
            state.genesis_validators_root() != self.genesis_validators_root
        {
            return Err(StateSourceError::BindingMismatch.into())
        }
        Ok(())
    }
}

/// Borrowed complete schema tied to an immutable owning guard.
#[derive(Clone, Copy, Debug)]
pub enum StateData<'a> {
    /// Original fork-aware state; earlier schemas are not relabeled on migration.
    Classic(&'a BeaconState),
    /// Dedicated Gloas schema with its privately checked consensus preset.
    Gloas(&'a gloas::BeaconState, Preset),
}
impl StateData<'_> {
    /// Actual complete state root.
    pub fn hash_tree_root(self) -> Result<Root, SszError> {
        match self {
            Self::Classic(s) => s.hash_tree_root(),
            Self::Gloas(s, p) => s.hash_tree_root(p),
        }
    }
    /// Exact schema.
    pub const fn fork(self) -> Fork {
        match self {
            Self::Classic(s) => s.schema_fork(),
            Self::Gloas(..) => Fork::Gloas,
        }
    }
    /// Exact consensus preset.
    pub const fn preset(self) -> Preset {
        match self {
            Self::Classic(s) => s.preset(),
            Self::Gloas(_, p) => p,
        }
    }
    /// Actual slot.
    pub const fn slot(self) -> u64 {
        match self {
            Self::Classic(s) => s.slot(),
            Self::Gloas(s, _) => s.slot,
        }
    }
    /// Actual beacon genesis time.
    pub const fn genesis_time(self) -> u64 {
        match self {
            Self::Classic(s) => s.genesis_time(),
            Self::Gloas(s, _) => s.genesis_time,
        }
    }
    /// Actual genesis registry commitment.
    pub const fn genesis_validators_root(self) -> Root {
        match self {
            Self::Classic(s) => s.genesis_validators_root(),
            Self::Gloas(s, _) => s.genesis_validators_root,
        }
    }
    /// Complete owned schema allocation, excluding allocator and graph bookkeeping.
    pub fn allocation_bytes(self) -> Result<usize, SszError> {
        match self {
            Self::Classic(s) => s.heap_bytes()?.checked_add(std::mem::size_of::<BeaconState>()),
            Self::Gloas(s, _) => {
                s.heap_bytes()?.checked_add(std::mem::size_of::<gloas::BeaconState>())
            }
        }
        .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or(SszError::Overflow)
    }
    /// Exact complete SSZ, without converting Gloas to a copied enum variant.
    pub fn to_ssz(self) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Classic(s) => s.to_ssz(),
            Self::Gloas(s, p) => s.to_ssz(p),
        }
    }
}

/// An immutable complete allocation owned for the entire lease lifetime.
///
/// Implementations must return the same immutable allocation on every call, including after
/// cache eviction or repository closure. The node implementation owns a concrete storage
/// `StateLease`, never a borrowed pointer or a new caller-provided state assertion.
pub trait LeasedState: Debug + Send + Sync {
    /// Borrow complete data while this owning lease remains alive.
    fn state(&self) -> &BeaconState;
}

/// Bounded reconstruction/publication failures; none mean an unknown graph root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum StateSourceError {
    /// Data for an already admitted root could not be obtained.
    #[error("admitted compact state data is unavailable")]
    Unavailable,
    /// The reconstructed data disagree with the graph's exact private binding.
    #[error("compact state does not match the admitted binding")]
    BindingMismatch,
    /// The repository namespace does not match the checked configuration.
    #[error("compact state repository identity mismatch")]
    IdentityMismatch,
    /// The resident/cached schema budget is exhausted.
    #[error("compact state read budget exceeded")]
    ResourceLimit,
    /// Required durable bytes/pins or their publication are corrupt/uncertain.
    #[error("compact state durable publication is corrupt or uncertain")]
    Durability,
}

/// Data-only repository interface. It cannot insert blocks, votes, finality or execution proofs.
/// Original complete signed block retained through the genuine strict import.
#[derive(Clone, Copy, Debug)]
pub enum SignedBlockData<'a> {
    /// Actual complete earlier-schema signed block.
    Classic(&'a ssz::SignedBeaconBlock),
    /// Actual complete Gloas signed block and checked preset.
    Gloas(&'a gloas::SignedBeaconBlock, Preset),
}
impl SignedBlockData<'_> {
    /// Exact canonical original signed bytes; no synthesized outer signature.
    pub fn to_ssz(self) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Classic(b) => b.to_ssz(),
            Self::Gloas(b, p) => b.to_ssz(p),
        }
    }
    /// Actual unsigned beacon root.
    pub fn message_root(self) -> Result<Root, SszError> {
        match self {
            Self::Classic(b) => b.message_root(),
            Self::Gloas(b, p) => b.message.hash_tree_root(p),
        }
    }
}

/// Complete import receipt created only after the private strict transition/Engine/DA path.
#[derive(Clone, Copy, Debug)]
pub struct AdmittedState<'a> {
    pub(crate) binding: &'a StateBinding,
    pub(crate) state: StateData<'a>,
    pub(crate) signed: SignedBlockData<'a>,
    pub(crate) verified_execution_hash: Option<Root>,
    pub(crate) execution_optimistic: bool,
    pub(crate) shared_state: Option<&'a StateRead>,
}
impl<'a> AdmittedState<'a> {
    /// Private graph-derived identity/commitment, never a caller-created claim.
    pub const fn binding(&self) -> &'a StateBinding {
        self.binding
    }
    /// Actual complete admitted post-state borrowed from a live owner.
    pub const fn state(&self) -> StateData<'a> {
        self.state
    }
    /// Actual original complete signed input.
    pub const fn signed_block(&self) -> SignedBlockData<'a> {
        self.signed
    }
    /// Actual strict private graph execution association; Gloas PENDING returns absence.
    pub const fn verified_execution_hash(&self) -> Option<Root> {
        self.verified_execution_hash
    }
    /// Full consensus and availability passed, but an actual Engine response deferred execution.
    /// This private admission marker cannot be manufactured by persistence or callers.
    pub const fn execution_optimistic(&self) -> bool {
        self.execution_optimistic
    }
    /// Share the exact immutable classic post-state already borrowed by this private receipt.
    /// This changes allocation ownership only; storage still validates its complete commitment.
    pub fn shared_classic_state(&self) -> Option<Arc<BeaconState>> {
        self.shared_state.and_then(OwnedStateRead::hot_arc)
    }
}

/// Canonical metadata derived from the private strict candidate graph.
#[derive(Clone, Debug)]
pub struct AdmittedSnapshot {
    pub(crate) snapshot: crate::StoreSnapshot,
    pub(crate) time_ms: u64,
    pub(crate) retained_roots: BTreeSet<Root>,
    pub(crate) verified_envelope_roots: BTreeSet<Root>,
    pub(crate) identity: StateSourceIdentity,
}
impl AdmittedSnapshot {
    /// Actual native owner clock at this exact candidate publication.
    pub const fn time_ms(&self) -> u64 {
        self.time_ms
    }
    /// Complete original block closure in the actual retained strict graph.
    pub const fn retained_roots(&self) -> &BTreeSet<Root> {
        &self.retained_roots
    }
    /// Exact independently verified payload envelopes in that native graph.
    pub const fn verified_envelope_roots(&self) -> &BTreeSet<Root> {
        &self.verified_envelope_roots
    }
    /// Actual graph checkpoints/head; persisted metadata remains a replay hint after restart.
    pub const fn snapshot(&self) -> crate::StoreSnapshot {
        self.snapshot
    }
    /// Exact checked storage/network namespace.
    pub const fn identity(&self) -> StateSourceIdentity {
        self.identity
    }
}

/// Original signed payload envelope admitted by actual Engine and DA validation.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedEnvelope<'a> {
    pub(crate) binding: &'a StateBinding,
    pub(crate) signed: &'a gloas::SignedExecutionPayloadEnvelope,
    pub(crate) verified_execution_hash: Root,
}
impl<'a> VerifiedEnvelope<'a> {
    /// Beacon committed post-state binding, separate from envelope and EL roots.
    pub const fn binding(&self) -> &'a StateBinding {
        self.binding
    }
    /// Original complete signed envelope; never synthesized from the retained message.
    pub const fn signed(&self) -> &'a gloas::SignedExecutionPayloadEnvelope {
        self.signed
    }
    /// Actual Engine-verified execution block hash.
    pub const fn verified_execution_hash(&self) -> Root {
        self.verified_execution_hash
    }
}

/// Durable data provider for graph-created immutable state bindings and sealed import receipts.
/// Every acquisition must recheck durable data/pins; immutable allocation reuse grants no
/// authority.
pub trait CompactStateSource: Debug + Send + Sync {
    /// Full checked namespace identity.
    fn identity(&self) -> StateSourceIdentity;
    /// Explicit graph ownership policy supplied by the node's decoded-state resource profile.
    /// The concrete source must reserve its own idle cache within the same configured budget.
    fn graph_limits(&self) -> CompactGraphLimits {
        CompactGraphLimits::default()
    }
    /// Verify exact admitted receipts and durable owner pins, then own the immutable allocation.
    fn acquire(&self, binding: &StateBinding) -> Result<Arc<dyn LeasedState>, StateSourceError>;
    /// Persist exact signed bytes/state/pins only after actual strict import established admission.
    fn retain_block(&self, _admitted: AdmittedState<'_>) -> Result<(), StateSourceError> {
        Err(StateSourceError::Unavailable)
    }
    /// Publish checked head/checkpoint receipts last, after all required data and pins.
    fn publish_snapshot(&self, _admitted: AdmittedSnapshot) -> Result<(), StateSourceError> {
        Err(StateSourceError::Unavailable)
    }
    /// Conservative durable current/previous recovery floor; it may only delay a graph cut.
    /// This data-only hint cannot establish finality or move the strict graph's checkpoint.
    fn retention_epoch_limit(&self) -> Result<Option<u64>, StateSourceError> {
        Ok(None)
    }
    /// Durably pin and stage the sealed finalized recovery boundary and retained inventories.
    /// Canonical pointer replacement and obsolete-owner release occur only after coordinator FCU.
    fn retain_finalized(&self, _admitted: FinalizedRetention<'_>) -> Result<(), StateSourceError> {
        Err(StateSourceError::Unavailable)
    }
    /// Persist the actual signed envelope admitted by mandatory execution and availability.
    fn retain_envelope(&self, _admitted: VerifiedEnvelope<'_>) -> Result<(), StateSourceError> {
        Err(StateSourceError::Unavailable)
    }
    /// Retain a privately generated checkpoint replay with independent durable pins.
    ///
    /// The receipt binds its exact admitted origin, checked config, target slot and result root.
    /// Implementations without derived-checkpoint persistence must fail closed.
    fn retain_checkpoint(
        &self,
        binding: &StateBinding,
        state: StateData<'_>,
    ) -> Result<(), StateSourceError>;
}

mod sealed {
    pub trait Sealed {}
}
/// Complete state schemas supported by owning guards; implementations are sealed.
pub trait StateSchema: sealed::Sealed + Debug + Send + Sync + 'static {
    #[doc(hidden)]
    fn data(&self, preset: Preset) -> StateData<'_>;
    #[doc(hidden)]
    fn project(state: &BeaconState) -> Option<&Self>;
}
impl sealed::Sealed for BeaconState {}
impl StateSchema for BeaconState {
    fn data(&self, _preset: Preset) -> StateData<'_> {
        StateData::Classic(self)
    }
    fn project(state: &BeaconState) -> Option<&Self> {
        Some(state)
    }
}
impl sealed::Sealed for gloas::BeaconState {}
impl StateSchema for gloas::BeaconState {
    fn data(&self, preset: Preset) -> StateData<'_> {
        StateData::Gloas(self, preset)
    }
    fn project(state: &BeaconState) -> Option<&Self> {
        match state {
            BeaconState::Gloas(state, _) => Some(state),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum ReadOwner<S> {
    Hot(Arc<S>),
    Lease(Arc<dyn LeasedState>),
}
impl<S> Clone for ReadOwner<S> {
    fn clone(&self) -> Self {
        match self {
            Self::Hot(s) => Self::Hot(s.clone()),
            Self::Lease(s) => Self::Lease(s.clone()),
        }
    }
}

/// Owns the exact allocation backing every borrowed consensus read.
///
/// Cloning this guard shares its allocation/lease; it never copies a complete state.
#[derive(Debug)]
pub struct OwnedStateRead<S: StateSchema> {
    owner: ReadOwner<S>,
    preset: Preset,
}
impl<S: StateSchema> Clone for OwnedStateRead<S> {
    fn clone(&self) -> Self {
        Self { owner: self.owner.clone(), preset: self.preset }
    }
}
impl<S: StateSchema> OwnedStateRead<S> {
    pub(crate) const fn hot(state: Arc<S>, preset: Preset) -> Self {
        Self { owner: ReadOwner::Hot(state), preset }
    }
    fn lease(lease: Arc<dyn LeasedState>, binding: &StateBinding) -> Result<Self, StoreError> {
        if lease.state().preset() != binding.preset || lease.state().schema_fork() != binding.fork {
            return Err(StateSourceError::BindingMismatch.into())
        }
        let state = S::project(lease.state()).ok_or(StateSourceError::BindingMismatch)?;
        binding.check(state.data(binding.preset))?;
        Ok(Self { owner: ReadOwner::Lease(lease), preset: binding.preset })
    }
    pub(crate) fn hot_arc(&self) -> Option<Arc<S>> {
        match &self.owner {
            ReadOwner::Hot(s) => Some(s.clone()),
            ReadOwner::Lease(_) => None,
        }
    }
    /// Actual checked schema without detaching its allocation owner.
    pub fn data(&self) -> StateData<'_> {
        self.deref().data(self.preset)
    }
    /// Checked complete state root.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        self.data().hash_tree_root()
    }
    /// Exact owning guard's preset.
    pub const fn preset(&self) -> Preset {
        self.preset
    }
}
impl<S: StateSchema> Deref for OwnedStateRead<S> {
    type Target = S;
    fn deref(&self) -> &S {
        match &self.owner {
            ReadOwner::Hot(state) => state,
            ReadOwner::Lease(lease) => {
                // The sealed projection was checked when constructing this guard. LeasedState
                // requires a stable immutable allocation for its entire owning lifetime.
                S::project(lease.state()).expect("checked immutable lease schema")
            }
        }
    }
}
impl<S: StateSchema> AsRef<S> for OwnedStateRead<S> {
    fn as_ref(&self) -> &S {
        self
    }
}
/// Owned fork-aware classic read.
pub type StateRead = OwnedStateRead<BeaconState>;
/// Owned typed Gloas read, projected from the same retained generic lease without a state copy.
pub type GloasStateRead = OwnedStateRead<gloas::BeaconState>;

/// Private state-table entry; cold membership is independent from cache residency.
/// Cold bindings are separately allocated so hot entries remain small.
#[derive(Debug)]
pub(crate) enum RetainedState<S: StateSchema> {
    Hot(Arc<S>),
    Cold {
        binding: Box<StateBinding>,
        source: Arc<dyn CompactStateSource>,
        validation: Arc<LeaseValidation>,
    },
}
impl<S: StateSchema> Clone for RetainedState<S> {
    fn clone(&self) -> Self {
        match self {
            Self::Hot(s) => Self::Hot(s.clone()),
            Self::Cold { binding, source, validation } => Self::Cold {
                binding: binding.clone(),
                source: source.clone(),
                validation: validation.clone(),
            },
        }
    }
}
impl<S: StateSchema + PartialEq> PartialEq for RetainedState<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Hot(a), Self::Hot(b)) => a == b,
            (Self::Cold { binding: a, .. }, Self::Cold { binding: b, .. }) => a == b,
            _ => false,
        }
    }
}
impl<S: StateSchema> From<Arc<S>> for RetainedState<S> {
    fn from(state: Arc<S>) -> Self {
        Self::Hot(state)
    }
}
impl<S: StateSchema> RetainedState<S> {
    #[cfg(test)]
    pub(crate) const fn hot_mut(&mut self) -> Option<&mut Arc<S>> {
        match self {
            Self::Hot(state) => Some(state),
            Self::Cold { .. } => None,
        }
    }
    pub(crate) const fn hot(&self) -> Option<&Arc<S>> {
        match self {
            Self::Hot(state) => Some(state),
            Self::Cold { .. } => None,
        }
    }
    pub(crate) fn hot_required(&self) -> Result<&Arc<S>, StoreError> {
        self.hot().ok_or_else(|| StateSourceError::Unavailable.into())
    }
    pub(crate) fn acquire(&self, preset: Preset) -> Result<OwnedStateRead<S>, StoreError> {
        match self {
            Self::Hot(state) => Ok(OwnedStateRead::hot(state.clone(), preset)),
            Self::Cold { binding, source, validation } => {
                if source.identity() != binding.identity || preset != binding.preset {
                    return Err(StateSourceError::IdentityMismatch.into())
                }
                validation.acquire(source.acquire(binding)?, binding)
            }
        }
    }
    /// Reuse a cold entry's private commitment only for the exact previously checked read owner.
    /// The read was obtained through acquire(), which rechecks source durability before issuing it.
    /// This weak identity proof cannot bless another allocation or keep decoded data resident.
    pub(crate) fn verified_binding_for(
        &self,
        read: &OwnedStateRead<S>,
    ) -> Result<Option<StateBinding>, StoreError> {
        let Self::Cold { binding, source, validation } = self else { return Ok(None) };
        let ReadOwner::Lease(owner) = &read.owner else { return Ok(None) };
        if source.identity() != binding.identity || read.preset != binding.preset {
            return Err(StateSourceError::IdentityMismatch.into())
        }
        let checked = validation.allocation.lock().map_err(|_| StateSourceError::Durability)?;
        if checked
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|verified| Arc::ptr_eq(&verified, owner))
        {
            return Ok(Some(binding.as_ref().clone()))
        }
        Ok(None)
    }
    pub(crate) fn cold(
        binding: StateBinding,
        source: Arc<dyn CompactStateSource>,
    ) -> Result<Self, StoreError> {
        if source.identity() != binding.identity {
            return Err(StateSourceError::IdentityMismatch.into())
        }
        // Acquiring checks the complete root/schema/network and exact durable publication.
        let validation = Arc::new(LeaseValidation::default());
        validation.acquire::<S>(source.acquire(&binding)?, &binding)?;
        Ok(Self::Cold { binding: Box::new(binding), source, validation })
    }
    /// Cool a read whose binding was just independently checked by the private graph kernel.
    /// Reuse that proof only when the durable source returns the exact same immutable schema
    /// allocation. Equal data copied into another allocation still undergoes full validation.
    pub(crate) fn cold_from_checked_read(
        binding: StateBinding,
        source: Arc<dyn CompactStateSource>,
        read: &OwnedStateRead<S>,
    ) -> Result<Self, StoreError> {
        if source.identity() != binding.identity || read.preset() != binding.preset {
            return Err(StateSourceError::IdentityMismatch.into())
        }
        let owner = source.acquire(&binding)?;
        let projected = S::project(owner.state()).ok_or(StateSourceError::BindingMismatch)?;
        let validation = Arc::new(LeaseValidation::default());
        if std::ptr::eq(projected, read.as_ref()) {
            *validation.allocation.lock().map_err(|_| StateSourceError::Durability)? =
                Some(Arc::downgrade(&owner));
        } else {
            validation.acquire::<S>(owner, &binding)?;
        }
        Ok(Self::Cold { binding: Box::new(binding), source, validation })
    }
    pub(crate) fn slot(&self, preset: Preset) -> u64 {
        match self {
            Self::Hot(state) => state.data(preset).slot(),
            Self::Cold { binding, .. } => binding.slot,
        }
    }
}

/// Owning typed history/Gloas read for shared internal registry queries.
#[derive(Clone, Debug)]
pub(crate) enum GraphStateRead {
    Classic(StateRead),
    Gloas(GloasStateRead),
}
impl GraphStateRead {
    pub(crate) fn data(&self) -> StateData<'_> {
        match self {
            Self::Classic(s) => s.data(),
            Self::Gloas(s) => s.data(),
        }
    }
    pub(crate) fn view(&self) -> crate::fast_confirmation::StateRef<'_> {
        match self {
            Self::Classic(s) => crate::fast_confirmation::StateRef::Classic(s),
            Self::Gloas(s) => crate::fast_confirmation::StateRef::Gloas(s, s.preset()),
        }
    }
    pub(crate) fn slot(&self) -> u64 {
        self.view().slot()
    }
    pub(crate) fn validators(&self) -> &[ssz::phase0::Validator] {
        self.view().validators()
    }
}

pub(crate) mod owned_views;
pub use owned_views::{
    ClassicConfirmationView, GloasConfirmationView, ReadGloasGossipView, ReadGossipView,
    StateAccess,
};

mod cache;
pub(crate) use cache::HotBudget;
pub use cache::{CompactGraphLimits, CompactGraphStats};

pub use crate::{gloas::compact::CompactGloasStore, store::compact::CompactStore};

mod retention;
pub use retention::{
    FinalizedRetention, FinalizedRetentionPolicy, PrunedGraph, RetainedExecutionAssociation,
    UnresolvedExecutionAssociation,
};

/// One weak proof per retained entry, so validation reuse neither owns decoded memory nor grows.
#[derive(Debug, Default)]
pub(crate) struct LeaseValidation {
    allocation: Mutex<Option<Weak<dyn LeasedState>>>,
}
impl LeaseValidation {
    fn acquire<S: StateSchema>(
        &self,
        lease: Arc<dyn LeasedState>,
        binding: &StateBinding,
    ) -> Result<OwnedStateRead<S>, StoreError> {
        let mut allocation = self.allocation.lock().map_err(|_| StateSourceError::Durability)?;
        if allocation
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|verified| Arc::ptr_eq(&verified, &lease))
        {
            // LeasedState requires one stable immutable allocation for this exact owner lifetime.
            // The source already rechecked durable receipt/pins before supplying this owner.
            return Ok(OwnedStateRead { owner: ReadOwner::Lease(lease), preset: binding.preset })
        }
        let state = OwnedStateRead::lease(lease.clone(), binding)?;
        *allocation = Some(Arc::downgrade(&lease));
        Ok(state)
    }
}

#[cfg(test)]
mod lease_validation_tests {
    use super::*;
    use reth_embedded_beacon_state_transition::ssz::phase0;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountedLease {
        state: BeaconState,
        reads: Arc<AtomicUsize>,
    }
    impl LeasedState for CountedLease {
        fn state(&self) -> &BeaconState {
            self.reads.fetch_add(1, Ordering::SeqCst);
            &self.state
        }
    }
    #[test]
    fn weak_validation_reuses_only_the_same_live_immutable_owner_and_checks_replacement() {
        // Exact official genesis bytes exercise immutable-data validation, not consensus admission.
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let state = BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        );
        let binding = StateBinding::admitted(
            [7; 32],
            state.hash_tree_root().unwrap(),
            StateData::Classic(&state),
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] },
        )
        .unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(CountedLease { state: state.clone(), reads: reads.clone() })
            as Arc<dyn LeasedState>;
        let validation = LeaseValidation::default();
        let first = validation.acquire::<BeaconState>(owner.clone(), &binding).unwrap();
        assert!(reads.load(Ordering::SeqCst) > 0);
        reads.store(0, Ordering::SeqCst);
        let second = validation.acquire::<BeaconState>(owner.clone(), &binding).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&owner), 3);
        drop(first);
        drop(second);
        drop(owner);
        let replaced = Arc::new(CountedLease { state: state.clone(), reads: reads.clone() })
            as Arc<dyn LeasedState>;
        validation.acquire::<BeaconState>(replaced, &binding).unwrap();
        assert!(reads.load(Ordering::SeqCst) > 0);
        let BeaconState::Phase0(mut corrupt, preset) = state else { unreachable!() };
        corrupt.slot += 1;
        let changed = Arc::new(CountedLease { state: BeaconState::Phase0(corrupt, preset), reads })
            as Arc<dyn LeasedState>;
        assert!(matches!(
            validation.acquire::<BeaconState>(changed, &binding),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
    }

    #[test]
    fn admitted_binding_rejects_a_state_changed_after_block_commitment() {
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let original = BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        );
        let committed_root = original.hash_tree_root().unwrap();
        let identity =
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] };
        let binding = StateBinding::admitted(
            [7; 32],
            committed_root,
            StateData::Classic(&original),
            identity,
        )
        .unwrap();
        let mut changed = original.clone();
        let BeaconState::Phase0(state, _) = &mut changed else { unreachable!() };
        state.slot += 1;

        assert!(matches!(
            StateBinding::admitted([7; 32], committed_root, StateData::Classic(&changed), identity),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
        binding.check(StateData::Classic(&original)).unwrap();
        assert_eq!(binding.state_root(), committed_root);
        assert_eq!(binding.origin_state_root(), committed_root);
        assert_eq!(binding.identity(), identity);
    }
    #[derive(Debug)]
    struct CheckedSource {
        owner: Arc<dyn LeasedState>,
        identity: Mutex<StateSourceIdentity>,
        retained: AtomicBool,
        acquisitions: AtomicUsize,
    }
    impl CompactStateSource for CheckedSource {
        fn identity(&self) -> StateSourceIdentity {
            *self.identity.lock().unwrap()
        }
        fn acquire(
            &self,
            binding: &StateBinding,
        ) -> Result<Arc<dyn LeasedState>, StateSourceError> {
            self.acquisitions.fetch_add(1, Ordering::SeqCst);
            if !self.retained.load(Ordering::SeqCst) {
                return Err(StateSourceError::Unavailable)
            }
            if binding.identity() != self.identity() {
                return Err(StateSourceError::IdentityMismatch)
            }
            Ok(self.owner.clone())
        }
        fn retain_checkpoint(
            &self,
            _: &StateBinding,
            _: StateData<'_>,
        ) -> Result<(), StateSourceError> {
            Err(StateSourceError::Unavailable)
        }
    }

    #[test]
    fn cold_binding_reuses_only_checked_owner_and_rechecks_source_identity_and_pins() {
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let state = BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        );
        let identity =
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] };
        let binding = StateBinding::admitted(
            [7; 32],
            state.hash_tree_root().unwrap(),
            StateData::Classic(&state),
            identity,
        )
        .unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(CheckedSource {
            owner: Arc::new(CountedLease { state: state.clone(), reads: reads.clone() }),
            identity: Mutex::new(identity),
            retained: AtomicBool::new(true),
            acquisitions: AtomicUsize::new(0),
        });
        let entry = RetainedState::<BeaconState>::cold(binding.clone(), source.clone()).unwrap();
        reads.store(0, Ordering::SeqCst);
        let read = entry.acquire(Preset::Minimal).unwrap();
        assert_eq!(source.acquisitions.load(Ordering::SeqCst), 2);
        assert_eq!(entry.verified_binding_for(&read).unwrap(), Some(binding.clone()));
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        let foreign = OwnedStateRead::<BeaconState>::lease(
            Arc::new(CountedLease { state: state.clone(), reads: reads.clone() }),
            &binding,
        )
        .unwrap();
        assert!(entry.verified_binding_for(&foreign).unwrap().is_none());
        let hot = OwnedStateRead::hot(Arc::new(state), Preset::Minimal);
        assert!(entry.verified_binding_for(&hot).unwrap().is_none());
        let wrong_preset =
            OwnedStateRead::<BeaconState> { owner: read.owner.clone(), preset: Preset::Mainnet };
        assert!(matches!(
            entry.verified_binding_for(&wrong_preset),
            Err(StoreError::CompactState(StateSourceError::IdentityMismatch))
        ));
        source.identity.lock().unwrap().consensus_config_digest[0] ^= 1;
        assert!(matches!(
            entry.verified_binding_for(&read),
            Err(StoreError::CompactState(StateSourceError::IdentityMismatch))
        ));
        *source.identity.lock().unwrap() = identity;
        source.retained.store(false, Ordering::SeqCst);
        assert!(matches!(
            entry.acquire(Preset::Minimal),
            Err(StoreError::CompactState(StateSourceError::Unavailable))
        ));
        assert_eq!(source.acquisitions.load(Ordering::SeqCst), 3);
    }

    #[derive(Debug)]
    struct SharedLease {
        state: Arc<BeaconState>,
        reads: Arc<AtomicUsize>,
    }
    impl LeasedState for SharedLease {
        fn state(&self) -> &BeaconState {
            self.reads.fetch_add(1, Ordering::SeqCst);
            &self.state
        }
    }

    #[test]
    fn cooling_reuses_only_the_exact_privately_checked_schema_owner() {
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let state = Arc::new(BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        ));
        let identity =
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] };
        let binding = StateBinding::admitted(
            [7; 32],
            state.hash_tree_root().unwrap(),
            StateData::Classic(&state),
            identity,
        )
        .unwrap();
        let read = StateRead::hot(state.clone(), Preset::Minimal);
        let reads = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(CheckedSource {
            owner: Arc::new(SharedLease { state: state.clone(), reads: reads.clone() }),
            identity: Mutex::new(identity),
            retained: AtomicBool::new(true),
            acquisitions: AtomicUsize::new(0),
        });
        let cold =
            RetainedState::cold_from_checked_read(binding.clone(), source.clone(), &read).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        reads.store(0, Ordering::SeqCst);
        let acquired = cold.acquire(Preset::Minimal).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(std::ptr::eq(acquired.as_ref(), state.as_ref()));
        source.retained.store(false, Ordering::SeqCst);
        assert!(matches!(
            cold.acquire(Preset::Minimal),
            Err(StoreError::CompactState(StateSourceError::Unavailable))
        ));

        let mut changed = state.as_ref().clone();
        let BeaconState::Phase0(value, _) = &mut changed else { unreachable!() };
        value.slot += 1;
        let changed_source = Arc::new(CheckedSource {
            owner: Arc::new(SharedLease { state: Arc::new(changed), reads }),
            identity: Mutex::new(identity),
            retained: AtomicBool::new(true),
            acquisitions: AtomicUsize::new(0),
        });
        assert!(matches!(
            RetainedState::cold_from_checked_read(binding, changed_source, &read),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
    }
}
