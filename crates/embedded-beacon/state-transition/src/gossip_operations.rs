//! Immutable operation gossip validation with mandatory signatures and bounded duplicate caches.
//!
//! Callers authenticate the retained head state and network parameters. Successful propagation
//! checks never mutate consensus state or establish block, execution or availability authority.

use crate::{
    fork_processing::TransitionParameters,
    phase0_processing::ProcessingError,
    ssz::{
        capella::SignedBLSToExecutionChange,
        gloas,
        phase0::{AttestationData, ForkInfo, ProposerSlashing, SignedVoluntaryExit, Validator},
        BeaconState, Preset,
    },
};
use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, fast_aggregate_verify, verify_signature,
};
use reth_embedded_beacon_primitives::{sha256, Fork, Root};
use std::{collections::BTreeSet, fmt};
use thiserror::Error;

/// Exact network constants used by operation propagation; fields cannot be independently changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationParameters {
    preset: Preset,
    slots_per_epoch: u64,
    slot_duration_ms: u64,
    shard_committee_period: u64,
    versions: [[u8; 4]; 8],
    capella_epoch: u64,
}

impl OperationParameters {
    /// Capture already coherent trusted transition configuration without copying state data.
    pub fn from_transition_parameters(
        parameters: &TransitionParameters,
    ) -> Result<Self, OperationGossipError> {
        parameters.validate()?;
        let phase0 = parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0;
        let slot_duration_ms = parameters
            .gloas
            .fulu
            .electra
            .deneb
            .capella
            .bellatrix
            .seconds_per_slot
            .checked_mul(1000)
            .ok_or(ProcessingError::Arithmetic("operation gossip slot duration"))?;
        if slot_duration_ms == 0 {
            return Err(ProcessingError::InvalidParameters(
                "operation gossip slot duration is zero",
            )
            .into());
        }
        let mut versions = [[0; 4]; 8];
        for (version, activation) in versions.iter_mut().zip(&parameters.schedule) {
            *version = activation.version;
        }
        Ok(Self {
            preset: parameters.preset(),
            slots_per_epoch: parameters.slots_per_epoch(),
            slot_duration_ms,
            shard_committee_period: phase0.shard_committee_period,
            versions,
            capella_epoch: parameters.schedule[3].epoch,
        })
    }
}

/// Propagation outcome; configuration and resource failures are separate from invalid messages.
#[derive(Debug, Error)]
pub enum OperationGossipError {
    /// An already observed, premature or redundant message may be queued or ignored.
    #[error("ignore operation gossip: {0}")]
    Ignore(&'static str),
    /// The message violates a consensus propagation rule.
    #[error("reject operation gossip: {0}")]
    Reject(&'static str),
    /// Checked network or retained-state context is unavailable or inconsistent.
    #[error(transparent)]
    Context(#[from] ProcessingError),
    /// A bounded cache cannot retain another checked message.
    #[error("operation gossip duplicate cache capacity exhausted")]
    Capacity,
}

/// Duplicate observations only; this cache cannot grant consensus validation authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeenOperations {
    voluntary_exits: BTreeSet<u64>,
    proposer_slashings: BTreeSet<u64>,
    attester_slashings: BTreeSet<u64>,
    credential_changes: BTreeSet<u64>,
    maximum_entries: usize,
    network_binding: Option<(Root, OperationParameters)>,
}

impl SeenOperations {
    /// Bound total retained validator identities across every operation category.
    pub fn new(maximum_entries: usize) -> Result<Self, OperationGossipError> {
        if maximum_entries == 0 || maximum_entries > 1_048_576 {
            return Err(ProcessingError::InvalidParameters(
                "operation gossip cache budget is out of range",
            )
            .into());
        }
        Ok(Self {
            voluntary_exits: BTreeSet::new(),
            proposer_slashings: BTreeSet::new(),
            attester_slashings: BTreeSet::new(),
            credential_changes: BTreeSet::new(),
            maximum_entries,
            network_binding: None,
        })
    }
    /// Exact identity count; allocator metadata is not included.
    pub fn entries(&self) -> usize {
        self.voluntary_exits.len() +
            self.proposer_slashings.len() +
            self.attester_slashings.len() +
            self.credential_changes.len()
    }
    fn check_binding(
        &self,
        state: OperationState<'_>,
        parameters: &OperationParameters,
    ) -> Result<(), OperationGossipError> {
        checked_fields(state, parameters)?;
        if self.network_binding.as_ref().is_some_and(|(root, bound)| {
            *root != state.genesis_validators_root() || bound != parameters
        }) {
            return Err(ProcessingError::InvalidState(
                "operation gossip duplicate cache network binding differs",
            )
            .into());
        }
        Ok(())
    }
    fn bind_success(&mut self, state: OperationState<'_>, parameters: &OperationParameters) {
        if self.network_binding.is_none() {
            self.network_binding = Some((state.genesis_validators_root(), parameters.clone()));
        }
    }
    fn admit(&self, additional: usize) -> Result<(), OperationGossipError> {
        if additional > self.maximum_entries.saturating_sub(self.entries()) {
            return Err(OperationGossipError::Capacity);
        }
        Ok(())
    }
}

/// Borrow complete indexed-attestation fields without shortening its wire container.
#[derive(Clone, Copy)]
pub struct IndexedAttestationRef<'a> {
    /// Canonical indices from the exact decoded fork-specific container.
    pub indices: &'a [u64],
    /// Full signed attestation data.
    pub data: &'a AttestationData,
    /// The actual aggregate signature.
    pub signature: &'a [u8; 96],
}

impl fmt::Debug for IndexedAttestationRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedAttestationRef")
            .field("participants", &self.indices.len())
            .field("slot", &self.data.slot)
            .field("target_epoch", &self.data.target.epoch)
            .finish_non_exhaustive()
    }
}
/// Borrow only the actual immutable registry/domain fields from a complete retained head state.
/// This view cannot extract a shared allocation or alter the underlying fork-specific state.
#[derive(Clone, Copy)]
pub struct OperationState<'a> {
    validators: &'a [Validator],
    preset: Preset,
    schema: Fork,
    slot: u64,
    genesis_time: u64,
    genesis_validators_root: Root,
    fork: &'a ForkInfo,
}

impl fmt::Debug for OperationState<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationState")
            .field("fork", &self.schema)
            .field("preset", &self.preset)
            .field("slot", &self.slot)
            .field("validators", &self.validators.len())
            .finish_non_exhaustive()
    }
}
impl<'a> OperationState<'a> {
    /// Borrow a complete classic or Gloas state without cloning any registry or cache.
    pub fn from_state(state: &'a BeaconState) -> Self {
        macro_rules! view {
            ($value:ident, $preset:ident, $schema:expr) => {
                Self {
                    validators: &$value.validators.values,
                    preset: *$preset,
                    schema: $schema,
                    slot: $value.slot,
                    genesis_time: $value.genesis_time,
                    genesis_validators_root: $value.genesis_validators_root,
                    fork: &$value.fork,
                }
            };
        }
        match state {
            BeaconState::Phase0(value, preset) => view!(value, preset, Fork::Phase0),
            BeaconState::Altair(value, preset) => view!(value, preset, Fork::Altair),
            BeaconState::Bellatrix(value, preset) => view!(value, preset, Fork::Bellatrix),
            BeaconState::Capella(value, preset) => view!(value, preset, Fork::Capella),
            BeaconState::Deneb(value, preset) => view!(value, preset, Fork::Deneb),
            BeaconState::Electra(value, preset) => view!(value, preset, Fork::Electra),
            BeaconState::Fulu(value, preset) => view!(value, preset, Fork::Fulu),
            BeaconState::Gloas(value, preset) => view!(value, preset, Fork::Gloas),
        }
    }
    /// Borrow a Gloas graph's exact retained state without constructing a replacement wrapper.
    pub fn from_gloas(state: &'a gloas::BeaconState, preset: Preset) -> Self {
        Self {
            validators: &state.validators.values,
            preset,
            schema: Fork::Gloas,
            slot: state.slot,
            genesis_time: state.genesis_time,
            genesis_validators_root: state.genesis_validators_root,
            fork: &state.fork,
        }
    }
    const fn schema_fork(self) -> Fork {
        self.schema
    }
    const fn slot(self) -> u64 {
        self.slot
    }
    const fn genesis_time(self) -> u64 {
        self.genesis_time
    }
    const fn genesis_validators_root(self) -> Root {
        self.genesis_validators_root
    }
    const fn fork_info(self) -> &'a ForkInfo {
        self.fork
    }
}

fn checked_fields<'a>(
    state: OperationState<'a>,
    parameters: &OperationParameters,
) -> Result<OperationState<'a>, OperationGossipError> {
    let ordinal = match state.schema_fork() {
        Fork::Phase0 => 0,
        Fork::Altair => 1,
        Fork::Bellatrix => 2,
        Fork::Capella => 3,
        Fork::Deneb => 4,
        Fork::Electra => 5,
        Fork::Fulu => 6,
        Fork::Gloas => 7,
    };
    if state.preset != parameters.preset ||
        state.fork_info().current_version != parameters.versions[ordinal]
    {
        return Err(ProcessingError::InvalidState(
            "operation gossip state preset or fork version differs from network",
        )
        .into());
    }
    Ok(state)
}

fn validator<'a>(
    view: &OperationState<'a>,
    index: u64,
    reason: &'static str,
) -> Result<&'a Validator, OperationGossipError> {
    usize::try_from(index)
        .ok()
        .and_then(|index| view.validators.get(index))
        .ok_or(OperationGossipError::Reject(reason))
}

fn domain(state: OperationState<'_>, domain_type: [u8; 4], epoch: u64) -> Root {
    let info = state.fork_info();
    compute_domain(
        domain_type,
        if epoch < info.epoch { info.previous_version } else { info.current_version },
        state.genesis_validators_root(),
    )
}

fn future_epoch(
    state: OperationState<'_>,
    parameters: &OperationParameters,
    epoch: u64,
    current_time_ms: u64,
) -> bool {
    // Wider arithmetic preserves mathematical epoch comparisons, including disabled fork epochs.
    let start = u128::from(state.genesis_time()) * 1000 +
        u128::from(epoch) *
            u128::from(parameters.slots_per_epoch) *
            u128::from(parameters.slot_duration_ms);
    u128::from(current_time_ms) + 500 < start
}

const fn slashable(value: &Validator, epoch: u64) -> bool {
    !value.slashed && value.activation_epoch <= epoch && epoch < value.withdrawable_epoch
}

/// Validate the exact exit gossip rules, including clock tolerance and the fixed late-fork domain.
/// The retained state, balances and pending withdrawal queues remain unchanged.
pub fn validate_voluntary_exit(
    seen: &mut SeenOperations,
    state: OperationState<'_>,
    signed: &SignedVoluntaryExit,
    current_time_ms: u64,
    parameters: &OperationParameters,
) -> Result<(), OperationGossipError> {
    seen.check_binding(state, parameters)?;
    let exit = &signed.message;
    if seen.voluntary_exits.contains(&exit.validator_index) {
        return Err(OperationGossipError::Ignore("already seen voluntary exit for this validator"));
    }
    if future_epoch(state, parameters, exit.epoch, current_time_ms) {
        return Err(OperationGossipError::Ignore("voluntary exit epoch is in the future"));
    }
    let view = checked_fields(state, parameters)?;
    let value = validator(&view, exit.validator_index, "validator index out of range")?;
    let current_epoch = state.slot() / parameters.slots_per_epoch;
    if value.exit_epoch != u64::MAX {
        return Err(OperationGossipError::Ignore("validator has already initiated exit"));
    }
    if !(value.activation_epoch <= current_epoch && current_epoch < value.exit_epoch) {
        return Err(OperationGossipError::Reject("validator is not active"));
    }
    if u128::from(current_epoch) <
        u128::from(value.activation_epoch) + u128::from(parameters.shard_committee_period)
    {
        return Err(OperationGossipError::Reject("validator has not been active long enough"));
    }
    let signing_domain = if state.schema_fork() >= Fork::Deneb {
        compute_domain([4, 0, 0, 0], parameters.versions[3], state.genesis_validators_root())
    } else {
        domain(state, [4, 0, 0, 0], exit.epoch)
    };
    let root = compute_signing_root(
        exit.hash_tree_root(parameters.preset).map_err(ProcessingError::from)?,
        signing_domain,
    );
    verify_signature(&value.pubkey, &root, &signed.signature)
        .map_err(|_| OperationGossipError::Reject("invalid voluntary exit signature"))?;
    seen.admit(1)?;
    seen.bind_success(state, parameters);
    seen.voluntary_exits.insert(exit.validator_index);
    Ok(())
}

/// Check matching conflicting headers and both real proposer signatures without applying penalties.
pub fn validate_proposer_slashing(
    seen: &mut SeenOperations,
    state: OperationState<'_>,
    operation: &ProposerSlashing,
    parameters: &OperationParameters,
) -> Result<(), OperationGossipError> {
    seen.check_binding(state, parameters)?;
    let first = operation.signed_header_1.message;
    let second = operation.signed_header_2.message;
    if seen.proposer_slashings.contains(&first.proposer_index) {
        return Err(OperationGossipError::Ignore(
            "already seen proposer slashing for this proposer",
        ));
    }
    if first.slot != second.slot {
        return Err(OperationGossipError::Reject("header slots do not match"));
    }
    if first.proposer_index != second.proposer_index {
        return Err(OperationGossipError::Reject("header proposer indices do not match"));
    }
    if first == second {
        return Err(OperationGossipError::Reject("headers are not different"));
    }
    let view = checked_fields(state, parameters)?;
    let value = validator(&view, first.proposer_index, "proposer index out of range")?;
    if !slashable(value, state.slot() / parameters.slots_per_epoch) {
        return Err(OperationGossipError::Reject("proposer is not slashable"));
    }
    for header in [&operation.signed_header_1, &operation.signed_header_2] {
        let root = compute_signing_root(
            header.message.hash_tree_root(),
            domain(state, [0; 4], header.message.slot / parameters.slots_per_epoch),
        );
        verify_signature(&value.pubkey, &root, &header.signature)
            .map_err(|_| OperationGossipError::Reject("invalid proposer slashing signature"))?;
    }
    seen.admit(1)?;
    seen.bind_success(state, parameters);
    seen.proposer_slashings.insert(first.proposer_index);
    Ok(())
}

fn maximum_attesters(state: OperationState<'_>, parameters: &OperationParameters) -> usize {
    if state.schema_fork() >= Fork::Electra {
        match parameters.preset {
            Preset::Mainnet => 131_072,
            Preset::Minimal => 8_192,
        }
    } else {
        2048
    }
}
fn indexed(
    view: &OperationState<'_>,
    state: OperationState<'_>,
    attestation: IndexedAttestationRef<'_>,
    parameters: &OperationParameters,
    invalid_index: &'static str,
    invalid_attestation: &'static str,
) -> Result<(), OperationGossipError> {
    for &index in attestation.indices {
        validator(view, index, invalid_index)?;
    }
    let maximum = maximum_attesters(state, parameters);
    if attestation.indices.is_empty() ||
        attestation.indices.len() > maximum ||
        attestation.indices.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(OperationGossipError::Reject(invalid_attestation));
    }
    let keys = attestation
        .indices
        .iter()
        .map(|&index| validator(view, index, invalid_index).map(|value| value.pubkey))
        .collect::<Result<Vec<_>, _>>()?;
    let root = compute_signing_root(
        attestation.data.hash_tree_root(parameters.preset).map_err(ProcessingError::from)?,
        domain(state, [1, 0, 0, 0], attestation.data.target.epoch),
    );
    fast_aggregate_verify(&keys, &root, attestation.signature)
        .map_err(|_| OperationGossipError::Reject(invalid_attestation))?;
    Ok(())
}

/// Validate both indexed signatures and the slashable intersection, then atomically cache all
/// indices. Gloas's complete progressive wire indices are borrowed directly rather than shortened.
pub fn validate_attester_slashing(
    seen: &mut SeenOperations,
    state: OperationState<'_>,
    first: IndexedAttestationRef<'_>,
    second: IndexedAttestationRef<'_>,
    parameters: &OperationParameters,
) -> Result<(), OperationGossipError> {
    seen.check_binding(state, parameters)?;
    let maximum = maximum_attesters(state, parameters);
    if first.indices.len() > maximum {
        return Err(OperationGossipError::Reject("invalid indexed attestation 1"));
    }
    if second.indices.len() > maximum {
        return Err(OperationGossipError::Reject("invalid indexed attestation 2"));
    }
    let first_indices = first.indices.iter().copied().collect::<BTreeSet<_>>();
    let second_indices = second.indices.iter().copied().collect::<BTreeSet<_>>();
    let intersection = first_indices.intersection(&second_indices).copied().collect::<Vec<_>>();
    let additional =
        intersection.iter().filter(|index| !seen.attester_slashings.contains(index)).count();
    if additional == 0 {
        return Err(OperationGossipError::Ignore("all attester slashing indices already seen"));
    }
    if !((first.data != second.data && first.data.target.epoch == second.data.target.epoch) ||
        (first.data.source.epoch < second.data.source.epoch &&
            second.data.target.epoch < first.data.target.epoch))
    {
        return Err(OperationGossipError::Reject("attestation data is not slashable"));
    }
    let view = checked_fields(state, parameters)?;
    indexed(
        &view,
        state,
        first,
        parameters,
        "validator index out of range in indexed attestation 1",
        "invalid indexed attestation 1",
    )?;
    indexed(
        &view,
        state,
        second,
        parameters,
        "validator index out of range in indexed attestation 2",
        "invalid indexed attestation 2",
    )?;
    if !intersection.iter().any(|&index| {
        validator(&view, index, "validator index out of range")
            .is_ok_and(|value| slashable(value, state.slot() / parameters.slots_per_epoch))
    }) {
        return Err(OperationGossipError::Reject("no slashable validators in intersection"));
    }
    seen.admit(additional)?;
    seen.bind_success(state, parameters);
    seen.attester_slashings.extend(intersection);
    Ok(())
}

/// Require matching BLS credentials and their actual withdrawal-key signature before caching.
pub fn validate_bls_to_execution_change(
    seen: &mut SeenOperations,
    state: OperationState<'_>,
    signed: &SignedBLSToExecutionChange,
    current_time_ms: u64,
    parameters: &OperationParameters,
) -> Result<(), OperationGossipError> {
    seen.check_binding(state, parameters)?;
    let change = &signed.message;
    if seen.credential_changes.contains(&change.validator_index) {
        return Err(OperationGossipError::Ignore(
            "already seen BLS to execution change for this validator",
        ));
    }
    if future_epoch(state, parameters, parameters.capella_epoch, current_time_ms) {
        return Err(OperationGossipError::Ignore("current epoch is pre-capella"));
    }
    let view = checked_fields(state, parameters)?;
    let value = validator(&view, change.validator_index, "validator index out of range")?;
    if value.withdrawal_credentials[0] != 0 {
        return Err(OperationGossipError::Reject(
            "validator does not have BLS withdrawal credentials",
        ));
    }
    if value.withdrawal_credentials[1..] != sha256(&change.from_bls_pubkey)[1..] {
        return Err(OperationGossipError::Reject(
            "pubkey does not match validator withdrawal credentials",
        ));
    }
    let root = compute_signing_root(
        change.hash_tree_root(parameters.preset).map_err(ProcessingError::from)?,
        compute_domain([10, 0, 0, 0], parameters.versions[0], state.genesis_validators_root()),
    );
    verify_signature(&change.from_bls_pubkey, &root, &signed.signature)
        .map_err(|_| OperationGossipError::Reject("invalid BLS to execution change signature"))?;
    seen.admit(1)?;
    seen.bind_success(state, parameters);
    seen.credential_changes.insert(change.validator_index);
    Ok(())
}

/// Explicit declared timing context for fixture propagation; it confers no runtime authority.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    use super::*;

    /// Separately typed fixture clock observations with no extraction into runtime parameters.
    #[derive(Debug)]
    pub struct SpecOperationParameters {
        inner: OperationParameters,
    }
    impl SpecOperationParameters {
        /// Bind actual state/domain parameters and the explicitly declared gossip activation.
        pub const fn new(mut parameters: OperationParameters, capella_gossip_epoch: u64) -> Self {
            parameters.capella_epoch = capella_gossip_epoch;
            Self { inner: parameters }
        }
        /// Apply identical mandatory-signature exit validation in the isolated fixture context.
        pub fn voluntary_exit(
            &self,
            seen: &mut SeenOperations,
            state: OperationState<'_>,
            signed: &SignedVoluntaryExit,
            time: u64,
        ) -> Result<(), OperationGossipError> {
            validate_voluntary_exit(seen, state, signed, time, &self.inner)
        }
        /// Apply identical mandatory-signature proposer validation in the fixture context.
        pub fn proposer_slashing(
            &self,
            seen: &mut SeenOperations,
            state: OperationState<'_>,
            operation: &ProposerSlashing,
        ) -> Result<(), OperationGossipError> {
            validate_proposer_slashing(seen, state, operation, &self.inner)
        }
        /// Apply identical indexed-signature validation in the fixture context.
        pub fn attester_slashing(
            &self,
            seen: &mut SeenOperations,
            state: OperationState<'_>,
            first: IndexedAttestationRef<'_>,
            second: IndexedAttestationRef<'_>,
        ) -> Result<(), OperationGossipError> {
            validate_attester_slashing(seen, state, first, second, &self.inner)
        }
        /// Apply mandatory credential signatures with only the declared fixture activation clock.
        pub fn credential_change(
            &self,
            seen: &mut SeenOperations,
            state: OperationState<'_>,
            signed: &SignedBLSToExecutionChange,
            time: u64,
        ) -> Result<(), OperationGossipError> {
            validate_bls_to_execution_change(seen, state, signed, time, &self.inner)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::phase0::{SignedBeaconBlockHeader, VoluntaryExit};
    use blst::min_pk::{AggregateSignature, SecretKey};
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint};

    #[cfg(feature = "spec-tests")]
    use crate::ssz::capella::BLSToExecutionChange;

    fn context() -> (BeaconState, Vec<SecretKey>, OperationParameters) {
        let (mut value, keys) = crate::bellatrix_processing::tests::state_and_keys();
        value.slot = 64 * 8;
        let state = BeaconState::Bellatrix(value, Preset::Minimal);
        let parameters = OperationParameters::from_transition_parameters(
            &TransitionParameters::for_preset(Preset::Minimal),
        )
        .unwrap();
        (state, keys, parameters)
    }
    fn exit(state: &BeaconState, key: &SecretKey) -> SignedVoluntaryExit {
        let message = VoluntaryExit { epoch: 64, validator_index: 0 };
        let root = compute_signing_root(
            message.hash_tree_root(Preset::Minimal).unwrap(),
            domain(OperationState::from_state(state), [4, 0, 0, 0], 64),
        );
        SignedVoluntaryExit {
            message,
            signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    }
    fn aggregate(state: &BeaconState, data: &AttestationData, keys: &[SecretKey]) -> [u8; 96] {
        let root = compute_signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            domain(OperationState::from_state(state), [1, 0, 0, 0], data.target.epoch),
        );
        let signatures =
            keys.iter().map(|key| key.sign(&root, ETHEREUM_BLS_DST, &[])).collect::<Vec<_>>();
        AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
            .unwrap()
            .to_signature()
            .to_bytes()
    }

    #[test]
    fn real_exit_clock_tolerance_cache_and_retained_state_are_exact() {
        let (state, keys, parameters) = context();
        let before = state.clone();
        let signed = exit(&state, &keys[0]);
        let boundary = 64 * parameters.slots_per_epoch * parameters.slot_duration_ms;
        let mut seen = SeenOperations::new(8).unwrap();
        let empty = seen.clone();
        assert!(matches!(
            validate_voluntary_exit(
                &mut seen,
                OperationState::from_state(&state),
                &signed,
                boundary - 501,
                &parameters
            ),
            Err(OperationGossipError::Ignore("voluntary exit epoch is in the future"))
        ));
        assert_eq!(seen, empty);
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert!(matches!(
            validate_voluntary_exit(
                &mut seen,
                OperationState::from_state(&state),
                &invalid,
                boundary - 500,
                &parameters
            ),
            Err(OperationGossipError::Reject("invalid voluntary exit signature"))
        ));
        assert_eq!(seen, empty);
        validate_voluntary_exit(
            &mut seen,
            OperationState::from_state(&state),
            &signed,
            boundary - 500,
            &parameters,
        )
        .unwrap();
        assert_eq!(seen.entries(), 1);
        assert!(matches!(
            validate_voluntary_exit(
                &mut seen,
                OperationState::from_state(&state),
                &signed,
                boundary,
                &parameters
            ),
            Err(OperationGossipError::Ignore("already seen voluntary exit for this validator"))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn proposer_requires_both_real_signatures_before_mutating_seen() {
        let (state, keys, parameters) = context();
        let header = BeaconBlockHeader {
            slot: 512,
            proposer_index: 0,
            parent_root: [2; 32],
            state_root: [3; 32],
            body_root: [4; 32],
        };
        let mut second = header;
        second.body_root = [5; 32];
        let sign = |message: BeaconBlockHeader| {
            let root = compute_signing_root(
                message.hash_tree_root(),
                domain(OperationState::from_state(&state), [0; 4], 64),
            );
            SignedBeaconBlockHeader {
                message,
                signature: keys[0].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
            }
        };
        let mut operation =
            ProposerSlashing { signed_header_1: sign(header), signed_header_2: sign(second) };
        let valid = operation.clone();
        operation.signed_header_2.signature = [0; 96];
        let mut seen = SeenOperations::new(8).unwrap();
        let empty = seen.clone();
        assert!(matches!(
            validate_proposer_slashing(
                &mut seen,
                OperationState::from_state(&state),
                &operation,
                &parameters
            ),
            Err(OperationGossipError::Reject("invalid proposer slashing signature"))
        ));
        assert_eq!(seen, empty);
        validate_proposer_slashing(
            &mut seen,
            OperationState::from_state(&state),
            &valid,
            &parameters,
        )
        .unwrap();
        assert_eq!(seen.entries(), 1);
        assert!(matches!(
            validate_proposer_slashing(
                &mut seen,
                OperationState::from_state(&state),
                &valid,
                &parameters
            ),
            Err(OperationGossipError::Ignore("already seen proposer slashing for this proposer"))
        ));
    }

    #[test]
    fn indexed_real_signatures_ordering_and_multi_index_capacity_are_atomic() {
        let (state, keys, parameters) = context();
        let first_data = AttestationData {
            slot: 512,
            index: 0,
            beacon_block_root: [2; 32],
            source: Checkpoint { epoch: 63, root: [3; 32] },
            target: Checkpoint { epoch: 64, root: [4; 32] },
        };
        let mut second_data = first_data.clone();
        second_data.beacon_block_root = [5; 32];
        let first_sig = aggregate(&state, &first_data, &keys[..2]);
        let second_sig = aggregate(&state, &second_data, &keys[..2]);
        let first =
            IndexedAttestationRef { indices: &[0, 1], data: &first_data, signature: &first_sig };
        let second =
            IndexedAttestationRef { indices: &[0, 1], data: &second_data, signature: &second_sig };
        let mut small = SeenOperations::new(1).unwrap();
        let empty = small.clone();
        assert!(matches!(
            validate_attester_slashing(
                &mut small,
                OperationState::from_state(&state),
                first,
                second,
                &parameters
            ),
            Err(OperationGossipError::Capacity)
        ));
        assert_eq!(small, empty);
        let mut seen = SeenOperations::new(8).unwrap();
        let empty = seen.clone();
        let unordered = IndexedAttestationRef { indices: &[1, 0], ..first };
        assert!(matches!(
            validate_attester_slashing(
                &mut seen,
                OperationState::from_state(&state),
                unordered,
                second,
                &parameters
            ),
            Err(OperationGossipError::Reject("invalid indexed attestation 1"))
        ));
        assert_eq!(seen, empty);
        let bad = IndexedAttestationRef { signature: &[0; 96], ..second };
        assert!(matches!(
            validate_attester_slashing(
                &mut seen,
                OperationState::from_state(&state),
                first,
                bad,
                &parameters
            ),
            Err(OperationGossipError::Reject("invalid indexed attestation 2"))
        ));
        assert_eq!(seen, empty);
        validate_attester_slashing(
            &mut seen,
            OperationState::from_state(&state),
            first,
            second,
            &parameters,
        )
        .unwrap();
        assert_eq!(seen.entries(), 2);
        assert!(matches!(
            validate_attester_slashing(
                &mut seen,
                OperationState::from_state(&state),
                first,
                second,
                &parameters
            ),
            Err(OperationGossipError::Ignore("all attester slashing indices already seen"))
        ));
    }

    #[test]
    fn duplicate_observations_are_bound_to_network_root_and_full_parameters() {
        let (mut state, keys, parameters) = context();
        let signed = exit(&state, &keys[0]);
        let boundary = 64 * parameters.slots_per_epoch * parameters.slot_duration_ms;
        let mut seen = SeenOperations::new(8).unwrap();
        validate_voluntary_exit(
            &mut seen,
            OperationState::from_state(&state),
            &signed,
            boundary,
            &parameters,
        )
        .unwrap();
        let before = seen.clone();
        let mut changed = parameters.clone();
        changed.capella_epoch = 1;
        assert!(matches!(
            validate_voluntary_exit(
                &mut seen,
                OperationState::from_state(&state),
                &signed,
                boundary,
                &changed
            ),
            Err(OperationGossipError::Context(ProcessingError::InvalidState(
                "operation gossip duplicate cache network binding differs"
            )))
        ));
        let BeaconState::Bellatrix(value, _) = &mut state else { unreachable!() };
        value.genesis_validators_root = [9; 32];
        assert!(matches!(
            validate_voluntary_exit(
                &mut seen,
                OperationState::from_state(&state),
                &signed,
                boundary,
                &parameters
            ),
            Err(OperationGossipError::Context(ProcessingError::InvalidState(
                "operation gossip duplicate cache network binding differs"
            )))
        ));
        assert_eq!(seen, before);
    }

    #[cfg(feature = "spec-tests")]
    #[test]
    fn declared_fixture_activation_keeps_mandatory_withdrawal_signature_and_original_state() {
        let (mut state, keys, parameters) = context();
        let message = BLSToExecutionChange {
            validator_index: 0,
            from_bls_pubkey: keys[1].sk_to_pk().to_bytes(),
            to_execution_address: [8; 20],
        };
        let BeaconState::Bellatrix(value, _) = &mut state else { unreachable!() };
        value.validators.values[0].withdrawal_credentials = sha256(&message.from_bls_pubkey);
        value.validators.values[0].withdrawal_credentials[0] = 0;
        let before = state.clone();
        let root = compute_signing_root(
            message.hash_tree_root(Preset::Minimal).unwrap(),
            compute_domain([10, 0, 0, 0], parameters.versions[0], state.genesis_validators_root()),
        );
        let signed = SignedBLSToExecutionChange {
            message,
            signature: keys[1].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        };
        let mut seen = SeenOperations::new(8).unwrap();
        assert!(matches!(
            validate_bls_to_execution_change(
                &mut seen,
                OperationState::from_state(&state),
                &signed,
                0,
                &parameters
            ),
            Err(OperationGossipError::Ignore("current epoch is pre-capella"))
        ));
        let fixture = spec_only::SpecOperationParameters::new(parameters, 0);
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert!(matches!(
            fixture.credential_change(&mut seen, OperationState::from_state(&state), &invalid, 0),
            Err(OperationGossipError::Reject("invalid BLS to execution change signature"))
        ));
        assert_eq!(seen.entries(), 0);
        fixture
            .credential_change(&mut seen, OperationState::from_state(&state), &signed, 0)
            .unwrap();
        assert_eq!(seen.entries(), 1);
        assert_eq!(state, before);
        assert!(std::ptr::eq(
            OperationState::from_state(&state).validators,
            fields_for_test(&state)
        ));
    }
    #[cfg(feature = "spec-tests")]
    fn fields_for_test(state: &BeaconState) -> &[Validator] {
        let BeaconState::Bellatrix(value, _) = state else { unreachable!() };
        &value.validators.values
    }
}
