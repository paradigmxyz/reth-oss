//! Immutable sync-committee gossip checks with real signatures and bounded observations.
//!
//! The caller authenticates the retained head and trusted network configuration. Gossip
//! acceptance changes only duplicate observations and does not grant consensus authority.

use crate::{
    fork_processing::TransitionParameters,
    phase0_processing::ProcessingError,
    ssz::{
        auxiliary::altair::{
            SignedContributionAndProof, SyncAggregatorSelectionData, SyncCommitteeMessage,
        },
        phase0::Validator,
        BeaconState, Preset,
    },
};
use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, eth_fast_aggregate_verify, verify_signature,
};
use reth_embedded_beacon_primitives::{sha256, Fork, Root};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Captured coherent network rules; individual signature and timing fields are not mutable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncCommitteeParameters {
    preset: Preset,
    slots_per_epoch: u64,
    slot_duration_ms: u64,
    epochs_per_period: u64,
    committee_size: usize,
    versions: [[u8; 4]; 8],
    fork_epochs: [u64; 8],
}

impl SyncCommitteeParameters {
    /// Capture validated transition policy without copying any consensus state or registry.
    pub fn from_transition_parameters(
        parameters: &TransitionParameters,
    ) -> Result<Self, SyncCommitteeGossipError> {
        parameters.validate()?;
        let slot_duration_ms = parameters
            .gloas
            .fulu
            .electra
            .deneb
            .capella
            .bellatrix
            .seconds_per_slot
            .checked_mul(1000)
            .ok_or(ProcessingError::Arithmetic("sync gossip slot duration"))?;
        if slot_duration_ms == 0 {
            return Err(
                ProcessingError::InvalidParameters("sync gossip slot duration is zero").into()
            )
        }
        let mut versions = [[0; 4]; 8];
        let mut fork_epochs = [0; 8];
        for ((version, epoch), activation) in
            versions.iter_mut().zip(&mut fork_epochs).zip(&parameters.schedule)
        {
            *version = activation.version;
            *epoch = activation.epoch;
        }
        Ok(Self {
            preset: parameters.preset(),
            slots_per_epoch: parameters.slots_per_epoch(),
            slot_duration_ms,
            epochs_per_period: match parameters.preset() {
                Preset::Mainnet => 256,
                Preset::Minimal => 8,
            },
            committee_size: match parameters.preset() {
                Preset::Mainnet => 512,
                Preset::Minimal => 32,
            },
            versions,
            fork_epochs,
        })
    }
}

/// Borrowed exact head fields used by sync gossip; no mutable state or authority token escapes.
#[derive(Clone, Copy)]
pub struct SyncCommitteeState<'a> {
    parameters: &'a SyncCommitteeParameters,
    validators: &'a [Validator],
    subcommittee_pubkeys: &'a [[u8; 48]],
    genesis_time: u64,
    genesis_validators_root: Root,
    previous_version: [u8; 4],
    current_version: [u8; 4],
    fork_epoch: u64,
    fork: Fork,
    head_slot: u64,
}

impl std::fmt::Debug for SyncCommitteeState<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SyncCommitteeState")
            .field("fork", &self.fork)
            .field("preset", &self.parameters.preset)
            .field("head_slot", &self.head_slot)
            .field("fork_epoch", &self.fork_epoch)
            .field("genesis_time", &self.genesis_time)
            .field("validator_count", &self.validators.len())
            .field("committee_pubkey_count", &self.subcommittee_pubkeys.len())
            .finish()
    }
}

impl<'a> SyncCommitteeState<'a> {
    /// Check complete structural bounds and capture the appropriate current/next committee.
    /// Authenticating this head's origin and ancestry remains the caller's responsibility.
    pub fn from_head_state(
        state: &'a BeaconState,
        parameters: &'a SyncCommitteeParameters,
    ) -> Result<Self, SyncCommitteeGossipError> {
        state.hash_tree_root().map_err(ProcessingError::from)?;
        let (validators, balances_count, current, next, preset, ordinal) = match state {
            BeaconState::Phase0(_, _) => {
                return Err(ProcessingError::InvalidState(
                    "sync committee unavailable before Altair",
                )
                .into())
            }
            BeaconState::Altair(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                1,
            ),
            BeaconState::Bellatrix(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                2,
            ),
            BeaconState::Capella(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                3,
            ),
            BeaconState::Deneb(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                4,
            ),
            BeaconState::Electra(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                5,
            ),
            BeaconState::Fulu(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                6,
            ),
            BeaconState::Gloas(value, preset) => (
                &value.validators.values[..],
                value.balances.values.len(),
                &value.current_sync_committee.pubkeys.values[..],
                &value.next_sync_committee.pubkeys.values[..],
                *preset,
                7,
            ),
        };
        let fork = state.fork_info();
        if preset != parameters.preset ||
            fork.current_version != parameters.versions[ordinal] ||
            balances_count != validators.len() ||
            current.len() != parameters.committee_size ||
            next.len() != parameters.committee_size
        {
            return Err(ProcessingError::InvalidState(
                "sync gossip head differs from network policy or registry bounds",
            )
            .into())
        }
        let next_slot = state
            .slot()
            .checked_add(1)
            .ok_or(ProcessingError::Arithmetic("sync gossip next head slot"))?;
        let current_period =
            state.slot() / parameters.slots_per_epoch / parameters.epochs_per_period;
        let next_period = next_slot / parameters.slots_per_epoch / parameters.epochs_per_period;
        Ok(Self {
            parameters,
            validators,
            subcommittee_pubkeys: if current_period == next_period { current } else { next },
            genesis_time: state.genesis_time(),
            genesis_validators_root: state.genesis_validators_root(),
            previous_version: fork.previous_version,
            current_version: fork.current_version,
            fork_epoch: fork.epoch,
            fork: state.schema_fork(),
            head_slot: state.slot(),
        })
    }
    fn subcommittee(&self, index: u64) -> Option<&'a [[u8; 48]]> {
        let index = usize::try_from(index).ok().filter(|index| *index < SUBNET_COUNT)?;
        let width = self.parameters.committee_size / SUBNET_COUNT;
        self.subcommittee_pubkeys.get(index * width..(index + 1) * width)
    }
    fn validator(&self, index: u64) -> Option<&'a Validator> {
        usize::try_from(index).ok().and_then(|index| self.validators.get(index))
    }
    fn domain(&self, domain_type: [u8; 4], slot: u64) -> Root {
        let epoch = slot / self.parameters.slots_per_epoch;
        let version =
            if epoch < self.fork_epoch { self.previous_version } else { self.current_version };
        compute_domain(domain_type, version, self.genesis_validators_root)
    }
    fn current_slot(
        &self,
        slot: u64,
        current_time_ms: u64,
    ) -> Result<bool, SyncCommitteeGossipError> {
        let duration = u128::from(self.parameters.slot_duration_ms);
        let genesis = u128::from(self.genesis_time) * 1000;
        let start = u128::from(slot)
            .checked_mul(duration)
            .and_then(|value| value.checked_add(genesis))
            .ok_or(ProcessingError::Arithmetic("sync gossip slot start"))?;
        let end = start
            .checked_add(duration)
            .and_then(|value| value.checked_add(CLOCK_DISPARITY_MS))
            .ok_or(ProcessingError::Arithmetic("sync gossip slot end"))?;
        let now = u128::from(current_time_ms);
        Ok(now + CLOCK_DISPARITY_MS >= start && now <= end)
    }
}

/// Duplicate observations bound to one exact chain and captured transition policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeenSyncCommittee {
    messages: BTreeSet<(u64, u64, u64)>,
    aggregators: BTreeSet<(u64, u64, u64)>,
    contributions: BTreeMap<(u64, Root, u64), Vec<Vec<u8>>>,
    parameters: SyncCommitteeParameters,
    genesis_time: u64,
    genesis_validators_root: Root,
    maximum_entries: usize,
    entries: usize,
}

impl SeenSyncCommittee {
    /// Bound total identities, contribution keys and bitsets before receiving messages.
    pub fn new(
        state: &SyncCommitteeState<'_>,
        maximum_entries: usize,
    ) -> Result<Self, SyncCommitteeGossipError> {
        if maximum_entries == 0 || maximum_entries > 1_048_576 {
            return Err(ProcessingError::InvalidParameters(
                "sync gossip cache budget is out of range",
            )
            .into())
        }
        Ok(Self {
            messages: BTreeSet::new(),
            aggregators: BTreeSet::new(),
            contributions: BTreeMap::new(),
            parameters: state.parameters.clone(),
            genesis_time: state.genesis_time,
            genesis_validators_root: state.genesis_validators_root,
            maximum_entries,
            entries: 0,
        })
    }
    /// Count retained identities, keys and bitsets; this excludes allocator bookkeeping.
    pub const fn entries(&self) -> usize {
        self.entries
    }
    /// Remove observations strictly older than the supplied slot.
    /// The caller must retain slots still inside the permitted clock-disparity window.
    pub fn prune_before_slot(&mut self, minimum_slot: u64) {
        self.messages.retain(|(slot, _, _)| *slot >= minimum_slot);
        self.aggregators.retain(|(slot, _, _)| *slot >= minimum_slot);
        self.contributions.retain(|(slot, _, _), _| *slot >= minimum_slot);
        self.entries = self.messages.len() +
            self.aggregators.len() +
            self.contributions.len() +
            self.contributions.values().map(Vec::len).sum::<usize>();
    }
    fn bind(&self, state: &SyncCommitteeState<'_>) -> Result<(), SyncCommitteeGossipError> {
        if self.parameters != *state.parameters ||
            self.genesis_time != state.genesis_time ||
            self.genesis_validators_root != state.genesis_validators_root
        {
            return Err(ProcessingError::InvalidParameters(
                "sync gossip observations belong to another chain or policy",
            )
            .into())
        }
        Ok(())
    }
    const fn admit(&self, additional: usize) -> Result<(), SyncCommitteeGossipError> {
        if additional > self.maximum_entries.saturating_sub(self.entries) {
            return Err(SyncCommitteeGossipError::Capacity)
        }
        Ok(())
    }
}

/// Propagation failures distinguish invalid messages from configuration and resource failures.
#[derive(Debug, Error)]
pub enum SyncCommitteeGossipError {
    /// Duplicate or out-of-time gossip may be ignored without changing observations.
    #[error("ignore sync gossip: {0}")]
    Ignore(&'static str),
    /// The message violates a mandatory consensus propagation check.
    #[error("reject sync gossip: {0}")]
    Reject(&'static str),
    /// Caller-provided head state or policy is inconsistent or overflows its checked bounds.
    #[error(transparent)]
    Context(#[from] ProcessingError),
    /// The observation budget cannot retain the complete checked message atomically.
    #[error("sync gossip observation cache capacity exhausted")]
    Capacity,
}

const SUBNET_COUNT: usize = 4;
const TARGET_AGGREGATORS: usize = 16;
const CLOCK_DISPARITY_MS: u128 = 500;

/// Validate exact subnet membership, current-slot tolerance and the actual validator signature.
pub fn validate_sync_committee_message(
    seen: &mut SeenSyncCommittee,
    state: &SyncCommitteeState<'_>,
    message: &SyncCommitteeMessage,
    current_time_ms: u64,
    subnet_id: u64,
) -> Result<(), SyncCommitteeGossipError> {
    seen.bind(state)?;
    let key = (message.slot, message.validator_index, subnet_id);
    if seen.messages.contains(&key) {
        return Err(SyncCommitteeGossipError::Ignore(
            "already seen message from this validator for this slot and subnet",
        ))
    }
    if !state.current_slot(message.slot, current_time_ms)? {
        return Err(SyncCommitteeGossipError::Ignore("message is not for the current slot"))
    }
    let validator = state
        .validator(message.validator_index)
        .ok_or(SyncCommitteeGossipError::Reject("validator index out of range"))?;
    if !state.subcommittee(subnet_id).is_some_and(|keys| keys.contains(&validator.pubkey)) {
        return Err(SyncCommitteeGossipError::Reject("subnet_id is not valid for the validator"))
    }
    let root =
        compute_signing_root(message.beacon_block_root, state.domain([7, 0, 0, 0], message.slot));
    verify_signature(&validator.pubkey, &root, &message.signature).map_err(|_| {
        SyncCommitteeGossipError::Reject("invalid sync committee message signature")
    })?;
    seen.admit(1)?;
    seen.messages.insert(key);
    seen.entries += 1;
    Ok(())
}

/// Validate all three signatures and aggregator selection before publishing any observations.
pub fn validate_sync_committee_contribution_and_proof(
    seen: &mut SeenSyncCommittee,
    state: &SyncCommitteeState<'_>,
    signed: &SignedContributionAndProof,
    current_time_ms: u64,
) -> Result<(), SyncCommitteeGossipError> {
    seen.bind(state)?;
    signed.hash_tree_root(state.parameters.preset).map_err(|_| {
        SyncCommitteeGossipError::Reject("noncanonical sync contribution container")
    })?;
    let message = &signed.message;
    let contribution = &message.contribution;
    let key = (contribution.slot, contribution.beacon_block_root, contribution.subcommittee_index);
    let bits = contribution.aggregation_bits.bits.as_ssz();
    if seen.contributions.get(&key).is_some_and(|prior| {
        prior.iter().any(|prior| prior.iter().zip(bits).all(|(old, new)| new & !old == 0))
    }) {
        return Err(SyncCommitteeGossipError::Ignore("already seen contribution for this data"))
    }
    let aggregator_key =
        (contribution.slot, message.aggregator_index, contribution.subcommittee_index);
    if seen.aggregators.contains(&aggregator_key) {
        return Err(SyncCommitteeGossipError::Ignore(
            "already seen contribution from this aggregator",
        ))
    }
    if !state.current_slot(contribution.slot, current_time_ms)? {
        return Err(SyncCommitteeGossipError::Ignore("contribution is not for the current slot"))
    }
    let subcommittee = state
        .subcommittee(contribution.subcommittee_index)
        .ok_or(SyncCommitteeGossipError::Reject("subcommittee index out of range"))?;
    if !bits.iter().any(|byte| *byte != 0) {
        return Err(SyncCommitteeGossipError::Reject("contribution has no participants"))
    }
    let modulo = (subcommittee.len() / TARGET_AGGREGATORS).max(1) as u64;
    let hash = sha256(&message.selection_proof);
    if u64::from_le_bytes(hash[..8].try_into().expect("eight hash bytes")) % modulo != 0 {
        return Err(SyncCommitteeGossipError::Reject("validator is not selected as aggregator"))
    }
    let aggregator = state
        .validator(message.aggregator_index)
        .ok_or(SyncCommitteeGossipError::Reject("aggregator index out of range"))?;
    if !subcommittee.contains(&aggregator.pubkey) {
        return Err(SyncCommitteeGossipError::Reject("aggregator is not a member of the committee"))
    }
    let selection = SyncAggregatorSelectionData {
        slot: contribution.slot,
        subcommittee_index: contribution.subcommittee_index,
    };
    let root = compute_signing_root(
        selection.hash_tree_root(state.parameters.preset).map_err(ProcessingError::from)?,
        state.domain([8, 0, 0, 0], contribution.slot),
    );
    verify_signature(&aggregator.pubkey, &root, &message.selection_proof)
        .map_err(|_| SyncCommitteeGossipError::Reject("invalid selection proof signature"))?;
    let root = compute_signing_root(
        message.hash_tree_root(state.parameters.preset).map_err(ProcessingError::from)?,
        state.domain([9, 0, 0, 0], contribution.slot),
    );
    verify_signature(&aggregator.pubkey, &root, &signed.signature)
        .map_err(|_| SyncCommitteeGossipError::Reject("invalid aggregator signature"))?;
    let participants = subcommittee
        .iter()
        .enumerate()
        .filter_map(|(index, pubkey)| {
            (contribution.aggregation_bits.bits.get(index) == Some(true)).then_some(*pubkey)
        })
        .collect::<Vec<_>>();
    let root = compute_signing_root(
        contribution.beacon_block_root,
        state.domain([7, 0, 0, 0], contribution.slot),
    );
    eth_fast_aggregate_verify(&participants, &root, &contribution.signature)
        .map_err(|_| SyncCommitteeGossipError::Reject("invalid aggregate signature"))?;
    let additional = 2 + usize::from(!seen.contributions.contains_key(&key));
    seen.admit(additional)?;
    seen.aggregators.insert(aggregator_key);
    seen.contributions.entry(key).or_default().push(bits.to_vec());
    seen.entries += additional;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::{
        auxiliary::altair::{
            ContributionAndProof, SyncCommitteeContribution, SyncSubcommitteeBits,
        },
        Ssz,
    };
    use blst::min_pk::{AggregateSignature, SecretKey};
    use reth_embedded_beacon_crypto::{aggregate_pubkeys, ETHEREUM_BLS_DST};
    use reth_embedded_beacon_primitives::Fork;

    #[test]
    fn message_signatures_duplicates_and_capacity_are_atomic() {
        let (state, keys) = fixture_state();
        let parameters = parameters();
        let view = SyncCommitteeState::from_head_state(&state, &parameters).unwrap();
        let message = signed_message(&view, &keys, 0, 0);
        let before = state.to_ssz().unwrap();
        let mut seen = SeenSyncCommittee::new(&view, 1).unwrap();
        let mut invalid = message.clone();
        invalid.signature = [0; 96];
        assert!(matches!(
            validate_sync_committee_message(&mut seen, &view, &invalid, 500, 0),
            Err(SyncCommitteeGossipError::Reject("invalid sync committee message signature"))
        ));
        assert_eq!(seen.entries(), 0);
        validate_sync_committee_message(&mut seen, &view, &message, 500, 0).unwrap();
        let checked = seen.clone();
        assert!(matches!(
            validate_sync_committee_message(&mut seen, &view, &message, 600, 0),
            Err(SyncCommitteeGossipError::Ignore(
                "already seen message from this validator for this slot and subnet"
            ))
        ));
        assert_eq!(seen, checked);
        let other = signed_message(&view, &keys, 1, 0);
        assert!(matches!(
            validate_sync_committee_message(&mut seen, &view, &other, 6_500, 0),
            Err(SyncCommitteeGossipError::Capacity)
        ));
        assert_eq!(seen, checked);
        assert_eq!(state.to_ssz().unwrap(), before);
        seen.prune_before_slot(1);
        assert_eq!(seen.entries(), 0);
    }

    #[test]
    fn contribution_checks_all_signatures_before_atomic_seen_publication() {
        let (state, keys) = fixture_state();
        let parameters = parameters();
        let view = SyncCommitteeState::from_head_state(&state, &parameters).unwrap();
        let signed = signed_contribution(&view, &keys, 3);
        let mut seen = SeenSyncCommittee::new(&view, 16).unwrap();
        let empty = seen.clone();
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        assert!(matches!(
            validate_sync_committee_contribution_and_proof(&mut seen, &view, &invalid, 500),
            Err(SyncCommitteeGossipError::Reject("invalid aggregator signature"))
        ));
        assert_eq!(seen, empty);
        invalid = signed.clone();
        invalid.message.contribution.signature = [0; 96];
        let key = &keys[invalid.message.aggregator_index as usize];
        let root = compute_signing_root(
            invalid.message.hash_tree_root(Preset::Minimal).unwrap(),
            view.domain([9, 0, 0, 0], 0),
        );
        invalid.signature = key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
        assert!(matches!(
            validate_sync_committee_contribution_and_proof(&mut seen, &view, &invalid, 500),
            Err(SyncCommitteeGossipError::Reject("invalid aggregate signature"))
        ));
        assert_eq!(seen, empty);
        let mut constrained = SeenSyncCommittee::new(&view, 2).unwrap();
        let constrained_before = constrained.clone();
        assert!(matches!(
            validate_sync_committee_contribution_and_proof(&mut constrained, &view, &signed, 500),
            Err(SyncCommitteeGossipError::Capacity)
        ));
        assert_eq!(constrained, constrained_before);
        validate_sync_committee_contribution_and_proof(&mut seen, &view, &signed, 500).unwrap();
        assert_eq!(seen.entries(), 3);
        let checked = seen.clone();
        let subset = signed_contribution(&view, &keys, 1);
        assert!(matches!(
            validate_sync_committee_contribution_and_proof(&mut seen, &view, &subset, 600),
            Err(SyncCommitteeGossipError::Ignore("already seen contribution for this data"))
        ));
        assert_eq!(seen, checked);
    }

    #[test]
    fn boundary_head_uses_next_committee_without_replacing_state() {
        let (mut state, keys) = fixture_state();
        let BeaconState::Bellatrix(value, _) = &mut state else { unreachable!() };
        let pubkeys = keys.iter().map(|key| key.sk_to_pk().to_bytes()).collect::<Vec<_>>();
        let aggregate = aggregate_pubkeys(&pubkeys).unwrap();
        value.current_sync_committee.pubkeys.values = pubkeys.clone();
        value.current_sync_committee.aggregate_pubkey = aggregate;
        value.next_sync_committee.pubkeys.values = pubkeys.into_iter().rev().collect();
        value.next_sync_committee.aggregate_pubkey = aggregate;
        value.slot = 63;
        let parameters = parameters();
        let view = SyncCommitteeState::from_head_state(&state, &parameters).unwrap();
        let message = signed_message(&view, &keys, 63, 0);
        assert_eq!(message.validator_index, 31);
        let mut seen = SeenSyncCommittee::new(&view, 8).unwrap();
        validate_sync_committee_message(&mut seen, &view, &message, 378_500, 0).unwrap();
        let mut previous = state.clone();
        let BeaconState::Bellatrix(value, _) = &mut previous else { unreachable!() };
        value.slot = 62;
        let earlier = SyncCommitteeState::from_head_state(&previous, &parameters).unwrap();
        let mut unseen = SeenSyncCommittee::new(&earlier, 8).unwrap();
        assert!(matches!(
            validate_sync_committee_message(&mut unseen, &earlier, &message, 378_500, 0),
            Err(SyncCommitteeGossipError::Reject("subnet_id is not valid for the validator"))
        ));
    }

    #[test]
    fn observations_bind_chain_and_policy_and_clock_edges_are_inclusive() {
        let (state, keys) = fixture_state();
        let parameters = parameters();
        let view = SyncCommitteeState::from_head_state(&state, &parameters).unwrap();
        assert!(view.current_slot(1, 5_500).unwrap());
        assert!(!view.current_slot(1, 5_499).unwrap());
        assert!(view.current_slot(1, 12_500).unwrap());
        assert!(!view.current_slot(1, 12_501).unwrap());
        let mut seen = SeenSyncCommittee::new(&view, 8).unwrap();
        let mut changed = state.clone();
        let BeaconState::Bellatrix(value, _) = &mut changed else { unreachable!() };
        value.genesis_validators_root[0] ^= 1;
        let other = SyncCommitteeState::from_head_state(&changed, &parameters).unwrap();
        let message = signed_message(&view, &keys, 0, 0);
        assert!(matches!(
            validate_sync_committee_message(&mut seen, &other, &message, 500, 0),
            Err(SyncCommitteeGossipError::Context(_))
        ));
        assert_eq!(seen.entries(), 0);
        let mut overflowing = parameters.clone();
        overflowing.slot_duration_ms = u64::MAX;
        let mut clock = SyncCommitteeState::from_head_state(&state, &overflowing).unwrap();
        clock.genesis_time = u64::MAX;
        assert!(matches!(
            clock.current_slot(u64::MAX, u64::MAX),
            Err(SyncCommitteeGossipError::Context(_))
        ));
    }

    fn fixture_state() -> (BeaconState, Vec<SecretKey>) {
        let (state, keys) = crate::bellatrix_processing::tests::state_and_keys();
        (BeaconState::Bellatrix(state, Preset::Minimal), keys)
    }
    fn parameters() -> SyncCommitteeParameters {
        let mut transition = TransitionParameters::for_preset(Preset::Minimal);
        for entry in &mut transition.schedule {
            if entry.fork <= Fork::Bellatrix {
                entry.epoch = 0;
            }
        }
        SyncCommitteeParameters::from_transition_parameters(&transition).unwrap()
    }
    fn key_index(state: &SyncCommitteeState<'_>, pubkey: &[u8; 48]) -> usize {
        state.validators.iter().position(|validator| validator.pubkey == *pubkey).unwrap()
    }
    fn signed_message(
        state: &SyncCommitteeState<'_>,
        keys: &[SecretKey],
        slot: u64,
        subnet: u64,
    ) -> SyncCommitteeMessage {
        let index = key_index(state, &state.subcommittee(subnet).unwrap()[0]);
        let root = compute_signing_root([3; 32], state.domain([7, 0, 0, 0], slot));
        SyncCommitteeMessage {
            slot,
            beacon_block_root: [3; 32],
            validator_index: index as u64,
            signature: keys[index].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    }
    fn signed_contribution(
        state: &SyncCommitteeState<'_>,
        keys: &[SecretKey],
        bits: u8,
    ) -> SignedContributionAndProof {
        let subcommittee = state.subcommittee(0).unwrap();
        let aggregator_index = key_index(state, &subcommittee[0]);
        let selection = SyncAggregatorSelectionData { slot: 0, subcommittee_index: 0 };
        let root = compute_signing_root(
            selection.hash_tree_root(Preset::Minimal).unwrap(),
            state.domain([8, 0, 0, 0], 0),
        );
        let selection_proof = keys[aggregator_index].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
        let root = compute_signing_root([4; 32], state.domain([7, 0, 0, 0], 0));
        let signatures = subcommittee
            .iter()
            .enumerate()
            .filter(|(index, _)| bits & (1 << index) != 0)
            .map(|(_, key)| keys[key_index(state, key)].sign(&root, ETHEREUM_BLS_DST, &[]))
            .collect::<Vec<_>>();
        let aggregate = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
            .unwrap()
            .to_signature()
            .to_bytes();
        let message = ContributionAndProof {
            aggregator_index: aggregator_index as u64,
            contribution: SyncCommitteeContribution {
                slot: 0,
                beacon_block_root: [4; 32],
                subcommittee_index: 0,
                aggregation_bits: SyncSubcommitteeBits::from_ssz(&[bits], Preset::Minimal).unwrap(),
                signature: aggregate,
            },
            selection_proof,
        };
        let root = compute_signing_root(
            message.hash_tree_root(Preset::Minimal).unwrap(),
            state.domain([9, 0, 0, 0], 0),
        );
        SignedContributionAndProof {
            message,
            signature: keys[aggregator_index].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    }
}
