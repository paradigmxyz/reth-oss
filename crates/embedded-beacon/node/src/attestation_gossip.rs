//! Ordered attestation propagation against borrowed authenticated head states.
//! Cache observations remain separate from consensus votes, payload validity and block import.

use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, fast_aggregate_verify, verify_signature,
};
use reth_embedded_beacon_fork_choice::{
    compact::{ReadGloasGossipView, ReadGossipView, StateAccess},
    fast_confirmation::StateRef,
    ExecutionStatus, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{
    sha256, BitList, BitVector, Checkpoint, Fork, Root, SszError,
};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing,
    fork_processing::TransitionParameters,
    fulu_processing,
    gloas_processing::{self, GloasParameters},
    phase0_processing::{self, ProcessingError},
    ssz::{
        self, auxiliary,
        phase0::{AttestationData, ForkInfo, Validator},
        BeaconState, Preset,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Finite identities and complete packed bitsets, bound to one exact chain and store policy.
#[derive(Clone, PartialEq, Eq)]
pub struct SeenAttestations {
    validators: BTreeSet<(u64, u64)>,
    aggregators: BTreeSet<(u64, u64)>,
    aggregates: BTreeMap<(Root, u64), Vec<SeenBits>>,
    maximum_entries: usize,
    maximum_bit_bytes: usize,
    bit_bytes: usize,
    binding: Option<Binding>,
}

impl fmt::Debug for SeenAttestations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeenAttestations")
            .field("entries", &self.entries())
            .field("bit_bytes", &self.bit_bytes)
            .field("bound", &self.binding.is_some())
            .finish_non_exhaustive()
    }
}

impl SeenAttestations {
    /// Bound total identity/bitset entries and independently bound their packed allocation bytes.
    pub fn new(
        maximum_entries: usize,
        maximum_bit_bytes: usize,
    ) -> Result<Self, AttestationGossipError> {
        if maximum_entries == 0 ||
            maximum_entries > 1_048_576 ||
            maximum_bit_bytes == 0 ||
            maximum_bit_bytes > 256 * 1024 * 1024
        {
            return Err(context("attestation cache budget is out of range"))
        }
        Ok(Self {
            validators: BTreeSet::new(),
            aggregators: BTreeSet::new(),
            aggregates: BTreeMap::new(),
            maximum_entries,
            maximum_bit_bytes,
            bit_bytes: 0,
            binding: None,
        })
    }
    /// Exact retained identity and bitset count without counting the same data key twice.
    pub fn entries(&self) -> usize {
        self.validators.len() +
            self.aggregators.len() +
            self.aggregates.values().map(Vec::len).sum::<usize>()
    }
    /// Prune old accepted epochs while preserving the original chain/policy binding.
    pub fn retain_from_epoch(&mut self, epoch: u64) {
        self.validators.retain(|(value, _)| *value >= epoch);
        self.aggregators.retain(|(value, _)| *value >= epoch);
        self.aggregates.retain(|_, sets| {
            sets.retain(|value| value.epoch >= epoch);
            !sets.is_empty()
        });
        self.bit_bytes = self.aggregates.values().flatten().map(|value| value.bytes.len()).sum();
    }
    /// Check legacy single-participant wire messages using mandatory real signatures.
    pub fn validate_attestation<V: ReadGossipView>(
        &mut self,
        store: &V,
        message: LegacyAttestation<'_>,
        time_ms: u64,
        subnet: u64,
    ) -> Result<(), AttestationGossipError> {
        self.legacy(&ClassicView(store), message.fields(), time_ms, subnet, message.fork())
    }
    /// Check the complete Electra/Fulu single-attestation wire format.
    pub fn validate_single<V: ReadGossipView>(
        &mut self,
        store: &V,
        message: SingleAttestation<'_>,
        time_ms: u64,
        subnet: u64,
    ) -> Result<(), AttestationGossipError> {
        self.single(&ClassicView(store), message.fields(), time_ms, subnet, false)
    }
    /// Check the complete progressive Gloas single-attestation wire format and payload status.
    pub fn validate_gloas_single<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: &auxiliary::gloas::SingleAttestation,
        time_ms: u64,
        subnet: u64,
    ) -> Result<(), AttestationGossipError> {
        self.single(
            &EnvelopeView(store),
            SingleFields {
                data: &message.data,
                committee: message.committee_index,
                participant: message.attester_index,
                signature: &message.signature,
            },
            time_ms,
            subnet,
            true,
        )
    }
    /// Check all three aggregate signatures and exact single-committee participation.
    pub fn validate_aggregate<V: ReadGossipView>(
        &mut self,
        store: &V,
        message: SignedAggregate<'_>,
        time_ms: u64,
    ) -> Result<(), AttestationGossipError> {
        self.aggregate(&ClassicView(store), message.fields(), time_ms, message.fork(), |preset| {
            message.message_root(preset)
        })
    }
    /// Check the actual progressive Gloas aggregate root and independently verified payload
    /// context.
    pub fn validate_gloas_aggregate<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: &auxiliary::gloas::SignedAggregateAndProof,
        time_ms: u64,
    ) -> Result<(), AttestationGossipError> {
        let proof = &message.message;
        let aggregate = &proof.aggregate;
        self.aggregate(
            &EnvelopeView(store),
            AggregateFields {
                data: &aggregate.data,
                bits: &aggregate.aggregation_bits.bits,
                committee_bits: Some(&aggregate.committee_bits.bits),
                aggregator: proof.aggregator_index,
                selection_proof: &proof.selection_proof,
                aggregate_signature: &aggregate.signature,
                outer_signature: &message.signature,
            },
            time_ms,
            Fork::Gloas,
            |preset| proof.hash_tree_root(preset),
        )
    }
    /// Validate an original earlier-fork single-participant wire against migrated graph context.
    /// The retained full schedule selects its real wire schema and signed domain.
    pub fn validate_historical_attestation<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: LegacyAttestation<'_>,
        time_ms: u64,
        subnet: u64,
    ) -> Result<(), AttestationGossipError> {
        let fields = message.fields();
        checked_historical_wire(store, fields.data.slot, message.fork())?;
        self.legacy(&EnvelopeView(store), fields, time_ms, subnet, message.fork())
    }
    /// Validate the original Electra/Fulu single wire without converting it to Gloas.
    pub fn validate_historical_single<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: SingleAttestation<'_>,
        time_ms: u64,
        subnet: u64,
    ) -> Result<(), AttestationGossipError> {
        let fields = message.fields();
        checked_historical_wire(store, fields.data.slot, message.fork())?;
        self.single(&EnvelopeView(store), fields, time_ms, subnet, false)
    }
    /// Validate every original earlier-fork aggregate signature against genuine migrated context.
    pub fn validate_historical_aggregate<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: SignedAggregate<'_>,
        time_ms: u64,
    ) -> Result<(), AttestationGossipError> {
        let fields = message.fields();
        checked_historical_wire(store, fields.data.slot, message.fork())?;
        self.aggregate(&EnvelopeView(store), fields, time_ms, message.fork(), |preset| {
            message.message_root(preset)
        })
    }
    fn checked_binding(&self, view: &impl View) -> Result<Binding, AttestationGossipError> {
        let binding = view.binding()?;
        if self.binding.as_ref().is_some_and(|current| current != &binding) {
            return Err(context("attestation cache chain or store policy differs"))
        }
        Ok(binding)
    }
    fn capacity(&self, entries: usize, bytes: usize) -> Result<(), AttestationGossipError> {
        if entries > self.maximum_entries.saturating_sub(self.entries()) ||
            bytes > self.maximum_bit_bytes.saturating_sub(self.bit_bytes)
        {
            return Err(AttestationGossipError::Capacity)
        }
        Ok(())
    }
    fn legacy(
        &mut self,
        view: &impl View,
        message: LegacyFields<'_>,
        time: u64,
        subnet: u64,
        fork: Fork,
    ) -> Result<(), AttestationGossipError> {
        let binding = self.checked_binding(view)?;
        let data = message.data;
        let head = checked_vote_head(view, data.beacon_block_root)?;
        let count = committee_count(&head, data.target.epoch, binding.slots_per_epoch);
        checked_committee_index(data.index, count)?;
        checked_subnet(count, data.slot, data.index, binding.slots_per_epoch, subnet)?;
        checked_time(&binding, data.slot, time, fork, false)?;
        checked_target_epoch(data, binding.slots_per_epoch)?;
        if (0..message.bits.len()).filter(|&index| message.bits.get(index) == Some(true)).count() !=
            1
        {
            return Err(AttestationGossipError::Reject("attestation is not unaggregated"))
        }
        let committee = head.committee(data.slot, data.index)?;
        checked_bit_length(message.bits, committee.len())?;
        let bit = (0..message.bits.len())
            .find(|&index| message.bits.get(index) == Some(true))
            .ok_or_else(|| context("unaggregated participant disappeared"))?;
        let participant = committee[bit];
        let key = (data.target.epoch, participant);
        if self.validators.contains(&key) {
            return Err(AttestationGossipError::Ignore(
                "already seen attestation for this epoch and validator",
            ))
        }
        checked_signature(
            &head,
            data,
            &[participant],
            message.signature,
            "invalid attestation signature",
        )?;
        checked_ancestry(view, data)?;
        self.capacity(1, 0)?;
        self.binding.get_or_insert(binding);
        self.validators.insert(key);
        Ok(())
    }
    fn single(
        &mut self,
        view: &impl View,
        message: SingleFields<'_>,
        time: u64,
        subnet: u64,
        gloas: bool,
    ) -> Result<(), AttestationGossipError> {
        let binding = self.checked_binding(view)?;
        let data = message.data;
        let key = (data.target.epoch, message.participant);
        if self.validators.contains(&key) {
            return Err(AttestationGossipError::Ignore(
                "already seen attestation for this epoch and validator",
            ))
        }
        if (!gloas && data.index != 0) || (gloas && data.index > 1) {
            return Err(AttestationGossipError::Reject(if gloas {
                "attestation data index must be 0 or 1"
            } else {
                "attestation data index is non-zero"
            }))
        }
        let head = checked_vote_head(view, data.beacon_block_root)?;
        let count = committee_count(&head, data.target.epoch, binding.slots_per_epoch);
        checked_committee_index(message.committee, count)?;
        checked_subnet(count, data.slot, message.committee, binding.slots_per_epoch, subnet)?;
        checked_time(&binding, data.slot, time, Fork::Electra, false)?;
        checked_target_epoch(data, binding.slots_per_epoch)?;
        let committee = head.committee(data.slot, message.committee)?;
        if !committee.contains(&message.participant) {
            return Err(AttestationGossipError::Reject("attester is not a member of the committee"))
        }
        checked_signature(
            &head,
            data,
            &[message.participant],
            message.signature,
            "invalid attestation signature",
        )?;
        checked_ancestry(view, data)?;
        if gloas {
            view.check_payload(data)?;
        }
        self.capacity(1, 0)?;
        self.binding.get_or_insert(binding);
        self.validators.insert(key);
        Ok(())
    }
    fn aggregate(
        &mut self,
        view: &impl View,
        message: AggregateFields<'_>,
        time: u64,
        fork: Fork,
        message_root: impl FnOnce(Preset) -> Result<Root, SszError>,
    ) -> Result<(), AttestationGossipError> {
        let binding = self.checked_binding(view)?;
        let data = message.data;
        let index = if fork >= Fork::Electra {
            if (fork != Fork::Gloas && data.index != 0) || (fork == Fork::Gloas && data.index > 1) {
                return Err(AttestationGossipError::Reject(if fork == Fork::Gloas {
                    "aggregate data index must be 0 or 1"
                } else {
                    "aggregate data index is non-zero"
                }))
            }
            let bits = message
                .committee_bits
                .ok_or_else(|| context("modern aggregate committee bits unavailable"))?;
            let mut indices = (0..bits.len()).filter(|&index| bits.get(index) == Some(true));
            let first = indices.next();
            if first.is_none() || indices.next().is_some() {
                return Err(AttestationGossipError::Reject(
                    "aggregate committee bits must specify exactly one committee",
                ))
            }
            first.ok_or_else(|| context("aggregate committee disappeared"))? as u64
        } else {
            data.index
        };
        if message.bits.packed().len() > self.maximum_bit_bytes {
            return Err(AttestationGossipError::Capacity)
        }
        let cache_key = (data.hash_tree_root(binding.configuration.preset())?, index);
        if self
            .aggregates
            .get(&cache_key)
            .is_some_and(|sets| sets.iter().any(|seen| seen.is_superset(message.bits)))
        {
            return Err(AttestationGossipError::Ignore("already seen aggregate for this data"))
        }
        let identity = (data.target.epoch, message.aggregator);
        if self.aggregators.contains(&identity) {
            return Err(AttestationGossipError::Ignore(
                "already seen aggregate for this epoch and aggregator",
            ))
        }
        let head = checked_vote_head(view, data.beacon_block_root)?;
        checked_committee_index(
            index,
            committee_count(&head, data.target.epoch, binding.slots_per_epoch),
        )?;
        checked_time(&binding, data.slot, time, fork, true)?;
        checked_target_epoch(data, binding.slots_per_epoch)?;
        let committee = head.committee(data.slot, index)?;
        checked_bit_length(message.bits, committee.len())?;
        let mut participants = committee
            .iter()
            .enumerate()
            .filter_map(|(index, &validator)| {
                (message.bits.get(index) == Some(true)).then_some(validator)
            })
            .collect::<Vec<_>>();
        if participants.is_empty() {
            return Err(AttestationGossipError::Reject("aggregate has no participants"))
        }
        let digest = sha256(message.selection_proof);
        let mut selection = [0; 8];
        selection.copy_from_slice(&digest[..8]);
        if u64::from_le_bytes(selection) % (committee.len() as u64 / 16).max(1) != 0 {
            return Err(AttestationGossipError::Reject("validator is not selected as aggregator"))
        }
        if !committee.contains(&message.aggregator) {
            return Err(AttestationGossipError::Reject(
                "aggregator is not a member of the committee",
            ))
        }
        let fields = head.fields();
        let aggregator = fields.validator(message.aggregator)?;
        let mut slot_root = [0; 32];
        slot_root[..8].copy_from_slice(&data.slot.to_le_bytes());
        let selected_root =
            compute_signing_root(slot_root, fields.domain([5, 0, 0, 0], data.target.epoch));
        verify_signature(&aggregator.pubkey, &selected_root, message.selection_proof)
            .map_err(|_| AttestationGossipError::Reject("invalid selection proof signature"))?;
        let outer_root = compute_signing_root(
            message_root(binding.configuration.preset())?,
            fields.domain([6, 0, 0, 0], data.target.epoch),
        );
        verify_signature(&aggregator.pubkey, &outer_root, message.outer_signature)
            .map_err(|_| AttestationGossipError::Reject("invalid aggregator signature"))?;
        participants.sort_unstable();
        checked_signature(
            &head,
            data,
            &participants,
            message.aggregate_signature,
            "invalid aggregate signature",
        )?;
        checked_ancestry(view, data)?;
        if fork == Fork::Gloas {
            view.check_payload(data)?;
        }
        self.capacity(2, message.bits.packed().len())?;
        let seen = SeenBits {
            epoch: data.target.epoch,
            len: message.bits.len(),
            bytes: message.bits.packed().to_vec(),
        };
        self.binding.get_or_insert(binding);
        self.aggregators.insert(identity);
        self.bit_bytes += seen.bytes.len();
        self.aggregates.entry(cache_key).or_default().push(seen);
        Ok(())
    }
}

/// Normative propagation rejection remains distinct from local state or capacity failure.
#[derive(Debug, thiserror::Error)]
pub enum AttestationGossipError {
    /// Redundant, premature or missing-prerequisite message.
    #[error("ignore attestation gossip: {0}")]
    Ignore(&'static str),
    /// Structurally or cryptographically invalid message.
    #[error("reject attestation gossip: {0}")]
    Reject(&'static str),
    /// Local trusted context is inconsistent or unavailable.
    #[error(transparent)]
    Context(#[from] ProcessingError),
    /// Authoritative retained graph query failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Complete typed SSZ object is invalid.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Local bounded identity or bitset capacity is exhausted.
    #[error("attestation gossip cache capacity exhausted")]
    Capacity,
}

#[derive(Clone, PartialEq, Eq)]
struct SeenBits {
    epoch: u64,
    len: usize,
    bytes: Vec<u8>,
}
impl SeenBits {
    fn is_superset(&self, incoming: &BitList) -> bool {
        self.len == incoming.len() &&
            self.bytes
                .iter()
                .zip(incoming.packed())
                .all(|(seen, incoming)| seen & incoming == *incoming)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    network: Root,
    genesis_ms: u64,
    slot_ms: u64,
    slots_per_epoch: u64,
    store: StoreParameters,
    configuration: Configuration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Configuration {
    Classic(TransitionParameters),
    Gloas(GloasParameters),
}
impl Configuration {
    const fn preset(&self) -> Preset {
        match self {
            Self::Classic(value) => value.preset(),
            Self::Gloas(value) => value.preset(),
        }
    }
}

struct LegacyFields<'a> {
    data: &'a AttestationData,
    bits: &'a BitList,
    signature: &'a [u8; 96],
}
struct SingleFields<'a> {
    data: &'a AttestationData,
    committee: u64,
    participant: u64,
    signature: &'a [u8; 96],
}
struct AggregateFields<'a> {
    data: &'a AttestationData,
    bits: &'a BitList,
    committee_bits: Option<&'a BitVector>,
    aggregator: u64,
    selection_proof: &'a [u8; 96],
    aggregate_signature: &'a [u8; 96],
    outer_signature: &'a [u8; 96],
}

enum Head<'a> {
    Classic(StateAccess<'a, BeaconState>, &'a TransitionParameters),
    Gloas(StateAccess<'a, ssz::gloas::BeaconState>, &'a GloasParameters),
}
struct Fields<'a> {
    validators: &'a [Validator],
    fork: ForkInfo,
    network: Root,
}
impl Fields<'_> {
    fn domain(&self, kind: [u8; 4], epoch: u64) -> Root {
        compute_domain(
            kind,
            if epoch < self.fork.epoch {
                self.fork.previous_version
            } else {
                self.fork.current_version
            },
            self.network,
        )
    }
    fn validator(&self, index: u64) -> Result<&Validator, AttestationGossipError> {
        usize::try_from(index)
            .ok()
            .and_then(|index| self.validators.get(index))
            .ok_or_else(|| context("checked committee validator is missing from retained registry"))
    }
}

trait View {
    fn head(&self) -> Result<Head<'_>, AttestationGossipError>;
    fn block_slot(&self, root: Root) -> Option<u64>;
    fn has_state(&self, root: Root) -> Result<bool, AttestationGossipError>;
    fn checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, AttestationGossipError>;
    fn finalized(&self) -> Checkpoint;
    fn binding(&self) -> Result<Binding, AttestationGossipError>;
    fn check_payload(&self, _: &AttestationData) -> Result<(), AttestationGossipError> {
        Err(context("Gloas payload context is unavailable"))
    }
}
struct ClassicView<'a, V>(&'a V);
impl<V: ReadGossipView> View for ClassicView<'_, V> {
    fn head(&self) -> Result<Head<'_>, AttestationGossipError> {
        if !V::VERIFY_SIGNATURES {
            return Err(context("attestation gossip requires a strict signature-policy view"))
        }
        let root = self.0.get_head()?.root;
        Ok(Head::Classic(
            self.0.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?,
            self.0
                .transition_parameters()
                .ok_or_else(|| context("attestation transition parameters unavailable"))?,
        ))
    }
    fn block_slot(&self, root: Root) -> Option<u64> {
        self.0.block(root).map(block_slot)
    }
    fn has_state(&self, root: Root) -> Result<bool, AttestationGossipError> {
        Ok(self.0.read_block_state(root)?.is_some())
    }
    fn checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, AttestationGossipError> {
        Ok(self.0.get_checkpoint_block(root, epoch)?)
    }
    fn finalized(&self) -> Checkpoint {
        self.0.checkpoints().1
    }
    fn binding(&self) -> Result<Binding, AttestationGossipError> {
        let head = self.head()?;
        let parameters = self
            .0
            .transition_parameters()
            .ok_or_else(|| context("attestation transition parameters unavailable"))?;
        parameters.validate()?;
        let binding = Binding {
            network: head.fields().network,
            genesis_ms: self.0.genesis_time_ms(),
            slot_ms: self.0.slot_duration_ms(),
            slots_per_epoch: self.0.slots_per_epoch(),
            store: self.0.store_parameters(),
            configuration: Configuration::Classic(parameters.clone()),
        };
        checked_clock_binding(&binding)?;
        Ok(binding)
    }
}
struct EnvelopeView<'a, V>(&'a V);
impl<V: ReadGloasGossipView> View for EnvelopeView<'_, V> {
    fn head(&self) -> Result<Head<'_>, AttestationGossipError> {
        let root = self.0.get_head()?.root;
        if let Some(state) = self.0.read_block_state(root)? {
            Ok(Head::Gloas(state, self.0.processing_parameters()))
        } else if let Some(state) = self.0.read_historical_block_state(root)? {
            Ok(Head::Classic(
                state,
                self.0
                    .historical_transition_parameters()
                    .ok_or_else(|| context("historical transition parameters unavailable"))?,
            ))
        } else {
            Err(StoreError::UnknownRoot.into())
        }
    }
    fn block_slot(&self, root: Root) -> Option<u64> {
        self.0
            .block(root)
            .map(|block| block.slot)
            .or_else(|| self.0.historical_block(root).map(block_slot))
    }
    fn has_state(&self, root: Root) -> Result<bool, AttestationGossipError> {
        Ok(self.0.read_block_state(root)?.is_some() ||
            self.0.read_historical_block_state(root)?.is_some())
    }
    fn checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, AttestationGossipError> {
        let slot = epoch.checked_mul(self.0.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        Ok(self.0.get_ancestor(self.0.node_for_root(root)?, slot)?.root)
    }
    fn finalized(&self) -> Checkpoint {
        self.0.finalized_checkpoint()
    }
    fn binding(&self) -> Result<Binding, AttestationGossipError> {
        let binding = Binding {
            network: self.head()?.fields().network,
            genesis_ms: self.0.genesis_time_ms(),
            slot_ms: self.0.slot_duration_ms(),
            slots_per_epoch: self.0.slots_per_epoch(),
            store: self.0.store_parameters(),
            configuration: if let Some(parameters) = self.0.historical_transition_parameters() {
                parameters.validate()?;
                if parameters.gloas != *self.0.processing_parameters() {
                    return Err(context("historical and Gloas processing identities differ"))
                }
                Configuration::Classic(parameters.clone())
            } else {
                Configuration::Gloas(self.0.processing_parameters().clone())
            },
        };
        checked_clock_binding(&binding)?;
        Ok(binding)
    }
    fn check_payload(&self, data: &AttestationData) -> Result<(), AttestationGossipError> {
        if self.block_slot(data.beacon_block_root) == Some(data.slot) && data.index != 0 {
            return Err(AttestationGossipError::Reject(
                "same-slot attestation must attest with index 0",
            ))
        }
        if data.index != 1 {
            return Ok(())
        }
        if self.0.historical_block(data.beacon_block_root).is_some() {
            return Err(AttestationGossipError::Reject(
                "historical block has no Gloas payload branch",
            ))
        }
        if self.0.payload(data.beacon_block_root).is_none() {
            return Err(AttestationGossipError::Ignore(
                "execution payload envelope has not been seen",
            ))
        }
        match self.0.execution_status(data.beacon_block_root) {
            ExecutionStatus::Valid => Ok(()),
            ExecutionStatus::Optimistic => {
                Err(AttestationGossipError::Ignore("attested payload is optimistic"))
            }
            ExecutionStatus::Invalid => {
                Err(AttestationGossipError::Reject("attested payload is invalid"))
            }
        }
    }
}

fn checked_historical_wire(
    view: &impl ReadGloasGossipView,
    slot: u64,
    fork: Fork,
) -> Result<(), AttestationGossipError> {
    let parameters = view
        .historical_transition_parameters()
        .ok_or_else(|| context("authenticated earlier-fork gossip context unavailable"))?;
    if fork >= Fork::Gloas || parameters.fork_at_slot(slot)? != fork {
        return Err(AttestationGossipError::Reject("wire schema differs from attestation slot fork"))
    }
    Ok(())
}
fn context(reason: &'static str) -> AttestationGossipError {
    ProcessingError::InvalidState(reason).into()
}
fn checked_clock_binding(binding: &Binding) -> Result<(), AttestationGossipError> {
    let expected_slots = match binding.configuration.preset() {
        Preset::Mainnet => 32,
        Preset::Minimal => 8,
    };
    if binding.slot_ms == 0 ||
        binding.slots_per_epoch != expected_slots ||
        binding.slot_ms != binding.store.slot_duration_ms
    {
        return Err(context("attestation gossip clock or preset differs from store policy"))
    }
    Ok(())
}
fn checked_vote_head<'a>(
    view: &'a impl View,
    root: Root,
) -> Result<Head<'a>, AttestationGossipError> {
    if view.block_slot(root).is_none() {
        return Err(AttestationGossipError::Ignore("block being voted for has not been seen"))
    }
    if !view.has_state(root)? {
        return Err(AttestationGossipError::Reject("block being voted for failed validation"))
    }
    view.head()
}
fn committee_count(head: &Head<'_>, epoch: u64, slots: u64) -> u64 {
    let active = head
        .fields()
        .validators
        .iter()
        .filter(|v| v.activation_epoch <= epoch && epoch < v.exit_epoch)
        .count() as u64;
    let (maximum, target) = match head.preset() {
        Preset::Mainnet => (64, 128),
        Preset::Minimal => (4, 4),
    };
    (active / slots / target).min(maximum).max(1)
}
const fn checked_committee_index(index: u64, count: u64) -> Result<(), AttestationGossipError> {
    if index >= count {
        return Err(AttestationGossipError::Reject("committee index out of range"))
    }
    Ok(())
}
const fn checked_subnet(
    count: u64,
    slot: u64,
    index: u64,
    slots: u64,
    subnet: u64,
) -> Result<(), AttestationGossipError> {
    if (count * (slot % slots) + index) % 64 != subnet {
        return Err(AttestationGossipError::Reject("attestation is for wrong subnet"))
    }
    Ok(())
}
fn checked_time(
    binding: &Binding,
    slot: u64,
    time: u64,
    fork: Fork,
    aggregate: bool,
) -> Result<(), AttestationGossipError> {
    let start = u128::from(binding.genesis_ms) + u128::from(slot) * u128::from(binding.slot_ms);
    let time = u128::from(time);
    if fork < Fork::Deneb {
        let end = start + 33 * u128::from(binding.slot_ms);
        if time + 500 < start || time > end + 500 {
            return Err(AttestationGossipError::Ignore(
                "attestation slot not within propagation range",
            ))
        }
    } else {
        if time + 500 < start {
            return Err(AttestationGossipError::Ignore(if aggregate {
                "aggregate slot is from a future slot"
            } else {
                "attestation slot is from a future slot"
            }))
        }
        let epoch = u128::from(slot / binding.slots_per_epoch);
        let epoch_start = u128::from(binding.genesis_ms) +
            epoch * u128::from(binding.slots_per_epoch) * u128::from(binding.slot_ms);
        let epoch_end =
            epoch_start + 2 * u128::from(binding.slots_per_epoch) * u128::from(binding.slot_ms);
        if time + 500 < epoch_start || time > epoch_end + 500 {
            return Err(AttestationGossipError::Ignore(if aggregate {
                "aggregate epoch is not current or previous epoch"
            } else {
                "attestation epoch is not current or previous epoch"
            }))
        }
    }
    Ok(())
}
const fn checked_target_epoch(
    data: &AttestationData,
    slots: u64,
) -> Result<(), AttestationGossipError> {
    if data.target.epoch != data.slot / slots {
        return Err(AttestationGossipError::Reject("attestation epoch does not match target epoch"))
    }
    Ok(())
}
const fn checked_bit_length(bits: &BitList, count: usize) -> Result<(), AttestationGossipError> {
    if bits.len() != count {
        return Err(AttestationGossipError::Reject(
            "aggregation bits length does not match committee size",
        ))
    }
    Ok(())
}
fn checked_signature(
    head: &Head<'_>,
    data: &AttestationData,
    participants: &[u64],
    signature: &[u8; 96],
    reason: &'static str,
) -> Result<(), AttestationGossipError> {
    let fields = head.fields();
    let keys = participants
        .iter()
        .map(|&index| fields.validator(index).map(|v| v.pubkey))
        .collect::<Result<Vec<_>, _>>()?;
    let signing_root = compute_signing_root(
        data.hash_tree_root(head.preset())?,
        fields.domain([1, 0, 0, 0], data.target.epoch),
    );
    fast_aggregate_verify(&keys, &signing_root, signature)
        .map_err(|_| AttestationGossipError::Reject(reason))?;
    Ok(())
}
fn checked_ancestry(
    view: &impl View,
    data: &AttestationData,
) -> Result<(), AttestationGossipError> {
    if view.checkpoint_block(data.beacon_block_root, data.target.epoch)? != data.target.root {
        return Err(AttestationGossipError::Reject(
            "target block is not an ancestor of LMD vote block",
        ))
    }
    let finalized = view.finalized();
    if view.checkpoint_block(data.beacon_block_root, finalized.epoch)? != finalized.root {
        return Err(AttestationGossipError::Ignore(
            "finalized checkpoint is not an ancestor of block",
        ))
    }
    Ok(())
}

/// Borrow the exact decoded fork-specific wire container without conversion or copying.
#[derive(Clone, Copy)]
pub enum LegacyAttestation<'a> {
    /// Complete Phase0 wire message.
    Phase0(&'a ssz::phase0::Attestation),
    /// Complete Altair wire message.
    Altair(&'a ssz::altair::Attestation),
    /// Complete Bellatrix wire message.
    Bellatrix(&'a ssz::bellatrix::Attestation),
    /// Complete Capella wire message.
    Capella(&'a ssz::capella::Attestation),
    /// Complete Deneb wire message.
    Deneb(&'a ssz::deneb::Attestation),
}
impl fmt::Debug for LegacyAttestation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LegacyAttestation").field("fork", &self.fork()).finish_non_exhaustive()
    }
}
impl<'a> LegacyAttestation<'a> {
    const fn fork(self) -> Fork {
        match self {
            Self::Phase0(_) => Fork::Phase0,
            Self::Altair(_) => Fork::Altair,
            Self::Bellatrix(_) => Fork::Bellatrix,
            Self::Capella(_) => Fork::Capella,
            Self::Deneb(_) => Fork::Deneb,
        }
    }
    const fn fields(self) -> LegacyFields<'a> {
        match self {
            Self::Phase0(value) => LegacyFields {
                data: &value.data,
                bits: &value.aggregation_bits.bits,
                signature: &value.signature,
            },
            Self::Altair(value) => LegacyFields {
                data: &value.data,
                bits: &value.aggregation_bits.bits,
                signature: &value.signature,
            },
            Self::Bellatrix(value) => LegacyFields {
                data: &value.data,
                bits: &value.aggregation_bits.bits,
                signature: &value.signature,
            },
            Self::Capella(value) => LegacyFields {
                data: &value.data,
                bits: &value.aggregation_bits.bits,
                signature: &value.signature,
            },
            Self::Deneb(value) => LegacyFields {
                data: &value.data,
                bits: &value.aggregation_bits.bits,
                signature: &value.signature,
            },
        }
    }
}

/// Borrow the exact decoded fork-specific wire container without conversion or copying.
#[derive(Clone, Copy)]
pub enum SingleAttestation<'a> {
    /// Complete Electra wire message.
    Electra(&'a auxiliary::electra::SingleAttestation),
    /// Complete Fulu wire message.
    Fulu(&'a auxiliary::fulu::SingleAttestation),
}
impl fmt::Debug for SingleAttestation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleAttestation").field("fork", &self.fork()).finish_non_exhaustive()
    }
}
impl<'a> SingleAttestation<'a> {
    const fn fork(self) -> Fork {
        match self {
            Self::Electra(_) => Fork::Electra,
            Self::Fulu(_) => Fork::Fulu,
        }
    }
    const fn fields(self) -> SingleFields<'a> {
        match self {
            Self::Electra(value) => SingleFields {
                data: &value.data,
                committee: value.committee_index,
                participant: value.attester_index,
                signature: &value.signature,
            },
            Self::Fulu(value) => SingleFields {
                data: &value.data,
                committee: value.committee_index,
                participant: value.attester_index,
                signature: &value.signature,
            },
        }
    }
}

/// Borrow the exact decoded fork-specific wire container without conversion or copying.
#[derive(Clone, Copy)]
pub enum SignedAggregate<'a> {
    /// Complete Phase0 wire message.
    Phase0(&'a auxiliary::phase0::SignedAggregateAndProof),
    /// Complete Altair wire message.
    Altair(&'a auxiliary::altair::SignedAggregateAndProof),
    /// Complete Bellatrix wire message.
    Bellatrix(&'a auxiliary::bellatrix::SignedAggregateAndProof),
    /// Complete Capella wire message.
    Capella(&'a auxiliary::capella::SignedAggregateAndProof),
    /// Complete Deneb wire message.
    Deneb(&'a auxiliary::deneb::SignedAggregateAndProof),
    /// Complete Electra wire message.
    Electra(&'a auxiliary::electra::SignedAggregateAndProof),
    /// Complete Fulu wire message.
    Fulu(&'a auxiliary::fulu::SignedAggregateAndProof),
}
impl fmt::Debug for SignedAggregate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignedAggregate").field("fork", &self.fork()).finish_non_exhaustive()
    }
}
impl<'a> SignedAggregate<'a> {
    const fn fork(self) -> Fork {
        match self {
            Self::Phase0(_) => Fork::Phase0,
            Self::Altair(_) => Fork::Altair,
            Self::Bellatrix(_) => Fork::Bellatrix,
            Self::Capella(_) => Fork::Capella,
            Self::Deneb(_) => Fork::Deneb,
            Self::Electra(_) => Fork::Electra,
            Self::Fulu(_) => Fork::Fulu,
        }
    }
    const fn fields(self) -> AggregateFields<'a> {
        match self {
            Self::Phase0(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: None,
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Altair(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: None,
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Bellatrix(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: None,
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Capella(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: None,
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Deneb(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: None,
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Electra(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: Some(&value.message.aggregate.committee_bits.bits),
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
            Self::Fulu(value) => AggregateFields {
                data: &value.message.aggregate.data,
                bits: &value.message.aggregate.aggregation_bits.bits,
                committee_bits: Some(&value.message.aggregate.committee_bits.bits),
                aggregator: value.message.aggregator_index,
                selection_proof: &value.message.selection_proof,
                aggregate_signature: &value.message.aggregate.signature,
                outer_signature: &value.signature,
            },
        }
    }
    fn message_root(self, preset: Preset) -> Result<Root, SszError> {
        match self {
            Self::Phase0(value) => value.message.hash_tree_root(preset),
            Self::Altair(value) => value.message.hash_tree_root(preset),
            Self::Bellatrix(value) => value.message.hash_tree_root(preset),
            Self::Capella(value) => value.message.hash_tree_root(preset),
            Self::Deneb(value) => value.message.hash_tree_root(preset),
            Self::Electra(value) => value.message.hash_tree_root(preset),
            Self::Fulu(value) => value.message.hash_tree_root(preset),
        }
    }
}

impl Head<'_> {
    const fn preset(&self) -> Preset {
        match self {
            Self::Classic(_, p) => p.preset(),
            Self::Gloas(_, p) => p.preset(),
        }
    }
    fn fields(&self) -> Fields<'_> {
        match self {
            Self::Classic(state, _) => Fields {
                validators: StateRef::Classic(state.as_ref()).validators(),
                fork: state.fork_info(),
                network: state.genesis_validators_root(),
            },
            Self::Gloas(state, _) => Fields {
                validators: state.validators.values.as_slice(),
                fork: state.fork.clone(),
                network: state.genesis_validators_root,
            },
        }
    }
    fn committee(&self, slot: u64, index: u64) -> Result<Vec<u64>, AttestationGossipError> {
        Ok(match self {
            Self::Classic(state, p) => match state.as_ref() {
                BeaconState::Phase0(value, _) => phase0_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
                )?,
                BeaconState::Altair(value, _) => altair_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra.deneb.capella.bellatrix.altair,
                )?,
                BeaconState::Bellatrix(value, _) => bellatrix_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra.deneb.capella.bellatrix,
                )?,
                BeaconState::Capella(value, _) => capella_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra.deneb.capella,
                )?,
                BeaconState::Deneb(value, _) => deneb_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra.deneb,
                )?,
                BeaconState::Electra(value, _) => electra_processing::get_beacon_committee(
                    value,
                    slot,
                    index,
                    &p.gloas.fulu.electra,
                )?,
                BeaconState::Fulu(value, _) => {
                    fulu_processing::get_beacon_committee(value, slot, index, &p.gloas.fulu)?
                }
                BeaconState::Gloas(value, _) => {
                    gloas_processing::get_beacon_committee(value, slot, index, &p.gloas)?
                }
            },
            Self::Gloas(value, p) => {
                gloas_processing::get_beacon_committee(value.as_ref(), slot, index, p)?
            }
        })
    }
}

const fn block_slot(block: &ssz::BeaconBlock) -> u64 {
    match block {
        ssz::BeaconBlock::Phase0(value, _) => value.slot,
        ssz::BeaconBlock::Altair(value, _) => value.slot,
        ssz::BeaconBlock::Bellatrix(value, _) => value.slot,
        ssz::BeaconBlock::Capella(value, _) => value.slot,
        ssz::BeaconBlock::Deneb(value, _) => value.slot,
        ssz::BeaconBlock::Electra(value, _) => value.slot,
        ssz::BeaconBlock::Fulu(value, _) => value.slot,
        ssz::BeaconBlock::Gloas(value, _) => value.slot,
    }
}

#[cfg(test)]
mod tests;
