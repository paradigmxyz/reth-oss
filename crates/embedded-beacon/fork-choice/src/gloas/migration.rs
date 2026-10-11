//! Authenticated fork-boundary import retaining genuine earlier schemas and vote provenance.

use super::*;
use crate::{compact::GraphStateRead, fast_confirmation::StateRef, Store};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::{
    fork_processing,
    ssz::{self, fulu},
};

impl GloasStore {
    /// Import the first Gloas block through the exact attached fork schedule.
    ///
    /// Earlier blocks and states retain their actual schemas and shared allocations. No
    /// envelope, execution verdict, vote slot, checkpoint or replacement anchor is invented.
    /// Any failure leaves the borrowed earlier store unchanged.
    pub async fn migrate_from_classic<E: fork_processing::ExecutionEngine>(
        previous: &Store,
        signed: &ssz::SignedBeaconBlock,
        engine: &E,
    ) -> Result<Self, StoreError> {
        let parameters = previous
            .transition_parameters()
            .ok_or(StoreError::ValidationUnavailable(Fork::Gloas))?;
        parameters.validate()?;
        let ssz::SignedBeaconBlock::Gloas(signed_gloas, preset) = signed else {
            return Err(StoreError::ValidationUnavailable(signed.schema_fork()))
        };
        if *preset != parameters.preset() ||
            parameters.fork_at_slot(signed_gloas.message.slot)? != Fork::Gloas
        {
            return Err(StoreError::AnchorMismatch)
        }
        let parent_root = signed_gloas.message.parent_root;
        let parent = previous.read_block_state(parent_root)?.ok_or(StoreError::UnknownRoot)?;
        if parent.schema_fork() >= Fork::Gloas {
            return Err(StoreError::AnchorMismatch)
        }
        previous.verified_execution_block_hash(parent_root)?;
        let (justified, finalized, unrealized_justified, unrealized_finalized) =
            previous.migration_checkpoints();
        let mut history = previous.clone();
        for checkpoint in [justified, finalized, unrealized_justified, unrealized_finalized] {
            history.ensure_migration_checkpoint(checkpoint)?;
        }
        let latest_messages = previous
            .migration_votes()?
            .into_iter()
            .map(|(index, slot, root)| {
                (index, LatestMessage { slot, root, payload_present: false })
            })
            .collect();
        let mut candidate = Self {
            execution_query_anchor: None,
            historical: Some(Arc::new(history)),
            blocks: BTreeMap::new(),
            block_states: BTreeMap::new(),
            checkpoint_states: BTreeMap::new(),
            block_timeliness: previous.migration_timeliness(),
            unrealized_justifications: previous.migration_unrealized(),
            payloads: BTreeMap::new(),
            timeliness_votes: BTreeMap::new(),
            availability_votes: BTreeMap::new(),
            latest_messages,
            equivocating: previous.migration_equivocations(),
            justified,
            finalized,
            unrealized_justified,
            unrealized_finalized,
            proposer_boost_root: previous.proposer_boost_root(),
            time_ms: previous.time_ms(),
            genesis_time_ms: previous.genesis_time_ms(),
            parameters: previous.migration_parameters(),
            processing: parameters.gloas.clone(),
        };
        candidate.check_budget()?;
        candidate.check_block_time_and_ancestry(&signed_gloas.message, parent.slot())?;
        let state = fork_processing::state_transition_owned(
            parent.as_ref().clone(),
            signed,
            parameters,
            engine,
        )
        .await
        .map_err(|error| StoreError::ExecutionImport(error.to_string()))?;
        let ssz::BeaconState::Gloas(state, _) = state else {
            return Err(StoreError::AnchorMismatch)
        };
        candidate.finish_block::<true>(signed_gloas, state)?;
        Ok(candidate)
    }

    pub(super) fn block_count(&self) -> usize {
        self.blocks.len() +
            self.historical.as_ref().map_or(0, |history| history.migration_roots().count())
    }

    pub(super) fn block_header(&self, root: Root) -> Result<BlockMetadata, StoreError> {
        if let Some(block) = self.blocks.get(&root) {
            return Ok(BlockMetadata {
                slot: block.slot,
                proposer_index: block.proposer_index,
                parent_root: block.parent_root,
            })
        }
        let block = self
            .historical
            .as_ref()
            .and_then(|history| history.block(root))
            .ok_or(StoreError::UnknownRoot)?;
        macro_rules! metadata {
            ($block:expr) => {
                BlockMetadata {
                    slot: $block.slot,
                    proposer_index: $block.proposer_index,
                    parent_root: $block.parent_root,
                }
            };
        }
        Ok(match block {
            ssz::BeaconBlock::Phase0(block, _) => metadata!(block),
            ssz::BeaconBlock::Altair(block, _) => metadata!(block),
            ssz::BeaconBlock::Bellatrix(block, _) => metadata!(block),
            ssz::BeaconBlock::Capella(block, _) => metadata!(block),
            ssz::BeaconBlock::Deneb(block, _) => metadata!(block),
            ssz::BeaconBlock::Electra(block, _) => metadata!(block),
            ssz::BeaconBlock::Fulu(block, _) => metadata!(block),
            ssz::BeaconBlock::Gloas(..) => return Err(StoreError::AnchorMismatch),
        })
    }

    pub(super) fn root_node(
        &self,
        root: Root,
        status: PayloadStatus,
    ) -> Result<ForkChoiceNode, StoreError> {
        self.block_header(root)?;
        Ok(ForkChoiceNode {
            root,
            payload_status: self.blocks.contains_key(&root).then_some(status),
        })
    }

    pub(super) fn parent_node(&self, block: &BeaconBlock) -> Result<ForkChoiceNode, StoreError> {
        if self.blocks.contains_key(&block.parent_root) {
            return Ok(node(block.parent_root, self.parent_payload_status(block)?))
        }
        self.root_node(block.parent_root, PayloadStatus::Pending)
    }

    pub(super) fn state_view(&self, root: Root) -> Result<GraphStateRead, StoreError> {
        if let Some(state) = self.read_block_state(root)? {
            return Ok(GraphStateRead::Gloas(state))
        }
        self.historical
            .as_ref()
            .ok_or(StoreError::UnknownRoot)?
            .read_block_state(root)?
            .map(GraphStateRead::Classic)
            .ok_or(StoreError::UnknownRoot)
    }

    pub(super) fn checkpoint_view(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<GraphStateRead, StoreError> {
        if let Some(state) = self.checkpoint_states.get(&(checkpoint.epoch, checkpoint.root)) {
            return Ok(GraphStateRead::Gloas(state.acquire(self.processing.preset())?))
        }
        self.historical
            .as_ref()
            .ok_or(StoreError::UnknownRoot)?
            .read_checkpoint_state(checkpoint)?
            .map(GraphStateRead::Classic)
            .ok_or(StoreError::UnknownRoot)
    }

    pub(super) fn ensure_checkpoint<const VERIFY_SIGNATURES: bool>(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        if self.blocks.contains_key(&checkpoint.root) {
            self.checkpoint_state::<VERIFY_SIGNATURES>(checkpoint)?;
        } else {
            // Historical migration is strictly authenticated; fixture constructors never
            // create historical production graphs.
            policy::require::<VERIFY_SIGNATURES>()?;
            let history = self.historical.as_mut().ok_or(StoreError::UnknownRoot)?;
            Arc::make_mut(history).ensure_migration_checkpoint(checkpoint)?;
            self.check_budget()?;
        }
        Ok(())
    }

    pub(super) fn check_block_time_and_ancestry(
        &self,
        block: &BeaconBlock,
        parent_slot: u64,
    ) -> Result<(), StoreError> {
        let finalized_slot =
            self.finalized.epoch.checked_mul(self.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        if block.slot > self.current_slot() || block.slot <= finalized_slot {
            return Err(StoreError::InvalidBlockTime)
        }
        if self.get_checkpoint_block(block.parent_root, self.finalized.epoch)? !=
            self.finalized.root
        {
            return Err(StoreError::ConflictsWithFinality)
        }
        if self.block_count() >= self.parameters.max_blocks ||
            block.slot.saturating_sub(parent_slot) > self.parameters.max_tick_slots
        {
            return Err(StoreError::ResourceLimit)
        }
        Ok(())
    }

    pub(super) fn historical_head_weak(
        &self,
        root: Root,
        mut weight: u64,
    ) -> Result<bool, StoreError> {
        let history = self.historical.as_ref().ok_or(StoreError::UnknownRoot)?;
        let block = self.block_header(root)?;
        let head = self.state_view(root)?;
        let epoch = block.slot / self.slots_per_epoch();
        let active = head
            .validators()
            .iter()
            .filter(|validator| validator.activation_epoch <= epoch && epoch < validator.exit_epoch)
            .count() as u64;
        let (target, max) = match self.processing.preset() {
            Preset::Mainnet => (128, 64),
            Preset::Minimal => (4, 4),
        };
        let committees = (active / self.slots_per_epoch() / target).clamp(1, max);
        let justified = self.justified_state()?;
        for index in 0..committees {
            for validator in history.migration_committee(root, block.slot, index)? {
                if self.equivocating.contains(&validator) {
                    let validator = usize::try_from(validator)
                        .ok()
                        .and_then(|index| justified.validators().get(index))
                        .ok_or(StoreError::InvalidAttestationTarget)?;
                    weight = weight
                        .checked_add(validator.effective_balance)
                        .ok_or(StoreError::Overflow)?;
                }
            }
        }
        Ok(weight < self.committee_fraction(self.parameters.reorg_head_weight_threshold)?)
    }

    pub(super) fn retained_roots(&self) -> impl Iterator<Item = Root> + '_ {
        self.historical
            .iter()
            .flat_map(|history| history.migration_roots())
            .chain(self.blocks.keys().copied())
    }

    pub(super) fn validate_checkpoint_attestation<const VERIFY_SIGNATURES: bool>(
        &self,
        checkpoint: Checkpoint,
        attestation: &gloas::Attestation,
    ) -> Result<Vec<u64>, StoreError> {
        let checkpoint_state = self.checkpoint_view(checkpoint)?;
        match checkpoint_state.view() {
            StateRef::Gloas(state, _) | StateRef::Classic(ssz::BeaconState::Gloas(state, _)) => {
                let indexed = policy::validated_attestation::<VERIFY_SIGNATURES>(
                    state,
                    attestation,
                    &self.processing,
                )?;
                Ok(indexed.attesting_indices.values)
            }
            StateRef::Classic(ssz::BeaconState::Fulu(state, _)) => {
                let wire = fulu::Attestation {
                    aggregation_bits: fulu::AggregationBits::from_ssz(
                        &attestation.aggregation_bits.to_ssz(self.processing.preset())?,
                        self.processing.preset(),
                    )?,
                    data: attestation.data.clone(),
                    signature: attestation.signature,
                    committee_bits: fulu::CommitteeBits::from_ssz(
                        &attestation.committee_bits.to_ssz(self.processing.preset())?,
                        self.processing.preset(),
                    )?,
                };
                // Keep the original Fulu registry, committee derivation and signed domain.
                // Migration never converts a retained historical state to a Gloas state.
                let indexed = reth_embedded_beacon_state_transition::fulu_processing::get_validated_indexed_attestation(
                    state, &wire, &self.processing.fulu,
                )?;
                Ok(indexed.attesting_indices.values)
            }
            _ => Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        }
    }

    pub(super) fn validate_historical_indexed(
        &self,
        indexed: &gloas::IndexedAttestation,
    ) -> Result<(), StoreError> {
        let state = self.justified_state()?;
        let (fork, genesis) = match state.view() {
            StateRef::Classic(ssz::BeaconState::Phase0(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Altair(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Bellatrix(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Capella(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Deneb(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Electra(s, _)) => {
                (&s.fork, s.genesis_validators_root)
            }
            StateRef::Classic(ssz::BeaconState::Fulu(s, _)) => (&s.fork, s.genesis_validators_root),
            StateRef::Classic(ssz::BeaconState::Gloas(s, _)) | StateRef::Gloas(s, _) => {
                (&s.fork, s.genesis_validators_root)
            }
        };
        let indices = &indexed.attesting_indices.values;
        let maximum = match self.processing.preset() {
            Preset::Mainnet => 131_072,
            Preset::Minimal => 8_192,
        };
        if indices.is_empty() ||
            indices.len() > maximum ||
            indices.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreError::InvalidSlashing)
        }
        let keys = indices
            .iter()
            .map(|&index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|index| state.validators().get(index))
                    .map(|validator| validator.pubkey)
                    .ok_or(StoreError::InvalidSlashing)
            })
            .collect::<Result<Vec<_>, _>>()?;
        // A retained justified state can precede the signed target epoch's fork.
        // Select the domain only from the privately attached authenticated schedule.
        let version = if let Some(parameters) =
            self.historical.as_ref().and_then(|history| history.transition_parameters())
        {
            parameters.validate()?;
            parameters
                .schedule
                .iter()
                .rev()
                .find(|activation| {
                    activation.epoch != u64::MAX && activation.epoch <= indexed.data.target.epoch
                })
                .ok_or(StoreError::ValidationUnavailable(Fork::Gloas))?
                .version
        } else if indexed.data.target.epoch < fork.epoch {
            fork.previous_version
        } else {
            fork.current_version
        };
        let root = compute_signing_root(
            indexed.data.hash_tree_root(self.processing.preset())?,
            compute_domain([1, 0, 0, 0], version, genesis),
        );
        fast_aggregate_verify(&keys, &root, &indexed.signature).map_err(ProcessingError::from)?;
        Ok(())
    }
}

/// Immutable structural ancestry metadata; it does not confer signature or execution authority.
pub(super) struct BlockMetadata {
    pub(super) slot: u64,
    pub(super) proposer_index: u64,
    pub(super) parent_root: Root,
}

#[cfg(test)]
mod tests;
