//! Complete signed equivocation witnesses survive retirement of their original competing block.
//! Saved rows contain exact SSZ, never validator flags or fork-choice weights.
use super::*;
use crate::compact_persistence::events::{DurableEvent, EventAction};
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind};

pub(super) fn original(signed: &SignedBeaconBlock, index: usize) -> Result<Vec<u8>, ImportError> {
    macro_rules! encode {
        ($block:ident, $preset:ident) => {
            $block
                .message
                .body
                .attester_slashings
                .values
                .get(index)
                .ok_or(ImportError::RecoveryRequired)?
                .to_ssz(*$preset)
                .map_err(|_| ImportError::Wire)
        };
    }
    match signed {
        SignedBeaconBlock::Phase0(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Altair(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Bellatrix(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Capella(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Deneb(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Electra(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Fulu(block, preset) => encode!(block, preset),
        SignedBeaconBlock::Gloas(block, preset) => encode!(block, preset),
    }
}
impl<E, C, G> Worker<E, C, G>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
    C: ClassicAvailability,
    G: GloasAvailability,
{
    pub(super) fn record_signed_slashing(
        &self,
        fork: Fork,
        bytes: &[u8],
        context: reth_embedded_beacon_fork_choice::slashing_replay::SlashingContext,
        received_ms: u64,
        candidate: &Graph,
    ) -> Result<(), ImportError> {
        if reth_embedded_beacon_fork_choice::slashing_replay::original_participants(
            bytes,
            fork,
            preset(&self.config),
        )?
        .is_empty()
        {
            return Ok(())
        }
        self.record_event(
            EventAction::Slashing {
                fork,
                verification_slot: context.verification_slot(),
                original: ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, bytes),
            },
            received_ms,
            candidate.time_ms(),
            candidate,
            Some(bytes),
        )
    }
    pub(super) fn apply_original_slashing(
        &mut self,
        event: &DurableEvent,
        fork: Fork,
    ) -> Result<(), ImportError> {
        let bytes = self
            .repository
            .original_observation(event)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let parameters = RuntimeParameters::from_config(&self.config)
            .map_err(|_| ImportError::Configuration)?
            .transitions;
        let EventAction::Slashing { verification_slot, .. } = event.action else {
            return Err(ImportError::RecoveryRequired)
        };
        let mut candidate = self.graph.clone();
        match &mut candidate {
            Graph::Classic { store, .. } => {
                store.on_original_attester_slashing(&bytes, fork, &parameters, verification_slot)?
            }
            Graph::Gloas { store, .. } => {
                store.on_original_attester_slashing(&bytes, fork, &parameters, verification_slot)?
            }
        }
        self.record_event(
            event.action.clone(),
            event.received_ms,
            event.begin_ms,
            &candidate,
            None,
        )?;
        self.graph = candidate;
        Ok(())
    }
}
