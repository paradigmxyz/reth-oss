//! Native gossip validation and independently transactional fork-choice votes.
//!
//! Accepted observations publish only after actual native validation and canonical graph
//! publication. Missing dependencies and local infrastructure never become invalid-peer caches.

use super::*;
use crate::{
    attestation_gossip::{AttestationGossipError, SeenAttestations, SignedAggregate},
    gloas_bids::{BidGossipError, SeenBids},
    gloas_payload_attestations::{PayloadAttestationGossipError, SeenPayloadAttestations},
};
use reth_embedded_beacon_fork_choice::store::block_gossip::{
    self, GossipBlockError, SeenBeaconBlocks,
};
use reth_embedded_beacon_network::{
    gossip::{parse_topic, GossipKind, GossipTopic},
    transport::GossipVerdict,
};
use reth_embedded_beacon_state_transition::ssz::auxiliary;
use std::collections::BTreeMap;

mod columns;
use columns::ColumnIdentity;
pub(crate) use columns::{ColumnRetention, ValidatedColumn};

#[derive(Clone)]
pub(super) struct GossipState {
    attestations: SeenAttestations,
    bids: SeenBids,
    payload_attestations: SeenPayloadAttestations,
    blocks: Option<(bool, SeenBeaconBlocks)>,
    columns: BTreeMap<ColumnIdentity, u64>,
}
impl GossipState {
    pub(super) fn new() -> Result<Self, ImportError> {
        Ok(Self {
            attestations: SeenAttestations::new(1_048_576, 16 * 1024 * 1024)
                .map_err(|_| ImportError::Bounds)?,
            bids: SeenBids::new(16384, 8192).map_err(|_| ImportError::Bounds)?,
            blocks: None,
            columns: BTreeMap::new(),
            payload_attestations: SeenPayloadAttestations::new(16384)
                .map_err(|_| ImportError::Bounds)?,
        })
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
    pub(super) fn validate_block_gossip(
        &self,
        wire: &WireBlock,
        time: u64,
    ) -> Result<GossipState, ImportError> {
        let signed = decode_block(wire, &self.config)?;
        let mut prepared = self.gossip_state.clone();
        let is_gloas = matches!(&self.graph, Graph::Gloas { .. });
        if prepared.blocks.as_ref().is_none_or(|(mode, _)| *mode != is_gloas) {
            let seen = match &self.graph {
                Graph::Classic { store, .. } => SeenBeaconBlocks::new(store, 16384),
                Graph::Gloas { store, .. } => SeenBeaconBlocks::new_gloas(store, 16384),
            }
            .map_err(block_error)?;
            prepared.blocks = Some((is_gloas, seen));
        }
        let seen = &mut prepared.blocks.as_mut().ok_or(ImportError::Bounds)?.1;
        seen.prune_before_slot(
            self.graph.current_slot().saturating_sub(2 * self.config.slots_per_epoch),
        );
        match (&self.graph, &signed) {
            (Graph::Classic { store, .. }, signed) if signed.schema_fork() < Fork::Gloas => {
                block_gossip::validate(store, seen, signed, time).map_err(block_error)?
            }
            (Graph::Gloas { store, .. }, SignedBeaconBlock::Gloas(signed, _)) => {
                block_gossip::validate_gloas(store, seen, signed, time).map_err(block_error)?
            }
            // The first Gloas block requires the strict migration importer before the new
            // graph can answer Gloas gossip queries. Retrieval, rather than propagation,
            // performs this authenticated migration.
            _ => return Err(ImportError::GossipIgnore("gossip graph schema not yet available")),
        }
        Ok(prepared)
    }
    pub(super) fn validate_envelope_gossip(
        &self,
        wire: &WireEnvelope,
        _time: u64,
    ) -> Result<GossipState, ImportError> {
        let Graph::Gloas { store, .. } = &self.graph else {
            return Err(ImportError::GossipIgnore("Gloas graph not yet available"))
        };
        let signed = decode_envelope(wire, store, &self.config)?;
        let mut prepared = self.gossip_state.clone();
        prepared.bids.validate_envelope(store, &signed).map_err(|error| match error {
            BidGossipError::Ignore(reason) => ImportError::GossipIgnore(reason),
            BidGossipError::Reject(reason) => ImportError::GossipReject(reason),
            BidGossipError::ResourceLimit => {
                ImportError::GossipIgnore("local gossip resource budget")
            }
            BidGossipError::Store(error) => error.into(),
            other => ImportError::PublicationInfrastructure(other.to_string()),
        })?;
        Ok(prepared)
    }

    /// Seed optional bid prerequisites only from the actual strictly admitted native graph.
    /// Cache pressure never changes a completed canonical publication into consensus failure.
    pub(super) fn record_admitted_envelope(
        &self,
        mut prepared: GossipState,
        root: Root,
    ) -> GossipState {
        if let Graph::Gloas { store, .. } = &self.graph {
            prepared.bids.retain_from_slot(
                self.graph.current_slot().saturating_sub(2 * self.config.slots_per_epoch),
            );
            if let Err(error) = prepared.bids.record_verified_payload(store, root) {
                tracing::debug!(target: "beacon::gossip", root = %B256::from(root), %error,
                    "[beacon] Verified payload metadata cache unavailable after canonical admission");
            }
        }
        prepared
    }
    pub(super) fn dispatch_gossip(
        &mut self,
        topic: GossipTopic,
        bytes: &[u8],
        time: u64,
    ) -> Result<GossipVerdict, ImportError> {
        if parse_topic(&self.config, &topic.name()).ok() != Some(topic) ||
            bytes.len() > self.config.max_payload_size
        {
            return Ok(GossipVerdict::Reject)
        }
        let preset = match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let mut observations = self.gossip_state.clone();
        let epoch = self.graph.current_slot() / self.config.slots_per_epoch;
        observations.attestations.retain_from_epoch(epoch.saturating_sub(2));
        observations.bids.retain_from_slot(epoch.saturating_sub(2) * self.config.slots_per_epoch);
        observations
            .payload_attestations
            .retain_from_slot(epoch.saturating_sub(2) * self.config.slots_per_epoch);
        observations
            .columns
            .retain(|_, slot| *slot >= epoch.saturating_sub(2) * self.config.slots_per_epoch);
        let mut candidate = self.graph.clone();
        let result = if matches!(topic.kind, GossipKind::Column(_)) {
            self.validate_column_gossip(&mut observations, topic, bytes, time).and_then(|receipt| {
                if let Some(publisher) = &self.column_retention {
                    publisher.retain(&receipt).map_err(Decision::Infrastructure)?;
                }
                Ok(false)
            })
        } else {
            validate(&mut observations, &mut candidate, &self.config, topic, bytes, preset, time)
        };
        match result {
            Ok(votes) => {
                if votes {
                    self.record_gossip_event(topic, bytes, time, &candidate)?;
                    self.publish(&mut candidate)?;
                    self.graph = candidate;
                }
                self.gossip_state = observations;
                Ok(GossipVerdict::Accept)
            }
            Err(decision) => {
                let result = match decision {
                    Decision::Reject => Ok(GossipVerdict::Reject),
                    Decision::Ignore => Ok(GossipVerdict::Ignore),
                    Decision::Infrastructure(error) => Err(error),
                };
                if !result.as_ref().is_err_and(ImportError::fatal) {
                    self.source
                        .discard_snapshot()
                        .map_err(|error| ImportError::Persistence(error.to_string()))?;
                }
                result
            }
        }
    }
}

fn validate(
    seen: &mut GossipState,
    graph: &mut Graph,
    config: &ChainConfig,
    topic: GossipTopic,
    bytes: &[u8],
    preset: Preset,
    time: u64,
) -> Result<bool, Decision> {
    match topic.kind {
        GossipKind::Aggregate => {
            macro_rules! aggregate {
                ($module:ident, $variant:ident) => {{
                    let message =
                        auxiliary::$module::SignedAggregateAndProof::from_ssz(bytes, preset)
                            .map_err(|_| Decision::Reject)?;
                    check_slot(config, topic, message.message.aggregate.data.slot)?;
                    match graph {
                        Graph::Classic { store, .. } => {
                            seen.attestations
                                .validate_aggregate(
                                    store,
                                    SignedAggregate::$variant(&message),
                                    time,
                                )
                                .map_err(attestation_error)?;
                            let wire = message
                                .message
                                .aggregate
                                .to_ssz(preset)
                                .map_err(|_| Decision::Reject)?;
                            store.on_attestation_wire(&wire, topic.fork).map_err(store_error)?;
                            Ok(true)
                        }
                        Graph::Gloas { store, .. } => {
                            seen.attestations
                                .validate_historical_aggregate(
                                    store,
                                    SignedAggregate::$variant(&message),
                                    time,
                                )
                                .map_err(attestation_error)?;
                            // Earlier wire types remain genuine historical observations. They
                            // never become fabricated progressive Gloas committee votes.
                            Ok(false)
                        }
                    }
                }};
            }
            match topic.fork {
                Fork::Phase0 => aggregate!(phase0, Phase0),
                Fork::Altair => aggregate!(altair, Altair),
                Fork::Bellatrix => aggregate!(bellatrix, Bellatrix),
                Fork::Capella => aggregate!(capella, Capella),
                Fork::Deneb => aggregate!(deneb, Deneb),
                Fork::Electra => aggregate!(electra, Electra),
                Fork::Fulu => aggregate!(fulu, Fulu),
                Fork::Gloas => {
                    let message =
                        auxiliary::gloas::SignedAggregateAndProof::from_ssz(bytes, preset)
                            .map_err(|_| Decision::Reject)?;
                    check_slot(config, topic, message.message.aggregate.data.slot)?;
                    let Graph::Gloas { store, .. } = graph else { return Err(Decision::Ignore) };
                    seen.attestations
                        .validate_gloas_aggregate(store, &message, time)
                        .map_err(attestation_error)?;
                    store.on_attestation(&message.message.aggregate).map_err(store_error)?;
                    Ok(true)
                }
            }
        }
        GossipKind::Bid => {
            let message = gloas::SignedExecutionPayloadBid::from_ssz(bytes, preset)
                .map_err(|_| Decision::Reject)?;
            check_slot(config, topic, message.message.slot)?;
            let Graph::Gloas { store, .. } = graph else { return Err(Decision::Ignore) };
            seen.bids.validate_bid(store, &message, time).map_err(bid_error)?;
            Ok(false)
        }
        GossipKind::Preferences => {
            let message = auxiliary::gloas::SignedProposerPreferences::from_ssz(bytes, preset)
                .map_err(|_| Decision::Reject)?;
            check_slot(config, topic, message.message.proposal_slot)?;
            let Graph::Gloas { store, .. } = graph else { return Err(Decision::Ignore) };
            seen.bids.validate_preferences(store, &message, time).map_err(bid_error)?;
            Ok(false)
        }
        GossipKind::PayloadAttestation => {
            let message = auxiliary::gloas::PayloadAttestationMessage::from_ssz(bytes, preset)
                .map_err(|_| Decision::Reject)?;
            check_slot(config, topic, message.data.slot)?;
            let Graph::Gloas { store, .. } = graph else { return Err(Decision::Ignore) };
            seen.payload_attestations
                .validate_message(store, &message, time)
                .map_err(payload_error)?;
            store.on_payload_attestation_message(&message).map_err(store_error)?;
            Ok(true)
        }
        // Block/envelope imports use the independently DA-serviced active import owner.
        // Unsubscribed subnet objects grant neither forwarding nor fabricated custody.
        _ => Err(Decision::Ignore),
    }
}
fn check_slot(config: &ChainConfig, topic: GossipTopic, slot: u64) -> Result<(), Decision> {
    if config.fork_at_slot(slot).map_err(|_| Decision::Reject)?.fork != topic.fork ||
        config.fork_digest(slot / config.slots_per_epoch).map_err(|_| Decision::Reject)? !=
            topic.digest
    {
        return Err(Decision::Reject)
    }
    Ok(())
}

enum Decision {
    Ignore,
    Reject,
    Infrastructure(ImportError),
}
fn store_error(error: StoreError) -> Decision {
    match error {
        StoreError::CompactState(_) => Decision::Infrastructure(error.into()),
        StoreError::ResourceLimit | StoreError::UnknownRoot | StoreError::UnavailableHistory => {
            Decision::Ignore
        }
        _ => Decision::Reject,
    }
}
fn attestation_error(error: AttestationGossipError) -> Decision {
    match error {
        AttestationGossipError::Ignore(_) | AttestationGossipError::Capacity => Decision::Ignore,
        AttestationGossipError::Reject(_) | AttestationGossipError::Ssz(_) => Decision::Reject,
        AttestationGossipError::Store(error) => store_error(error),
        AttestationGossipError::Context(error) => {
            Decision::Infrastructure(ImportError::PublicationInfrastructure(error.to_string()))
        }
    }
}
fn bid_error(error: BidGossipError) -> Decision {
    match error {
        BidGossipError::Ignore(_) | BidGossipError::ResourceLimit => Decision::Ignore,
        BidGossipError::Reject(_) | BidGossipError::Ssz(_) => Decision::Reject,
        BidGossipError::Store(error) => store_error(error),
        other => {
            Decision::Infrastructure(ImportError::PublicationInfrastructure(other.to_string()))
        }
    }
}
fn payload_error(error: PayloadAttestationGossipError) -> Decision {
    match error {
        PayloadAttestationGossipError::Ignore(_) | PayloadAttestationGossipError::ResourceLimit => {
            Decision::Ignore
        }
        PayloadAttestationGossipError::Reject(_) | PayloadAttestationGossipError::Ssz(_) => {
            Decision::Reject
        }
        PayloadAttestationGossipError::Store(error) => store_error(error),
        other => {
            Decision::Infrastructure(ImportError::PublicationInfrastructure(other.to_string()))
        }
    }
}

fn block_error(error: GossipBlockError) -> ImportError {
    match error {
        GossipBlockError::Ignore(reason) => ImportError::GossipIgnore(reason),
        GossipBlockError::Reject(reason) => ImportError::GossipReject(reason),
        GossipBlockError::Capacity => {
            ImportError::GossipIgnore("local gossip observation capacity")
        }
        GossipBlockError::Store(error) => error.into(),
        other => ImportError::PublicationInfrastructure(other.to_string()),
    }
}
