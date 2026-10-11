//! Authenticated bounded Ping/Metadata handlers use a separate immutable metadata sequence.

use super::*;
use crate::control_rpc::{ControlCodec, ControlProtocol, ControlReply, ControlRequest};

const MAX_CONTROL_PENDING: usize = 16;

impl StatusTransport {
    pub(crate) fn prepare_custody_metadata(
        &self,
        count: u64,
    ) -> Result<LocalMetadata, TransportError> {
        self.local_metadata.next_custody(count).ok_or(TransportError::Configuration)
    }
    pub(crate) const fn commit_custody_metadata(&mut self, metadata: LocalMetadata) {
        self.local_metadata = metadata;
    }
    /// Request liveness using the local metadata sequence, independent of discovery records.
    pub fn request_ping(&mut self, peer: PeerId) -> Result<(), TransportError> {
        self.request_control(peer, ControlProtocol::Ping)
    }
    /// Request the exact metadata schema for the current configured network fork.
    pub fn request_metadata(&mut self, peer: PeerId) -> Result<(), TransportError> {
        let fork = self
            .config
            .fork_at_slot(self.current_slot)
            .map_err(|_| TransportError::Configuration)?
            .fork;
        let protocol = if fork >= Fork::Fulu {
            ControlProtocol::MetadataV3
        } else if fork >= Fork::Altair {
            ControlProtocol::MetadataV2
        } else {
            ControlProtocol::MetadataV1
        };
        self.request_control(peer, protocol)
    }
    fn request_control(
        &mut self,
        peer: PeerId,
        protocol: ControlProtocol,
    ) -> Result<(), TransportError> {
        if !self.checked_peers.contains(&peer) {
            return Err(TransportError::Configuration)
        }
        if self.control_pending.len() >= MAX_CONTROL_PENDING ||
            self.control_pending.values().any(|pending| *pending == peer)
        {
            return Err(TransportError::Capacity)
        }
        let request = if protocol == ControlProtocol::Ping {
            ControlRequest::Ping(self.local_metadata.seq_number())
        } else {
            ControlRequest::Metadata
        };
        let id = self.controls_mut(protocol).send_request(&peer, request);
        self.control_pending.insert((protocol, id), peer);
        Ok(())
    }
    pub(super) fn handle_control_event(
        &mut self,
        protocol: ControlProtocol,
        event: request_response::Event<ControlRequest, ControlReply>,
    ) -> Option<TransportEvent> {
        let owned = match &event {
            request_response::Event::Message {
                message: Message::Response { request_id, .. },
                ..
            } |
            request_response::Event::OutboundFailure { request_id, .. } => {
                self.control_pending.get(&(protocol, *request_id)).copied()
            }
            _ => None,
        };
        match self.admit_rpc_event(&event, owned) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(rejected) => return Some(rejected),
        }
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                Message::Request { request, channel, .. } => {
                    // An authenticated connection may request metadata before status completes.
                    // No bulk retrieval or consensus admission is enabled by this control reply.
                    if !self.peers.contains(&peer) {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        })
                    }
                    let reply = match (protocol, request) {
                        (ControlProtocol::Ping, ControlRequest::Ping(_)) => {
                            ControlReply::Ping(self.local_metadata.seq_number())
                        }
                        (
                            ControlProtocol::MetadataV1 |
                            ControlProtocol::MetadataV2 |
                            ControlProtocol::MetadataV3,
                            ControlRequest::Metadata,
                        ) => ControlReply::Metadata(self.local_metadata.snapshot(protocol)),
                        _ => {
                            drop(channel);
                            self.reject(peer);
                            return Some(TransportEvent::Rejected {
                                peer,
                                reason: StatusRejection::Protocol,
                            })
                        }
                    };
                    if self.controls_mut(protocol).send_response(channel, reply).is_err() {
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        })
                    }
                    None
                }
                Message::Response { request_id, response } => {
                    if self.control_pending.remove(&(protocol, request_id)) != Some(peer) {
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        })
                    }
                    match (protocol, response) {
                        (ControlProtocol::Ping, ControlReply::Ping(seq_number)) => {
                            Some(TransportEvent::PeerPing { peer, seq_number })
                        }
                        (
                            ControlProtocol::MetadataV1 |
                            ControlProtocol::MetadataV2 |
                            ControlProtocol::MetadataV3,
                            ControlReply::Metadata(metadata),
                        ) => Some(TransportEvent::PeerMetadata { peer, metadata }),
                        (_, ControlReply::Error(code)) => {
                            Some(TransportEvent::ControlUnavailable { peer, code })
                        }
                        _ => {
                            self.reject(peer);
                            Some(TransportEvent::Rejected {
                                peer,
                                reason: StatusRejection::Protocol,
                            })
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, request_id, .. } => {
                // A retired original token must not reject a reconnected same-peer owner.
                self.control_pending.remove(&(protocol, request_id))?;
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::InboundFailure { peer, .. } => {
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::ResponseSent { .. } => None,
        }
    }
    fn controls_mut(
        &mut self,
        protocol: ControlProtocol,
    ) -> &mut request_response::Behaviour<ControlCodec> {
        let behaviour = self.swarm.behaviour_mut();
        match protocol {
            ControlProtocol::Ping => &mut behaviour.ping,
            ControlProtocol::MetadataV1 => &mut behaviour.metadata_v1,
            ControlProtocol::MetadataV2 => &mut behaviour.metadata_v2,
            ControlProtocol::MetadataV3 => &mut behaviour.metadata_v3,
        }
    }
}

pub(super) fn control_requests(
    protocol: ControlProtocol,
) -> request_response::Behaviour<ControlCodec> {
    request_response::Behaviour::with_codec(
        ControlCodec,
        [(StreamProtocol::new(protocol.id()), ProtocolSupport::Full)],
        request_response::Config::default()
            .with_request_timeout(REQUEST_TIMEOUT)
            // The handler shares this worker limit between inbound and outbound streams.
            // One in each direction prevents simultaneous metadata requests from deadlocking.
            .with_max_concurrent_streams(2),
    )
}

#[cfg(test)]
mod tests;
