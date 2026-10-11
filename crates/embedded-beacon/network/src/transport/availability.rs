//! Original sidecar RPC channels share peer, stream and retained-byte admission with blocks.

use super::*;

/// Opaque sidecar request token owned by one authenticated transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SidecarRequestId {
    owner: PeerId,
    sequence: u64,
}

#[derive(Debug)]
pub(super) struct PendingSidecarRequest {
    pub(super) peer: PeerId,
    id: SidecarRequestId,
    request: SidecarRequest,
}

#[derive(Debug)]
pub(super) struct InboundSidecarRequest {
    pub(super) peer: PeerId,
    request_id: InboundRequestId,
    protocol: Protocol,
    request: SidecarRequest,
    channel: ResponseChannel<SidecarReply>,
}

impl StatusTransport {
    /// Retrieve complete original sidecars only from a checked authenticated peer.
    /// All block, envelope and sidecar requests share one global/per-peer admission budget.
    pub fn request_sidecars(
        &mut self,
        peer: PeerId,
        request: SidecarRequest,
    ) -> Result<SidecarRequestId, TransportError> {
        request.validate_outbound().map_err(|_| TransportError::Configuration)?;
        if !self.checked_peers.contains(&peer) {
            return Err(TransportError::Configuration)
        }
        if self.block_pending.len() + self.envelope_pending.len() + self.sidecar_pending.len() >=
            MAX_BLOCK_REQUESTS ||
            self.block_pending.values().any(|pending| pending.peer == peer) ||
            self.envelope_pending.values().any(|pending| pending.peer == peer) ||
            self.sidecar_pending.values().any(|pending| pending.peer == peer)
        {
            return Err(TransportError::Capacity)
        }
        self.next_sidecar_request =
            self.next_sidecar_request.checked_add(1).ok_or(TransportError::Capacity)?;
        let id = SidecarRequestId { owner: self.peer_id(), sequence: self.next_sidecar_request };
        let protocol = request.protocol();
        let request_id = self.sidecars_mut(protocol)?.send_request(&peer, request.clone());
        self.sidecar_pending
            .insert((protocol, request_id), PendingSidecarRequest { peer, id, request });
        Ok(id)
    }

    /// Respond using only the original private channel after complete request/context matching.
    /// Serving callers must additionally require their retained data's real validation receipts.
    pub fn respond_sidecars(
        &mut self,
        id: SidecarRequestId,
        sidecars: Vec<WireSidecar>,
    ) -> Result<(), TransportError> {
        let pending = self.sidecar_inbound.get(&id).ok_or(TransportError::Configuration)?;
        let batch = SidecarBatch::serving(sidecars);
        pending.request.matches(&self.config, &batch).map_err(|_| TransportError::Configuration)?;
        let pending = self.sidecar_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.sidecars_mut(pending.protocol)?
            .send_response(pending.channel, SidecarReply::Sidecars(batch))
            .map_err(|_| TransportError::Configuration)
    }

    /// Report missing data without providing an asserted availability success.
    pub fn respond_sidecars_unavailable(
        &mut self,
        id: SidecarRequestId,
    ) -> Result<(), TransportError> {
        let pending = self.sidecar_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.sidecars_mut(pending.protocol)?
            .send_response(pending.channel, SidecarReply::Error(3))
            .map_err(|_| TransportError::Configuration)
    }

    pub(super) fn handle_sidecar_event(
        &mut self,
        protocol: Protocol,
        event: request_response::Event<SidecarRequest, SidecarReply>,
    ) -> Option<TransportEvent> {
        let owned = match &event {
            request_response::Event::Message {
                message: Message::Response { request_id, .. },
                ..
            } |
            request_response::Event::OutboundFailure { request_id, .. } => {
                self.sidecar_pending.get(&(protocol, *request_id)).map(|pending| pending.peer)
            }
            request_response::Event::InboundFailure { request_id, .. } => self
                .sidecar_inbound
                .values()
                .find(|pending| pending.protocol == protocol && pending.request_id == *request_id)
                .map(|pending| pending.peer),
            _ => None,
        };
        match self.admit_rpc_event(&event, owned) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(rejected) => return Some(rejected),
        }
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                Message::Request { request_id, request, channel } => {
                    if !self.checked_peers.contains(&peer) || request.protocol() != protocol {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        })
                    }
                    if self.block_inbound.len() +
                        self.envelope_inbound.len() +
                        self.sidecar_inbound.len() >=
                        MAX_INBOUND_BLOCK_REQUESTS
                    {
                        if let Ok(requests) = self.sidecars_mut(protocol) {
                            let _ = requests.send_response(channel, SidecarReply::Error(2));
                        }
                        return None
                    }
                    let Some(sequence) = self.next_sidecar_request.checked_add(1) else {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Capacity,
                        })
                    };
                    self.next_sidecar_request = sequence;
                    let id = SidecarRequestId { owner: self.peer_id(), sequence };
                    self.sidecar_inbound.insert(
                        id,
                        InboundSidecarRequest {
                            peer,
                            request_id,
                            protocol,
                            request: request.clone(),
                            channel,
                        },
                    );
                    Some(TransportEvent::SidecarRequest { peer, id, request })
                }
                Message::Response { request_id, response } => {
                    let Some(pending) = self
                        .sidecar_pending
                        .remove(&(protocol, request_id))
                        .filter(|pending| pending.peer == peer)
                    else {
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        })
                    };
                    match response {
                        SidecarReply::Sidecars(batch)
                            if pending.request.matches(&self.config, &batch).is_ok() =>
                        {
                            Some(TransportEvent::Sidecars { peer, id: pending.id, batch })
                        }
                        SidecarReply::Error(code) => {
                            Some(TransportEvent::SidecarsUnavailable { peer, id: pending.id, code })
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
                // Preserve a new same-peer owner when an already retired stream fails late.
                self.sidecar_pending.remove(&(protocol, request_id))?;
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::InboundFailure { peer, request_id, .. } => {
                self.sidecar_inbound.retain(|_, pending| {
                    pending.peer != peer ||
                        pending.protocol != protocol ||
                        pending.request_id != request_id
                });
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::ResponseSent { .. } => None,
        }
    }

    fn sidecars_mut(
        &mut self,
        protocol: Protocol,
    ) -> Result<&mut request_response::Behaviour<SidecarCodec>, TransportError> {
        Ok(match protocol {
            Protocol::BlobSidecarsByRangeV1 => &mut self.swarm.behaviour_mut().sidecars_blob_range,
            Protocol::BlobSidecarsByRootV1 => &mut self.swarm.behaviour_mut().sidecars_blob_root,
            Protocol::DataColumnsByRangeV1 => &mut self.swarm.behaviour_mut().sidecars_column_range,
            Protocol::DataColumnsByRootV1 => &mut self.swarm.behaviour_mut().sidecars_column_root,
            _ => return Err(TransportError::Configuration),
        })
    }
}

pub(super) fn sidecar_requests(
    codec: SidecarCodec,
    protocol: Protocol,
) -> request_response::Behaviour<SidecarCodec> {
    request_response::Behaviour::with_codec(
        codec,
        [(StreamProtocol::new(protocol.id()), ProtocolSupport::Full)],
        request_response::Config::default()
            .with_request_timeout(REQUEST_TIMEOUT)
            .with_max_concurrent_streams(MAX_WORKERS_PER_CONNECTION),
    )
}
