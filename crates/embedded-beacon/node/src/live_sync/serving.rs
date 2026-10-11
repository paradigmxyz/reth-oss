//! One bounded original-wire serving query runs alongside active consensus/proof work.
//! A queued read can be cancelled safely; mutable imports retain their original ownership.

use super::*;
use crate::import_coordinator::{ServingBatch, ServingRequest};

const SERVING_DEADLINE: Duration = Duration::from_secs(8);

pub(super) struct ActiveServing {
    pub(super) peer: PeerId,
    pub(super) token: ServingToken,
    pub(super) started: Instant,
    pub(super) reply:
        Pin<Box<dyn Future<Output = Result<Option<ServingBatch>, ImportError>> + Send>>,
}

#[derive(Clone, Copy)]
pub(super) enum ServingToken {
    Blocks(BlockRequestId),
    Envelopes(EnvelopeRequestId),
    Sidecars(SidecarRequestId),
}

impl LiveSync {
    pub(super) fn serve_request(
        &mut self,
        peer: PeerId,
        token: ServingToken,
        request: ServingRequest,
    ) -> Result<(), LiveSyncError> {
        if self.serving.is_some() {
            return self.serve_unavailable(token)
        }
        self.serving = Some(ActiveServing {
            peer,
            token,
            started: Instant::now(),
            reply: Box::pin(self.importer.serve(request)),
        });
        Ok(())
    }

    pub(super) fn serve_sidecars_request(
        &mut self,
        peer: PeerId,
        id: SidecarRequestId,
        request: reth_embedded_beacon_network::availability_rpc::SidecarRequest,
    ) -> Result<(), LiveSyncError> {
        let token = ServingToken::Sidecars(id);
        if self.serving.is_some() {
            return self.serve_unavailable(token)
        }
        self.serving = Some(ActiveServing {
            peer,
            token,
            started: Instant::now(),
            reply: Box::pin(self.availability.serve_sidecars(self.importer.clone(), request)),
        });
        Ok(())
    }

    pub(super) fn served(
        &mut self,
        result: Result<Option<ServingBatch>, ImportError>,
    ) -> Result<(), LiveSyncError> {
        let pending = self.serving.take().ok_or(LiveSyncError::Response)?;
        match result {
            Ok(Some(ServingBatch::Blocks(blocks))) => {
                let ServingToken::Blocks(id) = pending.token else {
                    return Err(LiveSyncError::Response)
                };
                let result = self
                    .peer
                    .as_mut()
                    .ok_or(LiveSyncError::PeerUnavailable)?
                    .respond_blocks(id, blocks);
                Self::serving_response(result)
            }
            Ok(Some(ServingBatch::Envelopes { envelopes, blocks })) => {
                let ServingToken::Envelopes(id) = pending.token else {
                    return Err(LiveSyncError::Response)
                };
                let result = self
                    .peer
                    .as_mut()
                    .ok_or(LiveSyncError::PeerUnavailable)?
                    .respond_envelopes(id, envelopes, &blocks);
                Self::serving_response(result)
            }
            Ok(Some(ServingBatch::Sidecars(sidecars))) => {
                let ServingToken::Sidecars(id) = pending.token else {
                    return Err(LiveSyncError::Response)
                };
                let result = self
                    .peer
                    .as_mut()
                    .ok_or(LiveSyncError::PeerUnavailable)?
                    .respond_sidecars(id, sidecars);
                Self::serving_response(result)
            }
            Ok(None) => self.serve_unavailable(pending.token),
            Err(error) if !error.fatal() => self.serve_unavailable(pending.token),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn expire_serving(&mut self) -> Result<(), LiveSyncError> {
        if self
            .serving
            .as_ref()
            .is_some_and(|pending| pending.started.elapsed() >= SERVING_DEADLINE)
        {
            let pending = self.serving.take().ok_or(LiveSyncError::Response)?;
            // Dropping this read future closes its receiver. The owner skips it if still queued;
            // a running immutable read owns its allocation until its bounded operation finishes.
            self.serve_unavailable(pending.token)?;
        }
        Ok(())
    }

    fn serve_unavailable(&mut self, token: ServingToken) -> Result<(), LiveSyncError> {
        let peer = self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?;
        let result = match token {
            ServingToken::Blocks(id) => peer.respond_blocks_unavailable(id),
            ServingToken::Envelopes(id) => peer.respond_envelopes_unavailable(id),
            ServingToken::Sidecars(id) => peer.respond_sidecars_unavailable(id),
        };
        Self::serving_response(result)
    }

    fn serving_response(result: Result<(), PeerStartupError>) -> Result<(), LiveSyncError> {
        match result {
            Ok(()) => Ok(()),
            Err(PeerStartupError::Peer(
                reth_embedded_beacon_network::peer::PeerRuntimeError::Transport(
                    reth_embedded_beacon_network::transport::TransportError::Configuration,
                ),
            )) => {
                // A disconnect or finite response deadline can retire the private token while
                // the read runs. No peer data or mutable consensus outcome is reclassified.
                tracing::debug!(target: "beacon::sync", "[beacon] Original serving channel has already closed");
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}
