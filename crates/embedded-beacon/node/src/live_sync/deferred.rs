//! Transient admission pressure or execution syncing retains the exact bounded original input.

use super::*;

pub(super) struct DeferredImport {
    pub(super) active: ActiveImport,
    ready: Instant,
}
impl LiveSync {
    pub(super) fn defer_import(&mut self, mut active: ActiveImport) -> Result<(), LiveSyncError> {
        if self.deferred_import.is_some() {
            return Err(LiveSyncError::Bounds)
        }
        if let Some(id) = active.gossip.take() {
            self.report_gossip(id, GossipVerdict::Ignore);
        }
        // The completed callback is a known non-authoritative result. Its original input and
        // candidate guard remain owned, while the already completed future can be dropped.
        active.reply = Box::pin(std::future::pending());
        tracing::debug!(target: "beacon::sync", root = %B256::from(active.root),
            "[beacon] Retained original consensus input for bounded retry");
        self.deferred_import =
            Some(DeferredImport { active, ready: Instant::now() + Duration::from_secs(1) });
        Ok(())
    }
    pub(super) async fn retry_deferred_import(&mut self) -> Result<(), LiveSyncError> {
        if self.active.is_some() ||
            self.deferred_import.as_ref().is_none_or(|pending| Instant::now() < pending.ready)
        {
            return Ok(())
        }
        let mut pending = self.deferred_import.take().ok_or(LiveSyncError::Response)?;
        let current_time = now()?;
        pending.active.reply = match &pending.active._batch {
            RetainedBatch::Blocks { _batch } => {
                let wire = _batch.blocks().first().cloned().ok_or(LiveSyncError::Response)?;
                if wire.root() != pending.active.root {
                    return Err(LiveSyncError::Response)
                }
                Box::pin(self.importer.import_block(wire, current_time))
            }
            RetainedBatch::Branch { _wire } => {
                if _wire.root() != pending.active.root {
                    return Err(LiveSyncError::Response)
                }
                Box::pin(self.importer.import_block(_wire.clone(), current_time))
            }
            RetainedBatch::Envelopes { _batch } => {
                let wire = _batch.envelopes().first().cloned().ok_or(LiveSyncError::Response)?;
                if wire.beacon_block_root() != pending.active.root {
                    return Err(LiveSyncError::Response)
                }
                Box::pin(self.importer.import_envelope(wire, current_time))
            }
            RetainedBatch::Gossip { _input } => match _input.topic().kind {
                GossipKind::Block => {
                    let wire = WireBlock::from_ssz(_input.ssz().to_vec(), &self.config)
                        .map_err(|_| LiveSyncError::Response)?;
                    if wire.root() != pending.active.root {
                        return Err(LiveSyncError::Response)
                    }
                    // Propagation was resolved Ignore exactly once. The strict importer still
                    // rechecks the original signature and complete consensus transition.
                    Box::pin(self.importer.import_block(wire, current_time))
                }
                GossipKind::Envelope => {
                    let reference = match self.importer.block_reference(pending.active.root).await {
                        Ok(Some(reference)) => reference,
                        Err(ImportError::Backpressure) => {
                            pending.ready = Instant::now() + Duration::from_secs(1);
                            self.deferred_import = Some(pending);
                            return Ok(())
                        }
                        Ok(None) => return Err(LiveSyncError::Response),
                        Err(error) => return Err(error.into()),
                    };
                    let wire = WireEnvelope::from_reference(
                        _input.ssz().to_vec(),
                        &reference,
                        &self.config,
                    )
                    .map_err(|_| LiveSyncError::Response)?;
                    Box::pin(self.importer.import_envelope(wire, current_time))
                }
                _ => return Err(LiveSyncError::Response),
            },
        };
        self.active = Some(pending.active);
        Ok(())
    }
}
