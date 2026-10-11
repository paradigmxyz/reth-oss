//! Small expiring provider preferences bound to signed discovery and checked transport identities.
//!
//! Remote custody claims are retrieval hints. They never exclude a provider, satisfy a sample,
//! skip original proofs or alter request ownership. The map shares the existing peer count cap.

use crate::{
    control_rpc::PeerMetadata, custody::CustodyParameters, discovery::DiscoveryRecord,
    transport::MAX_PEERS,
};
use libp2p::PeerId;
use reth_embedded_beacon_primitives::Root;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

const HINT_LIFETIME: Duration = Duration::from_secs(120);

#[derive(Default)]
pub(super) struct CustodyHints {
    peers: HashMap<PeerId, Hint>,
}

struct Hint {
    node_id: Option<Root>,
    record_sequence: Option<u64>,
    record_count: Option<u64>,
    metadata: Option<PeerMetadata>,
    columns: Option<u128>,
    observed: Instant,
    active: Instant,
    checked: bool,
}

impl CustodyHints {
    pub(super) fn record(&mut self, record: &DiscoveryRecord, now: Instant) {
        let Some(hint) = self.entry(record.peer_id(), now) else { return };
        let node_id = record.node_id().raw();
        if hint.node_id.is_some_and(|previous| previous != node_id) ||
            hint.record_sequence.is_some_and(|sequence| sequence > record.sequence()) ||
            (hint.record_sequence == Some(record.sequence()) &&
                hint.record_count != record.custody_group_count)
        {
            return
        }
        hint.node_id = Some(node_id);
        hint.record_sequence = Some(record.sequence());
        hint.record_count = record.custody_group_count;
        hint.columns = custody_columns(
            hint.node_id,
            hint.metadata.map_or(hint.record_count, |metadata| metadata.custody_group_count),
        );
        hint.observed = now;
        hint.active = now;
    }

    pub(super) fn checked(&mut self, peer: PeerId, now: Instant) {
        if let Some(hint) = self.entry(peer, now) {
            hint.checked = true;
            hint.active = now;
        }
    }

    pub(super) fn metadata(&mut self, peer: PeerId, metadata: PeerMetadata, now: Instant) {
        let Some(hint) = self.peers.get_mut(&peer) else { return };
        if !hint.checked ||
            metadata.custody_group_count.is_some_and(|count| count > 128) ||
            metadata.syncnets.is_some_and(|bits| bits & 0xf0 != 0) ||
            (metadata.custody_group_count.is_some() && metadata.syncnets.is_none()) ||
            hint.metadata.is_some_and(|previous| {
                metadata.seq_number < previous.seq_number ||
                    (metadata.seq_number == previous.seq_number && metadata != previous)
            })
        {
            return
        }
        hint.columns = custody_columns(hint.node_id, metadata.custody_group_count);
        hint.metadata = Some(metadata);
        hint.observed = now;
        hint.active = now;
    }

    pub(super) fn overlap(&self, peer: PeerId, columns: &[u64], now: Instant) -> usize {
        let Some(hint) = self.peers.get(&peer) else { return 0 };
        if !hint.checked || !hint.fresh(now) {
            return 0
        }
        let Some(mask) = hint.columns else { return 0 };
        columns.iter().filter(|column| **column < 128 && mask & (1u128 << **column) != 0).count()
    }

    pub(super) fn remove(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    pub(super) fn clear(&mut self) {
        self.peers.clear();
    }

    fn entry(&mut self, peer: PeerId, now: Instant) -> Option<&mut Hint> {
        self.peers.retain(|_, hint| hint.active_fresh(now));
        if self.peers.len() >= MAX_PEERS && !self.peers.contains_key(&peer) {
            return None
        }
        Some(self.peers.entry(peer).or_insert(Hint {
            node_id: None,
            record_sequence: None,
            record_count: None,
            metadata: None,
            columns: None,
            observed: now,
            active: now,
            checked: false,
        }))
    }
}

impl Hint {
    fn fresh(&self, now: Instant) -> bool {
        now.checked_duration_since(self.observed).is_some_and(|age| age < HINT_LIFETIME)
    }

    fn active_fresh(&self, now: Instant) -> bool {
        now.checked_duration_since(self.active).is_some_and(|age| age < HINT_LIFETIME)
    }
}

fn custody_columns(node_id: Option<Root>, count: Option<u64>) -> Option<u128> {
    let count = count?;
    if count > 128 {
        return None
    }
    if count == 128 {
        return Some(u128::MAX)
    }
    let mut node_id = node_id?;
    // Discovery NodeId is a big-endian unsigned integer; DAS selection consumes little-endian.
    node_id.reverse();
    let groups = CustodyParameters::default().get_custody_groups(node_id, count).ok()?;
    Some(groups.iter().fold(0, |mask, group| mask | (1u128 << group)))
}

#[cfg(test)]
mod tests;
