//! Control metadata retires only after elapsed lifetime and actual dependency cleanup.

use super::*;
use controls::ControlCharge;
use std::collections::VecDeque;

const MAX_CONTROL_RETAINED_BYTES: usize = 2 * 1024 * 1024;
const MAX_PEER_CONTROL_BYTES: usize = 256 * 1024;
const MAX_CONTROL_RESERVATIONS: usize = 4096;
// Configured local prune_backoff60s, heartbeat700ms, backoff_slack1 and one partial slot.
const BACKOFF_RING_REVOLUTION: u64 = 89;

#[derive(Default)]
pub(super) struct ControlBudget {
    entries: VecDeque<ControlReservation>,
    peers: HashMap<PeerId, usize>,
    bytes: usize,
    completed: u64,
    poisoned: bool,
}
struct ControlReservation {
    peer: PeerId,
    bytes: usize,
    lifetime: Duration,
    expires: Option<Instant>,
    after_elapsed: Option<u64>,
    backoff: bool,
}
impl ControlBudget {
    pub(super) fn reserve(&mut self, peer: PeerId, charge: ControlCharge) -> bool {
        if self.poisoned ||
            self.entries.len() >= MAX_CONTROL_RESERVATIONS ||
            charge.bytes == 0 ||
            charge.bytes > MAX_CONTROL_RETAINED_BYTES.saturating_sub(self.bytes) ||
            charge.bytes >
                MAX_PEER_CONTROL_BYTES.saturating_sub(*self.peers.get(&peer).unwrap_or(&0))
        {
            return false
        }
        self.bytes += charge.bytes;
        *self.peers.entry(peer).or_default() += charge.bytes;
        self.entries.push_back(ControlReservation {
            peer,
            bytes: charge.bytes,
            lifetime: Duration::from_secs(charge.backoff_seconds.saturating_add(1).max(120)),
            expires: None,
            after_elapsed: None,
            backoff: charge.backoff_seconds != 0,
        });
        true
    }
    pub(super) fn settled(&mut self) {
        let now = Instant::now();
        for entry in &mut self.entries {
            if entry.expires.is_none() {
                entry.expires = now.checked_add(entry.lifetime);
                self.poisoned |= entry.expires.is_none();
            }
        }
    }
    pub(super) fn completed_heartbeats(&mut self, completed: u64) {
        if completed < self.completed {
            self.poisoned = true;
            return
        }
        self.completed = completed;
        let now = Instant::now();
        self.entries.retain_mut(|entry| {
            if entry.expires.is_none_or(|expires| expires > now) {
                return true
            }
            let retire = if entry.backoff {
                // An elapsed deadline alone does not prove a private ring slot was visited.
                // Observe a complete actual revolution after expiry plus configured slack.
                let start = *entry.after_elapsed.get_or_insert(completed);
                completed.saturating_sub(start) >= BACKOFF_RING_REVOLUTION
            } else {
                let start = *entry.after_elapsed.get_or_insert(completed);
                completed.saturating_sub(start) >= 1
            };
            if retire {
                self.bytes = self.bytes.saturating_sub(entry.bytes);
                if let Some(bytes) = self.peers.get_mut(&entry.peer) {
                    *bytes = bytes.saturating_sub(entry.bytes);
                    if *bytes == 0 {
                        self.peers.remove(&entry.peer);
                    }
                }
            }
            !retire
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_backoff_retains_its_peer_charge_until_actual_full_ring_cleanup() {
        let peer = PeerId::random();
        let mut budget = ControlBudget::default();
        assert!(budget.reserve(
            peer,
            ControlCharge {
                bytes: MAX_PEER_CONTROL_BYTES,
                backoff_seconds: 60,
                ..Default::default()
            }
        ));
        assert!(!budget.reserve(peer, ControlCharge { bytes: 1, ..Default::default() }));
        budget.settled();
        for entry in &mut budget.entries {
            entry.expires = Some(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        }
        budget.completed_heartbeats(1);
        budget.completed_heartbeats(BACKOFF_RING_REVOLUTION);
        assert_eq!(budget.bytes, MAX_PEER_CONTROL_BYTES);
        budget.completed_heartbeats(BACKOFF_RING_REVOLUTION + 1);
        assert_eq!(budget.bytes, 0);
        assert!(budget.peers.is_empty());
        assert!(budget.reserve(peer, ControlCharge { bytes: 1, ..Default::default() }));
    }
}
