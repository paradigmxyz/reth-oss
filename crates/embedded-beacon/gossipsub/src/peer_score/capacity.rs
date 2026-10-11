// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Local admission evidence for the optional embedded transport's score-record cap.
//!
//! This query reserves nothing. Its caller must account for pending established admissions
//! before delegating connection handlers, and release those reservations on swarm outcome.

use super::{ConnectionStatus, PeerScore};
use libp2p_identity::PeerId;
use web_time::Instant;

impl PeerScore {
    pub(crate) fn can_admit_score_peer(&mut self, peer_id: &PeerId, max_peers: usize) -> bool {
        let now = Instant::now();
        let peer_ips = &mut self.peer_ips;
        self.peer_stats.retain(|peer, stats| {
            if let ConnectionStatus::Disconnected { expire } = stats.status {
                // Identical strict expiration and IP cleanup to refresh_scores(). No live
                // counters decay here and no unexpired negative score is forgotten.
                if now > expire {
                    Self::remove_ips_for_peer(stats, peer_ips, peer);
                    return false;
                }
            }
            true
        });
        self.peer_stats.contains_key(peer_id) || self.peer_stats.len() < max_peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_score::{PeerScoreParams, PeerScoreThresholds};
    use std::{collections::HashSet, net::IpAddr, time::Duration};

    fn score() -> PeerScore {
        PeerScore::new(PeerScoreParams::default(), PeerScoreThresholds::default())
    }

    #[test]
    fn capacity_denies_new_ids_without_evicting_unexpired_negative_or_connected_peers() {
        let mut score = score();
        let retained = PeerId::random();
        let connected = PeerId::random();
        let candidate = PeerId::random();
        score.add_peer(retained);
        score.add_peer(connected);
        score.set_application_score(&retained, -2.0);
        let retained_score = score.score_report(&retained).score;
        let connected_score = score.score_report(&connected).score;
        assert!(retained_score < 0.0);
        score.remove_peer(&retained);

        assert!(!score.can_admit_score_peer(&candidate, 2));
        assert_eq!(score.peer_stats.len(), 2);
        assert_eq!(score.score_report(&retained).score, retained_score);
        assert_eq!(score.score_report(&connected).score, connected_score);
        assert!(!score.peer_stats.contains_key(&candidate));
        // Reconnecting a retained identity consumes no additional unique score slot.
        assert!(score.can_admit_score_peer(&retained, 0));
        assert!(score.can_admit_score_peer(&connected, 0));
    }

    #[test]
    fn expired_negative_shared_ip_cleanup_preserves_the_connected_peer_and_score() {
        let mut score = score();
        let retained = PeerId::random();
        let connected = PeerId::random();
        let candidate = PeerId::random();
        let ip = "192.0.2.5".parse::<IpAddr>().unwrap();
        score.add_peer(retained);
        score.add_peer(connected);
        score.add_ip(&retained, ip);
        score.add_ip(&connected, ip);
        score.set_application_score(&retained, -2.0);
        score.set_application_score(&connected, 2.0);
        // Compare the real weighted reports. Application values are multiplied by the configured
        // app-specific weight; admission must preserve the report instead of assuming weight 1.
        let retained_score = score.score_report(&retained).score;
        let connected_score = score.score_report(&connected).score;
        assert!(retained_score < 0.0 && connected_score > 0.0);
        score.remove_peer(&retained);

        assert!(!score.can_admit_score_peer(&candidate, 2));
        assert_eq!(score.peer_ips.get(&ip), Some(&HashSet::from([retained, connected])));
        assert_eq!(score.score_report(&retained).score, retained_score);
        assert_eq!(score.score_report(&connected).score, connected_score);

        score.peer_stats.get_mut(&retained).unwrap().status = ConnectionStatus::Disconnected {
            expire: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
        };
        assert!(score.can_admit_score_peer(&candidate, 2));
        assert!(!score.peer_stats.contains_key(&retained));
        assert_eq!(score.peer_ips.get(&ip), Some(&HashSet::from([connected])));
        assert_eq!(score.peer_stats.get(&connected).unwrap().known_ips, HashSet::from([ip]));
        assert_eq!(score.score_report(&connected).score, connected_score);
        assert!(!score.peer_stats.contains_key(&candidate));
    }

    #[test]
    fn admission_does_not_decay_live_penalties_or_reset_unexpired_retained_penalties() {
        let mut score = score();
        let live = PeerId::random();
        let retained = PeerId::random();
        let candidate = PeerId::random();
        score.add_peer(live);
        score.add_peer(retained);
        score.add_penalty(&live, 2);
        score.add_penalty(&retained, 2);
        score.remove_peer(&retained);
        let live_score = score.score_report(&live).score;
        let retained_score = score.score_report(&retained).score;
        assert!(live_score < 0.0 && retained_score < 0.0);

        assert!(!score.can_admit_score_peer(&candidate, 2));
        assert_eq!(score.score_report(&live).score, live_score);
        assert_eq!(score.score_report(&retained).score, retained_score);
        assert!(matches!(score.peer_stats.get(&live).unwrap().status, ConnectionStatus::Connected));
        assert!(matches!(
            score.peer_stats.get(&retained).unwrap().status,
            ConnectionStatus::Disconnected { .. }
        ));
    }

    #[test]
    fn capacity_query_reserves_nothing_and_pending_slots_reduce_the_callers_limit() {
        let mut score = score();
        let first = PeerId::random();
        let second = PeerId::random();
        assert!(score.can_admit_score_peer(&first, 1));
        assert!(score.peer_stats.is_empty());
        // A pending reservation for first leaves no capacity for a different identity.
        assert!(!score.can_admit_score_peer(&second, 1usize.saturating_sub(1)));
        assert!(score.peer_stats.is_empty());
        // The caller can release a failed pending admission without altering score state.
        assert!(score.can_admit_score_peer(&second, 1));
    }
}
