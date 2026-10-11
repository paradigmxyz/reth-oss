//! Bounded recurring status exchanges keep connected peer heads useful for retrieval.
//! The returned head/finality values remain checked hints and cannot publish canonical state.

use super::*;

impl StatusTransport {
    pub(super) fn refresh_status(&mut self) {
        self.next_status_refresh = tokio::time::Instant::now() + STATUS_REFRESH_INTERVAL;
        let schema_v2 =
            self.config.fork_at_slot(self.current_slot).is_ok_and(|fork| fork.fork >= Fork::Fulu);
        // Checked connections and pending status requests already have separate finite caps.
        // One request per peer retains its original token until a reply/failure/disconnect.
        let peers = self.checked_peers.iter().copied().collect::<Vec<_>>();
        for peer in peers {
            if self.pending.len() == MAX_STREAMS {
                break;
            }
            if self.peers.contains(&peer) && !self.pending.values().any(|pending| *pending == peer)
            {
                let status = StatusAdvertisement::from_local(&self.local_status);
                let request = self.requests_mut(schema_v2).send_request(&peer, status);
                self.pending.insert((schema_v2, request), peer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    async fn connected(slot: Slot) -> (StatusTransport, StatusTransport) {
        let config = ChainConfig::hoodi();
        let checkpoint = Checkpoint { epoch: 100, root: [1; 32] };
        let status = StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(slot / 32).unwrap(),
                finalized_root: checkpoint.root,
                finalized_epoch: checkpoint.epoch,
                head_root: [2; 32],
                head_slot: slot,
            },
            earliest_available_slot: 7,
        };
        let make = || {
            StatusTransport::new(
                Keypair::generate_secp256k1(),
                config.clone(),
                slot,
                checkpoint,
                Vec::new(),
                status.clone(),
            )
            .unwrap()
        };
        let mut listener = make();
        let mut dialer = make();
        listener.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        let TransportEvent::Listening(address) =
            timeout(Duration::from_secs(10), listener.next_event()).await.unwrap()
        else {
            panic!("actual TCP listener did not start");
        };
        dialer.dial(listener.peer_id(), address).unwrap();
        super::super::tests::exchange_connected(
            &mut listener,
            &mut dialer,
            (slot >= 50700 * 32).then_some(7),
        )
        .await;
        (listener, dialer)
    }

    async fn advancing_heads(slot: Slot) {
        let (mut listener, mut dialer) = connected(slot).await;
        for owner in [&mut listener, &mut dialer] {
            let mut status = owner.local_status.clone();
            status.status.head_slot = slot + 1;
            status.status.head_root = [3; 32];
            owner.update_snapshot(slot + 1, owner.local_finalized, Vec::new(), status).unwrap();
            owner.next_status_refresh = tokio::time::Instant::now();
        }
        // The actual automatic timer, not a caller's injected observation, crosses the wire.
        super::super::tests::exchange_connected(
            &mut listener,
            &mut dialer,
            (slot >= 50700 * 32).then_some(7),
        )
        .await;
        assert_eq!(listener.checked_peers.len(), 1);
        assert_eq!(dialer.checked_peers.len(), 1);
        listener.refresh_status();
        dialer.refresh_status();
        assert_eq!(listener.pending.len(), 1);
        assert_eq!(dialer.pending.len(), 1);
        for _ in 0..32 {
            listener.refresh_status();
            dialer.refresh_status();
        }
        assert_eq!(listener.pending.len(), 1);
        assert_eq!(dialer.pending.len(), 1);
        listener.shutdown();
        dialer.shutdown();
        assert!(listener.pending.is_empty());
        assert!(dialer.pending.is_empty());
    }

    #[tokio::test]
    async fn actual_tcp_recurring_status_v1_follows_new_heads_without_duplicate_requests() {
        advancing_heads(3000 * 32).await;
    }

    #[tokio::test]
    async fn actual_tcp_recurring_status_v2_follows_new_heads_without_duplicate_requests() {
        advancing_heads(50700 * 32).await;
    }

    #[tokio::test]
    async fn refreshed_unknown_finalized_root_remains_a_retrieval_hint_on_actual_tcp() {
        let slot = 50700 * 32;
        let (mut listener, mut dialer) = connected(slot).await;
        let original_finalized = dialer.local_finalized;
        let original_status = dialer.local_status.clone();
        let finalized = Checkpoint { epoch: 101, root: [8; 32] };
        let mut status = listener.local_status.clone();
        status.status.finalized_epoch = finalized.epoch;
        status.status.finalized_root = finalized.root;
        status.status.head_slot = slot + 1;
        status.status.head_root = [3; 32];
        listener.update_snapshot(slot + 1, finalized, Vec::new(), status).unwrap();
        dialer
            .update_snapshot(slot + 1, original_finalized, Vec::new(), original_status.clone())
            .unwrap();
        dialer.next_status_refresh = tokio::time::Instant::now();
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = dialer.next_event() => {
                        if let TransportEvent::PeerStatus { peer, status, compatibility, .. } = event {
                            assert_eq!(peer, listener.peer_id());
                            assert_eq!(status.head_slot, slot + 1);
                            assert_eq!(compatibility, StatusCompatibility::NeedsFinalizedRoot { epoch: 101, root: [8; 32] });
                            break;
                        }
                    },
                    event = listener.next_event() => {
                        if let TransportEvent::Rejected { reason, .. } = event {
                            panic!("actual compatible status was rejected: {reason:?}");
                        }
                    },
                }
            }
        }).await.unwrap();
        assert_eq!(dialer.local_finalized, original_finalized);
        assert_eq!(dialer.local_status, original_status);
        listener.shutdown();
        dialer.shutdown();
    }
}
