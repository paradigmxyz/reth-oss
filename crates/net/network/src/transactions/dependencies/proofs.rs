//! Bounded, commitment-authenticated EIP-8512 retrieval for block synchronization.

use crate::{eth_requests::DEPENDENCY_PROOF_REQUEST_INTERVAL, NetworkHandle};
use alloy_primitives::{map::B256Map, Bytes};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use reth_eth_wire::{
    DependencyProofs, DisconnectReason, EthVersion, GetDependencyProofs, NetworkPrimitives,
};
use reth_network_api::{
    events::{NetworkEvent, PeerEvent, PeerRequest, PeerRequestSender},
    NetworkEventListenerProvider, PeerId, Peers,
};
use reth_network_p2p::error::RequestResult;
use reth_storage_api::{DependencyProofFetcher, DependencyProofRequest};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, warn};

const CAPACITY: usize = 16;
const MAX_PEERS: usize = 128;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PEER_TIMEOUT: Duration = Duration::from_secs(3);

/// A bounded source of eth/74 proofs authenticated against the requested header commitment.
#[derive(Debug)]
pub struct PeerDependencyProofFetcher(mpsc::Sender<Request>);

impl PeerDependencyProofFetcher {
    /// Subscribes before peers connect. Run the returned task for the node's lifetime.
    pub fn new<N: NetworkPrimitives>(
        network: NetworkHandle<N>,
    ) -> (Arc<Self>, BoxFuture<'static, ()>) {
        let events = network.event_listener();
        let (sender, requests) = mpsc::channel(CAPACITY);
        (Arc::new(Self(sender)), run(network, events, requests).boxed())
    }
}

impl DependencyProofFetcher for PeerDependencyProofFetcher {
    fn fetch(&self, request: DependencyProofRequest) -> reth_storage_api::DependencyProofFuture {
        let sender = self.0.clone();
        async move {
            let (response, receiver) = oneshot::channel();
            if sender.try_send(Request { request, response }).is_err() {
                return Ok(None)
            }
            Ok(
                match tokio::time::timeout(REQUEST_TIMEOUT + Duration::from_secs(1), receiver).await
                {
                    Ok(Ok(proof)) => proof,
                    Ok(Err(_)) | Err(_) => None,
                },
            )
        }
        .boxed()
    }
}

#[derive(Debug)]
struct Request {
    request: DependencyProofRequest,
    response: oneshot::Sender<Option<Bytes>>,
}

struct Pending {
    request: DependencyProofRequest,
    responses: Vec<oneshot::Sender<Option<Bytes>>>,
    tried: BTreeSet<PeerId>,
    peer: Option<PeerId>,
    deadline: Instant,
}

async fn run<N: NetworkPrimitives>(
    network: NetworkHandle<N>,
    mut events: impl futures::Stream<Item = NetworkEvent<PeerRequest<N>>> + Unpin,
    mut requests: mpsc::Receiver<Request>,
) {
    let mut peers = BTreeMap::<PeerId, PeerRequestSender<PeerRequest<N>>>::new();
    let mut pending = B256Map::<Pending>::default();
    let mut downloads = FuturesUnordered::<
        BoxFuture<'static, (Vec<DependencyProofRequest>, PeerId, RequestResult<DependencyProofs>)>,
    >::new();
    let mut next_request = BTreeMap::<PeerId, Instant>::new();
    // Preserve rotation across fetches so silent peers cannot monopolize every deadline.
    let mut last_peer = None;
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(Request { request, response }) = request else { return };
                if let Some(existing) = pending.get_mut(&request.block_hash) {
                    if existing.request.commitment == request.commitment && existing.responses.len() < CAPACITY {
                        existing.responses.push(response);
                    }
                } else if pending.len() < CAPACITY {
                    pending.insert(request.block_hash, Pending {
                        request, responses: vec![response], tried: BTreeSet::new(), peer: None,
                        deadline: Instant::now() + REQUEST_TIMEOUT,
                    });
                }
            },
            event = events.next() => match event {
                None => return,
                Some(NetworkEvent::ActivePeerSession { info, messages }) => {
                    peers.retain(|_, sender| !sender.to_session_tx.is_closed());
                    if info.version >= EthVersion::Eth74 && peers.len() < MAX_PEERS {
                        peers.insert(info.peer_id, messages);
                    }
                },
                Some(NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, .. })) => { peers.remove(&peer_id); },
                Some(NetworkEvent::Peer(_)) => {},
            },
            Some((batch, peer, result)) = downloads.next(), if !downloads.is_empty() => {
                let proofs = match result {
                    Ok(proofs) => {
                        let commitments = batch.iter().map(|request: &DependencyProofRequest| request.commitment).collect::<Vec<_>>();
                        if let Err(error) = proofs.validate_response(&commitments) {
                            warn!(target: "net::eth", %peer, %error, "Invalid dependency proof response");
                            network.disconnect_peer_with_reason(peer, DisconnectReason::SubprotocolSpecific);
                            peers.remove(&peer);
                            None
                        } else { Some(proofs) }
                    },
                    Err(error) => {
                        debug!(target: "net::eth", %peer, %error, "Dependency proof request failed");
                        None
                    },
                };
                let mut proofs = proofs.map(|proofs| proofs.0.into_iter());
                for request in batch {
                    let proof = proofs.as_mut().and_then(Iterator::next);
                    if let Some(download) = pending.get_mut(&request.block_hash) &&
                        download.peer == Some(peer) && download.request.commitment == request.commitment
                    {
                        download.peer = None;
                        if let Some(proof) = proof {
                            if (!proof.is_empty() || request.commitment == alloy_consensus::constants::KECCAK_EMPTY) &&
                                let Some(download) = pending.remove(&request.block_hash)
                            {
                                for response in download.responses {
                                    if response.send(Some(proof.clone())).is_err() {
                                        debug!(target: "net::eth", "Proof requester left");
                                    }
                                }
                            }
                        } else if proofs.is_some() {
                            // Truncated suffixes were not served: retry this peer after its quota resets.
                            download.tried.remove(&peer);
                        }
                    }
                }
            },
            _ = interval.tick() => {
                peers.retain(|_, sender| !sender.to_session_tx.is_closed());
                next_request.retain(|peer, _| peers.contains_key(peer));
                pending.retain(|_, download| {
                    download.responses.retain(|response| !response.is_closed());
                    !download.responses.is_empty() && download.deadline > Instant::now()
                });
            },
        }
        // Coalesce concurrent block requests to share peer quotas and avoid per-proof round trips.
        let start = last_peer.map_or(Unbounded, Excluded);
        for (peer, sender) in peers.range((start, Unbounded)).chain(peers.iter()) {
            if downloads.len() >= CAPACITY ||
                next_request.get(peer).is_some_and(|until| *until > Instant::now())
            {
                continue
            }
            let batch = pending
                .values()
                .filter(|download| {
                    download.peer.is_none() &&
                        download.tried.len() < MAX_PEERS &&
                        !download.tried.contains(peer)
                })
                .map(|download| download.request)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                continue
            }
            let (response, receiver) = oneshot::channel();
            let request = PeerRequest::GetDependencyProofs {
                request: GetDependencyProofs(
                    batch.iter().map(|request| request.block_hash).collect(),
                ),
                response,
            };
            // Full session queues are transient and do not consume the peer's retry attempt.
            if sender.try_send(request).is_ok() {
                for request in &batch {
                    if let Some(download) = pending.get_mut(&request.block_hash) {
                        download.tried.insert(*peer);
                        download.peer = Some(*peer);
                    }
                }
                next_request.insert(*peer, Instant::now() + DEPENDENCY_PROOF_REQUEST_INTERVAL);
                last_peer = Some(*peer);
                let peer = *peer;
                downloads.push(
                    async move {
                        let result = match tokio::time::timeout(PEER_TIMEOUT, receiver).await {
                            Ok(Ok(result)) => result,
                            Ok(Err(_)) => Err(reth_network_p2p::error::RequestError::ChannelClosed),
                            Err(_) => Err(reth_network_p2p::error::RequestError::Timeout),
                        };
                        (batch, peer, result)
                    }
                    .boxed(),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::PeerConfig;
    use alloy_primitives::{keccak256, B256};
    use reth_eth_wire::{Capabilities, DependencyProofs};
    use reth_network_api::events::SessionInfo;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    fn session(peer: PeerId, version: EthVersion) -> (NetworkEvent, mpsc::Receiver<PeerRequest>) {
        let (sender, receiver) = mpsc::channel(2);
        (
            NetworkEvent::ActivePeerSession {
                info: SessionInfo {
                    peer_id: peer,
                    remote_addr: "127.0.0.1:1".parse().unwrap(),
                    client_version: "test".into(),
                    capabilities: Arc::new(Capabilities::new(vec![
                        reth_eth_wire::Capability::eth(version),
                    ])),
                    status: Arc::new(Default::default()),
                    version,
                    peer_kind: Default::default(),
                },
                messages: PeerRequestSender::new(peer, sender),
            },
            receiver,
        )
    }

    async fn request(
        receiver: &mut mpsc::Receiver<PeerRequest>,
    ) -> (
        GetDependencyProofs,
        oneshot::Sender<reth_network_p2p::error::RequestResult<DependencyProofs>>,
    ) {
        let message =
            tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await.unwrap().unwrap();
        let PeerRequest::GetDependencyProofs { request, response } = message else {
            panic!("expected dependency proof request")
        };
        (request, response)
    }

    #[tokio::test]
    async fn unavailable_invalid_and_truncated_responses_retry() {
        for mode in 0..4 {
            let peer = PeerConfig::default().launch().await.unwrap();
            let network = peer.handle();
            let (events, incoming) = mpsc::unbounded_channel();
            let (sender, requests) = mpsc::channel(CAPACITY);
            let fetcher = Arc::new(PeerDependencyProofFetcher(sender));
            let (first, mut first_messages) = session(PeerId::repeat_byte(1), EthVersion::Eth74);
            let (second, mut second_messages) = session(PeerId::repeat_byte(2), EthVersion::Eth74);
            events.send(first).unwrap();
            events.send(second).unwrap();
            let driver =
                tokio::spawn(run(network, UnboundedReceiverStream::new(incoming), requests));
            let proof = Bytes::from_static(b"authenticated proof");
            let expected = DependencyProofRequest {
                block_hash: B256::repeat_byte(3),
                commitment: keccak256(&proof),
            };
            let requester = Arc::clone(&fetcher);
            let fetched = tokio::spawn(async move { requester.fetch(expected).await });
            let (hashes, response) = request(&mut first_messages).await;
            assert_eq!(hashes.0, vec![expected.block_hash]);
            let bad = match mode {
                0 => DependencyProofs(vec![Bytes::new()]),
                1 => DependencyProofs(vec![Bytes::from_static(b"wrong")]),
                2 => DependencyProofs(vec![]),
                _ => DependencyProofs(vec![proof.clone(), Bytes::new()]),
            };
            response.send(Ok(bad)).unwrap();
            let (_, response) = request(&mut second_messages).await;
            response.send(Ok(DependencyProofs(vec![proof.clone()]))).unwrap();
            assert_eq!(fetched.await.unwrap().unwrap(), Some(proof));
            assert_eq!(first_messages.is_closed(), matches!(mode, 1 | 3));
            driver.abort();
            assert!(driver.await.unwrap_err().is_cancelled());
        }
    }

    #[tokio::test]
    async fn truncated_suffix_is_retried_without_losing_batch_positions() {
        let peer = PeerConfig::default().launch().await.unwrap();
        let (events, incoming) = mpsc::unbounded_channel();
        let (sender, requests) = mpsc::channel(CAPACITY);
        let mut responses = Vec::new();
        for index in 0..3 {
            let (response, receiver) = oneshot::channel();
            sender
                .send(Request {
                    request: DependencyProofRequest {
                        block_hash: B256::repeat_byte(index),
                        commitment: keccak256([index]),
                    },
                    response,
                })
                .await
                .unwrap();
            responses.push(receiver);
        }
        let driver =
            tokio::spawn(run(peer.handle(), UnboundedReceiverStream::new(incoming), requests));
        // Wait until the driver has received the complete batch before announcing a peer.
        tokio::time::timeout(Duration::from_secs(5), async {
            while sender.capacity() != CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (event, mut messages) = session(PeerId::repeat_byte(1), EthVersion::Eth74);
        events.send(event).unwrap();
        let (hashes, response) = request(&mut messages).await;
        assert_eq!(hashes.0.len(), 3);
        response
            .send(Ok(DependencyProofs(vec![Bytes::copy_from_slice(&hashes.0[0][..1])])))
            .unwrap();
        let (retry, response) = request(&mut messages).await;
        assert_eq!(retry.0, hashes.0[1..]);
        response
            .send(Ok(DependencyProofs(
                retry.0.iter().map(|hash| Bytes::copy_from_slice(&hash[..1])).collect(),
            )))
            .unwrap();
        for (index, receiver) in responses.into_iter().enumerate() {
            assert_eq!(
                receiver.await.unwrap(),
                Some(Bytes::from(vec![u8::try_from(index).unwrap()]))
            );
        }
        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn older_sessions_are_skipped_and_empty_proof_is_available() {
        let peer = PeerConfig::default().launch().await.unwrap();
        let (events, incoming) = mpsc::unbounded_channel();
        let (sender, requests) = mpsc::channel(CAPACITY);
        let fetcher = Arc::new(PeerDependencyProofFetcher(sender));
        let (old, mut old_messages) = session(PeerId::repeat_byte(1), EthVersion::Eth73);
        let (new, mut new_messages) = session(PeerId::repeat_byte(2), EthVersion::Eth74);
        events.send(old).unwrap();
        events.send(new).unwrap();
        let driver =
            tokio::spawn(run(peer.handle(), UnboundedReceiverStream::new(incoming), requests));
        let requester = Arc::clone(&fetcher);
        let fetched = tokio::spawn(async move {
            requester
                .fetch(DependencyProofRequest {
                    block_hash: B256::ZERO,
                    commitment: keccak256(b""),
                })
                .await
        });
        let (_, response) = request(&mut new_messages).await;
        response.send(Ok(DependencyProofs(vec![Bytes::new()]))).unwrap();
        assert_eq!(fetched.await.unwrap().unwrap(), Some(Bytes::new()));
        assert!(matches!(old_messages.try_recv(), Err(mpsc::error::TryRecvError::Disconnected)));
        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn proof_fetch_rotation_survives_peer_cooldown() {
        let peer = PeerConfig::default().launch().await.unwrap();
        let (events, incoming) = mpsc::unbounded_channel();
        let (sender, requests) = mpsc::channel(CAPACITY);
        let fetcher = Arc::new(PeerDependencyProofFetcher(sender));
        let (first, mut first_messages) = session(PeerId::repeat_byte(1), EthVersion::Eth74);
        let (second, mut second_messages) = session(PeerId::repeat_byte(2), EthVersion::Eth74);
        events.send(first).unwrap();
        events.send(second).unwrap();
        let driver =
            tokio::spawn(run(peer.handle(), UnboundedReceiverStream::new(incoming), requests));
        for messages in [&mut first_messages, &mut second_messages] {
            let requester = Arc::clone(&fetcher);
            let proof = Bytes::from_static(b"authenticated proof");
            let expected = DependencyProofRequest {
                block_hash: B256::repeat_byte(3),
                commitment: keccak256(&proof),
            };
            let fetched = tokio::spawn(async move { requester.fetch(expected).await });
            let (hashes, response) = request(messages).await;
            assert_eq!(hashes.0, vec![expected.block_hash]);
            response.send(Ok(DependencyProofs(vec![proof.clone()]))).unwrap();
            assert_eq!(fetched.await.unwrap().unwrap(), Some(proof));
            // The next fetch must rotate even after the first peer's quota resets.
            tokio::time::sleep(DEPENDENCY_PROOF_REQUEST_INTERVAL).await;
        }
        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }
}
