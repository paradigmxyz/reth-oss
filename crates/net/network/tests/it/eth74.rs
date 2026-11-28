//! EIP-8512 exchange over negotiated eth sessions.

use alloy_consensus::Header;
use alloy_primitives::{keccak256, Bytes, B256};
use reth_eth_wire::{DependencyProofs, EthVersion, GetDependencyProofs};
use reth_network::{
    eth_requests::EthRequestHandler,
    test_utils::{PeerConfig, Testnet},
    transactions::dependencies::PeerDependencyProofFetcher,
    PeerRequest,
};
use reth_provider::{
    test_utils::MockEthProvider, DependencyProofFetcher, DependencyProofProvider,
    DependencyProofRequest, DependencyProofStore, ProviderError, ProviderResult,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct Proofs(Vec<(B256, Bytes)>);

impl DependencyProofProvider for Proofs {
    fn dependency_proof(&self, hash: B256) -> ProviderResult<Option<Bytes>> {
        Ok(self.0.iter().find(|(key, _)| *key == hash).map(|(_, proof)| proof.clone()))
    }
    fn insert_dependency_proof(&self, _: B256, _: Bytes) -> ProviderResult<()> {
        Err(ProviderError::UnsupportedProvider)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn eth74_sync_fetcher_and_positional_serving() {
    let provider = Arc::new(MockEthProvider::default());
    let proof = Bytes::from_static(b"proof served over eth/74");
    let hash = B256::repeat_byte(1);
    let missing = B256::repeat_byte(2);
    let empty = B256::repeat_byte(3);
    for (hash, commitment) in
        [(hash, keccak256(&proof)), (missing, keccak256(b"missing")), (empty, keccak256(b""))]
    {
        provider.add_header(
            hash,
            Header {
                dependency_proof_hash: Some(commitment),
                block_deps_hash: Some(B256::ZERO),
                ..Default::default()
            },
        );
    }
    let mut net = Testnet::from_configs([
        PeerConfig::new(provider.clone()).with_protocols([EthVersion::Eth74]),
        PeerConfig::new(provider.clone()).with_protocols([EthVersion::Eth74]),
    ])
    .await;
    let (fetcher, downloader) = PeerDependencyProofFetcher::new(net.peers()[0].handle());
    let downloader = tokio::spawn(downloader);
    let mut handlers = Vec::new();
    for peer in net.peers_mut() {
        let (sender, incoming) = mpsc::channel(16);
        peer.network_mut().set_eth_request_handler(sender);
        handlers.push(tokio::spawn(
            EthRequestHandler::new(provider.clone(), incoming).with_dependency_proof_store(
                DependencyProofStore::new(Proofs(vec![
                    (hash, proof.clone()),
                    (empty, Bytes::new()),
                ])),
            ),
        ));
    }
    let net = net.spawn();
    net.connect_peers().await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(10),
            fetcher
                .fetch(DependencyProofRequest { block_hash: hash, commitment: keccak256(&proof) })
        )
        .await
        .unwrap()
        .unwrap(),
        Some(proof.clone())
    );
    // The opposite direction has its own quota, independent of the prior fetch.
    let [responder, requester] = net.peers_array();
    let response = requester
        .request(*responder.peer_id(), |response| PeerRequest::GetDependencyProofs {
            request: GetDependencyProofs(vec![B256::ZERO, hash, missing, empty]),
            response,
        })
        .await
        .unwrap();
    assert_eq!(response, DependencyProofs(vec![Bytes::new(), proof, Bytes::new(), Bytes::new()]));
    for handler in handlers {
        handler.abort();
        assert!(handler.await.unwrap_err().is_cancelled());
    }
    downloader.abort();
    assert!(downloader.await.unwrap_err().is_cancelled());
}
