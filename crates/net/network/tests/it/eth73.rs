//! Eth/73 transaction announcements and metadata validation over a real peer session.

use crate::utils::{funded_transaction, poll_until};
use alloy_consensus::Transaction;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::Address;
use futures::{SinkExt, StreamExt};
use reth_chainspec::MAINNET;
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{
    message::RequestPair, EthMessage, EthNetworkPrimitives, EthStream, EthVersion,
    HelloMessageWithProtocols, NewPooledTransactionHashes73, P2PStream, PooledTransactions,
    StatusBuilder, UnauthedEthStream, UnauthedP2PStream,
};
use reth_ethereum_forks::EthereumHardfork;
use reth_ethereum_primitives::PooledTransactionVariant;
use reth_network::{
    config::rng_secret_key,
    test_utils::{PeerConfig, Testnet},
    transactions::config::{TransactionPropagationMode, TransactionsManagerConfig},
};
use reth_network_peers::pk2id;
use reth_network_types::ReputationChangeWeights;
use reth_transaction_pool::{PoolTransaction, TransactionPool};
use secp256k1::SECP256K1;
use std::time::Duration;
use tokio::net::TcpStream;

type RawEthStream = EthStream<P2PStream<ECIESStream<TcpStream>>, EthNetworkPrimitives>;

async fn next_message(stream: &mut RawEthStream) -> EthMessage<EthNetworkPrimitives> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("timed out awaiting eth message")
            .expect("stream terminated")
            .expect("stream errored");
        if !matches!(message, EthMessage::BlockRangeUpdate(_)) {
            return message
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn eth73_announcements_and_metadata_violations() {
    reth_tracing::init_test_tracing();
    let provider = reth_provider::test_utils::MockEthProvider::default().with_genesis_block();
    let net = Testnet::from_configs([
        PeerConfig::new(provider.clone()).with_protocols([EthVersion::Eth73])
    ])
    .await
    .with_eth_pool_config(TransactionsManagerConfig {
        propagation_mode: TransactionPropagationMode::Max(0),
        ..Default::default()
    })
    .spawn();
    let [node] = net.peers_array();
    let pool = node.pool().unwrap();
    let mut events = node.event_stream();
    let raw_key = rng_secret_key();
    let raw_id = pk2id(&raw_key.public_key(SECP256K1));
    let tcp = TcpStream::connect(node.local_addr()).await.unwrap();
    let ecies = ECIESStream::connect(tcp, raw_key, *node.peer_id()).await.unwrap();
    let hello = HelloMessageWithProtocols::builder(raw_id)
        .protocols(vec![EthVersion::Eth73.into()])
        .build();
    let (p2p, _) = UnauthedP2PStream::new(ecies).handshake(hello).await.unwrap();
    assert_eq!(p2p.shared_capabilities().eth_version(), Some(EthVersion::Eth73));
    let status = StatusBuilder::default().version(EthVersion::Eth73).build();
    let fork_filter = MAINNET.hardfork_fork_filter(EthereumHardfork::Frontier).unwrap();
    let (mut stream, _) = UnauthedEthStream::new(p2p)
        .handshake::<EthNetworkPrimitives>(status, fork_filter)
        .await
        .unwrap();
    assert_eq!(events.next_session_established().await.unwrap(), raw_id);
    let mut reputation = node.peer_handle().peer_by_id(raw_id).await.unwrap().reputation();

    let pending = funded_transaction(&provider);
    let source = pending.sender();
    let nonce = pending.nonce();
    let pending_hash = pool.add_external_transaction(pending).await.unwrap().hash;
    let EthMessage::NewPooledTransactionHashes73(announcement) = next_message(&mut stream).await
    else {
        panic!("expected eth/73 announcement")
    };
    assert_eq!(announcement.hashes, vec![pending_hash]);
    assert_eq!(announcement.tx_sources, vec![source]);
    assert_eq!(announcement.tx_nonces, vec![nonce]);
    assert_eq!(announcement.cell_mask, None);

    // A valid announcement must survive validation. Each field mismatch must penalize the
    // announcer while the valid transaction body remains eligible for pool admission.
    for corruption in 0..5 {
        let tx = funded_transaction(&provider);
        let hash = *tx.hash();
        let mut announcement = NewPooledTransactionHashes73 {
            types: vec![tx.ty()],
            sizes: vec![tx.encoded_length()],
            hashes: vec![hash],
            cell_mask: None,
            tx_sources: vec![tx.sender()],
            tx_nonces: vec![tx.nonce()],
        };
        match corruption {
            1 => announcement.tx_sources[0] = Address::ZERO,
            2 => announcement.tx_nonces[0] += 1,
            3 => announcement.types[0] = 1,
            4 => announcement.sizes[0] += 1,
            _ => {}
        }
        stream.send(EthMessage::NewPooledTransactionHashes73(announcement)).await.unwrap();
        let EthMessage::GetPooledTransactions(request) = next_message(&mut stream).await else {
            panic!("expected pooled transaction request")
        };
        assert_eq!(request.message.0, vec![hash]);
        let pooled = PooledTransactionVariant::try_from(tx.into_consensus().into_inner()).unwrap();
        stream
            .send(EthMessage::PooledTransactions(RequestPair {
                request_id: request.request_id,
                message: PooledTransactions(vec![pooled]),
            }))
            .await
            .unwrap();
        poll_until("fetched transaction imported", async || pool.contains(&hash).then_some(()))
            .await;
        if corruption > 0 {
            reputation += ReputationChangeWeights::default().bad_announcement;
        }
        poll_until("announcement reputation applied", async || {
            let actual = node.peer_handle().peer_by_id(raw_id).await.unwrap().reputation();
            (actual == reputation).then_some(())
        })
        .await;
    }
}
