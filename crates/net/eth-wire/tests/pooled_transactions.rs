//! Decoding tests for [`PooledTransactions`]

use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::hex;
use alloy_rlp::{Decodable, Encodable, Header};
use reth_eth_wire::{EthNetworkPrimitives, EthVersion, PooledTransactions, ProtocolMessage};
use std::{fs, path::PathBuf};
use test_fuzz::test_fuzz;

/// Pre-Osaka pooled transaction type using EIP-4844 sidecar format.
/// Test fixtures were generated with this format.
type PreOsakaPooledTransaction = alloy_consensus::EthereumTxEnvelope<
    alloy_consensus::TxEip4844WithSidecar<alloy_eips::eip4844::BlobTransactionSidecar>,
>;

/// Helper function to ensure encode-decode roundtrip works for [`PooledTransactions`].
#[test_fuzz]
fn roundtrip_pooled_transactions(hex_data: Vec<u8>) -> Result<(), alloy_rlp::Error> {
    let input_rlp = &mut &hex_data[..];
    let txs: PooledTransactions<PreOsakaPooledTransaction> = PooledTransactions::decode(input_rlp)?;

    // get the amount of bytes decoded in `decode` by subtracting the length of the original buf,
    // from the length of the remaining bytes
    let decoded_len = hex_data.len() - input_rlp.len();
    let expected_encoding = hex_data[..decoded_len].to_vec();

    // do a roundtrip test
    let mut buf = Vec::new();
    txs.encode(&mut buf);
    assert_eq!(expected_encoding, buf);

    // now do another decoding, on what we encoded - this should succeed
    let txs2: PooledTransactions<PreOsakaPooledTransaction> =
        PooledTransactions::decode(&mut &buf[..]).unwrap();

    // ensure that the payload length is the same
    assert_eq!(txs.length(), txs2.length());

    // ensure that the length is equal to the length of the encoded data
    assert_eq!(txs.length(), buf.len());

    Ok(())
}

#[test]
fn decode_pooled_transactions_data() {
    let network_data_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/pooled_transactions_with_blob");
    let data = fs::read_to_string(network_data_path).expect("Unable to read file");
    let hex_data = hex::decode(data.trim()).expect("Unable to decode hex");
    assert!(roundtrip_pooled_transactions(hex_data).is_ok());
}

#[test]
fn decode_request_pair_pooled_blob_transactions() {
    let network_data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/request_pair_pooled_blob_transactions");
    let data = fs::read_to_string(network_data_path).expect("Unable to read file");
    let hex_data = hex::decode(data.trim()).unwrap();
    let _txs: ProtocolMessage<EthNetworkPrimitives> =
        ProtocolMessage::decode_message(EthVersion::Eth68, &mut &hex_data[..]).unwrap();
}

#[test]
fn decode_blob_transaction_data() {
    let network_data_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/blob_transaction");
    let data = fs::read_to_string(network_data_path).expect("Unable to read file");
    let hex_data = hex::decode(data.trim()).unwrap();
    let _txs = PreOsakaPooledTransaction::decode(&mut &hex_data[..]).unwrap();
}

#[test]
fn decode_blob_rpc_transaction() {
    // test data pulled from hive test that sends blob transactions
    let network_data_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/rpc_blob_transaction");
    let data = fs::read_to_string(network_data_path).expect("Unable to read file");
    let hex_data = hex::decode(data.trim()).unwrap();
    let _txs = PreOsakaPooledTransaction::decode_2718(&mut hex_data.as_ref()).unwrap();
}

#[test]
fn encode_eth72_pooled_transactions_elides_blob_payloads() {
    let network_data_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/rpc_blob_transaction");
    let data = fs::read_to_string(network_data_path).expect("Unable to read file");
    let hex_data = hex::decode(data.trim()).unwrap();
    let tx = PreOsakaPooledTransaction::decode_2718(&mut hex_data.as_ref()).unwrap();

    let mut normal_tx = Vec::new();
    tx.encode_2718(&mut normal_tx);

    let mut eth72 = Vec::new();
    PooledTransactions(vec![tx]).encode_eth72(&mut eth72);

    let eth72_tx = single_pooled_transaction_payload(&eth72);
    let normal_sidecar = eip4844_sidecar_payload(&normal_tx);
    let eth72_sidecar = eip4844_sidecar_payload(eth72_tx);
    let normal_blobs_len = rlp_item_length(normal_sidecar);

    assert_eq!(eth72_sidecar[0], alloy_rlp::EMPTY_STRING_CODE);
    assert_eq!(&normal_sidecar[normal_blobs_len..], &eth72_sidecar[1..]);

    let decoded = PooledTransactions::<PreOsakaPooledTransaction>::decode_eth72_with_memory_budget(
        &mut &eth72[..],
        usize::MAX,
    );
    assert!(decoded.is_ok());
}

fn single_pooled_transaction_payload(encoded: &[u8]) -> &[u8] {
    let mut payload = encoded;
    let header = Header::decode(&mut payload).unwrap();
    assert!(header.list);
    payload
}

fn eip4844_sidecar_payload(encoded_tx: &[u8]) -> &[u8] {
    assert_eq!(encoded_tx[0], 0x03);
    let mut payload = &encoded_tx[1..];
    let header = Header::decode(&mut payload).unwrap();
    assert!(header.list);
    let signed_tx_len = rlp_item_length(payload);
    &payload[signed_tx_len..]
}

fn rlp_item_length(buf: &[u8]) -> usize {
    if buf[0] <= 0x7f {
        return 1
    }

    let mut tmp = buf;
    let header = Header::decode(&mut tmp).unwrap();
    header.length() + header.payload_length
}
