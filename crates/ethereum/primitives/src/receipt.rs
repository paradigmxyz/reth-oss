use alloc::vec::Vec;
use alloy_consensus::{
    Eip2718DecodableReceipt, Eip2718EncodableReceipt, Eip658Value, InMemorySize, ReceiptWithBloom,
    RlpDecodableReceipt, RlpEncodableReceipt, TxReceipt, TxType,
};
pub use alloy_consensus::{EthereumReceipt, ReceiptEnvelope, TxTy};
use alloy_eips::{
    eip2718::{Decodable2718, Eip2718Result, Encodable2718, IsTyped2718, Typed2718},
    eip8141::FrameReceiptPayload,
};
use alloy_primitives::{logs_bloom, Bloom, Log, B256};
use alloy_rlp::{BufMut, Decodable, Encodable, Header};
use reth_primitives_traits::proofs::ordered_trie_root_with_encoder;

/// Standard Ethereum receipt data shared by legacy and typed transactions.
pub type StandardReceipt = alloy_consensus::EthereumReceiptData<TxType, Log>;

/// Reth's storage receipt wrapper around Alloy's consensus receipt enum.
///
/// The additional flattened frame log vector is derived data required by the
/// `TxReceipt` interface and Reth's storage/indexing paths. The consensus
/// representation remains owned by Alloy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    inner: EthereumReceipt<TxType, Log>,
}

impl Default for Receipt {
    fn default() -> Self {
        Self::from_inner(EthereumReceipt::default())
    }
}

impl Receipt {
    fn from_inner(inner: EthereumReceipt<TxType, Log>) -> Self {
        Self { inner }
    }

    /// Constructs a standard receipt variant.
    pub const fn standard(
        tx_type: TxType,
        success: bool,
        cumulative_gas_used: u64,
        logs: Vec<Log>,
    ) -> Self {
        Self {
            inner: EthereumReceipt::Standard(StandardReceipt {
                tx_type,
                success,
                cumulative_gas_used,
                logs,
            }),
        }
    }
    /// Converts a consensus receipt envelope into the Reth storage representation.
    pub fn from_envelope(envelope: ReceiptEnvelope) -> Self {
        Self::from_inner(envelope.into())
    }

    /// Converts this receipt into its consensus envelope.
    pub fn to_envelope(&self) -> ReceiptEnvelope {
        match &self.inner {
            EthereumReceipt::Standard(data) => data.clone().into(),
            EthereumReceipt::Frame { payload, .. } => ReceiptEnvelope::Eip8141(payload.clone()),
        }
    }

    /// Returns the EIP-8141 frame receipt payload, if this is a frame transaction receipt.
    pub const fn as_eip8141(&self) -> Option<&FrameReceiptPayload<Log>> {
        match self {
            Self { inner: EthereumReceipt::Frame { payload, .. }, .. } => Some(payload),
            Self { inner: EthereumReceipt::Standard(_), .. } => None,
        }
    }

    /// Returns the transaction type associated with this receipt.
    pub const fn tx_type(&self) -> TxType {
        match &self.inner {
            EthereumReceipt::Standard(receipt) => receipt.tx_type,
            EthereumReceipt::Frame { .. } => TxType::Eip8141,
        }
    }

    /// Returns whether execution succeeded.
    pub const fn success(&self) -> bool {
        match &self.inner {
            EthereumReceipt::Standard(receipt) => receipt.success,
            EthereumReceipt::Frame { .. } => true,
        }
    }

    /// Returns the cumulative gas used by the transaction.
    pub const fn cumulative_gas_used(&self) -> u64 {
        match &self.inner {
            EthereumReceipt::Standard(receipt) => receipt.cumulative_gas_used,
            EthereumReceipt::Frame { payload, .. } => payload.cumulative_gas_used,
        }
    }

    /// Updates the cumulative gas used by this receipt.
    pub const fn set_cumulative_gas_used(&mut self, cumulative_gas_used: u64) {
        match &mut self.inner {
            EthereumReceipt::Standard(receipt) => receipt.cumulative_gas_used = cumulative_gas_used,
            EthereumReceipt::Frame { payload, .. } => {
                payload.cumulative_gas_used = cumulative_gas_used
            }
        }
    }

    /// Returns all logs emitted by the transaction.
    pub fn logs(&self) -> &[Log] {
        match &self.inner {
            EthereumReceipt::Standard(receipt) => &receipt.logs,
            EthereumReceipt::Frame { .. } => self.inner.logs(),
        }
    }

    fn rlp_payload_length(&self, bloom: &Bloom) -> usize {
        self.success().length() +
            self.cumulative_gas_used().length() +
            bloom.length() +
            self.logs().to_vec().length()
    }

    fn rlp_receipt_length(&self, bloom: &Bloom) -> usize {
        let payload_length = self.rlp_payload_length(bloom);
        Header { list: true, payload_length }.length() + payload_length
    }

    fn rlp_encode_receipt(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        Header { list: true, payload_length: self.rlp_payload_length(bloom) }.encode(out);
        self.success().encode(out);
        self.cumulative_gas_used().encode(out);
        bloom.encode(out);
        self.logs().to_vec().encode(out);
    }
}

impl From<ReceiptEnvelope> for Receipt {
    fn from(value: ReceiptEnvelope) -> Self {
        Self::from_envelope(value)
    }
}

impl From<Receipt> for ReceiptEnvelope {
    fn from(value: Receipt) -> Self {
        value.to_envelope()
    }
}

impl TxReceipt for Receipt {
    type Log = Log;

    fn status_or_post_state(&self) -> Eip658Value {
        self.success().into()
    }

    fn status(&self) -> bool {
        self.success()
    }

    fn bloom(&self) -> Bloom {
        logs_bloom(self.logs().iter())
    }

    fn cumulative_gas_used(&self) -> u64 {
        self.cumulative_gas_used()
    }

    fn logs(&self) -> &[Log] {
        self.logs()
    }

    fn into_logs(self) -> Vec<Log> {
        self.logs().to_vec()
    }
}

impl Typed2718 for Receipt {
    fn ty(&self) -> u8 {
        self.tx_type() as u8
    }
}

impl IsTyped2718 for Receipt {
    fn is_type(type_id: u8) -> bool {
        <TxType as IsTyped2718>::is_type(type_id)
    }
}

impl Encodable2718 for Receipt {
    fn encode_2718_len(&self) -> usize {
        self.eip2718_encoded_length_with_bloom(&self.bloom())
    }

    fn encode_2718(&self, out: &mut dyn BufMut) {
        self.eip2718_encode_with_bloom(&self.bloom(), out)
    }
}

impl Decodable2718 for Receipt {
    fn typed_decode(ty: u8, buf: &mut &[u8]) -> Eip2718Result<Self> {
        ReceiptEnvelope::typed_decode(ty, buf).map(Into::into)
    }

    fn fallback_decode(buf: &mut &[u8]) -> Eip2718Result<Self> {
        ReceiptEnvelope::fallback_decode(buf).map(Into::into)
    }
}

impl Encodable for Receipt {
    fn encode(&self, out: &mut dyn BufMut) {
        self.rlp_encode_with_bloom(&self.bloom(), out)
    }

    fn length(&self) -> usize {
        self.rlp_encoded_length_with_bloom(&self.bloom())
    }
}

impl Decodable for Receipt {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        ReceiptEnvelope::decode(buf).map(Into::into)
    }
}

impl RlpEncodableReceipt for Receipt {
    fn rlp_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        let payload_length = self.eip2718_encoded_length_with_bloom(bloom);
        if self.tx_type() == TxType::Legacy {
            payload_length
        } else {
            Header { list: false, payload_length }.length() + payload_length
        }
    }

    fn rlp_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        if self.tx_type() != TxType::Legacy {
            Header { list: false, payload_length: self.eip2718_encoded_length_with_bloom(bloom) }
                .encode(out);
        }
        self.eip2718_encode_with_bloom(bloom, out);
    }
}

impl RlpDecodableReceipt for Receipt {
    fn rlp_decode_with_bloom(buf: &mut &[u8]) -> alloy_rlp::Result<ReceiptWithBloom<Self>> {
        let ReceiptWithBloom { receipt, logs_bloom } =
            <ReceiptEnvelope as RlpDecodableReceipt>::rlp_decode_with_bloom(buf)?;
        Ok(ReceiptWithBloom { receipt: receipt.into(), logs_bloom })
    }
}

impl Eip2718EncodableReceipt for Receipt {
    fn eip2718_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        let type_len = usize::from(self.tx_type() != TxType::Legacy);
        type_len +
            if let Some(payload) = self.as_eip8141() {
                payload.length()
            } else {
                self.rlp_receipt_length(bloom)
            }
    }

    fn eip2718_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        if self.tx_type() != TxType::Legacy {
            out.put_u8(self.ty());
        }
        if let Some(payload) = self.as_eip8141() {
            payload.encode(out)
        } else {
            self.rlp_encode_receipt(bloom, out)
        }
    }
}

impl Eip2718DecodableReceipt for Receipt {
    fn typed_decode_with_bloom(ty: u8, buf: &mut &[u8]) -> Eip2718Result<ReceiptWithBloom<Self>> {
        let ReceiptWithBloom { receipt, logs_bloom } =
            <ReceiptEnvelope as Eip2718DecodableReceipt>::typed_decode_with_bloom(ty, buf)?;
        Ok(ReceiptWithBloom { receipt: receipt.into(), logs_bloom })
    }

    fn fallback_decode_with_bloom(buf: &mut &[u8]) -> Eip2718Result<ReceiptWithBloom<Self>> {
        let ReceiptWithBloom { receipt, logs_bloom } =
            <ReceiptEnvelope as Eip2718DecodableReceipt>::fallback_decode_with_bloom(buf)?;
        Ok(ReceiptWithBloom { receipt: receipt.into(), logs_bloom })
    }
}

impl InMemorySize for Receipt {
    fn size(&self) -> usize {
        core::mem::size_of::<Self>() +
            self.logs().iter().map(InMemorySize::size).sum::<usize>() +
            self.as_eip8141().map_or(0, |payload| {
                core::mem::size_of_val(payload) +
                    payload
                        .frame_receipts
                        .iter()
                        .map(|frame| {
                            core::mem::size_of_val(frame) +
                                frame.logs.iter().map(InMemorySize::size).sum::<usize>()
                        })
                        .sum::<usize>()
            })
    }
}

#[cfg(feature = "reth-codec")]
impl reth_codecs::Compact for Receipt {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        reth_codecs::Compact::to_compact(&self.to_envelope(), buf)
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let (receipt, buf) = <ReceiptEnvelope as reth_codecs::Compact>::from_compact(buf, len);
        (receipt.into(), buf)
    }
}

#[cfg(feature = "reth-codec")]
impl reth_codecs::Compress for Receipt {
    type Compressed = Vec<u8>;

    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        let _ = reth_codecs::Compact::to_compact(self, buf);
    }
}

#[cfg(feature = "reth-codec")]
impl reth_codecs::Decompress for Receipt {
    fn decompress(value: &[u8]) -> Result<Self, reth_codecs::DecompressError> {
        let (receipt, _) = reth_codecs::Compact::from_compact(value, value.len());
        Ok(receipt)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Receipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_envelope(), serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Receipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <ReceiptEnvelope as serde::Deserialize>::deserialize(deserializer).map(Into::into)
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Receipt {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        ReceiptEnvelope::arbitrary(u).map(Into::into)
    }
}

#[cfg(feature = "rpc")]
/// Receipt representation for RPC.
pub type RpcReceipt = ReceiptEnvelope<alloy_rpc_types_eth::Log>;

/// Calculates the receipt root for a header for the reference type of [`Receipt`].
///
/// NOTE: Prefer `proofs::calculate_receipt_root` if you have log blooms memoized.
pub fn calculate_receipt_root_no_memo(receipts: &[Receipt]) -> B256 {
    ordered_trie_root_with_encoder(receipts, |receipt, buf| receipt.encode_2718(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionSigned;
    use alloy_consensus::{ReceiptWithBloom, TxType};
    use alloy_eips::{
        eip2718::{Decodable2718, Encodable2718},
        eip8141::{FrameReceipt, FrameStatus},
    };
    #[cfg(feature = "reth-codec")]
    use alloy_primitives::Bytes;
    use alloy_primitives::{
        address, b256, bloom, bytes, hex_literal::hex, Address, Bloom, Log, LogData,
    };
    use alloy_rlp::{Decodable, Encodable};
    #[cfg(feature = "reth-codec")]
    use reth_codecs::Compact;
    use reth_primitives_traits::proofs::{
        calculate_receipt_root, calculate_transaction_root, calculate_withdrawals_root,
    };

    /// Ethereum full block.
    ///
    /// Withdrawals can be optionally included at the end of the RLP encoded message.
    pub(crate) type Block<T = TransactionSigned> = alloy_consensus::Block<T>;

    #[test]
    #[cfg(feature = "reth-codec")]
    fn test_decode_receipt() {
        reth_codecs::test_utils::test_decode::<Receipt>(&hex!(
            "c428b52ffd23fc42696156b10200f034792b6a94c3850215c2fef7aea361a0c31b79d9a32652eefc0d4e2e730036061cff7344b6fc6132b50cda0ed810a991ae58ef013150c12b2522533cb3b3a8b19b7786a8b5ff1d3cdc84225e22b02def168c8858df"
        ));
    }

    // Test vector from: https://eips.ethereum.org/EIPS/eip-2481
    #[test]
    fn encode_legacy_receipt() {
        let expected = hex!(
            "f901668001b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f85ff85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff"
        );

        let mut data = Vec::with_capacity(expected.length());
        let receipt = ReceiptWithBloom {
            receipt: Receipt::standard(
                TxType::Legacy,
                false,
                0x1u64,
                vec![Log::new_unchecked(
                    address!("0x0000000000000000000000000000000000000011"),
                    vec![
                        b256!("0x000000000000000000000000000000000000000000000000000000000000dead"),
                        b256!("0x000000000000000000000000000000000000000000000000000000000000beef"),
                    ],
                    bytes!("0100ff"),
                )],
            ),
            logs_bloom: [0; 256].into(),
        };

        receipt.encode(&mut data);

        // check that the rlp length equals the length of the expected rlp
        assert_eq!(receipt.length(), expected.len());
        assert_eq!(data, expected);
    }

    // Test vector from: https://eips.ethereum.org/EIPS/eip-2481
    #[test]
    fn decode_legacy_receipt() {
        let data = hex!(
            "f901668001b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f85ff85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff"
        );

        // EIP658Receipt
        let expected = ReceiptWithBloom {
            receipt: Receipt::standard(
                TxType::Legacy,
                false,
                0x1u64,
                vec![Log::new_unchecked(
                    address!("0x0000000000000000000000000000000000000011"),
                    vec![
                        b256!("0x000000000000000000000000000000000000000000000000000000000000dead"),
                        b256!("0x000000000000000000000000000000000000000000000000000000000000beef"),
                    ],
                    bytes!("0100ff"),
                )],
            ),
            logs_bloom: [0; 256].into(),
        };

        let receipt = ReceiptWithBloom::decode(&mut &data[..]).unwrap();
        assert_eq!(receipt, expected);
    }

    #[test]
    #[cfg(feature = "reth-codec")]
    fn gigantic_receipt() {
        let receipt = Receipt::standard(
            TxType::Legacy,
            true,
            16747627,
            vec![
                Log::new_unchecked(
                    address!("0x4bf56695415f725e43c3e04354b604bcfb6dfb6e"),
                    vec![b256!(
                        "0xc69dc3d7ebff79e41f525be431d5cd3cc08f80eaf0f7819054a726eeb7086eb9"
                    )],
                    Bytes::from(vec![1; 0xffffff]),
                ),
                Log::new_unchecked(
                    address!("0xfaca325c86bf9c2d5b413cd7b90b209be92229c2"),
                    vec![b256!(
                        "0x8cca58667b1e9ffa004720ac99a3d61a138181963b294d270d91c53d36402ae2"
                    )],
                    Bytes::from(vec![1; 0xffffff]),
                ),
            ],
        );

        let mut data = vec![];
        receipt.to_compact(&mut data);
        let (decoded, _) = Receipt::from_compact(&data[..], data.len());
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn frame_receipt_preserves_payload_and_exposes_logs() {
        let log = Log::new_unchecked(
            address!("0x0000000000000000000000000000000000000011"),
            vec![],
            bytes!("8141"),
        );
        let envelope = ReceiptEnvelope::Eip8141(FrameReceiptPayload {
            cumulative_gas_used: 42_000,
            payer: address!("0x0000000000000000000000000000000000000022"),
            frame_receipts: vec![FrameReceipt {
                status: FrameStatus::Success,
                gas_used: 21_000,
                logs: vec![log.clone()],
            }],
        });

        let receipt = Receipt::from_envelope(envelope.clone());
        assert_eq!(receipt.logs(), &[log]);
        assert_eq!(receipt.as_eip8141(), envelope.as_eip8141());

        let encoded = receipt.encoded_2718();
        let decoded = Receipt::decode_2718(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded, receipt);
        assert_eq!(decoded.to_envelope(), envelope);

        #[cfg(feature = "reth-codec")]
        {
            let mut compact = Vec::new();
            receipt.to_compact(&mut compact);
            let (decoded, remaining) = Receipt::from_compact(&compact, compact.len());
            assert!(remaining.is_empty());
            assert_eq!(decoded, receipt);
        }
    }

    #[test]
    fn test_encode_2718_length() {
        let receipt = ReceiptWithBloom {
            receipt: Receipt::standard(TxType::Eip1559, true, 21000, vec![]),
            logs_bloom: Bloom::default(),
        };

        let encoded = receipt.encoded_2718();
        assert_eq!(
            encoded.len(),
            receipt.encode_2718_len(),
            "Encoded length should match the actual encoded data length"
        );

        // Test for legacy receipt as well
        let legacy_receipt = ReceiptWithBloom {
            receipt: Receipt::standard(TxType::Legacy, true, 21000, vec![]),
            logs_bloom: Bloom::default(),
        };

        let legacy_encoded = legacy_receipt.encoded_2718();
        assert_eq!(
            legacy_encoded.len(),
            legacy_receipt.encode_2718_len(),
            "Encoded length for legacy receipt should match the actual encoded data length"
        );
    }

    #[test]
    fn check_transaction_root() {
        let data = &hex!(
            "f90262f901f9a092230ce5476ae868e98c7979cfc165a93f8b6ad1922acf2df62e340916efd49da01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347942adc25665018aa1fe0e6bc666dac8fc2697ff9baa02307107a867056ca33b5087e77c4174f47625e48fb49f1c70ced34890ddd88f3a08151d548273f6683169524b66ca9fe338b9ce42bc3540046c828fd939ae23bcba0c598f69a5674cae9337261b669970e24abc0b46e6d284372a239ec8ccbf20b0ab901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000083020000018502540be40082a8618203e800a00000000000000000000000000000000000000000000000000000000000000000880000000000000000f863f861800a8405f5e10094100000000000000000000000000000000000000080801ba07e09e26678ed4fac08a249ebe8ed680bf9051a5e14ad223e4b2b9d26e0208f37a05f6e3f188e3e6eab7d7d3b6568f5eac7d687b08d307d3154ccd8c87b4630509bc0"
        );
        let block_rlp = &mut data.as_slice();
        let block: Block = Block::decode(block_rlp).unwrap();

        let tx_root = calculate_transaction_root(&block.body.transactions);
        assert_eq!(block.transactions_root, tx_root, "Must be the same");
    }

    #[test]
    fn check_withdrawals_root() {
        // Single withdrawal, amount 0
        // https://github.com/ethereum/tests/blob/9760400e667eba241265016b02644ef62ab55de2/BlockchainTests/EIPTests/bc4895-withdrawals/amountIs0.json
        let data = &hex!(
            "f90238f90219a0151934ad9b654c50197f37018ee5ee9bb922dec0a1b5e24a6d679cb111cdb107a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347942adc25665018aa1fe0e6bc666dac8fc2697ff9baa0046119afb1ab36aaa8f66088677ed96cd62762f6d3e65642898e189fbe702d51a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008001887fffffffffffffff8082079e42a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42188000000000000000009a048a703da164234812273ea083e4ec3d09d028300cd325b46a6a75402e5a7ab95c0c0d9d8808094c94f5374fce5edbc8e2a8697c15331677e6ebf0b80"
        );
        let block: Block = Block::decode(&mut data.as_slice()).unwrap();
        assert!(block.body.withdrawals.is_some());
        let withdrawals = block.body.withdrawals.as_ref().unwrap();
        assert_eq!(withdrawals.len(), 1);
        let withdrawals_root = calculate_withdrawals_root(withdrawals);
        assert_eq!(block.withdrawals_root, Some(withdrawals_root));

        // 4 withdrawals, identical indices
        // https://github.com/ethereum/tests/blob/9760400e667eba241265016b02644ef62ab55de2/BlockchainTests/EIPTests/bc4895-withdrawals/twoIdenticalIndex.json
        let data = &hex!(
            "f9028cf90219a0151934ad9b654c50197f37018ee5ee9bb922dec0a1b5e24a6d679cb111cdb107a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347942adc25665018aa1fe0e6bc666dac8fc2697ff9baa0ccf7b62d616c2ad7af862d67b9dcd2119a90cebbff8c3cd1e5d7fc99f8755774a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008001887fffffffffffffff8082079e42a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42188000000000000000009a0a95b9a7b58a6b3cb4001eb0be67951c5517141cb0183a255b5cae027a7b10b36c0c0f86cda808094c94f5374fce5edbc8e2a8697c15331677e6ebf0b822710da028094c94f5374fce5edbc8e2a8697c15331677e6ebf0b822710da018094c94f5374fce5edbc8e2a8697c15331677e6ebf0b822710da028094c94f5374fce5edbc8e2a8697c15331677e6ebf0b822710"
        );
        let block: Block = Block::decode(&mut data.as_slice()).unwrap();
        assert!(block.body.withdrawals.is_some());
        let withdrawals = block.body.withdrawals.as_ref().unwrap();
        assert_eq!(withdrawals.len(), 4);
        let withdrawals_root = calculate_withdrawals_root(withdrawals);
        assert_eq!(block.withdrawals_root, Some(withdrawals_root));
    }
    #[test]
    fn check_receipt_root_optimism() {
        use alloy_consensus::ReceiptWithBloom;

        let logs = vec![Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![], Default::default()),
        }];
        let bloom = bloom!(
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
        );
        let receipt = ReceiptWithBloom {
            receipt: Receipt::standard(TxType::Eip2930, true, 102068, logs),
            logs_bloom: bloom,
        };
        let receipt = vec![receipt];
        let root = calculate_receipt_root(&receipt);
        assert_eq!(
            root,
            b256!("0xfe70ae4a136d98944951b2123859698d59ad251a381abc9960fa81cae3d0d4a0")
        );
    }

    // Ensures that reth and alloy receipts encode to the same JSON
    #[test]
    #[cfg(feature = "rpc")]
    fn test_receipt_serde() {
        use alloy_consensus::ReceiptEnvelope;

        let input = r#"{"status":"0x1","cumulativeGasUsed":"0x175cc0e","logs":[{"address":"0xa18b9ca2a78660d44ab38ae72e72b18792ffe413","topics":["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925","0x000000000000000000000000e7e7d8006cbff47bc6ac2dabf592c98e97502708","0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d"],"data":"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","blockHash":"0xbf9e6a368a399f996a0f0b27cab4191c028c3c99f5f76ea08a5b70b961475fcb","blockNumber":"0x164b59f","blockTimestamp":"0x68c9a713","transactionHash":"0x533aa9e57865675bb94f41aa2895c0ac81eee69686c77af16149c301e19805f1","transactionIndex":"0x14d","logIndex":"0x238","removed":false}],"logsBloom":"0x00000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000400000040000000000000004000000000000000000000000000000000000000000000020000000000000000000000000080000000000000000000000000200000020000000000000000000000000000000000000000000000000000000000000020000010000000000000000000000000000000000000000000000000000000000000","type":"0x2","transactionHash":"0x533aa9e57865675bb94f41aa2895c0ac81eee69686c77af16149c301e19805f1","transactionIndex":"0x14d","blockHash":"0xbf9e6a368a399f996a0f0b27cab4191c028c3c99f5f76ea08a5b70b961475fcb","blockNumber":"0x164b59f","gasUsed":"0xb607","effectiveGasPrice":"0x4a3ee768","from":"0xe7e7d8006cbff47bc6ac2dabf592c98e97502708","to":"0xa18b9ca2a78660d44ab38ae72e72b18792ffe413","contractAddress":null}"#;
        let receipt: RpcReceipt = serde_json::from_str(input).unwrap();
        let envelope: ReceiptEnvelope<alloy_rpc_types_eth::Log> =
            serde_json::from_str(input).unwrap();

        assert_eq!(envelope, receipt);

        let json_envelope = serde_json::to_value(&envelope).unwrap();
        let json_receipt = serde_json::to_value(receipt.into_with_bloom()).unwrap();
        assert_eq!(json_envelope, json_receipt);
    }
}
