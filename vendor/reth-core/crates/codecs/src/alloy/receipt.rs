//! Compact implementations for Alloy receipt types.

use crate::Compact;
use alloc::vec::Vec;
use alloy_consensus::{
    EthereumReceipt as AlloyEthereumReceipt, ReceiptEnvelope as AlloyReceiptEnvelope, TxType,
};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::Log;
use bytes::{Buf, BufMut};
use modular_bitfield::prelude::*;

/// Discriminator for the EIP-8141 receipt envelope compact encoding.
///
/// `0xff` is not a valid [`ReceiptFlags`] value: it would require a three-byte transaction type
/// and a fifteen-byte `u64`. Existing receipt encodings can therefore continue using their
/// unchanged compact representation.
const EIP8141_COMPACT_IDENTIFIER: u8 = u8::MAX;

#[allow(non_snake_case)]
mod flags {
    use super::*;

    /// Bitflag fieldset for receipt compact encoding.
    ///
    /// Used bytes: 1 | Unused bits: 0
    #[bitfield]
    #[derive(Clone, Copy, Debug, Default)]
    pub struct ReceiptFlags {
        pub tx_type_len: B2,
        pub success_len: B1,
        pub cumulative_gas_used_len: B4,
        pub __zstd: B1,
    }

    impl ReceiptFlags {
        /// Deserializes this fieldset and returns it, alongside the original slice in an advanced
        /// position.
        pub fn from(mut buf: &[u8]) -> (Self, &[u8]) {
            (Self::from_bytes([buf.get_u8()]), buf)
        }
    }
}

pub(crate) use flags::ReceiptFlags;

impl<T: Compact> Compact for AlloyEthereumReceipt<T> {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        let receipt = self;
        let mut flags = ReceiptFlags::default();
        let mut total_length = 0;
        let mut buffer = bytes::BytesMut::new();

        let tx_type_len = receipt.tx_type.to_compact(&mut buffer);
        flags.set_tx_type_len(tx_type_len as u8);
        let success_len = receipt.success.to_compact(&mut buffer);
        flags.set_success_len(success_len as u8);
        let cumulative_gas_used_len = receipt.cumulative_gas_used.to_compact(&mut buffer);
        flags.set_cumulative_gas_used_len(cumulative_gas_used_len as u8);
        receipt.logs.to_compact(&mut buffer);

        let zstd = buffer.len() > 7;
        if zstd {
            flags.set___zstd(1);
        }

        let flags = flags.into_bytes();
        total_length += flags.len() + buffer.len();
        buf.put_slice(&flags);
        if zstd {
            reth_zstd_compressors::with_receipt_compressor(|compressor| {
                let compressed = compressor.compress(&buffer).expect("Failed to compress.");
                buf.put(compressed.as_slice());
            });
        } else {
            buf.put(buffer);
        }
        total_length
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let (flags, mut buf) = ReceiptFlags::from(buf);
        if flags.__zstd() != 0 {
            reth_zstd_compressors::with_receipt_decompressor(|decompressor| {
                let decompressed = decompressor.decompress(buf);
                let original_buf = buf;
                let mut buf: &[u8] = decompressed;
                let (tx_type, new_buf) = T::from_compact(buf, flags.tx_type_len() as usize);
                buf = new_buf;
                let (success, new_buf) = bool::from_compact(buf, flags.success_len() as usize);
                buf = new_buf;
                let (cumulative_gas_used, new_buf) =
                    u64::from_compact(buf, flags.cumulative_gas_used_len() as usize);
                buf = new_buf;
                let (logs, _) = Vec::<Log>::from_compact(buf, buf.len());
                (Self { tx_type, success, cumulative_gas_used, logs }, original_buf)
            })
        } else {
            let (tx_type, new_buf) = T::from_compact(buf, flags.tx_type_len() as usize);
            buf = new_buf;
            let (success, new_buf) = bool::from_compact(buf, flags.success_len() as usize);
            buf = new_buf;
            let (cumulative_gas_used, new_buf) =
                u64::from_compact(buf, flags.cumulative_gas_used_len() as usize);
            buf = new_buf;
            let (logs, new_buf) = Vec::<Log>::from_compact(buf, buf.len());
            buf = new_buf;
            let obj = Self { tx_type, success, cumulative_gas_used, logs };
            (obj, buf)
        }
    }
}

impl Compact for AlloyReceiptEnvelope {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        match self {
            AlloyReceiptEnvelope::Legacy(receipt) => {
                standard(TxType::Legacy, &receipt.receipt).to_compact(buf)
            }
            AlloyReceiptEnvelope::Eip2930(receipt) => {
                standard(TxType::Eip2930, &receipt.receipt).to_compact(buf)
            }
            AlloyReceiptEnvelope::Eip1559(receipt) => {
                standard(TxType::Eip1559, &receipt.receipt).to_compact(buf)
            }
            AlloyReceiptEnvelope::Eip4844(receipt) => {
                standard(TxType::Eip4844, &receipt.receipt).to_compact(buf)
            }
            AlloyReceiptEnvelope::Eip7702(receipt) => {
                standard(TxType::Eip7702, &receipt.receipt).to_compact(buf)
            }
            AlloyReceiptEnvelope::Eip8141(_) => {
                let mut encoded = Vec::with_capacity(self.encode_2718_len());
                self.encode_2718(&mut encoded);
                buf.put_u8(EIP8141_COMPACT_IDENTIFIER);
                buf.put_slice(&encoded);
                encoded.len() + 1
            }
        }
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        if buf.first() == Some(&EIP8141_COMPACT_IDENTIFIER) {
            let frame_receipt_len =
                len.checked_sub(1).expect("EIP-8141 compact receipt is missing its payload");
            let end = 1 + frame_receipt_len;
            let mut encoded =
                buf.get(1..end).expect("EIP-8141 compact receipt length exceeds the input buffer");
            let receipt = Self::decode_2718(&mut encoded)
                .expect("invalid EIP-8141 receipt in compact database encoding");
            assert!(
                matches!(receipt, Self::Eip8141(_)),
                "compact receipt discriminator contained another type"
            );
            assert!(encoded.is_empty(), "trailing bytes in EIP-8141 compact receipt");
            return (receipt, &buf[end..])
        }

        let (receipt, buf) = AlloyEthereumReceipt::<TxType>::from_compact(buf, len);
        (
            AlloyReceiptEnvelope::try_from(receipt)
                .expect("standard receipt conversion cannot fail"),
            buf,
        )
    }
}

fn standard(tx_type: TxType, receipt: &alloy_consensus::Receipt) -> AlloyEthereumReceipt {
    AlloyEthereumReceipt {
        tx_type,
        success: receipt.status.coerce_status(),
        cumulative_gas_used: receipt.cumulative_gas_used,
        logs: receipt.logs.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::eip8141::{FrameGasUsed, FrameReceipt, FrameReceiptPayload, FrameStatus};
    use alloy_primitives::Address;
    use proptest::proptest;
    use proptest_arbitrary_interop::arb;

    proptest! {
        #[test]
        fn roundtrip_receipt(receipt in arb::<AlloyEthereumReceipt<TxType>>()) {
            let mut compacted_receipt = Vec::<u8>::new();
            let len = receipt.to_compact(&mut compacted_receipt);
            let (decoded, _) = AlloyEthereumReceipt::<TxType>::from_compact(&compacted_receipt, len);
            assert_eq!(receipt, decoded)
        }
    }

    #[test]
    fn non_frame_envelope_preserves_existing_compact_encoding() {
        let receipt = AlloyEthereumReceipt {
            tx_type: TxType::Eip1559,
            success: true,
            cumulative_gas_used: 21_000,
            logs: vec![Log::default()],
        };
        let envelope = AlloyReceiptEnvelope::try_from(receipt.clone()).unwrap();
        let mut receipt_buf = Vec::new();
        let mut envelope_buf = Vec::new();

        let receipt_len = receipt.to_compact(&mut receipt_buf);
        let envelope_len = envelope.to_compact(&mut envelope_buf);
        let (decoded, _) = AlloyReceiptEnvelope::from_compact(&envelope_buf, envelope_len);

        assert_eq!(envelope_buf, receipt_buf);
        assert_eq!(envelope_len, receipt_len);
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn frame_receipt_envelope_roundtrip() {
        let envelope = AlloyReceiptEnvelope::Eip8141(
            FrameReceiptPayload {
                cumulative_gas_used: 42_000,
                payer: Address::repeat_byte(0x11),
                frame_receipts: vec![
                    FrameReceipt {
                        status: FrameStatus::Success,
                        gas_used: FrameGasUsed { execution: 21_000, state: 0 },
                        logs: vec![Log::default()],
                    },
                    FrameReceipt {
                        status: FrameStatus::SkippedAtomicBatch,
                        gas_used: FrameGasUsed::default(),
                        logs: Vec::new(),
                    },
                ],
            }
            .into(),
        );
        let mut compact = Vec::new();

        let len = envelope.to_compact(&mut compact);
        let (decoded, remaining) = AlloyReceiptEnvelope::from_compact(&compact, len);

        assert_eq!(compact.first(), Some(&EIP8141_COMPACT_IDENTIFIER));
        assert_eq!(decoded, envelope);
        assert!(remaining.is_empty());
    }
}
