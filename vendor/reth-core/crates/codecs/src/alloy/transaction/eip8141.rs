//! Compact implementation for [`TxEip8141`].

use crate::Compact;
use alloy_consensus::TxEip8141;
use alloy_eips::eip2718::{Decodable2718, Encodable2718};

impl Compact for TxEip8141 {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        let len = self.encode_2718_len();
        self.encode_2718(buf);
        len
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let mut encoded =
            buf.get(..len).expect("EIP-8141 compact transaction length exceeds input");
        let tx = Self::decode_2718(&mut encoded)
            .expect("invalid EIP-8141 transaction in compact database encoding");
        let consumed = len - encoded.len();
        (tx, &buf[consumed..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::eip8141::{Frame, FrameMode};
    use alloy_primitives::{Address, Bytes, U256};

    #[test]
    fn frame_transaction_roundtrip() {
        let tx = TxEip8141 {
            chain_id: 1,
            nonce_keys: vec![U256::from(1), U256::from(2)],
            nonce_seq: 7,
            sender: Address::repeat_byte(0x11),
            frames: vec![Frame {
                mode: FrameMode::Sender,
                data: Bytes::copy_from_slice(&[0xaa, 0xbb]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut compact = Vec::new();

        let len = tx.to_compact(&mut compact);
        let (decoded, remaining) = TxEip8141::from_compact(&compact, len);

        assert_eq!(decoded, tx);
        assert!(remaining.is_empty());
    }
}
