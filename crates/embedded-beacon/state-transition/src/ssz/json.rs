//! Bounded Beacon API JSON for native complete SSZ values.
//! This is observational serialization and grants no signature or transition authority.

use super::{
    BeaconState, BitListValue, Bits, Length, List, Preset, PresetBitList, PresetBits,
    ProgressiveBitList, ProgressiveList, SignedBeaconBlock, Ssz, Vector,
};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint, SszError};
use thiserror::Error;

/// One response buffer whose allocation and emitted bytes are explicitly bounded.
#[derive(Debug)]
pub struct JsonEncoder {
    output: Vec<u8>,
    maximum: usize,
}
impl JsonEncoder {
    /// Create a serializer with a positive maximum response size.
    pub fn new(maximum: usize) -> Result<Self, JsonError> {
        if maximum == 0 {
            return Err(JsonError::Limit)
        }
        Ok(Self { output: Vec::new(), maximum })
    }
    /// Consume only a complete successful serialization.
    pub fn finish(self) -> Vec<u8> {
        self.output
    }
    pub(crate) fn raw(&mut self, bytes: &[u8]) -> Result<(), JsonError> {
        let next = self.output.len().checked_add(bytes.len()).ok_or(JsonError::Limit)?;
        if next > self.maximum {
            return Err(JsonError::Limit)
        }
        if next > self.output.capacity() {
            // Geometric growth stays within the declared response allocation budget.
            let capacity = next.max(self.output.capacity().saturating_mul(2)).min(self.maximum);
            self.output
                .try_reserve_exact(capacity - self.output.len())
                .map_err(|_| JsonError::Allocation)?;
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
    pub(crate) fn field(&mut self, name: &'static str, first: &mut bool) -> Result<(), JsonError> {
        if !*first {
            self.raw(b",")?;
        }
        *first = false;
        self.raw(b"\"")?;
        self.raw(name.as_bytes())?;
        self.raw(b"\":")
    }
    fn hex(&mut self, bytes: &[u8]) -> Result<(), JsonError> {
        let width = bytes
            .len()
            .checked_mul(2)
            .and_then(|size| size.checked_add(4))
            .ok_or(JsonError::Limit)?;
        if self.output.len().checked_add(width).ok_or(JsonError::Limit)? > self.maximum {
            return Err(JsonError::Limit)
        }
        self.raw(b"\"0x")?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in bytes {
            self.raw(&[HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]])?;
        }
        self.raw(b"\"")
    }
    fn decimal(&mut self, digits: &str) -> Result<(), JsonError> {
        self.raw(b"\"")?;
        self.raw(digits.as_bytes())?;
        self.raw(b"\"")
    }
}

/// Native structural JSON formatting failure, independent of consensus validity.
#[derive(Debug, Error)]
pub enum JsonError {
    /// The response would exceed its positive byte/allocation budget.
    #[error("Beacon JSON response exceeds its limit")]
    Limit,
    /// A bounded allocation could not be obtained.
    #[error("Beacon JSON response allocation unavailable")]
    Allocation,
    /// The native value violates its complete SSZ type bounds.
    #[error(transparent)]
    Schema(#[from] SszError),
    /// The requested preset differs from the native value's recorded preset.
    #[error("Beacon JSON preset does not match the native value")]
    Preset,
}

/// Complete native value formatted with decimal strings and exact hex byte sequences.
pub trait JsonSsz {
    /// Whether sequence elements are SSZ bytes rather than nested JSON values.
    const JSON_BYTE: bool = false;
    /// Return a byte only for the native uint8 implementation.
    fn json_byte(&self) -> Option<u8> {
        None
    }
    /// Append one complete native value under its published preset.
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError>;
    /// Keep named uint256 and participation-list semantics distinct from ordinary byte strings.
    fn write_json_field(
        &self,
        _name: &'static str,
        preset: Preset,
        output: &mut JsonEncoder,
    ) -> Result<(), JsonError> {
        self.write_json(preset, output)
    }
    /// Serialize only when all fields fit; callers publish no partial output on error.
    fn to_beacon_json(&self, preset: Preset, maximum: usize) -> Result<Vec<u8>, JsonError> {
        let mut output = JsonEncoder::new(maximum)?;
        self.write_json(preset, &mut output)?;
        Ok(output.finish())
    }
}
impl JsonSsz for u64 {
    fn write_json(&self, _: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.decimal(&self.to_string())
    }
}
impl JsonSsz for u8 {
    const JSON_BYTE: bool = true;
    fn json_byte(&self) -> Option<u8> {
        Some(*self)
    }
    fn write_json(&self, _: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.decimal(&self.to_string())
    }
}
impl JsonSsz for bool {
    fn write_json(&self, _: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.raw(if *self { b"true" } else { b"false" })
    }
}
impl<const N: usize> JsonSsz for [u8; N] {
    fn write_json(&self, _: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.hex(self)
    }
    fn write_json_field(
        &self,
        name: &'static str,
        preset: Preset,
        output: &mut JsonEncoder,
    ) -> Result<(), JsonError> {
        if name == "base_fee_per_gas" && N == 32 {
            output.decimal(&uint256_decimal(self))
        } else {
            self.write_json(preset, output)
        }
    }
}
impl JsonSsz for BeaconBlockHeader {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.raw(b"{")?;
        let mut first = true;
        output.field("slot", &mut first)?;
        self.slot.write_json(preset, output)?;
        output.field("proposer_index", &mut first)?;
        self.proposer_index.write_json(preset, output)?;
        output.field("parent_root", &mut first)?;
        self.parent_root.write_json(preset, output)?;
        output.field("state_root", &mut first)?;
        self.state_root.write_json(preset, output)?;
        output.field("body_root", &mut first)?;
        self.body_root.write_json(preset, output)?;
        output.raw(b"}")
    }
}
impl JsonSsz for Checkpoint {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.raw(b"{")?;
        let mut first = true;
        output.field("epoch", &mut first)?;
        self.epoch.write_json(preset, output)?;
        output.field("root", &mut first)?;
        self.root.write_json(preset, output)?;
        output.raw(b"}")
    }
}
impl<T: JsonSsz, L: Length> JsonSsz for List<T, L> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        self.write_json_field("", preset, output)
    }
    fn write_json_field(
        &self,
        name: &'static str,
        preset: Preset,
        output: &mut JsonEncoder,
    ) -> Result<(), JsonError> {
        let maximum = L::length(preset)?;
        if self.values.len() > maximum {
            return Err(
                SszError::InvalidLength { expected: maximum, actual: self.values.len() }.into()
            )
        }
        sequence(&self.values, name, preset, output)
    }
}
impl<T: JsonSsz, L: Length> JsonSsz for Vector<T, L> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        let expected = L::length(preset)?;
        if self.values.len() != expected {
            return Err(SszError::InvalidLength { expected, actual: self.values.len() }.into())
        }
        sequence(&self.values, "", preset, output)
    }
}
impl<T: JsonSsz> JsonSsz for ProgressiveList<T> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        self.write_json_field("", preset, output)
    }
    fn write_json_field(
        &self,
        name: &'static str,
        preset: Preset,
        output: &mut JsonEncoder,
    ) -> Result<(), JsonError> {
        sequence(&self.values, name, preset, output)
    }
}
macro_rules! bits_json {
    ($($ty:ty),* $(,)?) => { $(impl JsonSsz for $ty {
        fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
            output.hex(&self.to_ssz(preset)?)
        }
    })* };
}
bits_json!(ProgressiveBitList);
impl<const N: usize> JsonSsz for Bits<N> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.hex(&self.to_ssz(preset)?)
    }
}
impl<const N: usize> JsonSsz for BitListValue<N> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.hex(&self.to_ssz(preset)?)
    }
}
impl<L: Length> JsonSsz for PresetBits<L> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.hex(&self.to_ssz(preset)?)
    }
}
impl<L: Length> JsonSsz for PresetBitList<L> {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        output.hex(&self.to_ssz(preset)?)
    }
}
impl JsonSsz for SignedBeaconBlock {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        let recorded = match self {
            Self::Phase0(_, recorded) |
            Self::Altair(_, recorded) |
            Self::Bellatrix(_, recorded) |
            Self::Capella(_, recorded) |
            Self::Deneb(_, recorded) |
            Self::Electra(_, recorded) |
            Self::Fulu(_, recorded) |
            Self::Gloas(_, recorded) => *recorded,
        };
        if recorded != preset {
            return Err(JsonError::Preset)
        }
        match self {
            Self::Phase0(value, _) => value.write_json(preset, output),
            Self::Altair(value, _) => value.write_json(preset, output),
            Self::Bellatrix(value, _) => value.write_json(preset, output),
            Self::Capella(value, _) => value.write_json(preset, output),
            Self::Deneb(value, _) => value.write_json(preset, output),
            Self::Electra(value, _) => value.write_json(preset, output),
            Self::Fulu(value, _) => value.write_json(preset, output),
            Self::Gloas(value, _) => value.write_json(preset, output),
        }
    }
}
impl JsonSsz for BeaconState {
    fn write_json(&self, preset: Preset, output: &mut JsonEncoder) -> Result<(), JsonError> {
        if self.preset() != preset {
            return Err(JsonError::Preset)
        }
        match self {
            Self::Phase0(value, _) => value.write_json(preset, output),
            Self::Altair(value, _) => value.write_json(preset, output),
            Self::Bellatrix(value, _) => value.write_json(preset, output),
            Self::Capella(value, _) => value.write_json(preset, output),
            Self::Deneb(value, _) => value.write_json(preset, output),
            Self::Electra(value, _) => value.write_json(preset, output),
            Self::Fulu(value, _) => value.write_json(preset, output),
            Self::Gloas(value, _) => value.write_json(preset, output),
        }
    }
}
fn sequence<T: JsonSsz>(
    values: &[T],
    name: &'static str,
    preset: Preset,
    output: &mut JsonEncoder,
) -> Result<(), JsonError> {
    if T::JSON_BYTE &&
        !matches!(name, "previous_epoch_participation" | "current_epoch_participation")
    {
        output.raw(b"\"0x")?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for value in values {
            let byte = value.json_byte().ok_or(SszError::InvalidOffset)?;
            output.raw(&[HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]])?;
        }
        return output.raw(b"\"")
    }
    output.raw(b"[")?;
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.raw(b",")?;
        }
        value.write_json(preset, output)?;
    }
    output.raw(b"]")
}
fn uint256_decimal(bytes: &[u8]) -> String {
    let mut remaining = bytes.to_vec();
    let mut digits = Vec::with_capacity(78);
    while remaining.iter().any(|byte| *byte != 0) {
        let mut carry = 0u16;
        for byte in remaining.iter_mut().rev() {
            let value = carry * 256 + u16::from(*byte);
            *byte = (value / 10) as u8;
            carry = value % 10;
        }
        digits.push(b'0' + carry as u8);
    }
    if digits.is_empty() {
        digits.push(b'0');
    }
    digits.reverse();
    String::from_utf8(digits).expect("decimal digits are ASCII")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::{bellatrix, Fixed};

    #[test]
    fn wide_integer_and_native_scalar_json_have_exact_protocol_representations() {
        assert_eq!(
            u64::MAX.to_beacon_json(Preset::Mainnet, 32).unwrap(),
            b"\"18446744073709551615\""
        );
        let mut encoder = JsonEncoder::new(128).unwrap();
        [255u8; 32].write_json_field("base_fee_per_gas", Preset::Mainnet, &mut encoder).unwrap();
        assert_eq!(
            encoder.finish(),
            b"\"115792089237316195423570985008687907853269984665640564039457584007913129639935\""
        );
        assert_eq!(false.to_beacon_json(Preset::Minimal, 5).unwrap(), b"false");
    }
    #[test]
    fn byte_lists_nested_transactions_and_participation_arrays_remain_distinct() {
        let bytes = List::<u8, Fixed<8>>::from_vec(vec![0, 1, 255]);
        assert_eq!(bytes.to_beacon_json(Preset::Mainnet, 32).unwrap(), b"\"0x0001ff\"");
        let mut output = JsonEncoder::new(32).unwrap();
        bytes
            .write_json_field("current_epoch_participation", Preset::Mainnet, &mut output)
            .unwrap();
        assert_eq!(output.finish(), b"[\"0\",\"1\",\"255\"]");
        let transactions = List::<List<u8, Fixed<8>>, Fixed<2>>::from_vec(vec![bytes]);
        assert_eq!(transactions.to_beacon_json(Preset::Mainnet, 32).unwrap(), b"[\"0x0001ff\"]");
    }
    #[test]
    fn structural_collection_bounds_and_response_limits_apply_before_publication() {
        let invalid = List::<u8, Fixed<1>>::from_vec(vec![1, 2]);
        assert!(matches!(invalid.to_beacon_json(Preset::Mainnet, 32), Err(JsonError::Schema(_))));
        assert!(matches!([0u8; 96].to_beacon_json(Preset::Mainnet, 100), Err(JsonError::Limit)));
        assert!(matches!(JsonEncoder::new(0), Err(JsonError::Limit)));
        let bits = BitListValue::<8>::from_ssz(&[0x15], Preset::Mainnet).unwrap();
        assert_eq!(bits.to_beacon_json(Preset::Mainnet, 32).unwrap(), b"\"0x15\"");
    }
    #[test]
    fn json_does_not_mutate_the_original_native_payload_or_its_commitment() {
        let mut payload = bellatrix::ExecutionPayload::default_value(Preset::Minimal).unwrap();
        payload.base_fee_per_gas[0] = 42;
        payload.extra_data.values = vec![1, 2, 3];
        let before = payload.to_ssz(Preset::Minimal).unwrap();
        let root = payload.hash_tree_root(Preset::Minimal).unwrap();
        let rendered = payload.to_beacon_json(Preset::Minimal, 4096).unwrap();
        assert!(rendered
            .windows(b"\"base_fee_per_gas\":\"42\"".len())
            .any(|part| part == b"\"base_fee_per_gas\":\"42\""));
        assert_eq!(payload.to_ssz(Preset::Minimal).unwrap(), before);
        assert_eq!(payload.hash_tree_root(Preset::Minimal).unwrap(), root);
    }
}
