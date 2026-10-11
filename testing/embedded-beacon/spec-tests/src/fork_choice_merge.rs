//! Exact terminal-block ancestry checks over published synthetic `PoW` fixtures.
//! This fixture source is private and cannot establish live execution validity.

use super::{config_override, config_value_override, fixed_hex, Case};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::{auxiliary::bellatrix::PowBlock, Preset};
use std::{cmp::Ordering, collections::BTreeMap};

#[derive(Debug)]
pub(super) struct TerminalContext {
    blocks: BTreeMap<Root, PowBlock>,
    threshold: [u8; 32],
    override_hash: Root,
    activation_epoch: u64,
    slots_per_epoch: u64,
}

impl TerminalContext {
    pub(super) fn from_case(case: &Case, preset: Preset, max_bytes: usize) -> Result<Self, String> {
        let default = match preset {
            Preset::Mainnet => "58750000000000000000000",
            Preset::Minimal => {
                "115792089237316195423570985008687907853269984665640564039457584007913129638912"
            }
        };
        let value = config_value_override(case, "TERMINAL_TOTAL_DIFFICULTY", max_bytes)?;
        let decimal = match value {
            Some(value) if value.as_str().is_some() => {
                value.as_str().ok_or("invalid terminal difficulty")?.to_owned()
            }
            Some(value) if value.as_u64().is_some() => {
                value.as_u64().ok_or("invalid terminal difficulty")?.to_string()
            }
            None => default.to_owned(),
            _ => return Err("terminal difficulty must be an exact unsigned integer".into()),
        };
        let override_hash = config_value_override(case, "TERMINAL_BLOCK_HASH", max_bytes)?
            .map(|value| fixed_hex::<32>(&value))
            .transpose()?
            .unwrap_or([0; 32]);
        Ok(Self {
            blocks: BTreeMap::new(),
            threshold: decimal_uint256(&decimal)?,
            override_hash,
            activation_epoch: config_override(
                case,
                "TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH",
                u64::MAX,
                max_bytes,
            )?,
            slots_per_epoch: match preset {
                Preset::Mainnet => 32,
                Preset::Minimal => 8,
            },
        })
    }
    pub(super) fn insert(&mut self, block: PowBlock) -> Result<(), String> {
        if let Some(previous) = self.blocks.get(&block.block_hash) &&
            previous != &block
        {
            return Err("conflicting PoW fixture identity".into());
        }
        if self.blocks.len() >= 4096 {
            return Err("PoW fixture lookup exceeds its bound".into());
        }
        self.blocks.insert(block.block_hash, block);
        Ok(())
    }
    pub(super) fn verify(&self, parent_hash: Root, slot: u64) -> bool {
        if self.override_hash != [0; 32] {
            return slot / self.slots_per_epoch >= self.activation_epoch &&
                parent_hash == self.override_hash;
        }
        let Some(block) = self.blocks.get(&parent_hash) else {
            return false;
        };
        let Some(parent) = self.blocks.get(&block.parent_hash) else {
            return false;
        };
        compare(parent.total_difficulty, self.threshold) == Ordering::Less &&
            compare(block.total_difficulty, self.threshold) != Ordering::Less
    }
}

fn compare(first: [u8; 32], second: [u8; 32]) -> Ordering {
    first.iter().rev().cmp(second.iter().rev())
}

fn decimal_uint256(value: &str) -> Result<[u8; 32], String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("terminal difficulty is not decimal uint256".into());
    }
    let mut result = [0; 32];
    for digit in value.bytes() {
        let mut carry = u16::from(digit - b'0');
        for byte in &mut result {
            carry += u16::from(*byte) * 10;
            *byte = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            return Err("terminal difficulty exceeds uint256".into());
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_uint256_difficulty_preserves_low_bits_and_rejects_overflow() {
        assert_eq!(
            decimal_uint256(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
            )
            .unwrap(),
            [255; 32]
        );
        assert!(decimal_uint256(
            "115792089237316195423570985008687907853269984665640564039457584007913129639936"
        )
        .is_err());
        let mut expected = [255; 32];
        expected[0] = 0;
        expected[1] = 252;
        assert_eq!(
            decimal_uint256(
                "115792089237316195423570985008687907853269984665640564039457584007913129638912"
            )
            .unwrap(),
            expected
        );
    }
}
