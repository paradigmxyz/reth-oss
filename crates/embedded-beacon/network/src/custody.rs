//! Deterministic custody selection using the pinned network's unsigned node identity.

use reth_embedded_beacon_primitives::sha256;
use std::collections::BTreeSet;

/// Checked network dimensions; the public preset uses 128 groups and columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CustodyParameters {
    /// Number of custody groups available on this configured network.
    pub number_of_groups: u64,
    /// Number of extended data columns.
    pub number_of_columns: u64,
}

impl Default for CustodyParameters {
    fn default() -> Self {
        Self { number_of_groups: 128, number_of_columns: 128 }
    }
}

/// A malformed local network parameter, request or exhausted bounded computation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CustodyError {
    /// Network dimensions cannot describe a bounded complete partition.
    #[error("invalid custody network dimensions")]
    Parameters,
    /// The requested count or group index is outside the configured network.
    #[error("custody group out of range")]
    Group,
    /// The deterministic search exhausted its explicit resource bound.
    #[error("custody selection computation budget exhausted")]
    ComputationBudget,
}

impl CustodyParameters {
    /// Validate dimensions before allocating a group set or column vector.
    pub const fn validate(&self) -> Result<(), CustodyError> {
        if self.number_of_groups == 0 ||
            self.number_of_groups > 4096 ||
            self.number_of_columns == 0 ||
            self.number_of_columns > 65536 ||
            !self.number_of_columns.is_multiple_of(self.number_of_groups)
        {
            return Err(CustodyError::Parameters);
        }
        Ok(())
    }

    /// Select a sorted unique set from the complete 256-bit little-endian node identity.
    /// Integer overflow wraps to zero; identity bytes are never truncated to 64 bits.
    pub fn get_custody_groups(
        &self,
        node_id_le: [u8; 32],
        count: u64,
    ) -> Result<Vec<u64>, CustodyError> {
        self.validate()?;
        if count > self.number_of_groups {
            return Err(CustodyError::Group);
        }
        if count == self.number_of_groups {
            return Ok((0..count).collect());
        }
        let mut current = node_id_le;
        let mut groups = BTreeSet::new();
        let budget = self.number_of_groups * self.number_of_groups * 16;
        for _ in 0..budget {
            if groups.len() as u64 == count {
                return Ok(groups.into_iter().collect());
            }
            let hash = sha256(&current);
            let group = u64::from_le_bytes(hash[..8].try_into().expect("fixed hash width")) %
                self.number_of_groups;
            groups.insert(group);
            for byte in &mut current {
                let (incremented, carry) = byte.overflowing_add(1);
                *byte = incremented;
                if !carry {
                    break;
                }
            }
        }
        Err(CustodyError::ComputationBudget)
    }

    /// Return the complete column partition assigned to one custody group.
    pub fn compute_columns_for_custody_group(&self, group: u64) -> Result<Vec<u64>, CustodyError> {
        self.validate()?;
        if group >= self.number_of_groups {
            return Err(CustodyError::Group);
        }
        Ok((0..self.number_of_columns / self.number_of_groups)
            .map(|index| self.number_of_groups * index + group)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_identity_and_extreme_counts_are_preserved() {
        let parameters = CustodyParameters::default();
        let mut short_id = [0; 32];
        short_id[..8].copy_from_slice(&1_048_576u64.to_le_bytes());
        assert_eq!(parameters.get_custody_groups(short_id, 1).unwrap(), [65]);
        assert_eq!(
            parameters.get_custody_groups([255; 32], 128).unwrap(),
            (0..128).collect::<Vec<_>>()
        );
        assert!(parameters.get_custody_groups([0; 32], 0).unwrap().is_empty());
        assert_eq!(parameters.get_custody_groups([0; 32], 129), Err(CustodyError::Group));
    }

    #[test]
    fn groups_partition_all_columns_without_aliasing() {
        let parameters = CustodyParameters { number_of_groups: 4, number_of_columns: 16 };
        assert_eq!(parameters.compute_columns_for_custody_group(2).unwrap(), [2, 6, 10, 14]);
        let all = (0..4)
            .flat_map(|group| parameters.compute_columns_for_custody_group(group).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(all, (0..16).collect());
        assert_eq!(parameters.compute_columns_for_custody_group(4), Err(CustodyError::Group));
        assert_eq!(
            CustodyParameters { number_of_groups: 0, number_of_columns: 128 }.validate(),
            Err(CustodyError::Parameters)
        );
    }
}
