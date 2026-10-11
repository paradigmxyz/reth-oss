//! Committee-sized lookup of the first validator index for each requested public key.
//!
//! The complete registry remains independently validated. This lookup neither checks BLS
//! nor caches authority; it preserves the registry's first-match and committee duplicate rules.

use crate::ssz::phase0::Validator;
use std::collections::BTreeMap;

pub(crate) struct CommitteeIndices {
    indices: BTreeMap<[u8; 48], Option<usize>>,
}

impl CommitteeIndices {
    pub(crate) fn new<'a>(
        keys: &[[u8; 48]],
        validators: impl IntoIterator<Item = &'a Validator>,
    ) -> Self {
        // Bound allocations by unique committee keys instead of the complete validator registry.
        let mut indices = keys.iter().copied().map(|key| (key, None)).collect::<BTreeMap<_, _>>();
        let mut remaining = indices.len();
        if remaining == 0 {
            return Self { indices };
        }
        for (index, validator) in validators.into_iter().enumerate() {
            if let Some(first) = indices.get_mut(&validator.pubkey) &&
                first.is_none()
            {
                *first = Some(index);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        Self { indices }
    }

    pub(crate) fn get(&self, key: &[u8; 48]) -> Option<usize> {
        self.indices.get(key).copied().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_primitives::sha256;
    use std::{cell::Cell, hint::black_box, time::Instant};

    fn validator(key: u8) -> Validator {
        Validator {
            pubkey: [key; 48],
            withdrawal_credentials: [0; 32],
            effective_balance: 32_000_000_000,
            slashed: false,
            activation_eligibility_epoch: 0,
            activation_epoch: 0,
            exit_epoch: u64::MAX,
            withdrawable_epoch: u64::MAX,
        }
    }

    #[test]
    fn repeated_committee_keys_resolve_to_first_registry_index_in_original_order() {
        let keys = [[3; 48], [2; 48], [3; 48]];
        let validators = [validator(9), validator(2), validator(2), validator(3), validator(3)];
        let lookup = CommitteeIndices::new(&keys, &validators);
        assert_eq!(lookup.indices.len(), 2);
        assert_eq!(
            keys.iter().map(|key| lookup.get(key)).collect::<Vec<_>>(),
            vec![Some(3), Some(1), Some(3)]
        );
        // Reward each original committee position, including repeated keys, using its first index.
        let mut balances = [0u64, 10, 20, 30, 40];
        for (key, participated) in keys.iter().zip([true, false, true]) {
            let index = lookup.get(key).unwrap();
            if participated {
                balances[index] += 2;
            } else {
                balances[index] = balances[index].saturating_sub(2);
            }
        }
        assert_eq!(balances, [0, 8, 20, 34, 40]);
    }

    #[test]
    fn missing_committee_key_never_resolves_to_another_validator() {
        let keys = [[1; 48], [2; 48]];
        let validators = [validator(1), validator(1), validator(3)];
        let lookup = CommitteeIndices::new(&keys, &validators);
        assert_eq!(lookup.get(&keys[0]), Some(0));
        assert_eq!(lookup.get(&keys[1]), None);
        assert_eq!(lookup.get(&[3; 48]), None);
        assert_eq!(lookup.indices.len(), keys.len());
    }

    #[test]
    fn complete_committee_lookup_stops_without_scanning_unneeded_registry_tail() {
        let keys = [[1; 48], [2; 48], [1; 48]];
        let mut validators = vec![validator(1), validator(2)];
        validators.extend((0..1024).map(|_| validator(9)));
        let visited = Cell::new(0);
        let lookup = CommitteeIndices::new(
            &keys,
            validators.iter().inspect(|_| visited.set(visited.get() + 1)),
        );
        assert_eq!(visited.get(), 2);
        assert_eq!(lookup.indices.len(), 2);
        assert_eq!(lookup.get(&keys[0]), Some(0));
        assert_eq!(lookup.get(&keys[1]), Some(1));
        let empty = CommitteeIndices::new(
            &[],
            validators.iter().inspect(|_| panic!("empty committee cannot scan the registry")),
        );
        assert!(empty.indices.is_empty());
    }

    #[test]
    #[ignore = "explicit Hoodi-scale sync committee registry lookup measurement"]
    fn hoodi_sync_committee_registry_lookup_latency() {
        let count = 1_144_679;
        let validators = (0..count)
            .map(|index: usize| {
                let mut value = validator(index as u8);
                let digest = sha256(&(index as u64).to_le_bytes());
                value.pubkey[..32].copy_from_slice(&digest);
                value.pubkey[32..40].copy_from_slice(&(index as u64).to_le_bytes());
                value.pubkey[40..].copy_from_slice(&digest[..8]);
                value
            })
            .collect::<Vec<_>>();
        let mut positions = (0..512).map(|index| (count - 1) * index / 511).collect::<Vec<_>>();
        positions[510] = positions[15];
        let keys = positions.iter().map(|&index| validators[index].pubkey).collect::<Vec<_>>();
        let expected = CommitteeIndices::new(&keys, &validators);
        assert_eq!(expected.indices.len(), 511);
        for (key, &index) in keys.iter().zip(&positions) {
            assert_eq!(expected.get(key), Some(index));
        }
        let iterations = 4;
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(CommitteeIndices::new(black_box(&keys), black_box(&validators)));
        }
        let elapsed = started.elapsed();
        eprintln!("Hoodi sync committee: validators={count} committee_positions=512 unique_keys=511 lookup_ms={:.3}; exact_first_index_and_duplicates=true",
            elapsed.as_secs_f64() * 1_000.0 / f64::from(iterations));
    }
}
