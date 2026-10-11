//! Complete original slashing authentication against the genuine native validator registry.
//! Earlier wire schemas retain their exact roots and native verification-state signing domains.
use crate::StoreError;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    phase0_processing::ProcessingError,
    ssz::{
        electra, gloas, phase0,
        phase0::{AttestationData, Validator},
        Preset,
    },
};
use std::collections::BTreeSet;

struct Indexed<'a> {
    indices: &'a [u64],
    data: &'a AttestationData,
    signature: &'a [u8; 96],
}
fn pair(
    first: Indexed<'_>,
    second: Indexed<'_>,
    validators: Option<&[Validator]>,
    genesis: Root,
    parameters: Option<&TransitionParameters>,
    preset: Preset,
    verification_slot: u64,
) -> Result<BTreeSet<u64>, StoreError> {
    let (a, b) = (first.data, second.data);
    if !((a != b && a.target.epoch == b.target.epoch) ||
        (a.source.epoch < b.source.epoch && b.target.epoch < a.target.epoch))
    {
        return Err(StoreError::InvalidSlashing)
    }
    for indexed in [&first, &second] {
        if indexed.indices.is_empty() ||
            indexed.indices.len() >
                match preset {
                    Preset::Mainnet => 131_072,
                    Preset::Minimal => 8_192,
                } ||
            indexed.indices.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreError::InvalidSlashing)
        }
        if let Some(validators) = validators {
            let parameters = parameters.ok_or(StoreError::InvalidParameters)?;
            parameters.validate()?;
            if parameters.preset() != preset {
                return Err(StoreError::InvalidParameters)
            }
            let keys = indexed
                .indices
                .iter()
                .map(|index| {
                    usize::try_from(*index)
                        .ok()
                        .and_then(|index| validators.get(index))
                        .map(|validator| validator.pubkey)
                        .ok_or(StoreError::InvalidSlashing)
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Reproduce the actual verification state's previous/current ForkInfo. Target
            // epochs can lie outside that two-version window, so a target-epoch schedule
            // lookup alone would change native indexed-slashing acceptance after restart.
            let epoch = verification_slot / parameters.slots_per_epoch();
            let active = parameters
                .schedule
                .iter()
                .filter(|activation| activation.epoch != u64::MAX && activation.epoch <= epoch)
                .collect::<Vec<_>>();
            let current = active.last().ok_or(StoreError::InvalidParameters)?;
            let previous = active.iter().rev().nth(1).unwrap_or(current);
            let version = if indexed.data.target.epoch < current.epoch {
                previous.version
            } else {
                current.version
            };
            let root = compute_signing_root(
                indexed.data.hash_tree_root(preset)?,
                compute_domain([1, 0, 0, 0], version, genesis),
            );
            fast_aggregate_verify(&keys, &root, indexed.signature)
                .map_err(ProcessingError::from)?;
        }
    }
    Ok(first
        .indices
        .iter()
        .copied()
        .filter(|index| second.indices.binary_search(index).is_ok())
        .collect())
}
fn decode(
    bytes: &[u8],
    fork: Fork,
    validators: Option<&[Validator]>,
    genesis: Root,
    parameters: Option<&TransitionParameters>,
    preset: Preset,
    verification_slot: u64,
) -> Result<BTreeSet<u64>, StoreError> {
    macro_rules! decoded {
        ($module:ident) => {{
            let value = $module::AttesterSlashing::from_ssz(bytes, preset)?;
            pair(
                Indexed {
                    indices: &value.attestation_1.attesting_indices.values,
                    data: &value.attestation_1.data,
                    signature: &value.attestation_1.signature,
                },
                Indexed {
                    indices: &value.attestation_2.attesting_indices.values,
                    data: &value.attestation_2.data,
                    signature: &value.attestation_2.signature,
                },
                validators,
                genesis,
                parameters,
                preset,
                verification_slot,
            )
        }};
    }
    match fork {
        Fork::Gloas => decoded!(gloas),
        Fork::Electra | Fork::Fulu => decoded!(electra),
        _ => decoded!(phase0),
    }
}
/// Decode participants for bounded storage filtering. This does not authenticate signatures,
/// establish an equivocation, or grant permission to change any graph.
pub fn original_participants(
    bytes: &[u8],
    fork: Fork,
    preset: Preset,
) -> Result<BTreeSet<u64>, StoreError> {
    decode(bytes, fork, None, [0; 32], None, preset, 0)
}
pub(crate) fn authenticate(
    bytes: &[u8],
    fork: Fork,
    validators: &[Validator],
    genesis: Root,
    parameters: &TransitionParameters,
    verification_slot: u64,
) -> Result<BTreeSet<u64>, StoreError> {
    decode(
        bytes,
        fork,
        Some(validators),
        genesis,
        Some(parameters),
        parameters.preset(),
        verification_slot,
    )
}

/// Original native indexed-slashing verification clock, created only after genuine kernel success.
/// It carries no validator flags, weights or permission to bypass a signature check.
#[derive(Clone, Copy, Debug)]
pub struct SlashingContext {
    pub(crate) verification_slot: u64,
}
impl SlashingContext {
    /// Actual justified verification state slot; replay derives its configured `ForkInfo` domains.
    pub const fn verification_slot(self) -> u64 {
        self.verification_slot
    }
}
