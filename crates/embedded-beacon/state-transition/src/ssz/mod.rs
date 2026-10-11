//! Complete fork-aware state/block SSZ schemas pinned to consensus-specs v1.7.0-beta.3.
//!
//! Canonical schemas and roots do not establish transition or signature validity.

use reth_embedded_beacon_primitives::{BeaconBlockHeader, Fork, Root, SszError};
use thiserror::Error;

pub mod altair;
pub mod auxiliary;
pub mod bellatrix;
pub mod capella;
mod codec;
pub(crate) use codec::encode_container;
pub mod deneb;
pub mod electra;
pub mod fulu;
pub mod gloas;
pub mod json;
mod parallel_roots;
pub mod phase0;

pub use codec::{
    BitListValue, Bits, Fixed, Length, List, Preset, PresetBitList, PresetBits, PresetLength,
    ProgressiveBitList, ProgressiveList, Ssz, Vector,
};

/// Rejected complete schema data.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Bytes violate the selected complete schema or its preset bounds.
    #[error(transparent)]
    Ssz(#[from] SszError),
}

/// Completely typed `BeaconState` with an explicitly selected schema and preset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeaconState {
    /// Complete Phase0 container without implied consensus validity.
    Phase0(phase0::BeaconState, Preset),
    /// Complete Altair container without implied consensus validity.
    Altair(altair::BeaconState, Preset),
    /// Complete Bellatrix container without implied consensus validity.
    Bellatrix(bellatrix::BeaconState, Preset),
    /// Complete Capella container without implied consensus validity.
    Capella(capella::BeaconState, Preset),
    /// Complete Deneb container without implied consensus validity.
    Deneb(deneb::BeaconState, Preset),
    /// Complete Electra container without implied consensus validity.
    Electra(electra::BeaconState, Preset),
    /// Complete Fulu container without implied consensus validity.
    Fulu(fulu::BeaconState, Preset),
    /// Complete Gloas container without implied consensus validity.
    Gloas(gloas::BeaconState, Preset),
}

impl BeaconState {
    /// Recursively count exact owned allocation capacities, excluding this wrapper's inline size.
    /// Allocator bookkeeping and transient encoding/validation buffers are not included.
    pub fn heap_bytes(&self) -> Result<usize, SszError> {
        match self {
            Self::Phase0(value, _) => Ssz::heap_bytes(value),
            Self::Altair(value, _) => Ssz::heap_bytes(value),
            Self::Bellatrix(value, _) => Ssz::heap_bytes(value),
            Self::Capella(value, _) => Ssz::heap_bytes(value),
            Self::Deneb(value, _) => Ssz::heap_bytes(value),
            Self::Electra(value, _) => Ssz::heap_bytes(value),
            Self::Fulu(value, _) => Ssz::heap_bytes(value),
            Self::Gloas(value, _) => Ssz::heap_bytes(value),
        }
    }

    /// Decode every nested field under the caller-selected fork and exact preset.
    pub fn from_ssz(bytes: &[u8], fork: Fork, preset: Preset) -> Result<Self, SchemaError> {
        Ok(match fork {
            Fork::Phase0 => Self::Phase0(phase0::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Altair => Self::Altair(altair::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Bellatrix => {
                Self::Bellatrix(bellatrix::BeaconState::from_ssz(bytes, preset)?, preset)
            }
            Fork::Capella => Self::Capella(capella::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Deneb => Self::Deneb(deneb::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Electra => Self::Electra(electra::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Fulu => Self::Fulu(fulu::BeaconState::from_ssz(bytes, preset)?, preset),
            Fork::Gloas => Self::Gloas(gloas::BeaconState::from_ssz(bytes, preset)?, preset),
        })
    }

    /// Encode every field, rechecking structural collection bounds.
    pub fn to_ssz(&self) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Phase0(value, preset) => value.to_ssz(*preset),
            Self::Altair(value, preset) => value.to_ssz(*preset),
            Self::Bellatrix(value, preset) => value.to_ssz(*preset),
            Self::Capella(value, preset) => value.to_ssz(*preset),
            Self::Deneb(value, preset) => value.to_ssz(*preset),
            Self::Electra(value, preset) => value.to_ssz(*preset),
            Self::Fulu(value, preset) => value.to_ssz(*preset),
            Self::Gloas(value, preset) => value.to_ssz(*preset),
        }
    }

    /// Compute the complete selected schema root; never a checksum.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        match self {
            Self::Phase0(value, preset) => value.hash_tree_root(*preset),
            Self::Altair(value, preset) => value.hash_tree_root(*preset),
            Self::Bellatrix(value, preset) => value.hash_tree_root(*preset),
            Self::Capella(value, preset) => value.hash_tree_root(*preset),
            Self::Deneb(value, preset) => value.hash_tree_root(*preset),
            Self::Electra(value, preset) => value.hash_tree_root(*preset),
            Self::Fulu(value, preset) => value.hash_tree_root(*preset),
            Self::Gloas(value, preset) => value.hash_tree_root(*preset),
        }
    }

    /// Recheck the complete selected schema's bounds without creating root authority.
    /// Required state commitments still use `hash_tree_root` at their protocol boundaries.
    pub(crate) fn validate_structure(&self) -> Result<(), SszError> {
        match self {
            Self::Phase0(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Altair(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Bellatrix(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Capella(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Deneb(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Electra(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Fulu(value, preset) => Ssz::validate_structure(value, *preset),
            Self::Gloas(value, preset) => Ssz::validate_structure(value, *preset),
        }
    }

    /// Exact fork schema retained by the decoded wrapper.
    pub const fn schema_fork(&self) -> Fork {
        match self {
            Self::Phase0(_, _) => Fork::Phase0,
            Self::Altair(_, _) => Fork::Altair,
            Self::Bellatrix(_, _) => Fork::Bellatrix,
            Self::Capella(_, _) => Fork::Capella,
            Self::Deneb(_, _) => Fork::Deneb,
            Self::Electra(_, _) => Fork::Electra,
            Self::Fulu(_, _) => Fork::Fulu,
            Self::Gloas(_, _) => Fork::Gloas,
        }
    }

    /// Exact preset carried by this complete structural state.
    pub const fn preset(&self) -> Preset {
        match self {
            Self::Phase0(_, preset) |
            Self::Altair(_, preset) |
            Self::Bellatrix(_, preset) |
            Self::Capella(_, preset) |
            Self::Deneb(_, preset) |
            Self::Electra(_, preset) |
            Self::Fulu(_, preset) |
            Self::Gloas(_, preset) => *preset,
        }
    }

    /// Ordered canonical field widths for the explicitly selected complete state schema.
    pub fn container_field_layout(fork: Fork, preset: Preset) -> Vec<Option<usize>> {
        match fork {
            Fork::Phase0 => phase0::BeaconState::container_field_layout(preset),
            Fork::Altair => altair::BeaconState::container_field_layout(preset),
            Fork::Bellatrix => bellatrix::BeaconState::container_field_layout(preset),
            Fork::Capella => capella::BeaconState::container_field_layout(preset),
            Fork::Deneb => deneb::BeaconState::container_field_layout(preset),
            Fork::Electra => electra::BeaconState::container_field_layout(preset),
            Fork::Fulu => fulu::BeaconState::container_field_layout(preset),
            Fork::Gloas => gloas::BeaconState::container_field_layout(preset),
        }
    }
    /// Decoded state slot.
    pub const fn slot(&self) -> u64 {
        match self {
            Self::Phase0(value, _) => value.slot,
            Self::Altair(value, _) => value.slot,
            Self::Bellatrix(value, _) => value.slot,
            Self::Capella(value, _) => value.slot,
            Self::Deneb(value, _) => value.slot,
            Self::Electra(value, _) => value.slot,
            Self::Fulu(value, _) => value.slot,
            Self::Gloas(value, _) => value.slot,
        }
    }

    /// Decoded consensus genesis time.
    pub const fn genesis_time(&self) -> u64 {
        match self {
            Self::Phase0(value, _) => value.genesis_time,
            Self::Altair(value, _) => value.genesis_time,
            Self::Bellatrix(value, _) => value.genesis_time,
            Self::Capella(value, _) => value.genesis_time,
            Self::Deneb(value, _) => value.genesis_time,
            Self::Electra(value, _) => value.genesis_time,
            Self::Fulu(value, _) => value.genesis_time,
            Self::Gloas(value, _) => value.genesis_time,
        }
    }

    /// Decoded genesis validator registry root.
    pub const fn genesis_validators_root(&self) -> Root {
        match self {
            Self::Phase0(value, _) => value.genesis_validators_root,
            Self::Altair(value, _) => value.genesis_validators_root,
            Self::Bellatrix(value, _) => value.genesis_validators_root,
            Self::Capella(value, _) => value.genesis_validators_root,
            Self::Deneb(value, _) => value.genesis_validators_root,
            Self::Electra(value, _) => value.genesis_validators_root,
            Self::Fulu(value, _) => value.genesis_validators_root,
            Self::Gloas(value, _) => value.genesis_validators_root,
        }
    }

    /// Decoded latest header; normal state-root normalization remains required.
    pub const fn latest_block_header(&self) -> BeaconBlockHeader {
        match self {
            Self::Phase0(value, _) => value.latest_block_header,
            Self::Altair(value, _) => value.latest_block_header,
            Self::Bellatrix(value, _) => value.latest_block_header,
            Self::Capella(value, _) => value.latest_block_header,
            Self::Deneb(value, _) => value.latest_block_header,
            Self::Electra(value, _) => value.latest_block_header,
            Self::Fulu(value, _) => value.latest_block_header,
            Self::Gloas(value, _) => value.latest_block_header,
        }
    }

    /// Previous/current fork versions and their activation epoch.
    pub fn fork_info(&self) -> phase0::ForkInfo {
        match self {
            Self::Phase0(value, _) => value.fork.clone(),
            Self::Altair(value, _) => value.fork.clone(),
            Self::Bellatrix(value, _) => value.fork.clone(),
            Self::Capella(value, _) => value.fork.clone(),
            Self::Deneb(value, _) => value.fork.clone(),
            Self::Electra(value, _) => value.fork.clone(),
            Self::Fulu(value, _) => value.fork.clone(),
            Self::Gloas(value, _) => value.fork.clone(),
        }
    }
}

/// Completely typed `BeaconBlock` with an explicitly selected schema and preset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeaconBlock {
    /// Complete Phase0 container without implied consensus validity.
    Phase0(phase0::BeaconBlock, Preset),
    /// Complete Altair container without implied consensus validity.
    Altair(altair::BeaconBlock, Preset),
    /// Complete Bellatrix container without implied consensus validity.
    Bellatrix(bellatrix::BeaconBlock, Preset),
    /// Complete Capella container without implied consensus validity.
    Capella(capella::BeaconBlock, Preset),
    /// Complete Deneb container without implied consensus validity.
    Deneb(deneb::BeaconBlock, Preset),
    /// Complete Electra container without implied consensus validity.
    Electra(electra::BeaconBlock, Preset),
    /// Complete Fulu container without implied consensus validity.
    Fulu(fulu::BeaconBlock, Preset),
    /// Complete Gloas container without implied consensus validity.
    Gloas(gloas::BeaconBlock, Preset),
}

impl BeaconBlock {
    /// Recursively count exact owned allocation capacities, excluding this wrapper's inline size.
    /// Allocator bookkeeping and transient encoding/validation buffers are not included.
    pub fn heap_bytes(&self) -> Result<usize, SszError> {
        match self {
            Self::Phase0(value, _) => Ssz::heap_bytes(value),
            Self::Altair(value, _) => Ssz::heap_bytes(value),
            Self::Bellatrix(value, _) => Ssz::heap_bytes(value),
            Self::Capella(value, _) => Ssz::heap_bytes(value),
            Self::Deneb(value, _) => Ssz::heap_bytes(value),
            Self::Electra(value, _) => Ssz::heap_bytes(value),
            Self::Fulu(value, _) => Ssz::heap_bytes(value),
            Self::Gloas(value, _) => Ssz::heap_bytes(value),
        }
    }

    /// Decode every nested field under the caller-selected fork and exact preset.
    pub fn from_ssz(bytes: &[u8], fork: Fork, preset: Preset) -> Result<Self, SchemaError> {
        Ok(match fork {
            Fork::Phase0 => Self::Phase0(phase0::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Altair => Self::Altair(altair::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Bellatrix => {
                Self::Bellatrix(bellatrix::BeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Capella => Self::Capella(capella::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Deneb => Self::Deneb(deneb::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Electra => Self::Electra(electra::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Fulu => Self::Fulu(fulu::BeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Gloas => Self::Gloas(gloas::BeaconBlock::from_ssz(bytes, preset)?, preset),
        })
    }

    /// Encode every field, rechecking structural collection bounds.
    pub fn to_ssz(&self) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Phase0(value, preset) => value.to_ssz(*preset),
            Self::Altair(value, preset) => value.to_ssz(*preset),
            Self::Bellatrix(value, preset) => value.to_ssz(*preset),
            Self::Capella(value, preset) => value.to_ssz(*preset),
            Self::Deneb(value, preset) => value.to_ssz(*preset),
            Self::Electra(value, preset) => value.to_ssz(*preset),
            Self::Fulu(value, preset) => value.to_ssz(*preset),
            Self::Gloas(value, preset) => value.to_ssz(*preset),
        }
    }

    /// Compute the complete selected schema root; never a checksum.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        match self {
            Self::Phase0(value, preset) => value.hash_tree_root(*preset),
            Self::Altair(value, preset) => value.hash_tree_root(*preset),
            Self::Bellatrix(value, preset) => value.hash_tree_root(*preset),
            Self::Capella(value, preset) => value.hash_tree_root(*preset),
            Self::Deneb(value, preset) => value.hash_tree_root(*preset),
            Self::Electra(value, preset) => value.hash_tree_root(*preset),
            Self::Fulu(value, preset) => value.hash_tree_root(*preset),
            Self::Gloas(value, preset) => value.hash_tree_root(*preset),
        }
    }

    /// Exact fork schema retained by the decoded wrapper.
    pub const fn schema_fork(&self) -> Fork {
        match self {
            Self::Phase0(_, _) => Fork::Phase0,
            Self::Altair(_, _) => Fork::Altair,
            Self::Bellatrix(_, _) => Fork::Bellatrix,
            Self::Capella(_, _) => Fork::Capella,
            Self::Deneb(_, _) => Fork::Deneb,
            Self::Electra(_, _) => Fork::Electra,
            Self::Fulu(_, _) => Fork::Fulu,
            Self::Gloas(_, _) => Fork::Gloas,
        }
    }

    /// Unsigned block header derived from the complete decoded body.
    pub fn header(&self) -> Result<BeaconBlockHeader, SszError> {
        match self {
            Self::Phase0(value, preset) => value.header(*preset),
            Self::Altair(value, preset) => value.header(*preset),
            Self::Bellatrix(value, preset) => value.header(*preset),
            Self::Capella(value, preset) => value.header(*preset),
            Self::Deneb(value, preset) => value.header(*preset),
            Self::Electra(value, preset) => value.header(*preset),
            Self::Fulu(value, preset) => value.header(*preset),
            Self::Gloas(value, preset) => value.header(*preset),
        }
    }
}

/// Completely typed `SignedBeaconBlock` with an explicitly selected schema and preset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignedBeaconBlock {
    /// Complete Phase0 container without implied consensus validity.
    Phase0(phase0::SignedBeaconBlock, Preset),
    /// Complete Altair container without implied consensus validity.
    Altair(altair::SignedBeaconBlock, Preset),
    /// Complete Bellatrix container without implied consensus validity.
    Bellatrix(bellatrix::SignedBeaconBlock, Preset),
    /// Complete Capella container without implied consensus validity.
    Capella(capella::SignedBeaconBlock, Preset),
    /// Complete Deneb container without implied consensus validity.
    Deneb(deneb::SignedBeaconBlock, Preset),
    /// Complete Electra container without implied consensus validity.
    Electra(electra::SignedBeaconBlock, Preset),
    /// Complete Fulu container without implied consensus validity.
    Fulu(fulu::SignedBeaconBlock, Preset),
    /// Complete Gloas container without implied consensus validity.
    Gloas(gloas::SignedBeaconBlock, Preset),
}

impl SignedBeaconBlock {
    /// Recursively count exact owned allocation capacities, excluding this wrapper's inline size.
    /// Allocator bookkeeping and transient encoding/validation buffers are not included.
    pub fn heap_bytes(&self) -> Result<usize, SszError> {
        match self {
            Self::Phase0(value, _) => Ssz::heap_bytes(value),
            Self::Altair(value, _) => Ssz::heap_bytes(value),
            Self::Bellatrix(value, _) => Ssz::heap_bytes(value),
            Self::Capella(value, _) => Ssz::heap_bytes(value),
            Self::Deneb(value, _) => Ssz::heap_bytes(value),
            Self::Electra(value, _) => Ssz::heap_bytes(value),
            Self::Fulu(value, _) => Ssz::heap_bytes(value),
            Self::Gloas(value, _) => Ssz::heap_bytes(value),
        }
    }

    /// Decode every nested field under the caller-selected fork and exact preset.
    pub fn from_ssz(bytes: &[u8], fork: Fork, preset: Preset) -> Result<Self, SchemaError> {
        Ok(match fork {
            Fork::Phase0 => {
                Self::Phase0(phase0::SignedBeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Altair => {
                Self::Altair(altair::SignedBeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Bellatrix => {
                Self::Bellatrix(bellatrix::SignedBeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Capella => {
                Self::Capella(capella::SignedBeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Deneb => Self::Deneb(deneb::SignedBeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Electra => {
                Self::Electra(electra::SignedBeaconBlock::from_ssz(bytes, preset)?, preset)
            }
            Fork::Fulu => Self::Fulu(fulu::SignedBeaconBlock::from_ssz(bytes, preset)?, preset),
            Fork::Gloas => Self::Gloas(gloas::SignedBeaconBlock::from_ssz(bytes, preset)?, preset),
        })
    }

    /// Encode every field, rechecking structural collection bounds.
    pub fn to_ssz(&self) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Phase0(value, preset) => value.to_ssz(*preset),
            Self::Altair(value, preset) => value.to_ssz(*preset),
            Self::Bellatrix(value, preset) => value.to_ssz(*preset),
            Self::Capella(value, preset) => value.to_ssz(*preset),
            Self::Deneb(value, preset) => value.to_ssz(*preset),
            Self::Electra(value, preset) => value.to_ssz(*preset),
            Self::Fulu(value, preset) => value.to_ssz(*preset),
            Self::Gloas(value, preset) => value.to_ssz(*preset),
        }
    }

    /// Compute the complete selected schema root; never a checksum.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        match self {
            Self::Phase0(value, preset) => value.hash_tree_root(*preset),
            Self::Altair(value, preset) => value.hash_tree_root(*preset),
            Self::Bellatrix(value, preset) => value.hash_tree_root(*preset),
            Self::Capella(value, preset) => value.hash_tree_root(*preset),
            Self::Deneb(value, preset) => value.hash_tree_root(*preset),
            Self::Electra(value, preset) => value.hash_tree_root(*preset),
            Self::Fulu(value, preset) => value.hash_tree_root(*preset),
            Self::Gloas(value, preset) => value.hash_tree_root(*preset),
        }
    }

    /// Exact fork schema retained by the decoded wrapper.
    pub const fn schema_fork(&self) -> Fork {
        match self {
            Self::Phase0(_, _) => Fork::Phase0,
            Self::Altair(_, _) => Fork::Altair,
            Self::Bellatrix(_, _) => Fork::Bellatrix,
            Self::Capella(_, _) => Fork::Capella,
            Self::Deneb(_, _) => Fork::Deneb,
            Self::Electra(_, _) => Fork::Electra,
            Self::Fulu(_, _) => Fork::Fulu,
            Self::Gloas(_, _) => Fork::Gloas,
        }
    }

    /// Unsigned block header derived from the complete decoded body.
    pub fn header(&self) -> Result<BeaconBlockHeader, SszError> {
        match self {
            Self::Phase0(value, preset) => value.message.header(*preset),
            Self::Altair(value, preset) => value.message.header(*preset),
            Self::Bellatrix(value, preset) => value.message.header(*preset),
            Self::Capella(value, preset) => value.message.header(*preset),
            Self::Deneb(value, preset) => value.message.header(*preset),
            Self::Electra(value, preset) => value.message.header(*preset),
            Self::Fulu(value, preset) => value.message.header(*preset),
            Self::Gloas(value, preset) => value.message.header(*preset),
        }
    }

    /// Unsigned message root used for checkpoint and block identity.
    pub fn message_root(&self) -> Result<Root, SszError> {
        match self {
            Self::Phase0(value, preset) => value.message.hash_tree_root(*preset),
            Self::Altair(value, preset) => value.message.hash_tree_root(*preset),
            Self::Bellatrix(value, preset) => value.message.hash_tree_root(*preset),
            Self::Capella(value, preset) => value.message.hash_tree_root(*preset),
            Self::Deneb(value, preset) => value.message.hash_tree_root(*preset),
            Self::Electra(value, preset) => value.message.hash_tree_root(*preset),
            Self::Fulu(value, preset) => value.message.hash_tree_root(*preset),
            Self::Gloas(value, preset) => value.message.hash_tree_root(*preset),
        }
    }

    /// Complete unsigned message with the same preset; cloning copies all body fields.
    pub fn message(&self) -> BeaconBlock {
        match self {
            Self::Phase0(value, preset) => BeaconBlock::Phase0(value.message.clone(), *preset),
            Self::Altair(value, preset) => BeaconBlock::Altair(value.message.clone(), *preset),
            Self::Bellatrix(value, preset) => {
                BeaconBlock::Bellatrix(value.message.clone(), *preset)
            }
            Self::Capella(value, preset) => BeaconBlock::Capella(value.message.clone(), *preset),
            Self::Deneb(value, preset) => BeaconBlock::Deneb(value.message.clone(), *preset),
            Self::Electra(value, preset) => BeaconBlock::Electra(value.message.clone(), *preset),
            Self::Fulu(value, preset) => BeaconBlock::Fulu(value.message.clone(), *preset),
            Self::Gloas(value, preset) => BeaconBlock::Gloas(value.message.clone(), *preset),
        }
    }
}
