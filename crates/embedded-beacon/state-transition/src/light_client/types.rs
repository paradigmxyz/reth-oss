use crate::{
    fork_processing::TransitionParameters,
    phase0_processing::ProcessingError,
    ssz::{
        altair::{SyncAggregate, SyncCommittee},
        auxiliary, Preset,
    },
};
use reth_embedded_beacon_crypto::CryptoError;
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Fork, Root, SszError};
use thiserror::Error;

/// Exact verification failures; failed processing leaves the store unchanged.
#[derive(Debug, Error)]
pub enum LightClientError {
    /// Trusted schedule or inherited preset parameters are invalid.
    #[error(transparent)]
    Parameters(#[from] ProcessingError),
    /// Input SSZ fields violate their real collection bounds.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Mandatory sync-committee aggregate signature verification failed.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// The protocol's branch, timing, relevance or consistency rule failed.
    #[error("invalid light-client data: {0}")]
    Invalid(&'static str),
    /// A requested schema downgrade or unavailable Phase0 format is unsupported.
    #[error("unsupported light-client schema {0:?}")]
    Unsupported(Fork),
}

/// Caller-authenticated clock, fork schedule and validator-registry identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LightClientParameters {
    /// The same exact fork schedule and preset used by full consensus transitions.
    pub transitions: TransitionParameters,
    /// Trusted genesis validator-registry root used in every signing domain.
    pub genesis_validators_root: Root,
}
impl LightClientParameters {
    /// Exact published preset defaults; callers must bind the actual network schedule.
    pub fn for_preset(preset: Preset, genesis_validators_root: Root) -> Self {
        Self { transitions: TransitionParameters::for_preset(preset), genesis_validators_root }
    }
    /// Reject invalid inherited schedule, clock, version and preset parameters.
    pub fn validate(&self) -> Result<(), LightClientError> {
        self.transitions.validate()?;
        Ok(())
    }
    /// Collection bounds under the trusted preset.
    pub const fn preset(&self) -> Preset {
        self.transitions.preset()
    }
    /// Slots per epoch under the trusted preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.transitions.slots_per_epoch()
    }
    /// Exact epochs per sync-committee period under the selected preset.
    pub const fn epochs_per_sync_committee_period(&self) -> u64 {
        match self.preset() {
            Preset::Mainnet => 256,
            Preset::Minimal => 8,
        }
    }
    pub(super) fn epoch(&self, fork: Fork) -> u64 {
        self.transitions
            .schedule
            .iter()
            .find(|entry| entry.fork == fork)
            .map_or(u64::MAX, |entry| entry.epoch)
    }
    pub(super) fn version_at_slot(&self, slot: u64) -> Result<[u8; 4], LightClientError> {
        self.validate()?;
        self.transitions
            .schedule
            .iter()
            .rev()
            .find(|entry| entry.epoch != u64::MAX && entry.epoch <= slot / self.slots_per_epoch())
            .map(|entry| entry.version)
            .ok_or(LightClientError::Invalid("no enabled genesis version"))
    }
}

/// Complete fork-specific light-client header; execution fields are never discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Header {
    /// Altair and Bellatrix share the beacon-only header format.
    Altair(auxiliary::altair::LightClientHeader),
    /// Capella authenticates a full execution header within the body.
    Capella(Box<auxiliary::capella::LightClientHeader>),
    /// Deneb, Electra and Fulu share the execution-header format.
    Deneb(Box<auxiliary::deneb::LightClientHeader>),
    /// Gloas authenticates the prior execution block hash via a progressive body proof.
    Gloas(Box<auxiliary::gloas::LightClientHeader>),
}
impl Header {
    /// Beacon header used for sync-committee signing and trusted bootstrap roots.
    pub const fn beacon(&self) -> &BeaconBlockHeader {
        match self {
            Self::Altair(header) => &header.beacon,
            Self::Capella(header) => &header.beacon,
            Self::Deneb(header) => &header.beacon,
            Self::Gloas(header) => &header.beacon,
        }
    }
    /// Construct the exact empty header for the declared data schema.
    pub fn empty(schema: Fork, preset: Preset) -> Result<Self, LightClientError> {
        super::headers::empty_header(schema, preset)
    }
    /// Canonical full header bytes, preserving all authenticated execution fields.
    pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        match self {
            Self::Altair(header) => header.to_ssz(preset),
            Self::Capella(header) => header.to_ssz(preset),
            Self::Deneb(header) => header.to_ssz(preset),
            Self::Gloas(header) => header.to_ssz(preset),
        }
    }
    /// Complete container root, distinct from the signed beacon header root.
    pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        match self {
            Self::Altair(header) => header.hash_tree_root(preset),
            Self::Capella(header) => header.hash_tree_root(preset),
            Self::Deneb(header) => header.hash_tree_root(preset),
            Self::Gloas(header) => header.hash_tree_root(preset),
        }
    }
    /// Decode the exact wire header under its declared fork schema.
    pub fn from_ssz(bytes: &[u8], schema: Fork, preset: Preset) -> Result<Self, LightClientError> {
        match schema {
            Fork::Altair | Fork::Bellatrix => {
                Ok(Self::Altair(auxiliary::altair::LightClientHeader::from_ssz(bytes, preset)?))
            }
            Fork::Capella => Ok(Self::Capella(Box::new(
                auxiliary::capella::LightClientHeader::from_ssz(bytes, preset)?,
            ))),
            Fork::Deneb | Fork::Electra | Fork::Fulu => Ok(Self::Deneb(Box::new(
                auxiliary::deneb::LightClientHeader::from_ssz(bytes, preset)?,
            ))),
            Fork::Gloas => Ok(Self::Gloas(Box::new(
                auxiliary::gloas::LightClientHeader::from_ssz(bytes, preset)?,
            ))),
            Fork::Phase0 => Err(LightClientError::Unsupported(schema)),
        }
    }
    pub(super) const fn matches_schema(&self, schema: Fork) -> bool {
        matches!(
            (self, schema),
            (Self::Altair(_), Fork::Altair | Fork::Bellatrix) |
                (Self::Capella(_), Fork::Capella) |
                (Self::Deneb(_), Fork::Deneb | Fork::Electra | Fork::Fulu) |
                (Self::Gloas(_), Fork::Gloas)
        )
    }
}

/// Bootstrap fields normalized from one exact published wire schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bootstrap {
    /// Original wire schema; determines branch bounds and upgrade rules.
    pub schema: Fork,
    /// Trusted block header with complete execution evidence.
    pub header: Header,
    /// Current committee proved within this header's state root.
    pub current_sync_committee: SyncCommittee,
    /// Exact current-committee branch, including any normalized leading zeros.
    pub current_sync_committee_branch: Vec<Root>,
}
/// Complete light-client update with no caller-supplied validation verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update {
    /// Current data schema; processing requires the store to use the same schema.
    pub schema: Fork,
    /// Header signed by the selected authenticated sync committee.
    pub attested_header: Header,
    /// Next committee, empty when no committee branch is present.
    pub next_sync_committee: SyncCommittee,
    /// Exact normalized next-committee inclusion branch.
    pub next_sync_committee_branch: Vec<Root>,
    /// Finalized beacon header, empty when no finality branch is present.
    pub finalized_header: Header,
    /// Exact normalized finalized-checkpoint-root inclusion branch.
    pub finality_branch: Vec<Root>,
    /// Actual participant bits and mandatory aggregate BLS signature.
    pub sync_aggregate: SyncAggregate,
    /// Untrusted signing slot checked against the local clock and period.
    pub signature_slot: u64,
}
/// Compact finality update expanded with exact empty committee fields before verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalityUpdate {
    /// Original wire schema.
    pub schema: Fork,
    /// Header carrying the finality proof.
    pub attested_header: Header,
    /// Proved finalized header.
    pub finalized_header: Header,
    /// Exact finality branch.
    pub finality_branch: Vec<Root>,
    /// Actual committee aggregate signature.
    pub sync_aggregate: SyncAggregate,
    /// Untrusted signing slot.
    pub signature_slot: u64,
}
/// Compact optimistic update expanded with exact empty finality fields before verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptimisticUpdate {
    /// Original wire schema.
    pub schema: Fork,
    /// Header signed by the authenticated sync committee.
    pub attested_header: Header,
    /// Actual committee aggregate signature.
    pub sync_aggregate: SyncAggregate,
    /// Untrusted signing slot.
    pub signature_slot: u64,
}

/// Light-client store created only from a trusted root and verified bootstrap proof.
/// Read-only accessors prevent callers from injecting an unverified best update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LightClientStore {
    pub(super) schema: Fork,
    pub(super) parameters: LightClientParameters,
    pub(super) finalized_header: Header,
    pub(super) current_sync_committee: SyncCommittee,
    pub(super) next_sync_committee: SyncCommittee,
    pub(super) best_valid_update: Option<Update>,
    pub(super) optimistic_header: Header,
    pub(super) previous_max_active_participants: u64,
    pub(super) current_max_active_participants: u64,
}
impl LightClientStore {
    /// Schema into which subsequent wire data must be upgraded.
    pub const fn schema(&self) -> Fork {
        self.schema
    }
    /// Most recent finalized header (or the explicit optional timeout promotion).
    pub const fn finalized_header(&self) -> &Header {
        &self.finalized_header
    }
    /// Authenticated committee in the finalized period.
    pub const fn current_sync_committee(&self) -> &SyncCommittee {
        &self.current_sync_committee
    }
    /// Authenticated next committee, or the exact empty committee.
    pub const fn next_sync_committee(&self) -> &SyncCommittee {
        &self.next_sync_committee
    }
    /// Best verified update retained for the optional timeout policy.
    pub const fn best_valid_update(&self) -> Option<&Update> {
        self.best_valid_update.as_ref()
    }
    /// Most recent header above the protocol's dynamic safety threshold.
    pub const fn optimistic_header(&self) -> &Header {
        &self.optimistic_header
    }
    /// Maximum observed participation in the preceding finalized committee period.
    pub const fn previous_max_active_participants(&self) -> u64 {
        self.previous_max_active_participants
    }
    /// Maximum observed participation in the current finalized committee period.
    pub const fn current_max_active_participants(&self) -> u64 {
        self.current_max_active_participants
    }
}

pub(super) const fn branch_depths(schema: Fork) -> Result<(usize, usize), LightClientError> {
    match schema {
        Fork::Altair | Fork::Bellatrix | Fork::Capella | Fork::Deneb => Ok((5, 6)),
        Fork::Electra | Fork::Fulu => Ok((6, 7)),
        Fork::Gloas => Ok((11, 9)),
        Fork::Phase0 => Err(LightClientError::Unsupported(schema)),
    }
}
pub(super) const fn committee_size(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 512,
        Preset::Minimal => 32,
    }
}
pub(super) fn empty_committee(preset: Preset) -> SyncCommittee {
    SyncCommittee {
        pubkeys: crate::ssz::Vector::from_vec(vec![[0; 48]; committee_size(preset)]),
        aggregate_pubkey: [0; 48],
    }
}

macro_rules! wire_conversions {
    ($module:ident,$fork:ident) => {
        impl Bootstrap {
            #[doc=concat!("Normalize the complete ", stringify!($module), " bootstrap without dropping proof fields.")]
            pub fn $module(value:auxiliary::$module::LightClientBootstrap,preset:Preset)->Result<Self,LightClientError> {
                Ok(Self{schema:Fork::$fork,header:Header::from_ssz(&value.header.to_ssz(preset)?,Fork::$fork,preset)?,
                    current_sync_committee:SyncCommittee{pubkeys:value.current_sync_committee.pubkeys,aggregate_pubkey:value.current_sync_committee.aggregate_pubkey},
                    current_sync_committee_branch:value.current_sync_committee_branch.values})
            }
        }
        impl Update {
            #[doc=concat!("Normalize the complete ", stringify!($module), " update without dropping proof fields.")]
            pub fn $module(value:auxiliary::$module::LightClientUpdate,preset:Preset)->Result<Self,LightClientError> {
                Ok(Self{schema:Fork::$fork,attested_header:Header::from_ssz(&value.attested_header.to_ssz(preset)?,Fork::$fork,preset)?,
                    next_sync_committee:SyncCommittee{pubkeys:value.next_sync_committee.pubkeys,aggregate_pubkey:value.next_sync_committee.aggregate_pubkey},
                    next_sync_committee_branch:value.next_sync_committee_branch.values,
                    finalized_header:Header::from_ssz(&value.finalized_header.to_ssz(preset)?,Fork::$fork,preset)?,finality_branch:value.finality_branch.values,
                    sync_aggregate:SyncAggregate{sync_committee_bits:value.sync_aggregate.sync_committee_bits,sync_committee_signature:value.sync_aggregate.sync_committee_signature},signature_slot:value.signature_slot})
            }
        }
        impl FinalityUpdate {
            #[doc=concat!("Normalize the complete ", stringify!($module), " finality update.")]
            pub fn $module(value:auxiliary::$module::LightClientFinalityUpdate,preset:Preset)->Result<Self,LightClientError> {
                Ok(Self{schema:Fork::$fork,attested_header:Header::from_ssz(&value.attested_header.to_ssz(preset)?,Fork::$fork,preset)?,
                    finalized_header:Header::from_ssz(&value.finalized_header.to_ssz(preset)?,Fork::$fork,preset)?,finality_branch:value.finality_branch.values,
                    sync_aggregate:SyncAggregate{sync_committee_bits:value.sync_aggregate.sync_committee_bits,sync_committee_signature:value.sync_aggregate.sync_committee_signature},signature_slot:value.signature_slot})
            }
        }
        impl OptimisticUpdate {
            #[doc=concat!("Normalize the complete ", stringify!($module), " optimistic update.")]
            pub fn $module(value:auxiliary::$module::LightClientOptimisticUpdate,preset:Preset)->Result<Self,LightClientError> {
                Ok(Self{schema:Fork::$fork,attested_header:Header::from_ssz(&value.attested_header.to_ssz(preset)?,Fork::$fork,preset)?,
                    sync_aggregate:SyncAggregate{sync_committee_bits:value.sync_aggregate.sync_committee_bits,sync_committee_signature:value.sync_aggregate.sync_committee_signature},signature_slot:value.signature_slot})
            }
        }
    }
}
wire_conversions!(altair, Altair);
wire_conversions!(bellatrix, Bellatrix);
wire_conversions!(capella, Capella);
wire_conversions!(deneb, Deneb);
wire_conversions!(electra, Electra);
wire_conversions!(fulu, Fulu);
wire_conversions!(gloas, Gloas);

macro_rules! wire_decoder {
    ($normalized:ident,$wire:ident) => {
        impl $normalized {
            /// Decode the complete declared wire schema and retain every authenticated field.
            pub fn from_ssz(
                bytes: &[u8],
                schema: Fork,
                preset: Preset,
            ) -> Result<Self, LightClientError> {
                match schema {
                    Fork::Altair => {
                        Self::altair(auxiliary::altair::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Bellatrix => Self::bellatrix(
                        auxiliary::bellatrix::$wire::from_ssz(bytes, preset)?,
                        preset,
                    ),
                    Fork::Capella => {
                        Self::capella(auxiliary::capella::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Deneb => {
                        Self::deneb(auxiliary::deneb::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Electra => {
                        Self::electra(auxiliary::electra::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Fulu => {
                        Self::fulu(auxiliary::fulu::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Gloas => {
                        Self::gloas(auxiliary::gloas::$wire::from_ssz(bytes, preset)?, preset)
                    }
                    Fork::Phase0 => Err(LightClientError::Unsupported(schema)),
                }
            }
        }
    };
}
wire_decoder!(Bootstrap, LightClientBootstrap);
wire_decoder!(Update, LightClientUpdate);
wire_decoder!(FinalityUpdate, LightClientFinalityUpdate);
wire_decoder!(OptimisticUpdate, LightClientOptimisticUpdate);
