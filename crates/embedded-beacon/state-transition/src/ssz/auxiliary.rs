//! Complete additional pinned primary schemas.
//! Collection bounds and fork-specific layouts are retained exactly.
//! Decoding grants no signature or state-transition authority.

/// Additional Phase0 containers with complete inherited field types.
pub mod phase0 {
    use super::super::{codec::ssz_container, Preset, Ssz};
    use reth_embedded_beacon_primitives::{Root, SszError};

    ssz_container! {
        /// Complete pinned Phase0 `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::phase0::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Phase0 `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Phase0 collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Phase0 `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Phase0 `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Phase0 `HistoricalBatch` container.
        HistoricalBatch {
            /// Specification field `block_roots`.
            block_roots: super::super::phase0::BlockRoots,
            /// Specification field `state_roots`.
            state_roots: super::super::phase0::StateRoots,
        }
    }

    ssz_container! {
        /// Complete pinned Phase0 `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Phase0 `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }
}

/// Additional Altair containers with complete inherited field types.
pub mod altair {
    use super::super::{
        codec::ssz_container, Fixed, Preset, PresetBits, PresetLength, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Altair `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::altair::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Altair `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Altair collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Altair `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Altair collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Altair `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Altair collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Altair `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `HistoricalBatch` container.
        HistoricalBatch {
            /// Specification field `block_roots`.
            block_roots: super::super::altair::BlockRoots,
            /// Specification field `state_roots`.
            state_roots: super::super::altair::StateRoots,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::altair::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::altair::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::altair::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::altair::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::altair::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    /// Exact pinned Altair collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Altair `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Altair `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Altair `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Altair `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Altair `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Altair collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;
}

/// Additional Bellatrix containers with complete inherited field types.
pub mod bellatrix {
    use super::super::{
        codec::ssz_container, Fixed, Preset, PresetBits, PresetLength, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Bellatrix `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::bellatrix::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Bellatrix collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Bellatrix `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Bellatrix collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Bellatrix `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Bellatrix collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Bellatrix `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `HistoricalBatch` container.
        HistoricalBatch {
            /// Specification field `block_roots`.
            block_roots: super::super::bellatrix::BlockRoots,
            /// Specification field `state_roots`.
            state_roots: super::super::bellatrix::StateRoots,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::bellatrix::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::bellatrix::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::bellatrix::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::bellatrix::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::bellatrix::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::bellatrix::ExecutionPayload,
        }
    }

    /// Exact pinned Bellatrix collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Bellatrix `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Bellatrix `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Bellatrix collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;
}

/// Additional Capella containers with complete inherited field types.
pub mod capella {
    use super::super::{
        codec::ssz_container, Fixed, Preset, PresetBits, PresetLength, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Capella `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::capella::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Capella `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Capella collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Capella `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Capella collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Capella `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Capella collection bounds.
    pub type ExecutionBranch = Vector<Root, Fixed<4>>;

    /// Exact pinned Capella collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Capella `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::capella::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::capella::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
            /// Specification field `execution`.
            execution: super::super::capella::ExecutionPayloadHeader,
            /// Specification field `execution_branch`.
            execution_branch: ExecutionBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::capella::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::capella::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::capella::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::capella::ExecutionPayload,
        }
    }

    /// Exact pinned Capella collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Capella `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Capella `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Capella collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;
}

/// Additional Deneb containers with complete inherited field types.
pub mod deneb {
    use super::super::{
        codec::ssz_container, Fixed, List, Preset, PresetBits, PresetLength, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Deneb `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::deneb::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type Blob = Vector<u8, Fixed<131072>>;

    ssz_container! {
        /// Complete pinned Deneb `BlobIdentifier` container.
        BlobIdentifier {
            /// Specification field `block_root`.
            block_root: Root,
            /// Specification field `index`.
            index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `BlobSidecar` container.
        BlobSidecar {
            /// Specification field `index`.
            index: u64,
            /// Specification field `blob`.
            blob: Blob,
            /// Specification field `kzg_commitment`.
            kzg_commitment: [u8; 48],
            /// Specification field `kzg_proof`.
            kzg_proof: [u8; 48],
            /// Specification field `signed_block_header`.
            signed_block_header: super::super::phase0::SignedBeaconBlockHeader,
            /// Specification field `kzg_commitment_inclusion_proof`.
            kzg_commitment_inclusion_proof: KZGCommitmentInclusionProof,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Deneb `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Deneb `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type ExecutionBranch = Vector<Root, Fixed<4>>;

    /// Exact pinned Deneb collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Deneb `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type KZGCommitmentInclusionProof = Vector<Root, Fixed<17>>;

    ssz_container! {
        /// Complete pinned Deneb `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::deneb::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::deneb::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
            /// Specification field `execution`.
            execution: super::super::deneb::ExecutionPayloadHeader,
            /// Specification field `execution_branch`.
            execution_branch: ExecutionBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::deneb::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::deneb::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::deneb::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::deneb::ExecutionPayload,
            /// Specification field `versioned_hashes`.
            versioned_hashes: VersionedHashes,
            /// Specification field `parent_beacon_block_root`.
            parent_beacon_block_root: Root,
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<5>>;

    ssz_container! {
        /// Complete pinned Deneb `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Deneb `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Deneb collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;

    /// Exact pinned Deneb collection bounds.
    pub type VersionedHashes = List<Root, Fixed<4096>>;
}

/// Additional Electra containers with complete inherited field types.
pub mod electra {
    use super::super::{
        codec::ssz_container, Fixed, List, Preset, PresetBits, PresetLength, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Electra `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::electra::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type Blob = Vector<u8, Fixed<131072>>;

    ssz_container! {
        /// Complete pinned Electra `BlobIdentifier` container.
        BlobIdentifier {
            /// Specification field `block_root`.
            block_root: Root,
            /// Specification field `index`.
            index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `BlobSidecar` container.
        BlobSidecar {
            /// Specification field `index`.
            index: u64,
            /// Specification field `blob`.
            blob: Blob,
            /// Specification field `kzg_commitment`.
            kzg_commitment: [u8; 48],
            /// Specification field `kzg_proof`.
            kzg_proof: [u8; 48],
            /// Specification field `signed_block_header`.
            signed_block_header: super::super::phase0::SignedBeaconBlockHeader,
            /// Specification field `kzg_commitment_inclusion_proof`.
            kzg_commitment_inclusion_proof: KZGCommitmentInclusionProof,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Electra `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Electra `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type ExecutionBranch = Vector<Root, Fixed<4>>;

    /// Exact pinned Electra collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<7>>;

    ssz_container! {
        /// Complete pinned Electra `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type KZGCommitmentInclusionProof = Vector<Root, Fixed<17>>;

    ssz_container! {
        /// Complete pinned Electra `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::electra::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::electra::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
            /// Specification field `execution`.
            execution: super::super::electra::ExecutionPayloadHeader,
            /// Specification field `execution_branch`.
            execution_branch: ExecutionBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::electra::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::electra::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::electra::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::electra::ExecutionPayload,
            /// Specification field `versioned_hashes`.
            versioned_hashes: VersionedHashes,
            /// Specification field `parent_beacon_block_root`.
            parent_beacon_block_root: Root,
            /// Specification field `execution_requests`.
            execution_requests: super::super::electra::ExecutionRequests,
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<6>>;

    ssz_container! {
        /// Complete pinned Electra `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SingleAttestation` container.
        SingleAttestation {
            /// Specification field `committee_index`.
            committee_index: u64,
            /// Specification field `attester_index`.
            attester_index: u64,
            /// Specification field `data`.
            data: super::super::electra::AttestationData,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Electra `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Electra collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;

    /// Exact pinned Electra collection bounds.
    pub type VersionedHashes = List<Root, Fixed<4096>>;
}

/// Additional Fulu containers with complete inherited field types.
pub mod fulu {
    use super::super::{
        codec::ssz_container, Fixed, List, Preset, PresetBitList, PresetBits, PresetLength, Ssz,
        Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Fulu `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::fulu::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type Cell = Vector<u8, Fixed<2048>>;

    /// Exact pinned Fulu collection bounds.
    pub type CellsBitList = PresetBitList<Fixed<4096>>;

    ssz_container! {
        /// Complete pinned Fulu `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<6>>;

    /// Exact pinned Fulu collection bounds.
    pub type DataColumn = List<Cell, Fixed<4096>>;

    /// Exact pinned Fulu collection bounds.
    pub type DataColumnIndices = List<u64, Fixed<128>>;

    ssz_container! {
        /// Complete pinned Fulu `DataColumnSidecar` container.
        DataColumnSidecar {
            /// Specification field `index`.
            index: u64,
            /// Specification field `column`.
            column: DataColumn,
            /// Specification field `kzg_commitments`.
            kzg_commitments: super::super::fulu::BlobKZGCommitments,
            /// Specification field `kzg_proofs`.
            kzg_proofs: KZGProofs,
            /// Specification field `signed_block_header`.
            signed_block_header: super::super::phase0::SignedBeaconBlockHeader,
            /// Specification field `kzg_commitments_inclusion_proof`.
            kzg_commitments_inclusion_proof: KZGCommitmentsInclusionProof,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `DataColumnsByRootIdentifier` container.
        DataColumnsByRootIdentifier {
            /// Specification field `block_root`.
            block_root: Root,
            /// Specification field `columns`.
            columns: DataColumnIndices,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Fulu `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type ExecutionBranch = Vector<Root, Fixed<4>>;

    /// Exact pinned Fulu collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<7>>;

    ssz_container! {
        /// Complete pinned Fulu `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type KZGCommitmentsInclusionProof = Vector<Root, Fixed<4>>;

    /// Exact pinned Fulu collection bounds.
    pub type KZGProofs = List<[u8; 48], Fixed<4096>>;

    ssz_container! {
        /// Complete pinned Fulu `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::fulu::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::fulu::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
            /// Specification field `execution`.
            execution: super::super::fulu::ExecutionPayloadHeader,
            /// Specification field `execution_branch`.
            execution_branch: ExecutionBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::fulu::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::fulu::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::fulu::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `MatrixEntry` container.
        MatrixEntry {
            /// Specification field `cell`.
            cell: Cell,
            /// Specification field `kzg_proof`.
            kzg_proof: [u8; 48],
            /// Specification field `column_index`.
            column_index: u64,
            /// Specification field `row_index`.
            row_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::fulu::ExecutionPayload,
            /// Specification field `versioned_hashes`.
            versioned_hashes: VersionedHashes,
            /// Specification field `parent_beacon_block_root`.
            parent_beacon_block_root: Root,
            /// Specification field `execution_requests`.
            execution_requests: super::super::fulu::ExecutionRequests,
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<6>>;

    /// Exact pinned Fulu collection bounds.
    pub type OptionalPartialDataColumnHeader = List<PartialDataColumnHeader, Fixed<1>>;

    ssz_container! {
        /// Complete pinned Fulu `PartialDataColumnGroupID` container.
        PartialDataColumnGroupID {
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `PartialDataColumnHeader` container.
        PartialDataColumnHeader {
            /// Specification field `kzg_commitments`.
            kzg_commitments: super::super::fulu::BlobKZGCommitments,
            /// Specification field `signed_block_header`.
            signed_block_header: super::super::phase0::SignedBeaconBlockHeader,
            /// Specification field `kzg_commitments_inclusion_proof`.
            kzg_commitments_inclusion_proof: KZGCommitmentsInclusionProof,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `PartialDataColumnPartsMetadata` container.
        PartialDataColumnPartsMetadata {
            /// Specification field `available`.
            available: CellsBitList,
            /// Specification field `requests`.
            requests: CellsBitList,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `PartialDataColumnSidecar` container.
        PartialDataColumnSidecar {
            /// Specification field `cells_present_bitmap`.
            cells_present_bitmap: CellsBitList,
            /// Specification field `partial_column`.
            partial_column: DataColumn,
            /// Specification field `kzg_proofs`.
            kzg_proofs: KZGProofs,
            /// Specification field `header`.
            header: OptionalPartialDataColumnHeader,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SingleAttestation` container.
        SingleAttestation {
            /// Specification field `committee_index`.
            committee_index: u64,
            /// Specification field `attester_index`.
            attester_index: u64,
            /// Specification field `data`.
            data: super::super::fulu::AttestationData,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Fulu `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Fulu collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;

    /// Exact pinned Fulu collection bounds.
    pub type VersionedHashes = List<Root, Fixed<4096>>;
}

/// Additional Gloas containers with complete inherited field types.
pub mod gloas {
    use super::super::{
        codec::ssz_container, Fixed, List, Preset, PresetBits, PresetLength, ProgressiveBitList,
        ProgressiveList, Ssz, Vector,
    };
    use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root, SszError};

    ssz_container! {
        /// Complete pinned Gloas `AggregateAndProof` container.
        AggregateAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `aggregate`.
            aggregate: super::super::gloas::Attestation,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type Cell = Vector<u8, Fixed<2048>>;

    /// Exact pinned Gloas collection bounds.
    pub type CellsBitList = ProgressiveBitList;

    ssz_container! {
        /// Complete pinned Gloas `ContributionAndProof` container.
        ContributionAndProof {
            /// Specification field `aggregator_index`.
            aggregator_index: u64,
            /// Specification field `contribution`.
            contribution: SyncCommitteeContribution,
            /// Specification field `selection_proof`.
            selection_proof: [u8; 96],
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type CurrentSyncCommitteeBranch = Vector<Root, Fixed<11>>;

    /// Exact pinned Gloas collection bounds.
    pub type DataColumn = ProgressiveList<Cell>;

    /// Exact pinned Gloas collection bounds.
    pub type DataColumnIndices = List<u64, Fixed<128>>;

    ssz_container! {
        /// Complete pinned Gloas `DataColumnSidecar` container.
        DataColumnSidecar {
            /// Specification field `index`.
            index: u64,
            /// Specification field `column`.
            column: DataColumn,
            /// Specification field `kzg_proofs`.
            kzg_proofs: KZGProofs,
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `DataColumnsByRootIdentifier` container.
        DataColumnsByRootIdentifier {
            /// Specification field `block_root`.
            block_root: Root,
            /// Specification field `columns`.
            columns: DataColumnIndices,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `DepositMessage` container.
        DepositMessage {
            /// Specification field `pubkey`.
            pubkey: [u8; 48],
            /// Specification field `withdrawal_credentials`.
            withdrawal_credentials: Root,
            /// Specification field `amount`.
            amount: u64,
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type Domain = Root;

    ssz_container! {
        /// Complete pinned Gloas `Eth1Block` container.
        Eth1Block {
            /// Specification field `timestamp`.
            timestamp: u64,
            /// Specification field `deposit_root`.
            deposit_root: Root,
            /// Specification field `deposit_count`.
            deposit_count: u64,
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type ExecutionBranch = Vector<Root, Fixed<11>>;

    /// Exact pinned Gloas collection bounds.
    pub type FinalityBranch = Vector<Root, Fixed<9>>;

    ssz_container! {
        /// Complete pinned Gloas `ForkData` container.
        ForkData {
            /// Specification field `current_version`.
            current_version: [u8; 4],
            /// Specification field `genesis_validators_root`.
            genesis_validators_root: Root,
        }
    }

    ssz_container! { @progressive
        /// Complete pinned Gloas `IndexedPayloadAttestation` container.
        IndexedPayloadAttestation {
            /// Specification field `attesting_indices`.
            attesting_indices: PayloadTimelinessCommitteeIndices,
            /// Specification field `data`.
            data: super::super::gloas::PayloadAttestationData,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type KZGProofs = ProgressiveList<[u8; 48]>;

    ssz_container! {
        /// Complete pinned Gloas `LightClientBootstrap` container.
        LightClientBootstrap {
            /// Specification field `header`.
            header: LightClientHeader,
            /// Specification field `current_sync_committee`.
            current_sync_committee: super::super::gloas::SyncCommittee,
            /// Specification field `current_sync_committee_branch`.
            current_sync_committee_branch: CurrentSyncCommitteeBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `LightClientFinalityUpdate` container.
        LightClientFinalityUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::gloas::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `LightClientHeader` container.
        LightClientHeader {
            /// Specification field `beacon`.
            beacon: BeaconBlockHeader,
            /// Specification field `execution_block_hash`.
            execution_block_hash: Root,
            /// Specification field `execution_branch`.
            execution_branch: ExecutionBranch,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `LightClientOptimisticUpdate` container.
        LightClientOptimisticUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::gloas::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `LightClientUpdate` container.
        LightClientUpdate {
            /// Specification field `attested_header`.
            attested_header: LightClientHeader,
            /// Specification field `next_sync_committee`.
            next_sync_committee: super::super::gloas::SyncCommittee,
            /// Specification field `next_sync_committee_branch`.
            next_sync_committee_branch: NextSyncCommitteeBranch,
            /// Specification field `finalized_header`.
            finalized_header: LightClientHeader,
            /// Specification field `finality_branch`.
            finality_branch: FinalityBranch,
            /// Specification field `sync_aggregate`.
            sync_aggregate: super::super::gloas::SyncAggregate,
            /// Specification field `signature_slot`.
            signature_slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `MatrixEntry` container.
        MatrixEntry {
            /// Specification field `cell`.
            cell: Cell,
            /// Specification field `kzg_proof`.
            kzg_proof: [u8; 48],
            /// Specification field `column_index`.
            column_index: u64,
            /// Specification field `row_index`.
            row_index: u64,
        }
    }

    ssz_container! { @progressive
        /// Complete pinned Gloas `NewPayloadRequest` container.
        NewPayloadRequest {
            /// Specification field `execution_payload`.
            execution_payload: super::super::gloas::ExecutionPayload,
            /// Specification field `versioned_hashes`.
            versioned_hashes: VersionedHashes,
            /// Specification field `parent_beacon_block_root`.
            parent_beacon_block_root: Root,
            /// Specification field `execution_requests`.
            execution_requests: super::super::gloas::ExecutionRequests,
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type NextSyncCommitteeBranch = Vector<Root, Fixed<11>>;

    ssz_container! {
        /// Complete pinned Gloas `PartialDataColumnGroupID` container.
        PartialDataColumnGroupID {
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `slot`.
            slot: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `PartialDataColumnPartsMetadata` container.
        PartialDataColumnPartsMetadata {
            /// Specification field `available`.
            available: CellsBitList,
            /// Specification field `requests`.
            requests: CellsBitList,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `PartialDataColumnSidecar` container.
        PartialDataColumnSidecar {
            /// Specification field `cells_present_bitmap`.
            cells_present_bitmap: CellsBitList,
            /// Specification field `partial_column`.
            partial_column: DataColumn,
            /// Specification field `kzg_proofs`.
            kzg_proofs: KZGProofs,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `PayloadAttestationMessage` container.
        PayloadAttestationMessage {
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `data`.
            data: super::super::gloas::PayloadAttestationData,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type PayloadTimelinessCommitteeIndices = List<u64, PresetLength<512, 16>>;

    ssz_container! {
        /// Complete pinned Gloas `PowBlock` container.
        PowBlock {
            /// Specification field `block_hash`.
            block_hash: Root,
            /// Specification field `parent_hash`.
            parent_hash: Root,
            /// Specification field `total_difficulty`.
            total_difficulty: [u8; 32],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `ProposerPreferences` container.
        ProposerPreferences {
            /// Specification field `dependent_root`.
            dependent_root: Root,
            /// Specification field `proposal_slot`.
            proposal_slot: u64,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `fee_recipient`.
            fee_recipient: [u8; 20],
            /// Specification field `target_gas_limit`.
            target_gas_limit: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SignedAggregateAndProof` container.
        SignedAggregateAndProof {
            /// Specification field `message`.
            message: AggregateAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SignedContributionAndProof` container.
        SignedContributionAndProof {
            /// Specification field `message`.
            message: ContributionAndProof,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SignedProposerPreferences` container.
        SignedProposerPreferences {
            /// Specification field `message`.
            message: ProposerPreferences,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SigningData` container.
        SigningData {
            /// Specification field `object_root`.
            object_root: Root,
            /// Specification field `domain`.
            domain: Domain,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SingleAttestation` container.
        SingleAttestation {
            /// Specification field `committee_index`.
            committee_index: u64,
            /// Specification field `attester_index`.
            attester_index: u64,
            /// Specification field `data`.
            data: super::super::gloas::AttestationData,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SyncAggregatorSelectionData` container.
        SyncAggregatorSelectionData {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SyncCommitteeContribution` container.
        SyncCommitteeContribution {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `subcommittee_index`.
            subcommittee_index: u64,
            /// Specification field `aggregation_bits`.
            aggregation_bits: SyncSubcommitteeBits,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    ssz_container! {
        /// Complete pinned Gloas `SyncCommitteeMessage` container.
        SyncCommitteeMessage {
            /// Specification field `slot`.
            slot: u64,
            /// Specification field `beacon_block_root`.
            beacon_block_root: Root,
            /// Specification field `validator_index`.
            validator_index: u64,
            /// Specification field `signature`.
            signature: [u8; 96],
        }
    }

    /// Exact pinned Gloas collection bounds.
    pub type SyncSubcommitteeBits = PresetBits<PresetLength<128, 8>>;

    /// Exact pinned Gloas collection bounds.
    pub type VersionedHashes = List<Root, Fixed<4096>>;
}
