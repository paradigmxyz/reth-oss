//! Complete Bellatrix SSZ schemas pinned to consensus-specs v1.7.0-beta.3.

use super::{
    codec::ssz_container, Fixed, List, Preset, PresetBitList, PresetBits, PresetLength, Ssz, Vector,
};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint, Root, SszError};

pub use super::phase0::{
    AttestationData, Deposit, Eth1Data, ForkInfo, ProposerSlashing, SignedVoluntaryExit, Validator,
};

/// Exact Bellatrix specification type `AggregationBits`.
pub type AggregationBits = PresetBitList<Fixed<2048>>;

ssz_container! {
    /// Complete Bellatrix `Attestation` SSZ container.
    Attestation {
        /// Specification field `aggregation_bits`.
        aggregation_bits: AggregationBits,
        /// Specification field `data`.
        data: AttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Bellatrix specification type `Attestations`.
pub type Attestations = List<Attestation, Fixed<128>>;

ssz_container! {
    /// Complete Bellatrix `AttesterSlashing` SSZ container.
    AttesterSlashing {
        /// Specification field `attestation_1`.
        attestation_1: IndexedAttestation,
        /// Specification field `attestation_2`.
        attestation_2: IndexedAttestation,
    }
}

/// Exact Bellatrix specification type `AttesterSlashings`.
pub type AttesterSlashings = List<AttesterSlashing, Fixed<2>>;

/// Exact Bellatrix specification type `AttestingIndices`.
pub type AttestingIndices = List<u64, Fixed<2048>>;

/// Exact Bellatrix specification type `Balances`.
pub type Balances = List<u64, Fixed<1099511627776>>;

ssz_container! {
    /// Complete Bellatrix `BeaconBlock` SSZ container.
    BeaconBlock {
        /// Specification field `slot`.
        slot: u64,
        /// Specification field `proposer_index`.
        proposer_index: u64,
        /// Specification field `parent_root`.
        parent_root: Root,
        /// Specification field `state_root`.
        state_root: Root,
        /// Specification field `body`.
        body: BeaconBlockBody,
    }
}

ssz_container! {
    /// Complete Bellatrix `BeaconBlockBody` SSZ container.
    BeaconBlockBody {
        /// Specification field `randao_reveal`.
        randao_reveal: [u8; 96],
        /// Specification field `eth1_data`.
        eth1_data: Eth1Data,
        /// Specification field `graffiti`.
        graffiti: Root,
        /// Specification field `proposer_slashings`.
        proposer_slashings: ProposerSlashings,
        /// Specification field `attester_slashings`.
        attester_slashings: AttesterSlashings,
        /// Specification field `attestations`.
        attestations: Attestations,
        /// Specification field `deposits`.
        deposits: Deposits,
        /// Specification field `voluntary_exits`.
        voluntary_exits: VoluntaryExits,
        /// Specification field `sync_aggregate`.
        sync_aggregate: SyncAggregate,
        /// Specification field `execution_payload`.
        execution_payload: ExecutionPayload,
    }
}

ssz_container! {
    /// Complete Bellatrix `BeaconState` SSZ container.
    BeaconState {
        /// Specification field `genesis_time`.
        genesis_time: u64,
        /// Specification field `genesis_validators_root`.
        genesis_validators_root: Root,
        /// Specification field `slot`.
        slot: u64,
        /// Specification field `fork`.
        fork: ForkInfo,
        /// Specification field `latest_block_header`.
        latest_block_header: BeaconBlockHeader,
        /// Specification field `block_roots`.
        block_roots: BlockRoots,
        /// Specification field `state_roots`.
        state_roots: StateRoots,
        /// Specification field `historical_roots`.
        historical_roots: HistoricalRoots,
        /// Specification field `eth1_data`.
        eth1_data: Eth1Data,
        /// Specification field `eth1_data_votes`.
        eth1_data_votes: Eth1DataVotes,
        /// Specification field `eth1_deposit_index`.
        eth1_deposit_index: u64,
        /// Specification field `validators`.
        validators: Validators,
        /// Specification field `balances`.
        balances: Balances,
        /// Specification field `randao_mixes`.
        randao_mixes: RandaoMixes,
        /// Specification field `slashings`.
        slashings: Slashings,
        /// Specification field `previous_epoch_participation`.
        previous_epoch_participation: EpochParticipation,
        /// Specification field `current_epoch_participation`.
        current_epoch_participation: EpochParticipation,
        /// Specification field `justification_bits`.
        justification_bits: JustificationBits,
        /// Specification field `previous_justified_checkpoint`.
        previous_justified_checkpoint: Checkpoint,
        /// Specification field `current_justified_checkpoint`.
        current_justified_checkpoint: Checkpoint,
        /// Specification field `finalized_checkpoint`.
        finalized_checkpoint: Checkpoint,
        /// Specification field `inactivity_scores`.
        inactivity_scores: InactivityScores,
        /// Specification field `current_sync_committee`.
        current_sync_committee: SyncCommittee,
        /// Specification field `next_sync_committee`.
        next_sync_committee: SyncCommittee,
        /// Specification field `latest_execution_payload_header`.
        latest_execution_payload_header: ExecutionPayloadHeader,
    }
}

/// Exact Bellatrix specification type `BlockRoots`.
pub type BlockRoots = Vector<Root, PresetLength<8192, 64>>;

/// Exact Bellatrix specification type `Deposits`.
pub type Deposits = List<Deposit, Fixed<16>>;

/// Exact Bellatrix specification type `EpochParticipation`.
pub type EpochParticipation = List<u8, Fixed<1099511627776>>;

/// Exact Bellatrix specification type `Eth1DataVotes`.
pub type Eth1DataVotes = List<Eth1Data, PresetLength<2048, 32>>;

ssz_container! {
    /// Complete Bellatrix `ExecutionPayload` SSZ container.
    ExecutionPayload {
        /// Specification field `parent_hash`.
        parent_hash: Root,
        /// Specification field `fee_recipient`.
        fee_recipient: [u8; 20],
        /// Specification field `state_root`.
        state_root: Root,
        /// Specification field `receipts_root`.
        receipts_root: Root,
        /// Specification field `logs_bloom`.
        logs_bloom: LogsBloom,
        /// Specification field `prev_randao`.
        prev_randao: Root,
        /// Specification field `block_number`.
        block_number: u64,
        /// Specification field `gas_limit`.
        gas_limit: u64,
        /// Specification field `gas_used`.
        gas_used: u64,
        /// Specification field `timestamp`.
        timestamp: u64,
        /// Specification field `extra_data`.
        extra_data: ExtraData,
        /// Specification field `base_fee_per_gas`.
        base_fee_per_gas: [u8; 32],
        /// Specification field `block_hash`.
        block_hash: Root,
        /// Specification field `transactions`.
        transactions: Transactions,
    }
}

ssz_container! {
    /// Complete Bellatrix `ExecutionPayloadHeader` SSZ container.
    ExecutionPayloadHeader {
        /// Specification field `parent_hash`.
        parent_hash: Root,
        /// Specification field `fee_recipient`.
        fee_recipient: [u8; 20],
        /// Specification field `state_root`.
        state_root: Root,
        /// Specification field `receipts_root`.
        receipts_root: Root,
        /// Specification field `logs_bloom`.
        logs_bloom: LogsBloom,
        /// Specification field `prev_randao`.
        prev_randao: Root,
        /// Specification field `block_number`.
        block_number: u64,
        /// Specification field `gas_limit`.
        gas_limit: u64,
        /// Specification field `gas_used`.
        gas_used: u64,
        /// Specification field `timestamp`.
        timestamp: u64,
        /// Specification field `extra_data`.
        extra_data: ExtraData,
        /// Specification field `base_fee_per_gas`.
        base_fee_per_gas: [u8; 32],
        /// Specification field `block_hash`.
        block_hash: Root,
        /// Specification field `transactions_root`.
        transactions_root: Root,
    }
}

/// Exact Bellatrix specification type `ExtraData`.
pub type ExtraData = List<u8, Fixed<32>>;

/// Exact Bellatrix specification type `HistoricalRoots`.
pub type HistoricalRoots = List<Root, Fixed<16777216>>;

/// Exact Bellatrix specification type `InactivityScores`.
pub type InactivityScores = List<u64, Fixed<1099511627776>>;

ssz_container! {
    /// Complete Bellatrix `IndexedAttestation` SSZ container.
    IndexedAttestation {
        /// Specification field `attesting_indices`.
        attesting_indices: AttestingIndices,
        /// Specification field `data`.
        data: AttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Bellatrix specification type `JustificationBits`.
pub type JustificationBits = PresetBits<Fixed<4>>;

/// Exact Bellatrix specification type `LogsBloom`.
pub type LogsBloom = Vector<u8, Fixed<256>>;

/// Exact Bellatrix specification type `ProposerSlashings`.
pub type ProposerSlashings = List<ProposerSlashing, Fixed<16>>;

/// Exact Bellatrix specification type `RandaoMixes`.
pub type RandaoMixes = Vector<Root, PresetLength<65536, 64>>;

ssz_container! {
    /// Complete Bellatrix `SignedBeaconBlock` SSZ container.
    SignedBeaconBlock {
        /// Specification field `message`.
        message: BeaconBlock,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Bellatrix specification type `Slashings`.
pub type Slashings = Vector<u64, PresetLength<8192, 64>>;

/// Exact Bellatrix specification type `StateRoots`.
pub type StateRoots = Vector<Root, PresetLength<8192, 64>>;

ssz_container! {
    /// Complete Bellatrix `SyncAggregate` SSZ container.
    SyncAggregate {
        /// Specification field `sync_committee_bits`.
        sync_committee_bits: SyncCommitteeBits,
        /// Specification field `sync_committee_signature`.
        sync_committee_signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Bellatrix `SyncCommittee` SSZ container.
    SyncCommittee {
        /// Specification field `pubkeys`.
        pubkeys: SyncCommitteePubkeys,
        /// Specification field `aggregate_pubkey`.
        aggregate_pubkey: [u8; 48],
    }
}

/// Exact Bellatrix specification type `SyncCommitteeBits`.
pub type SyncCommitteeBits = PresetBits<PresetLength<512, 32>>;

/// Exact Bellatrix specification type `SyncCommitteePubkeys`.
pub type SyncCommitteePubkeys = Vector<[u8; 48], PresetLength<512, 32>>;

/// Exact Bellatrix specification type `Transaction`.
pub type Transaction = List<u8, Fixed<1073741824>>;

/// Exact Bellatrix specification type `Transactions`.
pub type Transactions = List<Transaction, Fixed<1048576>>;

/// Exact Bellatrix specification type `Validators`.
pub type Validators = List<Validator, Fixed<1099511627776>>;

/// Exact Bellatrix specification type `VoluntaryExits`.
pub type VoluntaryExits = List<SignedVoluntaryExit, Fixed<16>>;

impl BeaconBlock {
    /// Derive the unsigned header from the complete body root.
    pub fn header(&self, preset: Preset) -> Result<BeaconBlockHeader, SszError> {
        Ok(BeaconBlockHeader {
            slot: self.slot,
            proposer_index: self.proposer_index,
            parent_root: self.parent_root,
            state_root: self.state_root,
            body_root: self.body.hash_tree_root(preset)?,
        })
    }
}
