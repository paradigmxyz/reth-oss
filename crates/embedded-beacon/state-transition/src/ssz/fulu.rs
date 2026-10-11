//! Complete Fulu SSZ schemas pinned to consensus-specs v1.7.0-beta.3.

use super::{
    codec::ssz_container, Fixed, List, Preset, PresetBitList, PresetBits, PresetLength, Ssz, Vector,
};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint, Root, SszError};

pub use super::phase0::{
    AttestationData, Deposit, Eth1Data, ForkInfo, ProposerSlashing, SignedVoluntaryExit, Validator,
};

/// Exact Fulu specification type `AggregationBits`.
pub type AggregationBits = PresetBitList<PresetLength<131072, 8192>>;

ssz_container! {
    /// Complete Fulu `Attestation` SSZ container.
    Attestation {
        /// Specification field `aggregation_bits`.
        aggregation_bits: AggregationBits,
        /// Specification field `data`.
        data: AttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
        /// Specification field `committee_bits`.
        committee_bits: CommitteeBits,
    }
}

/// Exact Fulu specification type `Attestations`.
pub type Attestations = List<Attestation, Fixed<8>>;

ssz_container! {
    /// Complete Fulu `AttesterSlashing` SSZ container.
    AttesterSlashing {
        /// Specification field `attestation_1`.
        attestation_1: IndexedAttestation,
        /// Specification field `attestation_2`.
        attestation_2: IndexedAttestation,
    }
}

/// Exact Fulu specification type `AttesterSlashings`.
pub type AttesterSlashings = List<AttesterSlashing, Fixed<1>>;

/// Exact Fulu specification type `AttestingIndices`.
pub type AttestingIndices = List<u64, PresetLength<131072, 8192>>;

ssz_container! {
    /// Complete Fulu `BLSToExecutionChange` SSZ container.
    BLSToExecutionChange {
        /// Specification field `validator_index`.
        validator_index: u64,
        /// Specification field `from_bls_pubkey`.
        from_bls_pubkey: [u8; 48],
        /// Specification field `to_execution_address`.
        to_execution_address: [u8; 20],
    }
}

/// Exact Fulu specification type `BLSToExecutionChanges`.
pub type BLSToExecutionChanges = List<SignedBLSToExecutionChange, Fixed<16>>;

/// Exact Fulu specification type `Balances`.
pub type Balances = List<u64, Fixed<1099511627776>>;

ssz_container! {
    /// Complete Fulu `BeaconBlock` SSZ container.
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
    /// Complete Fulu `BeaconBlockBody` SSZ container.
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
        /// Specification field `bls_to_execution_changes`.
        bls_to_execution_changes: BLSToExecutionChanges,
        /// Specification field `blob_kzg_commitments`.
        blob_kzg_commitments: BlobKZGCommitments,
        /// Specification field `execution_requests`.
        execution_requests: ExecutionRequests,
    }
}

ssz_container! {
    /// Complete Fulu `BeaconState` SSZ container.
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
        /// Specification field `next_withdrawal_index`.
        next_withdrawal_index: u64,
        /// Specification field `next_withdrawal_validator_index`.
        next_withdrawal_validator_index: u64,
        /// Specification field `historical_summaries`.
        historical_summaries: HistoricalSummaries,
        /// Specification field `deposit_requests_start_index`.
        deposit_requests_start_index: u64,
        /// Specification field `deposit_balance_to_consume`.
        deposit_balance_to_consume: u64,
        /// Specification field `exit_balance_to_consume`.
        exit_balance_to_consume: u64,
        /// Specification field `earliest_exit_epoch`.
        earliest_exit_epoch: u64,
        /// Specification field `consolidation_balance_to_consume`.
        consolidation_balance_to_consume: u64,
        /// Specification field `earliest_consolidation_epoch`.
        earliest_consolidation_epoch: u64,
        /// Specification field `pending_deposits`.
        pending_deposits: PendingDeposits,
        /// Specification field `pending_partial_withdrawals`.
        pending_partial_withdrawals: PendingPartialWithdrawals,
        /// Specification field `pending_consolidations`.
        pending_consolidations: PendingConsolidations,
        /// Specification field `proposer_lookahead`.
        proposer_lookahead: ProposerLookahead,
    }
}

/// Exact Fulu specification type `BlobKZGCommitments`.
pub type BlobKZGCommitments = List<[u8; 48], Fixed<4096>>;

/// Exact Fulu specification type `BlockRoots`.
pub type BlockRoots = Vector<Root, PresetLength<8192, 64>>;

/// Exact Fulu specification type `CommitteeBits`.
pub type CommitteeBits = PresetBits<PresetLength<64, 4>>;

ssz_container! {
    /// Complete Fulu `ConsolidationRequest` SSZ container.
    ConsolidationRequest {
        /// Specification field `source_address`.
        source_address: [u8; 20],
        /// Specification field `source_pubkey`.
        source_pubkey: [u8; 48],
        /// Specification field `target_pubkey`.
        target_pubkey: [u8; 48],
    }
}

/// Exact Fulu specification type `ConsolidationRequests`.
pub type ConsolidationRequests = List<ConsolidationRequest, Fixed<2>>;

ssz_container! {
    /// Complete Fulu `DepositRequest` SSZ container.
    DepositRequest {
        /// Specification field `pubkey`.
        pubkey: [u8; 48],
        /// Specification field `withdrawal_credentials`.
        withdrawal_credentials: Root,
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `signature`.
        signature: [u8; 96],
        /// Specification field `index`.
        index: u64,
    }
}

/// Exact Fulu specification type `DepositRequests`.
pub type DepositRequests = List<DepositRequest, Fixed<8192>>;

/// Exact Fulu specification type `Deposits`.
pub type Deposits = List<Deposit, Fixed<16>>;

/// Exact Fulu specification type `EpochParticipation`.
pub type EpochParticipation = List<u8, Fixed<1099511627776>>;

/// Exact Fulu specification type `Eth1DataVotes`.
pub type Eth1DataVotes = List<Eth1Data, PresetLength<2048, 32>>;

ssz_container! {
    /// Complete Fulu `ExecutionPayload` SSZ container.
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
        /// Specification field `withdrawals`.
        withdrawals: Withdrawals,
        /// Specification field `blob_gas_used`.
        blob_gas_used: u64,
        /// Specification field `excess_blob_gas`.
        excess_blob_gas: u64,
    }
}

ssz_container! {
    /// Complete Fulu `ExecutionPayloadHeader` SSZ container.
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
        /// Specification field `withdrawals_root`.
        withdrawals_root: Root,
        /// Specification field `blob_gas_used`.
        blob_gas_used: u64,
        /// Specification field `excess_blob_gas`.
        excess_blob_gas: u64,
    }
}

ssz_container! {
    /// Complete Fulu `ExecutionRequests` SSZ container.
    ExecutionRequests {
        /// Specification field `deposits`.
        deposits: DepositRequests,
        /// Specification field `withdrawals`.
        withdrawals: WithdrawalRequests,
        /// Specification field `consolidations`.
        consolidations: ConsolidationRequests,
    }
}

/// Exact Fulu specification type `ExtraData`.
pub type ExtraData = List<u8, Fixed<32>>;

/// Exact Fulu specification type `HistoricalRoots`.
pub type HistoricalRoots = List<Root, Fixed<16777216>>;

/// Exact Fulu specification type `HistoricalSummaries`.
pub type HistoricalSummaries = List<HistoricalSummary, Fixed<16777216>>;

ssz_container! {
    /// Complete Fulu `HistoricalSummary` SSZ container.
    HistoricalSummary {
        /// Specification field `block_summary_root`.
        block_summary_root: Root,
        /// Specification field `state_summary_root`.
        state_summary_root: Root,
    }
}

/// Exact Fulu specification type `InactivityScores`.
pub type InactivityScores = List<u64, Fixed<1099511627776>>;

ssz_container! {
    /// Complete Fulu `IndexedAttestation` SSZ container.
    IndexedAttestation {
        /// Specification field `attesting_indices`.
        attesting_indices: AttestingIndices,
        /// Specification field `data`.
        data: AttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Fulu specification type `JustificationBits`.
pub type JustificationBits = PresetBits<Fixed<4>>;

/// Exact Fulu specification type `LogsBloom`.
pub type LogsBloom = Vector<u8, Fixed<256>>;

ssz_container! {
    /// Complete Fulu `PendingConsolidation` SSZ container.
    PendingConsolidation {
        /// Specification field `source_index`.
        source_index: u64,
        /// Specification field `target_index`.
        target_index: u64,
    }
}

/// Exact Fulu specification type `PendingConsolidations`.
pub type PendingConsolidations = List<PendingConsolidation, PresetLength<262144, 64>>;

ssz_container! {
    /// Complete Fulu `PendingDeposit` SSZ container.
    PendingDeposit {
        /// Specification field `pubkey`.
        pubkey: [u8; 48],
        /// Specification field `withdrawal_credentials`.
        withdrawal_credentials: Root,
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `signature`.
        signature: [u8; 96],
        /// Specification field `slot`.
        slot: u64,
    }
}

/// Exact Fulu specification type `PendingDeposits`.
pub type PendingDeposits = List<PendingDeposit, Fixed<134217728>>;

ssz_container! {
    /// Complete Fulu `PendingPartialWithdrawal` SSZ container.
    PendingPartialWithdrawal {
        /// Specification field `validator_index`.
        validator_index: u64,
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `withdrawable_epoch`.
        withdrawable_epoch: u64,
    }
}

/// Exact Fulu specification type `PendingPartialWithdrawals`.
pub type PendingPartialWithdrawals = List<PendingPartialWithdrawal, PresetLength<134217728, 64>>;

/// Exact Fulu specification type `ProposerLookahead`.
pub type ProposerLookahead = Vector<u64, PresetLength<64, 16>>;

/// Exact Fulu specification type `ProposerSlashings`.
pub type ProposerSlashings = List<ProposerSlashing, Fixed<16>>;

/// Exact Fulu specification type `RandaoMixes`.
pub type RandaoMixes = Vector<Root, PresetLength<65536, 64>>;

ssz_container! {
    /// Complete Fulu `SignedBLSToExecutionChange` SSZ container.
    SignedBLSToExecutionChange {
        /// Specification field `message`.
        message: BLSToExecutionChange,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Fulu `SignedBeaconBlock` SSZ container.
    SignedBeaconBlock {
        /// Specification field `message`.
        message: BeaconBlock,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Fulu specification type `Slashings`.
pub type Slashings = Vector<u64, PresetLength<8192, 64>>;

/// Exact Fulu specification type `StateRoots`.
pub type StateRoots = Vector<Root, PresetLength<8192, 64>>;

ssz_container! {
    /// Complete Fulu `SyncAggregate` SSZ container.
    SyncAggregate {
        /// Specification field `sync_committee_bits`.
        sync_committee_bits: SyncCommitteeBits,
        /// Specification field `sync_committee_signature`.
        sync_committee_signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Fulu `SyncCommittee` SSZ container.
    SyncCommittee {
        /// Specification field `pubkeys`.
        pubkeys: SyncCommitteePubkeys,
        /// Specification field `aggregate_pubkey`.
        aggregate_pubkey: [u8; 48],
    }
}

/// Exact Fulu specification type `SyncCommitteeBits`.
pub type SyncCommitteeBits = PresetBits<PresetLength<512, 32>>;

/// Exact Fulu specification type `SyncCommitteePubkeys`.
pub type SyncCommitteePubkeys = Vector<[u8; 48], PresetLength<512, 32>>;

/// Exact Fulu specification type `Transaction`.
pub type Transaction = List<u8, Fixed<1073741824>>;

/// Exact Fulu specification type `Transactions`.
pub type Transactions = List<Transaction, Fixed<1048576>>;

/// Exact Fulu specification type `Validators`.
pub type Validators = List<Validator, Fixed<1099511627776>>;

/// Exact Fulu specification type `VoluntaryExits`.
pub type VoluntaryExits = List<SignedVoluntaryExit, Fixed<16>>;

ssz_container! {
    /// Complete Fulu `Withdrawal` SSZ container.
    Withdrawal {
        /// Specification field `index`.
        index: u64,
        /// Specification field `validator_index`.
        validator_index: u64,
        /// Specification field `address`.
        address: [u8; 20],
        /// Specification field `amount`.
        amount: u64,
    }
}

ssz_container! {
    /// Complete Fulu `WithdrawalRequest` SSZ container.
    WithdrawalRequest {
        /// Specification field `source_address`.
        source_address: [u8; 20],
        /// Specification field `validator_pubkey`.
        validator_pubkey: [u8; 48],
        /// Specification field `amount`.
        amount: u64,
    }
}

/// Exact Fulu specification type `WithdrawalRequests`.
pub type WithdrawalRequests = List<WithdrawalRequest, Fixed<16>>;

/// Exact Fulu specification type `Withdrawals`.
pub type Withdrawals = List<Withdrawal, PresetLength<16, 4>>;

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
