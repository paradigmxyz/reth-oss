//! Complete Gloas SSZ schemas pinned to consensus-specs v1.7.0-beta.3.

use super::{
    codec::ssz_container, Fixed, List, Preset, PresetBits, PresetLength, ProgressiveBitList,
    ProgressiveList, Ssz, Vector,
};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint, Root, SszError};

pub use super::phase0::{
    AttestationData, Deposit, Eth1Data, ForkInfo, ProposerSlashing, SignedVoluntaryExit, Validator,
};

/// Exact Gloas specification type `AggregationBits`.
pub type AggregationBits = ProgressiveBitList;

ssz_container! { @progressive
    /// Complete Gloas `Attestation` SSZ container.
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

/// Exact Gloas specification type `Attestations`.
pub type Attestations = ProgressiveList<Attestation>;

ssz_container! {
    /// Complete Gloas `AttesterSlashing` SSZ container.
    AttesterSlashing {
        /// Specification field `attestation_1`.
        attestation_1: IndexedAttestation,
        /// Specification field `attestation_2`.
        attestation_2: IndexedAttestation,
    }
}

/// Exact Gloas specification type `AttesterSlashings`.
pub type AttesterSlashings = ProgressiveList<AttesterSlashing>;

/// Exact Gloas specification type `AttestingIndices`.
pub type AttestingIndices = ProgressiveList<u64>;

ssz_container! {
    /// Complete Gloas `BLSToExecutionChange` SSZ container.
    BLSToExecutionChange {
        /// Specification field `validator_index`.
        validator_index: u64,
        /// Specification field `from_bls_pubkey`.
        from_bls_pubkey: [u8; 48],
        /// Specification field `to_execution_address`.
        to_execution_address: [u8; 20],
    }
}

/// Exact Gloas specification type `BLSToExecutionChanges`.
pub type BLSToExecutionChanges = ProgressiveList<SignedBLSToExecutionChange>;

/// Exact Gloas specification type `Balances`.
pub type Balances = ProgressiveList<u64>;

ssz_container! {
    /// Complete Gloas `BeaconBlock` SSZ container.
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

ssz_container! { @progressive
    /// Complete Gloas `BeaconBlockBody` SSZ container.
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
        /// Specification field `bls_to_execution_changes`.
        bls_to_execution_changes: BLSToExecutionChanges,
        /// Specification field `signed_execution_payload_bid`.
        signed_execution_payload_bid: SignedExecutionPayloadBid,
        /// Specification field `payload_attestations`.
        payload_attestations: PayloadAttestations,
        /// Specification field `parent_execution_requests`.
        parent_execution_requests: ExecutionRequests,
    }
}

ssz_container! { @progressive
    /// Complete Gloas `BeaconState` SSZ container.
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
        /// Specification field `latest_block_hash`.
        latest_block_hash: Root,
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
        /// Specification field `builders`.
        builders: Builders,
        /// Specification field `next_withdrawal_builder_index`.
        next_withdrawal_builder_index: u64,
        /// Specification field `execution_payload_availability`.
        execution_payload_availability: ExecutionPayloadAvailability,
        /// Specification field `builder_pending_payments`.
        builder_pending_payments: BuilderPendingPayments,
        /// Specification field `builder_pending_withdrawals`.
        builder_pending_withdrawals: BuilderPendingWithdrawals,
        /// Specification field `latest_execution_payload_bid`.
        latest_execution_payload_bid: ExecutionPayloadBid,
        /// Specification field `payload_expected_withdrawals`.
        payload_expected_withdrawals: Withdrawals,
        /// Specification field `ptc_window`.
        ptc_window: PayloadTimelinessCommitteeWindow,
    }
}

/// Exact Gloas specification type `BlobKZGCommitments`.
pub type BlobKZGCommitments = ProgressiveList<[u8; 48]>;

/// Exact Gloas specification type `BlockAccessList`.
pub type BlockAccessList = ProgressiveList<u8>;

/// Exact Gloas specification type `BlockRoots`.
pub type BlockRoots = Vector<Root, PresetLength<8192, 64>>;

ssz_container! {
    /// Complete Gloas `Builder` SSZ container.
    Builder {
        /// Specification field `pubkey`.
        pubkey: [u8; 48],
        /// Specification field `version`.
        version: u8,
        /// Specification field `execution_address`.
        execution_address: [u8; 20],
        /// Specification field `balance`.
        balance: u64,
        /// Specification field `deposit_epoch`.
        deposit_epoch: u64,
        /// Specification field `withdrawable_epoch`.
        withdrawable_epoch: u64,
    }
}

ssz_container! {
    /// Complete Gloas `BuilderDepositRequest` SSZ container.
    BuilderDepositRequest {
        /// Specification field `pubkey`.
        pubkey: [u8; 48],
        /// Specification field `withdrawal_credentials`.
        withdrawal_credentials: Root,
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Gloas specification type `BuilderDepositRequests`.
pub type BuilderDepositRequests = ProgressiveList<BuilderDepositRequest>;

ssz_container! {
    /// Complete Gloas `BuilderExitRequest` SSZ container.
    BuilderExitRequest {
        /// Specification field `source_address`.
        source_address: [u8; 20],
        /// Specification field `pubkey`.
        pubkey: [u8; 48],
    }
}

/// Exact Gloas specification type `BuilderExitRequests`.
pub type BuilderExitRequests = ProgressiveList<BuilderExitRequest>;

ssz_container! {
    /// Complete Gloas `BuilderPendingPayment` SSZ container.
    BuilderPendingPayment {
        /// Specification field `weight`.
        weight: u64,
        /// Specification field `withdrawal`.
        withdrawal: BuilderPendingWithdrawal,
        /// Specification field `proposer_index`.
        proposer_index: u64,
    }
}

/// Exact Gloas specification type `BuilderPendingPayments`.
pub type BuilderPendingPayments = Vector<BuilderPendingPayment, PresetLength<64, 16>>;

ssz_container! {
    /// Complete Gloas `BuilderPendingWithdrawal` SSZ container.
    BuilderPendingWithdrawal {
        /// Specification field `fee_recipient`.
        fee_recipient: [u8; 20],
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `builder_index`.
        builder_index: u64,
    }
}

/// Exact Gloas specification type `BuilderPendingWithdrawals`.
pub type BuilderPendingWithdrawals = ProgressiveList<BuilderPendingWithdrawal>;

/// Exact Gloas specification type `Builders`.
pub type Builders = ProgressiveList<Builder>;

/// Exact Gloas specification type `CommitteeBits`.
pub type CommitteeBits = PresetBits<PresetLength<64, 4>>;

ssz_container! {
    /// Complete Gloas `ConsolidationRequest` SSZ container.
    ConsolidationRequest {
        /// Specification field `source_address`.
        source_address: [u8; 20],
        /// Specification field `source_pubkey`.
        source_pubkey: [u8; 48],
        /// Specification field `target_pubkey`.
        target_pubkey: [u8; 48],
    }
}

/// Exact Gloas specification type `ConsolidationRequests`.
pub type ConsolidationRequests = ProgressiveList<ConsolidationRequest>;

ssz_container! {
    /// Complete Gloas `DepositRequest` SSZ container.
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

/// Exact Gloas specification type `DepositRequests`.
pub type DepositRequests = ProgressiveList<DepositRequest>;

/// Exact Gloas specification type `Deposits`.
pub type Deposits = ProgressiveList<Deposit>;

/// Exact Gloas specification type `EpochParticipation`.
pub type EpochParticipation = ProgressiveList<u8>;

/// Exact Gloas specification type `Eth1DataVotes`.
pub type Eth1DataVotes = List<Eth1Data, PresetLength<2048, 32>>;

ssz_container! { @progressive
    /// Complete Gloas `ExecutionPayload` SSZ container.
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
        /// Specification field `block_access_list`.
        block_access_list: BlockAccessList,
        /// Specification field `slot_number`.
        slot_number: u64,
    }
}

/// Exact Gloas specification type `ExecutionPayloadAvailability`.
pub type ExecutionPayloadAvailability = PresetBits<PresetLength<8192, 64>>;

ssz_container! { @progressive
    /// Complete Gloas `ExecutionPayloadBid` SSZ container.
    ExecutionPayloadBid {
        /// Specification field `parent_block_hash`.
        parent_block_hash: Root,
        /// Specification field `parent_block_root`.
        parent_block_root: Root,
        /// Specification field `block_hash`.
        block_hash: Root,
        /// Specification field `prev_randao`.
        prev_randao: Root,
        /// Specification field `fee_recipient`.
        fee_recipient: [u8; 20],
        /// Specification field `gas_limit`.
        gas_limit: u64,
        /// Specification field `builder_index`.
        builder_index: u64,
        /// Specification field `slot`.
        slot: u64,
        /// Specification field `value`.
        value: u64,
        /// Specification field `execution_payment`.
        execution_payment: u64,
        /// Specification field `blob_kzg_commitments`.
        blob_kzg_commitments: BlobKZGCommitments,
        /// Specification field `execution_requests_root`.
        execution_requests_root: Root,
    }
}

ssz_container! { @progressive
    /// Complete Gloas `ExecutionPayloadEnvelope` SSZ container.
    ExecutionPayloadEnvelope {
        /// Specification field `payload`.
        payload: ExecutionPayload,
        /// Specification field `execution_requests`.
        execution_requests: ExecutionRequests,
        /// Specification field `builder_index`.
        builder_index: u64,
        /// Specification field `beacon_block_root`.
        beacon_block_root: Root,
        /// Specification field `parent_beacon_block_root`.
        parent_beacon_block_root: Root,
    }
}

ssz_container! { @progressive
    /// Complete Gloas `ExecutionRequests` SSZ container.
    ExecutionRequests {
        /// Specification field `deposits`.
        deposits: DepositRequests,
        /// Specification field `withdrawals`.
        withdrawals: WithdrawalRequests,
        /// Specification field `consolidations`.
        consolidations: ConsolidationRequests,
        /// Specification field `builder_deposits`.
        builder_deposits: BuilderDepositRequests,
        /// Specification field `builder_exits`.
        builder_exits: BuilderExitRequests,
    }
}

/// Exact Gloas specification type `ExtraData`.
pub type ExtraData = List<u8, Fixed<32>>;

/// Exact Gloas specification type `HistoricalRoots`.
pub type HistoricalRoots = List<Root, Fixed<16777216>>;

/// Exact Gloas specification type `HistoricalSummaries`.
pub type HistoricalSummaries = List<HistoricalSummary, Fixed<16777216>>;

ssz_container! {
    /// Complete Gloas `HistoricalSummary` SSZ container.
    HistoricalSummary {
        /// Specification field `block_summary_root`.
        block_summary_root: Root,
        /// Specification field `state_summary_root`.
        state_summary_root: Root,
    }
}

/// Exact Gloas specification type `InactivityScores`.
pub type InactivityScores = ProgressiveList<u64>;

ssz_container! { @progressive
    /// Complete Gloas `IndexedAttestation` SSZ container.
    IndexedAttestation {
        /// Specification field `attesting_indices`.
        attesting_indices: AttestingIndices,
        /// Specification field `data`.
        data: AttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Gloas specification type `JustificationBits`.
pub type JustificationBits = PresetBits<Fixed<4>>;

/// Exact Gloas specification type `LogsBloom`.
pub type LogsBloom = Vector<u8, Fixed<256>>;

ssz_container! { @progressive
    /// Complete Gloas `PayloadAttestation` SSZ container.
    PayloadAttestation {
        /// Specification field `aggregation_bits`.
        aggregation_bits: PayloadTimelinessCommitteeBits,
        /// Specification field `data`.
        data: PayloadAttestationData,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Gloas `PayloadAttestationData` SSZ container.
    PayloadAttestationData {
        /// Specification field `beacon_block_root`.
        beacon_block_root: Root,
        /// Specification field `slot`.
        slot: u64,
        /// Specification field `payload_present`.
        payload_present: bool,
        /// Specification field `blob_data_available`.
        blob_data_available: bool,
    }
}

/// Exact Gloas specification type `PayloadAttestations`.
pub type PayloadAttestations = ProgressiveList<PayloadAttestation>;

/// Exact Gloas specification type `PayloadTimelinessCommittee`.
pub type PayloadTimelinessCommittee = Vector<u64, PresetLength<512, 16>>;

/// Exact Gloas specification type `PayloadTimelinessCommitteeBits`.
pub type PayloadTimelinessCommitteeBits = PresetBits<PresetLength<512, 16>>;

/// Exact Gloas specification type `PayloadTimelinessCommitteeWindow`.
pub type PayloadTimelinessCommitteeWindow =
    Vector<PayloadTimelinessCommittee, PresetLength<96, 24>>;

ssz_container! {
    /// Complete Gloas `PendingConsolidation` SSZ container.
    PendingConsolidation {
        /// Specification field `source_index`.
        source_index: u64,
        /// Specification field `target_index`.
        target_index: u64,
    }
}

/// Exact Gloas specification type `PendingConsolidations`.
pub type PendingConsolidations = ProgressiveList<PendingConsolidation>;

ssz_container! {
    /// Complete Gloas `PendingDeposit` SSZ container.
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

/// Exact Gloas specification type `PendingDeposits`.
pub type PendingDeposits = ProgressiveList<PendingDeposit>;

ssz_container! {
    /// Complete Gloas `PendingPartialWithdrawal` SSZ container.
    PendingPartialWithdrawal {
        /// Specification field `validator_index`.
        validator_index: u64,
        /// Specification field `amount`.
        amount: u64,
        /// Specification field `withdrawable_epoch`.
        withdrawable_epoch: u64,
    }
}

/// Exact Gloas specification type `PendingPartialWithdrawals`.
pub type PendingPartialWithdrawals = ProgressiveList<PendingPartialWithdrawal>;

/// Exact Gloas specification type `ProposerLookahead`.
pub type ProposerLookahead = Vector<u64, PresetLength<64, 16>>;

/// Exact Gloas specification type `ProposerSlashings`.
pub type ProposerSlashings = ProgressiveList<ProposerSlashing>;

/// Exact Gloas specification type `RandaoMixes`.
pub type RandaoMixes = Vector<Root, PresetLength<65536, 64>>;

ssz_container! {
    /// Complete Gloas `SignedBLSToExecutionChange` SSZ container.
    SignedBLSToExecutionChange {
        /// Specification field `message`.
        message: BLSToExecutionChange,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Gloas `SignedBeaconBlock` SSZ container.
    SignedBeaconBlock {
        /// Specification field `message`.
        message: BeaconBlock,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Gloas `SignedExecutionPayloadBid` SSZ container.
    SignedExecutionPayloadBid {
        /// Specification field `message`.
        message: ExecutionPayloadBid,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Gloas `SignedExecutionPayloadEnvelope` SSZ container.
    SignedExecutionPayloadEnvelope {
        /// Specification field `message`.
        message: ExecutionPayloadEnvelope,
        /// Specification field `signature`.
        signature: [u8; 96],
    }
}

/// Exact Gloas specification type `Slashings`.
pub type Slashings = Vector<u64, PresetLength<8192, 64>>;

/// Exact Gloas specification type `StateRoots`.
pub type StateRoots = Vector<Root, PresetLength<8192, 64>>;

ssz_container! {
    /// Complete Gloas `SyncAggregate` SSZ container.
    SyncAggregate {
        /// Specification field `sync_committee_bits`.
        sync_committee_bits: SyncCommitteeBits,
        /// Specification field `sync_committee_signature`.
        sync_committee_signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Gloas `SyncCommittee` SSZ container.
    SyncCommittee {
        /// Specification field `pubkeys`.
        pubkeys: SyncCommitteePubkeys,
        /// Specification field `aggregate_pubkey`.
        aggregate_pubkey: [u8; 48],
    }
}

/// Exact Gloas specification type `SyncCommitteeBits`.
pub type SyncCommitteeBits = PresetBits<PresetLength<512, 32>>;

/// Exact Gloas specification type `SyncCommitteePubkeys`.
pub type SyncCommitteePubkeys = Vector<[u8; 48], PresetLength<512, 32>>;

/// Exact Gloas specification type `Transaction`.
pub type Transaction = ProgressiveList<u8>;

/// Exact Gloas specification type `Transactions`.
pub type Transactions = ProgressiveList<Transaction>;

/// Exact Gloas specification type `Validators`.
pub type Validators = ProgressiveList<Validator>;

/// Exact Gloas specification type `VoluntaryExits`.
pub type VoluntaryExits = ProgressiveList<SignedVoluntaryExit>;

ssz_container! {
    /// Complete Gloas `Withdrawal` SSZ container.
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
    /// Complete Gloas `WithdrawalRequest` SSZ container.
    WithdrawalRequest {
        /// Specification field `source_address`.
        source_address: [u8; 20],
        /// Specification field `validator_pubkey`.
        validator_pubkey: [u8; 48],
        /// Specification field `amount`.
        amount: u64,
    }
}

/// Exact Gloas specification type `WithdrawalRequests`.
pub type WithdrawalRequests = ProgressiveList<WithdrawalRequest>;

/// Exact Gloas specification type `Withdrawals`.
pub type Withdrawals = ProgressiveList<Withdrawal>;

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
