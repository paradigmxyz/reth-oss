//! Static fixture dispatch through complete typed schemas.

use reth_embedded_beacon_primitives::{self as primitives, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{self, auxiliary, Preset, Ssz};

pub(crate) fn supports(fork: Fork, _preset: Preset, handler: &str) -> bool {
    matches!(
        (fork, handler),
        (
            Fork::Phase0,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Checkpoint" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "Eth1Block" |
                "Eth1Data" |
                "Fork" |
                "ForkData" |
                "HistoricalBatch" |
                "IndexedAttestation" |
                "PendingAttestation" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBeaconBlockHeader" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "Validator" |
                "VoluntaryExit"
        ) | (
            Fork::Altair,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Checkpoint" |
                "ContributionAndProof" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "Eth1Block" |
                "Eth1Data" |
                "Fork" |
                "ForkData" |
                "HistoricalBatch" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit"
        ) | (
            Fork::Bellatrix,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Checkpoint" |
                "ContributionAndProof" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadHeader" |
                "Fork" |
                "ForkData" |
                "HistoricalBatch" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "NewPayloadRequest" |
                "PowBlock" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit"
        ) | (
            Fork::Capella,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BLSToExecutionChange" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Checkpoint" |
                "ContributionAndProof" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadHeader" |
                "Fork" |
                "ForkData" |
                "HistoricalSummary" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "NewPayloadRequest" |
                "PowBlock" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBLSToExecutionChange" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit" |
                "Withdrawal"
        ) | (
            Fork::Deneb,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BLSToExecutionChange" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "BlobIdentifier" |
                "BlobSidecar" |
                "Checkpoint" |
                "ContributionAndProof" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadHeader" |
                "Fork" |
                "ForkData" |
                "HistoricalSummary" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "NewPayloadRequest" |
                "PowBlock" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBLSToExecutionChange" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit" |
                "Withdrawal"
        ) | (
            Fork::Electra,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BLSToExecutionChange" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "BlobIdentifier" |
                "BlobSidecar" |
                "Checkpoint" |
                "ConsolidationRequest" |
                "ContributionAndProof" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "DepositRequest" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadHeader" |
                "ExecutionRequests" |
                "Fork" |
                "ForkData" |
                "HistoricalSummary" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "NewPayloadRequest" |
                "PendingConsolidation" |
                "PendingDeposit" |
                "PendingPartialWithdrawal" |
                "PowBlock" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBLSToExecutionChange" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SingleAttestation" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit" |
                "Withdrawal" |
                "WithdrawalRequest"
        ) | (
            Fork::Fulu,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BLSToExecutionChange" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Checkpoint" |
                "ConsolidationRequest" |
                "ContributionAndProof" |
                "DataColumnSidecar" |
                "DataColumnsByRootIdentifier" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "DepositRequest" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadHeader" |
                "ExecutionRequests" |
                "Fork" |
                "ForkData" |
                "HistoricalSummary" |
                "IndexedAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "MatrixEntry" |
                "NewPayloadRequest" |
                "PartialDataColumnGroupID" |
                "PartialDataColumnHeader" |
                "PartialDataColumnPartsMetadata" |
                "PartialDataColumnSidecar" |
                "PendingConsolidation" |
                "PendingDeposit" |
                "PendingPartialWithdrawal" |
                "PowBlock" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBLSToExecutionChange" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SingleAttestation" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit" |
                "Withdrawal" |
                "WithdrawalRequest"
        ) | (
            Fork::Gloas,
            "AggregateAndProof" |
                "Attestation" |
                "AttestationData" |
                "AttesterSlashing" |
                "BLSToExecutionChange" |
                "BeaconBlockBody" |
                "BeaconBlockHeader" |
                "Builder" |
                "BuilderDepositRequest" |
                "BuilderExitRequest" |
                "BuilderPendingPayment" |
                "BuilderPendingWithdrawal" |
                "Checkpoint" |
                "ConsolidationRequest" |
                "ContributionAndProof" |
                "DataColumnSidecar" |
                "DataColumnsByRootIdentifier" |
                "Deposit" |
                "DepositData" |
                "DepositMessage" |
                "DepositRequest" |
                "Eth1Block" |
                "Eth1Data" |
                "ExecutionPayload" |
                "ExecutionPayloadBid" |
                "ExecutionPayloadEnvelope" |
                "ExecutionRequests" |
                "Fork" |
                "ForkData" |
                "HistoricalSummary" |
                "IndexedAttestation" |
                "IndexedPayloadAttestation" |
                "LightClientBootstrap" |
                "LightClientFinalityUpdate" |
                "LightClientHeader" |
                "LightClientOptimisticUpdate" |
                "LightClientUpdate" |
                "MatrixEntry" |
                "NewPayloadRequest" |
                "PartialDataColumnGroupID" |
                "PartialDataColumnPartsMetadata" |
                "PartialDataColumnSidecar" |
                "PayloadAttestation" |
                "PayloadAttestationData" |
                "PayloadAttestationMessage" |
                "PendingConsolidation" |
                "PendingDeposit" |
                "PendingPartialWithdrawal" |
                "PowBlock" |
                "ProposerPreferences" |
                "ProposerSlashing" |
                "SignedAggregateAndProof" |
                "SignedBLSToExecutionChange" |
                "SignedBeaconBlockHeader" |
                "SignedContributionAndProof" |
                "SignedExecutionPayloadBid" |
                "SignedExecutionPayloadEnvelope" |
                "SignedProposerPreferences" |
                "SignedVoluntaryExit" |
                "SigningData" |
                "SingleAttestation" |
                "SyncAggregate" |
                "SyncAggregatorSelectionData" |
                "SyncCommittee" |
                "SyncCommitteeContribution" |
                "SyncCommitteeMessage" |
                "Validator" |
                "VoluntaryExit" |
                "Withdrawal" |
                "WithdrawalRequest"
        )
    )
}

pub(crate) fn decode(
    fork: Fork,
    preset: Preset,
    handler: &str,
    bytes: &[u8],
) -> Result<(Root, Vec<u8>), String> {
    match (fork, handler) {
        (Fork::Phase0, "AggregateAndProof") => {
            typed::<auxiliary::phase0::AggregateAndProof>(bytes, preset)
        }
        (Fork::Phase0, "Attestation") => typed::<ssz::phase0::Attestation>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "AttestationData",
        ) => typed::<ssz::phase0::AttestationData>(bytes, preset),
        (Fork::Phase0, "AttesterSlashing") => typed::<ssz::phase0::AttesterSlashing>(bytes, preset),
        (Fork::Phase0, "BeaconBlockBody") => typed::<ssz::phase0::BeaconBlockBody>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "BeaconBlockHeader",
        ) => typed::<primitives::BeaconBlockHeader>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "Checkpoint",
        ) => typed::<primitives::Checkpoint>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "Deposit",
        ) => typed::<ssz::phase0::Deposit>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "DepositData",
        ) => typed::<ssz::phase0::DepositData>(bytes, preset),
        (Fork::Phase0, "DepositMessage") => {
            typed::<auxiliary::phase0::DepositMessage>(bytes, preset)
        }
        (Fork::Phase0, "Eth1Block") => typed::<auxiliary::phase0::Eth1Block>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "Eth1Data",
        ) => typed::<ssz::phase0::Eth1Data>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "Fork",
        ) => typed::<ssz::phase0::ForkInfo>(bytes, preset),
        (Fork::Phase0, "ForkData") => typed::<auxiliary::phase0::ForkData>(bytes, preset),
        (Fork::Phase0, "HistoricalBatch") => {
            typed::<auxiliary::phase0::HistoricalBatch>(bytes, preset)
        }
        (Fork::Phase0, "IndexedAttestation") => {
            typed::<ssz::phase0::IndexedAttestation>(bytes, preset)
        }
        (Fork::Phase0, "PendingAttestation") => {
            typed::<ssz::phase0::PendingAttestation>(bytes, preset)
        }
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "ProposerSlashing",
        ) => typed::<ssz::phase0::ProposerSlashing>(bytes, preset),
        (Fork::Phase0, "SignedAggregateAndProof") => {
            typed::<auxiliary::phase0::SignedAggregateAndProof>(bytes, preset)
        }
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "SignedBeaconBlockHeader",
        ) => typed::<ssz::phase0::SignedBeaconBlockHeader>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "SignedVoluntaryExit",
        ) => typed::<ssz::phase0::SignedVoluntaryExit>(bytes, preset),
        (Fork::Phase0, "SigningData") => typed::<auxiliary::phase0::SigningData>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "Validator",
        ) => typed::<ssz::phase0::Validator>(bytes, preset),
        (
            Fork::Phase0 |
            Fork::Altair |
            Fork::Bellatrix |
            Fork::Capella |
            Fork::Deneb |
            Fork::Electra |
            Fork::Fulu |
            Fork::Gloas,
            "VoluntaryExit",
        ) => typed::<ssz::phase0::VoluntaryExit>(bytes, preset),
        (Fork::Altair, "AggregateAndProof") => {
            typed::<auxiliary::altair::AggregateAndProof>(bytes, preset)
        }
        (Fork::Altair, "Attestation") => typed::<ssz::altair::Attestation>(bytes, preset),
        (Fork::Altair, "AttesterSlashing") => typed::<ssz::altair::AttesterSlashing>(bytes, preset),
        (Fork::Altair, "BeaconBlockBody") => typed::<ssz::altair::BeaconBlockBody>(bytes, preset),
        (Fork::Altair, "ContributionAndProof") => {
            typed::<auxiliary::altair::ContributionAndProof>(bytes, preset)
        }
        (Fork::Altair, "DepositMessage") => {
            typed::<auxiliary::altair::DepositMessage>(bytes, preset)
        }
        (Fork::Altair, "Eth1Block") => typed::<auxiliary::altair::Eth1Block>(bytes, preset),
        (Fork::Altair, "ForkData") => typed::<auxiliary::altair::ForkData>(bytes, preset),
        (Fork::Altair, "HistoricalBatch") => {
            typed::<auxiliary::altair::HistoricalBatch>(bytes, preset)
        }
        (Fork::Altair, "IndexedAttestation") => {
            typed::<ssz::altair::IndexedAttestation>(bytes, preset)
        }
        (Fork::Altair, "LightClientBootstrap") => {
            typed::<auxiliary::altair::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Altair, "LightClientFinalityUpdate") => {
            typed::<auxiliary::altair::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Altair, "LightClientHeader") => {
            typed::<auxiliary::altair::LightClientHeader>(bytes, preset)
        }
        (Fork::Altair, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::altair::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Altair, "LightClientUpdate") => {
            typed::<auxiliary::altair::LightClientUpdate>(bytes, preset)
        }
        (Fork::Altair, "SignedAggregateAndProof") => {
            typed::<auxiliary::altair::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Altair, "SignedContributionAndProof") => {
            typed::<auxiliary::altair::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Altair, "SigningData") => typed::<auxiliary::altair::SigningData>(bytes, preset),
        (Fork::Altair, "SyncAggregate") => typed::<ssz::altair::SyncAggregate>(bytes, preset),
        (Fork::Altair, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::altair::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Altair, "SyncCommittee") => typed::<ssz::altair::SyncCommittee>(bytes, preset),
        (Fork::Altair, "SyncCommitteeContribution") => {
            typed::<auxiliary::altair::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Altair, "SyncCommitteeMessage") => {
            typed::<auxiliary::altair::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Bellatrix, "AggregateAndProof") => {
            typed::<auxiliary::bellatrix::AggregateAndProof>(bytes, preset)
        }
        (Fork::Bellatrix, "Attestation") => typed::<ssz::bellatrix::Attestation>(bytes, preset),
        (Fork::Bellatrix, "AttesterSlashing") => {
            typed::<ssz::bellatrix::AttesterSlashing>(bytes, preset)
        }
        (Fork::Bellatrix, "BeaconBlockBody") => {
            typed::<ssz::bellatrix::BeaconBlockBody>(bytes, preset)
        }
        (Fork::Bellatrix, "ContributionAndProof") => {
            typed::<auxiliary::bellatrix::ContributionAndProof>(bytes, preset)
        }
        (Fork::Bellatrix, "DepositMessage") => {
            typed::<auxiliary::bellatrix::DepositMessage>(bytes, preset)
        }
        (Fork::Bellatrix, "Eth1Block") => typed::<auxiliary::bellatrix::Eth1Block>(bytes, preset),
        (Fork::Bellatrix, "ExecutionPayload") => {
            typed::<ssz::bellatrix::ExecutionPayload>(bytes, preset)
        }
        (Fork::Bellatrix, "ExecutionPayloadHeader") => {
            typed::<ssz::bellatrix::ExecutionPayloadHeader>(bytes, preset)
        }
        (Fork::Bellatrix, "ForkData") => typed::<auxiliary::bellatrix::ForkData>(bytes, preset),
        (Fork::Bellatrix, "HistoricalBatch") => {
            typed::<auxiliary::bellatrix::HistoricalBatch>(bytes, preset)
        }
        (Fork::Bellatrix, "IndexedAttestation") => {
            typed::<ssz::bellatrix::IndexedAttestation>(bytes, preset)
        }
        (Fork::Bellatrix, "LightClientBootstrap") => {
            typed::<auxiliary::bellatrix::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Bellatrix, "LightClientFinalityUpdate") => {
            typed::<auxiliary::bellatrix::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Bellatrix, "LightClientHeader") => {
            typed::<auxiliary::bellatrix::LightClientHeader>(bytes, preset)
        }
        (Fork::Bellatrix, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::bellatrix::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Bellatrix, "LightClientUpdate") => {
            typed::<auxiliary::bellatrix::LightClientUpdate>(bytes, preset)
        }
        (Fork::Bellatrix, "NewPayloadRequest") => {
            typed::<auxiliary::bellatrix::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Bellatrix, "PowBlock") => typed::<auxiliary::bellatrix::PowBlock>(bytes, preset),
        (Fork::Bellatrix, "SignedAggregateAndProof") => {
            typed::<auxiliary::bellatrix::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Bellatrix, "SignedContributionAndProof") => {
            typed::<auxiliary::bellatrix::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Bellatrix, "SigningData") => {
            typed::<auxiliary::bellatrix::SigningData>(bytes, preset)
        }
        (Fork::Bellatrix, "SyncAggregate") => typed::<ssz::bellatrix::SyncAggregate>(bytes, preset),
        (Fork::Bellatrix, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::bellatrix::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Bellatrix, "SyncCommittee") => typed::<ssz::bellatrix::SyncCommittee>(bytes, preset),
        (Fork::Bellatrix, "SyncCommitteeContribution") => {
            typed::<auxiliary::bellatrix::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Bellatrix, "SyncCommitteeMessage") => {
            typed::<auxiliary::bellatrix::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Capella, "AggregateAndProof") => {
            typed::<auxiliary::capella::AggregateAndProof>(bytes, preset)
        }
        (Fork::Capella, "Attestation") => typed::<ssz::capella::Attestation>(bytes, preset),
        (Fork::Capella, "AttesterSlashing") => {
            typed::<ssz::capella::AttesterSlashing>(bytes, preset)
        }
        (Fork::Capella, "BLSToExecutionChange") => {
            typed::<ssz::capella::BLSToExecutionChange>(bytes, preset)
        }
        (Fork::Capella, "BeaconBlockBody") => typed::<ssz::capella::BeaconBlockBody>(bytes, preset),
        (Fork::Capella, "ContributionAndProof") => {
            typed::<auxiliary::capella::ContributionAndProof>(bytes, preset)
        }
        (Fork::Capella, "DepositMessage") => {
            typed::<auxiliary::capella::DepositMessage>(bytes, preset)
        }
        (Fork::Capella, "Eth1Block") => typed::<auxiliary::capella::Eth1Block>(bytes, preset),
        (Fork::Capella, "ExecutionPayload") => {
            typed::<ssz::capella::ExecutionPayload>(bytes, preset)
        }
        (Fork::Capella, "ExecutionPayloadHeader") => {
            typed::<ssz::capella::ExecutionPayloadHeader>(bytes, preset)
        }
        (Fork::Capella, "ForkData") => typed::<auxiliary::capella::ForkData>(bytes, preset),
        (Fork::Capella, "HistoricalSummary") => {
            typed::<ssz::capella::HistoricalSummary>(bytes, preset)
        }
        (Fork::Capella, "IndexedAttestation") => {
            typed::<ssz::capella::IndexedAttestation>(bytes, preset)
        }
        (Fork::Capella, "LightClientBootstrap") => {
            typed::<auxiliary::capella::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Capella, "LightClientFinalityUpdate") => {
            typed::<auxiliary::capella::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Capella, "LightClientHeader") => {
            typed::<auxiliary::capella::LightClientHeader>(bytes, preset)
        }
        (Fork::Capella, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::capella::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Capella, "LightClientUpdate") => {
            typed::<auxiliary::capella::LightClientUpdate>(bytes, preset)
        }
        (Fork::Capella, "NewPayloadRequest") => {
            typed::<auxiliary::capella::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Capella, "PowBlock") => typed::<auxiliary::capella::PowBlock>(bytes, preset),
        (Fork::Capella, "SignedAggregateAndProof") => {
            typed::<auxiliary::capella::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Capella, "SignedBLSToExecutionChange") => {
            typed::<ssz::capella::SignedBLSToExecutionChange>(bytes, preset)
        }
        (Fork::Capella, "SignedContributionAndProof") => {
            typed::<auxiliary::capella::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Capella, "SigningData") => typed::<auxiliary::capella::SigningData>(bytes, preset),
        (Fork::Capella, "SyncAggregate") => typed::<ssz::capella::SyncAggregate>(bytes, preset),
        (Fork::Capella, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::capella::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Capella, "SyncCommittee") => typed::<ssz::capella::SyncCommittee>(bytes, preset),
        (Fork::Capella, "SyncCommitteeContribution") => {
            typed::<auxiliary::capella::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Capella, "SyncCommitteeMessage") => {
            typed::<auxiliary::capella::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Capella, "Withdrawal") => typed::<ssz::capella::Withdrawal>(bytes, preset),
        (Fork::Deneb, "AggregateAndProof") => {
            typed::<auxiliary::deneb::AggregateAndProof>(bytes, preset)
        }
        (Fork::Deneb, "Attestation") => typed::<ssz::deneb::Attestation>(bytes, preset),
        (Fork::Deneb, "AttesterSlashing") => typed::<ssz::deneb::AttesterSlashing>(bytes, preset),
        (Fork::Deneb, "BLSToExecutionChange") => {
            typed::<ssz::deneb::BLSToExecutionChange>(bytes, preset)
        }
        (Fork::Deneb, "BeaconBlockBody") => typed::<ssz::deneb::BeaconBlockBody>(bytes, preset),
        (Fork::Deneb, "BlobIdentifier") => typed::<auxiliary::deneb::BlobIdentifier>(bytes, preset),
        (Fork::Deneb, "BlobSidecar") => typed::<auxiliary::deneb::BlobSidecar>(bytes, preset),
        (Fork::Deneb, "ContributionAndProof") => {
            typed::<auxiliary::deneb::ContributionAndProof>(bytes, preset)
        }
        (Fork::Deneb, "DepositMessage") => typed::<auxiliary::deneb::DepositMessage>(bytes, preset),
        (Fork::Deneb, "Eth1Block") => typed::<auxiliary::deneb::Eth1Block>(bytes, preset),
        (Fork::Deneb, "ExecutionPayload") => typed::<ssz::deneb::ExecutionPayload>(bytes, preset),
        (Fork::Deneb, "ExecutionPayloadHeader") => {
            typed::<ssz::deneb::ExecutionPayloadHeader>(bytes, preset)
        }
        (Fork::Deneb, "ForkData") => typed::<auxiliary::deneb::ForkData>(bytes, preset),
        (Fork::Deneb, "HistoricalSummary") => typed::<ssz::deneb::HistoricalSummary>(bytes, preset),
        (Fork::Deneb, "IndexedAttestation") => {
            typed::<ssz::deneb::IndexedAttestation>(bytes, preset)
        }
        (Fork::Deneb, "LightClientBootstrap") => {
            typed::<auxiliary::deneb::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Deneb, "LightClientFinalityUpdate") => {
            typed::<auxiliary::deneb::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Deneb, "LightClientHeader") => {
            typed::<auxiliary::deneb::LightClientHeader>(bytes, preset)
        }
        (Fork::Deneb, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::deneb::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Deneb, "LightClientUpdate") => {
            typed::<auxiliary::deneb::LightClientUpdate>(bytes, preset)
        }
        (Fork::Deneb, "NewPayloadRequest") => {
            typed::<auxiliary::deneb::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Deneb, "PowBlock") => typed::<auxiliary::deneb::PowBlock>(bytes, preset),
        (Fork::Deneb, "SignedAggregateAndProof") => {
            typed::<auxiliary::deneb::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Deneb, "SignedBLSToExecutionChange") => {
            typed::<ssz::deneb::SignedBLSToExecutionChange>(bytes, preset)
        }
        (Fork::Deneb, "SignedContributionAndProof") => {
            typed::<auxiliary::deneb::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Deneb, "SigningData") => typed::<auxiliary::deneb::SigningData>(bytes, preset),
        (Fork::Deneb, "SyncAggregate") => typed::<ssz::deneb::SyncAggregate>(bytes, preset),
        (Fork::Deneb, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::deneb::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Deneb, "SyncCommittee") => typed::<ssz::deneb::SyncCommittee>(bytes, preset),
        (Fork::Deneb, "SyncCommitteeContribution") => {
            typed::<auxiliary::deneb::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Deneb, "SyncCommitteeMessage") => {
            typed::<auxiliary::deneb::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Deneb, "Withdrawal") => typed::<ssz::deneb::Withdrawal>(bytes, preset),
        (Fork::Electra, "AggregateAndProof") => {
            typed::<auxiliary::electra::AggregateAndProof>(bytes, preset)
        }
        (Fork::Electra, "Attestation") => typed::<ssz::electra::Attestation>(bytes, preset),
        (Fork::Electra, "AttesterSlashing") => {
            typed::<ssz::electra::AttesterSlashing>(bytes, preset)
        }
        (Fork::Electra, "BLSToExecutionChange") => {
            typed::<ssz::electra::BLSToExecutionChange>(bytes, preset)
        }
        (Fork::Electra, "BeaconBlockBody") => typed::<ssz::electra::BeaconBlockBody>(bytes, preset),
        (Fork::Electra, "BlobIdentifier") => {
            typed::<auxiliary::electra::BlobIdentifier>(bytes, preset)
        }
        (Fork::Electra, "BlobSidecar") => typed::<auxiliary::electra::BlobSidecar>(bytes, preset),
        (Fork::Electra, "ConsolidationRequest") => {
            typed::<ssz::electra::ConsolidationRequest>(bytes, preset)
        }
        (Fork::Electra, "ContributionAndProof") => {
            typed::<auxiliary::electra::ContributionAndProof>(bytes, preset)
        }
        (Fork::Electra, "DepositMessage") => {
            typed::<auxiliary::electra::DepositMessage>(bytes, preset)
        }
        (Fork::Electra, "DepositRequest") => typed::<ssz::electra::DepositRequest>(bytes, preset),
        (Fork::Electra, "Eth1Block") => typed::<auxiliary::electra::Eth1Block>(bytes, preset),
        (Fork::Electra, "ExecutionPayload") => {
            typed::<ssz::electra::ExecutionPayload>(bytes, preset)
        }
        (Fork::Electra, "ExecutionPayloadHeader") => {
            typed::<ssz::electra::ExecutionPayloadHeader>(bytes, preset)
        }
        (Fork::Electra, "ExecutionRequests") => {
            typed::<ssz::electra::ExecutionRequests>(bytes, preset)
        }
        (Fork::Electra, "ForkData") => typed::<auxiliary::electra::ForkData>(bytes, preset),
        (Fork::Electra, "HistoricalSummary") => {
            typed::<ssz::electra::HistoricalSummary>(bytes, preset)
        }
        (Fork::Electra, "IndexedAttestation") => {
            typed::<ssz::electra::IndexedAttestation>(bytes, preset)
        }
        (Fork::Electra, "LightClientBootstrap") => {
            typed::<auxiliary::electra::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Electra, "LightClientFinalityUpdate") => {
            typed::<auxiliary::electra::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Electra, "LightClientHeader") => {
            typed::<auxiliary::electra::LightClientHeader>(bytes, preset)
        }
        (Fork::Electra, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::electra::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Electra, "LightClientUpdate") => {
            typed::<auxiliary::electra::LightClientUpdate>(bytes, preset)
        }
        (Fork::Electra, "NewPayloadRequest") => {
            typed::<auxiliary::electra::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Electra, "PendingConsolidation") => {
            typed::<ssz::electra::PendingConsolidation>(bytes, preset)
        }
        (Fork::Electra, "PendingDeposit") => typed::<ssz::electra::PendingDeposit>(bytes, preset),
        (Fork::Electra, "PendingPartialWithdrawal") => {
            typed::<ssz::electra::PendingPartialWithdrawal>(bytes, preset)
        }
        (Fork::Electra, "PowBlock") => typed::<auxiliary::electra::PowBlock>(bytes, preset),
        (Fork::Electra, "SignedAggregateAndProof") => {
            typed::<auxiliary::electra::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Electra, "SignedBLSToExecutionChange") => {
            typed::<ssz::electra::SignedBLSToExecutionChange>(bytes, preset)
        }
        (Fork::Electra, "SignedContributionAndProof") => {
            typed::<auxiliary::electra::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Electra, "SigningData") => typed::<auxiliary::electra::SigningData>(bytes, preset),
        (Fork::Electra, "SingleAttestation") => {
            typed::<auxiliary::electra::SingleAttestation>(bytes, preset)
        }
        (Fork::Electra, "SyncAggregate") => typed::<ssz::electra::SyncAggregate>(bytes, preset),
        (Fork::Electra, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::electra::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Electra, "SyncCommittee") => typed::<ssz::electra::SyncCommittee>(bytes, preset),
        (Fork::Electra, "SyncCommitteeContribution") => {
            typed::<auxiliary::electra::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Electra, "SyncCommitteeMessage") => {
            typed::<auxiliary::electra::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Electra, "Withdrawal") => typed::<ssz::electra::Withdrawal>(bytes, preset),
        (Fork::Electra, "WithdrawalRequest") => {
            typed::<ssz::electra::WithdrawalRequest>(bytes, preset)
        }
        (Fork::Fulu, "AggregateAndProof") => {
            typed::<auxiliary::fulu::AggregateAndProof>(bytes, preset)
        }
        (Fork::Fulu, "Attestation") => typed::<ssz::fulu::Attestation>(bytes, preset),
        (Fork::Fulu, "AttesterSlashing") => typed::<ssz::fulu::AttesterSlashing>(bytes, preset),
        (Fork::Fulu, "BLSToExecutionChange") => {
            typed::<ssz::fulu::BLSToExecutionChange>(bytes, preset)
        }
        (Fork::Fulu, "BeaconBlockBody") => typed::<ssz::fulu::BeaconBlockBody>(bytes, preset),
        (Fork::Fulu, "ConsolidationRequest") => {
            typed::<ssz::fulu::ConsolidationRequest>(bytes, preset)
        }
        (Fork::Fulu, "ContributionAndProof") => {
            typed::<auxiliary::fulu::ContributionAndProof>(bytes, preset)
        }
        (Fork::Fulu, "DataColumnSidecar") => {
            typed::<auxiliary::fulu::DataColumnSidecar>(bytes, preset)
        }
        (Fork::Fulu, "DataColumnsByRootIdentifier") => {
            typed::<auxiliary::fulu::DataColumnsByRootIdentifier>(bytes, preset)
        }
        (Fork::Fulu, "DepositMessage") => typed::<auxiliary::fulu::DepositMessage>(bytes, preset),
        (Fork::Fulu, "DepositRequest") => typed::<ssz::fulu::DepositRequest>(bytes, preset),
        (Fork::Fulu, "Eth1Block") => typed::<auxiliary::fulu::Eth1Block>(bytes, preset),
        (Fork::Fulu, "ExecutionPayload") => typed::<ssz::fulu::ExecutionPayload>(bytes, preset),
        (Fork::Fulu, "ExecutionPayloadHeader") => {
            typed::<ssz::fulu::ExecutionPayloadHeader>(bytes, preset)
        }
        (Fork::Fulu, "ExecutionRequests") => typed::<ssz::fulu::ExecutionRequests>(bytes, preset),
        (Fork::Fulu, "ForkData") => typed::<auxiliary::fulu::ForkData>(bytes, preset),
        (Fork::Fulu, "HistoricalSummary") => typed::<ssz::fulu::HistoricalSummary>(bytes, preset),
        (Fork::Fulu, "IndexedAttestation") => typed::<ssz::fulu::IndexedAttestation>(bytes, preset),
        (Fork::Fulu, "LightClientBootstrap") => {
            typed::<auxiliary::fulu::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Fulu, "LightClientFinalityUpdate") => {
            typed::<auxiliary::fulu::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Fulu, "LightClientHeader") => {
            typed::<auxiliary::fulu::LightClientHeader>(bytes, preset)
        }
        (Fork::Fulu, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::fulu::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Fulu, "LightClientUpdate") => {
            typed::<auxiliary::fulu::LightClientUpdate>(bytes, preset)
        }
        (Fork::Fulu, "MatrixEntry") => typed::<auxiliary::fulu::MatrixEntry>(bytes, preset),
        (Fork::Fulu, "NewPayloadRequest") => {
            typed::<auxiliary::fulu::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Fulu, "PartialDataColumnGroupID") => {
            typed::<auxiliary::fulu::PartialDataColumnGroupID>(bytes, preset)
        }
        (Fork::Fulu, "PartialDataColumnHeader") => {
            typed::<auxiliary::fulu::PartialDataColumnHeader>(bytes, preset)
        }
        (Fork::Fulu, "PartialDataColumnPartsMetadata") => {
            typed::<auxiliary::fulu::PartialDataColumnPartsMetadata>(bytes, preset)
        }
        (Fork::Fulu, "PartialDataColumnSidecar") => {
            typed::<auxiliary::fulu::PartialDataColumnSidecar>(bytes, preset)
        }
        (Fork::Fulu, "PendingConsolidation") => {
            typed::<ssz::fulu::PendingConsolidation>(bytes, preset)
        }
        (Fork::Fulu, "PendingDeposit") => typed::<ssz::fulu::PendingDeposit>(bytes, preset),
        (Fork::Fulu, "PendingPartialWithdrawal") => {
            typed::<ssz::fulu::PendingPartialWithdrawal>(bytes, preset)
        }
        (Fork::Fulu, "PowBlock") => typed::<auxiliary::fulu::PowBlock>(bytes, preset),
        (Fork::Fulu, "SignedAggregateAndProof") => {
            typed::<auxiliary::fulu::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Fulu, "SignedBLSToExecutionChange") => {
            typed::<ssz::fulu::SignedBLSToExecutionChange>(bytes, preset)
        }
        (Fork::Fulu, "SignedContributionAndProof") => {
            typed::<auxiliary::fulu::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Fulu, "SigningData") => typed::<auxiliary::fulu::SigningData>(bytes, preset),
        (Fork::Fulu, "SingleAttestation") => {
            typed::<auxiliary::fulu::SingleAttestation>(bytes, preset)
        }
        (Fork::Fulu, "SyncAggregate") => typed::<ssz::fulu::SyncAggregate>(bytes, preset),
        (Fork::Fulu, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::fulu::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Fulu, "SyncCommittee") => typed::<ssz::fulu::SyncCommittee>(bytes, preset),
        (Fork::Fulu, "SyncCommitteeContribution") => {
            typed::<auxiliary::fulu::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Fulu, "SyncCommitteeMessage") => {
            typed::<auxiliary::fulu::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Fulu, "Withdrawal") => typed::<ssz::fulu::Withdrawal>(bytes, preset),
        (Fork::Fulu, "WithdrawalRequest") => typed::<ssz::fulu::WithdrawalRequest>(bytes, preset),
        (Fork::Gloas, "AggregateAndProof") => {
            typed::<auxiliary::gloas::AggregateAndProof>(bytes, preset)
        }
        (Fork::Gloas, "Attestation") => typed::<ssz::gloas::Attestation>(bytes, preset),
        (Fork::Gloas, "AttesterSlashing") => typed::<ssz::gloas::AttesterSlashing>(bytes, preset),
        (Fork::Gloas, "BLSToExecutionChange") => {
            typed::<ssz::gloas::BLSToExecutionChange>(bytes, preset)
        }
        (Fork::Gloas, "BeaconBlockBody") => typed::<ssz::gloas::BeaconBlockBody>(bytes, preset),
        (Fork::Gloas, "Builder") => typed::<ssz::gloas::Builder>(bytes, preset),
        (Fork::Gloas, "BuilderDepositRequest") => {
            typed::<ssz::gloas::BuilderDepositRequest>(bytes, preset)
        }
        (Fork::Gloas, "BuilderExitRequest") => {
            typed::<ssz::gloas::BuilderExitRequest>(bytes, preset)
        }
        (Fork::Gloas, "BuilderPendingPayment") => {
            typed::<ssz::gloas::BuilderPendingPayment>(bytes, preset)
        }
        (Fork::Gloas, "BuilderPendingWithdrawal") => {
            typed::<ssz::gloas::BuilderPendingWithdrawal>(bytes, preset)
        }
        (Fork::Gloas, "ConsolidationRequest") => {
            typed::<ssz::gloas::ConsolidationRequest>(bytes, preset)
        }
        (Fork::Gloas, "ContributionAndProof") => {
            typed::<auxiliary::gloas::ContributionAndProof>(bytes, preset)
        }
        (Fork::Gloas, "DataColumnSidecar") => {
            typed::<auxiliary::gloas::DataColumnSidecar>(bytes, preset)
        }
        (Fork::Gloas, "DataColumnsByRootIdentifier") => {
            typed::<auxiliary::gloas::DataColumnsByRootIdentifier>(bytes, preset)
        }
        (Fork::Gloas, "DepositMessage") => typed::<auxiliary::gloas::DepositMessage>(bytes, preset),
        (Fork::Gloas, "DepositRequest") => typed::<ssz::gloas::DepositRequest>(bytes, preset),
        (Fork::Gloas, "Eth1Block") => typed::<auxiliary::gloas::Eth1Block>(bytes, preset),
        (Fork::Gloas, "ExecutionPayload") => typed::<ssz::gloas::ExecutionPayload>(bytes, preset),
        (Fork::Gloas, "ExecutionPayloadBid") => {
            typed::<ssz::gloas::ExecutionPayloadBid>(bytes, preset)
        }
        (Fork::Gloas, "ExecutionPayloadEnvelope") => {
            typed::<ssz::gloas::ExecutionPayloadEnvelope>(bytes, preset)
        }
        (Fork::Gloas, "ExecutionRequests") => typed::<ssz::gloas::ExecutionRequests>(bytes, preset),
        (Fork::Gloas, "ForkData") => typed::<auxiliary::gloas::ForkData>(bytes, preset),
        (Fork::Gloas, "HistoricalSummary") => typed::<ssz::gloas::HistoricalSummary>(bytes, preset),
        (Fork::Gloas, "IndexedAttestation") => {
            typed::<ssz::gloas::IndexedAttestation>(bytes, preset)
        }
        (Fork::Gloas, "IndexedPayloadAttestation") => {
            typed::<auxiliary::gloas::IndexedPayloadAttestation>(bytes, preset)
        }
        (Fork::Gloas, "LightClientBootstrap") => {
            typed::<auxiliary::gloas::LightClientBootstrap>(bytes, preset)
        }
        (Fork::Gloas, "LightClientFinalityUpdate") => {
            typed::<auxiliary::gloas::LightClientFinalityUpdate>(bytes, preset)
        }
        (Fork::Gloas, "LightClientHeader") => {
            typed::<auxiliary::gloas::LightClientHeader>(bytes, preset)
        }
        (Fork::Gloas, "LightClientOptimisticUpdate") => {
            typed::<auxiliary::gloas::LightClientOptimisticUpdate>(bytes, preset)
        }
        (Fork::Gloas, "LightClientUpdate") => {
            typed::<auxiliary::gloas::LightClientUpdate>(bytes, preset)
        }
        (Fork::Gloas, "MatrixEntry") => typed::<auxiliary::gloas::MatrixEntry>(bytes, preset),
        (Fork::Gloas, "NewPayloadRequest") => {
            typed::<auxiliary::gloas::NewPayloadRequest>(bytes, preset)
        }
        (Fork::Gloas, "PartialDataColumnGroupID") => {
            typed::<auxiliary::gloas::PartialDataColumnGroupID>(bytes, preset)
        }
        (Fork::Gloas, "PartialDataColumnPartsMetadata") => {
            typed::<auxiliary::gloas::PartialDataColumnPartsMetadata>(bytes, preset)
        }
        (Fork::Gloas, "PartialDataColumnSidecar") => {
            typed::<auxiliary::gloas::PartialDataColumnSidecar>(bytes, preset)
        }
        (Fork::Gloas, "PayloadAttestation") => {
            typed::<ssz::gloas::PayloadAttestation>(bytes, preset)
        }
        (Fork::Gloas, "PayloadAttestationData") => {
            typed::<ssz::gloas::PayloadAttestationData>(bytes, preset)
        }
        (Fork::Gloas, "PayloadAttestationMessage") => {
            typed::<auxiliary::gloas::PayloadAttestationMessage>(bytes, preset)
        }
        (Fork::Gloas, "PendingConsolidation") => {
            typed::<ssz::gloas::PendingConsolidation>(bytes, preset)
        }
        (Fork::Gloas, "PendingDeposit") => typed::<ssz::gloas::PendingDeposit>(bytes, preset),
        (Fork::Gloas, "PendingPartialWithdrawal") => {
            typed::<ssz::gloas::PendingPartialWithdrawal>(bytes, preset)
        }
        (Fork::Gloas, "PowBlock") => typed::<auxiliary::gloas::PowBlock>(bytes, preset),
        (Fork::Gloas, "ProposerPreferences") => {
            typed::<auxiliary::gloas::ProposerPreferences>(bytes, preset)
        }
        (Fork::Gloas, "SignedAggregateAndProof") => {
            typed::<auxiliary::gloas::SignedAggregateAndProof>(bytes, preset)
        }
        (Fork::Gloas, "SignedBLSToExecutionChange") => {
            typed::<ssz::gloas::SignedBLSToExecutionChange>(bytes, preset)
        }
        (Fork::Gloas, "SignedContributionAndProof") => {
            typed::<auxiliary::gloas::SignedContributionAndProof>(bytes, preset)
        }
        (Fork::Gloas, "SignedExecutionPayloadBid") => {
            typed::<ssz::gloas::SignedExecutionPayloadBid>(bytes, preset)
        }
        (Fork::Gloas, "SignedExecutionPayloadEnvelope") => {
            typed::<ssz::gloas::SignedExecutionPayloadEnvelope>(bytes, preset)
        }
        (Fork::Gloas, "SignedProposerPreferences") => {
            typed::<auxiliary::gloas::SignedProposerPreferences>(bytes, preset)
        }
        (Fork::Gloas, "SigningData") => typed::<auxiliary::gloas::SigningData>(bytes, preset),
        (Fork::Gloas, "SingleAttestation") => {
            typed::<auxiliary::gloas::SingleAttestation>(bytes, preset)
        }
        (Fork::Gloas, "SyncAggregate") => typed::<ssz::gloas::SyncAggregate>(bytes, preset),
        (Fork::Gloas, "SyncAggregatorSelectionData") => {
            typed::<auxiliary::gloas::SyncAggregatorSelectionData>(bytes, preset)
        }
        (Fork::Gloas, "SyncCommittee") => typed::<ssz::gloas::SyncCommittee>(bytes, preset),
        (Fork::Gloas, "SyncCommitteeContribution") => {
            typed::<auxiliary::gloas::SyncCommitteeContribution>(bytes, preset)
        }
        (Fork::Gloas, "SyncCommitteeMessage") => {
            typed::<auxiliary::gloas::SyncCommitteeMessage>(bytes, preset)
        }
        (Fork::Gloas, "Withdrawal") => typed::<ssz::gloas::Withdrawal>(bytes, preset),
        (Fork::Gloas, "WithdrawalRequest") => typed::<ssz::gloas::WithdrawalRequest>(bytes, preset),
        _ => Err(format!("unsupported complete static schema: {fork:?}/{handler}")),
    }
}

fn typed<T: Ssz>(bytes: &[u8], preset: Preset) -> Result<(Root, Vec<u8>), String> {
    let value = T::from_ssz(bytes, preset).map_err(|error| error.to_string())?;
    let encoded = value.to_ssz(preset).map_err(|error| error.to_string())?;
    if encoded != bytes {
        return Err("noncanonical typed SSZ roundtrip".into())
    }
    let root = value.hash_tree_root(preset).map_err(|error| error.to_string())?;
    Ok((root, encoded))
}
