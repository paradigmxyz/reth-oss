//! Complete Phase0 state, block and operation schemas from the pinned specification.

use super::{
    codec::{ssz_container, PresetLength},
    BitListValue, Bits, Fixed, List, Preset, Ssz, Vector,
};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint, Root, SszError};

/// Historical block-root ring vector.
pub type BlockRoots = Vector<Root, PresetLength<8192, 64>>;

/// Historical state-root ring vector.
pub type StateRoots = Vector<Root, PresetLength<8192, 64>>;

/// Accumulated historical batch roots.
pub type HistoricalRoots = List<Root, Fixed<16777216>>;

/// Validator registry with its exact specification limit.
pub type Validators = List<Validator, Fixed<1099511627776>>;

/// Packed validator balances.
pub type Balances = List<u64, Fixed<1099511627776>>;

/// Epoch RANDAO mix ring vector.
pub type RandaoMixes = Vector<Root, PresetLength<65536, 64>>;

/// Packed slashings ring vector.
pub type Slashings = Vector<u64, PresetLength<8192, 64>>;

/// Voting-period execution data votes.
pub type Eth1DataVotes = List<Eth1Data, PresetLength<2048, 32>>;

/// Pending attestations for one epoch.
pub type PendingAttestations = List<PendingAttestation, PresetLength<4096, 1024>>;

/// Deposit tree branch, including the deposit-count mix-in sibling.
pub type DepositProof = Vector<Root, Fixed<33>>;

/// Committee aggregation bitlist with its exact Phase0 bound.
pub type AggregationBits = BitListValue<2048>;

/// Sorted participant indexes; ordering is a transition check, not structural SSZ.
pub type AttestingIndices = List<u64, Fixed<2048>>;

/// Proposer slashings in one block.
pub type ProposerSlashings = List<ProposerSlashing, Fixed<16>>;

/// Attester slashings in one block.
pub type AttesterSlashings = List<AttesterSlashing, Fixed<2>>;

/// Attestations in one block.
pub type Attestations = List<Attestation, Fixed<128>>;

/// Deposits in one block.
pub type Deposits = List<Deposit, Fixed<16>>;

/// Signed voluntary exits in one block.
pub type VoluntaryExits = List<SignedVoluntaryExit, Fixed<16>>;

ssz_container! {
    /// The previous and current fork versions and activation epoch stored in a state.
    ForkInfo {
        /// Previous four-byte fork version.
        previous_version: [u8; 4],
        /// Current four-byte fork version.
        current_version: [u8; 4],
        /// Activation epoch of the current version.
        epoch: u64,
    }
}

ssz_container! {
    /// Complete validator record with canonical boolean encoding.
    Validator {
        /// Compressed BLS public key; contextual cryptographic checks are separate.
        pubkey: [u8; 48],
        /// Withdrawal credentials.
        withdrawal_credentials: Root,
        /// Effective balance in gwei.
        effective_balance: u64,
        /// Slashing status.
        slashed: bool,
        /// Activation eligibility epoch.
        activation_eligibility_epoch: u64,
        /// Activation epoch.
        activation_epoch: u64,
        /// Exit epoch.
        exit_epoch: u64,
        /// Withdrawability epoch.
        withdrawable_epoch: u64,
    }
}

ssz_container! {
    /// Execution deposit-tree vote data.
    Eth1Data {
        /// Deposit-tree root.
        deposit_root: Root,
        /// Number of deposits.
        deposit_count: u64,
        /// Execution block hash.
        block_hash: Root,
    }
}

ssz_container! {
    /// Phase0 attestation identity and source/target checkpoints.
    AttestationData {
        /// Attested slot.
        slot: u64,
        /// Committee index.
        index: u64,
        /// Attested beacon block root.
        beacon_block_root: Root,
        /// Source checkpoint.
        source: Checkpoint,
        /// Target checkpoint.
        target: Checkpoint,
    }
}

ssz_container! {
    /// Pending Phase0 participation record stored in the state.
    PendingAttestation {
        /// Aggregating committee participants.
        aggregation_bits: AggregationBits,
        /// Attestation identity and checkpoints.
        data: AttestationData,
        /// Slots between attestation and inclusion.
        inclusion_delay: u64,
        /// Index of the including proposer.
        proposer_index: u64,
    }
}

ssz_container! {
    /// Phase0 signed aggregate attestation.
    Attestation {
        /// Aggregating committee participants.
        aggregation_bits: AggregationBits,
        /// Attestation identity and checkpoints.
        data: AttestationData,
        /// Aggregate BLS signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Indexed Phase0 attestation used for slashings.
    IndexedAttestation {
        /// Participant validator indexes.
        attesting_indices: AttestingIndices,
        /// Attestation identity and checkpoints.
        data: AttestationData,
        /// Aggregate BLS signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Signed common beacon header.
    SignedBeaconBlockHeader {
        /// Unsigned header.
        message: BeaconBlockHeader,
        /// Proposer BLS signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Two conflicting proposer headers.
    ProposerSlashing {
        /// First signed header.
        signed_header_1: SignedBeaconBlockHeader,
        /// Second signed header.
        signed_header_2: SignedBeaconBlockHeader,
    }
}

ssz_container! {
    /// Two conflicting indexed attestations.
    AttesterSlashing {
        /// First indexed attestation.
        attestation_1: IndexedAttestation,
        /// Second indexed attestation.
        attestation_2: IndexedAttestation,
    }
}

ssz_container! {
    /// Deposit payload including proof of possession.
    DepositData {
        /// Validator BLS public key.
        pubkey: [u8; 48],
        /// Withdrawal credentials.
        withdrawal_credentials: Root,
        /// Deposit amount in gwei.
        amount: u64,
        /// Deposit signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Deposit data with the exact depth-33 Merkle branch.
    Deposit {
        /// Tree branch and count sibling.
        proof: DepositProof,
        /// Complete deposit payload.
        data: DepositData,
    }
}

ssz_container! {
    /// Voluntary validator exit.
    VoluntaryExit {
        /// Earliest exit epoch.
        epoch: u64,
        /// Exiting validator index.
        validator_index: u64,
    }
}

ssz_container! {
    /// Signed voluntary validator exit.
    SignedVoluntaryExit {
        /// Unsigned exit.
        message: VoluntaryExit,
        /// Validator BLS signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete Phase0 beacon block body, including every operation list.
    BeaconBlockBody {
        /// Proposer RANDAO reveal.
        randao_reveal: [u8; 96],
        /// Execution deposit-tree vote.
        eth1_data: Eth1Data,
        /// Proposer graffiti.
        graffiti: Root,
        /// Proposer slashings.
        proposer_slashings: ProposerSlashings,
        /// Attester slashings.
        attester_slashings: AttesterSlashings,
        /// Signed committee attestations.
        attestations: Attestations,
        /// Deposits and their tree proofs.
        deposits: Deposits,
        /// Signed voluntary exits.
        voluntary_exits: VoluntaryExits,
    }
}

ssz_container! {
    /// Complete unsigned Phase0 beacon block.
    BeaconBlock {
        /// Proposed slot.
        slot: u64,
        /// Proposer validator index.
        proposer_index: u64,
        /// Parent beacon block root.
        parent_root: Root,
        /// Claimed post-state root, authenticated only after normal transition validation.
        state_root: Root,
        /// Complete block body.
        body: BeaconBlockBody,
    }
}

ssz_container! {
    /// Complete signed Phase0 beacon block.
    SignedBeaconBlock {
        /// Unsigned block.
        message: BeaconBlock,
        /// Proposer BLS signature.
        signature: [u8; 96],
    }
}

ssz_container! {
    /// Complete 21-field Phase0 state with published preset-specific bounds.
    BeaconState {
        /// Consensus genesis Unix timestamp.
        genesis_time: u64,
        /// Genesis validator registry root.
        genesis_validators_root: Root,
        /// State slot.
        slot: u64,
        /// Previous and current fork versions.
        fork: ForkInfo,
        /// Latest block header.
        latest_block_header: BeaconBlockHeader,
        /// Historical block-root ring.
        block_roots: BlockRoots,
        /// Historical state-root ring.
        state_roots: StateRoots,
        /// Historical batch roots.
        historical_roots: HistoricalRoots,
        /// Current execution deposit-tree vote.
        eth1_data: Eth1Data,
        /// Voting-period execution deposit-tree votes.
        eth1_data_votes: Eth1DataVotes,
        /// Next deposit index.
        eth1_deposit_index: u64,
        /// Complete validator registry.
        validators: Validators,
        /// Validator balances.
        balances: Balances,
        /// Historical RANDAO mixes.
        randao_mixes: RandaoMixes,
        /// Slashings totals.
        slashings: Slashings,
        /// Previous-epoch pending attestations.
        previous_epoch_attestations: PendingAttestations,
        /// Current-epoch pending attestations.
        current_epoch_attestations: PendingAttestations,
        /// Four justification bits.
        justification_bits: Bits<4>,
        /// Previous justified checkpoint.
        previous_justified_checkpoint: Checkpoint,
        /// Current justified checkpoint.
        current_justified_checkpoint: Checkpoint,
        /// Finalized checkpoint.
        finalized_checkpoint: Checkpoint,
    }
}

impl BeaconBlock {
    /// Derive the complete unsigned block header from its typed body.
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

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_primitives::{container_fields, Fork};

    fn empty_state(preset: Preset) -> BeaconState {
        let roots = if preset == Preset::Mainnet { 8192 } else { 64 };
        let mixes = if preset == Preset::Mainnet { 65536 } else { 64 };
        let slashings = if preset == Preset::Mainnet { 8192 } else { 64 };
        BeaconState {
            genesis_time: 100,
            genesis_validators_root: [1; 32],
            slot: 32,
            fork: ForkInfo { previous_version: [0; 4], current_version: [0; 4], epoch: 0 },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::from_vec(vec![[0; 32]; roots]),
            state_roots: Vector::from_vec(vec![[0; 32]; roots]),
            historical_roots: List::default(),
            eth1_data: Eth1Data { deposit_root: [0; 32], deposit_count: 0, block_hash: [0; 32] },
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::default(),
            balances: List::default(),
            randao_mixes: Vector::from_vec(vec![[0; 32]; mixes]),
            slashings: Vector::from_vec(vec![0; slashings]),
            previous_epoch_attestations: List::default(),
            current_epoch_attestations: List::default(),
            justification_bits: Bits::from_ssz(&[0], preset).unwrap(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
        }
    }
    fn empty_block() -> BeaconBlock {
        BeaconBlock {
            slot: 32,
            proposer_index: 1,
            parent_root: [2; 32],
            state_root: [3; 32],
            body: BeaconBlockBody {
                randao_reveal: [4; 96],
                eth1_data: Eth1Data {
                    deposit_root: [5; 32],
                    deposit_count: 0,
                    block_hash: [6; 32],
                },
                graffiti: [7; 32],
                proposer_slashings: List::default(),
                attester_slashings: List::default(),
                attestations: List::default(),
                deposits: List::default(),
                voluntary_exits: List::default(),
            },
        }
    }

    #[test]
    fn complete_state_roundtrips_both_presets_and_exposes_identity() {
        for preset in [Preset::Minimal, Preset::Mainnet] {
            let state = empty_state(preset);
            let bytes = state.to_ssz(preset).unwrap();
            let decoded =
                super::super::BeaconState::from_ssz(&bytes, Fork::Phase0, preset).unwrap();
            assert_eq!(decoded.to_ssz().unwrap(), bytes);
            assert_eq!(decoded.hash_tree_root().unwrap(), state.hash_tree_root(preset).unwrap());
            assert_eq!(decoded.slot(), 32);
            assert_eq!(decoded.genesis_time(), 100);
            assert_eq!(decoded.genesis_validators_root(), [1; 32]);
            assert_eq!(decoded.latest_block_header(), BeaconBlockHeader::default());
        }
    }

    #[test]
    fn signed_and_unsigned_roots_and_full_body_header_are_distinct() {
        let block = empty_block();
        let signed = SignedBeaconBlock { message: block.clone(), signature: [8; 96] };
        let bytes = signed.to_ssz(Preset::Minimal).unwrap();
        let decoded =
            super::super::SignedBeaconBlock::from_ssz(&bytes, Fork::Phase0, Preset::Minimal)
                .unwrap();
        assert_eq!(decoded.to_ssz().unwrap(), bytes);
        assert_eq!(decoded.message_root().unwrap(), block.hash_tree_root(Preset::Minimal).unwrap());
        assert_ne!(decoded.message_root().unwrap(), decoded.hash_tree_root().unwrap());
        assert_eq!(decoded.header().unwrap().hash_tree_root(), decoded.message_root().unwrap());
    }

    #[test]
    fn malformed_offsets_vectors_boolean_and_preset_are_rejected() {
        let block = empty_block().to_ssz(Preset::Minimal).unwrap();
        let mut bad = block.clone();
        bad[80..84].copy_from_slice(&85u32.to_le_bytes());
        assert!(BeaconBlock::from_ssz(&bad, Preset::Minimal).is_err());
        let mut validator = vec![0; 121];
        validator[88] = 2;
        assert_eq!(Validator::from_ssz(&validator, Preset::Minimal), Err(SszError::InvalidBoolean));
        let state = empty_state(Preset::Minimal).to_ssz(Preset::Minimal).unwrap();
        assert!(BeaconState::from_ssz(&state, Preset::Mainnet).is_err());
        let mut state = empty_state(Preset::Minimal);
        state.block_roots.values.pop();
        assert!(state.to_ssz(Preset::Minimal).is_err());
        assert!(state.hash_tree_root(Preset::Minimal).is_err());
        assert!(container_fields(&block, &[Some(80), None]).is_ok());
    }

    #[test]
    fn variable_lists_enforce_count_offsets_and_bitlist_limits() {
        let preset = Preset::Minimal;
        let data = AttestationData {
            slot: 1,
            index: 0,
            beacon_block_root: [1; 32],
            source: Checkpoint::default(),
            target: Checkpoint::default(),
        };
        let attestation = Attestation {
            aggregation_bits: AggregationBits::from_ssz(&[0b1011], preset).unwrap(),
            data,
            signature: [0; 96],
        };
        let list = Attestations::from_vec(vec![attestation.clone(); 2]);
        let bytes = list.to_ssz(preset).unwrap();
        assert_eq!(Attestations::from_ssz(&bytes, preset).unwrap(), list);
        let mut bad = bytes;
        bad[..4].copy_from_slice(&9u32.to_le_bytes());
        assert!(Attestations::from_ssz(&bad, preset).is_err());
        let too_many = Attestations::from_vec(vec![attestation; 129]);
        assert!(too_many.to_ssz(preset).is_err());
        assert!(too_many.hash_tree_root(preset).is_err());
        let mut too_long = vec![0; 257];
        too_long[256] = 2;
        assert!(AggregationBits::from_ssz(&too_long, preset).is_err());
    }
}
