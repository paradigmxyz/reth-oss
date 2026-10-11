//! Concrete Gloas SSZ schemas pinned to consensus-specs v1.7.0-beta.3.
//!
//! These types perform structural SSZ validation and Merkleization. Import must
//! additionally check signatures, builder eligibility, payment balances, PTC
//! membership, execution validity, and fork-specific state-transition conditions.

use crate::{
    bytes_root, container_fields, hash_pair, merkleize, merkleize_progressive, mix_in_length,
    pack_bytes, progressive_container_root, read_bool, read_root, read_u64, uint64_root, BitVector,
    Root, Slot, SszError,
};

/// Gloas builder execution payload bid with all twelve active fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPayloadBid {
    /// Hash of the parent execution block.
    pub parent_block_hash: Root,
    /// Root of the parent beacon block.
    pub parent_block_root: Root,
    /// Execution block hash being offered.
    pub block_hash: Root,
    /// Epoch RANDAO mix.
    pub prev_randao: Root,
    /// Execution fee recipient.
    pub fee_recipient: [u8; 20],
    /// Execution gas limit.
    pub gas_limit: u64,
    /// Builder registry index or the self-build sentinel.
    pub builder_index: u64,
    /// Bid slot.
    pub slot: Slot,
    /// Bid payment in gwei.
    pub value: u64,
    /// Execution payment in gwei.
    pub execution_payment: u64,
    /// Blob commitments, represented as a progressive list.
    pub blob_kzg_commitments: Vec<[u8; 48]>,
    /// Root of the execution requests.
    pub execution_requests_root: Root,
}

impl ExecutionPayloadBid {
    /// Decode canonical SSZ and enforce the current network's commitment limit.
    pub fn from_ssz(bytes: &[u8], max_commitments: usize) -> Result<Self, SszError> {
        let fields = container_fields(
            bytes,
            &[
                Some(32),
                Some(32),
                Some(32),
                Some(32),
                Some(20),
                Some(8),
                Some(8),
                Some(8),
                Some(8),
                Some(8),
                None,
                Some(32),
            ],
        )?;
        let commitments = fixed_records::<48>(fields[10], max_commitments)?;
        Ok(Self {
            parent_block_hash: read_root(fields[0])?,
            parent_block_root: read_root(fields[1])?,
            block_hash: read_root(fields[2])?,
            prev_randao: read_root(fields[3])?,
            fee_recipient: fields[4].try_into().map_err(|_| SszError::InvalidOffset)?,
            gas_limit: read_u64(fields[5])?,
            builder_index: read_u64(fields[6])?,
            slot: read_u64(fields[7])?,
            value: read_u64(fields[8])?,
            execution_payment: read_u64(fields[9])?,
            blob_kzg_commitments: commitments,
            execution_requests_root: read_root(fields[11])?,
        })
    }

    /// Encode canonical SSZ, rejecting offsets beyond uint32.
    pub fn to_ssz(&self) -> Result<Vec<u8>, SszError> {
        let size = self
            .blob_kzg_commitments
            .len()
            .checked_mul(48)
            .and_then(|n| n.checked_add(224))
            .ok_or(SszError::Overflow)?;
        u32::try_from(size).map_err(|_| SszError::Overflow)?;
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(&self.parent_block_hash);
        bytes.extend_from_slice(&self.parent_block_root);
        bytes.extend_from_slice(&self.block_hash);
        bytes.extend_from_slice(&self.prev_randao);
        bytes.extend_from_slice(&self.fee_recipient);
        for scalar in
            [self.gas_limit, self.builder_index, self.slot, self.value, self.execution_payment]
        {
            bytes.extend_from_slice(&scalar.to_le_bytes());
        }
        bytes.extend_from_slice(&224u32.to_le_bytes());
        bytes.extend_from_slice(&self.execution_requests_root);
        for commitment in &self.blob_kzg_commitments {
            bytes.extend_from_slice(commitment);
        }
        Ok(bytes)
    }

    /// Compute the twelve-field progressive root, enforcing contextual commitment bounds.
    pub fn hash_tree_root(&self, max_commitments: usize) -> Result<Root, SszError> {
        check_limit(self.blob_kzg_commitments.len(), max_commitments)?;
        let roots = self
            .blob_kzg_commitments
            .iter()
            .map(|commitment| bytes_root(commitment, 48, false))
            .collect::<Result<Vec<_>, _>>()?;
        progressive_container_root(
            &[
                self.parent_block_hash,
                self.parent_block_root,
                self.block_hash,
                self.prev_randao,
                bytes_root(&self.fee_recipient, 20, false)?,
                uint64_root(self.gas_limit),
                uint64_root(self.builder_index),
                uint64_root(self.slot),
                uint64_root(self.value),
                uint64_root(self.execution_payment),
                mix_in_length(merkleize_progressive(&roots)?, roots.len() as u64),
                self.execution_requests_root,
            ],
            &[true; 12],
        )
    }
}

/// A signed payload bid; the wrapper is a standard two-field container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedExecutionPayloadBid {
    /// Builder's bid.
    pub message: ExecutionPayloadBid,
    /// Builder signature, or the self-build infinity sentinel.
    pub signature: [u8; 96],
}

impl SignedExecutionPayloadBid {
    /// Decode a signed bid using the network commitment bound.
    pub fn from_ssz(bytes: &[u8], max_commitments: usize) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[None, Some(96)])?;
        Ok(Self {
            message: ExecutionPayloadBid::from_ssz(fields[0], max_commitments)?,
            signature: fields[1].try_into().map_err(|_| SszError::InvalidOffset)?,
        })
    }

    /// Encode the standard signed wrapper.
    pub fn to_ssz(&self) -> Result<Vec<u8>, SszError> {
        encode_container(&[None, Some(&self.signature)], &[self.message.to_ssz()?])
    }

    /// Root of the standard signed wrapper.
    pub fn hash_tree_root(&self, max_commitments: usize) -> Result<Root, SszError> {
        Ok(hash_pair(
            self.message.hash_tree_root(max_commitments)?,
            bytes_root(&self.signature, 96, false)?,
        ))
    }
}

/// PTC vote data, which remains a standard container in Gloas.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PayloadAttestationData {
    /// Voted beacon block.
    pub beacon_block_root: Root,
    /// Voted slot.
    pub slot: Slot,
    /// Whether the execution payload was received.
    pub payload_present: bool,
    /// Whether blob data was available.
    pub blob_data_available: bool,
}

impl PayloadAttestationData {
    /// Decode the exact 42-byte representation, rejecting noncanonical booleans.
    pub fn from_ssz(bytes: &[u8]) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[Some(32), Some(8), Some(1), Some(1)])?;
        Ok(Self {
            beacon_block_root: read_root(fields[0])?,
            slot: read_u64(fields[1])?,
            payload_present: read_bool(fields[2])?,
            blob_data_available: read_bool(fields[3])?,
        })
    }

    /// Encode the fixed representation.
    pub fn to_ssz(self) -> [u8; 42] {
        let mut bytes = [0; 42];
        bytes[..32].copy_from_slice(&self.beacon_block_root);
        bytes[32..40].copy_from_slice(&self.slot.to_le_bytes());
        bytes[40] = u8::from(self.payload_present);
        bytes[41] = u8::from(self.blob_data_available);
        bytes
    }

    /// Root of the standard four-field container.
    pub fn hash_tree_root(self) -> Root {
        hash_pair(
            hash_pair(self.beacon_block_root, uint64_root(self.slot)),
            hash_pair(
                uint64_root(u64::from(self.payload_present)),
                uint64_root(u64::from(self.blob_data_available)),
            ),
        )
    }
}

/// Aggregated PTC vote with a preset-sized fixed participation bitvector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadAttestation {
    /// PTC participant positions.
    pub aggregation_bits: BitVector,
    /// Signed vote data.
    pub data: PayloadAttestationData,
    /// Aggregate signature.
    pub signature: [u8; 96],
}

impl PayloadAttestation {
    /// Decode with the explicitly selected preset's PTC size.
    pub fn from_ssz(bytes: &[u8], ptc_size: usize) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[Some(ptc_size.div_ceil(8)), Some(42), Some(96)])?;
        Ok(Self {
            aggregation_bits: BitVector::from_ssz(fields[0], ptc_size)?,
            data: PayloadAttestationData::from_ssz(fields[1])?,
            signature: fields[2].try_into().map_err(|_| SszError::InvalidOffset)?,
        })
    }

    /// Encode fixed fields.
    pub fn to_ssz(&self) -> Vec<u8> {
        let mut bytes = self.aggregation_bits.as_ssz().to_vec();
        bytes.extend_from_slice(&self.data.to_ssz());
        bytes.extend_from_slice(&self.signature);
        bytes
    }

    /// Root of the three-field progressive container.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        progressive_container_root(
            &[
                self.aggregation_bits.hash_tree_root()?,
                self.data.hash_tree_root(),
                bytes_root(&self.signature, 96, false)?,
            ],
            &[true; 3],
        )
    }
}

/// An individual PTC network message with validator index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadAttestationMessage {
    /// Voting validator index.
    pub validator_index: u64,
    /// Signed vote data.
    pub data: PayloadAttestationData,
    /// Individual signature.
    pub signature: [u8; 96],
}

impl PayloadAttestationMessage {
    /// Decode the fixed-size network message.
    pub fn from_ssz(bytes: &[u8]) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[Some(8), Some(42), Some(96)])?;
        Ok(Self {
            validator_index: read_u64(fields[0])?,
            data: PayloadAttestationData::from_ssz(fields[1])?,
            signature: fields[2].try_into().map_err(|_| SszError::InvalidOffset)?,
        })
    }

    /// Root of the standard three-field container.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        merkleize(
            &[
                uint64_root(self.validator_index),
                self.data.hash_tree_root(),
                bytes_root(&self.signature, 96, false)?,
            ],
            3,
        )
    }
}

/// Indexed PTC aggregate used for signature and committee validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedPayloadAttestation {
    /// Validator indices; ordering and membership require contextual validation.
    pub attesting_indices: Vec<u64>,
    /// Signed vote data.
    pub data: PayloadAttestationData,
    /// Aggregate signature.
    pub signature: [u8; 96],
}

impl IndexedPayloadAttestation {
    /// Decode the standard bounded list of indices inside the progressive container.
    pub fn from_ssz(bytes: &[u8], ptc_size: usize) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[None, Some(42), Some(96)])?;
        let indices =
            fixed_records::<8>(fields[0], ptc_size)?.into_iter().map(u64::from_le_bytes).collect();
        Ok(Self {
            attesting_indices: indices,
            data: PayloadAttestationData::from_ssz(fields[1])?,
            signature: fields[2].try_into().map_err(|_| SszError::InvalidOffset)?,
        })
    }

    /// Root with the preset-sized bounded index list, rather than a progressive index list.
    pub fn hash_tree_root(&self, ptc_size: usize) -> Result<Root, SszError> {
        check_limit(self.attesting_indices.len(), ptc_size)?;
        let bytes =
            self.attesting_indices.iter().flat_map(|index| index.to_le_bytes()).collect::<Vec<_>>();
        let indices = mix_in_length(
            merkleize(&pack_bytes(&bytes), ptc_size.div_ceil(4))?,
            self.attesting_indices.len() as u64,
        );
        progressive_container_root(
            &[indices, self.data.hash_tree_root(), bytes_root(&self.signature, 96, false)?],
            &[true; 3],
        )
    }
}

const fn check_limit(actual: usize, limit: usize) -> Result<(), SszError> {
    if actual > limit {
        Err(SszError::LimitExceeded { actual, limit })
    } else {
        Ok(())
    }
}

fn fixed_records<const SIZE: usize>(
    bytes: &[u8],
    limit: usize,
) -> Result<Vec<[u8; SIZE]>, SszError> {
    if SIZE == 0 || !bytes.len().is_multiple_of(SIZE) {
        return Err(SszError::InvalidLength { expected: SIZE, actual: bytes.len() });
    }
    check_limit(bytes.len() / SIZE, limit)?;
    bytes
        .chunks_exact(SIZE)
        .map(|record| record.try_into().map_err(|_| SszError::InvalidOffset))
        .collect()
}

fn encode_container(fixed: &[Option<&[u8]>], variable: &[Vec<u8>]) -> Result<Vec<u8>, SszError> {
    if fixed.iter().filter(|field| field.is_none()).count() != variable.len() {
        return Err(SszError::InvalidOffset);
    }
    let fixed_size = fixed.iter().try_fold(0usize, |size, field| {
        size.checked_add(field.map_or(4, <[u8]>::len)).ok_or(SszError::Overflow)
    })?;
    let total = variable.iter().try_fold(fixed_size, |size, field| {
        size.checked_add(field.len()).ok_or(SszError::Overflow)
    })?;
    u32::try_from(total).map_err(|_| SszError::Overflow)?;
    let mut output = Vec::with_capacity(total);
    let mut offset = fixed_size;
    let mut index = 0;
    for field in fixed {
        if let Some(field) = field {
            output.extend_from_slice(field);
        } else {
            output.extend_from_slice(&(offset as u32).to_le_bytes());
            offset += variable[index].len();
            index += 1;
        }
    }
    for field in variable {
        output.extend_from_slice(field);
    }
    Ok(output)
}

/// Caller-supplied resource bounds for untrusted Gloas SSZ messages.
///
/// These bounds do not replace contextual fork-specific operation limits.
#[derive(Clone, Copy, Debug)]
pub struct DecodeLimits {
    /// Maximum encoded bytes for a payload, requests object, or signed envelope.
    pub max_bytes: usize,
    /// Maximum transaction count.
    pub max_transactions: usize,
    /// Maximum bytes in one transaction.
    pub max_transaction_bytes: usize,
    /// Maximum withdrawals in a payload.
    pub max_withdrawals: usize,
    /// Maximum records in each execution request kind.
    pub max_requests_per_kind: usize,
    /// Maximum encoded block access list bytes.
    pub max_access_list_bytes: usize,
}

/// Gloas nineteen-field progressive execution payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPayload {
    /// Parent execution block hash.
    pub parent_hash: Root,
    /// Execution fee recipient.
    pub fee_recipient: [u8; 20],
    /// Execution post-state root.
    pub state_root: Root,
    /// Execution receipts root.
    pub receipts_root: Root,
    /// Execution logs bloom.
    pub logs_bloom: [u8; 256],
    /// Epoch RANDAO mix.
    pub prev_randao: Root,
    /// Execution block height.
    pub block_number: u64,
    /// Execution gas limit.
    pub gas_limit: u64,
    /// Execution gas consumed.
    pub gas_used: u64,
    /// Execution timestamp.
    pub timestamp: u64,
    /// Standard bounded SSZ `ExtraData` byte list.
    pub extra_data: Vec<u8>,
    /// Uint256 base fee, encoded little endian.
    pub base_fee_per_gas: Root,
    /// Execution block hash.
    pub block_hash: Root,
    /// Progressive list of progressive byte-list transactions.
    pub transactions: Vec<Vec<u8>>,
    /// Progressive list of standard 44-byte Withdrawal records.
    pub withdrawals: Vec<[u8; 44]>,
    /// Blob gas consumed.
    pub blob_gas_used: u64,
    /// Excess blob gas.
    pub excess_blob_gas: u64,
    /// Progressive byte list containing the encoded block access list.
    pub block_access_list: Vec<u8>,
    /// Consensus slot committed by the execution block.
    pub slot_number: u64,
}

impl ExecutionPayload {
    /// Decode canonical SSZ with explicit byte and collection bounds.
    pub fn from_ssz(bytes: &[u8], limits: DecodeLimits) -> Result<Self, SszError> {
        check_limit(bytes.len(), limits.max_bytes)?;
        let fields = container_fields(
            bytes,
            &[
                Some(32),
                Some(20),
                Some(32),
                Some(32),
                Some(256),
                Some(32),
                Some(8),
                Some(8),
                Some(8),
                Some(8),
                None,
                Some(32),
                Some(32),
                None,
                None,
                Some(8),
                Some(8),
                None,
                Some(8),
            ],
        )?;
        check_limit(fields[10].len(), 32)?;
        check_limit(fields[17].len(), limits.max_access_list_bytes)?;
        let transactions = variable_list_elements(fields[13], limits.max_transactions)?
            .into_iter()
            .map(|transaction| {
                check_limit(transaction.len(), limits.max_transaction_bytes)?;
                Ok(transaction.to_vec())
            })
            .collect::<Result<Vec<_>, SszError>>()?;
        Ok(Self {
            parent_hash: read_root(fields[0])?,
            fee_recipient: fields[1].try_into().map_err(|_| SszError::InvalidOffset)?,
            state_root: read_root(fields[2])?,
            receipts_root: read_root(fields[3])?,
            logs_bloom: fields[4].try_into().map_err(|_| SszError::InvalidOffset)?,
            prev_randao: read_root(fields[5])?,
            block_number: read_u64(fields[6])?,
            gas_limit: read_u64(fields[7])?,
            gas_used: read_u64(fields[8])?,
            timestamp: read_u64(fields[9])?,
            extra_data: fields[10].to_vec(),
            base_fee_per_gas: read_root(fields[11])?,
            block_hash: read_root(fields[12])?,
            transactions,
            withdrawals: fixed_records::<44>(fields[14], limits.max_withdrawals)?,
            blob_gas_used: read_u64(fields[15])?,
            excess_blob_gas: read_u64(fields[16])?,
            block_access_list: fields[17].to_vec(),
            slot_number: read_u64(fields[18])?,
        })
    }

    /// Encode canonical SSZ after validating collection resource bounds.
    pub fn to_ssz(&self, limits: DecodeLimits) -> Result<Vec<u8>, SszError> {
        self.validate_limits(limits)?;
        let scalars = [
            self.block_number,
            self.gas_limit,
            self.gas_used,
            self.timestamp,
            self.blob_gas_used,
            self.excess_blob_gas,
            self.slot_number,
        ]
        .map(u64::to_le_bytes);
        let transactions = encode_variable_list(&self.transactions)?;
        let withdrawals =
            self.withdrawals.iter().flat_map(|record| record.iter().copied()).collect::<Vec<_>>();
        let bytes = encode_container(
            &[
                Some(&self.parent_hash),
                Some(&self.fee_recipient),
                Some(&self.state_root),
                Some(&self.receipts_root),
                Some(&self.logs_bloom),
                Some(&self.prev_randao),
                Some(&scalars[0]),
                Some(&scalars[1]),
                Some(&scalars[2]),
                Some(&scalars[3]),
                None,
                Some(&self.base_fee_per_gas),
                Some(&self.block_hash),
                None,
                None,
                Some(&scalars[4]),
                Some(&scalars[5]),
                None,
                Some(&scalars[6]),
            ],
            &[self.extra_data.clone(), transactions, withdrawals, self.block_access_list.clone()],
        )?;
        check_limit(bytes.len(), limits.max_bytes)?;
        Ok(bytes)
    }

    /// Hash all nineteen active fields using the pinned progressive schemas.
    pub fn hash_tree_root(&self, limits: DecodeLimits) -> Result<Root, SszError> {
        self.validate_limits(limits)?;
        let transactions = self
            .transactions
            .iter()
            .map(|transaction| progressive_bytes_root(transaction))
            .collect::<Result<Vec<_>, _>>()?;
        let withdrawals = self
            .withdrawals
            .iter()
            .map(|record| fixed_record_root(record, &[8, 8, 20, 8]))
            .collect::<Result<Vec<_>, _>>()?;
        progressive_container_root(
            &[
                self.parent_hash,
                bytes_root(&self.fee_recipient, 20, false)?,
                self.state_root,
                self.receipts_root,
                bytes_root(&self.logs_bloom, 256, false)?,
                self.prev_randao,
                uint64_root(self.block_number),
                uint64_root(self.gas_limit),
                uint64_root(self.gas_used),
                uint64_root(self.timestamp),
                bytes_root(&self.extra_data, 32, true)?,
                self.base_fee_per_gas,
                self.block_hash,
                mix_in_length(merkleize_progressive(&transactions)?, transactions.len() as u64),
                mix_in_length(merkleize_progressive(&withdrawals)?, withdrawals.len() as u64),
                uint64_root(self.blob_gas_used),
                uint64_root(self.excess_blob_gas),
                progressive_bytes_root(&self.block_access_list)?,
                uint64_root(self.slot_number),
            ],
            &[true; 19],
        )
    }

    fn validate_limits(&self, limits: DecodeLimits) -> Result<(), SszError> {
        check_limit(self.extra_data.len(), 32)?;
        check_limit(self.transactions.len(), limits.max_transactions)?;
        check_limit(self.withdrawals.len(), limits.max_withdrawals)?;
        check_limit(self.block_access_list.len(), limits.max_access_list_bytes)?;
        let mut size = 540usize.checked_add(self.extra_data.len()).ok_or(SszError::Overflow)?;
        size = size
            .checked_add(self.transactions.len().checked_mul(4).ok_or(SszError::Overflow)?)
            .ok_or(SszError::Overflow)?;
        for transaction in &self.transactions {
            check_limit(transaction.len(), limits.max_transaction_bytes)?;
            size = size.checked_add(transaction.len()).ok_or(SszError::Overflow)?;
        }
        size = size
            .checked_add(self.withdrawals.len().checked_mul(44).ok_or(SszError::Overflow)?)
            .and_then(|size| size.checked_add(self.block_access_list.len()))
            .ok_or(SszError::Overflow)?;
        check_limit(size, limits.max_bytes)
    }
}

/// Five-kind progressive execution request container.
///
/// Record arrays preserve the exact standard SSZ element encoding. Signature and
/// request semantics are validated by the state transition, rather than decoding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecutionRequests {
    /// `DepositRequest` records: pubkey, credentials, amount, signature, index.
    pub deposits: Vec<[u8; 192]>,
    /// `WithdrawalRequest` records: source address, validator pubkey, amount.
    pub withdrawals: Vec<[u8; 76]>,
    /// `ConsolidationRequest` records: source address, source pubkey, target pubkey.
    pub consolidations: Vec<[u8; 116]>,
    /// `BuilderDepositRequest` records: pubkey, credentials, amount, signature.
    pub builder_deposits: Vec<[u8; 184]>,
    /// `BuilderExitRequest` records: source address and builder pubkey.
    pub builder_exits: Vec<[u8; 68]>,
}

impl ExecutionRequests {
    /// Decode the five progressive lists with canonical container offsets.
    pub fn from_ssz(bytes: &[u8], limits: DecodeLimits) -> Result<Self, SszError> {
        check_limit(bytes.len(), limits.max_bytes)?;
        let fields = container_fields(bytes, &[None; 5])?;
        let max = limits.max_requests_per_kind;
        Ok(Self {
            deposits: fixed_records::<192>(fields[0], max)?,
            withdrawals: fixed_records::<76>(fields[1], max)?,
            consolidations: fixed_records::<116>(fields[2], max)?,
            builder_deposits: fixed_records::<184>(fields[3], max)?,
            builder_exits: fixed_records::<68>(fields[4], max)?,
        })
    }

    /// Encode the request container.
    pub fn to_ssz(&self, limits: DecodeLimits) -> Result<Vec<u8>, SszError> {
        self.validate_limits(limits)?;
        let flatten = |records: &[&[u8]]| {
            records.iter().flat_map(|record| record.iter().copied()).collect::<Vec<_>>()
        };
        let variables = [
            flatten(&self.deposits.iter().map(|record| record.as_slice()).collect::<Vec<_>>()),
            flatten(&self.withdrawals.iter().map(|record| record.as_slice()).collect::<Vec<_>>()),
            flatten(
                &self.consolidations.iter().map(|record| record.as_slice()).collect::<Vec<_>>(),
            ),
            flatten(
                &self.builder_deposits.iter().map(|record| record.as_slice()).collect::<Vec<_>>(),
            ),
            flatten(&self.builder_exits.iter().map(|record| record.as_slice()).collect::<Vec<_>>()),
        ];
        encode_container(&[None; 5], &variables)
    }

    /// Hash each progressive list of standard request containers.
    pub fn hash_tree_root(&self, limits: DecodeLimits) -> Result<Root, SszError> {
        self.validate_limits(limits)?;
        progressive_container_root(
            &[
                progressive_records_root(&self.deposits, &[48, 32, 8, 96, 8])?,
                progressive_records_root(&self.withdrawals, &[20, 48, 8])?,
                progressive_records_root(&self.consolidations, &[20, 48, 48])?,
                progressive_records_root(&self.builder_deposits, &[48, 32, 8, 96])?,
                progressive_records_root(&self.builder_exits, &[20, 48])?,
            ],
            &[true; 5],
        )
    }

    fn validate_limits(&self, limits: DecodeLimits) -> Result<(), SszError> {
        let kinds = [
            (self.deposits.len(), 192usize),
            (self.withdrawals.len(), 76),
            (self.consolidations.len(), 116),
            (self.builder_deposits.len(), 184),
            (self.builder_exits.len(), 68),
        ];
        let mut size = 20usize;
        for (count, record_size) in kinds {
            check_limit(count, limits.max_requests_per_kind)?;
            size = size
                .checked_add(count.checked_mul(record_size).ok_or(SszError::Overflow)?)
                .ok_or(SszError::Overflow)?;
        }
        check_limit(size, limits.max_bytes)
    }
}

/// Five-field progressive execution payload envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPayloadEnvelope {
    /// Offered execution payload.
    pub payload: ExecutionPayload,
    /// Execution requests committed by the corresponding bid.
    pub execution_requests: ExecutionRequests,
    /// Builder registry index or self-build sentinel.
    pub builder_index: u64,
    /// Corresponding beacon block root.
    pub beacon_block_root: Root,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Root,
}

impl ExecutionPayloadEnvelope {
    /// Decode the envelope and both complete component schemas.
    pub fn from_ssz(bytes: &[u8], limits: DecodeLimits) -> Result<Self, SszError> {
        check_limit(bytes.len(), limits.max_bytes)?;
        let fields = container_fields(bytes, &[None, None, Some(8), Some(32), Some(32)])?;
        Ok(Self {
            payload: ExecutionPayload::from_ssz(fields[0], limits)?,
            execution_requests: ExecutionRequests::from_ssz(fields[1], limits)?,
            builder_index: read_u64(fields[2])?,
            beacon_block_root: read_root(fields[3])?,
            parent_beacon_block_root: read_root(fields[4])?,
        })
    }

    /// Encode both nested components with canonical offsets.
    pub fn to_ssz(&self, limits: DecodeLimits) -> Result<Vec<u8>, SszError> {
        let builder = self.builder_index.to_le_bytes();
        let bytes = encode_container(
            &[
                None,
                None,
                Some(&builder),
                Some(&self.beacon_block_root),
                Some(&self.parent_beacon_block_root),
            ],
            &[self.payload.to_ssz(limits)?, self.execution_requests.to_ssz(limits)?],
        )?;
        check_limit(bytes.len(), limits.max_bytes)?;
        Ok(bytes)
    }

    /// Root of all five active fields.
    pub fn hash_tree_root(&self, limits: DecodeLimits) -> Result<Root, SszError> {
        progressive_container_root(
            &[
                self.payload.hash_tree_root(limits)?,
                self.execution_requests.hash_tree_root(limits)?,
                uint64_root(self.builder_index),
                self.beacon_block_root,
                self.parent_beacon_block_root,
            ],
            &[true; 5],
        )
    }
}

/// Standard signed wrapper around the progressive payload envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedExecutionPayloadEnvelope {
    /// Builder's envelope.
    pub message: ExecutionPayloadEnvelope,
    /// Builder or self-building validator signature.
    pub signature: [u8; 96],
}

impl SignedExecutionPayloadEnvelope {
    /// Decode the signed wrapper and its complete nested schemas.
    pub fn from_ssz(bytes: &[u8], limits: DecodeLimits) -> Result<Self, SszError> {
        check_limit(bytes.len(), limits.max_bytes)?;
        let fields = container_fields(bytes, &[None, Some(96)])?;
        Ok(Self {
            message: ExecutionPayloadEnvelope::from_ssz(fields[0], limits)?,
            signature: fields[1].try_into().map_err(|_| SszError::InvalidOffset)?,
        })
    }

    /// Encode the standard signed wrapper.
    pub fn to_ssz(&self, limits: DecodeLimits) -> Result<Vec<u8>, SszError> {
        let bytes =
            encode_container(&[None, Some(&self.signature)], &[self.message.to_ssz(limits)?])?;
        check_limit(bytes.len(), limits.max_bytes)?;
        Ok(bytes)
    }

    /// Root of the signed envelope.
    pub fn hash_tree_root(&self, limits: DecodeLimits) -> Result<Root, SszError> {
        Ok(hash_pair(self.message.hash_tree_root(limits)?, bytes_root(&self.signature, 96, false)?))
    }
}

fn progressive_bytes_root(bytes: &[u8]) -> Result<Root, SszError> {
    Ok(mix_in_length(merkleize_progressive(&pack_bytes(bytes))?, bytes.len() as u64))
}

fn fixed_record_root(bytes: &[u8], sizes: &[usize]) -> Result<Root, SszError> {
    let schema = sizes.iter().copied().map(Some).collect::<Vec<_>>();
    let fields = container_fields(bytes, &schema)?;
    let roots = fields
        .into_iter()
        .zip(sizes)
        .map(|(field, size)| bytes_root(field, *size, false))
        .collect::<Result<Vec<_>, _>>()?;
    merkleize(&roots, roots.len())
}

fn progressive_records_root<const SIZE: usize>(
    records: &[[u8; SIZE]],
    sizes: &[usize],
) -> Result<Root, SszError> {
    let roots = records
        .iter()
        .map(|record| fixed_record_root(record, sizes))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(mix_in_length(merkleize_progressive(&roots)?, roots.len() as u64))
}

fn variable_list_elements(bytes: &[u8], max_elements: usize) -> Result<Vec<&[u8]>, SszError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() < 4 {
        return Err(SszError::InvalidOffset);
    }
    let first =
        u32::from_le_bytes(bytes[..4].try_into().map_err(|_| SszError::InvalidOffset)?) as usize;
    if first < 4 || first > bytes.len() || !first.is_multiple_of(4) {
        return Err(SszError::InvalidOffset);
    }
    let count = first / 4;
    check_limit(count, max_elements)?;
    let offsets = bytes[..first]
        .chunks_exact(4)
        .map(|offset| u32::from_le_bytes(offset.try_into().expect("four-byte offset")) as usize)
        .collect::<Vec<_>>();
    let mut elements = Vec::with_capacity(count);
    for (index, start) in offsets.iter().copied().enumerate() {
        let end = offsets.get(index + 1).copied().unwrap_or(bytes.len());
        if start < first || start > end || end > bytes.len() {
            return Err(SszError::InvalidOffset);
        }
        elements.push(&bytes[start..end]);
    }
    Ok(elements)
}

fn encode_variable_list(elements: &[Vec<u8>]) -> Result<Vec<u8>, SszError> {
    if elements.is_empty() {
        return Ok(Vec::new());
    }
    let fixed = vec![None; elements.len()];
    encode_container(&fixed, elements)
}

#[cfg(test)]
mod envelope_tests {
    use super::*;

    fn limits() -> DecodeLimits {
        DecodeLimits {
            max_bytes: 1024 * 1024,
            max_transactions: 1000,
            max_transaction_bytes: 1024,
            max_withdrawals: 32,
            max_requests_per_kind: 32,
            max_access_list_bytes: 1024,
        }
    }

    fn payload() -> ExecutionPayload {
        ExecutionPayload {
            parent_hash: [1; 32],
            fee_recipient: [2; 20],
            state_root: [3; 32],
            receipts_root: [4; 32],
            logs_bloom: [0; 256],
            prev_randao: [5; 32],
            block_number: 7,
            gas_limit: 60_000_000,
            gas_used: 21000,
            timestamp: 1742213412,
            extra_data: vec![1, 2, 3],
            base_fee_per_gas: uint64_root(42),
            block_hash: [6; 32],
            transactions: vec![vec![0x02, 1], vec![0x04, 2, 3]],
            withdrawals: vec![[0; 44]],
            blob_gas_used: 0,
            excess_blob_gas: 0,
            block_access_list: vec![4, 5],
            slot_number: 1,
        }
    }

    #[test]
    fn nested_payload_and_requests_round_trip() {
        let limits = limits();
        let payload = payload();
        assert_eq!(
            ExecutionPayload::from_ssz(&payload.to_ssz(limits).unwrap(), limits).unwrap(),
            payload
        );
        let requests = ExecutionRequests {
            deposits: vec![[1; 192]],
            withdrawals: vec![[2; 76]],
            consolidations: vec![[3; 116]],
            builder_deposits: vec![[4; 184]],
            builder_exits: vec![[5; 68]],
        };
        assert_eq!(
            ExecutionRequests::from_ssz(&requests.to_ssz(limits).unwrap(), limits).unwrap(),
            requests
        );
        let envelope = SignedExecutionPayloadEnvelope {
            message: ExecutionPayloadEnvelope {
                payload,
                execution_requests: requests,
                builder_index: 9,
                beacon_block_root: [7; 32],
                parent_beacon_block_root: [8; 32],
            },
            signature: [9; 96],
        };
        assert_eq!(
            SignedExecutionPayloadEnvelope::from_ssz(&envelope.to_ssz(limits).unwrap(), limits)
                .unwrap(),
            envelope
        );
        let mut changed = envelope.clone();
        changed.message.payload.slot_number += 1;
        assert_ne!(
            envelope.hash_tree_root(limits).unwrap(),
            changed.hash_tree_root(limits).unwrap()
        );
    }

    #[test]
    fn nested_offsets_and_resource_limits_fail_closed() {
        assert!(variable_list_elements(&[0; 4], 100).is_err());
        assert!(variable_list_elements(&[5, 0, 0, 0, 1], 100).is_err());
        let mut payload = payload();
        let mut small = limits();
        small.max_transactions = 1;
        assert!(ExecutionPayload::from_ssz(&payload.to_ssz(limits()).unwrap(), small).is_err());
        payload.extra_data.resize(33, 0);
        assert!(payload.hash_tree_root(limits()).is_err());
        let mut requests = ExecutionRequests::default().to_ssz(limits()).unwrap();
        requests[4..8].copy_from_slice(&19u32.to_le_bytes());
        assert_eq!(ExecutionRequests::from_ssz(&requests, limits()), Err(SszError::InvalidOffset));
    }

    #[test]
    fn roots_match_generated_pinned_reference_ssz_fixtures() {
        // Generated using eth-ssz-specs 0.1.0 and the pinned Gloas declarations.
        // These structural fixtures are distinct from published transition vectors.
        macro_rules! root_case {
            ($name:literal, $kind:ty, [$($decode:expr),*], [$($hash:expr),*]) => {{
                let bytes = include_bytes!(concat!("../testdata/generated-gloas/", $name, ".ssz"));
                let value = <$kind>::from_ssz(bytes, $($decode),*).unwrap();
                let expected = *include_bytes!(concat!("../testdata/generated-gloas/", $name, ".root"));
                assert_eq!(value.hash_tree_root($($hash),*).unwrap(), expected, $name);
            }};
        }
        root_case!("bid", ExecutionPayloadBid, [4096], [4096]);
        root_case!("signed-bid", SignedExecutionPayloadBid, [4096], [4096]);
        root_case!("ptc-vote", PayloadAttestation, [512], []);
        root_case!("ptc-vote-minimal", PayloadAttestation, [16], []);
        root_case!("indexed-ptc", IndexedPayloadAttestation, [512], [512]);
        root_case!("indexed-ptc-minimal", IndexedPayloadAttestation, [16], [16]);
        root_case!("payload", ExecutionPayload, [limits()], [limits()]);
        root_case!("requests", ExecutionRequests, [limits()], [limits()]);
        root_case!("envelope", ExecutionPayloadEnvelope, [limits()], [limits()]);
        root_case!("signed-envelope", SignedExecutionPayloadEnvelope, [limits()], [limits()]);
        let data = PayloadAttestationData::from_ssz(include_bytes!(
            "../testdata/generated-gloas/ptc-data.ssz"
        ))
        .unwrap();
        assert_eq!(
            data.hash_tree_root(),
            *include_bytes!("../testdata/generated-gloas/ptc-data.root")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bid() -> ExecutionPayloadBid {
        ExecutionPayloadBid {
            parent_block_hash: [1; 32],
            parent_block_root: [2; 32],
            block_hash: [3; 32],
            prev_randao: [4; 32],
            fee_recipient: [5; 20],
            gas_limit: 60_000_000,
            builder_index: 7,
            slot: 42,
            value: 8,
            execution_payment: 9,
            blob_kzg_commitments: vec![[6; 48]],
            execution_requests_root: [10; 32],
        }
    }

    #[test]
    fn bid_offsets_limits_and_signed_root() {
        let bid = bid();
        let bytes = bid.to_ssz().unwrap();
        assert_eq!(bytes.len(), 272);
        assert_eq!(ExecutionPayloadBid::from_ssz(&bytes, 1).unwrap(), bid);
        assert!(ExecutionPayloadBid::from_ssz(&bytes, 0).is_err());
        let mut invalid = bytes;
        invalid[188..192].copy_from_slice(&225u32.to_le_bytes());
        assert!(ExecutionPayloadBid::from_ssz(&invalid, 1).is_err());
        let signed = SignedExecutionPayloadBid { message: bid, signature: [11; 96] };
        assert_eq!(
            SignedExecutionPayloadBid::from_ssz(&signed.to_ssz().unwrap(), 1).unwrap(),
            signed
        );
        assert_ne!(signed.hash_tree_root(1).unwrap(), signed.message.hash_tree_root(1).unwrap());
    }

    #[test]
    fn ptc_rejects_noncanonical_booleans_and_wrong_bitvector() {
        let data = PayloadAttestationData {
            beacon_block_root: [1; 32],
            slot: 9,
            payload_present: true,
            blob_data_available: false,
        };
        assert_eq!(PayloadAttestationData::from_ssz(&data.to_ssz()).unwrap(), data);
        let mut bad = data.to_ssz();
        bad[40] = 2;
        assert_eq!(PayloadAttestationData::from_ssz(&bad), Err(SszError::InvalidBoolean));
        let vote = PayloadAttestation {
            aggregation_bits: BitVector::from_ssz(&[0x01; 64], 512).unwrap(),
            data,
            signature: [2; 96],
        };
        assert_eq!(PayloadAttestation::from_ssz(&vote.to_ssz(), 512).unwrap(), vote);
        assert!(PayloadAttestation::from_ssz(&vote.to_ssz(), 64).is_err());
    }
}
