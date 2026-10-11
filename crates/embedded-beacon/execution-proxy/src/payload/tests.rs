use super::*;
use alloy_primitives::b256;

const PRESET: Preset = Preset::Minimal;

fn zero_fixed<T: Ssz>() -> T {
    T::from_ssz(&vec![0; T::fixed_size(PRESET).unwrap()], PRESET).unwrap()
}

macro_rules! test_payload {
    ($name:ident, $module:ident, $($field:ident: $value:expr),* $(,)?) => {
        fn $name() -> $module::ExecutionPayload {
            let mut base_fee = [0; 32];
            base_fee[0] = 3;
            base_fee[31] = 0x80;
            $module::ExecutionPayload {
                parent_hash: [1; 32], fee_recipient: [2; 20], state_root: [3; 32],
                receipts_root: [4; 32], logs_bloom: $module::LogsBloom::from_vec(vec![5; 256]),
                prev_randao: [6; 32], block_number: u64::MAX - 1, gas_limit: 12345,
                gas_used: 678, timestamp: 98765, extra_data: $module::ExtraData::from_vec(vec![9; 32]),
                base_fee_per_gas: base_fee, block_hash: [10; 32],
                // Opaque bytes deliberately include a malformed transaction and an empty item.
                transactions: $module::Transactions::from_vec(vec![
                    $module::Transaction::from_vec(vec![0xff, 0x01, 0x80]),
                    $module::Transaction::from_vec(Vec::new()),
                    $module::Transaction::from_vec(vec![0x03, 0x00]),
                ]),
                $($field: $value,)*
            }
        }
    };
}

macro_rules! withdrawals {
    ($module:ident) => {
        $module::Withdrawals::from_vec(vec![
            $module::Withdrawal {
                index: u64::MAX,
                validator_index: 32,
                address: [11; 20],
                amount: 100,
            },
            $module::Withdrawal {
                index: 7,
                validator_index: u64::MAX - 1,
                address: [12; 20],
                amount: u64::MAX,
            },
        ])
    };
}

test_payload!(bellatrix_payload, bellatrix,);
test_payload!(capella_payload, capella, withdrawals: withdrawals!(capella));
test_payload!(deneb_payload, deneb, withdrawals: withdrawals!(deneb), blob_gas_used: u64::MAX, excess_blob_gas: 999);
test_payload!(electra_payload, electra, withdrawals: withdrawals!(electra), blob_gas_used: u64::MAX, excess_blob_gas: 999);
test_payload!(fulu_payload, fulu, withdrawals: withdrawals!(fulu), blob_gas_used: u64::MAX, excess_blob_gas: 999);
test_payload!(gloas_payload, gloas, withdrawals: withdrawals!(gloas), blob_gas_used: u64::MAX, excess_blob_gas: 999,
    block_access_list: gloas::BlockAccessList::from_vec(vec![0xff, 0x00, 0x42]), slot_number: 42);

macro_rules! test_block {
    ($name:ident, $module:ident, $payload:ident, $($field:ident: $value:expr),* $(,)?) => {
        fn $name() -> $module::BeaconBlock {
            $module::BeaconBlock {
                slot: 42, proposer_index: 3, parent_root: [21; 32], state_root: [22; 32],
                body: $module::BeaconBlockBody {
                    randao_reveal: [0; 96], eth1_data: zero_fixed(), graffiti: [0; 32],
                    proposer_slashings: Default::default(), attester_slashings: Default::default(),
                    attestations: Default::default(), deposits: Default::default(), voluntary_exits: Default::default(),
                    sync_aggregate: zero_fixed(), execution_payload: $payload(),
                    $($field: $value,)*
                },
            }
        }
    };
}

macro_rules! empty_requests {
    ($module:ident) => {
        $module::ExecutionRequests {
            deposits: Default::default(),
            withdrawals: Default::default(),
            consolidations: Default::default(),
        }
    };
}

test_block!(bellatrix_block, bellatrix, bellatrix_payload,);
test_block!(capella_block, capella, capella_payload, bls_to_execution_changes: Default::default());
test_block!(deneb_block, deneb, deneb_payload, bls_to_execution_changes: Default::default(),
    blob_kzg_commitments: deneb::BlobKZGCommitments::from_vec(vec![[0; 48], [1; 48]]));
test_block!(electra_block, electra, electra_payload, bls_to_execution_changes: Default::default(),
    blob_kzg_commitments: electra::BlobKZGCommitments::from_vec(vec![[0; 48], [1; 48]]), execution_requests: empty_requests!(electra));
test_block!(fulu_block, fulu, fulu_payload, bls_to_execution_changes: Default::default(),
    blob_kzg_commitments: fulu::BlobKZGCommitments::from_vec(vec![[0; 48], [1; 48]]), execution_requests: empty_requests!(fulu));

fn check_common(payload: &ExecutionPayloadV1) {
    assert_eq!(payload.parent_hash, B256::repeat_byte(1));
    assert_eq!(payload.fee_recipient, Address::repeat_byte(2));
    assert_eq!(payload.state_root, B256::repeat_byte(3));
    assert_eq!(payload.receipts_root, B256::repeat_byte(4));
    assert_eq!(payload.logs_bloom.as_slice(), &[5; 256]);
    assert_eq!(payload.prev_randao, B256::repeat_byte(6));
    assert_eq!(payload.block_number, u64::MAX - 1);
    assert_eq!(payload.gas_limit, 12345);
    assert_eq!(payload.gas_used, 678);
    assert_eq!(payload.timestamp, 98765);
    assert_eq!(payload.extra_data.as_ref(), &[9; 32]);
    assert_eq!(payload.base_fee_per_gas, (U256::from(1) << 255) + U256::from(3));
    assert_eq!(payload.block_hash, B256::repeat_byte(10));
    assert_eq!(
        payload.transactions.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
        vec![&[0xff, 0x01, 0x80][..], &[][..], &[0x03, 0x00][..]]
    );
}

fn check_withdrawals(payload: &ExecutionPayloadV2) {
    assert_eq!(
        payload.withdrawals,
        vec![
            Withdrawal {
                index: u64::MAX,
                validator_index: 32,
                address: Address::repeat_byte(11),
                amount: 100
            },
            Withdrawal {
                index: 7,
                validator_index: u64::MAX - 1,
                address: Address::repeat_byte(12),
                amount: u64::MAX
            },
        ]
    );
}

fn check_cancun(sidecar: &ExecutionPayloadSidecar) {
    assert_eq!(sidecar.parent_beacon_block_root(), Some(B256::repeat_byte(21)));
    // Independently calculated SHA-256 digests for 48 zero / one bytes, with version byte 0x01.
    assert_eq!(
        sidecar.versioned_hashes().unwrap(),
        &vec![
            b256!("01b0761f87b081d5cf10757ccc89f12be355c70e2e29df288b65b30710dcbcd1"),
            b256!("01f90435cd45d0475cbb0ace7e7e3a672ed2a3a4984ad3e8624d8878725a9c2e"),
        ]
    );
}

#[test]
fn all_embedded_payload_forks_preserve_fields_and_engine_versions() {
    let blocks = [
        BeaconBlock::Bellatrix(bellatrix_block(), PRESET),
        BeaconBlock::Capella(capella_block(), PRESET),
        BeaconBlock::Deneb(deneb_block(), PRESET),
        BeaconBlock::Electra(electra_block(), PRESET),
        BeaconBlock::Fulu(fulu_block(), PRESET),
    ];
    for (index, block) in blocks.iter().enumerate() {
        let engine = from_beacon_block(block).unwrap();
        check_common(engine.payload.as_v1());
        match index {
            0 => assert!(matches!(engine.payload, ExecutionPayload::V1(_))),
            1 => assert!(matches!(engine.payload, ExecutionPayload::V2(_))),
            _ => assert!(matches!(engine.payload, ExecutionPayload::V3(_))),
        }
        if index > 0 {
            check_withdrawals(engine.payload.as_v2().unwrap());
        }
        if index > 1 {
            assert_eq!(engine.payload.as_v3().unwrap().blob_gas_used, u64::MAX);
            assert_eq!(engine.payload.as_v3().unwrap().excess_blob_gas, 999);
            check_cancun(&engine.sidecar);
        } else {
            assert!(engine.sidecar.parent_beacon_block_root().is_none());
        }
        if index > 2 {
            assert!(engine.sidecar.requests().unwrap().is_empty());
        } else {
            assert!(engine.sidecar.requests().is_none());
        }
    }
}

macro_rules! filled_requests {
    ($module:ident) => {
        $module::ExecutionRequests {
            deposits: $module::DepositRequests::from_vec(vec![$module::DepositRequest {
                pubkey: [1; 48],
                withdrawal_credentials: [2; 32],
                amount: 0x0102030405060708,
                signature: [3; 96],
                index: 0x1112131415161718,
            }]),
            withdrawals: $module::WithdrawalRequests::from_vec(vec![$module::WithdrawalRequest {
                source_address: [4; 20],
                validator_pubkey: [5; 48],
                amount: 0x2122232425262728,
            }]),
            consolidations: $module::ConsolidationRequests::from_vec(vec![
                $module::ConsolidationRequest {
                    source_address: [6; 20],
                    source_pubkey: [7; 48],
                    target_pubkey: [8; 48],
                },
            ]),
        }
    };
}

fn check_three_requests(requests: &Requests) {
    let groups = requests.iter().collect::<Vec<_>>();
    assert_eq!(groups.len(), 3);
    assert_eq!(
        groups[0].as_ref(),
        [
            vec![0],
            vec![1; 48],
            vec![2; 32],
            vec![8, 7, 6, 5, 4, 3, 2, 1],
            vec![3; 96],
            vec![0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]
        ]
        .concat()
    );
    assert_eq!(
        groups[1].as_ref(),
        [vec![1], vec![4; 20], vec![5; 48], vec![0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21]]
            .concat()
    );
    assert_eq!(groups[2].as_ref(), [vec![2], vec![6; 20], vec![7; 48], vec![8; 48]].concat());
}

#[test]
fn electra_and_fulu_requests_are_nonempty_ordered_ssz_groups() {
    let mut electra = electra_block();
    electra.body.execution_requests = filled_requests!(electra);
    let mut fulu = fulu_block();
    fulu.body.execution_requests = filled_requests!(fulu);
    for block in [BeaconBlock::Electra(electra, PRESET), BeaconBlock::Fulu(fulu, PRESET)] {
        check_three_requests(from_beacon_block(&block).unwrap().sidecar.requests().unwrap());
    }
    let mut value = filled_requests!(electra);
    value.withdrawals.values.clear();
    let groups = electra_requests(&value, PRESET).unwrap();
    assert_eq!(groups.iter().map(|group| group[0]).collect::<Vec<_>>(), vec![0, 2]);
}

fn linked_gloas() -> (gloas::BeaconBlock, gloas::SignedExecutionPayloadEnvelope, GloasPayloadContext)
{
    let payload = gloas_payload();
    let common = filled_requests!(electra);
    let requests = gloas::ExecutionRequests {
        deposits: gloas::DepositRequests::from_ssz(
            &common.deposits.to_ssz(PRESET).unwrap(),
            PRESET,
        )
        .unwrap(),
        withdrawals: gloas::WithdrawalRequests::from_ssz(
            &common.withdrawals.to_ssz(PRESET).unwrap(),
            PRESET,
        )
        .unwrap(),
        consolidations: gloas::ConsolidationRequests::from_ssz(
            &common.consolidations.to_ssz(PRESET).unwrap(),
            PRESET,
        )
        .unwrap(),
        builder_deposits: gloas::BuilderDepositRequests::from_vec(vec![
            gloas::BuilderDepositRequest {
                pubkey: [9; 48],
                withdrawal_credentials: [10; 32],
                amount: 0x3132333435363738,
                signature: [11; 96],
            },
        ]),
        builder_exits: gloas::BuilderExitRequests::from_vec(vec![gloas::BuilderExitRequest {
            source_address: [12; 20],
            pubkey: [13; 48],
        }]),
    };
    let empty = gloas::ExecutionRequests {
        deposits: Default::default(),
        withdrawals: Default::default(),
        consolidations: Default::default(),
        builder_deposits: Default::default(),
        builder_exits: Default::default(),
    };
    let block = gloas::BeaconBlock {
        slot: 42,
        proposer_index: 3,
        parent_root: [21; 32],
        state_root: [22; 32],
        body: gloas::BeaconBlockBody {
            randao_reveal: [0; 96],
            eth1_data: zero_fixed(),
            graffiti: [0; 32],
            proposer_slashings: Default::default(),
            attester_slashings: Default::default(),
            attestations: Default::default(),
            deposits: Default::default(),
            voluntary_exits: Default::default(),
            sync_aggregate: zero_fixed(),
            bls_to_execution_changes: Default::default(),
            payload_attestations: Default::default(),
            parent_execution_requests: empty,
            signed_execution_payload_bid: gloas::SignedExecutionPayloadBid {
                message: gloas::ExecutionPayloadBid {
                    parent_block_hash: [1; 32],
                    parent_block_root: [21; 32],
                    block_hash: [10; 32],
                    prev_randao: [6; 32],
                    // Bid payment recipient has a different meaning and is deliberately distinct.
                    fee_recipient: [99; 20],
                    gas_limit: 12345,
                    builder_index: 7,
                    slot: 42,
                    value: 15,
                    execution_payment: 17,
                    blob_kzg_commitments: gloas::BlobKZGCommitments::from_vec(vec![
                        [0; 48], [1; 48],
                    ]),
                    execution_requests_root: requests.hash_tree_root(PRESET).unwrap(),
                },
                signature: [0; 96],
            },
        },
    };
    let context = GloasPayloadContext {
        parent_execution_hash: [1; 32],
        expected_timestamp: 98765,
        expected_withdrawals_root: payload.withdrawals.hash_tree_root(PRESET).unwrap(),
    };
    let envelope = gloas::SignedExecutionPayloadEnvelope {
        message: gloas::ExecutionPayloadEnvelope {
            payload,
            execution_requests: requests,
            builder_index: 7,
            beacon_block_root: block.hash_tree_root(PRESET).unwrap(),
            parent_beacon_block_root: [21; 32],
        },
        signature: [0; 96],
    };
    (block, envelope, context)
}

#[test]
fn gloas_preserves_bal_slot_all_request_groups_and_bid_commitment_order() {
    let (block, envelope, context) = linked_gloas();
    let engine = from_gloas_envelope(&block, &envelope, PRESET, &context).unwrap();
    check_common(engine.payload.as_v1());
    check_withdrawals(engine.payload.as_v2().unwrap());
    check_cancun(&engine.sidecar);
    let v4 = engine.payload.as_v4().unwrap();
    assert_eq!(v4.slot_number, 42);
    assert_eq!(v4.block_access_list.as_ref(), &[0xff, 0x00, 0x42]);
    let groups = engine.sidecar.requests().unwrap().iter().collect::<Vec<_>>();
    assert_eq!(groups.iter().map(|group| group[0]).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
    check_three_requests(&Requests::new(
        groups[..3].iter().map(|group| (*group).clone()).collect(),
    ));
    assert_eq!(
        groups[3].as_ref(),
        [
            vec![3],
            vec![9; 48],
            vec![10; 32],
            vec![0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31],
            vec![11; 96]
        ]
        .concat()
    );
    assert_eq!(groups[4].as_ref(), [vec![4], vec![12; 20], vec![13; 48]].concat());
    assert!(matches!(
        from_beacon_block(&BeaconBlock::Gloas(block, PRESET)),
        Err(PayloadConversionError::EnvelopeRequired)
    ));
}

#[test]
fn every_required_gloas_envelope_link_is_rejected_when_changed() {
    type Mutator = fn(
        &mut gloas::BeaconBlock,
        &mut gloas::SignedExecutionPayloadEnvelope,
        &mut GloasPayloadContext,
    );
    let cases: &[(&str, Mutator)] = &[
        ("beacon block root", |_, e, _| e.message.beacon_block_root[0] ^= 1),
        ("parent beacon root", |_, e, _| e.message.parent_beacon_block_root[0] ^= 1),
        ("builder index", |_, e, _| e.message.builder_index += 1),
        ("randao", |_, e, _| e.message.payload.prev_randao[0] ^= 1),
        ("gas limit", |_, e, _| e.message.payload.gas_limit += 1),
        ("execution hash", |_, e, _| e.message.payload.block_hash[0] ^= 1),
        ("execution requests root", |_, e, _| {
            e.message.execution_requests.builder_exits.values[0].source_address[0] ^= 1
        }),
        ("slot", |_, e, _| e.message.payload.slot_number += 1),
        ("parent execution hash", |_, e, _| e.message.payload.parent_hash[0] ^= 1),
        ("timestamp", |_, e, _| e.message.payload.timestamp += 1),
        ("expected withdrawals", |_, e, _| e.message.payload.withdrawals.values[0].amount += 1),
        ("parent execution hash", |_, _, c| c.parent_execution_hash[0] ^= 1),
        ("timestamp", |_, _, c| c.expected_timestamp += 1),
        ("expected withdrawals", |_, _, c| c.expected_withdrawals_root[0] ^= 1),
    ];
    for (expected, mutate) in cases {
        let (mut block, mut envelope, mut context) = linked_gloas();
        mutate(&mut block, &mut envelope, &mut context);
        match from_gloas_envelope(&block, &envelope, PRESET, &context) {
            Err(PayloadConversionError::Linkage(actual)) => assert_eq!(actual, *expected),
            other => panic!("expected {expected} rejection, got {other:?}"),
        }
    }
}

#[test]
fn committed_bid_parent_and_slot_are_checked_independently_of_block_root() {
    for field in ["bid parent beacon root", "slot", "parent execution hash"] {
        let (mut block, mut envelope, context) = linked_gloas();
        let bid = &mut block.body.signed_execution_payload_bid.message;
        match field {
            "bid parent beacon root" => bid.parent_block_root[0] ^= 1,
            "slot" => bid.slot += 1,
            _ => bid.parent_block_hash[0] ^= 1,
        }
        envelope.message.beacon_block_root = block.hash_tree_root(PRESET).unwrap();
        assert!(matches!(from_gloas_envelope(&block, &envelope, PRESET, &context),
            Err(PayloadConversionError::Linkage(actual)) if actual == field));
    }
}

#[test]
fn malformed_typed_ssz_bounds_are_checked_before_engine_input_exists() {
    let mut bloom = bellatrix_block();
    bloom.body.execution_payload.logs_bloom.values.pop();
    let mut extra = bellatrix_block();
    extra.body.execution_payload.extra_data.values.push(33);
    let mut withdrawals = capella_block();
    withdrawals.body.execution_payload.withdrawals.values = vec![zero_fixed(); 5];
    let mut commitments = deneb_block();
    commitments.body.blob_kzg_commitments.values.resize(4097, [0; 48]);
    let mut requests = electra_block();
    requests.body.execution_requests.consolidations.values = vec![zero_fixed(); 3];
    for block in [
        BeaconBlock::Bellatrix(bloom, PRESET),
        BeaconBlock::Bellatrix(extra, PRESET),
        BeaconBlock::Capella(withdrawals, PRESET),
        BeaconBlock::Deneb(commitments, PRESET),
        BeaconBlock::Electra(requests, PRESET),
    ] {
        assert!(matches!(from_beacon_block(&block), Err(PayloadConversionError::Ssz(_))));
    }
}

#[test]
fn aggregate_payload_budget_rejects_large_copies_and_empty_transaction_metadata() {
    let mut block = bellatrix_block();
    block.body.execution_payload.transactions.values[0].values.resize(MAX_CONVERSION_BYTES, 0);
    assert!(matches!(
        from_beacon_block(&BeaconBlock::Bellatrix(block, PRESET)),
        Err(PayloadConversionError::Oversized)
    ));
    let mut block = bellatrix_block();
    block.body.execution_payload.transactions.values =
        vec![
            bellatrix::Transaction::from_vec(Vec::new());
            MAX_CONVERSION_BYTES / core::mem::size_of::<Bytes>()
        ];
    assert!(matches!(
        from_beacon_block(&BeaconBlock::Bellatrix(block, PRESET)),
        Err(PayloadConversionError::Oversized)
    ));
    let (block, mut envelope, context) = linked_gloas();
    envelope.message.payload.block_access_list.values.resize(MAX_CONVERSION_BYTES - 1024, 0);
    assert!(matches!(
        from_gloas_envelope(&block, &envelope, PRESET, &context),
        Err(PayloadConversionError::Oversized)
    ));
}

#[test]
fn request_order_within_a_group_is_preserved_without_container_offsets() {
    let mut block = electra_block();
    block.body.execution_requests = filled_requests!(electra);
    let mut second = block.body.execution_requests.deposits.values[0].clone();
    second.index = 0x4142434445464748;
    block.body.execution_requests.deposits.values.push(second);
    let engine = from_beacon_block(&BeaconBlock::Electra(block, PRESET)).unwrap();
    let deposits = engine.sidecar.requests().unwrap().iter().next().unwrap();
    assert_eq!(deposits.len(), 1 + 192 * 2);
    assert_eq!(&deposits[185..193], &[0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]);
    assert_eq!(&deposits[377..385], &[0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41]);
}

#[test]
fn late_callback_candidates_preserve_full_payload_and_every_sidecar_group() {
    let electra = electra_block();
    let hashes = versioned_hashes(&electra.body.blob_kzg_commitments.values)
        .into_iter()
        .map(Into::into)
        .collect::<Vec<Root>>();
    let request_groups = electra_requests(&filled_requests!(electra), PRESET)
        .unwrap()
        .iter()
        .map(|group| group.to_vec())
        .collect::<Vec<_>>();
    let inputs = candidate::EngineSidecarInputs {
        parent_beacon_block_root: electra.parent_root,
        versioned_hashes: &hashes,
        execution_requests_list: &request_groups,
    };
    let converted =
        candidate::from_electra_payload(&electra.body.execution_payload, PRESET, inputs).unwrap();
    check_common(converted.payload.as_v1());
    check_withdrawals(converted.payload.as_v2().unwrap());
    check_cancun(&converted.sidecar);
    check_three_requests(converted.sidecar.requests().unwrap());

    let fulu = fulu_block();
    let converted =
        candidate::from_fulu_payload(&fulu.body.execution_payload, PRESET, inputs).unwrap();
    check_common(converted.payload.as_v1());
    check_withdrawals(converted.payload.as_v2().unwrap());
    check_cancun(&converted.sidecar);
    check_three_requests(converted.sidecar.requests().unwrap());

    let (block, envelope, context) = linked_gloas();
    let expected = from_gloas_envelope(&block, &envelope, PRESET, &context).unwrap();
    let requests =
        expected.sidecar.requests().unwrap().iter().map(|group| group.to_vec()).collect::<Vec<_>>();
    let converted = candidate::from_gloas_payload(
        &envelope.message.payload,
        PRESET,
        candidate::EngineSidecarInputs {
            parent_beacon_block_root: envelope.message.parent_beacon_block_root,
            versioned_hashes: &hashes,
            execution_requests_list: &requests,
        },
    )
    .unwrap();
    assert_eq!(converted.payload, expected.payload);
    assert_eq!(
        converted.sidecar.parent_beacon_block_root(),
        expected.sidecar.parent_beacon_block_root()
    );
    assert_eq!(converted.sidecar.versioned_hashes(), expected.sidecar.versioned_hashes());
    assert_eq!(
        converted.sidecar.requests().unwrap().iter().collect::<Vec<_>>(),
        expected.sidecar.requests().unwrap().iter().collect::<Vec<_>>()
    );
    assert!(converted.sidecar.inclusion_list_transactions().is_none());
}

#[test]
fn late_callback_candidates_reject_ambiguous_context_before_engine_dispatch() {
    let payload = electra_payload();
    let valid_requests = electra_requests(&filled_requests!(electra), PRESET)
        .unwrap()
        .iter()
        .map(|group| group.to_vec())
        .collect::<Vec<_>>();
    let malformed = [
        vec![Vec::new()],
        vec![vec![0]],
        vec![vec![5, 0]],
        vec![vec![0, 0]],
        vec![valid_requests[1].clone(), valid_requests[0].clone()],
        vec![valid_requests[0].clone(), valid_requests[0].clone()],
        vec![vec![3; 185]],
    ];
    for requests in &malformed {
        assert!(matches!(
            candidate::from_electra_payload(
                &payload,
                PRESET,
                candidate::EngineSidecarInputs {
                    parent_beacon_block_root: [21; 32],
                    versioned_hashes: &[],
                    execution_requests_list: requests,
                }
            ),
            Err(PayloadConversionError::ContextEncoding)
        ));
    }
    assert!(matches!(
        candidate::from_electra_payload(
            &payload,
            PRESET,
            candidate::EngineSidecarInputs {
                parent_beacon_block_root: [21; 32],
                versioned_hashes: &[[2; 32]],
                execution_requests_list: &[],
            }
        ),
        Err(PayloadConversionError::ContextEncoding)
    ));
    let oversized = vec![vec![0; MAX_CONVERSION_BYTES]];
    assert!(matches!(
        candidate::from_electra_payload(
            &payload,
            PRESET,
            candidate::EngineSidecarInputs {
                parent_beacon_block_root: [21; 32],
                versioned_hashes: &[],
                execution_requests_list: &oversized,
            }
        ),
        Err(PayloadConversionError::Oversized)
    ));
}
