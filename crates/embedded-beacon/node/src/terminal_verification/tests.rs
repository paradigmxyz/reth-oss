//! Complete-header hash and original provider evidence controls, not PoW/live-node proof.

use super::*;
use reth_chainspec::{Chain, ChainSpecBuilder, MAINNET};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule, Preset};
use std::{
    collections::HashMap,
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Provider {
    spec: Arc<ChainSpec>,
    headers: HashMap<B256, Header>,
    difficulties: HashMap<B256, U256>,
    reads: Arc<AtomicUsize>,
    fail: bool,
    replaced_spec: bool,
}
impl TerminalExecutionProvider for Provider {
    type Error = io::Error;
    fn chain_spec(&self) -> Arc<ChainSpec> {
        if self.replaced_spec {
            Arc::new((*self.spec).clone())
        } else {
            self.spec.clone()
        }
    }
    fn header(&self, hash: B256) -> Result<Option<Header>, io::Error> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(io::Error::other("original local header failure"))
        }
        Ok(self.headers.get(&hash).cloned())
    }
    fn total_difficulty(&self, hash: B256) -> Result<Option<U256>, io::Error> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.difficulties.get(&hash).copied())
    }
}
fn config() -> ChainConfig {
    ChainConfig {
        name: "terminal-provider-control".into(),
        preset: Preset::Mainnet,
        chain_id: 321,
        genesis_time: 100,
        genesis_validators_root: [9; 32],
        seconds_per_slot: 6,
        slots_per_epoch: 32,
        forks: ForkSchedule::new(
            [
                Fork::Phase0,
                Fork::Altair,
                Fork::Bellatrix,
                Fork::Capella,
                Fork::Deneb,
                Fork::Electra,
                Fork::Fulu,
                Fork::Gloas,
            ]
            .into_iter()
            .enumerate()
            .map(|(index, fork)| ForkActivation {
                fork,
                epoch: index as u64,
                version: [index as u8, 0, 0, 42],
            })
            .collect(),
        )
        .unwrap(),
        blob_schedule: Vec::new(),
        max_payload_size: 10 * 1024 * 1024,
    }
}
fn spec(threshold: U256, genesis_difficulty: U256) -> Arc<ChainSpec> {
    let mut genesis = MAINNET.genesis().clone();
    genesis.alloc.clear();
    genesis.config.chain_id = 321;
    genesis.config.terminal_total_difficulty = Some(threshold);
    genesis.difficulty = genesis_difficulty;
    genesis.timestamp = 0;
    Arc::new(
        ChainSpecBuilder::default()
            .chain(Chain::from(321))
            .genesis(genesis)
            .paris_at_ttd(threshold, 0)
            .build(),
    )
}
fn provider_fixture() -> (Provider, B256, B256) {
    let spec = spec(U256::from(10), U256::from(3));
    let genesis = spec.genesis_header().clone();
    let parent = Header {
        parent_hash: spec.genesis_hash(),
        number: 1,
        timestamp: 1,
        difficulty: U256::from(3),
        ..Header::default()
    };
    let parent_hash = parent.hash_slow();
    let terminal = Header {
        parent_hash,
        number: 2,
        timestamp: 2,
        difficulty: U256::from(4),
        ..Header::default()
    };
    let terminal_hash = terminal.hash_slow();
    (
        Provider {
            headers: HashMap::from([
                (spec.genesis_hash(), genesis),
                (parent_hash, parent),
                (terminal_hash, terminal),
            ]),
            difficulties: HashMap::from([
                (spec.genesis_hash(), U256::from(3)),
                (parent_hash, U256::from(6)),
                (terminal_hash, U256::from(10)),
            ]),
            spec,
            reads: Arc::new(AtomicUsize::new(0)),
            fail: false,
            replaced_spec: false,
        },
        terminal_hash,
        parent_hash,
    )
}
fn capture(provider: Provider) -> TerminalVerifier<Provider> {
    TerminalVerifier::capture(provider, config(), None, TerminalLimits::default()).unwrap()
}

#[test]
fn exact_complete_headers_and_total_difficulty_crossing_issue_a_sealed_bound_result() {
    let (provider, terminal, _) = provider_fixture();
    let reads = provider.reads.clone();
    let genesis = provider.spec.genesis_hash().into();
    let verifier = capture(provider);
    let proof = verifier.verify(terminal.into(), 64, 3).unwrap();
    assert_eq!(proof.parent_hash(), terminal.0);
    assert_eq!(proof.terminal_number(), 2);
    assert_eq!(proof.total_difficulty(), Some(U256::from(10)));
    assert!(proof.matches(&config(), terminal.into(), 64, 3, genesis));
    assert!(!proof.matches(&config(), terminal.into(), 65, 3, genesis));
    assert!(!proof.matches(&config(), terminal.into(), 64, 4, genesis));
    let mut other = config();
    other.chain_id += 1;
    assert!(!proof.matches(&other, terminal.into(), 64, 3, genesis));
    assert_eq!(reads.load(Ordering::SeqCst), 5);
}
#[test]
fn unavailable_terminal_parent_and_genesis_headers_remain_explicit_errors() {
    for which in 0..3 {
        let (mut provider, terminal, parent) = provider_fixture();
        let missing = match which {
            0 => terminal,
            1 => parent,
            _ => provider.spec.genesis_hash(),
        };
        provider.headers.remove(&missing);
        assert!(matches!(
            capture(provider).verify(terminal.into(), 64, 3),
            Err(TerminalVerificationError::MissingHeader)
        ));
    }
}
#[test]
fn missing_original_terminal_or_parent_difficulty_cannot_use_header_or_valid_shortcuts() {
    for which in 0..2 {
        let (mut provider, terminal, parent) = provider_fixture();
        provider.difficulties.remove(&if which == 0 { terminal } else { parent });
        assert!(matches!(
            capture(provider).verify(terminal.into(), 64, 3),
            Err(TerminalVerificationError::MissingDifficulty)
        ));
    }
}
#[test]
fn all_requested_complete_header_hashes_are_recomputed() {
    for which in 0..3 {
        let (mut provider, terminal, parent) = provider_fixture();
        let hash = match which {
            0 => terminal,
            1 => parent,
            _ => provider.spec.genesis_hash(),
        };
        provider.headers.get_mut(&hash).unwrap().state_root = B256::repeat_byte(77);
        assert!(matches!(
            capture(provider).verify(terminal.into(), 64, 3),
            Err(TerminalVerificationError::HeaderHash)
        ));
    }
}
#[test]
fn wrong_captured_chain_or_private_profile_is_rejected_before_reading_candidates() {
    let (provider, _, _) = provider_fixture();
    let mut other = config();
    other.chain_id += 1;
    assert!(matches!(
        TerminalVerifier::capture(provider, other, None, TerminalLimits::default()),
        Err(TerminalVerificationError::Identity)
    ));
    let (provider, _, _) = provider_fixture();
    let mut profile = private(&provider);
    profile.execution_genesis_hash = [8; 32];
    assert!(matches!(
        TerminalVerifier::capture(provider, config(), Some(&profile), TerminalLimits::default()),
        Err(TerminalVerificationError::Identity)
    ));
}
#[test]
fn stable_actual_provider_specification_allocation_is_required() {
    let (mut provider, terminal, _) = provider_fixture();
    provider.replaced_spec = true;
    let reads = provider.reads.clone();
    let verifier = capture(provider);
    assert!(matches!(
        verifier.verify(terminal.into(), 64, 3),
        Err(TerminalVerificationError::Identity)
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}
#[test]
fn threshold_requires_both_terminal_reached_and_parent_strictly_below() {
    for (parent_td, terminal_td) in [(5, 9), (10, 14), (11, 15)] {
        let (mut provider, terminal, parent) = provider_fixture();
        provider.difficulties.insert(parent, U256::from(parent_td));
        provider.difficulties.insert(terminal, U256::from(terminal_td));
        assert!(matches!(
            capture(provider).verify(terminal.into(), 64, 3),
            Err(TerminalVerificationError::Threshold)
        ));
    }
}
#[test]
fn original_difficulty_arithmetic_mismatch_and_overflow_reject() {
    for overflow in [false, true] {
        let (mut provider, terminal, parent) = provider_fixture();
        provider.difficulties.insert(parent, if overflow { U256::MAX } else { U256::from(6) });
        provider.difficulties.insert(terminal, U256::from(11));
        assert!(matches!(
            capture(provider).verify(terminal.into(), 64, 3),
            Err(TerminalVerificationError::DifficultyArithmetic)
        ));
    }
}
#[test]
fn genuine_header_ancestry_numbers_times_and_candidate_number_are_exact() {
    for defect in 0..4 {
        let (mut provider, terminal, parent) = provider_fixture();
        let mut header = provider.headers.remove(&terminal).unwrap();
        match defect {
            0 => header.number = 9,
            1 => header.timestamp = 1,
            2 => header.difficulty = U256::ZERO,
            _ => {}
        }
        let hash = header.hash_slow();
        provider.headers.insert(hash, header);
        let td = provider.difficulties.remove(&terminal).unwrap();
        provider.difficulties.insert(hash, td);
        assert!(matches!(
            capture(provider).verify(hash.into(), 64, if defect == 3 { 4 } else { 3 }),
            Err(TerminalVerificationError::Ancestry)
        ));
        assert_ne!(parent, B256::ZERO);
    }
}
#[test]
fn provider_read_budget_stops_before_an_extra_storage_operation() {
    let (provider, terminal, _) = provider_fixture();
    let reads = provider.reads.clone();
    let verifier = TerminalVerifier::capture(
        provider,
        config(),
        None,
        TerminalLimits { max_provider_reads: 4 },
    )
    .unwrap();
    assert!(matches!(
        verifier.verify(terminal.into(), 64, 3),
        Err(TerminalVerificationError::ReadBudget)
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 4);
    for bound in [0, 17] {
        let (provider, _, _) = provider_fixture();
        assert!(matches!(
            TerminalVerifier::capture(
                provider,
                config(),
                None,
                TerminalLimits { max_provider_reads: bound }
            ),
            Err(TerminalVerificationError::Bounds)
        ));
    }
}
#[test]
fn variable_header_data_is_bounded_before_any_hashing_authority() {
    let (mut provider, terminal, _) = provider_fixture();
    provider.headers.get_mut(&terminal).unwrap().extra_data = vec![0; 33].into();
    assert!(matches!(
        capture(provider).verify(terminal.into(), 64, 3),
        Err(TerminalVerificationError::HeaderBounds)
    ));
}
#[test]
fn original_provider_error_source_is_retained_and_no_result_issued() {
    let (mut provider, terminal, _) = provider_fixture();
    provider.fail = true;
    let Err(TerminalVerificationError::Provider(error)) =
        capture(provider).verify(terminal.into(), 64, 3)
    else {
        panic!("original provider failure required")
    };
    let original = error.downcast_ref::<io::Error>().unwrap();
    assert_eq!(original.kind(), io::ErrorKind::Other);
    assert_eq!(original.to_string(), "original local header failure");
}
#[test]
fn concurrent_owner_is_rejected_without_queuing_or_reading() {
    let (provider, terminal, _) = provider_fixture();
    let reads = provider.reads.clone();
    let verifier = capture(provider);
    let _held = verifier.work.lock().unwrap();
    assert!(matches!(
        verifier.verify(terminal.into(), 64, 3),
        Err(TerminalVerificationError::Busy)
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}
fn private(provider: &Provider) -> DevnetConfig {
    DevnetConfig {
        schema_version: 1,
        execution_genesis_hash: provider.spec.genesis_hash().into(),
        terminal_block_hash: provider.spec.genesis_hash().into(),
        terminal_block_hash_activation_epoch: 2,
        consensus: config(),
    }
}
#[test]
fn verified_private_genesis_override_obeys_epoch_and_needs_no_fabricated_td_parent() {
    let (mut provider, _, _) = provider_fixture();
    provider.difficulties.clear();
    let selected = provider.spec.genesis_hash();
    let profile = private(&provider);
    let reads = provider.reads.clone();
    let verifier =
        TerminalVerifier::capture(provider, config(), Some(&profile), TerminalLimits::default())
            .unwrap();
    assert!(matches!(
        verifier.verify(selected.into(), 63, 1),
        Err(TerminalVerificationError::Context)
    ));
    let proof = verifier.verify(selected.into(), 64, 1).unwrap();
    assert_eq!(proof.terminal_number(), 0);
    assert_eq!(proof.total_difficulty(), None);
    assert_eq!(proof.parent_hash(), selected.0);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(matches!(
        verifier.verify([17; 32], 64, 1),
        Err(TerminalVerificationError::WrongTerminalHash)
    ));
    assert!(matches!(
        verifier.verify(selected.into(), 64, 2),
        Err(TerminalVerificationError::Ancestry)
    ));
}
#[test]
fn malformed_private_activation_and_zero_context_cannot_select_override() {
    let (provider, terminal, _) = provider_fixture();
    let mut profile = private(&provider);
    profile.terminal_block_hash_activation_epoch = 3;
    assert!(matches!(
        TerminalVerifier::capture(provider, config(), Some(&profile), TerminalLimits::default()),
        Err(TerminalVerificationError::Identity)
    ));
    let (provider, _, _) = provider_fixture();
    let verifier = capture(provider);
    assert!(matches!(verifier.verify([0; 32], 64, 3), Err(TerminalVerificationError::Context)));
    assert!(matches!(
        verifier.verify(terminal.into(), 0, 3),
        Err(TerminalVerificationError::Context)
    ));
}
#[test]
fn zero_ttd_cannot_fabricate_a_pow_crossing_without_explicit_checked_override() {
    let (mut provider, terminal, _) = provider_fixture();
    provider.spec = spec(U256::ZERO, U256::from(3));
    // The exact genesis header is unchanged by the Paris threshold in this fixture.
    provider.headers.insert(provider.spec.genesis_hash(), provider.spec.genesis_header().clone());
    assert!(matches!(
        capture(provider).verify(terminal.into(), 64, 3),
        Err(TerminalVerificationError::Threshold)
    ));
}
