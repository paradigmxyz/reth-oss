//! Test runners for `BlockchainTests` in <https://github.com/ethereum/tests>

use crate::{
    models::{BlockchainTest, ForkSpec},
    Case, Error, Suite,
};
use alloy_eip7928::bal::Bal;
use alloy_rlp::Decodable;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus, HeaderValidator};
use reth_db_common::init::{insert_genesis_hashes, insert_genesis_history, insert_genesis_state};
use reth_ethereum_consensus::{validate_block_post_execution, EthBeaconConsensus};
use reth_ethereum_primitives::Block;
use reth_evm::{
    execute::{BlockExecutionError, BlockExecutionOutput, Executor},
    ConfigureEvm,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{ParallelBridgeBuffered, RecoveredBlock, SealedBlock};
use reth_provider::{
    test_utils::create_test_provider_factory_with_chain_spec, BlockWriter, DatabaseProviderFactory,
    ExecutionOutcome, HashedPostStateProvider, HistoryWriter, OriginalValuesKnown, StateProvider,
    StateWriteConfig, StateWriter, StaticFileProviderFactory, StaticFileSegment, StaticFileWriter,
    StorageSettingsCache, TrieWriter,
};
use reth_revm::database::StateProviderDatabase;
use reth_trie::StateRoot;
use reth_trie_db::DatabaseStateRoot;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

/// A handler for the blockchain test suite.
#[derive(Debug)]
pub struct BlockchainTests {
    suite_path: PathBuf,
}

impl BlockchainTests {
    /// Create a new suite for tests with blockchain tests format.
    pub const fn new(suite_path: PathBuf) -> Self {
        Self { suite_path }
    }
}

impl Suite for BlockchainTests {
    type Case = BlockchainTestCase;

    fn suite_path(&self) -> &Path {
        &self.suite_path
    }
}

/// An Ethereum blockchain test.
#[derive(Debug, PartialEq, Eq)]
pub struct BlockchainTestCase {
    /// The tests within this test case.
    pub tests: BTreeMap<String, BlockchainTest>,
    /// Whether to skip this test case.
    pub skip: bool,
}

impl BlockchainTestCase {
    /// Returns `true` if the fork is not supported.
    const fn excluded_fork(network: ForkSpec) -> bool {
        matches!(
            network,
            ForkSpec::ByzantiumToConstantinopleAt5 |
                ForkSpec::Constantinople |
                ForkSpec::ConstantinopleFix |
                ForkSpec::MergeEOF |
                ForkSpec::MergeMeterInitCode |
                ForkSpec::MergePush0
        )
    }

    /// Execute the entire fixture, including valid blocks after an expected rejection.
    pub fn run_single_case(_name: &str, case: &BlockchainTest) -> Result<(), Error> {
        run_case(case)
    }
}

impl Case for BlockchainTestCase {
    fn load(path: &Path) -> Result<Self, Error> {
        Ok(Self {
            tests: {
                let s = fs::read_to_string(path)
                    .map_err(|error| Error::Io { path: path.into(), error })?;
                serde_json::from_str(&s)
                    .map_err(|error| Error::CouldNotDeserialize { path: path.into(), error })?
            },
            skip: should_skip(path),
        })
    }

    /// Runs the test cases for the Ethereum Forks test suite.
    ///
    /// # Errors
    /// Returns an error if the test is flagged for skipping or encounters issues during execution.
    fn run(self) -> Result<(), Error> {
        // If the test is marked for skipping, return a Skipped error immediately.
        if self.skip {
            return Err(Error::Skipped);
        }

        if self.tests.is_empty() {
            return Err(Error::Assertion("fixture file contains no test cases".into()))
        }
        let has_excluded = self.tests.values().any(|case| Self::excluded_fork(case.network));
        self.tests
            .into_iter()
            .filter(|(_, case)| !Self::excluded_fork(case.network))
            .par_bridge_buffered()
            .with_min_len(64)
            .try_for_each(|(name, case)| {
                Self::run_single_case(&name, &case)
                    .map_err(|err| Error::NamedFixture { name, err: Box::new(err) })
            })?;
        // A partially or completely excluded file must never be counted as a complete pass.
        if has_excluded {
            Err(Error::Skipped)
        } else {
            Ok(())
        }
    }
}

/// Executes a single `BlockchainTest` returning an error as soon as any block has a consensus
/// validation failure.
///
/// A `BlockchainTest` represents a self-contained scenario:
/// - It initializes a fresh blockchain state.
/// - It sequentially decodes, executes, and inserts a predefined set of blocks.
/// - It then verifies that the resulting blockchain state (post-state) matches the expected
///   outcome.
///
/// Returns:
/// - `Ok(())` if all blocks execute successfully.
/// - `Err(Error)` if any block fails to execute correctly.
fn run_case(case: &BlockchainTest) -> Result<(), Error> {
    // Create a new test database and initialize a provider for the test case.
    let chain_spec = case.chain_spec()?;
    let factory = create_test_provider_factory_with_chain_spec(chain_spec.clone());
    let provider = factory.database_provider_rw().unwrap();

    // Insert initial test state into the provider.
    let genesis_block = SealedBlock::<Block>::from_sealed_parts(
        case.genesis_block_header.clone().into(),
        Default::default(),
    )
    .try_recover()
    .unwrap();

    provider.insert_block(&genesis_block).map_err(|err| Error::block_failed(0, err))?;

    // Increment block number for receipts static file
    provider
        .static_file_provider()
        .latest_writer(StaticFileSegment::Receipts)
        .and_then(|mut writer| writer.increment_block(0))
        .map_err(|err| Error::block_failed(0, err))?;

    let genesis_state = case.pre.clone().into_genesis_state();
    insert_genesis_state(&provider, genesis_state.iter())
        .map_err(|err| Error::block_failed(0, err))?;
    insert_genesis_hashes(&provider, genesis_state.iter())
        .map_err(|err| Error::block_failed(0, err))?;
    insert_genesis_history(&provider, genesis_state.iter())
        .map_err(|err| Error::block_failed(0, err))?;

    // Build the genesis trie, as `init_genesis` does, so block 1 reads stored nodes.
    let (computed_genesis_root, trie_updates) = reth_trie_db::with_adapter!(provider, |A| {
        StateRoot::<reth_trie_db::DatabaseTrieCursorFactory<_, A>, _>::from_tx(provider.tx_ref())
            .root_with_updates()
    })
    .map_err(|err| Error::block_failed(0, err))?;
    crate::assert::assert_equal(
        genesis_block.state_root,
        computed_genesis_root,
        "genesis allocation root does not match",
    )?;
    provider.write_trie_updates(trie_updates).map_err(|err| Error::block_failed(0, err))?;

    let executor_provider = EthEvmConfig::ethereum(chain_spec.clone());
    let mut parent = genesis_block;
    let mut bal_buf = Vec::new();

    for (block_index, fixture) in case.blocks.iter().enumerate() {
        let block_number = (block_index + 1) as u64;
        // Decode and validate one candidate before publishing any of its state. Rejected blocks
        // do not prevent later candidates from extending the last accepted parent.
        let attempt = (|| {
            let block = decode_block(fixture, block_number)?;
            // Consensus checks before block execution
            pre_execution_checks(chain_spec.clone(), &parent, &block)
                .map_err(|err| Error::block_failed(block_number, err))?;

            // Execute the block
            let state_provider = provider.latest();
            let state_db = StateProviderDatabase((&state_provider).into_evm_state_provider());
            let mut executor = executor_provider.batch_executor(state_db);

            let result = executor
                .execute_one(&block)
                .map_err(|err| classify_execution_error(block_number, err))?;
            // Check the block access list size cap and compute its hash for post-Amsterdam blocks
            // so the consensus check below validates it.
            let block_access_list_hash = executor
                .take_bal()
                .map(|bal| {
                    let bal = Bal::from(bal);
                    bal.validate_gas_limit(block.gas_limit)
                        .map(|()| bal.compute_hash_with_buf(&mut bal_buf))
                })
                .transpose()
                .map_err(|err| Error::block_failed(block_number, err))?;
            let output =
                BlockExecutionOutput { state: executor.into_state().take_bundle(), result };

            // Consensus checks after block execution
            validate_block_post_execution(
                &block,
                &chain_spec,
                &output,
                None,
                block_access_list_hash,
            )
            .map_err(|err| Error::block_failed(block_number, err))?;

            // Compute and check the post state root
            let hashed_state = state_provider
                .hashed_post_state(&output.state)
                .map_err(|err| Error::harness_failed(block_number, err))?;
            let sorted = hashed_state.clone_into_sorted();
            let (computed_state_root, trie_updates) = reth_trie_db::with_adapter!(provider, |A| {
            StateRoot::<reth_trie_db::DatabaseTrieCursorFactory<_, A>, _>::overlay_root_with_updates(
                provider.tx_ref(),
                &sorted,
            )
        })
        .map_err(|err| Error::harness_failed(block_number, err))?;
            if computed_state_root != block.state_root {
                return Err(Error::block_failed(
                    block_number,
                    Error::Assertion("state root mismatch".to_string()),
                ));
            }

            Ok::<_, Error>((block, output, hashed_state, trie_updates))
        })();
        let (block, output, hashed_state, trie_updates) = match (attempt, &fixture.expect_exception)
        {
            (Ok(_), Some(expected)) => {
                return Err(Error::Assertion(format!(
                    "block {block_number} succeeded but fixture expects {expected}"
                )))
            }
            (Err(Error::BlockProcessingFailed { .. }), Some(_)) => continue,
            (Err(error), _) => return Err(error),
            (Ok(result), None) => result,
        };
        provider.insert_block(&block).map_err(|err| Error::block_failed(block_number, err))?;
        provider
            .static_file_provider()
            .commit()
            .map_err(|err| Error::block_failed(block_number, err))?;

        // Commit the post state/state diff to the database
        provider
            .write_state(
                &ExecutionOutcome::single(block.number, output),
                OriginalValuesKnown::Yes,
                StateWriteConfig::default(),
            )
            .map_err(|err| Error::block_failed(block_number, err))?;

        provider
            .write_hashed_state(&hashed_state.into_sorted())
            .map_err(|err| Error::block_failed(block_number, err))?;
        // Persist the trie so later blocks read stored nodes.
        provider
            .write_trie_updates(trie_updates)
            .map_err(|err| Error::block_failed(block_number, err))?;
        provider
            .update_history_indices(block.number..=block.number)
            .map_err(|err| Error::block_failed(block_number, err))?;

        // Since there were no errors, update the parent block
        parent = block.clone()
    }

    match &case.post_state {
        Some(expected_post_state) => {
            // Validate the post-state for the test case.
            //
            // If we get here then it means that the post-state root checks
            // made after we execute each block was successful.
            //
            // If an error occurs here, then it is:
            // - Either an issue with the test setup
            // - Possibly an error in the test case where the post-state root in the last block does
            //   not match the post-state values.
            for (address, account) in expected_post_state {
                account.assert_db(*address, provider.tx_ref())?;
            }
        }
        None => {
            // Some tests may not have post-state (e.g., state-heavy benchmark tests).
            // In this case, we can skip the post-state validation.
        }
    }

    crate::assert::assert_equal(
        case.lastblockhash,
        parent.hash(),
        "last valid block hash does not match",
    )?;
    if let Some(expected) = case.post_state_hash {
        crate::assert::assert_equal(
            expected,
            parent.state_root,
            "final state root does not match",
        )?;
    }
    Ok(())
}

fn classify_execution_error(block_number: u64, error: BlockExecutionError) -> Error {
    match error {
        BlockExecutionError::Validation(error) => Error::block_failed(block_number, error),
        BlockExecutionError::Internal(error) => Error::harness_failed(block_number, error),
    }
}

fn decode_block(
    block: &crate::models::Block,
    block_number: u64,
) -> Result<RecoveredBlock<Block>, Error> {
    // The candidate index remains authoritative even when an invalid header lies about its number.
    let mut remaining = block.rlp.as_ref();
    let decoded = SealedBlock::<Block>::decode(&mut remaining)
        .map_err(|err| Error::block_failed(block_number, err))?;
    if !remaining.is_empty() {
        return Err(Error::block_failed(block_number, alloy_rlp::Error::UnexpectedLength))
    }
    decoded.try_recover().map_err(|err| Error::block_failed(block_number, err))
}

fn pre_execution_checks(
    chain_spec: Arc<ChainSpec>,
    parent: &RecoveredBlock<Block>,
    block: &RecoveredBlock<Block>,
) -> Result<(), Error> {
    let consensus: EthBeaconConsensus<ChainSpec> = EthBeaconConsensus::new(chain_spec);

    let sealed_header = block.sealed_header();

    <EthBeaconConsensus<ChainSpec> as Consensus<Block>>::validate_body_against_header(
        &consensus,
        block.body(),
        sealed_header,
    )?;
    consensus.validate_header_against_parent(sealed_header, parent.sealed_header())?;
    consensus.validate_header(sealed_header)?;
    consensus.validate_block_pre_execution(block)?;

    Ok(())
}

/// Returns whether the test at the given path should be skipped.
///
/// Some tests are edge cases that cannot happen on mainnet, while others are skipped for
/// convenience (e.g. they take a long time to run) or are temporarily disabled.
///
/// The reason should be documented in a comment above the file name(s).
pub fn should_skip(path: &Path) -> bool {
    let path_str = path.to_str().expect("Path is not valid UTF-8");
    let name = path.file_name().unwrap().to_str().unwrap();
    matches!(
        name,
        // funky test with `bigint 0x00` value in json :) not possible to happen on mainnet and require
        // custom json parser. https://github.com/ethereum/tests/issues/971
        | "ValueOverflow.json"
        | "ValueOverflowParis.json"

        // txbyte is of type 02 and we don't parse tx bytes for this test to fail.
        | "typeTwoBerlin.json"

        // Test checks if nonce overflows. We are handling this correctly but we are not parsing
        // exception in testsuite. There are more nonce overflow tests that are internal
        // call/create, and those tests are passing and are enabled.
        | "CreateTransactionHighNonce.json"

        // Test check if gas price overflows, we handle this correctly but does not match tests specific
        // exception.
        | "HighGasPrice.json"
        | "HighGasPriceParis.json"

        // Skip test where basefee/accesslist/difficulty is present but it shouldn't be supported in
        // London/Berlin/TheMerge. https://github.com/ethereum/tests/blob/5b7e1ab3ffaf026d99d20b17bb30f533a2c80c8b/GeneralStateTests/stExample/eip1559.json#L130
        // It is expected to not execute these tests.
        | "accessListExample.json"
        | "basefeeExample.json"
        | "eip1559.json"
        | "mergeTest.json"

        // These tests are passing, but they take a lot of time to execute so we are going to skip them.
        | "loopExp.json"
        | "Call50000_sha256.json"
        | "static_Call50000_sha256.json"
        | "loopMul.json"
        | "CALLBlake2f_MaxRounds.json"
        | "shiftCombinations.json"

        // Skipped by revm as well: <https://github.com/bluealloy/revm/blob/be92e1db21f1c47b34c5a58cfbf019f6b97d7e4b/bins/revme/src/cmd/statetest/runner.rs#L115-L125>
        | "RevertInCreateInInit_Paris.json"
        | "RevertInCreateInInit.json"
        | "dynamicAccountOverwriteEmpty.json"
        | "dynamicAccountOverwriteEmpty_Paris.json"
        | "RevertInCreateInInitCreate2Paris.json"
        | "create2collisionStorage.json"
        | "RevertInCreateInInitCreate2.json"
        | "create2collisionStorageParis.json"
        | "InitCollision.json"
        | "InitCollisionParis.json"
    )
    // Ignore outdated EOF tests that haven't been updated for Cancun yet.
    || path_contains(path_str, &["EIPTests", "stEOF"])
}

/// `str::contains` but for a path. Takes into account the OS path separator (`/` or `\`).
fn path_contains(path_str: &str, rhs: &[&str]) -> bool {
    let rhs = rhs.join(std::path::MAIN_SEPARATOR_STR);
    path_str.contains(&rhs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, B256, U256};

    fn authentic_fixture() -> BlockchainTest {
        serde_json::from_str::<BTreeMap<String, BlockchainTest>>(include_str!(
            "../../testdata/chainid-cancun.json"
        ))
        .unwrap()
        .into_values()
        .next()
        .unwrap()
    }

    #[test]
    fn operational_failures_and_empty_or_excluded_files_cannot_pass() {
        let error = classify_execution_error(7, BlockExecutionError::msg("database read failed"));
        assert!(matches!(error, Error::HarnessFailed { block_number: 7, .. }));
        let invalid =
            crate::models::Block { rlp: Bytes::from_static(&[0xff]), ..Default::default() };
        assert!(matches!(
            decode_block(&invalid, 7),
            Err(Error::BlockProcessingFailed { block_number: 7, .. })
        ));
        assert!(BlockchainTestCase { tests: BTreeMap::new(), skip: false }.run().is_err());
        let mut excluded = authentic_fixture();
        excluded.network = ForkSpec::Constantinople;
        assert!(matches!(
            BlockchainTestCase {
                tests: BTreeMap::from([("excluded".into(), excluded)]),
                skip: false
            }
            .run(),
            Err(Error::Skipped)
        ));
    }

    #[test]
    fn authentic_chain_id_fixture_executes_and_checks_final_state() {
        run_case(&authentic_fixture()).unwrap();
    }

    #[test]
    fn rejected_candidates_leave_valid_parent_and_post_state_intact() {
        let mut case = authentic_fixture();
        let rejected = crate::models::Block {
            rlp: Bytes::from_static(&[0xff]),
            expect_exception: Some("invalid RLP".into()),
            ..Default::default()
        };
        case.blocks.insert(0, rejected);
        case.blocks.push(crate::models::Block {
            rlp: Bytes::from_static(&[0xff]),
            expect_exception: Some("invalid RLP".into()),
            ..Default::default()
        });
        run_case(&case).unwrap();
    }

    #[test]
    fn valid_candidate_cannot_satisfy_expected_rejection() {
        let mut case = authentic_fixture();
        case.blocks[0].expect_exception = Some("invalid RLP".into());
        assert!(run_case(&case).is_err());
    }

    #[test]
    fn fixture_chain_id_changes_actual_execution_without_mutating_fork_cache() {
        let mut case = authentic_fixture();
        case.config.as_mut().unwrap().chain_id = U256::from(2);
        assert_eq!(case.chain_spec().unwrap().chain.id(), 2);
        assert_eq!(case.network.to_chain_spec().chain.id(), 1);
        assert!(run_case(&case).is_err());
    }

    #[test]
    fn final_hash_and_optional_state_root_are_enforced() {
        let mut case = authentic_fixture();
        case.lastblockhash = B256::ZERO;
        assert!(run_case(&case).is_err());
        let mut case = authentic_fixture();
        case.post_state_hash = Some(B256::ZERO);
        assert!(run_case(&case).is_err());
    }

    #[test]
    fn fixture_schedule_uses_hex_parameters_and_preserves_transaction_limit() {
        let mut case = authentic_fixture();
        case.network = ForkSpec::Amsterdam;
        let config = case.config.as_mut().unwrap();
        config.network = Some(ForkSpec::Amsterdam);
        config.blob_schedule.insert(
            "Amsterdam".into(),
            serde_json::from_str(
                r#"{"target":"0x0e","max":"0x15","baseFeeUpdateFraction":"0xb24b3f"}"#,
            )
            .unwrap(),
        );
        let spec = case.chain_spec().unwrap();
        let params = spec.blob_params.active_scheduled_params_at_timestamp(0).unwrap();
        assert_eq!(
            (
                params.target_blob_count,
                params.max_blob_count,
                params.update_fraction,
                params.max_blobs_per_tx
            ),
            (14, 21, 11_684_671, 6)
        );
    }

    #[test]
    fn conflicting_or_unknown_fixture_configuration_fails() {
        let mut case = authentic_fixture();
        case.config.as_mut().unwrap().network = Some(ForkSpec::Prague);
        assert!(case.chain_spec().is_err());
        let mut case = authentic_fixture();
        case.config.as_mut().unwrap().blob_schedule.insert(
            "Unknown".into(),
            serde_json::from_str(
                r#"{"target":"0x03","max":"0x06","baseFeeUpdateFraction":"0x32f0ed"}"#,
            )
            .unwrap(),
        );
        assert!(case.chain_spec().is_err());
    }
}
