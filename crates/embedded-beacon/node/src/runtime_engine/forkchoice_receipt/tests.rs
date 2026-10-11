//! Controlled typed reply tests authenticate receipt identity, never actual live EL execution.

use super::*;
use crate::execution_bootstrap::tests::{adapter, Reply};
use std::time::Duration;

fn state() -> ForkchoiceState {
    ForkchoiceState {
        head_block_hash: B256::from([41; 32]),
        safe_block_hash: B256::from([40; 32]),
        finalized_block_hash: B256::from([39; 32]),
    }
}
#[tokio::test]
async fn actual_strict_valid_reply_mints_only_exact_same_adapter_and_state_receipt() {
    let config = ChainConfig::hoodi();
    let (engine, backend) =
        adapter(config.clone(), vec![Reply::Valid, Reply::Syncing], Duration::from_secs(1));
    let expected = state();
    let receipt =
        engine.forkchoice_updated_receipt(ForkchoiceVersion::V3, expected, None).await.unwrap();
    assert!(receipt.is_bound_to(&engine));
    assert!(receipt.matches(&engine, expected));
    assert_eq!(backend.forkchoice_call_count(), 1);
    let (different, _) = adapter(config, vec![Reply::Valid], Duration::from_secs(1));
    assert!(!receipt.is_bound_to(&different));
    assert!(!receipt.matches(&different, expected));
    for wrong in [
        ForkchoiceState { head_block_hash: B256::ZERO, ..expected },
        ForkchoiceState { safe_block_hash: B256::ZERO, ..expected },
        ForkchoiceState { finalized_block_hash: B256::ZERO, ..expected },
    ] {
        assert!(!receipt.matches(&engine, wrong));
    }
    let pending = ForkchoiceState { head_block_hash: B256::from([42; 32]), ..expected };
    assert!(matches!(
        engine.optimistic_forkchoice_updated(ForkchoiceVersion::V3, pending, None).await.unwrap(),
        OptimisticForkchoice::Syncing
    ));
    assert!(receipt.is_bound_to(&engine));
    assert!(receipt.matches(&engine, expected));
    assert!(!receipt.matches(&engine, pending));
    assert_eq!(backend.forkchoice_call_count(), 2);
    engine.close_bootstrap_admission();
    assert!(!receipt.is_bound_to(&engine));
    assert!(!receipt.matches(&engine, expected));
}
#[tokio::test]
async fn every_non_valid_malformed_and_failed_original_reply_refuses_a_receipt() {
    for reply in [
        Reply::Syncing,
        Reply::Rejected,
        Reply::WrongHash,
        Reply::MissingHash,
        Reply::PayloadJob,
        Reply::Extended,
        Reply::Error,
    ] {
        let (engine, backend) = adapter(ChainConfig::hoodi(), vec![reply], Duration::from_secs(1));
        assert!(engine
            .forkchoice_updated_receipt(ForkchoiceVersion::V3, state(), None)
            .await
            .is_err());
        assert_eq!(backend.forkchoice_call_count(), 1);
    }
}
#[tokio::test]
async fn zero_head_and_unsupported_version_cannot_call_engine_or_mint_receipt() {
    let (engine, backend) =
        adapter(ChainConfig::hoodi(), vec![Reply::Valid], Duration::from_secs(1));
    assert!(matches!(
        engine
            .forkchoice_updated_receipt(
                ForkchoiceVersion::V3,
                ForkchoiceState { head_block_hash: B256::ZERO, ..state() },
                None
            )
            .await,
        Err(EngineVerificationError::Configuration)
    ));
    assert!(matches!(
        engine.forkchoice_updated_receipt(ForkchoiceVersion::V5, state(), None).await,
        Err(EngineVerificationError::Configuration)
    ));
    assert_eq!(backend.forkchoice_call_count(), 0);
}
