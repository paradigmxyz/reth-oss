//! Opt-in packed state through execution, persistence, proofs, reorgs, and restart.

use alloy_consensus::BlockHeader;
use alloy_primitives::{bytes, Bytes, B256, U256};
use alloy_provider::Provider;
use reth_chainspec::EthereumHardfork;
use reth_e2e_test_utils::{node::Finality, wait::poll_until, E2ETestSetupExt};
use reth_node_core::args::ExperimentalPacking;
use reth_node_ethereum::EthereumNode;
use reth_provider::{BlockHashReader, DatabaseProviderFactory};
use reth_prune::PruneSegment;

#[tokio::test]
async fn packed_state_execution_proof_reorg_and_restart() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    for mode in [None, Some(ExperimentalPacking::Dense), Some(ExperimentalPacking::Integer32)] {
        eprintln!("checking packed-state execution and restart: {mode:?}");
        let (mut node, wallet) = EthereumNode::test_setup_for(EthereumHardfork::Prague)
            .with_storage_v2(true)
            .with_restartable_nodes()
            .with_node_config_modifier(move |mut config| {
                config.pruning.minimal = true;
                // Produce the fully-pruned sender checkpoint on this two-block fixture.
                // Minimal mode's distance-based history retention still protects the reorg.
                config.pruning.block_interval = Some(1);
                config.pruning.minimum_distance = Some(0);
                config.db.experimental_state_packing = mode;
                config
            })
            .with_tree_config_modifier(|config| {
                config.with_persistence_threshold(0).with_memory_block_buffer_target(0)
            })
            .build_single()
            .await?;
        node.set_finality(Finality::Keep);
        let mut account =
            wallet.account(0).with_gas_limit(14_000_000).with_fees(20_000_000_000, 1_000_000_000);
        let contract = account.next_contract_address();
        // Transaction lookup is fully pruned, so check inclusion without fetching receipts by hash.
        let (_, deployment) = node.inject_and_advance(account.deploy(init_code()).await).await?;
        let anchor = deployment.block().hash();
        node.wait_for_persisted_block(deployment.block().number()).await?;
        assert_eq!(node.rpc_provider().get_storage_at(contract, U256::ZERO).await?, U256::ONE);
        account = account.with_gas_limit(100_000);
        let input = [U256::ZERO.to_be_bytes::<32>(), U256::from(999).to_be_bytes::<32>()].concat();
        let (_, update) =
            node.inject_and_advance(account.call(contract, Bytes::from(input)).await).await?;
        node.wait_for_persisted_block(update.block().number()).await?;
        let provider = node.rpc_provider();
        assert_eq!(provider.get_storage_at(contract, U256::ZERO).await?, U256::from(999));
        let proof = provider
            .get_proof(contract, vec![B256::ZERO.into(), B256::repeat_byte(255).into()])
            .await?;
        assert_eq!(proof.storage_proof[0].value, U256::from(999));
        assert_eq!(proof.storage_proof[1].value, U256::ZERO);
        drop(provider);
        node.wait_for_pool_head(update.block().hash()).await?;
        let replacement = node.advance_block_on(anchor).await?;
        let replacement_hash = replacement.block().hash();
        assert_ne!(replacement_hash, update.block().hash());
        poll_until("replacement block to be persisted", || async {
            let persisted = node.inner.provider.database_provider_ro()?;
            Ok((persisted.block_hash(replacement.block().number())? == Some(replacement_hash))
                .then_some(()))
        })
        .await?;
        // The full-prune marker is written once, when the initial sender file is removed.
        node.wait_for_prune_checkpoint(PruneSegment::SenderRecovery, 1).await?;
        assert_eq!(node.rpc_provider().get_storage_at(contract, U256::ZERO).await?, U256::ONE);
        let node = node.restart().await?;
        assert_eq!(node.rpc_provider().get_storage_at(contract, U256::ZERO).await?, U256::ONE);
        assert_eq!(node.block_hash(1), anchor);
        assert_eq!(node.block_hash(2), replacement_hash);
    }
    Ok(())
}

// Initialize 512 distinct storage leaves before returning a runtime that writes the slot
// and value supplied as two calldata words. The fixture exercises omitted trie branches.
fn init_code() -> Bytes {
    let mut code = Vec::new();
    for slot in 0..512u16 {
        code.push(0x61);
        code.extend_from_slice(&(slot + 1).to_be_bytes());
        code.push(0x61);
        code.extend_from_slice(&slot.to_be_bytes());
        code.push(0x55);
    }
    let offset = (code.len() + 11) as u16;
    code.extend_from_slice(&[0x60, 8, 0x61]);
    code.extend_from_slice(&offset.to_be_bytes());
    code.extend_from_slice(&[0x5f, 0x39, 0x60, 8, 0x5f, 0xf3]);
    code.extend_from_slice(&bytes!("6020356000355500"));
    Bytes::from(code)
}
