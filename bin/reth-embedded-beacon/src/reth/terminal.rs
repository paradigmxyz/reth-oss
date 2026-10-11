//! Terminal ancestry evidence read from the captured execution provider and its static files.
//! Missing or zero historical difficulty remains unavailable; no Engine result invents it.

use alloy_consensus::Header;
use alloy_primitives::{B256, U256};
use reth_chainspec::{ChainSpec, ChainSpecProvider, EthChainSpec};
use reth_db::static_file::TDWithHashMask;
use reth_embedded_beacon_node::terminal_verification::TerminalExecutionProvider;
use reth_ethereum_primitives::EthPrimitives;
use reth_provider::{HeaderProvider, ProviderError, StaticFileProviderFactory};
use reth_static_file_types::StaticFileSegment;
use std::{fmt::Debug, sync::Arc};

/// Adapter over the same built provider used for canonical execution and the typed Engine API.
#[derive(Debug)]
pub(super) struct TerminalProvider<P>(pub(super) P);
impl<P> TerminalExecutionProvider for TerminalProvider<P>
where
    P: HeaderProvider<Header = Header>
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + StaticFileProviderFactory<Primitives = EthPrimitives>
        + Debug
        + Sync,
{
    type Error = ProviderError;
    fn chain_spec(&self) -> Arc<ChainSpec> {
        self.0.chain_spec()
    }
    fn header(&self, hash: B256) -> Result<Option<Header>, Self::Error> {
        self.0.header(hash)
    }
    fn total_difficulty(&self, hash: B256) -> Result<Option<U256>, Self::Error> {
        let spec = self.0.chain_spec();
        if hash == spec.genesis_hash() {
            return Ok(Some(spec.genesis_header().difficulty))
        }
        let Some(header) = self.0.header(hash)? else { return Ok(None) };
        // Independently bind the canonical static row to the exact header requested by hash.
        if header.hash_slow() != hash {
            return Err(ProviderError::HeaderNotFound(hash.into()))
        }
        let files = self.0.static_file_provider();
        let segment = match files.get_segment_provider_for_block(
            StaticFileSegment::Headers,
            header.number,
            None,
        ) {
            Ok(segment) => segment,
            Err(ProviderError::MissingStaticFileBlock(_, _)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some((difficulty, stored_hash)) =
            segment.cursor()?.get_two::<TDWithHashMask>(header.number.into())?
        else {
            return Ok(None)
        };
        Ok((stored_hash == hash && *difficulty != U256::ZERO).then_some(*difficulty))
    }
}
