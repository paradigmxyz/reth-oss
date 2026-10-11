//! Exact-root JSON supplies only a bounded block schema hint when its SSZ header is absent.

use super::{parse_version, CheckpointDownloadError, CheckpointSource};
use reqwest::header;
use reth_embedded_beacon_primitives::{root_from_hex, Fork, Root};
use serde::Deserialize;

/// Metadata must agree with the locally scheduled, fully decoded original signed block.
pub(super) struct BlockMetadata {
    pub(super) version: Fork,
    pub(super) slot: u64,
    pub(super) state_root: Root,
}

#[derive(Deserialize)]
struct BlockResponse {
    version: String,
    data: SignedMessage,
}

#[derive(Deserialize)]
struct SignedMessage {
    message: Message,
}

#[derive(Deserialize)]
struct Message {
    slot: String,
    state_root: String,
}

impl CheckpointSource {
    pub(super) async fn block_metadata(
        &self,
        path: &str,
    ) -> Result<BlockMetadata, CheckpointDownloadError> {
        let url = self.base.join(path).map_err(|_| CheckpointDownloadError::Configuration)?;
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| CheckpointDownloadError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(CheckpointDownloadError::Status(response.status().as_u16()));
        }
        let content =
            response.headers().get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok());
        if content.and_then(|value| value.split(';').next()).map(str::trim) !=
            Some("application/json") ||
            response.headers().contains_key(header::CONTENT_ENCODING)
        {
            return Err(CheckpointDownloadError::Encoding);
        }
        let advertised = response
            .headers()
            .get("eth-consensus-version")
            .map(|value| {
                value.to_str().ok().and_then(parse_version).ok_or(CheckpointDownloadError::Version)
            })
            .transpose()?;
        // Full block JSON includes hexadecimal payload bytes. Bound this alternate
        // representation independently; original SSZ keeps its existing smaller limit.
        let maximum =
            self.limits.max_block_bytes.checked_mul(3).ok_or(CheckpointDownloadError::TooLarge)?;
        let bytes = Self::read_response(response, maximum).await?;
        let permit = self
            .decoder
            .clone()
            .try_acquire_owned()
            .map_err(|_| CheckpointDownloadError::Backpressure)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            decode_metadata(&bytes, advertised)
        })
        .await
        .map_err(|_| CheckpointDownloadError::Worker)?
    }
}

fn decode_metadata(
    bytes: &[u8],
    advertised: Option<Fork>,
) -> Result<BlockMetadata, CheckpointDownloadError> {
    let metadata: BlockResponse =
        serde_json::from_slice(bytes).map_err(|_| CheckpointDownloadError::Decode)?;
    let version = parse_version(&metadata.version).ok_or(CheckpointDownloadError::Version)?;
    if advertised.is_some_and(|value| value != version) {
        return Err(CheckpointDownloadError::Version);
    }
    if metadata.data.message.slot.is_empty() ||
        !metadata.data.message.slot.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CheckpointDownloadError::Decode);
    }
    let slot = metadata.data.message.slot.parse().map_err(|_| CheckpointDownloadError::Decode)?;
    let state_root = root_from_hex(&metadata.data.message.state_root)
        .map_err(|_| CheckpointDownloadError::Decode)?;
    Ok(BlockMetadata { version, slot, state_root })
}
