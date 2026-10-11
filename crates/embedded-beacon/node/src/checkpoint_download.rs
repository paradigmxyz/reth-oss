//! Bounded acquisition of untrusted original checkpoint SSZ through the standard beacon API.
//! Provider responses never select trust, establish finality, or change runtime readiness.

use crate::{BootstrapDecodeLimits, BootstrapError};
use reqwest::{header, redirect::Policy, Client, Url};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{Preset, SignedBeaconBlock};
use std::{fmt, net::IpAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::Semaphore;

mod metadata;

/// One exclusively borrowed downloader with no internal queue or background retry workers.
pub struct CheckpointSource {
    client: Client,
    base: Url,
    config: ChainConfig,
    limits: BootstrapDecodeLimits,
    timeout: Duration,
    decoder: Arc<Semaphore>,
}

impl CheckpointSource {
    /// Use HTTPS, or explicit literal loopback HTTP for an isolated local network.
    /// Requests never follow redirects or read environment-configured HTTP proxies.
    pub fn new(
        base: &str,
        config: ChainConfig,
        limits: BootstrapDecodeLimits,
        timeout: Duration,
    ) -> Result<Self, CheckpointDownloadError> {
        Self::build(base, config, limits, timeout, IpAddr::is_loopback)
    }

    /// Permit literal private container endpoints only for a completely checked devnet profile.
    /// The source still supplies untrusted bytes and cannot select consensus identity or trust.
    #[cfg(feature = "peer-runtime")]
    pub fn for_devnet(
        base: &str,
        devnet: &reth_embedded_beacon_config::DevnetConfig,
        limits: BootstrapDecodeLimits,
        timeout: Duration,
    ) -> Result<Self, CheckpointDownloadError> {
        devnet.validate().map_err(|_| CheckpointDownloadError::Configuration)?;
        let url = Url::parse(base).map_err(|_| CheckpointDownloadError::Configuration)?;
        let permitted = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| {
                reth_embedded_beacon_network::EndpointPolicy::PrivateNetwork.permits(ip)
            });
        if !permitted {
            return Err(CheckpointDownloadError::Configuration);
        }
        Self::build(base, devnet.consensus.clone(), limits, timeout, |ip| {
            reth_embedded_beacon_network::EndpointPolicy::PrivateNetwork.permits(*ip)
        })
    }

    fn build(
        base: &str,
        config: ChainConfig,
        limits: BootstrapDecodeLimits,
        timeout: Duration,
        permits_http: fn(&IpAddr) -> bool,
    ) -> Result<Self, CheckpointDownloadError> {
        config.validate().map_err(|_| CheckpointDownloadError::Configuration)?;
        if timeout.is_zero() ||
            timeout > Duration::from_secs(600) ||
            limits.max_block_bytes == 0 ||
            limits.max_block_bytes > 10 * 1024 * 1024 ||
            limits.max_state_bytes == 0 ||
            limits.max_state_bytes > 512 * 1024 * 1024
        {
            return Err(CheckpointDownloadError::Configuration);
        }
        let mut base = Url::parse(base).map_err(|_| CheckpointDownloadError::Configuration)?;
        let local_http = base
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| permits_http(&ip));
        if (base.scheme() != "https" && !(base.scheme() == "http" && local_http)) ||
            !base.username().is_empty() ||
            base.password().is_some() ||
            base.query().is_some() ||
            base.fragment().is_some() ||
            base.host_str().is_none()
        {
            return Err(CheckpointDownloadError::Configuration);
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let client = Client::builder()
            .no_proxy()
            .user_agent("reth-embedded-beacon")
            .redirect(Policy::none())
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(15)))
            .build()
            .map_err(|_| CheckpointDownloadError::Transport)?;
        Ok(Self { client, base, config, limits, timeout, decoder: Arc::new(Semaphore::new(1)) })
    }

    /// Fetch the exact explicitly trusted block and its committed state, then a boundary if needed.
    /// Only a later complete verifier can authenticate these bytes; HTTP metadata confers no trust.
    pub async fn download(
        &self,
        trusted: Checkpoint,
    ) -> Result<CheckpointInputs, CheckpointDownloadError> {
        tokio::time::timeout(self.timeout, self.download_inner(trusted))
            .await
            .map_err(|_| CheckpointDownloadError::Transport)?
    }

    async fn download_inner(
        &self,
        trusted: Checkpoint,
    ) -> Result<CheckpointInputs, CheckpointDownloadError> {
        let boundary = trusted
            .epoch
            .checked_mul(self.config.slots_per_epoch)
            .ok_or(CheckpointDownloadError::Configuration)?;
        let block_path = format!("eth/v2/beacon/blocks/{}", root_hex(trusted.root));
        let (block, version) =
            self.fetch_original(&block_path, self.limits.max_block_bytes, true).await?;
        // Only absent block metadata may use the exact-root JSON schema hint. Neither
        // representation selects trust, and the original signed SSZ remains mandatory.
        let metadata =
            if version.is_none() { Some(self.block_metadata(&block_path).await?) } else { None };
        let version = version
            .or_else(|| metadata.as_ref().map(|value| value.version))
            .ok_or(CheckpointDownloadError::Version)?;
        let block: Arc<[u8]> = block.into();
        let config = self.config.clone();
        let bytes = block.clone();
        let permit = self
            .decoder
            .clone()
            .try_acquire_owned()
            .map_err(|_| CheckpointDownloadError::Backpressure)?;
        let (slot, state_root) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let decoded = decode_block(&config, &bytes, trusted, version)?;
            if metadata
                .as_ref()
                .is_some_and(|value| value.slot != decoded.0 || value.state_root != decoded.1)
            {
                return Err(CheckpointDownloadError::Linkage);
            }
            Ok(decoded)
        })
        .await
        .map_err(|_| CheckpointDownloadError::Worker)??;
        if slot > boundary {
            return Err(CheckpointDownloadError::Linkage);
        }
        let post_fork = self
            .config
            .fork_at_slot(slot)
            .map_err(|_| CheckpointDownloadError::Configuration)?
            .fork;
        let (post, version) = self
            .fetch(
                &format!("eth/v2/debug/beacon/states/{}", root_hex(state_root)),
                self.limits.max_state_bytes,
            )
            .await?;
        if version != post_fork {
            return Err(CheckpointDownloadError::Version);
        }
        let post: Arc<[u8]> = post.into();
        let state = if slot == boundary {
            post.clone()
        } else {
            let (state, version) = self
                .fetch(
                    &format!("eth/v2/debug/beacon/states/{boundary}"),
                    self.limits.max_state_bytes,
                )
                .await?;
            if version !=
                self.config
                    .fork_at_slot(boundary)
                    .map_err(|_| CheckpointDownloadError::Configuration)?
                    .fork
            {
                return Err(CheckpointDownloadError::Version);
            }
            state.into()
        };
        tracing::info!(target: "beacon::bootstrap", epoch = trusted.epoch, block_slot = slot,
            boundary_slot = boundary, block_bytes = block.len(), state_bytes = state.len(),
            "[beacon] Downloaded original checkpoint inputs for mandatory authentication");
        Ok(CheckpointInputs {
            trusted,
            state_ssz: state,
            post_block_state_ssz: post,
            block_ssz: block,
            block_slot: slot,
        })
    }

    async fn fetch(
        &self,
        path: &str,
        maximum: usize,
    ) -> Result<(Vec<u8>, Fork), CheckpointDownloadError> {
        let (bytes, version) = self.fetch_original(path, maximum, false).await?;
        Ok((bytes, version.ok_or(CheckpointDownloadError::Version)?))
    }

    async fn fetch_original(
        &self,
        path: &str,
        maximum: usize,
        allow_missing_version: bool,
    ) -> Result<(Vec<u8>, Option<Fork>), CheckpointDownloadError> {
        let url = self.base.join(path).map_err(|_| CheckpointDownloadError::Configuration)?;
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .map_err(|_| CheckpointDownloadError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(CheckpointDownloadError::Status(response.status().as_u16()));
        }
        let content =
            response.headers().get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok());
        if content.and_then(|value| value.split(';').next()).map(str::trim) !=
            Some("application/octet-stream") ||
            response.headers().contains_key(header::CONTENT_ENCODING)
        {
            return Err(CheckpointDownloadError::Encoding);
        }
        let version = match response.headers().get("eth-consensus-version") {
            Some(value) => Some(
                value
                    .to_str()
                    .ok()
                    .and_then(parse_version)
                    .ok_or(CheckpointDownloadError::Version)?,
            ),
            None if allow_missing_version => None,
            None => return Err(CheckpointDownloadError::Version),
        };
        Ok((Self::read_response(response, maximum).await?, version))
    }

    async fn read_response(
        mut response: reqwest::Response,
        maximum: usize,
    ) -> Result<Vec<u8>, CheckpointDownloadError> {
        if response.content_length().is_some_and(|length| length > maximum as u64) {
            return Err(CheckpointDownloadError::TooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) =
            response.chunk().await.map_err(|_| CheckpointDownloadError::Transport)?
        {
            let next =
                bytes.len().checked_add(chunk.len()).ok_or(CheckpointDownloadError::TooLarge)?;
            if next > maximum {
                return Err(CheckpointDownloadError::TooLarge);
            }
            bytes.try_reserve(chunk.len()).map_err(|_| CheckpointDownloadError::TooLarge)?;
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(CheckpointDownloadError::Encoding);
        }
        Ok(bytes)
    }
}

/// Original downloaded bytes; this type is deliberately not a verification receipt.
#[derive(Clone, Debug)]
pub struct CheckpointInputs {
    /// Explicit operator trust, never learned from the provider's current head.
    pub trusted: Checkpoint,
    /// Original epoch-boundary state bytes, requiring complete authentication.
    pub state_ssz: Arc<[u8]>,
    /// Actual earlier block's directly committed state; distinct at an empty epoch boundary.
    pub post_block_state_ssz: Arc<[u8]>,
    /// Original signed block bytes; never a reconstructed unsigned message.
    pub block_ssz: Arc<[u8]>,
    /// Fully decoded block slot; this alone grants no bootstrap or import authority.
    pub block_slot: u64,
}

/// Bounded classifications omit source URLs, remote text and HTTP error bodies.
#[derive(Debug, Error)]
pub enum CheckpointDownloadError {
    /// Invalid explicit endpoint, network or resource policy.
    #[error("invalid checkpoint source configuration")]
    Configuration,
    /// A network or request deadline failed.
    #[error("checkpoint request transport failed")]
    Transport,
    /// Exact remote status; its response body is not logged or retained.
    #[error("checkpoint request returned status {0}")]
    Status(u16),
    /// Uncompressed SSZ response format is required.
    #[error("checkpoint response is not original SSZ")]
    Encoding,
    /// The advertised schema is missing, unknown or disagrees with the checked schedule.
    #[error("checkpoint response schema differs from the configured fork")]
    Version,
    /// One complete response exceeds its preallocation limit.
    #[error("checkpoint response exceeds its byte limit")]
    TooLarge,
    /// The explicit trusted root or epoch conflicts with complete block decoding.
    #[error("checkpoint response does not match the trusted block")]
    Linkage,
    /// Complete SSZ structural decoding failed.
    #[error("checkpoint block SSZ decoding failed")]
    Decode,
    /// The exclusively owned bounded decoding worker failed.
    #[error("checkpoint decoding worker failed")]
    Worker,
    /// The previous cancelled decode still owns the exclusive blocking-work reservation.
    #[error("checkpoint decoding work is still active")]
    Backpressure,
    /// Mandatory concrete authentication failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
}

fn decode_block(
    config: &ChainConfig,
    bytes: &[u8],
    trusted: Checkpoint,
    version: Fork,
) -> Result<(u64, Root), CheckpointDownloadError> {
    if bytes.get(..4) != Some(100u32.to_le_bytes().as_slice()) {
        return Err(CheckpointDownloadError::Decode);
    }
    let slot = bytes.get(100..108).ok_or(CheckpointDownloadError::Decode)?;
    let slot = u64::from_le_bytes(slot.try_into().map_err(|_| CheckpointDownloadError::Decode)?);
    let fork = config.fork_at_slot(slot).map_err(|_| CheckpointDownloadError::Configuration)?.fork;
    if fork != version {
        return Err(CheckpointDownloadError::Version);
    }
    let preset = match config.preset {
        ConfigPreset::Mainnet => Preset::Mainnet,
        ConfigPreset::Minimal => Preset::Minimal,
    };
    let block = SignedBeaconBlock::from_ssz(bytes, fork, preset)
        .map_err(|_| CheckpointDownloadError::Decode)?;
    if block.message_root().map_err(|_| CheckpointDownloadError::Decode)? != trusted.root {
        return Err(CheckpointDownloadError::Linkage);
    }
    Ok((slot, block.header().map_err(|_| CheckpointDownloadError::Decode)?.state_root))
}

fn parse_version(value: &str) -> Option<Fork> {
    match value {
        "phase0" => Some(Fork::Phase0),
        "altair" => Some(Fork::Altair),
        "bellatrix" => Some(Fork::Bellatrix),
        "capella" => Some(Fork::Capella),
        "deneb" => Some(Fork::Deneb),
        "electra" => Some(Fork::Electra),
        "fulu" => Some(Fork::Fulu),
        "gloas" => Some(Fork::Gloas),
        _ => None,
    }
}

fn root_hex(root: Root) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(66);
    text.push_str("0x");
    for byte in root {
        text.push(char::from(HEX[usize::from(byte >> 4)]));
        text.push(char::from(HEX[usize::from(byte & 15)]));
    }
    text
}

#[cfg(test)]
mod tests;

impl fmt::Debug for CheckpointSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointSource")
            .field("network", &self.config.name)
            .field("limits", &self.limits)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
