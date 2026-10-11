//! Exact bounded Ping and `MetaData` control codecs without consensus or custody authority.
//!
//! `MetaData` requests contain no framed SSZ bytes: orderly empty EOF is mandatory. Each
//! response has one code plus one SSZ-snappy chunk and no fork context. The current follower
//! advances its independent sequence only when proof-backed local custody changes. Dormant
//! startup advertises zero custody until native retained coverage has been verified.

use crate::{decode_request, encode_request, Limits, WireError, PING_V1};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use std::io;

/// Phase 0 metadata schema protocol, including the negotiated encoding.
pub const METADATA_V1: &str = "/eth2/beacon_chain/req/metadata/1/ssz_snappy";
/// Altair metadata schema, with four sync committee subnet bits.
pub const METADATA_V2: &str = "/eth2/beacon_chain/req/metadata/2/ssz_snappy";
/// Fulu metadata schema, with custody group count.
pub const METADATA_V3: &str = "/eth2/beacon_chain/req/metadata/3/ssz_snappy";

const CONTROL_LIMITS: Limits = Limits { max_payload: 256, max_compressed: 330 };
const MAX_RESPONSE_BYTES: usize = 341;

/// Exact decoded remote advertisement. These fields grant no availability or import authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerMetadata {
    /// Metadata sequence, independent of the signed discovery-record sequence.
    pub seq_number: u64,
    /// Persistent attestation subnet bit vector.
    pub attnets: [u8; 8],
    /// Four persistent sync committee subnet bits, absent in the Phase 0 schema.
    pub syncnets: Option<u8>,
    /// Custody group count, absent in schemas before Fulu.
    pub custody_group_count: Option<u64>,
}

/// A locally observed metadata version, with no arbitrary subscription or custody setter.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LocalMetadata {
    sequence: u64,
    custody: u64,
}
impl LocalMetadata {
    pub(crate) const fn seq_number(self) -> u64 {
        self.sequence
    }
    pub(crate) fn next_custody(self, custody: u64) -> Option<Self> {
        if !matches!(custody, 0 | 4) {
            return None
        }
        if self.custody == custody {
            return Some(self)
        }
        Some(Self { sequence: self.sequence.checked_add(1)?, custody })
    }
    pub(crate) const fn snapshot(self, protocol: ControlProtocol) -> PeerMetadata {
        PeerMetadata {
            seq_number: self.seq_number(),
            attnets: [0; 8],
            syncnets: if matches!(protocol, ControlProtocol::MetadataV1) { None } else { Some(0) },
            custody_group_count: if matches!(protocol, ControlProtocol::MetadataV3) {
                Some(self.custody)
            } else {
                None
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ControlProtocol {
    Ping,
    MetadataV1,
    MetadataV2,
    MetadataV3,
}
impl ControlProtocol {
    pub(crate) const fn id(self) -> &'static str {
        match self {
            Self::Ping => PING_V1,
            Self::MetadataV1 => METADATA_V1,
            Self::MetadataV2 => METADATA_V2,
            Self::MetadataV3 => METADATA_V3,
        }
    }
    fn from_id(id: &str) -> Result<Self, WireError> {
        match id {
            PING_V1 => Ok(Self::Ping),
            METADATA_V1 => Ok(Self::MetadataV1),
            METADATA_V2 => Ok(Self::MetadataV2),
            METADATA_V3 => Ok(Self::MetadataV3),
            _ => Err(WireError::Protocol),
        }
    }
    const fn response_bytes(self) -> usize {
        match self {
            Self::Ping => 8,
            Self::MetadataV1 => 16,
            Self::MetadataV2 => 17,
            Self::MetadataV3 => 25,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlRequest {
    Ping(u64),
    Metadata,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlReply {
    Ping(u64),
    Metadata(PeerMetadata),
    Error(u8),
}

#[derive(Clone, Debug)]
pub(crate) struct ControlCodec;
#[async_trait]
impl Codec for ControlCodec {
    type Protocol = StreamProtocol;
    type Request = ControlRequest;
    type Response = ControlReply;

    async fn read_request<T: AsyncRead + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request> {
        let protocol = ControlProtocol::from_id(protocol.as_ref()).map_err(wire_io)?;
        if protocol != ControlProtocol::Ping {
            let mut byte = [0];
            if io.read(&mut byte).await? != 0 {
                return Err(wire_io(WireError::TrailingBytes))
            }
            return Ok(ControlRequest::Metadata)
        }
        let bytes = read_bounded(io, 51).await?;
        let payload = decode_request(&bytes, Limits { max_payload: 8, max_compressed: 41 })
            .map_err(wire_io)?;
        if payload.len() != 8 {
            return Err(wire_io(WireError::Ssz))
        }
        Ok(ControlRequest::Ping(uint64(&payload, 0).map_err(wire_io)?))
    }

    async fn read_response<T: AsyncRead + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response> {
        let protocol = ControlProtocol::from_id(protocol.as_ref()).map_err(wire_io)?;
        let bytes = read_bounded(io, MAX_RESPONSE_BYTES).await?;
        let (&code, framed) = bytes.split_first().ok_or_else(|| wire_io(WireError::Truncated))?;
        if code != 0 && !valid_error_code(code) {
            return Err(wire_io(WireError::ResponseCode))
        }
        let payload = decode_request(framed, CONTROL_LIMITS).map_err(wire_io)?;
        if code != 0 {
            return Ok(ControlReply::Error(code))
        }
        if payload.len() != protocol.response_bytes() {
            return Err(wire_io(WireError::Ssz))
        }
        let seq_number = uint64(&payload, 0).map_err(wire_io)?;
        if protocol == ControlProtocol::Ping {
            return Ok(ControlReply::Ping(seq_number))
        }
        let mut attnets = [0; 8];
        attnets.copy_from_slice(&payload[8..16]);
        let syncnets = payload.get(16).copied();
        if syncnets.is_some_and(|bits| bits & 0xf0 != 0) {
            return Err(wire_io(WireError::Ssz))
        }
        let custody_group_count = if protocol == ControlProtocol::MetadataV3 {
            Some(uint64(&payload, 17).map_err(wire_io)?)
        } else {
            None
        };
        if custody_group_count.is_some_and(|count| count > 128) {
            return Err(wire_io(WireError::Ssz))
        }
        Ok(ControlReply::Metadata(PeerMetadata {
            seq_number,
            attnets,
            syncnets,
            custody_group_count,
        }))
    }

    async fn write_request<T: AsyncWrite + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        request: Self::Request,
    ) -> io::Result<()> {
        let protocol = ControlProtocol::from_id(protocol.as_ref()).map_err(wire_io)?;
        match (protocol, request) {
            (ControlProtocol::Ping, ControlRequest::Ping(sequence)) => {
                io.write_all(
                    &encode_request(&sequence.to_le_bytes(), CONTROL_LIMITS).map_err(wire_io)?,
                )
                .await?;
            }
            (
                ControlProtocol::MetadataV1 |
                ControlProtocol::MetadataV2 |
                ControlProtocol::MetadataV3,
                ControlRequest::Metadata,
            ) => {}
            _ => return Err(wire_io(WireError::Protocol)),
        }
        io.close().await
    }

    async fn write_response<T: AsyncWrite + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        response: Self::Response,
    ) -> io::Result<()> {
        let protocol = ControlProtocol::from_id(protocol.as_ref()).map_err(wire_io)?;
        let (code, payload) = match response {
            ControlReply::Ping(sequence) if protocol == ControlProtocol::Ping => {
                (0, sequence.to_le_bytes().to_vec())
            }
            ControlReply::Metadata(metadata) if protocol != ControlProtocol::Ping => {
                let expected = LocalMetadata::default().snapshot(protocol);
                if metadata.syncnets.is_some() != expected.syncnets.is_some() ||
                    metadata.custody_group_count.is_some() !=
                        expected.custody_group_count.is_some() ||
                    metadata.syncnets.is_some_and(|bits| bits & 0xf0 != 0) ||
                    metadata.custody_group_count.is_some_and(|count| count > 128)
                {
                    return Err(wire_io(WireError::Ssz))
                }
                let mut payload = metadata.seq_number.to_le_bytes().to_vec();
                payload.extend_from_slice(&metadata.attnets);
                if let Some(syncnets) = metadata.syncnets {
                    payload.push(syncnets);
                }
                if let Some(custody) = metadata.custody_group_count {
                    payload.extend_from_slice(&custody.to_le_bytes());
                }
                (0, payload)
            }
            ControlReply::Error(code) if valid_error_code(code) => (code, Vec::new()),
            _ => return Err(wire_io(WireError::Protocol)),
        };
        let framed = encode_request(&payload, CONTROL_LIMITS).map_err(wire_io)?;
        io.write_all(&[code]).await?;
        io.write_all(&framed).await?;
        io.close().await
    }
}

async fn read_bounded<T: AsyncRead + Unpin>(io: &mut T, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(limit);
    let mut buffer = [0; 64];
    loop {
        let count = io.read(&mut buffer).await?;
        if count == 0 {
            return Ok(bytes)
        }
        if count > limit.saturating_sub(bytes.len()) {
            return Err(wire_io(WireError::Oversized))
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}
fn uint64(bytes: &[u8], offset: usize) -> Result<u64, WireError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(WireError::Ssz)?
            .try_into()
            .map_err(|_| WireError::Ssz)?,
    ))
}
const fn valid_error_code(code: u8) -> bool {
    (code >= 1 && code <= 3) || code >= 128
}
fn wire_io(error: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests;
