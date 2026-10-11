// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Borrowed protobuf record counts before owned decoding can allocate repeated objects.
//!
//! Scalar parsing and unknown-field skipping use the pinned protobuf reader. Every nested
//! record has its own bounded slice. A capacity hit is remembered while the complete frame
//! structure is scanned, so a later malformed field retains InvalidData classification.

use super::IngressLimits;
use quick_protobuf::{BytesReader, Error, Result};
use std::io;

struct Cursor<'a> {
    bytes: &'a [u8],
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    fn varint32(&mut self) -> Result<u32> {
        let mut reader = BytesReader::from_bytes(self.bytes);
        let value = reader.read_varint32(self.bytes)?;
        self.bytes = &self.bytes[self.bytes.len() - reader.len()..];
        Ok(value)
    }
    fn varint64(&mut self) -> Result<u64> {
        let mut reader = BytesReader::from_bytes(self.bytes);
        let value = reader.read_varint64(self.bytes)?;
        self.bytes = &self.bytes[self.bytes.len() - reader.len()..];
        Ok(value)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.varint32()? as usize;
        let value = self.bytes.get(..length).ok_or(Error::UnexpectedEndOfBuffer)?;
        self.bytes = &self.bytes[length..];
        Ok(value)
    }
    fn string(&mut self) -> Result<()> {
        std::str::from_utf8(self.bytes()?).map_err(Error::from)?;
        Ok(())
    }
    fn unknown(&mut self, tag: u32) -> Result<()> {
        let mut reader = BytesReader::from_bytes(self.bytes);
        reader.read_unknown(self.bytes, tag)?;
        self.bytes = &self.bytes[self.bytes.len() - reader.len()..];
        Ok(())
    }
}

#[derive(Default)]
struct Scan {
    exceeded: bool,
}
impl Scan {
    fn count(&mut self, count: &mut usize, limit: usize) {
        *count = count.saturating_add(1);
        self.exceeded |= *count > limit;
    }
    fn rpc(&mut self, bytes: &[u8], limits: IngressLimits) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        let mut publications = 0;
        let mut subscriptions = 0;
        while !cursor.is_empty() {
            match cursor.varint32()? {
                10 => {
                    self.count(&mut subscriptions, limits.max_subscriptions);
                    subscription(cursor.bytes()?)?;
                }
                18 => {
                    self.count(&mut publications, limits.max_published_messages);
                    publication(cursor.bytes()?)?;
                }
                26 => self.controls(cursor.bytes()?, limits)?,
                tag => cursor.unknown(tag)?,
            }
        }
        Ok(())
    }
    fn controls(&mut self, bytes: &[u8], limits: IngressLimits) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        // A repeated optional control envelope replaces its predecessor in the native
        // decoder. Bound each envelope (including discarded ones), without summing their
        // mutually exclusive retained records or changing protobuf last-value semantics.
        let mut actions = 0;
        let mut ids = 0;
        while !cursor.is_empty() {
            match cursor.varint32()? {
                tag @ (10 | 18 | 26 | 34 | 42) => {
                    self.count(&mut actions, limits.max_controls);
                    let record = cursor.bytes()?;
                    match tag {
                        10 => self.ids(record, true, &mut ids, limits.max_message_ids)?,
                        18 | 42 => self.ids(record, false, &mut ids, limits.max_message_ids)?,
                        26 => graft(record)?,
                        34 => self.prune(record, limits.max_prune_peers)?,
                        _ => unreachable!(),
                    }
                }
                tag => cursor.unknown(tag)?,
            }
        }
        Ok(())
    }
    fn ids(&mut self, bytes: &[u8], ihave: bool, ids: &mut usize, limit: usize) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        while !cursor.is_empty() {
            match cursor.varint32()? {
                10 if ihave => cursor.string()?,
                18 if ihave => {
                    self.count(ids, limit);
                    cursor.bytes()?;
                }
                10 => {
                    self.count(ids, limit);
                    cursor.bytes()?;
                }
                tag => cursor.unknown(tag)?,
            }
        }
        Ok(())
    }
    fn prune(&mut self, bytes: &[u8], limit: usize) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        let mut peers = 0;
        while !cursor.is_empty() {
            match cursor.varint32()? {
                10 => cursor.string()?,
                18 => {
                    self.count(&mut peers, limit);
                    peer(cursor.bytes()?)?;
                }
                24 => {
                    cursor.varint64()?;
                }
                tag => cursor.unknown(tag)?,
            }
        }
        Ok(())
    }
}

fn publication(bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        match cursor.varint32()? {
            10 | 18 | 26 | 42 | 50 => {
                cursor.bytes()?;
            }
            34 => cursor.string()?,
            tag => cursor.unknown(tag)?,
        }
    }
    Ok(())
}
fn subscription(bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        match cursor.varint32()? {
            8 => {
                cursor.varint32()?;
            }
            18 => cursor.string()?,
            tag => cursor.unknown(tag)?,
        }
    }
    Ok(())
}
fn graft(bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        match cursor.varint32()? {
            10 => cursor.string()?,
            tag => cursor.unknown(tag)?,
        }
    }
    Ok(())
}
fn peer(bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while !cursor.is_empty() {
        match cursor.varint32()? {
            10 | 18 => {
                cursor.bytes()?;
            }
            tag => cursor.unknown(tag)?,
        }
    }
    Ok(())
}

/// Match the original codec's pointer-width unsigned length prefix without another dependency.
/// Incomplete prefixes leave the input untouched. The original decoder rejects a terminating
/// zero after the first byte and a continuation beyond its maximum byte count. Like that codec,
/// a terminating final byte is shifted into usize with its existing truncation semantics.
fn frame_length(bytes: &[u8]) -> io::Result<Option<(usize, &[u8])>> {
    let mut length = 0usize;
    let final_index = (usize::BITS as usize - 1) / 7;
    for (index, &byte) in bytes.iter().enumerate() {
        length |= usize::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if index != 0 && byte == 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "encoding is not minimal"));
            }
            return Ok(Some((length, &bytes[index + 1..])));
        }
        if index == final_index {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "input bytes exceed maximum"));
        }
    }
    Ok(None)
}
/// Inspect the first complete framed RPC without advancing, copying or allocating its bytes.
pub(super) fn check(
    bytes: &[u8],
    max_length: usize,
    limits: IngressLimits,
) -> std::result::Result<bool, quick_protobuf_codec::Error> {
    let Some((length, remaining)) = frame_length(bytes)? else { return Ok(false) };
    if length > max_length {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("message with {length}b exceeds maximum of {max_length}b"),
        )
        .into());
    }
    let Some(frame) = remaining.get(..length) else { return Ok(false) };
    let mut scan = Scan::default();
    scan.rpc(frame, limits).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if scan.exceeded {
        // ErrorKind alone avoids an owned error string or decoded object allocation.
        return Err(io::Error::from(io::ErrorKind::WouldBlock).into());
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
