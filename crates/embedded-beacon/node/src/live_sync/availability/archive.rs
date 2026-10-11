//! Bounded proof-input retention in the exact existing shared writer.
//! Stored checksums and sidecars confer no consensus or custody authority. Serving revalidates
//! mathematical proofs against actual retained original native blocks before returning bytes.

use super::*;
use crate::compact_persistence::retention_gc::RetentionDomain;
use reth_embedded_beacon_network::{
    availability_rpc::{SidecarKind, WireSidecar},
    custody::CustodyParameters,
    Protocol,
};
use reth_embedded_beacon_primitives::sha256;
use reth_embedded_beacon_shared_data::{
    journal::{ensure_published_durability, publish_file},
    Encoding, ObjectId, ObjectKind, Owner, SharedDataStore, StoreIdentity,
};
use reth_fs_util as fs;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    path::PathBuf,
};

const RETAINED_EPOCHS: u64 = 4096;
const MAX_RECORD_BYTES: u64 = 4096;
const MAX_EPOCH_DIRECTORIES: usize = 4098;

/// An immutable data archive, sharing the same physical writer and independent proof-input pins.
#[derive(Clone, Debug)]
pub(crate) struct SidecarArchive {
    config: Arc<ChainConfig>,
    config_digest: Root,
    shared: SharedDataStore,
    root: PathBuf,
    inner: Arc<Mutex<ArchiveState>>,
}

#[derive(Debug, Default)]
struct ArchiveState {
    node_id: Option<Root>,
    columns: BTreeSet<u64>,
    latest_epoch: Option<u64>,
    poisoned: bool,
    index_lock: Option<File>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema: u64,
    identity: StoreIdentity,
    config_digest: Root,
    root: Root,
    slot: u64,
    index: u64,
    context: [u8; 4],
    column: bool,
    object: ObjectId,
    checksum: Root,
}

impl SidecarArchive {
    /// Capture the exact existing writer without opening another writer or touching disk.
    pub(crate) fn new(
        config: Arc<ChainConfig>,
        digest: Root,
        shared: SharedDataStore,
    ) -> Result<Self, RetrievalError> {
        shared.ensure_healthy().map_err(|_| RetrievalError::Storage)?;
        if shared.identity().genesis_root != config.genesis_validators_root {
            return Err(RetrievalError::Configuration);
        }
        // The shared identity can include broader devnet policy. Its digest is retained exactly,
        // independently of the consensus ChainConfig digest; the two must never be conflated.
        let root = shared.namespace_root().join("beacon-sidecars-v1");
        Ok(Self {
            config,
            config_digest: digest,
            shared,
            root,
            inner: Arc::new(Mutex::new(ArchiveState::default())),
        })
    }

    pub(super) fn writer_identity(&self) -> StoreIdentity {
        self.shared.identity()
    }

    /// Bind minimum custody-column selection to the actual persisted discovery identity.
    /// Selection alone is no retention, serving or advertised custody receipt.
    pub(crate) fn bind_identity(&self, node_id: Root) -> Result<(), RetrievalError> {
        let mut state = self.inner.lock().map_err(|_| RetrievalError::Storage)?;
        if state.poisoned || state.node_id.is_some_and(|old| old != node_id) {
            return Err(RetrievalError::Storage);
        }
        state.columns = custody_columns(node_id)?;
        state.node_id = Some(node_id);
        Ok(())
    }

    /// Selection is derived from the actual persisted identity, never from stored sidecar hints.
    pub(super) fn selection(&self) -> Result<Option<(Root, [u64; 4])>, RetrievalError> {
        self.shared.ensure_healthy().map_err(|_| RetrievalError::Storage)?;
        let mut state = self.inner.lock().map_err(|_| RetrievalError::Storage)?;
        if state.poisoned {
            return Err(RetrievalError::Storage);
        }
        self.ensure_writer(&mut state)?;
        state
            .node_id
            .map(|node_id| {
                let columns = state.columns.iter().copied().collect::<Vec<_>>();
                Ok((node_id, columns.try_into().map_err(|_| RetrievalError::Configuration)?))
            })
            .transpose()
    }
    /// Retain only after the caller's actual candidate-associated mathematical proof succeeds.
    /// Blob originals remain complete; column originals retain the four native custody groups.
    pub(super) fn retain_verified(&self, originals: &[WireSidecar]) -> Result<(), RetrievalError> {
        let mut state = self.inner.lock().map_err(|_| RetrievalError::Storage)?;
        if state.poisoned || state.node_id.is_none() {
            return Err(RetrievalError::Storage);
        }
        let result = self.retain_inner(&mut state, originals);
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }

    fn retain_inner(
        &self,
        state: &mut ArchiveState,
        originals: &[WireSidecar],
    ) -> Result<(), RetrievalError> {
        self.shared.ensure_healthy().map_err(|_| RetrievalError::Storage)?;
        self.ensure_writer(state)?;
        for wire in originals {
            if wire.kind() == SidecarKind::Column && !state.columns.contains(&wire.index()) {
                continue;
            }
            let epoch = wire.slot() / self.config.slots_per_epoch;
            let latest = state.latest_epoch.unwrap_or(epoch).max(epoch);
            if epoch < latest.saturating_sub(RETAINED_EPOCHS) {
                continue;
            }
            if state.latest_epoch != Some(latest) {
                self.prune_history(latest)?;
                state.latest_epoch = Some(latest);
            }
            let epoch_path = self.root.join(format!("{epoch:020}"));
            fs::create_dir_all(&epoch_path).map_err(|_| RetrievalError::Storage)?;
            let block_path = epoch_path.join(root_name(wire.block_root()));
            if !block_path.exists() {
                let entries = fs::read_dir(&epoch_path).map_err(|_| RetrievalError::Storage)?;
                let maximum = self
                    .config
                    .slots_per_epoch
                    .checked_mul(2)
                    .ok_or(RetrievalError::Configuration)?;
                let mut count = 0u64;
                for entry in entries {
                    let entry = entry.map_err(|_| RetrievalError::Storage)?;
                    if !entry.file_type().map_err(|_| RetrievalError::Storage)?.is_dir() ||
                        parse_root(entry.file_name().to_str().ok_or(RetrievalError::Storage)?)
                            .is_none()
                    {
                        return Err(RetrievalError::Storage);
                    }
                    count += 1;
                    if count >= maximum {
                        return Err(RetrievalError::Storage);
                    }
                }
                fs::create_dir_all(&block_path).map_err(|_| RetrievalError::Storage)?;
            }
            let object = ObjectId::new(ObjectKind::BeaconDataSidecar, Encoding::Ssz, wire.ssz());
            let path = block_path.join(format!("{:03}.json", wire.index()));
            if path.exists() {
                let prior = self.read_record(&path)?;
                if prior.object != object ||
                    prior.root != wire.block_root() ||
                    prior.slot != wire.slot() ||
                    prior.index != wire.index() ||
                    prior.context != wire.context() ||
                    prior.column != (wire.kind() == SidecarKind::Column)
                {
                    return Err(RetrievalError::Storage);
                }
                ensure_published_durability(&path).map_err(|_| RetrievalError::Storage)?;
            } else {
                self.shared
                    .put(
                        ObjectKind::BeaconDataSidecar,
                        Encoding::Ssz,
                        wire.ssz(),
                        owner(wire.block_root()),
                    )
                    .map_err(|_| RetrievalError::Storage)?;
                let mut record = Record {
                    schema: 1,
                    identity: self.shared.identity(),
                    config_digest: self.config_digest,
                    root: wire.block_root(),
                    slot: wire.slot(),
                    index: wire.index(),
                    context: wire.context(),
                    column: wire.kind() == SidecarKind::Column,
                    object,
                    checksum: [0; 32],
                };
                record.checksum =
                    sha256(&serde_json::to_vec(&record).map_err(|_| RetrievalError::Storage)?);
                let bytes = serde_json::to_vec(&record).map_err(|_| RetrievalError::Storage)?;
                if bytes.len() as u64 > MAX_RECORD_BYTES {
                    return Err(RetrievalError::Storage);
                }
                publish_file(&path, &bytes).map_err(|_| RetrievalError::Storage)?;
            }
        }
        Ok(())
    }

    /// Read complete immutable proof inputs. The caller must independently revalidate against
    /// an actual native retained block; durable checksum integrity is not a validity receipt.
    pub(crate) fn read(
        &self,
        root: Root,
        slot: u64,
        index: u64,
        column: bool,
    ) -> Result<Option<WireSidecar>, RetrievalError> {
        let mut state = self.inner.lock().map_err(|_| RetrievalError::Storage)?;
        if state.poisoned {
            return Err(RetrievalError::Storage);
        }
        let path = self
            .root
            .join(format!("{:020}", slot / self.config.slots_per_epoch))
            .join(root_name(root))
            .join(format!("{index:03}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let result = (|| {
            let record = self.read_record(&path)?;
            if record.root != root || record.slot != slot || record.index != index {
                return Err(RetrievalError::Storage);
            }
            if record.column != column {
                return Ok(None);
            }
            let bytes = self
                .shared
                .get_bounded(record.object, self.config.max_payload_size.min(10 * 1024 * 1024))
                .map_err(|_| RetrievalError::Storage)?
                .ok_or(RetrievalError::Storage)?;
            if !self.shared.owner_objects(owner(root)).contains(&record.object) {
                return Err(RetrievalError::Storage);
            }
            let protocol =
                if column { Protocol::DataColumnsByRootV1 } else { Protocol::BlobSidecarsByRootV1 };
            let wire =
                WireSidecar::from_ssz(bytes.to_vec(), record.context, protocol, &self.config)
                    .map_err(|_| RetrievalError::Storage)?;
            if wire.block_root() != root || wire.slot() != slot || wire.index() != index {
                return Err(RetrievalError::Storage);
            }
            Ok(Some(wire))
        })();
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn prune_custody_clock_for_tests(
        &self,
        status: &crate::import_coordinator::AuthenticatedStatus,
    ) -> Result<(), RetrievalError> {
        if status.config_digest() != self.config_digest {
            return Err(RetrievalError::Configuration);
        }
        let mut state = self.inner.lock().map_err(|_| RetrievalError::Storage)?;
        self.ensure_writer(&mut state)?;
        self.prune_history(status.current_slot() / self.config.slots_per_epoch)
    }

    fn ensure_writer(&self, state: &mut ArchiveState) -> Result<(), RetrievalError> {
        if state.index_lock.is_none() {
            fs::create_dir_all(&self.root).map_err(|_| RetrievalError::Storage)?;
            let path = self.root.join("writer.lock");
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|_| RetrievalError::Storage)?;
            lock.try_lock().map_err(|_| RetrievalError::Storage)?;
            RetentionDomain::Sidecars
                .resume(&self.shared, self.config_digest)
                .map_err(|_| RetrievalError::Storage)?;
            state.index_lock = Some(lock);
        }
        Ok(())
    }

    fn read_record(&self, path: &std::path::Path) -> Result<Record, RetrievalError> {
        if fs::metadata(path).map_err(|_| RetrievalError::Storage)?.len() > MAX_RECORD_BYTES {
            return Err(RetrievalError::Storage);
        }
        let bytes = fs::read(path).map_err(|_| RetrievalError::Storage)?;
        let mut record =
            serde_json::from_slice::<Record>(&bytes).map_err(|_| RetrievalError::Storage)?;
        let checksum = record.checksum;
        record.checksum = [0; 32];
        if record.schema != 1 ||
            record.identity != self.shared.identity() ||
            record.config_digest != self.config_digest ||
            record.object.kind != ObjectKind::BeaconDataSidecar ||
            record.object.encoding != Encoding::Ssz ||
            sha256(&serde_json::to_vec(&record).map_err(|_| RetrievalError::Storage)?) !=
                checksum
        {
            return Err(RetrievalError::Storage);
        }
        record.checksum = checksum;
        Ok(record)
    }

    fn prune_history(&self, latest: u64) -> Result<(), RetrievalError> {
        if !self.root.exists() {
            return Ok(());
        }
        let mut count = 0usize;
        for entry in fs::read_dir(&self.root).map_err(|_| RetrievalError::Storage)? {
            let entry = entry.map_err(|_| RetrievalError::Storage)?;
            if entry.file_name().to_str().is_some_and(RetentionDomain::is_operational_file) &&
                entry.file_type().map_err(|_| RetrievalError::Storage)?.is_file()
            {
                continue;
            }
            count += 1;
            if count > MAX_EPOCH_DIRECTORIES ||
                !entry.file_type().map_err(|_| RetrievalError::Storage)?.is_dir()
            {
                return Err(RetrievalError::Storage);
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or(RetrievalError::Storage)?;
            let epoch = name.parse::<u64>().map_err(|_| RetrievalError::Storage)?;
            if name != format!("{epoch:020}") {
                return Err(RetrievalError::Storage);
            }
            if epoch >= latest.saturating_sub(RETAINED_EPOCHS) {
                continue;
            }
            let mut roots = Vec::new();
            for block in fs::read_dir(entry.path()).map_err(|_| RetrievalError::Storage)? {
                let block = block.map_err(|_| RetrievalError::Storage)?;
                if roots.len() as u64 >= self.config.slots_per_epoch * 2 ||
                    !block.file_type().map_err(|_| RetrievalError::Storage)?.is_dir()
                {
                    return Err(RetrievalError::Storage);
                }
                roots.push(
                    parse_root(block.file_name().to_str().ok_or(RetrievalError::Storage)?)
                        .ok_or(RetrievalError::Storage)?,
                );
            }
            RetentionDomain::Sidecars
                .prune_epoch(&self.shared, self.config_digest, epoch, roots)
                .map_err(|_| RetrievalError::Storage)?;
        }
        Ok(())
    }
}

fn owner(root: Root) -> Owner {
    let mut bytes = b"embedded-beacon-sidecar-proof-v1".to_vec();
    bytes.extend_from_slice(&root);
    Owner::Recovery(sha256(&bytes))
}
fn root_name(root: Root) -> String {
    root.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn parse_root(name: &str) -> Option<Root> {
    if name.len() != 64 ||
        !name.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut root = [0; 32];
    for (index, byte) in root.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&name[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(root)
}

/// The pinned DAS helper hashes the 256-bit `NodeID` in little-endian integer encoding and
/// increments with wrapping arithmetic, selecting four distinct sorted native groups.
fn custody_columns(mut node_id: Root) -> Result<BTreeSet<u64>, RetrievalError> {
    node_id.reverse();
    CustodyParameters::default()
        .get_custody_groups(node_id, 4)
        .map(|groups| groups.into_iter().collect())
        .map_err(|_| RetrievalError::Configuration)
}

#[cfg(test)]
mod tests;
