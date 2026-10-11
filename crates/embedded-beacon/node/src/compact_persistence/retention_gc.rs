//! Bounded operational recovery of independent retained-proof owner deletion.
//!
//! A descriptor is durably published before namespace unlink. It grants no graph selection,
//! validity or custody; its only scope is one exact numeric epoch in one fixed local namespace.

use reth_embedded_beacon_primitives::{sha256, Root};
use reth_embedded_beacon_shared_data::{
    journal::{ensure_published_durability, publish_file},
    Owner, SharedDataStore, StoreError, StoreIdentity,
};
use reth_fs_util as fs;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

const MAX_DESCRIPTOR_BYTES: u64 = 1024 * 1024;
const MAX_ROOTS: usize = 4096;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RetentionDomain {
    Sidecars,
    Contexts,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Deletion {
    schema: u8,
    identity: StoreIdentity,
    consensus_digest: Root,
    domain: RetentionDomain,
    epoch: u64,
    roots: Vec<Root>,
    checksum: Root,
}
impl RetentionDomain {
    pub(crate) fn directory(self, shared: &SharedDataStore) -> PathBuf {
        shared.namespace_root().join(match self {
            Self::Sidecars => "beacon-sidecars-v1",
            Self::Contexts => "beacon-sidecar-contexts-v1",
        })
    }
    pub(crate) fn owner(self, root: Root) -> Owner {
        let mut bytes = match self {
            Self::Sidecars => b"embedded-beacon-sidecar-proof-v1".to_vec(),
            Self::Contexts => b"embedded-beacon-sidecar-context-v1".to_vec(),
        };
        bytes.extend_from_slice(&root);
        Owner::Recovery(sha256(&bytes))
    }
    pub(crate) fn prune_epoch(
        self,
        shared: &SharedDataStore,
        digest: Root,
        epoch: u64,
        roots: Vec<Root>,
    ) -> Result<(), StoreError> {
        self.resume(shared, digest)?;
        if roots.len() > MAX_ROOTS || roots.iter().collect::<BTreeSet<_>>().len() != roots.len() {
            return Err(StoreError::InvalidPublication)
        }
        let mut descriptor = Deletion {
            schema: 1,
            identity: shared.identity(),
            consensus_digest: digest,
            domain: self,
            epoch,
            roots,
            checksum: [0; 32],
        };
        descriptor.checksum = sha256(&serde_json::to_vec(&descriptor)?);
        let bytes = serde_json::to_vec(&descriptor)?;
        if bytes.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidPublication)
        }
        publish_file(&self.directory(shared).join("pruning.json"), &bytes)?;
        self.resume(shared, digest)
    }
    pub(crate) fn resume(self, shared: &SharedDataStore, digest: Root) -> Result<(), StoreError> {
        shared.ensure_healthy()?;
        let directory = self.directory(shared);
        let pending = directory.join("pruning.pending");
        if pending.exists() && fs::metadata(&pending)?.len() > MAX_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidPublication)
        }
        let path = directory.join("pruning.json");
        if !path.exists() {
            return Ok(())
        }
        if fs::metadata(&path)?.len() > MAX_DESCRIPTOR_BYTES {
            return Err(StoreError::InvalidPublication)
        }
        let mut descriptor: Deletion = serde_json::from_slice(&fs::read(&path)?)?;
        let expected = descriptor.checksum;
        descriptor.checksum = [0; 32];
        if descriptor.schema != 1 ||
            descriptor.identity != shared.identity() ||
            descriptor.consensus_digest != digest ||
            descriptor.domain != self ||
            descriptor.roots.len() > MAX_ROOTS ||
            descriptor.roots.iter().collect::<BTreeSet<_>>().len() != descriptor.roots.len() ||
            sha256(&serde_json::to_vec(&descriptor)?) != expected
        {
            return Err(StoreError::CorruptJournal)
        }
        ensure_published_durability(&path)?;
        // Derive the target from the fixed writer namespace and checked unsigned epoch. No path
        // from serialized data is executed, and no operation touches a different owner domain.
        let epoch = directory.join(format!("{:020}", descriptor.epoch));
        if epoch.exists() {
            fs::remove_dir_all(&epoch)?
        }
        sync_directory(&directory)?;
        shared.release_owners(
            &descriptor.roots.into_iter().map(|root| self.owner(root)).collect::<Vec<_>>(),
        )?;
        fs::remove_file(&path)?;
        sync_directory(&directory)?;
        Ok(())
    }
    pub(crate) fn is_operational_file(name: &str) -> bool {
        matches!(name, "writer.lock" | "pruning.json" | "pruning.pending")
    }
}
#[cfg(unix)]
fn sync_directory(path: &std::path::Path) -> Result<(), StoreError> {
    fs::open(path)?.sync_all().map_err(|error| fs::FsPathError::fsync(error, path))?;
    Ok(())
}

#[cfg(not(unix))]
const fn sync_directory(_: &std::path::Path) -> Result<(), StoreError> {
    Ok(())
}

#[cfg(test)]
mod tests;
