//! Opt-in storage blobs and logical trie reconstruction, isolated from native MDBX tables.

use crate::{version::get_db_version, DatabaseError};
use std::path::Path;

mod cache;
mod codec;
mod cursor;
mod frontier;
mod store;
#[cfg(test)]
mod tests;
pub(crate) use cursor::{Logical, Move};
pub(crate) use store::{Shared, Store, TABLE};

/// Physical MDBX table containing experimental packed storage records.
pub const TABLE_NAME: &str = TABLE;

/// Sentinel retaining every storage trie path; ordinary reconstruction cutoffs remain 1..=8.
pub const FULL_TRIE_DEPTH: usize = 65;

/// Sentinel for policy-versioned adaptive boundaries, never a hashed-key prefix length.
pub const ADAPTIVE_TRIE_DEPTH: usize = 66;
pub(crate) const FRONTIER_TABLE: &str = frontier::TABLE;

/// Experimental on-disk storage encoding. A fresh, explicitly opted-in directory is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackingMode {
    /// Dense offsets and native Compact values.
    Dense,
    /// Independent groups of 32 U256 values, using adaptive frame-of-reference encoding.
    Integer32,
}

impl PackingMode {
    /// Dedicated database version, rejected by stock Reth and by the native backend.
    pub const fn database_version(self) -> u64 {
        match self {
            Self::Dense => 10003,
            Self::Integer32 => 10004,
        }
    }

    /// Adaptive retention has separate versions; existing layouts are never reinterpreted.
    pub const fn database_version_for_depth(self, depth: usize) -> u64 {
        if depth == ADAPTIVE_TRIE_DEPTH {
            self.database_version() + 2
        } else {
            self.database_version()
        }
    }
}

const CONFIG_FILE: &str = "experimental-packing.config";

pub(crate) fn check_directory(
    path: &Path,
    mode: Option<PackingMode>,
    depth: usize,
) -> Result<(), DatabaseError> {
    if mode.is_some() && !valid_depth(depth) {
        return Err(DatabaseError::Other(
            "experimental trie depth must be 1..=8, full or adaptive retention".into(),
        ));
    }
    let version = get_db_version(path);
    if let Some(mode) = mode {
        if version.ok() != Some(mode.database_version_for_depth(depth)) {
            return Err(DatabaseError::Other("experimental packing requires a fresh directory initialized with the matching flag; native databases are never converted in place".into()));
        }
        let marker = reth_fs_util::read_to_string(path.join(CONFIG_FILE))
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        if marker != layout_marker(mode, depth) {
            return Err(DatabaseError::Other(
                "experimental packing mode or trie depth differs from the persisted layout".into(),
            ));
        }
    } else if path.join(CONFIG_FILE).exists() ||
        version.is_ok_and(|v| {
            matches!(v, 10001 | 10002 | 10005 | 10006) ||
                v == PackingMode::Dense.database_version() ||
                v == PackingMode::Integer32.database_version()
        })
    {
        return Err(DatabaseError::Other(
            "experimental database requires its explicit --db.experimental-state-packing flag"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn prepare_directory(path: &Path, mode: PackingMode, depth: usize) -> eyre::Result<()> {
    eyre::ensure!(
        valid_depth(depth),
        "experimental trie depth must be 1..=8, full or adaptive retention"
    );
    if crate::is_database_empty(path) {
        reth_fs_util::create_dir_all(path)?;
        reth_fs_util::write(
            crate::version::db_version_file_path(path),
            mode.database_version_for_depth(depth).to_string(),
        )?;
        reth_fs_util::write(path.join(CONFIG_FILE), layout_marker(mode, depth))?;
    }
    check_directory(path, Some(mode), depth)?;
    Ok(())
}

fn valid_depth(depth: usize) -> bool {
    (1..=8).contains(&depth) || matches!(depth, FULL_TRIE_DEPTH | ADAPTIVE_TRIE_DEPTH)
}

fn layout_marker(mode: PackingMode, depth: usize) -> String {
    let version = mode.database_version_for_depth(depth);
    if depth == ADAPTIVE_TRIE_DEPTH {
        format!("{version}:adaptive:{}", frontier::POLICY_VERSION)
    } else {
        format!("{version}:{depth}")
    }
}
