//! Opt-in storage blobs and logical trie reconstruction, isolated from native MDBX tables.

use crate::{version::get_db_version, DatabaseError};
use std::path::Path;

mod cache;
mod codec;
mod cursor;
mod store;
#[cfg(test)]
mod tests;
pub(crate) use cursor::{Logical, Move};
pub(crate) use store::{Shared, Store, TABLE};

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
}

const CONFIG_FILE: &str = "experimental-packing.config";

pub(crate) fn check_directory(
    path: &Path,
    mode: Option<PackingMode>,
    depth: usize,
) -> Result<(), DatabaseError> {
    if mode.is_some() && !(1..=8).contains(&depth) {
        return Err(DatabaseError::Other("experimental trie depth must be 1..=8".into()));
    }
    let version = get_db_version(path);
    if let Some(mode) = mode {
        if version.ok() != Some(mode.database_version()) {
            return Err(DatabaseError::Other("experimental packing requires a fresh directory initialized with the matching flag; native databases are never converted in place".into()));
        }
        let marker = reth_fs_util::read_to_string(path.join(CONFIG_FILE))
            .map_err(|e| DatabaseError::Other(e.to_string()))?;
        if marker != format!("{}:{depth}", mode.database_version()) {
            return Err(DatabaseError::Other(
                "experimental packing mode or trie depth differs from the persisted layout".into(),
            ));
        }
    } else if path.join(CONFIG_FILE).exists() ||
        version.is_ok_and(|v| {
            matches!(v, 10001 | 10002) ||
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
    eyre::ensure!((1..=8).contains(&depth), "experimental trie depth must be 1..=8");
    if crate::is_database_empty(path) {
        reth_fs_util::create_dir_all(path)?;
        reth_fs_util::write(
            crate::version::db_version_file_path(path),
            mode.database_version().to_string(),
        )?;
        reth_fs_util::write(
            path.join(CONFIG_FILE),
            format!("{}:{depth}", mode.database_version()),
        )?;
    }
    check_directory(path, Some(mode), depth)?;
    Ok(())
}
