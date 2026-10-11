//! Identity-bound DAS sampling for the pinned 128-column network.
//!
//! The DAS custody-sampling rule requires `max(SAMPLES_PER_SLOT, custody_group_count)`
//! groups, including every custody group. For this profile that is eight samples and
//! four custody groups; selecting columns alone never establishes availability.

use super::*;
use reth_embedded_beacon_network::custody::CustodyParameters;
use std::{collections::BTreeSet, sync::OnceLock};

const SAMPLE_GROUPS: u64 = 8;
const CUSTODY_GROUPS: u64 = 4;
const MAX_COLUMNS: usize = 128;

/// Immutable sample selection bound to the actual persisted discovery identity.
#[derive(Debug, Default)]
pub(super) struct SamplingSelection {
    identity: Mutex<Option<Root>>,
    columns: OnceLock<Vec<u64>>,
}

impl SamplingSelection {
    /// Bind once; a foreign identity cannot replace an existing selection.
    pub(super) fn bind(&self, node_id: Root) -> Result<(), RetrievalError> {
        let mut identity = self.identity.lock().map_err(|_| RetrievalError::Closed)?;
        if let Some(bound) = *identity {
            return if bound == node_id { Ok(()) } else { Err(RetrievalError::Association) };
        }
        let columns = selected_columns(node_id)?;
        self.columns.set(columns).map_err(|_| RetrievalError::Closed)?;
        *identity = Some(node_id);
        Ok(())
    }

    /// All eight selected columns remain mandatory inputs to independent proof verification.
    pub(super) fn columns(&self) -> Result<Vec<u64>, RetrievalError> {
        let identity = self.identity.lock().map_err(|_| RetrievalError::Closed)?;
        if identity.is_none() {
            return Err(RetrievalError::Missing);
        }
        self.columns.get().cloned().ok_or(RetrievalError::Closed)
    }
}

fn selected_columns(mut node_id: Root) -> Result<Vec<u64>, RetrievalError> {
    // ENR node IDs encode the full integer big-endian; the pinned DAS helper takes little-endian.
    node_id.reverse();
    let parameters = CustodyParameters::default();
    let groups = parameters
        .get_custody_groups(node_id, SAMPLE_GROUPS.max(CUSTODY_GROUPS))
        .map_err(|_| RetrievalError::Configuration)?;
    let custody = parameters
        .get_custody_groups(node_id, CUSTODY_GROUPS)
        .map_err(|_| RetrievalError::Configuration)?;
    if groups.len() != SAMPLE_GROUPS as usize ||
        custody.len() != CUSTODY_GROUPS as usize ||
        !custody.iter().all(|group| groups.contains(group))
    {
        return Err(RetrievalError::Configuration);
    }
    let mut columns = BTreeSet::new();
    for group in groups {
        columns.extend(
            parameters
                .compute_columns_for_custody_group(group)
                .map_err(|_| RetrievalError::Configuration)?,
        );
    }
    if columns.len() != SAMPLE_GROUPS as usize ||
        columns.len() > MAX_COLUMNS ||
        columns.iter().any(|column| *column >= MAX_COLUMNS as u64)
    {
        return Err(RetrievalError::Configuration);
    }
    Ok(columns.into_iter().collect())
}

#[cfg(test)]
mod tests;
