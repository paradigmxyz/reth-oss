//! Durable custody columns accept only the graph owner's sealed native gossip receipt.
//!
//! A public Accept enum or remote SSZ cannot enter this callback. Publication completes before
//! the owner commits its prepared seen cache; infrastructure failures leave retries unpoisoned.

use super::*;
use crate::import_coordinator::{ColumnRetention, ImportError, ValidatedColumn};

impl ColumnRetention for RetrievalAvailability {
    fn config_digest(&self) -> Root {
        self.digest
    }
    fn retain(&self, receipt: &ValidatedColumn) -> Result<(), ImportError> {
        if receipt.config_digest() != self.digest {
            return Err(ImportError::Configuration)
        }
        let Some(archive) = &self.archive else { return Ok(()) };
        let Some((_, columns)) = archive.selection().map_err(serving::retention_error)? else {
            return Err(ImportError::Configuration)
        };
        if receipt.original().kind() ==
            reth_embedded_beacon_network::availability_rpc::SidecarKind::Column &&
            !columns.contains(&receipt.original().index())
        {
            return Ok(())
        }
        let result = archive.retain_verified(&[receipt.original().clone()]);
        self.check_health(&result);
        result.map_err(serving::retention_error)
    }
    fn ensure_healthy(&self) -> Result<(), ImportError> {
        if self.halted.load(Ordering::Acquire) {
            return Err(ImportError::RecoveryRequired)
        }
        if let Some(archive) = &self.archive {
            archive.selection().map_err(serving::retention_error)?;
        }
        Ok(())
    }
}
