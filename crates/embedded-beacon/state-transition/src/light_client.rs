//! Fork-aware light-client verification over authenticated bootstrap roots.
//!
//! Merkle inclusion and real sync-committee BLS verification are mandatory. This store
//! implements light-client safety thresholds; it does not authorize full beacon imports.

mod encoding;
mod headers;
mod processing;

pub mod merkle;

pub mod full_node;

pub mod data_collection;
mod types;
mod upgrades;

pub use headers::{get_lc_execution_root, is_valid_light_client_header};
pub use processing::{
    compute_sync_committee_period_at_slot, get_safety_threshold, initialize_light_client_store,
    is_better_update, is_finality_update, is_next_sync_committee_known, is_sync_committee_update,
    is_valid_normalized_merkle_branch, process_light_client_finality_update,
    process_light_client_optimistic_update, process_light_client_store_force_update,
    process_light_client_update, validate_light_client_update,
};
pub use types::{
    Bootstrap, FinalityUpdate, Header, LightClientError, LightClientParameters, LightClientStore,
    OptimisticUpdate, Update,
};
pub use upgrades::{upgrade_bootstrap, upgrade_header, upgrade_store, upgrade_update};

#[cfg(test)]
mod tests;
