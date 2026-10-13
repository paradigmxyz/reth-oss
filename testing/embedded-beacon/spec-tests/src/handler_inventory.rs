//! Manifest-only declared-handler coverage; no fixture inputs or handlers are executed.

use super::{checked_relative, declared_handler, Case, FixtureManifest, SPEC_RELEASE};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Exact inventory grouped by declared dispatch support, without conformance credit.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerInventory {
    /// Pinned schema release of the independently authenticated manifest.
    pub spec_release: String,
    /// Unique original cases identified by the manifest file paths.
    pub cases: usize,
    /// One entry for each preset, fork, runner, handler and suite combination.
    pub handlers: Vec<HandlerInventoryEntry>,
    /// Always false: no compressed fixture or YAML inputs are opened by inspection.
    pub fixture_inputs_authenticated: bool,
    /// Always false: dispatch declaration does not prove metadata-specific handlers pass.
    pub fixture_execution_performed: bool,
    /// Always false: this inventory cannot satisfy the full fixture gate.
    pub complete_conformance: bool,
    /// Always false: static dispatch is not independent synchronization evidence.
    pub runtime_synchronization_verified: bool,
}

/// A published suite and the actual runner's declared dispatch support.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerInventoryEntry {
    /// Source preset.
    pub preset: String,
    /// Consensus fork.
    pub fork: String,
    /// Published runner name.
    pub runner: String,
    /// Published handler name.
    pub handler: String,
    /// Published suite name.
    pub suite: String,
    /// Unique original cases in this suite.
    pub cases: usize,
    /// Whether the exact execution dispatch declares support for this suite.
    /// Input-specific artificial contexts can still return unsupported during actual execution.
    pub declared_dispatch: bool,
}

/// Inspect only original file-path metadata using the same dispatch predicate as execution.
/// The caller must authenticate the manifest bytes independently with `load_manifest` first.
/// This avoids fixture scans and preserves every case even when a handler is absent.
pub fn inspect_manifest(manifest: &FixtureManifest) -> Result<HandlerInventory, String> {
    if manifest.spec_release != SPEC_RELEASE {
        return Err("handler inventory uses another schema release".into());
    }
    let mut identities = BTreeSet::new();
    for relative in manifest.files.keys() {
        checked_relative(Path::new(""), relative)?;
        if relative.split('/').count() >= 7 &&
            (relative.ends_with(".yaml") || relative.ends_with(".ssz_snappy"))
        {
            identities.insert(relative.rsplit_once('/').ok_or("fixture lacks case path")?.0);
        }
    }
    if identities.is_empty() {
        return Err("handler inventory contains no original cases".into());
    }
    let mut handlers = BTreeMap::new();
    for relative in &identities {
        let parts = relative.split('/').collect::<Vec<_>>();
        let case = Case {
            path: Path::new(relative).to_path_buf(),
            relative: (*relative).into(),
            preset: parts[0].into(),
            fork: parts[1].into(),
            runner: parts[2].into(),
            handler: parts[3].into(),
            suite: parts[4].into(),
            name: parts[5..].join("/"),
            configuration: Default::default(),
        };
        let key = parts[..5].join("/");
        let entry = handlers.entry(key).or_insert_with(|| HandlerInventoryEntry {
            preset: case.preset.clone(),
            fork: case.fork.clone(),
            runner: case.runner.clone(),
            handler: case.handler.clone(),
            suite: case.suite.clone(),
            cases: 0,
            declared_dispatch: declared_handler(&case),
        });
        entry.cases += 1;
    }
    Ok(HandlerInventory {
        spec_release: manifest.spec_release.clone(),
        cases: identities.len(),
        handlers: handlers.into_values().collect(),
        fixture_inputs_authenticated: false,
        fixture_execution_performed: false,
        complete_conformance: false,
        runtime_synchronization_verified: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_paths_are_never_opened_and_missing_handlers_remain_explicit() {
        let mut manifest = FixtureManifest {
            spec_release: SPEC_RELEASE.into(),
            required_suites: vec![],
            files: BTreeMap::new(),
        };
        for relative in [
            "general/phase0/ssz_generic/boolean/valid/a/serialized.ssz_snappy",
            "general/phase0/ssz_generic/boolean/valid/a/value.yaml",
            "general/phase0/ssz_generic/boolean/valid/b/serialized.ssz_snappy",
            "minimal/gloas/not_implemented/handler/suite/a/data.yaml",
        ] {
            manifest.files.insert(relative.into(), "00".repeat(32));
        }
        let inventory = inspect_manifest(&manifest).unwrap();
        assert_eq!(inventory.cases, 3);
        assert_eq!(inventory.handlers.len(), 2);
        assert_eq!(inventory.handlers[0].cases, 2);
        assert!(inventory.handlers[0].declared_dispatch);
        assert!(!inventory.handlers[1].declared_dispatch);
        assert!(!inventory.fixture_inputs_authenticated);
        assert!(!inventory.fixture_execution_performed);
        assert!(!inventory.complete_conformance);
        assert!(!inventory.runtime_synchronization_verified);
    }

    #[test]
    fn source_release_unsafe_members_and_empty_inventories_are_rejected() {
        let mut manifest = FixtureManifest {
            spec_release: "master".into(),
            required_suites: vec![],
            files: BTreeMap::new(),
        };
        assert_eq!(
            inspect_manifest(&manifest).unwrap_err(),
            "handler inventory uses another schema release"
        );
        manifest.spec_release = SPEC_RELEASE.into();
        assert_eq!(
            inspect_manifest(&manifest).unwrap_err(),
            "handler inventory contains no original cases"
        );
        manifest.files.insert("../outside.yaml".into(), "00".repeat(32));
        assert_eq!(
            inspect_manifest(&manifest).unwrap_err(),
            "invalid relative fixture path: ../outside.yaml"
        );
    }
}
