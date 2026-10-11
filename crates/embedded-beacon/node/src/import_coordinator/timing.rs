//! Local timings for completed owner phases; these measurements grant no validation authority.

use alloy_primitives::B256;
use reth_embedded_beacon_fork_choice::store::BlockImportTimings;
use reth_embedded_beacon_primitives::Root;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(super) enum ImportPhase {
    Preparation,
    NativeImport,
    BodyObservations,
    Recording,
    Publication,
}

impl ImportPhase {
    const fn name(self) -> &'static str {
        match self {
            Self::Preparation => "preparation",
            Self::NativeImport => "native_import",
            Self::BodyObservations => "body_observations",
            Self::Recording => "recording",
            Self::Publication => "publication",
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ImportTimings {
    root: Root,
    pub(super) preparation_micros: u64,
    pub(super) native_import_micros: u64,
    pub(super) body_observations_micros: u64,
    pub(super) recording_micros: u64,
    pub(super) publication_micros: u64,
    pub(super) already_admitted: bool,
    pub(super) native: Option<BlockImportTimings>,
}

impl ImportTimings {
    pub(super) fn new(root: Root) -> Self {
        Self { root, ..Self::default() }
    }

    pub(super) fn start(&self, phase: ImportPhase) -> Instant {
        tracing::debug!(target: "beacon::import", root = %B256::from(self.root),
            phase = phase.name(), "[beacon] Starting checked import phase");
        Instant::now()
    }

    pub(super) fn record(&mut self, phase: ImportPhase, started: Instant) {
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let total = match phase {
            ImportPhase::Preparation => &mut self.preparation_micros,
            ImportPhase::NativeImport => &mut self.native_import_micros,
            ImportPhase::BodyObservations => &mut self.body_observations_micros,
            ImportPhase::Recording => &mut self.recording_micros,
            ImportPhase::Publication => &mut self.publication_micros,
        };
        *total = total.saturating_add(micros);
        tracing::debug!(target: "beacon::import", root = %B256::from(self.root),
            phase = phase.name(), elapsed_ms = micros / 1000,
            "[beacon] Completed checked import phase");
    }
}
