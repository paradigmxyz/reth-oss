//! Pinned consensus fixture discovery with failure accounting for every discovered case.

use reth_embedded_beacon_crypto::{
    aggregate_pubkeys, aggregate_verify, eth_fast_aggregate_verify, fast_aggregate_verify,
    verify_signature,
};
use reth_embedded_beacon_fork_choice::{StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{
    bytes_root,
    gloas::{
        DecodeLimits, ExecutionPayloadBid, ExecutionRequests, IndexedPayloadAttestation,
        PayloadAttestation, PayloadAttestationData, PayloadAttestationMessage,
        SignedExecutionPayloadBid,
    },
    merkleize, merkleize_progressive, mix_in_length, pack_bytes, read_bool, BeaconBlockHeader,
    BitList, BitVector, Fork, Root,
};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, compute_shuffled_indices,
    deneb_processing, electra_processing, fork_processing, fulu_processing, gloas_processing,
    phase0_processing,
    ssz::{
        altair, bellatrix, capella, deneb, electra, fulu, gloas,
        gloas::{ExecutionPayload, ExecutionPayloadEnvelope, SignedExecutionPayloadEnvelope},
        phase0, BeaconBlock, BeaconState, Preset as SszPreset, SignedBeaconBlock,
    },
};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::Read,
    path::{Component, Path, PathBuf},
    task::{Context, Poll, Waker},
    time::Instant,
};

mod attestation_gossip;
mod authenticated_release;
mod authentication;
mod blob_sidecars;
mod block_gossip;
mod block_sequences;
mod case_execution;
mod fast_confirmation;
mod fork_choice_availability;
mod fork_choice_merge;
mod fork_choice_store;
mod full_corpus;
mod fulu_columns;
mod genesis;
mod gloas_bids;
mod gloas_columns;
mod gloas_fork_choice;
mod gloas_payload_gossip;
mod gossip_operations;
mod handler_inventory;
mod light_client;
mod light_client_collection;
mod merkle_proof;
mod networking;
mod optimistic_sync;
mod processing_altair;
mod processing_bellatrix;
mod processing_capella;
mod processing_deneb;
mod processing_electra;
mod processing_fulu;
mod processing_gloas;
mod processing_phase0;
mod rewards;
mod ssz_static;
mod sync_committee_gossip;

pub use authenticated_release::{
    load_release_source, run_authenticated_release, AuthenticatedReleaseReport, ReleaseCorpus,
    ReleaseSourceRecord,
};

pub use full_corpus::{
    run_complete, CorpusAuthentication, FullCorpusEvidence, COMPLIANCE_MANIFEST_SHA256,
    PRIMARY_MANIFEST_SHA256,
};

pub use handler_inventory::{inspect_manifest, HandlerInventory, HandlerInventoryEntry};

use case_execution::execute_cases;
use processing_altair::run_altair_processing;
use processing_bellatrix::run_bellatrix_processing;
use processing_capella::run_capella_processing;
use processing_deneb::run_deneb_processing;
use processing_electra::run_electra_processing;
use processing_fulu::run_fulu_processing;
use processing_gloas::{fixture_gloas_parameters, run_gloas_processing};
use processing_phase0::run_phase0_processing;

/// The source and fixture release accepted by this runner.
pub const SPEC_RELEASE: &str = "v1.7.0-beta.3";
/// Historical forks that must all appear in a complete conformance run.
pub const FORKS: [&str; 8] =
    ["phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"];

/// Maximum native workers independently authenticating original fixture files.
pub const MAX_AUTHENTICATION_WORKERS: usize = 32;
/// Maximum independent whole-case workers selected explicitly by the caller.
pub const MAX_CASE_WORKERS: usize = 32;

/// Caller-trusted fixture inventory. A digest must be supplied independently when running the CLI.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureManifest {
    /// Exact spec release, never a moving branch.
    pub spec_release: String,
    /// Relative suite paths that the fixture source says must be present.
    pub required_suites: Vec<String>,
    /// Every fixture file's SHA-256, keyed by slash-separated relative path.
    pub files: BTreeMap<String, String>,
}

/// Fixture execution controls.
#[derive(Clone, Debug)]
pub struct RunOptions {
    /// Focused runs never assert complete protocol conformance.
    pub focused: bool,
    /// Maximum size of compressed, decompressed or YAML case data.
    pub max_case_bytes: usize,
    /// Emit execution progress after this many cases; zero disables it.
    pub progress_every: usize,
    /// Native authentication workers; every original file is checked independently.
    /// The accepted range is one through 32.
    pub authentication_workers: usize,
    /// Independent whole-case workers, from one through 32. Defaults to one to bound
    /// large mainnet state allocations; events inside each case remain ordered.
    pub case_workers: usize,
}

/// Machine-readable output, including unsupported cases as failures.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    /// Pinned spec release.
    pub spec_release: String,
    /// Whether the inventory and complete matrix were required.
    pub complete_matrix: bool,
    /// Inventory, coverage and setup errors.
    pub errors: Vec<String>,
    /// One outcome for every discovered case, without skip accounting.
    pub cases: Vec<CaseResult>,
    /// Fresh authentication identities of both full source corpora.
    /// Historical and focused reports have no complete-corpus evidence.
    #[serde(default)]
    pub full_corpus: Option<FullCorpusEvidence>,
}

/// A fixture outcome.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseResult {
    /// Slash-separated path from the fixture root.
    pub path: String,
    /// Runner result.
    pub outcome: Outcome,
    /// Exact failure reason; empty for passing cases.
    pub message: String,
    /// Wall time of this handler.
    pub seconds: f64,
    /// Explicit artificial fixture context; this is never a production validation proof.
    #[serde(default)]
    pub spec_only_bls_disabled: bool,
    /// The consensus fixture selects a private mock execution engine, not actual EL verification.
    #[serde(default)]
    pub spec_only_execution_mock: bool,
    /// Explicit private availability assumption in a fixture graph, never real proof coverage.
    #[serde(default)]
    pub spec_only_data_availability_mock: bool,
    /// Explicit fixture checkpoint/graph observations; these never establish runtime authority.
    #[serde(default)]
    pub spec_only_asserted_graph: bool,
}

/// Required handlers that do not exist are visibly distinct failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// All supported assertions passed.
    Passed,
    /// Assertions, input parsing or fixture integrity failed.
    Failed,
    /// The implementation does not yet support this required handler.
    Unsupported,
}

#[derive(Clone, Debug)]
struct Case {
    path: PathBuf,
    relative: String,
    preset: String,
    fork: String,
    runner: String,
    handler: String,
    suite: String,
    name: String,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            focused: false,
            max_case_bytes: 64 * 1024 * 1024,
            progress_every: 0,
            case_workers: 1,
            authentication_workers: std::thread::available_parallelism()
                .map_or(1, |parallelism| parallelism.get().min(MAX_AUTHENTICATION_WORKERS)),
        }
    }
}

impl Report {
    /// Success requires nonempty tests, no setup errors and no unsupported cases.
    pub fn passed(&self) -> bool {
        !self.cases.is_empty() &&
            self.errors.is_empty() &&
            self.cases.iter().all(|case| case.outcome == Outcome::Passed)
    }

    /// Complete fixture conformance requires a fresh exact full corpus and every passing case.
    /// Fixture assumptions remain visible; independent runtime synchronization is a separate gate.
    pub fn conformance_passed(&self) -> bool {
        full_corpus::report_is_complete(self)
    }

    /// `JUnit` deliberately records unsupported handlers as failures, never skipped tests.
    pub fn junit(&self) -> String {
        let failures = self.errors.len() +
            self.cases.iter().filter(|case| case.outcome != Outcome::Passed).count();
        let mut out = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"embedded-beacon-conformance\" tests=\"{}\" failures=\"{}\">\n", self.cases.len() + self.errors.len(), failures);
        for (index, error) in self.errors.iter().enumerate() {
            out.push_str(&format!(
                "<testcase name=\"setup-{index}\"><failure message=\"{}\"/></testcase>\n",
                xml_escape(error)
            ));
        }
        for case in &self.cases {
            out.push_str(&format!(
                "<testcase name=\"{}\" time=\"{}\">",
                xml_escape(&case.path),
                case.seconds
            ));
            out.push_str(&format!(
                "<properties><property name=\"spec_only_bls_disabled\" value=\"{}\"/><property name=\"spec_only_execution_mock\" value=\"{}\"/><property name=\"spec_only_data_availability_mock\" value=\"{}\"/><property name=\"spec_only_asserted_graph\" value=\"{}\"/></properties>",
                case.spec_only_bls_disabled,
                case.spec_only_execution_mock,
                case.spec_only_data_availability_mock,
                case.spec_only_asserted_graph,
            ));
            if case.outcome != Outcome::Passed {
                out.push_str(&format!(
                    "<failure type=\"{:?}\" message=\"{}\"/>",
                    case.outcome,
                    xml_escape(&case.message)
                ));
            }
            out.push_str("</testcase>\n");
        }
        out.push_str("</testsuite>\n");
        out
    }
}

/// Decode a separately authenticated manifest with a fixed 512 MiB byte budget.
/// Hashing and parsing consume the same stream; corpus case limits remain independent.
/// The larger manifest bound accommodates every published compliance file without reading
/// the complete encoded manifest into an additional allocation.
pub fn load_manifest(path: &Path, expected: Root) -> Result<FixtureManifest, String> {
    load_manifest_with_limit(path, expected, 512 * 1024 * 1024)
}

fn load_manifest_with_limit(
    path: &Path,
    expected: Root,
    maximum: u64,
) -> Result<FixtureManifest, String> {
    let file = reth_fs_util::open(path).map_err(|error| error.to_string())?;
    if file.metadata().map_err(|error| error.to_string())?.len() > maximum {
        return Err("manifest exceeds byte limit".into());
    }
    let mut reader = DigestReader {
        reader: std::io::BufReader::new(
            file.take(maximum.checked_add(1).ok_or("manifest limit overflow")?),
        ),
        hasher: Sha256::new(),
        bytes: 0,
    };
    let manifest = serde_json::from_reader::<_, FixtureManifest>(&mut reader)
        .map_err(|error| error.to_string())?;
    if reader.bytes > maximum {
        return Err("manifest exceeds byte limit".into());
    }
    if Root::from(reader.hasher.finalize()) != expected {
        return Err("manifest SHA-256 differs from separately supplied digest".into());
    }
    if manifest.files.len() > 2_000_000 || manifest.required_suites.len() > 100_000 {
        return Err("manifest exceeds entry limit".into());
    }
    Ok(manifest)
}

struct DigestReader<R> {
    reader: R,
    hasher: Sha256,
    bytes: u64,
}
impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.reader.read(buffer)?;
        self.hasher.update(&buffer[..count]);
        self.bytes = self
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| std::io::Error::other("manifest byte count overflow"))?;
        Ok(count)
    }
}

/// Build a complete local integrity inventory. This does not establish release authenticity;
/// obtain the release and expected digest independently before trusting this manifest.
pub fn inventory(root: &Path) -> Result<FixtureManifest, String> {
    let mut files = BTreeMap::new();
    let mut suites = BTreeSet::new();
    for path in walk_files(root)? {
        let relative = relative_path(root, &path)?;
        files.insert(relative, hex(&hash_file(&path)?));
    }
    for case in discover(root)? {
        suites.insert(format!(
            "{}/{}/{}/{}/{}",
            case.preset, case.fork, case.runner, case.handler, case.suite
        ));
    }
    Ok(FixtureManifest {
        spec_release: SPEC_RELEASE.into(),
        required_suites: suites.into_iter().collect(),
        files,
    })
}

/// Verify every inventory byte before executing any handler.
/// Use [`run_complete`] with both pinned source corpora for complete conformance.
pub fn run(root: &Path, manifest: &FixtureManifest, options: &RunOptions) -> Report {
    let mut report = empty_report(!options.focused);
    let cases = match authenticated_cases(root, manifest, options) {
        Ok(cases) => cases,
        Err(error) => {
            report.errors.push(error);
            return report;
        }
    };
    if cases.is_empty() {
        report.errors.push("no fixture cases discovered".into());
        return report;
    }
    if !options.focused {
        report.errors.push("complete conformance requires both pinned primary and compliance fixture corpora; use run-complete".into());
        check_full_matrix(&cases, &mut report.errors);
    }
    check_required_suites(&cases, manifest, &mut report.errors);
    execute_cases(&mut report, cases, options);
    report
}

fn empty_report(complete_matrix: bool) -> Report {
    Report {
        spec_release: SPEC_RELEASE.into(),
        complete_matrix,
        errors: Vec::new(),
        cases: Vec::new(),
        full_corpus: None,
    }
}

fn authenticated_cases(
    root: &Path,
    manifest: &FixtureManifest,
    options: &RunOptions,
) -> Result<Vec<Case>, String> {
    let files = authentication::verify_inventory(
        root,
        manifest,
        options.authentication_workers,
        options.progress_every != 0,
    )?;
    discover_files(root, &files)
}

fn check_full_matrix(cases: &[Case], errors: &mut Vec<String>) {
    let present = cases
        .iter()
        .map(|case| (case.preset.as_str(), case.fork.as_str()))
        .collect::<BTreeSet<_>>();
    for preset in ["minimal", "mainnet"] {
        for fork in FORKS {
            if !present.contains(&(preset, fork)) {
                errors.push(format!("missing required preset/fork: {preset}/{fork}"));
            }
        }
    }
    if !cases.iter().any(|case| case.preset == "general") {
        errors.push("missing general fixture suite".into());
    }
}

fn check_required_suites(cases: &[Case], manifest: &FixtureManifest, errors: &mut Vec<String>) {
    let present = cases
        .iter()
        .map(|case| {
            format!("{}/{}/{}/{}/{}", case.preset, case.fork, case.runner, case.handler, case.suite)
        })
        .collect::<BTreeSet<_>>();
    for required in &manifest.required_suites {
        if !present.contains(required) {
            errors.push(format!("missing or empty required suite: {required}"));
        }
    }
}

fn execute_case_result(case: &Case, max_case_bytes: usize) -> CaseResult {
    let started = Instant::now();
    let result = execute_case(case, max_case_bytes);
    let (outcome, message) = match result {
        Ok(()) => (Outcome::Passed, String::new()),
        Err((outcome, message)) => (outcome, message),
    };
    let fulu_artificial_context = fulu_columns::uses_artificial_context(case, max_case_bytes);
    let spec_only_bls_disabled = fixture_disables_bls(case, max_case_bytes).unwrap_or(false) ||
        fulu_artificial_context ||
        fast_confirmation::supports(case);
    let gloas_fixture_envelope = case.fork == "gloas" &&
        matches!(
            case.runner.as_str(),
            "fork_choice" | "fork_choice_compliance" | "fast_confirmation"
        ) &&
        yaml(&case.path.join("steps.yaml"), max_case_bytes)
            .ok()
            .and_then(|value| {
                value
                    .as_sequence()
                    .map(|steps| steps.iter().any(|step| step.get("execution_payload").is_some()))
            })
            .unwrap_or(false);
    let gloas_bid_execution_mock = gloas_bids::uses_execution_mock(case, max_case_bytes);
    let gloas_bid_asserted_graph = gloas_bids::uses_spec_store(case, max_case_bytes) &&
        gloas_bids::uses_asserted_graph(case, max_case_bytes);
    let spec_only_execution_mock = outcome != Outcome::Unsupported &&
        matches!(
            case.fork.as_str(),
            "bellatrix" | "capella" | "deneb" | "electra" | "fulu" | "gloas"
        ) &&
        ((matches!(
            case.runner.as_str(),
            "fork_choice" | "fork_choice_compliance" | "fast_confirmation"
        ) && case.fork != "gloas") ||
            gloas_fixture_envelope ||
            case.runner == "transition" ||
            case.runner == "sanity" && case.handler == "blocks" && case.fork != "gloas" ||
            case.runner == "operations" && case.handler == "execution_payload" ||
            case.runner == "light_client" && case.handler == "data_collection" ||
            fulu_columns::uses_execution_mock(case, max_case_bytes) ||
            gloas_bid_execution_mock ||
            block_sequences::uses_execution_mock(case) ||
            blob_sidecars::uses_execution_mock(case, max_case_bytes) ||
            block_gossip::uses_execution_mock(case, max_case_bytes) ||
            attestation_gossip::uses_execution_mock(case, max_case_bytes) ||
            optimistic_sync::supports(case));
    let spec_only_asserted_graph = fulu_artificial_context ||
        gloas_bid_asserted_graph ||
        gossip_operations::uses_asserted_graph(case, max_case_bytes) ||
        gloas_payload_gossip::uses_asserted_graph(case, max_case_bytes) ||
        blob_sidecars::uses_asserted_graph(case, max_case_bytes) ||
        block_gossip::uses_asserted_graph(case, max_case_bytes) ||
        attestation_gossip::uses_asserted_graph(case, max_case_bytes);
    CaseResult {
        path: case.relative.clone(),
        outcome,
        message,
        seconds: started.elapsed().as_secs_f64(),
        spec_only_bls_disabled,
        spec_only_execution_mock,
        spec_only_data_availability_mock: outcome != Outcome::Unsupported &&
            (gloas_fixture_envelope || gloas_bid_execution_mock),
        spec_only_asserted_graph,
    }
}

fn hash_file(path: &Path) -> Result<Root, String> {
    let mut file = reth_fs_util::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count =
            file.read(&mut buffer).map_err(|error| format!("{}: {error}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize().into())
}

fn checked_relative(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let path = Path::new(relative);
    if path.is_absolute() ||
        path.components().any(|component| !matches!(component, Component::Normal(_))) ||
        relative.contains('\\')
    {
        return Err(format!("invalid relative fixture path: {relative}"));
    }
    Ok(root.join(path))
}

fn relative_path(root: &Path, path: &Path) -> Result<String, String> {
    Ok(path
        .strip_prefix(root)
        .map_err(|error| error.to_string())?
        .to_str()
        .ok_or("non-UTF8 fixture path")?
        .replace('\\', "/"))
}

fn walk_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!("fixture symlink is not allowed: {}", path.display()));
        }
        if metadata.is_dir() {
            for entry in reth_fs_util::read_dir(&path).map_err(|error| error.to_string())? {
                pending.push(entry.map_err(|error| error.to_string())?.path());
            }
        } else if metadata.is_file() {
            files.push(path);
        } else {
            return Err("unsupported fixture filesystem entry".into());
        }
        if pending.len() + files.len() > 2_000_000 {
            return Err("fixture inventory exceeds entry limit".into());
        }
    }
    files.sort();
    Ok(files)
}

fn discover(root: &Path) -> Result<Vec<Case>, String> {
    discover_files(root, &walk_files(root)?)
}

// Discovery reuses the exact symlink-free inventory after every member checksum passed.
// This avoids a second full metadata scan of large authenticated compliance corpora.
fn discover_files(root: &Path, files: &[PathBuf]) -> Result<Vec<Case>, String> {
    let mut directories = BTreeSet::new();
    for path in files {
        let relative = relative_path(root, path)?;
        if relative.split('/').count() >= 7 &&
            (relative.ends_with(".yaml") || relative.ends_with(".ssz_snappy"))
        {
            directories
                .insert(path.parent().ok_or("fixture lacks parent directory")?.to_path_buf());
        }
    }
    directories
        .into_iter()
        .map(|path| {
            let relative = relative_path(root, &path)?;
            let parts = relative.split('/').collect::<Vec<_>>();
            if parts.len() < 6 {
                return Err(format!("invalid fixture hierarchy: {relative}"));
            }
            Ok(Case {
                path,
                relative: relative.clone(),
                preset: parts[0].into(),
                fork: parts[1].into(),
                runner: parts[2].into(),
                handler: parts[3].into(),
                suite: parts[4].into(),
                name: parts[5..].join("/"),
            })
        })
        .collect()
}

fn declared_handler(case: &Case) -> bool {
    let supported = matches!(
        (case.runner.as_str(), case.handler.as_str()),
        ("shuffling", "core") |
            (
                "bls",
                "verify" |
                    "fast_aggregate_verify" |
                    "aggregate_verify" |
                    "eth_fast_aggregate_verify" |
                    "eth_aggregate_pubkeys"
            ) |
            (
                "ssz_generic",
                "boolean" |
                    "uints" |
                    "basic_vector" |
                    "basic_progressive_list" |
                    "bitvector" |
                    "bitlist" |
                    "progressive_bitlist"
            )
    );
    let supported = supported ||
        (case.fork == "phase0" &&
            matches!(case.preset.as_str(), "mainnet" | "minimal") &&
            ((case.runner == "sanity" &&
                matches!(case.handler.as_str(), "slots" | "blocks")) ||
                (case.runner == "operations" &&
                    matches!(
                        case.handler.as_str(),
                        "block_header" |
                            "attestation" |
                            "attester_slashing" |
                            "proposer_slashing" |
                            "deposit" |
                            "voluntary_exit"
                    )) ||
                (case.runner == "epoch_processing" &&
                    matches!(
                        case.handler.as_str(),
                        "justification_and_finalization" |
                            "rewards_and_penalties" |
                            "registry_updates" |
                            "slashings" |
                            "eth1_data_reset" |
                            "effective_balance_updates" |
                            "slashings_reset" |
                            "randao_mixes_reset" |
                            "historical_roots_update" |
                            "participation_record_updates"
                    ))));
    let supported = supported ||
        (case.fork == "altair" &&
            matches!(case.preset.as_str(), "mainnet" | "minimal") &&
            ((case.runner == "fork" && case.handler == "fork") ||
                (case.runner == "sanity" &&
                    matches!(case.handler.as_str(), "slots" | "blocks")) ||
                (case.runner == "operations" &&
                    matches!(
                        case.handler.as_str(),
                        "block_header" |
                            "attestation" |
                            "attester_slashing" |
                            "proposer_slashing" |
                            "deposit" |
                            "voluntary_exit" |
                            "sync_aggregate"
                    )) ||
                (case.runner == "epoch_processing" &&
                    matches!(
                        case.handler.as_str(),
                        "justification_and_finalization" |
                            "inactivity_updates" |
                            "rewards_and_penalties" |
                            "registry_updates" |
                            "slashings" |
                            "eth1_data_reset" |
                            "effective_balance_updates" |
                            "slashings_reset" |
                            "randao_mixes_reset" |
                            "historical_roots_update" |
                            "participation_flag_updates" |
                            "sync_committee_updates"
                    ))));
    let supported = supported ||
        (matches!(
            case.fork.as_str(),
            "altair" | "bellatrix" | "capella" | "deneb" | "electra" | "fulu"
        ) && matches!(case.preset.as_str(), "minimal" | "mainnet") &&
            case.runner == "fork_choice" &&
            matches!(
                case.handler.as_str(),
                "get_head" |
                    "get_proposer_head" |
                    "on_block" |
                    "ex_ante" |
                    "reorg" |
                    "withholding" |
                    "deposit_with_reorg" |
                    "on_merge_block"
            ));
    let supported = supported ||
        (matches!(
            case.fork.as_str(),
            "altair" | "bellatrix" | "capella" | "deneb" | "electra" | "fulu"
        ) && case.preset == "minimal" &&
            case.runner == "fork_choice_compliance" &&
            matches!(
                case.handler.as_str(),
                "attester_slashing_test" |
                    "block_cover_test" |
                    "block_tree_test" |
                    "block_weight_test" |
                    "invalid_message_test" |
                    "shuffling_test"
            ));
    let supported = supported ||
        (matches!(
            case.fork.as_str(),
            "bellatrix" | "capella" | "deneb" | "electra" | "fulu" | "gloas"
        ) && matches!(case.preset.as_str(), "mainnet" | "minimal") &&
            ((case.runner == "fork" && case.handler == "fork") ||
                (case.runner == "sanity" &&
                    matches!(case.handler.as_str(), "slots" | "blocks")) ||
                (case.runner == "operations" &&
                    matches!(
                        case.handler.as_str(),
                        "block_header" |
                            "attestation" |
                            "attester_slashing" |
                            "proposer_slashing" |
                            "deposit" |
                            "voluntary_exit" |
                            "sync_aggregate" |
                            "execution_payload" |
                            "withdrawals" |
                            "bls_to_execution_change" |
                            "deposit_request" |
                            "withdrawal_request" |
                            "consolidation_request" |
                            "builder_deposit_request" |
                            "builder_exit_request" |
                            "execution_payload_bid" |
                            "parent_execution_payload" |
                            "payload_attestation" |
                            "voluntary_exit_churn"
                    )) ||
                (case.runner == "epoch_processing" &&
                    matches!(
                        case.handler.as_str(),
                        "justification_and_finalization" |
                            "inactivity_updates" |
                            "rewards_and_penalties" |
                            "registry_updates" |
                            "slashings" |
                            "eth1_data_reset" |
                            "effective_balance_updates" |
                            "slashings_reset" |
                            "randao_mixes_reset" |
                            "historical_roots_update" |
                            "historical_summaries_update" |
                            "pending_deposits" |
                            "pending_deposits_churn" |
                            "builder_pending_payments" |
                            "ptc_window" |
                            "pending_consolidations" |
                            "proposer_lookahead" |
                            "participation_flag_updates" |
                            "sync_committee_updates"
                    ))));
    let supported = supported ||
        (case.runner == "ssz_static" &&
            ((FORKS.contains(&case.fork.as_str()) &&
                matches!(case.preset.as_str(), "minimal" | "mainnet") &&
                matches!(
                    case.handler.as_str(),
                    "BeaconState" | "BeaconBlock" | "SignedBeaconBlock"
                )) ||
                case.handler == "BeaconBlockHeader" ||
                (case.fork == "gloas" &&
                    matches!(
                        case.handler.as_str(),
                        "ExecutionPayloadBid" |
                            "SignedExecutionPayloadBid" |
                            "PayloadAttestationData" |
                            "PayloadAttestation" |
                            "PayloadAttestationMessage" |
                            "IndexedPayloadAttestation" |
                            "ExecutionPayload" |
                            "ExecutionRequests" |
                            "ExecutionPayloadEnvelope" |
                            "SignedExecutionPayloadEnvelope"
                    ))));
    let supported = supported ||
        (case.runner == "ssz_static" &&
            schema_fork(case).ok().zip(schema_preset(case).ok()).is_some_and(
                |(fork, preset)| ssz_static::supports(fork, preset, &case.handler),
            ));
    let supported = supported ||
        case.runner == "transition" &&
            case.handler == "core" &&
            FORKS.contains(&case.fork.as_str()) &&
            matches!(case.preset.as_str(), "mainnet" | "minimal");
    supported ||
        attestation_gossip::supports(case) ||
        blob_sidecars::supports(case) ||
        block_gossip::supports(case) ||
        optimistic_sync::supports(case) ||
        block_sequences::supports(case) ||
        fast_confirmation::supports(case) ||
        genesis::supports(case) ||
        gossip_operations::supports(case) ||
        gloas_payload_gossip::supports(case) ||
        sync_committee_gossip::supports(case) ||
        gloas_bids::supports(case) ||
        networking::supports(case) ||
        rewards::supports(case) ||
        light_client::supports(case) ||
        merkle_proof::supports(case) ||
        gloas_fork_choice::supports(case) ||
        gloas_columns::supports(case) ||
        fulu_columns::supports(case) ||
        light_client_collection::supports(case)
}

fn execute_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    if !declared_handler(case) {
        return Err((
            Outcome::Unsupported,
            format!("required handler is not implemented: {}/{}", case.runner, case.handler),
        ));
    }
    let case_manifest = case.path.join("manifest.yaml");
    if case_manifest.is_file() {
        let metadata = yaml(&case_manifest, max_bytes).map_err(|error| (Outcome::Failed, error))?;
        for (field, expected) in [
            ("preset", &case.preset),
            ("fork", &case.fork),
            ("runner", &case.runner),
            ("handler", &case.handler),
            ("suite", &case.suite),
            ("case", &case.name),
        ] {
            if metadata[field].as_str() != Some(expected.as_str()) {
                return Err((Outcome::Failed, format!("case manifest identity mismatch: {field}")));
            }
        }
    }
    if attestation_gossip::supports(case) {
        return attestation_gossip::run_case(case, max_bytes);
    }
    if block_gossip::supports(case) {
        return block_gossip::run_case(case, max_bytes);
    }
    if blob_sidecars::supports(case) {
        return blob_sidecars::run_case(case, max_bytes);
    }
    if optimistic_sync::supports(case) {
        return optimistic_sync::run_case(case, max_bytes);
    }
    if block_sequences::supports(case) {
        return block_sequences::run_case(case, max_bytes);
    }
    if fast_confirmation::supports(case) {
        return fast_confirmation::run_case(case, max_bytes);
    }
    if genesis::supports(case) {
        return genesis::run_case(case, max_bytes);
    }
    if gloas_payload_gossip::supports(case) {
        return gloas_payload_gossip::run_case(case, max_bytes);
    }
    if sync_committee_gossip::supports(case) {
        return sync_committee_gossip::run_case(case, max_bytes);
    }
    if gossip_operations::supports(case) {
        return gossip_operations::run_case(case, max_bytes);
    }
    if gloas_bids::supports(case) {
        return gloas_bids::run_case(case, max_bytes);
    }
    if fulu_columns::supports(case) {
        return fulu_columns::run_case(case, max_bytes);
    }
    if light_client_collection::supports(case) {
        return light_client_collection::run_case(case, max_bytes);
    }
    if gloas_columns::supports(case) {
        return gloas_columns::run_case(case, max_bytes);
    }
    if gloas_fork_choice::supports(case) {
        return gloas_fork_choice::run_case(case, max_bytes);
    }
    if merkle_proof::supports(case) {
        return merkle_proof::run_case(case, max_bytes);
    }
    if light_client::supports(case) {
        return light_client::run_case(case, max_bytes);
    }
    if networking::supports(case) {
        return networking::run_case(case, max_bytes);
    }
    if rewards::supports(case) {
        return rewards::run_case(case, max_bytes);
    }
    if case.runner == "transition" {
        return run_transition(case, max_bytes).map_err(|error| (Outcome::Failed, error));
    }
    if matches!(case.runner.as_str(), "fork_choice" | "fork_choice_compliance") {
        return run_fork_choice(case, max_bytes);
    }
    if matches!(
        case.fork.as_str(),
        "bellatrix" | "capella" | "deneb" | "electra" | "fulu" | "gloas"
    ) && fixture_disables_bls(case, max_bytes).map_err(|error| (Outcome::Failed, error))? &&
        case.handler != "attestation"
    {
        return Err((
            Outcome::Unsupported,
            "required artificial later-fork BLS context is unavailable for this handler".into(),
        ));
    }
    let result = match case.runner.as_str() {
        "shuffling" => run_shuffling(case, max_bytes),
        "bls" => run_bls(case, max_bytes),
        "ssz_generic" => run_ssz(case, max_bytes),
        "ssz_static" => run_static_ssz(case, max_bytes),
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "gloas" => {
            run_gloas_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "capella" => {
            run_capella_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "electra" => {
            run_electra_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "fulu" => {
            run_fulu_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "deneb" => {
            run_deneb_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "bellatrix" => {
            run_bellatrix_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" | "fork" if case.fork == "altair" => {
            run_altair_processing(case, max_bytes)
        }
        "sanity" | "epoch_processing" | "operations" => run_phase0_processing(case, max_bytes),
        _ => unreachable!("supported runner already checked"),
    };
    result.map_err(|error| (Outcome::Failed, error))
}

fn read_bounded(path: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = reth_fs_util::metadata(path).map_err(|error| error.to_string())?;
    if metadata.len() > max_bytes as u64 {
        return Err(format!("fixture exceeds byte limit: {}", path.display()));
    }
    let file = reth_fs_util::open(path).map_err(|error| error.to_string())?;
    let limit = u64::try_from(max_bytes).map_err(|_| "fixture byte limit exceeds u64")?;
    let mut bytes = Vec::new();
    file.take(limit.checked_add(1).ok_or("fixture byte limit overflow")?)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > max_bytes {
        return Err(format!("fixture exceeds byte limit: {}", path.display()));
    }
    Ok(bytes)
}

fn yaml(path: &Path, max_bytes: usize) -> Result<Value, String> {
    serde_yaml::from_slice(&read_bounded(path, max_bytes)?).map_err(|error| error.to_string())
}

// Preserve unsigned hexadecimal byte strings and integers wider than the YAML Value number.
// Official configuration files use these plain scalars for versions, addresses and TTD.
fn config_yaml(path: &Path, max_bytes: usize) -> Result<Value, String> {
    let bytes = read_bounded(path, max_bytes)?;
    let text = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
    let mut encoded = String::with_capacity(text.len());
    for line in text.lines() {
        let mut quote = None;
        let mut cursor = 0;
        while cursor < line.len() {
            let byte = line.as_bytes()[cursor];
            if let Some(delimiter) = quote {
                let ch = line[cursor..].chars().next().ok_or("invalid quoted config character")?;
                encoded.push(ch);
                cursor += ch.len_utf8();
                if delimiter == b'"' && byte == b'\\' && cursor < line.len() {
                    let escaped = line[cursor..].chars().next().ok_or("invalid config escape")?;
                    encoded.push(escaped);
                    cursor += escaped.len_utf8();
                } else if byte == delimiter {
                    quote = None;
                }
            } else if matches!(byte, b'\'' | b'"') {
                quote = Some(byte);
                encoded.push(char::from(byte));
                cursor += 1;
            } else if byte == b'#' {
                encoded.push_str(&line[cursor..]);
                break;
            } else if byte.is_ascii_digit() {
                let end = line[cursor..]
                    .find(|ch: char| ch.is_ascii_whitespace() || matches!(ch, ',' | '}' | ']'))
                    .map_or(line.len(), |n| cursor + n);
                let token = &line[cursor..end];
                let hexadecimal = token.strip_prefix("0x").is_some_and(|digits| {
                    !digits.is_empty() && digits.bytes().all(|digit| digit.is_ascii_hexdigit())
                });
                let wide_integer = token.bytes().all(|digit| digit.is_ascii_digit()) &&
                    token.parse::<u64>().is_err();
                if hexadecimal || wide_integer {
                    encoded.push('\'');
                }
                encoded.push_str(token);
                if hexadecimal || wide_integer {
                    encoded.push('\'');
                }
                cursor = end;
            } else {
                // Configuration identifiers are ASCII; preserve other UTF-8 source characters.
                let ch = line[cursor..].chars().next().ok_or("invalid config character")?;
                encoded.push(ch);
                cursor += ch.len_utf8();
            }
        }
        encoded.push('\n');
    }
    serde_yaml::from_str(&encoded).map_err(|error| error.to_string())
}

fn config_override(case: &Case, key: &str, default: u64, max_bytes: usize) -> Result<u64, String> {
    let mut value = default;
    // Enclosing defaults are applied first, then the nearest case override.
    let mut ancestors = case.path.ancestors().take(6).collect::<Vec<_>>();
    ancestors.reverse();
    for ancestor in ancestors {
        let path = ancestor.join("config.yaml");
        if path.is_file() {
            let config = config_yaml(&path, max_bytes)?;
            if !config[key].is_null() {
                value = config[key]
                    .as_u64()
                    .or_else(|| config[key].as_str().and_then(|text| text.parse().ok()))
                    .ok_or_else(|| format!("invalid unsigned config override: {key}"))?;
            }
        }
    }
    Ok(value)
}
fn run_shuffling(case: &Case, max_bytes: usize) -> Result<(), String> {
    let data = yaml(&case.path.join("mapping.yaml"), max_bytes)?;
    let seed = fixed_hex::<32>(&data["seed"])?;
    let count = data["count"].as_u64().ok_or("missing shuffle count")?;
    let expected = data["mapping"].as_sequence().ok_or("missing shuffle mapping")?;
    if expected.len() as u64 != count {
        return Err("mapping length differs from shuffle count".into());
    }
    let default_rounds = match case.preset.as_str() {
        "minimal" => 10,
        "mainnet" => 90,
        _ => return Err("shuffling requires a known preset".into()),
    };
    let rounds =
        u8::try_from(config_override(case, "SHUFFLE_ROUND_COUNT", default_rounds, max_bytes)?)
            .map_err(|_| "shuffle round override out of range")?;
    if count == 0 {
        return Ok(());
    }
    let mapping =
        compute_shuffled_indices(count, seed, rounds).map_err(|error| error.to_string())?;
    for (index, (expected, actual)) in expected.iter().zip(mapping).enumerate() {
        let expected = expected.as_u64().ok_or("invalid shuffle mapping entry")?;
        if actual != expected {
            return Err(format!(
                "shuffle mismatch at index {index}: expected {expected}, received {actual}"
            ));
        }
    }
    Ok(())
}

fn run_bls(case: &Case, max_bytes: usize) -> Result<(), String> {
    let data = yaml(&case.path.join("data.yaml"), max_bytes)?;
    let input = &data["input"];
    let expected = data["output"].as_bool();
    if case.handler == "eth_aggregate_pubkeys" {
        let keys = input
            .as_sequence()
            .ok_or("missing aggregate public key list")?
            .iter()
            .map(fixed_hex::<48>)
            .collect::<Result<Vec<_>, _>>()?;
        let actual = aggregate_pubkeys(&keys).ok();
        let expected =
            if data["output"].is_null() { None } else { Some(fixed_hex::<48>(&data["output"])?) };
        return if actual == expected {
            Ok(())
        } else {
            Err("aggregate public key mismatch".into())
        };
    }
    let signature = fixed_hex::<96>(&input["signature"])?;
    let actual = match case.handler.as_str() {
        "verify" => {
            let public_key = fixed_hex::<48>(&input["pubkey"])?;
            let message = variable_hex(&input["message"])?;
            verify_signature(&public_key, &message, &signature).is_ok()
        }
        "fast_aggregate_verify" | "eth_fast_aggregate_verify" => {
            let public_keys = input["pubkeys"]
                .as_sequence()
                .ok_or("missing BLS public key list")?
                .iter()
                .map(fixed_hex::<48>)
                .collect::<Result<Vec<_>, _>>()?;
            let message = variable_hex(&input["message"])?;
            if case.handler == "eth_fast_aggregate_verify" {
                eth_fast_aggregate_verify(&public_keys, &message, &signature).is_ok()
            } else {
                fast_aggregate_verify(&public_keys, &message, &signature).is_ok()
            }
        }
        "aggregate_verify" => {
            let public_keys = input["pubkeys"]
                .as_sequence()
                .ok_or("missing BLS public key list")?
                .iter()
                .map(fixed_hex::<48>)
                .collect::<Result<Vec<_>, _>>()?;
            let messages = input["messages"]
                .as_sequence()
                .ok_or("missing BLS messages")?
                .iter()
                .map(variable_hex)
                .collect::<Result<Vec<_>, _>>()?;
            let borrowed = messages.iter().map(Vec::as_slice).collect::<Vec<_>>();
            aggregate_verify(&public_keys, &borrowed, &signature).is_ok()
        }
        _ => return Err("unsupported BLS handler".into()),
    };
    let expected = expected.ok_or("BLS expected output must be boolean")?;
    if actual != expected {
        return Err(format!("BLS verification mismatch: expected {expected}, received {actual}"));
    }
    Ok(())
}

fn read_snappy_ssz(path: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let compressed = read_bounded(path, max_bytes)?;
    let size = snap::raw::decompress_len(&compressed).map_err(|error| error.to_string())?;
    if size > max_bytes {
        return Err("decompressed fixture exceeds byte limit".into());
    }
    snap::raw::Decoder::new().decompress_vec(&compressed).map_err(|error| error.to_string())
}

fn run_ssz(case: &Case, max_bytes: usize) -> Result<(), String> {
    let compressed = read_bounded(&case.path.join("serialized.ssz_snappy"), max_bytes)?;
    let size = snap::raw::decompress_len(&compressed).map_err(|error| error.to_string())?;
    if size > max_bytes {
        return Err("decompressed fixture exceeds byte limit".into());
    }
    let serialized =
        snap::raw::Decoder::new().decompress_vec(&compressed).map_err(|error| error.to_string())?;
    let result = ssz_root(case, &serialized);
    if case.suite == "invalid" {
        return if result.is_err() {
            Ok(())
        } else {
            Err("invalid SSZ fixture decoded successfully".into())
        };
    }
    if case.suite != "valid" {
        return Err("unknown SSZ fixture suite".into());
    }
    let root = result?;
    let metadata = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    let expected = fixed_hex::<32>(&metadata["root"])?;
    if root != expected {
        return Err(format!(
            "SSZ root mismatch: expected {}, received {}",
            hex(&expected),
            hex(&root)
        ));
    }
    let value = yaml_ssz_value(case, max_bytes)?;
    let encoded = encode_value(case, &value)?;
    if encoded != serialized {
        return Err("SSZ value encoding differs from serialized fixture".into());
    }
    Ok(())
}

fn run_static_ssz(case: &Case, max_bytes: usize) -> Result<(), String> {
    let bytes = read_snappy_ssz(&case.path.join("serialized.ssz_snappy"), max_bytes)?;
    let roots = yaml(&case.path.join("roots.yaml"), max_bytes)?;
    let expected_root = fixed_hex::<32>(&roots["root"])?;
    let default_ptc_size = match case.preset.as_str() {
        "minimal" => 16,
        "mainnet" | "general" => 512,
        _ => return Err("unknown static SSZ preset".into()),
    };
    let ptc_size = usize::try_from(config_override(case, "PTC_SIZE", default_ptc_size, max_bytes)?)
        .map_err(|_| "PTC size override out of range")?;
    if ptc_size == 0 || ptc_size > max_bytes.saturating_mul(8) {
        return Err("PTC size override exceeds resource limit".into());
    }
    let limits = DecodeLimits {
        max_bytes,
        max_transactions: max_bytes / 4,
        max_transaction_bytes: max_bytes,
        max_withdrawals: max_bytes / 44,
        max_requests_per_kind: max_bytes / 16,
        max_access_list_bytes: max_bytes,
    };
    let max_commitments = max_bytes / 48;
    let (root, encoded) = match case.handler.as_str() {
        "BeaconState" => {
            let value = BeaconState::from_ssz(&bytes, schema_fork(case)?, schema_preset(case)?)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root().map_err(|error| error.to_string())?,
                value.to_ssz().map_err(|error| error.to_string())?,
            )
        }
        "BeaconBlock" => {
            let value = BeaconBlock::from_ssz(&bytes, schema_fork(case)?, schema_preset(case)?)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root().map_err(|error| error.to_string())?,
                value.to_ssz().map_err(|error| error.to_string())?,
            )
        }
        "SignedBeaconBlock" => {
            let fork = schema_fork(case)?;
            let preset = schema_preset(case)?;
            let value = SignedBeaconBlock::from_ssz(&bytes, fork, preset)
                .map_err(|error| error.to_string())?;
            (
                verify_complete_signed_roots(&bytes, fork, preset, &value)?,
                value.to_ssz().map_err(|error| error.to_string())?,
            )
        }
        _ if matches!(case.preset.as_str(), "minimal" | "mainnet") &&
            ssz_static::supports(schema_fork(case)?, schema_preset(case)?, &case.handler) =>
        {
            ssz_static::decode(schema_fork(case)?, schema_preset(case)?, &case.handler, &bytes)?
        }
        "BeaconBlockHeader" => {
            let value = BeaconBlockHeader::from_ssz(&bytes).map_err(|error| error.to_string())?;
            (value.hash_tree_root(), value.to_ssz().to_vec())
        }
        "ExecutionPayloadBid" => {
            let value = ExecutionPayloadBid::from_ssz(&bytes, max_commitments)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(max_commitments).map_err(|error| error.to_string())?,
                value.to_ssz().map_err(|error| error.to_string())?,
            )
        }
        "SignedExecutionPayloadBid" => {
            let value = SignedExecutionPayloadBid::from_ssz(&bytes, max_commitments)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(max_commitments).map_err(|error| error.to_string())?,
                value.to_ssz().map_err(|error| error.to_string())?,
            )
        }
        "PayloadAttestationData" => {
            let value =
                PayloadAttestationData::from_ssz(&bytes).map_err(|error| error.to_string())?;
            (value.hash_tree_root(), value.to_ssz().to_vec())
        }
        "PayloadAttestation" => {
            let value = PayloadAttestation::from_ssz(&bytes, ptc_size)
                .map_err(|error| error.to_string())?;
            (value.hash_tree_root().map_err(|error| error.to_string())?, value.to_ssz())
        }
        "PayloadAttestationMessage" => {
            let value =
                PayloadAttestationMessage::from_ssz(&bytes).map_err(|error| error.to_string())?;
            let mut encoded = Vec::new();
            encoded.extend_from_slice(&value.validator_index.to_le_bytes());
            encoded.extend_from_slice(&value.data.to_ssz());
            encoded.extend_from_slice(&value.signature);
            (value.hash_tree_root().map_err(|error| error.to_string())?, encoded)
        }
        "IndexedPayloadAttestation" => {
            let value = IndexedPayloadAttestation::from_ssz(&bytes, ptc_size)
                .map_err(|error| error.to_string())?;
            let mut encoded = Vec::new();
            encoded.extend_from_slice(&142u32.to_le_bytes());
            encoded.extend_from_slice(&value.data.to_ssz());
            encoded.extend_from_slice(&value.signature);
            for index in &value.attesting_indices {
                encoded.extend_from_slice(&index.to_le_bytes());
            }
            (value.hash_tree_root(ptc_size).map_err(|error| error.to_string())?, encoded)
        }
        "ExecutionPayload" => {
            let value = ExecutionPayload::from_ssz(&bytes, schema_preset(case)?)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(schema_preset(case)?).map_err(|error| error.to_string())?,
                value.to_ssz(schema_preset(case)?).map_err(|error| error.to_string())?,
            )
        }
        "ExecutionRequests" => {
            let value =
                ExecutionRequests::from_ssz(&bytes, limits).map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(limits).map_err(|error| error.to_string())?,
                value.to_ssz(limits).map_err(|error| error.to_string())?,
            )
        }
        "ExecutionPayloadEnvelope" => {
            let value = ExecutionPayloadEnvelope::from_ssz(&bytes, schema_preset(case)?)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(schema_preset(case)?).map_err(|error| error.to_string())?,
                value.to_ssz(schema_preset(case)?).map_err(|error| error.to_string())?,
            )
        }
        "SignedExecutionPayloadEnvelope" => {
            let value = SignedExecutionPayloadEnvelope::from_ssz(&bytes, schema_preset(case)?)
                .map_err(|error| error.to_string())?;
            (
                value.hash_tree_root(schema_preset(case)?).map_err(|error| error.to_string())?,
                value.to_ssz(schema_preset(case)?).map_err(|error| error.to_string())?,
            )
        }
        _ => ssz_static::decode(schema_fork(case)?, schema_preset(case)?, &case.handler, &bytes)?,
    };
    if root != expected_root {
        return Err(format!(
            "static SSZ root mismatch: expected {}, received {}",
            hex(&expected_root),
            hex(&root)
        ));
    }
    if encoded != bytes {
        return Err("static SSZ roundtrip differs from serialized fixture".into());
    }
    Ok(())
}
fn fixture_disables_bls(case: &Case, max_bytes: usize) -> Result<bool, String> {
    if !matches!(
        case.fork.as_str(),
        "phase0" | "altair" | "bellatrix" | "capella" | "deneb" | "electra" | "fulu" | "gloas"
    ) || !(matches!(
        case.runner.as_str(),
        "operations" |
            "genesis" |
            "fork_choice" |
            "fork_choice_compliance" |
            "fast_confirmation" |
            "finality" |
            "random"
    ) || case.runner == "sanity" && case.handler == "blocks")
    {
        return Ok(false);
    }
    let path = case.path.join("meta.yaml");
    if !path.is_file() {
        return Ok(false);
    }
    let metadata = yaml(&path, max_bytes)?;
    if metadata["bls_setting"].is_null() {
        return Ok(false);
    }
    match metadata["bls_setting"].as_u64() {
        Some(0 | 1) => Ok(false),
        Some(2) => Ok(true),
        _ => Err("fixture BLS setting must be 0, 1 or 2".into()),
    }
}

fn config_version_override(
    case: &Case,
    key: &str,
    default: [u8; 4],
    max_bytes: usize,
) -> Result<[u8; 4], String> {
    let mut value = default;
    let mut ancestors = case.path.ancestors().take(6).collect::<Vec<_>>();
    ancestors.reverse();
    for ancestor in ancestors {
        let path = ancestor.join("config.yaml");
        if path.is_file() {
            let config = config_yaml(&path, max_bytes)?;
            if !config[key].is_null() {
                value = fixed_hex::<4>(&config[key])?;
            }
        }
    }
    Ok(value)
}

#[derive(Debug)]
struct FixtureExecutionEngine {
    valid: bool,
    terminal: Option<fork_choice_merge::TerminalContext>,
}

impl FixtureExecutionEngine {
    fn from_case(case: &Case, max_bytes: usize, require_metadata: bool) -> Result<Self, String> {
        let path = case.path.join("execution.yaml");
        let valid = if path.is_file() {
            yaml(&path, max_bytes)?["execution_valid"]
                .as_bool()
                .ok_or("execution fixture validity must be boolean")?
        } else if require_metadata {
            return Err("missing required execution fixture mock metadata".into());
        } else {
            true
        };
        Ok(Self { valid, terminal: None })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("published consensus fixture mock rejected execution")]
struct FixtureExecutionError;

impl bellatrix_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_terminal_block(
        &self,
        _payload: &bellatrix::ExecutionPayload,
        _context: bellatrix_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        // Ordinary operation fixtures mock terminal eligibility. Merge graph fixtures use
        // their published synthetic ancestry and exact uint256 threshold instead.
        if self
            .terminal
            .as_ref()
            .is_none_or(|terminal| terminal.verify(_payload.parent_hash, _context.slot))
        {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
    async fn verify_payload(
        &self,
        _payload: &bellatrix::ExecutionPayload,
        _context: bellatrix_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}

impl capella_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_terminal_block(
        &self,
        _payload: &capella::ExecutionPayload,
        _context: capella_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        // Private published consensus context explicitly mocks terminal eligibility.
        // Runtime adapters must prove the configured terminal override or PoW ancestry.
        Ok(())
    }
    async fn verify_payload(
        &self,
        _payload: &capella::ExecutionPayload,
        _context: capella_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}
impl deneb_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_terminal_block(
        &self,
        _payload: &deneb::ExecutionPayload,
        _context: deneb_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        // Explicit private published-fixture mock, never runtime terminal ancestry proof.
        Ok(())
    }
    async fn verify_payload(
        &self,
        _payload: &deneb::ExecutionPayload,
        _context: deneb_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}

impl electra_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_terminal_block(
        &self,
        _payload: &electra::ExecutionPayload,
        _context: electra_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        // Explicit private published-fixture mock, never runtime terminal ancestry proof.
        Ok(())
    }
    async fn verify_payload(
        &self,
        _payload: &electra::ExecutionPayload,
        _context: electra_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}
impl fulu_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_terminal_block(
        &self,
        _payload: &fulu::ExecutionPayload,
        _context: fulu_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        // Explicit private published-fixture mock, never runtime terminal ancestry proof.
        Ok(())
    }
    async fn verify_payload(
        &self,
        _payload: &fulu::ExecutionPayload,
        _context: fulu_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}

impl gloas_processing::ExecutionEngine for FixtureExecutionEngine {
    type Error = FixtureExecutionError;
    async fn verify_payload(
        &self,
        _payload: &gloas::ExecutionPayload,
        _context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        if self.valid {
            Ok(())
        } else {
            Err(FixtureExecutionError)
        }
    }
}

fn poll_fixture_future<T>(future: impl Future<Output = T>) -> Result<T, String> {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => {
            Err("private specification execution fixture unexpectedly suspended".into())
        }
    }
}

fn config_value_override(
    case: &Case,
    name: &str,
    max_bytes: usize,
) -> Result<Option<Value>, String> {
    let mut directory = Some(case.path.as_path());
    for _ in 0..6 {
        let Some(path) = directory else { break };
        let config = path.join("config.yaml");
        if config.is_file() {
            let value = config_yaml(&config, max_bytes)?;
            if let Some(value) = value.get(name) {
                return Ok(Some(value.clone()));
            }
        }
        directory = path.parent();
    }
    Ok(None)
}

fn fixture_transition_parameters(
    case: &Case,
    preset: SszPreset,
    target: Fork,
    target_epoch: u64,
    max_bytes: usize,
) -> Result<fork_processing::TransitionParameters, String> {
    let mut parameters = fixture_gloas_parameters(case, preset, max_bytes)?;
    let mut schedule = Vec::new();
    for (index, name) in FORKS.iter().enumerate() {
        let fork = schema_fork_name(name)?;
        let version = config_version_override(
            case,
            &if fork == Fork::Phase0 {
                "GENESIS_FORK_VERSION".to_owned()
            } else {
                format!("{}_FORK_VERSION", name.to_ascii_uppercase())
            },
            [index as u8, 0, 0, if preset == SszPreset::Minimal { 1 } else { 0 }],
            max_bytes,
        )?;
        schedule.push(fork_processing::ForkActivation {
            fork,
            epoch: if fork < target {
                0
            } else if fork == target {
                target_epoch
            } else {
                u64::MAX
            },
            version,
        });
    }
    if schedule[6].epoch == u64::MAX {
        parameters.fulu.blob_schedule.clear();
    }
    fork_processing::TransitionParameters::new(parameters, schedule)
        .map_err(|error| error.to_string())
}

fn signed_fixture_slot(bytes: &[u8]) -> Result<u64, String> {
    // SignedBeaconBlock remains the regular message/signature container through Gloas.
    // The selected fork decoder independently validates every canonical offset and field.
    if bytes.get(..4) != Some(100u32.to_le_bytes().as_slice()) {
        return Err("invalid signed block message offset".into());
    }
    Ok(u64::from_le_bytes(
        bytes
            .get(100..108)
            .ok_or("truncated signed block slot")?
            .try_into()
            .map_err(|_| "invalid signed block slot length")?,
    ))
}

fn run_transition(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let target = schema_fork(case)?;
    let metadata = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    if metadata["post_fork"].as_str() != Some(case.fork.as_str()) {
        return Err("transition metadata post fork differs from case identity".into());
    }
    let epoch =
        metadata["fork_epoch"].as_u64().ok_or("transition activation epoch must be unsigned")?;
    let count =
        metadata["blocks_count"].as_u64().ok_or("transition block count must be unsigned")?;
    if count == 0 || count > 1024 {
        return Err("transition block count exceeds fixture resource budget".into());
    }
    let previous = FORKS
        .iter()
        .position(|name| *name == case.fork)
        .and_then(|index| index.checked_sub(1))
        .ok_or("transition has no preceding fork")?;
    let previous = schema_fork_name(FORKS[previous])?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut state =
        BeaconState::from_ssz(&bytes, previous, preset).map_err(|error| error.to_string())?;
    let parameters = fixture_transition_parameters(case, preset, target, epoch, max_bytes)?;
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let mut result = Ok(());
    for index in 0..count {
        let bytes =
            read_snappy_ssz(&case.path.join(format!("blocks_{index}.ssz_snappy")), max_bytes)?;
        let slot = signed_fixture_slot(&bytes)?;
        if slot.saturating_sub(state.slot()) > 8192 {
            return Err("transition fixture gap exceeds replay limit".into());
        }
        let block = SignedBeaconBlock::from_ssz(
            &bytes,
            parameters.fork_at_slot(slot).map_err(|error| error.to_string())?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        result = poll_fixture_future(fork_processing::state_transition(
            &mut state,
            &block,
            &parameters,
            &engine,
        ))?
        .map_err(|error| error.to_string());
        if result.is_err() {
            break;
        }
    }
    let post = case.path.join("post.ssz_snappy");
    match result {
        Err(error) if post.is_file() => return Err(error),
        Err(_) => return Ok(()),
        Ok(()) if !post.is_file() => {
            return Err("expected transition rejection, received success".into())
        }
        Ok(()) => {}
    }
    if state.schema_fork() != target {
        return Err("transition did not reach its required fork".into());
    }
    let expected = read_snappy_ssz(&post, max_bytes)?;
    let post =
        BeaconState::from_ssz(&expected, target, preset).map_err(|error| error.to_string())?;
    if state.hash_tree_root().map_err(|error| error.to_string())? !=
        post.hash_tree_root().map_err(|error| error.to_string())? ||
        state.to_ssz().map_err(|error| error.to_string())? != expected
    {
        return Err(
            "transition full post-state root or canonical bytes differ from published fixture"
                .into(),
        );
    }
    Ok(())
}

fn run_fork_choice(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    let steps =
        yaml(&case.path.join("steps.yaml"), max_bytes).map_err(|error| (Outcome::Failed, error))?;
    let steps =
        steps.as_sequence().ok_or_else(|| (Outcome::Failed, "missing fork-choice steps".into()))?;
    if steps.is_empty() || steps.len() > 100_000 {
        return Err((Outcome::Failed, "fork-choice step count exceeds bounded budget".into()));
    }
    for step in steps {
        let mapping = step
            .as_mapping()
            .ok_or_else(|| (Outcome::Failed, "fork-choice step must be a mapping".into()))?;
        if mapping.keys().any(|key| {
            !matches!(
                key.as_str(),
                Some(
                    "tick" |
                        "checks" |
                        "block" |
                        "attestation" |
                        "attester_slashing" |
                        "valid" |
                        "blobs" |
                        "proofs" |
                        "columns" |
                        "pow_block"
                )
            )
        }) {
            return Err((
                Outcome::Unsupported,
                "required fork-choice event is not implemented".into(),
            ));
        }
        if let Some(checks) = step["checks"].as_mapping() &&
            checks.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    Some(
                        "head" |
                            "get_proposer_head" |
                            "genesis_time" |
                            "time" |
                            "justified_checkpoint" |
                            "finalized_checkpoint" |
                            "proposer_boost_root" |
                            "viable_for_head_roots_and_weights"
                    )
                )
            })
        {
            return Err((
                Outcome::Unsupported,
                "required fork-choice assertion is not implemented".into(),
            ));
        }
    }
    run_fork_choice_steps(case, max_bytes, steps).map_err(|error| (Outcome::Failed, error))
}

fn fork_choice_fixture_path(case: &Case, value: &Value) -> Result<PathBuf, String> {
    let name = value.as_str().ok_or("fork-choice event filename must be a string")?;
    if name.is_empty() ||
        name.len() > 256 ||
        !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err("unsafe fork-choice event filename".into());
    }
    Ok(case.path.join(format!("{name}.ssz_snappy")))
}

fn fork_choice_event_result(result: Result<(), StoreError>, step: &Value) -> Result<(), String> {
    let expected = if step["valid"].is_null() {
        true
    } else {
        step["valid"].as_bool().ok_or("fork-choice validity expectation must be boolean")?
    };
    match result {
        Err(
            error @ (StoreError::Overflow |
            StoreError::ResourceLimit |
            StoreError::ValidationUnavailable(_)),
        ) => Err(error.to_string()),
        Err(error) if expected => Err(error.to_string()),
        Err(_) if !expected => Ok(()),
        Ok(()) if !expected => Err("expected fork-choice event rejection, received success".into()),
        Ok(()) => Ok(()),
        Err(_) => unreachable!("expected rejection handled above"),
    }
}

fn run_fork_choice_steps(case: &Case, max_bytes: usize, steps: &[Value]) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork(case)?;
    let anchor_state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_state.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let anchor_block = BeaconBlock::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_block.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0.genesis_fork_version = config_version_override(
        case,
        "GENESIS_FORK_VERSION",
        parameters.phase0.genesis_fork_version,
        max_bytes,
    )?;
    parameters.altair_fork_version = config_version_override(
        case,
        "ALTAIR_FORK_VERSION",
        parameters.altair_fork_version,
        max_bytes,
    )?;
    let legacy_seconds =
        config_override(case, "SECONDS_PER_SLOT", parameters.slot_duration_ms / 1000, max_bytes)?;
    parameters.slot_duration_ms = config_override(
        case,
        "SLOT_DURATION_MS",
        legacy_seconds.checked_mul(1000).ok_or("slot duration overflow")?,
        max_bytes,
    )?;
    parameters.phase0.ejection_balance =
        config_override(case, "EJECTION_BALANCE", parameters.phase0.ejection_balance, max_bytes)?;
    parameters.phase0.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT",
        parameters.phase0.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.phase0.churn_limit_quotient = config_override(
        case,
        "CHURN_LIMIT_QUOTIENT",
        parameters.phase0.churn_limit_quotient,
        max_bytes,
    )?;
    parameters.phase0.min_validator_withdrawability_delay = config_override(
        case,
        "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
        parameters.phase0.min_validator_withdrawability_delay,
        max_bytes,
    )?;
    parameters.phase0.shard_committee_period = config_override(
        case,
        "SHARD_COMMITTEE_PERIOD",
        parameters.phase0.shard_committee_period,
        max_bytes,
    )?;
    parameters.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.inactivity_score_recovery_rate = config_override(
        case,
        "INACTIVITY_SCORE_RECOVERY_RATE",
        parameters.inactivity_score_recovery_rate,
        max_bytes,
    )?;
    let transition_parameters = if fork <= Fork::Fulu {
        let mut inherited = fixture_gloas_parameters(case, preset, max_bytes)?;
        let mut schedule = Vec::new();
        for (index, name) in FORKS.iter().enumerate() {
            let own = schema_fork_name(name)?;
            let epoch = config_override(
                case,
                &format!("{}_FORK_EPOCH", name.to_ascii_uppercase()),
                if own <= fork { 0 } else { u64::MAX },
                max_bytes,
            )?;
            let version = config_version_override(
                case,
                &if own == Fork::Phase0 {
                    "GENESIS_FORK_VERSION".to_owned()
                } else {
                    format!("{}_FORK_VERSION", name.to_ascii_uppercase())
                },
                [index as u8, 0, 0, if preset == SszPreset::Minimal { 1 } else { 0 }],
                max_bytes,
            )?;
            schedule.push(fork_processing::ForkActivation { fork: own, epoch, version });
        }
        if schedule[6].epoch == u64::MAX {
            inherited.fulu.blob_schedule.clear();
        }
        Some(
            fork_processing::TransitionParameters::new(inherited, schedule)
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    let artificial = fixture_disables_bls(case, max_bytes)?;
    let mut store = fork_choice_store::FixtureStore::get_store(
        anchor_state,
        anchor_block,
        parameters,
        artificial,
    )
    .map_err(|error| error.to_string())?;
    if let Some(parameters) = transition_parameters {
        store.set_transition_parameters(parameters).map_err(|error| error.to_string())?;
    }
    let mut engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    if case.handler == "on_merge_block" {
        engine.terminal =
            Some(fork_choice_merge::TerminalContext::from_case(case, preset, max_bytes)?);
    }
    let mut assertions = 0;
    for (index, step) in steps.iter().enumerate() {
        let events = ["tick", "checks", "block", "attestation", "attester_slashing", "pow_block"]
            .iter()
            .filter(|name| !step[**name].is_null())
            .count();
        if events != 1 {
            return Err(format!("fork-choice step {index} must contain exactly one event"));
        }
        if !step["tick"].is_null() {
            let seconds = step["tick"].as_u64().ok_or("invalid fork-choice tick")?;
            store
                .on_tick(seconds.checked_mul(1000).ok_or("fork-choice tick overflow")?)
                .map_err(|error| error.to_string())?;
        } else if !step["pow_block"].is_null() {
            let bytes =
                read_snappy_ssz(&fork_choice_fixture_path(case, &step["pow_block"])?, max_bytes)?;
            let block = reth_embedded_beacon_state_transition::ssz::auxiliary::bellatrix::PowBlock::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            engine
                .terminal
                .as_mut()
                .ok_or("unexpected PoW event outside terminal ancestry fixture")?
                .insert(block)?;
        } else if !step["block"].is_null() {
            let bytes =
                read_snappy_ssz(&fork_choice_fixture_path(case, &step["block"])?, max_bytes)?;
            let block = SignedBeaconBlock::from_ssz(&bytes, fork, preset)
                .map_err(|error| error.to_string())?;
            let result = {
                let availability = fork_choice_availability::FixtureAvailability::new(
                    case,
                    step,
                    preset,
                    fixture_gloas_parameters(case, preset, max_bytes)?.fulu,
                    max_bytes,
                );
                if case.runner == "fork_choice_compliance" {
                    poll_fixture_future(store.on_compliance_block_and_contents(
                        &block,
                        &engine,
                        &availability,
                    ))?
                } else {
                    poll_fixture_future(store.on_block_and_contents_with_engine(
                        &block,
                        &engine,
                        &availability,
                    ))?
                }
            };
            fork_choice_event_result(result, step)
                .map_err(|error| format!("fork-choice block step {index}: {error}"))?;
        } else if !step["attestation"].is_null() {
            let bytes =
                read_snappy_ssz(&fork_choice_fixture_path(case, &step["attestation"])?, max_bytes)?;
            fork_choice_event_result(store.on_attestation_wire(&bytes, fork), step)
                .map_err(|error| format!("fork-choice attestation step {index}: {error}"))?;
        } else if !step["attester_slashing"].is_null() {
            let bytes = read_snappy_ssz(
                &fork_choice_fixture_path(case, &step["attester_slashing"])?,
                max_bytes,
            )?;
            fork_choice_event_result(store.on_attester_slashing_wire(&bytes, fork), step)
                .map_err(|error| format!("fork-choice slashing step {index}: {error}"))?;
        } else {
            let checks = &step["checks"];
            if checks.as_mapping().is_none_or(|mapping| mapping.is_empty()) {
                return Err(format!("fork-choice step {index} must contain actual assertions"));
            }
            assertions += 1;
            if !checks["time"].is_null() &&
                checks["time"].as_u64().and_then(|value| value.checked_mul(1000)) !=
                    Some(store.time_ms())
            {
                return Err(format!("fork-choice time assertion failed at step {index}"));
            }
            if !checks["genesis_time"].is_null() &&
                checks["genesis_time"].as_u64().and_then(|value| value.checked_mul(1000)) !=
                    Some(store.genesis_time_ms())
            {
                return Err(format!("fork-choice genesis time assertion failed at step {index}"));
            }
            if !checks["get_proposer_head"].is_null() &&
                store.get_proposer_head().map_err(|error| error.to_string())?.root !=
                    fixed_hex::<32>(&checks["get_proposer_head"])?
            {
                return Err(format!("fork-choice proposer head assertion failed at step {index}"));
            }
            if !checks["head"].is_null() {
                let actual = store.get_head().map_err(|error| error.to_string())?;
                if !checks["head"]["root"].is_null() &&
                    actual.root != fixed_hex::<32>(&checks["head"]["root"])?
                {
                    return Err(format!(
                        "fork-choice head root assertion failed at step {index}: received {}, slot {:?}, boost {}, checkpoints {:?}",
                        hex(&actual.root), store.block_state(actual.root).map(BeaconState::slot),
                        hex(&store.proposer_boost_root()), store.checkpoints()
                    ));
                }
                if !checks["head"]["slot"].is_null() &&
                    checks["head"]["slot"].as_u64() !=
                        store.block_state(actual.root).map(BeaconState::slot)
                {
                    return Err(format!("fork-choice head slot assertion failed at step {index}"));
                }
            }
            for (name, actual) in [
                ("justified_checkpoint", store.checkpoints().0),
                ("finalized_checkpoint", store.checkpoints().1),
            ] {
                if !checks[name].is_null() &&
                    (checks[name]["epoch"].as_u64() != Some(actual.epoch) ||
                        fixed_hex::<32>(&checks[name]["root"])? != actual.root)
                {
                    return Err(format!("fork-choice {name} assertion failed at step {index}"));
                }
            }
            if !checks["viable_for_head_roots_and_weights"].is_null() {
                let entries = checks["viable_for_head_roots_and_weights"]
                    .as_sequence()
                    .ok_or("viable leaf weights must be a sequence")?;
                if entries.len() > 4096 {
                    return Err("viable leaf assertion budget exceeded".into());
                }
                let mut expected = BTreeMap::new();
                for entry in entries {
                    let root = fixed_hex::<32>(&entry["root"])?;
                    let weight =
                        u128::from(entry["weight"].as_u64().ok_or("invalid viable leaf weight")?);
                    if expected.insert(root, weight).is_some() {
                        return Err("duplicate viable leaf assertion".into());
                    }
                }
                let actual = store.viable_leaf_weights().map_err(|error| error.to_string())?;
                if actual != expected {
                    return Err(format!("viable leaf weights differ at step {index}: expected {expected:?}, received {actual:?}"));
                }
            }
            if !checks["proposer_boost_root"].is_null() &&
                fixed_hex::<32>(&checks["proposer_boost_root"])? != store.proposer_boost_root()
            {
                return Err(format!("fork-choice proposer boost assertion failed at step {index}"));
            }
        }
    }
    if assertions == 0 {
        return Err("fork-choice fixture has no actual assertions".into());
    }
    Ok(())
}

fn verify_complete_signed_roots(
    bytes: &[u8],
    fork: Fork,
    preset: SszPreset,
    signed: &SignedBeaconBlock,
) -> Result<Root, String> {
    // Every targeted signed container has a canonical 100-byte fixed section:
    // one message offset followed by the 96-byte signature.
    let message = BeaconBlock::from_ssz(
        bytes.get(100..).ok_or("signed beacon block lacks its message")?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let message_root = message.hash_tree_root().map_err(|error| error.to_string())?;
    if signed.message_root().map_err(|error| error.to_string())? != message_root {
        return Err("signed container and separately decoded unsigned message roots differ".into());
    }
    if message.header().map_err(|error| error.to_string())?.hash_tree_root() != message_root {
        return Err("unsigned beacon block and complete body-derived header roots differ".into());
    }
    let signature_root =
        bytes_root(bytes.get(4..100).ok_or("signed beacon block lacks its signature")?, 96, false)
            .map_err(|error| error.to_string())?;
    let root = signed.hash_tree_root().map_err(|error| error.to_string())?;
    if merkleize(&[message_root, signature_root], 2).map_err(|error| error.to_string())? != root {
        return Err("signed beacon block root differs from its message/signature container".into());
    }
    Ok(root)
}

fn schema_fork(case: &Case) -> Result<Fork, String> {
    schema_fork_name(&case.fork)
}

fn schema_fork_name(name: &str) -> Result<Fork, String> {
    match name {
        "phase0" => Ok(Fork::Phase0),
        "altair" => Ok(Fork::Altair),
        "bellatrix" => Ok(Fork::Bellatrix),
        "capella" => Ok(Fork::Capella),
        "deneb" => Ok(Fork::Deneb),
        "electra" => Ok(Fork::Electra),
        "fulu" => Ok(Fork::Fulu),
        "gloas" => Ok(Fork::Gloas),
        _ => Err("complete beacon SSZ schema is outside the targeted fork sequence".into()),
    }
}

fn schema_preset(case: &Case) -> Result<SszPreset, String> {
    match case.preset.as_str() {
        "minimal" => Ok(SszPreset::Minimal),
        "mainnet" => Ok(SszPreset::Mainnet),
        _ => Err("complete beacon SSZ schemas require the mainnet or minimal preset".into()),
    }
}

fn ssz_root(case: &Case, bytes: &[u8]) -> Result<Root, String> {
    let parts = case.name.split('_').collect::<Vec<_>>();
    match case.handler.as_str() {
        "boolean" => {
            read_bool(bytes).map_err(|error| error.to_string())?;
            bytes_root(bytes, 1, false).map_err(|error| error.to_string())
        }
        "uints" => {
            let width = uint_width(parts.get(1).ok_or("missing uint width")?)?;
            if bytes.len() != width {
                return Err("wrong uint size".into());
            }
            bytes_root(bytes, width, false).map_err(|error| error.to_string())
        }
        "basic_vector" => {
            let width = basic_width(parts.get(1).ok_or("missing vector element type")?)?;
            let count = parts
                .get(2)
                .ok_or("missing vector length")?
                .parse::<usize>()
                .map_err(|error| error.to_string())?;
            if count == 0 || count.checked_mul(width) != Some(bytes.len()) {
                return Err("wrong vector size".into());
            }
            validate_basic(bytes, width, parts[1] == "bool")?;
            bytes_root(bytes, bytes.len(), false).map_err(|error| error.to_string())
        }
        "basic_progressive_list" => {
            let width = basic_width(parts.get(1).ok_or("missing progressive list element type")?)?;
            validate_basic(bytes, width, parts[1] == "bool")?;
            Ok(mix_in_length(
                merkleize_progressive(&pack_bytes(bytes)).map_err(|error| error.to_string())?,
                (bytes.len() / width) as u64,
            ))
        }
        "bitvector" => {
            let count = parts
                .get(1)
                .ok_or("missing bitvector length")?
                .parse::<usize>()
                .map_err(|error| error.to_string())?;
            BitVector::from_ssz(bytes, count)
                .and_then(|bits| bits.hash_tree_root())
                .map_err(|error| error.to_string())
        }
        "bitlist" => {
            let limit = parts
                .get(1)
                .ok_or("missing bitlist limit")?
                .parse::<usize>()
                .map_err(|error| error.to_string())?;
            BitList::from_ssz(bytes, limit)
                .and_then(|bits| bits.hash_tree_root(limit))
                .map_err(|error| error.to_string())
        }
        "progressive_bitlist" => BitList::from_ssz(bytes, bytes.len().saturating_mul(8))
            .and_then(|bits| bits.progressive_root())
            .map_err(|error| error.to_string()),
        _ => Err("unsupported SSZ handler".into()),
    }
}

fn yaml_ssz_value(case: &Case, max_bytes: usize) -> Result<Value, String> {
    let path = case.path.join("value.yaml");
    let numeric = case.handler == "uints" ||
        (matches!(case.handler.as_str(), "basic_vector" | "basic_progressive_list") &&
            !case.name.starts_with("vec_bool_") &&
            !case.name.starts_with("proglist_bool_"));
    if !numeric {
        return yaml(&path, max_bytes);
    }
    // serde_yaml's generic Number cannot preserve unquoted uint128/uint256 scalars.
    // These basic-type fixtures have a scalar or flat scalar-sequence schema, so retain
    // decimal tokens verbatim before the normal YAML value representation can round them.
    let bytes = read_bounded(&path, max_bytes)?;
    let text = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "---" && *line != "...")
        .collect::<Vec<_>>();
    let decimal =
        |token: &str| !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_digit());
    if case.handler == "uints" && lines.len() == 1 && decimal(lines[0]) {
        return Ok(Value::String(lines[0].into()));
    }
    if case.handler != "uints" &&
        !lines.is_empty() &&
        lines.iter().all(|line| line.strip_prefix("- ").is_some_and(&decimal))
    {
        return Ok(Value::Sequence(
            lines.iter().map(|line| Value::String(line[2..].into())).collect(),
        ));
    }
    let trimmed = text.trim();
    if case.handler != "uints" && trimmed.starts_with('[') && trimmed.ends_with(']') {
        let body = &trimmed[1..trimmed.len() - 1];
        if body.trim().is_empty() {
            return Ok(Value::Sequence(Vec::new()));
        }
        let tokens = body.split(',').map(str::trim).collect::<Vec<_>>();
        if tokens.iter().all(|token| decimal(token)) {
            return Ok(Value::Sequence(
                tokens.into_iter().map(|token| Value::String(token.into())).collect(),
            ));
        }
    }
    serde_yaml::from_slice(&bytes).map_err(|error| error.to_string())
}
fn encode_value(case: &Case, value: &Value) -> Result<Vec<u8>, String> {
    let parts = case.name.split('_').collect::<Vec<_>>();
    match case.handler.as_str() {
        "boolean" => Ok(vec![u8::from(value.as_bool().ok_or("invalid boolean YAML value")?)]),
        "uints" => integer_bytes(value, uint_width(parts.get(1).ok_or("missing uint width")?)?),
        "basic_vector" | "basic_progressive_list" => {
            let element = parts.get(1).ok_or("missing element type")?;
            let width = basic_width(element)?;
            let mut encoded = Vec::new();
            for element_value in value.as_sequence().ok_or("invalid vector YAML value")? {
                if *element == "bool" {
                    encoded.push(u8::from(
                        element_value.as_bool().ok_or("invalid boolean vector element")?,
                    ));
                } else {
                    encoded.extend(integer_bytes(element_value, width)?);
                }
            }
            Ok(encoded)
        }
        "bitvector" | "bitlist" | "progressive_bitlist" => {
            let values = value.as_sequence().ok_or("invalid bits YAML value")?;
            let list = case.handler != "bitvector";
            let bits = values.len() + usize::from(list);
            let mut encoded = vec![0; bits.div_ceil(8)];
            for (index, value) in values.iter().enumerate() {
                if value.as_bool().ok_or("invalid bit YAML value")? {
                    encoded[index / 8] |= 1 << (index % 8);
                }
            }
            if list {
                encoded[values.len() / 8] |= 1 << (values.len() % 8);
            }
            Ok(encoded)
        }
        _ => Err("unsupported SSZ YAML type".into()),
    }
}

fn validate_basic(bytes: &[u8], width: usize, boolean: bool) -> Result<(), String> {
    if !bytes.len().is_multiple_of(width) || (boolean && bytes.iter().any(|byte| *byte > 1)) {
        return Err("invalid basic type encoding".into());
    }
    Ok(())
}

fn basic_width(name: &str) -> Result<usize, String> {
    if name == "bool" {
        Ok(1)
    } else {
        uint_width(name.strip_prefix("uint").ok_or("invalid basic type")?)
    }
}

fn uint_width(bits: &str) -> Result<usize, String> {
    match bits {
        "8" => Ok(1),
        "16" => Ok(2),
        "32" => Ok(4),
        "64" => Ok(8),
        "128" => Ok(16),
        "256" => Ok(32),
        _ => Err("invalid SSZ uint width".into()),
    }
}

fn integer_bytes(value: &Value, width: usize) -> Result<Vec<u8>, String> {
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => return Err("invalid uint YAML value".into()),
    };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("uint YAML value is not unsigned decimal".into());
    }
    let mut out = vec![0; width];
    for digit in text.bytes() {
        let mut carry = u16::from(digit - b'0');
        for byte in &mut out {
            carry += u16::from(*byte) * 10;
            *byte = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            return Err("uint YAML value overflows its type".into());
        }
    }
    Ok(out)
}

fn variable_hex(value: &Value) -> Result<Vec<u8>, String> {
    let text = value.as_str().ok_or("hex field must be a string")?;
    let text = text.strip_prefix("0x").ok_or("hex field requires 0x prefix")?;
    if !text.len().is_multiple_of(2) {
        return Err("hex field has an odd digit count".into());
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).ok_or("invalid hex digit")?;
            let low = (pair[1] as char).to_digit(16).ok_or("invalid hex digit")?;
            Ok(((high << 4) | low) as u8)
        })
        .collect()
}

fn fixed_hex<const N: usize>(value: &Value) -> Result<[u8; N], String> {
    variable_hex(value)?.try_into().map_err(|_| format!("hex field must have {N} bytes"))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::from("0x");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests;
