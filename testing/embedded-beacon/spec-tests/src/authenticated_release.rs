//! Complete release conformance authenticated against original archive-derived source records.
//! This mode keeps the independently frozen local corpus proof unchanged.

use super::{
    authenticated_cases, check_full_matrix, check_required_suites, empty_report, execute_cases,
    full_corpus::{input_files_digest, sorted_case_ids_digest},
    CorpusAuthentication, FixtureManifest, FullCorpusEvidence, Report, RunOptions, FORKS,
    SPEC_RELEASE,
};
use reth_embedded_beacon_primitives::{root_from_hex, sha256, Root};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io::Read, path::Path};

/// Original archive-derived inventory for one complete release source role.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCorpus {
    /// Fixed source role: primary or compliance.
    pub role: String,
    /// Separately authenticated original manifest byte identity.
    pub manifest_sha256: String,
    /// Every original selected member, including static YAML values.
    pub files: usize,
    /// Distinct canonical original fixture cases.
    pub cases: usize,
    /// Every original nonempty suite.
    pub required_suites: usize,
}

/// Pre-execution source record derived from authenticated archives, never execution outcomes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseSourceRecord {
    /// Explicit record schema.
    pub format_version: u32,
    /// Reviewed immutable schema release.
    pub spec_release: String,
    /// Exact independently resolved release metadata identity.
    pub resolution_sha256: String,
    /// Fixed role ordering for domain-separated input hashes.
    pub corpus_role_order: Vec<String>,
    /// Reviewed fork coverage; unknown new schemas require explicit review.
    pub target_forks: Vec<String>,
    /// Exact union of original canonical case identities.
    pub canonical_case_ids_sha256: String,
    /// Exact original role-qualified file and checksum identities.
    pub canonical_input_files_sha256: String,
    /// Number of unique original cases, derived before any execution.
    pub required_case_count: usize,
    /// Number of original files checked before any execution.
    pub required_file_checks: usize,
    /// Fixed primary and compliance inventories.
    pub corpora: Vec<ReleaseCorpus>,
}

/// One fresh execution bound to a separately trusted original release source record.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedReleaseReport {
    /// Explicit wrapper schema distinct from the frozen local proof.
    pub format_version: u32,
    /// Separately supplied source record identity.
    pub source_record_sha256: String,
    /// Original input identities authenticated before any handlers execute.
    pub source: ReleaseSourceRecord,
    /// Original per-case outcomes and explicit specification-only assumptions.
    pub report: Report,
}

impl ReleaseSourceRecord {
    /// Check the reviewed record schema and bounded nonempty source counts.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1 ||
            self.spec_release != SPEC_RELEASE ||
            self.corpus_role_order != ["primary", "compliance"] ||
            self.target_forks != FORKS ||
            self.corpora.len() != 2
        {
            return Err("authenticated release source has an unreviewed schema or role order".into());
        }
        for digest in [
            &self.resolution_sha256,
            &self.canonical_case_ids_sha256,
            &self.canonical_input_files_sha256,
        ] {
            root_from_hex(digest).map_err(|error| error.to_string())?;
        }
        let mut files = 0usize;
        let mut cases = 0usize;
        for (corpus, role) in self.corpora.iter().zip(["primary", "compliance"]) {
            if corpus.role != role ||
                corpus.files == 0 ||
                corpus.files > 2_000_000 ||
                corpus.cases == 0 ||
                corpus.cases > corpus.files ||
                corpus.required_suites == 0 ||
                corpus.required_suites > 100_000
            {
                return Err("invalid authenticated release source inventory".into());
            }
            root_from_hex(&corpus.manifest_sha256).map_err(|error| error.to_string())?;
            files = files.checked_add(corpus.files).ok_or("release source count overflow")?;
            cases = cases.checked_add(corpus.cases).ok_or("release source count overflow")?;
        }
        if files != self.required_file_checks || cases != self.required_case_count {
            return Err(
                "authenticated release source totals differ from original role inventories".into()
            );
        }
        Ok(())
    }
}

impl AuthenticatedReleaseReport {
    /// Require every unique original case to pass in this fresh complete invocation.
    /// Independent runtime synchronization remains a separate acceptance gate.
    pub fn conformance_passed(&self) -> bool {
        if self.format_version != 1 ||
            self.source.validate().is_err() ||
            root_from_hex(&self.source_record_sha256).is_err() ||
            self.report.spec_release != self.source.spec_release ||
            !self.report.complete_matrix ||
            !self.report.passed() ||
            self.report.cases.len() != self.source.required_case_count
        {
            return false;
        }
        let mut present = BTreeSet::new();
        for case in &self.report.cases {
            let parts = case.path.split('/').collect::<Vec<_>>();
            if parts.len() < 6 ||
                parts.iter().any(|part| matches!(*part, "" | "." | "..")) ||
                case.path.contains('\\') ||
                case.path.contains(':')
            {
                return false;
            }
            present.insert((parts[0], parts[1]));
        }
        if !["mainnet", "minimal"]
            .into_iter()
            .all(|preset| FORKS.into_iter().all(|fork| present.contains(&(preset, fork)))) ||
            !present.iter().any(|(preset, _)| *preset == "general")
        {
            return false;
        }
        let Some(evidence) = &self.report.full_corpus else { return false };
        if evidence.case_ids_sha256 != self.source.canonical_case_ids_sha256 ||
            evidence.input_files_sha256 != self.source.canonical_input_files_sha256 ||
            evidence.corpora != source_counts(&self.source)
        {
            return false;
        }
        let mut paths = self.report.cases.iter().map(|case| case.path.as_str()).collect::<Vec<_>>();
        paths.sort_unstable();
        !paths.windows(2).any(|pair| pair[0] == pair[1]) &&
            sorted_case_ids_digest(paths) == self.source.canonical_case_ids_sha256
    }
}

/// Load a bounded source record with an independently supplied SHA-256.
pub fn load_release_source(path: &Path, expected: Root) -> Result<ReleaseSourceRecord, String> {
    let file = reth_fs_util::open(path).map_err(|error| error.to_string())?;
    if file.metadata().map_err(|error| error.to_string())?.len() > 64 * 1024 {
        return Err("authenticated release source record exceeds byte limit".into());
    }
    let mut bytes = Vec::new();
    file.take(64 * 1024 + 1).read_to_end(&mut bytes).map_err(|error| error.to_string())?;
    if bytes.len() > 64 * 1024 || sha256(&bytes) != expected {
        return Err(
            "authenticated release source record differs from separately supplied digest".into()
        );
    }
    let source: ReleaseSourceRecord =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    source.validate()?;
    Ok(source)
}

/// Authenticate both complete original role inventories before executing their exact unique union.
#[allow(clippy::too_many_arguments)]
pub fn run_authenticated_release(
    primary_root: &Path,
    primary: &FixtureManifest,
    compliance_root: &Path,
    compliance: &FixtureManifest,
    source: ReleaseSourceRecord,
    source_record_sha256: String,
    options: &RunOptions,
) -> AuthenticatedReleaseReport {
    let mut result = AuthenticatedReleaseReport {
        format_version: 1,
        source_record_sha256,
        source,
        report: empty_report(true),
    };
    if let Err(error) = result.source.validate() {
        result.report.errors.push(error);
        return result;
    }
    if root_from_hex(&result.source_record_sha256).is_err() {
        result
            .report
            .errors
            .push("invalid separately supplied release source record checksum".into());
        return result;
    }
    if options.focused {
        result.report.errors.push("authenticated release runs cannot be focused".into());
        return result;
    }
    let mut cases = Vec::new();
    let mut corpora = Vec::with_capacity(2);
    for (index, (role, root, manifest)) in
        [("primary", primary_root, primary), ("compliance", compliance_root, compliance)]
            .into_iter()
            .enumerate()
    {
        if options.progress_every != 0 {
            eprintln!("fixture source: authenticating original release {role} corpus");
        }
        let source_cases = match authenticated_cases(root, manifest, options) {
            Ok(cases) => cases,
            Err(error) => {
                result.report.errors.push(format!("{role} fixture setup: {error}"));
                return result;
            }
        };
        let expected = &result.source.corpora[index];
        if manifest.files.len() != expected.files ||
            source_cases.len() != expected.cases ||
            manifest.required_suites.len() != expected.required_suites
        {
            result
                .report
                .errors
                .push(format!("{role} inventory differs from original release source"));
        }
        check_required_suites(&source_cases, manifest, &mut result.report.errors);
        corpora.push(CorpusAuthentication {
            role: role.into(),
            files: manifest.files.len(),
            cases: source_cases.len(),
        });
        cases.extend(source_cases);
    }
    cases.sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
    if let Some(duplicate) = cases.windows(2).find(|pair| pair[0].relative == pair[1].relative) {
        result
            .report
            .errors
            .push(format!("duplicate canonical fixture case: {}", duplicate[0].relative));
        return result;
    }
    let input_files_sha256 = match input_files_digest(primary, compliance) {
        Ok(digest) => digest,
        Err(error) => {
            result.report.errors.push(error);
            return result;
        }
    };
    let case_ids_sha256 = sorted_case_ids_digest(cases.iter().map(|case| case.relative.as_str()));
    if cases.len() != result.source.required_case_count ||
        case_ids_sha256 != result.source.canonical_case_ids_sha256 ||
        input_files_sha256 != result.source.canonical_input_files_sha256 ||
        corpora != source_counts(&result.source)
    {
        result
            .report
            .errors
            .push("fixture identities differ from authenticated original release source".into());
    }
    check_full_matrix(&cases, &mut result.report.errors);
    result.report.full_corpus =
        Some(FullCorpusEvidence { case_ids_sha256, input_files_sha256, corpora });
    if result.report.errors.is_empty() {
        execute_cases(&mut result.report, cases, options);
    }
    result
}

fn source_counts(source: &ReleaseSourceRecord) -> Vec<CorpusAuthentication> {
    source
        .corpora
        .iter()
        .map(|corpus| CorpusAuthentication {
            role: corpus.role.clone(),
            files: corpus.files,
            cases: corpus.cases,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{inventory, CaseResult, Outcome};
    use tempfile::TempDir;

    fn corpus(name: &str) -> (TempDir, FixtureManifest) {
        let directory = TempDir::new().unwrap();
        let case =
            directory.path().join(format!("general/phase0/ssz_generic/boolean/invalid/{name}"));
        reth_fs_util::create_dir_all(&case).unwrap();
        reth_fs_util::write(
            case.join("serialized.ssz_snappy"),
            snap::raw::Encoder::new().compress_vec(&[2]).unwrap(),
        )
        .unwrap();
        let manifest = inventory(directory.path()).unwrap();
        (directory, manifest)
    }

    fn source(primary: &FixtureManifest, compliance: &FixtureManifest) -> ReleaseSourceRecord {
        let paths = [
            "general/phase0/ssz_generic/boolean/invalid/primary",
            "general/phase0/ssz_generic/boolean/invalid/compliance",
        ];
        ReleaseSourceRecord {
            format_version: 1,
            spec_release: SPEC_RELEASE.into(),
            resolution_sha256: format!("0x{}", "00".repeat(32)),
            corpus_role_order: vec!["primary".into(), "compliance".into()],
            target_forks: FORKS.iter().map(|fork| (*fork).into()).collect(),
            canonical_case_ids_sha256: sorted_case_ids_digest([paths[1], paths[0]]),
            canonical_input_files_sha256: input_files_digest(primary, compliance).unwrap(),
            required_case_count: 2,
            required_file_checks: primary.files.len() + compliance.files.len(),
            corpora: [("primary", primary), ("compliance", compliance)]
                .into_iter()
                .map(|(role, manifest)| ReleaseCorpus {
                    role: role.into(),
                    manifest_sha256: format!("0x{}", "00".repeat(32)),
                    files: manifest.files.len(),
                    cases: 1,
                    required_suites: manifest.required_suites.len(),
                })
                .collect(),
        }
    }

    #[test]
    fn corrupt_compliance_bytes_prevent_all_release_execution() {
        let (primary_root, primary) = corpus("primary");
        let (compliance_root, compliance) = corpus("compliance");
        let source = source(&primary, &compliance);
        let relative = compliance.files.keys().next().unwrap();
        reth_fs_util::write(compliance_root.path().join(relative), b"corrupt").unwrap();
        let result = run_authenticated_release(
            primary_root.path(),
            &primary,
            compliance_root.path(),
            &compliance,
            source,
            format!("0x{}", "00".repeat(32)),
            &RunOptions::default(),
        );
        assert_eq!(
            result.report.errors,
            [format!("compliance fixture setup: fixture checksum mismatch: {relative}")]
        );
        assert!(result.report.cases.is_empty());
        assert!(!result.conformance_passed());
    }

    #[test]
    fn dynamically_authenticated_subsets_still_require_all_forks_before_execution() {
        let (primary_root, primary) = corpus("primary");
        let (compliance_root, compliance) = corpus("compliance");
        let result = run_authenticated_release(
            primary_root.path(),
            &primary,
            compliance_root.path(),
            &compliance,
            source(&primary, &compliance),
            format!("0x{}", "00".repeat(32)),
            &RunOptions::default(),
        );
        assert!(result
            .report
            .errors
            .iter()
            .any(|error| error.starts_with("missing required preset/fork:")));
        assert!(result.report.cases.is_empty());
        assert!(!result.conformance_passed());
    }

    #[test]
    fn release_wrapper_rejects_omissions_substitutions_and_unsupported_results() {
        // Synthetic outcomes exercise gate policy only; they are never fixture execution evidence.
        let (_, primary) = corpus("primary");
        let (_, compliance) = corpus("compliance");
        let mut source = source(&primary, &compliance);
        let mut report = empty_report(true);
        let mut paths = ["mainnet", "minimal"]
            .into_iter()
            .flat_map(|preset| {
                FORKS.into_iter().map(move |fork| {
                    format!("{preset}/{fork}/ssz_static/BeaconState/suite/primary")
                })
            })
            .collect::<Vec<_>>();
        paths.extend([
            "general/phase0/ssz_generic/boolean/invalid/primary".into(),
            "minimal/gloas/ssz_static/BeaconState/suite/compliance".into(),
        ]);
        paths.sort_unstable();
        source.canonical_case_ids_sha256 = sorted_case_ids_digest(paths.iter().map(String::as_str));
        source.required_case_count = paths.len();
        source.corpora[0].cases = paths.len() - 1;
        source.corpora[0].files = paths.len() - 1;
        source.required_file_checks = paths.len();
        report.cases = paths
            .into_iter()
            .map(|path| CaseResult {
                path,
                outcome: Outcome::Passed,
                message: String::new(),
                seconds: 0.0,
                spec_only_bls_disabled: false,
                spec_only_execution_mock: false,
                spec_only_data_availability_mock: false,
                spec_only_asserted_graph: false,
            })
            .collect();
        report.full_corpus = Some(FullCorpusEvidence {
            case_ids_sha256: source.canonical_case_ids_sha256.clone(),
            input_files_sha256: source.canonical_input_files_sha256.clone(),
            corpora: source_counts(&source),
        });
        let mut result = AuthenticatedReleaseReport {
            format_version: 1,
            source_record_sha256: format!("0x{}", "00".repeat(32)),
            source,
            report,
        };
        assert!(result.conformance_passed());
        assert!(!result.report.conformance_passed());
        let original = result.report.cases.clone();
        result.report.cases.retain(|case| !case.path.starts_with("mainnet/gloas/"));
        result.source.required_case_count -= 1;
        result.source.corpora[0].cases -= 1;
        result.source.canonical_case_ids_sha256 =
            sorted_case_ids_digest(result.report.cases.iter().map(|case| case.path.as_str()));
        result.report.full_corpus.as_mut().unwrap().case_ids_sha256 =
            result.source.canonical_case_ids_sha256.clone();
        result.report.full_corpus.as_mut().unwrap().corpora = source_counts(&result.source);
        assert!(!result.conformance_passed());
        result.report.cases = original;
        result.source.required_case_count += 1;
        result.source.corpora[0].cases += 1;
        result.source.canonical_case_ids_sha256 =
            sorted_case_ids_digest(result.report.cases.iter().map(|case| case.path.as_str()));
        result.report.full_corpus.as_mut().unwrap().case_ids_sha256 =
            result.source.canonical_case_ids_sha256.clone();
        result.report.full_corpus.as_mut().unwrap().corpora = source_counts(&result.source);
        result.report.cases[0].outcome = Outcome::Unsupported;
        assert!(!result.conformance_passed());
        result.report.cases[0].outcome = Outcome::Passed;
        result.report.cases[0].path = result.report.cases[1].path.clone();
        assert!(!result.conformance_passed());
        result.report.cases.pop();
        assert!(!result.conformance_passed());
    }

    #[test]
    fn source_loader_rejects_changed_schema_and_untrusted_byte_identity() {
        let (_, primary) = corpus("primary");
        let (_, compliance) = corpus("compliance");
        let mut source = source(&primary, &compliance);
        source.spec_release = "unreviewed".into();
        assert!(source.validate().is_err());
        source.spec_release = SPEC_RELEASE.into();
        source.corpora.reverse();
        assert!(source.validate().is_err());
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("source.json");
        reth_fs_util::write(&path, b"{}").unwrap();
        assert!(load_release_source(&path, [0; 32])
            .unwrap_err()
            .contains("separately supplied digest"));
    }
}
