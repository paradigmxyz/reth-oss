//! Command-line conformance runner with separately trusted inventory digests.

use clap::{Args, Parser, Subcommand};
use reth_embedded_beacon_primitives::{root_from_hex, sha256};
use reth_embedded_beacon_spec_tests::{
    inspect_manifest, inventory, load_manifest, load_release_source, run,
    run_authenticated_release, run_complete, FixtureManifest, Report, RunOptions,
    COMPLIANCE_MANIFEST_SHA256, MAX_AUTHENTICATION_WORKERS, MAX_CASE_WORKERS,
    PRIMARY_MANIFEST_SHA256,
};
use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Debug, Parser)]
#[command(about = "Run pinned embedded beacon consensus fixtures")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inventory a separately verified release directory; this does not authenticate its source.
    Inventory {
        /// Root containing general/, mainnet/ and minimal/ preset directories.
        #[arg(long)]
        fixtures: PathBuf,
        /// Output manifest outside the fixture directory.
        #[arg(long)]
        output: PathBuf,
    },
    /// Inspect original manifest identities and declared handlers without opening fixtures.
    Inspect {
        /// Independently authenticated original inventory.
        #[arg(long)]
        manifest: PathBuf,
        /// SHA-256 obtained independently of this inspection.
        #[arg(long)]
        manifest_sha256: String,
        /// JSON inventory output; never a fixture execution report.
        #[arg(long)]
        json: PathBuf,
    },
    /// Check one inventory and run every discovered case. Unsupported handlers fail.
    Run {
        /// Root containing the preset directories.
        #[arg(long)]
        fixtures: PathBuf,
        /// Caller-trusted fixture inventory.
        #[arg(long)]
        manifest: PathBuf,
        /// SHA-256 of the manifest, obtained independently of this run.
        #[arg(long)]
        manifest_sha256: String,
        /// JSON report destination.
        #[arg(long)]
        json: PathBuf,
        /// `JUnit` report destination.
        #[arg(long)]
        junit: PathBuf,
        /// Permit a subset, explicitly marking the result as incomplete conformance.
        #[arg(long)]
        focused: bool,
        /// Emit progress after this many cases; zero disables it.
        #[arg(long, default_value_t = 1024)]
        progress_every: usize,
        /// Authentication workers, from one through 32; defaults to bounded host parallelism.
        #[arg(long, value_parser = parse_authentication_workers)]
        authentication_workers: Option<usize>,
        /// Independent case workers, from one through 32; defaults to one.
        #[arg(long, value_parser = parse_case_workers)]
        case_workers: Option<usize>,
    },
    /// Authenticate original published archives using a separately trusted pre-execution record.
    RunAuthenticatedRelease {
        #[command(flatten)]
        sources: CompleteSources,
        /// Original archive-derived source record, prepared before fixture execution.
        #[arg(long)]
        source_record: PathBuf,
        /// SHA-256 obtained from authenticated release preparation, never a result report.
        #[arg(long)]
        source_record_sha256: String,
        /// One complete fresh release report destination.
        #[arg(long)]
        json: PathBuf,
        /// One `JUnit` report destination.
        #[arg(long)]
        junit: PathBuf,
        /// Emit progress after this many cases; zero disables it.
        #[arg(long, default_value_t = 1024)]
        progress_every: usize,
        /// Authentication workers, from one through 32.
        #[arg(long, value_parser = parse_authentication_workers)]
        authentication_workers: Option<usize>,
        /// Independent case workers, from one through 32; defaults to one.
        #[arg(long, value_parser = parse_case_workers)]
        case_workers: Option<usize>,
    },
    /// Authenticate both original full corpora, then run their exact unique union once.
    RunComplete {
        #[command(flatten)]
        sources: CompleteSources,
        /// One JSON report destination for the complete fresh run.
        #[arg(long)]
        json: PathBuf,
        /// One `JUnit` report destination for the complete fresh run.
        #[arg(long)]
        junit: PathBuf,
        /// Emit progress after this many cases; zero disables it.
        #[arg(long, default_value_t = 1024)]
        progress_every: usize,
        /// Authentication workers, from one through 32; defaults to bounded host parallelism.
        #[arg(long, value_parser = parse_authentication_workers)]
        authentication_workers: Option<usize>,
        /// Independent case workers, from one through 32; defaults to one.
        #[arg(long, value_parser = parse_case_workers)]
        case_workers: Option<usize>,
    },
}

#[derive(Debug, Args)]
struct CompleteSources {
    /// Original primary root containing the preset directories.
    #[arg(long)]
    primary_fixtures: PathBuf,
    /// Independently trusted primary inventory, outside its fixture root.
    #[arg(long)]
    primary_manifest: PathBuf,
    /// Separately supplied exact original primary manifest SHA-256.
    #[arg(long)]
    primary_manifest_sha256: String,
    /// Original compliance root containing the preset directories.
    #[arg(long)]
    compliance_fixtures: PathBuf,
    /// Independently trusted compliance inventory, outside its fixture root.
    #[arg(long)]
    compliance_manifest: PathBuf,
    /// Separately supplied exact original compliance manifest SHA-256.
    #[arg(long)]
    compliance_manifest_sha256: String,
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli) -> Result<bool, String> {
    match cli.command {
        Command::Inventory { fixtures, output } => {
            let manifest = inventory(&fixtures)?;
            let data = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
            reth_fs_util::write(&output, &data).map_err(|error| error.to_string())?;
            let digest = sha256(&data);
            print!("manifest SHA-256: 0x");
            for byte in digest {
                print!("{byte:02x}");
            }
            println!();
            Ok(true)
        }
        Command::Inspect { manifest, manifest_sha256, json } => {
            let expected = root_from_hex(&manifest_sha256).map_err(|error| error.to_string())?;
            let manifest = load_manifest(&manifest, expected)?;
            let inventory = inspect_manifest(&manifest)?;
            reth_fs_util::write(
                json,
                serde_json::to_vec_pretty(&inventory).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            let absent_cases = inventory
                .handlers
                .iter()
                .filter(|handler| !handler.declared_dispatch)
                .map(|handler| handler.cases)
                .sum::<usize>();
            println!("original cases: {}; declared suites: {}; absent-handler cases: {}; fixture execution: false; complete conformance: false", inventory.cases, inventory.handlers.len(), absent_cases);
            Ok(true)
        }
        Command::Run {
            fixtures,
            manifest,
            manifest_sha256,
            json,
            junit,
            focused,
            progress_every,
            authentication_workers,
            case_workers,
        } => {
            let expected = root_from_hex(&manifest_sha256).map_err(|error| error.to_string())?;
            let manifest = load_manifest(&manifest, expected)?;
            let mut options = RunOptions { focused, progress_every, ..RunOptions::default() };
            if let Some(workers) = authentication_workers {
                options.authentication_workers = workers;
            }
            if let Some(workers) = case_workers {
                options.case_workers = workers;
            }
            let report = run(&fixtures, &manifest, &options);
            write_report(&report, &json, &junit)?;
            Ok(if focused { report.passed() } else { report.conformance_passed() })
        }
        Command::RunAuthenticatedRelease {
            sources,
            source_record,
            source_record_sha256,
            json,
            junit,
            progress_every,
            authentication_workers,
            case_workers,
        } => {
            let source_expected =
                root_from_hex(&source_record_sha256).map_err(|error| error.to_string())?;
            let source = load_release_source(&source_record, source_expected)?;
            let primary = load_complete_manifest(
                &sources.primary_manifest,
                &sources.primary_manifest_sha256,
                &source.corpora[0].manifest_sha256,
            )?;
            let compliance = load_complete_manifest(
                &sources.compliance_manifest,
                &sources.compliance_manifest_sha256,
                &source.corpora[1].manifest_sha256,
            )?;
            let mut options = RunOptions { progress_every, ..RunOptions::default() };
            if let Some(workers) = authentication_workers {
                options.authentication_workers = workers;
            }
            if let Some(workers) = case_workers {
                options.case_workers = workers;
            }
            let result = run_authenticated_release(
                &sources.primary_fixtures,
                &primary,
                &sources.compliance_fixtures,
                &compliance,
                source,
                source_record_sha256,
                &options,
            );
            reth_fs_util::write(
                &json,
                serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            reth_fs_util::write(&junit, result.report.junit())
                .map_err(|error| error.to_string())?;
            println!("original release cases: {}; setup errors: {}; authenticated release conformance: {}; runtime synchronization verified: false",
                result.report.cases.len(), result.report.errors.len(), result.conformance_passed());
            Ok(result.conformance_passed())
        }
        Command::RunComplete {
            sources,
            json,
            junit,
            progress_every,
            authentication_workers,
            case_workers,
        } => {
            let primary = load_complete_manifest(
                &sources.primary_manifest,
                &sources.primary_manifest_sha256,
                PRIMARY_MANIFEST_SHA256,
            )?;
            let compliance = load_complete_manifest(
                &sources.compliance_manifest,
                &sources.compliance_manifest_sha256,
                COMPLIANCE_MANIFEST_SHA256,
            )?;
            let mut options = RunOptions { progress_every, ..RunOptions::default() };
            if let Some(workers) = authentication_workers {
                options.authentication_workers = workers;
            }
            if let Some(workers) = case_workers {
                options.case_workers = workers;
            }
            let report = run_complete(
                &sources.primary_fixtures,
                &primary,
                &sources.compliance_fixtures,
                &compliance,
                &options,
            );
            write_report(&report, &json, &junit)?;
            Ok(report.conformance_passed())
        }
    }
}

fn load_complete_manifest(
    path: &Path,
    supplied: &str,
    pinned: &str,
) -> Result<FixtureManifest, String> {
    let expected = root_from_hex(supplied).map_err(|error| error.to_string())?;
    if expected != root_from_hex(pinned).map_err(|error| error.to_string())? {
        return Err(
            "complete fixture manifest SHA-256 differs from the pinned source identity".into()
        );
    }
    load_manifest(path, expected)
}

fn parse_authentication_workers(value: &str) -> Result<usize, String> {
    let workers = value.parse::<usize>().map_err(|error| error.to_string())?;
    if !(1..=MAX_AUTHENTICATION_WORKERS).contains(&workers) {
        return Err(format!(
            "fixture authentication workers must be between 1 and {MAX_AUTHENTICATION_WORKERS}"
        ));
    }
    Ok(workers)
}

fn parse_case_workers(value: &str) -> Result<usize, String> {
    let workers = value.parse::<usize>().map_err(|error| error.to_string())?;
    if !(1..=MAX_CASE_WORKERS).contains(&workers) {
        return Err(format!("fixture case workers must be between 1 and {MAX_CASE_WORKERS}"));
    }
    Ok(workers)
}

fn write_report(report: &Report, json: &Path, junit: &Path) -> Result<(), String> {
    reth_fs_util::write(
        json,
        serde_json::to_vec_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    reth_fs_util::write(junit, report.junit()).map_err(|error| error.to_string())?;
    println!(
        "cases: {}; failures: {}; setup errors: {}; artificial BLS-disabled cases: {}; specification execution mock cases: {}; specification availability mock cases: {}; specification asserted graph cases: {}; complete conformance: {}",
        report.cases.len(),
        report.cases.iter().filter(|case| case.outcome != reth_embedded_beacon_spec_tests::Outcome::Passed).count(),
        report.errors.len(),
        report.cases.iter().filter(|case| case.spec_only_bls_disabled).count(),
        report.cases.iter().filter(|case| case.spec_only_execution_mock).count(),
        report.cases.iter().filter(|case| case.spec_only_data_availability_mock).count(),
        report.cases.iter().filter(|case| case.spec_only_asserted_graph).count(),
        report.conformance_passed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    fn complete_arguments() -> Vec<&'static str> {
        vec![
            "fixtures",
            "run-complete",
            "--primary-fixtures",
            "primary",
            "--primary-manifest",
            "primary.json",
            "--primary-manifest-sha256",
            PRIMARY_MANIFEST_SHA256,
            "--compliance-fixtures",
            "compliance",
            "--compliance-manifest",
            "compliance.json",
            "--compliance-manifest-sha256",
            COMPLIANCE_MANIFEST_SHA256,
            "--json",
            "results.json",
            "--junit",
            "results.xml",
        ]
    }

    #[test]
    fn complete_parser_requires_both_independent_source_triples() {
        assert!(matches!(
            Cli::try_parse_from(complete_arguments()).unwrap().command,
            Command::RunComplete { .. }
        ));
        for flag in
            ["--compliance-fixtures", "--compliance-manifest", "--compliance-manifest-sha256"]
        {
            let mut arguments = complete_arguments();
            let index = arguments.iter().position(|argument| *argument == flag).unwrap();
            arguments.drain(index..index + 2);
            assert_eq!(
                Cli::try_parse_from(arguments).unwrap_err().kind(),
                ErrorKind::MissingRequiredArgument
            );
        }
    }

    #[test]
    fn complete_parser_rejects_conflicting_sources_and_focused_claims() {
        for arguments in [
            ["--compliance-fixtures", "another-compliance"].as_slice(),
            ["--compliance-manifest-sha256", PRIMARY_MANIFEST_SHA256].as_slice(),
        ] {
            let mut combined = complete_arguments();
            combined.extend_from_slice(arguments);
            assert_eq!(
                Cli::try_parse_from(combined).unwrap_err().kind(),
                ErrorKind::ArgumentConflict
            );
        }
        let mut focused = complete_arguments();
        focused.push("--focused");
        assert_eq!(Cli::try_parse_from(focused).unwrap_err().kind(), ErrorKind::UnknownArgument);
        for count in ["0", "33"] {
            let mut arguments = complete_arguments();
            arguments.extend(["--authentication-workers", count]);
            assert_eq!(
                Cli::try_parse_from(arguments).unwrap_err().kind(),
                ErrorKind::ValueValidation
            );
        }
    }

    #[test]
    fn published_release_parser_requires_independent_source_record_without_focused_mode() {
        let mut arguments = complete_arguments();
        arguments[1] = "run-authenticated-release";
        assert_eq!(
            Cli::try_parse_from(&arguments).unwrap_err().kind(),
            ErrorKind::MissingRequiredArgument
        );
        arguments.extend([
            "--source-record",
            "source.json",
            "--source-record-sha256",
            PRIMARY_MANIFEST_SHA256,
        ]);
        assert!(matches!(
            Cli::try_parse_from(&arguments).unwrap().command,
            Command::RunAuthenticatedRelease { .. }
        ));
        arguments.push("--focused");
        assert_eq!(Cli::try_parse_from(arguments).unwrap_err().kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn all_run_commands_accept_only_bounded_workers() {
        for command in ["run-complete", "run-authenticated-release", "run"] {
            let base = if command == "run" {
                vec![
                    "fixtures",
                    command,
                    "--fixtures",
                    "fixtures",
                    "--manifest",
                    "manifest.json",
                    "--manifest-sha256",
                    PRIMARY_MANIFEST_SHA256,
                    "--json",
                    "report.json",
                    "--junit",
                    "report.xml",
                ]
            } else {
                let mut base = complete_arguments();
                base[1] = command;
                if command == "run-authenticated-release" {
                    base.extend([
                        "--source-record",
                        "source.json",
                        "--source-record-sha256",
                        PRIMARY_MANIFEST_SHA256,
                    ]);
                }
                base
            };
            assert!(Cli::try_parse_from(&base).is_ok());
            for flag in ["--authentication-workers", "--case-workers"] {
                for count in ["1", "4", "16", "32"] {
                    let mut arguments = base.clone();
                    arguments.extend([flag, count]);
                    assert!(Cli::try_parse_from(arguments).is_ok());
                }
                for count in ["0", "33", "-1", "not-a-number"] {
                    let mut arguments = base.clone();
                    let invalid = format!("{flag}={count}");
                    arguments.push(&invalid);
                    assert_eq!(
                        Cli::try_parse_from(arguments).unwrap_err().kind(),
                        ErrorKind::ValueValidation
                    );
                }
                let mut arguments = base.clone();
                arguments.extend([flag, "1", flag, "32"]);
                assert_eq!(
                    Cli::try_parse_from(arguments).unwrap_err().kind(),
                    ErrorKind::ArgumentConflict
                );
            }
        }
    }

    #[test]
    fn complete_loader_rejects_a_substituted_source_pin_before_reading() {
        assert_eq!(
            load_complete_manifest(
                Path::new("does-not-exist"),
                PRIMARY_MANIFEST_SHA256,
                COMPLIANCE_MANIFEST_SHA256
            )
            .unwrap_err(),
            "complete fixture manifest SHA-256 differs from the pinned source identity"
        );
    }
}
