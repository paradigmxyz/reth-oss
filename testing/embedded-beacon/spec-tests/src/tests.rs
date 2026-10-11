use super::*;
use reth_embedded_beacon_primitives::root_from_hex;
use tempfile::TempDir;

fn write_case(root: &Path, path: &str, files: &[(&str, Vec<u8>)]) {
    let directory = root.join(path);
    reth_fs_util::create_dir_all(&directory).unwrap();
    for (name, contents) in files {
        reth_fs_util::write(directory.join(name), contents).unwrap();
    }
}

fn focused() -> RunOptions {
    RunOptions { focused: true, ..RunOptions::default() }
}

#[test]
fn missing_empty_and_unsupported_never_pass() {
    let directory = TempDir::new().unwrap();
    let manifest = FixtureManifest {
        spec_release: SPEC_RELEASE.into(),
        required_suites: Vec::new(),
        files: BTreeMap::new(),
    };
    assert!(!run(directory.path(), &manifest, &focused()).passed());
    write_case(
        directory.path(),
        "mainnet/gloas/unimplemented_protocol/unknown/pyspec_tests/not_implemented",
        &[("pre.ssz_snappy", vec![0])],
    );
    let manifest = inventory(directory.path()).unwrap();
    let report = run(directory.path(), &manifest, &focused());
    assert_eq!(report.cases.len(), 1);
    assert_eq!(report.cases[0].outcome, Outcome::Unsupported);
    assert!(!report.passed());
    assert!(!report.conformance_passed());
    assert!(!report.junit().contains("<skipped"));
    assert!(report.junit().contains("failures=\"1\""));
}

#[test]
fn fixture_integrity_and_pinned_release_are_enforced() {
    let directory = TempDir::new().unwrap();
    write_case(
        directory.path(),
        "general/phase0/bls/verify/small/case",
        &[("data.yaml", b"input: {}\noutput: false\n".to_vec())],
    );
    let mut manifest = inventory(directory.path()).unwrap();
    manifest.spec_release = "master".into();
    assert_eq!(
        run(directory.path(), &manifest, &focused()).errors,
        vec![format!("fixture release mismatch: expected {SPEC_RELEASE}, received master")]
    );
    manifest.spec_release = SPEC_RELEASE.into();
    reth_fs_util::write(
        directory.path().join("general/phase0/bls/verify/small/case/data.yaml"),
        b"changed",
    )
    .unwrap();
    assert_eq!(
        run(directory.path(), &manifest, &focused()).errors,
        vec!["fixture checksum mismatch: general/phase0/bls/verify/small/case/data.yaml"]
    );
}

#[test]
fn missing_fork_matrix_is_a_setup_failure() {
    let directory = TempDir::new().unwrap();
    write_case(
        directory.path(),
        "general/phase0/bls/verify/small/case",
        &[("data.yaml", b"input: {}\noutput: false\n".to_vec())],
    );
    let report =
        run(directory.path(), &inventory(directory.path()).unwrap(), &RunOptions::default());
    assert_eq!(report.errors.len(), 17);
    assert!(!report.passed());
}

#[test]
fn boolean_valid_and_invalid_fixtures_execute_real_ssz() {
    let directory = TempDir::new().unwrap();
    let root = bytes_root(&[1], 1, false).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&[1]).unwrap();
    write_case(
        directory.path(),
        "general/phase0/ssz_generic/boolean/valid/true",
        &[
            ("serialized.ssz_snappy", compressed),
            ("meta.yaml", format!("root: '{}'\n", hex(&root)).into_bytes()),
            ("value.yaml", b"true\n".to_vec()),
        ],
    );
    write_case(
        directory.path(),
        "general/phase0/ssz_generic/boolean/invalid/two",
        &[("serialized.ssz_snappy", snap::raw::Encoder::new().compress_vec(&[2]).unwrap())],
    );
    let report = run(directory.path(), &inventory(directory.path()).unwrap(), &focused());
    assert_eq!(report.cases.len(), 2);
    assert!(report.passed(), "{report:?}");
    assert!(!report.conformance_passed());
}

#[test]
fn invalid_named_unknown_type_does_not_count_as_success() {
    let directory = TempDir::new().unwrap();
    write_case(
        directory.path(),
        "general/phase0/ssz_generic/containers/invalid/UnknownStruct",
        &[("serialized.ssz_snappy", snap::raw::Encoder::new().compress_vec(&[]).unwrap())],
    );
    let report = run(directory.path(), &inventory(directory.path()).unwrap(), &focused());
    assert_eq!(report.cases[0].outcome, Outcome::Unsupported);
}

#[test]
fn integer_yaml_preserves_256_bit_precision() {
    let value = Value::String(
        "115792089237316195423570985008687907853269984665640564039457584007913129639935".into(),
    );
    assert_eq!(integer_bytes(&value, 32).unwrap(), vec![255; 32]);
    let overflow = Value::String(
        "115792089237316195423570985008687907853269984665640564039457584007913129639936".into(),
    );
    assert_eq!(integer_bytes(&overflow, 32), Err("uint YAML value overflows its type".into()));
    assert_eq!(
        integer_bytes(&Value::String("-1".into()), 8),
        Err("uint YAML value is not unsigned decimal".into())
    );
}

#[test]
fn decompression_bound_prevents_length_bombs() {
    let directory = TempDir::new().unwrap();
    write_case(
        directory.path(),
        "general/phase0/ssz_generic/boolean/invalid/bomb",
        &[("serialized.ssz_snappy", snap::raw::Encoder::new().compress_vec(&[0; 4096]).unwrap())],
    );
    let report = run(
        directory.path(),
        &inventory(directory.path()).unwrap(),
        &RunOptions { focused: true, max_case_bytes: 300, ..RunOptions::default() },
    );
    assert_eq!(report.cases[0].outcome, Outcome::Failed);
    assert_eq!(report.cases[0].message, "decompressed fixture exceeds byte limit");
}

#[test]
fn traversal_checks_and_xml_escaping() {
    assert!(checked_relative(Path::new("fixtures"), "../outside").is_err());
    assert!(checked_relative(Path::new("fixtures"), "a\\b").is_err());
    assert_eq!(xml_escape("<bad \"a\"> & 'b'"), "&lt;bad &quot;a&quot;&gt; &amp; &apos;b&apos;");
}

#[test]
fn official_pinned_altair_bls_vectors() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/official-general");
    let manifest = inventory(&fixtures).unwrap();
    let report = run(&fixtures, &manifest, &focused());
    assert_eq!(report.cases.len(), 20);
    assert!(report.passed(), "{report:#?}");
    assert!(!report.conformance_passed());
}

#[test]
fn successful_local_inventory_cannot_claim_complete_conformance() {
    let report = Report {
        spec_release: SPEC_RELEASE.into(),
        complete_matrix: true,
        full_corpus: None,
        errors: vec![],
        cases: vec![CaseResult {
            path: "invented".into(),
            outcome: Outcome::Passed,
            message: String::new(),
            seconds: 0.0,
            spec_only_bls_disabled: false,
            spec_only_execution_mock: false,
            spec_only_data_availability_mock: false,
            spec_only_asserted_graph: false,
        }],
    };
    assert!(report.passed());
    assert!(!report.conformance_passed());
}

#[test]
fn unquoted_yaml_uint256_is_not_rounded_by_generic_yaml_numbers() {
    let directory = TempDir::new().unwrap();
    let value = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    write_case(
        directory.path(),
        "general/phase0/ssz_generic/uints/valid/uint_256_max",
        &[("value.yaml", value.as_bytes().to_vec())],
    );
    let case = discover(directory.path()).unwrap().pop().unwrap();
    assert_eq!(encode_value(&case, &yaml_ssz_value(&case, 1000).unwrap()).unwrap(), vec![255; 32]);
}

#[test]
fn independently_generated_gloas_vectors_run_through_static_handlers() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../crates/embedded-beacon/primitives/testdata/generated-gloas");
    let metadata = serde_json::from_slice::<serde_json::Value>(
        &reth_fs_util::read(source.join("manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["kind"], "generated structural SSZ fixtures");
    assert_eq!(metadata["consensus_spec_pin"], SPEC_RELEASE);
    let cases = metadata["cases"].as_object().unwrap();
    assert_eq!(cases.len(), 11);
    let directory = TempDir::new().unwrap();
    for (name, metadata) in cases {
        let handler = match name.strip_suffix("-minimal").unwrap_or(name.as_str()) {
            "bid" => "ExecutionPayloadBid",
            "signed-bid" => "SignedExecutionPayloadBid",
            "ptc-data" => "PayloadAttestationData",
            "ptc-vote" => "PayloadAttestation",
            "indexed-ptc" => "IndexedPayloadAttestation",
            "payload" => "ExecutionPayload",
            "requests" => "ExecutionRequests",
            "envelope" => "ExecutionPayloadEnvelope",
            "signed-envelope" => "SignedExecutionPayloadEnvelope",
            other => panic!("new generated schema needs a runner handler: {other}"),
        };
        let preset = metadata["preset"].as_str().unwrap();
        let bytes = reth_fs_util::read(source.join(format!("{name}.ssz"))).unwrap();
        let root = reth_fs_util::read(source.join(format!("{name}.root"))).unwrap();
        assert_eq!(bytes.len() as u64, metadata["ssz_length"].as_u64().unwrap());
        assert_eq!(hex(&root), metadata["root"].as_str().unwrap());
        write_case(
            directory.path(),
            &format!("{preset}/gloas/ssz_static/{handler}/generated_structural/{name}"),
            &[
                ("serialized.ssz_snappy", snap::raw::Encoder::new().compress_vec(&bytes).unwrap()),
                ("roots.yaml", format!("root: '{}'\n", hex(&root)).into_bytes()),
            ],
        );
    }
    let report = run(directory.path(), &inventory(directory.path()).unwrap(), &focused());
    assert_eq!(report.cases.len(), 11);
    assert!(report.passed(), "{report:#?}");
    assert!(!report.conformance_passed());
}

#[test]
fn published_phase0_complete_containers_match_roots_and_canonical_bytes() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let data = reth_fs_util::read(base.join("official-phase0-manifest.json")).unwrap();
    assert_eq!(
        hex(&reth_embedded_beacon_primitives::sha256(&data)),
        "0x50b8dcdbcd96f6f9d5c54193fcce973e0af6bca4f745dc1e30f56213212146eb"
    );
    let manifest = serde_json::from_slice::<FixtureManifest>(&data).unwrap();
    let report = run(&base.join("official-phase0"), &manifest, &focused());
    assert_eq!(report.cases.len(), 6);
    assert!(report.passed(), "{report:#?}");
    assert!(!report.conformance_passed());
}

#[test]
fn untargeted_complete_fork_schema_stays_an_unsupported_failure() {
    let directory = TempDir::new().unwrap();
    write_case(
        directory.path(),
        "minimal/heze/ssz_static/BeaconState/ssz_random/not_targeted",
        &[("serialized.ssz_snappy", vec![0])],
    );
    let report = run(directory.path(), &inventory(directory.path()).unwrap(), &focused());
    assert_eq!(report.cases.len(), 1);
    assert_eq!(report.cases[0].outcome, Outcome::Unsupported);
    assert!(!report.passed());
    assert!(!report.conformance_passed());
}

#[test]
fn published_minimal_upgrade_containers_cover_every_targeted_fork() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let data = reth_fs_util::read(base.join("official-minimal-upgrades-manifest.json")).unwrap();
    assert_eq!(
        hex(&reth_embedded_beacon_primitives::sha256(&data)),
        "0xfbf97597761985ae182b5a6a2ec7851a50670afbb8667712b357ffee6b0e9e45"
    );
    let manifest = serde_json::from_slice::<FixtureManifest>(&data).unwrap();
    let report = run(&base.join("official-minimal-upgrades"), &manifest, &focused());
    assert_eq!(report.cases.len(), 21);
    assert!(report.passed(), "{report:#?}");
    for fork in &FORKS[1..] {
        for schema in ["BeaconState", "BeaconBlock", "SignedBeaconBlock"] {
            assert!(report.cases.iter().any(|case| {
                case.path.starts_with(&format!("minimal/{fork}/ssz_static/{schema}/")) &&
                    case.outcome == Outcome::Passed
            }));
        }
    }
    assert!(!report.conformance_passed());
}

#[test]
fn published_minimal_phase0_processing_and_operations() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let root = base.join("official-minimal-phase0-processing");
    let manifest: FixtureManifest = serde_json::from_slice(
        &std::fs::read(base.join("official-minimal-phase0-processing-manifest.json")).unwrap(),
    )
    .unwrap();
    let report = run(&root, &manifest, &focused());
    assert_eq!(report.cases.len(), 70);
    assert!(report.passed(), "{:?}", report.cases);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!report.conformance_passed());
}

#[test]
fn published_artificial_bls_disabled_context_is_explicit_and_not_runtime_conformance() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let manifest: FixtureManifest = serde_json::from_slice(
        &std::fs::read(base.join("official-spec-only-phase0-manifest.json")).unwrap(),
    )
    .unwrap();
    let report = run(&base.join("official-spec-only-phase0"), &manifest, &focused());
    assert_eq!(report.cases.len(), 3);
    assert!(report.passed(), "{:?}", report.cases);
    assert!(report.cases.iter().all(|case| case.spec_only_bls_disabled));
    assert!(!report.conformance_passed());
}

#[test]
fn published_minimal_altair_processing_covers_every_epoch_component() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let data =
        std::fs::read(base.join("official-minimal-altair-processing-manifest.json")).unwrap();
    assert_eq!(
        hex(&reth_embedded_beacon_primitives::sha256(&data)),
        "0x6f20f281d7937f78166491bd444d81eceedf6ba2e85371a6e8cfd55ef316db52"
    );
    let manifest = serde_json::from_slice::<FixtureManifest>(&data).unwrap();
    let report = run(&base.join("official-minimal-altair-processing"), &manifest, &focused());
    assert_eq!(report.cases.len(), 16);
    assert!(report.passed(), "{report:#?}");
    assert!(report.cases.iter().all(|case| !case.spec_only_bls_disabled));
    assert!(!report.conformance_passed());
}

#[test]
fn published_minimal_altair_fork_choice_uses_actual_private_store_events() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let data =
        std::fs::read(base.join("official-minimal-altair-fork-choice-manifest.json")).unwrap();
    assert_eq!(
        hex(&reth_embedded_beacon_primitives::sha256(&data)),
        "0x8d7a70bebcd532fd8f53209db9f3c6934f0de8b037f87b69a219c762967f1e61"
    );
    let manifest = serde_json::from_slice::<FixtureManifest>(&data).unwrap();
    let report = run(&base.join("official-minimal-altair-fork-choice"), &manifest, &focused());
    assert_eq!(report.cases.len(), 7);
    assert!(report.passed(), "{report:#?}");
    assert!(!report.conformance_passed());
}

/// Run an authenticated optional published corpus without requiring a second CLI executable.
/// This is explicitly ignored in default unit tests because the archive lives under ignored work.
#[test]
#[ignore = "requires an independently authenticated full published fixture corpus"]
fn authenticated_published_corpus() {
    let fixture = std::env::var("BEACON_FIXTURE_ROOT").expect("fixture root required");
    let manifest_path = std::env::var("BEACON_FIXTURE_MANIFEST").expect("manifest required");
    let expected =
        std::env::var("BEACON_FIXTURE_SHA256").expect("independent manifest SHA required");
    let expected_cases: usize =
        std::env::var("BEACON_FIXTURE_CASES").expect("exact case count required").parse().unwrap();
    let output = std::env::var("BEACON_FIXTURE_REPORT").expect("report destination required");
    let manifest =
        load_manifest(Path::new(&manifest_path), root_from_hex(&expected).unwrap()).unwrap();
    let mut options = focused();
    if let Ok(progress) = std::env::var("BEACON_FIXTURE_PROGRESS_EVERY") {
        options.progress_every = progress.parse().expect("invalid fixture progress interval");
    }
    if let Ok(workers) = std::env::var("BEACON_FIXTURE_AUTHENTICATION_WORKERS") {
        options.authentication_workers =
            workers.parse().expect("invalid authentication worker count");
    }
    let report = run(Path::new(&fixture), &manifest, &options);
    std::fs::write(format!("{output}.json"), serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    std::fs::write(format!("{output}.xml"), report.junit()).unwrap();
    assert_eq!(report.cases.len(), expected_cases);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for case in &report.cases {
        if case.outcome != Outcome::Passed {
            eprintln!("{}: {:?}: {}", case.path, case.outcome, case.message);
        }
    }
    assert!(!report.conformance_passed());
    assert!(
        report.passed(),
        "published corpus contains failures; exact report written to {output}"
    );
}

#[test]
fn published_config_preserves_unquoted_versions_addresses_and_wide_difficulty() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("config.yaml");
    let source = "GENESIS_FORK_VERSION: 0x00000001\nDEPOSIT_CONTRACT_ADDRESS: 0xffffffffffffffffffffffffffffffffffffffff\nTERMINAL_TOTAL_DIFFICULTY: 115792089237316195423570985008687907853269984665640564039457584007913129638912\nSLOT_DURATION_MS: 6000\nFULU_FORK_EPOCH: 18446744073709551615\n";
    std::fs::write(&path, source).unwrap();
    let parsed = config_yaml(&path, 4096).unwrap();
    assert_eq!(fixed_hex::<4>(&parsed["GENESIS_FORK_VERSION"]).unwrap(), [0, 0, 0, 1]);
    assert_eq!(fixed_hex::<20>(&parsed["DEPOSIT_CONTRACT_ADDRESS"]).unwrap(), [255; 20]);
    assert_eq!(
        parsed["TERMINAL_TOTAL_DIFFICULTY"].as_str(),
        Some("115792089237316195423570985008687907853269984665640564039457584007913129638912")
    );
    assert_eq!(parsed["SLOT_DURATION_MS"].as_u64(), Some(6000));
    assert_eq!(parsed["FULU_FORK_EPOCH"].as_u64(), Some(u64::MAX));
    assert_eq!(std::fs::read_to_string(path).unwrap(), source);
}

#[test]
fn manifest_loader_streams_exact_digest_and_enforces_its_separate_resource_bound() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("manifest.json");
    let manifest = FixtureManifest {
        spec_release: SPEC_RELEASE.into(),
        required_suites: vec!["minimal/phase0/sanity/slots/suite".into()],
        files: BTreeMap::from([(
            "minimal/phase0/sanity/slots/suite/case/pre.ssz_snappy".into(),
            hex(&[1; 32]),
        )]),
    };
    let bytes = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let root = reth_embedded_beacon_primitives::sha256(&bytes);
    assert_eq!(
        load_manifest_with_limit(&path, root, bytes.len() as u64).unwrap().files,
        manifest.files
    );
    assert_eq!(
        load_manifest_with_limit(&path, root, (bytes.len() - 1) as u64).unwrap_err(),
        "manifest exceeds byte limit"
    );
    assert_eq!(
        load_manifest_with_limit(&path, [0; 32], bytes.len() as u64).unwrap_err(),
        "manifest SHA-256 differs from separately supplied digest"
    );
}

#[test]
fn artificial_compliance_metadata_selects_and_labels_only_explicit_fixture_mode() {
    let directory = TempDir::new().unwrap();
    let case = Case {
        path: directory.path().to_owned(),
        relative: "minimal/altair/fork_choice_compliance/block_weight_test/suite/case".into(),
        preset: "minimal".into(),
        fork: "altair".into(),
        runner: "fork_choice_compliance".into(),
        handler: "block_weight_test".into(),
        suite: "suite".into(),
        name: "case".into(),
    };
    let metadata = directory.path().join("meta.yaml");
    std::fs::write(&metadata, "bls_setting: 2\n").unwrap();
    assert!(fixture_disables_bls(&case, 4096).unwrap());
    std::fs::write(&metadata, "bls_setting: 1\n").unwrap();
    assert!(!fixture_disables_bls(&case, 4096).unwrap());
    std::fs::write(&metadata, "bls_setting: 3\n").unwrap();
    assert_eq!(
        fixture_disables_bls(&case, 4096).unwrap_err(),
        "fixture BLS setting must be 0, 1 or 2"
    );
}

#[test]
fn fork_choice_infrastructure_errors_cannot_satisfy_invalid_event_expectations() {
    let invalid: Value = serde_yaml::from_str("valid: false\n").unwrap();
    for error in [
        StoreError::Overflow,
        StoreError::ResourceLimit,
        StoreError::ValidationUnavailable(Fork::Gloas),
    ] {
        let message = error.to_string();
        assert_eq!(fork_choice_event_result(Err(error), &invalid).unwrap_err(), message);
    }
    assert!(fork_choice_event_result(Err(StoreError::UnknownRoot), &invalid).is_ok());
}

#[test]
fn strict_genesis_adapter_rejects_artificial_signature_metadata() {
    let directory = TempDir::new().unwrap();
    let case = Case {
        path: directory.path().to_owned(),
        relative: "minimal/phase0/genesis/initialization/pyspec_tests/case".into(),
        preset: "minimal".into(),
        fork: "phase0".into(),
        runner: "genesis".into(),
        handler: "initialization".into(),
        suite: "pyspec_tests".into(),
        name: "case".into(),
    };
    std::fs::write(directory.path().join("meta.yaml"), "bls_setting: 2\n").unwrap();
    assert_eq!(
        genesis::run_case(&case, 4096),
        Err((
            Outcome::Failed,
            "strict genesis fixtures cannot disable deposit signature verification".into()
        ))
    );
}

/// Execute the fresh exact primary/compliance union from one immutable optimized libtest runner.
/// This entry never reads or combines older case reports.
#[test]
#[ignore = "requires both independently authenticated original full source corpora"]
fn authenticated_complete_published_corpus() {
    assert_eq!(
        std::env::var("BEACON_FIXTURE_COMPLETE").as_deref(),
        Ok("true"),
        "an explicit complete invocation is required"
    );
    let primary_root = std::env::var("BEACON_PRIMARY_FIXTURE_ROOT").expect("primary root required");
    let primary_path =
        std::env::var("BEACON_PRIMARY_FIXTURE_MANIFEST").expect("primary manifest required");
    let primary_sha = std::env::var("BEACON_PRIMARY_FIXTURE_SHA256")
        .expect("independent primary manifest SHA required");
    let compliance_root =
        std::env::var("BEACON_COMPLIANCE_FIXTURE_ROOT").expect("compliance root required");
    let compliance_path =
        std::env::var("BEACON_COMPLIANCE_FIXTURE_MANIFEST").expect("compliance manifest required");
    let compliance_sha = std::env::var("BEACON_COMPLIANCE_FIXTURE_SHA256")
        .expect("independent compliance manifest SHA required");
    let output =
        std::env::var("BEACON_FIXTURE_REPORT").expect("single report destination required");
    let primary_expected = root_from_hex(&primary_sha).unwrap();
    let compliance_expected = root_from_hex(&compliance_sha).unwrap();
    assert_eq!(primary_expected, root_from_hex(PRIMARY_MANIFEST_SHA256).unwrap());
    assert_eq!(compliance_expected, root_from_hex(COMPLIANCE_MANIFEST_SHA256).unwrap());
    let primary = load_manifest(Path::new(&primary_path), primary_expected).unwrap();
    let compliance = load_manifest(Path::new(&compliance_path), compliance_expected).unwrap();
    let mut options = RunOptions::default();
    if let Ok(progress) = std::env::var("BEACON_FIXTURE_PROGRESS_EVERY") {
        options.progress_every = progress.parse().expect("invalid fixture progress interval");
    }
    if let Ok(workers) = std::env::var("BEACON_FIXTURE_AUTHENTICATION_WORKERS") {
        options.authentication_workers =
            workers.parse().expect("invalid authentication worker count");
    }
    let report = run_complete(
        Path::new(&primary_root),
        &primary,
        Path::new(&compliance_root),
        &compliance,
        &options,
    );
    reth_fs_util::write(format!("{output}.json"), serde_json::to_vec_pretty(&report).unwrap())
        .unwrap();
    reth_fs_util::write(format!("{output}.xml"), report.junit()).unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.cases.len(), 73_992);
    assert!(
        report.conformance_passed(),
        "complete corpus contains failures; exact report written to {output}"
    );
}
