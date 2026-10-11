//! Actual local HTTP exchanges preserve genuine signed checkpoint bytes and explicit trust.

use super::*;
use crate::{
    bootstrap::boundary_replay::tests::fixture, CheckpointBlockFormat, SszCheckpointVerifier,
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread::{self, JoinHandle},
};

struct Reply {
    path: String,
    status: u16,
    headers: String,
    body: Vec<u8>,
    accept: &'static str,
}

fn reply(path: String, body: Vec<u8>, version: &str) -> Reply {
    Reply { path, status: 200, headers: format!("Content-Type: application/octet-stream\r\nEth-Consensus-Version: {version}\r\nContent-Length: {}\r\n", body.len()), body, accept: "application/octet-stream" }
}

fn server(replies: Vec<Reply>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let task = thread::spawn(move || {
        for reply in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                assert_eq!(stream.read(&mut byte).unwrap(), 1);
                request.push(byte[0]);
                assert!(request.len() <= 4096);
            }
            let request = String::from_utf8(request).unwrap();
            assert_eq!(request.lines().next().unwrap(), format!("GET {} HTTP/1.1", reply.path));
            assert!(request
                .lines()
                .any(|line| line.eq_ignore_ascii_case(&format!("accept: {}", reply.accept))));
            assert!(request
                .lines()
                .any(|line| line.eq_ignore_ascii_case("user-agent: reth-embedded-beacon")));
            let header = format!(
                "HTTP/1.1 {} response\r\n{}Connection: close\r\n\r\n",
                reply.status, reply.headers
            );
            stream.write_all(header.as_bytes()).unwrap();
            // Header rejection can deliberately close before consuming this untrusted body.
            let _ = stream.write_all(&reply.body);
        }
    });
    (url, task)
}

#[tokio::test]
async fn actual_http_fetches_exact_signed_block_and_two_distinct_states_then_authenticates_replay()
{
    let fixture = fixture(true);
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let state_root = fixture.post.hash_tree_root().unwrap();
    let replies = vec![
        reply(
            format!("/eth/v2/beacon/blocks/{}", root_hex(fixture.trusted.root)),
            fixture.block.clone(),
            "phase0",
        ),
        reply(
            format!("/eth/v2/debug/beacon/states/{}", root_hex(state_root)),
            post.clone(),
            "phase0",
        ),
        reply("/eth/v2/debug/beacon/states/8".into(), boundary.clone(), "altair"),
    ];
    let (url, task) = server(replies);
    let limits = BootstrapDecodeLimits::default();
    let source =
        CheckpointSource::new(&url, fixture.config.clone(), limits, Duration::from_secs(10))
            .unwrap();
    let inputs = source.download(fixture.trusted).await.unwrap();
    task.join().unwrap();
    assert_eq!(inputs.block_ssz.as_ref(), fixture.block);
    assert_eq!(inputs.state_ssz.as_ref(), boundary);
    assert_eq!(inputs.post_block_state_ssz.as_ref(), post);
    assert_eq!(inputs.block_slot, 1);
    assert!(!Arc::ptr_eq(&inputs.state_ssz, &inputs.post_block_state_ssz));
    let verifier =
        SszCheckpointVerifier::new(fixture.config, limits, CheckpointBlockFormat::Signed).unwrap();
    let proof = verifier
        .verify_checkpoint_replay(
            &inputs.state_ssz,
            &inputs.post_block_state_ssz,
            &inputs.block_ssz,
            inputs.trusted,
            fixture.now,
            4,
            7,
        )
        .unwrap();
    assert_eq!(proof.verified().state_root(), fixture.boundary.hash_tree_root().unwrap());
    assert_eq!(proof.post_block_fork(), Fork::Phase0);
    assert!(proof.matches_inputs(&boundary, &post, &fixture.block, CheckpointBlockFormat::Signed));
}

#[tokio::test]
async fn actual_http_wrong_root_and_advertised_schema_do_not_fetch_any_state() {
    let fixture = fixture(false);
    for wrong_version in [false, true] {
        let trusted = if wrong_version {
            fixture.trusted
        } else {
            Checkpoint { root: [99; 32], ..fixture.trusted }
        };
        let (url, task) = server(vec![reply(
            format!("/eth/v2/beacon/blocks/{}", root_hex(trusted.root)),
            fixture.block.clone(),
            if wrong_version { "altair" } else { "phase0" },
        )]);
        let source = CheckpointSource::new(
            &url,
            fixture.config.clone(),
            BootstrapDecodeLimits::default(),
            Duration::from_secs(5),
        )
        .unwrap();
        let error = source.download(trusted).await.unwrap_err();
        assert!(matches!(error, CheckpointDownloadError::Version) == wrong_version);
        assert!(matches!(error, CheckpointDownloadError::Linkage) != wrong_version);
        task.join().unwrap();
    }
}

#[tokio::test]
async fn actual_http_rejects_redirect_compressed_json_unknown_version_and_declared_size() {
    let fixture = fixture(false);
    for (status, headers, expected) in [
        (302, "Location: https://127.0.0.1/elsewhere\r\n", 0),
        (200, "Content-Type: application/json\r\nEth-Consensus-Version: phase0\r\n", 1),
        (200, "Content-Type: application/octet-stream\r\nEth-Consensus-Version: phase0\r\nContent-Encoding: gzip\r\n", 1),
        (200, "Content-Type: application/octet-stream\r\nEth-Consensus-Version: unknown\r\n", 2),
        (200, "Content-Type: application/octet-stream\r\nEth-Consensus-Version: phase0\r\nContent-Length: 10485761\r\n", 3),
    ] {
        let (url, task) = server(vec![Reply { path: format!("/eth/v2/beacon/blocks/{}", root_hex(fixture.trusted.root)), status, headers: headers.into(), body: vec![], accept: "application/octet-stream" }]);
        let source = CheckpointSource::new(&url, fixture.config.clone(), BootstrapDecodeLimits::default(), Duration::from_secs(5)).unwrap();
        let error = source.download(fixture.trusted).await.unwrap_err();
        match expected {
            0 => assert!(matches!(error, CheckpointDownloadError::Status(302))),
            1 => assert!(matches!(error, CheckpointDownloadError::Encoding)),
            2 => assert!(matches!(error, CheckpointDownloadError::Version)),
            3 => assert!(matches!(error, CheckpointDownloadError::TooLarge)),
            _ => unreachable!(),
        }
        task.join().unwrap();
    }
}

#[test]
fn endpoint_policy_bounds_and_schema_names_are_exact() {
    let config = fixture(false).config;
    for url in [
        "http://example.test/",
        "https://user:password@example.test/",
        "https://example.test/?key=value",
        "https://example.test/#fragment",
        "file:///tmp/state",
    ] {
        assert!(matches!(
            CheckpointSource::new(
                url,
                config.clone(),
                BootstrapDecodeLimits::default(),
                Duration::from_secs(5)
            ),
            Err(CheckpointDownloadError::Configuration)
        ));
    }
    assert!(CheckpointSource::new(
        "https://example.test/prefix",
        config.clone(),
        BootstrapDecodeLimits::default(),
        Duration::from_secs(5)
    )
    .is_ok());
    for timeout in [Duration::ZERO, Duration::from_secs(601)] {
        assert!(matches!(
            CheckpointSource::new(
                "http://127.0.0.1/",
                config.clone(),
                BootstrapDecodeLimits::default(),
                timeout
            ),
            Err(CheckpointDownloadError::Configuration)
        ));
    }
    assert_eq!(parse_version("gloas"), Some(Fork::Gloas));
    assert_eq!(parse_version("GLOAS"), None);
    assert_eq!(parse_version("gloas\n"), None);
    assert_eq!(root_hex([0xab; 32]), format!("0x{}", "ab".repeat(32)));
}

#[cfg(feature = "peer-runtime")]
#[test]
fn private_checkpoint_routes_require_complete_isolated_identity_and_literal_private_addresses() {
    use reth_embedded_beacon_config::{DevnetConfig, ForkActivation, ForkSchedule};

    let mut consensus = ChainConfig::hoodi();
    consensus.name = "checkpoint-devnet".into();
    consensus.chain_id = 3151908;
    consensus.genesis_validators_root = [2; 32];
    consensus.seconds_per_slot = 6;
    consensus.blob_schedule.clear();
    consensus.forks = ForkSchedule::new(
        [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
            Fork::Gloas,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, fork)| ForkActivation {
            fork,
            epoch: index as u64,
            version: [index as u8, 0, 0, 42],
        })
        .collect(),
    )
    .unwrap();
    let mut devnet = DevnetConfig {
        schema_version: 1,
        execution_genesis_hash: [3; 32],
        terminal_block_hash: [3; 32],
        terminal_block_hash_activation_epoch: 2,
        consensus,
    };
    devnet.validate().unwrap();
    let limits = BootstrapDecodeLimits::default();
    let timeout = Duration::from_secs(5);
    for url in ["http://172.18.0.3:5052/", "http://[fd00::3]:5052/", "http://127.0.0.1:5052/"] {
        assert!(CheckpointSource::for_devnet(url, &devnet, limits, timeout).is_ok());
        if !url.contains("127.0.0.1") {
            assert!(CheckpointSource::new(url, devnet.consensus.clone(), limits, timeout).is_err());
        }
    }
    for url in [
        "http://service:5052/",
        "http://8.8.8.8/",
        "https://8.8.8.8/",
        "http://0.0.0.0/",
        "http://169.254.1.2/",
        "http://[fe80::3]/",
    ] {
        assert!(matches!(
            CheckpointSource::for_devnet(url, &devnet, limits, timeout),
            Err(CheckpointDownloadError::Configuration)
        ));
    }
    devnet.consensus.chain_id = 1;
    assert!(matches!(
        CheckpointSource::for_devnet("http://172.18.0.3/", &devnet, limits, timeout),
        Err(CheckpointDownloadError::Configuration)
    ));
}

fn without_version(mut response: Reply) -> Reply {
    response.headers = response
        .headers
        .split_inclusive("\r\n")
        .filter(|line| !line.starts_with("Eth-Consensus-Version:"))
        .collect();
    response
}

fn metadata_reply(path: String, version: &str, slot: u64, state_root: Root) -> Reply {
    let body = serde_json::to_vec(&serde_json::json!({
        "version": version,
        "data": {"message": {"slot": slot.to_string(), "state_root": root_hex(state_root)}}
    }))
    .unwrap();
    Reply {
        path,
        status: 200,
        headers: format!("Content-Type: application/json\r\nContent-Length: {}\r\n", body.len()),
        body,
        accept: "application/json",
    }
}

#[tokio::test]
async fn missing_block_header_uses_exact_root_json_then_authenticates_original_ssz_replay() {
    let fixture = fixture(true);
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let state_root = fixture.post.hash_tree_root().unwrap();
    let path = format!("/eth/v2/beacon/blocks/{}", root_hex(fixture.trusted.root));
    let (url, task) = server(vec![
        without_version(reply(path.clone(), fixture.block.clone(), "phase0")),
        metadata_reply(path, "phase0", 1, state_root),
        reply(
            format!("/eth/v2/debug/beacon/states/{}", root_hex(state_root)),
            post.clone(),
            "phase0",
        ),
        reply("/eth/v2/debug/beacon/states/8".into(), boundary.clone(), "altair"),
    ]);
    let limits = BootstrapDecodeLimits::default();
    let source =
        CheckpointSource::new(&url, fixture.config.clone(), limits, Duration::from_secs(10))
            .unwrap();
    let inputs = source.download(fixture.trusted).await.unwrap();
    task.join().unwrap();
    assert_eq!(inputs.block_ssz.as_ref(), fixture.block);
    assert_eq!(inputs.post_block_state_ssz.as_ref(), post);
    assert_eq!(inputs.state_ssz.as_ref(), boundary);
    let verifier =
        SszCheckpointVerifier::new(fixture.config, limits, CheckpointBlockFormat::Signed).unwrap();
    let proof = verifier
        .verify_checkpoint_replay(
            &inputs.state_ssz,
            &inputs.post_block_state_ssz,
            &inputs.block_ssz,
            inputs.trusted,
            fixture.now,
            4,
            7,
        )
        .unwrap();
    assert!(proof.matches_inputs(&boundary, &post, &fixture.block, CheckpointBlockFormat::Signed));
}

#[tokio::test]
async fn exact_root_json_cannot_override_original_block_root_slot_state_or_scheduled_fork() {
    let fixture = fixture(false);
    let state_root = fixture.post.hash_tree_root().unwrap();
    for (version, slot, metadata_root, trusted, version_error) in [
        ("altair", 1, state_root, fixture.trusted, true),
        ("phase0", 2, state_root, fixture.trusted, false),
        ("phase0", 1, [99; 32], fixture.trusted, false),
        ("phase0", 1, state_root, Checkpoint { root: [99; 32], ..fixture.trusted }, false),
    ] {
        let path = format!("/eth/v2/beacon/blocks/{}", root_hex(trusted.root));
        let (url, task) = server(vec![
            without_version(reply(path.clone(), fixture.block.clone(), "phase0")),
            metadata_reply(path, version, slot, metadata_root),
        ]);
        let source = CheckpointSource::new(
            &url,
            fixture.config.clone(),
            BootstrapDecodeLimits::default(),
            Duration::from_secs(5),
        )
        .unwrap();
        let error = source.download(trusted).await.unwrap_err();
        assert_eq!(matches!(error, CheckpointDownloadError::Version), version_error);
        assert_eq!(matches!(error, CheckpointDownloadError::Linkage), !version_error);
        task.join().unwrap();
    }
}

#[tokio::test]
async fn missing_state_schema_header_remains_an_error_without_json_fallback() {
    let fixture = fixture(false);
    let state_root = fixture.post.hash_tree_root().unwrap();
    let (url, task) = server(vec![
        reply(
            format!("/eth/v2/beacon/blocks/{}", root_hex(fixture.trusted.root)),
            fixture.block.clone(),
            "phase0",
        ),
        without_version(reply(
            format!("/eth/v2/debug/beacon/states/{}", root_hex(state_root)),
            fixture.post.to_ssz().unwrap(),
            "phase0",
        )),
    ]);
    let source = CheckpointSource::new(
        &url,
        fixture.config,
        BootstrapDecodeLimits::default(),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(matches!(
        source.download(fixture.trusted).await.unwrap_err(),
        CheckpointDownloadError::Version
    ));
    task.join().unwrap();
}

#[tokio::test]
async fn metadata_fallback_rejects_conflicting_header_unknown_version_and_oversize_json() {
    let fixture = fixture(false);
    let state_root = fixture.post.hash_tree_root().unwrap();
    for kind in 0..3 {
        let path = format!("/eth/v2/beacon/blocks/{}", root_hex(fixture.trusted.root));
        let mut metadata = metadata_reply(
            path.clone(),
            if kind == 1 { "unknown" } else { "phase0" },
            1,
            state_root,
        );
        if kind == 0 {
            metadata.headers.push_str("Eth-Consensus-Version: altair\r\n");
        } else if kind == 2 {
            metadata.headers =
                "Content-Type: application/json\r\nContent-Length: 31457281\r\n".into();
        }
        let (url, task) =
            server(vec![without_version(reply(path, fixture.block.clone(), "phase0")), metadata]);
        let source = CheckpointSource::new(
            &url,
            fixture.config.clone(),
            BootstrapDecodeLimits::default(),
            Duration::from_secs(5),
        )
        .unwrap();
        let error = source.download(fixture.trusted).await.unwrap_err();
        if kind == 2 {
            assert!(matches!(error, CheckpointDownloadError::TooLarge));
        } else {
            assert!(matches!(error, CheckpointDownloadError::Version));
        }
        task.join().unwrap();
    }
}
