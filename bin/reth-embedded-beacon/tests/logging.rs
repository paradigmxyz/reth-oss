//! Actual portable-console regressions: levels, filtering, quiet output and JSON status.

#![cfg(not(feature = "embedded-beacon"))]

use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_reth-embedded-beacon"))
        .env_remove("RUST_LOG")
        .args(args)
        .output()
        .expect("portable launcher runs")
}

fn levels(output: &Output) -> Vec<&str> {
    std::str::from_utf8(&output.stderr)
        .expect("console logs are UTF-8")
        .lines()
        .map(|line| line.split_whitespace().nth(1).expect("timestamp followed by log level"))
        .collect()
}

#[test]
fn default_info_suppresses_debug_and_verbose_enables_it() {
    let info = run(&["node"]);
    assert_eq!(info.status.code(), Some(1));
    assert_eq!(levels(&info), ["INFO", "ERROR"]);
    assert!(info.stdout.is_empty());
    let debug = run(&["node", "-vvvv"]);
    assert_eq!(debug.status.code(), Some(1));
    assert_eq!(levels(&debug), ["INFO", "DEBUG", "ERROR"]);
    assert!(debug.stdout.is_empty());
}

#[test]
fn quiet_preserves_refusal_and_status_remains_json() {
    let quiet = run(&["node", "-q"]);
    assert_eq!(quiet.status.code(), Some(1));
    assert!(quiet.stdout.is_empty());
    assert!(quiet.stderr.is_empty());
    let status = run(&["status"]);
    assert!(status.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&status.stdout).expect("pure status JSON");
    assert_eq!(value["target_fork"], "gloas");
    assert!(status.stderr.is_empty());
}

#[test]
fn explicit_filter_and_error_verbosity_are_respected() {
    let debug = run(&["node", "--log.stdout.filter", "beacon::node=debug"]);
    assert_eq!(debug.status.code(), Some(1));
    assert_eq!(levels(&debug), ["INFO", "DEBUG", "ERROR"]);
    let error = run(&["node", "-v"]);
    assert_eq!(error.status.code(), Some(1));
    assert_eq!(levels(&error), ["ERROR"]);
}
