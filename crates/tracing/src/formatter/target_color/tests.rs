//! Local subscriber regressions for exact target styling and structured/file isolation.

use super::*;
use crate::LogFormat;
use std::{
    io,
    sync::{Arc, Mutex},
};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Buffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

fn emit() {
    tracing::info!(target: "beacon", block = 123, "exact target");
    tracing::warn!(target: "beacon::consensus", "descendant target");
    tracing::info!(target: "beaconish::consensus", "unrelated prefix");
    tracing::info!(target: "reth::node", "ordinary execution target");
}

#[test]
fn exact_and_descendant_console_targets_are_entirely_bright_cyan() {
    let output = Buffer::default();
    let sink = output.clone();
    let layer = tracing_subscriber::fmt::layer()
        .event_format(BrightCyanTargetFormat::new("beacon", false))
        .with_ansi(true)
        .with_writer(move || sink.clone());
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        let span = tracing::info_span!("sync", slot = 456);
        let _guard = span.enter();
        emit();
    });
    let text = output.text();
    let lines = text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4);
    for (line, expected) in lines.iter().zip([true, true, false, false]) {
        assert_eq!(line.starts_with("\x1b[96m"), expected);
        if expected {
            assert_eq!(line.matches('\x1b').count(), 2);
            assert!(line.ends_with("\x1b[0m"));
        }
    }
    assert!(lines[1].contains("WARN"));
    assert!(lines[0].contains("block=123"));
    assert!(lines[0].contains("slot=456"));
}

#[test]
fn ansi_disabled_delegates_native_plain_console_formatting() {
    let output = Buffer::default();
    let sink = output.clone();
    let layer = tracing_subscriber::fmt::layer()
        .event_format(BrightCyanTargetFormat::new("beacon", false))
        .with_ansi(false)
        .with_writer(move || sink.clone());
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        let span = tracing::info_span!("sync", slot = 456);
        let _guard = span.enter();
        emit();
    });
    let text = output.text();
    assert!(!text.as_bytes().contains(&0x1b));
    assert_eq!(text.lines().count(), 4);
}

#[test]
fn default_console_formatter_does_not_opt_in_beacon_bright_cyan() {
    let output = Buffer::default();
    let sink = output.clone();
    let layer = tracing_subscriber::fmt::layer().with_ansi(true).with_writer(move || sink.clone());
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        let span = tracing::info_span!("sync", slot = 456);
        let _guard = span.enter();
        emit();
    });
    assert!(!output.text().contains("\x1b[96m"));
}

#[test]
fn json_logfmt_and_file_layers_never_receive_target_escape_codes() {
    for format in [LogFormat::Json, LogFormat::LogFmt, LogFormat::Terminal] {
        let output = Buffer::default();
        let (writer, guard) = tracing_appender::non_blocking(output.clone());
        let layer = format.apply_with_bright_cyan_target(
            EnvFilter::new("trace"),
            None,
            false,
            Some(writer),
            Some("beacon"),
        );
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let span = tracing::info_span!("sync", slot = 456);
            let _guard = span.enter();
            emit();
        });
        drop(guard);
        let text = output.text();
        assert!(!text.as_bytes().contains(&0x1b));
        assert_eq!(text.lines().count(), 4);
        if format == LogFormat::Json {
            assert!(text.lines().all(|line| line.starts_with('{') && line.ends_with('}')));
        }
    }
}

#[test]
fn beacon_console_explains_only_exact_quic_reset_warning_and_preserves_severity() {
    for ansi in [false, true] {
        let output = Buffer::default();
        let sink = output.clone();
        let layer = tracing_subscriber::fmt::layer()
            .event_format(BrightCyanTargetFormat::new("beacon", false))
            .with_ansi(ansi)
            .with_writer(move || sink.clone());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!(target: "quinn_proto::endpoint", "duplicate reset token");
            tracing::warn!(target: "quinn_proto::endpoint", "duplicate reset token in another message");
            tracing::warn!(target: "other_quic::endpoint", "duplicate reset token");
            tracing::error!(target: "quinn_proto::endpoint", "duplicate reset token");
            tracing::warn!(target: "quinn_proto::endpoint", "another QUIC warning");
        });
        let text = output.text();
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 5);
        assert!(lines[0].contains("WARN"));
        assert!(lines[0]
            .contains("[beacon] QUIC peer reused a reset token; reset routing entry replaced"));
        assert_eq!(lines[0].starts_with("\x1b[96m"), ansi);
        assert!(lines[1].contains("duplicate reset token in another message"));
        assert!(lines[2].ends_with("duplicate reset token"));
        assert!(lines[3].contains("ERROR"));
        assert!(lines[3].ends_with("duplicate reset token"));
        assert!(lines[4].contains("another QUIC warning"));
        assert!(lines[1..].iter().all(|line| !line.starts_with("\x1b[96m")));
    }
}

#[test]
fn reset_warning_classification_requires_beacon_console_opt_in() {
    let output = Buffer::default();
    let sink = output.clone();
    let layer = tracing_subscriber::fmt::layer()
        .event_format(BrightCyanTargetFormat::new("other_component", false))
        .with_ansi(false)
        .with_writer(move || sink.clone());
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        tracing::warn!(target: "quinn_proto::endpoint", "duplicate reset token");
    });
    let text = output.text();
    assert!(text.contains("WARN"));
    assert!(text.trim_end().ends_with("duplicate reset token"));
    assert!(!text.contains("[beacon]"));
}

#[test]
fn reset_warning_structured_and_file_logs_keep_original_event() {
    for format in [LogFormat::Json, LogFormat::LogFmt, LogFormat::Terminal] {
        let output = Buffer::default();
        let (writer, guard) = tracing_appender::non_blocking(output.clone());
        let layer = format.apply_with_bright_cyan_target(
            EnvFilter::new("trace"),
            None,
            false,
            Some(writer),
            Some("beacon"),
        );
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!(target: "quinn_proto::endpoint", "duplicate reset token");
        });
        drop(guard);
        let text = output.text();
        assert!(text.contains("duplicate reset token"));
        assert!(!text.contains("[beacon]"));
        assert!(!text.as_bytes().contains(&0x1b));
    }
}
