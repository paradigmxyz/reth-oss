//! Pure head-transition and injected writer checks need no sockets or consensus overrides.

use super::*;
use reth_embedded_beacon_primitives::BeaconBlockHeader;

fn head(index: u8) -> HeadObservation {
    HeadObservation {
        root: [index; 32],
        slot: u64::from(index),
        execution_acknowledged: true,
        engine_closed: false,
        optimistic: false,
    }
}

fn object(head: HeadObservation, fork: Fork) -> ObjectObservation {
    ObjectObservation {
        fork,
        block_root: head.root,
        header: BeaconBlockHeader { slot: head.slot, ..Default::default() },
        canonical: true,
        finalized: false,
        execution_optimistic: false,
        json: Vec::new(),
    }
}

fn banner() -> ForkBanner {
    ForkBanner::new(OutputOptions::resolve("never", "terminal", false, None))
}

#[test]
fn canonical_fulu_to_gloas_announces_once_and_never_repeats_after_reorg() {
    let mut banner = banner();
    assert!(!banner.observe(head(1), &object(head(1), Fork::Fulu)));
    assert!(!banner.wants_header(head(1)));
    assert!(banner.observe(head(2), &object(head(2), Fork::Gloas)));
    assert!(!banner.observe(head(2), &object(head(2), Fork::Gloas)));
    assert!(!banner.observe(head(3), &object(head(3), Fork::Fulu)));
    assert!(!banner.observe(head(4), &object(head(4), Fork::Gloas)));
    assert!(!banner.wants_header(head(5)));
}

#[test]
fn starting_at_gloas_or_skipping_observed_fulu_is_not_a_transition() {
    for initial in [Fork::Gloas, Fork::Electra] {
        let mut banner = banner();
        assert!(!banner.observe(head(1), &object(head(1), initial)));
        assert!(!banner.observe(head(2), &object(head(2), Fork::Gloas)));
        assert!(!banner.observe(head(3), &object(head(3), Fork::Fulu)));
        assert!(!banner.observe(head(4), &object(head(4), Fork::Gloas)));
    }
}

#[test]
fn optimistic_unacknowledged_closed_and_mismatched_observations_cannot_announce() {
    let mut banner = banner();
    assert!(!banner.observe(head(1), &object(head(1), Fork::Fulu)));
    for candidate in [
        HeadObservation { optimistic: true, ..head(2) },
        HeadObservation { execution_acknowledged: false, ..head(2) },
        HeadObservation { engine_closed: true, ..head(2) },
    ] {
        assert!(!banner.wants_header(candidate));
        assert!(!banner.observe(candidate, &object(head(2), Fork::Gloas)));
    }
    let mut candidate = object(head(2), Fork::Gloas);
    candidate.canonical = false;
    assert!(!banner.observe(head(2), &candidate));
    let mut candidate = object(head(2), Fork::Gloas);
    candidate.execution_optimistic = true;
    assert!(!banner.observe(head(2), &candidate));
    candidate = object(head(3), Fork::Gloas);
    assert!(!banner.observe(head(2), &candidate));
    candidate = object(head(2), Fork::Gloas);
    candidate.header.slot = 3;
    assert!(!banner.observe(head(2), &candidate));
    assert!(banner.observe(head(2), &object(head(2), Fork::Gloas)));
}

#[derive(Default)]
struct Writer {
    writes: usize,
    bytes: Vec<u8>,
    fail: bool,
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        if self.fail {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn output_respects_color_modes_and_never_inserts_art_into_structured_formats() {
    for (color, terminal, style, ansi) in [
        ("always", false, None, true),
        ("never", true, None, false),
        ("auto", true, None, true),
        ("auto", false, None, false),
        ("always", true, Some("never"), false),
    ] {
        let banner = ForkBanner::new(OutputOptions::resolve(color, "terminal", terminal, style));
        let mut writer = Writer::default();
        banner.write(&mut writer).unwrap();
        assert_eq!(writer.writes, 1);
        assert!(writer.bytes.len() < 16 * 1024);
        assert_eq!(writer.bytes.contains(&0x1b), ansi);
        if !ansi {
            assert_eq!(writer.bytes, format!("{POLAR_BEAR}{TITLE}").as_bytes());
        }
    }
    for format in ["json", "logfmt"] {
        let mut writer = Writer::default();
        ForkBanner::new(OutputOptions::resolve("always", format, true, None))
            .write(&mut writer)
            .unwrap();
        assert_eq!(writer.writes, 0);
        assert!(writer.bytes.is_empty());
    }
}

#[test]
fn failed_banner_write_does_not_retry_or_reset_verified_transition() {
    let mut banner = banner();
    assert!(!banner.observe(head(1), &object(head(1), Fork::Fulu)));
    assert!(banner.observe(head(2), &object(head(2), Fork::Gloas)));
    let mut writer = Writer { fail: true, ..Default::default() };
    assert_eq!(banner.write(&mut writer).unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(writer.writes, 1);
    assert!(!banner.observe(head(3), &object(head(3), Fork::Gloas)));
}

#[test]
fn quiet_suppresses_the_automatic_mascot_even_if_another_log_layer_is_enabled() {
    let mut writer = Writer::default();
    ForkBanner::new(OutputOptions::resolve("always", "terminal", true, None).with_quiet(true))
        .write(&mut writer)
        .unwrap();
    assert_eq!(writer.writes, 0);
    assert_eq!(writer.bytes, Vec::<u8>::new());
}
