//! Deterministic activity rendering and cancellation cleanup use only injected writers.

use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Default)]
struct Writer {
    bytes: Arc<Mutex<Vec<u8>>>,
    writes: Arc<AtomicUsize>,
    fail: bool,
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn work() -> CheckpointWork {
    CheckpointWork {
        epoch: 12,
        state_bytes: 1024 * 1024,
        post_block_state_bytes: 1024 * 1024,
        block_bytes: 1024 * 1024,
    }
}

#[test]
fn activity_moves_without_claiming_a_percentage_or_total_duration() {
    let writer = Writer::default();
    let mut display = ProgressDisplay::new(
        writer,
        OutputOptions::resolve("never", "terminal", true, None),
        work(),
    );
    assert_eq!(
        display.next_message(Duration::from_secs(65)),
        "[beacon] [==>         ] Authenticating checkpoint | 65s elapsed | 3.0 MiB inputs"
    );
    assert_eq!(
        display.next_message(Duration::from_secs(66)),
        "[beacon] [ ==>        ] Authenticating checkpoint | 66s elapsed | 3.0 MiB inputs"
    );
    let maximal = CheckpointWork {
        epoch: u64::MAX,
        state_bytes: usize::MAX,
        post_block_state_bytes: usize::MAX,
        block_bytes: usize::MAX,
    };
    assert_eq!(maximal.total_bytes(), usize::MAX);
    display.work = maximal;
    for _ in 0..40 {
        let message = display.next_message(Duration::from_secs(u64::MAX));
        assert!(message.len() < CLEAR_WIDTH);
        assert_eq!(message.matches("==>").count(), 1);
        assert_eq!(message.matches('%').count(), 0);
    }
}

#[test]
fn piped_and_structured_outputs_never_receive_live_cursor_writes() {
    for (format, terminal) in [("terminal", false), ("json", true), ("logfmt", true)] {
        let writer = Writer::default();
        let mut display = ProgressDisplay::new(
            writer.clone(),
            OutputOptions::resolve("always", format, terminal, None),
            work(),
        );
        display.render(Duration::from_secs(1));
        assert_eq!(writer.writes.load(Ordering::Relaxed), 0);
        assert_eq!(*writer.bytes.lock().unwrap(), Vec::<u8>::new());
        drop(display);
        assert_eq!(writer.writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn inherited_color_is_respected_and_drop_clears_only_the_active_display() {
    for (color, style, ansi) in [
        ("always", None, true),
        ("auto", None, true),
        ("never", None, false),
        ("always", Some("never"), false),
    ] {
        let writer = Writer::default();
        {
            let mut display = ProgressDisplay::new(
                writer.clone(),
                OutputOptions::resolve(color, "terminal", true, style),
                work(),
            );
            display.render(Duration::from_secs(65));
            let bytes = writer.bytes.lock().unwrap();
            assert_eq!(bytes.contains(&0x1b), ansi);
            assert!(bytes.len() < CLEAR_WIDTH);
        }
        let bytes = writer.bytes.lock().unwrap();
        if ansi {
            assert!(bytes.ends_with(b"\r\x1b[2K"));
        } else {
            let mut clear = [b' '; CLEAR_WIDTH + 2];
            clear[0] = b'\r';
            clear[CLEAR_WIDTH + 1] = b'\r';
            assert!(bytes.ends_with(&clear));
        }
        assert_eq!(writer.writes.load(Ordering::Relaxed), 2);
    }
}

#[test]
fn broken_terminal_disables_further_frames_and_cleanup_is_idempotent() {
    let writer = Writer { fail: true, ..Default::default() };
    let mut display = ProgressDisplay::new(
        writer.clone(),
        OutputOptions::resolve("always", "terminal", true, None),
        work(),
    );
    display.render(Duration::from_secs(1));
    assert!(!display.interactive());
    display.render(Duration::from_secs(2));
    assert_eq!(writer.writes.load(Ordering::Relaxed), 1);
    display.finish();
    assert_eq!(writer.writes.load(Ordering::Relaxed), 2);
    display.finish();
    drop(display);
    assert_eq!(writer.writes.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn verification_result_is_returned_unchanged_without_waiting_for_a_tick() {
    let outcome = observe_checkpoint_authentication(
        std::future::ready(Err::<(), _>("invalid checkpoint")),
        OutputOptions::resolve("never", "json", false, None),
        work(),
    )
    .await;
    assert_eq!(outcome, Err("invalid checkpoint"));
}

#[test]
fn quiet_never_writes_a_live_display_even_with_color_enabled() {
    let writer = Writer::default();
    let mut display = ProgressDisplay::new(
        writer.clone(),
        OutputOptions::resolve("always", "terminal", true, None).with_quiet(true),
        work(),
    );
    display.render(Duration::from_secs(1));
    drop(display);
    assert_eq!(writer.writes.load(Ordering::Relaxed), 0);
}
