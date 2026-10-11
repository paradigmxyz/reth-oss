//! Honest checkpoint authentication activity, isolated from consensus results and authority.

use super::OutputOptions;
use std::{
    future::Future,
    io::{self, Write},
    time::{Duration, Instant},
};

const BAR_WIDTH: usize = 12;
const MARKER: &[u8; 3] = b"==>";
const CLEAR_WIDTH: usize = 160;
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Downloaded input sizes are observations; they do not estimate verification completion.
#[derive(Clone, Copy, Debug)]
pub(in crate::reth) struct CheckpointWork {
    pub(in crate::reth) epoch: u64,
    pub(in crate::reth) state_bytes: usize,
    pub(in crate::reth) post_block_state_bytes: usize,
    pub(in crate::reth) block_bytes: usize,
}

impl CheckpointWork {
    fn total_bytes(self) -> usize {
        self.state_bytes
            .saturating_add(self.post_block_state_bytes)
            .saturating_add(self.block_bytes)
    }
}

/// Observe the real verification future; output errors never change its result or cancellation.
pub(in crate::reth) async fn observe_checkpoint_authentication<F>(
    future: F,
    output: OutputOptions,
    work: CheckpointWork,
) -> F::Output
where
    F: Future,
{
    if output.quiet {
        return future.await;
    }
    let started = Instant::now();
    let mut display = ProgressDisplay::new(io::stdout(), output, work);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_report = REPORT_INTERVAL;
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = interval.tick() => {
                let elapsed = started.elapsed();
                if tracing::enabled!(target: "beacon::bootstrap", tracing::Level::INFO) {
                    display.render(elapsed);
                    if !display.interactive() && elapsed >= next_report {
                        if output.art {
                            let message = display.next_message(elapsed);
                            tracing::info!(target: "beacon::bootstrap", epoch = work.epoch,
                                elapsed_seconds = elapsed.as_secs(), input_bytes = work.total_bytes(),
                                phase = "checkpoint_authentication", "{message}");
                        } else {
                            tracing::info!(target: "beacon::bootstrap", epoch = work.epoch,
                                elapsed_seconds = elapsed.as_secs(), input_bytes = work.total_bytes(),
                                phase = "checkpoint_authentication",
                                "[beacon] Checkpoint authentication is running");
                        }
                        next_report = elapsed.saturating_add(REPORT_INTERVAL);
                    }
                }
            }
        }
    }
}

/// A bounded writer guard clears its live line when verification ends or startup is cancelled.
struct ProgressDisplay<W: Write> {
    writer: W,
    output: OutputOptions,
    work: CheckpointWork,
    frame: usize,
    active: bool,
    write_failed: bool,
}

impl<W: Write> ProgressDisplay<W> {
    const fn new(writer: W, output: OutputOptions, work: CheckpointWork) -> Self {
        Self { writer, output, work, frame: 0, active: false, write_failed: false }
    }

    fn interactive(&self) -> bool {
        self.output.art && self.output.terminal && !self.output.quiet && !self.write_failed
    }

    fn next_message(&mut self, elapsed: Duration) -> String {
        let mut bar = [b' '; BAR_WIDTH];
        let travel = BAR_WIDTH - MARKER.len();
        let step = self.frame % (travel * 2);
        let position = if step <= travel { step } else { travel * 2 - step };
        bar[position..position + MARKER.len()].copy_from_slice(MARKER);
        self.frame = self.frame.wrapping_add(1);
        format!(
            "[beacon] [{}] Authenticating checkpoint | {}s elapsed | {:.1} MiB inputs",
            std::str::from_utf8(&bar).expect("static ASCII activity marker"),
            elapsed.as_secs(),
            self.work.total_bytes() as f64 / (1024.0 * 1024.0),
        )
    }

    fn render(&mut self, elapsed: Duration) {
        if !self.interactive() {
            return;
        }
        let message = self.next_message(elapsed);
        let ansi_start = if self.output.ansi { "\x1b[96m" } else { "" };
        let ansi_end = if self.output.ansi { "\x1b[0m" } else { "" };
        let frame = format!("\r{ansi_start}{message}{ansi_end}");
        self.active = true;
        if let Err(error) =
            self.writer.write_all(frame.as_bytes()).and_then(|_| self.writer.flush())
        {
            self.write_failed = true;
            tracing::debug!(target: "beacon::bootstrap", kind = ?error.kind(),
                "[beacon] Checkpoint activity display unavailable");
        }
    }

    fn finish(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if self.output.ansi {
            let _ = self.writer.write_all(b"\r\x1b[2K");
        } else {
            let mut clear = [b' '; CLEAR_WIDTH + 2];
            clear[0] = b'\r';
            clear[CLEAR_WIDTH + 1] = b'\r';
            let _ = self.writer.write_all(&clear);
        }
        let _ = self.writer.flush();
    }
}

impl<W: Write> Drop for ProgressDisplay<W> {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests;
