//! Embedded terminal art observes already authenticated heads and never changes node authority.

use reth_embedded_beacon_node::import_coordinator::ObjectObservation;
use reth_embedded_beacon_primitives::{Fork, Root};
use std::io::{self, Write};

const POLAR_BEAR: &str = include_str!("presentation/polar-bear.txt");

const TITLE: &str = "\n             GLOAS - POLAR BEAR\n\n";

/// Inherited console controls resolved before the CLI hands ownership to node startup.
#[derive(Clone, Copy, Debug)]
pub(super) struct OutputOptions {
    art: bool,
    ansi: bool,
    terminal: bool,
    quiet: bool,
}

impl OutputOptions {
    pub(super) fn resolve(color: &str, format: &str, terminal: bool, style: Option<&str>) -> Self {
        Self {
            art: format == "terminal",
            terminal,
            quiet: false,
            ansi: color != "never" &&
                style != Some("never") &&
                (color == "always" || (color == "auto" && terminal)),
        }
    }

    /// Direct terminal writes must stay quiet even if a file tracing layer enables INFO.
    pub(super) const fn with_quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }
}

/// Minimal copy of the exact owner status used to select one native retained header.
#[derive(Clone, Copy, Debug)]
pub(super) struct HeadObservation {
    pub(super) root: Root,
    pub(super) slot: u64,
    pub(super) execution_acknowledged: bool,
    pub(super) engine_closed: bool,
    pub(super) optimistic: bool,
}

/// One process-local announcement; initial Gloas and later reorgs never fabricate a transition.
#[derive(Debug)]
pub(super) struct ForkBanner {
    output: OutputOptions,
    previous_fork: Option<Fork>,
    last_root: Option<Root>,
    seen_gloas: bool,
}

impl ForkBanner {
    pub(super) const fn new(output: OutputOptions) -> Self {
        Self { output, previous_fork: None, last_root: None, seen_gloas: false }
    }

    /// Avoid owner header queries while optimistic, closed, repeated or already at Gloas.
    pub(super) fn wants_header(&self, head: HeadObservation) -> bool {
        !self.seen_gloas &&
            self.last_root != Some(head.root) &&
            head.execution_acknowledged &&
            !head.engine_closed &&
            !head.optimistic
    }

    /// Match the exact status root and slot to the native canonical retained block schema.
    pub(super) fn observe(&mut self, head: HeadObservation, header: &ObjectObservation) -> bool {
        if !self.wants_header(head) ||
            header.block_root != head.root ||
            header.header.slot != head.slot ||
            !header.canonical ||
            header.execution_optimistic
        {
            return false;
        }
        let announce = self.previous_fork == Some(Fork::Fulu) && header.fork == Fork::Gloas;
        self.previous_fork = Some(header.fork);
        self.last_root = Some(head.root);
        self.seen_gloas = header.fork == Fork::Gloas;
        announce
    }

    /// One bounded write of the converted mascot; writer failures cannot affect runtime admission.
    pub(super) fn write(&self, writer: &mut impl Write) -> io::Result<()> {
        if !self.output.art || self.output.quiet {
            return Ok(());
        }
        let mut output = String::with_capacity(POLAR_BEAR.len() + TITLE.len() + 24);
        if self.output.ansi {
            output.push_str("\x1b[1;97m");
        }
        output.push_str(POLAR_BEAR);
        if self.output.ansi {
            output.push_str("\x1b[1;96m");
        }
        output.push_str(TITLE);
        if self.output.ansi {
            output.push_str("\x1b[0m");
        }
        writer.write_all(output.as_bytes())
    }
}

mod progress;

pub(super) use progress::{observe_checkpoint_authentication, CheckpointWork};

#[cfg(test)]
mod tests;
