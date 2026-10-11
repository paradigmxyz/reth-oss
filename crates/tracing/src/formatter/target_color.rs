//! Optional stdout target styling delegated to the native terminal formatter.
//! The layer never mutates events or introduces escape codes into structured/file output.

use std::fmt::{self, Write as _};
use tracing::{
    field::{Field, Visit},
    Event, Level, Subscriber,
};
use tracing_subscriber::{
    fmt::{
        format::{Format, Writer},
        FmtContext, FormatEvent, FormatFields,
    },
    registry::LookupSpan,
};

pub(super) struct BrightCyanTargetFormat {
    inner: Format,
    target: &'static str,
}

impl BrightCyanTargetFormat {
    pub(super) fn new(target: &'static str, show_target: bool) -> Self {
        Self { inner: Format::default().with_target(show_target), target }
    }

    fn matches(&self, target: &str) -> bool {
        target == self.target ||
            target.strip_prefix(self.target).is_some_and(|suffix| suffix.starts_with("::"))
    }

    fn is_beacon_reset_token_warning(&self, event: &Event<'_>) -> bool {
        let metadata = event.metadata();
        if self.target != "beacon" ||
            metadata.target() != "quinn_proto::endpoint" ||
            *metadata.level() != Level::WARN
        {
            return false
        }
        let mut fields = ResetTokenWarningFields::default();
        event.record(&mut fields);
        fields.exact_message
    }
}

impl<S, N> FormatEvent<S, N> for BrightCyanTargetFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let reset_token_warning = self.is_beacon_reset_token_warning(event);
        let cyan = writer.has_ansi_escapes() &&
            (self.matches(event.metadata().target()) || reset_token_warning);
        if !cyan && !reset_token_warning {
            return self.inner.format_event(context, writer, event)
        }
        // Preserve the native level and metadata. A reset-token collision is a real QUIC
        // routing warning, even though it does not itself reject consensus or close a peer.
        // Only matching console events allocate this small formatted line buffer.
        let mut line = String::new();
        self.inner.clone().with_ansi(false).format_event(context, Writer::new(&mut line), event)?;
        if reset_token_warning && let Some(prefix) = line.strip_suffix("duplicate reset token\n") {
            let prefix_len = prefix.len();
            line.truncate(prefix_len);
            line.push_str(
                "[beacon] QUIC peer reused a reset token; reset routing entry replaced\n",
            );
        }
        if !cyan {
            return writer.write_str(&line)
        }
        // Use one component color for the timestamp, level, message and fields.
        writer.write_str("\x1b[96m")?;
        let body = line.strip_suffix('\n').unwrap_or(&line);
        write_without_sgr(&mut writer, body)?;
        writer.write_str("\x1b[0m")?;
        if line.ends_with('\n') {
            writer.write_str("\n")?;
        }
        Ok(())
    }
}

// Span fields may already contain styling cached by the subscriber.
fn write_without_sgr(writer: &mut Writer<'_>, mut text: &str) -> fmt::Result {
    while let Some(start) = text.find("\x1b[") {
        writer.write_str(&text[..start])?;
        text = &text[start + 2..];
        if let Some(end) = text.find(|c: char| !c.is_ascii_digit() && c != ';' && c != ':') &&
            text.as_bytes()[end] == b'm'
        {
            text = &text[end + 1..];
        } else {
            writer.write_str("\x1b[")?;
        }
    }
    writer.write_str(text)
}

#[derive(Default)]
struct ResetTokenWarningFields {
    exact_message: bool,
}

impl Visit for ResetTokenWarningFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.exact_message = value == "duplicate reset token";
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            // Compare the complete fixed message without allocating arbitrary dependency text.
            let mut expected = ExactMessageWriter("duplicate reset token");
            self.exact_message =
                write!(&mut expected, "{value:?}").is_ok() && expected.0.is_empty();
        }
    }
}

struct ExactMessageWriter(&'static str);

impl fmt::Write for ExactMessageWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.strip_prefix(text).ok_or(fmt::Error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
