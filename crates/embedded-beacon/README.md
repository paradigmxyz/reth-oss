# Embedded beacon

An optional Ethereum consensus follower for Reth, targeting Gloas and earlier forks.
The execution node remains usable independently; embedded consensus requires the
explicit Cargo feature and runtime flag.

[Build, configure and test the embedded beacon](../../docs/vocs/docs/pages/run/embedded-beacon.mdx).

The implementation is split into consensus kernels (`state-transition`,
`fork-choice`), verification (`crypto`, `data-availability`), networking and sync,
shared persistence and node coordination. Each library keeps its production
modules and regression tests together. The optional launcher lives in
[bin/reth-embedded-beacon](../../bin/reth-embedded-beacon/); the Rust fixture runner
and its helpers live in [testing/embedded-beacon](../../testing/embedded-beacon/).

The complete authenticated 73,992-case consensus fixture run has passed.
Public-network synchronization remains gated pending independent runtime
acceptance; fixture and component tests do not establish Hoodi or mainnet sync.

The import owner separates canonical publication into preparation, Engine
acknowledgement and durable commit. Replay event contexts retain their checked
state lease until the event completes. Sidecar and envelope retrievals each own
one idle, waiting or requested state while availability and serving remain
concurrent with imports.
