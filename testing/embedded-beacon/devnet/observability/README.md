# Embedded beacon monitoring

This adds Prometheus and Grafana for a running Reth node, using the same exporter
and provisioning conventions as `etc/`. The dashboard has 143 panels in nine
expandable groups. Its overview shows scrape health, the authenticated consensus
head, execution optimism, Engine acknowledgement, peers and sync distance.

Run on the same Linux host as the node with Docker Compose, Bash, jq, curl,
OpenSSL, flock and ss (iproute2). No Python runtime is needed to launch monitoring.
A local Docker daemon is required so host networking can reach the node's
loopback metrics endpoint. The helper uses the caller's Docker context or
`DOCKER_HOST`; it never chooses a socket or starts or restarts the node.

Start the node with `--metrics 127.0.0.1:9001`, then from the repository root:

```bash
bash testing/embedded-beacon/devnet/observability/run.sh up
```

Open [Grafana](http://127.0.0.1:3000/d/embedded-beacon/embedded-beacon) or
[Prometheus targets](http://127.0.0.1:9090/targets). Both servers listen on
127.0.0.1. Grafana permits anonymous viewing; editing, sign-up, basic
authentication and the login form are disabled. A random administrator credential
is generated once in ignored `work/<project>/admin.env` and never printed.
The datasource and dashboard are read-only. Expand a group to see its charts.
Provisioned dashboard changes are polled every 30 seconds.

## Dashboard groups

- **Consensus progress and availability:** authenticated slots and native
  checkpoints, advertised retrieval hints, driver state, gossip outcomes,
  retained ancestry, graph state and schema estimates.
- **Consensus networking and data availability:** discovery and peer failures,
  block/envelope/sidecar request outcomes, original SSZ throughput, gossip
  reservations, availability job leases and custody/deferred-data flags.
- **Native imports, bounded queues and restart replay:** actual block/envelope
  attempts, checked outcomes, execution deferrals, processing time, reserved
  jobs and original bytes, high-water marks, backpressure and ordered replay.
- **Execution pipeline and canonical blocks:** committed stage checkpoints,
  entity progress, execution gas throughput, canonical notifications, reorgs,
  Engine method timings and the embedded typed proxy's transport statistics.
- **Execution networking, peers and sessions:** connections and discovery,
  directional disconnect reasons, request serving, invalid messages, backoff,
  outgoing queues and session pressure.
- **Headers and bodies downloader:** downloaded/flushed items, in-flight
  requests, buffers, contiguous queues, errors and individual response sizes.
- **MDBX, static files and pruning:** aggregate table pages, the largest known
  schema tables, entry counts, free pages, static segments, operations,
  transaction/commit timing and actual pruning progress.
- **Shared data ownership and immutable cache:** tracked objects, independent
  owner pins, logical reuse, cache occupancy, hit/miss rates, verified object
  reads, journal commits and garbage collection.
- **Process resources and filesystem:** resident/virtual/swap memory, CPU,
  threads/FDs, storage and logical IO, page faults, filesystem capacity,
  complete owned-area storage totals and explicitly partial scan observations.

## Reading the measurements

Execution checkpoints show committed progress; they can remain flat while a
batch is processed. The consensus head is authenticated by native consensus
checks, but an optimistic head still awaits execution verification. Optimism is
separate from acknowledgement of the current Engine fork-choice update. Neither
is inferred from peer counts. Checked import acceptance also does not imply an
Engine `VALID` response. Head-change counts include the initial observation;
import attempts include retries and checked duplicates.

Current gauges and cards also require a successful node scrape. When the target
goes down, they become No data at the next scrape/dashboard refresh; the scrape
health card remains unavailable in red. Historical event rates remain queryable.
Filtering happens before aggregation, preserving table/segment/area breakdowns.
Owner gauges are gated by the current owner-observation availability flag.
Shared-store gauges are gated by successful nonblocking observation. Optional
resource readings use availability flags, with IO and fault rates additionally
gated by their own complete-reading flags. Missing measurements stay **No data**;
queries do not add fallback zeros. Counters describe measured events; a zero
event rate alone is not an execution-validation or data-availability proof.

Storage panels distinguish these quantities:

- MDBX table bytes describe allocated table pages; they exclude the freelist
  and file preallocation. Free pages are reusable pages, not reclaimable bytes.
- Complete owned-area scans measure logical file lengths and allocated blocks
  separately. A bounded incomplete traversal publishes only a lower bound.
  Scan age/completeness remain visible.
- Shared tracked bytes describe metadata-known payloads and exclude object
  headers and orphans. Unique pinned bytes and logical owner-pinned bytes show
  repeated ownership. Their difference is logical reuse, not measured physical
  disk savings.
- Consensus schema estimates describe retained structures, not process RSS.
  Shared allocations may overlap between estimates and must not be summed.

Original consensus SSZ throughput excludes framing/compression and counts inputs
before mandatory validation. It is not wire bandwidth or accepted-block
throughput. Process storage IO is distinct from logical `rchar/wchar` IO,
which can include cache hits and non-storage streams. CPU seconds per second
measure occupied logical cores; the dashboard does not assume a core count.

## Metric names and query semantics

Queries use the normal `reth_` prefix and replace scope dots with underscores.
The scopes are `embedded_beacon.consensus`, `embedded_beacon.network`,
`embedded_beacon.import`, `embedded_beacon.execution`,
`embedded_beacon.execution_proxy`, `embedded_beacon.shared_data` and
`embedded_beacon.resources`.

This exporter preserves counter field names: `canonical_blocks` exports as
`reth_embedded_beacon_execution_canonical_blocks`, while a field explicitly
named `attempts_total` keeps that suffix. Some gauge names end in `_total`
for historical reasons; discovery session gauges use their values directly,
never `rate()`.

Timing distributions are exported as summaries with `_sum`, `_count` and
quantile series. Mean timings divide rates of actual sums by nonzero counts.
The MDBX p95 panel uses the actual summary quantile; it does not invent bucket
series or sum quantiles across instances. Import/shared cumulative durations are
microseconds and proxy durations are nanoseconds, converted to seconds in
queries.

All queries select the `embedded-beacon` scrape job. Storage/network breakdowns
use bounded schema-table, segment, operation, reason, direction or area labels.
There are no block-root, transaction-hash or individual-peer labels. The 143-panel
dashboard was checked against retained metric exposition and source annotations;
all 278 unique PromQL expressions passed evaluation on Prometheus. Retained
up=1 samples also verified unchanged table labels and aggregate values, while
the live down target verified that current table readings become unavailable. This proves
query validity, not runtime availability of each feature-dependent series.

## Configuration and lifecycle

The helper checks container names, Compose project ownership, volume ownership
and listener ports before creating anything. It refuses unrelated collisions.
Re-running `up` reuses this stack's containers and named volumes. Prometheus
retains two days of samples with a 512 MB retention threshold; the write-ahead log
and current head can use additional disk. Both services have a 512 MiB memory
limit and bounded Docker logs.

```bash
# Use the same target passed to the node's --metrics flag.
BEACON_METRICS_ADDR=127.0.0.1:9101 \
BEACON_GRAFANA_PORT=3001 \
BEACON_PROMETHEUS_PORT=9091 \
bash testing/embedded-beacon/devnet/observability/run.sh up

bash testing/embedded-beacon/devnet/observability/run.sh status
bash testing/embedded-beacon/devnet/observability/run.sh stop
bash testing/embedded-beacon/devnet/observability/run.sh config
```

Pass the same overrides to subsequent commands. `stop` preserves containers,
named volumes and node data. `config` validates Compose without starting
containers or pulling images. A different `BEACON_OBSERVABILITY_PROJECT`
creates an independent stack and requires different listener ports.

Official published images are pinned to `prom/prometheus:v3.13.4` and
`grafana/grafana:13.2.3`. See the
[Prometheus releases](https://prometheus.io/download/),
[Prometheus Docker installation](https://prometheus.io/docs/prometheus/latest/installation/)
and [Grafana image release](https://grafana.com/grafana/download/13.2.3?edition=oss&platform=docker).
Alternative explicit pins can be supplied through `BEACON_PROMETHEUS_IMAGE`
and `BEACON_GRAFANA_IMAGE`. There is no moving `latest` tag.
