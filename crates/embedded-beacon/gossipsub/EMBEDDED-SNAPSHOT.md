# Embedded gossip dependency snapshot

This is the exact registry package `libp2p-gossipsub` version `0.49.5`, copied for
the optional embedded beacon transport. It is not a workspace-wide package override.
Normal execution-node targets must keep their existing dependency graph.

`SOURCE-PROVENANCE.json` records the published archive checksum, source revision,
every original package file checksum, and each locally changed or added source file.
The registry's local acquisition marker is excluded. Original source notices are
preserved; `LICENSE-MIT` reproduces the package's source license notice.

The empty-IP cleanup is exactly the two hunks in
`patches/0001-release-empty-peer-ip-sets.patch`:

- Explicit IP removal deletes a map key when its final peer leaves.
- Bulk peer cleanup deletes an IP map key when its final retained peer is removed.

Nonempty shared-IP sets, score values, negative-score retention, connection
addresses, authentication, wire bytes, identifiers and validation remain unchanged.
Four deterministic regression tests in `src/peer_score/tests.rs` cover shared IPs,
positive-peer cleanup, negative-score retention and expiration, and 4,096 distinct
IP removals.

`patches/0003-bounded-peer-score-admission.patch` adds the isolated
`src/peer_score/capacity.rs` helper and `Behaviour::can_admit_score_peer(&PeerId,
usize)`. It synchronously removes only truly expired disconnected score records
with the existing cleanup semantics; it never decays live scores, forgets
unexpired penalties, or changes thresholds. Existing identities may reconnect at
capacity. The query reserves nothing: the embedded wrapper must reserve pending
unique identities before handler delegation, subtract those reservations from
its 4,096-record limit, and release them on established or failed swarm outcomes.
Scoring must stay enabled in the embedded transport. Disabled scoring has no
score map and makes the query return true.

Four further regression controls cover hard admission, unexpired negative-score
retention, disconnected shared-IP cleanup while a live peer remains, unchanged
live penalties, and the pending-slot contract. `patches/0004-bound-priority-queue-and-report-overflow.patch` bounds the priority
channel to 4,096 control slots plus its existing configured publish capacity.
With the embedded queue setting of eight, this is 4,100 total priority entries,
four publish entries, and four non-priority entries per connected peer. Admitted
controls retain their original order and values. Saturation returns the original
RPC, records an explicit priority failure, applies the existing slow-peer score
penalty, and emits the existing `SlowPeer` event on the next heartbeat. The
embedded event owner must disconnect the slow peer through its existing rejection
path. Mandatory controls are never reported as successfully queued when rejected.

The patch also releases publish reservations when the new bounded channel rejects
an RPC and when a stale publication is dropped, using atomic publish admission.
Five actual stalled-consumer controls cover FIFO and overflow, explicit scoring
and failure events, publish rollback, stale publication release, and preservation
of the existing publication limit. These thirteen source controls have not been
executed as part of snapshot preparation.

Integration contract: point only the embedded network's optional transport
dependency directly at this path. Remove that package's facade `gossipsub` feature
and use direct `libp2p_gossipsub` imports. Do not use a root `[patch.crates-io]`;
package overrides cannot be scoped to an optional feature. The workspace owner
must explicitly exclude this snapshot from workspace membership, or approve its
membership, before running Cargo. No workspace configuration is changed by this
snapshot.

The priority cap bounds queue entries, not the entire transport byte footprint.
Cache copies, control promises, backoffs, and decoded frame allocations remain
separate. Those remain subject to the embedded transport's
separate admission, retention and actual cache-retirement proof.
`patches/0005-preflight-ingress-records.patch` adds optional `IngressLimits` and
`ConfigBuilder::ingress_limits`. Generic defaults remain unset; only the embedded
transport opts in. Before owned protobuf decoding, a borrowed fixed-depth scanner
counts publications, subscriptions, control actions, message IDs and PRUNE peers.
The embedded limits are respectively 16 per RPC, 256 per RPC, 16 per control
envelope, 1,024 IDs per envelope and 16 peers per PRUNE action. A repeated optional
control envelope replaces its predecessor in the original decoder: each envelope
is checked independently, including discarded ones, while preserving last-value
semantics. During assignment the original decoder can briefly hold the preceding
and replacement envelopes, so retained control allocations must budget both.

The scanner creates no decoded records, strings or ID vectors. It leaves admitted
wire bytes untouched, then delegates the original decoder, message verification
and application consensus validation. Opaque publication data is not inspected.
On a complete structurally valid frame, exhaustion returns local `WouldBlock`
before original decoder consumption; it is not an invalid consensus verdict.
A capacity hit does not stop scanning: later malformed fields retain `InvalidData`.
Incomplete frames and the original frame-size error retain their original behavior.
Each nested record is a bounded slice. Opt-in mode also rejects malformed nested
lengths that the pinned reader can sometimes cross into sibling bytes; the generic
unset path retains the original decoder behavior for those malformed inputs.

Sixteen actual codec controls cover exact limits, 100,000 repeated empty records,
all action and ID types, PRUNE peers, repeated optional envelopes, opaque payloads,
unknown fields and accepted nonminimal protobuf varints, invalid fields after
capacity, malformed nested boundaries, incomplete and oversized frames, and
invalid stream prefixes and pointer-width final-byte/overflow/remainder parity.
Length-prefix parsing is local and dependency-free; the native codec retains its
existing declared dependency, so no production manifest edge is added. The 29 added source controls have not been executed as
part of snapshot preparation; execution evidence belongs to the build owner.

The published manifest drops an original workspace-only quickcheck test edge.
Independent verification may add registry quickcheck 1.0.3 only to its ignored
test manifest; this snapshot's original manifests and lock remain byte-preserved.
