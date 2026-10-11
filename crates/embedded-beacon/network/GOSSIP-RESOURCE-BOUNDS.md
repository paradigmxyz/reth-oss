# Embedded gossip resource bounds

These are source-enforced limits for the optional embedded networking feature. They are not a measurement of process RSS or a claim that gossip uses 32 MiB in total. Ordinary execution-node networking does not activate this dependency edge.

| Resource | Admission bound | Retirement / failure rule |
| --- | --- | --- |
| Established consensus connections | 16 total, 2 per peer | Actual connection close releases the connection. |
| Pending score admission | 24 connection IDs | Exact established, dial-failure or listen-failure event settles each reservation after the inner behaviour processes it. |
| Retained scored peer identities | 4096 including pending unique identities | Only strictly expired disconnected score records are purged. Existing peers reconnect without deleting negative score history. |
| Scored topic identities | 512 over this behaviour lifetime | New topics past the bound fail local subscription; canonical topic parsing prevents alias labels. |
| Original cached payload bytes | 32 MiB | Awaiting-verdict bytes require 89 completed cache shifts. Accept reschedules release after seven fresh completed shifts. Successful Ignore/Reject releases raw bytes after synchronous cache removal; duplicate IDs retain their full TTL. |
| Cached message IDs | 131072, exactly 20 bytes each | Capacity is reserved before the inner handler can insert an ID, including zero-length payloads. |
| Admitted control material | 2 MiB global, 256 KiB per peer, 4096 entries | ID leases require expiry plus a later completed heartbeat. Backoff metadata remains charged through advertised backoff and actual ring cleanup. |
| Publications decoded from one RPC frame | 16 | Borrowed, allocation-free frame preflight runs before quick-protobuf object allocation. |
| Subscriptions decoded from one frame | 256 | The same preflight applies before allocation. |
| One decoded control envelope | 16 actions, 1024 IDs, 16 peer records per PRUNE | Control preflight and original-control admission both apply. |
| Priority outbound channel per peer | 4100 entries | Mandatory control overflow enters the existing slow-peer score/event path and the embedded transport disconnects that exact peer. |
| Forward payload channel per peer | 4 entries | Payload enqueue remains bounded; actual dequeue settles the original queue counter. |
| Production local Publish route | Absent | The embedded wrapper exposes publish only to its tests. |
| Private metric labels | Fixed non-topic families | Both topic registration limits are zero; no topic-registration escape is exposed. |

The negotiated frame cap is `MAX_GOSSIP_WIRE_BYTES + 1024`. `MAX_GOSSIP_WIRE_BYTES` is the bounded raw Snappy size for a 10 MiB decoded consensus message. The preflight borrows the complete frame and uses a fixed schema depth. It continues structural validation after crossing a capacity bound so malformed input does not become a capacity classification.

At most two decoded control envelopes can coexist while an optional protobuf field is replaced. A conservative temporary bound is therefore 32 control actions, 2048 ID vectors and 512 PRUNE peer records. Owned byte and string fields still fit the negotiated frame budget; protocol/frame, conversion and queued copies are additional allocations.

Payload forwarding alone has a coarse byte bound of `16 * 4 * MAX_GOSSIP_WIRE_BYTES`, approximately 747 MiB. This intentionally pessimistic bound assumes every peer queue contains maximum-size objects. Each connection also has at most two peeked queue objects and an in-flight protobuf/frame object; the actual handler and encoder buffers must be included separately. Priority entries include bounded control/subscription material, not only an entry-count allocation.

An admitted unique ID can have one raw cache copy and one queued application event copy. Thus the 32 MiB raw cache allowance does not cover the application's queued event clone, active frame, decoded Snappy scratch, forwarding clones, ID maps, score tables, or connection buffers. Allocator overhead and resident high-water behaviour require measurement in the sync test.

Local capacity misses are filtered before inner message admission and do not become consensus invalidity or P4 penalties. Definitive native Reject retains the configured protocol scoring behaviour. A proof-input archive write receives only private native validation seals; mathematical candidate verification stages bounded bytes until strict block/envelope admission succeeds. Rejected candidates cannot consume durable archive owners.

The scoped dependency repairs retain upstream wire messages, real endpoints, scoring and generic default behaviour. Optional protobuf limits are enabled only by this embedded transport. Actual native controls and transport tests should accompany any change to these constants or release conditions.

Native validation has a 60-second wire-token deadline. Only messages awaiting a verdict use the separate 88-window history at a 700 ms heartbeat, giving at least 60.9 seconds before eviction. Accept transfers a message into a fresh six-window history for forwarding and gossip; it does not retain validated columns for another minute. Raw-byte release follows actual completed cache shifts, or a successful synchronous Ignore/Reject removal. Missing shift evidence cannot shorten an accepted-message byte lease. The 32 MiB cache budget and 16 pending callbacks remain fixed when validation is slow; saturation causes local Ignore rather than consensus rejection. A callback after retirement is a debug diagnostic with its exact Accept, Ignore or Reject verdict. Wire-token expiry only prevents propagation: it cannot revoke an independently committed consensus graph receipt, and a cached definitive Reject still applies the existing validation penalty. The longer pending history is explicitly enabled by the embedded transport; generic dependency defaults retain their original cache policy.
