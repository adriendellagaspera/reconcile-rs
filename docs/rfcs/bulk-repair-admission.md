# Bulk-repair admission

## Status

Accepted design contract. Runtime implementation is separate.

## Problem

A valid authenticated range mismatch can select dataset-scale work: enumerating differing ranges,
materializing `EntryUpdate` or `StateUpdate` messages, and sending them as a bulk dump.
Authentication and replay checks decide who may reach that path, but they do not make an
authenticated peer infallible. The existing active-dump slots bound concurrent snapshot work and
`bulk_send_rate` meters bytes inside a dump; neither limits how soon completed expensive work may
be selected again.

The admission boundary therefore applies only to newly selected bulk range enumeration. Small
refinement fingerprints, tombstone and convergence acknowledgements, and eager write broadcasts are
not bulk work.

## Decision

Bulk repair uses a pair of cooldown leases:

- one lease per source `IpAddr`, shared by the dated and value-only channels;
- one lease from a global pool whose capacity equals `max_concurrent_bulk_dumps`.

The source address is an operational key, not a cryptographic identity. A shared cluster key proves
membership, and any key holder can impersonate another holder. The per-address lease contains a
stable source; the global lease pool is the hard bound when authenticated sources churn or are
spoofed.

The first eligible bulk dump acquires both leases without delay. A lease is held for the whole bulk
task, including same-channel pending ranges that are already queued behind that active task, and
then remains unavailable for `bulk_dump_cooldown` after the task finishes. The construction-time
default is one second. The cooldown is deliberately independent of `repair_interval`: RTT/loss
tuning must not silently weaken an admission-security bound.

With cooldown `T` and global capacity `C`, one source can start at most one new bulk dump per
completed-dump-plus-`T` cycle. After the initial burst, the node can start at most `C` new dumps
per cooldown window when dumps themselves are short; long-running dumps are stricter because their
leases remain occupied while they send.

## Ordering

The receive path is ordered as follows:

1. authenticate, check wire version and replay state, then decode;
2. run range comparison/refinement;
3. if bulk differences are produced, claim the existing active dump slot;
4. acquire the per-address and global cooldown leases;
5. only then enumerate ranges and allocate bulk update messages.

A request that loses the existing active dump-slot race follows the pending-dump path. Pending-range
coalescing is a separate invariant: work discovered while an admitted same-channel dump is already
active belongs to that active transfer rather than consuming another temporal admission.

A request that obtains an active slot but is denied by temporal admission must release that active
slot without enumerating ranges and without adding new pending bulk work. It receives no synthetic
convergence acknowledgement. The peer's ordinary repair retries or background reconciliation may
try again after eligibility returns; those retries do not bypass admission.

If range enumeration yields no update messages, no expensive transfer occurred and the admission
lease need not enter cooldown.

## Interaction with existing bounds

`max_concurrent_bulk_dumps` continues to bound active snapshot tasks. `bulk_send_rate` continues
to meter bytes inside an admitted task. Temporal admission limits the frequency with which completed
dataset-scale work can be selected again.

These controls are intentionally independent. Disabling pacing removes the byte-rate bound but does
not remove temporal admission. Lowering or raising the RTT-scale repair timer changes retry timing
but not cooldown eligibility.

The per-source key is `IpAddr`, not `SocketAddr`, so changing UDP source ports cannot multiply the
per-source budget. Dated and value-only requests share the same lease state so alternating channels
cannot double it.

## Failure and retry semantics

A failed or aborted bulk task still consumed expensive enumeration and therefore enters cooldown.
Cooling state is monotonic-time runtime state only; it is not persisted and has no wire
representation.

The first eligible cold-catch-up dump is unchanged. A later repair may be delayed by at most the
remaining admission window before another expensive dump is eligible. Normal loss-free cold start
therefore pays no additional round.

## Observability

Temporal admission denial is observable separately for per-source and global exhaustion. Active
dump gauges retain their existing meaning and do not count cooling leases as active dumps.

## Adversarial benchmark contract

The security-amplification benchmark keeps a requester stale while emitting fresh authenticated
mismatch datagrams:

1. the first eligible request may trigger one full bulk dump;
2. requests while the lease is active or cooling trigger no additional full dump;
3. a request after eligibility returns can make progress again;
4. a multi-peer burst cannot exceed the global active-or-cooling lease capacity.

The same probe covers authoritative and value-only requesters. Bad-MAC, authenticated-malformed and
replay controls remain zero-response cases.
