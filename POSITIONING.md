# Positioning — where `reconcile-rs` sits

> **Reference document.** The landscape `reconcile-rs` is judged against: set reconciliation,
> diffable data structures, replica consistency. **Durable background** — field positioning and
> design taxonomy move slowly, unlike the code — and deliberately **status-free**.
>
> | Not here | There |
> |---|---|
> | Live correctness/security/maturity status | the `v1.0.0` milestone, [#206](https://github.com/Akvize/reconcile-rs/issues/206) |
> | Resolved-audit record (`Fxx` findings) | [`ARCHITECTURE.md`](./ARCHITECTURE.md) §8 |
> | Target design, deferred decisions | [`ARCHITECTURE.md`](./ARCHITECTURE.md) §5–§7 |
> | Fit-for-purpose guidance | [`README.md`](./README.md) "When to use this" |
> | Measured figures — bytes, message counts, timings | [`benches/README.md`](./benches/README.md) ([#346](https://github.com/Akvize/reconcile-rs/issues/346)) |
>
> **Scope:** the `FingerprintTreeMap` as a *data structure* and RBSR as an *algorithm*, against the
> published state of the art — not an audit of any particular commit. Literature reviewed through
> 2026-08-17. §1.3/§2.2 state the claim and verdict a benchmark run supports; a policy or benchmark
> change must never require editing this file.

---

## 1. Objective and relevance vs the SOTA

### 1.1 The stated objective

Per the README: *"a scalable Web service with a non-persistent and eventually consistent key-value
store [...] avoiding any latency related to using an external store such as Redis. All the data is
available locally on all instances"*. In other words: **each web-service replica embeds the full
dataset in memory**, replicas reconcile peer-to-peer, and the user is notified of changes via an
insertion hook.

### 1.2 Relevance and real niche

The niche is **real but narrow**: there is no mature equivalent in the Rust/Tokio ecosystem of
Hazelcast's *Replicated Map* or Akka/Pekko's *Distributed Data* (all JVM). For a **read-heavy** Rust
web service with a moderate working set and rare/benign conflicts (feature flags, routing tables,
presence, configuration), an in-memory replicated cache with local O(log n) reads and no Redis
dependency is legitimately attractive.

**But the "scalable / avoid Redis" positioning inverts the real trade-offs:**

- The latency argument only holds for **reads**. Writes are only *eventually* visible on peers;
  "avoiding Redis latency" actually amounts to **trading a synchronous consistent store for an
  asynchronous inconsistent one** — a consistency-model change dressed up as a latency optimization.
- The topology **does not scale by construction**: full dataset on every replica → memory bounded by
  the smallest node, and **every write is amplified to all nodes** → write throughput *decreases* as
  replicas are added. This is the documented failure mode of replicated caches (Oracle Coherence,
  Apache Ignite). Pekko Distributed Data explicitly recommends **not exceeding ~100,000 entries** in
  full replication — to be compared with the README's "millions of elements" promise.

### 1.3 The SOTA of set reconciliation (sourced)

| Family | Comm. | Compute | RTT | Knows *d*? | Adversarial robustness | Maturity |
|---|---|---|---|---|---|---|
| Naive XOR RBSR | O(d log n) | O(d log n) | **O(log n)** | No (self-adapting) | **Weak** (forgeable XOR) | Earthstar, Willow |
| **Secure-fingerprint RBSR (≥256-bit), fixed fan-out *b*** | O(d log n) | O(d log n) | O(log n) | No | Good | Negentropy (prod, *b*=16) |
| **↳ as instantiated in reconcile-rs** (`b`=16, swappable policy) | O(d log n) | O(d log n) | O(log_16 n) sequential | No | Good | reconcile-rs |
| IBLT / Difference Digest | O(d·(b+log U)) | **O(d)** | 1 (+estim.) | **Yes** | Weak | blockchains |
| **Rateless IBLT (SIGCOMM 2024)** | **≈ d** (3-4× < non-rateless) | **linear** (2-2000× < minisketch) | **1 streaming** | **No** | **Designed for adversarial** | Ethereum state-sync |
| minisketch / PinSketch (CPI) | **optimal ≈ b·d** | O(d²) | 1 (+ext.) | **Yes (capacity)** | deterministic if capacity OK | Bitcoin Erlay (BIP 330) |
| Merkle-tree diffing | O(d log n) | O(d log n) | O(log n) | No | hash-dependent | Dynamo, Cassandra, Riak |

`⌈log₁₆ n⌉` is refinement-tree *depth*, the quantity the RTT column's complexity bound is stated in;
`benches/protocol.rs` instead counts one-way protocol messages, a related but different number —
quote the one you mean, and see `benches/README.md`'s "Results: what RTT ≈ 0 was hiding" table for
both plus the wall-clock conversion. Sources: Meyer arXiv:2212.13567 & logperiodic.com/rbsr.html;
*Practical Rateless Set Reconciliation*, SIGCOMM 2024, arXiv:2402.02668; minisketch (bitcoin-core) &
BIP 330; Erlay (CCS 2019); arXiv:2603.19820 (RSOS, 2026).

**Only the `reconcile-rs` row is measured** (`benches/protocol.rs`); the rest are quoted from their
own papers, on different hardware/workloads/cost models, so treat cross-row comparison as
orientation, not a result — [#174](https://github.com/Akvize/reconcile-rs/issues/174) records why an
external comparison harness isn't run here, and
[#362](https://github.com/Akvize/reconcile-rs/issues/362) tracks reopening it narrowly against
Negentropy.

**The published O(d log n) / O(log n) figures assume a constant branching factor**, which is
`rbsr`'s default `RefinementPolicy = FixedFanOut(16)`. The fan-out is a *local, swappable* choice, not
a wire contract; `rbsr` also ships `SqrtFanOut` (cuts at `step = ⌊√m⌋`, trading Θ(√n) communication
for Θ(log log n) depth) — measured against the default in `benches/protocol.rs`, numbers in
[§2.2](#22-competitors-at-the-reconciliation-algorithm-level).

**Key takeaway:** for the **large-n / small-d / latency-sensitive** profile, fixed-*b* RBSR is the
**worst family on latency** (O(log n) sequential RTTs, confirmed at **1.00 × RTT** per round by
`benches/README.md`'s `service_reconcile_rtt` lane,
[#461](https://github.com/Akvize/reconcile-rs/issues/461)) while **Rateless IBLT** resolves in a
single streaming exchange with no *d* estimation and adversarial robustness — the current SOTA choice
for this profile. reconcile-rs's alternative `√m` policy moves along the fixed-*b* curve without
escaping it (§2.2); escaping it is [#185](https://github.com/Akvize/reconcile-rs/issues/185)'s job.
That ranking holds at one network point only: under loss, `reconcile_interval` per dropped datagram
dominates before RTT does ([#336](https://github.com/Akvize/reconcile-rs/issues/336)), and no family
in the table addresses that term — which family wins is a property of the path as much as of the
algorithm.

### 1.4 The embedded in-memory data grid (IMDG) use case

Framed as a product rather than an algorithm, reconcile-rs is an **embedded in-memory data grid**:
the state lives in-process, next to the application, fully replicated across a fleet of equal nodes.
Its category is the **masterless / AP / gossip** corner of the IMDG space — adjacent to Hazelcast,
Apache Ignite, Oracle Coherence and Infinispan (all JVM, all a separate cluster to operate), but as
a single embeddable Rust library. The pitch is "replicated state without standing up Redis/etcd":

- **Reads are local** — an in-process lookup, no network hop or (de)serialization. This is the one
  place reconcile-rs is unambiguously faster than a networked store; it is a *read-latency* and
  *operational-simplicity* play, not a write-path or consistency improvement.
- **Redundancy, not sharding** — full replication means any surviving node holds the whole dataset,
  so the grid tolerates losing nodes; the flip side is §1.2's memory / write-amplification ceiling.
- **Partition tolerance with automatic convergence** — nodes keep serving while partitioned and
  re-converge by anti-entropy on heal, with no manual conflict resolution (LWW).

The roadmap from §1.2's "real but narrow" niche to a credible Rust IMDG is §2.4's axis list, each
axis citing the issue that carries its live status.

---

## 2. Competitor audit and differentiators

> Scoped to the **`FingerprintTreeMap` as a data structure** and its protocol, not the full system.
> Methodological anchor: the `FingerprintTreeMap` **is not a Merkle tree in the MST/prolly sense**. It
> is a *Range-Summarizable Order-Statistics Store* (RSOS) — a B-tree augmented, per node, with a
> **composable subtree summary** (a 256-bit additive fingerprint) **and an order statistic** (the
> subtree size) — the abstraction formalized in 2026 (arXiv:2603.19820) as the backend range-based
> reconciliation (RBSR, Meyer 2023) needs. Its peer group is therefore the other diffable structures
> (§2.1), and its algorithmic competitor the other set-reconciliation families (§2.2).

### 2.1 Competitors at the "diffable data structure" level

#### Merkle Search Tree (MST) — Auvolat & Taïani, SRDS 2019
A search B-tree where a key's **level** is derived from the **hash of the key** (leading zeros →
fanout) ⇒ two replicas with the same key set produce the **same tree and same root hash**,
regardless of insertion order (*history-independence*). Diff = root-hash comparison (O(1)) then
descent comparing **internal node hashes**.
- ✅ History-independent (necessary because it diffs *nodes*); compact page serialization/diff;
  mature, **fuzz-tested** Rust crate (`merkle-search-tree`, domodwyer); production **Bluesky/atproto**
  (one MST per repository).
- ❌ **"Leading-zeros" attack**: an attacker forges keys with very deep hashes to inflate height and
  unbalance the tree. ❌ Only probabilistic balancing; no native rank/select.
- **vs FingerprintTreeMap:** MST *pays* for history-independence; FingerprintTreeMap does not (value-based diff, §2.3) and
  **escapes the leading-zeros attack**. But MST gains structural sharing (versioning) that FingerprintTreeMap
  lacks.

#### Prolly trees (Noms, Dolt) — *probabilistic B-trees*
A **content-addressed** B-tree, boundaries fixed by a **rolling-hash chunker** (~4 KB).
History-independent, self-balancing, and crucially **structural sharing**: unchanged subtrees share
identical chunks across versions.
- ✅ SOTA of **diffable AND versioned** ordered stores: diff/merge touch only changed chunks (the
  foundation of Dolt, "the first version-controlled relational database"). Dolt hashes **keys only**
  → a value update does not move boundaries. Resists the leading-zeros attack.
- ❌ Heavy machinery (rolling hash, chunks, CAS); higher latency than an in-mem B-tree; designed for
  **persistence**. The classic rolling-hash chunker also pays **cascading rechunking**: one
  insertion can shift a chunk boundary, which shifts the next, up to O(N) restructured chunks
  worst case. Rawat et al. 2026 bound this to one chunk plus an O(H) anchor-path update per
  insertion (≤2H hashes, expected height still O(log n)) — narrows this ❌, does not remove it
  (still more machinery than an in-mem B-tree write), and has no bearing on FingerprintTreeMap's
  history-independence-free diff (§2.3 #1), which is a different axis.
- **vs FingerprintTreeMap:** prolly = SOTA if you want **versioning + persistence + branch/merge**. FingerprintTreeMap is
  simpler/faster in memory but offers **none** of those. Central trade-off "simplicity/speed vs
  versioning/durability".

#### Merkle radix / Sparse Merkle Tree / "Merklized KV" (Gustafson 2023)
Position by the key's **prefix bits** (trie); history-independent by construction; the basis of
Ethereum (Merkle-Patricia) and SMTs.
- ✅ Deterministic, prefix scans, compact inclusion proofs.
- ❌ Depth ∝ key length (not log n); fixed fanout; less suited to arbitrary range diffs. Relevant
  mostly for **cryptographic proofs**, not for the "large in-memory KV, small diffs" profile.

#### Fixed-depth Merkle tree (Dynamo / Cassandra / Riak)
- ✅ Proven at massive production scale (anti-entropy repair).
- ❌ **Over-streaming**: a leaf covers a *range* of partitions (Cassandra: depth 15 = 32K leaves) →
  a single differing row forces streaming the whole leaf (~30 partitions for 1 bad in 1M). ❌ Tree
  rebuild when token ranges move.
- **vs FingerprintTreeMap:** this is precisely the defect RBSR/FingerprintTreeMap fix (the recursion tightens onto the
  actually-differing elements). **Clear advantage to FingerprintTreeMap** on this axis.

#### RSOS / AELMDB (arXiv:2603.19820, 2026) — *the most direct competitor*
The paper formalizes "**B+-tree augmented with subtree counts + composable summaries**" as the RSOS
abstraction, proves RBSR's local-cost bounds on this backend, and ships **AELMDB**: a **persistent,
memory-mapped** LMDB extension, evaluated with Negentropy. Read in the source
(`github.com/amparore/aelmdb`), the fork touches only **branch pages** — a branch node becomes
`[child pgno | aggregates | separator key]` with `aggregates := [entries?][keys?][hashsum?]` — and
binds the summary width into the on-disk format tag. It is **not** content-addressed: LMDB is a
copy-on-write B+-tree addressed by page number.
- **vs FingerprintTreeMap:** **it is the same design**, and its combiner is literally ours —
  addition modulo 2²⁵⁶ over little-endian 64-bit limbs with carry (`mdb_hashsum_add`), the C mirror
  of `Fingerprint::combine`. Two deltas run the *other* way, in our favour, and were not visible
  from the abstract alone:
  - **AELMDB does not hash.** Def. 3.4's lift φ is realized by *extracting a fixed-size byte slice*
    at a configured offset from the key or the value (`MDB_AGG_HASHSUM` + `mdb_set_hash_offset`);
    the engine assumes the application already embedded a collision-resistant id. `rsos` owns that
    end instead — BLAKE3 over the canonical encoding of (key, value) — so it summarizes the
    **value**, not merely an identity the caller vouches for.
  - **The comparison map is exact here, probabilistic there.** Negentropy's `f_p` is
    `SHA-256(Σ ‖ varint(count))` truncated to **128 bits** (format per the Negentropy protocol-v1
    spec in the reference repo — the paper's §6.1 carries no varint); the paper states plainly that
    this makes it "probabilistically sound rather than information-theoretically exact" and leaves
    the end-to-end collision analysis out of scope. `rbsr` compares the aggregate itself (full
    256-bit fingerprint + count), i.e. `f_p = id`, so Prop. 4.1's sound-skip assumption reduces to
    the injectivity of Σ with no truncation term. The price is ~2.2× the bytes per advertised range,
    measured against Negentropy rather than derived
    ([#362](https://github.com/Akvize/reconcile-rs/issues/362)) — see §2.2.
    What the exact count buys, and what it does not: a range whose peers hold **different
    cardinalities** can never be SKIPped — probability 1, no assumption on the hash — so a dropped
    write or an unreplicated tombstone is structurally covered. A **same-key/different-value
    conflict is not**: both records share a key, so no rank split ever separates them and every
    range containing that key is count-balanced at every depth. Re-ordering the store does not
    rescue it, and *injectivity* is not the lever that would —
    [§2.4.1](#241-open-research-questions). The failure mode `f_p = id` covers
    outright is the rarer one; the one an LWW register produces continuously falls back on Σ's
    injectivity alone. Truncating a count-folding hash (Negentropy) trades the probability-1 half
    away entirely; comparing `(count, Σ mod 2^τ)` would keep it for the price of a varint. The same
    boundary bounds any policy that keys off the count delta, which is why a divergence-adaptive
    fan-out was not built ([§2.4.1](#241-open-research-questions)).
- The remaining delta the other way is **persistence**: AELMDB is LMDB-backed (memory-mapped,
  durable); FingerprintTreeMap is in-memory only. **The structure's SOTA in this niche = "persistent
  RSOS with a secure fingerprint" — persistence is the gap that remains.**
- **What the paper's evaluation does and does not establish.** Its headline (AELMDB 4.69×–13.98×
  faster than the `BTreeLMDB` baseline on reconciliation time) is scoped by §7.1 to single-machine,
  fixed-protocol, reconciliation-heavy workloads. Two qualifications follow from its published
  `results-linux.csv`: the in-memory `Vector` backend is *faster than AELMDB in all six families*
  (0.39×–0.59×) despite not being an RSOS at all (its `fingerprint()` scans the range, O(k) not
  O(log n)); and the 13.98× family runs at **d ≈ 21 % of n** with 1–3 protocol messages — the
  opposite of RBSR's large-n/small-d target, a regime dominated by enumeration and point access
  rather than range aggregation. The transferable result is therefore *"an aggregate-augmented
  persistent engine costs ~2× an in-RAM array on the hot path"* — the figure the persistence
  build-vs-adopt call ([#271](https://github.com/Akvize/reconcile-rs/issues/271)) needed — not
  *"in-tree aggregates beat memory"*.

| Structure | Position/boundary | History-indep. | Diffs… | Structural sharing / versioning | Persistence | Resists leading-zeros | Maturity |
|---|---|---|---|---|---|---|---|
| **FingerprintTreeMap** | B-tree splits (insertion order) | **No** | **value ranges** | No | No (in-mem) | **Yes** (n/a) | pre-alpha |
| MST | level = hash(key) | Yes | nodes | partial | impl-dependent | **No** | mature (Bluesky) |
| Prolly tree | rolling-hash on content | Yes | chunks | **Yes** (CAS) | **Yes** | Yes | mature (Dolt) |
| Merkle radix/SMT | prefix bits | Yes | hash paths | partial | yes | Yes | mature (Ethereum) |
| Fixed-depth Merkle | token range | partial (rebuild) | nodes | no | yes | yes | mature (Cassandra) |
| **RSOS/AELMDB** | augmented B+-tree | not required | ranges | no | **Yes** (LMDB) | yes | research 2026 |

FingerprintTreeMap's **No** on history-independence is **not a weakness**: it is the RSOS family's whole point
(§2.3 #1). MST/prolly *need* history-independence because they diff internal-node hashes; FingerprintTreeMap
diffs value-defined ranges instead and never compares a node hash, so two peers with different tree
shapes still converge.

### 2.2 Competitors at the "reconciliation algorithm" level

The FingerprintTreeMap implements **RBSR**; its competitors are not tree structures.

| Family | Communication | Compute | RTT | Knows *d*? | Adversarial robustness | Maturity |
|---|---|---|---|---|---|---|
| **RBSR, fixed fan-out *b*** (secure fingerprint) | O(d log n) | O(d log n) | **O(log n) sequential** | No (self-adapting) | **Good** | Earthstar/Willow (naive XOR) — Negentropy (secure, *b*=16) |
| **↳ reconcile-rs, default policy** (`b`=16) | O(d log n) | O(d log n) | O(log_16 n) sequential | No | **Good** | reconcile-rs |
| **↳ reconcile-rs, `SqrtFanOut` policy** (fan-out `√m`) | **Θ(√n)**, d-independent | O(d log n) | Θ(log log n) sequential — *equal to `b`=16 below n≈10¹²* | No (self-adapting) | **Good** | reconcile-rs |
| **Rateless IBLT** (SIGCOMM 2024) | **≈ d** (3-4× < non-rateless) | **linear** (2-2000× < minisketch) | **1 streaming exchange** | **No** | **designed for adversarial** | Ethereum state-sync |
| **PBS** (VLDB 2020) | near-optimal ≈ d | **low, by design** | O(log d) rounds | **Yes (estimate)** | not stated | research |
| minisketch/PinSketch (CPI) | **optimal ≈ b·d** | O(d²) | 1 (+ext.) | **Yes (capacity)** | deterministic if capacity | Bitcoin Erlay (BIP 330) |
| CertainSync (2025) | bound f(d,U) | linear | rateless | No | **deterministic success** | SIGMETRICS research |
| Classic IBLT | O(d·(b+log U)) | O(d) | 1 (+estim.) | **Yes** | weak | blockchains |

**Critical reading (stated profile: large n, small d, latency-sensitive, P2P), verdicts only —
full derivations, sweeps and citations live in the linked issues and `benches/README.md`:**
- Fixed-*b* RBSR is the **worst family on latency**: O(log n) sequential RTTs to isolate a
  difference, priced at a measured **1.00 × RTT** per round with no hidden multiplier (F16,
  [#280](https://github.com/Akvize/reconcile-rs/issues/280)) — see
  [§1.3](#13-the-sota-of-set-reconciliation-sourced) for the model-vs-measured distinction.
- **`rbsr`'s `SqrtFanOut`** trades Θ(√n) communication for Θ(log log n) depth — a different
  complexity class in both columns, not a change of base — and is not the default: it costs
  ~14× the refinement bytes and ~47× the CPU time of `b`=16 at small *d*, closing to single-digit
  percent by d=100. Numbers and the datagram-fragmentation cost this trades into:
  `benches/README.md`, [#257](https://github.com/Akvize/reconcile-rs/issues/257).
- **The default fan-out is `b`=16.** The advertised-range count follows `b/ln b` (minimized at
  `b`=3 over the integers), but sweeping the full cost model — bytes, `T_loc`, round count — against
  measured (n, d, clustering) still lands on `b`=16 as the value never worse than `√m` on rounds
  while spending an order of magnitude fewer bytes; `b`=4 wins bytes/CPU but costs an extra round
  trip. Because the policy never crosses the wire, this is a per-node choice, not a wire contract.
  Decision record and rustdoc: [#257](https://github.com/Akvize/reconcile-rs/issues/257),
  `rbsr/src/policy.rs`. Contention-tree analysis reaches a near-identical optimum for the same
  "split a population into `q` groups, recurse on the conflicted ones" problem — Capetanakis, *Tree
  algorithms for packet broadcast channels*, `doi:10.1109/TCOM.1979.1094661`, with the `Q`-ary
  analysis in Mathys–Flajolet, `doi:10.1109/TIT.1985.1057013`. The correspondence stops at two
  points: both optimise channel throughput rather than wire bytes, and both assume fair coins, so
  the split *distribution* is never optimised. Whether an **uneven**, signal-driven split beats the
  balanced rank-cut `FixedFanOut`/`SqrtFanOut` both use is
  [#318](https://github.com/Akvize/reconcile-rs/issues/318).
- **N-party fleets don't get retries for free.** Every cost model on this page is two-party; a fleet
  that is converged but for one divergence has only 2 content classes over any retry count, so
  redundancy buys nothing exactly when it's healthy (refutes arXiv:2212.13567 §5.1 by derivation).
  [#354](https://github.com/Akvize/reconcile-rs/issues/354),
  [#471](https://github.com/Akvize/reconcile-rs/issues/471).
- **No enumeration threshold `t` beats not having one, by default.** Totalled across bytes (values +
  refinement), no swept `t` pays over the shipped policy except by a few percent at the smallest
  value size; untotalled, Negentropy's own cutoff `t`=2b wins on refinement bytes/messages alone, so
  the verdict is conditional on value size and RTT. [#468](https://github.com/Akvize/reconcile-rs/issues/468),
  [#315](https://github.com/Akvize/reconcile-rs/issues/315), `rbsr/src/policy.rs`.
- **The wire `RangeAggregate` (40 B: 32 B `Fingerprint` + 8 B count) costs ~2.2× Negentropy's
  per-range bytes**, rising with `n` — almost entirely the summary-width trade §2.1 makes
  deliberately (secure/exact vs. Negentropy's truncated/probabilistic). Separable from the fan-out
  cost above only by shrinking `b`, now a one-line policy choice.
  [#362](https://github.com/Akvize/reconcile-rs/issues/362), `benches/README.md`.
- **Rateless IBLT** resolves in a single streaming exchange with no *d* estimation and adversarial
  robustness — the strongest single-shot candidate on communication; **PBS** trades a few rounds for
  lower computation instead. RBSR keeps two assets sketches lack: self-adapting (no *d* estimation)
  and ordered-range/partial-prefix reconciliation.
- **Conclusion:** a hybrid design — RBSR to localize coarsely, a leaf sketch to drain the rest in one
  shot — would beat pure FingerprintTreeMap on latency without losing adaptiveness. Which sketch, and
  why not RIBLT by default (incremental maintainability is the selection criterion, not
  communication optimality): [#185](https://github.com/Akvize/reconcile-rs/issues/185).

### 2.3 Real differentiators of the approach (structural strengths)

1. **Value-based range diff ⇒ history-independence is not needed** *(the deepest differentiator)*.
   MST/prolly *must* be history-independent because they compare **internal node hashes** (different
   tree shapes → false positives). FingerprintTreeMap never compares nodes: it computes the **cumulative
   256-bit additive fingerprint over `[a,b)`**, identical on two peers **iff the range content is
   identical**, regardless of each one's B-tree shape. → Convergence guaranteed **without paying**
   for history-independence, and **immunity to the MST leading-zeros attack** (addition-with-carry is
   not GF(2)-linear, unlike XOR).

   **The strongest counter-argument on record**: Meyer & Scherer (2024) show RBSR can be realized
   with conventional (non-homomorphic) hashes over history-independent search trees instead — a
   different point on the same design plane, paying for history-independence but owing nothing to a
   composable-monoid summary. The additive combiner here is therefore a *choice*, not a requirement
   of RBSR; generalizing it to `RSOS<M: Monoid>` is waived to 2.0
   ([#298](https://github.com/Akvize/reconcile-rs/issues/298), `ARCHITECTURE.md` §7).

   **Refinement needs a property RBSR's literature leaves implicit**: every `SPLIT` must narrow the
   range it cuts, or terminate because the peer cuts instead — an oracle-coupled split policy can
   violate it and stall the protocol (measured up to 91.6% of drives pre-fix). This repository's
   fix is a driver guard, not a runtime check: `ARCHITECTURE.md` §5 invariant 13 makes a
   non-narrowing split structurally forced into an `Enumerate`, which is the guard this crate
   ships. Summary and full numbers: [#356](https://github.com/Akvize/reconcile-rs/issues/356),
   [#420](https://github.com/Akvize/reconcile-rs/issues/420),
   [#352](https://github.com/Akvize/reconcile-rs/issues/352)).

2. **It is a SOTA-2026-conformant RSOS**: the `tree_hash` cache (composable summary) + `tree_size`
   (order statistic) → range-summary and rank/select queries in **O(log n)** (the arXiv:2603.19820
   contract). Core *aligned* with the most recent theory.
3. **Cheap incremental maintenance**: `tree_hash ^= diff_hash` + `tree_size += 1` propagated along
   the single root→leaf path → O(log n) amortized. The 2-3× factor vs `BTreeMap` is the *expected*
   price of these two invariants, not an anomaly.
4. **A single structure stores AND reconciles**: no separate Merkle tree to maintain (contrast
   Cassandra which builds the tree at repair time). The store *is* the reconciliation index.
5. **Avoids Cassandra's over-streaming**: the SPLIT recursion tightens onto the ranges that
   actually differ instead of streaming a whole fixed partition.
6. **Rust-native, in-process, embeddable**: a real ecosystem niche (mature equivalents = JVM).

### 2.4 Design axes

The durable design axes for an RSOS in this family are below. They describe the engineering target,
not the project backlog; current implementation decisions belong in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §7 and open work belongs in the issue tracker.

| axis | target | current shape |
|---|---|---|
| summary security | wide, non-GF(2)-linear and keyed where an adversary can write values | 256-bit BLAKE3 lift, additive aggregate; keyed from the cluster key when configured |
| empty/equality semantics | emptiness decided by cardinality, comparisons use count + fingerprint | `Aggregate { size, fingerprint }` |
| canonical bytes | stable application-independent encoding | `rsos::encoding`, not `std::hash::Hash` |
| query cost | logarithmic rank/select/range summary | cached aggregate per B-tree node |
| snapshots | cheap immutable read views | persistent COW nodes behind `Arc`/`ArcSwap` |
| summary generality | reusable range-summary algebra | deliberately concrete in the 1.x API; generic monoid remains a future-major question |
| reconciliation | range refinement whose transport cost tracks the difference | RBSR with local `RefinementPolicy`, fixed fan-out default |
| conflicts | deterministic convergent merge | HLC + node-id total order, LWW register |
| deletion | no resurrection after local timeout alone | tombstones + causal-stability acknowledgements |
| hostile input | authenticated-before-decode, bounded allocation/state | shared-key auth/replay plus explicit peer/message/value bounds |
| write scalability | avoid unnecessary work on the hot root path | eager cached aggregates remain the main write-side cost |
| durability | restart without relearning the whole state | snapshot persistence; incremental persistence remains a separate engineering concern |

#### Why the fingerprint is keyed

A 256-bit additive multiset fingerprint is strong against accidental collision but is still an
algebraic object: a writer who knows the lift can search for cancelling changes. When a cluster key
is configured, `ClusterKey::derive_lift_key` makes the element lift secret from outsiders while
keeping the cached additive aggregate and its `O(log n)` update/query behavior.

This does not protect against a malicious holder of the shared cluster key. That boundary follows
from the authentication model itself; [`SECURITY.md`](./SECURITY.md) is canonical for it.

#### Why the tree keeps eager summaries

RBSR needs a range aggregate cheaply enough that a reconciliation round does not scan the range it
is trying to summarize. `FingerprintTreeMap` therefore updates cached summaries on the write path
and reads them on the reconciliation path. The trade is intentional: reads and reconciliation gain
predictable logarithmic work while writers touch the root path.

The contention benchmark exists to measure that cost rather than pretending it disappears. Its
methodology belongs in [`benches/README.md`](./benches/README.md).

#### Why persistence and content addressing are separate axes

Persistent COW nodes provide structural sharing between in-memory versions: unchanged subtrees keep
their allocation and cached aggregate. Content addressing would add stable identity across
versions/processes and could support incremental durable structures, but it is not required for the
snapshot-read model itself.

#### Why a leaf sketch is not part of this crate

A single-shot sketch such as an IBLT attacks a different trade-off from range refinement: it can
remove round trips when the symmetric difference is within its capacity, but needs its own sizing,
failure and maintenance model. Comparative sketch/policy research belongs outside the engineering
crate until it produces a tested design with a clear integration contract.

#### Capacity boundary

The core is fully replicated and in-memory. That gives it its strongest property — every read is
local and requires no external datastore — and also fixes its capacity ceiling at one node's RAM.
Larger-than-one-node datasets therefore require partial replication/sharding above this layer rather
than a transparent disk backend that would preserve full replication while losing local-memory
economics.

#### What the benchmarks can establish

The committed harness measures this implementation's structure, protocol and system behavior under
controlled workloads. It can establish internal scaling shapes, regressions and trade-offs. It does
not establish a universal ranking against another implementation unless that implementation is run
under a like-for-like harness. External papers remain context, not benchmark results for this code.

