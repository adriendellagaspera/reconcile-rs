# State of the Art — `reconcile-rs` positioning

> **Reference document.** Where `reconcile-rs` sits in the landscape of set reconciliation, diffable
> data structures, and replica consistency. This is **durable
> background**: the field positioning and the design taxonomy move slowly, unlike the code. It
> deliberately carries **no status or findings** — for live correctness/security/maturity status see
> the `v1.0.0` milestone and [issue #206](https://github.com/Akvize/reconcile-rs/issues/206); for the
> resolved-audit historical record see [`ARCHITECTURE.md`](./ARCHITECTURE.md) §8; for the target
> design see [`ARCHITECTURE.md`](./ARCHITECTURE.md).
>
> - **Literature survey dated:** 2026-05-30, with a targeted addendum on 2026-08-10 (arXiv:2603.19820
>   read in full against its four published repositories; §1.3/§2.1/§2.2/§2.3 revised), a
>   cross-community pass on 2026-08-14 (the `cs.IT`/`cs.NI` dialect this document had never searched;
>   §2.2 revised), and a weekly sweep on 2026-08-17 (§2.1's prolly-tree entry revised).
> - **Scope:** the FingerprintTreeMap as a *data structure* and RBSR as an *algorithm*, compared to the published
>   state of the art — not an audit of any particular commit.
> - **`Fxx`** denotes a finding from the original code audit; its resolution record lives in
>   [`ARCHITECTURE.md`](./ARCHITECTURE.md) §8.
> - **Measured figures live in `benches/README.md`, not here** ([#346](https://github.com/Akvize/reconcile-rs/issues/346),
>   option A): §1.3/§2.2 state the claim and verdict a benchmark run supports; the harness output
>   itself — bytes, message counts, timings — is reproduced there, and the decisions it drove are
>   cited by issue number against each axis in §2.4 below. A refinement-policy or benchmark change
>   should never require editing this file.

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

### 1.6 The embedded in-memory data grid (IMDG) use case

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

Fit-for-purpose guidance (good fit / wrong tool) lives once, in README.md's "When to use this" —
not duplicated here. **Path to best-of-breed:** the open performance/scaling roadmap that moves
reconcile-rs from the "real but narrow" niche of §1.2 to a credible Rust IMDG is the axis list in
§2.4 below, each cited against the issue that carries its live status — not here.

---

## 2. Competitor audit and differentiators

> This section refocuses the analysis on the **FingerprintTreeMap as a data structure** (and its protocol),
> not on the full system. *(All structure/algo names below are defined in the
> the surveyed literature.)* Methodological anchor: the FingerprintTreeMap **is not a Merkle tree in the
> MST/prolly sense**. It is a *Range-Summarizable Order-Statistics Store*
> (RSOS) — a B-tree augmented, per node, with a **composable subtree summary** (a 256-bit additive
> fingerprint)
> **+ an order statistic** (the subtree size). This abstraction was formalized in 2026
> (arXiv:2603.19820) as the backend that range-based reconciliation (RBSR, Meyer 2023) needs. Its
> **true peer group** = the other diffable structures; its **true algorithmic competitor** = the
> other set-reconciliation families.

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
    away entirely; comparing `(count, Σ mod 2^τ)` would keep it for the price of a varint. This
    boundary is also a **policy** signal, not only a correctness one:
    [#318](https://github.com/Akvize/reconcile-rs/issues/318)'s divergence-adaptive fan-out keys off
    the same count delta and inherits the same blind spot — settling #318's frequency question by
    the same fact: the delta reads zero on the regime "an LWW register produces continuously"
    (above) and nonzero only on the rarer one, where the shipped default already sits within single
    digits of `SqrtFanOut` (§2.2). **Decision: not built** — record in `rbsr/src/policy.rs`'s own
    rustdoc.
- The remaining delta the other way is **persistence**: AELMDB is LMDB-backed (memory-mapped,
  durable); FingerprintTreeMap is in-memory only. **The structure's SOTA in this niche = "persistent
  RSOS with a secure fingerprint" — persistence is the gap that remains.**
- **What the paper's evaluation does and does not establish.** Its headline (AELMDB 4.69×–13.98×
  faster than the `BTreeLMDB` baseline on reconciliation time) is scoped by §7.1 to single-machine,
  fixed-protocol, reconciliation-heavy workloads. Two readings the text does not foreground, both
  recomputed from the published `results-linux.csv`: the in-memory `Vector` backend is *faster than
  AELMDB in all six families* (0.39×–0.59×) despite not being an RSOS at all (its `fingerprint()`
  scans the range, O(k) not O(log n)); and the 13.98× family runs at **d ≈ 21 % of n** with 1–3
  protocol messages — the opposite of RBSR's large-n/small-d target, a regime where the cost is
  dominated by enumeration and point access rather than range aggregation. The result to carry
  forward is therefore *"an aggregate-augmented persistent engine costs ~2× an in-RAM array on the
  hot path"*, which is the number that was missing from the persistence build-vs-adopt call (see
  [#271](https://github.com/Akvize/reconcile-rs/issues/271)), not *"in-tree aggregates beat memory"*.

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
  `rbsr/src/policy.rs`. A forty-year-old analytical treatment of the same "split a population into
  `q` groups, recurse on the conflicted ones" problem lands near the same optimum from a different
  objective (channel throughput, not wire bytes) and reopens whether an **uneven**, signal-driven
  split beats the balanced rank-cut `FixedFanOut`/`SqrtFanOut` both use — tracked by
  [#318](https://github.com/Akvize/reconcile-rs/issues/318). The treatment is Capetanakis, *Tree
  algorithms for packet broadcast channels*, `doi:10.1109/TCOM.1979.1094661`, with the `Q`-ary
  analysis in Mathys–Flajolet, `doi:10.1109/TIT.1985.1057013`; both assume fair coins throughout, so
  the split *distribution* is never optimised, and the objective is channel throughput rather than
  wire bytes — which is the limit of the correspondence.
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
   [#352](https://github.com/Akvize/reconcile-rs/issues/352).

2. **It is a SOTA-2026-conformant RSOS**: the `tree_hash` cache (composable summary) + `tree_size`
   (order statistic) → range-summary and rank/select queries in **O(log n)** (the arXiv:2603.19820
   contract). Core *aligned* with the most recent theory.
3. **Cheap incremental maintenance**: `tree_hash ^= diff_hash` + `tree_size += 1` propagated along
   the single root→leaf path → O(log n) amortized. The 2-3× factor vs `BTreeMap` is the *expected*
   price of these two invariants, not an anomaly.
4. **A single structure stores AND reconciles**: no separate Merkle tree to maintain (contrast
   Cassandra which builds the tree at repair time). The store *is* the reconciliation index.
5. **Avoids Cassandra's over-streaming**: the SPLIT recursion tightens onto the ranges that
   actually differ instead of streaming a whole fixed partition. (The split fan-out here is
   `√m` by rank, not a fixed branching factor — arXiv:2603.19820's Algorithm 2 is stated for a
   fixed `b`, and Negentropy's default is `b = 16`; neither is what this implementation does. That
   deviation is no longer only a note: §2.2 quantifies what it costs, and it is the one place where
   this implementation is *worse* than the family it belongs to.)
6. **Rust-native, in-process, embeddable**: a real ecosystem niche (mature equivalents = JVM).

### 2.4 The design axes of a *true* SOTA RSOS

These are the axes along which an RSOS is judged against the state of the art — the **design
target** for a structure of this family ("persistent RSOS with a secure, generic fingerprint").
They are described here as durable design goals; each item cites the issue carrying its live
status, so this section never needs an edit when that status changes.

**P0 — Correctness of the structure itself:**
1. **Secure and wide fingerprint**: replace the 64-bit XOR with a **≥256-bit, non-GF(2)-linear**
   combiner (hash-then-add mod 2²⁵⁶, MSet-Mu-Hash/LtHash) or *keyed*. XOR = self-inverse + linear →
   craftable collisions (Gaussian elimination ~2 s even in 256-bit) + birthday at 2³². The path
   taken by Negentropy. **This is THE criterion that separates a "toy" structure from a SOTA one.**
   (cf. F6, [#111](https://github.com/Akvize/reconcile-rs/issues/111)) — but width alone settles
   only the *honest* model: modular addition at 256 bits stays Wagner-breakable, and the keyed-lift
   fix is [#337](https://github.com/Akvize/reconcile-rs/issues/337).
2. **Decouple "empty" from "hash==0"** (`size==0`) — otherwise the structure can claim "converged"
   while having lost data. (cf. F1, [#106](https://github.com/Akvize/reconcile-rs/issues/106))
3. **Stable, versioned hash as a wire contract** (pinned SipHash/xxHash/BLAKE3 + golden-vector).
   (cf. F8, [#111](https://github.com/Akvize/reconcile-rs/issues/111))

**P1 — Generality (what makes it a *structure*, not a special case):**
4. **Generic summary over a monoid**: today `rsos` hardwires its range summary to the 256-bit additive
   `Fingerprint` (`ARCHITECTURE.md` §7, tracked as `BYOLiftingMonoid`); generalizing to `RSOS<M: Monoid>`
   also enables sum/min/max/count and sketches. Enables **embedding a sketch in the leaves** (hybrid
   RBSR + a leaf sketch) to break the O(log n) RTT cost (§2.2). **Waived to 2.0** —
   [#298](https://github.com/Akvize/reconcile-rs/issues/298), decision recorded in `ARCHITECTURE.md` §7.
5. **Fully expose the RSOS contract** — ✅ **done**: `rank`/`select`/`range` are `pub` on the standalone
   `rsos` crate's `FingerprintTreeMap` (ARCHITECTURE.md §3.2), a reusable generic building block
   independent of `reconcile`. (Previously in tension with an earlier ARCHITECTURE.md draft that kept
   these `pub(crate)` inside the monolithic crate; resolved in favor of exposure once `rsos` became its
   own published-intent crate.) Remaining: lazy + double-ended iterators —
   [#92](https://github.com/Akvize/reconcile-rs/issues/92) (the umbrella that consolidated #89–#91),
   naming freezes tracked by [#291](https://github.com/Akvize/reconcile-rs/issues/291).

**P2 — Durability & distributed properties carried by the structure:**
6. **Persistence / content-addressing** *(the big gap vs prolly/AELMDB)*: (a) snapshot+WAL including
   tombstones, or (b) a persistent **copy-on-write** tree, which is what buys *structural sharing* —
   an untouched subtree keeps its node, so its cached aggregate survives untouched.
   **Node content-addressing** is a further step layered on that, and what it adds is *cross-version
   identity* (versioning, diff between snapshots, incremental cold start), not the sharing itself;
   the two are separable and priced separately.
   [#271](https://github.com/Akvize/reconcile-rs/issues/271) tracks the epic; its build-vs-adopt call
   against LMDB/AELMDB is settled in its own body, content addressing parked separately on
   [#188](https://github.com/Akvize/reconcile-rs/issues/188).
7. **Conflict metadata in the value**: HLC + total tie-break `(timestamp, node_id)`; ideally
   **pluggable CRDT** values; versioned tombstones with **causal-stability GC**. (cf. F4
   [#109](https://github.com/Akvize/reconcile-rs/issues/109), F5
   [#110](https://github.com/Akvize/reconcile-rs/issues/110)) — pluggable CRDT deferred, no trigger
   fired: [#184](https://github.com/Akvize/reconcile-rs/issues/184), decision recorded in
   `ARCHITECTURE.md` §7.
10. **Write cost under concurrency** *(the axis the family's cost models omit)*: answering
   `Aggregate(l, u)` in O(log n) requires an up-to-date summary on every node from leaf to root, so
   **every insert writes the root** — a contention point the contract creates, not an implementation
   defect. arXiv:2603.19820 §7.1 scopes its evaluation to single-machine with no concurrency, and no
   RBSR work prices it. The prior art is outside the line: **AB-tree** (Zhao–Xie–Li,
   `doi:10.14778/3538598.3538606`, VLDB 15(9) 2022)
   sheds the contention by storing inexact weights, which a sound SKIP cannot (`benches/README.md`'s
   `contention` benchmark, [#359](https://github.com/Akvize/reconcile-rs/issues/359)/#445/#446).
   **Measured, then re-measured** ([#454](https://github.com/Akvize/reconcile-rs/issues/454)): at
   `N=1` (no lock contention) the contract alone costs 0.298× a no-aggregate `BTreeMap` under the
   same lock, and 6.8 cached-aggregate writes per insert — a count identical on any machine. #359
   read the fp/btree *ratio*, saw it flat to `N=16`, and called the tax bounded. That does not
   follow: both arms sit behind the same lock, so `1/X = S + H(N)` for each, and a ratio of two
   terms that grow together is flat *because* they grow together, not because the root-write share
   is capped. **Subtracting** reciprocal throughputs cancels `H` where dividing them cannot, and
   leaves a gap growing 1.7× to `N=4` (#455) — an upper bound, so the growth is established and its
   attribution is not. Model, confounds and the many-core prediction: #457, #456,
   `benches/README.md`. Numbering starts at 10 so P0–P3's existing ids stay stable.

**P3 — What makes it *believed* to be SOTA:**
8. **Property-testing + fuzzing as a foundation**: `proptest` vs `BTreeMap` oracle +
   `check_invariants`, and especially **the convergence property** (two random trees → diff loop →
   identical state + ranges = true symmetric difference, under reordered/duplicated/dropped
   messages). The category standard (`merkle-search-tree` is fuzz-tested). (cf. F11,
   [#113](https://github.com/Akvize/reconcile-rs/issues/113))
9. **First-class adversarial robustness**: segment-bound validation, allocation bounds, bounded
   fan-out — to hold up against hostile peers (the MST/Willow use case).
   [#284](https://github.com/Akvize/reconcile-rs/issues/284) (RSOS contract),
   [#230](https://github.com/Akvize/reconcile-rs/issues/230) (oversize values),
   [#150](https://github.com/Akvize/reconcile-rs/issues/150) (bounded `peers` map).

### 2.4.1 Open research questions

Opened 2026-08-14: where this repository can test a claim the published work leaves open. One row
per issue, none of them a 1.0 gate; the claim and the evidence live in the issue, not here.

| Question | Issue |
|---|---|
| Is the refinement tree's comparison count sensitive to the *ordered shape* of the difference, and does the `(b, B)` pair matter? | [#353](https://github.com/Akvize/reconcile-rs/issues/353) |
| What is the false-convergence rate at reduced fingerprint width, and do the two layers scale as predicted? | [#355](https://github.com/Akvize/reconcile-rs/issues/355) |
| Post-#257 the comparison-map width is a security question, not a bandwidth one — price it in both models | [#357](https://github.com/Akvize/reconcile-rs/issues/357) |
| Can any path fold one multiset element twice, and what does that cost the summary? | [#358](https://github.com/Akvize/reconcile-rs/issues/358) |
| The contract writes the root on every insert (P2 item 10 above). Where does that bind? | [#359](https://github.com/Akvize/reconcile-rs/issues/359) |

Five results landed with this index rather than as open issues, because they close rather than open
a question: a divergence-adaptive policy is confined to the count, and the count is blind exactly
where the exact-count guarantee has already run out (folded into
[#318](https://github.com/Akvize/reconcile-rs/issues/318)); re-ordering the store does not rescue
that signal — a position-map experiment shows only a
leading-component reorder makes a divergence visible, so "make `π` injective" is the wrong
rule, relocation is; the multidimensional extension is settled on paper as a **no-go**, read against
`arXiv:2603.19820v1` itself — its §8 asks for a theory of *balancing **and** summarization* beyond
one dimension, and the two part company: balancing breaks at one line of its Algorithm 2 and
recovers, the box `Aggregate` Def. 3.9 already carries meets an unconditional cell-probe floor, so
the obstruction is the summary rather than the dimension, and the protocol side transports verbatim
([#360](https://github.com/Akvize/reconcile-rs/issues/360); mechanism and bounds in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §7, and the
write-up itself a preprint this repository deliberately does not version); `Comparison` no longer hands a policy the fingerprint
at all — narrowed to `span()`/`remote_size()`/`agrees()`, making the violation structurally
unspellable rather than merely bounded
([#352](https://github.com/Akvize/reconcile-rs/issues/352)); and a hash-derived
split rule does not cleanly exceed the bound — the sharper, statistically unambiguous result is
that it breaks the protocol's termination guarantee instead, in ~99.5% of drives
([#356](https://github.com/Akvize/reconcile-rs/issues/356), full numbers in §2.3's "Empirical
grounding for the split-boundary half of this claim").
[#354](https://github.com/Akvize/reconcile-rs/issues/354) left it the other way: opened as a
question, closed by derivation rather than the campaign it proposed (§2.2).

**SOTA target by axis:**

| Axis | SOTA target |
|---|---|
| Summary | ≥256-bit non-linear/keyed, **generic (monoid)** |
| Empty vs hash | emptiness/equality decided on `size`, never on the fingerprint |
| Hash | fixed, versioned hash as a wire contract |
| Backend | **persistent RSOS**, ideally content-addressed |
| Algo | **hybrid RBSR + a leaf sketch** for single-shot latency; which sketch is open, and incremental maintainability — not communication optimality — is the selection criterion |
| Writes | aggregate maintenance that does not serialise every writer on the root |
| Conflicts | HLC + deterministic total tie-break / pluggable CRDT |
| Deletions | causal-stability GC (no resurrection) |
| Confidence | property tests + convergence fuzzing against an oracle |

**In one sentence:** the FingerprintTreeMap starts from the **right skeleton** — an RSOS, the design validated by
2026 research, with a real differentiator (value-based diff that removes the need for
history-independence). The remaining distance to a *true* SOTA structure is along the axes above; the
structural ones (secure/generic fingerprint, persistence/content-addressing, property-testing
foundation) belong to the structure itself, while conflicts, GC and robustness belong to the
surrounding system.
