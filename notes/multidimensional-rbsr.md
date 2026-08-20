# The analysis transports, the contract does not

### Multidimensional range-based set reconciliation is blocked by summarization, not by order statistics

*A note on §8 of E. G. Amparore, "RBSR via Range-Summarizable Order-Statistics Stores"
(arXiv:2603.19820, 2026).*

---

## Abstract

§8 of [Amp26] names multidimensional reconciliation as the extension the paper does not take, and
identifies the obstacle correctly in one clause — *efficient cuts must balance element counts rather
than cutting a geometric domain in half*. This note finishes the sentence. We show that the RBSR
soundness argument transports to a product order in `δ > 1` dimensions essentially unchanged, with
exactly one exception: the depth bound, which holds only while cuts are count-balanced. Balancing
counts inside a box requires an order statistic **restricted to that box**, where Def. 3.9 of [Amp26]
supplies only a global `Rank`/`Select`. So the extension is a data-structure question, not an
analysis one.

Pricing that question against the range-selection and orthogonal-range-searching literature yields
the result we did not expect. In two dimensions the *missing* primitive is **cheaper** than the one
the contract already carries: range-restricted selection costs `O((lg n / lg lg n)²)` amortized in
linear space [HMN11], while the summary-carrying box aggregate — Def. 3.9's own `Aggregate` — is
subject to an unconditional `Ω((lg n / lg lg n)²)` cell-probe lower bound [Lar12, Păt07]. Since the
one-dimensional aggregate is `Θ(lg n)` and tight [PD04], **no `δ ≥ 2` RSOS answers Def. 3.9 in
`O(log n)`**, and the obstruction is attributable specifically to carrying a summary: the proven
bound is for *weighted* range counting, and matching it for unweighted counting is open [Păt07].

The barrier is the **range-summarizable** half of RSOS, not the **order-statistics** half.

We also report a measurement that refutes the natural repair. Faced with the reconciliation blind
spot for same-key conflicts, the obvious advice is to make the position map `π` injective so the two
versions of a record occupy distinct positions. Driving an unmodified RBSR implementation under
three position maps shows this is wrong: distinct-but-*adjacent* positions are exactly as
unseparable as a shared position, because cut points come from `Select` and no peer holds anything
between them. What decides visibility is whether the update **relocates** the record. That result
has a direct bearing on the main thesis: a lexicographic composite key — the tempting cheap route to
"multidimensional" — is a one-dimensional store wearing a tuple, and buys none of the box queries
whose cost this note is about.

---

## 1. The open question

[Amp26] formalizes the backend RBSR [Mey23] needs as a **range-summarizable order-statistics store**
(RSOS) and proves the algorithm's local-cost bounds over it. Its §8 names the multidimensional
extension as future work and does not take it. Willow [Wil] reconciles three-dimensional products of
ranges in production with no collision analysis attached to the practice. Between the two, what
actually blocks the extension had not been identified: whether the obstacle is the probabilistic
analysis, the protocol, or the backend contract.

This note answers that: it is the backend contract, and it is a specific, priced, well-studied piece
of it.

**Notation.** `δ` is the dimension throughout. (The partitioned-set-reconciliation literature uses
`δ` for difference size; we write `d` for that quantity here.) `n` is store size, `b` the refinement
fan-out, `t` the enumeration threshold, `lg` the base-2 logarithm.

---

## 2. Background

RBSR reconciles two ordered sets by exchanging summaries over shrinking ranges. Each round, a peer
answers each active range with one of three outcomes: **SKIP** when the two summaries agree,
**IDLIST** when the range is small enough to enumerate, or **SPLIT** into `b` children otherwise.
Def. 3.8 of [Amp26] specifies the split as a *balanced `b`-partition*: the cut points are chosen at
equally spaced **ranks**, not at equally spaced key values.

Def. 3.9 requires four operations of the backend:

| operation | meaning |
|---|---|
| `size()` | `\|X\|` |
| `Aggregate(l, u)` | the composable summary of `X ∩ [l, u)`, with its cardinality |
| `Rank(z)` | `\|{x ∈ X : x < z}\|` |
| `Select(r)` | the key at in-order position `r` |

`Rank` and `Select` are **global**: they are defined over the whole store's total order. This is the
detail the rest of this note turns on.

The reference implementation used for the measurements in §6 is `reconcile-rs`, whose `rbsr` crate
implements Algorithm 1 over this contract with `b = 16` and compares whole aggregates — the
comparison map `f_p` is the identity, so a range whose peers hold different cardinalities is never
skipped, with probability 1 and no assumption on the summary.

---

## 3. The analysis is dimension-free

Replace the total order by a product order on `D₁ × … × D_δ` and the ranges by axis-aligned boxes.
Every step of the soundness argument survives, for reasons that never mention dimension:

| step | why it survives in `δ > 1` |
|---|---|
| one-sided error — a SKIP is wrong only if two distinct contents share a summary | reads no order at all; a statement about the summary alone |
| compared ranges are pairwise nested-or-disjoint | children partition their parent and siblings are disjoint, by induction — a property of **partition refinement**, not of the underlying order |
| laminar-family bound, signature collapse | consequences of the row above, so they follow |
| live regions at depth `i` are `≤ min(bⁱ, p)` for `p` differing elements | disjointness plus pigeonhole |
| depth `≤ log_b(n/t)` | **holds if and only if the cut is count-balanced** |

The worst-case comparison count and the union bound over the refinement tree are therefore
dimension-free. Only the last row is conditional, and it is conditional on precisely the property
§8 of [Amp26] singles out.

**Why a geometric cut fails.** Halving a box along an axis by *value* places no bound on how the
elements inside are distributed between the two halves: an adversarial or merely skewed dataset puts
all of them on one side, the refinement makes no progress, and the depth is bounded by the domain's
resolution rather than by `log_b(n/t)`. The `b`-partition must be balanced in **counts**, which is
what Def. 3.8 already says in one dimension and what §8 correctly identifies as the requirement in
more.

---

## 4. The contract is not

To cut a box `B` into count-balanced children along axis `j`, a peer must answer:

> **Range-restricted select.** Given a box `B`, an axis `j`, and a rank `r`, return the value
> `z ∈ D_j` such that `|{x ∈ X ∩ B : x_j < z}| = r`.

Def. 3.9 supplies `Select(r)` over the whole store. In one dimension the two coincide, because a
range *is* a contiguous run of the total order and the box-restricted rank is the global rank offset
by `Rank(l)`. In `δ ≥ 2` they come apart: a box is not a contiguous run of any single order, so no
global order statistic answers the query.

This is the whole of the gap, stated as a primitive. It is also, and this is the point of the next
section, **not a new problem**.

**The primitive is the studied one.** In `δ = 2`, take `B = [x₁, x₂) × [y₁, y₂)` and `j = y`. Let
`T = {p ∈ X : p.x ∈ [x₁, x₂)}` and `c = |{p ∈ T : p.y < y₁}|`. Ordering `T` by `y` puts the `c`
elements below `y₁` first, then exactly `X ∩ B` in order. So the `r`-th smallest `y` in `X ∩ B` is
the `(c + r)`-th smallest `y` in `T`, for any `r ≤ |X ∩ B|` — which the protocol guarantees, since it
cuts at ranks within the box. The two ingredients are a three-sided range count and

> *"given `n` points in the plane, an `x`-range `Q` and an integer `k`, return the `k`-th smallest
> `y`-coordinate from the set of points that have `x`-coordinates in `Q`"*

which is verbatim the problem statement of [HMN11]. The operation multidimensional RBSR needs is
**dynamic range selection**, exactly as that literature defines it.

---

## 5. Pricing it — and the inversion

Per operation, cell size `w = Θ(lg n)`, any polylogarithmic update time:

| Def. 3.9 operation | `δ = 1` | `δ = 2` |
|---|---|---|
| `Aggregate` — group-valued summary over the range | `Θ(lg n)`, **tight** [PD04] | `Ω((lg n / lg lg n)²)` — **lower bound** [Lar12], strengthening [Păt07] |
| order statistics (`Rank`/`Select`, then range-restricted `Select`) | `O(lg n)`, counted B-tree | `O((lg n / lg lg n)²)` amortized, **linear space** [HMN11] |

Three observations, in order of how much they change the picture.

**5.1 The primitive is tight, not merely unimproved.** Statically, range selection costs
`Θ(lg n / lg lg n)`: the upper bound is [BJ09] and the matching cell-probe lower bound
`Ω(lg n / lg lg n)`, for any structure in `n · lg^O(1) n` bits, is [JL11]. So the `δ > 1` price is a
property of the problem, not an artifact of nobody having tried.

**5.2 The lower bound binds the incumbent operation, not the new one.** [Lar12] proves
`t_q = Ω((lg n / lg(w · t_u))²)` for **weighted** two-dimensional range counting, i.e.
`Ω((lg n / lg lg n)²)` at `w = Θ(lg n)` under any polylog update. Def. 3.9's `Aggregate` returns a
composable summary over the range — in `reconcile-rs`, a 256-bit fingerprint under addition modulo
`2²⁵⁶`. A structure answering box-aggregate queries for 256-bit group weights answers them for
`Θ(lg n)`-bit weights by padding, so the bound applies a fortiori. The operation it binds is the one
the contract has had since Def. 3.9.

**5.3 Therefore the missing primitive is the cheap half.** At `δ = 2`, range-restricted `Select`'s
best known **upper** bound coincides with the box `Aggregate`'s unconditional **lower** bound — and
achieves it in linear space, where a range tree carrying aggregates costs `O(n lg n)`. Adding the
operation Def. 3.9 lacks does not change the asymptotic cost of the contract. Dimension does, on
every operation at once.

> **Proposition.** For `δ ≥ 2`, no RSOS whose `Aggregate` carries a group-valued summary of at least
> `Θ(lg n)` bits answers Def. 3.9's operations over boxes in `O(lg n)` time per operation with
> polylogarithmic updates, in the cell-probe model with cell size `Θ(lg n)`.

*Proof.* Immediate from [Lar12] by the padding reduction in 5.2. ∎

This is a reduction to a known bound, not a new bound; the contribution is the connection, which
neither the reconciliation literature nor §8 had drawn.

**5.4 The tax, and where it actually hurts.** Since `δ = 1` is `Θ(lg n)` and tight, the dimensional
tax on the contract is `lg n / (lg lg n)²`. At deployment scale that floor is almost free — and the
gap between the floor and anything one knows how to build is not:

| `n` | `δ = 1`, `lg n` | `δ = 2` floor, `(lg n / lg lg n)²` | ratio | dynamic 2D range tree, `lg² n` | ratio |
|---|---|---|---|---|---|
| 10⁶ | 20 | 21 | **1.07×** | 397 | **20×** |
| 10⁹ | 30 | 37 | **1.24×** | 894 | **30×** |
| 10¹² | 40 | 56 | **1.41×** | 1589 | **40×** |

A dynamic range tree with subtree aggregates [WL85] — the direct `δ = 2` lift of an
aggregate-augmented B-tree, and what an implementer would actually write — costs `O(lg² n)` time and
`O(n lg n)` space. For an in-memory store, a 20–30× regression in both time and memory is
disqualifying on its own, independently of the asymptotic argument.

So the answer to "is a range-restricted `Select` affordable at `O(log n)`" is **no**, twice over and
for two different reasons: the floor is superlogarithmic, and the constructions that exist sit a
`(lg lg n)²` factor above the floor.

**5.5 The sharpest form of the thesis.** The bound in 5.2 is proved for *weighted* range counting.
Matching it for **unweighted** counting was posed as an open problem by [Păt07] and, to our
knowledge, remains open. Order statistics in two dimensions are affordable — linear space,
`(lg n / lg lg n)²` — and it is the summary that is provably not. RBSR cannot drop the summary: it is
what decides SKIP. Hence:

> In one dimension, order statistics and summarization coexist at `Θ(lg n)`. In two or more they
> separate, and it is summarization that becomes expensive. What blocks multidimensional RBSR is the
> **range-summarizable** half of RSOS, not the **order-statistics** half.

---

## 6. A measurement, and why "make `π` injective" is wrong advice

An RSOS is ordered by a **position map** `π`, the projection from a stored record to its position.
The exact-count guarantee — a range whose peers hold different cardinalities is never skipped — is
worth nothing on a divergence in which every range stays count-balanced. Same-key,
different-value conflicts, which a last-write-wins register produces continuously, are exactly that
case: the two versions occupy one position, so both peers report the same cardinality everywhere.

The natural repair is to make `π` injective, separating the two versions. We tested it. The harness
plants one divergence — 500 records per peer, differing only in the value at key 250 — drives the
unmodified `reconcile-rs` protocol at full fingerprint width, and counts the ranges the driver asked
about on which the two peers hold different cardinalities:

| arm | `π` | the two conflicting records | separable? | rounds | ranges asked | **unbalanced** |
|---|---|---|---|---|---|---|
| 1 | `(key)` | one shared position | no — they tie | 5 | 73 | **0** |
| 2 | `(key, version)` | distinct, but adjacent | **no — adjacency** | 5 | 73 | **0** |
| 3 | `(timestamp, key)` | relocated across the order | yes | 6 | 92 | **16** |

*(`rbsr/tests/balance_under_position_map.rs` in `reconcile-rs`; figures reproduced 2026-08-20.)*

**Arm 2 is the refutation.** Injectivity is achieved and nothing changes. Separating two records
requires a cut point strictly between them, and cut points come from `Select` — that is, from
positions some peer actually holds. `(250, 1)` and `(250, 2)` are adjacent; no peer holds anything
between them; no `Select` can ever produce a separating cut. Two distinct-but-adjacent positions are
exactly as unseparable as one shared position, by a different mechanism and with an identical
observable outcome.

What decides visibility is whether the update **relocates** the record — whether the *leading*
component of the order is the one that changed. `(key, version)` puts the stable component first and
keeps both versions contiguous. `(timestamp, key)`, which is Negentropy's order [Neg], puts the
changing component first, so rewriting a record moves it past every record whose timestamp lies
between the two — and those records are the cut points that separate them.

**The bearing on §5.** Arm 2 is a lexicographic composite key, which is the tempting cheap route to
a "multidimensional" store: keep one total order, put the extra coordinates behind the first. The
measurement shows what that buys, from the protocol's side: nothing the leading component did not
already give. A lex order is one-dimensional, its ranges are intervals rather than boxes, and every
operation stays at `Θ(lg n)` precisely because it answers no box query. Dimensionality is not
purchasable with a tuple key; it must be bought as a product order, at the price §5 establishes.

---

## 7. What this says about deployed systems

Willow [Wil] reconciles three-dimensional products of ranges. This note implies a fork, and which
horn a given implementation takes is checkable from its split rule:

- If it cuts boxes **geometrically**, the depth bound of §3 does not apply, and its round complexity
  is governed by the domain's resolution and the data's skew rather than by `log_b(n/t)`.
- If it cuts boxes at **balanced counts**, it is answering the range-restricted select of §4, and it
  pays §5's cost on every operation.

We have not audited any implementation and make no claim about which holds in practice; we note that
the question is well-posed, cheap to answer from source, and as far as we know has not been asked.
The same fork applies to any future multidimensional RSOS.

For one-dimensional deployments the note is reassuring rather than otherwise: §5.1 and [PD04]
together say that an aggregate-augmented B-tree answering `Aggregate` in `O(lg n)` and `Rank`/
`Select` in `O(lg n)` is **optimal**, not merely adequate.

---

## 8. Limitations, and what would change the answer

**Limitations.**

1. §3 is a transport argument in table form, not a formalized theorem: the `δ`-dimensional protocol
   is not fully specified here and the induction is not written out.
2. §5's Proposition is a reduction to [Lar12], not a new lower bound.
3. There is no `δ ≥ 2` implementation and no measurement at `δ ≥ 2`. The measurement in §6 is
   one-dimensional; it bears on the position map, not on the cost model.
4. §7 is a well-posed question about deployed systems, not an audit of one.

**Open questions.**

- **Does the `(lg lg n)²` gap close for a group-valued box aggregate?** The lower bound of §5.2 is
  confirmed; we found no matching upper bound. A structure achieving `O((lg n / lg lg n)²)` box
  aggregation for group weights — ideally in linear space, as [HMN11] achieves for selection — would
  cut the practical cost of `δ = 2` by more than an order of magnitude and reopen the engineering
  question that §5.4 closes. It would not affect the Proposition.
- **Is the unweighted case genuinely easier?** [Păt07]'s open problem is the hinge of §5.5. A
  matching bound for unweighted counting would generalize the obstruction from summarization to
  dimension itself; a separation would confirm the sharpened thesis.
- **Does a per-session static snapshot help?** RBSR observes one snapshot per round. Static range
  selection is `Θ(lg n / lg lg n)` [JL11, BJ09] — cheaper than one dimension. The question is whether
  a structure can be maintained dynamically but *queried* as a static one within a session without
  paying `Θ(n)` to build it; if so, the dynamic bound of §5.2 is the wrong one to quote.
- **Is the protocol's requirement weaker than exact selection?** §4 asks for an exact count-balanced
  cut. The depth bound tolerates an approximate one: a cut splitting `[αm, (1−α)m]` for constant `α`
  changes `log_b(n/t)` by a constant factor. Whether approximate range selection is asymptotically
  cheaper than exact is, to our knowledge, not settled by the results cited here, and it is the most
  promising route to a genuine `O(lg n)` multidimensional RSOS.

---

## 9. Conclusion

§8 of [Amp26] named the requirement correctly and left the cost unstated. The cost is: the RBSR
soundness analysis carries into `δ > 1` unchanged, the RSOS contract does not, the missing primitive
is dynamic range selection, and that primitive is the affordable half of the resulting contract. What
makes dimension expensive is the summary, which is the one thing RBSR cannot do without.

### Draft status — before submission

Two verification debts remain open on this draft, both recorded rather than resolved:

- The quotation from §8 and the numbering of Def. 3.4/3.5/3.8/3.9 and Prop. 4.1 are taken from this
  project's reading notes on [Amp26]. Network egress blocked every attempt to re-open the PDF while
  this draft was written, so they must be checked against the source before submission.
- The cell-probe theorem statements in §5 are as reported by secondary summaries, not read from
  [Lar12], [Păt07], [PD04], [JL11] or [HMN11] directly. The DOIs were confirmed; the exact
  quantifiers, space assumptions and update-time regimes were not.

Neither debt affects the direction of the argument, and both are load-bearing for its precision.

---

## References

- **[Amp26]** E. G. Amparore. *RBSR via Range-Summarizable Order-Statistics Stores.*
  arXiv:2603.19820, 2026.
- **[Mey23]** A. Meyer. *Range-Based Set Reconciliation.* arXiv:2212.13567; IEEE SRDS 2023.
- **[HMN11]** M. He, J. I. Munro, P. K. Nicholson. *Dynamic Range Selection in Linear Space.*
  ISAAC 2011, LNCS 7074, 160–169. `doi:10.1007/978-3-642-25591-5_18`; arXiv:1106.5076.
- **[JL11]** A. G. Jørgensen, K. G. Larsen. *Range Selection and Median: Tight Cell Probe Lower
  Bounds and Adaptive Data Structures.* SODA 2011, 805–813. `doi:10.1137/1.9781611973082.63`.
- **[Lar12]** K. G. Larsen. *The Cell Probe Complexity of Dynamic Range Counting.* STOC 2012, 85–94.
  `doi:10.1145/2213977.2213987`; arXiv:1105.5933.
- **[Păt07]** M. Pătraşcu. *Lower Bounds for 2-Dimensional Range Counting.* STOC 2007, 40–46.
  `doi:10.1145/1250790.1250797`.
- **[PD04]** M. Pătraşcu, E. D. Demaine. *Tight Bounds for the Partial-Sums Problem.* SODA 2004;
  journal version *Logarithmic Lower Bounds in the Cell-Probe Model*, SIAM J. Comput. 35(4):932–963,
  2006.
- **[BJ09]** G. S. Brodal, A. G. Jørgensen. *Data Structures for Range Median Queries.* ISAAC 2009,
  LNCS 5878.
- **[Aga17]** P. K. Agarwal. *Range Searching.* In *Handbook of Discrete and Computational Geometry*,
  3rd ed., ch. 41. CRC Press, 2017.
- **[WL85]** D. E. Willard, G. S. Lueker. *Adding Range Restriction Capability to Dynamic Data
  Structures.* J. ACM 32(3), 1985.
- **[Wil]** Willow Protocol. *3d Range-Based Set Reconciliation.*
  https://willowprotocol.org/specs/3d-range-based-set-reconciliation/index.html
- **[Neg]** Negentropy. https://github.com/hoytech/negentropy

---

*Source and measurements: [`reconcile-rs`](https://github.com/Akvize/reconcile-rs) —
`rbsr/tests/balance_under_position_map.rs` for §6, `ARCHITECTURE.md` §7 for the decision this note
records, [issue #360](https://github.com/Akvize/reconcile-rs/issues/360) for its history.*
