# Balancing is affordable, summarization is not

### An answer to §8 of *RBSR via Range-Summarizable Order-Statistics Stores*

*On E. G. Amparore, arXiv:2603.19820v1 (2026), whose §8 asks for "a clearer theory of balancing and
summarization beyond the one-dimensional setting considered here".*

---

## Abstract

[Amp26] closes by naming its own extension and the two things it would need:

> Finally, extensions to richer ordered domains (such as composite-key or multidimensional
> reconciliation) appear promising, but would require a clearer theory of **balancing and
> summarization** beyond the one-dimensional setting considered here. — §8

This note answers which of the two binds. The paper's correctness result transports to `δ > 1`
dimensions **verbatim**: the proof of Prop. 4.1 invokes only that a SPLIT's children are pairwise
disjoint with union the parent, which is a property of partition refinement and not of the
underlying order. So does its combinatorial accounting (Def. B.1, Eq. 1). What does not transport is
the storage side of the paper's own cost factorization — the `h` in `T_loc = O(Qh)` — and Lemma B.2
localizes the failure to two of the four line items it charges a SPLIT for.

**Balancing** breaks first but recovers. Algorithm 2 cuts at `Select(Rank(l) + ⌊jm/b⌋)`, a
composition that presumes the queried range occupies a contiguous run of one global order; in a
product order it is not even well-defined. The operation that replaces it is **dynamic range
selection**, a named and tight problem in the data-structures literature, available at
`O((lg n / lg lg n)²)` for queries and updates alike [BJ09, HMN11].

**Summarization** does not recover. `Aggregate` over a box, carrying a summary at least `Θ(lg n)`
bits wide, is dynamic weighted orthogonal range counting, for which there is an unconditional
cell-probe lower bound of `Ω((lg n / lg lg n)²)` under any polylogarithmic update time [Lar12],
strengthening [Păt07]. Since one-dimensional aggregation is `Θ(lg n)` and tight [PD04], **no
`δ ≥ 2` RSOS keeps Lemma B.2's `O(h)` per operation** — and the operation that costs it is one
Def. 3.9 already has.

So the missing primitive is the affordable half, and its best known upper bound lands exactly on the
incumbent operation's unconditional lower bound.

> Of the two things §8 asks for, balancing is a data-structure problem with a solved answer.
> Summarization is a lower bound.

We also report a measurement bearing on the other domain §8 names. For **composite-key**
reconciliation the natural design is to make the position map `π` injective by appending a version
component. Driving an unmodified RBSR implementation under three position maps shows that this buys
nothing: distinct-but-*adjacent* positions are exactly as unseparable as a shared position, because
cut points come from `Select` and no peer holds anything between them. What decides visibility is
whether the update **relocates** the record. The same result explains why composite keys are cheap —
a lexicographic tuple is a one-dimensional store, and it answers no box query.

---

## 1. What §8 asks, and what this note answers

[Amp26] formalizes the backend RBSR [Mey23] needs as a **range-summarizable order-statistics store**
(RSOS), proves that an aggregate-augmented B⁺-tree realizes it, and evaluates AELMDB, a persistent
realization inside LMDB. Its §8 names two extensions to richer ordered domains — composite-key and
multidimensional reconciliation — and declines both, for a stated reason: they "would require a
clearer theory of balancing and summarization beyond the one-dimensional setting considered here".

The requirement is named precisely and left unpriced. This note prices it, and finds the two halves
behave in opposite ways. It is written against the paper's own numbered results rather than against
a paraphrase of them: what transports is Prop. 4.1, Def. B.1 and Eq. (1); what breaks is Algorithm 2
and Lemma B.2.

**Notation.** `δ` is the dimension throughout. (The partitioned-set-reconciliation literature uses
`δ` for difference size; we write `d` for that quantity.) `n` is store size, `h` the RSOS tree
height, `b` the fan-out, `t` the enumeration threshold, `Q` the number of queried ranges, `lg` the
base-2 logarithm.

---

## 2. The paper's structure, as this note uses it

**The contract.** Def. 3.4 fixes an element-summary monoid `(M, ⊕, 0_M)` with `ϕ : U → M` and
`Σ(S) := ϕ(x₁) ⊕ … ⊕ ϕ(x_k)`. Def. 3.5 bundles it with cardinality into the aggregate monoid
`A := (ℕ × M, ⊗, (0, 0_M))`, `A(S) := (|S|, Σ(S))`. Def. 3.6 puts the comparison map `f_p : A → F`
in the protocol layer and explicitly scopes it out of the RSOS abstraction. Def. 3.7 gives global
`Rank_X(z) := |{x ∈ X : x ≺ z}|` and `Select_X(r) := x_r`. Def. 3.8 defines a **balanced
`b`-partition**: each part holds `⌊m/b⌋` or `⌈m/b⌉` of the `m := |X ∩ [l,u)|` elements. Def. 3.9
requires of the store

```
size()          → |X|
Aggregate(l, u) → A(X ∩ [l, u))
Rank(z)         → Rank_X(z)
Select(r)       → Select_X(r)
Enumerate(l, u) → the ordered contents of X ∩ [l, u)
```

together with `Insert(x)` and `Delete(x)`.

**The cut.** Algorithm 2 realizes Def. 3.8:

```
r0 ← O.Rank(l)
for j = 1 to b − 1:
    q_j ← ⌊j·m/b⌋
    append O.Select(r0 + q_j) to C
```

This is the mechanism the whole note turns on, and §4 returns to it.

**The cost model.** Def. B.1 fixes the reconciliation tree `T`, with `Q := |T|` queried ranges, `I`
internal SPLIT nodes and `L` leaves, and Eq. (1) relates them: `L ≤ 1 + (b−1)I`, `Q ≤ 1 + bI`.
Lemma B.2 charges one responder step:

| answer | cost | what it does |
|---|---|---|
| SKIP | `O(h)` | one `Aggregate(l, u)` |
| IDLIST | `O(h + k)` | one `Aggregate`, then `Enumerate` |
| SPLIT | `O(bh)` | one parent `Aggregate`, one `Rank(l)`, `b−1` `Select`, up to `b` child `Aggregate` — each `O(h)` |

Theorem B.3 sums it to `T_loc = O(hQ + bhI + K)`, and for fixed `b` and `t` to `T_loc = O(Qh)`. The
paper states the resulting separation explicitly:

> the protocol-side complexity, captured by the number `Q` of queried ranges and the total explicit
> output size `K`; the storage-side complexity, captured by the factor `h`. — §B.4

That factorization is this note's instrument. §3 shows the protocol side survives `δ > 1`. §4 and §5
show the storage side does not, and why the two halves of §8's request part company there.

The reference implementation for §6 is `reconcile-rs`, which implements Algorithm 1 with `b = 16`
and takes `f_p` to be the identity on `A`, so a range whose peers hold different cardinalities is
never skipped — with probability 1, and with no assumption on `Σ`.

---

## 3. The protocol side is dimension-free

Replace the total order by a product order on `D₁ × … × D_δ`, and ranges `[l, u)` by axis-aligned
boxes. Then:

| the paper's result | status in `δ > 1` |
|---|---|
| **Prop. 4.1** — sound SKIP decisions ⟹ RBSR computes the exact `Δ(X, Y)` | **holds verbatim.** Its proof uses SKIP soundness by hypothesis, exact resolution at IDLIST leaves, and — for SPLIT — only that "the child ranges are pairwise disjoint and their union is exactly the parent range", then inducts over the finite refinement tree. No step reads the order |
| **Termination** — cut values are drawn from a finite set of endpoints and keys present in a replica | holds: the argument counts candidate cuts, and a product order over finite replicas still offers finitely many |
| **Def. B.1 / Eq. (1)** — `L ≤ 1 + (b−1)I`, `Q ≤ 1 + bI` | holds: a statement about a tree of out-degree `≤ b`, indifferent to what a node denotes |

So the whole protocol side of §B.4's factorization — correctness, termination, and the accounting
that turns split counts into queried-range counts — carries over with no modification. A
multidimensional RBSR is not at risk of being *wrong*.

[Mey23]'s footnote 2 makes the first row stronger still: "for correctness it already suffices that
the subranges **cover** the original range" — partitions are preferred only because they "minimally
cover the original range without containing any duplicate items". An axis-aligned box decomposition
covers by construction. (Disjointness is what §6's exact-count argument needs, not what correctness
needs.)

**Where the bound on `Q` lives.** `T_loc = O(Qh)` is execution-sensitive by design: [Amp26]
parameterizes on `Q` rather than bounding it, and states no depth bound. That bound belongs to the
RBSR line. [Mey23] §3.2.2 gives a reconciliation tree of height `≤ 2·⌈log_b(n_min)⌉`, lowered by
`⌊log_b(t)⌋`, hence `2 + 2·⌈log_b(n_min)⌉ − ⌊log_b(t)⌋ ∈ O(lg n)` communication rounds. It rests on
each split dividing a range's item count by `b` — and [Mey23] realizes that cut exactly as [Amp26]
later does:

> it computes […] the number of items it has in the range, and uses this information for determining
> the sizes of the subranges to create. **Finding the boundaries of those subranges amounts to
> looking up items by index in an order-statistic tree**, and thus takes logarithmic time. — §4.3

Both papers reduce the cut to a lookup **by index**, four years apart, and neither remarks that "by
index" is meaningful only because a range is a contiguous run of the order. §4 is that remark.

---

## 4. Balancing breaks at one line of Algorithm 2, and recovers

The composition `Select(Rank(l) + q_j)` is correct in one dimension for a specific reason: a range
`[l, u)` is a contiguous run of the total order, so the `q_j`-th element *of the range* is the
`(r₀ + q_j)`-th element *of the store*. Range rank and global rank differ by the constant offset
`r₀ = Rank(l)`.

In `δ ≥ 2` there is no such offset. A box is not a contiguous run of any single order, so no global
order statistic reaches its interior, and `r₀ + q_j` denotes nothing. The operation Def. 3.8 needs
in a box is:

> **Range-restricted select.** Given a box `B`, an axis `j`, and a rank `r`, return the value
> `z ∈ D_j` such that `|{x ∈ X ∩ B : x_j ≺ z}| = r`.

This is a strictly stronger primitive than Def. 3.9's `Select`, and it is exactly what §8's "efficient
splits should balance item counts rather than merely cut a geometric domain in half" — which
[Amp26] credits to Willow's three-dimensional adaptation [Wil] — turns out to require.

**It is a solved problem.** In `δ = 2`, take `B = [x₁, x₂) × [y₁, y₂)` and `j = y`. Let
`T = {p ∈ X : p.x ∈ [x₁, x₂)}` and `c = |{p ∈ T : p.y ≺ y₁}|`. Ordering `T` by `y` places the `c`
elements below `y₁` first and then exactly `X ∩ B` in order, so the `r`-th smallest `y` in `X ∩ B`
is the `(c + r)`-th smallest `y` in `T`, for any `r ≤ |X ∩ B|` — which Algorithm 2 guarantees, since
`q_j < m`. The two ingredients are a three-sided range count and

> *"given `n` points in the plane, an `x`-range `Q` and an integer `k`, return the `k`-th smallest
> `y`-coordinate from the set of points that have `x`-coordinates in `Q`"*

which is verbatim the problem [HMN11] solves. Both ingredients are available at the same cost and in
one structure: [BJ09]'s dynamic structure supports "range selection queries […] and **dominance
queries (range rank)**" in `O((lg n / lg lg n)²)` for queries *and* updates, using
`O(n lg n / lg lg n)` space. [HMN11] improves the space to **linear** at the same time bounds, but
its Theorem 1 states the selection half only, so the citable figure for the *whole* primitive is
[BJ09]'s.

Statically the problem is tight at `Θ(lg n / lg lg n)`: [JL11] proves `Ω(lg n / lg(Sw/n))` for any
structure in `S` words, hence `Ω(lg n / lg lg n)` at `n·lg^O(1) n` space, and [BJ09] matches it in
**linear** space. [CW11] closes the adaptive form exactly — `O(1 + lg_w k)` for rank `k`, "exactly
matching the lower bound proved by Jørgensen and Larsen" — and states the connection this reduction
relies on in one line: range selection "is closely related to 2-D 3-sided orthogonal range counting".

So the price of balancing in a box is known, not merely unimproved. Where it is *not* pinned is the
fully-bounded box: [HMN11]'s query bounds one axis and leaves the cut axis free — the semi-bounded
case. The fully-bounded case contains it, so it is at least as hard, which can only make balancing
more expensive and never less. It therefore cannot rescue §5's no-go.

---

## 5. Summarization does not recover

Lemma B.2 charges a SPLIT for `1 + b` calls to `Aggregate` against `1 + (b−1)` calls to
`Rank`/`Select`. §4 fixes the second group. The first group is where the extension actually fails,
and it fails on an operation Def. 3.9 has had since the start.

**5.1 The bound.** [Lar12] proves `t_q = Ω((lg n / lg(w·t_u))²)` for **dynamic weighted orthogonal
range counting in two dimensions**: insertions of points each carrying a `Θ(lg n)`-bit integer
weight, and a query `q = (x, y)` returning the sum of the weights of the points **dominated** by `q`.
There `n` counts update operations, `t_u` is the **worst-case** update time, `t_q` the expected
average query time, and the bound holds for any cell size `w = Ω(lg n)`; at `w = Θ(lg n)` and
polylogarithmic `t_u` it reads `Ω((lg n / lg lg n)²)`. It strengthens [Păt07], which proved
`max{t_q, t_u} = Ω((lg n / lg lg n)²)` for the same problem but only for `lg^{2+ε} n`-bit weights;
[Lar12] brings that requirement down to logarithmic and calls the result "a partial answer" to
[Păt07]'s open problem.

Two steps carry it to Def. 3.9. A box aggregate answers a dominance query, because the quadrant
`(−∞, x] × (−∞, y]` is a box. And a summary that is an additive group of width `≥ Θ(lg n)` answers
`Θ(lg n)`-bit weighted counting by zero-padding the weight. Every fingerprint-style summary in use
meets the width condition, [Amp26]'s own evaluated protocol included: §6.1 records that Negentropy
sums 256-bit identifiers, `Σ(id₁,…,idₖ) = id₁ + … + idₖ (mod 2²⁵⁶)`, an additive group of width
`256 ≫ lg n`. So the bound binds the instantiation the paper measures, not merely a hypothetical
one.

**5.2 The baseline is tight too.** Dynamic partial sums cost `Θ(1 + lg n / lg(w/s))` for cell size
`w` and summand width `s` [PD04], hence `Θ(lg n)` once the summary is at least a word wide. Lemma
B.2's `O(h)` for `Aggregate` is therefore **optimal** in one dimension, not merely adequate — which
is what makes the comparison below a genuine separation rather than an artifact of a loose bound.
[Mey23] §4.3 reaches the same figure independently, from the other end: processing one range
fingerprint over a monoid tree with order-statistic labels costs `O(lg n_i)`. Both papers in the
RBSR line land on the same logarithmic per-operation cost, and the Proposition removes it from both.

> **Proposition.** Let an RSOS over a product order of dimension `δ ≥ 2` support Def. 3.9's
> operations over axis-aligned boxes, with an element-summary monoid containing an additive group of
> width `Ω(lg n)`, under polylogarithmic **worst-case** update time. Then in the cell-probe model with cell size
> `Θ(lg n)`, `Aggregate` requires `Ω((lg n / lg lg n)²)` time, and Lemma B.2's `O(h)` per operation
> — hence Theorem B.3's `T_loc = O(Qh)` — does not survive.
>
> *Proof.* Immediate from [Lar12] by the padding reduction of 5.1. ∎

This is a reduction to a known bound, not a new bound. The contribution is the connection: neither
the reconciliation literature nor §8 had reached for it.

**5.3 The inversion.** At `δ = 2` the primitive Def. 3.9 **lacks** costs `O((lg n / lg lg n)²)` in
`O(n lg n / lg lg n)` space [BJ09], or linear space for its selection half alone [HMN11]; the
operation Def. 3.9 **has** costs `Ω((lg n / lg lg n)²)` unconditionally, and a dynamic range tree
carrying group-valued aggregates [WL85] needs `O(n lg n)` space to get near it. Range selection's best known ceiling is the box aggregate's floor.
Adding what is missing changes nothing asymptotically; what changes everything is the dimension,
and it acts on the operation that was already there.

**5.4 The tax, and where it actually hurts.**

| `n` | `δ = 1`, `lg n` | `δ = 2` floor, `(lg n / lg lg n)²` | ratio | `lg² n`, what one would build | ratio |
|---|---|---|---|---|---|
| 10⁶ | 20 | 21 | **1.07×** | 397 | **20×** |
| 10⁹ | 30 | 37 | **1.24×** | 894 | **30×** |
| 10¹² | 40 | 56 | **1.41×** | 1589 | **40×** |

The floor is almost free; the gap between the floor and any construction we know of is not. A
dynamic 2D range tree with subtree aggregates — the direct `δ = 2` lift of Def. 5.1's
aggregate-augmented B⁺-tree — costs `O(lg² n)` time and `O(n lg n)` space. For an in-memory store a
20–30× regression in both is disqualifying on its own, independently of the Proposition.

**5.5 Why this answers §8 rather than merely restricting it.** [Lar12]'s bound is proved for
*weighted* counting; matching it for **unweighted** counting was posed as an open problem by
[Păt07]. Order statistics in two dimensions are affordable in linear space; it is the weight — the
summary — that is provably not. RBSR cannot drop it: Def. 3.6's `f_p` consumes `A(·)`, and without
`Σ` there is no SKIP test. Of the two things §8 asks for, one has an answer and the other has an
obstruction, and they are not symmetric.

---

## 6. Composite keys: the other domain §8 names

§8 names composite-key reconciliation alongside the multidimensional case. It is the cheaper of the
two — a composite key keeps the store one-dimensional, so every Def. 3.9 operation stays at `O(h)`
and nothing above applies. The question is what it buys.

An RSOS is ordered by a **position map** `π` from a stored record to its position. With `f_p = id`,
a range whose peers hold different cardinalities can never be SKIPped. That guarantee is worth
nothing on a divergence in which every range stays count-balanced — and a same-key, different-value
conflict, which a last-write-wins register produces continuously, is exactly that: both versions
occupy one position, so both peers report the same cardinality everywhere.

The natural repair is a composite key that makes `π` injective. We tested it: 500 records per peer,
differing only in the value at key 250, driven by the unmodified `reconcile-rs` protocol at full
summary width, counting the ranges the driver asked about on which the two peers hold different
cardinalities.

| arm | `π` | the two conflicting records | separable? | rounds | ranges asked | **unbalanced** |
|---|---|---|---|---|---|---|
| 1 | `(key)` | one shared position | no — they tie | 5 | 73 | **0** |
| 2 | `(key, version)` | distinct, but adjacent | **no — adjacency** | 5 | 73 | **0** |
| 3 | `(timestamp, key)` | relocated across the order | yes | 6 | 92 | **16** |

*(`rbsr/tests/balance_under_position_map.rs`; reproduced 2026-08-20.)*

**Arm 2 is the refutation.** Injectivity is achieved and nothing changes. Separating two records
requires a cut point strictly between them, and by Algorithm 2 cut points are `Select` values — that
is, positions some peer actually holds. `(250, 1)` and `(250, 2)` are adjacent; no peer holds
anything between them; no `Select` can produce a separating cut. Distinct-but-adjacent is exactly as
unseparable as tied, by a different mechanism and with an identical observable outcome.

What decides visibility is whether the update **relocates** the record — whether the *leading*
component of the order is the one that changed. `(key, version)` puts the stable component first and
keeps both versions contiguous. `(timestamp, key)`, Negentropy's order [Neg], puts the changing
component first, so a rewrite moves the record past every record whose timestamp lies between — and
those are the cut points that separate it from its old self.

**The connection to §5.** A composite key is cheap and a product order is expensive, and arm 2 is
the reason they are not substitutes: a lexicographic tuple is a one-dimensional store, its ranges
are intervals rather than boxes, and it answers no query about the trailing components. Both facts
have the same cause. Dimensionality is not purchasable with a tuple key.

---

## 7. A checkable question about Willow

[Amp26] cites Willow's three-dimensional adaptation [Wil] for the balance requirement. This note
implies a fork for any such implementation, decidable from its split rule:

- If it cuts boxes **geometrically**, Def. 3.8 is not satisfied, and `Q` is governed by the domain's
  resolution and the data's skew rather than by any depth argument.
- If it cuts boxes at **balanced counts**, it is answering §4's range-restricted select, and it pays
  §5's price on every `Aggregate`.

We have not audited any implementation and claim nothing about which holds. We note that the
question is well-posed, cheap to answer from source, and as far as we know has not been asked. It
applies to any future multidimensional RSOS.

For one-dimensional deployments the note is reassuring: §5.2 says Lemma B.2's `O(h)` is optimal, so
[Amp26]'s realization is not merely a good engineering choice at that dimension but the best
available one.

---

## 8. Limitations and open questions

**Limitations.**

1. §3 checks the paper's stated results against a product order by inspecting their proofs; it does
   not formalize the `δ`-dimensional protocol or write out the induction.
2. §5's Proposition is a reduction to [Lar12], not a new lower bound, and is scoped to summaries
   containing an additive group of width `Ω(lg n)`. Def. 3.4 requires only a monoid; a summary
   outside that scope (`min`/`max`, say) is not covered, and we make no claim about it.
3. There is no `δ ≥ 2` implementation and no measurement at `δ ≥ 2`. §6 is one-dimensional and bears
   on the position map, not on the cost model.
4. §7 poses a question about deployed systems; it is not an audit of one.
5. §4's cost is cited for the **semi-bounded** case, where [HMN11]/[BJ09] leave the cut axis free.
   RBSR refines fully-bounded boxes, which contain that case and are therefore at least as hard. No
   source read here pins the fully-bounded cost. This can only raise the balancing price, so §5's
   conclusion is unaffected — but §4's "solved" should be read as "solved for the case that lower-
   bounds it".
6. [Lar12]'s `t_u` is **worst-case**; the Proposition inherits that hypothesis. Whether the bound
   extends to amortized update time is not settled by the sources read. ([PD04]'s `δ = 1` bound
   does explicitly allow amortization and Las Vegas randomization, so the baseline half is safe.)

**Open questions.**

- **Does the `(lg lg n)²` gap close for a group-valued box aggregate at a useful update time?**
  [Lar12] states its bound "is also tight for any update time that is at least `lg^{2+ε} n`" — so
  the gap does close, but by *raising* the update cost, which for an RSOS is the wrong direction:
  the aggregate is maintained on every write. What is open is the corner an RSOS actually wants —
  `O((lg n / lg lg n)²)` box aggregation for group weights at *polylogarithmic* update time, ideally
  in linear space as [HMN11] achieves for selection. It would not affect the Proposition.
- **Is the unweighted case genuinely easier?** [Păt07]'s open problem is the hinge of §5.5. A
  matching bound for unweighted counting would move the obstruction from summarization to dimension
  itself; a separation would confirm the asymmetry.
- **Does a per-session static snapshot help?** This is the sharpest of the four, because the static
  figures are strikingly good: static range selection is `Θ(lg n / lg lg n)` in **linear** space
  [JL11, BJ09], adaptively `O(1 + lg_w k)` [CW11], and static 2-D box *counting* is `O(lg_w n)` in
  linear space, worst-case optimal [CW11]. All of these are **cheaper than one dynamic dimension**.
  RBSR observes one snapshot per round (Def. 3.9's operations are read-only within a round), so the
  question is whether a structure can be maintained dynamically but *queried* as a static one within
  a session without paying `Θ(n)` to build the static view. If it can, §5's dynamic bound is the
  wrong one to quote and the no-go weakens.
- **Is Def. 3.8 stronger than the protocol needs? — half-answered, and the open half is the best
  route left.** Def. 3.8 demands parts of exactly `⌊m/b⌋` or `⌈m/b⌉`. The round bound does not.
  [Mey23] §5.1 already establishes that peers may "split ranges into equally-sized subranges first,
  but then randomly shift the range boundaries by a small number of items", which "preserves a
  logarithmic number of communication rounds in the worst case" — and that boundaries "chosen fully
  at random" still give `O(lg n)` rounds with high probability, being the height of a random
  `b`-complete tree. Exactness is therefore **not** load-bearing on the protocol side, and §4's
  primitive is stronger than RBSR needs. What is *not* settled is the data-structures side: **is
  approximate or randomized selection within a box asymptotically cheaper than exact range
  selection?** Approximate selection is usually cheaper than exact, so this is a well-posed question
  with a plausible answer, and it is the one route left to an affordable balancing half at
  `δ ≥ 2`. It would not touch §5, which is where the no-go actually lives.

---

## 9. Conclusion

§8 of [Amp26] asks for a clearer theory of balancing and summarization beyond one dimension. The two
are not alike. Balancing fails at a nameable line — `Select(Rank(l) + ⌊jm/b⌋)`, Algorithm 2 — and
the operation that replaces it is dynamic range selection, tight, and available in linear space.
Summarization does not fail at a line; it meets a cell-probe lower bound that binds the aggregate
Def. 3.9 already carries, and that lower bound exists *because* of the summary and not because of
the dimension. The paper's correctness result, meanwhile, needs nothing from either and transports
untouched.

A multidimensional RSOS is therefore buildable and provably not cheap, and the cost is not where the
extension was expected to run into trouble.

---

### Draft status

Every source this note cites for a bound has now been read as a primary document, and each did
change something: [Lar12]'s dominance query and worst-case `t_u` (Limitation 6), [HMN11]'s
selection-only Theorem 1 (Limitation 5), [BJ09]'s range-rank support and `O(n lg n / lg lg n)` space
(§4), [JL11]'s space-parameterized form (§4), [CW11]'s exact adaptive bound and the
selection/3-sided-counting link (§4, §8). [Păt07] was supplied as a scanned document and could not
be machine-read; the claims attributed to it here — the `lg^{2+ε} n`-bit weighted bound and the
open problem for unweighted counting — are taken from [Lar12]'s own account of it, which is a
primary source for the attribution though not for the proof.

[Amp26] (v1) and [Mey23] have both been read in full, and every claim this note makes about either —
[Amp26]'s Def. 3.4–3.9, Algorithm 1–2, Prop. 4.1, Lemma B.2, Theorem B.3, Def. B.1, Eq. (1), §6.1
and both §8 quotations; [Mey23]'s §3.2.1 footnote 2, §3.2.2 height and round bounds, §4.3 cut
realization and per-fingerprint cost, and §5.1 randomized boundaries — is checked against the
source.

---

## References

- **[Amp26]** E. G. Amparore. *Range-Based Set Reconciliation via Range-Summarizable
  Order-Statistics Stores.* arXiv:2603.19820v1, 2026.
- **[Mey23]** A. Meyer. *Range-Based Set Reconciliation.* IEEE SRDS 2023; arXiv:2212.13567.
- **[HMN11]** M. He, J. I. Munro, P. K. Nicholson. *Dynamic Range Selection in Linear Space.*
  ISAAC 2011, LNCS 7074, 160–169. `doi:10.1007/978-3-642-25591-5_18`; arXiv:1106.5076.
- **[JL11]** A. G. Jørgensen, K. G. Larsen. *Range Selection and Median: Tight Cell Probe Lower
  Bounds and Adaptive Data Structures.* SODA 2011, 805–813. `doi:10.1137/1.9781611973082.63`.
- **[Lar12]** K. G. Larsen. *The Cell Probe Complexity of Dynamic Range Counting.* STOC 2012, 85–94.
  `doi:10.1145/2213977.2213987`; arXiv:1105.5933.
- **[Păt07]** M. Pătraşcu. *Lower Bounds for 2-Dimensional Range Counting.* STOC 2007, 40–46.
  `doi:10.1145/1250790.1250797`.
- **[PD04]** M. Pătraşcu, E. D. Demaine. *Tight Bounds for the Partial-Sums Problem.* SODA 2004;
  journal version *Logarithmic Lower Bounds in the Cell-Probe Model*, SIAM J. Comput.
  35(4):932–963, 2006.
- **[BJ09]** G. S. Brodal, A. G. Jørgensen. *Data Structures for Range Median Queries.* ISAAC 2009,
  LNCS 5878. Static: linear space, `O(lg n / lg lg n)`. Dynamic: `O(n lg n / lg lg n)` space,
  `O((lg n / lg lg n)²)` queries and updates, covering range selection **and** dominance (range
  rank).
- **[CW11]** T. M. Chan, B. T. Wilkinson. *Adaptive and Approximate Orthogonal Range Counting.*
  SODA 2011.
- **[Aga17]** P. K. Agarwal. *Range Searching.* In *Handbook of Discrete and Computational
  Geometry*, 3rd ed., ch. 41. CRC Press, 2017.
- **[WL85]** D. E. Willard, G. S. Lueker. *Adding Range Restriction Capability to Dynamic Data
  Structures.* J. ACM 32(3), 1985.
- **[Wil]** Willow Contributors. *3d Range-Based Set Reconciliation.*
  https://willowprotocol.org/specs/rbsr/ (the URL [Amp26] cites as its ref. [25])
- **[Neg]** Negentropy. https://github.com/hoytech/negentropy

---

*Source and measurements: [`reconcile-rs`](https://github.com/Akvize/reconcile-rs) —
`rbsr/tests/balance_under_position_map.rs` for §6, `ARCHITECTURE.md` §7 for the decision this note
records, [issue #360](https://github.com/Akvize/reconcile-rs/issues/360) for its history.*
