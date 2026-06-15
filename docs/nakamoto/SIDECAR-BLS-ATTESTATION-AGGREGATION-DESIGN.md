# Sidecar BLS Committee-Attestation Aggregation — Design

**Status:** DRAFT 2026-06-05. Extends [[BLS-AGGREGATE-SIGNATURE-DESIGN.md]] (task #23) with a SECOND application
(committee attestations) and moves the *aggregation* into the Go sidecar.

## 1. Goal

Collapse the committee-gate's **O(N) per-attestation crypto** at the chain tip to **O(1)**.

Today, for every metagraph binary, gl0 verifies N committee attestations, each costing **three** crypto ops
(`MetagraphCommitteeGate.verifyReceived`): Ed25519 (identity) + KES (freshness) + VRF (committee membership). Under
8gl0+4mg+4shards this CPU-saturated gl0 (load 156 on 24 cores), ground attestation-2/3 finality to **>70s/snapshot**
(step-like progression), stalled gl0 at ~ord 139 → metagraph-alignment timeout → e2e balance-assertion failure.

The Scala dedup (`alreadyRecorded`, committed `5bbfd3eef`) already kills *re-delivery* re-verification. This design
kills the **base O(N)** cost: the committee's N attestations become **one BLS aggregate** the sidecar builds and the
JVM verifies **once**.

**Invariant:** the sidecar makes the JVM verify *less and bigger* — it NEVER verifies *instead*. Verification stays
single-sourced in the JVM (no dual crypto stack → no cluster-split footgun).

## 2. Why this is a clean fit

- **Same-message aggregate.** Every committee member attests to the SAME message `(mg, parent, binaryHash)`. That is
  the textbook case for `BLS12_381ProofOfPossession.fastAggregateVerify(pks[], msg, aggSig)` — already the chosen
  primitive in the BLS design (§B). One pairing check verifies the whole committee.
- **PoP ciphersuite already chosen.** `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_` — the rogue-key-safe, same-message
  variant. The registry already carries `blsPoP` (96B G2). No new crypto decisions.
- **The sidecar already sees every attestation** on the `metagraph-attestation` / `shard-checkpoint-attestation`
  GossipSub topics (`PublishAttestation` + subscribe). It is the natural aggregation point — aggregation is just G2
  point addition (`BLS12_381Aggregation.aggregate`), trivial and fast in Go (blst/gnark-crypto).

## 3. Dependency map (what gates what)

```
#22 participating-set ─ epoch-anchored active ROSTER (HistoricalStakeSnapshot{participating} / participatingSetAt) ──┐
#23 BLS port (BC 1.85) ─ SIGNATURE primitive: BlsSigner, fastAggregateVerify, ValidatorKeyRegistry+blsPoP, PoP ──────┤
   │   (gated on BC 1.85 stable; proven via 1.85 spike, KAT byte-match py_ecc; repo pins 1.83)                       │
   ├─► Part B: snapshot-cert aggregation (existing #23 target)                                                       │
   └─► THIS: committee-attestation aggregation ◄───────────────────────────────────────────────────────────────────┘
            ──► sidecar aggregates (Go blst) ──► JVM fastAggregateVerify against participatingSetAt(N−2)[bitmap]
```

This design **reuses** #23's registry/PoP/`fastAggregateVerify` for the SIGNATURE and #22's `participatingSetAt` for
the ROSTER (never a static list). It adds: (a) a committee-attestation *envelope* carrying a BLS sig + bitmap index,
(b) Go-side aggregation in the sidecar, (c) a JVM aggregate-verify gate path, (d) cross-lang KATs (Go blst ↔ JVM BC
1.85). The participating set rotating at epoch boundaries (demotion = exclusion) is what keeps the roster — and thus
the bitmap — correct over time.

## 4. End-state flow

1. **Attest (each committee member, JVM):** member signs `msg = H(mg ‖ parent ‖ binaryHash ‖ eta-domain)` with its
   registered **BLS** key → 96B G2 sig. Gossips `{mg, parent, binaryHash, attesterIndex, blsSig}` (attesterIndex = the
   member's slot in the committee's canonical-ordered roster, so the receiver knows which pubkey).
2. **Aggregate (sidecar, Go):** per `(mg, parent, binaryHash)`, the sidecar accumulates a **running aggregate** G2 sig
   (point-add each incoming sig) + an **attester bitmap** (bit i set = committee slot i included). Dedups by
   `(key, attesterIndex)` so a re-delivery is a no-op (idempotent point-add guard). When the bitmap popcount ≥
   `kQuorum` (or a short batch window elapses), forward to the JVM: `{mg, parent, binaryHash, bitmap, aggSig}`.
3. **Verify-once (JVM):**
   a. Compute the **deterministic committee** for `(eta, mg, parent)` via the existing VRF sortition — ONCE, not per
      attestation. (`CommitteeSortition` is deterministic given eta + validator set.)
   b. Check `bitmap ⊆ committee` and `popcount(bitmap) ≥ kQuorum` (cheap set ops, no crypto).
   c. Gather the BLS pubkeys for the set bits from the `ValidatorKeyRegistry` (PoP already verified at registration).
   d. **`fastAggregateVerify(pubkeys, msg, aggSig)`** — one pairing check for the whole committee. O(1) in N.
   e. On success → admit the binary (replaces the N-way `recordReceivedAttestation` verify loop).

VRF membership cost goes O(N)-crypto → O(committee)-compute-once + O(N)-bitset; BLS identity verify goes O(N) → O(1).

## 5. Design decisions

- **D1 — BLS replaces Ed25519+KES *for the ephemeral committee attestation*; KES stays for the durable record.**
  The committee attestation is a real-time admission *vote*, consumed at the tip and then subsumed by the **finalized
  global snapshot** (whose certificate is the durable, hashed, long-range artifact — that's #23 Part B's KES+BLS
  target). Forward-security on the ephemeral vote buys little (a later key compromise can't un-finalize an
  already-finalized snapshot). So: committee attestation = **BLS-only** (aggregatable, fast); long-range/history-
  revision protection lives in the finalized snapshot cert (KES-protected, separately). **OPEN: confirm we accept
  BLS-static for the ephemeral vote** — the alternative (retain a per-attester KES alongside BLS) keeps forward-
  security but is NOT aggregatable → only a partial O(N)→O(1) win (BLS aggregates, KES stays O(N)). Recommend BLS-only.
- **D2 — The roster is the EPOCH-ANCHORED PARTICIPATING SET, NEVER static.** The validator set rotates
  (join / leave / demote / re-promote), so the bitmap must index the *dynamic* active set as of the attestation's
  signer epoch — not a fixed list. Use the deterministic, MPT-backed **participating set** from
  [[EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md]] (task #22): `StakeRegistry.participatingSetAt(signerEpoch)` — the
  `SortedMap[PeerId, ParticipationRatio]` recorded at the eta boundary in
  `HistoricalStakeSnapshot{stakes, eta, participating}` (or GlobalStateFieldId 25). The committee for `(eta, mg, parent)`
  is sortitioned FROM this set; the **bitmap indexes its canonical PeerId-sorted roster as of `signerEpoch = N−2`**
  (the Cardano-style staggering — the signer set is the already-FINALIZED epoch N−2, so the roster is stable + agreed
  before any attestation in epoch N is signed). Attester (pick its bit) and verifier (map bits→pubkeys via the
  registry) derive the SAME roster from the consensus-anchored participating set → the bitmap is cross-node-identical,
  and **rotation is handled for free** (the set is fixed within an epoch, rotates only at boundaries).
  **Security bonus:** demotion = *absent from that epoch's participating set* → no bitmap slot → a demoted or
  compromised validator's stale BLS key simply cannot contribute to the aggregate. **The participating set IS the
  access-control for who may attest.** This is the Map-vs-SortedMap boundary precisely: the *tally* stays local `Map`,
  but the *participating-set roster* the aggregate-verify consumes is the sorted, MPT-backed, deterministic basis.
- **D2a — Dependency on #22, not only #23.** The aggregation roster depends on the participating-set work landing.
  The participating set is the single deterministic "who is active this epoch" source already consumed by live finality
  (`TipTracker`) and the light-client cert (S5); the BLS bitmap is one MORE consumer of the SAME basis — do NOT invent
  a parallel roster. Sequencing: #22 (participating set) + #23 (BLS port) both precede the committee-aggregation slices.
- **D3 — Sidecar aggregates but does NOT verify.** It point-adds sigs and tracks the bitmap. A garbage sig that fails
  in the JVM fails the WHOLE aggregate → the JVM falls back to per-attester verify for that `(mg,parent,binary)` to
  isolate+drop the bad contributor (rare; only under an active malicious gossiper). The sidecar is untrusted; the JVM
  is the authority. (This fallback path is why we keep `verifyReceived` alive during + after migration.)
- **D4 — Cross-lang byte-compat (Go blst ↔ JVM BC 1.85).** Both implement BLS12-381 + the IETF POP ciphersuite;
  aggregation is curve point-addition (library-independent) and the wire form is standard compressed G2 (96B). Lock it
  with **KATs**: Go blst aggregate of K fixtures must `fastAggregateVerify` GREEN in BC 1.85 (mirror the metakit MPT
  cross-lang harness pattern). Pick **blst** (Eth2-grade, audited) for the sidecar.
- **D5 — Batch/quorum trigger.** Forward to the JVM when popcount ≥ kQuorum OR a small time window (e.g. 1 snapshot
  cadence) elapses, whichever first — so a binary that never reaches quorum still surfaces what it has, and a healthy
  binary admits as soon as the aggregate is sufficient. Late attesters past quorum are dropped (the admit is settled).

## 6. Slice plan

**Phase 0 — DONE.** Scala dedup `alreadyRecorded` (`5bbfd3eef`). Kills re-delivery re-verification.

**Phase 1 — Edge dedup + quorum short-circuit + bounded fan-out (NO BLS dep — do now for immediate relief).**
- 1a (Go): sidecar content-dedup by `(mg,parent,binary,sender)` before forwarding (catches cross-topic re-delivery the
  GossipSub message-id cache misses). Strictly upstream of the Scala dedup — saves gRPC + JVM receive.
- 1b (Scala): quorum short-circuit — skip `verifyReceived` once `thresholdReached(kQuorum)` for the binary.
- 1c (Scala): bound the verify fan-out (Semaphore / `parEvalMapN` ≈ cores) — the unbounded blocking-fiber fan-out is a
  big slice of load 156 on 24 cores.

**Phase 2 — Land the BLS primitive (= #23, BC 1.85).** Registry+blsPoP, `BlsSigner`, `fastAggregateVerify`, PoP at
registration, KATs. Gated on BC 1.85 stable (or proceed on a 1.85 branch behind a feature flag). Shared with snapshot
certs (#23 Part B).

**Phase 3 — Committee attestation carries BLS (Scala).** New attestation envelope field `blsSig` + `attesterIndex`;
canonical committee roster (D2); attester signs with its registry BLS key. Keep Ed25519+KES path in parallel behind a
flag for migration/fallback.

**Phase 4 — Sidecar aggregation (Go).** Per-key running aggregate + bitmap (D3), blst, dedup-idempotent point-add,
quorum/window trigger (D5), gRPC `SubmitAggregateAttestation{mg,parent,binary,bitmap,aggSig}` → JVM. KATs (D4).

**Phase 5 — JVM aggregate-verify gate (Scala).** `fastAggregateVerify(committee[bitmap], msg, aggSig)` replaces the
N-way verify loop; per-attester fallback (D3) on aggregate failure. Wire into `MetagraphCommitteeGate` /
`ShardCheckpointGl0AcceptanceManager` (same gate for shards).

**Phase 6 — Cutover + cleanup.** Default-on aggregate path; retire the per-attestation gossip→verify loop (keep the
fallback). Metrics: aggregate-verify count, fallback rate, attesters-per-aggregate.

## 7. Security

- **Rogue-key:** defended by PoP (each registered BLS key proven possessed) — the chosen `_POP_` ciphersuite +
  `fastAggregateVerify` is sound under PoP. PoP verified once at registration (#23), not per aggregate.
- **Bitmap integrity:** the JVM independently recomputes the committee (VRF sortition) and the pubkey set from the
  bitmap; a forged bitmap (claiming non-members or non-attesters) yields a pubkey set the aggregate sig won't verify
  against → reject. The sidecar cannot admit a binary the committee didn't actually sign.
- **Single verification authority:** Go aggregates (untrusted); JVM verifies (authority). Determinism preserved — the
  aggregate-verify input (roster order + msg) is deterministic (D2); the aggregate G2 sum is order-independent (point
  addition is commutative), so sidecars/peers that aggregate in different arrival orders produce the SAME aggregate.
- **Forward-security:** explicitly traded away for the *ephemeral vote* (D1); retained for the *durable snapshot cert*
  (#23 Part B). Confirm acceptance.

## 8. Risks / gating

- **BC 1.85 stable** — the hard gate for Phases 2-5 (Phase 1 is independent, do now). Proven via spike; one-line bump.
- **Cross-lang KAT** — must be GREEN before Phase 4 trust (Go blst aggregate ↔ JVM verify). Mirror the metakit MPT
  cross-lang harness.
- **Migration** — Phases 3-5 run dual-path (Ed25519+KES *and* BLS) behind a flag; cut over only after e2e-green on the
  aggregate path; keep the per-attester fallback permanently for the bad-contributor case.
- **D1 acceptance** (BLS-static ephemeral vote) — the one open product decision.

## 9. Integration with the eta-rotation amortization (the shared epoch boundary)

The eta boundary computes THREE epoch-scoped quantities — today all at once, which is the v18-v20 spike: (1) the eta
nonce (VRF fold over the first 2/3 of the period), (2) the stake snapshot, (3) the **participating set** (#22). The
**eta-rotation amortization** (rolling fold frozen at the finalized 2/3-mark + incremental stake / participation / SMT)
makes all three INCREMENTAL — running accumulators frozen at the boundary as O(1), finality-gated — so the boundary is
"freeze the accumulators," not "recompute the epoch."

The pivot: **the participating set (3) IS the BLS roster (D2).** So amortization and BLS aggregation are
producer↔consumer over the same artifact, under one finality-gate:

- **Producer — amortization.** Per finalized snapshot, fold incrementally: VRF-nonce → eta; stake Δ → stakeSnap;
  Slice-17 participation counter → participating set; commitment → smtRoot. Freeze all four at the finalized 2/3-mark.
  This is exactly where the **deferred eta finality-gate** (k1/R refactor item 5 — the `bestTip → finalized-tip` TODO
  in `NakamotoChainStore.vrfOutputsForPeriod`) lands: the fold reads FINALIZED snapshots, and `R ≥ 3·k1` guarantees the
  first-2/3 is settled by the boundary.
- **Consumer — BLS aggregation.** The frozen participating set as of `signerEpoch = N−2` is the roster the bitmap
  indexes (D2) — stable + agreed before any epoch-N attestation is signed ⇒ cross-node-identical bitmap.
- **One finality-gate, three consumers.** eta, participating set, and the BLS roster all read the SAME finalized
  2/3-mark state. The "33-ordinal runway IS the finality buffer" (R ≥ 3·k1) argument covers all three at once.

```
per-snapshot (O(1) incremental):   VRF-fold   +   stake-Δ   +   participation-counter   +   SMT-insert
                                       │            │                  │                       │
finalized-2/3-mark FREEZE:           eta       stakeSnap        PARTICIPATING SET           smtRoot
                                                                       │ (#22)
                          ┌─────────────────────────────────────────────┼────────────────────────────┐
                  live finality (TipTracker)        light-client cert (S5)        BLS bitmap roster (#30, this doc)
```

Two "make the tip cheap" fixes meeting at the participating set: the amortization kills the periodic eta-rotation CPU
SPIKE (the v18-v20 stall class); the BLS aggregation kills the per-snapshot committee O(N) crypto (the v21-class
near-tip cost).

**Sequencing.** The amortization lands independently (perf + correctness refactor of the existing eta/stake/participation
boundary; it also discharges the k1/R deferred finality-gate). It is the PREREQUISITE that makes the BLS roster a clean,
finalized-frozen artifact — so the order is **amortization → participating set finalized-frozen (#22) → BLS
aggregation (#30)**.

Related: [[BLS-AGGREGATE-SIGNATURE-DESIGN.md]] · [[EPOCH-PARTICIPATION-AND-DEMOTION-DESIGN.md]] · committee gate `MetagraphCommitteeGate.scala` / aggregator
`MetagraphAttestationAggregator.scala` · sidecar `p2p/internal/grpcserver/server.go` · shard gate
`ShardCheckpointGl0AcceptanceManager.scala`.
