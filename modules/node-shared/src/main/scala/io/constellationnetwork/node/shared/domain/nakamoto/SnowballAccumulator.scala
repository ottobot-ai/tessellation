package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Per-(ordinal, hash) lifetime accumulator implementing **Snowball** decision semantics from the Snow family
  * (Rocco et al. 2018, §3.2; Amores-Sesar & Schneider 2024).
  *
  * '''Why Snowball and not Snowflake.''' Snowflake (single counter per ordinal, reset on every hash flip) is silent
  * under coordinated split-honest adversaries: the counter never reaches β before resetting, so the node never
  * decides. Snowball replaces the reset-on-flip counter with a per-color persistent lifetime accumulator —
  * incoming evidence persists across flips. Decision rule is margin-based: hash `H` is decided when
  * `accum(H) − max{accum(H') : H' ≠ H} ≥ β` (proposal §2.2 algebraic statement; matches the GPU sim kernel
  * at `~/repos/research-nipopos-2026/sims/avalanche_attestation_calibration_gpu.py` commit `5ace3d36`).
  *
  * '''Why this restores Non-Interactive Determinism (NID).''' Snowball is observer-independent: every honest
  * observer plugging in the same attestation transcript reaches the same decision regardless of their own
  * identity. The pre-Snowball P-11b self-exclusion (commit `95471c7f`) was the stopgap that prevented
  * self-finalize-then-deadlock at the cost of NID — two observers given the same transcript could disagree on
  * whether ord N was finalized depending on which `selfId` they plugged in. Snowball deletes the deadlock
  * attractor structurally (peers don't sample themselves in classic Avalanche), so the P-11b workaround comes
  * off and NID is restored.
  *
  * '''Empirical parameters.''' Production-locked `(K=8, α=5, β=10)` for cluster sizes N ≥ 16, per
  * `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` §0.A and the GPU dual-mode sweep at commit `5ace3d36` of
  * `~/repos/research-nipopos-2026` (6696 cells × 10000 trials, three adversary modes — coordinated_lie,
  * split_honest, random_honest — zero safety violations across all measured N at f_adv=0.33). The K parameter
  * sets the peer-sample size for the upstream cascade (not used by this accumulator directly — see §2.4 of the
  * proposal for the relationship); α sets the per-round majority recruitment threshold; β is the
  * leader-minus-runner-up margin (in distinct peer attestation count) required to decide.
  *
  * '''Decision unit.''' β is counted in DISTINCT peer attestations: each peer contributes at most one count to
  * the accumulator for each (ordinal, hash) pair (the `recordAttestation` call replaces a peer's prior
  * attestation for the same ordinal if it switched hashes — Snowball's per-color persistence is what survives
  * a peer's flip, not a buggy double-count). This matches `avalanche_attestation_calibration_gpu.py` line 283+
  * (`d_b[i * 3 + slot] = new_v` — per-color increment in unit steps).
  *
  * '''What this accumulator does NOT do.''' It does not run the full Avalanche cascade — there is no K-sample
  * peer query loop here. This accumulator is the *receiver-side* aggregation of the persistent per-color
  * evidence that downstream finality triggers (`T_weight`, in particular) consume. The full per-validator
  * per-ordinal cascade (with K-sample queries, α-majority test, flip-symmetric rollback) is the future
  * proposal §2.2 work — landing this accumulator is the Phase 1 piece that decouples Snowball semantics from
  * the upstream cascade so the cascade can land later without re-touching the receiver.
  */
trait SnowballAccumulator[F[_]] {

  /** Record a peer's attestation. Adds one distinct-peer count to the (ordinal, hash) accumulator. If the same
    * peer previously attested a DIFFERENT hash at this ordinal, its prior contribution is moved from the old
    * hash to the new hash (per-peer at-most-one-per-ordinal invariant — matches the Snowball semantics where
    * a peer's preference flip carries the accumulator forward without zeroing prior accumulation by OTHER
    * peers, but does not let a single peer double-count itself).
    *
    * After updating the accumulator, evaluates the decision rule for this ordinal:
    * `leader_count − runner_up_count >= β` ⇒ decided on `leader_hash`. Decisions are monotone — once an
    * ordinal is decided, the decision is sticky (a later contradicting attestation does not unset it; this is
    * the "decision is irrevocable" property of Snowball, proposal §2.1 "decided ⇒ quiescent").
    */
  def recordAttestation(peerId: PeerId, ordinal: Long, hash: Hash): F[Unit]

  /** Look up the decided hash at an ordinal, if any. */
  def decidedAt(ordinal: Long): F[Option[Hash]]

  /** Look up the per-hash accumulator counts at an ordinal. Diagnostic / observability. */
  def accumAt(ordinal: Long): F[Map[Hash, Int]]

  /** Highest ordinal where Snowball has decided AND the decided hash matches the canonical-chain hash at that
    * ordinal. Walks decisions from highest ord down (consistent with `TipTracker.highestFinalizedOrdinal`'s
    * GRANDPA ancestor rule — deciding ord N with the canonical hash implies all ancestors of that hash were
    * also implicitly endorsed).
    *
    * '''NID property (load-bearing).''' This method takes NO `selfId` — Snowball's decision is observer-
    * independent. Two observers walking the same canonical chain post-hoc, given identical attestation
    * transcripts, reach IDENTICAL decisions here. This is what P-11b broke and what Snowball restores at the
    * T_weight position of the trigger stack.
    *
    * @param canonicalHashAt
    *   chain walk: returns our canonical hash at the given ordinal (typically `chainStore.walkBackTo`).
    *   Decisions whose hash doesn't match are skipped (they're decisions on a divergent fork that this
    *   observer is not on — same canonical-hash filter as `TipTracker.highestFinalizedOrdinal`).
    */
  def highestDecidedOnCanonical(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]]

  /** Drop accumulator state for ordinals strictly below `finalizedOrdinal`. Mirrors `TipTracker.pruneBelow` —
    * once an ordinal is finalized the per-color history below it is no longer needed for decisions.
    */
  def pruneBelow(finalizedOrdinal: Long): F[Unit]

  /** Re-bootstrap escape hatch — drop all accumulator state. Pairs with `TipTracker.unsafe_reset`. */
  def unsafe_reset: F[Unit]
}

object SnowballAccumulator {

  /** Beta (β) — the leader-minus-runner-up DISTINCT-peer margin required to decide a hash at an ordinal.
    *
    * '''Production default: 10.''' Empirical floor from the GPU dual-mode sweep at commit `5ace3d36` of
    * `~/repos/research-nipopos-2026` (file `sims/data/avalanche_attestation_full_gpu_n10000_v2.json`):
    * `(K=8, α=5, β=10)` zeroes safety violations across all measured N ∈ {16, 32, 100, 500, 1000} under
    * three adversary modes (coordinated_lie, split_honest, random_honest) at f_adv = 0.33.
    *
    * Override via env `NAKAMOTO_SNOWBALL_BETA`. Smaller β decides faster but tolerates a higher noise
    * envelope (proposal §3.4 — Snowball at K=3/β=10 leaks 31 % under split_honest because the per-color
    * accumulator is at noise floor; β=10 at K=8 is the structural fix).
    */
  val Beta: Int =
    sys.env.get("NAKAMOTO_SNOWBALL_BETA").flatMap(_.toIntOption).getOrElse(10)

  /** K — peer-sample size per cascade tick. Not used by this accumulator directly (the upstream cascade in
    * §2.2 of the proposal samples K peers per Δ); exposed here so the leader loop can read both K and β from
    * the same configuration surface.
    *
    * '''Production default: 8.''' Empirical floor from the GPU dual-mode sweep (commit `5ace3d36`) —
    * smaller K leaves the per-color accumulator at noise floor under split-honest adversaries.
    *
    * Override via env `NAKAMOTO_SNOWBALL_K`.
    */
  val K: Int =
    sys.env.get("NAKAMOTO_SNOWBALL_K").flatMap(_.toIntOption).getOrElse(8)

  /** Alpha (α) — per-round α-majority recruitment threshold (out of K) in the upstream cascade. Not used by
    * this accumulator directly. Exposed for cluster-wide configuration parity.
    *
    * '''Production default: 5.''' Empirically `α ≥ ⅝K` is the floor for safety under noise (proposal §0.A,
    * §2.4); at K=8 that gives α ≥ 5.
    *
    * Override via env `NAKAMOTO_SNOWBALL_ALPHA`.
    */
  val Alpha: Int =
    sys.env.get("NAKAMOTO_SNOWBALL_ALPHA").flatMap(_.toIntOption).getOrElse(5)

  /** Mutable internal state — kept in a single `Ref` so the decision evaluation can see a consistent
    * pair-snapshot of `accum` + `decided` + `lastByPeer`. The per-ordinal slice is a `Map[Hash, Int]` of
    * distinct-peer counts.
    *
    * - `accum(ord)(hash)` = number of distinct peers whose latest attestation at this ordinal pointed to
    *   `hash`. Counts are unit-step (proposal §2.2 line `accum(topHash) += 1`).
    * - `decided(ord)` = `Some(hash)` once the margin condition was first satisfied; sticky (Snowball
    *   irrevocability).
    * - `lastByPeer(peer)(ord)` = the peer's previously-recorded hash at this ordinal, so on flip we move the
    *   contribution from the old hash to the new one (preserving the at-most-one-per-peer-per-ordinal
    *   invariant).
    */
  final case class State(
    accum: Map[Long, Map[Hash, Int]],
    decided: Map[Long, Hash],
    lastByPeer: Map[PeerId, Map[Long, Hash]]
  )

  object State {
    val empty: State = State(Map.empty, Map.empty, Map.empty)
  }

  def make[F[_]: Sync](beta: Int = Beta): F[SnowballAccumulator[F]] =
    Ref.of[F, State](State.empty).map { stateRef =>
      new SnowballAccumulator[F] {

        def recordAttestation(peerId: PeerId, ordinal: Long, hash: Hash): F[Unit] =
          stateRef.update { st =>
            val peerHistory = st.lastByPeer.getOrElse(peerId, Map.empty)
            val priorAtOrd = peerHistory.get(ordinal)

            // Per-peer at-most-one-per-ordinal: if the peer previously attested a different hash at this
            // ordinal, move its contribution (the "flip" case — Snowball preserves other peers' lifetime
            // accumulation, but a single peer can't double-count itself).
            val ordAccum = st.accum.getOrElse(ordinal, Map.empty)
            val ordAccumAfterDrop = priorAtOrd match {
              case Some(prevHash) if prevHash =!= hash =>
                ordAccum.get(prevHash) match {
                  case Some(v) if v > 1 => ordAccum.updated(prevHash, v - 1)
                  case Some(_)          => ordAccum - prevHash
                  case None             => ordAccum
                }
              case _ => ordAccum
            }

            // Add this peer's vote for the new hash. If the peer previously attested the same hash, the
            // increment is a no-op (the prior contribution still counts; we don't double-count). Detect by
            // checking that priorAtOrd is None OR differs from hash.
            val ordAccumAfterAdd =
              if (priorAtOrd.contains(hash)) ordAccumAfterDrop // no change — already counted
              else ordAccumAfterDrop.updated(hash, ordAccumAfterDrop.getOrElse(hash, 0) + 1)

            val newAccum = st.accum.updated(ordinal, ordAccumAfterAdd)
            val newPeerHistory = peerHistory.updated(ordinal, hash)
            val newLastByPeer = st.lastByPeer.updated(peerId, newPeerHistory)

            // Decision rule: leader_count − runner_up_count >= β. Sticky once set.
            val newDecided = st.decided.get(ordinal) match {
              case Some(_) => st.decided // already decided; Snowball decisions are irrevocable
              case None =>
                val sortedDesc = ordAccumAfterAdd.toList.sortBy(-_._2)
                sortedDesc match {
                  case Nil => st.decided
                  case (leaderHash, leaderCount) :: rest =>
                    val runnerUp = rest.headOption.map(_._2).getOrElse(0)
                    if (leaderCount - runnerUp >= beta) st.decided.updated(ordinal, leaderHash)
                    else st.decided
                }
            }

            State(newAccum, newDecided, newLastByPeer)
          }

        def decidedAt(ordinal: Long): F[Option[Hash]] =
          stateRef.get.map(_.decided.get(ordinal))

        def accumAt(ordinal: Long): F[Map[Hash, Int]] =
          stateRef.get.map(_.accum.getOrElse(ordinal, Map.empty))

        def highestDecidedOnCanonical(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]] =
          stateRef.get.flatMap { st =>
            // Walk decisions from highest ord down; return the first one whose decided hash matches our
            // canonical hash at that ord. GRANDPA ancestor rule: a decision on the canonical chain at ord N
            // implicitly endorses all ancestors of that hash — the highest matching decision is the
            // qualifying ordinal.
            val descending = st.decided.toList.sortBy { case (ord, _) => -ord }
            descending.foldM[F, Option[Long]](Option.empty[Long]) {
              case (Some(found), _) => Sync[F].pure(Some(found))
              case (None, (ord, decidedHash)) =>
                canonicalHashAt(ord).map {
                  case Some(canonical) if canonical === decidedHash => Some(ord)
                  case _                                            => None
                }
            }
          }

        def pruneBelow(finalizedOrdinal: Long): F[Unit] =
          stateRef.update { st =>
            State(
              accum = st.accum.filter { case (ord, _) => ord >= finalizedOrdinal },
              decided = st.decided.filter { case (ord, _) => ord >= finalizedOrdinal },
              lastByPeer = st.lastByPeer.view.mapValues(_.filter {
                case (ord, _) => ord >= finalizedOrdinal
              }).toMap
            )
          }

        def unsafe_reset: F[Unit] =
          stateRef.set(State.empty)
      }
    }
}
