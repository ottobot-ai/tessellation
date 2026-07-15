package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Transitional per-(ordinal, hash) latest-attestation margin accumulator currently named `SnowballAccumulator`.
  *
  * '''Executable rule.''' Each peer contributes at most one current count at an ordinal. If that peer changes hash, this implementation
  * removes its old-color count and adds the new-color count. It permanently records the first hash whose current-count margin over the
  * runner-up reaches beta. That is a sticky latest-attestation margin, not Snowball's lifetime confidence accumulation.
  *
  * '''Safety gap.''' There is no K-peer query loop or alpha-majority cascade here. Sticky first-crossing is arrival-order sensitive: two
  * nodes can receive the same eventual peer attestations in different orders and retain different decided hashes. Consequently this type is
  * neither portable decided-attestation evidence nor a proof of the target Avalanche/Snowball optimistic Phase-2 rail.
  *
  * `K` and `Alpha` below are unused constants. Calibration of a different K/alpha/beta simulation does not establish safety of this
  * executable rule. Target `T_weight` must consume a fully specified, authenticated, exact-hash cascade decision; no state-changing sink
  * consumes this accumulator while optimistic activation is dark.
  */
trait SnowballAccumulator[F[_]] {

  /** Record a peer's attestation. Adds one distinct-peer current count to the (ordinal, hash) map. If the same peer previously attested a
    * different hash at this ordinal, its prior contribution is moved from the old hash to the new hash.
    *
    * After updating the accumulator, evaluates the decision rule for this ordinal: `leader_count − runner_up_count >= β` ⇒ decided on
    * `leader_hash`. Decisions are sticky in this implementation; later evidence cannot unset them. That stickiness is the source of the
    * arrival-order gap above and must not be read as proof of Snowball safety.
    */
  def recordAttestation(peerId: PeerId, ordinal: Long, hash: Hash): F[Unit]

  /** Record an attestation already accepted by an external latest-per-peer map. The claimed timestamp is checked again in this
    * accumulator's atomic state transition so concurrent handlers cannot apply an older color after a newer one. Equal timestamps are
    * idempotently rejected. This only keeps transitional telemetry internally coherent; it does not make the accumulator portable consensus
    * evidence.
    */
  def recordAttestationIfNewer(peerId: PeerId, ordinal: Long, hash: Hash, attestedAt: Long): F[Unit]

  /** Look up the decided hash at an ordinal, if any. */
  def decidedAt(ordinal: Long): F[Option[Hash]]

  /** Look up the per-hash accumulator counts at an ordinal. Diagnostic / observability. */
  def accumAt(ordinal: Long): F[Map[Hash, Int]]

  /** Highest ordinal where this accumulator has a sticky decision matching the canonical-chain hash at that ordinal. Walks decisions from
    * highest ordinal down; a matching exact tip identifies its canonical ancestor prefix without introducing a BFT ancestor-vote rule.
    *
    * This method takes no `selfId`, but the stored sticky decision can already differ across observers because receipt ordering differs.
    *
    * @param canonicalHashAt
    *   chain walk: returns our canonical hash at the given ordinal (typically `chainStore.walkBackTo`). Decisions whose hash doesn't match
    *   are skipped (they're decisions on a divergent fork that this observer is not on).
    */
  def highestDecidedOnCanonical(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]]

  /** Drop accumulator state for ordinals strictly below `finalizedOrdinal`. This is transitional ordinal-only pruning. Target reversible
    * Phase 2 must retain or reconstruct exact-hash decision evidence across a density reorg; pruning cannot make the ordinal immutable.
    */
  def pruneBelow(finalizedOrdinal: Long): F[Unit]

  /** Re-bootstrap escape hatch — drop all accumulator state. Pairs with `TipTracker.unsafe_reset`. */
  def unsafe_reset: F[Unit]
}

object SnowballAccumulator {

  /** Beta (β) — the leader-minus-runner-up distinct current-peer margin used by this transitional accumulator.
    *
    * Default: 10. Existing external K/alpha/beta simulations model the intended cascade, not this latest-attestation implementation, so
    * they do not validate this threshold as a production safety bound.
    */
  val Beta: Int = 10 // default; production value flows from HOCON `nakamoto.snowball-beta` via TipTracker.make

  /** K — intended peer-sample size per cascade tick. It is not used by this accumulator.
    */
  val K: Int = 8 // legacy intended default; unused until the target cascade consumes it

  /** Alpha (α) — intended per-round majority threshold out of K. It is not used by this accumulator.
    */
  val Alpha: Int = 5 // legacy intended default; unused until the target cascade consumes it

  /** Mutable internal state — kept in a single `Ref` so the decision evaluation can see a consistent pair-snapshot of `accum` + `decided` +
    * `lastByPeer`. The per-ordinal slice is a `Map[Hash, Int]` of distinct-peer counts.
    *
    *   - `accum(ord)(hash)` = number of distinct peers whose latest attestation at this ordinal pointed to `hash`.
    *   - `decided(ord)` = `Some(hash)` once the current-count margin condition was first satisfied; sticky thereafter.
    *   - `lastByPeer(peer)(ord)` = the peer's previously-recorded hash at this ordinal, so on flip we move the contribution from the old
    *     hash to the new one (preserving the at-most-one-per-peer-per-ordinal invariant).
    */
  final case class State(
    accum: Map[Long, Map[Hash, Int]],
    decided: Map[Long, Hash],
    lastByPeer: Map[PeerId, Map[Long, Hash]],
    latestAcceptedAt: Map[PeerId, Long]
  )

  object State {
    val empty: State = State(Map.empty, Map.empty, Map.empty, Map.empty)
  }

  def make[F[_]: Sync: Metrics](beta: Int = Beta): F[SnowballAccumulator[F]] =
    for {
      stateRef <- Ref.of[F, State](State.empty)
      highestDecidedRef <- Ref.of[F, Long](-1L)
      logger = Slf4jLogger.getLoggerFromName[F]("SnowballAccumulator")
    } yield
      new SnowballAccumulator[F] {

        private def record(
          peerId: PeerId,
          ordinal: Long,
          hash: Hash,
          acceptedAt: Option[Long]
        ): F[Unit] = {
          // `Ref.modify` so we can detect a newly-arrived decision and emit a log line / counter
          // outside the state transition (logging is effectful — keep it out of the pure update).
          // Returns `Some((leaderHash, leaderCount, runnerUp))` when this attestation crossed the
          // β margin for the first time at this ordinal; `None` otherwise (already decided OR margin
          // still short).
          val updated: F[Option[(Hash, Int, Int)]] = stateRef.modify { st =>
            val stale = acceptedAt.exists(ts => st.latestAcceptedAt.get(peerId).exists(_ >= ts))

            if (stale) (st, None)
            else {
              val peerHistory = st.lastByPeer.getOrElse(peerId, Map.empty)
              val priorAtOrd = peerHistory.get(ordinal)

              // Per-peer at-most-one-per-ordinal: if the peer previously attested a different hash at this
              // ordinal, move its current contribution. This is not lifetime confidence accumulation.
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
              val newLatestAcceptedAt = acceptedAt.fold(st.latestAcceptedAt)(ts => st.latestAcceptedAt.updated(peerId, ts))

              // Transitional rule: current leader_count - runner_up_count >= beta. Sticky once set, so arrival order matters.
              val newDecisionInfo: Option[(Hash, Int, Int)] = st.decided.get(ordinal) match {
                case Some(_) => None // already sticky under the transitional first-crossing rule
                case None =>
                  val sortedDesc = ordAccumAfterAdd.toList.sortBy(-_._2)
                  sortedDesc match {
                    case Nil => None
                    case (leaderHash, leaderCount) :: rest =>
                      val runnerUp = rest.headOption.map(_._2).getOrElse(0)
                      if (leaderCount - runnerUp >= beta) Some((leaderHash, leaderCount, runnerUp))
                      else None
                  }
              }
              val newDecided = newDecisionInfo match {
                case Some((h, _, _)) => st.decided.updated(ordinal, h)
                case None            => st.decided
              }

              (State(newAccum, newDecided, newLastByPeer, newLatestAcceptedAt), newDecisionInfo)
            }
          }

          updated.flatMap {
            case Some((leaderHash, leaderCount, runnerUp)) =>
              // INFO log + counter + monotone-ratchet gauge. Keep the log compact — emitted once per
              // first-time decision per ordinal, so volume is bounded by chain length.
              val msg =
                s"DECIDED ordinal=$ordinal hash=${leaderHash.value.take(16)} " +
                  s"leader=$leaderCount runnerUp=$runnerUp β=$beta margin=${leaderCount - runnerUp}"
              logger.info(msg) >>
                Metrics[F].incrementCounter("dag_nakamoto_snowball_decisions_total") >>
                highestDecidedRef
                  .modify(prev => (math.max(prev, ordinal), math.max(prev, ordinal)))
                  .flatMap(h => Metrics[F].updateGauge("dag_nakamoto_snowball_highest_decided_ordinal", h))
            case None => Sync[F].unit
          }
        }

        def recordAttestation(peerId: PeerId, ordinal: Long, hash: Hash): F[Unit] =
          record(peerId, ordinal, hash, None)

        def recordAttestationIfNewer(peerId: PeerId, ordinal: Long, hash: Hash, attestedAt: Long): F[Unit] =
          record(peerId, ordinal, hash, Some(attestedAt))

        def decidedAt(ordinal: Long): F[Option[Hash]] =
          stateRef.get.map(_.decided.get(ordinal))

        def accumAt(ordinal: Long): F[Map[Hash, Int]] =
          stateRef.get.map(_.accum.getOrElse(ordinal, Map.empty))

        def highestDecidedOnCanonical(canonicalHashAt: Long => F[Option[Hash]]): F[Option[Long]] =
          stateRef.get.flatMap { st =>
            // Walk decisions from highest ord down; return the first one whose decided hash matches our
            // canonical hash at that ordinal. The highest matching exact tip identifies its canonical prefix;
            // this is not a GRANDPA/BFT ancestor vote.
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
              lastByPeer = st.lastByPeer.view
                .mapValues(_.filter {
                  case (ord, _) => ord >= finalizedOrdinal
                })
                .toMap,
              latestAcceptedAt = st.latestAcceptedAt
            )
          }

        def unsafe_reset: F[Unit] =
          stateRef.set(State.empty)
      }
}
