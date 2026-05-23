package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.Monad
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.FinalityTrigger
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong

/** Per-shard composite finality trigger pair — Slice 6 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.4.
  *
  * Holds the two shard-layer Phase 1→2 [[FinalityTrigger]] instances built from the existing typeclass machinery
  * (`FinalityTrigger.scala:124-144`):
  *
  *   - `tCountShard` — qualifies a shard ord N when `attestationCountFor(checkpoint-at-N) >= ⌈2·K_S/3⌉` (per design doc §5.4 row 2,
  *     §5.3 quorum rule).
  *   - `tDepth1Shard` — qualifies a shard ord N when `bestTipOrd - N > k1Shard` (per design doc §5.4 row 3; degraded-liveness depth
  *     fallback that fires when the attestation gate stalls).
  *
  * The `latestQualifyingOrdinal` composite follows the same **max-of** composition as the gl0 finality monitor uses across its Phase
  * 1→2 triggers (see `docs/nakamoto/attestation-and-finality.md` §0.2 — "The semantics are max-of: the Phase 2 boundary at any tick is
  * the maximum `latestQualifying` ordinal across all registered Phase-2 triggers. Each trigger gives a *sufficient* condition, not a
  * *necessary* one."). Mirrors `FinalityTrigger.maxLatestQualifyingOrdinal` (`FinalityTrigger.scala:107-113`).
  *
  * '''Phase 2→3 trigger.''' Not included here — per design doc §5.4 row 4, "ARCHIVAL is NOT required for shard chains in v1 — gl0 is
  * the archival anchor". A shard checkpoint becomes archival-finalized when its gl0 admission ord (gl0 ord N that included
  * `shardCheckpoints[s] = sc`) reaches gl0's Phase 3 (T_depth2 at k₂ = 65536). The shard layer has no `T_depth2_shard` analog.
  *
  * '''Why the trigger functions ignore [[FinalityTrigger.ConsensusState]].''' The shard triggers' eval functions close over their
  * captured dependencies ([[ShardChainStore]] and [[ShardTipTracker]]) rather than reading from the [[FinalityTrigger.ConsensusState]]
  * argument. This matches the typeclass scaladoc — "concrete triggers ignore fields they don't care about" — and avoids inventing a
  * second `ConsensusState` shape just for the shard layer. `ConsensusState[F]` carries gl0-flavored fields (`selfId`,
  * `bestTipOrdinal`, `bestTipHash`, `canonicalHashAt`) that aren't load-bearing for shard finality; we pass a dummy state when invoking
  * `evaluateAndAdvance` (see [[ShardFinalityTriggers.advance]]).
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh composite for the shard layer. The gl0 finality monitor composes its own triggers via
  *     `FinalityTrigger.maxLatestQualifyingOrdinal` directly; this class is the analogous boundary for shards.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - `kTarget` and `k1Shard` are constructor params. Callers wire from `cfg.nakamoto.sharding.committeeKTarget` and
  *     `cfg.nakamoto.sharding.finality.k1Shard`. No `sys.env.get` anywhere in this slice.
  */
final case class ShardFinalityTriggers[F[_]](
  shardId: ShardId,
  tCountShard: FinalityTrigger[F],
  tDepth1Shard: FinalityTrigger[F],
  advance: F[ShardOrdinal]
) {

  /** Composite "max-of" qualifier — the highest shard-ordinal that has been qualified by EITHER `tCountShard` or `tDepth1Shard`. Same
    * semantics as the gl0 finality monitor uses across `T_weight + T_count + T_depth1` (`FinalityTrigger.maxLatestQualifyingOrdinal`).
    *
    * Pure read of each inner trigger's `latestQualifyingOrdinal` `Ref` — no chain walk, no recomputation. Bounded constant time.
    *
    * Note: this reads from the inner triggers' Refs that are advanced by [[advance]]; if no caller has invoked `advance` yet on this
    * composite, the result is [[ShardOrdinal.Genesis]].
    */
  def latestQualifyingOrdinal(implicit M: Monad[F]): F[ShardOrdinal] =
    FinalityTrigger
      .maxLatestQualifyingOrdinal[F](List(tCountShard, tDepth1Shard))
      .map(snap => ShardFinalityTriggers.snapshotOrdinalToShardOrdinal(snap))
}

object ShardFinalityTriggers {

  /** Construct the per-shard composite finality triggers. Returns a `F[ShardFinalityTriggers[F]]` because the inner
    * [[FinalityTrigger]] instances back themselves with `Ref[F, SnapshotOrdinal]` and so must be constructed in `F` (per
    * `FinalityTrigger.fromRef[F]`).
    *
    * @param shardId
    *   the shard this trigger pair is scoped to. Stamped on the returned record for diagnostic logging and observability; the inner
    *   triggers don't bind to it because they pull their inputs from the captured `chainStore` and `tipTracker`.
    * @param kTarget
    *   the shard committee's target size (`K_S` in design doc §5.4 row 2 — the cluster-wide constant from
    *   `cfg.nakamoto.sharding.committeeKTarget`). The `T_count_shard` qualifier compares against `⌈2·K_S/3⌉` distinct attesters. v1
    *   stable-σ rule (`[[project-216-committee-stake-drift-fix]]`) treats every committee member as equally weighted; the count check
    *   degenerates to a 1-validator-1-vote tally over a uniform committee.
    * @param k1Shard
    *   shard-layer depth-finality fallback (design doc §5.4 row 3 — the per-shard `k₁`). Default per HOCON is 8 shard-ords ≈ 56s,
    *   smaller than gl0's `k₁ = 255` because shard ords are sparser.
    * @param chainStore
    *   per-shard chain store from Slice 5 (`ShardChainStore`). Read on every advance — `bestTip` provides the head ordinal +
    *   canonical hash; `walkBackTo` (transitively, via `getByOrdinal`) is used by `T_count_shard` to resolve the canonical hash at the
    *   tip's ord for the attestation-count lookup.
    * @param tipTracker
    *   per-shard attestation tracker (Slice 6 — this same file's sibling `ShardTipTracker`). Drives `T_count_shard` via
    *   `attestationCountFor(checkpointHash, excludeSelf = true)`.
    */
  def make[F[_]: Async](
    shardId: ShardId,
    kTarget: Int,
    k1Shard: Long,
    chainStore: ShardChainStore[F],
    tipTracker: ShardTipTracker[F]
  ): F[ShardFinalityTriggers[F]] = {
    // ----- T_count_shard ----------------------------------------------------
    //
    // Qualifies a shard ord N when `attestationCountFor(canonicalHashAt(N), excludeSelf = true) >= ⌈2·K_S/3⌉`.
    //
    // Threshold math via `BigInt` (matches `TCountTrigger.scala:330-335` for cluster-wide hash-determinism — both honest nodes
    // recompute byte-equivalent `required` values and either both qualify or both don't).
    //
    // Self-exclusion is delegated to `ShardTipTracker.attestationCountFor`'s default (#133/P-11b). Citation: see
    // `TipTracker.highestFinalizedOrdinal` scaladoc lines 71-76 and `TCountTrigger` scaladoc lines 274-277.
    //
    // The eval function closes over `chainStore` and `tipTracker`; the `ConsensusState[F]` arg is ignored — the trigger is shard-scoped
    // and reads ALL its inputs from the captured shard dependencies.
    val requiredCount: BigInt = ceilTwoThirds(BigInt(kTarget))

    val countEval: FinalityTrigger.ConsensusState[F] => F[SnapshotOrdinal] = _ =>
      chainStore.bestTip.flatMap {
        case None =>
          // No tip yet → no checkpoint to count attestations for. Genesis-equivalent.
          Async[F].pure(SnapshotOrdinal.MinValue)

        case Some(tip) =>
          // Count attestations on the canonical-bestTip hash. The Ref-backed advance is monotone so calling this with the bestTip
          // hash at every tick is sufficient — once the count crosses threshold for ord N, the Ref locks in at N. A later reorg
          // that demotes ord N to a different hash doesn't roll the Ref back (that's the monotonicity contract — see
          // `FinalityTrigger.evaluateAndAdvance` lines 137-142).
          //
          // Why the bestTip hash specifically (and not "every known checkpoint"): the trigger answers "highest ord qualified", and
          // only the canonical chain matters for that question. A fork-branch checkpoint with K_S attestations doesn't qualify
          // anything on OUR canonical chain because it's not on it — same cross-fork filter principle as `TCountTrigger`'s
          // canonical-hash filter (lines 305-312 in that file).
          tipTracker.attestationCountFor(tip.hash, excludeSelf = true).map { count =>
            if (BigInt(count) >= requiredCount)
              shardOrdinalToSnapshotOrdinal(tip.signed.value.shardOrdinal)
            else SnapshotOrdinal.MinValue
          }
      }

    // ----- T_depth1_shard ---------------------------------------------------
    //
    // Qualifies ord N when `bestTipOrd - N > k1Shard`. Equivalent to gl0's `TDepth1Trigger` mechanics (`FinalityTrigger.scala:227-236`)
    // but scoped to the shard's chain store rather than gl0's `bestTipOrdinal` from `ConsensusState`. We read shard `bestTip` directly
    // from the captured `chainStore` and ignore the `ConsensusState[F]` argument for the same reason as `T_count_shard`.
    //
    // Note the `>` vs `>=`: design doc §5.4 row 3 says "depth > k1Shard" (strict), so an ord-N checkpoint qualifies once the chain
    // extends to ord N + k1Shard + 1 or later. Matches gl0's `bestOrd - k > 0` predicate at lines 230-234 of FinalityTrigger.scala
    // (effectively `bestOrd > k`, which is the strict form of "depth at ord 0 is bestOrd, must exceed k").
    val depthEval: FinalityTrigger.ConsensusState[F] => F[SnapshotOrdinal] = _ =>
      chainStore.bestTip.map {
        case None => SnapshotOrdinal.MinValue
        case Some(tip) =>
          val bestOrd = tip.signed.value.shardOrdinal.value
          // `bestOrd - k1Shard` is the highest ord whose depth at the tip is > k1Shard. Clamp at 0 — chains shorter than k1Shard
          // qualify nothing.
          val qualifying = bestOrd - k1Shard
          if (qualifying > 0L) shardOrdinalToSnapshotOrdinal(ShardOrdinal(qualifying))
          else SnapshotOrdinal.MinValue
      }

    for {
      tCount <- FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TCount, SnapshotOrdinal.MinValue)(countEval)
      tDepth1 <- FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TDepth1, SnapshotOrdinal.MinValue)(depthEval)
    } yield ShardFinalityTriggers(
      shardId = shardId,
      tCountShard = tCount,
      tDepth1Shard = tDepth1,
      advance = {
        // Drive both inner triggers via their `evaluateAndAdvance` with a dummy ConsensusState — the eval functions ignore the state,
        // so the values inside are irrelevant. We pass `selfId = dummySelfId`, ords at MinValue, an empty canonicalHashAt — pure
        // sentinels. The shard-scoped advance returns the new composite max-of in `F[ShardOrdinal]`.
        val dummyState = FinalityTrigger.ConsensusState[F](
          selfId = ShardFinalityTriggers.dummySelfId,
          bestTipOrdinal = SnapshotOrdinal.MinValue,
          bestTipHash = ShardFinalityTriggers.dummyHash,
          canonicalHashAt = _ => Async[F].pure(Option.empty[Hash])
        )
        for {
          _ <- tCount.evaluateAndAdvance(dummyState)
          _ <- tDepth1.evaluateAndAdvance(dummyState)
          maxOrd <- FinalityTrigger.maxLatestQualifyingOrdinal[F](List(tCount, tDepth1))
        } yield snapshotOrdinalToShardOrdinal(maxOrd)
      }
    )
  }

  /** Ceil-div of `2 * x / 3` over `BigInt` — byte-identical across all JVMs/CPUs. Mirrors the threshold math in
    * `TCountTrigger.scala:330-335` (which keys off the `Ratio` denominator/numerator pair; with the fixed shard-layer 2/3 threshold the
    * shape simplifies to this inline helper). Returning `BigInt` keeps the comparison `BigInt >=` consistent with the rest of the
    * shard threshold paths.
    */
  private[sharding] def ceilTwoThirds(x: BigInt): BigInt = {
    val n: BigInt = 2 * x
    val q: BigInt = n / 3
    if (n % 3 == 0) q else q + 1
  }

  /** Convert a shard-domain ordinal to the snapshot-domain ordinal used by the inner [[FinalityTrigger]] `Ref[F, SnapshotOrdinal]`.
    *
    * Bridging is byte-faithful for the shard domain (ord >= 0): `ShardOrdinal` is a raw `Long` per `ShardOrdinal.scala:24-29`
    * (negative-during-bootstrap-init tolerated), but the trigger Refs hold `SnapshotOrdinal` (`NonNegLong`-backed per
    * `SnapshotOrdinal.scala:21-23`). Negative shard ords are clamped to `SnapshotOrdinal.MinValue` (0) here so the bridge never
    * throws — matches the same clamp-at-zero discipline `TDepth1Trigger` uses for chains shorter than `k`.
    */
  private[sharding] def shardOrdinalToSnapshotOrdinal(o: ShardOrdinal): SnapshotOrdinal =
    if (o.value <= 0L) SnapshotOrdinal.MinValue
    else SnapshotOrdinal(NonNegLong.unsafeFrom(o.value))

  /** Inverse projection — the inner trigger Refs only ever hold non-negative values (clamped at construction), so the conversion is
    * total.
    */
  private[sharding] def snapshotOrdinalToShardOrdinal(o: SnapshotOrdinal): ShardOrdinal =
    ShardOrdinal(o.value.value)

  // --- Dummies for the unused ConsensusState fields ----------------------------------------------
  //
  // The shard triggers' eval functions ignore every field of `ConsensusState[F]` — the inputs come from the captured shard `chainStore`
  // and `tipTracker`. We pre-allocate sentinel values so we don't reconstruct them on every `advance` call.

  private val dummySelfId: PeerId =
    PeerId(io.constellationnetwork.security.hex.Hex("00" * 64))

  private val dummyHash: Hash = Hash("0" * 64)
}
