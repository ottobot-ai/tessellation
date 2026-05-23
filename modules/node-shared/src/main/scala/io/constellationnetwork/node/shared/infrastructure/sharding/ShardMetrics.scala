package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}

import eu.timepit.refined.auto._

/** Shard-scoped Prometheus emission helpers — Slice 19 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 19.
  *
  * Centralises the `dag_nakamoto_shard_*` metric keys and the (shardId, *) tagging pattern so call sites
  * ([[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager]],
  * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore]],
  * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker]]) emit with a single line each, against the same
  * `(metricKey, labelSet)` shape, so the dashboards never have to learn a per-emitter dialect. All keys are namespaced under
  * `dag_nakamoto_shard_` to keep the existing `dag_nakamoto_*` family separable in Grafana.
  *
  * '''Why a thin helper, not a typeclass.''' The Slice 19 surface is six metrics; introducing a new typeclass would require wiring the
  * helper as an injected dep through every call site even though the underlying `Metrics[F]` typeclass is already in scope. The thin helper
  * preserves the existing pattern from `NakamotoMetrics`
  * (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoMetrics.scala`) — top-level
  * constants + small `Metrics[F]`-using methods.
  *
  * '''Why `unsafeLabelName`.''' The label names are compile-time literals known to satisfy the `[a-z0-9]+(?:_[a-z0-9]+)*` Refined regex;
  * we'd otherwise wrap each in `refineV` boilerplate.
  */
object ShardMetrics {

  // ---- Metric keys ---------------------------------------------------------
  //
  // All under `dag_nakamoto_shard_*`. Counter keys end in `_total` per Prometheus convention; gauge keys do NOT
  // (matches the existing `dag_nakamoto_*` family in `NakamotoMetrics`).

  /** Counter — one increment per checkpoint accepted. Labels: `shard_id`, `path` ∈ {`t_count`, `t_depth1`}. */
  val CheckpointTotal: MetricKey = "dag_nakamoto_shard_checkpoint_total"

  /** Counter — one increment per checkpoint rejected. Labels: `shard_id`, `reason`. */
  val CheckpointRejectedTotal: MetricKey = "dag_nakamoto_shard_checkpoint_rejected_total"

  /** Counter — one increment per committee attestation received (recorded in `ShardTipTracker.recordAttestation`). Labels: `shard_id`. */
  val CommitteeAttestationTotal: MetricKey = "dag_nakamoto_shard_committee_attestation_total"

  /** Counter — one increment when a shard takes the `T_depth1_shard` fallback path (committee partly offline). Labels: `shard_id`. */
  val CommitteePartitionTotal: MetricKey = "dag_nakamoto_shard_committee_partition_total"

  /** Counter — one increment when a shard has been in `T_depth1`-only mode beyond `t-partition-hard-ms` without any `T_count` fire. Labels:
    * `shard_id`.
    */
  val PartitionHardTotal: MetricKey = "dag_nakamoto_shard_partition_hard_total"

  /** Gauge — per-shard `bestTip.shardOrdinal`. Labels: `shard_id`. */
  val ChainHeight: MetricKey = "dag_nakamoto_shard_chain_height"

  /** Gauge — per-shard `lastFinalizedOrdinal`. Labels: `shard_id`. */
  val ChainFinalizedOrdinal: MetricKey = "dag_nakamoto_shard_chain_finalized_ordinal"

  // ---- Label names ---------------------------------------------------------

  private val ShardIdLabel: LabelName = "shard_id"
  private val PathLabel: LabelName = "path"
  private val ReasonLabel: LabelName = "reason"

  /** Reason values for [[CheckpointRejectedTotal]]. We surface five buckets:
    *   - `pre_check_committee` — signer not in committee for `(shard, epoch)`
    *   - `pre_check_ed25519` — Ed25519 sig failed under signer's long-term VK
    *   - `pre_check_kes` — KES product sig failed under registered master VK
    *   - `pre_check_vrf` — VRF proof structural check failed
    *   - `unknown_shard` — shard not in local `finalityTriggers` map
    *   - `re_exec_mismatch` — `T_depth1` re-exec hash differs from committee-signed root
    *   - `other` — fallback bucket; ought to be empty in practice
    */
  object RejectReason {
    val PreCheckCommittee = "pre_check_committee"
    val PreCheckEd25519 = "pre_check_ed25519"
    val PreCheckKes = "pre_check_kes"
    val PreCheckVrf = "pre_check_vrf"
    val UnknownShard = "unknown_shard"
    val ReExecMismatch = "re_exec_mismatch"
    val Other = "other"

    /** Map a free-form diagnostic string from `ShardCheckpointAcceptResult.Rejected.reason` to a bucket label so we don't blow up
      * Prometheus cardinality. Pattern matching here is on the SAME diagnostic prefixes the manager emits in its log lines — see
      * `ShardCheckpointGl0AcceptanceManager.preCheck`.
      *
      * Note this is the ONLY place in the system that pattern-matches the diagnostic string, and only for label bucketing — control flow is
      * governed by the `ShardCheckpointAcceptResult` ADT branches (per `[[feedback-no-string-matching]]`). Bucketing for label cardinality
      * is the design intent of `[[feedback-no-string-matching]]`'s carve-out.
      */
    def fromDiagnostic(reason: String): String =
      if (reason.contains("not in committee")) PreCheckCommittee
      else if (reason.contains("Ed25519")) PreCheckEd25519
      else if (reason.contains("KES")) PreCheckKes
      else if (reason.contains("VRF")) PreCheckVrf
      else if (reason.contains("unknown shard")) UnknownShard
      else if (reason.contains("re-exec mismatch")) ReExecMismatch
      else Other
  }

  /** Path values for [[CheckpointTotal]]. */
  object Path {
    val TCount = "t_count"
    val TDepth1 = "t_depth1"
  }

  // ---- Tagging helpers -----------------------------------------------------

  /** Build a single-tag `TagSeq` carrying `shard_id`. Underlying `ShardId.value.value: Int` is rendered as decimal. */
  def shardIdTag(shardId: ShardId): TagSeq =
    Seq(ShardIdLabel -> shardId.value.value.toString)

  /** Build a two-tag `TagSeq` carrying `shard_id` + `path`. */
  def shardIdPathTags(shardId: ShardId, path: String): TagSeq =
    Seq(ShardIdLabel -> shardId.value.value.toString, PathLabel -> path)

  /** Build a two-tag `TagSeq` carrying `shard_id` + `reason`. */
  def shardIdReasonTags(shardId: ShardId, reason: String): TagSeq =
    Seq(ShardIdLabel -> shardId.value.value.toString, ReasonLabel -> reason)

  // ---- Counter emit helpers ------------------------------------------------

  /** Increment [[CheckpointTotal]]`{shard_id, path}`. Call from `ShardCheckpointGl0AcceptanceManager` on `Accepted`. */
  def incCheckpointAccepted[F[_]: Async: Metrics](shardId: ShardId, path: String): F[Unit] =
    Metrics[F].incrementCounter(CheckpointTotal, shardIdPathTags(shardId, path))

  /** Increment [[CheckpointRejectedTotal]]`{shard_id, reason}`. Caller supplies the bucket via [[RejectReason.fromDiagnostic]] or directly
    * with one of the [[RejectReason]] constants. Call from `ShardCheckpointGl0AcceptanceManager` on `Rejected` /
    * `RejectedReExecutionMismatch`.
    */
  def incCheckpointRejected[F[_]: Async: Metrics](shardId: ShardId, reason: String): F[Unit] =
    Metrics[F].incrementCounter(CheckpointRejectedTotal, shardIdReasonTags(shardId, reason))

  /** Increment [[CommitteeAttestationTotal]]`{shard_id}`. Call from `ShardTipTracker.recordAttestation` (every received committee
    * attestation, regardless of self-vs-peer or eventual quorum).
    */
  def incCommitteeAttestation[F[_]: Async: Metrics](shardId: ShardId): F[Unit] =
    Metrics[F].incrementCounter(CommitteeAttestationTotal, shardIdTag(shardId))

  /** Increment [[CommitteePartitionTotal]]`{shard_id}`. Call from `ShardCheckpointGl0AcceptanceManager` when the `T_depth1_shard` fallback
    * path fires (committee partly offline; depth fallback covers liveness).
    */
  def incCommitteePartition[F[_]: Async: Metrics](shardId: ShardId): F[Unit] =
    Metrics[F].incrementCounter(CommitteePartitionTotal, shardIdTag(shardId))

  /** Increment [[PartitionHardTotal]]`{shard_id}`. Call from `ShardPartitionMonitor` when the per-shard "last T_count fire" timestamp is
    * older than `t-partition-hard-ms`.
    */
  def incPartitionHard[F[_]: Async: Metrics](shardId: ShardId): F[Unit] =
    Metrics[F].incrementCounter(PartitionHardTotal, shardIdTag(shardId))

  // ---- Gauge update helpers ------------------------------------------------

  /** Set [[ChainHeight]]`{shard_id}` to `ord`. Call from `ShardChainStore.store` after recomputing `bestTip`. */
  def setChainHeight[F[_]: Async: Metrics](shardId: ShardId, ord: ShardOrdinal): F[Unit] =
    Metrics[F].updateGauge(ChainHeight, ord.value, shardIdTag(shardId))

  /** Set [[ChainFinalizedOrdinal]]`{shard_id}` to `ord`. Call from `ShardChainStore.finalize` after advancing `lastFinalizedOrdinal`. */
  def setChainFinalized[F[_]: Async: Metrics](shardId: ShardId, ord: ShardOrdinal): F[Unit] =
    Metrics[F].updateGauge(ChainFinalizedOrdinal, ord.value, shardIdTag(shardId))

  /** Internal helper exposed so unit tests can build the same `TagSeq` the production emit path uses without re-exporting the constants. */
  private[sharding] def labelNameShardId: LabelName = ShardIdLabel
  private[sharding] def labelNamePath: LabelName = PathLabel
  private[sharding] def labelNameReason: LabelName = ReasonLabel
  private[sharding] val _unusedLabelHelpers: Unit = {
    val _ = unsafeLabelName("dummy")
  }
}
